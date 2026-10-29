"""Single user-wide Unix socket coordinator; test endpoints require fake adapter."""
import fcntl
import json
import os
import secrets
import signal
import socket
import sqlite3
import stat
import time
import threading
import shutil
from pathlib import Path

from .adapter import AndroidAdapter, FakeAdapter
from .core import Fault, Library, secure_write


def production_runtime():
    # Fixed independent of cwd, FM_HOME, --data-dir and XDG overrides.
    return Path.home()/".device-library-runtime"


def validate_private(path):
    st = Path(path).lstat()
    if stat.S_ISLNK(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise Fault("unsafe_private_path", 6)


def read_credential(path):
    validate_private(path)
    value = Path(path).read_text().strip()
    if len(value) < 32:
        raise Fault("invalid_credential_file", 4)
    return value


class Server:
    def __init__(self, data_dir, *, initialize=False, test_runtime=None, fixture=None):
        if bool(test_runtime) != bool(fixture):
            raise Fault("test_runtime_requires_fake_fixture", 6)
        self.runtime = Path(test_runtime) if test_runtime else production_runtime()
        self.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        validate_private(self.runtime)
        if len(str(self.runtime/"coordinator.sock").encode()) >= 104:
            raise Fault("unix_socket_path_too_long", 6)
        self.lock = open(self.runtime/"coordinator.lock", "a+")
        os.chmod(self.runtime/"coordinator.lock", 0o600)
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise Fault("coordinator_already_running", 6)
        # Runtime remembers selected data directory: missing/switched ledger never silently reinitializes.
        data_dir = str(Path(data_dir).resolve())
        manifest = self.runtime/"library.json"
        if manifest.exists():
            record = json.loads(manifest.read_text())
            if record["data_dir"] != data_dir:
                self.lock.close()
                raise Fault("different_data_directory_requires_offline_migration", 6)
            if initialize:
                self.lock.close()
                raise Fault("already_initialized", 6)
        elif not initialize:
            self.lock.close()
            raise Fault("runtime_uninitialized", 6)
        if (self.runtime/"restore-in-progress").exists():
            self.lock.close()
            raise Fault("restore_incomplete_repeat_offline_restore", 6)
        self.admin_path = self.runtime/"operator.credential"
        if not self.admin_path.exists():
            if manifest.exists():
                raise Fault("operator_credential_missing", 6)
            secure_write(self.admin_path, secrets.token_urlsafe(32))
        self.admin = read_credential(self.admin_path)
        adapter = FakeAdapter(fixture) if fixture else AndroidAdapter()
        self.library = Library(data_dir, adapter, initialize=initialize)
        if not manifest.exists():
            secure_write(manifest, json.dumps({"data_dir": data_dir, "library_id": self.library.library_id, "test": bool(fixture)}))
        self.path = self.runtime/"coordinator.sock"
        if self.path.exists():
            if not stat.S_ISSOCK(self.path.lstat().st_mode):
                raise Fault("unexpected_socket_path", 6)
            self.path.unlink()
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(str(self.path))
        os.chmod(self.path, 0o600)
        self.socket.listen(32)
        self.socket.settimeout(1)
        self.running = True
        self.threads = []

    def operator(self, token):
        import hmac
        if not isinstance(token, str) or not hmac.compare_digest(token, self.admin):
            raise Fault("operator_credential_required", 4)

    def dispatch(self, req):
        with self.library.locked():
            return self._dispatch(req)

    def _dispatch(self, req):
        lib = self.library
        command = req.get("command")
        args = req.get("args", {})
        if not isinstance(args, dict):
            raise Fault("invalid_arguments")
        token = req.get("credential", "")
        if not isinstance(token, str):
            raise Fault("credential_must_be_string", 4)
        if command in ("register", "retire", "reconcile", "backup", "retention", "shutdown"):
            self.operator(token)
        if command == "doctor":
            return {"state": "running", "library_id": lib.library_id, "epoch": lib.epoch,
                    "inventory_complete": lib.complete, "last_observation_utc_epoch": lib.last_observation,
                    "observation_reason": lib.last_observation_error,
                    "claims": lib.count_claims(), "data_dir": str(lib.root),
                    "adapter": type(lib.adapter).__name__, "policy": "cooperative-v1",
                    "support": "single host/user; direct actions require cooperative stop; live integration unverified"}
        if command == "devices":
            return {"devices": lib.devices(), "claims": [dict(r) for r in lib.db.execute("SELECT * FROM claims")], "inventory_complete": lib.complete}
        if command == "register":
            return lib.register(args["kind"], args["alias"], args["spec"])
        if command == "retire":
            return lib.retire(args["id"], args["reason"], args.get("stopped", False))
        if command == "borrow":
            if type(args.get("owner_pid")) is not int or args["owner_pid"] <= 1:
                raise Fault("borrow_requires_owner_pid")
            return lib.borrow(args["borrower"], args["session"], args["key"], args["body"], token, owner_pid=args["owner_pid"])
        if command in ("request", "cancel"):
            lib.auth_request(args["id"], token)
            return lib.request(args["id"]) if command == "request" else lib.cancel(args["id"], token)
        if command == "lease":
            lib.auth_lease(args["id"], token, current=False)
            return lib.lease(args["id"])
        if command == "renew":
            return lib.renew(args["id"], token)
        if command == "return":
            return lib.return_lease(args["id"], token, args.get("reason", "borrower_return"))
        if command == "recover":
            return lib.recover(args["id"], token, args["kind"])
        if command == "acknowledge":
            return lib.acknowledge(args["id"], token, args["state"])
        if command == "reconcile":
            return lib.operator_reconcile(args["device"], args["reason"], args.get("stopped", False), args.get("operations_ended", False))
        if command == "operation":
            row = lib.db.execute("SELECT * FROM operations WHERE id=?", (args["id"],)).fetchone()
            if not row:
                raise Fault("operation_not_found")
            return lib.unpack(row)
        if command in ("history", "watch", "audit"):
            return {"events": lib.history(**args), "policy": "cooperative-v1", "ordinary_actions": "unobserved"}
        if command == "backup":
            return lib.backup(args["path"])
        if command == "retention":
            return lib.retention()
        if command == "shutdown":
            self.running = False
            return {"state": "stopping", "leases": "retained and fenced on restart"}
        raise Fault("unknown_command")

    def handle(self, conn):
        with conn:
            conn.settimeout(3)
            try:
                data = bytearray()
                while b"\n" not in data:
                    chunk = conn.recv(65536)
                    if not chunk:
                        raise Fault("incomplete_request")
                    data.extend(chunk)
                    if len(data) > 1024*1024:
                        raise Fault("request_too_large")
                req = json.loads(bytes(data).split(b"\n",1)[0])
                if not isinstance(req, dict):
                    raise Fault("request_must_be_object")
                result = self.dispatch(req)
                response = {"schema_version": 1, "ok": True, "library_id": self.library.library_id,
                            "epoch": self.library.epoch, "result": result}
            except Fault as exc:
                response = {"schema_version": 1, "ok": False, "reason": exc.reason, "exit_code": exc.code}
                if exc.details is not None:
                    response["details"] = exc.details
            except (KeyError, ValueError, TypeError, socket.timeout):
                response = {"schema_version": 1, "ok": False, "reason": "invalid_request", "exit_code": 2}
            except sqlite3.Error:
                self.running = False
                response = {"schema_version": 1, "ok": False, "reason": "ledger_unwritable_or_corrupt", "exit_code": 6}
            try:
                conn.sendall(json.dumps(response).encode()+b"\n")
            except (BrokenPipeError, ConnectionResetError):
                pass

    def maintenance(self):
        while self.running:
            try:
                self.library.tick()
            except sqlite3.Error:
                self.running = False
                break
            time.sleep(1)

    def serve(self):
        maint = threading.Thread(target=self.maintenance)
        maint.start()
        try:
            while self.running:
                try:
                    conn, _ = self.socket.accept()
                except socket.timeout:
                    continue
                self.threads = [t for t in self.threads if t.is_alive()]
                if len(self.threads) >= 32:
                    conn.close()
                    continue
                thread = threading.Thread(target=self.handle, args=(conn,))
                self.threads.append(thread)
                thread.start()
        finally:
            self.running = False
            maint.join()
            for thread in self.threads:
                thread.join()
            self.close()

    def close(self):
        self.socket.close()
        self.path.unlink(missing_ok=True)
        self.library.db.close()
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()


def call(command, args=None, credential="", runtime=None):
    path = (Path(runtime) if runtime else production_runtime())/"coordinator.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(300)
    try:
        sock.connect(str(path))
        sock.sendall(json.dumps({"command": command, "args": args or {}, "credential": credential}).encode()+b"\n")
        with sock.makefile("rb") as stream:
            data = stream.readline(2*1024*1024)
        if not data:
            raise Fault("coordinator_disconnected_result_unknown_retry_same_key", 7)
        return json.loads(data)
    except (OSError, ValueError) as exc:
        raise Fault("coordinator_unavailable_or_result_unknown", 6) from exc
    finally:
        sock.close()


def restore(data_dir, backup, *, runtime=None):
    """Offline only; fixed runtime lock prevents concurrent live restore."""
    root = Path(runtime) if runtime else production_runtime()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    validate_private(root)
    with open(root/"coordinator.lock", "a+") as lock:
        os.chmod(root/"coordinator.lock", 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise Fault("restore_requires_stopped_coordinator", 6)
        manifest = root/"library.json"
        if not manifest.exists() or json.loads(manifest.read_text())["data_dir"] != str(Path(data_dir).resolve()):
            raise Fault("restore_data_directory_mismatch", 6)
        source = sqlite3.connect(f"file:{Path(backup).resolve()}?mode=ro", uri=True)
        try:
            if source.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise Fault("backup_integrity_failed", 6)
            if source.execute("SELECT value FROM meta WHERE key='schema'").fetchone()[0] != "1":
                raise Fault("unsupported_schema", 6)
            if source.execute("SELECT value FROM meta WHERE key='library_id'").fetchone()[0] != json.loads(manifest.read_text())["library_id"]:
                raise Fault("restore_library_mismatch", 6)
            target = Path(data_dir)/"ledger.sqlite3"
            if target.exists():
                preserved = target.with_name("ledger.before-restore."+str(time.time_ns())+".sqlite3")
                # Service is stopped under the singleton lock. Preserve even a
                # corrupt ledger verbatim before replacing it with verified backup.
                shutil.copy2(target, preserved)
                os.chmod(preserved, 0o600)
            temporary = target.with_name("restore."+secrets.token_hex(8)+".sqlite3")
            secure_write(temporary, "")
            destination = sqlite3.connect(temporary)
            try:
                source.backup(destination)
                destination.execute("UPDATE devices SET state='quarantined',fence=fence+1 WHERE state!='retired'")
                destination.execute("UPDATE leases SET state='revoked',reason='backup_restore' WHERE state!='closed'")
                destination.execute("UPDATE operations SET state='unknown' WHERE state IN ('prepared','running','waiting')")
                # Old queued monotonic deadlines and intents cannot be resumed across backup restoration.
                destination.execute("UPDATE requests SET state='cancelled' WHERE state='queued'")
                destination.commit()
            finally:
                destination.close()
            marker = root/"restore-in-progress"
            if not marker.exists():
                secure_write(marker, "offline restore must finish before serve\n")
            # Preserve old rollback/WAL state together with the raw old database.
            # Never let stale sidecars replay into the restored database.
            stamp = str(time.time_ns())
            for suffix in ("-journal", "-wal", "-shm"):
                sidecar = Path(str(target)+suffix)
                if sidecar.exists():
                    preserved_sidecar = target.with_name("ledger.before-restore-sidecar."+stamp+suffix)
                    os.replace(sidecar, preserved_sidecar)
                    os.chmod(preserved_sidecar, 0o600)
            with temporary.open("rb") as synced:
                os.fsync(synced.fileno())
            os.replace(temporary, target)
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            marker.unlink()
        finally:
            source.close()
    return {"state": "restored_quarantined", "next": "serve then operator reconcile each device with explicit stopped/old-operation attestations"}
