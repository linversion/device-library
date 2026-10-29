"""Durable arbitration. All external actions happen between committed phases."""
import contextlib
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import time
import uuid
import threading
import functools
from datetime import datetime, timezone
from pathlib import Path

from .adapter import ObservationError, VerificationError
from . import process

SCHEMA = 1
POLICY = "cooperative-v1"
TTL = 1800
MAX_LEASE = 7200


class Fault(Exception):
    def __init__(self, reason, code=2, *, details=None):
        self.reason, self.code = reason, code
        self.details = details
        super().__init__(reason)


def ident():
    return uuid.uuid4().hex


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def secure_write(path, value):
    path = Path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as out:
        out.write(value)
        out.flush()
        os.fsync(out.fileno())


class Library:
    def __init__(self, directory, adapter, *, initialize=False, clock=time.monotonic, wall=time.time):
        self.root = Path(directory)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        self.adapter, self.clock, self.wall = adapter, clock, wall
        self._lock = threading.RLock()
        self._local = threading.local()
        self._revision = 0
        self._inventory_signature = None
        self._last_instances = []
        self._observation_unknown = False
        path = self.root / "ledger.sqlite3"
        if not path.exists() and not initialize:
            raise Fault("ledger_missing: explicit init or offline restore required", 6)
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        os.chmod(path, 0o600)
        existing_schema = self.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
        if existing_schema:
            version = self.db.execute("SELECT value FROM meta WHERE key='schema'").fetchone()
            if not version or version[0] != str(SCHEMA):
                self.db.close()
                raise Fault("unsupported_schema", 6)
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS devices(id TEXT PRIMARY KEY,kind TEXT NOT NULL,alias TEXT NOT NULL,
          spec TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'offline',fence INTEGER NOT NULL DEFAULT 0,
          observation TEXT NOT NULL DEFAULT '{}',observed REAL,version INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS bindings(id INTEGER PRIMARY KEY,device TEXT,observation TEXT,epoch TEXT,at REAL);
        CREATE TABLE IF NOT EXISTS requests(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE,
          borrower TEXT,session TEXT,request_key TEXT,body TEXT,state TEXT,deadline REAL,created REAL,
          UNIQUE(borrower,session,request_key));
        CREATE TABLE IF NOT EXISTS leases(id TEXT PRIMARY KEY,request TEXT UNIQUE REFERENCES requests(id),
          device TEXT REFERENCES devices(id),borrower TEXT,session TEXT,token_hash TEXT,fence INTEGER,
          epoch TEXT,state TEXT,start REAL,heartbeat REAL,deadline REAL,maximum REAL,reason TEXT,closed_at REAL);
        CREATE UNIQUE INDEX IF NOT EXISTS occupied_device ON leases(device) WHERE state != 'closed';
        CREATE TABLE IF NOT EXISTS claims(key TEXT PRIMARY KEY,device TEXT,state TEXT,operation TEXT);
        CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY,device TEXT,lease TEXT,kind TEXT,
          state TEXT,epoch TEXT,fence INTEGER,observation TEXT,result TEXT,created REAL);
        CREATE TABLE IF NOT EXISTS handoffs(id TEXT PRIMARY KEY,lease TEXT,operation TEXT,fence INTEGER,
          epoch TEXT,state TEXT,ack TEXT,source TEXT,created REAL);
        CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,at REAL,epoch TEXT,
          type TEXT,device TEXT,lease TEXT,request TEXT,operation TEXT,payload TEXT);
        ''')
        meta = dict(self.db.execute("SELECT key,value FROM meta"))
        if meta and int(meta.get("schema", "0")) != SCHEMA:
            raise Fault("unsupported_schema", 6)
        if not meta:
            if not initialize:
                raise Fault("ledger_uninitialized", 6)
            with self.tx():
                for k, v in {"schema": str(SCHEMA), "library_id": ident(), "policy": POLICY}.items():
                    self.db.execute("INSERT INTO meta VALUES(?,?)", (k, v))
        self.epoch = ident()
        self.library_id = self.db.execute("SELECT value FROM meta WHERE key='library_id'").fetchone()[0]
        self.complete = False
        self._tick_running = False
        self.last_observation = None
        self.last_observation_error = None
        self.previous_clock, self.previous_wall = self.clock(), self.wall()
        # Every restart fences all outstanding occupancy. No old external action is replayed.
        with self.tx():
            self.db.execute("INSERT OR REPLACE INTO meta VALUES('epoch',?)", (self.epoch,))
            self.db.execute("UPDATE devices SET fence=fence+1,state='quarantined' WHERE id IN (SELECT device FROM leases WHERE state != 'closed')")
            self.db.execute("UPDATE leases SET state='revoked',reason='coordinator_restart' WHERE state != 'closed'")
            self.db.execute("UPDATE operations SET state='unknown' WHERE state IN ('prepared','running','waiting')")
            self.db.execute("UPDATE requests SET state='cancelled' WHERE state='queued'")
            self.event("coordinator_started", payload={"policy": POLICY, "old_operations_require_attestation": True})
        self.reconcile()

    @contextlib.contextmanager
    def locked(self):
        self._lock.acquire()
        self._local.depth = getattr(self._local, "depth", 0) + 1
        try:
            yield
        finally:
            self._local.depth -= 1
            self._lock.release()

    def external(self, function, *args):
        # Release the state mutex as well as the DB transaction before slow work.
        # The reservation/fence must be revalidated by the caller on return.
        depth = getattr(self._local, "depth", 0)
        for _ in range(depth):
            self._lock.release()
        self._local.depth = 0
        try:
            return function(*args)
        finally:
            for _ in range(depth):
                self._lock.acquire()
            self._local.depth = depth

    @contextlib.contextmanager
    def tx(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
            self._revision += 1
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def event(self, kind, *, device=None, lease=None, request=None, operation=None, payload=None):
        self.db.execute("INSERT INTO events(at,epoch,type,device,lease,request,operation,payload) VALUES(?,?,?,?,?,?,?,?)",
                        (self.wall(), self.epoch, kind, device, lease, request, operation, json.dumps(payload or {})))

    @staticmethod
    def unpack(row):
        if row is None:
            return None
        d = dict(row)
        for key in ("spec", "observation", "body", "result", "payload"):
            if key in d and d[key] is not None:
                d[key] = json.loads(d[key])
        d.pop("token_hash", None)
        return d

    def devices(self):
        return [{**self.unpack(r), "attention": self._attention(r["id"])}
                for r in self.db.execute("SELECT * FROM devices ORDER BY alias,id")]

    def _attention(self, device):
        l = self.db.execute("SELECT l.* FROM leases l JOIN requests r ON r.id=l.request WHERE l.device=? AND l.state!='closed' AND r.state='expired'", (device,)).fetchone()
        if not l:
            return None
        op = self.db.execute("SELECT * FROM operations WHERE lease=? AND kind='auto-reclaim' ORDER BY created DESC LIMIT 1", (l["id"],)).fetchone()
        r = self.db.execute("SELECT body FROM requests WHERE id=?", (l["request"],)).fetchone()
        return {"state": "reclaim_pending" if op and op["state"] in ("running", "waiting") else "manual_intervention_required",
                "lease": l["id"], "request": l["request"], "borrower": l["borrower"],
                "session": l["session"], "task": json.loads(r[0]).get("task"),
                "reason": "lease_expired", "occupancy_retained": True,
                "message": "Lease expired; stop direct actions. Device remains quarantined until verified safe.",
                "reclaim": self.unpack(op) if op else None}

    def register(self, kind, alias, spec):
        if not isinstance(spec, dict):
            raise Fault("spec_must_be_object")
        if kind not in ("physical", "emulator") or not isinstance(alias, str) or not alias.strip():
            raise Fault("invalid_registration")
        required = ("hardware_serial", "model", "serials") if kind == "physical" else ("avd_name", "avd_path", "avd_identity", "sdk_root")
        if any(not spec.get(k) for k in required):
            raise Fault("incomplete_identity")
        string_fields = ("hardware_serial", "model") if kind == "physical" else required
        if any(not isinstance(spec[k], str) or not spec[k].strip() for k in string_fields):
            raise Fault("identity_fields_must_be_nonempty_strings")
        if kind == "physical" and (not isinstance(spec["serials"], list) or not spec["serials"] or any(not isinstance(serial, str) or not serial.strip() or serial.startswith("-") for serial in spec["serials"])):
            raise Fault("serials_must_be_nonempty_string_array")
        capabilities = spec.get("capabilities", {})
        if not self._tags_valid(capabilities):
            raise Fault("capabilities_must_be_scalar_object")
        if "lock_policy" in spec and spec["lock_policy"] != "no-credential-wake":
            raise Fault("unsupported_lock_policy")
        if kind == "emulator" and str(spec["avd_name"]).startswith("-"):
            raise Fault("invalid_avd_name")
        try:
            self.external(self.adapter.validate_registration, kind, spec)
        except (ObservationError, OSError, ValueError) as exc:
            raise Fault("invalid_registration: " + str(exc), 5) from exc
        for d in self.devices():
            if d["state"] == "retired":
                continue
            s = d["spec"]
            if kind == d["kind"] and (s.get("hardware_serial") == spec.get("hardware_serial") if kind == "physical" else s.get("avd_path") == spec.get("avd_path")):
                raise Fault("identity_already_registered")
            if kind == d["kind"] == "physical" and set(s["serials"]) & set(spec["serials"]):
                raise Fault("transport_alias_already_registered")
        device = ident()
        with self.tx():
            self.db.execute("INSERT INTO devices(id,kind,alias,spec) VALUES(?,?,?,?)", (device, kind, alias, json.dumps(spec)))
            self.event("device_registered", device=device, payload={"kind": kind, "alias": alias, "baseline": "retained; not certified clean"})
        self.reconcile()
        d = self.device(device)
        if d["kind"] == "emulator" and d["observation"].get("online"):
            with self.tx():
                self.db.execute("UPDATE devices SET state='quarantined' WHERE id=?", (device,))
                self.event("foreign_handoff_required", device=device)
        return self.device(device)

    def retire(self, device, reason, stopped=False):
        """Retire registration metadata only; never stop/delete the actual device."""
        d = self.device(device)
        if not isinstance(reason, str) or not reason.strip():
            raise Fault("retirement_reason_required")
        if stopped is not True:
            raise Fault("retirement_requires_old_client_stopped", 5)
        if self.db.execute("SELECT 1 FROM leases WHERE device=? AND state!='closed'", (device,)).fetchone():
            raise Fault("retirement_refused_occupied", 5)
        if self.db.execute("SELECT 1 FROM operations WHERE device=? AND state IN ('prepared','running','unknown','waiting')", (device,)).fetchone():
            raise Fault("retirement_refused_unresolved_operation", 5)
        if d["state"] == "retired":
            return d
        with self.tx():
            self.db.execute("UPDATE devices SET state='retired',fence=fence+1,version=version+1 WHERE id=?", (device,))
            # Runtime instances remain globally accounted; retirement does not
            # imply that an emulator process ended or transfer ownership.
            self.db.execute("UPDATE claims SET device=NULL,state=CASE WHEN state='live' THEN 'foreign' ELSE state END WHERE device=?", (device,))
            self.event("device_retired", device=device, payload={"reason": reason, "old_client_stopped": True, "device_action": "none; metadata only"})
        self.complete = False
        self.last_observation_error = "registration_retired_reconciliation_required"
        return self.device(device)

    def device(self, device):
        row = self.db.execute("SELECT * FROM devices WHERE id=?", (device,)).fetchone()
        if not row:
            raise Fault("device_not_found")
        return self.unpack(row)

    def delete(self, device):
        """Delete retired metadata using existing observations only; no adapter calls."""
        with self.tx():
            d = self.device(device)
            if d["state"] != "retired":
                raise Fault("deletion_requires_retired", 5)
            if self.db.execute("SELECT 1 FROM leases WHERE device=? AND state IS NOT 'closed'", (device,)).fetchone():
                raise Fault("deletion_refused_unclosed_lease", 5)
            if self.db.execute("SELECT 1 FROM claims WHERE device=?", (device,)).fetchone():
                raise Fault("deletion_refused_claim", 5)
            if self.db.execute("""SELECT 1 FROM requests r WHERE
                    (json_extract(r.body,'$.device')=? OR r.id IN (SELECT request FROM leases WHERE device=?))
                    AND COALESCE(r.state,'') NOT IN ('closed','cancelled','timed_out','expired')""", (device, device)).fetchone():
                raise Fault("deletion_refused_pending_request", 5)
            if self.db.execute("""SELECT 1 FROM operations WHERE
                    (device=? OR lease IN (SELECT id FROM leases WHERE device=?))
                    AND COALESCE(state,'') NOT IN ('completed','cancelled')""", (device, device)).fetchone():
                raise Fault("deletion_refused_unresolved_operation", 5)
            if self.db.execute("""SELECT 1 FROM handoffs WHERE state IS NOT 'completed' AND
                    (lease IN (SELECT id FROM leases WHERE device=?) OR
                     operation IN (SELECT id FROM operations WHERE device=?))""", (device, device)).fetchone():
                raise Fault("deletion_refused_pending_handoff", 5)

            # Retirement excludes a registration from discovery. Include positive
            # evidence from replacement registrations and foreign instances too.
            related = [d]
            for other in self.devices():
                if other["id"] == device or other["kind"] != d["kind"]:
                    continue
                a, b = d["spec"], other["spec"]
                if d["kind"] == "physical":
                    same = (a["hardware_serial"] == b["hardware_serial"] or
                            bool(set(a["serials"]) & set(b["serials"])))
                else:
                    same = any(a[k] == b[k] for k in ("avd_name", "avd_path", "avd_identity"))
                if same:
                    related.append(other)
            serials = {r["observation"].get("serial") for r in related} - {None}
            if d["kind"] == "physical":
                serials.update(d["spec"]["serials"])
            if (any(r["observation"].get("online") for r in related) or
                    any(i.get("device_id") == device or i.get("serial") in serials or
                        (d["kind"] == "emulator" and i.get("avd") == d["spec"]["avd_name"])
                        for i in self._last_instances) or
                    any(c["key"] in serials for c in self.db.execute("SELECT key FROM claims"))):
                raise Fault("deletion_refused_observed_online", 5)

            # Keep closed chains and their public device ID without retaining an
            # inventory row or breaking SQLite's live-device foreign key.
            for row in self.db.execute("SELECT id FROM leases WHERE device=?", (device,)).fetchall():
                self.db.execute("INSERT INTO meta VALUES(?,?)", ("lease_device:"+row["id"], device))
            self.db.execute("UPDATE leases SET device=NULL WHERE device=?", (device,))
            self.event("device_deleted", device=device, payload={
                "registration": d, "actor": {"uid": os.getuid(), "role": "operator"},
                "device_action": "none; metadata only"})
            self.db.execute("DELETE FROM devices WHERE id=?", (device,))
        return {"state": "deleted", "id": device}

    def _topology(self):
        return tuple(tuple(row) for row in self.db.execute("SELECT id,kind,spec,state,fence,observation FROM devices ORDER BY id")), tuple(tuple(row) for row in self.db.execute("SELECT * FROM claims ORDER BY key")), tuple(tuple(row) for row in self.db.execute("SELECT id,state FROM operations WHERE state IN ('prepared','running') ORDER BY id"))

    def reconcile(self):
        """Observation is never assumed complete following a failed command."""
        self.complete = False
        try:
            revision = self._topology()
            observation = self.external(self.adapter.observe, [d for d in self.devices() if d["state"] != "retired"])
            if revision != self._topology():
                self.last_observation_error = "observation_superseded"
                return {"complete": False, "reason": "observation_superseded", "claims": self.count_claims()}
            if observation.get("complete") is not True:
                raise ObservationError("inventory incomplete")
            with self.tx():
                seen = set()
                for instance in observation["instances"]:
                    key = instance["key"]
                    if key in seen:
                        raise ObservationError("duplicate instance")
                    seen.add(key)
                    device = instance.get("device_id")
                    active_ids = {d["id"] for d in self.devices() if d["state"] != "retired"}
                    if device not in active_ids:
                        device = None
                    if not device:
                        matches = [d for d in self.devices() if d["state"] != "retired" and d["kind"] == "emulator" and
                                   ((instance.get("serial") and d["observation"].get("serial") == instance.get("serial")) or
                                    d["spec"].get("avd_name") == instance.get("avd"))]
                        if len(matches) == 1:
                            device = matches[0]["id"]
                    # Replace prepared reservation only after positively identifying same device.
                    owned = bool(device and self.db.execute("SELECT 1 FROM claims WHERE device=? AND state!='foreign'", (device,)).fetchone())
                    if device and not owned:
                        # A registered AVD started elsewhere is still foreign until
                        # its previous user explicitly transfers the running instance.
                        self.db.execute("UPDATE devices SET state='quarantined' WHERE id=? AND state NOT IN ('loaned','preparing','returning')", (device,))
                    if device and owned:
                        self.db.execute("DELETE FROM claims WHERE device=? AND key!=?", (device, key))
                    self.db.execute("INSERT OR REPLACE INTO claims VALUES(?,?,?,NULL)", (key, device, "live" if owned else "foreign"))
                # Live/foreign observations may disappear; uncertain reservations never do automatically.
                for c in self.db.execute("SELECT * FROM claims").fetchall():
                    if c["key"] not in seen and c["state"] in ("live", "foreign"):
                        reclaim = self.db.execute("SELECT 1 FROM operations WHERE device=? AND kind='auto-reclaim' AND state IN ('running','waiting','unknown')", (c["device"],)).fetchone()
                        if reclaim:
                            self.db.execute("UPDATE claims SET state='uncertain' WHERE key=?", (c["key"],))
                        else:
                            self.db.execute("DELETE FROM claims WHERE key=?", (c["key"],))
                for d in self.devices():
                    if d["state"] == "retired":
                        continue
                    obs = observation["devices"].get(d["id"])
                    if obs is None:
                        raise ObservationError("registered identity missing from observation")
                    old = d["observation"]
                    occupied = self.db.execute("SELECT id FROM leases WHERE device=? AND state!='closed'", (d["id"],)).fetchone()
                    changed = bool(old.get("serial")) and (old.get("serial"), old.get("transport"), old.get("identity")) != (obs.get("serial"), obs.get("transport"), obs.get("identity"))
                    state = d["state"]
                    reclaiming = self.db.execute("SELECT 1 FROM operations WHERE device=? AND kind='auto-reclaim' AND state IN ('running','waiting') AND epoch=? AND fence=?", (d["id"], self.epoch, d["fence"])).fetchone()
                    if occupied and changed and d["state"] != "preparing" and not ((d["state"] == "returning" or reclaiming) and obs.get("online") is False):
                        state = "quarantined"
                        self.db.execute("UPDATE leases SET state='revoked',reason='connection_changed' WHERE device=? AND state!='closed'", (d["id"],))
                        self.db.execute("UPDATE devices SET fence=fence+1 WHERE id=?", (d["id"],))
                        self.event("connection_changed", device=d["id"], lease=occupied[0])
                    elif not occupied and state not in ("quarantined", "retired", "preparing", "returning"):
                        state = "available" if obs.get("online") or d["kind"] == "emulator" else "offline"
                    self.db.execute("UPDATE devices SET state=?,observation=?,observed=?,version=version+1 WHERE id=?", (state, json.dumps(obs), self.wall(), d["id"]))
                    if old != obs:
                        self.db.execute("INSERT INTO bindings(device,observation,epoch,at) VALUES(?,?,?,?)", (d["id"], json.dumps(obs), self.epoch, self.wall()))
                        self.event("identity_observed", device=d["id"], payload=obs)
                signature = json.dumps(observation, sort_keys=True) + str(self.count_claims())
                if signature != self._inventory_signature or self._observation_unknown:
                    self.event("inventory_reconciled", payload={"instances": len(seen), "claims": self.count_claims()})
                self._inventory_signature = signature
                self._observation_unknown = False
            self._last_instances = observation["instances"]
            self.complete = True
            self.last_observation = self.wall()
            self.last_observation_error = None
        except (ObservationError, OSError, ValueError, KeyError) as exc:
            self.last_observation_error = str(exc) if isinstance(exc, ObservationError) else type(exc).__name__
            if not self._observation_unknown:
                with self.tx():
                    self.event("observation_unknown", payload={"reason": self.last_observation_error})
            self._observation_unknown = True
            return {"complete": False, "reason": "observation_unknown", "claims": self.count_claims()}
        return {"complete": True, "claims": self.count_claims(), "over_capacity": self.count_claims() > 3}

    def count_claims(self):
        return self.db.execute("SELECT count(*) FROM claims").fetchone()[0]

    @staticmethod
    def _tags_valid(tags):
        return isinstance(tags, dict) and all(isinstance(k, str) and bool(k.strip()) and isinstance(v, (str, int, float, bool)) and (not isinstance(v, float) or math.isfinite(v)) for k,v in tags.items())

    def borrow(self, borrower, session, key, body, token, owner_pid=None):
        if not all(isinstance(x, str) and x for x in (borrower, session, key, token)) or len(token) < 32:
            raise Fault("identity_and_credential_required")
        if not isinstance(body, dict):
            raise Fault("body_must_be_object")
        if body.get("kind") not in ("physical", "emulator", "any"):
            raise Fault("explicit_kind_required")
        if not self._tags_valid(body.get("requires", {})):
            raise Fault("requires_must_be_scalar_object")
        if set(body) - {"kind", "requires", "device", "task", "queue_timeout"}:
            raise Fault("unsupported_request_fields")
        if body.get("device") is not None and (not isinstance(body["device"], str) or not body["device"].strip()):
            raise Fault("device_must_be_nonempty_string")
        if "task" in body and not isinstance(body["task"], str):
            raise Fault("task_must_be_string")
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
        existing = self.db.execute("SELECT * FROM requests WHERE borrower=? AND session=? AND request_key=?", (borrower, session, key)).fetchone()
        if existing:
            if existing["body"] != canonical:
                raise Fault("idempotency_conflict")
            lease = self.db.execute("SELECT token_hash FROM leases WHERE request=?", (existing["id"],)).fetchone()
            # Request credentials are persisted before allocation, including queued requests.
            stored = self.db.execute("SELECT value FROM meta WHERE key=?", ("request_auth:" + existing["id"],)).fetchone()[0]
            if not hmac.compare_digest(stored, digest(token)):
                raise Fault("unauthorized", 4)
            return self.request(existing["id"])
        owner_pid = os.getpid() if owner_pid is None else owner_pid
        try:
            birth = process.identity(owner_pid)
        except process.ProcessUnknown as exc:
            raise Fault("borrower_process_unverifiable", 5) from exc
        if birth is None:
            raise Fault("borrower_process_exited", 5)
        owner = {"pid": owner_pid, "identity": birth}
        req = ident()
        try:
            raw_wait = body.get("queue_timeout", 3600)
            wait = float(raw_wait)
        except (TypeError, ValueError):
            raise Fault("invalid_queue_timeout")
        if isinstance(raw_wait, bool) or not math.isfinite(wait) or wait <= 0 or wait > 86400:
            raise Fault("invalid_queue_timeout")
        with self.tx():
            self.db.execute("INSERT INTO requests(id,borrower,session,request_key,body,state,deadline,created) VALUES(?,?,?,?,?,'queued',?,?)", (req, borrower, session, key, canonical, self.clock()+wait, self.wall()))
            self.db.execute("INSERT INTO meta VALUES(?,?)", ("request_auth:"+req, digest(token)))
            self.db.execute("INSERT INTO meta VALUES(?,?)", ("request_owner:"+req, json.dumps(owner)))
            self.event("request_queued", request=req, payload={"borrower": borrower, "session": session, "task": body.get("task"), "kind": body["kind"]})
        return self.request(req)

    def request(self, req):
        row = self.db.execute("SELECT * FROM requests WHERE id=?", (req,)).fetchone()
        if not row:
            raise Fault("request_not_found")
        out = self.unpack(row)
        lease = self.db.execute("SELECT * FROM leases WHERE request=?", (req,)).fetchone()
        out["lease"] = self.lease(lease["id"]) if lease else None
        if lease:
            out["selector"] = out["lease"]["selector"]
        return out

    def auth_request(self, req, token):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", ("request_auth:"+req,)).fetchone()
        if not row or not hmac.compare_digest(row[0], digest(token)):
            raise Fault("unauthorized", 4)

    def auth_lease(self, lease, token, *, current=True):
        row = self.db.execute("SELECT * FROM leases WHERE id=?", (lease,)).fetchone()
        if not row or not hmac.compare_digest(row["token_hash"], digest(token)):
            raise Fault("unauthorized", 4)
        if current and (row["state"] != "active" or row["epoch"] != self.epoch or row["deadline"] <= self.clock() or self.device(row["device"])["fence"] != row["fence"]):
            raise Fault("lease_expired_or_fenced", 4, details={
                "message": "Lease expired or fenced; stop all direct actions and streams. Check lease status and contact the operator if attention is required.",
                "attention": self._attention(row["device"])})
        return row

    def matches(self, d, body):
        return (body["kind"] in ("any", d["kind"]) and
                (not body.get("device") or body["device"] == d["id"]) and
                all(str(d["spec"].get("capabilities", {}).get(k)) == str(v) for k, v in body.get("requires", {}).items()))

    def tick(self):
        if self._tick_running:
            return
        self._tick_running = True
        try:
            return self._tick()
        finally:
            self._tick_running = False

    def _check_owners(self):
        rows = self.db.execute("""SELECT r.id,l.id AS lease,l.state FROM requests r
            LEFT JOIN leases l ON l.request=r.id
            WHERE r.state IN ('queued','active','preparing')""").fetchall()
        for row in rows:
            saved = self.db.execute("SELECT value FROM meta WHERE key=?", ("request_owner:"+row['id'],)).fetchone()
            if not saved:
                continue  # Pre-upgrade requests have no provable owner.
            status = process.state(json.loads(saved[0]))
            if status == 'alive':
                continue
            reason = 'borrower_process_' + status
            if not row['lease']:
                with self.tx():
                    self.db.execute("UPDATE requests SET state='cancelled' WHERE id=?", (row['id'],))
                    self.event(reason, request=row['id'])
                continue
            l = self.db.execute("SELECT * FROM leases WHERE id=?", (row['lease'],)).fetchone()
            if l['state'] not in ('active', 'returning'):
                continue
            if l['state'] == 'returning':
                acknowledged = self.db.execute("SELECT 1 FROM handoffs WHERE lease=? AND ack='stopped' AND state!='completed'", (l['id'],)).fetchone()
                if acknowledged:
                    continue  # Existing stopped acknowledgement already owns cleanup.
            d = self.device(l['device'])
            if status == 'unknown' or d['kind'] == 'physical':
                with self.tx():
                    self._quarantine(l, reason)
                continue
            h = self._begin_return(l, reason)
            # Exit is not an attestation about surviving child processes. Always
            # stop the emulator and prove it offline before reusing its capacity.
            with self.tx():
                self.db.execute("UPDATE handoffs SET state='acknowledged',ack='stopped',source=? WHERE id=?",
                                (reason, h['id']))
                self.db.execute("UPDATE operations SET state='waiting' WHERE id=?", (h['operation'],))
                self.event(reason, device=d['id'], lease=l['id'])

    def _tick(self):
        self._check_owners()
        now, wall = self.clock(), self.wall()
        if abs((wall-self.previous_wall)-(now-self.previous_clock)) > 5:
            with self.tx():
                for l in self.db.execute("SELECT * FROM leases WHERE state='active'").fetchall():
                    self._quarantine(l, "clock_or_sleep_uncertain")
                self.event("clock_discontinuity")
        self.previous_clock, self.previous_wall = now, wall
        expired = []
        with self.tx():
            # An abandoned return/cancel handoff is still occupancy, but may be
            # reclaimed at its original deadline if no return action has begun.
            for l in self.db.execute("""SELECT * FROM leases WHERE epoch=? AND deadline<=? AND
                    (state='active' OR (state='returning' AND id IN
                    (SELECT h.lease FROM handoffs h JOIN operations o ON o.id=h.operation
                     WHERE h.state='awaiting_stop' AND o.kind='return' AND o.state='prepared')))""", (self.epoch, now)).fetchall():
                self._quarantine(l, "lease_expired", "expired")
                self.db.execute("UPDATE requests SET state='expired' WHERE id=?", (l["request"],))
                self.db.execute("UPDATE operations SET state='cancelled' WHERE lease=? AND state='prepared'", (l["id"],))
                self.db.execute("UPDATE handoffs SET state='completed' WHERE lease=? AND operation IN (SELECT id FROM operations WHERE state='cancelled')", (l["id"],))
                payload = self._expiry_context(l)
                self.event("lease_expiry_notice", device=l["device"], lease=l["id"], request=l["request"],
                           payload={**payload, "recipients": ["borrower", "operator"],
                                    "message": "Lease expired; stop all direct actions and streams.",
                                    "action": "auto_reclaim" if self.device(l["device"])["kind"] == "emulator" else "manual_intervention_required"})
                expired.append(l["id"])
            for r in self.db.execute("SELECT * FROM requests WHERE state='queued' AND deadline<=?", (now,)).fetchall():
                self.db.execute("UPDATE requests SET state='timed_out' WHERE id=?", (r["id"],))
                self.event("request_timed_out", request=r["id"])
        # Refresh before every dispatch; uncertainty globally blocks new starts/loans.
        for lid in expired:
            if self.device(self.lease(lid)["device"])["kind"] == "emulator":
                self._reclaim_expired(lid)
        self.reconcile()
        for op in self.db.execute("SELECT * FROM operations WHERE kind='auto-reclaim' AND state='waiting' AND epoch=?", (self.epoch,)).fetchall():
            self._finish_reclaim(op)
        if not self.complete:
            return
        # Resume acknowledged returns after a known stop or another operation
        # ends. Unknown operations still block; lifecycle calls are never replayed.
        pending = self.db.execute("""SELECT h.* FROM handoffs h JOIN operations o ON o.id=h.operation
            WHERE h.state IN ('acknowledged','awaiting_stop') AND h.ack='stopped' AND o.kind='return'
            AND o.state='waiting' AND o.epoch=?""", (self.epoch,)).fetchall()
        for h in pending:
            op = self.db.execute("SELECT * FROM operations WHERE id=?", (h["operation"],)).fetchone()
            l = self.db.execute("SELECT * FROM leases WHERE id=?", (h["lease"],)).fetchone()
            if h["fence"] != self.device(l["device"])["fence"] or l["state"] != "returning":
                continue
            with self.tx():
                self.db.execute("UPDATE handoffs SET state='acknowledged' WHERE id=?", (h["id"],))
                self.db.execute("UPDATE operations SET state='running' WHERE id=?", (op["id"],))
            self._finish_return(l, h, op)
        if not self.complete:
            return
        for r in self.db.execute("SELECT * FROM requests WHERE state='queued' ORDER BY seq").fetchall():
            body = json.loads(r["body"])
            for d in self.devices():
                if d["state"] != "available" or not self.matches(d, body):
                    continue
                if d["kind"] == "emulator" and not d["observation"].get("online") and self.count_claims() >= 3:
                    continue
                self._allocate(r, d)
                break

    def _quarantine(self, l, reason, state="revoked"):
        self.db.execute("UPDATE leases SET state=?,reason=? WHERE id=?", (state, reason, l["id"]))
        self.db.execute("UPDATE devices SET state='quarantined',fence=fence+1 WHERE id=?", (l["device"],))
        self.event(reason, lease=l["id"], device=l["device"], payload={"stop_required": True})

    def _expiry_context(self, l):
        r = self.db.execute("SELECT body FROM requests WHERE id=?", (l["request"],)).fetchone()
        return {"borrower": l["borrower"], "session": l["session"],
                "task": json.loads(r[0]).get("task"), "reason": "lease_expired",
                "previous_reason": l["reason"]}

    def _reclaim_current(self, op):
        l = self.db.execute("SELECT * FROM leases WHERE id=?", (op["lease"],)).fetchone()
        d = self.device(op["device"])
        current = self.db.execute("SELECT * FROM operations WHERE id=?", (op["id"],)).fetchone()
        if (not l or l["state"] != "expired" or l["reason"] != "lease_expired" or
                l["epoch"] != self.epoch or l["deadline"] > self.clock() or
                d["state"] != "quarantined" or d["fence"] != op["fence"] or
                op["epoch"] != self.epoch or current["state"] not in ("running", "waiting")):
            raise ObservationError("auto reclaim ownership changed")
        if self.db.execute("SELECT 1 FROM operations WHERE device=? AND id!=? AND state IN ('prepared','running','waiting','unknown')", (d["id"], op["id"])).fetchone():
            raise ObservationError("old coordinator operations need attestation")
        return l, d

    def _reclaim_failed(self, op, reason):
        # Never retry an unknown lifecycle action; retain its reservation even
        # when a subsequent observation happens to find no process.
        with self.tx():
            self.db.execute("UPDATE operations SET state='unknown',result=? WHERE id=?", (json.dumps({"reason": reason, "occupancy_retained": True}), op["id"]))
            self.db.execute("UPDATE claims SET state='uncertain' WHERE device=?", (op["device"],))
            l = self.db.execute("SELECT * FROM leases WHERE id=?", (op["lease"],)).fetchone()
            self.event("auto_reclaim_failed", device=op["device"], lease=op["lease"], operation=op["id"],
                       payload={**self._expiry_context(l), "failure_reason": reason, "action": "manual_intervention_required", "occupancy_retained": True})

    def _reclaim_expired(self, lid):
        l = self.db.execute("SELECT * FROM leases WHERE id=?", (lid,)).fetchone()
        d = self.device(l["device"])
        # Re-read after external observation: a renewal/return is never a hint
        # authorizing a stop. Only this epoch's durably expired lease qualifies.
        if l["state"] != "expired" or l["reason"] != "lease_expired" or l["epoch"] != self.epoch or d["kind"] != "emulator":
            return
        with self.tx():
            opid = self._operation(d, l, "auto-reclaim")
            self.db.execute("UPDATE operations SET state='running' WHERE id=?", (opid,))
            if not self.db.execute("SELECT 1 FROM claims WHERE device=?", (d["id"],)).fetchone():
                self.db.execute("INSERT INTO claims VALUES(?,?,'uncertain',?)", ("reclaim:"+opid, d["id"], opid))
        op = self.db.execute("SELECT * FROM operations WHERE id=?", (opid,)).fetchone()
        try:
            self.reconcile()
            l, d = self._reclaim_current(op)
            if not self.complete:
                raise ObservationError("inventory unknown before stop: " + str(self.last_observation_error))
            if d["observation"].get("online") is True:
                self.external(self.adapter.stop, d, d["observation"])
                self._reclaim_current(op)
            elif d["observation"].get("online") is not False:
                raise ObservationError("emulator offline state unproven")
            with self.tx():
                self.db.execute("UPDATE operations SET state='waiting',result=? WHERE id=?", ('{"stop_requested":true}', opid))
                self.event("auto_reclaim_stop_requested", device=d["id"], lease=lid, request=l["request"], operation=opid, payload=self._expiry_context(l))
            self.reconcile()
            self._finish_reclaim(op)
        except (ObservationError, OSError, ValueError, KeyError) as exc:
            self._reclaim_failed(op, str(exc))

    def _finish_reclaim(self, op):
        try:
            l, d = self._reclaim_current(op)
            if not self.complete or d["observation"].get("online") is not False:
                reason = self.last_observation_error or "emulator process exit pending"
                with self.tx():
                    result = {"stop_requested": True, "reason": reason, "occupancy_retained": True}
                    old = self.db.execute("SELECT result FROM operations WHERE id=?", (op["id"],)).fetchone()[0]
                    self.db.execute("UPDATE operations SET state='waiting',result=? WHERE id=?", (json.dumps(result), op["id"]))
                    if old != json.dumps(result):
                        self.event("auto_reclaim_pending", device=d["id"], lease=l["id"], operation=op["id"], payload=result)
                return
            # A complete adapter observation proves process/transport agreement,
            # not merely adb offline. Any remaining instance claim blocks closure.
            if self.db.execute("SELECT 1 FROM claims WHERE device=? AND state IN ('live','foreign')", (d["id"],)).fetchone():
                raise ObservationError("residual emulator instance")
            with self.tx():
                self.db.execute("UPDATE leases SET state='closed',closed_at=? WHERE id=?", (self.wall(), l["id"]))
                self.db.execute("UPDATE requests SET state='expired' WHERE id=?", (l["request"],))
                self.db.execute("UPDATE devices SET state='available',fence=fence+1 WHERE id=?", (d["id"],))
                self.db.execute("DELETE FROM claims WHERE device=?", (d["id"],))
                self.db.execute("UPDATE operations SET state='completed',result=? WHERE id=?", ('{"offline":true,"process_exit_verified":true}', op["id"]))
                self.event("auto_reclaim_completed", device=d["id"], lease=l["id"], request=l["request"], operation=op["id"],
                           payload={**self._expiry_context(l), "offline": True, "process_exit_verified": True, "occupancy_released": True})
        except (ObservationError, OSError, ValueError) as exc:
            self._reclaim_failed(op, str(exc))

    def _operation(self, d, l, kind):
        op = ident()
        self.db.execute("INSERT INTO operations VALUES(?,?,?,?,?,?,?,?,?,?)", (op, d["id"], l["id"] if l else None, kind, "prepared", self.epoch, d["fence"], json.dumps(d["observation"]), None, self.wall()))
        self.event("operation_prepared", device=d["id"], lease=l["id"] if l else None, operation=op, payload={"kind": kind})
        return op

    def _allocate(self, r, d):
        # Snapshot rows from FIFO scan are only hints; authority is checked under
        # the state lock immediately before the reservation transaction.
        current_request = self.db.execute("SELECT * FROM requests WHERE id=?", (r["id"],)).fetchone()
        d = self.device(d["id"])
        if not current_request or current_request["state"] != "queued" or current_request["deadline"] <= self.clock() or d["state"] != "available":
            return
        if self.db.execute("SELECT 1 FROM leases WHERE device=? AND state!='closed'", (d["id"],)).fetchone():
            return
        if d["kind"] == "emulator" and not d["observation"].get("online") and (not self.complete or self.count_claims() >= 3):
            return
        r = current_request
        lid = ident()
        needs_start = d["kind"] == "emulator" and not d["observation"].get("online")
        with self.tx():
            self.db.execute("UPDATE devices SET state='preparing',fence=fence+1 WHERE id=?", (d["id"],))
            d = self.device(d["id"])
            token_hash = self.db.execute("SELECT value FROM meta WHERE key=?", ("request_auth:"+r["id"],)).fetchone()[0]
            now = self.clock()
            self.db.execute("INSERT INTO leases VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (lid, r["id"], d["id"], r["borrower"], r["session"], token_hash, d["fence"], self.epoch, "reserved", now, now, now+TTL, now+MAX_LEASE, None, None))
            self.db.execute("UPDATE requests SET state='preparing' WHERE id=?", (r["id"],))
            op = self._operation(d, {"id": lid}, "prepare")
            self.db.execute("UPDATE operations SET state='running' WHERE id=?", (op,))
            if needs_start:
                self.db.execute("INSERT INTO claims VALUES(?,?,?,?)", ("reservation:"+op, d["id"], "reserved", op))
        try:
            if needs_start:
                self.external(self.adapter.start, d)
            self.reconcile()
            d = self.device(d["id"])
            if not self.complete:
                raise ObservationError("preparation inventory unknown")
            prepared = self.external(self.adapter.prepare, d, d["observation"])
            if not prepared.get("healthy"):
                raise ObservationError("preparation health unknown")
            with self.tx():
                # Slow preparation may race cancellation; revalidate authoritative rows.
                current = self.db.execute("SELECT * FROM leases WHERE id=?", (lid,)).fetchone()
                if current["state"] != "reserved" or current["fence"] != self.device(d["id"])["fence"] or self.device(d["id"])["state"] != "preparing" or self.db.execute("SELECT state FROM requests WHERE id=?", (r["id"],)).fetchone()[0] != "preparing":
                    # Preparation returned successfully, so no coordinator work
                    # remains even if cancellation prevented granting the lease.
                    self.db.execute("UPDATE operations SET state='completed',result=? WHERE id=?",
                                    (json.dumps({**prepared, "grant_suppressed": True}), op))
                    self.event("preparation_grant_suppressed", device=d["id"], lease=lid, operation=op)
                    return
                self.db.execute("UPDATE leases SET state='active',start=?,heartbeat=?,deadline=?,maximum=? WHERE id=?", (self.clock(), self.clock(), self.clock()+TTL, self.clock()+MAX_LEASE, lid))
                self.db.execute("UPDATE requests SET state='active' WHERE id=?", (r["id"],))
                self.db.execute("UPDATE devices SET state='loaned' WHERE id=?", (d["id"],))
                self.db.execute("UPDATE operations SET state='completed',result=? WHERE id=?", (json.dumps(prepared), op))
                self.event("lease_active", device=d["id"], lease=lid, request=r["id"], operation=op, payload={"selector": d["observation"], "preparation": prepared, "baseline": "retained; not clean-certified"})
        except (ObservationError, OSError, ValueError) as exc:
            self.fail_operation(op, lid, d["id"], str(exc) if isinstance(exc, ObservationError) else type(exc).__name__,
                                ended=isinstance(exc, VerificationError))

    def fail_operation(self, op, lease, device, reason, *, ended=False):
        with self.tx():
            own = self.db.execute("SELECT * FROM operations WHERE id=?", (op,)).fetchone()
            current = self.db.execute("SELECT * FROM leases WHERE id=?", (lease,)).fetchone()
            if not own or not current or current["state"] == "closed" or own["fence"] != self.device(device)["fence"]:
                self.db.execute("UPDATE operations SET state='unknown',result=? WHERE id=?", (json.dumps({"reason": reason, "stale_completion": True}), op))
                self.event("stale_operation_completion", device=device, lease=lease, operation=op)
                return
            self.db.execute("UPDATE operations SET state=?,result=? WHERE id=?",
                            ("failed" if ended else "unknown", json.dumps({"reason": reason}), op))
            if ended and current["state"] == "returning":
                # The waiting return can now verify health/exit itself. Retain its
                # fence and the borrower's existing stopped acknowledgement.
                self.event("operation_failed", device=device, lease=lease, operation=op, payload={"reason": reason})
                return
            self.db.execute("UPDATE devices SET state='quarantined',fence=fence+1 WHERE id=?", (device,))
            self.db.execute("UPDATE leases SET state='revoked',reason='operation_unknown' WHERE id=?", (lease,))
            self.db.execute("UPDATE claims SET state='uncertain' WHERE device=?", (device,))
            self.event("operation_failed" if ended else "operation_unknown", device=device, lease=lease, operation=op, payload={"reason": reason})

    def renew(self, lease, token):
        l = self.auth_lease(lease, token)
        deadline = min(self.clock()+TTL, l["maximum"])
        if deadline <= self.clock():
            raise Fault("maximum_lease_reached", 4)
        with self.tx():
            self.db.execute("UPDATE leases SET heartbeat=?,deadline=? WHERE id=?", (self.clock(), deadline, lease))
            self.event("lease_renewed", lease=lease, device=l["device"])
        return self.lease(lease)

    def lease(self, lease):
        row = self.db.execute("SELECT * FROM leases WHERE id=?", (lease,)).fetchone()
        if not row:
            raise Fault("lease_not_found")
        out = self.unpack(row)
        if row["state"] == "closed" and row["device"] is None:
            archived = self.db.execute("SELECT value FROM meta WHERE key=?", ("lease_device:"+lease,)).fetchone()
            if archived:
                out["device"] = archived[0]
        out["selector"] = self.device(row["device"])["observation"] if row["state"] == "active" and row["epoch"] == self.epoch else {}
        out["remaining_seconds"] = max(0, row["deadline"] - self.clock()) if row["epoch"] == self.epoch and row["state"] == "active" else 0
        out["deadline_utc"] = datetime.fromtimestamp(self.wall()+out["remaining_seconds"], timezone.utc).isoformat() if out["remaining_seconds"] else None
        for key in ("start", "heartbeat", "deadline", "maximum"):
            out[key+"_monotonic"] = out.pop(key)
        out["renewal_required"] = False
        saved = self.db.execute("SELECT value FROM meta WHERE key=?", ("request_owner:"+row['request'],)).fetchone()
        out["owner_process"] = json.loads(saved[0]) if saved else None
        out["automatic_return"] = "stop_emulator; physical_requires_attestation" if saved else "unavailable_without_owner"
        out["attention"] = self._attention(row["device"]) if row["state"] != "closed" else None
        return out

    def cancel(self, req, token):
        self.auth_request(req, token)
        r = self.request(req)
        if r["lease"] and r["lease"]["state"] != "closed":
            return self.return_lease(r["lease"]["id"], token, "request_cancelled")
        with self.tx():
            self.db.execute("UPDATE requests SET state='cancelled' WHERE id=? AND state='queued'", (req,))
            self.event("request_cancelled", request=req)
        return self.request(req)

    def return_lease(self, lease, token, reason):
        l = self.auth_lease(lease, token, current=False)
        return self._begin_return(l, reason)

    def _begin_return(self, l, reason):
        lease = l["id"]
        if l["state"] == "closed":
            return self.lease(lease)
        if self.db.execute("SELECT 1 FROM operations WHERE lease=? AND kind='auto-reclaim' AND state IN ('running','waiting')", (lease,)).fetchone():
            raise Fault("auto_reclaim_in_progress", 5)
        old = self.db.execute("SELECT * FROM handoffs WHERE lease=? AND state!='completed' AND operation IN (SELECT id FROM operations WHERE kind='return')", (lease,)).fetchone()
        if old and old["epoch"] == self.epoch and old["fence"] == self.device(l["device"])["fence"]:
            return dict(old)
        d = self.device(l["device"])
        with self.tx():
            self.db.execute("UPDATE operations SET state='cancelled' WHERE lease=? AND state='prepared' AND kind IN ('wake','close-system-anr')", (lease,))
            self.db.execute("UPDATE handoffs SET state='completed' WHERE lease=? AND operation IN (SELECT id FROM operations WHERE state='cancelled')", (lease,))
            self.db.execute("UPDATE leases SET state='returning',reason=? WHERE id=?", (reason, lease))
            self.db.execute("UPDATE devices SET state='returning' WHERE id=?", (d["id"],))
            op = self._operation(d, l, "return")
            hid = ident()
            self.db.execute("INSERT INTO handoffs VALUES(?,?,?,?,?,'awaiting_stop',NULL,NULL,?)", (hid, lease, op, d["fence"], self.epoch, self.wall()))
            self.event("stop_requested", device=d["id"], lease=lease, operation=op, payload={"handoff_id": hid, "reason": reason})
        return dict(self.db.execute("SELECT * FROM handoffs WHERE id=?", (hid,)).fetchone())

    def recover(self, lease, token, kind):
        l = self.auth_lease(lease, token)
        if kind not in ("wake", "close-system-anr"):
            raise Fault("unsupported_recovery")
        if self.db.execute("SELECT 1 FROM handoffs WHERE lease=? AND state!='completed'", (lease,)).fetchone():
            raise Fault("handoff_already_pending", 5)
        recent = self.db.execute("SELECT count(*) FROM operations WHERE device=? AND kind IN ('wake','close-system-anr') AND created>?", (l["device"], self.wall()-300)).fetchone()[0]
        if recent >= 3:
            with self.tx():
                self._quarantine(l, "recovery_budget_exhausted")
            raise Fault("recovery_budget_exhausted", 5)
        d = self.device(l["device"])
        with self.tx():
            op = self._operation(d, l, kind)
            hid = ident()
            self.db.execute("INSERT INTO handoffs VALUES(?,?,?,?,?,'awaiting_pause',NULL,NULL,?)", (hid, lease, op, l["fence"], self.epoch, self.wall()))
            self.event("pause_requested", device=d["id"], lease=lease, operation=op, payload={"handoff_id": hid, "test_interval": "interrupted/unknown if ANR"})
        return dict(self.db.execute("SELECT * FROM handoffs WHERE id=?", (hid,)).fetchone())

    def acknowledge(self, hid, token, state, source="borrower cooperative attestation"):
        h = self.db.execute("SELECT * FROM handoffs WHERE id=?", (hid,)).fetchone()
        if not h:
            raise Fault("handoff_not_found")
        l = self.auth_lease(h["lease"], token, current=False)
        if h["state"] == "completed":
            return {**dict(h), "resume_permitted": False, "reason": "historical_acknowledgement_only"}
        if h["state"] == "acknowledged":
            return {"state": "in_progress", "operation_id": h["operation"]}
        if h["epoch"] != self.epoch or h["fence"] != self.device(l["device"])["fence"]:
            raise Fault("handoff_fenced", 4)
        op = self.db.execute("SELECT * FROM operations WHERE id=?", (h["operation"],)).fetchone()
        required = "stopped" if op["kind"] == "return" else "paused"
        if state != required:
            raise Fault("acknowledgement_requires_"+required)
        if required == "paused":
            self.auth_lease(l["id"], token)
        with self.tx():
            self.db.execute("UPDATE handoffs SET ack=?,source=?,state='acknowledged' WHERE id=?", (state, source, hid))
            self.db.execute("UPDATE operations SET state='running' WHERE id=?", (op["id"],))
            self.event("handoff_acknowledged", lease=l["id"], device=l["device"], operation=op["id"], payload={"state": state, "source": source, "physical_exclusion": "cooperative only"})
        if required == "stopped":
            return self._finish_return(l, h, op)
        d = self.device(l["device"])
        try:
            self.reconcile()
            self.auth_lease(l["id"], token)
            d = self.device(d["id"])
            if not self.complete:
                raise ObservationError("inventory unknown")
            result = self.external(self.adapter.recover, d, d["observation"], op["kind"])
            self.auth_lease(l["id"], token)
            with self.tx():
                self.db.execute("UPDATE operations SET state='completed',result=? WHERE id=?", (json.dumps(result), op["id"]))
                self.db.execute("UPDATE handoffs SET state='completed' WHERE id=?", (hid,))
                self.event("resume_permitted", device=d["id"], lease=l["id"], operation=op["id"], payload=result)
            return {"state": "completed", "resume_permitted": True, "operation_id": op["id"], "selector": d["observation"]}
        except (ObservationError, OSError, ValueError, Fault) as exc:
            self.fail_operation(op["id"], l["id"], d["id"], str(exc) if isinstance(exc, (ObservationError, Fault)) else type(exc).__name__)
            return {"state": "quarantined", "resume_permitted": False}

    def _return_current(self, l, h, op):
        current = self.db.execute("SELECT * FROM leases WHERE id=?", (l["id"],)).fetchone()
        device = self.device(l["device"])
        if not current or current["state"] != "returning" or device["state"] != "returning" or device["fence"] != op["fence"] or h["epoch"] != self.epoch:
            raise ObservationError("return ownership changed")
        return device

    def _finish_return(self, l, h, op):
        d = self.device(l["device"])
        # Unknown old subprocesses need explicit operator observation; borrower stop alone is insufficient.
        unknown = self.db.execute("SELECT 1 FROM operations WHERE device=? AND state IN ('unknown','running','prepared') AND id!=?", (d["id"], op["id"])).fetchone()
        if unknown:
            with self.tx():
                self.db.execute("UPDATE operations SET state='waiting' WHERE id=?", (op["id"],))
                self.db.execute("UPDATE handoffs SET state='acknowledged' WHERE id=?", (h["id"],))
            return {"state": "quarantined", "reason": "old_coordinator_operations_need_attestation"}
        try:
            self._return_current(l, h, op)
            stopping = json.loads(op["result"] or "{}").get("stop_requested") is True
            self.reconcile()
            d = self._return_current(l, h, op)
            if not self.complete:
                if stopping:
                    self.db.execute("UPDATE operations SET state='waiting' WHERE id=?", (op["id"],))
                    return {"state": "returning", "reason": "stop_confirmation_pending",
                            "operation_id": op["id"], "occupancy_retained": True}
                raise ObservationError("inventory unknown")
            waiters = any(self.matches(d, json.loads(r["body"])) for r in self.db.execute("SELECT body FROM requests WHERE state='queued'"))
            if stopping or (d["kind"] == "emulator" and d["observation"].get("online") and (not waiters or h["source"] == "borrower_process_exited")):
                if not stopping:
                    self.external(self.adapter.stop, d, d["observation"])
                    self._return_current(l, h, op)
                    with self.tx():
                        self.db.execute("UPDATE operations SET result=? WHERE id=?",
                                        ('{"stop_requested":true}', op["id"]))
                        self.event("emulator_stop_requested", device=d["id"], lease=l["id"], operation=op["id"])
                self.reconcile()
                d = self._return_current(l, h, op)
                if not self.complete or d["observation"].get("online"):
                    with self.tx():
                        self.db.execute("UPDATE operations SET state='waiting' WHERE id=?", (op["id"],))
                    return {"state": "returning", "reason": "stop_confirmation_pending",
                            "operation_id": op["id"], "occupancy_retained": True}
            elif d["observation"].get("online"):
                if not self.external(self.adapter.healthy, d, d["observation"]):
                    raise ObservationError("handoff health unknown")
            elif d["kind"] == "physical":
                raise ObservationError("physical offline")
            self._return_current(l, h, op)
            with self.tx():
                self.db.execute("UPDATE leases SET state='closed',closed_at=? WHERE id=?", (self.wall(), l["id"]))
                self.db.execute("UPDATE requests SET state='closed' WHERE id=?", (l["request"],))
                self.db.execute("UPDATE devices SET state='available',fence=fence+1 WHERE id=?", (d["id"],))
                if not d["observation"].get("online"):
                    self.db.execute("DELETE FROM claims WHERE device=?", (d["id"],))
                self.db.execute("UPDATE operations SET state='completed',result=? WHERE id=?", ('{"baseline":"retained; borrower changes unobserved"}', op["id"]))
                self.db.execute("UPDATE handoffs SET state='completed' WHERE id=?", (h["id"],))
                self.event("lease_closed", device=d["id"], lease=l["id"], operation=op["id"], payload={"baseline": "retained; borrower changes unobserved"})
            return self.lease(l["id"])
        except (ObservationError, OSError, ValueError) as exc:
            self.fail_operation(op["id"], l["id"], d["id"], str(exc) if isinstance(exc, (ObservationError, Fault)) else type(exc).__name__)
            return {"state": "quarantined", "reason": "return_unknown"}

    def operator_reconcile(self, device, reason, stopped=False, operations_ended=False):
        if not reason:
            raise Fault("reconciliation_reason_required")
        if self.device(device)["state"] == "retired":
            raise Fault("retired_registration_requires_new_device_id", 5)
        if self.db.execute("SELECT 1 FROM operations WHERE device=? AND (state='running' OR (kind='auto-reclaim' AND state='waiting'))", (device,)).fetchone():
            raise Fault("device_operation_in_progress", 5)
        result = self.reconcile()
        if not self.complete:
            return result
        # Attestation explicitly covers old coordinator subprocesses, never inferred from PID.
        if operations_ended:
            with self.tx():
                self.db.execute("UPDATE operations SET state='operator_verified_ended' WHERE device=? AND state='unknown'", (device,))
                self.event("operator_operations_ended", device=device, payload={"reason": reason, "source": "operator attestation"})
        l = self.db.execute("SELECT * FROM leases WHERE device=? AND state!='closed'", (device,)).fetchone()
        if l and stopped:
            # Reuse return protocol with new epoch/fence, old handoffs stay historical.
            with self.tx():
                self.db.execute("UPDATE handoffs SET state='completed' WHERE lease=?", (l["id"],))
                self.db.execute("UPDATE leases SET state='returning' WHERE id=?", (l["id"],))
                self.db.execute("UPDATE devices SET state='returning' WHERE id=?", (device,))
                d = self.device(device)
                opid = self._operation(d, l, "return")
                hid = ident()
                self.db.execute("INSERT INTO handoffs VALUES(?,?,?,?,?,'acknowledged','stopped','operator attestation',?)", (hid, l["id"], opid, d["fence"], self.epoch, self.wall()))
                self.event("operator_stopped_attestation", device=device, lease=l["id"], payload={"reason": reason})
            h = self.db.execute("SELECT * FROM handoffs WHERE id=?", (hid,)).fetchone()
            op = self.db.execute("SELECT * FROM operations WHERE id=?", (opid,)).fetchone()
            return self._finish_return(l, h, op)
        if not l:
            pending = self.db.execute("SELECT 1 FROM operations WHERE device=? AND state='unknown'", (device,)).fetchone()
            if not pending and (self.device(device)["state"] != "quarantined" or stopped):
                d = self.device(device)
                with self.tx():
                    self.db.execute("UPDATE devices SET state=? WHERE id=?", ("available" if d["kind"] == "emulator" or d["observation"].get("online") else "offline", device))
                    if stopped:
                        self.db.execute("UPDATE claims SET state='live' WHERE device=? AND state='foreign'", (device,))
                    self.event("operator_reconciled", device=device, payload={"reason": reason, "old_client_stopped": stopped})
        return {"state": self.device(device)["state"], "occupancy_retained": bool(l)}

    def history(self, after=0, device=None, lease=None, task=None):
        sql, args = "SELECT * FROM events WHERE seq>?", [int(after)]
        if device:
            sql += " AND device=?"; args.append(device)
        if lease:
            sql += " AND lease=?"; args.append(lease)
        if task:
            sql += " AND (request IN (SELECT id FROM requests WHERE json_extract(body,'$.task')=?) OR lease IN (SELECT l.id FROM leases l JOIN requests r ON l.request=r.id WHERE json_extract(r.body,'$.task')=?))"; args.extend([task, task])
        result = [self.unpack(r) for r in self.db.execute(sql+" ORDER BY seq LIMIT 1000", args)]
        for event in result:
            event["policy_version"] = POLICY
            event["actor_uid"] = os.getuid()
            event["at_utc"] = datetime.fromtimestamp(event["at"], timezone.utc).isoformat()
        return result

    def backup(self, destination):
        target = Path(destination)
        if target.exists():
            raise Fault("backup_destination_exists")
        secure_write(target, "")
        out = sqlite3.connect(target)
        try:
            self.db.backup(out)
        finally:
            out.close()
        with self.tx():
            self.event("backup_created", payload={"consistent": True})
        return {"state": "backed_up", "path": str(target), "contains": "credential hashes, private task metadata"}

    def retention(self):
        cutoff = self.wall()-180*86400
        # Preserve all live chains; delete closed chains only as complete units.
        with self.tx():
            ids = [r[0] for r in self.db.execute("SELECT id FROM leases WHERE state='closed' AND closed_at<?", (cutoff,))]
            for lid in ids:
                r = self.db.execute("SELECT request FROM leases WHERE id=?", (lid,)).fetchone()[0]
                self.db.execute("DELETE FROM events WHERE lease=? OR request=?", (lid,r))
                self.db.execute("DELETE FROM handoffs WHERE lease=?", (lid,))
                self.db.execute("DELETE FROM operations WHERE lease=?", (lid,))
                self.db.execute("DELETE FROM leases WHERE id=?", (lid,))
                self.db.execute("DELETE FROM meta WHERE key=?", ("lease_device:"+lid,))
                self.db.execute("DELETE FROM requests WHERE id=?", (r,))
                self.db.execute("DELETE FROM meta WHERE key IN (?,?)", ("request_auth:"+r, "request_owner:"+r))
            unlinked = self.db.execute("DELETE FROM events WHERE at<? AND lease IS NULL AND request IS NULL AND operation IS NULL", (cutoff,)).rowcount
            self.db.execute("DELETE FROM bindings WHERE at<? AND device NOT IN (SELECT device FROM leases WHERE state!='closed')", (cutoff,))
            for request in self.db.execute("SELECT id FROM requests WHERE state IN ('cancelled','timed_out') AND created<? AND id NOT IN (SELECT request FROM leases)", (cutoff,)).fetchall():
                self.db.execute("DELETE FROM events WHERE request=?", (request[0],))
                self.db.execute("DELETE FROM meta WHERE key IN (?,?)", ("request_auth:"+request[0], "request_owner:"+request[0]))
                self.db.execute("DELETE FROM requests WHERE id=?", (request[0],))
            self.event("retention_applied", payload={"unlinked_events": unlinked, "closed_chains": len(ids), "metadata_days": 180, "attachments_days": 7})
        attachments = self.root/"attachments"
        removed = 0
        if attachments.exists():
            for p in attachments.iterdir():
                if p.is_file() and not p.is_symlink() and p.stat().st_mtime < self.wall()-7*86400:
                    p.unlink(); removed += 1
        return {"closed_chains": len(ids), "attachments_removed": removed}


def _synchronized(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        with self.locked():
            return method(self, *args, **kwargs)
    return wrapped

# One in-process state owner; external actions release this mutex. SQLite's
# BEGIN IMMEDIATE also protects durability; never share the ledger with a second daemon.
for _name in ("register", "retire", "delete", "reconcile", "borrow", "request", "device", "devices", "tick",
              "renew", "lease", "cancel", "return_lease", "recover", "acknowledge",
              "operator_reconcile", "history", "backup", "retention", "auth_lease", "auth_request"):
    setattr(Library, _name, _synchronized(getattr(Library, _name)))
