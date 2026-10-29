"""JSON CLI. Credentials are files, never command line token values."""
import argparse
import json
import math
import os
import secrets
import sqlite3
import sys
import time
from pathlib import Path
from .core import Fault, secure_write
from .server import Server, call, production_runtime, read_credential, restore


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--json", action="store_true", help="all output is JSON (accepted for compatibility)")
    p.add_argument("--test-runtime", help="isolated fake-only endpoint; requires test manifest")
    p.add_argument("--credential-file")
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--data-dir", default=str(Path.home()/".local/share/device-library"))
    s.add_argument("--init", action="store_true", help="explicitly create first ledger")
    s.add_argument("--fake-fixture", help="requires --test-runtime; never invokes Android tools")
    b = sub.add_parser("borrow")
    b.add_argument("--kind", choices=["physical","emulator","any"], required=True)
    b.add_argument("--borrower", required=True)
    b.add_argument("--session", required=True)
    b.add_argument("--request-key", required=True)
    b.add_argument("--task", default="")
    b.add_argument("--owner-pid", type=int, default=os.getppid(),
                   help="long-lived borrower PID (default: invoking parent); exit ends loan")
    b.add_argument("--device")
    b.add_argument("--requires", action="append", default=[], metavar="KEY=VALUE")
    b.add_argument("--wait", type=float, default=0, help="seconds to poll; timeout leaves request queued")
    b.add_argument("--queue-timeout", type=float, default=3600)
    for name in ("request","lease","handoff","operation"):
        s = sub.add_parser(name)
        s.add_argument("verb", choices={"request":["show","cancel"],"lease":["show","renew"],"handoff":["acknowledge"],"operation":["show"]}[name])
        s.add_argument("id")
        if name == "handoff":
            s.add_argument("--state", choices=["paused","stopped"], required=True)
    s = sub.add_parser("cancel"); s.add_argument("id")
    s = sub.add_parser("return"); s.add_argument("id"); s.add_argument("--reason", default="borrower_return")
    s.add_argument("--stopped", action="store_true", help="attest all direct actions and streams stopped; acknowledge the return handoff")
    s.add_argument("--wait", type=float, default=0, help="seconds to wait for closure after --stopped")
    s = sub.add_parser("recover"); s.add_argument("id"); s.add_argument("--kind", choices=["wake","close-system-anr"], required=True)
    s = sub.add_parser("devices")
    s.add_argument("verb", choices=["list","register","retire"])
    s.add_argument("id", nargs="?")
    s.add_argument("--reason")
    s.add_argument("--old-client-stopped", action="store_true")
    s.add_argument("--kind", choices=["physical","emulator"])
    s.add_argument("--alias")
    s.add_argument("--spec-file", help="JSON stable identity/policy; see README")
    for name in ("history","watch","audit"):
        s = sub.add_parser(name)
        if name == "audit":
            s.add_argument("verb", choices=["export"])
        s.add_argument("--after", type=int, default=0)
        s.add_argument("--device"); s.add_argument("--lease"); s.add_argument("--task")
        if name == "watch":
            s.add_argument("--follow", action="store_true")
    s = sub.add_parser("reconcile")
    s.add_argument("--device", required=True); s.add_argument("--reason", required=True)
    s.add_argument("--old-client-stopped", action="store_true", help="operator attests ALL old direct actions stopped")
    s.add_argument("--old-operations-ended", action="store_true", help="operator verified old coordinator subprocesses ended")
    s = sub.add_parser("backup"); s.add_argument("path")
    s = sub.add_parser("restore"); s.add_argument("path"); s.add_argument("--data-dir", required=True)
    sub.add_parser("retention")
    sub.add_parser("doctor")
    sub.add_parser("shutdown")
    return p


def main(argv=None):
    a = parser().parse_args(argv)
    try:
        if a.command == "serve":
            server = Server(a.data_dir, initialize=a.init, test_runtime=a.test_runtime, fixture=a.fake_fixture)
            import signal
            def stop(signum, frame):
                server.running = False
            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGINT, stop)
            server.serve()
            return 0
        if a.test_runtime:
            manifest = Path(a.test_runtime)/"library.json"
            if not manifest.exists() or not json.loads(manifest.read_text()).get("test"):
                raise Fault("test_endpoint_requires_fake_manifest", 6)
        if a.command == "restore":
            # Restore is offline and authorized by possession of operator credential.
            expected = (Path(a.test_runtime) if a.test_runtime else production_runtime())/"operator.credential"
            import hmac
            if not a.credential_file or not hmac.compare_digest(read_credential(a.credential_file), read_credential(expected)):
                raise Fault("operator_credential_required", 4)
            result = {"schema_version": 1, "ok": True, "result": restore(a.data_dir, a.path, runtime=a.test_runtime)}
        else:
            command, args = a.command, {}
            token = read_credential(a.credential_file) if a.credential_file and Path(a.credential_file).exists() else ""
            if command == "return" and (not math.isfinite(a.wait) or a.wait < 0 or a.wait > 86400):
                raise Fault("invalid_wait")
            if command == "return" and a.wait and not a.stopped:
                raise Fault("return_wait_requires_stopped_attestation")
            if command == "borrow":
                if not math.isfinite(a.wait) or a.wait < 0 or a.wait > 86400:
                    raise Fault("invalid_wait")
                if not a.credential_file:
                    raise Fault("borrow_requires_credential_file")
                # Create before contacting server: disconnect cannot lose the lease token.
                if not token:
                    secure_write(a.credential_file, secrets.token_urlsafe(32))
                    token = read_credential(a.credential_file)
                requires = {}
                for entry in a.requires:
                    k, v = entry.split("=", 1); requires[k] = v
                args = {"borrower":a.borrower,"session":a.session,"key":a.request_key,"owner_pid":a.owner_pid,
                        "body":{"kind":a.kind,"task":a.task,"device":a.device,"requires":requires,"queue_timeout":a.queue_timeout}}
            elif command in ("request","lease","handoff","operation"):
                command = {("request","cancel"):"cancel",("lease","renew"):"renew",("handoff","acknowledge"):"acknowledge"}.get((command,a.verb),command)
                args = {"id":a.id}
                if command == "acknowledge": args["state"] = a.state
            elif command in ("cancel","return","recover"):
                args = {"id":a.id}
                if command == "return": args["reason"] = a.reason
                if command == "recover": args["kind"] = a.kind
            elif command == "devices" and a.verb == "retire":
                if not a.id or not a.reason:
                    raise Fault("retirement_requires_id_reason")
                command = "retire"
                args = {"id":a.id,"reason":a.reason,"stopped":a.old_client_stopped}
            elif command == "devices" and a.verb == "register":
                if not a.spec_file or not a.kind or not a.alias:
                    raise Fault("registration_requires_kind_alias_spec")
                command = "register"
                args = {"kind":a.kind,"alias":a.alias,"spec":json.loads(Path(a.spec_file).read_text())}
            elif command in ("history","watch","audit"):
                args = {"after":a.after,"device":a.device,"lease":a.lease,"task":a.task}
            elif command == "reconcile":
                args = {"device":a.device,"reason":a.reason,"stopped":a.old_client_stopped,"operations_ended":a.old_operations_ended}
            elif command == "backup": args = {"path":a.path}
            result = call(command, args, token, a.test_runtime)
            if a.command == "return" and result.get("ok"):
                handoff = result["result"]
                if handoff.get("state") == "awaiting_stop" and not a.stopped:
                    handoff["next_action"] = {"command": "handoff acknowledge", "handoff_id": handoff["id"],
                                              "state": "stopped", "requires": "Stop all direct actions and streams before acknowledging"}
                if a.stopped and handoff.get("state") in ("awaiting_stop", "acknowledged"):
                    result = call("acknowledge", {"id": handoff["id"], "state": "stopped"}, token, a.test_runtime)
                if a.stopped and result.get("ok") and result["result"].get("state") in ("returning", "in_progress"):
                    deadline = time.monotonic() + a.wait
                    while time.monotonic() < deadline:
                        time.sleep(min(.2, max(0, deadline-time.monotonic())))
                        result = call("lease", {"id": a.id}, token, a.test_runtime)
                        if not result.get("ok") or result["result"].get("state") != "returning":
                            break
            if a.command == "borrow" and result.get("ok"):
                deadline = time.monotonic()+max(0,a.wait)
                while result["result"]["state"] in ("queued","preparing") and time.monotonic() < deadline:
                    time.sleep(min(.2,max(0,deadline-time.monotonic())))
                    result = call("request", {"id":result["result"]["id"]}, token, a.test_runtime)
                    if not result.get("ok"): break
            if a.command == "watch" and a.follow:
                while True:
                    print(json.dumps(result), flush=True)
                    if not result.get("ok"): return result.get("exit_code", 6)
                    events = result["result"]["events"]
                    if events: args["after"] = events[-1]["seq"]
                    time.sleep(1)
                    result = call("watch", args, token, a.test_runtime)
        print(json.dumps(result))
        if not result.get("ok"): return result.get("exit_code", 2)
        state = result.get("result", {}).get("state")
        if a.command == "return" and state in ("revoked", "expired"):
            return 5
        return 3 if state in ("queued","preparing","timed_out","awaiting_stop","acknowledged","returning","in_progress") else 5 if state == "quarantined" else 0
    except Fault as exc:
        print(json.dumps({"schema_version":1,"ok":False,"reason":exc.reason,"exit_code":exc.code}))
        return exc.code
    except sqlite3.Error:
        print(json.dumps({"schema_version":1,"ok":False,"reason":"ledger_unwritable_or_corrupt","exit_code":6}))
        return 6
    except (OSError, ValueError, KeyError) as exc:
        print(json.dumps({"schema_version":1,"ok":False,"reason":"invalid_input_or_io_failure","detail":type(exc).__name__,"exit_code":2}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
