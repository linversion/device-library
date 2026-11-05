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
    p.add_argument("--credential-file",
                   help="path to your private credential file (one per borrower); the credential is the file, never a command-line token")
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("serve",
                       help="(operator) run the lending coordinator service in the foreground; --init creates the first ledger")
    s.add_argument("--data-dir", default=str(Path.home()/".local/share/device-library"),
                   help="ledger data directory (default: %(default)s)")
    s.add_argument("--init", action="store_true", help="explicitly create first ledger")
    s.add_argument("--fake-fixture", help="requires --test-runtime; never invokes Android tools")
    b = sub.add_parser("borrow", formatter_class=argparse.RawDescriptionHelpFormatter,
                       help="borrow one Android device: queues or activates a 30-minute lease granting direct sim-use/adb access via an explicit selector",
                       epilog="""standard three-step flow:
  device-library --credential-file ./my-loan.credential borrow \\
      --kind emulator --borrower alice --session run-42 \\
      --request-key checkout-device --wait 60
  # ... operate the device directly with sim-use or adb using the granted selector ...
  device-library --credential-file ./my-loan.credential return LEASE_ID --stopped

--stopped attests that all direct actions and streams stopped and completes
the return handoff acknowledgement in one step.""")
    b.add_argument("--kind", choices=["physical","emulator","any"], required=True,
                   help="kind of device to borrow (a request borrows exactly one device)")
    b.add_argument("--borrower", required=True, help="your borrower identity label (attribution, not authentication)")
    b.add_argument("--session", required=True,
                   help="stable session ID; reuse it with the same key and credential file to retrieve your request")
    b.add_argument("--request-key", required=True,
                   help="idempotency key: retry the identical request to retrieve it; use a fresh key for a new loan")
    b.add_argument("--task", default="", help="optional task label recorded in the ledger")
    b.add_argument("--owner-pid", type=int, default=os.getppid(),
                   help="long-lived borrower PID (default: invoking parent); exit ends loan")
    b.add_argument("--device", help="request a specific registered device ID")
    b.add_argument("--requires", action="append", default=[], metavar="KEY=VALUE",
                   help="capability constraint, repeatable, e.g. --requires api=35")
    b.add_argument("--wait", type=float, default=0, help="seconds to poll; timeout leaves request queued")
    b.add_argument("--queue-timeout", type=float, default=3600,
                   help="seconds the request may stay queued before expiring (default: %(default)s)")
    for name in ("request","lease","handoff","operation"):
        s = sub.add_parser(name, help={
            "request": "inspect (show) a borrow request you made",
            "lease": "inspect your lease (show) or explicitly extend a still-active one (renew; two-hour continuous maximum)",
            "handoff": "acknowledge a coordinator handoff after pausing or stopping all direct device actions",
            "operation": "(operator) inspect (show) or attest an ended (end) coordinator operation"}[name])
        s.add_argument("verb", choices={"request":["show","cancel"],"lease":["show","renew"],"handoff":["acknowledge"],"operation":["show","end"]}[name],
                       help="action to perform on the record")
        s.add_argument("id", help="ID of the request, lease, handoff or operation")
        if name == "handoff":
            s.add_argument("--state", choices=["paused","stopped"], required=True,
                           help="paused: direct actions paused for recovery; stopped: all direct actions and streams stopped")
        if name == "operation":
            s.add_argument("--reason", help="required for 'end': why the operator verified this operation ended")
            s.add_argument("--old-client-stopped", action="store_true",
                           help="required for 'end' on running or current-fence operations")
    s = sub.add_parser("cancel", help="(operator) cancel a queued borrow request by ID")
    s.add_argument("id", help="request ID to cancel")
    s = sub.add_parser("return",
                       help="return a borrowed device; --stopped attests all direct actions stopped and completes the return handoff")
    s.add_argument("id", help="lease ID to return")
    s.add_argument("--reason", default="borrower_return", help="why the device is returned (default: %(default)s)")
    s.add_argument("--stopped", action="store_true", help="attest all direct actions and streams stopped; acknowledge the return handoff")
    s.add_argument("--wait", type=float, default=0, help="seconds to wait for closure after --stopped")
    s = sub.add_parser("recover",
                       help="request a narrowly scoped recovery on your leased device, then acknowledge the resulting handoff")
    s.add_argument("id", help="lease ID to recover")
    s.add_argument("--kind", choices=["wake","close-system-anr"], required=True,
                   help="wake: dismiss keyguard (physical); close-system-anr: dismiss a system ANR dialog (emulator)")
    s = sub.add_parser("devices",
                       help="list registered devices and their state; operator-only verbs register/retire/delete manage registrations")
    s.add_argument("verb", choices=["list","register","retire","delete"],
                   help="list is borrower-safe; register/retire/delete are operator-only")
    s.add_argument("id", nargs="?", help="device ID (required for retire/delete)")
    s.add_argument("--reason", help="why; required for retire")
    s.add_argument("--old-client-stopped", action="store_true",
                   help="operator attests all old direct actions stopped (retire)")
    s.add_argument("--kind", choices=["physical","emulator"], help="device kind (register)")
    s.add_argument("--alias", help="human-readable registration alias (register)")
    s.add_argument("--spec-file", help="JSON stable identity/policy; see docs/operator.md")
    for name in ("history","watch","audit"):
        s = sub.add_parser(name, help={
            "history": "(operator) read the event log, optionally filtered by --device/--lease/--task",
            "watch": "(operator) print ledger events; --follow keeps streaming new ones",
            "audit": "(operator) export audit events as a JSON envelope (audit export)"}[name])
        if name == "audit":
            s.add_argument("verb", choices=["export"], help="export (only verb)")
        s.add_argument("--after", type=int, default=0, help="paginate from this event sequence number")
        s.add_argument("--device", help="filter by device ID")
        s.add_argument("--lease", help="filter by lease ID")
        s.add_argument("--task", help="filter by task label")
        if name == "watch":
            s.add_argument("--follow", action="store_true", help="keep polling and print new events")
    s = sub.add_parser("reconcile",
                       help="(operator) release device quarantine after verifying old borrowers and coordinator operations ended")
    s.add_argument("--device", required=True, help="device ID to reconcile")
    s.add_argument("--reason", required=True, help="why you attest; a reason alone only records an observation")
    s.add_argument("--old-client-stopped", action="store_true", help="operator attests ALL old direct actions stopped")
    s.add_argument("--old-operations-ended", action="store_true", help="operator verified old coordinator subprocesses ended")
    s = sub.add_parser("backup", help="(operator) write a consistent SQLite backup of the ledger; refuses overwrite")
    s.add_argument("path", help="destination file for the backup")
    s = sub.add_parser("restore", help="(operator) offline-restore the ledger from a backup; requires the operator credential")
    s.add_argument("path", help="backup file to restore from")
    s.add_argument("--data-dir", required=True, help="data directory to restore into")
    sub.add_parser("retention", help="(operator) delete expired metadata per the 180-day/7-day retention policy")
    sub.add_parser("doctor", help="check service health, ledger freshness and support status without contacting or repairing devices")
    sub.add_parser("shutdown", help="(operator) stop the running coordinator service")
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
                command = {("request","cancel"):"cancel",("lease","renew"):"renew",("handoff","acknowledge"):"acknowledge",("operation","end"):"operation_end"}.get((command,a.verb),command)
                args = {"id":a.id}
                if command == "acknowledge": args["state"] = a.state
                if command == "operation_end":
                    if not a.reason:
                        raise Fault("operation_end_reason_required")
                    args["reason"] = a.reason
                    args["stopped"] = a.old_client_stopped
            elif command in ("cancel","return","recover"):
                args = {"id":a.id}
                if command == "return": args["reason"] = a.reason
                if command == "recover": args["kind"] = a.kind
            elif command == "devices" and a.verb == "delete":
                if not a.id:
                    raise Fault("deletion_requires_id")
                command = "delete"
                args = {"id":a.id}
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
