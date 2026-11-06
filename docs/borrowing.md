# Borrowing a device

You operate the borrowed device **directly** with sim-use (default) or adb, using the explicit selector granted by an active lease.
The coordinator only manages who may use which device, and for how long — it is not a test runner or command gateway.
All CLI replies are JSON with `ok`, plus `result` or `reason` and an exit code.

## Credential file

- One credential file per borrower, passed as a global option **before** the command:
  `device-library --credential-file ./my-loan.credential <command>`
- The credential **is the file** — never a command-line token value. The CLI creates the file (mode 0600) before contacting the service and never prints the token.
- Never share one credential file among concurrent clients.
- Retry the identical borrower/session/request-key with the same file to retrieve your original request, including after a lost connection. Use a fresh request key for a new loan.

## The three-command flow

```sh
# 1. Borrow (queues or activates a lease; --wait polls up to N seconds)
# DEVICE_LIBRARY_OWNER_PID is supplied by the agent/CI runtime: see Owner binding below.
device-library --credential-file ./my-loan.credential borrow \
  --kind emulator --requires api=35 --borrower alice --session run-42 \
  --task checkout-test --request-key checkout-device --owner-pid "$DEVICE_LIBRARY_OWNER_PID" --wait 60

# 2. Work: use sim-use or adb directly with the granted selector, only while the lease is active.

# 3. Return (--stopped attests all direct actions/streams stopped and completes the handoff)
device-library --credential-file ./my-loan.credential return LEASE_ID --reason done --stopped --wait 60
```

Without `--stopped`, `return` only requests the handoff, exits 3, and prints the required `handoff acknowledge HANDOFF_ID --state stopped` next action.

## Retrieving a lost request or lease ID

If you lost the printed request or lease ID, look your request up by the identity you borrowed with:

```sh
device-library --credential-file ./my-loan.credential request find \
  --borrower alice --session run-42 --request-key checkout-device
```

The lookup is read-only and authenticated by your own credential file — the same boundary as `request show`, no operator credential needed. It returns the full request view, including its lease (with selector, state and remedy), exactly as `request show` would. A wrong borrower/session/request-key combination returns `request_not_found`; another borrower's credential returns `unauthorized`. Closed and expired requests stay retrievable, so you can confirm a terminal outcome before borrowing again with a fresh request key.

## borrow options you will actually use

- `--kind physical|emulator|any` (required) — a request borrows exactly one device.
- `--requires KEY=VALUE` (repeatable) — capability constraints, e.g. `--requires api=35`.
- `--wait SECONDS` — poll client-side; timeout leaves the request queued (exit 3) and returns its ID for later `request show`.
- `--borrower` / `--session` / `--request-key` (required) — identity and idempotency; see above.
- `--owner-pid PID` (required) — a stable task/session process supplied by your runtime; there is no default.
- `--device ID`, `--task LABEL` — optional targeting and attribution.

## Owner binding

Choose the process whose lifetime actually covers the task and every device action/stream. The CLI never guesses a parent or ancestor. `owner_process` in the returned lease includes the selected PID and its birth identity; identical request retries retain the original binding.

- **Direct process launch:** a long-lived Python runner can pass `str(os.getpid())` as the `--owner-pid` value when it calls `subprocess.run(["device-library", ..., "borrow", ..., "--owner-pid", str(os.getpid())])`. That runner must stay alive through work and return; the short-lived CLI PID is unsuitable.
- **Separate short shells:** the runtime supplies the same stable task PID as `DEVICE_LIBRARY_OWNER_PID` to every shell. Use `--owner-pid "$DEVICE_LIBRARY_OWNER_PID"`; do not substitute the short shell's `$$` or infer a lifetime from `$PPID`. Ending a wrapper shell then leaves the loan intact.
- **CI:** use the PID of a job supervisor that stays alive through borrow, all test steps, stream cleanup and return. A single shell script that owns that entire sequence may pass `--owner-pid "$$"`; separate CI step shells must use the runtime's stable supervisor PID instead. If the runtime cannot supply a stable owner, do not borrow until that integration is available.

Owner exit returns emulator loans automatically. Physical devices remain quarantined until a stopped return or operator reconciliation. An unprovable owner identity fails closed; the PID is never silently rebound on retry.

## Lease rules

- A lease lasts **30 minutes** from activation, with a hard two-hour continuous maximum across explicit `lease renew LEASE_ID` calls. No periodic renewal is required.
- Only `usable: true` supplies an operation selector: the lease must be active, within its deadline, at the current epoch/device fence, and have no pending recovery handoff. Stop all direct actions when the lease expires, renewal fails, the epoch changes or the service disappears.
- `state` and `effective_state` describe current usability; `recorded_state` preserves the persisted status for audit. Expiry/fence checks apply immediately on `lease show`, `request show` and same-key borrow retries, without waiting for maintenance. Pending recovery hides the selector until acknowledgement succeeds.
- Requests own queue scheduling/history; once reserved, their effective lifecycle status comes from the lease. Preparation/return failures expose `revoked`, attention and a remedy, and borrow exits 5 immediately. Closed/expired retries retrieve the old loan and exit nonzero; use a fresh request key to borrow again. Stop actions and follow the returned remedy before returning/reconciling an uncertain device.

## Recovery

If the device misbehaves, request recovery, pause direct operations, then acknowledge the handoff:

```sh
device-library --credential-file ./my-loan.credential recover LEASE_ID --kind wake   # or close-system-anr (emulator)
device-library --credential-file ./my-loan.credential handoff acknowledge HANDOFF_ID --state paused
```

Continue only after `resume_permitted: true`.

Retrying `recover` with the same lease/kind while its handoff is pending returns the existing handoff/operation, without consuming another recovery attempt. Retrying a successful acknowledgement returns its original result only while the lease, epoch and fence remain valid and no later handoff has superseded it. Otherwise it returns `resume_permitted: false`, an empty selector and a reason; do not resume from an older cached response.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 2 | Invalid input/conflict, including missing explicit owner PID |
| 3 | Queued/preparing/recovery or return pending/wait elapsed |
| 4 | Unauthorized or lease expired/fenced |
| 5 | Quarantine/policy or terminal loan failure; borrow of a closed/cancelled/timed-out request |
| 6 | Service unavailable before sending any request, or an explicit persistence failure response |
| 7 | Result unknown: sending may have started, or the reply disconnected, timed out, was truncated or could not be decoded. The mutation may have committed. |

Exit 7 includes the command, request arguments (never the credential), failure stage and remedy. Reconnect using the same credential. Retry an identical borrow to retrieve its request; retry return to retrieve its handoff/status; retry recover only to retrieve a pending same-kind handoff; retry acknowledge with the same handoff/state to retrieve its guarded result. For a lost renew response, query `lease show` first because renewal may already have committed. Query status before repeating other mutations. Do not blindly start a new recovery when the previous one may already have completed.

See `docs/operator.md` for service administration and `docs/dev.md` for internals.
