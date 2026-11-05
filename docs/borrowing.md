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
device-library --credential-file ./my-loan.credential borrow \
  --kind emulator --requires api=35 --borrower alice --session run-42 \
  --task checkout-test --request-key checkout-device --wait 60

# 2. Work: use sim-use or adb directly with the granted selector, only while the lease is active.

# 3. Return (--stopped attests all direct actions/streams stopped and completes the handoff)
device-library --credential-file ./my-loan.credential return LEASE_ID --reason done --stopped --wait 60
```

Without `--stopped`, `return` only requests the handoff, exits 3, and prints the required `handoff acknowledge HANDOFF_ID --state stopped` next action.

## borrow options you will actually use

- `--kind physical|emulator|any` (required) — a request borrows exactly one device.
- `--requires KEY=VALUE` (repeatable) — capability constraints, e.g. `--requires api=35`.
- `--wait SECONDS` — poll client-side; timeout leaves the request queued (exit 3) and returns its ID for later `request show`.
- `--borrower` / `--session` / `--request-key` (required) — identity and idempotency; see above.
- `--device ID`, `--task LABEL` — optional targeting and attribution.

## Lease rules

- A lease lasts **30 minutes** from activation, with a hard two-hour continuous maximum across explicit `lease renew LEASE_ID` calls. No periodic renewal is required.
- Only an active, current-epoch lease supplies an operation selector. Stop all direct actions when the lease expires, renewal fails, the epoch changes or the service disappears.
- CLI loans bind to your invoking parent process (`--owner-pid` overrides). If that process exits, an emulator loan is returned automatically; a physical device stays quarantined until you return it with `--stopped` or an operator reconciles it.

## Recovery

If the device misbehaves, request recovery, pause direct operations, then acknowledge the handoff:

```sh
device-library --credential-file ./my-loan.credential recover LEASE_ID --kind wake   # or close-system-anr (emulator)
device-library --credential-file ./my-loan.credential handoff acknowledge HANDOFF_ID --state paused
```

Continue only after `resume_permitted: true`.

## Exit codes

0 success · 2 invalid input/conflict · 3 queued/preparing/return pending/wait elapsed · 4 unauthorized or lease expired/fenced · 5 quarantine/policy failure · 6 service or persistence failure · 7 disconnected, result unknown (retry the same key and credential — the request may have committed).

See `docs/operator.md` for service administration and `docs/dev.md` for internals.
