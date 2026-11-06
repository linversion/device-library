# device-library

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

English | [中文导航](README.zh-CN.md)

**Multiple AI agents debugging Android on the same machine will grab the same device at the same time and stomp on each other's sessions.** device-library is a local coordinator that stops the fight: agents borrow a device, get an exclusive lease, work on it directly, and return it when done.

An independent Python 3.10+ Android device lending coordinator with no external dependencies.
Borrowers operate devices **directly with sim-use (default) or adb**, using the explicit selector granted by an active lease. The coordinator provides lending, capacity accounting, return handshakes and narrowly scoped recovery.

## How it works

1. **Borrow** — `device-library borrow` queues or activates a 30-minute lease and returns an exclusive operation selector (the device serial/avd name) bound to your owner process.
2. **Work directly** — drive the device with your own sim-use or adb using that selector. The coordinator never proxies your actions.
3. **Return** — `device-library return LEASE_ID --stopped` attests all your actions stopped and completes the handoff, freeing the device for the next borrower.
4. **Fail safe** — one coordinator per host, a SQLite ledger as the source of truth, and epoch/lease fences that invalidate stale loans. When something goes wrong the device is quarantined for an operator, never blindly re-lent.

## 30-second start

```sh
# 1. Install
python3 -m pip install .

# 2. Start the coordinator (first run creates the ledger; register devices per docs/operator.md)
device-library serve --init

# 3. Borrow (from another terminal; --owner-pid is required, supplied by your runtime)
device-library --credential-file ./my-loan.credential borrow \
  --kind emulator --requires api=35 --borrower alice --session run-42 \
  --task checkout-test --request-key checkout-device --owner-pid "$DEVICE_LIBRARY_OWNER_PID" --wait 60

# ... work on the device directly with sim-use or adb using the granted selector ...

# 4. Return
device-library --credential-file ./my-loan.credential return LEASE_ID --reason done --stopped --wait 60
```

All replies are JSON with `ok`, plus `result` or `reason` and an exit code. Run `device-library -h` for the full command list.

## Honest boundaries

- **Cooperative coordination.** Credentials, socket and DB are mode-0600 files owned by one OS user; this is arbitration among cooperating clients, not isolation against malicious ones.
- **One host, one OS user.** Version 0.1 targets a single machine; multi-host and multi-user enforcement are out of scope.
- **Not a device farm.** No remote access, no web dashboard, no horizontal scaling — it coordinates the devices plugged into this host.
- **Not a UI-action proxy.** There is no command gateway, test runner, click/input/screenshot wrapper or borrower-process manager. You drive the device yourself; the coordinator only manages who may use which device, and for how long.

## Documentation map

- **Borrowing a device** — [docs/borrowing.md](docs/borrowing.md): the three-command loan flow, credential-file rules, lease rules, owner binding, recovery and exit codes.
- **Operating the service** — [docs/operator.md](docs/operator.md): running `serve`, registering/retiring devices, quarantine reconciliation, backup/restore and retention.
- **Developing or maintaining** — [docs/dev.md](docs/dev.md): test suite, fake runtime, ledger/coordinator protocol, adapter internals and acceptance status.

## License

[MIT](LICENSE) — Copyright (c) 2026 linversion
