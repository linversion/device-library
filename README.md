# Android device library

An independent Python 3.10+ Android device lending coordinator with no Firstmate dependencies.
Borrowers operate devices **directly with sim-use (default) or adb**, using the explicit selector granted by an active lease.
The coordinator provides lending, capacity accounting, return handshakes and narrowly scoped recovery.
There is no ordinary-action gateway, test runner, click/input/screenshot wrapper or borrower-process manager.

Version 0.1 targets one host and one OS user.
Credentials, socket and DB are mode 0600 files owned by that user, so this is cooperative arbitration rather than malicious-client isolation.

## Install

```sh
python3 -m pip install .
device-library -h
```

All replies are JSON with `ok`, plus `result` or `reason` and an exit code.
Global options such as `--credential-file` precede the command.

## Which document should I read?

- **I want to borrow a device** → `device-library -h` and `device-library borrow -h` cover the commands;
  [docs/borrowing.md](docs/borrowing.md) has the three-command loan flow, credential-file rules, lease rules and exit codes.
- **I operate the service** (deploy `serve`, register/retire devices, reconcile quarantine, backup/restore, retention) →
  [docs/operator.md](docs/operator.md).
- **I develop or maintain this tool** (test suite, fake runtime, ledger/coordinator protocol, adapter internals, acceptance status) →
  [docs/dev.md](docs/dev.md).
