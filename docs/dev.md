# Developer guide

Building, testing and understanding the internals of the device library.

## Install and test without devices

```sh
python3 -m pip install .
# Installation is optional for source execution:
python3 -m device_library --help
python3 -m unittest discover -s tests -v
```

Tests use temporary directories, FakeAdapter or injected command runners, and owned fake coordinator subprocesses that they clean up.
They never run adb/android or enumerate real devices.
The socket suite includes a complete independent CLI demonstration: register, borrow, renew, return, restart, consistent backup and offline restore.

## Fake runtime (test-runtime)

For an interactive fake-only demo, keep the runtime path short enough for Unix sockets (under 104 bytes including socket filename):

```sh
mkdir -m 700 .demo
printf '{}\n' > .demo/fixture.json
python3 -m device_library --test-runtime "$PWD/.demo/run" serve \
  --init --data-dir "$PWD/.demo/data" --fake-fixture "$PWD/.demo/fixture.json"
# In another terminal, same folder:
python3 -m device_library --test-runtime "$PWD/.demo/run" doctor
python3 -m device_library --test-runtime "$PWD/.demo/run" \
  --credential-file "$PWD/.demo/run/operator.credential" shutdown
```

`serve --test-runtime` requires `--fake-fixture`, and the inverse is also required.
A fake service cannot instantiate the real adapter, and client test endpoints require a fake manifest.
An alternate runtime cannot create another production device pool.
Do not reuse fixture data as a production ledger.

## Ledger and coordinator protocol

Short BEGIN IMMEDIATE transactions and unique request/device-occupancy constraints arbitrate lending.
The in-process state mutex is released around external commands; current operation, reservation and fence are rechecked before completion.
The scheduler prepares one device at a time without restricting the three concurrently running loans.
Discovery snapshots are discarded if relevant device/claim/operation topology changed during observation.
Timeout/unknown outcomes preserve occupancy and quarantine instead of replaying an action.
Events carry UTC time and policy, and state transitions share their SQLite transaction.
Identity binding history and operation results preserve routing/recovery facts without full screen or logcat payloads.
Ordinary borrower actions remain unobserved.
Epoch/fence validation protects coordinator APIs and cannot intercept direct sim-use/adb actions.

Queue discipline is FIFO among earliest satisfiable requests; an impossible constraint does not block later compatible requests.
The three-emulator cap includes starting, running, stopping/uncertain and foreign claims.
Unknown inventory blocks new loans; external lifecycle tools can still violate this cooperative cap.

## Borrower-process ownership

The server captures the owner PID's kernel birth identity (macOS start time including microseconds; Linux boot ID and process
start ticks), so PID reuse counts as owner exit. The recorded `owner_process` is immutable for an idempotent request.
Maintenance checks the owner each cycle: a confirmed exit cancels an unallocated request and starts an emulator return; physical
devices stay quarantined with reason `borrower_process_exited`. Unreadable process information quarantines an active loan with
reason `borrower_process_unknown`. Pre-upgrade records without an owner advertise `automatic_return=unavailable_without_owner`.

## Adapter details

- Process discovery selects emulator executables before reading their AVD arguments; words inside unrelated command arguments do not count as emulator processes.
- Layout JSON is read from Android CLI's `--output` file because stdout can also contain instrumentation installation messages.
  `--no-idle` requests the current full tree without requiring a quiet UI; Android CLI 1.0.16261425 can exit successfully without
  writing output when continuous layout events prevent idle. After a missing output, layout is reobserved at most twice, one
  second apart, using fresh output paths; unreadable or invalid output fails immediately, and three missing outputs fail preparation.
- Pixel 7 wake recovery uses WAKEUP and dismiss-keyguard, then reobserves keyguard policy (at most two seconds and 21 reads,
  100 ms apart) until explicit unlock evidence appears. Verification requires explicit `isStatusBarKeyguard`, `mShowingLockscreen`,
  `mKeyguardShowing` or `isKeyguardShowing=false` evidence without contradictory true/secure fields; on Android 15, absent those
  legacy fields, both `inputRestricted=false` and `dreaming=false` are required. Unexpected credential challenges fail closed.
- Emulator ANR recovery accepts Android CLI JSON elements with package/packageName=`android`, resourceId=`android:id/aerr_close`,
  enabled=true and integer bounds (object or four-item array), plus system-process/System UI ANR text and a system ANR window
  from dumpsys. It clicks the observed bounds once and verifies the dialog cleared and device health responds. Unrecognized or
  localized formats fail closed; app-owned Close buttons and app ANRs are not clicked. No screenshots are collected by default.

## Acceptance status

Version 0.1 targets one host and one OS user.
The state machine, Unix protocol and command construction are validated with sandbox fixtures and injected runners.
The real adapter has also passed register/start/borrow/renew/return/stop acceptance on an API 35 emulator and wake/borrow/renew/return
acceptance on a no-credential Android 15 Pixel 7.
Other Android CLI/process formats, locked Pixel 7 behavior, localized system ANR evidence, host resources for three emulators and
suspend behavior need separate live acceptance.
Multi-user/multi-host enforcement, malicious-client isolation, automatic clean baselines, external-process takeover, OS-service
installation and same-lease restart continuation are outside this release.
