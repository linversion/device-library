# Android device library

An independent Python 3.10+ Android device lending coordinator with no Firstmate dependencies.
Borrowers operate devices **directly with sim-use (default) or adb**, using the granted explicit selector.
The coordinator provides lending, capacity accounting, return handshakes and narrowly scoped recovery.
There is no ordinary-action gateway, test runner, click/input/screenshot wrapper or borrower-process manager.

## Status and trust boundary

Version 0.1 targets one host and one OS user.
The state machine, Unix protocol and command construction are validated with sandbox fixtures and injected runners.
The real adapter has also passed register/start/borrow/renew/return/stop acceptance on an API 35 emulator and wake/borrow/renew/return acceptance on a no-credential Android 15 Pixel 7.
Localized system ANRs, three-emulator host capacity and suspend behavior still need separate live acceptance.
Installation does not start a service or contact a device.
Production `serve` explicitly opts into discovery and device commands; it is not a harmless demo command.

The production socket and lock always live at `~/.device-library-runtime/`, independently of cwd, FM_HOME, XDG variables and `--data-dir`.
The selected data directory is pinned there; a different directory is refused rather than opening another pool.
Default data is `~/.local/share/device-library/`; use local storage, never network/cloud-synchronized storage.
Directories are private, and credentials, socket and DB are mode 0600.
The operator credential file is `~/.device-library-runtime/operator.credential`.
The same OS user can access these files, so this provides cooperative arbitration rather than malicious-client isolation.

Epoch/fence validation protects coordinator APIs and cannot intercept direct sim-use/adb actions.
Borrowers must stop using a device when its 30-minute lease expires, explicit renewal fails, the service disappears, the epoch changes or a connection changes.
They must stop all direct actions and streams before attesting `stopped`.
Never share a borrower credential file among concurrent clients.
Task/project labels provide attribution and are not authentication.
Read-only device/history APIs are visible to the OS user; mutation requires borrower or operator credentials.

## Install and validate without devices

From this folder:

```sh
python3 -m pip install .
# Installation is optional for source execution:
python3 -m device_library --help
python3 -m unittest discover -s tests -v
```

Tests use temporary directories, FakeAdapter or injected command runners, and owned fake coordinator subprocesses that they clean up.
They never run adb/android or enumerate real devices.
The socket suite includes a complete independent CLI demonstration: register, borrow, renew, return, restart, consistent backup and offline restore.
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

## Production operator setup

The following commands require authorization to discover and operate devices:

```sh
device-library serve --init
# Later starts omit --init; run in a supervised foreground terminal/service.
```

`--init` creates the first ledger only.
A missing ledger/manifest or unsupported schema fails closed; deleting a DB never proves devices are free.
Every service start creates a new epoch, revokes outstanding leases, cancels unallocated queued requests and quarantines outstanding devices.
It never replays old Android actions.
Same-lease continuation after restart is not implemented: stop and return/reconcile, then borrow again.
A pending old return can receive a new handoff, but unknown old coordinator operations still need operator verification.

Register a physical device using a JSON spec file:

```json
{
  "hardware_serial": "VERIFIED_HARDWARE_SERIAL",
  "serials": ["VERIFIED_USB_SERIAL", "VERIFIED_WIFI_SERIAL"],
  "model": "Pixel 7",
  "lock_policy": "no-credential-wake",
  "capabilities": {"api": "35"}
}
```

Both connection aliases belong to one generated stable device ID.
Discovery checks `ro.serialno` and `ro.product.model`; model names never merge identities.
For an emulator, register an existing AVD (this tool never creates one).
The launch name is the `.ini` descriptor basename in the AVD root, and its `path=` must resolve to the registered storage directory.
The directory basename may differ from the launch name; registration, start and ongoing observation reject a missing or mismatched descriptor.

```json
{
  "avd_name": "Pixel_API35",
  "avd_path": "/absolute/avd-root/Pixel_API35.avd",
  "avd_identity": "DEVICE:INODE:CONFIG_SHA256",
  "sdk_root": "/absolute/android-sdk",
  "capabilities": {"api": "35"}
}
```

Compute the storage identity without accessing a device:

```sh
python3 -c 'from pathlib import Path; from device_library.adapter import avd_identity; print(avd_identity(Path("/absolute/avd-root/Pixel_API35.avd")))'
device-library --credential-file ~/.device-library-runtime/operator.credential \
  devices register --kind emulator --alias Pixel_API35 --spec-file avd.json
# Physical registration uses --kind physical and its corresponding spec.
```

For obsolete registration metadata only:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential \
  devices retire DEVICE_ID --reason 'Verified storage recreated and old user stopped' \
  --old-client-stopped
```

AVD storage identity binds canonical path, filesystem device/inode and config.ini digest.
A recreated or changed AVD blocks observation and requires operator investigation; automatic identity migration is unsupported.
After verifying old direct actions stopped, retire the obsolete registration with the metadata-only command below, then register the new identity under a fresh device ID.
Retirement refuses any unclosed lease or unresolved coordinator operation and preserves all historical leases/bindings.
It never stops/deletes an AVD or frees a running instance slot; that instance remains foreign until transferred.
Retired registrations are excluded from identity checks and scheduling, including after restore.

Version 0.1.7 can permanently remove a retired registration from the ledger:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential \
  devices delete DEVICE_ID
```

Deletion requires the existing operator credential and refuses every non-`retired` state,
any unclosed lease, any linked claim, unresolved request targeting the device,
unresolved operation (including unknown outcomes), or unfinished handoff.
It also refuses positive online/running evidence from the ledger's observations,
including a replacement registration sharing the identity/storage and observed foreign emulator instances.
It never refreshes discovery, calls adb, inspects AVD storage, stops a device or deletes device files.
Retired observations are retained rather than refreshed; a retained online observation therefore
conservatively blocks deletion. This metadata command does not certify that a device is offline.
Use the normal operator investigation process for conflicting observations; deletion has no force flag.

Success returns `result: {"state":"deleted","id":"DEVICE_ID"}` in the usual JSON envelope.
The registration disappears from `devices list` and the registration count; capacity claims
and replacement registrations are unchanged. Policy refusals exit 5, missing operator credentials
exit 4, and an unknown or already deleted ID returns `device_not_found` with exit 2.
Deletion and its `device_deleted` audit event commit in one transaction.
The event retains the full registration snapshot (including id, alias and spec) and operator UID/role,
and is available through `history --device DEVICE_ID`, `audit export` and `watch`,
using their existing 1000-event/`--after` pagination and 180-day retention policy.
Closed leases, requests, operations, handoffs and bindings retain their history;
closed lease device IDs are archived in ledger metadata before removing the live foreign-key reference,
so `lease show` and `request show` continue reporting the original device ID.
This archive follows the closed chain's existing retention policy and is included in backups.

Duplicate AVD names across SDK/AVD roots are unsupported and block discovery.
Android CLI starts inherit explicit ANDROID_AVD_HOME, ANDROID_HOME and ANDROID_SDK_ROOT for the registered paths.
Inventory must match adb transports with host emulator processes.
A registered running AVD additionally needs executable SDK path and inherited AVD-root evidence from `ps eww`.
Unknown process/layout formats block starts rather than assuming spare capacity.
Foreign instances count against capacity and are never stopped automatically.
An already-running registration, or externally started registered AVD, remains quarantined until explicit operator transfer.

## Independent borrower example

Global options such as `--credential-file` precede the command.
Choose a stable session ID, request key and private credential file:

```sh
device-library --credential-file ./my-loan.credential borrow \
  --kind emulator --requires api=35 --borrower alice --session run-42 \
  --task checkout-test --request-key checkout-device --wait 60
```

The CLI creates its credential file before contacting the service and never prints the token.
The ledger stores credential hashes only.
Retry the identical borrower/session/key/body with that file to retrieve the original request, including after a lost socket response.
Changing the body conflicts; another credential cannot retrieve or cancel the request.
Use a fresh key for a new loan after closure/cancellation.

A result is queued or active with request/lease/device IDs.
Only an active current-epoch lease supplies an operation selector.
Do not operate the device until the lease is active.
Public expiry fields are `deadline_utc` and `remaining_seconds`; `renewal_required=false` replaces the old heartbeat recommendation.
Lease `*_monotonic` and queue `deadline` values belong to the coordinator clock and are not calendar timestamps.
Request `created` and event `at` are Unix UTC seconds, and events also provide `at_utc`.
`--wait` accepts seconds and polls client-side; timeout leaves the request persisted and returns its ID.
Slow preparation does not block borrow, status, renewal or cancellation RPCs.

Use sim-use or adb directly with the explicit selector according to those tools' own instructions.
No periodic renewal is required.
Use explicit renewal only to extend a still-active lease when needed:

```sh
device-library --credential-file ./my-loan.credential lease renew LEASE_ID
device-library --credential-file ./my-loan.credential request show REQUEST_ID
device-library --credential-file ./my-loan.credential request cancel REQUEST_ID
```

Lease TTL is 30 minutes from activation, with a hard two-hour continuous maximum across optional explicit renewals.
The coordinator does not extend the deadline automatically.
CLI loans bind to the invoking parent process by default; `borrow --owner-pid PID` selects the actual long-lived borrower when an agent or wrapper uses a transient command shell.
Do not bind to the short-lived `device-library borrow` command itself, a throwaway shell, or an unrelated persistent process.
The selected process must belong to the coordinator's OS user.
The server captures its kernel birth identity (macOS start time including microseconds; Linux boot ID and process start ticks), so PID reuse counts as owner exit.
The recorded `owner_process` is immutable for an idempotent request; retrying the same key never transfers ownership.
A live but stalled borrower remains leased until its deadline.

Maintenance checks the owner each cycle (normally every second, delayed by device operations).
A confirmed owner exit or zombie cancels an unallocated request.
For an active emulator loan, it starts a return that always stops the emulator, even with queued borrowers, and requires complete inventory proving it offline before closure and capacity reuse.
Owner exit does not prove child processes or detached direct-action streams stopped; physical devices therefore stay quarantined with reason `borrower_process_exited` until explicit stopped return or operator reconciliation.
Unreadable process information quarantines an active loan with reason `borrower_process_unknown`; it never implies exit or release.
An unverifiable or already-exited owner is refused before a new CLI request is persisted.
Direct Python `Library.borrow` calls default to the calling process as owner.
Pre-upgrade records without an owner advertise `automatic_return=unavailable_without_owner` and cannot be automatically released.
The socket borrow API requires `owner_pid`, including clients that bypass the CLI.
Normal explicit return and stopped handoff acknowledgement remain available regardless of owner monitoring.

At expiry the coordinator fences the lease and marks its request expired.
For an emulator it performs one targeted stop, then requires a complete inventory proving both an offline transport and no remaining emulator process before closing the lease with reason `lease_expired`, releasing capacity and making the device available.
An already exited emulator needs the same complete observation but no redundant stop command.
An abandoned return/cancel handoff becomes eligible only at the original lease deadline, before any return action has begun.
Successful commands with delayed process exit remain quarantined with occupancy retained; maintenance only repeats observation, never the stop command.
Stop failure, unproven identity/inventory or conflicting coordinator work retains quarantine and requires operator investigation.
Other revocations, including restart, clock uncertainty and connection changes, remain subject to explicit operator reconciliation.
Physical-device expiry never stops or releases the device automatically: the expired lease retains occupancy and the device stays quarantined until borrower return or operator reconciliation.
`lease show`, `request show` and `devices list` expose an `attention` record with the borrower, session, task and pending action; `history` and `watch --follow` deliver the durable `lease_expiry_notice` to borrowers and operators.
Consumers must poll those APIs or follow the event stream for reminders; this release does not send desktop or external chat notifications.
Failed renewal responses also include a stop-action reminder and the current attention record after authenticating the borrower.
Reclaim completion/failure events retain task attribution and proof or failure details.
Queue discipline is FIFO among earliest satisfiable requests; an impossible constraint does not block later compatible requests.
A request borrows exactly one device and explicitly chooses physical/emulator/any.
The three-emulator cap includes starting, running, stopping/uncertain and foreign claims.
Unknown inventory blocks new loans; external lifecycle tools can still violate this cooperative cap.

## Recovery and return

Request recovery, pause direct operations, then acknowledge the exact handoff with the same credential:

```sh
device-library --credential-file ./my-loan.credential recover LEASE_ID --kind wake
# Or --kind close-system-anr for emulators.
device-library --credential-file ./my-loan.credential handoff acknowledge HANDOFF_ID --state paused
```

Continue only after `resume_permitted: true` in the current epoch and valid lease.
Replayed completed acknowledgements are historical and do not grant fresh resume permission.
Duplicate in-progress acknowledgements do not execute a second action.
Three recoveries in five minutes are permitted; the next request quarantines the device.
Return/expiry during recovery suppresses resume and retains occupancy until old operations are verified ended.
ANR recovery marks an interrupted interval; borrowers decide which tests to rerun.

Pixel 7 recovery uses WAKEUP and dismiss-keyguard.
It then reobserves keyguard policy for at most two seconds and 21 reads, sleeping up to 100 ms between observations and stopping as soon as explicit unlock is verified.
Each built-in adb policy read is limited to the remaining observation budget; timeout or missing evidence still fails closed.
Verification requires explicit `isStatusBarKeyguard`, `mShowingLockscreen`, `mKeyguardShowing` or `isKeyguardShowing=false` evidence without contradictory true/secure fields.
When those legacy fields are absent, Android 15 requires both `inputRestricted=false` and `dreaming=false`, with no contradictory true/secure fields.
Generic `showing=false` is insufficient, and unexpected credential challenges fail closed.
Emulator recovery accepts Android CLI JSON elements with package/packageName=`android`, resourceId=`android:id/aerr_close`, enabled=true and integer bounds.
Bounds may be a left/top/right/bottom object or four-item array.
It additionally requires system-process/System UI ANR text and a system ANR window from dumpsys.
It clicks the observed bounds once and verifies the dialog cleared and device health responds.
Unrecognized/localized formats fail closed; app-owned Close buttons and app ANRs are not clicked.
Initial preparation can perform the same narrow recovery before granting the borrower, recording results in its preparation operation.
No screenshots are collected by default.

After stopping all direct commands, streams and tests:

```sh
device-library --credential-file ./my-loan.credential return LEASE_ID --reason done --stopped --wait 60
```

`--stopped` explicitly attests that all direct actions and streams have stopped, then acknowledges the return handoff.
Without it, `return` requests the handoff only, exits 3, and includes the required `handoff acknowledge HANDOFF_ID --state stopped` next action.
For an ordinary return before expiry, an offline emulator alone never substitutes for borrower acknowledgement.
Stopped acknowledgement is cooperative attestation, not OS-enforced proof.
A compatible waiter receives the emulator after health verification; without a waiter, Android CLI stops it and observation must prove it ended before releasing capacity.
A successful stop command may precede actual process exit; an acknowledged return remains pending and retains capacity until a complete inventory proves exit.
Maintenance completes that confirmation automatically without issuing another stop, including after a transiently unknown inventory.
A stop command failure remains unknown/quarantined and requires operator investigation; it is never retried automatically.
Physical devices remain connected.
Data is retained: there is no wipe, reset, reboot, uninstall or guessed-process termination.
The baseline is `retained; borrower changes unobserved`, never a certified clean fixture.
Failures return pending/quarantined while preserving occupancy and uncertain claims.

For abandoned/restored ownership, first verify all old direct actions and coordinator subprocesses have ended, then explicitly attest:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential reconcile \
  --device DEVICE_ID --reason 'Verified old users and owned operations stopped' \
  --old-client-stopped --old-operations-ended
```

A reason alone observes; it does not transfer or release quarantine.
An operation currently running in this coordinator cannot be attested away.
Unknown old operations require the separate old-operations-ended attestation.
These flags neither kill processes nor establish evidence automatically; the operator must possess that evidence.
There is no global adb-server kill, fuzzy PID kill or destructive reset path.

## Inspection and persistence

```sh
device-library doctor
device-library devices list
device-library operation show OPERATION_ID
device-library history --device DEVICE_ID
device-library history --lease LEASE_ID
device-library history --task checkout-test
device-library watch --after 123 --follow
device-library audit export --after 123
device-library --credential-file ~/.device-library-runtime/operator.credential backup /private/path/library.sqlite3
device-library --credential-file ~/.device-library-runtime/operator.credential retention
```

Replies are JSON with schema_version, ok and library_id/epoch when available.
History/audit/watch return at most 1000 ordered events; paginate with the last seq as `--after`.
Audit export is a JSON envelope, not raw JSONL.
Events carry UTC time and policy, and state transitions share their SQLite transaction.
Unchanged inventory observations are coalesced.
Identity binding history and operation results preserve routing/recovery facts without full screen or logcat payloads.
Ordinary borrower actions remain unobserved.

Backup uses SQLite's consistent backup API, mode 0600, and refuses overwrite.
Restore runs offline under the singleton lock and requires the operator credential:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential shutdown
device-library --credential-file ~/.device-library-runtime/operator.credential restore \
  /private/path/library.sqlite3 --data-dir ~/.local/share/device-library
device-library serve
```

Restore validates schema, integrity and library ID and preserves the old DB bytes, including corrupt targets and old journal/WAL sidecars.
Old sidecars are removed from the active DB basename before replacing it with the verified backup.
An interrupted restore marker blocks serving until offline restore finishes.
Every restored device is quarantined, even when no loan exists in the snapshot.
Stale queued requests are cancelled and the next serve creates a new epoch.
Absence of a newer loan from an older snapshot does not prove its user stopped, so operator attestation remains mandatory.
Old DBs contain private metadata and are retained for recovery.
Only schema 1 is supported; automatic migration is not implemented.

Metadata retention is 180 days and attachment retention is seven days.
Explicit `retention` removes complete closed-lease chains using UTC closure time, expired unallocated requests, old unlinked events and old binding history.
Live/unknown ownership is retained and a deletion marker records the policy action.
No recovery attachments are generated by default.
Backups and pre-restore snapshots are not automatically deleted.
Schedule retention operationally at the desired cadence; it is explicit in this release.
This is application-level auditing, not tamper-proof evidence against the same OS user.

Exit codes are 0 success; 2 invalid input/protocol/conflict; 3 queued/preparing/return pending/wait elapsed; 4 unauthorized or expired/fenced; 5 quarantine/policy failure; 6 service/persistence/singleton failure; 7 disconnected result unknown.
A transport failure may have committed a request: retry the same key and credential.
Doctor reports freshness/support without silently repairing devices.
A database write failure fail-stops the daemon, preventing further coordinator work without durable audit.

## Implementation and remaining integration acceptance

Short BEGIN IMMEDIATE transactions and unique request/device-occupancy constraints arbitrate lending.
The in-process state mutex is released around external commands; current operation, reservation and fence are rechecked before completion.
The scheduler prepares one device at a time without restricting the three concurrently running loans.
Discovery snapshots are discarded if relevant device/claim/operation topology changed during observation.
Timeout/unknown outcomes preserve occupancy and quarantine instead of replaying an action.

Other Android CLI/process formats, locked Pixel 7 behavior, localized system ANR evidence, host resources for three emulators and suspend behavior need separate live acceptance.
Process discovery selects emulator executables before reading their AVD arguments; words inside unrelated command arguments do not count as emulator processes.
Layout JSON is read from Android CLI's `--output` file because stdout can also contain instrumentation installation messages.
`--no-idle` requests the current full tree without requiring a quiet UI; Android CLI 1.0.16261425 can exit successfully without writing output when continuous layout events prevent idle.
After a missing output, layout is reobserved at most twice, one second apart, using fresh output paths to tolerate startup readiness without accepting stale files.
This snapshot still requires a readable file and valid JSON; unreadable or invalid output fails immediately, and three missing outputs fail preparation.
It does not certify UI stability.
Missing or invalid layout files fail preparation and retain quarantine/occupancy.
The process proof deliberately fails on formats it cannot establish; fake tests do not imply live compatibility.
Multi-user/multi-host enforcement, malicious-client isolation, automatic clean baselines, external-process takeover, OS-service installation and same-lease restart continuation are outside this release.
