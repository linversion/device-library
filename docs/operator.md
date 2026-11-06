# Operator guide

Service deployment, device registration, quarantine recovery and ledger administration.
All of this requires the operator credential file at `~/.device-library-runtime/operator.credential`.
Borrowers should read `docs/borrowing.md` instead.

## Runtime layout and trust boundary

The production socket and lock always live at `~/.device-library-runtime/`, independently of cwd, FM_HOME, XDG variables and `--data-dir`.
The selected data directory is pinned there; a different directory is refused rather than opening another pool.
Default data is `~/.local/share/device-library/`; use local storage, never network/cloud-synchronized storage.
Directories are private, and credentials, socket and DB are mode 0600.
The same OS user can access these files, so this provides cooperative arbitration rather than malicious-client isolation.
Read-only device/history APIs are visible to the OS user; mutation requires borrower or operator credentials.

## Running the service

```sh
device-library serve --init   # first run only: creates the first ledger
device-library serve          # later starts omit --init; supervised foreground terminal/service
```

Production `serve` explicitly opts into discovery and device commands; it is not a harmless demo command.
A missing ledger/manifest or unsupported schema fails closed; deleting a DB never proves devices are free.
Every service start creates a new epoch, revokes outstanding leases, cancels unallocated queued requests and quarantines outstanding devices.
It never replays old Android actions. Same-lease continuation after restart is not implemented: stop and return/reconcile, then borrow again.
A pending old return can receive a new handoff, but unknown old coordinator operations still need operator verification.
A database write failure fail-stops the daemon, preventing further coordinator work without durable audit.

## Registering devices

Physical device, via a JSON spec file:

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

Compute the storage identity without accessing a device, then register:

```sh
python3 -c 'from pathlib import Path; from device_library.adapter import avd_identity; print(avd_identity(Path("/absolute/avd-root/Pixel_API35.avd")))'
device-library --credential-file ~/.device-library-runtime/operator.credential \
  devices register --kind emulator --alias Pixel_API35 --spec-file avd.json
# Physical registration uses --kind physical and its corresponding spec.
```

Duplicate AVD names across SDK/AVD roots are unsupported and block discovery.
Android CLI starts inherit explicit ANDROID_AVD_HOME, ANDROID_HOME and ANDROID_SDK_ROOT for the registered paths.
Inventory must match adb transports with host emulator processes.
A registered running AVD additionally needs executable SDK path and inherited AVD-root evidence from `ps eww`.
Unknown process/layout formats block starts rather than assuming spare capacity.
Foreign instances count against capacity and are never stopped automatically.
An already-running registration, or externally started registered AVD, remains quarantined until explicit operator transfer.

## Retiring and deleting registrations

For obsolete registration metadata only:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential \
  devices retire DEVICE_ID --reason 'Verified storage recreated and old user stopped' \
  --old-client-stopped
```

AVD storage identity binds canonical path, filesystem device/inode and config.ini digest.
A recreated or changed AVD fails that device's observation: the registration is quarantined in isolation (healthy devices stay schedulable) and requires operator investigation; automatic identity migration is unsupported.
After verifying old direct actions stopped, retire the obsolete registration, then register the new identity under a fresh device ID.
Retirement refuses any unclosed lease or unresolved coordinator operation and preserves all historical leases/bindings.
It never stops/deletes an AVD or frees a running instance slot; that instance remains foreign until transferred.
Retired registrations are excluded from identity checks and scheduling, including after restore.

A retired registration can be permanently removed from the ledger:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential \
  devices delete DEVICE_ID
```

Deletion refuses every non-`retired` state, any unclosed lease, any linked claim, unresolved request targeting the device,
unresolved operation (including unknown outcomes), or unfinished handoff.
Operations in `completed`, `cancelled`, or `operator_verified_ended` allow deletion; all other states, including `failed`, block it.
It also refuses positive online/running evidence from the ledger's observations, including a replacement registration sharing the
identity/storage and observed foreign emulator instances. It never refreshes discovery, calls adb, inspects AVD storage, stops a
device or deletes device files, and it does not certify that a device is offline. There is no force flag.
Success returns `result: {"state":"deleted","id":"DEVICE_ID"}`; policy refusals exit 5, missing operator credentials exit 4, and
an unknown or already deleted ID returns `device_not_found` with exit 2. The `device_deleted` audit event retains the full
registration snapshot and is available through `history --device`, `audit export` and `watch`. Closed leases, requests,
operations, handoffs and bindings retain their history; closed lease device IDs are archived in ledger metadata so `lease show`
and `request show` keep reporting the original device ID.

## Reconcile: releasing quarantine

For abandoned/restored ownership, first verify all old direct actions and coordinator subprocesses have ended, then explicitly attest:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential reconcile \
  --device DEVICE_ID --reason 'Verified old users and owned operations stopped' \
  --old-client-stopped --old-operations-ended
```

A reason alone observes; it does not transfer or release quarantine.
An operation currently running in this coordinator cannot be attested away.
Unknown old operations require the separate `--old-operations-ended` attestation.
These flags neither kill processes nor establish evidence automatically; the operator must possess that evidence.
There is no global adb-server kill, fuzzy PID kill or destructive reset path.

When a failed return is retried after fencing, dead `prepared`/`waiting` operations from the old fence can remain.
A blocked return or reconcile reports `old_coordinator_operations_need_attestation` with a `blocking_operations` list naming each
operation ID, kind, state, epoch and fence, plus the remedy commands. `--old-operations-ended` sweeps every `unknown` operation
and every `prepared`/`waiting` operation fenced below the device's current fence; operations at the current fence and `running`
operations keep blocking.

To close one operation explicitly:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential \
  operation end OPERATION_ID --reason 'Borrower process exited; verified no coordinator subprocess remains'
```

A reason is mandatory and the event records the operator UID, previous state and attestation.
`unknown` and old-fence `prepared`/`waiting` operations are attestable directly; current-fence `prepared`/`waiting` and any
`running` operation additionally require `--old-client-stopped`, and `running` is refused without it.
Terminal operations (`completed`, `cancelled`, `failed`) are refused; repeating `end` on an already `operator_verified_ended`
operation is idempotent.

## Inspection

```sh
device-library doctor
device-library devices list
device-library operation show OPERATION_ID
device-library history --device DEVICE_ID        # also --lease / --task
device-library watch --after 123 --follow
device-library audit export --after 123
```

Replies are JSON with schema_version, ok and library_id/epoch when available.
History/audit/watch return at most 1000 ordered events; paginate with the last seq as `--after`.
Audit export is a JSON envelope, not raw JSONL. Unchanged inventory observations are coalesced.
`lease show`, `request show` and `devices list` expose an `attention` record with the borrower, session, task and pending action;
`history` and `watch --follow` deliver the durable `lease_expiry_notice`.
Doctor reports freshness/support without silently repairing devices, including the two-level observation trust:
`emulator_capacity_trusted` (global adb/process agreement; unknown capacity blocks emulator starts) and
`isolated_devices` (per-device observation failures quarantined one device at a time).

## Backup, restore, retention

Backup uses SQLite's consistent backup API, mode 0600, and refuses overwrite:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential backup /private/path/library.sqlite3
```

Restore runs offline under the singleton lock and requires the operator credential:

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential shutdown
device-library --credential-file ~/.device-library-runtime/operator.credential restore \
  /private/path/library.sqlite3 --data-dir ~/.local/share/device-library
device-library serve
```

Restore validates schema, integrity and library ID and preserves the old DB bytes, including corrupt targets and old journal/WAL sidecars.
An interrupted restore marker blocks serving until offline restore finishes.
Every restored device is quarantined, even when no loan exists in the snapshot; stale queued requests are cancelled and the next
serve creates a new epoch. Absence of a newer loan from an older snapshot does not prove its user stopped, so operator attestation
remains mandatory. Only schema 1 is supported; automatic migration is not implemented.

```sh
device-library --credential-file ~/.device-library-runtime/operator.credential retention
```

Metadata retention is 180 days and attachment retention is seven days.
Explicit `retention` removes complete closed-lease chains using UTC closure time, expired unallocated requests, old unlinked
events and old binding history. Live/unknown ownership is retained and a deletion marker records the policy action.
Backups and pre-restore snapshots are not automatically deleted. Schedule retention operationally; it is explicit in this release.
This is application-level auditing, not tamper-proof evidence against the same OS user.

## Exit codes

0 success · 2 invalid input/protocol/conflict · 3 queued/preparing/return pending/wait elapsed · 4 unauthorized or expired/fenced ·
5 quarantine/policy failure · 6 service/persistence/singleton failure · 7 disconnected result unknown.
A transport failure may have committed a request: retry the same key and credential.
