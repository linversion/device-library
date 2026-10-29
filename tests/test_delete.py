"""Offline deletion policy, archival integrity and transaction checks."""
import contextlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from device_library.adapter import FakeAdapter
from device_library.core import Fault, Library


class DeleteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fixture = self.root / 'fixture.json'
        self.fixture.write_text('{}')
        self.adapter = FakeAdapter(self.fixture)
        self.lib = Library(self.root / 'data', self.adapter, initialize=True)
        self.addCleanup(lambda: self.lib.db.close())
        self.token = 'test-secret-' * 4
        self.device = self.lib.register('emulator', 'old', {
            'avd_name': 'old', 'avd_path': str(self.root / 'old.avd'),
            'avd_identity': 'identity', 'sdk_root': 'fixture'})

    def retire(self):
        return self.lib.retire(self.device['id'], 'obsolete', stopped=True)

    def refuse(self, reason):
        before = self.lib.db.total_changes
        with self.assertRaises(Fault) as caught:
            self.lib.delete(self.device['id'])
        self.assertEqual(caught.exception.reason, reason)
        self.assertEqual(caught.exception.code, 5)
        self.assertEqual(self.lib.db.total_changes, before)
        self.assertEqual(len(self.lib.devices()), 1)

    def closed_loan(self):
        request = self.lib.borrow('b', 's', 'key', {'kind': 'emulator'}, self.token)
        self.lib.tick()
        lease = self.lib.request(request['id'])['lease']
        handoff = self.lib.return_lease(lease['id'], self.token, 'done')
        self.lib.acknowledge(handoff['id'], self.token, 'stopped')
        return request['id'], lease['id'], handoff['id']

    def test_success_removes_inventory_preserves_storage_and_audit(self):
        storage = Path(self.device['spec']['avd_path'])
        storage.mkdir()
        sentinel = storage / 'config.ini'
        sentinel.write_text('untouched')
        snapshot = self.retire()
        after = self.lib.history()[-1]['seq']
        # A deletion must not even attempt observation or any adapter access.
        class NoDeviceAccess:
            def __getattribute__(self, name):
                raise AssertionError('device access: ' + name)
        self.lib.adapter = NoDeviceAccess()
        with patch.object(self.lib, 'external', side_effect=AssertionError('external action')):
            result = self.lib.delete(self.device['id'])
        self.assertEqual(result, {'state': 'deleted', 'id': self.device['id']})
        self.assertEqual(self.lib.devices(), [])
        self.assertEqual(self.lib.db.execute('SELECT count(*) FROM devices').fetchone()[0], 0)
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertEqual(sentinel.read_text(), 'untouched')
        event, = self.lib.history(after=after, device=self.device['id'])
        self.assertEqual(event['type'], 'device_deleted')
        self.assertEqual(event['payload']['registration'], snapshot)
        self.assertEqual(event['payload']['actor'], {'uid': os.getuid(), 'role': 'operator'})
        self.assertIn('at_utc', event)
        self.assertEqual(self.lib.history(after=event['seq']), [])
        persisted = self.lib.db.execute("SELECT payload FROM events WHERE type='device_deleted'").fetchone()
        self.assertEqual(json.loads(persisted[0])['registration']['spec'], self.device['spec'])

    def test_all_nonretired_states_refused(self):
        for state in ('available', 'offline', 'loaned', 'preparing', 'returning', 'quarantined', 'unexpected'):
            with self.subTest(state=state):
                self.lib.db.execute('UPDATE devices SET state=?', (state,))
                self.refuse('deletion_requires_retired')

    def test_all_unclosed_lease_states_refused(self):
        r = self.lib.borrow('b', 's', 'key', {'kind': 'emulator'}, self.token)
        self.lib.tick()
        lid = self.lib.request(r['id'])['lease']['id']
        self.lib.db.execute("UPDATE devices SET state='retired'")
        for state in ('reserved', 'active', 'returning', 'revoked', 'expired', None):
            with self.subTest(state=state):
                self.lib.db.execute('UPDATE leases SET state=? WHERE id=?', (state, lid))
                self.refuse('deletion_refused_unclosed_lease')

    def test_claims_refused(self):
        self.retire()
        for state in ('reserved', 'live', 'foreign', 'uncertain'):
            with self.subTest(state=state):
                self.lib.db.execute('INSERT OR REPLACE INTO claims VALUES(?,?,?,NULL)', ('key', self.device['id'], state))
                self.refuse('deletion_refused_claim')

    def test_pending_requests_refused_and_terminal_requests_preserved(self):
        self.retire()
        r = self.lib.borrow('b', 's', 'key', {'kind': 'emulator', 'device': self.device['id']}, self.token)
        for state in ('queued', 'preparing', 'active', 'unexpected'):
            with self.subTest(state=state):
                self.lib.db.execute('UPDATE requests SET state=? WHERE id=?', (state, r['id']))
                self.refuse('deletion_refused_pending_request')
        self.lib.cancel(r['id'], self.token)
        self.lib.db.execute("UPDATE requests SET state='cancelled'")
        self.lib.delete(self.device['id'])
        self.assertEqual(self.lib.request(r['id'])['state'], 'cancelled')

    def test_online_registration_refused(self):
        self.lib.db.execute('UPDATE devices SET observation=?', (json.dumps({'online': True}),))
        self.retire()
        self.refuse('deletion_refused_observed_online')

    def test_online_replacement_with_same_storage_refused(self):
        self.retire()
        replacement = self.lib.register('emulator', 'renamed', {**self.device['spec'], 'avd_name': 'renamed'})
        self.lib.db.execute('UPDATE devices SET observation=? WHERE id=?', (json.dumps({'online': True}), replacement['id']))
        with self.assertRaisesRegex(Fault, 'deletion_refused_observed_online'):
            self.lib.delete(self.device['id'])
        self.assertEqual(len(self.lib.devices()), 2)

    def test_offline_replacement_keeps_inventory_and_capacity(self):
        self.retire()
        replacement = self.lib.register('emulator', 'renamed', {**self.device['spec'], 'avd_name': 'renamed'})
        self.assertEqual(len(self.lib.devices()), 2)
        claims = [tuple(r) for r in self.lib.db.execute('SELECT * FROM claims')]
        self.lib.delete(self.device['id'])
        self.assertEqual(self.lib.devices(), [{**replacement, 'attention': None}])
        self.assertEqual([tuple(r) for r in self.lib.db.execute('SELECT * FROM claims')], claims)

    def test_physical_online_evidence_and_offline_deletion(self):
        self.retire()
        self.lib.delete(self.device['id'])
        self.device = self.lib.register('physical', 'phone', {
            'hardware_serial': 'hardware', 'model': 'Pixel 7', 'serials': ['usb', 'wifi']})
        self.retire()
        self.lib.db.execute('UPDATE devices SET observation=?', (json.dumps({'online': True}),))
        self.refuse('deletion_refused_observed_online')
        self.lib.db.execute('UPDATE devices SET observation=?', (json.dumps({'online': False}),))
        self.lib.delete(self.device['id'])
        self.assertEqual(self.lib.devices(), [])

    def test_foreign_running_instance_refused_without_inventory_change(self):
        self.retire()
        self.fixture.write_text(json.dumps({'instances': [{'key': 'emulator-5554', 'avd': 'old'}]}))
        self.lib.reconcile()
        claims = [tuple(r) for r in self.lib.db.execute('SELECT * FROM claims')]
        self.refuse('deletion_refused_observed_online')
        self.assertEqual([tuple(r) for r in self.lib.db.execute('SELECT * FROM claims')], claims)

    def test_unresolved_operations_refused(self):
        self.retire()
        with self.lib.tx():
            op = self.lib._operation(self.lib.device(self.device['id']), None, 'prepare')
        for state in ('prepared', 'running', 'waiting', 'unknown', 'unexpected', None):
            with self.subTest(state=state):
                self.lib.db.execute('UPDATE operations SET state=? WHERE id=?', (state, op))
                self.refuse('deletion_refused_unresolved_operation')

    def test_pending_handoff_refused_even_with_completed_operation(self):
        _, lid, hid = self.closed_loan()
        self.retire()
        self.lib.db.execute("UPDATE handoffs SET state='awaiting_stop' WHERE id=?", (hid,))
        self.refuse('deletion_refused_pending_handoff')
        self.assertEqual(self.lib.lease(lid)['state'], 'closed')

    def test_closed_history_survives_deletion_restart_backup_and_retention(self):
        rid, lid, hid = self.closed_loan()
        self.retire()
        before = self.lib.lease(lid)
        bindings = [tuple(r) for r in self.lib.db.execute('SELECT * FROM bindings')]
        self.lib.delete(self.device['id'])
        self.assertEqual(self.lib.lease(lid), before)
        self.assertEqual(self.lib.request(rid)['lease'], before)
        self.assertFalse(self.lib.acknowledge(hid, self.token, 'stopped')['resume_permitted'])
        self.assertEqual(self.lib.return_lease(lid, self.token, 'again'), before)
        self.assertEqual([tuple(r) for r in self.lib.db.execute('SELECT * FROM bindings')], bindings)
        self.assertEqual(self.lib.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        backup = self.root / 'backup.sqlite3'
        self.lib.backup(backup)
        with contextlib.closing(sqlite3.connect(backup)) as db:
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])
            self.assertEqual(db.execute('SELECT count(*) FROM devices').fetchone()[0], 0)
        self.lib.db.close()
        self.lib = Library(self.root / 'data', self.adapter)
        self.assertEqual(self.lib.lease(lid)['device'], self.device['id'])
        self.assertEqual(self.lib.devices(), [])
        with patch.object(self.lib, 'wall', return_value=self.lib.wall() + 181 * 86400):
            self.assertEqual(self.lib.retention()['closed_chains'], 1)
        self.assertIsNone(self.lib.db.execute('SELECT value FROM meta WHERE key=?', ('lease_device:' + lid,)).fetchone())

    def test_repeat_delete_returns_not_found_without_duplicate_event(self):
        self.retire()
        self.lib.delete(self.device['id'])
        with self.assertRaises(Fault) as caught:
            self.lib.delete(self.device['id'])
        self.assertEqual((caught.exception.reason, caught.exception.code), ('device_not_found', 2))
        self.assertEqual(self.lib.db.execute("SELECT count(*) FROM events WHERE type='device_deleted'").fetchone()[0], 1)

    def test_audit_failure_rolls_back_deletion_and_closed_lease_archival(self):
        _, lid, _ = self.closed_loan()
        self.retire()
        self.lib.db.execute("""CREATE TRIGGER fail_delete_audit BEFORE INSERT ON events
            WHEN NEW.type='device_deleted' BEGIN SELECT RAISE(ABORT, 'test audit failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.lib.delete(self.device['id'])
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'retired')
        self.assertEqual(self.lib.db.execute('SELECT device FROM leases WHERE id=?', (lid,)).fetchone()[0], self.device['id'])
        self.assertIsNone(self.lib.db.execute('SELECT value FROM meta WHERE key=?', ('lease_device:' + lid,)).fetchone())

    def test_deletion_event_uses_existing_1000_event_pagination(self):
        self.retire()
        after = self.lib.history()[-1]['seq']
        with self.lib.tx():
            for _ in range(1000):
                self.lib.event('fixture', device=self.device['id'])
        self.lib.delete(self.device['id'])
        page = self.lib.history(after=after, device=self.device['id'])
        self.assertEqual(len(page), 1000)
        last, = self.lib.history(after=page[-1]['seq'], device=self.device['id'])
        self.assertEqual(last['type'], 'device_deleted')
