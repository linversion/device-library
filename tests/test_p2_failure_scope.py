"""P2 regressions: two-level observation trust and borrower self-service lookup.

Covers astra B6 (first half) and k3pro P2 item 2: a single corrupted
registration or an unknown manual emulator process must not stop scheduling
of healthy unrelated devices, and a borrower can retrieve its own
request/lease by borrower/session/request-key with its own credential.
"""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from device_library import cli
from device_library.adapter import AndroidAdapter, FakeAdapter
from device_library.core import Library, Fault
from device_library.server import Server


class FailureScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fixture = self.root / 'fixture.json'
        self.fixture.write_text('{}')
        self.adapter = FakeAdapter(self.fixture)
        self.mono, self.wall = 1000., 1_800_000_000.
        self.lib = Library(self.root / 'data', self.adapter, initialize=True,
                           clock=lambda: self.mono, wall=lambda: self.wall)
        self.token = 'test-secret-' * 4

    def tearDown(self):
        self.lib.db.close()
        self.temp.cleanup()

    def emulator(self, name='AVD', **caps):
        return self.lib.register('emulator', name, {'avd_name': name, 'avd_path': name,
                'avd_identity': name, 'sdk_root': 'fixture', 'capabilities': caps})

    def physical(self):
        d = self.lib.register('physical', 'Pixel 7', {
            'hardware_serial': 'pixel', 'model': 'Pixel 7',
            'lock_policy': 'no-credential-wake', 'serials': ['pixel']})
        f = self.adapter.read()
        f.setdefault('devices', {})[d['id']] = {'online': True, 'serial': 'pixel',
                                                'transport': '1', 'identity': 'pixel'}
        self.fixture.write_text(json.dumps(f))
        self.lib.reconcile()
        return d

    def borrow(self, key='k', kind='emulator', **body):
        r = self.lib.borrow('borrower', 'session', key, {'kind': kind, **body}, self.token)
        self.lib.tick()
        return self.lib.request(r['id'])

    def break_device(self, device, reason='AVD storage identity changed'):
        f = self.adapter.read()
        f.setdefault('device_errors', {})[device['id']] = reason
        self.fixture.write_text(json.dumps(f))

    def set_capacity(self, trusted):
        f = self.adapter.read()
        f['capacity_trusted'] = trusted
        self.fixture.write_text(json.dumps(f))

    def test_corrupted_registration_isolates_only_that_device(self):
        good = self.emulator('GOOD')
        bad = self.emulator('BAD')
        phone = self.physical()
        self.break_device(bad)
        result = self.lib.reconcile()
        self.assertTrue(result['complete'])
        self.assertTrue(result['capacity_trusted'])
        self.assertEqual(result['isolated_devices'], {bad['id']: 'AVD storage identity changed'})
        self.assertEqual(self.lib.device(bad['id'])['state'], 'quarantined')
        self.assertEqual(self.lib.device(good['id'])['state'], 'available')
        self.assertEqual(self.lib.device(phone['id'])['state'], 'available')
        events = [e for e in self.lib.history() if e['type'] == 'device_observation_untrusted']
        self.assertEqual([e['device'] for e in events], [bad['id']])
        # Healthy physical scheduling continues despite the broken registration.
        self.assertEqual(self.borrow('phone', kind='physical')['state'], 'active')
        # Capacity is still trusted, so emulator starts on healthy registrations continue.
        loan = self.borrow('emu', kind='emulator')
        self.assertEqual(loan['state'], 'active')
        self.assertEqual(loan['lease']['device'], good['id'])
        self.assertEqual(self.lib.device(bad['id'])['state'], 'quarantined')
        # Once the error clears, the device stays quarantined for the operator
        # but is no longer reported as an active observation failure.
        f = self.adapter.read()
        del f['device_errors'][bad['id']]
        self.fixture.write_text(json.dumps(f))
        self.lib.reconcile()
        self.assertEqual(self.lib.device(bad['id'])['state'], 'quarantined')
        self.assertEqual(self.lib._isolated_devices, {})

    def test_isolation_revokes_only_the_untrusted_devices_lease(self):
        phone = self.physical()
        avd = self.emulator()
        phone_loan = self.borrow('p', kind='physical')
        emu_loan = self.borrow('e', kind='emulator')
        self.assertEqual(phone_loan['state'], 'active')
        self.assertEqual(emu_loan['state'], 'active')
        self.break_device(phone, 'physical identity mismatch')
        self.lib.tick()
        lease = self.lib.lease(phone_loan['lease']['id'])
        self.assertEqual(lease['recorded_state'], 'revoked')
        self.assertEqual(lease['reason'], 'observation_untrusted')
        self.assertEqual(self.lib.device(phone['id'])['state'], 'quarantined')
        other = self.lib.lease(emu_loan['lease']['id'])
        self.assertEqual(other['state'], 'active')
        self.assertTrue(other['usable'])
        self.assertTrue(other['selector'])

    def test_unknown_manual_emulator_blocks_starts_but_not_physical_scheduling(self):
        avd = self.emulator()
        phone = self.physical()
        self.set_capacity(False)
        self.lib.tick()
        self.assertFalse(self.lib.complete)
        self.assertFalse(self.lib.capacity_trusted)
        self.assertIn('capacity', self.lib.last_observation_error)
        events = [e for e in self.lib.history() if e['type'] == 'observation_unknown']
        self.assertEqual(events[-1]['payload']['scope'], 'emulator_capacity')
        # Emulator loans are refused while global capacity is unknown.
        emu_request = self.borrow('e', kind='emulator')
        self.assertEqual(emu_request['state'], 'queued')
        self.assertFalse(self.adapter.calls)
        self.assertEqual(self.lib.device(avd['id'])['state'], 'available')
        # A healthy physical device is not dragged down with the emulator pool.
        self.assertEqual(self.borrow('p', kind='physical')['state'], 'active')
        # Capacity recovery resumes emulator scheduling.
        self.set_capacity(True)
        self.lib.tick()
        self.assertTrue(self.lib.complete)
        self.assertEqual(self.lib.request(emu_request['id'])['state'], 'active')

    def test_physical_prepare_survives_unrelated_capacity_failure(self):
        phone = self.physical()
        avd = self.emulator()
        self.set_capacity(False)
        loan = self.borrow('p', kind='physical')
        self.assertEqual(loan['state'], 'active')
        self.assertTrue(loan['selector']['online'])


class ManualEmulatorObservationTests(unittest.TestCase):
    """Real-adapter scope split: an unknown manual emulator degrades capacity,
    never the observation of an unrelated healthy physical device."""

    def test_unknown_manual_emulator_preserves_physical_observation(self):
        def runner(args):
            if args == ['adb', 'devices', '-l']:
                return 'List of devices attached\npixel\tdevice transport_id:1\n'
            if args == ['adb', '-s', 'pixel', 'shell', 'getprop', 'ro.serialno']:
                return 'pixel\n'
            if args == ['adb', '-s', 'pixel', 'shell', 'getprop', 'ro.product.model']:
                return 'Pixel 7\n'
            if args == ['ps', '-axo', 'pid=,comm=']:
                return '999 /sdk/emulator/emulator\n'
            return '/sdk/emulator/emulator -avd Stranger\n'
        phone = {'id': 'phone', 'kind': 'physical',
                 'spec': {'serials': ['pixel'], 'hardware_serial': 'pixel', 'model': 'Pixel 7'}}
        result = AndroidAdapter(runner=runner).observe([phone])
        self.assertFalse(result['capacity_trusted'])
        self.assertFalse(result['complete'])
        self.assertTrue(any('disagree' in e for e in result['capacity_errors']))
        self.assertEqual(result['device_errors'], {})
        self.assertTrue(result['devices']['phone']['online'])
        self.assertEqual(result['devices']['phone']['identity'], 'pixel')


class DoctorScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fixture = self.root / 'fixture.json'
        self.fixture.write_text('{}')
        self.server = Server(self.root / 'data', initialize=True,
                             test_runtime=str(self.root / 'runtime'), fixture=str(self.fixture))
        self.lib = self.server.library

    def tearDown(self):
        self.server.close()
        self.temp.cleanup()

    def doctor(self):
        return self.server._dispatch({'command': 'doctor', 'args': {}, 'credential': ''})

    def test_doctor_reports_two_level_trust(self):
        spec = {'avd_name': 'fixture', 'avd_path': 'fixture',
                'avd_identity': 'fixture', 'sdk_root': 'fixture'}
        d = self.lib.register('emulator', 'fixture', spec)
        out = self.doctor()
        self.assertTrue(out['inventory_complete'])
        self.assertTrue(out['emulator_capacity_trusted'])
        self.assertEqual(out['isolated_devices'], {})
        f = json.loads(self.fixture.read_text())
        f['device_errors'] = {d['id']: 'AVD storage identity changed'}
        self.fixture.write_text(json.dumps(f))
        self.lib.reconcile()
        out = self.doctor()
        self.assertTrue(out['inventory_complete'])
        self.assertTrue(out['emulator_capacity_trusted'])
        self.assertEqual(out['isolated_devices'], {d['id']: 'AVD storage identity changed'})
        f['capacity_trusted'] = False
        self.fixture.write_text(json.dumps(f))
        self.lib.reconcile()
        out = self.doctor()
        self.assertFalse(out['inventory_complete'])
        self.assertFalse(out['emulator_capacity_trusted'])
        self.assertIn('capacity', out['observation_reason'])

    def test_find_request_dispatch_uses_borrower_credential(self):
        from device_library.core import Fault
        spec = {'avd_name': 'fixture', 'avd_path': 'fixture',
                'avd_identity': 'fixture', 'sdk_root': 'fixture'}
        self.lib.register('emulator', 'fixture', spec)
        token = 'borrower-token-' * 4
        request = self.lib.borrow('b', 's', 'k', {'kind': 'emulator'}, token)
        found = self.server.dispatch({'command': 'find_request',
                                      'args': {'borrower': 'b', 'session': 's', 'key': 'k'},
                                      'credential': token})
        self.assertEqual(found['id'], request['id'])
        self.assertEqual(found['state'], 'queued')
        with self.assertRaisesRegex(Fault, 'unauthorized'):
            self.server.dispatch({'command': 'find_request',
                                  'args': {'borrower': 'b', 'session': 's', 'key': 'k'},
                                  'credential': 'other' * 8})


class FindRequestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fixture = self.root / 'fixture.json'
        self.fixture.write_text('{}')
        self.adapter = FakeAdapter(self.fixture)
        self.lib = Library(self.root / 'data', self.adapter, initialize=True)
        self.token = 'test-secret-' * 4

    def tearDown(self):
        self.lib.db.close()
        self.temp.cleanup()

    def emulator(self, name='AVD'):
        return self.lib.register('emulator', name, {'avd_name': name, 'avd_path': name,
                'avd_identity': name, 'sdk_root': 'fixture'})

    def borrow(self, key='k'):
        r = self.lib.borrow('borrower', 'session', key, {'kind': 'emulator'}, self.token)
        self.lib.tick()
        return self.lib.request(r['id'])

    def test_find_retrieves_active_and_closed_requests(self):
        self.emulator()
        a = self.borrow()
        self.assertEqual(a['state'], 'active')
        found = self.lib.find_request('borrower', 'session', 'k', self.token)
        self.assertEqual(found['id'], a['id'])
        self.assertEqual(found['lease']['id'], a['lease']['id'])
        self.assertTrue(found['selector'])
        h = self.lib.return_lease(a['lease']['id'], self.token, 'done')
        self.assertEqual(self.lib.acknowledge(h['id'], self.token, 'stopped')['state'], 'closed')
        found = self.lib.find_request('borrower', 'session', 'k', self.token)
        self.assertEqual(found['state'], 'closed')
        self.assertEqual(found['lease']['id'], a['lease']['id'])

    def test_find_queued_request_and_auth_boundary(self):
        for n in range(3):
            self.emulator(str(n))
        for n in range(3):
            self.borrow(str(n))
        queued = self.borrow('lost-key')
        self.assertEqual(queued['state'], 'queued')
        self.assertIsNone(queued['lease'])
        found = self.lib.find_request('borrower', 'session', 'lost-key', self.token)
        self.assertEqual(found['id'], queued['id'])
        self.assertEqual(found['state'], 'queued')
        with self.assertRaisesRegex(Fault, 'unauthorized'):
            self.lib.find_request('borrower', 'session', 'lost-key', 'other' * 8)
        with self.assertRaisesRegex(Fault, 'request_not_found'):
            self.lib.find_request('borrower', 'session', 'never-used', self.token)
        with self.assertRaisesRegex(Fault, 'request_not_found'):
            self.lib.find_request('someone-else', 'session', 'lost-key', self.token)
        with self.assertRaisesRegex(Fault, 'required'):
            self.lib.find_request('', 'session', 'lost-key', self.token)

    def test_cli_request_find_validation(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli.main(['request', 'find'])
        self.assertEqual(code, 2)
        self.assertIn('request_find_requires', out.getvalue())
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli.main(['request', 'show'])
        self.assertEqual(code, 2)
        self.assertIn('request_id_required', out.getvalue())


if __name__ == '__main__':
    unittest.main()
