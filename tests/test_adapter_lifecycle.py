"""Coordinator acceptance through the real adapter and a fake command boundary."""
from pathlib import Path
import os
from unittest.mock import patch
import tempfile
import unittest

from device_library.adapter import AndroidAdapter, ObservationError, avd_identity
from device_library.core import Library, Fault


class AdapterLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.avd = self.root / 'Test.avd'
        self.avd.mkdir()
        (self.avd / 'config.ini').write_text('AvdId=Test\n')
        (self.root / 'Test.ini').write_text(f'path={self.avd}\n')
        self.running = False
        self.launch_name = 'Test'
        self.delayed_stop = False
        self.calls = []
        self.layout = '{"elements":[]}'
        self.layout_stdout = ''
        self.write_layout = True
        self.require_no_idle = False
        self.missing_layouts = 0
        self.lib = Library(self.root / 'ledger', AndroidAdapter(runner=self.run_command),
                           initialize=True)
        self.device = self.lib.register('emulator', 'Test', {
            'avd_name': 'Test', 'avd_path': str(self.avd),
            'avd_identity': avd_identity(self.avd), 'sdk_root': '/sdk'})
        self.token = 'test-token-' * 8

    def tearDown(self):
        self.lib.db.close()
        self.temp.cleanup()

    def run_command(self, args):
        self.calls.append(args)
        if args[0] == '/usr/bin/env':
            self.running = True
            return 'Emulator started successfully\n'
        if args == ['android', 'emulator', 'stop', 'emulator-5554']:
            if not self.delayed_stop:
                self.running = False
            return 'Emulator stopped\n'
        if args == ['adb', 'devices', '-l']:
            return 'List of devices attached\n' + (
                'emulator-5554 device transport_id:1\n' if self.running else '')
        if args == ['adb', '-s', 'emulator-5554', 'emu', 'avd', 'name']:
            return self.launch_name + '\nOK\n'
        if args == ['ps', '-axo', 'pid=,command=']:
            return '123 /sdk/emulator/emulator -avd Test' if self.running else ''
        if args == ['ps', '-axo', 'pid=,comm=']:
            return '123 /sdk/emulator/emulator' if self.running else ''
        if args == ['ps', 'eww', '-p', '123', '-o', 'command=']:
            return f'/sdk/emulator/emulator -avd {self.launch_name} ANDROID_AVD_HOME={self.root}'
        if args[:5] == ['android', 'layout', '--device', 'emulator-5554', '--full']:
            if '--output' in args:
                if self.missing_layouts:
                    self.missing_layouts -= 1
                elif self.write_layout and (not self.require_no_idle or '--no-idle' in args):
                    Path(args[args.index('--output') + 1]).write_text(self.layout)
                return self.layout_stdout
            return self.layout_stdout + self.layout
        if args == ['adb', '-s', 'emulator-5554', 'shell', 'getprop', 'sys.boot_completed']:
            return '1\n'
        if args == ['adb', '-s', 'emulator-5554', 'shell', 'dumpsys', 'window', 'windows']:
            return 'mCurrentFocus=Launcher\n'
        self.fail(f'unexpected device command: {args}')

    def borrow(self):
        request = self.lib.borrow('worker', 'session', 'key',
                                  {'kind': 'emulator'}, self.token)
        self.lib.tick()
        return self.lib.request(request['id'])

    def test_full_real_adapter_lifecycle_with_fake_commands(self):
        request = self.borrow()
        lease = request['lease']
        self.assertEqual(request['state'], 'active')
        self.assertEqual(lease['state'], 'active')
        self.assertEqual(request['selector']['serial'], 'emulator-5554')
        self.assertEqual(self.lib.count_claims(), 1)
        for command in ('start', 'layout', 'sys.boot_completed', 'dumpsys'):
            self.assertTrue(any(command in args for args in self.calls), command)
        self.assertEqual(self.lib.renew(lease['id'], self.token)['state'], 'active')
        handoff = self.lib.return_lease(lease['id'], self.token, 'done')
        self.assertEqual(self.lib.acknowledge(handoff['id'], self.token, 'stopped')['state'], 'closed')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'available')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertFalse(self.running)

    def test_owner_exit_waits_for_shutdown_and_unknown_inventory(self):
        r=self.lib.borrow('worker','session','owner',{'kind':'emulator'},self.token,owner_pid=os.getpid())
        self.lib.tick();lid=self.lib.request(r['id'])['lease']['id']
        self.delayed_stop=True
        with patch('device_library.process.state',return_value='exited'):self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'returning')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertNotEqual(self.lib.device(self.device['id'])['state'],'available')
        # Crossing TTL during an acknowledged owner-exit return must not start expiry reclaim.
        now,wall=self.lib.clock(),self.lib.wall()
        self.lib.clock=lambda:now+1801
        self.lib.wall=lambda:wall+1801
        self.lib.adapter.observe=lambda devices: (_ for _ in ()).throw(ObservationError('incomplete'))
        self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'returning')
        self.assertEqual(self.lib.count_claims(),1)
        self.lib.adapter.observe=AndroidAdapter(runner=self.run_command).observe
        self.running=False
        self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'closed')
        self.assertEqual(self.lib.count_claims(),0)
        self.assertEqual(sum(args[:3]==['android','emulator','stop'] for args in self.calls),1)

    def expire(self):
        now, wall = self.lib.clock(), self.lib.wall()
        self.lib.clock = lambda: now + 1801
        self.lib.wall = lambda: wall + 1801
        self.lib.tick()

    def test_expiry_waits_for_real_adapter_process_exit_then_releases_once(self):
        request = self.borrow()
        self.delayed_stop = True
        self.expire()
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'quarantined')
        self.assertEqual(self.lib.count_claims(), 1)
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'expired')
        with self.assertRaisesRegex(Fault, 'auto_reclaim_in_progress'):
            self.lib.return_lease(request['lease']['id'], self.token, 'racing return')
        with self.assertRaisesRegex(Fault, 'device_operation_in_progress'):
            self.lib.operator_reconcile(self.device['id'], 'racing reconcile', stopped=True)
        self.lib.tick()
        self.running = False
        self.lib.tick()
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'available')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertEqual(sum(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls), 1)

    def test_expiry_residual_process_or_unknown_inventory_retains_capacity(self):
        request = self.borrow()
        self.delayed_stop = True
        self.expire()
        original = self.lib.adapter.runner
        def residual(args):
            if args == ['adb', 'devices', '-l']:
                return 'List of devices attached\n'
            return original(args)
        self.lib.adapter.runner = residual
        self.lib.tick()
        self.assertFalse(self.lib.complete)
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'expired')
        self.assertEqual(self.lib.count_claims(), 1)
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'quarantined')
        self.assertIn('inventories disagree', self.lib.devices()[0]['attention']['reclaim']['result']['reason'])
        self.running = False
        self.lib.tick()
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')

    def test_expiry_stop_failure_is_not_success_even_after_process_exits(self):
        request = self.borrow()
        original = self.lib.adapter.runner
        def failed(args):
            if args[:3] == ['android', 'emulator', 'stop']:
                raise ObservationError('stop command failed')
            return original(args)
        self.lib.adapter.runner = failed
        self.expire()
        self.running = False
        self.lib.tick()
        self.assertNotEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'quarantined')
        self.assertEqual(self.lib.count_claims(), 1)

    def test_expiry_already_exited_emulator_still_requires_complete_observation(self):
        request = self.borrow()
        self.running = False
        self.expire()
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertFalse(any(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls))

    def test_expiry_unknown_before_stop_never_runs_lifecycle_command(self):
        request = self.borrow()
        original = self.lib.adapter.observe
        def unknown(devices):
            raise ObservationError('unknown inventory before expiry stop')
        self.lib.adapter.observe = unknown
        self.expire()
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'quarantined')
        self.assertEqual(self.lib.count_claims(), 1)
        self.lib.adapter.observe = original
        self.lib.tick()
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'expired')
        self.assertFalse(any(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls))

    def test_expiry_pending_confirmation_restart_does_not_replay_or_release(self):
        request = self.borrow()
        self.delayed_stop = True
        self.expire()
        adapter = self.lib.adapter
        self.lib.db.close()
        self.lib = Library(self.root / 'ledger', adapter)
        self.running = False
        self.lib.tick()
        self.assertNotEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'quarantined')
        self.assertEqual(sum(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls), 1)

    def test_delayed_shutdown_finishes_after_observed_exit_without_second_stop(self):
        request = self.borrow()
        self.delayed_stop = True
        handoff = self.lib.return_lease(request['lease']['id'], self.token, 'done')
        result = self.lib.acknowledge(handoff['id'], self.token, 'stopped')
        self.assertEqual(result['state'], 'returning')
        self.assertEqual(self.lib.count_claims(), 1)
        self.lib.tick()
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'returning')
        self.running = False
        self.lib.tick()
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'available')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertEqual(sum(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls), 1)

    def test_cancel_delayed_stop_finishes_automatically_and_releases_capacity(self):
        request = self.borrow()
        self.delayed_stop = True
        handoff = self.lib.cancel(request['id'], self.token)
        self.assertEqual(self.lib.lease(request['lease']['id'])['reason'], 'request_cancelled')
        self.lib.acknowledge(handoff['id'], self.token, 'stopped')
        self.lib.tick()
        self.assertEqual(self.lib.count_claims(), 1)
        self.running = False
        self.lib.tick()
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'available')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertEqual(sum(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls), 1)

    def test_pending_stop_unknown_inventory_never_releases_then_recovers(self):
        request = self.borrow()
        self.delayed_stop = True
        handoff = self.lib.return_lease(request['lease']['id'], self.token, 'done')
        self.lib.acknowledge(handoff['id'], self.token, 'stopped')
        original = self.lib.adapter.observe
        def unknown(devices):
            raise ObservationError('test process/transport disagreement')
        self.lib.adapter.observe = unknown
        self.running = False
        self.lib.tick()
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'returning')
        self.assertEqual(self.lib.count_claims(), 1)
        self.lib.adapter.observe = original
        self.lib.tick()
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertEqual(sum(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls), 1)

    def test_pending_stop_restart_fences_and_requires_operator_attestation(self):
        request = self.borrow()
        self.delayed_stop = True
        handoff = self.lib.return_lease(request['lease']['id'], self.token, 'done')
        self.lib.acknowledge(handoff['id'], self.token, 'stopped')
        adapter = self.lib.adapter
        self.lib.db.close()
        self.lib = Library(self.root / 'ledger', adapter)
        self.running = False
        self.lib.tick()
        self.assertNotEqual(self.lib.lease(request['lease']['id'])['state'], 'closed')
        op = self.lib.db.execute('SELECT state FROM operations WHERE id=?', (handoff['operation'],)).fetchone()
        self.assertEqual(op['state'], 'unknown')
        self.assertEqual(self.lib.operator_reconcile(self.device['id'], 'verified old operations ended',
                                                   stopped=True, operations_ended=True)['state'], 'closed')
        self.assertEqual(self.lib.retire(self.device['id'], 'retire after closure', stopped=True)['state'], 'retired')

    def test_registration_rejects_descriptor_pointing_to_other_storage(self):
        spec = self.device['spec']
        self.lib.retire(self.device['id'], 'replace fixture', stopped=True)
        other = self.root / 'Other.avd'
        other.mkdir()
        (self.root / 'Test.ini').write_text(f'path={other}\n')
        with self.assertRaisesRegex(Fault, 'does not point'):
            self.lib.register('emulator', 'wrong storage', spec)
        self.assertEqual(len(self.lib.devices()), 1)

    def test_offline_without_stopped_attestation_never_closes_return(self):
        request = self.borrow()
        self.lib.return_lease(request['lease']['id'], self.token, 'done')
        self.running = False
        self.lib.tick()
        self.assertEqual(self.lib.lease(request['lease']['id'])['state'], 'returning')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'returning')
        self.assertFalse(any(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls))

    def test_externally_exited_emulator_return_does_not_issue_stop(self):
        request = self.borrow()
        self.running = False
        self.lib.tick()
        handoff = self.lib.return_lease(request['lease']['id'], self.token, 'externally exited')
        result = self.lib.acknowledge(handoff['id'], self.token, 'stopped')
        self.assertEqual(result['state'], 'closed')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'available')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertFalse(any(args[:3] == ['android', 'emulator', 'stop'] for args in self.calls))

    def test_registration_rejects_directory_name_used_as_launch_name(self):
        spec = self.device['spec']
        self.lib.retire(self.device['id'], 'replace fixture', stopped=True)
        (self.root / 'Test.ini').rename(self.root / 'Actual_Launch_Name.ini')
        with self.assertRaisesRegex(Fault, 'AVD launch descriptor'):
            self.lib.register('emulator', 'wrong', spec)
        self.assertEqual(len(self.lib.devices()), 1)
        corrected = self.lib.register('emulator', 'correct', {**spec, 'avd_name': 'Actual_Launch_Name'})
        self.assertEqual(corrected['state'], 'available')
        self.assertEqual(corrected['spec']['avd_path'], str(self.avd))

    def test_different_launch_and_directory_names_complete_lifecycle(self):
        spec = self.device['spec']
        self.lib.retire(self.device['id'], 'replace fixture', stopped=True)
        self.launch_name = 'Medium_Phone_API_36.1'
        (self.root / 'Test.ini').rename(self.root / (self.launch_name + '.ini'))
        self.device = self.lib.register('emulator', 'different names', {
            **spec, 'avd_name': self.launch_name})
        request = self.borrow()
        self.assertEqual(request['state'], 'active')
        lease = request['lease']
        self.assertEqual(self.lib.renew(lease['id'], self.token)['state'], 'active')
        handoff = self.lib.return_lease(lease['id'], self.token, 'done')
        self.assertEqual(self.lib.acknowledge(handoff['id'], self.token, 'stopped')['state'], 'closed')
        self.assertFalse(self.running)
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'available')
        self.assertEqual(self.lib.count_claims(), 0)

    def test_observation_rechecks_descriptor_and_storage_identity(self):
        adapter = self.lib.adapter
        descriptor = self.root / 'Test.ini'
        original = descriptor.read_text()
        other = self.root / 'Other.avd'
        other.mkdir()
        (other / 'config.ini').write_text('AvdId=Other\n')
        for target in ('missing', str(other), str(self.root / 'Absent.avd')):
            with self.subTest(target=target):
                if target == 'missing':
                    descriptor.unlink()
                else:
                    descriptor.write_text(f'path={target}\n')
                try:
                    with self.assertRaises(ObservationError):
                        adapter.observe([self.device])
                finally:
                    descriptor.write_text(original)
        (self.avd / 'config.ini').write_text('AvdId=changed\n')
        with self.assertRaisesRegex(ObservationError, 'storage identity changed'):
            adapter.observe([self.device])

    def test_observation_fails_when_storage_or_fingerprint_unreadable(self):
        config = self.avd / 'config.ini'
        config.unlink()
        for remove_directory in (False, True):
            with self.subTest(remove_directory=remove_directory):
                if remove_directory:
                    self.avd.rmdir()
                with self.assertRaises((ObservationError, OSError)):
                    self.lib.adapter.observe([self.device])

    def test_malformed_layout_keeps_device_and_slot_quarantined(self):
        self.layout = 'layout service not ready\n'
        request = self.borrow()
        self.assertEqual(request['lease']['state'], 'revoked')
        self.assertEqual(request['lease']['reason'], 'operation_unknown')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'quarantined')
        self.assertEqual(self.lib.count_claims(), 1)
        failures = [e for e in self.lib.history() if e['type'] == 'operation_unknown']
        self.assertEqual(failures[0]['payload']['reason'], 'layout output file contains invalid JSON')
        self.assertEqual(sum(args[:2] == ['android', 'layout'] for args in self.calls), 1)

    def test_layout_install_banner_is_not_json_protocol(self):
        # Captured from Android CLI 1.0.16261425 on first layout after boot.
        self.layout_stdout = 'Installing layout instrumentation server...\n'
        request = self.borrow()
        self.assertEqual(request['lease']['state'], 'active')
        self.assertEqual(request['selector']['serial'], 'emulator-5554')

    def test_busy_layout_snapshot_still_grants_real_selector(self):
        # Android CLI 1.0.16261425 exits 0 without writing output if the UI
        # never idles (reproduced with the running system stopwatch).
        self.require_no_idle = True
        self.layout = '{"elements":[{"text":"Pause"}]}'
        request = self.borrow()
        self.assertEqual(request['state'], 'active')
        self.assertEqual(request['selector']['serial'], 'emulator-5554')
        self.assertEqual(self.lib.count_claims(), 1)

    def test_transient_missing_layout_reobserved_before_grant(self):
        self.missing_layouts = 2
        request = self.borrow()
        self.assertEqual(request['state'], 'active')
        paths = [args[-1] for args in self.calls if args[:2] == ['android', 'layout']]
        self.assertEqual(len(paths), 3)
        self.assertEqual(len(set(paths)), 3)

    def test_missing_output_file_never_falls_back_to_stdout(self):
        self.write_layout = False
        self.layout_stdout = '{"elements":[]}'
        request = self.borrow()
        self.assertEqual(request['lease']['state'], 'revoked')
        self.assertEqual(self.lib.device(self.device['id'])['state'], 'quarantined')
        self.assertEqual(self.lib.count_claims(), 1)
        self.assertEqual(sum(args[:2] == ['android', 'layout'] for args in self.calls), 3)


if __name__ == '__main__':
    unittest.main()
