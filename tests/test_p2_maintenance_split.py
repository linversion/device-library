"""P2 regressions: maintenance control loop split from bounded device I/O.

Covers astra B5 and k3pro O5: owner/expiry control must never wait on slow
device operations, device I/O runs on a per-device-serial executor whose
results are rechecked against epoch/fence/operation identity, doctor exposes
maintenance health, a dead maintenance loop refuses new borrows, and the
emulator capacity is validated host configuration instead of a hardcoded 3.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from device_library.adapter import FakeAdapter, ObservationError
from device_library.core import Library, Fault
from device_library.server import Server

ROOT = Path(__file__).resolve().parents[1]


class SplitTests(unittest.TestCase):
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

    def borrow(self, key='k', kind='emulator', **body):
        r = self.lib.borrow('borrower', 'session', key, {'kind': kind, **body}, self.token)
        self.lib.tick()
        return self.lib.request(r['id'])

    def close(self, lease):
        h = self.lib.return_lease(lease, self.token, 'done')
        return self.lib.acknowledge(h['id'], self.token, 'stopped')

    @staticmethod
    def drain(lib):
        while True:
            job = lib.next_maintenance_job(timeout=0)
            if job is None:
                return
            lib.run_maintenance_job(job)

    @staticmethod
    def worker(lib, ran):
        SplitTests.drain(lib)
        ran.set()

    def test_control_tick_alone_transitions_expiry_without_device_io(self):
        d = self.emulator()
        a = self.borrow()
        self.adapter.calls.clear()
        self.mono += 1801
        self.wall += 1801
        self.lib.control_tick()
        # Durable transition and declared intent happen without one adapter call.
        lease = self.lib.lease(a['lease']['id'])
        self.assertEqual(lease['recorded_state'], 'expired')
        self.assertEqual(self.lib.device(d['id'])['state'], 'quarantined')
        self.assertEqual(self.adapter.calls, [])
        op = self.lib.db.execute("SELECT state,kind FROM operations WHERE lease=? AND kind='auto-reclaim'",
                                 (a['lease']['id'],)).fetchone()
        self.assertEqual((op['kind'], op['state']), ('auto-reclaim', 'running'))
        # The executor performs the bounded I/O and finishes the reclaim.
        self.drain(self.lib)
        self.assertEqual(self.lib.lease(a['lease']['id'])['recorded_state'], 'closed')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertEqual([c[0] for c in self.adapter.calls], ['stop'])

    def test_expiry_reclaim_completes_while_peer_stop_hangs(self):
        a_dev, b_dev = self.emulator('A'), self.emulator('B')
        a, b = self.borrow('a'), self.borrow('b')
        entered, release = threading.Event(), threading.Event()
        original = self.adapter.stop
        def slow(device, observation):
            if device['id'] == b_dev['id']:
                entered.set()
                release.wait(5)
            return original(device, observation)
        self.adapter.stop = slow
        self.adapter.calls.clear()
        self.mono += 1801
        self.wall += 1801
        self.lib.control_tick()
        workers = [threading.Thread(target=self.worker, args=(self.lib, threading.Event()))
                   for _ in range(2)]
        for w in workers:
            w.start()
        try:
            self.assertTrue(entered.wait(5))  # B's reclaim is hung inside stop.
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if self.lib.lease(a['lease']['id'])['recorded_state'] == 'closed':
                    break
                time.sleep(0.01)
            # A's reclaim finished; B's device being stuck must not hold it.
            self.assertEqual(self.lib.lease(a['lease']['id'])['recorded_state'], 'closed')
            self.assertEqual(self.lib.device(a_dev['id'])['state'], 'available')
            self.assertEqual(self.lib.lease(b['lease']['id'])['recorded_state'], 'expired')
            self.assertEqual(self.lib.device(b_dev['id'])['state'], 'quarantined')
        finally:
            release.set()
            for w in workers:
                w.join(10)
        self.assertFalse(any(w.is_alive() for w in workers))
        self.assertEqual(self.lib.lease(b['lease']['id'])['recorded_state'], 'closed')
        self.assertEqual(self.lib.count_claims(), 0)

    def test_borrow_and_owner_checks_proceed_while_peer_return_hangs(self):
        b_dev = self.emulator('B')
        b = self.lib.borrow('b', 's', 'b', {'kind': 'emulator'}, self.token, owner_pid=os.getpid())
        self.lib.tick()
        b_lease = self.lib.request(b['id'])['lease']['id']
        entered, release = threading.Event(), threading.Event()
        original = self.adapter.stop
        def slow(device, observation):
            entered.set()
            release.wait(5)
            return original(device, observation)
        self.adapter.stop = slow
        with patch('device_library.process.state', return_value='exited'):
            self.lib.control_tick()
        self.assertEqual(self.lib.lease(b_lease)['recorded_state'], 'returning')
        ran = threading.Event()
        w = threading.Thread(target=self.worker, args=(self.lib, ran))
        w.start()
        try:
            self.assertTrue(entered.wait(5))  # Executor stuck stopping B.
            # A new borrow on a healthy device is not dragged by B.
            a_dev = self.emulator('A')
            r = self.lib.borrow('b2', 's2', 'a', {'kind': 'emulator'}, self.token,
                                owner_pid=os.getpid())
            self.lib.control_tick()
            self.drain(self.lib)
            request = self.lib.request(r['id'])
            self.assertEqual(request['state'], 'active')
            self.assertEqual(request['lease']['device'], a_dev['id'])
            # Owner checks also stay on schedule while the executor is stuck.
            with patch('device_library.process.state', return_value='exited'):
                self.lib.control_tick()
            self.assertEqual(self.lib.lease(request['lease']['id'])['recorded_state'], 'returning')
            self.assertTrue(entered.is_set() and not ran.is_set())
        finally:
            release.set()
            w.join(10)
        self.assertFalse(w.is_alive())
        self.assertEqual(self.lib.lease(b_lease)['recorded_state'], 'closed')
        self.lib.control_tick()
        self.drain(self.lib)
        self.assertEqual(self.lib.count_claims(), 0)

    def test_failed_maintenance_refuses_only_new_borrows(self):
        self.emulator()
        a = self.borrow()
        lid = a['lease']['id']
        self.lib.maintenance_failed('executor', 'simulated worker death')
        with self.assertRaisesRegex(Fault, 'maintenance_loop_failed'):
            self.lib.borrow('b', 's', 'new', {'kind': 'emulator'}, self.token)
        # Idempotent replay, renew and return keep working for existing records.
        again = self.lib.borrow('borrower', 'session', 'k', {'kind': 'emulator'}, self.token)
        self.assertEqual(again['lease']['id'], lid)
        self.assertEqual(self.lib.renew(lid, self.token)['state'], 'active')
        self.assertEqual(self.close(lid)['state'], 'closed')
        events = [e for e in self.lib.history() if e['type'] == 'maintenance_loop_failed']
        self.assertEqual(events[0]['payload']['role'], 'executor')
        self.assertIn('simulated worker death', events[0]['payload']['error'])
        with self.assertRaisesRegex(Fault, 'unknown_maintenance_role'):
            self.lib.maintenance_failed('bogus', 'x')

    def test_job_faults_contained_but_unexpected_errors_propagate(self):
        self.lib._enqueue_job('observe', 'inventory')
        original = self.adapter.observe
        self.adapter.observe = lambda devices: (_ for _ in ()).throw(ObservationError('fixture down'))
        self.lib.run_maintenance_job(self.lib.next_maintenance_job(timeout=0))
        self.assertEqual(self.lib._executor_health['state'], 'ok')
        self.adapter.observe = original
        # A bug escaping a handler propagates so the supervisor fails the loop.
        self.lib._enqueue_job('reclaim', 'missing-lease')
        job = self.lib.next_maintenance_job(timeout=0)
        with self.assertRaises((Fault, TypeError)):
            self.lib.run_maintenance_job(job)
        # The failed job released its claim so nothing is wedged.
        self.assertEqual(self.lib.maintenance_status()['executor']['in_flight'], [])

    def test_emulator_starts_never_run_in_parallel(self):
        self.emulator('A')
        self.emulator('B')
        r1 = self.lib.borrow('b', 's', '1', {'kind': 'emulator'}, self.token)
        r2 = self.lib.borrow('b', 's', '2', {'kind': 'emulator'}, self.token)
        active, peak, lock = [0], [0], threading.Lock()
        original = self.adapter.start
        def start(device):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            try:
                time.sleep(0.05)
                return original(device)
            finally:
                with lock:
                    active[0] -= 1
        self.adapter.start = start
        self.lib.control_tick()
        workers = [threading.Thread(target=self.worker, args=(self.lib, threading.Event()))
                   for _ in range(2)]
        for w in workers:
            w.start()
        for w in workers:
            w.join(10)
        self.assertFalse(any(w.is_alive() for w in workers))
        self.assertEqual(peak[0], 1)
        self.assertEqual(self.lib.request(r1['id'])['state'], 'active')
        self.assertEqual(self.lib.request(r2['id'])['state'], 'active')

    def test_same_device_jobs_are_serialized(self):
        d = self.emulator()
        a = self.borrow()
        self.mono += 1801
        self.wall += 1801
        self.lib.control_tick()
        reclaim = self.lib.next_maintenance_job(timeout=0)
        self.assertEqual(reclaim['kind'], 'reclaim')
        op = self.lib.db.execute(
            "SELECT id FROM operations WHERE kind='auto-reclaim'").fetchone()[0]
        self.lib._enqueue_job('finish-reclaim', op, device=d['id'])
        # The same-device job waits while its sibling runs; the global
        # observation job stays runnable.
        followup = self.lib.next_maintenance_job(timeout=0)
        self.assertEqual(followup['kind'], 'observe')
        self.lib.run_maintenance_job(followup)
        # The same-device finish-reclaim stays queued while reclaim is in flight.
        self.assertIsNone(self.lib.next_maintenance_job(timeout=0))
        self.lib.run_maintenance_job(reclaim)
        followup = self.lib.next_maintenance_job(timeout=0)
        self.assertEqual(followup['kind'], 'finish-reclaim')
        self.lib.run_maintenance_job(followup)
        self.assertEqual(self.lib.lease(a['lease']['id'])['recorded_state'], 'closed')
        self.assertEqual(self.lib.count_claims(), 0)


class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fixture = self.root / 'fixture.json'
        self.fixture.write_text('{}')
        self.adapter = FakeAdapter(self.fixture)
        self.token = 'test-secret-' * 4

    def tearDown(self):
        self.temp.cleanup()

    def library(self, directory, **kwargs):
        return Library(directory, self.adapter, initialize=True, **kwargs)

    def test_capacity_validated_persisted_and_enforced(self):
        for bad in (0, -1, 65, 2.5, '3', True):
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(Fault, 'invalid_emulator_capacity'):
                    self.library(self.root / ('bad-%s' % bad), capacity=bad)
        lib = self.library(self.root / 'cap', capacity=1)
        try:
            self.assertEqual(lib.capacity, 1)
            devices = [lib.register('emulator', n, {'avd_name': n, 'avd_path': n,
                       'avd_identity': n, 'sdk_root': 'fixture'}) for n in ('A', 'B')]
            lib.borrow('b', 's', '1', {'kind': 'emulator'}, self.token)
            lib.tick()
            second = lib.borrow('b', 's', '2', {'kind': 'emulator'}, self.token)
            lib.tick()
            self.assertEqual(lib.request(second['id'])['state'], 'queued')
            self.assertEqual(lib.count_claims(), 1)
        finally:
            lib.db.close()
        # Persisted: reopening without the flag keeps the configured capacity.
        reopened = Library(self.root / 'cap', self.adapter)
        try:
            self.assertEqual(reopened.capacity, 1)
        finally:
            reopened.db.close()
        # An explicit new value is validated and recorded with its predecessor.
        updated = Library(self.root / 'cap', self.adapter, capacity=2)
        try:
            self.assertEqual(updated.capacity, 2)
            events = [e for e in updated.history() if e['type'] == 'emulator_capacity_configured']
            self.assertEqual(events[-1]['payload']['previous'], 1)
            self.assertEqual(events[-1]['payload']['capacity'], 2)
        finally:
            updated.db.close()

    def test_corrupt_stored_capacity_fails_closed(self):
        lib = self.library(self.root / 'cap')
        lib.db.execute("INSERT OR REPLACE INTO meta VALUES('emulator_capacity','lots')")
        lib.db.close()
        with self.assertRaisesRegex(Fault, 'invalid_emulator_capacity'):
            Library(self.root / 'cap', self.adapter)

    def test_default_capacity_is_three(self):
        lib = self.library(self.root / 'default')
        try:
            self.assertEqual(lib.capacity, 3)
        finally:
            lib.db.close()


class ServerHealthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fixture = self.root / 'fixture.json'
        self.fixture.write_text('{}')
        self.server = Server(self.root / 'data', initialize=True,
                             test_runtime=str(self.root / 'runtime'), fixture=str(self.fixture))
        self.lib = self.server.library
        self.token = 'test-secret-' * 4

    def tearDown(self):
        self.server.running = False
        self.server.close()
        self.temp.cleanup()

    def doctor(self):
        return self.server._dispatch({'command': 'doctor', 'args': {}, 'credential': ''})

    def borrow_args(self):
        return {'command': 'borrow', 'credential': self.token,
                'args': {'borrower': 'b', 'session': 's', 'key': 'k',
                         'owner_pid': os.getpid(), 'body': {'kind': 'emulator'}}}

    def test_doctor_exposes_maintenance_health(self):
        out = self.doctor()
        self.assertEqual(out['maintenance']['control']['state'], 'not_started')
        self.assertEqual(out['maintenance']['executor']['state'], 'not_started')
        self.assertEqual(out['maintenance']['oldest_pending_operation'], None)
        self.assertEqual(out['emulator_capacity'], 3)
        spec = {'avd_name': 'fixture', 'avd_path': 'fixture',
                'avd_identity': 'fixture', 'sdk_root': 'fixture'}
        d = self.lib.register('emulator', 'fixture', spec)
        request = self.lib.borrow('b', 's', 'k', {'kind': 'emulator'}, self.token)
        self.lib.tick()
        out = self.doctor()
        self.assertEqual(out['maintenance']['control']['state'], 'ok')
        self.assertIsNotNone(out['maintenance']['control']['last_success'])
        self.assertIsNotNone(out['maintenance']['control']['last_duration'])
        self.assertEqual(out['maintenance']['executor']['state'], 'ok')
        self.assertEqual(out['maintenance']['executor']['in_flight'], [])
        self.assertIn(d['id'], out['device_observation_age_seconds'])
        # A pending return operation shows up with its age and identity.
        handoff = self.lib.return_lease(self.lib.request(request['id'])['lease']['id'],
                                        self.token, 'held')
        out = self.doctor()
        pending = out['maintenance']['oldest_pending_operation']
        self.assertEqual(pending['kind'], 'return')
        self.assertEqual(pending['state'], 'prepared')
        self.assertIn('age_seconds', pending)
        self.assertEqual(handoff['operation'], pending['id'])

    def test_control_loop_failure_marks_failed_and_refuses_borrows(self):
        self.lib.control_tick = lambda: (_ for _ in ()).throw(RuntimeError('simulated control bug'))
        t = threading.Thread(target=self.server._control_loop)
        t.start()
        t.join(5)
        self.assertFalse(t.is_alive())
        out = self.doctor()
        self.assertEqual(out['maintenance']['control']['state'], 'failed')
        self.assertIn('simulated control bug', out['maintenance']['control']['last_error'])
        with self.assertRaisesRegex(Fault, 'maintenance_loop_failed'):
            self.server.dispatch(self.borrow_args())
        # Queries and operator surfaces still work while the server stays up.
        self.assertTrue(self.doctor()['inventory_complete'] is not None)

    def test_executor_loop_failure_marks_failed_and_refuses_borrows(self):
        self.lib._enqueue_job('observe', 'inventory')
        self.lib.run_maintenance_job = \
            lambda job: (_ for _ in ()).throw(RuntimeError('simulated executor bug'))
        t = threading.Thread(target=self.server._executor_loop, args=(0,))
        t.start()
        t.join(5)
        self.assertFalse(t.is_alive())
        out = self.doctor()
        self.assertEqual(out['maintenance']['executor']['state'], 'failed')
        self.assertIn('simulated executor bug', out['maintenance']['executor']['last_error'])
        with self.assertRaisesRegex(Fault, 'maintenance_loop_failed'):
            self.server.dispatch(self.borrow_args())


class CapacityCliTests(unittest.TestCase):
    """The serve flag plumbs validated capacity into a running coordinator."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='dl-', dir='/tmp')
        self.root = Path(self.temp.name)
        self.runtime = self.root / 'r'
        self.data = self.root / 'd'
        self.fixture = self.root / 'f'
        self.fixture.write_text('{}')
        self.proc = None

    def tearDown(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(8)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        if self.proc:
            self.proc.communicate()
        self.temp.cleanup()

    def start(self, init=False, *extra):
        args = [sys.executable, '-m', 'device_library', '--test-runtime', str(self.runtime),
                'serve', '--data-dir', str(self.data), '--fake-fixture', str(self.fixture)]
        if init:
            args += ['--init']
        args += list(extra)
        self.proc = subprocess.Popen(args, cwd=ROOT, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, text=True)
        for _ in range(100):
            if self.proc.poll() is not None:
                self.fail(self.proc.communicate())
            if (self.runtime / 'coordinator.sock').exists():
                return
            time.sleep(.05)
        self.fail('socket not ready')

    def test_serve_emulator_capacity_flag_validated_and_visible(self):
        from device_library.server import call
        self.start(True, '--emulator-capacity', '1')
        out = call('doctor', runtime=self.runtime)
        self.assertTrue(out['ok'])
        self.assertEqual(out['result']['emulator_capacity'], 1)
        self.proc.terminate()
        self.proc.wait(8)
        self.proc.communicate()
        # Persisted capacity survives a restart without the flag.
        self.start()
        out = call('doctor', runtime=self.runtime)
        self.assertEqual(out['result']['emulator_capacity'], 1)

    def test_serve_emulator_capacity_flag_rejects_invalid_values(self):
        args = [sys.executable, '-m', 'device_library', '--test-runtime', str(self.runtime),
                'serve', '--data-dir', str(self.data), '--fake-fixture', str(self.fixture),
                '--init', '--emulator-capacity', '0']
        proc = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=15)
        self.assertEqual(proc.returncode, 2)
        self.assertIn('invalid_emulator_capacity', proc.stdout)


if __name__ == '__main__':
    unittest.main()
