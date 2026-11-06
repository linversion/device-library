"""Offline regressions for the overlapping architecture-review findings."""
import contextlib
import io
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from device_library import cli
from device_library.adapter import VerificationError
from device_library.core import Fault
from device_library.server import Server, call
import test_library as fixtures


class LeaseViewTests(unittest.TestCase):
    setUp = fixtures.CoreTests.setUp
    tearDown = fixtures.CoreTests.tearDown
    emulator = fixtures.CoreTests.emulator
    borrow = fixtures.CoreTests.borrow
    close = fixtures.CoreTests.close

    def assert_unusable(self, request, state):
        self.assertEqual(request['state'], state)
        self.assertEqual(request['lease']['state'], state)
        self.assertFalse(request['lease']['usable'])
        self.assertEqual(request['selector'], {})
        self.assertTrue(request['remedy'])

    def test_expired_before_tick_hides_selector_on_show_and_same_key_borrow(self):
        self.emulator(); first = self.borrow()
        self.mono += 1800; self.wall += 1800
        self.assert_unusable(self.lib.request(first['id']), 'expired')
        retry = self.lib.borrow('borrower', 'session', 'k', {'kind':'emulator'}, self.token)
        self.assert_unusable(retry, 'expired')
        self.assertEqual(retry['lease']['recorded_state'], 'active')
        with self.assertRaisesRegex(Fault, 'lease_expired_or_fenced'):
            self.lib.renew(first['lease']['id'], self.token)

    def test_fence_mismatch_before_tick_uses_same_validity_as_writes(self):
        device = self.emulator(); first = self.borrow()
        self.lib.db.execute('UPDATE devices SET fence=fence+1 WHERE id=?', (device['id'],))
        self.assert_unusable(self.lib.request(first['id']), 'fenced')
        with self.assertRaisesRegex(Fault, 'lease_expired_or_fenced'):
            self.lib.renew(first['lease']['id'], self.token)

    def test_expiry_reads_while_another_prepare_blocks_tick(self):
        self.emulator('A'); first = self.borrow(); self.emulator('B')
        self.lib.borrow('borrower', 'session', 'b', {'kind':'emulator'}, self.token)
        entered, release = threading.Event(), threading.Event()
        original = self.adapter.start
        def slow(device):
            entered.set(); release.wait(5); original(device)
        self.adapter.start = slow
        worker = threading.Thread(target=self.lib.tick)
        worker.start()
        try:
            self.assertTrue(entered.wait(3))
            self.mono += 1801; self.wall += 1801; self.lib.tick()
            retry = self.lib.borrow('borrower', 'session', 'k', {'kind':'emulator'}, self.token)
            self.assert_unusable(retry, 'expired')
        finally:
            release.set(); worker.join(5)
        self.assertFalse(worker.is_alive())

    def test_prepare_and_return_failure_are_terminal_with_attention(self):
        self.emulator(); self.fixture.write_text('{"fail_start":true}')
        failed = self.borrow()
        self.assert_unusable(failed, 'revoked')
        self.assertEqual(failed['lease']['attention']['state'], 'manual_intervention_required')
        self.assertEqual(self.lib.devices()[0]['attention'], failed['lease']['attention'])
        self.assertEqual(self.lib.count_claims(), 1)
        # An ended verification failure must expose the same terminal view.
        self.lib.operator_reconcile(failed['lease']['device'], 'verified ended', stopped=True, operations_ended=True)
        self.fixture.write_text('{}')
        self.adapter.prepare = lambda *args: (_ for _ in ()).throw(VerificationError('unhealthy'))
        failed = self.borrow('verified-failure')
        self.assert_unusable(failed, 'revoked')
        self.assertIsNotNone(failed['lease']['attention'])

    def test_return_stop_failure_does_not_leave_request_active(self):
        self.emulator(); first = self.borrow()
        f = self.adapter.read(); f['fail_stop'] = True; self.fixture.write_text(json.dumps(f))
        self.assertEqual(self.close(first['lease']['id'])['state'], 'quarantined')
        self.assert_unusable(self.lib.request(first['id']), 'revoked')
        self.assertIsNotNone(self.lib.devices()[0]['attention'])

    def test_recovery_pending_retry_and_completed_ack_return_original_result(self):
        self.emulator(); first = self.borrow(); lid = first['lease']['id']
        h = self.lib.recover(lid, self.token, 'close-system-anr')
        self.assertEqual(self.lib.recover(lid, self.token, 'close-system-anr'), h)
        paused = self.lib.lease(lid)
        self.assertFalse(paused['usable']); self.assertEqual(paused['selector'], {})
        result = self.lib.acknowledge(h['id'], self.token, 'paused')
        self.assertTrue(result['resume_permitted'])
        self.assertEqual(self.lib.acknowledge(h['id'], self.token, 'paused'), result)
        self.assertEqual(sum(c[0]=='close-system-anr' for c in self.adapter.calls), 1)

    def test_old_recovery_ack_cannot_resume_after_new_handoff_or_expiry(self):
        self.emulator(); first = self.borrow(); lid = first['lease']['id']
        h = self.lib.recover(lid, self.token, 'close-system-anr')
        self.lib.acknowledge(h['id'], self.token, 'paused')
        later = self.lib.recover(lid, self.token, 'close-system-anr')
        self.assertFalse(self.lib.acknowledge(h['id'], self.token, 'paused')['resume_permitted'])
        self.lib.acknowledge(later['id'], self.token, 'paused')
        self.assertFalse(self.lib.acknowledge(h['id'], self.token, 'paused')['resume_permitted'])
        self.mono += 1800; self.wall += 1800
        self.assertFalse(self.lib.acknowledge(later['id'], self.token, 'paused')['resume_permitted'])
        self.assertEqual(self.lib.acknowledge(later['id'], self.token, 'paused')['selector'], {})

    def test_completed_ack_is_fenced_and_saved_result_follows_chain_retention(self):
        self.emulator(); first = self.borrow(); lid = first['lease']['id']
        h = self.lib.recover(lid, self.token, 'close-system-anr')
        self.lib.acknowledge(h['id'], self.token, 'paused')
        self.assertIsNotNone(self.lib.db.execute('SELECT value FROM meta WHERE key=?', ('recovery_result:'+h['id'],)).fetchone())
        self.lib.db.execute('UPDATE devices SET fence=fence+1 WHERE id=?', (first['lease']['device'],))
        retry = self.lib.acknowledge(h['id'], self.token, 'paused')
        self.assertFalse(retry['resume_permitted']); self.assertEqual(retry['selector'], {})
        self.close(lid)
        self.wall += 181*86400
        self.lib.retention()
        self.assertIsNone(self.lib.db.execute('SELECT value FROM meta WHERE key=?', ('recovery_result:'+h['id'],)).fetchone())

    def test_expired_fenced_and_closed_same_key_borrow_exit_nonzero(self):
        device = self.emulator(); first = self.borrow()
        credential = self.root/'credential'; credential.write_text(self.token)
        credential.chmod(0o600)
        args = ['--credential-file', str(credential), 'borrow', '--kind', 'emulator',
                '--borrower', 'borrower', '--session', 'session', '--request-key', 'k',
                '--owner-pid', str(os.getpid()), '--wait', '8']
        def execute(expected):
            retry = self.lib.borrow('borrower', 'session', 'k', {'kind':'emulator'}, self.token)
            with patch('device_library.cli.call', return_value={'ok':True,'result':retry}):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(cli.main(args), expected)
        self.mono += 1800; self.wall += 1800; execute(4)
        self.mono -= 1800; self.wall -= 1800
        self.lib.db.execute('UPDATE devices SET fence=fence+1 WHERE id=?', (device['id'],))
        execute(4)
        self.close(first['lease']['id']); execute(5)


class BorrowerCliTests(unittest.TestCase):
    setUp = fixtures.SocketTests.setUp
    tearDown = fixtures.SocketTests.tearDown
    start = fixtures.SocketTests.start
    rpc = fixtures.SocketTests.rpc
    cli = fixtures.SocketTests.cli

    def register(self):
        admin = (self.runtime/'operator.credential').read_text()
        spec = {'avd_name':'fixture','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        self.rpc('register', {'kind':'emulator','alias':'fixture','spec':spec}, admin)

    def command(self, credential, *extra):
        return [sys.executable, '-m', 'device_library', '--test-runtime', str(self.runtime),
                '--credential-file', str(credential), *extra]

    def test_short_shell_without_owner_is_explicitly_rejected(self):
        self.register(); credential = self.root/'no-owner.token'
        args = self.command(credential, 'borrow', '--kind', 'emulator', '--borrower', 'b',
                            '--session', 's', '--request-key', 'short', '--wait', '3')
        p = subprocess.run(['/bin/sh', '-c', shlex.join(args)+'; :'], cwd=fixtures.ROOT,
                           capture_output=True, text=True, timeout=10)
        out = json.loads(p.stdout)
        self.assertFalse(out['ok'])
        self.assertEqual(out['reason'], 'borrow_requires_owner_pid')
        self.assertEqual(self.rpc('devices')['result']['claims'], [])

    def test_two_independent_short_shells_keep_stable_owner_until_it_exits(self):
        self.register(); owner = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        try:
            credential = self.root/'owner.token'
            args = self.command(credential, 'borrow', '--kind', 'emulator', '--borrower', 'b',
                                '--session', 's', '--request-key', 'short', '--wait', '3',
                                '--owner-pid', str(owner.pid))
            p = subprocess.run(['/bin/sh', '-c', shlex.join(args)+'; :'], cwd=fixtures.ROOT,
                               capture_output=True, text=True, timeout=10)
            lease = json.loads(p.stdout)['result']['lease']; lid = lease['id']
            self.assertEqual(lease['owner_process']['pid'], owner.pid)
            # Allow maintenance to observe the first shell's exit before the second call.
            time.sleep(1.1)
            args = self.command(credential, 'lease', 'show', lid)
            p = subprocess.run(['/bin/sh', '-c', shlex.join(args)+'; :'], cwd=fixtures.ROOT,
                               capture_output=True, text=True, timeout=10)
            self.assertTrue(json.loads(p.stdout)['result']['usable'])
            owner.terminate(); owner.wait(5)
            for _ in range(40):
                result = self.cli('lease', 'show', lid, credential=credential)[1]['result']
                if result['state']=='closed': break
                time.sleep(.1)
            self.assertEqual(result['state'], 'closed')
        finally:
            if owner.poll() is None: owner.terminate(); owner.wait(5)

    def test_failed_prepare_borrow_exits_before_wait_and_same_key_is_terminal(self):
        self.register(); self.fixture.write_text('{"fail_start":true}')
        credential = self.root/'failed.token'
        args = ('borrow','--kind','emulator','--borrower','b','--session','s','--request-key','failure',
                '--owner-pid',str(os.getpid()),'--wait','8')
        started = time.monotonic(); code, out = self.cli(*args, credential=credential)
        self.assertEqual(code, 5, out)
        self.assertLess(time.monotonic()-started, 4)
        self.assertEqual(out['result']['state'], 'revoked')
        self.assertTrue(out['result']['remedy'])
        self.assertEqual(self.cli(*args, credential=credential)[0], 5)


class TransportTests(unittest.TestCase):
    def test_connect_send_read_decode_have_distinct_failure_codes(self):
        cases = [('connect', ConnectionRefusedError(), 6), ('sendall', BrokenPipeError(), 7),
                 ('readline', ConnectionResetError(), 7), ('readline', socket.timeout(), 7)]
        for stage, error, expected in cases:
            with self.subTest(stage=stage, error=type(error).__name__), patch('device_library.server.socket.socket') as factory:
                sock = factory.return_value
                if stage=='readline':
                    sock.makefile.return_value.__enter__.return_value.readline.side_effect = error
                else:
                    getattr(sock, stage).side_effect = error
                with self.assertRaises(Fault) as caught:
                    call('borrow', {'key':'stable-key'}, 'private-credential', '/tmp/unused')
                self.assertEqual(caught.exception.code, expected)
                self.assertNotIn('private-credential', str(caught.exception.details))
        for data in (b'', b'{"ok":', b'{"ok":true}', b'[]\n'):
            with self.subTest(data=data), patch('device_library.server.socket.socket') as factory:
                factory.return_value.makefile.return_value.__enter__.return_value.readline.return_value = data
                with self.assertRaises(Fault) as caught:
                    call('recover', {'id':'lease-id','kind':'wake'}, runtime='/tmp/unused')
                self.assertEqual(caught.exception.code, 7)

    def test_cli_preserves_unknown_result_retry_guidance(self):
        details = {'command':'recover','args':{'id':'lease-id','kind':'wake'}, 'remedy':'retry recover for pending handoff'}
        with patch('device_library.cli.call', side_effect=Fault('coordinator_result_unknown', 7, details=details)):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                code = cli.main(['recover','lease-id','--kind','wake'])
        self.assertEqual(code, 7)
        self.assertEqual(json.loads(output.getvalue())['details'], details)


class CommitDisconnectTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='dl-', dir='/tmp')
        self.root = Path(self.temp.name); self.runtime = self.root/'r'
        fixture = self.root/'f'; fixture.write_text('{}')
        self.server = Server(self.root/'d', initialize=True, test_runtime=self.runtime, fixture=fixture)
        self.lib = self.server.library; self.token = 't'*32
        spec = {'avd_name':'fixture','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        self.lib.register('emulator','fixture',spec)

    def tearDown(self):
        self.server.close(); self.temp.cleanup()

    def rpc(self, command, args, *, drop=False):
        response = {}
        class Connection:
            def __init__(self, conn): self.conn = conn
            def __enter__(self): return self
            def __exit__(self, *args): self.conn.close()
            def __getattr__(self, name): return getattr(self.conn, name)
            def sendall(self, data):
                response.update(json.loads(data))
                if drop: raise BrokenPipeError('lost committed response')
                self.conn.sendall(data)
        def serve_one():
            conn, _ = self.server.socket.accept()
            self.server.handle(Connection(conn))
        worker = threading.Thread(target=serve_one); worker.start()
        try:
            if drop:
                with self.assertRaises(Fault) as caught:
                    call(command, args, self.token, self.runtime)
                self.assertEqual(caught.exception.code, 7)
            else:
                result = call(command, args, self.token, self.runtime)
        finally:
            worker.join(5)
        self.assertFalse(worker.is_alive())
        return response['result'] if drop else result['result']

    def test_committed_mutations_are_retrievable_after_socket_disconnect(self):
        args = {'borrower':'b','session':'s','key':'k','body':{'kind':'emulator'},'owner_pid':os.getpid()}
        lost = self.rpc('borrow', args, drop=True)
        self.assertEqual(self.rpc('borrow', args)['id'], lost['id'])
        self.lib.tick(); lid = self.lib.request(lost['id'])['lease']['id']
        recovery = {'id':lid,'kind':'close-system-anr'}
        handoff = self.rpc('recover', recovery, drop=True)
        self.assertEqual(self.rpc('recover', recovery), handoff)
        ack = {'id':handoff['id'],'state':'paused'}
        result = self.rpc('acknowledge', ack, drop=True)
        self.assertEqual(self.rpc('acknowledge', ack), result)
        renewed = self.rpc('renew', {'id':lid}, drop=True)
        self.assertEqual(self.rpc('lease', {'id':lid})['deadline_monotonic'], renewed['deadline_monotonic'])
        returned = self.rpc('return', {'id':lid}, drop=True)
        self.assertEqual(self.rpc('return', {'id':lid}), returned)
        self.assertEqual(sum(c[0]=='close-system-anr' for c in self.lib.adapter.calls), 1)
