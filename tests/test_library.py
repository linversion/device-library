import concurrent.futures
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from device_library.adapter import AndroidAdapter, FakeAdapter, ObservationError, avd_identity
from device_library.core import Library, Fault
from device_library import process
from unittest.mock import patch
from device_library.server import call, Server, restore

ROOT = Path(__file__).resolve().parents[1]


class CoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fixture = self.root/'fixture.json'
        self.fixture.write_text('{}')
        self.adapter = FakeAdapter(self.fixture)
        self.mono, self.wall = 1000., 1_800_000_000.
        self.lib = Library(self.root/'data', self.adapter, initialize=True,
                           clock=lambda:self.mono, wall=lambda:self.wall)
        self.token = 'test-secret-'*4

    def tearDown(self):
        self.lib.db.close()
        self.temp.cleanup()

    def emulator(self, name='AVD', **caps):
        return self.lib.register('emulator', name, {'avd_name':name,'avd_path':name,
               'avd_identity':name,'sdk_root':'fixture', 'capabilities':caps})

    def borrow(self, key='k', kind='emulator', **body):
        r = self.lib.borrow('borrower', 'session', key, {'kind':kind,**body}, self.token)
        self.lib.tick()
        return self.lib.request(r['id'])

    def close(self, lease):
        h = self.lib.return_lease(lease, self.token, 'done')
        return self.lib.acknowledge(h['id'], self.token, 'stopped')

    def test_capacity_fifo_earliest_satisfiable_and_foreign(self):
        for n in range(4): self.emulator(str(n), api=35)
        rs = [self.borrow(str(n)) for n in range(4)]
        self.assertEqual([r['state'] for r in rs], ['active']*3+['queued'])
        self.assertEqual(self.lib.count_claims(),3)
        self.close(rs[0]['lease']['id']); self.lib.tick()
        self.assertEqual(self.lib.request(rs[3]['id'])['state'],'active')
        self.assertEqual(self.lib.count_claims(),3)

    def test_invalid_nonfinite_deadline_rejected_before_persistence(self):
        self.emulator()
        for invalid in (float('nan'), float('inf'), -float('inf'), [], {}, True):
            with self.assertRaisesRegex(Fault,'invalid_queue_timeout'):
                self.lib.borrow('b','s',str(invalid),{'kind':'emulator','queue_timeout':invalid},self.token)
        self.assertEqual(self.lib.db.execute('SELECT count(*) FROM requests').fetchone()[0],0)
        self.assertEqual(self.borrow()['state'],'active')
        with self.assertRaisesRegex(Fault,'body_must_be_object'):self.lib.borrow('b','s','x',[],self.token)
        with self.assertRaisesRegex(Fault,'spec_must_be_object'):self.lib.register('emulator','x',[])

    def test_retirement_refuses_occupancy_and_requires_attestation(self):
        d=self.emulator()
        with self.assertRaisesRegex(Fault,'requires_old_client_stopped'):
            self.lib.retire(d['id'],'old generation')
        a=self.borrow()
        with self.assertRaisesRegex(Fault,'occupied'):
            self.lib.retire(d['id'],'old generation',stopped=True)
        self.assertEqual(self.lib.device(d['id'])['state'],'loaned')
        self.close(a['lease']['id'])
        bindings=self.lib.db.execute('SELECT count(*) FROM bindings WHERE device=?',(d['id'],)).fetchone()[0]
        retired=self.lib.retire(d['id'],'storage recreated',stopped=True)
        self.assertEqual(retired['state'],'retired')
        self.assertEqual(self.lib.lease(a['lease']['id'])['device'],d['id'])
        self.assertEqual(self.lib.db.execute('SELECT count(*) FROM bindings WHERE device=?',(d['id'],)).fetchone()[0],bindings)
        with self.assertRaisesRegex(Fault,'new_device_id'):
            self.lib.operator_reconcile(d['id'],'cannot unretire',stopped=True)

    def test_retirement_refuses_unresolved_coordinator_operation(self):
        d=self.emulator()
        with self.lib.tx():op=self.lib._operation(d,None,'prepare')
        for state in ('prepared','running','unknown','waiting'):
            self.lib.db.execute('UPDATE operations SET state=? WHERE id=?',(state,op))
            with self.assertRaisesRegex(Fault,'unresolved_operation'):
                self.lib.retire(d['id'],'retire',stopped=True)
        self.assertEqual(self.lib.device(d['id'])['state'],'available')

    def test_retired_bad_identity_no_longer_poisons_discovery(self):
        d=self.emulator();original=self.adapter.observe
        def observe(devices):
            if any(item['id']==d['id'] for item in devices):
                raise ObservationError('AVD storage identity changed')
            return original(devices)
        self.adapter.observe=observe
        self.assertFalse(self.lib.reconcile()['complete'])
        self.lib.retire(d['id'],'confirmed recreated directory',stopped=True)
        self.assertTrue(self.lib.reconcile()['complete'])
        new=self.lib.register('emulator','AVD',{**d['spec'],'avd_identity':'new-generation'})
        self.assertNotEqual(new['id'],d['id'])
        self.assertEqual(self.lib.device(d['id'])['state'],'retired')
        self.assertEqual(self.borrow()['lease']['device'],new['id'])

    def test_retired_running_instance_stays_foreign_and_new_generation_quarantined(self):
        d=self.emulator();self.adapter.start(d)
        f=self.adapter.read();f['instances'][0]['avd']='AVD';self.fixture.write_text(json.dumps(f))
        self.lib.reconcile()
        calls=list(self.adapter.calls)
        self.lib.retire(d['id'],'previous direct user stopped; retire metadata',stopped=True)
        self.assertTrue(self.lib.reconcile()['complete'])
        self.assertEqual(self.lib.count_claims(),1)
        claim=dict(self.lib.db.execute('SELECT * FROM claims').fetchone())
        self.assertIsNone(claim['device']);self.assertEqual(claim['state'],'foreign')
        self.assertEqual(self.adapter.calls,calls)
        new=self.lib.register('emulator','AVD',{**d['spec'],'avd_identity':'new-generation'})
        self.assertNotEqual(new['id'],d['id'])
        self.assertEqual(new['state'],'quarantined')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertEqual(self.lib.device(d['id'])['state'],'retired')

    def test_poisoned_registration_and_request_shapes_never_persist(self):
        spec={'avd_name':'test','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        for field,value in [('capabilities',[]),('avd_name',[]),('sdk_root',7)]:
            with self.assertRaises(Fault):self.lib.register('emulator','bad',{**spec,field:value})
        for spec in ({'hardware_serial':'h','model':'Pixel 7','serials':[{}]}, {'hardware_serial':1,'model':'Pixel 7','serials':['s']}):
            with self.assertRaises(Fault):self.lib.register('physical','bad',spec)
        self.assertEqual(self.lib.devices(),[])
        self.emulator()
        for body in ({'kind':'emulator','requires':[]}, {'kind':'emulator','device':[]}, {'kind':'emulator','task':[]}, {'kind':'emulator','requires':{'api':[]}}):
            with self.assertRaises(Fault):self.lib.borrow('b','s','bad',body,self.token)
        self.assertEqual(self.lib.db.execute('SELECT count(*) FROM requests').fetchone()[0],0)
        self.assertEqual(self.borrow()['state'],'active')

    def test_concurrent_cross_session_borrow_has_three_slots(self):
        for n in range(4):self.emulator(str(n))
        def borrow(n):
            r=self.lib.borrow('home-'+str(n), 'session-'+str(n), 'shared-key', {'kind':'emulator'}, self.token)
            self.lib.tick()
            return r['id']
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            requests=list(pool.map(borrow,range(4)))
        self.lib.tick()
        states=[self.lib.request(r)['state'] for r in requests]
        self.assertEqual(states.count('active'),3)
        self.assertEqual(states.count('queued'),1)
        self.assertEqual(self.lib.count_claims(),3)
        self.assertEqual(len([c for c in self.adapter.calls if c[0]=='start']),3)

    def test_unsatisfiable_does_not_block_later(self):
        self.emulator(api=35)
        a=self.borrow('a', requires={'api':34}); b=self.borrow('b', requires={'api':35})
        self.assertEqual(a['state'],'queued');self.assertEqual(b['state'],'active')

    def test_foreign_instances_block_start(self):
        self.emulator()
        self.fixture.write_text(json.dumps({'instances':[{'key':str(n)} for n in range(3)]}))
        self.assertEqual(self.borrow()['state'],'queued')
        self.assertFalse(self.adapter.calls)

    def test_unknown_inventory_blocks_start(self):
        self.emulator();self.fixture.write_text('{"complete":false}')
        self.assertEqual(self.borrow()['state'],'queued');self.assertFalse(self.adapter.calls)

    def test_idempotency_auth_and_no_token_in_events(self):
        self.emulator();a=self.borrow();b=self.borrow()
        self.assertEqual(a['lease']['id'],b['lease']['id'])
        with self.assertRaisesRegex(Fault,'idempotency_conflict'):
            self.lib.borrow('borrower','session','k',{'kind':'physical'},self.token)
        with self.assertRaisesRegex(Fault,'unauthorized'):
            self.lib.borrow('borrower','session','k',{'kind':'emulator'},'other'*8)
        self.assertNotIn(self.token,json.dumps(self.lib.history()))
        self.assertEqual(len([c for c in self.adapter.calls if c[0]=='start']),1)

    def test_expiry_stops_emulator_and_closes_without_borrower_attestation(self):
        d=self.emulator();a=self.borrow(task='abandoned-test');lid=a['lease']['id']
        self.mono+=1801;self.wall+=1801;self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'closed')
        self.assertEqual(self.lib.lease(lid)['reason'],'lease_expired')
        self.assertEqual(self.lib.request(a['id'])['state'],'expired')
        self.assertEqual(self.lib.device(d['id'])['state'],'available')
        self.assertEqual(self.lib.count_claims(),0)
        events=self.lib.history(lease=lid)
        completed=[e for e in events if e['type']=='auto_reclaim_completed']
        self.assertEqual(completed[0]['payload']['task'],'abandoned-test')
        self.assertTrue(completed[0]['payload']['process_exit_verified'])
        with self.assertRaisesRegex(Fault,'expired_or_fenced'):self.lib.renew(lid,self.token)
        self.assertEqual(self.borrow('next')['state'],'active')

    def test_physical_expiry_notifies_borrower_and_operator_without_release(self):
        d=self.lib.register('physical','phone',{'hardware_serial':'phone','model':'Pixel 7','serials':['phone']})
        f=self.adapter.read();f['devices']={d['id']:{'online':True,'serial':'phone','identity':'phone','transport':'1'}}
        self.fixture.write_text(json.dumps(f));self.lib.reconcile()
        a=self.borrow(kind='physical',task='phone-test');lid=a['lease']['id']
        self.mono+=1801;self.wall+=1801;self.lib.tick();self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'expired')
        self.assertEqual(self.lib.device(d['id'])['state'],'quarantined')
        attention=self.lib.devices()[0]['attention']
        self.assertEqual(attention['state'],'manual_intervention_required')
        self.assertEqual(attention['borrower'],'borrower')
        self.assertEqual(self.lib.lease(lid)['attention']['task'],'phone-test')
        with self.assertRaises(Fault) as expired:
            self.lib.renew(lid,self.token)
        self.assertIn('stop all direct actions',expired.exception.details['message'])
        self.assertEqual(expired.exception.details['attention']['state'],'manual_intervention_required')
        events=[e for e in self.lib.history(task='phone-test') if e['type']=='lease_expiry_notice']
        self.assertEqual(len(events),1)
        self.assertEqual(events[0]['payload']['recipients'],['borrower','operator'])
        self.assertEqual(self.adapter.calls,[])
        self.assertEqual(self.borrow('other',kind='physical')['state'],'queued')
        self.assertEqual(self.close(lid)['state'],'closed')
        self.assertIsNone(self.lib.devices()[0]['attention'])

    def test_expiry_stop_failure_stays_quarantined_and_is_not_retried(self):
        d=self.emulator();a=self.borrow();f=self.adapter.read();f['fail_stop']=True;self.fixture.write_text(json.dumps(f))
        self.mono+=1801;self.wall+=1801;self.lib.tick();self.lib.tick()
        self.assertEqual(self.lib.device(d['id'])['state'],'quarantined')
        self.assertEqual(self.lib.lease(a['lease']['id'])['state'],'expired')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertEqual(sum(c[0]=='stop' for c in self.adapter.calls),1)
        self.assertEqual(self.borrow('other')['state'],'queued')
        self.assertEqual(self.lib.devices()[0]['attention']['state'],'manual_intervention_required')
        failures=[e for e in self.lib.history() if e['type']=='auto_reclaim_failed']
        self.assertIn('lifecycle failure',failures[0]['payload']['failure_reason'])

    def test_optional_renewal_and_maximum_deadline(self):
        d=self.emulator();a=self.borrow();lid=a['lease']['id']
        for _ in range(4):
            self.mono+=1799;self.wall+=1799;self.lib.renew(lid,self.token);self.lib.tick()
        self.assertEqual(self.lib.device(d['id'])['state'],'loaned')
        self.assertFalse(any(c[0]=='stop' for c in self.adapter.calls))
        self.mono=8200;self.wall=1_800_007_200.;self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'closed')

    def test_abandoned_cancel_reclaims_only_after_original_deadline(self):
        self.emulator();a=self.borrow();lid=a['lease']['id']
        self.lib.cancel(a['id'],self.token);self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'returning')
        self.assertFalse(any(c[0]=='stop' for c in self.adapter.calls))
        self.mono+=1801;self.wall+=1801;self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'closed')
        self.assertEqual(self.lib.request(a['id'])['state'],'expired')

    def test_reclaim_releases_one_of_three_slots_without_stopping_renewed_peers(self):
        devices=[self.emulator(str(n)) for n in range(4)]
        requests=[self.borrow(str(n),device=d['id']) for n,d in enumerate(devices)]
        self.assertEqual(requests[3]['state'],'queued')
        self.mono+=1799;self.wall+=1799
        for request in requests[1:3]:
            self.lib.renew(request['lease']['id'],self.token)
        self.mono+=2;self.wall+=2;self.lib.tick()
        self.assertEqual(self.lib.lease(requests[0]['lease']['id'])['state'],'closed')
        for request in requests[1:3]:
            self.assertEqual(self.lib.lease(request['lease']['id'])['state'],'active')
        self.assertEqual(self.lib.request(requests[3]['id'])['state'],'active')
        self.assertEqual(self.lib.count_claims(),3)
        self.assertEqual([c for c in self.adapter.calls if c[0]=='stop'],[('stop',devices[0]['id'])])

    def test_expiry_with_running_recovery_never_stops_under_old_operation(self):
        d=self.emulator();a=self.borrow();lid=a['lease']['id']
        h=self.lib.recover(lid,self.token,'wake')
        entered=threading.Event();release=threading.Event();original=self.adapter.recover
        def slow(*args):
            entered.set();release.wait(5);return original(*args)
        self.adapter.recover=slow
        t=threading.Thread(target=lambda:self.lib.acknowledge(h['id'],self.token,'paused'))
        t.start();self.assertTrue(entered.wait(3))
        try:
            self.mono+=1801;self.wall+=1801;self.lib.tick()
            self.assertEqual(self.lib.device(d['id'])['state'],'quarantined')
            self.assertEqual(self.lib.count_claims(),1)
            self.assertFalse(any(c[0]=='stop' for c in self.adapter.calls))
        finally:
            release.set();t.join(5)

    def test_renew_during_observation_before_expiry_is_not_stopped(self):
        self.emulator();a=self.borrow();lid=a['lease']['id']
        self.mono+=1799;self.wall+=1799
        original=self.adapter.observe
        def renew(devices):
            self.lib.renew(lid,self.token)
            return original(devices)
        self.adapter.observe=renew;self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'active')
        self.assertFalse(any(c[0]=='stop' for c in self.adapter.calls))

    def owned(self, key='owned', kind='emulator', pid=None):
        r=self.lib.borrow('b','s',key,{'kind':kind},self.token,owner_pid=pid or os.getpid())
        self.lib.tick()
        return self.lib.request(r['id'])

    def test_thirty_minutes_without_renewal_then_expiry(self):
        self.emulator();a=self.owned();lid=a['lease']['id']
        self.assertEqual(self.lib.lease(lid)['remaining_seconds'],1800)
        self.assertFalse(self.lib.lease(lid)['renewal_required'])
        for elapsed in (301,600,898):
            self.mono+=elapsed;self.wall+=elapsed;self.lib.tick()
            self.assertEqual(self.lib.lease(lid)['state'],'active')
        self.assertEqual(self.lib.lease(lid)['remaining_seconds'],1)
        self.mono+=1;self.wall+=1;self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'closed')
        self.assertEqual(self.lib.lease(lid)['reason'],'lease_expired')
        self.assertEqual(self.lib.count_claims(),0)

    def test_owner_exit_at_deadline_closes_once_without_double_stop(self):
        self.emulator();a=self.owned();lid=a['lease']['id']
        self.mono+=1800;self.wall+=1800
        with patch('device_library.process.state',return_value='exited'):self.lib.tick()
        self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'closed')
        self.assertEqual(self.lib.count_claims(),0)
        self.assertEqual(sum(c[0]=='stop' for c in self.adapter.calls),1)
        self.assertFalse(self.lib.db.execute("SELECT 1 FROM operations WHERE state IN ('running','waiting','unknown')").fetchone())

    def test_unknown_owner_at_deadline_is_not_bypassed_by_expiry_reclaim(self):
        d=self.emulator();a=self.owned();lid=a['lease']['id']
        self.mono+=1800;self.wall+=1800
        with patch('device_library.process.state',return_value='unknown'):self.lib.tick()
        self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['reason'],'borrower_process_unknown')
        self.assertEqual(self.lib.device(d['id'])['state'],'quarantined')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertFalse(any(c[0]=='stop' for c in self.adapter.calls))
        self.assertEqual(self.borrow('next')['state'],'queued')

    def test_owner_exit_forces_stop_even_with_waiter(self):
        self.emulator();a=self.owned()
        b=self.lib.borrow('b','s','waiting',{'kind':'emulator'},self.token)
        with patch('device_library.process.state',side_effect=['exited','alive']):self.lib.tick()
        self.assertEqual(self.lib.lease(a['lease']['id'])['state'],'closed')
        self.assertEqual(self.lib.request(b['id'])['state'],'active')
        actions=[c[0] for c in self.adapter.calls]
        self.assertEqual(actions.count('stop'),1)
        self.assertEqual(actions.count('start'),2)

    def test_unknown_owner_quarantines_without_stop_or_release(self):
        d=self.emulator();a=self.owned()
        with patch('device_library.process.state',return_value='unknown'):self.lib.tick()
        self.assertEqual(self.lib.device(d['id'])['state'],'quarantined')
        self.assertEqual(self.lib.lease(a['lease']['id'])['reason'],'borrower_process_unknown')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertFalse(any(c[0]=='stop' for c in self.adapter.calls))
        self.assertEqual(self.close(a['lease']['id'])['state'],'closed')

    def test_physical_owner_exit_requires_explicit_stopped_return(self):
        d=self.lib.register('physical','phone',{'hardware_serial':'hardware','model':'Pixel 7','serials':['usb']})
        self.fixture.write_text(json.dumps({'devices':{d['id']:{'online':True,'serial':'usb','transport':'1','identity':'hardware'}}}))
        self.lib.reconcile()
        a=self.owned(kind='physical')
        self.assertEqual(a['state'],'active')
        with patch('device_library.process.state',return_value='exited'):self.lib.tick()
        self.assertEqual(self.lib.device(d['id'])['state'],'quarantined')
        self.assertEqual(self.lib.lease(a['lease']['id'])['reason'],'borrower_process_exited')
        self.assertFalse(any(c[0]=='stop' for c in self.adapter.calls))
        self.assertEqual(self.close(a['lease']['id'])['state'],'closed')
        self.assertEqual(self.lib.device(d['id'])['state'],'available')

    def test_owner_exit_completes_unacknowledged_manual_return(self):
        self.emulator();a=self.owned();lid=a['lease']['id']
        self.lib.return_lease(lid,self.token,'manual')
        with patch('device_library.process.state',return_value='exited'):self.lib.tick()
        self.assertEqual(self.lib.lease(lid)['state'],'closed')
        self.assertEqual(self.lib.count_claims(),0)

    def test_idempotent_retry_never_rebinds_owner(self):
        self.emulator();a=self.owned()
        again=self.lib.borrow('b','s','owned',{'kind':'emulator'},self.token,owner_pid=99999999)
        self.assertEqual(again['lease']['owner_process'],a['lease']['owner_process'])
        self.assertEqual(again['id'],a['id'])

    def test_exit_stop_failure_keeps_claim_and_blocks_new_loan(self):
        self.emulator();a=self.owned()
        f=self.adapter.read();f['fail_stop']=True;self.fixture.write_text(json.dumps(f))
        with patch('device_library.process.state',return_value='exited'):self.lib.tick()
        self.assertEqual(self.lib.lease(a['lease']['id'])['state'],'revoked')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertEqual(self.borrow('next')['state'],'queued')

    def test_exit_unknown_operation_retains_capacity(self):
        self.emulator();a=self.owned()
        h=self.lib.recover(a['lease']['id'],self.token,'wake')
        self.lib.db.execute("UPDATE operations SET state='unknown' WHERE id=?",(h['operation'],))
        with patch('device_library.process.state',return_value='exited'):self.lib.tick()
        self.assertEqual(self.lib.lease(a['lease']['id'])['state'],'returning')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertFalse(any(c[0]=='stop' for c in self.adapter.calls))

    def test_dead_queued_owner_cancelled_without_start(self):
        self.emulator()
        r=self.lib.borrow('b','s','queued',{'kind':'emulator'},self.token,owner_pid=os.getpid())
        with patch('device_library.process.state',return_value='exited'):self.lib.tick()
        self.assertEqual(self.lib.request(r['id'])['state'],'cancelled')
        self.assertFalse(self.adapter.calls)

    def test_unreadable_or_exited_owner_rejected_before_persistence(self):
        with patch('device_library.process.identity',side_effect=process.ProcessUnknown('denied')):
            with self.assertRaises(Fault):self.lib.borrow('b','s','bad',{'kind':'emulator'},self.token,owner_pid=999)
        with patch('device_library.process.identity',return_value=None):
            with self.assertRaises(Fault):self.lib.borrow('b','s','bad',{'kind':'emulator'},self.token,owner_pid=999)
        self.assertEqual(self.lib.db.execute('SELECT count(*) FROM requests').fetchone()[0],0)

    def test_return_requires_stop_then_stops_idle_emulator(self):
        d=self.emulator();a=self.borrow();lid=a['lease']['id']
        h=self.lib.return_lease(lid,self.token,'done')
        self.assertEqual(h['id'],self.lib.return_lease(lid,self.token,'retry')['id'])
        with self.assertRaisesRegex(Fault,'requires_stopped'):self.lib.acknowledge(h['id'],self.token,'paused')
        self.assertEqual(self.lib.device(d['id'])['state'],'returning')
        self.assertEqual(self.lib.acknowledge(h['id'],self.token,'stopped')['state'],'closed')
        self.assertEqual(self.lib.count_claims(),0)
        self.assertEqual(self.lib.acknowledge(h['id'],self.token,'stopped')['resume_permitted'],False)

    def test_stop_failure_retains_slot_and_quarantine(self):
        d=self.emulator();a=self.borrow();f=self.adapter.read();f['fail_stop']=True;self.fixture.write_text(json.dumps(f))
        self.assertEqual(self.close(a['lease']['id'])['state'],'quarantined')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertEqual(self.borrow('next')['state'],'queued')

    def test_subprocess_timeout_preparation_retains_claim(self):
        from unittest.mock import patch
        self.emulator()
        self.adapter.start=lambda d:AndroidAdapter._run(['fake-adb'])
        with patch('device_library.adapter.subprocess.run', side_effect=subprocess.TimeoutExpired(['fake-adb'],90)):
            a=self.borrow()
        self.assertEqual(a['lease']['state'],'revoked')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertEqual(self.borrow('second')['state'],'queued')

    def test_start_failure_retains_uncertain_claim(self):
        self.emulator();self.fixture.write_text('{"fail_start":true}')
        a=self.borrow();self.assertEqual(a['lease']['state'],'revoked')
        self.assertEqual(self.lib.count_claims(),1)
        self.assertEqual(self.lib.db.execute('SELECT state FROM claims').fetchone()[0],'uncertain')

    def test_restart_fences_and_does_not_replay_start(self):
        d=self.emulator();a=self.borrow();lid=a['lease']['id']
        self.lib.db.close()
        self.lib=Library(self.root/'data',self.adapter,clock=lambda:self.mono,wall=lambda:self.wall)
        self.assertEqual(self.lib.lease(lid)['state'],'revoked')
        with self.assertRaisesRegex(Fault,'expired_or_fenced'):self.lib.renew(lid,self.token)
        self.assertEqual(self.borrow('next')['state'],'queued')
        self.assertEqual(len([c for c in self.adapter.calls if c[0]=='start']),1)
        self.assertEqual(self.lib.operator_reconcile(d['id'],'verified',stopped=True,operations_ended=True)['state'],'closed')

    def test_restart_during_prepared_keeps_unknown_operation(self):
        d=self.emulator();a=self.borrow();lid=a['lease']['id']
        with self.lib.tx():self.lib.db.execute("UPDATE operations SET state='prepared'")
        self.lib.db.close();self.lib=Library(self.root/'data',self.adapter)
        self.assertEqual(self.close(lid)['reason'],'old_coordinator_operations_need_attestation')
        self.assertEqual(self.lib.operator_reconcile(d['id'],'observed all old processes ended',stopped=True,operations_ended=True)['state'],'closed')

    def test_prepared_return_wedge_recovered_by_operation_end_and_reconcile(self):
        # Gap: a dead prepared return at an old fence blocked every later return
        # and operator reconcile could not collect it; the device was wedged.
        d = self.physical()
        a = self.borrow(kind='physical'); lid = a['lease']['id']
        h1 = self.lib.return_lease(lid, self.token, 'first attempt')
        op1 = h1['operation']
        self.assertEqual(dict(self.lib.db.execute('SELECT state,fence FROM operations WHERE id=?', (op1,)).fetchone())['state'], 'prepared')
        fence = self.lib.device(d['id'])['fence']
        # The client exited without acknowledging; a connection change fenced it.
        f = self.adapter.read(); f['devices'][d['id']]['transport'] = 'new'; self.fixture.write_text(json.dumps(f))
        self.lib.reconcile()
        self.assertEqual(self.lib.device(d['id'])['fence'], fence + 1)
        self.assertEqual(self.lib.device(d['id'])['state'], 'quarantined')
        # Borrower retry is refused by the dead prepared operation, named in diagnostics.
        h2 = self.lib.return_lease(lid, self.token, 'retry after fencing')
        result = self.lib.acknowledge(h2['id'], self.token, 'stopped')
        self.assertEqual(result['reason'], 'old_coordinator_operations_need_attestation')
        self.assertIn(op1, [o['id'] for o in result['blocking_operations']])
        self.assertEqual(result['blocking_operations'][0]['state'], 'prepared')
        # Reconcile alone still cannot finish while the prepared leftover blocks.
        blocked = self.lib.operator_reconcile(d['id'], 'old client stopped', stopped=True)
        self.assertEqual(blocked['state'], 'quarantined')
        self.assertEqual(blocked['reason'], 'old_coordinator_operations_need_attestation')
        self.assertIn(op1, [o['id'] for o in blocked['blocking_operations']])
        # The per-operation remedy collects the dead prepared operation.
        ended = self.lib.operation_end(op1, 'borrower process exited; no coordinator subprocess remains')
        self.assertEqual(ended['state'], 'operator_verified_ended')
        # The retried return operation is waiting at the current fence: refused
        # without stronger evidence, attestable with it.
        with self.assertRaisesRegex(Fault, 'current_fence_operation_requires_old_client_stopped'):
            self.lib.operation_end(h2['operation'], 'no evidence yet')
        self.assertEqual(self.lib.operation_end(h2['operation'], 'return verifier confirmed ended', stopped=True)['state'], 'operator_verified_ended')
        # Full operator repair now closes the lease and releases the device.
        result = self.lib.operator_reconcile(d['id'], 'verified old users and operations stopped', stopped=True, operations_ended=True)
        self.assertEqual(result['state'], 'closed')
        self.assertEqual(self.lib.device(d['id'])['state'], 'available')
        self.assertEqual(self.lib.lease(lid)['state'], 'closed')
        self.assertEqual(self.borrow('next', kind='physical')['state'], 'active')

    def test_reconcile_old_operations_ended_sweeps_dead_fenced_prepared_and_waiting(self):
        d = self.physical()
        a = self.borrow(kind='physical'); lid = a['lease']['id']
        h1 = self.lib.return_lease(lid, self.token, 'first attempt')
        f = self.adapter.read(); f['devices'][d['id']]['transport'] = 'new'; self.fixture.write_text(json.dumps(f))
        self.lib.reconcile()
        dead = [h1['operation']]
        for state in ('prepared', 'waiting'):
            with self.lib.tx():
                op = self.lib._operation(self.lib.device(d['id']), {'id': lid}, 'return')
                self.lib.db.execute("UPDATE operations SET state=?,fence=fence-1 WHERE id=?", (state, op))
            dead.append(op)
        result = self.lib.operator_reconcile(d['id'], 'verified old operations ended', stopped=True, operations_ended=True)
        self.assertEqual(result['state'], 'closed')
        states = {r[0]: r[1] for r in self.lib.db.execute('SELECT id,state FROM operations')}
        for op in dead:
            self.assertEqual(states[op], 'operator_verified_ended')
        self.assertEqual(self.lib.device(d['id'])['state'], 'available')

    def test_operation_end_refusals_and_guards(self):
        d = self.emulator()
        a = self.borrow(); lid = a['lease']['id']
        with self.lib.tx():
            running = self.lib._operation(self.lib.device(d['id']), {'id': lid}, 'wake')
            self.lib.db.execute("UPDATE operations SET state='running' WHERE id=?", (running,))
        with self.assertRaisesRegex(Fault, 'operation_end_reason_required'):
            self.lib.operation_end(running, '')
        with self.assertRaisesRegex(Fault, 'operation_end_reason_required'):
            self.lib.operation_end(running, None)
        with self.assertRaisesRegex(Fault, 'operation_not_found'):
            self.lib.operation_end('nonexistent', 'reason')
        # Running operations are never silently attestable.
        with self.assertRaisesRegex(Fault, 'running_operation_requires_old_client_stopped'):
            self.lib.operation_end(running, 'client exited but no evidence')
        ended = self.lib.operation_end(running, 'verified process tree exited', stopped=True)
        self.assertEqual(ended['state'], 'operator_verified_ended')
        self.assertEqual(ended['result']['previous_state'], 'running')
        # Idempotent repeat; terminal operations are refused.
        self.assertEqual(self.lib.operation_end(running, 'again')['state'], 'operator_verified_ended')
        h = self.lib.return_lease(lid, self.token, 'done')
        self.assertEqual(self.lib.acknowledge(h['id'], self.token, 'stopped')['state'], 'closed')
        with self.assertRaisesRegex(Fault, 'operation_already_terminal'):
            self.lib.operation_end(h['operation'], 'already closed')

    def test_recovery_pause_resume_budget(self):
        self.emulator();a=self.borrow();lid=a['lease']['id']
        for _ in range(3):
            h=self.lib.recover(lid,self.token,'close-system-anr')
            self.assertNotIn(('close-system-anr',a['lease']['device']),self.adapter.calls[-1:]) if _==0 else None
            result=self.lib.acknowledge(h['id'],self.token,'paused')
            self.assertTrue(result['resume_permitted'])
        with self.assertRaisesRegex(Fault,'budget'):self.lib.recover(lid,self.token,'close-system-anr')
        self.assertEqual(self.lib.device(a['lease']['device'])['state'],'quarantined')

    def test_recovery_ack_after_return_cannot_resume(self):
        self.emulator();a=self.borrow();lid=a['lease']['id']
        h=self.lib.recover(lid,self.token,'close-system-anr')
        self.lib.return_lease(lid,self.token,'done')
        self.assertFalse(self.lib.acknowledge(h['id'],self.token,'paused')['resume_permitted'])
        self.assertNotIn(('close-system-anr',a['lease']['device']),self.adapter.calls)

    def test_connection_change_fences(self):
        d=self.emulator();a=self.borrow();f=self.adapter.read();f['devices'][d['id']]['transport']='new';self.fixture.write_text(json.dumps(f))
        self.lib.reconcile()
        self.assertEqual(self.lib.lease(a['lease']['id'])['state'],'revoked')

    def test_clock_jump_quarantines(self):
        self.emulator();a=self.borrow();self.wall+=100;self.lib.tick()
        self.assertEqual(self.lib.lease(a['lease']['id'])['reason'],'clock_or_sleep_uncertain')

    def test_retention_uses_closure_wall_time(self):
        self.emulator();a=self.borrow();self.close(a['lease']['id'])
        self.assertEqual(self.lib.retention()['closed_chains'],0)
        self.wall+=181*86400
        self.assertEqual(self.lib.retention()['closed_chains'],1)

    def test_foreign_registration_requires_explicit_transfer(self):
        d=self.emulator();self.adapter.start(d)
        self.lib.db.execute("UPDATE devices SET state='quarantined'")
        self.assertEqual(self.lib.operator_reconcile(d['id'],'observe only')['state'],'quarantined')
        self.assertEqual(self.lib.operator_reconcile(d['id'],'owner stopped',stopped=True)['state'],'available')

    def test_cancel_while_start_blocked_is_responsive_and_never_grants(self):
        self.emulator();entered=threading.Event();release=threading.Event();original=self.adapter.start
        def slow(d):entered.set();release.wait(5);original(d)
        self.adapter.start=slow
        r=self.lib.borrow('b','s','slow',{'kind':'emulator'},self.token)
        t=threading.Thread(target=self.lib.tick);t.start();self.assertTrue(entered.wait(3))
        before=time.monotonic();cancel=self.lib.cancel(r['id'],self.token)
        self.assertLess(time.monotonic()-before,1)
        release.set();t.join(5);self.assertFalse(t.is_alive())
        self.assertNotEqual(self.lib.request(r['id'])['lease']['state'],'active')
        self.assertEqual(self.lib.count_claims(),1)

    def physical(self):
        d = self.lib.register('physical', 'Pixel 7', {
            'hardware_serial': 'pixel', 'model': 'Pixel 7',
            'lock_policy': 'no-credential-wake', 'serials': ['pixel']})
        self.fixture.write_text(json.dumps({'devices': {d['id']: {
            'online': True, 'serial': 'pixel', 'transport': '1', 'identity': 'pixel'}}}))
        self.lib.reconcile()
        return d

    def test_cancel_active_physical_completes_without_stop(self):
        d = self.physical()
        a = self.borrow(kind='physical')
        self.assertEqual(a['state'], 'active')
        h = self.lib.cancel(a['id'], self.token)
        self.assertEqual(self.lib.lease(a['lease']['id'])['reason'], 'request_cancelled')
        self.assertEqual(self.lib.acknowledge(h['id'], self.token, 'stopped')['state'], 'closed')
        self.assertEqual(self.lib.device(d['id'])['state'], 'available')
        self.assertFalse(self.adapter.calls)

    def test_cancel_after_failed_unlock_verification_can_return_healthy_physical(self):
        d = self.physical()
        # All device commands return; only the unlock postcondition is unproven.
        real = AndroidAdapter(runner=lambda args: 'isKeyguardShowing=true' if 'policy' in args else '1')
        self.adapter.prepare = real.prepare
        a = self.borrow(kind='physical')
        self.assertEqual(a['lease']['state'], 'revoked')
        h = self.lib.cancel(a['id'], self.token)
        self.assertEqual(self.lib.acknowledge(h['id'], self.token, 'stopped')['state'], 'closed')
        self.assertEqual(self.lib.device(d['id'])['state'], 'available')
        self.assertEqual(self.lib.count_claims(), 0)

    def test_cancel_during_successful_preparation_finishes_after_operation_ends(self):
        d = self.emulator()
        entered, release = threading.Event(), threading.Event()
        original = self.adapter.prepare
        def slow(device, observation):
            entered.set()
            self.assertTrue(release.wait(5))
            return original(device, observation)
        self.adapter.prepare = slow
        r = self.lib.borrow('b', 's', 'cancel-preparing', {'kind': 'emulator'}, self.token)
        t = threading.Thread(target=self.lib.tick)
        t.start()
        try:
            self.assertTrue(entered.wait(3))
            h = self.lib.cancel(r['id'], self.token)
            self.lib.acknowledge(h['id'], self.token, 'stopped')
            self.assertEqual(self.lib.lease(h['lease'])['state'], 'returning')
            self.assertEqual(self.lib.count_claims(), 1)
        finally:
            release.set()
            t.join(5)
        self.assertFalse(t.is_alive())
        self.lib.tick()
        self.assertEqual(self.lib.lease(h['lease'])['state'], 'closed')
        self.assertEqual(self.lib.device(d['id'])['state'], 'available')
        self.assertEqual(self.lib.count_claims(), 0)
        self.assertEqual(sum(c[0] == 'stop' for c in self.adapter.calls), 1)

    def test_cancel_during_failed_unlock_retains_ack_and_finishes(self):
        d = self.physical()
        entered, release = threading.Event(), threading.Event()
        real = AndroidAdapter(runner=lambda args: 'isKeyguardShowing=true' if 'policy' in args else '1')
        def slow(device, observation):
            entered.set()
            self.assertTrue(release.wait(5))
            return real.prepare(device, observation)
        self.adapter.prepare = slow
        r = self.lib.borrow('b', 's', 'cancel-unlock', {'kind': 'physical'}, self.token)
        t = threading.Thread(target=self.lib.tick)
        t.start()
        try:
            self.assertTrue(entered.wait(3))
            h = self.lib.cancel(r['id'], self.token)
            self.lib.acknowledge(h['id'], self.token, 'stopped')
        finally:
            release.set()
            t.join(5)
        self.assertFalse(t.is_alive())
        self.lib.tick()
        self.assertEqual(self.lib.lease(h['lease'])['state'], 'closed')
        self.assertEqual(self.lib.device(d['id'])['state'], 'available')
        self.assertFalse(self.adapter.calls)

    def test_cancel_after_unknown_prepare_never_closes_or_reloans(self):
        d = self.emulator()
        def unknown(*args):
            raise ObservationError('command result unknown: android')
        self.adapter.prepare = unknown
        a = self.borrow()
        h = self.lib.cancel(a['id'], self.token)
        self.lib.acknowledge(h['id'], self.token, 'stopped')
        next_request = self.lib.borrow('b', 's', 'next', {'kind': 'emulator'}, self.token)
        for _ in range(3):
            self.lib.tick()
        self.assertEqual(self.lib.lease(a['lease']['id'])['state'], 'returning')
        self.assertEqual(self.lib.device(d['id'])['state'], 'returning')
        self.assertEqual(self.lib.request(next_request['id'])['state'], 'queued')
        self.assertEqual(self.lib.count_claims(), 1)
        self.assertFalse(any(c[0] == 'stop' for c in self.adapter.calls))

    def test_cancel_failed_unlock_offline_physical_stays_unavailable(self):
        d = self.physical()
        self.adapter.prepare = AndroidAdapter(runner=lambda args: 'isKeyguardShowing=true' if 'policy' in args else '1').prepare
        a = self.borrow(kind='physical')
        h = self.lib.cancel(a['id'], self.token)
        self.fixture.write_text(json.dumps({'devices': {d['id']: {'online': False}}}))
        result = self.lib.acknowledge(h['id'], self.token, 'stopped')
        self.assertEqual(result['state'], 'quarantined')
        self.lib.tick()
        self.assertNotEqual(self.lib.device(d['id'])['state'], 'available')
        self.assertNotEqual(self.lib.lease(a['lease']['id'])['state'], 'closed')

    def test_duplicate_return_ack_does_not_double_stop(self):
        self.emulator();a=self.borrow();lid=a['lease']['id']
        h=self.lib.return_lease(lid,self.token,'done')
        entered=threading.Event();release=threading.Event();original=self.adapter.stop
        def slow(d,o):entered.set();release.wait(5);original(d,o)
        self.adapter.stop=slow
        results=[]
        t=threading.Thread(target=lambda:results.append(self.lib.acknowledge(h['id'],self.token,'stopped')))
        t.start();self.assertTrue(entered.wait(3))
        self.assertEqual(self.lib.acknowledge(h['id'],self.token,'stopped')['state'],'in_progress')
        r=self.lib.borrow('b','s','next',{'kind':'emulator'},self.token);self.lib.tick()
        self.assertEqual(self.lib.request(r['id'])['state'],'queued')
        with self.assertRaisesRegex(Fault,'in_progress'):
            self.lib.operator_reconcile(a['lease']['device'],'cannot bypass live operation',stopped=True,operations_ended=True)
        release.set();t.join(5);self.assertFalse(t.is_alive())
        self.assertEqual(results[0]['state'],'closed')
        self.assertEqual(len([c for c in self.adapter.calls if c[0]=='stop']),1)
        self.lib.tick();new=self.lib.request(r['id']);self.assertEqual(new['state'],'active')
        self.lib.acknowledge(h['id'],self.token,'stopped')
        self.assertEqual(self.lib.lease(new['lease']['id'])['state'],'active')

    def test_duplicate_recovery_ack_and_return_during_recovery(self):
        self.emulator();a=self.borrow();lid=a['lease']['id']
        h=self.lib.recover(lid,self.token,'close-system-anr')
        entered=threading.Event();release=threading.Event();original=self.adapter.recover
        def slow(d,o,k):entered.set();release.wait(5);return original(d,o,k)
        self.adapter.recover=slow;results=[]
        t=threading.Thread(target=lambda:results.append(self.lib.acknowledge(h['id'],self.token,'paused')))
        t.start();self.assertTrue(entered.wait(3))
        self.assertEqual(self.lib.acknowledge(h['id'],self.token,'paused')['state'],'in_progress')
        ret=self.lib.return_lease(lid,self.token,'done')
        self.assertEqual(self.lib.acknowledge(ret['id'],self.token,'stopped')['state'],'quarantined')
        release.set();t.join(5);self.assertFalse(t.is_alive())
        self.assertFalse(results[0]['resume_permitted'])
        self.assertNotEqual(self.lib.lease(lid)['state'],'closed')
        self.assertEqual(len([c for c in self.adapter.calls if c[0]=='close-system-anr']),1)

    def test_expiry_during_recovery_never_resumes(self):
        self.emulator();a=self.borrow();lid=a['lease']['id']
        h=self.lib.recover(lid,self.token,'close-system-anr')
        original=self.adapter.recover
        def expire(d,o,k):
            self.mono+=1801;self.wall+=1801
            return original(d,o,k)
        self.adapter.recover=expire
        self.assertFalse(self.lib.acknowledge(h['id'],self.token,'paused')['resume_permitted'])
        self.assertEqual(self.lib.device(a['lease']['device'])['state'],'quarantined')

    def test_cancel_snapshot_queued_while_prior_start_blocks(self):
        self.emulator('a');self.emulator('b')
        r1=self.lib.borrow('b','s','1',{'kind':'emulator'},self.token)
        r2=self.lib.borrow('b','s','2',{'kind':'emulator'},self.token)
        entered=threading.Event();release=threading.Event();original=self.adapter.start
        def slow(d):entered.set();release.wait(5);original(d)
        self.adapter.start=slow
        t=threading.Thread(target=self.lib.tick);t.start();self.assertTrue(entered.wait(3))
        self.assertEqual(self.lib.cancel(r2['id'],self.token)['state'],'cancelled')
        release.set();t.join(5)
        self.assertFalse(t.is_alive());self.assertIsNone(self.lib.request(r2['id'])['lease'])
        self.assertEqual(len([c for c in self.adapter.calls if c[0]=='start']),1)

    def test_registered_avd_started_externally_quarantines(self):
        d=self.emulator();self.adapter.start(d)
        self.lib.reconcile()
        self.assertEqual(self.lib.device(d['id'])['state'],'quarantined')
        self.assertEqual(self.lib.db.execute('SELECT state FROM claims').fetchone()[0],'foreign')
        self.assertEqual(self.borrow()['state'],'queued')
        self.lib.operator_reconcile(d['id'],'previous user stopped',stopped=True)
        self.lib.tick();self.assertEqual(self.borrow()['state'],'active')

    def test_unchanged_observation_events_coalesce_and_unlinked_prune(self):
        self.emulator();self.lib.reconcile()
        count=len(self.lib.history())
        for _ in range(5):self.lib.reconcile()
        self.assertEqual(len(self.lib.history()),count)
        self.wall+=181*86400;self.lib.retention()
        events=self.lib.history();self.assertEqual([e['type'] for e in events],['retention_applied'])

    def test_stale_observation_cannot_remove_new_reservation(self):
        d=self.emulator();entered=threading.Event();release=threading.Event();original=self.adapter.observe
        first=[True]
        def slow(devices):
            snapshot=original(devices)
            if first[0]:first[0]=False;entered.set();release.wait(5)
            return snapshot
        self.adapter.observe=slow
        t=threading.Thread(target=self.lib.reconcile);t.start();self.assertTrue(entered.wait(3))
        r=self.borrow();self.assertEqual(r['state'],'active')
        release.set();t.join(5)
        self.assertFalse(t.is_alive());self.assertEqual(self.lib.count_claims(),1)
        self.assertEqual(self.lib.lease(r['lease']['id'])['state'],'active')

    def test_newer_schema_rejected_without_schema_mutation(self):
        self.lib.db.close();db=sqlite3.connect(self.root/'data'/'ledger.sqlite3')
        db.execute("UPDATE meta SET value='999' WHERE key='schema'");db.commit()
        before=db.execute('SELECT sql FROM sqlite_master ORDER BY name').fetchall();db.close()
        with self.assertRaisesRegex(Fault,'unsupported_schema'):Library(self.root/'data',self.adapter)
        db=sqlite3.connect(self.root/'data'/'ledger.sqlite3')
        self.assertEqual(before,db.execute('SELECT sql FROM sqlite_master ORDER BY name').fetchall());db.close()
        self.lib.db=sqlite3.connect(self.root/'data'/'ledger.sqlite3')


class AdapterTests(unittest.TestCase):
    def device(self, kind='emulator'):
        return {'kind':kind,'spec':{'model':'Pixel 7','lock_policy':'no-credential-wake'}}

    def test_json_system_anr_close_and_verify(self):
        calls=[];layouts=iter([json.dumps({'elements':[
            {'packageName':'android','text':"System UI isn't responding"},
            {'packageName':'android','resourceId':'android:id/aerr_close','enabled':True,'bounds':{'left':10,'top':20,'right':30,'bottom':40}}]}),'{"elements":[]}'])
        def runner(args):
            calls.append(args)
            if args[:2]==['android','layout']:
                Path(args[args.index('--output')+1]).write_text(next(layouts))
                return 'Installing layout instrumentation server...\n'
            if 'getprop' in args:return '1'
            return 'Application Not Responding: system'
        a=AndroidAdapter(runner=runner)
        self.assertTrue(a.recover(self.device(),{'serial':'emulator-test','online':True},'close-system-anr')['interrupted'])
        self.assertIn(['adb','-s','emulator-test','shell','input','tap','20','30'],calls)

    def test_arbitrary_close_never_clicked(self):
        calls=[]
        def runner(args):
            calls.append(args)
            if 'layout' in args:
                Path(args[args.index('--output')+1]).write_text(json.dumps({'elements':[{'package':'com.app','text':'Close','resourceId':'android:id/aerr_close','enabled':True,'bounds':[0,0,10,10]}]}))
                return ''
            return 'Application Not Responding: system'
        with self.assertRaises(ObservationError):AndroidAdapter(runner=runner).recover(self.device(),{'serial':'emulator-test','online':True},'close-system-anr')
        self.assertFalse(any('tap' in c for c in calls))

    def test_cold_pixel_wake_waits_for_explicit_unlock(self):
        for samples in (
            ['inputRestricted=true dreaming=true', 'inputRestricted=false dreaming=true',
             'inputRestricted=false dreaming=false'],
            ['mKeyguardShowing=true', 'mKeyguardShowing=true', 'mKeyguardShowing=false'],
        ):
            with self.subTest(samples=samples):
                readings=iter(samples);calls=[]
                def runner(args):
                    calls.append(args)
                    return next(readings) if 'policy' in args else '1'
                with patch('device_library.adapter.time.sleep'):
                    result=AndroidAdapter(runner=runner).recover(
                        self.device('physical'),{'serial':'pixel','online':True},'wake')
                self.assertIn('health checked',result['verified'])
                self.assertEqual(sum('policy' in args for args in calls),3)
                self.assertEqual(sum('KEYCODE_WAKEUP' in args for args in calls),1)
                self.assertEqual(sum('dismiss-keyguard' in args for args in calls),1)

    def test_cold_pixel_still_locked_fails_with_bounded_reads(self):
        reads=[]
        def runner(args):
            if 'policy' in args:
                reads.append(args)
                return 'inputRestricted=true dreaming=true'
            return '1'
        with patch('device_library.adapter.time.sleep'):
            with self.assertRaisesRegex(ObservationError,'unlock not verified'):
                AndroidAdapter(runner=runner).recover(
                    self.device('physical'),{'serial':'pixel','online':True},'wake')
        self.assertGreater(len(reads),1)
        self.assertLessEqual(len(reads),21)

    def test_wake_policy_read_receives_remaining_time_budget(self):
        calls=[]
        def run(args, **kwargs):
            calls.append((args,kwargs['timeout']))
            out='inputRestricted=false dreaming=false' if 'policy' in args else '1'
            return subprocess.CompletedProcess(args,0,stdout=out,stderr='')
        with patch('device_library.adapter.subprocess.run',side_effect=run):
            AndroidAdapter().recover(self.device('physical'),{'serial':'pixel','online':True},'wake')
        budgets=[timeout for args,timeout in calls if 'policy' in args]
        self.assertEqual(len(budgets),1)
        self.assertGreater(budgets[0],0)
        self.assertLessEqual(budgets[0],2)

    def test_wake_deadline_stops_before_attempt_limit(self):
        clock=iter([0,0,1.95,2.01]);reads=[]
        def runner(args):
            if 'policy' in args:reads.append(args)
            return 'inputRestricted=true dreaming=true' if 'policy' in args else '1'
        with patch('device_library.adapter.time.monotonic',side_effect=lambda:next(clock)), \
             patch('device_library.adapter.time.sleep') as sleep:
            with self.assertRaisesRegex(ObservationError,'unlock not verified'):
                AndroidAdapter(runner=runner).recover(self.device('physical'),{'serial':'pixel','online':True},'wake')
        self.assertEqual(len(reads),1)
        self.assertAlmostEqual(sleep.call_args.args[0],.05)

    @patch("device_library.adapter.time.sleep")
    def test_keyguard_unknown_or_contradictory_rejected(self, sleep):
        for policy in ('showing=false', 'isStatusBarKeyguard=false mShowingLockscreen=true', 'mKeyguardShowing=false isKeyguardSecure=true'):
            def runner(args):return policy if 'policy' in args else '1'
            with self.assertRaises(ObservationError):AndroidAdapter(runner=runner).recover(self.device('physical'),{'serial':'pixel','online':True},'wake')

    def test_keyguard_explicit_unlocked(self):
        calls=[]
        def runner(args):
            calls.append(args)
            return 'mKeyguardShowing=false' if 'policy' in args else '1'
        AndroidAdapter(runner=runner).recover(self.device('physical'),{'serial':'pixel','online':True},'wake')
        self.assertIn(['adb','-s','pixel','shell','input','keyevent','KEYCODE_WAKEUP'],calls)

    def test_android15_keyguard_delegate_unlocked(self):
        # Pixel 7 / Android 15 window-policy output, captured 2026-10-28.
        policy = '''    mKeyguardOccluded=false mPendingKeyguardOccluded=false
    KeyguardServiceDelegate
      showing=false
      inputRestricted=false
      occluded=false
      trusted=false
      simSecure=false
      dreaming=false
      systemReady=true
      deviceHasKeyguard=true
      enabled=true
      screenState=SCREEN_STATE_ON
      interactiveState=INTERACTIVE_STATE_AWAKE
'''
        def runner(args):
            return policy if 'policy' in args else '1'
        result = AndroidAdapter(runner=runner).recover(
            self.device('physical'), {'serial':'pixel','online':True}, 'wake')
        self.assertIn('health checked', result['verified'])

    @patch("device_library.adapter.time.sleep")
    def test_android15_unknown_locked_dreaming_or_contradictory_rejected(self, sleep):
        policies = (
            '', 'deviceHasKeyguard=false', 'inputRestricted=false', 'dreaming=false',
            'inputRestricted=false dreaming=unknown',
            'inputRestricted=true dreaming=false',
            'inputRestricted=false dreaming=true',
            'inputRestricted=false inputRestricted=true dreaming=false',
            'inputRestricted=false dreaming=false dreaming=true',
            'inputRestricted=false dreaming=false isKeyguardSecure=true',
            'inputRestricted=false dreaming=false mInputRestricted=true',
            'inputRestricted=false dreaming=false mKeyguardShowing=true',
        )
        for policy in policies:
            with self.subTest(policy=policy):
                def runner(args):
                    return policy if 'policy' in args else '1'
                with self.assertRaisesRegex(ObservationError, 'unlock not verified'):
                    AndroidAdapter(runner=runner).recover(
                        self.device('physical'), {'serial':'pixel','online':True}, 'wake')

    def test_each_legacy_keyguard_field_retains_original_verdict(self):
        for field in ('isStatusBarKeyguard', 'mShowingLockscreen',
                      'mKeyguardShowing', 'isKeyguardShowing'):
            for value in ('true', 'false'):
                with self.subTest(field=field, value=value):
                    def runner(args):
                        return f'{field}={value}' if 'policy' in args else '1'
                    adapter = AndroidAdapter(runner=runner)
                    if value == 'false':
                        adapter.recover(self.device('physical'), {'serial':'pixel','online':True}, 'wake')
                    else:
                        with self.assertRaisesRegex(ObservationError, 'unlock not verified'):
                            adapter.recover(self.device('physical'), {'serial':'pixel','online':True}, 'wake')

    def test_timeout_is_unknown_and_empty_avd_rejected(self):
        from unittest.mock import patch
        with patch('device_library.adapter.subprocess.run', side_effect=subprocess.TimeoutExpired(['fake-adb'],90)):
            with self.assertRaisesRegex(ObservationError,'unknown'):AndroidAdapter._run(['fake-adb'])
        def runner(args):
            if args==['adb','devices','-l']:return 'List of devices attached\nemulator-5554 device transport_id:1\n'
            return ''
        with self.assertRaisesRegex(ObservationError,'empty'):AndroidAdapter(runner=runner).observe([])

    def test_lifecycle_start_binds_sdk_avd_roots(self):
        calls=[];a=AndroidAdapter(runner=lambda args:calls.append(args))
        with tempfile.TemporaryDirectory() as directory:
            avd=Path(directory).resolve()/'Test.avd';avd.mkdir()
            (avd/'config.ini').write_text('AvdId=Test\n')
            (avd.parent/'Test.ini').write_text(f'path={avd}\n')
            a.start({'spec':{'avd_name':'Test','avd_path':str(avd),'avd_identity':avd_identity(avd),'sdk_root':'/fixture/sdk'}})
            self.assertEqual(calls[0],['/usr/bin/env','ANDROID_AVD_HOME='+str(avd.parent),'ANDROID_HOME=/fixture/sdk','ANDROID_SDK_ROOT=/fixture/sdk','android','emulator','start','Test'])

    def test_inventory_process_transport_mismatch_is_unknown(self):
        def runner(args):
            if args==['adb','devices','-l']:return 'List of devices attached\n'
            if args==['ps','-axo','pid=,comm=']:return '123 /sdk/emulator/emulator'
            return '123 /sdk/emulator/emulator -avd Test'
        with self.assertRaises(ObservationError):AndroidAdapter(runner=runner).observe([])

    def test_inventory_ignores_emulator_words_in_non_emulator_arguments(self):
        def runner(args):
            if args==['adb','devices','-l']:return 'List of devices attached\n'
            if args==['ps','-axo','pid=,command=']:
                return '123 node agent --prompt inspect emulator now'
            if args==['ps','-axo','pid=,comm=']:return '123 node'
            self.fail(f'non-emulator process must not be inspected: {args}')
        result=AndroidAdapter(runner=runner).observe([])
        self.assertTrue(result['complete'])
        self.assertEqual(result['instances'],[])

    def test_real_emulator_executable_without_avd_remains_unknown(self):
        def runner(args):
            if args==['adb','devices','-l']:return 'List of devices attached\n'
            if args==['ps','-axo','pid=,comm=']:return '123 /sdk/emulator/qemu/qemu-system-aarch64'
            return '123 /sdk/emulator/qemu/qemu-system-aarch64 -no-window'
        with self.assertRaisesRegex(ObservationError,'identity uncertain'):
            AndroidAdapter(runner=runner).observe([])


class ProcessTests(unittest.TestCase):
    def test_invalid_pid_never_proves_exit(self):
        for pid in (True,None,0,1,-1,2**63,'12'):
            with self.assertRaises(process.ProcessUnknown):process.identity(pid)

    def test_real_process_birth_identity_and_exit(self):
        owner=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
        try:
            birth=process.identity(owner.pid)
            self.assertTrue(birth)
            self.assertEqual(process.identity(owner.pid),birth)
            saved={'pid':owner.pid,'identity':birth}
            self.assertEqual(process.state(saved),'alive')
            with patch('device_library.process.identity',return_value=birth+'different'):
                self.assertEqual(process.state(saved),'exited')
            with patch('device_library.process.identity',side_effect=process.ProcessUnknown('denied')):
                self.assertEqual(process.state(saved),'unknown')
            owner.terminate();owner.wait(5)
            self.assertEqual(process.state(saved),'exited')
        finally:
            if owner.poll() is None:owner.terminate();owner.wait(5)


class SocketTests(unittest.TestCase):
    def setUp(self):
        # Socket pathname limit on macOS: use short external scratch paths.
        self.temp=tempfile.TemporaryDirectory(prefix='dl-',dir='/tmp')
        self.root=Path(self.temp.name);self.runtime=self.root/'r';self.data=self.root/'d'
        self.fixture=self.root/'f';self.fixture.write_text('{}');self.proc=None
        self.start(True)

    def tearDown(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:self.proc.wait(8)
            except subprocess.TimeoutExpired:self.proc.kill();self.proc.wait()
        if self.proc:
            self.proc.communicate()
        self.temp.cleanup()

    def start(self, init=False):
        if self.proc and self.proc.poll() is not None:
            self.proc.communicate()
        args=[sys.executable,'-m','device_library','--test-runtime',str(self.runtime),'serve','--data-dir',str(self.data),'--fake-fixture',str(self.fixture)]
        if init:args+=['--init']
        self.proc=subprocess.Popen(args,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        for _ in range(100):
            if self.proc.poll() is not None:self.fail(self.proc.communicate())
            if (self.runtime/'coordinator.sock').exists():return
            time.sleep(.05)
        self.fail('socket not ready')

    def rpc(self, command,args=None,token=''):
        return call(command,args,token,self.runtime)

    def cli(self,*args, credential=None):
        command=[sys.executable,'-m','device_library','--test-runtime',str(self.runtime)]
        if credential:command+=['--credential-file',str(credential)]
        p=subprocess.run(command+list(args),cwd=ROOT,capture_output=True,text=True,timeout=10)
        return p.returncode,json.loads(p.stdout)

    def test_cli_real_owner_exit_automatically_returns_emulator(self):
        admin=(self.runtime/'operator.credential').read_text()
        spec={'avd_name':'fixture','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        self.rpc('register',{'kind':'emulator','alias':'fixture','spec':spec},admin)
        owner=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
        try:
            credential=self.root/'owner.token'
            code,out=self.cli('borrow','--kind','emulator','--borrower','b','--session','s',
                              '--request-key','owned','--owner-pid',str(owner.pid),'--wait','3',credential=credential)
            self.assertEqual(code,0,out)
            lease=out['result']['lease'];lid=lease['id']
            self.assertGreater(lease['remaining_seconds'],1795)
            self.assertEqual(lease['owner_process']['pid'],owner.pid)
            owner.terminate();owner.wait(5)
            for _ in range(60):
                result=self.cli('lease','show',lid,credential=credential)[1]['result']
                if result['state']=='closed':break
                time.sleep(.1)
            self.assertEqual(result['state'],'closed',result)
            inventory=self.rpc('devices')['result']
            self.assertEqual(inventory['devices'][0]['state'],'available')
            self.assertEqual(inventory['claims'],[])
        finally:
            if owner.poll() is None:owner.terminate();owner.wait(5)

    def test_cli_lifecycle_restart_backup_restore(self):
        admin=(self.runtime/'operator.credential').read_text()
        spec={'avd_name':'fixture','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        self.assertTrue(self.rpc('register',{'kind':'emulator','alias':'fixture','spec':spec},admin)['ok'])
        credential=self.root/'borrower.token'
        command=('borrow','--kind','emulator','--borrower','independent','--session','s','--request-key','key','--wait','3')
        code,out=self.cli(*command,credential=credential)
        self.assertEqual(code,0,out);lid=out['result']['lease']['id']
        self.assertEqual(out['result']['lease']['owner_process']['pid'],os.getpid())
        self.assertEqual(credential.stat().st_mode&0o777,0o600)
        again=self.cli(*command,credential=credential)[1]
        self.assertEqual(lid,again['result']['lease']['id'])
        self.assertTrue(self.cli('lease','renew',lid,credential=credential)[1]['ok'])
        backup=self.root/'backup.sqlite3'
        self.assertTrue(self.rpc('backup',{'path':str(backup)},admin)['ok'])
        self.proc.terminate();self.proc.wait(8);self.start()
        code,out=self.cli('lease','renew',lid,credential=credential);self.assertEqual(code,4,out)
        code,out=self.cli('return',lid,credential=credential)
        self.assertEqual(code,3,out)
        self.assertEqual(out['result']['next_action']['state'],'stopped')
        h=out['result']
        result=self.cli('handoff','acknowledge',h['id'],'--state','stopped',credential=credential)[1]
        self.assertEqual(result['result']['state'],'closed',result)
        self.proc.terminate();self.proc.wait(8)
        result=restore(self.data,backup,runtime=self.runtime)
        self.assertEqual(result['state'],'restored_quarantined')
        self.start();doctor=self.rpc('doctor');self.assertTrue(doctor['ok'])
        devices=self.rpc('devices')['result']['devices'];self.assertEqual(devices[0]['state'],'quarantined')

    def test_restore_corrupt_target_preserves_bytes_and_sidecars(self):
        admin=(self.runtime/'operator.credential').read_text()
        backup=self.root/'good.sqlite3'
        self.assertTrue(self.rpc('backup',{'path':str(backup)},admin)['ok'])
        self.proc.terminate();self.proc.wait(8)
        target=self.data/'ledger.sqlite3';bad=b'not a sqlite ledger; preserve me'
        target.write_bytes(bad)
        sidecar=Path(str(target)+'-journal');sidecar.write_bytes(b'old journal bytes')
        restore(self.data,backup,runtime=self.runtime)
        saved=list(self.data.glob('ledger.before-restore.*.sqlite3'))
        self.assertTrue(any(p.read_bytes()==bad for p in saved))
        self.assertFalse(sidecar.exists())
        self.assertTrue(any(p.read_bytes()==b'old journal bytes' for p in self.data.glob('ledger.before-restore-sidecar.*-journal')))
        self.start();self.assertTrue(self.rpc('doctor')['ok'])

    def test_cli_explicit_stopped_return_closes_without_manual_handoff(self):
        admin=(self.runtime/'operator.credential').read_text()
        spec={'avd_name':'fixture','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        self.rpc('register',{'kind':'emulator','alias':'fixture','spec':spec},admin)
        credential=self.root/'borrower.token'
        code,out=self.cli('borrow','--kind','emulator','--borrower','b','--session','s',
                          '--request-key','key','--wait','3',credential=credential)
        self.assertEqual(code,0,out)
        lid=out['result']['lease']['id']
        code,out=self.cli('return',lid,'--stopped','--wait','3',credential=credential)
        self.assertEqual(code,0,out)
        self.assertEqual(out['result']['state'],'closed')
        devices=self.rpc('devices')['result']
        self.assertEqual(devices['devices'][0]['state'],'available')
        self.assertEqual(devices['claims'],[])

    def test_retire_cli_requires_operator_and_stopped_attestation(self):
        admin=(self.runtime/'operator.credential').read_text()
        spec={'avd_name':'fixture','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        d=self.rpc('register',{'kind':'emulator','alias':'fixture','spec':spec},admin)['result']
        args={'id':d['id'],'reason':'storage recreated','stopped':True}
        self.assertFalse(self.rpc('retire',args)['ok'])
        code,out=self.cli('devices','retire',d['id'],'--reason','storage recreated','--old-client-stopped',credential=self.runtime/'operator.credential')
        self.assertEqual(code,0,out);self.assertEqual(out['result']['state'],'retired')
        new=self.rpc('register',{'kind':'emulator','alias':'fixture','spec':spec},admin)['result']
        self.assertNotEqual(new['id'],d['id'])

    def test_singleton_different_data_and_auth(self):
        with self.assertRaisesRegex(Fault,'already_running'):
            Server(self.root/'other',initialize=True,test_runtime=self.runtime,fixture=self.fixture)
        self.assertFalse(self.rpc('register',{'kind':'emulator','alias':'bad','spec':{}},'')['ok'])
        self.assertFalse((self.root/'other').exists())
        self.assertEqual(self.rpc('doctor')['result']['adapter'],'FakeAdapter')

    def test_delete_cli_auth_policy_audit_and_repeat(self):
        credential = self.runtime/'operator.credential'
        admin = credential.read_text()
        spec = {'avd_name':'fixture','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        d = self.rpc('register', {'kind':'emulator','alias':'obsolete','spec':spec}, admin)['result']
        code, out = self.cli('devices', 'delete', d['id'])
        self.assertEqual((code, out['reason']), (4, 'operator_credential_required'))
        code, out = self.cli('devices', 'delete', d['id'], credential=credential)
        self.assertEqual((code, out['reason']), (5, 'deletion_requires_retired'))
        code, out = self.cli('devices', 'delete', credential=credential)
        self.assertEqual((code, out['reason']), (2, 'deletion_requires_id'))
        self.rpc('retire', {'id':d['id'],'reason':'obsolete','stopped':True}, admin)
        code, out = self.cli('devices', 'delete', d['id'], credential=credential)
        self.assertEqual(code, 0, out)
        self.assertEqual(out['schema_version'], 1)
        self.assertTrue(out['ok'])
        self.assertEqual(out['result'], {'state':'deleted','id':d['id']})
        self.assertEqual(self.rpc('devices')['result']['devices'], [])
        self.assertEqual(self.rpc('devices')['result']['claims'], [])
        code, audit = self.cli('audit', 'export', '--device', d['id'])
        self.assertEqual(code, 0, audit)
        event = [e for e in audit['result']['events'] if e['type']=='device_deleted'][0]
        self.assertEqual(event['payload']['registration']['spec'], spec)
        self.assertEqual(event['payload']['registration']['alias'], 'obsolete')
        self.assertEqual(event['payload']['actor'], {'uid':os.getuid(),'role':'operator'})
        code, out = self.cli('devices', 'delete', d['id'], credential=credential)
        self.assertEqual((code, out['reason']), (2, 'device_not_found'))

    def test_operation_end_cli_auth_reason_and_attestation(self):
        credential = self.runtime/'operator.credential'
        admin = credential.read_text()
        spec = {'avd_name':'fixture','avd_path':'fixture','avd_identity':'fixture','sdk_root':'fixture'}
        self.rpc('register', {'kind':'emulator','alias':'fixture','spec':spec}, admin)
        borrower = self.root/'borrower.token'
        code, out = self.cli('borrow','--kind','emulator','--borrower','b','--session','s',
                             '--request-key','key','--wait','3',credential=borrower)
        self.assertEqual(code, 0, out)
        lid = out['result']['lease']['id']
        code, out = self.cli('return', lid, credential=borrower)
        self.assertEqual(code, 3, out)
        op = out['result']['operation']
        # Operator credential and a reason are both mandatory.
        code, out = self.cli('operation', 'end', op, '--reason', 'verified ended')
        self.assertEqual((code, out['reason']), (4, 'operator_credential_required'))
        code, out = self.cli('operation', 'end', op, credential=credential)
        self.assertEqual((code, out['reason']), (2, 'operation_end_reason_required'))
        # The return operation is prepared at the current fence: refused without
        # the old-client-stopped attestation, attestable with it.
        code, out = self.cli('operation', 'end', op, '--reason', 'no evidence', credential=credential)
        self.assertEqual((code, out['reason']), (5, 'current_fence_operation_requires_old_client_stopped'))
        code, out = self.cli('operation', 'end', op, '--reason', 'borrower gone; verified nothing running',
                             '--old-client-stopped', credential=credential)
        self.assertEqual(code, 0, out)
        self.assertEqual(out['result']['state'], 'operator_verified_ended')
        shown = self.cli('operation', 'show', op)[1]
        self.assertEqual(shown['result']['state'], 'operator_verified_ended')

    def test_socket_borrow_requires_explicit_verifiable_owner(self):
        args={'borrower':'b','session':'s','key':'k','body':{'kind':'emulator'}}
        result=self.rpc('borrow',args,'t'*32)
        self.assertEqual(result['reason'],'borrow_requires_owner_pid')
        result=self.rpc('borrow',{**args,'owner_pid':True},'t'*32)
        self.assertEqual(result['reason'],'borrow_requires_owner_pid')

    def test_fake_runtime_cannot_use_real_adapter(self):
        with self.assertRaisesRegex(Fault,'requires_fake'):Server(self.root/'other',test_runtime=self.root/'other-runtime')

    def test_malformed_nonobject_does_not_kill_daemon(self):
        import socket
        with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
            s.connect(str(self.runtime/'coordinator.sock'));s.sendall(b'[]\n');result=json.loads(s.recv(4096))
        self.assertFalse(result['ok']);self.assertTrue(self.rpc('doctor')['ok'])


if __name__=='__main__':unittest.main()
