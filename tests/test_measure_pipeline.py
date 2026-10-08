import asyncio
from dataclasses import asdict, replace
import json
import threading
import time
import pytest

from pairbot.config import Config
from pairbot.engine import Engine
from pairbot.feed import Journal, collect, read_entries
from pairbot.storage import StorageWorker, StorageBackpressure, Telemetry, ReportSnapshot
from pairbot.__main__ import load_engine, write_report
from tests.test_engine import T, M, engine, book, trade
from tests.test_recovery import FakeAPI, FakeSocket, Context


def test_seconds_integrate_activation_and_cancellation_boundary():
    e=engine()
    e.advance(T+1);e._activate()
    e.advance(T+2)
    e.cancel_all()
    e.advance(T+3)
    row=e.measurements.markets['c']
    assert row['observed_seconds']==3
    assert row['live_order_seconds']=={'u':1.5,'d':1.5}


def test_time_weighted_edge_excludes_stale_interval():
    e=engine()
    e.advance(T+20)
    r=e.measurements.markets['c']
    assert r['fresh_bid_seconds']==pytest.approx(5)
    assert r['edge_002_seconds']==pytest.approx(5)


def test_complement_candidate_observed_but_cannot_fill():
    e=engine()
    e.frame(trade('d',T+1,100,'BUY',.52),T+1)
    assert e.fills==0
    row=e.measurements.buy_prints[0]
    # Queue activation in the current message occurs after observational hook.
    e.frame(trade('d',T+2,100,'BUY',.52),T+2)
    row=e.measurements.buy_prints[-1]
    assert row['opposite_live'] and row['eligible_price'] and row['after_anchor']
    assert e.orders['u'].queue==15
    e.frame(book('u',T+3,size=2),T+3)
    assert row['next_depth']['size']==2
    assert e.orders['u'].queue==15
    assert e.fills==0


def test_above_limit_depth_change_does_not_deplete_queue():
    e=engine();e.advance(T+1);e._activate()
    e.frame(trade('u',T+2,100,'SELL',.49),T+2)
    row=e.measurements.above_limit[-1]
    e.frame(book('u',T+3,size=0),T+3)
    assert row['queue_before']==row['queue_after']==15
    assert row['consumed_by_model']==0
    assert e.fills==0


def test_complement_switch_requires_proof():
    assert not Config().complementary_fills
    with pytest.raises(ValueError,match='not proven'):
        Config(complementary_fills=True)


def test_measurement_receive_clock_is_separate_from_processing():
    e=engine()
    e.frame(book('u',T+1),T+8,received_at=T+1.1)
    lag=e.measurements.latency['book'][-1]
    assert lag==pytest.approx(.1)
    assert not e.fresh()
    e.frame(trade('u',T+1,100),T+8.1,received_at=T+1.2)
    assert e.fills==0 and e.trade_blocks['stale_print']==1


def test_slow_io_worker_does_not_block_event_loop(tmp_path):
    started=threading.Event();release=threading.Event()
    j=Journal(tmp_path/'j')
    j.write('header',0,schema=1,config=asdict(Config()),source='SYNTHETIC_TEST')
    worker=StorageWorker(j)
    def slow():
        started.set();release.wait(2)
    async def run():
        worker.submit(slow)
        await asyncio.to_thread(started.wait,1)
        ticks=[]
        for _ in range(5):
            ticks.append(time.monotonic());await asyncio.sleep(.005)
        assert len(ticks)==5 and not release.is_set()
        worker.write('market',T,market=M.data())
        worker.write('frame',T,message=book('u',T))
        release.set();await worker.finish()
    asyncio.run(run())
    assert len(list(read_entries(tmp_path/'j')))==3
    assert load_engine(tmp_path/'j').cash==50


def test_io_overflow_is_explicit_and_emergency_evidence_survives(tmp_path):
    j=Journal(tmp_path/'j');worker=StorageWorker(j,capacity=1)
    entered=threading.Event();release=threading.Event()
    def slow():entered.set();release.wait(2)
    async def run():
        worker.submit(slow);await asyncio.to_thread(entered.wait,1)
        worker.write('tick',T)
        with pytest.raises(StorageBackpressure,match='no silent drops'):
            worker.write('frame',T,message=book('u',T))
        worker.write('gap',T,reason='io_queue_full')
        release.set();await worker.finish()
    asyncio.run(run())
    assert [r['type'] for r in read_entries(tmp_path/'j')]==['tick','gap']


def test_snapshot_has_no_shared_mutable_report_or_audit():
    e=engine();snapshot=ReportSnapshot(e)
    before=json.dumps(snapshot.report(),sort_keys=True)
    e.advance(T+1);e.frame(trade('u',T+1,100),T+1)
    assert json.dumps(snapshot.report(),sort_keys=True)==before
    assert snapshot.audit is not e.audit


def test_worker_rotation_integrity_and_timing_evidence(tmp_path):
    j=Journal(tmp_path/'j');worker=StorageWorker(j)
    async def run():
        worker.write('header',0,schema=1,config=asdict(Config()),source='SYNTHETIC_TEST')
        worker.write('market',T,market=M.data())
        worker.submit(j._rotate)
        worker.write('frame',T,message=book('u',T))
        await worker.finish()
    asyncio.run(run())
    assert len(list(read_entries(tmp_path/'j')))==3
    manifest=json.loads((tmp_path/'j'/'journal.json').read_text())
    assert manifest['active_shard'] is None and len(manifest['sealed'])==2
    timings=[json.loads(x) for x in (tmp_path/'j'/'io-timings.jsonl').read_text().splitlines()]
    assert {'fsync','sha256'} <= {x['operation'] for x in timings}


def test_default_replay_measurements_deterministic(tmp_path):
    j=Journal(tmp_path/'j')
    j.write('header',0,schema=1,config=asdict(Config()),source='SYNTHETIC_TEST')
    j.write('market',T,market=M.data())
    for t,msg in [(T,book('u',T)),(T,book('d',T)),(T+1,trade('u',T+1,100))]:
        j.write('frame',t,message=msg)
    j.close()
    assert load_engine(tmp_path/'j').report()==load_engine(tmp_path/'j').report()


def test_trade_before_queue_anchor_still_cannot_fill():
    e=engine();e.advance(T+2);e._activate()
    e.frame(trade('u',T+1,100),T+2.1)
    assert e.fills==0 and e.trade_blocks['before_queue_or_stale']==1


def test_warmup_switch_default_off_and_research_only():
    assert Config().guard_mode=='up_mid' and Config().guard_warmup_seconds==0
    e=engine(guard_mode='combined_bid',guard_warmup_seconds=30)
    e.frame(book('u',T+2,.30,.40),T+2)
    assert not e.movement_active


def test_socket_keeps_reading_during_slow_report(tmp_path):
    origin=time.monotonic();reads=[];intervals=[]
    clock=lambda:T+60+(time.monotonic()-origin)*100
    mono=lambda:(time.monotonic()-origin)*100
    class Socket(FakeSocket):
        async def recv(self):
            wire=await super().recv();reads.append(time.monotonic());return wire
    class Conn(Context):
        def __init__(self,c):self.ws=Socket(c)
    def checkpoint(snapshot):
        start=time.monotonic();time.sleep(.07);intervals.append((start,time.monotonic()))
        write_report(snapshot,tmp_path/'j',quiet=True)
    j=Journal(tmp_path/'j');e=Engine(replace(Config(),cancel_latency_seconds=0))
    asyncio.run(collect(e,j,1,api=FakeAPI(clock),connector=lambda *a,**kw:Conn(clock),
                        wall_clock=clock,monotonic=mono,snapshot_checkpoint=checkpoint))
    assert intervals and any(a<ts<b for a,b in intervals for ts in reads)
    assert e.run_info['requested_duration_completed']


def test_worker_failure_is_visible_and_finalized(tmp_path):
    j=Journal(tmp_path/'j');worker=StorageWorker(j)
    def fail():raise OSError('disk full fixture')
    async def run():
        worker.submit(fail)
        with pytest.raises(RuntimeError,match='results incomplete'):
            await worker.finish()
    asyncio.run(run())
    assert not worker.thread.is_alive()


def test_receive_queue_overflow_is_logged_and_invalidates_data(tmp_path):
    origin=time.monotonic()
    clock=lambda:T+60+(time.monotonic()-origin)*100
    mono=lambda:(time.monotonic()-origin)*100
    class Socket(FakeSocket):
        async def recv(self):
            await asyncio.sleep(.001)
            msg=book(self.tokens[0],clock());msg['market']=self.tokens[0][1:]
            return json.dumps([msg]*5)
    class Conn(Context):
        def __init__(self,c):self.ws=Socket(c)
    e=Engine(replace(Config(),cancel_latency_seconds=0));j=Journal(tmp_path/'j')
    try:
        asyncio.run(collect(e,j,.02,api=FakeAPI(clock),connector=lambda *a,**kw:Conn(clock),
                            wall_clock=clock,monotonic=mono,receive_capacity=1))
    except RuntimeError as exc:
        assert 'No live frames' in str(exc)
    assert e.fills==0
    assert e.report()['execution_quality'] in {'NO_DATA','INCOMPLETE_DATA'}
    assert e.gaps>0 and e.run_info['receive_not_enqueued']>0
    assert 'Receive queue full' in (tmp_path/'j'/'errors.jsonl').read_text()
    assert j.file is None


def test_new_research_settings_cannot_enter_live_collector(tmp_path):
    e=engine(guard_warmup_seconds=30)
    j=Journal(tmp_path/'j')
    try:
        with pytest.raises(ValueError,match='offline replay only'):
            asyncio.run(collect(e,j,1))
    finally:
        j.close()
