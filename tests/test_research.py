"""Research unit fixtures, never market evidence or invented live executions."""
import asyncio
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import pytest
from pairbot.config import Config
from pairbot.engine import Engine
from pairbot.feed import read_entries
from pairbot.research_capture import clock_sample, reference_metadata, record, market_messages
from pairbot.research_engine import ResearchEngine
from pairbot.research import (Opportunities, BoundedMeasurements, markouts,
    selected_entries, train_model, evaluate, market_index)
from pairbot.research_stats import (FeeSchedule, acceptance, bootstrap_ci, calibration,
    chronological_split, claim_holdout, load_protocol, path_integral,
    realized_volatility, up_probability, upper_opportunity)
from .test_engine import M, T, book, trade

PROTOCOL,_=load_protocol('docs/research/protocol.json')
FEE=FeeSchedule(.07,provenance='unit fixture only')


def research(kind='fair_maker',bound='conservative',**cfg):
    e=ResearchEngine({'id':'test','kind':kind,'edge':.02},PROTOCOL,bound)
    e.cfg=replace(e.cfg,**cfg)
    e.select(M,T)
    e.fee_schedule=FEE
    e.feature(.8,T)
    e.frame(book('u',T),T)
    e.frame(book('d',T),T)
    return e


def test_fee_current_formula_rounding_and_maker_zero():
    assert FEE.fee(100,.5)==1.75
    assert FEE.fee(100,.3)==1.47
    assert FEE.fee(100,.7)==1.47
    assert FEE.fee(100,.5,maker=True)==0
    assert FEE.fee(.000001,.01)==0


@pytest.mark.parametrize('raw',[{}, {'feesEnabled':True},
    {'feesEnabled':True,'feeSchedule':{'rate':.07,'exponent':2,'takerOnly':True}}])
def test_fee_missing_or_old_curve_rejected(raw):
    with pytest.raises((ValueError,KeyError)):
        FeeSchedule.from_gamma(raw)


def test_explicit_fee_free_market():
    assert FeeSchedule.from_gamma({'feesEnabled':False}).fee(5,.5)==0


def test_upper_bounds_depth_fees_and_unknown_cost():
    result=upper_opportunity('taker_pair',[.45,.45],[2,7],FEE)
    assert result['quantity']==2
    assert result['net_usd_bound']==pytest.approx(.1307)
    assert result['qualifies']
    assert result['bound']=='UNKNOWN_OPERATION_COST_UPPER_BOUND'
    assert not upper_opportunity('taker_pair',[.49,.49],[5,5],FEE)['qualifies']
    assert upper_opportunity('mint_sell',[.56,.56],[5,6],FEE)['qualifies']
    assert not upper_opportunity('mint_sell',[.56,.56],[5,6],FEE,operation_cost=1)['qualifies']


def test_final_twap_probability_not_barrier():
    assert up_probability(100,100,120,.001)==pytest.approx(.5)
    assert up_probability(101,100,120,.001)>.5
    assert up_probability(99,100,120,.001)<.5
    assert up_probability(100,100,20,.001) is None
    assert up_probability(100,100,0,.001,known_integral=6000)==1
    assert up_probability(100,100,0,.001,known_integral=5999)==0
    assert up_probability(100,100,20,.001,known_integral=4100)>.9


def test_volatility_causal_and_insufficient_data():
    points=[(i,100+(i%2)) for i in range(40)]
    before=realized_volatility(points,39)
    assert before>0
    assert before==realized_volatility(points+[(1000,10000)],39)
    assert realized_volatility(points[:10],39) is None


def test_twap_path_does_not_bridge_gaps():
    assert path_integral([(0,100),(2,101),(4,99)],1,5)==pytest.approx(401)
    assert path_integral([(0,100),(20,101)],1,20) is None
    assert path_integral([(2,100)],1,3) is None


def test_calibration_weights_windows_not_message_counts():
    samples=[{'window':'a','model':.5,'winner':1}]*100+[{'window':'b','model':0,'winner':0}]
    result=calibration(samples)
    assert result['brier']==.125
    assert result['windows']==2
    assert sum(x['window_weight'] for x in result['reliability'])==pytest.approx(2)


def test_bootstrap_reproducibility_and_empty_sample():
    a=bootstrap_ci([-.2,.1,.4],replicates=100)
    assert a==bootstrap_ci([-.2,.1,.4],replicates=100)
    assert bootstrap_ci([],replicates=100)['lower'] is None
    assert bootstrap_ci([0]*20,replicates=100)['lower']==0
    assert bootstrap_ci([1]*20,replicates=100,block=3)['lower']==1


def test_protocol_hash_guard(tmp_path):
    path=tmp_path/'protocol.json'
    path.write_text('{}')
    path.with_suffix('.sha256').write_text(hashlib.sha256(b'{}').hexdigest())
    assert load_protocol(path)[0]=={}
    path.write_text('{"changed":1}')
    with pytest.raises(ValueError,match='hash mismatch'):
        load_protocol(path)


def test_chronological_split_and_single_use_claim(tmp_path):
    windows=[{'condition_id':str(i),'start':i} for i in reversed(range(8))]
    split=chronological_split(windows)
    assert [x['start'] for x in split['train']]==[0,1,2,3]
    assert [x['start'] for x in split['validation']]==[4,5]
    assert [x['start'] for x in split['test']]==[6,7]
    path=tmp_path/'claim.json'
    claim_holdout(path,'input','protocol')
    with pytest.raises(FileExistsError):
        claim_holdout(path,'input','different_protocol')


def test_acceptance_rejects_optimistic_incomplete_unknown_cost():
    row={'net_pnl':1,'pairs':279,'pair_windows':279,'ci':{'lower':.01},
         'block_ci':{'lower':.01},'max_drawdown':1,'quality':'COMPLETE_DATA',
         'costs_verified':True,'bound':'conservative','split':'test'}
    assert acceptance(row,PROTOCOL)['decision']=='GO'
    for change in ({'bound':'optimistic_upper'},{'costs_verified':False},
                   {'quality':'INCOMPLETE_DATA'},{'pair_windows':278}):
        assert acceptance({**row,**change},PROTOCOL)['decision']=='NO-GO'


def test_maker_one_fair_leg_no_touch_fill():
    e=research()
    assert list(e.orders)==['u']
    e.frame(book('u',T+1,bid=.48,ask=.48),T+1)
    assert e.fills==0 and e.cash==50


def test_strict_sell_anchor_and_queue():
    e=research(kill_move=.9)
    e.frame(book('u',T+1),T+1)
    anchor=e.orders['u'].queue_at
    assert e.orders['u'].queue==15
    e.frame(trade('u',T+.1,100),T+1.1)
    assert e.fills==0 and e.orders['u'].queue==15
    e.frame(trade('u',T+1.2,100,side='BUY'),T+1.2)
    assert e.fills==0
    e.frame(trade('u',T+1.3,20),T+1.3)
    assert e.fills==1 and e.positions['u'].size==5
    fill=next(x for x in e.audit if x['type']=='fill')
    assert fill['queue_anchor']==anchor
    assert fill['reason']=='observed_sell_print_exceeds_queue'


def test_delayed_data_cancels_without_fill():
    e=research()
    e.frame(book('u',T+1),T+1)
    e.frame(trade('u',T+1.1,100),T+8)
    assert e.fills==0 and e.delayed_frames>0
    assert all(o.cancel_at is not None for o in e.orders.values())


def test_optimistic_explicit_still_requires_print():
    e=research(bound='optimistic_upper')
    e.frame(book('u',T+1),T+1)
    assert e.cfg.queue_multiplier==1
    e.frame(trade('u',T+1.1,10),T+1.1)
    assert e.fills==0
    e.frame(trade('u',T+1.2,5),T+1.2)
    assert e.fills==1 and Config().queue_multiplier==1.5


def test_completion_rechecks_breakeven_at_arrival():
    e=research(kill_move=.9)
    e.orders.clear()
    e.positions['u'].size=5
    e.positions['u'].cost=1.5
    e.cash=48.5
    e.frame(book('d',T+1,bid=.5,ask=.6),T+1)
    assert e.taker_job is not None
    e.frame(book('d',T+1.6,bid=.6,ask=.7),T+1.6)
    assert e.positions['d'].size==0
    assert any(x['type']=='taker_aborted' and x['reason']=='arrival_breakeven' for x in e.audit)


def test_s3_sequential_arrival_can_leave_single_leg():
    e=ResearchEngine({'id':'S3','kind':'taker_opportunity','edge':.02},PROTOCOL)
    e.cfg=replace(e.cfg,kill_move=.9)
    e.select(M,T)
    e.fee_schedule=FEE
    e.frame(book('u',T,bid=.42,ask=.45),T)
    e.frame(book('d',T,bid=.42,ask=.45),T)
    assert e.taker_job is None and e.fills==0
    e.frame(book('u',T+.6,bid=.42,ask=.45),T+.6)
    assert e.taker_job and e.fills==0
    e.frame(book('u',T+1.2,bid=.42,ask=.45),T+1.2)
    assert e.fills==1 and e.positions['u'].size==5
    e.frame(book('d',T+1.8,bid=.42,ask=.55),T+1.8)
    assert e.positions['d'].size==0 and e.merges==0
    assert any(x['type']=='taker_aborted' for x in e.audit)


def test_s4_time_filter_combined_guard():
    e=research('filtered_fair_maker')
    assert not e.orders and e.cfg.guard_mode=='combined_mid'
    e.frame(book('u',T+31),T+31)
    e.frame(book('d',T+31),T+31)
    e.feature(.8,T+31)
    e.quote()
    assert list(e.orders)==['u']


def test_dynamic_edge_spread_volatility():
    e=research('dynamic_maker')
    low=e.edge('u')
    e.prob_vol=.1
    assert e.edge('u')>low
    assert all(o.price<=.48 for o in e.orders.values())


def test_markouts_do_not_bridge_gap_or_missing_horizon():
    points={'c':[{'ts':T,'mid_up':.5,'mid_down':.5,'gaps':0},
                 {'ts':T+5,'mid_up':.4,'mid_down':.6,'gaps':0},
                 {'ts':T+20,'mid_up':.3,'mid_down':.7,'gaps':1}]}
    fill={'ts':T,'type':'fill','condition_id':'c','outcome':'Up','price':.48}
    result=markouts([fill],points)[0]
    assert result['markout_5']==pytest.approx(-.1)
    assert result['value_vs_fill_5']==pytest.approx(-.08)
    assert result['markout_20'] is None and result['markout_60'] is None


def test_clock_bounds_do_not_change_five_second_veto():
    sample=clock_sample(100,98,99)
    assert sample['offset_server_minus_local']==1.5
    assert sample['uncertainty_seconds']==1
    assert sample['integer_time_offset_interval']==[1,3]
    assert Config().stale_seconds==5
    with pytest.raises(ValueError):
        clock_sample(100,100,99)


def test_reference_no_spot_substitution():
    assert reference_metadata({'lastTradePrice':.5})['opening_price'] is None
    raw={'events':[{'id':'a','eventMetadata':{'priceToBeat':'100'}}],
         'resolutionSource':'chainlink','cryptoMarketConfig':{'twapEnabled':True}}
    assert reference_metadata(raw)['opening_price']==100


def test_model_gate_stops_missing_data():
    result=train_model({'samples':{}},{'samples':{}},PROTOCOL)
    assert result['chosen_scale'] is None
    assert result['status']=='STOPPED_INSUFFICIENT_MODEL_DATA'


def test_final_rows_not_exposed(tmp_path):
    rows=[{'type':'market','ts':1,'market':{'condition_id':'train'}},
          {'type':'frame','ts':2,'message':{'secret':'train'}},
          {'type':'market','ts':3,'market':{'condition_id':'test'}},
          {'type':'frame','ts':4,'message':{'secret':'test'}},
          {'type':'settlement','ts':5,'condition_id':'test','winner':'hidden'},
          {'type':'settlement','ts':6,'condition_id':'train','winner':'visible'}]
    path=tmp_path/'events.jsonl'
    path.write_text(''.join(json.dumps(x)+'\n' for x in rows))
    result=list(selected_entries(path,{'train'}))
    assert len(result)==3
    assert all('hidden' not in json.dumps(x) and 'test' not in json.dumps(x) for x in result)


def test_bounded_measurement_preserves_execution():
    a,b=Engine(Config()),Engine(Config())
    b.measurements=BoundedMeasurements()
    for e in (a,b):
        e.select(M,T)
        e.frame(book('u',T),T)
        e.frame(book('d',T),T)
        e.frame(trade('u',T+1,20),T+1)
        e.frame(trade('d',T+2,20),T+2)
        e.advance(T+6)
    for field in ('cash','fills','merges','merge_pnl','max_drawdown'):
        assert getattr(a,field)==getattr(b,field)


def test_opportunity_continuity_and_gap():
    e=Engine(Config())
    e.record_only=True
    e.select(M,T)
    e.frame(book('u',T,bid=.42,ask=.45),T)
    e.frame(book('d',T,bid=.42,ask=.45),T)
    ops=Opportunities()
    ops.update(e,FEE)
    e.frame(book('u',T+.3,bid=.43,ask=.46),T+.3)
    ops.update(e,FEE)
    e.advance(T+1)
    ops.update(e,FEE)
    e.gap(T+1.1)
    ops.update(e,FEE)
    assert len(ops.events)==1 and ops.events[0]['survives_latency']
    assert ops.events[0]['duration_seconds']==pytest.approx(1.1)
    assert not ops.open


def test_sell_markout_opposite_sign():
    points={'c':[{'ts':T,'mid_up':.5,'mid_down':.5,'gaps':0},
                 {'ts':T+5,'mid_up':.6,'mid_down':.4,'gaps':0}]}
    sell={'type':'taker_fill','ts':T,'condition_id':'c','outcome':'Up','price':.5,'sell':True}
    assert markouts([sell],points)[0]['markout_5']==pytest.approx(-.1)


def test_validation_rejects_worse_than_market():
    train=[{'window':str(i),'model':.6,'mid':.9,'winner':1} for i in range(60)]
    val=[{'window':str(i),'model':.6,'mid':.9,'winner':1} for i in range(30)]
    result=train_model({'samples':{1:train,1.5:train}},{'samples':{1:val,1.5:val}},PROTOCOL)
    assert result['chosen_scale'] is None and result['gain_ci']['upper']<0


class FakeAPI:
    async def get(self,url,params=None):
        from pairbot.feed import AccessBlocked
        import time
        if 'binance' in url:
            raise AccessBlocked('fixture Binance HTTP 451, no bypass')
        if url.endswith('/time'):
            return int(time.time())
        from datetime import datetime,timezone
        start=int(time.time()//300)*300
        return [{'slug':f'btc-updown-5m-{start}','closed':False,'acceptingOrders':True,
                 'negRisk':False,'conditionId':'fixture','outcomes':['Up','Down'],
                 'clobTokenIds':['u','d'],'endDate':datetime.fromtimestamp(start+300,timezone.utc).isoformat(),
                 'orderPriceMinTickSize':.01,'orderMinSize':5}]
    async def close(self):
        pass


class FakeSocket:
    reads=0
    async def __aenter__(self):
        return self
    async def __aexit__(self,*args):
        pass
    async def send(self,message):
        pass
    async def recv(self):
        import time
        await asyncio.sleep(.001)
        self.reads+=1
        row=book('u' if self.reads%2 else 'd',time.time())
        row['market']='fixture'
        return json.dumps(row)


def test_heartbeat_runs_during_continuous_messages_and_stops_cleanly():
    import time
    class BusySocket(FakeSocket):
        def __init__(self):
            self.sent=[]
        async def send(self,message):
            self.sent.append(message)
    async def run():
        socket=BusySocket()
        controls=[]
        frames=[]
        async for row in market_messages(socket,time.monotonic()+.08,time.time()+1,
                lambda *a,**k:controls.append((a,k)),heartbeat_seconds=.01):
            frames.append(row)
        assert len(frames)>10 and len(socket.sent)>=3
        assert set(socket.sent)=={'PING'}
        sent=len(socket.sent)
        await asyncio.sleep(.02)
        assert len(socket.sent)==sent
        assert all(k['direction']=='sent' for _,k in controls)
    asyncio.run(run())


def test_heartbeat_failure_interrupts_idle_receiver():
    import time
    class BrokenSocket:
        async def send(self,message):
            raise OSError('fixture heartbeat failed')
        async def recv(self):
            await asyncio.sleep(30)
    async def run():
        with pytest.raises(OSError,match='heartbeat failed'):
            async for _ in market_messages(BrokenSocket(),time.monotonic()+1,time.time()+1,
                    lambda *a,**k:None,heartbeat_seconds=.01):
                pass
    asyncio.run(run())


def test_slow_io_socket_progress_and_journal_integrity(tmp_path,monkeypatch):
    import time
    from pairbot.feed import Journal
    original=Journal.write
    def slow(self,*args,**kwargs):
        time.sleep(.003)
        return original(self,*args,**kwargs)
    monkeypatch.setattr(Journal,'write',slow)
    socket=FakeSocket()
    path=tmp_path/'capture'
    summary=asyncio.run(record(path,.08,api=FakeAPI(),connector=lambda *a,**k:socket))
    assert socket.reads>10
    assert summary['fills']==0 and summary['orders']==0
    assert 'BINANCE_ACCESS_DENIED' in summary['issues']
    rows=list(read_entries(path))
    assert rows[-1]['type']=='stop'
    assert sum(x['type']=='frame' for x in rows)==socket.reads
    assert all('socket_monotonic_ns' in x and x['received_at']==x['ts'] for x in rows if x['type']=='frame')
    assert not json.loads((path/'journal.json').read_text())['active_shard']


def test_queue_overflow_explicit_durable_evidence(tmp_path):
    from pairbot.storage import StorageBackpressure
    path=tmp_path/'capture'
    with pytest.raises(StorageBackpressure):
        asyncio.run(record(path,.05,api=FakeAPI(),connector=lambda *a,**k:FakeSocket(),capacity=1))
    rows=list(read_entries(path))
    assert any(x['type']=='gap' and x.get('reason')=='research_receive_queue_overflow' for x in rows)
    assert [x['ts'] for x in rows]==sorted(x['ts'] for x in rows)


def test_research_deterministic_and_holdout_sealed(tmp_path):
    rows=[{'type':'market','ts':T,'market':M.data()},
          {'type':'frame','ts':T,'message':book('u',T)},
          {'type':'frame','ts':T,'message':book('d',T)},
          {'type':'tick','ts':T+1},{'type':'stop','ts':T+3}]
    source=tmp_path/'input.jsonl'
    source.write_text(''.join(json.dumps(x)+'\n' for x in rows))
    a=evaluate(source,tmp_path/'a',exploratory=True)
    b=evaluate(source,tmp_path/'b',exploratory=True)
    assert a==b and a['decision']=='NO-GO'
    assert a['final_status']=='SEALED_NOT_EVALUATED'
    assert 'NO_BINANCE_DATA_IN_SOCKET_JOURNAL' in a['blockers']


def test_equal_anchor_not_research_fill():
    e=research()
    e.frame(book('u',T+1),T+1)
    anchor=e.orders['u'].queue_at
    e.frame(trade('u',anchor,100),T+1.1)
    assert e.fills==0 and e.orders['u'].queue==15


def test_global_source_failure_survives_partial_window_exclusion(tmp_path):
    rows=[{'type':'market','ts':T,'market':{**M.data(),'condition_id':'excluded'}},
          {'type':'source_error','ts':T+1,'source':'Binance','error':'HTTP 451'},
          {'type':'frame','ts':T+2,'message':book('u',T+2)}]
    source=tmp_path/'input.jsonl'
    source.write_text(''.join(json.dumps(x)+'\n' for x in rows))
    selected=list(selected_entries(source,{'c'}))
    assert selected==[rows[1]]


def test_report_separates_order_measurements_and_book_availability(tmp_path):
    rows=[{'type':'market','ts':T,'market':M.data()},
          {'type':'frame','ts':T,'message':book('u',T)},
          {'type':'frame','ts':T,'message':book('d',T)},
          {'type':'tick','ts':T+1},{'type':'tick','ts':T+7},
          {'type':'stop','ts':T+8}]
    source=tmp_path/'input.jsonl'
    source.write_text(''.join(json.dumps(x)+'\n' for x in rows))
    report=evaluate(source,tmp_path/'report',exploratory=True)
    assert report['time_measurements']['scope']=='RECORD_ONLY_PROBE_NO_STRATEGY_ORDERS'
    states=report['book_availability']['markets']['c']
    assert states['fresh_two_sided_mid']>0
    assert states['stale_book']>0
    for row in report['strategies']:
        if row['status']=='EVALUATED_COUNTERFACTUAL':
            assert 'markets' in row['time_measurements']
    assert report['final_status']=='SEALED_NOT_EVALUATED'


def test_io_failure_closes_api_never_succeeds(tmp_path,monkeypatch):
    from pairbot.feed import Journal
    api=FakeAPI()
    api.closed=False
    async def close():
        api.closed=True
    api.close=close
    def broken(*a,**k):
        raise OSError('fixture disk failure')
    monkeypatch.setattr(Journal,'write',broken)
    with pytest.raises(RuntimeError,match='worker failed'):
        asyncio.run(record(tmp_path/'failed',.04,api=api,connector=lambda *a,**k:FakeSocket()))
    assert api.closed


def test_pre_window_books_excluded_from_opportunities():
    e=Engine(Config())
    e.record_only=True
    e.select(M,T-10)
    e.frame(book('u',T-10,bid=.42,ask=.45),T-10)
    e.frame(book('d',T-10,bid=.42,ask=.45),T-10)
    ops=Opportunities()
    ops.update(e,FEE)
    e.advance(T-9)
    ops.update(e,FEE)
    assert not ops.open and ops.fresh_seconds==0


def test_only_complete_windows_enter_new_splits(tmp_path):
    rows=[{'type':'research_header','ts':T+1},
          {'type':'market','ts':T+1,'market':M.data()},
          {'type':'stop','ts':T+299,'reason':'record_only_finished'}]
    path=tmp_path/'capture.jsonl'
    path.write_text(''.join(json.dumps(x)+'\n' for x in rows))
    assert len(market_index(path))==1 and market_index(path,complete_only=True)==[]


def test_cannot_evaluate_unsealed_recording(tmp_path):
    path=tmp_path/'capture'
    path.mkdir()
    (path/'journal.json').write_text(json.dumps({'shards':[],'active_shard':'partial'}))
    with pytest.raises(ValueError,match='active/incomplete'):
        evaluate(path,tmp_path/'out')



def test_auxiliary_merge_is_causal(tmp_path):
    from pairbot.research import merged_entries
    rows=[{'type':'market','ts':T,'market':M.data()},
          {'type':'frame','ts':T,'message':book('u',T)},
          {'type':'frame','ts':T+1,'message':book('u',T+1)}]
    path=tmp_path/'capture.jsonl'
    path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    auxiliary=[{'type':'binance','ts':T+.5,'message':{'p':'999'}}]
    merged=list(merged_entries(path,{'c'},auxiliary))
    assert [r['ts'] for r in merged]==[T,T,T+.5,T+1]


def test_auxiliary_sha_guard_and_original_cadence_retained(tmp_path):
    from pairbot.research import load_auxiliary
    path=tmp_path/'btc'
    path.mkdir()
    raw={'type':'binance_aux','ts':T,'received_at':T,'message':{'price':'100','time':T*1000}}
    body=(json.dumps(raw)+'\n').encode()
    file=path/'btc.jsonl'
    file.write_bytes(body)
    sha=hashlib.sha1(b'blob '+str(len(body)).encode()+b'\0'+body).hexdigest()
    (path/'manifest.json').write_text(json.dumps({'files':[{'path':'data/btc.jsonl','sha':sha}],
                                                'summary':{'cadence_seconds':2}}))
    rows,digest,report=load_auxiliary(path)
    assert rows[0]['message']['p']=='100'
    assert report['cadence_seconds']==2
    file.write_text(json.dumps({**raw,'received_at':T+1})+'\n')
    with pytest.raises(ValueError,match='hash mismatch'):
        load_auxiliary(path)
