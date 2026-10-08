import asyncio
import time
import pytest
from pairbot.reference_transport import Progress, SilentReference, consume, load_transport_policy, wait_for_reference
from pairbot.unified_paper import Feed, load_policy

class Log:
    def __init__(self):self.rows=[]
    def write(self,row):self.rows.append(row)

def tick(topic='crypto_prices_chainlink',symbol='btc/usd',timestamp=None):
    return {'topic':topic,'type':'update','payload':{'symbol':symbol,'timestamp':int((time.time() if timestamp is None else timestamp)*1000),'value':100,'window_s':60}}

def test_watchdog_requires_both_advancing_topics():
    p=Progress(0);now=time.time()
    assert p.observe(tick(timestamp=now),now,9)
    assert p.silent(10,10)==['crypto_prices_twap_sixty']
    assert p.observe(tick('crypto_prices_twap_sixty',timestamp=now),now,9)
    assert not p.silent(18,10)
    assert set(p.silent(19,10))==set(p.last)

@pytest.mark.parametrize('bad',['symbol','generic','duplicate','backfill','future','nan','wrong_twap'])
def test_keepalives_other_symbols_and_nonadvancing_ticks_do_not_heal(bad):
    p=Progress(0);now=time.time();m=tick(timestamp=now)
    p.observe(m,now,1)
    if bad=='symbol':m['payload']['symbol']='eth/usd'
    elif bad=='generic':m['topic']='crypto_prices'
    elif bad=='backfill':m['payload']['timestamp']=int((now-20)*1000)
    elif bad=='future':m['payload']['timestamp']=int((now+20)*1000)
    elif bad=='nan':m['payload']['value']=float('nan')
    elif bad=='wrong_twap':m['topic']='crypto_prices_twap_sixty';m['payload']['window_s']=30
    assert not p.observe(m,now,9)
    assert p.last['crypto_prices_chainlink']==1
    assert p.silent(11,10)

class Socket:
    def __init__(self,mode):self.mode=mode;self.sent=[]
    async def send(self,data):self.sent.append(data)
    async def recv(self):
        import json
        await asyncio.sleep(.001)
        if self.mode=='auth':return json.dumps({'error':'authentication required'})
        if self.mode=='empty':await asyncio.sleep(10)
        return json.dumps(tick(symbol='eth/usd'))

@pytest.mark.parametrize('mode',['other','empty'])
def test_live_socket_or_empty_socket_both_recover_on_btc_silence(mode):
    async def check():
        ws=Socket(mode);log=Log();p=load_transport_policy();p.update(btc_progress_timeout_seconds=.03,receive_check_seconds=.005,application_ping_seconds=.01)
        with pytest.raises(SilentReference):await consume(ws,log,lambda m,r:None,p)
        assert any(r['type']=='feed_silent' for r in log.rows)
        assert 'PING' in ws.sent
    asyncio.run(check())

def test_auth_wall_stops_without_retry_or_bypass():
    async def check():
        log=Log();ws=Socket('auth')
        assert await consume(ws,log,lambda m,r:None,load_transport_policy())=='AUTH_REQUIRED'
        assert log.rows[-1]['type']=='feed_blocked_auth'
        assert len(ws.sent)==1
    asyncio.run(check())

def test_reference_wait_stays_inside_original_tolerance():
    class Candidate:
        journal=Log();calls=0
        def features(self,start,now,policy):
            self.calls+=1
            if self.calls==1:raise ValueError('STALE_UNDERLYING_REFERENCE')
            return {'p_up':.6}
    f=Candidate();r,t=asyncio.run(wait_for_reference(f,1000,lambda:(1181,.01),load_policy()))
    assert r['p_up']==.6 and f.calls==2 and t==1181
    assert len(f.journal.rows)==1

@pytest.mark.parametrize('case',['deadline','late','gap'])
def test_reference_wait_never_extends_deadline_or_repairs_gap(case):
    class Candidate:
        journal=Log();calls=0
        def features(self,start,now,policy):
            self.calls+=1;raise ValueError('REFERENCE_HISTORY_GAP' if case=='gap' else 'STALE_UNDERLYING_REFERENCE')
    f=Candidate();clock=lambda:(1182.1 if case=='late' else 1181.98 if case=='deadline' else 1181,.01)
    with pytest.raises(ValueError):asyncio.run(wait_for_reference(f,1000,clock,load_policy()))
    assert f.calls<=1

def test_reconnect_invalidates_generation_and_logs_normal_close(monkeypatch):
    import pairbot.unified_paper as module
    class Context:
        close_code=1000;close_reason='test close'
        async def __aenter__(self):return self
        async def __aexit__(self,*a):return False
    calls=[]
    async def receiver(ws,journal,ingest,policy):
        calls.append(1)
        if len(calls)==1:raise SilentReference('test')
        await asyncio.sleep(10)
    monkeypatch.setattr(module,'connect',lambda *a,**kw:Context())
    monkeypatch.setattr(module,'consume',receiver)
    p=load_transport_policy();p['reconnect_delay_seconds']=.001
    monkeypatch.setattr(module,'load_transport_policy',lambda:p)
    async def check():
        f=Feed(Log());task=asyncio.create_task(f.run())
        try:
            for _ in range(30):
                if len(calls)==2:break
                await asyncio.sleep(.002)
            assert len(calls)==2 and f.generation==3
            assert any(x['type']=='feed_close' and x['code']==1000 for x in f.journal.rows)
            assert any(x['type']=='feed_gap' for x in f.journal.rows)
        finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
    asyncio.run(check())

def test_one_frozen_transport_group_without_touching_trading_policy():
    p=load_transport_policy();old=load_policy()
    assert p['comparison_groups']==1 and p['preflight_duration_seconds']==3600
    assert old['duration_seconds']==14400 and old['maximum_data_age_seconds']==2
    assert old['minimum_edge_per_share_after_entry_fee_and_slippage']==.03

def test_preflight_requires_both_frozen_limits_and_full_duration():
    from pairbot.reference_preflight import gate_result
    p=load_transport_policy()
    assert gate_result(3240,3600,2,p)['passed']
    assert not gate_result(3239,3600,2,p)['passed']
    assert not gate_result(3600,3600,1,p)['passed']
    assert not gate_result(3599,3599,5,p)['passed']
    assert not gate_result(0,0,0,p)['passed']
    assert gate_result(3600,3600,5,p)['financial_go'] is False
