import asyncio
from dataclasses import asdict, replace
import json
import time

import httpx
import pytest

from pairbot.config import Config
from pairbot.engine import Engine
from pairbot.feed import AccessBlocked, Journal, PublicAPI, apply, collect, read_entries
from pairbot.__main__ import load_engine, write_report
from pairbot.market import Market
from tests.test_engine import T, M, engine, book, trade


def test_delayed_deltas_preserve_books_and_do_not_fill():
    e = engine()
    e.frame({'event_type': 'price_change', 'market': 'c', 'timestamp': (T + 1)*1000,
             'price_changes': [{'asset_id':'u','price':'.48','size':'20','side':'BUY'},
                               {'asset_id':'d','price':'.48','size':'20','side':'BUY'}]}, T + 7)
    assert e.rejected_frames == 0
    assert e.delayed_frames == 2
    assert set(e.received) == {'u','d'}
    assert not e.fresh()
    e.frame(trade('u', T+1, 100), T+7.1)
    assert e.fills == 0
    # Fresh ordered traffic recovers without discarding the entire book.
    e.frame(book('u', T+8), T+8)
    e.frame(book('d', T+8), T+8)
    assert e.fresh()
    assert e.report()['execution_quality'] == 'INCOMPLETE_DATA'


def test_corrupt_token_does_not_clear_other_book():
    e = engine()
    e.frame(book('u', T+1, bid=float('nan')), T+1)
    assert 'd' in e.received
    assert 'u' not in e.received
    assert e.resync_tokens == {'u'}
    e.frame(book('u', T+2), T+2)
    assert e.fresh() and not e.resync_tokens


def test_out_of_order_needs_snapshot_but_no_data_loss_cascade():
    e = engine()
    e.frame(book('u', T+2), T+2)
    e.frame({'event_type':'price_change','market':'c','timestamp':(T+1)*1000,
             'price_changes':[{'asset_id':'u','price':'.48','size':'0','side':'BUY'}]}, T+2.1)
    assert e.out_of_order_frames == 1
    assert e.rejected_frames == 0
    assert e.books['u'].bids[.48] == 10  # older delta ignored
    assert e.resync_tokens == {'u'}
    e.frame(book('u', T+3), T+3)
    assert e.fresh()


def test_zero_one_boundary_levels_do_not_corrupt_snapshot():
    e = engine()
    message = book('u', T+1)
    message['bids'].append({'price':'0','size':'0'})
    message['asks'].append({'price':'1','size':'10'})
    e.frame(message,T+1)
    assert e.rejected_frames == 0


def test_journal_rotates_and_replays_exact_accounting(tmp_path):
    e = engine()
    j = Journal(tmp_path/'capture')
    j.write('header',0,schema=1,config=asdict(e.cfg),source='PUBLIC_LIVE')
    rows=[{'type':'market','ts':T,'market':M.data()},
          {'type':'frame','ts':T,'message':book('u',T)},
          {'type':'frame','ts':T,'message':book('d',T)},
          {'type':'frame','ts':T+1,'message':trade('u',T+1,20)},
          {'type':'frame','ts':T+2,'message':trade('d',T+2,17)}]
    for row in rows:
        d=dict(row);j.write(d.pop('type'),d.pop('ts'),**d)
    j._rotate()
    j.write('settlement',T+300,condition_id='c',winner='d')
    j.close()
    replay=load_engine(tmp_path/'capture')
    assert replay.cash == pytest.approx(48.64)
    assert replay.positions['u'].size == 0
    assert len(list(read_entries(tmp_path/'capture'))) == 7


def test_restart_preserves_exposure_and_cash(tmp_path):
    j=Journal(tmp_path/'capture')
    j.write('header',0,schema=1,config=asdict(Config()),source='PUBLIC_LIVE')
    j.write('market',T,market=M.data())
    j.write('frame',T,message=book('u',T))
    j.write('frame',T,message=book('d',T))
    j.write('frame',T+1,message=trade('u',T+1,20))
    j.close()
    e=load_engine(tmp_path/'capture')
    assert e.cash == pytest.approx(47.6)
    assert e.positions['u'].size == 5
    apply(e,{'type':'gap','ts':T+20})
    assert e.cash == pytest.approx(47.6) and e.positions['u'].size == 5


def test_checkpoint_is_atomic_and_audit_not_duplicated(tmp_path):
    e=engine()
    write_report(e,tmp_path,quiet=True)
    audit=(tmp_path/'audit.jsonl').read_text()
    write_report(e,tmp_path,quiet=True)
    assert (tmp_path/'audit.jsonl').read_text() == audit
    assert json.loads((tmp_path/'summary.json').read_text())['cash'] == 50
    assert not list(tmp_path.glob('*.tmp'))


def test_http_access_block_is_fatal_no_retry():
    count=0
    async def run():
        nonlocal count
        def handler(request):
            nonlocal count
            count+=1
            return httpx.Response(403,request=request)
        api=PublicAPI()
        await api.client.aclose()
        api.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            with pytest.raises(AccessBlocked):await api.get('https://gamma-api.polymarket.com/markets')
        finally:await api.close()
    asyncio.run(run())
    assert count==1


def test_http_rate_limit_respects_retry_after(monkeypatch):
    waits=[]
    async def no_wait(seconds):waits.append(seconds)
    async def run():
        count=0
        def handler(request):
            nonlocal count
            count+=1
            return httpx.Response(429,headers={'Retry-After':'7'},request=request) if count==1 else httpx.Response(200,json=[],request=request)
        api=PublicAPI();await api.client.aclose()
        api.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        monkeypatch.setattr('pairbot.feed.asyncio.sleep',no_wait)
        try: assert await api.get('https://gamma-api.polymarket.com/markets')==[]
        finally:await api.close()
    asyncio.run(run())
    assert waits==[7]


class FakeAPI:
    def __init__(self,clock):self.clock=clock;self.closed=False;self.discovered=[]
    async def discover(self,now):
        start=int(now//300)*300
        m=Market(f'btc-updown-5m-{start}',str(start),f'u{start}',f'd{start}',start,start+300,.01,5)
        self.discovered.append(m)
        return m
    async def resolved(self,m):return m.up
    async def close(self):self.closed=True


class FakeSocket:
    def __init__(self,clock):self.clock=clock;self.tokens=[];self.i=0
    async def send(self,wire):
        if wire.startswith('{'):self.tokens=json.loads(wire)['assets_ids']
    async def recv(self):
        await asyncio.sleep(.002)
        token=self.tokens[self.i%2];self.i+=1
        msg=book(token,self.clock())
        msg['market']=token[1:]
        return json.dumps(msg)


class Context:
    def __init__(self,clock):self.ws=FakeSocket(clock)
    async def __aenter__(self):return self.ws
    async def __aexit__(self,*args):pass


def test_collector_crosses_two_market_boundaries_and_saves(tmp_path, monkeypatch):
    # Virtual time advances only with frames, independent of disk/runner speed.
    elapsed = [0.0]
    clock = lambda: T + 60 + elapsed[0]
    mono = lambda: elapsed[0]
    real_sleep = asyncio.sleep
    async def yield_only(seconds):
        await real_sleep(0)
    monkeypatch.setattr('pairbot.feed.asyncio.sleep', yield_only)
    class Socket(FakeSocket):
        async def recv(self):
            await real_sleep(0)
            elapsed[0] += 100
            token = self.tokens[self.i % 2]; self.i += 1
            message = book(token, clock()); message['market'] = token[1:]
            return json.dumps(message)
    class Conn(Context):
        def __init__(self, clock): self.ws = Socket(clock)
    api = FakeAPI(clock)
    e = Engine(replace(Config(), cancel_latency_seconds=0, order_latency_seconds=0))
    j = Journal(tmp_path/'run')
    j.write('header', 0, schema=1, config=asdict(e.cfg), source='SYNTHETIC_TEST')
    saves = []
    asyncio.run(collect(e, j, 11, api=api, connector=lambda *a, **k: Conn(clock),
                        wall_clock=clock, monotonic=mono, checkpoint=lambda: saves.append(e.report())))
    j.close()
    assert len(e.markets) >= 3
    assert e.run_info['requested_duration_completed']
    assert e.run_info['run_status'] == 'COMPLETED'
    assert api.closed and saves
    assert all(x['cost'] >= 0 for x in e.pending)


def test_fatal_failure_persists_full_error_and_partial_report(tmp_path):
    class Blocked(FakeAPI):
        async def discover(self,now):raise AccessBlocked('HTTP 403 test')
    e=Engine(Config());j=Journal(tmp_path/'run')
    api=Blocked(time.time)
    with pytest.raises(AccessBlocked):
        asyncio.run(collect(e,j,1,api=api,checkpoint=lambda:write_report(e,j.directory,quiet=True)))
    j.close()
    assert api.closed
    report=json.loads((tmp_path/'run'/'summary.json').read_text())
    assert report['run']['stop_reason']=='access_blocked'
    assert report['run']['run_status']=='FAILED_OR_INTERRUPTED'
    assert not report['run']['requested_duration_completed']
    assert 'Traceback' in (tmp_path/'run'/'errors.jsonl').read_text()


def test_temporary_http_failure_eventually_recovers(monkeypatch):
    from pairbot.feed import DataUnavailable
    attempts=[]
    async def no_wait(seconds):pass
    async def run():
        def handler(request):
            attempts.append(1)
            return httpx.Response(503,request=request) if len(attempts)<3 else httpx.Response(200,json={'ok':True},request=request)
        api=PublicAPI();await api.client.aclose()
        api.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        monkeypatch.setattr('pairbot.feed.asyncio.sleep',no_wait)
        try: assert await api.get('https://gamma-api.polymarket.com/markets')=={'ok':True}
        finally:await api.close()
    asyncio.run(run())
    assert len(attempts)==3


def test_stream_continues_while_settlement_rest_waits(tmp_path):
    received, intervals = [], []
    class Slow(FakeAPI):
        async def resolved(self, m):
            start = time.monotonic()
            await asyncio.sleep(.15)
            intervals.append((start, time.monotonic()))
            return m.up
    origin = [None]
    clock = lambda: T + 301 + (time.monotonic() - origin[0]) * 100
    mono = lambda: (time.monotonic() - origin[0]) * 100
    class Socket(FakeSocket):
        async def recv(self):
            wire = await super().recv()
            received.append(time.monotonic())
            return wire
    class Conn(Context):
        def __init__(self, clock): self.ws = Socket(clock)
    api = Slow(clock)
    e = Engine(replace(Config(), cancel_latency_seconds=0))
    e.select(M, T + 299)
    j = Journal(tmp_path/'run')
    async def run():
        origin[0] = time.monotonic()
        await collect(e, j, 2, api=api, connector=lambda *a, **k: Conn(clock),
                      wall_clock=clock, monotonic=mono)
    asyncio.run(run()); j.close()
    assert intervals
    assert any(start < ts < end for start, end in intervals for ts in received)
    assert e.run_info['requested_duration_completed']


def test_failed_session_replay_matches_checkpoint(tmp_path):
    class Blocked(FakeAPI):
        async def discover(self,now):raise AccessBlocked('Blocked test')
    e=Engine(Config());e.source='PUBLIC_LIVE';j=Journal(tmp_path/'run')
    j.write('header',0,schema=1,config=asdict(e.cfg),source='PUBLIC_LIVE')
    with pytest.raises(AccessBlocked):
        asyncio.run(collect(e,j,1,api=Blocked(time.time),checkpoint=lambda:write_report(e,j.directory,quiet=True)))
    j.close()
    assert load_engine(tmp_path/'run').report()==json.loads((tmp_path/'run'/'summary.json').read_text())


def test_stale_exchange_depth_cannot_mark_residual_value():
    e = engine()
    e.positions['u'].size = 5
    e.positions['u'].cost = 2.4
    e.cash = 47.6
    e.frame(book('u', T+1), T+7)
    e.frame(book('d', T+1), T+7)
    assert e.equity() == pytest.approx(47.6)
    assert e.halted is None  # loss remains below configured stop


def test_trade_after_queue_anchor_can_arrive_after_newer_snapshot():
    e = engine()
    e.advance(T+1)
    e._activate()
    e.frame(book('u', T+3), T+3)
    queue = e.orders['u'].queue
    e.frame(trade('u', T+2, 100), T+3.1)
    assert e.fills == 1
    assert e.positions['u'].size == 5


def test_movement_window_extrema_expire_and_constant_prices_stay_bounded():
    e = engine()
    for i in range(10000):
        e.now = T + i / 10000
        e.received = {'u': e.now, 'd': e.now}
        e.exchange_ts = dict(e.received)
        e._market_risk()
    assert len(e.mid_min) == len(e.mid_max) == 1
    assert not e.blocked
    e.now = T + 62
    e.received = {'u': e.now, 'd': e.now}
    e.exchange_ts = dict(e.received)
    e._market_risk()
    assert all(t >= T + 2 for q in (e.mid_min,e.mid_max) for t,v in q)


def test_sealed_journal_detects_truncation_before_account_replay(tmp_path):
    j = Journal(tmp_path/'run')
    j.write('header', 0, schema=1, config=asdict(Config()), source='PUBLIC_LIVE')
    j.close()
    manifest = json.loads((tmp_path/'run'/'journal.json').read_text())
    assert len(manifest['sealed']) == 1
    p = tmp_path/'run'/manifest['shards'][0]
    p.write_bytes(p.read_bytes()[:-10])
    with pytest.raises(ValueError, match='changed or truncated'):
        load_engine(tmp_path/'run')


def test_resolution_lookup_explicitly_includes_closed_markets():
    calls = []
    async def run():
        api = PublicAPI()
        async def get(url, params):
            calls.append(params)
            assert params == {'slug': M.slug, 'closed': 'true'}
            return [{'conditionId': M.condition_id, 'closed': True,
                     'umaResolutionStatus': 'resolved', 'outcomes': '["Up", "Down"]',
                     'outcomePrices': '["1", "0"]'}]
        api.get = get
        try:
            assert await api.resolved(M) == M.up
        finally:
            await api.close()
    asyncio.run(run())
    assert len(calls) == 1


def test_stream_shards_are_published_atomically_after_footer(tmp_path):
    j = Journal(tmp_path/'run')
    j.write('header', 0, schema=1, config=asdict(Config()), source='PUBLIC_LIVE')
    manifest = json.loads((tmp_path/'run'/'journal.json').read_text())
    assert manifest['shards'] == []
    assert manifest['active_shard'].endswith('.partial')
    with pytest.raises(ValueError, match='active/interrupted'):
        load_engine(tmp_path/'run')
    j.close()
    manifest = json.loads((tmp_path/'run'/'journal.json').read_text())
    assert manifest['active_shard'] is None
    assert not list((tmp_path/'run').glob('*.partial'))
    assert load_engine(tmp_path/'run').cash == 50
