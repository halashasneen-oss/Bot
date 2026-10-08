import asyncio
import json
import time
import httpx
import pytest
from pairbot.feed import PublicAPI, AccessBlocked, read_entries
from pairbot.research_capture import record, clock_sample, market_messages, receive_timestamped
from .test_research import FakeAPI, FakeSocket


def test_receive_timestamp_is_taken_inside_receiver(monkeypatch):
    async def run():
        socket=FakeSocket()
        task=asyncio.create_task(receive_timestamped(socket))
        await asyncio.sleep(.02)
        wire,wall,mono=task.result()
        assert json.loads(wire)['event_type']=='book'
        assert time.time()-wall>=.005
        assert (time.monotonic_ns()-mono)/1e9>=.005
    asyncio.run(run())


def test_completed_receive_not_lost_when_heartbeat_ends(monkeypatch):
    async def finish_both(tasks,**kwargs):
        await asyncio.gather(*tasks)
        return set(tasks),set()
    monkeypatch.setattr(asyncio,'wait',finish_both)
    async def run():
        socket=FakeSocket()
        rows=[r async for r in market_messages(socket,time.monotonic()+.02,time.time()+1,lambda *a,**k:None,heartbeat_seconds=.01)]
        assert len(rows)==socket.reads==1
    asyncio.run(run())


def test_clock_records_monotonic_rtt_and_conservative_bound():
    result=clock_sample(100,100,100.2,sent_ns=10_000_000_000,received_ns=10_300_000_000)
    assert result['monotonic_rtt_seconds']==.3
    assert result['uncertainty_seconds']==.65
    assert result['wall_minus_monotonic_elapsed_seconds']==pytest.approx(-.1)
    with pytest.raises(ValueError,match='Monotonic'):
        clock_sample(100,100,100.2,sent_ns=10,received_ns=9)


def test_clock_get_once_does_not_retry_and_respects_access_block():
    async def run(status):
        calls=[]
        def handle(request):
            calls.append(request)
            return httpx.Response(status,json=100)
        api=PublicAPI()
        await api.client.aclose()
        api.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:
            if status==200:assert await api.get_once('https://fixture.invalid/time')==100
            elif status==451:
                with pytest.raises(AccessBlocked):await api.get_once('https://fixture.invalid/time')
            else:
                with pytest.raises(httpx.HTTPStatusError):await api.get_once('https://fixture.invalid/time')
            assert len(calls)==1
        finally:await api.close()
    for status in (200,429,451,500):asyncio.run(run(status))


def test_record_journals_connection_lifecycle_and_clock(tmp_path):
    class SingleAPI(FakeAPI):
        async def get_once(self,url):return int(time.time())
    path=tmp_path/'capture'
    asyncio.run(record(path,.04,api=SingleAPI(),connector=lambda *a,**k:FakeSocket()))
    rows=list(read_entries(path))
    lifecycle=[r for r in rows if r['type']=='socket_lifecycle']
    phases=[r['phase'] for r in lifecycle]
    assert phases[:4]==['connect_started','opened','subscribe_started','subscribed']
    assert 'first_frame' in phases and phases.count('initial_snapshots_received')==1
    assert all(r['connection_id']==1 for r in lifecycle)
    frame=next(r for r in rows if r['type']=='frame')
    assert frame['receive_timestamp_scope']=='APPLICATION_WS_RECV_RETURN'
    assert frame['received_monotonic_ns']>=frame['socket_monotonic_ns']
    clock=next(r for r in rows if r['type']=='clock')
    assert clock['request_mode']=='single_public_GET' and clock['monotonic_rtt_seconds']>=0


def test_blocked_clock_does_not_retry_or_bypass(tmp_path):
    class BlockedAPI(FakeAPI):
        calls=0
        async def get_once(self,url):
            self.calls+=1
            raise AccessBlocked('fixture HTTP 403')
    api=BlockedAPI();path=tmp_path/'capture'
    summary=asyncio.run(record(path,.03,api=api,connector=lambda *a,**k:FakeSocket()))
    assert api.calls==1
    assert summary['issues']['CLOCK_ACCESS_DENIED']==1
    assert not any(r['type']=='clock' for r in read_entries(path))
