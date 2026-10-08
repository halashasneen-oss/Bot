import asyncio
from websockets.asyncio.client import ClientConnection
from websockets.client import ClientProtocol
from websockets.frames import Frame,Opcode
from websockets.uri import parse_uri
from pairbot.capture_transport import TimedClientConnection


def test_transport_marks_match_fragmented_messages_and_close(monkeypatch):
    monkeypatch.setattr(ClientConnection,'process_event',lambda *a:None)
    async def fake_recv(*args,**kwargs):return 'fixture message'
    monkeypatch.setattr(ClientConnection,'recv',fake_recv)
    async def run():
        ws=TimedClientConnection(ClientProtocol(parse_uri('ws://fixture.invalid')))
        ws._chunk={'wall':10,'monotonic_ns':10_000_000_000,'bytes':30}
        ws.process_event(Frame(Opcode.TEXT,b'fi',fin=False))
        assert not ws._delivery_marks
        ws._chunk={'wall':11,'monotonic_ns':11_000_000_000,'bytes':40}
        ws.process_event(Frame(Opcode.CONT,b'xture',fin=True))
        assert len(ws._delivery_marks)==1
        await ws.recv()
        assert ws.last_message_timing['first_fragment_received_at']==10
        assert ws.last_message_timing['socket_received_at']==11
        assert ws.last_message_timing['socket_timestamp_scope']=='COMPLETING_ASYNCIO_TRANSPORT_CHUNK'
        ws.process_event(Frame(Opcode.CLOSE,(1000).to_bytes(2,'big')+b'fixture'))
        assert ws.last_close_timing['received_close_code']==1000
        assert ws.last_close_timing['received_close_reason']=='fixture'
        assert not ws._delivery_marks
    asyncio.run(run())


def test_transport_timestamp_precedes_frame_parser(monkeypatch):
    observed=[]
    def parse(self,data):
        observed.append(dict(self._chunk))
    monkeypatch.setattr(ClientConnection,'data_received',parse)
    async def run():
        ws=TimedClientConnection(ClientProtocol(parse_uri('ws://fixture.invalid')))
        ws.data_received(b'fixture')
        assert observed[0]['bytes']==7 and observed[0]['monotonic_ns']>0
    asyncio.run(run())


def test_timestamped_connection_on_local_public_fixture():
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve
    async def handler(ws):
        await ws.send(['fi','xture'])
        await ws.close(code=1000,reason='unit fixture')
    async def run():
        async with serve(handler,'127.0.0.1',0) as server:
            port=server.sockets[0].getsockname()[1]
            async with connect(f'ws://127.0.0.1:{port}',create_connection=TimedClientConnection,proxy=None) as ws:
                assert await ws.recv()=='fixture'
                assert ws.last_message_timing['socket_received_at']>0
                assert ws.last_message_timing['socket_monotonic_ns']>=ws.last_message_timing['first_fragment_monotonic_ns']
                await ws.wait_closed()
                assert ws.last_close_timing['received_close_code']==1000
                assert not ws._delivery_marks
    asyncio.run(run())
