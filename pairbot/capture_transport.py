"""Read-only timestamps before WebSocket parsing and receive-buffer delivery.

Asyncio transport delivery is after OS/TLS handling, not a kernel timestamp.
No socket tuning, orders, credentials or alternative network routes.
"""
from collections import deque
import time
from websockets.asyncio.client import ClientConnection
from websockets.frames import Frame, Opcode


class TimedClientConnection(ClientConnection):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self._chunk=None
        self._fragment_start=None
        self._delivery_marks=deque()
        self.last_message_timing=None
        self.last_close_timing=None

    def data_received(self,data):
        self._chunk={'wall':time.time(),'monotonic_ns':time.monotonic_ns(),'bytes':len(data)}
        super().data_received(data)

    def process_event(self,event):
        if isinstance(event,Frame) and self._chunk is not None:
            if event.opcode in (Opcode.TEXT,Opcode.BINARY):
                self._fragment_start=dict(self._chunk)
            if event.opcode in (Opcode.TEXT,Opcode.BINARY,Opcode.CONT) and event.fin:
                first=self._fragment_start or self._chunk
                self._delivery_marks.append({
                    'socket_received_at':self._chunk['wall'],
                    'socket_monotonic_ns':self._chunk['monotonic_ns'],
                    'first_fragment_received_at':first['wall'],
                    'first_fragment_monotonic_ns':first['monotonic_ns'],
                    'completing_transport_chunk_bytes':self._chunk['bytes'],
                    'socket_timestamp_scope':'COMPLETING_ASYNCIO_TRANSPORT_CHUNK'})
                self._fragment_start=None
            elif event.opcode is Opcode.CLOSE:
                self.last_close_timing={
                    'socket_received_at':self._chunk['wall'],
                    'socket_monotonic_ns':self._chunk['monotonic_ns'],
                    'received_close_code':int.from_bytes(event.data[:2],'big') if len(event.data)>=2 else None,
                    'received_close_reason':bytes(event.data[2:]).decode('utf-8',errors='replace'),
                    'socket_timestamp_scope':'COMPLETING_ASYNCIO_TRANSPORT_CHUNK'}
        super().process_event(event)

    async def recv(self,*args,**kwargs):
        wire=await super().recv(*args,**kwargs)
        self.last_message_timing=self._delivery_marks.popleft() if self._delivery_marks else None
        return wire
