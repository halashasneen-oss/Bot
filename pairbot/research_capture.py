"""Record public data only. No engine, wallets, keys, signing or exchange orders."""
import argparse
import asyncio
from collections import Counter
from email.utils import parsedate_to_datetime
import json
import math
from pathlib import Path
import time
from websockets.asyncio.client import connect
from .feed import PublicAPI, Journal, AccessBlocked, GAMMA, CLOB, WS
from .market import parse_market, resolution
from .storage import StorageWorker, StorageBackpressure, Telemetry
from .capture_transport import TimedClientConnection

BINANCE_TIME='https://api.binance.com/api/v3/time'
BINANCE_WS='wss://stream.binance.com:9443/ws/btcusdt@trade'


async def receive_timestamped(ws):
    """Application delivery time in the receiving task, before parent scheduling/JSON.

    This is not a kernel or remote network-arrival timestamp.
    """
    wire=await ws.recv()
    return wire,time.time(),time.monotonic_ns()


async def market_messages(ws,deadline,market_end,emit,*,heartbeat_seconds=10):
    """Text PING is independent of traffic; propagate heartbeat send failures."""
    async def heartbeat():
        while time.monotonic()<deadline and time.time()<market_end:
            await asyncio.sleep(min(heartbeat_seconds,max(0,deadline-time.monotonic()),
                                    max(0,market_end-time.time())))
            if time.monotonic()>=deadline or time.time()>=market_end:
                return
            await ws.send('PING')
            emit('socket_control',value='PING',direction='sent',
                 socket_monotonic_ns=time.monotonic_ns())
    ping=asyncio.create_task(heartbeat())
    recv=None
    try:
        while time.monotonic()<deadline and time.time()<market_end:
            recv=asyncio.create_task(receive_timestamped(ws))
            timeout=max(.001,min(10,deadline-time.monotonic(),market_end-time.time()))
            done,_=await asyncio.wait([recv,ping],timeout=timeout,
                                      return_when=asyncio.FIRST_COMPLETED)
            if recv in done:
                wire,received,mono=recv.result()
                recv=None
                yield wire,received,mono
                # Drain a completed receive even when the heartbeat ends in the
                # same wait cycle. The deadline never erases a delivered frame.
                if ping in done:
                    ping.result()
                    break
            elif ping in done:
                ping.result()
                break
            else:
                recv.cancel()
                await asyncio.gather(recv,return_exceptions=True)
                recv=None
    finally:
        for task in (recv,ping):
            if task is not None:
                task.cancel()
        await asyncio.gather(*(task for task in (recv,ping) if task is not None),
                             return_exceptions=True)


def clock_sample(server,sent,received,*,sent_ns=None,received_ns=None):
    if received < sent:
        raise ValueError('Wall clock moved backwards during clock probe')
    server=float(server)
    if server > 1e12:
        server/=1000
    elapsed=received-sent
    monotonic_rtt=None
    if sent_ns is not None and received_ns is not None:
        monotonic_rtt=(received_ns-sent_ns)/1e9
        if monotonic_rtt<0:
            raise ValueError('Monotonic clock moved backwards')
        elapsed=max(elapsed,monotonic_rtt)
    return {'offset_server_minus_local':server-(sent+received)/2,
            'integer_time_offset_interval':[server-received,server+1-sent],
            'integer_time_midpoint_offset':server+.5-(sent+received)/2,
            'offset_point_convention':'raw integer time minus request midpoint',
            'rtt_seconds':received-sent,'monotonic_rtt_seconds':monotonic_rtt,
            'wall_minus_monotonic_elapsed_seconds':None if monotonic_rtt is None else received-sent-monotonic_rtt,
            'uncertainty_seconds':elapsed/2+.5,
            'server_ts':server,'request_sent':sent,'received_at':received}


def reference_metadata(raw):
    for event in raw.get('events',[]):
        meta=event.get('eventMetadata') or {}
        if meta.get('priceToBeat') is not None:
            price=float(meta['priceToBeat'])
            if math.isfinite(price) and price > 0:
                return {'opening_price':price,'status':'OBSERVED_OFFICIAL_FIELD',
                        'provenance':f'Gamma event {event["id"]} eventMetadata.priceToBeat',
                        'resolution_source':raw.get('resolutionSource'),
                        'config':raw.get('cryptoMarketConfig')}
    return {'opening_price':None,'status':'MISSING_OFFICIAL_REFERENCE',
            'resolution_source':raw.get('resolutionSource'),'config':raw.get('cryptoMarketConfig')}


async def record(directory,seconds=7200,*,api=None,connector=connect,capacity=4096):
    if not math.isfinite(seconds) or not 0 < seconds <= 86400:
        raise ValueError('Record duration must be in (0,86400] seconds')
    api=api or PublicAPI()
    journal=Journal(directory)
    telemetry=Telemetry()
    storage=StorageWorker(journal,telemetry)
    queue=asyncio.Queue(capacity)
    deadline=time.monotonic()+seconds
    counts=Counter()
    issues=[]
    markets={}
    settled=set()
    last_ts=0.
    sequence=0
    overflow=[]
    failure=None

    def emit(kind,received=None,**data):
        nonlocal sequence
        sequence+=1
        wall=time.time() if received is None else received
        entry={'type':kind,'ts':wall,'received_at':wall,
               'received_monotonic_ns':time.monotonic_ns(),'sequence':sequence,**data}
        try:
            queue.put_nowait(entry)
        except asyncio.QueueFull as exc:
            overflow.append({'reason':'research_receive_queue_overflow','lost_sequence':sequence,
                             'capacity':capacity,'overflow_received_at':wall})
            raise StorageBackpressure('Research queue full; recording stops') from exc
        telemetry.add('receive_queue_depth',queue.qsize())

    async def delay(n):
        await asyncio.sleep(max(0,min(n,deadline-time.monotonic())))

    async def process():
        nonlocal last_ts
        while True:
            entry=await queue.get()
            try:
                if entry is None:
                    return
                started=time.perf_counter()
                if entry['ts'] < last_ts:
                    issues.append('LOCAL_WALL_CLOCK_REGRESSION')
                    entry['wall_clock_regression']=last_ts-entry['ts']
                entry['ts']=max(last_ts,entry['ts'])
                last_ts=entry['ts']
                telemetry.add('processing_queue_wait',max(0,time.time()-entry['received_at']))
                counts[entry['type']]+=1
                storage.write(entry.pop('type'),entry.pop('ts'),**entry)
                telemetry.add('message_processing',time.perf_counter()-started)
            finally:
                queue.task_done()

    async def clocks():
        while time.monotonic() < deadline:
            wait_seconds=60
            sent=time.time()
            sent_ns=time.monotonic_ns()
            try:
                getter=getattr(api,'get_once',api.get)
                server=await getter(f'{CLOB}/time')
                received=time.time()
                received_ns=time.monotonic_ns()
                emit('clock',received,source=f'{CLOB}/time',
                     request_sent_monotonic_ns=sent_ns,response_received_monotonic_ns=received_ns,
                     request_mode='single_public_GET' if hasattr(api,'get_once') else 'injected_API',
                     **{k:v for k,v in clock_sample(server,sent,received,sent_ns=sent_ns,received_ns=received_ns).items() if k!='received_at'})
            except Exception as exc:
                emit('source_error',source='clock',error=f'{type(exc).__name__}: {exc}')
                issues.append('CLOCK_PROBE_FAILED')
                if isinstance(exc,AccessBlocked):
                    issues.append('CLOCK_ACCESS_DENIED')
                    return
                response=getattr(exc,'response',None)
                if response is not None and response.status_code==429:
                    header=response.headers.get('Retry-After','')
                    try:
                        wait_seconds=max(wait_seconds,float(header))
                    except ValueError:
                        try:
                            wait_seconds=max(wait_seconds,parsedate_to_datetime(header).timestamp()-time.time())
                        except (ValueError,TypeError):
                            pass
            await delay(wait_seconds)

    async def binance():
        try:
            await api.get(BINANCE_TIME)
        except AccessBlocked as exc:
            emit('source_error',source='Binance',error=str(exc),access_denied=True)
            issues.append('BINANCE_ACCESS_DENIED')
            return
        except Exception as exc:
            emit('source_error',source='Binance',error=str(exc))
            issues.append('BINANCE_PREFLIGHT_FAILED')
            return
        while time.monotonic() < deadline:
            try:
                async with connector(BINANCE_WS,max_queue=256,open_timeout=10,proxy=None) as ws:
                    while time.monotonic() < deadline:
                        try:
                            wire=await asyncio.wait_for(ws.recv(),min(10,deadline-time.monotonic()))
                        except asyncio.TimeoutError:
                            continue
                        received=time.time()
                        mono=time.monotonic_ns()
                        message=json.loads(wire)
                        emit('binance',received,message=message,socket_monotonic_ns=mono,
                             exchange_ts=float(message['T'])/1000,source='BTCUSDT_trade')
            except Exception as exc:
                emit('gap',source='Binance',reason=str(exc))
                issues.append('BINANCE_GAP')
                await delay(2)

    async def resolutions():
        while time.monotonic() < deadline:
            for cid,market in list(markets.items()):
                if cid in settled or time.time() < market.end+30:
                    continue
                try:
                    rows=await api.get(f'{GAMMA}/markets',{'slug':market.slug,'closed':'true'})
                    for raw in rows:
                        winner=resolution(raw,market)
                        if winner:
                            emit('settlement',condition_id=cid,winner=winner,evidence=raw,
                                 reference=reference_metadata(raw),reference_available_at=time.time())
                            settled.add(cid)
                except Exception as exc:
                    emit('source_error',source='settlement',error=str(exc),condition_id=cid)
            await delay(30)

    async def market_reader():
        current=None
        connection_id=0
        while time.monotonic() < deadline:
            context={}
            ws=None
            try:
                slug=f'btc-updown-5m-{int(time.time()//300)*300}'
                rows=await api.get(f'{GAMMA}/markets',{'slug':slug})
                raw=next(x for x in rows if x.get('slug')==slug)
                market=parse_market(raw)
                if current != market.condition_id:
                    emit('market',market=market.data())
                    markets[market.condition_id]=market
                    current=market.condition_id
                emit('metadata',condition_id=market.condition_id,raw=raw,reference=reference_metadata(raw))
                connection_id+=1
                context={'connection_id':connection_id,'condition_id':market.condition_id}
                def lifecycle(phase,**fields):
                    emit('socket_lifecycle',phase=phase,**context,**fields)
                def controls(kind,received=None,**fields):
                    emit(kind,received,**context,**fields)
                lifecycle('connect_started',market_end=market.end)
                async with connector(WS,max_queue=256,open_timeout=10,ping_interval=20,ping_timeout=20,
                                     proxy=None,create_connection=TimedClientConnection) as ws:
                    lifecycle('opened')
                    lifecycle('subscribe_started')
                    await ws.send(json.dumps({'type':'market','assets_ids':list(market.tokens)}))
                    lifecycle('subscribed')
                    first=True
                    snapshots=set()
                    async for wire,received,mono in market_messages(ws,deadline,market.end,controls):
                        timing=getattr(ws,'last_message_timing',None) or {}
                        delivery={**timing,'application_received_monotonic_ns':mono,
                                  'application_received_at':received}
                        delivery.setdefault('socket_monotonic_ns',mono)
                        delivery.setdefault('socket_timestamp_scope','APPLICATION_WS_RECV_RETURN_FALLBACK')
                        if timing:
                            delivery['websocket_buffer_wait_seconds']=(mono-timing['socket_monotonic_ns'])/1e9
                        if wire in ('PONG','PING'):
                            controls('socket_control',received,value=wire,**delivery)
                            continue
                        if first:
                            controls('socket_lifecycle',received,phase='first_frame',socket_monotonic_ns=mono)
                            first=False
                        parsed=json.loads(wire)
                        for message in parsed if isinstance(parsed,list) else [parsed]:
                            ex=message.get('timestamp')
                            ex=float(ex) if ex is not None else None
                            if ex is not None and ex > 1e12:
                                ex/=1000
                            controls('frame',received,message=message,exchange_ts=ex,**delivery,
                                     receive_timestamp_scope='APPLICATION_WS_RECV_RETURN')
                            if message.get('event_type')=='book' and message.get('asset_id') in market.tokens:
                                snapshots.add(message['asset_id'])
                                if set(market.tokens)==snapshots:
                                    controls('socket_lifecycle',received,phase='initial_snapshots_received',socket_monotonic_ns=mono)
                                    snapshots.add('__complete__')
                    lifecycle('close_requested',reason='market_end' if time.time()>=market.end else 'session_deadline')
                lifecycle('closed',reason='local_context_exit')
            except AccessBlocked as exc:
                emit('source_error',source='Polymarket',error=str(exc),access_denied=True)
                issues.append('POLYMARKET_ACCESS_DENIED')
                return
            except StorageBackpressure:
                raise
            except Exception as exc:
                close_timing=getattr(ws,'last_close_timing',None)
                if close_timing:
                    emit('socket_lifecycle',close_timing['socket_received_at'],phase='close_frame_received',
                         **context,**close_timing)
                emit('socket_lifecycle',phase='failure_observed',**context,
                     error_type=type(exc).__name__,reason=str(exc),
                     received_close_code=getattr(getattr(exc,'rcvd',None),'code',None),
                     sent_close_code=getattr(getattr(exc,'sent',None),'code',None))
                emit('gap',source='Polymarket',reason=f'{type(exc).__name__}: {exc}',**context)
                issues.append('POLYMARKET_GAP')
                await delay(2)

    async def ticks():
        target=time.monotonic()
        last=target
        while time.monotonic() < deadline:
            target+=.1
            await delay(max(0,target-time.monotonic()))
            telemetry.add('loop_lag',max(0,time.monotonic()-target))
            if time.monotonic()-last >= 1:
                emit('tick')
                last=time.monotonic()

    def summary():
        return {'mode':'RECORD_ONLY','fills':0,'orders':0,'counts':dict(counts),
                'requested_seconds':seconds,'markets':len(markets),'settlements':len(settled),
                'issues':dict(Counter(issues)),'telemetry':telemetry.report(),
                'execution_quality':'INCOMPLETE_DATA' if issues else 'UNASSESSED_RECORDING'}

    started=time.time()
    emit('research_header',mode='RECORD_ONLY',requested_seconds=seconds,schema=2,
         receive_timestamp_scope='ASYNCIO_TRANSPORT_AND_APPLICATION_DELIVERY',connection_overlap=False,
         network_route='DIRECT_NO_PROXY')
    consumer=asyncio.create_task(process())
    producers=[asyncio.create_task(f()) for f in (clocks,binance,resolutions,market_reader,ticks)]
    group=asyncio.gather(*producers)
    try:
        done,_=await asyncio.wait([consumer,group],return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        if consumer.done():
            raise RuntimeError('Recorder processor stopped before readers')
    except BaseException as exc:
        issues.append(f'RECORDING_FAILED:{type(exc).__name__}')
        failure=f'{type(exc).__name__}: {exc}'
        raise
    finally:
        for task in producers:
            task.cancel()
        await asyncio.gather(*producers,return_exceptions=True)
        await asyncio.gather(group,return_exceptions=True)
        try:
            if not consumer.done():
                enqueue=queue.put
                await enqueue(None)
                await consumer
            for evidence in overflow:
                storage.write('gap',max(last_ts,time.time()),**evidence)
            if failure:
                storage.write('gap',max(last_ts,time.time()),reason=failure,
                              unprocessed_queue_entries=queue.qsize())
            storage.write('stop',max(last_ts,time.time()),reason='record_only_finished')
            final=summary()
            final.update(started_at=started,finished_at=time.time(),elapsed_seconds=time.time()-started)
            storage.checkpoint(lambda:(Path(directory)/'capture-summary.json').write_text(json.dumps(final,indent=2)))
        finally:
            try:
                await storage.finish()
            finally:
                await api.close()
    return final


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',required=True)
    parser.add_argument('--seconds',type=float,default=7200)
    args=parser.parse_args()
    print(json.dumps(asyncio.run(record(args.out,args.seconds)),indent=2))


if __name__=='__main__':
    main()
