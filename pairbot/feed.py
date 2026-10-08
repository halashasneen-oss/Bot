"""Public read-only HTTP + WebSocket capture. No signing SDK, wallet or POST."""
import asyncio
import gzip
import hashlib
import os
import traceback
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
from pathlib import Path
import time

import httpx
from websockets.asyncio.client import connect

from .market import Market, parse_market, resolution
from .storage import StorageWorker, Telemetry, ReportSnapshot, StorageBackpressure

GAMMA = 'https://gamma-api.polymarket.com'
CLOB = 'https://clob.polymarket.com'
WS = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'


class AccessBlocked(RuntimeError):
    pass


class DataUnavailable(RuntimeError):
    pass


class PublicAPI:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=10, follow_redirects=False, trust_env=False)

    async def close(self):
        await self.client.aclose()

    async def get_once(self, url, params=None):
        """One public GET for timing probes: no retry/backoff inside the RTT."""
        response = await self.client.get(url, params=params)
        if response.status_code in (401, 403, 451):
            raise AccessBlocked(f'Public endpoint returned HTTP {response.status_code}: {url}; no bypass attempted')
        response.raise_for_status()
        return response.json()

    async def get(self, url, params=None):
        for attempt in range(4):
            delay = 2 ** attempt
            try:
                r = await self.client.get(url, params=params)
                if r.status_code in (401, 403, 451):
                    raise AccessBlocked(f'Public endpoint returned HTTP {r.status_code}: {url}; no bypass attempted')
                if r.status_code == 429:
                    header = r.headers.get('Retry-After', '')
                    try:
                        delay = max(delay, float(header))
                    except ValueError:
                        if header:
                            try:
                                delay = max(delay, parsedate_to_datetime(header).timestamp() - time.time())
                            except (ValueError, TypeError):
                                pass
                elif r.status_code < 500:
                    r.raise_for_status()
                    return r.json()
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt == 3:
                    raise
            if attempt < 3:
                await asyncio.sleep(delay)
        raise DataUnavailable(f'Public API unavailable after retries: {url}')

    async def discover(self, now):
        start = int(now // 300) * 300
        slug = f'btc-updown-5m-{start}'
        rows = await self.get(f'{GAMMA}/markets', {'slug': slug})
        if not isinstance(rows, list):
            raise ValueError('Unexpected Gamma schema')
        for raw in rows:
            if raw.get('slug') == slug:
                return parse_market(raw)
        raise ValueError(f'Current market unavailable: {slug}')

    async def resolved(self, market):
        rows = await self.get(f'{GAMMA}/markets', {'slug': market.slug, 'closed': 'true'})
        if not isinstance(rows, list):
            raise ValueError('Unexpected settlement schema')
        for raw in rows:
            winner = resolution(raw, market)
            if winner:
                return winner
        return None


class Journal:
    """Compressed rotating shards + durable manifests, never overwrite a run."""
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.index = 0
        self.bytes_written = 0
        self.last_flush = time.monotonic()
        self.file = None
        self.active_path = None
        self.shards = []
        self.sealed = {}
        self.observer = None
        self.flush_seconds = 45.0
        self._rotate()

    def _fsync(self, fd):
        start = time.perf_counter()
        try:
            os.fsync(fd)
        finally:
            if self.observer:
                self.observer("fsync", time.perf_counter()-start)

    def _rotate(self):
        self._seal()
        self.index += 1
        name = f'events-{self.index:06d}.jsonl.gz'
        self.active_path = self.directory / (name + '.partial')
        self.file = gzip.open(self.active_path, 'xt', encoding='utf-8', compresslevel=1)
        self.bytes_written = 0
        self.manifest()

    def manifest(self):
        path = self.directory / 'journal.json'
        temp = path.with_suffix('.tmp')
        with temp.open('w', encoding='utf-8') as f:
            f.write(json.dumps({'schema': 1, 'shards': self.shards, 'sealed': self.sealed,
                               'active_shard': self.active_path.name if self.active_path else None}, indent=2))
            f.flush()
            self._fsync(f.fileno())
        os.replace(temp, path)
        if os.name == 'posix':
            fd = os.open(self.directory, os.O_RDONLY)
            try:
                self._fsync(fd)
            finally:
                os.close(fd)

    def write(self, kind, ts, **payload):
        entry = {'type': kind, 'ts': ts, **payload}
        line = json.dumps(entry, allow_nan=False) + '\n'
        self.file.write(line)
        self.bytes_written += len(line.encode())
        if self.bytes_written >= 16 * 1024 * 1024:
            self._rotate()
        if time.monotonic() - self.last_flush > self.flush_seconds or kind in {'error', 'status', 'stop'}:
            self.flush()
        return entry

    def flush(self):
        self.file.flush()
        self._fsync(self.file.fileno())
        self.last_flush = time.monotonic()

    def _seal(self):
        if self.file is None:
            return
        self.file.close()  # finish gzip footer before persisting the sealed shard
        self.file = None
        partial = self.active_path
        path = self.directory / partial.name.removesuffix('.partial')
        with partial.open('rb') as f:
            self._fsync(f.fileno())
        os.replace(partial, path)
        if os.name == 'posix':
            fd = os.open(self.directory, os.O_RDONLY)
            try:
                self._fsync(fd)
            finally:
                os.close(fd)
        self.active_path = None
        self.shards.append(path.name)
        start = time.perf_counter()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if self.observer:
            self.observer('sha256', time.perf_counter()-start)
        self.sealed[path.name] = {'bytes': path.stat().st_size, 'sha256': digest}

    def close(self):
        self._seal()
        self.manifest()


def read_entries(source):
    path = Path(source)
    if path.is_dir():
        manifest = json.loads((path / 'journal.json').read_text())
        if manifest.get('active_shard'):
            raise ValueError('Journal contains an active/interrupted shard; complete replay unavailable')
        files = [path / name for name in manifest['shards']]
        if any(x.parent != path or x.name != str(name) for x, name in zip(files, manifest['shards'])):
            raise ValueError('Invalid journal shard path')
    else:
        files = [path]
    for file in files:
        if path.is_dir() and file.name in manifest.get('sealed', {}):
            expected = manifest['sealed'][file.name]
            if (file.stat().st_size != expected['bytes'] or
                    hashlib.sha256(file.read_bytes()).hexdigest() != expected['sha256']):
                raise ValueError('Journal shard changed or truncated: ' + file.name)
        opener = gzip.open if file.suffix == '.gz' else open
        with opener(file, 'rt', encoding='utf-8') as f:
            for line in f:
                yield json.loads(line)


def apply(engine, entry):
    kind, ts = entry['type'], float(entry['ts'])
    if kind == 'market':
        engine.select(Market(**entry['market']), ts)
    elif kind == 'frame':
        engine.frame(entry['message'], ts, received_at=entry.get('received_at', ts))
    elif kind == 'gap':
        engine.gap(ts)
    elif kind == 'settlement':
        engine.settle(entry['condition_id'], entry['winner'], ts)
    elif kind == 'stop':
        engine.paused = True
        engine.advance(ts)
        engine.cancel_all()
    elif kind == 'tick':
        engine.advance(ts)
        engine.quote()
    elif kind == 'status':
        engine.run_info.update(entry['info'])
    elif kind not in {'header', 'error'}:
        raise ValueError(f'Unknown journal event: {kind}')


class ResyncRequired(RuntimeError):
    pass


async def collect(engine, journal, minutes, record_only=False, *, api=None,
                  connector=None, wall_clock=None, monotonic=None, checkpoint=None,
                  snapshot_checkpoint=None, receive_capacity=4096, io_capacity=8192):
    # New model/guard switches remain offline-only until validated by evidence.
    if (engine.cfg.guard_mode != 'up_mid' or engine.cfg.guard_warmup_seconds != 0
            or engine.cfg.trade_volume_multiplier != 1 or engine.cfg.complementary_fills):
        raise ValueError('Research switches are offline replay only')
    # Dependency injection is used by outage/rotation tests, not synthetic live data.
    api = api or PublicAPI()
    connector = connector or connect
    wall_clock = wall_clock or time.time
    monotonic = monotonic or time.monotonic
    checkpoint = checkpoint or (lambda: None)
    start_mono = monotonic()
    start_wall = max(wall_clock(), engine.now)
    deadline = start_mono + minutes * 60
    selected = None
    retry = 1
    settled = set()
    tasks = []
    engine.record_only = record_only
    engine.run_info = {'run_status': 'RUNNING', 'requested_seconds': minutes * 60,
                       'elapsed_seconds': 0.0, 'started_at': start_wall,
                       'frames_received': 0, 'connections': 0, 'last_error': None,
                       'requested_duration_completed': False}
    telemetry = Telemetry()
    writer = StorageWorker(journal, telemetry, capacity=io_capacity)
    stop_reason = 'duration_completed'
    last_progress = start_mono

    def now():
        # Never let an NTP wall-clock adjustment reverse journal/engine time.
        return start_wall + monotonic() - start_mono

    def remaining():
        return max(0.0, deadline - monotonic())

    def emit(kind, **payload):
        started = time.perf_counter()
        entry = writer.write(kind, max(now(), engine.now), **payload)
        apply(engine, entry)
        telemetry.add('message_processing_seconds', time.perf_counter()-started)
        return entry

    def save():
        nonlocal last_progress
        engine.run_info['elapsed_seconds'] = monotonic() - start_mono
        engine.run_info['last_checkpoint_at'] = now()
        engine.run_info['pipeline_telemetry'] = telemetry.report()
        emit('status', info=dict(engine.run_info))
        if snapshot_checkpoint:
            snapshot = ReportSnapshot(engine)
            snapshot.io_observer = writer.observe
            writer.checkpoint(lambda: snapshot_checkpoint(snapshot))
        else:
            # Legacy injected callbacks supported for existing external tests.
            writer.checkpoint(checkpoint)
        if monotonic() - last_progress >= 60:
            data = engine.report()
            print('Paper checkpoint: ' + json.dumps({
                'elapsed_seconds': data['run']['elapsed_seconds'], 'cash': data['cash'],
                'fills': data['fills'], 'merges': data['merges'], 'open_orders': len(engine.orders),
                'markets': data['markets_selected'], 'data_gaps': data['data_gaps'],
                'execution_diagnostics': data['execution_diagnostics']}), flush=True)
            last_progress = monotonic()

    def error(exc, stage):
        info = {'stage': stage, 'exception': type(exc).__name__, 'message': str(exc),
                'traceback': ''.join(traceback.format_exception(exc))}
        engine.run_info['last_error'] = {k: v for k, v in info.items() if k != 'traceback'}
        emit('error', **info)
        row = {'ts': now(), **info}
        def append_error():
            with (journal.directory / 'errors.jsonl').open('a', encoding='utf-8') as f:
                f.write(json.dumps(row) + '\n')
        writer.submit(append_error, emergency=True)
        print(f'Data error at {stage}: {type(exc).__name__}: {exc}', flush=True)

    async def bounded(coroutine):
        left = remaining()
        if left <= 0:
            coroutine.close()
            raise asyncio.TimeoutError('Session deadline')
        return await asyncio.wait_for(coroutine, timeout=left)

    async def timer():
        last_save = monotonic()
        while remaining() > 0:
            before = monotonic()
            delay = min(.5, remaining())
            await asyncio.sleep(delay)
            telemetry.add('loop_lag_seconds', max(0., monotonic()-before-delay))
            if monotonic() <= before:
                continue
            emit('tick')
            if monotonic() - last_save >= 45:
                save()
                last_save = monotonic()

    async def settlements():
        # REST awaits happen in their own task, never block WS reads.
        while remaining() > 0:
            for cid, market in list(engine.markets.items()):
                if market.end > now() or cid in settled:
                    continue
                try:
                    winner = await bounded(api.resolved(market))
                    if winner:
                        emit('settlement', condition_id=cid, winner=winner)
                        settled.add(cid)
                except AccessBlocked:
                    raise
                except (httpx.HTTPError, OSError, ValueError, RuntimeError) as exc:
                    error(exc, 'settlement')
            await asyncio.sleep(min(30, remaining()))

    def check_tasks():
        for task in tasks:
            if task.done() and not task.cancelled():
                exc = task.exception()
                if exc:
                    raise exc

    stage = 'discovery'
    try:
        save()
        tasks = [asyncio.create_task(timer()), asyncio.create_task(settlements())]
        while remaining() > 0:
            check_tasks()
            try:
                stage = 'discovery'
                market = await bounded(api.discover(now()))
                if market.end <= now():
                    raise ValueError('Discovered market expired before subscription')
                if selected is not None:
                    emit('stop', reason='rotate_or_reconnect')
                    await asyncio.sleep(min(engine.cfg.cancel_latency_seconds + .01, remaining()))
                    emit('tick')
                # Reconnecting the same market explicitly rebuilds snapshot state.
                emit('market', market=market.data())
                selected = market
                stage = 'websocket_connect'
                async with connector(WS, open_timeout=min(10, remaining()), ping_interval=10,
                                     ping_timeout=10, max_size=4_000_000, max_queue=256, proxy=None) as ws:
                    engine.run_info['connections'] += 1
                    await ws.send(json.dumps({'assets_ids': list(market.tokens), 'type': 'market'}))
                    incoming = asyncio.Queue(maxsize=receive_capacity)
                    stage = 'websocket_receive'

                    async def reader():
                        next_ping = monotonic()+8
                        last_wire = monotonic()
                        while remaining() > 0 and now() < market.end:
                            if monotonic() >= next_ping:
                                await ws.send('PING')
                                next_ping = monotonic()+8
                            try:
                                wire = await asyncio.wait_for(ws.recv(), timeout=min(.5, remaining(), max(.001,market.end-now())))
                            except asyncio.TimeoutError:
                                if monotonic()-last_wire > 15:
                                    raise ResyncRequired('No websocket frames/heartbeat for 15 seconds')
                                continue
                            last_wire = monotonic()
                            received_at = now()
                            if wire in ('PING', 'PONG'):
                                if wire == 'PING':
                                    await ws.send('PONG')
                                continue
                            messages = json.loads(wire)
                            for message in messages if isinstance(messages,list) else [messages]:
                                if not isinstance(message,dict):
                                    raise ValueError('Unexpected WebSocket schema')
                                try:
                                    incoming.put_nowait((received_at, message))
                                except asyncio.QueueFull as exc:
                                    engine.run_info['receive_not_enqueued'] = engine.run_info.get('receive_not_enqueued', 0)+1
                                    telemetry.add('receive_queue_overflow',1)
                                    raise ResyncRequired('Receive queue full; connection stopped; unprocessed frames counted as gap') from exc
                                telemetry.add('receive_queue_depth',incoming.qsize())
                            # Fairness under immediately-ready recv(), no engine or disk work.
                            await asyncio.sleep(0)

                    read_task = asyncio.create_task(reader())
                    try:
                        while not read_task.done() or not incoming.empty():
                            check_tasks()
                            # On transport failure, reject pending execution before draining.
                            if read_task.done() and not read_task.cancelled() and read_task.exception():
                                raise read_task.exception()
                            try:
                                received_at, message = await asyncio.wait_for(incoming.get(),timeout=.1)
                            except asyncio.TimeoutError:
                                continue
                            telemetry.add('internal_queue_wait_seconds',max(0.,now()-received_at))
                            emit('frame',message=message,received_at=received_at)
                            incoming.task_done()
                            engine.run_info['frames_received'] += 1
                            retry = 1
                            if engine.resync_tokens:
                                raise ResyncRequired('Fresh full snapshots required for tokens: '+','.join(sorted(engine.resync_tokens)))
                        await read_task
                    finally:
                        read_task.cancel()
                        await asyncio.gather(read_task, return_exceptions=True)
                        if not incoming.empty():
                            engine.run_info['unprocessed_frames'] = engine.run_info.get('unprocessed_frames',0)+incoming.qsize()
                            emit('gap',reason='connection_closed_with_pending_frames',pending=incoming.qsize())
                emit('stop', reason='market_expiry')
                await asyncio.sleep(min(engine.cfg.cancel_latency_seconds + .01, remaining()))
                emit('tick')
            except AccessBlocked as exc:
                error(exc, stage)
                emit('gap', reason='access_blocked')
                stop_reason = 'access_blocked'
                raise
            except asyncio.TimeoutError as exc:
                if remaining() <= 0:
                    break
                error(exc, stage)
                emit('gap', reason='timeout')
                save()
                await asyncio.sleep(min(retry, remaining()))
                retry = min(retry * 2, 30)
            except Exception as exc:
                from websockets.exceptions import ConnectionClosed, InvalidHandshake
                # Only known transport/data failures are retried. An invariant
                # failure must remain fatal, never buried in a reconnect loop.
                if not isinstance(exc, (OSError, httpx.HTTPError, ValueError,
                                        ResyncRequired, DataUnavailable, ConnectionClosed, InvalidHandshake)):
                    raise
                error(exc, stage)
                emit('gap', reason=type(exc).__name__)
                save()
                await asyncio.sleep(min(retry, remaining()))
                retry = min(retry * 2, 30)
        check_tasks()
        if engine.run_info['frames_received'] == 0:
            stop_reason = 'no_live_data'
            raise RuntimeError('No live frames received; no valid live result')
        engine.run_info['requested_duration_completed'] = remaining() <= 0
    except BaseException as exc:
        if stop_reason == 'duration_completed':
            stop_reason = 'operator_interrupted' if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else 'fatal_error'
        if isinstance(exc, StorageBackpressure) or writer.failure:
            # A journal failure or full I/O queue invalidates execution coverage.
            if writer.failure:
                engine.gap(max(now(),engine.now))
            else:
                emit('gap',reason='io_queue_full')
        if not isinstance(exc, asyncio.CancelledError):
            try:
                error(exc, stage)
            except (StorageBackpressure, RuntimeError):
                engine.run_info['persistence_failed'] = True
        raise
    finally:
        # Cancel workers before final checkpoint. Cleanup must not wait on a
        # cancelled network request before storing evidence of failure.
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if writer.failure:
            engine.run_info['requested_duration_completed'] = False
            stop_reason = 'persistence_failure'
        engine.run_info['run_status'] = 'COMPLETED' if engine.run_info['requested_duration_completed'] else 'FAILED_OR_INTERRUPTED'
        engine.run_info['stop_reason'] = stop_reason
        try:
            emit('stop', reason=stop_reason)
            # Save final status before awaiting cancellation latency, so a second
            # signal cannot leave a failed session labelled RUNNING.
            engine.run_info['run_status'] = 'COMPLETED' if engine.run_info['requested_duration_completed'] else 'FAILED_OR_INTERRUPTED'
            engine.run_info['stop_reason'] = stop_reason
            save()
            await asyncio.sleep(engine.cfg.cancel_latency_seconds + .01)
            emit('tick')
            engine.run_info['run_status'] = 'COMPLETED' if engine.run_info['requested_duration_completed'] else 'FAILED_OR_INTERRUPTED'
            engine.run_info['stop_reason'] = stop_reason
            save()
        finally:
            try:
                await asyncio.wait_for(api.close(), timeout=2)
            except Exception as exc:
                try:
                    error(exc, 'api_cleanup')
                    save()
                except (RuntimeError, StorageBackpressure):
                    engine.run_info['persistence_failed'] = True
            finally:
                await writer.finish()



def default_run_dir():
    return 'runs/' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
