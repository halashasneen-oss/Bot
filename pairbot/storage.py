"""Single-owner bounded I/O worker. Queue overflow is fatal, never silent loss."""
import asyncio
import copy
import json
import queue
import threading
import time
from collections import defaultdict, deque
from .measure import distribution


class StorageBackpressure(RuntimeError):
    pass


class Telemetry:
    def __init__(self):
        self.lock = threading.Lock()
        self.values = defaultdict(lambda: deque(maxlen=4096))
        self.counts = defaultdict(int)

    def add(self, name, value):
        with self.lock:
            self.counts[name] += 1
            self.values[name].append(value)

    def report(self):
        with self.lock:
            return {k: {**distribution(v), 'total_n': self.counts[k],
                        'window': 'latest 4096 observations'} for k,v in self.values.items()}


class ReportSnapshot:
    def __init__(self, engine):
        self.data = copy.deepcopy(engine.report())
        self.audit = list(engine.audit)
        engine.audit.clear()

    def report(self):
        return self.data


class StorageWorker:
    def __init__(self, journal, telemetry=None, capacity=8192):
        self.journal = journal
        self.telemetry = telemetry or Telemetry()
        # Reserve slots for explicit failure/status/stop evidence and shutdown.
        self.capacity = capacity
        self.jobs = queue.Queue(capacity+32)
        self.failure = None
        self.closed = False
        self.journal.observer = self.observe
        self.thread = threading.Thread(target=self.run, name='paper-journal-io', daemon=True)
        self.thread.start()

    def observe(self, name, elapsed):
        self.telemetry.add(name, elapsed)
        # Called only by the owner thread; every fsync/hash/report duration retained.
        self.timing_file.write(json.dumps({'operation': name, 'seconds': elapsed,
                                         'local_wall_ts': time.time()})+'\n')

    def submit(self, fn, *args, emergency=False, **kwargs):
        if self.failure:
            raise RuntimeError('Journal I/O worker failed') from self.failure
        if self.closed:
            raise RuntimeError('Storage worker closed')
        if not emergency and self.jobs.qsize() >= self.capacity:
            self.telemetry.add('io_queue_overflow', 1)
            raise StorageBackpressure('I/O queue full; stopping session, no silent drops')
        try:
            self.jobs.put_nowait((fn, args, kwargs))
        except queue.Full as exc:
            raise StorageBackpressure('Emergency I/O queue full') from exc
        self.telemetry.add('io_queue_depth', self.jobs.qsize())

    def write(self, kind, ts, **payload):
        entry = {'type': kind, 'ts': ts, **payload}
        self.submit(self.journal.write, kind, ts, emergency=kind in {'error','status','stop','gap'}, **payload)
        return entry

    def checkpoint(self, callback):
        def write():
            start = time.perf_counter()
            try:
                callback()
            finally:
                self.observe('write_report', time.perf_counter()-start)
        self.submit(write, emergency=True)

    def run(self):
        try:
            self._run()
        except BaseException as exc:
            self.failure = self.failure or exc
        finally:
            self.journal.observer = None

    def _run(self):
        with (self.journal.directory/'io-timings.jsonl').open('a', encoding='utf-8') as timing:
            self.timing_file = timing
            while True:
                job = self.jobs.get()
                try:
                    if job is None:
                        break
                    fn, args, kwargs = job
                    if self.failure is None:
                        fn(*args, **kwargs)
                except BaseException as exc:
                    self.failure = exc
                finally:
                    self.jobs.task_done()
            try:
                self.journal.flush()
                self.journal.close()
            except BaseException as exc:
                self.failure = self.failure or exc
        self.journal.observer = None

    async def finish(self):
        self.closed = True
        if not self.thread.is_alive():
            if self.failure:
                raise RuntimeError('Journal I/O worker failed; results incomplete') from self.failure
            return
        await asyncio.to_thread(self.jobs.put, None)
        await asyncio.to_thread(self.thread.join)
        if self.failure:
            raise RuntimeError('Journal I/O worker failed; results incomplete') from self.failure
