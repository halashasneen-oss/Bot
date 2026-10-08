import argparse
import asyncio
from dataclasses import asdict
import html
import json
import os
import signal
import traceback
import time
from pathlib import Path
import sys

from .config import Config, load
from .engine import Engine, MODEL_VERSION
from .feed import Journal, PublicAPI, CLOB, apply, collect, default_run_dir, read_entries


def atomic_write(path, text, observer=None):
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', encoding='utf-8') as f:
        f.write(text)
        f.flush()
        started = time.perf_counter()
        os.fsync(f.fileno())
        if observer:
            observer('fsync', time.perf_counter()-started)
    os.replace(temp, path)


def write_report(engine, directory, *, quiet=False):
    data = engine.report()
    observer = getattr(engine, 'io_observer', None)
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    atomic_write(path / 'summary.json', json.dumps(data, indent=2, allow_nan=False), observer)
    rows = ''.join(f'<tr><th>{html.escape(k)}</th><td>{html.escape(str(v))}</td></tr>'
                   for k, v in data.items() if k != 'config')
    atomic_write(path / 'report.html', '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
        '<title>Polymarket Paper Report</title><style>body{font:17px system-ui;background:#102033;color:#eff6ff;max-width:900px;margin:30px auto;padding:16px}'
        'td,th{padding:12px;text-align:left;border-bottom:1px solid #426080}table{width:100%;overflow-wrap:anywhere}</style>'
        '<h1>Paper simulation — not live profit</h1><p>محاكاة تقديرية وليست أرباح تداول حقيقي</p><table>' + rows + '</table>', observer)
    # Append audit exactly once; checkpoints do not rewrite an ever-growing file.
    with (path / 'audit.jsonl').open('a', encoding='utf-8') as f:
        for row in engine.audit:
            f.write(json.dumps(row, allow_nan=False) + '\n')
    engine.audit.clear()
    if not quiet:
        print(json.dumps(data, indent=2, allow_nan=False))
        print(f'Report: {path / "report.html"}')


def load_engine(source, *, require_current_model=False, config_override=None):
    engine = None
    for entry in read_entries(source):
        if engine is None:
            if entry.get('type') != 'header' or entry.get('schema') != 1:
                raise ValueError('Journal must start with schema-1 header/config')
            if require_current_model and entry.get('model_version') != MODEL_VERSION:
                raise ValueError('Cannot resume a journal from a different execution model; analyze it separately')
            if require_current_model and entry.get('mode') != 'paper':
                raise ValueError('Only paper accounting journals may be resumed; analyze recordings separately')
            engine = Engine(config_override or Config(**entry['config']))
            engine.source = entry.get('source', 'UNSPECIFIED')
        else:
            apply(engine, entry)
    if engine is None:
        raise ValueError('Empty journal')
    return engine


def replay(source, output):
    engine = load_engine(source)
    write_report(engine, output)


async def doctor():
    api = PublicAPI()
    try:
        server = await api.get(f'{CLOB}/time')
        market = await api.discover(__import__('time').time())
        books = []
        for token in market.tokens:
            b = await api.get(f'{CLOB}/book', {'token_id': token})
            if not isinstance(b.get('bids'), list) or not isinstance(b.get('asks'), list):
                raise ValueError('Unsupported order-book schema')
            books.append({'token': token, 'bids': len(b['bids']), 'asks': len(b['asks'])})
        print(json.dumps({'server_time': server, 'market': market.data(), 'books': books}, indent=2))
    finally:
        await api.close()


def main():
    parser = argparse.ArgumentParser(description='Read-only BTC pair research. No real trading implemented.')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('paper', 'record'):
        p = sub.add_parser(name)
        p.add_argument('--config', default='config/paper.toml')
        p.add_argument('--minutes', type=float)
        p.add_argument('--out', default=None)
        p.add_argument('--resume', default=None, help='Resume public paper journal; inventory retained, discontinuity recorded')
    p = sub.add_parser('replay')
    p.add_argument('journal')
    p.add_argument('--out', default=None)
    p = sub.add_parser('analyze', help='Evaluate recorded public frames with current model/config; not a new live run')
    p.add_argument('journal')
    p.add_argument('--config', default='config/paper.toml')
    p.add_argument('--out', default=None)
    sub.add_parser('doctor')
    args = parser.parse_args()
    try:
        if args.command == 'doctor':
            asyncio.run(doctor())
        elif args.command == 'replay':
            replay(args.journal, args.out or default_run_dir())
        elif args.command == 'analyze':
            engine = load_engine(args.journal, config_override=load(args.config))
            engine.source = 'RECORDED_PUBLIC_RESEARCH' if engine.source == 'PUBLIC_LIVE' else 'RECORDED_' + engine.source
            engine.run_info = {'analysis_only': True, 'run_status': 'RECORDED_DATA_ANALYSIS',
                               'source_run': dict(engine.run_info)}
            write_report(engine, args.out or default_run_dir())
        else:
            cfg = load(args.config)
            if args.minutes is not None:
                cfg = Config(**{**asdict(cfg), 'duration_minutes': args.minutes})
            engine = load_engine(args.resume, require_current_model=True) if args.resume else Engine(cfg)
            if args.resume:
                if engine.source != 'PUBLIC_LIVE':
                    raise ValueError('Only public live journals can be resumed as live')
                # Preserve original accounting/risk settings. Duration is for the
                # NEW segment, not a claim of continuous coverage across downtime.
                if args.config != 'config/paper.toml':
                    raise ValueError('Cannot change risk config when resuming')
            engine.source = "PUBLIC_LIVE"
            journal = Journal(args.out or default_run_dir())
            if args.resume:
                for old in read_entries(args.resume):
                    row = dict(old)
                    kind, ts = row.pop('type'), row.pop('ts')
                    journal.write(kind, ts, **row)
            else:
                journal.write('header', 0, schema=1, model_version=MODEL_VERSION, config=asdict(cfg), source='PUBLIC_LIVE', mode=args.command)
            try:
                async def run():
                    loop = asyncio.get_running_loop()
                    task = asyncio.current_task()
                    loop.add_signal_handler(signal.SIGTERM, task.cancel)
                    try:
                        if args.resume:
                            import time
                            row = journal.write('gap', max(time.time(), engine.now), reason='resumed_after_downtime')
                            apply(engine, row)
                        await collect(engine, journal, cfg.duration_minutes, args.command == 'record',
                                      snapshot_checkpoint=lambda snapshot: write_report(snapshot, journal.directory, quiet=True))
                    finally:
                        loop.remove_signal_handler(signal.SIGTERM)
                asyncio.run(run())
            finally:
                journal.close()
                if args.command in ('paper', 'record'):
                    write_report(engine, journal.directory)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print('Stopped by operator; inspect partial run and unsettled inventory.', file=sys.stderr)
        return 130
    except Exception as exc:
        traceback.print_exc()
        print(f'FAILED: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

