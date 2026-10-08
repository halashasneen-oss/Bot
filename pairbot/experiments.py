"""Offline replay matrix. Never imports or calls a live collector or public API."""
import argparse
from bisect import bisect_left
from dataclasses import replace
import hashlib
import gzip
import json
from pathlib import Path
import types

from .config import Config
from .engine import Engine
from .feed import read_entries, apply


def accounting(data):
    return {k: v for k,v in data.items() if k not in {'measurements', 'run', 'config'}}


def summarize(engine, rows):
    data = engine.report()
    ms = data['measurements']['markets']
    timeline = {}
    peak_one_sided_cost = 0.0
    one_sided_seconds = 0.0
    previous_ts = None
    was_one_sided = False
    guards = [x for x in engine.audit if x['type'] in {'movement_pause','guard_trigger'}]
    # Analysis reconstructs fresh price observations separately; no future data
    # is fed to the execution engine.
    probe = Engine(engine.cfg)
    for row in rows:
        if previous_ts is not None and was_one_sided:
            one_sided_seconds += max(0., row['ts']-previous_ts)
        apply(probe, row)
        previous_ts = row['ts']
        was_one_sided = False
        for cid in probe.exposed_markets:
            m = probe.markets[cid]
            a,b = probe.positions[m.up],probe.positions[m.down]
            held = a if a.size>b.size else b
            if abs(a.size-b.size)>1e-8:
                was_one_sided = True
                peak_one_sided_cost = max(peak_one_sided_cost,held.cost/held.size*abs(a.size-b.size))
        if row['type']=='frame' and probe.fresh():
            m = probe.markets[probe.active]
            b,a = probe.books[m.up].best_bid(),probe.books[m.up].best_ask()
            if b and a:
                timeline.setdefault(probe.active, []).append((probe.now,(a.price+b.price)/2,probe.gaps))
        probe.audit.clear()
    outcomes = []
    for g in guards:
        points = timeline.get(g['condition_id'], [])
        times = [p[0] for p in points]
        i = bisect_left(times, g['ts'])
        base = points[i] if i < len(points) and times[i]-g['ts'] <= engine.cfg.stale_seconds else None
        result = dict(g)
        for horizon in (5,20):
            j = bisect_left(times,g['ts']+horizon)
            future = points[j] if j<len(points) and times[j]-(g['ts']+horizon)<=engine.cfg.stale_seconds else None
            result[f'mid_change_after_{horizon}s'] = future[1]-base[1] if base and future and base[2]==future[2] else None
        outcomes.append(result)
    fresh = sum(r['fresh_bid_seconds'] for r in ms.values())
    edge = sum(r['edge_002_seconds'] for r in ms.values())
    return {'observed_market_seconds':sum(r['observed_seconds'] for r in ms.values()),
            'live_order_seconds_sum':sum(sum(r['live_order_seconds'].values()) for r in ms.values()),
            'cooldown_seconds':sum(r['cooldown_seconds'] for r in ms.values()),
            'fresh_bid_seconds':fresh, 'edge_002_seconds':edge,
            'edge_002_fraction_of_fresh_time':edge/fresh if fresh else None,
            'execution_diagnostics':data['execution_diagnostics'],
            'fills':data['fills'],'merges':data['merges'],'pnl_marked':data['pnl_marked'],
            'pnl_realized':data['pnl_realized'],'max_drawdown':data['max_drawdown'],
            'execution_quality':data['execution_quality'],
            'fill_evidence':[x for x in engine.audit if x['type']=='fill'],
            'peak_unpaired_cost_at_risk':peak_one_sided_cost,
            'one_sided_inventory_seconds':one_sided_seconds,
            'quote_plan_evidence':[x for x in engine.audit if x['type']=='quote'],
            'worst_case_remaining_unpaired_cost':sum(p.cost for p in engine.positions.values()),
            'loss_stop_usd':engine.cfg.loss_stop_usd,
            'guard_forward_outcomes':outcomes,
            'quote_limits_below_top': 'Current _quote_plan retained; each quote records edge-limited prices',
            'measurements':data['measurements']}


def run(rows, cfg):
    engine = Engine(cfg)
    for row in rows:
        apply(engine,row)
    return engine


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('journal')
    parser.add_argument('--out',required=True)
    parser.add_argument('--reference-manifest', help='Expected frame count and canonical SHA256')
    parser.add_argument('--baseline-engine', help='Local original engine.py for independent invariance check')
    args=parser.parse_args()
    digest=hashlib.sha256(Path(args.journal).read_bytes()).hexdigest() if Path(args.journal).is_file() else None
    rows=list(read_entries(args.journal))
    if args.reference_manifest:
        expected=json.loads(Path(args.reference_manifest).read_text())
        assert digest==expected['canonical_jsonl_sha256'], 'Reference checksum changed'
        assert sum(r['type']=='frame' for r in rows)==expected['frames'], 'Wrong reference frame count'
    cfg=Config(**rows[0]['config'])
    output=Path(args.out); output.mkdir(parents=True,exist_ok=True)
    baseline=run(rows,cfg)
    if args.baseline_engine:
        module=types.ModuleType('pairbot.original_engine');module.__package__='pairbot'
        import sys
        sys.modules[module.__name__]=module
        exec(compile(Path(args.baseline_engine).read_text(),args.baseline_engine,'exec'),module.__dict__)
        class Original(module.Engine):
            def frame(self,msg,ts,*,received_at=None):
                return super().frame(msg,ts)
        old=Original(cfg)
        for row in rows:
            apply(old,row)
        assert accounting(old.report())==accounting(baseline.report()), 'Default behavior changed'
    second=run(rows,cfg)
    assert baseline.report()==second.report(), 'Replay is not deterministic'
    variants=[('default',cfg)]
    for mode in ('up_mid','combined_mid','combined_bid'):
        for warmup in (0.,30.):
            if mode=='up_mid' and warmup==0:continue
            variants.append((f'{mode}_warmup_{int(warmup)}',replace(cfg,guard_mode=mode,guard_warmup_seconds=warmup)))
    for q in (1.,1.5,2.):
        for v in (.5,1.,1.5):
            if q==1.5 and v==1:continue
            variants.append((f'queue_{q}_volume_{v}',replace(cfg,queue_multiplier=q,trade_volume_multiplier=v)))
    results={}
    for name,setting in variants:
        engine=baseline if name=='default' else run(rows,setting)
        summary=summarize(engine,rows)
        # Full observation rows retained once; alternative runs keep their own
        # compact time/latency measures and execution evidence.
        if name != 'default':
            summary['measurements']={k:v for k,v in summary['measurements'].items()
                                     if k not in {'buy_prints','above_limit_sells'}}
        else:
            observations={k:summary['measurements'].pop(k) for k in ('buy_prints','above_limit_sells')}
            observations['guard_forward_outcomes']=summary['guard_forward_outcomes']
            (output/'default-observations.json.gz').write_bytes(
                gzip.compress(json.dumps(observations,indent=2,allow_nan=False).encode(),mtime=0))
            summary['observation_rows_file']='default-observations.json.gz'
        (output/(name+'.json')).write_text(json.dumps(summary,indent=2,allow_nan=False)+'\n')
        results[name]={k:v for k,v in summary.items() if k not in {'measurements','guard_forward_outcomes'}}
        print(name,summary['fills'],summary['merges'],round(summary['live_order_seconds_sum'],3),round(summary['cooldown_seconds'],3),flush=True)
    payload={'frames':sum(r['type']=='frame' for r in rows),
             'default_invariant_against_original':bool(args.baseline_engine),
             'default_replay_deterministic':True,
             'io_before_after': 'Cannot recover historical fsync/loop/queue timing. Offline replay does not establish live I/O improvement.',
             'complementary_fills':'Unavailable pending server-match proof; no inferred fills or above-limit queue depletion',
             'results':results}
    if digest:
        assert hashlib.sha256(Path(args.journal).read_bytes()).hexdigest()==digest, 'Input mutated during replay'
    payload['reference_sha256']=digest
    (output/'comparison.json').write_text(json.dumps(payload,indent=2,allow_nan=False)+'\n')


if __name__=='__main__':
    main()
