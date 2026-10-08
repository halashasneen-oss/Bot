"""Frozen descriptive timing study. Never evaluates a strategy or holdout."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from statistics import median
from .config import Config
from .engine import Engine
from .feed import read_entries
from .research import BoundedMeasurements, input_hash, research_apply


def bucket(value, edges):
    for a,b in zip(edges,edges[1:]):
        if a<=value<b:
            return f'[{a},{b})'
    return 'outside'


def clusters(frames, maximum_gap):
    result=[]
    for frame in sorted(frames,key=lambda x:x['received']):
        if not result or frame['received']-result[-1][-1]['received']>maximum_gap:
            result.append([])
        result[-1].append(frame)
    return [{'n':len(xs),'start':xs[0]['received'],'end':xs[-1]['received'],
             'receive_span':xs[-1]['received']-xs[0]['received'],
             'exchange_span':max(x['exchange'] for x in xs)-min(x['exchange'] for x in xs),
             'median_age':median(x['age'] for x in xs),'max_age':max(x['age'] for x in xs),
             'types':dict(Counter(x['kind'] for x in xs))} for xs in result]


def bad_clock_intervals(start, end, clocks, limit):
    """A probe bounds the following interval; never backfill an earlier window."""
    previous=start
    uncertainty=None
    for probe in sorted(clocks,key=lambda x:x['ts']):
        stop=min(end,max(start,probe['ts']))
        if uncertainty is None or uncertainty>limit:
            yield previous,stop
        previous=stop
        uncertainty=probe['uncertainty_seconds']
    if uncertainty is None or uncertainty>limit:
        yield previous,end


def analyze(source,policy_path,freeze_commit):
    policy_body=Path(policy_path).read_bytes()
    policy=json.loads(policy_body)
    digest=input_hash(source)
    rows=list(read_entries(source))
    start=next(r['ts'] for r in rows if r['type']=='research_header')
    end=next(r['ts'] for r in reversed(rows) if r['type']=='stop')
    bin_size=policy['clean_window']['bin_seconds']
    reasons=defaultdict(set)
    def reject(a,b,reason):
        a,b=max(start,a),min(end,b)
        if b<=a:
            return
        for i in range(int(math.floor((a-start)/bin_size)),int(math.ceil((b-start)/bin_size))):
            reasons[i].add(reason)
    e=Engine(Config())
    e.record_only=True
    e.measurements=BoundedMeasurements()
    prev=start
    frames=[]
    connections=[]
    gaps=[]
    markets={}
    conn=None
    missing_timestamps=0
    cw=policy['clean_window']
    clocks=[]
    for r in rows:
        ts=r['ts']
        if ts>prev:
            if not e.active or not e.fresh():
                reject(prev,ts,'book_not_initialized_or_not_fresh')
            else:
                m=e.markets[e.active]
                expiry=min(*(e.received[t]+cw['raw_age_max_seconds'] for t in m.tokens),
                           *(e.exchange_ts[t]+cw['raw_age_max_seconds'] for t in m.tokens))
                reject(max(prev,expiry),ts,'book_age_over_limit')
            prev=ts
        kind=r['type']
        if kind=='market':
            markets[r['market']['condition_id']]=r['market']
        elif kind=='metadata':
            cid=r['condition_id']
            conn={'id':len(connections),'market':cid,'metadata':r.get('received_at',ts),
                  'first_frame':None,'first_books':{},'snapshot_complete':None,'close':None}
            connections.append(conn)
        elif kind=='clock':
            clocks.append(r)
        elif kind=='gap':
            gap={'ts':r.get('received_at',ts),'reason':r.get('reason'),
                 'source':r.get('source'),'connection':conn['id'] if conn else None}
            if conn:
                conn['close']=gap['ts']
                m=markets[conn['market']]
                gap.update(market=conn['market'],market_start=m['start'],market_end=m['end'],
                           close_minus_market_end=gap['ts']-m['end'])
            gaps.append(gap)
            reject(gap['ts']-cw['event_guard_before_seconds'],gap['ts']+cw['event_guard_after_seconds'],'gap_event_guard')
        elif kind=='frame':
            received=r.get('received_at',ts)
            msg=r['message']
            if conn and conn['first_frame'] is None:
                conn['first_frame']=received
            if conn and msg.get('event_type')=='book':
                conn['first_books'].setdefault(msg['asset_id'],received)
                tokens=markets[conn['market']]['up'],markets[conn['market']]['down']
                if conn['snapshot_complete'] is None and all(t in conn['first_books'] for t in tokens):
                    conn['snapshot_complete']=max(conn['first_books'][t] for t in tokens)
            ex=msg.get('timestamp')
            if ex is None:
                missing_timestamps+=1
                reject(received,received+1e-6,'missing_exchange_timestamp')
            else:
                ex=float(ex)
                if ex>1e12:
                    ex/=1000
                age=received-ex
                item={'received':received,'exchange':ex,'age':age,
                      'kind':msg.get('event_type','unknown'),'connection':conn['id'] if conn else None,
                      'socket_ns':r.get('socket_monotonic_ns')}
                frames.append(item)
                if age>cw['raw_age_max_seconds']:
                    reject(received,received+1e-6,'late_frame')
                if age<-cw['clock_uncertainty_max_seconds']:
                    reject(received,received+1e-6,'exchange_time_in_future')
        research_apply(e,r)
        e.audit.clear()
    for c in connections:
        last=c['first_frame'] or c['metadata']
        reject(c['metadata']-cw['event_guard_before_seconds'],last+cw['event_guard_after_seconds'],'subscription_proxy_guard')
        if c['snapshot_complete'] is not None:
            reject(c['metadata'],c['snapshot_complete'],'initial_snapshots_incomplete')
        else:
            reject(c['metadata'],c['close'] or end,'initial_snapshots_incomplete')
    for a,b in bad_clock_intervals(start,end,clocks,cw['clock_uncertainty_max_seconds']):
        reject(a,b,'clock_bound_unverified')
    n_bins=math.ceil((end-start)/bin_size)
    clean_seconds=sum(min(bin_size,end-start-i*bin_size) for i in range(n_bins) if i not in reasons)
    def is_clean(t):
        return start<=t<end and int((t-start)//bin_size) not in reasons
    late=[x for x in frames if x['age']>cw['raw_age_max_seconds']]
    for g in gaps:
        if g['connection'] is None:
            continue
        c=connections[g['connection']]
        g['connection_age_bounds']=[g['ts']-(c['first_frame'] or c['metadata']),g['ts']-c['metadata']]
        following=connections[c['id']+1:]
        next_c=following[0] if following else None
        next_market=next((x for x in following if x['market']!=c['market']),None)
        g['next_subscription_proxy_delay']=None if next_c is None else next_c['metadata']-g['ts']
        g['next_first_frame_delay']=None if next_c is None or next_c['first_frame'] is None else next_c['first_frame']-g['ts']
        g['next_market_subscription_delay']=None if next_market is None else next_market['metadata']-g['ts']
        before=[x for x in frames if x['connection']==c['id'] and g['ts']-60<=x['received']<g['ts']]
        g['last_60_seconds_age_medians']=[{'seconds_before_close':[a,a+10],
            'n':len(xs),'median_age':median(x['age'] for x in xs) if xs else None}
            for a in range(-60,0,10)
            for xs in [[x for x in before if a<=x['received']-g['ts']<a+10]]]
    guards=[]
    for g in gaps:
        guards.append((g['ts']-cw['event_guard_before_seconds'],g['ts']+cw['event_guard_after_seconds']))
    for c in connections:
        guards.append((c['metadata']-cw['event_guard_before_seconds'],(c['first_frame'] or c['metadata'])+cw['event_guard_after_seconds']))
    outside_guard=[x for x in late if not any(a<=x['received']<=b for a,b in guards)]
    late_per_kind=Counter(x['kind'] for x in late)
    hist=defaultdict(Counter)
    event_hist=defaultdict(Counter)
    hist_by_kind=defaultdict(lambda: defaultdict(Counter))
    snapshot_near=Counter()
    per_market=defaultdict(Counter)
    batches=Counter(x['socket_ns'] for x in late)
    for x in late:
        if x['connection'] is None:
            continue
        c=connections[x['connection']]
        m=markets[c['market']]
        for dimension,age in [('connection_metadata_proxy_age',x['received']-c['metadata']),
                              ('connection_first_frame_age',x['received']-(c['first_frame'] or c['metadata'])),
                              ('market_age',x['received']-m['start'])]:
            key=bucket(age,policy['temporal_buckets_seconds'])
            hist[dimension][key]+=1
            hist_by_kind[dimension][key][x['kind']]+=1
        per_market[m['slug']][f"minute_{int((x['received']-m['start'])//60)}:{x['kind']}"]+=1
        for label,times in [('closure',[g['ts'] for g in gaps]),('subscription_proxy',[z['metadata'] for z in connections])]:
            if times:
                nearest=min(times,key=lambda t:abs(x['received']-t))
                key=bucket(x['received']-nearest,policy['event_relative_buckets_seconds'])
                event_hist[label][key]+=1
                hist_by_kind['nearest_'+label][key][x['kind']]+=1
        if c['first_books']:
            first=min(c['first_books'].values())
            snapshot_near['within_1s_after_first_book' if 0<=x['received']-first<=policy['initial_snapshot_near_seconds'] else 'other']+=1
    quality={'full_seconds':end-start,'clean_seconds':clean_seconds,
             'clean_fraction':clean_seconds/(end-start),'excluded_fraction':1-clean_seconds/(end-start),
             'bin_counts_by_reason_overlapping':dict(Counter(reason for rs in reasons.values() for reason in rs)),
             'clean_bins':[i for i in range(n_bins) if i not in reasons]}
    cohorts=[]
    for label,xs in [('full',frames),('clean',[x for x in frames if is_clean(x['received'])])]:
        samples=defaultdict(lambda: {'frames':0,'late':0,'types':Counter()})
        for x in xs:
            if x['connection'] is not None:
                m=markets[connections[x['connection']]['market']]
                key=f"{m['slug']}:minute_{int((x['received']-m['start'])//60)}"
                samples[key]['frames']+=1
                samples[key]['late']+=x['age']>cw['raw_age_max_seconds']
                samples[key]['types'][x['kind']]+=1
        cohorts.append({'cohort':label,'frames':len(xs),'late':sum(x['age']>5 for x in xs),
                        'types':dict(Counter(x['kind'] for x in xs)),
                        'per_market_minute':dict(samples),
                        'median_age':median(x['age'] for x in xs) if xs else None})
    gate=policy['longer_recording_GO']
    blockers=[]
    if quality['clean_fraction']<gate['minimum_clean_fraction']:blockers.append('CLEAN_FRACTION_BELOW_FROZEN_MINIMUM')
    if clean_seconds<gate['minimum_clean_seconds']:blockers.append('CLEAN_SECONDS_BELOW_FROZEN_MINIMUM')
    clean_markets={connections[x['connection']]['market'] for x in frames if is_clean(x['received']) and x['connection'] is not None}
    if len(clean_markets)<gate['minimum_markets_with_clean_data']:blockers.append('CLEAN_MARKET_COUNT_BELOW_FROZEN_MINIMUM')
    if len(late)/max(1,len(frames))>gate['maximum_raw_late_fraction']:blockers.append('FULL_SESSION_LATE_FRACTION_TOO_HIGH')
    if len(gaps)>gate['maximum_unscheduled_closures']:blockers.append('UNSCHEDULED_CLOSURES')
    result={'scope':'EXPLORATORY_STEP_1_ONLY','freeze_commit':freeze_commit,
            'policy_sha256':hashlib.sha256(policy_body).hexdigest(),'input_sha256':digest,
            'threshold_sets_tried':1,'start':start,'end':end,'connections':connections,'gaps':gaps,
            'late_total':len(late),'late_by_kind':dict(late_per_kind),'late_fraction_full':len(late)/max(1,len(frames)),
            'late_outside_event_guards':len(outside_guard),'late_histograms':{k:dict(v) for k,v in hist.items()},
            'nearest_event_histograms':{k:dict(v) for k,v in event_hist.items()},
            'late_temporal_histograms_by_kind':{k:dict(v) for k,v in hist_by_kind.items()},
            'late_near_initial_snapshot':dict(snapshot_near),'late_per_market_minute':{k:dict(v) for k,v in per_market.items()},
            'late_batches_same_socket_receive':{'n':len(batches),'largest':max(batches.values(),default=0),'multi_message_batches':sum(n>1 for n in batches.values())},
            'late_arrival_clusters':clusters(late,policy['batch']['max_arrival_gap_seconds']),
            'quality':quality,'cohorts':cohorts,'missing_timestamps':missing_timestamps,
            'longer_recording_decision':'NO-GO' if blockers else 'PENDING_NOT_GO',
            'numeric_gate_failures':blockers,'opportunity_and_cost_gate':'NOT_EVALUATED_AT_STEP_1',
            'clock_samples':clocks,'repairs_applied':False,'old_holdout_opened':False}
    if input_hash(source)!=digest:
        raise RuntimeError('Input changed')
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('source');p.add_argument('--policy',required=True);p.add_argument('--freeze-commit',required=True);p.add_argument('--out',required=True)
    a=p.parse_args();r=analyze(a.source,a.policy,a.freeze_commit)
    with Path(a.out).open('x') as f:json.dump(r,f,indent=2,allow_nan=False)
    print(json.dumps({k:r[k] for k in ('late_total','late_outside_event_guards','quality','cohorts','numeric_gate_failures')},indent=2))


if __name__=='__main__':main()
