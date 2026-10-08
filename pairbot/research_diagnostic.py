"""Read-only transport diagnostics; never evaluate strategies or final holdouts."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
from .feed import read_entries
from .research import Latencies, input_hash


def diagnose(source):
    digest=input_hash(source)
    counts=Counter()
    controls=Counter()
    latency=Latencies()
    gaps=[]
    errors=[]
    references=[]
    ping_times=[]
    current=None
    previous_ping={}
    ping_intervals=[]
    late=Counter()
    missing_exchange=Counter()
    clocks=[]
    lifecycle=[]
    transport_latency=Latencies()
    buffer_latency=Latencies()
    for row in read_entries(source):
        kind=row['type']
        counts[kind]+=1
        if kind=='market':
            current=row['market']['condition_id']
        elif kind=='frame':
            msg=row['message']
            event=msg.get('event_type','unknown')
            raw=msg.get('timestamp')
            if raw is None:
                missing_exchange[event]+=1
                continue
            ex=float(raw)
            if ex>1e12:
                ex/=1000
            received=row.get('received_at',row['ts'])
            latency.add(event,received,ex)
            if row.get('socket_received_at') is not None:
                transport_latency.add(event,row['socket_received_at'],ex)
            if row.get('application_received_monotonic_ns') is not None and row.get('socket_timestamp_scope')=='COMPLETING_ASYNCIO_TRANSPORT_CHUNK':
                buffer_latency.add(event,row['application_received_monotonic_ns']/1e9,row['socket_monotonic_ns']/1e9)
            if received-ex>5:
                late[event]+=1
        elif kind=='socket_control':
            direction=row.get('direction','received')
            controls[direction+':'+row['value']]+=1
            if direction=='sent' and row['value']=='PING':
                ts=row['socket_monotonic_ns']/1e9
                if current in previous_ping:
                    ping_intervals.append(ts-previous_ping[current])
                previous_ping[current]=ts
                ping_times.append(row['ts'])
        elif kind=='gap':
            gaps.append(row)
            previous_ping.pop(current,None)
        elif kind=='source_error':
            errors.append(row)
        elif kind=='metadata':
            references.append({'condition_id':row['condition_id'],'observed_at':row['ts'],
                               'reference':row.get('reference')})
        elif kind=='clock':
            clocks.append(row)
        elif kind=='socket_lifecycle':
            lifecycle.append(row)
    if input_hash(source)!=digest:
        raise RuntimeError('Input changed during diagnostics')
    return {'mode':'PAPER_ONLY_TRANSPORT_DIAGNOSTIC','input_sha256':digest,
            'strategy_evaluation':False,'profitability_claim':False,
            'counts':dict(counts),'controls':dict(controls),'gaps':gaps,'source_errors':errors,
            'latency':latency.report(),'raw_receipt_minus_exchange_over_5s':dict(late),
            'missing_exchange_timestamp':dict(missing_exchange),
            'heartbeat_interval_seconds':{'n':len(ping_intervals),
                'min':min(ping_intervals,default=None),'max':max(ping_intervals,default=None)},
            'reference_observations':references,'clock_observations':clocks,
            'socket_lifecycle':lifecycle,'transport_receipt_minus_exchange':transport_latency.report(),
            'websocket_buffer_wait_seconds':buffer_latency.report(),
            'interpretation':'Heartbeat compliance alone does not prove transport quality; no clock correction or stale-veto change.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source')
    parser.add_argument('--out',required=True)
    args=parser.parse_args()
    result=diagnose(args.source)
    out=Path(args.out)
    with out.open('x') as f:
        json.dump(result,f,indent=2,allow_nan=False)
    print(json.dumps({k:result[k] for k in ('counts','controls','raw_receipt_minus_exchange_over_5s')},indent=2))


if __name__=='__main__':
    main()
