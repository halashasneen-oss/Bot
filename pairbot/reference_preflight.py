"""One frozen one-hour technical gate; record only, zero orders and paper fills."""
import asyncio
from collections import Counter
import json
import time
from pathlib import Path
from .unified_paper import Feed,load_policy
from .directional import Journal,read_journal
from .journal_lifecycle import stop_producers
from .ntp_probe import diagnostic
from .reference_transport import load_transport_policy,wait_for_reference,POLICY_SHA,FREEZE_COMMIT


def gate_result(live_samples,total_samples,ready_windows,policy):
    ratio=live_samples/total_samples if total_samples else 0
    checks={'complete_sampling':total_samples==policy['preflight_duration_seconds'],
            'both_btc_topics_live':ratio>=policy['minimum_both_topics_live_sample_fraction'],
            'model_ready_windows':ready_windows>=policy['minimum_model_ready_windows']}
    return {'checks':checks,'passed':all(checks.values()),'live_sample_fraction':ratio,
            'financial_go':False,'old_protocol_go':False}


async def run():
    p=load_transport_policy();strategy=load_policy()
    root=Path('runs/reference-preflight');root.mkdir(parents=True,exist_ok=False)
    started=time.time();mono=time.monotonic();first=(int(started//300)+1)*300
    windows=list(range(first,int(started+p['preflight_duration_seconds']-300)+1,300))
    journal=Journal(root/'journal.jsonl',{'mode':'TRANSPORT_PREFLIGHT_RECORD_ONLY','started_at':started,
        'transport_policy_sha256':POLICY_SHA,'transport_freeze_commit':FREEZE_COMMIT,
        'strategy_policy_sha256':'5efae6c98d14469597bec251035e3d4da6cc3378870130291fedf2c3ebf1228d',
        'predeclared_windows':windows,'actual_orders':0,'paper_fills':0})
    feed=Feed(journal);clock=None;samples=[];decisions=[];attempted=set();reasons=Counter()
    async def clocks():
        nonlocal clock
        while True:
            clock=await asyncio.to_thread(diagnostic,strategy)
            journal.write({'type':'clock','data':clock});await asyncio.sleep(60)
    def now():
        if not clock or not clock['all_samples_within_frozen_bound'] or not clock['intervals_consistent']:raise ValueError('CLOCK_UNVERIFIED')
        sample=clock['samples'][-1]
        if not 0<=time.time()-sample['observed_at']<=90:raise ValueError('CLOCK_STALE')
        return time.time()+sample['offset_seconds'],clock['worst_uncertainty_seconds']
    def live():
        wall=time.time()
        for kind in ('spot','twap'):
            rows=[v for v in feed.series[kind].values() if v['generation']==feed.generation]
            if not rows:return False
            v=max(rows,key=lambda x:x['timestamp'])
            if not 0<=wall-v['received_at']<=p['btc_progress_timeout_seconds'] or not 0<=wall-v['timestamp']<=p['btc_progress_timeout_seconds']:return False
        return True
    tasks=[asyncio.create_task(feed.run()),asyncio.create_task(clocks())]
    def checkpoint(complete=False,error=None):
        ready=sum(d['ready'] for d in decisions)
        result={'started_at':started,'updated_at':time.time(),'elapsed_monotonic_seconds':time.monotonic()-mono,
            'complete':complete,'transport_policy_sha256':POLICY_SHA,'freeze_commit':FREEZE_COMMIT,
            'duration_seconds':p['preflight_duration_seconds'],'predeclared_windows':windows,
            'total_samples':len(samples),'live_samples':sum(samples),'model_ready_windows':ready,
            'decisions':decisions,'reason_counts':dict(reasons),'actual_orders':0,'paper_fills':0,'error':error,
            **gate_result(sum(samples),len(samples),ready,p)}
        result['passed']=result['passed'] and complete and error is None
        tmp=root/'summary.tmp';tmp.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n');tmp.replace(root/'summary.json');return result
    try:
        print(json.dumps({'event':'one_hour_transport_gate_started','policy_sha256':POLICY_SHA,'started_at':started}),flush=True)
        while time.monotonic()-mono<p['preflight_duration_seconds']:
            elapsed=int(time.monotonic()-mono)
            if elapsed>=len(samples):
                while len(samples)<elapsed:
                    journal.write({'type':'transport_sample','second':len(samples),'both_topics_live':False,'reason':'MISSED_SAMPLE'});samples.append(False)
                value=live();journal.write({'type':'transport_sample','second':elapsed,'both_topics_live':value,'generation':feed.generation,'at':time.time()});samples.append(value)
            for start in windows:
                if time.time()>=start+strategy['decision_age_seconds'] and start not in attempted:
                    attempted.add(start);d={'start':start,'ready':False,'reason':None}
                    try:
                        features,at=await wait_for_reference(feed,start,now,strategy)
                        d.update(ready=True,features=features,features_at=at)
                    except ValueError as exc:d['reason']=str(exc);reasons[str(exc)]+=1
                    decisions.append(d);journal.write({'type':'preflight_decision',**d});checkpoint()
                    print(json.dumps({'event':'preflight_decision','start':start,'ready':d['ready'],'reason':d['reason']}),flush=True)
            await asyncio.sleep(.05)
        while len(samples)<p['preflight_duration_seconds']:
            journal.write({'type':'transport_sample','second':len(samples),'both_topics_live':False,'reason':'MISSED_SAMPLE'});samples.append(False)
        for start in windows:
            if start not in attempted:
                d={'start':start,'ready':False,'reason':'MISSING_FIXED_DECISION'};decisions.append(d);journal.write({'type':'preflight_decision',**d})
        await stop_producers(tasks)
        result=checkpoint(True);journal.write({'type':'complete','finished_at':time.time(),'gate_passed':result['passed'],'actual_orders':0,'paper_fills':0})
        print(json.dumps({k:v for k,v in result.items() if k not in ('decisions','predeclared_windows')}),flush=True)
    except BaseException as exc:
        checkpoint(False,str(exc));journal.write({'type':'preflight_failed','error':str(exc),'at':time.time()});raise
    finally:
        try:await stop_producers(tasks)
        finally:journal.close()
    list(read_journal(root/'journal.jsonl'))
    return result

if __name__=='__main__':asyncio.run(run())
