"""Once-only end-of-cohort accuracy evaluation; unknown costs forbid financial GO."""
import argparse
import datetime as dt
import hashlib
import json
import math
import random
import statistics
import time
from pathlib import Path
from .daily_model import load_policy,POLICY_SHA256,FREEZE_COMMIT
from .daily_recorder import verify_journal,day_times,decision_row,market_tokens,canonical
from .daily_results import final_label


def block_lower(series,policy):
    rng=random.Random(policy['bootstrap_seed']);n=len(series);samples=[]
    for _ in range(policy['bootstrap_repetitions']):
        drawn=[]
        while len(drawn)<n:
            start=rng.randrange(n)
            drawn.extend(series[(start+j)%n]for j in range(7))
        values=[v for v in drawn[:n]if v is not None]
        if values:samples.append(statistics.mean(values))
    if len(samples)!=policy['bootstrap_repetitions']:return None
    samples.sort();return samples[int((len(samples)-1)*policy['lower_confidence_quantile'])]


def audit_forecast(rows,market,row,day):
    clock=next(r['evidence']for r in rows if r.get('type')=='clock')
    metadata=next(r for r in rows if r.get('role')=='metadata')
    candles=next(r for r in rows if r.get('role')=='candles')
    fee=next(r for r in rows if r.get('role')=='fee'and r['url'].endswith('/'+market['conditionId']))
    tokens=market_tokens(market)
    pair=[next(r for r in rows if r.get('role')=='book'and r['params']['token_id']==tokens[o])for o in ('Yes','No')]
    computed=decision_row(market,day,candles['data'],dict(zip(('Yes','No'),[r['data']for r in pair])),fee['data'],row['created_at'],clock,[metadata,candles,fee,*pair])
    if computed.get('p_yes')!=row.get('p_yes')or computed.get('benchmark_yes_midpoint')!=row.get('benchmark_yes_midpoint')or computed.get('clean_point')!=row.get('clean_point'):
        raise ValueError('Forecast not reproducible from sealed causal inputs')


def evaluate(root,asof=None,synthetic=False):
    policy=load_policy();root=Path(root)
    if root.name!='daily-prospective':raise ValueError('Only dedicated daily-prospective cohort root allowed')
    first=dt.date.fromisoformat(policy['first_settlement_day']);days=[(first+dt.timedelta(days=i)).isoformat()for i in range(policy['consecutive_settlement_days'])]
    asof=time.time()if asof is None else asof
    if asof<day_times(days[-1])[1]:raise ValueError('No interim performance evaluation; wait for entire frozen cohort')
    summaries=[];full_series=[];clean_series=[];total_points=0;excluded=0;synthetic_seen=False
    for day in days:
        folder=root/day;journal=folder/'journal.jsonl'
        summary={'day':day,'status':'MISSING_DAY','points':0,'scored_points':0,'clean_scored_points':0,
                 'brier_improvement':None,'clean_brier_improvement':None,'net_payoff':None}
        values=[];clean=[];model_scores=[];benchmark_scores=[];market_results=[]
        if journal.exists():
            rows=verify_journal(journal);header=rows[0];_,target,_=day_times(day)
            if header.get('day')!=day or header.get('freeze_commit')!=FREEZE_COMMIT or header.get('nominal_target')!=target:
                raise ValueError('Cohort identity or freeze mismatch')
            synthetic_seen|=header.get('mode')!='REAL_PUBLIC_RECORD_ONLY'
            roster=next((x['markets']for x in rows if x.get('type')=='universe'),[])
            forecasts={}
            for row in rows:
                if row.get('type')=='forecast':
                    cid=row['condition_id']
                    if cid in forecasts:raise ValueError('Duplicate forecast')
                    if row.get('financial_action')!='ABSTAIN' or row.get('net_payoff')is not None or row.get('fills',0)!=0:
                        raise ValueError('Financial result contradicts frozen unknown-cost policy')
                    forecasts[cid]=row
            digest=hashlib.sha256(journal.read_bytes()).hexdigest();labels={}
            for file in sorted(folder.glob('resolution-*.json')):
                evidence=json.loads(file.read_text())
                if evidence.get('journal_sha256')!=digest or evidence.get('policy_sha256')!=POLICY_SHA256 or evidence.get('day')!=day:
                    raise ValueError('Resolution evidence bound to different journal')
                for request in evidence.get('evidence',[]):
                    if request.get('url')!='https://data-api.polymarket.com/v2/resolutions' or request.get('status')!=200 or 'error'in request:continue
                    for state in request.get('payload',{}).get('data',[]):
                        for market in roster:
                            if state.get('condition_id')==market['conditionId']:
                                label=final_label(state,market,target,request['received_at'])
                                if label is not None:
                                    cid=market['conditionId']
                                    if cid in labels and labels[cid]!=label:raise ValueError('Conflicting official final labels')
                                    labels[cid]=label
            seen=set()
            for market in roster:
                cid=market['conditionId']
                if cid in seen:raise ValueError('Duplicate frozen roster')
                seen.add(cid);total_points+=1;summary['points']+=1
                row=forecasts.get(cid,{});p=row.get('p_yes');mid=row.get('benchmark_yes_midpoint');label=labels.get(cid)
                usable=(label is not None and p is not None and mid is not None and
                        all(math.isfinite(float(v))and 0<=float(v)<=1 for v in (p,mid)))
                if usable:
                    if header.get('mode')=='REAL_PUBLIC_RECORD_ONLY':audit_forecast(rows,market,row,day)
                    if not abs(row['created_at']-day_times(day)[0])<=policy['decision_tolerance_seconds']:
                        raise ValueError('Forecast outside frozen decision window')
                    model_score=(p-label)**2;benchmark_score=(mid-label)**2
                    model_scores.append(model_score);benchmark_scores.append(benchmark_score)
                    value=benchmark_score-model_score;values.append(value)
                    if row.get('clean_point')is True:clean.append(value)
                    else:excluded+=1
                else:excluded+=1
                market_results.append({'condition_id':cid,'p_yes':p,'benchmark_yes_midpoint':mid,'yes_label':label,'scored':usable,'clean_point':row.get('clean_point')is True})
            summary.update(status='OBSERVED',scored_points=len(values),clean_scored_points=len(clean),
                           brier_improvement=statistics.mean(values)if values else None,
                           clean_brier_improvement=statistics.mean(clean)if clean else None,
                           model_brier=statistics.mean(model_scores)if model_scores else None,
                           benchmark_brier=statistics.mean(benchmark_scores)if benchmark_scores else None,market_results=market_results)
        summaries.append(summary);full_series.append(summary['brier_improvement']);clean_series.append(summary['clean_brier_improvement'])
    # Missing days remain in their chronological positions for block resampling.
    scored_days=sum(v is not None for v in full_series)
    clean_days=sum(s['points']>0 and s['clean_scored_points']==s['points']for s in summaries)
    return {'policy_sha256':POLICY_SHA256,'freeze_commit':FREEZE_COMMIT,'mode':'SYNTHETIC_ONLY'if synthetic or synthetic_seen else 'REAL_DESCRIPTIVE_UNAUDITED_ARCHIVE',
            'cohort_days':len(days),'scored_days':scored_days,'clean_days':clean_days,
            'clean_day_fraction':clean_days/len(days),'full':{'mean_day_brier_improvement':statistics.mean([v for v in full_series if v is not None])if scored_days else None,'lower_95_bound':block_lower(full_series,policy)if scored_days else None},
            'clean':{'mean_day_brier_improvement':statistics.mean([v for v in clean_series if v is not None])if any(v is not None for v in clean_series)else None,'lower_95_bound':block_lower(clean_series,policy)if any(v is not None for v in clean_series)else None},
            'excluded_point_fraction':excluded/total_points if total_points else None,
            'missing_day_fraction':sum(s['status']=='MISSING_DAY'for s in summaries)/len(days),
            'excluded_time_fraction':None,'days':summaries,'selected_resolved_days':0,
            'net_payoff':None,'realized_pnl':None,'fills':0,'decision':'NO_GO',
            'blockers':['Other costs unknown; no financial selections or payoff','External archive timestamps require independent audit; local hashes are not proof'],
            'accuracy_gate_passes':scored_days>=policy['minimum_resolved_scored_days']and clean_days/len(days)>=policy['minimum_clean_day_fraction']and (block_lower(clean_series,policy)or 0)>0}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();output=Path(args.output)
    if output.exists():raise ValueError('Final evaluation already exists; no repeated final calculation')
    # Refuse interim calculations before creating the once-only cohort lock.
    policy=load_policy();last=(dt.date.fromisoformat(policy['first_settlement_day'])+dt.timedelta(days=policy['consecutive_settlement_days']-1)).isoformat()
    if time.time()<day_times(last)[1]:raise ValueError('No interim performance evaluation')
    root=Path(args.root)
    if root.name!='daily-prospective':raise ValueError('Dedicated cohort root required')
    (root/'final-evaluation.lock').mkdir(exist_ok=False)
    result=evaluate(args.root)
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('x')as f:json.dump(result,f,indent=2,allow_nan=False);f.write('\n')
