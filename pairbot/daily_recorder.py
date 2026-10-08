"""Causal, bounded daily public forecast journal. No financial selection or orders."""
import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import math
import re
import time
from pathlib import Path
from zoneinfo import ZoneInfo
import httpx
from .daily_capture import book_status
from .daily_model import load_policy, probability, POLICY_SHA256, FREEZE_COMMIT
from .daily_readiness import fee_match
from .ntp_probe import diagnostic

# Exact rules observed before implementation. Unknown variants abstain; no guess.
RULES = ('This market will resolve to "Yes" if the Binance 1 minute candle for BTC/USDT '
         '12:00 in the ET timezone (noon) on the date specified in the title has a final '
         '"Close" price higher than the price specified in the title. Otherwise, this market '
         'will resolve to "No".\n\nThe resolution source for this market is Binance, specifically '
         'the BTC/USDT "Close" prices currently available at https://www.binance.com/en/trade/BTC_USDT '
         'with "1m" and "Candles" selected on the top bar.\n\nPlease note that this market is about '
         'the price according to Binance BTC/USDT, not according to other exchanges or trading pairs.\n\n'
         'Price precision is determined by the number of decimal places in the source.')


def canonical(row):
    return json.dumps(row, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


class Journal:
    def __init__(self, path):
        self.file=Path(path).open('x')
        self.previous='0'*64
        self.sequence=0

    def append(self, payload):
        row={'sequence':self.sequence,'previous_sha256':self.previous,'payload':payload}
        row['sha256']=hashlib.sha256(canonical(row)).hexdigest()
        self.file.write(json.dumps(row,allow_nan=False)+'\n')
        self.file.flush();os.fsync(self.file.fileno())
        self.sequence+=1;self.previous=row['sha256']
        return row['sha256']

    def close(self):self.file.close()


def verify_journal(path):
    rows=[];previous='0'*64
    for index,line in enumerate(Path(path).read_text().splitlines()):
        row=json.loads(line);digest=row.pop('sha256')
        if row['sequence']!=index or row['previous_sha256']!=previous or hashlib.sha256(canonical(row)).hexdigest()!=digest:
            raise ValueError('Journal chain mismatch')
        previous=digest;rows.append(row['payload'])
    if not rows or rows[0].get('policy_sha256')!=POLICY_SHA256:
        raise ValueError('Not a frozen daily journal')
    if rows[-1].get('type')!='sealed' or sum(r.get('type')=='sealed' for r in rows)!=1:
        raise ValueError('Partial or ambiguously sealed journal')
    return rows


def day_times(day):
    policy=load_policy();date=dt.date.fromisoformat(day)
    first=dt.date.fromisoformat(policy['first_settlement_day'])
    if not first<=date<first+dt.timedelta(days=policy['consecutive_settlement_days']):
        raise ValueError('Day outside frozen prospective cohort')
    zone=ZoneInfo(policy['decision_timezone'])
    decision=dt.datetime.combine(date,dt.time.fromisoformat(policy['decision_local_time']),zone).timestamp()
    target=dt.datetime.combine(date,dt.time.fromisoformat(policy['nominal_target_local_time']),zone).timestamp()
    slug=f'bitcoin-above-on-{date.strftime("%B").lower()}-{date.day}-{date.year}'
    return decision,target,slug


def market_tokens(market):
    outcomes=market['outcomes'];ids=market['clobTokenIds']
    outcomes=json.loads(outcomes) if isinstance(outcomes,str) else outcomes
    ids=json.loads(ids) if isinstance(ids,str) else ids
    if len(outcomes)!=2 or set(outcomes)!={'Yes','No'} or len(ids)!=2 or len(set(ids))!=2:
        raise ValueError('Invalid binary mapping')
    if not all(str(t).isdigit() for t in ids):raise ValueError('Invalid token')
    return dict(zip(outcomes,map(str,ids)))


def audit_market(market, day):
    _,target,event_slug=day_times(day)
    slug=market['slug'];match=re.fullmatch(r'bitcoin-above-(\d+)k-on-'+re.escape(event_slug.removeprefix('bitcoin-above-on-')),slug)
    if not match or market.get('description','').strip()!=RULES:
        raise ValueError('Unknown contract or rules variant')
    threshold=int(match[1])*1000
    question=market.get('question','').replace(',','')
    if not re.search(r'(?<!\d)'+str(threshold)+r'(?!\d)',question):
        raise ValueError('Question/slug threshold mismatch')
    if market.get('closed') is not False or market.get('umaResolutionStatus') not in (None,'') or not market.get('enableOrderBook'):
        raise ValueError('Market closed/resolution-known or unavailable')
    end=dt.datetime.fromisoformat(market['endDate'].replace('Z','+00:00')).timestamp()
    if end!=target:raise ValueError('Market nominal end differs from frozen target')
    cid=market['conditionId']
    if not re.fullmatch(r'0x[0-9a-fA-F]{64}',cid):raise ValueError('Invalid condition id')
    return threshold,market_tokens(market)


def clock_evidence(clock, now):
    policy=load_policy();samples=clock.get('samples',[])
    if clock.get('source')!=policy['clock_source'] or len(samples)!=2 or not all(s.get('ok') for s in samples) or not clock.get('intervals_consistent'):
        raise ValueError('Clock evidence unavailable/inconsistent')
    for s in samples:
        if not all(math.isfinite(float(s[k])) for k in ('observed_at','uncertainty_seconds','offset_seconds','wall_minus_monotonic_elapsed_seconds')):
            raise ValueError('Nonfinite clock evidence')
        if not 0<=now-s['observed_at']<=policy['maximum_clock_sample_age_seconds'] or not 0<=s['uncertainty_seconds']<=policy['maximum_clock_uncertainty_seconds']:
            raise ValueError('Clock evidence stale/outside bound')
        if abs(s['wall_minus_monotonic_elapsed_seconds'])>policy['maximum_local_clock_step_seconds']:
            raise ValueError('Local clock stepped')
    # Both retained; use latest offset, never select minimum uncertainty.
    return samples[-1]['offset_seconds'],max(s['uncertainty_seconds'] for s in samples)


def decision_row(market, day, candles, books, fee, now, clock, requests):
    policy=load_policy();decision,target,_=day_times(day)
    result={'type':'forecast','day':day,'condition_id':market.get('conditionId'),
            'market_slug':market.get('slug'),'p_yes':None,'benchmark_yes_midpoint':None,
            'financial_action':'ABSTAIN','reason':'OTHER_COSTS_UNKNOWN','net_payoff':None,
            'fills':0,'realized_pnl':None,'created_at':now}
    try:
        offset,width=clock_evidence(clock,now)
        corrected=now+offset
        if abs(corrected-decision)+width>policy['decision_tolerance_seconds']:
            raise ValueError('Outside decision window')
        threshold,tokens=audit_market(market,day)
        if not fee_match(market,fee,list(tokens.values())):raise ValueError('Fee/token mismatch')
        for row in requests:
            if not all(math.isfinite(float(row[k])) for k in ('received_at','rtt_seconds','wall_elapsed_seconds')) or row['rtt_seconds']<0:
                raise ValueError('Invalid request timing')
            if 'error'in row or row['received_at']>now or row['rtt_seconds']>policy['maximum_http_rtt_seconds'] or abs(row['wall_elapsed_seconds']-row['rtt_seconds'])>policy['maximum_local_clock_step_seconds']:
                raise ValueError('Request failed, future, slow or clock-stepped')
            # No carry-forward of earlier books: all receipts belong to this window.
            if abs(row['received_at']+offset-decision)+width>policy['decision_tolerance_seconds']:
                raise ValueError('Input outside decision window')
        if len(requests)<5:raise ValueError('Missing request provenance')
        pair=[r for r in requests if r.get('role')=='book']
        if len(pair)!=2 or max(r['received_at']for r in pair)-min(r['received_at']for r in pair)>policy['maximum_pair_receipt_span_seconds']:
            raise ValueError('Pair receipt skew')
        yes,no=books['Yes'],books['No']
        if book_status(yes,tokens['Yes'])!='two_sided' or book_status(no,tokens['No'])!='two_sided':
            raise ValueError('Book not two sided/non crossed')
        model=probability(candles,threshold,corrected-width,target)
        result.update(model)
        result['benchmark_yes_midpoint']=(max(float(x['price'])for x in yes['bids'])+min(float(x['price'])for x in yes['asks']))/2
        result['clean_point']=model['p_yes'] is not None
    except (ValueError,KeyError,TypeError,IndexError) as exc:
        result.update(reason=str(exc),clean_point=False)
    return result


class PublicReader:
    def __init__(self,journal):
        self.client=httpx.AsyncClient(timeout=2,trust_env=False,follow_redirects=False)
        self.journal=journal

    async def get(self,url,params=None,role=None):
        row={'type':'input','url':url,'params':params,'role':role,'sent_at':time.time()}
        mono=time.monotonic()
        try:
            response=await self.client.get(url,params=params)
            received=time.time();elapsed=time.monotonic()-mono
            row.update(received_at=received,rtt_seconds=elapsed,wall_elapsed_seconds=received-row['sent_at'],
                       status=response.status_code,body_sha256=hashlib.sha256(response.content).hexdigest())
            response.raise_for_status();row['data']=response.json()
        except Exception as exc:
            row.setdefault('received_at',time.time());row['error']=str(exc)
        self.journal.append(row)
        return row

    async def close(self):await self.client.aclose()


async def record(day,output):
    decision,target,slug=day_times(day);policy=load_policy()
    output=Path(output)/day;output.mkdir(parents=True,exist_ok=False)
    journal=Journal(output/'journal.jsonl')
    journal.append({'type':'header','mode':'REAL_PUBLIC_RECORD_ONLY','policy_sha256':POLICY_SHA256,
                    'freeze_commit':FREEZE_COMMIT,'day':day,'nominal_target':target,
                    'created_at':time.time(),'receipt_semantics':'HTTP body completion before JSON, not socket receipt'})
    api=PublicReader(journal);count=0
    try:
        # Missed days remain explicit; no network backfill and no reconstructed predictions.
        if abs(time.time()-decision)>policy['decision_tolerance_seconds']:
            journal.append({'type':'missing_day','reason':'OUTSIDE_DECISION_WINDOW','p_yes':None})
        else:
            clock=await asyncio.to_thread(diagnostic,policy);journal.append({'type':'clock','evidence':clock})
            metadata=await api.get('https://gamma-api.polymarket.com/events',{'slug':slug},'metadata')
            events=metadata.get('data',[])
            event=next((e for e in events if e.get('slug')==slug),None)
            roster=event.get('markets',[]) if event else []
            condition_ids=[m.get('conditionId') for m in roster]
            if not event or not 1<=len(roster)<=32 or len(set(condition_ids))!=len(condition_ids):
                journal.append({'type':'missing_day','reason':'UNIVERSE_UNAVAILABLE','p_yes':None})
            else:
                markets=event['markets']
                journal.append({'type':'universe','markets':markets,'received_at':metadata['received_at']})
                end=int(time.time()//3600)*3600
                iso=lambda t:dt.datetime.fromtimestamp(t,dt.timezone.utc).isoformat()
                candles=await api.get(policy['feature_source'],{'granularity':3600,'start':iso(end-26*3600),'end':iso(end)},'candles')
                for market in markets:
                    # Unknown rules/invalid ids never reach dynamic URLs.
                    try:
                        if abs(time.time()-decision)>policy['decision_tolerance_seconds']:
                            raise ValueError('Outside decision window; no late backfill')
                        _,tokens=audit_market(market,day)
                    except (ValueError,KeyError,TypeError) as exc:
                        row={'type':'forecast','day':day,'condition_id':market.get('conditionId'),
                             'market_slug':market.get('slug'),'p_yes':None,'clean_point':False,
                             'financial_action':'ABSTAIN','reason':str(exc),'net_payoff':None,'fills':0}
                    else:
                        fee=await api.get('https://clob.polymarket.com/clob-markets/'+market['conditionId'],role='fee')
                        pair=await asyncio.gather(*(api.get('https://clob.polymarket.com/book',{'token_id':tokens[o]},'book')for o in ('Yes','No')))
                        row=decision_row(market,day,candles.get('data'),dict(zip(('Yes','No'),[r.get('data')for r in pair])),
                                         fee.get('data'),time.time(),clock,[metadata,candles,fee,*pair])
                    journal.append(row);count+=1
        journal.append({'type':'sealed','forecast_count':count,'sealed_at':time.time(),
                        'fills':0,'realized_pnl':None,'decision':'NO_GO_FINANCIAL_UNKNOWN_COSTS'})
    finally:
        await api.close();journal.close()
    verify_journal(output/'journal.jsonl')
    print(json.dumps({'day':day,'forecasts_written':count,'fills':0,'decision':'NO_GO_FINANCIAL_UNKNOWN_COSTS'}))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();asyncio.run(record(args.day,args.output))
