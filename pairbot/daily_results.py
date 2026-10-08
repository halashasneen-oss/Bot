"""Final official daily labels, saved separately from immutable forecasts."""
import argparse
import asyncio
import datetime as dt
import hashlib
import json
import math
import re
import time
from pathlib import Path
import httpx
from .daily_recorder import verify_journal, day_times, market_tokens
from .daily_model import POLICY_SHA256

URL='https://data-api.polymarket.com/v2/resolutions'


def final_label(state,market,target,received):
    try:
        if state['condition_id']!=market['conditionId'] or state['status'].lower()!='resolved':
            return None
        resolved=dt.datetime.fromisoformat(state['resolved_at'].replace('Z','+00:00'))
        if resolved.tzinfo is None or not target<=resolved.timestamp()<=received:
            return None
        if not isinstance(state['resolved_block'],int) or state['resolved_block']<=0:
            return None
        payouts=[float(p)for p in state['payouts']]
        if len(payouts)!=2 or not all(math.isfinite(p)and p>=0 for p in payouts) or sum(p>0 for p in payouts)!=1:
            return None
        outcomes=market['outcomes'];outcomes=json.loads(outcomes)if isinstance(outcomes,str)else outcomes
        market_tokens(market)
        winner=outcomes[next(i for i,p in enumerate(payouts)if p>0)]
        return 1 if winner=='Yes' else 0
    except (KeyError,ValueError,TypeError,AttributeError,StopIteration):return None


async def collect(journal_path,output):
    journal_path=Path(journal_path);rows=verify_journal(journal_path)
    header=rows[0];day=header['day'];_,target,_=day_times(day)
    if time.time()<=target:raise ValueError('Resolution lookup before target forbidden')
    roster=next((r['markets']for r in rows if r.get('type')=='universe'),[])
    ids=[m['conditionId']for m in roster]
    if len(ids)!=len(set(ids)) or len(ids)>32 or not all(re.fullmatch(r'0x[0-9a-fA-F]{64}',c)for c in ids):
        raise ValueError('Invalid frozen roster')
    evidence=[];states={}
    async with httpx.AsyncClient(timeout=5,trust_env=False,follow_redirects=False)as client:
        for index in range(0,len(ids),20):
            item={'url':URL,'params':{'condition':','.join(ids[index:index+20])},'sent_at':time.time()}
            try:
                response=await client.get(URL,params=item['params'])
                item.update(received_at=time.time(),status=response.status_code)
                response.raise_for_status();item['payload']=response.json()
                for state in item['payload']['data']:
                    cid=state['condition_id']
                    if cid not in ids or cid in states:raise ValueError('Unexpected/duplicate resolution row')
                    states[cid]=(state,item['received_at'])
            except Exception as exc:item['error']=str(exc)
            evidence.append(item)
    labels=[]
    if not any('error'in x for x in evidence):
        for market in roster:
            pair=states.get(market['conditionId'])
            value=final_label(pair[0],market,target,pair[1])if pair else None
            labels.append({'condition_id':market['conditionId'],'yes_label':value,
                           'status':'FINAL'if value is not None else 'PENDING_OR_UNSUPPORTED'})
    result={'type':'official_daily_resolution','day':day,'policy_sha256':POLICY_SHA256,
            'journal_sha256':hashlib.sha256(journal_path.read_bytes()).hexdigest(),
            'observed_at':time.time(),'evidence':evidence,'labels':labels,
            'mode':header.get('mode'),'predictions_modified':False}
    path=Path(output);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x')as f:json.dump(result,f,indent=2,allow_nan=False);f.write('\n')
    print(json.dumps({'day':day,'final_labels':sum(x['yes_label']is not None for x in labels),'errors':sum('error'in x for x in evidence)}))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();asyncio.run(collect(args.journal,args.output))
