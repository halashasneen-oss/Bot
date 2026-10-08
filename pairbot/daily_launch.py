"""Launch bounded prospective recording and collect past official labels."""
import asyncio
import datetime as dt
import json
import hashlib
import time
from pathlib import Path
from .daily_recorder import day_times,record,verify_journal
from .daily_model import load_policy
from .daily_archive import hydrate
from .daily_results import collect,final_label
from .daily_evaluate import evaluate


async def collect_history(root):
    for folder in sorted(root.iterdir()):
        if not folder.is_dir():continue
        journal=folder/'journal.jsonl'
        if not journal.exists():continue
        rows=verify_journal(journal)
        roster=next((r['markets']for r in rows if r.get('type')=='universe'),[])
        done=set()
        for file in folder.glob('resolution-*.json'):
            data=json.loads(file.read_text())
            if data.get('journal_sha256')!=hashlib.sha256(journal.read_bytes()).hexdigest():raise ValueError('Past label evidence mismatch')
            target=day_times(rows[0]['day'])[1]
            for request in data.get('evidence',[]):
                if request.get('url')!='https://data-api.polymarket.com/v2/resolutions'or request.get('status')!=200 or 'error'in request:continue
                for state in request.get('payload',{}).get('data',[]):
                    for market in roster:
                        if final_label(state,market,target,request['received_at'])is not None:done.add(market['conditionId'])
        if roster and not {m['conditionId']for m in roster}<=done:
            await collect(journal,folder/f'resolution-{int(time.time())}.json')


def main():
    config=json.loads(Path('config/daily-launch.json').read_text())
    if config.get('enabled')is not True:
        print('Launch armed for future config commit; no recording now');return
    policy=load_policy();day=config['day'];date=dt.date.fromisoformat(day)
    first=dt.date.fromisoformat(policy['first_settlement_day']);last=first+dt.timedelta(days=policy['consecutive_settlement_days'])
    if config.get('final_evaluation')is True:
        if date!=last:raise ValueError('Final collection only follows full frozen cohort')
    else:
        decision,_,_=day_times(day);delay=decision+2-time.time()
        if delay>600:raise ValueError('Launch too early; bounded wait is ten minutes')
        # Executed in future CI job, never an interactive assistant turn.
        if delay>0:time.sleep(delay)
        asyncio.run(record(day,'runs/daily-prospective'))
    root=hydrate('runs/daily-history');asyncio.run(collect_history(root))
    if config.get('final_evaluation')is True:
        result=evaluate(root)
        path=Path('runs/final-report.json')
        with path.open('x')as f:json.dump(result,f,indent=2,allow_nan=False);f.write('\n')

if __name__=='__main__':main()
