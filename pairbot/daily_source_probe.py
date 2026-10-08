"""Public Coinbase feature-source availability; never a Binance settlement source."""
import asyncio
import datetime
import json
import math
import time
from pathlib import Path
import httpx


def closed_hourly(rows, asof):
    end = int(asof // 3600) * 3600
    expected = set(range(end-25*3600, end, 3600))
    selected = {}
    for row in rows:
        if len(row) != 6:
            raise ValueError('Unexpected candle schema')
        ts, low, high, opened, close, volume = map(float, row)
        if not all(math.isfinite(x) for x in (ts,low,high,opened,close,volume)):
            raise ValueError('Nonfinite candle')
        if ts != int(ts) or int(ts) % 3600 or not 0 < low <= min(opened,close) <= max(opened,close) <= high or volume < 0:
            raise ValueError('Invalid candle')
        if ts in expected:
            if ts in selected:
                raise ValueError('Duplicate candle')
            selected[int(ts)] = close
    if set(selected) != expected:
        raise ValueError('Missing closed hourly candle; no interpolation')
    return [(ts,selected[ts]) for ts in sorted(selected)]


async def run():
    path=Path('runs/daily-source');path.mkdir(parents=True,exist_ok=False)
    rows=[]
    now=time.time();end=int(now//3600)*3600
    iso=lambda ts:datetime.datetime.fromtimestamp(ts,datetime.timezone.utc).isoformat()
    async with httpx.AsyncClient(timeout=5,trust_env=False,follow_redirects=False) as client:
        for suffix,params in [('candles',{'granularity':3600,'start':iso(end-26*3600),'end':iso(end)}),('ticker',None)]:
            row={'url':'https://api.exchange.coinbase.com/products/BTC-USD/'+suffix,'params':params,'sent_at':time.time()}
            try:
                response=await client.get(row['url'],params=params)
                row.update(received_at=time.time(),status=response.status_code)
                response.raise_for_status();row['payload']=response.json()
            except Exception as exc:row['error']=str(exc)
            rows.append(row)
    error=None
    try: count=len(closed_hourly(rows[0]['payload'],now))
    except Exception as exc:count=0;error=str(exc)
    result={'mode':'feature-source-only','source':'Coinbase Exchange BTC-USD','asof':now,'requests':rows,
            'closed_hourly_count':count,'intervals':max(0,count-1),'validation_error':error,
            'settlement_reference_admitted':False,'predictions':0,'fills':0,'realized_pnl':None}
    (path/'result.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps({k:result[k]for k in ('closed_hourly_count','validation_error')}))

if __name__=='__main__':asyncio.run(run())
