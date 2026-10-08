"""One bounded unauthenticated public RTDS availability probe; no auth bypass."""
import asyncio
import json
import time
from pathlib import Path
from collections import Counter
from websockets.asyncio.client import connect
import httpx

TOPICS = ('crypto_prices_chainlink', 'crypto_prices_twap_sixty')


async def probe():
    output = Path('runs/unified-feed-probe')
    output.mkdir(parents=True, exist_ok=False)
    result = {'mode': 'PUBLIC_FEED_AVAILABILITY_ONLY', 'started_at': time.time(),
        'url': 'wss://ws-live-data.polymarket.com', 'topics': list(TOPICS),
        'messages': [], 'errors': [], 'credentials': False, 'proxy': False,
        'source_matching_proven': False, 'fills': 0}
    async with httpx.AsyncClient(timeout=3, trust_env=False, follow_redirects=False) as api:
        slug = 'btc-updown-5m-'+str(int(time.time()//300)*300)
        try:
            resp = await api.get('https://gamma-api.polymarket.com/events', params={'slug': slug})
            result['metadata'] = {'slug': slug, 'status': resp.status_code, 'received_at': time.time(), 'data': resp.json()}
        except Exception as exc: result['errors'].append(str(exc))
    try:
        async with connect(result['url'], proxy=None, open_timeout=8, ping_interval=10) as ws:
            await ws.send(json.dumps({'action': 'subscribe', 'subscriptions': [
                {'topic': t, 'type': 'update'} for t in TOPICS]}))
            deadline = time.monotonic()+35
            last_ping = time.monotonic()
            while time.monotonic()<deadline:
                if time.monotonic()-last_ping>=5:
                    await ws.send('PING'); last_ping=time.monotonic()
                try: raw = await asyncio.wait_for(ws.recv(), min(2, max(.01, deadline-time.monotonic())))
                except asyncio.TimeoutError: continue
                received = time.time()
                try: message = json.loads(raw)
                except (ValueError, TypeError): continue
                result['messages'].append({'received_at': received, 'data': message})
                if isinstance(message,dict) and any(x in str(message).lower() for x in ('unauthorized','authentication required','invalid api key')):
                    result['errors'].append('AUTH_REQUIRED_STOP_NO_BYPASS'); break
    except Exception as exc: result['errors'].append(str(exc))
    counts = Counter()
    for row in result['messages']:
        m=row['data']
        if isinstance(m,dict) and isinstance(m.get('payload'),dict) and m['payload'].get('symbol')=='btc/usd':
            counts[m.get('topic')]+=1
    result.update(finished_at=time.time(), btc_message_counts=dict(counts))
    (output/'result.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('messages','metadata')}))


if __name__=='__main__': asyncio.run(probe())
