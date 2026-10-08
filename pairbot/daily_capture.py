"""Fixed bounded daily metadata/book GET capture. No prediction or execution."""
import asyncio
import hashlib
import json
import time
from pathlib import Path

import httpx

SLUG = 'bitcoin-above-on-october-5-2026'
ROUNDS = 12
INTERVAL = 10


def universe(events):
    event = next(e for e in events if e.get('slug') == SLUG)
    markets = event['markets']
    if not 1 <= len(markets) <= 32:
        raise ValueError('Unexpected universe size; never silently truncate')
    tokens = []
    for market in markets:
        outcomes = market['outcomes']
        ids = market['clobTokenIds']
        outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        ids = json.loads(ids) if isinstance(ids, str) else ids
        if len(ids) != 2 or set(outcomes) != {'Yes', 'No'} or len(set(ids)) != 2:
            raise ValueError('Invalid binary outcome mapping')
        if market.get('closed') or not market.get('enableOrderBook'):
            raise ValueError('Universe contains unavailable market')
        for outcome, token in zip(outcomes, ids):
            if not str(token).isdigit():
                raise ValueError('Invalid token id')
            tokens.append({'condition_id': market['conditionId'], 'slug': market['slug'],
                           'outcome': outcome, 'token_id': str(token)})
    if len({t['token_id'] for t in tokens}) != len(tokens):
        raise ValueError('Duplicate token')
    return tokens


def book_status(payload, token):
    try:
        if str(payload['asset_id']) != token:
            return 'token_mismatch'
        levels = {}
        for side in ('bids', 'asks'):
            levels[side] = [(float(x['price']), float(x['size'])) for x in payload[side]]
            if not all(0 < p < 1 and 0 < s < float('inf') for p, s in levels[side]):
                return 'invalid_levels'
        if not levels['bids'] or not levels['asks']:
            return 'one_sided'
        if max(p for p, _ in levels['bids']) >= min(p for p, _ in levels['asks']):
            return 'crossed_or_locked'
        return 'two_sided'
    except (KeyError, TypeError, ValueError):
        return 'invalid_schema'


async def capture(output):
    output.mkdir(parents=True, exist_ok=False)
    journal = output / 'journal.jsonl'
    counts = {}
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient(trust_env=False, timeout=5, follow_redirects=False) as client:
        with journal.open('x') as target:
            def write(row):
                target.write(json.dumps(row, allow_nan=False) + '\n')
                target.flush()

            async def get(url, params=None, context=None):
                async with sem:
                    sent = time.time()
                    sent_mono = time.monotonic()
                    row = {'url': url, 'params': params, 'context': context, 'sent_at': sent}
                    try:
                        response = await client.get(url, params=params)
                        # HTTP body arrival, before JSON processing; not raw socket receipt.
                        row.update(received_at=time.time(), rtt_seconds=time.monotonic()-sent_mono,
                                   status=response.status_code,
                                   body_sha256=hashlib.sha256(response.content).hexdigest())
                        response.raise_for_status()
                        row['payload'] = response.json()
                    except Exception as exc:
                        row['error'] = str(exc)
                        row.setdefault('received_at', time.time())
                    write(row)
                    return row

            write({'mode': 'bounded-daily-record-only', 'slug': SLUG, 'rounds': ROUNDS,
                   'interval_seconds': INTERVAL, 'universe': 'all event thresholds',
                   'receipt_semantics': 'HTTP body completion before JSON processing',
                   'clock_policy_admitted': False, 'fills': 0, 'predictions': 0})
            meta = await get('https://gamma-api.polymarket.com/events', {'slug': SLUG})
            tokens = []
            error = None
            try:
                tokens = universe(meta['payload'])
            except Exception as exc:
                error = str(exc)
            if tokens:
                write({'type': 'frozen_capture_universe', 'tokens': tokens})
                await asyncio.gather(*(get('https://clob.polymarket.com/fee-rate',
                    {'token_id': t['token_id']}, {'type': 'fee', **t}) for t in tokens))
                start = time.monotonic()
                for index in range(ROUNDS):
                    await asyncio.sleep(max(0, start + index*INTERVAL-time.monotonic()))
                    await get('https://clob.polymarket.com/time', context={'type': 'clock', 'round': index})
                    rows = await asyncio.gather(*(get('https://clob.polymarket.com/book',
                        {'token_id': t['token_id']}, {'type': 'book', 'round': index, **t}) for t in tokens))
                    for row in rows:
                        status = 'request_failed' if 'error' in row else book_status(row['payload'], row['context']['token_id'])
                        counts[status] = counts.get(status, 0) + 1
                await get('https://gamma-api.polymarket.com/events', {'slug': SLUG}, {'type': 'end_metadata'})
    result = {'mode': 'bounded-daily-record-only', 'slug': SLUG, 'markets': len(tokens)//2,
              'planned_book_requests': len(tokens)*ROUNDS, 'book_status_counts': counts,
              'universe_error': error, 'predictions': 0, 'fills': 0, 'realized_pnl': None,
              'decision': 'NO_GO_PENDING_DATA_REVIEW', 'other_costs_verified': False,
              'clock_policy_admitted': False,
              'journal_sha256': hashlib.sha256(journal.read_bytes()).hexdigest()}
    (output/'summary.json').write_text(json.dumps(result, indent=2)+'\n')
    print('DAILY_CAPTURE_RESULT_JSON='+json.dumps(result))


if __name__ == '__main__':
    asyncio.run(capture(Path('runs/daily-capture')))
