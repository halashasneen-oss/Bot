"""Bounded public fee/clock evidence. No clock admission or performance test."""
import asyncio
import json
import math
import time
from pathlib import Path
import httpx
from .daily_capture import SLUG, universe
from .ntp_probe import diagnostic


def fee_match(gamma, clob, expected_tokens):
    """Missing or contradictory fields never become zero fees."""
    try:
        g, f = gamma['feeSchedule'], clob['fd']
        token_ids = {str(t['t']) for t in clob['t']}
        rate, exponent = float(f['r']), float(f['e'])
        return (gamma['feesEnabled'] is True and f['to'] is True and g['takerOnly'] is True
                and token_ids == set(expected_tokens) and math.isfinite(rate + exponent)
                and 0 < rate <= 1 and exponent == 1
                and rate == float(g['rate']) and exponent == float(g['exponent']))
    except (KeyError, TypeError, ValueError):
        return False


async def run():
    output = Path('runs/daily-readiness')
    output.mkdir(parents=True, exist_ok=False)
    rows = []
    async with httpx.AsyncClient(timeout=5, trust_env=False, follow_redirects=False) as client:
        async def get(url, params=None):
            row = {'url': url, 'params': params, 'sent_at': time.time()}
            start = time.monotonic()
            try:
                resp = await client.get(url, params=params)
                row.update(received_at=time.time(), rtt_seconds=time.monotonic()-start,
                           status=resp.status_code)
                resp.raise_for_status()
                row['payload'] = resp.json()
            except Exception as exc:
                row['error'] = str(exc)
            rows.append(row)
            return row.get('payload')
        events = await get('https://gamma-api.polymarket.com/events', {'slug': SLUG})
        checks = []
        try:
            tokens = universe(events)
            event = next(e for e in events if e['slug'] == SLUG)
            for market in event['markets']:
                cid = market['conditionId']
                info = await get('https://clob.polymarket.com/clob-markets/'+cid)
                expected = [t['token_id'] for t in tokens if t['condition_id']==cid]
                checks.append({'slug': market['slug'], 'fee_schedule': market.get('feeSchedule'),
                               'clob_fee_details': info.get('fd') if isinstance(info, dict) else None,
                               'fee_fields_and_tokens_match': fee_match(market, info, expected)})
        except Exception as exc:
            checks.append({'universe_error': str(exc), 'fee_fields_and_tokens_match': False})
        clock = await asyncio.to_thread(diagnostic, {'maximum_clock_uncertainty_seconds': .5})
        server_time = await get('https://clob.polymarket.com/time')
    result = {'mode': 'daily-fee-clock-evidence-only', 'slug': SLUG, 'requests': rows,
              'fee_checks': checks, 'all_fee_fields_match': bool(checks) and all(
                  c['fee_fields_and_tokens_match'] for c in checks), 'ntp': clock,
              'clob_server_time': server_time, 'clock_admitted': False,
              'other_costs_verified': False, 'decision': 'NO_GO_PERFORMANCE',
              'predictions': 0, 'fills': 0, 'realized_pnl': None}
    (output/'result.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    print(json.dumps({k: result[k] for k in ('all_fee_fields_match', 'clock_admitted', 'decision')}))


if __name__ == '__main__':
    asyncio.run(run())
