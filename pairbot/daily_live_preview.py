"""Bounded current-market exploratory quote replay; no simulated executions."""
import asyncio
import datetime as dt
import hashlib
import json
import math
import re
import time
from pathlib import Path
from zoneinfo import ZoneInfo
from .daily_model import load_policy, probability, POLICY_SHA256
from .daily_recorder import Journal, PublicReader, RULES, market_tokens, clock_evidence
from .daily_capture import book_status
from .daily_readiness import fee_match
from .ntp_probe import diagnostic

ROUNDS = 12
INTERVAL = 10


def current_event(now):
    local = dt.datetime.fromtimestamp(now, ZoneInfo('America/New_York'))
    day = local.date()
    target = dt.datetime.combine(day, dt.time(12), local.tzinfo).timestamp()
    if now >= target:
        day += dt.timedelta(days=1)
        target = dt.datetime.combine(day, dt.time(12), local.tzinfo).timestamp()
    return day.isoformat(), target, f'bitcoin-above-on-{day.strftime("%B").lower()}-{day.day}-{day.year}'


def contract(market, slug, target):
    suffix = slug.removeprefix('bitcoin-above-on-')
    match = re.fullmatch(r'bitcoin-above-(\d+)k-on-' + re.escape(suffix), market['slug'])
    if not match or market.get('description', '').strip() != RULES:
        raise ValueError('Unknown rules or threshold')
    threshold = int(match[1]) * 1000
    if not re.search(r'(?<!\d)' + str(threshold) + r'(?!\d)', market.get('question', '').replace(',', '')):
        raise ValueError('Question threshold mismatch')
    if market.get('closed') is not False or not market.get('enableOrderBook') or market.get('umaResolutionStatus') not in (None, ''):
        raise ValueError('Closed or resolution-known market')
    if dt.datetime.fromisoformat(market['endDate'].replace('Z', '+00:00')).timestamp() != target:
        raise ValueError('Target mismatch')
    if not re.fullmatch(r'0x[0-9a-fA-F]{64}', market['conditionId']):
        raise ValueError('Invalid condition')
    return threshold, market_tokens(market)


def quote_preview(market, slug, target, candles, fee, pair, now, clock):
    policy = load_policy()
    result = {'market_slug': market.get('slug'), 'p_yes': None, 'quotes': {},
              'financial_action': 'ABSTAIN', 'reason': 'OTHER_COSTS_UNKNOWN',
              'fills': 0, 'realized_pnl': None, 'net_payoff': None, 'timing_checked': False}
    try:
        threshold, tokens = contract(market, slug, target)
        offset, width = clock_evidence(clock, now)
        if not fee_match(market, fee, list(tokens.values())):
            raise ValueError('Fee/token mismatch')
        if len(pair) != 2 or max(r['received_at'] for r in pair) - min(r['received_at'] for r in pair) > policy['maximum_pair_receipt_span_seconds']:
            raise ValueError('Pair receipt skew')
        for row in pair:
            if not all(math.isfinite(float(row[k])) for k in ('received_at', 'rtt_seconds', 'wall_elapsed_seconds')) or 'error' in row or not 0 <= now-row['received_at'] <= 2 or not 0 <= row['rtt_seconds'] <= policy['maximum_http_rtt_seconds'] or abs(row['wall_elapsed_seconds']-row['rtt_seconds']) > policy['maximum_local_clock_step_seconds']:
                raise ValueError('Failed, stale, slow or clock-stepped book')
        model = probability(candles, threshold, now+offset-width, target)
        result.update(p_yes=model['p_yes'], threshold=threshold, timing_checked=True)
        if model['p_yes'] is None:
            result['reason'] = model['reason']
            return result
        quantity = policy['hypothetical_quantity_shares']
        rate = float(fee['fd']['r'])
        for outcome, row in zip(('Yes', 'No'), pair):
            book = row['data']
            status = book_status(book, tokens[outcome])
            if status != 'two_sided':
                result['quotes'][outcome] = {'status': status}
                continue
            bid = max(float(x['price']) for x in book['bids'])
            ask = min(float(x['price']) for x in book['asks'])
            depth = sum(float(x['size']) for x in book['asks'] if float(x['price']) == ask)
            p = model['p_yes'] if outcome == 'Yes' else 1-model['p_yes']
            fee_usd = math.ceil(quantity*rate*ask*(1-ask)*100000)/100000
            edge = p-ask-fee_usd/quantity-policy['slippage_buffer_per_share']
            result['quotes'][outcome] = {'status': status, 'bid': bid, 'ask': ask,
                'midpoint': (bid+ask)/2, 'ask_depth_shares': depth, 'model_probability': p,
                'hypothetical_quantity': quantity, 'taker_fee_usd': fee_usd,
                'edge_before_unknown_other_costs_per_share': edge,
                'informational_candidate': depth >= quantity and edge >= policy['minimum_net_edge_per_share'],
                'executed': False}
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        result['reason'] = str(exc)
    return result


async def run():
    output = Path('runs/live-preview')
    output.mkdir(parents=True, exist_ok=False)
    policy = load_policy()
    started = time.time()
    day, target, slug = current_event(started)
    journal = Journal(output/'journal.jsonl')
    api = PublicReader(journal)
    points = []
    roster = []
    error = None
    journal.append({'type': 'header', 'mode': 'EXPLORATORY_CURRENT_MARKET_ONLY',
        'policy_sha256': POLICY_SHA256, 'day': day, 'slug': slug, 'target': target,
        'rounds': ROUNDS, 'interval_seconds': INTERVAL, 'started_at': started,
        'cohort_admitted': False, 'fills': 0, 'net_payoff': None,
        'receipt_semantics': 'HTTP body completion before JSON, not raw socket'})
    try:
        metadata = await api.get('https://gamma-api.polymarket.com/events', {'slug': slug}, 'metadata')
        event = next(e for e in metadata['data'] if e.get('slug') == slug)
        roster = event['markets']
        if not 1 <= len(roster) <= 32 or len({m['conditionId'] for m in roster}) != len(roster):
            raise ValueError('Invalid full event universe')
        journal.append({'type': 'universe', 'markets': roster})
        valid = []
        for market in roster:
            try:
                _, tokens = contract(market, slug, target)
                fee = await api.get('https://clob.polymarket.com/clob-markets/'+market['conditionId'], role='fee')
                valid.append((market, tokens, fee.get('data')))
            except (KeyError, ValueError, TypeError) as exc:
                points.append({'market_slug': market.get('slug'), 'round': None, 'reason': str(exc), 'p_yes': None, 'quotes': {}})
        start_mono = time.monotonic()
        for index in range(ROUNDS):
            await asyncio.sleep(max(0, start_mono+index*INTERVAL-time.monotonic()))
            if index % 6 == 0:
                clock = await asyncio.to_thread(diagnostic, policy)
                journal.append({'type': 'clock', 'round': index, 'evidence': clock})
                end = int(time.time()//3600)*3600
                iso = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).isoformat()
                candle_row = await api.get(policy['feature_source'], {'granularity': 3600, 'start': iso(end-26*3600), 'end': iso(end)}, 'candles')
            for market, tokens, fee in valid:
                pair = await asyncio.gather(*(api.get('https://clob.polymarket.com/book', {'token_id': tokens[o]}, 'book') for o in ('Yes', 'No')))
                point = quote_preview(market, slug, target, candle_row.get('data'), fee, pair, time.time(), clock)
                point.update(round=index, received_at=max(r['received_at'] for r in pair))
                journal.append({'type': 'preview', **point})
                points.append(point)
    except (KeyError, ValueError, TypeError, StopIteration) as exc:
        error = str(exc)
        journal.append({'type': 'preview_error', 'reason': error})
    finally:
        journal.append({'type': 'sealed', 'sealed_at': time.time(), 'fills': 0})
        await api.close()
        journal.close()
    summary = {'mode': 'EXPLORATORY_CURRENT_MARKET_ONLY', 'slug': slug, 'day': day,
        'started_at': started, 'ended_at': time.time(), 'target': target,
        'markets': len(roster), 'rounds_planned': ROUNDS,
        'sample_points': len(points), 'model_points': sum(p.get('p_yes') is not None for p in points),
        'informational_candidate_points': sum(q.get('informational_candidate', False) for p in points for q in p['quotes'].values()),
        'points': points, 'error': error, 'fills': 0, 'realized_pnl': None,
        'net_payoff': None, 'financial_action': 'ABSTAIN', 'other_costs_usd': None,
        'decision': 'NO_GO_FINANCIAL_UNKNOWN_COSTS', 'cohort_admitted': False,
        'journal_sha256': hashlib.sha256((output/'journal.jsonl').read_bytes()).hexdigest(),
        'note': 'Polling snapshots do not prove continuous opportunity duration or execution; model accuracy and settlement are not measured here.'}
    (output/'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False)+'\n')
    print(json.dumps({k: v for k, v in summary.items() if k != 'points'}))


if __name__ == '__main__':
    asyncio.run(run())
