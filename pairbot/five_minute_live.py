"""One predeclared live BTC 5m window, public GET only, no reference substitution."""
import asyncio
import datetime as dt
import hashlib
import json
import math
import re
import time
from pathlib import Path
import httpx
from .directional import Journal, read_journal, load_policy, POLICY_SHA256, FREEZE_COMMIT
from .daily_capture import book_status
from .daily_readiness import fee_match
from .ntp_probe import diagnostic

ROUNDS = 31
INTERVAL = 10


def next_window(now):
    return (math.floor((now+15)/300)+1)*300


def parse_contract(raw, start):
    if raw.get('slug') != f'btc-updown-5m-{start}' or raw.get('closed') is not False or not raw.get('enableOrderBook'):
        raise ValueError('Unavailable or mismatched window')
    end = dt.datetime.fromisoformat(raw['endDate'].replace('Z', '+00:00')).timestamp()
    if end != start+300 or not re.fullmatch(r'0x[0-9a-fA-F]{64}', raw['conditionId']):
        raise ValueError('Invalid window boundaries or condition')
    outcomes, ids = raw['outcomes'], raw['clobTokenIds']
    outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
    ids = json.loads(ids) if isinstance(ids, str) else ids
    if len(outcomes) != 2 or set(outcomes) != {'Up', 'Down'} or len(ids) != 2 or len(set(ids)) != 2 or not all(str(t).isdigit() for t in ids):
        raise ValueError('Invalid Up/Down token mapping')
    return dict(zip(outcomes, map(str, ids)))


def public_opening_fields(raw, events):
    # Preserve exact fields and provenance. Availability is not source admission.
    observations = []
    for event in list(raw.get('events') or [])+list(events or []):
        meta = event.get('eventMetadata') or {}
        for key in ('priceToBeat', 'finalPrice'):
            if meta.get(key) is not None:
                observations.append({'source': 'Gamma eventMetadata', 'event_id': event.get('id'),
                    'field': key, 'value': meta[key], 'matched_reference_admitted': False})
    return observations


def snapshot(row, token):
    if 'error' in row:
        return {'status': 'REQUEST_FAILED', 'error': row['error']}
    book = row['data']
    status = book_status(book, token)
    out = {'status': status, 'received_at': row['received_at'], 'rtt_seconds': row['rtt_seconds'],
           'exchange_timestamp_raw': book.get('timestamp')}
    if status == 'two_sided':
        bid = max(float(x['price']) for x in book['bids'])
        ask = min(float(x['price']) for x in book['asks'])
        out.update(bid=bid, ask=ask, midpoint=(bid+ask)/2,
                   ask_depth_shares=sum(float(x['size']) for x in book['asks'] if float(x['price']) == ask))
    return out


def final_winner(state, raw, end, received):
    try:
        if state['condition_id'] != raw['conditionId'] or state['status'].lower() != 'resolved':
            return None
        when = dt.datetime.fromisoformat(state['resolved_at'].replace('Z', '+00:00'))
        if when.tzinfo is None or not end <= when.timestamp() <= received or not isinstance(state['resolved_block'], int) or state['resolved_block'] <= 0:
            return None
        payouts = list(map(float, state['payouts']))
        if len(payouts) != 2 or not all(math.isfinite(p) and p >= 0 for p in payouts) or sum(p>0 for p in payouts) != 1:
            return None
        outcomes = raw['outcomes']
        outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        if len(outcomes) != 2 or set(outcomes) != {'Up', 'Down'}:
            return None
        return outcomes[next(i for i, p in enumerate(payouts) if p>0)]
    except (KeyError, ValueError, TypeError, AttributeError, StopIteration):
        return None


async def run():
    output = Path('runs/five-minute-live')
    output.mkdir(parents=True, exist_ok=False)
    policy = load_policy()
    started = time.time()
    start = next_window(started)
    end = start+300
    slug = f'btc-updown-5m-{start}'
    journal = Journal(output/'journal.jsonl', {'mode': 'EXPLORATORY_FIVE_MINUTE_LIVE',
        'policy_sha256': POLICY_SHA256, 'policy_freeze_commit': FREEZE_COMMIT,
        'predeclared_start': start, 'predeclared_end': end, 'recording_started_at': started,
        'rounds': ROUNDS, 'interval': INTERVAL, 'network_route': 'DIRECT_NO_PROXY',
        'reference_admitted': False, 'fills': 0})
    points, openings, errors = [], [], []
    raw = {}
    winner = None
    fees = None
    decision = None
    clocks = []
    async with httpx.AsyncClient(timeout=2, trust_env=False, follow_redirects=False) as client:
        async def get(url, params=None, role=None):
            row = {'type': 'input', 'url': url, 'params': params, 'role': role, 'sent_at': time.time()}
            mono = time.monotonic()
            try:
                response = await client.get(url, params=params)
                received = time.time()
                row.update(received_at=received, rtt_seconds=time.monotonic()-mono,
                    wall_elapsed_seconds=received-row['sent_at'], status=response.status_code,
                    body_sha256=hashlib.sha256(response.content).hexdigest())
                response.raise_for_status()
                row['data'] = response.json()
            except Exception as exc:
                row.setdefault('received_at', time.time())
                row['error'] = str(exc)
            journal.write(row)
            return row
        async def metadata():
            m, e = await asyncio.gather(
                get('https://gamma-api.polymarket.com/markets', {'slug': slug}, 'metadata'),
                get('https://gamma-api.polymarket.com/events', {'slug': slug}, 'events'))
            matched = next((x for x in m.get('data', []) if x.get('slug') == slug), {})
            obs = public_opening_fields(matched, [x for x in e.get('data', []) if x.get('slug') == slug])
            for x in obs:
                x.update(received_at=max(m['received_at'], e['received_at']),
                    phase='PRE_START' if time.time()<start else 'DURING' if time.time()<end else 'AFTER_END')
            openings.extend(obs)
            return matched
        try:
            raw = await metadata()
            tokens = parse_contract(raw, start)
            journal.write({'type': 'universe', 'market': raw, 'tokens': tokens})
            fee = await get('https://clob.polymarket.com/clob-markets/'+raw['conditionId'], role='fee')
            fees = {'matched': fee_match(raw, fee.get('data'), list(tokens.values())),
                    'gamma': raw.get('feeSchedule'), 'clob_fd': (fee.get('data') or {}).get('fd')}
            for index in range(ROUNDS):
                await asyncio.sleep(max(0, start+index*INTERVAL-time.time()))
                if index % 6 == 0:
                    evidence = await asyncio.to_thread(diagnostic, policy)
                    clocks.append(evidence)
                    journal.write({'type': 'clock', 'round': index, 'evidence': evidence})
                    await metadata()
                rows = await asyncio.gather(*(get('https://clob.polymarket.com/book', {'token_id': tokens[o]}, 'book') for o in ('Up', 'Down')))
                point = {'type': 'snapshot', 'round': index, 'age_seconds': time.time()-start,
                    'books': {o: snapshot(r, tokens[o]) for o, r in zip(('Up', 'Down'), rows)}}
                points.append(point)
                journal.write(point)
                if index == 18:
                    decision = {'type': 'directional_decision', 'scheduled_age_seconds': policy['decision_age_seconds'],
                        'observed_age_seconds': time.time()-start, 'choice': 'ABSTAIN', 'p_up': None,
                        'reasons': ['MISSING_AUDITED_MATCHED_PUBLIC_UNDERLYING_REFERENCE',
                                    'NO_CAUSAL_MATCHED_OPENING_ADMITTED', 'OTHER_COSTS_UNKNOWN'],
                        'fills': 0, 'realized_pnl': None, 'net_payoff': None}
                    journal.write(decision)
            for lag in (10, 30, 60):
                await asyncio.sleep(max(0, end+lag-time.time()))
                await metadata()
                row = await get('https://data-api.polymarket.com/v2/resolutions', {'condition': raw['conditionId']}, 'resolution')
                states = (row.get('data') or {}).get('data', [])
                if isinstance(states, list):
                    for state in states:
                        label = final_winner(state, raw, end, row['received_at'])
                        if label is not None:
                            winner = label
                if winner is not None:
                    break
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(str(exc))
            journal.write({'type': 'failure', 'reason': str(exc)})
        finally:
            journal.write({'type': 'complete', 'completed_at': time.time(), 'fills': 0})
            journal.close()
    list(read_journal(output/'journal.jsonl'))
    summary = {'mode': 'EXPLORATORY_FIVE_MINUTE_LIVE', 'slug': slug, 'start': start, 'end': end,
        'started_at': started, 'finished_at': time.time(), 'rules_text': raw.get('description'),
        'resolution_source': raw.get('resolutionSource'), 'crypto_market_config': raw.get('cryptoMarketConfig'),
        'opening_fields': openings, 'source_admitted': False, 'fees': fees, 'points': points,
        'clock_checks': [{'all_samples_within_bound': c['all_samples_within_frozen_bound'],
            'consistent': c['intervals_consistent'], 'worst_uncertainty': c['worst_uncertainty_seconds']} for c in clocks],
        'scheduled_decision': decision, 'official_final_winner': winner,
        'final_status': 'FINAL' if winner else 'PENDING_OR_UNSUPPORTED', 'errors': errors,
        'fills': 0, 'realized_pnl': None, 'net_payoff': None, 'decision': 'NO_GO_REFERENCE_AND_COSTS',
        'polling_note': 'HTTP body completion before JSON. Snapshots do not establish continuous prices or executions.',
        'journal_sha256': hashlib.sha256((output/'journal.jsonl').read_bytes()).hexdigest()}
    (output/'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False)+'\n')
    print(json.dumps({k: v for k, v in summary.items() if k not in ('points', 'opening_fields', 'rules_text')}))


if __name__ == '__main__':
    asyncio.run(run())
