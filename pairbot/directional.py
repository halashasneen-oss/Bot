"""Independent causal prediction journal. No execution, credentials or network I/O.

The input is a normalized, prospectively observed public-data stream, not an old
pairbot journal. Source admission is an evidence claim that an adapter/operator
must audit; a URL/verified flag alone cannot prove that a feed matches settlement.
"""
import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re


FREEZE_COMMIT = 'f869678fb0f545ac7ae1bf69ab467e78a92e3312'
POLICY_SHA256 = '579fbb066e681bb3272b1fa8ca60a21b0577774e5398234dc06d62afc9521c15'
POLICY_PATH = Path(__file__).resolve().parent.parent / 'docs/directional/policy.json'
FREEZE_TIME = 1791144301.0  # Exact policy commit timestamp: 2026-10-04 20:05:01 UTC.


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def finite(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('non-finite or non-numeric input')
    return float(value)


def evidence_url(value):
    return isinstance(value, str) and value.startswith('https://') and len(value) > 12


def load_policy():
    raw = POLICY_PATH.read_bytes()
    if hashlib.sha256(raw).hexdigest() != POLICY_SHA256:
        raise ValueError('Frozen directional policy changed; register a new comparison')
    return json.loads(raw)


def probability(price, opening, sigma_price, now, end, kind, width=60, observed_area=None):
    """Zero-drift arithmetic Brownian baseline; not a calibrated prediction model.

    TWAP forecasts the last `width` seconds with continuous Brownian covariance.
    If part of that interval has passed, its exact causal area must be supplied;
    no interpolation or gap fill is performed here. Raw TWAP ticks must NOT be
    used as independent underlying spot increments.
    """
    price, opening, sigma_price, now, end = map(finite, (price, opening, sigma_price, now, end))
    if price <= 0 or opening <= 0 or sigma_price <= 0 or now >= end:
        raise ValueError('invalid forecast inputs')
    remaining = end - now
    if kind == 'terminal':
        mean, variance = price, sigma_price ** 2 * remaining
    elif kind == 'twap':
        width = finite(width)
        if width <= 0:
            raise ValueError('invalid TWAP width')
        a = max(now, end - width)
        duration = end - a
        if now > end - width:
            if observed_area is None or finite(observed_area) <= 0:
                raise ValueError('missing exact observed TWAP area')
            area = observed_area
        else:
            area = 0
        mean = (area + price * duration) / width
        variance = sigma_price ** 2 * ((a - now) * duration ** 2 + duration ** 3 / 3) / width ** 2
    else:
        raise ValueError('unsupported settlement rule')
    return .5 * (1 + math.erf((mean - opening) / math.sqrt(2 * variance)))


def volatility(history, now, policy):
    points = []
    for item in history:
        t, received, price = map(finite, (item['exchange_at'], item['received_at'], item['price']))
        if received > now['wall'] or t > now['server'] or price <= 0:
            raise ValueError('future or invalid reference history')
        if now['server'] - policy['volatility_lookback_seconds'] <= t <= now['server']:
            points.append((t, price))
    if len(points) - 1 < policy['minimum_price_intervals']:
        raise ValueError('insufficient reference history')
    if points[0][0] > now['server'] - policy['volatility_lookback_seconds'] + policy['maximum_price_gap_seconds']:
        raise ValueError('incomplete volatility lookback')
    total, elapsed = 0., 0.
    for (t0, p0), (t1, p1) in zip(points, points[1:]):
        dt = t1 - t0
        if not 0 < dt <= policy['maximum_price_gap_seconds']:
            raise ValueError('unordered or gapped reference history')
        total += math.log(p1 / p0) ** 2
        elapsed += dt
    if total <= 0:
        raise ValueError('zero measured volatility')
    return math.sqrt(total / elapsed)


def fresh(item, wall, server, policy):
    received, exchange = finite(item['received_at']), finite(item['exchange_at'])
    return (0 <= wall - received <= policy['maximum_data_age_seconds']
            and 0 <= server - exchange <= policy['maximum_data_age_seconds'])


def book_cost(book, quantity, rate, buffer):
    bids, asks = book['bids'], book['asks']
    if not bids or not asks:
        raise ValueError('empty book')
    for levels, reverse in ((bids, True), (asks, False)):
        prices = []
        for price, size in levels:
            price, size = finite(price), finite(size)
            if not 0 < price < 1 or size <= 0:
                raise ValueError('invalid depth')
            prices.append(price)
        if prices != sorted(set(prices), reverse=reverse):
            raise ValueError('unordered or duplicate depth')
    if bids[0][0] >= asks[0][0]:
        raise ValueError('crossed or locked book')
    remaining, cost, fee = quantity, 0., 0.
    for price, size in asks:
        take = min(size, remaining)
        if price + buffer >= 1:
            raise ValueError('invalid buffered price')
        cost += take * (price + buffer)
        # Conservative fee bound over the slippage interval, rounded UP per
        # level, rather than treating the configured fee as constant bps.
        fee_price = min(max(.5, price), price + buffer)
        fee += math.ceil(take * rate * fee_price * (1 - fee_price) * 1e5) / 1e5
        remaining -= take
        if remaining <= 1e-9:
            break
    if remaining > 1e-9:
        raise ValueError('insufficient ask depth')
    return cost + fee, (bids[0][0] + asks[0][0]) / 2


def decide(row, policy):
    """Return an abstention on missing/late/unverified data, never infer a fill."""
    output = {'market_id': row.get('market', {}).get('id'), 'received_at': row.get('received_at'),
              'p_yes': None, 'market_p_yes': None, 'choice': 'ABSTAIN', 'data_clean': False,
              'reasons': [], 'bounds': {}, 'fills': 0, 'realized_pnl': None}
    try:
        wall = finite(row['received_at'])
        market, ref, clock = row['market'], row['reference'], row['clock']
        start, end = finite(market['start']), finite(market['end'])
        slug = re.fullmatch(r'btc-updown-5m-(\d+)', market['slug'])
        if (market['asset'] != 'BTC' or not slug or int(slug[1]) != start
                or end - start != 300 or not market['id'] or not market['yes_token']
                or not market['no_token'] or market['yes_token'] == market['no_token']):
            raise ValueError('invalid BTC five-minute market')
        uncertainty = finite(clock['uncertainty_seconds'])
        if (not 0 <= uncertainty <= policy['maximum_clock_uncertainty_seconds']
                or not 0 <= wall - finite(clock['observed_at']) <= policy['maximum_clock_probe_age_seconds']):
            raise ValueError('clock quality failed')
        server = wall + finite(clock['offset_seconds'])
        age = server - start
        if not policy['decision_age_seconds'] <= age <= policy['decision_age_seconds'] + policy['decision_tolerance_seconds']:
            raise ValueError('outside fixed decision time')
        rules, opening = market['rules'], market['opening']
        rule_hash = rules['sha256']
        if (rules.get('verified') is not True or not evidence_url(rules.get('url'))
                or len(rule_hash) != 64 or any(c not in '0123456789abcdef' for c in rule_hash)
                or finite(rules['observed_at']) > wall):
            raise ValueError('unverified settlement rules')
        if rules['kind'] == 'twap' and rules['window_seconds'] != 60:
            raise ValueError('unsupported TWAP interval')
        if (not rules['source_id'] or ref['source_id'] != rules['source_id']
                or opening['source_id'] != rules['source_id']
                or ref.get('kind') != 'underlying_spot'
                or not evidence_url(opening.get('url')) or not evidence_url(ref.get('url'))
                or opening.get('verified') is not True or ref.get('verified') is not True):
            raise ValueError('missing matched verified reference')
        if (finite(opening['reference_at']) != start
                or not start <= finite(opening['received_at']) <= min(wall, start + policy['maximum_data_age_seconds'])):
            raise ValueError('opening not observed causally at window start')
        if ref.get('gap') is not False or not fresh(ref, wall, server, policy):
            raise ValueError('reference stale or gapped')
        hist = ref['history']
        if not hist or finite(hist[-1]['exchange_at']) != finite(ref['exchange_at']) or finite(hist[-1]['price']) != finite(ref['price']):
            raise ValueError('reference history does not end at current price')
        sigma = volatility(hist, {'wall': wall, 'server': server}, policy) * finite(ref['price'])
        output['p_yes'] = probability(ref['price'], opening['price'], sigma, ref['exchange_at'], end,
                                      rules['kind'], rules.get('window_seconds', 60), ref.get('observed_area'))
        output['rule_sha256'] = rule_hash
        output['source_id'] = rules['source_id']
        quantity = policy['quantity_shares']
        if finite(market['minimum_shares']) > quantity:
            raise ValueError('quantity below market minimum')
        fee = row['fees']
        if (fee.get('verified') is not True or not evidence_url(fee.get('url'))
                or not 0 <= wall - finite(fee['observed_at']) <= policy['maximum_clock_probe_age_seconds']
                or finite(fee['exponent']) != 1 or not 0 <= finite(fee['rate']) <= 1):
            raise ValueError('unknown or unsupported market fees')
        other = row.get('other_cost_per_share')
        if (other is None or finite(other) < 0 or row.get('other_cost_verified') is not True
                or not evidence_url(row.get('other_cost_evidence_url'))):
            raise ValueError('unknown other costs')
        for outcome, token in (('YES', market['yes_token']), ('NO', market['no_token'])):
            book = row['books'][outcome]
            if (book['token_id'] != token or book.get('snapshot') is not True
                    or book.get('gap') is not False or not fresh(book, wall, server, policy)):
                raise ValueError('book stale, gapped or mismatched')
            cost, mid = book_cost(book, quantity, fee['rate'], policy['slippage_buffer_per_share'])
            cost += quantity * other
            p = output['p_yes'] if outcome == 'YES' else 1 - output['p_yes']
            output['bounds'][outcome] = {'cost_usd_bound': cost, 'expected_edge_per_share': p - cost / quantity}
            if outcome == 'YES':
                output['market_p_yes'] = mid
        output['data_clean'] = True
        best = max(output['bounds'], key=lambda k: output['bounds'][k]['expected_edge_per_share'])
        bound = output['bounds'][best]
        if bound['cost_usd_bound'] > policy['maximum_hypothetical_loss_usd']:
            output['reasons'].append('hypothetical loss cap')
        elif bound['expected_edge_per_share'] < policy['minimum_edge_per_share']:
            output['reasons'].append('no sufficient net expected edge')
        else:
            output['choice'] = best
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        output['reasons'].append(str(exc))
    return output


class Journal:
    """Exclusive new file + hash chain. Predictions are flushed before outcomes."""
    def __init__(self, path, header):
        self.file = Path(path).open('x', encoding='utf-8')
        self.previous = '0' * 64
        self.write({'type': 'directional_header', **header})

    def write(self, row):
        payload = {'previous_sha256': self.previous, **row}
        digest = hashlib.sha256(canonical(payload).encode()).hexdigest()
        self.file.write(canonical({**payload, 'sha256': digest}) + '\n')
        self.file.flush()
        os.fsync(self.file.fileno())
        self.previous = digest

    def close(self):
        self.file.close()


def read_journal(path):
    previous = '0' * 64
    last_type = None
    with Path(path).open() as stream:
        for line in stream:
            row = json.loads(line)
            digest = row.pop('sha256')
            if row['previous_sha256'] != previous or hashlib.sha256(canonical(row).encode()).hexdigest() != digest:
                raise ValueError('Prediction journal altered or truncated out of order')
            previous = digest
            last_type = row['type']
            yield row
    if last_type != 'complete':
        raise ValueError('Incomplete prediction journal')


def bootstrap_lower(values, policy):
    if not values:
        return None
    rng = random.Random(policy['bootstrap_seed'])
    means = sorted(sum(rng.choice(values) for _ in values) / len(values)
                   for _ in range(policy['bootstrap_repetitions']))
    return means[int(policy['bootstrap_lower_quantile'] * (len(means) - 1))]


def summarize(predictions, resolutions, policy, synthetic=False):
    def cohort(rows):
        scored, selected = [], []
        for row in rows:
            winner = resolutions.get(row['market_id'])
            if winner is None or row['p_yes'] is None or row['market_p_yes'] is None:
                continue
            y = int(winner == 'YES')
            brier = (row['p_yes'] - y) ** 2
            baseline = (row['market_p_yes'] - y) ** 2
            scored.append((brier, baseline))
            if row['choice'] in ('YES', 'NO'):
                cost = row['bounds'][row['choice']]['cost_usd_bound']
                selected.append(int(row['choice'] == winner) - cost / policy['quantity_shares'])
        return {'decisions': len(rows), 'resolved_scored_markets': len(scored),
                'selected_resolved_markets': len(selected),
                'brier': sum(x[0] for x in scored) / len(scored) if scored else None,
                'market_brier': sum(x[1] for x in scored) / len(scored) if scored else None,
                'brier_improvement_lower95': bootstrap_lower([b - a for a, b in scored], policy),
                'hypothetical_payoff_bound_mean_per_share': sum(selected) / len(selected) if selected else None,
                'hypothetical_payoff_bound_lower95_per_share': bootstrap_lower(selected, policy),
                'realized_pnl': None, 'fills': 0}
    clean = [r for r in predictions if r['data_clean']]
    full_report, clean_report = cohort(predictions), cohort(clean)
    fraction = len(clean) / len(predictions) if predictions else 0
    checks = {'real_data': not synthetic,
              'clean_fraction': fraction >= policy['minimum_clean_decision_fraction'],
              'resolved_sample': clean_report['resolved_scored_markets'] >= policy['minimum_resolved_markets'],
              'selected_sample': clean_report['selected_resolved_markets'] >= policy['minimum_selected_markets'],
              'calibration_advantage': (clean_report['brier_improvement_lower95'] or 0) > 0,
              'positive_payoff_bound': (clean_report['hypothetical_payoff_bound_lower95_per_share'] or 0) > 0}
    return {'synthetic': synthetic, 'full': full_report, 'clean': clean_report,
            'excluded_decision_fraction': 1 - fraction, 'excluded_time_fraction': None,
            'exclusion_unit': 'one scheduled decision per market; time coverage not measured',
            'block_reasons': dict(Counter(reason for row in predictions for reason in row['reasons'])),
            'checks': checks, 'decision': 'GO_LONGER_PAPER_MEASUREMENT' if all(checks.values()) else 'NO_GO',
            'policy_sha256': POLICY_SHA256, 'policy_freeze_commit': FREEZE_COMMIT,
            'old_protocol_result': 'NOT_EVALUATED', 'profitability_proven': False}


def run(source, journal_path, report_path):
    policy = load_policy()
    # Header admission BEFORE processing; no pairbot/research/final-holdout input.
    with Path(source).open() as stream:
        header = json.loads(next(stream))
        if header.get('type') != 'directional_input_header' or header.get('schema') != 'btc-directional-v1':
            raise ValueError('Only independent directional-v1 input accepted; old journals prohibited')
        synthetic = header.get('synthetic') is True
        if not synthetic and finite(header['recording_started_at']) < FREEZE_TIME:
            raise ValueError('New prospective recording required')
        first = finite(header['first_market_start'])
        count = header['market_count']
        if first % 300 or isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 10000:
            raise ValueError('Invalid predeclared market universe')
        if not synthetic and first < max(FREEZE_TIME, header['recording_started_at']):
            raise ValueError('Market universe predates prospective recording')
        expected_starts = {first + 300 * i for i in range(count)}
        seen_starts = set()
        if Path(report_path).exists():
            raise ValueError('Report exists; refusing overwrite')
        journal = Journal(journal_path, {'schema': 'btc-directional-v1', 'synthetic': synthetic,
                           'policy_sha256': POLICY_SHA256, 'policy_freeze_commit': FREEZE_COMMIT,
                           'mode': 'prediction-record-only', 'input_header': header})
        predictions, resolutions, markets = {}, {}, {}
        last_received = -math.inf
        try:
            for line in stream:
                row = json.loads(line)
                received = finite(row['received_at'])
                if received < last_received:
                    raise ValueError('Arrival order regression')
                last_received = received
                if row['type'] == 'candidate':
                    if not synthetic and ('SYNTHETIC' in canonical(row) or 'fixture.invalid' in canonical(row)):
                        raise ValueError('Synthetic fixture cannot be relabelled real')
                    market = row['market']
                    cid = market['id']
                    if cid in predictions or cid in resolutions:
                        raise ValueError('Duplicate or post-outcome prediction')
                    market_start = finite(market['start'])
                    if market_start not in expected_starts or market_start in seen_starts:
                        raise ValueError('Duplicate window or outside predeclared market universe')
                    seen_starts.add(market_start)
                    if not synthetic and finite(market['start']) < max(FREEZE_TIME, header['recording_started_at']):
                        raise ValueError('Market predates prospective recording')
                    prediction = decide(row, policy)
                    journal.write({'type': 'prediction', **prediction, 'input_snapshot': row,
                                   'input_sha256': hashlib.sha256(canonical(row).encode()).hexdigest()})
                    predictions[cid] = prediction
                    markets[cid] = market
                elif row['type'] == 'resolution':
                    cid = row['market_id']
                    if (cid not in markets or cid in resolutions or row['winner'] not in ('YES', 'NO')
                            or row.get('status') != 'resolved' or not evidence_url(row.get('url'))
                            or received < finite(markets[cid]['end'])):
                        raise ValueError('Invalid, early or duplicate official resolution')
                    resolutions[cid] = row['winner']
                    journal.write(row)
                else:
                    raise ValueError('Unknown directional event')
            for start in sorted(expected_starts - seen_starts):
                missing = {'market_id': f'MISSING-{int(start)}', 'received_at': None,
                           'p_yes': None, 'market_p_yes': None, 'choice': 'ABSTAIN',
                           'data_clean': False, 'reasons': ['missing scheduled decision'],
                           'bounds': {}, 'fills': 0, 'realized_pnl': None}
                journal.write({'type': 'missing_decision', 'scheduled_market_start': start, **missing})
                predictions[missing['market_id']] = missing
            result = summarize(list(predictions.values()), resolutions, policy, synthetic)
            result['predeclared_market_count'] = count
            journal.write({'type': 'complete', 'decisions': len(predictions)})
            with Path(report_path).open('x') as target:
                target.write(json.dumps(result, indent=2, allow_nan=False) + '\n')
            return result
        finally:
            journal.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, help='New normalized prospective directional JSONL')
    parser.add_argument('--journal', required=True, help='New prediction journal (never overwritten)')
    parser.add_argument('--report', required=True, help='New descriptive report (not realized PnL)')
    args = parser.parse_args()
    result = run(args.input, args.journal, args.report)
    print(json.dumps({'decision': result['decision'], 'realized_pnl': None, 'synthetic': result['synthetic']}))


if __name__ == '__main__':
    main()
