"""Explicitly synthetic integration fixture; never market evidence."""
import argparse
import hashlib
import json
from pathlib import Path


def candidate(direction='YES', start=1800000000):
    now = start + 180
    # 121 known points, 120 intervals, deliberately nonzero measured variance.
    level = 80100 if direction == 'YES' else 79900
    history = [{'exchange_at': now - 120 + i, 'received_at': now - 120 + i,
                'price': level + (i % 2) * 2} for i in range(121)]
    def book(token, ask):
        return {'token_id': token, 'bids': [[ask - .02, 20]], 'asks': [[ask, 20]],
                'exchange_at': now, 'received_at': now, 'snapshot': True, 'gap': False}
    return {'type': 'candidate', 'received_at': now,
            'market': {'id': f'SYNTHETIC-{start}', 'asset': 'BTC', 'slug': f'btc-updown-5m-{start}',
                       'start': start, 'end': start + 300,
                       'yes_token': 'synthetic-yes', 'no_token': 'synthetic-no', 'minimum_shares': 5,
                       'rules': {'kind': 'twap', 'window_seconds': 60, 'source_id': 'SYNTHETIC-BTC-SPOT',
                                 'verified': True, 'sha256': hashlib.sha256(b'SYNTHETIC RULES').hexdigest(),
                                 'url': 'https://fixture.invalid/rules', 'observed_at': start},
                       'opening': {'price': 80000, 'reference_at': start, 'received_at': start,
                                   'source_id': 'SYNTHETIC-BTC-SPOT', 'verified': True,
                                   'url': 'https://fixture.invalid/opening'}},
            'reference': {'source_id': 'SYNTHETIC-BTC-SPOT', 'kind': 'underlying_spot', 'price': level,
                          'exchange_at': now, 'received_at': now, 'history': history, 'gap': False,
                          'verified': True, 'url': 'https://fixture.invalid/reference'},
            'clock': {'observed_at': now, 'offset_seconds': 0, 'uncertainty_seconds': .1},
            'books': {'YES': book('synthetic-yes', .55), 'NO': book('synthetic-no', .55)},
            'fees': {'rate': .07, 'exponent': 1, 'observed_at': now, 'verified': True,
                     'url': 'https://fixture.invalid/market-fees'},
            'other_cost_per_share': .001, 'other_cost_verified': True,
            'other_cost_evidence_url': 'https://fixture.invalid/cost-assumptions'}


def rows():
    yield {'type': 'directional_input_header', 'schema': 'btc-directional-v1', 'synthetic': True,
           'first_market_start': 1800000000, 'market_count': 3}
    for i, direction in enumerate(('YES', 'NO', 'YES')):
        row = candidate(direction, 1800000000 + i * 300)
        if i == 2:
            row['market'].pop('opening')  # Must abstain, never substitute spot.
        yield row
        yield {'type': 'resolution', 'received_at': row['market']['end'] + 1,
               'market_id': row['market']['id'], 'status': 'resolved', 'winner': direction,
               'url': 'https://fixture.invalid/resolution'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    with Path(args.output).open('x') as target:
        for row in rows():
            target.write(json.dumps(row, allow_nan=False) + '\n')


if __name__ == '__main__':
    main()
