"""Bounded public GET preflight. No wallet, credentials, proxy or reference bypass.

This inspects metadata availability; it does not capture prices, predict, or
admit a settlement reference. Integer /time cannot meet the frozen 0.5s clock
bound with a nonzero RTT. A better evidenced clock is required; no retuning.
"""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import time

from .directional import FREEZE_COMMIT, POLICY_SHA256, load_policy
from .feed import PublicAPI, GAMMA, CLOB
from .research_capture import clock_sample, reference_metadata


async def probe(api=None, now=None):
    owned = api is None
    api = api or PublicAPI()
    now = time.time() if now is None else now
    start = int(now // 300) * 300
    slug = f'btc-updown-5m-{start}'
    result = {'mode': 'bounded-public-get-preflight', 'observed_at': now, 'slug': slug,
              'policy_sha256': POLICY_SHA256, 'policy_freeze_commit': FREEZE_COMMIT,
              'decision': 'NO_GO', 'predictions': 0, 'fills': 0, 'realized_pnl': None,
              'reference_admitted': False, 'errors': [],
              'blockers': ['No audited public underlying reference adapter matching settlement',
                           'Other costs not verified',
                           'Integer /time with nonzero RTT cannot satisfy frozen 0.5s uncertainty'],
              'reference_documentation': 'https://docs.polymarket.com/market-data/realtime-data'}
    try:
        sent, sent_ns = time.time(), time.monotonic_ns()
        try:
            server = await api.get_once(CLOB + '/time')
            result['clock'] = clock_sample(server, sent, time.time(), sent_ns=sent_ns,
                                            received_ns=time.monotonic_ns())
            result['clock_meets_policy'] = result['clock']['uncertainty_seconds'] <= load_policy()['maximum_clock_uncertainty_seconds']
        except Exception as exc:
            result['errors'].append({'source': 'clock', 'error': f'{type(exc).__name__}: {exc}'})
        try:
            rows = await api.get_once(GAMMA + '/markets', {'slug': slug})
            if not isinstance(rows, list):
                raise ValueError('Unexpected Gamma schema')
            raw = next((r for r in rows if r.get('slug') == slug), None)
            if raw is None:
                raise ValueError('Current market absent')
            result['market_metadata_sha256'] = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
            result['metadata_received_at'] = time.time()
            result['condition_id'] = raw.get('conditionId')
            result['rules_text'] = raw.get('description')
            result['resolution_source'] = raw.get('resolutionSource')
            result['crypto_market_config'] = raw.get('cryptoMarketConfig')
            result['fee_schedule'] = raw.get('feeSchedule')
            result['opening_observation'] = reference_metadata(raw)
            # Also check event-level metadata once if nested Gamma lacks the field.
            events = await api.get_once(GAMMA + '/events', {'slug': slug})
            result['event_metadata'] = [e.get('eventMetadata') for e in events if e.get('slug') == slug]
            result['opening_note'] = 'Observed fields are availability evidence only; opening/source alignment not admitted'
        except Exception as exc:
            result['errors'].append({'source': 'Gamma', 'error': f'{type(exc).__name__}: {exc}'})
        return result
    finally:
        if owned:
            await api.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    path = Path(args.output)
    if path.exists():
        raise ValueError('Refusing to overwrite preflight result')
    result = asyncio.run(probe())
    with path.open('x') as target:
        target.write(json.dumps(result, indent=2, allow_nan=False) + '\n')
    print(json.dumps({'decision': result['decision'], 'predictions': 0, 'errors': result['errors']}))


if __name__ == '__main__':
    main()
