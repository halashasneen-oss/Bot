"""One bounded readiness attempt. Only public GETs and two NTP time queries."""
import argparse
import asyncio
import json
from pathlib import Path
import time

from .directional import load_policy, POLICY_SHA256, FREEZE_COMMIT
from .directional_probe import probe
from .feed import PublicAPI
from .ntp_probe import diagnostic


async def readiness():
    result = {'observed_at': time.time(), 'policy_sha256': POLICY_SHA256,
              'policy_freeze_commit': FREEZE_COMMIT, 'mode': 'bounded-public-readiness',
              'source_admission': False, 'clock_admission': False, 'decision': 'NO_GO',
              'predictions': 0, 'fills': 0, 'realized_pnl': None}
    # NTP is outside the event loop, but two samples take at most ~10 seconds.
    result['ntp'] = await asyncio.to_thread(diagnostic, load_policy())
    result['public_metadata'] = await probe()
    api = PublicAPI()
    try:
        url = 'https://api.dataengine.chain.link/api/v1/discovery'
        try:
            catalog = await api.get_once(url, {'base_asset': 'BTC', 'quote_asset': 'USD', 'status': 'live'})
            result['chainlink_catalog'] = {'ok': True, 'url': url, 'body': catalog,
                                           'role': 'metadata only, not reference prices'}
        except Exception as exc:
            result['chainlink_catalog'] = {'ok': False, 'url': url, 'error': f'{type(exc).__name__}: {exc}'}
    finally:
        await api.close()
    result['reference_research'] = {
        'polymarket_stream': {'status': 'AUTHENTICATION_REQUIRED',
                             'source': 'https://docs.polymarket.com/market-data/realtime-data'},
        'chainlink_reports': {'status': 'AUTHENTICATION_REQUIRED',
                              'source': 'https://docs.chain.link/data-streams/reference/data-streams-api/authentication'},
        'chainlink_discovery': {'status': 'PUBLIC_METADATA_ONLY',
                                'source': 'https://docs.chain.link/data-streams/reference/data-streams-api/discovery-endpoint'},
        'public_chainlink_page': {'status': 'NOT_PROVEN_CAUSAL_OR_MATCHED',
                                  'source': 'https://data.chain.link/streams/btc-usd-cexprice-streams'},
        'opening_fields': 'Metadata observation alone is not a matched live underlying reference',
        'onchain_feed': 'Different feed and update process; not admitted as a Data Streams substitute'}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    path = Path(args.output)
    if path.exists():
        raise ValueError('Refusing overwrite')
    result = asyncio.run(readiness())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as target:
        target.write(json.dumps(result, indent=2, allow_nan=False) + '\n')
    # Small complete diagnostic in logs for reproducible review; catalog can be
    # large and remains only in the artifact, not in a trading input.
    summary = {k: v for k, v in result.items() if k != 'chainlink_catalog'}
    summary['chainlink_catalog'] = {k: v for k, v in result['chainlink_catalog'].items() if k != 'body'}
    print('READINESS_RESULT_JSON=' + json.dumps(summary, allow_nan=False))


if __name__ == '__main__':
    main()
