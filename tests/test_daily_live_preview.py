import copy
import datetime as dt
import pytest
from pairbot.daily_live_preview import current_event, quote_preview
from pairbot.daily_recorder import day_times, RULES


def setup():
    decision, target, slug = day_times('2026-10-06')
    now = decision+10
    market = {'slug': 'bitcoin-above-84k-on-october-6-2026', 'question': 'Bitcoin above $84,000?',
        'conditionId': '0x'+'a'*64, 'endDate': dt.datetime.fromtimestamp(target, dt.timezone.utc).isoformat(),
        'description': RULES, 'closed': False, 'enableOrderBook': True,
        'outcomes': ['Yes', 'No'], 'clobTokenIds': ['11', '22'], 'feesEnabled': True,
        'feeSchedule': {'rate': .07, 'exponent': 1, 'takerOnly': True}}
    end = int(now//3600)*3600
    candles = [[t, 83000, 85000, 84000, 84000+(i%2)*100, 10]
        for i, t in enumerate(range(end-25*3600, end, 3600))]
    pair = [{'received_at': now-1, 'rtt_seconds': .1, 'wall_elapsed_seconds': .1,
        'data': {'asset_id': token, 'bids': [{'price': '.4', 'size': '5'}],
                 'asks': [{'price': '.6', 'size': '5'}]}} for token in ('11', '22')]
    fee = {'fd': {'r': .07, 'e': 1, 'to': True}, 't': [{'t': '11'}, {'t': '22'}]}
    clock = {'source': 'time.cloudflare.com', 'intervals_consistent': True, 'samples': [
        {'ok': True, 'observed_at': now-5+i*2, 'offset_seconds': 0,
         'uncertainty_seconds': .01, 'wall_minus_monotonic_elapsed_seconds': 0} for i in range(2)]}
    return dict(market=market, slug=slug, target=target, candles=candles,
                fee=fee, pair=pair, now=now, clock=clock)


def test_current_event_rolls_at_noon_and_uses_new_york_date():
    ts = dt.datetime(2026, 10, 5, 0, tzinfo=dt.timezone.utc).timestamp()
    day, target, slug = current_event(ts)
    assert day == '2026-10-05' and slug == 'bitcoin-above-on-october-5-2026'
    assert dt.datetime.fromtimestamp(target, dt.timezone.utc).hour == 16
    assert current_event(target)[0] == '2026-10-06'


def test_quote_preview_is_informational_never_fills_or_net_profit():
    a = setup()
    # Cheap ask intentionally produces a synthetic informational candidate.
    a['pair'][0]['data']['bids'][0]['price'] = '.09'
    a['pair'][0]['data']['asks'][0]['price'] = '.1'
    row = quote_preview(**a)
    q = row['quotes']['Yes']
    assert row['p_yes'] is not None and q['informational_candidate']
    assert q['taker_fee_usd'] >= 5*.07*.1*.9
    assert q['edge_before_unknown_other_costs_per_share'] < row['p_yes']-.1
    assert row['financial_action'] == 'ABSTAIN' and row['fills'] == 0
    assert row['net_payoff'] is None and row['realized_pnl'] is None
    assert not q['executed']


@pytest.mark.parametrize('case', ['resolved', 'rules', 'fee', 'clock', 'skew', 'slow', 'nan', 'candle'])
def test_invalid_evidence_cannot_produce_a_candidate(case):
    a = copy.deepcopy(setup())
    if case == 'resolved': a['market']['umaResolutionStatus'] = 'resolved'
    if case == 'rules': a['market']['description'] = 'Unknown'
    if case == 'fee': a['fee']['fd']['r'] = 0
    if case == 'clock': a['clock']['samples'][0]['observed_at'] -= 100
    if case == 'skew': a['pair'][0]['received_at'] -= 3
    if case == 'slow': a['pair'][0]['rtt_seconds'] = 3
    if case == 'nan': a['pair'][0]['wall_elapsed_seconds'] = float('nan')
    if case == 'candle': a['candles'].pop()
    row = quote_preview(**a)
    assert row['p_yes'] is None and not row['quotes'] and row['fills'] == 0
