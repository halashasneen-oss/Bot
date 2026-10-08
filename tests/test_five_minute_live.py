import datetime as dt
import pytest
from pairbot.five_minute_live import next_window, parse_contract, public_opening_fields, final_winner, snapshot


def market():
    return {'slug': 'btc-updown-5m-300', 'closed': False, 'enableOrderBook': True,
        'conditionId': '0x'+'a'*64, 'endDate': dt.datetime.fromtimestamp(600, dt.timezone.utc).isoformat(),
        'outcomes': '["Down","Up"]', 'clobTokenIds': '["11","22"]'}


def test_next_window_is_predeclared_with_setup_lead():
    for now in (0, 100, 280, 290, 300):
        assert next_window(now)%300 == 0 and next_window(now)>now+15


def test_up_down_mapping_does_not_assume_token_order():
    assert parse_contract(market(), 300) == {'Down': '11', 'Up': '22'}


@pytest.mark.parametrize('case', ['closed', 'duration', 'outcomes', 'tokens', 'condition'])
def test_invalid_market_is_not_silently_substituted(case):
    m = market()
    if case == 'closed': m['closed'] = True
    if case == 'duration': m['endDate'] = dt.datetime.fromtimestamp(900, dt.timezone.utc).isoformat()
    if case == 'outcomes': m['outcomes'] = '["Yes","No"]'
    if case == 'tokens': m['clobTokenIds'] = '["11","11"]'
    if case == 'condition': m['conditionId'] = 'bad'
    with pytest.raises(ValueError): parse_contract(m, 300)


def test_opening_field_observation_never_admits_reference():
    result = public_opening_fields({'events': [{'id': 1, 'eventMetadata': {'priceToBeat': 86000}}]}, [])
    assert result[0]['value'] == 86000 and not result[0]['matched_reference_admitted']


def test_final_winner_strict_and_preserves_down_first_mapping():
    state = {'condition_id': market()['conditionId'], 'status': 'resolved',
        'resolved_at': dt.datetime.fromtimestamp(650, dt.timezone.utc).isoformat(),
        'resolved_block': 123, 'payouts': [1, 0]}
    assert final_winner(state, market(), 600, 700) == 'Down'
    for change in ({'status': 'proposed'}, {'payouts': [.5, .5]}, {'resolved_block': 0},
                   {'condition_id': 'wrong'}, {'resolved_at': '1970-01-01T00:00:00+00:00'}):
        assert final_winner({**state, **change}, market(), 600, 700) is None


def test_one_sided_or_mismatched_book_has_no_quote():
    row = {'data': {'asset_id': '11', 'bids': [], 'asks': []},
           'received_at': 1, 'rtt_seconds': .1}
    assert snapshot(row, '11')['status'] == 'one_sided'
    assert 'ask' not in snapshot(row, '11')
    assert snapshot(row, '22')['status'] == 'token_mismatch'
