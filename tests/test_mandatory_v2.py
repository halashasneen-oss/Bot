import json
from types import SimpleNamespace

import pytest

from pairbot.mandatory_v2 import (
    FROZEN_V2, WEIGHTS, Account, V2Reference, best_ask_or_none,
    clob_clock_sample, fresh_book_v2, load_config, micro_quote,
    next_full_window_start, record_execution_result, score_decision,
)
from pairbot.momentum import Failures, TOPIC


def book(token='1', bid=.49, ask=.51, bid_size=100, ask_size=100, minimum=5):
    return dict(asset_id=token, timestamp='540000', min_order_size=str(minimum),
                bids=[dict(price=str(bid), size=str(bid_size))],
                asks=[dict(price=str(ask), size=str(ask_size))])


def reference(prices):
    series = {}
    for index, price in enumerate(prices):
        ts = 300 + index * 60
        series[ts] = dict(timestamp=ts, price=price, received_at=ts + .2, generation=1)
    return SimpleNamespace(series=series), series[300], series[max(series)]


def test_v2_frozen_parameters_are_100_bankroll_and_five_dollar_stake(tmp_path):
    assert FROZEN_V2['entry_time_before_close_sec'] == 60
    assert FROZEN_V2['entry_tolerance_sec'] == 5
    assert FROZEN_V2['starting_capital_usd'] == 100
    assert FROZEN_V2['stake_usd'] == 5
    assert FROZEN_V2['max_stale_data_sec'] == 2
    assert FROZEN_V2['execution_delay_sec'] == 2
    assert FROZEN_V2['live'] is False
    assert 'min_move_usd' not in FROZEN_V2
    assert sum(WEIGHTS.values()) == 100
    path = tmp_path/'v2.yaml'
    path.write_text('\n'.join(f'{k}: {json.dumps(v)}' for k,v in FROZEN_V2.items()))
    assert dict(load_config(path)) == dict(FROZEN_V2)
    path.write_text(path.read_text().replace('stake_usd: 5', 'stake_usd: 6'))
    with pytest.raises(ValueError, match='Frozen mandatory v2'):
        load_config(path)


def test_score_follows_strong_up_and_down_structure():
    up_ref, up_open, up_latest = reference([100, 102, 104, 106, 108])
    up = score_decision(up_ref, 300, up_open, up_latest,
                        book('1', .58, .60, 200, 50),
                        book('2', .40, .42, 50, 200))
    assert up['side'] == 'Up' and up['total_score'] > 0

    down_ref, down_open, down_latest = reference([108, 106, 104, 102, 100])
    down = score_decision(down_ref, 300, down_open, down_latest,
                          book('1', .40, .42, 50, 200),
                          book('2', .58, .60, 200, 50))
    assert down['side'] == 'Down' and down['total_score'] < 0


def test_tiny_move_still_gets_a_direction_no_80_dollar_gate():
    ref, opening, latest = reference([100, 100.1, 100.2, 100.3, 100.5])
    result = score_decision(ref, 300, opening, latest,
                            book('1', .49, .51), book('2', .49, .51))
    assert result['delta'] == pytest.approx(.5)
    assert result['side'] == 'Up'
    assert result['total_score'] > 0


def test_exact_tie_is_deterministic_not_random():
    ref, opening, latest = reference([100, 100, 100, 100, 100])
    result = score_decision(ref, 300, opening, latest,
                            book('1', .49, .51), book('2', .49, .51))
    assert result['total_score'] == pytest.approx(0)
    assert result['side'] == 'Up'


def test_five_dollar_fill_uses_real_asks_and_tracks_live_minimum():
    q = micro_quote(book('1', .49, .50, minimum=5), '1', .07, FROZEN_V2)
    assert 4.999 <= q['cost'] <= 5
    assert q['shares'] >= 5
    assert q['live_minimum_met'] is True

    ask_only = book('1', .49, .50, minimum=5)
    ask_only['bids'] = []
    q2 = micro_quote(ask_only, '1', .07, FROZEN_V2)
    assert 4.999 <= q2['cost'] <= 5

    bid_only = book('1', .49, .50, minimum=5)
    bid_only['asks'] = []
    with pytest.raises(ValueError, match='no_executable_ask'):
        micro_quote(bid_only, '1', .07, FROZEN_V2)


def test_account_never_risks_more_than_five_dollars_per_entry():
    account = Account(300, 100, 5)
    row = {}
    quote = dict(cost=5, shares=6, live_minimum_met=True)
    account.enter(300, {}, 'Up', quote, row, 540)
    assert account.cash == pytest.approx(95)
    with pytest.raises(ValueError):
        account.enter(600, {}, 'Down', quote, {}, 840)


class Sink:
    def write(self, row):
        pass


def message(ts, price):
    return dict(topic=TOPIC, type='update', payload=dict(
        symbol='btc/usd', window_s=60, timestamp=ts*1000, value=price))


def test_opening_survives_reconnect_when_causally_observed():
    r = V2Reference(Sink(), Failures())
    r.connected = True
    r.generation = 1
    r.ingest(message(300, 80000), 300.5)
    r.ingest(message(360, 80010), 360.4)
    r.ingest(message(420, 80020), 420.4)

    r.generation = 2
    r.ingest(message(480, 80030), 480.4)
    r.ingest(message(540, 80040), 540.4)

    opening, latest = r.prices(300, 540.8, 540.8, FROZEN_V2)
    assert opening['price'] == 80000
    assert opening['generation'] == 1
    assert latest['price'] == 80040
    assert latest['generation'] == 2


def test_score_uses_valid_history_across_reconnect_generations():
    r = V2Reference(Sink(), Failures())
    r.connected = True
    r.generation = 1
    for ts, price in [(300,100),(360,101),(420,102)]:
        r.ingest(message(ts, price), ts+.2)
    r.generation = 2
    for ts, price in [(480,103),(540,104)]:
        r.ingest(message(ts, price), ts+.2)

    opening, latest = r.prices(300, 540.5, 540.5, FROZEN_V2)
    result = score_decision(
        r, 300, opening, latest,
        book('1', .58, .60, 200, 50),
        book('2', .40, .42, 50, 200),
    )
    assert result['side'] == 'Up'
    assert result['total_score'] > 0


def test_reconnect_does_not_relax_latest_tick_freshness():
    r = V2Reference(Sink(), Failures())
    r.connected = True
    r.generation = 1
    r.ingest(message(300, 80000), 300.5)
    r.generation = 2
    r.ingest(message(537, 80040), 537.2)
    with pytest.raises(ValueError, match='stale_reference'):
        r.prices(300, 540, 540, FROZEN_V2)



def test_book_clock_uncertainty_allows_small_apparent_future_only():
    row = dict(data=book('1'), received_at=540.2, rtt=.1)
    row['data']['timestamp'] = '540600'
    fresh_book_v2(row, '1', 540.0, .75, 540.2, FROZEN_V2)

    row['data']['timestamp'] = '541000'
    with pytest.raises(ValueError, match='stale_book'):
        fresh_book_v2(row, '1', 540.0, .75, 540.2, FROZEN_V2)


def test_book_upper_stale_limit_remains_two_seconds():
    row = dict(data=book('1'), received_at=540.2, rtt=.1)
    row['data']['timestamp'] = '537990'
    with pytest.raises(ValueError, match='stale_book'):
        fresh_book_v2(row, '1', 540.0, 1.0, 540.2, FROZEN_V2)


def test_opening_capture_has_bounded_live_arrival_grace():
    r = V2Reference(Sink(), Failures())
    r.connected = True
    r.generation = 1
    r.ingest(message(300, 80000), 304.0)
    r.ingest(message(540, 80020), 540.5)
    opening, _ = r.prices(300, 540.6, 540.6, FROZEN_V2)
    assert opening['price'] == 80000

    bad = V2Reference(Sink(), Failures())
    bad.connected = True
    bad.generation = 1
    bad.ingest(message(300, 80000), 306.0)
    bad.ingest(message(540, 80020), 540.5)
    with pytest.raises(ValueError, match='missing_causal'):
        bad.prices(300, 540.6, 540.6, FROZEN_V2)


def test_next_run_starts_on_complete_future_window_with_warmup():
    assert next_full_window_start(601, 15) == 900
    assert next_full_window_start(885, 15) == 900
    assert next_full_window_start(886, 15) == 1200
    assert next_full_window_start(900, 15) == 1200



def test_one_sided_pair_does_not_block_score_or_invent_market_midpoint():
    up = book('1', .01, .99)
    down = book('2', .01, .99)
    up['asks'] = []
    down['bids'] = []
    ref, opening, latest = reference([100, 101, 102, 103, 104])
    result = score_decision(ref, 300, opening, latest, up, down)
    assert result['side'] == 'Up'
    assert result['up_mid'] == pytest.approx(.5)
    assert result['down_mid'] == pytest.approx(.5)
    assert result['order_book_score'] == pytest.approx(0)
    assert result['pricing_score'] == pytest.approx(0)
    assert result['up_has_ask'] is False
    assert result['down_has_ask'] is True


def test_scaled_resolution_payout_identifies_winner():
    from pairbot.five_minute_live import final_winner
    raw = {'conditionId':'0x'+'1'*64, 'outcomes':['Up','Down']}
    state = {
        'condition_id':raw['conditionId'],
        'status':'resolved',
        'resolved_at':'1970-01-01T00:10:50+00:00',
        'resolved_block':123,
        'payouts':[0, 1000000],
    }
    assert final_winner(state, raw, 600, 700) == 'Down'



def test_clob_integer_second_clock_is_centered_not_biased_low():
    sample = clob_clock_sample(100, 100.8, 101.0)
    assert sample['offset'] == pytest.approx(-0.4)
    assert sample['uncertainty'] == pytest.approx(.6)

    # Mirrors the prior false-stale pattern: the book is only 50ms old by
    # receive time, while a floor-second /time sample made it look future.
    row = dict(data=book('1'), received_at=110.05, rtt=.1)
    row['data']['timestamp'] = '110000'
    estimated_server = row['received_at'] + sample['offset']
    fresh_book_v2(
        row, '1', estimated_server, sample['uncertainty'],
        row['received_at'], FROZEN_V2)


def test_empty_selected_ask_is_explicit_not_min_runtime_error():
    bid_only = book('1')
    bid_only['asks'] = []
    assert best_ask_or_none(bid_only) is None
    with pytest.raises(ValueError, match='no_executable_ask'):
        micro_quote(bid_only, '1', .07, FROZEN_V2)


def test_stale_book_is_window_abstention_not_execution_kill_switch():
    failures = Failures()
    failures.fail('execution')
    assert failures.counts['execution'] == 1
    record_execution_result(failures, 'stale_book')
    assert failures.counts['execution'] == 0
    assert failures.halted is False


def test_unknown_runtime_reason_counts_as_execution_failure():
    failures = Failures()
    record_execution_result(failures, 'unexpected_runtime_bug')
    assert failures.counts['execution'] == 1
    assert failures.halted is False
