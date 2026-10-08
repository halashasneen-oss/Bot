from types import SimpleNamespace

import pytest

from pairbot.mandatory_v3 import (
    FROZEN_V3, Account, V3Reference, paper_quote_pair,
    price_cap_for_strength, score_decision, validate_quote, validate_signal,
)
from pairbot.momentum import Failures, TOPIC


def book(token='1', bid=.49, ask=.51, bid_size=100, ask_size=100, minimum=5):
    return dict(
        asset_id=token, timestamp='540000', min_order_size=str(minimum),
        bids=[] if bid is None else [dict(price=str(bid), size=str(bid_size))],
        asks=[] if ask is None else [dict(price=str(ask), size=str(ask_size))],
    )


def reference(prices):
    series = {}
    for index, price in enumerate(prices):
        ts = 300 + index * 60
        series[ts] = dict(
            timestamp=ts, price=price, received_at=ts + .2, generation=1)
    return SimpleNamespace(series=series, invalid=set()), series[300], series[max(series)]


def test_v3_config_is_frozen_value_aware_and_100_5():
    assert FROZEN_V3['candidate_entry_times_before_close_sec'] == [150,120,90,60,45]
    assert FROZEN_V3['starting_capital_usd'] == 100
    assert FROZEN_V3['stake_usd'] == 5
    assert FROZEN_V3['min_core_strength'] == pytest.approx(.20)
    assert FROZEN_V3['price_cap_ceiling'] == pytest.approx(.78)
    assert FROZEN_V3['max_adverse_slippage'] == pytest.approx(.04)
    assert FROZEN_V3['live'] is False


def test_market_price_cannot_flip_btc_core_direction():
    ref, opening, latest = reference([110,108,106,104,100])
    result = score_decision(
        ref, 300, opening, latest,
        book('1', .90, .91, 500, 20),
        book('2', .09, .10, 20, 500),
        FROZEN_V3)
    assert result['core_score'] < 0
    assert result['side'] == 'Down'


def test_price_cap_is_stricter_for_weaker_signal():
    weak = price_cap_for_strength(.25, FROZEN_V3)
    strong = price_cap_for_strength(1.0, FROZEN_V3)
    assert weak < strong
    assert strong == pytest.approx(.78)
    assert weak < .65


def test_prior_high_price_loss_pattern_is_rejected():
    strength = abs(-41.866719)/75
    score = dict(
        core_strength=strength, signal_agreement=1.0,
        price_cap=price_cap_for_strength(strength, FROZEN_V3))
    validate_signal(score, FROZEN_V3)
    with pytest.raises(ValueError, match='price_above_cap'):
        validate_quote(dict(effective_entry=.90), score, FROZEN_V3)


def test_prior_059_value_pattern_can_pass_price_gate():
    strength = abs(-20.832472)/75
    score = dict(
        core_strength=strength, signal_agreement=2/3,
        price_cap=price_cap_for_strength(strength, FROZEN_V3))
    validate_signal(score, FROZEN_V3)
    validate_quote(dict(effective_entry=.59), score, FROZEN_V3)


def test_complementary_bid_is_real_fallback_when_direct_ask_missing():
    selected = book('1', .55, None)
    opposite = book('2', .40, .42)
    quote = paper_quote_pair(
        selected, opposite, '1', '2', .07, FROZEN_V3)
    assert quote['execution_route'] == 'complementary_bid'
    assert .59 <= quote['vwap'] <= .61
    assert quote['cost'] <= 5


def test_direct_ask_is_preferred_to_avoid_double_counting():
    selected = book('1', .54, .56)
    opposite = book('2', .43, .45)
    quote = paper_quote_pair(
        selected, opposite, '1', '2', .07, FROZEN_V3)
    assert quote['execution_route'] == 'direct_ask'
    assert quote['vwap'] == pytest.approx(.56)


def test_adverse_slippage_guard():
    score = dict(price_cap=.75)
    validate_quote(dict(effective_entry=.70), score, FROZEN_V3, .68)
    with pytest.raises(ValueError, match='adverse_slippage'):
        validate_quote(dict(effective_entry=.73), score, FROZEN_V3, .68)


def test_one_position_and_fixed_five_dollar_stake_remain():
    account = Account(300, 100, 5)
    row = {}
    quote = dict(cost=5, shares=7, live_minimum_met=True)
    account.enter(300, {}, 'Up', quote, row, 450)
    assert account.cash == pytest.approx(95)
    with pytest.raises(ValueError, match='open_position_limit'):
        account.enter(600, {}, 'Down', quote, {}, 750)


class Sink:
    def write(self, row):
        pass


def message(ts, price):
    return dict(topic=TOPIC, type='update', payload=dict(
        symbol='btc/usd', window_s=60, timestamp=ts*1000, value=price))


def test_reference_opening_survives_reconnect_in_v3():
    r = V3Reference(Sink(), Failures())
    r.connected = True
    r.generation = 1
    r.ingest(message(300, 80000), 300.5)
    r.generation = 2
    r.ingest(message(450, 80020), 450.5)
    opening, latest = r.prices(300, 450.8, 450.8, FROZEN_V3)
    assert opening['price'] == 80000
    assert latest['generation'] == 2
