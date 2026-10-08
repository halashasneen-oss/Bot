import json
from dataclasses import replace, asdict
import pytest

from pairbot.config import Config
from pairbot.engine import Engine
from pairbot.market import Market, parse_market, resolution
from pairbot.feed import apply

T = 1800000000.0
M = Market('btc-updown-5m-1800000000', 'c', 'u', 'd', T, T + 300, .01, 5)


def book(token, ts, bid=.48, ask=.52, size=10):
    return {'event_type': 'book', 'market': 'c', 'asset_id': token, 'timestamp': ts * 1000,
            'bids': [{'price': str(bid), 'size': str(size)}],
            'asks': [{'price': str(ask), 'size': str(size)}]}


def trade(token, ts, size, side='SELL', price=.48):
    return {'event_type': 'last_trade_price', 'market': 'c', 'asset_id': token,
            'timestamp': ts * 1000, 'price': str(price), 'size': str(size), 'side': side}


def engine(**kwargs):
    e = Engine(replace(Config(), **kwargs))
    e.select(M, T)
    e.frame(book('u', T), T)
    e.frame(book('d', T), T)
    return e


def test_reserve_both_legs_and_no_fills_on_touch():
    e = engine()
    assert len(e.orders) == 2
    assert e.reserved() == pytest.approx(4.8)
    e.frame(book('u', T+1, size=0), T+1)
    assert e.fills == 0
    assert e.cash == 50


def test_buy_aggressor_cannot_fill_bid():
    e = engine()
    e.frame(trade('u', T+1, 100, 'BUY'), T+1)
    assert e.fills == 0


def test_queue_partial_merge_and_cash_conservation():
    e = engine()
    e.frame(trade('u', T+1, 17), T+1)  # 15 ahead, 2 filled
    assert e.positions['u'].size == 2
    e.frame(trade('d', T+2, 17), T+2)
    assert len(e.pending) == 1
    assert e.cash == pytest.approx(48.08)
    assert e.merge_pnl == 0
    e.advance(T+5)
    assert e.cash == pytest.approx(50.08)
    assert e.merge_pnl == pytest.approx(.08)
    assert e.positions['u'].size == e.positions['d'].size == 0


def test_duplicate_print_not_double_filled():
    e = engine()
    msg = trade('u', T+1, 17)
    e.frame(msg, T+1)
    e.frame(msg, T+1.1)
    assert e.positions['u'].size == 2


def test_latency_prevents_early_fill():
    e = engine()
    e.frame(trade('u', T+.1, 100), T+.1)
    assert e.fills == 0


def test_post_only_rejects_crossing_at_arrival():
    e = engine()
    e.frame(book('u', T+1, bid=.44, ask=.47), T+1)
    assert 'u' not in e.orders
    assert e.fills == 0


def test_cancel_inflight_can_still_fill():
    e = engine()
    e.advance(T+1)
    e.cancel_all()
    e.frame(trade('u', T+1.2, 20), T+1.2)
    assert e.fills == 1
    e.advance(T+2)
    assert not e.orders


def test_stale_data_no_fill_and_gap_flag():
    e = engine()
    e.gap(T+1)
    e.frame(trade('u', T+1.1, 100), T+1.1)
    assert e.fills == 0
    assert e.report()['execution_quality'] == 'INCOMPLETE_DATA'


def test_loss_stop_includes_unrealized_loss():
    e = engine(loss_stop_usd=.5)
    e.frame(trade('u', T+1, 20), T+1)
    e.frame(book('u', T+2, bid=.20, ask=.24), T+2)
    assert e.halted == 'session_loss_stop'
    assert e.report()['pnl_marked'] == pytest.approx(-1.4)


def test_residual_loss_offsets_pair_profit():
    e = engine()
    e.frame(trade('u', T+1, 20), T+1)
    e.frame(trade('d', T+2, 17), T+2)
    e.settle('c', 'd', T+300)
    assert e.merge_pnl == pytest.approx(.08)
    assert e.residual_pnl == pytest.approx(-1.44)
    assert e.cash == pytest.approx(48.64)
    assert e.report()['pnl_realized'] == pytest.approx(-1.36)


def test_budget_and_inventory_cap():
    e = engine(max_inventory_usd=4)
    assert not e.orders
    e = engine(order_size=100)
    assert not e.orders


def test_unmatched_timeout_blocks_market():
    e = engine()
    e.frame(trade('u', T+1, 20), T+1)
    e.advance(T+2)
    e.advance(T+23)
    assert 'c' in e.blocked
    assert e.positions['u'].size == 5  # no invented liquidation


def test_no_requote_over_residual():
    e = engine()
    e.frame(trade('u', T+1, 17), T+1)
    e.cancel_all()
    e.advance(T+2)
    e.frame(book('u', T+2), T+2)
    e.frame(book('d', T+2), T+2)
    assert not e.orders


def test_movement_cancels_resting_orders():
    e = engine()
    e.frame(book('u', T+1, bid=.52, ask=.56), T+1)
    assert 'c' in e.movement_active
    assert 'c' not in e.blocked
    assert all(x.cancel_at is not None for x in e.orders.values())


def test_fees_in_pair_edge_and_accounting():
    e = engine(maker_fee_bps=100)
    e.frame(trade('u', T+1, 20), T+1)
    e.frame(trade('d', T+2, 20), T+2)
    e.advance(T+5)
    assert e.fees == pytest.approx(.048)
    assert e.merge_pnl == pytest.approx(.152)
    assert e.cash == pytest.approx(50.152)


def test_depth_valuation_no_midpoint_profit():
    e = engine()
    e.frame(trade('u', T+1, 20), T+1)
    e.frame(book('u', T+2, size=1), T+2)
    assert e.equity() == pytest.approx(48.08)  # 47.6 cash + 0.48 depth


def test_stop_never_reopens_orders():
    e = engine()
    apply(e, {'type': 'stop', 'ts': T+1})
    apply(e, {'type': 'tick', 'ts': T+2})
    assert not e.orders


def test_settlement_requires_actual_resolved_marker():
    raw = {'conditionId': 'c', 'closed': True, 'outcomes': '["Up","Down"]', 'outcomePrices': '["1","0"]'}
    assert resolution(raw, M) is None
    raw['umaResolutionStatus'] = 'resolved'
    assert resolution(raw, M) == 'u'
    raw['outcomePrices'] = '["0.999","0.001"]'
    assert resolution(raw, M) is None


def test_outcome_mapping_does_not_assume_order():
    raw = {'slug': M.slug, 'conditionId': 'c', 'acceptingOrders': True,
           'outcomes': '["Down","Up"]', 'clobTokenIds': '["d","u"]',
           'endDate': '2027-01-15T08:05:00Z', 'orderPriceMinTickSize': .01, 'orderMinSize': 5}
    # Calculate fixture end instead of assuming a date conversion.
    from datetime import datetime, timezone
    raw['endDate'] = datetime.fromtimestamp(T+300, timezone.utc).isoformat()
    assert parse_market(raw) == M


def test_replay_deterministic():
    records = [{'type': 'market', 'ts': T, 'market': M.data()},
               {'type': 'frame', 'ts': T, 'message': book('u', T)},
               {'type': 'frame', 'ts': T, 'message': book('d', T)},
               {'type': 'frame', 'ts': T+1, 'message': trade('u', T+1, 20)},
               {'type': 'frame', 'ts': T+2, 'message': trade('d', T+2, 20)},
               {'type': 'stop', 'ts': T+3}, {'type': 'tick', 'ts': T+6}]
    a, b = Engine(Config()), Engine(Config())
    for row in records:
        apply(a, row)
        apply(b, json.loads(json.dumps(row)))
    assert a.report() == b.report()
    assert a.report()['final_pnl_available']
    assert a.cash == pytest.approx(50.2)


@pytest.mark.parametrize('key,value', [('bankroll',float('nan')),('queue_multiplier',.5),('order_size',0),('duration_minutes',-1),('kill_move',1)])
def test_bad_config(key, value):
    with pytest.raises(ValueError):
        Config(**{key: value})


def test_nan_snapshot_and_time_reversal_rejected():
    e = engine()
    e.frame(book('u', T+1, bid=float('nan')), T+1)
    assert e.rejected_frames == 1
    assert not e.fresh()
    with pytest.raises(ValueError):
        e.advance(T)


def test_old_trade_cannot_fill_new_order():
    e = engine()
    e.frame(trade('u', T-1, 100), T+1)
    assert e.fills == 0


def test_minimum_order_size_respected():
    e = engine(order_size=4)
    assert not e.orders


def test_no_data_never_reported_as_valid_result():
    report = Engine(Config()).report()
    assert report['execution_quality'] == 'NO_DATA'
    assert not report['final_pnl_available']


def test_pending_merge_cannot_fund_new_cycle():
    e = engine()
    e.frame(trade('u', T+1, 20), T+1)
    e.frame(trade('d', T+2, 20), T+2)
    e.quote()
    assert e.pending and not e.orders
    assert e.cash == pytest.approx(45.2)


def test_cash_conservation_with_fee_and_losing_residual():
    e = engine(maker_fee_bps=25, merge_cost_usd=.01)
    e.frame(trade('u', T+1, 20), T+1)
    e.frame(trade('d', T+2, 17), T+2)
    e.settle('c', 'd', T+300)
    assert e.cash - 50 == pytest.approx(e.merge_pnl + e.residual_pnl)
    assert e.cash >= 0


def test_read_only_runtime_has_no_signing_or_write_requests():
    import ast
    from pathlib import Path
    for path in Path('pairbot').glob('*.py'):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in {'post', 'put', 'patch', 'delete', 'sign_transaction', 'send_raw_transaction'}
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or '').startswith(('web3', 'eth_account', 'py_clob_client'))
