from dataclasses import asdict
import pytest

from pairbot.config import Config
from pairbot.engine import Engine, MODEL_VERSION
from pairbot.feed import Journal, apply
from pairbot.__main__ import load_engine
from pairbot.market import Market
from tests.test_engine import T, M, engine, book, trade


def refresh(e, ts, bid=.48, ask=.52):
    e.frame(book('u', ts, bid=bid, ask=ask), ts)
    e.frame(book('d', ts), ts)


def test_movement_resumes_only_after_cooldown_and_observed_stability():
    e = engine()
    refresh(e, T+1, .52, .56)
    assert e.movement_active == {'c'} and 'c' not in e.blocked
    for i in range(2, 16):
        refresh(e, T+i, .52, .56)
        assert not e.orders or all(o.cancel_at is not None for o in e.orders.values())
    refresh(e, T+16, .52, .56)
    assert not e.movement_active
    assert e.orders
    assert e.execution_counts['movement_resumes'] == 1


def test_repeated_short_shocks_extend_cooldown():
    e = engine()
    refresh(e, T+1, .52, .56)
    for i in range(2, 22):
        refresh(e, T+i, .52 if i % 2 else .48, .56 if i % 2 else .52)
    assert e.movement_active == {'c'}
    assert e.movement_until['c'] > T+21
    assert not e.orders


def test_gap_cannot_count_as_stable_market_time():
    e = engine()
    refresh(e, T+1, .52, .56)
    e.gap(T+2)
    refresh(e, T+20, .52, .56)
    assert e.movement_active == {'c'} and not e.orders
    for i in range(21, 26):
        refresh(e, T+i, .52, .56)
    assert not e.movement_active and e.orders
    assert e.report()['execution_quality'] == 'INCOMPLETE_DATA'


def test_reconnect_preserves_movement_pause():
    e = engine()
    refresh(e, T+1, .52, .56)
    e.gap(T+2)
    e.advance(T+3)
    e.select(M, T+3)
    refresh(e, T+3, .52, .56)
    assert e.movement_active == {'c'} and not e.orders


def test_unchanged_quote_keeps_queue_and_original_arrival():
    e = engine()
    refresh(e, T+1)
    e.frame(trade('u', T+1.1, 5), T+1.1)
    o = e.orders['u']
    queue, arrival, anchor = o.queue, o.active_at, o.queue_at
    for i in range(2, 12):
        refresh(e, T+i)
    assert e.orders['u'] is o
    assert o.cancel_at is None and o.queue == queue
    assert (o.active_at, o.queue_at) == (arrival, anchor)
    assert e.execution_counts['order_retained'] == 2
    e.frame(trade('u', T+11.1, queue+2), T+11.1)
    assert e.positions['u'].size == 2


def test_changed_quote_cancels_with_latency_without_resetting_queue():
    e = engine()
    refresh(e, T+1)
    o = e.orders['u']
    for i in range(2, 11):
        refresh(e, T+i, .49, .53)
    assert o.cancel_at == pytest.approx(T+10.5)
    assert e.orders['u'] is o
    assert o.queue == 15


def test_no_future_queue_snapshot_used_for_an_earlier_trade():
    e = engine()
    e.frame(book('u', T+2), T+2)
    o = e.orders['u']
    assert o.queue_at == T+2
    e.frame(trade('u', T+1, 100), T+2.1)
    assert e.fills == 0 and o.queue == 15
    assert e.trade_blocks['before_queue_or_stale'] == 1


def test_trade_watermark_rejects_older_print_without_queue_consumption():
    e = engine()
    refresh(e, T+1)
    e.frame(trade('u', T+2, 5), T+2)
    q = e.orders['u'].queue
    e.frame(trade('u', T+1.5, 100), T+2.1)
    assert e.fills == 0 and e.orders['u'].queue == q
    assert e.trade_blocks['out_of_order_trade'] == 1


def test_full_residual_can_requote_only_missing_side_and_merge():
    e = engine()
    e.frame(trade('u', T+1, 20), T+1)
    e.cancel_all()
    e.advance(T+2)
    refresh(e, T+2)
    assert set(e.orders) == {'d'}
    assert e.orders['d'].remaining == 5
    assert e.orders['d'].price + e.positions['u'].cost/5 <= .98 + 1e-8
    e.frame(trade('d', T+3, 20), T+3)
    e.advance(T+6)
    assert e.fills == 2 and e.merges == 1
    assert e.cash == pytest.approx(50.2)
    assert e.cash-50 == pytest.approx(e.merge_pnl+e.residual_pnl)


def test_residual_requote_remains_subject_to_inventory_cap():
    e = engine(max_inventory_usd=4.8)
    e.frame(trade('u', T+1, 20), T+1)
    e.cancel_all()
    e.advance(T+2)
    e.positions['u'].cost = 4.0
    refresh(e, T+2)
    # Complement ceiling .18 * 5 plus held $4 = $4.90 > cap $4.80.
    assert not e.orders and e.quote_blocks['inventory_cap'] > 0


def test_residual_quote_accounts_for_entry_fee_and_merge_cost():
    e = engine(maker_fee_bps=100, merge_cost_usd=.01)
    e.frame(trade('u', T+1, 20), T+1)
    e.cancel_all(); e.advance(T+2)
    refresh(e, T+2)
    o = e.orders['d']
    total = e.positions['u'].cost + o.price*o.remaining*1.01 + .01
    assert 5-total >= 5*e.cfg.min_pair_edge - 1e-8
    e.frame(trade('d', T+3, 20), T+3)
    e.advance(T+6)
    assert e.cash-50 == pytest.approx(e.merge_pnl)
    assert e.fees > 0 and e.merge_costs == .01


def test_small_residual_never_rounded_up_into_directional_exposure():
    e = engine()
    e.frame(trade('u', T+1, 17), T+1)
    e.cancel_all(); e.advance(T+2)
    refresh(e, T+2)
    assert not e.orders
    assert e.positions['u'].size == 2
    assert e.quote_blocks['residual_below_minimum'] > 0


def test_residual_timeout_never_cleared_by_movement_recovery():
    e = engine()
    e.frame(trade('u', T+1, 20), T+1)
    e.advance(T+2)
    for i in range(3, 24):
        refresh(e, T+i)
    assert 'c' in e.blocked
    e.advance(T+24)
    refresh(e, T+24)
    assert not e.orders and e.positions['u'].size == 5
    assert e.quote_blocks['market_blocked'] > 0


def test_execution_diagnostics_partition_quote_checks():
    e = engine()
    e.frame(trade('u', T+1, 5, 'BUY'), T+1)
    d = e.report()['execution_diagnostics']
    assert sum(d['quote_blocks'].values()) + d['counts']['quote_cycles'] == d['quote_checks']
    assert sum(d['trade_blocks'].values()) + e.fills == d['trade_checks']


def test_resume_refuses_old_model_and_current_journal_replays_exactly(tmp_path):
    j = Journal(tmp_path/'old')
    j.write('header', 0, schema=1, config=asdict(Config()), source='PUBLIC_LIVE')
    j.close()
    with pytest.raises(ValueError, match='different execution model'):
        load_engine(tmp_path/'old', require_current_model=True)
    e = Engine(Config()); e.source='PUBLIC_LIVE'
    j = Journal(tmp_path/'new')
    j.write('header', 0, schema=1, model_version=MODEL_VERSION, mode='paper', config=asdict(e.cfg), source=e.source)
    rows = [{'type':'market','ts':T,'market':M.data()},
            {'type':'frame','ts':T,'message':book('u',T)},
            {'type':'frame','ts':T,'message':book('d',T)},
            {'type':'frame','ts':T+1,'message':trade('u',T+1,20)},
            {'type':'frame','ts':T+2,'message':trade('d',T+2,20)},
            {'type':'stop','ts':T+3}, {'type':'tick','ts':T+6}]
    for row in rows:
        apply(e,row)
        entry=dict(row);j.write(entry.pop('type'),entry.pop('ts'),**entry)
    j.close()
    assert load_engine(tmp_path/'new', require_current_model=True).report() == e.report()


def test_empty_old_markets_do_not_add_work_to_per_frame_accounting():
    e = Engine(Config())
    for i in range(100):
        start = T + i*300
        m = Market(f'btc-updown-5m-{int(start)}', str(i), f'u{i}', f'd{i}', start, start+300, .01, 5)
        e.select(m, start)
    class Counting(dict):
        reads = 0
        def __getitem__(self, key):
            self.reads += 1
            return super().__getitem__(key)
    e.positions = Counting(e.positions)
    e.advance(T+99*300+1)
    assert e.positions.reads == 0
    assert e.equity() == 50
    # Even direct position changes update the index, so accounting cannot hide
    # old-market residuals by optimizing away empty-market visits.
    e.positions['u0'].size = 5
    e.positions['u0'].cost = 2.4
    e.cash = 47.6
    assert e.exposed_markets == {'0'}
    assert e.equity() == pytest.approx(47.6)
    e.settle('0', 'u0', T+99*300+2)
    assert not e.exposed_markets
    assert e.cash == pytest.approx(52.6)


@pytest.mark.parametrize('name', ['movement_cooldown_seconds','movement_stability_seconds'])
def test_recovery_config_requires_positive_duration(name):
    with pytest.raises(ValueError):
        Config(**{name:0})
