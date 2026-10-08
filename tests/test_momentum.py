"""Frozen-v1 boundary and safety checks; synthetic inputs, no performance claims."""
import json
from pathlib import Path
import pytest
from pairbot.momentum import (FROZEN, load_config, side_for, quote, Reference,
    Failures, Account, blank_row, fresh_book, TOPIC, BTC_FILTER,
    subscription_payload, record_execution_result)

C = FROZEN


def book(price=.5, size=100, timestamp=481000):
    return dict(asset_id='1', timestamp=str(timestamp), min_order_size='5',
                bids=[dict(price='.1', size='100')],
                asks=[dict(price=str(price), size=str(size))])


@pytest.mark.parametrize('delta,side', [(80,'Up'),(-80,'Down'),(79.999,None),
    (-79.999,None),(0,None),(1000,'Up'),(-1000,'Down')])
def test_threshold_direction_only(delta, side):
    assert side_for(delta, C) == side


def test_config_frozen_and_read_only(tmp_path):
    p = tmp_path/'config.yaml'
    p.write_text(Path('config.yaml').read_text())
    loaded = load_config(p)
    with pytest.raises(TypeError):
        loaded['stake_usd'] = 10
    p.write_text(p.read_text().replace('min_move_usd: 80', 'min_move_usd: 79'))
    with pytest.raises(ValueError):
        load_config(p)


def test_live_even_double_confirmation_not_available(tmp_path, monkeypatch):
    monkeypatch.setenv('CONFIRM_LIVE','YES')
    p = tmp_path/'config.yaml'
    p.write_text(Path('config.yaml').read_text().replace('live: false', 'live: true'))
    with pytest.raises(ValueError):
        load_config(p)


def test_fee_inclusive_budget():
    q = quote(book(), '1', .07, C)
    assert 4.9999 < q['cost'] <= 5
    assert q['fee'] > 0 and q['principal'] < 5
    assert q['vwap'] == .5


@pytest.mark.parametrize('price', [.91,.99])
def test_vwap_above_cap(price):
    with pytest.raises(ValueError, match='vwap_above'):
        quote(book(price), '1', .07, C)


def test_exact_cap_allowed():
    assert quote(book(.90), '1', .07, C)['vwap'] <= .90


def test_vwap_not_per_level_cap():
    b = book(.8, 5)
    b['asks'].append(dict(price='.95', size='100'))
    q = quote(b, '1', .07, C)
    assert q['vwap'] < .90
    assert q['levels'][-1]['price'] == .95


def test_insufficient_depth_never_partial():
    with pytest.raises(ValueError, match='no_fill'):
        quote(book(.5, 1), '1', .07, C)


def test_min_shares_enforced():
    b = book()
    b['min_order_size'] = '20'
    with pytest.raises(ValueError, match='no_fill'):
        quote(b, '1', .07, C)


def test_book_exchange_age_not_just_response_age():
    row = dict(data=book(timestamp=479000), received_at=482, rtt=.1)
    with pytest.raises(ValueError, match='stale_book'):
        fresh_book(row, '1', 482, 482, C)
    row['data']['timestamp'] = '480000'
    fresh_book(row, '1', 482, 482, C)


class Sink:
    def write(self, row):
        pass


def message(ts, price, topic=TOPIC, kind='update'):
    return dict(topic=topic, type=kind, payload=dict(symbol='btc/usd',
        window_s=60, timestamp=ts*1000, value=price))


def ref():
    r = Reference(Sink(), Failures())
    r.generation = 1
    r.connected = True
    r.ingest(message(300, 80000), 301)
    r.ingest(message(480, 80080), 481)
    return r


def test_matched_twap_open_and_entry():
    a,b = ref().prices(300, 481, 481, C)
    assert b['price']-a['price'] == 80


def test_spot_and_history_cannot_backfill_opening():
    r = Reference(Sink(), Failures())
    assert not r.ingest(message(300,80000,topic='crypto_prices_chainlink'),301)
    assert not r.ingest(message(300,80000,kind='snapshot'),301)
    assert not r.series


def test_rtds_subscription_filters_btc_only_as_compact_json_string():
    payload = subscription_payload()
    assert payload['action'] == 'subscribe'
    assert len(payload['subscriptions']) == 1
    sub = payload['subscriptions'][0]
    assert sub['topic'] == TOPIC and sub['type'] == 'update'
    assert sub['filters'] == BTC_FILTER == '{"symbol":"btc/usd"}'
    assert json.loads(sub['filters']) == {'symbol':'btc/usd'}
    encoded = json.dumps(payload, separators=(',', ':'))
    assert '"filters":"{\\"symbol\\":\\"btc/usd\\"}"' in encoded


def test_decision_waits_inside_existing_tolerance_for_fresh_reference(monkeypatch):
    import asyncio
    from pairbot import momentum as m

    class CaptureSink:
        def __init__(self):
            self.rows = []
        def write(self, row):
            self.rows.append(row)

    sink = CaptureSink()
    r = Reference(sink, Failures())
    r.generation = 1
    r.connected = True
    r.ingest(message(300, 80000), 301)
    r.ingest(message(478, 80070), 478.2)
    wall = [480.2]
    real_sleep = asyncio.sleep

    async def sleep(seconds):
        assert seconds == pytest.approx(.05)
        wall[0] += .1
        r.ingest(message(480, 80080), wall[0])
        await real_sleep(0)

    monkeypatch.setattr(m.asyncio, 'sleep', sleep)
    opening, latest = asyncio.run(r.decision_prices(
        300, lambda:(wall[0], 0), lambda:wall[0], C))
    assert opening['price'] == 80000 and latest['price'] == 80080
    assert C['max_stale_data_sec'] == 2
    assert any(row.get('type') == 'reference_wait' for row in sink.rows)


def test_stale_reference_is_window_skip_not_execution_kill_switch():
    f = Failures()
    for _ in range(10):
        record_execution_result(f, 'stale_reference')
    assert f.counts['execution'] == 0 and not f.halted
    for _ in range(3):
        record_execution_result(f, 'stale_book')
    assert f.halted


def test_late_opening_rejected():
    r = ref()
    r.series[300]['received_at'] = 303
    with pytest.raises(ValueError, match='missing_causal'):
        r.prices(300,481,481,C)


def test_no_cross_generation_or_stale_reference():
    r = ref()
    with pytest.raises(ValueError, match='stale_reference'):
        r.prices(300,483,483,C)
    r.generation += 1
    with pytest.raises(ValueError, match='ambiguous_reference'):
        r.prices(300,481,481,C)


def test_reference_revision_blocks():
    r = ref()
    with pytest.raises(ValueError):
        r.ingest(message(300,80001),302)
    with pytest.raises(ValueError, match='ambiguous_reference'):
        r.prices(300,481,481,C)


def test_three_consecutive_failure_latch():
    f = Failures()
    for _ in range(3):
        f.fail('websocket')
        f.success('api')
    assert f.halted
    f.success('websocket')
    assert f.halted  # Requires manual new session.


def test_success_breaks_same_channel_streak():
    f = Failures()
    f.fail('api'); f.fail('api'); f.success('api'); f.fail('api')
    assert not f.halted


def lose(account, start):
    account.enter(start, {}, 'Up', dict(cost=5, shares=10), blank_row(start), start)
    account.settle('Down', start+300)


def test_daily_realized_loss_reset_utc():
    a = Account(300)
    lose(a,300); lose(a,600)
    assert a.cash == 40 and a.entry_block(1000) == 'daily_loss_limit'
    assert a.entry_block(86400) is None
    assert a.daily_pnl == 0


def test_hard_stop_survives_utc_day():
    a = Account(300)
    a.cash = 35
    lose(a,300)
    assert a.hard_halt
    assert a.entry_block(86400) == 'hard_stop_balance_30'


def test_open_position_duplicate_and_fee_pnl():
    a = Account(300)
    q = quote(book(), '1', .07, C)
    a.enter(300, {}, 'Up', q, blank_row(300), 480)
    with pytest.raises(ValueError, match='open_position'):
        a.enter(600, {}, 'Down', q, blank_row(600), 700)
    p = a.settle('Up', 610)
    assert p['pnl'] == pytest.approx(q['shares']-q['cost'])
    with pytest.raises(ValueError, match='duplicate'):
        a.enter(300, {}, 'Up', q, blank_row(300), 700)


def test_locked_cash_not_realized_balance_hard_stop():
    a = Account(300)
    a.cash = 34
    a.enter(300, {}, 'Up', dict(cost=5, shares=10), blank_row(300), 480)
    assert a.entry_block(481) == 'open_position_limit'
    assert not a.hard_halt


@pytest.mark.parametrize('post_delay_price,filled,reason', [(.8, True, ''),
    (.95, False, 'vwap_above_0.90')])
def test_complete_session_uses_after_delay_book_once(monkeypatch, tmp_path, post_delay_price, filled, reason):
    import asyncio
    import datetime as dt
    from types import SimpleNamespace
    from pairbot import momentum as m
    from pairbot.directional import read_journal
    wall = [479.]
    real_sleep = asyncio.sleep
    delays = []
    calls = []

    async def sleep(seconds):
        delays.append(seconds)
        wall[0] += seconds
        await real_sleep(0)

    class Ref:
        def __init__(self, *args):
            pass
        async def run(self):
            await real_sleep(3600)
        def prices(self, *args):
            return dict(price=80000), dict(price=80080)
        async def decision_prices(self, *args):
            return self.prices()

    condition = '0x'+'1'*64
    raw = dict(slug='btc-updown-5m-300', closed=False, enableOrderBook=True,
        endDate=dt.datetime.fromtimestamp(600,dt.timezone.utc).isoformat(),
        conditionId=condition, outcomes=['Up','Down'], clobTokenIds=['1','2'],
        description=m.RULE_TEXT, resolutionSource=m.SOURCE,
        cryptoMarketConfig=dict(twapLookbackSeconds=60), feesEnabled=True,
        feeSchedule=dict(rate=.07,exponent=1,takerOnly=True))

    class Response:
        def __init__(self, data):
            self.data = data
        def raise_for_status(self):
            pass
        def json(self):
            return self.data

    class Client:
        def __init__(self, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def get(self, url, params=None):
            calls.append((url, params, wall[0]))
            if url.endswith('/time'):
                return Response(int(wall[0]))
            if url.endswith('/events'):
                return Response([dict(slug=raw['slug'],markets=[raw])])
            if '/clob-markets/' in url:
                return Response(dict(fd=dict(r=.07,e=1,to=True),t=[dict(t='1'),dict(t='2')]))
            if url.endswith('/book'):
                n = sum(u.endswith('/book') for u,_,_ in calls)
                return Response(book(.5 if n == 1 else post_delay_price, timestamp=int(wall[0]*1000)))
            if url.endswith('/v2/resolutions'):
                return Response(dict(data=[dict(condition_id=condition,status='resolved',
                    resolved_at=dt.datetime.fromtimestamp(600,dt.timezone.utc).isoformat(),
                    resolved_block=1,payouts=[1,0])]))
            raise AssertionError(url)

    monkeypatch.setattr(m, 'Reference', Ref)
    monkeypatch.setattr(m, 'time', SimpleNamespace(time=lambda:wall[0],monotonic=lambda:wall[0]))
    monkeypatch.setattr(m.asyncio, 'sleep', sleep)
    monkeypatch.setattr(m.httpx, 'AsyncClient', Client)
    out = tmp_path/'segment1'
    result = asyncio.run(m.session('config.yaml', out, 603))
    rows = json.loads((out/'state.json').read_text())['rows']
    assert len(rows) == 1 and rows[0]['filled'] is filled
    assert rows[0]['skip_reason'] == reason
    books = [(p,t) for u,p,t in calls if u.endswith('/book')]
    assert len(books) == 2 and books[1][1]-books[0][1] >= 2
    assert all(p['token_id'] == '1' for p,_ in books)
    assert delays.count(2) == 1
    assert result['actual_orders'] == 0
    list(read_journal(out/'journal.jsonl'))
    if filled:
        assert rows[0]['vwap_fill'] == pytest.approx(.8)
        assert result['settled_trades'] == 1
        cash = result['cash']
        # Deadline resumes preserve capital/results, never reset to $50.
        resumed = asyncio.run(m.session('config.yaml', tmp_path/'segment2', 604, out))
        assert resumed['cash'] == cash and resumed['trades'] == 1
