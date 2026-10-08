"""Mandatory Decision v3 FINAL PAPER engine.

Public data only. No wallet, signing, private keys, or order submission.
Each BTC 5-minute window is scanned at several causal checkpoints. At most one
UP/DOWN PAPER trade may be opened per window. The fixed PAPER stake is $5
including fees with a $100 starting bankroll. Direct or documented
complementary CLOB liquidity may be used; missing liquidity is never invented.
"""
import argparse
import asyncio
import csv
import datetime as dt
import hashlib
import json
import math
import time
from pathlib import Path
from types import MappingProxyType

import httpx

from .directional import Journal
from .five_minute_live import parse_contract, final_winner
from .daily_readiness import fee_match
from .unified_paper import RULE_TEXT
from .momentum import (
    Reference as V1Reference, Failures, number, utc_day,
    TOPIC, SOURCE, BTC_FILTER,
)

FROZEN_V3 = MappingProxyType(dict(
    candidate_entry_times_before_close_sec=[150, 120, 90, 60, 45],
    entry_tolerance_sec=5,
    starting_capital_usd=100,
    stake_usd=5,
    max_stale_data_sec=2,
    execution_delay_sec=2,
    min_core_strength=0.20,
    price_cap_floor=0.57,
    price_cap_ceiling=0.78,
    max_adverse_slippage=0.04,
    live=False,
))
WEIGHTS = MappingProxyType(dict(
    direction=40,
    recent=20,
    persistence=15,
    order_book=15,
    market_pricing=10,
))
FIELDS = (
    'window_id ts_open candidate_before_close btc_open btc_at_entry delta side '
    'total_score confidence core_score core_strength signal_agreement price_cap '
    'direction_score recent_score persistence_score order_book_score pricing_score '
    'best_ask vwap_fill effective_entry execution_route filled skip_reason '
    'stake fee live_minimum_met outcome pnl balance_after'
).split()

OPENING_CAPTURE_GRACE_SEC = 5
METADATA_RETRY_SEC = 5
DEFAULT_SETTLEMENT_GRACE_SEC = 60
RUN_WARMUP_SEC = 15
CLOB_TIME_BUCKET_SEC = 1.0


def next_full_window_start(now, warmup_sec=RUN_WARMUP_SEC):
    """Return a future 5m boundary with enough time to capture its opening live."""
    now, warmup_sec = number(now), number(warmup_sec)
    if warmup_sec < 0:
        raise ValueError('invalid_warmup')
    return int(math.ceil((now + warmup_sec) / 300) * 300)


def clob_clock_sample(server_second, sent_at, received_at):
    """Center CLOB /time's integer-second bucket and return offset uncertainty."""
    server_second = number(server_second)
    sent_at, received_at = number(sent_at), number(received_at)
    if received_at < sent_at:
        raise ValueError('ambiguous_clock')
    rtt = received_at - sent_at
    midpoint = (sent_at + received_at) / 2
    server_center = server_second + CLOB_TIME_BUCKET_SEC / 2
    return dict(
        offset=server_center - midpoint,
        uncertainty=rtt / 2 + CLOB_TIME_BUCKET_SEC / 2,
    )


def best_ask_or_none(book):
    asks = book.get('asks') or []
    if not asks:
        return None
    values = [number(level['price']) for level in asks]
    if not all(0 < price < 1 for price in values):
        raise ValueError('ambiguous_book')
    return min(values)


def fresh_book_v2(row, token, server, uncertainty, wall, config):
    """Validate CLOB freshness while honoring measured /time uncertainty.

    The stale upper bound remains <=2s. A book may appear slightly ahead of
    /time only within the already measured clock uncertainty.
    """
    book = row['data']
    timestamp = number(book['timestamp']) / 1000
    server, uncertainty, wall = number(server), number(uncertainty), number(wall)
    if book.get('asset_id') != token:
        raise ValueError('ambiguous_book_token')
    age = server - timestamp
    response_age = wall - number(row['received_at'])
    rtt = number(row['rtt'])
    if not (-uncertainty <= age <= config['max_stale_data_sec']
            and 0 <= response_age <= config['max_stale_data_sec']
            and 0 <= rtt <= config['max_stale_data_sec']):
        raise ValueError('stale_book')


def load_config(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.split('#', 1)[0].strip()
        if not line:
            continue
        key, value = line.split(':', 1)
        key, value = key.strip(), value.strip()
        if key in values or key not in (*FROZEN_V3, 'fixed_time_delta_direction', 'experimental_max_entry_price', 'experimental_stop_after_consecutive_losses', 'experimental_reconnect_websocket'):
            raise ValueError('Unknown or duplicate v2 config field')
        values[key] = json.loads(value)
    if values.get('fixed_time_delta_direction') is True:
        expected = dict(FROZEN_V3)
        expected.update(candidate_entry_times_before_close_sec=[75], starting_capital_usd=50,
                        fixed_time_delta_direction=True)
        if values.get('experimental_max_entry_price') == 0.70:
            expected.update(candidate_entry_times_before_close_sec=[75, 60, 45, 30],
                            experimental_max_entry_price=0.70)
            if values.get('experimental_stop_after_consecutive_losses') == 5 and values.get('experimental_reconnect_websocket') is True:
                expected.update(experimental_stop_after_consecutive_losses=5,
                                experimental_reconnect_websocket=True)
    else:
        expected = dict(FROZEN_V3)
    if values != expected:
        raise ValueError('Unsupported config; PAPER only')
    return MappingProxyType(values)


def clamp(value, low=-1.0, high=1.0):
    return min(high, max(low, number(value)))


class V3Reference(V1Reference):
    """Keep causally observed window history across transparent RTDS reconnects.

    The opening tick remains valid if it was received live within a bounded five-second
    capture grace and was never revised. Only the latest decision tick
    must belong to the currently connected generation and satisfy the frozen
    <=2s freshness requirement.
    """

    def prices(self, start, server, wall, config):
        opening = self.series.get(start)
        if (not opening
                or not start <= opening['received_at'] <= start + OPENING_CAPTURE_GRACE_SEC
                or start in self.invalid):
            raise ValueError('missing_causal_matched_opening')

        available = [
            row for ts, row in self.series.items()
            if ts <= server and row['received_at'] <= wall and ts not in self.invalid
        ]
        if not available:
            raise ValueError('missing_reference')
        latest = max(available, key=lambda row: row['timestamp'])

        if latest['timestamp'] in self.invalid:
            raise ValueError('ambiguous_reference_revision')
        if not self.connected or latest['generation'] != self.generation:
            raise ValueError('reference_reconnecting')
        if not (0 <= server - latest['timestamp'] <= config['max_stale_data_sec']
                and 0 <= wall - latest['received_at'] <= config['max_stale_data_sec']):
            raise ValueError('stale_reference')
        return opening, latest

    async def decision_prices_at(self, start, before_close, clock_fn, wall_fn, config):
        """Wait only inside one candidate's ± tolerance for a fresh causal tick."""
        target = start + 300 - before_close
        waited_reason = None
        while True:
            if self.failures.halted:
                raise ValueError('three_consecutive_failures')
            server, uncertainty = clock_fn()
            if abs(server - target) + uncertainty > config['entry_tolerance_sec']:
                raise ValueError(waited_reason or 'missed_candidate_time')
            try:
                return self.prices(start, server, wall_fn(), config)
            except ValueError as exc:
                reason = str(exc)
                if reason not in ('stale_reference', 'reference_reconnecting'):
                    raise
                if waited_reason is None:
                    waited_reason = reason
                    self.journal.write({
                        'type':'reference_wait', 'start':start, 'at':wall_fn(),
                        'candidate_before_close':before_close, 'reason':reason,
                        'deadline':target + config['entry_tolerance_sec'] - uncertainty,
                    })
                await asyncio.sleep(.05)


def _samples(reference, start, latest):
    invalid = getattr(reference, 'invalid', set())
    rows = [
        row for ts, row in reference.series.items()
        if start <= ts <= latest['timestamp'] and ts not in invalid
    ]
    rows.sort(key=lambda row: row['timestamp'])
    if len(rows) < 2:
        raise ValueError('insufficient_reference_history')
    return rows


def _book_levels(book):
    bids = sorted(
        [(number(x['price']), number(x['size'])) for x in book.get('bids', [])],
        key=lambda x: x[0], reverse=True)
    asks = sorted(
        [(number(x['price']), number(x['size'])) for x in book.get('asks', [])],
        key=lambda x: x[0])
    if not bids and not asks:
        raise ValueError('ambiguous_book')
    for price, size in bids + asks:
        if not (0 < price < 1) or size <= 0:
            raise ValueError('ambiguous_book')
    if bids and asks and bids[0][0] >= asks[0][0]:
        raise ValueError('ambiguous_book')
    return bids, asks


def _two_sided_metrics(bids, asks):
    if not bids or not asks:
        return None
    midpoint = (bids[0][0] + asks[0][0]) / 2
    bid_notional = sum(price * size for price, size in bids[:3])
    ask_notional = sum(price * size for price, size in asks[:3])
    total = bid_notional + ask_notional
    pressure = 0.0 if total <= 0 else (bid_notional - ask_notional) / total
    return dict(mid=midpoint, pressure=clamp(pressure))


def _pair_book_metrics(up_book, down_book):
    """Use one-sided books without inventing a missing quote."""
    up_bids, up_asks = _book_levels(up_book)
    down_bids, down_asks = _book_levels(down_book)
    up = _two_sided_metrics(up_bids, up_asks)
    down = _two_sided_metrics(down_bids, down_asks)

    if up and down:
        up_mid, down_mid = up['mid'], down['mid']
        pressure_diff = clamp(up['pressure'] - down['pressure'])
    elif up:
        up_mid, down_mid = up['mid'], 1 - up['mid']
        pressure_diff = 0.0
    elif down:
        down_mid, up_mid = down['mid'], 1 - down['mid']
        pressure_diff = 0.0
    else:
        up_mid = down_mid = 0.5
        pressure_diff = 0.0

    return dict(up_mid=up_mid, down_mid=down_mid,
                pressure_diff=pressure_diff,
                up_has_ask=bool(up_asks), down_has_ask=bool(down_asks))


def price_cap_for_strength(strength, config):
    strength = clamp(strength, 0.0, 1.0)
    floor = number(config['price_cap_floor'])
    ceiling = number(config['price_cap_ceiling'])
    if not 0 < floor <= ceiling < 1:
        raise ValueError('ambiguous_price_cap')
    return floor + (ceiling - floor) * strength


def score_decision(reference, start, opening, latest, up_book, down_book, config):
    """BTC core signal chooses direction; market data confirms but cannot flip it."""
    rows = _samples(reference, start, latest)
    prices = [number(row['price']) for row in rows]
    open_price = number(opening['price'])
    current = number(latest['price'])
    delta = current - open_price

    span = max(prices) - min(prices)
    direction_score = WEIGHTS['direction'] * clamp(delta / max(span, 1.0))

    cutoff = latest['timestamp'] - 60
    recent = [row for row in rows if row['timestamp'] >= cutoff]
    if len(recent) < 2:
        recent = rows[-2:]
    recent_prices = [number(row['price']) for row in recent]
    recent_delta = recent_prices[-1] - recent_prices[0]
    recent_span = max(recent_prices) - min(recent_prices)
    recent_score = WEIGHTS['recent'] * clamp(
        recent_delta / max(recent_span, 1.0))

    signs = [1 if price > open_price else -1 if price < open_price else 0
             for price in prices[1:]]
    persistence_score = WEIGHTS['persistence'] * (
        sum(signs) / len(signs) if signs else 0.0)

    market = _pair_book_metrics(up_book, down_book)
    order_book_score = (WEIGHTS['order_book'] / 2) * market['pressure_diff']
    pricing_score = WEIGHTS['market_pricing'] * clamp(
        market['up_mid'] - market['down_mid'])

    core_score = direction_score + recent_score + persistence_score
    core_max = WEIGHTS['direction'] + WEIGHTS['recent'] + WEIGHTS['persistence']
    core_strength = abs(core_score) / core_max if core_max else 0.0

    if core_score > 1e-12:
        side, sign = 'Up', 1
    elif core_score < -1e-12:
        side, sign = 'Down', -1
    elif delta > 0 or (delta == 0 and recent_delta > 0):
        side, sign = 'Up', 1
    elif delta < 0 or recent_delta < 0:
        side, sign = 'Down', -1
    else:
        side, sign = 'Up', 1

    components = (direction_score, recent_score, persistence_score)
    signal_agreement = sum(
        1 for value in components
        if (value > 0 and sign > 0) or (value < 0 and sign < 0)
    ) / len(components)

    parts = dict(
        direction_score=direction_score,
        recent_score=recent_score,
        persistence_score=persistence_score,
        order_book_score=order_book_score,
        pricing_score=pricing_score,
    )
    total = max(-100.0, min(100.0, sum(parts.values())))
    return dict(
        **parts, total_score=total, confidence=core_strength,
        core_score=core_score, core_strength=core_strength,
        signal_agreement=signal_agreement,
        price_cap=price_cap_for_strength(core_strength, config),
        side=side, delta=delta, recent_delta=recent_delta,
        up_mid=market['up_mid'], down_mid=market['down_mid'],
        pressure_diff=market['pressure_diff'],
        up_has_ask=market['up_has_ask'],
        down_has_ask=market['down_has_ask'],
    )


def validate_signal(score, config):
    if score['core_strength'] < config['min_core_strength']:
        raise ValueError('weak_core_signal')
    if score['signal_agreement'] < (2 / 3):
        raise ValueError('signal_disagreement')


def validate_quote(quote, score, config, decision_effective=None):
    effective = number(quote['effective_entry'])
    if effective > score['price_cap'] + 1e-12:
        raise ValueError('price_above_cap')
    if (decision_effective is not None
            and effective - number(decision_effective)
            > config['max_adverse_slippage'] + 1e-12):
        raise ValueError('adverse_slippage')


def micro_quote(book, token, rate, config):
    """PAPER fill from real displayed asks only, including entry fees."""
    rate = number(rate)
    if not 0 < rate <= 1 or book.get('asset_id') != token:
        raise ValueError('ambiguous_book_or_fee')
    if not book.get('asks'):
        raise ValueError('no_executable_ask')
    remaining = config['stake_usd']
    shares = principal = fee_total = 0.0
    levels = []
    for level in sorted(book['asks'], key=lambda x: number(x['price'])):
        price, size = number(level['price']), number(level['size'])
        if not (0 < price < 1) or size <= 0:
            raise ValueError('ambiguous_book_or_fee')
        quantity = math.floor(min(
            size, remaining / (price + rate * price * (1 - price))) * 1e6) / 1e6
        fee = math.ceil(quantity * rate * price * (1 - price) * 1e5) / 1e5
        if quantity * price + fee > remaining:
            quantity = max(0.0, math.floor(
                (remaining - 1e-5) / (price + rate * price * (1 - price)) * 1e6) / 1e6)
            fee = math.ceil(quantity * rate * price * (1 - price) * 1e5) / 1e5
        if quantity <= 0:
            continue
        cost = quantity * price + fee
        if cost > remaining + 1e-10:
            raise ValueError('ambiguous_fee_rounding')
        remaining -= cost
        shares += quantity
        principal += quantity * price
        fee_total += fee
        levels.append({'price': price, 'shares': quantity, 'fee': fee})
        if remaining < .0001:
            break
    if remaining >= .0001 or shares <= 0:
        raise ValueError('no_fill')
    minimum = number(book.get('min_order_size', 5))
    return dict(shares=shares, principal=principal, fee=fee_total,
                cost=principal + fee_total, vwap=principal / shares,
                levels=levels, min_order_size=minimum,
                live_minimum_met=shares >= minimum)


def paper_quote_pair(selected_book, opposite_book, selected_token,
                     opposite_token, rate, config):
    """Prefer a direct ask; otherwise use a complementary opposite bid."""
    if selected_book.get('asset_id') != selected_token:
        raise ValueError('ambiguous_book_token')
    if opposite_book.get('asset_id') != opposite_token:
        raise ValueError('ambiguous_book_token')

    if selected_book.get('asks'):
        quote = micro_quote(selected_book, selected_token, rate, config)
        quote['execution_route'] = 'direct_ask'
    else:
        opposite_bids = opposite_book.get('bids') or []
        if not opposite_bids:
            raise ValueError('no_executable_ask')
        synthetic_asks = []
        for level in opposite_bids:
            price = 1 - number(level['price'])
            size = number(level['size'])
            if not (0 < price < 1) or size <= 0:
                raise ValueError('ambiguous_book_or_fee')
            synthetic_asks.append({'price': price, 'size': size})
        synthetic = {
            'asset_id': selected_token,
            'timestamp': selected_book.get('timestamp'),
            'min_order_size': selected_book.get(
                'min_order_size', opposite_book.get('min_order_size', 5)),
            'bids': [],
            'asks': synthetic_asks,
        }
        quote = micro_quote(synthetic, selected_token, rate, config)
        quote['execution_route'] = 'complementary_bid'

    quote['effective_entry'] = quote['cost'] / quote['shares']
    return quote


def record_execution_result(failures, reason):
    if reason in ('api_failure', 'three_consecutive_failures',
                  'daily_loss_limit', 'hard_stop_balance_30',
                  'balance_below_stake'):
        return
    if reason.startswith('ambiguous_'):
        failures.fail('execution')
    elif reason in (
            'stale_book', 'stale_reference', 'reference_reconnecting',
            'missing_reference', 'missing_causal_matched_opening',
            'insufficient_reference_history', 'no_fill',
            'no_executable_ask', 'open_position_limit',
            'metadata_unavailable', 'missed_candidate_time',
            'entry_deadline_passed', 'weak_core_signal',
            'signal_disagreement', 'price_above_cap', 'adverse_slippage'):
        failures.success('execution')
    else:
        failures.fail('execution')


class RecoveringFeedFailures(Failures):
    """Reconnect WS instead of halting after transient feed failures.

    A disconnected feed never supplies eligible prices; existing stale-data
    and causal opening guards still reject entries until fresh ticks return.
    API and execution failure safeguards remain unchanged.
    """
    def fail(self, channel):
        if channel == 'websocket':
            self.counts[channel] += 1
            return
        super().fail(channel)


class Account:
    def __init__(self, now, starting_capital=100.0, stake_usd=5.0, loss_streak_limit=None):
        self.starting_capital = number(starting_capital)
        self.stake_usd = number(stake_usd)
        if self.starting_capital <= 0 or self.stake_usd <= 0:
            raise ValueError('ambiguous_account_config')
        self.cash = self.starting_capital
        self.position = None
        self.daily_pnl = 0.0
        self.day = utc_day(now)
        self.hard_halt = False
        self.traded = set()
        self.trades = []
        self.loss_streak = 0
        self.loss_streak_limit = loss_streak_limit

    def entry_block(self, now):
        if utc_day(now) != self.day:
            self.day, self.daily_pnl = utc_day(now), 0.0
        if self.hard_halt:
            return 'hard_stop_balance_30'
        if self.loss_streak_limit is not None:
            if self.loss_streak >= self.loss_streak_limit:
                return 'five_consecutive_losses'
        elif self.daily_pnl - self.stake_usd < -10 - 1e-9:
            return 'daily_loss_limit'
        if self.position:
            return 'open_position_limit'
        if self.cash <= 30 and self.loss_streak_limit is None:
            self.hard_halt = True
            return 'hard_stop_balance_30'
        if self.cash < self.stake_usd:
            return 'balance_below_stake'
        return None

    def enter(self, start, raw, side, quote, row, now):
        reason = self.entry_block(now)
        if (reason or start in self.traded or side not in ('Up', 'Down')
                or not 0 < quote['cost'] <= self.stake_usd):
            raise ValueError(reason or 'ambiguous_duplicate_or_entry')
        self.cash -= quote['cost']
        self.traded.add(start)
        self.position = dict(start=start, raw=raw, side=side, quote=quote,
                             row=row, opened_at=now, settled=False)
        self.trades.append(self.position)

    def settle(self, winner, now):
        if not self.position or winner not in ('Up', 'Down'):
            raise ValueError('ambiguous_resolution')
        if utc_day(now) != self.day:
            self.day, self.daily_pnl = utc_day(now), 0.0
        position = self.position
        payout = position['quote']['shares'] if winner == position['side'] else 0.0
        pnl = payout - position['quote']['cost']
        self.cash += payout
        self.daily_pnl += pnl
        self.hard_halt = self.hard_halt or (self.cash <= 30 and self.loss_streak_limit is None)
        self.loss_streak = self.loss_streak + 1 if pnl < -1e-9 else 0
        position.update(settled=True, winner=winner, pnl=pnl, settled_at=now)
        position['row'].update(outcome=winner, pnl=pnl, balance_after=self.cash)
        self.position = None
        return position


def blank_row(start):
    row = {key: '' for key in FIELDS}
    row.update(window_id=f'btc-updown-5m-{start}', ts_open=start,
               filled=False, stake=0, fee=0)
    return row


async def session(config_path, output, stop_at=None, entry_start_at=None,
                  settlement_grace_sec=DEFAULT_SETTLEMENT_GRACE_SEC):
    config = load_config(config_path)
    started = time.time()
    settlement_grace_sec = number(settlement_grace_sec)
    if settlement_grace_sec < 0 or settlement_grace_sec > 180:
        raise ValueError('invalid_settlement_grace')
    if entry_start_at is not None:
        entry_start_at = int(number(entry_start_at))
        if entry_start_at % 300 or entry_start_at < started:
            raise ValueError('entry_start_at_must_be_future_5m_boundary')
    if stop_at is not None and stop_at <= started:
        raise ValueError('Deadline elapsed; refusing to start experiment')
    if entry_start_at is not None and stop_at is not None and stop_at <= entry_start_at:
        raise ValueError('stop_at_must_follow_entry_start_at')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    config_bytes = Path(config_path).read_bytes()
    (output / 'config.yaml').write_bytes(config_bytes)

    account = Account(started, config['starting_capital_usd'], config['stake_usd'],
                      config.get('experimental_stop_after_consecutive_losses'))
    failures = RecoveringFeedFailures() if config.get('experimental_reconnect_websocket') else Failures()
    rows = []
    journal = Journal(output / 'journal.jsonl', dict(
        mode='PAPER_MANDATORY_V3_FINAL', config=dict(config),
        config_sha256=hashlib.sha256(config_bytes).hexdigest(),
        started_at=started, entry_start_at=entry_start_at, stop_entry_at=stop_at,
        settlement_grace_sec=settlement_grace_sec, source=SOURCE, topic=TOPIC,
        reference_filter=BTC_FILTER, weights=dict(WEIGHTS),
        budget_includes_fees=True, synthetic_microstake=False, actual_orders=0))
    reference = V3Reference(journal, failures)
    task = asyncio.create_task(reference.run())
    clock = None
    roster, meta_errors, meta_retry_at, evaluated = {}, {}, {}, set()
    candidate_attempts, candidate_skip_reasons = {}, {}
    start = entry_start_at if entry_start_at is not None else int(started // 300) * 300
    complete = False
    halt_reason = None

    async with httpx.AsyncClient(timeout=2, trust_env=False, follow_redirects=False) as client:
        async def get(url, params=None, role=None):
            sent, mono = time.time(), time.monotonic()
            try:
                response = await client.get(url, params=params)
                received, rtt = time.time(), time.monotonic() - mono
                response.raise_for_status()
                data = response.json()
                result = dict(type='http', role=role, url=url, params=params,
                              sent_at=sent, received_at=received, rtt=rtt, data=data)
                journal.write(result)
                failures.success('api')
                return result
            except Exception as exc:
                failures.fail('api')
                journal.write(dict(type='api_failure', role=role, at=time.time(),
                                   reason=type(exc).__name__, failures=failures.counts.copy()))
                raise ValueError('api_failure') from exc

        def now():
            if clock is None or not 0 <= time.time() - clock['received_at'] <= 90:
                raise ValueError('ambiguous_clock')
            return time.time() + clock['offset'], clock['uncertainty']

        async def sync_clock():
            nonlocal clock
            result = await get('https://clob.polymarket.com/time', role='clock')
            sample = clob_clock_sample(
                result['data'], result['sent_at'], result['received_at'])
            if sample['uncertainty'] > 1.5:
                raise ValueError('ambiguous_clock')
            clock = dict(received_at=result['received_at'], **sample)
            journal.write({'type': 'clock', **clock})

        async def metadata(window):
            slug = f'btc-updown-5m-{window}'
            result = await get('https://gamma-api.polymarket.com/events', {'slug': slug}, 'metadata')
            events = [x for x in result['data'] if x.get('slug') == slug]
            if len(events) != 1:
                raise ValueError('ambiguous_market_metadata')
            markets = [x for x in events[0].get('markets', []) if x.get('slug') == slug]
            if len(markets) != 1:
                raise ValueError('ambiguous_market_metadata')
            raw = markets[0]
            tokens = parse_contract(raw, window)
            if (not raw.get('description', '').startswith(RULE_TEXT)
                    or raw.get('resolutionSource') != SOURCE
                    or (raw.get('cryptoMarketConfig') or {}).get('twapLookbackSeconds') != 60):
                raise ValueError('ambiguous_settlement_source')
            fee = await get('https://clob.polymarket.com/clob-markets/' + raw['conditionId'], role='fee')
            if not fee_match(raw, fee['data'], list(tokens.values())):
                raise ValueError('ambiguous_fee_or_token_mapping')
            roster[window] = (raw, tokens, float(fee['data']['fd']['r']))

        def checkpoint():
            tmp = output / 'windows.tmp'
            with tmp.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            tmp.replace(output / 'windows.csv')
            below_min = sum(not x['quote']['live_minimum_met'] for x in account.trades)
            skip_reasons = {}
            for window_row in rows:
                reason = window_row.get('skip_reason')
                if reason:
                    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            entry_deadline_reached = (stop_at is None or time.time() >= stop_at)
            direct_trades = sum(
                x['quote'].get('execution_route') == 'direct_ask'
                for x in account.trades)
            complementary_trades = sum(
                x['quote'].get('execution_route') == 'complementary_bid'
                for x in account.trades)
            report = dict(mode='PAPER_MANDATORY_V3_FINAL', started_at=started,
                          entry_start_at=entry_start_at, updated_at=time.time(),
                          stop_entry_at=stop_at, settlement_grace_sec=settlement_grace_sec,
                          complete=complete, all_settled=account.position is None,
                          entry_deadline_reached=entry_deadline_reached,
                          ended_early=bool(stop_at is not None
                                           and not entry_deadline_reached
                                           and halt_reason is not None),
                          evaluated_windows=len(rows), skip_reasons=skip_reasons,
                          candidate_attempts=sum(
                              len(v) for v in candidate_attempts.values()),
                          candidate_skip_reasons=candidate_skip_reasons.copy(),
                          direct_ask_trades=direct_trades,
                          complementary_bid_trades=complementary_trades,
                          cash=account.cash,
                          starting_capital=config['starting_capital_usd'],
                          stake_usd=config['stake_usd'], trades=len(account.trades),
                          settled_trades=sum(x['settled'] for x in account.trades),
                          realized_pnl=sum(x.get('pnl', 0) for x in account.trades),
                          pending_positions=int(account.position is not None),
                          daily_pnl=account.daily_pnl, daily_utc=account.day,
                          hard_halt=account.hard_halt, loss_streak=account.loss_streak,
                          failure_halt=failures.halted,
                          failures=failures.counts.copy(), halt_reason=halt_reason,
                          source=SOURCE, weights=dict(WEIGHTS), config=dict(config),
                          synthetic_microstake=False, below_live_minimum_trades=below_min,
                          live_minimum_met_trades=len(account.trades) - below_min,
                          actual_orders=0, actual_fills=0, profitability_proven=False,
                          fill_model='V3_DIRECT_OR_COMPLEMENTARY_AFTER_2S_5USD')
            tmp = output / 'summary.tmp'
            tmp.write_text(json.dumps(report, indent=2) + '\n')
            tmp.replace(output / 'summary.json')
            return report

        async def reconcile():
            position = account.position
            if not position or time.time() < position['start'] + 300:
                return
            result = await get('https://data-api.polymarket.com/v2/resolutions',
                               {'condition': position['raw']['conditionId']}, 'resolution')
            states = result['data']['data']
            if not isinstance(states, list):
                raise ValueError('ambiguous_resolution')
            winners = set()
            for state in states:
                winner = final_winner(state, position['raw'], position['start'] + 300,
                                      result['received_at'])
                if winner:
                    # Payout magnitudes may be scaled (observed 0/1,000,000).
                    # final_winner validates exactly one positive winner.
                    winners.add(winner)
            if len(winners) > 1:
                raise ValueError('ambiguous_resolution')
            if winners:
                settled = account.settle(next(iter(winners)), result['received_at'])
                journal.write(dict(type='paper_settlement', start=position['start'],
                                   winner=settled['winner'], pnl=settled['pnl'], cash=account.cash))

        def finalize_window(window, row, reason):
            if window in evaluated:
                return
            row['skip_reason'] = reason
            row['balance_after'] = account.cash
            rows.append(row)
            evaluated.add(window)
            journal.write({'type': 'window', **row})
            checkpoint()

        async def evaluate_candidate(window, before_close, final_candidate=False):
            row = blank_row(window)
            row['candidate_before_close'] = before_close
            try:
                server, uncertainty = now()
                target = window + 300 - before_close
                if abs(server - target) + uncertainty > config['entry_tolerance_sec']:
                    raise ValueError('missed_candidate_time')
                if failures.halted:
                    raise ValueError('three_consecutive_failures')
                block = account.entry_block(time.time())
                if block:
                    raise ValueError(block)
                if window not in roster:
                    raise ValueError('metadata_unavailable')

                raw, tokens, rate = roster[window]
                opening, latest = await reference.decision_prices_at(
                    window, before_close, now, time.time, config)
                row.update(
                    btc_open=opening['price'], btc_at_entry=latest['price'],
                    delta=latest['price'] - opening['price'])

                books = {}
                for side_name in ('Up', 'Down'):
                    token_id = tokens[side_name]
                    result = await get(
                        'https://clob.polymarket.com/book',
                        {'token_id': token_id},
                        f'candidate_t{before_close}_{side_name.lower()}_book')
                    server, uncertainty = now()
                    fresh_book_v2(
                        result, token_id, server, uncertainty, time.time(), config)
                    books[side_name] = result['data']

                if config.get('fixed_time_delta_direction', False):
                    delta = latest['price'] - opening['price']
                    if delta == 0:
                        raise ValueError('zero_delta_no_direction')
                    side_fixed = 'Up' if delta > 0 else 'Down'
                    score = dict(side=side_fixed, total_score=0, confidence=0,
                                 core_score=0, core_strength=0, signal_agreement=0,
                                 price_cap=config.get('experimental_max_entry_price', 1.0), direction_score=0, recent_score=0,
                                 persistence_score=0, order_book_score=0, pricing_score=0)
                else:
                    score = score_decision(
                        reference, window, opening, latest,
                        books['Up'], books['Down'], config)
                    validate_signal(score, config)

                side = score['side']
                opposite = 'Down' if side == 'Up' else 'Up'
                token = tokens[side]
                opposite_token = tokens[opposite]
                decision_quote = paper_quote_pair(
                    books[side], books[opposite], token, opposite_token,
                    rate, config)
                validate_quote(decision_quote, score, config)

                row.update(
                    side=side.upper(), total_score=score['total_score'],
                    confidence=score['confidence'],
                    core_score=score['core_score'],
                    core_strength=score['core_strength'],
                    signal_agreement=score['signal_agreement'],
                    price_cap=score['price_cap'],
                    direction_score=score['direction_score'],
                    recent_score=score['recent_score'],
                    persistence_score=score['persistence_score'],
                    order_book_score=score['order_book_score'],
                    pricing_score=score['pricing_score'],
                    best_ask=decision_quote['vwap'],
                    effective_entry=decision_quote['effective_entry'],
                    execution_route=decision_quote['execution_route'])

                journal.write(dict(
                    type='paper_candidate_intent', start=window,
                    candidate_before_close=before_close, side=side,
                    score=score, quote=decision_quote,
                    budget=config['stake_usd'], at=time.time()))

                await asyncio.sleep(config['execution_delay_sec'])

                refreshed = {}
                for side_name in (side, opposite):
                    token_id = tokens[side_name]
                    result = await get(
                        'https://clob.polymarket.com/book',
                        {'token_id': token_id},
                        f'execution_{side_name.lower()}_book')
                    server, uncertainty = now()
                    fresh_book_v2(
                        result, token_id, server, uncertainty, time.time(), config)
                    refreshed[side_name] = result['data']

                if failures.halted:
                    raise ValueError('three_consecutive_failures')
                if (server >= window + 300
                        or (stop_at is not None and time.time() >= stop_at)):
                    raise ValueError('entry_deadline_passed')

                quote = paper_quote_pair(
                    refreshed[side], refreshed[opposite], token,
                    opposite_token, rate, config)
                validate_quote(
                    quote, score, config,
                    decision_effective=decision_quote['effective_entry'])

                account.enter(window, raw, side, quote, row, time.time())
                failures.success('execution')
                row.update(
                    best_ask=quote['vwap'], vwap_fill=quote['vwap'],
                    effective_entry=quote['effective_entry'],
                    execution_route=quote['execution_route'],
                    filled=True, stake=config['stake_usd'], fee=quote['fee'],
                    live_minimum_met=quote['live_minimum_met'],
                    outcome='PENDING', balance_after=account.cash)
                rows.append(row)
                evaluated.add(window)
                journal.write(dict(
                    type='paper_fill', start=window,
                    candidate_before_close=before_close, side=side,
                    score=score, quote=quote, at=time.time(),
                    actual_orders=0))
                checkpoint()
                return True

            except (KeyError, TypeError, ValueError, IndexError) as exc:
                reason = str(exc) if isinstance(exc, ValueError) else 'ambiguous_state'
                record_execution_result(failures, reason)
                candidate_skip_reasons[reason] = (
                    candidate_skip_reasons.get(reason, 0) + 1)
                journal.write(dict(
                    type='candidate_skip', start=window,
                    candidate_before_close=before_close,
                    at=time.time(), reason=reason,
                    failures=failures.counts.copy()))

                terminal = (
                    reason in (
                        'missing_causal_matched_opening',
                        'ambiguous_reference_revision',
                        'daily_loss_limit', 'hard_stop_balance_30',
                        'balance_below_stake', 'three_consecutive_failures',
                        'five_consecutive_losses')
                    or failures.halted or account.hard_halt)
                if final_candidate or terminal:
                    finalize_window(window, row, reason)
                    return True
                return False

        last_reconcile = last_clock = last_checkpoint = 0.0
        try:
            checkpoint()
            while stop_at is None or time.time() < stop_at:
                wall = time.time()
                if account.hard_halt or failures.halted:
                    halt_reason = ('hard_stop_balance_30' if account.hard_halt
                                   else 'three_consecutive_failures')
                    break
                if wall - last_clock >= 60:
                    last_clock = wall
                    try:
                        await sync_clock()
                    except ValueError:
                        pass
                server = wall + clock['offset'] if clock else wall
                while server >= start + 300:
                    if start not in evaluated:
                        row = blank_row(start)
                        row.update(skip_reason='missed_candidate_window', balance_after=account.cash)
                        rows.append(row)
                        journal.write({'type': 'window', **row})
                        evaluated.add(start)
                    start += 300
                if (server >= start and start not in roster
                        and wall >= meta_retry_at.get(start, 0)):
                    try:
                        await metadata(start)
                        meta_errors.pop(start, None)
                        meta_retry_at.pop(start, None)
                    except (ValueError, KeyError, TypeError, IndexError) as exc:
                        meta_errors[start] = (str(exc) if isinstance(exc, ValueError)
                                              else 'ambiguous_metadata')
                        meta_retry_at[start] = wall + METADATA_RETRY_SEC
                if start not in evaluated:
                    attempted = candidate_attempts.setdefault(start, set())
                    candidates = config['candidate_entry_times_before_close_sec']
                    for before_close in candidates:
                        if before_close in attempted:
                            continue
                        target = start + 300 - before_close
                        if server < target:
                            break
                        attempted.add(before_close)
                        final_candidate = before_close == candidates[-1]
                        if server - target > config['entry_tolerance_sec']:
                            reason = 'missed_candidate_time'
                            candidate_skip_reasons[reason] = (
                                candidate_skip_reasons.get(reason, 0) + 1)
                            journal.write(dict(
                                type='candidate_skip', start=start,
                                candidate_before_close=before_close,
                                at=time.time(), reason=reason))
                            if final_candidate:
                                row = blank_row(start)
                                row['candidate_before_close'] = before_close
                                finalize_window(start, row, reason)
                            continue
                        await evaluate_candidate(
                            start, before_close, final_candidate)
                        break
                if wall - last_reconcile >= 5:
                    last_reconcile = wall
                    try:
                        await reconcile()
                    except (ValueError, KeyError, TypeError, IndexError) as exc:
                        journal.write(dict(type='resolution_pending', at=time.time(),
                                           reason=type(exc).__name__))
                if account.hard_halt or failures.halted:
                    halt_reason = ('hard_stop_balance_30' if account.hard_halt
                                   else 'three_consecutive_failures')
                    break
                if wall - last_checkpoint >= 15:
                    checkpoint()
                    last_checkpoint = wall
                await asyncio.sleep(.1)
            settle_deadline = time.time() + settlement_grace_sec
            while account.position and time.time() < settle_deadline:
                try:
                    await reconcile()
                except (ValueError, KeyError, TypeError, IndexError) as exc:
                    journal.write(dict(type='resolution_pending', at=time.time(),
                                       reason=type(exc).__name__, phase='settlement_grace'))
                if account.position:
                    await asyncio.sleep(2)
            complete = True
            result = checkpoint()
            journal.write(dict(type='complete', finished_at=time.time(), summary=result))
            return result
        except BaseException:
            halt_reason = 'session_interrupted'
            checkpoint()
            raise
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            journal.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='config-mandatory-v3.yaml')
    parser.add_argument('--output', required=True)
    parser.add_argument('--stop-at', help='Absolute ISO timestamp with timezone; omit for continuous PAPER')
    parser.add_argument('--entry-start-at', help='Future aligned 5m ISO timestamp for first evaluated window')
    parser.add_argument('--settlement-grace-sec', type=float, default=DEFAULT_SETTLEMENT_GRACE_SEC)
    args = parser.parse_args()
    stop_at = None
    entry_start_at = None
    if args.stop_at:
        value = dt.datetime.fromisoformat(args.stop_at.replace('Z', '+00:00'))
        if value.tzinfo is None:
            parser.error('--stop-at must include timezone')
        stop_at = value.timestamp()
    if args.entry_start_at:
        value = dt.datetime.fromisoformat(args.entry_start_at.replace('Z', '+00:00'))
        if value.tzinfo is None:
            parser.error('--entry-start-at must include timezone')
        entry_start_at = value.timestamp()
    asyncio.run(session(args.config, args.output, stop_at, entry_start_at,
                        args.settlement_grace_sec))


if __name__ == '__main__':
    main()
