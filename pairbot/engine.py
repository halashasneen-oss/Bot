"""Deterministic counterfactual maker simulation; no exchange writes.

Fills require an observed SELL print and consume a conservative visible queue.
Order-book touches/cancellations never create fills. This is an estimate, not
proof of executable profit. Gaps invalidate execution-quality claims.
"""
from collections import deque, Counter
from .measure import Measurements
from dataclasses import dataclass, asdict, field
import math

from polymaker.domain import Side
from polymaker.marketdata.orderbook import OrderBook
from polymaker.marketdata.parse import parse_book, parse_price_changes, parse_last_trade, parse_tick_size_change

EPS = 1e-8
MODEL_VERSION = 3


@dataclass
class Position:
    size: float = 0.0
    cost: float = 0.0
    on_change: object = field(default=None, repr=False, compare=False)

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        callback = getattr(self, 'on_change', None)
        if key == 'size' and callback is not None:
            callback()

    def remove(self, size):
        if size < -EPS or size > self.size + EPS:
            raise ValueError('Insufficient shares')
        cost = self.cost * size / self.size if self.size else 0.0
        self.size -= size
        self.cost -= cost
        if self.size < EPS:
            self.size = self.cost = 0.0
        return cost


@dataclass
class Order:
    token: str
    price: float
    remaining: float
    created: float
    active_at: float
    queue: float | None = None
    cancel_at: float | None = None
    queue_at: float | None = None
    reviewed_at: float | None = None


class Engine:
    def __init__(self, config):
        self.cfg = config
        self.source = "UNSPECIFIED"
        self.record_only = False
        self.run_info = {}
        self.cash = config.bankroll
        self.markets = {}
        self.positions = {}
        self.exposed_markets = set()
        self.books = {}
        self.received = {}
        self.exchange_ts = {}
        self.orders = {}
        self.pending = []
        self.active = None
        self.paused = False
        self.now = 0.0
        self.halted = None
        self.blocked = set()
        self.unpaired_since = {}
        self.mid_min = deque()
        self.mid_max = deque()
        self.seen = set()
        self.seen_fifo = deque()
        self.fills = 0
        self.merges = 0
        self.merge_pnl = 0.0
        self.residual_pnl = 0.0
        self.fees = 0.0
        self.merge_costs = 0.0
        self.turnover = 0.0
        self.gaps = 0
        self.rejected_frames = 0
        self.book_events = 0
        self.delayed_frames = 0
        self.out_of_order_frames = 0
        self.resync_tokens = set()
        self.rejection_reasons = {}
        self.audit = []
        self.peak_equity = config.bankroll
        self.max_drawdown = 0.0
        self.fill_clock = {}
        self.quote_checks = 0
        self.quote_blocks = Counter()
        self.trade_checks = 0
        self.trade_blocks = Counter()
        self.execution_counts = Counter()
        self.movement_until = {}
        self.movement_active = set()
        self.stable_min = deque()
        self.stable_max = deque()
        self.stable_since = None
        self.measurements = Measurements()
        self.spread_guard_active = False

    def log(self, kind, **data):
        self.audit.append({'ts': self.now, 'type': kind, **data})

    def select(self, market, ts):
        reconnect = self.active == market.condition_id
        self.advance(ts)
        self.cancel_all()
        if any(o.cancel_at is None or o.cancel_at > ts for o in self.orders.values()):
            raise ValueError('Advance cancellation latency before switching market')
        self.markets[market.condition_id] = market
        self.active = market.condition_id
        self.paused = False
        self.spread_guard_active = False
        for token in market.tokens:
            if token not in self.positions:
                self.positions[token] = self._position(market.condition_id)
            self.books[token] = OrderBook(market.tick)
            self.received.pop(token, None)
            self.exchange_ts.pop(token, None)
        if not reconnect:
            self.mid_min.clear()
            self.mid_max.clear()
        self.stable_min.clear()
        self.stable_max.clear()
        self.stable_since = None
        self.resync_tokens.clear()
        self.log('market', slug=market.slug)

    def _position(self, cid):
        def changed():
            m = self.markets[cid]
            sizes = [self.positions[t].size for t in m.tokens if t in self.positions]
            if any(size > EPS for size in sizes):
                self.exposed_markets.add(cid)
            else:
                self.exposed_markets.discard(cid)
            if len(sizes) == 2 and abs(sizes[0] - sizes[1]) > EPS:
                self.unpaired_since.setdefault(cid, self.now)
            else:
                self.unpaired_since.pop(cid, None)
        return Position(on_change=changed)

    def reserved(self):
        f = 1 + self.cfg.maker_fee_bps / 10000
        return sum(o.price * o.remaining * f for o in self.orders.values())

    def committed(self):
        return self.reserved() + sum(p.cost for p in self.positions.values()) + sum(x['cost'] for x in self.pending)

    def cancel_all(self):
        for o in self.orders.values():
            if o.cancel_at is None:
                o.cancel_at = self.now + self.cfg.cancel_latency_seconds

    def gap(self, ts):
        self.advance(ts)
        self.gaps += 1
        self.cancel_all()
        self.received.clear()
        self.exchange_ts.clear()
        self.stable_min.clear()
        self.stable_max.clear()
        self.stable_since = None
        self.log('data_gap', warning='Unobserved fills during gap cannot be reconstructed')

    def advance(self, ts):
        if not math.isfinite(ts) or ts < self.now:
            raise ValueError('Receive clock must be finite and monotonic')
        self.measurements.interval(self, ts)
        self.now = ts
        for token, o in list(self.orders.items()):
            if o.cancel_at is not None and ts >= o.cancel_at:
                del self.orders[token]
        for item in list(self.pending):
            if ts >= item['due']:
                self.cash += item['amount'] - item['fee']
                self.merge_pnl += item['amount'] - item['cost'] - item['fee']
                self.merge_costs += item['fee']
                self.merges += 1
                self.pending.remove(item)
                self.log('merge_confirmed', **item)
        for cid in sorted(self.exposed_markets):
            m = self.markets[cid]
            imbalance = abs(self.positions[m.up].size - self.positions[m.down].size)
            if imbalance > EPS:
                since = self.unpaired_since.setdefault(cid, ts)
                if ts - since >= self.cfg.max_unpaired_seconds:
                    if cid not in self.blocked:
                        self.execution_counts['residual_timeouts'] += 1
                        self.log('residual_timeout', condition_id=cid, shares=imbalance)
                    self.blocked.add(cid)
                    if cid == self.active:
                        self.cancel_all()
            else:
                self.unpaired_since.pop(cid, None)
        if self.active:
            m = self.markets[self.active]
            if ts >= m.end - self.cfg.stop_before_close_seconds or not self.fresh():
                self.cancel_all()
            if not self.fresh():
                self.stable_min.clear()
                self.stable_max.clear()
                self.stable_since = None
        self._risk()
        self._review_orders()

    def _review_orders(self):
        # Reviewing an unchanged limit does not cancel/reinsert it or invent
        # queue improvement. Changed limits use the configured cancel latency.
        for token, o in self.orders.items():
            reviewed = o.created if o.reviewed_at is None else o.reviewed_at
            if o.cancel_at is not None or self.now - reviewed < self.cfg.quote_lifetime_seconds:
                continue
            plan, reason = self._quote_plan()
            if (not self.paused and not self.halted and self.active not in self.blocked
                    and not self._movement_paused() and reason is None and token in plan
                    and abs(plan[token][0] - o.price) <= EPS):
                o.reviewed_at = self.now
                self.execution_counts['order_retained'] += 1
                self.log('order_retained', token=token, price=o.price, queue=o.queue)
            else:
                o.cancel_at = self.now + self.cfg.cancel_latency_seconds
                self.execution_counts['quote_expired'] += 1

    def _movement_paused(self):
        return self.active in self.movement_active or self.now < self.movement_until.get(self.active, 0)

    def fresh(self):
        if not self.active:
            return False
        return all(t in self.received and self.now - self.received[t] <= self.cfg.stale_seconds
                   and self.now - self.exchange_ts.get(t, 0) <= self.cfg.stale_seconds
                   and t not in self.resync_tokens
                   for t in self.markets[self.active].tokens)

    def _risk(self):
        equity = self.equity()
        self.peak_equity = max(self.peak_equity, equity)
        self.max_drawdown = max(self.max_drawdown, self.peak_equity - equity)
        if self.cfg.bankroll - equity >= self.cfg.loss_stop_usd:
            if self.halted is None:
                self.halted = 'session_loss_stop'
                self.log('halt', reason=self.halted)
            self.cancel_all()

    def equity(self):
        """Conservative mark: walk bid depth, unmatched depth valued at zero.
        A complete set retains its merge value even when quotes are unavailable.
        """
        value = self.cash + sum(x['amount'] - x['fee'] for x in self.pending)
        for cid in sorted(self.exposed_markets):
            m = self.markets[cid]
            a, b = self.positions[m.up], self.positions[m.down]
            paired = min(a.size, b.size)
            value += paired
            for token, pos in ((m.up, a), (m.down, b)):
                remaining = pos.size - paired
                if (token not in self.received or self.now - self.received[token] > self.cfg.stale_seconds
                        or self.now - self.exchange_ts.get(token, 0) > self.cfg.stale_seconds
                        or token in self.resync_tokens):
                    continue
                book = self.books[token]
                for price in reversed(book.bids):
                    q = min(remaining, book.bids[price])
                    value += q * price
                    remaining -= q
                    if remaining <= EPS:
                        break
        return value

    def _valid_ts(self, token, exchange):
        # Structural time validation is separate from trading freshness. A delayed
        # but ordered update is needed to maintain L2 state; it cannot cause fills.
        return (math.isfinite(exchange) and exchange > 0 and exchange <= self.now + 1)

    @staticmethod
    def _levels_valid(levels):
        # 0 and 1 can appear at expiry or as zero-size deleted levels.
        return all(math.isfinite(p) and math.isfinite(s) and 0 <= p <= 1 and s >= 0 for p, s in levels)

    def _accept_time(self, token, exchange):
        if not self._valid_ts(token, exchange):
            raise ValueError('invalid_exchange_timestamp')
        if exchange < self.exchange_ts.get(token, 0):
            self.out_of_order_frames += 1
            self.cancel_all()
            self.resync_tokens.add(token)
            return False
        if self.now - exchange > self.cfg.stale_seconds:
            self.delayed_frames += 1
            self.cancel_all()
        return True

    def frame(self, msg, ts, *, received_at=None):
        self.advance(ts)
        if isinstance(msg, dict):
            self.measurements.frame(self, msg, ts if received_at is None else received_at)
        if not isinstance(msg, dict) or not self.active:
            return
        market = self.markets[self.active]
        if msg.get('market') and msg['market'] != market.condition_id:
            return
        kind = msg.get('event_type')
        affected = set()
        try:
            if kind == 'book':
                token = str(msg.get('asset_id', ''))
                if token not in market.tokens:
                    return
                affected.add(token)
                x = parse_book(msg)
                if not x or not self._levels_valid(x.bids + x.asks):
                    raise ValueError('invalid_snapshot_levels')
                if len(x.bids) != len(msg.get('bids', [])) or len(x.asks) != len(msg.get('asks', [])):
                    raise ValueError('malformed_snapshot_levels')
                # A new full snapshot is the recovery boundary. Its timestamp must
                # still not move backwards within a connection.
                if self._accept_time(token, x.ts):
                    self.books[token].apply_snapshot(x.bids, x.asks, x.ts, x.book_hash)
                    self.book_events += 1
                    self.received[token] = ts
                    self.exchange_ts[token] = x.ts
                    self.resync_tokens.discard(token)
            elif kind == 'price_change':
                changes = parse_price_changes(msg)
                raw_changes = msg.get('price_changes', [])
                affected = {str(c.get('asset_id')) for c in raw_changes if isinstance(c, dict)} & set(market.tokens)
                if len(changes) != len(raw_changes):
                    raise ValueError('malformed_delta')
                # Validate the full batch before mutating either token.
                accepted = []
                for x in changes:
                    if x.asset_id not in market.tokens:
                        continue
                    if x.asset_id not in self.received or x.asset_id in self.resync_tokens:
                        self.resync_tokens.add(x.asset_id)
                        continue
                    if not self._levels_valid([(x.price, x.size)]):
                        raise ValueError('invalid_delta_levels')
                    if self._accept_time(x.asset_id, x.ts):
                        accepted.append(x)
                for x in accepted:
                    self.books[x.asset_id].apply_delta(x.side, x.price, x.size, x.ts)
                    self.received[x.asset_id] = ts
                    self.exchange_ts[x.asset_id] = x.ts
            elif kind == 'tick_size_change':
                token = str(msg.get('asset_id', ''))
                if token not in market.tokens:
                    return
                affected.add(token)
                x = parse_tick_size_change(msg)
                if x and math.isfinite(x.tick_size) and 0 < x.tick_size < 1:
                    self.books[token].set_tick_size(x.tick_size)
                    self.cancel_all()
                else:
                    raise ValueError('invalid_tick')
            elif kind == 'last_trade_price':
                token = str(msg.get('asset_id', ''))
                if token not in market.tokens:
                    return
                x = parse_last_trade(msg)
                if not x or not self._valid_ts(token, x.ts) or not self._levels_valid([(x.price, x.size)]):
                    raise ValueError('invalid_trade')
                # Stale prints neither deplete queues nor generate execution.
                if self.now - x.ts > self.cfg.stale_seconds:
                    self.measurements.trade(self, x, msg)
                    self.delayed_frames += 1
                    self.trade_checks += 1
                    self.trade_blocks['stale_print'] += 1
                    self.cancel_all()
                else:
                    self._activate()
                    self.measurements.trade(self, x, msg)
                    self._trade(x, msg)
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            self.rejected_frames += 1
            reason = str(exc) or type(exc).__name__
            self.rejection_reasons[reason] = self.rejection_reasons.get(reason, 0) + 1
            self.cancel_all()
            # Only corrupt tokens need re-snapshotting, not every valid book.
            for token in affected:
                self.received.pop(token, None)
                self.resync_tokens.add(token)
            self.log('frame_rejected', reason=reason, event_type=kind, tokens=sorted(affected))
        if kind in {'book', 'price_change'}:
            self.measurements.depth(self, affected - self.resync_tokens)
        self._market_risk()
        self._activate()
        self._merge()
        self._risk()
        self.quote()

    def _market_risk(self):
        if not self.active or not self.fresh():
            self.stable_min.clear()
            self.stable_max.clear()
            self.stable_since = None
            return
        tops = [(self.books[t].best_bid(), self.books[t].best_ask()) for t in self.markets[self.active].tokens]
        if any(b is None or a is None or a.price - b.price <= 0 or a.price - b.price > self.cfg.kill_spread for b, a in tops):
            if not self.spread_guard_active:
                self.log('guard_trigger', condition_id=self.active, reason='spread',
                         seconds_from_start=self.now-self.markets[self.active].start,
                         spreads=[a.price-b.price if a and b else None for b,a in tops], range=None)
            self.spread_guard_active = True
            self.stable_min.clear()
            self.stable_max.clear()
            self.stable_since = None
            self.cancel_all()
            return
        self.spread_guard_active = False
        if self.now-self.markets[self.active].start < self.cfg.guard_warmup_seconds:
            return
        mid = (tops[0][0].price + tops[0][1].price) / 2
        signals = [mid]
        if self.cfg.guard_mode == 'combined_mid':
            signals = [mid, 1-(tops[1][0].price+tops[1][1].price)/2]
        elif self.cfg.guard_mode == 'combined_bid':
            signals = [tops[0][0].price, 1-tops[1][0].price]
        if self.active in self.movement_active:
            if self.stable_since is None:
                self.stable_since = self.now
            for signal in signals:
                self._extrema(self.stable_min, self.stable_max, signal, self.cfg.movement_stability_seconds)
            short_move = self.stable_max[0][1] - self.stable_min[0][1]
            if short_move + EPS >= self.cfg.kill_move:
                self.movement_until[self.active] = self.now + self.cfg.movement_cooldown_seconds
            if (self.now >= self.movement_until[self.active]
                    and self.now - self.stable_since >= self.cfg.movement_stability_seconds
                    and short_move + EPS < self.cfg.kill_move):
                self.movement_active.discard(self.active)
                self.movement_until.pop(self.active)
                self.mid_min.clear()
                self.mid_max.clear()
                self.mid_min.append((self.now, mid))
                self.mid_max.append((self.now, mid))
                self.execution_counts['movement_resumes'] += 1
                self.log('movement_resume', condition_id=self.active, stable_range=short_move)
            else:
                self.cancel_all()
            return
        # Monotonic queues preserve the exact rolling extrema in amortized O(1).
        # Scanning every frame's minute-long history caused a slow-consumer backlog.
        for signal in signals:
            self._extrema(self.mid_min, self.mid_max, signal, self.cfg.kill_window_seconds)
        movement = self.mid_max[0][1] - self.mid_min[0][1]
        if movement + EPS >= self.cfg.kill_move:
            if self.active not in self.movement_active:
                self.execution_counts['movement_pauses'] += 1
                self.log('movement_pause', condition_id=self.active, range=movement,
                         reason='best_bid' if self.cfg.guard_mode == 'combined_bid' else 'mid_range',
                         seconds_from_start=self.now-self.markets[self.active].start,
                         spreads=[a.price-b.price for b,a in tops])
            self.movement_active.add(self.active)
            self.movement_until[self.active] = self.now + self.cfg.movement_cooldown_seconds
            self.stable_since = self.now
            self.stable_min.clear()
            self.stable_max.clear()
            self.cancel_all()

    def _extrema(self, low, high, value, window):
        for queue in (low, high):
            while queue and self.now - queue[0][0] > window:
                queue.popleft()
        while low and low[-1][1] >= value:
            low.pop()
        while high and high[-1][1] <= value:
            high.pop()
        low.append((self.now, value))
        high.append((self.now, value))

    def _activate(self):
        for token, o in list(self.orders.items()):
            if o.queue is not None or self.now < o.active_at:
                continue
            b = self.books[token]
            ask = b.best_ask()
            if not self.fresh() or ask is None or o.price >= ask.price - EPS:
                del self.orders[token]  # post-only rejected on arrival
                self.log('post_only_reject', token=token)
                self.execution_counts['post_only_rejected'] += 1
                continue
            o.queue = self.cfg.queue_multiplier * sum(s for p, s in b.bids.items() if p >= o.price - EPS)
            o.queue_at = self.now
            self.execution_counts['orders_activated'] += 1
            self.log('order_active', token=token, price=o.price, queue=o.queue, queue_at=o.queue_at)

    def _trade(self, x, raw):
        self.trade_checks += 1
        def blocked(reason):
            self.trade_blocks[reason] += 1
        key = (x.asset_id, x.ts, x.aggressor.value, x.price, x.size, raw.get('transaction_hash'))
        if key in self.seen:
            blocked('duplicate')
            return
        self.seen.add(key)
        self.seen_fifo.append(key)
        if len(self.seen_fifo) > 20000:
            self.seen.discard(self.seen_fifo.popleft())
        # Book timestamps and trade timestamps are independent streams. A fresh
        # print can arrive after a newer book without being a duplicate or old
        # order fill. Anchor execution to the actual queue snapshot and a trade
        # watermark; never replay a print preceding that snapshot.
        if x.ts < self.fill_clock.get(x.asset_id, 0):
            blocked('out_of_order_trade')
            return
        self.fill_clock[x.asset_id] = x.ts
        o = self.orders.get(x.asset_id)
        if not o:
            blocked('no_order')
            return
        if o.queue is None:
            blocked('not_active')
            return
        if not self.fresh():
            blocked('stale_book')
            return
        if x.aggressor != Side.SELL:
            blocked('buy_print')
            return
        if x.ts < max(o.active_at, o.queue_at) or x.ts > self.now + 1 or self.now - x.ts > self.cfg.stale_seconds:
            blocked('before_queue_or_stale')
            return
        if x.price > o.price + EPS:
            blocked('above_limit')
            return
        queue_before = o.queue
        quantity = max(0.0, x.size * self.cfg.trade_volume_multiplier - o.queue)
        o.queue = max(0.0, o.queue - x.size * self.cfg.trade_volume_multiplier)
        q = min(o.remaining, quantity)
        if q <= EPS:
            blocked('queue_ahead')
            return
        cost = q * o.price
        fee = cost * self.cfg.maker_fee_bps / 10000
        if cost + fee > self.cash + EPS:
            raise RuntimeError('Reservation accounting failed')
        self.cash -= cost + fee
        p = self.positions[x.asset_id]
        p.size += q
        p.cost += cost + fee
        o.remaining -= q
        self.fees += fee
        self.turnover += cost
        self.fills += 1
        self.execution_counts['fills'] += 1
        self.log('fill', token=x.asset_id, price=o.price, size=q, fee=fee,
                 exchange_ts=x.ts, print_price=x.price, print_size=x.size,
                 queue_before=queue_before, queue_anchor=o.queue_at, active_at=o.active_at,
                 reason='observed_sell_print_exceeds_queue')
        if o.remaining < EPS:
            del self.orders[x.asset_id]

    def _merge(self):
        if not self.active:
            return
        m = self.markets[self.active]
        a, b = self.positions[m.up], self.positions[m.down]
        q = min(a.size, b.size)
        if q <= EPS or q <= self.cfg.merge_cost_usd:
            return
        cost = a.remove(q) + b.remove(q)
        self.pending.append({'condition_id': m.condition_id, 'amount': q, 'cost': cost,
                             'fee': self.cfg.merge_cost_usd, 'due': self.now + self.cfg.merge_delay_seconds})
        self.log('merge_submitted', amount=q, cost=cost)

    def quote(self):
        self.quote_checks += 1
        reason = ('record_only' if self.record_only else 'paused' if self.paused else
                  'no_market' if not self.active else 'loss_stop' if self.halted else
                  'market_blocked' if self.active in self.blocked else
                  'movement_cooldown' if self._movement_paused() else
                  'existing_orders' if self.orders else None)
        if reason:
            self.quote_blocks[reason] += 1
            return
        plan, reason = self._quote_plan()
        if reason:
            self.quote_blocks[reason] += 1
            return
        factor = 1 + self.cfg.maker_fee_bps / 10000
        required = sum(price * size * factor for price, size in plan.values())
        if required > self.cash - self.reserved() + EPS:
            self.quote_blocks['cash_cap'] += 1
            return
        if self.committed() + required > self.cfg.max_inventory_usd + EPS:
            self.quote_blocks['inventory_cap'] += 1
            return
        for token, (price, size) in plan.items():
            self.orders[token] = Order(token, price, size, self.now, self.now + self.cfg.order_latency_seconds)
        self.execution_counts['quote_cycles'] += 1
        self.execution_counts['orders_created'] += len(plan)
        self.log('quote', orders={t: {'price':p,'shares':s,
                 'best_bid':self.books[t].best_bid().price,
                 'distance_below_best_bid':self.books[t].best_bid().price-p}
                 for t,(p,s) in plan.items()})

    def _quote_plan(self):
        if not self.active or not self.fresh():
            return {}, 'stale_or_missing_book'
        m = self.markets[self.active]
        if not m.start <= self.now < m.end - self.cfg.stop_before_close_seconds:
            return {}, 'close_buffer_or_not_started'
        if self.pending:
            return {}, 'pending_merge'
        tops = [(self.books[t].best_bid(), self.books[t].best_ask()) for t in m.tokens]
        if any(b is None or a is None for b, a in tops):
            return {}, 'missing_depth'
        if any(a.price - b.price <= 0 or a.price - b.price > self.cfg.kill_spread for b, a in tops):
            return {}, 'spread_guard'
        size = self.cfg.order_size
        if size + EPS < m.minimum:
            return {}, 'below_market_minimum'
        factor = 1 + self.cfg.maker_fee_bps / 10000
        # After a partial or one-sided fill, only quote the missing shares. Never
        # add directional exposure, waive the minimum, or release reserved cash.
        a, b = self.positions[m.up], self.positions[m.down]
        if a.size > EPS or b.size > EPS:
            if abs(a.size-b.size) <= EPS:
                return {}, 'paired_inventory_pending_merge'
            held, token, top = (a, m.down, tops[1]) if a.size>b.size else (b, m.up, tops[0])
            missing = abs(a.size-b.size)
            if missing + EPS < m.minimum:
                return {}, 'residual_below_minimum'
            size = min(size, missing)
            if size + EPS < m.minimum:
                return {}, 'residual_below_minimum'
            ceiling = (1-self.cfg.min_pair_edge-held.cost/held.size-self.cfg.merge_cost_usd/size)/factor
            price = min(top[0].price, ceiling)
            tick = self.books[token].tick_size
            price = math.floor((price+EPS)/tick)*tick
            if price <= 0 or price >= top[1].price:
                return {}, 'no_residual_pair_price'
            return {token: (price,size)}, None
        prices = [b.price for b,a in tops]
        ceiling = (1 - self.cfg.min_pair_edge - self.cfg.merge_cost_usd / size) / factor
        excess = max(0.0, sum(prices) - ceiling)
        prices = [math.floor((p - excess / 2 + EPS) / self.books[t].tick_size) * self.books[t].tick_size
                  for p, t in zip(prices, m.tokens)]
        if any(p <= 0 or p >= top[1].price for p, top in zip(prices, tops)):
            return {}, 'no_pair_price'
        return dict(zip(m.tokens, ((p,size) for p in prices))), None

    def settle(self, cid, winner, ts):
        self.advance(ts)
        m = self.markets[cid]
        if winner not in m.tokens or ts < m.end:
            raise ValueError('Invalid settlement')
        if cid == self.active:
            self.cancel_all()
        payout = self.positions[winner].size
        costs = sum(self.positions[t].cost for t in m.tokens)
        self.cash += payout
        self.residual_pnl += payout - costs
        for t in m.tokens:
            self.positions[t] = self._position(cid)
        self.exposed_markets.discard(cid)
        self.unpaired_since.pop(cid, None)
        self.blocked.add(cid)
        self.log('settlement', condition_id=cid, winner=winner, payout=payout, cost=costs)

    def report(self):
        equity = self.equity()
        outstanding = sum(p.size for p in self.positions.values())
        return {'mode': 'PAPER_ONLY', 'data_source': self.source, 'run': dict(self.run_info), 'config': asdict(self.cfg), 'cash': self.cash,
                'model_version': MODEL_VERSION,
                'measurements': self.measurements.report(),
                'execution_diagnostics': {'quote_checks':self.quote_checks, 'quote_blocks':dict(self.quote_blocks),
                                          'trade_checks':self.trade_checks, 'trade_blocks':dict(self.trade_blocks),
                                          'counts':dict(self.execution_counts)},
                'equity_conservative': equity, 'pnl_marked': equity - self.cfg.bankroll,
                'pnl_realized': self.merge_pnl + self.residual_pnl,
                'paired_pnl': self.merge_pnl, 'residual_pnl': self.residual_pnl,
                'maker_fees': self.fees, 'merge_costs': self.merge_costs, 'rebates': 0,
                'turnover': self.turnover, 'fills': self.fills, 'merges': self.merges,
                'pending_merges': len(self.pending), 'unsettled_shares': outstanding,
                'reserved_cash': self.reserved(), 'max_drawdown': self.max_drawdown,
                'halt_reason': self.halted, 'data_gaps': self.gaps,
                'rejected_frames': self.rejected_frames,
                'rejection_reasons': dict(self.rejection_reasons),
                'delayed_frames': self.delayed_frames,
                'out_of_order_frames': self.out_of_order_frames,
                'markets_selected': len(self.markets),
                'book_events': self.book_events,
                'final_pnl_available': self.book_events >= 2 and outstanding <= EPS and not self.pending and not self.orders,
                'execution_quality': 'NO_DATA' if self.book_events == 0 else ('INCOMPLETE_DATA' if self.gaps or self.rejected_frames or self.delayed_frames or self.out_of_order_frames else 'MODEL_ESTIMATE_ONLY'),
                'warning': 'Counterfactual fills; no proof of live profitability. Missing depth valued at zero. Fees are configured assumptions; rebates excluded.'}

