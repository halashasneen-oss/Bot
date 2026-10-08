"""Observations only. Book deletions and price touches never consume a queue."""
from collections import defaultdict, Counter
import math


def distribution(values):
    values = sorted(values)
    if not values:
        return {'n': 0, 'p50': None, 'p99': None}
    return {'n': len(values), 'min': values[0], 'max': values[-1],
            'p50': values[int((len(values)-1)*.5)],
            'p99': values[int((len(values)-1)*.99)]}


class Measurements:
    def __init__(self):
        self.markets = {}
        self.latency = defaultdict(list)
        self.minute_latency = defaultdict(lambda: defaultdict(list))
        self.bid_sum_seconds = Counter()
        self.buy_prints = []
        self.above_limit = []
        self.pending_depth = defaultdict(list)
        self.print_index = defaultdict(list)

    def interval(self, e, ts):
        if not e.active or ts <= e.now:
            return
        m = e.markets[e.active]
        end, start = min(ts, m.end), max(e.now, m.start)
        if end <= start:
            return
        row = self.markets.setdefault(e.active, {'slug': m.slug, 'market_duration_seconds': m.end-m.start,
            'tokens': {'Up': m.up, 'Down': m.down}, 'observed_seconds': 0.,
            'spread_guard_seconds': 0.,
            'cooldown_seconds': 0., 'live_order_seconds': {t: 0. for t in m.tokens},
            'fresh_bid_seconds': 0., 'edge_002_seconds': 0.})
        dt = end-start
        row['observed_seconds'] += dt
        # Movement active can persist past movement_until until stability recovers.
        pause_end = end if e.active in e.movement_active else min(end, e.movement_until.get(e.active, start))
        row['cooldown_seconds'] += max(0., pause_end-start)
        if e.spread_guard_active:
            row['spread_guard_seconds'] += dt
        for t in m.tokens:
            o = e.orders.get(t)
            if o and o.queue is not None:
                row['live_order_seconds'][t] += max(0., min(end, o.cancel_at or end)-max(start, o.active_at, o.queue_at))
        if e.fresh():
            valid_end = min(end, *(e.received[t]+e.cfg.stale_seconds for t in m.tokens),
                            *(e.exchange_ts[t]+e.cfg.stale_seconds for t in m.tokens))
            weight = max(0., valid_end-start)
            bids = [e.books[t].best_bid() for t in m.tokens]
            if all(bids):
                total = sum(b.price for b in bids)
                row['fresh_bid_seconds'] += weight
                if total <= .98+1e-8:
                    row['edge_002_seconds'] += weight
                self.bid_sum_seconds[round(total, 6)] += weight

    def frame(self, e, msg, received):
        try:
            ex = float(msg.get('timestamp', 0))
            if ex > 1e12:
                ex /= 1000
            if ex <= 0 or not math.isfinite(ex):
                return
            lag = received-ex
            kind = msg.get('event_type', 'unknown')
            self.latency[kind].append(lag)
            self.minute_latency[kind][int(received//60)*60].append(lag)
        except (ValueError, TypeError):
            pass

    def trade(self, e, x, raw):
        m = e.markets[e.active]
        other = m.down if x.asset_id == m.up else m.up
        o = e.orders.get(other if x.aggressor.value == 'BUY' else x.asset_id)
        bid = e.books[other if x.aggressor.value == 'BUY' else x.asset_id].best_bid()
        signature = (e.active, x.ts, round(x.size, 8), raw.get('transaction_hash'))
        peers = self.print_index[signature]
        if x.aggressor.value == 'BUY':
            live = bool(o and o.queue is not None and e.now >= o.active_at and
                        (o.cancel_at is None or e.now < o.cancel_at))
            row = {'ts': e.now, 'exchange_ts': x.ts, 'market': e.active,
                   'token': x.asset_id, 'opposite_token': other, 'price': x.price,
                   'size': x.size, 'opposite_live': live,
                   'opposite_limit': o.price if o else None,
                   'eligible_price': bool(live and o.price+1e-8 >= 1-x.price),
                   'after_anchor': bool(live and x.ts >= max(o.active_at, o.queue_at)),
                   'fresh': e.fresh() and e.now-x.ts <= e.cfg.stale_seconds,
                   'queue_before': o.queue if o else None,
                   'best_bid_depth_before': bid.size if bid else None,
                   'best_bid_price_before': bid.price if bid else None,
                   'next_depth': None, 'paired_print_candidate': False,
                   'exact_transaction_pair': False,
                   'transaction_hash': raw.get('transaction_hash')}
            self.buy_prints.append(row)
            self.pending_depth[other].append(row)
        else:
            row = {'token': x.asset_id, 'price': x.price}
            if o and x.price > o.price+1e-8:
                row = {**row, 'ts': e.now, 'exchange_ts': x.ts, 'market': e.active,
                       'size': x.size, 'limit': o.price, 'queue_before': o.queue,
                       'queue_after': o.queue, 'consumed_by_model': 0.,
                       'best_bid_depth_before': bid.size if bid else None,
                       'best_bid_price_before': bid.price if bid else None, 'next_depth': None,
                       'interpretation': 'Depth change cannot prove queue consumption'}
                self.above_limit.append(row)
                self.pending_depth[x.asset_id].append(row)
        for peer in peers:
            if peer['token'] != x.asset_id and abs(peer['price']+x.price-1) < 1e-8:
                for item in (peer, row):
                    item['paired_print_candidate'] = True
                    item['exact_transaction_pair'] = bool(raw.get('transaction_hash'))
        peers.append(row)
        # Only simultaneous exchange timestamps can match; bounded index.
        if len(self.print_index) > 20000:
            del self.print_index[next(iter(self.print_index))]

    def depth(self, e, tokens):
        for t in tokens:
            bid = e.books[t].best_bid()
            for row in self.pending_depth.pop(t, []):
                row['next_depth'] = {'ts': e.now, 'price': bid.price if bid else None,
                                     'size': bid.size if bid else None}

    def report(self):
        return {'markets': self.markets,
                'receive_minus_exchange_seconds': {k: distribution(v) for k,v in self.latency.items()},
                'latency_by_utc_minute': {k: {str(t): distribution(v) for t,v in bins.items()}
                                        for k,bins in self.minute_latency.items()},
                'best_bid_sum_seconds': dict(sorted(self.bid_sum_seconds.items())),
                'clock_offset': 'Not identifiable from one-way exchange/receive timestamps; no correction applied',
                'buy_prints': self.buy_prints, 'above_limit_sells': self.above_limit,
                'legacy_receive_clock': 'Old journals timestamp processing, not socket receipt; backlog and clock offset cannot be separated'}
