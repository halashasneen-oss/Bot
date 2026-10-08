"""Frozen momentum v1. Public data only; no account, signing or order API.

Approved clarifications: $5 INCLUDES entry fees, fixed two-second delay,
matched Chainlink TWAP opening/current source, PAPER-only implementation.
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
from websockets.asyncio.client import connect

from .directional import Journal
from .five_minute_live import parse_contract, final_winner
from .daily_readiness import fee_match
from .daily_capture import book_status
from .unified_paper import RULE_TEXT

FROZEN = MappingProxyType(dict(entry_time_before_close_sec=120,
    entry_tolerance_sec=5, min_move_usd=80, max_entry_price=.90,
    stake_usd=5, max_stale_data_sec=2, live=False))
TOPIC = 'crypto_prices_twap_sixty'
SOURCE = 'https://data.chain.link/streams/btc-usd-twap-60s-streams'
BTC_FILTER = json.dumps({'symbol':'btc/usd'}, separators=(',', ':'))
FIELDS = 'window_id ts_open btc_open btc_at_entry delta side best_ask vwap_fill filled skip_reason stake fee outcome pnl balance_after'.split()


def subscription_payload():
    """RTDS requires TWAP filters as a compact JSON string, not a JSON object."""
    return {'action':'subscribe', 'subscriptions':[{
        'topic':TOPIC, 'type':'update', 'filters':BTC_FILTER}]}


def load_config(path):
    # This frozen flat YAML schema intentionally excludes tags, aliases and
    # arbitrary objects. No third-party YAML dependency or runtime mutation.
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.split('#', 1)[0].strip()
        if not line:
            continue
        key, value = line.split(':', 1)
        key, value = key.strip(), value.strip()
        if key in values or key not in FROZEN:
            raise ValueError('Unknown or duplicate config field')
        values[key] = json.loads(value)
    if set(values) != set(FROZEN) or any(type(values[k]) is bool and k != 'live' for k in values):
        raise ValueError('Invalid frozen config schema')
    if values != dict(FROZEN):
        raise ValueError('Frozen v1 config changed; LIVE not implemented or enabled')
    return MappingProxyType(values)


def number(value):
    if isinstance(value, bool):
        raise ValueError('ambiguous_numeric_data')
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('ambiguous_numeric_data')
    return result


def side_for(delta, config):
    delta = number(delta)
    if delta >= config['min_move_usd']:
        return 'Up'
    if delta <= -config['min_move_usd']:
        return 'Down'
    return None


def quote(book, token, rate, config):
    """Full cash budget, VWAP cap (not a per-level price cap), fee inclusive.

    Conservatively round fee UP per level and shares DOWN to six decimals.
    A residual under $0.0001 is precision dust, never a partial fill.
    """
    rate = number(rate)
    if not 0 < rate <= 1 or book_status(book, token) != 'two_sided':
        raise ValueError('ambiguous_book_or_fee')
    remaining = config['stake_usd']
    shares = principal = fee_total = 0.
    levels = []
    for level in sorted(book['asks'], key=lambda x: number(x['price'])):
        price, size = number(level['price']), number(level['size'])
        quantity = math.floor(min(size, remaining/(price+rate*price*(1-price)))*1e6)/1e6
        fee = math.ceil(quantity*rate*price*(1-price)*1e5)/1e5
        if quantity*price+fee > remaining:
            quantity = max(0, math.floor((remaining-1e-5)/(price+rate*price*(1-price))*1e6)/1e6)
            fee = math.ceil(quantity*rate*price*(1-price)*1e5)/1e5
        if quantity <= 0:
            continue
        cost = quantity*price+fee
        if cost > remaining+1e-10:
            raise ValueError('ambiguous_fee_rounding')
        remaining -= cost
        shares += quantity
        principal += quantity*price
        fee_total += fee
        levels.append({'price': price, 'shares': quantity, 'fee': fee})
        if remaining < .0001:
            break
    if remaining >= .0001 or shares <= 0 or shares < number(book.get('min_order_size', 5)):
        raise ValueError('no_fill')
    vwap = principal/shares
    if vwap > config['max_entry_price']+1e-12:
        raise ValueError('vwap_above_0.90')
    return dict(shares=shares, principal=principal, fee=fee_total,
                cost=principal+fee_total, vwap=vwap, levels=levels)


def fresh_book(row, token, server, wall, config):
    book = row['data']
    timestamp = number(book['timestamp'])/1000
    if book.get('asset_id') != token:
        raise ValueError('ambiguous_book_token')
    if not (0 <= server-timestamp <= config['max_stale_data_sec']
            and 0 <= wall-row['received_at'] <= config['max_stale_data_sec']
            and row['rtt'] <= config['max_stale_data_sec']):
        raise ValueError('stale_book')


class Failures:
    """Independent consecutive failure streaks; unrelated GETs cannot hide WS failures."""
    def __init__(self):
        self.counts = {'api': 0, 'websocket': 0, 'execution': 0}
        self.halted = False

    def fail(self, channel):
        self.counts[channel] += 1
        if self.counts[channel] >= 3:
            self.halted = True

    def success(self, channel):
        self.counts[channel] = 0


def record_execution_result(failures, reason):
    """Classify a completed window without weakening the three-failure latch."""
    if reason.startswith('ambiguous_') or reason == 'stale_book':
        failures.fail('execution')
    elif reason in ('move_below_80', 'no_fill', 'vwap_above_0.90', 'stale_reference'):
        # A stale reference is a data-availability abstention for this window,
        # not an execution-system failure. It therefore cannot trip the latch.
        failures.success('execution')


class Reference:
    def __init__(self, journal, failures):
        self.journal, self.failures = journal, failures
        self.series = {}
        self.generation = 0
        self.connected = False
        self.invalid = set()

    def ingest(self, message, received):
        if not isinstance(message, dict) or message.get('topic') != TOPIC or message.get('type') != 'update':
            return False
        p = message.get('payload')
        if not isinstance(p, dict) or p.get('symbol') != 'btc/usd' or p.get('window_s') != 60:
            return False
        timestamp, price = number(p['timestamp'])/1000, number(p['value'])
        if price <= 0 or timestamp > received+2:
            raise ValueError('ambiguous_reference')
        old = self.series.get(timestamp)
        if old and old['price'] != price:
            self.invalid.add(timestamp)
            raise ValueError('ambiguous_reference_revision')
        # Only live updates are admitted. Subscription history never fills an opening.
        self.series.setdefault(timestamp, dict(timestamp=timestamp, price=price,
                                received_at=received, generation=self.generation))
        self.series = {t:v for t,v in self.series.items() if t >= received-1200}
        self.invalid = {t for t in self.invalid if t >= received-1200}
        return 0 <= received-timestamp <= 2

    def prices(self, start, server, wall, config):
        opening = self.series.get(start)
        if not opening or not start <= opening['received_at'] <= start+2:
            raise ValueError('missing_causal_matched_opening')
        available = [v for t,v in self.series.items() if t <= server and v['received_at'] <= wall]
        if not available:
            raise ValueError('missing_reference')
        latest = max(available, key=lambda x:x['timestamp'])
        if (not self.connected or opening['generation'] != self.generation
                or latest['generation'] != self.generation or start in self.invalid
                or latest['timestamp'] in self.invalid):
            raise ValueError('ambiguous_reference_connection_or_revision')
        if not (0 <= server-latest['timestamp'] <= config['max_stale_data_sec']
                and 0 <= wall-latest['received_at'] <= config['max_stale_data_sec']):
            raise ValueError('stale_reference')
        return opening, latest

    async def decision_prices(self, start, clock_fn, wall_fn, config):
        """Wait only inside the existing T-120 ±5s decision tolerance for a fresh tick."""
        target = start+300-config['entry_time_before_close_sec']
        waited = False
        while True:
            if self.failures.halted:
                raise ValueError('three_consecutive_failures')
            server, uncertainty = clock_fn()
            if abs(server-target)+uncertainty > config['entry_tolerance_sec']:
                raise ValueError('stale_reference' if waited else 'missed_entry_time')
            try:
                return self.prices(start, server, wall_fn(), config)
            except ValueError as exc:
                if str(exc) != 'stale_reference':
                    raise
                if not waited:
                    self.journal.write({'type':'reference_wait', 'start':start,
                        'at':wall_fn(), 'reason':'stale_reference',
                        'deadline':target+config['entry_tolerance_sec']-uncertainty})
                    waited = True
                await asyncio.sleep(.05)

    async def run(self):
        while not self.failures.halted:
            heartbeat = None
            try:
                async with connect('wss://ws-live-data.polymarket.com', proxy=None,
                                   open_timeout=5, ping_interval=10, max_queue=128) as ws:
                    self.generation += 1
                    self.connected = True
                    await ws.send(json.dumps(subscription_payload(), separators=(',', ':')))
                    self.journal.write({'type':'feed_open', 'at':time.time(),
                        'generation':self.generation, 'topic':TOPIC, 'filters':BTC_FILTER})
                    async def ping():
                        while True:
                            await asyncio.sleep(5)
                            await ws.send('PING')
                    heartbeat = asyncio.create_task(ping())
                    progress = time.monotonic()
                    last_timestamp = None
                    while not self.failures.halted:
                        if heartbeat.done():
                            heartbeat.result()
                            raise RuntimeError('heartbeat_stopped')
                        if time.monotonic()-progress >= 10:
                            raise RuntimeError('websocket_btc_silent')
                        try:
                            raw = await asyncio.wait_for(ws.recv(), 1)
                        except asyncio.TimeoutError:
                            continue
                        received = time.time()
                        if raw in ('PONG', 'PING', ''):
                            continue
                        message = json.loads(raw)
                        self.journal.write({'type':'feed_message', 'received_at':received, 'data':message})
                        if (isinstance(message, dict)
                                and 'invalid request body' in str(message.get('message', '')).lower()):
                            raise RuntimeError('rtds_subscription_rejected')
                        if self.ingest(message, received):
                            timestamp = float(message['payload']['timestamp'])/1000
                            if last_timestamp is None or timestamp > last_timestamp:
                                progress = time.monotonic()
                                last_timestamp = timestamp
                                self.failures.success('websocket')
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.failures.fail('websocket')
                self.journal.write({'type':'websocket_failure', 'at':time.time(),
                                    'reason':type(exc).__name__, 'failures':self.failures.counts.copy()})
            finally:
                self.connected = False
                if heartbeat:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
            if not self.failures.halted:
                # A new connection is allowed; a failed window is NEVER retried.
                await asyncio.sleep(1)


def utc_day(now):
    return dt.datetime.fromtimestamp(now, dt.timezone.utc).date().isoformat()


class Account:
    def __init__(self, now):
        self.cash = 50.
        self.position = None
        self.daily_pnl = 0.
        self.day = utc_day(now)
        self.hard_halt = False
        self.traded = set()
        self.trades = []

    def entry_block(self, now):
        if utc_day(now) != self.day:
            self.day, self.daily_pnl = utc_day(now), 0.
        # Balance here is settled account equity; cash locked in an open
        # position must not be mistaken for a realized hard-stop loss.
        if self.hard_halt:
            return 'hard_stop_balance_30'
        if self.daily_pnl <= -10:
            return 'daily_loss_limit'
        if self.position:
            return 'open_position_limit'
        if self.cash <= 30:
            self.hard_halt = True
            return 'hard_stop_balance_30'
        if self.cash < 5:
            return 'balance_below_5'
        return None

    def enter(self, start, raw, side, q, row, now):
        reason = self.entry_block(now)
        if reason or start in self.traded or side not in ('Up', 'Down') or not 0 < q['cost'] <= 5:
            raise ValueError(reason or 'ambiguous_duplicate_or_entry')
        self.cash -= q['cost']
        self.traded.add(start)
        self.position = dict(start=start, raw=raw, side=side, quote=q, row=row,
                             opened_at=now, settled=False)
        self.trades.append(self.position)

    def settle(self, winner, now):
        if not self.position or winner not in ('Up', 'Down'):
            raise ValueError('ambiguous_resolution')
        if utc_day(now) != self.day:
            self.day, self.daily_pnl = utc_day(now), 0.
        p = self.position
        payout = p['quote']['shares'] if winner == p['side'] else 0.
        pnl = payout-p['quote']['cost']
        self.cash += payout
        self.daily_pnl += pnl
        self.hard_halt = self.hard_halt or self.cash <= 30
        p.update(settled=True, winner=winner, pnl=pnl, settled_at=now)
        p['row'].update(outcome=winner, pnl=pnl, balance_after=self.cash)
        self.position = None
        return p


def blank_row(start):
    row = {k:'' for k in FIELDS}
    row.update(window_id=f'btc-updown-5m-{start}', ts_open=start, filled=False,
               stake=0, fee=0)
    return row


async def session(config_path, output, stop_at=None, resume_from=None):
    config = load_config(config_path)
    started = time.time()
    if stop_at is not None and stop_at <= started:
        raise ValueError('Deadline elapsed; refusing to start another experiment')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output/'config.yaml').write_bytes(Path(config_path).read_bytes())
    account = Account(started)
    failures = Failures()
    rows = []
    resumed = None
    if resume_from:
        state_path = Path(resume_from)/'state.json'
        state_bytes = state_path.read_bytes()
        resumed = json.loads(state_bytes)
        if resumed['config'] != dict(config) or resumed['mode'] != 'PAPER_MOMENTUM_V1':
            raise ValueError('Ambiguous resume configuration')
        if hashlib.sha256((Path(resume_from)/'config.yaml').read_bytes()).hexdigest() != hashlib.sha256(Path(config_path).read_bytes()).hexdigest():
            raise ValueError('Resume frozen config mismatch')
        rows = resumed['rows']
        account.cash = number(resumed['cash'])
        account.day = resumed['daily_utc']
        account.daily_pnl = number(resumed['daily_pnl'])
        account.hard_halt = resumed['hard_halt']
        account.trades = resumed['all_trades']
        index = {r['window_id']:r for r in rows}
        account.traded = {t['start'] for t in account.trades}
        for trade in account.trades:
            trade['row'] = index[f"btc-updown-5m-{trade['start']}"]
        pending = [t for t in account.trades if not t['settled']]
        if len(pending) > 1:
            raise ValueError('Ambiguous multiple resumed positions')
        account.position = pending[0] if pending else None
        failures.counts = resumed['failures']
        failures.halted = resumed['failure_halt']
    journal = Journal(output/'journal.jsonl', dict(mode='PAPER_MOMENTUM_V1',
        config=dict(config), config_sha256=hashlib.sha256(Path(config_path).read_bytes()).hexdigest(),
        started_at=started, stop_entry_at=stop_at, source=SOURCE, topic=TOPIC,
        reference_filter=BTC_FILTER, budget_includes_fees=True, delay_seconds=2, actual_orders=0,
        resumed_state_sha256=hashlib.sha256(state_bytes).hexdigest() if resumed else None))
    reference = Reference(journal, failures)
    clock = None
    task = asyncio.create_task(reference.run())
    roster, meta_errors, evaluated = {}, {}, set()
    evaluated = {int(r['ts_open']) for r in rows}
    first = max(int(r['ts_open']) for r in rows)+300 if rows else int(started//300)*300
    # Include the startup window explicitly, even if its opening was missed.
    start = first
    complete = False
    halt_reason = None
    async with httpx.AsyncClient(timeout=2, trust_env=False, follow_redirects=False) as client:
        async def get(url, params=None, role=None):
            sent = time.time()
            mono = time.monotonic()
            try:
                response = await client.get(url, params=params)
                received, rtt = time.time(), time.monotonic()-mono
                response.raise_for_status()
                data = response.json()
                row = dict(type='http', role=role, url=url, params=params,
                           sent_at=sent, received_at=received, rtt=rtt, data=data)
                journal.write(row)
                failures.success('api')
                return row
            except Exception as exc:
                failures.fail('api')
                journal.write(dict(type='api_failure', role=role, at=time.time(),
                                   reason=type(exc).__name__, failures=failures.counts.copy()))
                raise ValueError('api_failure') from exc

        def now():
            if clock is None or not 0 <= time.time()-clock['received_at'] <= 90:
                raise ValueError('ambiguous_clock')
            return time.time()+clock['offset'], clock['uncertainty']

        async def sync_clock():
            nonlocal clock
            r = await get('https://clob.polymarket.com/time', role='clock')
            server = number(r['data'])
            uncertainty = r['rtt']/2+.5  # integer /time timestamp precision
            if uncertainty > 1.5:
                raise ValueError('ambiguous_clock')
            clock = dict(received_at=r['received_at'], uncertainty=uncertainty,
                         offset=server-(r['sent_at']+r['received_at'])/2)
            journal.write({'type':'clock', **clock})

        async def metadata(s):
            slug = f'btc-updown-5m-{s}'
            r = await get('https://gamma-api.polymarket.com/events', {'slug':slug}, 'metadata')
            events = [e for e in r['data'] if e.get('slug') == slug]
            if len(events) != 1:
                raise ValueError('ambiguous_market_metadata')
            markets = [m for m in events[0].get('markets', []) if m.get('slug') == slug]
            if len(markets) != 1:
                raise ValueError('ambiguous_market_metadata')
            raw = markets[0]
            tokens = parse_contract(raw, s)
            if (not raw.get('description', '').startswith(RULE_TEXT)
                    or raw.get('resolutionSource') != SOURCE
                    or (raw.get('cryptoMarketConfig') or {}).get('twapLookbackSeconds') != 60):
                raise ValueError('ambiguous_settlement_source')
            fee = await get('https://clob.polymarket.com/clob-markets/'+raw['conditionId'], role='fee')
            if not fee_match(raw, fee['data'], list(tokens.values())):
                raise ValueError('ambiguous_fee_or_token_mapping')
            roster[s] = (raw, tokens, float(fee['data']['fd']['r']))

        def checkpoint():
            # One row per window. Pending rows are UPDATED on resolution,
            # never appended a second time or relabelled as wins by price touch.
            tmp = output/'windows.tmp'
            with tmp.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            tmp.replace(output/'windows.csv')
            report = dict(mode='PAPER_MOMENTUM_V1', started_at=started, updated_at=time.time(),
                stop_entry_at=stop_at, complete=complete, cash=account.cash,
                starting_capital=50, trades=len(account.trades),
                settled_trades=sum(t['settled'] for t in account.trades),
                realized_pnl=sum(t.get('pnl', 0) for t in account.trades),
                pending_positions=int(account.position is not None), daily_pnl=account.daily_pnl,
                daily_utc=account.day, hard_halt=account.hard_halt,
                failure_halt=failures.halted, failures=failures.counts.copy(),
                halt_reason=halt_reason, source=SOURCE, config=dict(config),
                actual_orders=0, actual_fills=0, profitability_proven=False,
                other_costs=None, fill_model='CONDITIONAL_DISPLAYED_DEPTH_AFTER_2S')
            tmp = output/'summary.tmp'
            tmp.write_text(json.dumps(report, indent=2)+'\n')
            tmp.replace(output/'summary.json')
            state = dict(report, rows=rows, all_trades=account.trades)
            tmp = output/'state.tmp'
            tmp.write_text(json.dumps(state, indent=2)+'\n')
            tmp.replace(output/'state.json')
            return report

        async def reconcile():
            p = account.position
            if not p or time.time() < p['start']+300:
                return
            r = await get('https://data-api.polymarket.com/v2/resolutions',
                          {'condition':p['raw']['conditionId']}, 'resolution')
            states = r['data']['data']
            if not isinstance(states, list):
                raise ValueError('ambiguous_resolution')
            winners = set()
            for state in states:
                w = final_winner(state, p['raw'], p['start']+300, r['received_at'])
                if w:
                    payouts = list(map(number, state['payouts']))
                    if sorted(payouts) != [0., 1.]:
                        raise ValueError('ambiguous_resolution_payout')
                    winners.add(w)
            if len(winners) > 1:
                raise ValueError('ambiguous_resolution')
            if winners:
                settled = account.settle(next(iter(winners)), r['received_at'])
                journal.write({'type':'paper_settlement', 'start':p['start'],
                               'winner':settled['winner'], 'pnl':settled['pnl'], 'cash':account.cash})
                print(json.dumps({'event':'settlement', 'start':p['start'], 'pnl':settled['pnl'], 'balance':account.cash}), flush=True)

        async def evaluate(s):
            row = blank_row(s)
            rows.append(row)
            evaluated.add(s)  # Once only, BEFORE any I/O; no retry on ambiguity.
            try:
                server, uncertainty = now()
                if abs(server-(s+300-config['entry_time_before_close_sec']))+uncertainty > config['entry_tolerance_sec']:
                    raise ValueError('missed_entry_time')
                block = account.entry_block(time.time())
                if failures.halted:
                    raise ValueError('three_consecutive_failures')
                if block:
                    raise ValueError(block)
                if s in meta_errors:
                    raise ValueError(meta_errors[s])
                raw, tokens, rate = roster[s]
                opening, latest = await reference.decision_prices(s, now, time.time, config)
                row.update(btc_open=opening['price'], btc_at_entry=latest['price'],
                           delta=latest['price']-opening['price'])
                side = side_for(row['delta'], config)
                if side is None:
                    raise ValueError('move_below_80')
                row['side'] = side.upper()
                token = tokens[side]
                book = await get('https://clob.polymarket.com/book', {'token_id':token}, 'decision_book')
                server, _ = now()
                fresh_book(book, token, server, time.time(), config)
                row['best_ask'] = min(number(x['price']) for x in book['data']['asks'])
                quote(book['data'], token, rate, config)
                journal.write({'type':'paper_intent', 'start':s, 'side':side,
                               'delta':row['delta'], 'budget':5, 'at':time.time()})
                await asyncio.sleep(2)
                refreshed = await get('https://clob.polymarket.com/book', {'token_id':token}, 'execution_book')
                server, _ = now()
                fresh_book(refreshed, token, server, time.time(), config)
                # The momentum decision is frozen once per window. Only data
                # integrity, book VWAP/depth and risk are rechecked after delay.
                reference.prices(s, server, time.time(), config)
                if failures.halted:
                    raise ValueError('three_consecutive_failures')
                if server >= s+300 or (stop_at is not None and time.time() >= stop_at):
                    raise ValueError('entry_deadline_passed')
                q = quote(refreshed['data'], token, rate, config)
                row['best_ask'] = min(number(x['price']) for x in refreshed['data']['asks'])
                account.enter(s, raw, side, q, row, time.time())
                failures.success('execution')
                row.update(filled=True, stake=config['stake_usd'], fee=q['fee'],
                           vwap_fill=q['vwap'], outcome='PENDING')
                journal.write({'type':'paper_fill', 'start':s, 'side':side, 'quote':q,
                               'at':time.time(), 'actual_orders':0})
            except (KeyError, TypeError, ValueError, IndexError) as exc:
                reason = str(exc) if isinstance(exc, ValueError) else 'ambiguous_state'
                row['skip_reason'] = reason
                record_execution_result(failures, reason)
            row['balance_after'] = account.cash
            journal.write({'type':'window', **row})
            checkpoint()
            print(json.dumps({'event':'window', **row}), flush=True)

        last_reconcile = last_clock = last_checkpoint = 0.
        try:
            checkpoint()
            print(json.dumps({'event':'session_started', 'mode':'PAPER_MOMENTUM_V1',
                'started_at':started, 'stop_at':stop_at, 'bankroll':50, 'stake_including_fees':5,
                'cash':account.cash, 'resumed':bool(resumed),
                'frozen_config':dict(config), 'actual_orders':0}), flush=True)
            while stop_at is None or time.time() < stop_at:
                wall = time.time()
                if account.hard_halt or failures.halted:
                    halt_reason = 'hard_stop_balance_30' if account.hard_halt else 'three_consecutive_failures'
                    break
                if wall-last_clock >= 60:
                    last_clock = wall
                    try:
                        await sync_clock()
                    except ValueError:
                        pass
                server = wall+clock['offset'] if clock else wall
                # Process EVERY consecutive window, logging any missed ones.
                while server >= start+300:
                    if start not in evaluated:
                        row = blank_row(start)
                        row.update(skip_reason='missed_entry_time', balance_after=account.cash)
                        rows.append(row)
                        journal.write({'type':'window', **row})
                        evaluated.add(start)
                    start += 300
                if start not in roster and start not in meta_errors:
                    try:
                        await metadata(start)
                    except (ValueError, KeyError, TypeError, IndexError) as exc:
                        meta_errors[start] = str(exc) if isinstance(exc, ValueError) else 'ambiguous_metadata'
                target = start+300-config['entry_time_before_close_sec']
                if server >= target and start not in evaluated:
                    await evaluate(start)
                if wall-last_reconcile >= 15:
                    last_reconcile = wall
                    try:
                        await reconcile()
                    except (ValueError, KeyError, TypeError, IndexError) as exc:
                        journal.write({'type':'resolution_pending', 'at':time.time(), 'reason':type(exc).__name__})
                if account.hard_halt or failures.halted:
                    halt_reason = 'hard_stop_balance_30' if account.hard_halt else 'three_consecutive_failures'
                    # Stop entries permanently for this run; no automatic restart.
                    break
                if wall-last_checkpoint >= 15:
                    checkpoint()
                    last_checkpoint = wall
                await asyncio.sleep(.1)
            # No deadline liquidation. Report an unresolved position honestly.
            if account.position:
                try:
                    await reconcile()
                except (ValueError, KeyError, TypeError, IndexError):
                    pass
            complete = True
            result = checkpoint()
            journal.write({'type':'complete', 'finished_at':time.time(), 'summary':result})
            print(json.dumps({'event':'session_complete', **result}), flush=True)
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
    parser.add_argument('--config', default='config.yaml')
    parser.add_argument('--output', required=True)
    parser.add_argument('--stop-at', help='Absolute ISO timestamp with timezone; omitted for continuous PAPER')
    parser.add_argument('--resume-from', help='Previous segment directory; preserves bankroll, positions and risk latches')
    args = parser.parse_args()
    stop_at = None
    if args.stop_at:
        value = dt.datetime.fromisoformat(args.stop_at.replace('Z', '+00:00'))
        if value.tzinfo is None:
            parser.error('--stop-at must include timezone')
        stop_at = value.timestamp()
    asyncio.run(session(args.config, args.output, stop_at, args.resume_from))


if __name__ == '__main__':
    main()
