"""Deterministic paper-research math. No network, signing or exchange writes."""
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import math
import os
from pathlib import Path
import random
from statistics import NormalDist


def load_protocol(path):
    path = Path(path)
    body = path.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    if path.with_suffix('.sha256').read_text().strip() != digest:
        raise ValueError('Frozen protocol hash mismatch')
    return json.loads(body), digest


class FeeSchedule:
    def __init__(self, rate, exponent=1, taker_only=True, provenance=''):
        if not math.isfinite(float(rate)) or not 0 <= float(rate) <= 1:
            raise ValueError('Invalid fee rate')
        if exponent != 1 or taker_only is not True or not provenance:
            raise ValueError('Unsupported or unproven fee schedule')
        self.rate = Decimal(str(rate))
        self.provenance = provenance

    @classmethod
    def from_gamma(cls, raw):
        if raw.get('feesEnabled') is False:
            return cls(0, provenance='Gamma feesEnabled=false')
        s = raw.get('feeSchedule')
        if raw.get('feesEnabled') is not True or not isinstance(s, dict):
            raise ValueError('Missing per-market fee schedule')
        return cls(s['rate'], s['exponent'], s['takerOnly'],
                   'Gamma feeSchedule: '+str(raw.get('conditionId', 'unknown')))

    def fee(self, size, price, maker=False):
        if not math.isfinite(size+price) or size < 0 or not 0 <= price <= 1:
            raise ValueError('Invalid fee inputs')
        if maker:
            return 0.
        q, p = Decimal(str(size)), Decimal(str(price))
        return float((q*self.rate*p*(1-p)).quantize(Decimal('.00001'), rounding=ROUND_HALF_UP))


def upper_opportunity(kind, prices, depths, fee, edge=.02, size=5, operation_cost=None):
    """Instantaneous visible-depth bound. Unknown operation cost is never verified zero."""
    if kind not in ('taker_pair', 'mint_sell'):
        raise ValueError('Unknown opportunity')
    if any(p is None or not 0 < p < 1 for p in prices) or min(depths) <= 0:
        return None
    quantity = min(size, *depths)
    fees = sum(fee.fee(quantity, p) for p in prices)
    op = 0 if operation_cost is None else operation_cost
    gross = quantity*(1-sum(prices)) if kind == 'taker_pair' else quantity*(sum(prices)-1)
    net = gross-fees-op
    return {'kind':kind, 'quantity':quantity, 'prices':prices,
            'available_depth':min(depths), 'taker_fees':fees, 'net_usd_bound':net,
            'edge_per_share':net/quantity, 'qualifies':net > quantity*edge+1e-8,
            'operation_cost':operation_cost,
            'bound':'UNKNOWN_OPERATION_COST_UPPER_BOUND' if operation_cost is None else 'VISIBLE_BOOK_UPPER_BOUND'}


def realized_volatility(points, now, lookback=120, minimum=20):
    recent = [(t,p) for t,p in points if now-lookback <= t <= now and p > 0]
    returns = [math.log(b/a) for (ta,a),(tb,b) in zip(recent,recent[1:]) if tb > ta]
    duration = recent[-1][0]-recent[0][0] if recent else 0
    if len(returns) < minimum or duration <= 0:
        return None
    mean = sum(returns)/len(returns)
    return math.sqrt(sum((r-mean)**2 for r in returns)/duration)


def up_probability(current, opening, seconds_left, sigma_log, scale=1, *,
                   twap_seconds=60, known_integral=None):
    """Final average probability, not barrier crossing; arithmetic Brownian approximation."""
    if any(not math.isfinite(x) for x in (current,opening,seconds_left,sigma_log,scale,twap_seconds)):
        raise ValueError('Nonfinite model input')
    if min(current,opening,scale,twap_seconds) <= 0 or seconds_left < 0 or sigma_log < 0:
        raise ValueError('Invalid model input')
    t, l = seconds_left, twap_seconds
    if t >= l:
        mean, variance = current, t-2*l/3
    else:
        if known_integral is None:
            return None
        mean, variance = (known_integral+current*t)/l, t**3/(3*l*l)
    sd = current*sigma_log*scale*math.sqrt(max(0,variance))
    if sd == 0:
        return float(mean >= opening)
    return min(1-1e-6,max(1e-6,NormalDist().cdf((mean-opening)/sd)))


def path_integral(points, start, end, max_gap=5):
    prior = [(t,p) for t,p in points if t <= start]
    if not prior or start-prior[-1][0] > max_gap or not points or end-points[-1][0] > max_gap:
        return None
    selected = [(start,prior[-1][1])]+[(t,p) for t,p in points if start < t <= end]
    selected.append((end,selected[-1][1]))
    if any(b[0]-a[0] > max_gap for a,b in zip(selected,selected[1:])):
        return None
    return sum((b[0]-a[0])*a[1] for a,b in zip(selected,selected[1:]))


def window_scores(samples, prediction='model'):
    scores = defaultdict(list)
    for r in samples:
        if r.get('winner') in (0,1) and r.get(prediction) is not None:
            scores[r['window']].append((r[prediction]-r['winner'])**2)
    return {k:sum(v)/len(v) for k,v in scores.items()}


def calibration(samples, prediction='model', bins=10):
    scores = window_scores(samples,prediction)
    valid = [r for r in samples if r.get('winner') in (0,1) and r.get(prediction) is not None]
    counts = defaultdict(int)
    for r in valid:
        counts[r['window']] += 1
    buckets = [[] for _ in range(bins)]
    for r in valid:
        p = r[prediction]
        buckets[min(bins-1,int(p*bins))].append((p,r['winner'],1/counts[r['window']]))
    rows = []
    for i, group in enumerate(buckets):
        total = sum(x[2] for x in group)
        rows.append({'bin':i, 'samples':len(group), 'window_weight':total,
                     'predicted':sum(p*w for p,y,w in group)/total if total else None,
                     'observed':sum(y*w for p,y,w in group)/total if total else None})
    return {'windows':len(scores), 'samples':len(valid),
            'brier':sum(scores.values())/len(scores) if scores else None, 'reliability':rows}


def bootstrap_ci(values, seed=20261004, replicates=10000, alpha=.05/8, block=1):
    if not values:
        return {'lower':None,'upper':None,'n':0,'block':block}
    rng = random.Random(seed)
    n = len(values)
    means = []
    for _ in range(replicates):
        picked = []
        while len(picked) < n:
            start = rng.randrange(n)
            picked.extend(values[(start+j)%n] for j in range(block))
        means.append(sum(picked[:n])/n)
    means.sort()
    return {'lower':means[int((replicates-1)*alpha)],
            'upper':means[int((replicates-1)*(1-alpha))],
            'n':n,'block':block,'alpha_one_sided':alpha,'seed':seed,'replicates':replicates}


def chronological_split(windows, fractions=(.5,.25,.25)):
    ordered = sorted(windows,key=lambda w:(w['start'],w['condition_id']))
    a = int(len(ordered)*fractions[0])
    b = int(len(ordered)*(fractions[0]+fractions[1]))
    return {'train':ordered[:a],'validation':ordered[a:b],'test':ordered[b:]}


def claim_holdout(path, input_hash, protocol_hash):
    path = Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x') as f:
        f.write(json.dumps({'input_sha256':input_hash,'protocol_sha256':protocol_hash,
                            'state':'FINAL_EVALUATION_CLAIMED'}))
        f.flush()
        os.fsync(f.fileno())


def acceptance(row, protocol):
    checks = [
        (row.get('net_pnl') is not None and row['net_pnl'] > 0,'NONPOSITIVE_OR_UNKNOWN_NET_PNL'),
        (row.get('pairs',0) >= protocol['minimum_completed_pairs'],'INSUFFICIENT_COMPLETED_PAIRS'),
        (row.get('pair_windows',0) >= protocol['minimum_test_windows'],'INSUFFICIENT_INDEPENDENT_PAIR_WINDOWS'),
        (row.get('ci',{}).get('lower') is not None and row['ci']['lower'] > 0,'LOWER_CI_NOT_POSITIVE'),
        (row.get('block_ci',{}).get('lower') is not None and row['block_ci']['lower'] > 0,'BLOCK_LOWER_CI_NOT_POSITIVE'),
        (row.get('max_drawdown',float('inf')) <= protocol['max_drawdown_usd'],'DRAWDOWN_LIMIT'),
        (row.get('quality') == 'COMPLETE_DATA','INCOMPLETE_DATA'),
        (row.get('costs_verified') is True,'COSTS_UNVERIFIED'),
        (row.get('bound') == 'conservative','OPTIMISTIC_RESULT_EXCLUDED'),
        (row.get('split') == 'test','NOT_FINAL_HOLDOUT')]
    reasons = [reason for check,reason in checks if not check]
    return {'decision':'NO-GO' if reasons else 'GO','reasons':reasons}
