"""Pure daily probability baseline. No live runner, fills, or account operations."""
import hashlib
import json
import math
from pathlib import Path
from .daily_source_probe import closed_hourly

POLICY_SHA256 = '3e09168aa7ba22dbfb59c3ec357c25cb8d2d944b2bc50b2d3301f7e8ecd61cb7'
FREEZE_COMMIT = '28e51e5a910112026507440022519af23e5a0fd1'


def load_policy(path=None):
    raw=(Path(path) if path else Path(__file__).resolve().parents[1]/'docs/daily/policy.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=POLICY_SHA256:
        raise ValueError('Frozen daily policy changed; comparisons must be declared')
    return json.loads(raw)


def probability(rows, threshold, asof, nominal_target):
    """Caller must audit contract/timing/provenance before journaling real forecasts."""
    load_policy()
    if not all(math.isfinite(float(x)) for x in (threshold,asof,nominal_target)) or threshold<=0 or nominal_target<=asof:
        raise ValueError('Invalid forecast horizon or threshold')
    candles=closed_hourly(rows,asof)
    prices=[p for _,p in candles]
    variance_rate=sum(math.log(b/a)**2 for a,b in zip(prices,prices[1:]))/(24*3600)
    last_end=candles[-1][0]+3600
    if variance_rate<=0:
        return {'p_yes':None,'reason':'ZERO_VOLATILITY','financial_action':'ABSTAIN',
                'net_payoff':None,'fills':0}
    z=math.log(prices[-1]/threshold)/math.sqrt(variance_rate*(nominal_target-last_end))
    return {'p_yes':0.5*(1+math.erf(z/math.sqrt(2))),
            'model':'zero-drift-log-brownian','feature_role':'Coinbase predictor, not settlement',
            'financial_action':'ABSTAIN','reason':'OTHER_COSTS_UNKNOWN',
            'net_payoff':None,'fills':0,'realized_pnl':None,
            'policy_sha256':POLICY_SHA256,'freeze_commit':FREEZE_COMMIT}
