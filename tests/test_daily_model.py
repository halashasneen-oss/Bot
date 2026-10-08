import pytest
from pairbot.daily_model import load_policy, probability

ASOF=30*3600+20

def rows():return [[t,99,102,100,100+(i%2),10]for i,t in enumerate(range(5*3600,30*3600,3600))]

def test_forecast_monotonic_and_unknown_cost_abstention():
    low=probability(rows(),95,ASOF,36*3600)
    high=probability(rows(),105,ASOF,36*3600)
    assert 0<=high['p_yes']<low['p_yes']<=1
    assert low['financial_action']=='ABSTAIN' and low['net_payoff'] is None and low['fills']==0


def test_latest_incomplete_candle_cannot_change_probability():
    r=rows();p=probability(r,100,ASOF,36*3600)
    r.append([30*3600,1,10000,100,10000,10])
    assert probability(r,100,ASOF,36*3600)==p


def test_policy_tampering_fails(tmp_path):
    p=tmp_path/'policy.json';p.write_text('{}')
    with pytest.raises(ValueError):load_policy(p)

@pytest.mark.parametrize('threshold,target',[(0,36*3600),(100,ASOF),(float('nan'),36*3600)])
def test_invalid_forecast_rejected(threshold,target):
    with pytest.raises(ValueError):probability(rows(),threshold,ASOF,target)


def test_zero_volatility_abstains():
    r=rows()
    for x in r:x[4]=100
    assert probability(r,100,ASOF,36*3600)['p_yes'] is None
