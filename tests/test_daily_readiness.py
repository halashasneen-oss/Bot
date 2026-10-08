import pytest
from pairbot.daily_readiness import fee_match

def fixtures():
    return ({'feesEnabled': True, 'feeSchedule': {'rate': .07, 'exponent': 1, 'takerOnly': True}},
            {'fd': {'r': .07, 'e': 1, 'to': True}, 't': [{'t': '11'}, {'t': '22'}]})

def test_fee_match():
    g,c=fixtures()
    assert fee_match(g,c,['11','22'])
    assert not fee_match(g,c,['11','33'])

@pytest.mark.parametrize('field,value', [('r', .1),('r',float('nan')),('e',2),('to',False)])
def test_mismatch_and_nonfinite_rejected(field,value):
    g,c=fixtures();c['fd'][field]=value
    assert not fee_match(g,c,['11','22'])

@pytest.mark.parametrize('bad',[{},None,{'fd':None}])
def test_missing_not_zero(bad):
    g,_=fixtures()
    assert not fee_match(g,bad,['11','22'])
