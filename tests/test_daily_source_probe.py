import pytest
from pairbot.daily_source_probe import closed_hourly

ASOF=30*3600+20

def candles():return [[t,99,101,100,100,10]for t in range(5*3600,30*3600,3600)]

def test_only_closed_consecutive_lookback_and_order():
    r=candles()[::-1]+[[30*3600,99,102,100,102,10]]
    assert len(closed_hourly(r,ASOF))==25
    assert closed_hourly(r,ASOF)[-1][0]==29*3600

@pytest.mark.parametrize('case',['gap','duplicate','nonfinite','ohlc','off_grid'])
def test_unusable_candles_rejected(case):
    r=candles()
    if case=='gap':r.pop(0)
    if case=='duplicate':r.append(r[0])
    if case=='nonfinite':r[0][4]=float('nan')
    if case=='ohlc':r[0][4]=200
    if case=='off_grid':r[0][0]+=1
    with pytest.raises(ValueError):closed_hourly(r,ASOF)
