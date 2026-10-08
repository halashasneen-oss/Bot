import math
import pytest
from pairbot.unified_paper import Feed, Ledger, load_policy, quote_budget

class Log:
    def __init__(self):self.rows=[]
    def write(self,row):self.rows.append(row)

def book(price=.5,size=100):
    return {'asset_id':'1','bids':[{'price':str(price/2),'size':'100'}],
            'asks':[{'price':str(price),'size':str(size)}],'min_order_size':'5'}

@pytest.mark.parametrize('price',[.01,.03,.1,.5,.9,.99])
def test_budget_never_exceeded_and_fee_not_zero(price):
    q=quote_budget(book(price,1000),'1',min(.999,price+.005),.07)
    assert 4.9999<q['cost_usd']<=5
    assert q['entry_fee_usd']>0
    assert q['cost_usd']==pytest.approx(q['principal_usd']+q['entry_fee_usd'])
    assert q['shares']<=1000
    for x in q['levels']:
        exact=x['shares']*.07*x['price']*(1-x['price'])
        assert exact<=x['fee_usd']<=exact+.0000100001

def test_walk_depth_with_hard_original_limit():
    b=book(.49,3);b['asks'] += [{'price':'.50','size':'30'},{'price':'.51','size':'1000'}]
    q=quote_budget(b,'1',.50,.07)
    assert len(q['levels'])==2 and max(x['price'] for x in q['levels'])<=.50
    with pytest.raises(ValueError):quote_budget(b,'1',.49,.07)

@pytest.mark.parametrize('bad',['thin','token','crossed','nan','minimum'])
def test_bad_books_abstain(bad):
    b=book()
    if bad=='thin':b['asks'][0]['size']='1'
    elif bad=='token':b['asset_id']='2'
    elif bad=='crossed':b['bids'][0]['price']='.5'
    elif bad=='nan':b['asks'][0]['size']='nan'
    else:b['min_order_size']='100'
    with pytest.raises(ValueError):quote_budget(b,'1',.505,.07)

@pytest.mark.parametrize('rate',[float('nan'),-.07,1.])
def test_invalid_fees(rate):
    with pytest.raises(ValueError):quote_budget(book(),'1',.505,rate)

def test_ledger_cash_pending_and_idempotent_settlement():
    l=Ledger();q=quote_budget(book(),'1',.505,.07)
    l.open('a','Up',q,{'start':0},1)
    assert l.cash==pytest.approx(50-q['cost_usd'])
    assert l.report()['total_net_pnl_after_all_costs'] is None
    assert l.report()['settled_trades']==0
    with pytest.raises(ValueError):l.open('b','Down',q,{},2)
    p=l.settle('a','Up',3)
    assert l.cash==pytest.approx(50+q['shares']-q['cost_usd'])
    assert p['source_match_status']=='PENDING'
    assert l.report()['source_verified_settled_trades']==0
    old=l.cash;assert l.settle('a','Up',4) is None;assert l.cash==old
    with pytest.raises(ValueError):l.open('a','Up',q,{},5)
    l.open('b','Down',q,{},5);l.settle('b','Up',6)
    assert l.report()['max_drawdown_settled_equity_usd']==pytest.approx(q['cost_usd'])
    assert l.report()['actual_orders']==l.report()['actual_fills']==0

def push(f,kind,t,p,received=None):
    f.ingest({'topic':'crypto_prices_chainlink' if kind=='spot' else 'crypto_prices_twap_sixty',
        'payload':{'symbol':'btc/usd','timestamp':t*1000,'value':p,'window_s':60}},t+1 if received is None else received)

def good_feed(monkeypatch):
    start=1000;now=1180;monkeypatch.setattr('pairbot.unified_paper.time.time',lambda:now)
    f=Feed(Log());push(f,'twap',start,100)
    for t in range(1060,1180):push(f,'spot',t,100+(.01 if t%2 else -.01))
    return f,start,now

def test_matched_feed_causal_probability(monkeypatch):
    f,start,now=good_feed(monkeypatch);x=f.features(start,now,load_policy())
    assert 0<x['p_up']<1 and x['intervals']>=60 and x['opening']['timestamp']==start

@pytest.mark.parametrize('bad',['late_opening','no_spot','gap','stale','revision','reconnect'])
def test_feed_rejects_gap_backfill_and_revisions(monkeypatch,bad):
    f,start,now=good_feed(monkeypatch)
    if bad=='late_opening':f.series['twap'][start]['received_at']=start+5
    elif bad=='no_spot':f.series['spot']={}
    elif bad=='gap':
        for t in range(1100,1104):del f.series['spot'][t]
    elif bad=='stale':
        for t in range(1177,1180):del f.series['spot'][t]
    elif bad=='revision':push(f,'spot',1179,200,now)
    else:f.generation+=1
    with pytest.raises(ValueError):f.features(start,now,load_policy())

def test_generic_snapshot_is_not_chainlink_and_future_nan_rejected():
    f=Feed(Log());f.ingest({'topic':'crypto_prices','payload':{'symbol':'btc/usd','timestamp':1000,'value':100}},2)
    assert not f.series['spot']
    push(f,'spot',100,100,1);push(f,'spot',1,float('nan'),2)
    assert not f.series['spot']

def test_policy_frozen_one_group_and_four_hours():
    p=load_policy();assert p['duration_seconds']==14400 and p['comparison_groups']==1
    assert p['bankroll_usd']==50 and p['trade_budget_usd_including_taker_fee']==5
    assert p['other_costs_usd'] is None and p['old_protocol_effect']=='NONE'
