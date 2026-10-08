import copy
import datetime as dt
import json
import pytest
from pairbot.daily_recorder import (RULES,Journal,verify_journal,day_times,audit_market,decision_row,record)
from pairbot.daily_model import POLICY_SHA256

DAY='2026-10-06'
CID='0x'+'a'*64

def inputs():
    decision,target,slug=day_times(DAY);now=decision+10
    market={'slug':'bitcoin-above-84k-on-october-6-2026','question':'Will the price of Bitcoin be above $84,000 on October 6?',
            'conditionId':CID,'endDate':dt.datetime.fromtimestamp(target,dt.timezone.utc).isoformat(),
            'description':RULES,'closed':False,'enableOrderBook':True,'umaResolutionStatus':None,
            'outcomes':'["Yes","No"]','clobTokenIds':'["11","22"]','feesEnabled':True,
            'feeSchedule':{'rate':.07,'exponent':1,'takerOnly':True}}
    end=int(now//3600)*3600
    candles=[[t,83000,85000,84000,84000+(i%2)*100,10]for i,t in enumerate(range(end-25*3600,end,3600))]
    book=lambda token:{'asset_id':token,'bids':[{'price':'.4','size':'5'}],'asks':[{'price':'.6','size':'5'}]}
    fee={'fd':{'r':.07,'e':1,'to':True},'t':[{'t':'11'},{'t':'22'}]}
    clock={'source':'time.cloudflare.com','intervals_consistent':True,'samples':[
        {'ok':True,'observed_at':now-5+i*2,'offset_seconds':0,'uncertainty_seconds':.01,
         'wall_minus_monotonic_elapsed_seconds':0}for i in range(2)]}
    requests=[{'received_at':now-1,'rtt_seconds':.1,'wall_elapsed_seconds':.1,'role':role}
              for role in ('metadata','candles','fee','book','book')]
    return dict(market=market,day=DAY,candles=candles,books={'Yes':book('11'),'No':book('22')},fee=fee,now=now,clock=clock,requests=requests)


def test_prospective_probability_benchmark_and_no_financial_selection():
    r=decision_row(**inputs())
    assert r['clean_point'] and 0<=r['p_yes']<=1 and r['benchmark_yes_midpoint']==.5
    assert r['financial_action']=='ABSTAIN' and r['net_payoff'] is None and r['fills']==0

@pytest.mark.parametrize('case',['known_result','rule_variant','threshold','token','clock_stale','clock_nan','clock_step','skew','future_input','slow','timing_nan','late_decision','missing_candle','missing_provenance'])
def test_causal_and_quality_gates(case):
    a=inputs()
    if case=='known_result':a['market']['umaResolutionStatus']='resolved'
    if case=='rule_variant':a['market']['description']=RULES.replace('12:00','13:00')
    if case=='threshold':a['market']['question']='Bitcoin above 86,000?'
    if case=='token':a['books']['Yes']['asset_id']='22'
    if case=='clock_stale':a['clock']['samples'][0]['observed_at']-=100
    if case=='clock_nan':a['clock']['samples'][0]['offset_seconds']=float('nan')
    if case=='clock_step':a['requests'][0]['wall_elapsed_seconds']+=1
    if case=='skew':a['requests'][-1]['received_at']-=3
    if case=='future_input':a['requests'][0]['received_at']=a['now']+1
    if case=='slow':a['requests'][0]['rtt_seconds']=3
    if case=='timing_nan':a['requests'][0]['rtt_seconds']=float('nan')
    if case=='late_decision':a['now']+=40
    if case=='missing_candle':a['candles'].pop()
    if case=='missing_provenance':a['requests'].pop()
    r=decision_row(**a)
    assert r['p_yes'] is None and not r['clean_point'] and r['financial_action']=='ABSTAIN'


def test_journal_flush_chain_and_no_overwrite(tmp_path):
    path=tmp_path/'journal.jsonl';j=Journal(path)
    j.append({'type':'header','mode':'SYNTHETIC_TEST_ONLY','policy_sha256':POLICY_SHA256})
    j.append(decision_row(**inputs()));j.append({'type':'sealed'});j.close()
    assert len(verify_journal(path))==3
    with pytest.raises(FileExistsError):Journal(path)
    r=[json.loads(x)for x in path.read_text().splitlines()];r[1]['payload']['p_yes']=.99
    path.write_text('\n'.join(json.dumps(x)for x in r)+'\n')
    with pytest.raises(ValueError):verify_journal(path)


def test_cohort_boundaries_and_dst():
    with pytest.raises(ValueError):day_times('2026-10-05')
    with pytest.raises(ValueError):day_times('2027-01-04')
    assert dt.datetime.fromtimestamp(day_times('2026-10-06')[0],dt.timezone.utc).hour==10
    assert dt.datetime.fromtimestamp(day_times('2026-11-02')[0],dt.timezone.utc).hour==11


def test_outside_window_records_missing_without_network(tmp_path,monkeypatch):
    import asyncio
    from pairbot import daily_recorder as module
    decision,_,_=day_times(DAY);monkeypatch.setattr(module.time,'time',lambda:decision+100)
    async def forbidden(*args,**kwargs):raise AssertionError('Network must not backfill')
    monkeypatch.setattr(module.PublicReader,'get',forbidden)
    asyncio.run(record(DAY,tmp_path))
    rows=verify_journal(tmp_path/DAY/'journal.jsonl')
    assert rows[1]['type']=='missing_day' and rows[2]['forecast_count']==0


def test_partial_journal_not_admitted(tmp_path):
    path=tmp_path/'partial.jsonl';j=Journal(path)
    j.append({'type':'header','mode':'SYNTHETIC_TEST_ONLY','policy_sha256':POLICY_SHA256})
    j.append(decision_row(**inputs()));j.close()
    with pytest.raises(ValueError):verify_journal(path)
