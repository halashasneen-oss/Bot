import datetime as dt
import hashlib
import json
import math
import pytest
from pairbot.daily_results import final_label,collect
from pairbot.daily_evaluate import evaluate,block_lower
from pairbot.daily_recorder import Journal,day_times
from pairbot.daily_model import POLICY_SHA256,FREEZE_COMMIT,load_policy

DAY='2026-10-06';CID='0x'+'b'*64

def market():return {'conditionId':CID,'outcomes':'["No","Yes"]','clobTokenIds':'["22","11"]'}

def state(day=DAY):
    _,target,_=day_times(day)
    return {'condition_id':CID,'status':'resolved','resolved_at':dt.datetime.fromtimestamp(target+7200,dt.timezone.utc).isoformat(),
            'resolved_block':1,'payouts':[0,1]}


def test_final_label_outcome_order():
    _,target,_=day_times(DAY)
    assert final_label(state(),market(),target,target+8000)==1
    s=state();s['payouts']=[1,0]
    assert final_label(s,market(),target,target+8000)==0

@pytest.mark.parametrize('case',['pending','wrong_condition','future','early','tie','nan','missing_block','missing_date'])
def test_nonfinal_and_ambiguous_not_labels(case):
    s=state();_,target,_=day_times(DAY)
    if case=='pending':s['status']='closed'
    if case=='wrong_condition':s['condition_id']='0x'+'c'*64
    if case=='future':s['resolved_at']=dt.datetime.fromtimestamp(target+9000,dt.timezone.utc).isoformat()
    if case=='early':s['resolved_at']=dt.datetime.fromtimestamp(target-1,dt.timezone.utc).isoformat()
    if case=='tie':s['payouts']=[1,1]
    if case=='nan':s['payouts']=[0,float('nan')]
    if case=='missing_block':s.pop('resolved_block')
    if case=='missing_date':s.pop('resolved_at')
    assert final_label(s,market(),target,target+8000)is None


def fixture(root,day,clean=True):
    folder=root/day;folder.mkdir(parents=True)
    _,target,_=day_times(day);path=folder/'journal.jsonl';j=Journal(path)
    j.append({'type':'header','mode':'SYNTHETIC_TEST_ONLY','day':day,'nominal_target':target,'policy_sha256':POLICY_SHA256,'freeze_commit':FREEZE_COMMIT})
    j.append({'type':'universe','markets':[market()]})
    j.append({'type':'forecast','day':day,'condition_id':CID,'p_yes':.8,'benchmark_yes_midpoint':.5,'clean_point':clean,
              'created_at':day_times(day)[0]+10,'financial_action':'ABSTAIN','net_payoff':None,'fills':0})
    j.append({'type':'sealed'});j.close()
    evidence={'day':day,'policy_sha256':POLICY_SHA256,'journal_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
              'evidence':[{'url':'https://data-api.polymarket.com/v2/resolutions','status':200,'received_at':target+8000,
                           'payload':{'data':[state(day)]}}]}
    (folder/'resolution-1.json').write_text(json.dumps(evidence))
    return path


def test_no_interim_evaluation_or_old_cohort_root(tmp_path):
    with pytest.raises(ValueError):evaluate(tmp_path/'daily-prospective',asof=day_times(DAY)[1])
    with pytest.raises(ValueError):evaluate(tmp_path/'legacy-holdout',asof=1e10)


def test_full_clean_and_missing_days_remain_visible(tmp_path):
    root=tmp_path/'daily-prospective';root.mkdir()
    fixture(root,DAY);fixture(root,'2026-10-07',clean=False)
    r=evaluate(root,asof=1e10,synthetic=True)
    assert r['cohort_days']==90 and r['scored_days']==2 and r['clean_days']==1
    assert math.isclose(r['full']['mean_day_brier_improvement'],.21)
    assert r['excluded_point_fraction']==.5 and r['missing_day_fraction']==88/90
    assert r['decision']=='NO_GO' and r['net_payoff']is None and r['selected_resolved_days']==0
    assert r['mode']=='SYNTHETIC_ONLY' and r['excluded_time_fraction']is None


def test_resolution_binding_and_conflicts_rejected(tmp_path):
    root=tmp_path/'daily-prospective';root.mkdir();fixture(root,DAY)
    path=root/DAY/'resolution-1.json';r=json.loads(path.read_text());r['journal_sha256']='wrong';path.write_text(json.dumps(r))
    with pytest.raises(ValueError):evaluate(root,asof=1e10,synthetic=True)


def test_seed_and_blocks_deterministic():
    policy=load_policy();series=[None,.2,-.1]*30
    assert block_lower(series,policy)==block_lower(series,policy)
    assert block_lower([None]*90,policy)is None


def test_collect_never_queries_before_target(tmp_path,monkeypatch):
    import asyncio
    from pairbot import daily_results as module
    root=tmp_path/'daily-prospective';root.mkdir();path=fixture(root,DAY)
    monkeypatch.setattr(module.time,'time',lambda:day_times(DAY)[1]-1)
    with pytest.raises(ValueError):asyncio.run(collect(path,tmp_path/'result.json'))
    assert not (tmp_path/'result.json').exists()


def test_launcher_disarmed_never_records(tmp_path,monkeypatch):
    from pairbot import daily_launch as module
    (tmp_path/'config').mkdir();(tmp_path/'config/daily-launch.json').write_text('{"enabled":false}')
    monkeypatch.chdir(tmp_path);module.main()


def test_archive_rejects_synthetic_forecast(tmp_path,monkeypatch):
    import zipfile
    from pairbot.daily_archive import hydrate
    root=tmp_path/'fixtures/daily-prospective';root.mkdir(parents=True);path=fixture(root,DAY)
    captures=tmp_path/'docs/daily/captures';captures.mkdir(parents=True)
    with zipfile.ZipFile(captures/'sample.zip','w')as z:z.writestr(DAY+'/journal.jsonl',path.read_bytes())
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):hydrate(tmp_path/'history')


def test_launcher_refuses_unbounded_early_wait(tmp_path,monkeypatch):
    from pairbot import daily_launch as module
    (tmp_path/'config').mkdir();(tmp_path/'config/daily-launch.json').write_text(json.dumps({'enabled':True,'day':DAY}))
    monkeypatch.chdir(tmp_path);monkeypatch.setattr(module.time,'time',lambda:day_times(DAY)[0]-1000)
    with pytest.raises(ValueError):module.main()


def test_real_forged_forecast_cannot_score_without_inputs(tmp_path):
    from pairbot.daily_recorder import canonical
    root=tmp_path/'daily-prospective';root.mkdir();path=fixture(root,DAY)
    raw=[json.loads(x)for x in path.read_text().splitlines()]
    payloads=[x['payload']for x in raw];payloads[0]['mode']='REAL_PUBLIC_RECORD_ONLY'
    path.unlink();j=Journal(path)
    for row in payloads:j.append(row)
    j.close()
    evidence=root/DAY/'resolution-1.json';r=json.loads(evidence.read_text());r['journal_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();evidence.write_text(json.dumps(r))
    with pytest.raises((ValueError,StopIteration)):evaluate(root,asof=1e10)
