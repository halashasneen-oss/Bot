import hashlib
import json
from pathlib import Path
import pytest
from pairbot.config import Config
from pairbot.engine import Engine
from pairbot.exploratory_opportunities import analyze, visible_candidate, walk
from pairbot.research_stats import FeeSchedule
from pairbot.research import BoundedMeasurements, input_hash
from .test_engine import M,T,book

POLICY=Path('docs/research/exploratory-diagnostic-policy.json')
SPEC=json.loads(POLICY.read_text())['opportunities']
FEE=FeeSchedule(.07,provenance='unit fixture only')


def engine(ask=.45,bid=.55):
    e=Engine(Config());e.record_only=True;e.measurements=BoundedMeasurements();e.select(M,T)
    for token in M.tokens:e.frame(book(token,T,bid=bid,ask=ask,size=10),T)
    return e


def test_fee_depth_walk_and_unknown_cost_are_separate():
    e=engine(ask=.45,bid=.44)
    row=visible_candidate(e,FEE,'taker_pair',SPEC)
    assert row['quote_qualifies'] and row['walked_depth_and_edge_qualifies']
    assert row['walked_net_before_operation_cost']==pytest.approx(.32674)
    assert row['net_after_all_costs'] is None and row['operation_cost'] is None
    assert visible_candidate(e,None,'taker_pair',SPEC) is None
    e.books[M.up].asks.clear();e.books[M.up].asks[.45]=1;e.books[M.up].asks[.7]=4
    row=visible_candidate(e,FEE,'taker_pair',SPEC)
    assert row['quote_qualifies'] and not row['walked_depth_and_edge_qualifies']
    e.books[M.up].asks.clear()
    assert visible_candidate(e,FEE,'taker_pair',SPEC) is None


def test_mint_sell_and_missing_depth_no_assumed_fill():
    e=engine(ask=.57,bid=.56)
    row=visible_candidate(e,FEE,'mint_sell',SPEC)
    assert row['quote_qualifies'] and row['walked_depth_and_edge_qualifies']
    e.books[M.up].bids.clear();e.books[M.up].bids[.56]=2
    assert walk(e.books[M.up],5,FEE,True) is None
    row=visible_candidate(e,FEE,'mint_sell',SPEC)
    assert row['quote_qualifies'] and not row['walked_depth_and_edge_qualifies']
    assert row['walked_net_before_operation_cost'] is None
    e.gap(T+1)
    assert visible_candidate(e,FEE,'mint_sell',SPEC) is None


def fixture(tmp_path,clean=True):
    raw={'feesEnabled':True,'feeSchedule':{'rate':.07,'exponent':1,'takerOnly':True},'conditionId':M.condition_id}
    rows=[{'type':'research_header','ts':T},{'type':'market','ts':T,'market':M.data()},
          {'type':'metadata','ts':T,'condition_id':M.condition_id,'raw':raw}]
    for token in M.tokens:rows.append({'type':'frame','ts':T,'message':book(token,T,bid=.44,ask=.45,size=10)})
    rows.append({'type':'gap','ts':T+1,'source':'Polymarket'})
    rows.append({'type':'tick','ts':T+2})
    # One snapshot after a gap must not reconstruct the missing second leg.
    rows.append({'type':'frame','ts':T+2,'message':book(M.up,T+2,bid=.44,ask=.45,size=10)})
    rows.append({'type':'stop','ts':T+3})
    source=tmp_path/'input.jsonl';source.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    quality={'start':T,'end':T+3,'freeze_commit':'fixture only','input_sha256':input_hash(source),
             'policy_sha256':hashlib.sha256(POLICY.read_bytes()).hexdigest(),
             'quality':{'clean_bins':[0] if clean else [],'excluded_fraction':2/3 if clean else 1},'numeric_gate_failures':[]}
    mask=tmp_path/'quality.json';mask.write_text(json.dumps(quality))
    return source,mask


def test_interval_integral_clean_mask_and_gap_boundary(tmp_path):
    source,mask=fixture(tmp_path)
    r=analyze(source,POLICY,mask)
    full,clean=r['cohorts']
    assert full['totals']['taker_pair']['observed_seconds']==3
    assert full['totals']['taker_pair']['quote_opportunity_seconds']==1
    assert clean['totals']['taker_pair']['observed_seconds']==1
    assert clean['totals']['taker_pair']['quote_opportunity_seconds']==1
    assert len(full['events'])==1 and full['events'][0]['duration_seconds']==1
    assert full['events'][0]['survives_recorded_latency']
    assert r['fills']==r['orders']==0 and r['net_pnl'] is None
    assert r==analyze(source,POLICY,mask)


def test_empty_clean_sample_and_mismatched_mask(tmp_path):
    source,mask=fixture(tmp_path,False)
    r=analyze(source,POLICY,mask)
    assert r['cohorts'][1]['events']==[]
    assert r['cohorts'][1]['totals']['taker_pair']['observed_seconds']==0
    q=json.loads(mask.read_text());q['input_sha256']='bad';mask.write_text(json.dumps(q))
    with pytest.raises(ValueError,match='mask does not match'):analyze(source,POLICY,mask)


def test_crossed_reconstruction_and_source_quotes_are_disclosed():
    e=engine(ask=.45,bid=.55)
    quotes={token:{'bid':.4,'ask':.6} for token in M.tokens}
    row=visible_candidate(e,FEE,'taker_pair',SPEC,quotes)
    assert row['quote_qualifies']
    assert not row['individual_books_coherent']
    assert not row['reported_touch_matches_reconstruction']
    assert row['net_after_all_costs'] is None


def test_survival_uses_receipt_lag_in_addition_to_half_second():
    from pairbot.exploratory_opportunities import Cohort
    e=engine(ask=.45,bid=.44)
    for token in M.tokens:e.exchange_ts[token]=T-.6
    candidate=visible_candidate(e,FEE,'taker_pair',SPEC)
    cohort=Cohort('fixture',SPEC)
    cohort.segment(T,T+1,M.data(),{'taker_pair':candidate})
    event=cohort.finish()['events'][0]
    assert event['duration_seconds']==1
    assert event['required_duration_seconds']==pytest.approx(1.1,abs=1e-6)
    assert not event['survives_recorded_latency']
