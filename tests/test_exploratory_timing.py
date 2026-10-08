import json
from pathlib import Path
from pairbot.exploratory_timing import analyze, bucket, clusters, bad_clock_intervals
from .test_engine import M, T, book


def test_bucket_edges_and_late_clusters():
    assert bucket(-5,[-10,-5,0,5])=='[-5,0)'
    xs=[{'received':t,'exchange':t-7,'age':7,'kind':'book'} for t in (100,100.05,101)]
    result=clusters(xs,.1)
    assert [r['n'] for r in result]==[2,1]
    assert result[0]['receive_span']>0


def test_clock_quality_applies_only_to_following_windows():
    probes=[{'ts':5,'uncertainty_seconds':.5},
            {'ts':10,'uncertainty_seconds':2},
            {'ts':20,'uncertainty_seconds':.2}]
    assert list(bad_clock_intervals(0,30,probes,1))==[(0,5),(10,20)]
    assert list(bad_clock_intervals(0,30,[],1))==[(0,30)]


def test_midmarket_gap_and_fixed_clean_window_guards(tmp_path):
    rows=[{'type':'research_header','ts':T},
          {'type':'market','ts':T,'market':M.data()},
          {'type':'metadata','ts':T,'condition_id':M.condition_id},
          {'type':'clock','ts':T,'uncertainty_seconds':.5}]
    for i in range(30):
        if i==15:rows.append({'type':'gap','ts':T+i,'source':'Polymarket','reason':'fixture 1000'})
        if i==16:rows.append({'type':'metadata','ts':T+i,'condition_id':M.condition_id})
        for token in M.tokens:
            rows.append({'type':'frame','ts':T+i,'received_at':T+i,'socket_monotonic_ns':i*1000000000,
                         'message':book(token,T+i)})
    rows.append({'type':'stop','ts':T+30})
    source=tmp_path/'fixture.jsonl'
    source.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    policy=Path('docs/research/exploratory-diagnostic-policy.json')
    result=analyze(source,policy,'fixture freeze identifier only')
    assert result['gaps'][0]['close_minus_market_end']==-285
    assert result['gaps'][0]['next_subscription_proxy_delay']==1
    assert result['gaps'][0]['next_market_subscription_delay'] is None
    assert result['quality']['full_seconds']==30
    assert result['quality']['clean_seconds']==4
    assert result['cohorts'][0]['frames']==60
    assert result['cohorts'][1]['frames']==8
    assert result['threshold_sets_tried']==1
    assert result['repairs_applied'] is False and result['old_holdout_opened'] is False
    assert result==analyze(source,policy,'fixture freeze identifier only')
