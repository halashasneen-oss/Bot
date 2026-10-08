import json
from pairbot.research_diagnostic import diagnose


def test_transport_counts_use_socket_receipt_without_pnl_or_time_correction(tmp_path):
    rows=[{'type':'market','ts':100,'market':{'condition_id':'c'}},
          {'type':'frame','ts':112,'received_at':110,
           'message':{'event_type':'book','timestamp':103}},
          {'type':'socket_control','ts':110,'direction':'sent','value':'PING','socket_monotonic_ns':10000000000},
          {'type':'socket_control','ts':110.1,'value':'PONG'},
          {'type':'socket_control','ts':120,'direction':'sent','value':'PING','socket_monotonic_ns':20000000000},
          {'type':'source_error','ts':121,'source':'Binance','error':'HTTP 451'},
          {'type':'gap','ts':122,'source':'Polymarket','reason':'fixture'}]
    path=tmp_path/'fixture.jsonl'
    path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    report=diagnose(path)
    assert report['raw_receipt_minus_exchange_over_5s']=={'book':1}
    assert report['controls']=={'sent:PING':2,'received:PONG':1}
    assert report['heartbeat_interval_seconds']=={'n':1,'min':10,'max':10}
    assert report['source_errors'][0]['source']=='Binance'
    assert len(report['gaps'])==1
    assert report['strategy_evaluation'] is False and report['profitability_claim'] is False
    assert report==diagnose(path)


def test_transport_and_buffer_delays_are_reported_separately(tmp_path):
    rows=[{'type':'socket_lifecycle','phase':'opened','ts':100,'connection_id':1},
          {'type':'frame','ts':103,'received_at':103,'socket_received_at':101,
           'socket_monotonic_ns':1000000000,'application_received_monotonic_ns':3000000000,
           'socket_timestamp_scope':'COMPLETING_ASYNCIO_TRANSPORT_CHUNK',
           'message':{'event_type':'book','timestamp':100}}]
    source=tmp_path/'timing.jsonl';source.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    r=diagnose(source)
    assert r['latency']['book']['p50']==3
    assert r['transport_receipt_minus_exchange']['book']['p50']==1
    assert r['websocket_buffer_wait_seconds']['book']['p50']==2
    assert r['socket_lifecycle'][0]['phase']=='opened'
