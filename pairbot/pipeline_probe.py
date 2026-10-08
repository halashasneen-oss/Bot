"""Offline synthetic I/O obstruction probe; no real connection or market data."""
import argparse
import asyncio
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
import time
import types

from .config import Config
from .engine import Engine
from .feed import Journal, collect
from .market import Market


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--original-feed',required=True)
    p.add_argument('--out',required=True)
    args=p.parse_args()
    out=Path(args.out);out.mkdir(parents=True,exist_ok=False)
    module=types.ModuleType('pairbot.original_feed');module.__package__='pairbot'
    sys.modules[module.__name__]=module
    exec(compile(Path(args.original_feed).read_text(),args.original_feed,'exec'),module.__dict__)
    results={}
    for label,collector in [('before',module.collect),('after',collect)]:
        origin=time.monotonic();t=1800000060.
        clock=lambda:t+time.monotonic()-origin
        reads=[];io=[]
        m=Market('btc-updown-5m-1800000000','test','up','down',1800000000.,1800000300.,.01,5)
        class API:
            async def discover(self,now):return m
            async def resolved(self,m):return None
            async def close(self):pass
        class Socket:
            i=0
            async def send(self,wire):pass
            async def recv(self):
                await asyncio.sleep(.002)
                self.i+=1;reads.append(time.monotonic())
                return json.dumps({'event_type':'book','market':'test',
                    'asset_id':'up' if self.i%2 else 'down','timestamp':clock()*1000,
                    'bids':[{'price':'.48','size':'10'}],'asks':[{'price':'.52','size':'10'}]})
        class Connection:
            async def __aenter__(self):self.ws=Socket();return self.ws
            async def __aexit__(self,*a):pass
        def delayed_report(*args):
            a=time.monotonic();time.sleep(.12);io.append((a,time.monotonic()))
        engine=Engine(replace(Config(),cancel_latency_seconds=0))
        engine.source='SYNTHETIC_TEST'
        j=Journal(out/label)
        j.write('header',0,schema=1,config=asdict(engine.cfg),source='SYNTHETIC_TEST')
        kw={'checkpoint':delayed_report} if label=='before' else {'snapshot_checkpoint':delayed_report}
        asyncio.run(collector(engine,j,.1,record_only=True,api=API(),connector=lambda *a,**kw:Connection(),wall_clock=clock,**kw))
        j.close()
        results[label]={'reads_during_deliberately_slow_report':sum(any(a<ts<b for a,b in io) for ts in reads),
                        'frames':len(reads),'slow_report_calls':len(io),
                        'pipeline_telemetry':engine.run_info.get('pipeline_telemetry'),
                        'note':'SYNTHETIC_TEST / offline fixture / deliberately imposed 120 ms report delay; not real-market performance'}
    assert results['before']['reads_during_deliberately_slow_report']==0
    assert results['after']['reads_during_deliberately_slow_report']>0
    (out/'comparison.json').write_text(json.dumps(results,indent=2)+'\n')
    print(json.dumps(results,indent=2))


if __name__=='__main__':main()
