import asyncio
import json
import time
import pytest
from pairbot import until_0100_paper as runner
from pairbot import unified_paper
from pairbot.directional import read_journal


def test_frozen_deadline_is_one_am_amman_not_one_pm():
    from datetime import datetime,timezone,timedelta
    plan=runner.load_plan()
    assert datetime.fromtimestamp(plan['stop_entry_at_epoch'],timezone(timedelta(hours=3))).isoformat()=='2026-10-06T01:00:00+03:00'
    assert runner.load_policy()['duration_seconds']==14400


def test_deadline_wrapper_keeps_strategy_unchanged(monkeypatch):
    seen={}
    async def session(output,policy,**kwargs):seen.update(output=output,policy=policy,**kwargs)
    monkeypatch.setattr(runner,'session',session)
    asyncio.run(runner.run())
    assert seen['stop_at']==1791237600 and 'duration_seconds' not in seen
    assert seen['policy']==runner.load_policy()
    assert seen['execution_plan']['freeze_commit']==runner.PLAN_FREEZE


@pytest.mark.parametrize('deadline',[0,float('nan'),float('inf'),True])
def test_invalid_deadline_does_not_create_output(tmp_path,deadline):
    path=tmp_path/'invalid'
    with pytest.raises(ValueError,match='deadline'):
        asyncio.run(unified_paper.session(path,runner.load_policy(),stop_at=deadline))
    assert not path.exists()


def test_actual_runner_stops_at_absolute_deadline(tmp_path,monkeypatch):
    class Feed:
        def __init__(self,journal):self.journal=journal
        async def run(self):
            try:await asyncio.Event().wait()
            finally:self.journal.write({'type':'feed_close','code':1000})
    monkeypatch.setattr(unified_paper,'Feed',Feed)
    monkeypatch.setattr(unified_paper,'diagnostic',lambda p:{})
    p=runner.load_policy();p['drain_seconds']=0  # synthetic lifecycle test only
    deadline=time.time()+1
    out=tmp_path/'absolute'
    asyncio.run(unified_paper.session(out,p,stop_at=deadline))
    s=json.loads((out/'summary.json').read_text());rows=list(read_journal(out/'journal.jsonl'))
    assert s['stop_entry_at']==rows[0]['stop_entry_at']==deadline
    assert abs(s['started_at']+s['planned_duration_seconds']-deadline)<1e-6
    assert deadline<=rows[-1]['finished_at']<deadline+1
    assert s['ledger']['actual_orders']==s['ledger']['actual_fills']==0

