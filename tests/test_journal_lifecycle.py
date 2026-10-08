import asyncio
import json
import pytest
from pairbot.directional import Journal, read_journal
from pairbot.journal_lifecycle import stop_producers


def closing_feed(journal):
    async def feed():
        try:
            await asyncio.Event().wait()
        finally:
            journal.write({'type': 'feed_close', 'code': 1000})
    return feed()


def test_producer_cleanup_precedes_terminal_record(tmp_path):
    async def check():
        path = tmp_path / 'journal.jsonl'
        journal = Journal(path, {'actual_orders': 0})
        task = asyncio.create_task(closing_feed(journal))
        await asyncio.sleep(0)
        await stop_producers([task])
        journal.write({'type': 'complete'})
        await stop_producers([task])  # finally cleanup is idempotent
        journal.close()
        assert [r['type'] for r in read_journal(path)] == ['directional_header', 'feed_close', 'complete']
    asyncio.run(check())


def test_shutdown_failure_cannot_be_sealed_as_complete(tmp_path):
    async def failing():
        try:
            await asyncio.Event().wait()
        finally:
            raise ValueError('cleanup failed')
    async def check():
        task = asyncio.create_task(failing())
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match='producer failed'):
            await stop_producers([task])
    asyncio.run(check())


def test_strict_reader_still_rejects_old_close_after_complete(tmp_path):
    path = tmp_path / 'journal.jsonl'
    journal = Journal(path, {})
    journal.write({'type': 'complete'})
    journal.write({'type': 'feed_close'})
    journal.close()
    with pytest.raises(ValueError, match='Incomplete'):
        list(read_journal(path))


def test_actual_paper_runner_shutdown_and_declared_duration(tmp_path, monkeypatch):
    import pairbot.unified_paper as runner
    class Feed:
        def __init__(self, journal): self.journal = journal
        async def run(self): await closing_feed(self.journal)
    monkeypatch.setattr(runner, 'Feed', Feed)
    monkeypatch.setattr(runner, 'diagnostic', lambda p: {})
    policy = runner.load_policy()
    policy['drain_seconds'] = 0  # synthetic fast lifecycle test, not a live policy
    path = tmp_path / 'paper'
    asyncio.run(runner.session(path, policy, duration_seconds=1, execution_plan={'synthetic': True}))
    rows = list(read_journal(path / 'journal.jsonl'))
    summary = json.loads((path / 'summary.json').read_text())
    assert rows[-1]['type'] == 'complete'
    assert next(i for i,r in enumerate(rows) if r['type']=='feed_close') < len(rows)-1
    assert rows[0]['planned_duration_seconds'] == summary['planned_duration_seconds'] == 1
    assert summary['elapsed_monotonic_seconds'] >= 1
    assert summary['complete'] and summary['ledger']['actual_orders'] == 0


def test_actual_preflight_shutdown_seals_last(tmp_path, monkeypatch):
    import pairbot.reference_preflight as runner
    class Feed:
        def __init__(self, journal): self.journal=journal; self.series={'spot':{},'twap':{}}; self.generation=1
        async def run(self): await closing_feed(self.journal)
    p = runner.load_transport_policy(); p['preflight_duration_seconds']=1
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(runner, 'load_transport_policy', lambda: p)
    monkeypatch.setattr(runner, 'Feed', Feed)
    monkeypatch.setattr(runner, 'diagnostic', lambda p: {})
    result = asyncio.run(runner.run())
    rows = list(read_journal(tmp_path / 'runs/reference-preflight/journal.jsonl'))
    assert rows[-2]['type']=='feed_close' and rows[-1]['type']=='complete'
    assert result['complete'] and result['elapsed_monotonic_seconds']>=1
    assert result['actual_orders']==result['paper_fills']==0

