import asyncio
import pytest
from pairbot import one_hour_paper as runner


def test_one_hour_plan_and_original_strategy_are_both_frozen():
    plan = runner.load_plan(); policy = runner.load_policy()
    assert plan['duration_seconds'] == 3600
    assert policy['duration_seconds'] == 14400
    assert plan['strategy_policy_sha256'] == runner.POLICY_SHA
    assert policy['trade_budget_usd_including_taker_fee'] == plan['trade_budget_usd_including_entry_fee'] == 5


def test_short_runner_passes_duration_without_mutating_policy(monkeypatch):
    seen = {}
    async def session(output, policy, **kwargs):
        seen.update(output=output, policy=policy, **kwargs)
    monkeypatch.setattr(runner, 'session', session)
    asyncio.run(runner.run())
    assert seen['duration_seconds'] == 3600
    assert seen['policy']['duration_seconds'] == 14400
    assert seen['output'] == 'runs/unified-one-hour'
    assert seen['execution_plan']['freeze_commit'] == runner.PLAN_FREEZE
    assert not seen['execution_plan']['original_technical_gate_passed']
