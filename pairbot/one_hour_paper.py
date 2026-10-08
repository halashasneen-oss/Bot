"""Explicit user-requested one-hour duration; frozen trading policy unchanged."""
import asyncio
import hashlib
import json
from pathlib import Path
from .unified_paper import session, load_policy, POLICY_SHA
from .reference_transport import load_transport_policy, POLICY_SHA as TRANSPORT_SHA

PLAN_SHA = '891520644e121fd3e34ee66074ba8bf2a6dcc8cc72013c347d03045f59a6010a'
PLAN_FREEZE = '9f65b895b0535de546bc56e0a5c03c74f07d72ef'


def load_plan():
    raw = (Path(__file__).resolve().parents[1] / 'docs/unified/one-hour-plan.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != PLAN_SHA:
        raise ValueError('Frozen one-hour execution plan changed')
    plan = json.loads(raw)
    if plan['strategy_policy_sha256'] != POLICY_SHA or plan['transport_policy_sha256'] != TRANSPORT_SHA:
        raise ValueError('Execution plan policy mismatch')
    if plan['duration_seconds'] != 3600 or plan['maximum_resolution_drain_seconds'] != 600:
        raise ValueError('Invalid one-hour execution duration')
    return plan


async def run():
    plan = load_plan(); policy = load_policy(); load_transport_policy()
    if policy['drain_seconds'] != plan['maximum_resolution_drain_seconds']:
        raise ValueError('Frozen settlement drain changed')
    await session('runs/unified-one-hour', policy, duration_seconds=plan['duration_seconds'],
                  execution_plan={'sha256': PLAN_SHA, 'freeze_commit': PLAN_FREEZE,
                                  'original_policy_duration_seconds': policy['duration_seconds'],
                                  'original_technical_gate_passed': False,
                                  'user_authorized_short_exploratory_session': True})


if __name__ == '__main__':
    asyncio.run(run())
