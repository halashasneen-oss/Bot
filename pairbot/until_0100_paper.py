"""One explicit frozen-deadline PAPER run; no account/order APIs."""
import asyncio
import hashlib
import json
from pathlib import Path
from .unified_paper import session, load_policy, POLICY_SHA
from .reference_transport import load_transport_policy, POLICY_SHA as TRANSPORT_SHA

PLAN_SHA = '7c218b0f68972b94d3fa1b043d1caeb7314b41032218aa0fc93589a255349915'
PLAN_FREEZE = 'fb9a547b54e882ebd9a06d1c8ce6e7c573f2c9c1'


def load_plan():
    raw = (Path(__file__).resolve().parents[1] / 'docs/unified/until-0100-plan.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != PLAN_SHA:
        raise ValueError('Frozen deadline execution plan changed')
    plan = json.loads(raw)
    if plan['strategy_policy_sha256'] != POLICY_SHA or plan['transport_policy_sha256'] != TRANSPORT_SHA:
        raise ValueError('Deadline plan policy mismatch')
    if plan['stop_entry_at_epoch'] != 1791237600 or plan['maximum_resolution_drain_seconds'] != 600:
        raise ValueError('Invalid frozen deadline or drain')
    return plan


async def run():
    plan = load_plan(); policy = load_policy(); load_transport_policy()
    if policy['drain_seconds'] != plan['maximum_resolution_drain_seconds']:
        raise ValueError('Frozen settlement drain changed')
    await session('runs/unified-until-0100', policy, stop_at=plan['stop_entry_at_epoch'],
                  execution_plan={'sha256': PLAN_SHA, 'freeze_commit': PLAN_FREEZE,
                                  'original_policy_duration_seconds': policy['duration_seconds'],
                                  'original_technical_gate_passed': False,
                                  'user_authorized_fixed_deadline_session': True,
                                  'stop_entry_at_utc': plan['stop_entry_at_utc'],
                                  'stop_entry_at_amman': plan['stop_entry_at_amman']})


if __name__ == '__main__':
    asyncio.run(run())
