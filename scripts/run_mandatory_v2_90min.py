"""Run Mandatory v2 PAPER for 90 active minutes (18 complete BTC 5m windows).

RTDS warms up before the first aligned window. After the 90-minute entry
interval, a short settlement-only grace is allowed; no extra trade is opened.
"""
import asyncio
import time

from pairbot.mandatory_v2 import (
    DEFAULT_SETTLEMENT_GRACE_SEC, next_full_window_start, session,
)

DURATION_SECONDS = 90 * 60

if __name__ == "__main__":
    launched = time.time()
    first_window = next_full_window_start(launched)
    asyncio.run(session(
        "config-mandatory-v2.yaml",
        "runs/mandatory-v2-90min",
        stop_at=first_window + DURATION_SECONDS,
        entry_start_at=first_window,
        settlement_grace_sec=DEFAULT_SETTLEMENT_GRACE_SEC,
    ))
