"""Run Mandatory v3 FINAL PAPER for 60 active minutes.

RTDS warms up before the first aligned BTC 5-minute window. The entry period
covers exactly 12 complete windows. A short settlement-only grace follows.
"""
import asyncio
import time

from pairbot.mandatory_v3 import (
    DEFAULT_SETTLEMENT_GRACE_SEC, next_full_window_start, session,
)

DURATION_SECONDS = 60 * 60

if __name__ == "__main__":
    launched = time.time()
    first_window = next_full_window_start(launched)
    asyncio.run(session(
        "config-mandatory-v3.yaml",
        "runs/mandatory-v3-60min",
        stop_at=first_window + DURATION_SECONDS,
        entry_start_at=first_window,
        settlement_grace_sec=DEFAULT_SETTLEMENT_GRACE_SEC,
    ))
