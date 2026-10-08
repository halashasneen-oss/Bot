"""Run Mandatory v2 PAPER from the first complete window until 19:00 Amman."""
import asyncio
import datetime as dt
import time
from zoneinfo import ZoneInfo

from pairbot.mandatory_v2 import (
    DEFAULT_SETTLEMENT_GRACE_SEC, next_full_window_start, session,
)

DEADLINE = dt.datetime(2026, 10, 6, 19, 0, 0, tzinfo=ZoneInfo("Asia/Amman")).timestamp()

if __name__ == "__main__":
    launched = time.time()
    if launched >= DEADLINE:
        raise RuntimeError("19:00 Asia/Amman deadline already elapsed")
    first_window = next_full_window_start(launched)
    if first_window >= DEADLINE:
        raise RuntimeError("No complete 5-minute window remains before deadline")
    asyncio.run(session(
        "config-mandatory-v2.yaml",
        "runs/mandatory-v2-until-1900",
        stop_at=DEADLINE,
        entry_start_at=first_window,
        settlement_grace_sec=DEFAULT_SETTLEMENT_GRACE_SEC,
    ))
