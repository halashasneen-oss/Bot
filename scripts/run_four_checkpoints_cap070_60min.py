"""PAPER-only four checkpoints, maximum effective entry 0.70; no real orders."""
import asyncio
import time
from pairbot.mandatory_v3 import DEFAULT_SETTLEMENT_GRACE_SEC, next_full_window_start, session

if __name__ == "__main__":
    first = next_full_window_start(time.time())
    asyncio.run(session("config-experiment-four-checkpoints-cap070-paper.yaml",
                        "runs/four-checkpoints-cap070-60min",
                        stop_at=first + 60 * 60, entry_start_at=first,
                        settlement_grace_sec=DEFAULT_SETTLEMENT_GRACE_SEC))
