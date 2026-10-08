"""20-minute PAPER-only BTC 5m T+225s direction experiment."""
import asyncio
import time
from pairbot.mandatory_v3 import DEFAULT_SETTLEMENT_GRACE_SEC, next_full_window_start, session

if __name__ == '__main__':
    first = next_full_window_start(time.time())
    asyncio.run(session('config-fixed-t225-paper.yaml', 'runs/fixed-t225-20min',
                        stop_at=first + 20 * 60, entry_start_at=first,
                        settlement_grace_sec=DEFAULT_SETTLEMENT_GRACE_SEC))
