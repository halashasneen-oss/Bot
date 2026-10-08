"""Validated paper-only settings. No credentials or live mode exist."""
from dataclasses import dataclass, asdict
from pathlib import Path
import math
import tomllib


@dataclass(frozen=True)
class Config:
    bankroll: float = 50.0
    order_size: float = 5.0
    max_inventory_usd: float = 15.0
    loss_stop_usd: float = 2.5
    min_pair_edge: float = 0.02
    maker_fee_bps: float = 0.0
    merge_cost_usd: float = 0.0
    merge_delay_seconds: float = 3.0
    order_latency_seconds: float = 0.5
    cancel_latency_seconds: float = 0.5
    quote_lifetime_seconds: float = 10.0
    max_unpaired_seconds: float = 20.0
    stale_seconds: float = 5.0
    stop_before_close_seconds: float = 20.0
    kill_move: float = 0.03
    kill_window_seconds: float = 60.0
    movement_cooldown_seconds: float = 15.0
    movement_stability_seconds: float = 5.0
    kill_spread: float = 0.10
    queue_multiplier: float = 1.5
    duration_minutes: float = 60.0
    # Research switches: default execution is unchanged.
    guard_mode: str = 'up_mid'
    guard_warmup_seconds: float = 0.0
    trade_volume_multiplier: float = 1.0
    complementary_fills: bool = False

    def __post_init__(self):
        if self.guard_mode not in {'up_mid', 'combined_mid', 'combined_bid'}:
            raise ValueError('unknown guard_mode')
        if type(self.complementary_fills) is not bool:
            raise ValueError('complementary_fills must be boolean')
        if self.complementary_fills:
            raise ValueError('complementary_fills unavailable: server matching not proven')
        for key, value in asdict(self).items():
            if key in {'guard_mode', 'complementary_fills'}:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f'{key}: finite number required')
            if value < 0:
                raise ValueError(f'{key}: cannot be negative')
        for key in ('bankroll', 'order_size', 'max_inventory_usd', 'loss_stop_usd',
                    'stale_seconds', 'duration_minutes', 'quote_lifetime_seconds',
                    'max_unpaired_seconds', 'kill_window_seconds', 'movement_cooldown_seconds', 'movement_stability_seconds'):
            if getattr(self, key) <= 0:
                raise ValueError(f'{key}: must be positive')
        if not 0 < self.min_pair_edge < 1 or not 0 < self.kill_move < 1 or not 0 < self.kill_spread < 1:
            raise ValueError('edge and kill thresholds must be in (0,1)')
        if self.queue_multiplier < 1 or self.max_inventory_usd > self.bankroll:
            raise ValueError('queue multiplier >= 1; inventory cap <= bankroll')
        if self.stop_before_close_seconds >= 300 or self.maker_fee_bps > 10000:
            raise ValueError('invalid close buffer or fee')


def load(path=None):
    return Config(**tomllib.loads(Path(path).read_text())) if path else Config()

