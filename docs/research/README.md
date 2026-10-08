# Paper-only research

No exchange orders, wallet, signing SDK, private keys or exchange credentials.
The original live defaults and engine are unchanged. New policies are available
only in the separate offline research module.

## Reproduce

From the repository root, Python 3.12 and requirements installed:

    python -m pytest -q
    python -m pairbot.research_capture --seconds 7200 --out runs/new-record-only
    python -m pairbot.research runs/new-record-only --out runs/new-research

Use fresh output directories. Research journals include extra observation events;
use this research command, not the original replay command. Original replay is
unchanged. The manual Record-only strategy research workflow records public data.
Its initial branch-specific push trigger resumes the authorized capture after the
original workspace became disconnected. No workflow sends exchange orders.

An optional independently recorded public BTC price stream can be joined:

    python -m pairbot.research runs/new-record-only --binance-aux-dir data/research-binance-20261004 --out runs/joint-analysis

This source is explicitly POLLED_LATEST_TRADE_NOT_SOCKET_STREAM, timestamps the
connector response locally, and retains the exchange trade time. It is neither
an exhaustive trade tape nor a substitute for the settlement oracle. The direct
Binance feed's HTTP 451 remains recorded. Cross-host clock alignment remains an
acceptance veto until independently established. Auxiliary raw Git blob hashes
are checked, and prices are merged only at their actual receipt times.

## Frozen design

Protocol written and hashed before collecting or evaluating new observations:
2026-10-04 13:12:57 UTC. Protocol commit e1346c07da727f130a96d89fa1e9ec1a41a993ee
preserves the preregistration. Its SHA256 is
5f5ca36da6c9da2400aae1cbfd90ac78446d0ac341bc0fa4c8b818823e7a89eb.

Eight strategy parameter choices, two volatility scales trained only on training
windows; complete five-minute windows split chronologically 50%/25%/25%.
First/last partial windows are excluded by capture timing, before looking at
profits. Final outcomes are never passed to the evaluator while prerequisites
fail. A single-use exclusive durable ledger is independent of report directory.
Preserve the ledger when moving the dataset to another machine.

Power planning: $0.10 target mean/window, assumed $0.50 SD, 80% one-sided power
and family alpha .05/8. Normal planning gives 279 independent test windows and
at least 279 completed pairs in distinct windows. With a 25% holdout this means
1,116 complete windows, or 93 hours before exclusions. This is a planning
assumption, not power established by these observations. Serial dependence may
require more. Seed 20261004, 10,000 bootstrap replicates; both window and
three-window block lower bounds must exceed zero. Drawdown limit remains $2.50.
Queue remains 1.5. Optimistic queue 1.0 is only a sensitivity scenario and cannot
grant GO. No touch, BUY, deletion, or above-limit SELL causes a maker fill.

## Data and economic assumptions

Socket receipt is captured before parsing, with wall time, monotonic socket
timestamp and exchange timestamp. Bounded processing and I/O queues fail
explicitly rather than silently dropping. Thread-owned journal I/O flushes at
45 seconds plus errors/status/shutdown/rotation; shard hashes and manifests
retain the existing durability guarantees. Clock probes retain request times,
integer server time, uncertainty and RTT. No correction weakens the five-second
stale-message veto.

Per-market Gamma feeSchedule is required; current crypto rate .07, exponent 1,
taker only. Fee = shares * rate * price * (1-price), rounded to five decimals.
Missing or unsupported schedules are rejected; rebates excluded. Legacy
base_fee/fee_rate_bps cannot silently replace the explicit schedule.

Current markets specify Chainlink 60-second TWAP. Official opening price is
saved only with eventMetadata.priceToBeat provenance. A value learned after
closure is target evidence and never injected into an earlier decision.
Reference-price streaming currently requires authentication. No authentication
or key was added. Binance spot is a proxy, not the official oracle.

The fair model is a short-horizon arithmetic Brownian approximation for final
TWAP, not first passage. For remaining time below 60 seconds it requires the
already elapsed path integral. Volatility scale is trained on training Brier;
validation uses window weighting and comparison with market mid, with a positive
lower confidence bound. Insufficient samples or no improvement stops S1/S2/S4.

Actual split/merge route cost and gasless eligibility cannot be established with
no wallet or keys. Unknown costs stay null; zero is used only for an economic
upper bound, never verified net profit or GO. Mint opportunities assume already
split inventory; on-demand confirmation time is additionally unknown. Current
documentation describes minimum order size in USDC while the legacy market
endpoint lacks a unit annotation. The inherited five-share baseline does not
establish current order admissibility; this remains unproved.

## Policies

S0: original baseline. S1: one fair-cheap maker leg, complete missing leg only
under first-leg all-in cost plus edge and operation-cost cap; taker completion
rechecks that cap and depth after .5 seconds. S2: spread/empirical probability
volatility edge and inventory skew, preserving hard completion cap. S3: only
fee-adjusted observed pair/mint opportunities, with past persistence of .5
seconds plus receipt lag, then sequential .5-second arrivals that can abort
separately and leave a single leg. Stale data resets persistence. S4: S1/S2 plus
30-second warmup, 20-second close buffer and combined Up/1-Down guard.

Strict maker fills require real SELL prints strictly after anchor and size
beyond the conservative queue. Upper queue sensitivity still needs prints.
Visible taker depth is locally consumed until its specific level refreshes.
Book-based taker results remain counterfactual; venue races, hidden liquidity
and counterfactual feedback are unproved. Queue 1.5 is conservative under its
assumptions, not a proven mathematical lower bound; queue 1.0 is not a
mathematical upper bound on path-dependent strategy PnL.

Mark-outs use fresh observations within 1.1 seconds of 5/20/60-second horizons.
Gaps or missing horizons produce null. Mid change and future mid minus fill
price are distinct. Single-leg cost, exposure duration, terminal resolved
payoff and unresolved positions are reported separately.

The original reference is already seen and exploratory only. Before the
workspace disconnected, S0 matched existing engine state after all 175,127
events (173,654 market frames), preserving default results. The saved partial
new-data checkpoint is not a completed two-hour capture. The replacement
recording and replay are tracked in GitHub Actions and their own provenance.

GO requires conservative final-holdout positive verified net profit, sample
size, positive lower confidence limits, bounded drawdown and complete data.
Insufficient evidence is NO-GO, not proof that no strategy can ever work.
A GO would propose another paper verification session; it never enables real
trading.

## Primary sources checked 2026-10-04

- https://docs.polymarket.com/trading/fees
- https://docs.polymarket.com/market-data/market-details
- https://docs.polymarket.com/market-data/realtime-data
- https://docs.polymarket.com/trading/positions/manage
- https://docs.polymarket.com/api-spec/clob-openapi.yaml
- Per-market Gamma metadata retained in public-data recordings.

The documented gasless position route and reference-price stream require
authentication. Reading those documents proves neither access nor free
operation cost for this project.
