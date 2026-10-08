# Model and audit notes

## Upstream findings

Audited at upstream commit 4f32103591c9582ccd012bdf10f77d86e5879444:
- `execution/gateway.py::_paper_order` fabricates order IDs; it does not model fills.
- `engine.py::_maybe_merge` returns in paper mode. Paper quoting is not a PnL backtest.
- The original CLI defaults to real trading. This project exposes only public-read commands.
- Reused source: domain types, wire parsers, per-token L2 book. Original license retained.
- Private/authenticated SDK dependencies were deliberately not imported into the paper runtime.

## Execution assumptions

An arriving bid is rejected if it crosses the currently observed ask. At activation,
queue ahead is 1.5 times displayed bid depth at or above the limit. Only observed
SELL prints at/below that limit reduce this estimate. Cancellation alone never
improves priority. Fill notional uses our resting limit, not the cheaper print.
No fill before order arrival or before the actual queue snapshot; cancellation has
latency and can race with prints. Book and trade timestamps have independent
watermarks: a newer book does not discard a fresh print following the queue anchor.
An older trade than the trade watermark remains rejected. A ten-second review
retains an unchanged valid limit and the exact remaining queue; changed or unsafe
limits are cancelled with latency. No cancellations improve priority.
Duplicate prints use a fingerprint; indistinguishable legitimate prints may be
undercounted. This cannot establish a guaranteed lower bound: hidden liquidity,
feed semantics, cross-token matching, impact and data gaps remain unknown.

On a gap all quotes are scheduled for cancellation and snapshots invalidated.
Unknown fills during the disconnected interval cannot be recovered. The run is
flagged INCOMPLETE_DATA, not certified as valid. Market expiry cancels quotes;
positions are never silently removed. The stop command prevents tick-based reopening.

The worst-case committed-cost cap includes inventory, reserved orders and merge
cost basis awaiting confirmation. Session loss includes unrealized marks and can
overshoot during latency. This is a per-run stop, not a persisted 24-hour loss limit.
Restart without --resume creates a new virtual account. --resume replays the prior
public journal and retains cash, inventory, pending merges and risk state. It records
a data gap; a restarted segment is never labelled continuous coverage.

Paired shares use average cost basis. Matched shares are removed on merge submission;
cash returns after configured latency, with configured transaction cost deducted.
Maker fees enter position cost basis and are not deducted twice. Rebates are zero.
No live on-chain success rate is assumed: this simulated merge is a model component.
Fixed fees on very small partial merges can eliminate the target edge.

Residual policy is conservative and explicit: cancel after timeout, wait for official
resolution. It is NOT a model of executable emergency liquidation. Missing/stale bid
depth is valued at zero; a full pair retains complete-set value. Therefore liquidity
loss or feed outages can trigger the loss stop without an actual realized loss.

Before the residual timeout, a cancelled/expired missing leg may be quoted again,
only up to the unmatched shares, market minimum and configured size. Its limit
includes average held cost, configured maker fee, fixed merge cost and target edge.
No new same-side position is opened. Sub-minimum residuals remain exposed until
official settlement. Inventory and cash caps still apply to the entire commitment.

Model 3 replaces permanent movement blocking with a bounded recovery policy:
the same three-cent rolling-minute trigger pauses quoting; cooldown is 15 seconds,
short stability window is five seconds. Further three-cent short-window shocks
extend the deadline. Recovery requires fresh books, valid spreads and observed
stability, then starts a new rolling movement baseline. Gaps/reconnects clear
stability observations and preserve the pause. Residual timeouts and loss stops
are separate and never cleared by this policy. These parameters are research
assumptions, not validated profitable tuning.

Per-frame accounting visits markets with nonzero holdings, tracked by position
size changes. Empty historical markets do not increase the execution hot-path work;
their metadata remains for settlement and audit. Direct size changes also update
the index, so missing exposure cannot silently disappear from valuation.

No BTC price predictor, maker rebate optimizer, Safe relayer or real order executor
is implemented. They are not necessary to run the research version, and future live
work requires a separate audited implementation and explicit user authorization.

## Data and reproducibility

Journal schema 1 stores public raw frames, receive timestamps, market metadata,
config, gap/stop events and confirmed resolution events. Replay uses journal time,
not wall clock. Synthetic fixture is labeled and is never substituted for live data.
The legacy public WS/Gamma schema is verified by doctor/record at runtime; incompatible
schemas are errors or leave no usable data. Historical price candles are not used to
invent fills. Both recording and paper capture poll confirmations independently from the WS
consumer; unresolved shares remain visible when replayed. Capture until enough confirmations exist or extend research
with a separately recorded settlement dataset.

An hour of data is a smoke test, not evidence of persistent profitability. Evaluate
several untouched days and harsher queue/latency/cost assumptions before any live work.


## Recovery and durable evidence (0.2.0)

Exchange timestamp validity/order and execution freshness are separate. Ordered but
late book deltas maintain state; late trades never consume simulated queue or fill.
Out-of-order events do not roll back books. Malformed deltas invalidate only affected
tokens and request full snapshots through reconnect. A reconnect always rebuilds both
books, including reconnects within the same market. There is no fabricated snapshot.

The independent monotonic timer drives deadline, cancellation and periodic checkpoints.
Market discovery uses each new current five-minute slug; REST settlement polling cannot
block WS consumption. Known network errors back off; access denial and engine invariants
fail with traceback. Gaps and incomplete execution data remain explicit even after recovery.

Raw schema-1 events are gzip shards (16 MiB uncompressed each), with a manifest. Stream shards publish atomically after closing and syncing their
footer; active .partial shards make complete replay fail explicitly. Reports
are atomically replaced every five seconds, audits appended once, and errors include stage
and traceback. SIGTERM cancels workers, stores final status, and drains cancellation latency.
A hard kill can truncate the final shard: strict replay refuses corruption, rather than
silently inventing a complete account. Reports survive but are not an exact substitute.

Record mode suppresses quoting; replay evaluates the paper strategy on the recorded
frames. Paper journal replay is expected to reproduce its final report exactly.
This equality applies within the same execution model. New paper journals include
`model_version=3`; resume refuses older models or record-mode journals. `analyze`
evaluates old recorded data with current config and labels its output as research,
without overwriting the original result or claiming a new live session.
