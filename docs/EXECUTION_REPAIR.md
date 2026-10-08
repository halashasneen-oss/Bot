# Execution diagnosis and repair — 2026-10-04

Base: `c5a54788fc93bd896589e6ed2f8fda6876fc0601`; paper-only model 3.
No wallet, private key, exchange order, signing or real MERGE was introduced.

## Evidence and scope

The completed 350-minute run (`37177417110`) reported zero fills, $50 cash,
70 selected markets, 40 gaps, 178470 delayed updates and 30 out-of-order updates.
Those final logs establish incomplete execution data, but do not explain every
unfilled order. The full 597 MB GitHub binary artifact was not downloaded through
the text-only connector, so no full-run journal replay/audit is claimed here.

An available **separate** valid public recording, `durable-multimarket-20261004`,
contains 173654 frames over 660 seconds and four markets. All gzip shards and
manifest hashes were verified on read. No synthetic frames were substituted.
The original engine was restored from the base commit for baseline instrumentation.
See `execution-baseline-analysis.json` and `execution-repaired-analysis.json`.

Baseline results:

- 174980 quote checks; 164136 (93.8%) returned because the market was permanently
  blocked by the movement guard. This is a share of checks, **not wall-clock time**.
- Three observed blocks occurred at 40.97, 15.76 and 11.59 seconds from market start.
  The first selected market was already near expiry and did not have such a block.
- Six quote cycles. Of 1840 fresh parsed trades, 1652 had no order, 18 saw orders
  not activated, 167 were BUY prints, two were above our bid, and one only depleted
  estimated queue. There were zero fills.
- The newer-book timestamp filter did not account for these baseline eligible
  prints; it is a separately reproduced model defect, not asserted as the main
  cause of the 350-minute result.

## Changes

1. Movement guard now pauses instead of permanently retiring the market. It keeps
   the three-cent trigger, then requires a 15-second cooldown and observed
   five-second stability before resetting its movement baseline. New short shocks
   extend the cooldown. Gaps/reconnects never count as stability. Residual timeout
   and session loss stops remain terminal for their existing scope.
2. Ten-second quote review keeps an unchanged valid limit and its remaining queue.
   Changed/unsafe limits use cancellation latency. Queue is not reset or improved
   merely because displayed liquidity disappeared.
3. Execution uses the actual queue snapshot time plus a separate trade watermark.
   Fresh ordered prints after this anchor may be consumed even after a newer book
   arrived. Prints before the anchor, duplicates, older trades, stale prints,
   BUY prints and inadequate queue consumption cannot fabricate fills.
4. Missing-leg orders can be recreated during the existing unpaired deadline.
   Only missing shares are quoted, including held average cost, maker fee, merge
   cost and target edge. Size, market minimum, cash and inventory caps apply.
   Sub-minimum residuals are not rounded up; no invented liquidation is assumed.
5. Per-frame accounting visits nonzero-exposure markets. Historical metadata is
   retained but empty old markets no longer add growing work to the hot path.
   A regression fixture verifies 100 empty markets and an old residual position.
   This removes a demonstrated source of processing work; it does not establish
   that it caused every observed network timeout.
6. Reports count quote/trade blocking reasons, activations, reviews, expiry,
   movement recovery and residual timeout. Console checkpoints show progress
   every minute. New headers record model version; resume refuses a different
   model or a recording-mode journal to prevent silently changing account history.
7. Live validation is now manual-only, preserving the request to wait before a
   fresh live simulation. Ordinary CI runs deterministic tests and synthetic replay.

## Verification and honest result

70 local tests passed, including 18 new targeted cases and all prior 52.
Tests verify actual queue anchoring, older-print rejection, cancellation races,
cooldown/stability and reconnects, queue retention, residual minimum/fees/caps,
old-market accounting, deterministic current-model replay and incompatible resume.
Synthetic execution tests validate logic and accounting, not profitability.

On the same untouched recorded public frames, the repaired model produced:

- 12 quote cycles, 24 orders created, 23 activated, two retained reviews.
- Six movement pauses and four recoveries.
- Zero fills, zero merges, zero PnL/fees/exposure; cash $50.
- Trade reasons: 1495 no order; 28 not active; 302 BUY prints; one before queue
  anchor; 12 above limit; two depleted queue without reaching us; one stale print.
- 149616 quote checks still stopped by temporary movement protection. Active
  market volatility remains a real limitation of this conservative strategy.
- 941 delayed updates keep this recording's execution quality `INCOMPLETE_DATA`.

These are **recorded-data research results**, not a newly launched live run and
not rewritten results for the 350-minute session. The implementation can now
explain inactivity and recover from the proven lockout, but does not guarantee
fills or profitability. Initial quotes still target both sides; this is not an
identified clone of the public trader's proprietary algorithm. The missing side
may fill/requote later within the stated risk bounds.

The next authorized live run should start a new results directory. Hosted 350
minutes remains the maximum supported with upload margin; exact 360 minutes still
requires an already available authorized self-hosted runner. No new live session
was launched during this repair request.
