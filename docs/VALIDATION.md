# Validation status

## Execution repair — model 3 (2026-10-04)

70 local tests pass. Analysis of a separate intact four-market public recording
reproduced permanent movement lockout in 93.8% of quote checks; the repaired model
resumed four times and opened 12 cycles versus six, still with zero fills.
See [execution evidence and limits](EXECUTION_REPAIR.md). No new live simulation
was started during this repair. Older live validation results below apply to their
listed commits; they do not certify the new execution policy. Live validation now
requires manual dispatch. Model-version guards prohibit mixing old accounting with
new rules during resume. Current-model journal replay is checked deterministically.

- Local unit/integration tests cover paired accounting, residual losses, partial fills,
  queue priority, duplicates, latency, post-only rejection, cancellation races,
  inventory/cash caps, stale feeds, unrealized loss stop, movement stop, outcome mapping,
  official settlement requirements, malformed input and deterministic replay.
- Synthetic replay is a correctness check only. It is not historical/live PnL evidence.
- Live public API smoke test is attempted separately; see final commit status below.
- No wallet connected, no signature produced, no order submitted, no on-chain merge.
- Real-money support is intentionally absent.

See GitHub Actions for the independently executed CI result for the commit you use.
A green unit-test workflow does not imply that the public-data session succeeded.

## Verified in this workspace

32 tests passed. Public REST doctor succeeded. One-minute live WebSocket capture
received 184 full book snapshots, 4074 price-change messages and 90 trade prints.
No parser rejections or disconnect gaps. Zero simulated fills, zero PnL.
Replay of the live journal reproduced the complete summary exactly.
See `live-smoke-summary.json` for config, counts, receive timestamps and journal hash.
This establishes connectivity and deterministic processing only, not profitable execution.


## Repair validation — 2026-10-04

52 local tests pass, retaining the original 32. Added cases cover delayed ordered data,
freshness-gated fills, out-of-order recovery, token-local invalidation, terminal 0/1
levels, gzip shard replay, inventory-preserving restart, atomic checkpoints/audit
append, access denial, Retry-After, server failures, fatal tracebacks, slow settlement
without stream starvation, stale-depth valuation, stale print ordering, and three accelerated market transitions. Accelerated
fixtures verify behavior, never live profitability.

The prior hour request stopped after about 1.38 minutes. Its terminal exception was
not captured, so the exact process failure remains unknown. Replay established a
separate rejection cascade caused by treating ordered late data as corruption and
clearing all snapshot state. Both that defect and synchronous settlement blocking
have been repaired; new errors preserve full traceback and stop reason.

Live 11-minute capture and GitHub CI results are reported separately. A successful
short multi-market capture does not establish six-hour stability. Exact six-hour
capture cannot fit a GitHub-hosted job's six-hour total limit including setup/upload.
No established self-hosted environment is assumed, and split jobs are not a continuous
six-hour experiment. Repository artifacts contain public data only; no credentials.

The repair live capture also revealed explicit `slow consumer: send buffer full`
and keepalive timeout disconnects. Review found the movement guard scanning the
entire rolling minute on every frame, creating quadratic work under dense traffic.
It now uses exact monotonic extrema queues (amortized constant time). Constant-price
high-volume input is checked for bounded storage without weakening the movement stop.
This is an observed new failure; the original missing exception remains unknown.

Local journal replay uncovered truncated sealed gzip shards in the first capture;
the exact origin of the missing writes is not established. Those captures cannot
be treated as reproducible evidence. Journal flushes now fsync, close persists the
gzip footer, and the manifest records sealed-shard sizes and SHA-256 hashes. Replay
refuses changed/truncated shards before inventing reconstructed exposure. The hosted
validation includes a full replay check on a separate runner filesystem.

Hosted execution exposed two timing-dependent fixture failures despite a passing
parallel unit job. The rotation fixture now advances virtual time only when a frame
arrives, independent of filesystem speed. The slow-REST fixture asserts actual
stream activity during its outstanding request, not a wall-clock maximum gap. The
access-denial fixture has sufficient deadline for durable initial checkpoint I/O.
These changes retain assertions rather than rerunning away intermittent failures.

A direct public API check of a captured expired market returned no rows without
`closed=true`, but returned `closed=true`, `umaResolutionStatus=resolved` and exact
[1,0] outcome prices with that filter. Settlement queries now explicitly include
closed markets. A regression test preserves this filter and the strict official
resolution checks. The initial 11-minute local capture did not observe confirmations
because of this query defect; zero inventory means its cash/PnL was unaffected.

The first resumed local recording failed its new sealed-shard hash check despite
final status COMPLETED. Its settlement observations are not claimed as a complete
reproducible account. Stream shards now write to .partial paths and publish only
after closing the gzip footer, fsync, and atomic rename. A manifest with an active
or interrupted shard is explicitly rejected by complete replay. The exact origin
of the changed local bytes remains unproven; independent hosted replay is required.


## Completed local live checks

Initial segment: 660 seconds, four BTC five-minute markets, 173654 public frames,
3698 book snapshots, no gaps/rejections and 941 delayed updates. Both outcome tokens
had snapshots in every selected market. Full paper replay matched the report in
7.12 seconds. This segment preceded the closed-market settlement query repair.

Final atomic-publish restart check: a separate 120-second segment restored the initial
journal and received 25259 new live frames. The combined journal replay matched the
final report in 8.58 seconds, preserving cash/exposure and four official resolution
events. Six distinct markets across the two segments; the downtime is explicitly
one gap. This is not a continuous 13-minute or six-hour experiment.

Cash $50; paired PnL $0; settled residual PnL $0; maker fees/merge costs $0; zero fills,
zero outstanding inventory and zero pending merges. Delayed-data metrics remain
explicit, so the initial/local cumulative execution quality is INCOMPLETE_DATA.
No profitability claim follows from zero execution. See repair-live-summary.json.


## Independent hosted result — latest runtime db238e4

Both unit CI (52 tests) and Multi-market live validation succeeded on GitHub.
Live capture requested 660 seconds; reported elapsed 670.526 includes WS shutdown.
217037 public frames, four distinct markets, 3672 snapshots, zero reported gaps,
zero delayed/out-of-order/rejected frames. Full journal replay exactly matched the
saved report, and artifact upload succeeded. Cash $50, zero fills/PnL/fees/exposure.
This confirms short multi-market operation only, not six-hour stability/profitability.

- Unit run: https://github.com/khaledhasneen1993/polymarket-pair-bot/actions/runs/37165606848
- Live run: https://github.com/khaledhasneen1993/polymarket-pair-bot/actions/runs/37165606825
- Artifact: multi-market-live-validation, 21948854 bytes, expires 2026-10-07.
- Details/config/accounting/artifact digest: hosted-validation-summary.json.

The six-hour capture was NOT started. GitHub-hosted jobs cap the entire job at six
hours including setup/upload; a full six-hour data segment cannot fit. Runner inventory
is not accessible through the available GitHub connection, and no authorized persistent
self-hosted machine is established. No access restriction was bypassed, no paid service
purchased, and local/split segments are not represented as a continuous six-hour run.
