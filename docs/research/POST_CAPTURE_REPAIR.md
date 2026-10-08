# Post-capture diagnostic repair — 2026-10-04

The successful two-hour capture/analysis run 37208627944 is not a profitability
validation. Its 17 descriptive windows produced no S0/S3 fills; six final
windows remain sealed. The frozen acceptance protocol is unchanged.

## Corrected reporting

- Each evaluated strategy now reports its own live-order/cooldown measurements.
  The old top-level measurements came from the record-only probe and could not
  represent S0's 170 created / 158 activated orders. The probe is now explicitly
  scoped as having no strategy orders.
- Once-per-second observation counts distinguish missing books, stale books,
  resync requirements, fresh books without a two-sided mid, and valid mids.
  Counts are not duration estimates. Missing usable mid observations alone do
  not prove socket downtime. Existing coverage and stale-data vetoes stay intact.
- Global Binance/clock source failures remain visible when an initial partial
  market is excluded from chronological evaluation.

Validation: 130 local tests pass, including report attribution and preservation
of a source failure from an excluded partial window. No execution rule changed.

## Read-only connectivity probes

The existing api.binance.com/api/v3/time endpoint still returned HTTP 451 from
this workspace, explicitly refusing access from the runner's location. No proxy
or restriction bypass was introduced. Polymarket /time returned HTTP 200.
Gamma's active btc-updown-5m-1791132000 market had event 1125227 with null
eventMetadata and no official opening reference. Its resolution source remained
Chainlink BTC/USD 60-second TWAP. This one-market probe cannot establish that
all official public reference sources are unavailable.

## Still unresolved

- A permitted BTCUSDT stream accessible from the recording environment.
- Official opening reference available before decisions, with provenance.
- Attribution of each recorded Polymarket gap and long receipt delay; fresh-mid
  coverage diagnostics alone are not transport diagnostics.
- Verified operation costs and sufficient preregistered sample size.

Do not launch another long validation capture merely to repeat the known
missing prerequisites. Keep the existing raw capture and holdout sealed.
These changes need a future descriptive reanalysis of the same training and
validation windows to quantify the new diagnostics; no such replay is claimed
by this checkpoint.
