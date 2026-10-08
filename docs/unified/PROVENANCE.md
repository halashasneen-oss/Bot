# Reviewed concepts and independent implementation

No third-party bot was executed, installed, copied wholesale or given credentials. The new runner uses this repository's pinned dependencies and existing pure validation/math helpers. This is a synthesis of useful architectures, not evidence that combining bots creates an edge.

| Repository | Useful reviewed idea | Excluded limitation |
|---|---|---|
| https://github.com/warproxxx/poly-maker | separate market-data, risk and execution concerns | maker queue/reward assumptions do not justify $5 BTC taker fills |
| https://github.com/GoPolymarket/polymarket-trader | organized public-market discovery and orchestration | real order/signing/account code not admitted |
| https://github.com/frankda/jev-poly-crypto-demo | distinct Chainlink spot and TWAP feeds; exact boundary opening; official resolution | no LICENSE found: no copying; AI-key dependency and unvalidated model omitted |
| https://github.com/ethanwei6/Bitcoin5Min | FOK fresh-book recheck after latency, depth walk, fee-inclusive ledger | median spot opening proxy and quote/closed-based settlement excluded |
| https://github.com/etteehustle/polymarket-btc5mins-paperbot | chained five-minute session reports | old spot reference and inconsistent fee-rate conversion excluded |
| Josh-Alot public educational paper bot reviewed during research | public subscription without filters can prevent silently missing updates | spot-based local settlement and top-ask fills without depth/fees excluded |

The probability baseline is independently specified as zero-drift arithmetic Brownian TWAP, not an externally proven signal. Configuration frozen before results; one strategy comparison group. Public-source probe 1 used filters (historical snapshots only); probe 2 used `type:update` without filters (34 live BTC spot and 34 live BTC TWAP messages). This feed availability check does not establish price provenance or profitability. Full probe evidence remains preserved.

Fee documentation: https://docs.polymarket.com/trading/fees ; actual per-market fee descriptors fetched and matched at runtime. Official market rule/source comes from Gamma market metadata. No Binance reference replacement or geographic bypass is used. Original engine, frozen protocols and final holdout are untouched.
