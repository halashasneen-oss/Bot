# Recovery notes

Restored from user-supplied Oct 6 ZIP, with T+225-second BTC 5m PAPER strategy: at 75s before close compare Chainlink opening and latest prices, Up if higher, Down if lower, skip exact tie; require executable ask and fresh data. $50 bankroll, $5 stake, one position per window, PAPER only. No real trades.

The Oct 8 research branch history and run artifacts could not be exported from the suspended GitHub account. This archive is a reconstructed working tree, not a byte-for-byte mirror of that branch.

Workflows are renamed to .disabled to avoid accidentally triggering compute while account suspension is unresolved.

Publishing: `bash scripts/publish_recovery.sh` uploads the recovered source to `halashasneen-oss/Bot` using locally authenticated Git, without force-push. Do not run it while GitHub's account suspension or related restrictions prohibit the activity. No GitHub Actions workflows are enabled in this archive.
