# Both venues at once, 2026-10-25

`KALSHI_DEMO=1 ./demo.sh both 60` from the laptop (India): one collector per venue, side by side,
60 s each, then `verify` on each file.

| Venue | Markets | Updates | Snapshots replayed | Mismatches |
|---|---|---|---|---|
| Polymarket | 2,000 (7,996 symbols) | 74,843 | 7,600 | 0 |
| Kalshi demo | 500 (1,000 symbols) | 95,333 | 820 | 0 |

The latency figures are from a laptop behind a VPN and only show that both ran; the measured
latency is in docs/PERFORMANCE.md. The `.sbe` files are not committed.
