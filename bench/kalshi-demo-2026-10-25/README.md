# Kalshi demo runs, 2026-10-25

Live runs of `--venue kalshi` against Kalshi's demo environment (demo-api.kalshi.co), from a
laptop in India, with a read-only demo API key (Ed25519). Results: [docs/KALSHI.md](../../docs/KALSHI.md).

| Run | Flags beyond the demo URLs | Logs |
|---|---|---|
| smoke | `--max-markets 200 --duration-secs 60` | `collect-smoke.log`, `verify-smoke.txt` |
| 15m | `--max-markets 1000 --duration-secs 900` | `collect-15m.log`, `verify-15m.txt` |
| big | `--core-threads 1 --max-markets 2500 --duration-secs 150` (2,500 markets on one subscription) | `collect-big.log`, `verify-big.txt` |

Demo URLs: `--ws-url wss://demo-api.kalshi.co/trade-api/ws/v2 --rest-url https://demo-api.kalshi.co/trade-api/v2`.
The `.sbe` outputs (64–192 MiB) are not kept.
