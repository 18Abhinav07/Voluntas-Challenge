# Two-hour run, both venues (2026-10-25)

Two EC2 hosts launched at the same time and run for 2 hours each on build 23db0d7. The build is
`host.sh` with `MAIN_SECS` and the `kalshi` phase; the collector code is the same as 89055df.

| | Polymarket | Kalshi (demo environment) |
|---|---|---|
| Host | c7i.xlarge, eu-west-2 (London), 4 vCPU, 8 GB | c7i-flex.large, us-east-1 (Virginia), 2 vCPU, 4 GB |
| Command | `host.sh main` with `MAIN_SECS=7200` | `host.sh kalshi` with `MAIN_SECS=7200` |
| Flags | `--supervise --pin --kernel-ts --rotate-mins 10` | the same, plus the demo `--ws-url` / `--rest-url` |
| Collector start / stop (UTC) | 06:21:21 / 08:21:32 | 06:20:54 / 08:21:01 |
| Tests on the host | all passed (`tests.log`) | all passed (`tests.log`) |
| Universe | 487.8k–488.4k tokens, 392–400 sockets | 200k–205k tokens (about 100k markets), 162–166 sockets |
| Sourced | 99.999% of listed tokens, `unplaced: 0` | 100% |
| Files verified | 14, about 58 GB | 14, about 6.0 GB |
| Updates | 231.7M (31–34k/s) | 32.3M (3.4–4.9k/s) |
| Snapshots replayed against the rebuilt book | 118.2M matched, **0 did not** | 48.9M matched, **0 did not** |
| recv − exch, per file | p50 5–6 ms, p90 7–8 ms, p99 19–128 ms | p50 33–36 ms, p90 37–44 ms, p99 43–49 ms |
| Host delay (kernel → app), 30 s windows | p50 ≤ 16–32 µs; p99 ≤ 128–256 µs in 210 of 239 windows | p50 ≤ 32–64 µs; p99 ≤ 1–2 ms (2 vCPU shared by 164 sockets) |
| CPU, steady mean, % of one CPU | core 41, tokio 48, writer 7; load 1.1 of 4 | core 3.5, tokio 6.7, writer 1; load 0.2 of 2 |
| Memory | RSS 1,785 MB mean, 1,969 MB peak | RSS 466 MB mean, 629 MB peak |
| Network in, mean | 220 Mbit/s | 27 Mbit/s |
| Writer | queue peak 18 MB, 0 backpressure waits, 0 stalls | queue peak 3 MB, 0 waits, 0 stalls |
| `kill -9` of the child at t = 6,900 s | restarted in 1 s; the open file was recovered and verified; the new child ran 291 s and exited 0 | restarted in 1 s; the new child ran 297 s and exited 0 |
| Venue-side drops | 110 server closes (73 of them "slow consumer") and 106 resets, each reconnected while the peer replica held the book | 162 connection resets and 24 `429` on reconnect, retried with backoff; 23 quiet-socket reconnects |

The cost was about $0.72. That is 2.22 h of on-demand time per host ($0.2121/h and $0.0848/h,
from the AWS price list), plus gp3 volumes and two public IPv4 addresses. Afterwards, nothing
tagged `Project=voluntas-run` was left in either region. The Kalshi demo key was copied to the
Virginia host only, shredded there before teardown, and never written to these files.

## Files

| File | What it holds |
|---|---|
| `*/markers` | Each phase's `_OK` / `_FAILED` line with its time |
| `*/collect-main.log` | The collector's own log: a status line every 10 s, host delay every 30 s |
| `*/verify-main.txt` | One line per 10-minute file: size, span, counts, replay result, recv − exch |
| `*/summary.txt` | Resource means and peaks, warnings by kind, supervisor events, the final metrics |
| `*/res-main.txt` | 10 s samples: RSS, network in, disk out, CPU per thread group |
| `*/clock-main.txt` | chrony state every 5 minutes |
| `*/perf-top.txt` | A 30 s `perf` profile at t = 3,600 s |
| `*/env.txt` | Instance, CPU topology, clock sources, DNS and ping to the venue |
| `polymarket-eu-west-2/bench.txt` | The offline decode and memory bench on the same host |
| `polymarket-eu-west-2/sample/` | A 2-minute, 500-market capture after the run (`verify.txt`); the `.sbe` is not committed |
| `monitor.log` | The laptop's check every 10 minutes during the run |
