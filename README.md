# Polymarket order-book collector

A Rust collector that subscribes to every live Polymarket market, keeps an L2 book per outcome
token, and writes deltas plus a full snapshot of every book each minute to an aligned FIX SBE file
(the challenge template's format and helpers).

Polymarket is the venue it runs against: Kalshi could not be reached from where this was built
(India), and the challenge allows that fallback. A Kalshi adapter exists too (`--venue kalshi`).
It runs on the same engine and passes end-to-end tests against a mock of Kalshi's documented API,
but it has never run live, which needs a Kalshi API key. See [docs/KALSHI.md](docs/KALSHI.md).

**Grading:** [docs/GRADING.md](docs/GRADING.md) has a five-minute path (build, test, a 2-minute
live run, `verify`), a table from each claim to the command that reproduces it, and what every
counter means.

| Doc | What it covers |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, data flow, and how each extension is met |
| [docs/DECISIONS.md](docs/DECISIONS.md) | The choices made, with the alternative and why it lost |
| [docs/LATENCY.md](docs/LATENCY.md) | Ext 4: what is measured, what is built, what is still untested |
| [docs/KALSHI.md](docs/KALSHI.md) | Ext 3: the Kalshi adapter, what the trait had to change, what a live run must check |
| [docs/GRADING.md](docs/GRADING.md) | Five-minute check, claim → evidence table, flags, counters, clock-corrected latency |
| [docs/DEPLOY.md](docs/DEPLOY.md) | Host requirements, AWS / GCP / Hetzner equivalents, systemd unit, shipping output |
| [docs/ENGINEERING-LOG.md](docs/ENGINEERING-LOG.md) | Every run, bug and fix, in order |

## Requirements checklist

| Requirement | Where it is met | Evidence |
|---|---|---|
| Every live market, new listings in, resolved ones out | Gamma keyset discovery every 10 min, `new_market` / `market_resolved` events, universe cache (D16) | ~283k markets, ~566k tokens; e2e `resolved_close` |
| Many websockets, tokens balanced across them | Shards of `--tokens-per-conn`, × `--replicas` (`core/plan.rs`) | ~450 sockets on the full universe |
| One L2 book per token | `book.rs`, owned by core threads | hash check on every delta (98.9–100%) |
| `PriceLevelUpdate` per level change, `Snapshot` per symbol every 60 s | `core/emit.rs` → `writer.rs`, template `AlignedSbeWriter`, 64 MiB chunks | `verify`: 0 replay mismatches in every run |
| Snapshot on a source switch | `core/emit.rs` `adopt`: diff, then snapshot (D8) | e2e `dropped_socket`: `switch_snaps` ≥ 1 |
| SIGINT: close sockets, flush, close the file | `main.rs` shutdown order; also on a panic (D20) | every run ends with `done` and a readable file |
| Ext 1: redundancy | 2 replicas per shard on different edges, source arbitration, lag check, `--supervise` | e2e drop / silent / stall / delay / supervise; 224-socket edge kill live |
| Ext 2: silent failures and exchange-side decay | venue hash ladder, quiet resubscribe, per-socket quiet reconnect, make-before-break recycling | e2e corrupt / stall; hourly recycle of 448 sockets with no gaps |
| Ext 3: a second exchange | `ExchangeAdapter` trait; Polymarket and Kalshi adapters (`src/adapter/`), `--venue` | Kalshi e2e: seq gap → failover, listing → subscribe; never run live ([docs/KALSHI.md](docs/KALSHI.md)) |
| Ext 4: latency | per-socket latency (`--lat-detail`), `--fast-source`, a TCP source-port experiment (`--src-ports`), clock offset (D21) | [docs/LATENCY.md](docs/LATENCY.md) |

## Build and run

Needs a stable Rust toolchain (edition 2024).

```bash
cargo build --release --bins
```

The SBE stream goes to stdout and logs go to stderr:

```bash
./target/release/collector > orderbook.sbe
```

Or to a file:

```bash
./target/release/collector --out orderbook.sbe
```

It refuses to write binary to a terminal. Stop with Ctrl-C: the first SIGINT closes every
websocket, writes the last chunk and closes the output. A second SIGINT forces an exit.

To restart it automatically if it dies:

```bash
./target/release/collector --supervise > orderbook.sbe
```

A small run for trying it out:

```bash
./target/release/collector --out small.sbe --max-markets 2000 --duration-secs 120
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--venue` | `polymarket` | `polymarket` or `kalshi`. Kalshi needs `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH` (docs/KALSHI.md). |
| `--out` | `-` | `-` is stdout, anything else a file path. Truncated on start. |
| `--supervise` | off | Run as a child process and restart it when it dies (D20). |
| `--rotate-mins` | `0` | Cut output into files of this many minutes, named by period start. Each file replays on its own. |
| `--tokens-per-conn` | `2500` | Tokens per websocket (one shard). |
| `--replicas` | `2` | Independent sockets per shard, pinned to different edge IPs when DNS returns more than one. |
| `--max-conns` | `600` | Cap on steady-state sockets (shards × replicas). |
| `--core-threads` | `0` | Threads that own book state. 0 = half the CPUs, at most 8. |
| `--core-queue` | `65536` | Decoded frames queued per core before socket readers wait. |
| `--io-threads` | `0` | Tokio workers for sockets, TLS and decoding. 0 = CPUs − core threads − 1, at least 1. |
| `--writer-queue-mb` | `256` | Output queued in memory before the cores wait for the writer. Nothing is dropped; the status line logs `queue_hwm_mb` and `queue_waits`. |
| `--pin` | off | Linux: each core thread on a CPU of its own, every other thread on the rest. |
| `--kernel-ts` | off | Linux: `recv_ts` is the kernel receive time (`SO_TIMESTAMPNS`); logs `host delay` (app − kernel) every 30 s. |
| `--recycle-mins` | `60` | Make-before-break reconnect of every socket older than this (0 = never). |
| `--split-ratio` | `0` | Split a shard once its rate passes this multiple of the median (0 = never; see DECISIONS). |
| `--rediscover-mins` | `10` | Re-run full discovery this often. |
| `--universe-cache` | `universe.json` (`universe-kalshi.json` for Kalshi) | Instrument list from the last run, used to subscribe before discovery finishes. |
| `--max-markets` | `0` | Only collect this many markets (0 = all). |
| `--opens-per-sec` | `10` | Websocket handshakes per second across all sockets. |
| `--duration-secs` | `0` | Stop after this long (0 = run until SIGINT). |
| `--lat-detail` | off | Log per-socket and per-edge latency every 30 s (Ext 4, see LATENCY). |
| `--fast-source` | off | Prefer each shard's fastest socket as source and recycle one that stays slowest (see LATENCY, Experiment 2). |
| `--fast-margin-ms` | `3` | With `--fast-source`: how much slower the preferred socket must be before it changes. |
| `--fast-weight` | `0.25` | With `--fast-source`: weight of the newest 30 s window in a socket's score. |
| `--edge-ip`, `--src-ports` | off | Experiment 3: pin every socket to one edge IP; bind replica r to the r-th local port (0 = ephemeral). |
| `--ntp-server` | `time.google.com:123` | Measure and log the host clock offset at start and every 10 min (D21). Empty = off. |

## Output

Each outcome token is written as two symbols, `<token_id>:bid` and `<token_id>:ask`, because the
schema's `PriceLevelUpdate` has no side field. The template's `Snapshot` has both a bids and an
asks list, but each of our snapshots fills only the side its symbol names.

- **Prices and sizes** are fixed point, `i64` with 1,000,000 = 1.0.
- **`PriceLevelUpdate`** carries the absolute size at a price. Size 0 removes the level.
- **`Snapshot`** is written for both sides of every book once a minute, spread over the minute,
  and whenever a book first appears.
- **Timestamps:** `recv_ts` is our receive time. `exch_ts` is the exchange's timestamp of the last
  event applied to that book.
- **A market that closes** gets a size-0 update for each of its remaining levels, so readers'
  books end empty.
- **Invariant:** each side's last snapshot plus the updates since it equals the next snapshot,
  exactly. That holds across source switches (see ARCHITECTURE).
- **Chunks** are 64 MiB, the size the template reader is fixed at.

## Verify a file

```bash
./target/release/verify orderbook.sbe
```

`verify` replays every side from its snapshots and updates and checks that each rebuild matches
the next snapshot exactly. It also flags:
- bad prices or sizes;
- an update before its symbol's first snapshot;
- unordered or two-sided snapshots;
- malformed symbols;
- crossed books.

It exits non-zero on any failure. Every output file from every run in the log passed with 0
mismatches.

## Tests

```bash
cargo test --release
```

- Unit tests cover the core (failover, integrity, Ext 2, recycling, rotation, removal), the
  connection actor, the writer and fixed-point parsing.
- Three golden tests in `tests/golden.rs` replay captured live Polymarket frames from
  `tests/fixtures/` through decode and the hash check. They pin the verdict counts, including
  the 98.9% pass rate on the trade-heavy capture.
- Nine end-to-end tests in `tests/e2e.rs` run the real binary against a mock venue on
  localhost (Gamma over HTTP, the market websocket with correct hashes). Each injects one
  fault and checks the reaction in the final metrics, then runs `verify` on the output:
  dropped socket, silent socket, one stalled subscription, a 5 s delay, a corrupt update,
  Gamma 429s, a resolved-assets close, and a panic under `--supervise`. About 45 s.
- Three end-to-end tests in `tests/kalshi_e2e.rs` run the same binary with `--venue kalshi`
  against a mock Kalshi (signed handshake, `sid` acks, relative deltas with `seq`):
  a baseline, a `seq` gap that must fail over and reconnect, and a lifecycle listing that must
  join the existing subscription.
- CI (`.github/workflows/ci.yml`) runs clippy with `-D warnings` and all of the above.

## Results

All runs were against live Polymarket. A GitHub Codespace in WestEurope (4 vCPU, 16 GB) did the
full-universe runs.

- **Coverage:** about 283k markets, which is about 566k tokens, on about 450 sockets.
- **2.5-hour soak (build 1a4d84c):**
  - 320M updates and 184M snapshots in 16 files of 10 minutes each;
  - 0 replay mismatches in any file;
  - 0 restarts; 535 venue disconnects, all healed by reconnect and failover;
  - RSS rose from 2.8 to 3.3 GB as the universe grew, by 279k listings and 3.6k removals;
  - CPU 186–197% of 400%.
- **Volume:** a 15-minute run writes about 30M updates. One 10-minute file is about 5 GB.
- **Memory:** RSS 2.8–3.3 GB on the full universe.
- **Latency (2.5 h):** `wire` p99 ≤ 16.4 ms and `queue` p99 ≤ 65.5 ms. Both are histogram bucket
  bounds, not exact values.
  - The host clock drifted from +19 to +88 ms against time.google.com, which the clock check
    logged, so `wire` reads low by that much.
  - Eight disk stalls of 1.0–1.5 s made the writer apply backpressure. That is the Codespace disk
    at about 8.5 MB/s sustained, not the collector.
- **Faults injected:**
  - Killing all 224 sockets to one edge moved those tokens to their replicas with no replay error.
  - A 15 s process freeze recovered.
  - The hourly recycle of all 448 sockets left no gaps.
- **Correctness:** 0 replay mismatches in every file of every run.

The full table is at the top of [docs/ENGINEERING-LOG.md](docs/ENGINEERING-LOG.md).

## Known limits

- **A crash loses the unwritten chunk.** Output is written in whole 64 MiB chunks (D11), so a
  `kill -9` loses what was still in memory, up to one chunk per open file. A panic does not: it
  shuts down in order and writes the last chunk. Under `--supervise` the collector is back
  within seconds of a crash, from the universe cache; the gap is marked in the output (D20).
- **Failover is not instant.** A silent subscription is detected when a peer replica has seen
  an event the source has not for 3 s. Until then the source's stale book is what readers have.
- **Latency is measured but not tuned.** One experiment has run: 8 sockets on the same 400
  markets (`--lat-detail`). The fastest socket was 1.4 ms behind the first copy on average and the
  slowest 10.3 ms, and the edge IP mattered less than the connection. `--fast-source` acts on it
  (opt-in): in two A/B runs it gave no consistent gain, so it is off by default. See
  [docs/LATENCY.md](docs/LATENCY.md).
- **Kalshi is untested live.** The adapter passes against a mock built from Kalshi's docs.
  KALSHI.md lists what the first live run must check (per-subscription market limit, `seq`
  scope, lifecycle event names).
- **Core CPU.** Two core threads run at about 31–38% each at steady state. The integrity hash
  is the largest part of core time.

## Layout

| Path | What it is |
|---|---|
| `src/main.rs` | CLI, venue choice, discovery and lookup tasks, routing to core threads, shutdown |
| `src/core/` | Book state and source arbitration (`mod.rs`); shard planning (`plan.rs`); socket lifecycle and recycling (`conns.rs`); output, rotation and switches (`emit.rs`); integrity ladder and Ext 2 (`health.rs`); speed-aware source (`speed.rs`); counters (`metrics.rs`) |
| `src/conn.rs` | Websocket actor: connect, edge pinning, watchdog, reconnect, open limiter |
| `src/supervise.rs`, `src/clock.rs` | `--supervise` restart and `.part` recovery; NTP clock offset |
| `src/adapter/` | `ExchangeAdapter` trait, shared REST retry, the Polymarket and Kalshi adapters |
| `src/writer.rs` | Writer thread, rotation, background fsync |
| `src/book.rs`, `src/fixed.rs` | Book type, fixed-point parsing |
| `src/bin/verify.rs` | Output checker |
| `src/util/` | The template's SBE helpers, unchanged |
| `bench/` | Scripts and raw logs from the profiled, A/B and latency runs |

The template's original notes on the SBE schema are in
[challenge_orderbook.yaml](sbe_schemas/engineering_challenge/challenge_orderbook.yaml) and
`src/util/`.
