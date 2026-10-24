# Polymarket order-book collector

A Rust collector that subscribes to every live Polymarket market, keeps an L2 book per outcome
token, and writes deltas plus a full snapshot of every book each minute to an aligned FIX SBE file
(the challenge template's format and helpers).

Polymarket instead of Kalshi: Kalshi could not be reached from where this was built (India).
The challenge allows that fallback. [docs/KALSHI.md](docs/KALSHI.md) shows what a Kalshi adapter
would take.

| Doc | What it covers |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, data flow, and how each extension is met |
| [docs/DECISIONS.md](docs/DECISIONS.md) | The choices made, with the alternative and why it lost |
| [docs/LATENCY.md](docs/LATENCY.md) | Ext 4: what is measured, what is built, what is still untested |
| [docs/KALSHI.md](docs/KALSHI.md) | Ext 3: mapping Kalshi onto the adapter trait, and the trait gaps |
| [docs/ENGINEERING-LOG.md](docs/ENGINEERING-LOG.md) | Every run, bug and fix, in order |

## Build and run

Needs a stable Rust toolchain (edition 2024).

```bash
cargo build --release --bins
```

```bash
./target/release/collector --out orderbook.sbe
```

Stop with Ctrl-C. The first SIGINT closes every websocket, writes the last chunk and closes the
file. A second SIGINT forces an exit.

Logs go to stderr, so `--out -` writes the SBE stream to stdout:

```bash
./target/release/collector --out - > orderbook.sbe
```

A small run for trying it out:

```bash
./target/release/collector --out small.sbe --max-markets 2000 --duration-secs 120
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--out` | `orderbook.sbe` | Output file, or `-` for stdout. Truncated on start. |
| `--rotate-mins` | `0` | Cut output into files of this many minutes, named by period start. Each file replays on its own. |
| `--tokens-per-conn` | `2500` | Tokens per websocket (one shard). |
| `--replicas` | `2` | Independent sockets per shard, pinned to different edge IPs when DNS returns more than one. |
| `--max-conns` | `600` | Cap on steady-state sockets (shards × replicas). |
| `--core-threads` | `0` | Threads that own book state. 0 = half the CPUs, at most 8. |
| `--core-queue` | `65536` | Decoded frames queued per core before socket readers wait. |
| `--recycle-mins` | `60` | Make-before-break reconnect of every socket older than this (0 = never). |
| `--split-ratio` | `0` | Split a shard once its rate passes this multiple of the median (0 = never; see DECISIONS). |
| `--rediscover-mins` | `10` | Re-run full discovery this often. |
| `--universe-cache` | `universe.json` | Instrument list from the last run, used to subscribe before discovery finishes. |
| `--max-markets` | `0` | Only collect this many markets (0 = all). |
| `--opens-per-sec` | `10` | Websocket handshakes per second across all sockets. |
| `--duration-secs` | `0` | Stop after this long (0 = run until SIGINT). |
| `--lat-detail` | off | Log per-socket and per-edge latency every 30 s (Ext 4, see LATENCY). |
| `--fast-source` | off | Prefer each shard's fastest socket as source and recycle one that stays slowest (see LATENCY, Experiment 2). |
| `--fast-margin-ms` | `3` | With `--fast-source`: how much slower the preferred socket must be before it changes. |
| `--fast-weight` | `0.25` | With `--fast-source`: weight of the newest 30 s window in a socket's score. |

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

## Results

All runs were against live Polymarket. A GitHub Codespace in WestEurope (4 vCPU, 16 GB) did the
full-universe runs.

- **Coverage:** about 283k markets, which is about 566k tokens, on about 450 sockets.
- **Volume:** a 15-minute run writes about 30M updates. The 90-minute soak wrote 185M updates.
- **Memory:** RSS was flat at 3.6–3.9 GB over 90 minutes.
- **Faults injected:**
  - Killing all 224 sockets to one edge moved those tokens to their replicas with no replay error.
  - A 15 s process freeze recovered.
  - The hourly recycle of all 448 sockets left no gaps.
- **Correctness:** 0 replay mismatches in every file of every run.

The full table is at the top of [docs/ENGINEERING-LOG.md](docs/ENGINEERING-LOG.md).

## Known limits

- **No crash restart.** A crash is not restarted from inside the process.
  - With `--rotate-mins`, every finished file is complete. A restart re-subscribes from the
    universe cache within seconds.
  - Without rotation, a restart truncates the file.
  - Running it under a supervisor (systemd, a shell loop) is left to deployment.
- **Failover is not instant.** A silent subscription is detected when a peer replica has seen
  an event the source has not for 3 s. Until then the source's stale book is what readers have.
- **Latency is measured but not tuned.** One experiment has run: 8 sockets on the same 400
  markets (`--lat-detail`). The fastest socket was 1.4 ms behind the first copy on average and the
  slowest 10.3 ms, and the edge IP mattered less than the connection. `--fast-source` acts on it
  (opt-in): in two A/B runs it gave no consistent gain, so it is off by default. See
  [docs/LATENCY.md](docs/LATENCY.md).
- **Polymarket only.** No Kalshi adapter exists.
- **Core CPU.** Two core threads run at about 31–38% each at steady state. The integrity hash
  is the largest part of core time.

## Layout

| Path | What it is |
|---|---|
| `src/main.rs` | CLI, discovery and lookup tasks, routing to core threads, shutdown |
| `src/core.rs` | Book state, source arbitration, integrity ladder, Ext 2, recycling, rotation |
| `src/conn.rs` | Websocket actor: connect, edge pinning, watchdog, reconnect, open limiter |
| `src/adapter/` | `ExchangeAdapter` trait and the Polymarket implementation |
| `src/writer.rs` | Writer thread, rotation, background fsync |
| `src/book.rs`, `src/fixed.rs` | Book type, fixed-point parsing |
| `src/bin/verify.rs` | Output checker |
| `src/util/` | The template's SBE helpers, unchanged |
| `bench/` | Scripts and raw logs from the profiled, A/B and latency runs |

The template's original notes on the SBE schema are in
[challenge_orderbook.yaml](sbe_schemas/engineering_challenge/challenge_orderbook.yaml) and
`src/util/`.
