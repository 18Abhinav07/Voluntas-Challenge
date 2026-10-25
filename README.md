# Order-book collector for Polymarket and Kalshi

A Rust service that subscribes to every live market on a prediction-market exchange and keeps an
L2 order book for each one. It writes every price-level change, plus a full snapshot of every
book once a minute, in the challenge template's aligned SBE format.

The same engine runs both venues. Only the websocket encode/decode layer changes between them
(Ext 3):

- **Polymarket** is the venue it was built and measured on. It has run the full universe on AWS:
  about 550k tokens for an hour, and about 488k tokens for two hours.
- **Kalshi** needs an API key even for public data. It ran the whole demo environment (about 100k
  markets) for two hours from Virginia: 32.3M updates, 48.9M snapshots replayed, 0 mismatches.
  Production is the same code with a production key and the default URLs.

**The presentation:** [18abhinav07.github.io/Voluntas-Challenge](https://18abhinav07.github.io/Voluntas-Challenge/)
covers how it was built, every run, the fixes and what each one moved, with charts. It is also in
this repo at [docs/index.html](docs/index.html); open it in a browser.

| | Full universe, AWS c7i.xlarge (London), 60 min | The same, 120 min, a day later |
|---|---|---|
| Coverage | 548k–554k tokens on 442–452 websockets | 488k tokens on 392–400 websockets |
| Throughput | 41–43k updates/s | 31–34k updates/s (231.7M in total) |
| Correctness | about 55M snapshots replayed from the deltas: **0 mismatches** | 118.2M snapshots replayed: **0 mismatches** |
| Latency | exchange → collector p50 8–9 ms; kernel → collector p50 ≤ 32 µs | p50 5–6 ms; kernel → collector p50 ≤ 16–32 µs |
| Resources | 1.9 GB RSS peak, load 1.2 of 4 vCPU | 1.97 GB RSS peak, load 1.1 of 4 vCPU |

## Run it

Needs stable Rust (edition 2024): [rustup.rs](https://rustup.rs). Other ways (Docker, a Codespace,
AWS, a systemd service) are in [HOW_TO_RUN.md](HOW_TO_RUN.md).

### The whole check in one command

```bash
./demo.sh
```

This runs four steps and stops at the first failure:
1. Build.
2. Replay the committed sample capture (offline).
3. Run every test against mock venues on localhost (offline).
4. Collect 2,000 live Polymarket markets for two minutes, then replay that file.

### Polymarket

No key is needed.

```bash
cargo build --release --bins
```

```bash
./target/release/collector --out ob.sbe --max-markets 2000 --duration-secs 120
```

```bash
./target/release/verify ob.sbe
```

Leave out `--max-markets` and `--duration-secs` to collect every market until Ctrl-C.

### Kalshi

Create an API key at kalshi.com (or at demo.kalshi.co, which needs no funds), then put it in a
`.env` file. Git ignores `.env` and `*.pem`.

```bash
KALSHI_API_KEY_ID=your-key-id
KALSHI_PRIVATE_KEY_PATH=/path/to/kalshi-key.pem
```

```bash
./demo.sh kalshi
```

```bash
KALSHI_DEMO=1 ./demo.sh kalshi
```

The first command uses production; the second uses the demo environment. To run Polymarket and
Kalshi side by side, one process each, use `./demo.sh both`.

### Stopping

Ctrl-C (SIGINT) closes every websocket, writes the last chunk and closes the file. SIGTERM does
the same. A second Ctrl-C forces an exit.

## What it does

| Requirement | How |
|---|---|
| Every live market; new ones joined, finished ones dropped | Discovery every 10 min plus the venue's listing and resolution events |
| Markets spread over many websockets | Shards of 2,500 tokens, about 450 sockets on the full universe |
| One L2 book per symbol | Flat sorted books owned by core threads |
| Deltas plus a snapshot every 60 s, using the template's helpers | `PriceLevelUpdate` and `Snapshot` through `AlignedSbeWriter`, 64 MiB chunks |
| Output to stdout | `--out -` is the default; a path writes a file |
| SIGINT closes the sockets and the file | Also on SIGTERM and on a panic |
| No MVEs (Kalshi) | `mve_filter=exclude` on discovery and lookup |
| **Ext 1:** redundant websockets | 2 sockets per shard on different edges, source arbitration, a snapshot on every switch, `--supervise` |
| **Ext 2:** subscription monitoring | Per-token rate checks, quiet resubscribe, quiet-socket reconnect, hourly make-before-break recycling |
| **Ext 3:** generic per-exchange encode/decode | The `ExchangeAdapter` trait; Polymarket and Kalshi adapters in `src/adapter/` |
| **Ext 4:** latency | Four experiments, written up in [docs/LATENCY.md](docs/LATENCY.md) |

[docs/GRADING.md](docs/GRADING.md) matches each claim to the command that reproduces it.

## Output format

- **Symbols** are `<id>:bid` and `<id>:ask`, because the schema's `PriceLevelUpdate` has no side
  field.
- **Prices and sizes** are fixed point, `i64`, with 1,000,000 = 1.0.
- **Updates** carry the absolute size at a price. Size 0 removes the level.
- **Timestamps:** `recv_ts` is our receive time; `exch_ts` is the exchange's timestamp.
- **Snapshots** are written for every side once a minute, and when a book first appears or
  changes source.
- **Invariant:** a side's snapshot plus the updates after it equals its next snapshot, exactly.
  `verify` checks this for every snapshot in a file and exits non-zero on any mismatch.

## Main flags

`collector --help` lists every flag. The ones you are likely to need:

| Flag | Default | Meaning |
|---|---|---|
| `--venue` | `polymarket` | `polymarket` or `kalshi` |
| `--out` | `-` | `-` is stdout; anything else is a file path |
| `--max-markets` | `0` | Collect only this many markets (0 = all) |
| `--duration-secs` | `0` | Stop after this long (0 = until Ctrl-C) |
| `--supervise` | off | Restart automatically if the process dies |
| `--rotate-mins` | `0` | Cut the output into files of this many minutes |
| `--replicas` | `2` | Redundant sockets per shard (Ext 1) |
| `--kernel-ts`, `--pin` | off | Linux: kernel receive timestamps; pin core threads to CPUs |

## Tests

```bash
cargo test --release
```

About 2 minutes, all offline:
- unit tests;
- golden tests on captured live frames;
- 10 fault injections against a mock Polymarket: a dropped socket, a silent socket, a stall, a
  delay, a corrupt update, 429s, a resolution, a panic, `kill -9`, SIGTERM;
- 3 against a mock Kalshi: a baseline, a `seq` gap and a new listing.

CI runs the same tests plus clippy and a Docker build.

## Docs

| Read first | |
|---|---|
| [HOW_TO_RUN.md](HOW_TO_RUN.md) | Every way to run it: local, Docker, Codespace, AWS, systemd |
| [docs/GRADING.md](docs/GRADING.md) | The five-minute check, claim → evidence, and what each counter means |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, data flow, and how each extension is met |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Each design choice, the alternative, and why it lost |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | The AWS numbers: latency, CPU, memory, regions |
| [docs/LATENCY.md](docs/LATENCY.md) | Ext 4: what was measured, what helped, what did not |
| [docs/KALSHI.md](docs/KALSHI.md) | Ext 3: the Kalshi adapter and its live demo runs |

[docs/appendix/](docs/appendix/) holds the deeper records: the full engineering log, the
performance plan, the AWS run kit and the deployment notes.

## Operating notes

- **Kalshi environment.** The live Kalshi runs used the demo environment, which serves the same
  API as production. Production is the default endpoint; only the key changes.
- **A `kill -9` loses up to about 1 MiB** that was still queued (a whole chunk when writing to
  stdout). Under `--supervise` the collector is back in about a second.
- **Failover waits for evidence.** A silent source is replaced when a peer has been ahead for
  `--lag-ms` (3 s by default).
- **Latency is at the venue's floor.** Of the four experiments, none beat the defaults by enough to
  change them. The rest of the latency is in the venue and the network path.

## Layout

| Path | What it is |
|---|---|
| `src/main.rs` | CLI, venue choice, discovery, routing, shutdown |
| `src/core/` | Books, source arbitration, sharding, socket lifecycle, output, health checks |
| `src/conn.rs` | Websocket actor: connect, watchdog, reconnect |
| `src/adapter/` | The `ExchangeAdapter` trait and the Polymarket and Kalshi adapters |
| `src/writer.rs`, `src/supervise.rs` | Writer thread and rotation; `--supervise` |
| `src/bin/verify.rs` | The output checker |
| `src/util/` | The template's SBE helpers, unchanged |
| `tests/` | Golden and end-to-end tests, mock venues |
| `bench/` | Scripts and raw logs from every measured run |
| `sample/` | Two minutes of live output from the AWS run |
| `docs/index.html` | The presentation page |
