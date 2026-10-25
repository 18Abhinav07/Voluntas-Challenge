# Performance

What the collector measured on a production-like host, how, and how to reproduce it. Every number
here comes from the AWS run of 24 October 2026 on build 7170677. The raw files are in
[bench/aws-2026-10-24/](../bench/aws-2026-10-24/) and the run plan is in [AWS-RUN.md](appendix/AWS-RUN.md).

## The host

| | |
|---|---|
| Instance | c7i.xlarge in eu-west-2a (London): 4 vCPU (2 cores × 2 threads), 8 GB, about $0.21/h |
| CPU | Intel Xeon Platinum 8488C |
| Kernel | 7.0.0-1014-aws (Ubuntu 24.04); no PTP device exposed |
| Clock | chrony on Amazon Time Sync, RMS offset 0.4–9 µs over the hour |
| Flags | `--supervise --pin --kernel-ts --rotate-mins 10`, everything else at its default |

## Full universe, 60 minutes

| Check | Result |
|---|---|
| Coverage | 548k–554k tokens sourced of 549k–556k listed, on 442–452 sockets; 280,559 markets seen, 467 removed during the hour |
| Throughput | 41–43k updates/s steady; 9 MB/s to disk (5.4 GB per 10-minute file) |
| Correctness | 8 files, about 33 GB: about 55M snapshots matched the rebuilt book, **0 did not** |
| Host delay (app − kernel receive), 119 windows of 30 s | p50 ≤ 32 µs in every window; p99 ≤ 128–256 µs in most; worst window p99 ≤ 2 ms |
| recv − exch | p50 8–9 ms, p90 20–34 ms, p99 50–348 ms |
| Wire / core queue | wire p50 ≤ 16 ms, p99 ≤ 66 ms; core queue p99 ≤ 8 ms |
| CPU, steady mean, % of one CPU | core threads 47, tokio workers 54, writer 6; load average 1.2 of 4 |
| Memory | RSS 1,815 MB mean, 1,919 MB peak, flat across the hour |
| Writer | queue peak 20 MB of 256; 0 backpressure waits; 0 disk stalls |
| `kill -9` of the child at 55 min | the supervisor restarted it in 1 s and recovered the open file (`dropped_bytes=0`); the new child ran 298 s and exited 0 |
| Venue-side closes | 29 "slow consumer" closes in the hour, each reconnected; 6 quiet-socket reconnects |

Notes on reading these:

- Latency values ending in `≤` are upper bounds of power-of-two histogram buckets, not exact
  values.
- **recv − exch** is our kernel receive time minus the venue's event timestamp. It includes the
  venue's own processing and the network path. Its maximum (600–1,100 s) is the venue stamping
  initial snapshots with the time of the book's last change, so only the percentiles mean anything.
- **Host delay** is the time from the kernel receiving a frame to the collector reading it
  (`--kernel-ts`, `SO_TIMESTAMPNS`). This is the part the collector controls.

## Tuning rounds

Same host type and region, one more day (2026-10-25): five A/B rounds on `--fast-source`,
replicas, the failover window and kernel busy polling. None beat the defaults by enough to change
them; LATENCY, Experiment 4 has the tables. Two results that belong here:

- **Two full-universe collectors on one c7i.xlarge** (round 1) each wrote about 40k updates/s,
  each replayed with 0 mismatches, each held 1.72–1.74 GB RSS and used 54–55% of a CPU in the
  core threads. Host delay p99 rose to ≤ 2 ms: the four vCPUs were shared by two.
- **`kill -9` without the supervisor** (round 5, 500 markets, at 120 s): the file kept 119.8 s
  of data and replayed 3,862 snapshots with 0 mismatches. Before the in-place writer (D11) the
  same kill left nothing.

## Region probe

All three hosts started at the same second and ran 2,000 markets for 10 minutes with
`--lat-detail --kernel-ts`.

| Region | Updates | Replay | recv − exch p50 / p90 / p99 / p99.9 |
|---|---|---|---|
| eu-west-2 London | 675k | 81,288 matched, 0 not | 7 / 21 / 118 / 381 ms |
| eu-west-1 Ireland | 680k | 82,052 matched, 0 not | 12 / 25 / 73 / 232 ms |
| us-east-1 Virginia | 679k | 82,008 matched, 0 not | 43 / 62 / 349 / 725 ms |

London has the best median and Ireland the tightest tail. Virginia adds about 35 ms, the
transatlantic path to the venue's origin. [DEPLOY](appendix/DEPLOY.md) recommends London.

## Offline benchmarks

Run on the same host by `cargo run --release --example bench`. Both use the captured frames in
`tests/fixtures/`.

| Bench | Result |
|---|---|
| `bench cpu`: decode | 1,410 ns per frame, 460 MB/s |
| `bench cpu`: core | 4,665 ns per event applied (2 replicas), 2.43 allocations per event |
| `bench mem 283000` | heap 4,574 B per token with every book at its captured depth (79 levels on average); RSS 2,990 MB |

The memory bench fills every book to the depth of the captured books, which is a worst case. Live
books are shallower, which is why the live RSS (1.9 GB) is below it.

## Where the time goes

From a 30 s `perf` profile at 30 minutes ([perf-top.txt](../bench/aws-2026-10-24/main-eu-west-2/perf-top.txt)):

The profile is flat: no function takes more than 3% of all samples.

- On each core thread the largest items are fixed-point encoding of the hash input
  (`fixed::encode`, 2.3–2.9%), the venue's SHA-1 book hash (`sha1::compress`, 2.3–2.5%) and
  building the hash prefix (1.3–1.4%). Together that is the integrity check, which is what
  catches silent book corruption (Ext 2), so it stays on. Applying events to the book is about
  1.2%.
- On the tokio workers the largest items are JSON decoding (`serde_json`, about 5% across two
  functions), allocation (mimalloc, about 3.3%) and the websocket read path (about 1.4%).

## Against the Codespace

The earlier full-universe runs were on a GitHub Codespace (4 vCPU, 16 GB, West Europe), on older
builds.

| | Codespace | c7i.xlarge |
|---|---|---|
| Host delay p50 | ≤ 128 µs | ≤ 32 µs |
| Host delay p99 | ≤ 2–4 ms | ≤ 128–256 µs |
| RSS on the full universe | 2.8–3.3 GB (build 1a4d84c) | 1.8–1.9 GB (build 7170677) |
| Disk stalls | 8 of 1.0–1.5 s in 2.5 h (about 8.5 MB/s disk) | 0 |
| Clock | drifted +19 to +88 ms against time.google.com | 0.4–9 µs RMS |

The memory difference is mostly the collector, not the host: builds between the two runs cut the
per-token overhead. The latency and disk differences are the host.

## Reproducing it

The whole run, launched and torn down from a laptop with AWS credentials:

```bash
bench/aws/run.sh preflight
```

```bash
caffeinate -i bench/aws/run.sh all
```

It cost about $0.50. The pieces on any Linux host with 4 vCPU and 8 GB:

```bash
cargo run --release --example bench -- cpu
```

```bash
./target/release/collector --supervise --pin --kernel-ts --rotate-mins 10 --out ob.sbe --duration-secs 3600
```

```bash
./target/release/verify ob.20261024T1840Z.sbe
```

The status line every 10 s and the `host delay` lines every 30 s go to stderr.
