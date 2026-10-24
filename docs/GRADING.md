# Grading guide

How to check every claim in the README yourself: a five-minute path first, then a table that
maps each claim to the test or run that shows it.

## The five-minute path

Needs stable Rust (edition 2024) and network access to Polymarket.

```bash
cargo build --release --bins
```

```bash
cargo test --release
```

This takes about 2 minutes. It runs every unit, golden and end-to-end test, including:
- nine fault injections against a mock Polymarket;
- three against a mock Kalshi.

All of them run offline, on localhost.

```bash
./target/release/collector --out small.sbe --max-markets 2000 --duration-secs 120
```

This is two minutes against live Polymarket: 2000 markets, about 4000 tokens, a handful of
sockets.

```bash
./target/release/verify small.sbe
```

It should end in `OK` with `0 did not`. That means every snapshot in the file equals the book
rebuilt from the snapshot before it plus the updates in between.

What to look at in the collector's stderr while it runs:
- The `status` line every 10 s shows tokens, sockets and the counters (see "Reading the
  counters" below).
- The `done` line at the end gives totals. A clean run has `unplaced: 0` and a high `hash_ok`.

The full universe (about 550k tokens, about 450 sockets) needs about 3.3 GB RAM, 2 cores, and
about 30 GB of disk an hour. Run it with no `--max-markets`. `bench/gate.sh` does that with
faults injected (see below).

## Claim → evidence

| Claim | Shown by | How to rerun |
|---|---|---|
| Output replays exactly | `verify` on every file of every run: 0 mismatches | `./target/release/verify <file>` |
| Every live market is covered | `status`: `tokens` ≈ `sourced`, `unplaced: 0`. The gap (about 1%) is tokens Gamma lists but the CLOB has no book for (`/book` answers "No orderbook exists"); `RUST_LOG=debug` lists them | full-universe run, or `bench/gate.sh` |
| New listings join, resolved markets leave | `markets_added` / `markets_removed` in `done`; e2e `resolved_close_drops_tokens_without_reconnecting` | `cargo test --release --test e2e resolved_close` |
| A dropped socket fails over to its replica | e2e `dropped_socket_fails_over_and_reconnects` | `cargo test --release --test e2e dropped_socket` |
| A silent socket is detected and replaced | e2e `silent_socket_is_switched_off_then_reconnected` | `cargo test --release --test e2e silent_socket` |
| One silent subscription on a live socket is caught | e2e `stalled_subscription_switches_that_token` | `cargo test --release --test e2e stalled_subscription` |
| A slow socket loses the source | e2e `delayed_socket_loses_the_source` | `cargo test --release --test e2e delayed_socket` |
| A corrupt update is caught and repaired | e2e `corrupt_update_is_caught_by_the_hash_and_repaired` | `cargo test --release --test e2e corrupt_update` |
| REST rate limits slow discovery but do not break it | e2e `gamma_429_slows_discovery_down_but_it_completes` | `cargo test --release --test e2e gamma_429` |
| A crash is restarted and the file recovered | e2e `supervisor_restarts_after_a_panic`; live kill -9 in the gate | `cargo test --release --test e2e supervisor` |
| The venue hash check matches Polymarket's | golden tests on captured live frames | `cargo test --release --test golden` |
| Kalshi: a second venue on the same engine | `tests/kalshi_e2e.rs`: baseline, seq gap → failover, listing | `cargo test --release --test kalshi_e2e` |
| Failover, integrity and recycling logic | 46 core unit tests | `cargo test --release --lib core::` |
| Full universe for hours, no restart | 2.5 h soak: 16 files, 0 mismatches, 0 restarts | `REV=<commit> DUR=9000 ROT=10 NAME=soak bash bench/gate.sh` |
| Freeze and kill -9 recover live | 25-min gate: SIGSTOP 15 s, kill -9, 0 mismatches in 7 files | `REV=<commit> DUR=1500 STOP_AT=500 KILL_AT=900 bash bench/gate.sh` |
| Losing an edge moves its tokens | Soak 1: all 224 sockets to one edge killed, 0 replay errors (ENGINEERING-LOG) | done by hand during that soak; no script |
| Which socket is fastest, and whether picking it helps | Experiment 1 and 2 in LATENCY | `bench/lat.sh`, `bench/fast.sh` |
| Whether the TCP source port picks a faster path | Experiment 3 in LATENCY: weak, not significant (p ≈ 0.1) | `REV=<commit> bash bench/port.sh` |

`bench/gate.sh` runs on any Linux host with Rust and a few hundred GB of disk. It:
- checks out `REV` and builds it;
- runs the full universe under `--supervise`;
- verifies each finished file and deletes it;
- samples RSS and CPU every minute.

Its log is `$NAME.log`, with `collect-$NAME.log` for the collector's own output.

## Required flags vs experiment flags

A grader never needs any flag. Every default is the production setting.

| Kind | Flags |
|---|---|
| **Output** | `--out`, `--rotate-mins`, `--supervise`, `--duration-secs`, `--max-markets` (for small runs) |
| **Venue** | `--venue` (`polymarket` by default; `kalshi` needs a key, see KALSHI.md) |
| **Tuning, safe to leave** | `--tokens-per-conn`, `--replicas`, `--max-conns`, `--core-threads`, `--core-queue`, `--recycle-mins`, `--rediscover-mins`, `--opens-per-sec`, `--universe-cache`, `--ntp-server` |
| **Experiments, off by default** | `--lat-detail` (per-socket latency log), `--fast-source` with `--fast-margin-ms` / `--fast-weight` (no consistent gain in two A/B runs), `--split-ratio` (hot-shard splits), `--edge-ip` / `--src-ports` (Experiment 3) |
| **Test only, hidden** | `--ws-url`, `--rest-url` (point the collector at a mock venue) |

## Reading the counters

The `status` and `done` lines print every counter. Most are evidence that a path ran, not
errors.

| Counter | Meaning | Healthy |
|---|---|---|
| `updates`, `snapshots` | Records written | rising |
| `switches` | Tokens whose source moved to another replica. Each is written as a diff, then a snapshot (D8), so readers never see a jump | any; high on a busy venue |
| `switch_snaps` | Snapshots written because of a switch | ≈ switches that changed a book |
| `lag_switches` | Switches because the source fell 3 s behind its peer | small |
| `hash_ok` / `hash_miss` | Polymarket's book hash after each delta matched / did not match our book | ok ≫ miss |
| `hash_healed` | Misses already fixed by a newer full book on that replica | most misses |
| `hash_agreed` | Misses where the other replica has the identical book. Two sockets agree, so the miss is the hash check's own ambiguity, not lost data | most of the rest |
| `divergent` | Misses where replicas disagree. These trigger a resnapshot | small |
| `resnapshots` | Fresh books requested to repair a token | follows divergent and quiet |
| `book_miss`, `book_diffs` | Mid-stream full books that differed from ours; the difference is written as level updates | small |
| `disconnects` | Sockets the venue or the network dropped. Each reconnects; its tokens fail over first | Polymarket drops a few a minute on the full universe |
| `recycles` | Planned make-before-break reconnects of sockets older than `--recycle-mins` | at most 1 per socket an hour (a reconnect resets the age) |
| `quiet_resubs` / `quiet_caught` | Tokens resubscribed because every replica went quiet / of those, how many had really missed data (Ext 2) | caught ≪ resubs |
| `quiet_reconnects` | Sockets reconnected for being quiet while their peer was not | small |
| `unplaced` | Tokens with no socket because `--max-conns` was reached | **0** |
| `rot_early` | Rotation snapshots taken ahead of the drain because an update arrived first | any |
| `stalls` | Times a socket reader found its core queue full and waited. This is backpressure, not loss | 0, except after a freeze or a slow disk |
| `wire` | Venue timestamp → our socket read. **Includes the host clock offset** (next section) | p99 in tens of ms |
| `queue` | Socket read → core apply, one clock | p50 ≈ 0.1 ms |

## Clock-corrected latency

`wire` is our receive time minus the venue's timestamp, so it is off by however far the host
clock is from the venue's. The collector measures that offset against an NTP server (D21) at
start and every 10 minutes:

```
clock offset server=time.google.com:123 offset_ms=19.3 rtt_ms=5.2
```

`offset_ms` is server minus host, the usual NTP sign. A positive value means the host is behind,
so `wire` reads low by that much. To correct it:

**corrected `wire` = printed `wire` + `offset_ms`**, using the offset logged closest in time.
The error is about `rtt_ms / 2`, around 3 ms here.

- **Short runs:** the correction is one number. In the first 25-minute gate (build 174cb77) the offset went from +6 to
  +18 ms, so its `wire` p99 of ≤ 32.8 ms is about 39–51 ms against true time.
- **Long runs on a drifting host:** the printed histogram is cumulative, so it mixes samples taken
  under different offsets.
  - In the 2.5 h soak the Codespace clock ran from +19 to +88 ms behind. Its printed `wire` p99 of
    ≤ 16.4 ms therefore cannot be corrected with one number; true time is that plus 19 to 88 ms.
  - On a real host, chrony keeps the offset under 1 ms and the correction does not matter.
- **What does not need correcting:**
  - `queue` and `behind` (LATENCY) use one clock.
  - Comparisons between sockets on the same host, such as Experiment 1, are unaffected: the
    offset is common to every socket and cancels.

The collector never changes a timestamp by the offset, so a clock step mid-run cannot reorder
the output (D21).
