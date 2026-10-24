# Latency (Ext 4)

The challenge asks for latency experiments, with correctness first. This file says what is
measured, what is built to keep latency low, what the numbers are, the one experiment run so
far (many sockets, per edge), and which experiments have **not** been run yet. Nothing here claims a latency improvement that was not measured.

## What is measured

Two histograms run in every core, and their sum is in the status line every 30 s:

| Name | From | To | What it includes |
|---|---|---|---|
| `wire` | the venue's event `timestamp` | our receive time on the socket | venue processing, network, TLS, websocket, **and the clock offset between the two machines** |
| `queue` | socket receive | core apply | decode wait and time spent in the core's queue: our own process only |

`verify` also prints `recv_ts − exch_ts` quantiles (p1 to max) for every update in a file.

With `--lat-detail` each core also keeps, **per socket** (`core.rs` `note_arrival`):

| Name | What it is | Clock offset |
|---|---|---|
| `wire` | `recv − exch` on this socket, in signed 1 ms buckets from −512 to +1535 ms, with an exact mean | included, but the same on every socket of the host, so it cancels when two sockets are compared |
| `behind` | how long after the **first** socket to deliver a market event this socket delivered it; 0 when it was first | none: both times are read on our clock |
| `first`, `late` | events this socket delivered first; copies that arrived after the market's 16-event window had moved on (not timed) | — |

Events are matched across sockets by (market, exchange timestamp). Polymarket stamps an event
once and sends the same stamp on every socket. A socket that sends several events in the same
millisecond is timed on its first. Every 30 s the main thread logs one line per socket (with its
edge IP) and one per edge. It costs a 16-entry window per market, so it is off by default.

**Caveats that bound what these numbers can say:**
- **Polymarket timestamps are in milliseconds.** Nothing below 1 ms can be seen.
- **The histogram buckets are powers of two in µs.** A reported p99 is the upper bound of its
  bucket, so 262 ms means 131–262 ms and 524 ms means 262–524 ms. `verify` reports exact
  quantiles.
- **`wire` includes the clock offset.** On the Codespace the clock drifted about 1 ms per minute
  against the exchange. Over the 90-minute soak the median moved from −15 ms to −92 ms: a
  negative latency, which is only possible because of the offset. Absolute `wire` figures are
  therefore only good for comparing runs on the same host and in the same hour.
- `queue` uses one clock and has none of these problems.

## What is built

| Choice | Where | Why |
|---|---|---|
| `TCP_NODELAY` on every socket | `conn.rs` `connect` | no Nagle delay on our pings and commands |
| Each replica pinned to a different edge IP | `conn.rs` `resolve_edges` | the replicas fail independently. Replica `r` uses edge `r mod edges`; each socket's edge is logged when it opens. |
| The source is the replica that is ahead | `core.rs` `switch`, `check_lag` | on a switch, the replica that has seen the most is chosen. A source that falls 3 s behind a peer is replaced. |
| Bounded queues with backpressure | `main.rs`, `writer.rs` | nothing is dropped. A slow stage shows up as `queue` latency, not as loss. |
| Rotation work off the hot path | `writer.rs` finisher thread, spread snapshots | the fsync at rotation used to stall every socket for 1–6 s |
| Make-before-break recycling | `core.rs` | an hourly reconnect never leaves a shard without a live socket |

The source rule is the closest thing to the challenge's "open many, keep the fast ones". Each
shard has two independent sockets, and the output follows whichever is ahead. It is a
correctness mechanism first. It switches on lag of seconds, not milliseconds.

## Results

Full universe (about 566k tokens, about 450 sockets) unless noted.

| Run | Host | `wire` p99 | `queue` p99 | Note |
|---|---|---|---|---|
| Run 7 | MacBook in India, VPN to Singapore | p50 130 s | 61 ms | a 3.3 Mbit/s link cannot carry about 300 MB of start-up dumps |
| Cloud 1 | Codespace WestEurope | 173 ms | 524 ms | 250k tokens (capped) |
| Cloud 3 | Codespace | 524 ms (p99.9 2.46 s) | 524 ms | after the hash hot-spot fix |
| Soak 1, 90 min | Codespace | — | 262 ms | 2 core threads |
| A/B arm A, 35 min, 15-min files | Codespace | 262 ms | 1.05 s | after the rotation-stall fix; its 15 s freeze is diluted over 35 min |
| A/B arm A, 15 min, 5-min files | Codespace | 8.4 s | 1.05 s | includes a forced 15 s freeze |
| Lat 1, 20 min, 400 markets × 8 sockets | Codespace WestEurope | 262 ms (`verify`: p50 7 ms, p99 153 ms) | <1 ms | the experiment below; a small load, so no in-process stalls |

Earlier, from the design probe (8 tokens, 30 s): from India via Singapore, `recv − exch` was p50
166 ms and p90 255 ms.

**What the numbers show:**
- **Location dominated everything.** Moving from the VPN to a host near London took the wire
  latency from minutes to a p99 of a few hundred ms. No code change came close.
- **At full scale, our own process was the next limit.** In the runs before the fixes, the p99
  was set by stalls inside the process, not by the network:
  - timer starvation took the p99 to 71 s;
  - the inline fsync at rotation stopped everything for 1–6 s every 5 minutes;
  - the core CPU was saturated.
  Each was fixed and measured (see the log). The p99 fell to 262 ms at 15-minute rotation.
- **A 15 s freeze shows up as an 8–13 s p99 in that file**, and then recovers. The data stays
  correct, because the books are refilled from the venue rather than guessed.

## Experiment 1: many sockets on the same tokens, by socket and by edge

**Question:** given N sockets carrying the same events, is one consistently faster? Is the
difference a property of the edge IP or of the single connection? Would "open many, keep the
fastest" buy anything?

**Setup** (`bench/lat.sh`, raw log and summary in `bench/lat-2026-10-24/`):
- Codespace in WestEurope, 4 vCPU, build 2b6c150, 2026-10-24 06:01–06:21 UTC.
- 400 markets (800 tokens), the first 400 Gamma lists. One core thread, one shard, **8 replica
  sockets**, all on the same 800 tokens.
- DNS returned 2 edge IPs, 104.18.34.205 and 172.64.153.51, so sockets 0, 2, 4, 6 used the first
  and 1, 3, 5, 7 the second. Four sockets on the same edge differ only by local TCP port and by
  whatever the edge routes them to.
- About 154k market events, each delivered on all 8 sockets. 0 disconnects, 0 lag switches. The
  output verified with 0 replay mismatches.

**Per socket** (`behind` in ms; h1 and h2 are the mean `behind` over the first and second
10 minutes):

| Socket | Edge | First | `behind` mean | p50 | p90 | p99 | h1 | h2 | `wire` mean |
|---|---|---|---|---|---|---|---|---|---|
| 2 | 104.18.34.205 | 76,968 | **1.4** | 0 | 1 | 17 | 1.8 | 0.9 | 12.5 |
| 7 | 172.64.153.51 | 5,170 | 1.9 | 0 | 2 | 14 | 1.9 | 1.9 | 12.8 |
| 1 | 172.64.153.51 | 16,641 | 2.3 | 0 | 1 | 44 | 2.3 | 2.3 | 13.3 |
| 3 | 172.64.153.51 | 36,474 | 2.4 | 0 | 2 | 48 | 2.6 | 2.1 | 13.5 |
| 4 | 104.18.34.205 | 1,971 | 3.2 | 0 | 2 | 60 | 3.3 | 3.1 | 14.2 |
| 5 | 172.64.153.51 | 594 | 3.4 | 1 | 2 | 47 | 4.4 | 2.1 | 14.4 |
| 0 | 104.18.34.205 | 16,436 | 5.0 | 1 | 11 | 26 | 2.4 | **8.3** | 16.0 |
| 6 | 104.18.34.205 | 16 | **10.3** | 7 | 10 | 75 | 9.3 | 11.6 | 21.5 |

**Per edge:**

| Edge | Sockets | `behind` mean | p50 | p90 | p99 | `wire` mean | TCP connect (median of 10) | TLS done |
|---|---|---|---|---|---|---|---|---|
| 104.18.34.205 | 0, 2, 4, 6 | 5.0 | 1 | 8 | 47 | 16.0 | 2.52 ms | 23.6 ms |
| 172.64.153.51 | 1, 3, 5, 7 | 2.5 | 0 | 2 | 39 | 13.5 | 2.48 ms | 23.6 ms |

**What it shows:**
- **Sockets differ more than edges do.** The best socket (2) and the worst (6) share an edge. The
  edge means differ by 2.5 ms, but that gap is sockets 0 and 6. Without them, the two edges are
  within 1 ms. The two edges are equally near at the network level: a 2.5 ms TCP connect and a
  23.6 ms TLS handshake to both. Both are Cloudflare addresses, so they probably reach the same
  location, and choosing between them gains nothing here.
- **A slow socket stays slow.** Socket 6 was 7 ms behind at the median, and about 10 ms in
  both halves. It came first 16 times in 154k events. Socket 2 came first half the time, and both
  halves agree. So the speed belongs to the connection, through whatever it is routed to behind
  the edge, and it persists for at least 20 minutes.
- **But it can change.** Socket 0 went from 2.4 ms to 8.3 ms between the halves without
  reconnecting. A choice of fastest socket has to be remade over time, not once at connect.
- **`wire` agrees with `behind`, and the clock offset cancels.** The `wire` means rank the
  sockets the same way. All of them moved by −4 to −9 ms between the halves, except socket 0,
  which got slower. A shift common to every socket is the host clock or the market, not a
  socket. This is why `behind` is the number to compare.
- **Size of the prize:** a socket picked at random averages about 4 ms behind the fastest of 8.
  The best single socket averages 1.4 ms. With `wire` at a median of 7 ms, keeping the fastest
  socket saves a few ms on average. In the tail it is tens of ms: p99 of 14–17 ms on the best
  sockets against 44–75 ms on the others.
- **The output did not use any of this.** The source is the replica that has *seen the most*,
  and it changes only on failure, not on speed. In this run it changed 469 times, each with a resnapshot
  from the hash ladder, and never on lag.

**Limits:**
- One 20-minute run, at one time of day, from one host.
- 400 markets, about 0.14% of the universe. Under the full load the process's own stalls
  dominate the tail (see Results).
- 8 sockets. That is enough to see a spread, not to model it.
- The `behind` floor is set by our own reads. A socket that is read later in the same scheduling
  turn looks slower by microseconds; the 1 ms buckets hide that.
- The 3 s lag window (DECISIONS D10) is about 40 times the worst socket's p99 here (75 ms).

**What would turn this into a gain** (not built):
1. Keep `behind` per socket over a rolling window, not since start.
2. Prefer the replica with the lowest recent `behind` as source, but switch only when the
   difference is large and lasting. A switch writes a diff, so it costs records.
3. Open more replicas than needed, and recycle the slowest every few minutes instead of every
   hour by age.
4. Run it under the full universe, where it competes with the per-socket CPU cost.

## Not done yet

Done: many sockets on the same tokens, and per-edge comparison (Experiment 1). Four sockets on
the same edge already differ by local port, so Experiment 1 partly covers the source-port idea:
connections to one edge with different ports did differ, by up to 9 ms at the mean. Port was not
varied on purpose, though, so it is still listed below.

| Experiment | How it would run here | What it needs |
|---|---|---|
| **Keep the fastest, as a policy** | Experiment 1's steps 1–3 above, then the same run with and without it, comparing the output's `wire` | rolling per-socket `behind`, a speed-aware source choice |
| **Reconnect the slow socket** | Close the slowest of N, reopen, and see whether the new connection lands somewhere faster | a rule for picking which socket to recycle |
| **Change the TCP source port** | Reconnect the same edge with different local ports, which may land on different load-balancer paths. Compare per connection. | bind the local port before `connect` |
| **Connect right after maintenance** | Note the venue's maintenance windows and compare sockets opened just after one against older ones. | Polymarket's maintenance timing (not looked up) |
| **Host region** | Repeat a run from a second region (e.g. us-east-1) at the same time and compare `wire` medians. | a second host; the user has no AWS account |
| **Clock offset** | Sync the host with chrony or a PTP source before the run, so `wire` can be read as an absolute number. | root on the host |

Every experiment above can reuse `--lat-detail` and `bench/lat.sh`.

**Design rules for running them:**
- report median and p99, and keep null results;
- never let an experiment change what is written: experiments pick which socket is source, they
  never alter the books;
- run them for at least 15 minutes, since the market mix changes through the day.
