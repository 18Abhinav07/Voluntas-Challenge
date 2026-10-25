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

With `--lat-detail` each core also keeps, **per socket** (`core/speed.rs` `note_arrival`):

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
  therefore only good for comparing runs on the same host and in the same hour. Since D21 the
  collector logs the offset every 10 minutes. [GRADING.md](GRADING.md) shows how to correct
  `wire` with it, and the 2.5 h soak's drift (+19 → +88 ms).
- `queue` uses one clock and has none of these problems.

## What is built

| Choice | Where | Why |
|---|---|---|
| `TCP_NODELAY` on every socket | `conn.rs` `connect` | no Nagle delay on our pings and commands |
| Each replica pinned to a different edge IP | `conn.rs` `resolve_edges` | the replicas fail independently. Replica `r` uses edge `r mod edges`; each socket's edge is logged when it opens. |
| The source is the replica that is ahead | `core/emit.rs` `switch`, `core/health.rs` `check_lag` | on a switch, the replica that has seen the most is chosen. A source that falls 3 s behind a peer is replaced. |
| Bounded queues with backpressure | `main.rs`, `writer.rs` | nothing is dropped. A slow stage shows up as `queue` latency, not as loss. |
| Rotation work off the hot path | `writer.rs` finisher thread, spread snapshots | the fsync at rotation used to stall every socket for 1–6 s |
| Make-before-break recycling | `core/conns.rs` | an hourly reconnect never leaves a shard without a live socket |

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
| AWS, 60 min | c7i.xlarge eu-west-2, chrony | ≤ 66 ms (p50 ≤ 16 ms) | ≤ 8 ms | full universe; host delay (kernel → app) p50 ≤ 32 µs, p99 ≤ 128–256 µs |

**Region probe** (AWS, the same 2,000 markets for 10 minutes, started at the same second, chrony
on every host; `recv − exch` from `verify`):

| Region | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|
| eu-west-2 London | 7 ms | 21 ms | 118 ms | 381 ms |
| eu-west-1 Ireland | 12 ms | 25 ms | 73 ms | 232 ms |
| us-east-1 Virginia | 43 ms | 62 ms | 349 ms | 725 ms |

London is fastest by median, Ireland has the tightest tail, and Virginia pays about 35 ms for
the Atlantic. Details in PERFORMANCE.md.

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
- **The output did not use any of this** (before Experiment 2). The source is the replica that has *seen the most*,
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

**What would turn this into a gain** (steps 1–3 are built as `--fast-source`, Experiment 2):
1. Keep `behind` per socket over a rolling window, not since start.
2. Prefer the replica with the lowest recent `behind` as source, but switch only when the
   difference is large and lasting. A switch writes a diff, so it costs records.
3. Open more replicas than needed, and recycle the slowest every few minutes instead of every
   hour by age.
4. Run it under the full universe, where it competes with the per-socket CPU cost.

## Experiment 2: a speed-aware source (`--fast-source`)

Experiment 1's steps 1–3, built and run against the old choice.

**What it does** (`Core::check_speed`, every 30 s window):
- Each settled socket's mean `behind` over the window is folded into a score:
  `(1 − w) × old + w × window`, with w = 0.5 in run 1 and 0.25 (`--fast-weight`, now the default)
  in run 2. A window counts only with at least 50 timed events.
- The socket with the lowest score in a shard becomes the preferred one. It changes only when
  the current one is more than a margin worse than the best (hysteresis): 1 ms in run 1, 3 ms
  (`--fast-margin-ms`, now the default) in run 2.
- Tokens move onto the preferred socket only when its book equals the source's book, so the move
  writes nothing. A failover among replicas that have seen the same events also picks it.
- A socket that is the slowest in its shard and at least 4 ms behind the best for 10 windows in a
  row (5 minutes) is recycled, make-before-break, by the same path as the hourly recycle.
- A new connection starts with no score.

**Run** (`bench/fast.sh`, `bench/fast-2026-10-24/`): WestEurope Codespace, 4 CPUs, 06:52–07:12
UTC. Two collectors ran at the same time on the same 400 markets: A with `--lat-detail` (old
choice), B with `--fast-source`. 4 replicas, one shard, one core each. Both verified OK: 1,600
symbols, 31,794 snapshots, 0 mismatches. 0 disconnects and 0 lag switches in both.

`behind` of the events actually written (`src_behind`):

| | n | mean | p50 | p90 | p99 ms | 5-min means ms |
|---|---|---|---|---|---|---|
| A, old choice | 152,976 | 4.8 | 1 | 2 | 88 | 10.5 / 5.0 / 1.7 / 1.8 |
| B, `--fast-source` | 153,050 | 4.3 | 0 | 1 | 145 | 7.5 / 6.6 / 0.7 / 1.3 |

- **B is better at the body:** mean −10%, p50 1 → 0 ms, p90 2 → 1 ms. In the last 10 minutes
  its means were 40–60% lower.
- **B is worse at p99** (145 against 88 ms). The first 10 minutes were noisy in both, and the
  preferred socket changed 7 times (sockets 0, 1, 2, 4, 0, 4, 0). 1 ms of hysteresis is too
  little at this load.
- **The recycle fired once:** socket 3 at 07:00, 7.7 ms behind the best. Make-before-break, no
  gap.
- **Moves:** 5,175 token moves onto the preferred socket and 536 ordinary switches, against 567
  switches in A.
- **One bad record in B:** verify's max receive − exchange was 240,576 ms (A: 3,905 ms). The
  likely cause: a move onto a replica that had seen as many events but held a different book
  wrote a diff, stamped with that replica's last event time, which on a quiet token can be a
  minutes-old book dump. **Fixed after the run:** a move now needs equal books, and a disagreeing
  standby is counted (`fast_disagree`) and left alone. It is not hashed (D9), so speed is no
  reason to trust it.

**Verdict on run 1:** a small gain at the median and p90, a loss at p99 from flapping.

**Rerun with the fix, a 3 ms margin and a longer score** (`bench/fast-2026-10-24-b/`): same
host type, same region, 07:21–07:41 UTC, commit 91211f0. B ran with `--fast-margin-ms 3
--fast-weight 0.25`: the newest window now weighs a quarter, so the score remembers about 4
windows (2 minutes). Both verified OK: 1,600 symbols, 31,794 snapshots, 0 mismatches. 0
disconnects and 0 lag switches in both.

| | n | mean | p50 | p90 | p99 ms | 5-min means ms |
|---|---|---|---|---|---|---|
| A, old choice | 173,370 | 3.0 | 0 | 3 | 20 | 1.7 / 7.0 / 1.7 / 1.9 |
| B, `--fast-source` | 173,369 | 3.2 | 0 | 2 | 26 | 0.5 / 7.6 / 2.9 / 2.1 |

- **No gain this time.** p90 was 1 ms better, the mean 0.2 ms and p99 6 ms worse. The market was
  calmer than in the first run (A's p99 was 20 ms here against 88 ms).
- **Flapping is fixed:** the preferred socket changed twice in 20 minutes (socket 1 at the start,
  socket 0 at 07:29), against 7 times.
- **The outlier is gone:** verify's max receive − exchange was 5,815 ms in both, the same record.
  32 moves were refused because the faster socket's book differed (`fast_disagree`).
- **The recycle fired once again:** socket 2 at 07:26, 10.9 ms behind the best.
- **Where B won and lost:** the first 5 minutes (0.5 against 1.7 ms), when socket 1 was clearly
  fastest. Between 07:26 and 07:31 every socket fell 7 ms behind together, and the pick made then
  (socket 0) did worse than A's choice for the next 5 minutes. A score built in a common stall
  says little about which socket is faster afterwards.
- **Moves:** 1,893 token moves onto the preferred socket, against 5,175 in the first run.

**Verdict after two runs:** the safety problems are fixed (no flapping, nothing stale written),
but there is no consistent latency gain. Run 1 helped the body and hurt p99; run 2 was a tie. At
400 markets the sockets differ by a few ms and that difference does not persist reliably. It
stays opt-in. What could still change this: ignoring windows where every socket is behind (a
common stall), and a run under the full universe, where the spread between sockets is larger.

## Experiment 3: does the TCP source port pick the path?

The challenge names changing the TCP source port as a way to find a faster connection. The idea:
a load balancer hashes the connection's 4-tuple (source IP and port, destination IP and port)
to choose a path or a server. Changing the source port changes the hash. If a port reliably
lands on a fast path, a collector could find good ports and keep them.

**How it was isolated** (`bench/port.sh`, `bench/port_an.py`, `bench/port-2026-10-24/`):
- **One edge.** `--edge-ip` pins all 12 sockets to one Cloudflare edge (104.18.34.205), so
  only the source port differs between them.
- **Two groups:**
  - replicas 0–5 bind the same local ports in every round (41001–41006, `--src-ports`);
  - replicas 6–11 get a new ephemeral port each round.
- **Six rounds:**
  - each round is a fresh 4-minute run on the same 400 markets;
  - rounds are 75 s apart, longer than Linux's 60 s TIME_WAIT, so a fixed port can connect to
    the same edge again.
- **What is measured:** `behind`, how far each socket's copy of an event trails the first copy
  on any socket (Experiment 1). It uses one clock, so the Codespace clock drift does not affect
  it.
- **The test:**
  - if the port decides the path, a fixed-port socket stays fast or slow from round to round, and
    an ephemeral one does not;
  - this is scored by the rank correlation of the sockets between consecutive rounds, and by the
    share of variance that belongs to the slot (ICC);
  - each is checked against a permutation test, which shuffles the slots within each round
    20,000 times.

**Run:** WestEurope Codespace, 4 CPUs, 14:13–14:44 UTC, commit 9cc68f0. Every round:
- verified OK;
- 0 connections lost;
- all six fixed ports bound as asked.

Mean `behind` (ms) per socket and round:

| Slot | Port | r1 | r2 | r3 | r4 | r5 | r6 | mean | sd |
|---|---|---|---|---|---|---|---|---|---|
| 0 | fixed 41001 | 4.6 | 2.1 | 2.0 | 6.5 | 12.8 | 3.6 | 5.3 | 3.7 |
| 1 | fixed 41002 | 4.0 | 1.7 | 2.0 | 7.2 | 2.1 | 1.9 | 3.2 | 2.0 |
| 2 | fixed 41003 | 10.1 | 4.4 | 13.2 | 3.7 | 14.5 | 4.8 | 8.5 | 4.4 |
| 3 | fixed 41004 | 1.9 | 3.3 | 8.8 | 10.6 | 3.7 | 2.7 | 5.2 | 3.3 |
| 4 | fixed 41005 | 16.7 | 4.4 | 3.7 | 10.7 | 10.5 | 2.1 | 8.0 | 5.1 |
| 5 | fixed 41006 | 3.5 | 3.8 | 3.1 | 5.9 | 4.7 | 4.2 | 4.2 | 0.9 |
| 6 | ephemeral | 11.6 | 7.8 | 6.3 | 9.2 | 4.4 | 3.3 | 7.1 | 2.8 |
| 7 | ephemeral | 3.4 | 10.1 | 4.7 | 13.7 | 14.0 | 7.1 | 8.8 | 4.1 |
| 8 | ephemeral | 3.8 | 4.1 | 3.5 | 7.3 | 3.7 | 2.9 | 4.2 | 1.4 |
| 9 | ephemeral | 4.1 | 13.2 | 2.8 | 6.5 | 2.8 | 13.9 | 7.2 | 4.7 |
| 10 | ephemeral | 4.9 | 12.6 | 6.3 | 4.7 | 5.8 | 4.0 | 6.4 | 2.9 |
| 11 | ephemeral | 11.1 | 1.8 | 9.7 | 4.8 | 2.6 | 12.9 | 7.2 | 4.3 |

| Group | Rank correlation, round to round | Mean | p | ICC | p |
|---|---|---|---|---|---|
| Fixed port | +0.54 +0.66 −0.09 −0.49 +0.71 | +0.27 | 0.10 | +0.12 | 0.09 |
| Ephemeral | −0.26 −0.49 −0.43 +0.43 −0.37 | −0.22 | 0.87 | −0.02 | 0.54 |

**Findings:**
- **Sockets on one edge differ a lot.** Within each round, the slowest socket's mean was 10–15 ms
  behind the fastest one's. So the spread Experiment 1 saw within one edge is real and repeats.
- **A fixed port keeps its speed only weakly.** Fixed-port sockets kept their order a little
  (ICC 0.12: about an eighth of their variance belongs to the port). Ephemeral sockets, as
  expected, kept none.
  - The difference is not significant (p ≈ 0.1).
  - Even taken at face value, the port explains about an eighth of the spread. The rest changes
    from connection to connection.
- **Picking a port does not beat picking a connection.**
  - The steadiest fixed port (41006: 4.2 ms, sd 0.9) did no better than the best ephemeral slot
    (4.2 ms, sd 1.4).
  - The fastest fixed port in round 1 (41004, 1.9 ms) averaged 5.8 ms over the next five rounds,
    worse than the group.
  - The fixed group's mean (5.7 ms against 6.8 ms) was lower in only 4 of 6 rounds, and mostly
    because of rounds 2 and 6, so it is not a stable gain.
- **What this suggests about the path:**
  - Cloudflare's load balancer does hash the 4-tuple, which would explain the weak persistence.
  - But most of a socket's speed is set after that: which websocket backend behind Cloudflare
    serves the connection, and how loaded it is at the time. Neither is tied to the port.

**Verdict:** on Polymarket, choosing the source port is not a useful lever. At best it shows a
weak effect that does not reach significance. The lever that matters is measuring each live
connection and choosing among connections. That is Experiment 2's approach, plus recycling the
slow ones, since a new connection draws again from the spread.

**Limits:**
- 6 rounds of 12 sockets, one edge, one region, one half-hour on a Saturday afternoon UTC.
- 400 markets; the `behind` floor is our own read order (Experiment 1).
- Ports 41001–41006 only. Another range could hash differently, but nothing here suggests a
  range effect.
- Kalshi has not been tried. Its edge layout differs, and the experiment needs a key.

## Experiment 4: tuning on AWS (`bench/aws/host.sh tune`)

One c7i.xlarge in London (eu-west-2), 2026-10-25, rev 5252f41. Each round ran two arms at the same
time on the same markets, so an arm is compared only with its partner: both share one host,
one network path and the same minutes of venue traffic. Round 4 ran its arms one after another,
since a sysctl is host-wide. `python3 bench/tune_an.py bench/aws-tune-2026-10-25/main-eu-west-2`
prints every number below. "Written" is recv − exch of the deltas the source wrote; "behind" is
how far the source's copy came after the first copy from any socket.

| Round | Arm | Written mean / p50 / p99 ms | Behind mean / p99 ms | Switches (lag) + tokens moved to the fast socket | Snapshots for switches | Verify |
|---|---|---|---|---|---|---|
| r1 full universe, 20 min | default (`--lat-detail`) | 9.1 / 5 / 52 | 1.5 / 13 | 44,767 (8) | 45,316 | OK |
| | `--fast-source` | 8.8 / 5 / 50 | 1.2 / 11 | 48,492 (614) + 167,160 fast | 216,296 | OK |
| r2 2,000 markets, 15 min | `--fast-source` | 7.5 / 5 / 30 | 0.4 / 3 | 366 (0) + 191 fast | 557 | OK |
| | `--fast-source --replicas 4` | 7.6 / 5 / 31 | 0.7 / 5 | 373 (0) + 4,127 fast | 4,500 | OK |
| r3 2,000 markets, 15 min | `--lag-ms 3000` (default) | 7.7 / 5 / 36 | 1.1 / 15 | 4,136 (228) | 4,145 | OK |
| | `--lag-ms 1000` | 7.1 / 5 / 28 | 0.8 / 6 | 289 (0) | 296 | OK |

**r1, the speed-aware source under the full universe.** The common-stall filter fired 318 times
(windows where even the best socket was ≥ 4 ms behind and were left unscored). With it,
`--fast-source` took 0.3 ms off the written mean and 2 ms off the p99, and 0.3 ms off the mean
behind. The price is churn: 167,160 tokens moved onto a faster socket, about 4.8× the
snapshots spent on switching, and 614 lag switches instead of 8 (a token just moved has a source
its peers are often ahead of). 14,067 moves were refused because the fast socket's book differed
from the source's. Both arms
wrote about 40k updates/s and replayed with 0 mismatches. **Kept opt-in:** a 3% gain on the mean
does not pay for 4.8× the switching.

**r2, more replicas.** Four sockets per shard instead of two gave no gain: behind got slightly
worse (0.4 → 0.7 ms mean) and tokens moved rose 20×. More copies give the score more chances
to chase noise. **Kept at 2.**

**r3, a 1 s failover window.** The 1 s arm raised **0** lag switches in 15 minutes on 2,000
markets, so a window a third as long caused no false switching at this load. The 3 s arm's
228 lag switches and worse p99 came from its own sockets (it also had 2 disconnects and 14× the
switches), not from the window: a shorter window cannot remove switches. So r3 shows that 1 s is
safe at 2,000 markets; it does not show that 1 s is faster. **Default kept at 3 s:** the full
universe has in-process stalls (r1: 8 lag switches at 3 s) and 1 s has not been run there. For
up to a few thousand markets, `--lag-ms 1000` cuts the worst-case stale window by two thirds.

**r4, kernel busy polling** (`net.core.busy_read=50`, `busy_poll=50`), 2,000 markets, 6 minutes
each, off → on → off:

| Arm | Host delay p50 / p99 µs (median window) | Wire p99 ms | tokio CPU % |
|---|---|---|---|
| off (1st) | ≤ 128 / ≤ 512 | ≤ 131 | 1 |
| on | ≤ 64 / ≤ 512 | ≤ 65.5 | 3 |
| off (2nd) | ≤ 64 / ≤ 512 | ≤ 32.8 | 2 |

The second "off" arm matched or beat "on", so the difference between the arms is drift in the
venue's traffic, not polling. Host delay is already tens of µs against a recv − exch of
milliseconds. **Null result; not applied.**

**r5, `kill -9` with the in-place chunk writer.** 500 markets, unsupervised, killed at 120 s.
The file kept 119.8 s of data (5,804 snapshots, 7,255 updates); `verify` replayed 3,862
snapshots with 0 mismatches. The old writer would have left an empty file.

**Host delay under the full universe.** In r1 two full-universe collectors shared the 4 vCPUs, and
host delay p99 rose to ≤ 2 ms (median window), against ≤ 128–256 µs for one collector
(PERFORMANCE). Two collectors per host is a test setup; it says the host has no room for a
second copy, not that one copy is slow.

**What this settles:** the defaults stand: two replicas, a 3 s window and a plain source. The
latency left on the table is in the venue and the network path, which no flag here moves:
recv − exch p50 was 5 ms in every arm.

## Not done yet

Done:
- many sockets on the same tokens, and a per-edge comparison (Experiment 1);
- a speed-aware source, and recycling the slowest socket (Experiment 2);
- the TCP source port (Experiment 3);
- three host regions at the same time, with synced clocks (the region probe above);
- the speed-aware source with a common-stall filter under the full universe, replicas, the
  failover window and kernel busy polling (Experiment 4).

| Experiment | How it would run here | What it needs |
|---|---|---|
| **A 1 s failover window on the full universe** | r3 of Experiment 4 at full scale, `--lag-ms 1000` against the default | one more 20-minute A/B |
| **Does a recycle land somewhere faster?** | Experiment 2 recycled once. Track the new socket's score against the old one's over many recycles | more recycles: a lower `slow_windows`, or a longer run |
| **Connect right after maintenance** | Note the venue's maintenance windows and compare sockets opened just after one against older ones. | Polymarket's maintenance timing (not looked up) |
| **PTP clock** | chrony on Amazon Time Sync already gives µs-level offsets; the ENA PTP device would tighten it further. | an instance type that exposes `/dev/ptp*` (the c7i.xlarge did not) |

Every experiment above can reuse `--lat-detail` and `bench/lat.sh`.

**Design rules for running them:**
- report median and p99, and keep null results;
- never let an experiment change what is written: experiments pick which socket is source, they
  never alter the books;
- run them for at least 15 minutes, since the market mix changes through the day.
