# Latency (Ext 4)

The challenge asks for latency experiments, with correctness first. This file says what is
measured, what is built to keep latency low, what the numbers are, and which experiments have
**not** been run yet. Nothing here claims a latency improvement that was not measured.

## What is measured

Two histograms run in every core, and their sum is in the status line every 30 s:

| Name | From | To | What it includes |
|---|---|---|---|
| `wire` | the venue's event `timestamp` | our receive time on the socket | venue processing, network, TLS, websocket, **and the clock offset between the two machines** |
| `queue` | socket receive | core apply | decode wait and time spent in the core's queue: our own process only |

`verify` also prints `recv_ts − exch_ts` quantiles (p1 to max) for every update in a file.

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
| Each replica pinned to a different edge IP | `conn.rs` `resolve_edges` | the replicas fail independently. It also makes per-edge comparison possible (not yet done). |
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

## Not done yet

None of the challenge's suggested experiments have been run:

| Experiment | How it would run here | What it needs |
|---|---|---|
| **Open many connections, keep the fastest** | Open N sockets for the same token set and record per-socket `wire` for the same event (Polymarket's event timestamps are shared, so identical events can be matched across sockets). Keep the best K. | per-socket `wire` histograms, and a mode with N replicas over a small token set |
| **Per-edge comparison** | Same as above, with each socket pinned to a different resolved edge IP. Compare median and p99 by edge. | log the resolved edge list (the runs so far did not record it) |
| **Change the TCP source port** | Reconnect the same edge with different local ports, which may land on different load-balancer paths. Compare per connection. | bind the local port before `connect` |
| **Connect right after maintenance** | Note the venue's maintenance windows and compare sockets opened just after one against older ones. | Polymarket's maintenance timing (not looked up) |
| **Host region** | Repeat a run from a second region (e.g. us-east-1) at the same time and compare `wire` medians. | a second host; the user has no AWS account |
| **Clock offset** | Sync the host with chrony or a PTP source before the run, so `wire` can be read as an absolute number. | root on the host |

The cheapest first step is per-socket `wire` histograms. Every experiment above needs them, and
the core already sees `recv − exch_ts` per connection. The reporting is the only missing part.

**Design rules for running them:**
- report median and p99, and keep null results;
- never let an experiment change what is written: experiments pick which socket is source, they
  never alter the books;
- run them for at least 15 minutes, since the market mix changes through the day.
