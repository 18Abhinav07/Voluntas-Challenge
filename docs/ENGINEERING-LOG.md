# Engineering log

What we asked for, found, changed, tested and measured, in the order it happened. Newest entry is
at the bottom. Every entry ends with what was still open. All runs are against live Polymarket
(Kalshi is not available from India).

Hardware notes:
- "Local" means a MacBook in India, behind a VPN to Singapore. Its link carried about 3.3 Mbit/s.
- "Codespace" means a GitHub Codespace in WestEurope (Amsterdam): 4 vCPU, 16 GB RAM, a few ms
  from Polymarket's London origin.
- Its clock drifts about 1 ms per minute against the exchange, so absolute latency figures from it
  are only good relative to each other.

## Results at a glance

| Run | Where | Scale | Updates | Exch→recv p99 | Core queue p99 | Replay mismatches | Main finding |
|---|---|---|---|---|---|---|---|
| Run 4 | Local, VPN | 131k tokens, 265 sockets | — | — | — | file cut off | VPN collapsed; reconnect storm |
| Run 6 | Local, VPN | 47.5k of 131k tokens got a book | 1.01M / 300 s | 40 s | 60 ms | 0 | Coverage gap |
| Run 7 | Local, VPN | ~96k of 131k | — | wire p50 130 s | 61 ms | 0 / 614k | Link is the bottleneck |
| Cloud 1 | Codespace | 250k of 446k (cap) | 4.44M / 300 s | 173 ms | 524 ms | 0 / 2.27M | Universe bigger; 429s |
| Cloud 2 | Codespace | 459k tokens, all placed | 5.67M / 300 s | 71 s | 1.05 s | 0 | Timers starved, core saturated |
| Cloud 3 | Codespace | 557k tokens | 9.68M / 300 s | 524 ms | 524 ms | 0 / 5.27M | Hash hot spot fixed |
| Soak 1 | Codespace, 90 min | 559k tokens, 452 sockets, 2 cores | 185M | 262 ms (queue) | 262 ms | 0, all 10 files | Ext 2 over-tuned; slow-consumer closes |
| Soak 2 | Codespace, 30 min | 560k tokens, 2 cores | 64M | 66–145 ms (13 s after freeze) | 2.1 s after freeze | 0, all 4 files | Ext 2 fixed; core CPU is the limit |
| Profile 2 | Codespace, 10 min | 561k tokens, 452 sockets | 18.6M | 247 ms–13.5 s per file | p99 4.2 s | 0 / 10.3M, all 3 files | Core CPU −38%; rotation fsync stalls everything |
| A/B, A | Codespace, 15 + 35 min | 566k tokens | 29.9M + 78.7M | 8.4 s / 262 ms (p99) | 1.05 s | 0 / 55.1M, all files | Fix 1 alone removes rotation stalls |
| A/B, B | Codespace, 15 + 35 min | 566k tokens | 29.9M + 78.7M | 8.4 s / 524 ms (p99) | 1.0 s / 0.52 s | 0 / 54.7M, all files | Fix 2 small gain; fix 3 causes resolve-loop disconnects |

---

## Phase 0 — Design and first build

**Ask:** build the Voluntas challenge as production code (not mocks) against Polymarket: subscribe
to every live market, write `PriceLevelUpdate` records and a `Snapshot` every 60 s through the
template's `AlignedSbeWriter`, and shut down cleanly on SIGINT. Plus the four extensions.

**Decisions approved by the user:**
- Symbols are `<token>:bid` and `<token>:ask`; each snapshot carries one side.
- `--out` takes a path; `-` means stdout.
- A snapshot's `exch_ts` is the last applied event's timestamp.
- Collect all markets.

**Built** (commits `b7d7f97`, `3b26b69`):
- Fixed-point decimals (1e6 = 1), an L2 book, and the `ExchangeAdapter` trait.
- The Polymarket adapter: decode, Gamma discovery, and `price_change` hash verification.
  - Golden tests replay three live captures, and the hash pass rates match the Python reference
    (100%, 100%, 98.9%). The verifier has an inherent ~1% false-miss rate.
- The deterministic core. Time is passed in as an argument, and every side effect goes through an
  `Effects` trait.
  - Each socket keeps its own replica of a book, and there is one emitted book per symbol (what
    readers hold).
  - Updates are emitted only from the source socket.
  - A source switch emits the diff between the books, so a snapshot always equals the previous
    snapshot plus the updates.
- The connection actor: jittered reconnect, a silence watchdog, and an open limiter.
- The SBE writer thread.
- A `verify` binary: it rebuilds every book from the file and checks it against each snapshot.

**Bugs found while building, and fixed:**
- **Pending tokens were never flushed during discovery** (484 tokens, 400 sourced).
  - Fix: `pending_since`, plus a flush on the tick after 5 s.
- **Switch storm.** A fresh book's timestamp made the healthy source look like it was lagging.
  - Fix: track `last_delta` separately; a book dump's timestamp is not evidence of missed deltas.
  - The lag check became asymmetric.
- **15.5k replay mismatches** from switch snapshots.
  - Fix: the emitted-book design with diff-on-switch. Result: 0 mismatches.
- **The universe cache was never saved**, because discovery takes more than 3 minutes.
  - Fix: also save it at shutdown.

## Turn — Run 4 failed at full scale (local, VPN)

**Found:**
- 65.5k markets, 131k tokens and 265 sockets through the VPN collapsed after about 17 minutes:
  2,410 connect timeouts, 1,224 silent sockets and 383 resets. By 09:32 no token had a live source.
- The run was killed by a shell timer, so the output file was cut off.
- Nothing limited total load, and every socket retried on its own, so there was a reconnect storm.

**Open:** a full run from a laptop over a VPN will probably never be clean.

## Turn — Scale fixes

**Changed:**
- Tokens per socket went from 1,000 to 2,500, so 131k tokens need 106 sockets instead of 265.
- A socket cap, `--max-conns 200`. Tokens over the cap are counted as `unplaced`.
- A global outage backoff: after 8 failed connects in a row across all sockets, the gap grows to
  30 s. One received frame resets it.
- Latency counters: `wire` (exchange to socket) and `queue` (socket to core).
- Test runs now use the collector's own `--duration-secs`.

**Tested:** 24 unit tests and 3 golden tests passed. Run 5 got no data: the ISP blocks Polymarket
(DNS resolved to a blocking IP) and the VPN was down. It did shut down cleanly, and the backoff
held retries to about one per 30 s.

## Turn — Run 6: coverage gap

**Found:**
- The 300 s run verified clean: 1.01M updates, 434k snapshots, 0 mismatches.
- But only 47.5k of 131k tokens ever got a book. A REST sample says about 85k should have.
- Coverage grew in steps of 2,500 (one shard) every 30 s, then stopped.

## Turn — Coverage bug diagnosed and fixed (Run 7)

**Root causes:**
1. The open limiter booked each queued socket a fixed slot, using the 30 s outage gap in force when
   it joined the queue. About 100 sockets queued during the opening outage, so their slots ran
   roughly 50 minutes ahead.
   - Fix: read the current gap when the socket's turn comes.
2. A lost initial book was never asked for again.
   - Fix: each replica has a book deadline (120 s after a subscribe, 30 s after a resnapshot).
     Overdue books are re-asked in one command per socket.
3. At full scale the first frame took 27–45 s, longer than the 30 s silence timeout.
   - Fix: a 90 s grace before the first frame.

**Ruled out by direct probes:** a 2,500-token subscribe returns all 2,500 books, and subscribing
during a dump does not cancel it.

**Result:**
- Coverage went from 47.5k to about 96k tokens, with 0 mismatches over 614k snapshots.
- Wire p50 was 130 s. The VPN link (about 3.3 Mbit/s) cannot carry roughly 300 MB of startup
  dumps, so the link is the bottleneck, not the code: 27 s of CPU in 300 s.

## Turn — Choosing a cloud host

- **AWS eu-west-1:** the user has no account.
- **Railway:** the trial credit was refused.
- **Chosen:** GitHub Codespaces in WestEurope. It's free (120 core-hours a month), with 4 vCPU and
  16 GB, and close to London.
- Branch `cloud-run` was pushed to the public repo with the user's approval.

## Turn — Cloud run 1

**Found:**
- Without the VPN, wire latency dropped from minutes to milliseconds: p99 173 ms, 0 disconnects.
  Verify passed with 0 mismatches over 2.27M snapshots.
- The real universe is 222,947 markets, which is 446k tokens. The earlier 65.5k came from a
  discovery that never finished over the VPN.
  - The 200-socket cap held only 250k tokens, so 196k tokens went uncollected.
- 102 new-market lookups got 429 (rate limited) from Gamma.

## Turn — Cloud run 2: cap raised, lookups batched (`3a8ea19`)

**Changed:**
- The socket cap was raised to cover the whole universe.
- New-market lookups are batched; Gamma accepts repeated `condition_ids`.
- Lookups back off on 429.

**Found:**
- All 229k markets and 459k tokens were placed, with no 429s and 0 mismatches.
- The single main loop could not keep up at that size:
  - Exchange→receive p99 rose to 71 s.
  - Only 1 status line was logged in 5 minutes, and only 1.07M snapshots were written.
  - Discovery never finished.
- **Cause:** the loop's biased `select!` put inbound messages ahead of the timers, so under load the
  timers never ran. Then the 8192-slot channel filled, the socket readers stopped reading, and data
  sat in TCP buffers.

## Turn — Timer starvation fixed, core profiled, hash hot spot fixed (`d143760`, `726020a`)

**Changed:**
- Timers now run before inbound messages, and inbound is taken in batches of 256 per turn.
- The open-file limit is raised at startup (the Codespace default is 1024).

**Profile** (perf, 20 s, full universe):
- The main thread was at 99.9% CPU; the 4 async workers were at about 10% each.
- About 45% of samples were in `HashCheck::digest`. Half of that was number formatting, which
  allocated a new string per price level.
- The whole book was re-hashed for each candidate last-trade price (up to about 42 per check).
- SHA1 itself was only about 4%.

**Fix:**
- Each book is hashed once per check, and that partial hash is copied for each candidate price.
- Numbers are written without allocating, and hashes are compared as raw bytes.
- A property test confirms the result equals hashing the full JSON.

**Result:**

| 300 s, full universe | Before | After |
|---|---|---|
| Updates | 5.67M (18.9k/s) | 9.68M (32.3k/s) |
| Snapshots | 1.07M | 6.39M |
| Exch→recv p99 | 71 s | 524 ms (p99.9 2.46 s) |
| Core queue p99 | 1.05 s | 524 ms |
| Verify | OK | OK, 5.27M replayed, 0 mismatches |
| Peak RSS | 2.74 GB | 3.12 GB |

**Open:** the main thread is still at 99.9%, so headroom is thin. The hash miss rate is 0.29%.

## Turn — Review against the challenge spec

**Found (score about 6/10 at that point):**
- Core collector and Ext 1 are strong.
- Ext 2 (per-symbol activity monitoring) was missing.
- Ext 3 had only Polymarket. Kalshi is out of reach from India; the user decided not to build it.
  - Kalshi would need four trait changes: auth headers on connect, sequence-number integrity,
    relative deltas, and per-socket subscription ids.
- Ext 4 had no `LATENCY.md`, and the README was still the template.

**Production gaps, by risk:**
1. One core thread with no CPU headroom.
2. A feed that goes silent on both sockets goes unnoticed.
3. A 3 s failover lag.
4. Nothing had run longer than 5 minutes.

## Turn — Ext 2, per-shard core threads, output rotation, 90-minute soak (`819f8ae`)

**Changed:**
- **Ext 2, quiet-subscription detection:**
  - Each token keeps a smoothed delta rate per minute.
  - A token silent for `factor` expected gaps (clamped between a floor and a ceiling) is
    resubscribed on every replica.
  - Strikes double the wait.
  - Tokens slower than 1 delta a minute are left to periodic recycling.
  - Socket-level detection: a socket whose tokens all go quiet is reconnected.
- **Per-shard core threads:**
  - K core threads (`--core-threads`; the default is half the CPUs, between 1 and 8). Each owns
    whole markets, routed by hash.
  - The main runtime keeps discovery, lookups and all sockets, decode included.
- **Output rotation:** `--rotate-mins` cuts files named by period start. A file is closed only once
  every core has moved past its period.
- Recycling is on by default every 60 minutes, make-before-break.

**Soak 1** (90 min, 4 vCPU, 2 cores, faults injected):
- 185M updates and 110M snapshots. All 10 rotated files verified with 0 mismatches.
- 559k tokens on 452 sockets. Core threads at 50–90% (before: 99.9%). Queue p99 262 ms.
- RSS 3.6–3.9 GB and flat, so no leak.
- **Fault 1:** all 224 sockets to the busiest edge were killed. Tokens moved to their replicas and
  the sockets reconnected.
- **Fault 2:** the process was frozen for 15 s. The p99 in that file rose to 12 s, then recovered.
- 448 recycles at 60 minutes, with no gaps. Clean exit.

**Problems found:**
- **Ext 2 over-tuned:** 400,570 resubscribes in 90 minutes, always at the 200-per-round cap.
  Normal bursty markets were being treated as dead.
- **"Slow consumer" closes** from Polymarket on a few sockets in the last 10 minutes, with the core
  threads at about 90%.
- **Clock drift:** median latency moved from −15 ms to −92 ms over the run.

## Turn — Ext 2 tuned, reader stalls measured, 30-minute soak (`fc4eeac`)

**Changed:**
- Deltas no longer clear strikes. Only a resubscribe whose book corrects the output clears them
  (new metric `quiet_caught`). Strikes decay while a token stays active.
- Each round takes the most overdue tokens first, not hash-map order.
- Defaults: factor 10 → 20, floor 20 s → 60 s, cap 200 → 25 per round.
- The core queue went from 8192 to 65536 (`--core-queue`).
- New `stalls` counter: the number of times a socket reader found its core's queue full and had to
  wait.
- A flaky writer test (one fixed 100 ms sleep) now polls instead.

**Tested:** 41 unit tests and 3 integration tests, three runs in a row. A 2-minute local live run
on 2,000 markets: 0 stalls, 0 mismatches.

**Soak 2** (30 min; fault 1 at 10 min, fault 2 at 20 min):

| | Soak 1 (90 min) | Soak 2 (30 min) |
|---|---|---|
| Quiet resubscribes | 400,570 (~4,450/min) | 14,015 (~470/min) |
| Resubscribes that caught a real miss | not measured | 724 (5%) |
| Replay mismatches | 0 | 0, all 4 files |
| Peak RSS | 3.9 GB | 3.77 GB |
| Slow-consumer closes | a few, last 10 min | 36 |

**Found:**
- `stalls` was 0 for the first 20 minutes, including through fault 1. It jumped to 286,206 in the
  5 minutes after the 15 s freeze, and every slow-consumer close came after the freeze.
- Both core threads were at 90–100% CPU, with no headroom to drain the backlog.
- The last file's exch→recv p99 was 13 s. The data was still correct, because the replicas filled
  in.
- **Conclusion:** the core threads' CPU per update is the limit. A bigger queue only absorbs short
  bursts.

**Open:** find where the core actually spends its CPU, across every component in one run instead
of guessing one suspect at a time.

## Turn — Whole-process profile: every component in one run

**Ask:** stop chasing one suspect at a time. Measure every component in a single run and find the
real bottleneck.

**Method** (`bench/profile-2026-10-23/`; Codespace, 4 vCPU, full universe, 600 s, 5-minute rotation):
- Every 10 s: CPU per thread group (from `/proc/<pid>/task/*/stat`), network rx, disk writes and
  the `stalls` counter.
- perf over the whole process with dwarf call graphs, in two 30 s windows: steady state (t=240 s),
  and recovery right after a 15 s SIGSTOP at t=330 s.
- Reports by thread, then by function for each thread group (self time and inclusive time).
- The first automatic report produced nothing: the thread-name filter was built from output that
  still had call-graph lines in it. The reports were re-run by hand from the same `.data` files.

**Result:** 559,910 tokens on 450 sockets, 19.8M updates and 12.6M snapshots. All 3 rotated files
verified with 0 mismatches (10.3M snapshots replayed). Exit 0.

**CPU per component** (100% = one core; 400% available):

| Component | Steady state | After the freeze |
|---|---|---|
| Order-book threads `core-0` + `core-1` | 50–60% each | 62–80% each, for about 30 s |
| Socket readers (tokio, 4 workers together) | 80–100% in total | 83–98% |
| Writer (SBE + disk) | 6–9% | 5–10% |
| Main thread | ~0% | ~0% |
| Network in | 23–35 MB/s | 27–33 MB/s |
| Disk out | 6–19 MB/s | up to 32 MB/s |

The machine was about half used. No component was pinned at 100% in steady state.

**Where the order-book threads spend their time** (perf, as a share of core-thread samples):

| Function | Steady | Recovery |
|---|---|---|
| Hash verification (`HashCheck::on_event`: build the book prefix, format numbers, SHA1, match) | **58%** | **59%** |
| Periodic tick (`on_tick`, mostly the `check_lag` scan over every replica) | 20% (`check_lag` 16%) | 18% |
| Network interrupts the kernel serviced on these threads (softirq) | 26% | 22% |
| Applying levels to books, emitting output, everything else | ~14% | ~16% |

These shares overlap a little, because interrupts land in the middle of the other functions.
- Hash verification runs on every event for every replica. That's why `hash_ok` (40M) is about
  twice `updates` (19.8M): each token has 2 replicas and both are verified.
- Each check rebuilds the full book prefix (`fixed::encode` for every level, then SHA1).
- `on_tick` walks every replica on every tick to check for lag.

**Socket readers** (42% of all samples): websocket and TLS reads about 16% (mostly syscalls), JSON
decode about 11%. The decode goes through serde's buffered `Content` path (the untagged
`OneOrMany` type) and allocates heavily.

**Writer:** 2.5% of samples, almost all `memcpy`. Not a factor.

**Stalls** (a reader blocked on a full core queue): 7,914 during the startup dumps (t≈55 s), then
**none from 61 s to 354 s**, then 292,770 in the 40 s after the freeze, then none again.

**New finding:** all 18 "slow consumer" closes were on just two sockets, `conn=206` and `conn=207`,
at t=66, 277, 360, 370, and 560–582 s.
- Closes at 277 s and 560–582 s happened while the stall counter was flat. So in those cases the
  core queue was not full and the order-book threads were not the cause.
- From 560 to 582 s, `conn=207` was closed 6 times in 23 s. That looks like a loop: reconnect, get
  the shard's whole book dump, fall behind on it, get closed again.
- The last file's p99 (15.8 s) is most likely this shard repeatedly losing and rebuilding its
  source.

**Verdict:**
1. **At steady state there is no CPU bottleneck.** Every component has headroom.
2. **The order-book threads are the limit during bursts** (startup dump, catch-up after a freeze).
   Almost 60% of their time is hash verification, and another 16–20% is the per-tick lag scan.
   Cutting those two gives the most burst capacity.
3. **The repeated slow-consumer closes are a separate, per-socket problem.** They happen without
   core backpressure, on one shard. The suspect is a single socket's read+decode task not keeping
   up with that shard's volume or its book dump on reconnect. This needs per-connection rx and
   decode counters to confirm.
4. The writer, disk and main thread are ruled out.

**Next fixes, ranked by expected gain:**
- Verify the hash only on the source replica, and check the standby replica only when the source
  misses or on a sample. Expected: about half the hash cost, roughly −30% core CPU.
- Make the lag check incremental (or run it every N ticks) instead of scanning every replica each
  tick. Expected: about −15% core CPU.
- Add per-connection rx and decode counters. Then fix the slow shard: split hot tokens into their
  own socket, or decode off the read task.
- Decode with a tagged or borrowed serde path instead of buffered `Content`, to cut reader
  allocations.

---

## Turn — Hash only the source, incremental lag check, per-socket counters, profile again

**Ask:** verify the hash only on the source replica, make the lag check incremental, add
per-connection rx/decode counters, then rerun the all-component profile and log it.

**Changes** (commit 7ecb6a6):
1. **Hash only the source.** The `BookView` that the hash checker reads from now returns a book only
   when the asking connection is that token's source. Standby replicas are no longer hashed.
   - When the source misses, its standby is still compared book-to-book (that never needed a
     hash).
   - A standby starts being hashed the moment it becomes source.
   - Cost: a standby that drifts is only caught once it becomes source. Its first miss there, plus a
     disagreeing peer, refreshes it at once.
2. **Incremental lag check.** When a standby replica's newest delta advances, its token is put on
   a candidate list, once. Each tick looks only at the listed tokens:
   - the source caught up: drop it;
   - a peer is still ahead and the lag window has not run out: keep it;
   - the window ran out: switch source.
   Before this, every tick walked all 560k tokens.
3. **Per-socket counters.** Each reader counts bytes received, frames, time spent decoding, and
   time spent blocked on a full core queue. Every 30 s the collector logs the totals and the five
   busiest sockets by bytes and by blocked time, then resets the counters.

**Tests:** 2 new unit tests (standby is not offered for hashing; a caught-up source leaves the
candidate list). 46 tests pass, clippy is clean.

**Run:** the same script as last turn. Codespace WestEurope, 4 vCPU. 600 s with 5-minute files,
perf for 30 s at t=240 s, a 15 s freeze (SIGSTOP) at t=330 s, then perf for 30 s during recovery.
561k tokens on 452 sockets, 18.6M updates, 12.9M snapshots. Output in
`bench/profile-2026-10-23-b/`.

**Correctness:** all 3 files verified. 10.3M snapshots replayed, **0 mismatches**. Exit 0.

**CPU, before vs after** (100% = one core, from per-thread `/proc` samples every 10 s):

| Component | Last turn, steady | Now, steady | Last turn, after freeze | Now, after freeze |
|---|---|---|---|---|
| Order-book threads (core-0, core-1), each | 50–60% | **31–38%** | 62–80% | **52–57%** |
| Socket readers (tokio pool), total | 80–100% | 81–100% | — | 99–111% |
| Writer | 6–9% | 7–12% | — | 11% |
| Network in | 23–35 MB/s | 23–40 MB/s | | |

**perf, share of all samples:**

| Thread | Last turn | Now |
|---|---|---|
| Socket readers | 42.1% | **54.7%** |
| core-0 + core-1 | 55.4% | **42.0%** |
| Writer | 2.5% | 3.3% |

- Order-book thread CPU fell by about 38% at steady state. The socket readers are now the largest
  user of CPU.
- `on_tick` self time went from 7.9% of all samples to 0.5%. The lag scan is gone as a cost.
- Hash verification is still 57% of what is left of core time (24.1% of all samples, down from
  32.1% of a larger total). In absolute terms that is about 0.4 of a core, down from about 0.65.
- `hash_ok` is now 18.9M against 18.6M updates. Last turn it was 40M against 19.8M, because both
  replicas were hashed.
- What remains in the hash checker: rebuilding the book prefix (`fixed::encode` for every level,
  4% of all samples on its own), SHA1, and the per-event bookkeeping that still runs on standby
  events (recent prices, last-trade candidates).

**Counters at the end, before vs after:**

| | Last turn | Now |
|---|---|---|
| lag_switches | 1,616 | **294** |
| hash_miss | 224,702 | 100,992 |
| hash_agreed | 190,532 | 95,143 |
| divergent | 622 | 2,126 |
| resnapshots | 47,283 | 23,804 |
| switches | 31,602 | 50,724 |
| disconnects | 13 | 23 |
| slow-consumer closes | 18 | 27 |
| stalls | 302,985 | 147,332 |

- Misses and resnapshots halved, as expected: only one replica is checked.
- `switches` went up with the disconnects: each lost socket moves ~1,200 tokens to their peers.
- `divergent` (a source miss where the peer's book disagrees) went up about 3×. It is still 2% of
  misses. Hashing only the source does not create divergence; it more likely follows the extra
  disconnects and their rebuilds. Watch it in the next soak.

**What the per-socket counters showed:**

1. **The traffic is very uneven.** Four sockets, `206`/`207` (one shard on core-0, both replicas)
   and `16777422`/`16777423` (one shard on core-1), each carry 30–47 MB and 50–79k frames per 30 s
   window. That is 1.0–1.6 MB/s and ~2,000 frames/s each, about 5× the next busiest socket. Those
   four are exactly the sockets the venue closes as slow consumers. All 27 closes were on them.
2. **Decoding is not why they fall behind.** Even the hottest socket spends 0.25–0.5 s decoding per
   30 s (1–2% of a core). All 452 sockets together decode for 7–10 s per 30 s, about 0.27 of a core.
3. **Stalls are global, and they line up with file rotation.** Blocked time is zero in every window
   except three:
   - 16:20:06, the window holding the 16:20 rotation: 330 s of blocked time summed over sockets
     (about 0.7–1.0 s on each).
   - 16:23:36: the planned freeze.
   - 16:25:36, the window holding the 16:25 rotation: 1,153 s summed (about 3.1–3.3 s on each).
     The core logged `writer backpressure waited=6.2s` at 16:25:07. The first slow-consumer close
     followed 0.7 s later.

**Cause of the rotation stall** (from `src/writer.rs`, `Part::finish`): when the last core crosses
into a new period, the writer thread flushes the old file, then calls `sync_all()` on it, then
renames it, all inline. The file was 2.6 GB. While the fsync runs the writer does not drain its
channel. Then:
1. The cores block in `emit`.
2. The core queues fill.
3. Every reader blocks.
4. The hottest sockets' server-side buffers overflow and the venue closes them.

Rotation also asks each core for a snapshot of every published book (1.1M records) in the same
instant, which adds to the burst.

Some closes (16:25:29, 16:26:19) came in windows with no blocked time. Those were the hot sockets
reconnecting and getting their shard's full dump, then falling behind on it again. This is the same
loop seen last turn.

**Verdict:**
1. The two fixes did what they should. Order-book CPU is down ~38% at steady state and ~25–30%
   after the freeze. The lag scan cost is gone and hashing costs about half.
2. **The remaining stalls are caused by the writer's inline fsync at rotation, not by CPU.** It
   stops the whole pipeline for 1–6 s every 5 minutes. This is the next fix and the cheapest.
3. **The slow-consumer closes are on 4 hot sockets.** Rotation stalls trigger them, and the dump
   on reconnect keeps them going. The fix is to split those shards.
4. The socket readers are now the largest CPU user (~1 core). That is TLS plus websocket plus
   decode. Decode is about a quarter of it.

**Next fixes, ranked:**
- Finish rotated files (flush, fsync, rename) on a background thread, so the writer keeps draining.
  Expected: no stalls at rotation.
- Spread rotation snapshots over the following ticks, or make them come from the writer's own copy,
  instead of 1.1M records in one instant.
- Split hot shards: size shards by observed message rate, not by token count. Or move a shard's
  hottest markets to their own socket once their rate is known.
- Hash cost: cache each side's encoded levels so that a delta re-encodes one level, not the whole
  book. Also skip the standby bookkeeping in `HashCheck::on_event`.
- Readers: decode through a tagged or borrowed serde path to cut allocations.

---

## Turn — Fixes 1–4 built, then an A/B of fixes 2 and 3

**Ask:** build the four fixes from the last turn. Then run an A/B: A = fixes 1 and 4, B = A plus
fixes 2 and 3. Use more than one rotation interval, and decide whether 2 and 3 make the collector
better in general, not just on the profiled run.

The fixes are general by design. Nothing names a market, enlarges a queue, or tunes a timeout to
this 4-core host. Fix 3 measures message rates at runtime and decides for itself.

### What was built

**A — commit a0c6d43**
1. **Fix 1, finish rotated files on a background thread.** The writer hands the old file to a
   `finisher` thread, which flushes, fsyncs and renames it. The writer goes straight back to
   draining its channel.
2. **Fix 4, cheaper hash bookkeeping.** `recent` keeps prices as numbers (`VecDeque<Fx>`), not
   strings.
   - We also benchmarked a reusable encode buffer. It gave nothing: SHA1 is about 62% of the digest
     cost. One digest costs 645 / 1,829 / 4,790 ns at book depth 5 / 20 / 60. We reverted it.
   - We dropped the planned per-level encoding cache. By that benchmark it would save about 2% of a
     core, which is not worth the extra state.

**B — commit 6e4df8b = A plus:**
1. **Fix 2, spread rotation snapshots.** Rotation no longer snapshots every published book in one
   tick. Announced tokens join a queue, drained over up to 10 ticks
   (`ROTATE_SPREAD_SECS`, and never more than half the period).
   - A token that changes before its turn is snapshotted first, on the spot (`rot_early`). So
     every file still starts each book with a snapshot. The verifier is unchanged and stays strict.
2. **Fix 3, split hot shards.** Every 60 ticks, after a 180-tick warm-up, each core sums the
   measured rates per shard.
   - It needs at least 3 live shards.
   - If the hottest shard is over `max(median × split_ratio, 20k msg/s)` and both of its sockets
     are up, the core moves the shard's hottest markets to a new shard, until about half its rate
     has moved.
   - The handover is make-before-break, like recycling: the new sockets subscribe first, and the old
     ones unsubscribe once every moved book has arrived (or after the grace period).
   - The new shard is closed to markets listed later. `--split-ratio` defaults to 3, and 0 turns
     splitting off.

**Tests:** 48 unit tests, 6 of them new: lazy rotation, early snapshot, make-before-break split,
no split near the median, handover after grace with a market resolving mid-split, closed shard. The
3 integration tests pass and clippy is clean. A local 300 s smoke run of B with 1-minute files
gave 6 files, all verified, 0 mismatches.

### How the A/B ran

- Two Codespaces of the same type (WestEurope, 4 vCPU, 16 GB), one per arm.
- Both runs were scripted from the same start second, so they saw the same market and crossed the
  same rotations. That was 23 Oct, 17:39–18:54 UTC.
- No perf was attached. Per-thread CPU came from `/proc` every 10 s.
- Each file was verified (niced) and deleted as soon as it was finished.
- Script: `bench/ab.sh`.

| Config | Duration | Files | Freeze (SIGSTOP 15 s) |
|---|---|---|---|
| full-rot5 | 900 s, all markets | 5 min | at t=600 s |
| full-rot15 | 2,100 s, all markets | 15 min | at t=1,500 s |
| small-rot5 | 900 s, `--max-markets 4000` | 5 min | none |

### Results

**Correctness, both arms, every config:** every file verified with **0 mismatches**.
- full-rot5: 16.0M (A) and 15.8M (B) snapshots replayed.
- full-rot15: 39.2M and 38.9M.
- small-rot5: 0.24M each.

**Full universe** (283k markets, ~566k tokens, ~450 sockets). Percentages are of one core.

| | rot5 A | rot5 B | rot15 A | rot15 B |
|---|---|---|---|---|
| Updates | 29.92M | 29.91M | 78.70M | 78.71M |
| Order-book threads, mean (steady) | 76% | 72% | 83% | 80% |
| Order-book threads, peak 10 s | 426% | 363% | 1,120% | 1,068% |
| Socket readers, mean | 109% | 107% | 129% | 124% |
| Writer, mean | 10% | 10% | 11% | 11% |
| Blocked time, rotation windows | **0** | **0** | **0** | **0** |
| Blocked time, freeze window | 502 s | 487 s | 957 s | 1,112 s |
| Writer backpressure warnings | 0 | 0 | 2 | 2 |
| Slow-consumer closes | 11 | 3 | 2 | 3 |
| **Disconnects** | 15 | **163** | 34 | **633** |
| …of which "invalid subscription payload" | 0 | 149 | 0 | 592 |
| Source switches | 49,300 | 40,082 | 91,885 | 79,088 |
| Lag switches | 114 | 1,496 | 683 | 1,166 |
| Exch→recv p99 | 8.4 s | 8.4 s | 262 ms | 524 ms |
| Core queue p99 | 1.05 s | 1.00 s | 1.05 s | 0.52 s |
| `rot_early` | — | 46,755 | — | 54,564 |
| Splits (tokens moved) | — | 19 (394) | — | 8 (20) |

Blocked time is reader time spent waiting on a full core queue, summed over all sockets, per 30 s
window. The rot5 p99 latencies include the freeze.

**small-rot5:** CPU 1–3% everywhere, no stalls, no disconnects, no splits. The two arms are **not
comparable on volume**. Each Codespace wrote its own universe cache in a different order, so
`--max-markets 4000` took a different 4,000 markets on each (A: 792k updates, B: 422k). Only the
correctness and idle-cost results carry over.

### What it shows

1. **Fix 1 alone removed the rotation stall.**
   - Arm A had zero blocked time in every rotation window across 11 rotations, at both intervals.
     Profile 2 had 330 s and 1,153 s blocked in its two rotation windows.
   - Writer backpressure appeared only around the freeze.
   - So by the time fix 2 arrives, there is no rotation stall left for it to remove.
2. **Fix 2 is correct and cheap, but its gain is small.**
   - Order-book CPU was 3–4 points lower on average, and 5–15% lower at the peak.
   - That is about the size of the difference two VMs can show anyway, so we do not claim it as a
     proven speed-up.
   - What it does guarantee is structural: rotation now emits at most a tenth of the books per tick
     instead of all 1.1M at once, whatever the universe size.
   - About 47–55k books per file were snapshotted early because they changed before their turn. The
     verifier stays strict and passes.
   - **Keep it.**
3. **Fix 3, as built, makes things worse.** Disconnects went up 11× at 5-minute files and 19× at
   15-minute files. Lag switches went up too. Slow-consumer closes fell from 11 to 3 at 5-minute
   files, but rose from 2 to 3 at 15-minute files. The cause is the market, not the code path we
   tested:
   - **The hot markets are short-lived.** A shard's rate is dominated by the recurring crypto
     up/down markets, which last 5–15 minutes. A split moves exactly those markets. When they all
     resolve, the venue closes the socket with "all subscribed assets resolved".
   - **Then a reconnect loop.** The reader still holds those tokens. It reconnects, re-sends them,
     gets "invalid subscription payload" (policy close), and retries until the core drops the
     markets. Splits made 98% of all disconnects in B. The bug exists in A too, but a token-count
     shard almost never has all of its markets resolve.
   - **Splitting does not cool the shard.** New hot markets keep landing on the open tail shard. On
     each core, shard 103 was split 6 times in 15 minutes, and the hottest sockets in B's windows
     were still `206`/`207` at 33–39 MB per 30 s.
   - Rebalancing by measured rate assumes a shard's rate is stable. On Polymarket it is not. The
     load moves to whatever markets are newest.

**Verdict:** A + fix 2 is a general improvement: the same output, the same correctness, no rotation
burst, and slightly less core CPU. Fix 3 is a regression in its current form. It should not ship
switched on.

**Open:**
- **The resolved-assets reconnect loop is a real bug in both arms.** When the venue says every
  subscribed asset resolved, the reader should not resubscribe those tokens, and the core should
  drop them.
- **Fix 3 should default to off** (`--split-ratio 0`) until it accounts for market lifetime. One
  way is to place newly listed markets by their expected rate, for example from the series they
  belong to, so the tail shard does not keep re-heating.
- If fix 3 is reworked, rerun it against a fresh arm A at the same time. Pin the universe cache so
  that the small config compares the same markets.

## Turn — Resolved-assets reconnect loop fixed, splitting off by default

These are the two "Open" items from the A/B above.

**The bug.** When every market on a socket resolved, Polymarket closed the socket with
`Normal, "all subscribed assets resolved"`. The `market_resolved` events often never arrived,
because both replicas were closed in the same instant. The reader kept its tokens, reconnected,
re-sent them, and got a `Policy, "invalid subscription payload"` close. It repeated this until
something else dropped the markets. Two more gaps made it worse:
- The core's `Unsubscribe` commands sat in the channel during the backoff, because `cmds` was only
  read inside a live session.
- An empty token list (for example from a split whose live set was empty) could be sent as a
  subscribe or unsubscribe. The venue rejects that payload.

**The fix.**
- **Adapter.** A new trait method, `ExchangeAdapter::all_resolved(close_reason)`, defaults to
  false. Polymarket matches the exact reason string. The venue knowledge stays in the adapter.
- **Reader (`conn.rs`).**
  - That close frame now ends the session as `Exit::Resolved` instead of `Exit::Lost`.
  - The reader drops its tokens and tells the core with the new `Inbound::Resolved { conn, tokens }`,
    followed by `Down`. It does not back off and does not reconnect.
  - While it holds no tokens, it opens no socket. It waits on its command channel and starts again
    when the core gives it tokens. `Close`, or the channel closing, ends it.
  - After each backoff, it applies commands that queued while it was offline, through
    `apply_offline`, before reconnecting. A market removed during the backoff is therefore not
    re-sent.
  - Empty-list subscribe, unsubscribe and resnapshot commands are skipped.
- **Core.** `Core::on_resolved` ends every market that has a token in the list, through
  `remove_market`. That emits zero-quantity updates for its levels and unsubscribes the other
  replica. A new metric, `resolved_closes`, counts these closes. A market listed in that same
  instant could be ended too. The next discovery pass lists it again, so this heals itself.
- **Splitting off by default.** `--split-ratio` now defaults to `0`, and `Config::split_ratio` to
  `None`. The A/B showed that splits chase short-lived markets: the split shard keeps re-heating as
  new markets list, then empties as they resolve. The code stays, and you can opt in.

**Tests:** 50 unit tests and 3 golden tests pass. `cargo clippy --all-targets` is clean. New tests:
- `socket_closed_as_all_resolved_ends_its_markets`: with no `market_resolved` event, the levels go
  to zero, the tokens are gone, and both replicas are unsubscribed.
- `offline_commands_update_the_token_set`: subscribe, unsubscribe and reconnect commands update the
  token set offline, and close or a dropped channel stops the reader.
- The split tests now opt in with `split_ratio: Some(3.0)`.

**Not yet verified live.** No full-universe run has been done on this build. The check is a 15- or
35-minute run with splitting on: "invalid subscription payload" should not appear,
`resolved_closes` should count roughly once per emptied socket, and the disconnect count should be
close to arm A's.

### Live check on `12cc8fe`: splitting forced on (2026-10-24)

The setup matched the A/B's `full-rot5` arm: the full universe, 900 s, 5-minute files, a 15 s
SIGSTOP at about t=600 s, and `--split-ratio 3` to force splitting back on. The script is
`bench/check.sh` (`bench/ab.sh` cut to one config). The log is
`bench/check-2026-10-24/check.log`.

| full-rot5                      | A (fixes 1+4) | B (+2+3, old) | this build, split on |
|--------------------------------|---------------|---------------|----------------------|
| disconnects (metric)           | 15            | 163           | **19**               |
| "invalid subscription payload" | 0             | 149           | **0**                |
| splits                         | 0             | 19            | 23                   |
| slow-consumer closes           | 11            | 3             | 4                    |
| lag switches                   | 114           | 1496          | 38                   |

- **The loop is gone.** At 05:22:26 the venue closed four sockets, both replicas of two split-off
  shards, with "all subscribed assets resolved". Each reader logged "dropping them" and went idle,
  with no reconnect and no payload error. `resolved_closes` = 4.
- **The core had already ended those markets** (`markets=0` in `on_resolved`): `market_resolved`
  arrived first this time. One replica of each pair still held 2 tokens, because the core's
  `Unsubscribe` was still in flight. The other held 0. With 0 tokens, the old reader would have
  reconnected and sent an empty subscribe. The idle path covers both cases.
- **Disconnects are back to arm A's level** (19 against 15), even with 23 splits. The remaining
  closes were 8 quiet-socket reconnects, 4 slow-consumer closes and 3 "server shutting down".
- **The rest is healthy.** All 4 files verified, with 0 snapshot mismatches. Stalls were 0 except
  the SIGSTOP window. Writer backpressure was 0.
- **Caveat:** this run was at 05:10 UTC, and the A/B was at about 17:00 UTC, so the market mix
  differs. These numbers show the loop is fixed. They are not a new A/B.
- Splits still chase the short-lived hot markets: `shard=28` was split 3 times in a row. So
  `--split-ratio 0` stays the default.
