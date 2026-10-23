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
