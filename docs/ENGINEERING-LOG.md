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
| Soak 3 | Codespace, 2.5 h, `--supervise` | 551k tokens, 452 sockets | 320M | wire ≤ 16.4 ms | ≤ 65.5 ms | 0, all 16 files | Stable 2.5 h; RSS 2.8–3.3 GB; disk waits on rotation |
| Final gate | Codespace, 25 min, kill -9 + SIGSTOP | 559k tokens | 23.5M | — | — | 0, all 7 files | Every recovery path works on the submission build |
| Perf gate 1 | Codespace, 10 min, `--pin --kernel-ts` | 562.7k tokens | — | recv−exch p99 57–321 ms; host p99 ≤ 2–4 ms | — | 0 / 9.67M | **RSS 1.73 GB (−43%)**; found `--pin` 1-core bug |
| Perf gate 2 | Codespace, 10 min, 2 cores, io 1 | 561k tokens | — | host p99 ≤ 8–16 ms | — | 0 / 10.77M | One io worker too few → default ≥ 2 |

## Bottlenecks: how each was found, fixed and measured

One line per limit we hit, in order. The turn entries below hold the detail and the logs.

| # | Bottleneck | How we found it | Fix | Before → after |
|---|---|---|---|---|
| 1 | Laptop link behind a VPN (3.3 Mbit/s) | Run 4 collapsed into a reconnect storm; Run 7 had wire p50 130 s with 27 s of CPU in 300 s | Move to a cloud host near London (Codespace WestEurope); global outage backoff, socket cap | wire p99 minutes → 173 ms |
| 2 | Coverage stuck at 47.5k of 131k tokens | Coverage grew one shard per 30 s, then stopped; probes ruled out the venue | Open limiter reads the current gap; book deadline and re-ask; 90 s first-frame grace | 47.5k → 96k (all the link could carry), later 100% placed |
| 3 | Socket cap and Gamma 429s | Cloud 1: 196k tokens unplaced, 102 lookups rate-limited | Cap sized to the universe; batched lookups with 429 backoff | 250k → 459k tokens placed, 0 429s |
| 4 | Timers starved by inbound messages | Cloud 2: one status line in 5 min, p99 71 s | Timers first, inbound in batches of 256 | p99 71 s → 524 ms |
| 5 | Hash check: 45% of the core | `perf` profile: number formatting allocated per level, whole book rehashed per candidate price | Hash once per check, copy the partial hash per candidate, format without allocating | 18.9k → 32.3k updates/s |
| 6 | One core thread at 99.9% | Per-thread CPU from `/proc` | K core threads, each owning whole markets | 99.9% → 50–90% per core |
| 7 | Ext 2 over-tuned | Soak 1: 400k resubscribes in 90 min, always at the cap | Rate-aware quiet detection, strikes double the wait | Resubscribes cut to the truly quiet tokens |
| 8 | Hashing standbys, full-universe lag walk | Whole-process profile | Hash only the source; incremental lag-candidate list | Core 50–60% → 31–38% |
| 9 | Rotation fsync stalled every reader | Profile 2: 330 s and 1,153 s blocked in rotation windows | Finisher thread fsyncs and renames off the writer; rotation snapshots spread over 10 ticks | Blocked time in rotation → 0 in 11 rotations |
| 10 | Hot-shard split caused a reconnect loop | A/B: disconnects ×11–19; venue closes "all subscribed assets resolved" | `all_resolved` exit in the adapter; drop resolved tokens; splitting off by default | Loop gone; split stays opt-in |
| 11 | Memory 2.8–3.3 GB (BTreeMap books, HashMap replicas, SipHash, 3 symbols per token) | Offline `bench mem`: 7.6 KB/token heap | mimalloc, flat sorted books, inline replicas, boxed cold fields, one symbol, lazy hash state, FxHash | RSS 3.0 GB → **1.73 GB** live; overhead 1,528 → 652 B/token |
| 12 | Disk stalls could reach the books | 2.5 h soak: 8 waits of 1–1.5 s at rotation | Byte-bounded 256 MiB writer queue with hwm/waits counters | Gate: hwm 21 MB, 0 waits |
| 13 | `recv_ts` included TLS, parse and scheduling time | Design review of LATENCY | `--kernel-ts`: SO_TIMESTAMPNS kernel arrival time, `host delay` histogram | Host adds p50 ≤ 128 µs, p99 ≤ 4–8 ms |
| 14 | `--pin` started 1 core thread instead of 2 | Gate 1 log: `cores=2` budget, `cores=1` started | Count CPUs once in `main`, before pinning | 2 core threads, 281k/280k tokens each |
| 15 | One tokio worker for ~450 TLS sockets | Gate 2 host delay 4× worse; 5-minute A/B of three layouts | io workers default to at least 2 | host p99 8–16 ms → 4–8 ms |

**Not bottlenecks, ruled out by measurement:** the TCP source port (Ext 4 Exp. 3, weak, p ≈ 0.1);
hyperthread siblings for the core threads (A/B); the ~1% unsourced tokens (the venue has no book).

**Still open:** levels are held 3× at 16 B each (about 1 GB of the 1.7 GB); a `u16` tick plus a
quantity array would save about 500 MB. Absolute latency needs a host with chrony (the AWS run).

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

## Turn — Part 2 docs: README, ARCHITECTURE, LATENCY, DECISIONS, KALSHI

**Ask:** write the submission docs.

**Written** (docs only, no code changed):
- `README.md` replaces the template text. It covers build and run, flags, the output format,
  `verify`, tests, results and known limits.
- `docs/ARCHITECTURE.md`: components, the replica and emitted-book model, and a table of how each
  Ext 1 failure is detected and handled. Also covers Ext 2, the adapter boundary, rotation and CPU.
- `docs/LATENCY.md` (Ext 4): what `wire`, `queue` and `verify` measure, and their limits:
  ms venue timestamps, power-of-two buckets, clock drift. Also what is built and the results by
  run. **It says plainly that none of the challenge's latency experiments have been run**, and
  gives a plan for each.
- `docs/DECISIONS.md`: 17 decisions with the rejected option and the evidence.
- `docs/KALSHI.md` (Ext 3): Kalshi's feed and the mapping (`:ask` = NO bids at 1−p). It lists
  six gaps against the trait, four of them real trait changes. Unverified details are marked
  "to confirm".

**Found while checking the docs against the code:**
- **A source switch writes a diff, not the snapshot the challenge asks for.** This was decided in
  Phase 0, after switch snapshots caused 15.5k replay mismatches. It is now written up as
  deviation D8, with the one-line change that would revert it.
- **The 3 s failover window has no measurement behind it.** The per-socket delivery gap was never
  measured, and the window has no CLI flag.
- **The resolved edge IPs were never logged** in any run, so per-edge comparison needs that
  first.
- **Process crashes are not restarted in-process.** This is listed under known limits.

**Open:**
- Ext 4 experiments, starting with per-socket `wire` histograms.
- A `--lag-ms` flag.
- A supervisor example.
- Zip and email (ask first).

## Turn — Ext 4: per-socket latency, then 8 sockets on the same tokens

**Ask:** add per-socket wire histograms and edge logging, run the many-connections and per-edge
experiment on a Codespace, fill in LATENCY.md.

**Built** (2b6c150):
- `--lat-detail`. Per socket, the core keeps:
  - `wire` (`recv − exch`) in signed 1 ms buckets, with an exact mean, so a negative clock
    offset is no longer clamped to 0;
  - `behind`: time after the first socket to deliver the same (market, exch_ts). It needs no
    clock sync.
- Matching uses a 16-event window per market. A copy that misses the window is counted `late`,
  not timed. A single "last event" slot would have dropped the slow sockets' copies on busy
  markets and biased the result (DECISIONS D18).
- Every 30 s and at shutdown, main logs one line per socket with its edge IP, and one per edge.
  Socket opens log their edge at info.
- 2 new unit tests: negative wire and an exact mean; first/behind/late and same-ms dedup. 52
  unit + 3 golden pass, clippy clean.

**Run** (`bench/lat.sh`, `bench/lat-2026-10-24/`): WestEurope Codespace, 06:01–06:21 UTC, 400
markets, one core, one shard, 8 replica sockets, 2 edges (104.18.34.205, 172.64.153.51), 4
sockets each. About 154k events on every socket. 0 disconnects, 0 lag switches, verify OK with 0
mismatches. Codespace deleted.

| | best socket (2) | worst socket (6) | edge .34.205 | edge .153.51 |
|---|---|---|---|---|
| `behind` mean / p50 / p99 ms | 1.4 / 0 / 17 | 10.3 / 7 / 75 | 5.0 / 1 / 47 | 2.5 / 0 / 39 |
| delivered first | 76,968 | 16 | | |

- **Per-socket spread is larger than per-edge.** The best and worst sockets share an edge. TCP
  connect (2.5 ms) and TLS (23.6 ms) are the same to both edges.
- **Speed sticks to the connection:** socket 6 was about 10 ms behind in both halves, and socket
  2 was first about half the time in both.
- **It can drift:** socket 0 went from 2.4 to 8.3 ms with no reconnect.
- **`wire` means rank the sockets identically.** Every socket's `wire` mean moved about −4 to −9
  ms between the halves, which is the common clock/market shift that `behind` removes.
- **The 3 s lag window is about 40×** the worst socket's p99 at this load.
- **Gain if acted on:** a few ms at the mean and tens of ms at p99. The source choice does not
  use it.

**Docs:** LATENCY gains Experiment 1 (per-socket and per-edge tables, findings, limits, the
four steps to make it a policy). DECISIONS D10 now cites the measurement, and D18 is new. README
and ARCHITECTURE mention `--lat-detail`.

**Open:**
- Speed-aware source choice / recycle the slowest socket.
- A shorter `--lag-ms`.
- A repeat at another hour and under the full universe.
- A supervisor example.
- Zip and email (ask first).

## Turn — Speed-aware source and slow-socket recycle (`--fast-source`)

**Asked:** prefer the socket with the lowest recent `behind`, recycle a socket that stays
slowest, test it and log it.

**Built** (`src/core.rs`, opt-in `--fast-source`):
- Per socket, `behind` is summed per 30 s window and folded into a score (EWMA, weight 0.5). A
  window needs 50 timed events. Only settled sockets are scored (not retiring, not a replacement).
  A reconnect resets the score.
- Per shard, a preferred socket (`Shard.fast`), changed only past a 1 ms margin.
- Tokens move onto it when its book equals the source's (first version: when it had seen as
  many events; see below). Failover breaks ties towards it.
- A socket that is slowest and at least 4 ms behind the best for 10 windows is recycled
  make-before-break through a new shared `recycle()` helper, with the hourly recycle's limits.
- New metrics: `fast_picks`, `fast_switches`, `slow_recycles`, `fast_disagree`, and
  `src_behind`, the `behind` of the events actually written.
- 3 new unit tests: following the fastest past the margin (and not onto a disagreeing book),
  failover among equals, and the slow recycle. 55 unit (1 ignored) + 3 golden pass, clippy clean.

**Run** (`bench/fast.sh`, `bench/fast-2026-10-24/`): WestEurope Codespace, 06:52–07:12 UTC, two
collectors at once on the same 400 markets, 4 replicas each. A: old choice (`--lat-detail`). B:
`--fast-source`. Both verify OK, 31,794 snapshots, 0 mismatches. 0 disconnects, 0 lag switches.
The first start failed because `fast.sh` ran from `bench/` (fixed in 103ac51).

| `src_behind` | n | mean | p50 | p90 | p99 ms |
|---|---|---|---|---|---|
| A | 152,976 | 4.8 | 1 | 2 | 88 |
| B | 153,050 | 4.3 | 0 | 1 | 145 |

- Body better (mean −10%, p50/p90 −1 ms), p99 worse. The preferred socket changed 7 times.
- One slow recycle (socket 3, 7.7 ms behind).
- B's verify max receive − exchange was 240.6 s (A: 3.9 s). Likely a move onto a replica with a
  different book, whose diff carried an old book-dump timestamp. **Fixed:** a move needs equal
  books; disagreeing standbys are counted in `fast_disagree`. Not rerun with the fix.

**Docs:** LATENCY Experiment 2 and an updated "Not done yet"; DECISIONS D19; README flag row and
known-limits line; ARCHITECTURE metrics bullet. Codespace deleted.

**Open:**
- Rerun the A/B with the equal-book fix, a 3 ms margin and a longer score.
- A shorter `--lag-ms`.
- A repeat at another hour and under the full universe.
- A supervisor example.
- Zip and email (ask first).

## Turn — Fast-source A/B rerun: equal-book fix, 3 ms margin, longer score

**Asked:** rerun the A/B with the equal-book fix, a 3 ms margin and a longer score, then update
LATENCY Experiment 2.

**Changed:** `Fast.weight` (the newest window's share of the score) and two flags,
`--fast-margin-ms` and `--fast-weight`. Defaults are now 3 ms and 0.25 (about 4 windows of
memory). `bench/fast.sh` passes `FAST_ARGS` to B. The unit tests pin the old 1 ms / 0.5 where
their numbers depend on it. 55 unit (1 ignored) + 3 golden pass, clippy clean. Commit 91211f0.

**Run** (`bench/fast-2026-10-24-b/`): WestEurope Codespace, 4 CPUs, 07:21–07:41 UTC, 400 markets,
4 replicas each. Both verify OK, 31,794 snapshots, 0 mismatches, 0 disconnects.

| `src_behind` | n | mean | p50 | p90 | p99 ms |
|---|---|---|---|---|---|
| A | 173,370 | 3.0 | 0 | 3 | 20 |
| B | 173,369 | 3.2 | 0 | 2 | 26 |

- A tie. A calmer market than run 1 (A's p99 20 ms against 88).
- Preferred socket changed twice (7 in run 1). 1,893 token moves (5,175). 32 moves refused for
  differing books.
- Max receive − exchange 5,815 ms in both: the 240 s outlier of run 1 is gone.
- One slow recycle (socket 2, 10.9 ms behind).
- B lost ground after picking during a common 7 ms stall at 07:26–07:31.

**Housekeeping:** stopped two stale local ssh tasks (one for the deleted `voluntas-check`
Codespace, running 2.5 h). Codespace `voluntas-fast2` deleted.

**Open:**
- Skip score windows in which every socket is behind (common stall).
- Fast-source under the full universe.
- A shorter `--lag-ms`; a supervisor example.
- Zip and email (ask first).

## Turn — Task set 1: stdout default, switch snapshot, fail-fast, `--supervise`

**Asked:** make all the fixes from task sets 1–3 (Kalshi aside), then soak for 2–3 hours once
everything is right.

**Changed:**
- **stdout is the default output** (D4). `--out <file>` still works. It refuses a terminal.
- **A source switch writes the diff, then a snapshot** (D8). All source changes go through
  `Core::adopt`. New metric `switch_snaps`. Fast-source moves (equal books) now write only the
  snapshot (D19).
- **Fail fast on a panic** (D20). A panic hook cancels the main loop. Shutdown runs in order and
  the exit is non-zero. Hidden `--fault-panic-after-secs` injects one for tests.
- **`--supervise`** (D20, `src/supervise.rs`). Restart with backoff, whole-chunk relay, reset
  marker `*`, `.part` recovery, `rN` names for reopened periods. `verify` counts resets.
- Tests: 60 unit (1 ignored) + 3 golden pass. New: relay, restart loop, reset marker read back
  through the template reader, `.part` recovery, free name.

**Runs** (local Mac, live Polymarket, 50 markets):

| Fault | Restarts | verify |
|---|---|---|
| Panic injected every 25 s, 2 min | 3 | OK, 0 mismatches, 3 resets |
| `kill -9` child, single file | 1 | OK, 0 mismatches, 1 reset |
| `kill -9` child, `--rotate-mins 1` | 1 | both files OK; empty `.part` removed |

- `kill -9` lost the first child's data in the single-file run: at 50 markets it never filled a
  64 MiB chunk. That is the D11 cost, now written in README "Known limits".

**Open:** mock venue and end-to-end fault tests, CI, clock offset, p99 profiling, gate run, soak.

## Turn — Task set 2: mock venue, end-to-end fault tests, lag-check fix

**Changed:**
- **Endpoints can be overridden.** Hidden flags `--ws-url` and `--gamma-url`, and
  `Polymarket::with_endpoints`. The defaults are unchanged.
- **`tests/e2e.rs`: a mock venue on localhost.** It has a Gamma keyset and lookup over raw
  HTTP, and a market websocket with books, `price_change` deltas carrying correct SHA1
  hashes, and PING/PONG. Socket 0 is every token's source, because the other socket gets
  its books 500 ms later. Each test runs the real `collector` binary, reads the final
  metrics, and runs `verify` on the output.

| Test | Fault | Asserted |
|---|---|---|
| baseline | none | 2 sockets, hashes verify, no switch or miss |
| drop | TCP dropped at 5 s | switches + switch snapshots, disconnect, reconnect |
| silent | no frames or PONGs from 5 s | lag switch at once, watchdog reconnect |
| stall | one token silent on socket 0 | lag switch, socket stays up |
| delay | socket 0 frames 5 s late | lag switch |
| corrupt | one wrong size, true hash | hash miss, book replaced |
| Gamma 429 | first 2 requests 429 | discovery completes after retries |
| resolved | close "all subscribed assets resolved" | resolved_closes, no reconnect |
| supervise | panic at 5 s under `--supervise` | restart, reset marker, verify OK |

  `verify` passes every run with 0 mismatches. Three repeat runs gave 9/9 each time, in 42 s.

**Bug found by the mock: the lag check never fired on a ticking token.** It timed the
source from the peer's *newest* delta (`adv_at`), which resets on every delta. A stalled or
late source was switched only if the peer also went quiet for 3 s. So on any active token a
dead subscription was caught only by the hash, the quiet check or the watchdog. Now each
replica keeps `unseen`: its oldest delta the source has not seen, and when it arrived. The
lag runs from that. When the source catches up, `unseen` moves to the peer's newest delta,
so detection can be late but never early. `adopt` clears it. Two new unit tests cover it: a
stalled source switches while the peer ticks every 900 ms, and a source 100 ms behind a peer
on every delta never switches.

- Tests: 62 unit (1 ignored) + 3 golden + 9 end-to-end pass. Clippy clean.

**Open:** CI, clock offset, p99 profiling, README checklist, gate run, soak.

## Turn — Task set 3: CI, clock offset, README checklist, gate run

**Changed:**
- **CI** (`.github/workflows/ci.yml`): Ubuntu, clippy with `-D warnings`, unit + golden tests,
  then the end-to-end tests. The first run passed in 2m35s.
- **Clock offset (D21).** `src/clock.rs` measures the host clock against an NTP server
  (`--ntp-server`, default `time.google.com:123`, empty turns it off). It runs at start and every
  10 minutes; the best of 4 tries by round trip is logged. It warns above 25 ms. It never changes
  a timestamp.
- **README:** a requirements checklist, the e2e and CI paragraph, and the new flag.
- **`bench/gate.sh`:** one full-universe run under `--supervise` with optional SIGSTOP and kill -9
  of the child. It verifies and deletes each finished file, and samples RSS and CPU every minute.
- Two small fixes after the gate: an empty histogram prints `n=0` instead of fake percentiles,
  and `gate.sh` sends SIGTERM once.

**Gate run** (Codespace WestEurope, 4 vCPU, build 174cb77, 2026-10-24 08:42–09:08 UTC, 25 min,
5-minute rotation, SIGSTOP 15 s at t=501 s, kill -9 of the child at t=902 s):

| Check | Result |
|---|---|
| Coverage | 547.7k tokens, 547.1k sourced, 442 sockets, 2 cores |
| Written | 21.3M updates, 13.8M snapshots, 7 files |
| `verify` | 0 mismatches in all 7 files |
| kill -9 | supervisor restarted the child after 1 s; `.part` recovered with 0 bytes dropped; the new child wrote `ob.…0855Z.r1.sbe` |
| SIGSTOP 15 s | recovered; no stalls counted |
| Memory | RSS 2.8–3.1 GB, flat; 2.85 GB right after the restart |
| CPU | 130–195% of 400% |
| wire / queue p99 | ≤ 32.8 ms / ≤ 32.8 ms; max 2.0 s / 1.0 s (the freeze) |
| Source | 19.1k switches, 19.9k switch snapshots, 0 lag switches, 0 disconnects |
| Clock | offset +6.2, +10.7, +12.9, +17.7 ms (rtt ~5.5 ms) |

**What it shows:**
- Every recovery path ran live at once, with zero replay errors.
- **The Codespace clock drifts about 0.5 ms a minute** against time.google.com, and runs
  *behind* it. Since `wire` is host time minus venue time, it reads low by the offset: the
  printed p50 of ≤ 0 ms is that drift, not a real zero. Over a 2–3 h soak the offset should
  cross the 25 ms warning; that is the check working, not a fault.
- The source-port experiment was not run: it would share the CPU with the gate, and
  Experiment 1 already saw up to 9 ms between ports on one edge. It stays in LATENCY "Not done".

- Tests: 64 unit (1 ignored) + 3 golden + 9 end-to-end pass. Clippy clean.

**Open:** 2–3 h soak, Kalshi (needs a US VPN), zip and email.

## Turn — `core.rs` split into `core/` modules (no code change)

**Changed:** the single `src/core.rs` became `src/core/`, with one module per job:
- `mod.rs`: the `Core` struct, the loop, routing, and a module map in its docs;
- `config.rs`: the configuration;
- `plan.rs`: shard planning;
- `conns.rs`: connection lifecycle;
- `health.rs`: health and failover;
- `speed.rs`: the speed-aware source;
- `emit.rs`: output;
- `metrics.rs`: counters;
- `tests.rs`: the tests.

Items moved verbatim with a script, one commit per module, so each diff is a pure move.
Tests and clippy passed after every step.

## Turn — Kalshi adapter (Ext 3), mock-tested only

**Changed:**
- **`src/adapter/kalshi.rs`**, a second `ExchangeAdapter`:
  - signed handshake with RSA-PSS or Ed25519;
  - paged discovery, plus lookup by ticker;
  - `orderbook_delta` + `market_lifecycle_v2`;
  - relative deltas turned into absolute sizes in a per-socket session book;
  - a per-`sid` `seq` check, where a gap, a negative level, or error 25 drops the socket and
    fails it over;
  - `get_snapshot` for re-snapshot.
- **Trait:** `ws_request()`, `type Session`, `ping_frame()`, `resnapshot()`, and a typed `Resync`
  error. The core, the writer and `verify` are unchanged. The REST retry loop is now shared
  (`adapter::get_json`).
- **`main.rs`:** generic over the adapter. `--venue polymarket|kalshi` selects it, and
  `--gamma-url` became `--rest-url` (the old name still works). Kalshi caches its universe in
  `universe-kalshi.json`.
- **Secrets:** the key is read only from `KALSHI_API_KEY_ID` / `KALSHI_PRIVATE_KEY_PATH`.
  `.env` and `*.pem` are gitignored.
- **`docs/KALSHI.md`:** rewritten from a gap analysis to the design as built, with the unknowns
  that need a live run.

**Tests:**
- 10 Kalshi unit tests on documented frames. Both signature types are verified over the exact
  signed string.
- `tests/kalshi_e2e.rs`: the collector binary against a mock Kalshi, with three cases:
  - baseline;
  - a `seq` gap at 3 s, where the log shows `resync: sid 1: seq 265 after 263`, failover, a
    reconnect, and `verify` 16 / 16 snapshots matched;
  - a lifecycle listing that joins the existing `sid` through `update_subscription`.
- All suites pass: 74 unit, 3 golden, 9 + 3 end-to-end. Clippy is clean with `-D warnings`.

**Not done:** no live Kalshi run, because it needs an account and key. `KALSHI.md` "Not done"
lists what to check first.

## Turn — 2.5-hour soak

**Run:** Codespace WestEurope (4 vCPU, 16 GB), build 1a4d84c, under `--supervise`, with 10-minute
rotation and no injected faults. 2026-10-24 09:11:57–11:42:23 UTC (14:41–17:12 IST), 9011 s.

| Check | Result |
|---|---|
| Coverage | 551k tokens, 548k sourced, 452 sockets, 2 cores |
| Written | 320.3M updates, 183.5M snapshots, 16 files (about 5 GB per 10 min) |
| `verify` | **0 mismatches in all 16 files**; 162M snapshots replayed against the rebuilt book |
| Supervisor | 0 restarts; child exited 0 after 9010.9 s |
| Memory | RSS 2818 → 3297 MB. The rise follows the universe: 279k markets added, 3.6k removed, symbols 1.093M → 1.101M |
| CPU | 172–197% of 400% |
| wire / queue p99 | ≤ 16.4 ms / ≤ 65.5 ms (bucket bounds); max 6.0 s / 1.9 s |
| Venue churn | 535 disconnects (481 WebSocket resets, 32 server Away/Again closes); 16 quiet sockets reconnected; 348 recycles; 0 unplaced tokens |
| Source | 1.63M switches, 272 lag switches |
| Hash check | 319.5M ok, 0.90M miss, 1.04M healed, 12.1k divergent |
| Writer | 8 backpressure waits of 1.0–1.5 s, each logged twice (once per core) |
| Clock | offset +19.3 → +87.6 ms; warned from +28 ms on |

**What it shows:**
- 2.5 h of the live full universe, with venue disconnects every few seconds, and the output still
  replays exactly. No restart was needed.
- **Memory:** the RSS climb tracks the universe growing. It is not a leak: it rises in steps when
  listings arrive and is flat in between, for example 3031 MB from t=609 to t=1020 s.
- **Clock:** the drift is the Codespace's clock, at about 0.45 ms a minute. The clock check
  flagged it as designed. A production host runs chrony, and LATENCY explains how to correct
  `wire` by the logged offset.
- **Backpressure** waits fall on file rotations (for example 10:00:05 and 10:40:02) and
  disk-bound moments. The writer blocks the cores instead of dropping data, which is the intended
  trade-off (D11). A faster disk (NVMe on a real host) removes it.
- `queue` p99 rose from ≤ 32.8 ms in the 25-min gate to ≤ 65.5 ms here: one bucket up, from the
  same stalls.

## Turn — Final gate on the submission build

**Run:** same Codespace, build 0d400ec (the Kalshi adapter, generic `main`, `core/` split),
2026-10-24 11:47–12:12 UTC (17:17–17:42 IST), 25 min, 5-minute rotation, SIGSTOP 15 s at
t=543 s, kill -9 of the child at t=900 s.

| Check | Result |
|---|---|
| Coverage | 558.8k tokens, 548.8k sourced, 450 sockets, 2 cores |
| Written | 23.5M updates, 13.8M snapshots, 7 files |
| `verify` | **0 mismatches in all 7 files** |
| kill -9 | supervisor restarted the child after 1 s; `ob.…1200Z.sbe` recovered with 0 bytes dropped; the new child wrote `ob.…1200Z.r1.sbe` |
| SIGSTOP 15 s | recovered; the readers then found the core queues full 269k times and waited (`stalls`). Nothing was dropped, and no socket was lost |
| Writer | one 3.5 s disk wait in the second child, followed by 241 reader waits |
| Memory | RSS 2.96–3.12 GB after the restart |
| CPU | ~191% of 400% |
| Venue | 5 server Away closes, 0 unplaced tokens, 31 lag switches |
| Clock | offset +90 → +101 ms (Codespace drift, warned) |

**What it shows:** the refactor and the generic adapter changed nothing visible. Every recovery
path ran again on the final build with zero replay errors.

**About `stalls`:** the counter is reader waits on a full core queue, which is backpressure, not
loss. It is 0 in normal running and rises only after a freeze or a slow disk. It resets on a
restart, because it lives in the child.

## Turn — Grading and deployment docs

**Changed:**
- **`docs/GRADING.md`** (new):
  - a five-minute check: build, test, a 2-minute live run, `verify`;
  - a table from each README claim to the test or run that shows it, with the command to rerun it;
  - required flags against experiment flags;
  - what every counter in `status` / `done` means and what a healthy value looks like;
  - how to correct `wire` by the logged clock offset.
- **`docs/DEPLOY.md`** (new):
  - host requirements, measured in the soak;
  - the AWS, GCP and Hetzner machines that match the tested Codespace;
  - chrony, a systemd unit (`--supervise` inside, `KillSignal=SIGINT`), and shipping finished
    files to object storage after `verify`.
  - Only the Codespace has been run; the other rows are on paper.
- **README:**
  - Kalshi described as built but never run live;
  - a pointer to GRADING; `--venue` in the flag table; the Kalshi e2e tests;
  - the Layout shows `core/`, `supervise.rs` and `clock.rs`;
  - "Polymarket only" in Known limits became "Kalshi untested live".
- **ARCHITECTURE:** the `core/` module map, the current trait (`Session`, `ws_request`,
  `resnapshot`, `Resync`), and both adapters.
- **LATENCY / DECISIONS:** `core.rs` references now point at the `core/` files, and D1's cost
  reflects the adapter.

**Open:** the zip and the email to jobs@voluntas.io, both waiting on Abhinav's go-ahead.

## Turn — Ext 4, Experiment 3: the TCP source port

**Why:** the challenge names changing the source port as a latency lever. It was the last named
idea not tested on purpose; Experiment 1 only saw ports differ by accident.

**Built:**
- `--edge-ip` pins every socket to one edge.
- `--src-ports` binds replica r to a given local port: `TcpSocket` with `SO_REUSEADDR`, bind,
  then connect.
- Each fixed-port connect is logged with its port.
- A unit test checks two things against a local websocket:
  - the port is bound, and can be reused toward another peer while the first connection is in
    TIME_WAIT;
  - the default path stays ephemeral.
- `bench/port.sh` runs the rounds; `bench/port_an.py` scores them.

**Run:** WestEurope Codespace, 14:13–14:44 UTC, commit 9cc68f0. Setup:
- 12 sockets on one Cloudflare edge: 6 on fixed ports, 6 ephemeral;
- 6 fresh 4-minute rounds, 75 s apart;
- 400 markets.

All rounds verified OK, with 0 connections lost.

**Result:**
- Sockets on one edge spread 10–15 ms apart in mean `behind`, every round.
- A fixed port kept its rank only weakly:
  - rank correlation +0.27, ICC +0.12, permutation p ≈ 0.1;
  - ephemeral ports: −0.22, p 0.87.
- Not significant. The best fixed port did no better than the best ephemeral socket.
- **Verdict:** the port is not a useful lever on Polymarket; choosing among live connections is
  (Experiment 2).

**First launch failed:** the script ran from `bench/` and could not find the binary (exit 127 in
three rounds). Fixed to run from the repo root and relaunched; those rounds were discarded.

**Docs:** LATENCY Experiment 3, and the source-port row removed from "Not done"; a GRADING row and
flag list; README flag rows.

**Tests:** 90 passed (one new), clippy clean.

## Turn — Plan for the last performance round, and the submission

**Why:** the remaining gaps against a production market-data collector are memory (about 3 GB
RSS, about 5.4 KB per token), CPU on the hot path, thread oversubscription, disk stalls reaching
the books, and timestamps that are neither kernel-level nor clock-synced.

**Plan:** [PERF-PLAN.md](PERF-PLAN.md). In brief:
- Step 0: offline harnesses for bytes per token and ns per event, and baselines.
- Memory: mimalloc, flat books with inline storage, inline replicas, symbol interning, lazy
  hash state.
- CPU: FxHash on interned ids, LTO, SIMD JSON (kept only if it wins and the golden tests stay
  exact).
- Threading: a tokio worker budget, a writer queue bounded in bytes with a high-water counter,
  Linux pinning.
- Time accuracy (Linux): kernel receive timestamps, chrony on AWS.
- A region test, then a 60-minute full-universe AWS run as the final gate.
- Submission: a "Start here" block, `scripts/demo.sh`, a sample capture, a Dockerfile, CI.

**Code read for the plan:**
- `recv_ts` is taken in `conn.rs` when the websocket frame completes. That time includes TLS,
  websocket parsing and tokio scheduling.
- `#[tokio::main]` uses the default worker count, so on 4 vCPU there are 8 busy threads.
- Every map uses std SipHash over `Arc<str>` token ids.
- Each token holds 3 `BTreeMap` books.

Nothing has been changed in the code yet.

## Turn — The performance round: memory, CPU, threads, kernel timestamps

**Why:** carry out [PERF-PLAN.md](PERF-PLAN.md). Each step was measured offline with
`examples/bench.rs` before and after. `mem` is 283k markets (566k tokens) on 454 sockets, every
book on both replicas. `cpu` replays `frames_trades` through decode, then the core on 2 replicas.

**Results** (Mac M-series; `cargo run --release --example bench -- mem|cpu`):

| Step | Commit | Heap B/token | Empty books B/token | RSS | Core ns/event | Allocs/event | Decode ns/frame |
|---|---|---|---|---|---|---|---|
| Baseline | 658bc57 | 7,598 | 1,528 | 4.5 GB | 6,600 | 2.87 | 1,821 |
| M1 mimalloc + C2 fat LTO | 028f1d5 | | | | | | 1,289 |
| M2 flat books (one sorted `Vec` per book, bids then asks) | f5752a8 | 5,777 | 1,434 | | 6,279 | | |
| M4 replicas inline (`Vec<(ConnId, Replica)>`) | f5752a8 | 5,448 | 1,106 | | | | |
| M7/M5 boxed `Tok`, one token `Sym` (the writer appends `:bid`/`:ask`) | f5752a8 | 5,089 | 746 | 3.3 GB | 6,196 | 2.43 | |
| M6 hash checker: shared `Meta`, only changed ticks stored, lazy market state | fe299a6 | 4,574 | 652 | 3.0 GB | | | |
| C1 FxHash for the core's and checker's maps | 596044c | | | | 5,970 | | ~1,295 |

- The bench is pessimistic on depth: its books average 79 levels per token, against a
  per-side p50 of 25 in the captures. Levels alone are 3,808 B/token. What remains is mostly
  level data: 4,574 − 652 = 3,922 B/token of book contents. The live universe is shallower.
- Heap overhead outside the levels fell from 1,528 to 652 B/token (−57%). Total heap fell 40%,
  and core time per event fell 10%.

**Tried and not kept:**
- M5 full interning (`SymId(u32)`): dropping the two side symbols took most of the gain. A full
  interner would touch every adapter and the writer for about 100 B/token more.
- M3 `u32` prices: levels would go from 16 B to 12 B. Skipped, because with padding a
  `(u32, i64)` is still 16 B.
- C4 `sonic-rs`: not attempted. Decode is 1.3 µs/frame, below the core's per-frame cost, and
  it does not show in the profile.
- C5 events-buffer pool: not attempted. Allocations are 2.43 per event, and malloc does not
  show in the profile under mimalloc.
- C6 SHA-1 backends: `ring` was slower than the `sha1` crate. The crate already uses SHA-NI on
  x86 (AWS). On ARM, SHA-1 compression is the top of the profile (289 samples, against 100
  for `fixed::encode`).

**Threads and backpressure (bb52484):**
- **T1:** `main` builds the runtime itself. The tokio worker count is CPUs − core threads − 1
  (for the writer), at least 1, with `--io-threads` to override. The start-up log has a
  `thread budget` line.
- **T2:** the writer queue is bounded in bytes (`--writer-queue-mb`, default 256). A
  semaphore permit per batch is released once the batch is written, and a batch larger than
  the bound still goes through alone. Nothing is dropped: past the bound the cores wait. The
  status line and shutdown log `queue_hwm_mb` and `queue_waits`.
- **T3:** `--pin` (Linux). The main thread is restricted to the non-core CPUs before the
  runtime starts, so every thread it starts inherits that set. Each core thread then moves to
  its own CPU. It warns and does nothing on macOS, or when the CPUs are not more than the core
  threads.

**Kernel timestamps (e2a0647):**
- **H1:** `--kernel-ts` (Linux). The websocket's TCP stream is wrapped in `kts::Stamped`,
  which reads with `recvmsg` and `SO_TIMESTAMPNS`.
- A frame's `recv_ts` becomes the kernel time of the latest read when the frame completes.
  This excludes TLS, websocket parsing and tokio scheduling.
- `app − kernel` per frame goes into a power-of-two histogram, logged every 30 s as `host
  delay`.
- Tests: a unit test for the histogram, and on Linux a loopback read that must carry a
  timestamp within the test's window.

**Local check:** a 45 s smoke run reached discovery, but the websocket host timed out from
this network (a curl to it timed out too). The live gate runs on a Codespace instead.

**Live gate, run 1** (Codespace WestEurope, 4 vCPU, 16 GB, build e2a0647, full universe,
`--pin --kernel-ts`, 10 minutes, rotated every 5 minutes):

| Check | Result |
|---|---|
| clippy, tests on Linux | clean; 78 + 9 + 3 + 3 passed, incl. the loopback kernel-stamp test |
| Coverage | 562,702 tokens, all sourced, on 452 sockets |
| Replay (`verify`, 3 files, 5.4 GB) | 9.67M snapshots matched the rebuilt book, **0 did not** |
| Peak RSS | **1,729 MB**, against 2.8–3.3 GB in the earlier full runs (−43%) |
| Writer queue | peak 21.6 MB of 256 MiB, 0 waits; disk never reached the cores |
| Stalls | 0 |
| Host delay (app − kernel) | p50 ≤ 128 µs, p90 ≤ 512 µs, p99 ≤ 2–4 ms, p99.9 ≤ 8 ms |
| recv − exch | p50 23–26 ms, p99 57–321 ms |
| "Slow consumer" closes | 8, all on one hot shard (`conn=394`/`395`), 16:19–16:21; earlier soaks had 18–27 |

- Live memory is about 3.0 KB per token. About 650 B of it is overhead; the rest is book
  levels, held three times (two replicas plus the published book) at 16 B each. Getting below
  1 GB would mean smaller levels (a `u16` tick index plus a separate quantity array) or
  publishing from the leading replica. Neither is done.
- **Bug found:** the log said `thread budget cores=2 pinned=true`, then `core threads started
  cores=1`. `run()` counted the CPUs again after `pin::set` had narrowed the main thread's
  set, so it saw 2 CPUs and chose 1 core. Fixed in d2a77de: the count is made once in `main`
  and passed down. Run 1 therefore carried all 562k books on one core thread, with 0 stalls.

**Live gate, run 2** (build d2a77de, same flags, 10 minutes):

| Check | Result |
|---|---|
| Core threads | `core threads started cores=2`, `per_core=[281218, 280216]`: the fix works |
| Replay (3 new files, 5.9 GB) | 10.77M snapshots matched, **0 did not** |
| Peak RSS | 1,760 MB |
| Writer queue | peak 20.8 MB, 0 waits; stalls 0; disconnects 0; **0 "slow consumer" closes** |
| Host delay (app − kernel) | p50 ≤ 256–1,024 µs, p90 ≤ 2–8 ms, p99 ≤ 8–16 ms: about 4× worse than run 1 |
| Sourced | 558,985 of 561,254 tokens at the end (2.2k short; run 1 had all); lag switches 260 against 6 |

- `lscpu` on the Codespace: 4 vCPUs are 2 physical cores (EPYC 7763) with 2 hyperthreads
  each; CPUs 2 and 3 are siblings. `--pin` gives the core threads the last two CPUs, so both
  core threads share **one physical core**. In run 1 the single core thread had that physical
  core to itself. A slower core thread fills its queue, the readers wait on it, and that wait
  is inside `app − kernel`. This fits the worse host delay and the extra lag switches.
- That was the first guess. The comparison below shows the cause was the single tokio worker,
  not the shared physical core.

**Thread layout, 5 minutes each** (same Codespace, build d2a77de, full universe, `--kernel-ts`):

| Layout | Host delay p50 / p90 / p99 / p99.9 | Lag switches | Slow-consumer closes |
|---|---|---|---|
| `--pin`, io 1 (run 2) | ≤ 256–1,024 µs / 2–8 ms / 8–16 ms / 16–32 ms | 260 | 0 |
| `--pin --io-threads 2` | ≤ 128 µs / 256 µs–1 ms / 4–8 ms / 8–16 ms | 138 | 0 |
| no pin, io 1 | ≤ 256–1,024 µs / 2–4 ms / 8 ms / 16 ms | 0 | 0 |

- One tokio worker cannot keep up with about 450 TLS sockets: frames wait in the kernel
  buffer for milliseconds. Two workers bring the delay back to run 1's level, even though that
  is 5 busy threads on 4 vCPUs. The default is now at least 2 (`CPUs − cores − 1`, minimum 2).

**The unsourced tokens are not a bug.** Every 2-core run ends a few thousand tokens short of
`sourced = tokens` (2.2k–7k here; 0.6k–10k in the earlier soaks). A debug log (behind
`RUST_LOG=debug`) lists the shards that hold them:
- about 50 in *every* shard, not one shard gone dark. Both sockets are up and hold valid books
  for the other ~2,450 tokens, and the missing tokens' book requests stay outstanding.
- The CLOB REST endpoint answers `{"error":"No orderbook exists for the requested token id"}`
  for each sampled one. Gamma lists them, but the venue has no book to send.
- So coverage is complete for every token that has a book. Run 1 showing all sourced is not
  explained yet; it was one core thread on a slightly earlier universe.

## Turn — The AWS run

The one-shot run from `docs/AWS-RUN.md`, on build 7170677. The first attempt failed at launch
because the account was on the Free plan, which allows only free-tier instance types
(`--dry-run` does not apply that limit). Preflight now reads the plan. After the upgrade to
Paid, every phase passed: `SETUP SMOKE PROBE TESTS BENCH MAIN SAMPLE ALL_DONE`. Teardown left
0 instances, security groups, key pairs or volumes in all three regions. Results:
`bench/aws-2026-10-24/` (the 64 MB sample capture is kept out of git).

**Hosts:** main c7i.xlarge in eu-west-2a (Xeon Platinum 8488C, 4 vCPU = 2 cores × 2 threads,
kernel 7.0.0-1014-aws, no PTP device exposed); probes c7i-flex.large in eu-west-1 and
us-east-1. Clock: chrony on Amazon Time Sync, RMS offset 0.4–9 µs over the hour; the
collector's own NTP check read 0.0 ms offset, 0.1 ms RTT.

**Region probe** (10 min, 2,000 markets, `--lat-detail --kernel-ts`, same start second):

| Region | Updates | Replay | recv − exch p50 / p90 / p99 / p99.9 | Host delay p50 / p99 |
|---|---|---|---|---|
| eu-west-2 London | 675k | 81,288 matched, 0 not | 7 / 21 / 118 / 381 ms | ≤ 128 / ≤ 512 µs |
| eu-west-1 Ireland | 680k | 82,052 matched, 0 not | 12 / 25 / 73 / 232 ms | ≤ 32 / ≤ 256 µs |
| us-east-1 Virginia | 679k | 82,008 matched, 0 not | 43 / 62 / 349 / 725 ms | ≤ 64 / ≤ 256 µs |

- London is the right region by median; Ireland has the tighter tail. Virginia adds about
  35 ms, which is the transatlantic path to the venue's origin.

**Offline, on the main host:** `cargo test --release` 78 + 9 + 3 + 3 passed. `bench cpu`:
decode 1,410 ns/frame (460 MB/s), core 4,665 ns/event, 2.43 allocs/event. `bench mem 283000`:
4,574 B/token heap, RSS 2,990 MB with every book filled to its captured depth.

**Full universe, 60 min supervised** (`--supervise --pin --kernel-ts --rotate-mins 10`):

| Check | Result |
|---|---|
| Coverage | 548k–554k tokens sourced of 549k–556k listed, on 442–452 sockets; 280,559 markets seen, 467 removed during the hour |
| Throughput | 41–43k updates/s steady; 9 MB/s to disk (5.4 GB per 10-min file) |
| Replay (`verify`, 8 files, about 33 GB) | about 55M snapshots matched the rebuilt book, **0 did not** |
| Host delay (app − kernel), 119 windows | p50 ≤ 32 µs in every window; p90 ≤ 64 µs, p99 ≤ 128–256 µs, p99.9 ≤ 512 µs–1 ms in most; worst window p99 ≤ 2 ms |
| recv − exch | p50 8–9 ms, p90 20–34 ms, p99 50–348 ms |
| Wire / queue (collector's own) | wire p50 ≤ 16 ms, p99 ≤ 66 ms; core queue p50 ≤ 0 ms, p99 ≤ 8 ms |
| CPU (steady mean, % of one CPU) | core threads 47, tokio 54, writer 6; load 1.2 on 4 vCPU |
| RSS | 1,815 MB mean, 1,919 MB peak, flat across the hour |
| Writer queue | peak 20 MB, 0 waits; stalls 0; writer backpressure 0 |
| `kill -9` at 55 min | supervisor restarted in 1 s, recovered the open file with `dropped_bytes=0`; the restarted child ran 298 s and exited 0 |
| Server "slow consumer" closes | 29 in the hour, each reconnected; 6 quiet-socket reconnects |

- On a real 4-vCPU host, the host delay is about 4× better than on the Codespace (p50 ≤ 32 µs
  against ≤ 128 µs) and the p99 tail is about 16× better.
- The `recv − exch` max of 600–1,100 s is the venue stamping the initial snapshots with the
  time of the last book change, not real delay. The percentiles are unaffected.
- Kit bug: the sampler's `rx_mbps` read 0 all hour. It parsed `/proc/net/dev` by column, and
  the columns shift with the interface name's padding, so it summed packets, not bytes. It now
  reads `/sys/class/net/<default-route interface>/statistics/rx_bytes` (checked in an Ubuntu
  24.04 container). The inbound rate for this run is not recoverable.
- The summary's host-delay section is complete (119 windows); an earlier read of it filtered
  those lines out by mistake.
- Cost: about 1.5 h of c7i.xlarge and 0.4 h per probe, roughly $0.50 of the credits.

## Turn — Packaging

- **Bug: SIGTERM lost the last chunk.** Only `--supervise` handled SIGTERM; a collector run
  directly (as the Docker image runs it, or under systemd, `kill` or Kubernetes) took the default
  action and died with up to one chunk unwritten. `main.rs` now treats SIGTERM like SIGINT. New
  e2e test `sigterm_flushes_and_exits_cleanly`: SIGTERM after 10 s, exit 0, `verify` clean.
- **`sample/`:** the two-minute, 500-market capture from the AWS run, gzipped to 588 KB (the
  64 MiB chunk is mostly padding), with its log. It replays with 3,950 snapshots matched, 0 not,
  and needs no network.
- **`demo.sh`:** build, verify the sample, all tests, a 2-minute live run and its verify. The
  live step fails if the file has no snapshots: from this laptop the ISP answers the venue's DNS
  with a block page (49.44.79.236), and an empty file replays cleanly, so the check would
  otherwise pass on no data.
- **`Dockerfile`:** two stages, `rust:1-bookworm` to `debian:bookworm-slim`. TLS roots are
  compiled in, so the runtime image needs no CA bundle.
- **CI:** now also runs `kalshi_e2e`, replays the sample, and builds the image and runs `verify`
  on the sample inside it.
- **Docs:** new PERFORMANCE.md with the AWS numbers; README, GRADING, DEPLOY and LATENCY now
  quote the AWS run instead of the Codespace where both exist.

## Turn — End-to-end check and README charts

**Ask:** with the laptop on a VPN, check everything works end to end for the solution and for
grading, check the submission against the challenge brief, and chart in the README how each fix
moved its metric.

**Checked** (laptop, VPN to Singapore):
- `./demo.sh all` passed. The sample replayed 3,950 snapshots with 0 mismatches. Tests passed:
  77 lib, 10 e2e, 3 golden, 3 Kalshi. A live run of 2,000 markets for 120 s wrote 108,401
  updates and 23,344 snapshots; 15,344 replayed, 0 mismatches.
- stdout output, live: 50 markets for 20 s, redirected to a file; `verify` OK, 58 matched.
- Against the brief:
  - Every core item and Ext 1–4 maps to code and a test or run.
  - Shutdown sends a websocket Close on every socket before the file is closed.
  - Kalshi discovery and lookup pass `mve_filter=exclude`.
  - The README checklist gained rows for the stdout default and MVE exclusion.

**Added:**
- `docs/charts/data.json`: the per-run values, copied from this log and PERFORMANCE.md.
- `make_charts.py`: standard library only. It draws one SVG line chart per metric. x is the
  run, labelled with the fixes that landed before it.
- The palette is the validated two-slot categorical pair, light and dark.
- Eight charts: coverage, core queue p99, updates/s, heap per token, live RSS, core thread CPU,
  host delay p99 and writer waits.
- The README section "How each fix moved the numbers" holds the charts and the same data as a
  fix → before → after table.

**Left out of the charts:**
- Soak 2's 2.1 s queue p99, which came from a deliberate 15 s freeze, not a fix.
- The AWS core CPU of 47%, which is not per thread.

**Open:** the zip and the email, on the user's go.


## Turn — Crash loss, failover window, AWS tuning

**Ask:** apart from the three limits left as they are (Kalshi live, core CPU, ISP blocks), fix
the rest, then use AWS to run the tuning that the Codespace could not.

**Fixed:**
- **`kill -9` lost the open chunk.** The writer now sizes each file chunk to 64 MiB when it opens
  and writes whole messages into it in place whenever its queue empties or 1 MiB is pending
  (D11). The bytes on disk are what the template's writer would produce. A kill loses only what
  was queued. New tests: a unit test replays a chunk that is synced but unfinished, and an e2e
  test kills the process with SIGKILL and verifies the file. The e2e test fails with the old writer.
- **The 3 s failover window had no flag.** `--lag-ms` sets it, and the lag check now runs on its
  own timer (a quarter of the window, 100 ms to 1 s) rather than only when traffic arrives. Test:
  a 500 ms window switches at 600 ms and not at 250 ms.
- **The speed-aware source scored venue-wide stalls.** `check_speed` now skips a shard's window
  when even its best socket averaged ≥ `--fast-stall-ms` (4 ms) behind, and counts it as
  `fast_stalls`. Test: alternating 12 ms stalls leave the scores and the source unchanged.
- **New metric:** `src_wire`, recv − exch of the written deltas, so A/B arms compare on what
  readers get.

**AWS run** (`bench/aws/run.sh tune`): one c7i.xlarge in London, 82 minutes of rounds.
- r1 full universe, default against `--fast-source`: mean 9.1 → 8.8 ms, p99 52 → 50 ms, for
  4.8× the switch snapshots. Kept opt-in.
- r2 `--replicas 4` against 2: no gain, 20× the token moves. Kept at 2.
- r3 `--lag-ms 1000` against 3000, 2,000 markets: 0 lag switches in 15 minutes, so no false
  switching. Default kept at 3 s until a full-universe run.
- r4 kernel busy polling: the second "off" arm matched "on". Null.
- r5 `kill -9` at 120 s: 119.8 s kept, 3,862 snapshots replayed, 0 mismatches.
- Every arm's `verify` was OK.

**Teardown:** the AWS login session expired during the run, so the driver could not terminate
the host. The host was launched to halt itself and terminate (volume deleted on termination)
240 minutes after launch.

**Open:** a 1 s failover window on the full universe; the zip and the email, on the user's go.
