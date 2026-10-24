# Decisions

Each entry gives the choice, what was rejected, and the evidence. The numbers come from runs in
[ENGINEERING-LOG.md](ENGINEERING-LOG.md).

## D1. Polymarket, not Kalshi

- **Choice:** Polymarket.
- **Why:** Kalshi is US-only and its API could not be reached from where this was built (India).
  The challenge allows Polymarket in that case.
- **Cost:** Ext 3 has one implementation. [KALSHI.md](KALSHI.md) maps Kalshi onto the trait and
  lists what the trait would need.

## D2. Two symbols per token: `<token>:bid` and `<token>:ask`

- **Problem:** the schema's `PriceLevelUpdate` has no side field, and each Polymarket token has
  bids and asks.
- **Rejected:**
  - **Infer the side from the price**, using `px ≤ best_bid` → bid. A new level inside the spread
    is ambiguous.
  - **Bids only**, relying on the YES asks being the NO bids at 1−p. The mirroring is visible in
    the feed, but exact equivalence was not verified, and the output would depend on it.
- **Choice:** two symbols per token. It is lossless and needs no inference. Each snapshot fills
  only its own side, and `verify` checks that.

## D3. Raw token ids as symbols

- **Choice:** the token id (a 77-digit integer) plus the side suffix.
- **Rejected:** `slug:outcome`. Slugs are not guaranteed stable or unique, and the token id is
  what the venue's book is keyed by.

## D4. `--out` takes a path; `-` is stdout

- **Problem:** the challenge says "write to stdout" and also "close the file you are writing to".
- **Choice:** a path by default, `-` for stdout. Logs always go to stderr.
- Rotation (`--rotate-mins`) is file-only.

## D5. A snapshot's `exchange_timestamp` is the last applied event's

- **Rejected:** the snapshot's own write time, since that is already `received_at`.
- **Rejected:** the venue `book` message's timestamp. That is the book's last-change time, and in
  an initial dump it can be tens of seconds old (26.7 s seen).

## D6. Collect every live market

- **Choice:** collect everything Gamma lists as live, about 283k markets.
- **Why:** many are quiet, but a quiet market is still part of "all live markets", and a filter by
  activity would drop a market at the moment it wakes up.
- **Cost:** about 566k tokens on about 450 sockets. A large initial dump per socket.

## D7. Replicas with one source per token, not a merged feed

- **Choice:** each shard on 2 independent sockets, pinned to different edge IPs. Each replica
  keeps its own book. One replica per token is the source.
- **Rejected:** first-arrival merge across sockets. Polymarket has no sequence numbers or message
  ids. Deduplicating by `(timestamp, price, size)` is a heuristic, and it would collapse two real,
  identical updates in the same millisecond.
- **Why it works:** a failover target already has a live, checked book. Switching costs no
  round-trip to the venue.

## D8. A source switch writes a diff, not a fresh snapshot

The challenge asks for a snapshot on a source switch. This is a deliberate deviation.

- **What happens:** the core keeps the book readers hold (the emitted book). On a switch it writes
  the level changes from that book to the new source's book as ordinary updates.
- **Why:**
  - **The output stays checkable.** A mid-minute snapshot that replaces the book hides whatever
    was lost before it. A reader cannot tell "the source switched" from "data went missing". With
    the diff, each snapshot must equal the previous one plus every update since, and `verify`
    checks this exactly on every file.
  - **Evidence:** the first build did write a snapshot on each switch. That produced 15.5k replay
    mismatches, every one an unexplained jump in the stream. With the diff: 0 mismatches in every
    file since, over tens of millions of replayed snapshots.
  - The reader ends up in the same state either way. The diff is usually a handful of levels; a
    snapshot is the whole book.
- **To switch back:** a one-line change in `Core::adopt` (`core.rs`): call `snapshot` instead of
  `emit_diff`.

## D9. The venue hash never switches the source on its own

- **Background:** Polymarket's `price_change` carries a SHA1 of the book after the change. It is
  the only integrity signal the venue offers, because there are no sequence numbers.
- **Problem:** recomputing it is not exact. The golden captures pass 100%, 100% and 98.9% on
  books that are correct. The likely cause on the trade-heavy one: after a sweep, the hashed
  `last_trade_price` is the last fill level, which the feed does not report, so it has to be
  guessed from recent prices.
- **Choice:** a strike ladder. A miss refreshes the replica only when one of these holds:
  - a peer replica at the same event position disagrees with our book;
  - it is the second miss in a row.
  A refresh moves the source off the replica first, then asks the venue for a new book.
- **Rejected:** "miss → resnapshot". At about 1% false misses on 566k tokens, that would be a
  constant resnapshot storm.
- **Only the source is hashed.** A standby is compared book-to-book when the source misses. This
  cut core CPU by about 38%. The cost: a standby that drifts is only caught once it becomes source.

## D10. Silent-subscription failover waits 3 s

- **Detection:** a peer replica holds a newer delta than the source, and has held it for 3 s.
  Only a peer's delta counts; a fresh `book` dump's timestamp is not evidence. Counting dumps
  caused a switch storm in the first build.
- **Why 3 s:** a margin well above the normal delivery gap between two sockets, so ordinary
  jitter does not switch the source. Measured later (LATENCY.md, Experiment 1, 8 sockets,
  400 markets): the slowest socket's p99 behind the first copy was 75 ms, so 3 s is about 40×
  that. A window of a few hundred ms looks safe at that load, but it has not been tried, and the
  full universe has longer in-process stalls.
- **Cost:** up to 3 s of stale book on a silent subscription.
- **Not done:** the window is a `Config` field (`lag`) but has no CLI flag.

## D11. 64 MiB chunks

- **Why:** the template's `AlignedSbeReader::open` always reads with `DEFAULT_CHUNK_SIZE`
  (64 MiB). The helpers are not to be edited. A file written with another chunk size cannot be
  read by the graders' reader.
- **Consequence:** `flush` pads to a full chunk. The writer flushes only when a chunk fills and at
  shutdown, never on a timer, which would write 64 MiB of mostly padding per tick.

## D12. One owner of book state per core thread

- **Choice:** each core is a single-threaded state machine that owns every book of its markets.
  Markets are routed to cores by hash.
- **Rejected:** `Arc<Mutex<Book>>` per token. Locks on the hot path, and no single order of events
  per market.
- **Why it works:** snapshot and update order is correct by construction. Time and side effects
  are injected, so 39 unit tests drive the core directly.
- **Evidence:** one core saturated at about 560k tokens (99.9% CPU). Two cores run at 31–38%
  each.

## D13. Writer on its own thread; fsync on another

- **Writer:** a bounded channel into a dedicated OS thread. Disk latency never stalls the async
  runtime. A full channel slows the cores rather than dropping data.
- **Finisher:** finishing a rotated file (last chunk, fsync, rename) moved to a second thread. Done
  inline, the fsync of a multi-GB file stopped the pipeline for 1–6 s at every rotation: 330 s and
  1,153 s of total reader blocked time in two rotation windows. After the move: 0 in all 11
  rotations of the A/B.

## D14. Hot-shard splitting is off by default

- **Idea:** move the hottest markets of an overloaded shard onto their own sockets.
- **Result in the A/B:** worse.
  - Disconnects rose 11× at 5-minute files and 19× at 15-minute files.
  - The hot markets on Polymarket are the short-lived crypto up/down markets (5–15 minutes). A
    split moves exactly those, then they resolve, and new hot markets land on the open tail shard.
    The same shard was split 6 times in 15 minutes.
  - It also exposed a real bug, the resolved-assets reconnect loop, which is now fixed.
- **Choice:** `--split-ratio 0` (off). The code stays, and you can opt in.
- **Rework needed:** place new markets by expected rate (for example, by their series) instead of
  by measured rate.

## D15. Make-before-break recycling every 60 minutes

- **Why:** a long-lived socket can degrade without failing: a slow edge, a stalled subscription
  the lag check has not yet seen.
- **How:**
  - The new socket subscribes first.
  - The old one keeps serving until every book has arrived on the new one, or 60 s pass.
  - At most two shards at a time, and one replica per shard.
- **Evidence:** the 90-minute soak recycled all 448 sockets with 0 replay mismatches.

## D16. Start from a cached universe

- **Choice:** subscribe the cached universe on start, then reconcile with Gamma.
- **Why:** full Gamma discovery takes over 3 minutes.
- The cache is also saved at shutdown, because a run shorter than one discovery would otherwise
  never write it.

## D17. Exact fixed-point parsing

- **Choice:** decimal strings are parsed straight to `i64` (1e6 = 1). Never through `f64`.
- **Why:** Polymarket prices have at most 3 dp and sizes at most 2, so every value is exact.
- Anything with more than 6 dp is rejected loudly rather than rounded.

## D18. Per-socket latency is opt-in (`--lat-detail`)

- **Choice:** per-socket `wire` and `behind` histograms are kept only with the flag.
- **Why:** `behind` needs a window of recent events per market to match copies across sockets.
  At about 283k markets that is about 110 MB (16 entries of 24 bytes each), plus 16 KB of
  histograms per socket, all for a measurement. The always-on `wire` and `queue` histograms stay.
- **Rejected:** first-arrival dedup with a single "last event" per market. A socket 10 ms behind
  on a busy market finds the newer event already in place, and its copy would go untimed. That
  biases the measurement towards the fast sockets. The window is 16 events, and copies that miss
  it are counted as `late` (at most 98 of 154k on the slowest socket).

## D19. A speed-aware source is opt-in (`--fast-source`)

- **Choice:** with the flag, each shard prefers the socket with the lowest recent `behind`, and
  recycles a socket that stays slowest. Off by default.
- **How it is kept safe:**
  - **Hysteresis.** The preferred socket changes only when it is more than 1 ms worse than the
    best, on a score smoothed over 30 s windows with at least 50 timed events each.
  - **A move writes nothing.** Tokens move onto the preferred socket only when its book equals
    the source's. A standby is not hashed (D9), so a faster socket that disagrees is not trusted.
  - **Failover is unchanged** except for ties: among replicas that have seen as much, the
    preferred one wins.
  - **The recycle is make-before-break** (D15), and respects the same limits: two shards at a
    time, one replica per shard. It needs 10 windows in a row (5 minutes) as slowest and at least
    4 ms behind the best.
- **Why opt-in:** in the one A/B run (LATENCY, Experiment 2), p50 and p90 of the written events
  improved by 1 ms and the mean by 10%, but p99 got worse (88 → 145 ms) as the preferred socket
  changed 7 times in 20 minutes. It also costs the per-market windows of D18.
- **Rejected:** first-arrival merge across sockets (D7), which would remove the choice entirely
  but needs a dedup key the venue does not give.
