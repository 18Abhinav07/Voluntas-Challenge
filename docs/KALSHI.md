# Kalshi adapter

Kalshi is the venue the challenge names. `--venue kalshi` runs the same engine (core, socket
actor, writer, `verify`) on a Kalshi adapter, [`src/adapter/kalshi.rs`](../src/adapter/kalshi.rs).
It was written from Kalshi's public API docs (docs.kalshi.com, read 2026-10-24) and is tested:

- on Kalshi's example frames, in the adapter's unit tests;
- end to end, against a mock of the documented API ([`tests/kalshi_e2e.rs`](../tests/kalshi_e2e.rs)):
  the collector binary, two replicas, and `verify` on the output;
- **live, on Kalshi's demo environment** (2026-10-25): 1,000 markets for 15 minutes, 1.73M
  updates, 0 replay mismatches; then the whole demo universe (about 100k markets) for 2 hours
  from AWS in Virginia, 32.3M updates, 0 mismatches. See [Live run on the demo environment](#live-run-on-the-demo-environment).
  The demo environment serves the same API as production; production is the default endpoint.

Items marked **(to confirm)** are not stated in the docs. Check them against the live API before
relying on them.

## Running it

Kalshi needs an API key even for public market data. Create one in your Kalshi account, then put
it in a gitignored `.env` (`.env` and `*.pem` are both in `.gitignore`):

```bash
KALSHI_API_KEY_ID=...
KALSHI_PRIVATE_KEY_PATH=/path/to/kalshi-key.pem
```

```bash
set -a; . ./.env; set +a; cargo run --release --bin collector -- --venue kalshi > kalshi.sbe
```

Without a key it stops at once with `--venue kalshi needs an API key`.

Kalshi's demo environment (demo.kalshi.co) issues keys the same way and needs no real funds. Point
the collector at it with:

```bash
--ws-url wss://demo-api.kalshi.co/trade-api/ws/v2 --rest-url https://demo-api.kalshi.co/trade-api/v2
```

The universe cache defaults to `universe-kalshi.json`, so it never reads Polymarket's.

## The feed, as implemented

- **Endpoint:** `wss://external-api-ws.kalshi.com/trade-api/ws/v2`. REST:
  `https://external-api.kalshi.com/trade-api/v2`.
- **Auth:** `ws_request()` adds three headers to each handshake, made fresh for each connect:
  - `KALSHI-ACCESS-KEY`;
  - `KALSHI-ACCESS-TIMESTAMP` (ms);
  - `KALSHI-ACCESS-SIGNATURE`: base64 of a signature over `timestamp + "GET" + "/trade-api/ws/v2"`.
- **Keys:**
  - RSA, which Kalshi issues as PKCS#1 PEM, is signed with RSA-PSS (SHA-256, MGF1, salt = digest
    length). Ed25519 keys (PKCS#8) are accepted too.
  - Tests sign with both key types and verify the signature over the exact string above.
  - Advisory RUSTSEC-2023-0071 (Marvin) in the `rsa` crate is a timing attack on *decryption*, and
    this client only signs.
- **Subscribe:**
  - one `subscribe` to `orderbook_delta` with the shard's tickers;
  - one `subscribe` to `market_lifecycle_v2`.
- **Session state:** the ack (`{"id":1,"type":"subscribed","msg":{"channel":…,"sid":…}}`) maps the
  command id to the subscription id (`sid`). After the ack:
  - a token added later joins that `sid` with `update_subscription add_markets`;
  - `delete_markets` removes a token;
  - `get_snapshot` re-snapshots a token, keeping the subscription.
- **Books:**
  - `orderbook_snapshot` → `NormEvent::Book`.
  - `orderbook_delta` carries a relative `delta_fp`. The session adds it to its own copy of the
    book and emits the absolute size as `NormEvent::Levels`.
- **Integrity:**
  - The socket is dropped (`Resync`), so the core fails its tokens over to the other replica and
    reconnects, when:
    - a `seq` gap appears on any `sid`;
    - a delta would take a level below zero;
    - a delta arrives before its market's snapshot;
    - error 25 (slow consumer) arrives.
  - Kalshi sends no book checksum, so `integrity()` is a no-op. The `seq` check is the integrity
    check, and it is exact, where Polymarket's hash check is heuristic.
- **Lifecycle:**
  - `created` / `activated` → `Listed`. The core looks the market up with
    `/markets?tickers=…` and subscribes it.
  - `determined` / `settled` → `Resolved`.
  - Every socket gets the whole lifecycle channel. The core already dedups listings seen on
    several sockets.
- **Discovery:** `/markets?status=open&limit=1000&mve_filter=exclude`, paged by `cursor`. Markets
  that are closed or settled are dropped.
- **Keepalive:** websocket Ping frames every 10 s (no text `PING`).

## Output mapping

- One Kalshi market becomes two symbols, as on Polymarket:
  - `<ticker>:bid` = the YES bids;
  - `<ticker>:ask` = the NO bids at price `1 − p`.
- **Why this is exact:** a NO bid at `p` is, by Kalshi's own definition, an offer to sell YES at
  `1 − p`.
- Prices are dollar strings parsed into the same `i64` fixed point. Counts are contracts.

## What the trait had to change

The earlier version of this note found four places where the trait did not fit. Each was resolved
as below. The core, the writer and `verify` did not change.

| # | Gap | Change made |
|---|---|---|
| 1 | Signed handshake | `fn ws_request(&self) -> Result<Request>`. The default is a plain GET of `ws_url()`, and Kalshi signs inside it. `conn.rs` connects with the request rather than a URL. |
| 2 | Relative deltas | A per-socket `type Session`, owned by the socket actor and passed to `subscribe`, `unsubscribe`, `resnapshot` and `decode`. Kalshi keeps its books there, so `LevelChange` still carries absolute sizes and the core is unchanged. Polymarket's session is `()`. |
| 3 | Sequence integrity | The same session holds the last `seq` per `sid`. `decode` returns a typed `Resync` error, which `conn.rs` turns into a reconnect, the same path as a lost socket. No change to `NormEvent` or `Integrity`. |
| 4 | `sid`-scoped commands | The same session maps tickers to `sid`. |
| 5 | Keepalive | `fn ping_frame() -> Option<&str>`: Polymarket's text `PING`, or `None` for a websocket Ping. |
| 6 | Re-snapshot | `fn resnapshot()`. The default is unsubscribe then subscribe; Kalshi sends one `get_snapshot`. |

The REST retry loop (backoff on 429, 5xx and transport errors, honouring `Retry-After`) moved from
the Polymarket adapter to `adapter::get_json`, so both venues share it.

So the Ext 3 claim now holds for the trait as well as the core: a venue is one file implementing
`ExchangeAdapter`, and `main.rs` picks it with `--venue`.

## Live run on the demo environment

Three runs on 2026-10-25 from a laptop in India, with a read-only Ed25519 demo key. Logs and
`verify` output: [`bench/kalshi-demo-2026-10-25/`](../bench/kalshi-demo-2026-10-25/).

| Run | Markets | Sockets | Length | Updates | Snapshots replayed | Mismatches | Disconnects |
|---|---|---|---|---|---|---|---|
| Smoke | 200 | 12 | 60 s | 80,062 | 332 | 0 | 0 |
| Main | 1,000 | 12 | 15 min | 1,726,633 | 29,806 | 0 | 0 |
| One subscription | 2,500 | 2 | 150 s | 276,552 | 12,006 | 0 | 0 |

Then a two-hour run on the whole demo universe, from a c7i-flex.large in us-east-1 (Virginia),
next to the exchange. Files: [`bench/aws-2h-2026-10-25/kalshi-us-east-1/`](../bench/aws-2h-2026-10-25/kalshi-us-east-1/).

| | Two hours, Virginia |
|---|---|
| Universe | 200k–205k tokens (about 100k markets), 100% sourced, 162–166 sockets |
| Updates | 32.3M in 14 files (3.4–4.9k/s), about 6.0 GB |
| Snapshots replayed | 48.9M matched, **0 did not**; 0 crossed books |
| recv − exch | p50 33–36 ms, p90 37–44 ms, p99 43–49 ms (from India: p50 214–222 ms) |
| Host delay | p50 ≤ 32–64 µs, p99 ≤ 1–2 ms on 2 vCPU |
| Resources | core 3.5%, tokio 6.7% of a CPU; RSS 629 MB peak; 27 Mbit/s in |
| `kill -9` at t = 6,900 s | restarted in 1 s; the new child ran 297 s and exited 0 |
| Drops | 162 connection resets and 24 `429` on reconnect, each retried with backoff; 23 quiet-socket reconnects |

The demo key was copied to that host for the run and shredded before it was terminated.

What this settles:

- **The signed handshake is accepted.** Ed25519 over `timestamp + "GET" + "/trade-api/ws/v2"`,
  on every socket. Without the headers the same endpoint answers 401.
- **Subscribe, acks and relative deltas work as implemented.** The books the collector built
  from snapshots plus deltas matched every later snapshot `verify` replayed.
- **`seq` per `sid` holds** as far as these runs can show: 1.73M deltas on 12 sockets, each
  with two subscriptions, raised no `seq` gap and so no resync.
- **2,500 markets on one subscription is accepted.** No error 26, so `--tokens-per-conn 2500`
  stands for Kalshi too. The real limit is still unknown; it is at least 2,500.
- **The output format carries Kalshi unchanged.** `verify` reads the files with no
  venue-specific code.

Reading these numbers:

- **Scale.** Demo and production keys are separate, and production books are deeper and busier.
  The demo rate was about 1,900 updates/s for 1,000 markets.
- **Latency.** recv − exch p50 was 214–222 ms from India and 33–36 ms from Virginia. The
  difference is the path; host delay stayed in microseconds on both.
- **Removals.** In the laptop runs `--max-markets` capped the universe, so new listings were
  outside it; the Virginia run had no cap. Five
  markets left the main run (`markets_removed`) and their books were dropped cleanly.
- **One corrected book.** In the main run one mid-stream snapshot on a source socket differed
  from the book built from deltas (`book_diffs: 1`, out of about 31,800 snapshots). The core
  wrote the difference as updates, so the file stayed correct and `verify` passed. This is the
  self-healing path working as designed: no `seq` gap, no reconnect, no lost data.

## Operating notes

- **Lifecycle `event_type` values:** `created`/`activated` add a market and
  `determined`/`settled` remove it, as documented; other values are ignored.
- **Command rate limit (error 27):** surfaces as a decode error (logged, socket kept). The open
  limiter (`--opens-per-sec`) is Polymarket's value.
- **Failover signals:** Kalshi uses the same health signals as Polymarket (the lag check across
  replicas, quiet-socket and quiet-token checks), plus its own `seq` gap check per subscription.
- **Pending unsubscribe:** a ticker unsubscribed before its `subscribe` is acknowledged stays
  subscribed on the venue until the socket closes. The core ignores its books.
