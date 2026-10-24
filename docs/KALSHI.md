# Kalshi adapter (Ext 3)

`--venue kalshi` runs the same engine (core, socket actor, writer, `verify`) on a Kalshi adapter,
[`src/adapter/kalshi.rs`](../src/adapter/kalshi.rs). **It has never run against Kalshi.** It was
written from Kalshi's public API docs (docs.kalshi.com, read 2026-10-24) and is tested only:

- on Kalshi's example frames, in the adapter's unit tests;
- end to end, against a mock of the documented API ([`tests/kalshi_e2e.rs`](../tests/kalshi_e2e.rs)):
  the collector binary, two replicas, and `verify` on the output.

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
set -a; . ./.env; set +a; cargo run --release --bin collector -- --venue kalshi --out kalshi.sbe
```

Without a key it stops at once with `--venue kalshi needs an API key`. The universe cache defaults
to `universe-kalshi.json`, so it never reads Polymarket's.

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

## Not done, and what to watch

- **Live run:** none. The first live run should check:
  - that the signed path is accepted;
  - that `seq` is per `sid` **(to confirm)**: the docs give a minimum of 1 and no scope;
  - the real lifecycle `event_type` values.
- **Market limit per subscription (error 26):** the number is not documented. `--tokens-per-conn`
  defaults to 2500, which is Polymarket's value. If Kalshi rejects that, lower it; a shard is one
  subscription.
- **Command rate limit (error 27):** surfaces as a decode error (logged, socket kept). The open
  limiter (`--opens-per-sec`) is Polymarket's value.
- **Clock steps:** Kalshi's clock can go backwards during maintenance. The lag check compares
  event times across replicas, so it would read a step as one peer being ahead. It is not handled.
- **Cross-replica message comparison:** both sockets see the same exchange clock and order, so
  replicas could be compared message by message. This was not built; failover uses the same
  health signals as Polymarket.
- **Pending unsubscribe:** a ticker unsubscribed before its `subscribe` is acknowledged stays
  subscribed on the venue until the socket closes. The core ignores its books.
