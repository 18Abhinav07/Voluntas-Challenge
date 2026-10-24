# Kalshi adapter (Ext 3)

There is no Kalshi adapter. Kalshi could not be reached from where this was built. This note
maps Kalshi's websocket feed onto `ExchangeAdapter` and lists what the trait would have to change.
The goal of Ext 3 is that only encode/decode changes per venue. This note checks that claim.

Sources: Kalshi's public API docs (docs.kalshi.com), read 2026-10-24. Items marked **(to
confirm)** were not stated on the pages read, or come from memory, and must be checked against
the live API before relying on them. Nothing here was run against Kalshi.

## The feed

- **Endpoint:** `wss://external-api-ws.kalshi.com/`.
- **Auth:** required, even for public market data. The handshake carries three headers:
  - `KALSHI-ACCESS-KEY`: the API key id.
  - `KALSHI-ACCESS-TIMESTAMP`: the time in ms.
  - `KALSHI-ACCESS-SIGNATURE`: a signature over timestamp + method + path, without the query
    string.
  - The signature is Ed25519, or RSA-PSS with SHA-256 (MGF1, salt length equal to the digest
    length).
  - Which path the websocket handshake signs **(to confirm)**.
- **Subscribe:**
  ```json
  {"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["..."]}}
  ```
- **Change a subscription:** `update_subscription` with `add_markets`, `delete_markets`, or
  `get_snapshot`, which resends a snapshot and keeps the subscription.
- **Unsubscribe:** by subscription id (`sids`).
- **Book messages:**
  - `orderbook_snapshot`: `yes_dollars_fp` and `no_dollars_fp`, each a list of `[price, count]`
    strings. Both are **bids only**. A Kalshi market is one book of YES bids and NO bids.
  - `orderbook_delta`: `price_dollars`, `delta_fp` (a **relative** change, e.g. `"-54.00"`), and
    `side` (`yes` or `no`).
  - Every message on a subscription carries `seq`, which increases by one. A gap means a lost
    message.
- **Clock:** messages carry the exchange clock. It is the same on every connection, and deltas
  arrive in the same order everywhere. The clock can go backwards during maintenance.
- **Errors that matter:**
  - 25: the server's outbound buffer for us overflowed. This is Kalshi's "slow consumer".
  - 26: too many markets on one subscription.
  - 27: command rate limit.
- **Lifecycle:** `market_lifecycle_v2` for opens, closes and settlements;
  `multivariate_market_lifecycle` for MVEs.
- **Discovery:** REST `/markets` with cursor paging. MVEs, which the challenge excludes, would be
  filtered there; the `mve_filter=exclude` parameter is **(to confirm)**.

## Mapping onto the output

- One Kalshi market becomes two symbols, as on Polymarket:
  - `<ticker>:bid` = the YES bids;
  - `<ticker>:ask` = the NO bids at price `1 − p`.
- **Why this is exact:** a NO bid at `p` is, by Kalshi's own definition, an offer to sell YES at
  `1 − p`.
- Prices are dollars as strings and parse into the same `i64` fixed point. Counts are contracts.

Compare Polymarket: it lists each outcome as its own token with bids and asks, so the mirroring
is implicit there. Kalshi makes it the definition.

## Where the trait does not fit

| # | Gap | Today | What Kalshi needs | Change |
|---|---|---|---|---|
| 1 | **Auth on connect** | `ws_url()` returns a URL, and `conn.rs` opens it as-is | signed headers on the handshake, made fresh for each connect | Add `fn ws_request(&self) -> Request` (default: plain GET of `ws_url`). Kalshi signs inside it. The key stays in a gitignored `.env`. |
| 2 | **Relative deltas** | `LevelChange.qty` is the absolute size at a price | `delta_fp` is a change | Either the adapter keeps a per-socket book to turn deltas into absolute sizes, or `LevelChange` gets a `Rel` variant and `Book` applies it. The second is cleaner, because the core already holds a book per replica. |
| 3 | **Sequence integrity** | `Integrity` checks a venue hash | `seq` per subscription | A `SeqCheck: Integrity` that reports a `Miss` on any gap. `NormEvent` needs to carry `(sid, seq)`. A gap is exact evidence, so it should refresh at once, without the strike ladder used for Polymarket's ~1% false misses. |
| 4 | **Per-socket subscription state** | `subscribe` and `unsubscribe` take tokens and return a frame | `unsubscribe` takes the `sid` from the subscribe ack, which is only known on that socket | A per-connection adapter session (`type Session`), created on connect and fed every decoded ack, that `subscribe` and `unsubscribe` can read. |
| 5 | **Discovery** | `discover` streams pages of instruments | REST cursor paging plus `market_lifecycle_v2` | Fits as-is. Lifecycle events decode to `Listed` and `Resolved`. |
| 6 | **Slow consumer** | Polymarket closes the socket | error code 25 | Map it in `decode` to the same reconnect path. It fits the existing `Exit` handling. |

Gaps 1–4 are real trait changes. With them, the core, the socket actor, the writer and `verify`
stay unchanged. Without them, a Kalshi adapter cannot be written against today's trait. The
claim that "only encode/decode changes" holds for the core, but not yet for the trait.

## What Kalshi would make better

- **Exact cross-replica checking.** Two sockets see the same exchange clock and the same order, so
  replicas can be compared message by message. Polymarket only allows comparing books.
- **Exact gap detection.** `seq` replaces the hash heuristic. The integrity ladder becomes "gap →
  switch source, resnapshot via `get_snapshot`".
- **Resnapshot without resubscribing.** `get_snapshot` keeps the subscription, so a refresh is one
  command instead of an unsubscribe and a resubscribe.

## What to watch

- **Maintenance:** the clock can go backwards. The lag check compares event times across
  replicas, so it must reset on a clock step instead of reading it as a peer being ahead.
- **Limits:** error 26 caps markets per subscription, and error 27 caps commands. The shard size
  and the open limiter would need Kalshi's values.
