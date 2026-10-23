//! Polymarket CLOB market channel + Gamma discovery + `hash` verification.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    time::{Duration, Instant},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tokio::sync::mpsc;

use super::{
    BookView, ConnId, ExchangeAdapter, Instrument, Integrity, LevelChange, NormEvent, Sym, Verdict,
};
use crate::{
    book::{Book, Side},
    fixed::{self, Fx, SCALE},
};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA: &str = "https://gamma-api.polymarket.com";

/// Per-market metadata the `hash` covers and the stream does not always repeat.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Meta {
    pub neg_risk: bool,
    pub tick: String,
    pub min_size: String,
}

pub struct Polymarket {
    http: reqwest::Client,
}

impl Polymarket {
    pub fn new() -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("voluntas-collector/0.1")
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { http })
    }
}

// ---------------------------------------------------------------- wire types

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<'a> {
    #[serde(borrow)]
    Many(Vec<Raw<'a>>),
    #[serde(borrow)]
    One(Raw<'a>),
}

#[derive(Deserialize)]
struct Raw<'a> {
    event_type: Option<&'a str>,
    market: Option<&'a str>,
    asset_id: Option<&'a str>,
    timestamp: Option<&'a str>,
    hash: Option<&'a str>,
    #[serde(borrow)]
    bids: Option<Vec<Lvl<'a>>>,
    #[serde(borrow)]
    asks: Option<Vec<Lvl<'a>>>,
    #[serde(borrow)]
    price_changes: Option<Vec<Pc<'a>>>,
    price: Option<&'a str>,
    tick_size: Option<&'a str>,
    new_tick_size: Option<&'a str>,
    last_trade_price: Option<&'a str>,
}

#[derive(Deserialize)]
struct Lvl<'a> {
    price: &'a str,
    size: &'a str,
}

#[derive(Deserialize)]
struct Pc<'a> {
    asset_id: &'a str,
    price: &'a str,
    size: &'a str,
    side: &'a str,
    hash: Option<&'a str>,
}

fn px(s: &str) -> anyhow::Result<Fx> {
    fixed::parse(s).with_context(|| format!("bad decimal {s:?}"))
}

fn ts_us(s: Option<&str>) -> anyhow::Result<i64> {
    let ms: i64 = s.context("missing timestamp")?.parse()?;
    Ok(ms * 1000)
}

fn levels(l: &[Lvl]) -> anyhow::Result<Vec<(Fx, Fx)>> {
    l.iter().map(|x| Ok((px(x.price)?, px(x.size)?))).collect()
}

fn sym(s: &str) -> Sym {
    s.into()
}

fn opt(s: Option<&str>) -> Option<Box<str>> {
    s.filter(|s| !s.is_empty()).map(Into::into)
}

// ---------------------------------------------------------------- gamma

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    condition_id: Option<String>,
    clob_token_ids: Option<String>,
    #[serde(default)]
    enable_order_book: bool,
    #[serde(default)]
    accepting_orders: bool,
    #[serde(default)]
    neg_risk: bool,
    order_min_size: Option<f64>,
    order_price_min_tick_size: Option<f64>,
}

#[derive(Deserialize)]
struct KeysetPage {
    #[serde(default)]
    markets: Vec<GammaMarket>,
    next_cursor: Option<String>,
}

fn num(x: Option<f64>, default: Fx) -> String {
    fixed::to_string(x.map_or(default, |v| (v * SCALE as f64).round() as Fx))
}

fn to_instrument(m: GammaMarket) -> Option<Instrument<Meta>> {
    if !(m.enable_order_book && m.accepting_orders) {
        return None;
    }
    let tokens: Vec<String> = serde_json::from_str(m.clob_token_ids.as_deref()?).ok()?;
    if tokens.is_empty() {
        return None;
    }
    Some(Instrument {
        market: m.condition_id?.into(),
        tokens: tokens.iter().map(|t| sym(t)).collect(),
        meta: Meta {
            neg_risk: m.neg_risk,
            tick: num(m.order_price_min_tick_size, 10_000),
            min_size: num(m.order_min_size, 5 * SCALE),
        },
    })
}

impl ExchangeAdapter for Polymarket {
    type Meta = Meta;

    const NAME: &'static str = "polymarket";

    fn ws_url(&self) -> &str {
        WS_URL
    }

    fn ping_interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    fn ping_frame(&self) -> &'static str {
        "PING"
    }

    fn subscribe_initial(&self, tokens: &[Sym]) -> String {
        serde_json::json!({"assets_ids": tokens, "type": "market", "initial_dump": true, "custom_feature_enabled": true})
            .to_string()
    }

    fn subscribe(&self, tokens: &[Sym]) -> String {
        serde_json::json!({"assets_ids": tokens, "operation": "subscribe", "initial_dump": true, "custom_feature_enabled": true})
            .to_string()
    }

    fn unsubscribe(&self, tokens: &[Sym]) -> String {
        serde_json::json!({"assets_ids": tokens, "operation": "unsubscribe"}).to_string()
    }

    fn decode(&self, frame: &str, out: &mut Vec<NormEvent>) -> anyhow::Result<()> {
        if frame == "PONG" || frame.is_empty() {
            return Ok(());
        }
        let raws = match serde_json::from_str::<OneOrMany>(frame)? {
            OneOrMany::Many(v) => v,
            OneOrMany::One(r) => vec![r],
        };
        for r in raws {
            let et = r
                .event_type
                .unwrap_or(if r.bids.is_some() { "book" } else { "" });
            let ev = match et {
                "book" => NormEvent::Book {
                    token: sym(r.asset_id.context("book without asset_id")?),
                    market: sym(r.market.context("book without market")?),
                    bids: levels(r.bids.as_deref().unwrap_or_default())?,
                    asks: levels(r.asks.as_deref().unwrap_or_default())?,
                    exch_ts: ts_us(r.timestamp)?,
                    check: opt(r.hash),
                    tick: opt(r.tick_size),
                    last_trade: opt(r.last_trade_price),
                },
                "price_change" => NormEvent::Levels {
                    market: sym(r.market.context("price_change without market")?),
                    exch_ts: ts_us(r.timestamp)?,
                    changes: r
                        .price_changes
                        .unwrap_or_default()
                        .iter()
                        .map(|c| {
                            Ok(LevelChange {
                                token: sym(c.asset_id),
                                side: if c.side == "BUY" {
                                    Side::Bid
                                } else {
                                    Side::Ask
                                },
                                px: px(c.price)?,
                                qty: px(c.size)?,
                                check: opt(c.hash),
                            })
                        })
                        .collect::<anyhow::Result<_>>()?,
                },
                "last_trade_price" => NormEvent::Trade {
                    market: sym(r.market.context("trade without market")?),
                    token: sym(r.asset_id.context("trade without asset_id")?),
                    px: px(r.price.context("trade without price")?)?,
                    exch_ts: ts_us(r.timestamp)?,
                },
                "tick_size_change" => NormEvent::TickSize {
                    token: sym(r.asset_id.context("tick change without asset_id")?),
                    tick: r.new_tick_size.context("tick change without size")?.into(),
                },
                "new_market" => NormEvent::Listed {
                    market: sym(r.market.context("new_market without market")?),
                },
                "market_resolved" => NormEvent::Resolved {
                    market: sym(r.market.context("market_resolved without market")?),
                },
                _ => continue, // best_bid_ask: derivable from the book
            };
            out.push(ev);
        }
        Ok(())
    }

    async fn discover(&self, pages: mpsc::Sender<Vec<Instrument<Meta>>>) -> anyhow::Result<()> {
        let mut cursor: Option<String> = None;
        loop {
            let mut req = self.http.get(format!("{GAMMA}/markets/keyset")).query(&[
                ("active", "true"),
                ("closed", "false"),
                ("limit", "100"),
            ]);
            if let Some(c) = &cursor {
                req = req.query(&[("after_cursor", c)]);
            }
            let page: KeysetPage = req.send().await?.error_for_status()?.json().await?;
            let batch: Vec<_> = page.markets.into_iter().filter_map(to_instrument).collect();
            if !batch.is_empty() && pages.send(batch).await.is_err() {
                return Ok(());
            }
            match page.next_cursor.filter(|c| !c.is_empty()) {
                Some(c) => cursor = Some(c),
                None => return Ok(()),
            }
        }
    }

    async fn lookup(&self, market: &str) -> anyhow::Result<Vec<Instrument<Meta>>> {
        let ms: Vec<GammaMarket> = self
            .http
            .get(format!("{GAMMA}/markets"))
            .query(&[("condition_ids", market)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(ms.into_iter().filter_map(to_instrument).collect())
    }

    fn integrity(&self) -> Box<dyn Integrity<Meta>> {
        Box::new(HashCheck::default())
    }
}

// ---------------------------------------------------------------- hash check

/// Reproduces Polymarket's per-token `hash`: SHA1 over compact JSON of the book with a fixed
/// key order. Rules established on live captures (see ARCHITECTURE.md §2):
/// 1. entries are batched by (market, timestamp); a hash covers the state after the batch,
///    or sometimes the intermediate state after its own event;
/// 2. `last_trade_price` is market-level and always 3dp; after a trade it is `p` or `1-p`,
///    and on a sweep it is the *last* fill level, which only the touched prices reveal;
/// 3. a trade takes effect before its `last_trade_price` frame arrives, so a miss is
///    deferred until the market's next trade (or a timeout).
#[derive(Default)]
pub struct HashCheck {
    meta: HashMap<Sym, (Sym, Meta)>, // token -> (market, meta)
    tick: HashMap<Sym, String>,
    mkts: HashMap<(ConnId, Sym), MarketState>,
    batch: HashMap<ConnId, Batch>,
}

#[derive(Default)]
struct MarketState {
    ltp: String,
    cand: BTreeSet<String>,
    recent: VecDeque<String>,
    deferred: Vec<Deferred>,
}

struct Batch {
    market: Sym,
    ts: i64,
    pending: Vec<(Sym, Box<str>)>,
}

struct Deferred {
    token: Sym,
    hash: Box<str>,
    ts: i64,
    book: Book,
    since: Instant,
}

const DEFER: Duration = Duration::from_secs(5);
const RECENT: usize = 40;

impl HashCheck {
    fn digest(
        &self,
        token: &str,
        market: &str,
        ts_us: i64,
        book: &Book,
        ltp: &str,
    ) -> Option<String> {
        let (_, meta) = self.meta.get(token)?;
        let tick = self.tick.get(token).unwrap_or(&meta.tick);
        let mut s = String::with_capacity(64 * (book.bids.len() + book.asks.len()) + 256);
        s.push_str("{\"market\":\"");
        s.push_str(market);
        s.push_str("\",\"asset_id\":\"");
        s.push_str(token);
        s.push_str("\",\"timestamp\":\"");
        s.push_str(&(ts_us / 1000).to_string());
        s.push_str("\",\"hash\":\"\",\"bids\":[");
        let lv = |s: &mut String, it: &mut dyn Iterator<Item = (&Fx, &Fx)>| {
            for (i, (p, q)) in it.enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push_str("{\"price\":\"");
                fixed::format(*p, s);
                s.push_str("\",\"size\":\"");
                fixed::format(*q, s);
                s.push_str("\"}");
            }
        };
        lv(&mut s, &mut book.bids.iter());
        s.push_str("],\"asks\":[");
        lv(&mut s, &mut book.asks.iter().rev());
        s.push_str("],\"min_order_size\":\"");
        s.push_str(&meta.min_size);
        s.push_str("\",\"tick_size\":\"");
        s.push_str(tick);
        s.push_str("\",\"neg_risk\":");
        s.push_str(if meta.neg_risk { "true" } else { "false" });
        s.push_str(",\"last_trade_price\":\"");
        s.push_str(ltp);
        s.push_str("\"}");
        Some(hex::encode(Sha1::digest(s.as_bytes())))
    }

    /// Tries the market's candidate last-trade prices; on a hit, calibrates `ltp` to it.
    #[allow(clippy::too_many_arguments)]
    fn try_match(
        &mut self,
        conn: ConnId,
        market: &Sym,
        token: &str,
        ts: i64,
        book: &Book,
        hash: &str,
        sweep: bool,
    ) -> bool {
        let st = self.mkts.entry((conn, market.clone())).or_default();
        let mut opts: Vec<String> = Vec::with_capacity(4 + st.cand.len());
        opts.push(st.ltp.clone());
        opts.extend(st.cand.iter().cloned());
        if sweep {
            opts.extend(st.recent.iter().cloned());
        }
        for o in opts {
            if self.digest(token, market, ts, book, &o).as_deref() == Some(hash) {
                let st = self
                    .mkts
                    .get_mut(&(conn, market.clone()))
                    .expect("inserted above");
                if o != st.ltp {
                    st.ltp = o;
                    st.cand.clear();
                }
                return true;
            }
        }
        false
    }

    /// Retries deferred checks of `market` (only `token`'s, if given); whatever still fails is a miss.
    fn settle(&mut self, conn: ConnId, market: &Sym, token: Option<&Sym>, out: &mut Vec<Verdict>) {
        let Some(st) = self.mkts.get_mut(&(conn, market.clone())) else {
            return;
        };
        let (due, keep) = std::mem::take(&mut st.deferred)
            .into_iter()
            .partition(|d| token.is_none_or(|t| d.token == *t));
        st.deferred = keep;
        for d in due {
            let ok = self.try_match(conn, market, &d.token, d.ts, &d.book, &d.hash, true);
            out.push(if ok {
                Verdict::Ok {
                    conn,
                    token: d.token,
                    exch_ts: d.ts,
                }
            } else {
                Verdict::Miss {
                    conn,
                    token: d.token,
                    exch_ts: d.ts,
                }
            });
        }
    }

    fn close_batch(
        &mut self,
        conn: ConnId,
        books: &dyn BookView,
        now: Instant,
        out: &mut Vec<Verdict>,
    ) {
        let Some(b) = self.batch.remove(&conn) else {
            return;
        };
        for (token, hash) in b.pending {
            let Some(book) = books.book(&token) else {
                continue;
            };
            if self.try_match(conn, &b.market, &token, b.ts, book, &hash, true) {
                out.push(Verdict::Ok {
                    conn,
                    token,
                    exch_ts: b.ts,
                });
            } else {
                let book = book.clone();
                self.mkts
                    .entry((conn, b.market.clone()))
                    .or_default()
                    .deferred
                    .push(Deferred {
                        token,
                        hash,
                        ts: b.ts,
                        book,
                        since: now,
                    });
            }
        }
    }
}

fn q3(p: Fx) -> String {
    fixed::format_3dp(p)
}

impl Integrity<Meta> for HashCheck {
    fn register(&mut self, inst: &Instrument<Meta>) {
        for t in &inst.tokens {
            self.meta
                .insert(t.clone(), (inst.market.clone(), inst.meta.clone()));
        }
    }

    /// A batch's final state is the book before the first event outside it is applied.
    fn pre_event(
        &mut self,
        conn: ConnId,
        ev: &NormEvent,
        books: &dyn BookView,
        now: Instant,
        out: &mut Vec<Verdict>,
    ) {
        let same_batch = match (ev, self.batch.get(&conn)) {
            (
                NormEvent::Levels {
                    market, exch_ts, ..
                },
                Some(b),
            ) => b.market == *market && b.ts == *exch_ts,
            _ => false,
        };
        if !same_batch {
            self.close_batch(conn, books, now, out);
        }
    }

    fn on_event(
        &mut self,
        conn: ConnId,
        ev: &NormEvent,
        books: &dyn BookView,
        now: Instant,
        out: &mut Vec<Verdict>,
    ) {
        let _ = now;
        match ev {
            NormEvent::Book {
                token,
                market,
                exch_ts,
                check,
                tick,
                last_trade,
                ..
            } => {
                if let Some(t) = tick {
                    self.tick.insert(token.clone(), t.to_string());
                }
                // The snapshot's last_trade_price may be the trade a deferred check waits for.
                let st = self.mkts.entry((conn, market.clone())).or_default();
                if let Some(l) = last_trade {
                    st.cand.insert(l.to_string());
                }
                self.settle(conn, market, Some(token), out);
                let st = self.mkts.entry((conn, market.clone())).or_default();
                if let Some(l) = last_trade {
                    st.ltp = l.to_string();
                }
                let (Some(h), Some(book)) = (check, books.book(token)) else {
                    return;
                };
                let ltp = st.ltp.clone();
                if self.digest(token, market, *exch_ts, book, &ltp).as_deref() == Some(&**h) {
                    out.push(Verdict::Ok {
                        conn,
                        token: token.clone(),
                        exch_ts: *exch_ts,
                    });
                } else if self.meta.contains_key(token) {
                    out.push(Verdict::BookMiss {
                        conn,
                        token: token.clone(),
                    });
                }
            }
            NormEvent::Levels {
                market,
                exch_ts,
                changes,
            } => {
                let st = self.mkts.entry((conn, market.clone())).or_default();
                for c in changes {
                    st.recent.push_back(q3(c.px));
                    st.recent.push_back(q3(SCALE - c.px));
                }
                while st.recent.len() > RECENT {
                    st.recent.pop_front();
                }
                for c in changes {
                    let (Some(h), Some(book)) = (&c.check, books.book(&c.token)) else {
                        continue;
                    };
                    if self.try_match(conn, market, &c.token, *exch_ts, book, h, false) {
                        out.push(Verdict::Ok {
                            conn,
                            token: c.token.clone(),
                            exch_ts: *exch_ts,
                        });
                    } else {
                        let b = self.batch.entry(conn).or_insert_with(|| Batch {
                            market: market.clone(),
                            ts: *exch_ts,
                            pending: Vec::new(),
                        });
                        b.pending.push((c.token.clone(), h.clone()));
                    }
                }
            }
            NormEvent::Trade { market, px, .. } => {
                let st = self.mkts.entry((conn, market.clone())).or_default();
                st.cand.insert(q3(*px));
                st.cand.insert(q3(SCALE - *px));
                self.settle(conn, market, None, out);
            }
            NormEvent::TickSize { token, tick } => {
                self.tick.insert(token.clone(), tick.to_string());
            }
            NormEvent::Listed { .. } | NormEvent::Resolved { .. } => {}
        }
    }

    fn on_tick(&mut self, now: Instant, out: &mut Vec<Verdict>) {
        for ((conn, _), st) in self.mkts.iter_mut() {
            st.deferred.retain(|d| {
                let expired = now.duration_since(d.since) >= DEFER;
                if expired {
                    out.push(Verdict::Miss {
                        conn: *conn,
                        token: d.token.clone(),
                        exch_ts: d.ts,
                    });
                }
                !expired
            });
        }
    }

    fn forget_conn(&mut self, conn: ConnId) {
        self.batch.remove(&conn);
        self.mkts.retain(|(c, _), _| *c != conn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_price_change() {
        let f = r#"{"market":"0xab","price_changes":[{"asset_id":"1","price":"0.9","size":"3123.9","side":"BUY","hash":"610c","best_bid":"0.95","best_ask":"0.96"}],"timestamp":"1792742997166","event_type":"price_change"}"#;
        let mut out = Vec::new();
        Polymarket::new().unwrap().decode(f, &mut out).unwrap();
        let NormEvent::Levels {
            exch_ts, changes, ..
        } = &out[0]
        else {
            panic!()
        };
        assert_eq!(*exch_ts, 1_792_742_997_166_000);
        assert_eq!(
            (changes[0].side, changes[0].px, changes[0].qty),
            (Side::Bid, 900_000, 3_123_900_000)
        );
    }
}
