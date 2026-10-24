//! Kalshi `orderbook_delta` channel + REST `/markets` discovery (Ext 3). Written from Kalshi's
//! API docs and tested on their example frames only: it has never run against Kalshi
//! (docs/KALSHI.md).
//!
//! A Kalshi market is one book of YES bids and NO bids. It is written as two symbols, like a
//! Polymarket token: `<ticker>:bid` holds the YES bids, and `<ticker>:ask` holds the NO bids at
//! `1 - p`, since a NO bid at `p` is by definition an offer to sell YES at `1 - p`.

use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, bail};
use base64::Engine;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};

use super::{
    BookView, ConnId, ExchangeAdapter, Instrument, Integrity, LevelChange, NormEvent, Resync, Sym, Verdict,
};
use crate::{
    book::{Book, Side},
    fixed::{self, Fx, SCALE},
};

pub const WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const REST: &str = "https://external-api.kalshi.com/trade-api/v2";
/// The page size `/markets` allows at most.
const PAGE: &str = "1000";

/// The API key, from the environment (a gitignored `.env`): `KALSHI_API_KEY_ID`, and
/// `KALSHI_PRIVATE_KEY_PATH` naming the PEM file Kalshi issued with it.
pub struct Auth {
    key_id: String,
    key: Key,
}

enum Key {
    /// What Kalshi issues by default: PKCS#1 PEM, signed with RSA-PSS (SHA-256, MGF1 SHA-256,
    /// salt as long as the digest).
    Rsa(rsa::pss::SigningKey<Sha256>),
    Ed25519(ed25519_dalek::SigningKey),
}

impl Auth {
    pub fn from_env() -> anyhow::Result<Self> {
        let id = std::env::var("KALSHI_API_KEY_ID").context("KALSHI_API_KEY_ID is not set")?;
        let path = std::env::var("KALSHI_PRIVATE_KEY_PATH").context("KALSHI_PRIVATE_KEY_PATH is not set")?;
        let pem = std::fs::read_to_string(&path).with_context(|| format!("reading {path}"))?;
        Self::from_pem(&id, &pem)
    }

    /// Takes an RSA key (PKCS#1 or PKCS#8) or an Ed25519 key (PKCS#8).
    pub fn from_pem(key_id: &str, pem: &str) -> anyhow::Result<Self> {
        use rsa::{pkcs1::DecodeRsaPrivateKey, pkcs8::DecodePrivateKey};
        let key = if pem.contains("BEGIN RSA PRIVATE KEY") {
            Key::Rsa(rsa::pss::SigningKey::new(rsa::RsaPrivateKey::from_pkcs1_pem(pem)?))
        } else if let Ok(k) = <ed25519_dalek::SigningKey as ed25519_dalek::pkcs8::DecodePrivateKey>::from_pkcs8_pem(pem)
        {
            Key::Ed25519(k)
        } else {
            Key::Rsa(rsa::pss::SigningKey::new(rsa::RsaPrivateKey::from_pkcs8_pem(pem).context("not an RSA or Ed25519 key")?))
        };
        Ok(Self { key_id: key_id.into(), key })
    }

    /// The three handshake headers: the signature covers `timestamp + method + path`, with no
    /// query string.
    fn headers(&self, ts_ms: i64, path: &str) -> [(&'static str, String); 3] {
        let msg = format!("{ts_ms}GET{path}");
        let sig = match &self.key {
            Key::Rsa(k) => k.sign_with_rng(&mut rand::thread_rng(), msg.as_bytes()).to_vec(),
            Key::Ed25519(k) => ed25519_dalek::Signer::sign(k, msg.as_bytes()).to_bytes().to_vec(),
        };
        [
            ("kalshi-access-key", self.key_id.clone()),
            ("kalshi-access-signature", base64::engine::general_purpose::STANDARD.encode(sig)),
            ("kalshi-access-timestamp", ts_ms.to_string()),
        ]
    }
}

/// Per-market metadata from `/markets`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Meta {
    pub event: String,
    pub price_level_structure: String,
}

pub struct Kalshi {
    http: reqwest::Client,
    ws_url: String,
    rest_url: String,
    /// `None` connects unsigned: only the mock venue in the tests accepts that.
    auth: Option<Auth>,
}

impl Kalshi {
    pub fn new(auth: Auth) -> anyhow::Result<Self> {
        Self::with_endpoints(WS_URL, REST, Some(auth))
    }

    pub fn with_endpoints(ws_url: &str, rest_url: &str, auth: Option<Auth>) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("voluntas-collector/0.1")
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { http, ws_url: ws_url.into(), rest_url: rest_url.trim_end_matches('/').into(), auth })
    }
}

/// One socket's view of its subscriptions and books.
#[derive(Default)]
pub struct Session {
    next_id: u64,
    /// `subscribe` commands not acknowledged yet: command id -> its tickers.
    pending: HashMap<u64, Vec<Sym>>,
    /// The orderbook subscription (`sid`) carrying each acknowledged ticker.
    sid_of: HashMap<Sym, u64>,
    /// The first orderbook subscription acknowledged: later tickers join it.
    main_sid: Option<u64>,
    /// The last `seq` seen on each subscription.
    seq: HashMap<u64, u64>,
    /// This socket's books, already in output form (YES bids, NO bids as asks at `1 - p`).
    /// Kalshi sends relative deltas; the core takes absolute sizes, so they are summed here.
    books: HashMap<Sym, Book>,
}

impl Session {
    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// Groups acknowledged tickers by subscription; tickers still pending are left out.
    fn by_sid(&self, tokens: &[Sym]) -> Vec<(u64, Vec<Sym>)> {
        let mut m: HashMap<u64, Vec<Sym>> = HashMap::new();
        for t in tokens {
            if let Some(sid) = self.sid_of.get(t) {
                m.entry(*sid).or_default().push(t.clone());
            }
        }
        let mut v: Vec<_> = m.into_iter().collect();
        v.sort_unstable_by_key(|(sid, _)| *sid);
        v
    }

    fn update(&mut self, tokens: &[Sym], action: &str) -> Vec<String> {
        self.by_sid(tokens)
            .into_iter()
            .map(|(sid, ts)| {
                let id = self.id();
                serde_json::json!({"id": id, "cmd": "update_subscription",
                    "params": {"sids": [sid], "market_tickers": ts, "action": action}})
                .to_string()
            })
            .collect()
    }
}

// ---------------------------------------------------------------- wire types

#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    msg: Option<Msg<'a>>,
}

#[derive(Default, Deserialize)]
struct Msg<'a> {
    channel: Option<&'a str>,
    sid: Option<u64>,
    market_ticker: Option<&'a str>,
    #[serde(borrow, default)]
    yes_dollars_fp: Vec<[&'a str; 2]>,
    #[serde(borrow, default)]
    no_dollars_fp: Vec<[&'a str; 2]>,
    price_dollars: Option<&'a str>,
    delta_fp: Option<&'a str>,
    side: Option<&'a str>,
    ts_ms: Option<i64>,
    event_type: Option<&'a str>,
    code: Option<i64>,
    /// An error's text.
    #[serde(borrow)]
    msg: Option<Cow<'a, str>>,
}

fn px(s: &str) -> anyhow::Result<Fx> {
    fixed::parse(s).with_context(|| format!("bad decimal {s:?}"))
}

/// A signed decimal such as `"-54.00"`.
fn signed(s: &str) -> anyhow::Result<Fx> {
    match s.strip_prefix('-') {
        Some(m) => Ok(-px(m)?),
        None => px(s),
    }
}

/// YES levels are bids as they are; NO levels become asks at `1 - p`.
fn levels(l: &[[&str; 2]], no: bool) -> anyhow::Result<Vec<(Fx, Fx)>> {
    let mut v = Vec::with_capacity(l.len());
    for [p, q] in l {
        let (p, q) = (px(p)?, px(q)?);
        if q > 0 {
            v.push((if no { SCALE - p } else { p }, q));
        }
    }
    Ok(v)
}

// ---------------------------------------------------------------- REST

#[derive(Deserialize)]
struct RestMarket {
    ticker: String,
    #[serde(default)]
    event_ticker: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    price_level_structure: String,
}

#[derive(Deserialize)]
struct MarketsPage {
    #[serde(default)]
    markets: Vec<RestMarket>,
    cursor: Option<String>,
}

fn to_instrument(m: RestMarket) -> Option<Instrument<Meta>> {
    // A market announced on the stream may not be open yet; it is subscribed anyway and its
    // book fills once it opens. Only finished markets are left out.
    if matches!(m.status.as_str(), "closed" | "determined" | "settled" | "finalized") {
        return None;
    }
    let t: Sym = m.ticker.into();
    Some(Instrument {
        market: t.clone(),
        tokens: vec![t],
        meta: Meta { event: m.event_ticker, price_level_structure: m.price_level_structure },
    })
}

impl ExchangeAdapter for Kalshi {
    type Meta = Meta;
    type Session = Session;

    const NAME: &'static str = "kalshi";

    fn ws_url(&self) -> &str {
        &self.ws_url
    }

    fn ws_request(&self) -> anyhow::Result<Request> {
        let mut req = self.ws_url.as_str().into_client_request()?;
        if let Some(auth) = &self.auth {
            let ts = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_millis() as i64;
            let path = req.uri().path().to_string();
            for (k, v) in auth.headers(ts, &path) {
                req.headers_mut().insert(k, v.parse()?);
            }
        }
        Ok(req)
    }

    fn ping_interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    /// Kalshi keeps the socket alive with websocket pings; a text `PING` is not a command.
    fn ping_frame(&self) -> Option<&'static str> {
        None
    }

    /// The books, plus `market_lifecycle_v2` for listings and settlements. Lifecycle has no
    /// ticker filter, so every socket gets all of it; the core already dedups listings and
    /// removals that arrive on several sockets.
    fn subscribe_initial(&self, s: &mut Session, tokens: &[Sym]) -> Vec<String> {
        let mut v = self.subscribe(s, tokens);
        let id = s.id();
        v.push(serde_json::json!({"id": id, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}).to_string());
        v
    }

    fn subscribe(&self, s: &mut Session, tokens: &[Sym]) -> Vec<String> {
        let Some(sid) = s.main_sid else {
            let id = s.id();
            s.pending.insert(id, tokens.to_vec());
            return vec![
                serde_json::json!({"id": id, "cmd": "subscribe",
                    "params": {"channels": ["orderbook_delta"], "market_tickers": tokens}})
                .to_string(),
            ];
        };
        for t in tokens {
            s.sid_of.insert(t.clone(), sid);
        }
        s.update(tokens, "add_markets")
    }

    /// A ticker whose subscribe is not acknowledged yet stays subscribed on the venue; the core
    /// ignores books for tokens it no longer holds.
    fn unsubscribe(&self, s: &mut Session, tokens: &[Sym]) -> Vec<String> {
        let v = s.update(tokens, "delete_markets");
        for t in tokens {
            s.sid_of.remove(t);
            s.books.remove(t);
            for p in s.pending.values_mut() {
                p.retain(|x| x != t);
            }
        }
        v
    }

    /// `get_snapshot` resends the books and keeps the subscription: one command, no gap.
    /// Pending tickers need nothing: their first snapshot is on the way.
    fn resnapshot(&self, s: &mut Session, tokens: &[Sym]) -> Vec<String> {
        s.update(tokens, "get_snapshot")
    }

    fn decode(&self, s: &mut Session, frame: &str, out: &mut Vec<NormEvent>) -> anyhow::Result<()> {
        let f: Frame = serde_json::from_str(frame)?;
        // Every message on a subscription carries the next `seq`: a gap is a lost message,
        // and the books on this socket can no longer be trusted.
        if let (Some(sid), Some(seq)) = (f.sid, f.seq)
            && let Some(prev) = s.seq.insert(sid, seq)
            && seq != prev + 1
        {
            return Err(Resync(format!("sid {sid}: seq {seq} after {prev}")).into());
        }
        let m = f.msg.unwrap_or_default();
        match f.kind {
            "subscribed" => {
                if m.channel == Some("orderbook_delta")
                    && let Some(sid) = m.sid
                {
                    for t in f.id.and_then(|id| s.pending.remove(&id)).unwrap_or_default() {
                        s.sid_of.insert(t, sid);
                    }
                    s.main_sid.get_or_insert(sid);
                }
            }
            "orderbook_snapshot" => {
                let t: Sym = m.market_ticker.context("snapshot without market_ticker")?.into();
                let bids = levels(&m.yes_dollars_fp, false)?;
                let asks = levels(&m.no_dollars_fp, true)?;
                s.books.insert(t.clone(), Book::from_levels(&bids, &asks));
                out.push(NormEvent::Book {
                    token: t.clone(),
                    market: t,
                    bids,
                    asks,
                    exch_ts: m.ts_ms.or(f.sending_ts_ms).unwrap_or(0) * 1000,
                    check: None,
                    tick: None,
                    last_trade: None,
                });
            }
            "orderbook_delta" => {
                let t: Sym = m.market_ticker.context("delta without market_ticker")?.into();
                let p = px(m.price_dollars.context("delta without price_dollars")?)?;
                let d = signed(m.delta_fp.context("delta without delta_fp")?)?;
                let (side, p) = match m.side {
                    Some("yes") => (Side::Bid, p),
                    Some("no") => (Side::Ask, SCALE - p),
                    other => bail!("delta with side {other:?}"),
                };
                let Some(book) = s.books.get_mut(&t) else {
                    return Err(Resync(format!("{t}: delta before its snapshot")).into());
                };
                let qty = book.get(side, p) + d;
                if qty < 0 {
                    return Err(Resync(format!("{t}: delta takes {p} below zero")).into());
                }
                book.apply(side, p, qty);
                out.push(NormEvent::Levels {
                    market: t.clone(),
                    exch_ts: m.ts_ms.or(f.sending_ts_ms).unwrap_or(0) * 1000,
                    changes: vec![LevelChange { token: t, side, px: p, qty, check: None }],
                });
            }
            "market_lifecycle_v2" => {
                let market: Sym = m.market_ticker.context("lifecycle without market_ticker")?.into();
                match m.event_type {
                    Some("created" | "activated") => out.push(NormEvent::Listed { market }),
                    Some("determined" | "settled") => out.push(NormEvent::Resolved { market }),
                    _ => {}
                }
            }
            "error" => {
                let (code, text) = (m.code.unwrap_or(0), m.msg.unwrap_or_default());
                // 25: the venue's buffer for this socket overflowed and it dropped messages.
                if code == 25 {
                    return Err(Resync(format!("slow consumer (error 25): {text}")).into());
                }
                bail!("kalshi error {code}: {text}");
            }
            _ => {} // ok, unsubscribed: only their seq matters
        }
        Ok(())
    }

    async fn discover(&self, pages: mpsc::Sender<Vec<Instrument<Meta>>>) -> anyhow::Result<()> {
        let mut cursor: Option<String> = None;
        loop {
            let page: MarketsPage = super::get_json(|| {
                let req = self.http.get(format!("{}/markets", self.rest_url)).query(&[
                    ("status", "open"),
                    ("limit", PAGE),
                    ("mve_filter", "exclude"),
                ]);
                match &cursor {
                    Some(c) => req.query(&[("cursor", c)]),
                    None => req,
                }
            })
            .await?;
            let batch: Vec<_> = page.markets.into_iter().filter_map(to_instrument).collect();
            if !batch.is_empty() && pages.send(batch).await.is_err() {
                return Ok(());
            }
            match page.cursor.filter(|c| !c.is_empty()) {
                Some(c) => cursor = Some(c),
                None => return Ok(()),
            }
        }
    }

    async fn lookup(&self, markets: &[Sym]) -> anyhow::Result<Vec<Instrument<Meta>>> {
        let tickers = markets.join(",");
        let page: MarketsPage = super::get_json(|| {
            self.http
                .get(format!("{}/markets", self.rest_url))
                .query(&[("tickers", tickers.as_str()), ("mve_filter", "exclude"), ("limit", PAGE)])
        })
        .await?;
        Ok(page.markets.into_iter().filter_map(to_instrument).collect())
    }

    fn integrity(&self) -> Box<dyn Integrity<Meta>> {
        Box::new(NoCheck)
    }
}

/// Kalshi sends no book checksum. Its integrity check is the `seq` check in
/// [`Kalshi::decode`], which drops the socket on any gap.
pub struct NoCheck;

impl Integrity<Meta> for NoCheck {
    fn register(&mut self, _: &Instrument<Meta>) {}
    fn pre_event(&mut self, _: ConnId, _: &NormEvent, _: &dyn BookView, _: Instant, _: &mut Vec<Verdict>) {}
    fn on_event(&mut self, _: ConnId, _: &NormEvent, _: &dyn BookView, _: Instant, _: &mut Vec<Verdict>) {}
    fn on_tick(&mut self, _: Instant, _: &mut Vec<Verdict>) {}
    fn forget_conn(&mut self, _: ConnId) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn venue() -> Kalshi {
        Kalshi::with_endpoints(WS_URL, REST, None).unwrap()
    }

    fn decode(k: &Kalshi, s: &mut Session, f: &str) -> anyhow::Result<Vec<NormEvent>> {
        let mut out = Vec::new();
        k.decode(s, f, &mut out).map(|()| out)
    }

    fn is_resync(r: anyhow::Result<Vec<NormEvent>>) -> bool {
        r.is_err_and(|e| e.downcast_ref::<Resync>().is_some())
    }

    const SNAP: &str = r#"{"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"x","yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],"no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}}"#;

    #[test]
    fn snapshot_maps_no_bids_to_asks() {
        let (k, mut s) = (venue(), Session::default());
        let ev = decode(&k, &mut s, SNAP).unwrap();
        let [NormEvent::Book { token, bids, asks, .. }] = &ev[..] else { panic!("{ev:?}") };
        assert_eq!(&**token, "FED-23DEC-T3.00");
        assert_eq!(bids, &[(80_000, 300 * SCALE), (220_000, 333 * SCALE)]);
        // NO bids at 0.54 and 0.56 are YES offers at 0.46 and 0.44.
        assert_eq!(asks, &[(460_000, 20 * SCALE), (440_000, 146 * SCALE)]);
    }

    #[test]
    fn relative_deltas_become_absolute_sizes() {
        let (k, mut s) = (venue(), Session::default());
        decode(&k, &mut s, SNAP).unwrap();
        let delta = |seq: u64, px: &str, d: &str, side: &str| {
            format!(
                r#"{{"type":"orderbook_delta","sid":2,"seq":{seq},"msg":{{"market_ticker":"FED-23DEC-T3.00","price_dollars":"{px}","delta_fp":"{d}","side":"{side}","ts_ms":1669149841000}}}}"#
            )
        };
        let qty = |ev: &[NormEvent]| match ev {
            [NormEvent::Levels { exch_ts, changes, .. }] => {
                assert_eq!(*exch_ts, 1_669_149_841_000_000);
                (changes[0].side, changes[0].px, changes[0].qty)
            }
            _ => panic!("{ev:?}"),
        };
        let ev = decode(&k, &mut s, &delta(3, "0.0800", "-54.00", "yes")).unwrap();
        assert_eq!(qty(&ev), (Side::Bid, 80_000, 246 * SCALE));
        let ev = decode(&k, &mut s, &delta(4, "0.5400", "-20.00", "no")).unwrap();
        assert_eq!(qty(&ev), (Side::Ask, 460_000, 0));
        let ev = decode(&k, &mut s, &delta(5, "0.3000", "10.50", "no")).unwrap();
        assert_eq!(qty(&ev), (Side::Ask, 700_000, 10_500_000));
        // A delta below zero means the books disagree with the venue's.
        assert!(is_resync(decode(&k, &mut s, &delta(6, "0.0800", "-999.00", "yes"))));
    }

    #[test]
    fn sequence_gap_drops_the_socket() {
        let (k, mut s) = (venue(), Session::default());
        decode(&k, &mut s, SNAP).unwrap();
        let ok = r#"{"id":5,"sid":2,"seq":3,"type":"ok","msg":{"market_tickers":["FED-23DEC-T3.00"]}}"#;
        decode(&k, &mut s, ok).unwrap();
        let skipped = SNAP.replace(r#""seq":2"#, r#""seq":5"#);
        assert!(is_resync(decode(&k, &mut s, &skipped)));
        // Another subscription keeps its own count.
        let other = SNAP.replace(r#""sid":2,"seq":2"#, r#""sid":9,"seq":40"#);
        decode(&k, &mut s, &other).unwrap();
    }

    #[test]
    fn delta_before_snapshot_drops_the_socket() {
        let (k, mut s) = (venue(), Session::default());
        let d = r#"{"type":"orderbook_delta","sid":2,"seq":1,"msg":{"market_ticker":"A","price_dollars":"0.5","delta_fp":"1.00","side":"yes"}}"#;
        assert!(is_resync(decode(&k, &mut s, d)));
    }

    #[test]
    fn slow_consumer_error_drops_the_socket() {
        let (k, mut s) = (venue(), Session::default());
        let e = |code| format!(r#"{{"id":3,"type":"error","msg":{{"code":{code},"msg":"Buffer \"overflow\""}}}}"#);
        assert!(is_resync(decode(&k, &mut s, &e(25))));
        let other = decode(&k, &mut s, &e(6)).unwrap_err();
        assert!(other.downcast_ref::<Resync>().is_none());
        assert!(other.to_string().contains("error 6"), "{other}");
    }

    #[test]
    fn subscriptions_follow_the_acknowledged_sid() {
        let (k, mut s) = (venue(), Session::default());
        let a: Vec<Sym> = vec!["A".into(), "B".into()];
        let init = k.subscribe_initial(&mut s, &a);
        assert_eq!(init.len(), 2);
        let sub: serde_json::Value = serde_json::from_str(&init[0]).unwrap();
        assert_eq!(sub["id"], 1);
        assert_eq!(sub["params"]["channels"][0], "orderbook_delta");
        assert_eq!(sub["params"]["market_tickers"], serde_json::json!(["A", "B"]));
        assert!(init[1].contains("market_lifecycle_v2"));
        // Before the ack there is no sid to refresh.
        assert!(k.resnapshot(&mut s, &a).is_empty());
        decode(&k, &mut s, r#"{"id":2,"type":"subscribed","msg":{"channel":"market_lifecycle_v2","sid":1}}"#).unwrap();
        decode(&k, &mut s, r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}"#).unwrap();
        let add = k.subscribe(&mut s, &["C".into()]);
        let add: serde_json::Value = serde_json::from_str(&add[0]).unwrap();
        assert_eq!(add["cmd"], "update_subscription");
        assert_eq!(add["params"]["sids"], serde_json::json!([7]));
        assert_eq!(add["params"]["action"], "add_markets");
        let snap = k.resnapshot(&mut s, &["A".into(), "C".into()]);
        assert!(snap[0].contains(r#""action":"get_snapshot""#) && snap[0].contains(r#""market_tickers":["A","C"]"#));
        let del = k.unsubscribe(&mut s, &["A".into(), "B".into(), "C".into()]);
        assert_eq!(del.len(), 1, "{del:?}");
        assert!(del[0].contains(r#""action":"delete_markets""#) && del[0].contains(r#""market_tickers":["A","B","C"]"#));
        assert!(k.unsubscribe(&mut s, &["A".into()]).is_empty());
    }

    #[test]
    fn lifecycle_lists_and_resolves() {
        let (k, mut s) = (venue(), Session::default());
        let l = |e: &str| {
            format!(r#"{{"type":"market_lifecycle_v2","sid":1,"msg":{{"market_ticker":"NEW-1","event_type":"{e}"}}}}"#)
        };
        assert!(matches!(&decode(&k, &mut s, &l("activated")).unwrap()[..], [NormEvent::Listed { market }] if &**market == "NEW-1"));
        assert!(matches!(&decode(&k, &mut s, &l("settled")).unwrap()[..], [NormEvent::Resolved { .. }]));
        assert!(decode(&k, &mut s, &l("metadata_updated")).unwrap().is_empty());
    }

    #[test]
    fn markets_page_becomes_instruments() {
        let page: MarketsPage = serde_json::from_str(
            r#"{"cursor":"abc","markets":[
                {"ticker":"A","event_ticker":"EV","status":"active","market_type":"binary","price_level_structure":"linear_cent"},
                {"ticker":"B","event_ticker":"EV","status":"settled"}]}"#,
        )
        .unwrap();
        let v: Vec<_> = page.markets.into_iter().filter_map(to_instrument).collect();
        assert_eq!(v.len(), 1);
        assert_eq!((&*v[0].market, v[0].tokens.len(), v[0].meta.event.as_str()), ("A", 1, "EV"));
    }

    /// Checks the three headers on a fresh handshake request against `verify`.
    fn check_handshake(auth: Auth, verify: impl Fn(&[u8], &[u8]) -> bool) {
        let k = Kalshi::with_endpoints(WS_URL, REST, Some(auth)).unwrap();
        let req = k.ws_request().unwrap();
        let h = |n: &str| req.headers()[n].to_str().unwrap().to_string();
        assert_eq!(h("kalshi-access-key"), "key-id");
        let msg = format!("{}GET/trade-api/ws/v2", h("kalshi-access-timestamp"));
        let sig = base64::engine::general_purpose::STANDARD.decode(h("kalshi-access-signature")).unwrap();
        assert!(verify(msg.as_bytes(), &sig));
    }

    #[test]
    fn rsa_pss_handshake_signature_verifies() {
        use rand::SeedableRng;
        use rsa::{pkcs1::EncodeRsaPrivateKey, signature::Verifier};
        let key = rsa::RsaPrivateKey::new(&mut rand::rngs::StdRng::seed_from_u64(7), 1024).unwrap();
        let pem = key.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let vk = rsa::pss::VerifyingKey::<Sha256>::new(key.to_public_key());
        check_handshake(Auth::from_pem("key-id", &pem).unwrap(), |m, s| {
            vk.verify(m, &rsa::pss::Signature::try_from(s).unwrap()).is_ok()
        });
    }

    #[test]
    fn ed25519_handshake_signature_verifies() {
        use ed25519_dalek::{Verifier, pkcs8::EncodePrivateKey};
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let pem = key.to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF).unwrap();
        let vk = key.verifying_key();
        check_handshake(Auth::from_pem("key-id", &pem).unwrap(), |m, s| {
            vk.verify(m, &ed25519_dalek::Signature::from_slice(s).unwrap()).is_ok()
        });
    }
}
