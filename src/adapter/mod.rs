//! Ext 3: the exchange boundary. Everything outside `adapter/` (connections, books,
//! arbiter, health, recycling, writer) is venue-agnostic and works on [`NormEvent`]s.

pub mod kalshi;
pub mod polymarket;

use std::{future::Future, sync::Arc, time::Duration};

use reqwest::{StatusCode, header::RETRY_AFTER};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};

use crate::{
    book::{Book, Side},
    fixed::Fx,
};

/// Interned identifier (token id, market id). Cheap to clone and hash.
pub type Sym = Arc<str>;
/// Microseconds since the Unix epoch.
pub type Ts = i64;
/// Identifies one websocket connection for its whole life (reconnects keep the id).
pub type ConnId = u32;

/// One tradable market and the order books (tokens) it owns.
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct Instrument<M> {
    pub market: Sym,
    pub tokens: Vec<Sym>,
    pub meta: M,
}

/// A decoded websocket event, normalized. Decoding happens in the connection task.
#[derive(Clone, Debug)]
pub enum NormEvent {
    /// Full book for one token. `check` is the venue's checksum of this state, if any.
    Book {
        token: Sym,
        market: Sym,
        bids: Vec<(Fx, Fx)>,
        asks: Vec<(Fx, Fx)>,
        exch_ts: Ts,
        check: Option<Box<str>>,
        tick: Option<Box<str>>,
        last_trade: Option<Box<str>>,
    },
    /// One atomic exchange event: absolute level quantities, applied together.
    Levels {
        market: Sym,
        exch_ts: Ts,
        changes: Vec<LevelChange>,
    },
    Trade {
        market: Sym,
        token: Sym,
        px: Fx,
        exch_ts: Ts,
    },
    TickSize {
        token: Sym,
        tick: Box<str>,
    },
    Listed {
        market: Sym,
    },
    Resolved {
        market: Sym,
    },
}

#[derive(Clone, Debug)]
pub struct LevelChange {
    pub token: Sym,
    pub side: Side,
    pub px: Fx,
    pub qty: Fx,
    /// Venue checksum of the token's book after this change (Polymarket `hash`).
    pub check: Option<Box<str>>,
}

/// Outcome of an integrity check of one replica's book.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Ok {
        conn: ConnId,
        token: Sym,
        exch_ts: Ts,
    },
    /// The checksum still disagrees after every allowed deferral.
    Miss {
        conn: ConnId,
        token: Sym,
        exch_ts: Ts,
    },
    /// A full book whose own checksum does not verify (counted, nothing to repair).
    BookMiss { conn: ConnId, token: Sym },
}

/// Read access to one connection's replica books.
pub trait BookView {
    fn book(&self, token: &str) -> Option<&Book>;
}

/// Venue-specific verification of replica books against exchange checksums.
/// The core calls `pre_event` with the replica's books *before* applying an event and
/// `on_event` right after, so checks can see both the old and the new state.
pub trait Integrity<M>: Send {
    /// Registers instrument metadata the checker needs (tick, flags, ...).
    fn register(&mut self, inst: &Instrument<M>);
    fn pre_event(
        &mut self,
        conn: ConnId,
        ev: &NormEvent,
        books: &dyn BookView,
        now: std::time::Instant,
        out: &mut Vec<Verdict>,
    );
    fn on_event(
        &mut self,
        conn: ConnId,
        ev: &NormEvent,
        books: &dyn BookView,
        now: std::time::Instant,
        out: &mut Vec<Verdict>,
    );
    /// Expires deferred checks.
    fn on_tick(&mut self, now: std::time::Instant, out: &mut Vec<Verdict>);
    fn forget_conn(&mut self, conn: ConnId);
}

/// A decode error meaning the socket's stream can no longer be trusted: a sequence gap, or the
/// venue saying it dropped messages for us. The connection drops the socket and reconnects,
/// which brings fresh books; the core fails the replica over meanwhile, as for any lost socket.
#[derive(Debug)]
pub struct Resync(pub String);

impl std::fmt::Display for Resync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "resync: {}", self.0)
    }
}

impl std::error::Error for Resync {}

pub trait ExchangeAdapter: Send + Sync + 'static {
    type Meta: Clone + Send + Sync + Serialize + DeserializeOwned + 'static;
    /// State one socket keeps between frames, made fresh on every connect. Polymarket needs
    /// none; Kalshi keeps subscription ids, sequence numbers, and the books it needs to turn
    /// relative deltas into absolute sizes.
    type Session: Default + Send;

    const NAME: &'static str;

    fn ws_url(&self) -> &str;
    /// The handshake request, built afresh for each connect. Default: a plain GET of `ws_url`.
    fn ws_request(&self) -> anyhow::Result<Request> {
        Ok(self.ws_url().into_client_request()?)
    }
    fn ping_interval(&self) -> Duration;
    /// A text frame to send as the keepalive, or `None` for a websocket ping.
    fn ping_frame(&self) -> Option<&'static str>;
    fn subscribe_initial(&self, s: &mut Self::Session, tokens: &[Sym]) -> Vec<String>;
    fn subscribe(&self, s: &mut Self::Session, tokens: &[Sym]) -> Vec<String>;
    fn unsubscribe(&self, s: &mut Self::Session, tokens: &[Sym]) -> Vec<String>;
    /// Asks for fresh books. Default: unsubscribe, then subscribe again.
    fn resnapshot(&self, s: &mut Self::Session, tokens: &[Sym]) -> Vec<String> {
        let mut v = self.unsubscribe(s, tokens);
        v.extend(self.subscribe(s, tokens));
        v
    }
    /// Whether a close frame's reason says every asset the socket subscribed to has resolved.
    /// Resubscribing those assets is refused, so the socket must drop them instead.
    fn all_resolved(&self, _close_reason: &str) -> bool {
        false
    }

    /// Decodes one text frame. Must not allocate per level beyond the output vectors.
    /// A [`Resync`] error drops the socket; any other error skips the frame.
    fn decode(&self, s: &mut Self::Session, frame: &str, out: &mut Vec<NormEvent>) -> anyhow::Result<()>;

    /// One full pass over the venue's live instruments, streamed page by page.
    fn discover(
        &self,
        pages: mpsc::Sender<Vec<Instrument<Self::Meta>>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    /// Looks up a batch of markets announced on the stream, in one request where the venue allows.
    fn lookup(
        &self,
        markets: &[Sym],
    ) -> impl Future<Output = anyhow::Result<Vec<Instrument<Self::Meta>>>> + Send;

    fn integrity(&self) -> Box<dyn Integrity<Self::Meta>>;
}

/// REST GETs are retried this many times in all; the wait doubles from `HTTP_BACKOFF`.
const HTTP_ATTEMPTS: u32 = 8;
const HTTP_BACKOFF: Duration = Duration::from_millis(500);
const HTTP_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Sends a GET built by `req`, backing off on 429, 5xx and transport errors (honouring
/// `Retry-After`) so a rate limit slows a discovery pass down instead of ending it.
pub(crate) async fn get_json<T: DeserializeOwned>(req: impl Fn() -> reqwest::RequestBuilder) -> anyhow::Result<T> {
    let mut wait = HTTP_BACKOFF;
    for attempt in 1..=HTTP_ATTEMPTS {
        let retry_after = match req().send().await {
            Ok(r) if r.status() == StatusCode::TOO_MANY_REQUESTS || r.status().is_server_error() => {
                let after = r
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()?.parse().ok())
                    .map(Duration::from_secs);
                if attempt == HTTP_ATTEMPTS {
                    r.error_for_status()?;
                }
                after
            }
            Ok(r) => return Ok(r.error_for_status()?.json().await?),
            Err(e) if attempt == HTTP_ATTEMPTS => return Err(e.into()),
            Err(_) => None,
        };
        tokio::time::sleep(retry_after.unwrap_or(wait).min(HTTP_BACKOFF_MAX)).await;
        wait = (wait * 2).min(HTTP_BACKOFF_MAX);
    }
    unreachable!("the last attempt returns")
}
