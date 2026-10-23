//! Ext 3: the exchange boundary. Everything outside `adapter/` (connections, books,
//! arbiter, health, recycling, writer) is venue-agnostic and works on [`NormEvent`]s.

pub mod polymarket;

use std::{future::Future, sync::Arc, time::Duration};

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;

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

pub trait ExchangeAdapter: Send + Sync + 'static {
    type Meta: Clone + Send + Sync + Serialize + DeserializeOwned + 'static;

    const NAME: &'static str;

    fn ws_url(&self) -> &str;
    fn ping_interval(&self) -> Duration;
    fn ping_frame(&self) -> &'static str;
    fn subscribe_initial(&self, tokens: &[Sym]) -> String;
    fn subscribe(&self, tokens: &[Sym]) -> String;
    fn unsubscribe(&self, tokens: &[Sym]) -> String;

    /// Decodes one text frame. Must not allocate per level beyond the output vectors.
    fn decode(&self, frame: &str, out: &mut Vec<NormEvent>) -> anyhow::Result<()>;

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
