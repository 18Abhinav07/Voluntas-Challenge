//! The single owner of all book state (§5.4–5.6): replica books per connection, the per-token
//! source arbiter, failure detection, shard planning and recycling. Synchronous and
//! deterministic: time comes in as arguments and every side effect goes through [`Effects`],
//! so the whole failover logic is unit-testable without sockets.
//!
//! This file holds the data model ([`Core`] and its per-token, per-shard and per-socket state),
//! event intake and the tick loop. Behaviour is split by concern, each an `impl Core` block:
//!
//! | File | Concern |
//! |---|---|
//! | `config.rs` | tuning ([`Config`], [`Quiet`], [`Fast`]) |
//! | `metrics.rs` | counters and latency histograms ([`Metrics`], [`SockLat`]) |
//! | `plan.rs` | placing markets on shards and sockets, removing them, hot shard splits |
//! | `conns.rs` | socket up/down/gone, make-before-break recycling (Ext 2) |
//! | `emit.rs` | what readers see: rotation, snapshots, diffs, source switches (D8) |
//! | `health.rs` | integrity ladder (§5.5), resnapshots, quiet and lag checks (Ext 2) |
//! | `speed.rs` | speed-aware source choice (Ext 4) |

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use crate::{
    adapter::{BookView, ConnId, Integrity, NormEvent, Sym, Ts, Verdict},
    book::Book,
    conn::ConnCmd,
    writer::Rec,
};

mod config;
mod conns;
mod emit;
mod health;
mod metrics;
mod plan;
mod speed;

pub use config::{Config, Fast, Quiet};
pub use metrics::{Hist, Metrics, MsHist, SockLat};
use speed::Arrivals;

/// Tokens found mid-discovery wait at most this long for a full shard before getting one.
const PENDING_MAX_WAIT: Duration = Duration::from_secs(5);

/// Shard rates are smoothed per minute; splits wait until they mean something.
const SPLIT_WARMUP_TICKS: u64 = 180;

/// What the core asks the outside world to do.
pub trait Effects {
    /// Opens connection `id` (the `replica`-th of its shard) subscribed to `tokens`.
    fn spawn_conn(&mut self, id: ConnId, replica: usize, tokens: Vec<Sym>);
    fn send(&mut self, id: ConnId, cmd: ConnCmd);
    /// Resolves a market announced on the stream into instruments (see [`Core::add`]).
    fn lookup(&mut self, market: Sym);
}

struct Replica {
    book: Book,
    valid: bool,
    /// Timestamp of the last event applied (the `exch_ts` of a snapshot taken from it).
    last_exch: Ts,
    /// Newest delta (`price_change`) timestamp. Kept apart from `book`s: a fresh dump is
    /// stamped at dump time, so it says nothing about which deltas a peer has missed.
    last_delta: Ts,
    /// When `last_delta` last advanced.
    adv_at: Instant,
    /// The oldest delta here that the source had not seen, and when it arrived: the lag check
    /// times the source from it. Once the source catches up it moves to `last_delta`, which
    /// may be later than the next unseen delta; detection can be late, never early.
    unseen: Option<(Ts, Instant)>,
    last_book_ts: Ts,
    last_ok_ts: Ts,
    /// Hash misses since the last verified match.
    strikes: u8,
    /// While the replica waits for a `book`: when to ask again if none has come.
    book_due: Option<Instant>,
}

impl Replica {
    /// Newest venue state this replica has acknowledged, by delta or by book.
    fn seen(&self) -> Ts {
        self.last_delta.max(self.last_book_ts)
    }

    fn new(now: Instant) -> Self {
        Self {
            book: Book::default(),
            valid: false,
            last_exch: 0,
            last_delta: 0,
            adv_at: now,
            unseen: None,
            last_book_ts: 0,
            last_ok_ts: 0,
            strikes: 0,
            book_due: None,
        }
    }
}

/// A token's replicas by socket. Two or three entries, so a vector searched in place: a
/// `HashMap` costs about 500 bytes per token for the same thing.
#[derive(Default)]
struct Reps(Vec<(ConnId, Replica)>);

impl Reps {
    fn get(&self, c: &ConnId) -> Option<&Replica> {
        self.0.iter().find(|e| e.0 == *c).map(|e| &e.1)
    }
    fn get_mut(&mut self, c: &ConnId) -> Option<&mut Replica> {
        self.0.iter_mut().find(|e| e.0 == *c).map(|e| &mut e.1)
    }
    fn insert(&mut self, c: ConnId, r: Replica) {
        match self.get_mut(&c) {
            Some(old) => *old = r,
            None => {
                self.0.reserve_exact(1);
                self.0.push((c, r));
            }
        }
    }
    fn remove(&mut self, c: &ConnId) -> Option<Replica> {
        let i = self.0.iter().position(|e| e.0 == *c)?;
        let r = self.0.swap_remove(i).1;
        self.0.shrink_to_fit();
        Some(r)
    }
    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.len()
    }
    fn iter(&self) -> impl Iterator<Item = (&ConnId, &Replica)> {
        self.0.iter().map(|(c, r)| (c, r))
    }
    fn iter_mut(&mut self) -> impl Iterator<Item = (&ConnId, &mut Replica)> {
        self.0.iter_mut().map(|(c, r)| (&*c, r))
    }
    fn keys(&self) -> impl Iterator<Item = &ConnId> {
        self.0.iter().map(|e| &e.0)
    }
    fn values(&self) -> impl Iterator<Item = &Replica> {
        self.0.iter().map(|e| &e.1)
    }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut Replica> {
        self.0.iter_mut().map(|e| &mut e.1)
    }
}

impl std::ops::Index<&ConnId> for Reps {
    type Output = Replica;
    fn index(&self, c: &ConnId) -> &Replica {
        self.get(c).expect("no replica on this socket")
    }
}

struct Tok {
    shard: usize,
    /// Records name a side as `<token>:bid` / `<token>:ask`; the writer adds the suffix.
    token: Sym,
    source: Option<ConnId>,
    replicas: Reps,
    /// The book readers of the output hold: every emitted record is applied here, so
    /// snapshots always agree with the update stream, across source switches too.
    emitted: Book,
    emitted_exch: Ts,
    /// Whether the first snapshot has been written.
    announced: bool,
    /// Newest delta timestamp any replica has seen, and when it last advanced (Ext 2).
    newest: Ts,
    active_at: Instant,
    /// New deltas so far this minute, and the smoothed per-minute rate.
    this_min: u32,
    rate: f32,
    /// Last quiet resubscribe and how many in a row have not brought deltas back.
    quiet_at: Option<Instant>,
    quiet_strikes: u8,
    /// A quiet resubscribe is out: the source's next book says whether it caught a miss.
    quiet_probe: bool,
    /// Listed in `Core::lag_cands`: a non-source replica advanced since the last lag check.
    lag_cand: bool,
    /// Rotation period this token was last snapshotted in. A period's file must open each book
    /// with a snapshot before its first update; see [`Core::maybe_rotate`].
    snap_epoch: i64,
}

impl Tok {
    /// How long this token may stay silent before it counts as quiet; `None` = too slow to tell.
    fn quiet_after(&self, q: &Quiet) -> Option<Duration> {
        if self.rate < q.min_rate {
            return None;
        }
        let base = Duration::from_secs_f32(q.factor * 60.0 / self.rate).clamp(q.floor, q.ceiling);
        Some(base.saturating_mul(1 << self.quiet_strikes.min(10)).min(q.ceiling))
    }
}

struct Shard {
    tokens: Vec<Sym>,
    conns: Vec<ConnId>,
    /// Split off a hot shard: late arrivals go elsewhere.
    closed: bool,
    /// The socket preferred as source, by speed (`Config::fast`).
    fast: Option<ConnId>,
}

/// A hot shard's markets moving to a new shard. The old sockets stay replicas of the moved
/// tokens (and `from` keeps listing them) until the new sockets have every book.
struct Split {
    from: usize,
    to: usize,
    moved: Vec<Sym>,
    started: Instant,
}

struct ConnState {
    shard: usize,
    replica: usize,
    born: Instant,
    /// Set while this connection is the new half of a make-before-break recycle.
    replacing: Option<ConnId>,
    awaiting: HashSet<Sym>,
    retiring: bool,
    up_since: Option<Instant>,
    /// Delta events this quiet window, and the smoothed count per window.
    win_events: u64,
    win_rate: f64,
    /// `behind` summed (µs) and counted this window, the smoothed score in ms, and windows
    /// in a row scored slowest of the shard (`Config::fast`).
    beh_sum: i64,
    beh_n: u64,
    speed: Option<f64>,
    slow_windows: u32,
}

pub struct Core<M> {
    cfg: Config,
    integrity: Box<dyn Integrity<M>>,
    /// Boxed: the map keeps about twice as many slots as entries, and a slot is then 8 bytes, not 200.
    tokens: HashMap<Sym, Box<Tok>>,
    markets: HashMap<Sym, Vec<Sym>>,
    shards: Vec<Shard>,
    /// Tokens collected for the next shard, not yet subscribed anywhere.
    pending: Vec<Sym>,
    /// When `pending` became non-empty; a partial shard is opened after [`PENDING_MAX_WAIT`].
    pending_since: Option<Instant>,
    conns: HashMap<ConnId, ConnState>,
    next_conn: ConnId,
    looking_up: HashSet<Sym>,
    /// 60 one-second snapshot buckets; a token's bucket is a hash of its id.
    buckets: Vec<Vec<Sym>>,
    /// Rotation period the output is in; `None` until the first tick.
    epoch: Option<i64>,
    /// Books still to snapshot into the current period, and how many to take per tick.
    rot_queue: Vec<Sym>,
    rot_per_tick: usize,
    tick_no: u64,
    verdicts: Vec<Verdict>,
    /// Tokens the lag check must look at: only these can have a peer ahead of their source.
    lag_cands: Vec<Sym>,
    /// At most one shard split in flight per core.
    split: Option<Split>,
    /// Per-socket latency and per-market arrival windows, only with `cfg.lat_detail`.
    pub lat: HashMap<ConnId, SockLat>,
    arrivals: HashMap<Sym, Arrivals>,
    pub out: Vec<Rec>,
    pub metrics: Metrics,
}

/// What the integrity checker may verify: `conn`'s book, but only where `conn` is the token's
/// source. Standby replicas are not hashed; a peer is compared book-to-book when the source
/// misses, and a standby is checked from the moment it becomes source.
struct View<'a> {
    tokens: &'a HashMap<Sym, Box<Tok>>,
    conn: ConnId,
}

impl BookView for View<'_> {
    fn book(&self, token: &str) -> Option<&Book> {
        let tok = self.tokens.get(token)?;
        if tok.source != Some(self.conn) {
            return None;
        }
        tok.replicas.get(&self.conn).filter(|r| r.valid).map(|r| &r.book)
    }
}

fn bucket_of(token: &str, n: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    token.hash(&mut h);
    (h.finish() % n as u64) as usize
}

impl<M> Core<M> {
    pub fn new(cfg: Config, integrity: Box<dyn Integrity<M>>) -> Self {
        let n = cfg.snapshot_every.as_secs().max(1) as usize;
        Self {
            next_conn: cfg.conn_id_base,
            cfg,
            integrity,
            tokens: HashMap::new(),
            markets: HashMap::new(),
            shards: Vec::new(),
            pending: Vec::new(),
            pending_since: None,
            conns: HashMap::new(),
            looking_up: HashSet::new(),
            buckets: vec![Vec::new(); n],
            epoch: None,
            rot_queue: Vec::new(),
            rot_per_tick: 0,
            tick_no: 0,
            verdicts: Vec::new(),
            lag_cands: Vec::new(),
            split: None,
            lat: HashMap::new(),
            arrivals: HashMap::new(),
            out: Vec::new(),
            metrics: Metrics::default(),
        }
    }

    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }

    pub fn sourced_count(&self) -> usize {
        self.tokens.values().filter(|t| t.source.is_some()).count()
    }

    pub fn conn_count(&self) -> usize {
        self.conns.len()
    }

    pub fn on_events(&mut self, conn: ConnId, recv: Ts, events: &[NormEvent], now: Instant, fx: &mut dyn Effects) {
        if let Some(cs) = self.conns.get_mut(&conn) {
            cs.win_events += events.iter().filter(|e| matches!(e, NormEvent::Levels { .. })).count() as u64;
        }
        for ev in events {
            self.integrity.pre_event(conn, ev, &View { tokens: &self.tokens, conn }, now, &mut self.verdicts);
            self.apply(conn, recv, ev, now, fx);
            self.integrity.on_event(conn, ev, &View { tokens: &self.tokens, conn }, now, &mut self.verdicts);
            self.handle_verdicts(recv, now, fx);
        }
    }

    fn apply(&mut self, conn: ConnId, recv: Ts, ev: &NormEvent, now: Instant, fx: &mut dyn Effects) {
        match ev {
            NormEvent::Book { token, bids, asks, exch_ts, .. } => {
                let Some(tok) = self.tokens.get_mut(token) else { return };
                let Some(rep) = tok.replicas.get_mut(&conn) else { return };
                rep.book = Book::from_levels(bids, asks);
                rep.valid = true;
                rep.book_due = None;
                rep.last_book_ts = rep.last_book_ts.max(*exch_ts);
                rep.last_exch = rep.last_exch.max(*exch_ts);
                if tok.source == Some(conn) {
                    // Mid-stream book on the source: emit whatever it corrects.
                    let fixed = Self::emit_diff(tok, conn, self.epoch, recv, &mut self.out, &mut self.metrics);
                    if fixed {
                        self.metrics.book_diffs += 1;
                    }
                    if std::mem::take(&mut tok.quiet_probe) && fixed {
                        self.metrics.quiet_caught += 1;
                        tok.quiet_strikes = 0;
                    }
                } else if tok.source.is_none() {
                    Self::adopt(tok, conn, self.epoch, recv, &mut self.out, &mut self.metrics);
                }
                if let Some(cs) = self.conns.get_mut(&conn) {
                    cs.awaiting.remove(token);
                }
                self.maybe_finish_recycle(conn, recv, fx);
                self.maybe_finish_split(recv, fx);
            }
            NormEvent::Levels { market, exch_ts, changes } => {
                self.metrics.wire.record(recv - exch_ts);
                if (self.cfg.lat_detail || self.cfg.fast.is_some())
                    && let Some(b) = self.note_arrival(conn, market, *exch_ts, recv)
                    && changes.first().and_then(|c| self.tokens.get(&c.token)).is_some_and(|t| t.source == Some(conn))
                {
                    self.metrics.src_behind.record(b);
                }
                for c in changes {
                    let Some(tok) = self.tokens.get_mut(&c.token) else { continue };
                    let src_seen = tok.source.filter(|&s| s != conn).and_then(|s| tok.replicas.get(&s)).map(Replica::seen);
                    let Some(rep) = tok.replicas.get_mut(&conn) else { continue };
                    if *exch_ts > tok.newest {
                        tok.newest = *exch_ts;
                        tok.active_at = now;
                        tok.this_min += 1;
                    }
                    if !rep.valid {
                        continue;
                    }
                    rep.book.apply(c.side, c.px, c.qty);
                    rep.last_exch = rep.last_exch.max(*exch_ts);
                    if *exch_ts > rep.last_delta {
                        rep.last_delta = *exch_ts;
                        rep.adv_at = now;
                        if src_seen.is_some_and(|s| *exch_ts > s) && rep.unseen.is_none() {
                            rep.unseen = Some((*exch_ts, now));
                        }
                        if tok.source != Some(conn) && !tok.lag_cand {
                            tok.lag_cand = true;
                            self.lag_cands.push(c.token.clone());
                        }
                    }
                    if tok.source == Some(conn) {
                        Self::open_period(tok, self.epoch, recv, &mut self.out, &mut self.metrics);
                    }
                    if tok.source == Some(conn) && tok.emitted.apply(c.side, c.px, c.qty) {
                        tok.emitted_exch = *exch_ts;
                        self.out.push(Rec::Update { sym: tok.token.clone(), side: c.side, recv, exch: *exch_ts, px: c.px, qty: c.qty });
                        self.metrics.updates += 1;
                    }
                }
            }
            NormEvent::Listed { market } => {
                if !self.markets.contains_key(market) && self.looking_up.insert(market.clone()) {
                    fx.lookup(market.clone());
                }
            }
            NormEvent::Resolved { market } => self.remove_market(market, recv, fx),
            NormEvent::Trade { .. } | NormEvent::TickSize { .. } => {}
        }
    }

    /// Call once per second.
    pub fn on_tick(&mut self, now: Instant, recv: Ts, fx: &mut dyn Effects) {
        self.maybe_rotate(recv);
        self.drain_rotation(recv);
        self.integrity.on_tick(now, &mut self.verdicts);
        self.handle_verdicts(recv, now, fx);
        self.check_lag(now, recv, fx);
        if self.tick_no.is_multiple_of(10) {
            self.retry_resnapshots(now, fx);
        }
        if self.tick_no > 0 && self.tick_no.is_multiple_of(60) {
            self.roll_rates(now);
            if self.tick_no >= SPLIT_WARMUP_TICKS {
                self.maybe_split(now, fx);
            }
        }
        if self.split.as_ref().is_some_and(|s| now.duration_since(s.started) >= self.cfg.recycle_grace) {
            self.finish_split(recv, fx);
        }
        if self.tick_no.is_multiple_of(5) {
            self.check_quiet(now, fx);
        }
        let win = self.cfg.quiet.conn_window.as_secs().max(1);
        if self.tick_no > 0 && self.tick_no.is_multiple_of(win) {
            self.check_quiet_conns(now, fx);
            self.check_speed(now, recv, fx);
        }

        let b = (self.tick_no % self.buckets.len() as u64) as usize;
        self.tick_no += 1;
        let tokens = &self.tokens;
        self.buckets[b].retain(|t| tokens.contains_key(t));
        for t in &self.buckets[b] {
            let tok = self.tokens.get_mut(t).expect("retained above");
            if tok.source.is_some() {
                Self::snapshot(tok, self.epoch, recv, &mut self.out, &mut self.metrics);
            }
        }
        if self.pending_since.is_some_and(|t| now.duration_since(t) >= PENDING_MAX_WAIT) {
            self.flush_pending(now, fx);
        }
        self.maybe_start_recycle(now, fx);
        let grace = self.cfg.recycle_grace;
        let overdue: Vec<ConnId> = self
            .conns
            .iter()
            .filter(|(_, c)| c.replacing.is_some() && now.duration_since(c.born) >= grace)
            .map(|(id, _)| *id)
            .collect();
        for id in overdue {
            self.finish_recycle(id, recv, fx);
        }
    }

}

#[cfg(test)]
mod tests;
