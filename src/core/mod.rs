//! The single owner of all book state (§5.4–5.6): replica books per connection, the per-token
//! source arbiter, failure detection, shard planning and recycling. Synchronous and
//! deterministic: time comes in as arguments and every side effect goes through [`Effects`],
//! so the whole failover logic is unit-testable without sockets.

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use crate::{
    adapter::{BookView, ConnId, Instrument, Integrity, NormEvent, Sym, Ts, Verdict},
    book::{Book, Side},
    conn::ConnCmd,
    writer::Rec,
};

mod config;
mod conns;
mod emit;
mod health;
mod metrics;

pub use config::{Config, Fast, Quiet};
pub use metrics::{Hist, Metrics, MsHist, SockLat};

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

const ARRIVALS: usize = 16;

/// A market's last few events by exchange timestamp: (exch, first recv, replicas seen as bits).
#[derive(Default)]
struct Arrivals {
    ring: [(Ts, Ts, u64); ARRIVALS],
    next: usize,
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

struct Tok {
    shard: usize,
    syms: [Sym; 2], // bid, ask
    source: Option<ConnId>,
    replicas: HashMap<ConnId, Replica>,
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
    tokens: HashMap<Sym, Tok>,
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
    tokens: &'a HashMap<Sym, Tok>,
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

    // ------------------------------------------------------------ discovery / planning

    /// Adds instruments (deduplicated). Both tokens of a market land in the same shard.
    pub fn add(&mut self, insts: Vec<Instrument<M>>, fx: &mut dyn Effects) {
        for inst in insts {
            self.looking_up.remove(&inst.market);
            if self.markets.contains_key(&inst.market) {
                continue;
            }
            self.integrity.register(&inst);
            self.metrics.markets_added += 1;
            // Late arrivals join the last live shard while it has room.
            let tail = self.shards.iter().rposition(|s| !s.closed).filter(|&s| {
                self.pending.is_empty() && self.shards[s].tokens.len() + inst.tokens.len() <= self.cfg.tokens_per_conn
            });
            let shard = tail.unwrap_or(self.shards.len());
            let now = Instant::now();
            for t in &inst.tokens {
                self.tokens.insert(
                    t.clone(),
                    Tok {
                        shard,
                        syms: [format!("{t}:bid").into(), format!("{t}:ask").into()],
                        source: None,
                        replicas: HashMap::new(),
                        emitted: Book::default(),
                        emitted_exch: 0,
                        announced: false,
                        newest: 0,
                        active_at: now,
                        this_min: 0,
                        rate: 0.0,
                        quiet_at: None,
                        quiet_strikes: 0,
                        quiet_probe: false,
                        lag_cand: false,
                        snap_epoch: i64::MIN,
                    },
                );
                let b = bucket_of(t, self.buckets.len());
                self.buckets[b].push(t.clone());
            }
            self.markets.insert(inst.market.clone(), inst.tokens.clone());
            if let Some(s) = tail {
                self.shards[s].tokens.extend(inst.tokens.iter().cloned());
                for &c in &self.shards[s].conns.clone() {
                    for t in &inst.tokens {
                        let mut r = Replica::new(now);
                        r.book_due = Some(now + self.cfg.book_timeout);
                        self.tokens.get_mut(t).expect("inserted above").replicas.insert(c, r);
                    }
                    fx.send(c, ConnCmd::Subscribe(inst.tokens.clone()));
                }
            } else {
                self.pending_since.get_or_insert_with(Instant::now);
                self.pending.extend(inst.tokens.iter().cloned());
                if self.pending.len() >= self.cfg.tokens_per_conn {
                    self.flush_pending(Instant::now(), fx);
                }
            }
        }
    }

    /// Opens connections for tokens collected so far (call when discovery pauses or ends).
    pub fn flush_pending(&mut self, now: Instant, fx: &mut dyn Effects) {
        if self.pending.is_empty() {
            return;
        }
        if (self.shards.len() + 1) * self.cfg.replicas > self.cfg.max_conns {
            if self.metrics.unplaced == 0 {
                tracing::warn!(max_conns = self.cfg.max_conns, "connection cap reached; new tokens stay unplaced");
            }
            self.metrics.unplaced = self.pending.len() as u64;
            self.pending_since = None;
            return;
        }
        self.pending_since = None;
        let shard = self.shards.len();
        let tokens = std::mem::take(&mut self.pending);
        self.shards.push(Shard { tokens: tokens.clone(), conns: Vec::new(), closed: false, fast: None });
        for r in 0..self.cfg.replicas {
            self.open_conn(shard, r, now, None, fx);
        }
    }

    fn open_conn(&mut self, shard: usize, replica: usize, now: Instant, replacing: Option<ConnId>, fx: &mut dyn Effects) -> ConnId {
        let id = self.next_conn;
        self.next_conn += 1;
        let tokens = self.shards[shard].tokens.clone();
        for t in &tokens {
            if let Some(tok) = self.tokens.get_mut(t) {
                tok.replicas.insert(id, Replica::new(now));
            }
        }
        self.shards[shard].conns.push(id);
        let awaiting = if replacing.is_some() { tokens.iter().cloned().collect() } else { HashSet::new() };
        self.conns.insert(
            id,
            ConnState {
                shard,
                replica,
                born: now,
                replacing,
                awaiting,
                retiring: false,
                up_since: None,
                win_events: 0,
                win_rate: 0.0,
                beh_sum: 0,
                beh_n: 0,
                speed: None,
                slow_windows: 0,
            },
        );
        fx.spawn_conn(id, replica, tokens);
        id
    }

    /// Times one delta event on `conn`: its `wire`, and how far behind the first socket to
    /// deliver the same (market, exch_ts) it came. Events are matched by exchange timestamp,
    /// which Polymarket stamps once and sends to every socket.
    fn note_arrival(&mut self, conn: ConnId, market: &Sym, exch: Ts, recv: Ts) -> Option<i64> {
        let replica = self.conns.get(&conn)?.replica;
        let bit = 1u64 << (replica % 64);
        let lat = self.lat.entry(conn).or_insert_with(|| SockLat { replica, ..Default::default() });
        lat.wire.record(recv - exch);
        if !self.arrivals.contains_key(market) {
            self.arrivals.insert(market.clone(), Arrivals::default());
        }
        let a = self.arrivals.get_mut(market).expect("just inserted");
        let behind = if let Some(e) = a.ring.iter_mut().find(|e| e.2 != 0 && e.0 == exch) {
            // A socket sends several events in one millisecond: only its first is timed.
            if e.2 & bit != 0 {
                return None;
            }
            e.2 |= bit;
            recv - e.1
        } else if a.ring.iter().all(|e| e.2 == 0 || e.0 < exch) {
            a.ring[a.next] = (exch, recv, bit);
            a.next = (a.next + 1) % ARRIVALS;
            lat.first += 1;
            0
        } else {
            lat.late += 1;
            return None;
        };
        lat.behind.record(behind);
        let cs = self.conns.get_mut(&conn).expect("looked up above");
        cs.beh_sum += behind;
        cs.beh_n += 1;
        Some(behind)
    }

    /// Ends a market: zero every level a reader holds for it, then unsubscribe and forget it.
    pub fn remove_market(&mut self, market: &str, recv: Ts, fx: &mut dyn Effects) {
        let Some(toks) = self.markets.remove(market) else { return };
        self.arrivals.remove(market);
        self.metrics.markets_removed += 1;
        let mut by_conn: HashMap<ConnId, Vec<Sym>> = HashMap::new();
        for t in &toks {
            let Some(mut tok) = self.tokens.remove(t) else { continue };
            Self::open_period(&mut tok, self.epoch, recv, &mut self.out, &mut self.metrics);
            for (i, side) in [Side::Bid, Side::Ask].into_iter().enumerate() {
                for &px in tok.emitted.side(side).keys() {
                    self.out.push(Rec::Update { sym: tok.syms[i].clone(), recv, exch: tok.emitted_exch, px, qty: 0 });
                    self.metrics.updates += 1;
                }
            }
            for c in tok.replicas.keys() {
                by_conn.entry(*c).or_default().push(t.clone());
            }
            let from = self.split.as_ref().filter(|s| s.to == tok.shard).map(|s| s.from);
            for s in [Some(tok.shard), from].into_iter().flatten() {
                self.shards[s].tokens.retain(|x| x != t);
            }
        }
        for (c, ts) in by_conn {
            fx.send(c, ConnCmd::Unsubscribe(ts));
        }
    }

    /// The venue closed `conn` because every token it carried resolved. Their
    /// `market_resolved` events may never come (both replicas are closed at the same moment),
    /// so end those markets here. A market listed in that same instant could be ended with
    /// them; the next discovery pass lists it again.
    pub fn on_resolved(&mut self, conn: ConnId, tokens: &[Sym], recv: Ts, fx: &mut dyn Effects) {
        self.metrics.resolved_closes += 1;
        let gone: HashSet<&Sym> = tokens.iter().collect();
        let markets: Vec<Sym> =
            self.markets.iter().filter(|(_, ts)| ts.iter().any(|t| gone.contains(t))).map(|(m, _)| m.clone()).collect();
        tracing::info!(conn, markets = markets.len(), "socket's assets all resolved; ending their markets");
        for m in &markets {
            self.remove_market(m, recv, fx);
        }
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
                        let sym = tok.syms[c.side as usize].clone();
                        self.out.push(Rec::Update { sym, recv, exch: *exch_ts, px: c.px, qty: c.qty });
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

    // ------------------------------------------------------------ timers

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

    // ------------------------------------------------------------ speed-aware source (Ext 4)

    /// Scores each socket by this window's mean `behind`, moves each shard's sources onto its
    /// fastest socket, and recycles a socket that stays far behind. See [`Fast`].
    fn check_speed(&mut self, now: Instant, recv: Ts, fx: &mut dyn Effects) {
        let Some(f) = self.cfg.fast.clone() else { return };
        for cs in self.conns.values_mut() {
            if cs.beh_n >= f.min_samples {
                let m = cs.beh_sum as f64 / cs.beh_n as f64 / 1000.0;
                cs.speed = Some(cs.speed.map_or(m, |s| (1.0 - f.weight) * s + f.weight * m));
            }
            (cs.beh_sum, cs.beh_n) = (0, 0);
        }
        let mut slow = Vec::new();
        for s in 0..self.shards.len() {
            // Only settled sockets are scored: not one mid-recycle, and not one still dumping.
            let mut scored: Vec<(f64, ConnId)> = self.shards[s]
                .conns
                .iter()
                .filter_map(|c| {
                    let cs = self.conns.get(c)?;
                    let settled = cs.up_since.is_some() && !cs.retiring && cs.replacing.is_none();
                    Some((cs.speed.filter(|_| settled)?, *c))
                })
                .collect();
            if scored.len() < 2 {
                continue;
            }
            scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let (best, worst) = (scored[0], scored[scored.len() - 1]);
            let held = self.shards[s].fast.and_then(|c| scored.iter().find(|x| x.1 == c)).filter(|x| x.0 <= best.0 + f.margin_ms);
            let pick = held.map_or(best.1, |x| x.1);
            if self.shards[s].fast != Some(pick) {
                tracing::info!(shard = s, conn = pick, behind_ms = format!("{:.1}", best.0), "preferring fastest socket");
                self.shards[s].fast = Some(pick);
                self.metrics.fast_picks += 1;
            }
            // Only onto a replica that holds the same book as the source, so the move writes
            // nothing. A standby is not hashed (D9): one that disagrees may be the one that is
            // wrong, and speed is no reason to trust it.
            for t in &self.shards[s].tokens {
                let Some(tok) = self.tokens.get_mut(t) else { continue };
                let Some(src) = tok.source.filter(|&c| c != pick) else { continue };
                let (Some(p), Some(o)) = (tok.replicas.get(&pick), tok.replicas.get(&src)) else { continue };
                if !p.valid {
                    continue;
                }
                if p.book != o.book {
                    self.metrics.fast_disagree += 1;
                    continue;
                }
                Self::adopt(tok, pick, self.epoch, recv, &mut self.out, &mut self.metrics);
                self.metrics.fast_switches += 1;
            }
            for &(score, c) in &scored {
                let cs = self.conns.get_mut(&c).expect("scored");
                if c == worst.1 && score - best.0 >= f.slow_ms {
                    cs.slow_windows += 1;
                    if cs.slow_windows >= f.slow_windows {
                        slow.push((c, s, cs.replica, score - best.0));
                    }
                } else {
                    cs.slow_windows = 0;
                }
            }
        }
        let mut busy: HashSet<usize> = self.conns.values().filter(|c| c.retiring).map(|c| c.shard).collect();
        for (c, shard, replica, by) in slow {
            let splitting = self.split.as_ref().is_some_and(|sp| sp.from == shard || sp.to == shard);
            if busy.len() >= self.cfg.recycle_parallel || splitting || !busy.insert(shard) {
                continue;
            }
            tracing::info!(conn = c, shard, behind_ms = format!("{by:.1}"), "socket stays slowest; recycling (make-before-break)");
            self.conns.get_mut(&c).expect("scored").slow_windows = 0;
            self.metrics.slow_recycles += 1;
            self.recycle(c, shard, replica, now, fx);
        }
    }

    // ------------------------------------------------------------ hot shard splits

    /// Moves the hottest markets of the hottest shard to a new shard when that shard carries
    /// `split_ratio` times the median shard's rate. Make-before-break, like recycling: the new
    /// sockets subscribe first and the old ones keep serving until every book has arrived.
    fn maybe_split(&mut self, now: Instant, fx: &mut dyn Effects) {
        let Some(ratio) = self.cfg.split_ratio else { return };
        if self.split.is_some() || (self.shards.len() + 1) * self.cfg.replicas > self.cfg.max_conns {
            return;
        }
        let mut rate = vec![0f32; self.shards.len()];
        for tok in self.tokens.values() {
            rate[tok.shard] += tok.rate;
        }
        let mut live: Vec<f32> = rate.iter().zip(&self.shards).filter(|(_, s)| !s.tokens.is_empty()).map(|(r, _)| *r).collect();
        if live.len() < 3 {
            return;
        }
        live.sort_unstable_by(f32::total_cmp);
        let median = live[live.len() / 2];
        let bar = (median * ratio).max(self.cfg.split_min_rate);
        let ready = |s: usize| {
            let conns = &self.shards[s].conns;
            conns.len() == self.cfg.replicas
                && conns.iter().filter_map(|c| self.conns.get(c)).all(|c| c.up_since.is_some() && !c.retiring && c.replacing.is_none())
        };
        let Some(from) = (0..self.shards.len()).filter(|&s| rate[s] > bar && ready(s)).max_by(|&a, &b| rate[a].total_cmp(&rate[b])) else {
            return;
        };
        let mut markets: Vec<(f32, &Vec<Sym>)> = self
            .markets
            .values()
            .filter(|ts| ts.first().and_then(|t| self.tokens.get(t)).is_some_and(|t| t.shard == from))
            .map(|ts| (ts.iter().filter_map(|t| self.tokens.get(t)).map(|t| t.rate).sum(), ts))
            .collect();
        if markets.len() < 2 {
            return; // one market is as small as a shard gets
        }
        markets.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
        // Hottest first until about half the rate has moved; at least one market stays.
        let (mut moved, mut moved_rate) = (Vec::new(), 0.0);
        for (r, ts) in &markets[..markets.len() - 1] {
            if moved_rate >= rate[from] / 2.0 {
                break;
            }
            moved_rate += r;
            moved.extend(ts.iter().cloned());
        }
        let to = self.shards.len();
        tracing::info!(shard = from, rate = rate[from], median, moved = moved.len(), moved_rate, "splitting hot shard");
        for t in &moved {
            self.tokens.get_mut(t).expect("listed above").shard = to;
        }
        self.shards.push(Shard { tokens: moved.clone(), conns: Vec::new(), closed: true, fast: None });
        for r in 0..self.cfg.replicas {
            let id = self.open_conn(to, r, now, None, fx);
            self.conns.get_mut(&id).expect("opened").awaiting = moved.iter().cloned().collect();
        }
        self.metrics.splits += 1;
        self.metrics.split_tokens += moved.len() as u64;
        self.split = Some(Split { from, to, moved, started: now });
    }

    fn maybe_finish_split(&mut self, recv: Ts, fx: &mut dyn Effects) {
        let Some(sp) = &self.split else { return };
        let conns = &self.shards[sp.to].conns;
        if conns.len() == self.cfg.replicas && conns.iter().filter_map(|c| self.conns.get(c)).all(|c| c.awaiting.is_empty()) {
            self.finish_split(recv, fx);
        }
    }

    /// The new sockets have every book (or the grace ran out): the old ones drop the moved tokens.
    fn finish_split(&mut self, recv: Ts, fx: &mut dyn Effects) {
        let Some(sp) = self.split.take() else { return };
        let old = self.shards[sp.from].conns.clone();
        let moved: HashSet<&Sym> = sp.moved.iter().collect();
        self.shards[sp.from].tokens.retain(|t| !moved.contains(t));
        let mut live = Vec::new();
        for t in &sp.moved {
            let Some(tok) = self.tokens.get_mut(t) else { continue }; // resolved meanwhile
            for c in &old {
                tok.replicas.remove(c);
            }
            if tok.source.is_some_and(|s| old.contains(&s)) {
                self.switch(t, recv);
            }
            live.push(t.clone());
        }
        for c in old {
            fx.send(c, ConnCmd::Unsubscribe(live.clone()));
        }
        for c in &self.shards[sp.to].conns {
            if let Some(cs) = self.conns.get_mut(c) {
                cs.awaiting.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests;
