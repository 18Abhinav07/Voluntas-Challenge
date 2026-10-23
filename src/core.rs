//! The single owner of all book state (§5.4–5.6): replica books per connection, the per-token
//! source arbiter, failure detection, shard planning and recycling. Synchronous and
//! deterministic: time comes in as arguments and every side effect goes through [`Effects`],
//! so the whole failover logic is unit-testable without sockets.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use crate::{
    adapter::{BookView, ConnId, Instrument, Integrity, NormEvent, Sym, Ts, Verdict},
    book::{Book, Side},
    conn::ConnCmd,
    writer::Rec,
};

/// Tokens found mid-discovery wait at most this long for a full shard before getting one.
const PENDING_MAX_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct Config {
    pub tokens_per_conn: usize,
    pub replicas: usize,
    /// A source that has not seen an event a peer saw this long ago is lagging.
    pub lag: Duration,
    pub snapshot_every: Duration,
    /// Make-before-break recycle connections older than this.
    pub max_conn_age: Option<Duration>,
    /// How long a recycle waits for the new connection's books before retiring the old one.
    pub recycle_grace: Duration,
    /// Minimum gap between two resnapshot requests for the same (conn, token).
    pub resnap_gap: Duration,
    /// How long a (re)subscribed connection gets to deliver its initial books before the
    /// missing ones are asked for again. A full-universe dump can take ~100 s to drain.
    pub book_timeout: Duration,
    /// Most steady-state connections (shards × replicas); tokens past it wait unplaced.
    pub max_conns: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tokens_per_conn: 2500,
            replicas: 2,
            lag: Duration::from_secs(3),
            snapshot_every: Duration::from_secs(60),
            max_conn_age: None,
            recycle_grace: Duration::from_secs(60),
            resnap_gap: Duration::from_secs(30),
            book_timeout: Duration::from_secs(120),
            max_conns: 200,
        }
    }
}

/// What the core asks the outside world to do.
pub trait Effects {
    /// Opens connection `id` (the `replica`-th of its shard) subscribed to `tokens`.
    fn spawn_conn(&mut self, id: ConnId, replica: usize, tokens: Vec<Sym>);
    fn send(&mut self, id: ConnId, cmd: ConnCmd);
    /// Resolves a market announced on the stream into instruments (see [`Core::add`]).
    fn lookup(&mut self, market: Sym);
}

/// Log2-bucketed latency histogram in µs; prints p50/p99/max in ms.
#[derive(Clone, Default)]
pub struct Hist {
    buckets: [u64; 32],
    n: u64,
    max: i64,
}

impl Hist {
    pub fn record(&mut self, us: i64) {
        let us = us.max(0);
        self.buckets[(64 - (us as u64).leading_zeros() as usize).min(31)] += 1;
        self.n += 1;
        self.max = self.max.max(us);
    }

    /// Upper bound of the bucket holding quantile `q`, in µs.
    pub fn quantile(&self, q: f64) -> i64 {
        let want = (self.n as f64 * q).ceil() as u64;
        let mut seen = 0;
        for (i, b) in self.buckets.iter().enumerate() {
            seen += b;
            if seen >= want.max(1) {
                return (1i64 << i).min(self.max);
            }
        }
        self.max
    }
}

impl std::fmt::Debug for Hist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ms = |us: i64| us as f64 / 1e3;
        write!(f, "n={} p50<={:.1}ms p99<={:.1}ms max={:.1}ms", self.n, ms(self.quantile(0.5)), ms(self.quantile(0.99)), ms(self.max))
    }
}

#[derive(Debug, Default, Clone)]
pub struct Metrics {
    pub updates: u64,
    pub snapshots: u64,
    pub switches: u64,
    pub lag_switches: u64,
    pub hash_ok: u64,
    pub hash_miss: u64,
    /// Misses already superseded by a newer `book` on that replica.
    pub hash_healed: u64,
    /// Misses where a peer at the same event position holds a different book.
    pub divergent: u64,
    /// Misses where a peer at the same event position holds the identical book: two
    /// independent sockets agree, so the miss is the checker's ambiguity, not lost data.
    pub hash_agreed: u64,
    pub resnapshots: u64,
    pub book_miss: u64,
    /// Mid-stream `book`s on the source that differed from the local book (levels emitted).
    pub book_diffs: u64,
    pub disconnects: u64,
    pub recycles: u64,
    pub markets_added: u64,
    pub markets_removed: u64,
    /// Tokens waiting because the connection cap is reached.
    pub unplaced: u64,
    /// `recv - exch` per delta event per replica: venue to socket read.
    pub wire: Hist,
    /// Socket read to core apply: time spent queued inside this process.
    pub queue: Hist,
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
}

struct Shard {
    tokens: Vec<Sym>,
    conns: Vec<ConnId>,
}

struct ConnState {
    shard: usize,
    replica: usize,
    born: Instant,
    /// Set while this connection is the new half of a make-before-break recycle.
    replacing: Option<ConnId>,
    awaiting: HashSet<Sym>,
    retiring: bool,
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
    tick_no: u64,
    recycle_started: Option<Instant>,
    verdicts: Vec<Verdict>,
    pub out: Vec<Rec>,
    pub metrics: Metrics,
}

struct View<'a> {
    tokens: &'a HashMap<Sym, Tok>,
    conn: ConnId,
}

impl BookView for View<'_> {
    fn book(&self, token: &str) -> Option<&Book> {
        self.tokens.get(token)?.replicas.get(&self.conn).filter(|r| r.valid).map(|r| &r.book)
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
            cfg,
            integrity,
            tokens: HashMap::new(),
            markets: HashMap::new(),
            shards: Vec::new(),
            pending: Vec::new(),
            pending_since: None,
            conns: HashMap::new(),
            next_conn: 0,
            looking_up: HashSet::new(),
            buckets: vec![Vec::new(); n],
            tick_no: 0,
            recycle_started: None,
            verdicts: Vec::new(),
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
            let tail = self.shards.len().checked_sub(1).filter(|&s| {
                self.pending.is_empty() && self.shards[s].tokens.len() + inst.tokens.len() <= self.cfg.tokens_per_conn
            });
            let shard = tail.unwrap_or(self.shards.len());
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
                    },
                );
                let b = bucket_of(t, self.buckets.len());
                self.buckets[b].push(t.clone());
            }
            self.markets.insert(inst.market.clone(), inst.tokens.clone());
            if let Some(s) = tail {
                let now = Instant::now();
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
        self.shards.push(Shard { tokens: tokens.clone(), conns: Vec::new() });
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
        self.conns.insert(id, ConnState { shard, replica, born: now, replacing, awaiting, retiring: false });
        fx.spawn_conn(id, replica, tokens);
        id
    }

    /// Ends a market: zero every level a reader holds for it, then unsubscribe and forget it.
    pub fn remove_market(&mut self, market: &str, recv: Ts, fx: &mut dyn Effects) {
        let Some(toks) = self.markets.remove(market) else { return };
        self.metrics.markets_removed += 1;
        let mut by_conn: HashMap<ConnId, Vec<Sym>> = HashMap::new();
        for t in &toks {
            let Some(tok) = self.tokens.remove(t) else { continue };
            for (i, side) in [Side::Bid, Side::Ask].into_iter().enumerate() {
                for &px in tok.emitted.side(side).keys() {
                    self.out.push(Rec::Update { sym: tok.syms[i].clone(), recv, exch: tok.emitted_exch, px, qty: 0 });
                    self.metrics.updates += 1;
                }
            }
            for c in tok.replicas.keys() {
                by_conn.entry(*c).or_default().push(t.clone());
            }
            if let Some(s) = self.shards.get_mut(tok.shard) {
                s.tokens.retain(|x| x != t);
            }
        }
        for (c, ts) in by_conn {
            fx.send(c, ConnCmd::Unsubscribe(ts));
        }
    }

    // ------------------------------------------------------------ connection events

    /// The socket is (re)subscribed to every token it owns; their books are on the way.
    pub fn on_up(&mut self, conn: ConnId, now: Instant) {
        tracing::debug!(conn, "up");
        let Some(shard) = self.conns.get(&conn).map(|c| c.shard) else { return };
        let due = now + self.cfg.book_timeout;
        for t in &self.shards[shard].tokens {
            if let Some(r) = self.tokens.get_mut(t).and_then(|t| t.replicas.get_mut(&conn))
                && !r.valid
            {
                r.book_due = Some(due);
            }
        }
    }

    pub fn on_down(&mut self, conn: ConnId, recv: Ts) {
        self.metrics.disconnects += 1;
        self.integrity.forget_conn(conn);
        let Some(shard) = self.conns.get(&conn).map(|c| c.shard) else { return };
        for t in self.shards[shard].tokens.clone() {
            let Some(tok) = self.tokens.get_mut(&t) else { continue };
            if let Some(r) = tok.replicas.get_mut(&conn) {
                r.valid = false;
                r.book = Book::default();
                r.book_due = None; // set again when the socket is back up and resubscribed
            }
            if tok.source == Some(conn) {
                self.switch(&t, recv);
            }
        }
    }

    /// The connection's actor exited; it is no longer a replica of anything.
    pub fn on_gone(&mut self, conn: ConnId, recv: Ts) {
        let Some(st) = self.conns.remove(&conn) else { return };
        self.integrity.forget_conn(conn);
        if let Some(s) = self.shards.get_mut(st.shard) {
            s.conns.retain(|&c| c != conn);
            for t in s.tokens.clone() {
                let Some(tok) = self.tokens.get_mut(&t) else { continue };
                tok.replicas.remove(&conn);
                if tok.source == Some(conn) {
                    self.switch(&t, recv);
                }
            }
        }
    }

    pub fn on_events(&mut self, conn: ConnId, recv: Ts, events: &[NormEvent], now: Instant, fx: &mut dyn Effects) {
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
                    if Self::emit_diff(tok, conn, recv, &mut self.out, &mut self.metrics) {
                        self.metrics.book_diffs += 1;
                    }
                } else if tok.source.is_none() {
                    Self::adopt(tok, conn, recv, &mut self.out, &mut self.metrics);
                }
                if let Some(cs) = self.conns.get_mut(&conn) {
                    cs.awaiting.remove(token);
                }
                self.maybe_finish_recycle(conn, recv, fx);
            }
            NormEvent::Levels { exch_ts, changes, .. } => {
                self.metrics.wire.record(recv - exch_ts);
                for c in changes {
                    let Some(tok) = self.tokens.get_mut(&c.token) else { continue };
                    let Some(rep) = tok.replicas.get_mut(&conn) else { continue };
                    if !rep.valid {
                        continue;
                    }
                    rep.book.apply(c.side, c.px, c.qty);
                    rep.last_exch = rep.last_exch.max(*exch_ts);
                    if *exch_ts > rep.last_delta {
                        rep.last_delta = *exch_ts;
                        rep.adv_at = now;
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

    /// Writes both sides of what readers hold.
    fn snapshot(tok: &Tok, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) {
        out.push(Rec::Snapshot {
            sym: tok.syms[0].clone(),
            recv,
            exch: tok.emitted_exch,
            side: Side::Bid,
            levels: tok.emitted.bids.iter().rev().map(|(&p, &q)| (p, q)).collect(),
        });
        out.push(Rec::Snapshot {
            sym: tok.syms[1].clone(),
            recv,
            exch: tok.emitted_exch,
            side: Side::Ask,
            levels: tok.emitted.asks.iter().map(|(&p, &q)| (p, q)).collect(),
        });
        m.snapshots += 2;
    }

    /// Emits the level changes that take readers from `emitted` to `conn`'s book.
    fn emit_diff(tok: &mut Tok, conn: ConnId, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) -> bool {
        let rep = &tok.replicas[&conn];
        let diff = tok.emitted.diff(&rep.book);
        for &(side, px, qty) in &diff {
            out.push(Rec::Update { sym: tok.syms[side as usize].clone(), recv, exch: rep.last_exch, px, qty });
        }
        m.updates += diff.len() as u64;
        tok.emitted.clone_from(&rep.book);
        tok.emitted_exch = rep.last_exch;
        !diff.is_empty()
    }

    /// Makes `conn` the token's source: the first time with a snapshot, afterwards with the
    /// diff from what readers already hold, so the output stream never jumps.
    fn adopt(tok: &mut Tok, conn: ConnId, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) {
        tok.source = Some(conn);
        if tok.announced {
            Self::emit_diff(tok, conn, recv, out, m);
        } else {
            let rep = &tok.replicas[&conn];
            tok.emitted.clone_from(&rep.book);
            tok.emitted_exch = rep.last_exch;
            tok.announced = true;
            Self::snapshot(tok, recv, out, m);
        }
    }

    /// Moves the token's source to its most advanced valid replica. With none valid the token
    /// goes dark until the next `book` (never emit deltas against an untrusted book).
    fn switch(&mut self, token: &Sym, recv: Ts) {
        let Some(tok) = self.tokens.get_mut(token) else { return };
        let old = tok.source.take();
        let best = tok
            .replicas
            .iter()
            .filter(|(c, r)| r.valid && Some(**c) != old)
            .max_by_key(|(c, r)| (r.seen(), std::cmp::Reverse(**c)))
            .map(|(c, _)| *c);
        if let Some(c) = best {
            Self::adopt(tok, c, recv, &mut self.out, &mut self.metrics);
            self.metrics.switches += 1;
        }
    }

    fn resnapshot(&mut self, conn: ConnId, token: &Sym, now: Instant, fx: &mut dyn Effects) {
        let gap = self.cfg.resnap_gap;
        let Some(rep) = self.tokens.get_mut(token).and_then(|t| t.replicas.get_mut(&conn)) else { return };
        if rep.book_due.is_some_and(|due| now < due) {
            return;
        }
        rep.book_due = Some(now + gap);
        self.metrics.resnapshots += 1;
        fx.send(conn, ConnCmd::Resnapshot(vec![token.clone()]));
    }

    // ------------------------------------------------------------ integrity ladder (§5.5)

    fn handle_verdicts(&mut self, recv: Ts, now: Instant, fx: &mut dyn Effects) {
        for v in std::mem::take(&mut self.verdicts) {
            match v {
                Verdict::Ok { conn, token, exch_ts } => {
                    self.metrics.hash_ok += 1;
                    if let Some(r) = self.tokens.get_mut(&token).and_then(|t| t.replicas.get_mut(&conn)) {
                        r.last_ok_ts = r.last_ok_ts.max(exch_ts);
                        r.strikes = 0;
                    }
                }
                Verdict::BookMiss { .. } => self.metrics.book_miss += 1,
                Verdict::Miss { conn, token, exch_ts } => {
                    let Some(tok) = self.tokens.get_mut(&token) else { continue };
                    let Some(rep) = tok.replicas.get_mut(&conn) else { continue };
                    if !rep.valid || rep.last_book_ts >= exch_ts {
                        self.metrics.hash_healed += 1;
                        continue;
                    }
                    self.metrics.hash_miss += 1;
                    rep.strikes = rep.strikes.saturating_add(1);
                    let (strikes, rep) = (rep.strikes, &tok.replicas[&conn]);
                    // The checker has a known ~1% false-miss rate (§5.5), so one miss alone is
                    // weak evidence. A peer at the same event position is the second opinion.
                    let peers = || tok.replicas.iter().filter(|(c, r)| **c != conn && r.valid && r.last_exch == rep.last_exch);
                    let corroborated = peers().any(|(_, r)| r.book != rep.book);
                    if !corroborated && peers().any(|(_, r)| r.book == rep.book) {
                        self.metrics.hash_agreed += 1;
                    }
                    if corroborated {
                        self.metrics.divergent += 1;
                    }
                    if corroborated || strikes >= 2 {
                        self.refresh(conn, &token, recv, now, fx);
                    }
                }
            }
        }
    }

    /// Distrusts one replica of a token until a fresh `book` arrives: moves the source off it
    /// first, so the resnapshot gap never reaches the output.
    fn refresh(&mut self, conn: ConnId, token: &Sym, recv: Ts, now: Instant, fx: &mut dyn Effects) {
        let Some(tok) = self.tokens.get_mut(token) else { return };
        if let Some(r) = tok.replicas.get_mut(&conn) {
            r.valid = false;
            r.strikes = 0;
        }
        if tok.source == Some(conn) {
            self.switch(token, recv);
        }
        self.resnapshot(conn, token, now, fx);
    }

    /// Re-asks for books that a resnapshot requested but never delivered.
    /// Re-asks every book that is overdue: lost resnapshots, and initial books a dump never
    /// delivered. One command per connection, however many tokens it covers.
    fn retry_resnapshots(&mut self, now: Instant, fx: &mut dyn Effects) {
        let gap = self.cfg.resnap_gap;
        let mut by_conn: BTreeMap<ConnId, Vec<Sym>> = BTreeMap::new();
        for (t, tok) in self.tokens.iter_mut() {
            for (c, r) in tok.replicas.iter_mut() {
                if !r.valid && r.book_due.is_some_and(|due| now >= due) {
                    r.book_due = Some(now + gap);
                    by_conn.entry(*c).or_default().push(t.clone());
                }
            }
        }
        for (c, ts) in by_conn {
            self.metrics.resnapshots += ts.len() as u64;
            fx.send(c, ConnCmd::Resnapshot(ts));
        }
    }

    // ------------------------------------------------------------ timers

    /// Call once per second.
    pub fn on_tick(&mut self, now: Instant, recv: Ts, fx: &mut dyn Effects) {
        self.integrity.on_tick(now, &mut self.verdicts);
        self.handle_verdicts(recv, now, fx);
        self.check_lag(now, recv, fx);
        if self.tick_no.is_multiple_of(10) {
            self.retry_resnapshots(now, fx);
        }

        let b = (self.tick_no % self.buckets.len() as u64) as usize;
        self.tick_no += 1;
        let tokens = &self.tokens;
        self.buckets[b].retain(|t| tokens.contains_key(t));
        for t in &self.buckets[b] {
            let tok = &self.tokens[t];
            if tok.source.is_some() {
                Self::snapshot(tok, recv, &mut self.out, &mut self.metrics);
            }
        }
        if self.pending_since.is_some_and(|t| now.duration_since(t) >= PENDING_MAX_WAIT) {
            self.flush_pending(now, fx);
        }
        self.maybe_start_recycle(now, fx);
        if self.recycle_started.is_some_and(|t| now.duration_since(t) >= self.cfg.recycle_grace) {
            let ids: Vec<ConnId> = self.conns.iter().filter(|(_, c)| c.replacing.is_some()).map(|(id, _)| *id).collect();
            for id in ids {
                self.finish_recycle(id, recv, fx);
            }
        }
    }

    /// Silent-subscription detector: the source has not seen an event a peer saw `lag` ago.
    fn check_lag(&mut self, now: Instant, recv: Ts, fx: &mut dyn Effects) {
        let lag = self.cfg.lag;
        let mut lagging = Vec::new();
        for (t, tok) in &self.tokens {
            let Some(src) = tok.source.and_then(|s| tok.replicas.get(&s).map(|r| (s, r))) else { continue };
            // Only a peer's delta proves the source missed something; see `Replica::last_delta`.
            let behind = tok
                .replicas
                .iter()
                .any(|(c, r)| *c != src.0 && r.valid && r.last_delta > src.1.seen() && now.duration_since(r.adv_at) >= lag);
            if behind {
                lagging.push((t.clone(), src.0));
            }
        }
        for (t, c) in lagging {
            tracing::info!(conn = c, token = %t, "source lagging a peer; switching");
            self.metrics.lag_switches += 1;
            self.refresh(c, &t, recv, now, fx);
        }
    }

    // ------------------------------------------------------------ recycling (Ext 2)

    fn maybe_start_recycle(&mut self, now: Instant, fx: &mut dyn Effects) {
        let Some(age) = self.cfg.max_conn_age else { return };
        if self.recycle_started.is_some() {
            return;
        }
        let Some((&old, st)) = self
            .conns
            .iter()
            .filter(|(_, c)| !c.retiring && c.replacing.is_none() && now.duration_since(c.born) >= age)
            .min_by_key(|(id, c)| (c.born, **id))
        else {
            return;
        };
        let (shard, replica) = (st.shard, st.replica);
        tracing::info!(conn = old, shard, "recycling connection (make-before-break)");
        self.conns.get_mut(&old).expect("listed").retiring = true;
        self.recycle_started = Some(now);
        self.open_conn(shard, replica, now, Some(old), fx);
    }

    fn maybe_finish_recycle(&mut self, conn: ConnId, recv: Ts, fx: &mut dyn Effects) {
        if self.conns.get(&conn).is_some_and(|c| c.replacing.is_some() && c.awaiting.is_empty()) {
            self.finish_recycle(conn, recv, fx);
        }
    }

    fn finish_recycle(&mut self, new: ConnId, recv: Ts, fx: &mut dyn Effects) {
        let Some(old) = self.conns.get_mut(&new).and_then(|c| c.replacing.take()) else { return };
        self.conns.get_mut(&new).expect("exists").awaiting.clear();
        self.recycle_started = None;
        self.metrics.recycles += 1;
        fx.send(old, ConnCmd::Close);
        // Hand over sources now rather than waiting for the old actor to exit.
        self.on_gone(old, recv);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::LevelChange;

    #[derive(Default)]
    struct Fx {
        spawned: Vec<(ConnId, usize, Vec<Sym>)>,
        sent: Vec<(ConnId, String)>,
        lookups: Vec<Sym>,
    }

    impl Effects for Fx {
        fn spawn_conn(&mut self, id: ConnId, replica: usize, tokens: Vec<Sym>) {
            self.spawned.push((id, replica, tokens));
        }
        fn send(&mut self, id: ConnId, cmd: ConnCmd) {
            self.sent.push((id, format!("{cmd:?}")));
        }
        fn lookup(&mut self, market: Sym) {
            self.lookups.push(market);
        }
    }

    struct NoCheck;
    impl Integrity<()> for NoCheck {
        fn register(&mut self, _: &Instrument<()>) {}
        fn pre_event(&mut self, _: ConnId, _: &NormEvent, _: &dyn BookView, _: Instant, _: &mut Vec<Verdict>) {}
        fn on_event(&mut self, _: ConnId, _: &NormEvent, _: &dyn BookView, _: Instant, _: &mut Vec<Verdict>) {}
        fn on_tick(&mut self, _: Instant, _: &mut Vec<Verdict>) {}
        fn forget_conn(&mut self, _: ConnId) {}
    }

    fn inst(m: &str, toks: &[&str]) -> Instrument<()> {
        Instrument { market: m.into(), tokens: toks.iter().map(|&t| t.into()).collect(), meta: () }
    }

    fn book(t: &str, ts: Ts, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> NormEvent {
        NormEvent::Book {
            token: t.into(),
            market: "m".into(),
            bids: bids.to_vec(),
            asks: asks.to_vec(),
            exch_ts: ts,
            check: None,
            tick: None,
            last_trade: None,
        }
    }

    fn lvl(t: &str, ts: Ts, side: Side, px: i64, qty: i64) -> NormEvent {
        NormEvent::Levels {
            market: "m".into(),
            exch_ts: ts,
            changes: vec![LevelChange { token: t.into(), side, px, qty, check: None }],
        }
    }

    fn setup() -> (Core<()>, Fx, Instant) {
        let mut core = Core::new(Config { tokens_per_conn: 10, ..Config::default() }, Box::new(NoCheck));
        let mut fx = Fx::default();
        let now = Instant::now();
        core.add(vec![inst("m", &["a", "b"])], &mut fx);
        core.flush_pending(now, &mut fx);
        (core, fx, now)
    }

    #[test]
    fn initial_book_that_never_comes_is_asked_again() {
        let (mut core, mut fx, now) = setup();
        core.on_up(0, now);
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.retry_resnapshots(now + Duration::from_secs(60), &mut fx);
        assert_eq!(resnaps(&fx), 0, "dump still within its timeout");

        core.retry_resnapshots(now + core.cfg.book_timeout, &mut fx);
        assert_eq!(fx.sent, vec![(0, r#"Resnapshot(["b"])"#.to_string())], "only the missing book, one command");

        core.retry_resnapshots(now + core.cfg.book_timeout + Duration::from_secs(1), &mut fx);
        assert_eq!(resnaps(&fx), 1, "debounced by resnap_gap");
    }

    #[test]
    fn socket_that_never_came_up_is_not_asked() {
        let (mut core, mut fx, now) = setup();
        core.retry_resnapshots(now + Duration::from_secs(600), &mut fx);
        assert_eq!(resnaps(&fx), 0, "a socket still waiting to open gets its dump on subscribe");

        core.on_up(1, now);
        core.on_down(1, 1);
        core.retry_resnapshots(now + Duration::from_secs(600), &mut fx);
        assert_eq!(resnaps(&fx), 0, "a dropped socket resubscribes everything when it is back");
    }

    #[test]
    fn connection_cap_leaves_tokens_unplaced() {
        let mut core = Core::new(Config { tokens_per_conn: 2, max_conns: 2, ..Config::default() }, Box::new(NoCheck));
        let mut fx = Fx::default();
        core.add(vec![inst("m", &["a", "b"]), inst("n", &["c", "d"])], &mut fx);
        core.flush_pending(Instant::now(), &mut fx);
        assert_eq!((fx.spawned.len(), core.metrics.unplaced), (2, 2), "one shard × two replicas, then capped");
    }

    #[test]
    fn plans_one_shard_with_two_replicas() {
        let (core, fx, _) = setup();
        assert_eq!(fx.spawned.len(), 2);
        assert_eq!(fx.spawned[0].2, fx.spawned[1].2);
        assert_eq!(core.conn_count(), 2);
    }

    #[test]
    fn first_book_snapshots_then_only_source_emits() {
        let (mut core, mut fx, now) = setup();
        core.on_events(1, 10, &[book("a", 5, &[(400, 1)], &[(600, 2)])], now, &mut fx);
        assert!(matches!(&core.out[..], [Rec::Snapshot { exch: 5, .. }, Rec::Snapshot { .. }]));
        core.out.clear();
        core.on_events(0, 11, &[book("a", 5, &[(400, 1)], &[(600, 2)])], now, &mut fx);
        assert!(core.out.is_empty(), "non-source book is silent");
        core.on_events(0, 12, &[lvl("a", 6, Side::Bid, 410, 3)], now, &mut fx);
        assert!(core.out.is_empty(), "non-source level is silent");
        core.on_events(1, 13, &[lvl("a", 6, Side::Bid, 410, 3)], now, &mut fx);
        assert_eq!(core.out, vec![Rec::Update { sym: "a:bid".into(), recv: 13, exch: 6, px: 410, qty: 3 }]);
    }

    #[test]
    fn disconnect_fails_over_with_snapshot_from_peer() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 2, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 3, &[lvl("a", 7, Side::Ask, 600, 9)], now, &mut fx);
        core.out.clear();
        core.on_down(0, 4);
        assert_eq!(core.out, vec![Rec::Update { sym: "a:ask".into(), recv: 4, exch: 7, px: 600, qty: 9 }], "peer's extra level, as a diff");
        assert_eq!(core.metrics.switches, 1);
    }

    #[test]
    fn no_valid_replica_goes_dark_until_book() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_down(0, 2);
        core.out.clear();
        core.on_events(0, 3, &[lvl("a", 6, Side::Bid, 400, 0)], now, &mut fx);
        assert!(core.out.is_empty(), "no deltas against an untrusted book");
        core.on_events(1, 4, &[book("a", 6, &[], &[])], now, &mut fx);
        assert_eq!(core.out, vec![Rec::Update { sym: "a:bid".into(), recv: 4, exch: 6, px: 400, qty: 0 }]);
    }

    #[test]
    fn mid_stream_book_on_source_emits_diff() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1), (300, 1)], &[])], now, &mut fx);
        core.out.clear();
        core.on_events(0, 2, &[book("a", 9, &[(400, 2)], &[])], now, &mut fx);
        let mut got = core.out.clone();
        got.sort_by_key(|r| format!("{r:?}"));
        assert_eq!(
            got,
            vec![
                Rec::Update { sym: "a:bid".into(), recv: 2, exch: 9, px: 300, qty: 0 },
                Rec::Update { sym: "a:bid".into(), recv: 2, exch: 9, px: 400, qty: 2 },
            ]
        );
        assert_eq!(core.metrics.book_diffs, 1);
    }

    #[test]
    fn lagging_source_switches_and_resnapshots() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 2, &[lvl("a", 6, Side::Bid, 400, 2)], now, &mut fx);
        core.out.clear();
        core.on_tick(now + Duration::from_secs(1), 3, &mut fx);
        assert_eq!(core.metrics.lag_switches, 0, "within the lag window");
        core.on_tick(now + Duration::from_secs(4), 4, &mut fx);
        assert_eq!(core.metrics.lag_switches, 1);
        assert!(fx.sent.iter().any(|(c, s)| *c == 0 && s.starts_with("Resnapshot")));
        assert_eq!(core.out, vec![Rec::Update { sym: "a:bid".into(), recv: 4, exch: 6, px: 400, qty: 2 }]);
    }

    /// Both replicas hold the same book at ts 5; the source (conn 0) then sees one more delta.
    fn ahead_by_one() -> (Core<()>, Fx, Instant) {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(0, 2, &[lvl("a", 6, Side::Bid, 410, 1)], now, &mut fx);
        (core, fx, now)
    }

    fn miss(core: &mut Core<()>, fx: &mut Fx, now: Instant, conn: ConnId) {
        core.verdicts.push(Verdict::Miss { conn, token: "a".into(), exch_ts: 6 });
        core.handle_verdicts(3, now, fx);
    }

    fn resnaps(fx: &Fx) -> usize {
        fx.sent.iter().filter(|(_, s)| s.starts_with("Resnapshot")).count()
    }

    #[test]
    fn single_uncorroborated_miss_is_tolerated() {
        let (mut core, mut fx, now) = ahead_by_one();
        miss(&mut core, &mut fx, now, 0);
        assert_eq!((core.metrics.hash_miss, resnaps(&fx), core.metrics.switches), (1, 0, 0));
        miss(&mut core, &mut fx, now, 0);
        assert_eq!(resnaps(&fx), 1, "second strike refreshes");
        assert_eq!(core.metrics.switches, 1, "source moves off before the gap opens");
    }

    #[test]
    fn verified_match_clears_strikes() {
        let (mut core, mut fx, now) = ahead_by_one();
        miss(&mut core, &mut fx, now, 0);
        core.verdicts.push(Verdict::Ok { conn: 0, token: "a".into(), exch_ts: 6 });
        miss(&mut core, &mut fx, now, 0);
        assert_eq!(resnaps(&fx), 0);
    }

    #[test]
    fn miss_corroborated_by_peer_refreshes_at_once() {
        let (mut core, mut fx, now) = ahead_by_one();
        core.on_events(1, 2, &[lvl("a", 6, Side::Bid, 420, 1)], now, &mut fx);
        miss(&mut core, &mut fx, now, 0);
        assert_eq!((core.metrics.divergent, resnaps(&fx)), (1, 1));
    }

    #[test]
    fn miss_with_agreeing_peer_counts_as_agreed() {
        let (mut core, mut fx, now) = ahead_by_one();
        core.on_events(1, 2, &[lvl("a", 6, Side::Bid, 410, 1)], now, &mut fx);
        miss(&mut core, &mut fx, now, 0);
        assert_eq!((core.metrics.hash_agreed, core.metrics.divergent, resnaps(&fx)), (1, 0, 0));
    }

    #[test]
    fn fresh_peer_book_is_not_lag() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 2, &[book("a", 50, &[(400, 1)], &[])], now, &mut fx);
        core.on_tick(now + Duration::from_secs(10), 3, &mut fx);
        assert_eq!(core.metrics.lag_switches, 0, "a dump's timestamp is not evidence of missed deltas");
    }

    #[test]
    fn resolved_market_zeroes_levels_and_unsubscribes() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[(600, 1)])], now, &mut fx);
        core.out.clear();
        core.on_events(0, 2, &[NormEvent::Resolved { market: "m".into() }], now, &mut fx);
        assert_eq!(core.out.len(), 2);
        assert!(core.out.iter().all(|r| matches!(r, Rec::Update { qty: 0, .. })));
        assert_eq!(core.token_count(), 0);
        assert_eq!(fx.sent.iter().filter(|(_, s)| s.starts_with("Unsubscribe")).count(), 2);
    }

    #[test]
    fn listed_market_is_looked_up_once() {
        let (mut core, mut fx, now) = setup();
        let ev = NormEvent::Listed { market: "n".into() };
        core.on_events(0, 1, std::slice::from_ref(&ev), now, &mut fx);
        core.on_events(1, 1, std::slice::from_ref(&ev), now, &mut fx);
        assert_eq!(fx.lookups.len(), 1);
        core.add(vec![inst("n", &["c", "d"])], &mut fx);
        assert_eq!(fx.sent.iter().filter(|(_, s)| s.starts_with("Subscribe")).count(), 2, "joins the tail shard");
    }

    #[test]
    fn snapshots_every_period_per_token() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(0, 1, &[book("b", 5, &[(400, 1)], &[])], now, &mut fx);
        core.out.clear();
        for s in 0..60 {
            core.on_tick(now + Duration::from_secs(s), s as i64, &mut fx);
        }
        assert_eq!(core.out.iter().filter(|r| matches!(r, Rec::Snapshot { .. })).count(), 4);
    }

    #[test]
    fn partial_shard_opens_after_wait() {
        let (mut core, mut fx, now) = setup();
        core.add(vec![inst("x", &["p", "q", "r", "s", "t", "u", "v", "w", "y"])], &mut fx);
        assert_eq!(fx.spawned.len(), 2, "tail shard full: waits for more tokens");
        core.on_tick(now + PENDING_MAX_WAIT * 2, 1, &mut fx);
        assert_eq!(fx.spawned.len(), 4);
    }

    #[test]
    fn recycle_is_make_before_break() {
        let mut core = Core::new(
            Config { tokens_per_conn: 10, max_conn_age: Some(Duration::from_secs(10)), ..Config::default() },
            Box::new(NoCheck),
        );
        let mut fx = Fx::default();
        let now = Instant::now();
        core.add(vec![inst("m", &["a"])], &mut fx);
        core.flush_pending(now, &mut fx);
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_tick(now + Duration::from_secs(11), 2, &mut fx);
        assert_eq!(fx.spawned.len(), 3, "new connection opened first");
        assert!(!fx.sent.iter().any(|(_, s)| s == "Close"), "old one still open");
        core.out.clear();
        core.on_events(2, 3, &[book("a", 6, &[(400, 1)], &[])], now, &mut fx);
        assert!(fx.sent.iter().any(|(c, s)| *c == 0 && s == "Close"));
        assert_eq!(core.metrics.recycles, 1);
        assert!(core.out.is_empty(), "identical books: handover is invisible in the output");
    }
}
