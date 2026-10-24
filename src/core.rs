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

/// Shard rates are smoothed per minute; splits wait until they mean something.
const SPLIT_WARMUP_TICKS: u64 = 180;

/// Rotation snapshots are spread over at most this many ticks (half the period if shorter).
const ROTATE_SPREAD_SECS: u64 = 10;

#[derive(Clone, Debug)]
pub struct Config {
    pub tokens_per_conn: usize,
    pub replicas: usize,
    /// A source that has not seen an event a peer saw this long ago is lagging.
    pub lag: Duration,
    pub snapshot_every: Duration,
    /// Cut the output into wall-clock periods of this length (see `writer`).
    pub rotate_every: Option<Duration>,
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
    /// At most this many make-before-break recycles in flight, never two in one shard.
    pub recycle_parallel: usize,
    /// First connection id this core hands out; cores on other threads use disjoint ranges.
    pub conn_id_base: ConnId,
    pub quiet: Quiet,
    /// Split a shard whose message rate is this many times the median shard's (`None` = never).
    /// Shards are planned by token count, but a few markets carry most of the traffic, and the
    /// venue closes a socket that falls behind it ("slow consumer"). Rates are measured, not
    /// configured: the hottest markets move to a new shard, make-before-break.
    pub split_ratio: Option<f32>,
    /// ...and at least this many new deltas a minute (smoothed), so idle universes never split.
    pub split_min_rate: f32,
    /// Keep per-socket latency ([`SockLat`]) and the arrival windows it needs (Ext 4).
    pub lat_detail: bool,
    /// Prefer each shard's fastest socket as source, and recycle one that stays slowest.
    /// Needs the arrival windows, so it keeps them whether or not `lat_detail` is set.
    pub fast: Option<Fast>,
}

/// Speed-aware source choice (Ext 4). Every `quiet.conn_window` each socket's mean `behind`
/// (how long after the shard's first copy it delivers an event) is folded into a smoothed
/// score. The best-scored socket of a shard becomes its preferred source, and keeps the role
/// until another beats it by `margin_ms`. A socket scored worst of its shard and `slow_ms`
/// behind the best for `slow_windows` windows in a row is recycled make-before-break: on the
/// measured run speed stuck to the connection, not to the edge.
#[derive(Clone, Debug)]
pub struct Fast {
    pub margin_ms: f64,
    /// Weight of the newest 30 s window in a socket's score; 0.25 remembers about 4 windows.
    pub weight: f64,
    pub slow_ms: f64,
    pub slow_windows: u32,
    /// A window with fewer timed events than this leaves a socket's score as it was.
    pub min_samples: u64,
}

impl Default for Fast {
    fn default() -> Self {
        Self { margin_ms: 3.0, weight: 0.25, slow_ms: 4.0, slow_windows: 10, min_samples: 50 }
    }
}

/// Ext 2 thresholds. A token is quiet when no replica has seen a delta for
/// `factor` expected gaps (from its smoothed per-minute rate), clamped to `floor..=ceiling`
/// and doubled per strike. Tokens slower than `min_rate` are left to periodic recycling:
/// their silence is indistinguishable from a dead feed.
///
/// Every resubscribe is a strike. Only a resubscribe whose book corrects what readers hold
/// (a real miss) clears them; deltas do not, because bursty markets fall silent for longer
/// than their average gap all the time. Strikes step down once a minute while the token is
/// active and its last resubscribe is older than `ceiling`. The first soak resubscribed
/// 400k times in 90 minutes with deltas clearing strikes.
#[derive(Clone, Debug)]
pub struct Quiet {
    pub min_rate: f32,
    pub factor: f32,
    pub floor: Duration,
    pub ceiling: Duration,
    /// Most tokens resubscribed per check; a venue-wide lull must not become a storm.
    pub max_per_round: usize,
    /// A socket that delivers no delta in a window while a peer of its shard delivers at
    /// least `conn_min` (or while its own smoothed rate is 4× that) is reconnected.
    pub conn_window: Duration,
    pub conn_min: u64,
}

impl Default for Quiet {
    fn default() -> Self {
        Self {
            min_rate: 1.0,
            factor: 20.0,
            floor: Duration::from_secs(60),
            ceiling: Duration::from_secs(30 * 60),
            max_per_round: 25,
            conn_window: Duration::from_secs(30),
            conn_min: 30,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tokens_per_conn: 2500,
            replicas: 2,
            lag: Duration::from_secs(3),
            snapshot_every: Duration::from_secs(60),
            rotate_every: None,
            max_conn_age: None,
            recycle_grace: Duration::from_secs(60),
            resnap_gap: Duration::from_secs(30),
            book_timeout: Duration::from_secs(120),
            max_conns: 600,
            recycle_parallel: 2,
            conn_id_base: 0,
            quiet: Quiet::default(),
            split_ratio: None,
            split_min_rate: 20_000.0,
            lat_detail: false,
            fast: None,
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

    pub fn merge(&mut self, o: &Hist) {
        for (a, b) in self.buckets.iter_mut().zip(o.buckets) {
            *a += b;
        }
        self.n += o.n;
        self.max = self.max.max(o.max);
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
    /// Tokens resubscribed because every replica went quiet (Ext 2).
    pub quiet_resubs: u64,
    /// Quiet resubscribes whose book corrected the output: real misses caught.
    pub quiet_caught: u64,
    /// Sockets reconnected because they went quiet while a peer did not (Ext 2).
    pub quiet_reconnects: u64,
    /// Tokens waiting because the connection cap is reached.
    pub unplaced: u64,
    /// Rotation snapshots taken early because an update arrived before the drain reached them.
    pub rot_early: u64,
    /// Sockets the venue closed because every asset they carried resolved.
    pub resolved_closes: u64,
    /// Hot shards split, and tokens moved by those splits.
    pub splits: u64,
    pub split_tokens: u64,
    /// Shards whose preferred (fastest) socket changed, tokens moved onto it, and sockets
    /// recycled for staying slowest (`Config::fast`).
    pub fast_picks: u64,
    pub fast_switches: u64,
    pub slow_recycles: u64,
    /// Tokens left on their source because the fast socket's book differed from it.
    pub fast_disagree: u64,
    /// Source changes of a published book, each written as a diff then a snapshot (D8).
    pub switch_snaps: u64,
    /// `recv - exch` per delta event per replica: venue to socket read.
    pub wire: Hist,
    /// Socket read to core apply: time spent queued inside this process.
    pub queue: Hist,
    /// `behind` of the events the source delivered, i.e. of what is written: how much the
    /// choice of source costs against the first copy. Kept with the arrival windows only.
    pub src_behind: MsHist,
}

impl Metrics {
    /// Folds another core's metrics into these.
    pub fn merge(&mut self, o: &Metrics) {
        macro_rules! sum { ($($f:ident),*) => { $(self.$f += o.$f;)* } }
        sum!(updates, snapshots, switches, lag_switches, hash_ok, hash_miss, hash_healed, divergent, hash_agreed,
            resnapshots, book_miss, book_diffs, disconnects, recycles, markets_added, markets_removed,
            quiet_resubs, quiet_caught, quiet_reconnects, unplaced, rot_early, resolved_closes, splits, split_tokens,
            fast_picks, fast_switches, slow_recycles, fast_disagree, switch_snaps);
        self.src_behind.merge(&o.src_behind);
        self.wire.merge(&o.wire);
        self.queue.merge(&o.queue);
    }
}

const MS_LO: i64 = -512;
const MS_SPAN: usize = 2048;

/// 1 ms buckets over -512..1536 ms, signed so a socket's `wire` survives a negative clock
/// offset. Values outside land in the end buckets; the mean is exact.
#[derive(Clone)]
pub struct MsHist {
    buckets: Box<[u32; MS_SPAN]>,
    pub n: u64,
    sum_us: i64,
}

impl Default for MsHist {
    fn default() -> Self {
        Self { buckets: Box::new([0; MS_SPAN]), n: 0, sum_us: 0 }
    }
}

impl MsHist {
    pub fn record(&mut self, us: i64) {
        let i = (us.div_euclid(1000) - MS_LO).clamp(0, MS_SPAN as i64 - 1) as usize;
        self.buckets[i] += 1;
        self.n += 1;
        self.sum_us += us;
    }

    pub fn merge(&mut self, o: &MsHist) {
        for (a, b) in self.buckets.iter_mut().zip(o.buckets.iter()) {
            *a += b;
        }
        self.n += o.n;
        self.sum_us += o.sum_us;
    }

    /// Lower edge, in ms, of the bucket holding quantile `q`.
    pub fn quantile_ms(&self, q: f64) -> i64 {
        let want = ((self.n as f64 * q).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, b) in self.buckets.iter().enumerate() {
            seen += *b as u64;
            if seen >= want {
                return i as i64 + MS_LO;
            }
        }
        MS_LO + MS_SPAN as i64 - 1
    }

    pub fn mean_ms(&self) -> f64 {
        if self.n == 0 { 0.0 } else { self.sum_us as f64 / self.n as f64 / 1e3 }
    }
}

impl std::fmt::Debug for MsHist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let q = |q| self.quantile_ms(q);
        write!(f, "n={} mean={:.1} p50={} p90={} p99={}ms", self.n, self.mean_ms(), q(0.5), q(0.9), q(0.99))
    }
}

/// One socket's latency (`Config::lat_detail`). `behind` needs no clock sync; nor does the
/// difference between two sockets' `wire`, since both are read on the same host clock.
#[derive(Clone, Default, Debug)]
pub struct SockLat {
    pub replica: usize,
    /// `recv - exch` per delta event on this socket.
    pub wire: MsHist,
    /// How long after the first socket to deliver a market event this one delivered it (0 when first).
    pub behind: MsHist,
    /// Events this socket delivered first.
    pub first: u64,
    /// Events that arrived after their first copy had left the market's window: not timed.
    pub late: u64,
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

    // ------------------------------------------------------------ connection events

    /// The socket is (re)subscribed to every token it owns; their books are on the way.
    pub fn on_up(&mut self, conn: ConnId, now: Instant) {
        tracing::debug!(conn, "up");
        let Some(cs) = self.conns.get_mut(&conn) else { return };
        cs.up_since = Some(now);
        cs.win_events = 0;
        // A new TCP connection: its old speed says nothing about this one.
        (cs.beh_sum, cs.beh_n, cs.speed, cs.slow_windows) = (0, 0, None, 0);
        let shard = cs.shard;
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
        let Some(cs) = self.conns.get_mut(&conn) else { return };
        cs.up_since = None;
        let shard = cs.shard;
        if self.shards[shard].fast == Some(conn) {
            self.shards[shard].fast = None;
        }
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
            if s.fast == Some(conn) {
                s.fast = None;
            }
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

    /// Starts a new output period: the marker, then every published book, so the period's
    /// file replays without the ones before it. Writing every book at the cut is two records
    /// per token in one tick (1.1M on the full universe), so the books are queued and drained
    /// over a few seconds instead; a book that changes before its turn is snapshotted right
    /// before that change ([`Self::open_period`]). Every file still opens each book with a
    /// snapshot ahead of its first update.
    fn maybe_rotate(&mut self, recv: Ts) {
        let Some(p) = self.cfg.rotate_every else { return };
        let e = recv / p.as_micros().max(1) as i64;
        if self.epoch == Some(e) {
            return;
        }
        self.epoch = Some(e);
        self.out.push(Rec::Rotate { epoch: e });
        self.rot_queue = self.tokens.iter().filter(|(_, t)| t.announced).map(|(s, _)| s.clone()).collect();
        let spread = (p.as_secs() / 2).clamp(1, ROTATE_SPREAD_SECS) as usize;
        self.rot_per_tick = self.rot_queue.len().div_ceil(spread);
    }

    /// Takes this tick's share of the rotation snapshots.
    fn drain_rotation(&mut self, recv: Ts) {
        let n = self.rot_per_tick.min(self.rot_queue.len());
        for t in self.rot_queue.split_off(self.rot_queue.len() - n) {
            if let Some(tok) = self.tokens.get_mut(&t)
                && tok.announced
                && Some(tok.snap_epoch) != self.epoch
            {
                Self::snapshot(tok, self.epoch, recv, &mut self.out, &mut self.metrics);
            }
        }
    }

    /// Snapshots a published book not yet written in the current period; call before any
    /// update to it is pushed.
    fn open_period(tok: &mut Tok, epoch: Option<i64>, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) {
        if let Some(e) = epoch
            && tok.announced
            && tok.snap_epoch != e
        {
            m.rot_early += 1;
            Self::snapshot(tok, epoch, recv, out, m);
        }
    }

    /// Writes both sides of what readers hold.
    fn snapshot(tok: &mut Tok, epoch: Option<i64>, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) {
        if let Some(e) = epoch {
            tok.snap_epoch = e;
        }
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
    fn emit_diff(tok: &mut Tok, conn: ConnId, epoch: Option<i64>, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) -> bool {
        let diff = tok.emitted.diff(&tok.replicas[&conn].book);
        if !diff.is_empty() {
            Self::open_period(tok, epoch, recv, out, m);
        }
        let rep = &tok.replicas[&conn];
        for &(side, px, qty) in &diff {
            out.push(Rec::Update { sym: tok.syms[side as usize].clone(), recv, exch: rep.last_exch, px, qty });
        }
        m.updates += diff.len() as u64;
        tok.emitted.clone_from(&rep.book);
        tok.emitted_exch = rep.last_exch;
        !diff.is_empty()
    }

    /// Makes `conn` the token's source: the first time with a snapshot, afterwards with the
    /// diff from what readers already hold and then a snapshot of the new book (D8). The diff
    /// keeps the stream checkable (snapshot = previous + updates); the snapshot is what the
    /// challenge asks for on a switch.
    fn adopt(tok: &mut Tok, conn: ConnId, epoch: Option<i64>, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) {
        tok.source = Some(conn);
        tok.replicas.values_mut().for_each(|r| r.unseen = None);
        if tok.announced {
            Self::emit_diff(tok, conn, epoch, recv, out, m);
            Self::snapshot(tok, epoch, recv, out, m);
            m.switch_snaps += 1;
        } else {
            let rep = &tok.replicas[&conn];
            tok.emitted.clone_from(&rep.book);
            tok.emitted_exch = rep.last_exch;
            tok.announced = true;
            Self::snapshot(tok, epoch, recv, out, m);
        }
    }

    /// Moves the token's source to its most advanced valid replica, the shard's preferred
    /// socket among equals. With none valid the token goes dark until the next `book` (never
    /// emit deltas against an untrusted book).
    fn switch(&mut self, token: &Sym, recv: Ts) {
        let Some(tok) = self.tokens.get_mut(token) else { return };
        let fast = self.shards.get(tok.shard).and_then(|s| s.fast);
        let old = tok.source.take();
        let best = tok
            .replicas
            .iter()
            .filter(|(c, r)| r.valid && Some(**c) != old)
            .max_by_key(|(c, r)| (r.seen(), Some(**c) == fast, std::cmp::Reverse(**c)))
            .map(|(c, _)| *c);
        if let Some(c) = best {
            Self::adopt(tok, c, self.epoch, recv, &mut self.out, &mut self.metrics);
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

    // ------------------------------------------------------------ activity monitoring (Ext 2)

    /// Folds this minute's delta count into each token's smoothed per-minute rate.
    fn roll_rates(&mut self, now: Instant) {
        let settle = self.cfg.quiet.ceiling;
        for tok in self.tokens.values_mut() {
            if tok.this_min > 0 && tok.quiet_at.is_none_or(|a| now.duration_since(a) >= settle) {
                tok.quiet_strikes = tok.quiet_strikes.saturating_sub(1);
            }
            tok.rate = 0.9 * tok.rate + 0.1 * tok.this_min as f32;
            tok.this_min = 0;
        }
    }

    /// Resubscribes tokens that no replica has heard from for much longer than their usual
    /// gap. The peer lag check cannot see this: it needs one replica that still gets data.
    fn check_quiet(&mut self, now: Instant, fx: &mut dyn Effects) {
        let q = &self.cfg.quiet;
        // Most overdue first, so a capped round spends its budget where silence is least likely.
        let mut due: Vec<(f32, &Sym)> = Vec::new();
        for (t, tok) in &self.tokens {
            let Some(after) = tok.quiet_after(q) else { continue };
            let since = tok.quiet_at.map_or(tok.active_at, |a| a.max(tok.active_at));
            let quiet = now.duration_since(since);
            // A replica still waiting for its book is not evidence either way.
            if quiet < after || tok.replicas.values().all(|r| !r.valid) {
                continue;
            }
            due.push((quiet.as_secs_f32() / after.as_secs_f32(), t));
        }
        if due.len() > q.max_per_round {
            due.select_nth_unstable_by(q.max_per_round, |a, b| b.0.total_cmp(&a.0));
            due.truncate(q.max_per_round);
        }
        let due: Vec<Sym> = due.into_iter().map(|(_, t)| t.clone()).collect();
        let mut by_conn: BTreeMap<ConnId, Vec<Sym>> = BTreeMap::new();
        let n = due.len();
        for t in due {
            let tok = self.tokens.get_mut(&t).expect("listed above");
            tok.quiet_at = Some(now);
            tok.quiet_strikes = tok.quiet_strikes.saturating_add(1);
            tok.quiet_probe = true;
            for c in tok.replicas.keys() {
                by_conn.entry(*c).or_default().push(t.clone());
            }
        }
        if n > 0 {
            tracing::info!(tokens = n, "quiet subscriptions; resubscribing");
        }
        self.metrics.quiet_resubs += n as u64;
        for (c, ts) in by_conn {
            fx.send(c, ConnCmd::Resnapshot(ts));
        }
    }

    /// Reconnects sockets that delivered nothing for a whole window while their shard's peer
    /// kept delivering: a socket can keep answering PINGs, and so pass the watchdog, long
    /// after the venue has stopped pushing it data.
    fn check_quiet_conns(&mut self, now: Instant, fx: &mut dyn Effects) {
        let (win, min) = (self.cfg.quiet.conn_window, self.cfg.quiet.conn_min);
        let mut quiet = Vec::new();
        for shard in &self.shards {
            let peer_max = |me: ConnId| shard.conns.iter().filter(|&&c| c != me).filter_map(|c| self.conns.get(c)).map(|c| c.win_events).max();
            for &c in &shard.conns {
                let Some(cs) = self.conns.get(&c) else { continue };
                let settled = cs.up_since.is_some_and(|u| now.duration_since(u) >= win);
                if settled && !cs.retiring && cs.win_events == 0 && (peer_max(c).unwrap_or(0) >= min || cs.win_rate >= 4.0 * min as f64) {
                    quiet.push(c);
                }
            }
        }
        for cs in self.conns.values_mut() {
            cs.win_rate = 0.7 * cs.win_rate + 0.3 * cs.win_events as f64;
            cs.win_events = 0;
        }
        for c in quiet {
            tracing::warn!(conn = c, "socket quiet while its peer is not; reconnecting");
            self.metrics.quiet_reconnects += 1;
            if let Some(cs) = self.conns.get_mut(&c) {
                cs.up_since = None;
            }
            fx.send(c, ConnCmd::Reconnect);
        }
    }

    /// Silent-subscription detector: the source has not seen an event a peer saw `lag` ago.
    fn check_lag(&mut self, now: Instant, recv: Ts, fx: &mut dyn Effects) {
        let lag = self.cfg.lag;
        let mut lagging = Vec::new();
        // Only tokens whose standby advanced are looked at. A candidate stays listed while a
        // peer is ahead of its source and the lag window has not run out.
        let tokens = &mut self.tokens;
        self.lag_cands.retain(|t| {
            let Some(tok) = tokens.get_mut(t) else { return false };
            let Some(src) = tok.source.and_then(|s| tok.replicas.get(&s).map(|r| (s, r.seen()))) else {
                tok.lag_cand = false;
                return false;
            };
            // Only a peer's delta proves the source missed something; see `Replica::last_delta`.
            let ahead = tok.replicas.iter_mut().filter(|(c, r)| **c != src.0 && r.valid && r.last_delta > src.1);
            let (mut pending, mut behind) = (false, false);
            for (_, r) in ahead {
                pending = true;
                if r.unseen.is_none_or(|(ts, _)| ts <= src.1) {
                    r.unseen = Some((r.last_delta, r.adv_at));
                }
                behind |= r.unseen.is_some_and(|(_, at)| now.duration_since(at) >= lag);
            }
            if behind {
                lagging.push((t.clone(), src.0));
            }
            tok.lag_cand = pending && !behind;
            tok.lag_cand
        });
        for (t, c) in lagging {
            tracing::info!(conn = c, token = %t, "source lagging a peer; switching");
            self.metrics.lag_switches += 1;
            self.refresh(c, &t, recv, now, fx);
        }
    }

    // ------------------------------------------------------------ recycling (Ext 2)

    fn maybe_start_recycle(&mut self, now: Instant, fx: &mut dyn Effects) {
        let Some(age) = self.cfg.max_conn_age else { return };
        let busy: HashSet<usize> = self.conns.values().filter(|c| c.retiring).map(|c| c.shard).collect();
        if busy.len() >= self.cfg.recycle_parallel {
            return;
        }
        let splitting = |s: usize| self.split.as_ref().is_some_and(|sp| sp.from == s || sp.to == s);
        // Oldest first; one recycle per shard at a time so its other replica stays put.
        let mut due: Vec<(Instant, ConnId, usize, usize)> = self
            .conns
            .iter()
            .filter(|(_, c)| {
                !c.retiring && c.replacing.is_none() && now.duration_since(c.born) >= age && !busy.contains(&c.shard) && !splitting(c.shard)
            })
            .map(|(&id, c)| (c.born, id, c.shard, c.replica))
            .collect();
        due.sort_unstable();
        let mut busy = busy;
        for (_, old, shard, replica) in due {
            if busy.len() >= self.cfg.recycle_parallel {
                break;
            }
            if !busy.insert(shard) {
                continue;
            }
            tracing::info!(conn = old, shard, "recycling connection (make-before-break)");
            self.recycle(old, shard, replica, now, fx);
        }
    }

    fn recycle(&mut self, old: ConnId, shard: usize, replica: usize, now: Instant, fx: &mut dyn Effects) {
        self.conns.get_mut(&old).expect("listed").retiring = true;
        self.open_conn(shard, replica, now, Some(old), fx);
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

    fn maybe_finish_recycle(&mut self, conn: ConnId, recv: Ts, fx: &mut dyn Effects) {
        if self.conns.get(&conn).is_some_and(|c| c.replacing.is_some() && c.awaiting.is_empty()) {
            self.finish_recycle(conn, recv, fx);
        }
    }

    fn finish_recycle(&mut self, new: ConnId, recv: Ts, fx: &mut dyn Effects) {
        let Some(old) = self.conns.get_mut(&new).and_then(|c| c.replacing.take()) else { return };
        self.conns.get_mut(&new).expect("exists").awaiting.clear();
        self.metrics.recycles += 1;
        fx.send(old, ConnCmd::Close);
        // Hand over sources now rather than waiting for the old actor to exit.
        self.on_gone(old, recv);
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

    /// The updates in `out`, and how many snapshots follow them. A source switch writes its diff
    /// and then a snapshot of both sides (D8).
    fn diff_then_snap(out: &[Rec]) -> (Vec<Rec>, usize) {
        let n = out.iter().rev().take_while(|r| matches!(r, Rec::Snapshot { .. })).count();
        (out[..out.len() - n].to_vec(), n)
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
        assert_eq!(
            diff_then_snap(&core.out),
            (vec![Rec::Update { sym: "a:ask".into(), recv: 4, exch: 7, px: 600, qty: 9 }], 2),
            "peer's extra level as a diff, then a snapshot"
        );
        assert!(matches!(&core.out[2], Rec::Snapshot { levels, .. } if levels == &[(600, 9)]));
        assert_eq!((core.metrics.switches, core.metrics.switch_snaps), (1, 1));
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
        assert_eq!(diff_then_snap(&core.out), (vec![Rec::Update { sym: "a:bid".into(), recv: 4, exch: 6, px: 400, qty: 0 }], 2));
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
        assert_eq!(diff_then_snap(&core.out), (vec![Rec::Update { sym: "a:bid".into(), recv: 4, exch: 6, px: 400, qty: 2 }], 2));
    }

    #[test]
    fn stalled_source_switches_while_the_peer_keeps_ticking() {
        // The peer's newest delta is always fresh; the lag runs from its oldest unseen one.
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        for i in 0..5 {
            let t = now + Duration::from_millis(900 * i);
            core.on_events(1, 2, &[lvl("a", 6 + i as i64, Side::Bid, 400, 2 + i as i64)], t, &mut fx);
            core.on_tick(t, 3, &mut fx);
        }
        assert_eq!(core.metrics.lag_switches, 1);
    }

    #[test]
    fn source_racing_a_peer_is_not_lag() {
        // Each peer delta reaches the source 100 ms later: never behind by the lag window.
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        for i in 0..50 {
            let t = now + Duration::from_millis(200 * i);
            let e = lvl("a", 6 + i as i64, Side::Bid, 400, 2 + i as i64);
            core.on_events(1, 2, std::slice::from_ref(&e), t, &mut fx);
            core.on_tick(t + Duration::from_millis(50), 3, &mut fx);
            core.on_events(0, 2, &[e], t + Duration::from_millis(100), &mut fx);
        }
        assert_eq!(core.metrics.lag_switches, 0);
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
    fn only_the_source_is_offered_for_hashing() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        assert!(View { tokens: &core.tokens, conn: 0 }.book("a").is_some());
        assert!(View { tokens: &core.tokens, conn: 1 }.book("a").is_none(), "standby is not hashed");
    }

    #[test]
    fn caught_up_source_leaves_the_lag_candidates() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 2, &[lvl("a", 6, Side::Bid, 400, 2)], now, &mut fx);
        assert_eq!(core.lag_cands.len(), 1);
        core.on_events(0, 2, &[lvl("a", 6, Side::Bid, 400, 2)], now, &mut fx);
        core.on_tick(now + Duration::from_secs(1), 3, &mut fx);
        assert!(core.lag_cands.is_empty() && !core.tokens["a"].lag_cand);
        core.on_tick(now + Duration::from_secs(10), 4, &mut fx);
        assert_eq!(core.metrics.lag_switches, 0);
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
    fn socket_closed_as_all_resolved_ends_its_markets() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[(600, 1)])], now, &mut fx);
        core.out.clear();
        // No market_resolved event: the venue just closes the socket.
        core.on_resolved(0, &["a".into(), "b".into()], 2, &mut fx);
        assert_eq!(core.metrics.resolved_closes, 1);
        assert_eq!(core.out.len(), 2);
        assert!(core.out.iter().all(|r| matches!(r, Rec::Update { qty: 0, .. })));
        assert_eq!(core.token_count(), 0);
        assert_eq!(fx.sent.iter().filter(|(_, s)| s.starts_with("Unsubscribe")).count(), 2);
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
    fn rotation_marks_the_cut_and_snapshots_every_published_book() {
        let (mut core, mut fx, now) = setup();
        core.cfg.rotate_every = Some(Duration::from_secs(10));
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(0, 1, &[book("b", 5, &[(400, 1)], &[])], now, &mut fx);
        let cut = |out: &[Rec]| out.iter().position(|r| matches!(r, Rec::Rotate { .. }));
        let snaps = |out: &[Rec], t: &str| out.iter().filter(|r| matches!(r, Rec::Snapshot { sym, .. } if sym.starts_with(t))).count();
        core.out.clear();
        core.on_tick(now, 3_000_000, &mut fx);
        assert_eq!(core.out[0], Rec::Rotate { epoch: 0 });
        assert_eq!(snaps(&core.out, ""), 2, "spread over half the period: one book per tick");
        core.on_tick(now + Duration::from_secs(1), 9_999_999, &mut fx);
        assert_eq!(cut(&core.out[1..]), None, "same period");
        assert_eq!(snaps(&core.out, "a"), 2);
        assert_eq!(snaps(&core.out, "b"), 2);
        core.out.clear();
        core.on_tick(now + Duration::from_secs(2), 10_000_000, &mut fx);
        assert_eq!(core.out[0], Rec::Rotate { epoch: 1 });
        for s in 3..8 {
            core.on_tick(now + Duration::from_secs(s), 10_000_000 + s as i64, &mut fx);
        }
        assert!(snaps(&core.out, "a") >= 2 && snaps(&core.out, "b") >= 2, "every book again in the new period");
    }

    #[test]
    fn update_before_its_rotation_turn_snapshots_first() {
        let (mut core, mut fx, now) = setup();
        core.cfg.rotate_every = Some(Duration::from_secs(600));
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(0, 1, &[book("b", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_tick(now, 3_000_000, &mut fx);
        core.on_tick(now, 600_000_000, &mut fx);
        core.out.clear();
        core.on_events(0, 600_000_001, &[lvl("a", 6, Side::Bid, 400, 2)], now, &mut fx);
        core.on_events(0, 600_000_001, &[lvl("b", 6, Side::Bid, 400, 3)], now, &mut fx);
        let kinds: Vec<&str> = core.out.iter().map(|r| match r { Rec::Snapshot { .. } => "S", Rec::Update { .. } => "U", _ => "?" }).collect();
        // One of a/b was drained at the cut; the other is snapshotted right before its update.
        assert!(kinds == ["U", "S", "S", "U"] || kinds == ["S", "S", "U", "U"], "{kinds:?}");
        assert_eq!(core.metrics.rot_early, 1);
        for s in 1..12 {
            core.on_tick(now + Duration::from_secs(s), 600_000_000 + s as i64, &mut fx);
        }
        assert_eq!(core.metrics.rot_early, 1, "the drain skips books already written this period");
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
        assert_eq!(diff_then_snap(&core.out), (vec![], 2), "identical books: the handover writes only a snapshot");
    }

    fn set_rate(core: &mut Core<()>, t: &str, rate: f32) {
        core.tokens.get_mut(t).unwrap().rate = rate;
    }

    #[test]
    fn quiet_token_is_resubscribed_on_every_replica() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        // 6 deltas a minute: one every 10 s, so quiet after 20 gaps = 200 s.
        set_rate(&mut core, "a", 6.0);
        core.check_quiet(now + Duration::from_secs(199), &mut fx);
        assert_eq!(core.metrics.quiet_resubs, 0, "within its usual silence");
        core.check_quiet(now + Duration::from_secs(201), &mut fx);
        assert_eq!(core.metrics.quiet_resubs, 1);
        let mut to: Vec<ConnId> = fx.sent.iter().filter(|(_, s)| s == r#"Resnapshot(["a"])"#).map(|(c, _)| *c).collect();
        to.sort();
        assert_eq!(to, vec![0, 1], "every replica resubscribes: none of them is hearing anything");
    }

    #[test]
    fn quiet_backs_off_through_deltas_until_a_resubscribe_catches_a_miss() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        set_rate(&mut core, "a", 6.0);
        let at = |s| now + Duration::from_secs(s);
        core.check_quiet(at(201), &mut fx);
        core.check_quiet(at(500), &mut fx);
        assert_eq!(core.metrics.quiet_resubs, 1, "second wait is doubled to 400 s");
        core.check_quiet(at(602), &mut fx);
        assert_eq!(core.metrics.quiet_resubs, 2);
        // A burst after the silence is how bursty markets look; it is no proof of a miss.
        core.on_events(0, 2, &[lvl("a", 6, Side::Bid, 410, 1)], at(610), &mut fx);
        assert_eq!(core.tokens["a"].quiet_strikes, 2);
        // The answering book matches what readers hold: a clean probe, strikes stay.
        core.on_events(0, 3, &[book("a", 7, &[(400, 1), (410, 1)], &[])], at(611), &mut fx);
        assert_eq!((core.tokens["a"].quiet_strikes, core.metrics.quiet_caught), (2, 0));
        core.check_quiet(at(1409), &mut fx);
        assert_eq!(core.metrics.quiet_resubs, 2, "800 s wait from the last delta");
        core.check_quiet(at(1411), &mut fx);
        assert_eq!(core.metrics.quiet_resubs, 3);
        // This time the book shows a level we never heard about: a real miss.
        core.on_events(0, 4, &[book("a", 8, &[(400, 1), (410, 1), (420, 3)], &[])], at(1413), &mut fx);
        assert_eq!((core.tokens["a"].quiet_strikes, core.metrics.quiet_caught), (0, 1));
    }

    #[test]
    fn strikes_settle_while_active_long_after_the_last_resubscribe() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        let tok = core.tokens.get_mut("a").unwrap();
        (tok.quiet_strikes, tok.quiet_at) = (3, Some(now));
        core.on_events(0, 2, &[lvl("a", 6, Side::Bid, 410, 1)], now, &mut fx);
        core.roll_rates(now + Duration::from_secs(60));
        assert_eq!(core.tokens["a"].quiet_strikes, 3, "too soon after the resubscribe");
        core.on_events(0, 2, &[lvl("a", 7, Side::Bid, 410, 2)], now, &mut fx);
        core.roll_rates(now + core.cfg.quiet.ceiling);
        assert_eq!(core.tokens["a"].quiet_strikes, 2);
        core.roll_rates(now + core.cfg.quiet.ceiling * 2);
        assert_eq!(core.tokens["a"].quiet_strikes, 2, "only while deltas flow");
    }

    #[test]
    fn capped_round_takes_the_most_overdue() {
        let (mut core, mut fx, now) = setup();
        core.cfg.quiet.max_per_round = 1;
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[]), book("b", 5, &[(400, 1)], &[])], now, &mut fx);
        set_rate(&mut core, "a", 6.0);
        set_rate(&mut core, "b", 60.0);
        core.check_quiet(now + Duration::from_secs(300), &mut fx);
        assert!(fx.sent.iter().any(|(_, s)| s == r#"Resnapshot(["b"])"#), "b is 5x overdue, a 1.5x");
        assert_eq!(core.metrics.quiet_resubs, 1);
    }

    #[test]
    fn slow_token_is_left_to_recycling() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        set_rate(&mut core, "a", 0.5);
        core.check_quiet(now + Duration::from_secs(3 * 3600), &mut fx);
        assert_eq!(core.metrics.quiet_resubs, 0, "below min_rate silence says nothing");
    }

    #[test]
    fn rates_follow_delta_counts() {
        let (mut core, mut fx, now) = setup();
        core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
        for ts in 6..16 {
            core.on_events(0, 2, &[lvl("a", ts, Side::Bid, 400, ts)], now, &mut fx);
            core.on_events(1, 2, &[lvl("a", ts, Side::Bid, 400, ts)], now, &mut fx);
        }
        core.roll_rates(now);
        assert_eq!(core.tokens["a"].rate, 1.0, "10 distinct deltas (seen on two sockets) smoothed by 0.1");
        assert_eq!(core.tokens["b"].rate, 0.0);
    }

    fn busy_window(core: &mut Core<()>, fx: &mut Fx, now: Instant, conn: ConnId, n: i64) {
        for ts in 0..n {
            core.on_events(conn, 2, &[lvl("a", 100 + ts, Side::Bid, 400, 1)], now, fx);
        }
    }

    #[test]
    fn socket_quiet_while_peer_is_busy_reconnects() {
        let (mut core, mut fx, now) = setup();
        core.on_up(0, now);
        core.on_up(1, now);
        busy_window(&mut core, &mut fx, now, 1, 40);
        core.check_quiet_conns(now + Duration::from_secs(31), &mut fx);
        assert_eq!(fx.sent, vec![(0, "Reconnect".to_string())]);
        assert_eq!(core.metrics.quiet_reconnects, 1);
        core.check_quiet_conns(now + Duration::from_secs(62), &mut fx);
        assert_eq!(core.metrics.quiet_reconnects, 1, "not again until it is back up for a window");
    }

    #[test]
    fn shard_quiet_on_both_sockets_is_not_a_socket_fault() {
        let (mut core, mut fx, now) = setup();
        core.on_up(0, now);
        core.on_up(1, now);
        busy_window(&mut core, &mut fx, now, 0, 5);
        core.check_quiet_conns(now + Duration::from_secs(31), &mut fx);
        assert_eq!(core.metrics.quiet_reconnects, 0, "a peer below conn_min is no evidence");
        core.check_quiet_conns(now + Duration::from_secs(10), &mut fx);
        assert_eq!(core.metrics.quiet_reconnects, 0);
    }

    #[test]
    fn recycles_run_in_parallel_across_shards_only() {
        let mut core = Core::new(
            Config { tokens_per_conn: 1, max_conn_age: Some(Duration::from_secs(10)), recycle_parallel: 2, ..Config::default() },
            Box::new(NoCheck),
        );
        let mut fx = Fx::default();
        let now = Instant::now();
        core.add(vec![inst("m", &["a"]), inst("n", &["b"]), inst("o", &["c"])], &mut fx);
        core.flush_pending(now, &mut fx);
        assert_eq!(fx.spawned.len(), 6, "three shards × two replicas");
        core.maybe_start_recycle(now + Duration::from_secs(11), &mut fx);
        let new: Vec<_> = fx.spawned[6..].iter().map(|(id, _, t)| (*id, t.clone())).collect();
        assert_eq!(new.len(), 2, "capped at recycle_parallel");
        assert_ne!(new[0].1, new[1].1, "never both replicas of one shard");
        core.maybe_start_recycle(now + Duration::from_secs(12), &mut fx);
        assert_eq!(fx.spawned.len(), 8, "no more until one finishes");
    }

    /// Three shards of two replicas: [a..f] on 0/1, [g..h3] on 2/3, [i j] on 4/5, all up with
    /// books on both replicas and 10 deltas a minute per token.
    fn three_shards() -> (Core<()>, Fx, Instant) {
        let cfg = Config { tokens_per_conn: 6, split_ratio: Some(3.0), split_min_rate: 100.0, ..Config::default() };
        let mut core = Core::new(cfg, Box::new(NoCheck));
        let mut fx = Fx::default();
        let now = Instant::now();
        core.add(vec![inst("m1", &["a", "b"]), inst("m2", &["c", "d"]), inst("m3", &["e", "f"])], &mut fx);
        core.add(vec![inst("m4", &["g", "h"]), inst("m7", &["g2", "h2"]), inst("m8", &["g3", "h3"])], &mut fx);
        core.add(vec![inst("m5", &["i", "j"])], &mut fx);
        core.flush_pending(now, &mut fx);
        for c in 0..6 {
            core.on_up(c, now);
            for t in &core.shards[c as usize / 2].tokens.clone() {
                core.on_events(c, 1, &[book(t, 5, &[(400, 1)], &[])], now, &mut fx);
            }
        }
        for t in core.tokens.keys().cloned().collect::<Vec<_>>() {
            set_rate(&mut core, &t, 10.0);
        }
        (core, fx, now)
    }

    #[test]
    fn hot_shard_splits_make_before_break() {
        let (mut core, mut fx, now) = three_shards();
        set_rate(&mut core, "a", 1000.0);
        set_rate(&mut core, "b", 1000.0);
        core.maybe_split(now, &mut fx);
        assert_eq!(core.metrics.splits, 1);
        let new: Vec<_> = fx.spawned[6..].iter().map(|(c, _, ts)| (*c, ts.clone())).collect();
        assert_eq!(new, vec![(6, vec![Sym::from("a"), "b".into()]), (7, vec!["a".into(), "b".into()])], "only the hot market moves");
        assert!(!fx.sent.iter().any(|(_, s)| s.starts_with("Unsubscribe")), "old sockets keep serving meanwhile");
        assert_eq!(core.tokens["a"].source, Some(0));
        core.out.clear();
        for c in [6, 7] {
            core.on_up(c, now);
            core.on_events(c, 2, &[book("a", 6, &[(400, 1)], &[]), book("b", 6, &[(400, 1)], &[])], now, &mut fx);
        }
        let unsub: Vec<_> = fx.sent.iter().filter(|(_, s)| s.starts_with("Unsubscribe")).cloned().collect();
        assert_eq!(unsub, vec![(0, r#"Unsubscribe(["a", "b"])"#.to_string()), (1, r#"Unsubscribe(["a", "b"])"#.to_string())]);
        assert!(core.tokens["a"].source.is_some_and(|s| s >= 6), "source handed to the new socket");
        assert_eq!(core.tokens["a"].replicas.len(), 2);
        assert_eq!(core.shards[0].tokens.len(), 4);
        assert_eq!(diff_then_snap(&core.out), (vec![], 4), "identical books: the split writes only snapshots");
        // The old socket dropping no longer touches the moved tokens.
        core.on_down(0, 3);
        assert!(core.tokens["a"].source.is_some_and(|s| s >= 6));
    }

    #[test]
    fn shard_near_the_median_does_not_split() {
        let (mut core, mut fx, now) = three_shards();
        set_rate(&mut core, "a", 25.0);
        core.maybe_split(now, &mut fx);
        assert_eq!(core.metrics.splits, 0, "75 a minute against a median of 60: under 3x");
        core.cfg.split_min_rate = 10_000.0;
        set_rate(&mut core, "a", 1000.0);
        core.maybe_split(now, &mut fx);
        assert_eq!(core.metrics.splits, 0, "under the absolute floor");
    }

    #[test]
    fn split_hands_over_after_grace_and_survives_a_resolved_market() {
        let (mut core, mut fx, now) = three_shards();
        set_rate(&mut core, "a", 1000.0);
        set_rate(&mut core, "c", 900.0);
        core.maybe_split(now, &mut fx);
        assert_eq!(core.split.as_ref().unwrap().moved.len(), 2, "m1 alone is half the rate");
        core.remove_market("m1", 2, &mut fx);
        assert_eq!(core.shards[0].tokens.len(), 4, "gone from both shards");
        core.on_tick(now + Duration::from_secs(61), 3, &mut fx);
        assert!(core.split.is_none(), "grace ran out");
        assert_eq!(core.shards[0].tokens.len(), 4);
    }

    #[test]
    fn late_markets_do_not_join_a_split_off_shard() {
        let (mut core, mut fx, now) = three_shards();
        set_rate(&mut core, "a", 1000.0);
        set_rate(&mut core, "b", 1000.0);
        core.maybe_split(now, &mut fx);
        core.add(vec![inst("m6", &["k", "l"])], &mut fx);
        assert_eq!(core.tokens["k"].shard, 2, "joins the last ordinary shard");
    }

    #[test]
    fn conn_ids_start_at_the_core_base() {
        let mut core = Core::new(Config { tokens_per_conn: 10, conn_id_base: 1 << 20, ..Config::default() }, Box::new(NoCheck));
        let mut fx = Fx::default();
        core.add(vec![inst("m", &["a"])], &mut fx);
        core.flush_pending(Instant::now(), &mut fx);
        assert_eq!(fx.spawned.iter().map(|s| s.0).collect::<Vec<_>>(), vec![1 << 20, (1 << 20) + 1]);
    }

    #[test]
    fn ms_hist_keeps_negative_wire_and_exact_mean() {
        let mut h = MsHist::default();
        for us in [-20_000, 3_400, 3_900, 9_000_000] {
            h.record(us);
        }
        assert_eq!((h.n, h.quantile_ms(0.25), h.quantile_ms(0.5), h.quantile_ms(1.0)), (4, -20, 3, 1535), "a 9 s outlier lands in the top bucket");
        assert!((h.mean_ms() - 2_246.825).abs() < 1e-9, "the mean is not clamped");
    }

    #[test]
    fn behind_is_timed_against_the_first_socket_to_deliver_an_event() {
        let mut core = Core::new(Config { tokens_per_conn: 10, lat_detail: true, ..Config::default() }, Box::new(NoCheck));
        let mut fx = Fx::default();
        let now = Instant::now();
        core.add(vec![inst("m", &["a", "b"])], &mut fx);
        core.flush_pending(now, &mut fx);
        let ev = |ts| [lvl("a", ts, Side::Bid, 400, 1)];
        core.on_events(1, 10_000, &ev(5_000), now, &mut fx);
        core.on_events(1, 10_100, &ev(5_000), now, &mut fx);
        core.on_events(0, 13_000, &ev(5_000), now, &mut fx);
        core.on_events(0, 20_000, &ev(9_000), now, &mut fx);
        core.on_events(1, 20_500, &ev(9_000), now, &mut fx);
        for ts in 10..30 {
            core.on_events(0, 30_000 + ts * 1_000, &ev(ts * 1_000), now, &mut fx);
        }
        core.on_events(1, 99_000, &ev(5_000), now, &mut fx);

        let (r0, r1) = (&core.lat[&0], &core.lat[&1]);
        assert_eq!((r1.first, r1.behind.n, r1.late), (1, 2, 1), "second same-ms event untimed; a copy past the window is late");
        assert_eq!((r0.first, r0.behind.n), (21, 22));
        assert_eq!(r0.behind.quantile_ms(0.0), 0);
        assert!((r0.behind.mean_ms() - 3.0 / 22.0).abs() < 1e-9, "3 ms behind once, first otherwise");
        assert!((r1.behind.mean_ms() - 0.25).abs() < 1e-9, "first once, 0.5 ms behind once");
        assert_eq!(r1.wire.n, 4, "wire counts every event, timed or not");
    }

    /// One shard [a b] on sockets 0 and 1, both up with identical books, 0 the source of both.
    fn fast_pair(f: Fast) -> (Core<()>, Fx, Instant) {
        let mut core = Core::new(Config { tokens_per_conn: 10, fast: Some(f), ..Config::default() }, Box::new(NoCheck));
        let mut fx = Fx::default();
        let now = Instant::now();
        core.add(vec![inst("m", &["a", "b"])], &mut fx);
        core.flush_pending(now, &mut fx);
        for c in [0, 1] {
            core.on_up(c, now);
            core.on_events(c, 1, &[book("a", 1, &[(400, 1)], &[]), book("b", 1, &[(300, 1)], &[])], now, &mut fx);
        }
        (core, fx, now)
    }

    /// 60 events on "a", each delivered by `fast` first and by the other socket `gap_us` later.
    fn race(core: &mut Core<()>, fx: &mut Fx, now: Instant, fast: ConnId, gap_us: i64, from: i64) {
        for i in from..from + 60 {
            let ev = [lvl("a", i * 10_000, Side::Bid, 400, i)];
            core.on_events(fast, i * 10_000 + 1_000, &ev, now, fx);
            core.on_events(1 - fast, i * 10_000 + 1_000 + gap_us, &ev, now, fx);
        }
    }

    #[test]
    fn sources_follow_the_fastest_socket_past_a_margin() {
        let (mut core, mut fx, now) = fast_pair(Fast { margin_ms: 1.0, weight: 0.5, ..Fast::default() });
        assert_eq!(core.tokens["a"].source, Some(0));
        race(&mut core, &mut fx, now, 1, 5_000, 1);
        assert_eq!((core.metrics.src_behind.n, core.metrics.src_behind.mean_ms()), (60, 5.0), "written events came 5 ms late");
        core.out.clear();
        core.check_speed(now, 2, &mut fx);
        assert_eq!((core.tokens["a"].source, core.tokens["b"].source), (Some(1), Some(1)));
        assert_eq!((core.metrics.fast_picks, core.metrics.fast_switches), (1, 2));
        assert_eq!(diff_then_snap(&core.out), (vec![], 4), "equal books: the move writes only snapshots");

        // A fast socket whose book differs from the source's is not trusted for speed.
        core.on_events(0, 5, &[book("b", 2, &[(300, 9)], &[])], now, &mut fx);

        // Socket 0 now wins by 0.5 ms: within the margin, so the sources stay.
        race(&mut core, &mut fx, now, 0, 500, 100);
        core.check_speed(now, 3, &mut fx);
        assert_eq!((core.tokens["a"].source, core.metrics.fast_picks), (Some(1), 1));
        // By 10 ms: the scores cross by more than the margin, and the sources move back.
        race(&mut core, &mut fx, now, 0, 10_000, 200);
        core.check_speed(now, 4, &mut fx);
        assert_eq!((core.tokens["a"].source, core.metrics.fast_picks), (Some(0), 2));
        assert_eq!((core.tokens["b"].source, core.metrics.fast_disagree), (Some(1), 1), "books differ: b stays");
    }

    #[test]
    fn failover_prefers_the_fast_socket_among_equals() {
        let cfg = Config { tokens_per_conn: 10, replicas: 3, fast: Some(Fast::default()), ..Config::default() };
        let mut core = Core::new(cfg, Box::new(NoCheck));
        let mut fx = Fx::default();
        let now = Instant::now();
        core.add(vec![inst("m", &["a"])], &mut fx);
        core.flush_pending(now, &mut fx);
        for c in [0, 1, 2] {
            core.on_events(c, 1, &[book("a", 1, &[(400, 1)], &[])], now, &mut fx);
        }
        core.shards[0].fast = Some(2);
        core.on_down(0, 5);
        assert_eq!(core.tokens["a"].source, Some(2), "1 and 2 are level; 2 is preferred");
    }

    #[test]
    fn socket_that_stays_slowest_is_recycled() {
        let (mut core, mut fx, now) = fast_pair(Fast { slow_windows: 2, weight: 0.5, ..Fast::default() });
        race(&mut core, &mut fx, now, 1, 10_000, 1);
        core.check_speed(now, 2, &mut fx);
        assert_eq!(core.metrics.slow_recycles, 0, "one slow window is not enough");
        race(&mut core, &mut fx, now, 1, 10_000, 100);
        core.check_speed(now, 3, &mut fx);
        assert_eq!(core.metrics.slow_recycles, 1);
        let (id, replica, _) = fx.spawned.last().unwrap();
        assert_eq!((*id, *replica), (2, 0), "a new socket for replica 0, opened before 0 closes");
        assert!(core.conns[&0].retiring && !fx.sent.iter().any(|(_, s)| s == "Close"));
        race(&mut core, &mut fx, now, 1, 10_000, 200);
        core.check_speed(now, 4, &mut fx);
        assert_eq!(core.metrics.slow_recycles, 1, "a retiring socket is not scored");
    }

    #[test]
    fn metrics_merge_sums_counts_and_histograms() {
        let mut a = Metrics { updates: 2, quiet_resubs: 1, ..Metrics::default() };
        a.wire.record(1000);
        let mut b = Metrics { updates: 3, ..Metrics::default() };
        b.wire.record(5000);
        a.merge(&b);
        assert_eq!((a.updates, a.quiet_resubs, a.wire.n, a.wire.max), (5, 1, 2, 5000));
    }
}
