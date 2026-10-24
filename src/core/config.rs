//! Core tuning: shard sizes, replicas, timeouts and the Ext 2 / Ext 4 thresholds.

use std::time::Duration;

use crate::adapter::ConnId;

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
