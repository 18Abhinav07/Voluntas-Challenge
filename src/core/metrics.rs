//! Counters and latency histograms the core keeps and `main` prints.

/// Log2-bucketed latency histogram in µs; prints p50/p99/max in ms.
#[derive(Clone, Default)]
pub struct Hist {
    buckets: [u64; 32],
    pub(super) n: u64,
    pub(super) max: i64,
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
    /// Speed windows skipped because every socket of the shard was behind (a common stall).
    pub fast_stalls: u64,
    /// Source changes of a published book, each written as a diff then a snapshot (D8).
    pub switch_snaps: u64,
    /// `recv - exch` per delta event per replica: venue to socket read.
    pub wire: Hist,
    /// Socket read to core apply: time spent queued inside this process.
    pub queue: Hist,
    /// `behind` of the events the source delivered, i.e. of what is written: how much the
    /// choice of source costs against the first copy. Kept with the arrival windows only.
    pub src_behind: MsHist,
    /// `recv - exch` of the events the source delivered: the absolute latency of what is
    /// written, comparable across replica counts. Kept with the arrival windows only.
    pub src_wire: MsHist,
}

impl Metrics {
    /// Folds another core's metrics into these.
    pub fn merge(&mut self, o: &Metrics) {
        macro_rules! sum { ($($f:ident),*) => { $(self.$f += o.$f;)* } }
        sum!(updates, snapshots, switches, lag_switches, hash_ok, hash_miss, hash_healed, divergent, hash_agreed,
            resnapshots, book_miss, book_diffs, disconnects, recycles, markets_added, markets_removed,
            quiet_resubs, quiet_caught, quiet_reconnects, unplaced, rot_early, resolved_closes, splits, split_tokens,
            fast_picks, fast_switches, slow_recycles, fast_disagree, fast_stalls, switch_snaps);
        self.src_behind.merge(&o.src_behind);
        self.src_wire.merge(&o.src_wire);
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
        if self.n == 0 {
            return write!(f, "n=0");
        }
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
