//! Shard planning: placing discovered markets on shards and connections, ending resolved
//! markets, and splitting hot shards.

use std::time::Instant;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use super::{ConnState, Core, Effects, Replica, Reps, Shard, Split, Tok, bucket_of};
use crate::{
    adapter::{ConnId, Instrument, Sym, Ts},
    book::{Book, Side},
    conn::ConnCmd,
    writer::Rec,
};

impl<M> Core<M> {
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
                    Box::new(Tok {
                        shard,
                        token: t.clone(),
                        source: None,
                        replicas: Reps::default(),
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
                    }),
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

    pub(super) fn open_conn(&mut self, shard: usize, replica: usize, now: Instant, replacing: Option<ConnId>, fx: &mut dyn Effects) -> ConnId {
        let id = self.next_conn;
        self.next_conn += 1;
        let tokens = self.shards[shard].tokens.clone();
        for t in &tokens {
            if let Some(tok) = self.tokens.get_mut(t) {
                tok.replicas.insert(id, Replica::new(now));
            }
        }
        self.shards[shard].conns.push(id);
        let awaiting = if replacing.is_some() { tokens.iter().cloned().collect() } else { HashSet::default() };
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

    /// Ends a market: zero every level a reader holds for it, then unsubscribe and forget it.
    pub fn remove_market(&mut self, market: &str, recv: Ts, fx: &mut dyn Effects) {
        let Some(toks) = self.markets.remove(market) else { return };
        self.arrivals.remove(market);
        self.metrics.markets_removed += 1;
        let mut by_conn: HashMap<ConnId, Vec<Sym>> = HashMap::default();
        for t in &toks {
            let Some(mut tok) = self.tokens.remove(t) else { continue };
            Self::open_period(&mut tok, self.epoch, recv, &mut self.out, &mut self.metrics);
            for side in [Side::Bid, Side::Ask] {
                for &(px, _) in tok.emitted.side(side) {
                    self.out.push(Rec::Update { sym: tok.token.clone(), side, recv, exch: tok.emitted_exch, px, qty: 0 });
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

    /// Moves the hottest markets of the hottest shard to a new shard when that shard carries
    /// `split_ratio` times the median shard's rate. Make-before-break, like recycling: the new
    /// sockets subscribe first and the old ones keep serving until every book has arrived.
    pub(super) fn maybe_split(&mut self, now: Instant, fx: &mut dyn Effects) {
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

    pub(super) fn maybe_finish_split(&mut self, recv: Ts, fx: &mut dyn Effects) {
        let Some(sp) = &self.split else { return };
        let conns = &self.shards[sp.to].conns;
        if conns.len() == self.cfg.replicas && conns.iter().filter_map(|c| self.conns.get(c)).all(|c| c.awaiting.is_empty()) {
            self.finish_split(recv, fx);
        }
    }

    /// The new sockets have every book (or the grace ran out): the old ones drop the moved tokens.
    pub(super) fn finish_split(&mut self, recv: Ts, fx: &mut dyn Effects) {
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
