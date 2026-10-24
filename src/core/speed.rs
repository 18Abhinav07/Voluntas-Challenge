//! Speed-aware source choice (Ext 4): timing each socket against the first copy of every
//! market event, preferring the fastest socket per shard and recycling one that stays slowest.

use std::{collections::HashSet, time::Instant};

use super::{Core, Effects, SockLat};
use crate::adapter::{ConnId, Sym, Ts};

const ARRIVALS: usize = 16;

/// A market's last few events by exchange timestamp: (exch, first recv, replicas seen as bits).
#[derive(Default)]
pub(super) struct Arrivals {
    ring: [(Ts, Ts, u64); ARRIVALS],
    next: usize,
}

impl<M> Core<M> {
    /// Times one delta event on `conn`: its `wire`, and how far behind the first socket to
    /// deliver the same (market, exch_ts) it came. Events are matched by exchange timestamp,
    /// which Polymarket stamps once and sends to every socket.
    pub(super) fn note_arrival(&mut self, conn: ConnId, market: &Sym, exch: Ts, recv: Ts) -> Option<i64> {
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

    /// Scores each socket by this window's mean `behind`, moves each shard's sources onto its
    /// fastest socket, and recycles a socket that stays far behind. See [`Fast`].
    pub(super) fn check_speed(&mut self, now: Instant, recv: Ts, fx: &mut dyn Effects) {
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
}
