//! Keeping replicas honest: the integrity ladder (§5.5), resnapshots, and the activity and
//! lag checks of Ext 2 that catch a socket gone silent or behind.

use std::{collections::BTreeMap, time::Instant};

use super::{Core, Effects};
use crate::{
    adapter::{ConnId, Sym, Ts, Verdict},
    conn::ConnCmd,
};

impl<M> Core<M> {
    pub(super) fn resnapshot(&mut self, conn: ConnId, token: &Sym, now: Instant, fx: &mut dyn Effects) {
        let gap = self.cfg.resnap_gap;
        let Some(rep) = self.tokens.get_mut(token).and_then(|t| t.replicas.get_mut(&conn)) else { return };
        if rep.book_due.is_some_and(|due| now < due) {
            return;
        }
        rep.book_due = Some(now + gap);
        self.metrics.resnapshots += 1;
        fx.send(conn, ConnCmd::Resnapshot(vec![token.clone()]));
    }

    pub(super) fn handle_verdicts(&mut self, recv: Ts, now: Instant, fx: &mut dyn Effects) {
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
    pub(super) fn refresh(&mut self, conn: ConnId, token: &Sym, recv: Ts, now: Instant, fx: &mut dyn Effects) {
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
    pub(super) fn retry_resnapshots(&mut self, now: Instant, fx: &mut dyn Effects) {
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

    /// Folds this minute's delta count into each token's smoothed per-minute rate.
    pub(super) fn roll_rates(&mut self, now: Instant) {
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
    pub(super) fn check_quiet(&mut self, now: Instant, fx: &mut dyn Effects) {
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
    pub(super) fn check_quiet_conns(&mut self, now: Instant, fx: &mut dyn Effects) {
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
    pub(super) fn check_lag(&mut self, now: Instant, recv: Ts, fx: &mut dyn Effects) {
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
}
