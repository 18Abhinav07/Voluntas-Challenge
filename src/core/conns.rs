//! Connection lifecycle: up, down and gone events, and make-before-break recycling (Ext 2).

use std::{collections::HashSet, time::Instant};

use super::{Core, Effects};
use crate::{
    adapter::{ConnId, Ts},
    book::Book,
    conn::ConnCmd,
};

impl<M> Core<M> {
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

    pub(super) fn maybe_start_recycle(&mut self, now: Instant, fx: &mut dyn Effects) {
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

    pub(super) fn recycle(&mut self, old: ConnId, shard: usize, replica: usize, now: Instant, fx: &mut dyn Effects) {
        self.conns.get_mut(&old).expect("listed").retiring = true;
        self.open_conn(shard, replica, now, Some(old), fx);
    }

    pub(super) fn maybe_finish_recycle(&mut self, conn: ConnId, recv: Ts, fx: &mut dyn Effects) {
        if self.conns.get(&conn).is_some_and(|c| c.replacing.is_some() && c.awaiting.is_empty()) {
            self.finish_recycle(conn, recv, fx);
        }
    }

    pub(super) fn finish_recycle(&mut self, new: ConnId, recv: Ts, fx: &mut dyn Effects) {
        let Some(old) = self.conns.get_mut(&new).and_then(|c| c.replacing.take()) else { return };
        self.conns.get_mut(&new).expect("exists").awaiting.clear();
        self.metrics.recycles += 1;
        fx.send(old, ConnCmd::Close);
        // Hand over sources now rather than waiting for the old actor to exit.
        self.on_gone(old, recv);
    }
}
