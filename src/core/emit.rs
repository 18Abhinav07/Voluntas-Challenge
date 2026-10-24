//! What readers see: rotation periods, snapshots, diffs and source switches (D8).

use super::{Core, Metrics, Tok};
use crate::{
    adapter::{ConnId, Sym, Ts},
    book::Side,
    writer::Rec,
};

/// Rotation snapshots are spread over at most this many ticks (half the period if shorter).
const ROTATE_SPREAD_SECS: u64 = 10;

impl<M> Core<M> {
    /// Starts a new output period: the marker, then every published book, so the period's
    /// file replays without the ones before it. Writing every book at the cut is two records
    /// per token in one tick (1.1M on the full universe), so the books are queued and drained
    /// over a few seconds instead; a book that changes before its turn is snapshotted right
    /// before that change ([`Self::open_period`]). Every file still opens each book with a
    /// snapshot ahead of its first update.
    pub(super) fn maybe_rotate(&mut self, recv: Ts) {
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
    pub(super) fn drain_rotation(&mut self, recv: Ts) {
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
    pub(super) fn open_period(tok: &mut Tok, epoch: Option<i64>, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) {
        if let Some(e) = epoch
            && tok.announced
            && tok.snap_epoch != e
        {
            m.rot_early += 1;
            Self::snapshot(tok, epoch, recv, out, m);
        }
    }

    /// Writes both sides of what readers hold.
    pub(super) fn snapshot(tok: &mut Tok, epoch: Option<i64>, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) {
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
    pub(super) fn emit_diff(tok: &mut Tok, conn: ConnId, epoch: Option<i64>, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) -> bool {
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
    pub(super) fn adopt(tok: &mut Tok, conn: ConnId, epoch: Option<i64>, recv: Ts, out: &mut Vec<Rec>, m: &mut Metrics) {
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
    pub(super) fn switch(&mut self, token: &Sym, recv: Ts) {
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
}
