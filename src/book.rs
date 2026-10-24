//! One L2 book: both sides in one sorted vector of `(price, absolute quantity)` levels.
//!
//! The collector keeps about 1.7 million books (three per token: two replicas and what readers
//! hold), most a few dozen levels deep. A `BTreeMap` per side spends a 200-byte node on even a
//! one-level side; one exact-size vector per book needs 16 bytes a level, and a binary search
//! over it is as fast as a tree walk at these depths.

use crate::fixed::Fx;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    Bid,
    Ask,
}

/// `lv[..nb]` are the bids and `lv[nb..]` the asks, each in ascending price order, all with
/// non-zero quantity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Book {
    lv: Vec<(Fx, Fx)>,
    nb: usize,
}

impl Book {
    /// Levels may come in any order; a repeated price keeps its last quantity.
    pub fn from_levels(bids: &[(Fx, Fx)], asks: &[(Fx, Fx)]) -> Self {
        let mut lv = Vec::with_capacity(bids.len() + asks.len());
        let side = |l: &[(Fx, Fx)], lv: &mut Vec<(Fx, Fx)>| {
            let from = lv.len();
            lv.extend(l.iter().copied().filter(|&(_, q)| q > 0));
            lv[from..].sort_by_key(|&(p, _)| p);
            let mut keep = from;
            for i in from..lv.len() {
                if keep > from && lv[keep - 1].0 == lv[i].0 {
                    lv[keep - 1].1 = lv[i].1;
                } else {
                    lv[keep] = lv[i];
                    keep += 1;
                }
            }
            lv.truncate(keep);
        };
        side(bids, &mut lv);
        let nb = lv.len();
        side(asks, &mut lv);
        lv.shrink_to_fit();
        Self { lv, nb }
    }

    /// One side's levels, ascending by price.
    pub fn side(&self, side: Side) -> &[(Fx, Fx)] {
        match side {
            Side::Bid => &self.lv[..self.nb],
            Side::Ask => &self.lv[self.nb..],
        }
    }

    pub fn bids(&self) -> &[(Fx, Fx)] {
        self.side(Side::Bid)
    }

    pub fn asks(&self) -> &[(Fx, Fx)] {
        self.side(Side::Ask)
    }

    /// Quantity at `px`, 0 if there is no level.
    pub fn get(&self, side: Side, px: Fx) -> Fx {
        let s = self.side(side);
        s.binary_search_by_key(&px, |&(p, _)| p).map_or(0, |i| s[i].1)
    }

    /// Sets a level to an absolute quantity (0 removes it). Returns whether anything changed.
    pub fn apply(&mut self, side: Side, px: Fx, qty: Fx) -> bool {
        let (from, to) = match side {
            Side::Bid => (0, self.nb),
            Side::Ask => (self.nb, self.lv.len()),
        };
        match self.lv[from..to].binary_search_by_key(&px, |&(p, _)| p) {
            Ok(i) if qty == 0 => {
                self.lv.remove(from + i);
                if side == Side::Bid {
                    self.nb -= 1;
                }
                // Give memory back once a book has thinned out well below its peak.
                if self.lv.capacity() > 2 * self.lv.len() + 8 {
                    self.lv.shrink_to(self.lv.len() + self.lv.len() / 4);
                }
                true
            }
            Ok(i) => std::mem::replace(&mut self.lv[from + i].1, qty) != qty,
            Err(_) if qty == 0 => false,
            Err(i) => {
                if self.lv.len() == self.lv.capacity() {
                    // Grow by a quarter, not double: books grow slowly and there are many.
                    self.lv.reserve_exact((self.lv.len() / 4).max(4));
                }
                self.lv.insert(from + i, (px, qty));
                if side == Side::Bid {
                    self.nb += 1;
                }
                true
            }
        }
    }

    /// Level changes that turn `self` into `next`, as `(side, px, new_qty)`; `new_qty` 0 = removed.
    pub fn diff(&self, next: &Book) -> Vec<(Side, Fx, Fx)> {
        let mut out = Vec::new();
        for side in [Side::Bid, Side::Ask] {
            let (mut a, mut b) = (self.side(side).iter().peekable(), next.side(side).iter().peekable());
            loop {
                match (a.peek(), b.peek()) {
                    (Some(&&(pa, qa)), Some(&&(pb, qb))) if pa == pb => {
                        if qa != qb {
                            out.push((side, pb, qb));
                        }
                        a.next();
                        b.next();
                    }
                    (Some(&&(pa, _)), Some(&&(pb, _))) if pa < pb => {
                        out.push((side, pa, 0));
                        a.next();
                    }
                    (Some(&&(pa, _)), None) => {
                        out.push((side, pa, 0));
                        a.next();
                    }
                    (_, Some(&&(pb, qb))) => {
                        out.push((side, pb, qb));
                        b.next();
                    }
                    (None, None) => break,
                }
            }
        }
        out
    }

    pub fn best_bid(&self) -> Option<Fx> {
        self.bids().last().map(|l| l.0)
    }

    pub fn best_ask(&self) -> Option<Fx> {
        self.asks().first().map(|l| l.0)
    }

    /// Heap bytes held, for the memory report.
    pub fn heap_bytes(&self) -> usize {
        self.lv.capacity() * std::mem::size_of::<(Fx, Fx)>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    fn op() -> impl Strategy<Value = (bool, Fx, Fx)> {
        (any::<bool>(), 1i64..20, prop_oneof![Just(0i64), 1i64..5])
    }

    proptest! {
        // The book behaves like a naive hash map of levels.
        #[test]
        fn matches_naive_model(ops in proptest::collection::vec(op(), 0..200)) {
            let mut book = Book::default();
            let mut naive: HashMap<(bool, Fx), Fx> = HashMap::new();
            for (bid, px, q) in ops {
                let side = if bid { Side::Bid } else { Side::Ask };
                let before = naive.get(&(bid, px)).copied().unwrap_or(0);
                prop_assert_eq!(book.apply(side, px, q), before != q);
                if q == 0 { naive.remove(&(bid, px)); } else { naive.insert((bid, px), q); }
            }
            let mut flat: Vec<_> = naive.into_iter().collect();
            flat.sort();
            prop_assert!(book.bids().windows(2).all(|w| w[0].0 < w[1].0));
            prop_assert!(book.asks().windows(2).all(|w| w[0].0 < w[1].0));
            let mut got: Vec<_> = book.bids().iter().map(|&(p, q)| ((true, p), q))
                .chain(book.asks().iter().map(|&(p, q)| ((false, p), q))).collect();
            got.sort();
            prop_assert_eq!(got, flat);
        }

        // Applying diff(a, b) to a yields b.
        #[test]
        fn diff_applies(a in proptest::collection::vec(op(), 0..60), b in proptest::collection::vec(op(), 0..60)) {
            let build = |ops: &[(bool, Fx, Fx)]| {
                let mut bk = Book::default();
                for &(bid, px, q) in ops { bk.apply(if bid { Side::Bid } else { Side::Ask }, px, q); }
                bk
            };
            let (a, b) = (build(&a), build(&b));
            let mut x = a.clone();
            for (side, px, q) in a.diff(&b) { x.apply(side, px, q); }
            prop_assert_eq!(x, b);
        }

        // Building from unsorted levels keeps the last quantity per price and drops zeros.
        #[test]
        fn from_levels_sorts_and_dedups(ops in proptest::collection::vec(op(), 0..80)) {
            let (mut bids, mut asks) = (Vec::new(), Vec::new());
            for (bid, px, q) in ops {
                if bid { bids.push((px, q)) } else { asks.push((px, q)) }
            }
            let got = Book::from_levels(&bids, &asks);
            let last = |l: &[(Fx, Fx)]| {
                let mut m = std::collections::BTreeMap::new();
                for &(p, q) in l.iter().filter(|l| l.1 > 0) { m.insert(p, q); }
                m.into_iter().collect::<Vec<_>>()
            };
            prop_assert_eq!(got.bids(), &last(&bids)[..]);
            prop_assert_eq!(got.asks(), &last(&asks)[..]);
        }
    }
}
