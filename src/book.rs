//! One L2 book: two sorted price -> absolute-quantity maps.

use std::collections::BTreeMap;

use crate::fixed::Fx;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Book {
    pub bids: BTreeMap<Fx, Fx>,
    pub asks: BTreeMap<Fx, Fx>,
}

impl Book {
    pub fn from_levels(bids: &[(Fx, Fx)], asks: &[(Fx, Fx)]) -> Self {
        let keep = |l: &[(Fx, Fx)]| l.iter().copied().filter(|&(_, q)| q > 0).collect();
        Self {
            bids: keep(bids),
            asks: keep(asks),
        }
    }

    pub fn side(&self, side: Side) -> &BTreeMap<Fx, Fx> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    /// Sets a level to an absolute quantity (0 removes it). Returns whether anything changed.
    pub fn apply(&mut self, side: Side, px: Fx, qty: Fx) -> bool {
        let map = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if qty == 0 {
            map.remove(&px).is_some()
        } else {
            map.insert(px, qty) != Some(qty)
        }
    }

    /// Level changes that turn `self` into `next`, as `(side, px, new_qty)`; `new_qty` 0 = removed.
    pub fn diff(&self, next: &Book) -> Vec<(Side, Fx, Fx)> {
        let mut out = Vec::new();
        for side in [Side::Bid, Side::Ask] {
            let (a, b) = (self.side(side), next.side(side));
            for (&px, &q) in a {
                if b.get(&px) != Some(&q) {
                    out.push((side, px, b.get(&px).copied().unwrap_or(0)));
                }
            }
            for (&px, &q) in b {
                if !a.contains_key(&px) {
                    out.push((side, px, q));
                }
            }
        }
        out
    }

    pub fn best_bid(&self) -> Option<Fx> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Fx> {
        self.asks.keys().next().copied()
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
        // The BTreeMap book behaves like a naive hash map of levels.
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
            let mut got: Vec<_> = book.bids.iter().map(|(&p, &q)| ((true, p), q))
                .chain(book.asks.iter().map(|(&p, &q)| ((false, p), q))).collect();
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
    }
}
