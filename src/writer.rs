//! The only owner of the output file: a dedicated OS thread fed by a bounded channel, so
//! disk latency never stalls the async runtime and nothing is ever dropped (§5.7).

use std::{path::PathBuf, thread::JoinHandle};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::{
    adapter::{Sym, Ts},
    book::Side,
    fixed::Fx,
    util::{
        AlignedSbeWriter,
        dtos::challenge_orderbook::{
            PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
        },
    },
};

/// The reader is fixed at 64 MiB chunks.
pub const CHUNK: usize = 64 << 20;

/// One output message. `sym` is already the side-symbol (`<token>:bid` / `<token>:ask`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rec {
    Update { sym: Sym, recv: Ts, exch: Ts, px: Fx, qty: Fx },
    /// One side of one book, best level first.
    Snapshot { sym: Sym, recv: Ts, exch: Ts, side: Side, levels: Vec<(Fx, Fx)> },
}

pub type Batch = Vec<Rec>;

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub updates: u64,
    pub snapshots: u64,
}

fn ts(us: Ts) -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(us).ok_or_else(|| anyhow::anyhow!("timestamp out of range: {us}"))
}

/// Starts the writer thread. It exits after the last sender is dropped and the channel is
/// drained, flushing the final chunk and syncing the file.
pub fn spawn(path: PathBuf, cap: usize) -> anyhow::Result<(mpsc::Sender<Batch>, JoinHandle<anyhow::Result<Stats>>)> {
    let target = if path.as_os_str() == "-" { PathBuf::from("/dev/stdout") } else { path };
    let mut w = AlignedSbeWriter::create(&target, CHUNK)?;
    let (tx, mut rx) = mpsc::channel::<Batch>(cap);
    let h = std::thread::Builder::new().name("writer".into()).spawn(move || {
        let mut stats = Stats::default();
        // Reused across messages: the symbol `String` is the only per-message allocation.
        let mut upd = PriceLevelUpdateDomain {
            backtest_timestamp: DateTime::UNIX_EPOCH,
            exchange_timestamp: DateTime::UNIX_EPOCH,
            price_level: 0,
            quantity: 0,
            symbol: String::new(),
        };
        while let Some(batch) = rx.blocking_recv() {
            for rec in batch {
                match rec {
                    Rec::Update { sym, recv, exch, px, qty } => {
                        upd.backtest_timestamp = ts(recv)?;
                        upd.exchange_timestamp = ts(exch)?;
                        upd.price_level = px;
                        upd.quantity = qty;
                        upd.symbol.clear();
                        upd.symbol.push_str(&sym);
                        w.write_msg(&upd)?;
                        stats.updates += 1;
                    }
                    Rec::Snapshot { sym, recv, exch, side, levels } => {
                        let (bids, asks) = match side {
                            Side::Bid => (
                                levels
                                    .iter()
                                    .map(|&(price_level, quantity)| SnapshotBidsEntryDomain { price_level, quantity })
                                    .collect(),
                                Vec::new(),
                            ),
                            Side::Ask => (
                                Vec::new(),
                                levels
                                    .iter()
                                    .map(|&(price_level, quantity)| SnapshotAsksEntryDomain { price_level, quantity })
                                    .collect(),
                            ),
                        };
                        let snap = SnapshotDomain {
                            received_at: ts(recv)?,
                            exchange_timestamp: ts(exch)?,
                            bids,
                            asks,
                            symbol: sym.to_string(),
                        };
                        w.write_msg(&snap)?;
                        stats.snapshots += 1;
                    }
                }
            }
        }
        w.flush()?;
        drop(w);
        if target != std::path::Path::new("/dev/stdout") {
            std::fs::OpenOptions::new().write(true).open(&target)?.sync_all()?;
        }
        Ok(stats)
    })?;
    Ok((tx, h))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage};

    #[test]
    fn round_trips_through_template_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.sbe");
        let (tx, h) = spawn(path.clone(), 4).unwrap();
        let sym: Sym = "123:bid".into();
        tx.blocking_send(vec![
            Rec::Snapshot { sym: sym.clone(), recv: 2, exch: 1, side: Side::Bid, levels: vec![(500_000, 10), (400_000, 20)] },
            Rec::Update { sym: sym.clone(), recv: 4, exch: 3, px: 500_000, qty: 0 },
        ])
        .unwrap();
        drop(tx);
        let stats = h.join().unwrap().unwrap();
        assert_eq!((stats.snapshots, stats.updates), (1, 1));

        let mut r = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
        let Some(AnyMessage::Snapshot(s)) = r.read_msg().unwrap() else { panic!("expected snapshot") };
        let s = SnapshotDomain::try_from_decoder(s).unwrap();
        assert_eq!((s.symbol.as_str(), s.bids.len(), s.asks.len()), ("123:bid", 2, 0));
        assert_eq!(s.exchange_timestamp.timestamp_micros(), 1);
        let Some(AnyMessage::PriceLevelUpdate(u)) = r.read_msg().unwrap() else { panic!("expected update") };
        let u = PriceLevelUpdateDomain::try_from_decoder(u).unwrap();
        assert_eq!((u.price_level, u.quantity, u.backtest_timestamp.timestamp_micros()), (500_000, 0, 4));
        assert!(r.read_msg().unwrap().is_none());
    }
}
