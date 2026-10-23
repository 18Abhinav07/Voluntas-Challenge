//! Checks a collector output file. The strongest check is replay: each side's book, rebuilt
//! from its last snapshot plus the updates since, must equal the next snapshot exactly.
//!
//! Usage: `verify <file.sbe>`. Exits non-zero if any invariant is violated.

use std::collections::{BTreeMap, HashMap};

use engineering_challenge::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};

const ONE: i64 = 1_000_000;

#[derive(Default)]
struct Report {
    snapshots: u64,
    updates: u64,
    replay_ok: u64,
    replay_bad: u64,
    update_before_snapshot: u64,
    bad_price: u64,
    bad_qty: u64,
    unordered_snapshot: u64,
    two_sided_snapshot: u64,
    bad_symbol: u64,
    crossed: u64,
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).ok_or_else(|| anyhow::anyhow!("usage: verify <file.sbe>"))?;
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut rep = Report::default();
    let mut books: HashMap<String, BTreeMap<i64, i64>> = HashMap::new();
    let (mut first, mut last) = (i64::MAX, i64::MIN);
    let mut bad_examples = Vec::new();
    // recv - exch per update, µs: venue-to-us latency (includes clock offset; venue ts is ms).
    let mut lat: Vec<i64> = Vec::new();

    while let Some(msg) = r.read_msg()? {
        match msg {
            AnyMessage::Snapshot(s) => {
                let s = SnapshotDomain::try_from_decoder(s)?;
                rep.snapshots += 1;
                let t = s.received_at.timestamp_micros();
                (first, last) = (first.min(t), last.max(t));
                let is_bid = s.symbol.ends_with(":bid");
                if !is_bid && !s.symbol.ends_with(":ask") {
                    rep.bad_symbol += 1;
                }
                if !s.bids.is_empty() && !s.asks.is_empty() {
                    rep.two_sided_snapshot += 1;
                }
                let levels: Vec<(i64, i64)> = if is_bid {
                    s.bids.iter().map(|e| (e.price_level, e.quantity)).collect()
                } else {
                    s.asks.iter().map(|e| (e.price_level, e.quantity)).collect()
                };
                let ordered = levels.windows(2).all(|w| if is_bid { w[0].0 > w[1].0 } else { w[0].0 < w[1].0 });
                if !ordered {
                    rep.unordered_snapshot += 1;
                }
                for &(p, q) in &levels {
                    rep.bad_price += u64::from(!(0 < p && p < ONE));
                    rep.bad_qty += u64::from(q <= 0);
                }
                let snap: BTreeMap<i64, i64> = levels.into_iter().collect();
                if let Some(prev) = books.insert(s.symbol.clone(), snap.clone()) {
                    if prev == snap {
                        rep.replay_ok += 1;
                    } else {
                        rep.replay_bad += 1;
                        if bad_examples.len() < 5 {
                            bad_examples.push(format!(
                                "{} @ {t}: rebuilt {} levels, snapshot {}",
                                s.symbol,
                                prev.len(),
                                snap.len()
                            ));
                        }
                    }
                }
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(u)?;
                rep.updates += 1;
                let t = u.backtest_timestamp.timestamp_micros();
                (first, last) = (first.min(t), last.max(t));
                lat.push(t - u.exchange_timestamp.timestamp_micros());
                rep.bad_price += u64::from(!(0 < u.price_level && u.price_level < ONE));
                rep.bad_qty += u64::from(u.quantity < 0);
                let Some(b) = books.get_mut(&u.symbol) else {
                    rep.update_before_snapshot += 1;
                    continue;
                };
                if u.quantity == 0 {
                    b.remove(&u.price_level);
                } else {
                    b.insert(u.price_level, u.quantity);
                }
                if let Some((tok, _)) = u.symbol.rsplit_once(':') {
                    let bb = books.get(&format!("{tok}:bid")).and_then(|b| b.keys().next_back().copied());
                    let ba = books.get(&format!("{tok}:ask")).and_then(|b| b.keys().next().copied());
                    if let (Some(bb), Some(ba)) = (bb, ba) {
                        rep.crossed += u64::from(bb >= ba);
                    }
                }
            }
            _ => {}
        }
    }

    let secs = (last - first).max(0) as f64 / 1e6;
    println!("file:      {path}");
    println!("span:      {secs:.1}s, {} symbols", books.len());
    println!(
        "messages:  {} snapshots, {} updates ({:.0}/s)",
        rep.snapshots,
        rep.updates,
        rep.updates as f64 / secs.max(1e-9)
    );
    println!("replay:    {} snapshots matched the rebuilt book, {} did not", rep.replay_ok, rep.replay_bad);
    println!("crossed:   {} updates left a crossed book (informational)", rep.crossed);
    if !lat.is_empty() {
        lat.sort_unstable();
        let q = |f: f64| lat[((lat.len() - 1) as f64 * f) as usize] as f64 / 1e3;
        println!(
            "latency:   recv-exch ms p1 {:.0} p50 {:.0} p90 {:.0} p99 {:.0} p99.9 {:.0} max {:.0}",
            q(0.01),
            q(0.5),
            q(0.9),
            q(0.99),
            q(0.999),
            q(1.0)
        );
    }
    for e in &bad_examples {
        println!("  mismatch: {e}");
    }
    let violations = [
        ("replay mismatch", rep.replay_bad),
        ("update before snapshot", rep.update_before_snapshot),
        ("price outside (0,1)", rep.bad_price),
        ("bad quantity", rep.bad_qty),
        ("unordered snapshot", rep.unordered_snapshot),
        ("two-sided snapshot", rep.two_sided_snapshot),
        ("bad symbol", rep.bad_symbol),
    ];
    let mut failed = false;
    for (name, n) in violations {
        if n > 0 {
            println!("FAIL {name}: {n}");
            failed = true;
        }
    }
    if failed {
        std::process::exit(1);
    }
    println!("OK");
    Ok(())
}
