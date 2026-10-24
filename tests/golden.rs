//! Replays live captures (2026-10-23) through `decode` -> book apply -> `HashCheck` and
//! checks the hash-verification rates match the Python reference (`probe/verify_hash.py`).

use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    time::{Duration, Instant},
};

use engineering_challenge::{
    adapter::{
        BookView, ExchangeAdapter, Instrument, NormEvent, Sym, Verdict,
        polymarket::{Meta, Polymarket},
    },
    book::Book,
    fixed,
};

struct Books(HashMap<Sym, Book>);

impl BookView for Books {
    fn book(&self, token: &str) -> Option<&Book> {
        self.0.get(token)
    }
}

fn instruments(path: &str) -> Vec<Instrument<Meta>> {
    let v: Vec<serde_json::Value> =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    v.iter()
        .map(|m| {
            let tokens: Vec<String> =
                serde_json::from_str(m["clobTokenIds"].as_str().unwrap()).unwrap();
            let fx = |x: &serde_json::Value| {
                fixed::to_string((x.as_f64().unwrap() * 1e6).round() as i64)
            };
            Instrument {
                market: m["conditionId"].as_str().unwrap().into(),
                tokens: tokens.iter().map(|t| t.as_str().into()).collect(),
                meta: Meta {
                    neg_risk: m["negRisk"].as_bool().unwrap(),
                    tick: fx(&m["orderPriceMinTickSize"]),
                    min_size: fx(&m["orderMinSize"]),
                },
            }
        })
        .collect()
}

#[derive(Debug, Default)]
struct Rates {
    ok: usize,
    miss: usize,
    book_miss: usize,
}

fn replay(frames: &str, markets: &str) -> Rates {
    let pm = Polymarket::new().unwrap();
    let mut check = pm.integrity();
    for i in instruments(markets) {
        check.register(&i);
    }
    let mut books = Books(HashMap::new());
    let (mut evs, mut out) = (Vec::new(), Vec::new());
    let t0 = Instant::now();
    let file = std::fs::File::open(frames).unwrap();
    for (n, line) in BufReader::new(flate2::read::GzDecoder::new(file))
        .lines()
        .enumerate()
    {
        let line: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        evs.clear();
        // Event time stands still within the capture; deferrals resolve on the next trade.
        let now = t0 + Duration::from_millis(n as u64);
        if let Err(e) = pm.decode(&mut (), line["raw"].as_str().unwrap(), &mut evs) {
            panic!("decode failed on line {n}: {e:#}");
        }
        for ev in &evs {
            check.pre_event(0, ev, &books, now, &mut out);
            match ev {
                NormEvent::Book {
                    token, bids, asks, ..
                } => {
                    books.0.insert(token.clone(), Book::from_levels(bids, asks));
                }
                NormEvent::Levels { changes, .. } => {
                    for c in changes {
                        if let Some(b) = books.0.get_mut(&c.token) {
                            b.apply(c.side, c.px, c.qty);
                        }
                    }
                }
                _ => {}
            }
            check.on_event(0, ev, &books, now, &mut out);
        }
    }
    check.on_tick(t0 + Duration::from_secs(3600), &mut out);
    let mut r = Rates::default();
    for v in out {
        match v {
            Verdict::Ok { .. } => r.ok += 1,
            Verdict::Miss { .. } => r.miss += 1,
            Verdict::BookMiss { .. } => r.book_miss += 1,
        }
    }
    r
}

fn fx(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn quiet_capture_all_hashes_verify() {
    let r = replay(&fx("frames.jsonl.gz"), &fx("top.json"));
    eprintln!("{r:?}");
    assert_eq!((r.ok, r.miss, r.book_miss), (538 + 14, 0, 0));
}

#[test]
fn active_capture_all_hashes_verify() {
    let r = replay(&fx("frames_active.jsonl.gz"), &fx("active.json"));
    eprintln!("{r:?}");
    assert_eq!((r.ok, r.miss, r.book_miss), (326 + 6, 0, 0));
}

#[test]
fn trade_heavy_capture_matches_reference() {
    let r = replay(&fx("frames_trades.jsonl.gz"), &fx("trades.json"));
    let rate = r.ok as f64 / (r.ok + r.miss) as f64;
    eprintln!("{r:?} rate={rate:.4}");
    // Every entry and book gets exactly one verdict: 74,438 price_change entries + 672 books.
    assert_eq!(r.ok + r.miss + r.book_miss, 75_110);
    // Python reference: 73620/74438 = 98.9%, 2 book misses.
    assert!(rate >= 0.988, "{rate}");
    assert!(r.book_miss <= 2);
}
