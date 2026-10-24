//! Offline benchmarks for the performance work (docs/PERFORMANCE.md).
//!
//! `cargo run --release --example bench -- mem [markets]`: fills a synthetic universe the size
//! of Polymarket's (default 283,000 markets, two tokens each) through the real core and hash
//! checker, every book on both replicas at a depth drawn from the captured snapshots, and
//! reports heap bytes per token and RSS.
//!
//! `cargo run --release --example bench -- cpu`: replays the captured live frames
//! (`tests/fixtures/frames_trades.jsonl.gz`) through `decode`, then through the core on two
//! replicas, and reports ns per frame and ns per event.

use std::{
    alloc::{GlobalAlloc, Layout},
    io::{BufRead, BufReader},
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
    time::Instant,
};

use engineering_challenge::{
    adapter::{
        ConnId, ExchangeAdapter, Instrument, NormEvent, Sym,
        polymarket::{Meta, Polymarket},
    },
    conn::ConnCmd,
    core::{Config, Core, Effects},
    fixed,
};

/// Counts live heap bytes over the same allocator the collector uses.
struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size() as isize, Ordering::Relaxed);
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { mimalloc::MiMalloc.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed);
        unsafe { mimalloc::MiMalloc.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        LIVE.fetch_add(new as isize - l.size() as isize, Ordering::Relaxed);
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { mimalloc::MiMalloc.realloc(p, l, new) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Default)]
struct Spawned(Vec<ConnId>);

impl Effects for Spawned {
    fn spawn_conn(&mut self, id: ConnId, _: usize, _: Vec<Sym>) {
        self.0.push(id);
    }
    fn send(&mut self, _: ConnId, _: ConnCmd) {}
    fn lookup(&mut self, _: Sym) {}
}

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

/// The `raw` frames of a capture.
fn frames(name: &str) -> Vec<String> {
    let file = std::fs::File::open(fixture(name)).expect("fixture");
    BufReader::new(flate2::read::GzDecoder::new(file))
        .lines()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(&l.unwrap()).unwrap();
            v["raw"].as_str().unwrap().to_owned()
        })
        .collect()
}

fn instruments(name: &str) -> Vec<Instrument<Meta>> {
    let v: Vec<serde_json::Value> = serde_json::from_str(&std::fs::read_to_string(fixture(name)).unwrap()).unwrap();
    let fx = |x: &serde_json::Value| fixed::to_string((x.as_f64().unwrap() * 1e6).round() as i64);
    v.iter()
        .map(|m| {
            let tokens: Vec<String> = serde_json::from_str(m["clobTokenIds"].as_str().unwrap()).unwrap();
            Instrument {
                market: m["conditionId"].as_str().unwrap().into(),
                tokens: tokens.iter().map(|t| t.as_str().into()).collect(),
                meta: Meta { neg_risk: m["negRisk"].as_bool().unwrap(), tick: fx(&m["orderPriceMinTickSize"]), min_size: fx(&m["orderMinSize"]) },
            }
        })
        .collect()
}

fn rss_mb() -> f64 {
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps").args(["-o", "rss=", "-p", &pid]).output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().parse::<f64>().unwrap_or(0.0) / 1024.0
}

fn core(insts: Vec<Instrument<Meta>>) -> (Core<Meta>, Vec<ConnId>) {
    let pm = Polymarket::new().unwrap();
    let mut core = Core::new(Config::default(), pm.integrity());
    let mut fx = Spawned::default();
    core.add(insts, &mut fx);
    core.flush_pending(Instant::now(), &mut fx);
    let now = Instant::now();
    for &c in &fx.0 {
        core.on_up(c, now);
    }
    (core, fx.0)
}

/// Levels per side of every book in the captures, as (bids, asks).
fn depths() -> Vec<(usize, usize)> {
    let pm = Polymarket::new().unwrap();
    let mut evs = Vec::new();
    let mut out = Vec::new();
    for name in ["frames.jsonl.gz", "frames_active.jsonl.gz", "frames_trades.jsonl.gz"] {
        for f in frames(name) {
            evs.clear();
            let _ = pm.decode(&mut (), &f, &mut evs);
            for e in &evs {
                if let NormEvent::Book { bids, asks, .. } = e {
                    out.push((bids.len(), asks.len()));
                }
            }
        }
    }
    out
}

fn pct(v: &mut [usize], q: f64) -> usize {
    v.sort_unstable();
    v[((v.len() - 1) as f64 * q) as usize]
}

fn mem(markets: usize) {
    let d = depths();
    let mut all: Vec<usize> = d.iter().flat_map(|&(b, a)| [b, a]).collect();
    println!(
        "depth from {} captured books: levels/side p50 {} p90 {} p99 {} max {}",
        d.len(),
        pct(&mut all, 0.5),
        pct(&mut all, 0.9),
        pct(&mut all, 0.99),
        pct(&mut all, 1.0)
    );
    let mut rng = 0x9e3779b97f4a7c15u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let id = |n: &mut dyn FnMut() -> u64| -> String { (0..5).map(|_| format!("{:019}", n() % 10u64.pow(19))).collect::<String>()[..77].to_owned() };
    let insts: Vec<Instrument<Meta>> = (0..markets)
        .map(|_| Instrument {
            market: format!("0x{:016x}{:016x}{:016x}{:016x}", next(), next(), next(), next()).into(),
            tokens: vec![id(&mut next).into(), id(&mut next).into()],
            meta: Meta { neg_risk: false, tick: "0.01".into(), min_size: "5".into() },
        })
        .collect();
    let tokens = 2 * markets;
    let base = LIVE.load(Ordering::Relaxed);
    let rss0 = rss_mb();
    let t = Instant::now();
    let (mut core, conns) = core(insts.clone());
    let after_add = LIVE.load(Ordering::Relaxed);
    // Each socket gets a book for every token of its shard, in subscribe order.
    let fx = &mut Spawned::default();
    let now = Instant::now();
    let mut k = 0usize;
    let shard_of = |i: usize| i * 2 / Config::default().tokens_per_conn;
    let mut evs = Vec::with_capacity(2);
    for (i, inst) in insts.iter().enumerate() {
        let shard = shard_of(i);
        for t in &inst.tokens {
            let (nb, na) = d[k % d.len()];
            k += 1;
            let lv = |n: usize, from: i64, step: i64| (0..n as i64).map(|j| (from + step * j, 1_000_000 + j * 37_000)).collect::<Vec<_>>();
            evs.push(NormEvent::Book {
                token: t.clone(),
                market: inst.market.clone(),
                bids: lv(nb, 10_000, 10_000),
                asks: lv(na, 990_000, -10_000),
                exch_ts: 1,
                check: None,
                tick: None,
                last_trade: Some("0.500".into()),
            });
        }
        for r in 0..2 {
            if let Some(&c) = conns.get(shard * 2 + r) {
                core.on_events(c, 1, &evs, now, fx);
            }
        }
        evs.clear();
        core.out.clear();
    }
    let live = LIVE.load(Ordering::Relaxed) - base;
    println!(
        "mem: {markets} markets, {tokens} tokens, {} sockets, filled in {:.1}s (sourced {})",
        conns.len(),
        t.elapsed().as_secs_f64(),
        core.sourced_count()
    );
    println!(
        "mem: heap {:.0} MB = {:.0} B/token (empty books {:.0} B/token); RSS {:.0} MB (+{:.0} MB)",
        live as f64 / 1e6,
        live as f64 / tokens as f64,
        (after_add - base) as f64 / tokens as f64,
        rss_mb(),
        rss_mb() - rss0
    );
    drop(insts);
    std::hint::black_box(&core);
}

fn cpu() {
    let raw = frames("frames_trades.jsonl.gz");
    let bytes: usize = raw.iter().map(String::len).sum();
    let pm = Polymarket::new().unwrap();
    let mut evs = Vec::new();
    let mut decoded: Vec<Vec<NormEvent>> = Vec::with_capacity(raw.len());
    for f in &raw {
        let mut v = Vec::new();
        let _ = pm.decode(&mut (), f, &mut v);
        decoded.push(v);
    }
    let n_ev: usize = decoded.iter().map(Vec::len).sum();
    let mut best = f64::MAX;
    for _ in 0..5 {
        let t = Instant::now();
        for f in &raw {
            evs.clear();
            let _ = pm.decode(&mut (), f, &mut evs);
        }
        best = best.min(t.elapsed().as_secs_f64());
    }
    println!(
        "decode: {} frames, {:.1} MB, {} events: {:.0} ns/frame, {:.0} MB/s",
        raw.len(),
        bytes as f64 / 1e6,
        n_ev,
        best * 1e9 / raw.len() as f64,
        bytes as f64 / best / 1e6
    );

    let mut best = f64::MAX;
    let mut allocs = 0;
    for _ in 0..5 {
        let (mut core, conns) = core(instruments("trades.json"));
        let fx = &mut Spawned::default();
        let now = Instant::now();
        let a0 = ALLOCS.load(Ordering::Relaxed);
        let t = Instant::now();
        for (i, evs) in decoded.iter().enumerate() {
            for &c in &conns {
                core.on_events(c, i as i64, evs, now, fx);
            }
            core.out.clear();
        }
        let el = t.elapsed().as_secs_f64();
        if el < best {
            best = el;
            allocs = ALLOCS.load(Ordering::Relaxed) - a0;
        }
        std::hint::black_box(&core);
    }
    let applied = n_ev * 2;
    println!(
        "core: {applied} event applications (2 replicas): {:.0} ns/event, {:.2} allocs/event",
        best * 1e9 / applied as f64,
        allocs as f64 / applied as f64
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("mem") => mem(args.get(2).and_then(|s| s.parse().ok()).unwrap_or(283_000)),
        Some("cpu") => cpu(),
        _ => eprintln!("usage: bench mem [markets] | bench cpu"),
    }
}
