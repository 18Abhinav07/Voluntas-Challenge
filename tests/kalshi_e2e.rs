//! The collector binary with `--venue kalshi` against a mock of Kalshi's documented API on
//! localhost: `/markets` over HTTP, and a websocket that acknowledges `subscribe`, sends an
//! `orderbook_snapshot` per market, then signed `orderbook_delta`s, every message numbered by
//! its subscription's `seq`. Nothing here has been checked against the live venue.

use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use engineering_challenge::fixed;
use futures_util::{SinkExt, StreamExt};
use rand::{Rng, SeedableRng, rngs::StdRng};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tokio_tungstenite::tungstenite::Message;

const SCALE: i64 = 1_000_000;
/// Listed by `/markets?status=open`.
const LISTED: [&str; 6] = ["KX-A", "KX-B", "KX-C", "KX-D", "KX-E", "KX-F"];
/// Announced on the lifecycle channel only, then found by `/markets?tickers=`.
const NEW: &str = "KX-NEW";

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    /// Socket 0 skips one `seq` after this long.
    SeqGap(Duration),
    /// Every socket announces [`NEW`] after this long.
    Listing(Duration),
}

/// One side's resting sizes by price, YES or NO bids.
type Side = BTreeMap<i64, i64>;

struct Delta {
    /// Global order, so a socket sends only deltas newer than its snapshot.
    n: u64,
    ticker: String,
    msg: String,
}

struct Venue {
    books: Mutex<(u64, BTreeMap<String, [Side; 2]>)>,
    tx: broadcast::Sender<Arc<Delta>>,
    fault: Fault,
    accepts: AtomicUsize,
    /// `update_subscription` actions received, in order.
    actions: Mutex<Vec<String>>,
}

fn px(p: i64) -> String {
    fixed::to_string(p)
}

fn levels(s: &Side) -> String {
    let v: Vec<_> = s.iter().map(|(p, q)| format!("[\"{}\",\"{}\"]", px(*p), px(*q))).collect();
    format!("[{}]", v.join(","))
}

impl Venue {
    fn new(fault: Fault) -> Arc<Self> {
        let mut books = BTreeMap::new();
        for t in LISTED.iter().chain([&NEW]) {
            let side = || (0..4).map(|i| (300_000 + i * 20_000, 100 * SCALE)).collect::<Side>();
            books.insert(t.to_string(), [side(), side()]);
        }
        Arc::new(Self {
            books: Mutex::new((0, books)),
            tx: broadcast::channel(1 << 16).0,
            fault,
            accepts: 0.into(),
            actions: Mutex::new(Vec::new()),
        })
    }

    /// One delta every 10 ms. YES and NO bids both stay at or below 0.49, so YES offers
    /// (`1 - NO bid`) stay at or above 0.51 and the book never crosses.
    async fn generate(self: Arc<Self>) {
        let mut rng = StdRng::seed_from_u64(11);
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            tick.tick().await;
            let mut g = self.books.lock().unwrap();
            g.0 += 1;
            let n = g.0;
            let ticker = g.1.keys().nth(n as usize % g.1.len()).unwrap().clone();
            let no = rng.gen_bool(0.5);
            let p = rng.gen_range(30..50) * 10_000;
            let side = &mut g.1.get_mut(&ticker).unwrap()[no as usize];
            let have = side.get(&p).copied().unwrap_or(0);
            let d = if have > 0 && rng.gen_bool(0.4) {
                -(if rng.gen_bool(0.5) { have } else { rng.gen_range(1..=have / SCALE) * SCALE })
            } else {
                rng.gen_range(1..300) * SCALE
            };
            match have + d {
                0 => side.remove(&p),
                q => side.insert(p, q),
            };
            let delta = if d < 0 { format!("-{}", px(-d)) } else { px(d) };
            let ts = chrono::Utc::now().timestamp_millis();
            let msg = format!(
                r#"{{"market_ticker":"{ticker}","price_dollars":"{}","delta_fp":"{delta}","side":"{}","ts_ms":{ts}}}"#,
                px(p),
                if no { "no" } else { "yes" }
            );
            let _ = self.tx.send(Arc::new(Delta { n, ticker, msg }));
        }
    }

    async fn serve_ws(self: Arc<Self>, tcp: TcpStream) {
        let k = self.accepts.fetch_add(1, Ordering::SeqCst);
        let mut rx = self.tx.subscribe();
        let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await else { return };
        // This socket's orderbook subscription is sid 1, lifecycle sid 2; `seq` is per sid.
        let (mut seq, mut since): (u64, BTreeMap<String, u64>) = (0, BTreeMap::new());
        let mut out: Vec<String> = Vec::new();
        let at = |f: Fault| match (f, self.fault) {
            (Fault::SeqGap(_), Fault::SeqGap(d)) if k == 0 => Some(d),
            (Fault::Listing(_), Fault::Listing(d)) => Some(d),
            _ => None,
        };
        let gap = at(Fault::SeqGap(Duration::ZERO));
        let listing = at(Fault::Listing(Duration::ZERO));
        let start = tokio::time::Instant::now();
        let (mut gapped, mut listed) = (false, false);
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                m = ws.next() => match m {
                    Some(Ok(Message::Text(t))) => {
                        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                        let (id, p) = (v["id"].as_u64().unwrap(), &v["params"]);
                        let tickers: Vec<String> = p["market_tickers"].as_array().map_or_else(Vec::new, |a| {
                            a.iter().map(|x| x.as_str().unwrap().to_string()).collect()
                        });
                        let snapshot = |t: &str, seq: &mut u64, since: &mut BTreeMap<String, u64>| {
                            let g = self.books.lock().unwrap();
                            let [yes, no] = &g.1[t];
                            since.insert(t.to_string(), g.0);
                            *seq += 1;
                            format!(
                                r#"{{"type":"orderbook_snapshot","sid":1,"seq":{seq},"msg":{{"market_ticker":"{t}","yes_dollars_fp":{},"no_dollars_fp":{}}}}}"#,
                                levels(yes),
                                levels(no)
                            )
                        };
                        match v["cmd"].as_str().unwrap() {
                            "subscribe" if p["channels"][0] == "market_lifecycle_v2" => {
                                out.push(format!(r#"{{"id":{id},"type":"subscribed","msg":{{"channel":"market_lifecycle_v2","sid":2}}}}"#));
                            }
                            "subscribe" => {
                                out.push(format!(r#"{{"id":{id},"type":"subscribed","msg":{{"channel":"orderbook_delta","sid":1}}}}"#));
                                for t in &tickers {
                                    out.push(snapshot(t, &mut seq, &mut since));
                                }
                            }
                            "update_subscription" => {
                                let action = p["action"].as_str().unwrap();
                                self.actions.lock().unwrap().push(format!("{action} {}", tickers.join(",")));
                                match action {
                                    "delete_markets" => {
                                        tickers.iter().for_each(|t| { since.remove(t); });
                                        seq += 1;
                                        out.push(format!(r#"{{"id":{id},"type":"ok","sid":1,"seq":{seq},"msg":{{}}}}"#));
                                    }
                                    _ => {
                                        for t in &tickers {
                                            out.push(snapshot(t, &mut seq, &mut since));
                                        }
                                    }
                                }
                            }
                            other => panic!("unexpected command {other}"),
                        }
                    }
                    Some(Ok(_)) => {}
                    _ => return,
                },
                d = rx.recv() => {
                    let Ok(d) = d else { return };
                    if since.get(&d.ticker).is_some_and(|s| d.n > *s) {
                        seq += 1;
                        if gap.is_some_and(|g| start.elapsed() >= g) && !gapped {
                            gapped = true;
                            seq += 1;
                        }
                        out.push(format!(r#"{{"type":"orderbook_delta","sid":1,"seq":{seq},"msg":{}}}"#, d.msg));
                    }
                }
                _ = tick.tick() => {
                    if listing.is_some_and(|l| start.elapsed() >= l) && !listed {
                        listed = true;
                        out.push(format!(r#"{{"type":"market_lifecycle_v2","sid":2,"msg":{{"market_ticker":"{NEW}","event_type":"activated"}}}}"#));
                    }
                }
            }
            for f in out.drain(..) {
                if ws.send(Message::Text(f)).await.is_err() {
                    return;
                }
            }
        }
    }

    async fn serve_http(self: Arc<Self>, mut tcp: TcpStream) {
        let mut req = Vec::new();
        let mut buf = [0u8; 4096];
        while !req.windows(4).any(|w| w == b"\r\n\r\n") {
            match tcp.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => req.extend_from_slice(&buf[..n]),
            }
        }
        let line = String::from_utf8_lossy(&req).lines().next().unwrap_or_default().to_owned();
        let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
        let wanted: Option<HashSet<String>> = path
            .split(['?', '&'])
            .find_map(|kv| kv.strip_prefix("tickers="))
            .map(|v| v.split("%2C").flat_map(|s| s.split(',')).map(String::from).collect());
        let market = |t: &str| serde_json::json!({"ticker": t, "event_ticker": "KX", "status": "active"});
        let markets: Vec<_> = match &wanted {
            Some(w) => LISTED.iter().chain([&NEW]).filter(|t| w.contains(**t)).map(|t| market(t)).collect(),
            None => LISTED.iter().map(|t| market(t)).collect(),
        };
        let (status, body) = if path.starts_with("/markets") {
            ("200 OK", serde_json::json!({"markets": markets, "cursor": ""}).to_string())
        } else {
            ("404 Not Found", String::new())
        };
        let resp = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = tcp.write_all(resp.as_bytes()).await;
        let _ = tcp.shutdown().await;
    }
}

struct Run {
    log: String,
    verify: String,
    verify_ok: bool,
    accepts: usize,
    actions: Vec<String>,
}

fn run(fault: Fault, secs: u64) -> Run {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let venue = Venue::new(fault);
    let (ws, http) = rt.block_on(async {
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ports = (ws.local_addr().unwrap().port(), http.local_addr().unwrap().port());
        tokio::spawn(venue.clone().generate());
        let v = venue.clone();
        tokio::spawn(async move {
            while let Ok((s, _)) = ws.accept().await {
                tokio::spawn(v.clone().serve_ws(s));
            }
        });
        let v = venue.clone();
        tokio::spawn(async move {
            while let Ok((s, _)) = http.accept().await {
                tokio::spawn(v.clone().serve_http(s));
            }
        });
        ports
    });
    let dir = tempfile::tempdir().unwrap();
    let (out, log) = (dir.path().join("ob.sbe"), dir.path().join("log.txt"));
    let st = Command::new(env!("CARGO_BIN_EXE_collector"))
        .args(["--venue", "kalshi"])
        .args(["--ws-url", &format!("ws://127.0.0.1:{ws}/trade-api/ws/v2")])
        .args(["--rest-url", &format!("http://127.0.0.1:{http}")])
        .arg("--out")
        .arg(&out)
        .arg("--universe-cache")
        .arg(dir.path().join("universe.json"))
        .args(["--recycle-mins", "0", "--core-threads", "1", "--ntp-server", ""])
        .args(["--duration-secs", &secs.to_string()])
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .env_remove("KALSHI_API_KEY_ID")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .status()
        .unwrap();
    assert!(st.success(), "collector failed: {}", std::fs::read_to_string(&log).unwrap());
    let (verify_ok, verify) = verify(&out);
    let log = std::fs::read_to_string(&log).unwrap();
    if std::env::var_os("E2E_LOG").is_some() {
        eprintln!("{log}\n{verify}");
    }
    let actions = venue.actions.lock().unwrap().clone();
    Run { log, verify, verify_ok, accepts: venue.accepts.load(Ordering::SeqCst), actions }
}

fn verify(path: &Path) -> (bool, String) {
    let o = Command::new(env!("CARGO_BIN_EXE_verify")).arg(path).output().unwrap();
    (o.status.success(), String::from_utf8_lossy(&o.stdout).into_owned())
}

/// The last `done` line's counter.
fn metric(log: &str, name: &str) -> u64 {
    let line = log.lines().rev().find(|l| l.contains(" done ")).expect("no done line");
    let key = format!(" {name}: ");
    let at = line.find(&key).unwrap_or_else(|| panic!("no {name} in {line}")) + key.len();
    line[at..].chars().take_while(char::is_ascii_digit).collect::<String>().parse().unwrap()
}

fn assert_clean(r: &Run) {
    assert!(r.verify_ok, "verify failed:\n{}", r.verify);
    assert!(r.verify.contains("0 did not"), "{}", r.verify);
    assert!(metric(&r.log, "updates") > 100, "too few updates: {}", metric(&r.log, "updates"));
}

#[test]
fn kalshi_baseline() {
    let r = run(Fault::None, 8);
    assert_clean(&r);
    assert_eq!(r.accepts, 2, "two replicas, no reconnect");
    assert_eq!(metric(&r.log, "disconnects"), 0);
}

#[test]
fn kalshi_seq_gap_reconnects_and_the_output_stays_exact() {
    let r = run(Fault::SeqGap(Duration::from_secs(3)), 10);
    assert_clean(&r);
    assert!(r.log.contains("seq") && r.log.contains("connection lost"), "{}", r.log);
    assert!(r.accepts >= 3, "socket 0 reconnects after the gap: {} accepts", r.accepts);
}

#[test]
fn kalshi_lifecycle_listing_joins_the_existing_subscription() {
    let r = run(Fault::Listing(Duration::from_secs(3)), 10);
    assert_clean(&r);
    assert!(r.log.contains(&format!("market={NEW}")) && r.log.contains("new market listed"), "{}", r.log);
    assert!(r.actions.iter().any(|a| a == &format!("add_markets {NEW}")), "{:?}", r.actions);
}
