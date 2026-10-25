//! End-to-end fault tests: the collector binary against a mock venue on localhost.
//!
//! The mock lists `MARKETS` markets on a Gamma-like HTTP endpoint and serves a market websocket
//! with Polymarket's frames: a `book` per token on subscribe, then `price_change` deltas carrying
//! correct hashes. Each test injects one fault, mostly on socket 0, which becomes every token's
//! source (the other sockets get their books 500 ms later). It then checks the collector's
//! reaction in its final metrics, and that `verify` passes on the output.

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use engineering_challenge::fixed;
use futures_util::{SinkExt, StreamExt};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sha1::{Digest, Sha1};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
};
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

const MARKETS: usize = 10;
const SCALE: i64 = 1_000_000;
/// The token a stall fault silences.
const STALLED: &str = "1000";

#[derive(Clone, Copy)]
enum Fault {
    None,
    /// Socket 0's TCP connection is dropped, with no close frame.
    Drop { after: Duration },
    /// Socket 0 stops sending anything, PONGs included, but stays open.
    Silent { after: Duration },
    /// Socket 0 stops sending updates for one token ([`STALLED`]).
    Stall { after: Duration },
    /// Socket 0 delivers every frame this much later.
    Delay { after: Duration, by: Duration },
    /// Socket 0 sends one update with a wrong size (and the true book's hash).
    Corrupt { after: Duration },
    /// Every socket closes with the venue's "all subscribed assets resolved" reason.
    Resolved { after: Duration },
    /// The first this many Gamma requests get 429 with `Retry-After: 1`.
    Gamma429(usize),
}

impl Fault {
    fn after(self) -> Option<Duration> {
        match self {
            Fault::Drop { after }
            | Fault::Silent { after }
            | Fault::Stall { after }
            | Fault::Delay { after, .. }
            | Fault::Corrupt { after }
            | Fault::Resolved { after } => Some(after),
            Fault::None | Fault::Gamma429(_) => None,
        }
    }
}

struct Tok {
    market: String,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

struct State {
    toks: BTreeMap<String, Tok>,
    seq: u64,
    ts_ms: i64,
}

struct Ev {
    seq: u64,
    token: String,
    frame: String,
    /// The same update with a wrong size, for the corrupt fault.
    bad: String,
}

struct Venue {
    state: Mutex<State>,
    tx: broadcast::Sender<Arc<Ev>>,
    fault: Fault,
    accepts: AtomicUsize,
    gamma_requests: AtomicUsize,
}

fn markets() -> Vec<(String, [String; 2])> {
    (0..MARKETS).map(|m| (format!("0xm{m:02}"), [format!("{}", 1000 + 2 * m), format!("{}", 1001 + 2 * m)])).collect()
}

fn levels(it: &mut dyn Iterator<Item = (&i64, &i64)>) -> String {
    let v: Vec<_> = it
        .map(|(p, q)| format!("{{\"price\":\"{}\",\"size\":\"{}\"}}", fixed::to_string(*p), fixed::to_string(*q)))
        .collect();
    format!("[{}]", v.join(","))
}

/// Polymarket's book hash: SHA1 over the book's JSON (bids ascending, asks descending).
fn hash(token: &str, t: &Tok, ts_ms: i64) -> String {
    let json = format!(
        "{{\"market\":\"{}\",\"asset_id\":\"{token}\",\"timestamp\":\"{ts_ms}\",\"hash\":\"\",\"bids\":{},\"asks\":{},\"min_order_size\":\"5\",\"tick_size\":\"0.01\",\"neg_risk\":false,\"last_trade_price\":\"0.500\"}}",
        t.market,
        levels(&mut t.bids.iter()),
        levels(&mut t.asks.iter().rev())
    );
    hex::encode(Sha1::digest(json.as_bytes()))
}

fn book_frame(token: &str, t: &Tok, ts_ms: i64) -> String {
    format!(
        "{{\"event_type\":\"book\",\"market\":\"{}\",\"asset_id\":\"{token}\",\"timestamp\":\"{ts_ms}\",\"hash\":\"{}\",\"bids\":{},\"asks\":{},\"tick_size\":\"0.01\",\"last_trade_price\":\"0.500\"}}",
        t.market,
        hash(token, t, ts_ms),
        levels(&mut t.bids.iter()),
        levels(&mut t.asks.iter().rev())
    )
}

impl Venue {
    fn new(fault: Fault) -> Arc<Self> {
        let mut toks = BTreeMap::new();
        for (m, ts) in markets() {
            for t in ts {
                let side = |lo: i64| (0..5).map(|i| (lo + i * 10_000, 100 * SCALE)).collect();
                toks.insert(t, Tok { market: m.clone(), bids: side(400_000), asks: side(520_000) });
            }
        }
        let state = Mutex::new(State { toks, seq: 0, ts_ms: chrono::Utc::now().timestamp_millis() });
        Arc::new(Self { state, tx: broadcast::channel(1 << 16).0, fault, accepts: 0.into(), gamma_requests: 0.into() })
    }

    /// One update every 10 ms, round-robin over the tokens.
    async fn generate(self: Arc<Self>) {
        let mut rng = StdRng::seed_from_u64(7);
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            tick.tick().await;
            let mut st = self.state.lock().unwrap();
            st.seq += 1;
            st.ts_ms = chrono::Utc::now().timestamp_millis().max(st.ts_ms + 1);
            let (seq, ts) = (st.seq, st.ts_ms);
            let token = st.toks.keys().nth(seq as usize % st.toks.len()).unwrap().clone();
            let bid = rng.gen_bool(0.5);
            // Bids stay at or below 0.49 and asks at or above 0.51, so the book never crosses.
            let px = if bid { rng.gen_range(30..50) } else { rng.gen_range(51..71) } * 10_000;
            let qty = if rng.gen_bool(0.3) { 0 } else { rng.gen_range(1..500) * SCALE };
            let t = st.toks.get_mut(&token).unwrap();
            let side = if bid { &mut t.bids } else { &mut t.asks };
            if qty == 0 {
                side.remove(&px);
            } else {
                side.insert(px, qty);
            }
            let h = hash(&token, t, ts);
            let frame = |q: i64| {
                format!(
                    "{{\"market\":\"{}\",\"price_changes\":[{{\"asset_id\":\"{token}\",\"price\":\"{}\",\"size\":\"{}\",\"side\":\"{}\",\"hash\":\"{h}\"}}],\"timestamp\":\"{ts}\",\"event_type\":\"price_change\"}}",
                    t.market,
                    fixed::to_string(px),
                    fixed::to_string(q),
                    if bid { "BUY" } else { "SELL" }
                )
            };
            let ev = Ev { seq, token: token.clone(), frame: frame(qty), bad: frame(qty + 7 * SCALE) };
            let _ = self.tx.send(Arc::new(ev));
        }
    }

    async fn serve_ws(self: Arc<Self>, tcp: TcpStream) {
        let k = self.accepts.fetch_add(1, Ordering::SeqCst);
        let mut rx = self.tx.subscribe();
        let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else { return };
        let (mut sink, mut stream) = ws.split();
        let start = Instant::now();
        let mine = k == 0 || matches!(self.fault, Fault::Resolved { .. });
        let delay = match self.fault {
            Fault::Delay { after, by } if mine => Some((after, by)),
            _ => None,
        };
        let (otx, mut orx) = mpsc::unbounded_channel::<(Instant, Message)>();
        let writer = tokio::spawn(async move {
            while let Some((at, m)) = orx.recv().await {
                if let Some((_, by)) = delay.filter(|(after, _)| at.duration_since(start) >= *after) {
                    tokio::time::sleep_until((at + by).into()).await;
                }
                if sink.send(m).await.is_err() {
                    break;
                }
            }
        });
        let send = |m: Message| {
            let _ = otx.send((Instant::now(), m));
        };
        let after = self.fault.after().filter(|_| mine);
        let timer = tokio::time::sleep(after.unwrap_or(Duration::from_secs(1 << 20)));
        tokio::pin!(timer);
        let (mut fired, mut corrupted) = (false, false);
        let mut subs: HashMap<String, u64> = HashMap::new();
        loop {
            let silent = fired && matches!(self.fault, Fault::Silent { .. });
            tokio::select! {
                m = stream.next() => match m {
                    Some(Ok(Message::Text(t))) if t == "PING" => {
                        if !silent {
                            send(Message::Text("PONG".into()));
                        }
                    }
                    Some(Ok(Message::Text(t))) => {
                        let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                        let ids: Vec<String> = v["assets_ids"].as_array().unwrap().iter().map(|x| x.as_str().unwrap().into()).collect();
                        if v["operation"] == "unsubscribe" {
                            ids.iter().for_each(|i| { subs.remove(i); });
                            continue;
                        }
                        if k > 0 {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        let st = self.state.lock().unwrap();
                        let books: Vec<_> = ids
                            .iter()
                            .filter_map(|i| st.toks.get(i).map(|t| (i, t)))
                            .map(|(i, t)| {
                                subs.insert(i.clone(), st.seq);
                                book_frame(i, t, st.ts_ms)
                            })
                            .collect();
                        drop(st);
                        if !books.is_empty() && !silent {
                            send(Message::Text(format!("[{}]", books.join(","))));
                        }
                    }
                    Some(Ok(_)) => {}
                    _ => break,
                },
                ev = rx.recv() => {
                    let Ok(ev) = ev else { break };
                    if silent || !subs.get(&ev.token).is_some_and(|s| ev.seq > *s) {
                        continue;
                    }
                    if fired && matches!(self.fault, Fault::Stall { .. }) && ev.token == STALLED {
                        continue;
                    }
                    let corrupt = fired && !corrupted && matches!(self.fault, Fault::Corrupt { .. });
                    corrupted |= corrupt;
                    send(Message::Text(if corrupt { ev.bad.clone() } else { ev.frame.clone() }));
                },
                _ = &mut timer, if !fired && after.is_some() => {
                    fired = true;
                    match self.fault {
                        Fault::Drop { .. } => {
                            writer.abort();
                            return;
                        }
                        Fault::Resolved { .. } => {
                            let reason = "all subscribed assets resolved".into();
                            send(Message::Close(Some(CloseFrame { code: CloseCode::Normal, reason })));
                        }
                        _ => {}
                    }
                },
            }
        }
        drop(otx);
        let _ = writer.await;
    }

    fn gamma_market(m: &str, ts: &[String; 2]) -> serde_json::Value {
        serde_json::json!({
            "conditionId": m,
            "clobTokenIds": serde_json::to_string(ts).unwrap(),
            "enableOrderBook": true,
            "acceptingOrders": true,
            "negRisk": false,
            "orderMinSize": 5.0,
            "orderPriceMinTickSize": 0.01,
        })
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
        let n = self.gamma_requests.fetch_add(1, Ordering::SeqCst);
        let line = String::from_utf8_lossy(&req).lines().next().unwrap_or_default().to_owned();
        let path = line.split(' ').nth(1).unwrap_or_default();
        let all: Vec<_> = markets().iter().map(|(m, ts)| Self::gamma_market(m, ts)).collect();
        let (status, body) = match self.fault {
            Fault::Gamma429(k) if n < k => ("429 Too Many Requests\r\nRetry-After: 1", String::new()),
            _ if path.starts_with("/markets/keyset") => {
                ("200 OK", serde_json::json!({"markets": all, "next_cursor": ""}).to_string())
            }
            _ if path.starts_with("/markets") => ("200 OK", serde_json::Value::Array(all).to_string()),
            _ => ("404 Not Found", String::new()),
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
    gamma_requests: usize,
}

impl Run {
    /// A counter from the collector's final `done` line (the last one, under --supervise).
    fn metric(&self, name: &str) -> u64 {
        let line = self.log.lines().rev().find(|l| l.contains(" done ")).expect("no done line");
        let key = format!(" {name}: ");
        let at = line.find(&key).unwrap_or_else(|| panic!("no {name} in {line}")) + key.len();
        line[at..].chars().take_while(char::is_ascii_digit).collect::<String>().parse().unwrap()
    }
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c == '\x1b' {
            it.by_ref().take_while(|&c| c != 'm').for_each(drop);
        } else {
            out.push(c);
        }
    }
    out
}

/// How a run ends after `secs`.
#[derive(PartialEq)]
enum Stop {
    /// `--duration-secs`: the collector stops itself.
    Duration,
    /// SIGTERM to the collector, as `kill`, systemd and Docker send.
    Sigterm,
    /// SIGTERM to a `--supervise` parent, which passes it on to the child.
    Supervised,
    /// SIGKILL to the collector: no shutdown, no last flush.
    Sigkill,
}

/// Runs the collector for `secs` against a mock venue with `fault`, then stops it by `stop`.
fn run(fault: Fault, secs: u64, extra: &[&str], stop: Stop) -> Run {
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
    let out = dir.path().join("ob.sbe");
    let log = dir.path().join("log.txt");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_collector"));
    cmd.args(["--ws-url", &format!("ws://127.0.0.1:{ws}/ws/market")])
        .args(["--gamma-url", &format!("http://127.0.0.1:{http}")])
        .arg("--out")
        .arg(&out)
        .arg("--universe-cache")
        .arg(dir.path().join("universe.json"))
        .args(["--recycle-mins", "0", "--core-threads", "1", "--ntp-server", ""])
        .args(extra)
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap());
    if stop != Stop::Duration {
        if stop == Stop::Supervised {
            cmd.arg("--supervise");
        }
        let mut child = cmd.spawn().unwrap();
        std::thread::sleep(Duration::from_secs(secs));
        if stop == Stop::Sigkill {
            unsafe { libc::kill(child.id() as i32, libc::SIGKILL) };
            child.wait().unwrap();
        } else {
            unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
            assert!(child.wait().unwrap().success(), "exits 0 on SIGTERM: {}", std::fs::read_to_string(&log).unwrap());
        }
    } else {
        let st = cmd.args(["--duration-secs", &secs.to_string()]).status().unwrap();
        assert!(st.success(), "collector failed: {}", std::fs::read_to_string(&log).unwrap());
    }
    let v = verify(&out);
    let log = strip_ansi(&std::fs::read_to_string(&log).unwrap());
    if std::env::var_os("E2E_LOG").is_some() {
        eprintln!("{log}\n{}", v.1);
    }
    Run {
        log,
        verify_ok: v.0,
        verify: v.1,
        accepts: venue.accepts.load(Ordering::SeqCst),
        gamma_requests: venue.gamma_requests.load(Ordering::SeqCst),
    }
}

fn verify(path: &Path) -> (bool, String) {
    let o = Command::new(env!("CARGO_BIN_EXE_verify")).arg(path).output().unwrap();
    (o.status.success(), String::from_utf8_lossy(&o.stdout).into_owned())
}

fn after(s: u64) -> Duration {
    Duration::from_secs(s)
}

fn assert_clean(r: &Run) {
    assert!(r.verify_ok, "verify failed:\n{}", r.verify);
    assert!(r.verify.contains("0 did not"), "{}", r.verify);
    assert!(r.metric("updates") > 100, "too few updates: {}", r.metric("updates"));
}

#[test]
fn baseline_no_fault() {
    let r = run(Fault::None, 12, &[], Stop::Duration);
    assert_clean(&r);
    assert_eq!(r.accepts, 2, "two replicas, no reconnect");
    assert!(r.metric("hash_ok") > 100, "the mock's hashes verify");
    assert_eq!((r.metric("hash_miss"), r.metric("switches"), r.metric("disconnects")), (0, 0, 0));
}

#[test]
fn dropped_socket_fails_over_and_reconnects() {
    let r = run(Fault::Drop { after: after(5) }, 14, &[], Stop::Duration);
    assert_clean(&r);
    assert!(r.metric("switches") >= 1, "tokens move to the replica");
    assert!(r.metric("switch_snaps") >= 1, "each switch writes a snapshot");
    assert!(r.metric("disconnects") >= 1);
    assert!(r.accepts >= 3, "socket 0 reconnects");
}

#[test]
fn silent_socket_is_switched_off_then_reconnected() {
    // No frames at all, PONGs included: the lag check moves tokens off at once (3 s), and the
    // 30 s watchdog reconnects the socket.
    let r = run(Fault::Silent { after: after(5) }, 42, &[], Stop::Duration);
    assert_clean(&r);
    assert!(r.metric("lag_switches") >= 1);
    assert!(r.accepts >= 3, "the watchdog reconnects socket 0");
}

#[test]
fn stalled_subscription_switches_that_token() {
    let r = run(Fault::Stall { after: after(5) }, 14, &[], Stop::Duration);
    assert_clean(&r);
    assert!(r.metric("lag_switches") >= 1, "the stalled token moves to its replica");
    assert_eq!(r.accepts, 2, "the socket itself stays up");
}

#[test]
fn delayed_socket_loses_the_source() {
    let r = run(Fault::Delay { after: after(5), by: Duration::from_secs(5) }, 16, &[], Stop::Duration);
    assert_clean(&r);
    assert!(r.metric("lag_switches") >= 1);
}

#[test]
fn corrupt_update_is_caught_by_the_hash_and_repaired() {
    let r = run(Fault::Corrupt { after: after(5) }, 14, &[], Stop::Duration);
    assert_clean(&r);
    assert!(r.metric("hash_miss") >= 1, "the venue hash catches the bad size");
    assert!(r.metric("switches") + r.metric("resnapshots") >= 1, "the bad book is replaced");
}

#[test]
fn gamma_429_slows_discovery_down_but_it_completes() {
    let r = run(Fault::Gamma429(2), 12, &[], Stop::Duration);
    assert_clean(&r);
    assert!(r.gamma_requests >= 3, "retried after the 429s");
    assert!(r.log.contains("discovery pass complete"));
}

#[test]
fn resolved_close_drops_tokens_without_reconnecting() {
    let r = run(Fault::Resolved { after: after(5) }, 12, &[], Stop::Duration);
    assert!(r.verify_ok, "verify failed:\n{}", r.verify);
    assert!(r.metric("resolved_closes") >= 1);
    assert_eq!(r.accepts, 2, "no reconnect for resolved assets");
}

#[test]
fn supervisor_restarts_after_a_panic() {
    let r = run(Fault::None, 16, &["--fault-panic-after-secs", "5"], Stop::Supervised);
    assert!(r.verify_ok, "verify failed:\n{}", r.verify);
    assert!(r.verify.contains("0 did not"), "{}", r.verify);
    assert!(r.verify.contains("resets:"), "a reset marker between runs: {}", r.verify);
    assert!(r.log.contains("supervisor: child died; restarting"));
}

#[test]
fn sigkill_keeps_what_was_written() {
    // A few seconds of the mock is far less than one 64 MiB chunk, so before the writer handed
    // each batch to the kernel this file held nothing at all.
    let r = run(Fault::None, 10, &[], Stop::Sigkill);
    assert!(r.verify_ok, "verify failed:\n{}", r.verify);
    assert!(r.verify.contains("0 did not"), "{}", r.verify);
    let line = r.verify.lines().find(|l| l.starts_with("messages:")).expect("messages line");
    let n: u64 = line.split_whitespace().nth(3).and_then(|w| w.replace(',', "").parse().ok()).unwrap_or(0);
    assert!(n > 100, "the killed run's file replays its batches: {line}");
}

#[test]
fn sigterm_flushes_and_exits_cleanly() {
    let r = run(Fault::None, 10, &[], Stop::Sigterm);
    assert_clean(&r);
    assert!(r.log.contains("stop signal: shutting down"));
}
