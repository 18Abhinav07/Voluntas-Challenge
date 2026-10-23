//! One websocket connection as an actor (§5.1). It owns its token set, reconnects with
//! jittered backoff, and stamps `recv_ts` and decodes frames before handing them to the core.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, interval, sleep, sleep_until, timeout},
};
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

use crate::adapter::{ConnId, ExchangeAdapter, NormEvent, Sym, Ts};

/// Times a reader found its core's queue full and had to wait. While it waits it does not
/// read its socket, and the server closes it as a slow consumer once its buffer fills.
pub static STALLS: AtomicU64 = AtomicU64::new(0);

/// Per-connection counters since the last [`top_conns`]: which sockets carry the bytes, which
/// spend their time decoding, and which wait on a full core queue.
#[derive(Default)]
pub struct ConnStats {
    rx_bytes: AtomicU64,
    frames: AtomicU64,
    decode_ns: AtomicU64,
    stall_ns: AtomicU64,
}

static CONN_STATS: std::sync::Mutex<Vec<(ConnId, Arc<ConnStats>)>> = std::sync::Mutex::new(Vec::new());

fn register_stats(id: ConnId) -> Arc<ConnStats> {
    let s = Arc::new(ConnStats::default());
    CONN_STATS.lock().expect("stats lock").push((id, s.clone()));
    s
}

fn drop_stats(id: ConnId) {
    CONN_STATS.lock().expect("stats lock").retain(|(c, _)| *c != id);
}

/// Resets every connection's counters and describes the `n` busiest by bytes and by stall
/// time over the window just ended, plus the totals.
pub fn top_conns(n: usize) -> String {
    let mut rows: Vec<(ConnId, [u64; 4])> = CONN_STATS
        .lock()
        .expect("stats lock")
        .iter()
        .map(|(c, s)| {
            let take = |a: &AtomicU64| a.swap(0, Ordering::Relaxed);
            (*c, [take(&s.rx_bytes), take(&s.frames), take(&s.decode_ns), take(&s.stall_ns)])
        })
        .collect();
    let mut sum = [0u64; 4];
    for (_, v) in &rows {
        for (s, x) in sum.iter_mut().zip(v) {
            *s += x;
        }
    }
    let fmt = |(c, v): &(ConnId, [u64; 4])| {
        format!("{c}:{}MB/{}f/dec{}ms/stall{}ms", v[0] >> 20, v[1], v[2] / 1_000_000, v[3] / 1_000_000)
    };
    rows.sort_unstable_by_key(|(_, v)| std::cmp::Reverse(v[0]));
    let by_rx: Vec<String> = rows.iter().take(n).map(fmt).collect();
    rows.sort_unstable_by_key(|(_, v)| std::cmp::Reverse(v[3]));
    let by_stall: Vec<String> = rows.iter().take(n).filter(|(_, v)| v[3] > 0).map(fmt).collect();
    format!(
        "all: {}MB {}f dec{}ms stall{}ms | top rx [{}] | top stall [{}]",
        sum[0] >> 20,
        sum[1],
        sum[2] / 1_000_000,
        sum[3] / 1_000_000,
        by_rx.join(" "),
        by_stall.join(" ")
    )
}

/// No frame (not even a PONG) for this long means the socket is dead.
const WATCHDOG: Duration = Duration::from_secs(30);
/// Before the first frame the venue may still be queueing a large initial dump: at full
/// universe the first frame can take 45 s, and the PONGs wait behind it.
const FIRST_FRAME_GRACE: Duration = Duration::from_secs(90);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const BACKOFF_MIN: Duration = Duration::from_millis(250);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum ConnCmd {
    Subscribe(Vec<Sym>),
    Unsubscribe(Vec<Sym>),
    /// Asks for fresh `book`s: unsubscribe, then subscribe with the initial dump.
    Resnapshot(Vec<Sym>),
    /// Drops the socket and reconnects (resubscribing everything), keeping the actor.
    Reconnect,
    Close,
}

#[derive(Debug)]
pub enum Inbound {
    Events { conn: ConnId, recv_ts: Ts, events: Vec<NormEvent> },
    /// Connected and (re)subscribed to every token it owns; books follow.
    Up { conn: ConnId },
    /// Lost the socket; the core must stop trusting this replica until its books return.
    Down { conn: ConnId, reason: String },
    /// The actor exited (closed or cancelled).
    Gone { conn: ConnId },
}

pub struct ConnHandle {
    pub id: ConnId,
    pub cmd: mpsc::UnboundedSender<ConnCmd>,
    pub task: JoinHandle<()>,
}

/// Failed opens in a row, across all connections, that mean the network or venue is down
/// rather than one socket.
const OUTAGE_STREAK: u32 = 8;
const OUTAGE_GAP_MAX: Duration = Duration::from_secs(30);

/// Spaces out websocket handshakes across all connections: the venue times out handshakes
/// that arrive in bursts (150 at once: 15 timed out, §12). It is also the circuit breaker:
/// once opens keep failing everywhere, the gap between attempts grows to [`OUTAGE_GAP_MAX`],
/// so hundreds of sockets retrying on their own cannot turn an outage into a storm.
pub struct OpenLimiter {
    last: Mutex<Option<Instant>>,
    gap: Duration,
    streak: AtomicU32,
}

impl OpenLimiter {
    pub fn new(per_sec: f64) -> Self {
        Self { last: Mutex::new(None), gap: Duration::from_secs_f64(1.0 / per_sec), streak: AtomicU32::new(0) }
    }

    fn gap(&self) -> Duration {
        match self.streak.load(Ordering::Relaxed).checked_sub(OUTAGE_STREAK) {
            None => self.gap,
            Some(n) => Duration::from_millis(500).saturating_mul(1 << n.min(6)).min(OUTAGE_GAP_MAX),
        }
    }

    /// Waits for this open's turn. The gap is read when the turn comes, not when the wait
    /// starts: booking slots ahead would keep an outage's 30 s gap for every queued socket
    /// (100 queued = 50 minutes) long after opens recover.
    async fn wait(&self) {
        loop {
            let due = {
                let mut last = self.last.lock().await;
                let now = Instant::now();
                let due = last.map_or(now, |l| l + self.gap());
                if now >= due {
                    *last = Some(now);
                    return;
                }
                due
            };
            // The gap can shrink while we sleep, so look again after one normal gap at most.
            sleep_until(due.min(Instant::now() + self.gap)).await;
        }
    }

    fn ok(&self) {
        if self.streak.swap(0, Ordering::Relaxed) >= OUTAGE_STREAK {
            tracing::info!("connections succeeding again; outage backoff cleared");
        }
    }

    fn fail(&self) {
        if self.streak.fetch_add(1, Ordering::Relaxed) + 1 == OUTAGE_STREAK {
            tracing::warn!(streak = OUTAGE_STREAK, "opens failing everywhere; backing off globally");
        }
    }
}

pub fn now_us() -> Ts {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_micros() as Ts)
}

pub struct ConnSpec<A> {
    pub id: ConnId,
    pub adapter: Arc<A>,
    pub tokens: Vec<Sym>,
    /// Edge IP to pin this replica to (TLS SNI still uses the URL host); `None` = DNS.
    pub edge: Option<SocketAddr>,
    pub limiter: Arc<OpenLimiter>,
    pub out: mpsc::Sender<Inbound>,
    pub cancel: CancellationToken,
}

pub fn spawn<A: ExchangeAdapter>(spec: ConnSpec<A>) -> ConnHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    let id = spec.id;
    let task = tokio::spawn(run(spec, rx));
    ConnHandle { id, cmd: tx, task }
}

enum Exit {
    Closed,
    Lost(String),
}

async fn run<A: ExchangeAdapter>(spec: ConnSpec<A>, mut cmds: mpsc::UnboundedReceiver<ConnCmd>) {
    let mut tokens: BTreeSet<Sym> = spec.tokens.iter().cloned().collect();
    let stats = register_stats(spec.id);
    let mut failures = 0u32;
    loop {
        let started = Instant::now();
        let exit = tokio::select! {
            _ = spec.cancel.cancelled() => Exit::Closed,
            r = session(&spec, &mut tokens, &mut cmds, &stats) => r,
        };
        match exit {
            Exit::Closed => break,
            Exit::Lost(reason) => {
                tracing::warn!(conn = spec.id, %reason, "connection lost");
                if spec.out.send(Inbound::Down { conn: spec.id, reason }).await.is_err() {
                    break;
                }
            }
        }
        // A session that stayed up for a while resets the backoff.
        failures = if started.elapsed() > Duration::from_secs(60) { 1 } else { failures + 1 };
        let cap = BACKOFF_MIN.saturating_mul(1 << failures.min(7)).min(BACKOFF_MAX);
        let wait = rand::thread_rng().gen_range(BACKOFF_MIN..=cap);
        tokio::select! {
            _ = spec.cancel.cancelled() => break,
            _ = sleep(wait) => {}
        }
    }
    drop_stats(spec.id);
    let _ = spec.out.send(Inbound::Gone { conn: spec.id }).await;
}

/// One connected session. Returns when the socket dies, the actor is told to close, or the
/// core is gone.
async fn session<A: ExchangeAdapter>(
    spec: &ConnSpec<A>,
    tokens: &mut BTreeSet<Sym>,
    cmds: &mut mpsc::UnboundedReceiver<ConnCmd>,
    stats: &ConnStats,
) -> Exit {
    let a = &spec.adapter;
    spec.limiter.wait().await;
    let ws = match timeout(CONNECT_TIMEOUT, connect(a.ws_url(), spec.edge)).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            spec.limiter.fail();
            return Exit::Lost(format!("connect: {e:#}"));
        }
        Err(_) => {
            spec.limiter.fail();
            return Exit::Lost("connect timed out".into());
        }
    };
    let (mut sink, mut stream) = ws.split();
    if !tokens.is_empty() {
        let all: Vec<Sym> = tokens.iter().cloned().collect();
        if let Err(e) = sink.send(Message::Text(a.subscribe_initial(&all))).await {
            return Exit::Lost(format!("subscribe: {e}"));
        }
    }
    if spec.out.send(Inbound::Up { conn: spec.id }).await.is_err() {
        return Exit::Closed;
    }
    let mut ping = interval(a.ping_interval());
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_frame = Instant::now();
    // A handshake alone does not prove the path works; the first frame back does.
    let mut first_frame = true;
    loop {
        tokio::select! {
            biased;
            msg = stream.next() => {
                let recv_ts = now_us();
                if first_frame {
                    first_frame = false;
                    spec.limiter.ok();
                }
                last_frame = Instant::now();
                let text = match msg {
                    Some(Ok(Message::Text(t))) => t,
                    Some(Ok(Message::Close(f))) => return Exit::Lost(format!("server closed: {f:?}")),
                    Some(Ok(_)) => continue, // binary/ping/pong: tungstenite answers pings itself
                    Some(Err(e)) => return Exit::Lost(format!("read: {e}")),
                    None => return Exit::Lost("stream ended".into()),
                };
                stats.rx_bytes.fetch_add(text.len() as u64, Ordering::Relaxed);
                stats.frames.fetch_add(1, Ordering::Relaxed);
                let t = Instant::now();
                let mut events = Vec::new();
                let decoded = a.decode(&text, &mut events);
                stats.decode_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                if let Err(e) = decoded {
                    tracing::warn!(conn = spec.id, error = %e, frame = %text.chars().take(200).collect::<String>(), "undecodable frame");
                    continue;
                }
                if events.is_empty() {
                    continue;
                }
                let item = Inbound::Events { conn: spec.id, recv_ts, events };
                let sent = match spec.out.try_send(item) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(item)) => {
                        STALLS.fetch_add(1, Ordering::Relaxed);
                        let t = Instant::now();
                        let r = spec.out.send(item).await.map_err(drop);
                        stats.stall_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        r
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(()),
                };
                if sent.is_err() {
                    return Exit::Closed;
                }
            }
            cmd = cmds.recv() => {
                let msg = match cmd {
                    None | Some(ConnCmd::Close) => {
                        let _ = sink.send(Message::Close(None)).await;
                        return Exit::Closed;
                    }
                    Some(ConnCmd::Subscribe(t)) => {
                        tokens.extend(t.iter().cloned());
                        vec![a.subscribe(&t)]
                    }
                    Some(ConnCmd::Unsubscribe(t)) => {
                        for x in &t {
                            tokens.remove(x);
                        }
                        vec![a.unsubscribe(&t)]
                    }
                    Some(ConnCmd::Resnapshot(t)) => vec![a.unsubscribe(&t), a.subscribe(&t)],
                    Some(ConnCmd::Reconnect) => {
                        let _ = sink.send(Message::Close(None)).await;
                        return Exit::Lost("reconnect requested".into());
                    }
                };
                for m in msg {
                    if let Err(e) = sink.send(Message::Text(m)).await {
                        return Exit::Lost(format!("send: {e}"));
                    }
                }
            }
            _ = ping.tick() => {
                let limit = if first_frame { FIRST_FRAME_GRACE } else { WATCHDOG };
                if last_frame.elapsed() > limit {
                    spec.limiter.fail();
                    return Exit::Lost(format!("no frame for {limit:?}"));
                }
                if let Err(e) = sink.send(Message::Text(a.ping_frame().into())).await {
                    return Exit::Lost(format!("ping: {e}"));
                }
            }
        }
    }
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

async fn connect(url: &str, edge: Option<SocketAddr>) -> anyhow::Result<Ws> {
    let uri: tokio_tungstenite::tungstenite::http::Uri = url.parse()?;
    let host = uri.host().ok_or_else(|| anyhow::anyhow!("no host in {url}"))?;
    let tcp = match edge {
        Some(addr) => TcpStream::connect(addr).await?,
        None => TcpStream::connect((host, uri.port_u16().unwrap_or(443))).await?,
    };
    tcp.set_nodelay(true)?;
    let cfg = WebSocketConfig { max_message_size: Some(512 << 20), max_frame_size: Some(512 << 20), ..Default::default() };
    let (ws, _) = client_async_tls_with_config(url, tcp, Some(cfg), None).await?;
    Ok(ws)
}

/// Resolves every edge address of the websocket host, so replicas can be pinned apart.
pub async fn resolve_edges(url: &str) -> anyhow::Result<Vec<SocketAddr>> {
    let uri: tokio_tungstenite::tungstenite::http::Uri = url.parse()?;
    let host = uri.host().ok_or_else(|| anyhow::anyhow!("no host in {url}"))?;
    let mut v: Vec<SocketAddr> =
        tokio::net::lookup_host((host, uri.port_u16().unwrap_or(443))).await?.filter(|a| a.is_ipv4()).collect();
    v.sort();
    v.dedup();
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn outage_gap_is_not_kept_once_opens_recover() {
        let l = Arc::new(OpenLimiter::new(10.0));
        for _ in 0..OUTAGE_STREAK + 6 {
            l.fail();
        }
        l.wait().await; // takes the first turn while the gap is 30 s
        let t0 = Instant::now();
        let waiters: Vec<_> = (0..20)
            .map(|_| {
                let l = l.clone();
                tokio::spawn(async move { l.wait().await })
            })
            .collect();
        sleep(Duration::from_secs(1)).await;
        l.ok(); // the outage clears: queued opens go back to 10/s
        for w in waiters {
            w.await.unwrap();
        }
        assert!(t0.elapsed() < Duration::from_secs(5), "20 queued opens took {:?}", t0.elapsed());
    }
}
