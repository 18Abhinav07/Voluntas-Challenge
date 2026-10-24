//! Prediction-market orderbook collector: discovers every live market, keeps redundant
//! websocket replicas of each book and writes level updates plus periodic snapshots as aligned
//! SBE. Polymarket is the venue it is built and measured on; `--venue kalshi` runs the same
//! engine on the Kalshi adapter, which has only been tested against Kalshi's documented frames.
//!
//! Threads: the main runtime runs discovery, lookups and every socket (decoding included).
//! Book state is split across `--core-threads` core threads, each owning whole markets, their
//! shards and those shards' sockets, so no state is shared and per-token order is kept.
//! All cores feed the one writer thread.

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use clap::{Parser, ValueEnum};
use engineering_challenge::{
    pin,
    adapter::{
        ConnId, ExchangeAdapter, Instrument, Sym,
        kalshi::{self, Kalshi},
        polymarket::{self, Polymarket},
    },
    clock,
    conn::{self, ConnCmd, ConnHandle, ConnSpec, Inbound, OpenLimiter, now_us},
    core::{Config, Core, Effects, Fast, Metrics, SockLat},
    supervise::{self, Supervisor},
    writer,
};
use tokio::{
    runtime::Handle,
    sync::mpsc,
    time::{MissedTickBehavior, interval, timeout},
};
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(about = "Collects prediction-market L2 orderbooks into an aligned SBE file", args_override_self = true)]
struct Args {
    /// The exchange. Kalshi reads its API key from `KALSHI_API_KEY_ID` and
    /// `KALSHI_PRIVATE_KEY_PATH` (docs/KALSHI.md); it has not been run against the live venue.
    #[arg(long, value_enum, default_value_t = Venue::Polymarket)]
    venue: Venue,
    /// Output: `-` (the default) is stdout, anything else a file path, truncated on start.
    /// Logs always go to stderr.
    #[arg(long, default_value = "-")]
    out: PathBuf,
    /// Tokens subscribed per websocket.
    #[arg(long, default_value_t = 2500)]
    tokens_per_conn: usize,
    /// Cap on steady-state websockets (shards × replicas); tokens beyond it are not collected.
    #[arg(long, default_value_t = 600)]
    max_conns: usize,
    /// Independent websocket replicas per shard (pinned to different edges when possible).
    #[arg(long, default_value_t = 2)]
    replicas: usize,
    /// Instrument list from the last run, used to subscribe before discovery finishes
    /// [default: universe.json, or universe-kalshi.json with --venue kalshi].
    #[arg(long)]
    universe_cache: Option<PathBuf>,
    /// Make-before-break recycle connections older than this many minutes (0 = never).
    #[arg(long, default_value_t = 60)]
    recycle_mins: u64,
    /// Split a shard off when its measured message rate passes this many times the median
    /// shard's (0 = never, the default: on Polymarket the hot markets are the short-lived ones,
    /// so a split shard re-heats as new ones list and empties as they resolve; see the log).
    #[arg(long, default_value_t = 0.0)]
    split_ratio: f32,
    /// Cut output into files of this many minutes, named by period start (0 = one file).
    #[arg(long, default_value_t = 0)]
    rotate_mins: u64,
    /// Threads that own book state (0 = half the CPUs, at most 8).
    #[arg(long, default_value_t = 0)]
    core_threads: usize,
    /// Tokio workers for sockets, TLS and decoding (0 = the CPUs left after the core threads
    /// and the writer, at least 1).
    #[arg(long, default_value_t = 0)]
    io_threads: usize,
    /// Linux: pin each core thread to a CPU of its own and keep every other thread off those
    /// CPUs. A no-op elsewhere, or with fewer CPUs than core threads + 1.
    #[arg(long)]
    pin: bool,
    /// Output queued in memory, in MiB, before the cores wait for the writer (nothing is dropped).
    #[arg(long, default_value_t = 256)]
    writer_queue_mb: usize,
    /// Decoded frames queued per core before socket readers wait (and stop reading).
    #[arg(long, default_value_t = 65536)]
    core_queue: usize,
    /// Re-run discovery every this many minutes to pick up new markets.
    #[arg(long, default_value_t = 10)]
    rediscover_mins: u64,
    /// Only collect this many markets (0 = all).
    #[arg(long, default_value_t = 0)]
    max_markets: usize,
    /// Websocket handshakes per second across all connections.
    #[arg(long, default_value_t = 10.0)]
    opens_per_sec: f64,
    /// Stop after this many seconds (0 = run until SIGINT).
    #[arg(long, default_value_t = 0)]
    duration_secs: u64,
    /// Log per-socket and per-edge latency every 30 s (Ext 4). Costs a window per market.
    #[arg(long)]
    lat_detail: bool,
    /// Prefer each shard's fastest socket as source and recycle one that stays slowest (Ext 4).
    /// Keeps the same per-market windows as --lat-detail, and logs its latency lines too.
    #[arg(long)]
    fast_source: bool,
    /// Experiment: connect every replica to this edge IP instead of spreading them over the
    /// resolved edges (Ext 4, bench/port.sh).
    #[arg(long)]
    edge_ip: Option<std::net::IpAddr>,
    /// Experiment: local TCP port per replica, comma-separated, 0 = ephemeral. Replica r binds
    /// the r-th entry; replicas past the list are ephemeral (Ext 4, bench/port.sh).
    #[arg(long, value_delimiter = ',')]
    src_ports: Vec<u16>,
    /// With --fast-source: move off the preferred socket only when it is this much slower
    /// than the best.
    #[arg(long, default_value_t = Fast::default().margin_ms)]
    fast_margin_ms: f64,
    /// With --fast-source: weight of the newest 30 s window in a socket's score (1 = no memory).
    #[arg(long, default_value_t = Fast::default().weight)]
    fast_weight: f64,
    /// Run the collector as a child process and restart it whenever it dies (D20). Output
    /// stays readable: a torn chunk is dropped, and a reset marker separates two runs.
    #[arg(long)]
    supervise: bool,
    /// NTP server (`host:port`) to measure the host clock offset against, at start and every
    /// 10 minutes; it is logged, not applied (D21). Empty = off.
    #[arg(long, default_value = "time.google.com:123")]
    ntp_server: String,
    /// Test only: panic after this many seconds, to exercise fail-fast and --supervise.
    #[arg(long, hide = true)]
    fault_panic_after_secs: Option<u64>,
    /// Test only: the market websocket (the mock venue in tests/e2e.rs).
    #[arg(long, hide = true)]
    ws_url: Option<String>,
    /// Test only: the REST base URL (Gamma, or Kalshi's trade API).
    #[arg(long, hide = true, alias = "gamma-url")]
    rest_url: Option<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Venue {
    Polymarket,
    Kalshi,
}

/// Cancelled by a panic anywhere, so a dead thread or task stops the whole process (D20).
static FATAL: OnceLock<CancellationToken> = OnceLock::new();
static PANICKED: AtomicBool = AtomicBool::new(false);

/// Runs this binary again as a supervised child, with the same arguments minus `--supervise`.
fn supervise(args: &Args) -> anyhow::Result<()> {
    let mut child: Vec<_> = std::env::args_os().skip(1).filter(|a| a != "--supervise").collect();
    let output = if args.rotate_mins > 0 {
        anyhow::ensure!(args.out.as_os_str() != "-", "rotation needs a file path, not stdout");
        supervise::Output::Rotated(args.out.clone())
    } else {
        // The parent owns the one output; the child writes to its pipe.
        child.extend(["--out".into(), "-".into()]);
        if args.out.as_os_str() == "-" { supervise::Output::Stdout } else { supervise::Output::File(args.out.clone()) }
    };
    let restarts = Supervisor {
        program: std::env::current_exe()?,
        args: child,
        output,
        chunk: writer::CHUNK,
        max_restarts: None,
    }
    .run()?;
    tracing::info!(restarts, "supervisor: done");
    Ok(())
}

/// Carries out the side effects one core asks for.
struct Runtime<A> {
    /// The main runtime: sockets run (and decode) there, not on the core's thread.
    main: Handle,
    adapter: Arc<A>,
    conns: HashMap<ConnId, ConnHandle>,
    edges: Vec<SocketAddr>,
    src_ports: Vec<u16>,
    limiter: Arc<OpenLimiter>,
    inbound: mpsc::Sender<Inbound>,
    lookups: mpsc::UnboundedSender<Sym>,
    cancel: CancellationToken,
}

impl<A: ExchangeAdapter> Effects for Runtime<A> {
    fn spawn_conn(&mut self, id: ConnId, replica: usize, tokens: Vec<Sym>) {
        let edge = (!self.edges.is_empty()).then(|| self.edges[replica % self.edges.len()]);
        let src_port = self.src_ports.get(replica).copied().filter(|&p| p != 0);
        tracing::info!(conn = id, replica, ?edge, ?src_port, tokens = tokens.len(), "opening connection");
        let _rt = self.main.enter();
        let h = conn::spawn(ConnSpec {
            id,
            adapter: self.adapter.clone(),
            tokens,
            edge,
            src_port,
            limiter: self.limiter.clone(),
            out: self.inbound.clone(),
            cancel: self.cancel.child_token(),
        });
        self.conns.insert(id, h);
    }

    fn send(&mut self, id: ConnId, cmd: ConnCmd) {
        if let Some(h) = self.conns.get(&id) {
            let _ = h.cmd.send(cmd);
        }
    }

    fn lookup(&mut self, market: Sym) {
        let _ = self.lookups.send(market);
    }
}

/// Stream-announced markets wait this long for others to join their lookup request.
const LOOKUP_LINGER: Duration = Duration::from_millis(500);
const LOOKUP_BATCH: usize = 50;

/// One worker serves every lookup: a burst of listings becomes a few batched requests in
/// sequence rather than one concurrent request each, which is what tripped Gamma's rate limit.
fn spawn_lookups<A: ExchangeAdapter>(
    adapter: Arc<A>,
    found: mpsc::UnboundedSender<Vec<Instrument<A::Meta>>>,
) -> mpsc::UnboundedSender<Sym> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Sym>();
    tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            tokio::time::sleep(LOOKUP_LINGER).await;
            let mut batch = vec![first];
            if !rx.is_empty() {
                rx.recv_many(&mut batch, LOOKUP_BATCH - 1).await;
            }
            // Several cores can see the same listing.
            batch.sort_unstable();
            batch.dedup();
            match adapter.lookup(&batch).await {
                Ok(v) => {
                    let _ = found.send(v);
                }
                Err(e) => tracing::warn!(markets = batch.len(), error = %e, "market lookup failed"),
            }
        }
    });
    tx
}

fn load_cache<M: DeserializeOwned>(path: &PathBuf) -> Vec<Instrument<M>> {
    match std::fs::read(path) {
        Ok(b) => serde_json::from_slice(&b).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "ignoring unreadable universe cache");
            Vec::new()
        }),
        Err(_) => Vec::new(),
    }
}

fn save_cache<M: Serialize>(path: &PathBuf, insts: &[Instrument<M>]) -> anyhow::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec(insts)?)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// Runs one discovery pass, forwarding each page and then `None` when the pass is over.
fn discover<A: ExchangeAdapter>(adapter: Arc<A>, tx: mpsc::UnboundedSender<Option<Vec<Instrument<A::Meta>>>>) {
    tokio::spawn(async move {
        let (ptx, mut prx) = mpsc::channel(4);
        let fwd = {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(p) = prx.recv().await {
                    let _ = tx.send(Some(p));
                }
            })
        };
        if let Err(e) = adapter.discover(ptx).await {
            tracing::warn!(error = %e, "discovery pass failed");
        }
        let _ = fwd.await;
        let _ = tx.send(None);
    });
}

/// Inbound messages handled per core-loop turn before timers get another look.
const INBOUND_BATCH: usize = 256;
/// Connection ids of core `i` start at `i << CONN_ID_BITS`.
const CONN_ID_BITS: u32 = 24;

fn apply<A: ExchangeAdapter>(core: &mut Core<A::Meta>, rt: &mut Runtime<A>, msg: Inbound) {
    let now = std::time::Instant::now();
    match msg {
        Inbound::Events { conn, recv_ts, events } => {
            core.metrics.queue.record(now_us() - recv_ts);
            core.on_events(conn, recv_ts, &events, now, rt)
        }
        Inbound::Up { conn } => core.on_up(conn, now),
        Inbound::Down { conn, .. } => core.on_down(conn, now_us()),
        Inbound::Resolved { conn, tokens } => core.on_resolved(conn, &tokens, now_us(), rt),
        Inbound::Gone { conn } => {
            rt.conns.remove(&conn);
            core.on_gone(conn, now_us());
        }
    }
}

async fn emit<M>(tx: &writer::Tx, src: usize, core: &mut Core<M>) -> anyhow::Result<()> {
    if core.out.is_empty() {
        return Ok(());
    }
    let waited = tx.send((src, std::mem::take(&mut core.out))).await?;
    if waited > Duration::from_secs(1) {
        tracing::warn!(?waited, "writer backpressure");
    }
    Ok(())
}

/// What the main thread tells a core.
enum Ctl<M> {
    Add(Vec<Instrument<M>>),
    /// Open shards for whatever is pending (discovery paused or ended).
    Flush,
    Shutdown,
}

struct Status {
    core: usize,
    tokens: usize,
    sourced: usize,
    conns: usize,
    metrics: Metrics,
    lat: HashMap<ConnId, SockLat>,
}

/// Logs each socket's latency since start, then the same summed by edge IP. A socket's edge is
/// its replica's, as in [`Runtime::spawn_conn`].
fn lat_report(latest: &[Option<Status>], edges: &[SocketAddr]) {
    let mut socks: Vec<(ConnId, &SockLat)> = latest.iter().flatten().flat_map(|s| s.lat.iter().map(|(c, l)| (*c, l))).collect();
    if socks.is_empty() {
        return;
    }
    socks.sort_by_key(|(c, _)| *c);
    let edge = |r: usize| edges.get(r % edges.len().max(1)).map_or_else(|| "-".to_string(), |e| e.ip().to_string());
    let mut by_edge: std::collections::BTreeMap<String, (usize, SockLat)> = Default::default();
    for (c, l) in &socks {
        let e = edge(l.replica);
        tracing::info!(conn = c, replica = l.replica, edge = %e, first = l.first, late = l.late, wire = ?l.wire, behind = ?l.behind, "socket latency");
        let (n, sum) = by_edge.entry(e).or_default();
        *n += 1;
        sum.wire.merge(&l.wire);
        sum.behind.merge(&l.behind);
        sum.first += l.first;
        sum.late += l.late;
    }
    for (e, (n, l)) in &by_edge {
        tracing::info!(edge = %e, sockets = n, first = l.first, late = l.late, wire = ?l.wire, behind = ?l.behind, "edge latency");
    }
}

/// The core that owns a market: stable for the process, so a market never moves.
fn core_of(market: &str, n: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    market.hash(&mut h);
    (h.finish() % n as u64) as usize
}

fn route<M>(ctls: &[mpsc::UnboundedSender<Ctl<M>>], v: Vec<Instrument<M>>) {
    let mut parts: Vec<Vec<Instrument<M>>> = (0..ctls.len()).map(|_| Vec::new()).collect();
    for i in v {
        parts[core_of(&i.market, ctls.len())].push(i);
    }
    for (c, p) in ctls.iter().zip(parts) {
        if !p.is_empty() {
            let _ = c.send(Ctl::Add(p));
        }
    }
}

fn flush_all<M>(ctls: &[mpsc::UnboundedSender<Ctl<M>>]) {
    for c in ctls {
        let _ = c.send(Ctl::Flush);
    }
}

struct CoreSpec<A: ExchangeAdapter> {
    idx: usize,
    /// The CPU this core runs on alone (--pin).
    cpu: Option<usize>,
    cfg: Config,
    rt: Runtime<A>,
    ctl: mpsc::UnboundedReceiver<Ctl<A::Meta>>,
    inbound: mpsc::Receiver<Inbound>,
    writer: writer::Tx,
    status: mpsc::UnboundedSender<Status>,
    /// Cancelled by a core that fails, so the whole process stops rather than limps.
    fatal: CancellationToken,
}

/// Runs one core on its own OS thread with a current-thread runtime for its timers and channels.
fn spawn_core<A: ExchangeAdapter>(spec: CoreSpec<A>) -> std::io::Result<std::thread::JoinHandle<anyhow::Result<Metrics>>> {
    std::thread::Builder::new().name(format!("core-{}", spec.idx)).spawn(move || {
        let fatal = spec.fatal.clone();
        if let Some(c) = spec.cpu {
            pin::set(&[c]);
        }
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let r = rt.block_on(core_loop(spec));
        if let Err(e) = &r {
            tracing::error!(error = %e, "core failed");
            fatal.cancel();
        }
        r
    })
}

async fn core_loop<A: ExchangeAdapter>(spec: CoreSpec<A>) -> anyhow::Result<Metrics> {
    let CoreSpec { idx, cfg, mut rt, mut ctl, inbound: mut irx, writer: wtx, status, .. } = spec;
    let mut core = Core::new(cfg, rt.adapter.integrity());
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut report = interval(Duration::from_secs(10));
    report.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Timers and control come first in the biased select: under load the inbound channel is
    // always ready, and polling it first starved ticks and snapshots. Inbound is drained in
    // bounded batches so one wakeup cannot monopolise the loop.
    let mut inbox = Vec::with_capacity(INBOUND_BATCH);
    let result: anyhow::Result<()> = async {
        loop {
            tokio::select! {
                biased;
                c = ctl.recv() => match c {
                    Some(Ctl::Add(v)) => core.add(v, &mut rt),
                    Some(Ctl::Flush) => core.flush_pending(std::time::Instant::now(), &mut rt),
                    Some(Ctl::Shutdown) | None => break,
                },
                _ = tick.tick() => core.on_tick(std::time::Instant::now(), now_us(), &mut rt),
                _ = report.tick() => {
                    let _ = status.send(Status {
                        core: idx,
                        tokens: core.token_count(),
                        sourced: core.sourced_count(),
                        conns: core.conn_count(),
                        metrics: core.metrics.clone(),
                        lat: core.lat.iter().map(|(&c, l)| (c, l.clone())).collect(),
                    });
                }
                n = irx.recv_many(&mut inbox, INBOUND_BATCH) => {
                    if n == 0 {
                        break;
                    }
                    for msg in inbox.drain(..) {
                        apply(&mut core, &mut rt, msg);
                    }
                }
            }
            emit(&wtx, idx, &mut core).await?;
        }
        Ok(())
    }
    .await;

    // Shutdown: stop the sockets, apply what they already delivered, then hand back metrics.
    for h in rt.conns.values() {
        let _ = h.cmd.send(ConnCmd::Close);
    }
    rt.cancel.cancel();
    let drain = async {
        while !rt.conns.is_empty() {
            let Some(msg) = irx.recv().await else { break };
            // Only apply data here: replaying Down/Gone would fail every token over as sockets close.
            match msg {
                Inbound::Events { conn, recv_ts, events } => {
                    core.on_events(conn, recv_ts, &events, std::time::Instant::now(), &mut rt)
                }
                Inbound::Gone { conn } => {
                    rt.conns.remove(&conn);
                }
                _ => {}
            }
            let _ = emit(&wtx, idx, &mut core).await;
        }
    };
    if timeout(Duration::from_secs(5), drain).await.is_err() {
        tracing::warn!(core = idx, left = rt.conns.len(), "connections did not exit in time");
    }
    let _ = emit(&wtx, idx, &mut core).await;
    result.map(|()| core.metrics)
}

/// Each websocket is a file descriptor and full coverage needs several hundred, more than the
/// usual 1024 soft limit once replicas and recycling overlap. Raise the soft limit to the hard one.
fn raise_nofile() {
    #[cfg(unix)]
    unsafe {
        let mut lim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return;
        }
        let before = lim.rlim_cur;
        // macOS rejects RLIM_INFINITY here; its OPEN_MAX (10240) bounds it.
        #[cfg(target_os = "macos")]
        let max = lim.rlim_max.min(10240);
        #[cfg(not(target_os = "macos"))]
        let max = lim.rlim_max;
        lim.rlim_cur = max;
        if lim.rlim_cur > before && libc::setrlimit(libc::RLIMIT_NOFILE, &lim) == 0 {
            tracing::info!(from = before, to = lim.rlim_cur, "raised open-file limit");
        }
    }
}

/// Core threads: `--core-threads`, or half the CPUs, at most 8.
fn n_cores(args: &Args) -> usize {
    match args.core_threads {
        0 => (std::thread::available_parallelism().map_or(2, |n| n.get()) / 2).clamp(1, 8),
        n => n,
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    #[cfg(unix)]
    if args.out.as_os_str() == "-" && unsafe { libc::isatty(libc::STDOUT_FILENO) } == 1 {
        anyhow::bail!("refusing to write binary SBE to a terminal: redirect stdout or pass --out <file>");
    }
    if args.supervise {
        return supervise(&args);
    }
    raise_nofile();
    let cpus = std::thread::available_parallelism().map_or(2, |n| n.get());
    let cores = n_cores(&args);
    // The writer counts as one; the finisher is idle between rotations.
    let io = match args.io_threads {
        0 => cpus.saturating_sub(cores + 1).max(1),
        n => n,
    };
    let pins = if args.pin { pin::split(cores) } else { None };
    if let Some(p) = &pins {
        // Inherited by every thread started from here on; each core thread then moves to its own.
        pin::set(&p.rest);
    }
    tracing::info!(cpus, cores, io, pinned = pins.is_some(), "thread budget");
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(io).enable_all().build()?;
    rt.block_on(start(args, pins.map(|p| p.cores)))
}

async fn start(args: Args, pins: Option<Vec<usize>>) -> anyhow::Result<()> {
    match args.venue {
        Venue::Polymarket => {
            let pm = Polymarket::with_endpoints(
                args.ws_url.as_deref().unwrap_or(polymarket::WS_URL),
                args.rest_url.as_deref().unwrap_or(polymarket::GAMMA),
            )?;
            run(args, Arc::new(pm), pins).await
        }
        Venue::Kalshi => {
            // Only the mock venue in the tests (an overridden --ws-url) takes no key.
            let auth = match kalshi::Auth::from_env() {
                Ok(a) => Some(a),
                Err(_) if args.ws_url.is_some() => None,
                Err(e) => return Err(e.context("--venue kalshi needs an API key (docs/KALSHI.md)")),
            };
            let k = Kalshi::with_endpoints(
                args.ws_url.as_deref().unwrap_or(kalshi::WS_URL),
                args.rest_url.as_deref().unwrap_or(kalshi::REST),
                auth,
            )?;
            run(args, Arc::new(k), pins).await
        }
    }
}

/// Everything after the venue is chosen: the engine does not know which one it runs.
async fn run<A: ExchangeAdapter>(args: Args, adapter: Arc<A>, pins: Option<Vec<usize>>) -> anyhow::Result<()> {
    let cache_path = args.universe_cache.clone().unwrap_or_else(|| match args.venue {
        Venue::Polymarket => "universe.json".into(),
        Venue::Kalshi => "universe-kalshi.json".into(),
    });
    let cancel = CancellationToken::new();
    FATAL.set(cancel.clone()).expect("main runs once");
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        PANICKED.store(true, Ordering::SeqCst);
        tracing::error!("panic: shutting down");
        if let Some(c) = FATAL.get() {
            c.cancel();
        }
    }));
    if let Some(secs) = args.fault_panic_after_secs {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(secs)).await;
            panic!("injected fault (--fault-panic-after-secs)");
        });
    }
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("SIGINT: shutting down (again to force)");
            cancel.cancel();
            let _ = tokio::signal::ctrl_c().await;
            std::process::exit(130);
        });
    }
    if !args.ntp_server.is_empty() {
        tokio::spawn(clock::watch(args.ntp_server.clone(), Duration::from_secs(600)));
    }
    if args.duration_secs > 0 {
        let (cancel, d) = (cancel.clone(), Duration::from_secs(args.duration_secs));
        tokio::spawn(async move {
            tokio::time::sleep(d).await;
            tracing::info!("duration elapsed: shutting down");
            cancel.cancel();
        });
    }

    let n_cores = n_cores(&args);
    let rotate = (args.rotate_mins > 0).then(|| Duration::from_secs(args.rotate_mins * 60));
    let (wtx, writer) = writer::spawn(args.out.clone(), args.writer_queue_mb.max(1) << 20, rotate, n_cores)?;
    let edges = match args.edge_ip {
        Some(ip) => vec![SocketAddr::new(ip, 443)],
        None => conn::resolve_edges(adapter.ws_url()).await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "edge resolution failed; using DNS per connection");
            Vec::new()
        }),
    };
    tracing::info!(?edges, "websocket edges");

    let (ftx, mut frx) = mpsc::unbounded_channel();
    let (dtx, mut drx) = mpsc::unbounded_channel();
    let (stx, mut srx) = mpsc::unbounded_channel::<Status>();
    let lookups = spawn_lookups(adapter.clone(), ftx);
    let limiter = Arc::new(OpenLimiter::new(args.opens_per_sec));
    let conn_cancel = CancellationToken::new();
    let mut ctls = Vec::with_capacity(n_cores);
    let mut cores = Vec::with_capacity(n_cores);
    for idx in 0..n_cores {
        let (ctx, crx) = mpsc::unbounded_channel();
        let (itx, irx) = mpsc::channel::<Inbound>(args.core_queue.max(1));
        let cfg = Config {
            tokens_per_conn: args.tokens_per_conn,
            replicas: args.replicas.max(1),
            max_conns: args.max_conns.div_ceil(n_cores),
            max_conn_age: (args.recycle_mins > 0).then(|| Duration::from_secs(args.recycle_mins * 60)),
            conn_id_base: (idx as ConnId) << CONN_ID_BITS,
            rotate_every: rotate,
            split_ratio: (args.split_ratio > 0.0).then_some(args.split_ratio),
            lat_detail: args.lat_detail,
            fast: args.fast_source.then(|| Fast { margin_ms: args.fast_margin_ms, weight: args.fast_weight, ..Fast::default() }),
            ..Config::default()
        };
        let rt = Runtime {
            main: Handle::current(),
            adapter: adapter.clone(),
            conns: HashMap::new(),
            edges: edges.clone(),
            src_ports: args.src_ports.clone(),
            limiter: limiter.clone(),
            inbound: itx,
            lookups: lookups.clone(),
            cancel: conn_cancel.child_token(),
        };
        cores.push(spawn_core(CoreSpec {
            idx,
            cpu: pins.as_ref().map(|p| p[idx]),
            cfg,
            rt,
            ctl: crx,
            inbound: irx,
            writer: wtx.clone(),
            status: stx.clone(),
            fatal: cancel.clone(),
        })?);
        ctls.push(ctx);
    }
    drop((stx, lookups));
    tracing::info!(cores = n_cores, "core threads started");

    // Boot from the cache so books flow within seconds, then reconcile with discovery.
    let cap = |v: &mut Vec<Instrument<A::Meta>>, have: usize| {
        if args.max_markets > 0 {
            v.truncate(args.max_markets.saturating_sub(have));
        }
    };
    let mut universe: HashMap<Sym, Instrument<A::Meta>> = HashMap::new();
    let mut cached = load_cache(&cache_path);
    cap(&mut cached, 0);
    if !cached.is_empty() {
        tracing::info!(markets = cached.len(), "subscribing from universe cache");
        for i in &cached {
            universe.insert(i.market.clone(), i.clone());
        }
        route(&ctls, cached);
        flush_all(&ctls);
    }
    discover(adapter.clone(), dtx.clone());
    let mut discovering = true;

    let mut report = interval(Duration::from_secs(30));
    report.reset();
    let mut rediscover = interval(Duration::from_secs(args.rediscover_mins.max(1) * 60));
    rediscover.reset();
    let mut latest: Vec<Option<Status>> = (0..n_cores).map(|_| None).collect();
    let total = |latest: &[Option<Status>]| {
        let mut m = Metrics::default();
        let (mut tokens, mut sourced, mut conns) = (0, 0, 0);
        for s in latest.iter().flatten() {
            m.merge(&s.metrics);
            (tokens, sourced, conns) = (tokens + s.tokens, sourced + s.sourced, conns + s.conns);
        }
        (tokens, sourced, conns, m)
    };

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            Some(s) = srx.recv() => {
                let i = s.core;
                latest[i] = Some(s);
            }
            _ = report.tick() => {
                let (tokens, sourced, conns, m) = total(&latest);
                let per_core: Vec<usize> = latest.iter().map(|s| s.as_ref().map_or(0, |s| s.tokens)).collect();
                let stalls = conn::STALLS.load(std::sync::atomic::Ordering::Relaxed);
                let (queue_hwm_mb, queue_waits) = (wtx.hwm() as f64 / (1 << 20) as f64, wtx.waits());
                tracing::info!(tokens, sourced, conns, ?per_core, stalls, queue_hwm_mb, queue_waits, m = ?m, "status");
                tracing::info!(window = %conn::top_conns(5), "conn stats");
                lat_report(&latest, &edges);
            }
            _ = rediscover.tick(), if !discovering => {
                discovering = true;
                discover(adapter.clone(), dtx.clone());
            }
            Some(page) = drx.recv() => match page {
                Some(mut v) => {
                    v.retain(|i| !universe.contains_key(&i.market));
                    cap(&mut v, universe.len());
                    for i in &v {
                        universe.insert(i.market.clone(), i.clone());
                    }
                    route(&ctls, v);
                }
                None => {
                    discovering = false;
                    flush_all(&ctls);
                    let all: Vec<_> = universe.values().cloned().collect();
                    if let Err(e) = save_cache(&cache_path, &all) {
                        tracing::warn!(error = %e, "could not save universe cache");
                    }
                    tracing::info!(markets = universe.len(), "discovery pass complete");
                }
            },
            Some(mut v) = frx.recv() => {
                v.retain(|i| !universe.contains_key(&i.market));
                cap(&mut v, universe.len());
                for i in &v {
                    universe.insert(i.market.clone(), i.clone());
                    tracing::info!(market = %i.market, "new market listed");
                }
                route(&ctls, v);
                if !discovering {
                    flush_all(&ctls);
                }
            }
        }
    }

    lat_report(&latest, &edges);

    // Shutdown: each core closes its sockets and applies what they already delivered; the
    // writer finishes once every core has dropped its sender.
    tracing::info!("closing connections");
    for c in &ctls {
        let _ = c.send(Ctl::Shutdown);
    }
    let mut metrics = Metrics::default();
    let mut failed = None;
    for h in cores {
        match tokio::task::spawn_blocking(move || h.join()).await? {
            Ok(Ok(m)) => metrics.merge(&m),
            Ok(Err(e)) => failed = Some(e),
            Err(_) => failed = Some(anyhow::anyhow!("core thread panicked")),
        }
    }
    conn_cancel.cancel();
    tracing::info!(queue_hwm_mb = wtx.hwm() as f64 / (1 << 20) as f64, queue_waits = wtx.waits(), "writer queue");
    drop(wtx);
    let all: Vec<_> = universe.values().cloned().collect();
    if let Err(e) = save_cache(&cache_path, &all) {
        tracing::warn!(error = %e, "could not save universe cache");
    }
    let stats = tokio::task::spawn_blocking(move || writer.join())
        .await?
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    tracing::info!(updates = stats.updates, snapshots = stats.snapshots, m = ?metrics, "done");
    if PANICKED.load(Ordering::SeqCst) && failed.is_none() {
        failed = Some(anyhow::anyhow!("a thread or task panicked"));
    }
    failed.map_or(Ok(()), Err)
}
