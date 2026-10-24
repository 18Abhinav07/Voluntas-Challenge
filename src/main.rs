//! Polymarket orderbook collector: discovers every live market, keeps redundant websocket
//! replicas of each book and writes level updates plus periodic snapshots as aligned SBE.
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
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use engineering_challenge::{
    adapter::{
        ConnId, ExchangeAdapter, Instrument, Sym,
        polymarket::{Meta, Polymarket},
    },
    conn::{self, ConnCmd, ConnHandle, ConnSpec, Inbound, OpenLimiter, now_us},
    core::{Config, Core, Effects, Metrics, SockLat},
    writer::{self, Tagged},
};
use tokio::{
    runtime::Handle,
    sync::mpsc,
    time::{Instant, MissedTickBehavior, interval, timeout},
};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(about = "Collects Polymarket L2 orderbooks into an aligned SBE file")]
struct Args {
    /// Output file (`-` for stdout). Truncated on start.
    #[arg(long, default_value = "orderbook.sbe")]
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
    /// Instrument list from the last run, used to subscribe before Gamma discovery finishes.
    #[arg(long, default_value = "universe.json")]
    universe_cache: PathBuf,
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
}

/// Carries out the side effects one core asks for.
struct Runtime {
    /// The main runtime: sockets run (and decode) there, not on the core's thread.
    main: Handle,
    adapter: Arc<Polymarket>,
    conns: HashMap<ConnId, ConnHandle>,
    edges: Vec<SocketAddr>,
    limiter: Arc<OpenLimiter>,
    inbound: mpsc::Sender<Inbound>,
    lookups: mpsc::UnboundedSender<Sym>,
    cancel: CancellationToken,
}

impl Effects for Runtime {
    fn spawn_conn(&mut self, id: ConnId, replica: usize, tokens: Vec<Sym>) {
        let edge = (!self.edges.is_empty()).then(|| self.edges[replica % self.edges.len()]);
        tracing::info!(conn = id, replica, ?edge, tokens = tokens.len(), "opening connection");
        let _rt = self.main.enter();
        let h = conn::spawn(ConnSpec {
            id,
            adapter: self.adapter.clone(),
            tokens,
            edge,
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
fn spawn_lookups(
    adapter: Arc<Polymarket>,
    found: mpsc::UnboundedSender<Vec<Instrument<Meta>>>,
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

fn load_cache(path: &PathBuf) -> Vec<Instrument<Meta>> {
    match std::fs::read(path) {
        Ok(b) => serde_json::from_slice(&b).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "ignoring unreadable universe cache");
            Vec::new()
        }),
        Err(_) => Vec::new(),
    }
}

fn save_cache(path: &PathBuf, insts: &[Instrument<Meta>]) -> anyhow::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec(insts)?)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// Runs one discovery pass, forwarding each page and then `None` when the pass is over.
fn discover(adapter: Arc<Polymarket>, tx: mpsc::UnboundedSender<Option<Vec<Instrument<Meta>>>>) {
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

fn apply(core: &mut Core<Meta>, rt: &mut Runtime, msg: Inbound) {
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

async fn emit(tx: &mpsc::Sender<Tagged>, src: usize, core: &mut Core<Meta>) -> anyhow::Result<()> {
    if core.out.is_empty() {
        return Ok(());
    }
    let batch = (src, std::mem::take(&mut core.out));
    match tx.try_send(batch) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(batch)) => {
            let t = Instant::now();
            tx.send(batch).await.map_err(|_| anyhow::anyhow!("writer stopped"))?;
            if t.elapsed() > Duration::from_secs(1) {
                tracing::warn!(waited = ?t.elapsed(), "writer backpressure");
            }
            Ok(())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => anyhow::bail!("writer stopped"),
    }
}

/// What the main thread tells a core.
enum Ctl {
    Add(Vec<Instrument<Meta>>),
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

fn route(ctls: &[mpsc::UnboundedSender<Ctl>], v: Vec<Instrument<Meta>>) {
    let mut parts: Vec<Vec<Instrument<Meta>>> = (0..ctls.len()).map(|_| Vec::new()).collect();
    for i in v {
        parts[core_of(&i.market, ctls.len())].push(i);
    }
    for (c, p) in ctls.iter().zip(parts) {
        if !p.is_empty() {
            let _ = c.send(Ctl::Add(p));
        }
    }
}

fn flush_all(ctls: &[mpsc::UnboundedSender<Ctl>]) {
    for c in ctls {
        let _ = c.send(Ctl::Flush);
    }
}

struct CoreSpec {
    idx: usize,
    cfg: Config,
    rt: Runtime,
    ctl: mpsc::UnboundedReceiver<Ctl>,
    inbound: mpsc::Receiver<Inbound>,
    writer: mpsc::Sender<Tagged>,
    status: mpsc::UnboundedSender<Status>,
    /// Cancelled by a core that fails, so the whole process stops rather than limps.
    fatal: CancellationToken,
}

/// Runs one core on its own OS thread with a current-thread runtime for its timers and channels.
fn spawn_core(spec: CoreSpec) -> std::io::Result<std::thread::JoinHandle<anyhow::Result<Metrics>>> {
    std::thread::Builder::new().name(format!("core-{}", spec.idx)).spawn(move || {
        let fatal = spec.fatal.clone();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let r = rt.block_on(core_loop(spec));
        if let Err(e) = &r {
            tracing::error!(error = %e, "core failed");
            fatal.cancel();
        }
        r
    })
}

async fn core_loop(spec: CoreSpec) -> anyhow::Result<Metrics> {
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
                        lat: core.lat.clone(),
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    raise_nofile();
    let adapter = Arc::new(Polymarket::new()?);

    let cancel = CancellationToken::new();
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
    if args.duration_secs > 0 {
        let (cancel, d) = (cancel.clone(), Duration::from_secs(args.duration_secs));
        tokio::spawn(async move {
            tokio::time::sleep(d).await;
            tracing::info!("duration elapsed: shutting down");
            cancel.cancel();
        });
    }

    let n_cores = match args.core_threads {
        0 => (std::thread::available_parallelism().map_or(2, |n| n.get()) / 2).clamp(1, 8),
        n => n,
    };
    let rotate = (args.rotate_mins > 0).then(|| Duration::from_secs(args.rotate_mins * 60));
    let (wtx, writer) = writer::spawn(args.out.clone(), 1024, rotate, n_cores)?;
    let edges = conn::resolve_edges(adapter.ws_url()).await.unwrap_or_else(|e| {
        tracing::warn!(error = %e, "edge resolution failed; using DNS per connection");
        Vec::new()
    });
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
            ..Config::default()
        };
        let rt = Runtime {
            main: Handle::current(),
            adapter: adapter.clone(),
            conns: HashMap::new(),
            edges: edges.clone(),
            limiter: limiter.clone(),
            inbound: itx,
            lookups: lookups.clone(),
            cancel: conn_cancel.child_token(),
        };
        cores.push(spawn_core(CoreSpec {
            idx,
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

    // Boot from the cache so books flow within seconds, then reconcile with Gamma.
    let cap = |v: &mut Vec<Instrument<Meta>>, have: usize| {
        if args.max_markets > 0 {
            v.truncate(args.max_markets.saturating_sub(have));
        }
    };
    let mut universe: HashMap<Sym, Instrument<Meta>> = HashMap::new();
    let mut cached = load_cache(&args.universe_cache);
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
                tracing::info!(tokens, sourced, conns, ?per_core, stalls, m = ?m, "status");
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
                    if let Err(e) = save_cache(&args.universe_cache, &all) {
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
    drop(wtx);
    let all: Vec<_> = universe.values().cloned().collect();
    if let Err(e) = save_cache(&args.universe_cache, &all) {
        tracing::warn!(error = %e, "could not save universe cache");
    }
    let stats = tokio::task::spawn_blocking(move || writer.join())
        .await?
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    tracing::info!(updates = stats.updates, snapshots = stats.snapshots, m = ?metrics, "done");
    failed.map_or(Ok(()), Err)
}
