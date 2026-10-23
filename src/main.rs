//! Polymarket orderbook collector: discovers every live market, keeps redundant websocket
//! replicas of each book and writes level updates plus periodic snapshots as aligned SBE.

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use clap::Parser;
use engineering_challenge::{
    adapter::{
        ConnId, ExchangeAdapter, Instrument, Sym,
        polymarket::{Meta, Polymarket},
    },
    conn::{self, ConnCmd, ConnHandle, ConnSpec, Inbound, OpenLimiter, now_us},
    core::{Config, Core, Effects},
    writer::{self, Batch},
};
use tokio::{
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
    #[arg(long, default_value_t = 0)]
    recycle_mins: u64,
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
}

/// Carries out the side effects the core asks for.
struct Runtime {
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
        tracing::debug!(conn = id, replica, ?edge, tokens = tokens.len(), "opening connection");
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

/// Inbound messages handled per main-loop turn before timers get another look.
const INBOUND_BATCH: usize = 256;

fn apply(core: &mut Core<Meta>, rt: &mut Runtime, msg: Inbound) {
    let now = std::time::Instant::now();
    match msg {
        Inbound::Events { conn, recv_ts, events } => {
            core.metrics.queue.record(now_us() - recv_ts);
            core.on_events(conn, recv_ts, &events, now, rt)
        }
        Inbound::Up { conn } => core.on_up(conn, now),
        Inbound::Down { conn, .. } => core.on_down(conn, now_us()),
        Inbound::Gone { conn } => {
            rt.conns.remove(&conn);
            core.on_gone(conn, now_us());
        }
    }
}

async fn emit(tx: &mpsc::Sender<Batch>, core: &mut Core<Meta>) -> anyhow::Result<()> {
    if core.out.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(&mut core.out);
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

    let (wtx, writer) = writer::spawn(args.out.clone(), 1024)?;
    let edges = conn::resolve_edges(adapter.ws_url()).await.unwrap_or_else(|e| {
        tracing::warn!(error = %e, "edge resolution failed; using DNS per connection");
        Vec::new()
    });
    tracing::info!(?edges, "websocket edges");

    let (itx, mut irx) = mpsc::channel::<Inbound>(8192);
    let (ftx, mut frx) = mpsc::unbounded_channel();
    let (dtx, mut drx) = mpsc::unbounded_channel();
    let conn_cancel = CancellationToken::new();
    let mut rt = Runtime {
        adapter: adapter.clone(),
        conns: HashMap::new(),
        edges,
        limiter: Arc::new(OpenLimiter::new(args.opens_per_sec)),
        inbound: itx,
        lookups: spawn_lookups(adapter.clone(), ftx),
        cancel: conn_cancel.clone(),
    };
    let cfg = Config {
        tokens_per_conn: args.tokens_per_conn,
        replicas: args.replicas.max(1),
        max_conns: args.max_conns,
        max_conn_age: (args.recycle_mins > 0).then(|| Duration::from_secs(args.recycle_mins * 60)),
        ..Config::default()
    };
    let mut core = Core::new(cfg, adapter.integrity());

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
        core.add(cached, &mut rt);
        core.flush_pending(std::time::Instant::now(), &mut rt);
    }
    discover(adapter.clone(), dtx.clone());
    let mut discovering = true;

    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut report = interval(Duration::from_secs(30));
    report.reset();
    let mut rediscover = interval(Duration::from_secs(args.rediscover_mins.max(1) * 60));
    rediscover.reset();

    // Timers and control channels come first in the biased select: under load the inbound
    // channel is always ready, and polling it first starved ticks, snapshots and discovery.
    // Inbound is drained in bounded batches so one wakeup cannot monopolise the loop.
    let mut inbox = Vec::with_capacity(INBOUND_BATCH);
    let result: anyhow::Result<()> = async {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = tick.tick() => core.on_tick(std::time::Instant::now(), now_us(), &mut rt),
                _ = report.tick() => {
                    tracing::info!(
                        tokens = core.token_count(),
                        sourced = core.sourced_count(),
                        conns = core.conn_count(),
                        m = ?core.metrics,
                        "status"
                    );
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
                        core.add(v, &mut rt);
                    }
                    None => {
                        discovering = false;
                        core.flush_pending(std::time::Instant::now(), &mut rt);
                        let all: Vec<_> = universe.values().cloned().collect();
                        if let Err(e) = save_cache(&args.universe_cache, &all) {
                            tracing::warn!(error = %e, "could not save universe cache");
                        }
                        tracing::info!(
                            markets = universe.len(),
                            tokens = core.token_count(),
                            conns = core.conn_count(),
                            "discovery pass complete"
                        );
                    }
                },
                Some(mut v) = frx.recv() => {
                    v.retain(|i| !universe.contains_key(&i.market));
                    cap(&mut v, universe.len());
                    for i in &v {
                        universe.insert(i.market.clone(), i.clone());
                        tracing::info!(market = %i.market, "new market listed");
                    }
                    core.add(v, &mut rt);
                    if !discovering {
                        core.flush_pending(std::time::Instant::now(), &mut rt);
                    }
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
            emit(&wtx, &mut core).await?;
        }
        Ok(())
    }
    .await;

    // Shutdown: stop the sockets, apply what they already delivered, then let the writer finish.
    tracing::info!("closing connections");
    for h in rt.conns.values() {
        let _ = h.cmd.send(ConnCmd::Close);
    }
    conn_cancel.cancel();
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
            let _ = emit(&wtx, &mut core).await;
        }
    };
    if timeout(Duration::from_secs(5), drain).await.is_err() {
        tracing::warn!(left = rt.conns.len(), "connections did not exit in time");
    }
    let _ = emit(&wtx, &mut core).await;
    drop(wtx);
    let all: Vec<_> = universe.values().cloned().collect();
    if let Err(e) = save_cache(&args.universe_cache, &all) {
        tracing::warn!(error = %e, "could not save universe cache");
    }
    let stats = tokio::task::spawn_blocking(move || writer.join())
        .await?
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    tracing::info!(updates = stats.updates, snapshots = stats.snapshots, m = ?core.metrics, "done");
    result
}
