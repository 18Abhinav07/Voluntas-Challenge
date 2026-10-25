//! The only owner of the output files: a dedicated OS thread fed by a queue bounded in bytes,
//! so disk latency never stalls the async runtime and nothing is ever dropped (§5.7). Past the
//! bound the cores wait; [`Tx::hwm`] says how close a run came to it.
//!
//! With rotation, output is cut into wall-clock periods. Each core marks the cut in its own
//! stream (`Rec::Rotate`) and follows it with a snapshot of every book it publishes, so each
//! file replays on its own. A period's file is written as `.part` and renamed once every core
//! has moved past it: a consumer can take any `.sbe` file as complete.
//!
//! Finishing a file (last chunk, fsync, rename) runs on a separate `finisher` thread. An fsync
//! of a multi-GB file takes seconds; done inline it stopped the writer, which filled its
//! channel and blocked every core and socket reader behind it.
//!
//! A file on disk is written by [`ChunkFile`]: the template's chunk format, handed to the kernel
//! as each batch lands rather than once per 64 MiB chunk, so a `kill -9` loses only what was
//! still queued. Stdout keeps the template's [`AlignedSbeWriter`].

use std::{
    collections::BTreeMap,
    fs::File,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::{
    adapter::{Sym, Ts},
    book::Side,
    fixed::Fx,
    util::{
        AlignedSbeWriter,
        dtos::{
            SbeEncode,
            challenge_orderbook::{
                PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
            },
        },
    },
};

/// The reader is fixed at 64 MiB chunks.
pub const CHUNK: usize = 64 << 20;

/// One output message. `sym` is the token; the written symbol is `<token>:bid` / `<token>:ask`
/// by `side`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rec {
    Update { sym: Sym, side: Side, recv: Ts, exch: Ts, px: Fx, qty: Fx },
    /// One side of one book, best level first.
    Snapshot { sym: Sym, recv: Ts, exch: Ts, side: Side, levels: Vec<(Fx, Fx)> },
    /// What follows from this source belongs to rotation period `epoch` (wall µs / period).
    Rotate { epoch: i64 },
}

fn suffix(side: Side) -> &'static str {
    match side {
        Side::Bid => ":bid",
        Side::Ask => ":ask",
    }
}

pub type Batch = Vec<Rec>;
/// A batch tagged with the core that produced it: rotation is tracked per source.
pub type Tagged = (usize, Batch);

/// Heap bytes a batch holds while queued (the shared symbols are not counted).
fn queued_bytes(b: &Batch) -> usize {
    let levels: usize = b.iter().map(|r| if let Rec::Snapshot { levels, .. } = r { levels.len() } else { 0 }).sum();
    b.len() * std::mem::size_of::<Rec>() + levels * std::mem::size_of::<(Fx, Fx)>()
}

struct Budget {
    sem: Arc<Semaphore>,
    bytes: usize,
    hwm: AtomicUsize,
    waits: AtomicU64,
}

/// The cores' end of the writer queue. A batch holds its bytes of the budget until the writer
/// has written it.
#[derive(Clone)]
pub struct Tx {
    tx: mpsc::UnboundedSender<(Tagged, OwnedSemaphorePermit)>,
    budget: Arc<Budget>,
}

impl Tx {
    /// Queues a batch, waiting while the queue is over its byte bound. Returns how long it
    /// waited.
    pub async fn send(&self, t: Tagged) -> anyhow::Result<Duration> {
        let b = &self.budget;
        // A batch larger than the whole bound still goes through, alone.
        let n = queued_bytes(&t.1).clamp(1, b.bytes) as u32;
        let start = Instant::now();
        let permit = match b.sem.clone().try_acquire_many_owned(n) {
            Ok(p) => p,
            Err(_) => {
                b.waits.fetch_add(1, Ordering::Relaxed);
                b.sem.clone().acquire_many_owned(n).await.map_err(|_| anyhow::anyhow!("writer stopped"))?
            }
        };
        b.hwm.fetch_max(b.bytes - b.sem.available_permits(), Ordering::Relaxed);
        self.tx.send((t, permit)).map_err(|_| anyhow::anyhow!("writer stopped"))?;
        Ok(start.elapsed())
    }

    #[cfg(test)]
    fn blocking_send(&self, t: Tagged) -> anyhow::Result<()> {
        tokio::runtime::Builder::new_current_thread().build()?.block_on(self.send(t)).map(drop)
    }

    /// The most bytes queued at once so far.
    pub fn hwm(&self) -> usize {
        self.budget.hwm.load(Ordering::Relaxed)
    }

    /// Sends that found the queue full and waited.
    pub fn waits(&self) -> u64 {
        self.budget.waits.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub updates: u64,
    pub snapshots: u64,
}

fn ts(us: Ts) -> anyhow::Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(us).ok_or_else(|| anyhow::anyhow!("timestamp out of range: {us}"))
}

/// Bytes held back before [`ChunkFile::sync`] runs even with the queue still busy.
const SYNC_BYTES: usize = 1 << 20;

/// Byte-for-byte the format of [`AlignedSbeWriter`] (whole messages, each chunk zero-padded to
/// [`CHUNK`]), but the open chunk lives in the file, not only in memory. Opening a chunk sets the
/// file length to the chunk's end, so the not-yet-written tail is already the zero sentinel the
/// reader stops at; [`Self::sync`] then writes only the new whole messages at their offset. The
/// file is a valid capture after every sync, and a killed process leaves it in the page cache.
struct ChunkFile {
    file: File,
    buf: Vec<u8>,
    /// File offset of the open chunk.
    base: u64,
    /// Bytes of `buf` already handed to the kernel.
    synced: usize,
}

impl ChunkFile {
    fn create(path: &Path) -> anyhow::Result<Self> {
        let file = File::create(path)?;
        file.set_len(CHUNK as u64)?;
        Ok(Self { file, buf: Vec::with_capacity(CHUNK), base: 0, synced: 0 })
    }

    /// As [`AlignedSbeWriter::write_msg`]: a message that does not fit starts the next chunk.
    fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        // The template's eight-byte header, left as trailing zeros in every chunk.
        let limit = CHUNK - 8;
        let len = message.encoded_length_with_header()?;
        anyhow::ensure!(len <= limit, "SBE message does not fit in a chunk");
        if len > limit - self.buf.len() {
            self.sync()?;
            self.buf.clear();
            self.synced = 0;
            self.base += CHUNK as u64;
            self.file.set_len(self.base + CHUNK as u64)?;
        }
        let start = self.buf.len();
        self.buf.resize(start + len, 0);
        let result = message.encode(&mut self.buf[start..]).and_then(|written| {
            anyhow::ensure!(written == len, "SBE encoded length mismatch: expected {len} bytes, wrote {written}");
            Ok(())
        });
        if result.is_err() {
            self.buf.truncate(start);
        }
        result
    }

    fn pending(&self) -> usize {
        self.buf.len() - self.synced
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        if self.pending() > 0 {
            self.file.write_all_at(&self.buf[self.synced..], self.base + self.synced as u64)?;
            self.synced = self.buf.len();
        }
        Ok(())
    }
}

enum Out {
    File(ChunkFile),
    Stdout(AlignedSbeWriter),
}

impl Out {
    fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        match self {
            Out::File(f) => f.write_msg(message),
            Out::Stdout(w) => w.write_msg(message),
        }
    }

    fn sync(&mut self, force: bool) -> anyhow::Result<()> {
        match self {
            Out::File(f) if force || f.pending() >= SYNC_BYTES => f.sync(),
            _ => Ok(()),
        }
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        match self {
            Out::File(f) => f.sync(),
            Out::Stdout(w) => w.flush(),
        }
    }
}

struct Part {
    w: Out,
    tmp: PathBuf,
    fin: PathBuf,
}

impl Part {
    fn finish(self) -> anyhow::Result<()> {
        let Part { mut w, tmp, fin } = self;
        w.flush()?;
        drop(w);
        if fin != std::path::Path::new("/dev/stdout") {
            std::fs::OpenOptions::new().write(true).open(&tmp)?.sync_all()?;
            if tmp != fin {
                std::fs::rename(&tmp, &fin)?;
                tracing::info!(file = %fin.display(), "rotated output file complete");
            }
        }
        Ok(())
    }
}

/// `orderbook.sbe` for period starting 2026-10-23 12:00 becomes `orderbook.20261023T1200Z.sbe`.
fn period_path(base: &std::path::Path, start_us: Ts) -> anyhow::Result<PathBuf> {
    let stem = base.file_stem().and_then(|s| s.to_str()).unwrap_or("orderbook");
    let ext = base.extension().and_then(|s| s.to_str()).unwrap_or("sbe");
    Ok(base.with_file_name(format!("{stem}.{}.{ext}", ts(start_us)?.format("%Y%m%dT%H%MZ"))))
}

/// `fin`, or `<stem>.rN.sbe` if `fin` or its `.part` is already there: a restarted collector
/// (D20) opens the period it died in again, and must not overwrite what the last one wrote.
fn free_name(fin: PathBuf) -> PathBuf {
    let taken = |p: &std::path::Path| p.exists() || p.with_extension("sbe.part").exists();
    if !taken(&fin) {
        return fin;
    }
    let stem = fin.file_stem().and_then(|s| s.to_str()).unwrap_or("orderbook").to_owned();
    (1..).map(|n| fin.with_file_name(format!("{stem}.r{n}.sbe"))).find(|p| !taken(p)).expect("some free name")
}

/// Starts the writer thread for `sources` producers, queueing at most `bytes` (see
/// [`queued_bytes`]). It exits after the last sender is dropped and the queue is drained,
/// flushing and syncing every open file.
pub fn spawn(
    path: PathBuf,
    bytes: usize,
    rotate: Option<Duration>,
    sources: usize,
) -> anyhow::Result<(Tx, JoinHandle<anyhow::Result<Stats>>)> {
    let target = if path.as_os_str() == "-" { PathBuf::from("/dev/stdout") } else { path };
    let period = rotate.map(|p| p.as_micros().max(1) as i64);
    anyhow::ensure!(
        period.is_none() || target != std::path::Path::new("/dev/stdout"),
        "rotation needs a file path, not stdout"
    );
    let open = move |epoch: i64| -> anyhow::Result<Part> {
        let fin = match period {
            Some(p) => free_name(period_path(&target, epoch * p)?),
            None => target.clone(),
        };
        let tmp = if period.is_some() { fin.with_extension("sbe.part") } else { fin.clone() };
        let w = if tmp == Path::new("/dev/stdout") { Out::Stdout(AlignedSbeWriter::create(&tmp, CHUNK)?) } else { Out::File(ChunkFile::create(&tmp)?) };
        Ok(Part { w, tmp, fin })
    };
    let now_us = chrono::Utc::now().timestamp_micros();
    let start = period.map_or(0, |p| now_us / p);
    // Open before the thread starts so a bad path fails the process at once.
    let mut files = BTreeMap::from([(start, open(start)?)]);
    let bytes = bytes.clamp(1, Semaphore::MAX_PERMITS.min(u32::MAX as usize));
    let budget = Arc::new(Budget { sem: Arc::new(Semaphore::new(bytes)), bytes, hwm: AtomicUsize::new(0), waits: AtomicU64::new(0) });
    let (tx, mut rx) = mpsc::unbounded_channel::<(Tagged, OwnedSemaphorePermit)>();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<Part>();
    let finisher = std::thread::Builder::new().name("finisher".into()).spawn(move || -> anyhow::Result<()> {
        let mut res = Ok(());
        for p in done_rx {
            let fin = p.fin.clone();
            if let Err(e) = p.finish() {
                tracing::error!(file = %fin.display(), error = %e, "finishing rotated file failed");
                res = res.and(Err(e));
            }
        }
        res
    })?;
    let h = std::thread::Builder::new().name("writer".into()).spawn(move || {
        let mut stats = Stats::default();
        // The period each source is currently writing into.
        let mut at = vec![start; sources.max(1)];
        // Reused across messages: the symbol `String` is the only per-message allocation.
        let mut upd = PriceLevelUpdateDomain {
            backtest_timestamp: DateTime::UNIX_EPOCH,
            exchange_timestamp: DateTime::UNIX_EPOCH,
            price_level: 0,
            quantity: 0,
            symbol: String::new(),
        };
        // The permit is released once the batch is written.
        while let Some(((src, batch), _permit)) = rx.blocking_recv() {
            for rec in batch {
                if let Rec::Rotate { epoch } = rec {
                    if period.is_some() && epoch > at[src] {
                        at[src] = epoch;
                        let low = *at.iter().min().unwrap_or(&epoch);
                        while files.first_key_value().is_some_and(|(&e, _)| e < low) {
                            let (_, p) = files.pop_first().expect("checked above");
                            done_tx.send(p).map_err(|_| anyhow::anyhow!("finisher thread exited"))?;
                        }
                    }
                    continue;
                }
                let e = at[src];
                let w = match files.entry(e) {
                    std::collections::btree_map::Entry::Occupied(o) => o.into_mut(),
                    std::collections::btree_map::Entry::Vacant(v) => v.insert(open(e)?),
                };
                let w = &mut w.w;
                match rec {
                    Rec::Update { sym, side, recv, exch, px, qty } => {
                        upd.backtest_timestamp = ts(recv)?;
                        upd.exchange_timestamp = ts(exch)?;
                        upd.price_level = px;
                        upd.quantity = qty;
                        upd.symbol.clear();
                        upd.symbol.push_str(&sym);
                        upd.symbol.push_str(suffix(side));
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
                            symbol: format!("{sym}{}", suffix(side)),
                        };
                        w.write_msg(&snap)?;
                        stats.snapshots += 1;
                    }
                    Rec::Rotate { .. } => unreachable!("handled above"),
                }
            }
            // Hand the new bytes to the kernel once the queue is drained (or 1 MiB has built up),
            // so a burst costs one write call, not one per batch.
            let idle = rx.is_empty();
            for p in files.values_mut() {
                p.w.sync(idle)?;
            }
        }
        drop(done_tx);
        let finished = finisher.join().map_err(|_| anyhow::anyhow!("finisher thread panicked"))?;
        for (_, p) in files {
            p.finish()?;
        }
        finished?;
        Ok(stats)
    })?;
    Ok((Tx { tx, budget }, h))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage};

    #[test]
    fn round_trips_through_template_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.sbe");
        let (tx, h) = spawn(path.clone(), 4, None, 1).unwrap();
        let sym: Sym = "123".into();
        tx.blocking_send((0, vec![
            Rec::Snapshot { sym: sym.clone(), recv: 2, exch: 1, side: Side::Bid, levels: vec![(500_000, 10), (400_000, 20)] },
            Rec::Update { sym: sym.clone(), side: Side::Bid, recv: 4, exch: 3, px: 500_000, qty: 0 },
        ]))
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

    #[test]
    fn a_synced_chunk_file_replays_before_it_is_finished() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.sbe");
        let mut f = ChunkFile::create(&path).unwrap();
        let upd = |q| PriceLevelUpdateDomain {
            backtest_timestamp: DateTime::UNIX_EPOCH,
            exchange_timestamp: DateTime::UNIX_EPOCH,
            price_level: 1,
            quantity: q,
            symbol: "1:bid".into(),
        };
        f.write_msg(&upd(1)).unwrap();
        f.sync().unwrap();
        f.write_msg(&upd(2)).unwrap();
        // Not synced: the second message is still only in memory, as at a `kill -9`.
        assert_eq!(std::fs::metadata(&path).unwrap().len(), CHUNK as u64);
        assert_eq!(count(&path), 1);
        f.sync().unwrap();
        assert_eq!(count(&path), 2);
        // Same bytes as the template writer.
        let tpl = dir.path().join("tpl.sbe");
        let mut t = AlignedSbeWriter::create(&tpl, CHUNK).unwrap();
        t.write_msg(&upd(1)).unwrap();
        t.write_msg(&upd(2)).unwrap();
        t.flush().unwrap();
        assert!(std::fs::read(&tpl).unwrap() == std::fs::read(&path).unwrap());
        // Filling the chunk opens the next one, already zero-padded to its end.
        let msg = upd(3).encoded_length_with_header().unwrap();
        for _ in 0..(CHUNK - 8) / msg {
            f.write_msg(&upd(3)).unwrap();
        }
        f.sync().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 2 * CHUNK as u64);
        assert_eq!(count(&path), 2 + (CHUNK - 8) / msg);
    }

    fn count(path: &std::path::Path) -> usize {
        let mut r = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
        std::iter::from_fn(|| r.read_msg().unwrap().map(|_| ())).count()
    }

    #[test]
    fn a_taken_period_name_gets_a_restart_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("ob.20261024T0000Z.sbe");
        assert_eq!(free_name(fin.clone()), fin);
        std::fs::write(&fin, b"").unwrap();
        let r1 = dir.path().join("ob.20261024T0000Z.r1.sbe");
        assert_eq!(free_name(fin.clone()), r1);
        std::fs::write(r1.with_extension("sbe.part"), b"").unwrap();
        assert_eq!(free_name(fin), dir.path().join("ob.20261024T0000Z.r2.sbe"));
    }

    #[test]
    fn rotation_closes_a_period_once_every_source_has_left_it() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("ob.sbe");
        let period = Duration::from_secs(3600);
        let (tx, h) = spawn(base.clone(), 4, Some(period), 2).unwrap();
        let p = period.as_micros() as i64;
        let now = chrono::Utc::now().timestamp_micros() / p;
        let upd = |recv| Rec::Update { sym: "1".into(), side: Side::Bid, recv, exch: recv, px: 1, qty: 1 };
        let done = |e: i64| period_path(&base, e * p).unwrap();
        let sync = || std::thread::sleep(Duration::from_millis(100));

        tx.blocking_send((0, vec![upd(1), Rec::Rotate { epoch: now + 1 }, upd(2)])).unwrap();
        sync();
        // Source 1 is still in the first period, so its file stays open.
        assert!(!done(now).exists());
        tx.blocking_send((1, vec![upd(3), Rec::Rotate { epoch: now + 1 }])).unwrap();
        // Poll rather than sleep once: a loaded test run can delay the writer thread.
        for _ in 0..50 {
            if done(now).exists() {
                break;
            }
            sync();
        }
        assert!(done(now).exists());
        assert_eq!(count(&done(now)), 2);
        assert!(!done(now + 1).exists(), "current period stays .part");
        drop(tx);
        let stats = h.join().unwrap().unwrap();
        assert_eq!(stats.updates, 3);
        assert_eq!(count(&done(now + 1)), 1);
    }
}
