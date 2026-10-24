//! `--supervise`: a parent process that restarts the collector when it dies (D20).
//!
//! The collector stops itself on any fatal error or panic (a dead core would otherwise drop
//! its markets silently until shutdown). The parent turns that into a short gap rather than an
//! outage: it restarts the child with backoff, and the universe cache brings books back within
//! seconds.
//!
//! Output stays readable across a restart:
//! - **One stream** (stdout or one file): the child writes to a pipe and the parent owns the
//!   output. It passes on whole chunks only, so a chunk torn by a crash is dropped, never
//!   written. Before the next child's data it writes a reset marker, a snapshot of the symbol
//!   [`RESET_SYMBOL`] with no levels: every book before it is void, and each book starts again
//!   with a snapshot. `verify` resets its books there.
//! - **Rotated files**: the child writes them itself. After a crash the parent cuts each
//!   unfinished `.part` file back to whole chunks and renames it, and the next child opens new
//!   names for periods that already have a file (see `writer`), so nothing is overwritten.
//!
//! SIGINT and SIGTERM are passed to the child as SIGINT. The child runs in its own process
//! group, so a terminal Ctrl-C reaches it once, through the parent.

use std::{
    ffi::OsString,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, AtomicI32, Ordering},
    time::{Duration, Instant},
};

use chrono::DateTime;

use crate::util::dtos::challenge_orderbook::SnapshotDomain;

/// Symbol of the reset marker written between two children's output.
pub const RESET_SYMBOL: &str = "*";

/// A child that ran this long counts as healthy: the next restart waits the shortest backoff.
const HEALTHY: Duration = Duration::from_secs(60);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

static STOP: AtomicBool = AtomicBool::new(false);
static CHILD: AtomicI32 = AtomicI32::new(0);

extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
    let pid = CHILD.load(Ordering::SeqCst);
    if pid > 0 {
        // Async-signal-safe.
        unsafe { libc::kill(pid, libc::SIGINT) };
    }
}

pub enum Output {
    Stdout,
    File(PathBuf),
    /// Rotated files under this base path, written by the child itself.
    Rotated(PathBuf),
}

pub struct Supervisor {
    /// The child's program and arguments.
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub output: Output,
    pub chunk: usize,
    /// Stop after this many restarts (tests); `None` restarts forever.
    pub max_restarts: Option<usize>,
}

/// One chunk holding only the reset marker.
pub fn reset_chunk(chunk: usize, at_us: i64) -> anyhow::Result<Vec<u8>> {
    let t = DateTime::from_timestamp_micros(at_us).ok_or_else(|| anyhow::anyhow!("bad time {at_us}"))?;
    let m = SnapshotDomain { received_at: t, exchange_timestamp: t, bids: vec![], asks: vec![], symbol: RESET_SYMBOL.into() };
    let mut buf = vec![0; chunk];
    let n = m.encoded_length_with_header()?;
    anyhow::ensure!(n + 8 <= chunk, "chunk too small for the reset marker");
    m.encode(&mut buf[..n])?;
    Ok(buf)
}

/// Cuts unfinished rotated files (`<stem>.*.sbe.part`) back to whole chunks and renames them to
/// `.sbe`. Returns the files kept.
pub fn recover_parts(base: &Path, chunk: usize) -> anyhow::Result<Vec<PathBuf>> {
    let dir = base.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let stem = base.file_stem().and_then(|s| s.to_str()).unwrap_or("orderbook");
    let mut kept = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let p = e?.path();
        let Some(name) = p.file_name().and_then(|n| n.to_str()) else { continue };
        if !(name.starts_with(&format!("{stem}.")) && name.ends_with(".sbe.part")) {
            continue;
        }
        let len = std::fs::metadata(&p)?.len();
        let whole = len - len % chunk as u64;
        if whole == 0 {
            std::fs::remove_file(&p)?;
            tracing::warn!(file = %p.display(), bytes = len, "removed crashed file with no whole chunk");
            continue;
        }
        let f = std::fs::OpenOptions::new().write(true).open(&p)?;
        f.set_len(whole)?;
        f.sync_all()?;
        let fin = p.with_extension("");
        std::fs::rename(&p, &fin)?;
        tracing::warn!(file = %fin.display(), dropped_bytes = len - whole, "recovered crashed rotated file");
        kept.push(fin);
    }
    Ok(kept)
}

/// Copies whole chunks from `r` to `w` until EOF. Returns the bytes of a torn last chunk,
/// which are dropped.
fn relay(r: &mut impl Read, w: &mut impl Write, chunk: usize) -> std::io::Result<usize> {
    let mut buf = vec![0u8; chunk];
    let mut have = 0;
    loop {
        match r.read(&mut buf[have..]) {
            Ok(0) => return Ok(have),
            Ok(n) => {
                have += n;
                if have == chunk {
                    w.write_all(&buf)?;
                    w.flush()?;
                    have = 0;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

fn sleep_unless_stopped(d: Duration) {
    let end = Instant::now() + d;
    while !STOP.load(Ordering::SeqCst) && Instant::now() < end {
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl Supervisor {
    /// Runs children until one exits cleanly or a signal arrives. Returns the number of
    /// restarts.
    pub fn run(self) -> anyhow::Result<usize> {
        let handler = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
        unsafe {
            libc::signal(libc::SIGINT, handler);
            libc::signal(libc::SIGTERM, handler);
        }
        let mut out: Option<Box<dyn Write>> = match &self.output {
            Output::Stdout => Some(Box::new(std::io::stdout())),
            Output::File(p) => Some(Box::new(std::fs::File::create(p)?)),
            Output::Rotated(_) => None,
        };
        let (mut restarts, mut backoff) = (0, BACKOFF_MIN);
        loop {
            let mut cmd = Command::new(&self.program);
            cmd.args(&self.args).stdin(Stdio::null()).stderr(Stdio::inherit());
            cmd.stdout(if out.is_some() { Stdio::piped() } else { Stdio::inherit() });
            #[cfg(unix)]
            std::os::unix::process::CommandExt::process_group(&mut cmd, 0);
            let started = Instant::now();
            let mut child = cmd.spawn()?;
            CHILD.store(child.id() as i32, Ordering::SeqCst);
            tracing::info!(pid = child.id(), restarts, "supervisor: child started");
            let torn = match (&mut out, child.stdout.take()) {
                (Some(w), Some(mut r)) => relay(&mut r, w, self.chunk)?,
                _ => 0,
            };
            let status = child.wait()?;
            CHILD.store(0, Ordering::SeqCst);
            let ran = started.elapsed();
            if torn > 0 {
                tracing::warn!(bytes = torn, "supervisor: dropped a torn last chunk");
            }
            if status.success() || STOP.load(Ordering::SeqCst) {
                tracing::info!(%status, ?ran, restarts, "supervisor: child finished");
                return Ok(restarts);
            }
            if self.max_restarts.is_some_and(|m| restarts >= m) {
                anyhow::bail!("child failed ({status}) after {restarts} restarts");
            }
            backoff = if ran >= HEALTHY { BACKOFF_MIN } else { (backoff * 2).min(BACKOFF_MAX) };
            tracing::error!(%status, ?ran, wait = ?backoff, "supervisor: child died; restarting");
            if let Output::Rotated(base) = &self.output {
                recover_parts(base, self.chunk)?;
            }
            sleep_unless_stopped(backoff);
            if STOP.load(Ordering::SeqCst) {
                return Ok(restarts);
            }
            if let Some(w) = &mut out {
                w.write_all(&reset_chunk(self.chunk, chrono::Utc::now().timestamp_micros())?)?;
                w.flush()?;
            }
            restarts += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage};

    #[test]
    fn relay_passes_whole_chunks_and_drops_a_torn_one() {
        let data: Vec<u8> = (0..25u8).collect();
        let mut out = Vec::new();
        let torn = relay(&mut &data[..], &mut out, 10).unwrap();
        assert_eq!((out, torn), ((0..20u8).collect::<Vec<_>>(), 5));
    }

    #[test]
    fn restarts_a_dying_child_and_marks_each_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        // Each run writes one whole 64-byte chunk and a torn half, then dies.
        let s = Supervisor {
            program: "sh".into(),
            args: vec!["-c".into(), "head -c 96 /dev/zero | tr '\\0' 'x'; exit 3".into()],
            output: Output::File(path.clone()),
            chunk: 64,
            max_restarts: Some(2),
        };
        assert!(s.run().is_err(), "gives up after max_restarts");
        let got = std::fs::read(&path).unwrap();
        // 3 runs × 1 chunk, with a marker chunk between runs.
        assert_eq!(got.len(), 5 * 64);
        for (i, c) in got.chunks(64).enumerate() {
            let data = c.iter().all(|&b| b == b'x');
            assert_eq!(data, i % 2 == 0, "chunk {i}: runs at even positions, markers between");
        }
    }

    #[test]
    fn reset_marker_reads_back_through_the_template_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.sbe");
        std::fs::write(&path, reset_chunk(crate::writer::CHUNK, 1_000).unwrap()).unwrap();
        let mut r = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
        let Some(AnyMessage::Snapshot(s)) = r.read_msg().unwrap() else { panic!("expected snapshot") };
        let s = SnapshotDomain::try_from_decoder(s).unwrap();
        assert_eq!((s.symbol.as_str(), s.bids.len(), s.asks.len()), (RESET_SYMBOL, 0, 0));
        assert!(r.read_msg().unwrap().is_none());
    }

    #[test]
    fn crashed_part_files_are_cut_to_whole_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("ob.sbe");
        std::fs::write(dir.path().join("ob.20261024T0000Z.sbe.part"), vec![1u8; 150]).unwrap();
        std::fs::write(dir.path().join("ob.20261024T0100Z.sbe.part"), vec![1u8; 30]).unwrap();
        std::fs::write(dir.path().join("other.20261024T0000Z.sbe.part"), vec![1u8; 30]).unwrap();
        let kept = recover_parts(&base, 64).unwrap();
        assert_eq!(kept, vec![dir.path().join("ob.20261024T0000Z.sbe")]);
        assert_eq!(std::fs::metadata(&kept[0]).unwrap().len(), 128);
        assert!(!dir.path().join("ob.20261024T0100Z.sbe.part").exists(), "no whole chunk: removed");
        assert!(dir.path().join("other.20261024T0000Z.sbe.part").exists(), "another stem is left alone");
    }
}
