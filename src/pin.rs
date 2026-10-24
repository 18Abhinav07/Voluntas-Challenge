//! CPU pinning for `--pin` (Linux): each core thread gets a CPU of its own, and every other
//! thread (tokio workers, writer, finisher) shares the rest, so the books never migrate or
//! queue behind socket work. Elsewhere it does nothing.

/// The CPUs a run is split over.
pub struct Pins {
    /// One per core thread.
    pub cores: Vec<usize>,
    /// Everything else.
    pub rest: Vec<usize>,
}

/// Splits the CPUs this process may use: the last `cores` go to the core threads. `None` when
/// pinning is unsupported or would leave nothing for the other threads.
pub fn split(cores: usize) -> Option<Pins> {
    let all = allowed();
    if all.len() <= cores {
        if cfg!(target_os = "linux") {
            tracing::warn!(cpus = all.len(), cores, "--pin needs more CPUs than core threads; not pinning");
        } else {
            tracing::warn!("--pin is Linux only; not pinning");
        }
        return None;
    }
    let (rest, cores) = all.split_at(all.len() - cores);
    Some(Pins { cores: cores.to_vec(), rest: rest.to_vec() })
}

#[cfg(target_os = "linux")]
fn allowed() -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }
        (0..libc::CPU_SETSIZE as usize).filter(|&c| libc::CPU_ISSET(c, &set)).collect()
    }
}

#[cfg(not(target_os = "linux"))]
fn allowed() -> Vec<usize> {
    Vec::new()
}

/// Restricts the calling thread (and the threads it starts later) to `cpus`.
#[cfg(target_os = "linux")]
pub fn set(cpus: &[usize]) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            tracing::warn!(?cpus, error = %std::io::Error::last_os_error(), "sched_setaffinity failed");
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set(_: &[usize]) {}
