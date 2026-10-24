//! Kernel receive timestamps for `--kernel-ts` (H1, Linux). The websocket's TCP stream is read
//! with `recvmsg` and `SO_TIMESTAMPNS`, so each read carries the time the kernel received its
//! last segment. A frame's `recv_ts` is that time for the read that completed it, which leaves
//! out TLS, websocket parsing and tokio scheduling. How much those add (app clock − kernel
//! clock) is kept as a histogram and logged with the conn stats. Elsewhere the stream reads
//! plainly and frames keep the app clock.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

use crate::adapter::Ts;

/// The kernel time (µs since the epoch) of a socket's latest read; 0 until the first one.
#[derive(Default)]
pub struct Stamp(AtomicI64);

impl Stamp {
    pub fn get(&self) -> Option<Ts> {
        Some(self.0.load(Ordering::Relaxed)).filter(|&t| t != 0)
    }
}

/// A TCP stream that stamps its reads when `on` (and the OS supports it).
pub struct Stamped {
    tcp: TcpStream,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    on: bool,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    stamp: Arc<Stamp>,
}

impl Stamped {
    /// Wraps `tcp`, turning kernel timestamps on if asked. Fails only if the socket option does.
    pub fn new(tcp: TcpStream, on: bool) -> io::Result<(Self, Arc<Stamp>)> {
        let on = on && enable(&tcp)?;
        let stamp = Arc::new(Stamp::default());
        Ok((Self { tcp, on, stamp: stamp.clone() }, stamp))
    }
}

#[cfg(target_os = "linux")]
fn enable(tcp: &TcpStream) -> io::Result<bool> {
    use std::os::fd::AsRawFd;
    let one: libc::c_int = 1;
    let r = unsafe {
        libc::setsockopt(
            tcp.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            (&one as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if r != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(true)
}

#[cfg(not(target_os = "linux"))]
fn enable(_: &TcpStream) -> io::Result<bool> {
    Ok(false)
}

/// One `recvmsg` into `buf`: the bytes read, and the kernel time if it came with them.
#[cfg(target_os = "linux")]
fn recv_stamped(fd: std::os::fd::RawFd, buf: &mut [u8]) -> io::Result<(usize, Option<Ts>)> {
    #[repr(C, align(8))]
    struct Ctl([u8; 64]);
    let mut ctl = Ctl([0; 64]);
    let mut iov = libc::iovec { iov_base: buf.as_mut_ptr().cast(), iov_len: buf.len() };
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = ctl.0.as_mut_ptr().cast();
    msg.msg_controllen = ctl.0.len() as _;
    let n = unsafe { libc::recvmsg(fd, &mut msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut ts = None;
    unsafe {
        let mut c = libc::CMSG_FIRSTHDR(&msg);
        while !c.is_null() {
            if (*c).cmsg_level == libc::SOL_SOCKET && (*c).cmsg_type == libc::SCM_TIMESTAMPNS {
                let t: libc::timespec = std::ptr::read_unaligned(libc::CMSG_DATA(c).cast());
                ts = Some(t.tv_sec * 1_000_000 + t.tv_nsec / 1_000);
            }
            c = libc::CMSG_NXTHDR(&msg, c);
        }
    }
    Ok((n as usize, ts))
}

impl AsyncRead for Stamped {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        #[cfg(target_os = "linux")]
        if me.on {
            use std::os::fd::AsRawFd;
            let fd = me.tcp.as_raw_fd();
            loop {
                std::task::ready!(me.tcp.poll_read_ready(cx))?;
                match me.tcp.try_io(tokio::io::Interest::READABLE, || recv_stamped(fd, buf.initialize_unfilled())) {
                    Ok((n, ts)) => {
                        buf.advance(n);
                        if let Some(t) = ts {
                            me.stamp.0.store(t, Ordering::Relaxed);
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
        }
        Pin::new(&mut me.tcp).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stamped {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().tcp).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().tcp).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().tcp).poll_shutdown(cx)
    }
}

/// Host delay (app clock − kernel clock) per frame, in power-of-two µs buckets.
static DELAY: [AtomicU64; 32] = [const { AtomicU64::new(0) }; 32];

/// Records one frame's host delay.
pub fn record(app: Ts, kernel: Ts) {
    let d = (app - kernel).max(0) as u64;
    let b = (64 - d.leading_zeros()) as usize;
    DELAY[b.min(31)].fetch_add(1, Ordering::Relaxed);
}

/// Resets the histogram and describes the window just ended: upper bounds in µs of the bucket
/// holding each percentile. `None` when nothing was stamped.
pub fn take_report() -> Option<String> {
    let h: Vec<u64> = DELAY.iter().map(|a| a.swap(0, Ordering::Relaxed)).collect();
    let n: u64 = h.iter().sum();
    if n == 0 {
        return None;
    }
    let at = |q: f64| {
        let want = ((n as f64) * q).ceil().max(1.0) as u64;
        let mut acc = 0;
        for (b, &c) in h.iter().enumerate() {
            acc += c;
            if acc >= want {
                return if b == 0 { 0 } else { 1u64 << b };
            }
        }
        1u64 << 31
    };
    Some(format!("frames {n} app-kernel us p50<={} p90<={} p99<={} p99.9<={} max<={}", at(0.5), at(0.9), at(0.99), at(0.999), at(1.0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_buckets_by_power_of_two() {
        for d in [0, 3, 3, 3, 100] {
            record(1_000 + d, 1_000);
        }
        // 3 µs lands in (2, 4], 100 µs in (64, 128].
        assert_eq!(take_report().unwrap(), "frames 5 app-kernel us p50<=4 p90<=128 p99<=128 p99.9<=128 max<=128");
        assert!(take_report().is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reads_carry_a_kernel_timestamp() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (tcp, srv) = tokio::join!(TcpStream::connect(addr), l.accept());
        let (mut s, stamp) = Stamped::new(tcp.unwrap(), true).unwrap();
        let before = crate::conn::now_us();
        srv.unwrap().0.write_all(b"hello").await.unwrap();
        let mut b = [0u8; 5];
        s.read_exact(&mut b).await.unwrap();
        let k = stamp.get().expect("stamped");
        assert!(k >= before - 1_000 && k <= crate::conn::now_us(), "{k} vs {before}");
    }
}
