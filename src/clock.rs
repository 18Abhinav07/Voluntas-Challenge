//! Host clock offset from an NTP server (D21), measured with one SNTP exchange.
//!
//! `received_at` is the host clock, and every latency figure is `received_at` minus the venue's
//! timestamp, so a host clock off by 30 ms moves all of them by 30 ms. The collector does not
//! correct timestamps (a step mid-run would reorder them); it measures the offset and logs it,
//! so a run's latency can be read against it.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;

/// Seconds from the NTP epoch (1900) to the Unix epoch.
const NTP_UNIX: u64 = 2_208_988_800;
/// Offsets beyond this are logged as warnings.
pub const WARN_MS: f64 = 25.0;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    /// Server clock minus host clock.
    pub offset_ms: f64,
    pub rtt_ms: f64,
}

fn to_ntp(t: SystemTime) -> u64 {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    ((d.as_secs() + NTP_UNIX) << 32) | ((u64::from(d.subsec_nanos()) << 32) / 1_000_000_000)
}

fn ntp_ms(t: u64) -> f64 {
    ((t >> 32) as f64 - NTP_UNIX as f64) * 1e3 + (t & 0xffff_ffff) as f64 * 1e3 / 4_294_967_296.0
}

/// Offset and round trip from the four timestamps of an exchange: client send `t0`, server
/// receive `t1`, server send `t2`, client receive `t3`.
pub fn sample(t0: u64, t1: u64, t2: u64, t3: u64) -> Sample {
    let (t0, t1, t2, t3) = (ntp_ms(t0), ntp_ms(t1), ntp_ms(t2), ntp_ms(t3));
    Sample { offset_ms: ((t1 - t0) + (t2 - t3)) / 2.0, rtt_ms: (t3 - t0) - (t2 - t1) }
}

/// One SNTP exchange with `server` (`host:port`).
pub async fn measure(server: &str) -> anyhow::Result<Sample> {
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    sock.connect(server).await?;
    let mut req = [0u8; 48];
    req[0] = 0x23; // LI 0, version 4, mode 3 (client)
    let t0 = to_ntp(SystemTime::now());
    req[40..48].copy_from_slice(&t0.to_be_bytes());
    sock.send(&req).await?;
    let mut resp = [0u8; 48];
    let n = tokio::time::timeout(Duration::from_secs(3), sock.recv(&mut resp)).await??;
    let t3 = to_ntp(SystemTime::now());
    anyhow::ensure!(n == 48 && resp[0] & 0x07 == 4, "not an NTP server reply");
    anyhow::ensure!(resp[1] != 0, "kiss-of-death (stratum 0)");
    let ts = |i: usize| u64::from_be_bytes(resp[i..i + 8].try_into().unwrap());
    anyhow::ensure!(ts(24) == t0, "reply is not for this request");
    Ok(sample(t0, ts(32), ts(40), t3))
}

/// Measures at start and then every `every`, logging each sample. Of a few tries the one with
/// the shortest round trip is kept: its offset has the smallest error bound (rtt / 2).
pub async fn watch(server: String, every: Duration) {
    loop {
        let mut best: Option<Sample> = None;
        for _ in 0..4 {
            match measure(&server).await {
                Ok(s) if best.is_none_or(|b| s.rtt_ms < b.rtt_ms) => best = Some(s),
                Ok(_) => {}
                Err(e) => tracing::debug!(%server, error = %e, "ntp probe failed"),
            }
        }
        let best = best.map(|s| Sample { offset_ms: (s.offset_ms * 10.0).round() / 10.0, rtt_ms: (s.rtt_ms * 10.0).round() / 10.0 });
        match best {
            Some(s) if s.offset_ms.abs() > WARN_MS => {
                tracing::warn!(%server, offset_ms = s.offset_ms, rtt_ms = s.rtt_ms, "host clock is off; latency figures shift by the offset")
            }
            Some(s) => tracing::info!(%server, offset_ms = s.offset_ms, rtt_ms = s.rtt_ms, "clock offset"),
            None => tracing::warn!(%server, "clock offset unknown: no NTP reply"),
        }
        tokio::time::sleep(every).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ntp_round_trips_through_unix_time() {
        let t = UNIX_EPOCH + Duration::from_millis(1_761_300_000_123);
        assert!((ntp_ms(to_ntp(t)) - 1_761_300_000_123.0).abs() < 0.001);
    }

    #[test]
    fn offset_and_rtt_from_the_four_timestamps() {
        // Server 40 ms ahead; 10 ms each way; 2 ms inside the server.
        let ms = |v: u64| to_ntp(UNIX_EPOCH + Duration::from_millis(v));
        let s = sample(ms(1_000), ms(1_050), ms(1_052), ms(1_022));
        assert!((s.offset_ms - 40.0).abs() < 0.01 && (s.rtt_ms - 20.0).abs() < 0.01, "{s:?}");
    }
}
