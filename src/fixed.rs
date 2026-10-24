//! Exact decimal-string <-> fixed-point conversion. `1_000_000` = 1 USDC (or 1 share).
//!
//! Prices and sizes never pass through `f64`: the exchange's checksum is computed over
//! the decimal strings, so the round trip `parse -> format` must reproduce them exactly.

pub const SCALE: i64 = 1_000_000;
const DP: usize = 6;

/// Fixed-point price or quantity.
pub type Fx = i64;

/// Parses a non-negative decimal string such as `"0.531"` or `"31007643.55"`.
/// Rejects signs, exponents, and more than 6 decimal places (which would lose precision).
pub fn parse(s: &str) -> Option<Fx> {
    let (int, frac) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    if int.is_empty() && frac.is_empty() || frac.len() > DP {
        return None;
    }
    let digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    if !digits(int) || !digits(frac) {
        return None;
    }
    let int: i64 = if int.is_empty() { 0 } else { int.parse().ok()? };
    let mut f: i64 = if frac.is_empty() {
        0
    } else {
        frac.parse().ok()?
    };
    for _ in frac.len()..DP {
        f *= 10;
    }
    int.checked_mul(SCALE)?.checked_add(f)
}

/// Formats with trailing zeros trimmed, the exchange's canonical form (`500_000` -> `"0.5"`).
pub fn format(v: Fx, out: &mut String) {
    let mut b = [0; 24];
    out.push_str(std::str::from_utf8(encode(v, &mut b)).expect("ascii digits"));
}

/// [`format()`] into a stack buffer, returning the used tail. No `fmt` and no allocation:
/// the hash check runs this for every level of every book it digests.
pub fn encode(v: Fx, buf: &mut [u8; 24]) -> &[u8] {
    debug_assert!(v >= 0, "prices and sizes are non-negative");
    let (i, mut f) = ((v / SCALE) as u64, (v % SCALE) as u64);
    let mut p = buf.len();
    if f != 0 {
        let mut n = DP;
        while f % 10 == 0 {
            f /= 10;
            n -= 1;
        }
        for _ in 0..n {
            p -= 1;
            buf[p] = b'0' + (f % 10) as u8;
            f /= 10;
        }
        p -= 1;
        buf[p] = b'.';
    }
    let used = encode_int(i, &mut buf[..p]);
    let start = p - used;
    &buf[start..]
}

/// Writes `v`'s decimal digits at the end of `buf`, returning how many were written.
pub fn encode_int(mut v: u64, buf: &mut [u8]) -> usize {
    let mut p = buf.len();
    loop {
        p -= 1;
        buf[p] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    buf.len() - p
}

pub fn to_string(v: Fx) -> String {
    let mut s = String::new();
    format(v, &mut s);
    s
}

/// Formats with exactly three decimals (`500_000` -> `"0.500"`).
pub fn format_3dp(v: Fx) -> String {
    format!("{}.{:03}", v / SCALE, (v % SCALE) / 1000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn known_values() {
        assert_eq!(parse("0.531"), Some(531_000));
        assert_eq!(parse("31007643.55"), Some(31_007_643_550_000));
        assert_eq!(parse("30"), Some(30_000_000));
        assert_eq!(parse(".5"), Some(500_000));
        assert_eq!(parse("0.0000001"), None);
        assert_eq!(parse("-1"), None);
        assert_eq!(parse("1e3"), None);
        assert_eq!(parse(""), None);
        assert_eq!(to_string(500_000), "0.5");
        assert_eq!(to_string(30_000_000), "30");
        assert_eq!(format_3dp(500_000), "0.500");
        assert_eq!(format_3dp(1_000_000), "1.000");
        assert_eq!(to_string(0), "0");
        assert_eq!(to_string(1), "0.000001");
        assert_eq!(to_string(i64::MAX), "9223372036854.775807");
    }

    proptest! {
        #[test]
        fn round_trip(v in 0i64..i64::MAX / 2) {
            prop_assert_eq!(parse(&to_string(v)), Some(v));
        }

        #[test]
        fn canonical_strings_round_trip(i in 0u32..10_000_000, f in 0u32..1_000_000) {
            let mut s = i.to_string();
            let mut frac = format!("{f:06}");
            while frac.ends_with('0') { frac.pop(); }
            if !frac.is_empty() { s = format!("{s}.{frac}"); }
            prop_assert_eq!(to_string(parse(&s).unwrap()), s);
        }
    }
}
