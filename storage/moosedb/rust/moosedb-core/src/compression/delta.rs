//! Delta and delta-of-delta encoding of signed integers (timestamps, counters).
//!
//! ```text
//! stream := first:i64 simple8b(zigzag(d1), zigzag(d2), ...)
//! ```
//!
//! With delta-of-delta, regular intervals (every 10 s, every 1 min) encode to
//! runs of zeros that Simple8b packs 240 per word. All arithmetic wraps, so
//! any i64 sequence round-trips.

use super::simple8b;
use crate::bytes::{put_i64, ByteReader};
use crate::error::Result;

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

/// `order` 1 = delta, 2 = delta-of-delta.
fn encode(values: &[i64], order: u8, out: &mut Vec<u8>) {
    let Some(&first) = values.first() else { return };
    put_i64(out, first);
    let mut prev_delta = 0i64;
    let residuals: Vec<u64> = values
        .windows(2)
        .map(|w| {
            let d = w[1].wrapping_sub(w[0]);
            let r = if order == 2 { d.wrapping_sub(prev_delta) } else { d };
            prev_delta = d;
            zigzag(r)
        })
        .collect();
    simple8b::encode(&residuals, out);
}

#[cfg(test)]
fn decode(r: &mut ByteReader<'_>, count: usize, order: u8) -> Result<Vec<i64>> {
    decode_prefix(r, count, count, order)
}

/// Decodes the first `upto` of `count` values (`upto >= count` = all).
fn decode_prefix(r: &mut ByteReader<'_>, count: usize, upto: usize, order: u8) -> Result<Vec<i64>> {
    if count == 0 || upto == 0 {
        return Ok(Vec::new());
    }
    let first = r.i64()?;
    let residuals = simple8b::decode_prefix(r, count - 1, upto.min(count) - 1)?;
    // `residuals` is as long as the stream really holds: never reserve from `count`.
    let mut out = Vec::with_capacity(residuals.len() + 1);
    out.push(first);
    let (mut prev, mut prev_delta) = (first, 0i64);
    for z in residuals {
        let res = unzigzag(z);
        let d = if order == 2 { prev_delta.wrapping_add(res) } else { res };
        prev = prev.wrapping_add(d);
        prev_delta = d;
        out.push(prev);
    }
    Ok(out)
}

pub(crate) fn encode_delta(values: &[i64], out: &mut Vec<u8>) {
    encode(values, 1, out);
}

#[cfg(test)]
pub(crate) fn decode_delta(r: &mut ByteReader<'_>, count: usize) -> Result<Vec<i64>> {
    decode(r, count, 1)
}

pub(crate) fn decode_delta_prefix(r: &mut ByteReader<'_>, count: usize, upto: usize) -> Result<Vec<i64>> {
    decode_prefix(r, count, upto, 1)
}

pub(crate) fn decode_delta_of_delta_prefix(r: &mut ByteReader<'_>, count: usize, upto: usize) -> Result<Vec<i64>> {
    decode_prefix(r, count, upto, 2)
}

pub(crate) fn encode_delta_of_delta(values: &[i64], out: &mut Vec<u8>) {
    encode(values, 2, out);
}

#[cfg(test)]
pub(crate) fn decode_delta_of_delta(r: &mut ByteReader<'_>, count: usize) -> Result<Vec<i64>> {
    decode(r, count, 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_roundtrip() {
        for v in [0, 1, -1, 63, -64, i64::MAX, i64::MIN] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
    }

    #[test]
    fn regular_timestamps_compress_to_almost_nothing() {
        let ts: Vec<i64> = (0..10_000).map(|i| 1_700_000_000_000_000 + i * 10_000_000).collect();
        let mut buf = Vec::new();
        encode_delta_of_delta(&ts, &mut buf);
        assert!(buf.len() < 400, "{} bytes", buf.len());
        assert_eq!(decode_delta_of_delta(&mut ByteReader::new(&buf), ts.len()).unwrap(), ts);
    }

    #[test]
    fn a_forged_count_cannot_reserve_gigabytes() {
        // 4.5 MB of selector-0 words (240 zeros each) encode 2^27 values: the
        // stream is real, so the count must be refused before it is expanded.
        let count = 1usize << 27;
        let mut buf = Vec::new();
        buf.extend_from_slice(&0i64.to_le_bytes());
        buf.push(0); // packed mode
        for _ in 0..count.div_ceil(240) {
            buf.extend_from_slice(&0u64.to_le_bytes());
        }
        assert!(decode_delta(&mut ByteReader::new(&buf), count).is_err());
        assert!(decode_delta_of_delta(&mut ByteReader::new(&buf), usize::MAX).is_err());
    }

    #[test]
    fn prefixes_match_the_full_decode() {
        let vals: Vec<i64> = (0..700).map(|i| i * i * 3 - 5000 + (i % 5)).collect();
        for order in [1u8, 2] {
            let mut buf = Vec::new();
            encode(&vals, order, &mut buf);
            for upto in [0, 1, 2, 240, 241, 699, 700, 900] {
                let got = decode_prefix(&mut ByteReader::new(&buf), vals.len(), upto, order).unwrap();
                assert_eq!(got, vals[..upto.min(vals.len())], "order {order} upto {upto}");
            }
        }
    }

    #[test]
    fn extremes_roundtrip() {
        let vals = vec![i64::MIN, i64::MAX, 0, -1, i64::MIN, 5, 5, 5];
        for order in [1, 2] {
            let mut buf = Vec::new();
            encode(&vals, order, &mut buf);
            assert_eq!(decode(&mut ByteReader::new(&buf), vals.len(), order).unwrap(), vals);
        }
        let mut buf = Vec::new();
        encode_delta(&[], &mut buf);
        assert!(buf.is_empty());
        assert!(decode_delta(&mut ByteReader::new(&buf), 0).unwrap().is_empty());
        encode_delta(&[42], &mut buf);
        assert_eq!(decode_delta(&mut ByteReader::new(&buf), 1).unwrap(), vec![42]);
    }
}
