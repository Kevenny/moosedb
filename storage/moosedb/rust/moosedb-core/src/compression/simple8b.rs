//! Simple8b: packs many small unsigned integers into 64-bit words.
//!
//! Each word = 4-bit selector + 60 bits of payload. The selector says how many
//! values the word holds and with how many bits each. Values above 2^60-1
//! cannot be packed; streams containing one fall back to raw u64s, flagged by
//! the leading mode byte:
//!
//! ```text
//! stream := 0 word*        (Simple8b)
//!         | 1 u64*         (raw)
//! ```

use crate::bytes::{put_u64, put_u8, ByteReader};
use crate::error::{corrupt, Result};

const MAX_VALUE: u64 = (1 << 60) - 1;
const MODE_PACKED: u8 = 0;
const MODE_RAW: u8 = 1;

/// (values per word, bits per value). Selectors 0 and 1 encode runs of zeros.
const SELECTORS: [(usize, u32); 16] = [
    (240, 0),
    (120, 0),
    (60, 1),
    (30, 2),
    (20, 3),
    (15, 4),
    (12, 5),
    (10, 6),
    (8, 7),
    (7, 8),
    (6, 10),
    (5, 12),
    (4, 15),
    (3, 20),
    (2, 30),
    (1, 60),
];

pub(crate) fn encode(values: &[u64], out: &mut Vec<u8>) {
    if values.iter().any(|&v| v > MAX_VALUE) {
        put_u8(out, MODE_RAW);
        for &v in values {
            put_u64(out, v);
        }
        return;
    }
    put_u8(out, MODE_PACKED);
    let mut i = 0;
    while i < values.len() {
        let rest = &values[i..];
        for (sel, &(n, bits)) in SELECTORS.iter().enumerate() {
            if n > rest.len() {
                continue;
            }
            let group = &rest[..n];
            let fits = if bits == 0 { group.iter().all(|&v| v == 0) } else { group.iter().all(|&v| v >> bits == 0) };
            if !fits {
                continue;
            }
            let mut word = (sel as u64) << 60;
            if bits > 0 {
                for (k, &v) in group.iter().enumerate() {
                    word |= v << (k as u32 * bits);
                }
            }
            put_u64(out, word);
            i += n;
            break;
        }
        // Selector 15 (one 60-bit value) always fits, so the loop always advances.
    }
}

/// Decodes exactly `count` values; the reader must hold nothing else.
pub(crate) fn decode(r: &mut ByteReader<'_>, count: usize) -> Result<Vec<u64>> {
    if count == 0 && r.remaining() == 0 {
        return Ok(Vec::new());
    }
    // A few bytes can claim an enormous count (240 zeros per word): never
    // expand more than the decode limit allows, whatever the stream says.
    if count > super::MAX_DECODE_BYTES / 8 {
        return Err(corrupt(format!("integer stream claims {count} values")));
    }
    match r.u8()? {
        MODE_RAW => {
            if count.checked_mul(8) != Some(r.remaining()) {
                return Err(corrupt("raw integer stream length mismatch"));
            }
            (0..count).map(|_| r.u64()).collect()
        }
        MODE_PACKED => {
            if r.remaining() % 8 != 0 {
                return Err(corrupt("simple8b stream is not word aligned"));
            }
            let mut out = Vec::with_capacity(count.min(r.remaining() / 8 * 240));
            while out.len() < count {
                let word = r.u64()?;
                let (n, bits) = SELECTORS[(word >> 60) as usize];
                if out.len() + n > count {
                    return Err(corrupt("simple8b stream holds more values than expected"));
                }
                if bits == 0 {
                    out.resize(out.len() + n, 0);
                } else {
                    let mask = (1u64 << bits) - 1;
                    out.extend((0..n).map(|k| (word >> (k as u32 * bits)) & mask));
                }
            }
            if r.remaining() != 0 {
                return Err(corrupt("trailing bytes after simple8b stream"));
            }
            Ok(out)
        }
        m => Err(corrupt(format!("unknown integer stream mode {m}"))),
    }
}

/// Decodes the first `upto` of `count` values. The rest of the stream is not
/// examined (no trailing-bytes check): callers use it to read a leading part
/// of a column whose block was already CRC-verified. `upto >= count` is the
/// strict [`decode`].
pub(crate) fn decode_prefix(r: &mut ByteReader<'_>, count: usize, upto: usize) -> Result<Vec<u64>> {
    if upto >= count {
        return decode(r, count);
    }
    if count > super::MAX_DECODE_BYTES / 8 {
        return Err(corrupt(format!("integer stream claims {count} values")));
    }
    match r.u8()? {
        MODE_RAW => {
            if count.checked_mul(8) != Some(r.remaining()) {
                return Err(corrupt("raw integer stream length mismatch"));
            }
            (0..upto).map(|_| r.u64()).collect()
        }
        MODE_PACKED => {
            if r.remaining() % 8 != 0 {
                return Err(corrupt("simple8b stream is not word aligned"));
            }
            let mut out = Vec::with_capacity(upto.min(r.remaining() / 8 * 240));
            let mut total = 0usize;
            while out.len() < upto {
                let word = r.u64()?;
                let (n, bits) = SELECTORS[(word >> 60) as usize];
                total += n;
                if total > count {
                    return Err(corrupt("simple8b stream holds more values than expected"));
                }
                if bits == 0 {
                    out.resize(out.len() + n, 0);
                } else {
                    let mask = (1u64 << bits) - 1;
                    out.extend((0..n).map(|k| (word >> (k as u32 * bits)) & mask));
                }
            }
            out.truncate(upto);
            Ok(out)
        }
        m => Err(corrupt(format!("unknown integer stream mode {m}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_decoding_matches_the_full_decode() {
        let values: Vec<u64> = (0..1000u64).map(|i| if i % 7 == 0 { 0 } else { i * 31 % 1000 }).collect();
        let mut buf = Vec::new();
        encode(&values, &mut buf);
        let mut raw = Vec::new();
        encode(&[u64::MAX, 3, 4, 5], &mut raw);
        for upto in [0, 1, 239, 240, 241, 999, 1000] {
            let got = decode_prefix(&mut ByteReader::new(&buf), values.len(), upto).unwrap();
            assert_eq!(got, values[..upto], "upto {upto}");
        }
        assert_eq!(decode_prefix(&mut ByteReader::new(&raw), 4, 2).unwrap(), vec![u64::MAX, 3]);
        assert!(decode_prefix(&mut ByteReader::new(&buf), 3, 2).is_err(), "count smaller than the stream");
    }

    fn roundtrip(values: &[u64]) -> usize {
        let mut buf = Vec::new();
        encode(values, &mut buf);
        assert_eq!(decode(&mut ByteReader::new(&buf), values.len()).unwrap(), values);
        buf.len()
    }

    #[test]
    fn packs_small_values_densely() {
        let zeros = vec![0u64; 1000];
        assert_eq!(roundtrip(&zeros), 1 + 8 * 6, "4x240 zeros + 30 + 10");
        let small: Vec<u64> = (0..1000).map(|i| i % 4).collect();
        assert!(roundtrip(&small) <= 1 + 8 * 34);
    }

    #[test]
    fn mixed_and_edge_values() {
        roundtrip(&[]);
        roundtrip(&[MAX_VALUE, 0, 1, MAX_VALUE]);
        roundtrip(&[1, 2, 3, 1 << 30, 7, 0, 0, 0]);
        let n = roundtrip(&[u64::MAX, 1]);
        assert_eq!(n, 1 + 16, "values over 60 bits fall back to raw");
    }

    #[test]
    fn rejects_count_mismatch() {
        let mut buf = Vec::new();
        encode(&[1, 2, 3], &mut buf);
        assert!(decode(&mut ByteReader::new(&buf), 2).is_err());
        assert!(decode(&mut ByteReader::new(&buf), 300).is_err());
    }
}
