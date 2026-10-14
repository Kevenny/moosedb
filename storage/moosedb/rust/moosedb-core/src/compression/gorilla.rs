//! Gorilla XOR encoding of floating point values (Pelkonen et al., VLDB 2015).
//!
//! The first value is stored verbatim; each following value is XORed with
//! its predecessor. Identical values cost one bit; slowly changing values
//! share sign/exponent bits and only the "meaningful" middle bits are stored.
//! Works on the IEEE-754 bit pattern, so every value (NaN payloads, -0.0,
//! infinities) round-trips exactly. `width` is 64 for f64 and 32 for f32.

use crate::error::{corrupt, Result};

struct BitWriter {
    buf: Vec<u8>,
    /// Pending bits not yet in `buf`: the low `used` (< 8) bits of `acc`.
    acc: u64,
    used: u32,
}

impl BitWriter {
    fn new(capacity: usize) -> BitWriter {
        BitWriter { buf: Vec::with_capacity(capacity), acc: 0, used: 0 }
    }

    #[inline]
    fn bit(&mut self, b: bool) {
        self.bits(u64::from(b), 1);
    }

    /// Appends the low `n <= 64` bits of `v`, most significant first.
    #[inline]
    fn bits(&mut self, v: u64, n: u32) {
        if n == 0 {
            return;
        }
        if n > 32 {
            // `used < 8` plus 32 new bits always fits the 64-bit accumulator.
            self.bits(v >> 32, n - 32);
            self.bits(v & 0xFFFF_FFFF, 32);
            return;
        }
        let v = v & ((1u64 << n) - 1);
        self.acc = (self.acc << n) | v;
        self.used += n;
        while self.used >= 8 {
            self.used -= 8;
            self.buf.push((self.acc >> self.used) as u8);
        }
        self.acc &= (1u64 << self.used) - 1;
    }

    /// Flushes the partial last byte (padded with zero bits) and returns the stream.
    fn finish(mut self) -> Vec<u8> {
        if self.used > 0 {
            self.buf.push((self.acc << (8 - self.used)) as u8);
        }
        self.buf
    }
}

struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl BitReader<'_> {
    fn bit(&mut self) -> Result<bool> {
        let byte = self.data.get(self.pos / 8).ok_or_else(|| corrupt("gorilla stream truncated"))?;
        let b = byte & (0x80 >> (self.pos % 8)) != 0;
        self.pos += 1;
        Ok(b)
    }

    /// The next `n <= 64` bits, most significant first.
    fn bits(&mut self, n: u32) -> Result<u64> {
        if n == 0 {
            return Ok(0);
        }
        let end = self
            .pos
            .checked_add(n as usize)
            .filter(|&e| e <= self.data.len() * 8)
            .ok_or_else(|| corrupt("gorilla stream truncated"))?;
        let (byte, shift) = (self.pos / 8, (self.pos % 8) as u32);
        // Fast path: one 64-bit big-endian load (plus a ninth byte if the
        // field straddles it); the last bytes of the stream use the bit loop.
        let word = self.data.get(byte..byte + 8).and_then(|c| <[u8; 8]>::try_from(c).ok()).map(u64::from_be_bytes);
        let v = match word {
            Some(w) if shift + n <= 64 => (w << shift) >> (64 - n),
            Some(w) => {
                let ninth = u64::from(*self.data.get(byte + 8).ok_or_else(|| corrupt("gorilla stream truncated"))?);
                let extra = shift + n - 64;
                ((w << shift) >> (64 - n)) | (ninth >> (8 - extra))
            }
            None => {
                let mut v = 0u64;
                for i in 0..n as usize {
                    let p = self.pos + i;
                    v = (v << 1) | u64::from(self.data[p / 8] & (0x80 >> (p % 8)) != 0);
                }
                v
            }
        };
        self.pos = end;
        Ok(v)
    }
}

pub(crate) fn encode(values: &[u64], width: u32, out: &mut Vec<u8>) {
    let Some(&first) = values.first() else { return };
    let mut w = BitWriter::new(values.len() * 2);
    w.bits(first, width);
    let mut prev = first;
    let mut window: Option<(u32, u32)> = None; // (leading, trailing) of the last stored XOR
    for &v in &values[1..] {
        let x = v ^ prev;
        prev = v;
        if x == 0 {
            w.bit(false);
            continue;
        }
        w.bit(true);
        let lead = (x.leading_zeros() - (64 - width)).min(31);
        let trail = x.trailing_zeros();
        match window {
            Some((pl, pt)) if lead >= pl && trail >= pt => {
                w.bit(false);
                w.bits(x >> pt, width - pl - pt);
            }
            _ => {
                w.bit(true);
                let len = width - lead - trail;
                w.bits(u64::from(lead), 5);
                w.bits(u64::from(len - 1), 6);
                w.bits(x >> trail, len);
                window = Some((lead, trail));
            }
        }
    }
    out.extend_from_slice(&w.finish());
}

#[cfg(test)]
pub(crate) fn decode(data: &[u8], count: usize, width: u32) -> Result<Vec<u64>> {
    decode_prefix(data, count, count, width)
}

/// Decodes the first `upto` of `count` values; the tail is not examined when
/// `upto < count` (the block was CRC-verified by the caller).
pub(crate) fn decode_prefix(data: &[u8], count: usize, upto: usize, width: u32) -> Result<Vec<u64>> {
    if count == 0 {
        return if data.is_empty() { Ok(Vec::new()) } else { Err(corrupt("gorilla stream for zero values")) };
    }
    let upto = upto.min(count);
    if upto == 0 {
        return Ok(Vec::new());
    }
    let mut r = BitReader { data, pos: 0 };
    let mut out = Vec::with_capacity(upto.min(data.len() * 8));
    let mut prev = r.bits(width)?;
    out.push(prev);
    let mut window: Option<(u32, u32)> = None;
    while out.len() < upto {
        if r.bit()? {
            let (lead, len) = if r.bit()? {
                let lead = r.bits(5)? as u32;
                let len = r.bits(6)? as u32 + 1;
                if lead + len > width {
                    return Err(corrupt("gorilla window exceeds value width"));
                }
                window = Some((lead, width - lead - len));
                (lead, len)
            } else {
                let (pl, pt) = window.ok_or_else(|| corrupt("gorilla reuses a window before defining one"))?;
                (pl, width - pl - pt)
            };
            let trail = width - lead - len;
            prev ^= r.bits(len)? << trail;
        }
        out.push(prev);
    }
    // Only zero padding may follow the last value.
    if upto == count && r.pos.div_ceil(8) != data.len() {
        return Err(corrupt("trailing bytes after gorilla stream"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt64(values: &[f64]) -> usize {
        let bits: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let mut buf = Vec::new();
        encode(&bits, 64, &mut buf);
        assert_eq!(decode(&buf, bits.len(), 64).unwrap(), bits);
        buf.len()
    }

    #[test]
    fn slowly_changing_series_compress() {
        let vals: Vec<f64> = (0..10_000).map(|i| 20.0 + ((i / 50) as f64) * 0.25).collect();
        let n = rt64(&vals);
        assert!(n < 10_000 * 8 / 20, "{n} bytes");
    }

    #[test]
    fn special_values_roundtrip() {
        rt64(&[]);
        rt64(&[1.0]);
        rt64(&[f64::NAN, -0.0, 0.0, f64::INFINITY, f64::NEG_INFINITY, f64::MIN_POSITIVE, f64::MAX, 1e-300]);
        rt64(&(0..1000).map(|i| (i as f64).sin()).collect::<Vec<_>>());
    }

    #[test]
    fn word_reads_match_the_bit_loop_at_every_offset_and_width() {
        let data: Vec<u8> = (0..40u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8).collect();
        for start in 0..data.len() * 8 {
            for n in [0u32, 1, 2, 5, 7, 8, 9, 31, 32, 33, 56, 57, 63, 64] {
                let mut fast = BitReader { data: &data, pos: start };
                let mut slow = BitReader { data: &data, pos: start };
                let mut want = 0u64;
                let mut ok = true;
                for _ in 0..n {
                    match slow.bit() {
                        Ok(b) => want = (want << 1) | u64::from(b),
                        Err(_) => ok = false,
                    }
                }
                match fast.bits(n) {
                    Ok(v) => assert!(ok && v == want, "start {start} n {n}"),
                    Err(_) => assert!(!ok, "start {start} n {n}"),
                }
            }
        }
    }

    #[test]
    fn prefixes_match_the_full_decode() {
        let bits: Vec<u64> = (0..500).map(|i| ((i as f64 * 0.37).sin() * 100.0).to_bits()).collect();
        let mut buf = Vec::new();
        encode(&bits, 64, &mut buf);
        for upto in [0, 1, 2, 250, 499, 500, 800] {
            assert_eq!(decode_prefix(&buf, bits.len(), upto, 64).unwrap(), bits[..upto.min(500)], "upto {upto}");
        }
        assert!(decode_prefix(&buf[..buf.len() / 2], bits.len(), 499, 64).is_err());
    }

    #[test]
    fn bit_writer_matches_the_bit_by_bit_layout() {
        // Reference: one bit at a time, most significant first, zero padded.
        let mut state = 0x1234_5678_9ABC_DEF0u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut w = BitWriter::new(0);
        let mut reference: Vec<bool> = Vec::new();
        for _ in 0..5000 {
            let n = (next() % 65) as u32;
            let v = next();
            w.bits(v, n);
            for i in (0..n).rev() {
                reference.push((v >> i) & 1 == 1);
            }
            let b = next() % 2 == 1;
            w.bit(b);
            reference.push(b);
        }
        let mut want = vec![0u8; reference.len().div_ceil(8)];
        for (i, &b) in reference.iter().enumerate() {
            if b {
                want[i / 8] |= 0x80 >> (i % 8);
            }
        }
        assert_eq!(w.finish(), want);
    }

    /// Streams written by the bit-by-bit writer that preceded the 64-bit
    /// accumulator (bytes captured from it): the new encoder must reproduce
    /// them exactly and the decoder must read them.
    #[test]
    #[allow(clippy::approx_constant)] // arbitrary values of the captured fixture
    fn streams_of_the_old_bit_writer_are_unchanged() {
        let f64s: Vec<u64> =
            [20.0f64, 20.0, 20.25, 20.5, 20.5, 19.75, -0.0, 0.0, 1e-300, f64::MAX, 3.14159, 3.14159, 2.71828]
                .iter()
                .map(|f| f.to_bits())
                .collect();
        let old64: &[u8] = &[
            0x40, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x71, 0x03, 0xc0, 0x1d, 0xb4, 0x4e, 0xe0, 0x47, 0x00, 0xcf,
            0xa0, 0x00, 0x0c, 0xfc, 0x69, 0x5b, 0x87, 0xf0, 0xbe, 0x3c, 0xd6, 0x70, 0xfb, 0xf9, 0x2a, 0x47, 0x80, 0xf4,
            0x1c, 0x32, 0x9e, 0x2f, 0x7f, 0xe6, 0xde, 0x06, 0x0f, 0xe4, 0x79, 0x91, 0x40, 0x06, 0x4f, 0x78, 0x32, 0xd8,
            0xb8, 0xff, 0x00,
        ];
        let mut out = Vec::new();
        encode(&f64s, 64, &mut out);
        assert_eq!(out, old64);
        assert_eq!(decode(old64, f64s.len(), 64).unwrap(), f64s);

        let f32s: Vec<u64> = [1.5f32, 1.5, -2.25, 0.1, 3.4e38, 0.1].iter().map(|f| u64::from(f.to_bits())).collect();
        let old32: &[u8] = &[
            0x3f, 0xc0, 0x00, 0x00, 0x60, 0x2f, 0xff, 0x70, 0x3f, 0xfb, 0xb9, 0x99, 0x9b, 0x21, 0x59, 0x82, 0xa9, 0xc8,
            0x56, 0x60, 0xaa, 0x60,
        ];
        let mut out = Vec::new();
        encode(&f32s, 32, &mut out);
        assert_eq!(out, old32);
        assert_eq!(decode(old32, f32s.len(), 32).unwrap(), f32s);
    }

    #[test]
    fn f32_width() {
        let vals: Vec<u64> =
            [1.5f32, 1.5, -2.25, f32::NAN, 0.1, 3.4e38].iter().map(|v| u64::from(v.to_bits())).collect();
        let mut buf = Vec::new();
        encode(&vals, 32, &mut buf);
        assert_eq!(decode(&buf, vals.len(), 32).unwrap(), vals);
    }

    #[test]
    fn truncation_detected() {
        let bits: Vec<u64> = (0..100).map(|i| (i as f64 * 1.1).to_bits()).collect();
        let mut buf = Vec::new();
        encode(&bits, 64, &mut buf);
        assert!(decode(&buf[..buf.len() / 2], bits.len(), 64).is_err());
    }
}
