//! Fast hasher for hash tables keyed by **internal** integers only.
//!
//! The standard `HashMap` hashes with SipHash-1-3 and a per-map random key:
//! robust against hash flooding but costly for the millions of `u64` lookups
//! the write and read paths do. This is a folded-multiply hasher with one
//! per-process seed. It has no flooding-resistance guarantee, so it is **only**
//! for keys a client cannot choose: series ids, chunk file uids, row positions
//! and the like, assigned by the engine. A map keyed by client data (tag bytes,
//! timestamps, key-column values) must use `std::collections::HashMap`.
//!
//! Not stable across processes and never persisted: it only picks buckets.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::OnceLock;

/// `HashMap` with [`FastState`].
pub(crate) type FastMap<K, V> = HashMap<K, V, FastState>;

const K1: u64 = 0x9E37_79B9_7F4A_7C15;
const K2: u64 = 0xD6E8_FEB8_6659_FD93;

/// Per-process seed, odd and never zero.
fn seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| RandomState::new().hash_one(0x006d_6f6f_7365_6462_u64) | 1)
}

#[inline]
fn fold_mul(a: u64, b: u64) -> u64 {
    let p = u128::from(a) * u128::from(b);
    (p as u64) ^ ((p >> 64) as u64)
}

#[derive(Clone, Copy)]
pub(crate) struct FastState {
    seed: u64,
}

impl Default for FastState {
    fn default() -> Self {
        FastState { seed: seed() }
    }
}

impl BuildHasher for FastState {
    type Hasher = FastHasher;

    #[inline]
    fn build_hasher(&self) -> FastHasher {
        FastHasher { acc: self.seed }
    }
}

pub(crate) struct FastHasher {
    acc: u64,
}

impl FastHasher {
    #[inline]
    fn mix(&mut self, word: u64) {
        self.acc = fold_mul(self.acc ^ word, K1);
    }
}

impl Hasher for FastHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            let mut w = [0u8; 8];
            w.copy_from_slice(c);
            self.mix(u64::from_le_bytes(w));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut w = [0u8; 8];
            w[..rest.len()].copy_from_slice(rest);
            // The length keeps "ab\0" and "ab" apart.
            self.mix(u64::from_le_bytes(w) ^ ((rest.len() as u64) << 56));
        }
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.mix(u64::from(i));
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.mix(u64::from(i));
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.mix(i);
    }

    #[inline]
    fn write_i64(&mut self, i: i64) {
        self.mix(i as u64);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.mix(i as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        fold_mul(self.acc, K2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn behaves_as_a_map_and_separates_lengths() {
        let mut m: FastMap<Vec<u8>, u64> = FastMap::default();
        for i in 0..10_000u64 {
            m.insert(i.to_le_bytes().to_vec(), i);
        }
        m.insert(b"ab".to_vec(), 1);
        m.insert(b"ab\0".to_vec(), 2);
        m.insert(Vec::new(), 3);
        assert_eq!(m.len(), 10_003);
        assert_eq!(m.get(&b"ab"[..]), Some(&1));
        assert_eq!(m.get(&b"ab\0"[..]), Some(&2));
        assert_eq!(m.get(&7u64.to_le_bytes()[..]), Some(&7));
    }

    #[test]
    fn sequential_u64_keys_spread_over_buckets() {
        let st = FastState::default();
        let mut low = std::collections::HashSet::new();
        for i in 0..4096u64 {
            low.insert(st.hash_one(i) & 0xfff);
        }
        // A good hash fills ~63% of 4096 buckets with 4096 keys.
        assert!(low.len() > 2200, "{}", low.len());
    }
}
