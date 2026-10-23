//! The MANIFEST is the single source of truth for which chunk files are live
//! and how much of the WAL they cover. Every structural change (flush,
//! truncate, retention, compaction) follows the same protocol:
//!
//! 1. write the new files (chunks) and fsync them,
//! 2. atomically replace the MANIFEST (the commit point),
//! 3. delete what the new MANIFEST no longer references.
//!
//! Recovery deletes chunk files not listed in the MANIFEST and WAL segments
//! with `seq < replay_seq`, so a crash at any step leaves a consistent table.
//!
//! ```text
//! v2: magic "TFLM" | version:u16 | reserved:u16 | wal_seq:u64 | next_chunk_id:u64
//!     | replay_seq:u64 | chunk_count:u32 | chunk_id:u64 * chunk_count | crc32:u32
//! v1: same without `replay_seq` (read as `wal_seq + 1`)
//! v3: v2 header, then
//!     tseq:u64                                   highest tombstone sequence ever assigned
//!     chunk_count:u32 | (chunk_id:u64 born:u64) * chunk_count
//!     range_count:u32 | range tombstone * range_count     (see `tomb::encode_range`)
//!     rows_count:u32  | row tombstone * rows_count        (see `tomb::encode_rows`)
//!     crc32:u32
//! ```
//!
//! A table that never had a `DELETE` keeps writing v2 byte for byte (an older
//! binary still opens it); the first tombstone makes the MANIFEST v3, which an
//! older binary refuses ("version 3 not supported") instead of showing rows
//! that were deleted.
//!
//! An **encrypted** table writes v4 instead, from its first MANIFEST on:
//!
//! ```text
//! v4: magic "TFLM" | version:u16 = 4 | reserved:u16 = 0
//!     | key_id:u32 key_version:u32 iv[16] reserved:u32 crc:u32     (`CipherParams`, 32 bytes)
//!     | AES-256-GCM(body) | tag[16]
//! body: wal_seq:u64 next_chunk_id:u64 replay_seq:u64 tseq:u64
//!       chunk_count:u32 | (chunk_id:u64 born:u64 chunk_iv[16]) * chunk_count
//!       range_count:u32 | ... | rows_count:u32 | ...                (as v3)
//! ```
//!
//! The tag covers everything: the 40 header bytes plus the unit tag
//! `UNIT_MANIFEST` are the AAD, the body is the ciphertext. The IV is fresh
//! random per write (a MANIFEST is rewritten at every commit; the nonce is
//! never reused) and the key is the latest version of the table's key id, like
//! any new file. `chunk_iv` is the IV of each listed chunk file, checked when
//! the table opens (`Table::open`): a chunk copied from another table or an
//! older copy of the same id has another IV and is refused. A v4 that fails
//! authentication is an error, never an empty table.
//!
//! A v1/v2/v3 MANIFEST of an encrypted table (written before v4) is read as
//! before, with a warning, and rewritten as v4 by that same open. Because an
//! attacker could write such a MANIFEST on purpose, `Table::open` refuses it
//! when any chunk or WAL segment carries the "written by an authenticating
//! version" flag (`chunk::flags::AUTH_MANIFEST`, WAL `FLAG_AUTH_MANIFEST`):
//! those flags sit in headers the GCM tags are bound to, so they cannot be
//! stripped, only deleted along with the data.
//!
//! `wal_seq` is the flush point: every batch whose COMMIT lies in a segment
//! `<= wal_seq` is already in `chunks`. `replay_seq` is where replay starts:
//! it trails `wal_seq + 1` while a batch that logged rows before the flush
//! is still open, because its rows precede the flush point but its COMMIT
//! will follow it. `tseq` plays the same role for deletes: a `DELETE` entry of
//! the WAL with `seq <= tseq` is already reflected in the tombstones listed
//! here (or was applied physically), so replay only re-applies it to the rows
//! it brings back into the MemTable.

use std::fs;
use std::path::Path;

use crate::bytes::{put_u16, put_u32, put_u64, ByteReader};
use crate::crypto::{CipherParams, IV_LEN, PARAMS_LEN, TAG_LEN};
use crate::error::{corrupt, Error, Result};
use crate::format::{MANIFEST_AEAD as VERSION_AEAD, MANIFEST_PLAIN as VERSION_PLAIN, MANIFEST_TOMB as VERSION, MANIFEST_V1};
use crate::fsutil::write_atomic;
use crate::tomb::{decode_range, decode_rows, encode_range, encode_rows, RangeTomb, RowsTomb};

pub(crate) const MANIFEST_FILE: &str = "MANIFEST";
const MAGIC: &[u8; 4] = b"TFLM";
/// AAD unit tag of the sealed MANIFEST (blocks use 0, the chunk index 1).
const UNIT_MANIFEST: u8 = 2;
/// Magic, version and reserved: the part of a v4 header before `CipherParams`.
const SEALED_PREFIX: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Manifest {
    /// Every batch committed in a WAL segment with `seq <= wal_seq` is fully
    /// reflected in `chunks` (the flush point).
    pub wal_seq: u64,
    /// First WAL segment recovery must read; always `<= wal_seq + 1`.
    pub replay_seq: u64,
    pub next_chunk_id: u64,
    /// Live chunk ids, ascending.
    pub chunks: Vec<u64>,
    /// `born` of each chunk of `chunks`, same order (see `tomb`).
    pub born: Vec<u64>,
    /// Highest tombstone sequence number ever assigned (0 = never any).
    pub tseq: u64,
    pub ranges: Vec<RangeTomb>,
    pub rows: Vec<RowsTomb>,
    /// Key id the next `store` seals with (v4); `None` writes the plain formats.
    /// After `load` it is `Some` only for a MANIFEST that was authenticated.
    pub key: Option<u32>,
    /// File IV of each chunk of `chunks`, same order; only kept for sealed
    /// MANIFESTs (empty otherwise).
    pub ivs: Vec<[u8; IV_LEN]>,
}

impl Manifest {
    pub(crate) fn empty() -> Manifest {
        Manifest {
            wal_seq: 0,
            replay_seq: 1,
            next_chunk_id: 1,
            chunks: Vec::new(),
            born: Vec::new(),
            tseq: 0,
            ranges: Vec::new(),
            rows: Vec::new(),
            key: None,
            ivs: Vec::new(),
        }
    }

    /// `born` of chunk `id` (0 for a chunk the MANIFEST predates tombstones for).
    #[cfg(test)]
    pub(crate) fn born_of(&self, id: u64) -> u64 {
        self.chunks.iter().position(|&c| c == id).and_then(|i| self.born.get(i)).copied().unwrap_or(0)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let Some(key) = self.key else { return Ok(self.encode_plain()) };
        if self.ivs.len() != self.chunks.len() || self.born.len() != self.chunks.len() {
            return Err(Error::Crypto("internal error: MANIFEST chunk identities are missing".into()));
        }
        // table.rs stands in an all-zero IV for a chunk without a cipher: never a real one.
        if self.ivs.contains(&[0u8; IV_LEN]) {
            return Err(Error::Crypto("internal error: a chunk of an encrypted table has no IV".into()));
        }
        let mut body = Vec::with_capacity(48 + self.chunks.len() * (16 + IV_LEN));
        put_u64(&mut body, self.wal_seq);
        put_u64(&mut body, self.next_chunk_id);
        put_u64(&mut body, self.replay_seq);
        put_u64(&mut body, self.tseq);
        put_u32(&mut body, self.chunks.len() as u32);
        for (i, id) in self.chunks.iter().enumerate() {
            put_u64(&mut body, *id);
            put_u64(&mut body, self.born[i]);
            body.extend_from_slice(&self.ivs[i]);
        }
        put_u32(&mut body, self.ranges.len() as u32);
        for t in &self.ranges {
            encode_range(&mut body, t);
        }
        put_u32(&mut body, self.rows.len() as u32);
        for t in &self.rows {
            encode_rows(&mut body, t);
        }
        seal_body(key, body)
    }

    fn encode_plain(&self) -> Vec<u8> {
        let plain = self.tseq == 0 && self.ranges.is_empty() && self.rows.is_empty();
        let mut buf = Vec::with_capacity(48 + self.chunks.len() * 16);
        buf.extend_from_slice(MAGIC);
        put_u16(&mut buf, if plain { VERSION_PLAIN } else { VERSION });
        put_u16(&mut buf, 0);
        put_u64(&mut buf, self.wal_seq);
        put_u64(&mut buf, self.next_chunk_id);
        put_u64(&mut buf, self.replay_seq);
        if plain {
            put_u32(&mut buf, self.chunks.len() as u32);
            for id in &self.chunks {
                put_u64(&mut buf, *id);
            }
        } else {
            put_u64(&mut buf, self.tseq);
            put_u32(&mut buf, self.chunks.len() as u32);
            for (i, id) in self.chunks.iter().enumerate() {
                put_u64(&mut buf, *id);
                put_u64(&mut buf, self.born.get(i).copied().unwrap_or(0));
            }
            put_u32(&mut buf, self.ranges.len() as u32);
            for t in &self.ranges {
                encode_range(&mut buf, t);
            }
            put_u32(&mut buf, self.rows.len() as u32);
            for t in &self.rows {
                encode_rows(&mut buf, t);
            }
        }
        let crc = crc32fast::hash(&buf);
        put_u32(&mut buf, crc);
        buf
    }

    fn decode(data: &[u8]) -> Result<Manifest> {
        if data.len() < 4 + 4 {
            return Err(corrupt("MANIFEST too short"));
        }
        if &data[..4] == MAGIC && u16::from_le_bytes([data[4], data[5]]) == VERSION_AEAD {
            return Manifest::decode_sealed(data);
        }
        let (body, crc) = data.split_at(data.len() - 4);
        if crc32fast::hash(body).to_le_bytes() != crc {
            return Err(corrupt("MANIFEST checksum mismatch"));
        }
        let mut r = ByteReader::new(body);
        if r.take(4)? != MAGIC {
            return Err(corrupt("MANIFEST bad magic"));
        }
        let version = r.u16()?;
        if version != VERSION && version != VERSION_PLAIN && version != MANIFEST_V1 {
            return Err(corrupt(format!("MANIFEST version {version} not supported")));
        }
        r.u16()?;
        Manifest::parse_body(&mut r, version)
    }

    /// The fields from `wal_seq` on, common to every version.
    fn parse_body(r: &mut ByteReader, version: u16) -> Result<Manifest> {
        let sealed = version == VERSION_AEAD;
        let wal_seq = r.u64()?;
        let next_chunk_id = r.u64()?;
        let after_flush = wal_seq.saturating_add(1);
        let replay_seq = if version >= 2 { r.u64()?.min(after_flush) } else { after_flush };
        if version < VERSION {
            let n = r.u32()? as usize;
            if n.checked_mul(8) != Some(r.remaining()) {
                return Err(corrupt("MANIFEST chunk list length mismatch"));
            }
            let chunks = (0..n).map(|_| r.u64()).collect::<Result<Vec<_>>>()?;
            let born = vec![0; chunks.len()];
            return Ok(Manifest { wal_seq, replay_seq, next_chunk_id, chunks, born, ..Manifest::empty() });
        }
        let tseq = r.u64()?;
        let n = r.u32()? as usize;
        let entry = if sealed { 16 + IV_LEN } else { 16 };
        if n.checked_mul(entry).map_or(true, |b| b > r.remaining()) {
            return Err(corrupt("MANIFEST chunk list length mismatch"));
        }
        let mut chunks = Vec::with_capacity(n);
        let mut born = Vec::with_capacity(n);
        let mut ivs = Vec::with_capacity(if sealed { n } else { 0 });
        for _ in 0..n {
            chunks.push(r.u64()?);
            born.push(r.u64()?);
            if sealed {
                let mut iv = [0u8; IV_LEN];
                iv.copy_from_slice(r.take(IV_LEN)?);
                ivs.push(iv);
            }
        }
        let nr = r.u32()? as usize;
        // Each tombstone takes far more than a byte: the count cannot exceed what is left.
        if nr > r.remaining() {
            return Err(corrupt("MANIFEST tombstone count longer than its data"));
        }
        let mut ranges = Vec::with_capacity(nr.min(1 << 16));
        for _ in 0..nr {
            ranges.push(decode_range(r)?);
        }
        let nw = r.u32()? as usize;
        if nw > r.remaining() {
            return Err(corrupt("MANIFEST row tombstone count longer than its data"));
        }
        let mut rows = Vec::with_capacity(nw.min(1 << 16));
        for _ in 0..nw {
            rows.push(decode_rows(r)?);
        }
        if r.remaining() != 0 {
            return Err(corrupt("MANIFEST has trailing bytes"));
        }
        if ranges.iter().any(|t| t.seq > tseq) || rows.iter().any(|t| t.seq > tseq) {
            return Err(corrupt("MANIFEST tombstone newer than its highest sequence"));
        }
        Ok(Manifest { wal_seq, replay_seq, next_chunk_id, chunks, born, tseq, ranges, rows, key: None, ivs })
    }

    /// Verifies and decrypts a v4 file: the key id it was sealed with and the body.
    pub(crate) fn open_body(data: &[u8]) -> Result<(u32, Vec<u8>)> {
        const HEAD: usize = SEALED_PREFIX + PARAMS_LEN;
        if data.len() < HEAD + TAG_LEN {
            return Err(corrupt("MANIFEST too short"));
        }
        crate::crypto::ensure_available()?;
        let c = CipherParams::decode(&data[SEALED_PREFIX..HEAD])?;
        let key_id = c.key_id;
        let c = c.authenticated(data[..HEAD].to_vec());
        let (body, tag) = data[HEAD..].split_at(data.len() - HEAD - TAG_LEN);
        let mut body = body.to_vec();
        if !c.open(&mut body, tag, 0, &[UNIT_MANIFEST]) {
            return Err(Error::Crypto(
                "MANIFEST failed authentication (altered, taken from another table, or wrong key)".into(),
            ));
        }
        Ok((key_id, body))
    }

    fn decode_sealed(data: &[u8]) -> Result<Manifest> {
        let (key_id, body) = Manifest::open_body(data)?;
        let mut r = ByteReader::new(&body);
        Ok(Manifest { key: Some(key_id), ..Manifest::parse_body(&mut r, VERSION_AEAD)? })
    }

    pub(crate) fn load(dir: &Path) -> Result<Manifest> {
        match fs::read(dir.join(MANIFEST_FILE)) {
            Ok(data) => Manifest::decode(&data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(format!("{} has no MANIFEST", dir.display())))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn store(&self, dir: &Path) -> Result<()> {
        write_atomic(&dir.join(MANIFEST_FILE), &self.encode()?)
    }
}

/// Seals `body` under the latest version of `key` with a fresh random IV:
/// header (prefix + `CipherParams`) | ciphertext | tag.
pub(crate) fn seal_body(key: u32, mut body: Vec<u8>) -> Result<Vec<u8>> {
    crate::crypto::ensure_available()?;
    let c = CipherParams::for_new_file(key)?;
    let mut out = Vec::with_capacity(SEALED_PREFIX + PARAMS_LEN + body.len() + TAG_LEN);
    out.extend_from_slice(MAGIC);
    put_u16(&mut out, VERSION_AEAD);
    put_u16(&mut out, 0);
    out.extend_from_slice(&c.encode());
    let c = c.authenticated(out.clone());
    let tag = c.seal(&mut body, 0, &[UNIT_MANIFEST])?;
    out.extend_from_slice(&body);
    out.extend_from_slice(&tag);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tomb::Sel;

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let m = Manifest {
            wal_seq: 9,
            replay_seq: 7,
            next_chunk_id: 12,
            chunks: vec![3, 7, 11],
            born: vec![0, 0, 0],
            ..Manifest::empty()
        };
        m.store(dir.path()).unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), m);
    }

    #[test]
    fn a_table_without_deletes_still_writes_the_v2_layout() {
        let m = Manifest { chunks: vec![4], born: vec![0], ..Manifest::empty() };
        let bytes = m.encode().unwrap();
        assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), 2);
        // 4 magic + 4 flags + 3*8 header + 4 count + 8 id + 4 crc
        assert_eq!(bytes.len(), 4 + 4 + 24 + 4 + 8 + 4);
    }

    #[test]
    fn v3_roundtrips_tombstones_and_borns() {
        let dir = tempfile::tempdir().unwrap();
        let m = Manifest {
            wal_seq: 9,
            replay_seq: 9,
            next_chunk_id: 20,
            chunks: vec![3, 7],
            born: vec![2, 5],
            tseq: 6,
            ranges: vec![RangeTomb { seq: 6, lo: 1, hi: 99, sel: Sel::from_ids(vec![4, 2]), hits: vec![(3, 10)] }],
            rows: vec![RowsTomb { seq: 4, chunk: 7, entry: 3, runs: vec![(0, 2), (8, 1)] }],
            ..Manifest::empty()
        };
        m.store(dir.path()).unwrap();
        let back = Manifest::load(dir.path()).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.born_of(7), 5);
        assert_eq!(back.born_of(99), 0);
        let bytes = m.encode().unwrap();
        assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), 3);
        // a tombstone newer than tseq, or a forged count, is corruption
        let bad = Manifest { tseq: 3, ..m.clone() };
        assert!(matches!(Manifest::decode(&bad.encode().unwrap()), Err(Error::Corrupt(_))));
        let mut forged = bytes.clone();
        let at = 4 + 4 + 24 + 8; // chunk_count
        forged[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let crc = crc32fast::hash(&forged[..forged.len() - 4]);
        let n = forged.len();
        forged[n - 4..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(Manifest::decode(&forged), Err(Error::Corrupt(_))));
    }

    #[test]
    fn reads_v1_manifests() {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        put_u16(&mut buf, 1);
        put_u16(&mut buf, 0);
        put_u64(&mut buf, 5); // wal_seq
        put_u64(&mut buf, 9); // next_chunk_id
        put_u32(&mut buf, 1);
        put_u64(&mut buf, 4);
        let crc = crc32fast::hash(&buf);
        put_u32(&mut buf, crc);
        let m = Manifest::decode(&buf).unwrap();
        assert_eq!(
            m,
            Manifest { wal_seq: 5, replay_seq: 6, next_chunk_id: 9, chunks: vec![4], born: vec![0], ..Manifest::empty() }
        );
    }

    #[test]
    fn detects_corruption() {
        let mut data = Manifest::empty().encode().unwrap();
        data[10] ^= 0xff;
        assert!(matches!(Manifest::decode(&data), Err(Error::Corrupt(_))));
        assert!(matches!(Manifest::decode(&[1, 2]), Err(Error::Corrupt(_))));
    }

    #[test]
    fn missing_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(Manifest::load(dir.path()), Err(Error::NotFound(_))));
    }
}

#[cfg(all(test, feature = "encryption"))]
mod sealed_tests {
    use super::*;
    use crate::crypto::test_keys;

    fn sample() -> Manifest {
        Manifest {
            wal_seq: 9,
            replay_seq: 8,
            next_chunk_id: 12,
            chunks: vec![3, 7],
            born: vec![0, 2],
            tseq: 2,
            key: Some(1),
            ivs: vec![[3; IV_LEN], [7; IV_LEN]],
            ..Manifest::empty()
        }
    }

    #[test]
    fn roundtrips_and_never_reuses_a_nonce() {
        test_keys::install();
        let dir = tempfile::tempdir().unwrap();
        let m = sample();
        m.store(dir.path()).unwrap();
        let first = fs::read(dir.path().join(MANIFEST_FILE)).unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), m);
        m.store(dir.path()).unwrap();
        let second = fs::read(dir.path().join(MANIFEST_FILE)).unwrap();
        // same content, different IV (bytes 16..32 of the header) and so different everything
        assert_ne!(first[16..32], second[16..32]);
        assert_eq!(first.len(), 4 + 4 + PARAMS_LEN + 4 * 8 + 4 + 2 * (16 + IV_LEN) + 4 + 4 + TAG_LEN);
    }

    #[test]
    fn any_bit_flip_is_refused_and_a_truncation_too() {
        test_keys::install();
        let bytes = sample().encode().unwrap();
        for pos in 0..bytes.len() {
            let mut b = bytes.clone();
            b[pos] ^= 1 << (pos % 8);
            assert!(Manifest::decode(&b).is_err(), "byte {pos} accepted");
        }
        for n in 0..bytes.len() {
            assert!(Manifest::decode(&bytes[..n]).is_err(), "truncated to {n} accepted");
        }
    }

    #[test]
    fn missing_chunk_identities_are_an_error_not_a_weaker_file() {
        test_keys::install();
        let m = Manifest { ivs: vec![], ..sample() };
        assert!(m.encode().is_err());
    }
}

#[cfg(all(test, feature = "encryption"))]
mod cost {
    use super::*;

    /// Run with `cargo test --release -p moosedb-core manifest::cost -- --ignored --nocapture`.
    #[test]
    #[ignore = "measures"]
    fn sealed_vs_plain_store() {
        crate::crypto::test_keys::install();
        let dir = tempfile::tempdir().unwrap();
        for n in [1u64, 100, 2000] {
            for key in [None, Some(1)] {
                let m = Manifest {
                    chunks: (1..=n).collect(),
                    born: vec![0; n as usize],
                    ivs: if key.is_some() { vec![[7; IV_LEN]; n as usize] } else { vec![] },
                    key,
                    ..Manifest::empty()
                };
                let (rounds, mut enc_t, mut full_t) = (300, std::time::Duration::ZERO, std::time::Duration::ZERO);
                let mut len = 0;
                for _ in 0..rounds {
                    let t = std::time::Instant::now();
                    let b = m.encode().unwrap();
                    enc_t += t.elapsed();
                    len = b.len();
                    let t = std::time::Instant::now();
                    m.store(dir.path()).unwrap();
                    full_t += t.elapsed();
                }
                println!(
                    "{n:5} chunks {:6}: {len:6} bytes, encode {:7.1} us, store (encode + tmp + fsync + rename) {:7.1} us",
                    if key.is_some() { "sealed" } else { "plain" },
                    enc_t.as_secs_f64() * 1e6 / rounds as f64,
                    full_t.as_secs_f64() * 1e6 / rounds as f64
                );
            }
        }
    }
}
