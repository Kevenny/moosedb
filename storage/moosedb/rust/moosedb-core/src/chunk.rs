//! On-disk chunk format (`.tfl`), versions 2 and 3.
//!
//! ```text
//! ┌─────────────────────────── Header (64 bytes, plaintext) ─────────────────┐
//! │ 0  magic "TFLW"        4  version:u16      6  flags:u16                  │
//! │ 8  ts_min:i64          16 ts_max:i64       24 row_count:u64              │
//! │ 32 series_count:u32    36 column_count:u32 40 chunk_id:u64               │
//! │ 48 wal_seq:u64         56 schema_fp:u32    60 header_crc:u32 (of 0..60)  │
//! ├────────────── Encryption header (32 bytes, only if ENCRYPTED) ───────────┤
//! │ key_id:u32 key_version:u32 iv[16] reserved:u32 crc:u32                   │
//! ├──────────────────── Data blocks  ─┐ v3 (ENCRYPTED): each block is       │
//! │ column blocks (see `block`)       │ AES-256-GCM sealed and followed by   │
//! ├──────────────────── Series index ─┤ its 16-byte tag (BlockRef.len does   │
//! │ raw_bytes:u64 bucket:i64×2 n:u32  │ not count the tag, offsets do);      │
//! │ entries (see below)               │ index + Bloom filter are one sealed  │
//! ├──────────────────── Bloom filter ─┘ unit plus tag. v2 ENCRYPTED (legacy,│
//! │ read-only): AES-256-CTR, keystream offset 0 = first byte after headers,  │
//! │ no tags.                                                                 │
//! ├──────────────────────────── Footer (24 bytes, plaintext) ────────────────┤
//! │ index_offset:u64 bloom_offset:u64 file_crc:u32 "TFLE"                    │
//! └──────────────────────────────────────────────────────────────────────────┘
//! series entry := id:u64 rows:u32 ts_min:i64 ts_max:i64
//!                 tag_count:u16 tag values (codec)
//!                 block_count:u16 (offset:u64 len:u32)*
//! ```
//!
//! With the `ZONES` flag the index+Bloom unit ends with a zone-map section
//! after the Bloom filter (`zone`): per series entry and data column a
//! min/max/NULL count (numbers) or a value Bloom (strings). It sits inside the
//! unit, so a v3 chunk seals and authenticates it with the index (header flag
//! in the AAD: it cannot be added, removed or edited unnoticed).
//!
//! `file_crc` covers every byte before it (as stored, i.e. ciphertext), so
//! CHECK TABLE works without the key. Each block also has its own CRC over
//! the plaintext. In v3 the GCM tags (nonce and AAD: see `crypto`) are what
//! authenticate the file: the header bytes are part of every unit's AAD and
//! the unit position part of its nonce, so blocks cannot be swapped, moved or
//! edited, and the index (which names the blocks) cannot be forged either.
//! Plaintext chunks stay v2 (older binaries read them); v3 is refused by
//! binaries that predate it.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use crate::bytes::{put_i64, put_u32, put_u64, ByteReader};
use crate::compression::Codec;
use crate::crypto::{CipherParams, PARAMS_LEN, TAG_LEN};
use crate::error::{corrupt, Result};
use crate::format::{CHUNK_AEAD, CHUNK_PLAIN};
use crate::fsutil::remove_if_exists;
use crate::index::bloom::Bloom;
use crate::schema::Value;

pub(crate) const MAGIC: &[u8; 4] = b"TFLW";
pub(crate) const FOOTER_MAGIC: &[u8; 4] = b"TFLE";
pub(crate) const HEADER_LEN: usize = 64;
pub(crate) const ENC_HEADER_LEN: usize = PARAMS_LEN;
pub(crate) const FOOTER_LEN: usize = 24;
/// AAD unit tag of a column block (v3).
pub(crate) const UNIT_BLOCK: u8 = 0;

/// AAD of the sealed index+bloom unit (v3): unit tag and where the Bloom
/// filter starts, so the split between the two cannot be moved.
pub(crate) fn index_ctx(bloom_offset: u64) -> [u8; 9] {
    let mut c = [1u8; 9];
    c[1..].copy_from_slice(&bloom_offset.to_le_bytes());
    c
}

pub mod flags {
    pub const COMPRESSED: u16 = 1 << 0;
    pub const ENCRYPTED: u16 = 1 << 1;
    pub const SEALED: u16 = 1 << 2;
    /// A zone-map section follows the series Bloom, inside the same
    /// index+Bloom unit (see `zone`). Readers that predate it ignore the
    /// flag and the trailing bytes, so no version bump: a chunk without
    /// useful summaries does not set it.
    pub const ZONES: u16 = 1 << 3;
    /// Bits 8-9: codec the chunk was written with (`compression::CODEC_*`).
    pub const CODEC_SHIFT: u16 = 8;
    pub const CODEC_MASK: u16 = 0b11 << CODEC_SHIFT;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    pub version: u16,
    pub flags: u16,
    pub ts_min: i64,
    pub ts_max: i64,
    pub row_count: u64,
    pub series_count: u32,
    pub column_count: u32,
    pub chunk_id: u64,
    pub wal_seq: u64,
    pub schema_fingerprint: u32,
}

impl ChunkHeader {
    pub(crate) fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = Vec::with_capacity(HEADER_LEN);
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(&self.version.to_le_bytes());
        b.extend_from_slice(&self.flags.to_le_bytes());
        put_i64(&mut b, self.ts_min);
        put_i64(&mut b, self.ts_max);
        put_u64(&mut b, self.row_count);
        put_u32(&mut b, self.series_count);
        put_u32(&mut b, self.column_count);
        put_u64(&mut b, self.chunk_id);
        put_u64(&mut b, self.wal_seq);
        put_u32(&mut b, self.schema_fingerprint);
        let crc = crc32fast::hash(&b);
        put_u32(&mut b, crc);
        let mut out = [0u8; HEADER_LEN];
        out.copy_from_slice(&b);
        out
    }

    pub(crate) fn decode(b: &[u8]) -> Result<ChunkHeader> {
        if b.len() < HEADER_LEN {
            return Err(corrupt("chunk header truncated"));
        }
        let b = &b[..HEADER_LEN];
        if crc32fast::hash(&b[..60]).to_le_bytes() != b[60..64] {
            return Err(corrupt("chunk header checksum mismatch"));
        }
        let mut r = ByteReader::new(b);
        if r.take(4)? != MAGIC {
            return Err(corrupt("chunk bad magic"));
        }
        let h = ChunkHeader {
            version: r.u16()?,
            flags: r.u16()?,
            ts_min: r.i64()?,
            ts_max: r.i64()?,
            row_count: r.u64()?,
            series_count: r.u32()?,
            column_count: r.u32()?,
            chunk_id: r.u64()?,
            wal_seq: r.u64()?,
            schema_fingerprint: r.u32()?,
        };
        if h.version != CHUNK_PLAIN && h.version != CHUNK_AEAD {
            return Err(corrupt(format!(
                "chunk format version {} not supported (this binary reads {CHUNK_PLAIN} and {CHUNK_AEAD})",
                h.version
            )));
        }
        if h.version == CHUNK_AEAD && !h.is_encrypted() {
            return Err(corrupt("chunk version 3 without the ENCRYPTED flag"));
        }
        Ok(h)
    }

    pub fn codec_id(&self) -> u8 {
        ((self.flags & flags::CODEC_MASK) >> flags::CODEC_SHIFT) as u8
    }

    pub fn is_encrypted(&self) -> bool {
        self.flags & flags::ENCRYPTED != 0
    }

    /// Whether blocks and index carry GCM tags (format v3).
    pub(crate) fn is_authenticated(&self) -> bool {
        self.version == CHUNK_AEAD
    }

    /// Bytes a stored block takes beyond its `BlockRef.len`.
    pub(crate) fn block_overhead(&self) -> usize {
        if self.is_authenticated() {
            TAG_LEN
        } else {
            0
        }
    }

    /// Offset of the first data block.
    pub(crate) fn data_start(&self) -> u64 {
        (HEADER_LEN + if self.is_encrypted() { ENC_HEADER_LEN } else { 0 }) as u64
    }
}

pub(crate) fn chunk_flags(codec: Codec, encrypted: bool) -> u16 {
    let mut f = flags::SEALED | (u16::from(codec.id()) << flags::CODEC_SHIFT);
    if codec != Codec::None {
        f |= flags::COMPRESSED;
    }
    if encrypted {
        f |= flags::ENCRYPTED;
    }
    f
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRef {
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SeriesEntry {
    pub series_id: u64,
    pub row_count: u32,
    pub ts_min: i64,
    pub ts_max: i64,
    /// Tag values in schema tag order.
    pub tags: Vec<Value>,
    /// One block per data column, in `Schema::data_indices` order.
    pub blocks: Vec<BlockRef>,
}

/// Entry indexes sorted by `(series_id, index)`: valid whatever order the
/// entries are stored in (older or compacted chunks may hold a series in
/// several entries, not in id order). `None` past `u32` entries.
pub(crate) fn build_by_id(series: &[SeriesEntry]) -> Option<Vec<u32>> {
    let n = u32::try_from(series.len()).ok()?;
    let mut v: Vec<u32> = (0..n).collect();
    v.sort_unstable_by_key(|&i| (series.get(i as usize).map_or(0, |e| e.series_id), i));
    Some(v)
}

/// Ascending indexes of the entries whose series is in `ids`, through the
/// `build_by_id` order.
pub(crate) fn entries_in(series: &[SeriesEntry], by_id: &[u32], ids: &HashSet<u64>) -> Vec<usize> {
    let id_of = |i: u32| series.get(i as usize).map(|e| e.series_id);
    let mut out = Vec::new();
    for &id in ids {
        let start = by_id.partition_point(|&i| id_of(i).is_some_and(|x| x < id));
        for &i in by_id.get(start..).unwrap_or(&[]) {
            if id_of(i) != Some(id) {
                break;
            }
            out.push(i as usize);
        }
    }
    out.sort_unstable();
    out
}

/// Owns a chunk file's lifetime. When a chunk leaves the live set (flush
/// replaced it, retention expired it, compaction merged it, TRUNCATE) it is
/// only *marked* obsolete; the file is deleted when the last snapshot that
/// can still read it lets go. Scans and row positions therefore never see a
/// file vanish underneath them.
#[derive(Debug)]
pub struct ChunkFile {
    pub(crate) uid: u64,
    pub path: PathBuf,
    obsolete: AtomicBool,
}

impl ChunkFile {
    pub(crate) fn new(path: PathBuf) -> Arc<ChunkFile> {
        static NEXT_UID: AtomicU64 = AtomicU64::new(1);
        Arc::new(ChunkFile { uid: NEXT_UID.fetch_add(1, Ordering::Relaxed), path, obsolete: AtomicBool::new(false) })
    }

    pub(crate) fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }
}

impl Drop for ChunkFile {
    fn drop(&mut self) {
        crate::cache::forget_file(self.uid);
        if self.obsolete.load(Ordering::Acquire) {
            if let Err(e) = remove_if_exists(&self.path) {
                crate::log::warn(&format!("cannot delete {}: {e}", self.path.display()));
            }
        }
    }
}

/// Everything about a chunk except its data blocks. Kept in memory for every
/// live chunk.
#[derive(Clone, Debug)]
pub struct ChunkMeta {
    pub id: u64,
    pub file: Arc<ChunkFile>,
    pub header: ChunkHeader,
    /// Half-open time bucket `[start, end)` this chunk belongs to.
    pub bucket: (i64, i64),
    /// Size of the column data as plain arrays (before encoding/compression).
    pub raw_bytes: u64,
    pub file_size: u64,
    pub series: Vec<SeriesEntry>,
    /// Ordinal of the first row of each series entry.
    pub series_offsets: Vec<u64>,
    pub bloom: Bloom,
    /// Value summaries per (entry, data column); `None` for chunks written
    /// before they existed (they are simply never pruned by value).
    pub zones: Option<crate::zone::ZoneMaps>,
    pub(crate) cipher: Option<CipherParams>,
    /// Indexes of `series`, sorted by `(series_id, index)`; built on first use
    /// (see `entries_of`). `None` when the chunk has more entries than `u32`.
    pub(crate) by_id: OnceLock<Option<Vec<u32>>>,
    /// Highest tombstone sequence the rows of this chunk already reflect (see
    /// `tomb`): a range tombstone newer than this hides rows of the chunk.
    /// Kept in the MANIFEST, not in the chunk file. `u64::MAX` = no tombstone
    /// applies (a chunk staged by a statement that has not committed yet).
    pub(crate) born: u64,
}

impl ChunkMeta {
    /// Indexes into `series` (ascending) of the entries of the given series:
    /// O(wanted x log entries) instead of a pass over every entry, so
    /// opening a scan for one series of a chunk with thousands of series is
    /// cheap. `None` when the chunk cannot be indexed (the caller walks it).
    pub(crate) fn entries_of(&self, ids: &HashSet<u64>) -> Option<Vec<usize>> {
        let by_id = self.by_id.get_or_init(|| build_by_id(&self.series)).as_ref()?;
        Some(entries_in(&self.series, by_id, ids))
    }

    pub fn path(&self) -> &Path {
        &self.file.path
    }

    pub fn file_name(&self) -> String {
        self.file.path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()
    }

    pub fn overlaps(&self, lo: i64, hi: i64) -> bool {
        self.header.ts_min <= hi && self.header.ts_max >= lo
    }

    /// Index of the series entry holding row `ordinal`.
    pub(crate) fn series_for_ordinal(&self, ordinal: u64) -> Option<usize> {
        let idx = self.series_offsets.partition_point(|&o| o <= ordinal).checked_sub(1)?;
        let e = self.series.get(idx)?;
        (ordinal < self.series_offsets[idx] + u64::from(e.row_count)).then_some(idx)
    }
}

pub(crate) fn series_offsets(series: &[SeriesEntry]) -> Vec<u64> {
    let mut acc = 0u64;
    series
        .iter()
        .map(|e| {
            let o = acc;
            acc += u64::from(e.row_count);
            o
        })
        .collect()
}

pub(crate) fn chunk_file_name(bucket: (i64, i64), id: u64) -> String {
    format!("chunk_{}_{}_{id:06}.tfl", crate::time::format_compact(bucket.0), crate::time::format_compact(bucket.1))
}

/// Extracts the chunk id from a file name produced by `chunk_file_name`.
pub(crate) fn parse_chunk_id(name: &str) -> Option<u64> {
    let stem = name.strip_prefix("chunk_")?.strip_suffix(".tfl")?;
    stem.rsplit('_').next()?.parse().ok()
}

pub(crate) fn encode_series_entry(buf: &mut Vec<u8>, e: &SeriesEntry) {
    put_u64(buf, e.series_id);
    put_u32(buf, e.row_count);
    put_i64(buf, e.ts_min);
    put_i64(buf, e.ts_max);
    buf.extend_from_slice(&(e.tags.len() as u16).to_le_bytes());
    for t in &e.tags {
        crate::codec::encode_value(buf, t);
    }
    buf.extend_from_slice(&(e.blocks.len() as u16).to_le_bytes());
    for b in &e.blocks {
        put_u64(buf, b.offset);
        put_u32(buf, b.len);
    }
}

pub(crate) fn decode_series_entry(r: &mut ByteReader<'_>) -> Result<SeriesEntry> {
    let series_id = r.u64()?;
    let row_count = r.u32()?;
    let ts_min = r.i64()?;
    let ts_max = r.i64()?;
    let ntags = r.u16()? as usize;
    let tags = (0..ntags).map(|_| crate::codec::decode_value(r)).collect::<Result<Vec<_>>>()?;
    let nblocks = r.u16()? as usize;
    let blocks = (0..nblocks).map(|_| Ok(BlockRef { offset: r.u64()?, len: r.u32()? })).collect::<Result<Vec<_>>>()?;
    Ok(SeriesEntry { series_id, row_count, ts_min, ts_max, tags, blocks })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: u64) -> SeriesEntry {
        SeriesEntry { series_id: id, row_count: 1, ts_min: 0, ts_max: 0, tags: Vec::new(), blocks: Vec::new() }
    }

    /// Entries out of id order and one series split over several entries (old
    /// or compacted chunks): the lookup finds exactly the matching entries.
    #[test]
    fn entries_of_handles_unsorted_and_repeated_series() {
        let ids_in_file = [9u64, 3, 3, 7, 9, 1, 3, 9, 5];
        let series: Vec<SeriesEntry> = ids_in_file.iter().map(|&i| entry(i)).collect();
        let by_id = build_by_id(&series).unwrap();
        for wanted in [vec![3u64], vec![9, 1], vec![4], vec![5, 7, 3], vec![], vec![u64::MAX, 0]] {
            let set: HashSet<u64> = wanted.iter().copied().collect();
            let want: Vec<usize> = (0..series.len()).filter(|&i| set.contains(&ids_in_file[i])).collect();
            assert_eq!(entries_in(&series, &by_id, &set), want, "{wanted:?}");
        }
    }

    #[test]
    fn header_roundtrip_and_crc() {
        let h = ChunkHeader {
            version: CHUNK_AEAD,
            flags: chunk_flags(Codec::Zstd(3), true),
            ts_min: -5,
            ts_max: 99,
            row_count: 1234,
            series_count: 3,
            column_count: 4,
            chunk_id: 77,
            wal_seq: 12,
            schema_fingerprint: 0xdead_beef,
        };
        let mut b = h.encode();
        let d = ChunkHeader::decode(&b).unwrap();
        assert_eq!(d, h);
        assert_eq!(d.codec_id(), crate::compression::CODEC_ZSTD);
        assert!(d.is_encrypted());
        assert_eq!(d.data_start(), 96);
        b[9] ^= 1;
        assert!(ChunkHeader::decode(&b).is_err());
    }

    #[test]
    fn file_names() {
        let name = chunk_file_name((0, crate::time::MICROS_PER_DAY), 42);
        assert_eq!(name, "chunk_19700101T000000_19700102T000000_000042.tfl");
        assert_eq!(parse_chunk_id(&name), Some(42));
        assert_eq!(parse_chunk_id("chunk_x.tfl.corrupt"), None);
        assert_eq!(parse_chunk_id("MANIFEST"), None);
    }

    #[test]
    fn obsolete_files_are_deleted_on_last_drop() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.tfl");
        std::fs::write(&p, b"x").unwrap();
        let f = ChunkFile::new(p.clone());
        let reader = f.clone();
        f.mark_obsolete();
        drop(f);
        assert!(p.exists(), "a reader still holds the file");
        drop(reader);
        assert!(!p.exists());

        let q = dir.path().join("y.tfl");
        std::fs::write(&q, b"y").unwrap();
        drop(ChunkFile::new(q.clone()));
        assert!(q.exists(), "live files are never deleted");
    }
}
