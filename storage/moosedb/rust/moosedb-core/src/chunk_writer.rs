//! Incremental construction of a sealed chunk file.
//!
//! Series are added one at a time (already sorted by timestamp); their column
//! blocks are encoded and compressed immediately, so building a chunk only
//! holds the *compressed* output plus one series in memory. This is what lets
//! compaction merge large time buckets.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::block;
use crate::bytes::{put_i64, put_u32, put_u64};
use crate::chunk::{
    chunk_file_name, chunk_flags, encode_series_entry, series_offsets, BlockRef, ChunkFile, ChunkHeader, ChunkMeta,
    SeriesEntry, FOOTER_MAGIC, HEADER_LEN,
};
use crate::compression::Codec;
use crate::crypto::{CipherParams, TAG_LEN};
use crate::format::{CHUNK_AEAD, CHUNK_PLAIN};
use crate::error::{invalid, Result};
use crate::index::bloom::Bloom;
use crate::schema::{Row, Schema, Value};

/// The rows of one series within one time bucket, cut into parts that each
/// fit one column block (see `compaction::split_series`).
pub(crate) struct SeriesParts<'a> {
    pub series_id: u64,
    pub tags: &'a [Value],
    pub parts: Vec<&'a [&'a Row]>,
}

/// Number of chunk files a bucket needs: part `k` of every series goes to
/// chunk `k`, so a series never appears twice in one chunk.
pub(crate) fn chunks_needed(series: &[SeriesParts<'_>]) -> usize {
    series.iter().map(|s| s.parts.len()).max().unwrap_or(0)
}

/// Writes the chunk files of one bucket with ids `first_id..`, appending each
/// finished chunk to `written` (so the caller can discard them if a later
/// step fails). Files are fsynced and renamed into place, not yet listed in
/// the MANIFEST.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_bucket(
    schema: &Schema,
    codec: Codec,
    key_id: Option<u32>,
    dir: &Path,
    bucket: (i64, i64),
    series: &[SeriesParts<'_>],
    first_id: u64,
    wal_seq: u64,
    written: &mut Vec<ChunkMeta>,
) -> Result<()> {
    let mut builders = Vec::new();
    for _ in 0..chunks_needed(series) {
        let cipher = key_id.map(CipherParams::for_new_file).transpose()?;
        builders.push(ChunkBuilder::new(schema, codec, cipher));
    }
    for sp in series {
        for (k, part) in sp.parts.iter().enumerate() {
            builders[k].add_series(sp.series_id, sp.tags, part)?;
        }
    }
    for (k, b) in builders.into_iter().enumerate() {
        written.push(b.finish(dir, first_id + k as u64, wal_seq, bucket)?);
    }
    Ok(())
}

pub(crate) struct ChunkBuilder<'a> {
    schema: &'a Schema,
    codec: Codec,
    cipher: Option<CipherParams>,
    data_start: u64,
    /// Data section bytes (plaintext until `finish`; with a cipher, each
    /// block is followed by `TAG_LEN` zero bytes that `finish` fills in).
    data: Vec<u8>,
    entries: Vec<SeriesEntry>,
    raw_bytes: u64,
}

impl<'a> ChunkBuilder<'a> {
    pub(crate) fn new(schema: &'a Schema, codec: Codec, cipher: Option<CipherParams>) -> ChunkBuilder<'a> {
        let data_start = (HEADER_LEN + if cipher.is_some() { crate::chunk::ENC_HEADER_LEN } else { 0 }) as u64;
        ChunkBuilder { schema, codec, cipher, data_start, data: Vec::new(), entries: Vec::new(), raw_bytes: 0 }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Appends one series. `rows` must be sorted by timestamp and non-empty.
    pub(crate) fn add_series(&mut self, series_id: u64, tags: &[Value], rows: &[&Row]) -> Result<()> {
        let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
            return Err(invalid("empty series"));
        };
        let n = u32::try_from(rows.len()).map_err(|_| invalid("too many rows in one series chunk"))?;
        if u64::from(n) > crate::compression::MAX_SERIES_ROWS {
            return Err(invalid(format!("{n} rows in one series chunk exceed the limit")));
        }
        let mut blocks = Vec::with_capacity(self.schema.data_indices().len());
        let mut column: Vec<&Value> = Vec::with_capacity(rows.len());
        for &col in self.schema.data_indices() {
            column.clear();
            column.extend(rows.iter().map(|r| &r[col]));
            let b = block::encode(self.schema.columns()[col].ty, &column, self.codec)?;
            blocks.push(BlockRef {
                offset: self.data_start + self.data.len() as u64,
                len: u32::try_from(b.bytes.len()).map_err(|_| invalid("column block exceeds 4GiB"))?,
            });
            self.data.extend_from_slice(&b.bytes);
            if self.cipher.is_some() {
                self.data.extend_from_slice(&[0; TAG_LEN]);
            }
            self.raw_bytes += b.plain_len as u64;
        }
        self.entries.push(SeriesEntry {
            series_id,
            row_count: n,
            ts_min: self.schema.row_ts(first),
            ts_max: self.schema.row_ts(last),
            tags: tags.to_vec(),
            blocks,
        });
        Ok(())
    }

    /// Writes `<dir>/<name>.tmp`, fsyncs it and renames it into place. The
    /// caller syncs the directory and publishes the chunk via the MANIFEST.
    pub(crate) fn finish(self, dir: &Path, id: u64, wal_seq: u64, bucket: (i64, i64)) -> Result<ChunkMeta> {
        if self.entries.is_empty() {
            return Err(invalid("refusing to write an empty chunk"));
        }
        let mut bloom = Bloom::with_capacity(self.entries.len(), crate::settings::get().bloom_fpr());
        for e in &self.entries {
            bloom.insert(e.series_id);
        }
        let ts_min = self.entries.iter().map(|e| e.ts_min).min().unwrap_or(0);
        let ts_max = self.entries.iter().map(|e| e.ts_max).max().unwrap_or(0);
        let row_count: u64 = self.entries.iter().map(|e| u64::from(e.row_count)).sum();

        let start = self.data_start as usize;
        let mut buf = vec![0u8; start];
        buf.extend_from_slice(&self.data);
        let index_offset = buf.len() as u64;
        put_u64(&mut buf, self.raw_bytes);
        put_i64(&mut buf, bucket.0);
        put_i64(&mut buf, bucket.1);
        put_u32(&mut buf, self.entries.len() as u32);
        for e in &self.entries {
            encode_series_entry(&mut buf, e);
        }
        let bloom_offset = buf.len() as u64;
        bloom.encode(&mut buf);

        let header = ChunkHeader {
            version: if self.cipher.is_some() { CHUNK_AEAD } else { CHUNK_PLAIN },
            flags: chunk_flags(self.codec, self.cipher.is_some()),
            ts_min,
            ts_max,
            row_count,
            series_count: self.entries.len() as u32,
            column_count: self.schema.columns().len() as u32,
            chunk_id: id,
            wal_seq,
            schema_fingerprint: self.schema.fingerprint(),
        };
        let header_bytes = header.encode();
        buf[..HEADER_LEN].copy_from_slice(&header_bytes);
        // Authenticated encryption: every block and the index+bloom section are
        // sealed separately; the tags are bound to the header just written.
        let cipher = self.cipher.map(|c| c.authenticated(header_bytes.to_vec()));
        if let Some(c) = &cipher {
            buf[HEADER_LEN..start].copy_from_slice(&c.encode());
            for b in self.entries.iter().flat_map(|e| &e.blocks) {
                let (from, to) = (b.offset as usize, b.offset as usize + b.len as usize);
                let tag = c.seal(&mut buf[from..to], b.offset - self.data_start, &[crate::chunk::UNIT_BLOCK])?;
                buf[to..to + TAG_LEN].copy_from_slice(&tag);
            }
            let ctx = crate::chunk::index_ctx(bloom_offset);
            let tag = c.seal(&mut buf[index_offset as usize..], index_offset - self.data_start, &ctx)?;
            buf.extend_from_slice(&tag);
        }
        put_u64(&mut buf, index_offset);
        put_u64(&mut buf, bloom_offset);
        let file_crc = crc32fast::hash(&buf);
        put_u32(&mut buf, file_crc);
        buf.extend_from_slice(FOOTER_MAGIC);

        let name = chunk_file_name(bucket, id);
        let path = dir.join(&name);
        let tmp = dir.join(format!("{name}.tmp"));
        let stored = (|| -> Result<()> {
            let mut f = File::create(&tmp)?;
            f.write_all(&buf)?;
            crate::fsutil::sync_all(&f)?;
            drop(f);
            fs::rename(&tmp, &path)?;
            Ok(())
        })();
        if let Err(e) = stored {
            // ENOSPC and friends: do not leave a half-written `.tmp` behind
            // until the next recovery (it would also eat the space we lack).
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }

        Ok(ChunkMeta {
            id,
            file: ChunkFile::new(path),
            header,
            bucket,
            raw_bytes: self.raw_bytes,
            file_size: buf.len() as u64,
            series_offsets: series_offsets(&self.entries),
            series: self.entries,
            bloom,
            cipher,
            by_id: Default::default(),
            // Callers that publish the chunk set the real value (`Table`).
            born: u64::MAX,
        })
    }
}
