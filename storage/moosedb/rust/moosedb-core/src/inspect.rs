//! Read-only diagnostics for INFORMATION_SCHEMA.MOOSEDB_TABLES/CHUNKS.
//!
//! An open table is described from its live state. A closed table is read
//! from disk without modifying anything (no recovery): MANIFEST, OPTIONS,
//! chunk metadata, and a count of the rows still waiting in the WAL.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::chunk::{parse_chunk_id, ChunkMeta};
use crate::chunk_reader::read_meta;
use crate::error::Result;
use crate::manifest::Manifest;
use crate::options::TableOptions;
use crate::table::OPTIONS_FILE;
use crate::wal;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ChunkStatus {
    /// Newer than HOT_THRESHOLD.
    Hot = 0,
    /// Older than HOT_THRESHOLD but not yet re-encoded with the cold codec.
    Warm = 1,
    /// In its final, cold encoding.
    Cold = 2,
    /// Being merged right now.
    Compacting = 3,
    /// Past RETENTION_PERIOD, waiting for the next sweep.
    Expired = 4,
}

impl ChunkStatus {
    pub fn name(self) -> &'static str {
        match self {
            ChunkStatus::Hot => "HOT",
            ChunkStatus::Warm => "WARM",
            ChunkStatus::Cold => "COLD",
            ChunkStatus::Compacting => "COMPACTING",
            ChunkStatus::Expired => "EXPIRED",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChunkInfo {
    pub id: u64,
    pub file_name: String,
    pub ts_min: i64,
    pub ts_max: i64,
    pub rows: u64,
    pub series: u32,
    pub data_bytes: u64,
    pub file_bytes: u64,
    pub codec: u8,
    pub encrypted: bool,
    pub status: ChunkStatus,
    /// File modification time (chunks are immutable once sealed), µs.
    pub sealed_at: i64,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub is_open: bool,
    pub row_count: u64,
    /// Rows not yet sealed into a chunk (MemTable, or WAL for closed tables).
    pub pending_rows: u64,
    pub series_count: u64,
    pub data_bytes: u64,
    pub file_bytes: u64,
    pub options: Option<TableOptions>,
    pub chunks: Vec<ChunkInfo>,
}

impl TableInfo {
    pub fn count(&self, status: ChunkStatus) -> u32 {
        self.chunks.iter().filter(|c| c.status == status).count() as u32
    }
}

fn status(c: &ChunkMeta, opts: Option<&TableOptions>, compacting: &HashSet<u64>, now: i64) -> ChunkStatus {
    if compacting.contains(&c.id) {
        return ChunkStatus::Compacting;
    }
    let Some(o) = opts else { return ChunkStatus::Hot };
    if o.retention.cutoff(now).is_some_and(|cut| c.header.ts_max < cut) {
        ChunkStatus::Expired
    } else if o.is_hot(c.bucket.1, now) {
        ChunkStatus::Hot
    } else if c.header.codec_id() == o.cold_codec().id() {
        ChunkStatus::Cold
    } else {
        ChunkStatus::Warm
    }
}

fn chunk_info(c: &ChunkMeta, st: ChunkStatus, dead: u64) -> ChunkInfo {
    let sealed_at = fs::metadata(c.path())
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|d| i64::try_from(d.as_micros()).ok())
        .unwrap_or(0);
    ChunkInfo {
        id: c.id,
        file_name: c.file_name(),
        ts_min: c.header.ts_min,
        ts_max: c.header.ts_max,
        rows: c.header.row_count.saturating_sub(dead),
        series: c.header.series_count,
        data_bytes: c.raw_bytes,
        file_bytes: c.file_size,
        codec: c.header.codec_id(),
        encrypted: c.header.is_encrypted(),
        status: st,
        sealed_at,
    }
}

/// Describes the table stored in `dir` at time `now` (µs).
pub fn inspect(dir: &Path, now: i64) -> Result<TableInfo> {
    if let Some(t) = crate::maintenance::lookup(dir) {
        let opts = t.config().opts.clone();
        let (chunks, compacting, pending, series, mem_bytes) = {
            let inner = t.lock()?;
            (
                inner.chunks.clone(),
                inner.compacting.clone(),
                inner.mem_len() as u64,
                inner.series.len() as u64,
                inner.mem_bytes() as u64,
            )
        };
        let tombs = t.lock()?.tombs.clone();
        return Ok(build(true, &chunks, Some(opts), &compacting, pending, Some(series), mem_bytes, now, &tombs));
    }

    // A closed table is described from its files: deletes still only in the WAL are not
    // subtracted from `row_count` (they are not applied without opening the table).
    let manifest = Manifest::load(dir)?;
    let options = fs::read_to_string(dir.join(OPTIONS_FILE)).ok().and_then(|t| TableOptions::from_text(&t).ok());
    let mut files = std::collections::HashMap::new();
    for e in fs::read_dir(dir)? {
        let e = e?;
        if let Some(id) = e.file_name().to_str().and_then(parse_chunk_id) {
            files.insert(id, e.path());
        }
    }
    let mut chunks = Vec::new();
    for id in &manifest.chunks {
        if let Some(p) = files.get(id) {
            chunks.push(Arc::new(read_meta(p, *id)?));
        }
    }
    let mut pending = 0u64;
    let segs: Vec<u64> = wal::list_segments(dir)?.into_iter().filter(|&s| s >= manifest.replay_seq).collect();
    wal::replay_committed(dir, &segs, manifest.wal_seq, false, options.as_ref().map(|o| o.encryption_key_id.is_some()), &wal::ReplayLimits::standard(), &|_| false, |_, _| {
        pending += 1;
        Ok(())
    })?;
    let mut tombs = crate::tomb::TombSet::default();
    for t in &manifest.ranges {
        tombs.add_range(t.clone());
    }
    for t in &manifest.rows {
        tombs.add_rows(t.clone());
    }
    Ok(build(false, &chunks, options, &HashSet::new(), pending, None, 0, now, &tombs))
}

#[allow(clippy::too_many_arguments)]
fn build(
    is_open: bool,
    chunks: &[Arc<ChunkMeta>],
    options: Option<TableOptions>,
    compacting: &HashSet<u64>,
    pending_rows: u64,
    series_count: Option<u64>,
    pending_bytes: u64,
    now: i64,
    tombs: &crate::tomb::TombSet,
) -> TableInfo {
    let infos: Vec<ChunkInfo> =
        chunks.iter().map(|c| chunk_info(c, status(c, options.as_ref(), compacting, now), tombs.dead_rows(c.id))).collect();
    let series_count = series_count.unwrap_or_else(|| {
        chunks.iter().flat_map(|c| c.series.iter().map(|e| e.series_id)).collect::<HashSet<_>>().len() as u64
    });
    TableInfo {
        is_open,
        row_count: infos.iter().map(|c| c.rows).sum::<u64>() + pending_rows,
        pending_rows,
        series_count,
        data_bytes: infos.iter().map(|c| c.data_bytes).sum::<u64>() + pending_bytes,
        file_bytes: infos.iter().map(|c| c.file_bytes).sum(),
        options,
        chunks: infos,
    }
}
