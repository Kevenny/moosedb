//! Compaction: merges the chunks of one time bucket into a single chunk and
//! re-encodes chunks that turned cold with the table's cold codec (ZSTD).
//!
//! Every flush adds one chunk per touched bucket, so a busy bucket
//! accumulates many small chunks; merging them makes series contiguous
//! (better compression, fewer blocks to read per query).
//!
//! Protocol (crash-safe, concurrent with reads and writes):
//! 1. under `maint`: pick the inputs and reserve an id (short `inner` lock);
//! 2. build the output chunk(s) *without* holding `inner`, one series at a
//!    time (a series too big for one column block is split across several
//!    output chunks of the same bucket, like flushes already produce);
//! 3. take `inner`, check the inputs are all still live (retention or
//!    TRUNCATE may have removed some meanwhile — then the output is
//!    discarded), switch the MANIFEST, mark the inputs obsolete.
//!
//! A crash before step 3 leaves unlisted output files, deleted by recovery.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::chunk::ChunkMeta;
use crate::chunk_reader::decode_series;
use crate::chunk_writer::ChunkBuilder;
use crate::crypto::CipherParams;
use crate::error::{Error, Result};
use crate::fsutil::sync_dir;
use crate::log::warn;
use crate::schema::{ColumnType, Row};
use crate::table::Table;
use crate::tomb::{Sel, TombSet};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactionReport {
    /// Buckets rewritten.
    pub groups: usize,
    pub chunks_in: usize,
    pub chunks_out: usize,
}

/// Range tombstones kept pending (below the 25 % dead-rows trigger) before the chunks they touch are rewritten anyway.
const MAX_PENDING_TOMBS: usize = 4096;

/// A series is cut into parts of at most this many estimated bytes (a
/// generous over-estimate of its largest encoded column block) and rows.
const SPLIT_BYTES: usize = crate::compression::MAX_BLOCK_RAW / 2;
const SPLIT_ROWS: usize = crate::compression::MAX_SERIES_ROWS as usize;

#[cfg(test)]
thread_local! {
    static SPLIT_ROWS_OVERRIDE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Test hook: lowers the rows-per-part limit of the current thread.
#[cfg(test)]
pub(crate) fn set_split_rows(n: Option<usize>) {
    SPLIT_ROWS_OVERRIDE.with(|o| o.set(n));
}

fn split_rows() -> usize {
    #[cfg(test)]
    if let Some(n) = SPLIT_ROWS_OVERRIDE.with(std::cell::Cell::get) {
        return n;
    }
    SPLIT_ROWS
}

/// Cuts sorted rows into consecutive parts that each fit one column block.
pub(crate) fn split_series<R: std::borrow::Borrow<Row>>(rows: &[R]) -> Vec<&[R]> {
    let max_rows = split_rows().max(1);
    let mut parts = Vec::new();
    let (mut start, mut bytes) = (0usize, 0usize);
    for (i, r) in rows.iter().enumerate() {
        let sz: usize = r.borrow().iter().map(crate::schema::Value::mem_size).sum();
        if i > start && (i - start >= max_rows || bytes + sz > SPLIT_BYTES) {
            parts.push(&rows[start..i]);
            start = i;
            bytes = 0;
        }
        bytes += sz;
    }
    if start < rows.len() {
        parts.push(&rows[start..]);
    }
    parts
}

impl Table {
    /// Explicit compaction (OPTIMIZE TABLE, `CALL moosedb_compact`): every
    /// bucket overlapping `[lo, hi]` that holds more than one chunk, or a
    /// chunk not yet in the cold codec, is rewritten.
    pub fn compact(&self, lo: i64, hi: i64, now: i64) -> Result<CompactionReport> {
        self.run_compaction(now, |bucket| bucket.0 <= hi && bucket.1 > lo, true)
    }

    /// Background compaction: buckets with at least
    /// `moosedb_compaction_trigger_chunks` chunks, and cold re-encoding.
    pub fn compact_auto(&self, now: i64) -> Result<CompactionReport> {
        self.run_compaction(now, |_| true, false)
    }

    /// Whether `compact_auto` would do anything (cheap; scheduler use).
    pub(crate) fn compaction_pending(&self, now: i64) -> bool {
        self.lock()
            .map(|inner| {
                inner.chunks.iter().any(|c| inner.tombs.dead_rows(c.id) >= c.header.row_count)
                    || !self.plan(&inner.chunks, now, &|_| true, false, None, &inner.no_gain, &inner.tombs).is_empty()
            })
            .unwrap_or(false)
    }

    /// Drops the chunks whose every row a DELETE has hidden: there is nothing
    /// left to rewrite. The count is checked row by row before a chunk goes
    /// (the bookkeeping that nominated it is statistics, not proof). Runs
    /// under `maint`.
    pub(crate) fn drop_dead_chunks(&self) -> Result<usize> {
        let (candidates, tombs) = {
            let inner = self.lock()?;
            inner.check_writable()?;
            let c: Vec<Arc<ChunkMeta>> = inner
                .chunks
                .iter()
                .filter(|c| inner.tombs.dead_rows(c.id) >= c.header.row_count && !inner.compacting.contains(&c.id))
                .cloned()
                .collect();
            (c, inner.tombs.clone())
        };
        if candidates.is_empty() {
            return Ok(0);
        }
        let mut dead: HashSet<u64> = HashSet::new();
        for c in &candidates {
            let alive =
                crate::delete::count_hits(&self.schema, std::slice::from_ref(c), &tombs, &Sel::All, i64::MIN, i64::MAX)?;
            if alive.is_empty() {
                dead.insert(c.id);
            }
        }
        if dead.is_empty() {
            return Ok(0);
        }
        let mut guard = self.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        // Chunks never change and a delete only hides more: still dead.
        let kept: Vec<Arc<ChunkMeta>> = inner.chunks.iter().filter(|c| !dead.contains(&c.id)).cloned().collect();
        let gone = inner.chunks.len() - kept.len();
        if gone == 0 {
            return Ok(0);
        }
        let tombs = (*inner.tombs).clone();
        inner.publish(&self.config.dir, kept, tombs, |_| {})?;
        Ok(gone)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan(
        &self,
        chunks: &[Arc<ChunkMeta>],
        now: i64,
        in_scope: &dyn Fn((i64, i64)) -> bool,
        force: bool,
        rekey_below: Option<u32>,
        no_gain: &HashSet<Vec<u64>>,
        tombs: &TombSet,
    ) -> Vec<Vec<Arc<ChunkMeta>>> {
        let opts = &self.config.opts;
        let trigger = crate::settings::get().compaction_trigger_chunks() as usize;
        // Thousands of pending tombstones cost every read of their series and every
        // MANIFEST write: past this many, chunks with dead rows are rewritten at once.
        let many_tombs = tombs.ranges().len() > MAX_PENDING_TOMBS;
        // Only numeric/time/bool data columns always get a zone (VARCHAR/DECIMAL may be
        // over the Bloom cap): without one, a rewrite can stay unzoned and OPTIMIZE
        // would redo it every time.
        let can_zone = self.schema.data_indices().iter().any(|&i| {
            i != self.schema.ts_index()
                && matches!(
                    self.schema.columns()[i].ty,
                    ColumnType::Int64 | ColumnType::Timestamp | ColumnType::Bool | ColumnType::Float32 | ColumnType::Float64
                )
        });
        let mut by_bucket: BTreeMap<(i64, i64), Vec<Arc<ChunkMeta>>> = BTreeMap::new();
        for c in chunks.iter().filter(|c| in_scope(c.bucket)) {
            by_bucket.entry(c.bucket).or_default().push(c.clone());
        }
        by_bucket
            .into_iter()
            .filter(|(bucket, group)| {
                // A group whose last compaction gained nothing (or failed) is
                // left alone until its set of chunks changes; only an explicit
                // OPTIMIZE retries it.
                if !force && no_gain.contains(&group.iter().map(|c| c.id).collect::<Vec<_>>()) {
                    return false;
                }
                let cold = !opts.is_hot(bucket.1, now);
                let recode = cold && group.iter().any(|c| c.header.codec_id() != opts.cold_codec().id());
                // OPTIMIZE re-encrypts chunks sealed with an older key version
                // (rewriting always uses the latest), so old versions can be retired.
                let rekey = rekey_below
                    .is_some_and(|v| group.iter().any(|c| c.cipher.as_ref().is_some_and(|k| k.key_version < v)));
                // OPTIMIZE also upgrades chunks of the unauthenticated CTR format
                // (format v2 with the ENCRYPTED flag) to authenticated ones.
                let legacy = force && group.iter().any(|c| c.cipher.as_ref().is_some_and(|k| !k.is_authenticated()));
                // OPTIMIZE gives chunks written before zone maps existed their summaries.
                // Not in background: it would be I/O for no urgency.
                // shortcut: a bucket whose zonable columns are all NULL stays unzoned and is
                // rewritten by every OPTIMIZE; track "rewritten, still unzoned" if that matters.
                let unzoned = force && can_zone && group.iter().any(|c| c.zones.is_none()); // also heals an unreadable summary
                // Rows a DELETE hid are only removed by rewriting: when a quarter of
                // a chunk is dead (or at OPTIMIZE), not for a handful of rows.
                let dirty = group.iter().any(|c| {
                    let d = tombs.dead_rows(c.id);
                    d > 0 && (force || d.saturating_mul(4) >= c.header.row_count || many_tombs)
                });
                group.len() >= trigger || (force && group.len() > 1) || recode || rekey || legacy || unzoned || dirty
            })
            .map(|(_, g)| g)
            .collect()
    }

    fn run_compaction(&self, now: i64, in_scope: impl Fn((i64, i64)) -> bool, force: bool) -> Result<CompactionReport> {
        // `maint` guards no data: a panic in an earlier job must not disable
        // maintenance of this table for good.
        let _maint = self.maint.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.defunct.load(Ordering::Acquire) {
            return Ok(CompactionReport::default());
        }
        let rekey_below = match self.config.opts.encryption_key_id {
            Some(k) if force => match crate::crypto::latest_version(k) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn(&format!(
                        "{}: cannot re-encrypt chunks with old key versions, latest version of key {k} unavailable: {e}",
                        self.config.dir.display()
                    ));
                    None
                }
            },
            _ => None,
        };
        // Chunks a DELETE emptied need no rewrite.
        self.drop_dead_chunks()?;
        let groups = {
            let inner = self.lock()?;
            inner.check_writable()?;
            self.plan(&inner.chunks, now, &in_scope, force, rekey_below, &inner.no_gain, &inner.tombs)
        };
        let mut report = CompactionReport::default();
        let mut first_error = None;
        for group in groups {
            let n = group.len();
            let mut key: Vec<u64> = group.iter().map(|c| c.id).collect();
            key.sort_unstable();
            match self.compact_group(group, now) {
                Ok(Some(out)) => {
                    report.groups += 1;
                    report.chunks_in += n;
                    report.chunks_out += out.len();
                    if out.len() > 1 {
                        // The output was split (a series too big for one block):
                        // merging it again would only split it again, so leave
                        // it alone until its set of chunks changes.
                        self.remember_no_gain(out);
                    }
                }
                Ok(None) => {}
                // The table is unusable: stop.
                Err(e @ Error::ReadOnly(_)) => return Err(e),
                // One bad bucket must not starve the others, nor be retried every tick.
                Err(e) => {
                    warn(&format!("compaction of chunks {key:?} failed, skipping them until they change: {e}"));
                    self.remember_no_gain(key);
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) if report.groups == 0 => Err(e),
            _ => Ok(report),
        }
    }

    fn remember_no_gain(&self, mut ids: Vec<u64>) {
        ids.sort_unstable();
        if let Ok(mut inner) = self.lock() {
            if inner.no_gain.len() >= 1024 {
                inner.no_gain.clear();
            }
            inner.no_gain.insert(ids);
        }
    }

    /// Returns the ids of the chunks written, or `None` when the inputs
    /// changed concurrently and nothing was done.
    fn compact_group(&self, group: Vec<Arc<ChunkMeta>>, now: i64) -> Result<Option<Vec<u64>>> {
        let ids: Vec<u64> = group.iter().map(|c| c.id).collect();
        let (id, tombs, upto) = {
            let mut inner = self.lock()?;
            inner.check_writable()?;
            if !ids.iter().all(|i| inner.chunks.iter().any(|c| c.id == *i)) {
                return Ok(None);
            }
            let id = inner.reserve_chunk_ids(1)?;
            inner.compacting.extend(ids.iter().copied());
            (id, inner.tombs.clone(), inner.tseq)
        };

        let built = self.build_merged(&group, id, now, &tombs, upto);

        let metas = {
            let mut inner = self.lock()?;
            for i in &ids {
                inner.compacting.remove(i);
            }
            match built {
                Ok(m) => m,
                Err(e) => {
                    if crate::fsutil::is_fsync_error(&e) {
                        inner.poison(format!("fsync of a compacted chunk failed: {e}"));
                    }
                    return Err(e);
                }
            }
        };
        let out_ids: Vec<u64> = metas.iter().map(|m| m.id).collect();
        let outputs: Vec<Arc<ChunkMeta>> = metas.into_iter().map(Arc::new).collect();
        let discard = |outputs: &[Arc<ChunkMeta>]| {
            for m in outputs {
                m.file.mark_obsolete();
            }
        };
        // The output holds no row of a delete up to `upto`; newer deletes still
        // hide rows of it (its `born` says so): count, per output chunk, what each
        // of them hides there (in order, each one excluding what the earlier hid).
        // Done without the table mutex for the deletes known now; the few that
        // arrive meanwhile are counted under it.
        let mut fresh: Vec<(u64, Vec<(u64, u64)>)> = Vec::new();
        let mut seen = TombSet::default();
        let mut counted_upto = upto;
        let count_newer = |newer: &[Arc<crate::tomb::RangeTomb>],
                           fresh: &mut Vec<(u64, Vec<(u64, u64)>)>,
                           seen: &mut TombSet|
         -> Result<()> {
            for t in newer {
                let hits = crate::delete::count_hits(&self.schema, &outputs, seen, &t.sel, t.lo, t.hi)?;
                seen.add_range(crate::tomb::RangeTomb { hits: hits.clone(), ..(**t).clone() });
                fresh.push((t.seq, hits));
            }
            Ok(())
        };
        let newer0: Vec<Arc<crate::tomb::RangeTomb>> = {
            let inner = self.lock()?;
            inner.tombs.ranges().iter().filter(|t| t.seq > upto).cloned().collect()
        };
        if let Err(e) = count_newer(&newer0, &mut fresh, &mut seen) {
            discard(&outputs);
            return Err(e);
        }
        if let Some(last) = newer0.last() {
            counted_upto = last.seq;
        }

        let mut inner = self.lock()?;
        let still_live = ids.iter().all(|i| inner.chunks.iter().any(|c| c.id == *i));
        // A delete of exact rows that landed on an input after the snapshot
        // names ordinals of a file that is about to go: leave it for the next pass.
        let raced = inner.tombs.rows_tombs().iter().any(|t| t.seq > upto && ids.contains(&t.chunk));
        if !still_live || raced || self.defunct.load(Ordering::Acquire) {
            discard(&outputs);
            return Ok(None);
        }
        let mut tombs = (*inner.tombs).clone();
        let newer1: Vec<Arc<crate::tomb::RangeTomb>> =
            tombs.ranges().iter().filter(|t| t.seq > counted_upto).cloned().collect();
        if let Err(e) = count_newer(&newer1, &mut fresh, &mut seen) {
            discard(&outputs);
            return Err(e);
        }
        let mut chunks: Vec<Arc<ChunkMeta>> = inner.chunks.iter().filter(|c| !ids.contains(&c.id)).cloned().collect();
        chunks.extend(outputs);
        let inputs: HashSet<u64> = ids.iter().copied().collect();
        tombs.rehit(&inputs, upto, &fresh);
        inner.publish(&self.config.dir, chunks, tombs, |_| {})?;
        Ok(Some(out_ids))
    }

    /// Merges `group` into one chunk, or into several (all of the same
    /// bucket) when a series is too large for a single column block. `id` is
    /// the first output id; more are reserved on demand.
    fn build_merged(
        &self,
        group: &[Arc<ChunkMeta>],
        id: u64,
        now: i64,
        tombs: &TombSet,
        upto: u64,
    ) -> Result<Vec<ChunkMeta>> {
        let opts = &self.config.opts;
        let bucket = group[0].bucket;
        let codec = opts.codec_for(bucket.1, now);
        let new_builder = || -> Result<ChunkBuilder<'_>> {
            let cipher = opts.encryption_key_id.map(CipherParams::for_new_file).transpose()?;
            Ok(ChunkBuilder::new(&self.schema, codec, cipher))
        };
        let mut builders = vec![new_builder()?];

        let positions: Vec<HashMap<u64, usize>> =
            group.iter().map(|c| c.series.iter().enumerate().map(|(i, e)| (e.series_id, i)).collect()).collect();
        let series_ids: BTreeSet<u64> = positions.iter().flat_map(|m| m.keys().copied()).collect();
        let ts = self.schema.ts_index();
        for sid in series_ids {
            let mut rows: Vec<Row> = Vec::new();
            let mut tags = Vec::new();
            for (c, pos) in group.iter().zip(&positions) {
                if let Some(&idx) = pos.get(&sid) {
                    let mut part = decode_series(&self.schema, c, idx)?;
                    if !tombs.is_empty() {
                        // Rows a DELETE hid (up to the snapshot `tombs` was taken at) are not copied.
                        let holes = tombs.holes(c.id, idx);
                        let mut i = 0usize;
                        part.retain(|r| {
                            let t = match r.get(ts) {
                                Some(crate::schema::Value::Timestamp(t)) => *t,
                                _ => i64::MIN,
                            };
                            let dead = tombs.range_hides(sid, c.born, t)
                                || crate::tomb::in_runs(holes, i);
                            i += 1;
                            !dead
                        });
                    }
                    if !part.is_empty() {
                        rows.extend(part);
                        tags.clone_from(&c.series[idx].tags);
                    }
                }
            }
            // Inputs are in id (= write) order and each is sorted, so a stable
            // sort keeps arrival order among equal timestamps.
            rows.sort_by_key(|r| match r.get(ts) {
                Some(crate::schema::Value::Timestamp(t)) => *t,
                _ => i64::MIN,
            });
            // Part k of every series goes to output chunk k, so a series never
            // appears twice in one chunk.
            for (k, part) in split_series(&rows).into_iter().enumerate() {
                if k == builders.len() {
                    builders.push(new_builder()?);
                }
                let refs: Vec<&Row> = part.iter().collect();
                builders[k].add_series(sid, &tags, &refs)?;
            }
        }
        let wal_seq = group.iter().map(|c| c.header.wal_seq).max().unwrap_or(0);
        // Everything hidden was left out: the output starts clean (a series whose
        // rows were all deleted leaves nothing, and a bucket of such series no chunk).
        builders.retain(|b| !b.is_empty());
        if builders.is_empty() {
            return Ok(Vec::new());
        }

        // Extra ids (rare) come from the shared counter; the first was reserved up front.
        let extra = builders.len() - 1;
        let first_extra = if extra > 0 { self.lock()?.reserve_chunk_ids(extra as u64)? } else { 0 };
        let mut metas: Vec<ChunkMeta> = Vec::with_capacity(builders.len());
        for (k, b) in builders.into_iter().enumerate() {
            let cid = if k == 0 { id } else { first_extra + (k as u64 - 1) };
            let done =
                b.finish(&self.config.dir, cid, wal_seq, bucket).and_then(|m| match sync_dir(&self.config.dir) {
                    Ok(()) => Ok(m),
                    Err(e) => {
                        m.file.mark_obsolete();
                        Err(e)
                    }
                });
            match done {
                Ok(mut m) => {
                    m.born = upto;
                    metas.push(m);
                }
                Err(e) => {
                    for m in &metas {
                        m.file.mark_obsolete();
                    }
                    return Err(e);
                }
            }
        }
        Ok(metas)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::{RawOptions, TableConfig};
    use crate::scan::ScanFilter;
    use crate::schema::{Column, ColumnType, Schema, Value};

    fn config(dir: &std::path::Path) -> TableConfig {
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "v".into(), ty: ColumnType::Float64 },
            ],
            0,
        )
        .unwrap();
        TableConfig::new(dir, schema, &RawOptions::default()).unwrap()
    }

    fn row(i: i64, host: &str) -> Row {
        vec![
            Value::Timestamp(1_700_000_000_000_000 + i),
            Value::Bytes(host.as_bytes().to_vec()),
            Value::Float64(i as f64),
        ]
    }

    fn scan_all(t: &Table) -> Vec<Row> {
        let mut s = t.scan(&ScanFilter::default(), true).unwrap();
        let mut out = Vec::new();
        while let Some((_, r)) = s.next_row().unwrap() {
            out.push(r);
        }
        out
    }

    #[test]
    fn optimize_does_not_rewrite_a_chunk_that_can_never_get_zone_maps() {
        // ts + TAG + a VARCHAR over the Bloom cap: no column gets a zone, so the
        // chunk stays unzoned and a rewrite would be pure I/O, every OPTIMIZE.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "msg".into(), ty: ColumnType::Varchar },
            ],
            0,
        )
        .unwrap();
        let cfg = TableConfig::new(&path, schema, &RawOptions::default()).unwrap();
        Table::create(&cfg).unwrap();
        let t = Table::open(cfg).unwrap();
        for i in 0..70_000i64 {
            t.write(vec![
                Value::Timestamp(1_700_000_000_000_000 + i),
                Value::Bytes(b"a".to_vec()),
                Value::Bytes(format!("m{i}").into_bytes()),
            ])
            .unwrap();
        }
        t.flush().unwrap();
        assert!(t.chunks().unwrap().iter().all(|c| c.zones.is_none()));
        let report = t.compact(i64::MIN, i64::MAX, 2_000_000_000_000_000).unwrap();
        assert_eq!(report.groups, 0);
    }

    #[test]
    fn split_series_keeps_every_part_within_the_block_budget() {
        // Three rows of ~100 MiB each (never touched, so no memory is used)
        // cannot share one 256 MiB column block with room to spare.
        let big = |ts: i64| -> Row { vec![Value::Timestamp(ts), Value::Bytes(vec![0u8; 100 << 20])] };
        let rows: Vec<Row> = (0..3).map(big).collect();
        let parts = split_series(&rows);
        assert_eq!(parts.len(), 3);
        assert!(parts.iter().all(|p| p.len() == 1));
        let small: Vec<Row> = (0..1000).map(|i| vec![Value::Timestamp(i), Value::Float64(1.0)]).collect();
        assert_eq!(split_series(&small).len(), 1);
    }

    #[test]
    fn oversized_series_is_split_into_several_chunks_and_stays_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let cfg = config(&path);
        Table::create(&cfg).unwrap();
        let t = Table::open(cfg.clone()).unwrap();
        let expected: Vec<Row> = (0..100).map(|i| row(i, if i % 10 == 0 { "b" } else { "a" })).collect();
        for chunk in expected.chunks(25) {
            for r in chunk {
                t.write(r.clone()).unwrap();
            }
            t.flush().unwrap();
        }
        assert_eq!(t.chunks().unwrap().len(), 4);

        set_split_rows(Some(30)); // series "a" (90 rows) needs 3 parts, "b" (10) one
        let report = t.compact(i64::MIN, i64::MAX, 1_700_000_000_000_000);
        set_split_rows(None);
        let report = report.unwrap();
        assert_eq!((report.groups, report.chunks_in, report.chunks_out), (1, 4, 3));
        assert_eq!(t.chunks().unwrap().len(), 3);
        assert_eq!(scan_all(&t), expected);

        // The background pass must not pick the split output again and again.
        assert!(!t.compaction_pending(1_700_000_000_000_000 + 1));
        drop(t);
        let t = Table::open(cfg).unwrap();
        assert_eq!(scan_all(&t), expected, "survives a restart");
    }

    #[test]
    fn failing_group_is_skipped_not_retried_and_does_not_block_others() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let cfg = config(&path);
        Table::create(&cfg).unwrap();
        let t = Table::open(cfg).unwrap();
        let day = crate::time::MICROS_PER_DAY;
        let base = (1_700_000_000_000_000 / day) * day;
        // Two buckets (days) with two chunks each.
        for d in 0..2 {
            for round in 0..2 {
                t.write(vec![
                    Value::Timestamp(base + d * day + round),
                    Value::Bytes(b"a".to_vec()),
                    Value::Float64(1.0),
                ])
                .unwrap();
                t.flush().unwrap();
            }
        }
        // Damage the chunks of the first bucket so merging them fails.
        let chunks = t.chunks().unwrap();
        let first = chunks.iter().filter(|c| c.bucket.0 == base).collect::<Vec<_>>();
        assert_eq!(first.len(), 2);
        for c in &first {
            let mut data = std::fs::read(c.path()).unwrap();
            data[crate::chunk::HEADER_LEN + 12] ^= 0xFF; // inside a data block
            std::fs::write(c.path(), data).unwrap();
        }
        let report = t.compact(i64::MIN, i64::MAX, base + 10 * day).unwrap();
        assert_eq!(report.groups, 1, "the healthy bucket was still compacted");
        let still_two = t.chunks().unwrap().iter().filter(|c| c.bucket.0 == base).count();
        assert_eq!(still_two, 2);
        // Background passes leave the broken group alone.
        assert!(!t.compaction_pending(base + 10 * day));
    }
}
