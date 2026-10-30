//! `DELETE`: logical deletes by tombstone (see `tomb` for the model).
//!
//! Two operations, both atomic and durable like an `INSERT` statement:
//!
//! * [`Table::delete_range`] -- every row of some series (or all) between two
//!   timestamps. One WAL entry, one tombstone, no per-row work: this is what
//!   the Zabbix housekeeper does thousands of times.
//! * [`Table::delete_rows`] -- exact rows (found by content), for any other
//!   `WHERE`. Rows in the MemTable are removed from it; rows in chunks get a
//!   row tombstone naming their ordinals.
//!
//! Protocol of one delete (the same ordering rule as a batch COMMIT):
//!
//! 1. under `del_gate` (deletes of one table run one at a time, so the dead-row
//!    counts stay exact), with the table mutex *not* held, find what the delete
//!    hides in the chunks of a snapshot (counting, or locating rows);
//! 2. take the table mutex; if the set of chunks changed meanwhile (a flush, a
//!    compaction) go back to 1; otherwise
//! 3. append one DELETE entry to the WAL and sync it -- this is the commit
//!    point, and a failure here changes nothing in memory;
//! 4. only then apply it: remove the rows from the MemTable, add the tombstone,
//!    invalidate the UNIQUE state of the series touched, publish.
//!
//! The MANIFEST is not rewritten by a delete. The tombstones live in the WAL
//! until the next structural change (flush, compaction...) stores them in the
//! MANIFEST; recovery re-applies the DELETE entries newer than the MANIFEST's
//! `tseq` (see `Table::open`). A crash anywhere leaves the table as it was
//! before the delete or as it is after it.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError};

use crate::chunk::ChunkMeta;
use crate::chunk_reader::load_window;
use crate::codec::encode_row;
use crate::error::{corrupt, Error, Result};
use crate::index::series::{row_tags, SeriesIndex};
use crate::memtable::MemTable;
use crate::schema::{Row, Schema, Value};
use crate::table::{Inner, Table};
use crate::tomb::{in_runs, runs_of, RangeTomb, RowsTomb, Sel, TombSet};
use crate::unique::UniqueState;
use crate::wal::{self, DeleteEntry};

/// Attempts at step 1-2 before the count is done under the table mutex.
const OPTIMISTIC_ATTEMPTS: usize = 6;
/// Rows per atomic piece of [`Table::delete_rows`] (bytes of the rows).
const PIECE_BYTES: usize = 2 << 20;

/// Result of [`Table::delete_range`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeDelete {
    /// This many rows were deleted.
    Deleted(u64),
    /// More rows than `max_rows` match; nothing was deleted.
    Exceeds(u64),
}

/// Rows of series entry `idx` of `c` with `a <= ts <= b` that no row tombstone
/// has hidden (range tombstones are the caller's business: it cut `[a, b]`).
fn alive_rows_in(schema: &Schema, c: &ChunkMeta, idx: usize, tombs: &TombSet, a: i64, b: i64) -> Result<u64> {
    let Some(e) = c.series.get(idx) else { return Ok(0) };
    let (lo, hi) = if e.ts_min >= a && e.ts_max <= b {
        (0usize, e.row_count as usize)
    } else {
        let none = vec![false; schema.columns().len()];
        match load_window(schema, c, idx, (a, b), &none, false)? {
            Some(w) => (w.lo, w.hi),
            None => return Ok(0),
        }
    };
    let dead: usize = tombs
        .holes(c.id, idx)
        .iter()
        .map(|&(s, n)| {
            let (s, e) = (s as usize, s as usize + n as usize);
            e.min(hi).saturating_sub(s.max(lo))
        })
        .sum();
    Ok((hi - lo).saturating_sub(dead) as u64)
}

/// Whether two values are the same stored value (floats bit for bit).
fn same_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float64(x), Value::Float64(y)) => x.to_bits() == y.to_bits(),
        (Value::Float32(x), Value::Float32(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

fn encoded(row: &[Value]) -> Vec<u8> {
    let mut b = Vec::new();
    encode_row(&mut b, row);
    b
}


/// Rows `sel` selects between `lo` and `hi` that the chunks of `chunks`
/// still show, per chunk: `(chunk id, rows)`, chunks with none left out.
pub(crate) fn count_hits(
    schema: &Schema,
    chunks: &[Arc<ChunkMeta>],
    tombs: &TombSet,
    sel: &Sel,
    lo: i64,
    hi: i64,
) -> Result<Vec<(u64, u64)>> {
    let ids: Option<HashSet<u64>> = match sel {
        Sel::All => None,
        Sel::Ids(v) => Some(v.iter().copied().collect()),
    };
    let mut out = Vec::new();
    for c in chunks {
        if !c.overlaps(lo, hi) {
            continue;
        }
        let entries: Vec<usize> = match &ids {
            None => (0..c.series.len()).collect(),
            Some(ids) => {
                if ids.len() <= 64 && !ids.iter().any(|&id| c.bloom.may_contain(id)) {
                    continue;
                }
                match c.entries_of(ids) {
                    Some(v) => v,
                    None => (0..c.series.len()).filter(|&i| ids.contains(&c.series[i].series_id)).collect(),
                }
            }
        };
        let mut n = 0u64;
        for idx in entries {
            let e = &c.series[idx];
            if e.ts_max < lo || e.ts_min > hi {
                continue;
            }
            for (a, b) in tombs.alive_intervals(e.series_id, c.born, lo, hi) {
                n += alive_rows_in(schema, c, idx, tombs, a, b)?;
            }
        }
        if n > 0 {
            out.push((c.id, n));
        }
    }
    Ok(out)
}

impl Table {
    /// Rows a [`Table::delete_range`] with the same arguments would delete
    /// right now (a snapshot count: a statement racing with writers may see a
    /// different number when it runs).
    pub fn count_range(&self, series: Option<&[u64]>, lo: i64, hi: i64) -> Result<u64> {
        let Some(sel) = Self::selection(series, lo, hi) else { return Ok(0) };
        let (chunks, tombs, in_mem) = {
            let guard = self.lock()?;
            let mut n = 0u64;
            guard.visit_mem_range(&self.schema, lo, hi, |r| n += u64::from(sel.contains(r.series_id)));
            (guard.chunks.clone(), guard.tombs.clone(), n)
        };
        let hits = count_hits(&self.schema, &chunks, &tombs, &sel, lo, hi)?;
        Ok(in_mem + hits.iter().map(|h| h.1).sum::<u64>())
    }

    fn selection(series: Option<&[u64]>, lo: i64, hi: i64) -> Option<Sel> {
        if lo > hi {
            return None;
        }
        match series {
            None => Some(Sel::All),
            Some([]) => None,
            Some(ids) => Some(Sel::from_ids(ids.to_vec())),
        }
    }

    /// Deletes every row of the series in `series` (all series if `None`) with
    /// `lo <= ts <= hi`. With `max_rows`, nothing is deleted when more rows
    /// match (`RangeDelete::Exceeds`). `durable`: fsync the WAL before
    /// returning (otherwise it is only handed to the OS, as for inserts).
    pub fn delete_range(
        &self,
        series: Option<&[u64]>,
        lo: i64,
        hi: i64,
        max_rows: Option<u64>,
        durable: bool,
    ) -> Result<RangeDelete> {
        let Some(sel) = Self::selection(series, lo, hi) else { return Ok(RangeDelete::Deleted(0)) };
        let _gate = self.del_gate.lock().unwrap_or_else(PoisonError::into_inner);
        for _ in 0..OPTIMISTIC_ATTEMPTS {
            let (chunks, tombs, gen, epoch) = {
                let g = self.lock()?;
                g.check_writable()?;
                (g.chunks.clone(), g.tombs.clone(), g.layout_gen, g.epoch)
            };
            let hits = count_hits(&self.schema, &chunks, &tombs, &sel, lo, hi)?;
            // Only the table may hold the tombstone set while the delete adds to it (copy-on-write).
            drop((chunks, tombs));
            // Idle: the delete edits the MemTable's rows, which a flush may be sealing.
            let mut guard = self.lock_idle()?;
            let inner = &mut *guard;
            inner.check_writable()?;
            if inner.layout_gen != gen || inner.epoch != epoch {
                continue;
            }
            return self.commit_range_delete(inner, sel, lo, hi, hits, max_rows, durable);
        }
        // Writers keep reshaping the chunks: count under the mutex.
        let mut guard = self.lock_idle()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        let hits = count_hits(&self.schema, &inner.chunks, &inner.tombs, &sel, lo, hi)?;
        self.commit_range_delete(inner, sel, lo, hi, hits, max_rows, durable)
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_range_delete(
        &self,
        inner: &mut Inner,
        sel: Sel,
        lo: i64,
        hi: i64,
        hits: Vec<(u64, u64)>,
        max_rows: Option<u64>,
        durable: bool,
    ) -> Result<RangeDelete> {
        let in_chunks: u64 = hits.iter().map(|h| h.1).sum();
        let mut in_mem = 0u64;
        inner.memtable.visit_range(&self.schema, lo, hi, |r| in_mem += u64::from(sel.contains(r.series_id)));
        let total = in_chunks + in_mem;
        if total == 0 {
            return Ok(RangeDelete::Deleted(0));
        }
        if max_rows.is_some_and(|m| total > m) {
            return Ok(RangeDelete::Exceeds(total));
        }
        let seq = inner.tseq.checked_add(1).ok_or_else(|| corrupt("tombstone sequence numbers are exhausted"))?;
        let tag_lists: Option<Vec<&[Value]>> = match &sel {
            Sel::All => None,
            Sel::Ids(ids) => Some(ids.iter().filter_map(|&id| inner.series.tags(id)).collect()),
        };
        let entry = wal::range_delete_entry(seq, lo, hi, tag_lists.as_deref());
        if entry.len() > wal::MAX_DELETE_ENTRY {
            return Err(Error::Unsupported(format!(
                "the DELETE selects too many series to log atomically ({} bytes > {})",
                entry.len(),
                wal::MAX_DELETE_ENTRY
            )));
        }
        log_delete(inner, &entry, durable)?;

        // Committed. Apply.
        inner.changed();
        let removed = inner.memtable.remove_where(&self.schema, lo, hi, |r| sel.contains(r.series_id));
        if removed > 0 {
            inner.mem_generation += 1;
        }
        inner.tseq = seq;
        if !hits.is_empty() {
            Arc::make_mut(&mut inner.tombs).add_range(RangeTomb { seq, lo, hi, sel: sel.clone(), hits });
        }
        forget_unique(&mut inner.uniq, &sel);
        Ok(RangeDelete::Deleted(total))
    }

    /// Deletes exactly these rows (each one row of the table; equal rows are
    /// deleted once each, if the table holds that many). Rows the table does
    /// not (or no longer) hold are skipped; returns how many were deleted.
    ///
    /// The rows are processed in pieces of about [`PIECE_BYTES`], each one
    /// atomic and durable; a crash between two pieces leaves a prefix.
    pub fn delete_rows(&self, rows: Vec<Row>, durable: bool) -> Result<u64> {
        for r in &rows {
            self.schema.validate_row(r)?;
        }
        let _gate = self.del_gate.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut total, mut piece, mut bytes) = (0u64, Vec::new(), 0usize);
        for r in rows {
            let sz: usize = r.iter().map(Value::mem_size).sum();
            if bytes + sz > PIECE_BYTES && !piece.is_empty() {
                total += self.delete_piece(std::mem::take(&mut piece), durable)?;
                bytes = 0;
            }
            bytes += sz;
            piece.push(r);
        }
        if !piece.is_empty() {
            total += self.delete_piece(piece, durable)?;
        }
        Ok(total)
    }

    fn delete_piece(&self, rows: Vec<Row>, durable: bool) -> Result<u64> {
        // Equal rows form a group: `k` of them are to go.
        struct Group {
            row: Row,
            ts: i64,
            tags: Vec<Value>,
            k: usize,
        }
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut groups: Vec<Group> = Vec::new();
        for row in rows {
            let key = encoded(&row);
            match index.get(&key) {
                Some(&g) => groups[g].k += 1,
                None => {
                    index.insert(key, groups.len());
                    groups.push(Group { ts: self.schema.row_ts(&row), tags: row_tags(&self.schema, &row), row, k: 1 });
                }
            }
        }
        let want: Vec<bool> = (0..self.schema.columns().len()).map(|c| self.schema.tag_position(c).is_none()).collect();
        for attempt in 0..=OPTIMISTIC_ATTEMPTS {
            let last = attempt == OPTIMISTIC_ATTEMPTS;
            // Step 1: where could each group's rows be in the chunks?
            let (chunks, tombs, gen, epoch, sids) = {
                let g = self.lock()?;
                g.check_writable()?;
                let sids: Vec<Option<u64>> = groups.iter().map(|x| g.series.lookup(&x.tags)).collect();
                (g.chunks.clone(), g.tombs.clone(), g.layout_gen, g.epoch, sids)
            };
            let mut guard;
            let found: Vec<Vec<(u64, u32, u32)>>;
            if last {
                drop((chunks, tombs));
                guard = self.lock_idle()?;
                guard.check_writable()?;
                let sids2: Vec<Option<u64>> = groups.iter().map(|x| guard.series.lookup(&x.tags)).collect();
                found = groups
                    .iter()
                    .zip(&sids2)
                    .map(|(g, s)| self.locate(&guard.chunks, &guard.tombs, *s, g.ts, &g.row, g.k, &want))
                    .collect::<Result<_>>()?;
            } else {
                found = groups
                    .iter()
                    .zip(&sids)
                    .map(|(g, s)| self.locate(&chunks, &tombs, *s, g.ts, &g.row, g.k, &want))
                    .collect::<Result<_>>()?;
                drop((chunks, tombs));
                guard = self.lock_idle()?;
                guard.check_writable()?;
                if guard.layout_gen != gen || guard.epoch != epoch {
                    continue;
                }
            }
            let inner = &mut *guard;

            // Step 2: the MemTable first (cheaper, and what an equal row in a
            // chunk would lose to), then the chunks for what is left.
            let sids: Vec<Option<u64>> = groups.iter().map(|x| inner.series.lookup(&x.tags)).collect();
            let (lo, hi) = groups.iter().fold((i64::MAX, i64::MIN), |(l, h), g| (l.min(g.ts), h.max(g.ts)));
            // (series, timestamp) of every group: a cheap test before a row is encoded.
            let mut by_key: HashSet<(u64, i64)> = HashSet::new();
            for (gi, s) in sids.iter().enumerate() {
                if let Some(s) = s {
                    by_key.insert((*s, groups[gi].ts));
                }
            }
            let ks: Vec<usize> = groups.iter().map(|g| g.k).collect();
            let take_from_mem = |left: &mut Vec<usize>, r: &crate::memtable::MemRow, ts: i64| -> bool {
                if !by_key.contains(&(r.series_id, ts)) {
                    return false;
                }
                match index.get(&encoded(&r.row)) {
                    Some(&gi) if left[gi] > 0 => {
                        left[gi] -= 1;
                        true
                    }
                    _ => false,
                }
            };
            let mut left = ks.clone();
            let mut mem_rows: Vec<Row> = Vec::new();
            inner.memtable.visit_range(&self.schema, lo, hi, |r| {
                let ts = self.schema.row_ts(&r.row);
                if take_from_mem(&mut left, r, ts) {
                    mem_rows.push(r.row.clone());
                }
            });
            let from_mem: Vec<usize> = ks.iter().zip(&left).map(|(k, l)| k - l).collect();

            // What the chunks give for the rest.
            let mut dead: HashMap<(u64, u32), Vec<u32>> = HashMap::new();
            for (gi, f) in found.iter().enumerate() {
                let need = ks[gi] - from_mem[gi];
                for &(c, e, o) in f.iter().take(need) {
                    dead.entry((c, e)).or_default().push(o);
                }
            }
            let mut tombs_new: Vec<RowsTomb> = Vec::with_capacity(dead.len());
            let mut keys: Vec<(u64, u32)> = dead.keys().copied().collect();
            keys.sort_unstable();
            let seq = inner.tseq.checked_add(1).ok_or_else(|| corrupt("tombstone sequence numbers are exhausted"))?;
            for key in keys {
                let mut ords = dead.remove(&key).unwrap_or_default();
                ords.sort_unstable();
                ords.dedup();
                tombs_new.push(RowsTomb { seq, chunk: key.0, entry: key.1, runs: runs_of(&ords) });
            }
            let in_chunks: u64 = tombs_new.iter().map(RowsTomb::rows).sum();
            let total = mem_rows.len() as u64 + in_chunks;
            if total == 0 {
                return Ok(0);
            }
            let refs: Vec<&[Value]> = mem_rows.iter().map(Vec::as_slice).collect();
            let entry = wal::rows_delete_entry(seq, &refs, &tombs_new);
            if entry.len() > wal::MAX_DELETE_ENTRY {
                return Err(Error::Unsupported(format!(
                    "a DELETE piece of {} bytes exceeds the {} byte log entry limit",
                    entry.len(),
                    wal::MAX_DELETE_ENTRY
                )));
            }
            log_delete(inner, &entry, durable)?;

            // Committed. Apply.
            inner.changed();
            if !mem_rows.is_empty() {
                let mut left = ks.clone();
                let removed = inner.memtable.remove_where(&self.schema, lo, hi, |r| {
                    let ts = self.schema.row_ts(&r.row);
                    take_from_mem(&mut left, r, ts)
                });
                debug_assert_eq!(removed, mem_rows.len());
                inner.mem_generation += 1;
            }
            inner.tseq = seq;
            let set = Arc::make_mut(&mut inner.tombs);
            for t in tombs_new {
                set.add_rows(t);
            }
            for (gi, s) in sids.iter().enumerate() {
                if let Some(s) = s {
                    if ks[gi] > 0 {
                        inner.uniq.invalidate(*s);
                    }
                }
            }
            return Ok(total);
        }
        Err(Error::Busy("the table kept changing while a DELETE looked for its rows".into()))
    }

    /// Up to `k` rows of series `sid` at `ts` that equal `row` in the chunks:
    /// `(chunk id, series entry, ordinal within the entry)`.
    #[allow(clippy::too_many_arguments)]
    fn locate(
        &self,
        chunks: &[Arc<ChunkMeta>],
        tombs: &TombSet,
        sid: Option<u64>,
        ts: i64,
        row: &Row,
        k: usize,
        want: &[bool],
    ) -> Result<Vec<(u64, u32, u32)>> {
        let mut out = Vec::new();
        let Some(sid) = sid else { return Ok(out) };
        let ids: HashSet<u64> = std::iter::once(sid).collect();
        for c in chunks {
            if out.len() >= k {
                break;
            }
            if !c.overlaps(ts, ts) || !c.bloom.may_contain(sid) || tombs.range_hides(sid, c.born, ts) {
                continue;
            }
            let entries: Vec<usize> = match c.entries_of(&ids) {
                Some(v) => v,
                None => (0..c.series.len()).filter(|&i| c.series[i].series_id == sid).collect(),
            };
            for idx in entries {
                let e = &c.series[idx];
                if e.ts_min > ts || e.ts_max < ts {
                    continue;
                }
                let Some(win) = load_window(&self.schema, c, idx, (ts, ts), want, false)? else { continue };
                let holes = tombs.holes(c.id, idx);
                for i in win.lo..win.hi {
                    if out.len() >= k {
                        break;
                    }
                    if in_runs(holes, i) {
                        continue;
                    }
                    let equal = self.schema.data_indices().iter().all(|&col| {
                        let stored = win.cols.get(col).and_then(Option::as_ref).and_then(|d| d.value_at(i));
                        matches!((&stored, row.get(col)), (Some(a), Some(b)) if same_value(a, b))
                    });
                    if equal {
                        out.push((c.id, idx as u32, i as u32));
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Appends a DELETE entry and syncs it; on failure nothing was applied.
fn log_delete(inner: &mut Inner, entry: &[u8], durable: bool) -> Result<()> {
    if let Err(e) = inner.wal.append(entry) {
        if !matches!(e, Error::InvalidArg(_)) {
            inner.poison(format!("WAL append failed: {e}"));
        }
        return Err(e);
    }
    if let Err(e) = inner.wal.sync(durable) {
        inner.poison(format!("WAL sync failed: {e}"));
        return Err(e);
    }
    Ok(())
}

/// After a delete the "extras at the highest timestamp" of a series may name
/// rows that are gone: forget them (the bound `max_ts` stays valid).
fn forget_unique(uniq: &mut UniqueState, sel: &Sel) {
    match sel {
        Sel::All => uniq.invalidate_all(),
        Sel::Ids(ids) => {
            for id in ids {
                uniq.invalidate(*id);
            }
        }
    }
}

/// Everything recovery needs to re-apply a DELETE entry.
pub(crate) struct Replay<'a> {
    pub schema: &'a Schema,
    pub chunks: &'a [Arc<ChunkMeta>],
    pub tombs: &'a mut TombSet,
    pub tseq: &'a mut u64,
    /// `tseq` of the MANIFEST: entries up to it are already in `tombs`.
    pub manifest_tseq: u64,
    pub series: &'a SeriesIndex,
    pub memtable: &'a mut MemTable,
    pub uniq: &'a mut UniqueState,
}

impl Replay<'_> {
    pub(crate) fn apply(&mut self, payload: &[u8]) -> Result<()> {
        let schema = self.schema;
        match wal::decode_delete(payload)? {
            DeleteEntry::Range { seq, lo, hi, series } => {
                let sel = match series {
                    None => Sel::All,
                    Some(list) => {
                        let mut ids = Vec::with_capacity(list.len());
                        for tags in &list {
                            if tags.len() != schema.tag_indices().len() {
                                return Err(corrupt("DELETE entry with tags that do not match the schema"));
                            }
                            if let Some(id) = self.series.lookup(tags) {
                                ids.push(id);
                            }
                        }
                        Sel::from_ids(ids)
                    }
                };
                self.memtable.remove_where(schema, lo, hi, |r| sel.contains(r.series_id));
                if seq > self.manifest_tseq {
                    let hits = count_hits(schema, self.chunks, self.tombs, &sel, lo, hi)?;
                    if !hits.is_empty() {
                        self.tombs.add_range(RangeTomb { seq, lo, hi, sel: sel.clone(), hits });
                    }
                }
                *self.tseq = (*self.tseq).max(seq);
                forget_unique(self.uniq, &sel);
            }
            DeleteEntry::Rows { seq, mem, hits } => {
                for r in &mem {
                    schema.validate_row(r).map_err(|e| corrupt(format!("DELETE entry holds a bad row: {e}")))?;
                }
                if !mem.is_empty() {
                    let mut left: HashMap<Vec<u8>, usize> = HashMap::new();
                    let (mut lo, mut hi) = (i64::MAX, i64::MIN);
                    for r in &mem {
                        *left.entry(encoded(r)).or_default() += 1;
                        let ts = schema.row_ts(r);
                        lo = lo.min(ts);
                        hi = hi.max(ts);
                    }
                    self.memtable.remove_where(schema, lo, hi, |r| match left.get_mut(&encoded(&r.row)) {
                        Some(n) if *n > 0 => {
                            *n -= 1;
                            true
                        }
                        _ => false,
                    });
                    for r in &mem {
                        if let Some(id) = self.series.lookup(&row_tags(schema, r)) {
                            self.uniq.invalidate(id);
                        }
                    }
                }
                if seq > self.manifest_tseq {
                    for h in hits {
                        let live = self.chunks.iter().find(|c| c.id == h.chunk);
                        // A chunk recovery had to set aside takes its tombstones with it.
                        if let Some(c) = live {
                            if let Some(e) = c.series.get(h.entry as usize) {
                                if h.runs.iter().all(|&(s, n)| u64::from(s) + u64::from(n) <= u64::from(e.row_count)) {
                                    self.uniq.invalidate(e.series_id);
                                    self.tombs.add_rows(h);
                                    continue;
                                }
                            }
                            return Err(corrupt(format!(
                                "DELETE entry names rows outside entry {} of chunk {}",
                                h.entry, h.chunk
                            )));
                        }
                    }
                }
                *self.tseq = (*self.tseq).max(seq);
            }
        }
        Ok(())
    }
}

/// Bytes of rows a [`DeleteBatch`] holds before it hands them to the table.
const BATCH_FLUSH_BYTES: usize = 32 << 20;

/// The rows one `DELETE` statement removes one by one (a `WHERE` the engine
/// cannot turn into a range): collected here, matched against the table and
/// deleted when the statement ends, in atomic pieces of about
/// [`PIECE_BYTES`]. A statement that deletes more than [`BATCH_FLUSH_BYTES`]
/// of rows applies them in several steps, each durable on its own.
pub struct DeleteBatch {
    table: Arc<Table>,
    durable: bool,
    rows: Vec<Row>,
    bytes: usize,
    deleted: u64,
}

impl Table {
    /// Starts collecting rows to delete (see [`DeleteBatch`]).
    pub fn begin_delete(self: &Arc<Self>, durable: bool) -> DeleteBatch {
        DeleteBatch { table: self.clone(), durable, rows: Vec::new(), bytes: 0, deleted: 0 }
    }
}

impl DeleteBatch {
    pub fn schema(&self) -> &Schema {
        &self.table.schema
    }

    /// Adds a row of the table to delete. Rows can be told apart only by their
    /// content, so the caller passes the row as it read it.
    pub fn add(&mut self, row: Row) -> Result<()> {
        self.table.schema.validate_row(&row)?;
        self.bytes += row.iter().map(Value::mem_size).sum::<usize>() + std::mem::size_of::<Row>();
        self.rows.push(row);
        if self.bytes >= BATCH_FLUSH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let rows = std::mem::take(&mut self.rows);
        self.bytes = 0;
        self.deleted += self.table.delete_rows(rows, self.durable)?;
        Ok(())
    }

    /// Deletes what was collected; returns the rows deleted in all (those a
    /// concurrent delete had already taken are not counted).
    pub fn commit(mut self) -> Result<u64> {
        if !self.rows.is_empty() {
            self.flush()?;
        }
        Ok(self.deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsutil::{fail_fsync_after, restore_fsync};
    use crate::options::{RawOptions, TableConfig};
    use crate::scan::ScanFilter;
    use crate::schema::{Column, ColumnType};

    fn open(dir: &std::path::Path) -> (Arc<Table>, TableConfig) {
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
            ],
            0,
        )
        .unwrap();
        let cfg = TableConfig::new(dir, schema, &RawOptions::default()).unwrap();
        Table::create(&cfg).unwrap();
        (Arc::new(Table::open(cfg.clone()).unwrap()), cfg)
    }

    fn count(t: &Table) -> usize {
        let mut s = t.scan(&ScanFilter::default(), false).unwrap();
        let mut n = 0;
        while s.next_row().unwrap().is_some() {
            n += 1;
        }
        n
    }

    #[test]
    fn a_failed_fsync_of_the_delete_entry_changes_nothing_in_memory_and_poisons_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let (t, cfg) = open(&path);
        for i in 0..10 {
            t.write(vec![Value::Timestamp(i), Value::Bytes(b"a".to_vec())]).unwrap();
        }
        t.flush().unwrap();
        t.write(vec![Value::Timestamp(50), Value::Bytes(b"a".to_vec())]).unwrap();
        t.sync_wal(true).unwrap();
        fail_fsync_after(0);
        let r = t.delete_range(None, 0, 100, None, true);
        restore_fsync();
        assert!(r.is_err());
        // memory is as before (the delete was never applied) and writes are refused
        assert_eq!(count(&t), 11);
        assert!(matches!(t.write(vec![Value::Timestamp(1), Value::Bytes(b"a".to_vec())]), Err(Error::ReadOnly(_))));
        assert!(matches!(t.delete_range(None, 0, 100, None, true), Err(Error::ReadOnly(_))));
        drop(t);
        // after recovery the delete either took place whole or not at all
        let t = Table::open(cfg).unwrap();
        let n = count(&t);
        assert!(n == 11 || n == 0, "{n}");
        assert_eq!(t.stats().unwrap().row_count as usize, n);
    }
}
