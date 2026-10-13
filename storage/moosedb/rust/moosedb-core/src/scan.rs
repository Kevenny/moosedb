//! Table scans over immutable snapshots.
//!
//! A [`Snapshot`] is the list of live chunks plus a frozen view of the
//! MemTable. Chunk files are deleted only when the last snapshot referencing
//! them is dropped, and MemTable segments are shared, so a snapshot can read
//! every row it covers for as long as it lives — this is what makes row
//! positions (`rnd_pos`) safe while other sessions write, flush or compact.
//!
//! Scan modes:
//! * **stream** — chunk by chunk, series by series, then MemTable rows.
//!   Forward only; holds one decoded series at a time.
//! * **merge** — timestamp-ordered k-way merge of every (chunk, series) run
//!   and the sorted MemTable rows, ascending or descending. A run is decoded
//!   only when it reaches the top of the heap (its metadata bounds stand in
//!   until then), so `ORDER BY ts DESC LIMIT n` touches only the newest data.
//! * **materialized** — fallback when a merge changes direction mid-scan
//!   (`index_next` after `index_prev`): the range is collected and sorted.
//!
//! Rows are totally ordered by `(ts, source, ordinal)`.
//!
//! Reads are columnar: a scan decodes only the projected columns, only the
//! rows of each series inside the timestamp range (series are sorted by
//! timestamp, so the range is a binary search), and hands rows out in
//! [`ScanBatch`]es of typed, reused buffers (`Scan::next_batch`).
//! `next_row`/`prev_row` are the one-row form of the same path.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::sync::Arc;

use crate::chunk::ChunkMeta;
use crate::chunk_reader::{fetch_row, load_window, Window};
use crate::error::{invalid, Error, Result};
use crate::memtable::MemSnapshot;
use crate::rowbatch::{Plan, ScanBatch, DEFAULT_BATCH_ROWS, MAX_BATCH_ROWS};
use crate::schema::{Row, Schema, Value};

pub const POSITION_LEN: usize = 16;
/// High bit of `Position::source` marks MemTable rows; the remaining bits
/// carry the MemTable generation the ordinal belongs to.
pub(crate) const MEM_SOURCE: u64 = 1 << 63;

/// Stable reference to a row within a snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    /// Chunk id, or `MEM_SOURCE | generation` for MemTable rows.
    pub source: u64,
    /// Row number within the source.
    pub ordinal: u64,
}

impl Position {
    pub fn to_bytes(self) -> [u8; POSITION_LEN] {
        let mut b = [0u8; POSITION_LEN];
        b[..8].copy_from_slice(&self.source.to_le_bytes());
        b[8..].copy_from_slice(&self.ordinal.to_le_bytes());
        b
    }

    pub fn from_bytes(b: &[u8; POSITION_LEN]) -> Position {
        let mut s = [0u8; 8];
        let mut o = [0u8; 8];
        s.copy_from_slice(&b[..8]);
        o.copy_from_slice(&b[8..]);
        Position { source: u64::from_le_bytes(s), ordinal: u64::from_le_bytes(o) }
    }
}

/// Total order key of a row.
type Key = (i64, u64, u64);

fn key_of(ts: i64, p: Position) -> Key {
    (ts, p.source, p.ordinal)
}

fn pos_of(k: Key) -> Position {
    Position { source: k.1, ordinal: k.2 }
}

/// Predicate pushed down to the storage layer. Rows outside it are never
/// returned; the SQL layer still re-evaluates its full WHERE clause.
#[derive(Clone, Debug)]
pub struct ScanFilter {
    /// Inclusive timestamp bounds.
    pub ts_min: i64,
    pub ts_max: i64,
    /// `(schema column index, value)` equality predicates on TAG columns.
    pub tags: Vec<(usize, Value)>,
    /// Restrict to these series ids (from `Table::series`), if set.
    pub series: Option<Vec<u64>>,
}

impl Default for ScanFilter {
    fn default() -> Self {
        ScanFilter { ts_min: i64::MIN, ts_max: i64::MAX, tags: Vec::new(), series: None }
    }
}

impl ScanFilter {
    pub fn range(ts_min: i64, ts_max: i64) -> ScanFilter {
        ScanFilter { ts_min, ts_max, ..ScanFilter::default() }
    }
}

/// `ScanFilter` resolved against the series index: tag predicates become a
/// set of series ids.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedFilter {
    pub ts_min: i64,
    pub ts_max: i64,
    pub series: Option<HashSet<u64>>,
}

/// Bloom filters are consulted when at most this many series are wanted.
const BLOOM_PROBE_LIMIT: usize = 64;

impl ResolvedFilter {
    pub(crate) fn is_empty(&self) -> bool {
        self.ts_min > self.ts_max || self.series.as_ref().is_some_and(HashSet::is_empty)
    }

    pub(crate) fn ts_matches(&self, ts: i64) -> bool {
        self.ts_min <= ts && ts <= self.ts_max
    }

    pub(crate) fn series_matches(&self, id: u64) -> bool {
        self.series.as_ref().map_or(true, |s| s.contains(&id))
    }

    fn chunk_wanted(&self, c: &ChunkMeta) -> bool {
        if !c.overlaps(self.ts_min, self.ts_max) {
            return false;
        }
        match &self.series {
            Some(s) if s.len() <= BLOOM_PROBE_LIMIT => s.iter().any(|&id| c.bloom.may_contain(id)),
            _ => true,
        }
    }
}

/// Immutable view of a table: live chunks and frozen MemTable.
pub struct Snapshot {
    pub(crate) schema: Arc<Schema>,
    pub(crate) chunks: Vec<Arc<ChunkMeta>>,
    pub(crate) mem: MemSnapshot,
    pub(crate) version: u64,
}

impl Snapshot {
    /// Changes whenever the table's contents change.
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Re-reads the row at `pos`; `NotFound` if this snapshot does not cover it.
    pub fn fetch(&self, pos: Position) -> Result<Row> {
        let missing = || Error::NotFound("row position is not covered by this snapshot".into());
        if pos.source & MEM_SOURCE != 0 {
            if pos.source & !MEM_SOURCE != self.mem.generation {
                return Err(missing());
            }
            return self.mem.get(pos.ordinal).map(|m| m.row.clone()).ok_or_else(missing);
        }
        let meta = self
            .chunks
            .binary_search_by_key(&pos.source, |c| c.id)
            .ok()
            .and_then(|i| self.chunks.get(i))
            .ok_or_else(missing)?;
        let idx = meta.series_for_ordinal(pos.ordinal).ok_or_else(missing)?;
        // Not `decode_series(..).nth(..)`: that decodes the whole series per
        // fetch (O(series) per `rnd_pos`); this goes through the series cache.
        let within = usize::try_from(pos.ordinal - meta.series_offsets[idx]).map_err(|_| missing())?;
        fetch_row(&self.schema, meta, idx, within)?.ok_or_else(missing)
    }
}

// ─── stream mode ────────────────────────────────────────────────────────────

/// The decoded window of the series the stream is reading.
struct ChunkCursor {
    chunk: usize,
    series: usize,
    win: Window,
    /// Next row (within the series) to return.
    next: usize,
}

struct StreamState {
    next_chunk: usize,
    next_series: usize,
    cur: Option<ChunkCursor>,
    mem_next: u64,
}

// ─── merge mode ─────────────────────────────────────────────────────────────

/// Rows `[front, back)` of a decoded chunk series still to be returned.
struct RunCursor {
    win: Window,
    front: usize,
    back: usize,
}

enum Run {
    /// A series of a chunk; `cur` is filled when the run first reaches the top.
    Chunk { chunk: usize, series: usize, loaded: bool, cur: Option<RunCursor> },
    /// MemTable rows, already in scan order.
    Mem { keys: Vec<Key>, next: usize },
}

struct HeapEntry {
    key: Key,
    run: usize,
    desc: bool,
}

impl PartialEq for HeapEntry {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HeapEntry {
    /// BinaryHeap is a max-heap: ascending merges invert the order.
    fn cmp(&self, o: &Self) -> Ordering {
        let c = self.key.cmp(&o.key).then(self.run.cmp(&o.run));
        if self.desc {
            c
        } else {
            c.reverse()
        }
    }
}

struct MergeState {
    desc: bool,
    runs: Vec<Run>,
    heap: BinaryHeap<HeapEntry>,
    /// Key of the row returned last.
    last: Option<Key>,
    /// Rows of one timestamp already put in order (UNIQUE tables, see `Scan::tie`)
    /// that did not fit the batch yet.
    pending: VecDeque<Pend>,
}

/// A row of a timestamp group waiting for its turn (tie-ordered scans).
struct Pend {
    /// Values of the tie-break columns (the extra columns of the UNIQUE key).
    extras: [i64; 2],
    key: Key,
    src: PendSrc,
}

enum PendSrc {
    /// Row `i` of the decoded series of run `run`.
    Chunk { run: usize, i: usize },
    /// MemTable row `key.2`.
    Mem,
}

enum Mode {
    Stream(StreamState),
    Merge(MergeState),
    Materialized { rows: Vec<(Key, Row)>, cursor: isize },
}

/// Identifies one (chunk, series) run inside a scan, for TAG sharing in a batch.
fn run_key(chunk: usize, series: usize) -> u64 {
    ((chunk as u64) << 32) | series as u64
}

pub struct Scan {
    snap: Arc<Snapshot>,
    filter: ResolvedFilter,
    mode: Mode,
    plan: Plan,
    /// Columns that order rows of equal timestamp in sorted scans (the extra
    /// columns of a UNIQUE key; empty = no tie-break, arrival order).
    tie: Vec<usize>,
    /// Output buffers, reused across calls.
    batch: Option<ScanBatch>,
}

impl Scan {
    pub(crate) fn new(
        snap: Arc<Snapshot>,
        filter: ResolvedFilter,
        sorted: bool,
        projection: Option<&[bool]>,
        tie: &[usize],
    ) -> Result<Scan> {
        if let Some(m) = projection {
            if m.len() != snap.schema.columns().len() {
                return Err(invalid(format!(
                    "projection has {} flags, table has {} columns",
                    m.len(),
                    snap.schema.columns().len()
                )));
            }
        }
        // Tie-ordered scans must decode the tie columns even when not asked for.
        let tie: Vec<usize> = if sorted { tie.to_vec() } else { Vec::new() };
        let forced: Option<Vec<bool>> = (!tie.is_empty()).then(|| {
            let mut m: Vec<bool> = projection.map_or_else(|| vec![true; snap.schema.columns().len()], <[bool]>::to_vec);
            for &c in &tie {
                if let Some(w) = m.get_mut(c) {
                    *w = true;
                }
            }
            m
        });
        let plan = Plan::new(&snap.schema, forced.as_deref().or(projection));
        let mut scan = Scan {
            snap,
            filter,
            mode: Mode::Stream(StreamState { next_chunk: 0, next_series: 0, cur: None, mem_next: 0 }),
            plan,
            tie,
            batch: None,
        };
        if sorted {
            scan.mode = Mode::Merge(scan.new_merge(false));
        }
        Ok(scan)
    }

    pub fn snapshot(&self) -> &Arc<Snapshot> {
        &self.snap
    }

    #[cfg(test)]
    fn set_bytes_cap(&mut self, cap: usize) {
        let batch = self.batch.get_or_insert_with(|| ScanBatch::new(&self.snap.schema));
        batch.set_bytes_cap(cap);
    }

    pub fn is_sorted(&self) -> bool {
        !matches!(self.mode, Mode::Stream(_))
    }

    /// Next row in scan order, or `None` at the end. Columns outside the
    /// projection read as NULL.
    pub fn next_row(&mut self) -> Result<Option<(Position, Row)>> {
        self.step_row(false)
    }

    /// Previous row (sorted scans only).
    pub fn prev_row(&mut self) -> Result<Option<(Position, Row)>> {
        self.step_row(true)
    }

    fn step_row(&mut self, backward: bool) -> Result<Option<(Position, Row)>> {
        let b = self.next_batch(1, backward)?;
        Ok((b.rows() > 0).then(|| (Position::from_bytes(&b.positions()[0]), b.row_at(0))))
    }

    /// Returns up to `max_rows` (at most [`MAX_BATCH_ROWS`]) following rows,
    /// or the preceding ones with `backward` (sorted scans only), in columns.
    /// An empty batch means the end. The batch is owned by the scan and valid
    /// until the next call on it; the scan's cursor ends after the batch's
    /// last row, so mixing directions across batches continues from there.
    pub fn next_batch(&mut self, max_rows: usize, backward: bool) -> Result<&ScanBatch> {
        let max = if max_rows == 0 { DEFAULT_BATCH_ROWS } else { max_rows.min(MAX_BATCH_ROWS) };
        let mut batch = self.batch.take().unwrap_or_else(|| ScanBatch::new(&self.snap.schema));
        batch.begin(&self.plan.want);
        let res = self.fill(&mut batch, max, backward);
        if res.is_err() {
            batch.begin(&self.plan.want);
        }
        let out = self.batch.insert(batch);
        res?;
        Ok(out)
    }

    fn fill(&mut self, batch: &mut ScanBatch, max: usize, backward: bool) -> Result<()> {
        match &self.mode {
            Mode::Stream(_) if backward => Err(Error::Unsupported("backward iteration on an unsorted scan".into())),
            Mode::Stream(_) => self.stream_fill(batch, max),
            Mode::Merge(m) if m.desc == backward => self.merge_step(batch, max),
            Mode::Merge(_) => {
                self.materialize()?;
                self.materialized_fill(batch, max, backward)
            }
            Mode::Materialized { .. } => self.materialized_fill(batch, max, backward),
        }
    }

    /// Rewinds the scan to before its first row, on the SAME snapshot (rows
    /// written since the scan opened stay invisible). Works on sorted and
    /// unsorted scans; `next_row`/`next_batch` then return the first row.
    pub fn seek_start(&mut self) -> Result<()> {
        self.mode = if matches!(self.mode, Mode::Stream(_)) {
            Mode::Stream(StreamState { next_chunk: 0, next_series: 0, cur: None, mem_next: 0 })
        } else {
            Mode::Merge(self.new_merge(false))
        };
        Ok(())
    }

    /// Positions a sorted scan after its last row: `prev_row` then yields
    /// rows in descending order.
    pub fn seek_end(&mut self) -> Result<()> {
        if matches!(self.mode, Mode::Stream(_)) {
            return Err(Error::Unsupported("seek on an unsorted scan".into()));
        }
        self.mode = Mode::Merge(self.new_merge(true));
        Ok(())
    }

    // ── stream ──

    fn stream_fill(&mut self, batch: &mut ScanBatch, max: usize) -> Result<()> {
        let Scan { snap, filter: f, mode, plan, .. } = self;
        let Mode::Stream(st) = mode else { return Ok(()) };
        while batch.rows() < max {
            if let Some(cur) = &mut st.cur {
                let meta = &snap.chunks[cur.chunk];
                let entry = &meta.series[cur.series];
                let key = run_key(cur.chunk, cur.series);
                let want = (cur.win.hi - cur.next).min(max - batch.rows());
                if !batch.tags_room(plan, key, &entry.tags) {
                    return Ok(());
                }
                let k = batch.fit_window(plan, &cur.win, cur.next, want);
                if k == 0 {
                    return Ok(());
                }
                batch.push_window(
                    plan,
                    &cur.win,
                    &entry.tags,
                    key,
                    meta.id,
                    meta.series_offsets[cur.series],
                    cur.next,
                    k,
                );
                cur.next += k;
                if cur.next >= cur.win.hi {
                    st.cur = None;
                }
                if k < want {
                    return Ok(());
                }
                continue;
            }
            if let Some(meta) = snap.chunks.get(st.next_chunk) {
                if st.next_series == 0 && !f.chunk_wanted(meta) {
                    st.next_chunk += 1;
                    continue;
                }
                let Some(entry) = meta.series.get(st.next_series) else {
                    st.next_chunk += 1;
                    st.next_series = 0;
                    continue;
                };
                let idx = st.next_series;
                st.next_series += 1;
                if entry.ts_min <= f.ts_max && entry.ts_max >= f.ts_min && f.series_matches(entry.series_id) {
                    if let Some(win) = load_window(&snap.schema, meta, idx, (f.ts_min, f.ts_max), &plan.want, false)? {
                        st.cur = Some(ChunkCursor { chunk: st.next_chunk, series: idx, next: win.lo, win });
                    }
                }
                continue;
            }
            let source = MEM_SOURCE | snap.mem.generation;
            while batch.rows() < max {
                let Some(m) = snap.mem.get(st.mem_next) else { return Ok(()) };
                if f.series_matches(m.series_id) && f.ts_matches(snap.schema.row_ts(&m.row)) {
                    if !batch.row_room(plan, &m.row) {
                        return Ok(());
                    }
                    batch.push_row(plan, &m.row, Position { source, ordinal: st.mem_next }.to_bytes());
                }
                st.mem_next += 1;
            }
            return Ok(());
        }
        Ok(())
    }

    // ── merge ──

    fn new_merge(&self, desc: bool) -> MergeState {
        let f = &self.filter;
        let mut runs = Vec::new();
        let mut heap = BinaryHeap::new();
        if !f.is_empty() {
            for (ci, c) in self.snap.chunks.iter().enumerate() {
                if !f.chunk_wanted(c) {
                    continue;
                }
                // A few wanted series in a chunk with many: look them up instead
                // of testing every entry (the cost of opening one lookup scan).
                let picked = f.series.as_ref().filter(|s| s.len().saturating_mul(16) < c.series.len());
                let picked = picked.and_then(|s| c.entries_of(s));
                let visit: Box<dyn Iterator<Item = usize> + '_> = match &picked {
                    Some(v) => Box::new(v.iter().copied()),
                    None => Box::new(0..c.series.len()),
                };
                for si in visit {
                    let Some(e) = c.series.get(si) else { continue };
                    if e.ts_min > f.ts_max || e.ts_max < f.ts_min || !f.series_matches(e.series_id) {
                        continue;
                    }
                    let base = c.series_offsets[si];
                    // A bound that sorts no later than the run's real first row.
                    let key = if desc {
                        (e.ts_max.min(f.ts_max), c.id, (base + u64::from(e.row_count)).saturating_sub(1))
                    } else {
                        (e.ts_min.max(f.ts_min), c.id, base)
                    };
                    heap.push(HeapEntry { key, run: runs.len(), desc });
                    runs.push(Run::Chunk { chunk: ci, series: si, loaded: false, cur: None });
                }
            }
            let source = MEM_SOURCE | self.snap.mem.generation;
            let schema = &self.snap.schema;
            let mem = &self.snap.mem;
            // Few wanted series in a large MemTable: per-segment series index
            // (built once per segment, shared by all scans) instead of a pass
            // over every row.
            let indexed = f
                .series
                .as_ref()
                .filter(|s| s.len().saturating_mul(mem.segment_count().max(1)).saturating_mul(4) <= mem.row_total());
            let mut keys: Vec<Key> = match indexed {
                Some(s) => mem
                    .rows_of_series(s)
                    .into_iter()
                    .filter(|(_, m)| f.ts_matches(schema.row_ts(&m.row)))
                    .map(|(o, m)| key_of(schema.row_ts(&m.row), Position { source, ordinal: o }))
                    .collect(),
                None => mem
                    .iter()
                    .filter(|(_, m)| f.series_matches(m.series_id) && f.ts_matches(schema.row_ts(&m.row)))
                    .map(|(o, m)| key_of(schema.row_ts(&m.row), Position { source, ordinal: o }))
                    .collect(),
            };
            keys.sort_unstable();
            if desc {
                keys.reverse();
            }
            if let Some(k) = keys.first() {
                heap.push(HeapEntry { key: *k, run: runs.len(), desc });
                runs.push(Run::Mem { keys, next: 0 });
            }
        }
        MergeState { desc, runs, heap, last: None, pending: VecDeque::new() }
    }

    fn merge_fill(&mut self, batch: &mut ScanBatch, max: usize) -> Result<()> {
        let Scan { snap, filter: f, mode, plan, .. } = self;
        let Mode::Merge(m) = mode else { return Ok(()) };
        let desc = m.desc;
        while batch.rows() < max {
            let Some(top) = m.heap.pop() else { break };
            let Some(run) = m.runs.get_mut(top.run) else { continue };
            match run {
                Run::Chunk { chunk, series, loaded, cur } => {
                    let meta = &snap.chunks[*chunk];
                    let base = meta.series_offsets[*series];
                    // Key of the row the cursor returns next.
                    let next_key = |c: &RunCursor| -> Result<Key> {
                        let i = if desc { c.back - 1 } else { c.front };
                        Ok((c.win.ts_at(i)?, meta.id, base + i as u64))
                    };
                    if !*loaded {
                        *loaded = true;
                        let range = (f.ts_min, f.ts_max);
                        if let Some(win) = load_window(&snap.schema, meta, *series, range, &plan.want, true)? {
                            let c = RunCursor { front: win.lo, back: win.hi, win };
                            m.heap.push(HeapEntry { key: next_key(&c)?, run: top.run, desc });
                            *cur = Some(c);
                        }
                        continue;
                    }
                    let Some(c) = cur else { continue };
                    let i = if desc { c.back - 1 } else { c.front };
                    let entry = &meta.series[*series];
                    let key = run_key(*chunk, *series);
                    if !batch.tags_room(plan, key, &entry.tags) || batch.fit_window(plan, &c.win, i, 1) == 0 {
                        m.heap.push(top);
                        break;
                    }
                    batch.push_window(plan, &c.win, &entry.tags, key, meta.id, base, i, 1);
                    m.last = Some((c.win.ts_at(i)?, meta.id, base + i as u64));
                    if desc {
                        c.back -= 1;
                    } else {
                        c.front += 1;
                    }
                    if c.front < c.back {
                        m.heap.push(HeapEntry { key: next_key(c)?, run: top.run, desc });
                    }
                }
                Run::Mem { keys, next } => {
                    let Some(&key) = keys.get(*next) else { continue };
                    if let Some(mr) = snap.mem.get(key.2) {
                        if !batch.row_room(plan, &mr.row) {
                            m.heap.push(top);
                            break;
                        }
                        batch.push_row(plan, &mr.row, pos_of(key).to_bytes());
                        m.last = Some(key);
                    }
                    *next += 1;
                    if let Some(&k) = keys.get(*next) {
                        m.heap.push(HeapEntry { key: k, run: top.run, desc });
                    }
                }
            }
        }
        Ok(())
    }

    fn merge_step(&mut self, batch: &mut ScanBatch, max: usize) -> Result<()> {
        if self.tie.is_empty() {
            self.merge_fill(batch, max)
        } else {
            self.merge_fill_tie(batch, max)
        }
    }

    /// [`Scan::merge_fill`] for tables with a UNIQUE key: rows of the same
    /// timestamp leave ordered by the key's extra columns (`Scan::tie`), then
    /// by position, ascending or descending like the timestamps. Stored series
    /// are only sorted by timestamp, so the rows of one timestamp are collected
    /// from every run first (a timestamp group is small: the key is unique),
    /// put in order and handed out from `pending`, which survives the end of a
    /// batch.
    fn merge_fill_tie(&mut self, batch: &mut ScanBatch, max: usize) -> Result<()> {
        let Scan { snap, filter: f, mode, plan, tie, .. } = self;
        let Mode::Merge(m) = mode else { return Ok(()) };
        let MergeState { desc, runs, heap, last, pending } = m;
        let desc = *desc;
        let tie_of_row = |row: &[Value]| -> [i64; 2] {
            let mut x = [i64::MIN; 2];
            for (slot, &c) in x.iter_mut().zip(tie.iter()) {
                if let Some(Value::Int(v)) = row.get(c) {
                    *slot = *v;
                }
            }
            x
        };
        while batch.rows() < max {
            if let Some(p) = pending.front() {
                match p.src {
                    PendSrc::Mem => {
                        if let Some(mr) = snap.mem.get(p.key.2) {
                            if !batch.row_room(plan, &mr.row) {
                                break;
                            }
                            batch.push_row(plan, &mr.row, pos_of(p.key).to_bytes());
                            *last = Some(p.key);
                        }
                    }
                    PendSrc::Chunk { run, i } => {
                        if let Some(Run::Chunk { chunk, series, cur: Some(c), .. }) = runs.get(run) {
                            let meta = &snap.chunks[*chunk];
                            let entry = &meta.series[*series];
                            let key = run_key(*chunk, *series);
                            if !batch.tags_room(plan, key, &entry.tags) || batch.fit_window(plan, &c.win, i, 1) == 0 {
                                break;
                            }
                            batch.push_window(
                                plan,
                                &c.win,
                                &entry.tags,
                                key,
                                meta.id,
                                meta.series_offsets[*series],
                                i,
                                1,
                            );
                            *last = Some(p.key);
                        }
                    }
                }
                pending.pop_front();
                continue;
            }

            // Collect every row of the next timestamp.
            let mut group: Vec<Pend> = Vec::new();
            let mut t0: Option<i64> = None;
            loop {
                let entry = match t0 {
                    Some(t) => match heap.peek() {
                        Some(h) if h.key.0 == t => heap.pop(),
                        _ => None,
                    },
                    None => heap.pop(),
                };
                let Some(top) = entry else { break };
                let Some(run) = runs.get_mut(top.run) else { continue };
                match run {
                    Run::Chunk { chunk, series, loaded, cur } => {
                        let meta = &snap.chunks[*chunk];
                        let base = meta.series_offsets[*series];
                        if !*loaded {
                            *loaded = true;
                            let range = (f.ts_min, f.ts_max);
                            if let Some(win) = load_window(&snap.schema, meta, *series, range, &plan.want, true)? {
                                let c = RunCursor { front: win.lo, back: win.hi, win };
                                let i = if desc { c.back - 1 } else { c.front };
                                heap.push(HeapEntry {
                                    key: (c.win.ts_at(i)?, meta.id, base + i as u64),
                                    run: top.run,
                                    desc,
                                });
                                *cur = Some(c);
                            }
                            continue;
                        }
                        let Some(c) = cur else { continue };
                        let i = if desc { c.back - 1 } else { c.front };
                        let ts = c.win.ts_at(i)?;
                        let mut x = [i64::MIN; 2];
                        for (slot, &col) in x.iter_mut().zip(tie.iter()) {
                            let v = c
                                .win
                                .cols
                                .get(col)
                                .and_then(Option::as_ref)
                                .and_then(|d| d.ints())
                                .and_then(|v| v.get(i));
                            if let Some(v) = v {
                                *slot = *v;
                            }
                        }
                        if desc {
                            c.back -= 1;
                        } else {
                            c.front += 1;
                        }
                        if c.front < c.back {
                            let j = if desc { c.back - 1 } else { c.front };
                            heap.push(HeapEntry {
                                key: (c.win.ts_at(j)?, meta.id, base + j as u64),
                                run: top.run,
                                desc,
                            });
                        }
                        group.push(Pend {
                            extras: x,
                            key: (ts, meta.id, base + i as u64),
                            src: PendSrc::Chunk { run: top.run, i },
                        });
                        t0 = Some(ts);
                    }
                    Run::Mem { keys, next } => {
                        let Some(&key) = keys.get(*next) else { continue };
                        *next += 1;
                        if let Some(&k) = keys.get(*next) {
                            heap.push(HeapEntry { key: k, run: top.run, desc });
                        }
                        if let Some(mr) = snap.mem.get(key.2) {
                            group.push(Pend { extras: tie_of_row(&mr.row), key, src: PendSrc::Mem });
                            t0 = Some(key.0);
                        }
                    }
                }
            }
            if group.is_empty() {
                break;
            }
            group.sort_by_key(|a| (a.extras, a.key.1, a.key.2));
            if desc {
                group.reverse();
            }
            pending.extend(group);
        }
        Ok(())
    }

    // ── materialized ──

    /// Switches a merge to materialized mode, keeping the cursor on the row
    /// returned last.
    fn materialize(&mut self) -> Result<()> {
        let Mode::Merge(m) = &self.mode else { return Ok(()) };
        let (desc, last) = (m.desc, m.last);
        let fresh = self.new_merge(false);
        // Work on a fresh merge but keep the old mode until the rows are all
        // collected: an error leaves the scan as it was.
        let old = std::mem::replace(&mut self.mode, Mode::Merge(fresh));
        let mut one = ScanBatch::new(&self.snap.schema);
        let mut rows = Vec::new();
        loop {
            one.begin(&self.plan.want);
            if let Err(e) = self.merge_step(&mut one, 1) {
                self.mode = old;
                return Err(e);
            }
            let key = match &self.mode {
                Mode::Merge(m) if one.rows() > 0 => m.last,
                _ => None,
            };
            let Some(key) = key else { break };
            rows.push((key, one.row_at(0)));
        }
        let cursor = match last {
            // Tie-ordered rows are not sorted by key alone: find the row itself.
            Some(k) if !self.tie.is_empty() => rows.iter().position(|(rk, _)| *rk == k).map_or(-1, |i| i as isize),
            Some(k) => rows.binary_search_by_key(&k, |(rk, _)| *rk).map_or_else(|i| i as isize, |i| i as isize),
            None if desc => rows.len() as isize,
            None => -1,
        };
        self.mode = Mode::Materialized { rows, cursor };
        Ok(())
    }

    fn materialized_fill(&mut self, batch: &mut ScanBatch, max: usize, backward: bool) -> Result<()> {
        let Scan { mode, plan, .. } = self;
        let Mode::Materialized { rows, cursor } = mode else { return Ok(()) };
        let step: isize = if backward { -1 } else { 1 };
        while batch.rows() < max {
            let len = rows.len() as isize;
            let next = (*cursor + step).clamp(-1, len);
            let Some((k, r)) = usize::try_from(next).ok().and_then(|i| rows.get(i)) else {
                // Past either end: park there only if nothing was returned, so a
                // batch that ends at the boundary leaves the cursor on its last row.
                if batch.rows() == 0 {
                    *cursor = next;
                }
                break;
            };
            if !batch.row_room(plan, r) {
                break;
            }
            *cursor = next;
            batch.push_row(plan, r, pos_of(*k).to_bytes());
        }
        Ok(())
    }
}

pub(crate) fn check_tag_columns(schema: &Schema, f: &ScanFilter) -> Result<()> {
    for (col, _) in &f.tags {
        if schema.tag_position(*col).is_none() {
            return Err(invalid(format!("column {col} is not a TAG column")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_roundtrip() {
        let p = Position { source: MEM_SOURCE | 3, ordinal: 99 };
        assert_eq!(Position::from_bytes(&p.to_bytes()), p);
    }

    #[test]
    fn heap_order() {
        let mut h = BinaryHeap::new();
        for (i, ts) in [5, 1, 3].iter().enumerate() {
            h.push(HeapEntry { key: (*ts, 0, 0), run: i, desc: false });
        }
        assert_eq!(h.pop().unwrap().key.0, 1);
        let mut h = BinaryHeap::new();
        for (i, ts) in [5, 1, 3].iter().enumerate() {
            h.push(HeapEntry { key: (*ts, 0, 0), run: i, desc: true });
        }
        assert_eq!(h.pop().unwrap().key.0, 5);
    }
}

#[cfg(test)]
mod batch_cap_tests {
    use super::*;
    use crate::options::RawOptions;
    use crate::schema::{Column, ColumnType};
    use crate::{Table, TableConfig};

    #[test]
    fn a_batch_ends_before_its_variable_width_bytes_pass_the_cap() {
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "note".into(), ty: ColumnType::Varchar },
            ],
            0,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cfg = TableConfig::new(dir.path().join("t"), schema, &RawOptions::default()).unwrap();
        Table::create(&cfg).unwrap();
        let t = Table::open(cfg).unwrap();
        let write = |i: i64| {
            let note = vec![b'x'; 1000];
            t.write(vec![Value::Timestamp(i), Value::Bytes(b"h".to_vec()), Value::Bytes(note)]).unwrap();
        };
        (0..60).for_each(write);
        t.flush().unwrap();
        (60..100).for_each(write);
        for sorted in [false, true] {
            let mut s = t.scan(&ScanFilter::default(), sorted).unwrap();
            s.set_bytes_cap(10_000);
            let (mut total, mut sizes) = (0, Vec::new());
            loop {
                let rows = s.next_batch(1000, false).unwrap().rows();
                if rows == 0 {
                    break;
                }
                total += rows;
                sizes.push(rows);
            }
            assert_eq!(total, 100, "sorted {sorted}");
            assert!(sizes.iter().all(|&r| r <= 10), "{sizes:?}");
            // A value larger than the cap still travels, one row at a time.
            let mut s = t.scan(&ScanFilter::default(), sorted).unwrap();
            s.set_bytes_cap(10);
            assert_eq!(s.next_batch(1000, false).unwrap().rows(), 1);
        }
    }
}

#[cfg(test)]
mod selective_scan_tests {
    use super::*;
    use crate::options::RawOptions;
    use crate::schema::{Column, ColumnType};
    use crate::{Table, TableConfig};

    fn table(dir: &std::path::Path) -> Table {
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "v".into(), ty: ColumnType::Int64 },
            ],
            0,
        )
        .unwrap();
        let cfg = TableConfig::new(dir.join("t"), schema, &RawOptions::default()).unwrap();
        Table::create(&cfg).unwrap();
        Table::open(cfg).unwrap()
    }

    fn rows(s: &mut Scan, backward: bool) -> Vec<(i64, i64)> {
        let mut out = Vec::new();
        loop {
            let b = s.next_batch(100, backward).unwrap();
            if b.rows() == 0 {
                return out;
            }
            for i in 0..b.rows() {
                let r = b.row_at(i);
                let (Value::Timestamp(ts), Value::Int(v)) = (&r[0], &r[2]) else { panic!("{r:?}") };
                out.push((*ts, *v));
            }
        }
    }

    /// Looking up a few series of many through the per-chunk and per-segment
    /// indexes returns exactly what testing every entry / every row returns.
    #[test]
    fn a_few_wanted_series_match_a_full_pass_over_chunks_and_memtable() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(dir.path());
        let write = |round: i64| {
            for h in 0..400i64 {
                for k in 0..6i64 {
                    let ts = round * 1000 + k * 7 + h % 5;
                    let row = vec![
                        Value::Timestamp(ts),
                        Value::Bytes(format!("h{h}").into_bytes()),
                        Value::Int(h * 100 + ts),
                    ];
                    t.write(row).unwrap();
                }
            }
        };
        write(0);
        t.flush().unwrap(); // sealed chunk with 400 series
        write(1); // MemTable, first segment
        let _ = t.snapshot().unwrap();
        write(2); // a second segment
        let ids: Vec<u64> = t.series().unwrap().into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids.len(), 400);
        let wanted = |sel: &[u64]| ScanFilter { series: Some(sel.to_vec()), ..ScanFilter::default() };
        for sel in [vec![ids[7]], vec![ids[0], ids[399]], vec![ids[3], ids[3], ids[250]], vec![u64::MAX]] {
            for (lo, hi) in [(i64::MIN, i64::MAX), (1000, 1999), (5, 5000)] {
                let mut f = wanted(&sel);
                f.ts_min = lo;
                f.ts_max = hi;
                // reference: the whole table, series filtered by the caller
                let mut all = t.scan(&ScanFilter::range(lo, hi), true).unwrap();
                let want: Vec<(i64, i64)> = {
                    let mut v = Vec::new();
                    while let Some((_, r)) = all.next_row().unwrap() {
                        let (Value::Timestamp(ts), Value::Bytes(h), Value::Int(x)) = (&r[0], &r[1], &r[2]) else {
                            panic!()
                        };
                        let name = String::from_utf8(h.clone()).unwrap();
                        let idx: usize = name[1..].parse().unwrap();
                        if sel.contains(&ids[idx]) {
                            v.push((*ts, *x));
                        }
                    }
                    v
                };
                let mut fwd = t.scan(&f, true).unwrap();
                assert_eq!(rows(&mut fwd, false), want, "forward {sel:?} {lo}..{hi}");
                let mut bwd = t.scan(&f, true).unwrap();
                bwd.seek_end().unwrap();
                let mut rev = want.clone();
                rev.reverse();
                assert_eq!(rows(&mut bwd, true), rev, "backward {sel:?} {lo}..{hi}");
            }
        }
    }
}
