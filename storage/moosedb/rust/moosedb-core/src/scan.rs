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
use crate::chunk_reader::{fetch_row, load_more_columns, load_window, Window};
use crate::error::{invalid, Error, Result};
use crate::memtable::MemSnapshot;
use crate::rowbatch::{Plan, ScanBatch, DEFAULT_BATCH_ROWS, MAX_BATCH_ROWS};
use crate::schema::{Row, Schema, Value};
use crate::tomb::{split_by_holes, TombSet};
use crate::zone::{ColPred, Pruner};

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
    /// Value predicates on data columns, ANDed. Unlike the fields above they
    /// are only a **skip hint**: series entries (chunk blocks) whose zone map
    /// or column Bloom proves no row can match are not read; rows of the
    /// entries that are read come back whether they match or not, and the
    /// MemTable is not pruned. See [`ColPred`] for what the caller may push.
    pub cols: Vec<ColPred>,
}

impl Default for ScanFilter {
    fn default() -> Self {
        ScanFilter { ts_min: i64::MIN, ts_max: i64::MAX, tags: Vec::new(), series: None, cols: Vec::new() }
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
    /// Zone-map / column-Bloom pruning of entries (see `zone`).
    pub prune: Pruner,
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

    /// Entry `si` of chunk `c` passed the time and series tests; whether the
    /// value summaries still allow a matching row.
    pub(crate) fn entry_wanted(&self, c: &ChunkMeta, si: usize) -> bool {
        !self.prune.entry_pruned(c, si)
    }

    pub(crate) fn chunk_wanted(&self, c: &ChunkMeta) -> bool {
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
    /// Deletes in force for the chunks above (see `tomb`).
    pub(crate) tombs: Arc<TombSet>,
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
    /// End (exclusive) of the stretch being returned.
    end: usize,
    /// Later stretches of the window, last first (a window cut by dead rows).
    rest: Vec<(usize, usize)>,
}

struct StreamState {
    next_chunk: usize,
    next_series: usize,
    cur: Option<ChunkCursor>,
    mem_next: u64,
    /// The entry being visited, window by window (see [`Visible`]).
    todo: Option<Visible>,
}

impl StreamState {
    fn new() -> StreamState {
        StreamState {
            next_chunk: 0,
            next_series: 0,
            cur: None,
            mem_next: 0,
            todo: None,
        }
    }
}

/// What of one chunk entry is visible in a wanted timestamp range: the range
/// minus the range tombstones (one interval each), and within each interval
/// the rows the row tombstones leave. The single place that applies deletes to
/// chunk reads; scans and aggregations both go through it.
/// A decoded window and its live stretches.
pub(crate) type Loaded = (Window, Vec<(usize, usize)>);

pub(crate) struct Visible {
    chunk: usize,
    entry: usize,
    /// Intervals still to be read, last first.
    ivs: Vec<(i64, i64)>,
    /// The single interval of an entry with no tombstones around: no allocation.
    first: Option<(i64, i64)>,
    holes: Vec<(u32, u32)>,
}

impl Visible {
    pub(crate) fn plan(snap: &Snapshot, chunk: usize, entry: usize, lo: i64, hi: i64) -> Visible {
        if snap.tombs.is_empty() {
            return Visible { chunk, entry, ivs: Vec::new(), first: (lo <= hi).then_some((lo, hi)), holes: Vec::new() };
        }
        let meta = &snap.chunks[chunk];
        let p = snap.tombs.plan(meta, entry, meta.series[entry].series_id, lo, hi);
        let mut ivs = p.ivs;
        ivs.reverse();
        Visible { chunk, entry, ivs, first: None, holes: p.holes }
    }

    /// The next decoded window and its live stretches `[a, b)` (ascending,
    /// never empty), or `None` when the entry is done.
    pub(crate) fn next(&mut self, snap: &Snapshot, want: &[bool]) -> Result<Option<Loaded>> {
        while let Some(iv) = self.first.take().or_else(|| self.ivs.pop()) {
            let meta = &snap.chunks[self.chunk];
            if let Some(win) = load_window(&snap.schema, meta, self.entry, iv, want, false)? {
                let stretches = split_by_holes(win.lo, win.hi, &self.holes);
                if !stretches.is_empty() {
                    return Ok(Some((win, stretches)));
                }
            }
        }
        Ok(None)
    }
}

/// Calls `f(window, a, b)` for every live stretch (rows `[a, b)` of the
/// window) of entry `entry` of chunk `chunk` with timestamp in `[lo, hi]`.
pub(crate) fn visible_stretches(
    snap: &Snapshot,
    chunk: usize,
    entry: usize,
    (lo, hi): (i64, i64),
    want: &[bool],
    mut f: impl FnMut(&Window, usize, usize) -> Result<()>,
) -> Result<()> {
    let mut v = Visible::plan(snap, chunk, entry, lo, hi);
    while let Some((win, stretches)) = v.next(snap, want)? {
        for (a, b) in stretches {
            f(&win, a, b)?;
        }
    }
    Ok(())
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
    Chunk {
        chunk: usize,
        series: usize,
        loaded: bool,
        cur: Option<RunCursor>,
        /// Timestamp interval of the entry this run reads.
        range: (i64, i64),
        /// Dead ordinals of the entry (row tombstones), as sorted runs.
        holes: Vec<(u32, u32)>,
    },
    /// MemTable rows, already in scan order.
    Mem { keys: Vec<Key>, next: usize },
}

/// Cursors over the stretches of `win` that the dead ordinals `holes` leave.
fn cursors_of(win: Window, holes: &[(u32, u32)]) -> Vec<RunCursor> {
    if holes.is_empty() {
        let (front, back) = (win.lo, win.hi);
        return vec![RunCursor { win, front, back }];
    }
    split_by_holes(win.lo, win.hi, holes).into_iter().map(|(front, back)| RunCursor { win: win.clone(), front, back }).collect()
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
    /// Grouped scans (see [`Scan::grouped`]): the series of each group, and
    /// the group the merge reads. Empty for other scans.
    groups: Vec<HashSet<u64>>,
    group: usize,
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
        // Tie-ordered scans need the tie columns only for rows of equal timestamp
        // (decoded then, per run: see `merge_fill_tie`), not for every row.
        let tie: Vec<usize> = if sorted { tie.to_vec() } else { Vec::new() };
        let plan = Plan::new(&snap.schema, projection);
        let mut scan = Scan {
            snap,
            filter,
            mode: Mode::Stream(StreamState::new()),
            plan,
            tie,
            batch: None,
            groups: Vec::new(),
            group: 0,
        };
        if sorted {
            scan.mode = Mode::Merge(scan.new_merge(false));
        }
        Ok(scan)
    }

    /// A forward, timestamp-ordered scan that returns the rows of
    /// `groups[0]`'s series, then those of `groups[1]`, and so on: one ordered
    /// read per group (an ordered walk over the tag tuples of an index) on one
    /// snapshot. A group's runs are only set up once the previous group is
    /// read out. Backward reads go through the materialized fallback;
    /// `seek_end` is unsupported.
    pub(crate) fn grouped(
        snap: Arc<Snapshot>,
        mut filter: ResolvedFilter,
        groups: Vec<HashSet<u64>>,
        projection: Option<&[bool]>,
        tie: &[usize],
    ) -> Result<Scan> {
        filter.series = Some(groups.first().cloned().unwrap_or_default());
        let mut scan = Scan::new(snap, filter, true, projection, tie)?;
        scan.groups = groups;
        Ok(scan)
    }

    /// Grouped scans: starts the merge of the next group once the current one
    /// is read out (ascending only). `false` when there is none.
    fn next_group(&mut self) -> bool {
        let last = match &self.mode {
            Mode::Merge(m) if !m.desc && m.heap.is_empty() && m.pending.is_empty() => m.last,
            _ => return false,
        };
        let Some(next) = self.groups.get(self.group + 1) else { return false };
        self.group += 1;
        self.filter.series = Some(next.clone());
        let mut m = self.new_merge(false);
        m.last = last;
        self.mode = Mode::Merge(m);
        true
    }

    /// Back to the merge of the first group (grouped scans).
    fn first_group(&mut self) {
        if let Some(g) = self.groups.first() {
            self.group = 0;
            self.filter.series = Some(g.clone());
        }
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
            Mode::Merge(m) if m.desc == backward => {
                self.merge_step(batch, max)?;
                while batch.rows() < max && self.next_group() {
                    self.merge_step(batch, max)?;
                }
                Ok(())
            }
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
            Mode::Stream(StreamState::new())
        } else {
            self.first_group();
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
        if !self.groups.is_empty() {
            return Err(Error::Unsupported("seek_end on a grouped scan".into()));
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
                let want = (cur.end - cur.next).min(max - batch.rows());
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
                if cur.next >= cur.end {
                    match cur.rest.pop() {
                        Some((a, b)) => {
                            cur.next = a;
                            cur.end = b;
                        }
                        None => st.cur = None,
                    }
                }
                if k < want {
                    return Ok(());
                }
                continue;
            }
            // Windows of the entry being visited (several when tombstones cut its range).
            if let Some(v) = &mut st.todo {
                match v.next(snap, &plan.want)? {
                    Some((win, mut stretches)) => {
                        stretches.reverse();
                        if let Some((x, y)) = stretches.pop() {
                            let (chunk, series) = (v.chunk, v.entry);
                            st.cur = Some(ChunkCursor { chunk, series, next: x, end: y, rest: stretches, win });
                        }
                    }
                    None => st.todo = None,
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
                if entry.ts_min <= f.ts_max
                    && entry.ts_max >= f.ts_min
                    && f.series_matches(entry.series_id)
                    && f.entry_wanted(meta, idx)
                {
                    st.todo = Some(Visible::plan(snap, st.next_chunk, idx, f.ts_min, f.ts_max));
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
                    if e.ts_min > f.ts_max || e.ts_max < f.ts_min || !f.series_matches(e.series_id) || !f.entry_wanted(c, si) {
                        continue;
                    }
                    let base = c.series_offsets[si];
                    // A bound that sorts no later than the run's real first row.
                    let bound = |lo: i64, hi: i64| {
                        if desc {
                            (e.ts_max.min(hi), c.id, (base + u64::from(e.row_count)).saturating_sub(1))
                        } else {
                            (e.ts_min.max(lo), c.id, base)
                        }
                    };
                    if self.snap.tombs.is_empty() {
                        heap.push(HeapEntry { key: bound(f.ts_min, f.ts_max), run: runs.len(), desc });
                        runs.push(Run::Chunk {
                            chunk: ci,
                            series: si,
                            loaded: false,
                            cur: None,
                            range: (f.ts_min, f.ts_max),
                            holes: Vec::new(),
                        });
                        continue;
                    }
                    // Tombstones cut the wanted range into the intervals still
                    // alive: one run each (a run is one sorted stretch of the entry).
                    let p = self.snap.tombs.plan(c, si, e.series_id, f.ts_min, f.ts_max);
                    for (a, b) in p.ivs {
                        heap.push(HeapEntry { key: bound(a, b), run: runs.len(), desc });
                        runs.push(Run::Chunk {
                            chunk: ci,
                            series: si,
                            loaded: false,
                            cur: None,
                            range: (a, b),
                            holes: p.holes.clone(),
                        });
                    }
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
                    .rows_of_series(s, schema.ts_index())
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
        let Scan { snap, mode, plan, .. } = self;
        let Mode::Merge(m) = mode else { return Ok(()) };
        let desc = m.desc;
        while batch.rows() < max {
            let Some(top) = m.heap.pop() else { break };
            let Some(run) = m.runs.get_mut(top.run) else { continue };
            match run {
                Run::Chunk { chunk, series, loaded, cur, range, holes } => {
                    let meta = &snap.chunks[*chunk];
                    let base = meta.series_offsets[*series];
                    // Key of the row the cursor returns next.
                    let next_key = |c: &RunCursor| -> Result<Key> {
                        let i = if desc { c.back - 1 } else { c.front };
                        Ok((c.win.ts_at(i)?, meta.id, base + i as u64))
                    };
                    if !*loaded {
                        *loaded = true;
                        if let Some(win) = load_window(&snap.schema, meta, *series, *range, &plan.want, true)? {
                            let mut stretches = cursors_of(win, holes).into_iter();
                            if let Some(c) = stretches.next() {
                                m.heap.push(HeapEntry { key: next_key(&c)?, run: top.run, desc });
                                *cur = Some(c);
                            }
                            // Dead rows split the window: each further stretch is a run of its own.
                            let (ci, si, r) = (*chunk, *series, *range);
                            for c in stretches {
                                m.heap.push(HeapEntry { key: next_key(&c)?, run: m.runs.len(), desc });
                                m.runs.push(Run::Chunk {
                                    chunk: ci,
                                    series: si,
                                    loaded: true,
                                    cur: Some(c),
                                    range: r,
                                    holes: Vec::new(),
                                });
                            }
                        }
                        continue;
                    }
                    let Some(c) = cur else { continue };
                    let i = if desc { c.back - 1 } else { c.front };
                    let entry = &meta.series[*series];
                    let key = run_key(*chunk, *series);
                    // Ascending: every row of this run that sorts before the best
                    // row of the other runs leaves in one go (a run is sorted, and
                    // the keys of different rows never tie).
                    let mut n = 1;
                    if !desc {
                        if let Some(ts) = c.win.ts_slice() {
                            debug_assert!(c.back <= ts.len(), "the window holds the timestamps of rows [0, hi)");
                            let rest = ts.get(i..c.back).unwrap_or(&[]);
                            n = match m.heap.peek() {
                                None => rest.len(),
                                Some(best) => {
                                    let bound = best.key;
                                    let (mut lo, mut hi) = (0usize, rest.len());
                                    while lo < hi {
                                        let mid = lo + (hi - lo) / 2;
                                        if (rest[mid], meta.id, base + (i + mid) as u64) < bound {
                                            lo = mid + 1;
                                        } else {
                                            hi = mid;
                                        }
                                    }
                                    lo
                                }
                            }
                            .clamp(1, max - batch.rows());
                        }
                    }
                    let n = batch.fit_window(plan, &c.win, i, n);
                    if !batch.tags_room(plan, key, &entry.tags) || n == 0 {
                        m.heap.push(top);
                        break;
                    }
                    batch.push_window(plan, &c.win, &entry.tags, key, meta.id, base, i, n);
                    let last_i = if desc { i } else { i + n - 1 };
                    m.last = Some((c.win.ts_at(last_i)?, meta.id, base + last_i as u64));
                    if desc {
                        c.back -= 1;
                    } else {
                        c.front += n;
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
        let Scan { snap, mode, plan, tie, .. } = self;
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

            // Fast path (ascending): rows of the run on top whose timestamp is
            // below everything the other runs hold and differs from the next row
            // of the run are single-row groups: no tie to break, no group to build.
            let mut first = heap.pop();
            if !desc {
                if let Some(top_run) = first.as_ref().map(|h| h.run) {
                    if let Some(Run::Chunk { chunk, series, loaded: true, cur: Some(c), .. }) = runs.get_mut(top_run) {
                        let meta = &snap.chunks[*chunk];
                        let base = meta.series_offsets[*series];
                        let entry = &meta.series[*series];
                        let key = run_key(*chunk, *series);
                        if let Some(ts) = c.win.ts_slice() {
                            debug_assert!(c.back <= ts.len(), "the window holds the timestamps of rows [0, hi)");
                            let rest = ts.get(c.front..c.back).unwrap_or(&[]);
                            let below = match heap.peek() {
                                None => rest.len(),
                                Some(best) => rest.partition_point(|&t| t < best.key.0),
                            };
                            let mut n = below.min(max - batch.rows());
                            // A row followed by one of the same timestamp starts a group.
                            if let Some(pos) = rest[..(n + 1).min(rest.len())].windows(2).position(|w| w[0] == w[1]) {
                                n = pos;
                            }
                            let n = if n > 0 && batch.tags_room(plan, key, &entry.tags) {
                                batch.fit_window(plan, &c.win, c.front, n)
                            } else {
                                0
                            };
                            if n > 0 {
                                batch.push_window(plan, &c.win, &entry.tags, key, meta.id, base, c.front, n);
                                let l = c.front + n - 1;
                                *last = Some((ts[l], meta.id, base + l as u64));
                                c.front += n;
                                if c.front < c.back {
                                    let j = c.front;
                                    heap.push(HeapEntry { key: (ts[j], meta.id, base + j as u64), run: top_run, desc });
                                }
                                first = None;
                                continue;
                            }
                        }
                    }
                }
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
                    None => first.take().or_else(|| heap.pop()),
                };
                let Some(top) = entry else { break };
                let Some(run) = runs.get_mut(top.run) else { continue };
                match run {
                    Run::Chunk { chunk, series, loaded, cur, range, holes } => {
                        let meta = &snap.chunks[*chunk];
                        let base = meta.series_offsets[*series];
                        if !*loaded {
                            *loaded = true;
                            if let Some(win) = load_window(&snap.schema, meta, *series, *range, &plan.want, true)? {
                                let first_key = |c: &RunCursor| -> Result<Key> {
                                    let i = if desc { c.back - 1 } else { c.front };
                                    Ok((c.win.ts_at(i)?, meta.id, base + i as u64))
                                };
                                let mut stretches = cursors_of(win, holes).into_iter();
                                if let Some(c) = stretches.next() {
                                    heap.push(HeapEntry { key: first_key(&c)?, run: top.run, desc });
                                    *cur = Some(c);
                                }
                                let (ci, si, r) = (*chunk, *series, *range);
                                for c in stretches {
                                    heap.push(HeapEntry { key: first_key(&c)?, run: runs.len(), desc });
                                    runs.push(Run::Chunk {
                                        chunk: ci,
                                        series: si,
                                        loaded: true,
                                        cur: Some(c),
                                        range: r,
                                        holes: Vec::new(),
                                    });
                                }
                            }
                            continue;
                        }
                        let Some(c) = cur else { continue };
                        load_more_columns(&snap.schema, meta, *series, &mut c.win, tie)?;
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
        let (group, series) = (self.group, self.filter.series.clone());
        self.first_group();
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
                (self.group, self.filter.series) = (group, series);
                return Err(e);
            }
            if one.rows() == 0 && self.next_group() {
                continue;
            }
            let key = match &self.mode {
                Mode::Merge(m) if one.rows() > 0 => m.last,
                _ => None,
            };
            let Some(key) = key else { break };
            rows.push((key, one.row_at(0)));
        }
        let cursor = match last {
            // Tie-ordered (or grouped) rows are not sorted by key alone: find the row itself.
            Some(k) if !self.tie.is_empty() || !self.groups.is_empty() => rows.iter().position(|(rk, _)| *rk == k).map_or(-1, |i| i as isize),
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

    #[test]
    fn a_grouped_scan_returns_group_after_group_in_time_order() {
        let schema = Schema::new(
            vec![Column { name: "ts".into(), ty: ColumnType::Timestamp }, Column { name: "host".into(), ty: ColumnType::Tag }],
            0,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cfg = TableConfig::new(dir.path().join("t"), schema, &RawOptions::default()).unwrap();
        Table::create(&cfg).unwrap();
        let t = Table::open(cfg).unwrap();
        // ts i belongs to host a/b/c by i % 3; half in a chunk, half in the MemTable
        let write = |i: i64| {
            let h = [b"a", b"b", b"c"][(i % 3) as usize];
            t.write(vec![Value::Timestamp(i), Value::Bytes(h.to_vec())]).unwrap();
        };
        (0..30).for_each(write);
        t.flush().unwrap();
        (30..60).for_each(write);
        let id = |h: &[u8]| t.series().unwrap().into_iter().find(|(_, v)| v[0] == Value::Bytes(h.to_vec())).unwrap().0;
        let groups = vec![vec![id(b"c")], vec![], vec![id(b"a"), id(b"b")]];
        let ts_of = |r: &Row| match r[0] {
            Value::Timestamp(v) => v,
            _ => unreachable!(),
        };
        let mut want: Vec<i64> = (10..60).filter(|i| i % 3 == 2).collect();
        want.extend((10..60).filter(|i| i % 3 != 2));
        let mut s = t.scan_groups(10, i64::MAX, &groups, None).unwrap();
        let mut got = Vec::new();
        loop {
            let b = s.next_batch(7, false).unwrap();
            if b.rows() == 0 {
                break;
            }
            got.extend((0..b.rows()).map(|i| ts_of(&b.row_at(i))));
        }
        assert_eq!(got, want);
        // a change of direction continues from the row returned last
        s.seek_start().unwrap();
        for w in &want[..20] {
            assert_eq!(ts_of(&s.next_row().unwrap().unwrap().1), *w);
        }
        assert_eq!(ts_of(&s.prev_row().unwrap().unwrap().1), want[18]);
        assert_eq!(ts_of(&s.next_row().unwrap().unwrap().1), want[19]);
        assert!(s.seek_end().is_err());
    }

    #[test]
    fn a_grouped_scan_keeps_unique_ties_inside_each_group() {
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "ns".into(), ty: ColumnType::Int64 },
            ],
            0,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cfg = TableConfig::new(dir.path().join("t"), schema, &RawOptions::default()).unwrap().with_unique(vec![2]).unwrap();
        Table::create(&cfg).unwrap();
        let t = Arc::new(Table::open(cfg).unwrap());
        // 3 rows per timestamp, ns written in descending order; part flushed
        for (i, h) in [(0, b"a"), (1, b"b"), (2, b"c")] {
            for ts in 0..6 {
                for ns in (0..3).rev() {
                    t.write_shared(vec![Value::Timestamp(ts), Value::Bytes(h.to_vec()), Value::Int(ns + 10 * i)]).unwrap();
                }
                if ts == 2 {
                    t.flush().unwrap();
                }
            }
        }
        let id = |h: &[u8]| t.series().unwrap().into_iter().find(|(_, v)| v[0] == Value::Bytes(h.to_vec())).unwrap().0;
        let groups = vec![vec![id(b"b")], vec![id(b"a"), id(b"c")]];
        let key = |r: &Row| match (&r[0], &r[2]) {
            (Value::Timestamp(ts), Value::Int(ns)) => (*ts, *ns),
            _ => unreachable!(),
        };
        let mut want: Vec<(i64, i64)> = (0..6).flat_map(|ts| (10..13).map(move |ns| (ts, ns))).collect();
        want.extend((0..6).flat_map(|ts| (0..3).chain(20..23).map(move |ns| (ts, ns))));
        want[18..].sort();
        let mut s = t.scan_groups(i64::MIN, i64::MAX, &groups, None).unwrap();
        let mut got = Vec::new();
        loop {
            let b = s.next_batch(4, false).unwrap();
            if b.rows() == 0 {
                break;
            }
            got.extend((0..b.rows()).map(|i| key(&b.row_at(i))));
        }
        assert_eq!(got, want);
        // back and forth across the border of the two groups
        s.seek_start().unwrap();
        for w in &want[..19] {
            assert_eq!(key(&s.next_row().unwrap().unwrap().1), *w);
        }
        assert_eq!(key(&s.prev_row().unwrap().unwrap().1), want[17]);
        assert_eq!(key(&s.next_row().unwrap().unwrap().1), want[18]);
        assert_eq!(key(&s.next_row().unwrap().unwrap().1), want[19]);
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
