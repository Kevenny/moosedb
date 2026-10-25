//! Aggregation inside the engine (hash aggregation over one snapshot).
//!
//! An [`Agg`] holds ONE [`Snapshot`] (the same cut a non-ordered scan gets: the
//! rows committed when it was opened; the private batch of the statement is not
//! part of it) and the `[lo, hi]` / series restriction. [`Agg::run`] reads it
//! with a plan of group keys, functions and filters and leaves a columnar
//! [`AggResult`]. Deletes are applied by [`crate::scan::visible_stretches`],
//! the same code the scans use.
//!
//! * **Keys**: the series (one state per series, a tight loop over the window)
//!   and/or common columns (Int64/Timestamp/Bool as integers, Varchar/Decimal
//!   as bytes). NULL is a value of its own. Keys come from the client, so the
//!   group tables hash them with SipHash-1-3 under a random per-run key
//!   (`RandomState`), never the fast internal hash. The table itself is an
//!   open-addressing index over a key arena (one allocation for all keys of a
//!   worker, the hash kept per group so a merge never rehashes).
//! * **Filters** come in two kinds. Dictionary masks: a run with the single
//!   key `Column(c)` and no filter or function records the distinct values of
//!   `c` in result order (the dictionary); the caller evaluates its predicate
//!   on each value and passes the accepted ones as a mask of that length.
//!   Value filters ([`ValueFilter`]): the exact set of accepted values (bytes
//!   or integers), no dictionary needed; NULL is never accepted. Several
//!   filters are ANDed. Values a mask's dictionary does not hold are rejected.
//! * **Parallelism**: `std::thread::scope`; units of ~[`UNIT_ROWS`] rows (chunk
//!   entries and slices of MemTable segments) are claimed through an atomic
//!   counter; every worker owns a group table. With more than one worker the
//!   groups are then merged **by hash partition** (a power of two of
//!   partitions, claimed by the workers again), each partition is turned into
//!   result columns by the thread that merged it, and the pieces are
//!   concatenated. A run over few rows stays on the calling thread. The result
//!   is the same for any number of threads (integers only).
//! * **Top-N** ([`TopN`]): with a single key, the K best groups by one
//!   function, selected per partition and then overall, so the caller does not
//!   receive or sort millions of groups.
//! * **Cancellation**: the caller's callback runs only on the calling thread
//!   (about every 10 ms); workers poll an `AtomicBool`.
//! * **Memory**: keys + states + overhead of every group table and of the
//!   result are charged to one shared budget (in batches, so workers do not
//!   contend on it); running over it is an error. The worker tables stay alive
//!   while the partitions are merged, so a parallel run peaks at about twice
//!   the memory of the groups the workers saw.

use std::cmp::Ordering as Cmp;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::panic::resume_unwind;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::block::{ColumnData, DenseRef};
use crate::chunk_reader::Window;
use crate::error::{corrupt, invalid, Error, Result};
use crate::memtable::MemRow;
use crate::scan::{visible_stretches, ResolvedFilter, Snapshot};
use crate::schema::{ColumnType, Value};
use crate::zone::{ColPred, Pruner};

pub const MAX_KEYS: usize = 16;
pub const MAX_FUNCS: usize = 64;
pub const MAX_FILTERS: usize = 16;
pub const MAX_THREADS: usize = 64;

/// Rows per work unit (a unit always holds whole series entries, at most
/// [`UNIT_ENTRIES`] of them).
const UNIT_ROWS: usize = 1 << 15;
const UNIT_ENTRIES: usize = 1024;
/// MemTable rows per work unit.
const UNIT_MEM_ROWS: usize = 1 << 16;
/// A run over fewer rows than this stays on the calling thread (see
/// [`Agg::set_min_parallel_rows`]): starting and joining threads costs more
/// than the work.
pub const DEFAULT_MIN_PARALLEL_ROWS: usize = 1 << 17;
/// Fewer groups than this are merged on the calling thread.
const MERGE_PARALLEL_GROUPS: usize = 1 << 13;
const POLL_EVERY: Duration = Duration::from_millis(10);
/// Rows of a stretch that no group takes.
const NONE: u32 = u32::MAX;
/// Bytes charged per group on top of its key and states (table slots, hash,
/// key end, vector slack).
const GROUP_OVERHEAD: usize = 80;
/// Partitions per worker thread (a power of two overall, at most `MAX_PARTS`).
const PARTS_PER_THREAD: usize = 4;
const MAX_PARTS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeySpec {
    Series,
    Column(usize),
    /// A VARCHAR column compared under a binary PAD SPACE collation of a
    /// charset whose space is the single byte 0x20 (utf8mb4, utf8mb3, latin1,
    /// ascii): values that differ only by trailing 0x20 bytes are ONE group; the
    /// key returned is the shortest value of the group (the smallest by bytes).
    /// A top-N then breaks ties in that collation's order (see [`pad_cmp`]).
    ColumnPad(usize),
    /// A TIMESTAMP column (microseconds since 1970, no time zone) cut in buckets of
    /// `width` microseconds (`width > 0`) aligned to the epoch: the key is the START of the
    /// bucket, `floor(v / width) * width` (floor, so instants before 1970 fall in the bucket
    /// that contains them). NULL is its own group.
    Bucket(usize, i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuncSpec {
    CountStar,
    /// Non-NULL values of a column.
    Count(usize),
    /// Integer sum in a checked 128-bit accumulator (overflow is an error).
    Sum(usize),
    /// Integer or timestamp minimum / maximum.
    Min(usize),
    Max(usize),
}

/// Accepted values of the dictionary of `col`, by dictionary position.
#[derive(Clone, Debug)]
pub struct ColFilter {
    pub col: usize,
    pub mask: Vec<bool>,
}

/// Accepted values of `col` by exact value: a row is kept iff its value (never
/// NULL) is in the set. `ints` is for INT64/TIMESTAMP/BOOL columns, `bytes`
/// for VARCHAR/DECIMAL ones (compared byte for byte: the caller may use this
/// only when the SQL comparison is exact, e.g. a binary NO PAD collation); the
/// other set must be empty. An empty set rejects every row.
#[derive(Clone, Debug, Default)]
pub struct ValueFilter {
    pub col: usize,
    pub ints: Vec<i64>,
    pub bytes: Vec<Vec<u8>>,
    /// PAD SPACE comparison for `bytes`: trailing 0x20 bytes are ignored on
    /// both sides (the set and the row), as a binary PAD SPACE collation of a
    /// charset whose space is the byte 0x20 (utf8mb4, utf8mb3, latin1, ascii)
    /// compares. Not for ucs2/utf16/utf32. NULL is still never accepted.
    pub pad_space: bool,
}

/// Exact test of a FLOAT64 / FLOAT32 column: a row is kept iff its value (never
/// NULL, never NaN) lies in the interval `lo..hi` (each end closed unless the
/// `_open` flag is set; `-inf`/`+inf` leave an end unbounded) and, when `points`
/// is `Some`, equals one of them (IEEE equality, so `-0.0 == 0.0`; an empty
/// list keeps nothing). A FLOAT32 value is widened to `f64` first, as the SQL
/// comparison does. The caller may use this only for a plain `FLOAT`/`DOUBLE`
/// (no `(M,D)`: those compare with an epsilon).
#[derive(Clone, Debug)]
pub struct FloatFilter {
    pub col: usize,
    pub lo: f64,
    pub hi: f64,
    pub lo_open: bool,
    pub hi_open: bool,
    pub points: Option<Vec<f64>>,
}

/// Keep only the `limit` best groups by function `func` (an index into
/// `funcs`): the result then holds at most `limit` groups, best first. Needs
/// exactly one key. Order: the function's value (`COUNT` as a number; `SUM`,
/// `MIN`, `MAX` as numbers, a group without values being NULL, the smallest
/// value, so first when ascending), `desc` reverses it; ties are broken by the
/// key, always ascending (series id, integer value, or bytes compared
/// lexicographically as unsigned, a shorter prefix first; a NULL key first).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopN {
    pub func: usize,
    pub desc: bool,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub struct AggSpec {
    pub keys: Vec<KeySpec>,
    pub funcs: Vec<FuncSpec>,
    pub filters: Vec<ColFilter>,
    pub value_filters: Vec<ValueFilter>,
    pub float_filters: Vec<FloatFilter>,
    /// Extra zone-map tests (hints, ANDed with the filters): entries that cannot
    /// hold a row inside them are not read. Never change the result as long as
    /// each predicate is implied by the caller's WHERE (NULL never matches);
    /// the exact evaluation stays in `filters`.
    pub preds: Vec<ColPred>,
    pub top: Option<TopN>,
    /// Budget in bytes (keys, states, overhead, result).
    pub max_memory: usize,
    /// Worker threads (1 = run on the calling thread).
    pub threads: usize,
}

impl Default for AggSpec {
    fn default() -> Self {
        AggSpec {
            keys: Vec::new(),
            funcs: Vec::new(),
            filters: Vec::new(),
            value_filters: Vec::new(),
            float_filters: Vec::new(),
            preds: Vec::new(),
            top: None,
            max_memory: 1 << 30,
            threads: 1,
        }
    }
}

// ─── result ──────────────────────────────────────────────────────────────────

/// One key column of the result, one entry per group.
#[derive(Clone, Debug, PartialEq)]
pub enum KeyCol {
    Series(Vec<u64>),
    Int { nulls: Vec<bool>, vals: Vec<i64> },
    Bytes { nulls: Vec<bool>, starts: Vec<u64>, lens: Vec<u32>, data: Vec<u8> },
}

impl KeyCol {
    fn len(&self) -> usize {
        match self {
            KeyCol::Series(v) => v.len(),
            KeyCol::Int { vals, .. } => vals.len(),
            KeyCol::Bytes { lens, .. } => lens.len(),
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            KeyCol::Series(v) => v.len() * 8,
            KeyCol::Int { vals, .. } => vals.len() * 9,
            KeyCol::Bytes { lens, data, .. } => lens.len() * 13 + data.len(),
        }
    }

    fn empty(k: &KeySpec, kind: Kind) -> KeyCol {
        match (k, kind) {
            (KeySpec::Series, _) => KeyCol::Series(Vec::new()),
            (_, Kind::Int) => KeyCol::Int { nulls: Vec::new(), vals: Vec::new() },
            (_, Kind::Bytes) => KeyCol::Bytes { nulls: Vec::new(), starts: Vec::new(), lens: Vec::new(), data: Vec::new() },
        }
    }

    /// Appends `o` (the same kind) after `self`.
    fn append(&mut self, o: KeyCol) -> Result<()> {
        let oom = || Error::Full("out of memory while joining the result".into());
        match (self, o) {
            (KeyCol::Series(a), KeyCol::Series(b)) => {
                a.try_reserve(b.len()).map_err(|_| oom())?;
                a.extend_from_slice(&b);
            }
            (KeyCol::Int { nulls, vals }, KeyCol::Int { nulls: n2, vals: v2 }) => {
                nulls.try_reserve(n2.len()).map_err(|_| oom())?;
                vals.try_reserve(v2.len()).map_err(|_| oom())?;
                nulls.extend_from_slice(&n2);
                vals.extend_from_slice(&v2);
            }
            (
                KeyCol::Bytes { nulls, starts, lens, data },
                KeyCol::Bytes { nulls: n2, starts: s2, lens: l2, data: d2 },
            ) => {
                let base = data.len() as u64;
                nulls.try_reserve(n2.len()).map_err(|_| oom())?;
                starts.try_reserve(s2.len()).map_err(|_| oom())?;
                lens.try_reserve(l2.len()).map_err(|_| oom())?;
                data.try_reserve(d2.len()).map_err(|_| oom())?;
                nulls.extend_from_slice(&n2);
                starts.extend(s2.iter().map(|s| s + base));
                lens.extend_from_slice(&l2);
                data.extend_from_slice(&d2);
            }
            _ => return Err(corrupt("key columns of different kinds")),
        }
        Ok(())
    }
}

/// One function of the result, one entry per group. `count` is the number of
/// rows (`CountStar`), of non-NULL values (`Count`) or of values that went into
/// `Sum`/`Min`/`Max` (0 = the SQL result is NULL). `sum_*` hold the 128-bit sum
/// (`lo` unsigned, `hi` signed); `val` the minimum or maximum. Only the
/// vectors the function uses are filled (`sum_*` for `Sum`, `val` for `Min` and
/// `Max`); the others are empty.
#[derive(Clone, Debug, Default)]
pub struct FuncCol {
    pub count: Vec<u64>,
    pub sum_lo: Vec<u64>,
    pub sum_hi: Vec<i64>,
    pub val: Vec<i64>,
}

#[derive(Clone, Debug, Default)]
pub struct AggResult {
    pub groups: usize,
    pub keys: Vec<KeyCol>,
    pub funcs: Vec<FuncCol>,
}

impl AggResult {
    /// Appends the groups of `o` (same plan) after these.
    fn append(&mut self, o: AggResult) -> Result<()> {
        let oom = || Error::Full("out of memory while joining the result".into());
        for (a, b) in self.keys.iter_mut().zip(o.keys) {
            a.append(b)?;
        }
        for (a, b) in self.funcs.iter_mut().zip(o.funcs) {
            a.count.try_reserve(b.count.len()).map_err(|_| oom())?;
            a.sum_lo.try_reserve(b.sum_lo.len()).map_err(|_| oom())?;
            a.sum_hi.try_reserve(b.sum_hi.len()).map_err(|_| oom())?;
            a.val.try_reserve(b.val.len()).map_err(|_| oom())?;
            a.count.extend_from_slice(&b.count);
            a.sum_lo.extend_from_slice(&b.sum_lo);
            a.sum_hi.extend_from_slice(&b.sum_hi);
            a.val.extend_from_slice(&b.val);
        }
        self.groups += o.groups;
        Ok(())
    }
}

// ─── handle ──────────────────────────────────────────────────────────────────

/// An aggregation over one snapshot (see the module docs). Not shared between
/// threads while `run` executes; the snapshot itself is.
pub struct Agg {
    snap: Arc<Snapshot>,
    filter: ResolvedFilter,
    dicts: HashMap<usize, KeyCol>,
    result: AggResult,
    min_parallel_rows: usize,
}

impl Agg {
    pub(crate) fn new(snap: Arc<Snapshot>, filter: ResolvedFilter) -> Agg {
        Agg {
            snap,
            filter,
            dicts: HashMap::new(),
            result: AggResult::default(),
            min_parallel_rows: DEFAULT_MIN_PARALLEL_ROWS,
        }
    }

    pub fn snapshot(&self) -> &Arc<Snapshot> {
        &self.snap
    }

    /// The result of the last successful `run` (empty before and after a failed one).
    pub fn result(&self) -> &AggResult {
        &self.result
    }

    /// Runs with fewer than `rows` estimated rows stay on the calling thread
    /// whatever `AggSpec::threads` says (default [`DEFAULT_MIN_PARALLEL_ROWS`];
    /// 0 always honours `threads`). The result does not depend on it.
    pub fn set_min_parallel_rows(&mut self, rows: usize) {
        self.min_parallel_rows = rows;
    }

    /// Runs the aggregation. `cancel` is called on this thread about every
    /// 10 ms; returning `true` aborts the run with [`Error::Cancelled`].
    /// A panic in a worker thread is re-raised here.
    pub fn run(&mut self, spec: &AggSpec, mut cancel: Option<&mut dyn FnMut() -> bool>) -> Result<()> {
        self.result = AggResult::default();
        let filters = self.compile_filters(spec)?;
        let mut plan = Plan::new(&self.snap, &self.filter, spec, filters)?;
        let units = plan.units();
        let wanted = spec.threads.clamp(1, MAX_THREADS);
        let threads = if units.est_rows < self.min_parallel_rows { 1 } else { wanted.min(units.list.len().max(1)) };
        plan.parts = if threads > 1 { (threads * PARTS_PER_THREAD).next_power_of_two().min(MAX_PARTS) } else { 1 };
        let next = AtomicUsize::new(0);
        let locals: Vec<Local> = if threads == 1 {
            vec![plan.work(&units, &next, reborrow(&mut cancel))?]
        } else {
            run_parallel(&plan, threads, reborrow(&mut cancel), &|poll| plan.work(&units, &next, poll))?
        };
        plan.check_stop()?;
        let result = plan.reduce(locals, threads, reborrow(&mut cancel))?;
        plan.check_stop()?;
        // The distinct values of a column, in result order, are its dictionary.
        let plain = spec.filters.is_empty() && spec.value_filters.is_empty() && spec.float_filters.is_empty();
        if let ([KeySpec::Column(c)], true, true, true) = (&spec.keys[..], plain, spec.funcs.is_empty(), spec.top.is_none()) {
            plan.charge(result.keys[0].heap_bytes())?;
            self.dicts.insert(*c, result.keys[0].clone());
        }
        self.result = result;
        Ok(())
    }

    fn compile_filters(&self, spec: &AggSpec) -> Result<Vec<(usize, FilterSet)>> {
        if spec.filters.len() + spec.value_filters.len() + spec.float_filters.len() > MAX_FILTERS {
            return Err(invalid(format!("at most {MAX_FILTERS} filters")));
        }
        let cols = self.snap.schema.columns();
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let mut count = |set: &FilterSet| -> Result<()> {
            bytes = bytes.saturating_add(set.bytes.iter().map(|b| b.len() + 48).sum::<usize>() + set.ints.len() * 16);
            if bytes > spec.max_memory {
                return Err(budget_error(spec.max_memory));
            }
            Ok(())
        };
        for f in &spec.filters {
            let dict = self
                .dicts
                .get(&f.col)
                .ok_or_else(|| invalid(format!("no dictionary for column {}: run it as the only key first", f.col)))?;
            if dict.len() != f.mask.len() {
                return Err(invalid(format!(
                    "filter mask of column {} has {} entries, its dictionary {}",
                    f.col,
                    f.mask.len(),
                    dict.len()
                )));
            }
            let mut set = FilterSet::default();
            match dict {
                KeyCol::Series(_) => return Err(invalid("a series dictionary cannot filter")),
                KeyCol::Int { nulls, vals } => {
                    for (i, _) in f.mask.iter().enumerate().filter(|(_, &m)| m) {
                        if nulls[i] {
                            set.null_ok = true;
                        } else {
                            set.ints.insert(vals[i]);
                        }
                    }
                }
                KeyCol::Bytes { nulls, starts, lens, data } => {
                    for (i, _) in f.mask.iter().enumerate().filter(|(_, &m)| m) {
                        if nulls[i] {
                            set.null_ok = true;
                        } else {
                            let s = starts[i] as usize;
                            set.bytes.insert(data[s..s + lens[i] as usize].into());
                        }
                    }
                }
            }
            set.seal();
            count(&set)?;
            out.push((f.col, set));
        }
        for f in &spec.value_filters {
            let ty = cols.get(f.col).ok_or_else(|| invalid(format!("column {} out of range", f.col)))?.ty;
            let mut set = FilterSet::default();
            match key_kind(ty) {
                Some(Kind::Int) if f.bytes.is_empty() => set.ints.extend(f.ints.iter().copied()),
                Some(Kind::Bytes) if f.ints.is_empty() => {
                    set.pad_space = f.pad_space;
                    set.bytes.extend(f.bytes.iter().map(|b| {
                        let b = if f.pad_space { trim_spaces(b) } else { b.as_slice() };
                        Box::<[u8]>::from(b)
                    }));
                }
                Some(_) => return Err(invalid(format!("value filter of column {} holds the wrong kind of values", f.col))),
                None => return Err(invalid(format!("column {} cannot be filtered", f.col))),
            }
            set.seal();
            count(&set)?;
            out.push((f.col, set));
        }
        for f in &spec.float_filters {
            let ty = cols.get(f.col).ok_or_else(|| invalid(format!("column {} out of range", f.col)))?.ty;
            if !matches!(ty, ColumnType::Float64 | ColumnType::Float32) {
                return Err(invalid(format!("float filter on column {}, which is {ty:?}", f.col)));
            }
            let mut points = f.points.clone();
            if f.lo.is_nan() || f.hi.is_nan() || points.iter().flatten().any(|p| !p.is_finite()) {
                return Err(invalid("float filter: NaN bound or non-finite point"));
            }
            if let Some(p) = &mut points {
                p.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Cmp::Equal));
            }
            let float = FloatSet { lo: f.lo, hi: f.hi, lo_open: f.lo_open, hi_open: f.hi_open, points };
            let set = FilterSet { float: Some(float), ..FilterSet::default() };
            count(&set)?;
            out.push((f.col, set));
        }
        Ok(out)
    }
}

fn budget_error(max: usize) -> Error {
    Error::Full(format!(
        "aggregation exceeds moosedb_agg_max_memory ({max} bytes); narrow the query or raise the limit"
    ))
}

// ─── plan ────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct FilterSet {
    ints: HashSet<i64>,
    bytes: HashSet<Box<[u8]>>,
    /// The only accepted byte string, when there is exactly one (`=`): compared
    /// directly, no hashing.
    single: Option<Box<[u8]>>,
    null_ok: bool,
    /// Trailing 0x20 bytes are ignored (`bytes` then holds trimmed values).
    pad_space: bool,
    /// A float column test (see [`FloatFilter`]); the other sets are then empty.
    float: Option<FloatSet>,
}

/// [`FloatFilter`] with `points` sorted.
struct FloatSet {
    lo: f64,
    hi: f64,
    lo_open: bool,
    hi_open: bool,
    points: Option<Vec<f64>>,
}

impl FloatSet {
    #[inline(always)]
    fn accepts(&self, v: f64) -> bool {
        // NaN fails the comparisons anyway; it is excluded first for the point search.
        !v.is_nan()
            && (if self.lo_open { v > self.lo } else { v >= self.lo })
            && (if self.hi_open { v < self.hi } else { v <= self.hi })
            && self.points.as_ref().map_or(true, |p| p.binary_search_by(|x| x.partial_cmp(&v).unwrap_or(Cmp::Equal)).is_ok())
    }
}

/// `v` without its trailing 0x20 bytes.
fn trim_spaces(v: &[u8]) -> &[u8] {
    let n = v.iter().rposition(|&c| c != b' ').map_or(0, |i| i + 1);
    &v[..n]
}

impl FilterSet {
    fn seal(&mut self) {
        if self.bytes.len() == 1 {
            self.single = self.bytes.iter().next().cloned();
        }
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Int,
    Bytes,
}

fn key_kind(ty: ColumnType) -> Option<Kind> {
    match ty {
        ColumnType::Int64 | ColumnType::Timestamp | ColumnType::Bool => Some(Kind::Int),
        ColumnType::Varchar | ColumnType::Decimal => Some(Kind::Bytes),
        _ => None,
    }
}

struct Plan<'a> {
    snap: &'a Snapshot,
    filter: &'a ResolvedFilter,
    keys: Vec<KeySpec>,
    /// The functions of the spec, then one hidden `Min` per [`KeySpec::ColumnPad`]
    /// (the shortest raw length seen in the group, see `pad_slots`).
    funcs: Vec<FuncSpec>,
    /// Functions of the spec: `funcs[..nuf]`.
    nuf: usize,
    /// Per PAD key: (index in `keys`, column, index of its hidden function).
    pad_slots: Vec<(usize, usize, usize)>,
    filters: Vec<(usize, FilterSet)>,
    /// The filters as zone-map / column-Bloom tests: entries that cannot hold
    /// an accepted row are not read (they would create no group either).
    prune: Pruner,
    kinds: Vec<Kind>,
    want: Vec<bool>,
    /// Some key is a column (the group changes from row to row).
    col_keys: bool,
    top: Option<TopN>,
    /// Initial state of each function.
    init: Vec<Acc>,
    /// SipHash keys of this run: every table of the run hashes alike, so the
    /// partition of a group is the same in every worker.
    hasher: RandomState,
    /// Hash partitions of the merge (a power of two; 1 = no partitioning).
    parts: usize,
    max: usize,
    /// Bytes a worker accumulates before charging the shared budget.
    charge_batch: usize,
    used: AtomicUsize,
    abort: AtomicBool,
    cancelled: AtomicBool,
    err: Mutex<Option<Error>>,
}

enum Unit {
    /// Entries `[from, to)` of chunk `chunk`'s candidate list.
    Chunk {
        chunk: usize,
        from: usize,
        to: usize,
    },
    Mem {
        seg: usize,
        from: usize,
        to: usize,
    },
}

struct Units {
    list: Vec<Unit>,
    /// Per chunk, the series entries that can hold wanted rows.
    cands: Vec<Vec<usize>>,
    /// Rows in the candidate entries and the MemTable (an upper bound).
    est_rows: usize,
}

impl<'a> Plan<'a> {
    fn new(
        snap: &'a Snapshot,
        filter: &'a ResolvedFilter,
        spec: &AggSpec,
        filters: Vec<(usize, FilterSet)>,
    ) -> Result<Plan<'a>> {
        let cols = snap.schema.columns();
        if spec.keys.len() > MAX_KEYS || spec.funcs.len() > MAX_FUNCS {
            return Err(invalid(format!("at most {MAX_KEYS} keys and {MAX_FUNCS} functions")));
        }
        if spec.max_memory == 0 {
            return Err(invalid("max_memory must be positive"));
        }
        if let Some(t) = &spec.top {
            if spec.keys.len() != 1 {
                return Err(invalid("a top-N needs exactly one group key"));
            }
            if t.func >= spec.funcs.len() {
                return Err(invalid(format!("top-N function {} out of range", t.func)));
            }
        }
        let mut want = vec![false; cols.len()];
        let col = |c: usize| cols.get(c).ok_or_else(|| invalid(format!("column {c} out of range")));
        let mut kinds = Vec::new();
        let mut col_keys = false;
        for k in &spec.keys {
            match *k {
                KeySpec::Series => kinds.push(Kind::Int),
                KeySpec::Column(c) | KeySpec::ColumnPad(c) | KeySpec::Bucket(c, _) => {
                    let kind = key_kind(col(c)?.ty).ok_or_else(|| {
                        invalid(format!("column {c} cannot be a group key (use the series for TAGs)"))
                    })?;
                    if matches!(k, KeySpec::ColumnPad(_)) && !matches!(kind, Kind::Bytes) {
                        return Err(invalid(format!("column {c}: PAD SPACE needs a string key")));
                    }
                    if let KeySpec::Bucket(_, w) = *k {
                        if w <= 0 || col(c)?.ty != ColumnType::Timestamp {
                            return Err(invalid(format!("column {c}: a bucket needs a TIMESTAMP column and a positive width")));
                        }
                    }
                    kinds.push(kind);
                    want[c] = true;
                    col_keys = true;
                }
            }
        }
        for f in &spec.funcs {
            match *f {
                FuncSpec::CountStar => {}
                FuncSpec::Count(c) => {
                    if col(c)?.ty == ColumnType::Tag {
                        return Err(invalid("COUNT of a TAG column: use COUNT(*)"));
                    }
                    want[c] = true;
                }
                FuncSpec::Sum(c) => {
                    if col(c)?.ty != ColumnType::Int64 {
                        return Err(invalid(format!("SUM needs an INT64 column, column {c} is {:?}", cols[c].ty)));
                    }
                    want[c] = true;
                }
                FuncSpec::Min(c) | FuncSpec::Max(c) => {
                    if !matches!(col(c)?.ty, ColumnType::Int64 | ColumnType::Timestamp) {
                        return Err(invalid(format!(
                            "MIN/MAX need an INT64 or TIMESTAMP column, column {c} is {:?}",
                            cols[c].ty
                        )));
                    }
                    want[c] = true;
                }
            }
        }
        let mut funcs = spec.funcs.clone();
        let mut pad_slots = Vec::new();
        for (j, k) in spec.keys.iter().enumerate() {
            if let KeySpec::ColumnPad(c) = *k {
                pad_slots.push((j, c, funcs.len()));
                funcs.push(FuncSpec::Min(c));
            }
        }
        let mut prune = Pruner::from_cols(&snap.schema, &spec.preds)?;
        for (c, set) in &filters {
            if set.float.is_some() {
                // no float test of its own here: the caller's `preds` hints prune
                want[*c] = true;
                continue;
            }
            match key_kind(col(*c)?.ty) {
                None => return Err(invalid(format!("column {c} cannot be filtered"))),
                Some(Kind::Int) => prune.add_ints(&snap.schema, *c, set.ints.iter().copied(), set.null_ok),
                Some(Kind::Bytes) => {
                    prune.add_bytes(&snap.schema, *c, set.bytes.iter().map(|b| &**b), set.pad_space, set.null_ok)
                }
            }
            want[*c] = true;
        }
        Ok(Plan {
            snap,
            filter,
            keys: spec.keys.clone(),
            init: funcs.iter().map(|&f| Acc::init(f)).collect(),
            nuf: spec.funcs.len(),
            funcs,
            pad_slots,
            filters,
            prune,
            kinds,
            want,
            col_keys,
            top: spec.top,
            hasher: RandomState::new(),
            parts: 1,
            max: spec.max_memory,
            charge_batch: (spec.max_memory / 64).clamp(1, 32 << 10),
            used: AtomicUsize::new(0),
            abort: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            err: Mutex::new(None),
        })
    }

    fn charge(&self, n: usize) -> Result<()> {
        if self.used.fetch_add(n, Ordering::Relaxed).saturating_add(n) > self.max {
            self.abort.store(true, Ordering::Relaxed);
            return Err(budget_error(self.max));
        }
        Ok(())
    }

    fn release(&self, n: usize) {
        let mut cur = self.used.load(Ordering::Relaxed);
        while let Err(now) =
            self.used.compare_exchange_weak(cur, cur.saturating_sub(n), Ordering::Relaxed, Ordering::Relaxed)
        {
            cur = now;
        }
    }

    fn fail(&self, e: Error) {
        self.abort.store(true, Ordering::Relaxed);
        let mut slot = self.err.lock().unwrap_or_else(PoisonError::into_inner);
        slot.get_or_insert(e);
    }

    /// The error or cancellation that stopped the run, if any.
    fn check_stop(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(Error::Cancelled("aggregation cancelled".into()));
        }
        match self.err.lock().unwrap_or_else(PoisonError::into_inner).take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn group_bytes(&self, key: usize) -> usize {
        key + GROUP_OVERHEAD + self.funcs.len() * std::mem::size_of::<Acc>()
    }

    #[inline]
    fn hash(&self, key: &[u8]) -> u64 {
        let mut h = self.hasher.build_hasher();
        h.write(key);
        h.finish()
    }

    /// Partition of a group (high bits: the table slot uses the low ones).
    #[inline]
    fn part_of(&self, hash: u64) -> usize {
        (hash >> 40) as usize & (self.parts - 1)
    }

    /// Entries of the chunks and rows of the MemTable that can hold wanted
    /// rows, cut into work units of about [`UNIT_ROWS`] rows.
    fn units(&self) -> Units {
        let f = self.filter;
        let mut units = Vec::new();
        let mut cands = Vec::new();
        let mut est_rows = 0usize;
        if !f.is_empty() {
            for (ci, c) in self.snap.chunks.iter().enumerate() {
                if !f.chunk_wanted(c) {
                    cands.push(Vec::new());
                    continue;
                }
                let picked = f.series.as_ref().filter(|s| s.len().saturating_mul(16) < c.series.len());
                let list: Vec<usize> = match picked.and_then(|s| c.entries_of(s)) {
                    Some(v) => v,
                    None => (0..c.series.len()).collect(),
                };
                // Entries the time/series tests drop anyway stay (the worker
                // skips them); the others go through the value summaries.
                let list: Vec<usize> = if self.prune.is_empty() {
                    list
                } else {
                    list.into_iter()
                        .filter(|&si| {
                            c.series.get(si).map_or(true, |e| {
                                let live = e.ts_min <= f.ts_max && e.ts_max >= f.ts_min && f.series_matches(e.series_id);
                                !live || !self.prune.entry_pruned(c, si)
                            })
                        })
                        .collect()
                };
                let (mut from, mut rows) = (0, 0usize);
                for (k, &si) in list.iter().enumerate() {
                    rows = rows.saturating_add(c.series.get(si).map_or(0, |e| e.row_count as usize));
                    if rows >= UNIT_ROWS || k + 1 - from >= UNIT_ENTRIES || k + 1 == list.len() {
                        units.push(Unit::Chunk { chunk: ci, from, to: k + 1 });
                        est_rows = est_rows.saturating_add(rows);
                        from = k + 1;
                        rows = 0;
                    }
                }
                cands.push(list);
            }
            for (seg, rows) in self.snap.mem.segment_rows().enumerate() {
                let mut from = 0;
                while from < rows.len() {
                    let to = (from + UNIT_MEM_ROWS).min(rows.len());
                    units.push(Unit::Mem { seg, from, to });
                    from = to;
                }
                est_rows = est_rows.saturating_add(rows.len());
            }
        }
        Units { list: units, cands, est_rows }
    }
}

// ─── partial state ───────────────────────────────────────────────────────────

/// State of one function in one group, 24 bytes whatever the function: `n`
/// counts the values folded in; `Sum` keeps its signed 128-bit sum in
/// (`hi`, `v` as unsigned low half), `Min`/`Max` their extreme in `v`.
#[derive(Clone, Copy)]
struct Acc {
    n: u64,
    v: i64,
    hi: i64,
}

impl Acc {
    /// The state of `f` before any value.
    fn init(f: FuncSpec) -> Acc {
        match f {
            FuncSpec::Min(_) => Acc { n: 0, v: i64::MAX, hi: 0 },
            FuncSpec::Max(_) => Acc { n: 0, v: i64::MIN, hi: 0 },
            _ => Acc { n: 0, v: 0, hi: 0 },
        }
    }

    #[inline(always)]
    fn sum(&self) -> i128 {
        (i128::from(self.hi) << 64) | i128::from(self.v as u64)
    }

    #[inline(always)]
    fn set_sum(&mut self, s: i128) {
        self.v = s as i64;
        self.hi = (s >> 64) as i64;
    }

    /// Folds the state `o` of the same function in; `false` = SUM overflow.
    fn merge(&mut self, o: &Acc, f: FuncSpec) -> bool {
        self.n += o.n;
        match f {
            FuncSpec::Sum(_) => match self.sum().checked_add(o.sum()) {
                Some(s) => self.set_sum(s),
                None => return false,
            },
            FuncSpec::Min(_) => self.v = self.v.min(o.v),
            FuncSpec::Max(_) => self.v = self.v.max(o.v),
            _ => {}
        }
        true
    }
}

fn overflow() -> Error {
    Error::Full("SUM exceeds the 128-bit accumulator".into())
}

trait Fold {
    const VAL: bool;
    /// `false` = overflow.
    fn add(acc: &mut Acc, v: i64) -> bool;
}
struct FCount;
struct FSum;
struct FMin;
struct FMax;
impl Fold for FCount {
    const VAL: bool = false;
    #[inline(always)]
    fn add(acc: &mut Acc, _: i64) -> bool {
        acc.n += 1;
        true
    }
}
impl Fold for FSum {
    const VAL: bool = true;
    #[inline(always)]
    fn add(acc: &mut Acc, v: i64) -> bool {
        acc.n += 1;
        match acc.sum().checked_add(i128::from(v)) {
            Some(s) => {
                acc.set_sum(s);
                true
            }
            None => false,
        }
    }
}
impl Fold for FMin {
    const VAL: bool = true;
    #[inline(always)]
    fn add(acc: &mut Acc, v: i64) -> bool {
        acc.n += 1;
        acc.v = acc.v.min(v);
        true
    }
}
impl Fold for FMax {
    const VAL: bool = true;
    #[inline(always)]
    fn add(acc: &mut Acc, v: i64) -> bool {
        acc.n += 1;
        acc.v = acc.v.max(v);
        true
    }
}

#[derive(Clone, Copy)]
enum Gid<'a> {
    One(u32),
    /// Group of row `a + k` at `k`.
    Rows(&'a [u32]),
}

impl Gid<'_> {
    #[inline(always)]
    fn at(&self, k: usize) -> u32 {
        match self {
            Gid::One(g) => *g,
            Gid::Rows(r) => r[k],
        }
    }
}

/// Folds rows `[a, b)` of one column into the states of function `fi`.
#[allow(clippy::too_many_arguments)]
fn kernel<F: Fold>(
    accs: &mut [Acc],
    nf: usize,
    fi: usize,
    vals: &[i64],
    nulls: &[bool],
    a: usize,
    b: usize,
    gid: Gid<'_>,
) -> bool {
    let has_nulls = !nulls.is_empty();
    if let Gid::One(g) = gid {
        if g == NONE {
            return true;
        }
        let mut acc = accs[g as usize * nf + fi];
        let mut ok = true;
        for i in a..b {
            if has_nulls && nulls[i] {
                continue;
            }
            ok &= F::add(&mut acc, if F::VAL { vals[i] } else { 0 });
        }
        accs[g as usize * nf + fi] = acc;
        return ok;
    }
    for i in a..b {
        let g = gid.at(i - a);
        if g == NONE || (has_nulls && nulls[i]) {
            continue;
        }
        if !F::add(&mut accs[g as usize * nf + fi], if F::VAL { vals[i] } else { 0 }) {
            return false;
        }
    }
    true
}

/// A column of a decoded window, checked to hold rows `[0, need)`.
enum Col<'a> {
    Ints(&'a [i64], &'a [bool]),
    Bools(&'a [bool], &'a [bool]),
    Bytes {
        off: &'a [u32],
        data: &'a [u8],
        nulls: &'a [bool],
    },
    /// Floats: bit patterns (f64 / f32) for the float filters; COUNT needs only the NULLs.
    F64(&'a [u64], &'a [bool]),
    F32(&'a [u32], &'a [bool]),
}

impl<'a> Col<'a> {
    fn of(win: &'a Window, c: usize, need: usize) -> Result<Col<'a>> {
        let d: &ColumnData =
            win.cols.get(c).and_then(Option::as_ref).ok_or_else(|| corrupt("wanted column not decoded"))?;
        let nulls = d.null_flags().unwrap_or(&[]);
        let short = || corrupt("decoded column shorter than its series window");
        if d.len() < need || (!nulls.is_empty() && nulls.len() < need) {
            return Err(short());
        }
        Ok(match d.dense() {
            DenseRef::Ints(v) => Col::Ints(v, nulls),
            DenseRef::Bools(v) => Col::Bools(v, nulls),
            DenseRef::Bytes { offsets, data } => Col::Bytes { off: offsets, data, nulls },
            DenseRef::Bits64(v) => Col::F64(v, nulls),
            DenseRef::Bits32(v) => Col::F32(v, nulls),
        })
    }

    fn nulls(&self) -> &'a [bool] {
        match self {
            Col::Ints(_, n) | Col::Bools(_, n) | Col::F64(_, n) | Col::F32(_, n) | Col::Bytes { nulls: n, .. } => n,
        }
    }

    #[inline(always)]
    fn is_null(&self, i: usize) -> bool {
        let n = self.nulls();
        !n.is_empty() && n[i]
    }

    #[inline(always)]
    fn bytes(&self, i: usize) -> Result<&'a [u8]> {
        let Col::Bytes { off, data, .. } = self else { return Ok(&[]) };
        let (s, e) = (
            *off.get(i).ok_or_else(|| corrupt("offsets"))? as usize,
            *off.get(i + 1).ok_or_else(|| corrupt("offsets"))? as usize,
        );
        data.get(s..e).ok_or_else(|| corrupt("byte column offsets out of range"))
    }

    fn int(&self, i: usize) -> i64 {
        match self {
            Col::Ints(v, _) => v[i],
            Col::Bools(v, _) => i64::from(v[i]),
            _ => 0,
        }
    }

    /// Whether rows `i` and `j` have the same key under `key` (NULL equals NULL).
    #[inline(always)]
    fn same(&self, key: &KeySpec, i: usize, j: usize) -> bool {
        match (self.is_null(i), self.is_null(j)) {
            (true, true) => true,
            (false, false) => match self {
                Col::Bytes { .. } => matches!((self.bytes(i), self.bytes(j)), (Ok(x), Ok(y)) if x == y),
                _ => bucket_of(key, self.int(i)) == bucket_of(key, self.int(j)),
            },
            _ => false,
        }
    }

    /// Appends the key image of row `i` under `key`: a string drops its trailing 0x20 bytes
    /// ([`KeySpec::ColumnPad`]), a timestamp becomes its bucket start ([`KeySpec::Bucket`]).
    fn encode(&self, key: &KeySpec, i: usize, out: &mut Vec<u8>) -> Result<()> {
        let pad = matches!(key, KeySpec::ColumnPad(_));
        if self.is_null(i) {
            out.push(0);
            return Ok(());
        }
        out.push(1);
        match self {
            Col::Bytes { .. } => {
                let b = self.bytes(i)?;
                let b = if pad { trim_spaces(b) } else { b };
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
            _ => out.extend_from_slice(&bucket_of(key, self.int(i)).to_le_bytes()),
        }
        Ok(())
    }
}

/// `v` itself, or the start of its bucket for a [`KeySpec::Bucket`] (floor division; the start
/// lies within `width` below `v`, saturating at `i64::MIN`).
#[inline(always)]
fn bucket_of(key: &KeySpec, v: i64) -> i64 {
    match *key {
        KeySpec::Bucket(_, w) if w > 0 => v.saturating_sub(v.rem_euclid(w)),
        _ => v,
    }
}

impl FilterSet {
    fn accepts_row(&self, col: &Col<'_>, i: usize, last: &mut Option<(i64, bool)>) -> Result<bool> {
        if col.is_null(i) {
            return Ok(self.null_ok);
        }
        if let Some(fs) = &self.float {
            let short = || corrupt("float column shorter than its window");
            return Ok(match col {
                Col::F64(v, _) => fs.accepts(f64::from_bits(*v.get(i).ok_or_else(short)?)),
                Col::F32(v, _) => fs.accepts(f64::from(f32::from_bits(*v.get(i).ok_or_else(short)?))),
                _ => false,
            });
        }
        if let Col::Bytes { .. } = col {
            let v = col.bytes(i)?;
            let v = if self.pad_space { trim_spaces(v) } else { v };
            return Ok(match &self.single {
                Some(s) => v == &**s,
                None => self.bytes.contains(v),
            });
        }
        let v = col.int(i);
        match last {
            Some((x, ok)) if *x == v => Ok(*ok),
            _ => {
                let ok = self.ints.contains(&v);
                *last = Some((v, ok));
                Ok(ok)
            }
        }
    }

    fn accepts_value(&self, v: &Value) -> bool {
        if let Some(fs) = &self.float {
            return match v {
                Value::Float64(f) => fs.accepts(*f),
                Value::Float32(f) => fs.accepts(f64::from(*f)),
                _ => false,
            };
        }
        match v {
            Value::Null => self.null_ok,
            Value::Int(i) | Value::Timestamp(i) => self.ints.contains(i),
            Value::Bool(b) => self.ints.contains(&i64::from(*b)),
            Value::Bytes(b) => self.bytes.contains(if self.pad_space { trim_spaces(b) } else { b.as_slice() }),
            _ => false,
        }
    }
}

/// Groups of one worker or of one merged partition, in creation order: the
/// key images live back to back in one arena and an open-addressing index
/// (linear probing, load at most 1/2) maps a hash to a group number. Keys come
/// from the client, so the hash is the keyed SipHash of the [`Plan`]; the hash
/// of every group is kept, so growing the index and merging never rehash.
struct Groups {
    /// `0` = free, else group number + 1. Power-of-two length (empty only in
    /// the candidate lists of a top-N, which are never probed).
    slots: Vec<u32>,
    hashes: Vec<u64>,
    /// End of the key image of each group in `arena`.
    ends: Vec<usize>,
    arena: Vec<u8>,
    /// `funcs.len()` states per group.
    accs: Vec<Acc>,
}

impl Groups {
    fn new() -> Groups {
        Groups { slots: vec![0; 16], hashes: Vec::new(), ends: Vec::new(), arena: Vec::new(), accs: Vec::new() }
    }

    fn len(&self) -> usize {
        self.hashes.len()
    }

    /// Room for `n` more groups without growing (best effort: a failed
    /// reservation just leaves the table to grow as it fills).
    #[allow(clippy::slow_vector_initialization)] // try_reserve first: no abort on OOM
    fn presize(&mut self, n: usize, nf: usize) {
        let n = n.min(1 << 28);
        let slots = (self.hashes.len() + n).saturating_mul(2).next_power_of_two().max(16);
        if slots > self.slots.len() && self.hashes.is_empty() {
            let mut v: Vec<u32> = Vec::new();
            if v.try_reserve_exact(slots).is_ok() {
                v.resize(slots, 0);
                self.slots = v;
            }
        }
        let _ = self.hashes.try_reserve(n);
        let _ = self.ends.try_reserve(n);
        let _ = self.accs.try_reserve(n.saturating_mul(nf));
    }

    /// Makes room for `n` more groups (the index never grows during a batch).
    fn reserve_groups(&mut self, n: usize, max: usize) -> Result<()> {
        while (self.hashes.len() + n) * 2 > self.slots.len() {
            self.grow(max)?;
        }
        Ok(())
    }

    /// Touches what looking up `hashes` will read (slots, then hashes and keys
    /// of the occupied ones) in loops whose loads do not depend on each other,
    /// so the cache misses overlap instead of queueing one after another.
    fn warm(&self, hashes: &[u64], buf: &mut Vec<u32>) {
        let mask = self.slots.len() - 1;
        buf.clear();
        buf.extend(hashes.iter().map(|&h| self.slots[h as usize & mask]));
        let mut sink = 0u64;
        for &c in buf.iter().filter(|&&c| c != 0) {
            sink ^= self.hashes[(c - 1) as usize];
        }
        for &c in buf.iter().filter(|&&c| c != 0) {
            let g = (c - 1) as usize;
            let start = if g == 0 { 0 } else { self.ends[g - 1] };
            sink ^= u64::from(self.arena.get(start).copied().unwrap_or(0));
        }
        std::hint::black_box(sink);
    }

    fn key(&self, g: usize) -> &[u8] {
        let start = if g == 0 { 0 } else { self.ends[g - 1] };
        &self.arena[start..self.ends[g]]
    }

    fn grow(&mut self, max: usize) -> Result<()> {
        let n = (self.slots.len() * 2).max(16);
        let mut slots: Vec<u32> = Vec::new();
        slots.try_reserve_exact(n).map_err(|_| budget_error(max))?;
        slots.resize(n, 0);
        for (g, &h) in self.hashes.iter().enumerate() {
            let mut i = h as usize & (n - 1);
            while slots[i] != 0 {
                i = (i + 1) & (n - 1);
            }
            slots[i] = g as u32 + 1;
        }
        self.slots = slots;
        Ok(())
    }

    /// The group of `key` (whose hash is `hash`), created with the `init` states
    /// (one per function) on first sight; `true` when it was created.
    fn find_or_add(&mut self, hash: u64, key: &[u8], init: &[Acc], max: usize) -> Result<(u32, bool)> {
        if (self.hashes.len() + 1) * 2 > self.slots.len() {
            self.grow(max)?;
        }
        let mask = self.slots.len() - 1;
        let mut i = hash as usize & mask;
        loop {
            let s = self.slots[i];
            if s == 0 {
                break;
            }
            let g = (s - 1) as usize;
            if self.hashes[g] == hash && self.key(g) == key {
                return Ok((g as u32, false));
            }
            i = (i + 1) & mask;
        }
        let g = u32::try_from(self.hashes.len()).map_err(|_| Error::Full("too many groups".into()))?;
        if g == u32::MAX - 1 {
            return Err(Error::Full("too many groups".into()));
        }
        let oom = |_| budget_error(max);
        self.hashes.try_reserve(1).map_err(oom)?;
        self.ends.try_reserve(1).map_err(oom)?;
        self.arena.try_reserve(key.len()).map_err(oom)?;
        self.accs.try_reserve(init.len()).map_err(oom)?;
        self.arena.extend_from_slice(key);
        self.ends.push(self.arena.len());
        self.hashes.push(hash);
        self.accs.extend_from_slice(init);
        self.slots[i] = g + 1;
        Ok((g, true))
    }

    /// The groups `ids`, in that order, without an index (candidates of a top-N).
    fn extract(&self, ids: &[u32], nf: usize) -> Groups {
        let mut out = Groups { slots: Vec::new(), hashes: Vec::new(), ends: Vec::new(), arena: Vec::new(), accs: Vec::new() };
        for &g in ids {
            let g = g as usize;
            out.arena.extend_from_slice(self.key(g));
            out.ends.push(out.arena.len());
            out.hashes.push(self.hashes[g]);
            out.accs.extend_from_slice(&self.accs[g * nf..(g + 1) * nf]);
        }
        out
    }

    /// Appends the groups of `o` (an index-less list) after these.
    fn append(&mut self, o: Groups) {
        let base = self.arena.len();
        self.arena.extend_from_slice(&o.arena);
        self.ends.extend(o.ends.iter().map(|e| e + base));
        self.hashes.extend_from_slice(&o.hashes);
        self.accs.extend_from_slice(&o.accs);
    }
}

/// Key images of a run of rows, resolved to groups together.
#[derive(Default)]
struct Batch {
    imgs: Vec<u8>,
    /// End of each image in `imgs` (relative to the batch).
    ends: Vec<usize>,
    hashes: Vec<u64>,
    /// Group of every representative row of the stretch, in order.
    gids: Vec<u32>,
    /// Per row of the stretch: its representative (an index into `gids`) or `NONE`.
    rep_of: Vec<u32>,
    warm: Vec<u32>,
}

/// Representatives resolved per round: small enough that the lines `warm`
/// touches are still in the cache when the lookups come.
const BATCH_REPS: usize = 256;

/// One worker's groups.
struct Local {
    groups: Groups,
    /// Bytes this worker has accounted for its groups.
    bytes: usize,
    /// Part of `bytes` not charged to the shared budget yet.
    pending: usize,
    scratch: Vec<u8>,
    last: Vec<u8>,
    last_gid: u32,
    gids: Vec<u32>,
    keep: Vec<bool>,
    /// Raw lengths of a PAD key column over a stretch (scratch of `fold_all`).
    padlens: Vec<i64>,
    /// Batch of distinct key images of a stretch (see `rows_to_groups`).
    batch: Batch,
    /// Group numbers by hash partition: `part_order[part_start[p]..part_start[p + 1]]`
    /// (empty when the run is not partitioned).
    part_order: Vec<u32>,
    part_start: Vec<usize>,
}

impl Local {
    fn new(p: &Plan<'_>) -> Result<Local> {
        let mut l = Local {
            groups: Groups::new(),
            bytes: 0,
            pending: 0,
            scratch: Vec::new(),
            last: Vec::new(),
            last_gid: NONE,
            gids: Vec::new(),
            keep: Vec::new(),
            padlens: Vec::new(),
            batch: Batch::default(),
            part_order: Vec::new(),
            part_start: Vec::new(),
        };
        if p.keys.is_empty() {
            // Implicit aggregation: one group even over no rows.
            l.scratch.clear();
            l.group_of(p)?;
        }
        Ok(l)
    }

    /// Accounts for `n` more bytes; charges the shared budget in batches.
    fn note(&mut self, p: &Plan<'_>, n: usize) -> Result<()> {
        self.bytes += n;
        self.pending += n;
        if self.pending >= p.charge_batch {
            let n = std::mem::take(&mut self.pending);
            p.charge(n)?;
        }
        Ok(())
    }

    /// Charges what is pending and, in a partitioned run, sorts the groups by partition.
    fn end_work(&mut self, p: &Plan<'_>) -> Result<()> {
        let n = std::mem::take(&mut self.pending);
        p.charge(n)?;
        if p.parts > 1 {
            let (np, n) = (p.parts, self.groups.len());
            let oom = |_| budget_error(p.max);
            let cost = n * 4 + (np + 1) * 8;
            p.charge(cost)?;
            self.bytes += cost;
            let mut start = vec![0usize; np + 1];
            for &h in &self.groups.hashes {
                start[p.part_of(h) + 1] += 1;
            }
            for i in 0..np {
                start[i + 1] += start[i];
            }
            let mut fill = start.clone();
            let mut order: Vec<u32> = Vec::new();
            order.try_reserve_exact(n).map_err(oom)?;
            order.resize(n, 0);
            for (g, &h) in self.groups.hashes.iter().enumerate() {
                let q = p.part_of(h);
                order[fill[q]] = g as u32;
                fill[q] += 1;
            }
            self.part_order = order;
            self.part_start = start;
        }
        Ok(())
    }

    /// Group of the key image in `scratch` (created on first sight).
    fn group_of(&mut self, p: &Plan<'_>) -> Result<u32> {
        if self.last_gid != NONE && self.scratch == self.last {
            return Ok(self.last_gid);
        }
        let hash = p.hash(&self.scratch);
        let (g, new) = self.groups.find_or_add(hash, &self.scratch, &p.init, p.max)?;
        if new {
            let cost = p.group_bytes(self.scratch.len());
            self.note(p, cost)?;
        }
        std::mem::swap(&mut self.scratch, &mut self.last);
        self.last_gid = g;
        Ok(g)
    }

    /// Rows `[a, b)` of the decoded window of a chunk entry of series `sid`.
    fn stretch(&mut self, p: &Plan<'_>, win: &Window, sid: u64, a: usize, b: usize) -> Result<()> {
        let n = b - a;
        let filtered = !p.filters.is_empty();
        if filtered {
            self.keep.clear();
            self.keep.resize(n, true);
            for (c, set) in &p.filters {
                let col = Col::of(win, *c, b)?;
                let mut last = None;
                for i in a..b {
                    if self.keep[i - a] && !set.accepts_row(&col, i, &mut last)? {
                        self.keep[i - a] = false;
                    }
                }
            }
        }
        // Series part of the key (the same for the whole stretch).
        self.scratch.clear();
        let mut cols: Vec<(usize, Col<'_>)> = Vec::new();
        for (k, key) in p.keys.iter().enumerate() {
            match *key {
                KeySpec::Series => self.scratch.extend_from_slice(&sid.to_le_bytes()),
                KeySpec::Column(c) | KeySpec::ColumnPad(c) | KeySpec::Bucket(c, _) => cols.push((k, Col::of(win, c, b)?)),
            }
        }
        let mut gids = std::mem::take(&mut self.gids);
        gids.clear();
        let g = if p.col_keys {
            self.rows_to_groups(p, &cols, sid, a, b, &mut gids)?;
            None
        } else {
            // A stretch with no accepted row must not create its group.
            if filtered && !self.keep.iter().any(|&k| k) {
                self.gids = gids;
                return Ok(());
            }
            let g = self.group_of(p)?;
            if filtered {
                gids.extend(self.keep.iter().map(|&k| if k { g } else { NONE }));
                None
            } else {
                Some(g)
            }
        };
        let gid = match g {
            Some(g) => Gid::One(g),
            None => Gid::Rows(&gids),
        };
        let res = self.fold_all(p, win, a, b, gid);
        self.gids = gids;
        res
    }

    /// Fills `gids` with the group of each row `[a, b)` of the stretch
    /// (`NONE` for rows the filters dropped), creating groups as needed. A row
    /// whose key columns equal those of the kept row before it shares its
    /// image; the distinct images are looked up in batches (see `Groups::warm`).
    fn rows_to_groups(
        &mut self,
        p: &Plan<'_>,
        cols: &[(usize, Col<'_>)],
        sid: u64,
        a: usize,
        b: usize,
        gids: &mut Vec<u32>,
    ) -> Result<()> {
        let filtered = !p.filters.is_empty();
        let mut bt = std::mem::take(&mut self.batch);
        bt.gids.clear();
        bt.rep_of.clear();
        bt.imgs.clear();
        bt.ends.clear();
        bt.hashes.clear();
        let res = (|| -> Result<()> {
            // (row, representative) of the kept row before this one
            let mut prev: Option<(usize, u32)> = None;
            for i in a..b {
                if filtered && !self.keep[i - a] {
                    bt.rep_of.push(NONE);
                    prev = None;
                    continue;
                }
                if let Some((pi, pr)) = prev {
                    if cols.iter().all(|(k, c)| c.same(&p.keys[*k], pi, i)) {
                        bt.rep_of.push(pr);
                        continue;
                    }
                }
                let start = bt.imgs.len();
                let mut at = 0;
                for spec in &p.keys {
                    match spec {
                        KeySpec::Series => bt.imgs.extend_from_slice(&sid.to_le_bytes()),
                        KeySpec::Column(_) | KeySpec::ColumnPad(_) | KeySpec::Bucket(..) => {
                            let Some((_, c)) = cols.get(at) else { return Err(corrupt("key column list")) };
                            c.encode(spec, i, &mut bt.imgs)?;
                            at += 1;
                        }
                    }
                }
                bt.hashes.push(p.hash(&bt.imgs[start..]));
                bt.ends.push(bt.imgs.len());
                let rep = u32::try_from(bt.gids.len() + bt.ends.len() - 1).map_err(|_| Error::Full("too many keys".into()))?;
                bt.rep_of.push(rep);
                prev = Some((i, rep));
                if bt.ends.len() >= BATCH_REPS {
                    self.resolve(p, &mut bt)?;
                }
            }
            self.resolve(p, &mut bt)
        })();
        gids.clear();
        if res.is_ok() {
            gids.extend(bt.rep_of.iter().map(|&r| if r == NONE { NONE } else { bt.gids[r as usize] }));
        }
        self.batch = bt;
        res
    }

    /// Looks up the pending images of `bt`, appending their groups to `bt.gids`.
    fn resolve(&mut self, p: &Plan<'_>, bt: &mut Batch) -> Result<()> {
        let n = bt.ends.len();
        if n == 0 {
            return Ok(());
        }
        self.groups.reserve_groups(n, p.max)?;
        self.groups.warm(&bt.hashes, &mut bt.warm);
        let mut start = 0;
        for r in 0..n {
            let key = &bt.imgs[start..bt.ends[r]];
            let (g, new) = self.groups.find_or_add(bt.hashes[r], key, &p.init, p.max)?;
            if new {
                let cost = p.group_bytes(key.len());
                self.note(p, cost)?;
            }
            bt.gids.push(g);
            start = bt.ends[r];
        }
        bt.imgs.clear();
        bt.ends.clear();
        bt.hashes.clear();
        Ok(())
    }

    fn fold_all(&mut self, p: &Plan<'_>, win: &Window, a: usize, b: usize, gid: Gid<'_>) -> Result<()> {
        let nf = p.funcs.len();
        let ts = p.snap.schema.ts_index();
        let accs = &mut self.groups.accs;
        // Shortest raw length of each PAD key per group (the key image is trimmed).
        for &(_, c, fi) in &p.pad_slots {
            let col = Col::of(win, c, b)?;
            self.padlens.clear();
            self.padlens.resize(a, 0);
            for i in a..b {
                self.padlens.push(if col.is_null(i) { 0 } else { col.bytes(i)?.len() as i64 });
            }
            if !kernel::<FMin>(accs, nf, fi, &self.padlens, col.nulls(), a, b, gid) {
                return Err(overflow());
            }
        }
        for (fi, f) in p.funcs[..p.nuf].iter().enumerate() {
            let ok = match *f {
                FuncSpec::CountStar => kernel::<FCount>(accs, nf, fi, &[], &[], a, b, gid),
                FuncSpec::Count(c) => {
                    let col = Col::of(win, c, b)?;
                    kernel::<FCount>(accs, nf, fi, &[], col.nulls(), a, b, gid)
                }
                FuncSpec::Sum(c) | FuncSpec::Min(c) | FuncSpec::Max(c) => {
                    let col = Col::of(win, c, b)?;
                    let Col::Ints(vals, nulls) = col else { return Err(corrupt("integer column expected")) };
                    // A series is sorted by time: its extremes are its ends.
                    if let (FuncSpec::Min(_) | FuncSpec::Max(_), Gid::One(g), true) = (f, gid, c == ts) {
                        if g != NONE && a < b {
                            let acc = &mut accs[g as usize * nf + fi];
                            acc.n += (b - a) as u64;
                            acc.v = if matches!(f, FuncSpec::Min(_)) { acc.v.min(vals[a]) } else { acc.v.max(vals[b - 1]) };
                        }
                        continue;
                    }
                    match f {
                        FuncSpec::Sum(_) => kernel::<FSum>(accs, nf, fi, vals, nulls, a, b, gid),
                        FuncSpec::Min(_) => kernel::<FMin>(accs, nf, fi, vals, nulls, a, b, gid),
                        _ => kernel::<FMax>(accs, nf, fi, vals, nulls, a, b, gid),
                    }
                }
            };
            if !ok {
                return Err(overflow());
            }
        }
        Ok(())
    }

    /// MemTable rows.
    fn mem_rows(&mut self, p: &Plan<'_>, rows: &[MemRow]) -> Result<()> {
        let f = p.filter;
        let schema = &p.snap.schema;
        let nf = p.funcs.len();
        'rows: for m in rows {
            if !f.series_matches(m.series_id) || !f.ts_matches(schema.row_ts(&m.row)) {
                continue;
            }
            for (c, set) in &p.filters {
                if !set.accepts_value(cell(m, *c)?) {
                    continue 'rows;
                }
            }
            self.scratch.clear();
            for key in &p.keys {
                match *key {
                    KeySpec::Series => self.scratch.extend_from_slice(&m.series_id.to_le_bytes()),
                    KeySpec::Column(c) | KeySpec::ColumnPad(c) | KeySpec::Bucket(c, _) => match cell(m, c)? {
                        Value::Null => self.scratch.push(0),
                        Value::Int(i) | Value::Timestamp(i) => {
                            self.scratch.push(1);
                            self.scratch.extend_from_slice(&bucket_of(key, *i).to_le_bytes());
                        }
                        Value::Bool(b) => {
                            self.scratch.push(1);
                            self.scratch.extend_from_slice(&i64::from(*b).to_le_bytes());
                        }
                        Value::Bytes(b) => {
                            let b = if matches!(key, KeySpec::ColumnPad(_)) { trim_spaces(b) } else { b.as_slice() };
                            self.scratch.push(1);
                            self.scratch.extend_from_slice(&(b.len() as u32).to_le_bytes());
                            self.scratch.extend_from_slice(b);
                        }
                        _ => return Err(corrupt("unexpected value type in a key column")),
                    },
                }
            }
            let g = self.group_of(p)? as usize;
            for &(_, c, fi) in &p.pad_slots {
                if let Value::Bytes(b) = cell(m, c)? {
                    FMin::add(&mut self.groups.accs[g * nf + fi], b.len() as i64);
                }
            }
            for (fi, f) in p.funcs[..p.nuf].iter().enumerate() {
                let acc = &mut self.groups.accs[g * nf + fi];
                let ok = match *f {
                    FuncSpec::CountStar => FCount::add(acc, 0),
                    FuncSpec::Count(c) => cell(m, c)?.is_null() || FCount::add(acc, 0),
                    FuncSpec::Sum(c) | FuncSpec::Min(c) | FuncSpec::Max(c) => match cell(m, c)? {
                        Value::Int(v) | Value::Timestamp(v) => match f {
                            FuncSpec::Sum(_) => FSum::add(acc, *v),
                            FuncSpec::Min(_) => FMin::add(acc, *v),
                            _ => FMax::add(acc, *v),
                        },
                        _ => true,
                    },
                };
                if !ok {
                    return Err(overflow());
                }
            }
        }
        Ok(())
    }
}

// ─── execution ───────────────────────────────────────────────────────────────

/// Sets the abort flag if its thread unwinds, so the other workers stop.
struct AbortOnUnwind<'a>(&'a AtomicBool);

impl Drop for AbortOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Relaxed);
        }
    }
}

/// Counts a worker out (and wakes the calling thread) however it ends.
struct Done<'a>(&'a AtomicUsize, &'a std::thread::Thread);

impl Drop for Done<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
        self.1.unpark();
    }
}

/// Reborrows the optional callback for a shorter lifetime.
fn reborrow<'b>(o: &'b mut Option<&mut dyn FnMut() -> bool>) -> Option<&'b mut dyn FnMut() -> bool> {
    match o {
        Some(c) => Some(&mut **c),
        None => None,
    }
}

/// What a merged partition leaves: its result columns, or (top-N) its best groups.
enum Out {
    Piece(AggResult, usize),
    Cands(Groups),
}

impl Plan<'_> {
    /// Claims units until none is left or the run is aborted. `poll` (the
    /// calling thread only) is the caller's cancel callback.
    fn work(&self, units: &Units, next: &AtomicUsize, mut poll: Option<&mut dyn FnMut() -> bool>) -> Result<Local> {
        let _guard = AbortOnUnwind(&self.abort);
        let mut local = Local::new(self)?;
        // The first check is due at once, then every `POLL_EVERY`.
        let mut last_poll = Instant::now().checked_sub(POLL_EVERY).unwrap_or_else(Instant::now);
        let mut tick = 0u32;
        let mut stop_requested = |this: &Plan<'_>| -> bool {
            if this.abort.load(Ordering::Relaxed) {
                return true;
            }
            if let Some(p) = poll.as_mut() {
                if last_poll.elapsed() >= POLL_EVERY {
                    last_poll = Instant::now();
                    if p() {
                        this.cancelled.store(true, Ordering::Relaxed);
                        this.abort.store(true, Ordering::Relaxed);
                        return true;
                    }
                }
            }
            false
        };
        let (list, cands) = (&units.list, &units.cands);
        let f = self.filter;
        loop {
            if stop_requested(self) {
                break;
            }
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(unit) = list.get(i) else { break };
            let r = match *unit {
                Unit::Mem { seg, from, to } => {
                    let rows = self.snap.mem.segment_rows().nth(seg).and_then(|r| r.get(from..to)).unwrap_or(&[]);
                    local.mem_rows(self, rows)
                }
                Unit::Chunk { chunk, from, to } => (|| {
                    let meta = &self.snap.chunks[chunk];
                    for &si in &cands[chunk][from..to] {
                        let Some(e) = meta.series.get(si) else { continue };
                        if e.ts_min > f.ts_max || e.ts_max < f.ts_min || !f.series_matches(e.series_id) {
                            continue;
                        }
                        tick = tick.wrapping_add(1);
                        if tick % 16 == 0 && stop_requested(self) {
                            return Ok(());
                        }
                        let sid = e.series_id;
                        visible_stretches(self.snap, chunk, si, (f.ts_min, f.ts_max), &self.want, |win, a, b| {
                            local.stretch(self, win, sid, a, b)
                        })?;
                    }
                    Ok(())
                })(),
            };
            if let Err(e) = r {
                self.fail(e);
                break;
            }
        }
        if !self.abort.load(Ordering::Relaxed) {
            local.end_work(self)?;
        }
        Ok(local)
    }

    /// The result of the workers' groups: a single worker's table is the
    /// result; several are merged by hash partition.
    fn reduce(&self, mut locals: Vec<Local>, threads: usize, cancel: Option<&mut dyn FnMut() -> bool>) -> Result<AggResult> {
        let one = match locals.len() {
            0 => Some(Local::new(self)?),
            1 => locals.pop(),
            _ => None,
        };
        if let Some(l) = one {
            let (r, _) = self.output(&l.groups)?;
            return Ok(r);
        }
        let total: usize = locals.iter().map(|l| l.groups.len()).sum();
        let next = AtomicUsize::new(0);
        let tasks = |poll: Option<&mut dyn FnMut() -> bool>| self.merge_tasks(&locals, &next, poll);
        let outs: Vec<Vec<(usize, Out)>> = if threads > 1 && total >= MERGE_PARALLEL_GROUPS {
            let o = run_parallel(self, threads, cancel, &tasks);
            self.check_stop()?;
            o?
        } else {
            match tasks(cancel) {
                Ok(o) => vec![o],
                Err(e) => {
                    self.check_stop()?;
                    return Err(e);
                }
            }
        };
        self.check_stop()?;
        // The worker tables are no longer needed.
        let held: usize = locals.iter().map(|l| l.bytes).sum();
        drop(locals);
        self.release(held);
        let mut outs: Vec<(usize, Out)> = outs.into_iter().flatten().collect();
        outs.sort_by_key(|(p, _)| *p);
        if let Some(t) = self.top {
            let mut all = Groups { slots: Vec::new(), hashes: Vec::new(), ends: Vec::new(), arena: Vec::new(), accs: Vec::new() };
            for (_, o) in outs {
                if let Out::Cands(g) = o {
                    all.append(g);
                }
            }
            let ids = self.select_top(&all, t)?;
            return Ok(self.piece(&all, Some(&ids))?.0);
        }
        let mut pieces = outs.into_iter().filter_map(|(_, o)| if let Out::Piece(r, b) = o { Some((r, b)) } else { None });
        let (mut result, _) = pieces.next().map_or_else(|| self.piece(&Groups::new(), None), Ok)?;
        let rest: Vec<(AggResult, usize)> = pieces.collect();
        // Joining copies the pieces: charge the copy, then release the pieces.
        self.charge(rest.iter().map(|(_, b)| *b).sum())?;
        for (r, b) in rest {
            result.append(r)?;
            self.release(b);
        }
        Ok(result)
    }

    /// Merges the partitions claimed through `next` into one table each and
    /// turns each into its [`Out`].
    fn merge_tasks(
        &self,
        locals: &[Local],
        next: &AtomicUsize,
        mut poll: Option<&mut dyn FnMut() -> bool>,
    ) -> Result<Vec<(usize, Out)>> {
        let _guard = AbortOnUnwind(&self.abort);
        let mut outs = Vec::new();
        let mut last_poll = Instant::now();
        loop {
            if let Some(p) = poll.as_mut() {
                if last_poll.elapsed() >= POLL_EVERY {
                    last_poll = Instant::now();
                    if p() {
                        self.cancelled.store(true, Ordering::Relaxed);
                        self.abort.store(true, Ordering::Relaxed);
                    }
                }
            }
            if self.abort.load(Ordering::Relaxed) {
                return Err(Error::Cancelled("aggregation stopped".into()));
            }
            let part = next.fetch_add(1, Ordering::Relaxed);
            if part >= self.parts {
                break;
            }
            let (groups, bytes) = match self.merge_part(locals, part) {
                Ok(g) => g,
                Err(e) => {
                    self.fail(e);
                    return Err(Error::Cancelled("aggregation stopped".into()));
                }
            };
            let out = if self.top.is_some() {
                self.top_cands(&groups).map(Out::Cands)
            } else {
                self.piece(&groups, None).map(|(r, b)| Out::Piece(r, b))
            };
            drop(groups);
            self.release(bytes);
            match out {
                Ok(o) => outs.push((part, o)),
                Err(e) => {
                    self.fail(e);
                    return Err(Error::Cancelled("aggregation stopped".into()));
                }
            }
        }
        Ok(outs)
    }

    /// One partition of every worker's groups, merged. Returns the table and
    /// the bytes accounted for it.
    fn merge_part(&self, locals: &[Local], part: usize) -> Result<(Groups, usize)> {
        let nf = self.funcs.len();
        let mut out = Groups::new();
        out.presize(locals.iter().map(|l| l.part_start[part + 1] - l.part_start[part]).sum(), nf);
        let (mut bytes, mut pending) = (0usize, 0usize);
        let (mut hs, mut warm): (Vec<u64>, Vec<u32>) = (Vec::new(), Vec::new());
        for l in locals {
            let (s, e) = (l.part_start[part], l.part_start[part + 1]);
            for chunk in l.part_order[s..e].chunks(BATCH_REPS) {
                if self.abort.load(Ordering::Relaxed) {
                    return Err(Error::Cancelled("aggregation stopped".into()));
                }
                hs.clear();
                hs.extend(chunk.iter().map(|&g| l.groups.hashes[g as usize]));
                out.reserve_groups(chunk.len(), self.max)?;
                out.warm(&hs, &mut warm);
                for (&g, &h) in chunk.iter().zip(&hs) {
                    let g = g as usize;
                    let key = l.groups.key(g);
                    let (ng, new) = out.find_or_add(h, key, &self.init, self.max)?;
                    if new {
                        let cost = self.group_bytes(key.len());
                        bytes += cost;
                        pending += cost;
                        if pending >= self.charge_batch {
                            self.charge(std::mem::take(&mut pending))?;
                        }
                    }
                    let ng = ng as usize * nf;
                    let src = &l.groups.accs[g * nf..(g + 1) * nf];
                    for ((d, s), f) in out.accs[ng..ng + nf].iter_mut().zip(src).zip(&self.funcs) {
                        if !d.merge(s, *f) {
                            return Err(overflow());
                        }
                    }
                }
            }
        }
        self.charge(pending)?;
        Ok((out, bytes))
    }

    /// The result of a table of groups: all of them, or the top-N.
    fn output(&self, g: &Groups) -> Result<(AggResult, usize)> {
        match self.top {
            Some(t) => {
                let ids = self.select_top(g, t)?;
                self.piece(g, Some(&ids))
            }
            None => self.piece(g, None),
        }
    }

    /// The best groups of a partition as an index-less list.
    fn top_cands(&self, g: &Groups) -> Result<Groups> {
        let Some(t) = self.top else { return Err(corrupt("no top-N")) };
        let ids = self.select_top(g, t)?;
        Ok(g.extract(&ids, self.funcs.len()))
    }

    /// The value a top-N orders by (`None` = SQL NULL).
    fn order_val(&self, g: &Groups, gi: usize, fi: usize) -> Option<i128> {
        let a = &g.accs[gi * self.funcs.len() + fi];
        match self.funcs[fi] {
            FuncSpec::CountStar | FuncSpec::Count(_) => Some(i128::from(a.n)),
            FuncSpec::Sum(_) => (a.n > 0).then_some(a.sum()),
            FuncSpec::Min(_) | FuncSpec::Max(_) => (a.n > 0).then_some(i128::from(a.v)),
        }
    }

    /// Order of two key images of the single key (NULL first).
    fn key_cmp(&self, a: &[u8], b: &[u8]) -> Cmp {
        match (self.keys[0], self.kinds[0]) {
            (KeySpec::Series, _) => series_of(a).cmp(&series_of(b)),
            (_, Kind::Int) => int_of(a).cmp(&int_of(b)),
            (KeySpec::ColumnPad(_), _) => match (bytes_of(a), bytes_of(b)) {
                (Some(x), Some(y)) => pad_cmp(x, y),
                (x, y) => x.cmp(&y),
            },
            (_, Kind::Bytes) => bytes_of(a).cmp(&bytes_of(b)),
        }
    }

    /// The best `t.limit` groups of `g`, best first.
    fn select_top(&self, g: &Groups, t: TopN) -> Result<Vec<u32>> {
        let n = g.len();
        let k = t.limit.min(n);
        if k == 0 {
            return Ok(Vec::new());
        }
        let cost = n.saturating_mul(4);
        self.charge(cost)?;
        let mut ids: Vec<u32> = Vec::new();
        ids.try_reserve_exact(n).map_err(|_| budget_error(self.max))?;
        ids.extend(0..n as u32);
        let cmp = |a: &u32, b: &u32| {
            let (a, b) = (*a as usize, *b as usize);
            let o = self.order_val(g, a, t.func).cmp(&self.order_val(g, b, t.func));
            let o = if t.desc { o.reverse() } else { o };
            o.then_with(|| self.key_cmp(g.key(a), g.key(b)))
        };
        if k < n {
            ids.select_nth_unstable_by(k - 1, cmp);
            ids.truncate(k);
        }
        ids.sort_unstable_by(cmp);
        self.release(cost);
        Ok(ids)
    }

    /// Columnar result of the groups `ids` of `l` (all of them when `None`).
    /// Returns it with the bytes charged for it.
    fn piece(&self, l: &Groups, ids: Option<&[u32]>) -> Result<(AggResult, usize)> {
        let nf = self.funcs.len();
        let nuf = self.nuf;
        let groups = ids.map_or(l.len(), <[u32]>::len);
        let gid = |k: usize| ids.map_or(k, |v| v[k] as usize);
        // A PAD key is returned as its group's shortest raw value: the trimmed image plus spaces.
        let pad_of = |j: usize| self.pad_slots.iter().find(|s| s.0 == j).map(|s| s.2);
        // shortest raw length of the group's PAD key j (0 for a NULL key: its minimum was never set)
        let pad_raw = |g: usize, j: usize| -> usize {
            pad_of(j).map_or(0, |fi| {
                let a = &l.accs[g * nf + fi];
                if a.n == 0 { 0 } else { usize::try_from(a.v).unwrap_or(0) }
            })
        };
        // (the spaces added never exceed the raw length: an upper bound for the budget)
        let key_bytes: usize = (0..groups)
            .map(|k| {
                let g = gid(k);
                l.key(g).len() + self.pad_slots.iter().map(|s| pad_raw(g, s.0)).sum::<usize>()
            })
            .sum();
        let charged = groups.saturating_mul(self.keys.len() * 13 + nf * 16).saturating_add(key_bytes);
        self.charge(charged)?;
        let oom = || budget_error(self.max);
        let mut keys: Vec<KeyCol> = self.keys.iter().zip(&self.kinds).map(|(k, &kind)| KeyCol::empty(k, kind)).collect();
        for kc in &mut keys {
            match kc {
                KeyCol::Series(v) => v.try_reserve_exact(groups).map_err(|_| oom())?,
                KeyCol::Int { nulls, vals } => {
                    nulls.try_reserve_exact(groups).map_err(|_| oom())?;
                    vals.try_reserve_exact(groups).map_err(|_| oom())?;
                }
                KeyCol::Bytes { nulls, starts, lens, data } => {
                    nulls.try_reserve_exact(groups).map_err(|_| oom())?;
                    starts.try_reserve_exact(groups).map_err(|_| oom())?;
                    lens.try_reserve_exact(groups).map_err(|_| oom())?;
                    data.try_reserve_exact(key_bytes).map_err(|_| oom())?;
                }
            }
        }
        let mut funcs: Vec<FuncCol> = vec![FuncCol::default(); nuf];
        for (fc, f) in funcs.iter_mut().zip(&self.funcs[..nuf]) {
            fc.count.try_reserve_exact(groups).map_err(|_| oom())?;
            if matches!(f, FuncSpec::Sum(_)) {
                fc.sum_lo.try_reserve_exact(groups).map_err(|_| oom())?;
                fc.sum_hi.try_reserve_exact(groups).map_err(|_| oom())?;
            }
            if matches!(f, FuncSpec::Min(_) | FuncSpec::Max(_)) {
                fc.val.try_reserve_exact(groups).map_err(|_| oom())?;
            }
        }
        let short = || corrupt("group key image too short");
        for k in 0..groups {
            let g = gid(k);
            let mut r = l.key(g);
            let mut take = |n: usize| -> Result<&[u8]> {
                let (h, t) = (r.get(..n).ok_or_else(short)?, &r[n..]);
                r = t;
                Ok(h)
            };
            for (j, kc) in keys.iter_mut().enumerate() {
                match kc {
                    KeyCol::Series(v) => v.push(u64::from_le_bytes(take(8)?.try_into().map_err(|_| short())?)),
                    KeyCol::Int { nulls, vals } => {
                        let null = take(1)?[0] == 0;
                        nulls.push(null);
                        vals.push(if null { 0 } else { i64::from_le_bytes(take(8)?.try_into().map_err(|_| short())?) });
                    }
                    KeyCol::Bytes { nulls, starts, lens, data } => {
                        let null = take(1)?[0] == 0;
                        nulls.push(null);
                        starts.push(data.len() as u64);
                        if null {
                            lens.push(0);
                        } else {
                            let n = u32::from_le_bytes(take(4)?.try_into().map_err(|_| short())?);
                            data.extend_from_slice(take(n as usize)?);
                            let extra = pad_raw(g, j).saturating_sub(n as usize);
                            // within the `key_bytes` reserved above (pad_raw bounds n + extra)
                            data.resize(data.len() + extra, b' ');
                            let total = u32::try_from(n as usize + extra).map_err(|_| corrupt("PAD key longer than 4 GiB"))?;
                            lens.push(total);
                        }
                    }
                }
            }
            for (fi, (fc, f)) in funcs.iter_mut().zip(&self.funcs[..nuf]).enumerate() {
                let a = &l.accs[g * nf + fi];
                fc.count.push(a.n);
                match f {
                    FuncSpec::Sum(_) => {
                        fc.sum_lo.push(a.v as u64);
                        fc.sum_hi.push(a.hi);
                    }
                    FuncSpec::Min(_) | FuncSpec::Max(_) => fc.val.push(a.v),
                    _ => {}
                }
            }
        }
        Ok((AggResult { groups, keys, funcs }, charged))
    }
}

/// Order of two strings already without trailing 0x20 under a binary PAD SPACE
/// collation (`strnncollsp`): bytes as unsigned up to the first difference; if
/// one is a prefix of the other, the longer one is compared with spaces, so it
/// is smaller when its next byte is below 0x20 (a followed by byte 1 sorts before a), else larger.
fn pad_cmp(a: &[u8], b: &[u8]) -> Cmp {
    let n = a.len().min(b.len());
    match a[..n].cmp(&b[..n]) {
        Cmp::Equal => {}
        o => return o,
    }
    let (rest, longer_is_a) = if a.len() > n { (&a[n..], true) } else { (&b[n..], false) };
    let o = match rest.iter().find(|&&c| c != b' ') {
        None => return Cmp::Equal,
        Some(&c) if c < b' ' => Cmp::Less,
        Some(_) => Cmp::Greater,
    };
    if longer_is_a { o } else { o.reverse() }
}

fn series_of(img: &[u8]) -> u64 {
    img.get(..8).and_then(|b| b.try_into().ok()).map_or(0, u64::from_le_bytes)
}

/// The integer of a key image (`None` = NULL).
fn int_of(img: &[u8]) -> Option<i64> {
    match img.first() {
        Some(1) => img.get(1..9).and_then(|b| b.try_into().ok()).map(i64::from_le_bytes),
        _ => None,
    }
}

/// The bytes of a key image (`None` = NULL).
fn bytes_of(img: &[u8]) -> Option<&[u8]> {
    match img.first() {
        Some(1) => img.get(5..),
        _ => None,
    }
}

type Body<'b, T> = dyn Fn(Option<&mut dyn FnMut() -> bool>) -> Result<T> + Sync + 'b;

/// Runs `body` on `threads` workers (the calling thread only watches the
/// cancel callback and the workers) and returns what each produced. A worker's
/// error is recorded in the plan (see [`Plan::check_stop`]); a panic is re-raised.
fn run_parallel<T: Send>(
    plan: &Plan<'_>,
    threads: usize,
    mut cancel: Option<&mut dyn FnMut() -> bool>,
    body: &Body<'_, T>,
) -> Result<Vec<T>> {
    let live = AtomicUsize::new(0);
    let me = std::thread::current();
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for _ in 0..threads {
            live.fetch_add(1, Ordering::SeqCst);
            let spawned = std::thread::Builder::new().name("moosedb-agg".into()).spawn_scoped(s, || {
                let _done = Done(&live, &me);
                body(None)
            });
            match spawned {
                Ok(h) => handles.push(h),
                Err(_) => {
                    live.fetch_sub(1, Ordering::SeqCst);
                    break;
                }
            }
        }
        if handles.is_empty() {
            // No thread could start: do the work here.
            return Ok(vec![body(reborrow(&mut cancel))?]);
        }
        let mut last = Instant::now().checked_sub(POLL_EVERY).unwrap_or_else(Instant::now);
        loop {
            if let Some(p) = cancel.as_mut() {
                if last.elapsed() >= POLL_EVERY {
                    last = Instant::now();
                    if p() {
                        plan.cancelled.store(true, Ordering::Relaxed);
                        plan.abort.store(true, Ordering::Relaxed);
                    }
                }
            }
            if live.load(Ordering::SeqCst) == 0 {
                break;
            }
            std::thread::park_timeout(POLL_EVERY);
        }
        let mut outs = Vec::new();
        let mut panic = None;
        for h in handles {
            match h.join() {
                Ok(Ok(l)) => outs.push(l),
                Ok(Err(e)) => plan.fail(e),
                Err(p) => {
                    panic.get_or_insert(p);
                }
            }
        }
        if let Some(p) = panic {
            resume_unwind(p);
        }
        Ok(outs)
    })
}

#[allow(dead_code)]
const fn assert_send_sync<T: Send + Sync>() {}
const _: () = assert_send_sync::<Snapshot>();
const _: () = assert_send_sync::<Agg>();

fn cell(m: &MemRow, c: usize) -> Result<&Value> {
    m.row.get(c).ok_or_else(|| corrupt("MemTable row shorter than the schema"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_sum(sum: i128) -> Acc {
        let mut a = Acc::init(FuncSpec::Sum(0));
        a.set_sum(sum);
        a
    }

    #[test]
    fn sums_never_wrap_the_128_bit_accumulator() {
        let mut acc = with_sum(i128::MAX - 1);
        assert!(FSum::add(&mut acc, 1));
        assert!(!FSum::add(&mut acc, 1), "i128::MAX + 1");
        let mut low = with_sum(i128::MIN + 1);
        assert!(!FSum::add(&mut low, -2));
        // the same through the kernel and through the merge of partial states
        let mut accs = [with_sum(i128::MAX)];
        assert!(!kernel::<FSum>(&mut accs, 1, 0, &[1, 2], &[], 0, 2, Gid::One(0)));
        let mut accs = [with_sum(i128::MAX)];
        assert!(!kernel::<FSum>(&mut accs, 1, 0, &[1, 2], &[], 0, 2, Gid::Rows(&[0, 0])));
        let mut a = with_sum(i128::MAX);
        assert!(!a.merge(&with_sum(1), FuncSpec::Sum(0)));
        // the 128-bit value round-trips through (v, hi)
        for s in [0, 1, -1, i128::MAX, i128::MIN, 1 << 64, -(1 << 64), (1 << 63) + 5] {
            assert_eq!(with_sum(s).sum(), s);
        }
        // NULLs are skipped, NONE rows too
        let mut accs = [Acc::init(FuncSpec::Sum(0))];
        assert!(kernel::<FSum>(&mut accs, 1, 0, &[5, 7, 9], &[false, true, false], 0, 3, Gid::Rows(&[0, 0, NONE])));
        assert_eq!((accs[0].n, accs[0].sum()), (1, 5));
        // min / max start at their identities and merge by extreme
        let (mut lo, mut hi) = (Acc::init(FuncSpec::Min(0)), Acc::init(FuncSpec::Max(0)));
        assert!(FMin::add(&mut lo, 3) && FMax::add(&mut hi, 3));
        assert!(lo.merge(&Acc { n: 1, v: -4, hi: 0 }, FuncSpec::Min(0)));
        assert!(hi.merge(&Acc { n: 1, v: 9, hi: 0 }, FuncSpec::Max(0)));
        assert_eq!((lo.v, lo.n, hi.v, hi.n), (-4, 2, 9, 2));
    }
}
