//! Aggregation inside the engine (hash aggregation over one snapshot).
//!
//! An [`Agg`] holds ONE [`Snapshot`] (the same cut a non-ordered scan gets: the
//! rows committed when it was opened; the private batch of the statement is not
//! part of it) and the `[lo, hi]` / series restriction. [`Agg::run`] reads it
//! with a plan of group keys, functions and dictionary filters and leaves a
//! columnar [`AggResult`]. Deletes are applied by [`crate::scan::visible_stretches`],
//! the same code the scans use.
//!
//! * **Keys**: the series (one state per series, a tight loop over the window)
//!   and/or common columns (Int64/Timestamp/Bool as integers, Varchar/Decimal
//!   as bytes). NULL is a value of its own. Keys come from the client, so the
//!   maps are std `HashMap`s (SipHash), never the fast internal hash.
//! * **Filters** are dictionary masks: a run with the single key
//!   `Column(c)` and no filter or function records the distinct values of `c`
//!   in result order (the dictionary); the caller evaluates its predicate on
//!   each value and passes the accepted ones as a mask of that length. Several
//!   filters are ANDed. Values the dictionary does not hold are rejected.
//! * **Parallelism**: `std::thread::scope`; units are (chunk, ~256 entries)
//!   and slices of MemTable segments, claimed through an atomic counter; every
//!   worker owns a partial map and the partials are merged sequentially. The
//!   result is the same for any number of threads (integers only).
//! * **Cancellation**: the caller's callback runs only on the calling thread
//!   (about every 10 ms); workers poll an `AtomicBool`.
//! * **Memory**: keys + states + overhead of every partial map and of the
//!   result are charged to one shared budget; running over it is an error.

use std::collections::{HashMap, HashSet};
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

pub const MAX_KEYS: usize = 16;
pub const MAX_FUNCS: usize = 64;
pub const MAX_FILTERS: usize = 16;
pub const MAX_THREADS: usize = 64;

/// Series entries per work unit.
const UNIT_ENTRIES: usize = 256;
/// MemTable rows per work unit.
const UNIT_MEM_ROWS: usize = 1 << 16;
const POLL_EVERY: Duration = Duration::from_millis(10);
/// Rows of a stretch that no group takes.
const NONE: u32 = u32::MAX;
/// Bytes charged per group on top of its key and states (map entry, boxes).
const GROUP_OVERHEAD: usize = 80;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeySpec {
    Series,
    Column(usize),
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

#[derive(Clone, Debug)]
pub struct AggSpec {
    pub keys: Vec<KeySpec>,
    pub funcs: Vec<FuncSpec>,
    pub filters: Vec<ColFilter>,
    /// Budget in bytes (keys, states, overhead, result).
    pub max_memory: usize,
    /// Worker threads (1 = run on the calling thread).
    pub threads: usize,
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
}

/// One function of the result, one entry per group. `count` is the number of
/// rows (`CountStar`), of non-NULL values (`Count`) or of values that went into
/// `Sum`/`Min`/`Max` (0 = the SQL result is NULL). `sum_*` hold the 128-bit sum
/// (`lo` unsigned, `hi` signed); `val` the minimum or maximum.
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

// ─── handle ──────────────────────────────────────────────────────────────────

/// An aggregation over one snapshot (see the module docs). Not shared between
/// threads while `run` executes; the snapshot itself is.
pub struct Agg {
    snap: Arc<Snapshot>,
    filter: ResolvedFilter,
    dicts: HashMap<usize, KeyCol>,
    result: AggResult,
}

impl Agg {
    pub(crate) fn new(snap: Arc<Snapshot>, filter: ResolvedFilter) -> Agg {
        Agg { snap, filter, dicts: HashMap::new(), result: AggResult::default() }
    }

    pub fn snapshot(&self) -> &Arc<Snapshot> {
        &self.snap
    }

    /// The result of the last successful `run` (empty before and after a failed one).
    pub fn result(&self) -> &AggResult {
        &self.result
    }

    /// Runs the aggregation. `cancel` is called on this thread about every
    /// 10 ms; returning `true` aborts the run with [`Error::Cancelled`].
    /// A panic in a worker thread is re-raised here.
    pub fn run(&mut self, spec: &AggSpec, mut cancel: Option<&mut dyn FnMut() -> bool>) -> Result<()> {
        self.result = AggResult::default();
        let filters = self.compile_filters(spec)?;
        let plan = Plan::new(&self.snap, &self.filter, spec, filters)?;
        let units = plan.units();
        let threads = spec.threads.clamp(1, MAX_THREADS).min(units.0.len().max(1));
        let next = AtomicUsize::new(0);
        let mut locals: Vec<Local> = Vec::new();
        if threads == 1 {
            locals.push(plan.work(&units, &next, reborrow(&mut cancel))?);
        } else {
            locals = run_parallel(&plan, &units, &next, threads, reborrow(&mut cancel))?;
        }
        if plan.cancelled.load(Ordering::Relaxed) {
            return Err(Error::Cancelled("aggregation cancelled".into()));
        }
        if let Some(e) = plan.err.lock().unwrap_or_else(PoisonError::into_inner).take() {
            return Err(e);
        }
        let merged = plan.merge(locals)?;
        let result = plan.finish(merged)?;
        // The distinct values of a column, in result order, are its dictionary.
        if let ([KeySpec::Column(c)], true, true) = (&spec.keys[..], spec.filters.is_empty(), spec.funcs.is_empty()) {
            plan.charge(result.keys[0].heap_bytes())?;
            self.dicts.insert(*c, result.keys[0].clone());
        }
        self.result = result;
        Ok(())
    }

    fn compile_filters(&self, spec: &AggSpec) -> Result<Vec<(usize, FilterSet)>> {
        if spec.filters.len() > MAX_FILTERS {
            return Err(invalid(format!("at most {MAX_FILTERS} filters")));
        }
        let mut out = Vec::new();
        let mut bytes = 0usize;
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
            bytes = bytes.saturating_add(set.bytes.iter().map(|b| b.len() + 48).sum::<usize>() + set.ints.len() * 16);
            if bytes > spec.max_memory {
                return Err(budget_error(spec.max_memory));
            }
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
    null_ok: bool,
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
    funcs: Vec<FuncSpec>,
    filters: Vec<(usize, FilterSet)>,
    kinds: Vec<Kind>,
    want: Vec<bool>,
    /// Some key is a column (the group changes from row to row).
    col_keys: bool,
    max: usize,
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
        let mut want = vec![false; cols.len()];
        let col = |c: usize| cols.get(c).ok_or_else(|| invalid(format!("column {c} out of range")));
        let mut kinds = Vec::new();
        let mut col_keys = false;
        for k in &spec.keys {
            match *k {
                KeySpec::Series => kinds.push(Kind::Int),
                KeySpec::Column(c) => {
                    let kind = key_kind(col(c)?.ty).ok_or_else(|| {
                        invalid(format!("column {c} cannot be a group key (use the series for TAGs)"))
                    })?;
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
        for (c, _) in &filters {
            if key_kind(col(*c)?.ty).is_none() {
                return Err(invalid(format!("column {c} cannot be filtered")));
            }
            want[*c] = true;
        }
        Ok(Plan {
            snap,
            filter,
            keys: spec.keys.clone(),
            funcs: spec.funcs.clone(),
            filters,
            kinds,
            want,
            col_keys,
            max: spec.max_memory,
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

    fn fail(&self, e: Error) {
        self.abort.store(true, Ordering::Relaxed);
        let mut slot = self.err.lock().unwrap_or_else(PoisonError::into_inner);
        slot.get_or_insert(e);
    }

    fn group_bytes(&self, key: usize) -> usize {
        key + GROUP_OVERHEAD + self.funcs.len() * std::mem::size_of::<Acc>()
    }

    /// Entries of the chunks and rows of the MemTable that can hold wanted
    /// rows, cut into work units.
    fn units(&self) -> Units {
        let f = self.filter;
        let mut units = Vec::new();
        let mut cands = Vec::new();
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
                let mut from = 0;
                while from < list.len() {
                    let to = (from + UNIT_ENTRIES).min(list.len());
                    units.push(Unit::Chunk { chunk: ci, from, to });
                    from = to;
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
            }
        }
        (units, cands)
    }
}

// ─── partial state ───────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Acc {
    n: u64,
    sum: i128,
    min: i64,
    max: i64,
}

const ACC0: Acc = Acc { n: 0, sum: 0, min: i64::MAX, max: i64::MIN };

impl Acc {
    fn merge(&mut self, o: &Acc) -> bool {
        self.n += o.n;
        match self.sum.checked_add(o.sum) {
            Some(s) => self.sum = s,
            None => return false,
        }
        self.min = self.min.min(o.min);
        self.max = self.max.max(o.max);
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
        match acc.sum.checked_add(i128::from(v)) {
            Some(s) => {
                acc.sum = s;
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
        acc.min = acc.min.min(v);
        true
    }
}
impl Fold for FMax {
    const VAL: bool = true;
    #[inline(always)]
    fn add(acc: &mut Acc, v: i64) -> bool {
        acc.n += 1;
        acc.max = acc.max.max(v);
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
    /// Floats: only their NULLs matter (COUNT).
    Other(&'a [bool]),
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
            DenseRef::Bits64(_) | DenseRef::Bits32(_) => Col::Other(nulls),
        })
    }

    fn nulls(&self) -> &'a [bool] {
        match self {
            Col::Ints(_, n) | Col::Bools(_, n) | Col::Other(n) | Col::Bytes { nulls: n, .. } => n,
        }
    }

    #[inline(always)]
    fn is_null(&self, i: usize) -> bool {
        let n = self.nulls();
        !n.is_empty() && n[i]
    }

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

    /// Whether rows `i` and `j` hold the same value (NULL equals NULL).
    fn same(&self, i: usize, j: usize) -> bool {
        match (self.is_null(i), self.is_null(j)) {
            (true, true) => true,
            (false, false) => match self {
                Col::Bytes { .. } => matches!((self.bytes(i), self.bytes(j)), (Ok(x), Ok(y)) if x == y),
                _ => self.int(i) == self.int(j),
            },
            _ => false,
        }
    }

    /// Appends the key image of row `i`.
    fn encode(&self, i: usize, out: &mut Vec<u8>) -> Result<()> {
        if self.is_null(i) {
            out.push(0);
            return Ok(());
        }
        out.push(1);
        match self {
            Col::Bytes { .. } => {
                let b = self.bytes(i)?;
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
            _ => out.extend_from_slice(&self.int(i).to_le_bytes()),
        }
        Ok(())
    }
}

impl FilterSet {
    fn accepts_row(&self, col: &Col<'_>, i: usize, last: &mut Option<(i64, bool)>) -> Result<bool> {
        if col.is_null(i) {
            return Ok(self.null_ok);
        }
        if let Col::Bytes { .. } = col {
            return Ok(self.bytes.contains(col.bytes(i)?));
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
        match v {
            Value::Null => self.null_ok,
            Value::Int(i) | Value::Timestamp(i) => self.ints.contains(i),
            Value::Bool(b) => self.ints.contains(&i64::from(*b)),
            Value::Bytes(b) => self.bytes.contains(b.as_slice()),
            _ => false,
        }
    }
}

/// One worker's groups.
struct Local {
    map: HashMap<Box<[u8]>, u32>,
    accs: Vec<Acc>,
    /// Bytes charged for this map.
    bytes: usize,
    scratch: Vec<u8>,
    last: Vec<u8>,
    last_gid: u32,
    gids: Vec<u32>,
    keep: Vec<bool>,
}

impl Local {
    fn new(p: &Plan<'_>) -> Result<Local> {
        let mut l = Local {
            map: HashMap::new(),
            accs: Vec::new(),
            bytes: 0,
            scratch: Vec::new(),
            last: Vec::new(),
            last_gid: NONE,
            gids: Vec::new(),
            keep: Vec::new(),
        };
        if p.keys.is_empty() {
            // Implicit aggregation: one group even over no rows.
            l.scratch.clear();
            l.group_of(p)?;
        }
        Ok(l)
    }

    /// Group of the key image in `scratch` (created on first sight).
    fn group_of(&mut self, p: &Plan<'_>) -> Result<u32> {
        if self.last_gid != NONE && self.scratch == self.last {
            return Ok(self.last_gid);
        }
        let g = match self.map.get(self.scratch.as_slice()) {
            Some(&g) => g,
            None => {
                let g = u32::try_from(self.map.len()).map_err(|_| Error::Full("too many groups".into()))?;
                let cost = p.group_bytes(self.scratch.len());
                p.charge(cost)?;
                self.bytes += cost;
                let nf = p.funcs.len();
                self.accs.try_reserve(nf).map_err(|_| budget_error(p.max))?;
                self.map.try_reserve(1).map_err(|_| budget_error(p.max))?;
                self.accs.extend(std::iter::repeat(ACC0).take(nf));
                self.map.insert(self.scratch.as_slice().into(), g);
                g
            }
        };
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
                KeySpec::Column(c) => cols.push((k, Col::of(win, c, b)?)),
            }
        }
        let mut gids = std::mem::take(&mut self.gids);
        gids.clear();
        let g = if p.col_keys {
            // The group changes by row: build every key image (Series parts are
            // spliced in key order).
            // (row, group) of the row before this one when it was kept: a row whose
            // key columns equal it needs no key image and no lookup.
            let mut prev: Option<(usize, u32)> = None;
            for i in a..b {
                if filtered && !self.keep[i - a] {
                    gids.push(NONE);
                    prev = None;
                    continue;
                }
                if let Some((pi, pg)) = prev {
                    if cols.iter().all(|(_, c)| c.same(pi, i)) {
                        gids.push(pg);
                        prev = Some((i, pg));
                        continue;
                    }
                }
                let mut at = 0;
                let mut key = std::mem::take(&mut self.scratch);
                key.clear();
                for (k, spec) in p.keys.iter().enumerate() {
                    match spec {
                        KeySpec::Series => key.extend_from_slice(&sid.to_le_bytes()),
                        KeySpec::Column(_) => {
                            cols[at].1.encode(i, &mut key)?;
                            debug_assert_eq!(cols[at].0, k);
                            at += 1;
                        }
                    }
                }
                self.scratch = key;
                let g = self.group_of(p)?;
                gids.push(g);
                prev = Some((i, g));
            }
            None
        } else {
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

    fn fold_all(&mut self, p: &Plan<'_>, win: &Window, a: usize, b: usize, gid: Gid<'_>) -> Result<()> {
        let nf = p.funcs.len();
        let ts = p.snap.schema.ts_index();
        for (fi, f) in p.funcs.iter().enumerate() {
            let ok = match *f {
                FuncSpec::CountStar => kernel::<FCount>(&mut self.accs, nf, fi, &[], &[], a, b, gid),
                FuncSpec::Count(c) => {
                    let col = Col::of(win, c, b)?;
                    kernel::<FCount>(&mut self.accs, nf, fi, &[], col.nulls(), a, b, gid)
                }
                FuncSpec::Sum(c) | FuncSpec::Min(c) | FuncSpec::Max(c) => {
                    let col = Col::of(win, c, b)?;
                    let Col::Ints(vals, nulls) = col else { return Err(corrupt("integer column expected")) };
                    // A series is sorted by time: its extremes are its ends.
                    if let (FuncSpec::Min(_) | FuncSpec::Max(_), Gid::One(g), true) = (f, gid, c == ts) {
                        if g != NONE && a < b {
                            let acc = &mut self.accs[g as usize * nf + fi];
                            acc.n += (b - a) as u64;
                            acc.min = acc.min.min(vals[a]);
                            acc.max = acc.max.max(vals[b - 1]);
                        }
                        continue;
                    }
                    match f {
                        FuncSpec::Sum(_) => kernel::<FSum>(&mut self.accs, nf, fi, vals, nulls, a, b, gid),
                        FuncSpec::Min(_) => kernel::<FMin>(&mut self.accs, nf, fi, vals, nulls, a, b, gid),
                        _ => kernel::<FMax>(&mut self.accs, nf, fi, vals, nulls, a, b, gid),
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
                    KeySpec::Column(c) => match cell(m, c)? {
                        Value::Null => self.scratch.push(0),
                        Value::Int(i) | Value::Timestamp(i) => {
                            self.scratch.push(1);
                            self.scratch.extend_from_slice(&i.to_le_bytes());
                        }
                        Value::Bool(b) => {
                            self.scratch.push(1);
                            self.scratch.extend_from_slice(&i64::from(*b).to_le_bytes());
                        }
                        Value::Bytes(b) => {
                            self.scratch.push(1);
                            self.scratch.extend_from_slice(&(b.len() as u32).to_le_bytes());
                            self.scratch.extend_from_slice(b);
                        }
                        _ => return Err(corrupt("unexpected value type in a key column")),
                    },
                }
            }
            let g = self.group_of(p)? as usize;
            for (fi, f) in p.funcs.iter().enumerate() {
                let acc = &mut self.accs[g * nf + fi];
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

type Units = (Vec<Unit>, Vec<Vec<usize>>);

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
        let (list, cands) = units;
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
        Ok(local)
    }

    /// Folds the partial maps into one, sequentially.
    fn merge(&self, mut locals: Vec<Local>) -> Result<Local> {
        locals.sort_by_key(|l| std::cmp::Reverse(l.map.len()));
        let mut it = locals.into_iter();
        let mut base = it.next().map_or_else(|| Local::new(self), Ok)?;
        let nf = self.funcs.len();
        for other in it {
            for (key, g) in other.map {
                let og = g as usize * nf;
                match base.map.get(&key) {
                    Some(&bg) => {
                        let bg = bg as usize * nf;
                        for k in 0..nf {
                            if !base.accs[bg + k].merge(&other.accs[og + k]) {
                                return Err(overflow());
                            }
                        }
                    }
                    None => {
                        let ng = u32::try_from(base.map.len()).map_err(|_| Error::Full("too many groups".into()))?;
                        base.bytes += self.group_bytes(key.len());
                        base.accs.try_reserve(nf).map_err(|_| budget_error(self.max))?;
                        base.map.try_reserve(1).map_err(|_| budget_error(self.max))?;
                        base.accs.extend_from_slice(&other.accs[og..og + nf]);
                        base.map.insert(key, ng);
                    }
                }
            }
        }
        // The partial maps are gone: what is held now is the merged map.
        self.used.store(base.bytes, Ordering::Relaxed);
        if base.bytes > self.max {
            return Err(budget_error(self.max));
        }
        Ok(base)
    }

    /// Columnar result from the merged groups.
    fn finish(&self, l: Local) -> Result<AggResult> {
        let groups = l.map.len();
        let nf = self.funcs.len();
        let key_bytes: usize = l.map.keys().map(|k| k.len()).sum();
        self.charge(groups.saturating_mul(self.keys.len() * 13 + nf * 32).saturating_add(key_bytes))?;
        let oom = || budget_error(self.max);
        let mut keys: Vec<KeyCol> = Vec::new();
        for (k, kind) in self.keys.iter().zip(&self.kinds) {
            keys.push(match (k, kind) {
                (KeySpec::Series, _) => KeyCol::Series(Vec::new()),
                (_, Kind::Int) => KeyCol::Int { nulls: Vec::new(), vals: Vec::new() },
                (_, Kind::Bytes) => {
                    KeyCol::Bytes { nulls: Vec::new(), starts: Vec::new(), lens: Vec::new(), data: Vec::new() }
                }
            });
        }
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
        let mut funcs: Vec<FuncCol> = vec![FuncCol::default(); nf];
        for fc in &mut funcs {
            fc.count.try_reserve_exact(groups).map_err(|_| oom())?;
            fc.sum_lo.try_reserve_exact(groups).map_err(|_| oom())?;
            fc.sum_hi.try_reserve_exact(groups).map_err(|_| oom())?;
            fc.val.try_reserve_exact(groups).map_err(|_| oom())?;
        }
        let short = || corrupt("group key image too short");
        for (key, &g) in &l.map {
            let mut r = &key[..];
            let mut take = |n: usize| -> Result<&[u8]> {
                let (h, t) = (r.get(..n).ok_or_else(short)?, &r[n..]);
                r = t;
                Ok(h)
            };
            for kc in &mut keys {
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
                            lens.push(n);
                            data.extend_from_slice(take(n as usize)?);
                        }
                    }
                }
            }
            for (fi, (fc, f)) in funcs.iter_mut().zip(&self.funcs).enumerate() {
                let a = &l.accs[g as usize * nf + fi];
                fc.count.push(a.n);
                fc.sum_lo.push(a.sum as u64);
                fc.sum_hi.push((a.sum >> 64) as i64);
                fc.val.push(match f {
                    FuncSpec::Min(_) => a.min,
                    FuncSpec::Max(_) => a.max,
                    _ => 0,
                });
            }
        }
        Ok(AggResult { groups, keys, funcs })
    }
}

fn run_parallel(
    plan: &Plan<'_>,
    units: &Units,
    next: &AtomicUsize,
    threads: usize,
    mut cancel: Option<&mut dyn FnMut() -> bool>,
) -> Result<Vec<Local>> {
    let live = AtomicUsize::new(0);
    let me = std::thread::current();
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for _ in 0..threads {
            live.fetch_add(1, Ordering::SeqCst);
            let spawned = std::thread::Builder::new().name("moosedb-agg".into()).spawn_scoped(s, || {
                let _done = Done(&live, &me);
                plan.work(units, next, None)
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
            return Ok(vec![plan.work(units, next, reborrow(&mut cancel))?]);
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
        let mut locals = Vec::new();
        let mut panic = None;
        for h in handles {
            match h.join() {
                Ok(Ok(l)) => locals.push(l),
                Ok(Err(e)) => plan.fail(e),
                Err(p) => {
                    panic.get_or_insert(p);
                }
            }
        }
        if let Some(p) = panic {
            resume_unwind(p);
        }
        Ok(locals)
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

    #[test]
    fn sums_never_wrap_the_128_bit_accumulator() {
        let mut acc = Acc { sum: i128::MAX - 1, ..ACC0 };
        assert!(FSum::add(&mut acc, 1));
        assert!(!FSum::add(&mut acc, 1), "i128::MAX + 1");
        let mut low = Acc { sum: i128::MIN + 1, ..ACC0 };
        assert!(!FSum::add(&mut low, -2));
        // the same through the kernel and through the merge of partial states
        let mut accs = [Acc { sum: i128::MAX, ..ACC0 }];
        assert!(!kernel::<FSum>(&mut accs, 1, 0, &[1, 2], &[], 0, 2, Gid::One(0)));
        let mut accs = [Acc { sum: i128::MAX, ..ACC0 }];
        assert!(!kernel::<FSum>(&mut accs, 1, 0, &[1, 2], &[], 0, 2, Gid::Rows(&[0, 0])));
        let mut a = Acc { sum: i128::MAX, ..ACC0 };
        assert!(!a.merge(&Acc { sum: 1, ..ACC0 }));
        // NULLs are skipped, NONE rows too
        let mut accs = [ACC0];
        assert!(kernel::<FSum>(&mut accs, 1, 0, &[5, 7, 9], &[false, true, false], 0, 3, Gid::Rows(&[0, 0, NONE])));
        assert_eq!((accs[0].n, accs[0].sum), (1, 5));
    }
}
