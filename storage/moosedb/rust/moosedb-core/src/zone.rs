//! Zone maps and column Blooms: pruning of whole series entries by value.
//!
//! Every (series entry, data column) of a chunk -- the unit a scan skips -- gets
//! a small summary written with the chunk (see `chunk` for where it lives):
//!
//! * INT64 / TIMESTAMP / BOOL: `min`, `max` (BOOL as 0/1) and the NULL count;
//! * FLOAT64 / FLOAT32: `min`, `max` over the non-NaN values, "has NaN", NULLs;
//! * VARCHAR / DECIMAL: a Bloom filter of the values (exact bytes) and NULLs.
//!   No min/max: byte order is not the SQL order under a non-binary collation,
//!   and DECIMAL travels as text.
//!
//! A summary can only say "no row of this entry can match"; it is never used
//! to decide a row matches. All of it is conservative: a missing, unknown or
//! malformed summary prunes nothing; deletes leave the summary wider than the
//! live rows (never narrower); MemTable rows are never pruned (they have none).
//!
//! Record: `flags:u8` (bits 0-1 kind: 0 none, 1 int, 2 float, 3 bloom; bit 2
//! range present, bit 3 NaN present, bit 4 NULLs present), `nulls:u32` if bit 4,
//! `min:u64 max:u64` if bit 2 (i64 / f64 bits), or the Bloom encoding for kind 3.
//! Section (after the series Bloom, inside the same sealed unit):
//! `ndata:u32 len:u32 records[len] crc32:u32` (CRC of everything before it).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use crate::bytes::{put_u32, put_u64, put_u8, ByteReader};
use crate::chunk::{ChunkMeta, SeriesEntry};
use crate::error::{corrupt, invalid, Result};
use crate::index::bloom::Bloom;
use crate::schema::{ColumnType, Schema, Value};

const KIND_NONE: u8 = 0;
const KIND_INT: u8 = 1;
const KIND_FLOAT: u8 = 2;
const KIND_BLOOM: u8 = 3;
const KIND_MASK: u8 = 3;
const HAS_RANGE: u8 = 1 << 2;
const HAS_NAN: u8 = 1 << 3;
const HAS_NULLS: u8 = 1 << 4;
const KNOWN_BITS: u8 = KIND_MASK | HAS_RANGE | HAS_NAN | HAS_NULLS;

/// A block with more distinct values (incl. padded forms) than this gets no Bloom:
/// 65536 values take about 80 KiB, kept in memory for every chunk (lab: 2.9%
/// of the rows sit in blocks above it, 12% at 1 << 14).
const MAX_BLOOM_DISTINCT: usize = 1 << 16;
const BLOOM_FPR: f64 = 0.01;
/// Zone records of a chunk are decoded only up to this many data columns.
const MAX_DATA_COLUMNS: u32 = 1 << 16;

// ─── what the SQL layer pushes ───────────────────────────────────────────────

/// A predicate on a data column used **only to skip entries** that cannot
/// match. Rows of entries that are not skipped come back whether they match or
/// not, so the caller still evaluates its WHERE clause. NULL never matches any
/// of these (the caller must not push `IS NULL`, `<=>`, `OR`, ...). Bounds are
/// inclusive; a strict bound is pushed as the inclusive one (a wider range is
/// always safe).
#[derive(Clone, Debug, PartialEq)]
pub enum ColPred {
    /// `lo <= v <= hi` on an INT64 / TIMESTAMP / BOOL (0/1) column.
    Int { col: usize, lo: i64, hi: i64 },
    /// `lo <= v <= hi` on a FLOAT64 / FLOAT32 column (IEEE comparison; a NaN
    /// bound prunes nothing; use `-inf` / `+inf` for an open side).
    Float { col: usize, lo: f64, hi: f64 },
    /// `v` equals one of `values` on a VARCHAR / DECIMAL column, byte for byte
    /// -- only valid when the SQL comparison is exactly that (binary NO PAD
    /// collation), or, with `pad_space`, binary PAD SPACE of a charset whose
    /// space is the byte 0x20 (see `agg::ValueFilter`). An empty list matches
    /// nothing.
    Bytes { col: usize, values: Vec<Vec<u8>>, pad_space: bool },
}

// ─── counters ────────────────────────────────────────────────────────────────

static TESTED: AtomicU64 = AtomicU64::new(0);
static BY_ZONE: AtomicU64 = AtomicU64::new(0);
static BY_BLOOM: AtomicU64 = AtomicU64::new(0);
static ROWS: AtomicU64 = AtomicU64::new(0);
static SKIPPED: AtomicU64 = AtomicU64::new(0);
static READ: AtomicU64 = AtomicU64::new(0);
static READ_NS: AtomicU64 = AtomicU64::new(0);

/// Process-wide pruning counters (all tables, scans and aggregations), for
/// benchmarks. An entry is one series of one chunk (its column blocks are read
/// together); pruning it skips every block it holds. A sorted scan that is
/// rewound counts its entries again.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PruneStats {
    /// Entries that reached the value test (a value predicate was pushed and
    /// the chunk has summaries).
    pub entries_tested: u64,
    pub pruned_by_zone: u64,
    pub pruned_by_bloom: u64,
    /// Rows of the pruned entries (not decoded).
    pub rows_skipped: u64,
    /// Column blocks inside the pruned entries (one per data column), never read.
    pub blocks_skipped: u64,
    /// Column blocks read from disk (block-cache misses), from any scan.
    pub blocks_read: u64,
    /// Nanoseconds spent reading, decrypting and decompressing those blocks
    /// (summed over threads).
    pub block_read_ns: u64,
}

pub fn prune_stats() -> PruneStats {
    PruneStats {
        entries_tested: TESTED.load(Relaxed),
        pruned_by_zone: BY_ZONE.load(Relaxed),
        pruned_by_bloom: BY_BLOOM.load(Relaxed),
        rows_skipped: ROWS.load(Relaxed),
        blocks_skipped: SKIPPED.load(Relaxed),
        blocks_read: READ.load(Relaxed),
        block_read_ns: READ_NS.load(Relaxed),
    }
}

/// A block was read from disk and unpacked in `ns` nanoseconds.
pub(crate) fn note_block_read(ns: u64) {
    READ.fetch_add(1, Relaxed);
    READ_NS.fetch_add(ns, Relaxed);
}

pub fn reset_prune_stats() {
    for c in [&TESTED, &BY_ZONE, &BY_BLOOM, &ROWS, &SKIPPED, &READ, &READ_NS] {
        c.store(0, Relaxed);
    }
}

// ─── one summary ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Zone {
    flags: u8,
    nulls: u32,
    lo: u64,
    hi: u64,
    bloom: Option<Box<Bloom>>,
}

/// Stable 64-bit hash of a value (FNV-1a). Persisted: never change it.
fn hash_bytes(b: &[u8]) -> u64 {
    b.iter().fold(0xcbf2_9ce4_8422_2325, |h, &c| (h ^ u64::from(c)).wrapping_mul(0x0000_0100_0000_01b3))
}

/// `v` without its trailing 0x20 bytes.
fn trim_spaces(v: &[u8]) -> &[u8] {
    let n = v.iter().rposition(|&c| c != b' ').map_or(0, |i| i + 1);
    &v[..n]
}

impl Zone {
    pub(crate) fn none() -> Zone {
        Zone { flags: KIND_NONE, nulls: 0, lo: 0, hi: 0, bloom: None }
    }

    fn kind(&self) -> u8 {
        self.flags & KIND_MASK
    }

    /// Summary of one column block's values.
    pub(crate) fn build(ty: ColumnType, vals: &[&Value]) -> Zone {
        let Ok(nulls) = u32::try_from(vals.iter().filter(|v| v.is_null()).count()) else { return Zone::none() };
        let mut z = Zone::none();
        if nulls > 0 {
            z.flags |= HAS_NULLS;
            z.nulls = nulls;
        }
        match ty {
            ColumnType::Int64 | ColumnType::Timestamp | ColumnType::Bool => {
                let mut r: Option<(i64, i64)> = None;
                for v in vals {
                    let i = match v {
                        Value::Null => continue,
                        Value::Int(i) | Value::Timestamp(i) => *i,
                        Value::Bool(b) => i64::from(*b),
                        _ => return Zone::none(),
                    };
                    r = Some(r.map_or((i, i), |(lo, hi)| (lo.min(i), hi.max(i))));
                }
                z.flags |= KIND_INT;
                if let Some((lo, hi)) = r {
                    z.flags |= HAS_RANGE;
                    (z.lo, z.hi) = (lo as u64, hi as u64);
                }
            }
            ColumnType::Float64 | ColumnType::Float32 => {
                let mut r: Option<(f64, f64)> = None;
                for v in vals {
                    let f = match v {
                        Value::Null => continue,
                        Value::Float64(f) => *f,
                        Value::Float32(f) => f64::from(*f),
                        _ => return Zone::none(),
                    };
                    if f.is_nan() {
                        z.flags |= HAS_NAN;
                    } else {
                        // IEEE order: -0.0 and 0.0 are equal here, either may be kept
                        r = Some(r.map_or((f, f), |(lo, hi)| (if f < lo { f } else { lo }, if f > hi { f } else { hi })));
                    }
                }
                z.flags |= KIND_FLOAT;
                if let Some((lo, hi)) = r {
                    z.flags |= HAS_RANGE;
                    (z.lo, z.hi) = (lo.to_bits(), hi.to_bits());
                }
            }
            ColumnType::Varchar | ColumnType::Decimal => {
                // Both the exact value and its PAD SPACE form: a lookup uses
                // whichever its comparison needs (see `Pruner::add_bytes`).
                // Sized by the distinct values of THIS block; over the cap, no Bloom.
                let mut hs: std::collections::HashSet<u64> = std::collections::HashSet::new();
                for v in vals {
                    match v {
                        Value::Null => {}
                        Value::Bytes(b) => {
                            hs.insert(hash_bytes(b));
                            let t = trim_spaces(b);
                            if t.len() != b.len() {
                                hs.insert(hash_bytes(t));
                            }
                            if hs.len() > MAX_BLOOM_DISTINCT {
                                return Zone::none();
                            }
                        }
                        _ => return Zone::none(),
                    }
                }
                let mut b = Bloom::with_capacity(hs.len(), BLOOM_FPR);
                for h in hs {
                    b.insert(h);
                }
                z.flags |= KIND_BLOOM;
                z.bloom = Some(Box::new(b));
            }
            ColumnType::Tag => return Zone::none(),
        }
        z
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        put_u8(buf, self.flags);
        if self.flags & HAS_NULLS != 0 {
            put_u32(buf, self.nulls);
        }
        if self.flags & HAS_RANGE != 0 {
            put_u64(buf, self.lo);
            put_u64(buf, self.hi);
        }
        if let Some(b) = &self.bloom {
            b.encode(buf);
        }
    }

    /// `rows` is the row count of the entry (bounds the NULL count).
    fn decode(r: &mut ByteReader<'_>, rows: u32) -> Result<Zone> {
        let bad = || corrupt("invalid zone record");
        let flags = r.u8()?;
        if flags & !KNOWN_BITS != 0 {
            return Err(bad());
        }
        let kind = flags & KIND_MASK;
        let nulls = if flags & HAS_NULLS != 0 { r.u32()? } else { 0 };
        if nulls > rows || (flags & HAS_NULLS != 0 && nulls == 0) {
            return Err(bad());
        }
        let mut z = Zone { flags, nulls, lo: 0, hi: 0, bloom: None };
        match kind {
            KIND_NONE if flags != KIND_NONE => return Err(bad()),
            KIND_INT | KIND_FLOAT => {
                if flags & HAS_RANGE != 0 {
                    (z.lo, z.hi) = (r.u64()?, r.u64()?);
                    let ordered = if kind == KIND_INT {
                        (z.lo as i64) <= (z.hi as i64)
                    } else {
                        let (lo, hi) = (f64::from_bits(z.lo), f64::from_bits(z.hi));
                        !lo.is_nan() && !hi.is_nan() && lo <= hi
                    };
                    if !ordered {
                        return Err(bad());
                    }
                } else if flags & HAS_NAN == 0 && nulls != rows {
                    // no value at all, so every row must be NULL
                    return Err(bad());
                }
                if kind == KIND_INT && flags & HAS_NAN != 0 {
                    return Err(bad());
                }
            }
            KIND_BLOOM => {
                if flags & (HAS_RANGE | HAS_NAN) != 0 {
                    return Err(bad());
                }
                z.bloom = Some(Box::new(Bloom::decode(r)?));
            }
            _ => {}
        }
        Ok(z)
    }

    // Each `may_*` answers "can a row of this entry satisfy it?"; `true` when unsure.

    fn may_int_range(&self, lo: i64, hi: i64) -> bool {
        if self.kind() != KIND_INT {
            return true;
        }
        self.flags & HAS_RANGE != 0 && !((self.hi as i64) < lo || (self.lo as i64) > hi)
    }

    fn may_float_range(&self, lo: f64, hi: f64) -> bool {
        if self.kind() != KIND_FLOAT || self.flags & HAS_NAN != 0 || lo.is_nan() || hi.is_nan() {
            return true;
        }
        self.flags & HAS_RANGE != 0 && !(f64::from_bits(self.hi) < lo || f64::from_bits(self.lo) > hi)
    }

    /// `sorted` is ascending.
    fn may_ints(&self, sorted: &[i64], null_ok: bool) -> bool {
        if self.kind() != KIND_INT || (null_ok && self.nulls > 0) {
            return true;
        }
        if self.flags & HAS_RANGE == 0 {
            return false;
        }
        let i = sorted.partition_point(|&v| v < self.lo as i64);
        sorted.get(i).is_some_and(|&v| v <= self.hi as i64)
    }

    fn may_hashes(&self, hashes: &[u64], null_ok: bool) -> bool {
        match &self.bloom {
            Some(b) if self.kind() == KIND_BLOOM && !(null_ok && self.nulls > 0) => hashes.iter().any(|&h| b.may_contain(h)),
            _ => true,
        }
    }
}

// ─── the section of a chunk ──────────────────────────────────────────────────

/// Summaries of every (entry, data column) of a chunk, entry-major.
#[derive(Clone, Debug)]
pub struct ZoneMaps {
    ndata: usize,
    zones: Vec<Zone>,
}

impl ZoneMaps {
    pub(crate) fn new(ndata: usize, zones: Vec<Zone>) -> ZoneMaps {
        ZoneMaps { ndata, zones }
    }

    fn get(&self, entry: usize, pos: usize) -> Option<&Zone> {
        if pos >= self.ndata {
            return None;
        }
        self.zones.get(entry.checked_mul(self.ndata)?.checked_add(pos)?)
    }

    pub fn data_columns(&self) -> usize {
        self.ndata
    }
}

/// Appends the section for `zones` (`ndata` per entry). `false` (nothing
/// written) when there is nothing worth storing or it would not fit.
pub(crate) fn encode_section(buf: &mut Vec<u8>, ndata: usize, zones: &[Zone]) -> bool {
    if ndata == 0 || zones.iter().all(|z| z.kind() == KIND_NONE) {
        return false;
    }
    let mut body = Vec::new();
    for z in zones {
        z.encode(&mut body);
    }
    let (Ok(nd), Ok(len)) = (u32::try_from(ndata), u32::try_from(body.len())) else { return false };
    let start = buf.len();
    put_u32(buf, nd);
    put_u32(buf, len);
    buf.extend_from_slice(&body);
    let crc = crc32fast::hash(&buf[start..]);
    put_u32(buf, crc);
    true
}

/// Parses the section; any inconsistency yields `None` (no pruning), never an
/// error: a summary is an optimization, and in an encrypted chunk the sealed
/// unit already authenticated these bytes.
pub(crate) fn decode_section(bytes: &[u8], entries: &[SeriesEntry]) -> Option<ZoneMaps> {
    let mut r = ByteReader::new(bytes);
    let ndata = r.u32().ok()?;
    let len = r.u32().ok()? as usize;
    let body = r.take(len).ok()?;
    let crc = r.u32().ok()?;
    if crc32fast::hash(&bytes[..8 + len]) != crc || ndata == 0 || ndata > MAX_DATA_COLUMNS {
        return None;
    }
    let ndata = ndata as usize;
    let mut br = ByteReader::new(body);
    // every record takes at least one byte: `len` bounds the allocation
    let mut zones = Vec::with_capacity(entries.len().saturating_mul(ndata).min(len));
    for e in entries {
        if e.blocks.len() != ndata {
            return None;
        }
        for _ in 0..ndata {
            zones.push(Zone::decode(&mut br, e.row_count).ok()?);
        }
    }
    (br.remaining() == 0 && r.remaining() == 0).then_some(ZoneMaps { ndata, zones })
}

// ─── pruning ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum PredKind {
    Int { lo: i64, hi: i64 },
    Float { lo: f64, hi: f64 },
    Ints { sorted: Vec<i64>, null_ok: bool },
    Hashes { hashes: Vec<u64>, null_ok: bool },
}

#[derive(Clone, Debug)]
struct Pred {
    /// Position among the data columns (the index of the entry's block).
    pos: usize,
    kind: PredKind,
}

/// Value predicates resolved against a schema; decides per entry. Empty =
/// prunes nothing.
#[derive(Clone, Debug, Default)]
pub(crate) struct Pruner {
    fingerprint: u32,
    ndata: usize,
    preds: Vec<Pred>,
}

impl Pruner {
    pub(crate) fn new(schema: &Schema) -> Pruner {
        Pruner { fingerprint: schema.fingerprint(), ndata: schema.data_indices().len(), preds: Vec::new() }
    }

    pub(crate) fn from_cols(schema: &Schema, cols: &[ColPred]) -> Result<Pruner> {
        let mut p = Pruner::new(schema);
        for c in cols {
            match c {
                ColPred::Int { col, lo, hi } => {
                    check_type(schema, *col, "an integer", |t| {
                        matches!(t, ColumnType::Int64 | ColumnType::Timestamp | ColumnType::Bool)
                    })?;
                    p.push(schema, *col, PredKind::Int { lo: *lo, hi: *hi });
                }
                ColPred::Float { col, lo, hi } => {
                    check_type(schema, *col, "a float", |t| matches!(t, ColumnType::Float64 | ColumnType::Float32))?;
                    // A FLOAT constant may be compared as the float it rounds to
                    // or as the double: cover both.
                    let (mut lo, mut hi) = (*lo, *hi);
                    if schema.columns()[*col].ty == ColumnType::Float32 {
                        if !lo.is_nan() {
                            lo = lo.min(f64::from(lo as f32));
                        }
                        if !hi.is_nan() {
                            hi = hi.max(f64::from(hi as f32));
                        }
                    }
                    p.push(schema, *col, PredKind::Float { lo, hi });
                }
                ColPred::Bytes { col, values, pad_space } => {
                    check_type(schema, *col, "a string or decimal", |t| {
                        matches!(t, ColumnType::Varchar | ColumnType::Decimal)
                    })?;
                    p.add_bytes(schema, *col, values.iter().map(Vec::as_slice), *pad_space, false);
                }
            }
        }
        Ok(p)
    }

    fn push(&mut self, schema: &Schema, col: usize, kind: PredKind) {
        // the table timestamp has no summary (the entry bounds cover it)
        if col == schema.ts_index() {
            return;
        }
        if let Some(pos) = schema.data_indices().iter().position(|&c| c == col) {
            self.preds.push(Pred { pos, kind });
        }
    }

    /// Accepted integers of `col` (the exact set of an aggregation filter).
    pub(crate) fn add_ints(&mut self, schema: &Schema, col: usize, ints: impl Iterator<Item = i64>, null_ok: bool) {
        let mut sorted: Vec<i64> = ints.collect();
        sorted.sort_unstable();
        sorted.dedup();
        self.push(schema, col, PredKind::Ints { sorted, null_ok });
    }

    /// Accepted byte strings of `col`; with `pad_space` both sides ignore
    /// trailing 0x20 (the summary holds the exact and the trimmed hash).
    pub(crate) fn add_bytes<'a>(
        &mut self,
        schema: &Schema,
        col: usize,
        values: impl Iterator<Item = &'a [u8]>,
        pad_space: bool,
        null_ok: bool,
    ) {
        let hashes = values.map(|v| hash_bytes(if pad_space { trim_spaces(v) } else { v })).collect();
        self.push(schema, col, PredKind::Hashes { hashes, null_ok });
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.preds.is_empty()
    }

    /// Whether entry `si` of chunk `c` cannot hold a row that satisfies all the
    /// predicates (counted in [`prune_stats`]).
    pub(crate) fn entry_pruned(&self, c: &ChunkMeta, si: usize) -> bool {
        if self.preds.is_empty() {
            return false;
        }
        let Some(zm) = &c.zones else { return false };
        // a chunk written under another column layout is not trusted
        if zm.ndata != self.ndata || c.header.schema_fingerprint != self.fingerprint {
            return false;
        }
        TESTED.fetch_add(1, Relaxed);
        for p in &self.preds {
            let Some(z) = zm.get(si, p.pos) else { continue };
            let may = match &p.kind {
                PredKind::Int { lo, hi } => z.may_int_range(*lo, *hi),
                PredKind::Float { lo, hi } => z.may_float_range(*lo, *hi),
                PredKind::Ints { sorted, null_ok } => z.may_ints(sorted, *null_ok),
                PredKind::Hashes { hashes, null_ok } => z.may_hashes(hashes, *null_ok),
            };
            if !may {
                let counter = if matches!(p.kind, PredKind::Hashes { .. }) { &BY_BLOOM } else { &BY_ZONE };
                counter.fetch_add(1, Relaxed);
                ROWS.fetch_add(c.series.get(si).map_or(0, |e| u64::from(e.row_count)), Relaxed);
                SKIPPED.fetch_add(self.ndata as u64, Relaxed);
                return true;
            }
        }
        false
    }
}

fn check_type(schema: &Schema, col: usize, what: &str, ok: impl Fn(ColumnType) -> bool) -> Result<()> {
    match schema.columns().get(col) {
        None => Err(invalid(format!("column {col} out of range"))),
        Some(c) if ok(c.ty) => Ok(()),
        Some(c) => Err(invalid(format!("column {col} is {:?}, the predicate needs {what} column", c.ty))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints(v: &[Option<i64>]) -> Vec<Value> {
        v.iter().map(|x| x.map_or(Value::Null, Value::Int)).collect()
    }

    fn build(ty: ColumnType, v: &[Value]) -> Zone {
        Zone::build(ty, &v.iter().collect::<Vec<_>>())
    }

    fn roundtrip(z: &Zone, rows: u32) -> Zone {
        let mut b = Vec::new();
        z.encode(&mut b);
        let mut r = ByteReader::new(&b);
        let d = Zone::decode(&mut r, rows).unwrap();
        assert_eq!(r.remaining(), 0);
        d
    }

    #[test]
    fn int_zone_ranges_and_nulls() {
        let v = ints(&[Some(5), None, Some(-3), Some(9)]);
        let z = roundtrip(&build(ColumnType::Int64, &v), 4);
        assert!(z.may_int_range(9, 20) && z.may_int_range(-10, -3) && z.may_int_range(0, 0));
        assert!(!z.may_int_range(10, 20) && !z.may_int_range(i64::MIN, -4));
        assert!(z.may_ints(&[-3], false) && z.may_ints(&[4, 100], false));
        assert!(!z.may_ints(&[10, 11], false) && !z.may_ints(&[], false));
        assert!(z.may_ints(&[], true), "NULLs present: a mask that accepts NULL matches");
        let all_null = roundtrip(&build(ColumnType::Int64, &ints(&[None, None])), 2);
        assert!(!all_null.may_int_range(i64::MIN, i64::MAX));
        assert!(!all_null.may_ints(&[1], false) && all_null.may_ints(&[1], true));
        // extremes do not overflow
        let ext = roundtrip(&build(ColumnType::Int64, &ints(&[Some(i64::MIN), Some(i64::MAX)])), 2);
        assert!(ext.may_int_range(0, 0) && ext.may_int_range(i64::MAX, i64::MAX));
        let b = roundtrip(&build(ColumnType::Bool, &[Value::Bool(true), Value::Bool(true)]), 2);
        assert!(b.may_ints(&[1], false) && !b.may_ints(&[0], false));
    }

    #[test]
    fn float_zone_edge_cases() {
        let f = |v: &[f64]| roundtrip(&build(ColumnType::Float64, &v.iter().map(|&x| Value::Float64(x)).collect::<Vec<_>>()), v.len() as u32);
        let z = f(&[1.5, -2.0, 7.0]);
        assert!(z.may_float_range(7.0, 8.0) && z.may_float_range(f64::NEG_INFINITY, -2.0));
        assert!(!z.may_float_range(7.5, f64::INFINITY) && !z.may_float_range(-9.0, -2.5));
        assert!(z.may_float_range(f64::NAN, 1.0), "NaN bound: unsure");
        // NaN in the block: nothing can be pruned
        assert!(f(&[1.0, f64::NAN]).may_float_range(100.0, 101.0));
        // only NaN: no range, still unsure
        assert!(f(&[f64::NAN]).may_float_range(0.0, 1.0));
        // signed zeros are equal in IEEE order
        let zero = f(&[-0.0]);
        assert!(zero.may_float_range(0.0, 0.0) && zero.may_float_range(-0.0, -0.0));
        assert!(f(&[0.0]).may_float_range(-0.0, -0.0));
        assert!(!f(&[0.0]).may_float_range(1e-300, 1.0));
        // infinities
        let inf = f(&[f64::INFINITY, 1.0]);
        assert!(inf.may_float_range(1e308, f64::INFINITY));
        // float32 is widened exactly
        let z32 = roundtrip(&build(ColumnType::Float32, &[Value::Float32(0.1)]), 1);
        assert!(z32.may_float_range(f64::from(0.1f32), f64::from(0.1f32)));
        // the raw test is plain IEEE; `Pruner::from_cols` widens FLOAT32 bounds
        assert!(!z32.may_float_range(0.1, 0.1));
    }

    #[test]
    fn bytes_bloom_exact_and_pad_space() {
        let v = vec![Value::Bytes(b"abc".to_vec()), Value::Bytes(b"x  ".to_vec()), Value::Null];
        let z = roundtrip(&build(ColumnType::Varchar, &v), 3);
        let h = |s: &[&str], pad: bool| -> Vec<u64> {
            s.iter().map(|s| hash_bytes(if pad { trim_spaces(s.as_bytes()) } else { s.as_bytes() })).collect()
        };
        assert!(z.may_hashes(&h(&["abc"], false), false));
        assert!(z.may_hashes(&h(&["x  "], false), false), "exact");
        assert!(z.may_hashes(&h(&["x"], true), false), "pad space: 'x' = 'x  '");
        assert!(z.may_hashes(&h(&["abc   "], true), false));
        assert!(!z.may_hashes(&h(&["zzz-not-here"], false), false));
        assert!(!z.may_hashes(&[], false) && z.may_hashes(&[], true), "empty set + a NULL the mask accepts");
        // too many distinct values: no bloom, nothing pruned
        let many: Vec<Value> = (0..70000u32).map(|i| Value::Bytes(i.to_le_bytes().to_vec())).collect();
        let big = build(ColumnType::Varchar, &many);
        assert_eq!(big.kind(), KIND_NONE);
        assert!(big.may_hashes(&h(&["nope"], false), false));
    }

    #[test]
    fn decode_rejects_inconsistent_records() {
        let rec = |f: &dyn Fn(&mut Vec<u8>)| {
            let mut b = Vec::new();
            f(&mut b);
            Zone::decode(&mut ByteReader::new(&b), 4).is_err()
        };
        assert!(rec(&|b| put_u8(b, 0x80)), "unknown bit");
        assert!(rec(&|b| put_u8(b, KIND_INT)), "no range, not all NULL");
        assert!(rec(&|b| {
            put_u8(b, KIND_INT | HAS_RANGE);
            put_u64(b, 5);
            put_u64(b, 4);
        }), "min > max");
        assert!(rec(&|b| {
            put_u8(b, KIND_INT | HAS_NULLS);
            put_u32(b, 9);
        }), "more NULLs than rows");
        assert!(rec(&|b| put_u8(b, KIND_BLOOM | HAS_RANGE)));
        assert!(!rec(&|b| {
            put_u8(b, KIND_INT | HAS_NULLS);
            put_u32(b, 4);
        }), "all NULL is fine");
    }

    #[test]
    fn section_roundtrip_crc_and_shape() {
        let entries: Vec<SeriesEntry> = (0..3)
            .map(|i| SeriesEntry {
                series_id: i,
                row_count: 2,
                ts_min: 0,
                ts_max: 1,
                tags: Vec::new(),
                blocks: vec![crate::chunk::BlockRef { offset: 0, len: 0 }; 2],
            })
            .collect();
        let mut zones = Vec::new();
        for i in 0..3i64 {
            zones.push(build(ColumnType::Int64, &ints(&[Some(i), Some(i + 1)])));
            zones.push(Zone::none());
        }
        let mut buf = Vec::new();
        assert!(encode_section(&mut buf, 2, &zones));
        let zm = decode_section(&buf, &entries).unwrap();
        assert_eq!(zm.zones, zones);
        assert!(!zm.get(0, 0).unwrap().may_int_range(5, 5) && zm.get(2, 0).unwrap().may_int_range(3, 3));
        assert!(zm.get(0, 2).is_none() && zm.get(3, 0).is_none());
        // any flipped byte, a truncation or a wrong shape: no summaries
        for i in 0..buf.len() {
            let mut b = buf.clone();
            b[i] ^= 0x40;
            assert!(decode_section(&b, &entries).is_none(), "byte {i}");
        }
        assert!(decode_section(&buf[..buf.len() - 1], &entries).is_none());
        assert!(decode_section(&buf, &entries[..2]).is_none());
        // nothing useful: no section at all
        let mut empty = Vec::new();
        assert!(!encode_section(&mut empty, 2, &[Zone::none(), Zone::none()]) && empty.is_empty());
    }
}
