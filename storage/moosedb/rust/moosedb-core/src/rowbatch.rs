//! Columnar output of a scan: up to N rows in typed buffers owned by the scan
//! and reused from one call to the next (no allocation per row once warm).
//!
//! Per projected column: a NULL map (only when the batch holds a NULL), then
//!
//! | column type            | values                                  |
//! |------------------------|-----------------------------------------|
//! | TIMESTAMP, INT64       | `u64` array (two's complement `i64`)    |
//! | FLOAT64                | `u64` array (IEEE-754 bits)             |
//! | FLOAT32                | `u32` array (IEEE-754 bits)             |
//! | BOOL                   | `u8` array (0 / 1)                      |
//! | VARCHAR, DECIMAL, TAG  | `starts[i]`/`lens[i]` into one `data`   |
//!
//! A NULL row holds a zero / empty value in the array. TAG values are stored
//! once per series run in `data`; every row of the run points at the same
//! `start`, so equal `start`s inside one batch mean equal values.

use crate::block::{ColumnData, DenseRef};
use crate::scan::POSITION_LEN;
use crate::schema::{ColumnType, Row, Schema, Value};

/// Largest number of rows a batch holds.
pub const MAX_BATCH_ROWS: usize = 1 << 16;
/// Rows per batch when the caller asks for 0.
pub const DEFAULT_BATCH_ROWS: usize = 1024;
/// Soft cap on the bytes of one variable-width column per batch: a batch ends
/// early rather than growing past it (a single row may exceed it).
const BYTES_CAP: usize = 256 << 20;
/// Above this capacity the byte buffer is released at the next batch.
const KEEP_CAPACITY: usize = 64 << 20;
const NO_RUN: u64 = u64::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    W64,
    W32,
    W8,
    Bytes,
}

fn shape_of(ty: ColumnType) -> Shape {
    match ty {
        ColumnType::Timestamp | ColumnType::Int64 | ColumnType::Float64 => Shape::W64,
        ColumnType::Float32 => Shape::W32,
        ColumnType::Bool => Shape::W8,
        ColumnType::Varchar | ColumnType::Tag | ColumnType::Decimal => Shape::Bytes,
    }
}

/// One column of a [`ScanBatch`].
pub struct BatchColumn {
    ty: ColumnType,
    shape: Shape,
    present: bool,
    has_null: bool,
    nulls: Vec<u8>,
    w64: Vec<u64>,
    w32: Vec<u32>,
    w8: Vec<u8>,
    starts: Vec<u32>,
    lens: Vec<u32>,
    data: Vec<u8>,
    run_key: u64,
    run_start: u32,
    cap: usize,
}

impl BatchColumn {
    fn new(ty: ColumnType) -> BatchColumn {
        BatchColumn {
            ty,
            shape: shape_of(ty),
            present: false,
            has_null: false,
            nulls: Vec::new(),
            w64: Vec::new(),
            w32: Vec::new(),
            w8: Vec::new(),
            starts: Vec::new(),
            lens: Vec::new(),
            data: Vec::new(),
            run_key: NO_RUN,
            run_start: 0,
            cap: BYTES_CAP,
        }
    }

    fn reset(&mut self, present: bool) {
        self.present = present;
        self.has_null = false;
        self.nulls.clear();
        self.w64.clear();
        self.w32.clear();
        self.w8.clear();
        self.starts.clear();
        self.lens.clear();
        self.data.clear();
        self.run_key = NO_RUN;
        if self.data.capacity() > KEEP_CAPACITY {
            self.data = Vec::new();
        }
    }

    pub fn column_type(&self) -> ColumnType {
        self.ty
    }

    /// Whether the column was requested by the projection. Absent columns
    /// hold nothing.
    pub fn is_present(&self) -> bool {
        self.present
    }

    /// One byte per row (non-zero = NULL), or `None` when no row is NULL.
    pub fn nulls(&self) -> Option<&[u8]> {
        self.has_null.then_some(self.nulls.as_slice())
    }

    pub fn values_w64(&self) -> &[u64] {
        &self.w64
    }

    pub fn values_w32(&self) -> &[u32] {
        &self.w32
    }

    pub fn values_w8(&self) -> &[u8] {
        &self.w8
    }

    pub fn starts(&self) -> &[u32] {
        &self.starts
    }

    pub fn lens(&self) -> &[u32] {
        &self.lens
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    fn mark_nulls(&mut self, at: usize, k: usize, src: Option<&[bool]>) {
        match src {
            Some(flags) if flags.iter().any(|&b| b) => {
                self.nulls.resize(at, 0);
                self.has_null = true;
                self.nulls.extend(flags.iter().map(|&b| u8::from(b)));
            }
            _ => {
                if self.has_null {
                    self.nulls.resize(at + k, 0);
                }
            }
        }
    }

    fn push_null(&mut self, at: usize) {
        self.nulls.resize(at, 0);
        self.has_null = true;
        self.nulls.push(1);
        match self.shape {
            Shape::W64 => self.w64.push(0),
            Shape::W32 => self.w32.push(0),
            Shape::W8 => self.w8.push(0),
            Shape::Bytes => {
                self.starts.push(self.data.len() as u32);
                self.lens.push(0);
            }
        }
    }

    fn push_value(&mut self, at: usize, v: &Value) {
        match (self.shape, v) {
            (Shape::W64, Value::Timestamp(x) | Value::Int(x)) => self.w64.push(*x as u64),
            (Shape::W64, Value::Float64(f)) => self.w64.push(f.to_bits()),
            (Shape::W32, Value::Float32(f)) => self.w32.push(f.to_bits()),
            (Shape::W8, Value::Bool(b)) => self.w8.push(u8::from(*b)),
            (Shape::Bytes, Value::Bytes(b)) => {
                self.starts.push(self.data.len() as u32);
                self.lens.push(b.len() as u32);
                self.data.extend_from_slice(b);
            }
            // NULL, or a value that does not fit the column (never stored).
            _ => return self.push_null(at),
        }
        if self.has_null {
            self.nulls.push(0);
        }
    }

    /// Whether one more row of `len` bytes keeps a variable-width column
    /// under the cap (a batch always takes at least one row).
    fn bytes_room(&self, len: usize, rows: usize) -> bool {
        rows == 0 || self.data.len().saturating_add(len) <= self.cap
    }

    /// Largest `j <= k` such that rows `[lo, lo + j)` of a variable-width
    /// column fit under the cap.
    fn fit_bytes(&self, offsets: &[u32], lo: usize, k: usize) -> usize {
        let avail = self.cap.saturating_sub(self.data.len());
        let base = offsets[lo] as usize;
        offsets[lo..=lo + k].partition_point(|&o| (o as usize - base) <= avail).saturating_sub(1)
    }

    /// Appends rows `[lo, lo + k)` of a decoded column.
    fn extend_from(&mut self, at: usize, col: &ColumnData, lo: usize, k: usize) {
        let nulls = col.null_flags().map(|f| &f[lo..lo + k]);
        match col.dense() {
            DenseRef::Ints(v) => self.w64.extend(v[lo..lo + k].iter().map(|&x| x as u64)),
            DenseRef::Bits64(v) => self.w64.extend_from_slice(&v[lo..lo + k]),
            DenseRef::Bits32(v) => self.w32.extend_from_slice(&v[lo..lo + k]),
            DenseRef::Bools(v) => self.w8.extend(v[lo..lo + k].iter().map(|&b| u8::from(b))),
            DenseRef::Bytes { offsets, data } => {
                let base = offsets[lo];
                let start = self.data.len() as u32;
                self.starts.extend(offsets[lo..lo + k].iter().map(|&o| start + (o - base)));
                self.lens.extend(offsets[lo..=lo + k].windows(2).map(|w| w[1] - w[0]));
                self.data.extend_from_slice(&data[base as usize..offsets[lo + k] as usize]);
            }
        }
        self.mark_nulls(at, k, nulls);
    }

    /// Whether the TAG value of run `key` (of `len` bytes) can be added.
    fn tag_room(&self, key: u64, len: usize, rows: usize) -> bool {
        self.run_key == key || self.bytes_room(len, rows)
    }

    /// Appends `k` rows carrying the series TAG value `v`, stored once per run.
    fn extend_tag(&mut self, at: usize, key: u64, v: &Value, k: usize) {
        match v {
            Value::Bytes(b) => {
                if self.run_key != key {
                    self.run_key = key;
                    self.run_start = self.data.len() as u32;
                    self.data.extend_from_slice(b);
                }
                self.starts.resize(at + k, self.run_start);
                self.lens.resize(at + k, b.len() as u32);
                if self.has_null {
                    self.nulls.resize(at + k, 0);
                }
            }
            _ => {
                self.nulls.resize(at, 0);
                self.has_null = true;
                self.nulls.resize(at + k, 1);
                self.starts.resize(at + k, self.data.len() as u32);
                self.lens.resize(at + k, 0);
            }
        }
    }
}

/// Up to N rows of a scan, in columns. Owned by the [`Scan`](crate::Scan) that
/// fills it; valid until that scan is used again.
pub struct ScanBatch {
    rows: usize,
    positions: Vec<[u8; POSITION_LEN]>,
    cols: Vec<BatchColumn>,
}

impl ScanBatch {
    #[cfg(test)]
    pub(crate) fn set_bytes_cap(&mut self, cap: usize) {
        for c in &mut self.cols {
            c.cap = cap;
        }
    }

    pub(crate) fn new(schema: &Schema) -> ScanBatch {
        ScanBatch {
            rows: 0,
            positions: Vec::new(),
            cols: schema.columns().iter().map(|c| BatchColumn::new(c.ty)).collect(),
        }
    }

    /// Empties the batch for a new call; `want[c]` = column `c` is projected.
    pub(crate) fn begin(&mut self, want: &[bool]) {
        self.rows = 0;
        self.positions.clear();
        for (c, w) in self.cols.iter_mut().zip(want) {
            c.reset(*w);
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// `POSITION_LEN` bytes per row, as `Position::to_bytes`.
    pub fn positions(&self) -> &[[u8; POSITION_LEN]] {
        &self.positions
    }

    pub fn columns(&self) -> &[BatchColumn] {
        &self.cols
    }

    /// Row `i` as a full `Row`; columns that are not projected read as NULL.
    pub fn row_at(&self, i: usize) -> Row {
        self.cols
            .iter()
            .map(|c| {
                if !c.present || i >= self.rows || c.nulls().is_some_and(|n| n[i] != 0) {
                    return Value::Null;
                }
                match c.ty {
                    ColumnType::Timestamp => Value::Timestamp(c.w64[i] as i64),
                    ColumnType::Int64 => Value::Int(c.w64[i] as i64),
                    ColumnType::Float64 => Value::Float64(f64::from_bits(c.w64[i])),
                    ColumnType::Float32 => Value::Float32(f32::from_bits(c.w32[i])),
                    ColumnType::Bool => Value::Bool(c.w8[i] != 0),
                    ColumnType::Varchar | ColumnType::Tag | ColumnType::Decimal => {
                        let s = c.starts[i] as usize;
                        Value::Bytes(c.data[s..s + c.lens[i] as usize].to_vec())
                    }
                }
            })
            .collect()
    }

    // ── filling (used by `scan`) ──

    /// How many of `want` rows of a chunk series window can be appended,
    /// given the byte cap of the variable-width columns. `0` = batch full.
    pub(crate) fn fit_window(&self, plan: &Plan, win: &crate::chunk_reader::Window, lo: usize, want: usize) -> usize {
        let mut k = want;
        for &c in &plan.bytes_data_cols {
            if let Some(col) = win.cols[c].as_ref() {
                if let DenseRef::Bytes { offsets, .. } = col.dense() {
                    k = k.min(self.cols[c].fit_bytes(offsets, lo, k));
                }
            }
        }
        if k == 0 && self.rows == 0 {
            1
        } else {
            k
        }
    }

    /// Whether the TAG values of run `key` fit (see [`ScanBatch::fit_window`]).
    pub(crate) fn tags_room(&self, plan: &Plan, key: u64, tags: &[Value]) -> bool {
        plan.tag_cols.iter().all(|&(c, t)| match tags.get(t) {
            Some(Value::Bytes(b)) => self.cols[c].tag_room(key, b.len(), self.rows),
            _ => true,
        })
    }

    /// Appends rows `[lo, lo + k)` of a chunk series window.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_window(
        &mut self,
        plan: &Plan,
        win: &crate::chunk_reader::Window,
        tags: &[Value],
        run_key: u64,
        source: u64,
        ordinal0: u64,
        lo: usize,
        k: usize,
    ) {
        let at = self.rows;
        for &c in &plan.data_cols {
            if let Some(col) = win.cols[c].as_ref() {
                self.cols[c].extend_from(at, col, lo, k);
            } else {
                // A loaded window always has its projected columns; stay total.
                for j in 0..k {
                    self.cols[c].push_null(at + j);
                }
            }
        }
        for &(c, t) in &plan.tag_cols {
            self.cols[c].extend_tag(at, run_key, tags.get(t).unwrap_or(&Value::Null), k);
        }
        self.positions
            .extend((0..k).map(|i| crate::scan::Position { source, ordinal: ordinal0 + (lo + i) as u64 }.to_bytes()));
        self.rows += k;
    }

    /// Whether `row` can be appended (byte cap); a batch takes at least one row.
    pub(crate) fn row_room(&self, plan: &Plan, row: &[Value]) -> bool {
        plan.bytes_cols.iter().all(|&c| match row.get(c) {
            Some(Value::Bytes(b)) => self.cols[c].bytes_room(b.len(), self.rows),
            _ => true,
        })
    }

    /// Appends one full row (MemTable rows, materialized scans).
    pub(crate) fn push_row(&mut self, plan: &Plan, row: &[Value], pos: [u8; POSITION_LEN]) {
        let at = self.rows;
        for &c in &plan.all_cols {
            self.cols[c].push_value(at, row.get(c).unwrap_or(&Value::Null));
        }
        self.positions.push(pos);
        self.rows += 1;
    }
}

/// The projection of a scan, resolved against the schema once.
#[derive(Clone, Debug)]
pub(crate) struct Plan {
    /// `want[c]`: schema column `c` is projected.
    pub want: Vec<bool>,
    /// Projected non-TAG columns (stored in data blocks).
    pub data_cols: Vec<usize>,
    /// Projected TAG columns: `(schema column, position in the tag list)`.
    pub tag_cols: Vec<(usize, usize)>,
    /// Projected columns of any kind.
    pub all_cols: Vec<usize>,
    /// Projected variable-width columns.
    pub bytes_cols: Vec<usize>,
    /// Projected variable-width non-TAG columns.
    pub bytes_data_cols: Vec<usize>,
}

impl Plan {
    pub(crate) fn new(schema: &Schema, mask: Option<&[bool]>) -> Plan {
        let n = schema.columns().len();
        let want: Vec<bool> = (0..n).map(|c| mask.map_or(true, |m| m.get(c).copied().unwrap_or(false))).collect();
        let mut p = Plan {
            want,
            data_cols: Vec::new(),
            tag_cols: Vec::new(),
            all_cols: Vec::new(),
            bytes_cols: Vec::new(),
            bytes_data_cols: Vec::new(),
        };
        for c in 0..n {
            if !p.want[c] {
                continue;
            }
            p.all_cols.push(c);
            let is_bytes = shape_of(schema.columns()[c].ty) == Shape::Bytes;
            if is_bytes {
                p.bytes_cols.push(c);
            }
            match schema.tag_position(c) {
                Some(t) => p.tag_cols.push((c, t)),
                None => {
                    p.data_cols.push(c);
                    if is_bytes {
                        p.bytes_data_cols.push(c);
                    }
                }
            }
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Column;

    fn schema() -> Schema {
        Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "h".into(), ty: ColumnType::Tag },
                Column { name: "v".into(), ty: ColumnType::Float64 },
                Column { name: "s".into(), ty: ColumnType::Varchar },
            ],
            0,
        )
        .unwrap()
    }

    #[test]
    fn rows_roundtrip_with_nulls_and_projection() {
        let schema = schema();
        let plan = Plan::new(&schema, Some(&[true, true, true, true]));
        let mut b = ScanBatch::new(&schema);
        b.begin(&plan.want);
        let rows = [
            vec![Value::Timestamp(1), Value::Bytes(b"a".to_vec()), Value::Float64(1.5), Value::Bytes(b"xy".to_vec())],
            vec![Value::Timestamp(2), Value::Null, Value::Null, Value::Bytes(vec![])],
            vec![Value::Timestamp(3), Value::Bytes(b"c".to_vec()), Value::Float64(-0.0), Value::Null],
        ];
        for (i, r) in rows.iter().enumerate() {
            assert!(b.row_room(&plan, r));
            b.push_row(&plan, r, [i as u8; POSITION_LEN]);
        }
        assert_eq!(b.rows(), 3);
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(b.row_at(i), *r);
        }
        assert!(b.columns()[0].nulls().is_none(), "no NULL timestamp, no map");
        assert_eq!(b.columns()[2].nulls(), Some(&[0u8, 1, 0][..]));

        let only_v = Plan::new(&schema, Some(&[false, false, true, false]));
        b.begin(&only_v.want);
        b.push_row(&only_v, &rows[0], [0; POSITION_LEN]);
        assert_eq!(b.row_at(0), vec![Value::Null, Value::Null, Value::Float64(1.5), Value::Null]);
        assert!(!b.columns()[1].is_present());
    }
}
