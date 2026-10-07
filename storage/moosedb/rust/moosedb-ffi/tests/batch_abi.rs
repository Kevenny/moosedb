//! The columnar batch API and scan projection, driven through the C ABI the
//! way `ha_moosedb.cc` will: open projected, pull batches, read raw buffers.

use std::ffi::{c_char, CStr, CString};
use std::ptr;

use moosedb::*;

fn check(s: TFStatus) -> Result<(), (TFStatus, String)> {
    if s == TFStatus::TF_OK {
        return Ok(());
    }
    let p = moosedb_last_error();
    let msg = if p.is_null() { String::new() } else { unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned() };
    unsafe { moosedb_free_str(p) };
    Err((s, msg))
}

// ts, host TAG, value DOUBLE, cnt BIGINT, note VARCHAR, ok BOOL, f FLOAT
const TYPES: [TFColumnType; 7] = [
    TFColumnType::TF_COL_TIMESTAMP,
    TFColumnType::TF_COL_TAG,
    TFColumnType::TF_COL_FLOAT64,
    TFColumnType::TF_COL_INT64,
    TFColumnType::TF_COL_VARCHAR,
    TFColumnType::TF_COL_BOOL,
    TFColumnType::TF_COL_FLOAT32,
];
const NAMES: [&str; 7] = ["ts", "host", "value", "cnt", "note", "ok", "f"];

/// What a row holds, independent of the C representation.
#[derive(Clone, Debug, PartialEq)]
struct Row {
    ts: i64,
    host: String,
    value: Option<f64>,
    cnt: Option<i64>,
    note: Option<String>,
    ok: Option<bool>,
    f: Option<f32>,
}

fn make_row(i: i64) -> Row {
    Row {
        ts: 1_000_000 * i,
        host: format!("srv{}", i % 4),
        value: (i % 5 != 0).then_some(i as f64 * 0.25),
        cnt: (i % 7 != 0).then_some(i * i),
        note: (i % 3 != 0).then(|| "n".repeat((i % 6) as usize)),
        ok: (i % 4 != 1).then_some(i % 2 == 0),
        f: (i % 9 != 0).then_some(i as f32 / 8.0),
    }
}

fn val(kind: TFColumnType, data: TFValueData) -> TFValue {
    TFValue { kind: kind as u8, is_null: false, data }
}

fn null(kind: TFColumnType) -> TFValue {
    TFValue { kind: kind as u8, is_null: true, data: TFValueData { int_val: 0 } }
}

fn text(kind: TFColumnType, s: &str) -> TFValue {
    val(kind, TFValueData { str_val: TFStr { ptr: s.as_ptr().cast(), len: s.len() as u32 } })
}

struct Table(*mut MooseDBTable);

impl Drop for Table {
    fn drop(&mut self) {
        check(unsafe { moosedb_table_close(self.0) }).unwrap();
    }
}

impl Table {
    fn create(path: &str) -> Table {
        let names: Vec<CString> = NAMES.iter().map(|n| CString::new(*n).unwrap()).collect();
        let name_ptrs: Vec<*const c_char> = names.iter().map(|c| c.as_ptr()).collect();
        let types: Vec<u8> = TYPES.iter().map(|t| *t as u8).collect();
        let interval = CString::new("1 HOUR").unwrap();
        let cfg = TFTableConfig {
            data_dir: ptr::null(),
            chunk_interval: interval.as_ptr(),
            retention_period: ptr::null(),
            compression: ptr::null(),
            compression_level: 0,
            hot_threshold: ptr::null(),
            memtable_size_bytes: 0,
            ts_column_index: 0,
            column_count: 7,
            column_names: name_ptrs.as_ptr(),
            column_types: types.as_ptr(),
            encryption_key_id: 0,
        };
        let p = CString::new(path).unwrap();
        check(unsafe { moosedb_table_create(p.as_ptr(), &cfg) }).unwrap();
        let mut t = ptr::null_mut();
        check(unsafe { moosedb_table_open(p.as_ptr(), &cfg, &mut t) }).unwrap();
        Table(t)
    }

    fn write(&self, r: &Row) {
        use TFColumnType::*;
        let mut v = [
            val(TF_COL_TIMESTAMP, TFValueData { ts_us: r.ts }),
            text(TF_COL_TAG, &r.host),
            r.value.map_or(null(TF_COL_FLOAT64), |x| val(TF_COL_FLOAT64, TFValueData { float_val: x })),
            r.cnt.map_or(null(TF_COL_INT64), |x| val(TF_COL_INT64, TFValueData { int_val: x })),
            r.note.as_deref().map_or(null(TF_COL_VARCHAR), |s| text(TF_COL_VARCHAR, s)),
            r.ok.map_or(null(TF_COL_BOOL), |x| val(TF_COL_BOOL, TFValueData { bool_val: u8::from(x) })),
            r.f.map_or(null(TF_COL_FLOAT32), |x| val(TF_COL_FLOAT32, TFValueData { float32_val: x })),
        ];
        let row = TFRow { col_count: 7, values: v.as_mut_ptr() };
        check(unsafe { moosedb_write_row(self.0, &row) }).unwrap();
    }

    fn open(
        &self,
        mask: Option<&[u8]>,
        sorted: bool,
        series: Option<&[u64]>,
    ) -> Result<*mut MooseDBScan, (TFStatus, String)> {
        let (mp, ml) = mask.map_or((ptr::null(), 0), |m| (m.as_ptr(), m.len() as u32));
        let (sp, sc) = series.map_or((ptr::null(), -1), |s| (s.as_ptr(), s.len() as i64));
        let mut scan = ptr::null_mut();
        check(unsafe { moosedb_scan_open_projected(self.0, i64::MIN, i64::MAX, sp, sc, sorted, mp, ml, &mut scan) })?;
        Ok(scan)
    }
}

/// Reads the raw buffers of one batch back into `Row`s (absent columns keep `default`).
///
/// # Safety
/// `b` must be a batch returned by the library and still valid.
unsafe fn decode(b: &TFBatch, mask: &[bool]) -> Vec<Row> {
    let n = b.row_count as usize;
    assert_eq!(b.col_count, 7);
    let cols = unsafe { std::slice::from_raw_parts(b.columns, 7) };
    let mut rows =
        vec![Row { ts: i64::MIN, host: String::new(), value: None, cnt: None, note: None, ok: None, f: None }; n];
    for (c, col) in cols.iter().enumerate() {
        assert_eq!(col.kind, TYPES[c] as u8);
        assert_eq!(col.present != 0, mask[c], "column {c}");
        if !mask[c] {
            assert!(col.nulls.is_null() && col.values.is_null() && col.starts.is_null() && col.data.is_null());
            continue;
        }
        let nulls: Option<&[u8]> = (!col.nulls.is_null()).then(|| unsafe { std::slice::from_raw_parts(col.nulls, n) });
        let is_null = |i: usize| nulls.is_some_and(|m| m[i] != 0);
        for (i, row) in rows.iter_mut().enumerate() {
            match TYPES[c] {
                TFColumnType::TF_COL_TIMESTAMP => row.ts = unsafe { *col.values.cast::<i64>().add(i) },
                TFColumnType::TF_COL_FLOAT64 => {
                    row.value = (!is_null(i)).then(|| unsafe { *col.values.cast::<f64>().add(i) });
                }
                TFColumnType::TF_COL_INT64 => {
                    row.cnt = (!is_null(i)).then(|| unsafe { *col.values.cast::<i64>().add(i) });
                }
                TFColumnType::TF_COL_BOOL => {
                    row.ok = (!is_null(i)).then(|| unsafe { *col.values.cast::<u8>().add(i) } != 0);
                }
                TFColumnType::TF_COL_FLOAT32 => {
                    row.f = (!is_null(i)).then(|| unsafe { *col.values.cast::<f32>().add(i) });
                }
                TFColumnType::TF_COL_VARCHAR | TFColumnType::TF_COL_TAG => {
                    let (s, l) = unsafe { (*col.starts.add(i) as usize, *col.lens.add(i) as usize) };
                    assert!(
                        s.checked_add(l).is_some_and(|e| e as u64 <= col.data_len),
                        "row {i} of column {c} leaves data"
                    );
                    let text = if l == 0 {
                        String::new()
                    } else {
                        unsafe { std::slice::from_raw_parts(col.data.add(s), l) }.iter().map(|&b| b as char).collect()
                    };
                    if TYPES[c] == TFColumnType::TF_COL_TAG {
                        row.host = text;
                    } else {
                        row.note = (!is_null(i)).then_some(text);
                    }
                }
                _ => unreachable!(),
            }
        }
    }
    rows
}

fn mask_row(r: &Row, mask: &[bool]) -> Row {
    Row {
        ts: if mask[0] { r.ts } else { i64::MIN },
        host: if mask[1] { r.host.clone() } else { String::new() },
        value: if mask[2] { r.value } else { None },
        cnt: if mask[3] { r.cnt } else { None },
        note: if mask[4] { r.note.clone() } else { None },
        ok: if mask[5] { r.ok } else { None },
        f: if mask[6] { r.f } else { None },
    }
}

type Pos = [u8; TF_POSITION_LEN];

/// Pulls batches until the end; returns rows and positions. Checks the
/// end-of-scan contract on the way.
fn drain(scan: *mut MooseDBScan, max: u32, backward: bool, mask: &[bool]) -> (Vec<Row>, Vec<Pos>) {
    let (mut rows, mut positions) = (Vec::new(), Vec::new());
    loop {
        let mut b = TFBatch { row_count: 0, col_count: 0, columns: ptr::null(), positions: ptr::null() };
        let mut end = false;
        let st = unsafe {
            if backward {
                moosedb_scan_prev_batch(scan, max, &mut b, &mut end)
            } else {
                moosedb_scan_next_batch(scan, max, &mut b, &mut end)
            }
        };
        check(st).unwrap();
        assert_eq!(end, b.row_count == 0, "end is reported by an empty batch only");
        if end {
            return (rows, positions);
        }
        assert!(b.row_count <= if max == 0 { 1024 } else { max });
        let got = unsafe { decode(&b, mask) };
        let pos = unsafe { std::slice::from_raw_parts(b.positions, b.row_count as usize * TF_POSITION_LEN) };
        positions.extend(pos.chunks(TF_POSITION_LEN).map(|c| <Pos>::try_from(c).unwrap()));
        // The scan's own notion of "current position" is the batch's last row.
        let mut cur = [0u8; TF_POSITION_LEN];
        check(unsafe { moosedb_scan_position(scan, cur.as_mut_ptr()) }).unwrap();
        assert_eq!(&cur[..], &pos[pos.len() - TF_POSITION_LEN..]);
        rows.extend(got);
    }
}

fn written(t: &Table, n: i64, flush_at: i64) -> Vec<Row> {
    let mut all = Vec::new();
    for i in 0..n {
        let r = make_row(i * 5 + (i % 3)); // spread over several hourly chunks
        let r = Row { ts: i * 20_000_000 + (i % 3) * 1_000_000, ..r };
        t.write(&r);
        all.push(r);
        if i + 1 == flush_at {
            check(unsafe { moosedb_flush(t.0) }).unwrap();
        }
    }
    all
}

const ALL: [bool; 7] = [true; 7];

#[test]
fn full_batches_match_what_was_written_and_positions_resolve() {
    let dir = tempfile::tempdir().unwrap();
    let t = Table::create(dir.path().join("t").to_str().unwrap());
    let mut expected = written(&t, 700, 500); // 500 in chunks, 200 in the MemTable
    expected.sort_by_key(|r| (r.ts, r.host.clone()));

    for sorted in [false, true] {
        for max in [0, 1, 13, 1000, 65_536, 1 << 30] {
            let scan = t.open(None, sorted, None).unwrap();
            let (rows, positions) = drain(scan, max, false, &ALL);
            assert_eq!(rows.len(), expected.len(), "sorted {sorted} max {max}");
            let mut got = rows.clone();
            got.sort_by_key(|r| (r.ts, r.host.clone()));
            assert_eq!(got, expected);
            if sorted {
                assert!(rows.windows(2).all(|w| w[0].ts <= w[1].ts));
            }
            // Positions are the ones rnd_pos resolves, row for row.
            let mut snap = ptr::null_mut();
            check(unsafe { moosedb_scan_snapshot(scan, &mut snap) }).unwrap();
            for (i, (r, p)) in rows.iter().zip(&positions).enumerate().step_by(17) {
                let mut full = TFRow { col_count: 0, values: ptr::null_mut() };
                check(unsafe { moosedb_snapshot_fetch(snap, p.as_ptr(), &mut full) }).unwrap();
                let vals = unsafe { std::slice::from_raw_parts(full.values, 7) };
                assert_eq!(unsafe { vals[0].data.ts_us }, r.ts, "row {i}");
                assert_eq!(vals[2].is_null, r.value.is_none(), "row {i}");
            }
            check(unsafe { moosedb_snapshot_close(snap) }).unwrap();
            check(unsafe { moosedb_scan_close(scan) }).unwrap();
        }
    }
}

#[test]
fn projection_leaves_unrequested_columns_absent_and_null() {
    let dir = tempfile::tempdir().unwrap();
    let t = Table::create(dir.path().join("t").to_str().unwrap());
    let mut expected = written(&t, 300, 200);
    expected.sort_by_key(|r| r.ts);

    for mask in [
        [false, false, true, false, false, false, false],
        [false, false, false, false, true, false, false],
        [true, true, false, false, false, false, false],
        [false, false, false, true, false, true, true],
        [false; 7],
        ALL,
    ] {
        let bytes: Vec<u8> = mask.iter().map(|&b| u8::from(b)).collect();
        let scan = t.open(Some(&bytes), true, None).unwrap();
        let (rows, _) = drain(scan, 50, false, &mask);
        let want: Vec<Row> = expected.iter().map(|r| mask_row(r, &mask)).collect();
        let mut a = rows.clone();
        let mut b = want;
        a.sort_by_key(|r| (r.ts, format!("{r:?}")));
        b.sort_by_key(|r| (r.ts, format!("{r:?}")));
        if mask[0] {
            assert_eq!(a, b, "mask {mask:?}");
        } else {
            assert_eq!(a.len(), b.len(), "mask {mask:?}");
        }
        check(unsafe { moosedb_scan_close(scan) }).unwrap();

        // The one-row calls honour the projection too: unrequested columns are NULL.
        let scan = t.open(Some(&bytes), false, None).unwrap();
        let mut row = TFRow { col_count: 0, values: ptr::null_mut() };
        let mut eof = false;
        check(unsafe { moosedb_scan_next(scan, &mut row, &mut eof) }).unwrap();
        assert!(!eof);
        let vals = unsafe { std::slice::from_raw_parts(row.values, 7) };
        for c in 0..7 {
            if !mask[c] {
                assert!(vals[c].is_null, "column {c} of mask {mask:?}");
            }
        }
        check(unsafe { moosedb_scan_close(scan) }).unwrap();
    }

    // Bad masks and pointers are errors, not crashes.
    assert_eq!(t.open(Some(&[1, 1]), false, None).unwrap_err().0, TFStatus::TF_ERR_INVALID_ARG);
    let mut scan = ptr::null_mut();
    let st = unsafe { moosedb_scan_open_projected(t.0, 0, 1, ptr::null(), -1, false, ptr::null(), 7, &mut scan) };
    assert_eq!(st, TFStatus::TF_ERR_INVALID_ARG, "NULL mask with a length");
    let st = unsafe { moosedb_scan_open_projected(t.0, 0, 1, ptr::null(), -1, false, ptr::null(), 0, ptr::null_mut()) };
    assert_eq!(st, TFStatus::TF_ERR_INVALID_ARG, "NULL out pointer");
}

#[test]
fn reverse_batches_empty_scans_and_error_contracts() {
    let dir = tempfile::tempdir().unwrap();
    let t = Table::create(dir.path().join("t").to_str().unwrap());
    let expected = written(&t, 300, 100);

    // Descending: seek_end then prev batches are the exact reverse of ascending.
    let scan = t.open(None, true, None).unwrap();
    let (asc, asc_pos) = drain(scan, 64, false, &ALL);
    check(unsafe { moosedb_scan_seek_end(scan) }).unwrap();
    let (desc, desc_pos) = drain(scan, 64, true, &ALL);
    assert_eq!(asc.len(), expected.len());
    assert_eq!(desc, asc.iter().rev().cloned().collect::<Vec<_>>());
    assert_eq!(desc_pos, asc_pos.iter().rev().copied().collect::<Vec<_>>());
    // At the beginning, further calls stay at the end.
    let (more, _) = drain(scan, 64, true, &ALL);
    assert!(more.is_empty());
    check(unsafe { moosedb_scan_close(scan) }).unwrap();

    // Backward batches need a sorted scan.
    let scan = t.open(None, false, None).unwrap();
    let mut b = TFBatch { row_count: 7, col_count: 7, columns: ptr::null(), positions: ptr::null() };
    let mut end = true;
    assert_eq!(unsafe { moosedb_scan_prev_batch(scan, 10, &mut b, &mut end) }, TFStatus::TF_ERR_UNSUPPORTED);
    assert_eq!((b.row_count, b.col_count, end), (0, 0, false), "a failed call leaves the output empty");
    assert!(b.columns.is_null() && b.positions.is_null());
    // The scan is still usable.
    assert_eq!(drain(scan, 100, false, &ALL).0.len(), expected.len());
    check(unsafe { moosedb_scan_close(scan) }).unwrap();

    // An empty series restriction: end at once, in both modes.
    for sorted in [false, true] {
        let scan = t.open(None, sorted, Some(&[])).unwrap();
        assert!(drain(scan, 100, false, &ALL).0.is_empty());
        check(unsafe { moosedb_scan_close(scan) }).unwrap();
    }

    // One series only, through the series id.
    let mut list = ptr::null_mut();
    check(unsafe { moosedb_series_list(t.0, &mut list) }).unwrap();
    let mut id = 0u64;
    let mut tags = TFRow { col_count: 0, values: ptr::null_mut() };
    check(unsafe { moosedb_series_list_get(list, 2, &mut id, &mut tags) }).unwrap();
    let host = {
        let s = unsafe { (*tags.values).data.str_val };
        String::from_utf8(unsafe { std::slice::from_raw_parts(s.ptr.cast::<u8>(), s.len as usize) }.to_vec()).unwrap()
    };
    check(unsafe { moosedb_series_list_close(list) }).unwrap();
    let scan = t.open(None, false, Some(&[id])).unwrap();
    let (rows, _) = drain(scan, 33, false, &ALL);
    assert_eq!(rows.len(), expected.iter().filter(|r| r.host == host).count());
    assert!(rows.iter().all(|r| r.host == host));
    check(unsafe { moosedb_scan_close(scan) }).unwrap();

    // NULL pointers.
    let mut end = false;
    let mut b = TFBatch { row_count: 0, col_count: 0, columns: ptr::null(), positions: ptr::null() };
    assert_eq!(unsafe { moosedb_scan_next_batch(ptr::null_mut(), 1, &mut b, &mut end) }, TFStatus::TF_ERR_INVALID_ARG);
    let scan = t.open(None, false, None).unwrap();
    assert_eq!(unsafe { moosedb_scan_next_batch(scan, 1, ptr::null_mut(), &mut end) }, TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(unsafe { moosedb_scan_next_batch(scan, 1, &mut b, ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);
    check(unsafe { moosedb_scan_close(scan) }).unwrap();
}

#[test]
fn a_tag_is_stored_once_per_series_run_in_a_batch() {
    let dir = tempfile::tempdir().unwrap();
    let t = Table::create(dir.path().join("t").to_str().unwrap());
    for i in 0..40 {
        t.write(&Row { ts: i * 1000, host: ["alpha", "beta"][(i % 2) as usize].into(), ..make_row(i) });
    }
    check(unsafe { moosedb_flush(t.0) }).unwrap();
    let mask = [false, true, false, false, false, false, false];
    let bytes: Vec<u8> = mask.iter().map(|&b| u8::from(b)).collect();
    let scan = t.open(Some(&bytes), false, None).unwrap();
    let mut b = TFBatch { row_count: 0, col_count: 0, columns: ptr::null(), positions: ptr::null() };
    let mut end = false;
    check(unsafe { moosedb_scan_next_batch(scan, 100, &mut b, &mut end) }).unwrap();
    assert_eq!(b.row_count, 40);
    let col = unsafe { &*b.columns.add(1) };
    let starts = unsafe { std::slice::from_raw_parts(col.starts, 40) };
    let distinct: std::collections::BTreeSet<_> = starts.iter().collect();
    assert_eq!(distinct.len(), 2, "one stored copy per series");
    check(unsafe { moosedb_scan_close(scan) }).unwrap();
}

/// One batch as (ts, position) per row.
fn pull(scan: *mut MooseDBScan, max: u32, backward: bool) -> Vec<(i64, Pos)> {
    let mut b = TFBatch { row_count: 0, col_count: 0, columns: ptr::null(), positions: ptr::null() };
    let mut end = false;
    let st = unsafe {
        if backward {
            moosedb_scan_prev_batch(scan, max, &mut b, &mut end)
        } else {
            moosedb_scan_next_batch(scan, max, &mut b, &mut end)
        }
    };
    check(st).unwrap();
    let rows = unsafe { decode(&b, &ALL) };
    let pos = if b.row_count == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(b.positions, b.row_count as usize * TF_POSITION_LEN) }
    };
    rows.iter().zip(pos.chunks(TF_POSITION_LEN)).map(|(r, p)| (r.ts, <Pos>::try_from(p).unwrap())).collect()
}

/// The way the handler serves rows from batches: a buffer in the current
/// direction; on a direction change it asks for the opposite batch and skips
/// the rows it had read ahead.
struct Handler {
    scan: *mut MooseDBScan,
    buf: Vec<(i64, Pos)>,
    used: usize,
    backward: bool,
    primed: bool,
}

impl Handler {
    fn step(&mut self, backward: bool, size: u32) -> Option<(i64, Pos)> {
        if self.primed && self.backward != backward {
            let ahead = self.buf.len() - self.used;
            self.buf = pull(self.scan, size + ahead as u32, backward);
            self.buf.drain(..ahead.min(self.buf.len()));
            self.used = 0;
            self.backward = backward;
        }
        if self.used == self.buf.len() {
            self.buf = pull(self.scan, size, backward);
            self.used = 0;
            self.backward = backward;
        }
        self.primed = true;
        let row = self.buf.get(self.used).copied();
        self.used += usize::from(row.is_some());
        row
    }
}

#[test]
fn handler_style_reads_ahead_and_direction_changes_match_a_model() {
    let dir = tempfile::tempdir().unwrap();
    let t = Table::create(dir.path().join("t").to_str().unwrap());
    written(&t, 400, 250); // chunks + MemTable
    let scan = t.open(None, true, None).unwrap();
    let all: Vec<(i64, Pos)> =
        std::iter::from_fn(|| Some(pull(scan, 64, false))).take_while(|b| !b.is_empty()).flatten().collect();
    let n = all.len() as isize;
    assert_eq!(n, 400);
    check(unsafe { moosedb_scan_close(scan) }).unwrap();

    let mut seed = 5u64;
    let mut rnd = |m: u64| {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % m
    };
    for walk in 0..30 {
        let scan = t.open(None, true, None).unwrap();
        let mut h = Handler { scan, buf: Vec::new(), used: 0, backward: false, primed: false };
        let mut cur: isize = -1; // index of the row the handler is on
        if walk % 3 == 0 {
            check(unsafe { moosedb_scan_seek_end(scan) }).unwrap();
            cur = n;
            h.backward = true;
            h.primed = true; // empty buffer, going backwards
        }
        for step in 0..120 {
            let size = [1u64, 3, 8, 64, 1000][rnd(5) as usize] as u32;
            let mut back = rnd(2) == 0;
            if back && cur <= 0 || !back && cur >= n - 1 {
                back = !back;
            }
            if back && cur <= 0 || !back && cur >= n - 1 {
                break;
            }
            let got = h.step(back, size).unwrap_or_else(|| panic!("walk {walk} step {step}: unexpected end"));
            cur += if back { -1 } else { 1 };
            assert_eq!(got, all[cur as usize], "walk {walk} step {step} back {back} size {size}");
        }
        check(unsafe { moosedb_scan_close(scan) }).unwrap();
    }
}

#[test]
fn seek_start_and_seek_end_continue_after_an_end_on_the_same_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let t = Table::create(dir.path().join("t").to_str().unwrap());
    written(&t, 300, 200);
    for sorted in [false, true] {
        let scan = t.open(None, sorted, None).unwrap();
        let first: Vec<_> =
            std::iter::from_fn(|| Some(pull(scan, 50, false))).take_while(|b| !b.is_empty()).flatten().collect();
        let total = first.len();
        assert!(total >= 300);
        // New rows appear after the scan opened: a rewind must not see them.
        for i in 0..40 {
            t.write(&make_row(10_000 + i));
        }
        check(unsafe { moosedb_flush(t.0) }).unwrap();
        check(unsafe { moosedb_scan_seek_start(scan) }).unwrap();
        assert_eq!(
            pull(scan, 4, false).iter().map(|r| r.0).collect::<Vec<_>>(),
            first[..4].iter().map(|r| r.0).collect::<Vec<_>>()
        );
        let rest: Vec<_> =
            std::iter::from_fn(|| Some(pull(scan, 77, false))).take_while(|b| !b.is_empty()).flatten().collect();
        assert_eq!(rest.len(), total - 4, "same snapshot, sorted {sorted}");
        // Rewinding in the middle of a scan works too, and positions agree.
        check(unsafe { moosedb_scan_seek_start(scan) }).unwrap();
        assert_eq!(pull(scan, 65_536, false), first);
        if sorted {
            // BOF, then seek_start / seek_end.
            check(unsafe { moosedb_scan_seek_end(scan) }).unwrap();
            let back: Vec<_> =
                std::iter::from_fn(|| Some(pull(scan, 64, true))).take_while(|b| !b.is_empty()).flatten().collect();
            assert_eq!(back.len(), total);
            assert_eq!(back[0], first[total - 1]);
            check(unsafe { moosedb_scan_seek_start(scan) }).unwrap();
            assert_eq!(pull(scan, 2, false), first[..2]);
            check(unsafe { moosedb_scan_seek_end(scan) }).unwrap();
            assert_eq!(pull(scan, 2, true), [first[total - 1], first[total - 2]]);
            // seek_start then prev: nothing before the first row.
            check(unsafe { moosedb_scan_seek_start(scan) }).unwrap();
            assert!(pull(scan, 5, true).is_empty());
        }
        check(unsafe { moosedb_scan_close(scan) }).unwrap();
        assert_eq!(unsafe { moosedb_scan_seek_start(ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);
    }
    // A scan opened now sees all 340 rows.
    let scan = t.open(None, false, None).unwrap();
    assert_eq!(drain(scan, 100, false, &ALL).0.len(), 380);
    check(unsafe { moosedb_scan_close(scan) }).unwrap();
}
