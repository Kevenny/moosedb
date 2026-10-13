//! The same read benchmark through the C ABI (ignored). Run in release:
//!
//! ```text
//! cargo test --release -p moosedb-ffi --test read_perf_ffi -- --ignored --nocapture
//! ```
//!
//! Same data as `read_perf.rs`: 500 series x 4000 rows, one 1-day chunk.

mod common;

use std::ffi::{c_char, CString};
use std::ptr;
use std::time::Instant;

use common::count_allocs;
use moosedb::*;

fn env(name: &str, default: i64) -> i64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn ok(s: TFStatus) {
    if s != TFStatus::TF_OK {
        let p = moosedb_last_error();
        let msg = if p.is_null() {
            String::new()
        } else {
            unsafe { std::ffi::CStr::from_ptr(p) }.to_string_lossy().into_owned()
        };
        panic!("{s:?}: {msg}");
    }
}

const STEP_US: i64 = 10_000_000;
const COLS: usize = 5;
/// Loads the data through the core (its MemTable may be larger than the FFI
/// accepts, which keeps the data in one chunk), then opens it through the ABI.
fn build(path: &str, hosts: i64, rows: i64) -> (*mut MooseDBTable, i64) {
    use moosedb_core::{Column, ColumnType, RawOptions, Schema, Table, TableConfig, Value};
    let schema = Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "metric".into(), ty: ColumnType::Tag },
            Column { name: "value".into(), ty: ColumnType::Float64 },
            Column { name: "counter".into(), ty: ColumnType::Int64 },
        ],
        0,
    )
    .unwrap();
    let raw = RawOptions { compression: Some("ZSTD"), chunk_interval: Some("1 DAY"), ..RawOptions::default() };
    let mut cfg = TableConfig::new(path, schema, &raw).unwrap();
    cfg.opts.memtable_size_bytes = 1 << 31;
    Table::create(&cfg).unwrap();
    let base = moosedb_core::time::days_from_civil(2026, 1, 1) * moosedb_core::time::MICROS_PER_DAY;
    {
        let t = Table::open(cfg).unwrap();
        for h in 0..hosts {
            for (k, m) in ["cpu", "mem", "disk", "net", "load"].iter().enumerate() {
                for s in 0..rows {
                    let v = 50.0 + 25.0 * ((s as f64) / 50.0 + h as f64).sin() + ((s * 7919 + h) % 13) as f64 / 10.0;
                    t.write(vec![
                        Value::Timestamp(base + s * STEP_US),
                        Value::Bytes(format!("host-{h}").into_bytes()),
                        Value::Bytes(m.as_bytes().to_vec()),
                        Value::Float64(v),
                        Value::Int(s * (k as i64 + 1) * 3 + h * 1000),
                    ])
                    .unwrap();
                }
            }
        }
        t.flush().unwrap();
    }
    use TFColumnType::*;
    let names: Vec<CString> =
        ["ts", "host", "metric", "value", "counter"].iter().map(|n| CString::new(*n).unwrap()).collect();
    let name_ptrs: Vec<*const c_char> = names.iter().map(|c| c.as_ptr()).collect();
    let types = [TF_COL_TIMESTAMP as u8, TF_COL_TAG as u8, TF_COL_TAG as u8, TF_COL_FLOAT64 as u8, TF_COL_INT64 as u8];
    let (interval, comp) = (CString::new("1 DAY").unwrap(), CString::new("ZSTD").unwrap());
    let cfg = TFTableConfig {
        data_dir: ptr::null(),
        chunk_interval: interval.as_ptr(),
        retention_period: ptr::null(),
        compression: comp.as_ptr(),
        compression_level: 0,
        hot_threshold: ptr::null(),
        memtable_size_bytes: 0,
        ts_column_index: 0,
        column_count: 5,
        column_names: name_ptrs.as_ptr(),
        column_types: types.as_ptr(),
        encryption_key_id: 0,
        unique: false,
        unique_extra_count: 0,
        unique_extra_cols: std::ptr::null(),
    };
    let p = CString::new(path).unwrap();
    let mut t = ptr::null_mut();
    ok(unsafe { moosedb_table_open(p.as_ptr(), &cfg, &mut t) });
    (t, base)
}

fn open(t: *mut MooseDBTable, lo: i64, hi: i64, sorted: bool, mask: Option<&[bool]>) -> *mut MooseDBScan {
    let mut scan = ptr::null_mut();
    // BEGIN-BATCH
    if let Some(m) = mask {
        let bytes: Vec<u8> = m.iter().map(|&b| u8::from(b)).collect();
        ok(unsafe {
            moosedb_scan_open_projected(
                t,
                lo,
                hi,
                ptr::null(),
                -1,
                sorted,
                bytes.as_ptr(),
                bytes.len() as u32,
                &mut scan,
            )
        });
        return scan;
    }
    // END-BATCH
    let _ = mask;
    ok(unsafe { moosedb_scan_open_filtered(t, lo, hi, ptr::null(), -1, sorted, &mut scan) });
    scan
}

// BEGIN-LEGACY
/// One `moosedb_scan_next` per row, summing `value` like the handler does.
fn drain_rows(scan: *mut MooseDBScan) -> (u64, f64) {
    let (mut n, mut sum) = (0u64, 0.0);
    let mut row = TFRow { col_count: 0, values: ptr::null_mut() };
    let mut eof = false;
    loop {
        ok(unsafe { moosedb_scan_next(scan, &mut row, &mut eof) });
        if eof {
            break;
        }
        let vals = unsafe { std::slice::from_raw_parts(row.values, row.col_count as usize) };
        n += 1;
        if !vals[3].is_null {
            sum += unsafe { vals[3].data.float_val };
        }
    }
    ok(unsafe { moosedb_scan_close(scan) });
    (n, sum)
}
// END-LEGACY

// BEGIN-BATCH
/// One `moosedb_scan_next_batch` per `max` rows, reading the raw buffers.
fn drain_batches(scan: *mut MooseDBScan, max: u32) -> (u64, f64) {
    let (mut n, mut sum, mut other) = (0u64, 0.0f64, 0u64);
    let mut b = TFBatch { row_count: 0, col_count: 0, columns: ptr::null(), positions: ptr::null() };
    let mut end = false;
    loop {
        ok(unsafe { moosedb_scan_next_batch(scan, max, &mut b, &mut end) });
        if end {
            break;
        }
        let rows = b.row_count as usize;
        n += rows as u64;
        let cols = unsafe { std::slice::from_raw_parts(b.columns, b.col_count as usize) };
        if cols[3].present != 0 {
            let v = unsafe { std::slice::from_raw_parts(cols[3].values.cast::<f64>(), rows) };
            sum += v.iter().sum::<f64>();
        }
        if cols[4].present != 0 {
            let v = unsafe { std::slice::from_raw_parts(cols[4].values.cast::<i64>(), rows) };
            other = other.wrapping_add(v.iter().fold(0i64, |a, &x| a.wrapping_add(x)) as u64);
        }
        if cols[0].present != 0 {
            let v = unsafe { std::slice::from_raw_parts(cols[0].values.cast::<i64>(), rows) };
            other = other.wrapping_add(v.iter().fold(0i64, |a, &x| a.wrapping_add(x)) as u64);
        }
        for t in [1, 2] {
            if cols[t].present != 0 {
                let l = unsafe { std::slice::from_raw_parts(cols[t].lens, rows) };
                other = other.wrapping_add(l.iter().map(|&x| u64::from(x)).sum::<u64>());
            }
        }
    }
    ok(unsafe { moosedb_scan_close(scan) });
    (n, sum + (other & 1) as f64 * 0.0)
}
// END-BATCH

fn measure(reps: usize, mut run: impl FnMut() -> (u64, f64)) -> (f64, f64, u64, f64) {
    let mut best = f64::MAX;
    let (mut rows, mut sum) = (0, 0.0);
    for _ in 0..reps {
        let t0 = Instant::now();
        (rows, sum) = run();
        best = best.min(t0.elapsed().as_secs_f64());
    }
    let (_, allocs) = count_allocs(&mut run);
    (best * 1e9 / rows.max(1) as f64, allocs as f64 / rows.max(1) as f64, rows, sum)
}

fn report(name: &str, r: (f64, f64, u64, f64)) {
    println!("BENCH {name:46} {:8.1} ns/row  {:8.4} mallocs/row  rows={} sum={:.3}", r.0, r.1, r.2, r.3);
}

#[test]
#[ignore]
fn read_perf_ffi() {
    let hosts = env("MOOSEDB_BENCH_SERIES", 500) / 5;
    let rows = env("MOOSEDB_BENCH_ROWS", 4000);
    let dir = tempfile::tempdir().unwrap();
    let (t, base) = build(dir.path().join("t").to_str().unwrap(), hosts, rows);
    println!("BENCH ffi data: {} series x {rows} rows", hosts * 5);
    let mid = base + (rows / 2) * STEP_US;
    let (hour_lo, hour_hi) = (mid, mid + 3600 * 1_000_000 - 1);

    // BEGIN-LEGACY
    report("ffi legacy scan_next full", measure(5, || drain_rows(open(t, i64::MIN, i64::MAX, false, None))));
    report("ffi legacy scan_next range 1h", measure(5, || drain_rows(open(t, hour_lo, hour_hi, false, None))));
    report("ffi legacy scan_next sorted full", measure(3, || drain_rows(open(t, i64::MIN, i64::MAX, true, None))));
    // END-LEGACY

    // BEGIN-BATCH
    let m = |cols: &[usize]| -> Vec<bool> { (0..COLS).map(|c| cols.contains(&c)).collect() };
    let value = m(&[3]);
    let counter = m(&[4]);
    let tags = m(&[1, 2]);
    let q1 = m(&[3, 4]);
    let cases: [(&str, Option<&[bool]>); 6] = [
        ("all columns", None),
        ("value only", Some(&value)),
        ("counter only", Some(&counter)),
        ("host+metric (TAG) only", Some(&tags)),
        ("value+counter", Some(&q1)),
        ("no columns (COUNT(*))", Some(&[false; COLS])),
    ];
    for (name, mask) in cases {
        report(
            &format!("ffi batch full, {name}"),
            measure(5, || drain_batches(open(t, i64::MIN, i64::MAX, false, mask), 1024)),
        );
    }
    for (name, mask) in cases {
        report(
            &format!("ffi batch range 1h, {name}"),
            measure(5, || drain_batches(open(t, hour_lo, hour_hi, false, mask), 1024)),
        );
    }
    report(
        "ffi batch sorted full, value only",
        measure(3, || drain_batches(open(t, i64::MIN, i64::MAX, true, Some(&value)), 1024)),
    );
    report("ffi scan_next full on the new path", measure(3, || drain_rows(open(t, i64::MIN, i64::MAX, false, None))));
    // END-BATCH
    ok(unsafe { moosedb_table_close(t) });
}
