//! Read path of the core: allocation-per-row test and benchmark.
//!
//! * `batch_scans_do_not_allocate_per_row` runs by default: it counts the
//!   allocations of the scanning thread and fails if a full or range scan
//!   allocates in proportion to the rows.
//! * `read_perf` is the benchmark (ignored). Run in release:
//!
//! ```text
//! cargo test --release -p moosedb-ffi --test read_perf -- --ignored --nocapture
//! ```
//!
//! Data shape: 100 hosts x 5 metrics = 500 series of 4000 rows (2M rows),
//! `ts DATETIME(6)` every 10 s, TAG host/metric, DOUBLE value, BIGINT counter,
//! one 1-day chunk. `MOOSEDB_BENCH_SERIES` / `MOOSEDB_BENCH_ROWS` resize it.
//!
//! The counting allocator lives in `moosedb-ffi` because it needs `unsafe`.

mod common;

use std::time::Instant;

use common::count_allocs;
use moosedb_core::time::{days_from_civil, MICROS_PER_DAY};
use moosedb_core::{Column, ColumnType, RawOptions, Scan, ScanFilter, Schema, Table, TableConfig, Value};

fn env(name: &str, default: i64) -> i64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

const STEP_US: i64 = 10_000_000;
const COLS: usize = 5; // ts, host, metric, value, counter

fn build(dir: &std::path::Path, hosts: i64, rows: i64) -> (Table, i64) {
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
    let mut cfg = TableConfig::new(dir.join("t"), schema, &raw).unwrap();
    cfg.opts.memtable_size_bytes = 1 << 31;
    Table::create(&cfg).unwrap();
    let t = Table::open(cfg).unwrap();
    let base = days_from_civil(2026, 1, 1) * MICROS_PER_DAY;
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
    (t, base)
}

/// Legacy path: one `Vec<Value>` per row.
fn drain_rows(mut s: Scan) -> (u64, f64) {
    let (mut n, mut sum) = (0u64, 0.0);
    while let Some((_, row)) = s.next_row().unwrap() {
        n += 1;
        if let Value::Float64(v) = row[3] {
            sum += v;
        }
    }
    (n, sum)
}

/// Batch path: touches every projected column the way a consumer would
/// (sums `value`, `counter` and `ts`, and the TAG lengths).
fn drain_batches(mut s: Scan, max: usize) -> (u64, f64) {
    let (mut n, mut sum, mut other) = (0u64, 0.0f64, 0u64);
    loop {
        let b = s.next_batch(max, false).unwrap();
        if b.rows() == 0 {
            return (n, sum + (other & 1) as f64 * 0.0);
        }
        n += b.rows() as u64;
        let c = b.columns();
        if c[3].is_present() {
            sum += c[3].values_w64().iter().map(|&x| f64::from_bits(x)).sum::<f64>();
        }
        if c[4].is_present() {
            other = other.wrapping_add(c[4].values_w64().iter().fold(0u64, |a, &x| a.wrapping_add(x)));
        }
        if c[0].is_present() {
            other = other.wrapping_add(c[0].values_w64().iter().fold(0u64, |a, &x| a.wrapping_add(x)));
        }
        for t in [1, 2] {
            if c[t].is_present() {
                other = other.wrapping_add(c[t].lens().iter().map(|&l| u64::from(l)).sum::<u64>());
            }
        }
    }
}

fn mask(cols: &[usize]) -> Vec<bool> {
    (0..COLS).map(|c| cols.contains(&c)).collect()
}

/// Best of `reps` timed runs (ns per returned row, rows, checksum), plus the
/// allocations per returned row of one more run.
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
fn read_perf() {
    let hosts = env("MOOSEDB_BENCH_SERIES", 500) / 5;
    let rows = env("MOOSEDB_BENCH_ROWS", 4000);
    let dir = tempfile::tempdir().unwrap();
    let (t, base) = build(dir.path(), hosts, rows);
    println!("BENCH data: {} series x {rows} rows", hosts * 5);
    let hour = ScanFilter::range(base + (rows / 2) * STEP_US, base + (rows / 2) * STEP_US + 3600 * 1_000_000 - 1);
    let all = ScanFilter::default();
    let full = |f: &ScanFilter, sorted: bool, m: Option<&[bool]>| t.scan_projected(f, sorted, m).unwrap();

    report("legacy next_row full (all columns)", measure(5, || drain_rows(full(&all, false, None))));
    report("legacy next_row range 1h", measure(5, || drain_rows(full(&hour, false, None))));
    report("legacy next_row sorted full", measure(3, || drain_rows(full(&all, true, None))));
    report("legacy next_row sorted range 1h", measure(3, || drain_rows(full(&hour, true, None))));

    println!("BENCH --- batches of 1024 ---");
    let value = mask(&[3]);
    let counter = mask(&[4]);
    let ts_only = mask(&[0]);
    let tags = mask(&[1, 2]);
    let q1 = mask(&[3, 4]);
    let proj: [(&str, Option<&[bool]>); 7] = [
        ("all columns", None),
        ("value only", Some(&value)),
        ("counter only", Some(&counter)),
        ("ts only", Some(&ts_only)),
        ("host+metric (TAG) only", Some(&tags)),
        ("value+counter", Some(&q1)),
        ("no columns (COUNT(*))", Some(&[false; COLS])),
    ];
    for (name, m) in proj {
        report(&format!("batch full, {name}"), measure(5, || drain_batches(full(&all, false, m), 1024)));
    }
    for (name, m) in proj {
        report(&format!("batch range 1h, {name}"), measure(5, || drain_batches(full(&hour, false, m), 1024)));
    }
    report("batch sorted full, all columns", measure(3, || drain_batches(full(&all, true, None), 1024)));
    report("batch sorted full, value only", measure(3, || drain_batches(full(&all, true, Some(&value)), 1024)));
    report("batch sorted range 1h, all columns", measure(3, || drain_batches(full(&hour, true, None), 1024)));
    report("batch sorted range 1h, value only", measure(3, || drain_batches(full(&hour, true, Some(&value)), 1024)));
    for max in [16, 128, 1024, 8192, 65_536] {
        report(
            &format!("batch full value only, max_rows {max}"),
            measure(3, || drain_batches(full(&all, false, Some(&value)), max)),
        );
    }
    println!("BENCH --- the same one-row calls on the new path ---");
    report("next_row full (all columns, new path)", measure(3, || drain_rows(full(&all, false, None))));
}

/// A scan allocates per series (decoded columns) and per batch call at most,
/// never per row.
#[test]
fn batch_scans_do_not_allocate_per_row() {
    let (series_hosts, rows) = (4, 3000);
    let dir = tempfile::tempdir().unwrap();
    let (t, base) = build(dir.path(), series_hosts, rows);
    let total = (series_hosts * 5 * rows) as f64;
    let mid = ScanFilter::range(base + 1000 * STEP_US, base + 1999 * STEP_US);
    let value = mask(&[3]);
    let cases: Vec<(&str, ScanFilter, bool, Option<Vec<bool>>)> = vec![
        ("full, all columns", ScanFilter::default(), false, None),
        ("full, value only", ScanFilter::default(), false, Some(value.clone())),
        ("range, all columns", mid.clone(), false, None),
        ("range, value only", mid.clone(), false, Some(value.clone())),
        ("sorted full, all columns", ScanFilter::default(), true, None),
        ("sorted range, value only", mid, true, Some(value)),
    ];
    for (name, filter, sorted, m) in cases {
        let scan = t.scan_projected(&filter, sorted, m.as_deref()).unwrap();
        let ((n, _), allocs) = count_allocs(|| drain_batches(scan, 1024));
        assert!(n > 0, "{name}");
        let per_row = allocs as f64 / n as f64;
        println!("{name}: {n} rows, {allocs} allocations, {per_row:.5}/row (table of {total} rows)");
        assert!(per_row < 0.02, "{name}: {allocs} allocations for {n} rows");
        assert!(allocs > 0, "the counter must be live");
    }
}
