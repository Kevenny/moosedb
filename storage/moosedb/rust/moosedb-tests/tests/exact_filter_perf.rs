//! Cost of an equality predicate on a non-TAG column (`WHERE file = 'x'`), the
//! exact byte filter with late materialization, and the scans without a
//! predicate that must not slow down. Run on demand:
//! `cargo test --release -p moosedb-tests --test exact_filter_perf -- --ignored --nocapture`
//! (`MOOSE_PERF_ROWS` scales the data). Own binary: timing.

#![forbid(unsafe_code)]

use std::time::Instant;

use moosedb_core::agg::{AggSpec, FuncSpec, KeySpec};
use moosedb_core::{prune_stats, reset_prune_stats, ColPred, Column, ColumnType, RawOptions, ScanFilter, Schema, Table, TableConfig, Value};

const FILE: usize = 2;
const N: usize = 3;
const TS0: i64 = 1_800_000_000_000_000;

fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "file".into(), ty: ColumnType::Varchar },
            Column { name: "n".into(), ty: ColumnType::Int64 },
            Column { name: "pid".into(), ty: ColumnType::Int64 },
            Column { name: "cmd".into(), ty: ColumnType::Varchar },
            Column { name: "user".into(), ty: ColumnType::Varchar },
        ],
        0,
    )
    .unwrap()
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

/// `series` series, `rows` rows, a chunk per `per_chunk`; `distinct` file names.
fn build(dir: &std::path::Path, series: usize, rows: usize, per_chunk: usize, distinct: u64) -> Table {
    let cfg = TableConfig::new(dir, schema(), &RawOptions::default()).unwrap();
    Table::create(&cfg).unwrap();
    let t = Table::open(cfg).unwrap();
    let mut g = Lcg(42);
    for i in 0..rows {
        let s = i % series;
        let file = match g.next() % 1000 {
            _ if i % 100_003 == 0 => "/rare/file".to_string(),
            0 => "/popular/file".to_string(),
            _ => format!("/var/lib/some/dir/file{}", g.next() % distinct),
        };
        t.write(vec![
            Value::Timestamp(TS0 + i as i64 * 1000),
            Value::Bytes(format!("host{s}").into_bytes()),
            Value::Bytes(file.into_bytes()),
            Value::Int((g.next() % 1000) as i64),
            Value::Int((g.next() % 4000) as i64),
            Value::Bytes(format!("/usr/bin/process{} --flag=value --other", g.next() % 500).into_bytes()),
            Value::Bytes(format!("user{}", g.next() % 50).into_bytes()),
        ])
        .unwrap();
        if (i + 1) % per_chunk == 0 {
            t.flush().unwrap();
        }
    }
    t.flush().unwrap();
    t
}

fn best<T>(mut f: impl FnMut() -> T) -> (f64, T) {
    let mut best = f64::MAX;
    let mut out = None;
    let reps: usize = std::env::var("MOOSE_PERF_REPS").ok().and_then(|v| v.parse().ok()).unwrap_or(7);
    for _ in 0..reps {
        let t0 = Instant::now();
        let r = f();
        best = best.min(t0.elapsed().as_secs_f64() * 1000.0);
        out = Some(r);
    }
    (best, out.unwrap())
}

fn drain(t: &Table, f: &ScanFilter, sorted: bool) -> usize {
    drain_with(t, f, sorted, false)
}

/// `touch`: also builds every row (what a row-at-a-time consumer pays).
fn drain_with(t: &Table, f: &ScanFilter, sorted: bool, touch: bool) -> usize {
    let mut s = t.scan(f, sorted).unwrap();
    let mut n = 0;
    loop {
        let b = s.next_batch(4096, false).unwrap();
        if b.rows() == 0 {
            return n;
        }
        if touch {
            for i in 0..b.rows() {
                std::hint::black_box(b.row_at(i));
            }
        }
        n += b.rows();
    }
}

fn pred(v: &str) -> ScanFilter {
    ScanFilter { cols: vec![ColPred::Bytes { col: FILE, values: vec![v.as_bytes().to_vec()], pad_space: false }], ..Default::default() }
}

fn sum(t: &Table) -> u64 {
    let mut agg = t.agg_open(TS0, TS0 + i64::MAX / 4, None).unwrap();
    let spec = AggSpec { keys: vec![KeySpec::Series], funcs: vec![FuncSpec::Sum(N)], threads: 1, ..AggSpec::default() };
    agg.run(&spec, None).unwrap();
    agg.result().funcs[0].count.iter().sum()
}

#[test]
#[ignore = "benchmark"]
fn exact_filter_perf() {
    let rows: usize = std::env::var("MOOSE_PERF_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(4_000_000);
    // (label, series, distinct names)
    for (label, series, distinct) in [("many_series_bloom_ok", 1000usize, 20_000u64), ("few_series_no_bloom", 4, 400_000)] {
        let dir = tempfile::tempdir().unwrap();
        let t = build(&dir.path().join("t"), series, rows, rows / 8, distinct);
        for _ in 0..2 {
            drain(&t, &ScanFilter::default(), false); // warm
        }
        let nopred = std::env::var("MOOSE_PERF_NOPRED").is_ok();
        for (q, v) in [("rare", "/rare/file"), ("popular", "/popular/file"), ("absent", "/nope")].into_iter().filter(|_| !nopred) {
            reset_prune_stats();
            let (ms, n) = best(|| drain(&t, &pred(v), false));
            println!("RESULT {label} q={q} ms={ms:.1} rows={n} stats={:?}", prune_stats());
            let (ms, n) = best(|| drain_with(&t, &pred(v), false, true));
            println!("RESULT {label} q={q}_rowat ms={ms:.1} rows={n}");
        }
        if std::env::var("MOOSE_PERF_ONLY_SUM").is_ok() {
            let (ms, n) = best(|| sum(&t));
            println!("RESULT {label} q=sum_range ms={ms:.2} rows={n}");
            continue;
        }
        let (ms, n) = best(|| drain(&t, &ScanFilter::default(), false));
        println!("RESULT {label} q=full_scan ms={ms:.1} rows={n}");
        let (ms, n) = best(|| drain(&t, &ScanFilter::default(), true));
        println!("RESULT {label} q=ordered_scan ms={ms:.1} rows={n}");
        let (ms, n) = best(|| sum(&t));
        println!("RESULT {label} q=sum_range ms={ms:.1} rows={n}");
    }
}
