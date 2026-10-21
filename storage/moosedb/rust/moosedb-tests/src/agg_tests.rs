//! Aggregation inside the engine against a naive aggregation over a scan:
//! chunks, MemTable, range and row tombstones, concurrent writes / DELETE /
//! flush / compaction under an open snapshot, threads, budget, cancellation.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use moosedb_core::agg::{Agg, AggSpec, ColFilter, FuncSpec, KeyCol, KeySpec};
use moosedb_core::time::{days_from_civil, MICROS_PER_DAY, MICROS_PER_SEC};
use moosedb_core::{Column, ColumnType, Error, RawOptions, Row, ScanFilter, Schema, Table, TableConfig, Value};

const BASE_DAY: i64 = 0;

fn base() -> i64 {
    days_from_civil(2026, 10, 1) * MICROS_PER_DAY + BASE_DAY
}

/// `(ts, host TAG, n INT64, name VARCHAR, f DOUBLE)`.
fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "n".into(), ty: ColumnType::Int64 },
            Column { name: "name".into(), ty: ColumnType::Varchar },
            Column { name: "f".into(), ty: ColumnType::Float64 },
        ],
        0,
    )
    .unwrap()
}

fn open_new(dir: &Path) -> Arc<Table> {
    let cfg = TableConfig::new(dir, schema(), &RawOptions::default()).unwrap();
    Table::create(&cfg).unwrap();
    Arc::new(Table::open(cfg).unwrap())
}

fn row(i: i64, hosts: i64) -> Row {
    vec![
        Value::Timestamp(base() + ((i * 7919) % 4320) * 60 * MICROS_PER_SEC),
        Value::Bytes(format!("h{}", i % hosts).into_bytes()),
        if i % 11 == 0 { Value::Null } else { Value::Int(i * 31 % 1000 - 500) },
        if i % 13 == 0 { Value::Null } else { Value::Bytes(format!("k{}", i % 7).into_bytes()) },
        Value::Float64(i as f64),
    ]
}

fn put(t: &Arc<Table>, rows: impl Iterator<Item = Row>) {
    let mut b = t.begin_batch().unwrap();
    for r in rows {
        b.write(r).unwrap();
    }
    b.commit(true).unwrap();
}

/// 4 flushed batches (several chunks) + 2 batches left in the MemTable, then
/// deletes that hit both: a range of one host and 60 single rows.
fn build(t: &Arc<Table>, hosts: i64) {
    for k in 0..6 {
        put(t, (k * 4000..(k + 1) * 4000).map(|i| row(i, hosts)));
        if k < 4 {
            t.flush().unwrap();
        }
    }
    let h3: Vec<u64> = t
        .series()
        .unwrap()
        .into_iter()
        .filter(|(_, tags)| tags[0] == Value::Bytes(b"h3".to_vec()))
        .map(|(id, _)| id)
        .collect();
    let lo = base() + 600 * 60 * MICROS_PER_SEC;
    t.delete_range(Some(&h3), lo, lo + 1500 * 60 * MICROS_PER_SEC, None, true).unwrap();
    t.delete_rows((0..60).map(|j| row(j * 397 + 5, hosts)).collect(), true).unwrap();
}

// ─── oracle ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default)]
struct Stats {
    star: u64,
    cnt: u64,
    sum: i128,
    min: Option<i64>,
    max: Option<i64>,
    tmin: Option<i64>,
    tmax: Option<i64>,
    cf: u64,
}

type Key = (u64, Option<Vec<u8>>);
type Groups = BTreeMap<Key, Stats>;
type Accept<'a> = dyn Fn(&Option<Vec<u8>>) -> bool + 'a;

fn funcs() -> Vec<FuncSpec> {
    vec![
        FuncSpec::CountStar,
        FuncSpec::Count(2),
        FuncSpec::Sum(2),
        FuncSpec::Min(2),
        FuncSpec::Max(2),
        FuncSpec::Min(0),
        FuncSpec::Max(0),
        FuncSpec::Count(4),
    ]
}

fn sid_of(t: &Table) -> BTreeMap<Vec<u8>, u64> {
    t.series()
        .unwrap()
        .into_iter()
        .map(|(id, tags)| match &tags[0] {
            Value::Bytes(b) => (b.clone(), id),
            _ => panic!(),
        })
        .collect()
}

/// What the scan sees in `[lo, hi]` / `series`, aggregated the slow way.
fn naive(
    t: &Table,
    lo: i64,
    hi: i64,
    series: Option<&[u64]>,
    by_series: bool,
    by_name: bool,
    accept: Option<&Accept<'_>>,
) -> Groups {
    let ids = sid_of(t);
    let mut filter = ScanFilter::range(lo, hi);
    filter.series = series.map(<[u64]>::to_vec);
    let mut s = t.scan(&filter, false).unwrap();
    let mut out = Groups::new();
    while let Some((_, r)) = s.next_row().unwrap() {
        let Value::Bytes(host) = &r[1] else { panic!() };
        let name = match &r[3] {
            Value::Bytes(b) => Some(b.clone()),
            _ => None,
        };
        if accept.is_some_and(|f| !f(&name)) {
            continue;
        }
        let key = (if by_series { ids[host] } else { 0 }, if by_name { name } else { None });
        let st = out.entry(key).or_default();
        let Value::Timestamp(ts) = r[0] else { panic!() };
        st.star += 1;
        if let Value::Int(n) = r[2] {
            st.cnt += 1;
            st.sum += i128::from(n);
            st.min = Some(st.min.map_or(n, |m| m.min(n)));
            st.max = Some(st.max.map_or(n, |m| m.max(n)));
        }
        st.tmin = Some(st.tmin.map_or(ts, |m| m.min(ts)));
        st.tmax = Some(st.tmax.map_or(ts, |m| m.max(ts)));
        if !r[4].is_null() {
            st.cf += 1;
        }
    }
    if !by_series && !by_name {
        out.entry((0, None)).or_default();
    }
    out
}

fn spec(by_series: bool, by_name: bool, threads: usize) -> AggSpec {
    let mut keys = Vec::new();
    if by_series {
        keys.push(KeySpec::Series);
    }
    if by_name {
        keys.push(KeySpec::Column(3));
    }
    AggSpec { keys, funcs: funcs(), filters: Vec::new(), max_memory: 1 << 30, threads }
}

/// The result of an `Agg` in the oracle's shape.
fn groups_of(agg: &Agg, by_series: bool, by_name: bool) -> Groups {
    let r = agg.result();
    let mut out = Groups::new();
    for g in 0..r.groups {
        let mut at = 0;
        let mut sid = 0;
        let mut name = None;
        if by_series {
            let KeyCol::Series(v) = &r.keys[at] else { panic!() };
            sid = v[g];
            at += 1;
        }
        if by_name {
            let KeyCol::Bytes { nulls, starts, lens, data } = &r.keys[at] else { panic!() };
            if !nulls[g] {
                let s = starts[g] as usize;
                name = Some(data[s..s + lens[g] as usize].to_vec());
            }
        }
        let f = &r.funcs;
        let opt = |c: &moosedb_core::agg::FuncCol| (c.count[g] > 0).then_some(c.val[g]);
        let st = Stats {
            star: f[0].count[g],
            cnt: f[1].count[g],
            sum: (i128::from(f[2].sum_hi[g]) << 64) | i128::from(f[2].sum_lo[g]),
            min: opt(&f[3]),
            max: opt(&f[4]),
            tmin: opt(&f[5]),
            tmax: opt(&f[6]),
            cf: f[7].count[g],
        };
        assert!(out.insert((sid, name), st).is_none(), "a group appears twice");
    }
    out
}

fn check(t: &Table, lo: i64, hi: i64, series: Option<&[u64]>, by_series: bool, by_name: bool) {
    let want = naive(t, lo, hi, series, by_series, by_name, None);
    let mut serial = None;
    for threads in [1, 4] {
        let mut agg = t.agg_open(lo, hi, series.map(<[u64]>::to_vec)).unwrap();
        agg.run(&spec(by_series, by_name, threads), None).unwrap();
        let got = groups_of(&agg, by_series, by_name);
        assert_eq!(got, want, "series={by_series} name={by_name} threads={threads}");
        if let Some(s) = &serial {
            assert_eq!(&got, s);
        }
        serial = Some(got);
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

#[test]
fn matches_a_naive_aggregation_over_chunks_memtable_and_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    assert!(t.stats().unwrap().chunk_count >= 4);
    let (lo, hi) = (i64::MIN, i64::MAX);
    for (s, n) in [(false, false), (true, false), (false, true), (true, true)] {
        check(&t, lo, hi, None, s, n);
    }
    // a time range cutting chunks and a few series
    let ids = sid_of(&t);
    let some = [ids[&b"h1".to_vec()], ids[&b"h3".to_vec()], ids[&b"h9".to_vec()]];
    let (lo, hi) = (base() + 500 * 60 * MICROS_PER_SEC, base() + 2500 * 60 * MICROS_PER_SEC);
    for (s, n) in [(false, false), (true, false), (true, true)] {
        check(&t, lo, hi, None, s, n);
        check(&t, lo, hi, Some(&some), s, n);
    }
    // nothing matches: implicit aggregation still gives one row of zeros, grouped gives none
    check(&t, 1, 2, None, false, false);
    check(&t, 1, 2, None, true, true);
    check(&t, lo, hi, Some(&[]), false, false);
}

#[test]
fn dictionary_filters_keep_only_accepted_values_and_and_together() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
    // a filter without a dictionary is refused
    let mut s = spec(true, false, 2);
    s.filters = vec![ColFilter { col: 3, mask: vec![true] }];
    assert!(matches!(agg.run(&s, None), Err(Error::InvalidArg(_))));
    // dictionary phase: the distinct values of `name` (NULL included)
    let dict =
        AggSpec { keys: vec![KeySpec::Column(3)], funcs: vec![], filters: vec![], max_memory: 1 << 30, threads: 3 };
    agg.run(&dict, None).unwrap();
    let KeyCol::Bytes { nulls, starts, lens, data } = agg.result().keys[0].clone() else { panic!() };
    assert_eq!(nulls.len(), 8, "k0..k6 and NULL");
    assert_eq!(nulls.iter().filter(|&&n| n).count(), 1);
    let value =
        |i: usize| (!nulls[i]).then(|| data[starts[i] as usize..starts[i] as usize + lens[i] as usize].to_vec());
    let accepts = |v: &Option<Vec<u8>>| matches!(v.as_deref(), None | Some(b"k1") | Some(b"k4") | Some(b"k6"));
    let mask: Vec<bool> = (0..nulls.len()).map(|i| accepts(&value(i))).collect();
    // a mask of the wrong length is refused
    let mut bad = spec(false, false, 1);
    bad.filters = vec![ColFilter { col: 3, mask: vec![true; 3] }];
    assert!(matches!(agg.run(&bad, None), Err(Error::InvalidArg(_))));
    for (by_series, by_name) in [(false, false), (true, false), (false, true)] {
        let want = naive(&t, i64::MIN, i64::MAX, None, by_series, by_name, Some(&accepts));
        for threads in [1, 4] {
            let mut sp = spec(by_series, by_name, threads);
            sp.filters = vec![ColFilter { col: 3, mask: mask.clone() }];
            agg.run(&sp, None).unwrap();
            assert_eq!(
                groups_of(&agg, by_series, by_name),
                want,
                "series={by_series} name={by_name} threads={threads}"
            );
        }
    }
    // two filters on the same column are ANDed
    let mask2: Vec<bool> = (0..nulls.len()).map(|i| matches!(value(i).as_deref(), Some(b"k4") | Some(b"k6"))).collect();
    let both = |v: &Option<Vec<u8>>| accepts(v) && matches!(v.as_deref(), Some(b"k4") | Some(b"k6"));
    let mut sp = spec(false, true, 2);
    sp.filters = vec![ColFilter { col: 3, mask: mask.clone() }, ColFilter { col: 3, mask: mask2 }];
    agg.run(&sp, None).unwrap();
    assert_eq!(groups_of(&agg, false, true), naive(&t, i64::MIN, i64::MAX, None, false, true, Some(&both)));
}

#[test]
fn an_open_aggregation_keeps_its_snapshot_through_writes_deletes_flush_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    let want = naive(&t, i64::MIN, i64::MAX, None, true, true, None);
    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
    // everything the table can do meanwhile
    put(&t, (100_000..103_000).map(|i| row(i, 12)));
    t.delete_range(None, i64::MIN, i64::MAX, None, true).unwrap();
    t.flush().unwrap();
    put(&t, (200_000..201_000).map(|i| row(i, 12)));
    t.flush().unwrap();
    t.compact(i64::MIN, i64::MAX, base() + 400 * MICROS_PER_DAY).unwrap();
    for threads in [1, 4] {
        agg.run(&spec(true, true, threads), None).unwrap();
        assert_eq!(groups_of(&agg, true, true), want);
    }
    // a new aggregation sees the new state
    let fresh = naive(&t, i64::MIN, i64::MAX, None, true, true, None);
    let mut agg2 = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
    agg2.run(&spec(true, true, 2), None).unwrap();
    assert_eq!(groups_of(&agg2, true, true), fresh);
    assert_ne!(fresh, want);
}

#[test]
fn concurrent_writers_and_deleters_never_break_a_running_aggregation() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w = {
        let (t, stop) = (t.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut i = 500_000;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                put(&t, (i..i + 500).map(|k| row(k, 12)));
                i += 500;
                if i % 2000 == 0 {
                    t.flush().unwrap();
                    t.delete_range(None, base(), base() + 60 * MICROS_PER_SEC, None, true).unwrap();
                }
            }
        })
    };
    for _ in 0..5 {
        // the aggregation and the scan opened right before it can differ only by
        // writes in between; the aggregation itself must be self-consistent
        let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
        agg.run(&spec(true, true, 4), None).unwrap();
        let a = groups_of(&agg, true, true);
        agg.run(&spec(true, true, 1), None).unwrap();
        assert_eq!(a, groups_of(&agg, true, true), "same snapshot, same answer");
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    w.join().unwrap();
}

#[test]
fn argument_errors_budget_and_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
    let base_spec = spec(false, false, 1);
    let bad = |f: &dyn Fn(&mut AggSpec)| {
        let mut s = base_spec.clone();
        f(&mut s);
        s
    };
    for s in [
        bad(&|s| s.keys = vec![KeySpec::Column(1)]),  // TAG
        bad(&|s| s.keys = vec![KeySpec::Column(4)]),  // DOUBLE
        bad(&|s| s.keys = vec![KeySpec::Column(99)]), // out of range
        bad(&|s| s.funcs = vec![FuncSpec::Sum(3)]),   // VARCHAR
        bad(&|s| s.funcs = vec![FuncSpec::Min(4)]),   // DOUBLE
        bad(&|s| s.funcs = vec![FuncSpec::Count(1)]), // TAG
        bad(&|s| s.max_memory = 0),
    ] {
        assert!(matches!(agg.run(&s, None), Err(Error::InvalidArg(_))), "{s:?}");
    }
    // one group per series and per name needs more than 2 KiB
    let mut tiny = spec(true, true, 1);
    tiny.max_memory = 2048;
    let e = agg.run(&tiny, None).unwrap_err();
    assert!(matches!(e, Error::Full(_)) && e.to_string().contains("moosedb_agg_max_memory"), "{e}");
    tiny.threads = 4;
    assert!(matches!(agg.run(&tiny, None), Err(Error::Full(_))));
    assert_eq!(agg.result().groups, 0, "a failed run leaves no result");
    // the same handle still works afterwards
    agg.run(&spec(true, true, 4), None).unwrap();
    assert!(agg.result().groups > 12);
    // cancellation: serial and parallel
    for threads in [1, 4] {
        let mut calls = 0;
        let mut cancel = || {
            calls += 1;
            true
        };
        let e = agg.run(&spec(true, true, threads), Some(&mut cancel)).unwrap_err();
        assert!(matches!(e, Error::Cancelled(_)), "{e}");
        assert!(calls >= 1);
    }
    // a callback that never cancels changes nothing
    let mut cancel = || false;
    agg.run(&spec(true, false, 4), Some(&mut cancel)).unwrap();
    assert_eq!(groups_of(&agg, true, false), naive(&t, i64::MIN, i64::MAX, None, true, false, None));
}

/// Throughput on ~10 M synthetic rows: `cargo test --release -p moosedb-tests
/// agg_throughput -- --ignored --nocapture`.
#[test]
#[ignore]
fn agg_throughput() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    let (series, per_series) = (1000i64, 10_000i64);
    let t0 = std::time::Instant::now();
    for chunk in 0..10 {
        let mut b = t.begin_batch().unwrap();
        for s in 0..series {
            for k in chunk * per_series / 10..(chunk + 1) * per_series / 10 {
                b.write(vec![
                    Value::Timestamp(base() + k * 10 * MICROS_PER_SEC),
                    Value::Bytes(format!("h{s}").into_bytes()),
                    Value::Int((k * 7 + s) % 1000),
                    Value::Bytes(format!("name{}", (k / 50 + s) % 20).into_bytes()),
                    Value::Float64(k as f64),
                ])
                .unwrap();
            }
        }
        b.commit(false).unwrap();
        t.flush().unwrap();
    }
    println!("loaded {} rows in {:?}", series * per_series, t0.elapsed());
    let rows = (series * per_series) as f64;
    for (label, keys) in
        [("Series", vec![KeySpec::Series]), ("Column(name)", vec![KeySpec::Column(3)]), ("none", vec![])]
    {
        for threads in [1, 4] {
            let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
            let s = AggSpec {
                keys: keys.clone(),
                funcs: vec![FuncSpec::CountStar, FuncSpec::Sum(2), FuncSpec::Min(2), FuncSpec::Max(2)],
                filters: vec![],
                max_memory: 1 << 30,
                threads,
            };
            let mut best = f64::MAX;
            let mut first = 0.0;
            for _ in 0..3 {
                let t1 = std::time::Instant::now();
                agg.run(&s, None).unwrap();
                let e = t1.elapsed().as_secs_f64();
                if first == 0.0 {
                    first = e;
                }
                best = best.min(e);
            }
            println!("key {label:>13} threads {threads}: 1st run {:.3} s ({:.1} M rows/s), best {:.3} s ({:.1} M rows/s), {} groups", first, rows / first / 1e6, best, rows / best / 1e6, agg.result().groups);
        }
    }
}
