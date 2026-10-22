//! Aggregation inside the engine against a naive aggregation over a scan:
//! chunks, MemTable, range and row tombstones, concurrent writes / DELETE /
//! flush / compaction under an open snapshot, threads, budget, cancellation.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use moosedb_core::agg::{Agg, AggSpec, ColFilter, FuncSpec, KeyCol, KeySpec, TopN, ValueFilter};
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
    AggSpec { keys, funcs: funcs(), max_memory: 1 << 30, threads, ..AggSpec::default() }
}

/// The result of an `Agg` in the oracle's shape, in result order.
fn rows_of(agg: &Agg, by_series: bool, by_name: bool) -> Vec<(Key, Stats)> {
    let r = agg.result();
    let mut out = Vec::new();
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
        out.push(((sid, name), st));
    }
    out
}

fn groups_of(agg: &Agg, by_series: bool, by_name: bool) -> Groups {
    let mut out = Groups::new();
    for (k, st) in rows_of(agg, by_series, by_name) {
        assert!(out.insert(k, st).is_none(), "a group appears twice");
    }
    out
}

fn check(t: &Table, lo: i64, hi: i64, series: Option<&[u64]>, by_series: bool, by_name: bool) {
    let want = naive(t, lo, hi, series, by_series, by_name, None);
    let mut serial = None;
    for threads in [1, 4] {
        let mut agg = t.agg_open(lo, hi, series.map(<[u64]>::to_vec)).unwrap();
        agg.set_min_parallel_rows(0);
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
    agg.set_min_parallel_rows(0);
    // a filter without a dictionary is refused
    let mut s = spec(true, false, 2);
    s.filters = vec![ColFilter { col: 3, mask: vec![true] }];
    assert!(matches!(agg.run(&s, None), Err(Error::InvalidArg(_))));
    // dictionary phase: the distinct values of `name` (NULL included)
    let dict =
        AggSpec { keys: vec![KeySpec::Column(3)], max_memory: 1 << 30, threads: 3, ..AggSpec::default() };
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
    agg.set_min_parallel_rows(0);
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
    agg2.set_min_parallel_rows(0);
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
        agg.set_min_parallel_rows(0);
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
    agg.set_min_parallel_rows(0);
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

fn parallel_agg(t: &Table) -> Agg {
    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
    agg.set_min_parallel_rows(0);
    agg
}

#[test]
fn value_filters_match_the_naive_filter_and_never_leave_empty_groups() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    let by_bytes =
        |vals: &[&[u8]]| ValueFilter { col: 3, bytes: vals.iter().map(|v| v.to_vec()).collect(), ..Default::default() };
    let cases: Vec<(ValueFilter, Box<Accept<'_>>)> = vec![
        (by_bytes(&[b"k1"]), Box::new(|v| v.as_deref() == Some(b"k1".as_slice()))),
        (
            by_bytes(&[b"k1", b"k4", b"k1", b"", b"k9"]),
            Box::new(|v| matches!(v.as_deref(), Some(b"k1") | Some(b"k4"))),
        ),
        (by_bytes(&[]), Box::new(|_| false)),
        (by_bytes(&[b"nope"]), Box::new(|_| false)),
    ];
    for (vf, accept) in &cases {
        for (by_series, by_name) in [(false, false), (true, false), (false, true), (true, true)] {
            let want = naive(&t, i64::MIN, i64::MAX, None, by_series, by_name, Some(accept.as_ref()));
            for threads in [1, 4] {
                let mut agg = parallel_agg(&t);
                let mut sp = spec(by_series, by_name, threads);
                sp.value_filters = vec![vf.clone()];
                agg.run(&sp, None).unwrap();
                let got = groups_of(&agg, by_series, by_name);
                assert_eq!(got, want, "{vf:?} series={by_series} name={by_name} threads={threads}");
                assert!(got.values().all(|s| s.star > 0) || (!by_series && !by_name), "empty group");
            }
        }
    }
    // integer values: n = 7 or n = -500
    let mut agg = parallel_agg(&t);
    let mut sp = spec(true, false, 3);
    sp.value_filters = vec![ValueFilter { col: 2, ints: vec![7, -500, 7], ..Default::default() }];
    agg.run(&sp, None).unwrap();
    let mut s = t.scan(&ScanFilter::range(i64::MIN, i64::MAX), false).unwrap();
    let mut want = 0u64;
    while let Some((_, r)) = s.next_row().unwrap() {
        want += u64::from(matches!(r[2], Value::Int(7) | Value::Int(-500)));
    }
    assert!(want > 0);
    assert_eq!(rows_of(&agg, true, false).iter().map(|(_, s)| s.star).sum::<u64>(), want);
    // wrong kind of values, bad column
    for vf in [
        ValueFilter { col: 3, ints: vec![1], ..Default::default() },
        ValueFilter { col: 2, bytes: vec![b"x".to_vec()], ..Default::default() },
        ValueFilter { col: 4, ints: vec![1], ..Default::default() },
        ValueFilter { col: 99, ..Default::default() },
    ] {
        let mut sp = spec(false, false, 1);
        sp.value_filters = vec![vf];
        assert!(matches!(agg.run(&sp, None), Err(Error::InvalidArg(_))));
    }
}

#[test]
fn pad_space_value_filters_ignore_trailing_spaces_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    let names: [&[u8]; 9] = [b"a", b"a ", b"a   ", b" a", b"a\t", b"", b"   ", b"b", b"a b "];
    let mk = |i: usize| -> Row {
        vec![
            Value::Timestamp(base() + i as i64 * MICROS_PER_SEC),
            Value::Bytes(b"h0".to_vec()),
            Value::Int(i as i64),
            Value::Bytes(names[i % names.len()].to_vec()),
            Value::Float64(0.0),
        ]
    };
    put(&t, (0..45).map(mk));
    t.flush().unwrap();
    put(&t, (45..90).map(mk)); // MemTable too
    let count = |vals: &[&[u8]], pad: bool| -> u64 {
        let mut agg = parallel_agg(&t);
        let sp = AggSpec {
            funcs: vec![FuncSpec::CountStar],
            value_filters: vec![ValueFilter {
                col: 3,
                bytes: vals.iter().map(|v| v.to_vec()).collect(),
                pad_space: pad,
                ..Default::default()
            }],
            threads: 3,
            ..AggSpec::default()
        };
        agg.run(&sp, None).unwrap();
        agg.result().funcs[0].count[0]
    };
    // naive: how many of the 90 rows equal one of `vals` under the rule
    let naive = |vals: &[&[u8]], pad: bool| -> u64 {
        let trim = |v: &[u8]| -> Vec<u8> {
            let mut v = v.to_vec();
            while pad && v.last() == Some(&b' ') {
                v.pop();
            }
            v
        };
        (0..90).filter(|&i| vals.iter().any(|v| trim(v) == trim(names[i % names.len()]))).count() as u64
    };
    for vals in [
        vec![b"a".as_slice()],
        vec![b"a "],
        vec![b"a   ", b"b"],
        vec![b" a"],
        vec![b"a\t"],
        vec![b""],
        vec![b"   "],
        vec![b"a b"],
        vec![b"zz"],
        vec![],
    ] {
        for pad in [false, true] {
            assert_eq!(count(&vals, pad), naive(&vals, pad), "{vals:?} pad={pad}");
        }
    }
    // spot checks of the rule: 'a' = {'a', 'a ', 'a   '} (10 rows each) and not ' a' / 'a\t'
    assert_eq!(count(&[b"a"], true), 30);
    assert_eq!(count(&[b"a"], false), 10);
    assert_eq!(count(&[b""], true), 20, "'' matches '' and '   '");
    assert_eq!(count(&[b"a "], true), 30, "a set value with spaces matches 'a'");
    assert_eq!(count(&[b" a"], true), 10);
}

#[test]
fn pad_space_keys_group_by_trimmed_value_and_top_n_ties_follow_the_collation() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    // classes under PAD SPACE: {a, "a ", "a   "}, {""," "}, {"a\x01"} < {"a"}, {"a\t"}, {" a"}, {"a b"}, {"b"}, NULL
    let names: [Option<&[u8]>; 12] = [
        Some(b"a"),
        Some(b"a "),
        Some(b"a   "),
        Some(b""),
        Some(b"  "),
        Some(b"a"),
        Some(b"a	"),
        Some(b" a"),
        Some(b"a b "),
        Some(b"b"),
        Some(b"b"),
        None,
    ];
    let mk = |i: usize| -> Row {
        vec![
            Value::Timestamp(base() + i as i64 * MICROS_PER_SEC),
            Value::Bytes(format!("h{}", i % 3).into_bytes()),
            Value::Int(1),
            names[i % names.len()].map_or(Value::Null, |n| Value::Bytes(n.to_vec())),
            Value::Float64(0.0),
        ]
    };
    put(&t, (0..60).map(mk));
    t.flush().unwrap();
    put(&t, (60..120).map(mk)); // MemTable too: 10 rows per name slot
    // oracle: group by the trimmed value, shown as its shortest raw member ("a b " stays "a b ");
    // order = count, then the collation (pad with spaces, bytes)
    let trim = |v: &[u8]| v.iter().rposition(|&c| c != b' ').map_or(&v[..0], |i| &v[..=i]).to_vec();
    let mut cls: BTreeMap<Option<Vec<u8>>, (u64, usize)> = BTreeMap::new();
    for i in 0..120 {
        let raw = names[i % names.len()];
        let e = cls.entry(raw.map(trim)).or_insert((0, usize::MAX));
        e.0 += 1;
        e.1 = e.1.min(raw.map_or(0, <[u8]>::len));
    }
    // (order key, shown key, count)
    type OptBytes = Option<Vec<u8>>;
    let want: Vec<(OptBytes, OptBytes, u64)> = cls
        .into_iter()
        .map(|(t, (n, len))| {
            let shown = t.clone().map(|mut v| {
                v.resize(len, b' ');
                v
            });
            (t, shown, n)
        })
        .collect();
    let padded = |v: &Option<Vec<u8>>| -> Option<Vec<u8>> {
        v.as_ref().map(|v| {
            let mut v = v.clone();
            v.resize(8, b' ');
            v
        })
    };
    let run = |top: Option<TopN>, threads: usize| -> Vec<(Option<Vec<u8>>, u64)> {
        let mut agg = parallel_agg(&t);
        let sp = AggSpec {
            keys: vec![KeySpec::ColumnPad(3)],
            funcs: vec![FuncSpec::CountStar],
            top,
            threads,
            ..AggSpec::default()
        };
        agg.run(&sp, None).unwrap();
        let r = agg.result();
        let KeyCol::Bytes { nulls, starts, lens, data } = &r.keys[0] else { panic!() };
        (0..r.groups)
            .map(|g| {
                let k = (!nulls[g]).then(|| data[starts[g] as usize..starts[g] as usize + lens[g] as usize].to_vec());
                (k, r.funcs[0].count[g])
            })
            .collect()
    };
    for threads in [1, 3] {
        let mut all = run(None, threads);
        all.sort();
        let mut exp: Vec<_> = want.iter().map(|(_, s, n)| (s.clone(), *n)).collect();
        exp.sort();
        assert_eq!(all, exp, "groups, threads={threads}");
        for desc in [true, false] {
            let mut full = want.clone();
            full.sort_by(|x, y| {
                let o = x.2.cmp(&y.2);
                let o = if desc { o.reverse() } else { o };
                o.then_with(|| padded(&x.0).cmp(&padded(&y.0)))
            });
            let full: Vec<_> = full.into_iter().map(|(_, s, n)| (s, n)).collect();
            for limit in [0, 1, 2, 3, 5, 8, 100] {
                let got = run(Some(TopN { func: 0, desc, limit }), threads);
                assert_eq!(got, full[..limit.min(full.len())], "desc={desc} limit={limit} threads={threads}");
            }
        }
    }
    // a PAD key needs a string column
    let mut agg = parallel_agg(&t);
    let sp = AggSpec { keys: vec![KeySpec::ColumnPad(2)], funcs: vec![FuncSpec::CountStar], ..AggSpec::default() };
    assert!(matches!(agg.run(&sp, None), Err(Error::InvalidArg(_))));
}

#[test]
fn series_groups_without_an_accepted_row_are_not_returned() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    let mut agg = parallel_agg(&t);
    let dict = AggSpec { keys: vec![KeySpec::Column(3)], threads: 2, ..AggSpec::default() };
    agg.run(&dict, None).unwrap();
    let n = agg.result().groups;
    let mut sp = spec(true, false, 2);
    sp.filters = vec![ColFilter { col: 3, mask: vec![false; n] }];
    agg.run(&sp, None).unwrap();
    assert_eq!(agg.result().groups, 0);
    // without keys the implicit group stays, with count 0
    let mut sp = spec(false, false, 2);
    sp.filters = vec![ColFilter { col: 3, mask: vec![false; n] }];
    agg.run(&sp, None).unwrap();
    assert_eq!(agg.result().groups, 1);
    assert_eq!(agg.result().funcs[0].count[0], 0);
}

/// Sorts `rows` as a top-N documents: value (NULL smallest), reversed when
/// descending, ties by ascending key.
fn top_oracle(rows: &[(Key, Stats)], func: usize, desc: bool, limit: usize) -> Vec<(Key, Stats)> {
    let val = |s: &Stats| -> Option<i128> {
        match func {
            0 => Some(i128::from(s.star)),
            1 => Some(i128::from(s.cnt)),
            2 => (s.cnt > 0).then_some(s.sum),
            3 => s.min.map(i128::from),
            4 => s.max.map(i128::from),
            5 => s.tmin.map(i128::from),
            6 => s.tmax.map(i128::from),
            _ => Some(i128::from(s.cf)),
        }
    };
    let mut v = rows.to_vec();
    v.sort_by(|a, b| {
        let o = val(&a.1).cmp(&val(&b.1));
        let o = if desc { o.reverse() } else { o };
        o.then_with(|| a.0.cmp(&b.0))
    });
    v.truncate(limit);
    v
}

fn check_top(t: &Table, by_series: bool) {
    let mut agg = parallel_agg(t);
    agg.run(&spec(by_series, !by_series, 1), None).unwrap();
    let all = rows_of(&agg, by_series, !by_series);
    let groups = all.len();
    for func in 0..8 {
        for desc in [false, true] {
            for limit in [0, 1, 3, groups, groups + 5, 10_000_000] {
                let want = top_oracle(&all, func, desc, limit);
                for threads in [1, 4] {
                    let mut sp = spec(by_series, !by_series, threads);
                    sp.top = Some(TopN { func, desc, limit });
                    agg.run(&sp, None).unwrap();
                    assert_eq!(
                        rows_of(&agg, by_series, !by_series),
                        want,
                        "func={func} desc={desc} limit={limit} threads={threads} series={by_series}"
                    );
                }
            }
        }
    }
}

#[test]
fn top_n_equals_the_full_ordering_cut_at_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    check_top(&t, false);
    check_top(&t, true);
    let mut agg = parallel_agg(&t);
    let mut sp = spec(true, true, 1);
    sp.top = Some(TopN { func: 0, desc: true, limit: 3 });
    assert!(matches!(agg.run(&sp, None), Err(Error::InvalidArg(_))), "two keys");
    let mut sp = spec(true, false, 1);
    sp.top = Some(TopN { func: 99, desc: true, limit: 3 });
    assert!(matches!(agg.run(&sp, None), Err(Error::InvalidArg(_))), "function out of range");
    sp.keys = vec![];
    sp.top = Some(TopN { func: 0, desc: true, limit: 3 });
    assert!(matches!(agg.run(&sp, None), Err(Error::InvalidArg(_))), "no key");
}

fn top_series(agg: &mut Agg, func: usize, desc: bool, limit: usize, want: &[(Key, Stats)]) {
    for threads in [1, 4, 8] {
        let mut sp = spec(true, false, threads);
        sp.top = Some(TopN { func, desc, limit });
        agg.run(&sp, None).unwrap();
        assert_eq!(rows_of(agg, true, false), want, "func={func} desc={desc} threads={threads}");
    }
}

#[test]
fn many_groups_merge_by_partition_like_a_single_thread() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    // 20 000 series (over the parallel merge threshold), several chunks and the MemTable
    for k in 0..4 {
        put(&t, (k * 15_000..(k + 1) * 15_000).map(|i| row(i, 20_000)));
        if k < 3 {
            t.flush().unwrap();
        }
    }
    let mut agg = parallel_agg(&t);
    agg.run(&spec(true, false, 1), None).unwrap();
    let serial = groups_of(&agg, true, false);
    assert!(serial.len() >= 19_000);
    assert_eq!(serial, naive(&t, i64::MIN, i64::MAX, None, true, false, None));
    for threads in [2, 4, 8] {
        agg.run(&spec(true, false, threads), None).unwrap();
        assert_eq!(groups_of(&agg, true, false), serial, "threads={threads}");
    }
    // top-N across partitions
    let all = rows_of(&agg, true, false);
    for (func, desc) in [(0, true), (2, false), (3, true), (4, false)] {
        top_series(&mut agg, func, desc, 25, &top_oracle(&all, func, desc, 25));
    }
    // a name key (8 distinct values): few groups, many partitions
    let one_sp = spec(false, true, 1);
    agg.run(&one_sp, None).unwrap();
    let one = groups_of(&agg, false, true);
    let mut sp = one_sp.clone();
    sp.threads = 6;
    agg.run(&sp, None).unwrap();
    assert_eq!(groups_of(&agg, false, true), one);
}

/// Throughput on ~4 M synthetic rows per scenario: `cargo test --release -p
/// moosedb-tests agg_throughput -- --ignored --nocapture`. `AGG_BENCH_ROWS`
/// scales the row count (default 4 000 000), `AGG_BENCH_THREADS` ("1,4,8")
/// the thread counts, `AGG_BENCH_ONLY` ("B") the scenarios by label prefix.
#[test]
#[ignore]
fn agg_throughput() {
    let rows: i64 = std::env::var("AGG_BENCH_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(4_000_000);
    let threads_list: Vec<usize> = std::env::var("AGG_BENCH_THREADS")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 4, 8]);
    let fns = || vec![FuncSpec::CountStar, FuncSpec::Sum(2), FuncSpec::Min(2), FuncSpec::Max(2)];
    // (label, series, distinct names (0 = file-like names, ~rows/2), keys)
    let scenarios: Vec<(&str, i64, i64, Vec<KeySpec>)> = vec![
        ("A varchar high-card", 1000, rows / 2, vec![KeySpec::Column(3)]),
        ("B 70k small series", 70_000, 20, vec![KeySpec::Series]),
        ("C few groups (20)", 1000, 20, vec![KeySpec::Column(3)]),
        ("D series (1000)", 1000, 20, vec![KeySpec::Series]),
        ("E series + name(20)", 1000, 20, vec![KeySpec::Series, KeySpec::Column(3)]),
    ];
    let only = std::env::var("AGG_BENCH_ONLY").unwrap_or_default();
    for (label, series, names, keys) in scenarios {
        if !label.starts_with(&only) {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let t = open_new(&dir.path().join("t"));
        let per_series = rows / series;
        let total = per_series * series;
        let t0 = std::time::Instant::now();
        let step = 1_000_000 / series.max(1) + 1;
        let mut k0 = 0;
        while k0 < per_series {
            let k1 = (k0 + step).min(per_series);
            let mut b = t.begin_batch().unwrap();
            for s in 0..series {
                for k in k0..k1 {
                    let n = s * per_series + k;
                    // multiplicative hash: names repeat ~2x in the high-cardinality scenario
                    let id = (n.wrapping_mul(0x9E37_79B9_7F4A_7C15u64 as i64) as u64 >> 20) as i64 % names;
                    b.write(vec![
                        Value::Timestamp(base() + k * 10 * MICROS_PER_SEC),
                        Value::Bytes(format!("h{s}").into_bytes()),
                        Value::Int((k * 7 + s) % 1000),
                        Value::Bytes(format!("/var/lib/data/file{id}.log").into_bytes()),
                        Value::Float64(k as f64),
                    ])
                    .unwrap();
                }
            }
            b.commit(false).unwrap();
            t.flush().unwrap();
            k0 = k1;
        }
        println!("== {label}: loaded {total} rows in {:?}", t0.elapsed());
        for &threads in &threads_list {
            let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
            let s = AggSpec { keys: keys.clone(), funcs: fns(), max_memory: 4 << 30, threads, ..AggSpec::default() };
            let mut times = Vec::new();
            for _ in 0..3 {
                let t1 = std::time::Instant::now();
                agg.run(&s, None).unwrap();
                times.push(t1.elapsed().as_secs_f64());
            }
            let best = times.iter().copied().fold(f64::MAX, f64::min);
            println!(
                "{label:>22} threads {threads}: 1st {:.3} s, best {:.3} s = {:.1} M rows/s, {} groups",
                times[0],
                best,
                total as f64 / best / 1e6,
                agg.result().groups
            );
        }
        if label.starts_with('A') {
            let target = b"/var/lib/data/file0.log".to_vec();
            for &threads in &threads_list {
                let funcs = vec![FuncSpec::CountStar];
                // dictionary phase + mask (what a handler without exact filters does)
                let mut best_mask = f64::MAX;
                let mut best_exact = f64::MAX;
                let mut best_top = f64::MAX;
                let (mut n_mask, mut n_exact) = (0, 0);
                for _ in 0..3 {
                    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
                    let t1 = std::time::Instant::now();
                    let dict = AggSpec { keys: vec![KeySpec::Column(3)], max_memory: 4 << 30, threads, ..AggSpec::default() };
                    agg.run(&dict, None).unwrap();
                    let KeyCol::Bytes { nulls, starts, lens, data } = agg.result().keys[0].clone() else { panic!() };
                    let mask: Vec<bool> = (0..nulls.len())
                        .map(|i| !nulls[i] && data[starts[i] as usize..starts[i] as usize + lens[i] as usize] == target[..])
                        .collect();
                    let sp = AggSpec { funcs: funcs.clone(), filters: vec![ColFilter { col: 3, mask }], max_memory: 4 << 30, threads, ..AggSpec::default() };
                    agg.run(&sp, None).unwrap();
                    best_mask = best_mask.min(t1.elapsed().as_secs_f64());
                    n_mask = agg.result().funcs[0].count[0];
                    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
                    let t1 = std::time::Instant::now();
                    let sp = AggSpec {
                        funcs: funcs.clone(),
                        value_filters: vec![ValueFilter { col: 3, bytes: vec![target.clone()], ..Default::default() }],
                        max_memory: 4 << 30,
                        threads,
                        ..AggSpec::default()
                    };
                    agg.run(&sp, None).unwrap();
                    best_exact = best_exact.min(t1.elapsed().as_secs_f64());
                    n_exact = agg.result().funcs[0].count[0];
                    let t1 = std::time::Instant::now();
                    let sp = AggSpec {
                        keys: vec![KeySpec::Column(3)],
                        funcs: funcs.clone(),
                        top: Some(TopN { func: 0, desc: true, limit: 20 }),
                        max_memory: 4 << 30,
                        threads,
                        ..AggSpec::default()
                    };
                    agg.run(&sp, None).unwrap();
                    best_top = best_top.min(t1.elapsed().as_secs_f64());
                }
                assert_eq!(n_mask, n_exact);
                println!(
                    "{label:>22} threads {threads}: filter file=x: dictionary+mask {best_mask:.3} s, exact {best_exact:.3} s ({n_exact} rows); top-20 by COUNT {best_top:.3} s"
                );
            }
        }
        // the dictionary phase of the key column (single Column key, no function)
        if let Some(KeySpec::Column(c)) = keys.first().filter(|_| keys.len() == 1) {
            for &threads in &threads_list {
                let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
                let s = AggSpec { keys: vec![KeySpec::Column(*c)], max_memory: 4 << 30, threads, ..AggSpec::default() };
                let mut best = f64::MAX;
                for _ in 0..3 {
                    let t1 = std::time::Instant::now();
                    agg.run(&s, None).unwrap();
                    best = best.min(t1.elapsed().as_secs_f64());
                }
                println!("{label:>22} dictionary threads {threads}: best {best:.3} s, {} values", agg.result().groups);
            }
        }
    }
}

/// `KeySpec::Bucket`: the key is the epoch-aligned bucket start (floor, also before 1970 and on
/// the exact boundaries), over chunks and MemTable, on 1 and 4 threads; bad specs are refused.
#[test]
fn bucket_keys_floor_to_the_epoch_aligned_start() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"));
    build(&t, 12);
    let edge = |ts: i64| vec![Value::Timestamp(ts), Value::Bytes(b"h0".to_vec()), Value::Int(1), Value::Null, Value::Float64(0.0)];
    let m = 60 * MICROS_PER_SEC;
    put(&t, [-m - 1, -m, -1, 0, m - 1, m, 3_600 * MICROS_PER_SEC - 1, -86_400 * MICROS_PER_SEC].into_iter().map(edge));
    let mut s = t.scan(&ScanFilter::range(i64::MIN, i64::MAX), false).unwrap();
    let mut all = Vec::new();
    while let Some((_, r)) = s.next_row().unwrap() {
        let (Value::Timestamp(ts), n) = (&r[0], &r[2]) else { panic!() };
        all.push((*ts, if let Value::Int(n) = n { Some(*n) } else { None }));
    }
    for w in [1, MICROS_PER_SEC, m, 60 * m, MICROS_PER_DAY, 7] {
        let mut want: BTreeMap<i64, (u64, i128)> = BTreeMap::new();
        for &(ts, n) in &all {
            let e = want.entry(ts - ts.rem_euclid(w)).or_default();
            e.0 += 1;
            e.1 += i128::from(n.unwrap_or(0));
        }
        for threads in [1, 4] {
            let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
            agg.set_min_parallel_rows(0);
            let sp = AggSpec {
                keys: vec![KeySpec::Bucket(0, w)],
                funcs: vec![FuncSpec::CountStar, FuncSpec::Sum(2)],
                max_memory: 1 << 30,
                threads,
                ..AggSpec::default()
            };
            agg.run(&sp, None).unwrap();
            let r = agg.result();
            let KeyCol::Int { nulls, vals } = &r.keys[0] else { panic!() };
            let got: BTreeMap<i64, (u64, i128)> = (0..r.groups)
                .map(|g| {
                    assert!(!nulls[g]);
                    (vals[g], (r.funcs[0].count[g], (i128::from(r.funcs[1].sum_hi[g]) << 64) | i128::from(r.funcs[1].sum_lo[g])))
                })
                .collect();
            assert_eq!(got, want, "width={w} threads={threads}");
        }
    }
    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
    for k in [KeySpec::Bucket(0, 0), KeySpec::Bucket(0, -5), KeySpec::Bucket(2, 60), KeySpec::Bucket(1, 60)] {
        let sp = AggSpec { keys: vec![k], funcs: funcs(), max_memory: 1 << 30, threads: 1, ..AggSpec::default() };
        assert!(matches!(agg.run(&sp, None), Err(Error::InvalidArg(_))), "{k:?}");
    }
}
