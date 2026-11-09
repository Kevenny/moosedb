//! Zone maps and column Blooms: pruned scans and aggregations give exactly the
//! rows an unpruned one gives (model test over random data with NaN, signed
//! zeros, infinities, NULLs, padded strings, deletes, MemTable rows and
//! compaction), chunks written by older code are read unchanged, and a damaged
//! summary is never served.

use std::collections::BTreeMap;

use super::helpers::*;
use moosedb_core::agg::{AggSpec, FuncSpec, KeyCol, KeySpec, ValueFilter};
use moosedb_core::time::MICROS_PER_SEC;
use moosedb_core::{prune_stats, ColPred, RawOptions, Row, ScanFilter, Table, TableConfig, Value};

/// Columns of the helper schema.
const VALUE: usize = 3;
const INT: usize = 4;
const NOTE: usize = 5;

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn trim(b: &[u8]) -> &[u8] {
    let n = b.iter().rposition(|&c| c != b' ').map_or(0, |i| i + 1);
    &b[..n]
}

/// What the SQL layer would decide for a row (NULL and NaN never match).
fn matches(r: &Row, p: &ColPred) -> bool {
    match p {
        ColPred::Int { col, lo, hi } => matches!(&r[*col], Value::Int(v) if lo <= v && v <= hi),
        ColPred::Float { col, lo, hi } => matches!(&r[*col], Value::Float64(v) if *lo <= *v && *v <= *hi),
        ColPred::Bytes { col, values, pad_space } => match &r[*col] {
            Value::Bytes(b) => values.iter().any(|x| if *pad_space { trim(x) == trim(b) } else { x == b }),
            _ => false,
        },
    }
}

fn model_row(g: &mut Lcg, host: u64, sec: i64) -> Row {
    let value = match g.below(40) {
        0 => Value::Float64(f64::NAN),
        1 => Value::Float64(-0.0),
        2 => Value::Float64(0.0),
        3 => Value::Float64(f64::INFINITY),
        4 => Value::Float64(f64::NEG_INFINITY),
        5 => Value::Null,
        _ => Value::Float64(host as f64 * 100.0 + (g.below(2000) as f64) / 100.0 - 10.0),
    };
    let int = if g.below(15) == 0 { Value::Null } else { Value::Int(host as i64 * 1000 + g.below(300) as i64 - 100) };
    let note = match g.below(12) {
        0 => Value::Null,
        1 => Value::Bytes(Vec::new()),
        _ => Value::Bytes(format!("n{}-{}{}", host, g.below(4), " ".repeat(g.below(3) as usize)).into_bytes()),
    };
    vec![
        Value::Timestamp(base_ts() + sec * MICROS_PER_SEC),
        Value::Bytes(format!("h{host}").into_bytes()),
        Value::Bytes(b"cpu".to_vec()),
        value,
        int,
        note,
    ]
}

fn scan_rows(t: &Table, f: &ScanFilter, sorted: bool) -> Vec<String> {
    let mut s = t.scan(f, sorted).unwrap();
    let mut out = Vec::new();
    while let Some((_, r)) = s.next_row().unwrap() {
        out.push(format!("{r:?}"));
    }
    out
}

fn rows_matching(t: &Table, f: &ScanFilter, sorted: bool, preds: &[ColPred]) -> Vec<String> {
    let mut s = t.scan(f, sorted).unwrap();
    let mut out = Vec::new();
    while let Some((_, r)) = s.next_row().unwrap() {
        if preds.iter().all(|p| matches(&r, p)) {
            out.push(format!("{r:?}"));
        }
    }
    out.sort();
    out
}

fn random_pred(g: &mut Lcg) -> ColPred {
    let num = |g: &mut Lcg| match g.below(8) {
        0 => f64::NEG_INFINITY,
        1 => f64::INFINITY,
        2 => 0.0,
        3 => -0.0,
        4 => f64::NAN,
        _ => g.below(700) as f64 - 10.0,
    };
    match g.below(3) {
        0 => {
            let c = g.below(6) as i64 * 1000 + g.below(300) as i64 - 100;
            let w = [0, 1, 5, 150, 2500, i64::MAX][g.below(6) as usize];
            ColPred::Int { col: INT, lo: c.saturating_sub(w / 2), hi: c.saturating_add(w / 2) }
        }
        1 => {
            let (a, b) = (num(g), num(g));
            ColPred::Float { col: VALUE, lo: a, hi: if g.below(3) == 0 { a } else { b } }
        }
        _ => {
            let n = 1 + g.below(3);
            let values = (0..n)
                .map(|_| match g.below(10) {
                    0 => Vec::new(),
                    1 => b"absent".to_vec(),
                    _ => format!("n{}-{}{}", g.below(6), g.below(4), " ".repeat(g.below(3) as usize)).into_bytes(),
                })
                .collect();
            ColPred::Bytes { col: NOTE, values, pad_space: g.below(2) == 0 }
        }
    }
}

fn check_predicates(t: &Table, g: &mut Lcg, rounds: usize) {
    let all = ScanFilter::default();
    for _ in 0..rounds {
        let preds: Vec<ColPred> = (0..1 + g.below(2)).map(|_| random_pred(g)).collect();
        let pruned = ScanFilter { cols: preds.clone(), ..ScanFilter::default() };
        for sorted in [false, true] {
            assert_eq!(
                rows_matching(t, &pruned, sorted, &preds),
                rows_matching(t, &all, sorted, &preds),
                "sorted={sorted} {preds:?}"
            );
        }
    }
}

fn run_model(opts: impl Fn() -> RawOptions<'static>) {
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), opts());
    let mut g = Lcg(0x5eed);
    let mut sec = 0i64;
    for round in 0..4 {
        for _ in 0..1500 {
            sec += 1 + g.below(3) as i64;
            let host = g.below(6);
            t.write(model_row(&mut g, host, sec)).unwrap();
        }
        if round < 3 {
            t.flush().unwrap();
        }
    }
    // a range delete on one host and a few single rows
    let h2: Vec<u64> = t
        .series()
        .unwrap()
        .into_iter()
        .filter(|(_, tags)| tags[0] == Value::Bytes(b"h2".to_vec()))
        .map(|(id, _)| id)
        .collect();
    let lo = base_ts() + 300 * MICROS_PER_SEC;
    t.delete_range(Some(&h2), lo, lo + 900 * MICROS_PER_SEC, None, true).unwrap();
    assert!(t.chunks().unwrap().iter().all(|c| c.zones.is_some()), "flushed chunks carry summaries");

    let before = prune_stats();
    check_predicates(&t, &mut g, 120);
    assert!(prune_stats().entries_tested > before.entries_tested);
    assert!(
        prune_stats().pruned_by_zone + prune_stats().pruned_by_bloom > before.pruned_by_zone + before.pruned_by_bloom,
        "the model must actually prune something"
    );

    // OPTIMIZE rewrites with summaries computed again from the live rows
    t.flush().unwrap();
    t.compact(i64::MIN, i64::MAX, base_ts()).unwrap();
    assert!(t.chunks().unwrap().iter().all(|c| c.zones.is_some()));
    check_predicates(&t, &mut g, 60);
    // and after a reopen (summaries come back from disk)
    drop(t);
    let t = Table::open(config(&dir.path().join("t"), opts())).unwrap();
    assert!(t.chunks().unwrap().iter().all(|c| c.zones.is_some()));
    check_predicates(&t, &mut g, 60);
}

#[test]
fn pruned_scan_equals_unpruned_scan() {
    run_model(RawOptions::default);
}

#[test]
fn pruned_scan_equals_unpruned_scan_encrypted() {
    crate::encryption_tests::install();
    run_model(|| RawOptions { encryption_key_id: Some(7), ..Default::default() });
}

#[test]
fn nothing_is_pruned_on_the_memtable_and_invalid_predicates_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), RawOptions::default());
    for i in 0..10 {
        t.write(row(i, "srv1", i as f64)).unwrap();
    }
    let f = |cols| ScanFilter { cols, ..ScanFilter::default() };
    let miss = ColPred::Int { col: INT, lo: 1000, hi: 2000 };
    assert_eq!(scan_rows(&t, &f(vec![miss.clone()]), false).len(), 10, "MemTable rows are all returned");
    t.flush().unwrap();
    assert_eq!(scan_rows(&t, &f(vec![miss]), false).len(), 0, "the chunk is skipped");
    // wrong type, TAG column, out of range
    for bad in [
        ColPred::Float { col: INT, lo: 0.0, hi: 1.0 },
        ColPred::Int { col: 1, lo: 0, hi: 1 },
        ColPred::Bytes { col: VALUE, values: vec![], pad_space: false },
        ColPred::Int { col: 99, lo: 0, hi: 1 },
    ] {
        assert!(t.scan(&f(vec![bad.clone()]), false).is_err(), "{bad:?}");
    }
    // the table timestamp is not summarized: accepted, prunes nothing
    let ts = ColPred::Int { col: 0, lo: 0, hi: 1 };
    assert_eq!(scan_rows(&t, &f(vec![ts]), false).len(), 10);
}

#[test]
fn float32_constants_are_widened_to_both_roundings() {
    use moosedb_core::{Column, ColumnType, Schema, TableConfig};
    let schema = Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "h".into(), ty: ColumnType::Tag },
            Column { name: "f".into(), ty: ColumnType::Float32 },
        ],
        0,
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = TableConfig::new(dir.path().join("t"), schema, &RawOptions::default()).unwrap();
    Table::create(&cfg).unwrap();
    let t = Table::open(cfg).unwrap();
    t.write(vec![Value::Timestamp(base_ts()), Value::Bytes(b"a".to_vec()), Value::Float32(0.1)]).unwrap();
    t.flush().unwrap();
    // 0.1 as a double is below the float 0.1: SQL may compare either way
    let f = ScanFilter { cols: vec![ColPred::Float { col: 2, lo: 0.1, hi: 0.1 }], ..ScanFilter::default() };
    assert_eq!(scan_rows(&t, &f, false).len(), 1);
}

// ─── aggregation ─────────────────────────────────────────────────────────────

#[test]
fn aggregation_filters_prune_without_changing_the_result() {
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), RawOptions::default());
    let mut g = Lcg(99);
    let mut sec = 0;
    for round in 0..4 {
        for _ in 0..1200 {
            sec += 1;
            let host = g.below(6);
            t.write(model_row(&mut g, host, sec)).unwrap();
        }
        if round < 3 {
            t.flush().unwrap();
        }
    }
    let names = ["n1-0", "n3-1  ", "n3-1", "absent", ""];
    for round in 0..40u64 {
        let ints: Vec<i64> = (0..g.below(4)).map(|_| g.below(6) as i64 * 1000 + g.below(300) as i64 - 100).collect();
        let by_name = round % 2 == 0;
        let want_names: Vec<Vec<u8>> =
            (0..g.below(3)).map(|_| names[g.below(names.len() as u64) as usize].as_bytes().to_vec()).collect();
        let pad = g.below(2) == 0;
        let mut vf = vec![ValueFilter { col: INT, ints: ints.clone(), ..Default::default() }];
        if by_name {
            vf.push(ValueFilter { col: NOTE, bytes: want_names.clone(), pad_space: pad, ..Default::default() });
        }
        // oracle: the scan, filtered row by row
        let mut want: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        let mut s = t.scan(&ScanFilter::default(), false).unwrap();
        while let Some((_, r)) = s.next_row().unwrap() {
            let ok_int = matches!(&r[INT], Value::Int(v) if ints.contains(v));
            let ok_name = !by_name
                || matches!(&r[NOTE], Value::Bytes(b)
                    if want_names.iter().any(|x| if pad { trim(x) == trim(b) } else { x == b }));
            if ok_int && ok_name {
                let Value::Bytes(h) = &r[1] else { panic!() };
                *want.entry(h.clone()).or_default() += 1;
            }
        }
        let ids: BTreeMap<u64, Vec<u8>> = t
            .series()
            .unwrap()
            .into_iter()
            .map(|(id, tags)| match &tags[0] {
                Value::Bytes(b) => (id, b.clone()),
                _ => panic!(),
            })
            .collect();
        let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
        let spec = AggSpec {
            keys: vec![KeySpec::Series],
            funcs: vec![FuncSpec::CountStar],
            value_filters: vf,
            threads: 1 + (round % 3) as usize,
            ..AggSpec::default()
        };
        agg.set_min_parallel_rows(0);
        agg.run(&spec, None).unwrap();
        let r = agg.result();
        let KeyCol::Series(sids) = &r.keys[0] else { panic!() };
        let got: BTreeMap<Vec<u8>, u64> =
            sids.iter().zip(&r.funcs[0].count).map(|(id, &n)| (ids[id].clone(), n)).collect();
        assert_eq!(got, want, "ints {ints:?} names {want_names:?} pad {pad}");
    }
}

#[test]
fn aggregation_filter_with_null_ok_mask_keeps_null_entries() {
    // A dictionary mask that accepts NULL must keep entries that hold only NULLs.
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), RawOptions::default());
    for i in 0..6 {
        let mut r = row(i, "only-null", 1.0);
        r[INT] = Value::Null;
        t.write(r).unwrap();
    }
    for i in 0..6 {
        t.write(row(100 + i, "has-values", 1.0)).unwrap();
    }
    t.flush().unwrap();
    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
    let dict = AggSpec { keys: vec![KeySpec::Column(INT)], ..AggSpec::default() };
    agg.run(&dict, None).unwrap();
    let KeyCol::Int { nulls, vals } = agg.result().keys[0].clone() else { panic!() };
    let mask: Vec<bool> = nulls.iter().zip(&vals).map(|(&n, _)| n).collect(); // accept only NULL
    let spec = AggSpec {
        keys: vec![KeySpec::Series],
        funcs: vec![FuncSpec::CountStar],
        filters: vec![moosedb_core::agg::ColFilter { col: INT, mask }],
        ..AggSpec::default()
    };
    agg.run(&spec, None).unwrap();
    assert_eq!(agg.result().groups, 1);
    assert_eq!(agg.result().funcs[0].count, vec![6]);
}

/// The float stored with the integer `v` in the hints model: every special value
/// is reachable from the INT column, so an exact filter on INT is exactly a
/// filter on the float.
fn float_of(v: i64) -> f64 {
    match v.rem_euclid(40) {
        0 => f64::NAN,
        1 => -0.0,
        2 => 0.0,
        3 => f64::INFINITY,
        4 => f64::NEG_INFINITY,
        _ => v as f64 / 100.0,
    }
}

/// Per-series COUNT(*) of the rows `keep` accepts (an exact mask on the INT
/// dictionary), with or without `hint`; the dictionary phase gets the hint too,
/// as in the gbh.
fn counts(t: &Table, keep: &dyn Fn(i64) -> bool, hint: Option<&ColPred>, threads: usize) -> BTreeMap<u64, u64> {
    let mut agg = t.agg_open(i64::MIN, i64::MAX, None).unwrap();
    agg.set_min_parallel_rows(0);
    let preds: Vec<ColPred> = hint.cloned().into_iter().collect();
    agg.run(&AggSpec { keys: vec![KeySpec::Column(INT)], preds: preds.clone(), threads, ..AggSpec::default() }, None)
        .unwrap();
    let Some(KeyCol::Int { nulls, vals }) = agg.result().keys.first().cloned() else { return BTreeMap::new() };
    let mask = nulls.iter().zip(&vals).map(|(&n, &v)| !n && keep(v)).collect();
    let spec = AggSpec {
        keys: vec![KeySpec::Series],
        funcs: vec![FuncSpec::CountStar],
        filters: vec![moosedb_core::agg::ColFilter { col: INT, mask }],
        preds,
        threads,
        ..AggSpec::default()
    };
    agg.run(&spec, None).unwrap();
    let r = agg.result();
    let Some(KeyCol::Series(ids)) = r.keys.first() else { return BTreeMap::new() }; // no group
    ids.iter().copied().zip(r.funcs[0].count.iter().copied()).collect()
}

/// Range hints (`AggSpec::preds`) only cut entries: with the exact filter that
/// is equivalent to the predicate, the result with hints (dictionary phase and
/// final run) equals the one without, and both equal the row-by-row oracle
/// (chunks + MemTable + range delete).
#[test]
fn aggregation_range_hints_do_not_change_the_result() {
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), RawOptions::default());
    let mut g = Lcg(4242);
    let mut sec = 0;
    for round in 0..4 {
        for _ in 0..1500 {
            sec += 1 + g.below(3) as i64;
            let host = g.below(6);
            let mut r = model_row(&mut g, host, sec);
            r[VALUE] = match r[INT] {
                Value::Int(v) => Value::Float64(float_of(v)),
                _ => Value::Null,
            };
            t.write(r).unwrap();
        }
        if round < 3 {
            t.flush().unwrap();
        }
    }
    let by_host: BTreeMap<Vec<u8>, u64> = t
        .series()
        .unwrap()
        .into_iter()
        .map(|(id, tags)| match &tags[0] {
            Value::Bytes(b) => (b.clone(), id),
            _ => panic!(),
        })
        .collect();
    let lo = base_ts() + 300 * MICROS_PER_SEC;
    t.delete_range(Some(&[by_host[&b"h2".to_vec()]]), lo, lo + 900 * MICROS_PER_SEC, None, true).unwrap();
    let before = prune_stats();
    for round in 0..120u64 {
        let threads = 1 + (round % 3) as usize;
        let p = loop {
            let p = random_pred(&mut g);
            if !matches!(p, ColPred::Bytes { .. }) {
                break p;
            }
        };
        let keep = |v: i64| match &p {
            ColPred::Int { lo, hi, .. } => *lo <= v && v <= *hi,
            ColPred::Float { lo, hi, .. } => *lo <= float_of(v) && float_of(v) <= *hi,
            ColPred::Bytes { .. } => unreachable!(),
        };
        let mut want: BTreeMap<u64, u64> = BTreeMap::new();
        let mut s = t.scan(&ScanFilter::default(), false).unwrap();
        while let Some((_, r)) = s.next_row().unwrap() {
            if matches(&r, &p) {
                let Value::Bytes(h) = &r[1] else { panic!() };
                *want.entry(by_host[h]).or_default() += 1;
            }
        }
        let plain = counts(&t, &keep, None, threads);
        let hinted = counts(&t, &keep, Some(&p), threads);
        assert_eq!(hinted, plain, "{p:?}");
        let got: BTreeMap<u64, u64> = plain.into_iter().filter(|&(_, n)| n > 0).collect();
        assert_eq!(got, want, "{p:?}");
    }
    assert!(prune_stats().pruned_by_zone > before.pruned_by_zone, "the hints must prune something");
}

// ─── exact byte filter (late materialization) ───────────────────────────────

fn random_bytes_pred(g: &mut Lcg) -> ColPred {
    loop {
        if let p @ ColPred::Bytes { .. } = random_pred(g) {
            return p;
        }
    }
}

/// Positions and rows of a scan, in order.
fn scan_pos(t: &Table, f: &ScanFilter, sorted: bool, proj: Option<&[bool]>) -> Vec<([u8; 16], Row)> {
    let mut s = t.scan_projected(f, sorted, proj).unwrap();
    let mut out = Vec::new();
    while let Some((p, r)) = s.next_row().unwrap() {
        out.push((p.to_bytes(), r));
    }
    out
}

fn check_exact(t: &Table, g: &mut Lcg, rounds: usize) {
    let all = ScanFilter::default();
    // everything but the filtered column, and only the timestamp
    let no_note: Vec<bool> = (0..6).map(|c| c != NOTE).collect();
    let only_ts: Vec<bool> = (0..6).map(|c| c == 0).collect();
    for _ in 0..rounds {
        let preds: Vec<ColPred> = (0..1 + g.below(2)).map(|_| random_bytes_pred(g)).collect();
        let f = ScanFilter { cols: preds.clone(), ..ScanFilter::default() };
        for sorted in [false, true] {
            let proj: [Option<&[bool]>; 3] = [None, Some(&no_note), Some(&only_ts)];
            for proj in proj {
                let full = scan_pos(t, &all, sorted, None);
                let want: Vec<[u8; 16]> =
                    full.iter().filter(|(_, r)| preds.iter().all(|p| matches(r, p))).map(|(p, _)| *p).collect();
                let got: Vec<[u8; 16]> = scan_pos(t, &f, sorted, proj).into_iter().map(|(p, _)| p).collect();
                if sorted {
                    // a merge run may read a span of passing rows: a superset, same order
                    let mut it = got.iter();
                    assert!(want.iter().all(|w| it.any(|x| x == w)), "sorted lost a row: {preds:?}");
                } else {
                    assert_eq!(got, want, "stream is exact: {preds:?} proj {proj:?}");
                }
            }
        }
    }
}

#[test]
fn exact_bytes_filter_loses_no_row_and_returns_only_matches() {
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), RawOptions::default());
    let mut g = Lcg(0xe4ac7);
    let mut sec = 0i64;
    for round in 0..4 {
        for _ in 0..1500 {
            sec += 1 + g.below(3) as i64;
            let host = g.below(6);
            t.write(model_row(&mut g, host, sec)).unwrap();
        }
        if round < 3 {
            t.flush().unwrap();
        }
    }
    let h2: Vec<u64> = t
        .series()
        .unwrap()
        .into_iter()
        .filter(|(_, tags)| tags[0] == Value::Bytes(b"h2".to_vec()))
        .map(|(id, _)| id)
        .collect();
    let lo = base_ts() + 300 * MICROS_PER_SEC;
    t.delete_range(Some(&h2), lo, lo + 900 * MICROS_PER_SEC, None, true).unwrap();
    let before = prune_stats().rows_filtered;
    check_exact(&t, &mut g, 40);
    assert!(prune_stats().rows_filtered > before, "rows were dropped by the filter");
    t.flush().unwrap();
    t.compact(i64::MIN, i64::MAX, base_ts()).unwrap();
    check_exact(&t, &mut g, 20);
}

#[test]
fn exact_bytes_filter_handles_decoded_series_without_summaries_and_empty_lists() {
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), RawOptions::default());
    for i in 0..50 {
        t.write(row(i, "srv1", 1.0)).unwrap();
    }
    t.flush().unwrap();
    for i in 50..60 {
        t.write(row(i, "srv1", 1.0)).unwrap();
    }
    let one = |values: Vec<Vec<u8>>, pad_space| ScanFilter {
        cols: vec![ColPred::Bytes { col: NOTE, values, pad_space }],
        ..ScanFilter::default()
    };
    // chunk row 7 and MemTable row 52; NULLs (every third second) never match
    assert_eq!(scan_rows(&t, &one(vec![b"n7".to_vec(), b"n52".to_vec()], false), false).len(), 2);
    assert_eq!(scan_rows(&t, &one(vec![b"n7  ".to_vec()], false), false).len(), 0);
    assert_eq!(scan_rows(&t, &one(vec![b"n7  ".to_vec()], true), false).len(), 1);
    assert_eq!(scan_rows(&t, &one(vec![], true), false).len(), 0, "an empty list matches nothing");
}

// ─── exact byte filter x ordered scan of a UNIQUE table (merge_fill_tie) ────

/// `(ts, itemid TAG, ns, note VARCHAR)`, key `(itemid, ts, ns)`.
fn unique_note_config(dir: &std::path::Path) -> TableConfig {
    use moosedb_core::{Column, ColumnType, Schema};
    let schema = Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "itemid".into(), ty: ColumnType::Tag },
            Column { name: "ns".into(), ty: ColumnType::Int64 },
            Column { name: "note".into(), ty: ColumnType::Varchar },
        ],
        0,
    )
    .unwrap();
    TableConfig::new(dir, schema, &RawOptions::default()).unwrap().with_unique(vec![2]).unwrap()
}

/// The ordered scan with an exact filter is a superset (same order) of the
/// rows that match, and exactly them while at most MERGE_KEEP_RUNS (16) runs
/// match; with more, a run may keep a span of non-matching rows.
#[test]
fn exact_bytes_filter_on_ordered_unique_scan_with_ties() {
    const NOTE_U: usize = 3;
    let dir = tempfile::tempdir().unwrap();
    let cfg = unique_note_config(&dir.path().join("t"));
    Table::create(&cfg).unwrap();
    let t = std::sync::Arc::new(Table::open(cfg).unwrap());
    let mut g = Lcg(0x7137);
    let mut written: Vec<Row> = Vec::new();
    // 24 series x 3 chunks + MemTable: well over 16 runs when "common" matches.
    for round in 0..4 {
        for _ in 0..600 {
            let (item, sec, ns) = (g.below(24), g.below(60) as i64, g.below(3) as i64);
            let note = match (item, g.below(10)) {
                (_, 0) => Value::Null,
                (0..=1, 1) => Value::Bytes(b"rare".to_vec()), // only 2 series
                (_, 2..=5) => Value::Bytes(b"common".to_vec()),
                (_, 6) => Value::Bytes(b"common  ".to_vec()),
                _ => Value::Bytes(format!("x{}", g.below(5)).into_bytes()),
            };
            let row = vec![
                Value::Timestamp(base_ts() + sec * MICROS_PER_SEC),
                Value::Bytes(format!("i{item}").into_bytes()),
                Value::Int(ns),
                note,
            ];
            // a duplicate key is refused: that is part of the model
            if t.write_shared(row.clone()).is_ok() {
                written.push(row);
            }
        }
        if round < 3 {
            t.flush().unwrap();
        }
    }
    // deletes hit chunk rows and MemTable rows
    let gone: Vec<Row> = written.iter().step_by(11).cloned().collect();
    assert_eq!(t.delete_rows(gone, true).unwrap(), written.iter().step_by(11).count() as u64);

    let all = ScanFilter::default();
    let proj_all: [Option<&[bool]>; 3] = [None, Some(&[true, true, true, false]), Some(&[true, false, false, false])];
    let lists: [(&[&[u8]], bool, bool); 5] = [
        (&[b"rare"], false, true),
        (&[b"common"], false, false),
        (&[b"common"], true, false),
        (&[b"x1", b"x3", b"rare"], false, false),
        (&[b"absent"], false, true),
    ];
    let before = prune_stats().rows_filtered;
    for (values, pad_space, exact) in lists {
        let p = ColPred::Bytes { col: NOTE_U, values: values.iter().map(|v| v.to_vec()).collect(), pad_space };
        let f = ScanFilter { cols: vec![p.clone()], ..ScanFilter::default() };
        for proj in proj_all {
            let full = scan_pos(&t, &all, true, None);
            let want: Vec<[u8; 16]> = full.iter().filter(|(_, r)| matches(r, &p)).map(|(q, _)| *q).collect();
            assert!(!want.is_empty() || values == [b"absent"], "{values:?} must match something");
            let got: Vec<[u8; 16]> = scan_pos(&t, &f, true, proj).into_iter().map(|(q, _)| q).collect();
            let mut it = got.iter();
            assert!(want.iter().all(|w| it.any(|x| x == w)), "lost or reordered a row: {values:?} proj {proj:?}");
            if exact {
                assert_eq!(got, want, "<= 16 matching runs is exact: {values:?} proj {proj:?}");
            }
        }
    }
    assert!(prune_stats().rows_filtered > before, "the exact filter dropped rows");
}
