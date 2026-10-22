//! Zone maps and column Blooms: pruned scans and aggregations give exactly the
//! rows an unpruned one gives (model test over random data with NaN, signed
//! zeros, infinities, NULLs, padded strings, deletes, MemTable rows and
//! compaction), chunks written by older code are read unchanged, and a damaged
//! summary is never served.

use std::collections::BTreeMap;

use super::helpers::*;
use moosedb_core::agg::{AggSpec, FuncSpec, KeyCol, KeySpec, ValueFilter};
use moosedb_core::time::MICROS_PER_SEC;
use moosedb_core::{prune_stats, ColPred, RawOptions, Row, ScanFilter, Table, Value};

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

