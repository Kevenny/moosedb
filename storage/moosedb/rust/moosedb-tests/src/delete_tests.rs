//! `DELETE`: tombstones over the MemTable and sealed chunks, durability and
//! replay, reinsertion of deleted keys, compaction, concurrency.

use std::path::Path;
use std::sync::Arc;
use std::thread;

use moosedb_core::delete::RangeDelete;
use moosedb_core::{Column, ColumnType, RawOptions, Row, ScanFilter, Schema, Table, TableConfig, Value};

/// `(clock, itemid TAG, value, ns)`.
fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "clock".into(), ty: ColumnType::Timestamp },
            Column { name: "itemid".into(), ty: ColumnType::Tag },
            Column { name: "value".into(), ty: ColumnType::Float64 },
            Column { name: "ns".into(), ty: ColumnType::Int64 },
        ],
        0,
    )
    .unwrap()
}

fn config(dir: &Path, unique: bool) -> TableConfig {
    let cfg = TableConfig::new(dir, schema(), &RawOptions::default()).unwrap();
    if unique {
        cfg.with_unique(vec![3]).unwrap()
    } else {
        cfg
    }
}

fn open_new(dir: &Path, unique: bool) -> Arc<Table> {
    let cfg = config(dir, unique);
    Table::create(&cfg).unwrap();
    Arc::new(Table::open(cfg).unwrap())
}

fn reopen(dir: &Path, unique: bool) -> Arc<Table> {
    Arc::new(Table::open(config(dir, unique)).unwrap())
}

const SEC: i64 = 1_000_000;

fn row(item: &str, clock: i64, ns: i64) -> Row {
    vec![
        Value::Timestamp(clock * SEC),
        Value::Bytes(item.as_bytes().to_vec()),
        Value::Float64((clock * 10 + ns) as f64),
        Value::Int(ns),
    ]
}

fn put(t: &Arc<Table>, rows: Vec<Row>) {
    let mut b = t.begin_batch().unwrap();
    for r in rows {
        b.write(r).unwrap();
    }
    b.commit(true).unwrap();
}

/// `(item, clock, ns)` of every row, sorted.
fn dump(t: &Table) -> Vec<(String, i64, i64)> {
    let mut out = Vec::new();
    for sorted in [false, true] {
        let mut s = t.scan(&ScanFilter::default(), sorted).unwrap();
        let mut v = Vec::new();
        while let Some((_, r)) = s.next_row().unwrap() {
            let (Value::Timestamp(ts), Value::Bytes(i), Value::Int(ns)) = (&r[0], &r[1], &r[3]) else { panic!() };
            v.push((String::from_utf8(i.clone()).unwrap(), ts / SEC, *ns));
        }
        v.sort();
        if out.is_empty() {
            out = v;
        } else {
            assert_eq!(out, v, "stream and merge scans agree");
        }
    }
    out
}

fn ids(t: &Table, items: &[&str]) -> Vec<u64> {
    t.series()
        .unwrap()
        .into_iter()
        .filter(|(_, tags)| items.iter().any(|i| tags[0] == Value::Bytes(i.as_bytes().to_vec())))
        .map(|(id, _)| id)
        .collect()
}

fn del(t: &Table, items: Option<&[&str]>, lo: i64, hi: i64) -> u64 {
    let sel = items.map(|i| ids(t, i));
    match t.delete_range(sel.as_deref(), lo * SEC, hi * SEC, None, true).unwrap() {
        RangeDelete::Deleted(n) => n,
        RangeDelete::Exceeds(n) => panic!("exceeds {n}"),
    }
}

fn expect(rows: &[(&str, i64, i64)]) -> Vec<(String, i64, i64)> {
    let mut v: Vec<_> = rows.iter().map(|(i, c, n)| (i.to_string(), *c, *n)).collect();
    v.sort();
    v
}

/// 3 items x clocks 1..=10, half in a chunk, half in the MemTable.
fn fill(t: &Arc<Table>) -> Vec<(String, i64, i64)> {
    let mut all = Vec::new();
    for round in 0..2 {
        let mut rows = Vec::new();
        for item in ["a", "b", "c"] {
            for c in (1 + round * 5)..=(5 + round * 5) {
                rows.push(row(item, c, 0));
                all.push((item.to_string(), c, 0));
            }
        }
        put(t, rows);
        if round == 0 {
            t.flush().unwrap();
        }
    }
    all.sort();
    all
}

#[test]
fn range_delete_hides_rows_in_memtable_and_chunks_and_counts_them() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    let all = fill(&t);
    assert_eq!(dump(&t), all);
    // a: clocks 3..=7 straddle the chunk (1-5) and the MemTable (6-10)
    assert_eq!(del(&t, Some(&["a"]), 3, 7), 5);
    let mut want = all.clone();
    want.retain(|(i, c, _)| !(i == "a" && (3..=7).contains(c)));
    assert_eq!(dump(&t), want);
    assert_eq!(t.stats().unwrap().row_count as usize, want.len());
    // deleting again finds nothing
    assert_eq!(del(&t, Some(&["a"]), 3, 7), 0);
    // overlapping range counts only what is still there
    assert_eq!(del(&t, Some(&["a"]), 1, 4), 2);
    // all series
    assert_eq!(del(&t, None, 9, 100), 6);
    want.retain(|(i, c, _)| !(i == "a" && *c <= 4) && *c < 9);
    assert_eq!(dump(&t), want);
    assert_eq!(t.stats().unwrap().row_count as usize, want.len());
    // ranges outside the data and empty ranges are no-ops
    assert_eq!(del(&t, Some(&["a"]), 500, 600), 0);
    assert_eq!(t.delete_range(Some(&[]), 0, 10 * SEC, None, true).unwrap(), RangeDelete::Deleted(0));
    assert_eq!(t.delete_range(None, 10, 5, None, true).unwrap(), RangeDelete::Deleted(0));
}

#[test]
fn max_rows_refuses_without_deleting_and_count_range_agrees() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    let all = fill(&t);
    let a = ids(&t, &["a"]);
    assert_eq!(t.count_range(Some(&a), 0, 100 * SEC).unwrap(), 10);
    assert_eq!(t.delete_range(Some(&a), 0, 100 * SEC, Some(9), true).unwrap(), RangeDelete::Exceeds(10));
    assert_eq!(dump(&t), all);
    assert_eq!(t.delete_range(Some(&a), 0, 100 * SEC, Some(10), true).unwrap(), RangeDelete::Deleted(10));
}

#[test]
fn rows_inserted_after_a_delete_are_visible_even_in_the_same_range() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    fill(&t);
    del(&t, Some(&["b"]), 1, 10);
    put(&t, vec![row("b", 4, 1)]);
    let seen = |t: &Arc<Table>| dump(t).into_iter().filter(|r| r.0 == "b").collect::<Vec<_>>();
    assert_eq!(seen(&t), expect(&[("b", 4, 1)]));
    t.flush().unwrap(); // the new row is sealed into a chunk born after the delete
    assert_eq!(seen(&t), expect(&[("b", 4, 1)]));
    drop(t);
    let t = reopen(&dir.path().join("t"), false);
    assert_eq!(seen(&t), expect(&[("b", 4, 1)]));
    // and a second delete takes it
    assert_eq!(del(&t, Some(&["b"]), 4, 4), 1);
    assert!(seen(&t).is_empty());
}

#[test]
fn delete_survives_restart_by_replay_and_by_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    let all = fill(&t);
    del(&t, Some(&["c"]), 2, 8);
    let mut want = all.clone();
    want.retain(|(i, c, _)| !(i == "c" && (2..=8).contains(c)));
    // no flush since: the delete lives in the WAL only
    drop(t);
    let t = reopen(&path, false);
    assert_eq!(dump(&t), want);
    assert_eq!(t.stats().unwrap().row_count as usize, want.len());
    // a flush stores the tombstones in the MANIFEST and retires the WAL
    t.flush().unwrap();
    drop(t);
    let t = reopen(&path, false);
    assert_eq!(dump(&t), want);
    assert_eq!(t.stats().unwrap().row_count as usize, want.len());
    // more deletes after that, replayed on top of the stored ones
    del(&t, Some(&["a"]), 1, 3);
    want.retain(|(i, c, _)| !(i == "a" && *c <= 3));
    drop(t);
    let t = reopen(&path, false);
    assert_eq!(dump(&t), want);
}

#[test]
fn a_crash_after_the_delete_was_logged_keeps_it_and_one_before_never_half_applies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    let all = fill(&t);
    del(&t, None, 1, 2);
    // kill -9: no destructor, no flush
    std::mem::forget(t);
    let t = reopen(&path, false);
    let mut want = all.clone();
    want.retain(|(_, c, _)| *c > 2);
    assert_eq!(dump(&t), want);
}

#[test]
fn compaction_removes_dead_rows_physically_and_drops_dead_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    let all = fill(&t);
    t.flush().unwrap();
    let before = t.chunks().unwrap().len();
    assert!(before >= 2);
    del(&t, Some(&["a"]), 1, 10);
    let mut want = all.clone();
    want.retain(|r| r.0 != "a");
    t.compact(i64::MIN, i64::MAX, 2_000_000_000 * SEC).unwrap();
    assert_eq!(dump(&t), want);
    let rows: u64 = t.chunks().unwrap().iter().map(|c| c.header.row_count).sum();
    assert_eq!(rows as usize, want.len(), "dead rows were not copied");
    assert_eq!(t.stats().unwrap().row_count as usize, want.len());
    // delete everything: the chunks go without being rewritten
    del(&t, None, 0, 1000);
    t.compact(i64::MIN, i64::MAX, 2_000_000_000 * SEC).unwrap();
    assert!(t.chunks().unwrap().is_empty());
    assert!(dump(&t).is_empty());
    drop(t);
    assert!(dump(&reopen(&path, false)).is_empty());
}

#[test]
fn a_delete_during_a_scan_does_not_disturb_it() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    fill(&t);
    for sorted in [false, true] {
        let total = dump(&t).len();
        let mut s = t.scan(&ScanFilter::default(), sorted).unwrap();
        let first = s.next_row().unwrap().unwrap();
        assert!(matches!(first.1[0], Value::Timestamp(_)));
        del(&t, None, 0, 1000);
        let mut n = 1;
        while s.next_row().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, total, "the snapshot opened before the delete sees all of it");
        put(&t, fill_again());
    }
}

fn fill_again() -> Vec<Row> {
    let mut v = Vec::new();
    for item in ["a", "b", "c"] {
        for c in 1..=5 {
            v.push(row(item, c, 0));
        }
    }
    v
}

#[test]
fn unique_key_can_be_reinserted_after_a_delete_and_duplicates_still_fail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, true);
    put(&t, vec![row("a", 5, 1), row("a", 5, 2), row("a", 6, 0)]);
    let dup = |t: &Arc<Table>, r: Row| {
        let mut b = t.begin_batch().unwrap();
        let res = b.write(r);
        b.abort();
        res
    };
    assert!(matches!(dup(&t, row("a", 5, 1)), Err(moosedb_core::Error::Duplicate(_))));
    assert!(matches!(dup(&t, row("a", 6, 0)), Err(moosedb_core::Error::Duplicate(_))));
    // delete the highest timestamp and the older pair
    assert_eq!(del(&t, Some(&["a"]), 6, 6), 1);
    dup(&t, row("a", 6, 0)).unwrap();
    assert_eq!(del(&t, Some(&["a"]), 5, 5), 2);
    for ns in [1, 2] {
        dup(&t, row("a", 5, ns)).unwrap();
    }
    put(&t, vec![row("a", 5, 1)]);
    assert!(matches!(dup(&t, row("a", 5, 1)), Err(moosedb_core::Error::Duplicate(_))));
    t.flush().unwrap();
    // in a chunk: the same
    assert_eq!(del(&t, Some(&["a"]), 5, 5), 1);
    dup(&t, row("a", 5, 1)).unwrap();
    drop(t);
    let t = reopen(&path, true);
    dup(&t, row("a", 5, 1)).unwrap();
    put(&t, vec![row("a", 5, 1)]);
    assert!(matches!(dup(&t, row("a", 5, 1)), Err(moosedb_core::Error::Duplicate(_))));
    assert_eq!(dump(&t), expect(&[("a", 5, 1)]));
}

#[test]
fn exact_row_delete_by_content_in_memtable_and_chunks_with_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    // three identical rows (one in a chunk, two in the MemTable) and two others
    put(&t, vec![row("a", 5, 0), row("a", 6, 0)]);
    t.flush().unwrap();
    put(&t, vec![row("a", 5, 0), row("a", 5, 0), row("b", 5, 0)]);
    // delete two of the three identical rows and a row that does not exist
    let n = t.delete_rows(vec![row("a", 5, 0), row("a", 5, 0), row("a", 9, 0), row("z", 1, 1)], true).unwrap();
    assert_eq!(n, 2);
    assert_eq!(dump(&t).iter().filter(|r| r.0 == "a" && r.1 == 5).count(), 1);
    assert_eq!(t.stats().unwrap().row_count, 3);
    // the last copy, which sits in the chunk (the MemTable gave its two first)
    assert_eq!(t.delete_rows(vec![row("a", 5, 0)], true).unwrap(), 1);
    assert_eq!(dump(&t), expect(&[("a", 6, 0), ("b", 5, 0)]));
    // a different value at the same key is another row
    put(&t, vec![row("a", 6, 1)]);
    assert_eq!(t.delete_rows(vec![row("a", 6, 1)], true).unwrap(), 1);
    assert_eq!(dump(&t), expect(&[("a", 6, 0), ("b", 5, 0)]));
    drop(t);
    let t = reopen(&path, false);
    assert_eq!(dump(&t), expect(&[("a", 6, 0), ("b", 5, 0)]));
    t.flush().unwrap();
    t.compact(i64::MIN, i64::MAX, 2_000_000_000 * SEC).unwrap();
    assert_eq!(dump(&t), expect(&[("a", 6, 0), ("b", 5, 0)]));
    assert_eq!(t.stats().unwrap().row_count, 2);
    drop(t);
    assert_eq!(dump(&reopen(&path, false)), expect(&[("a", 6, 0), ("b", 5, 0)]));
}

#[test]
fn truncate_forgets_tombstones_and_new_rows_are_not_hidden() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    fill(&t);
    del(&t, None, 0, 1000);
    t.truncate().unwrap();
    put(&t, fill_again());
    t.flush().unwrap();
    assert_eq!(dump(&t).len(), 15);
    drop(t);
    assert_eq!(dump(&reopen(&path, false)).len(), 15);
}

#[test]
fn many_small_deletes_then_reads_stay_correct_and_compaction_cleans_up() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    let items: Vec<String> = (0..200).map(|i| format!("i{i}")).collect();
    let mut rows = Vec::new();
    for it in &items {
        for c in 0..20 {
            rows.push(row(it, c, 0));
        }
    }
    put(&t, rows);
    t.flush().unwrap();
    let mut want = 0usize;
    for (n, it) in items.iter().enumerate() {
        let id = ids(&t, &[it.as_str()]);
        let cut = (n % 20) as i64;
        let r = t.delete_range(Some(&id), 0, (cut - 1) * SEC, None, false).unwrap();
        assert_eq!(r, RangeDelete::Deleted(cut as u64));
        want += 20 - cut as usize;
    }
    t.sync_wal(true).unwrap();
    assert_eq!(dump(&t).len(), want);
    assert_eq!(t.stats().unwrap().row_count as usize, want);
    drop(t);
    let t = reopen(&path, false);
    assert_eq!(dump(&t).len(), want);
    t.compact(i64::MIN, i64::MAX, 2_000_000_000 * SEC).unwrap();
    assert_eq!(dump(&t).len(), want);
    let rows: u64 = t.chunks().unwrap().iter().map(|c| c.header.row_count).sum();
    assert_eq!(rows as usize, want);
}

#[test]
fn concurrent_inserts_and_deletes_of_the_same_series_lose_nothing_new() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), true);
    // old data
    put(&t, (0..100).map(|c| row("a", c, 0)).collect());
    let writer = {
        let t = t.clone();
        thread::spawn(move || {
            for c in 1000..1300 {
                put(&t, vec![row("a", c, 0)]);
                if c % 50 == 0 {
                    t.flush().unwrap();
                }
            }
        })
    };
    let deleter = {
        let t = t.clone();
        thread::spawn(move || {
            let a = ids(&t, &["a"]);
            let mut n = 0u64;
            for c in 0..100 {
                if let RangeDelete::Deleted(k) = t.delete_range(Some(&a), c * SEC, c * SEC, None, false).unwrap() {
                    n += k;
                }
            }
            n
        })
    };
    writer.join().unwrap();
    assert_eq!(deleter.join().unwrap(), 100);
    let got = dump(&t);
    assert_eq!(got.len(), 300);
    assert!(got.iter().all(|r| r.1 >= 1000));
}

/// The housekeeper at scale: 5000 items x 3 days in chunks, one delete per item,
/// without and with a long-lived scan holding a snapshot (copy-on-write of the
/// tombstone set). `cargo test --release -p moosedb-tests delete_perf -- --ignored --nocapture`
#[test]
#[ignore]
fn delete_perf_housekeeper() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), true);
    let items: Vec<String> = (0..5000).map(|i| format!("{i}")).collect();
    for day in 0..3i64 {
        let mut b = t.begin_batch().unwrap();
        for c in 0..144i64 {
            for it in &items {
                b.write(row(it, day * 86400 + c * 600, 0)).unwrap();
            }
        }
        b.commit(true).unwrap();
        t.flush().unwrap();
    }
    let all: std::collections::HashMap<Vec<u8>, u64> = t
        .series()
        .unwrap()
        .into_iter()
        .map(|(id, tags)| match &tags[0] {
            Value::Bytes(b) => (b.clone(), id),
            _ => panic!(),
        })
        .collect();
    // a quarter of the day only, so the chunks stay alive with their tombstones pending
    for (label, day, hold) in [("no reader", 0i64, false), ("a scan held open", 1, true)] {
        let _held = hold.then(|| {
            let mut s = t.scan(&ScanFilter::default(), true).unwrap();
            s.next_batch(10, false).unwrap();
            s
        });
        let cut = (day * 86400 + 20000) * SEC;
        let start = std::time::Instant::now();
        let mut gone = 0;
        for it in &items {
            let id = all[it.as_bytes()];
            if let RangeDelete::Deleted(k) = t.delete_range(Some(&[id]), day * 86400 * SEC, cut, None, false).unwrap() {
                gone += k;
            }
        }
        println!("{label}: {} deletes ({gone} rows) in {:?}", items.len(), start.elapsed());
    }
    let s = std::time::Instant::now();
    t.flush().unwrap();
    println!("flush with 10000 tombstones pending (writes the MANIFEST): {:?}", s.elapsed());
    let s = std::time::Instant::now();
    let n = t.scan(&ScanFilter::default(), true).unwrap().next_batch(1000, false).unwrap().rows();
    println!("first batch of {n} rows with the tombstones pending: {:?}", s.elapsed());
}

// ─── more ground: NULLs and every column type, descending scans with holes,
// spilled batches, compaction racing with deletes ──────────────────────────

fn wide_schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "note".into(), ty: ColumnType::Varchar },
            Column { name: "f".into(), ty: ColumnType::Float64 },
            Column { name: "g".into(), ty: ColumnType::Float32 },
            Column { name: "b".into(), ty: ColumnType::Bool },
            Column { name: "n".into(), ty: ColumnType::Int64 },
        ],
        0,
    )
    .unwrap()
}

fn wide(i: i64) -> Row {
    vec![
        Value::Timestamp(1_000 + i / 3), // three rows share each timestamp
        Value::Bytes(b"h".to_vec()),
        if i % 4 == 0 { Value::Null } else { Value::Bytes(vec![i as u8, 0, 255, 10]) },
        if i % 5 == 0 { Value::Float64(-0.0) } else { Value::Float64(i as f64 / 7.0) },
        if i % 6 == 0 { Value::Null } else { Value::Float32(i as f32 * 1.5) },
        Value::Bool(i % 2 == 0),
        if i % 3 == 0 { Value::Null } else { Value::Int(i) },
    ]
}

fn wide_dump(t: &Table, backward: bool) -> Vec<Row> {
    let mut s = t.scan(&ScanFilter::default(), true).unwrap();
    if backward {
        s.seek_end().unwrap();
    }
    let mut out = Vec::new();
    loop {
        let next = if backward { s.prev_row() } else { s.next_row() };
        match next.unwrap() {
            Some((_, r)) => out.push(r),
            None => break,
        }
    }
    out
}

fn wide_table(path: &Path) -> Arc<Table> {
    let cfg = TableConfig::new(path, wide_schema(), &RawOptions::default()).unwrap();
    if !path.exists() {
        Table::create(&cfg).unwrap();
    }
    Arc::new(Table::open(cfg).unwrap())
}

#[test]
fn rows_with_nulls_blobs_floats_are_deleted_exactly_and_scans_skip_the_holes_both_ways() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = wide_table(&path);
    let all: Vec<Row> = (0..60).map(wide).collect();
    {
        let mut b = t.begin_batch().unwrap();
        for r in &all[..40] {
            b.write(r.clone()).unwrap();
        }
        b.commit(true).unwrap();
    }
    t.flush().unwrap(); // 0..40 in a chunk
    {
        let mut b = t.begin_batch().unwrap();
        for r in &all[40..] {
            b.write(r.clone()).unwrap();
        }
        b.commit(true).unwrap();
    }
    let gone: Vec<usize> = vec![0, 1, 2, 5, 6, 7, 8, 13, 14, 20, 21, 22, 23, 24, 39, 41, 50, 59];
    let n = t.delete_rows(gone.iter().map(|&i| all[i].clone()).collect(), true).unwrap();
    assert_eq!(n as usize, gone.len());
    let want: Vec<Row> = all.iter().enumerate().filter(|(i, _)| !gone.contains(i)).map(|(_, r)| r.clone()).collect();
    let sorted = |mut v: Vec<Row>| {
        v.sort_by_key(|r| format!("{r:?}"));
        v
    };
    assert_eq!(sorted(wide_dump(&t, false)), sorted(want.clone()));
    let fwd = wide_dump(&t, false);
    let mut back = wide_dump(&t, true);
    back.reverse();
    let ts = |v: &Vec<Row>| v.iter().map(|r| r[0].clone()).collect::<Vec<_>>();
    assert_eq!(ts(&fwd), ts(&back));
    assert_eq!(sorted(back), sorted(want.clone()));
    assert_eq!(t.stats().unwrap().row_count as usize, want.len());
    // the same after a restart and after the compaction removed the holes
    drop(t);
    let t = wide_table(&path);
    assert_eq!(sorted(wide_dump(&t, true)), sorted(want.clone()));
    t.flush().unwrap();
    t.compact(i64::MIN, i64::MAX, 2_000_000_000 * SEC).unwrap();
    assert_eq!(sorted(wide_dump(&t, false)), sorted(want.clone()));
    let rows: u64 = t.chunks().unwrap().iter().map(|c| c.header.row_count).sum();
    assert_eq!(rows as usize, want.len());
    // and a second delete of the same rows finds nothing
    assert_eq!(t.delete_rows(vec![all[0].clone(), all[41].clone()], true).unwrap(), 0);
}

#[test]
fn a_spilled_batch_is_not_hit_by_a_delete_that_ran_while_it_was_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(&dir.path().join("t"), true);
    cfg.opts.memtable_size_bytes = 8 * 1024;
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    put(&t, (0..200).map(|c| row("a", c, 0)).collect());
    t.flush().unwrap();
    let mut b = t.begin_batch().unwrap();
    for c in 1000..1600 {
        b.write(row("b", c, 0)).unwrap(); // spills into staged chunks
    }
    // the delete runs while the statement is open: old rows only
    assert_eq!(del(&t, None, 0, 100_000), 200);
    for c in 1600..1700 {
        b.write(row("b", c, 0)).unwrap();
    }
    b.commit(true).unwrap();
    assert_eq!(dump(&t).len(), 700);
    assert!(dump(&t).iter().all(|r| r.0 == "b"));
    t.flush().unwrap();
    drop(t);
    let t = reopen(&dir.path().join("t"), true);
    assert_eq!(dump(&t).len(), 700);
    // those rows can be deleted afterwards
    assert_eq!(del(&t, Some(&["b"]), 1000, 1099), 100);
}

#[test]
fn compaction_flush_and_inserts_racing_with_deletes_keep_exactly_the_right_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    for part in 0..4 {
        put(&t, (part * 500..(part + 1) * 500).map(|c| row("a", c, 0)).collect());
        t.flush().unwrap();
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let compactor = {
        let (t, stop) = (t.clone(), stop.clone());
        thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                t.flush().unwrap();
                t.compact(i64::MIN, i64::MAX, 2_000_000_000 * SEC).unwrap();
            }
        })
    };
    let writer = {
        let t = t.clone();
        thread::spawn(move || {
            for c in 0..400 {
                put(&t, vec![row("b", c, 0)]);
            }
        })
    };
    let a = ids(&t, &["a"]);
    let mut deleted = 0;
    for c in 0..2000 {
        if c % 7 == 0 {
            // an exact-row delete of the series being compacted, too
            deleted += t.delete_rows(vec![row("a", c, 0)], false).unwrap();
        } else if let RangeDelete::Deleted(k) = t.delete_range(Some(&a), c * SEC, c * SEC, None, false).unwrap() {
            deleted += k;
        }
    }
    writer.join().unwrap();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    compactor.join().unwrap();
    assert_eq!(deleted, 2000);
    let got = dump(&t);
    assert_eq!(got.len(), 400);
    assert!(got.iter().all(|r| r.0 == "b"));
    assert_eq!(t.stats().unwrap().row_count, 400);
    t.compact(i64::MIN, i64::MAX, 2_000_000_000 * SEC).unwrap();
    assert_eq!(t.stats().unwrap().row_count, 400);
    let rows: u64 = t.chunks().unwrap().iter().map(|c| c.header.row_count).sum();
    assert_eq!(rows, 400, "no dead row survives a final compaction");
    drop(t);
    assert_eq!(dump(&reopen(&path, false)).len(), 400);
}
