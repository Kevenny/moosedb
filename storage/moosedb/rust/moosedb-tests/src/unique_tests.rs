//! UNIQUE / PRIMARY KEY `(TAG..., time, extras)`: duplicates against every
//! place a row can live, races between statements, tie order and recovery.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use moosedb_core::{Batch, Column, ColumnType, Error, RawOptions, Row, ScanFilter, Schema, Table, TableConfig, Value};

/// `(clock, itemid TAG, value, ns)`; key `(itemid, clock, ns)`.
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

fn config(dir: &Path, small: bool) -> TableConfig {
    let mut cfg = TableConfig::new(dir, schema(), &RawOptions::default()).unwrap().with_unique(vec![3]).unwrap();
    if small {
        cfg.opts.memtable_size_bytes = 8 * 1024;
    }
    cfg
}

fn open_new(dir: &Path, small: bool) -> Arc<Table> {
    let cfg = config(dir, small);
    Table::create(&cfg).unwrap();
    Arc::new(Table::open(cfg).unwrap())
}

fn reopen(dir: &Path) -> Arc<Table> {
    Arc::new(Table::open(config(dir, false)).unwrap())
}

fn r(item: &str, clock: i64, ns: i64) -> Row {
    vec![
        Value::Timestamp(clock * 1_000_000),
        Value::Bytes(item.as_bytes().to_vec()),
        Value::Float64((clock * 10 + ns) as f64),
        Value::Int(ns),
    ]
}

fn is_dup<T: std::fmt::Debug>(res: Result<T, Error>) -> bool {
    matches!(res, Err(Error::Duplicate(_)))
}

/// Writes retrying while another statement holds the series.
fn put(b: &mut Batch, row: Row) -> Result<(), Error> {
    loop {
        match b.write(row.clone()) {
            Err(Error::Busy(_)) => continue,
            other => return other,
        }
    }
}

fn insert_one(t: &Arc<Table>, row: Row) -> Result<(), Error> {
    let mut b = t.begin_batch().unwrap();
    put(&mut b, row)?;
    b.commit(false)
}

fn scan_all(t: &Table, sorted: bool, backward: bool) -> Vec<(String, i64, i64)> {
    let mut s = t.scan(&ScanFilter::default(), sorted).unwrap();
    if backward {
        s.seek_end().unwrap();
    }
    let mut out = Vec::new();
    loop {
        let next = if backward { s.prev_row() } else { s.next_row() };
        let Some((_, row)) = next.unwrap() else { break };
        let (Value::Timestamp(ts), Value::Bytes(item), Value::Int(ns)) = (&row[0], &row[1], &row[3]) else { panic!() };
        out.push((String::from_utf8(item.clone()).unwrap(), ts / 1_000_000, *ns));
    }
    out
}

#[test]
fn duplicates_are_found_in_every_place_a_row_can_live() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);

    // inside one statement
    let mut b = t.begin_batch().unwrap();
    put(&mut b, r("a", 100, 1)).unwrap();
    assert!(is_dup(put(&mut b, r("a", 100, 1))), "same statement, still buffered");
    put(&mut b, r("a", 100, 2)).unwrap();
    put(&mut b, r("a", 99, 1)).unwrap(); // older than the highest: looked up, not found
    assert!(is_dup(put(&mut b, r("a", 99, 1))), "an out-of-order row is also remembered");
    assert!(is_dup(put(&mut b, r("a", 100, 2))));
    put(&mut b, r("b", 100, 1)).unwrap(); // another series
    b.commit(false).unwrap();
    assert_eq!(t.stats().unwrap().row_count, 4, "refused rows were not stored");

    // MemTable
    assert!(is_dup(insert_one(&t, r("a", 100, 1))));
    assert!(is_dup(insert_one(&t, r("a", 99, 1))));
    insert_one(&t, r("a", 101, 1)).unwrap();

    // sealed chunk
    t.flush().unwrap();
    assert!(is_dup(insert_one(&t, r("a", 100, 2))));
    assert!(is_dup(insert_one(&t, r("a", 101, 1))));
    assert!(is_dup(insert_one(&t, r("b", 100, 1))));
    insert_one(&t, r("a", 100, 3)).unwrap(); // same time, new ns, below the top
    assert!(is_dup(insert_one(&t, r("a", 100, 3))));

    // after compaction
    t.flush().unwrap();
    t.compact(i64::MIN, i64::MAX, i64::MAX / 2).unwrap();
    for (c, ns) in [(100, 1), (100, 2), (100, 3), (99, 1), (101, 1)] {
        assert!(is_dup(insert_one(&t, r("a", c, ns))), "{c} {ns}");
    }
    assert_eq!(t.stats().unwrap().row_count, 6);
}

#[test]
fn state_is_rebuilt_after_a_reopen_and_from_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    {
        let t = open_new(&path, false);
        insert_one(&t, r("a", 10, 1)).unwrap();
        insert_one(&t, r("a", 20, 1)).unwrap();
        t.flush().unwrap(); // chunk: a@10, a@20
        insert_one(&t, r("a", 20, 2)).unwrap(); // WAL only
        insert_one(&t, r("a", 30, 1)).unwrap();
        t.sync_wal(true).unwrap();
        std::mem::forget(t); // crash: the WAL is replayed
    }
    let t = reopen(&path);
    assert_eq!(t.stats().unwrap().row_count, 4);
    for (c, ns) in [(10, 1), (20, 1), (20, 2), (30, 1)] {
        assert!(is_dup(insert_one(&t, r("a", c, ns))), "{c} {ns}");
    }
    insert_one(&t, r("a", 30, 2)).unwrap();
    insert_one(&t, r("a", 25, 1)).unwrap();
    // clean restart: everything in chunks, the bound is rebuilt from them
    t.flush().unwrap();
    drop(t);
    let t = reopen(&path);
    for (c, ns) in [(10, 1), (20, 2), (25, 1), (30, 1), (30, 2)] {
        assert!(is_dup(insert_one(&t, r("a", c, ns))), "{c} {ns}");
    }
    insert_one(&t, r("a", 31, 1)).unwrap();
    assert_eq!(t.stats().unwrap().row_count, 7);
}

#[test]
fn a_spilled_statement_checks_against_its_own_staged_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), true);
    let mut b = t.begin_batch().unwrap();
    for c in 0..400 {
        put(&mut b, r("a", c, 1)).unwrap();
    }
    // 8 KiB per spill: the first rows are staged chunks by now
    assert!(is_dup(put(&mut b, r("a", 0, 1))));
    assert!(is_dup(put(&mut b, r("a", 17, 1))));
    assert!(is_dup(put(&mut b, r("a", 399, 1))));
    put(&mut b, r("a", 17, 2)).unwrap();
    assert!(is_dup(put(&mut b, r("a", 17, 2))));
    b.commit(false).unwrap();
    assert_eq!(t.stats().unwrap().row_count, 401);
    assert!(is_dup(insert_one(&t, r("a", 17, 2))));
    assert!(is_dup(insert_one(&t, r("a", 3, 1))));
}

#[test]
fn a_failed_row_leaves_the_statement_usable_and_earlier_rows_stay() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    insert_one(&t, r("a", 5, 5)).unwrap();
    let mut b = t.begin_batch().unwrap();
    put(&mut b, r("a", 6, 1)).unwrap();
    assert!(is_dup(put(&mut b, r("a", 5, 5))));
    put(&mut b, r("a", 7, 1)).unwrap();
    b.commit(false).unwrap();
    assert_eq!(scan_all(&t, true, false), vec![("a".into(), 5, 5), ("a".into(), 6, 1), ("a".into(), 7, 1)]);
}

#[test]
fn rows_of_one_timestamp_come_ordered_by_the_extra_columns() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    // same second, ns out of order, spread over a chunk, a second chunk and the MemTable
    insert_one(&t, r("a", 100, 5)).unwrap();
    insert_one(&t, r("a", 100, 9)).unwrap();
    insert_one(&t, r("a", 99, 4)).unwrap();
    t.flush().unwrap();
    insert_one(&t, r("a", 100, 1)).unwrap();
    insert_one(&t, r("a", 101, 2)).unwrap();
    t.flush().unwrap();
    insert_one(&t, r("a", 100, 7)).unwrap();
    insert_one(&t, r("a", 100, 3)).unwrap();
    insert_one(&t, r("a", 98, 8)).unwrap();
    insert_one(&t, r("b", 100, 6)).unwrap();
    let a_only =
        |rows: Vec<(String, i64, i64)>| rows.into_iter().filter(|x| x.0 == "a").map(|x| (x.1, x.2)).collect::<Vec<_>>();
    let want: Vec<(i64, i64)> = vec![(98, 8), (99, 4), (100, 1), (100, 3), (100, 5), (100, 7), (100, 9), (101, 2)];
    assert_eq!(a_only(scan_all(&t, true, false)), want);
    let mut rev = want.clone();
    rev.reverse();
    assert_eq!(a_only(scan_all(&t, true, true)), rev);

    // batch boundaries inside a timestamp group, and a change of direction
    let ids: Vec<u64> = t
        .series()
        .unwrap()
        .into_iter()
        .filter(|(_, tags)| tags[0] == Value::Bytes(b"a".to_vec()))
        .map(|(i, _)| i)
        .collect();
    let f = ScanFilter { series: Some(ids), ..ScanFilter::default() };
    let mut s = t.scan(&f, true).unwrap();
    let mut got = Vec::new();
    loop {
        let b = s.next_batch(2, false).unwrap();
        if b.rows() == 0 {
            break;
        }
        for i in 0..b.rows() {
            let row = b.row_at(i);
            let (Value::Timestamp(ts), Value::Int(ns)) = (&row[0], &row[3]) else { panic!() };
            got.push((ts / 1_000_000, *ns));
        }
    }
    assert_eq!(got, want, "two rows per batch");
    let mut s = t.scan(&f, true).unwrap();
    let mut seen = Vec::new();
    for _ in 0..4 {
        let (_, row) = s.next_row().unwrap().unwrap();
        let (Value::Timestamp(ts), Value::Int(ns)) = (&row[0], &row[3]) else { panic!() };
        seen.push((ts / 1_000_000, *ns));
    }
    assert_eq!(seen, want[..4]);
    let (_, row) = s.prev_row().unwrap().unwrap();
    let (Value::Timestamp(ts), Value::Int(ns)) = (&row[0], &row[3]) else { panic!() };
    assert_eq!((ts / 1_000_000, *ns), want[2], "prev after next: the row before the last one returned");
}

#[test]
fn exactly_one_of_two_racing_inserts_of_a_key_wins() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    for round in 0..200i64 {
        let barrier = Arc::new(Barrier::new(2));
        let wins = Arc::new(AtomicUsize::new(0));
        let dups = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let (t, barrier, wins, dups) = (t.clone(), barrier.clone(), wins.clone(), dups.clone());
                thread::spawn(move || {
                    let mut b = t.begin_batch().unwrap();
                    barrier.wait();
                    match put(&mut b, r("hot", 1000 + round / 4, round % 4)) {
                        Ok(()) => {
                            // keep the statement open a moment so the other one has to wait
                            thread::sleep(Duration::from_micros(200));
                            b.commit(false).unwrap();
                            wins.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(Error::Duplicate(_)) => {
                            dups.fetch_add(1, Ordering::SeqCst);
                            b.commit(false).unwrap();
                        }
                        Err(e) => panic!("{e}"),
                    }
                })
            })
            .collect();
        for th in threads {
            th.join().unwrap();
        }
        assert_eq!((wins.load(Ordering::SeqCst), dups.load(Ordering::SeqCst)), (1, 1), "round {round}");
    }
    assert_eq!(t.stats().unwrap().row_count, 200);
}

#[test]
fn many_writers_with_overlapping_keys_store_each_key_once() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), true);
    let stored = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..6)
        .map(|w| {
            let (t, stored) = (t.clone(), stored.clone());
            thread::spawn(move || {
                // every writer inserts the same 5 series x 120 timestamps, each in its own order
                for stmt in 0..6 {
                    let mut b = t.begin_batch().unwrap();
                    for k in 0..100i64 {
                        let i = (k * 7 + w * 13 + stmt * 31) % 600;
                        let (item, clock) = (format!("s{}", i % 5), i / 5);
                        match put(&mut b, r(&item, clock, 0)) {
                            Ok(()) => {
                                stored.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(Error::Duplicate(_)) => {}
                            Err(Error::Deadlock(_)) => {
                                // a statement that closes a cycle is stopped; the rest is not retried here
                                break;
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                    b.commit(false).unwrap();
                }
            })
        })
        .collect();
    for th in threads {
        th.join().unwrap();
    }
    let rows = scan_all(&t, false, false);
    let mut keys: Vec<_> = rows.iter().map(|x| (x.0.clone(), x.1, x.2)).collect();
    let n = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), n, "no key stored twice");
    assert_eq!(n, stored.load(Ordering::SeqCst), "every accepted row is stored");
    assert!(n > 100, "{n}");
}

#[test]
fn statements_that_wait_for_each_other_are_told_apart_from_a_plain_wait() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    let mut a = t.begin_batch().unwrap();
    let mut b = t.begin_batch().unwrap();
    a.write(r("s1", 1, 1)).unwrap();
    b.write(r("s2", 1, 1)).unwrap();
    // a wants s2 (held by b): it waits, in slices
    assert!(matches!(a.write(r("s2", 2, 1)), Err(Error::Busy(_))));
    // b wants s1 (held by a), but a is not waiting at this moment: a plain wait
    assert!(matches!(b.write(r("s1", 2, 1)), Err(Error::Busy(_))));
    // once a finishes, b goes on
    a.commit(false).unwrap();
    b.write(r("s1", 2, 1)).unwrap();
    b.commit(false).unwrap();
    assert_eq!(t.stats().unwrap().row_count, 3);
}

#[test]
fn a_cycle_of_waiting_statements_is_reported_as_a_deadlock() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    let barrier = Arc::new(Barrier::new(2));
    let outcome = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = [("s1", "s2"), ("s2", "s1")]
        .into_iter()
        .map(|(first, second)| {
            let (t, barrier, outcome) = (t.clone(), barrier.clone(), outcome.clone());
            thread::spawn(move || {
                let mut b = t.begin_batch().unwrap();
                b.write(r(first, 1, 1)).unwrap();
                barrier.wait();
                match put(&mut b, r(second, 2, 1)) {
                    Ok(()) => {}
                    Err(Error::Deadlock(_)) => {
                        outcome.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => panic!("{e}"),
                }
                b.commit(false).unwrap();
            })
        })
        .collect();
    for th in threads {
        th.join().unwrap();
    }
    assert_eq!(outcome.load(Ordering::SeqCst), 1, "one of them is stopped, the other goes on");
    assert!(t.stats().unwrap().row_count >= 3);
}

#[test]
fn unchecked_inserts_skip_the_check_but_keep_the_state_for_later_ones() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    insert_one(&t, r("a", 10, 1)).unwrap();
    let mut b = t.begin_batch().unwrap();
    b.set_unique_checks(false);
    b.write(r("a", 10, 1)).unwrap(); // duplicate, accepted: the risk of unique_checks=0
    b.write(r("a", 11, 1)).unwrap();
    b.commit(false).unwrap();
    assert_eq!(t.stats().unwrap().row_count, 3);
    // checked again: what was recorded is known
    assert!(is_dup(insert_one(&t, r("a", 11, 1))));
    assert!(is_dup(insert_one(&t, r("a", 10, 1))));
}

#[test]
fn truncate_forgets_the_keys_and_releases_open_statements() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    insert_one(&t, r("a", 10, 1)).unwrap();
    let mut open = t.begin_batch().unwrap();
    put(&mut open, r("a", 11, 1)).unwrap();
    t.truncate().unwrap();
    assert!(open.commit(false).is_err(), "a statement older than the TRUNCATE cannot commit");
    insert_one(&t, r("a", 10, 1)).unwrap();
    assert!(is_dup(insert_one(&t, r("a", 10, 1))));
    assert_eq!(t.stats().unwrap().row_count, 1);
}

#[test]
fn a_dropped_statement_leaves_no_phantom_keys_behind() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    let mut b = t.begin_batch().unwrap();
    put(&mut b, r("a", 10, 1)).unwrap();
    b.abort();
    insert_one(&t, r("a", 10, 1)).unwrap(); // the aborted row never existed
    assert!(is_dup(insert_one(&t, r("a", 10, 1))));
    assert_eq!(t.stats().unwrap().row_count, 1);
}

#[test]
fn write_shared_checks_single_rows() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    t.write_shared(r("a", 1, 1)).unwrap();
    assert!(is_dup(t.write_shared(r("a", 1, 1))));
    assert!(matches!(t.write(r("a", 2, 1)), Err(Error::Unsupported(_))));
}

#[test]
fn a_table_without_extra_columns_is_unique_by_series_and_time() {
    let dir = tempfile::tempdir().unwrap();
    let cfg =
        TableConfig::new(dir.path().join("t"), schema(), &RawOptions::default()).unwrap().with_unique(vec![]).unwrap();
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    insert_one(&t, r("a", 1, 1)).unwrap();
    assert!(is_dup(insert_one(&t, r("a", 1, 2))), "ns is not part of the key");
    insert_one(&t, r("b", 1, 1)).unwrap();
}

#[test]
fn invalid_key_columns_are_refused() {
    let s = schema();
    assert!(moosedb_core::UniqueSpec::new(&s, vec![2]).is_err(), "FLOAT64 is not a key column");
    assert!(moosedb_core::UniqueSpec::new(&s, vec![3, 3]).is_err());
    assert!(moosedb_core::UniqueSpec::new(&s, vec![9]).is_err());
    assert!(moosedb_core::UniqueSpec::new(&s, vec![3, 3, 3]).is_err());
}

#[test]
fn an_unchecked_statement_that_does_not_commit_leaves_no_phantom_keys() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    insert_one(&t, r("a", 10, 1)).unwrap();
    let mut b = t.begin_batch().unwrap();
    b.set_unique_checks(false);
    b.write(r("a", 20, 1)).unwrap();
    b.write(r("a", 20, 2)).unwrap();
    b.abort(); // e.g. a commit that failed: the rows never became visible
    insert_one(&t, r("a", 20, 1)).unwrap();
    insert_one(&t, r("a", 20, 2)).unwrap();
    assert!(is_dup(insert_one(&t, r("a", 10, 1))));
    assert_eq!(t.stats().unwrap().row_count, 3);
}

/// Sorted scans hand out whole stretches of a run at once; the rows (and their
/// order, ties included) must not depend on how big the batches are.
#[test]
fn sorted_scans_return_the_same_rows_whatever_the_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    // Overlapping chunks (rounds interleave in time), timestamps repeated with
    // different ns, a MemTable tail, three series.
    for round in 0..4i64 {
        for item in ["a", "b", "c"] {
            let mut b = t.begin_batch().unwrap();
            for k in 0..120i64 {
                let clock = k * 4 + round;
                put(&mut b, r(item, clock, 1)).unwrap();
                if k % 5 == 0 {
                    put(&mut b, r(item, clock, 2)).unwrap();
                }
            }
            b.commit(false).unwrap();
        }
        if round < 3 {
            t.flush().unwrap();
        }
    }
    let collect = |filter: &ScanFilter, max: usize| -> Vec<(i64, String, i64)> {
        let mut s = t.scan(filter, true).unwrap();
        let mut out = Vec::new();
        loop {
            let b = s.next_batch(max, false).unwrap();
            if b.rows() == 0 {
                return out;
            }
            for i in 0..b.rows() {
                let row = b.row_at(i);
                let (Value::Timestamp(ts), Value::Bytes(item), Value::Int(ns)) = (&row[0], &row[1], &row[3]) else {
                    panic!("{row:?}")
                };
                out.push((*ts, String::from_utf8(item.clone()).unwrap(), *ns));
            }
        }
    };
    let series: Vec<u64> = t.series().unwrap().into_iter().map(|(id, _)| id).collect();
    let filters = [
        ScanFilter::default(),
        ScanFilter::range(100 * 1_000_000, 300 * 1_000_000),
        ScanFilter { series: Some(vec![series[1]]), ..ScanFilter::default() },
        ScanFilter { series: Some(vec![series[0], series[2]]), ..ScanFilter::range(0, 200 * 1_000_000) },
    ];
    for f in &filters {
        let one = collect(f, 1);
        assert!(!one.is_empty());
        assert!(one.windows(2).all(|w| (w[0].0, w[0].2) <= (w[1].0, w[1].2) || w[0].0 < w[1].0), "ordered by time");
        for max in [2, 3, 7, 64, 100_000] {
            assert_eq!(collect(f, max), one, "batch size {max}");
        }
    }
    assert_eq!(collect(&ScanFilter::default(), 1).len() as u64, t.stats().unwrap().row_count);
}

/// A projection without the extra key columns does not decode them unless rows
/// of one timestamp meet; the rows still leave in the tie order (the value
/// column encodes `ns`, so the projected scan shows the order).
#[test]
fn tie_order_does_not_need_the_extra_columns_in_the_projection() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    for round in 0..3i64 {
        let mut b = t.begin_batch().unwrap();
        for item in ["a", "b"] {
            for k in 0..50i64 {
                // timestamps repeat across rounds (different ns), so runs of
                // different chunks hold rows of the same timestamp
                let clock = k * 3;
                put(&mut b, r(item, clock, 10 - round)).unwrap();
            }
        }
        b.commit(false).unwrap();
        if round < 2 {
            t.flush().unwrap();
        }
    }
    let only_clock_and_value = [true, false, true, false];
    let mut full = t.scan(&ScanFilter::default(), true).unwrap();
    let mut proj = t.scan_projected(&ScanFilter::default(), true, Some(&only_clock_and_value)).unwrap();
    let mut n = 0;
    loop {
        let (a, b) = (full.next_row().unwrap(), proj.next_row().unwrap());
        match (a, b) {
            (None, None) => break,
            (Some((pa, ra)), Some((pb, rb))) => {
                assert_eq!(pa, pb);
                assert_eq!((&ra[0], &ra[2]), (&rb[0], &rb[2]));
                assert_eq!(rb[3], Value::Null, "ns was not asked for");
                n += 1;
            }
            other => panic!("different lengths: {other:?}"),
        }
    }
    assert_eq!(n, 300);
}

/// A UNIQUE statement reuses the series ids it resolved while admitting rows.
/// Even when it spills (which clears that bookkeeping) and keeps writing the
/// same new series, and when a second statement creates other series meanwhile,
/// every row ends up in the right series.
#[test]
fn a_spilling_statement_and_a_concurrent_one_keep_every_row_in_its_series() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), true); // 8 KiB MemTable: spills early
    let mut b1 = t.begin_batch().unwrap();
    let mut b2 = t.begin_batch().unwrap();
    put(&mut b2, r("other", 1, 1)).unwrap();
    for clock in 0..400i64 {
        put(&mut b1, r("new", clock, 1)).unwrap(); // spills part-way, keeps writing "new"
        put(&mut b1, r(&format!("s{}", clock % 7), clock, 1)).unwrap();
    }
    b1.commit(false).unwrap();
    put(&mut b2, r("new", 1000, 1)).unwrap(); // "new" is free now
    b2.commit(false).unwrap();
    let all = scan_all(&t, true, false);
    let count = |name: &str| all.iter().filter(|x| x.0 == name).count();
    assert_eq!(count("new"), 401);
    assert_eq!(count("other"), 1);
    assert_eq!((0..7).map(|i| count(&format!("s{i}"))).sum::<usize>(), 400);
    assert_eq!(t.series().unwrap().len(), 9);
    assert!(is_dup(insert_one(&t, r("new", 1000, 1))));
    assert!(is_dup(insert_one(&t, r("new", 399, 1))));
}
