//! Design A: an UPDATE statement may hold 4 x MEMTABLE_SIZE of pairs (it used to be
//! 1 x). Own test binary: heavy, and the global batch budget stays at its default.

#![forbid(unsafe_code)]

use std::sync::Arc;

use moosedb_core::batch::batch_memory_in_use;
use moosedb_core::{Column, ColumnType, Error, RawOptions, Row, Schema, Table, TableConfig, Value};

const BASE: i64 = 1_800_000_000_000_000;
/// 3 x the pairs the old limit (1 x 64 MiB) let through (150 k measured).
const BIG: usize = 450_000;
/// The two tests share the process-wide batch memory counter: one at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "v".into(), ty: ColumnType::Float64 },
        ],
        0,
    )
    .unwrap()
}

fn row(i: usize, v: usize) -> Row {
    vec![Value::Timestamp(BASE + i as i64 * 1000), Value::Bytes(format!("h{}", i % 60).into_bytes()), Value::Float64(v as f64)]
}

/// `(count, sum of v)` of every visible row, and whether each `v` is `want(i)`.
fn check(t: &Table, want: impl Fn(usize) -> usize) -> usize {
    let mut s = t.scan(&Default::default(), false).unwrap();
    let mut n = 0usize;
    while let Some((_, r)) = s.next_row().unwrap() {
        let (Value::Timestamp(ts), Value::Float64(v)) = (&r[0], &r[2]) else { panic!("{r:?}") };
        let i = ((ts - BASE) / 1000) as usize;
        assert_eq!(*v as usize, want(i), "row {i}");
        n += 1;
    }
    n
}

#[test]
fn an_update_of_three_times_the_old_limit_commits_replays_and_flushes() {
    let _g = SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let cfg = TableConfig::new(dir.path().join("t"), schema(), &RawOptions::default()).unwrap();
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg.clone()).unwrap());
    let mut b = t.begin_batch().unwrap();
    for i in 0..BIG {
        b.write(row(i, i)).unwrap();
    }
    b.commit(true).unwrap();
    t.flush().unwrap(); // the old rows are in chunks: the commit tombstones them

    let mut b = t.begin_batch().unwrap();
    for i in 0..BIG {
        b.update(row(i, i), row(i, i + 7)).unwrap();
    }
    let per_pair = batch_memory_in_use() / BIG as u64;
    eprintln!("memory per pair: {per_pair} B");
    b.commit(true).unwrap(); // ~450 k rows land in the MemTable at once
    assert_eq!(batch_memory_in_use(), 0);
    assert_eq!(check(&t, |i| i + 7), BIG);

    // The flush after the big commit copes, and so does a write behind it.
    t.write(row(BIG, BIG)).unwrap();
    t.flush().unwrap();
    assert_eq!(check(&t, |i| if i < BIG { i + 7 } else { i }), BIG + 1);
    assert!(t.check().unwrap().is_empty());

    // Replay: a second big update that is still in the WAL at the reopen.
    let mut b = t.begin_batch().unwrap();
    for i in 0..BIG {
        b.update(row(i, i + 7), row(i, i + 9)).unwrap();
    }
    b.commit(true).unwrap();
    drop(t);
    let t = Table::open(cfg).unwrap();
    assert_eq!(check(&t, |i| if i < BIG { i + 9 } else { i }), BIG + 1);
    t.flush().unwrap();
    assert_eq!(check(&t, |i| if i < BIG { i + 9 } else { i }), BIG + 1);
}

#[test]
fn an_update_past_the_new_limit_is_refused_and_nothing_is_applied() {
    let _g = SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let cfg = TableConfig::new(dir.path().join("t"), schema(), &RawOptions::default()).unwrap();
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    t.write(row(0, 0)).unwrap();
    let mut b = t.begin_batch().unwrap();
    let mut n = 0usize;
    let err = loop {
        match b.update(row(n, 0), row(n, 1)) {
            Ok(()) => n += 1,
            Err(e) => break e,
        }
    };
    eprintln!("update limit: {n} pairs, {} B/pair", batch_memory_in_use() / n as u64);
    assert!(matches!(err, Error::Unsupported(_)), "{err}");
    assert!(n > 3 * 150_000 && n < 5 * 150_000, "{n}");
    drop(b);
    assert_eq!(batch_memory_in_use(), 0);
    assert_eq!(check(&t, |_| 0), 1);
}
