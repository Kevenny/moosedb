//! A flush seals the MemTable outside the table mutex (see `Table::begin_flush`):
//! what readers, writers, UNIQUE checks, DELETE/TRUNCATE and recovery see while
//! it runs, and what is left when it fails.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::chunk::parse_chunk_id;
use crate::delete::RangeDelete;
use crate::error::{invalid, Error, Result};
use crate::options::{RawOptions, TableConfig};
use crate::scan::ScanFilter;
use crate::schema::{Column, ColumnType, Row, Schema, Value};
use crate::table::{Table, FLUSH_HOOK};

fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "v".into(), ty: ColumnType::Float64 },
            Column { name: "n".into(), ty: ColumnType::Int64 },
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

fn row(i: i64, host: &str) -> Row {
    vec![
        Value::Timestamp(1_700_000_000_000_000 + i),
        Value::Bytes(host.as_bytes().to_vec()),
        Value::Float64(i as f64),
        Value::Int(i),
    ]
}

fn ns(r: &Row) -> i64 {
    match &r[3] {
        Value::Int(n) => *n,
        v => panic!("{v:?}"),
    }
}

/// The `n` column of every row a fresh scan returns, in no particular order.
fn all(t: &Table) -> Vec<i64> {
    let mut s = t.scan(&ScanFilter::default(), false).unwrap();
    let mut out = Vec::new();
    while let Some(r) = s.next_row().unwrap() {
        out.push(ns(&r.1));
    }
    out
}

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort_unstable();
    v
}

fn insert(t: &Arc<Table>, r: Row) -> Result<()> {
    let mut b = t.begin_batch()?;
    loop {
        match b.write(r.clone()) {
            Err(Error::Busy(_)) => continue,
            Err(e) => return Err(e),
            Ok(()) => break,
        }
    }
    b.commit(false)
}

/// A flush paused after its chunks are written and before they are installed.
struct Paused {
    started: Receiver<()>,
    go: Sender<()>,
    flusher: JoinHandle<Result<()>>,
}

impl Paused {
    /// Starts `t.flush()` on another thread; returns once the rows are taken,
    /// the chunks written and the flush waits for `release`.
    fn start(t: &Arc<Table>) -> Paused {
        let (started_tx, started) = channel();
        let (go, go_rx) = channel::<()>();
        let t = t.clone();
        let flusher = thread::spawn(move || {
            FLUSH_HOOK.with(|h| {
                let paused = std::cell::Cell::new(false);
                *h.borrow_mut() = Some(Box::new(move || {
                    if paused.replace(true) {
                        return Ok(()); // only the first flush of the thread pauses
                    }
                    started_tx.send(()).unwrap();
                    let _ = go_rx.recv_timeout(Duration::from_secs(60));
                    Ok(())
                }));
            });
            t.flush()
        });
        started.recv_timeout(Duration::from_secs(60)).expect("the flush reached its pause");
        Paused { started, go, flusher }
    }

    fn release(self) -> Result<()> {
        let _ = &self.started;
        self.go.send(()).unwrap();
        self.flusher.join().unwrap()
    }
}

#[test]
fn readers_and_writers_see_every_row_once_while_a_flush_runs() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    for i in 0..100 {
        t.write(row(i, if i % 2 == 0 { "a" } else { "b" })).unwrap();
    }
    let before = t.scan(&ScanFilter::default(), false).unwrap();
    let flush = Paused::start(&t);

    assert_eq!(sorted(all(&t)), (0..100).collect::<Vec<_>>(), "rows being flushed are visible, once");
    for i in 100..110 {
        t.write(row(i, "a")).unwrap();
    }
    assert_eq!(sorted(all(&t)), (0..110).collect::<Vec<_>>());
    let st = t.stats().unwrap();
    assert_eq!((st.row_count, st.memtable_rows, st.chunk_count), (110, 110, 0));
    let mut sorted_scan = t.scan(&ScanFilter::default(), true).unwrap();
    let mut n = 0;
    while sorted_scan.next_row().unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 110);

    flush.release().unwrap();
    assert_eq!(sorted(all(&t)), (0..110).collect::<Vec<_>>(), "no loss, no duplicate with the new chunks");
    let st = t.stats().unwrap();
    assert_eq!((st.row_count, st.memtable_rows, st.chunk_count), (110, 10, 1));
    // A scan opened before the flush ended keeps its own view.
    let mut before = before;
    let mut n = 0;
    while before.next_row().unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 100);
    drop(t);
    let t = Arc::new(Table::open(config(&dir.path().join("t"), false)).unwrap());
    assert_eq!(sorted(all(&t)), (0..110).collect::<Vec<_>>(), "recovery: chunk plus WAL, no duplicate");
}

#[test]
fn unique_finds_a_duplicate_of_a_row_that_is_being_flushed() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), true);
    for i in 0..50 {
        insert(&t, row(i, "a")).unwrap();
    }
    let flush = Paused::start(&t);
    // Same (host, ts, n) as a row in `flushing`; the series' bounds do not exclude it.
    assert!(matches!(insert(&t, row(7, "a")), Err(Error::Duplicate(_))));
    // Same time, different extra: not a duplicate.
    let mut other = row(7, "a");
    other[3] = Value::Int(1_000);
    insert(&t, other).unwrap();
    insert(&t, row(60, "a")).unwrap();
    flush.release().unwrap();
    assert!(matches!(insert(&t, row(7, "a")), Err(Error::Duplicate(_))), "now found in the chunk");
    assert!(matches!(insert(&t, row(60, "a")), Err(Error::Duplicate(_))), "and in the MemTable");
    assert_eq!(all(&t).len(), 52);
}

#[test]
fn delete_and_truncate_wait_for_the_flush_and_then_apply_to_its_rows() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), false);
    for i in 0..100 {
        t.write(row(i, "a")).unwrap();
    }
    let flush = Paused::start(&t);
    assert_eq!(t.count_range(None, i64::MIN, i64::MAX).unwrap(), 100, "counting does not wait and sees the rows");

    let (done_tx, done_rx) = channel();
    let deleter = {
        let t = t.clone();
        thread::spawn(move || {
            let r = t.delete_range(None, i64::MIN, 1_700_000_000_000_049, None, false);
            done_tx.send(()).unwrap();
            r
        })
    };
    assert!(done_rx.recv_timeout(Duration::from_millis(300)).is_err(), "the DELETE waits for the flush");
    // The table is not held while it waits.
    t.write(row(200, "a")).unwrap();
    assert_eq!(t.stats().unwrap().row_count, 101);
    flush.release().unwrap();
    assert_eq!(deleter.join().unwrap().unwrap(), RangeDelete::Deleted(50));
    assert_eq!(sorted(all(&t)), (50..100).chain([200]).collect::<Vec<_>>());

    for i in 300..320 {
        t.write(row(i, "a")).unwrap();
    }
    let flush = Paused::start(&t);
    let (done_tx, done_rx) = channel();
    let truncator = {
        let t = t.clone();
        thread::spawn(move || {
            let r = t.truncate();
            done_tx.send(()).unwrap();
            r
        })
    };
    assert!(done_rx.recv_timeout(Duration::from_millis(300)).is_err(), "TRUNCATE waits for the flush");
    flush.release().unwrap();
    truncator.join().unwrap().unwrap();
    assert!(all(&t).is_empty());
    drop(t);
    let t = Arc::new(Table::open(config(&dir.path().join("t"), false)).unwrap());
    assert!(all(&t).is_empty(), "the flush's chunks did not come back");
}

fn chunk_files(dir: &Path) -> usize {
    fs::read_dir(dir).unwrap().filter(|e| parse_chunk_id(&e.as_ref().unwrap().file_name().to_string_lossy()).is_some()).count()
}

#[test]
fn a_flush_that_fails_after_writing_its_chunks_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    for i in 0..40 {
        t.write(row(i, "a")).unwrap();
    }
    FLUSH_HOOK.with(|h| *h.borrow_mut() = Some(Box::new(|| Err(invalid("injected failure")))));
    assert!(t.flush().is_err());
    FLUSH_HOOK.with(|h| *h.borrow_mut() = None);
    assert_eq!(chunk_files(&path), 0, "chunks of the failed flush are discarded");
    assert_eq!(sorted(all(&t)), (0..40).collect::<Vec<_>>(), "rows are back in the MemTable");
    assert_eq!(t.stats().unwrap().memtable_rows, 40);
    // Writable, and the next flush takes the old rows with the new ones.
    for i in 40..50 {
        t.write(row(i, "a")).unwrap();
    }
    t.flush().unwrap();
    assert_eq!(chunk_files(&path), 1);
    assert_eq!(sorted(all(&t)), (0..50).collect::<Vec<_>>());
    drop(t);
    let t = Table::open(config(&path, false)).unwrap();
    assert_eq!(sorted(all(&t)), (0..50).collect::<Vec<_>>());
}

/// Copies the table directory as a crash would leave it (files as they are now).
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

#[test]
fn a_crash_between_the_chunks_and_the_manifest_recovers_everything_from_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let crashed = dir.path().join("crashed");
    let t = open_new(&path, false);
    for i in 0..60 {
        t.write(row(i, "a")).unwrap();
    }
    t.sync_wal(true).unwrap();
    // A statement that logged its rows before the flush and commits during it.
    let mut open_batch = t.begin_batch().unwrap();
    for i in 1000..1005 {
        open_batch.write(row(i, "b")).unwrap();
    }
    let (p, c) = (path.clone(), crashed.clone());
    FLUSH_HOOK.with(|h| *h.borrow_mut() = Some(Box::new(move || {
        copy_dir(&p, &c);
        Ok(())
    })));
    t.flush().unwrap();
    FLUSH_HOOK.with(|h| *h.borrow_mut() = None);
    assert!(chunk_files(&crashed) >= 1, "the copy holds the chunk files, not yet listed");
    let r = Table::open(config(&crashed, false)).unwrap();
    assert_eq!(sorted(all(&r)), (0..60).collect::<Vec<_>>(), "rows come back from the WAL");
    assert_eq!(r.chunks().unwrap().len(), 0);
    assert_eq!(chunk_files(&crashed), 0, "unlisted chunks are removed by recovery");

    // The batch commits after the flush point; crash right after the flush ended.
    open_batch.commit(true).unwrap();
    t.sync_wal(true).unwrap();
    let again = dir.path().join("again");
    copy_dir(&path, &again);
    let r = Table::open(config(&again, false)).unwrap();
    assert_eq!(sorted(all(&r)), (0..60).chain(1000..1005).collect::<Vec<_>>());
}

#[test]
fn a_batch_open_across_a_flush_survives_a_crash_after_the_flush_installs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    for i in 0..30 {
        t.write(row(i, "a")).unwrap();
    }
    let mut b = t.begin_batch().unwrap();
    for i in 500..505 {
        b.write(row(i, "b")).unwrap();
    }
    let flush = Paused::start(&t);
    // Commits while the flush is between its sections: COMMIT lands in the new
    // segment, its rows are in the old one, which the checkpoint must keep.
    b.commit(true).unwrap();
    flush.release().unwrap();
    let crashed = dir.path().join("crashed");
    copy_dir(&path, &crashed);
    let r = Table::open(config(&crashed, false)).unwrap();
    assert_eq!(sorted(all(&r)), (0..30).chain(500..505).collect::<Vec<_>>());
    assert_eq!(sorted(all(&t)), (0..30).chain(500..505).collect::<Vec<_>>());
}

#[test]
fn concurrent_writers_and_readers_with_continuous_flushes_lose_and_duplicate_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let mut cfg = config(&path, false);
    cfg.opts.memtable_size_bytes = 16 * 1024;
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    const WRITERS: i64 = 4;
    const PER: i64 = 3000;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let (t, stop) = (t.clone(), stop.clone());
            thread::spawn(move || {
                let mut last = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let v = all(&t);
                    let distinct: HashSet<i64> = v.iter().copied().collect();
                    assert_eq!(distinct.len(), v.len(), "a row seen twice");
                    assert!(v.len() >= last, "rows vanished: {} < {last}", v.len());
                    last = v.len();
                }
            })
        })
        .collect();
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let t = t.clone();
            thread::spawn(move || {
                for i in 0..PER {
                    let host = format!("h{w}");
                    if i % 7 == 0 {
                        let mut b = t.begin_batch().unwrap();
                        for k in 0..3 {
                            b.write(row(w * PER + i + k * 100_000, &host)).unwrap();
                        }
                        b.commit(false).unwrap();
                    } else {
                        t.write(row(w * PER + i, &host)).unwrap();
                    }
                }
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
    t.flush().unwrap();
    let mut want: Vec<i64> = Vec::new();
    for w in 0..WRITERS {
        for i in 0..PER {
            want.push(w * PER + i);
            if i % 7 == 0 {
                want.extend((1..3).map(|k| w * PER + i + k * 100_000));
            }
        }
    }
    // `row(i)` of the batch rows with k = 0 is the plain one: counted once above.
    assert_eq!(sorted(all(&t)), sorted(want.clone()));
    t.sync_wal(true).unwrap();
    drop(t);
    let t = Table::open(config(&path, false)).unwrap();
    assert_eq!(sorted(all(&t)), sorted(want));
}

fn small(path: &Path, bytes: u64) -> TableConfig {
    let mut cfg = config(path, false);
    cfg.opts.memtable_size_bytes = bytes;
    cfg
}

fn fail_flushes() {
    FLUSH_HOOK.with(|h| *h.borrow_mut() = Some(Box::new(|| Err(invalid("injected failure")))));
}

fn heal_flushes() {
    FLUSH_HOOK.with(|h| *h.borrow_mut() = None);
}

#[test]
fn sync_wal_after_the_flush_rotated_the_log_still_covers_earlier_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    for i in 0..20 {
        t.write(row(i, "a")).unwrap();
    }
    // Rows 0..20 are only handed to the OS; the rotation must make them durable
    // before the new segment exists.
    let flush = Paused::start(&t);
    t.write(row(100, "a")).unwrap();
    t.sync_wal(true).unwrap();
    let crashed = dir.path().join("crashed");
    copy_dir(&path, &crashed);
    flush.release().unwrap();
    let r = Table::open(config(&crashed, false)).unwrap();
    assert_eq!(sorted(all(&r)), (0..20).chain([100]).collect::<Vec<_>>());
}

#[test]
fn backpressure_reports_full_at_twice_the_limit_when_flushes_keep_failing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let cfg = small(&path, 4096);
    Table::create(&cfg).unwrap();
    let t = Table::open(cfg).unwrap();
    fail_flushes();
    let mut ok = 0;
    let mut err = None;
    for i in 0..5000 {
        match t.write(row(i, "a")) {
            Ok(()) => ok += 1,
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    heal_flushes();
    assert!(matches!(err, Some(Error::Full(_))), "{err:?}");
    assert_eq!(all(&t).len(), ok, "rows of the failed flushes are all still there");
    // Once flushes work again, writes go through and nothing was lost.
    t.write(row(9999, "a")).unwrap();
    t.flush().unwrap();
    assert_eq!(all(&t).len(), ok + 1);
}

#[test]
fn two_failed_flushes_in_a_row_then_truncate_or_success() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, false);
    for i in 0..10 {
        t.write(row(i, "a")).unwrap();
    }
    fail_flushes();
    assert!(t.flush().is_err());
    t.write(row(10, "a")).unwrap();
    assert!(t.flush().is_err());
    heal_flushes();
    assert_eq!(sorted(all(&t)), (0..11).collect::<Vec<_>>());
    t.flush().unwrap();
    assert_eq!(sorted(all(&t)), (0..11).collect::<Vec<_>>());
    t.write(row(11, "a")).unwrap();
    fail_flushes();
    assert!(t.flush().is_err());
    heal_flushes();
    t.truncate().unwrap();
    assert!(all(&t).is_empty());
    t.write(row(12, "a")).unwrap();
    t.sync_wal(true).unwrap();
    drop(t);
    let t = Table::open(config(&path, false)).unwrap();
    assert_eq!(all(&t), vec![12]);
}

#[test]
fn a_spilled_batch_commits_while_a_flush_runs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let cfg = small(&path, 8 * 1024);
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    for i in 0..20 {
        t.write(row(i, "a")).unwrap();
    }
    let mut b = t.begin_batch().unwrap();
    for i in 1000..1300 {
        b.write(row(i, "b")).unwrap();
    }
    let flush = Paused::start(&t);
    b.commit(true).unwrap();
    flush.release().unwrap();
    let want: Vec<i64> = (0..20).chain(1000..1300).collect();
    assert_eq!(sorted(all(&t)), want);
    let crashed = dir.path().join("crashed");
    copy_dir(&path, &crashed);
    let r = Table::open(config(&crashed, false)).unwrap();
    assert_eq!(sorted(all(&r)), want);
}

#[test]
fn rows_that_refill_the_memtable_during_a_flush_are_flushed_right_after() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let cfg = small(&path, 8 * 1024);
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    for i in 0..10 {
        t.write(row(i, "a")).unwrap();
    }
    let flush = Paused::start(&t);
    // Over the limit (8 KiB) but under twice of it, where writers would wait.
    let mut n = 10;
    while t.stats().unwrap().data_bytes < 10_000 {
        t.write(row(n, "a")).unwrap();
        n += 1;
    }
    flush.release().unwrap();
    assert_eq!(t.stats().unwrap().memtable_rows, 0, "the next flush ran without waiting for another write");
    assert_eq!(sorted(all(&t)), (0..n).collect::<Vec<_>>());
}
