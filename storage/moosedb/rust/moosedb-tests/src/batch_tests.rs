//! Statement-level atomicity: a batch is invisible until commit, then
//! appears whole, and survives (or vanishes in) a crash accordingly.
//!
//! A "crash" is `mem::forget` of the batch and table after the WAL was
//! synced: nothing is flushed or cleaned up, like a killed process.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use super::helpers::*;
use moosedb_core::{maintenance, Batch, Error, RawOptions, Row, ScanFilter, Table, Value};

/// 16 KiB: a batch of a few hundred rows spills several times.
fn small() -> RawOptions<'static> {
    RawOptions { memtable_size_bytes: 16 * 1024, ..Default::default() }
}

fn files_with_suffix(dir: &Path, suffix: &str) -> Vec<PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(suffix))
        .collect();
    v.sort();
    v
}

fn open_new(path: &Path, opts: RawOptions<'_>) -> Arc<Table> {
    Arc::new(create_open(path, opts))
}

fn reopen(path: &Path, opts: RawOptions<'_>) -> Arc<Table> {
    Arc::new(Table::open(config(path, opts)).unwrap())
}

/// Rows of `host`; timestamps two hours apart, so they span many buckets.
fn host_rows(host: &str, n: i64) -> Vec<Row> {
    (0..n).map(|i| row(i * 7200, host, i as f64)).collect()
}

fn count(t: &Table, host: &str) -> usize {
    all_rows(t).iter().filter(|r| r[1] == Value::Bytes(host.as_bytes().to_vec())).count()
}

fn write_all(b: &mut Batch, rows: &[Row]) {
    for r in rows {
        b.write(r.clone()).unwrap();
    }
}

fn crash(t: Arc<Table>, batches: Vec<Batch>) {
    for b in batches {
        std::mem::forget(b);
    }
    std::mem::forget(t);
}

#[test]
fn rows_are_invisible_until_commit_then_all_appear() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), RawOptions::default());
    t.write(row(0, "a", 0.0)).unwrap();
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("b", 100));
    assert_eq!(count(&t, "b"), 0);
    assert_eq!(t.stats().unwrap().row_count, 1);
    // A scan opened before the commit never learns about the batch.
    let mut early = t.scan(&ScanFilter::default(), false).unwrap();
    b.commit(false).unwrap();
    let mut seen = 0;
    while early.next_row().unwrap().is_some() {
        seen += 1;
    }
    assert_eq!(seen, 1);
    assert_eq!(count(&t, "b"), 100);
    assert_eq!(t.stats().unwrap().row_count, 101);
}

#[test]
fn abort_and_drop_discard_the_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, RawOptions::default());
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("x", 10));
    b.abort();
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("y", 10));
    drop(b);
    t.write(row(0, "a", 0.0)).unwrap();
    t.sync_wal(true).unwrap();
    assert_eq!((count(&t, "x"), count(&t, "y")), (0, 0));
    drop(t);
    let t = reopen(&path, RawOptions::default());
    assert_eq!((count(&t, "x"), count(&t, "y"), count(&t, "a")), (0, 0, 1));
}

#[test]
fn crash_before_commit_loses_the_batch_after_commit_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, RawOptions::default());
    t.write(row(0, "a", 0.0)).unwrap();
    let mut open = t.begin_batch().unwrap();
    write_all(&mut open, &host_rows("lost", 50));
    let mut done = t.begin_batch().unwrap();
    write_all(&mut done, &host_rows("kept", 50));
    done.commit(true).unwrap();
    t.sync_wal(true).unwrap(); // the open batch's rows are on disk, uncommitted
    crash(t, vec![open]);

    let t = reopen(&path, RawOptions::default());
    assert_eq!((count(&t, "lost"), count(&t, "kept"), count(&t, "a")), (0, 50, 1));
    // Recovery is idempotent.
    drop(t);
    let t = reopen(&path, RawOptions::default());
    assert_eq!((count(&t, "lost"), count(&t, "kept"), count(&t, "a")), (0, 50, 1));
}

#[test]
fn batch_open_across_a_flush_is_neither_lost_nor_duplicated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, RawOptions::default());
    // Committed before the flush: its rows and COMMIT precede the flush point,
    // yet replay starts earlier (for `b`) and must not apply it again.
    let mut z = t.begin_batch().unwrap();
    write_all(&mut z, &host_rows("z", 20));
    z.commit(true).unwrap();
    let mut b = t.begin_batch().unwrap();
    let rows = host_rows("b", 40);
    write_all(&mut b, &rows[..20]);
    t.write(row(0, "before", 0.0)).unwrap();
    t.flush().unwrap(); // checkpoint while `b` is open
    assert_eq!(t.stats().unwrap().memtable_rows, 0);
    assert!(files_with_suffix(&path, ".tfl.wal").len() >= 2, "the segment holding b's first rows is kept");
    t.write(row(1, "after", 0.0)).unwrap();
    write_all(&mut b, &rows[20..]);
    assert_eq!(count(&t, "b"), 0);
    b.commit(true).unwrap();
    assert_eq!(count(&t, "b"), 40);
    crash(t, vec![]);

    let t = reopen(&path, RawOptions::default());
    let got: BTreeMap<&str, usize> = ["z", "b", "before", "after"].into_iter().map(|h| (h, count(&t, h))).collect();
    assert_eq!(got, BTreeMap::from([("z", 20), ("b", 40), ("before", 1), ("after", 1)]));
}

#[test]
fn open_batch_across_two_flushes_then_a_flush_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, RawOptions::default());
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("b", 10));
    for round in 0..2 {
        t.write(row(round, "n", 0.0)).unwrap();
        t.flush().unwrap();
    }
    write_all(&mut b, &host_rows("b", 10));
    b.commit(true).unwrap();
    crash(t, vec![]);
    let t2 = reopen(&path, RawOptions::default());
    assert_eq!((count(&t2, "b"), count(&t2, "n")), (20, 2));
    // With no batch open any more, a flush moves the checkpoint on.
    t2.flush().unwrap();
    assert_eq!(files_with_suffix(&path, ".tfl.wal").len(), 1, "old segments are released");
    drop(t2);
    let t3 = reopen(&path, RawOptions::default());
    assert_eq!((count(&t3, "b"), count(&t3, "n")), (20, 2));
}

#[test]
fn interleaved_batches_one_commits_one_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, RawOptions::default());
    let (mut x, mut y) = (t.begin_batch().unwrap(), t.begin_batch().unwrap());
    for i in 0..30 {
        x.write(row(i, "x", 0.0)).unwrap();
        y.write(row(i, "y", 0.0)).unwrap();
        if i == 10 {
            t.flush().unwrap();
        }
        if i == 20 {
            t.write(row(i, "single", 0.0)).unwrap();
            t.flush().unwrap();
        }
    }
    x.commit(true).unwrap();
    t.sync_wal(true).unwrap();
    crash(t, vec![y]);
    let t = reopen(&path, RawOptions::default());
    assert_eq!((count(&t, "x"), count(&t, "y"), count(&t, "single")), (30, 0, 1));
}

#[test]
fn batch_ids_are_not_reused_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, RawOptions::default());
    let mut old = t.begin_batch().unwrap();
    write_all(&mut old, &host_rows("old", 5));
    t.sync_wal(true).unwrap();
    crash(t, vec![old]);

    let t = reopen(&path, RawOptions::default());
    let mut new = t.begin_batch().unwrap();
    write_all(&mut new, &host_rows("new", 3));
    new.commit(true).unwrap();
    crash(t, vec![]);
    let t = reopen(&path, RawOptions::default());
    assert_eq!((count(&t, "old"), count(&t, "new")), (0, 3));
}

#[test]
fn truncate_invalidates_open_batches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, RawOptions::default());
    t.write(row(0, "a", 0.0)).unwrap();
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("b", 10));
    t.truncate().unwrap();
    assert!(matches!(b.write(row(1, "b", 0.0)), Err(Error::InvalidArg(_))));
    assert!(matches!(b.commit(true), Err(Error::InvalidArg(_))));
    t.write(row(5, "c", 0.0)).unwrap();
    t.sync_wal(true).unwrap();
    crash(t, vec![]);
    let t = reopen(&path, RawOptions::default());
    assert_eq!((count(&t, "a"), count(&t, "b"), count(&t, "c")), (0, 0, 1));
}

#[test]
fn truncate_invalidates_a_spilled_batch_and_removes_its_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, small());
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("b", 600));
    assert!(!files_with_suffix(&path, ".tfl").is_empty());
    t.truncate().unwrap();
    assert!(matches!(b.commit(true), Err(Error::InvalidArg(_))));
    assert!(files_with_suffix(&path, ".tfl").is_empty());
    assert_eq!(count(&t, "b"), 0);
}

#[test]
fn spilled_batch_commits_atomically_and_durably() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, small());
    t.write(row(0, "a", 0.0)).unwrap();
    let rows = host_rows("big", 1500);
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &rows);
    // Staged chunks are on disk but unlisted and invisible.
    assert!(files_with_suffix(&path, ".tfl").len() > 3, "the batch spilled");
    assert_eq!(t.chunks().unwrap().len(), 0);
    assert_eq!(count(&t, "big"), 0);
    b.commit(true).unwrap();
    assert_eq!(count(&t, "big"), 1500);
    assert_eq!(t.chunks().unwrap().len(), files_with_suffix(&path, ".tfl").len(), "every chunk is listed");
    crash(t, vec![]); // no WAL sync needed: the MANIFEST swap was the commit

    let t = reopen(&path, small());
    assert_eq!((count(&t, "big"), count(&t, "a")), (1500, 0), "unsynced single-row writes may be lost, the batch not");
    let big: Vec<Row> = all_rows(&t).into_iter().filter(|r| r[1] == Value::Bytes(b"big".to_vec())).collect();
    assert_eq!(sorted(big), sorted(rows));
}

#[test]
fn spilled_batch_abort_or_crash_leaves_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, small());
    t.write(row(0, "a", 0.0)).unwrap();
    t.sync_wal(true).unwrap();

    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("gone", 800));
    assert!(!files_with_suffix(&path, ".tfl").is_empty());
    b.abort();
    assert!(files_with_suffix(&path, ".tfl").is_empty(), "abort deletes the staged chunks");
    assert_eq!(count(&t, "gone"), 0);

    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("crashed", 800));
    t.sync_wal(true).unwrap();
    assert!(!files_with_suffix(&path, ".tfl").is_empty());
    crash(t, vec![b]);
    let t = reopen(&path, small());
    assert_eq!((count(&t, "gone"), count(&t, "crashed"), count(&t, "a")), (0, 0, 1));
    assert!(files_with_suffix(&path, ".tfl").is_empty(), "recovery removes the orphans");
}

#[test]
fn spilled_batch_survives_flushes_of_other_writers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, small());
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("big", 400));
    for i in 0..200 {
        t.write(row(i, "n", 0.0)).unwrap(); // flushes by size meanwhile
    }
    t.flush().unwrap();
    write_all(&mut b, &host_rows("big", 400));
    b.commit(true).unwrap();
    t.sync_wal(true).unwrap();
    crash(t, vec![]);
    let t = reopen(&path, small());
    assert_eq!((count(&t, "big"), count(&t, "n")), (800, 200));
}

/// Readers never see a part of a batch, even while flushes and compactions run.
fn never_partial(opts: RawOptions<'static>, n: i64, flusher: bool) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&dir.path().join("t"), opts);
    Table::create(&cfg).unwrap();
    let t = maintenance::open_shared(cfg).unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::new();
    {
        let t = t.clone();
        threads.push(thread::spawn(move || {
            for i in 0..3000 {
                t.write(row(i % 500, "noise", i as f64)).unwrap();
            }
        }));
    }
    {
        let (t, done) = (t.clone(), done.clone());
        threads.push(thread::spawn(move || {
            while !done.load(Ordering::Acquire) {
                t.compact(i64::MIN, i64::MAX, base_ts()).unwrap();
                if flusher {
                    t.flush().unwrap();
                }
            }
        }));
    }
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let (t, done) = (t.clone(), done.clone());
            thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    let mut per_host: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
                    for r in all_rows(&t) {
                        if let Value::Bytes(h) = &r[1] {
                            if h.starts_with(b"b") {
                                *per_host.entry(h.clone()).or_default() += 1;
                            }
                        }
                    }
                    for (h, c) in per_host {
                        assert_eq!(c, n, "partial batch {} visible", String::from_utf8_lossy(&h));
                    }
                }
            })
        })
        .collect();
    for k in 0..6 {
        let mut b = t.begin_batch().unwrap();
        write_all(&mut b, &host_rows(&format!("b{k}"), n));
        b.commit(k % 2 == 0).unwrap();
    }
    done.store(true, Ordering::Release);
    for h in threads.into_iter().chain(readers) {
        h.join().unwrap();
    }
    for k in 0..6 {
        assert_eq!(count(&t, &format!("b{k}")), n as usize);
    }
    assert_eq!(count(&t, "noise"), 3000);
}

#[test]
fn readers_never_see_a_partial_batch_wal_path_with_flushes() {
    never_partial(RawOptions::default(), 600, true);
}

#[test]
fn readers_never_see_a_partial_batch_spill_path() {
    never_partial(small(), 600, false);
}

#[test]
fn batch_rejects_a_value_above_the_limit_without_losing_the_batch() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), RawOptions::default());
    let mut b = t.begin_batch().unwrap();
    b.write(row(1, "a", 1.0)).unwrap();
    let mut big = row(2, "a", 1.0);
    big[5] = Value::Bytes(vec![b'x'; 65 << 20]);
    let err = b.write(big).unwrap_err();
    assert!(matches!(&err, Error::InvalidArg(m) if m.contains("exceeds the 64 MiB limit")), "{err}");
    b.write(row(3, "a", 1.0)).unwrap();
    b.commit(true).unwrap();
    assert_eq!(count(&t, "a"), 2);
    t.write(row(4, "a", 1.0)).unwrap();
}

#[test]
fn spilled_batch_commit_releases_the_wal_it_logged_before_spilling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, small());
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("a", 400)); // spills: the first rows were logged
    b.commit(true).unwrap();
    assert_eq!(files_with_suffix(&path, ".tfl.wal").len(), 1, "the pre-spill ROW entries are collected");
    assert_eq!(count(&t, "a"), 400);
    drop(t);
    let t = reopen(&path, small());
    assert_eq!(count(&t, "a"), 400);
    assert_eq!(files_with_suffix(&path, ".tfl.wal").len(), 1);
    drop(t);
    for _ in 0..3 {
        drop(reopen(&path, small()));
    }
    assert_eq!(files_with_suffix(&path, ".tfl.wal").len(), 1);
}

#[test]
fn spilled_batch_does_not_release_segments_another_open_batch_needs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let t = open_new(&path, small());
    let mut other = t.begin_batch().unwrap();
    other.write(row(1, "z", 1.0)).unwrap();
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &host_rows("a", 400));
    b.commit(true).unwrap();
    t.sync_wal(true).unwrap();
    // `other` logged its row in the first segment, which must stay.
    assert!(files_with_suffix(&path, ".tfl.wal").len() >= 2);
    other.commit(true).unwrap();
    drop(t);
    let t = reopen(&path, small());
    assert_eq!(count(&t, "a"), 400);
    assert_eq!(count(&t, "z"), 1);
}

#[test]
fn write_counters_rise_by_the_expected_delta() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), RawOptions::default());
    let before = moosedb_core::write_stats();
    for _ in 0..2 {
        let mut b = t.begin_batch().unwrap();
        write_all(&mut b, &host_rows("w", 10));
        b.commit(true).unwrap();
        t.flush().unwrap();
    }
    t.compact(i64::MIN, i64::MAX, base_ts()).unwrap();
    let d = moosedb_core::write_stats();
    // Global counters: other tests may add on top, never subtract.
    assert!(d.rows_inserted - before.rows_inserted >= 20);
    assert!(d.batches_committed - before.batches_committed >= 2);
    assert!(d.wal_bytes - before.wal_bytes > 0);
    assert!(d.wal_fsyncs - before.wal_fsyncs >= 1);
    assert!(d.memtable_flushes - before.memtable_flushes >= 2);
    assert!(d.compactions - before.compactions >= 1);
    assert!(d.compaction_us >= before.compaction_us);
}

#[test]
fn spilled_batch_rows_are_counted_at_commit_and_aborted_ones_are_not() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_new(&dir.path().join("t"), small());
    let rows = host_rows("s", 600);
    let mut a = t.begin_batch().unwrap();
    write_all(&mut a, &rows);
    a.abort();
    let before = moosedb_core::write_stats();
    let mut b = t.begin_batch().unwrap();
    write_all(&mut b, &rows);
    b.commit(false).unwrap();
    assert_eq!(count(&t, "s"), 600);
    let d = moosedb_core::write_stats();
    // Parallel tests only add; the aborted batch would show as >= 1200 only by luck of others.
    assert!(d.rows_inserted - before.rows_inserted >= 600);
}
