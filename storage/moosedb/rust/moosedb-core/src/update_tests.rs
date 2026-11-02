//! `Batch::update`: UPDATE pairs applied at the statement's single commit point
//! (COMMIT_UPDATE in the WAL, see `wal`): all or nothing, visible only at commit.

use std::path::Path;
use std::sync::Arc;

use crate::error::Error;
use crate::fsutil::{fail_fsync_after, restore_fsync};
use crate::options::{RawOptions, TableConfig};
use crate::scan::ScanFilter;
use crate::schema::{Column, ColumnType, Row, Schema, Value};
use crate::table::Table;
use crate::wal::{self, TornTail};

const BASE: i64 = 1_700_000_000_000_000;

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

fn config(dir: &Path, unique: bool, raw: &RawOptions<'_>) -> TableConfig {
    let cfg = TableConfig::new(dir, schema(), raw).unwrap();
    if unique {
        cfg.with_unique(vec![3]).unwrap()
    } else {
        cfg
    }
}

fn create(dir: &Path, unique: bool) -> (Arc<Table>, TableConfig) {
    let cfg = config(dir, unique, &RawOptions::default());
    Table::create(&cfg).unwrap();
    (Arc::new(Table::open(cfg.clone()).unwrap()), cfg)
}

fn mk(ts: i64, host: &str, v: i64, n: i64) -> Row {
    vec![Value::Timestamp(BASE + ts), Value::Bytes(host.as_bytes().to_vec()), Value::Float64(v as f64), Value::Int(n)]
}

type K = (i64, String, i64, i64);

/// Every visible row as `(ts, host, v, n)`, sorted.
fn all(t: &Table) -> Vec<K> {
    let mut s = t.scan(&ScanFilter::default(), false).unwrap();
    let mut out = Vec::new();
    while let Some((_, r)) = s.next_row().unwrap() {
        let (Value::Timestamp(ts), Value::Bytes(h), Value::Float64(v), Value::Int(n)) = (&r[0], &r[1], &r[2], &r[3])
        else {
            panic!("unexpected row {r:?}")
        };
        out.push((ts - BASE, String::from_utf8(h.clone()).unwrap(), *v as i64, *n));
    }
    out.sort();
    out
}

fn k(ts: i64, host: &str, v: i64, n: i64) -> K {
    (ts, host.to_string(), v, n)
}

fn update_all(t: &Arc<Table>, pairs: Vec<(Row, Row)>) {
    let mut b = t.begin_batch().unwrap();
    for (o, n) in pairs {
        b.update(o, n).unwrap();
    }
    b.commit(true).unwrap();
}

/// Rows 0..5 in a chunk, 5..10 in the MemTable.
fn fill(t: &Table) {
    for i in 0..5 {
        t.write(mk(i, "a", i, i)).unwrap();
    }
    t.flush().unwrap();
    for i in 5..10 {
        t.write(mk(i, "a", i, i)).unwrap();
    }
}

#[test]
fn update_of_a_value_in_a_chunk_and_in_the_memtable_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (t, cfg) = create(&dir.path().join("t"), false);
    fill(&t);
    update_all(&t, vec![(mk(2, "a", 2, 2), mk(2, "a", 99, 2)), (mk(7, "a", 7, 7), mk(7, "a", 77, 7))]);
    let want: Vec<K> = (0..10)
        .map(|i| match i {
            2 => k(2, "a", 99, 2),
            7 => k(7, "a", 77, 7),
            _ => k(i, "a", i, i),
        })
        .collect();
    assert_eq!(all(&t), want);
    assert_eq!(t.stats().unwrap().row_count, 10);
    drop(t); // WAL replay: COMMIT_UPDATE with a chunk tombstone and a MemTable delete
    let t = Table::open(cfg.clone()).unwrap();
    assert_eq!(all(&t), want);
    t.flush().unwrap();
    assert_eq!(all(&t), want);
    drop(t);
    assert_eq!(all(&Table::open(cfg).unwrap()), want);
}

#[test]
fn tag_and_time_may_change_on_a_table_without_a_key() {
    let dir = tempfile::tempdir().unwrap();
    let (t, _) = create(&dir.path().join("t"), false);
    fill(&t);
    update_all(&t, vec![(mk(1, "a", 1, 1), mk(1, "b", 1, 1)), (mk(8, "a", 8, 8), mk(500, "a", 8, 8))]);
    let mut want: Vec<K> = (0..10).filter(|&i| i != 1 && i != 8).map(|i| k(i, "a", i, i)).collect();
    want.push(k(1, "b", 1, 1));
    want.push(k(500, "a", 8, 8));
    want.sort();
    assert_eq!(all(&t), want);
}

#[test]
fn nothing_is_visible_before_commit_and_inserts_share_the_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (t, _) = create(&dir.path().join("t"), false);
    fill(&t);
    let before = all(&t);
    let mut b = t.begin_batch().unwrap();
    b.write(mk(20, "a", 20, 20)).unwrap();
    b.update(mk(3, "a", 3, 3), mk(3, "a", 33, 3)).unwrap();
    b.update(mk(9, "a", 9, 9), mk(9, "a", 99, 9)).unwrap();
    b.write(mk(21, "a", 21, 21)).unwrap();
    assert_eq!(all(&t), before);
    b.commit(true).unwrap();
    let mut want = before;
    want.retain(|r| r.0 != 3 && r.0 != 9);
    want.extend([k(3, "a", 33, 3), k(9, "a", 99, 9), k(20, "a", 20, 20), k(21, "a", 21, 21)]);
    want.sort();
    assert_eq!(all(&t), want);
    // An aborted batch with pairs changes nothing.
    let mut b = t.begin_batch().unwrap();
    b.update(mk(0, "a", 0, 0), mk(0, "a", 1000, 0)).unwrap();
    b.abort();
    assert_eq!(all(&t), want);
}

#[test]
fn equal_rows_are_updated_one_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let (t, _) = create(&dir.path().join("t"), false);
    t.write(mk(1, "a", 5, 5)).unwrap();
    t.flush().unwrap();
    t.write(mk(1, "a", 5, 5)).unwrap(); // an equal row, in the MemTable
    // Three pairs for two rows: the third finds nothing, the statement fails whole.
    let mut b = t.begin_batch().unwrap();
    for v in [6, 7, 8] {
        b.update(mk(1, "a", 5, 5), mk(1, "a", v, 5)).unwrap();
    }
    assert!(matches!(b.commit(true), Err(Error::Conflict(_))));
    assert_eq!(all(&t), vec![k(1, "a", 5, 5), k(1, "a", 5, 5)]);
    update_all(&t, vec![(mk(1, "a", 5, 5), mk(1, "a", 6, 5)), (mk(1, "a", 5, 5), mk(1, "a", 7, 5))]);
    assert_eq!(all(&t), vec![k(1, "a", 6, 5), k(1, "a", 7, 5)]);
    // One update of two equal rows leaves the other alone.
    t.write(mk(2, "a", 1, 1)).unwrap();
    t.write(mk(2, "a", 1, 1)).unwrap();
    update_all(&t, vec![(mk(2, "a", 1, 1), mk(2, "a", 2, 1))]);
    assert_eq!(all(&t), vec![k(1, "a", 6, 5), k(1, "a", 7, 5), k(2, "a", 1, 1), k(2, "a", 2, 1)]);
}

#[test]
fn a_swap_in_one_statement_reads_only_rows_that_existed_before_it() {
    let dir = tempfile::tempdir().unwrap();
    let (t, cfg) = create(&dir.path().join("t"), false);
    t.write(mk(1, "a", 1, 0)).unwrap();
    t.write(mk(2, "a", 2, 0)).unwrap();
    t.flush().unwrap();
    update_all(&t, vec![(mk(1, "a", 1, 0), mk(1, "a", 2, 0)), (mk(2, "a", 2, 0), mk(2, "a", 1, 0))]);
    assert_eq!(all(&t), vec![k(1, "a", 2, 0), k(2, "a", 1, 0)]);
    drop(t);
    assert_eq!(all(&Table::open(cfg).unwrap()), vec![k(1, "a", 2, 0), k(2, "a", 1, 0)]);
}

#[test]
fn a_pair_whose_old_row_was_deleted_meanwhile_fails_the_statement() {
    let dir = tempfile::tempdir().unwrap();
    let (t, cfg) = create(&dir.path().join("t"), false);
    fill(&t);
    let mut b = t.begin_batch().unwrap();
    b.update(mk(2, "a", 2, 2), mk(2, "a", 22, 2)).unwrap(); // chunk row, deleted below
    b.update(mk(8, "a", 8, 8), mk(8, "a", 88, 8)).unwrap(); // MemTable row, deleted below
    b.update(mk(4, "a", 4, 4), mk(4, "a", 44, 4)).unwrap(); // still there
    let mut d = t.begin_delete(false);
    d.add(mk(2, "a", 2, 2)).unwrap();
    d.add(mk(8, "a", 8, 8)).unwrap();
    assert_eq!(d.commit().unwrap(), 2);
    let mut want: Vec<K> = [0, 1, 3, 4, 5, 6, 7, 9].iter().map(|&i| k(i, "a", i, i)).collect();
    want.sort();
    // Nothing applies, not even the pair whose row is still there.
    let e = b.commit(true).unwrap_err();
    assert!(matches!(e, Error::Conflict(_)), "{e:?}");
    assert_eq!(all(&t), want);
    // Only vanished rows: the commit fails too.
    let mut b = t.begin_batch().unwrap();
    b.update(mk(2, "a", 2, 2), mk(2, "a", 22, 2)).unwrap();
    assert!(matches!(b.commit(true), Err(Error::Conflict(_))));
    assert_eq!(all(&t), want);
    drop(t);
    assert_eq!(all(&Table::open(cfg).unwrap()), want);
}

#[test]
fn unique_table_keeps_its_key_and_refuses_a_change_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let (t, cfg) = create(&dir.path().join("t"), true);
    for i in 0..4 {
        t.write_shared(mk(i, "a", i, i)).unwrap();
    }
    t.flush().unwrap();
    t.write_shared(mk(4, "a", 4, 4)).unwrap();
    let mut b = t.begin_batch().unwrap();
    // tag, time and extra column each refused
    assert!(matches!(b.update(mk(1, "a", 1, 1), mk(1, "b", 1, 1)), Err(Error::Unsupported(_))));
    assert!(matches!(b.update(mk(1, "a", 1, 1), mk(2, "a", 1, 1)), Err(Error::Unsupported(_))));
    assert!(matches!(b.update(mk(1, "a", 1, 1), mk(1, "a", 1, 9)), Err(Error::Unsupported(_))));
    // the value may change: chunk row and MemTable row
    b.update(mk(1, "a", 1, 1), mk(1, "a", 10, 1)).unwrap();
    b.update(mk(4, "a", 4, 4), mk(4, "a", 40, 4)).unwrap();
    b.commit(true).unwrap();
    let want = vec![k(0, "a", 0, 0), k(1, "a", 10, 1), k(2, "a", 2, 2), k(3, "a", 3, 3), k(4, "a", 40, 4)];
    assert_eq!(all(&t), want);
    // The key is still taken, and a second update of the new row works.
    assert!(matches!(t.write_shared(mk(1, "a", 0, 1)), Err(Error::Duplicate(_))));
    update_all(&t, vec![(mk(1, "a", 10, 1), mk(1, "a", 11, 1))]);
    assert_eq!(all(&t)[1], k(1, "a", 11, 1));
    drop(t);
    let t = Arc::new(Table::open(cfg).unwrap());
    assert_eq!(all(&t)[1], k(1, "a", 11, 1));
    assert!(matches!(t.write_shared(mk(1, "a", 0, 1)), Err(Error::Duplicate(_))));
    t.write_shared(mk(1, "a", 0, 2)).unwrap(); // a different key is fine
}

#[test]
fn a_statement_buffer_too_small_for_the_pairs_is_refused_whole() {
    let dir = tempfile::tempdir().unwrap();
    let raw = RawOptions { memtable_size_bytes: 1 << 20, ..RawOptions::default() };
    let cfg = config(&dir.path().join("t"), false, &raw);
    Table::create(&cfg).unwrap();
    let t = Arc::new(Table::open(cfg).unwrap());
    for i in 0..10 {
        t.write(mk(i, "a", i, i)).unwrap();
    }
    let mut b = t.begin_batch().unwrap();
    let mut refused = false;
    for i in 0..50_000 {
        match b.update(mk(i % 10, "a", i % 10, i % 10), mk(i % 10, "a", 1000 + i, i % 10)) {
            Ok(()) => {}
            Err(Error::Unsupported(_)) => {
                refused = true;
                break;
            }
            Err(e) => panic!("{e}"),
        }
    }
    assert!(refused);
    drop(b);
    assert_eq!(all(&t), (0..10).map(|i| k(i, "a", i, i)).collect::<Vec<_>>());
}

#[test]
fn a_failed_fsync_of_an_update_applies_nothing_and_a_reopen_shows_all_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (t, cfg) = create(&dir.path().join("t"), false);
    fill(&t);
    t.sync_wal(true).unwrap();
    let before = all(&t);
    let mut b = t.begin_batch().unwrap();
    b.write(mk(30, "a", 30, 30)).unwrap();
    b.update(mk(2, "a", 2, 2), mk(2, "a", 22, 2)).unwrap();
    b.update(mk(7, "a", 7, 7), mk(7, "a", 77, 7)).unwrap();
    fail_fsync_after(0);
    let r = b.commit(true);
    restore_fsync();
    assert!(r.is_err());
    assert_eq!(all(&t), before, "nothing applied in memory");
    assert!(matches!(t.write(mk(40, "a", 40, 40)), Err(Error::ReadOnly(_))));
    assert!(matches!(t.begin_batch(), Err(Error::ReadOnly(_))));
    drop(t);
    let t = Table::open(cfg).unwrap();
    let mut done = before.clone();
    done.retain(|r| r.0 != 2 && r.0 != 7);
    done.extend([k(2, "a", 22, 2), k(7, "a", 77, 7), k(30, "a", 30, 30)]);
    done.sort();
    let now = all(&t);
    assert!(now == before || now == done, "{now:?}");
}

#[test]
fn deferred_commit_with_pairs_waits_for_the_fsync_and_replays() {
    let dir = tempfile::tempdir().unwrap();
    let (t, cfg) = create(&dir.path().join("t"), false);
    fill(&t);
    let mut b = t.begin_batch().unwrap();
    b.update(mk(1, "a", 1, 1), mk(1, "a", 11, 1)).unwrap();
    let token = b.commit_deferred().unwrap().expect("a logged update owes an fsync");
    assert!(all(&t).contains(&k(1, "a", 11, 1)));
    token.wait().unwrap();
    drop(t);
    assert!(all(&Table::open(cfg).unwrap()).contains(&k(1, "a", 11, 1)));
}

/// Payloads of the entries of the table's first WAL segment.
fn wal_payloads(dir: &Path) -> Vec<Vec<u8>> {
    let seg = wal::list_segments(dir).unwrap()[0];
    let mut out = Vec::new();
    wal::replay(dir, seg, TornTail::Ignore, None, |_, p| {
        out.push(p.to_vec());
        Ok(())
    })
    .unwrap();
    out
}

#[test]
fn a_batch_without_pairs_logs_exactly_the_row_and_commit_entries_it_always_did() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let (t, _) = create(&path, false);
    let mut b = t.begin_batch().unwrap();
    b.write(mk(1, "a", 1, 1)).unwrap();
    b.write(mk(2, "b", 2, 2)).unwrap();
    b.commit(true).unwrap();
    // The first batch id of a new table is 1.
    let want = vec![
        wal::row_entry(1, &mk(1, "a", 1, 1)),
        wal::row_entry(1, &mk(2, "b", 2, 2)),
        vec![2u8, 1, 0, 0, 0, 0, 0, 0, 0], // COMMIT, batch id 1 (little endian)
    ];
    assert_eq!(wal_payloads(&path), want);
}

#[test]
fn an_update_logs_one_commit_update_entry_with_the_delete_and_the_new_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t");
    let (t, _) = create(&path, false);
    t.write(mk(1, "a", 1, 1)).unwrap();
    let mut b = t.begin_batch().unwrap();
    b.write(mk(2, "b", 2, 2)).unwrap();
    b.update(mk(1, "a", 1, 1), mk(1, "a", 9, 1)).unwrap();
    b.commit(true).unwrap();
    let p = wal_payloads(&path);
    // ROW_COMMIT of the first write, ROW of the batch, then the single COMMIT_UPDATE.
    assert_eq!(p.len(), 3);
    assert_eq!(p[2][0], 6, "kind COMMIT_UPDATE: unknown to binaries that predate it, which refuse the segment");
    assert!(p.iter().all(|e| e[0] != 2), "no plain COMMIT for a batch with pairs");
}

#[test]
fn updating_the_same_key_twice_in_a_statement_is_refused_on_a_unique_table_only() {
    let dir = tempfile::tempdir().unwrap();
    let (t, _) = create(&dir.path().join("u"), true);
    t.write_shared(mk(1, "a", 1, 1)).unwrap();
    let mut b = t.begin_batch().unwrap();
    b.update(mk(1, "a", 1, 1), mk(1, "a", 2, 1)).unwrap();
    let e = b.update(mk(1, "a", 1, 1), mk(1, "a", 3, 1)).unwrap_err();
    assert!(matches!(e, Error::Unsupported(_)), "{e:?}");
    // The batch is still usable and the first pair applies alone.
    b.commit(true).unwrap();
    assert_eq!(all(&t), vec![k(1, "a", 2, 1)]);
    // Without a UNIQUE key equal old rows are still counted one at a time.
    let (t, _) = create(&dir.path().join("p"), false);
    t.write(mk(1, "a", 1, 1)).unwrap();
    t.write(mk(1, "a", 1, 1)).unwrap();
    update_all(&t, vec![(mk(1, "a", 1, 1), mk(1, "a", 2, 1)), (mk(1, "a", 1, 1), mk(1, "a", 3, 1))]);
    assert_eq!(all(&t), vec![k(1, "a", 2, 1), k(1, "a", 3, 1)]);
}
