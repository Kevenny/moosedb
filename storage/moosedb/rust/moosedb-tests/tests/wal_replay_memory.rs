//! M5: WAL replay streams the log and buffers only what will be applied, within
//! fixed limits. A forged log must not make `Table::open` allocate in
//! proportion to its size. Own test binary: it reads the process peak RSS.

#![forbid(unsafe_code)]

use std::fs;
use std::io::Write;
use std::path::Path;

use moosedb_core::{Column, ColumnType, Error, RawOptions, Schema, Table, TableConfig, Value};

const TARGET_BYTES: usize = 300 << 20;
/// Enough committed rows to pass the replay MemTable ceiling (512 MiB of row memory).
const COMMITTED_TARGET_BYTES: usize = 480 << 20;
const COMMIT_ENTRY_LEN: usize = 8 + 9; // crc + len + (kind + batch id)
const HEADER_LEN: usize = 8;

fn schema() -> Schema {
    Schema::new(
        vec![
            Column { name: "ts".into(), ty: ColumnType::Timestamp },
            Column { name: "host".into(), ty: ColumnType::Tag },
            Column { name: "note".into(), ty: ColumnType::Varchar },
        ],
        0,
    )
    .unwrap()
}

fn config(dir: &Path) -> TableConfig {
    TableConfig::new(dir, schema(), &RawOptions::default()).unwrap()
}

/// A real WAL segment: 300 rows of one batch followed by its COMMIT.
fn real_segment(dir: &Path) -> Vec<u8> {
    let cfg = config(dir);
    Table::create(&cfg).unwrap();
    let t = std::sync::Arc::new(Table::open(cfg).unwrap());
    let mut b = t.begin_batch().unwrap();
    for i in 0..300i64 {
        b.write(vec![
            Value::Timestamp(1_800_000_000_000_000 + i),
            Value::Bytes(b"h".to_vec()),
            Value::Bytes(vec![b'x'; 100]),
        ])
        .unwrap();
    }
    b.commit(true).unwrap();
    let mut bytes = fs::read(dir.join("wal_000001.tfl.wal")).unwrap();
    // The segment ends in preallocated zeros: walk the entries (crc, len, payload) to find the end.
    let mut end = HEADER_LEN;
    while u32::from_le_bytes(bytes[end..end + 4].try_into().unwrap()) != 0 {
        end += 8 + u32::from_le_bytes(bytes[end + 4..end + 8].try_into().unwrap()) as usize;
    }
    bytes.truncate(end);
    // The last entry must be the COMMIT: crc, len = 9, kind 2, batch id.
    assert_eq!(bytes[bytes.len() - COMMIT_ENTRY_LEN + 4], 9);
    bytes
}

/// A table directory whose only WAL segment is `header + rows * k [+ commit]`,
/// at least `TARGET_BYTES` long, written in small pieces.
fn forged_table(dir: &Path, real: &[u8], with_commit: bool, target: usize) -> TableConfig {
    let cfg = config(dir);
    Table::create(&cfg).unwrap();
    let rows = &real[HEADER_LEN..real.len() - COMMIT_ENTRY_LEN];
    let mut f = std::io::BufWriter::new(fs::File::create(dir.join("wal_000001.tfl.wal")).unwrap());
    f.write_all(&real[..HEADER_LEN]).unwrap();
    let mut written = 0;
    while written < target {
        f.write_all(rows).unwrap();
        written += rows.len();
    }
    if with_commit {
        f.write_all(&real[real.len() - COMMIT_ENTRY_LEN..]).unwrap();
    }
    f.flush().unwrap();
    cfg
}

/// Peak resident set (KiB) since the last reset; Linux only.
fn peak_rss_kib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find(|l| l.starts_with("VmHWM:"))?.split_whitespace().nth(1)?.parse().ok()
}

fn reset_peak_rss() {
    // Writing 5 resets the peak RSS (VmHWM) of the process.
    let _ = fs::write("/proc/self/clear_refs", "5");
}

#[test]
fn forged_wal_does_not_make_open_allocate_in_proportion_to_its_size() {
    let dir = tempfile::tempdir().unwrap();
    let real = real_segment(&dir.path().join("real"));

    // A: 300 MiB of rows of a batch that never commits. Dead weight: it is
    // read twice as a stream and none of it is kept.
    let cfg = forged_table(&dir.path().join("uncommitted"), &real, false, TARGET_BYTES);
    let size = fs::metadata(cfg.dir.join("wal_000001.tfl.wal")).unwrap().len();
    reset_peak_rss();
    let before = peak_rss_kib();
    let t = Table::open(cfg).unwrap();
    let after = peak_rss_kib();
    assert_eq!(t.stats().unwrap().row_count, 0);
    if let (Some(b), Some(a)) = (before, after) {
        let grown = a.saturating_sub(b) << 10;
        eprintln!("REPLAY_RSS uncommitted file={size} grown={grown}");
        assert!(grown < 64 << 20, "open grew the peak RSS by {grown} bytes for a {size} byte log");
    }
    drop(t);
    fs::remove_dir_all(dir.path().join("uncommitted")).unwrap();

    // B: the same rows, now followed by a COMMIT: they are committed data, so
    // they are applied to the MemTable, whose replay ceiling (512 MiB, a bound
    // the writer itself respects) stops it with a clean error. The rows
    // waiting for the COMMIT cost 16 bytes each, not their payload.
    let cfg = forged_table(&dir.path().join("committed"), &real, true, COMMITTED_TARGET_BYTES);
    reset_peak_rss();
    let before = peak_rss_kib();
    let err = Table::open(cfg).err().expect("must refuse to load the forged log");
    let after = peak_rss_kib();
    assert!(matches!(&err, Error::Corrupt(m) if m.contains("refusing to load")), "{err}");
    if let (Some(b), Some(a)) = (before, after) {
        let grown = a.saturating_sub(b) << 10;
        eprintln!("REPLAY_RSS committed file={size} grown={grown}");
        assert!(grown < 900 << 20, "open grew the peak RSS by {grown} bytes");
    }
}
