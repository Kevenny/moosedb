//! Process-wide write-path counters (relaxed atomics; monotonic since load).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub(crate) static ROWS: AtomicU64 = AtomicU64::new(0);
pub(crate) static BATCHES: AtomicU64 = AtomicU64::new(0);
pub(crate) static WAL_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static WAL_FSYNCS: AtomicU64 = AtomicU64::new(0);
pub(crate) static FLUSHES: AtomicU64 = AtomicU64::new(0);
pub(crate) static COMPACTIONS: AtomicU64 = AtomicU64::new(0);
pub(crate) static COMPACTION_US: AtomicU64 = AtomicU64::new(0);

pub(crate) fn add(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Relaxed);
}

/// Snapshot of the write counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct WriteStats {
    pub rows_inserted: u64,
    pub batches_committed: u64,
    pub wal_bytes: u64,
    pub wal_fsyncs: u64,
    pub memtable_flushes: u64,
    pub compactions: u64,
    pub compaction_us: u64,
}

pub fn write_stats() -> WriteStats {
    WriteStats {
        rows_inserted: ROWS.load(Relaxed),
        batches_committed: BATCHES.load(Relaxed),
        wal_bytes: WAL_BYTES.load(Relaxed),
        wal_fsyncs: WAL_FSYNCS.load(Relaxed),
        memtable_flushes: FLUSHES.load(Relaxed),
        compactions: COMPACTIONS.load(Relaxed),
        compaction_us: COMPACTION_US.load(Relaxed),
    }
}
