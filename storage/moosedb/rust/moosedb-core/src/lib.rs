//! MooseDB storage core.
//!
//! Everything that touches bytes on disk lives here, in safe Rust. The
//! `moosedb-ffi` crate is a thin C ABI wrapper around this crate.
//!
//! Write path: `Table::write` → WAL append → MemTable; when the MemTable
//! exceeds its threshold it is sealed into one chunk per time bucket and the
//! MANIFEST is switched to include them (see `manifest` for the protocol).
//! Background maintenance (`maintenance`) applies retention and compacts
//! buckets; reads work on immutable snapshots (`scan`).

#![forbid(unsafe_code)]

pub mod agg;
pub mod batch;
mod block;
mod bytes;
mod cache;
pub mod chunk;
mod chunk_reader;
mod chunk_writer;
mod codec;
pub mod compaction;
pub mod compression;
pub mod crypto;
pub mod delete;
pub mod error;
#[cfg(feature = "fuzz")]
#[doc(hidden)]
pub mod fuzz_api;
mod fasthash;
mod format;
mod fsutil;
#[cfg(test)]
mod flush_tests;
pub mod index;
pub mod inspect;
mod log;
pub mod maintenance;
mod manifest;
mod memtable;
#[cfg(test)]
mod memprobe;
pub mod options;
mod rowbatch;
pub mod scan;
pub mod schema;
pub mod settings;
pub mod table;
pub mod time;
mod tomb;
pub mod unique;
mod wal;
pub mod zone;

pub use batch::{Batch, Durability};
pub use delete::DeleteBatch;
pub use compaction::CompactionReport;
pub use error::{Error, Result};
pub use index::series::SeriesSnapshot;
pub use options::{RawOptions, TableConfig, TableOptions};
pub use rowbatch::{BatchColumn, ScanBatch, DEFAULT_BATCH_ROWS, MAX_BATCH_ROWS};
pub use scan::{Position, Scan, ScanFilter, Snapshot, POSITION_LEN};
pub use schema::{Column, ColumnType, Row, Schema, Value};
pub use table::{Table, TableStats};
pub use unique::UniqueSpec;
pub use zone::{prune_stats, reset_prune_stats, ColPred, PruneStats};

/// Bytes held right now by the process-wide decoded block/series cache
/// (bounded by `settings::chunk_cache_bytes`).
pub fn chunk_cache_used_bytes() -> usize {
    cache::used_bytes()
}
