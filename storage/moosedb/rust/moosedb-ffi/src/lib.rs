//! C ABI of the MooseDB storage core.
//!
//! This crate only marshals arguments between C and `moosedb-core`; it holds
//! no storage logic. Every entry point:
//! * validates pointers before dereferencing them,
//! * runs the body under `catch_unwind`, so a Rust panic becomes
//!   `TF_ERR_INTERNAL` instead of unwinding into C++ (undefined behaviour),
//! * records a human-readable message retrievable with `moosedb_last_error`.
//!
//! Threading: a `MooseDBTable` may be used from many threads concurrently.
//! Scans, snapshots, series lists and info handles must be used by one thread
//! at a time.
//!
//! Lifetimes: values returned through `TFRow` (including string pointers) are
//! owned by the handle that produced them and stay valid until the next call
//! on that handle.

#![deny(unsafe_op_in_unsafe_fn)]

use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use moosedb_core::agg::{Agg, AggSpec, ColFilter, FloatFilter, FuncSpec, KeyCol, KeySpec, TopN, ValueFilter};
use moosedb_core::delete::RangeDelete;
use moosedb_core::DeleteBatch;
use moosedb_core::compression::Codec;
use moosedb_core::inspect::{ChunkStatus, TableInfo};
use moosedb_core::{
    crypto, maintenance, settings, Batch, BatchColumn, ColPred, Column, ColumnType, Error, Position, RawOptions, Row, Scan,
    ScanFilter, Schema, SeriesSnapshot, Snapshot, Table, TableConfig, Value, POSITION_LEN,
};

/// Size in bytes of a row position (`handler::ref_length`).
pub const TF_POSITION_LEN: usize = 16;
// Spelled as a literal so cbindgen can emit it; kept in sync at compile time.
const _: () = assert!(TF_POSITION_LEN == POSITION_LEN);

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TFStatus {
    TF_OK = 0,
    TF_ERR_IO = 1,
    TF_ERR_CORRUPT = 2,
    TF_ERR_FULL = 3,
    TF_ERR_NOT_FOUND = 4,
    TF_ERR_INVALID_ARG = 5,
    TF_ERR_OOM = 6,
    TF_ERR_READONLY = 7,
    /// A Rust panic was caught at the boundary.
    TF_ERR_INTERNAL = 8,
    /// The operation or feature is not available.
    TF_ERR_UNSUPPORTED = 9,
    /// Decryption failed or the encryption key / key version is not available
    /// (wrong or retired key, damaged ciphertext): distinct from
    /// `TF_ERR_CORRUPT` because the data may be intact.
    TF_ERR_CRYPTO = 10,
    /// A UNIQUE/PRIMARY key already exists (the row was not stored).
    TF_ERR_DUPLICATE = 11,
    /// Another statement holds the series this row needs; nothing changed,
    /// call again (see `moosedb_batch_write`).
    TF_ERR_BUSY = 12,
    /// Waiting for that series would close a cycle of waiting statements.
    TF_ERR_DEADLOCK = 13,
    /// The caller cancelled a long operation (aggregation).
    TF_ERR_CANCELLED = 14,
}

/// Column type codes used in `TFTableConfig::column_types` and `TFValue::kind`.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TFColumnType {
    TF_COL_TIMESTAMP = 0,
    TF_COL_INT64 = 1,
    TF_COL_FLOAT64 = 2,
    TF_COL_FLOAT32 = 3,
    TF_COL_BOOL = 4,
    TF_COL_VARCHAR = 5,
    TF_COL_TAG = 6,
    TF_COL_DECIMAL = 7,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFStr {
    pub ptr: *const c_char,
    pub len: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union TFValueData {
    /// Microseconds since the Unix epoch (TF_COL_TIMESTAMP).
    pub ts_us: i64,
    pub int_val: i64,
    pub float_val: f64,
    pub float32_val: f32,
    /// 0 or 1. A byte rather than `bool` so any value written by C is valid.
    pub bool_val: u8,
    /// TF_COL_VARCHAR, TF_COL_TAG and TF_COL_DECIMAL (canonical string form).
    pub str_val: TFStr,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFValue {
    /// A `TFColumnType` code; must match the column's declared type.
    pub kind: u8,
    pub is_null: bool,
    pub data: TFValueData,
}

#[repr(C)]
pub struct TFRow {
    pub col_count: u32,
    pub values: *mut TFValue,
}

/// One column of a `TFBatch` (see `moosedb_scan_next_batch`). All pointers
/// belong to the scan and follow the lifetime of the batch.
///
/// `row_count` below is `TFBatch::row_count`. If `present == 0` the column was
/// not requested by the projection and every pointer is NULL.
///
/// * `nulls`: NULL when no row of the batch is NULL in this column; otherwise
///   `row_count` bytes, non-zero = NULL. A NULL row holds 0 / an empty string in
///   the value arrays below.
/// * fixed-width columns use `values` (`row_count` elements):
///   `TF_COL_TIMESTAMP` int64 microseconds, `TF_COL_INT64` int64,
///   `TF_COL_FLOAT64` double, `TF_COL_FLOAT32` float, `TF_COL_BOOL` uint8 (0/1).
///   Elements are naturally aligned for their type.
/// * variable-width columns (`TF_COL_VARCHAR`, `TF_COL_TAG`, `TF_COL_DECIMAL`)
///   use `starts` and `lens` (`row_count` uint32 each): the value of row `i`
///   is `data[starts[i] .. starts[i] + lens[i]]`; `data` may be NULL when
///   every length is 0. Bytes are not NUL-terminated. A TAG value is stored
///   once per series run: rows of the same series in one batch share the
///   same `starts[i]`, so a handler may skip re-storing a TAG whose `starts`
///   equals the previous row's.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFColumnBatch {
    /// A `TFColumnType` code.
    pub kind: u8,
    /// 1 if projected (filled), 0 if not requested.
    pub present: u8,
    pub nulls: *const u8,
    pub values: *const c_void,
    pub starts: *const u32,
    pub lens: *const u32,
    pub data: *const u8,
    /// Size in bytes of `data`; `starts[i] + lens[i] <= data_len` for every row.
    pub data_len: u64,
}

/// Up to N rows of a scan in columnar form (`moosedb_scan_next_batch`).
/// Owned by the scan; valid until the next call on it.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFBatch {
    /// Rows in this batch; 0 means the end of the scan.
    pub row_count: u32,
    /// Number of table columns: `columns[c]` describes schema column `c`.
    pub col_count: u32,
    pub columns: *const TFColumnBatch,
    /// `row_count * TF_POSITION_LEN` bytes: the position of each row.
    pub positions: *const u8,
}

/// Table configuration. String options may be NULL to select defaults.
#[repr(C)]
pub struct TFTableConfig {
    /// Overrides the table directory (DATA DIRECTORY); NULL = use `name`.
    pub data_dir: *const c_char,
    pub chunk_interval: *const c_char,
    pub retention_period: *const c_char,
    pub compression: *const c_char,
    /// 0 = default (3).
    pub compression_level: u8,
    pub hot_threshold: *const c_char,
    /// 0 = default (64 MiB).
    pub memtable_size_bytes: u64,
    pub ts_column_index: u32,
    pub column_count: u32,
    /// May be NULL; otherwise `column_count` NUL-terminated names.
    pub column_names: *const *const c_char,
    /// `column_count` `TFColumnType` codes.
    pub column_types: *const u8,
    /// Key id for encryption at rest; 0 = not encrypted.
    pub encryption_key_id: u32,
    /// The table has a UNIQUE/PRIMARY key `(all TAG columns, time column,
    /// <unique_extra_cols>)`: inserting an existing key fails with
    /// `TF_ERR_DUPLICATE`. Not persisted; supplied at every open.
    pub unique: bool,
    /// Number of key columns after the time column (at most 2).
    pub unique_extra_count: u32,
    /// `unique_extra_count` schema column indexes (INT64 columns); may be NULL
    /// when the count is 0.
    pub unique_extra_cols: *const u32,
}

/// Process-wide tunables (the `moosedb_*` system variables).
#[repr(C)]
pub struct TFGlobalSettings {
    pub retention_check_interval_secs: u64,
    pub compaction_trigger_chunks: u32,
    pub bloom_filter_false_positive_rate: f64,
    /// Bytes of decoded blocks and decoded series kept for reads
    /// (`moosedb_chunk_cache_size`); 0 disables the cache.
    pub chunk_cache_bytes: u64,
    pub max_open_chunks: u32,
    /// Bytes the row buffers of all open statement batches may hold together
    /// (`moosedb_batch_memory_budget`). Above it, the batches that hold at
    /// least 1 MiB spill to disk early. 0 = default (1 GiB).
    pub batch_memory_budget_bytes: u64,
    /// `moosedb_recover_damaged_chunks`: see `Settings::recover_damaged_chunks`.
    pub recover_damaged_chunks: bool,
}

/// Fetches an encryption key from the server. `version == 0` asks for the
/// latest version. Writes the version used to `*out_version` and the 32-byte
/// key to `out_key`. Returns 0 on success.
pub type TFKeyCallback =
    Option<unsafe extern "C" fn(key_id: u32, version: u32, out_version: *mut u32, out_key: *mut u8) -> i32>;

/// Table-level diagnostics (INFORMATION_SCHEMA.MOOSEDB_TABLES). Strings are
/// owned by the `MooseDBInfo` handle.
#[repr(C)]
pub struct TFTableInfo {
    pub is_open: bool,
    pub encrypted: bool,
    pub row_count: u64,
    pub pending_rows: u64,
    pub series_count: u64,
    pub data_bytes: u64,
    pub compressed_bytes: u64,
    pub chunk_count: u32,
    pub hot_chunks: u32,
    pub warm_chunks: u32,
    pub cold_chunks: u32,
    pub compacting_chunks: u32,
    pub expired_chunks: u32,
    pub chunk_interval: *const c_char,
    pub retention_period: *const c_char,
    pub compression: *const c_char,
}

/// One chunk (INFORMATION_SCHEMA.MOOSEDB_CHUNKS).
#[repr(C)]
pub struct TFChunkInfo {
    pub chunk_id: u64,
    pub ts_min_us: i64,
    pub ts_max_us: i64,
    pub rows: u64,
    pub series: u32,
    pub data_bytes: u64,
    pub compressed_bytes: u64,
    pub sealed_at_us: i64,
    pub encrypted: bool,
    /// "HOT", "WARM", "COLD", "COMPACTING" or "EXPIRED".
    pub status: *const c_char,
    /// "NONE", "LZ4" or "ZSTD".
    pub compression: *const c_char,
    pub file_name: *const c_char,
}

/// Opaque table handle.
pub struct MooseDBTable {
    table: Arc<Table>,
}

/// Durability token of a deferred batch commit (see `moosedb_batch_commit_deferred`).
pub struct MooseDBDurability {
    durability: moosedb_core::Durability,
}

/// Opaque statement batch (see `moosedb_batch_begin`). Used by one thread.
pub struct MooseDBBatch {
    batch: Batch,
}

/// Buffers that keep a `TFRow` valid between calls.
struct RowOut {
    current: Row,
    out: Vec<TFValue>,
}

impl RowOut {
    fn new() -> RowOut {
        RowOut { current: Vec::new(), out: Vec::new() }
    }

    fn publish(&mut self, row: Row, types: impl Iterator<Item = ColumnType>, dst: &mut TFRow) {
        self.current = row;
        self.out = self.current.iter().zip(types).map(|(v, t)| value_to_c(v, t)).collect();
        dst.col_count = self.out.len() as u32;
        dst.values = self.out.as_mut_ptr();
    }
}

fn clear_row(dst: &mut TFRow) {
    dst.col_count = 0;
    dst.values = ptr::null_mut();
}

/// Opaque scan handle.
pub struct MooseDBScan {
    scan: Scan,
    types: Vec<ColumnType>,
    position: Option<Position>,
    row: RowOut,
    /// Descriptors the last `TFBatch` points to.
    batch_cols: Vec<TFColumnBatch>,
}

/// Opaque snapshot handle: resolves row positions (`rnd_pos`).
pub struct MooseDBSnapshot {
    snap: Arc<Snapshot>,
    types: Vec<ColumnType>,
    row: RowOut,
}

/// Opaque list of the series of a table: a shared, immutable snapshot of the
/// series index (taking it copies nothing).
pub struct MooseDBSeriesList {
    snap: SeriesSnapshot,
    row: RowOut,
}

/// Opaque diagnostics handle.
pub struct MooseDBInfo {
    table: TFTableInfo,
    chunks: Vec<TFChunkInfo>,
    _strings: Vec<CString>,
}

// ─── error plumbing ──────────────────────────────────────────────────────────

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(msg: &str) {
    let c = CString::new(msg.replace('\0', " ")).unwrap_or_default();
    let _ = LAST_ERROR.try_with(|e| *e.borrow_mut() = Some(c));
}

fn status_of(e: &Error) -> TFStatus {
    match e {
        Error::Io(io) if io.kind() == std::io::ErrorKind::OutOfMemory => TFStatus::TF_ERR_OOM,
        Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound => TFStatus::TF_ERR_NOT_FOUND,
        Error::Io(_) => TFStatus::TF_ERR_IO,
        Error::Corrupt(_) => TFStatus::TF_ERR_CORRUPT,
        Error::Crypto(_) => TFStatus::TF_ERR_CRYPTO,
        Error::Full(_) => TFStatus::TF_ERR_FULL,
        Error::NotFound(_) => TFStatus::TF_ERR_NOT_FOUND,
        Error::InvalidArg(_) => TFStatus::TF_ERR_INVALID_ARG,
        Error::ReadOnly(_) => TFStatus::TF_ERR_READONLY,
        Error::Unsupported(_) => TFStatus::TF_ERR_UNSUPPORTED,
        Error::Duplicate(_) => TFStatus::TF_ERR_DUPLICATE,
        Error::Busy(_) => TFStatus::TF_ERR_BUSY,
        Error::Deadlock(_) => TFStatus::TF_ERR_DEADLOCK,
        Error::Cancelled(_) => TFStatus::TF_ERR_CANCELLED,
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".into())
}

fn guard(f: impl FnOnce() -> Result<(), Error>) -> TFStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => TFStatus::TF_OK,
        Ok(Err(e)) => {
            set_last_error(&e.to_string());
            status_of(&e)
        }
        Err(payload) => {
            set_last_error(&format!("internal error (panic): {}", panic_message(payload.as_ref())));
            TFStatus::TF_ERR_INTERNAL
        }
    }
}

/// `guard` for functions that return a value instead of a status: a panic is
/// recorded and `on_panic` returned, never unwound into C.
fn guard_value<T>(on_panic: T, f: impl FnOnce() -> T) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(payload) => {
            set_last_error(&format!("internal error (panic): {}", panic_message(payload.as_ref())));
            on_panic
        }
    }
}

fn invalid(msg: &str) -> Error {
    Error::InvalidArg(msg.into())
}

// ─── pointer helpers ─────────────────────────────────────────────────────────

/// # Safety
/// `p` must be NULL or point to a NUL-terminated string valid for `'a`.
unsafe fn opt_str<'a>(p: *const c_char, what: &str) -> Result<Option<&'a str>, Error> {
    if p.is_null() {
        return Ok(None);
    }
    // SAFETY: non-null and NUL-terminated per the caller contract.
    let s = unsafe { CStr::from_ptr(p) };
    s.to_str().map(Some).map_err(|_| invalid(&format!("{what} is not valid UTF-8")))
}

/// # Safety
/// Same as [`opt_str`].
unsafe fn req_str<'a>(p: *const c_char, what: &str) -> Result<&'a str, Error> {
    // SAFETY: forwarded caller contract.
    unsafe { opt_str(p, what) }?.ok_or_else(|| invalid(&format!("{what} is NULL")))
}

/// # Safety
/// `p` must be NULL (only if `n == 0`) or point to `n` initialized `T`s valid for `'a`.
unsafe fn slice<'a, T>(p: *const T, n: usize, what: &str) -> Result<&'a [T], Error> {
    if n == 0 {
        return Ok(&[]);
    }
    if p.is_null() {
        return Err(invalid(&format!("{what} is NULL")));
    }
    // SAFETY: non-null, `n` elements per the caller contract.
    Ok(unsafe { std::slice::from_raw_parts(p, n) })
}

/// # Safety
/// `config` must be NULL or point to a valid `TFTableConfig` whose pointers obey
/// the documented contract.
unsafe fn config_from_c(
    name: *const c_char,
    config: *const TFTableConfig,
    existing: bool,
) -> Result<TableConfig, Error> {
    // SAFETY: caller contract.
    let cfg = unsafe { config.as_ref() }.ok_or_else(|| invalid("config is NULL"))?;
    // SAFETY: caller contract for every pointer below.
    unsafe {
        let dir = match opt_str(cfg.data_dir, "data_dir")? {
            Some(d) => d,
            None => req_str(name, "table name")?,
        };
        let n = cfg.column_count as usize;
        let types = slice(cfg.column_types, n, "column_types")?;
        let names: Vec<String> = if cfg.column_names.is_null() {
            (0..n).map(|i| format!("c{i}")).collect()
        } else {
            slice(cfg.column_names, n, "column_names")?
                .iter()
                .map(|&p| req_str(p, "column name").map(str::to_owned))
                .collect::<Result<_, _>>()?
        };
        let columns = names
            .into_iter()
            .zip(types)
            .map(|(name, &t)| {
                let ty = ColumnType::from_u8(t).ok_or_else(|| invalid(&format!("unknown column type {t}")))?;
                Ok(Column { name, ty })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let schema = Schema::new(columns, cfg.ts_column_index as usize)?;
        let raw = RawOptions {
            chunk_interval: opt_str(cfg.chunk_interval, "chunk_interval")?,
            retention_period: opt_str(cfg.retention_period, "retention_period")?,
            compression: opt_str(cfg.compression, "compression")?,
            compression_level: cfg.compression_level,
            hot_threshold: opt_str(cfg.hot_threshold, "hot_threshold")?,
            memtable_size_bytes: cfg.memtable_size_bytes,
            encryption_key_id: (cfg.encryption_key_id != 0).then_some(cfg.encryption_key_id),
        };
        let cfg_t =
            if existing { TableConfig::for_existing(dir, schema, &raw) } else { TableConfig::new(dir, schema, &raw) }?;
        if cfg.unique {
            if cfg.unique_extra_count as usize > moosedb_core::unique::MAX_EXTRAS {
                return Err(invalid(&format!(
                    "unique_extra_count {} exceeds the maximum of {}",
                    cfg.unique_extra_count,
                    moosedb_core::unique::MAX_EXTRAS
                )));
            }
            let extras: Vec<usize> =
                slice(cfg.unique_extra_cols, cfg.unique_extra_count as usize, "unique_extra_cols")?
                    .iter()
                    .map(|&c| c as usize)
                    .collect();
            cfg_t.with_unique(extras)
        } else {
            Ok(cfg_t)
        }
    }
}

/// # Safety
/// `v` must be a valid `TFValue`; string pointers must reference `len` bytes.
unsafe fn value_from_c(v: &TFValue, ty: ColumnType) -> Result<Value, Error> {
    if v.is_null {
        return Ok(Value::Null);
    }
    if v.kind != ty as u8 {
        return Err(invalid(&format!("value kind {} does not match column type {:?}", v.kind, ty)));
    }
    // SAFETY: `kind` tells which union member C initialized.
    unsafe {
        Ok(match ty {
            ColumnType::Timestamp => Value::Timestamp(v.data.ts_us),
            ColumnType::Int64 => Value::Int(v.data.int_val),
            ColumnType::Float64 => Value::Float64(v.data.float_val),
            ColumnType::Float32 => Value::Float32(v.data.float32_val),
            ColumnType::Bool => Value::Bool(v.data.bool_val != 0),
            ColumnType::Varchar | ColumnType::Tag | ColumnType::Decimal => {
                let s = v.data.str_val;
                Value::Bytes(slice(s.ptr.cast::<u8>(), s.len as usize, "string value")?.to_vec())
            }
        })
    }
}

fn value_to_c(v: &Value, ty: ColumnType) -> TFValue {
    let kind = ty as u8;
    let (is_null, data) = match v {
        Value::Null => (true, TFValueData { int_val: 0 }),
        Value::Timestamp(t) => (false, TFValueData { ts_us: *t }),
        Value::Int(i) => (false, TFValueData { int_val: *i }),
        Value::Float64(f) => (false, TFValueData { float_val: *f }),
        Value::Float32(f) => (false, TFValueData { float32_val: *f }),
        Value::Bool(b) => (false, TFValueData { bool_val: u8::from(*b) }),
        Value::Bytes(b) => (false, TFValueData { str_val: TFStr { ptr: b.as_ptr().cast(), len: b.len() as u32 } }),
    };
    TFValue { kind, is_null, data }
}

fn table_ref<'a>(t: *const MooseDBTable) -> Result<&'a MooseDBTable, Error> {
    // SAFETY: handles come from `moosedb_table_open`/`_lookup` and stay valid
    // until `moosedb_table_close`, per the API contract.
    unsafe { t.as_ref() }.ok_or_else(|| invalid("table handle is NULL"))
}

fn handle_mut<'a, T>(p: *mut T, what: &str) -> Result<&'a mut T, Error> {
    // SAFETY: the caller passes a valid handle or out-parameter, or NULL.
    unsafe { p.as_mut() }.ok_or_else(|| invalid(&format!("{what} is NULL")))
}

fn handle_ref<'a, T>(p: *const T, what: &str) -> Result<&'a T, Error> {
    // SAFETY: as `handle_mut`, read-only.
    unsafe { p.as_ref() }.ok_or_else(|| invalid(&format!("{what} is NULL")))
}

fn read_position(pos: *const u8) -> Result<Position, Error> {
    let bytes = handle_ref(pos.cast::<[u8; POSITION_LEN]>(), "pos")?;
    Ok(Position::from_bytes(bytes))
}

/// Takes back ownership of a boxed handle and drops it. NULL is ignored.
fn release<T>(p: *mut T) {
    if !p.is_null() {
        // SAFETY: produced by `Box::into_raw` in this library, released once.
        drop(unsafe { Box::from_raw(p) });
    }
}

// ─── global configuration ────────────────────────────────────────────────────

/// Applies the `moosedb_*` system variables. Safe to call at any time.
///
/// # Safety
/// `s` must point to a valid `TFGlobalSettings`.
#[no_mangle]
pub unsafe extern "C" fn moosedb_set_globals(s: *const TFGlobalSettings) -> TFStatus {
    guard(|| {
        let s = handle_ref(s, "settings")?;
        let g = settings::get();
        g.set_retention_check_interval_secs(s.retention_check_interval_secs);
        g.set_compaction_trigger_chunks(s.compaction_trigger_chunks);
        g.set_bloom_fpr(s.bloom_filter_false_positive_rate);
        g.set_chunk_cache_bytes(s.chunk_cache_bytes);
        g.set_max_open_chunks(s.max_open_chunks);
        g.set_batch_memory_budget_bytes(s.batch_memory_budget_bytes);
        g.set_recover_damaged_chunks(s.recover_damaged_chunks);
        Ok(())
    })
}

/// Installs (or, with NULL, removes) the encryption key provider.
#[no_mangle]
pub extern "C" fn moosedb_set_key_callback(cb: TFKeyCallback) -> TFStatus {
    guard(|| {
        let provider: Option<Arc<crypto::KeyProvider>> = cb.map(|f| {
            let p: Arc<crypto::KeyProvider> = Arc::new(move |key_id: u32, version: Option<u32>| {
                let mut out_version = 0u32;
                let mut key = [0u8; crypto::KEY_LEN];
                // SAFETY: the callback contract: writes one u32 and KEY_LEN bytes.
                let rc = unsafe { f(key_id, version.unwrap_or(0), &mut out_version, key.as_mut_ptr()) };
                if rc == 2 {
                    return Err(Error::InvalidArg(format!("encryption key {key_id} is not a 256-bit key")));
                }
                if rc != 0 {
                    return Err(Error::Crypto(format!(
                        "encryption key {key_id} (version {}) is not available",
                        version.map_or("latest".to_string(), |v| v.to_string())
                    )));
                }
                Ok((out_version, key))
            });
            p
        });
        crypto::set_key_provider(provider);
        Ok(())
    })
}

/// Starts the background maintenance pool (retention, compaction).
#[no_mangle]
pub extern "C" fn moosedb_maintenance_start(threads: u32) -> TFStatus {
    guard(|| {
        maintenance::start(threads as usize);
        Ok(())
    })
}

/// Stops the background maintenance pool, waiting for running jobs.
#[no_mangle]
pub extern "C" fn moosedb_maintenance_stop() -> TFStatus {
    guard(|| {
        maintenance::stop();
        Ok(())
    })
}

// ─── table lifecycle ─────────────────────────────────────────────────────────

/// Creates the on-disk structure of a new table.
///
/// # Safety
/// `name` must be a NUL-terminated path; `config` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_table_create(name: *const c_char, config: *const TFTableConfig) -> TFStatus {
    guard(|| {
        // SAFETY: caller contract.
        let cfg = unsafe { config_from_c(name, config, false) }?;
        Table::create(&cfg)
    })
}

/// Opens a table (running crash recovery) or attaches to the instance already
/// open in this process. Release the handle with `moosedb_table_close`.
///
/// # Safety
/// As `moosedb_table_create`; `out_table` must be writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_table_open(
    name: *const c_char,
    config: *const TFTableConfig,
    out_table: *mut *mut MooseDBTable,
) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_table, "out_table")?;
        *out = ptr::null_mut();
        // SAFETY: caller contract.
        let cfg = unsafe { config_from_c(name, config, true) }?;
        let table = maintenance::open_shared(cfg)?;
        *out = Box::into_raw(Box::new(MooseDBTable { table }));
        Ok(())
    })
}

/// Handle to the table at `path` if it is open in this process
/// (`TF_ERR_NOT_FOUND` otherwise). Release with `moosedb_table_close`.
///
/// # Safety
/// `path` must be NUL-terminated; `out_table` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_table_lookup(path: *const c_char, out_table: *mut *mut MooseDBTable) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_table, "out_table")?;
        *out = ptr::null_mut();
        // SAFETY: caller contract.
        let p = unsafe { req_str(path, "path") }?;
        let table = maintenance::lookup(p.as_ref()).ok_or_else(|| Error::NotFound(format!("{p} is not open")))?;
        *out = Box::into_raw(Box::new(MooseDBTable { table }));
        Ok(())
    })
}

/// Releases a table handle; pending WAL data is flushed to the OS. The table
/// itself closes when its last handle (and background job) is gone.
///
/// # Safety
/// `table` must come from `moosedb_table_open`/`_lookup` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_table_close(table: *mut MooseDBTable) -> TFStatus {
    guard(|| {
        if table.is_null() {
            return Ok(());
        }
        // SAFETY: ownership is transferred back from C, exactly once.
        let t = unsafe { Box::from_raw(table) };
        t.table.sync_wal(true)
    })
}

/// # Safety
/// `name` must be a NUL-terminated path.
#[no_mangle]
pub unsafe extern "C" fn moosedb_table_drop(name: *const c_char) -> TFStatus {
    guard(|| {
        // SAFETY: caller contract.
        let dir = unsafe { req_str(name, "table name") }?;
        Table::drop_table(dir.as_ref())
    })
}

/// # Safety
/// `from` and `to` must be NUL-terminated paths.
#[no_mangle]
pub unsafe extern "C" fn moosedb_table_rename(from: *const c_char, to: *const c_char) -> TFStatus {
    guard(|| {
        // SAFETY: caller contract.
        let (from, to) = unsafe { (req_str(from, "from")?, req_str(to, "to")?) };
        Table::rename(from.as_ref(), to.as_ref())
    })
}

// ─── writes ──────────────────────────────────────────────────────────────────

/// Appends one row (WAL + MemTable). Durable after `moosedb_sync_wal(.., true)`.
///
/// # Safety
/// `row` must point to `col_count` valid values matching the table's columns.
#[no_mangle]
pub unsafe extern "C" fn moosedb_write_row(table: *mut MooseDBTable, row: *const TFRow) -> TFStatus {
    guard(|| {
        let t = table_ref(table)?;
        // SAFETY: caller contract.
        let row = unsafe { row_from_c(t.table.schema(), row) }?;
        t.table.write_shared(row)
    })
}

/// Converts a caller-owned `TFRow` into a core row (marshalling only).
///
/// # Safety
/// `row` must be NULL or point to `col_count` valid values.
unsafe fn row_from_c(schema: &Schema, row: *const TFRow) -> Result<Row, Error> {
    let row = handle_ref(row, "row")?;
    // SAFETY: caller contract.
    let values = unsafe { slice(row.values, row.col_count as usize, "row values") }?;
    let cols = schema.columns();
    if values.len() != cols.len() {
        return Err(invalid(&format!("row has {} values, table has {} columns", values.len(), cols.len())));
    }
    values
        .iter()
        .zip(cols)
        // SAFETY: values come from the caller-validated slice.
        .map(|(v, c)| unsafe { value_from_c(v, c.ty) })
        .collect()
}

/// Starts a statement batch. Rows written with `moosedb_batch_write` are
/// invisible to every reader until `moosedb_batch_commit`, which publishes
/// them all at once; a crash before the commit loses all of them, after a
/// synced commit none. Any number of batches (and `moosedb_write_row` calls)
/// may be open at the same time. Memory use is bounded: large batches spill
/// to disk. The handle must end in `moosedb_batch_commit` or
/// `moosedb_batch_abort`, which release it.
///
/// # Safety
/// `table` must be a valid handle; `out_batch` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_batch_begin(table: *mut MooseDBTable, out_batch: *mut *mut MooseDBBatch) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_batch, "out_batch")?;
        *out = ptr::null_mut();
        let t = table_ref(table)?;
        let batch = t.table.begin_batch()?;
        *out = Box::into_raw(Box::new(MooseDBBatch { batch }));
        Ok(())
    })
}

/// Turns the UNIQUE key checks of this statement on (the default) or off
/// (`unique_checks = 0`). Off, rows are stored without checking and without
/// claiming their series, so a key stored this way can be a duplicate and later
/// checks cannot tell; the engine still records them for what comes after.
/// No effect on a table without a UNIQUE key.
///
/// # Safety
/// `batch` must be a live batch handle.
#[no_mangle]
pub unsafe extern "C" fn moosedb_batch_set_unique_checks(batch: *mut MooseDBBatch, enabled: bool) -> TFStatus {
    guard(|| {
        handle_mut(batch, "batch")?.batch.set_unique_checks(enabled);
        Ok(())
    })
}

/// Adds one row to the batch (same value convention as `moosedb_write_row`).
/// On error the row is not part of the batch; the batch stays usable.
///
/// On a table with a UNIQUE key: `TF_ERR_DUPLICATE` = the key exists (stored,
/// or in an open statement); `TF_ERR_BUSY` = another statement holds the series
/// (after waiting a short while): nothing changed, call again with the same
/// row; `TF_ERR_DEADLOCK` = waiting would close a cycle between statements.
///
/// # Safety
/// `batch` must be a live batch handle; `row` as in `moosedb_write_row`.
#[no_mangle]
pub unsafe extern "C" fn moosedb_batch_write(batch: *mut MooseDBBatch, row: *const TFRow) -> TFStatus {
    guard(|| {
        let b = handle_mut(batch, "batch")?;
        // SAFETY: caller contract.
        let row = unsafe { row_from_c(b.batch.schema(), row) }?;
        b.batch.write(row)
    })
}

/// Publishes every row of the batch at once and releases the handle (also on
/// error: the handle is invalid afterwards). With `sync` the commit is durable
/// when this returns.
///
/// # Safety
/// `batch` must come from `moosedb_batch_begin` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_batch_commit(batch: *mut MooseDBBatch, sync: bool) -> TFStatus {
    guard(|| {
        if batch.is_null() {
            return Err(invalid("batch is NULL"));
        }
        // SAFETY: ownership is transferred back from C, exactly once.
        let b = unsafe { Box::from_raw(batch) };
        b.batch.commit(sync)
    })
}

/// Like `moosedb_batch_commit(batch, true)`, but returns before the fsync. The
/// rows are visible and handed to the OS; `*out_token` receives a durability
/// token (NULL when there is nothing to wait for: empty batch or spilled
/// batch, already durable) that must end in `moosedb_durability_wait` or
/// `moosedb_durability_free`. The batch handle is released, also on error. The
/// token keeps the table alive, so it stays valid after `moosedb_table_close`.
///
/// # Safety
/// `batch` must come from `moosedb_batch_begin` and not be used afterwards;
/// `out_token` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_batch_commit_deferred(
    batch: *mut MooseDBBatch,
    out_token: *mut *mut MooseDBDurability,
) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_token, "out_token")?;
        *out = ptr::null_mut();
        if batch.is_null() {
            return Err(invalid("batch is NULL"));
        }
        // SAFETY: ownership is transferred back from C, exactly once.
        let b = unsafe { Box::from_raw(batch) };
        if let Some(d) = b.batch.commit_deferred()? {
            *out = Box::into_raw(Box::new(MooseDBDurability { durability: d }));
        }
        Ok(())
    })
}

/// Blocks until the commit behind `token` is on stable storage (group commit)
/// and releases the token, also on error. NULL is ignored.
///
/// # Safety
/// `token` must come from `moosedb_batch_commit_deferred` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_durability_wait(token: *mut MooseDBDurability) -> TFStatus {
    guard(|| {
        if token.is_null() {
            return Ok(());
        }
        // SAFETY: ownership is transferred back from C, exactly once.
        let t = unsafe { Box::from_raw(token) };
        t.durability.wait()
    })
}

/// Releases the token without waiting. NULL is ignored.
///
/// # Safety
/// `token` must come from `moosedb_batch_commit_deferred` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_durability_free(token: *mut MooseDBDurability) -> TFStatus {
    guard(|| {
        if !token.is_null() {
            // SAFETY: ownership is transferred back from C, exactly once.
            drop(unsafe { Box::from_raw(token) });
        }
        Ok(())
    })
}

/// Discards the batch (none of its rows ever becomes visible) and releases the
/// handle. NULL is ignored.
///
/// # Safety
/// `batch` must come from `moosedb_batch_begin` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_batch_abort(batch: *mut MooseDBBatch) -> TFStatus {
    guard(|| {
        if !batch.is_null() {
            // SAFETY: ownership is transferred back from C, exactly once.
            drop(unsafe { Box::from_raw(batch) });
        }
        Ok(())
    })
}

/// Statement-end hook: flushes buffered WAL entries to the OS and, when
/// `durable`, fsyncs them.
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn moosedb_sync_wal(table: *mut MooseDBTable, durable: bool) -> TFStatus {
    guard(|| table_ref(table)?.table.sync_wal(durable))
}

/// Seals the MemTable into chunk files.
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn moosedb_flush(table: *mut MooseDBTable) -> TFStatus {
    guard(|| table_ref(table)?.table.flush())
}

/// Removes all rows (TRUNCATE TABLE / DELETE without WHERE).
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn moosedb_truncate(table: *mut MooseDBTable) -> TFStatus {
    guard(|| table_ref(table)?.table.truncate())
}


// ─── deletes ─────────────────────────────────────────────────────────────────

/// Opaque set of rows a `DELETE` removes one by one (see `moosedb_delete_begin`).
pub struct MooseDBDelete {
    del: Option<DeleteBatch>,
}

/// Deletes every row of the given series (`series_count < 0` = all series) with
/// `ts_start_us <= ts <= ts_end_us`, as one atomic, logged step. With
/// `max_rows >= 0` nothing is deleted when more rows match: `*out_exceeds` is
/// set and `*out_rows` holds the number that match. Otherwise `*out_rows` is
/// the number deleted. `durable` fsyncs the log before returning.
///
/// # Safety
/// `series_ids` must hold `series_count` ids when `series_count > 0`; the out
/// pointers must be writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_delete_range(
    table: *mut MooseDBTable,
    series_ids: *const u64,
    series_count: i64,
    ts_start_us: i64,
    ts_end_us: i64,
    max_rows: i64,
    durable: bool,
    out_rows: *mut u64,
    out_exceeds: *mut bool,
) -> TFStatus {
    guard(|| {
        let rows = handle_mut(out_rows, "out_rows")?;
        let exceeds = handle_mut(out_exceeds, "out_exceeds")?;
        *rows = 0;
        *exceeds = false;
        let t = table_ref(table)?;
        let ids = if series_count >= 0 {
            // SAFETY: caller contract.
            Some(unsafe { slice(series_ids, series_count as usize, "series_ids") }?)
        } else {
            None
        };
        let max = (max_rows >= 0).then_some(max_rows as u64);
        match t.table.delete_range(ids, ts_start_us, ts_end_us, max, durable)? {
            RangeDelete::Deleted(n) => *rows = n,
            RangeDelete::Exceeds(n) => {
                *rows = n;
                *exceeds = true;
            }
        }
        Ok(())
    })
}

/// How many rows `moosedb_delete_range` with the same arguments would delete now.
///
/// # Safety
/// As `moosedb_delete_range`.
#[no_mangle]
pub unsafe extern "C" fn moosedb_count_range(
    table: *mut MooseDBTable,
    series_ids: *const u64,
    series_count: i64,
    ts_start_us: i64,
    ts_end_us: i64,
    out_rows: *mut u64,
) -> TFStatus {
    guard(|| {
        let rows = handle_mut(out_rows, "out_rows")?;
        *rows = 0;
        let t = table_ref(table)?;
        let ids = if series_count >= 0 {
            // SAFETY: caller contract.
            Some(unsafe { slice(series_ids, series_count as usize, "series_ids") }?)
        } else {
            None
        };
        *rows = t.table.count_range(ids, ts_start_us, ts_end_us)?;
        Ok(())
    })
}

/// Starts collecting the rows of one `DELETE` statement. They are matched by
/// content and deleted by `moosedb_delete_commit` (also in pieces if the
/// statement is huge). The handle ends in commit or abort.
///
/// # Safety
/// `table` must be a valid handle; `out_delete` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_delete_begin(
    table: *mut MooseDBTable,
    durable: bool,
    out_delete: *mut *mut MooseDBDelete,
) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_delete, "out_delete")?;
        *out = ptr::null_mut();
        let t = table_ref(table)?;
        *out = Box::into_raw(Box::new(MooseDBDelete { del: Some(t.table.begin_delete(durable)) }));
        Ok(())
    })
}

/// Adds one row (same value convention as `moosedb_write_row`) to delete.
///
/// # Safety
/// `del` must be a live handle; `row` as in `moosedb_write_row`.
#[no_mangle]
pub unsafe extern "C" fn moosedb_delete_add(del: *mut MooseDBDelete, row: *const TFRow) -> TFStatus {
    guard(|| {
        let d = handle_mut(del, "delete")?;
        let b = d.del.as_mut().ok_or_else(|| invalid("the delete is already finished"))?;
        // SAFETY: caller contract.
        let row = unsafe { row_from_c(b.schema(), row) }?;
        b.add(row)
    })
}

/// Deletes the collected rows and releases the handle (also on error).
/// `*out_rows` receives the number of rows deleted.
///
/// # Safety
/// `del` must come from `moosedb_delete_begin` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_delete_commit(del: *mut MooseDBDelete, out_rows: *mut u64) -> TFStatus {
    guard(|| {
        if del.is_null() {
            return Err(invalid("delete is NULL"));
        }
        // SAFETY: ownership is transferred back from C, exactly once.
        let mut d = unsafe { Box::from_raw(del) };
        let b = d.del.take().ok_or_else(|| invalid("the delete is already finished"))?;
        let n = b.commit()?;
        // SAFETY: caller contract (NULL allowed).
        if let Some(p) = unsafe { out_rows.as_mut() } {
            *p = n;
        }
        Ok(())
    })
}

/// Discards the collected rows and releases the handle. NULL is ignored.
///
/// # Safety
/// `del` must come from `moosedb_delete_begin` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_delete_abort(del: *mut MooseDBDelete) -> TFStatus {
    guard(|| {
        if !del.is_null() {
            // SAFETY: ownership is transferred back from C, exactly once.
            drop(unsafe { Box::from_raw(del) });
        }
        Ok(())
    })
}

// ─── scans ───────────────────────────────────────────────────────────────────

fn open_scan(
    table: *mut MooseDBTable,
    filter: &ScanFilter,
    sorted: bool,
    projection: Option<&[bool]>,
    out_scan: *mut *mut MooseDBScan,
) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_scan, "out_scan")?;
        *out = ptr::null_mut();
        let t = table_ref(table)?;
        let scan = t.table.scan_projected(filter, sorted, projection)?;
        let types = t.table.schema().columns().iter().map(|c| c.ty).collect();
        *out = Box::into_raw(Box::new(MooseDBScan {
            scan,
            types,
            position: None,
            row: RowOut::new(),
            batch_cols: Vec::new(),
        }));
        Ok(())
    })
}

/// Opens an unordered full-table scan (forward only).
///
/// # Safety
/// `table` must be a valid handle; `out_scan` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_open(table: *mut MooseDBTable, out_scan: *mut *mut MooseDBScan) -> TFStatus {
    open_scan(table, &ScanFilter::default(), false, None, out_scan)
}

/// Opens a scan of rows with `ts_start_us <= ts <= ts_end_us` whose TAG columns
/// equal the given values, ordered by timestamp. Supports `moosedb_scan_prev`.
///
/// # Safety
/// `tag_col_indices` and `tag_values` must hold `tag_count` elements each.
#[no_mangle]
pub unsafe extern "C" fn moosedb_range_scan_open(
    table: *mut MooseDBTable,
    ts_start_us: i64,
    ts_end_us: i64,
    tag_col_indices: *const u32,
    tag_values: *const TFValue,
    tag_count: u32,
    out_scan: *mut *mut MooseDBScan,
) -> TFStatus {
    let mut filter = ScanFilter::range(ts_start_us, ts_end_us);
    let status = guard(|| {
        let n = tag_count as usize;
        // SAFETY: caller contract.
        let (cols, vals) =
            unsafe { (slice(tag_col_indices, n, "tag_col_indices")?, slice(tag_values, n, "tag_values")?) };
        for (&c, v) in cols.iter().zip(vals) {
            // SAFETY: caller contract.
            filter.tags.push((c as usize, unsafe { value_from_c(v, ColumnType::Tag) }?));
        }
        Ok(())
    });
    if status != TFStatus::TF_OK {
        return status;
    }
    open_scan(table, &filter, true, None, out_scan)
}

/// Series restriction of a general scan: `series_count < 0` = all series.
///
/// # Safety
/// `series_ids` must hold `series_count` elements when `series_count > 0`.
unsafe fn series_filter(
    ts_start_us: i64,
    ts_end_us: i64,
    series_ids: *const u64,
    series_count: i64,
) -> Result<ScanFilter, Error> {
    let mut filter = ScanFilter::range(ts_start_us, ts_end_us);
    if series_count >= 0 {
        // SAFETY: caller contract.
        let ids = unsafe { slice(series_ids, series_count as usize, "series_ids") }?;
        // Copying can fail (allocation): the callers run this under `guard`.
        filter.series = Some(ids.to_vec());
    }
    Ok(filter)
}

/// General scan: timestamp range, optional series restriction
/// (`series_count < 0` = all series), unordered or timestamp-ordered.
///
/// # Safety
/// `series_ids` must hold `series_count` elements when `series_count > 0`.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_open_filtered(
    table: *mut MooseDBTable,
    ts_start_us: i64,
    ts_end_us: i64,
    series_ids: *const u64,
    series_count: i64,
    sorted: bool,
    out_scan: *mut *mut MooseDBScan,
) -> TFStatus {
    let mut filter = ScanFilter::default();
    let status = guard(|| {
        // SAFETY: caller contract.
        filter = unsafe { series_filter(ts_start_us, ts_end_us, series_ids, series_count) }?;
        Ok(())
    });
    if status != TFStatus::TF_OK {
        return status;
    }
    open_scan(table, &filter, sorted, None, out_scan)
}

/// `moosedb_scan_open_filtered` that decodes and returns only some columns.
///
/// `columns_mask` holds `mask_len` bytes, one per table column in schema
/// order; a non-zero byte requests the column. `mask_len` must equal the
/// number of columns of the table, else `TF_ERR_INVALID_ARG`.
/// `columns_mask == NULL` with `mask_len == 0` requests every column.
///
/// Columns that are not requested are never decoded. Through
/// `moosedb_scan_next` / `moosedb_scan_prev` they read as NULL; through the
/// batch calls they are reported with `present == 0`. The timestamp is
/// decoded internally when the range or the order needs it, whether or not it
/// is requested. `moosedb_snapshot_fetch` ignores the projection: it returns
/// whole rows.
///
/// # Safety
/// `series_ids` must hold `series_count` elements when `series_count > 0`;
/// `columns_mask` must hold `mask_len` readable bytes (or be NULL with
/// `mask_len == 0`).
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_open_projected(
    table: *mut MooseDBTable,
    ts_start_us: i64,
    ts_end_us: i64,
    series_ids: *const u64,
    series_count: i64,
    sorted: bool,
    columns_mask: *const u8,
    mask_len: u32,
    out_scan: *mut *mut MooseDBScan,
) -> TFStatus {
    let mut filter = ScanFilter::default();
    let mut mask: Option<Vec<bool>> = None;
    let status = guard(|| {
        // SAFETY: caller contract for both pointers.
        unsafe {
            filter = series_filter(ts_start_us, ts_end_us, series_ids, series_count)?;
            if mask_len > 0 || !columns_mask.is_null() {
                mask = Some(slice(columns_mask, mask_len as usize, "columns_mask")?.iter().map(|&b| b != 0).collect());
            }
        }
        Ok(())
    });
    if status != TFStatus::TF_OK {
        return status;
    }
    open_scan(table, &filter, sorted, mask.as_deref(), out_scan)
}

/// `TFColPred::kind`: `lo <= v <= hi` on an INT64 / TIMESTAMP / BOOL (0/1)
/// column, bounds in `int_lo` / `int_hi`.
pub const TF_PRED_INT: u8 = 0;
/// `TFColPred::kind`: `lo <= v <= hi` on a FLOAT64 / FLOAT32 column, bounds in
/// `float_lo` / `float_hi` (IEEE; use -inf / +inf for an open side).
pub const TF_PRED_FLOAT: u8 = 1;
/// `TFColPred::kind`: the value equals one of the listed byte strings, on a
/// VARCHAR / DECIMAL column.
pub const TF_PRED_BYTES: u8 = 2;

/// A value predicate that lets a scan **skip** chunk blocks (zone maps / column
/// Blooms); ANDed with the others. It is a hint, not a filter: rows of blocks
/// that are read come back whether they match or not (the SQL layer keeps
/// evaluating its WHERE), and MemTable rows are never skipped. Rules for the
/// caller: NULL must never satisfy the SQL condition it replaces (no `IS NULL`,
/// `<=>`, `OR` across columns, ...); a strict bound is passed as the inclusive
/// one; `TF_PRED_BYTES` only when the SQL comparison is a byte comparison
/// (binary NO PAD collation; with `pad_space` non-zero: binary PAD SPACE of a
/// charset whose space is the single byte 0x20, as `TFAggFilter::pad_space`).
/// An empty list matches nothing. Only the fields of its `kind` are read.
/// A column of the wrong type, a TAG or out-of-range column is
/// `TF_ERR_INVALID_ARG`; the table's timestamp column is accepted and ignored.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFColPred {
    pub col: u32,
    /// `TF_PRED_INT`, `TF_PRED_FLOAT` or `TF_PRED_BYTES`.
    pub kind: u8,
    pub pad_space: u8,
    pub int_lo: i64,
    pub int_hi: i64,
    pub float_lo: f64,
    pub float_hi: f64,
    /// `TF_PRED_BYTES`: value `i` is
    /// `value_data[value_starts[i] .. value_starts[i] + value_lens[i]]`.
    pub value_starts: *const u64,
    pub value_lens: *const u32,
    pub value_count: u64,
    pub value_data: *const u8,
    pub value_data_len: u64,
}

/// # Safety
/// `preds` must hold `count` valid `TFColPred`s whose arrays obey their contract.
unsafe fn col_preds(preds: *const TFColPred, count: u32) -> Result<Vec<ColPred>, Error> {
    // SAFETY: caller contract.
    let preds = unsafe { slice(preds, count as usize, "preds") }?;
    let mut out = Vec::with_capacity(preds.len());
    for p in preds {
        let col = p.col as usize;
        out.push(match p.kind {
            TF_PRED_INT => ColPred::Int { col, lo: p.int_lo, hi: p.int_hi },
            TF_PRED_FLOAT => ColPred::Float { col, lo: p.float_lo, hi: p.float_hi },
            TF_PRED_BYTES => {
                let to_n = |v: u64, what: &str| usize::try_from(v).map_err(|_| invalid(&format!("{what} too long")));
                let (nv, nd) = (to_n(p.value_count, "value_count")?, to_n(p.value_data_len, "value_data_len")?);
                // SAFETY: caller contract for every array.
                let (starts, lens, data) = unsafe {
                    (
                        slice(p.value_starts, nv, "pred.value_starts")?,
                        slice(p.value_lens, nv, "pred.value_lens")?,
                        slice(p.value_data, nd, "pred.value_data")?,
                    )
                };
                let mut values = Vec::with_capacity(nv);
                for (&st, &len) in starts.iter().zip(lens) {
                    let range = usize::try_from(st).ok().and_then(|st| st.checked_add(len as usize).map(|e| (st, e)));
                    let (st, end) = range
                        .filter(|&(_, e)| e <= data.len())
                        .ok_or_else(|| invalid("predicate value out of range"))?;
                    values.push(data[st..end].to_vec());
                }
                ColPred::Bytes { col, values, pad_space: p.pad_space != 0 }
            }
            other => return Err(invalid(&format!("unknown predicate kind {other}"))),
        });
    }
    Ok(out)
}

/// `moosedb_scan_open_projected` plus value predicates that skip chunk blocks
/// (see `TFColPred`). `preds` holds `pred_count` elements (may be NULL with 0).
///
/// # Safety
/// As `moosedb_scan_open_projected`; `preds` and the arrays inside it must be
/// valid for the call.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_open_pruned(
    table: *mut MooseDBTable,
    ts_start_us: i64,
    ts_end_us: i64,
    series_ids: *const u64,
    series_count: i64,
    sorted: bool,
    columns_mask: *const u8,
    mask_len: u32,
    preds: *const TFColPred,
    pred_count: u32,
    out_scan: *mut *mut MooseDBScan,
) -> TFStatus {
    let mut filter = ScanFilter::default();
    let mut mask: Option<Vec<bool>> = None;
    let status = guard(|| {
        // SAFETY: caller contract for every pointer.
        unsafe {
            filter = series_filter(ts_start_us, ts_end_us, series_ids, series_count)?;
            filter.cols = col_preds(preds, pred_count)?;
            if mask_len > 0 || !columns_mask.is_null() {
                mask = Some(slice(columns_mask, mask_len as usize, "columns_mask")?.iter().map(|&b| b != 0).collect());
            }
        }
        Ok(())
    });
    if status != TFStatus::TF_OK {
        return status;
    }
    open_scan(table, &filter, sorted, mask.as_deref(), out_scan)
}

/// Process-wide counters of the zone-map / column-Bloom pruning (scans and
/// aggregations of every table). An "entry" is one series of one chunk: its
/// column blocks are read together and pruned together.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFPruneStats {
    /// Entries that reached the value test.
    pub entries_tested: u64,
    pub pruned_by_zone: u64,
    pub pruned_by_bloom: u64,
    /// Rows of the pruned entries, never decoded.
    pub rows_skipped: u64,
    /// Column blocks inside pruned entries, never read.
    pub blocks_skipped: u64,
    /// Column blocks read from disk (cache misses).
    pub blocks_read: u64,
    /// Nanoseconds reading + decrypting + decompressing those blocks (all threads).
    pub block_read_ns: u64,
}

/// Reads the pruning counters.
///
/// # Safety
/// `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_prune_stats(out: *mut TFPruneStats) -> TFStatus {
    guard(|| {
        let s = moosedb_core::prune_stats();
        *handle_mut(out, "out")? = TFPruneStats {
            entries_tested: s.entries_tested,
            pruned_by_zone: s.pruned_by_zone,
            pruned_by_bloom: s.pruned_by_bloom,
            rows_skipped: s.rows_skipped,
            blocks_skipped: s.blocks_skipped,
            blocks_read: s.blocks_read,
            block_read_ns: s.block_read_ns,
        };
        Ok(())
    })
}

/// Zeroes the pruning counters.
#[no_mangle]
pub extern "C" fn moosedb_prune_stats_reset() -> TFStatus {
    guard(|| {
        moosedb_core::reset_prune_stats();
        Ok(())
    })
}

fn step(scan: *mut MooseDBScan, out_row: *mut TFRow, out_end: *mut bool, backward: bool) -> TFStatus {
    guard(|| {
        let s = handle_mut(scan, "scan")?;
        let (row, end) = (handle_mut(out_row, "out_row")?, handle_mut(out_end, "out_end")?);
        let next = if backward { s.scan.prev_row()? } else { s.scan.next_row()? };
        match next {
            None => {
                *end = true;
                s.position = None;
                clear_row(row);
            }
            Some((pos, r)) => {
                *end = false;
                s.position = Some(pos);
                s.row.publish(r, s.types.iter().copied(), row);
            }
        }
        Ok(())
    })
}

/// Advances the scan. At the end, `*out_eof` is set and `out_row` emptied.
///
/// # Safety
/// `scan` must be valid; `out_row` and `out_eof` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_next(
    scan: *mut MooseDBScan,
    out_row: *mut TFRow,
    out_eof: *mut bool,
) -> TFStatus {
    step(scan, out_row, out_eof, false)
}

/// Steps backwards (sorted scans only). Before the first row, `*out_bof` is set.
///
/// # Safety
/// As `moosedb_scan_next`.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_prev(
    scan: *mut MooseDBScan,
    out_row: *mut TFRow,
    out_bof: *mut bool,
) -> TFStatus {
    step(scan, out_row, out_bof, true)
}

/// Pointer to the first element, or NULL for an empty slice.
fn ptr_or_null<T>(s: &[T]) -> *const T {
    if s.is_empty() {
        ptr::null()
    } else {
        s.as_ptr()
    }
}

fn column_to_c(c: &BatchColumn) -> TFColumnBatch {
    let mut out = TFColumnBatch {
        kind: c.column_type() as u8,
        present: 0,
        nulls: ptr::null(),
        values: ptr::null(),
        starts: ptr::null(),
        lens: ptr::null(),
        data: ptr::null(),
        data_len: 0,
    };
    if !c.is_present() {
        return out;
    }
    out.present = 1;
    out.nulls = c.nulls().map_or(ptr::null(), ptr_or_null);
    match c.column_type() {
        ColumnType::Timestamp | ColumnType::Int64 | ColumnType::Float64 => {
            out.values = ptr_or_null(c.values_w64()).cast();
        }
        ColumnType::Float32 => out.values = ptr_or_null(c.values_w32()).cast(),
        ColumnType::Bool => out.values = ptr_or_null(c.values_w8()).cast(),
        ColumnType::Varchar | ColumnType::Tag | ColumnType::Decimal => {
            out.starts = ptr_or_null(c.starts());
            out.lens = ptr_or_null(c.lens());
            out.data = ptr_or_null(c.data());
            out.data_len = c.data().len() as u64;
        }
    }
    out
}

fn step_batch(
    scan: *mut MooseDBScan,
    max_rows: u32,
    out: *mut TFBatch,
    out_end: *mut bool,
    backward: bool,
) -> TFStatus {
    guard(|| {
        let s = handle_mut(scan, "scan")?;
        let (out, end) = (handle_mut(out, "out")?, handle_mut(out_end, "out_end")?);
        // Whatever the previous call returned is invalid from here on.
        *out = TFBatch { row_count: 0, col_count: 0, columns: ptr::null(), positions: ptr::null() };
        *end = false;
        s.position = None;
        let max = max_rows as usize; // 0 selects the core default
        let MooseDBScan { scan, position, batch_cols, .. } = s;
        let batch = scan.next_batch(max, backward)?;
        let rows = batch.rows();
        batch_cols.clear();
        batch_cols.extend(batch.columns().iter().map(column_to_c));
        *position = batch.positions().last().map(Position::from_bytes);
        *end = rows == 0;
        out.row_count = rows as u32;
        out.col_count = batch_cols.len() as u32;
        out.columns = ptr_or_null(batch_cols);
        out.positions = ptr_or_null(batch.positions()).cast();
        Ok(())
    })
}

/// Reads the next rows of the scan into columnar buffers owned by the scan.
///
/// Fills `*out` with up to `max_rows` rows (`0` = 1024; at most 65536). When
/// the scan is exhausted `out->row_count` is 0 and `*out_end` is set; a
/// non-empty batch never sets `*out_end` (the end is reported by the next
/// call). A batch may hold fewer rows than asked for.
///
/// Lifetime: everything `*out` points to (the `columns` array, every column
/// buffer and `positions`) belongs to the scan and stays valid until the next
/// call on that scan (`moosedb_scan_next`, `_prev`, `_next_batch`,
/// `_prev_batch`, `_seek_end`) or `moosedb_scan_close`. It is invalidated even
/// if that next call fails. Buffers are reused, so the steady state does not
/// allocate.
///
/// Position: after the call, `moosedb_scan_position` returns the position of
/// the last row of the batch. `positions` holds `row_count` entries of
/// `TF_POSITION_LEN` bytes, one per row, in the format `moosedb_scan_position`
/// writes and `moosedb_snapshot_fetch` takes, so `position()` can be served
/// for any row of the batch without calling back.
///
/// Direction: the batch calls work on sorted and unsorted scans alike, but
/// `moosedb_scan_prev_batch` needs a sorted scan (`TF_ERR_UNSUPPORTED`
/// otherwise). The cursor ends after the last row of the batch, not after the
/// row the handler is serving: a handler that switches direction
/// (`index_prev` after `index_next`) must account for the rows read ahead
/// (the next row in the other direction is the one before the batch's last).
/// Reading ahead decodes data early; `ORDER BY ... LIMIT n` should start with
/// a small `max_rows`.
///
/// # Safety
/// `scan` must be valid; `out` and `out_end` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_next_batch(
    scan: *mut MooseDBScan,
    max_rows: u32,
    out: *mut TFBatch,
    out_end: *mut bool,
) -> TFStatus {
    step_batch(scan, max_rows, out, out_end, false)
}

/// Like `moosedb_scan_next_batch`, for the rows before the cursor (sorted
/// scans only), nearest first: after `moosedb_scan_seek_end` the batch holds
/// the newest rows, newest first.
///
/// # Safety
/// As `moosedb_scan_next_batch`.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_prev_batch(
    scan: *mut MooseDBScan,
    max_rows: u32,
    out: *mut TFBatch,
    out_end: *mut bool,
) -> TFStatus {
    step_batch(scan, max_rows, out, out_end, true)
}

/// Rewinds the scan to before its first row on the same snapshot it opened with
/// (no new snapshot: rows written since the scan opened stay invisible). Works
/// on sorted and unsorted scans; the next `moosedb_scan_next` /
/// `moosedb_scan_next_batch` returns the first row.
///
/// After a scan reported its end (EOF with `next*`, BOF with `prev*`), the
/// only supported way to continue is `moosedb_scan_seek_start` or
/// `moosedb_scan_seek_end`; what `prev`/`next` do straight after an end is
/// unspecified.
///
/// # Safety
/// `scan` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_seek_start(scan: *mut MooseDBScan) -> TFStatus {
    guard(|| {
        let s = handle_mut(scan, "scan")?;
        s.position = None;
        s.scan.seek_start()
    })
}

/// Positions a sorted scan after its last row (for `index_last`).
///
/// # Safety
/// `scan` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_seek_end(scan: *mut MooseDBScan) -> TFStatus {
    guard(|| handle_mut(scan, "scan")?.scan.seek_end())
}

/// Writes the `TF_POSITION_LEN`-byte position of the row last returned.
///
/// # Safety
/// `out_pos` must have room for `TF_POSITION_LEN` bytes.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_position(scan: *mut MooseDBScan, out_pos: *mut u8) -> TFStatus {
    guard(|| {
        let s = handle_mut(scan, "scan")?;
        let pos = s.position.ok_or_else(|| invalid("no current row"))?;
        *handle_mut(out_pos.cast::<[u8; POSITION_LEN]>(), "out_pos")? = pos.to_bytes();
        Ok(())
    })
}

/// Returns a handle on the snapshot the scan reads, which resolves the
/// positions of its rows even after the scan is closed.
///
/// # Safety
/// `scan` must be valid; `out_snapshot` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_snapshot(
    scan: *mut MooseDBScan,
    out_snapshot: *mut *mut MooseDBSnapshot,
) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_snapshot, "out_snapshot")?;
        *out = ptr::null_mut();
        let s = handle_mut(scan, "scan")?;
        let snap = s.scan.snapshot().clone();
        *out = Box::into_raw(Box::new(MooseDBSnapshot { snap, types: s.types.clone(), row: RowOut::new() }));
        Ok(())
    })
}

/// # Safety
/// `scan` must come from a `*_scan_open*` call and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_scan_close(scan: *mut MooseDBScan) -> TFStatus {
    guard(|| {
        release(scan);
        Ok(())
    })
}

/// Identifies the table contents the snapshot shows: equal versions mean
/// identical snapshots.
///
/// # Safety
/// `snapshot` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_snapshot_version(snapshot: *const MooseDBSnapshot) -> u64 {
    guard_value(0, || handle_ref(snapshot, "snapshot").map_or(0, |s| s.snap.version()))
}

/// Re-reads the row at `pos` (handler `rnd_pos`). `TF_ERR_NOT_FOUND` if the
/// snapshot does not cover that position.
///
/// # Safety
/// `pos` must point to `TF_POSITION_LEN` readable bytes; `out_row` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_snapshot_fetch(
    snapshot: *mut MooseDBSnapshot,
    pos: *const u8,
    out_row: *mut TFRow,
) -> TFStatus {
    guard(|| {
        let s = handle_mut(snapshot, "snapshot")?;
        let row = s.snap.fetch(read_position(pos)?)?;
        s.row.publish(row, s.types.iter().copied(), handle_mut(out_row, "out_row")?);
        Ok(())
    })
}

/// # Safety
/// `snapshot` must come from `moosedb_scan_snapshot` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_snapshot_close(snapshot: *mut MooseDBSnapshot) -> TFStatus {
    guard(|| {
        release(snapshot);
        Ok(())
    })
}

// ─── aggregation ─────────────────────────────────────────────────────────────

/// Group key kinds of `TFAggKey::kind`.
pub const TF_AGG_KEY_SERIES: u8 = 0;
pub const TF_AGG_KEY_COLUMN: u8 = 1;
/// A VARCHAR column under a binary PAD SPACE collation (utf8mb4_bin, latin1_bin, ...; charsets whose
/// space is the byte 0x20): values differing only by trailing 0x20 bytes are one group, keyed by the
/// value without them; a top-N breaks ties in that collation order.
pub const TF_AGG_KEY_COLUMN_PAD: u8 = 2;
/// A TIMESTAMP column cut in buckets of `TFAggKey::width_us` microseconds aligned to the epoch
/// (floor): the key value is the bucket start, in microseconds.
pub const TF_AGG_KEY_BUCKET: u8 = 3;
/// Function codes of `TFAggFunc::op`.
pub const TF_AGG_COUNT_STAR: u8 = 0;
pub const TF_AGG_COUNT: u8 = 1;
pub const TF_AGG_SUM: u8 = 2;
pub const TF_AGG_MIN: u8 = 3;
pub const TF_AGG_MAX: u8 = 4;
/// Result key column kinds (`TFAggKeyBatch::kind`).
pub const TF_AGG_KEYCOL_SERIES: u8 = 0;
pub const TF_AGG_KEYCOL_INT: u8 = 1;
pub const TF_AGG_KEYCOL_BYTES: u8 = 2;

/// One group-by key: the series (`col` ignored) or a column (`width_us` only for `TF_AGG_KEY_BUCKET`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFAggKey {
    /// `TF_AGG_KEY_SERIES`, `TF_AGG_KEY_COLUMN`, `TF_AGG_KEY_COLUMN_PAD` or `TF_AGG_KEY_BUCKET`.
    pub kind: u8,
    pub col: u32,
    /// Bucket width in microseconds (> 0), `TF_AGG_KEY_BUCKET` only; 0 otherwise.
    pub width_us: i64,
}

/// One aggregate function (`col` ignored for `TF_AGG_COUNT_STAR`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFAggFunc {
    /// `TF_AGG_COUNT_STAR` .. `TF_AGG_MAX`.
    pub op: u8,
    pub col: u32,
}

/// `TFAggFilter::mode`: dictionary mask (the zero value, the original behaviour).
pub const TF_AGG_FILTER_MASK: u8 = 0;
/// `TFAggFilter::mode`: exact set of accepted values.
pub const TF_AGG_FILTER_VALUES: u8 = 1;
/// `TFAggSpec::top_mode`: no top-N (the zero value).
pub const TF_AGG_TOP_NONE: u8 = 0;
/// `TFAggSpec::top_mode`: the `top_limit` smallest groups by `funcs[top_func]`.
pub const TF_AGG_TOP_ASC: u8 = 1;
/// `TFAggSpec::top_mode`: the `top_limit` largest groups by `funcs[top_func]`.
pub const TF_AGG_TOP_DESC: u8 = 2;

/// A filter on a column, ANDed with the others.
///
/// * `mode == TF_AGG_FILTER_MASK` (0): dictionary mask. `mask[i] != 0` accepts
///   the i-th value of the dictionary that an earlier run with the single key
///   `{COLUMN, col}`, no functions and no filters left in the result (result
///   order). The fields after `mask_len` are ignored.
/// * `mode == TF_AGG_FILTER_VALUES` (1): a row is kept iff its value in `col`
///   is one of the listed values, compared **byte for byte** (a NULL value is
///   never kept; an empty list keeps nothing; duplicates are harmless). No
///   dictionary run is needed. For INT64/TIMESTAMP/BOOL columns the values are
///   `int_values[0..int_count]` (`bool` as 0/1) and `value_count` must be 0;
///   for VARCHAR/DECIMAL columns value `i` is
///   `value_data[value_starts[i] .. value_starts[i] + value_lens[i]]`
///   (`value_starts[i] + value_lens[i] <= value_data_len`) and `int_count`
///   must be 0. `mask` is ignored. **The caller may use this only when the SQL
///   comparison it replaces is exactly a byte comparison** (integers, or a
///   binary / NO PAD collation); with any other collation (case or accent
///   insensitive, PAD SPACE) it would give wrong answers.
///
/// A zero-initialized tail (every field after `mask_len` zero) is a plain
/// dictionary mask.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFAggFilter {
    pub col: u32,
    pub mask: *const u8,
    pub mask_len: u64,
    /// `TF_AGG_FILTER_MASK` or `TF_AGG_FILTER_VALUES`.
    pub mode: u8,
    pub int_values: *const i64,
    pub int_count: u64,
    pub value_starts: *const u64,
    pub value_lens: *const u32,
    pub value_count: u64,
    pub value_data: *const u8,
    pub value_data_len: u64,
    /// `TF_AGG_FILTER_VALUES` only; non-zero = PAD SPACE comparison: trailing
    /// 0x20 bytes are ignored on both sides (the listed values and the row's
    /// value), as a binary PAD SPACE collation (`utf8mb4_bin`, `latin1_bin`,
    /// ...) of a charset whose space is the single byte 0x20 and never a
    /// continuation byte (utf8mb4, utf8mb3, latin1, ascii; NOT ucs2, utf16,
    /// utf32) compares. Other whitespace (tab...) is not trimmed. NULL is
    /// still never accepted. Zero = exact bytes (NO PAD).
    pub pad_space: u8,
}

/// `TFAggFloatFilter::flags`: the lower bound is exclusive (`v > lo`).
pub const TF_FLOAT_LO_OPEN: u8 = 1;
/// `TFAggFloatFilter::flags`: the upper bound is exclusive (`v < hi`).
pub const TF_FLOAT_HI_OPEN: u8 = 2;
/// `TFAggFloatFilter::flags`: the value must also equal one of `points[0..point_count]`
/// (an empty list then keeps nothing).
pub const TF_FLOAT_POINTS: u8 = 4;

/// Exact filter on a FLOAT64 / FLOAT32 column, ANDed with the others: a row is
/// kept iff its value is non-NULL, not NaN, within `lo..hi` (`-INFINITY` /
/// `INFINITY` = unbounded; ends closed unless `flags` says open) and, with
/// `TF_FLOAT_POINTS`, equal to one of the points (IEEE equality: `-0.0 == 0.0`).
/// A FLOAT32 value is widened to double first. NaN bounds and non-finite
/// points are `TF_ERR_INVALID_ARG`. **The caller may use this only for a plain
/// `FLOAT`/`DOUBLE` column** (not `(M,D)`, which the server compares with an epsilon).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFAggFloatFilter {
    pub col: u32,
    pub lo: f64,
    pub hi: f64,
    pub flags: u8,
    pub points: *const f64,
    pub point_count: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFAggSpec {
    pub keys: *const TFAggKey,
    pub key_count: u32,
    pub funcs: *const TFAggFunc,
    pub func_count: u32,
    pub filters: *const TFAggFilter,
    pub filter_count: u32,
    /// Budget in bytes for keys, states and result.
    pub max_memory: u64,
    /// Worker threads (1 = the calling thread only).
    pub threads: u32,
    /// Top-N: `TF_AGG_TOP_NONE` (0, every group is returned, in no particular
    /// order), `TF_AGG_TOP_ASC` or `TF_AGG_TOP_DESC`. Needs exactly one key
    /// (`TF_ERR_INVALID_ARG` otherwise). Only the `top_limit` best groups by
    /// `funcs[top_func]` are returned, best first (`total_groups` of the
    /// result is that count). The order is the SQL order of the function
    /// value (`COUNT` as a number; `SUM`/`MIN`/`MAX` as numbers with "no
    /// value" = NULL, the smallest, so first when ascending); groups with the
    /// same value come in ascending order of the key: series id, integer
    /// value, or bytes compared as unsigned (a shorter prefix first); a NULL
    /// key first. **The caller may use this only when that order is the one
    /// the query asks for**: integer keys, or byte keys under a binary /
    /// NO PAD collation, and ties that the SQL leaves unspecified anyway.
    /// `top_limit == 0` returns no group.
    pub top_mode: u8,
    pub top_func: u32,
    pub top_limit: u64,
    /// Zone-map hints (see `TFColPred`): entries that cannot hold a row inside
    /// them are skipped, on top of the filters. `preds` may be NULL with 0.
    pub preds: *const TFColPred,
    pub pred_count: u32,
    /// Exact float filters (`TFAggFloatFilter`), ANDed with `filters`. May be NULL with 0.
    pub float_filters: *const TFAggFloatFilter,
    pub float_filter_count: u32,
}

/// Polled about every 10 ms on the calling thread; a non-zero return cancels
/// the run (`TF_ERR_CANCELLED`).
pub type TFAggCancel = Option<unsafe extern "C" fn(ctx: *mut c_void) -> i32>;

/// A key column of the result, one entry per group. `nulls` (NULL for the
/// series) holds 1 for a NULL value. `values` holds `uint64_t` series ids
/// (`TF_AGG_KEYCOL_SERIES`) or `int64_t` values (`TF_AGG_KEYCOL_INT`);
/// `starts`/`lens`/`data` describe byte strings (`TF_AGG_KEYCOL_BYTES`):
/// `starts[i] + lens[i] <= data_len`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFAggKeyBatch {
    pub kind: u8,
    pub nulls: *const u8,
    pub values: *const c_void,
    pub starts: *const u64,
    pub lens: *const u32,
    pub data: *const u8,
    pub data_len: u64,
}

/// A function column of the result, one entry per group. `count` is the number
/// of rows (COUNT(*)), of non-NULL values (COUNT) or of values aggregated by
/// SUM/MIN/MAX (0 = the SQL result is NULL). SUM: the signed 128-bit value
/// `(sum_hi << 64) | sum_lo`. MIN/MAX: `val`. Pointers the op does not use are NULL.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFAggFuncBatch {
    pub op: u8,
    pub count: *const u64,
    pub sum_lo: *const u64,
    pub sum_hi: *const i64,
    pub val: *const i64,
}

/// Groups `[start, start + row_count)` of the last result. Owned by the
/// handle; valid until the next call on it.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TFAggBatch {
    pub row_count: u64,
    /// Groups in the whole result.
    pub total_groups: u64,
    pub key_count: u32,
    pub func_count: u32,
    pub keys: *const TFAggKeyBatch,
    pub funcs: *const TFAggFuncBatch,
}

/// Opaque aggregation handle: one snapshot, used by one thread at a time.
pub struct MooseDBAgg {
    agg: Agg,
    ops: Vec<u8>,
    keys_out: Vec<TFAggKeyBatch>,
    funcs_out: Vec<TFAggFuncBatch>,
}

/// Opens an aggregation over a snapshot of the table, with the visibility of
/// an unordered scan: rows with `ts_start_us <= ts <= ts_end_us`, only in the
/// given series (`series_count < 0` = all series).
///
/// # Safety
/// `series_ids` must hold `series_count` elements when `series_count > 0`;
/// `out_agg` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_agg_open(
    table: *mut MooseDBTable,
    ts_start_us: i64,
    ts_end_us: i64,
    series_ids: *const u64,
    series_count: i64,
    out_agg: *mut *mut MooseDBAgg,
) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_agg, "out_agg")?;
        *out = ptr::null_mut();
        let t = table_ref(table)?;
        // SAFETY: caller contract.
        let filter = unsafe { series_filter(ts_start_us, ts_end_us, series_ids, series_count) }?;
        let agg = t.table.agg_open(ts_start_us, ts_end_us, filter.series)?;
        *out = Box::into_raw(Box::new(MooseDBAgg { agg, ops: Vec::new(), keys_out: Vec::new(), funcs_out: Vec::new() }));
        Ok(())
    })
}

/// Runs the aggregation described by `spec`; the result is read with
/// `moosedb_agg_result` (it replaces the previous one; a failed run leaves
/// none). `cancel` may be NULL. Errors: `TF_ERR_INVALID_ARG` (bad column or
/// type, missing dictionary, mask of the wrong length), `TF_ERR_FULL` (memory
/// budget, SUM overflow), `TF_ERR_CANCELLED`, `TF_ERR_INTERNAL` (a panic in a
/// worker thread).
///
/// # Safety
/// `agg` valid; `spec` and the arrays it points to valid for the call;
/// `cancel(ctx)` must be callable from this thread.
#[no_mangle]
pub unsafe extern "C" fn moosedb_agg_run(
    agg: *mut MooseDBAgg,
    spec: *const TFAggSpec,
    cancel: TFAggCancel,
    ctx: *mut c_void,
) -> TFStatus {
    guard(|| {
        let a = handle_mut(agg, "agg")?;
        let s = handle_ref(spec, "spec")?;
        a.ops.clear();
        a.keys_out.clear();
        a.funcs_out.clear();
        // SAFETY: caller contract for every array.
        let (keys, funcs, filters, float_filters) = unsafe {
            (
                slice(s.keys, s.key_count as usize, "spec.keys")?,
                slice(s.funcs, s.func_count as usize, "spec.funcs")?,
                slice(s.filters, s.filter_count as usize, "spec.filters")?,
                slice(s.float_filters, s.float_filter_count as usize, "spec.float_filters")?,
            )
        };
        let mut ffs = Vec::new();
        for f in float_filters {
            let points = if f.flags & TF_FLOAT_POINTS != 0 {
                let n = usize::try_from(f.point_count).map_err(|_| invalid("float filter points too long"))?;
                // SAFETY: caller contract.
                Some(unsafe { slice(f.points, n, "float_filter.points") }?.to_vec())
            } else {
                None
            };
            ffs.push(FloatFilter {
                col: f.col as usize,
                lo: f.lo,
                hi: f.hi,
                lo_open: f.flags & TF_FLOAT_LO_OPEN != 0,
                hi_open: f.flags & TF_FLOAT_HI_OPEN != 0,
                points,
            });
        }
        let keys = keys
            .iter()
            .map(|k| match k.kind {
                TF_AGG_KEY_SERIES => Ok(KeySpec::Series),
                TF_AGG_KEY_COLUMN => Ok(KeySpec::Column(k.col as usize)),
                TF_AGG_KEY_COLUMN_PAD => Ok(KeySpec::ColumnPad(k.col as usize)),
                TF_AGG_KEY_BUCKET => Ok(KeySpec::Bucket(k.col as usize, k.width_us)),
                other => Err(invalid(&format!("unknown key kind {other}"))),
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let ops: Vec<u8> = funcs.iter().map(|f| f.op).collect();
        let funcs = funcs
            .iter()
            .map(|f| {
                let c = f.col as usize;
                match f.op {
                    TF_AGG_COUNT_STAR => Ok(FuncSpec::CountStar),
                    TF_AGG_COUNT => Ok(FuncSpec::Count(c)),
                    TF_AGG_SUM => Ok(FuncSpec::Sum(c)),
                    TF_AGG_MIN => Ok(FuncSpec::Min(c)),
                    TF_AGG_MAX => Ok(FuncSpec::Max(c)),
                    other => Err(invalid(&format!("unknown function {other}"))),
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut fs = Vec::new();
        let mut vfs = Vec::new();
        for f in filters {
            match f.mode {
                TF_AGG_FILTER_MASK => {
                    let n = usize::try_from(f.mask_len).map_err(|_| invalid("filter mask too long"))?;
                    // SAFETY: caller contract.
                    let mask = unsafe { slice(f.mask, n, "filter.mask") }?;
                    fs.push(ColFilter { col: f.col as usize, mask: mask.iter().map(|&m| m != 0).collect() });
                }
                TF_AGG_FILTER_VALUES => {
                    let to_n = |v: u64, what: &str| usize::try_from(v).map_err(|_| invalid(&format!("{what} too long")));
                    let (ni, nv, nd) = (
                        to_n(f.int_count, "int_count")?,
                        to_n(f.value_count, "value_count")?,
                        to_n(f.value_data_len, "value_data_len")?,
                    );
                    // SAFETY: caller contract for every array.
                    let (ints, starts, lens, data) = unsafe {
                        (
                            slice(f.int_values, ni, "filter.int_values")?,
                            slice(f.value_starts, nv, "filter.value_starts")?,
                            slice(f.value_lens, nv, "filter.value_lens")?,
                            slice(f.value_data, nd, "filter.value_data")?,
                        )
                    };
                    let mut bytes = Vec::with_capacity(nv);
                    for (&st, &len) in starts.iter().zip(lens) {
                        let range = usize::try_from(st).ok().and_then(|st| st.checked_add(len as usize).map(|e| (st, e)));
                        let (st, end) = range
                            .filter(|&(_, e)| e <= data.len())
                            .ok_or_else(|| invalid("filter value out of range"))?;
                        bytes.push(data[st..end].to_vec());
                    }
                    vfs.push(ValueFilter { col: f.col as usize, ints: ints.to_vec(), bytes, pad_space: f.pad_space != 0 });
                }
                other => return Err(invalid(&format!("unknown filter mode {other}"))),
            }
        }
        let top = match s.top_mode {
            TF_AGG_TOP_NONE => None,
            m @ (TF_AGG_TOP_ASC | TF_AGG_TOP_DESC) => Some(TopN {
                func: s.top_func as usize,
                desc: m == TF_AGG_TOP_DESC,
                limit: usize::try_from(s.top_limit).unwrap_or(usize::MAX),
            }),
            other => return Err(invalid(&format!("unknown top mode {other}"))),
        };
        let spec = AggSpec {
            keys,
            funcs,
            filters: fs,
            value_filters: vfs,
            float_filters: ffs,
            // SAFETY: caller contract for `preds`.
            preds: unsafe { col_preds(s.preds, s.pred_count) }?,
            top,
            max_memory: usize::try_from(s.max_memory).unwrap_or(usize::MAX),
            threads: s.threads as usize,
        };
        let mut poll = cancel.map(|cb| {
            move || {
                // SAFETY: the caller guarantees `cb(ctx)` is callable on this thread.
                unsafe { cb(ctx) != 0 }
            }
        });
        let poll: Option<&mut dyn FnMut() -> bool> = poll.as_mut().map(|p| p as &mut dyn FnMut() -> bool);
        a.agg.run(&spec, poll)?;
        a.ops = ops;
        Ok(())
    })
}

/// Groups `[start, start + max_groups)` of the last result, in columns (an
/// empty batch when `start` is past the end). Order is unspecified but stable
/// until the next run, except for a run with a top-N, whose groups come best
/// first. Valid until the next call on the handle.
///
/// # Safety
/// `agg` valid; `out` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_agg_result(
    agg: *mut MooseDBAgg,
    start: u64,
    max_groups: u64,
    out: *mut TFAggBatch,
) -> TFStatus {
    guard(|| {
        let a = handle_mut(agg, "agg")?;
        let out = handle_mut(out, "out")?;
        *out = TFAggBatch {
            row_count: 0,
            total_groups: 0,
            key_count: 0,
            func_count: 0,
            keys: ptr::null(),
            funcs: ptr::null(),
        };
        let r = a.agg.result();
        let total = r.groups;
        let from = usize::try_from(start).unwrap_or(usize::MAX).min(total);
        let to = from.saturating_add(usize::try_from(max_groups).unwrap_or(usize::MAX)).min(total);
        a.keys_out.clear();
        a.funcs_out.clear();
        for k in &r.keys {
            a.keys_out.push(match k {
                KeyCol::Series(v) => TFAggKeyBatch {
                    kind: TF_AGG_KEYCOL_SERIES,
                    nulls: ptr::null(),
                    values: ptr_or_null(&v[from..to]).cast(),
                    starts: ptr::null(),
                    lens: ptr::null(),
                    data: ptr::null(),
                    data_len: 0,
                },
                KeyCol::Int { nulls, vals } => TFAggKeyBatch {
                    kind: TF_AGG_KEYCOL_INT,
                    nulls: ptr_or_null(&nulls[from..to]).cast(),
                    values: ptr_or_null(&vals[from..to]).cast(),
                    starts: ptr::null(),
                    lens: ptr::null(),
                    data: ptr::null(),
                    data_len: 0,
                },
                KeyCol::Bytes { nulls, starts, lens, data } => TFAggKeyBatch {
                    kind: TF_AGG_KEYCOL_BYTES,
                    nulls: ptr_or_null(&nulls[from..to]).cast(),
                    values: ptr::null(),
                    starts: ptr_or_null(&starts[from..to]),
                    lens: ptr_or_null(&lens[from..to]),
                    data: ptr_or_null(data),
                    data_len: data.len() as u64,
                },
            });
        }
        for (f, &op) in r.funcs.iter().zip(&a.ops) {
            let (sum, val) = (op == TF_AGG_SUM, op == TF_AGG_MIN || op == TF_AGG_MAX);
            a.funcs_out.push(TFAggFuncBatch {
                op,
                count: ptr_or_null(&f.count[from..to]),
                sum_lo: if sum { ptr_or_null(&f.sum_lo[from..to]) } else { ptr::null() },
                sum_hi: if sum { ptr_or_null(&f.sum_hi[from..to]) } else { ptr::null() },
                val: if val { ptr_or_null(&f.val[from..to]) } else { ptr::null() },
            });
        }
        out.row_count = (to - from) as u64;
        out.total_groups = total as u64;
        out.key_count = a.keys_out.len() as u32;
        out.func_count = a.funcs_out.len() as u32;
        out.keys = ptr_or_null(&a.keys_out);
        out.funcs = ptr_or_null(&a.funcs_out);
        Ok(())
    })
}

/// # Safety
/// `agg` must come from `moosedb_agg_open` and not be used afterwards. NULL is ignored.
#[no_mangle]
pub unsafe extern "C" fn moosedb_agg_close(agg: *mut MooseDBAgg) -> TFStatus {
    guard(|| {
        release(agg);
        Ok(())
    })
}

// ─── series ──────────────────────────────────────────────────────────────────

/// Version of the table's set of series: it changes whenever a series is added
/// or the table is truncated, so equal values mean the same series. Lock-free
/// and O(1): call it before every pushdown and rebuild a cached index (from
/// `moosedb_series_list`) only when it differs from the cached one.
///
/// # Safety
/// `table` must be valid; `out_version` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_series_version(table: *mut MooseDBTable, out_version: *mut u64) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_version, "out_version")?;
        *out = table_ref(table)?.table.series_version();
        Ok(())
    })
}

/// Lists every series of the table (for TAG predicate pushdown). The list is
/// a shared snapshot: O(1) and no copy of the index under the table lock, at
/// any number of series. Entries come in registration order (not sorted by
/// id); the list never changes after it is returned.
///
/// # Safety
/// `table` must be valid; `out_list` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_series_list(
    table: *mut MooseDBTable,
    out_list: *mut *mut MooseDBSeriesList,
) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_list, "out_list")?;
        *out = ptr::null_mut();
        let snap = table_ref(table)?.table.series_snapshot()?;
        *out = Box::into_raw(Box::new(MooseDBSeriesList { snap, row: RowOut::new() }));
        Ok(())
    })
}

/// Number of series in the list.
///
/// # Safety
/// `list` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_series_list_len(list: *const MooseDBSeriesList) -> u64 {
    guard_value(0, || handle_ref(list, "list").map_or(0, |l| l.snap.len() as u64))
}

/// The `moosedb_series_version` value this list corresponds to (read together
/// with its content, so a cache keyed by it is never stale).
///
/// # Safety
/// `list` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_series_list_version(list: *const MooseDBSeriesList) -> u64 {
    guard_value(0, || handle_ref(list, "list").map_or(0, |l| l.snap.version()))
}

/// Identity of the log behind the list, unique in the process: it changes at
/// every TRUNCATE and every table open and never repeats. Guarantee: two lists
/// of the same table with the same epoch are prefixes of one append-only log, so
/// the later list is the earlier one plus new entries (same series at the same
/// indexes); a different epoch means the list was replaced and must be reread
/// from scratch. (Versions only say "something changed", not how.)
///
/// # Safety
/// `list` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_series_list_epoch(list: *const MooseDBSeriesList) -> u64 {
    guard_value(0, || handle_ref(list, "list").map_or(0, |l| l.snap.epoch()))
}

/// Series `index`: its id and its TAG values (in table TAG-column order).
///
/// # Safety
/// `list` must be valid; `out_id`, `out_tags` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_series_list_get(
    list: *mut MooseDBSeriesList,
    index: u64,
    out_id: *mut u64,
    out_tags: *mut TFRow,
) -> TFStatus {
    guard(|| {
        let l = handle_mut(list, "list")?;
        let (id, tags) = l
            .snap
            .get(usize::try_from(index).unwrap_or(usize::MAX))
            .ok_or_else(|| invalid("series index out of range"))?;
        *handle_mut(out_id, "out_id")? = id;
        let n = tags.len();
        l.row.publish(tags.to_vec(), std::iter::repeat(ColumnType::Tag).take(n), handle_mut(out_tags, "out_tags")?);
        Ok(())
    })
}

/// TAG values of the series with this id (`TF_ERR_NOT_FOUND` if the list does
/// not have it): O(1), for a caller that needs a few of many series.
///
/// # Safety
/// `list` must be valid; `out_tags` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_series_list_find(
    list: *mut MooseDBSeriesList,
    id: u64,
    out_tags: *mut TFRow,
) -> TFStatus {
    guard(|| {
        let l = handle_mut(list, "list")?;
        let tags = l.snap.get_by_id(id).ok_or_else(|| Error::NotFound(format!("series {id}")))?;
        let n = tags.len();
        l.row.publish(tags.to_vec(), std::iter::repeat(ColumnType::Tag).take(n), handle_mut(out_tags, "out_tags")?);
        Ok(())
    })
}

/// # Safety
/// `list` must come from `moosedb_series_list` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_series_list_close(list: *mut MooseDBSeriesList) -> TFStatus {
    guard(|| {
        release(list);
        Ok(())
    })
}

// ─── maintenance ─────────────────────────────────────────────────────────────

/// Merges the chunks of every time bucket overlapping `[ts_start_us,
/// ts_end_us]` and re-encodes cold chunks with the cold codec.
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn moosedb_compact(table: *mut MooseDBTable, ts_start_us: i64, ts_end_us: i64) -> TFStatus {
    guard(|| {
        table_ref(table)?.table.compact(ts_start_us, ts_end_us, moosedb_core::time::now_micros())?;
        Ok(())
    })
}

/// Deletes chunks entirely older than RETENTION_PERIOD.
///
/// # Safety
/// `table` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn moosedb_apply_retention(table: *mut MooseDBTable) -> TFStatus {
    guard(|| table_ref(table)?.table.apply_retention(moosedb_core::time::now_micros()).map(drop))
}

/// Verifies the CRC32 of every chunk. `*out_bad_chunks` receives the number of
/// damaged chunks; when non-zero, `moosedb_last_error` describes them.
///
/// # Safety
/// `table` must be valid; `out_bad_chunks` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_check(table: *mut MooseDBTable, out_bad_chunks: *mut u32) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_bad_chunks, "out_bad_chunks")?;
        let problems = table_ref(table)?.table.check()?;
        *out = problems.len() as u32;
        if !problems.is_empty() {
            let msg: Vec<String> = problems.iter().map(|(f, why)| format!("{f}: {why}")).collect();
            set_last_error(&msg.join("; "));
        }
        Ok(())
    })
}

// ─── info ────────────────────────────────────────────────────────────────────

/// Any out pointer may be NULL if the caller is not interested.
///
/// # Safety
/// `table` must be valid; non-NULL out pointers writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_table_stats(
    table: *mut MooseDBTable,
    out_row_count: *mut u64,
    out_data_bytes: *mut u64,
    out_compressed_bytes: *mut u64,
    out_chunk_count: *mut u32,
) -> TFStatus {
    guard(|| {
        let s = table_ref(table)?.table.stats()?;
        // SAFETY: caller contract (NULL allowed).
        unsafe {
            if let Some(p) = out_row_count.as_mut() {
                *p = s.row_count;
            }
            if let Some(p) = out_data_bytes.as_mut() {
                *p = s.data_bytes;
            }
            if let Some(p) = out_compressed_bytes.as_mut() {
                *p = s.compressed_bytes;
            }
            if let Some(p) = out_chunk_count.as_mut() {
                *p = s.chunk_count;
            }
        }
        Ok(())
    })
}

/// Optimizer estimate of rows with `ts_start_us <= ts <= ts_end_us`.
///
/// # Safety
/// `table` must be valid; `out_rows` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_estimate_rows(
    table: *mut MooseDBTable,
    ts_start_us: i64,
    ts_end_us: i64,
    out_rows: *mut u64,
) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_rows, "out_rows")?;
        *out = table_ref(table)?.table.estimate_rows(ts_start_us, ts_end_us)?;
        Ok(())
    })
}

fn build_info(info: TableInfo) -> Result<MooseDBInfo, Error> {
    let mut strings = Vec::new();
    let mut keep = |s: &str| -> *const c_char {
        let c = CString::new(s.replace('\0', " ")).unwrap_or_default();
        let p = c.as_ptr();
        strings.push(c);
        p
    };
    let opts = info.options.as_ref();
    let table = TFTableInfo {
        is_open: info.is_open,
        encrypted: opts.is_some_and(|o| o.encryption_key_id.is_some()),
        row_count: info.row_count,
        pending_rows: info.pending_rows,
        series_count: info.series_count,
        data_bytes: info.data_bytes,
        compressed_bytes: info.file_bytes,
        chunk_count: info.chunks.len() as u32,
        hot_chunks: info.count(ChunkStatus::Hot),
        warm_chunks: info.count(ChunkStatus::Warm),
        cold_chunks: info.count(ChunkStatus::Cold),
        compacting_chunks: info.count(ChunkStatus::Compacting),
        expired_chunks: info.count(ChunkStatus::Expired),
        chunk_interval: keep(opts.map_or("", |o| o.chunk_interval_text.as_str())),
        retention_period: keep(opts.map_or("", |o| o.retention_text.as_str())),
        compression: keep(opts.map_or("", |o| o.compression_name())),
    };
    let chunks = info
        .chunks
        .iter()
        .map(|c| TFChunkInfo {
            chunk_id: c.id,
            ts_min_us: c.ts_min,
            ts_max_us: c.ts_max,
            rows: c.rows,
            series: c.series,
            data_bytes: c.data_bytes,
            compressed_bytes: c.file_bytes,
            sealed_at_us: c.sealed_at,
            encrypted: c.encrypted,
            status: keep(c.status.name()),
            compression: keep(Codec::name_of(c.codec)),
            file_name: keep(&c.file_name),
        })
        .collect();
    Ok(MooseDBInfo { table, chunks, _strings: strings })
}

/// Describes the table stored at `path` (open or not) for diagnostics.
///
/// # Safety
/// `path` must be NUL-terminated; `out_info` writable.
#[no_mangle]
pub unsafe extern "C" fn moosedb_inspect(path: *const c_char, out_info: *mut *mut MooseDBInfo) -> TFStatus {
    guard(|| {
        let out = handle_mut(out_info, "out_info")?;
        *out = ptr::null_mut();
        // SAFETY: caller contract.
        let p = unsafe { req_str(path, "path") }?;
        let info = moosedb_core::inspect::inspect(p.as_ref(), moosedb_core::time::now_micros())?;
        *out = Box::into_raw(Box::new(build_info(info)?));
        Ok(())
    })
}

/// # Safety
/// `info` must be valid. The result lives as long as `info`.
#[no_mangle]
pub unsafe extern "C" fn moosedb_info_table(info: *const MooseDBInfo) -> *const TFTableInfo {
    guard_value(ptr::null(), || handle_ref(info, "info").map_or(ptr::null(), |i| &i.table as *const TFTableInfo))
}

/// # Safety
/// `info` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_info_chunk_count(info: *const MooseDBInfo) -> u32 {
    guard_value(0, || handle_ref(info, "info").map_or(0, |i| i.chunks.len() as u32))
}

/// Chunk `index`, or NULL when out of range. The result lives as long as `info`.
///
/// # Safety
/// `info` must be valid.
#[no_mangle]
pub unsafe extern "C" fn moosedb_info_chunk(info: *const MooseDBInfo, index: u32) -> *const TFChunkInfo {
    guard_value(ptr::null(), || {
        handle_ref(info, "info")
            .ok()
            .and_then(|i| i.chunks.get(index as usize))
            .map_or(ptr::null(), |c| c as *const TFChunkInfo)
    })
}

/// # Safety
/// `info` must come from `moosedb_inspect` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn moosedb_info_close(info: *mut MooseDBInfo) {
    guard_value((), || release(info));
}

// ─── memory ──────────────────────────────────────────────────────────────────

/// Message of the last error raised on the calling thread, or NULL. The
/// caller owns the result and must release it with `moosedb_free_str`.
#[no_mangle]
pub extern "C" fn moosedb_last_error() -> *mut c_char {
    // On a panic (even `LAST_ERROR` access during thread teardown) report "no error".
    guard_value(ptr::null_mut(), || {
        LAST_ERROR
            .try_with(|e| e.borrow().as_ref().map_or(ptr::null_mut(), |c| c.clone().into_raw()))
            .unwrap_or(ptr::null_mut())
    })
}

/// Releases a string allocated by this library. NULL is ignored.
///
/// # Safety
/// `ptr` must be NULL or a pointer returned by this library, freed only once.
#[no_mangle]
pub unsafe extern "C" fn moosedb_free_str(ptr: *mut c_char) {
    guard_value((), || {
        if !ptr.is_null() {
            // SAFETY: produced by `CString::into_raw` in this library.
            drop(unsafe { CString::from_raw(ptr) });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crypto_errors_have_their_own_status() {
        assert_eq!(TFStatus::TF_ERR_CRYPTO as i32, 10);
        assert_eq!(status_of(&Error::Crypto("wrong key".into())), TFStatus::TF_ERR_CRYPTO);
        assert_eq!(status_of(&Error::Corrupt("x".into())), TFStatus::TF_ERR_CORRUPT);
        // Existing codes keep their values.
        assert_eq!(TFStatus::TF_ERR_UNSUPPORTED as i32, 9);
        assert_eq!(TFStatus::TF_ERR_CORRUPT as i32, 2);
    }

    #[test]
    fn scan_open_filtered_reports_bad_pointers_instead_of_crashing() {
        let mut out: *mut MooseDBScan = ptr::null_mut();
        // SAFETY: a NULL table and a NULL id list with a positive count are
        // rejected before anything is dereferenced.
        let st = unsafe { moosedb_scan_open_filtered(ptr::null_mut(), 0, 1, ptr::null(), 3, false, &mut out) };
        assert_ne!(st, TFStatus::TF_OK);
        assert!(out.is_null());
    }
}
