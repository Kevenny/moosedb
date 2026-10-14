//! Statement-level atomic batches.
//!
//! A [`Batch`] collects the rows of one statement privately. Nothing reaches
//! the shared MemTable until [`Batch::commit`], which publishes every row at
//! once under the table mutex: a snapshot taken before sees none of them, one
//! taken after sees all.
//!
//! Durability, two paths:
//!
//! * **WAL path** (small batches): every row is logged as a ROW entry tagged
//!   with the batch id, commit appends one COMMIT entry. Replay applies the
//!   rows only if it finds the COMMIT, so a crash before the commit leaves no
//!   row of the batch. While the batch is open and has logged rows, flushes
//!   keep `replay_seq` at or before the segment of its first row.
//! * **Spill path** (the buffer reached `memtable_size`): the buffered rows
//!   are written as *staged* chunks (`.tmp` → fsync → rename, ids reserved,
//!   one per time bucket) that the MANIFEST does not list, and the batch stops
//!   logging. Commit stages the remainder and swaps the MANIFEST once to list
//!   all of them — that swap is the commit point; no COMMIT is written, so
//!   replay drops the batch's WAL rows. Staged chunks of a batch that never
//!   commits are unlisted orphans: removed on abort, by recovery after a
//!   crash. Memory stays bounded by the spill limit whatever the batch size.
//!
//! A TRUNCATE invalidates open batches (their commit fails): otherwise the
//! rows logged before it could come back at replay.
//!
//! Memory and WAL retention are bounded on three levels:
//!
//! * per batch: spill at `MEMTABLE_SIZE` (rows *and* the series/tag
//!   bookkeeping of the batch are counted);
//! * process-wide: the buffers of all open batches together are counted in
//!   one atomic (`batch_memory_in_use`); above the configured budget
//!   (`settings::batch_memory_budget_bytes`) a batch that is at least
//!   `MIN_FORCED_SPILL` big spills at its next write, whatever its own limit.
//!   Small batches are left alone (a spill makes a chunk file; spilling a
//!   handful of rows would trade memory for file churn), so the worst case is
//!   the budget plus `MIN_FORCED_SPILL` per open batch;
//! * WAL retention: a batch that has logged rows pins every segment from its
//!   first one on. When a flush sees one pinning more than
//!   `MAX_RETAINED_SEGMENTS` it sets the batch's `spill_hint`; the batch spills
//!   at its next write (it stops logging and stops pinning). A batch that never
//!   writes again cannot be released from outside — its rows live in its own
//!   thread's buffer, and its COMMIT will refer to the old segments — so it
//!   keeps them until it commits or aborts (the warning is logged once per batch).
//!   Evicting it instead would fail a statement the binlog already recorded.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, MutexGuard};
use std::time::{Duration, Instant};

use crate::chunk::ChunkMeta;
use crate::chunk_writer::{chunks_needed, write_bucket, SeriesParts};
use crate::codec::encode_value;
use crate::compaction::split_series;
use crate::error::{corrupt, invalid, Error, Result};
use crate::fasthash::FastMap;
use crate::fsutil::{is_fsync_error, sync_dir};
use crate::index::series::row_tags;
use crate::log::warn;
use crate::memtable::{group_rows, row_bytes, MemRow};
use crate::scan::Snapshot;
use crate::schema::{Row, Schema, Value};
use crate::table::{Inner, OpenBatch, Table};
use crate::unique::{existing_extras, Claim, Extras, Plan, UniqueSpec};
use crate::wal;
use std::collections::HashMap;

/// Bytes held by the row buffers of every open batch of the process.
static BATCH_BYTES: AtomicU64 = AtomicU64::new(0);
/// A batch smaller than this is never spilled because of the global budget.
const MIN_FORCED_SPILL: usize = 1 << 20;
/// Most rows / series a new batch reserves up front from the previous one
/// (4096 rows x 40 B + 4096 series x ~50 B: well under a MiB per statement).
const BATCH_HINT_CAP: usize = 4096;

/// An empty vector with room for `n` items when `wanted`; an allocator that
/// refuses leaves it empty (normal growth), never aborts.
fn reserved<T>(n: usize, wanted: bool) -> Vec<T> {
    let mut v = Vec::new();
    if wanted && n >= 16 {
        let _ = v.try_reserve(n);
    }
    v
}

/// Bytes the open batches of this process hold in memory right now (rows plus
/// series bookkeeping): what `moosedb_batch_memory_budget` limits.
pub fn batch_memory_in_use() -> u64 {
    BATCH_BYTES.load(Ordering::Relaxed)
}

fn budget() -> u64 {
    crate::settings::get().batch_memory_budget_bytes()
}

/// Memory of one new series in a batch: the key in `local_ids`, its map
/// entry, and the tag values (held by the first row of the series).
fn series_cost<'a>(key_len: usize, tags: impl Iterator<Item = &'a Value>) -> usize {
    key_len
        + std::mem::size_of::<Vec<u8>>()
        + 16
        + std::mem::size_of::<Vec<Value>>()
        + tags.map(Value::mem_size).sum::<usize>()
}

pub struct Batch {
    table: Arc<Table>,
    id: u64,
    /// `Inner::epoch` when the batch began.
    epoch: u64,
    /// Buffered rows; `series_id` is an index into `local_first` until commit.
    rows: Vec<MemRow>,
    /// Sum of `row_bytes` over `rows` (what the MemTable will account).
    bytes: usize,
    /// Bytes of `local_ids` and the tag values of the first rows of the series.
    series_bytes: usize,
    /// What this batch has added to `BATCH_BYTES` and not yet given back.
    accounted: usize,
    /// Timestamp range of `rows` (the MemTable keeps it for estimates).
    ts_min: i64,
    ts_max: i64,
    /// Set by a flush when this batch pins too many WAL segments.
    spill_hint: Arc<AtomicBool>,
    local_ids: HashMap<Vec<u8>, u64>,
    /// Position in `rows` of the first row of each local series: its tag
    /// values are read from there when a series needs them (a new global
    /// series at commit, a spill), so a series costs one key allocation per
    /// statement, not a copy of its tags.
    local_first: Vec<usize>,
    /// Reused encoding buffers of `write` (series key, WAL payload).
    key_buf: Vec<u8>,
    wal_buf: Vec<u8>,
    /// Rows were logged to the WAL and the batch is registered as open there.
    logged: bool,
    /// The batch moved to the spill path: no more WAL logging.
    spilled: bool,
    /// Staged chunks, not listed in the MANIFEST.
    staged: Vec<Arc<ChunkMeta>>,
    spill_limit: usize,
    /// Commit reached its point of no return, or the batch was cleaned up.
    finished: bool,
    /// UNIQUE key checks on (the default); off = `unique_checks = 0`.
    unique_checks: bool,
    /// Series (global ids) this statement claimed and has not given back.
    claimed: Vec<u64>,
    /// Series written with `unique_checks = 0` (no claim): their key state is
    /// dropped too if the statement does not commit.
    unchecked: std::collections::HashSet<u64>,
    /// Global series id of each local id (UNIQUE tables only).
    local_gid: Vec<u64>,
    /// Keys of the rows now in `rows`, built when a lookup first needs them.
    own: Option<HashMap<(u64, i64), Vec<Extras>>>,
}

/// Half-open time interval `[start, end)` of a chunk.
type Bucket = (i64, i64);
/// A series of a bucket (local id) with its rows cut into column-block-sized parts.
type LocalParts<'a> = (u64, Vec<&'a [&'a Row]>);

fn stale() -> Error {
    invalid("the table was truncated while the batch was open")
}

fn duplicate() -> Error {
    Error::Duplicate("the UNIQUE key already exists".into())
}

/// Heap a key kept for the lookups of a batch is charged (map entry, list, key).
const OWN_KEY_COST: usize = 96;
/// How long one write waits for a series another statement holds before it
/// reports `Busy` (the caller then checks for kill and timeout and retries).
const CLAIM_WAIT_SLICE: Duration = Duration::from_millis(50);

impl Table {
    /// Starts a batch (one statement's worth of rows). See [`Batch`].
    pub fn begin_batch(self: &Arc<Self>) -> Result<Batch> {
        let mut guard = self.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        let id = inner.next_batch_id;
        inner.next_batch_id =
            id.checked_add(1).ok_or_else(|| corrupt("batch ids are exhausted (implausible id in the WAL)"))?;
        let epoch = inner.epoch;
        drop(guard);
        let spill_limit = usize::try_from(self.config.opts.memtable_size_bytes).unwrap_or(usize::MAX).max(1);
        // A statement like the last one: reserve its buffers now. The reserve is
        // small (a few hundred KiB at most, outside the batch budget by design)
        // and degrades to normal growth if the allocator refuses it.
        let hint = self.batch_hint.load(Ordering::Relaxed);
        let (hint_rows, hint_series) = ((hint >> 32) as usize, (hint & 0xFFFF_FFFF) as usize);
        let (hint_rows, hint_series) = (hint_rows.min(BATCH_HINT_CAP), hint_series.min(BATCH_HINT_CAP));
        let unique_hint = self.config.unique.is_some();
        let mut local_ids = HashMap::new();
        if hint_series >= 16 {
            let _ = local_ids.try_reserve(hint_series);
        }
        Ok(Batch {
            table: self.clone(),
            id,
            epoch,
            rows: reserved(hint_rows, true),
            bytes: 0,
            series_bytes: 0,
            accounted: 0,
            ts_min: i64::MAX,
            ts_max: i64::MIN,
            spill_hint: Arc::new(AtomicBool::new(false)),
            local_ids,
            local_first: reserved(hint_series, true),
            key_buf: Vec::new(),
            wal_buf: Vec::new(),
            logged: false,
            spilled: false,
            staged: Vec::new(),
            spill_limit,
            finished: false,
            unique_checks: true,
            claimed: reserved(hint_series, unique_hint),
            unchecked: std::collections::HashSet::new(),
            local_gid: reserved(hint_series, unique_hint),
            own: None,
        })
    }
}

/// How long [`Table::write_shared`] keeps retrying a series held by another
/// statement.
const WRITE_SHARED_WAIT: Duration = Duration::from_secs(50);

impl Table {
    /// [`Table::write`] that also works on tables with a UNIQUE key: the row is
    /// checked and stored as a one-row statement (the WAL is synced by the
    /// caller, as for `write`).
    pub fn write_shared(self: &Arc<Self>, row: Row) -> Result<()> {
        if self.config.unique.is_none() {
            return self.write(row);
        }
        let deadline = Instant::now() + WRITE_SHARED_WAIT;
        let mut b = self.begin_batch()?;
        loop {
            match b.write(row.clone()) {
                Ok(()) => break,
                Err(Error::Busy(_)) if Instant::now() < deadline => continue,
                Err(e) => return Err(e),
            }
        }
        b.commit(false)
    }
}

impl Batch {
    pub fn schema(&self) -> &Schema {
        &self.table.schema
    }

    /// Turns the `unique_checks` of this statement on or off (default on). Off,
    /// rows are not checked and claim nothing, but still update the key state.
    pub fn set_unique_checks(&mut self, on: bool) {
        self.unique_checks = on;
    }

    /// Buffers one row (and logs it unless the batch has spilled). The row is
    /// invisible to readers until `commit`.
    ///
    /// On a table with a UNIQUE key the key is checked first: `Duplicate` if it
    /// exists (committed, or earlier in this or another open statement), `Busy`
    /// if another statement holds the series (nothing changed, call again) and
    /// `Deadlock` if waiting would close a cycle. After any error the row is not
    /// part of the batch and the batch stays usable.
    pub fn write(&mut self, row: Row) -> Result<()> {
        if self.finished {
            return Err(invalid("the batch is already finished"));
        }
        let table = self.table.clone();
        table.schema.validate_for_write(&row)?;
        // Spill *before* taking the row: a failed spill then fails this row
        // alone instead of reporting an error for a row that was kept.
        if !self.rows.is_empty() && self.should_spill() {
            self.spill()?;
        }
        // The series key and the WAL payload are encoded into buffers the batch
        // keeps between rows (no allocation per row once they have grown).
        let mut key = std::mem::take(&mut self.key_buf);
        let mut wal_buf = std::mem::take(&mut self.wal_buf);
        let result = self.write_row(&table, row, &mut key, &mut wal_buf);
        self.key_buf = key;
        self.wal_buf = wal_buf;
        result
    }

    fn write_row(&mut self, table: &Arc<Table>, row: Row, key: &mut Vec<u8>, wal_buf: &mut Vec<u8>) -> Result<()> {
        let payload = if self.spilled {
            None
        } else {
            wal::row_entry_into(wal_buf, self.id, &row);
            // An oversized row is the caller's mistake, not a storage failure.
            if wal_buf.len() > wal::max_entry() {
                return Err(invalid(format!(
                    "row of {} bytes exceeds the {} byte limit",
                    wal_buf.len(),
                    wal::max_entry()
                )));
            }
            Some(&wal_buf[..])
        };
        key.clear();
        for &i in table.schema.tag_indices() {
            encode_value(key, &row[i]);
        }
        let ts = table.schema.row_ts(&row);
        let gid = match table.config.unique.as_ref() {
            Some(spec) => Some(self.admit(spec, key, &row, ts, payload)?),
            None => {
                if let Some(p) = payload {
                    let mut guard = table.lock()?;
                    let inner = &mut *guard;
                    inner.check_writable()?;
                    if inner.epoch != self.epoch {
                        return Err(stale());
                    }
                    self.log_row(inner, p)?;
                }
                None
            }
        };
        let rb = row_bytes(&row);
        let mut added = rb;
        let local = match self.local_ids.get(&key[..]) {
            Some(&l) => l,
            None => {
                let l = self.local_first.len() as u64;
                let tags = table.schema.tag_indices().iter().map(|&i| &row[i]);
                let cost = series_cost(key.len(), tags);
                self.series_bytes += cost;
                added += cost;
                self.local_first.push(self.rows.len());
                self.local_ids.insert(key.clone(), l);
                if let Some(g) = gid {
                    self.local_gid.push(g);
                }
                l
            }
        };
        if let (Some(g), Some(spec), Some(own)) = (gid, table.config.unique.as_ref(), self.own.as_mut()) {
            if let Some(x) = spec.extras_of(&row) {
                own.entry((g, ts)).or_default().push(x);
                added += OWN_KEY_COST;
                self.series_bytes += OWN_KEY_COST;
            }
        }
        self.bytes += rb;
        self.ts_min = self.ts_min.min(ts);
        self.ts_max = self.ts_max.max(ts);
        self.rows.push(MemRow { series_id: local, row });
        self.accounted += added;
        BATCH_BYTES.fetch_add(added as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Tag values of every local series, in local-id order (read from the
    /// first buffered row of each).
    fn local_tags(&self) -> Vec<Vec<Value>> {
        self.local_first.iter().map(|&i| row_tags(&self.table.schema, &self.rows[i].row)).collect()
    }

    /// Appends the row to the WAL and registers the batch as open there.
    fn log_row(&mut self, inner: &mut Inner, p: &[u8]) -> Result<()> {
        if let Err(e) = inner.wal.append(p) {
            if !matches!(e, Error::InvalidArg(_)) {
                inner.poison(format!("WAL append failed: {e}"));
            }
            return Err(e);
        }
        let seg = inner.wal.seq();
        let hint = self.spill_hint.clone();
        inner.open_batches.entry(self.id).or_insert_with(|| OpenBatch { seg, spill_hint: hint });
        self.logged = true;
        Ok(())
    }

    /// UNIQUE tables: claims the row's series, checks the key, logs the row and
    /// records the key. Returns the global id of the series.
    fn admit(&mut self, spec: &UniqueSpec, key: &[u8], row: &Row, ts: i64, payload: Option<&[u8]>) -> Result<u64> {
        let table = self.table.clone();
        let Some(ex) = spec.extras_of(row) else {
            return Err(invalid("a column of the UNIQUE key is NULL"));
        };
        let checks = self.unique_checks;
        let mut guard = table.lock()?;
        let mut lookup: Option<Arc<Snapshot>> = None;
        let sid;
        {
            let inner = &mut *guard;
            inner.check_writable()?;
            if inner.epoch != self.epoch {
                return Err(stale());
            }
            sid = inner.series.get_or_insert_keyed_with(key, || row_tags(&table.schema, row));
            if checks {
                match inner.uniq.claim(sid, self.id) {
                    Claim::Held { new } => {
                        if new {
                            self.claimed.push(sid);
                        }
                    }
                    Claim::Busy(owner) => return Err(self.wait_for_claim(guard, owner)),
                    Claim::Deadlock => {
                        return Err(Error::Deadlock(
                            "statements wait for each other's UNIQUE key series; the statement was stopped".into(),
                        ))
                    }
                }
                match inner.uniq.plan(sid, ts, &ex) {
                    Plan::Duplicate => return Err(duplicate()),
                    Plan::Accept => {}
                    Plan::Lookup => lookup = Some(table.snapshot_locked(inner)),
                }
            }
        }
        let mut existing: Option<Vec<Extras>> = None;
        if let Some(snap) = lookup {
            // The series is claimed: nobody else can add rows to it while the
            // chunks are read outside the table mutex.
            drop(guard);
            let found = self.find_existing(spec, &snap, sid, ts)?;
            if found.contains(&ex) {
                return Err(duplicate());
            }
            existing = Some(found);
            guard = table.lock()?;
            guard.check_writable()?;
            if guard.epoch != self.epoch {
                return Err(stale());
            }
        }
        let inner = &mut *guard;
        if let Some(p) = payload {
            self.log_row(inner, p)?;
        }
        inner.uniq.accept(sid, ts, ex, existing.as_deref());
        if !checks {
            self.unchecked.insert(sid);
        }
        Ok(sid)
    }

    /// Waits (a short slice) for the statement that holds a series; the caller
    /// retries the write on `Busy`.
    fn wait_for_claim(&self, mut guard: MutexGuard<'_, Inner>, owner: u64) -> Error {
        guard.uniq.set_waiting(self.id, owner);
        match self.table.claims_cv.wait_timeout(guard, CLAIM_WAIT_SLICE) {
            Ok((mut g, _)) => {
                g.uniq.clear_waiting(self.id);
                Error::Busy("another statement is inserting into the same series".into())
            }
            Err(_) => Error::ReadOnly("table must be reopened: a thread panicked while holding the table state".into()),
        }
    }

    /// Extras tuples stored for `(sid, ts)`: committed data, chunks staged by
    /// this batch, and the rows still buffered in it.
    fn find_existing(&mut self, spec: &UniqueSpec, snap: &Snapshot, sid: u64, ts: i64) -> Result<Vec<Extras>> {
        let table = self.table.clone();
        let mut out = existing_extras(&table.schema, spec, snap, &self.staged, sid, ts)?;
        if self.own.is_none() {
            let mut m: HashMap<(u64, i64), Vec<Extras>> = HashMap::new();
            for r in &self.rows {
                let gid = usize::try_from(r.series_id).ok().and_then(|l| self.local_gid.get(l)).copied();
                if let (Some(g), Some(x)) = (gid, spec.extras_of(&r.row)) {
                    m.entry((g, table.schema.row_ts(&r.row))).or_default().push(x);
                }
            }
            let cost = self.rows.len() * OWN_KEY_COST;
            self.series_bytes += cost;
            self.accounted += cost;
            BATCH_BYTES.fetch_add(cost as u64, Ordering::Relaxed);
            self.own = Some(m);
        }
        if let Some(v) = self.own.as_ref().and_then(|m| m.get(&(sid, ts))) {
            out.extend_from_slice(v);
        }
        Ok(out)
    }

    /// Gives back the claims after the rows became visible (table mutex held).
    fn release_claims_locked(&mut self, inner: &mut Inner) {
        self.unchecked.clear();
        if !self.claimed.is_empty() {
            inner.uniq.release(&self.claimed, self.id, false);
            self.claimed.clear();
            self.table.claims_cv.notify_all();
        }
    }

    /// Gives back the claims; with `invalidate` the key state of those series is
    /// dropped too (the rows of this statement may not survive).
    fn release_claims(&mut self, invalidate: bool) {
        if self.claimed.is_empty() && self.unchecked.is_empty() {
            return;
        }
        if let Ok(mut g) = self.table.lock() {
            g.uniq.release(&self.claimed, self.id, invalidate);
            if invalidate {
                for sid in &self.unchecked {
                    g.uniq.invalidate(*sid);
                }
            }
        }
        self.unchecked.clear();
        self.table.claims_cv.notify_all();
        self.claimed.clear();
    }

    /// Whether the next write must first move the buffer to staged chunks.
    fn should_spill(&self) -> bool {
        let local = self.bytes + self.series_bytes;
        if local >= self.spill_limit {
            return true;
        }
        // Pinning too many WAL segments (only a batch that still logs pins any).
        if !self.spilled && self.spill_hint.load(Ordering::Relaxed) {
            return true;
        }
        local >= MIN_FORCED_SPILL && BATCH_BYTES.load(Ordering::Relaxed) > budget()
    }

    /// Gives this batch's share back to the process-wide counter.
    fn release_budget(&mut self) {
        if self.accounted > 0 {
            BATCH_BYTES.fetch_sub(self.accounted as u64, Ordering::Relaxed);
            self.accounted = 0;
        }
    }

    /// Writes the buffered rows as staged chunks and drops them from memory.
    fn spill(&mut self) -> Result<()> {
        let local_tags = self.local_tags();
        let metas = stage_rows(&self.table, &self.rows, &local_tags, self.epoch)?;
        self.staged.extend(metas.into_iter().map(Arc::new));
        self.rows = Vec::new();
        self.bytes = 0;
        self.series_bytes = 0;
        self.ts_min = i64::MAX;
        self.ts_max = i64::MIN;
        self.release_budget();
        self.local_ids = HashMap::new();
        self.local_first = Vec::new();
        self.local_gid = Vec::new();
        self.own = None;
        if !self.spilled {
            self.spilled = true;
            if self.logged {
                // The rows logged so far will never be committed through the
                // WAL, so they no longer pin the replay start.
                if let Ok(mut g) = self.table.lock() {
                    g.open_batches.remove(&self.id);
                }
            }
        }
        Ok(())
    }

    /// Publishes every row of the batch at once. With `sync` the commit is
    /// durable on return (WAL fsync, or — spill path — the MANIFEST swap,
    /// which is always fsynced). The batch is consumed, also on error.
    pub fn commit(mut self, sync: bool) -> Result<()> {
        self.commit_inner(sync)
    }

    fn commit_inner(&mut self, sync: bool) -> Result<()> {
        let table = self.table.clone();
        if !self.spilled {
            if self.rows.is_empty() {
                self.cleanup();
                return Ok(());
            }
            if !self.logged {
                return Err(invalid("internal error: batch rows were not logged"));
            }
            let limit = usize::try_from(table.config.opts.memtable_size_bytes).unwrap_or(usize::MAX);
            let mut guard = table.lock()?;
            let inner = &mut *guard;
            inner.check_writable()?;
            if inner.epoch != self.epoch {
                return Err(stale());
            }
            if inner.memtable.bytes() >= limit.saturating_mul(2) {
                // Flushes have been failing; refuse rather than grow without bound.
                table
                    .flush_locked(inner)
                    .map_err(|e| Error::Full(format!("MemTable is full and flushing failed: {e}")))?;
            }
            if let Err(e) = inner.wal.append(&wal::commit_entry(self.id)) {
                if !matches!(e, Error::InvalidArg(_)) {
                    inner.poison(format!("WAL append failed: {e}"));
                }
                return Err(e);
            }
            // The COMMIT is in the log buffer. Make it reach the OS/disk before
            // publishing, all under the table mutex (no flush can slip in between):
            // if the sync fails the statement errors and no reader ever sees the rows.
            if let Err(e) = inner.wal.sync(sync) {
                inner.poison(format!("WAL sync failed: {e}"));
                return Err(e);
            }
            self.finished = true;
            inner.open_batches.remove(&self.id);
            // Global id of each local series. A UNIQUE table already resolved
            // them while admitting the rows (same epoch, checked above).
            let ids: Vec<u64> = if self.local_gid.len() == self.local_first.len() {
                std::mem::take(&mut self.local_gid)
            } else {
                // Arrival order, so ids are assigned as they always were.
                let mut ids = Vec::with_capacity(self.local_first.len());
                let mut key = std::mem::take(&mut self.key_buf);
                for &first in &self.local_first {
                    let row = &self.rows[first].row;
                    key.clear();
                    for &i in table.schema.tag_indices() {
                        encode_value(&mut key, &row[i]);
                    }
                    ids.push(inner.series.get_or_insert_keyed_with(&key, || row_tags(&table.schema, row)));
                }
                self.key_buf = key;
                ids
            };
            let hint =
                ((self.rows.len() as u64).min(u64::from(u32::MAX)) << 32) | (ids.len() as u64).min(u64::from(u32::MAX));
            table.batch_hint.store(hint, Ordering::Relaxed);
            let mut rows = std::mem::take(&mut self.rows);
            for r in &mut rows {
                r.series_id = ids[r.series_id as usize];
            }
            inner.memtable.append(rows, self.bytes, self.ts_min, self.ts_max);
            // The rows are visible: the series it held can be written by others.
            self.release_claims_locked(inner);
            // The rows now belong to the MemTable, which has its own limits.
            self.release_budget();
            inner.changed();
            if inner.memtable.bytes() >= limit {
                // The rows are safe in the WAL: a failed flush is retried later.
                if let Err(e) = table.flush_locked(inner) {
                    warn(&format!("MemTable flush failed, will retry: {e}"));
                }
            }
            return Ok(());
        }

        if !self.rows.is_empty() {
            self.spill()?;
        }
        if self.staged.is_empty() {
            self.cleanup();
            return Ok(());
        }
        let mut guard = table.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        if inner.epoch != self.epoch {
            return Err(stale());
        }
        let mut manifest = inner.manifest.clone();
        manifest.chunks.extend(self.staged.iter().map(|m| m.id));
        manifest.chunks.sort_unstable();
        // The files now belong to this MANIFEST swap. If it fails we cannot
        // know whether it reached the disk (the table is poisoned), so they
        // are neither listed in memory nor deleted: recovery decides.
        let staged = std::mem::take(&mut self.staged);
        self.finished = true;
        inner.commit_manifest(&table.config.dir, manifest)?;
        let mut chunks = inner.chunks.clone();
        chunks.extend(staged);
        inner.replace_chunks(chunks);
        self.release_claims_locked(inner);
        // The rows this batch logged before spilling are dead weight in the
        // WAL: collect them now, unless another batch still needs those
        // segments (the check is inside).
        if let Err(e) = table.collect_wal_locked(inner) {
            warn(&format!("cannot collect WAL segments after a batch commit: {e}"));
        }
        Ok(())
    }

    /// Discards the batch: its rows never become visible. (Dropping the batch
    /// without committing does the same.)
    pub fn abort(self) {}

    /// Releases everything an unfinished batch holds.
    fn cleanup(&mut self) {
        self.finished = true;
        for m in &self.staged {
            m.file.mark_obsolete();
        }
        self.staged.clear();
        self.rows = Vec::new();
        self.release_budget();
        self.release_claims(true);
        if self.logged {
            if let Ok(mut g) = self.table.lock() {
                g.open_batches.remove(&self.id);
            }
        }
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        if !self.finished {
            self.cleanup();
        }
        // A commit that failed before publishing leaves its share behind.
        self.release_budget();
        // Claims a finished-but-failed commit still holds.
        self.release_claims(true);
    }
}

/// Writes `rows` (whose `series_id` indexes `local_tags`) as staged chunks,
/// one per time bucket. The files are fsynced and renamed into place but not
/// listed in the MANIFEST. Memory: the chunk being built plus the compressed
/// output of the others — never a copy of all the rows.
fn stage_rows(table: &Table, rows: &[MemRow], local_tags: &[Vec<Value>], epoch: u64) -> Result<Vec<ChunkMeta>> {
    let opts = &table.config.opts;
    let dir = &table.config.dir;
    let groups = group_rows(rows.iter(), &table.schema, |ts| opts.chunk_interval.bucket(ts));
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    // A series too big for one column block is cut into parts; part k of every
    // series goes to chunk k of its bucket (as flushes and compaction do).
    let split: Vec<(Bucket, Vec<LocalParts>)> = groups
        .iter()
        .map(|(bucket, series)| (*bucket, series.iter().map(|(&local, rows)| (local, split_series(rows))).collect()))
        .collect();
    let total: u64 = split.iter().map(|(_, s)| s.iter().map(|(_, parts)| parts.len()).max().unwrap_or(0) as u64).sum();
    // Series ids and chunk ids come from shared state: take them in one short
    // critical section. (Series registered for a batch that later aborts stay
    // in the index, without rows; harmless, and gone after a reopen.)
    let (global, first_id, wal_seq) = {
        let mut guard = table.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        if inner.epoch != epoch {
            return Err(stale());
        }
        let mut global: FastMap<u64, u64> = FastMap::default();
        for series in groups.values() {
            for &local in series.keys() {
                global.entry(local).or_insert_with(|| inner.series.get_or_insert(&local_tags[local as usize]));
            }
        }
        let first = inner.reserve_chunk_ids(total)?;
        (global, first, inner.wal.seq())
    };

    let now = crate::time::now_micros();
    let mut written: Vec<ChunkMeta> = Vec::new();
    let result = (|| -> Result<()> {
        let mut next = first_id;
        for (bucket, series) in split {
            let mut parts: Vec<SeriesParts<'_>> = series
                .into_iter()
                .map(|(local, parts)| SeriesParts {
                    series_id: global[&local],
                    tags: &local_tags[local as usize],
                    parts,
                })
                .collect();
            parts.sort_unstable_by_key(|p| p.series_id);
            let codec = opts.codec_for(bucket.1, now);
            write_bucket(
                &table.schema,
                codec,
                opts.encryption_key_id,
                dir,
                bucket,
                &parts,
                next,
                wal_seq,
                &mut written,
            )?;
            next += chunks_needed(&parts) as u64;
        }
        sync_dir(dir)
    })();
    if let Err(e) = result {
        for m in &written {
            m.file.mark_obsolete();
        }
        if is_fsync_error(&e) {
            if let Ok(mut g) = table.lock() {
                g.poison(format!("fsync of a staged chunk failed: {e}"));
            }
        }
        return Err(e);
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsutil::{fail_fsync_after, restore_fsync};
    use crate::options::{RawOptions, TableConfig};
    use crate::schema::{Column, ColumnType};

    fn config(dir: &std::path::Path) -> TableConfig {
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
            ],
            0,
        )
        .unwrap();
        TableConfig::new(dir, schema, &RawOptions::default()).unwrap()
    }

    fn open(dir: &std::path::Path) -> Arc<Table> {
        let cfg = config(dir);
        Table::create(&cfg).unwrap();
        Arc::new(Table::open(cfg).unwrap())
    }

    fn segments(dir: &std::path::Path) -> usize {
        wal::list_segments(dir).unwrap().len()
    }

    /// A continuous writer: every flush starts a new WAL segment.
    fn churn(t: &Table, from: i64, n: usize) {
        for i in 0..n {
            t.write(row(from + i as i64)).unwrap();
            t.flush().unwrap();
        }
    }

    const LAG: usize = crate::table::MAX_RETAINED_SEGMENTS as usize + 6;

    #[test]
    fn a_batch_pinning_too_many_wal_segments_spills_at_its_next_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = open(&path);
        let mut slow = t.begin_batch().unwrap();
        slow.write(row(0)).unwrap();
        t.sync_wal(false).unwrap();
        churn(&t, 1000, LAG);
        assert!(segments(&path) > LAG - 3, "the pin keeps every segment: {}", segments(&path));
        assert!(slow.spill_hint.load(Ordering::Relaxed), "the flush asked the batch to spill");
        assert!(!slow.spilled);

        // Its next write moves the buffer into staged chunks: it stops logging
        // and stops pinning, and the next flush collects the segments.
        slow.write(row(1)).unwrap();
        assert!(slow.spilled);
        churn(&t, 5000, 1);
        assert!(segments(&path) <= 2, "{} segments left", segments(&path));

        // Atomicity is untouched: invisible until commit, then whole.
        assert_eq!(t.stats().unwrap().row_count as usize, LAG + 1);
        slow.write(row(2)).unwrap();
        slow.commit(true).unwrap();
        assert_eq!(t.stats().unwrap().row_count as usize, LAG + 1 + 3);
        drop(t);
        let t = Table::open(config(&path)).unwrap();
        assert_eq!(t.stats().unwrap().row_count as usize, LAG + 1 + 3, "durable across a reopen");
    }

    #[test]
    fn a_batch_that_never_writes_again_releases_the_segments_when_it_commits_or_aborts() {
        for commit in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("t");
            let t = open(&path);
            let mut idle = t.begin_batch().unwrap();
            idle.write(row(0)).unwrap();
            churn(&t, 1000, LAG);
            assert!(idle.spill_hint.load(Ordering::Relaxed));
            let pinned = segments(&path);
            assert!(pinned > LAG - 3, "an idle batch cannot be released from outside: {pinned}");
            // The flush keeps flagging it but cannot evict it: its rows live in
            // its own buffer and its COMMIT will refer to the old segments.
            churn(&t, 3000, 3);
            assert!(segments(&path) > pinned);
            if commit {
                idle.commit(true).unwrap();
            } else {
                idle.abort();
            }
            churn(&t, 6000, 1);
            assert!(segments(&path) <= 2, "{} segments left after commit={commit}", segments(&path));
            let want = LAG + 3 + 1 + usize::from(commit);
            assert_eq!(t.stats().unwrap().row_count as usize, want);
            drop(t);
            assert_eq!(Table::open(config(&path)).unwrap().stats().unwrap().row_count as usize, want);
        }
    }

    #[test]
    fn a_forced_spill_that_never_commits_leaves_nothing_after_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = open(&path);
        let mut slow = t.begin_batch().unwrap();
        slow.write(row(0)).unwrap();
        churn(&t, 1000, LAG);
        slow.write(row(1)).unwrap(); // spills
        slow.write(row(2)).unwrap();
        t.sync_wal(true).unwrap();
        std::mem::forget(slow);
        std::mem::forget(t);
        let t = Table::open(config(&path)).unwrap();
        assert_eq!(t.stats().unwrap().row_count as usize, LAG, "only the committed rows of the other writer");
        assert!(t.check().unwrap().is_empty());
    }

    #[test]
    fn series_bookkeeping_counts_toward_the_batch_size() {
        let dir = tempfile::tempdir().unwrap();
        let t = open(&dir.path().join("t"));
        let mut b = t.begin_batch().unwrap();
        b.write(vec![Value::Timestamp(1), Value::Bytes(b"a-very-long-host-name-for-one-series".to_vec())]).unwrap();
        let one = b.bytes + b.series_bytes;
        assert!(b.series_bytes > 36, "key and tags of a new series are charged: {}", b.series_bytes);
        b.write(vec![Value::Timestamp(2), Value::Bytes(b"a-very-long-host-name-for-one-series".to_vec())]).unwrap();
        let two = b.bytes + b.series_bytes;
        assert!(two - one < one, "a known series costs only its row");
        assert_eq!(b.accounted, two);
        b.abort();
    }

    fn row(i: i64) -> Row {
        vec![Value::Timestamp(i), Value::Bytes(b"h".to_vec())]
    }

    #[test]
    fn commit_fsync_failure_poisons_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let t = open(&dir.path().join("t"));
        let mut b = t.begin_batch().unwrap();
        b.write(row(1)).unwrap();
        fail_fsync_after(0);
        let r = b.commit(true);
        restore_fsync();
        assert!(r.is_err());
        assert_eq!(t.stats().unwrap().row_count, 0, "rows of a failed commit are never visible");
        assert!(matches!(t.begin_batch(), Err(Error::ReadOnly(_))));
        assert!(matches!(t.write(row(2)), Err(Error::ReadOnly(_))));
    }

    #[test]
    fn oversized_batch_row_is_rejected_without_poisoning() {
        let dir = tempfile::tempdir().unwrap();
        let t = open(&dir.path().join("t"));
        let mut b = t.begin_batch().unwrap();
        b.write(row(1)).unwrap();
        wal::set_max_entry(16);
        let err = b.write(vec![Value::Timestamp(2), Value::Bytes(vec![b'x'; 100])]).unwrap_err();
        wal::set_max_entry(1 << 30);
        assert!(matches!(err, Error::InvalidArg(_)));
        b.write(row(3)).unwrap();
        b.commit(true).unwrap();
        assert_eq!(t.stats().unwrap().row_count, 2);
    }

    #[test]
    fn empty_batch_commit_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let t = open(&dir.path().join("t"));
        t.begin_batch().unwrap().commit(true).unwrap();
        assert_eq!(t.stats().unwrap().row_count, 0);
    }
}
