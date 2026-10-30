//! A MooseDB table: one directory holding the MANIFEST, OPTIONS, WAL
//! segments and chunks.
//!
//! Mutable state lives behind one mutex (`inner`). Readers only hold it
//! while taking a [`Snapshot`]; maintenance (compaction, retention) does its
//! heavy work outside it and takes it briefly to commit. Maintenance jobs are
//! serialized per table by `maint`.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::bytes::ByteReader;
use crate::chunk::{parse_chunk_id, ChunkMeta};
use crate::chunk_reader::{read_meta, verify_file};
use crate::chunk_writer::{chunks_needed, write_bucket, SeriesParts};
use crate::codec::decode_row;
use crate::compaction::split_series;
use crate::crypto::CipherParams;
use crate::error::{corrupt, invalid, Error, Result};
use crate::fsutil::{is_fsync_error, remove_if_exists, sync_dir, write_atomic};
use crate::index::series::{row_tags, SeriesIndex, SeriesSnapshot};
use crate::log::warn;
use crate::manifest::Manifest;
use crate::memtable::{MemRow, MemSnapshot, MemTable};
use crate::options::TableConfig;
use crate::scan::{check_tag_columns, Position, ResolvedFilter, Scan, ScanFilter, Snapshot};
use crate::schema::{Row, Schema, Value};
use crate::tomb::TombSet;
use crate::unique::UniqueState;
use crate::wal::{self, list_segments, segment_path, Wal};

pub(crate) const OPTIONS_FILE: &str = "OPTIONS";
/// An open batch that keeps more WAL segments than this alive is asked to
/// spill at its next write (see `Inner::flag_laggards`).
pub(crate) const MAX_RETAINED_SEGMENTS: u64 = 64;
/// Largest value WAL replay puts in the MemTable. Older versions accepted
/// values up to 1 GiB; a flush cannot store one above half the block limit
/// (it would fail forever and make the table unusable), so replay sets such a
/// row aside instead (see `Table::open`). Writes are held to the much lower
/// `schema::MAX_VALUE_BYTES`.
const MAX_REPLAY_VALUE_BYTES: usize = crate::compression::MAX_BLOCK_RAW / 2;
#[cfg(test)]
thread_local! {
    static REPLAY_VALUE_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(MAX_REPLAY_VALUE_BYTES) };
}

/// Largest value replay keeps (lowerable per thread in tests).
fn max_replay_value() -> usize {
    #[cfg(test)]
    return REPLAY_VALUE_LIMIT.with(std::cell::Cell::get);
    #[cfg(not(test))]
    MAX_REPLAY_VALUE_BYTES
}

/// Suffix of the copies of WAL segments kept when replay set rows aside.
const OVERSIZED_SUFFIX: &str = ".oversized";

/// Copies a WAL segment to `dst` crash-safely and returns its size: write
/// `dst.tmp`, fsync, rename. A `dst` left by an earlier run is trusted only if
/// it is byte-identical to `src` (size and CRC); otherwise it is replaced.
/// The caller syncs the directory.
fn preserve_segment(src: &Path, dst: &Path) -> Result<u64> {
    use std::io::Read;
    fn crc(path: &Path) -> Result<(u64, u32)> {
        let mut f = fs::File::open(path)?;
        let (mut h, mut n, mut buf) = (crc32fast::Hasher::new(), 0u64, vec![0u8; 1 << 20]);
        loop {
            let k = f.read(&mut buf)?;
            if k == 0 {
                return Ok((n, h.finalize()));
            }
            h.update(&buf[..k]);
            n += k as u64;
        }
    }
    let want = crc(src)?;
    if dst.exists() && crc(dst)? == want {
        return Ok(want.0);
    }
    let mut tmp = dst.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let copied = (|| -> Result<()> {
        let mut out = fs::File::create(&tmp)?;
        let mut input = fs::File::open(src)?;
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let k = input.read(&mut buf)?;
            if k == 0 {
                break;
            }
            std::io::Write::write_all(&mut out, &buf[..k])?;
            #[cfg(test)]
            if COPY_FAILS_MIDWAY.with(std::cell::Cell::get) {
                return Err(Error::Io(std::io::Error::other("injected failure in the middle of the copy")));
            }
        }
        crate::fsutil::sync_all(&out)?;
        drop(out);
        fs::rename(&tmp, dst)?;
        Ok(())
    })();
    if let Err(e) = copied {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(want.0)
}

#[cfg(test)]
thread_local! {
    static COPY_FAILS_MIDWAY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The sequence number after `seq`; running out means forged metadata.
fn succ(seq: u64) -> Result<u64> {
    seq.checked_add(1)
        .ok_or_else(|| corrupt("WAL segment numbers are exhausted (implausible sequence in the table files)"))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableStats {
    pub row_count: u64,
    /// Size of the data as plain arrays (chunks + MemTable estimate).
    pub data_bytes: u64,
    /// Bytes on disk used by chunk files.
    pub compressed_bytes: u64,
    pub chunk_count: u32,
    pub memtable_rows: u64,
    pub series_count: u64,
    /// Estimated heap bytes of the series index (tag values, keys, maps).
    pub series_bytes: u64,
    /// Bytes charged to the UNIQUE-key state (bounded, see `unique.rs`).
    pub unique_bytes: u64,
}

pub(crate) struct Inner {
    pub(crate) manifest: Manifest,
    /// Live chunks, ascending id.
    pub(crate) chunks: Vec<Arc<ChunkMeta>>,
    pub(crate) wal: Wal,
    pub(crate) memtable: MemTable,
    /// Rows a flush is sealing into chunks outside the mutex (see
    /// `Table::begin_flush`). Still visible: a snapshot shows them before the
    /// MemTable's rows. Empty unless `flush_running`.
    pub(crate) flushing: MemTable,
    /// A flush took `flushing` and has not installed its chunks (or given the
    /// rows back) yet. At most one at a time; `Table::flush_cv` announces its end.
    pub(crate) flush_running: bool,
    pub(crate) series: SeriesIndex,
    /// Bumped whenever MemTable rows move into chunks or vanish.
    pub(crate) mem_generation: u64,
    /// Bumped on every change of the table's contents.
    version: u64,
    snapshot_cache: Option<Arc<Snapshot>>,
    /// Chunks currently being merged (diagnostics).
    pub(crate) compacting: HashSet<u64>,
    /// Chunk-id sets (sorted) whose compaction failed or only split data again;
    /// background passes skip them until the set of chunks changes.
    pub(crate) no_gain: HashSet<Vec<u64>>,
    /// Set when a failure left disk and memory possibly inconsistent; the
    /// table then refuses writes until reopened (which runs recovery).
    poisoned: Option<String>,
    /// Next batch id; larger than any id found in the WAL at open.
    pub(crate) next_batch_id: u64,
    /// Open batches that logged rows to the WAL (and will commit through it),
    /// by batch id. The checkpoint can never move replay past the oldest
    /// first-entry segment among them.
    pub(crate) open_batches: HashMap<u64, OpenBatch>,
    /// Bumped by TRUNCATE; a batch begun earlier can no longer commit.
    pub(crate) epoch: u64,
    /// UNIQUE key state (empty and unused when the table declares none).
    pub(crate) uniq: UniqueState,
    /// Deletes in force against the chunks (see `tomb`). Shared with snapshots;
    /// changed copy-on-write.
    pub(crate) tombs: Arc<TombSet>,
    /// Highest tombstone sequence number assigned so far.
    pub(crate) tseq: u64,
    /// Bumped whenever the set of live chunks changes.
    pub(crate) layout_gen: u64,
}

/// A batch that has logged rows to the WAL and has not finished.
pub(crate) struct OpenBatch {
    /// Segment of its first logged entry.
    pub(crate) seg: u64,
    /// Set by a flush when this batch pins too many segments: the batch
    /// spills (and stops pinning) on its next write instead of logging on.
    pub(crate) spill_hint: Arc<AtomicBool>,
}

impl Inner {
    /// Oldest segment an open batch still needs, if any batch logged rows.
    pub(crate) fn oldest_pin(&self) -> Option<u64> {
        self.open_batches.values().map(|b| b.seg).min()
    }

    /// Asks every open batch that keeps more than `MAX_RETAINED_SEGMENTS`
    /// segments (counted up to `current`, the segment of the checkpoint)
    /// to spill at its next write. A batch cannot be released from outside:
    /// its rows are private to the statement's thread, and its COMMIT will
    /// refer to rows in the old segments, which therefore have to stay until
    /// the batch spills (its rows then go to chunks), commits or aborts.
    pub(crate) fn flag_laggards(&self, dir: &Path, current: u64) {
        for (id, b) in &self.open_batches {
            let lag = current.saturating_sub(b.seg);
            if lag > MAX_RETAINED_SEGMENTS && !b.spill_hint.swap(true, Ordering::Relaxed) {
                warn(&format!(
                    "{}: batch {id} keeps {lag} WAL segments alive (oldest needed: {}, current: {current}); \
                     it is asked to spill at its next write",
                    dir.display(),
                    b.seg
                ));
            }
        }
    }

    /// Rows not yet in chunks: the MemTable and the ones being flushed.
    pub(crate) fn mem_len(&self) -> usize {
        self.memtable.len() + self.flushing.len()
    }

    pub(crate) fn mem_bytes(&self) -> usize {
        self.memtable.bytes() + self.flushing.bytes()
    }

    /// [`MemTable::visit_range`] over the rows being flushed and the MemTable.
    pub(crate) fn visit_mem_range(&self, schema: &Schema, lo: i64, hi: i64, mut visit: impl FnMut(&MemRow)) {
        self.flushing.visit_range(schema, lo, hi, &mut visit);
        self.memtable.visit_range(schema, lo, hi, visit);
    }

    pub(crate) fn check_writable(&self) -> Result<()> {
        match &self.poisoned {
            Some(why) => Err(Error::ReadOnly(format!("table must be reopened after an earlier failure: {why}"))),
            None => Ok(()),
        }
    }

    pub(crate) fn poison(&mut self, why: String) {
        // Keep the first cause: it is the one that explains the rest.
        if self.poisoned.is_none() {
            warn(&format!("table switched to read-only: {why}"));
            self.poisoned = Some(why);
        }
    }

    /// Reserves `n` consecutive chunk ids and returns the first.
    pub(crate) fn reserve_chunk_ids(&mut self, n: u64) -> Result<u64> {
        let first = self.manifest.next_chunk_id;
        match first.checked_add(n) {
            Some(next) => {
                self.manifest.next_chunk_id = next;
                Ok(first)
            }
            None => Err(corrupt("chunk ids are exhausted (the MANIFEST holds an implausible next_chunk_id)")),
        }
    }

    pub(crate) fn changed(&mut self) {
        self.version += 1;
        self.snapshot_cache = None;
    }

    /// Publishes `manifest`. If the store fails we cannot know whether the
    /// rename reached the disk, so the table is poisoned; reopening resolves
    /// the ambiguity through recovery.
    pub(crate) fn commit_manifest(&mut self, dir: &Path, manifest: Manifest) -> Result<()> {
        // Sealing can fail (key provider) before anything touches the disk: that
        // is an ordinary error, not an ambiguous commit, so it does not poison.
        let bytes = manifest.encode()?;
        if let Err(e) = write_atomic(&dir.join(crate::manifest::MANIFEST_FILE), &bytes) {
            self.poison(format!("MANIFEST update failed: {e}"));
            return Err(e);
        }
        self.manifest = manifest;
        Ok(())
    }

    /// Replaces the live chunk list after a committed MANIFEST change; chunks
    /// that left the set are deleted once no snapshot references them.
    pub(crate) fn replace_chunks(&mut self, mut chunks: Vec<Arc<ChunkMeta>>) {
        chunks.sort_by_key(|c| c.id);
        let keep: HashSet<u64> = chunks.iter().map(|c| c.id).collect();
        for c in &self.chunks {
            if !keep.contains(&c.id) {
                c.file.mark_obsolete();
            }
        }
        self.chunks = chunks;
        self.layout_gen += 1;
        self.changed();
    }

    /// The MANIFEST describing the state in memory, with `chunks` and `tombs`
    /// as the live chunks and deletes. Every MANIFEST the table writes starts
    /// from here: whichever change it makes, it must carry the tombstones, or
    /// a WAL checkpoint that retires their DELETE entries would lose them.
    pub(crate) fn manifest_for(&self, chunks: &[Arc<ChunkMeta>], tombs: &TombSet) -> Manifest {
        build_manifest(&self.manifest, chunks, tombs, self.tseq)
    }

    /// [`Inner::manifest_for`] the current chunks and tombstones.
    pub(crate) fn manifest_now(&self) -> Manifest {
        self.manifest_for(&self.chunks, &self.tombs)
    }

    /// Commits a new chunk set and tombstone set: `tune` adjusts the MANIFEST
    /// (checkpoint, next id), which is then switched atomically (the commit
    /// point), and only then do memory and the obsolete chunks follow. A
    /// failure to store poisons the table, as in `commit_manifest`.
    pub(crate) fn publish(
        &mut self,
        dir: &Path,
        mut chunks: Vec<Arc<ChunkMeta>>,
        mut tombs: TombSet,
        tune: impl FnOnce(&mut Manifest),
    ) -> Result<()> {
        // Chunks created now (flush, staged by a statement, compaction output
        // whose caller did not choose) reflect every tombstone assigned so far.
        for c in &mut chunks {
            if c.born == u64::MAX {
                Arc::make_mut(c).born = self.tseq;
            }
        }
        let keep: HashSet<u64> = chunks.iter().map(|c| c.id).collect();
        let gone: HashSet<u64> = self.chunks.iter().map(|c| c.id).filter(|id| !keep.contains(id)).collect();
        tombs.forget_chunks(&gone);
        tombs.prune(&chunks);
        let mut manifest = self.manifest_for(&chunks, &tombs);
        tune(&mut manifest);
        self.commit_manifest(dir, manifest)?;
        self.tombs = Arc::new(tombs);
        self.replace_chunks(chunks);
        Ok(())
    }

    /// Starts a fresh WAL segment after a checkpoint. Writing to the old
    /// segment would be unsafe (recovery discards it), hence poison on failure.
    fn rotate_wal(&mut self, dir: &Path, key: Option<u32>) -> Result<()> {
        let created = succ(self.manifest.wal_seq).and_then(|seq| Wal::create(dir, seq, key));
        match created {
            Ok(w) => {
                self.wal = w;
                Ok(())
            }
            Err(e) => {
                self.poison(format!("cannot create WAL segment: {e}"));
                Err(e)
            }
        }
    }
}

/// `base` with the chunk list (and each chunk's `born`), the tombstones and
/// their highest sequence number replaced by the given state.
pub(crate) fn build_manifest(base: &Manifest, chunks: &[Arc<ChunkMeta>], tombs: &TombSet, tseq: u64) -> Manifest {
    let mut m = base.clone();
    let mut sorted: Vec<&Arc<ChunkMeta>> = chunks.iter().collect();
    sorted.sort_by_key(|c| c.id);
    m.chunks = sorted.iter().map(|c| c.id).collect();
    m.born = sorted.iter().map(|c| if c.born == u64::MAX { tseq } else { c.born }).collect();
    m.tseq = tseq;
    m.ranges = tombs.ranges().iter().map(|t| (**t).clone()).collect();
    m.rows = tombs.rows_tombs().iter().map(|t| (**t).clone()).collect();
    // A sealed MANIFEST names each chunk file by its IV (see `manifest`).
    m.ivs = if m.key.is_some() {
        sorted.iter().map(|c| c.cipher.as_ref().map_or([0; crate::crypto::IV_LEN], |k| k.iv())).collect()
    } else {
        Vec::new()
    };
    m
}

pub struct Table {
    pub(crate) config: TableConfig,
    pub(crate) schema: Arc<Schema>,
    pub(crate) inner: Mutex<Inner>,
    /// Version of the series set (bumped when a series is added or the table
    /// is truncated); readable without the table mutex.
    series_version: Arc<AtomicU64>,
    /// Serializes maintenance jobs (compaction, retention) of this table.
    pub(crate) maint: Mutex<()>,
    /// Set when the directory is being dropped or renamed: maintenance stops.
    pub(crate) defunct: AtomicBool,
    /// Unix seconds of the last retention sweep.
    pub(crate) last_retention: AtomicI64,
    /// Whether a maintenance job for this table is queued (scheduler use).
    pub(crate) queued: AtomicBool,
    /// Signalled when a statement gives back the series it claimed (UNIQUE
    /// tables); waits on it use the table mutex.
    pub(crate) claims_cv: Condvar,
    /// Size of the last committed (unspilled) batch: `rows << 32 | series`. The
    /// next batch reserves that much up front instead of growing its buffers.
    pub(crate) batch_hint: AtomicU64,
    /// Signalled (with `inner`) when a flush ends; whoever needs the MemTable
    /// to be the only place with uncommitted-to-chunk rows waits on it.
    flush_cv: Condvar,
    /// Deletes of this table run one at a time (see `delete`); held outside
    /// the table mutex.
    pub(crate) del_gate: Mutex<()>,
    /// Group commit: who is already durable. Never held together with `inner`
    /// (nor during the fsync).
    group: Mutex<GroupSync>,
    /// Signalled when a leader's fsync ends.
    group_cv: Condvar,
}

/// The leader of a group fsync; dropping it publishes the outcome and wakes
/// every waiter.
struct Leader<'a> {
    table: &'a Table,
    durable: Option<(u64, u64)>,
    failed: bool,
}

impl Drop for Leader<'_> {
    fn drop(&mut self) {
        let mut g = self.table.group.lock().unwrap_or_else(|p| p.into_inner());
        g.syncing = false;
        g.failed |= self.failed;
        if let Some(d) = self.durable {
            g.durable = g.durable.max(d);
        }
        drop(g);
        self.table.group_cv.notify_all();
    }
}

#[derive(Default)]
struct GroupSync {
    /// Everything up to `(segment, offset)` is on stable storage.
    durable: (u64, u64),
    /// A leader is running an fsync; the others wait on `group_cv`.
    syncing: bool,
    /// An fsync failed: nothing is ever confirmed again.
    failed: bool,
    /// fsyncs issued by `wait_durable` (tests count them).
    fsyncs: u64,
}

fn warn_flush(e: &Error) {
    warn(&format!("MemTable flush failed, will retry: {e}"));
}

// Test hook: runs on the thread of a flush after its chunks are written and
// fsynced and before they are installed; an `Err` makes the flush fail there.
#[cfg(test)]
pub(crate) type FlushHook = Box<dyn Fn() -> Result<()>>;
#[cfg(test)]
thread_local! {
    pub(crate) static FLUSH_HOOK: std::cell::RefCell<Option<FlushHook>> =
        const { std::cell::RefCell::new(None) };
}

/// A flush between its two critical sections: it owns the rows it took out of
/// the MemTable and, unless it installs them, gives them back when dropped
/// (error or panic), so the rows are never lost nor left invisible.
pub(crate) struct FlushJob<'a> {
    table: &'a Table,
    /// The rows being sealed (shares the segments of `Inner::flushing`).
    snap: MemSnapshot,
    series: SeriesSnapshot,
    /// Last WAL segment holding rows of `snap`.
    covered: u64,
    /// Oldest segment an open batch had logged rows in when the rows were taken.
    pin: Option<u64>,
    /// Chunk files written and not yet handed to the MANIFEST: discarded if
    /// the job is dropped before the install (error or panic).
    written: Vec<ChunkMeta>,
    done: bool,
}

impl Drop for FlushJob<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.written.iter().for_each(|m| m.file.mark_obsolete());
        // A poisoned mutex means a panic left the state half-updated; the table is unusable then.
        if let Ok(mut inner) = self.table.inner.lock() {
            let older = std::mem::take(&mut inner.flushing);
            inner.memtable.prepend(older);
            inner.flush_running = false;
        }
        self.table.flush_cv.notify_all();
    }
}

/// Deletes WAL segments `<= upto`, logging (not failing) on errors: leftover
/// segments are ignored by recovery anyway.
fn delete_wal_upto(dir: &Path, upto: u64) {
    match list_segments(dir) {
        Ok(segs) => {
            for seq in segs.into_iter().filter(|&s| s <= upto) {
                if let Err(e) = remove_if_exists(&segment_path(dir, seq)) {
                    warn(&format!("cannot delete WAL segment {seq}: {e}"));
                }
            }
        }
        Err(e) => warn(&format!("cannot list WAL segments: {e}")),
    }
}

fn quarantine(path: &Path, why: &str) -> Result<()> {
    let mut target = path.as_os_str().to_owned();
    target.push(".corrupt");
    warn(&format!("{} is corrupt ({why}); moved aside to {}", path.display(), PathBuf::from(&target).display()));
    fs::rename(path, &target)?;
    Ok(())
}

impl Table {
    /// Creates the table directory with an empty MANIFEST and the OPTIONS file.
    pub fn create(config: &TableConfig) -> Result<()> {
        if let Some(k) = config.opts.encryption_key_id {
            crate::crypto::ensure_available()?;
            CipherParams::for_new_file(k).map_err(|e| invalid(format!("encryption key {k} is not available: {e}")))?;
        }
        let dir = &config.dir;
        match fs::create_dir(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read_dir(dir)?.next().is_some() {
                    return Err(invalid(format!("{} already exists and is not empty", dir.display())));
                }
            }
            Err(e) => return Err(e.into()),
        }
        write_atomic(&dir.join(OPTIONS_FILE), config.opts.to_text().as_bytes())?;
        Manifest { key: config.opts.encryption_key_id, ..Manifest::empty() }.store(dir)?;
        if let Some(parent) = dir.parent() {
            sync_dir(parent)?;
        }
        Ok(())
    }

    /// Removes the table directory and everything in it. Waits for any
    /// maintenance job still running on an open instance.
    pub fn drop_table(dir: &Path) -> Result<()> {
        crate::maintenance::retire(dir);
        match fs::remove_dir_all(dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(format!("{} does not exist", dir.display())))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn rename(from: &Path, to: &Path) -> Result<()> {
        if to.exists() {
            return Err(invalid(format!("{} already exists", to.display())));
        }
        crate::maintenance::retire(from);
        fs::rename(from, to).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound(format!("{} does not exist", from.display())),
            _ => e.into(),
        })?;
        if let Some(parent) = to.parent() {
            sync_dir(parent)?;
        }
        Ok(())
    }

    /// Opens the table, running crash recovery:
    /// 1. load the MANIFEST;
    /// 2. delete temp files and chunk files the MANIFEST does not list;
    /// 3. load metadata of live chunks, quarantining corrupt ones
    ///    (`.tfl.corrupt`), and fully verify the newest chunk's CRC;
    /// 4. rebuild the series index from chunk metadata;
    /// 5. drop WAL segments covered by the checkpoint and replay the rest
    ///    into the MemTable (a torn tail in the last segment is truncated);
    /// 6. start a new WAL segment.
    ///
    /// Use [`crate::maintenance::open_shared`] instead to guarantee a single
    /// instance per directory and background maintenance.
    pub fn open(config: TableConfig) -> Result<Table> {
        let dir = config.dir.clone();
        if !dir.is_dir() {
            return Err(Error::NotFound(format!("table directory {} does not exist", dir.display())));
        }
        let schema = Arc::new(config.schema.clone());
        let mut manifest = Manifest::load(&dir)?;

        let mut chunk_files: HashMap<u64, PathBuf> = HashMap::new();
        // Chunks an earlier recovery already moved aside (`.tfl.corrupt`).
        let mut quarantined: HashSet<u64> = HashSet::new();
        let mut wal_segs = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else { continue };
            if name.ends_with(".tmp") {
                remove_if_exists(&path)?;
            } else if let Some(id) = parse_chunk_id(&name) {
                chunk_files.insert(id, path);
            } else if let Some(id) = name.strip_suffix(".corrupt").and_then(parse_chunk_id) {
                quarantined.insert(id);
            } else if let Some(seq) = wal::parse_segment_name(&name) {
                wal_segs.push(seq);
            }
        }
        wal_segs.sort_unstable();

        // Authenticated MANIFEST (encrypted tables). One written before it
        // existed is accepted once and rewritten sealed below, unless a file
        // written by an authenticating version is present: then someone put
        // an old-format MANIFEST back on purpose. Checked before the unlisted
        // chunks are deleted.
        let key = config.opts.encryption_key_id;
        // Client-visible messages name the table and the file; the datadir path goes to the log.
        let tname = dir.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let legacy_encrypted = key.is_some() && manifest.key.is_none();
        match (key, manifest.key) {
            (None, Some(_)) => {
                return Err(invalid(format!("table {tname} has an encrypted MANIFEST but is configured without encryption")));
            }
            (Some(k), Some(m)) if k != m => {
                return Err(Error::Crypto(format!(
                    "table {tname}: the authenticated MANIFEST was sealed with key id {m} but the table is configured with key id {k}; refusing to open"
                )));
            }
            (Some(_), None) => {
                // A file that cannot be inspected counts as evidence (fail closed).
                let marked = (|| -> Result<bool> {
                    if wal::any_marks_authenticated_manifest(&dir, &wal_segs)? {
                        return Ok(true);
                    }
                    for p in chunk_files.values() {
                        if crate::chunk_reader::marks_authenticated_manifest(p)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })();
                if !matches!(marked, Ok(false)) {
                    warn(&format!("{}: unauthenticated MANIFEST next to authenticated files ({marked:?})", dir.display()));
                    return Err(Error::Crypto(format!(
                        "table {tname} has an unauthenticated MANIFEST but files written with an authenticated one, \
                         or files that cannot be inspected: the MANIFEST was replaced (downgrade); refusing to open"
                    )));
                }
                warn(&format!(
                    "{} has an unauthenticated MANIFEST (written before MANIFEST authentication); it is rewritten authenticated now",
                    dir.display()
                ));
            }
            _ => {}
        }
        manifest.key = key;

        let born_of: HashMap<u64, u64> = manifest.chunks.iter().copied().zip(manifest.born.iter().copied()).collect();
        let mut chunks = Vec::with_capacity(manifest.chunks.len());
        let mut dropped = false;
        // Whether the MANIFEST read was authenticated: then nothing may drop
        // a listed chunk, and each file must be the one the MANIFEST names.
        let sealed = key.is_some() && !legacy_encrypted;
        // Opt-in recovery (`moosedb_recover_damaged_chunks`, admin only): a damaged
        // listed chunk is set aside as in an unauthenticated table.
        let recover = crate::settings::get().recover_damaged_chunks();
        let fname = |p: &Path| p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let remedy = "restore that file from a backup, or give a file renamed to .corrupt its original name back; \
                      as a last resort an administrator can SET GLOBAL moosedb_recover_damaged_chunks=ON to drop the \
                      chunk (its rows are lost) and reopen, then set it OFF again";
        for (idx, &id) in manifest.chunks.iter().enumerate() {
            let Some(path) = chunk_files.get(&id) else {
                if quarantined.contains(&id) && (!sealed || recover) {
                    // An earlier recovery quarantined it but crashed before
                    // rewriting the MANIFEST: finish that step.
                    dropped = true;
                    continue;
                }
                // A listed chunk is committed data. Dropping it silently
                // would turn a lost file into permanent data loss.
                warn(&format!("{}: chunk {id} listed in the MANIFEST is missing", dir.display()));
                return Err(corrupt(format!(
                    "chunk {id} listed in the MANIFEST of table {tname} is missing; refusing to open ({remedy})"
                )));
            };
            match read_meta(path, id) {
                Ok(meta) => {
                    if meta.header.is_encrypted() != config.opts.encryption_key_id.is_some() {
                        return Err(invalid(format!(
                            "{} is {} but the table is configured {}",
                            fname(meta.path()),
                            if meta.header.is_encrypted() { "encrypted" } else { "not encrypted" },
                            if config.opts.encryption_key_id.is_some() {
                                "with encryption"
                            } else {
                                "without encryption"
                            },
                        )));
                    }
                    if sealed && meta.cipher.as_ref().map(|c| c.iv()) != manifest.ivs.get(idx).copied() {
                        warn(&format!("{}: not the chunk the authenticated MANIFEST lists", meta.path().display()));
                        return Err(Error::Crypto(format!(
                            "{} of table {tname} is not the chunk {id} the authenticated MANIFEST lists (replaced, copied from \
                             another table or restored from an older copy); refusing to open ({remedy})",
                            fname(meta.path())
                        )));
                    }
                    let mut meta = meta;
                    meta.born = born_of.get(&id).copied().unwrap_or(0);
                    chunks.push(meta);
                }
                // Only damage provable without a key (header/footer CRC,
                // structure) reaches this arm; a failure inside the encrypted
                // index is `Error::Crypto` and fails the open instead.
                Err(Error::Corrupt(why)) if !sealed || recover => {
                    if sealed {
                        warn(&format!(
                            "RECOVERY: {} is listed in the authenticated MANIFEST and damaged ({why}); \
                             dropped because moosedb_recover_damaged_chunks=ON, its rows are lost",
                            path.display()
                        ));
                    }
                    quarantine(path, &why)?;
                    dropped = true;
                }
                // Damage anyone can fake (the CRCs are not keyed) must not be
                // a way to drop a chunk from an authenticated MANIFEST.
                Err(Error::Corrupt(why)) => {
                    warn(&format!("{}: damaged ({why})", path.display()));
                    return Err(Error::Crypto(format!(
                        "{} of table {tname} is damaged ({why}) and the authenticated MANIFEST lists it; refusing to open ({remedy})",
                        fname(path)
                    )));
                }
                Err(e) => return Err(e),
            }
        }
        chunks.sort_by_key(|c| c.id);
        if let Some(newest) = chunks.last() {
            match verify_file(newest.path()) {
                Ok(()) => {}
                Err(Error::Corrupt(why)) if sealed && !recover => {
                    warn(&format!("{}: damaged ({why})", newest.path().display()));
                    return Err(Error::Crypto(format!(
                        "{} of table {tname} is damaged ({why}) and the authenticated MANIFEST lists it; refusing to open ({remedy})",
                        fname(newest.path())
                    )));
                }
                Err(Error::Corrupt(why)) => {
                    let path = newest.path().to_path_buf();
                    chunks.pop();
                    quarantine(&path, &why)?;
                    dropped = true;
                }
                Err(e) => return Err(e),
            }
        }
        let fingerprint = schema.fingerprint();
        if let Some(c) = chunks.iter().find(|c| c.header.schema_fingerprint != fingerprint) {
            return Err(invalid(format!("{} was written with a different table schema", c.path().display())));
        }
        let mut tombs = TombSet::default();
        for t in &manifest.ranges {
            tombs.add_range(t.clone());
        }
        for t in &manifest.rows {
            tombs.add_rows(t.clone());
        }
        {
            // A chunk recovery set aside takes the deletes it held with it.
            let live: HashSet<u64> = chunks.iter().map(|c| c.id).collect();
            let missing: HashSet<u64> = manifest.chunks.iter().copied().filter(|id| !live.contains(id)).collect();
            tombs.forget_chunks(&missing);
        }
        // Nothing is deleted before every listed chunk has been validated: a
        // stale (but authentic) MANIFEST must not cost the chunks it does not list.
        let listed: HashSet<u64> = manifest.chunks.iter().copied().collect();
        for (id, path) in &chunk_files {
            if !listed.contains(id) {
                remove_if_exists(path)?;
            }
        }
        let chunks: Vec<Arc<ChunkMeta>> = chunks.into_iter().map(Arc::new).collect();
        if dropped || legacy_encrypted {
            manifest = build_manifest(&manifest, &chunks, &tombs, manifest.tseq);
            manifest.store(&dir)?;
        }
        let mut tseq = manifest.tseq.max(tombs.max_seq());

        let mut series = SeriesIndex::default();
        let mut uniq = UniqueState::default();
        let unique = config.unique.clone();
        for c in &chunks {
            for e in &c.series {
                series.register(e.series_id, &e.tags)?;
                if unique.is_some() {
                    uniq.note_chunk(e.series_id, e.ts_max);
                }
            }
        }

        let mut memtable = MemTable::default();
        // Segments before `replay_seq` hold nothing recovery still needs. The
        // ones from there on are read even when they precede the flush point
        // (`wal_seq`): a batch that was open at the flush has its first rows
        // there. Only commits after the flush point are applied.
        for &seq in wal_segs.iter().filter(|&&s| s < manifest.replay_seq) {
            remove_if_exists(&segment_path(&dir, seq))?;
        }
        let replay: Vec<u64> = wal_segs.iter().copied().filter(|&s| s >= manifest.replay_seq).collect();
        // Needed for a row to hold a value above the limit: cheap pre-check, then exact.
        let too_big = |payload: &[u8]| {
            payload.len() > max_replay_value()
                && decode_row(&mut ByteReader::new(payload))
                    .is_ok_and(|row| row.iter().any(|v| matches!(v, Value::Bytes(b) if b.len() > max_replay_value())))
        };
        let limits = wal::ReplayLimits::standard();
        let manifest_tseq = manifest.tseq;
        let replayed =
            wal::replay_events(&dir, &replay, manifest.wal_seq, true, Some(config.opts.encryption_key_id.is_some()), &limits, &too_big, |seq, event| match event {
                wal::ReplayEvent::Row(payload) => {
                    let row = decode_row(&mut ByteReader::new(payload))?;
                    schema.validate_row(&row).map_err(|e| {
                        corrupt(format!("WAL segment {seq} holds a row that does not match the schema: {e}"))
                    })?;
                    let sid = series.get_or_insert(&row_tags(&schema, &row));
                    if let Some(spec) = &unique {
                        uniq.note_row(sid, schema.row_ts(&row), spec.extras_of(&row));
                    }
                    memtable.push(schema.row_ts(&row), sid, row);
                    if memtable.bytes() > limits.applied {
                        // A MemTable flushes long before this: only a forged or damaged
                        // log holds that many committed rows past the checkpoint.
                        return Err(corrupt(format!(
                    "WAL segment {seq}: the committed rows to replay exceed {} bytes of memory; refusing to load them",
                    limits.applied
                )));
                    }
                    Ok(())
                }
                wal::ReplayEvent::Delete(payload) => crate::delete::Replay {
                    schema: &schema,
                    chunks: &chunks,
                    tombs: &mut tombs,
                    tseq: &mut tseq,
                    manifest_tseq,
                    series: &series,
                    memtable: &mut memtable,
                    uniq: &mut uniq,
                }
                .apply(payload),
            })?;
        if !replayed.rejected.is_empty() {
            // Accepted by an older version, but no chunk can hold such a value:
            // keeping it would make every flush (and so the table) fail for
            // good. The whole batch is withheld (all or nothing) and the
            // segments holding its rows are preserved, before the checkpoint
            // below can delete them.
            let mut segs: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
            for r in &replayed.rejected {
                segs.extend(r.segments.iter().copied());
            }
            let mut copies = Vec::new();
            let mut total = 0u64;
            for &seq in &segs {
                let src = segment_path(&dir, seq);
                let mut dst = src.as_os_str().to_owned();
                dst.push(OVERSIZED_SUFFIX);
                let dst = PathBuf::from(dst);
                total += preserve_segment(&src, &dst)?;
                copies.push(dst.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default());
            }
            sync_dir(&dir)?;
            for r in &replayed.rejected {
                warn(&format!(
                    "{}: withheld {} committed batch with {} row(s), {} bytes: a value exceeds {} bytes and no chunk can store it",
                    dir.display(),
                    r.batch.map_or("single-row".to_string(), |id| format!("batch {id}")),
                    r.rows,
                    r.bytes,
                    max_replay_value()
                ));
            }
            warn(&format!(
                "{}: kept {total} bytes of WAL as {} (same directory). {}Delete them once the data is no longer needed.",
                dir.display(),
                copies.join(", "),
                if config.opts.encryption_key_id.is_some() {
                    "The copies stay encrypted with their original key version: retiring that version makes them unreadable. "
                } else {
                    ""
                }
            ));
        }

        let next_seq = succ(wal_segs.last().copied().unwrap_or(0).max(manifest.wal_seq))?;
        let next_batch_id = replayed
            .max_batch_id
            .checked_add(1)
            .ok_or_else(|| corrupt("the WAL holds a batch id of u64::MAX; refusing to open (forged or damaged log)"))?;
        let wal = Wal::create(&dir, next_seq, config.opts.encryption_key_id)?;
        if memtable.is_empty() && (manifest.replay_seq != next_seq || manifest.wal_seq != next_seq - 1) {
            // Nothing recovered is still needed: every committed row is in a
            // chunk and batches without a COMMIT are dead (new ids are larger).
            // Move the checkpoint past the old segments, which are then
            // deleted, so reopening an idle table does not pile up segments
            // (and no old key version stays referenced by a WAL file).
            // The manifest it writes carries every delete replayed above: their
            // log entries are in the segments deleted next.
            let mut m = build_manifest(&manifest, &chunks, &tombs, tseq);
            m.wal_seq = next_seq - 1;
            m.replay_seq = next_seq;
            match m.store(&dir) {
                Ok(()) => {
                    manifest = m;
                    delete_wal_upto(&dir, next_seq - 1);
                }
                Err(e) => warn(&format!("cannot advance the WAL checkpoint at open: {e}")),
            }
        }
        let series_version = series.version_handle();
        Ok(Table {
            config,
            schema,
            inner: Mutex::new(Inner {
                manifest,
                chunks,
                wal,
                memtable,
                flushing: MemTable::default(),
                flush_running: false,
                series,
                mem_generation: 0,
                version: 0,
                snapshot_cache: None,
                compacting: HashSet::new(),
                no_gain: HashSet::new(),
                poisoned: None,
                next_batch_id,
                open_batches: HashMap::new(),
                epoch: 0,
                uniq,
                tombs: Arc::new(tombs),
                tseq,
                layout_gen: 0,
            }),
            series_version,
            maint: Mutex::new(()),
            defunct: AtomicBool::new(false),
            // First background sweep one interval after opening; explicit
            // sweeps (OPTIMIZE, CALL moosedb_apply_retention) run at once.
            last_retention: AtomicI64::new(crate::time::now_micros() / crate::time::MICROS_PER_SEC),
            queued: AtomicBool::new(false),
            claims_cv: Condvar::new(),
            flush_cv: Condvar::new(),
            batch_hint: AtomicU64::new(0),
            del_gate: Mutex::new(()),
            group: Mutex::new(GroupSync::default()),
            group_cv: Condvar::new(),
        })
    }

    /// Locks the table state. A panic while holding it poisons the mutex and
    /// may leave the state half-updated (e.g. rows both in a chunk and still
    /// in the MemTable). Serving reads from it could return duplicates, which
    /// is worse than an error, so every operation fails with a clear message
    /// until the table is reopened (recovery rebuilds the state). Nothing
    /// waits: the error is immediate.
    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, Inner>> {
        self.inner.lock().map_err(|_| {
            Error::ReadOnly("table must be reopened: a thread panicked while holding the table state".into())
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    pub fn dir(&self) -> &Path {
        &self.config.dir
    }

    /// Appends a row: WAL first, then MemTable. The row is durable once
    /// `sync_wal(true)` returns.
    pub fn write(&self, row: Row) -> Result<()> {
        if self.config.unique.is_some() {
            return Err(Error::Unsupported(
                "a table with a UNIQUE key is written through write_shared or a batch".into(),
            ));
        }
        self.schema.validate_for_write(&row)?;
        let payload = wal::row_commit_entry(&row);
        // Reject before touching the WAL: an oversized row is the caller's
        // mistake, not a storage failure, and must not poison the table.
        if payload.len() > wal::max_entry() {
            return Err(invalid(format!("row of {} bytes exceeds the {} byte limit", payload.len(), wal::max_entry())));
        }
        let tags = row_tags(&self.schema, &row);

        let limit = usize::try_from(self.config.opts.memtable_size_bytes).unwrap_or(usize::MAX);
        let guard = self.lock()?;
        let mut guard = self.make_room(guard, limit)?;
        let inner = &mut *guard;
        inner.check_writable()?;
        if let Err(e) = inner.wal.append(&payload) {
            // Only a failed I/O leaves the segment in an unknown state.
            if !matches!(e, Error::InvalidArg(_)) {
                inner.poison(format!("WAL append failed: {e}"));
            }
            return Err(e);
        }
        let sid = inner.series.get_or_insert(&tags);
        inner.memtable.push(self.schema.row_ts(&row), sid, row);
        inner.changed();
        // The row is already safe in the WAL, so a failed flush must not fail
        // the write; it is retried on the next write.
        let job = if inner.memtable.bytes() >= limit { self.begin_flush(inner).unwrap_or_else(|e| { warn_flush(&e); None }) } else { None };
        drop(guard);
        if let Some(job) = job {
            if let Err(e) = self.run_flush(job) {
                warn_flush(&e);
            }
        }
        Ok(())
    }

    /// Backpressure: while the rows not yet in chunks (MemTable plus the ones
    /// being flushed) are at twice the limit, waits for the running flush or
    /// runs one (flushes have been failing, or writers outrun them). Returns the
    /// guard of a later acquisition of the mutex: the caller must check again
    /// whatever it checked before.
    pub(crate) fn make_room<'a>(&'a self, mut guard: MutexGuard<'a, Inner>, limit: usize) -> Result<MutexGuard<'a, Inner>> {
        let full = |e: Error| Error::Full(format!("MemTable is full and flushing failed: {e}"));
        loop {
            guard.check_writable()?;
            if guard.mem_bytes() < limit.saturating_mul(2) {
                return Ok(guard);
            }
            if guard.flush_running {
                guard = self.wait_flush(guard)?;
                continue;
            }
            let job = self.begin_flush(&mut guard).map_err(full)?;
            drop(guard);
            if let Some(job) = job {
                self.run_flush(job).map_err(full)?;
            }
            guard = self.lock()?;
        }
    }

    fn wait_flush<'a>(&'a self, guard: MutexGuard<'a, Inner>) -> Result<MutexGuard<'a, Inner>> {
        self.flush_cv
            .wait(guard)
            .map_err(|_| Error::ReadOnly("table must be reopened: a thread panicked while holding the table state".into()))
    }

    /// [`Table::lock`] once no flush is running: for operations that change or
    /// drop the MemTable's rows (DELETE, TRUNCATE...) and so cannot share them
    /// with a flush that is sealing them. The wait releases the mutex; whatever
    /// the caller read before must be validated again after this returns.
    pub(crate) fn lock_idle(&self) -> Result<MutexGuard<'_, Inner>> {
        let mut guard = self.lock()?;
        while guard.flush_running {
            guard = self.wait_flush(guard)?;
        }
        Ok(guard)
    }

    /// Statement-end hook: pushes buffered WAL entries to the OS and, if
    /// `durable`, to stable storage.
    pub fn sync_wal(&self, durable: bool) -> Result<()> {
        let mut guard = self.lock()?;
        guard.check_writable()?;
        let inner = &mut *guard;
        if let Err(e) = inner.wal.sync(durable) {
            // Never report success on a retry: the data may not have reached the disk.
            inner.poison(format!("WAL sync failed: {e}"));
            return Err(e);
        }
        Ok(())
    }

    /// Blocks until the WAL is on stable storage up to `target` (segment,
    /// offset), as returned by `Wal::flush_for_commit`. One fsync serves every
    /// waiting caller: the first becomes the leader and syncs all the bytes
    /// handed to the OS so far; the others sleep on `group_cv` (the `group`
    /// mutex is not held during the fsync) and are all woken at once when it
    /// ends. The fsync runs outside the table mutex.
    pub(crate) fn wait_durable(&self, target: (u64, u64)) -> Result<()> {
        let mut g = self.group.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if g.failed {
                return Err(Error::ReadOnly("WAL fsync failed earlier; the table must be reopened".into()));
            }
            if g.durable >= target {
                return Ok(());
            }
            if !g.syncing {
                break;
            }
            g = self.group_cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
        g.syncing = true;
        g.fsyncs += 1;
        drop(g);
        // Ends the leadership however this returns. Anything but a successful
        // fsync (an error or a panic) is sticky: the table is unusable then anyway.
        let mut leader = Leader { table: self, durable: None, failed: true };
        let (seq, os_offset, handle) = {
            let guard = self.lock()?;
            guard.check_writable()?;
            guard.wal.sync_point()?
        };
        // Every rotation fsyncs the old segment before the new one exists
        // (flush, collect), or a TRUNCATE made its rows irrelevant; syncing the
        // current segment is merely redundant for a target in an older one.
        if let Err(e) = crate::fsutil::sync_data(&handle) {
            if let Ok(mut guard) = self.lock() {
                guard.wal.mark_failed();
                guard.poison(format!("WAL sync failed: {e}"));
            }
            return Err(e);
        }
        // Our own bytes were handed to the OS before we got here: covered.
        leader.durable = Some((seq, os_offset));
        leader.failed = false;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn group_fsyncs(&self) -> u64 {
        self.group.lock().unwrap().fsyncs
    }

    /// Seals the MemTable into chunks (one per time bucket). Waits for a flush
    /// already running, then flushes what is left; returns when the rows that
    /// were in the MemTable on entry are in chunks.
    pub fn flush(&self) -> Result<()> {
        let mut guard = self.lock_idle()?;
        let job = self.begin_flush(&mut guard)?;
        drop(guard);
        match job {
            Some(job) => self.run_flush(job),
            None => Ok(()),
        }
    }

    /// First critical section of a flush (table mutex held): moves the MemTable's
    /// rows to `Inner::flushing` (O(1); they stay visible) and starts a new WAL
    /// segment, so that every row taken is committed in a segment `<= covered`
    /// and everything committed from now on lands in a later one. `None` when a
    /// flush is already running, or when the MemTable is empty (then the WAL is
    /// merely collected). The caller must call [`Table::run_flush`] on the job
    /// after releasing the mutex.
    pub(crate) fn begin_flush(&self, inner: &mut Inner) -> Result<Option<FlushJob<'_>>> {
        inner.check_writable()?;
        if inner.flush_running {
            return Ok(None);
        }
        if inner.memtable.is_empty() {
            // Nothing to seal, but the WAL may hold only dead entries
            // (spilled or aborted batches): FLUSH TABLES / OPTIMIZE collect them.
            self.collect_wal_locked(inner)?;
            return Ok(None);
        }
        let dir = &self.config.dir;
        let covered = inner.wal.seq();
        let after_covered = succ(covered)?;
        // The old segment must be durable before any entry exists in the next one:
        // a COMMIT there may refer to rows logged here, and a torn tail followed
        // by a newer segment is corruption at replay. One fsync per flush.
        if let Err(e) = inner.wal.sync(true) {
            inner.poison(format!("WAL sync failed: {e}"));
            return Err(e);
        }
        // Nothing has changed if this fails (unlike a rotation after the MANIFEST switch).
        let created = Wal::create(dir, after_covered, self.config.opts.encryption_key_id);
        let fresh = match created {
            Ok(w) => w,
            Err(e) => {
                if is_fsync_error(&e) {
                    inner.poison(format!("cannot create WAL segment: {e}"));
                } else if !matches!(&e, Error::Io(io) if io.kind() == std::io::ErrorKind::AlreadyExists) {
                    // A failure after the file was created (directory fsync...) would
                    // make every retry fail with AlreadyExists.
                    let _ = remove_if_exists(&segment_path(dir, after_covered));
                }
                return Err(e);
            }
        };
        inner.wal = fresh;
        inner.flushing = inner.memtable.take();
        inner.flush_running = true;
        // The view (the rows taken, then the MemTable's) shows the same rows in
        // the same order as before: ordinals and cached snapshots stay valid.
        let snap = inner.flushing.snapshot(inner.mem_generation);
        Ok(Some(FlushJob {
            table: self,
            snap,
            series: inner.series.snapshot(),
            covered,
            pin: inner.oldest_pin(),
            written: Vec::new(),
            done: false,
        }))
    }

    /// Second part of a flush, without the table mutex: fsync the rotated-away
    /// segments, group, encode, write and fsync the chunks (`.tmp`, fsync,
    /// rename, `sync_dir`); then, under the mutex again, switch the MANIFEST
    /// (the commit point), list the chunks and drop the rows taken. On any
    /// error the chunks written are discarded and the rows go back to the MemTable.
    pub(crate) fn run_flush(&self, job: FlushJob<'_>) -> Result<()> {
        // Writers outran the flush: the MemTable is already over the limit
        // again. One more flush at most, so this statement's latency stays
        // bounded (and DELETE/TRUNCATE get their turn); the next write over the
        // limit starts the one after.
        if let Some(job) = self.run_flush_once(job, true)? {
            if let Err(e) = self.run_flush_once(job, false) {
                warn_flush(&e);
            }
        }
        Ok(())
    }

    fn run_flush_once<'a>(&'a self, mut job: FlushJob<'a>, chain: bool) -> Result<Option<FlushJob<'a>>> {
        let dir = &self.config.dir;
        let opts = &self.config.opts;
        let limit = usize::try_from(opts.memtable_size_bytes).unwrap_or(usize::MAX);
        let now = crate::time::now_micros();

        // Plan: which chunks the rows make (and so how many ids they need).
        let groups = job.snap.group(&self.schema, |ts| opts.chunk_interval.bucket(ts));
        struct Planned<'a> {
            sid: u64,
            tags: Arc<[Value]>,
            parts: Vec<&'a [&'a Row]>,
        }
        let mut plan: Vec<((i64, i64), Vec<Planned<'_>>)> = Vec::with_capacity(groups.len());
        let mut total_ids = 0u64;
        for (bucket, series) in &groups {
            let mut planned = Vec::with_capacity(series.len());
            let mut need = 0u64;
            for (sid, rows) in series {
                let tags = job.series.get_by_id(*sid).ok_or_else(|| corrupt(format!("unknown series {sid}")))?;
                // A series too big for one column block goes to several
                // chunks of the bucket, as compaction does.
                let parts = split_series(rows);
                need = need.max(parts.len() as u64);
                planned.push(Planned { sid: *sid, tags, parts });
            }
            total_ids = total_ids
                .checked_add(need)
                .ok_or_else(|| corrupt("chunk ids are exhausted (the MANIFEST holds an implausible next_chunk_id)"))?;
            plan.push((*bucket, planned));
        }
        let mut next_id = {
            let mut inner = self.lock()?;
            inner.check_writable()?;
            // Ids burnt by a failure are harmless (as for compaction).
            inner.reserve_chunk_ids(total_ids)?
        };

        let result = (|| -> Result<()> {
            for (bucket, planned) in &plan {
                let parts: Vec<SeriesParts> = planned
                    .iter()
                    .map(|p| SeriesParts { series_id: p.sid, tags: &p.tags[..], parts: p.parts.clone() })
                    .collect();
                let n = chunks_needed(&parts) as u64;
                let codec = opts.codec_for(bucket.1, now);
                write_bucket(&self.schema, codec, opts.encryption_key_id, dir, *bucket, &parts, next_id, job.covered, &mut job.written)?;
                next_id += n;
            }
            sync_dir(dir)
        })();
        #[cfg(test)]
        let result = result.and_then(|()| FLUSH_HOOK.with(|h| h.borrow().as_ref().map_or(Ok(()), |f| f())));
        if let Err(e) = result {
            if is_fsync_error(&e) {
                if let Ok(mut inner) = self.lock() {
                    inner.poison(format!("fsync of a new chunk failed: {e}"));
                }
            }
            return Err(e);
        }
        let chunks_new: Vec<Arc<ChunkMeta>> = std::mem::take(&mut job.written).into_iter().map(Arc::new).collect();

        // Install.
        let mut guard = match self.lock().and_then(|g| g.check_writable().map(|()| g)) {
            Ok(g) => g,
            Err(e) => {
                chunks_new.iter().for_each(|m| m.file.mark_obsolete());
                return Err(e);
            }
        };
        let inner = &mut *guard;
        let covered = job.covered;
        let after_covered = succ(covered)?;
        // A batch that logged rows before the rows were taken and committed (or
        // not) since has its rows in an older segment and its COMMIT after the
        // flush point: replay must still reach its first row. Batches that
        // began later log in segments after `covered`.
        let replay_seq = job.pin.map_or(after_covered, |s| s.min(after_covered));
        inner.flag_laggards(dir, covered);
        // The new chunks are born now: no delete ran since the rows were taken
        // (DELETE waits for the flush), and earlier ones are applied to those rows.
        let mut chunks = inner.chunks.clone();
        chunks.extend(chunks_new.iter().cloned());
        let tombs = (*inner.tombs).clone();
        // On failure the files are neither listed nor deleted: recovery decides
        // (as in compaction); the rows return to the MemTable through `job`.
        inner.publish(dir, chunks, tombs, |m| {
            m.wal_seq = covered;
            m.replay_seq = replay_seq;
        })?;
        inner.flushing = MemTable::default();
        inner.flush_running = false;
        inner.mem_generation += 1;
        inner.changed();
        job.done = true;
        let next = if chain && inner.memtable.bytes() >= limit { self.begin_flush(inner).unwrap_or_else(|e| { warn_flush(&e); None }) } else { None };
        drop(guard);
        self.flush_cv.notify_all();
        delete_wal_upto(dir, replay_seq.saturating_sub(1));
        Ok(next)
    }

    /// Checkpoint without a flush: with an empty MemTable every committed row
    /// is already in a chunk, so the WAL segments that only hold flushed or
    /// dead entries (rows of spilled or aborted batches, which can never
    /// commit through the log) can go. Segments still needed by an open batch
    /// that logged rows are kept (`replay_seq` stops at the oldest of them).
    ///
    /// Unlike a flush this creates the new segment *before* switching the
    /// MANIFEST, so a failure to create it changes nothing and does not
    /// poison the table.
    pub(crate) fn collect_wal_locked(&self, inner: &mut Inner) -> Result<()> {
        // Rows being flushed are in no chunk yet: the log still holds them.
        if !inner.memtable.is_empty() || inner.flush_running {
            return Ok(());
        }
        let dir = &self.config.dir;
        let active = inner.wal.seq();
        let pin = inner.oldest_pin();
        if inner.wal.is_empty() {
            // Keep appending to the empty active segment; only older ones go.
            let target = pin.map_or(active, |p| p.min(active));
            if target <= inner.manifest.replay_seq {
                return Ok(());
            }
            let mut manifest = inner.manifest_now();
            manifest.wal_seq = manifest.wal_seq.max(active.saturating_sub(1));
            manifest.replay_seq = target;
            inner.commit_manifest(dir, manifest)?;
            delete_wal_upto(dir, target - 1);
            return Ok(());
        }
        // The active segment holds entries; open batches' rows in it must be
        // durable before the log moves on.
        if let Err(e) = inner.wal.sync(true) {
            inner.poison(format!("WAL sync failed: {e}"));
            return Err(e);
        }
        let next = succ(active)?;
        let fresh = Wal::create(dir, next, self.config.opts.encryption_key_id)?;
        let mut manifest = inner.manifest_now();
        manifest.wal_seq = active;
        manifest.replay_seq = pin.map_or(next, |p| p.min(next));
        let replay_seq = manifest.replay_seq;
        inner.flag_laggards(dir, active);
        inner.commit_manifest(dir, manifest)?;
        inner.wal = fresh;
        delete_wal_upto(dir, replay_seq - 1);
        Ok(())
    }

    /// Removes every row. Crash-safe: the MANIFEST switch is the commit point.
    pub fn truncate(&self) -> Result<()> {
        let mut guard = self.lock_idle()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        let dir = &self.config.dir;
        let old_seq = inner.wal.seq();
        let after_old = succ(old_seq)?;

        let mut manifest = inner.manifest_for(&[], &TombSet::default());
        manifest.wal_seq = old_seq;
        manifest.replay_seq = after_old;
        inner.commit_manifest(dir, manifest)?;

        inner.tombs = Arc::new(TombSet::default());
        inner.replace_chunks(Vec::new());
        inner.memtable.clear();
        inner.series.clear();
        inner.mem_generation += 1;
        // Open batches must not resurrect rows from before the truncate: their
        // commit is refused (epoch) and their logged rows lie before replay_seq.
        inner.open_batches.clear();
        inner.epoch += 1;
        inner.uniq.clear();
        self.claims_cv.notify_all();
        inner.rotate_wal(dir, self.config.opts.encryption_key_id)?;
        delete_wal_upto(dir, old_seq);
        Ok(())
    }

    /// Drops every chunk whose newest row is older than the retention cutoff
    /// relative to `now` (µs). Returns the number of chunks removed.
    pub fn apply_retention(&self, now: i64) -> Result<usize> {
        self.last_retention.store(now / crate::time::MICROS_PER_SEC, Ordering::Relaxed);
        let Some(cutoff) = self.config.opts.retention.cutoff(now) else { return Ok(0) };
        let mut guard = self.lock()?;
        let inner = &mut *guard;
        inner.check_writable()?;
        let kept: Vec<_> = inner.chunks.iter().filter(|c| c.header.ts_max >= cutoff).cloned().collect();
        let expired = inner.chunks.len() - kept.len();
        if expired == 0 {
            return Ok(0);
        }
        let tombs = (*inner.tombs).clone();
        inner.publish(&self.config.dir, kept, tombs, |_| {})?;
        Ok(expired)
    }

    /// Immutable view of the current contents. Cheap when nothing changed
    /// since the previous call (the view is shared).
    pub fn snapshot(&self) -> Result<Arc<Snapshot>> {
        let mut guard = self.lock()?;
        Ok(self.snapshot_locked(&mut guard))
    }

    pub(crate) fn snapshot_locked(&self, inner: &mut Inner) -> Arc<Snapshot> {
        if let Some(s) = &inner.snapshot_cache {
            return s.clone();
        }
        // Rows being flushed are older than the MemTable's and not in any chunk yet.
        let mut mem = inner.flushing.snapshot(inner.mem_generation);
        mem.extend(inner.memtable.snapshot(inner.mem_generation));
        let s = Arc::new(Snapshot {
            schema: self.schema.clone(),
            chunks: inner.chunks.clone(),
            mem,
            version: inner.version,
            tombs: inner.tombs.clone(),
        });
        inner.snapshot_cache = Some(s.clone());
        s
    }

    /// `all` is the series snapshot taken under the same lock as the table
    /// snapshot (only needed when the filter has TAG predicates).
    fn resolve(&self, filter: &ScanFilter, all: Option<SeriesSnapshot>) -> Result<ResolvedFilter> {
        check_tag_columns(&self.schema, filter)?;
        let mut series: Option<HashSet<u64>> = filter.series.as_ref().map(|s| s.iter().copied().collect());
        if !filter.tags.is_empty() {
            // The walk over every series happens on a shared snapshot, not
            // under the table mutex.
            let all = all.ok_or_else(|| invalid("internal error: series snapshot missing"))?;
            let matching: HashSet<u64> = all
                .iter()
                .filter(|(_, tags)| {
                    filter
                        .tags
                        .iter()
                        .all(|(col, v)| self.schema.tag_position(*col).and_then(|p| tags.get(p)) == Some(v))
                })
                .map(|(id, _)| id)
                .collect();
            series = Some(match series {
                Some(s) => s.intersection(&matching).copied().collect(),
                None => matching,
            });
        }
        let prune = crate::zone::Pruner::from_cols(&self.schema, &filter.cols)?;
        Ok(ResolvedFilter { ts_min: filter.ts_min, ts_max: filter.ts_max, series, prune })
    }

    /// Opens a scan over a snapshot of the table. With `sorted`, rows come in
    /// timestamp order and the scan supports backward iteration.
    pub fn scan(&self, filter: &ScanFilter, sorted: bool) -> Result<Scan> {
        self.scan_projected(filter, sorted, None)
    }

    /// [`Table::scan`] returning only the columns flagged in `columns` (one flag
    /// per schema column; `None` = all). Other columns are not decoded and read
    /// as NULL. The timestamp is decoded internally whenever the range or the
    /// order needs it, even if not projected.
    pub fn scan_projected(&self, filter: &ScanFilter, sorted: bool, columns: Option<&[bool]>) -> Result<Scan> {
        let (snap, resolved) = self.resolved_snapshot(filter)?;
        let tie = self.config.unique.as_ref().map_or(&[][..], |u| u.extra_columns());
        Scan::new(snap, resolved, sorted, columns, tie)
    }

    /// Opens an aggregation (see `agg`) over one snapshot of the table, with
    /// the same visibility as an unordered scan: `lo <= ts <= hi`, optionally
    /// only the given series (`None` = all).
    pub fn agg_open(&self, lo: i64, hi: i64, series: Option<Vec<u64>>) -> Result<crate::agg::Agg> {
        let filter = ScanFilter { ts_min: lo, ts_max: hi, series, ..ScanFilter::default() };
        let (snap, resolved) = self.resolved_snapshot(&filter)?;
        Ok(crate::agg::Agg::new(snap, resolved))
    }

    fn resolved_snapshot(&self, filter: &ScanFilter) -> Result<(Arc<Snapshot>, ResolvedFilter)> {
        // Both views under ONE acquisition of the mutex: a commit between two
        // acquisitions could show the rows of a statement's series X but not
        // of its new series Y (a partly visible statement).
        let (snap, all) = {
            let mut guard = self.lock()?;
            let all = (!filter.tags.is_empty()).then(|| guard.series.snapshot());
            (self.snapshot_locked(&mut guard), all)
        };
        Ok((snap, self.resolve(filter, all)?))
    }

    /// Re-reads the row at `pos` from the current contents.
    pub fn fetch(&self, pos: Position) -> Result<Row> {
        self.snapshot()?.fetch(pos)
    }

    /// Every known series: `(series_id, tag values in tag order)`, sorted by
    /// id. Copies the series (outside the table mutex); prefer
    /// [`Table::series_snapshot`] for large series sets.
    pub fn series(&self) -> Result<Vec<(u64, Vec<Value>)>> {
        Ok(self.series_snapshot()?.to_sorted_vec())
    }

    /// Shared, immutable view of the series: O(1) under the table mutex
    /// whatever the number of series (no copy of the index).
    pub fn series_snapshot(&self) -> Result<SeriesSnapshot> {
        Ok(self.lock()?.series.snapshot())
    }

    /// Changes whenever a series is added or the table is truncated; equal
    /// values mean the same set of series. Lock-free.
    pub fn series_version(&self) -> u64 {
        self.series_version.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> Result<TableStats> {
        let inner = self.lock()?;
        let mut s = TableStats {
            chunk_count: inner.chunks.len() as u32,
            memtable_rows: inner.mem_len() as u64,
            series_count: inner.series.len() as u64,
            series_bytes: inner.series.bytes() as u64,
            unique_bytes: inner.uniq.bytes() as u64,
            ..TableStats::default()
        };
        for c in &inner.chunks {
            s.row_count += c.header.row_count;
            s.data_bytes += c.raw_bytes;
            s.compressed_bytes += c.file_size;
        }
        s.row_count = s.row_count.saturating_sub(inner.tombs.dead_total());
        s.row_count += s.memtable_rows;
        s.data_bytes += inner.mem_bytes() as u64;
        Ok(s)
    }

    /// Estimated number of rows with `lo <= ts <= hi`, for the optimizer.
    pub fn estimate_rows(&self, lo: i64, hi: i64) -> Result<u64> {
        if lo > hi {
            return Ok(0);
        }
        let inner = self.lock()?;
        let mut total = 0u64;
        for c in inner.chunks.iter().filter(|c| c.overlaps(lo, hi)) {
            let (cmin, cmax) = (c.header.ts_min, c.header.ts_max);
            let span = (cmax as i128 - cmin as i128).max(1);
            let overlap = (hi.min(cmax) as i128 - lo.max(cmin) as i128 + 1).clamp(1, span);
            total += ((c.header.row_count as i128 * overlap / span) as u64).max(1);
        }
        total += inner.flushing.estimate_in_range(lo, hi) + inner.memtable.estimate_in_range(lo, hi);
        Ok(total)
    }

    /// Verifies the CRC32 of every live chunk. Returns `(chunk file, problem)`
    /// for each failure; an empty list means the table is healthy.
    pub fn check(&self) -> Result<Vec<(String, String)>> {
        let chunks = self.lock()?.chunks.clone();
        let mut problems = Vec::new();
        for c in &chunks {
            if let Err(e) = verify_file(c.path()) {
                let why = match e {
                    Error::Corrupt(why) => why,
                    e => e.to_string(),
                };
                problems.push((c.file_name(), why));
            }
        }
        Ok(problems)
    }

    /// Snapshot of the live chunks' metadata (diagnostics).
    pub fn chunks(&self) -> Result<Vec<Arc<ChunkMeta>>> {
        Ok(self.lock()?.chunks.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsutil::{fail_fsync_after, restore_fsync};
    use crate::options::RawOptions;
    use crate::schema::{Column, ColumnType};

    fn config(dir: &Path) -> TableConfig {
        config_with(dir, &RawOptions::default())
    }

    fn config_with(dir: &Path, raw: &RawOptions<'_>) -> TableConfig {
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "v".into(), ty: ColumnType::Float64 },
            ],
            0,
        )
        .unwrap();
        TableConfig::new(dir, schema, raw).unwrap()
    }

    fn row(i: i64, host: &str) -> Row {
        vec![
            Value::Timestamp(1_700_000_000_000_000 + i),
            Value::Bytes(host.as_bytes().to_vec()),
            Value::Float64(i as f64),
        ]
    }

    fn rows(t: &Table) -> usize {
        let mut s = t.scan(&ScanFilter::default(), false).unwrap();
        let mut n = 0;
        while s.next_row().unwrap().is_some() {
            n += 1;
        }
        n
    }

    fn open_new(dir: &Path) -> Table {
        let cfg = config(dir);
        Table::create(&cfg).unwrap();
        Table::open(cfg).unwrap()
    }

    #[test]
    fn oversized_row_is_rejected_without_poisoning() {
        let dir = tempfile::tempdir().unwrap();
        let t = open_new(&dir.path().join("t"));
        t.write(row(1, "a")).unwrap();
        wal::set_max_entry(64);
        let big = vec![Value::Timestamp(5), Value::Bytes(b"a".to_vec()), Value::Float64(1.0)];
        let mut huge = big.clone();
        huge[1] = Value::Bytes(vec![b'x'; 1000]);
        let err = t.write(huge).unwrap_err();
        wal::set_max_entry(1 << 30);
        assert!(matches!(err, Error::InvalidArg(_)), "{err}");
        // Still fully writable and durable.
        t.write(row(2, "a")).unwrap();
        t.sync_wal(true).unwrap();
        t.flush().unwrap();
        assert_eq!(rows(&t), 2);
    }

    #[test]
    fn wal_fsync_failure_poisons_and_never_succeeds_on_retry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = open_new(&path);
        t.write(row(1, "a")).unwrap();
        fail_fsync_after(0);
        assert!(t.sync_wal(true).is_err());
        restore_fsync(); // the disk "recovers"; the table must not believe it
        assert!(t.sync_wal(true).is_err(), "retry must not report success");
        assert!(matches!(t.write(row(2, "a")), Err(Error::ReadOnly(_))));
        assert!(matches!(t.flush(), Err(Error::ReadOnly(_))));
        drop(t);
        // Reopening runs recovery and the table is usable again.
        let t = Table::open(config(&path)).unwrap();
        t.write(row(3, "a")).unwrap();
        t.sync_wal(true).unwrap();
    }

    #[test]
    fn chunk_fsync_failure_during_flush_poisons() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = open_new(&path);
        for i in 0..5 {
            t.write(row(i, "a")).unwrap();
        }
        // flush: WAL sync succeeds (1), then the chunk's fsync fails.
        fail_fsync_after(1);
        assert!(t.flush().is_err());
        restore_fsync();
        assert!(matches!(t.write(row(9, "a")), Err(Error::ReadOnly(_))));
        drop(t);
        let t = Table::open(config(&path)).unwrap();
        assert_eq!(rows(&t), 5, "rows are recovered from the WAL");
    }

    /// Crash during the MANIFEST switch of an encrypted table: whichever fsync
    /// fails (the chunk's, the MANIFEST's temp file, the directory), the table
    /// reopens with every row, the MANIFEST on disk is authentic (old or new)
    /// and no temp file is left.
    #[cfg(feature = "encryption")]
    #[test]
    fn a_failed_fsync_anywhere_in_a_flush_leaves_an_authentic_manifest() {
        crate::crypto::test_keys::install();
        let raw = RawOptions { encryption_key_id: Some(1), ..Default::default() };
        for ok in 0..12u32 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("t");
            let cfg = config_with(&path, &raw);
            Table::create(&cfg).unwrap();
            let t = Table::open(cfg).unwrap();
            for i in 0..5 {
                t.write(row(i, "a")).unwrap();
            }
            fail_fsync_after(ok);
            let _ = t.flush();
            restore_fsync();
            drop(t);
            let m = Manifest::load(&path).unwrap_or_else(|e| panic!("ok={ok}: MANIFEST not authentic: {e}"));
            assert_eq!(m.key, Some(1));
            let t = Table::open(config_with(&path, &raw)).unwrap_or_else(|e| panic!("ok={ok}: {e}"));
            assert_eq!(rows(&t), 5, "ok={ok}");
            assert!(tmp_files(&path).is_empty(), "ok={ok}");
            t.flush().unwrap();
            drop(t);
            assert_eq!(rows(&Table::open(config_with(&path, &raw)).unwrap()), 5, "ok={ok}");
        }
    }

    /// Migration of an encrypted table written before the authenticated MANIFEST:
    /// an fsync failing at any point of the first `open` leaves a table that
    /// reopens with all its rows, and once it has opened cleanly the old MANIFEST
    /// can no longer be put back. (Between a failed open that already stored the v4
    /// MANIFEST and the next open there is a window; see architecture.md.)
    #[cfg(feature = "encryption")]
    #[test]
    fn an_fsync_failure_anywhere_in_the_migration_open_loses_nothing_and_shuts_the_downgrade() {
        crate::crypto::test_keys::install();
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../moosedb-tests/fixtures");
        let schema = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "metric".into(), ty: ColumnType::Tag },
                Column { name: "value".into(), ty: ColumnType::Float64 },
                Column { name: "value_int".into(), ty: ColumnType::Int64 },
                Column { name: "note".into(), ty: ColumnType::Varchar },
            ],
            0,
        )
        .unwrap();
        let cfg = |p: &Path| {
            TableConfig::new(p, schema.clone(), &RawOptions { encryption_key_id: Some(7), ..Default::default() }).unwrap()
        };
        for case in ["aead_v3", "ctr_v2"] {
            let legacy = fs::read(fixtures.join(case).join("MANIFEST")).unwrap();
            for ok in 0..14u32 {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("t");
                fs::create_dir(&path).unwrap();
                for e in fs::read_dir(fixtures.join(case)).unwrap() {
                    let e = e.unwrap();
                    fs::copy(e.path(), path.join(e.file_name())).unwrap();
                }
                fail_fsync_after(ok);
                let first = Table::open(cfg(&path));
                restore_fsync();
                drop(first);
                let t = Table::open(cfg(&path)).unwrap_or_else(|e| panic!("{case}/{ok}: {e}"));
                assert_eq!(rows(&t), 35, "{case}/{ok}");
                drop(t);
                fs::write(path.join("MANIFEST"), &legacy).unwrap();
                assert!(matches!(Table::open(cfg(&path)), Err(Error::Crypto(_))), "{case}/{ok}: downgrade accepted");
            }
        }
    }

    #[test]
    fn listed_chunk_missing_fails_open_but_orphans_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = open_new(&path);
        for i in 0..5 {
            t.write(row(i, "a")).unwrap();
        }
        t.flush().unwrap();
        let chunk = t.chunks().unwrap()[0].path().to_path_buf();
        drop(t);
        // An unlisted chunk file (crash before the MANIFEST switch) is an orphan.
        let orphan = path.join("chunk_19700101T000000_19700102T000000_000099.tfl");
        fs::copy(&chunk, &orphan).unwrap();
        let t = Table::open(config(&path)).unwrap();
        assert_eq!(rows(&t), 5);
        assert!(!orphan.exists());
        drop(t);
        // A *listed* chunk that vanished is an error, and nothing is rewritten.
        let manifest_before = fs::read(path.join("MANIFEST")).unwrap();
        fs::rename(&chunk, dir.path().join("saved.tfl")).unwrap();
        assert!(matches!(Table::open(config(&path)), Err(Error::Corrupt(_))));
        assert_eq!(fs::read(path.join("MANIFEST")).unwrap(), manifest_before);
        fs::rename(dir.path().join("saved.tfl"), &chunk).unwrap();
        assert_eq!(rows(&Table::open(config(&path)).unwrap()), 5);
    }

    fn wal_segments(path: &Path) -> Vec<u64> {
        list_segments(path).unwrap()
    }

    fn tmp_files(path: &Path) -> Vec<String> {
        fs::read_dir(path)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect()
    }

    #[test]
    fn replay_sets_aside_values_no_chunk_can_hold_and_the_table_stays_usable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        drop(open_new(&path));
        REPLAY_VALUE_LIMIT.with(|l| l.set(1000));
        // What an older version accepted: a row whose (tag) value is too big to store.
        let mut w = Wal::create(&path, 2, None).unwrap();
        w.append(&wal::row_commit_entry(&row(1, "a"))).unwrap();
        w.append(&wal::row_commit_entry(&row(2, &"x".repeat(5000)))).unwrap();
        w.append(&wal::row_commit_entry(&row(3, "a"))).unwrap();
        w.sync(true).unwrap();
        drop(w);

        let t = Table::open(config(&path)).unwrap();
        assert_eq!(rows(&t), 2, "the storable rows are recovered");
        assert!(path.join("wal_000002.tfl.wal.oversized").exists(), "the segment holding the big row is kept");
        t.write(row(4, "a")).unwrap();
        t.flush().unwrap();
        assert_eq!(rows(&t), 3);
        drop(t);
        let t = Table::open(config(&path)).unwrap();
        assert_eq!(rows(&t), 3, "flushed, and not replayed (or set aside) again");
        assert!(wal_segments(&path).len() <= 2);
        REPLAY_VALUE_LIMIT.with(|l| l.set(MAX_REPLAY_VALUE_BYTES));
    }

    #[test]
    fn forged_metadata_near_u64_max_is_corruption_not_an_overflow() {
        let dir = tempfile::tempdir().unwrap();
        // A ROW of batch u64::MAX in the last segment.
        let path = dir.path().join("batch");
        drop(open_new(&path));
        let mut w = Wal::create(&path, 2, None).unwrap();
        w.append(&wal::row_entry(u64::MAX, &row(1, "a"))).unwrap();
        w.sync(true).unwrap();
        drop(w);
        assert!(matches!(Table::open(config(&path)), Err(Error::Corrupt(_))));

        // A MANIFEST whose WAL checkpoint is u64::MAX.
        let path = dir.path().join("walseq");
        drop(open_new(&path));
        Manifest { wal_seq: u64::MAX, replay_seq: u64::MAX, ..Manifest::empty() }.store(&path).unwrap();
        assert!(matches!(Table::open(config(&path)), Err(Error::Corrupt(_))));

        // A MANIFEST that has no chunk ids left: the flush fails cleanly.
        let path = dir.path().join("chunkid");
        drop(open_new(&path));
        Manifest { next_chunk_id: u64::MAX, ..Manifest::empty() }.store(&path).unwrap();
        let t = Table::open(config(&path)).unwrap();
        t.write(row(1, "a")).unwrap();
        assert!(matches!(t.flush(), Err(Error::Corrupt(_))));
        assert_eq!(rows(&t), 1, "the row is still there");
    }

    #[test]
    fn a_panic_under_the_table_lock_refuses_everything_until_reopened() {
        let dir = tempfile::tempdir().unwrap();
        let t = Arc::new(open_new(&dir.path().join("t")));
        t.write(row(1, "a")).unwrap();
        let t2 = t.clone();
        let joined = std::thread::spawn(move || {
            let _guard = t2.inner.lock().unwrap();
            panic!("simulated bug while holding the table state");
        })
        .join();
        assert!(joined.is_err() && t.inner.is_poisoned());
        // Half-updated state could show duplicates: everything refuses, at once.
        assert!(matches!(t.scan(&ScanFilter::default(), false), Err(Error::ReadOnly(m)) if m.contains("reopened")));
        assert!(matches!(t.stats(), Err(Error::ReadOnly(_))));
        assert!(matches!(t.write(row(2, "a")), Err(Error::ReadOnly(_))));
        assert!(matches!(t.sync_wal(true), Err(Error::ReadOnly(_))));
        assert!(matches!(t.flush(), Err(Error::ReadOnly(_))));
        drop(t);
        // Reopening recovers.
        let t = Table::open(config(&dir.path().join("t"))).unwrap();
        assert_eq!(rows(&t), 1);
    }

    #[test]
    fn reopening_an_idle_table_does_not_pile_up_wal_segments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        drop(open_new(&path));
        for _ in 0..6 {
            drop(Table::open(config(&path)).unwrap());
        }
        assert_eq!(wal_segments(&path).len(), 1, "{:?}", wal_segments(&path));

        // With unflushed rows the segments are still needed: nothing is lost
        // across reopenings, and one flush releases them all.
        let t = Table::open(config(&path)).unwrap();
        for i in 0..5 {
            t.write(row(i, "a")).unwrap();
        }
        t.sync_wal(true).unwrap();
        drop(t);
        for _ in 0..3 {
            let t = Table::open(config(&path)).unwrap();
            assert_eq!(rows(&t), 5);
            drop(t);
        }
        let t = Table::open(config(&path)).unwrap();
        t.flush().unwrap();
        assert_eq!(wal_segments(&path).len(), 1);
        drop(t);
        let t = Table::open(config(&path)).unwrap();
        assert_eq!(rows(&t), 5);
        assert_eq!(wal_segments(&path).len(), 1);
    }

    #[test]
    fn flushing_an_empty_memtable_collects_dead_wal_entries_but_not_an_open_batchs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = Arc::new(open_new(&path));
        // Rows of an aborted batch stay in the log as dead weight.
        let mut dead = t.begin_batch().unwrap();
        for i in 0..5 {
            dead.write(row(i, "a")).unwrap();
        }
        dead.abort();
        t.sync_wal(true).unwrap();
        assert_eq!(wal_segments(&path), vec![1]);
        t.flush().unwrap();
        assert_eq!(wal_segments(&path), vec![2], "the dead segment is gone");
        t.flush().unwrap();
        assert_eq!(wal_segments(&path), vec![2], "and flushing again changes nothing");

        // An open batch that logged rows keeps its segment.
        let mut open = t.begin_batch().unwrap();
        open.write(row(10, "b")).unwrap();
        t.sync_wal(true).unwrap();
        t.flush().unwrap();
        assert_eq!(wal_segments(&path), vec![2, 3]);
        open.commit(true).unwrap();
        assert_eq!(rows(&t), 1);
        t.flush().unwrap();
        assert_eq!(wal_segments(&path).len(), 1);
        drop(t);
        assert_eq!(rows(&Table::open(config(&path)).unwrap()), 1);
    }

    #[test]
    fn a_series_too_big_for_one_block_is_split_across_chunks_by_a_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = open_new(&path);
        crate::compaction::set_split_rows(Some(30));
        for i in 0..90 {
            t.write(row(i, "a")).unwrap();
        }
        for i in 0..10 {
            t.write(row(i, "b")).unwrap();
        }
        t.flush().unwrap();
        crate::compaction::set_split_rows(None);
        let chunks = t.chunks().unwrap();
        assert_eq!(chunks.len(), 3, "series a needs three parts");
        assert!(chunks.iter().all(|c| c.bucket == chunks[0].bucket));
        assert_eq!(chunks.iter().map(|c| c.id).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(rows(&t), 100);
        assert!(t.check().unwrap().is_empty());
        drop(t);
        assert_eq!(rows(&Table::open(config(&path)).unwrap()), 100);
    }

    #[test]
    fn failed_chunk_write_leaves_no_tmp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let t = open_new(&path);
        for i in 0..5 {
            t.write(row(i, "a")).unwrap();
        }
        fail_fsync_after(1); // the WAL sync passes, the chunk's fsync fails
        assert!(t.flush().is_err());
        restore_fsync();
        assert!(tmp_files(&path).is_empty(), "{:?}", tmp_files(&path));
    }

    #[test]
    fn a_batch_with_an_unstorable_row_is_withheld_whole_and_only_its_segments_are_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        drop(open_new(&path));
        REPLAY_VALUE_LIMIT.with(|l| l.set(1000));
        let write_seg = |seq: u64, entries: Vec<Vec<u8>>| {
            let mut w = Wal::create(&path, seq, None).unwrap();
            for e in entries {
                w.append(&e).unwrap();
            }
            w.sync(true).unwrap();
        };
        // Batch 7 spans segments 2 and 3 and holds a row that cannot be stored.
        write_seg(2, vec![wal::row_entry(7, &row(1, "a")), wal::row_entry(7, &row(2, &"x".repeat(5000)))]);
        write_seg(3, vec![wal::row_entry(7, &row(3, "a")), wal::commit_entry(7)]);
        write_seg(4, vec![wal::row_commit_entry(&row(4, "a")), wal::row_commit_entry(&row(5, "a"))]);
        let t = Table::open(config(&path)).unwrap();
        assert_eq!(rows(&t), 2, "only the unrelated rows; nothing of batch 7");
        let kept = |n: &str| path.join(format!("wal_{n}.tfl.wal.oversized")).exists();
        assert!(kept("000002") && kept("000003"), "segments with the batch's rows are preserved");
        assert!(!kept("000004"), "unrelated segments are not copied");
        drop(t);
        assert_eq!(rows(&Table::open(config(&path)).unwrap()), 2);
        REPLAY_VALUE_LIMIT.with(|l| l.set(MAX_REPLAY_VALUE_BYTES));
    }

    #[test]
    fn the_oversized_copy_is_atomic_and_a_damaged_one_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        drop(open_new(&path));
        REPLAY_VALUE_LIMIT.with(|l| l.set(1000));
        let mut w = Wal::create(&path, 2, None).unwrap();
        w.append(&wal::row_commit_entry(&row(1, "a"))).unwrap();
        w.append(&wal::row_commit_entry(&row(2, &"x".repeat(5000)))).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let original = fs::read(segment_path(&path, 2)).unwrap();

        // The copy dies in the middle: the open fails, the original stays, no stray files.
        COPY_FAILS_MIDWAY.with(|c| c.set(true));
        assert!(Table::open(config(&path)).is_err());
        COPY_FAILS_MIDWAY.with(|c| c.set(false));
        assert_eq!(fs::read(segment_path(&path, 2)).unwrap(), original);
        assert!(!path.join("wal_000002.tfl.wal.oversized").exists());

        // A truncated copy from a crash is not trusted: it is rewritten before
        // the checkpoint deletes the original.
        fs::write(path.join("wal_000002.tfl.wal.oversized"), &original[..10]).unwrap();
        let t = Table::open(config(&path)).unwrap();
        assert_eq!(rows(&t), 1);
        assert_eq!(fs::read(path.join("wal_000002.tfl.wal.oversized")).unwrap(), original);
        assert!(tmp_files(&path).is_empty());
        REPLAY_VALUE_LIMIT.with(|l| l.set(MAX_REPLAY_VALUE_BYTES));
    }
}
