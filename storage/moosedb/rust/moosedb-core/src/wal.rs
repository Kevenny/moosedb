//! Write-ahead log.
//!
//! The WAL is a sequence of segment files `wal_NNNNNN.tfl.wal`:
//!
//! ```text
//! segment := header entry*
//! header  := "TFWL" version:u16 flags:u16 [cipher params (32 bytes) if ENCRYPTED]
//! entry   := crc32:u32 len:u32 payload[len]     crc32 covers len || payload (as stored)
//! ```
//!
//! The payload is encrypted with AES-256-CTR (keystream offset = file offset)
//! when the table is encrypted. Appends are buffered; durability is reached
//! by `sync`, which the handler calls at statement end.
//!
//! Payload by segment version:
//!
//! ```text
//! v1: row                                  (every entry is committed)
//! v2: kind:u8 body
//!       1 ROW         batch_id:u64 row     row of a batch, not yet committed
//!       2 COMMIT      batch_id:u64         makes every ROW of the batch count
//!       3 ROW_COMMIT  row                  a one-row batch (plain `write`)
//! ```
//!
//! Batches of one table may interleave in the log. Replay applies a batch
//! only when it reaches its COMMIT; rows of batches without one are dropped.
//!
//! A segment number is also the checkpoint unit: the MANIFEST records
//! `wal_seq = N` once every batch committed in segments `<= N` is in sealed
//! chunks, and `replay_seq` (the first segment still needed, which trails
//! `N + 1` while an older batch is open). Segments `< replay_seq` are deleted.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::bytes::{put_u64, ByteReader};
use crate::codec::encode_row;
use crate::crypto::{CipherParams, PARAMS_LEN};
use crate::error::{corrupt, Error, Result};
use crate::fsutil::{self, sync_dir};
use crate::log::warn;
use crate::schema::Value;

const WAL_PREFIX: &str = "wal_";
const WAL_SUFFIX: &str = ".tfl.wal";
const MAGIC: &[u8; 4] = b"TFWL";
/// Version written by new segments; version 1 is still readable.
const VERSION: u16 = 2;
const K_ROW: u8 = 1;
const K_COMMIT: u8 = 2;
const K_ROW_COMMIT: u8 = 3;
const FLAG_ENCRYPTED: u16 = 1;
const BASE_HEADER: usize = 8;
const ENTRY_HEADER: usize = 8;
/// Sanity bound on a single entry; anything larger is treated as corruption.
pub(crate) const MAX_ENTRY: usize = 1 << 30;

#[cfg(test)]
thread_local! {
    static ENTRY_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(MAX_ENTRY) };
}

/// Largest entry accepted by `append` (lowerable per thread in tests).
pub(crate) fn max_entry() -> usize {
    #[cfg(test)]
    return ENTRY_LIMIT.with(std::cell::Cell::get);
    #[cfg(not(test))]
    MAX_ENTRY
}

/// Test hook: lowers the entry limit of the current thread.
#[cfg(test)]
pub(crate) fn set_max_entry(n: usize) {
    ENTRY_LIMIT.with(|l| l.set(n));
}

/// Payload of a ROW entry (a row of batch `batch`, not yet committed), into a
/// reused buffer (cleared first).
pub(crate) fn row_entry_into(p: &mut Vec<u8>, batch: u64, row: &[Value]) {
    p.clear();
    p.push(K_ROW);
    put_u64(p, batch);
    encode_row(p, row);
}

/// [`row_entry_into`] into a fresh buffer.
#[cfg(test)]
pub(crate) fn row_entry(batch: u64, row: &[Value]) -> Vec<u8> {
    let mut p = Vec::with_capacity(9 + 16 * row.len());
    row_entry_into(&mut p, batch, row);
    p
}

/// Payload of a ROW_COMMIT entry (a row that is its own committed batch).
pub(crate) fn row_commit_entry(row: &[Value]) -> Vec<u8> {
    let mut p = Vec::with_capacity(1 + 16 * row.len());
    p.push(K_ROW_COMMIT);
    encode_row(&mut p, row);
    p
}

/// Payload of a COMMIT entry.
pub(crate) fn commit_entry(batch: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(9);
    p.push(K_COMMIT);
    put_u64(&mut p, batch);
    p
}

enum Entry<'a> {
    Row(u64, &'a [u8]),
    Commit(u64),
    RowCommit(&'a [u8]),
}

fn parse_entry(version: u16, payload: &[u8]) -> Result<Entry<'_>> {
    if version == 1 {
        return Ok(Entry::RowCommit(payload));
    }
    let mut r = ByteReader::new(payload);
    match r.u8()? {
        K_ROW => {
            let batch = r.u64()?;
            Ok(Entry::Row(batch, &payload[r.position()..]))
        }
        K_COMMIT => Ok(Entry::Commit(r.u64()?)),
        K_ROW_COMMIT => Ok(Entry::RowCommit(&payload[1..])),
        k => Err(corrupt(format!("WAL entry of unknown kind {k}"))),
    }
}

pub(crate) fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{WAL_PREFIX}{seq:06}{WAL_SUFFIX}"))
}

pub(crate) fn parse_segment_name(name: &str) -> Option<u64> {
    name.strip_prefix(WAL_PREFIX)?.strip_suffix(WAL_SUFFIX)?.parse().ok()
}

/// Sorted sequence numbers of the WAL segments present in `dir`.
pub(crate) fn list_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(seq) = entry.file_name().to_str().and_then(parse_segment_name) {
            seqs.push(seq);
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}

pub(crate) struct Wal {
    seq: u64,
    out: BufWriter<File>,
    cipher: Option<CipherParams>,
    /// Logical file offset of the next byte to append.
    offset: u64,
    /// Size of the segment header: `offset == header_len` means no entries.
    header_len: u64,
    /// Whether bytes were appended since the last `sync`.
    dirty: bool,
    /// Set by the first failed write/flush/fsync. The buffered data may be
    /// lost or half-written and a later fsync could report success without
    /// the data being durable, so the segment never accepts or confirms
    /// anything again.
    failed: bool,
}

impl Wal {
    /// Creates a brand-new, empty segment (header fsynced). Fails if it exists.
    pub(crate) fn create(dir: &Path, seq: u64, encryption_key: Option<u32>) -> Result<Wal> {
        let cipher = encryption_key.map(CipherParams::for_new_file).transpose()?;
        let path = segment_path(dir, seq);
        let mut file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        let mut header = Vec::with_capacity(BASE_HEADER + PARAMS_LEN);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&(if cipher.is_some() { FLAG_ENCRYPTED } else { 0 }).to_le_bytes());
        if let Some(c) = &cipher {
            header.extend_from_slice(&c.encode());
        }
        let written = file.write_all(&header).map_err(Error::from).and_then(|()| fsutil::sync_all(&file));
        if let Err(e) = written {
            // A segment with a partial header is useless; do not leave it behind.
            drop(file);
            let _ = fs::remove_file(&path);
            return Err(e);
        }
        sync_dir(dir)?;
        Ok(Wal {
            seq,
            out: BufWriter::with_capacity(1 << 20, file),
            cipher,
            offset: header.len() as u64,
            header_len: header.len() as u64,
            dirty: false,
            failed: false,
        })
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// Whether nothing was appended since the segment was created.
    pub(crate) fn is_empty(&self) -> bool {
        self.offset == self.header_len
    }

    pub(crate) fn append(&mut self, payload: &[u8]) -> Result<()> {
        self.check_usable()?;
        let len = u32::try_from(payload.len())
            .ok()
            .filter(|&l| (l as usize) <= max_entry())
            .ok_or_else(|| crate::error::invalid("WAL entry too large"))?;
        let len_bytes = len.to_le_bytes();
        let mut stored;
        let body = match &self.cipher {
            Some(c) => {
                stored = payload.to_vec();
                c.apply(&mut stored, self.offset + ENTRY_HEADER as u64);
                &stored[..]
            }
            None => payload,
        };
        let mut h = crc32fast::Hasher::new();
        h.update(&len_bytes);
        h.update(body);
        let written = self
            .out
            .write_all(&h.finalize().to_le_bytes())
            .and_then(|()| self.out.write_all(&len_bytes))
            .and_then(|()| self.out.write_all(body));
        if let Err(e) = written {
            self.failed = true;
            return Err(e.into());
        }
        self.offset += (ENTRY_HEADER + body.len()) as u64;
        self.dirty = true;
        Ok(())
    }

    /// Pushes buffered entries to the OS and, if `durable`, to stable storage.
    pub(crate) fn sync(&mut self, durable: bool) -> Result<()> {
        self.check_usable()?;
        if !self.dirty {
            return Ok(());
        }
        let synced = self.out.flush().map_err(Error::from).and_then(|()| {
            if durable {
                fsutil::sync_data(self.out.get_ref())
            } else {
                Ok(())
            }
        });
        if let Err(e) = synced {
            self.failed = true;
            return Err(e);
        }
        self.dirty = false;
        Ok(())
    }

    fn check_usable(&self) -> Result<()> {
        if self.failed {
            return Err(Error::ReadOnly("WAL segment failed earlier; the table must be reopened".into()));
        }
        Ok(())
    }
}

/// What to do with an invalid tail at the end of a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TornTail {
    /// Last segment during recovery: cut the garbage off.
    Truncate,
    /// A segment followed by newer ones: invalid bytes mean corruption.
    Error,
    /// Read-only inspection: count what is valid, change nothing.
    Ignore,
}

pub(crate) struct ReplayOutcome {
    /// The segment never held anything (header missing or incomplete): safe to delete.
    pub unused: bool,
    pub entries: u64,
    /// Bytes at the end of the segment that did not form a valid entry.
    pub torn_bytes: u64,
    /// Length of the valid prefix (header and complete, checked entries).
    pub valid_len: u64,
}

/// Feeds every valid entry of a segment to `apply`, in order.
///
/// The segment is read as a stream, one entry at a time (memory is the
/// largest entry, never the segment). An entry's claimed length must fit in
/// what the file still holds, so a forged length cannot reserve more than the
/// file really contains.
///
/// A segment may end in a torn (partially written) entry — the expected
/// state after a crash mid-append. With `truncate_torn` the garbage tail is
/// cut off; otherwise (a segment followed by newer ones) it is corruption.
/// A segment whose header itself is incomplete was never used and holds no
/// rows.
///
/// `end` limits the read to the first `end` bytes of the file (the valid
/// length found by an earlier pass), so a segment that grows meanwhile is
/// seen the same way twice.
pub(crate) fn replay(
    dir: &Path,
    seq: u64,
    torn: TornTail,
    end: Option<u64>,
    mut apply: impl FnMut(u16, &[u8]) -> Result<()>,
) -> Result<ReplayOutcome> {
    replay_at(dir, seq, torn, end, |version, _, payload| apply(version, payload))
}

/// [`replay`] that also tells `apply` where each entry starts in the file
/// (so it can be read again later without keeping its payload).
pub(crate) fn replay_at(
    dir: &Path,
    seq: u64,
    torn: TornTail,
    end: Option<u64>,
    mut apply: impl FnMut(u16, u64, &[u8]) -> Result<()>,
) -> Result<ReplayOutcome> {
    let path = segment_path(dir, seq);
    let mut file = File::open(&path)?;
    let on_disk = file.metadata()?.len();
    let file_len = end.map_or(on_disk, |e| e.min(on_disk));
    let max_header = (BASE_HEADER + PARAMS_LEN) as u64;
    let mut prefix = Vec::with_capacity(max_header as usize);
    Read::by_ref(&mut file).take(max_header).read_to_end(&mut prefix)?;

    // A crash while the header was being written leaves a short (or, on some
    // file systems, zero-filled) file. Nothing was ever appended to it, in any
    // position among the segments: it is empty, never corruption.
    let never_used = |n: u64| ReplayOutcome { unused: true, entries: 0, torn_bytes: n, valid_len: 0 };
    let header_ok = prefix.len() >= BASE_HEADER && &prefix[..4] == MAGIC;
    if !header_ok {
        if file_len < BASE_HEADER as u64 || (file_len <= max_header && prefix.iter().all(|&b| b == 0)) {
            return Ok(never_used(file_len));
        }
        return Err(corrupt(format!("{} has an invalid header", path.display())));
    }
    let mut r = ByteReader::new(&prefix);
    r.take(4)?;
    let version = r.u16()?;
    let flags = r.u16()?;
    if version != VERSION && version != 1 {
        return Err(corrupt(format!("{}: WAL version {version} not supported", path.display())));
    }
    let mut header_len = BASE_HEADER as u64;
    let cipher = if flags & FLAG_ENCRYPTED != 0 {
        crate::crypto::ensure_available()?;
        header_len = max_header;
        // A segment with a header and nothing else holds no data, so its
        // key is irrelevant: do not ask for it. Otherwise a retired key
        // version would make an idle table (whose only WAL file is the
        // empty segment its last open created) impossible to open.
        if file_len < max_header {
            return Ok(never_used(file_len));
        }
        if file_len == max_header {
            return Ok(never_used(0));
        }
        Some(CipherParams::decode(r.take(PARAMS_LEN)?)?)
    } else {
        None
    };

    file.seek(SeekFrom::Start(header_len))?;
    let mut input = BufReader::with_capacity(1 << 20, file);
    let mut pos = header_len;
    let mut entries = 0u64;
    let mut body = Vec::new();
    let mut plain = Vec::new();
    while file_len - pos >= ENTRY_HEADER as u64 {
        let mut head = [0u8; ENTRY_HEADER];
        input.read_exact(&mut head)?;
        let crc = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);
        let len = u32::from_le_bytes([head[4], head[5], head[6], head[7]]) as usize;
        if len > MAX_ENTRY || len as u64 > file_len - pos - ENTRY_HEADER as u64 {
            break;
        }
        body.clear();
        // Grows with the bytes actually read.
        Read::by_ref(&mut input).take(len as u64).read_to_end(&mut body)?;
        if body.len() != len {
            break;
        }
        let mut h = crc32fast::Hasher::new();
        h.update(&head[4..8]);
        h.update(&body);
        if h.finalize() != crc {
            break;
        }
        let payload = match &cipher {
            Some(c) => {
                plain.clear();
                plain.extend_from_slice(&body);
                c.apply(&mut plain, pos + ENTRY_HEADER as u64);
                &plain[..]
            }
            None => &body[..],
        };
        apply(version, pos, payload)?;
        entries += 1;
        pos += (ENTRY_HEADER + len) as u64;
    }
    drop(input);

    let torn_bytes = file_len - pos;
    if torn_bytes > 0 {
        if torn == TornTail::Error {
            return Err(corrupt(format!(
                "WAL segment {} has {torn_bytes} invalid bytes before later segments",
                path.display()
            )));
        }
        if torn == TornTail::Truncate {
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(pos)?;
            f.sync_all()?;
        }
    }
    Ok(ReplayOutcome { unused: false, entries, torn_bytes, valid_len: pos })
}

pub(crate) struct Replayed {
    /// Highest batch id seen in the replayed segments (0 if none): new batches
    /// must use larger ids, or a COMMIT could adopt the dropped rows of an
    /// old uncommitted batch that still sits in a retained segment.
    pub max_batch_id: u64,
    /// Committed batches (or single rows) that `reject` vetoed: none of their
    /// rows were handed to `on_row`.
    pub rejected: Vec<Rejected>,
}

/// A committed batch held back whole because one of its rows was vetoed.
pub(crate) struct Rejected {
    /// `None` for a single-row commit.
    pub batch: Option<u64>,
    pub rows: usize,
    /// Total payload bytes of its rows.
    pub bytes: usize,
    /// Segments that hold its rows.
    pub segments: std::collections::BTreeSet<u64>,
}

/// Memory ceilings of a replay. The log is untrusted input: without them a
/// forged file would make recovery buffer all of it (an OOM aborts `mysqld`).
/// Exceeding one is corruption ("refusing to open"), never a truncation.
///
/// Row *payloads* are never buffered: a batch waiting for its COMMIT is kept
/// as `(segment, offset)` pairs (16 bytes a row) and its rows are read again
/// from the WAL, still encrypted on disk, when the COMMIT arrives. So no
/// amount of legitimate WAL (any batch memory budget, any number of
/// concurrent batches) can exceed a limit; only the row *count* is bounded.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplayLimits {
    /// Rows (of batches that will commit) remembered while waiting for their COMMIT.
    pub pending_rows: usize,
    /// MemTable bytes (`row_bytes` accounting) replay may build; checked by
    /// the caller that owns the MemTable.
    pub applied: usize,
    /// COMMIT entries remembered by the first pass.
    pub commits: usize,
}

impl ReplayLimits {
    /// What a legitimate WAL can need, from the writer's own bounds:
    /// * pending rows end up applied, and the MemTable that takes them is
    ///   capped by the writer (it flushes at the threshold and refuses
    ///   writes from twice that, plus one batch of at most `MAX_MEMTABLE_SIZE`
    ///   and one 128 MiB row): `applied` = four times the maximum plus a
    ///   margin, independent of the batch budget in force when it was written.
    ///   The smallest row costs 56 bytes there, so a full MemTable holds at
    ///   most ~7.5M rows; `pending_rows` allows twice that (16 bytes each);
    /// * a commit entry is 25 bytes on disk and the first pass keeps about 16
    ///   bytes of it in a `HashSet<u64>`: eight million (~128 MiB) cover it.
    ///
    /// The margins are deliberately generous: a limit that is too low turns
    /// a good log into a table that cannot be opened.
    pub(crate) fn standard() -> ReplayLimits {
        let max_memtable = crate::options::MAX_MEMTABLE_SIZE as usize;
        ReplayLimits { pending_rows: 16 << 20, applied: 4 * max_memtable + (128 << 20), commits: 8 << 20 }
    }
}

/// Rows of a batch whose COMMIT has not been replayed yet.
#[derive(Default)]
struct PendingBatch {
    /// `(segment, entry offset)` of each row; emptied once the batch is vetoed.
    rows: Vec<(u64, u64)>,
    /// A row was vetoed: the batch will be withheld whole, so it keeps only
    /// the counters below.
    vetoed: bool,
    row_count: usize,
    payload_bytes: usize,
    segments: std::collections::BTreeSet<u64>,
}

/// Reads single entries back by offset (one open segment at a time; rows of a
/// batch come in segment order).
struct RowReader<'a> {
    dir: &'a Path,
    cur: Option<(u64, File, u16, Option<CipherParams>)>,
    body: Vec<u8>,
}

impl RowReader<'_> {
    /// The row payload of the ROW entry at `pos` of segment `seq`.
    fn row(&mut self, seq: u64, pos: u64) -> Result<&[u8]> {
        if self.cur.as_ref().map(|c| c.0) != Some(seq) {
            let mut f = File::open(segment_path(self.dir, seq))?;
            let mut head = Vec::with_capacity(BASE_HEADER + PARAMS_LEN);
            Read::by_ref(&mut f).take((BASE_HEADER + PARAMS_LEN) as u64).read_to_end(&mut head)?;
            let changed = || corrupt(format!("WAL segment {seq} changed while it was being replayed"));
            if head.len() < BASE_HEADER {
                return Err(changed());
            }
            let version = u16::from_le_bytes([head[4], head[5]]);
            let flags = u16::from_le_bytes([head[6], head[7]]);
            let cipher = if flags & FLAG_ENCRYPTED != 0 {
                Some(CipherParams::decode(head.get(BASE_HEADER..).ok_or_else(changed)?)?)
            } else {
                None
            };
            self.cur = Some((seq, f, version, cipher));
        }
        let Some((_, f, version, cipher)) = self.cur.as_mut() else { return Err(corrupt("no WAL segment open")) };
        let changed = || corrupt(format!("WAL segment {seq} changed while it was being replayed"));
        f.seek(SeekFrom::Start(pos))?;
        let mut head = [0u8; ENTRY_HEADER];
        f.read_exact(&mut head).map_err(|_| changed())?;
        let crc = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);
        let len = u32::from_le_bytes([head[4], head[5], head[6], head[7]]) as usize;
        if len > MAX_ENTRY {
            return Err(changed());
        }
        self.body.clear();
        Read::by_ref(f).take(len as u64).read_to_end(&mut self.body)?;
        let mut h = crc32fast::Hasher::new();
        h.update(&head[4..8]);
        h.update(&self.body);
        if self.body.len() != len || h.finalize() != crc {
            return Err(changed());
        }
        if let Some(c) = cipher {
            c.apply(&mut self.body, pos + ENTRY_HEADER as u64);
        }
        let version = *version;
        let payload = &self.body[..];
        match parse_entry(version, payload)? {
            Entry::Row(_, _) => Ok(&self.body[row_offset(version)..]),
            _ => Err(changed()),
        }
    }
}

/// True if segment `seq` never received an entry: its header was never
/// completed (short or zero-filled file) or it is an encrypted header with
/// nothing after it. Mirrors the "never used" cases of [`replay_at`] without
/// asking for a key or reading entries.
fn segment_has_no_entries(dir: &Path, seq: u64) -> Result<bool> {
    let mut file = File::open(segment_path(dir, seq))?;
    let len = file.metadata()?.len();
    let max_header = (BASE_HEADER + PARAMS_LEN) as u64;
    let mut prefix = Vec::with_capacity(max_header as usize);
    Read::by_ref(&mut file).take(max_header).read_to_end(&mut prefix)?;
    if prefix.len() < BASE_HEADER || &prefix[..4] != MAGIC {
        return Ok(len < BASE_HEADER as u64 || (len <= max_header && prefix.iter().all(|&b| b == 0)));
    }
    let flags = u16::from_le_bytes([prefix[6], prefix[7]]);
    Ok(if flags & FLAG_ENCRYPTED != 0 { len <= max_header } else { len <= BASE_HEADER as u64 })
}

/// Where the row starts inside a ROW payload (kind byte + batch id).
fn row_offset(version: u16) -> usize {
    if version == 1 {
        0
    } else {
        9
    }
}

/// Replays `segs` (ascending) and hands every *committed* row to `on_row`
/// as `(segment, payload)`, in commit order.
///
/// Two streaming passes, so nothing that will never apply is ever buffered
/// (the dead rows of aborted or spilled batches can be most of a WAL):
///
/// 1. reads every entry and notes only the batches whose COMMIT lies after
///    `flushed_seq` (and the highest batch id). With `recover`, a torn tail
///    of the last segment is cut off and never-used segments are deleted;
///    otherwise the files are left untouched;
/// 2. reads again and buffers the rows of exactly those batches until their
///    COMMIT, then applies them. Commits at or before `flushed_seq` are
///    already in chunks and are skipped (their rows may still be read: the
///    first rows of a batch can precede the flush point).
///
/// `reject` is asked about every buffered row as it is read; if it says yes
/// for any row, the whole batch is withheld (all or nothing) and reported in
/// `Replayed::rejected` (keeping only its counters). Buffered bytes stay
/// within `limits`.
pub(crate) fn replay_committed(
    dir: &Path,
    segs: &[u64],
    flushed_seq: u64,
    recover: bool,
    limits: &ReplayLimits,
    reject: &dyn Fn(&[u8]) -> bool,
    mut on_row: impl FnMut(u64, &[u8]) -> Result<()>,
) -> Result<Replayed> {
    let too_much = |what: &str, seq: u64, used: usize, limit: usize| {
        corrupt(format!(
            "WAL segment {seq}: {what} ({used}) exceeds the replay limit of {limit}; \
             refusing to load it (forged or damaged log)"
        ))
    };

    // The last segment that ever received an entry is the one whose torn
    // tail is a crash artifact. Later segments without any entry (header
    // never completed, or an encrypted header and nothing else) cannot have
    // been relied upon, so they do not make an earlier tail "corrupt".
    let last_used = if recover {
        let mut last = segs.len().saturating_sub(1);
        while last > 0 && segment_has_no_entries(dir, segs[last])? {
            last -= 1;
        }
        last
    } else {
        segs.len().saturating_sub(1)
    };

    // Pass 1.
    let mut live: Vec<(u64, u64)> = Vec::with_capacity(segs.len());
    let mut commits: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut max_batch_id = 0u64;
    for (i, &seq) in segs.iter().enumerate() {
        let torn = match (recover, i >= last_used) {
            (false, _) => TornTail::Ignore,
            (true, true) => TornTail::Truncate,
            (true, false) => TornTail::Error,
        };
        let outcome = replay(dir, seq, torn, None, |version, payload| {
            match parse_entry(version, payload)? {
                Entry::Row(batch, _) => max_batch_id = max_batch_id.max(batch),
                Entry::Commit(batch) => {
                    max_batch_id = max_batch_id.max(batch);
                    if seq > flushed_seq {
                        commits.insert(batch);
                        if commits.len() > limits.commits {
                            return Err(too_much("the commits to replay", seq, commits.len(), limits.commits));
                        }
                    }
                }
                Entry::RowCommit(_) => {}
            }
            Ok(())
        })?;
        if recover {
            if outcome.unused {
                // Header never completed (crash right after creating it).
                fsutil::remove_if_exists(&segment_path(dir, seq))?;
            }
            if outcome.torn_bytes > 0 {
                warn(&format!(
                    "WAL segment {seq}: discarded {} torn bytes after {} valid entries",
                    outcome.torn_bytes, outcome.entries
                ));
            }
        }
        if !outcome.unused {
            live.push((seq, outcome.valid_len));
        }
    }

    // Pass 2.
    let mut rejected: Vec<Rejected> = Vec::new();
    let mut pending: HashMap<u64, PendingBatch> = HashMap::new();
    let mut pending_rows = 0usize;
    let mut dead_rows = 0u64;
    let mut reader = RowReader { dir, cur: None, body: Vec::new() };
    for &(seq, valid_len) in &live {
        replay_at(dir, seq, TornTail::Ignore, Some(valid_len), |version, pos, payload| {
            match parse_entry(version, payload)? {
                Entry::Row(batch, row) => {
                    if !commits.contains(&batch) {
                        // Never commits (or already flushed): nothing to keep.
                        dead_rows += 1;
                        return Ok(());
                    }
                    let p = pending.entry(batch).or_default();
                    p.row_count += 1;
                    p.payload_bytes += row.len();
                    p.segments.insert(seq);
                    if !p.vetoed && reject(row) {
                        // Withheld whole: keep the counters, free the offsets.
                        p.vetoed = true;
                        pending_rows -= p.rows.len();
                        p.rows = Vec::new();
                    }
                    if !p.vetoed {
                        p.rows.push((seq, pos));
                        pending_rows += 1;
                        if pending_rows > limits.pending_rows {
                            return Err(too_much(
                                "the rows of batches waiting for their COMMIT",
                                seq,
                                pending_rows,
                                limits.pending_rows,
                            ));
                        }
                    }
                }
                Entry::Commit(batch) => {
                    if seq <= flushed_seq {
                        return Ok(());
                    }
                    // Rows of a batch that appear after its COMMIT never count.
                    commits.remove(&batch);
                    let p = pending.remove(&batch).ok_or_else(|| {
                        corrupt(format!("WAL segment {seq}: COMMIT of batch {batch} without its rows"))
                    })?;
                    pending_rows -= p.rows.len();
                    if p.vetoed {
                        rejected.push(Rejected {
                            batch: Some(batch),
                            rows: p.row_count,
                            bytes: p.payload_bytes,
                            segments: p.segments,
                        });
                    } else {
                        for &(rseg, rpos) in &p.rows {
                            on_row(seq, reader.row(rseg, rpos)?)?;
                        }
                    }
                }
                Entry::RowCommit(row) => {
                    if seq > flushed_seq {
                        if reject(row) {
                            rejected.push(Rejected {
                                batch: None,
                                rows: 1,
                                bytes: row.len(),
                                segments: std::iter::once(seq).collect(),
                            });
                        } else {
                            on_row(seq, row)?;
                        }
                    }
                }
            }
            Ok(())
        })?;
    }
    if recover && dead_rows > 0 {
        warn(&format!("WAL replay: discarded {dead_rows} row(s) of batches that never committed or are in chunks"));
    }
    Ok(Replayed { max_batch_id, rejected })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reusable-buffer encoder writes exactly the WAL v2 ROW payload layout:
    /// `K_ROW`, batch id (u64 LE), column count (u16 LE), tagged values.
    #[test]
    fn row_entry_into_matches_the_v2_layout_byte_for_byte() {
        let row =
            vec![Value::Timestamp(-5), Value::Bytes(b"ab".to_vec()), Value::Float64(1.5), Value::Null, Value::Int(7)];
        let mut want = vec![K_ROW];
        want.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        want.extend_from_slice(&5u16.to_le_bytes());
        want.push(1);
        want.extend_from_slice(&(-5i64).to_le_bytes());
        want.push(6);
        want.extend_from_slice(&2u32.to_le_bytes());
        want.extend_from_slice(b"ab");
        want.push(3);
        want.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
        want.push(0);
        want.push(2);
        want.extend_from_slice(&7i64.to_le_bytes());
        let mut buf = vec![9u8; 100]; // stale contents are discarded
        row_entry_into(&mut buf, 0x0102_0304_0506_0708, &row);
        assert_eq!(buf, want);
        assert_eq!(row_entry(0x0102_0304_0506_0708, &row), want);
    }

    fn collect(dir: &Path, seq: u64, truncate: bool) -> Result<(Vec<Vec<u8>>, ReplayOutcome)> {
        let mut got = Vec::new();
        let out = replay(dir, seq, if truncate { TornTail::Truncate } else { TornTail::Error }, None, |_, p| {
            got.push(p.to_vec());
            Ok(())
        })?;
        Ok((got, out))
    }

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(b"hello").unwrap();
        w.append(b"").unwrap();
        w.append(b"world").unwrap();
        w.sync(true).unwrap();
        let (got, out) = collect(dir.path(), 1, false).unwrap();
        assert_eq!(got, vec![b"hello".to_vec(), vec![], b"world".to_vec()]);
        assert_eq!(out.torn_bytes, 0);
        assert_eq!(list_segments(dir.path()).unwrap(), vec![1]);
        assert!(Wal::create(dir.path(), 1, None).is_err(), "segments are never reused");
    }

    #[test]
    fn torn_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 3, None).unwrap();
        w.append(b"one").unwrap();
        w.append(b"two").unwrap();
        w.sync(true).unwrap();
        drop(w);
        let path = segment_path(dir.path(), 3);
        let full = fs::metadata(&path).unwrap().len();
        OpenOptions::new().write(true).open(&path).unwrap().set_len(full - 2).unwrap();

        assert!(collect(dir.path(), 3, false).is_err(), "torn data in a non-final segment is corruption");
        let (got, out) = collect(dir.path(), 3, true).unwrap();
        assert_eq!(got, vec![b"one".to_vec()]);
        assert_eq!(out.torn_bytes, 9);
        assert_eq!(fs::metadata(&path).unwrap().len(), (BASE_HEADER + 11) as u64);
    }

    #[test]
    fn bit_flip_stops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(b"aaaa").unwrap();
        w.append(b"bbbb").unwrap();
        w.sync(true).unwrap();
        drop(w);
        let path = segment_path(dir.path(), 1);
        let mut data = fs::read(&path).unwrap();
        data[BASE_HEADER + ENTRY_HEADER + 4 + ENTRY_HEADER] ^= 0x01;
        fs::write(&path, &data).unwrap();
        let (got, _) = collect(dir.path(), 1, true).unwrap();
        assert_eq!(got, vec![b"aaaa".to_vec()]);
    }

    #[test]
    fn empty_or_headerless_segment() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(segment_path(dir.path(), 7), b"TF").unwrap();
        let (got, out) = collect(dir.path(), 7, true).unwrap();
        assert!(got.is_empty());
        assert_eq!(out.torn_bytes, 2);
        fs::write(segment_path(dir.path(), 8), b"garbage-garbage").unwrap();
        assert!(collect(dir.path(), 8, true).is_err());
    }

    #[test]
    fn partial_header_segments_are_unused_in_any_position() {
        let dir = tempfile::tempdir().unwrap();
        for (seq, bytes) in [(1u64, &b""[..]), (2, b"TFW"), (3, &[0u8; 20][..])] {
            fs::write(segment_path(dir.path(), seq), bytes).unwrap();
            // Even when newer segments exist (strict mode), it is just empty.
            let (got, out) = collect(dir.path(), seq, false).unwrap();
            assert!(got.is_empty() && out.unused, "segment of {} bytes", bytes.len());
        }
    }

    #[test]
    fn failed_header_write_leaves_no_segment() {
        let dir = tempfile::tempdir().unwrap();
        fsutil::fail_fsync_after(0);
        let r = Wal::create(dir.path(), 1, None);
        fsutil::restore_fsync();
        assert!(r.is_err());
        assert!(list_segments(dir.path()).unwrap().is_empty());
        assert!(Wal::create(dir.path(), 1, None).is_ok());
    }

    #[test]
    fn fsync_failure_is_sticky() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(b"row").unwrap();
        fsutil::fail_fsync_after(0);
        let first = w.sync(true);
        fsutil::restore_fsync();
        assert!(first.is_err());
        assert!(fsutil::is_fsync_error(&first.unwrap_err()));
        // The disk "recovered", but success can no longer be vouched for.
        assert!(w.sync(true).is_err());
        assert!(w.sync(false).is_err());
        assert!(w.append(b"more").is_err());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_segments() {
        crate::crypto::test_keys::install();
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, Some(5)).unwrap();
        w.append(b"secret row one").unwrap();
        w.append(b"secret row two").unwrap();
        w.sync(true).unwrap();
        drop(w);
        let raw = fs::read(segment_path(dir.path(), 1)).unwrap();
        assert!(!raw.windows(6).any(|w| w == b"secret"), "plaintext leaked into the WAL");
        let (got, _) = collect(dir.path(), 1, true).unwrap();
        assert_eq!(got, vec![b"secret row one".to_vec(), b"secret row two".to_vec()]);
    }

    fn r(i: i64) -> Vec<Value> {
        vec![Value::Timestamp(i)]
    }

    fn committed(dir: &Path, segs: &[u64], flushed: u64) -> (Vec<(u64, Vec<u8>)>, u64) {
        let mut got = Vec::new();
        let out = replay_committed(dir, segs, flushed, true, &ReplayLimits::standard(), &|_| false, |seq, p| {
            got.push((seq, p.to_vec()));
            Ok(())
        })
        .unwrap();
        (got, out.max_batch_id)
    }

    fn enc(i: i64) -> Vec<u8> {
        let mut b = Vec::new();
        encode_row(&mut b, &r(i));
        b
    }

    #[test]
    fn interleaved_batches_apply_only_at_commit_in_commit_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(&row_entry(1, &r(10))).unwrap();
        w.append(&row_entry(2, &r(20))).unwrap();
        w.append(&row_commit_entry(&r(30))).unwrap();
        w.append(&row_entry(1, &r(11))).unwrap();
        w.append(&commit_entry(1)).unwrap(); // batch 2 never commits
        w.sync(true).unwrap();
        drop(w);
        let (got, max) = committed(dir.path(), &[1], 0);
        let want: Vec<_> = [30, 10, 11].iter().map(|&i| (1, enc(i))).collect();
        assert_eq!(got, want);
        assert_eq!(max, 2);
    }

    #[test]
    fn commits_before_the_flush_point_are_skipped_but_open_batches_keep_their_early_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(&row_entry(1, &r(1))).unwrap(); // batch 1 spans the flush point
        w.append(&row_entry(2, &r(2))).unwrap();
        w.append(&commit_entry(2)).unwrap(); // flushed already
        w.sync(true).unwrap();
        drop(w);
        let mut w = Wal::create(dir.path(), 2, None).unwrap();
        w.append(&row_entry(1, &r(3))).unwrap();
        w.append(&commit_entry(1)).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let (got, _) = committed(dir.path(), &[1, 2], 1);
        assert_eq!(got, vec![(2, enc(1)), (2, enc(3))]);
    }

    /// Power-loss finding: a torn tail in segment N followed only by segments
    /// that never received an entry must be cut off like a last-segment tail,
    /// not make the table unopenable. A tail followed by real entries stays
    /// an error.
    #[test]
    fn a_torn_tail_before_segments_without_entries_is_truncated() {
        let stubs: [&dyn Fn(&Path); 4] = [
            &|d| fs::write(segment_path(d, 2), b"").unwrap(),
            &|d| fs::write(segment_path(d, 2), b"TFW").unwrap(),
            &|d| fs::write(segment_path(d, 2), [0u8; 20]).unwrap(),
            &|d| drop(Wal::create(d, 2, None).unwrap()), // header only
        ];
        for (k, stub) in stubs.iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let mut w = Wal::create(dir.path(), 1, None).unwrap();
            w.append(&row_commit_entry(&r(1))).unwrap();
            w.append(&row_entry(5, &r(2))).unwrap();
            w.append(&commit_entry(5)).unwrap();
            w.sync(true).unwrap();
            drop(w);
            let mut f = fs::OpenOptions::new().append(true).open(segment_path(dir.path(), 1)).unwrap();
            f.write_all(&[0xAB, 0xCD, 0x01, 0x00, 0x77]).unwrap(); // torn entry prefix
            drop(f);
            stub(dir.path());
            let (got, _) = committed(dir.path(), &[1, 2], 0);
            assert_eq!(got, vec![(1, enc(1)), (1, enc(2))], "stub variant {k}");
            // The cut is durable: a second recovery sees a clean segment.
            let (again, _) = committed(dir.path(), &list_segments(dir.path()).unwrap(), 0);
            assert_eq!(again, got, "stub variant {k}, second recovery");
        }

        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(&row_commit_entry(&r(1))).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let mut f = fs::OpenOptions::new().append(true).open(segment_path(dir.path(), 1)).unwrap();
        f.write_all(&[0xAB, 0xCD, 0x01, 0x00, 0x77]).unwrap();
        drop(f);
        let mut w = Wal::create(dir.path(), 2, None).unwrap();
        w.append(&row_commit_entry(&r(9))).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let err = replay_committed(dir.path(), &[1, 2], 0, true, &ReplayLimits::standard(), &|_| false, |_, _| Ok(()))
            .err()
            .unwrap();
        assert!(matches!(err, Error::Corrupt(_)), "a tail followed by real entries is corruption");
    }

    #[test]
    fn commit_without_rows_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(&commit_entry(9)).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let err = replay_committed(dir.path(), &[1], 0, true, &ReplayLimits::standard(), &|_| false, |_, _| Ok(()))
            .err()
            .unwrap();
        assert!(matches!(err, Error::Corrupt(_)));
    }

    #[test]
    fn version_1_segments_replay_as_committed_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&0u16.to_le_bytes());
        for i in [5, 6] {
            let body = enc(i);
            let len = (body.len() as u32).to_le_bytes();
            let mut h = crc32fast::Hasher::new();
            h.update(&len);
            h.update(&body);
            data.extend_from_slice(&h.finalize().to_le_bytes());
            data.extend_from_slice(&len);
            data.extend_from_slice(&body);
        }
        fs::write(segment_path(dir.path(), 4), &data).unwrap();
        let (got, max) = committed(dir.path(), &[4], 3);
        assert_eq!(got, vec![(4, enc(5)), (4, enc(6))]);
        assert_eq!(max, 0);
        // At or before the flush point they are already in chunks.
        assert!(committed(dir.path(), &[4], 4).0.is_empty());
    }

    #[test]
    fn segment_names() {
        assert_eq!(parse_segment_name("wal_000042.tfl.wal"), Some(42));
        assert_eq!(parse_segment_name("wal_x.tfl.wal"), None);
        assert_eq!(parse_segment_name("chunk_1.tfl"), None);
    }

    fn big_row(i: i64, n: usize) -> Vec<Value> {
        vec![Value::Timestamp(i), Value::Bytes(vec![b'x'; n])]
    }

    fn limits(pending_rows: usize, commits: usize) -> ReplayLimits {
        ReplayLimits { pending_rows, applied: usize::MAX, commits }
    }

    fn replay_with(dir: &Path, segs: &[u64], l: &ReplayLimits) -> Result<(Vec<Vec<u8>>, Replayed)> {
        let mut got = Vec::new();
        let r = replay_committed(dir, segs, 0, true, l, &|_| false, |_, p| {
            got.push(p.to_vec());
            Ok(())
        })?;
        Ok((got, r))
    }

    #[test]
    fn rows_that_will_never_apply_are_not_buffered_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        // 300 rows of 1 KiB (300 KiB) of a batch that never commits, and one
        // more of a batch that committed before the flush point (segment 1).
        for i in 0..300 {
            w.append(&row_entry(1, &big_row(i, 1024))).unwrap();
        }
        w.append(&row_entry(2, &r(7))).unwrap();
        w.append(&commit_entry(2)).unwrap();
        drop(w);
        let mut w = Wal::create(dir.path(), 2, None).unwrap();
        w.append(&row_entry(3, &r(8))).unwrap();
        w.append(&commit_entry(3)).unwrap();
        w.sync(true).unwrap();
        drop(w);
        // Batch 1 alone has 300 rows, far over the limit of 4: it never commits, so it is never remembered.
        let l = limits(4, 16);
        let mut got = Vec::new();
        let out = replay_committed(dir.path(), &[1, 2], 1, true, &l, &|_| false, |s, p| {
            got.push((s, p.to_vec()));
            Ok(())
        })
        .unwrap();
        assert_eq!(got, vec![(2, enc(8))], "only the commit after the flush point applies");
        assert_eq!(out.max_batch_id, 3);
    }

    #[test]
    fn a_committing_batch_over_the_row_limit_is_corruption_not_an_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        for i in 0..100 {
            w.append(&row_entry(1, &big_row(i, 1024))).unwrap();
        }
        w.append(&commit_entry(1)).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let err = replay_with(dir.path(), &[1], &limits(50, 16)).err().unwrap();
        assert!(matches!(&err, Error::Corrupt(m) if m.contains("replay limit")), "{err}");
        // Within the limit it replays all 100 rows.
        let (got, _) = replay_with(dir.path(), &[1], &limits(1000, 16)).unwrap();
        assert_eq!(got.len(), 100);
    }

    #[test]
    fn batches_waiting_for_their_commit_are_limited_together() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        for b in 1..=40u64 {
            w.append(&row_entry(b, &big_row(b as i64, 1024))).unwrap();
        }
        for b in 1..=40u64 {
            w.append(&commit_entry(b)).unwrap();
        }
        w.sync(true).unwrap();
        drop(w);
        // Forty rows waiting for their COMMITs at once are over a limit of ten.
        let err = replay_with(dir.path(), &[1], &limits(10, 100)).err().unwrap();
        assert!(matches!(&err, Error::Corrupt(m) if m.contains("waiting for their COMMIT")), "{err}");
        assert_eq!(replay_with(dir.path(), &[1], &limits(100, 100)).unwrap().0.len(), 40);
    }

    fn enc_big(i: i64) -> Vec<u8> {
        let mut v = Vec::new();
        encode_row(&mut v, &big_row(i, 4096));
        v
    }

    #[test]
    fn byte_volume_waiting_for_commits_is_not_limited_only_row_count_is() {
        // 8 concurrent batches log 300 rows of 4 KiB each (~9.6 MB) before any
        // commits: a payload buffer of a few MB would fail; offsets do not.
        let dir = tempfile::tempdir().unwrap();
        let mut keys: Vec<Option<u32>> = vec![None];
        #[cfg(feature = "encryption")]
        {
            crate::crypto::test_keys::install();
            keys.push(Some(5));
        }
        for key in keys {
            let sub = dir.path().join(format!("k{}", key.is_some()));
            fs::create_dir(&sub).unwrap();
            let mut w = Wal::create(&sub, 1, key).unwrap();
            for i in 0..300 {
                for b in 1..=8u64 {
                    w.append(&row_entry(b, &big_row(b as i64 * 1000 + i, 4096))).unwrap();
                }
            }
            for b in [3u64, 1, 8, 2, 7, 4, 6, 5] {
                w.append(&commit_entry(b)).unwrap();
            }
            w.sync(true).unwrap();
            drop(w);
            let (got, out) = replay_with(&sub, &[1], &limits(2400, 100)).unwrap();
            assert_eq!(got.len(), 2400);
            assert!(out.rejected.is_empty());
            // Commit order, rows of a batch in log order.
            assert_eq!(got[0], enc_big(3000));
            assert_eq!(got[299], enc_big(3299));
            assert_eq!(got[300], enc_big(1000));
        }
    }

    #[test]
    fn a_flood_of_commits_is_limited() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        for b in 1..=50u64 {
            w.append(&commit_entry(b)).unwrap();
        }
        w.sync(true).unwrap();
        drop(w);
        let err = replay_with(dir.path(), &[1], &limits(100, 10)).err().unwrap();
        assert!(matches!(&err, Error::Corrupt(m) if m.contains("commits")), "{err}");
    }

    #[test]
    fn a_vetoed_batch_keeps_only_its_counters() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        // The vetoed row comes first, so nothing of the batch is ever buffered.
        w.append(&row_entry(1, &big_row(0, 5000))).unwrap();
        for i in 1..200 {
            w.append(&row_entry(1, &big_row(i, 1000))).unwrap();
        }
        w.append(&commit_entry(1)).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let mut applied = 0;
        let out = replay_committed(dir.path(), &[1], 0, true, &limits(10, 16), &|p| p.len() > 3000, |_, _| {
            applied += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(applied, 0, "all or nothing");
        assert_eq!(out.rejected.len(), 1);
        assert_eq!(out.rejected[0].rows, 200);
        assert_eq!(out.rejected[0].batch, Some(1));
        assert!(out.rejected[0].bytes > 200 * 1000);
    }

    #[test]
    fn a_forged_entry_length_cannot_reserve_more_than_the_file_holds() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(&row_commit_entry(&r(1))).unwrap();
        w.sync(true).unwrap();
        drop(w);
        // An entry header that claims 900 MiB, followed by almost nothing.
        let path = segment_path(dir.path(), 1);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap();
        f.write_all(&(900u32 << 20).to_le_bytes()).unwrap();
        f.write_all(&[7u8; 100]).unwrap();
        drop(f);
        let (got, out) = collect(dir.path(), 1, true).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(out.torn_bytes, 108, "the forged tail is a torn tail, cut off");
    }

    #[test]
    fn a_second_pass_sees_the_segment_as_the_first_did() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::create(dir.path(), 1, None).unwrap();
        w.append(&row_commit_entry(&r(1))).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let path = segment_path(dir.path(), 1);
        let first = replay(dir.path(), 1, TornTail::Ignore, None, |_, _| Ok(())).unwrap();
        // Bytes appended after the first pass are not part of the second.
        let mut w = Wal::create(dir.path(), 2, None).unwrap();
        w.append(&row_commit_entry(&r(2))).unwrap();
        w.sync(true).unwrap();
        drop(w);
        let mut extra = fs::read(segment_path(dir.path(), 2)).unwrap();
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&extra.split_off(BASE_HEADER)).unwrap();
        drop(f);
        let mut n = 0;
        replay(dir.path(), 1, TornTail::Error, Some(first.valid_len), |_, _| {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 1);
    }
}
