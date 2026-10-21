//! Process-wide LRU caches: decoded chunk blocks (and decoded series, for
//! random row fetches, decoded columns of narrow reads) and open chunk files.
//!
//! All are keyed by a chunk file's `uid`, a process-unique number assigned
//! when the file is opened, so entries never alias across tables or across a
//! deleted-and-recreated path. Blocks, decoded series and columns share one LRU and one
//! byte budget (`moosedb_chunk_cache_size`).

use std::collections::BTreeMap;
use std::fs::File;
use std::hash::Hash;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::block::ColumnData;
use crate::error::Result;
use crate::fasthash::{FastMap, FastState};
use crate::settings;

struct Lru<K, V> {
    map: FastMap<K, (V, u64, usize)>,
    order: BTreeMap<u64, K>,
    tick: u64,
    used: usize,
}

impl<K: Hash + Eq + Clone, V: Clone> Lru<K, V> {
    fn new() -> Self {
        Lru { map: FastMap::default(), order: BTreeMap::new(), tick: 0, used: 0 }
    }

    fn get(&mut self, k: &K) -> Option<V> {
        self.tick += 1;
        let tick = self.tick;
        // An entry among the most recently used ~6% keeps its place: a hot
        // entry read in a loop would otherwise rewrite the order map on every
        // hit. The LRU is therefore approximate: ticks count every get and
        // insert, so an entry touched within the last `len / 16` ticks is
        // treated as fresh and its recency is not updated; it can be evicted
        // up to that many ticks "too early" relative to exact LRU. (Small
        // caches, `len < 16`, always refresh, so their order stays exact.)
        let fresh = (self.map.len() / 16) as u64;
        let (v, t, _) = self.map.get_mut(k)?;
        if tick - *t <= fresh {
            return Some(v.clone());
        }
        self.order.remove(t);
        *t = tick;
        self.order.insert(tick, k.clone());
        Some(v.clone())
    }

    fn remove(&mut self, k: &K) {
        if let Some((_, t, size)) = self.map.remove(k) {
            self.order.remove(&t);
            self.used -= size;
        }
    }

    fn insert(&mut self, k: K, v: V, size: usize, capacity: usize) {
        self.remove(&k);
        if size > capacity {
            // Also honours a capacity that was lowered since the last insert.
            self.shrink(capacity);
            return;
        }
        self.tick += 1;
        self.order.insert(self.tick, k.clone());
        self.map.insert(k, (v, self.tick, size));
        self.used += size;
        self.shrink(capacity);
    }

    /// Adds `k` (replacing it) without evicting anything.
    fn put(&mut self, k: K, v: V, size: usize) {
        self.remove(&k);
        self.tick += 1;
        self.order.insert(self.tick, k.clone());
        self.map.insert(k, (v, self.tick, size));
        self.used += size;
    }

    /// Drops the least recently used entry; `false` when there is none.
    fn pop_oldest(&mut self) -> bool {
        let Some((_, k)) = self.order.pop_first() else { return false };
        if let Some((_, _, size)) = self.map.remove(&k) {
            self.used -= size;
        }
        true
    }

    fn shrink(&mut self, capacity: usize) {
        while self.used > capacity {
            let Some((_, k)) = self.order.pop_first() else { break };
            if let Some((_, _, size)) = self.map.remove(&k) {
                self.used -= size;
            }
        }
    }

    fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        let dead: Vec<K> = self.map.keys().filter(|k| !keep(k)).cloned().collect();
        for k in dead {
            self.remove(&k);
        }
    }
}

/// A decoded (decrypted, checksummed, decompressed) column block.
pub(crate) struct BlockPayload {
    pub encoding: u8,
    pub data: Vec<u8>,
}

/// One series of a chunk with every data column decoded into native-width
/// arrays: a row is then built from them in O(columns), whatever the series
/// length. `columns[c]` is `None` for TAG columns (constant per series).
pub(crate) struct DecodedSeries {
    pub columns: Vec<Option<ColumnData>>,
}

impl DecodedSeries {
    fn size(&self) -> usize {
        64 + self.columns.iter().flatten().map(|c| c.heap_bytes() + 64).sum::<usize>()
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    /// (file uid, block offset)
    Block(u64, u64),
    /// (file uid, series entry index)
    Series(u64, u32),
    /// (file uid, series entry index, schema column)
    Column(u64, u32, u32),
}

impl CacheKey {
    fn uid(&self) -> u64 {
        match self {
            CacheKey::Block(uid, _) | CacheKey::Series(uid, _) | CacheKey::Column(uid, _, _) => *uid,
        }
    }
}

#[derive(Clone)]
enum Cached {
    Block(Arc<BlockPayload>),
    Series(Arc<DecodedSeries>),
    Column(Arc<ColumnData>),
}

/// Shards of the block/series/column cache. A parallel aggregation asks the
/// cache for every series window, so one mutex serialized the workers; keys of
/// different series land in different shards.
const SHARDS: usize = 16;

/// One byte budget over [`SHARDS`] independent LRUs. A shard evicts its own
/// oldest entries first (a sampled LRU); only when that is not enough does it
/// take the oldest entries of the others, one lock at a time (never nested).
struct Sharded {
    shards: Vec<Mutex<Lru<CacheKey, Cached>>>,
    /// Sum of the shards' `used`, maintained under their locks.
    total: AtomicUsize,
}

impl Sharded {
    fn shard(&self, key: &CacheKey) -> &Mutex<Lru<CacheKey, Cached>> {
        let (uid, n) = match key {
            CacheKey::Block(uid, off) => (*uid, *off),
            CacheKey::Series(uid, i) | CacheKey::Column(uid, i, _) => (*uid, u64::from(*i)),
        };
        // series / column entries of one series share a shard
        let h = (uid ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        &self.shards[(h >> 59) as usize % SHARDS]
    }

    /// Runs `f` on a locked shard and keeps `total` in step with its `used`.
    fn locked<R>(&self, m: &Mutex<Lru<CacheKey, Cached>>, f: impl FnOnce(&mut Lru<CacheKey, Cached>) -> R) -> Option<R> {
        let mut g = m.lock().ok()?;
        let before = g.used;
        let r = f(&mut g);
        self.apply(before, g.used);
        Some(r)
    }

    fn get(&self, key: &CacheKey) -> Option<Cached> {
        self.locked(self.shard(key), |l| l.get(key))?
    }

    /// Moves `total` by a shard's change of `used`; returns the new total.
    fn apply(&self, before: usize, after: usize) -> usize {
        if after == before {
            // a hit touches no shared line
            self.total.load(Ordering::Relaxed)
        } else if after > before {
            self.total.fetch_add(after - before, Ordering::Relaxed) + (after - before)
        } else {
            self.total.fetch_sub(before - after, Ordering::Relaxed) - (before - after)
        }
    }

    fn insert(&self, key: CacheKey, v: Cached, size: usize, capacity: usize) {
        let Ok(mut l) = self.shard(&key).lock() else { return };
        let before = l.used;
        l.remove(&key);
        if size <= capacity {
            l.put(key, v, size);
        }
        let mut total = self.apply(before, l.used);
        // own oldest entries first (never the new one: it is the newest)
        while total > capacity && l.map.len() > 1 {
            let b = l.used;
            l.pop_oldest();
            total = self.apply(b, l.used);
        }
        drop(l);
        self.trim(capacity);
    }

    /// Takes the oldest entries of any shard until the budget holds.
    fn trim(&self, capacity: usize) {
        // start at a different shard each time, so none is always the first victim
        static START: AtomicUsize = AtomicUsize::new(0);
        while self.total.load(Ordering::Relaxed) > capacity {
            let mut any = false;
            let first = START.fetch_add(1, Ordering::Relaxed);
            for k in 0..SHARDS {
                let m = &self.shards[(first + k) % SHARDS];
                if self.total.load(Ordering::Relaxed) <= capacity {
                    return;
                }
                any |= self.locked(m, Lru::pop_oldest).unwrap_or(false);
            }
            if !any {
                return;
            }
        }
    }
}

impl Sharded {
    fn new() -> Sharded {
        Sharded { shards: (0..SHARDS).map(|_| Mutex::new(Lru::new())).collect(), total: AtomicUsize::new(0) }
    }

    /// Drops every entry of chunk file `uid`, in every shard.
    fn forget(&self, uid: u64) {
        for m in &self.shards {
            self.locked(m, |c| c.retain(|k| k.uid() != uid));
        }
    }
}

fn blocks() -> &'static Sharded {
    static C: OnceLock<Sharded> = OnceLock::new();
    C.get_or_init(Sharded::new)
}

fn files() -> &'static Mutex<Lru<u64, Arc<File>>> {
    static C: OnceLock<Mutex<Lru<u64, Arc<File>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Lru::new()))
}

pub(crate) fn get_block(uid: u64, offset: u64) -> Option<Arc<BlockPayload>> {
    match blocks().get(&CacheKey::Block(uid, offset))? {
        Cached::Block(b) => Some(b),
        _ => None,
    }
}

fn capacity() -> usize {
    usize::try_from(settings::get().chunk_cache_bytes()).unwrap_or(usize::MAX)
}

pub(crate) fn put_block(uid: u64, offset: u64, block: Arc<BlockPayload>) {
    let capacity = capacity();
    if capacity == 0 {
        return;
    }
    let size = block.data.len() + 64;
    blocks().insert(CacheKey::Block(uid, offset), Cached::Block(block), size, capacity);
}

pub(crate) fn get_series(uid: u64, index: u32) -> Option<Arc<DecodedSeries>> {
    match blocks().get(&CacheKey::Series(uid, index))? {
        Cached::Series(s) => Some(s),
        _ => None,
    }
}

/// Keeps a decoded series; one bigger than the whole cache is not kept.
pub(crate) fn put_series(uid: u64, index: u32, series: Arc<DecodedSeries>) {
    let capacity = capacity();
    if capacity == 0 {
        return;
    }
    let size = series.size();
    blocks().insert(CacheKey::Series(uid, index), Cached::Series(series), size, capacity);
}

/// A fully decoded data column of one series (all its rows), kept for the
/// narrow reads that would otherwise decode the same column again and again.
pub(crate) fn get_column(uid: u64, series: u32, col: u32) -> Option<Arc<ColumnData>> {
    match blocks().get(&CacheKey::Column(uid, series, col))? {
        Cached::Column(c) => Some(c),
        _ => None,
    }
}

/// Keeps a decoded column; one bigger than the whole cache is not kept.
pub(crate) fn put_column(uid: u64, series: u32, col: u32, data: Arc<ColumnData>) {
    let capacity = capacity();
    if capacity == 0 {
        return;
    }
    let size = data.heap_bytes() + 64;
    blocks().insert(CacheKey::Column(uid, series, col), Cached::Column(data), size, capacity);
}

/// Slots of the "touched once" table (8 bytes each).
const TOUCH_SLOTS: usize = 1 << 16;

/// Whether this (chunk file, series, column) was decoded before; marks it as
/// touched. A column read for the first time is decoded for the rows at hand
/// and not kept; one read again is decoded whole and cached (see
/// `chunk_reader::load_window`), so a scan that visits every series once never
/// fills the cache, while queries that repeat do. Fixed-size, direct-mapped:
/// a collision can only make a column look seen (it gets cached) or forget it.
pub(crate) fn touched_before(uid: u64, series: u32, col: u32) -> bool {
    // Lock-free: parallel readers (the aggregation) call this once per window.
    static SLOTS: OnceLock<Vec<AtomicU64>> = OnceLock::new();
    let hash = std::hash::BuildHasher::hash_one(&FastState::default(), (uid, series, col)) | 1;
    let slots = SLOTS.get_or_init(|| (0..TOUCH_SLOTS).map(|_| AtomicU64::new(0)).collect());
    slots[(hash >> 1) as usize % TOUCH_SLOTS].swap(hash, Ordering::Relaxed) == hash
}

/// Applies a new capacity at once: entries over it are evicted now, not at
/// the next insert (a lowered `moosedb_chunk_cache_size` must free memory).
pub(crate) fn resize(capacity: u64) {
    blocks().trim(usize::try_from(capacity).unwrap_or(usize::MAX));
}

/// Bytes currently held by the block/series cache (diagnostics, tests).
pub fn used_bytes() -> usize {
    blocks().total.load(Ordering::Relaxed)
}

/// Opens (or reuses) a read handle for a chunk file.
pub(crate) fn open_file(uid: u64, path: &Path) -> Result<Arc<File>> {
    if let Some(f) = files().lock().ok().and_then(|mut c| c.get(&uid)) {
        return Ok(f);
    }
    let f = Arc::new(File::open(path)?);
    if let Ok(mut c) = files().lock() {
        c.insert(uid, f.clone(), 1, settings::get().max_open_chunks() as usize);
    }
    Ok(f)
}

/// Drops every cached entry of a chunk file (it is being deleted).
pub(crate) fn forget_file(uid: u64) {
    if let Ok(mut c) = files().lock() {
        c.remove(&uid);
    }
    blocks().forget(uid);
}

/// Reads exactly `buf.len()` bytes at `offset` without moving a shared cursor.
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            match file.seek_read(&mut buf[done..], offset + done as u64)? {
                0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                n => done += n,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_least_recently_used() {
        let mut l: Lru<u32, u32> = Lru::new();
        l.insert(1, 10, 1, 2);
        l.insert(2, 20, 1, 2);
        assert_eq!(l.get(&1), Some(10));
        l.insert(3, 30, 1, 2);
        assert_eq!(l.get(&2), None, "2 was least recently used");
        assert_eq!(l.get(&1), Some(10));
        assert_eq!(l.get(&3), Some(30));
        l.insert(4, 40, 5, 2);
        assert_eq!(l.get(&4), None, "entries larger than the cache are not kept");
        l.retain(|k| *k != 1);
        assert_eq!(l.get(&1), None);
        assert_eq!(l.used, 1);
    }

    fn block(n: usize) -> Cached {
        Cached::Block(Arc::new(BlockPayload { encoding: 0, data: vec![0; n] }))
    }

    fn shard_sum(c: &Sharded) -> usize {
        c.shards.iter().map(|m| m.lock().unwrap().used).sum()
    }

    #[test]
    fn sharded_cache_keeps_its_budget_under_concurrent_inserts() {
        let cap = 64 << 10;
        let c = Sharded::new();
        std::thread::scope(|s| {
            for t in 0..8u64 {
                let c = &c;
                s.spawn(move || {
                    for i in 0..4000u64 {
                        // sizes 100 B .. ~6 KB, a few reads in between
                        let k = CacheKey::Block(t % 3, i * 7 + t);
                        c.insert(k.clone(), block(100 + (i as usize * 37) % 6000), 100 + (i as usize * 37) % 6000, cap);
                        let _ = c.get(&k);
                    }
                });
            }
        });
        // at rest: the budget holds and the counter matches the shards
        assert!(c.total.load(Ordering::Relaxed) <= cap, "{}", c.total.load(Ordering::Relaxed));
        assert_eq!(c.total.load(Ordering::Relaxed), shard_sum(&c));
        assert!(shard_sum(&c) > 0);
        // an entry bigger than the whole budget is not kept
        c.insert(CacheKey::Block(9, 9), block(1), cap + 1, cap);
        assert!(c.get(&CacheKey::Block(9, 9)).is_none());
        // lowering the capacity evicts at once, from every shard
        c.trim(cap / 4);
        assert!(c.total.load(Ordering::Relaxed) <= cap / 4);
        assert_eq!(c.total.load(Ordering::Relaxed), shard_sum(&c));
        c.trim(0);
        assert_eq!((c.total.load(Ordering::Relaxed), shard_sum(&c)), (0, 0));
    }

    #[test]
    fn forgetting_a_file_removes_its_entries_from_every_shard() {
        let c = Sharded::new();
        let cap = 1 << 30;
        for i in 0..2000u64 {
            c.insert(CacheKey::Block(1, i), block(10), 10, cap);
            c.insert(CacheKey::Column(1, i as u32, 2), block(10), 10, cap);
            c.insert(CacheKey::Series(1, i as u32), block(10), 10, cap);
            c.insert(CacheKey::Block(2, i), block(10), 10, cap);
        }
        assert!(c.shards.iter().all(|m| !m.lock().unwrap().map.is_empty()), "entries spread over all shards");
        c.forget(1);
        assert_eq!(c.total.load(Ordering::Relaxed), 2000 * 10);
        assert_eq!(shard_sum(&c), 2000 * 10);
        for i in 0..2000u64 {
            assert!(c.get(&CacheKey::Block(1, i)).is_none());
            assert!(c.get(&CacheKey::Column(1, i as u32, 2)).is_none());
            assert!(c.get(&CacheKey::Series(1, i as u32)).is_none());
            assert!(c.get(&CacheKey::Block(2, i)).is_some(), "other files stay");
        }
    }
}
