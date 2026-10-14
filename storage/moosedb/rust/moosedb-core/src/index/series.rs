//! Mapping from tag values to `series_id`.
//!
//! The map is not persisted separately: every chunk stores the tag values of
//! the series it contains, and the WAL stores full rows, so the index is
//! rebuilt on open from chunk metadata plus WAL replay. Ids are assigned
//! sequentially; since rebuilding visits chunks in id order and then replays
//! the WAL in order, the assignment is deterministic.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::codec::encode_value;
use crate::error::{corrupt, Result};
use crate::fasthash::FastMap;
use crate::schema::{Schema, Value};

/// Canonical byte encoding of a series' tag values.
pub(crate) fn series_key(tags: &[Value]) -> Vec<u8> {
    let mut key = Vec::new();
    for v in tags {
        encode_value(&mut key, v);
    }
    key
}

/// Tag values of a full row, in tag order.
pub(crate) fn row_tags(schema: &Schema, row: &[Value]) -> Vec<Value> {
    schema.tag_indices().iter().map(|&i| row[i].clone()).collect()
}

/// Append-only record of the series in arrival order. Entries are never
/// changed, so a [`SeriesSnapshot`] is a shared handle plus a length: taking
/// one copies nothing, whatever the number of series (TRUNCATE starts a new
/// log; snapshots of the old one stay valid).
struct SeriesLog {
    /// Identity of this log, unique in the process (see `SeriesSnapshot::epoch`).
    epoch: u64,
    items: RwLock<Vec<(u64, Arc<[Value]>)>>,
}

/// Last epoch handed out; every log (new index, clear, reopen) takes the next.
static EPOCHS: AtomicU64 = AtomicU64::new(0);

impl Default for SeriesLog {
    fn default() -> Self {
        SeriesLog { epoch: EPOCHS.fetch_add(1, Ordering::AcqRel) + 1, items: RwLock::default() }
    }
}

/// Immutable view of the series of a table at one instant: `len()` entries,
/// `(series_id, tag values in TAG-column order)`, in registration order (not
/// sorted by id). Cheap to take and to clone; readers never block writers
/// beyond the brief append lock.
#[derive(Clone)]
pub struct SeriesSnapshot {
    log: Arc<SeriesLog>,
    len: usize,
    version: u64,
}

impl SeriesSnapshot {
    /// Number of series.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Identity of the underlying log, unique in the process: it changes on
    /// every TRUNCATE and every table open, and never repeats. Two snapshots
    /// with the same epoch are prefixes of one append-only log, so the later
    /// one is the earlier one plus new entries (same entries at the same indexes).
    pub fn epoch(&self) -> u64 {
        self.log.epoch
    }

    /// Version of the series set this snapshot shows (see `Table::series_version`).
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Entry `index` (`< len()`).
    pub fn get(&self, index: usize) -> Option<(u64, Arc<[Value]>)> {
        if index >= self.len {
            return None;
        }
        let items = self.log.items.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        items.get(index).cloned()
    }

    /// Every entry, in registration order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, Arc<[Value]>)> + '_ {
        (0..self.len).filter_map(|i| self.get(i))
    }

    /// Every entry sorted by series id (the order of `Table::series`).
    pub fn to_sorted_vec(&self) -> Vec<(u64, Vec<Value>)> {
        let mut v: Vec<(u64, Vec<Value>)> = self.iter().map(|(id, t)| (id, t.to_vec())).collect();
        v.sort_by_key(|(id, _)| *id);
        v
    }
}

/// Last version handed out by any index of the process. Versions come from
/// here, so one never repeats within the process: not across a TRUNCATE, not
/// across a reopen or a drop-and-recreate of a table (a cache keyed by it
/// cannot mistake new content for old).
static VERSIONS: AtomicU64 = AtomicU64::new(0);

fn next_version() -> u64 {
    VERSIONS.fetch_add(1, Ordering::AcqRel) + 1
}

pub(crate) struct SeriesIndex {
    by_key: HashMap<Vec<u8>, u64>,
    tags_by_id: FastMap<u64, Arc<[Value]>>,
    next_id: u64,
    log: Arc<SeriesLog>,
    /// Number of entries of `log`.
    logged: usize,
    /// Bumped whenever a series is added or the index is cleared; shared with
    /// the table so it can be read without taking the table mutex.
    version: Arc<AtomicU64>,
}

impl Default for SeriesIndex {
    fn default() -> Self {
        SeriesIndex {
            by_key: HashMap::new(),
            tags_by_id: FastMap::default(),
            next_id: 0,
            log: Arc::default(),
            logged: 0,
            version: Arc::new(AtomicU64::new(next_version())),
        }
    }
}

impl SeriesIndex {
    pub(crate) fn len(&self) -> usize {
        self.tags_by_id.len()
    }

    /// Handle on the version counter (read lock-free).
    pub(crate) fn version_handle(&self) -> Arc<AtomicU64> {
        self.version.clone()
    }

    /// Shares the current series without copying them.
    pub(crate) fn snapshot(&self) -> SeriesSnapshot {
        SeriesSnapshot { log: self.log.clone(), len: self.logged, version: self.version.load(Ordering::Acquire) }
    }

    fn add(&mut self, key: Vec<u8>, id: u64, tags: &[Value]) {
        let tags: Arc<[Value]> = Arc::from(tags);
        self.by_key.insert(key, id);
        self.tags_by_id.insert(id, tags.clone());
        self.log.items.write().unwrap_or_else(std::sync::PoisonError::into_inner).push((id, tags));
        self.logged += 1;
        self.version.store(next_version(), Ordering::Release);
    }

    /// Id of the series with these tags, if there is one.
    pub(crate) fn lookup(&self, tags: &[Value]) -> Option<u64> {
        self.by_key.get(&series_key(tags)).copied()
    }

    pub(crate) fn tags(&self, id: u64) -> Option<&[Value]> {
        self.tags_by_id.get(&id).map(|t| &t[..])
    }

    /// Returns the id of the series with these tags, assigning a new one if needed.
    pub(crate) fn get_or_insert(&mut self, tags: &[Value]) -> u64 {
        let key = series_key(tags);
        if let Some(&id) = self.by_key.get(&key) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.add(key, id, tags);
        id
    }

    /// [`SeriesIndex::get_or_insert`] for a caller that already has the key
    /// (`series_key(tags)`) and builds the tag values only when
    /// the series is new: a known series costs one lookup and builds nothing.
    pub(crate) fn get_or_insert_keyed_with(&mut self, key: &[u8], tags: impl FnOnce() -> Vec<Value>) -> u64 {
        if let Some(&id) = self.by_key.get(key) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.add(key.to_vec(), id, &tags());
        id
    }

    /// Registers a series found in a chunk. The same series may appear in many
    /// chunks, but an id must always map to the same tags and vice versa.
    pub(crate) fn register(&mut self, id: u64, tags: &[Value]) -> Result<()> {
        let key = series_key(tags);
        match self.by_key.get(&key) {
            Some(&existing) if existing == id => return Ok(()),
            Some(&existing) => {
                return Err(corrupt(format!("series tags map to both id {existing} and id {id}")));
            }
            None => {}
        }
        if self.tags_by_id.contains_key(&id) {
            return Err(corrupt(format!("series id {id} used for two different tag sets")));
        }
        self.add(key, id, tags);
        self.next_id = self.next_id.max(id + 1);
        Ok(())
    }

    /// Forgets every series. The version keeps growing (never repeats), and
    /// snapshots taken earlier keep showing what they showed.
    pub(crate) fn clear(&mut self) {
        let version = self.version.clone();
        *self = SeriesIndex { version, ..SeriesIndex::default() };
        self.version.store(next_version(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(s: &str) -> Vec<Value> {
        vec![Value::Bytes(s.as_bytes().to_vec())]
    }

    #[test]
    fn assigns_stable_ids() {
        let mut idx = SeriesIndex::default();
        let a = idx.get_or_insert(&tags("a"));
        let b = idx.get_or_insert(&tags("b"));
        assert_ne!(a, b);
        assert_eq!(idx.get_or_insert(&tags("a")), a);
        assert_eq!(idx.lookup(&tags("b")), Some(b));
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn register_rejects_conflicts() {
        let mut idx = SeriesIndex::default();
        idx.register(5, &tags("a")).unwrap();
        idx.register(5, &tags("a")).unwrap();
        assert!(idx.register(6, &tags("a")).is_err());
        assert!(idx.register(5, &tags("b")).is_err());
        assert_eq!(idx.get_or_insert(&tags("c")), 6, "next id continues after registered ids");
    }

    #[test]
    fn snapshots_share_the_log_and_do_not_see_later_series() {
        let mut idx = SeriesIndex::default();
        let v0 = idx.snapshot().version();
        idx.get_or_insert(&tags("a"));
        idx.get_or_insert(&tags("b"));
        let s = idx.snapshot();
        assert_eq!(s.len(), 2);
        assert!(s.version() > v0);
        idx.get_or_insert(&tags("a")); // known: nothing changes
        assert_eq!(idx.snapshot().version(), s.version());
        idx.get_or_insert(&tags("c"));
        assert_eq!(s.len(), 2, "an older snapshot never grows");
        assert_eq!(idx.snapshot().len(), 3);
        assert!(idx.snapshot().version() > s.version());
        assert_eq!(s.get(1).unwrap().1.to_vec(), tags("b"));
        assert!(s.get(2).is_none());
        let before = idx.snapshot();
        idx.clear();
        assert!(idx.snapshot().is_empty());
        assert!(idx.snapshot().version() > before.version(), "versions never repeat across a clear");
        assert_eq!(before.len(), 3, "snapshots outlive a clear");
        assert_ne!(before.epoch(), idx.snapshot().epoch(), "a clear starts a new log");
        assert_ne!(SeriesIndex::default().snapshot().epoch(), idx.snapshot().epoch());
        assert_eq!(before.to_sorted_vec().len(), 3);
        assert_eq!(idx.get_or_insert(&tags("z")), 0);
    }

    #[test]
    fn versions_never_repeat_across_indexes_of_the_process() {
        // A table reopened (or dropped and recreated) starts a new index.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let mut idx = SeriesIndex::default();
            assert!(seen.insert(idx.snapshot().version()));
            idx.get_or_insert(&tags("a"));
            assert!(seen.insert(idx.snapshot().version()), "same content size, different version");
            idx.clear();
            assert!(seen.insert(idx.snapshot().version()));
        }
    }

    #[test]
    fn null_and_empty_tags_are_distinct() {
        let mut idx = SeriesIndex::default();
        let n = idx.get_or_insert(&[Value::Null]);
        let e = idx.get_or_insert(&[Value::Bytes(vec![])]);
        assert_ne!(n, e);
    }
}
