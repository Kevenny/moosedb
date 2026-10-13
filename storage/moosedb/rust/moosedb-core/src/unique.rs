//! `UNIQUE` / `PRIMARY KEY (TAG..., time, extra...)`.
//!
//! The engine has no index structure, so uniqueness is checked against the
//! data itself, with three pieces of in-memory state that make the common
//! case (append in time order) free:
//!
//! * **per series, the highest timestamp ever accepted** (`max_ts`, committed
//!   *and* in flight) and, when known (`complete`), the set of extra-column
//!   tuples already present at exactly that timestamp (`top`). A row whose
//!   timestamp is above `max_ts` cannot be a duplicate; one at `max_ts` is
//!   answered from `top`. Anything older needs a **lookup**: the series is
//!   read at that one timestamp in the chunks, the MemTable and the chunks
//!   this statement staged, plus the rows the statement itself buffered.
//!   `max_ts` is only an upper bound (retention and aborted statements never
//!   lower it), so state that is lost or stale can make a check slower, never
//!   wrong. It is rebuilt at open from the series entries of the chunks
//!   (`max_ts`, `complete = false`) and the replayed WAL rows.
//! * **series claims.** A statement's rows are invisible to other sessions
//!   until it commits, so two statements could both see "no such key" and both
//!   insert it. The first row a statement writes to a series *claims* the
//!   series until the statement commits or aborts; a statement that needs a
//!   series claimed by another one waits for it (`Error::Busy`, retried by the
//!   caller, the wait itself happens on a condition variable) and then checks
//!   against committed data. Exactly one of two racing inserts of the same key
//!   wins, the other gets `Error::Duplicate`. A wait that would close a cycle
//!   (A holds s1 wants s2, B holds s2 wants s1) fails with `Error::Deadlock`.
//! * **unchecked mode** (`unique_checks = 0`): rows are not checked and do not
//!   claim, but still update `max_ts` / `top`, so later checked statements stay
//!   correct for everything that was recorded. Keys inserted unchecked can be
//!   duplicates; they are not detected afterwards.
//!
//! The key has at most [`MAX_EXTRAS`] extra columns, all `INT64`, `NOT NULL`.

use std::collections::HashSet;
use std::sync::Arc;

use crate::chunk::ChunkMeta;
use crate::chunk_reader::load_window;
use crate::error::{invalid, Result};
use crate::fasthash::FastMap;
use crate::scan::Snapshot;
use crate::schema::{ColumnType, Schema, Value};

/// Most extra key columns after the time column.
pub const MAX_EXTRAS: usize = 2;
/// Values of the extra key columns of a row (unused slots are 0).
pub type Extras = [i64; MAX_EXTRAS];

/// The unique key of a table: `(all TAG columns, time column, extra columns)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueSpec {
    extras: Vec<usize>,
}

impl UniqueSpec {
    /// `extras`: schema indexes of the key columns that follow the time
    /// column, in key order.
    pub fn new(schema: &Schema, extras: Vec<usize>) -> Result<UniqueSpec> {
        if extras.len() > MAX_EXTRAS {
            return Err(invalid(format!("a unique key takes at most {MAX_EXTRAS} columns after the time column")));
        }
        for (i, &c) in extras.iter().enumerate() {
            let col = schema.columns().get(c).ok_or_else(|| invalid(format!("unique key column {c} out of range")))?;
            if col.ty != ColumnType::Int64 {
                return Err(invalid(format!("unique key column '{}' must be an integer column", col.name)));
            }
            if extras[..i].contains(&c) {
                return Err(invalid(format!("unique key column '{}' appears twice", col.name)));
            }
        }
        Ok(UniqueSpec { extras })
    }

    /// Schema indexes of the columns after the time column.
    pub fn extra_columns(&self) -> &[usize] {
        &self.extras
    }

    /// The key columns of `row`, `None` if one of them is NULL or not an integer.
    pub(crate) fn extras_of(&self, row: &[Value]) -> Option<Extras> {
        let mut out = [0i64; MAX_EXTRAS];
        for (slot, &c) in out.iter_mut().zip(&self.extras) {
            match row.get(c) {
                Some(Value::Int(v)) => *slot = *v,
                _ => return None,
            }
        }
        Some(out)
    }
}

/// Extras of the rows at the highest timestamp of a series.
#[derive(Debug)]
enum Top {
    Empty,
    One(Extras),
    Many(HashSet<Extras>),
}

impl Top {
    fn contains(&self, e: &Extras) -> bool {
        match self {
            Top::Empty => false,
            Top::One(x) => x == e,
            Top::Many(s) => s.contains(e),
        }
    }

    fn insert(&mut self, e: Extras) {
        match self {
            Top::Empty => *self = Top::One(e),
            Top::One(x) if *x == e => {}
            Top::One(x) => {
                let mut s = HashSet::new();
                s.insert(*x);
                s.insert(e);
                *self = Top::Many(s);
            }
            Top::Many(s) => {
                s.insert(e);
            }
        }
    }
}

#[derive(Debug)]
struct SeriesTop {
    /// Upper bound of the timestamps of the series (committed + in flight).
    max_ts: i64,
    /// `top` holds exactly the extras of the rows at `max_ts`.
    complete: bool,
    top: Top,
}

/// What the state alone can say about a row.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Plan {
    /// Cannot be a duplicate.
    Accept,
    /// Is a duplicate.
    Duplicate,
    /// Look the key up in the data.
    Lookup,
}

/// Result of trying to claim a series for a statement.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Claim {
    /// The statement holds the series; `new` = it did not before.
    Held { new: bool },
    /// Held by that statement: wait.
    Busy(u64),
    /// Waiting would close a cycle.
    Deadlock,
}

/// Unique-key state of a table, guarded by the table mutex.
#[derive(Debug, Default)]
pub(crate) struct UniqueState {
    series: FastMap<u64, SeriesTop>,
    /// series id -> id of the statement (batch) that holds it
    claims: FastMap<u64, u64>,
    /// batch id -> batch id it waits for
    waiting: FastMap<u64, u64>,
}

impl UniqueState {
    pub(crate) fn clear(&mut self) {
        self.series.clear();
        self.claims.clear();
        self.waiting.clear();
    }

    /// A chunk holds rows of `sid` up to `ts_max` (their extras are unknown).
    pub(crate) fn note_chunk(&mut self, sid: u64, ts_max: i64) {
        match self.series.get_mut(&sid) {
            None => {
                self.series.insert(sid, SeriesTop { max_ts: ts_max, complete: false, top: Top::Empty });
            }
            Some(st) if ts_max > st.max_ts => {
                *st = SeriesTop { max_ts: ts_max, complete: false, top: Top::Empty };
            }
            Some(st) if ts_max == st.max_ts => {
                st.complete = false;
                st.top = Top::Empty;
            }
            Some(_) => {}
        }
    }

    /// A row exists (replay) or was just stored without being checked. `e` is
    /// `None` when its key columns are unusable (NULL): the series is then no
    /// longer fully known at that timestamp.
    pub(crate) fn note_row(&mut self, sid: u64, ts: i64, e: Option<Extras>) {
        match e {
            Some(e) => self.accept(sid, ts, e, None),
            None => self.note_chunk(sid, ts),
        }
    }

    /// Records an accepted row. `existing` = every extras tuple already stored
    /// at `ts` (only used to make a state complete that was not).
    pub(crate) fn accept(&mut self, sid: u64, ts: i64, e: Extras, existing: Option<&[Extras]>) {
        match self.series.get_mut(&sid) {
            None => {
                self.series.insert(sid, SeriesTop { max_ts: ts, complete: true, top: Top::One(e) });
            }
            Some(st) if ts > st.max_ts => {
                *st = SeriesTop { max_ts: ts, complete: true, top: Top::One(e) };
            }
            Some(st) if ts == st.max_ts => {
                if st.complete {
                    st.top.insert(e);
                } else if let Some(all) = existing {
                    let mut top = Top::Empty;
                    for x in all {
                        top.insert(*x);
                    }
                    top.insert(e);
                    st.top = top;
                    st.complete = true;
                }
            }
            Some(_) => {}
        }
    }

    pub(crate) fn plan(&self, sid: u64, ts: i64, e: &Extras) -> Plan {
        let Some(st) = self.series.get(&sid) else { return Plan::Accept };
        if ts > st.max_ts {
            return Plan::Accept;
        }
        if ts == st.max_ts && st.complete {
            return if st.top.contains(e) { Plan::Duplicate } else { Plan::Accept };
        }
        Plan::Lookup
    }

    /// What a statement that stored rows it may not keep leaves behind is
    /// unknown: forget the extras (the bound stays valid).
    pub(crate) fn invalidate(&mut self, sid: u64) {
        if let Some(st) = self.series.get_mut(&sid) {
            st.complete = false;
            st.top = Top::Empty;
        }
    }

    pub(crate) fn claim(&mut self, sid: u64, me: u64) -> Claim {
        match self.claims.get(&sid).copied() {
            None => {
                self.claims.insert(sid, me);
                Claim::Held { new: true }
            }
            Some(o) if o == me => Claim::Held { new: false },
            Some(owner) => {
                // Follow the chain of who waits for whom: reaching `me` is a cycle.
                let mut cur = owner;
                for _ in 0..1024 {
                    match self.waiting.get(&cur).copied() {
                        Some(n) if n == me => return Claim::Deadlock,
                        Some(n) => cur = n,
                        None => break,
                    }
                }
                Claim::Busy(owner)
            }
        }
    }

    pub(crate) fn set_waiting(&mut self, me: u64, owner: u64) {
        self.waiting.insert(me, owner);
    }

    pub(crate) fn clear_waiting(&mut self, me: u64) {
        self.waiting.remove(&me);
    }

    /// Gives back the claims of statement `me`. With `invalidate` the state of
    /// those series is also dropped (the statement did not commit).
    pub(crate) fn release(&mut self, sids: &[u64], me: u64, invalidate: bool) {
        for sid in sids {
            if self.claims.get(sid) == Some(&me) {
                self.claims.remove(sid);
                if invalidate {
                    self.invalidate(*sid);
                }
            }
        }
        self.waiting.remove(&me);
    }

    #[cfg(test)]
    pub(crate) fn max_ts(&self, sid: u64) -> Option<i64> {
        self.series.get(&sid).map(|s| s.max_ts)
    }

    #[cfg(test)]
    pub(crate) fn is_complete(&self, sid: u64) -> bool {
        self.series.get(&sid).is_some_and(|s| s.complete)
    }
}

/// Every extras tuple stored for series `sid` at exactly `ts`, in the chunks of
/// `snap`, in `staged` chunks (not yet listed in the MANIFEST) and in the
/// MemTable of `snap`.
pub(crate) fn existing_extras(
    schema: &Schema,
    spec: &UniqueSpec,
    snap: &Snapshot,
    staged: &[Arc<ChunkMeta>],
    sid: u64,
    ts: i64,
) -> Result<Vec<Extras>> {
    let mut out: Vec<Extras> = Vec::new();
    let mut want = vec![false; schema.columns().len()];
    for &c in spec.extra_columns() {
        if let Some(w) = want.get_mut(c) {
            *w = true;
        }
    }
    let ids: HashSet<u64> = std::iter::once(sid).collect();
    for meta in snap.chunks.iter().chain(staged.iter()) {
        if !meta.overlaps(ts, ts) || !meta.bloom.may_contain(sid) {
            continue;
        }
        let entries: Vec<usize> = match meta.entries_of(&ids) {
            Some(v) => v,
            None => (0..meta.series.len()).collect(),
        };
        for si in entries {
            let Some(e) = meta.series.get(si) else { continue };
            if e.series_id != sid || e.ts_min > ts || e.ts_max < ts {
                continue;
            }
            let Some(win) = load_window(schema, meta, si, (ts, ts), &want, false)? else { continue };
            for i in win.lo..win.hi {
                let mut x = [0i64; MAX_EXTRAS];
                let mut usable = true;
                for (slot, &c) in x.iter_mut().zip(spec.extra_columns()) {
                    let col = win.cols.get(c).and_then(Option::as_ref);
                    let null = col.and_then(|d| d.null_flags()).is_some_and(|n| n.get(i).copied().unwrap_or(true));
                    match col.and_then(|d| d.ints()).and_then(|v| v.get(i)) {
                        Some(v) if !null => *slot = *v,
                        _ => usable = false,
                    }
                }
                if usable {
                    out.push(x);
                }
            }
        }
    }
    for (_, m) in snap.mem.rows_of_series(&ids) {
        if schema.row_ts(&m.row) != ts {
            continue;
        }
        if let Some(x) = spec.extras_of(&m.row) {
            out.push(x);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_in_order_is_always_accepted_and_the_top_is_exact() {
        let mut s = UniqueState::default();
        assert_eq!(s.plan(1, 10, &[0, 0]), Plan::Accept);
        s.accept(1, 10, [5, 0], None);
        assert_eq!(s.plan(1, 11, &[5, 0]), Plan::Accept, "a later timestamp cannot collide");
        assert_eq!(s.plan(1, 10, &[5, 0]), Plan::Duplicate);
        assert_eq!(s.plan(1, 10, &[6, 0]), Plan::Accept, "same time, other extra");
        s.accept(1, 10, [6, 0], None);
        s.accept(1, 10, [7, 0], None);
        for x in [5, 6, 7] {
            assert_eq!(s.plan(1, 10, &[x, 0]), Plan::Duplicate);
        }
        assert_eq!(s.plan(1, 9, &[1, 0]), Plan::Lookup, "older than the highest: look it up");
        s.accept(1, 12, [1, 0], None);
        assert_eq!(s.plan(1, 10, &[5, 0]), Plan::Lookup, "the top moved on");
        assert_eq!(s.plan(2, 1, &[0, 0]), Plan::Accept, "another series is independent");
    }

    #[test]
    fn chunk_state_is_an_incomplete_bound_that_a_lookup_can_complete() {
        let mut s = UniqueState::default();
        s.note_chunk(1, 100);
        assert_eq!(s.max_ts(1), Some(100));
        assert!(!s.is_complete(1));
        assert_eq!(s.plan(1, 101, &[0, 0]), Plan::Accept);
        assert_eq!(s.plan(1, 100, &[3, 0]), Plan::Lookup);
        s.accept(1, 100, [3, 0], Some(&[[1, 0], [2, 0]]));
        assert!(s.is_complete(1));
        for x in [1, 2, 3] {
            assert_eq!(s.plan(1, 100, &[x, 0]), Plan::Duplicate);
        }
        assert_eq!(s.plan(1, 100, &[4, 0]), Plan::Accept);
        // replay never lowers the bound
        s.note_chunk(1, 50);
        assert_eq!(s.max_ts(1), Some(100));
        // a row without usable extras makes the top unknown again
        s.note_row(1, 100, None);
        assert!(!s.is_complete(1));
        assert_eq!(s.plan(1, 100, &[1, 0]), Plan::Lookup);
    }

    #[test]
    fn invalidate_keeps_the_bound_and_forgets_the_extras() {
        let mut s = UniqueState::default();
        s.accept(1, 10, [1, 0], None);
        s.invalidate(1);
        assert_eq!(s.max_ts(1), Some(10));
        assert_eq!(s.plan(1, 10, &[1, 0]), Plan::Lookup);
        assert_eq!(s.plan(1, 11, &[1, 0]), Plan::Accept);
    }

    #[test]
    fn claims_wait_and_detect_cycles() {
        let mut s = UniqueState::default();
        assert_eq!(s.claim(1, 100), Claim::Held { new: true });
        assert_eq!(s.claim(1, 100), Claim::Held { new: false });
        assert_eq!(s.claim(2, 200), Claim::Held { new: true });
        // 100 wants series 2, held by 200
        assert_eq!(s.claim(2, 100), Claim::Busy(200));
        s.set_waiting(100, 200);
        // 200 wants series 1, held by 100 who waits for 200: a cycle
        assert_eq!(s.claim(1, 200), Claim::Deadlock);
        // a third party only waits
        assert_eq!(s.claim(1, 300), Claim::Busy(100));
        s.release(&[2], 200, false);
        assert_eq!(s.claim(2, 100), Claim::Held { new: true });
        // releasing something not owned changes nothing
        s.release(&[1], 999, true);
        assert_eq!(s.claim(1, 300), Claim::Busy(100));
        s.release(&[1], 100, true);
        assert_eq!(s.claim(1, 300), Claim::Held { new: true });
    }

    #[test]
    fn many_extras_at_one_timestamp_use_a_set() {
        let mut s = UniqueState::default();
        for i in 0..1000 {
            assert_ne!(s.plan(1, 7, &[i, 0]), Plan::Duplicate);
            s.accept(1, 7, [i, 0], None);
        }
        assert_eq!(s.plan(1, 7, &[999, 0]), Plan::Duplicate);
        assert_eq!(s.plan(1, 7, &[1000, 0]), Plan::Accept);
    }
}
