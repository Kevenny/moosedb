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
//!   wrong. The state is **bounded** (see [`UniqueState`]): past a byte budget
//!   the coldest series are dropped and a floor (`evicted`) remembers how far
//!   their bounds reached, so an absent series is only trusted above it. It is rebuilt at open from the series entries of the chunks
//!   (`max_ts`, `complete = false`) and the replayed WAL rows.
//! * **key holds** (the default concurrency control). A statement's rows are
//!   invisible to other sessions until it commits, so two statements could both
//!   see "no such key" and both insert it. Each checked row therefore *holds*
//!   its exact key `(series, ts, extras)` from the moment it is checked until the
//!   statement commits or aborts, and the hold is registered before the table
//!   mutex is dropped for the lookup. A statement that wants a key another one
//!   holds waits for it (`Error::Busy`, retried by the caller, the wait itself
//!   happens on a condition variable) and then checks against committed data:
//!   exactly one of two racing inserts of the same key wins, the other gets
//!   `Error::Duplicate`. Statements that insert *different* keys into the same
//!   series never wait for each other, whatever the order. A wait that would
//!   close a cycle (A holds k1 wants k2, B holds k2 wants k1) fails with
//!   `Error::Deadlock`. Holds are charged to the memory budget of the batch and
//!   capped (`HOLDS_PER_BATCH` per statement, `HOLDS_TOTAL` per table).
//! * **series claims** (fallback). Past those caps the rows that follow *claim*
//!   the whole series instead, until the statement ends. A claim is exclusive:
//!   it waits while another statement holds keys in the series or claims it, and
//!   while it lasts other statements wait for the series (even for a key). The
//!   holds a statement already has stay valid until it ends. Claim waits take
//!   part in the same cycle detection.
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
    /// Boxed so a series with one extras tuple (the common case) stays small.
    #[allow(clippy::box_collection)] // the box keeps `Top` at 24 bytes
    Many(Box<HashSet<Extras>>),
}

/// Most extras tuples kept for the top timestamp of one series; more and the
/// top becomes unknown (rows at that instant are answered by a lookup).
const MANY_MAX: usize = 1024;
/// Bytes charged per series entry: the map bucket is ~50 B; 146 B/series of RSS
/// were measured at 2M series with the slack of a power-of-two growth.
const ENTRY_BYTES: usize = 150;
/// Bytes charged per element of a `Top::Many` set.
const MANY_ELEM_BYTES: usize = 32;

fn top_cost(t: &Top) -> usize {
    match t {
        Top::Many(s) => s.capacity() * MANY_ELEM_BYTES,
        _ => 0,
    }
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
                *self = Top::Many(Box::new(s));
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
    /// (`hold` only) the hold budget is spent: claim the whole series instead.
    Fallback,
}

/// Most key holds one statement keeps before it falls back to series claims.
pub(crate) const HOLDS_PER_BATCH: usize = 65_536;
/// Most key holds of all statements of a table together.
pub(crate) const HOLDS_TOTAL: usize = 1 << 20;
/// Bytes a hold is charged: `keys` bucket (40 B + control byte, up to 2x slack
/// right after a growth), its one-element list (24 B, 32 with the allocator),
/// the `(sid, ts)` entry in `held` (16 B, vector growth slack), `holders`.
pub(crate) const HOLD_COST: usize = 160;

/// Unique-key state of a table, guarded by the table mutex.
///
/// Bounded: `series` plus the `Top::Many` sets are charged `ENTRY_BYTES` per
/// series and `MANY_ELEM_BYTES` per element against `settings::unique_state_bytes`
/// (per table). Past it the coldest quarter of the series (lowest `max_ts`) is
/// dropped and `evicted` records the highest bound dropped. Safe because a
/// missing entry then means "bound at most `evicted`": [`UniqueState::plan`]
/// answers `Lookup` at or below it, and entries re-created below it take it as
/// their bound and start incomplete. State is only an accelerator.
#[derive(Debug, Default)]
pub(crate) struct UniqueState {
    series: FastMap<u64, SeriesTop>,
    /// Charged bytes of the `Top::Many` sets in `series`.
    many_bytes: usize,
    /// Highest `max_ts` among the entries dropped to stay in budget.
    evicted: Option<i64>,
    /// Byte budget; 0 = the process setting.
    cap: usize,
    /// series id -> id of the statement (batch) that holds it
    claims: FastMap<u64, u64>,
    /// (series, ts) -> (extras, batch) of the keys held by open statements
    keys: FastMap<(u64, i64), Vec<(Extras, u64)>>,
    /// series id -> statements that hold at least one key in it
    holders: FastMap<u64, Vec<u64>>,
    /// batch id -> the (series, ts) of its holds
    held: FastMap<u64, Vec<(u64, i64)>>,
    /// Total holds in `held`.
    held_total: usize,
    /// Bumped whenever a statement gives back holds or claims: a lookup that
    /// started before the bump may have missed the keys that statement had.
    released: u64,
    /// Test override of the per-batch / per-table caps (0 = the constants).
    batch_cap: usize,
    total_cap: usize,
    /// batch id -> batch id it waits for
    waiting: FastMap<u64, u64>,
}

impl UniqueState {
    /// A DELETE may have removed the rows the extras of any series were
    /// recorded from: forget them all (the bounds stay valid).
    pub(crate) fn invalidate_all(&mut self) {
        for st in self.series.values_mut() {
            st.complete = false;
            st.top = Top::Empty;
        }
        self.many_bytes = 0;
    }

    pub(crate) fn clear(&mut self) {
        self.many_bytes = 0;
        self.evicted = None;
        self.series = FastMap::default();
        self.claims.clear();
        self.keys.clear();
        self.holders.clear();
        self.held.clear();
        self.held_total = 0;
        self.released += 1;
        self.waiting.clear();
    }

    fn budget(&self) -> usize {
        if self.cap > 0 {
            self.cap
        } else {
            usize::try_from(crate::settings::get().unique_state_bytes()).unwrap_or(usize::MAX)
        }
    }

    /// Bytes charged for the state (what the budget limits).
    pub(crate) fn bytes(&self) -> usize {
        self.series.len().saturating_mul(ENTRY_BYTES).saturating_add(self.many_bytes)
    }

    /// Sets the entry of `sid`, dropping the coldest series first if a new
    /// one would not fit.
    fn put(&mut self, sid: u64, new: SeriesTop) {
        if !self.series.contains_key(&sid) {
            self.make_room();
        }
        self.many_bytes += top_cost(&new.top);
        if let Some(old) = self.series.insert(sid, new) {
            self.many_bytes = self.many_bytes.saturating_sub(top_cost(&old.top));
        }
    }

    fn make_room(&mut self) {
        let budget = self.budget();
        while !self.series.is_empty() && self.bytes().saturating_add(ENTRY_BYTES) > budget {
            let mut ts: Vec<i64> = self.series.values().map(|s| s.max_ts).collect();
            let k = ts.len() / 4;
            let (_, cut, _) = ts.select_nth_unstable(k);
            let cut = *cut;
            drop(ts);
            self.evicted = Some(self.evicted.map_or(cut, |e| e.max(cut)));
            let mut freed = 0usize;
            self.series.retain(|_, s| {
                let keep = s.max_ts > cut;
                if !keep {
                    freed += top_cost(&s.top);
                }
                keep
            });
            self.many_bytes = self.many_bytes.saturating_sub(freed);
            // Do not keep the memory of the dropped entries.
            if self.series.len() * 2 < self.series.capacity() {
                self.series.shrink_to_fit();
            }
        }
    }

    /// Entry for a series that has none and now has a row at `ts`: exact above
    /// the eviction floor, otherwise only the floor is known.
    fn fresh(&self, ts: i64, e: Option<Extras>) -> SeriesTop {
        match (self.evicted, e) {
            (Some(f), _) if ts <= f => SeriesTop { max_ts: f, complete: false, top: Top::Empty },
            (_, Some(e)) => SeriesTop { max_ts: ts, complete: true, top: Top::One(e) },
            (_, None) => SeriesTop { max_ts: ts, complete: false, top: Top::Empty },
        }
    }

    /// A chunk holds rows of `sid` up to `ts_max` (their extras are unknown).
    pub(crate) fn note_chunk(&mut self, sid: u64, ts_max: i64) {
        match self.series.get_mut(&sid) {
            None => {
                let f = self.evicted.map_or(ts_max, |f| f.max(ts_max));
                self.put(sid, SeriesTop { max_ts: f, complete: false, top: Top::Empty });
            }
            Some(st) if ts_max > st.max_ts => {
                let old = top_cost(&st.top);
                *st = SeriesTop { max_ts: ts_max, complete: false, top: Top::Empty };
                self.many_bytes = self.many_bytes.saturating_sub(old);
            }
            Some(st) if ts_max == st.max_ts => {
                let old = top_cost(&st.top);
                st.complete = false;
                st.top = Top::Empty;
                self.many_bytes = self.many_bytes.saturating_sub(old);
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

    /// Rows of an unchecked statement (no holds) became visible: lookups that
    /// began before may have missed them.
    pub(crate) fn note_unchecked_commit(&mut self) {
        self.released += 1;
    }

    /// Value to capture (under the mutex) before a lookup that runs without it.
    pub(crate) fn generation(&self) -> u64 {
        self.released
    }

    /// [`UniqueState::accept`] for a row whose lookup began at `generation`: if
    /// a statement released holds since, `existing` may lack keys it committed
    /// (and no hold remembers them), so it must not complete the top.
    pub(crate) fn accept_checked(&mut self, sid: u64, ts: i64, e: Extras, existing: Option<&[Extras]>, generation: u64) {
        let existing = existing.filter(|_| generation == self.released);
        self.accept(sid, ts, e, existing);
    }

    /// Records an accepted row. `existing` = every extras tuple already stored
    /// at `ts` (only used to make a state complete that was not).
    pub(crate) fn accept(&mut self, sid: u64, ts: i64, e: Extras, existing: Option<&[Extras]>) {
        let Some(st) = self.series.get_mut(&sid) else {
            let fresh = self.fresh(ts, Some(e));
            self.put(sid, fresh);
            return;
        };
        let old = top_cost(&st.top);
        if ts > st.max_ts {
            *st = SeriesTop { max_ts: ts, complete: true, top: Top::One(e) };
        } else if ts == st.max_ts {
            if st.complete {
                st.top.insert(e);
            } else if let Some(all) = existing {
                let mut top = Top::Empty;
                for x in all {
                    top.insert(*x);
                }
                // `all` is what is stored; other statements' rows at this instant
                // are in flight, known only by their holds.
                for (x, _) in self.keys.get(&(sid, ts)).into_iter().flatten() {
                    top.insert(*x);
                }
                top.insert(e);
                st.top = top;
                st.complete = true;
            }
            if matches!(&st.top, Top::Many(s) if s.len() > MANY_MAX) {
                st.complete = false;
                st.top = Top::Empty;
            }
        }
        let new = top_cost(&st.top);
        self.many_bytes = (self.many_bytes + new).saturating_sub(old);
    }

    pub(crate) fn plan(&self, sid: u64, ts: i64, e: &Extras) -> Plan {
        let Some(st) = self.series.get(&sid) else {
            return if self.evicted.is_some_and(|f| ts <= f) { Plan::Lookup } else { Plan::Accept };
        };
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
            let old = top_cost(&st.top);
            st.complete = false;
            st.top = Top::Empty;
            self.many_bytes = self.many_bytes.saturating_sub(old);
        }
    }

    /// Does waiting for `from` end up waiting for `me`?
    fn reaches(&self, from: u64, me: u64) -> bool {
        let mut cur = from;
        for _ in 0..1024 {
            match self.waiting.get(&cur).copied() {
                Some(n) if n == me => return true,
                Some(n) => cur = n,
                None => break,
            }
        }
        false
    }

    /// `me` must wait for one of `owners` (none of them is `me`); none = free.
    fn blocked(&self, owners: impl Iterator<Item = u64>, me: u64) -> Claim {
        let mut first = None;
        for o in owners {
            if self.reaches(o, me) {
                return Claim::Deadlock;
            }
            first.get_or_insert(o);
        }
        first.map_or(Claim::Held { new: false }, Claim::Busy)
    }

    /// Claims the whole series for `me`: exclusive, so no other statement may
    /// claim it or hold a key in it.
    pub(crate) fn claim(&mut self, sid: u64, me: u64) -> Claim {
        match self.claims.get(&sid).copied() {
            None => {
                let others = self.holders.get(&sid).into_iter().flatten().copied().filter(|b| *b != me);
                match self.blocked(others, me) {
                    Claim::Held { .. } => {
                        self.claims.insert(sid, me);
                        Claim::Held { new: true }
                    }
                    other => other,
                }
            }
            Some(o) if o == me => Claim::Held { new: false },
            Some(owner) => self.blocked(std::iter::once(owner), me),
        }
    }

    /// Holds the key `(sid, ts, e)` for `me` until [`UniqueState::release`].
    /// `Held { new }`: `new` = a hold was registered (the caller charges it);
    /// `Busy`/`Deadlock`: the series is claimed by, or the key held by, another
    /// statement; `Fallback`: the caps are spent (the caller claims the series).
    pub(crate) fn hold(&mut self, sid: u64, ts: i64, e: Extras, me: u64) -> Claim {
        match self.claims.get(&sid).copied() {
            Some(o) if o == me => return Claim::Held { new: false }, // exclusive already
            Some(o) => return self.blocked(std::iter::once(o), me),
            None => {}
        }
        if let Some((_, b)) = self.keys.get(&(sid, ts)).and_then(|v| v.iter().find(|(x, _)| *x == e)) {
            return if *b == me { Claim::Held { new: false } } else { self.blocked(std::iter::once(*b), me) };
        }
        let bc = if self.batch_cap > 0 { self.batch_cap } else { HOLDS_PER_BATCH };
        let tc = if self.total_cap > 0 { self.total_cap } else { HOLDS_TOTAL };
        let mine = self.held.get(&me).map_or(0, Vec::len);
        if mine >= bc || self.held_total >= tc {
            return Claim::Fallback;
        }
        self.held.entry(me).or_default().push((sid, ts));
        self.held_total += 1;
        self.keys.entry((sid, ts)).or_insert_with(|| Vec::with_capacity(1)).push((e, me));
        let h = self.holders.entry(sid).or_default();
        if !h.contains(&me) {
            h.push(me);
        }
        Claim::Held { new: true }
    }

    pub(crate) fn set_waiting(&mut self, me: u64, owner: u64) {
        self.waiting.insert(me, owner);
    }

    pub(crate) fn clear_waiting(&mut self, me: u64) {
        self.waiting.remove(&me);
    }

    /// Gives back the claims and key holds of statement `me`. With `invalidate` the state of
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
        self.released += 1;
        if let Some(mine) = self.held.remove(&me) {
            self.held_total = self.held_total.saturating_sub(mine.len());
            for (sid, ts) in mine {
                if let Some(v) = self.keys.get_mut(&(sid, ts)) {
                    v.retain(|(_, b)| *b != me);
                    if v.is_empty() {
                        self.keys.remove(&(sid, ts));
                    }
                }
                if let Some(h) = self.holders.get_mut(&sid) {
                    h.retain(|b| *b != me);
                    if h.is_empty() {
                        self.holders.remove(&sid);
                    }
                }
                if invalidate {
                    self.invalidate(sid);
                }
            }
        }
        self.waiting.remove(&me);
    }

    #[cfg(test)]
    pub(crate) fn set_hold_caps(&mut self, per_batch: usize, total: usize) {
        self.batch_cap = per_batch;
        self.total_cap = total;
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
            // Rows a DELETE hid are not there any more.
            if snap.tombs.range_hides(sid, meta.born, ts) {
                continue;
            }
            let Some(win) = load_window(schema, meta, si, (ts, ts), &want, false)? else { continue };
            let holes = snap.tombs.holes(meta.id, si);
            for i in win.lo..win.hi {
                if crate::tomb::in_runs(holes, i) {
                    continue;
                }
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
    fn holds_of_different_keys_in_one_series_coexist_and_a_cycle_is_a_deadlock() {
        let mut s = UniqueState::default();
        assert_eq!(s.hold(1, 10, [0, 0], 100), Claim::Held { new: true });
        assert_eq!(s.hold(1, 10, [0, 0], 100), Claim::Held { new: false });
        assert_eq!(s.hold(1, 11, [0, 0], 200), Claim::Held { new: true }, "same series, other key");
        assert_eq!(s.hold(1, 10, [1, 0], 200), Claim::Held { new: true }, "same instant, other extras");
        assert_eq!(s.hold(1, 10, [0, 0], 200), Claim::Busy(100));
        s.set_waiting(100, 200);
        assert_eq!(s.hold(1, 11, [0, 0], 200), Claim::Held { new: false });
        // 100 waits for 200; 200 now wants 100's key: a cycle
        assert_eq!(s.hold(1, 10, [0, 0], 200), Claim::Deadlock);
        s.release(&[], 100, false);
        assert_eq!(s.hold(1, 10, [0, 0], 200), Claim::Held { new: true });
        s.release(&[], 200, true);
        assert!(s.keys.is_empty() && s.holders.is_empty() && s.held.is_empty() && s.held_total == 0);
    }

    #[test]
    fn past_the_hold_cap_a_statement_claims_the_series_and_excludes_key_holders() {
        let mut s = UniqueState::default();
        s.set_hold_caps(2, 0);
        assert_eq!(s.hold(1, 1, [0, 0], 100), Claim::Held { new: true });
        assert_eq!(s.hold(1, 2, [0, 0], 100), Claim::Held { new: true });
        assert_eq!(s.hold(1, 3, [0, 0], 100), Claim::Fallback);
        assert_eq!(s.claim(1, 100), Claim::Held { new: true }, "only its own holds in the series");
        assert_eq!(s.hold(1, 4, [0, 0], 100), Claim::Held { new: false }, "the claim covers it");
        assert_eq!(s.hold(1, 9, [0, 0], 200), Claim::Busy(100), "claimed series: others wait");
        s.release(&[1], 100, false);
        // a key holder blocks a later claimer
        assert_eq!(s.hold(1, 9, [0, 0], 200), Claim::Held { new: true });
        assert_eq!(s.claim(1, 100), Claim::Busy(200));
        s.set_waiting(200, 100);
        assert_eq!(s.claim(1, 100), Claim::Deadlock, "the holder waits for the claimer");
        s.release(&[], 200, false);
        assert_eq!(s.claim(1, 100), Claim::Held { new: true });
        // the total cap too
        let mut t = UniqueState::default();
        t.set_hold_caps(0, 1);
        assert_eq!(t.hold(1, 1, [0, 0], 1), Claim::Held { new: true });
        assert_eq!(t.hold(2, 1, [0, 0], 2), Claim::Fallback);
    }

    #[test]
    fn a_stale_lookup_does_not_complete_the_top_after_another_statement_released_holds() {
        let mut s = UniqueState::default();
        s.note_chunk(1, 100); // max_ts == 100, top unknown
        // A holds k1 and plans a lookup (nothing stored at 100), mutex dropped
        assert_eq!(s.hold(1, 100, [1, 0], 7), Claim::Held { new: true });
        assert_eq!(s.plan(1, 100, &[1, 0]), Plan::Lookup);
        let gen = s.generation();
        let stale: Vec<Extras> = Vec::new();
        // B takes k2 at the same instant, is accepted without completing the
        // top, commits and lets go of its hold
        assert_eq!(s.hold(1, 100, [2, 0], 8), Claim::Held { new: true });
        s.accept(1, 100, [2, 0], None);
        s.release(&[], 8, false);
        // A comes back with its old `existing`
        s.accept_checked(1, 100, [1, 0], Some(&stale), gen);
        assert!(!s.is_complete(1), "the stale list must not make the top complete");
        assert_eq!(s.plan(1, 100, &[2, 0]), Plan::Lookup, "k2 is not told apart from a new key");
        // without an intervening release the same call does complete it
        let mut t = UniqueState::default();
        t.note_chunk(1, 100);
        let gen = t.generation();
        t.accept_checked(1, 100, [1, 0], Some(&stale), gen);
        assert!(t.is_complete(1));
    }

    #[test]
    fn a_commit_of_unchecked_rows_also_invalidates_lookups_in_flight() {
        let mut s = UniqueState::default();
        s.note_chunk(1, 100);
        assert_eq!(s.hold(1, 100, [1, 0], 7), Claim::Held { new: true });
        let gen = s.generation();
        // B (unique_checks=0) wrote k2 at the same instant, no hold, and commits
        s.note_row(1, 100, Some([2, 0]));
        s.note_unchecked_commit();
        s.accept_checked(1, 100, [1, 0], Some(&[]), gen);
        assert!(!s.is_complete(1), "the stale list must not make the top complete");
        assert_eq!(s.plan(1, 100, &[2, 0]), Plan::Lookup);
    }

    #[test]
    fn completing_the_top_counts_the_keys_other_statements_hold() {
        let mut s = UniqueState::default();
        s.note_chunk(1, 100);
        assert_eq!(s.hold(1, 100, [1, 0], 7), Claim::Held { new: true }); // in flight, not in `top`
        assert_eq!(s.hold(1, 100, [2, 0], 8), Claim::Held { new: true });
        s.accept(1, 100, [2, 0], Some(&[[9, 0]]));
        s.release(&[], 7, false);
        assert_eq!(s.plan(1, 100, &[1, 0]), Plan::Duplicate, "the key 7 held is known");
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

    /// Ground truth for the bounded state: whatever it evicts, `Duplicate` and
    /// `Accept` must always agree with the set of keys really stored.
    fn run_model(cap: usize, seed: u64, rebuild_every: usize) {
        use std::collections::BTreeSet;
        let mut rng = seed;
        let mut next = move |n: u64| {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (rng >> 33) % n
        };
        let fresh = || UniqueState { cap, ..UniqueState::default() };
        let mut s = fresh();
        let mut truth: BTreeSet<(u64, i64, i64)> = BTreeSet::new();
        for step in 0..20_000usize {
            if rebuild_every > 0 && step % rebuild_every == 0 {
                // restart: the state is rebuilt from what is stored (bounds only)
                s = fresh();
                let mut top: std::collections::BTreeMap<u64, i64> = Default::default();
                for &(sid, ts, _) in &truth {
                    let m = top.entry(sid).or_insert(ts);
                    *m = (*m).max(ts);
                }
                for (sid, ts) in top {
                    s.note_chunk(sid, ts);
                }
            }
            let (sid, ts, x) = (next(60), next(25) as i64, next(3) as i64);
            let plan = s.plan(sid, ts, &[x, 0]);
            let stored = truth.contains(&(sid, ts, x));
            match plan {
                Plan::Duplicate => assert!(stored, "false positive at step {step}"),
                Plan::Accept => assert!(!stored, "false negative at step {step}"),
                Plan::Lookup => {}
            }
            if stored {
                continue;
            }
            let existing: Vec<Extras> =
                truth.range((sid, ts, i64::MIN)..=(sid, ts, i64::MAX)).map(|&(_, _, e)| [e, 0]).collect();
            s.accept(sid, ts, [x, 0], (plan == Plan::Lookup).then_some(&existing[..]));
            truth.insert((sid, ts, x));
            assert!(s.bytes() <= cap.max(ENTRY_BYTES) + MANY_ELEM_BYTES * 64, "over budget: {}", s.bytes());
        }
    }

    #[test]
    fn evicting_state_never_changes_the_answer() {
        for (cap, rebuild) in [(5 * ENTRY_BYTES, 0), (12 * ENTRY_BYTES, 0), (12 * ENTRY_BYTES, 777), (1_000 * ENTRY_BYTES, 0)] {
            for seed in 1..6 {
                run_model(cap, seed, rebuild);
            }
        }
    }

    #[test]
    fn eviction_keeps_a_floor_for_absent_series() {
        let mut s = UniqueState { cap: 4 * ENTRY_BYTES, ..UniqueState::default() };
        for sid in 0..4u64 {
            s.accept(sid, 10 + sid as i64, [1, 0], None);
        }
        s.accept(9, 100, [1, 0], None); // does not fit: the coldest are dropped
        assert!(s.series.len() < 5);
        assert_eq!(s.plan(0, 10, &[1, 0]), Plan::Lookup, "dropped series at or below the floor: look it up");
        assert_eq!(s.plan(77, 5, &[1, 0]), Plan::Lookup, "a never-seen series cannot be told from a dropped one");
        assert_eq!(s.plan(77, 1_000, &[1, 0]), Plan::Accept, "above every dropped bound");
        s.clear();
        assert_eq!(s.plan(77, 5, &[1, 0]), Plan::Accept, "TRUNCATE forgets the floor");
    }

    #[test]
    fn a_huge_top_becomes_unknown_instead_of_growing() {
        let mut s = UniqueState::default();
        for i in 0..(MANY_MAX as i64 + 10) {
            s.accept(1, 7, [i, 0], None);
        }
        assert!(!s.is_complete(1));
        assert_eq!(s.plan(1, 7, &[3, 0]), Plan::Lookup);
        assert_eq!(s.plan(1, 8, &[3, 0]), Plan::Accept);
        assert_eq!(s.bytes(), ENTRY_BYTES);
    }

    /// `cargo test -p moosedb-core --release -- --ignored --nocapture unique_state_memory`
    #[test]
    #[ignore = "memory measurement (millions of series)"]
    fn unique_state_memory_per_series() {
        let n: u64 = 2_000_000;
        let before = crate::memprobe::rss_bytes();
        let mut s = UniqueState::default();
        for sid in 0..n {
            s.accept(sid, 1_000 + sid as i64, [sid as i64, 0], None);
        }
        let per = (crate::memprobe::rss_bytes() - before) as f64 / n as f64;
        eprintln!("unique state: {per:.1} B/series ({n} series, accounted {} B)", s.bytes());
        // 8 extras at one timestamp of 50k series
        let before = crate::memprobe::rss_bytes();
        let mut t = UniqueState::default();
        for sid in 0..50_000u64 {
            for x in 0..8 {
                t.accept(sid, 7, [x, 0], None);
            }
        }
        let per = (crate::memprobe::rss_bytes() - before) as f64 / 50_000.0;
        eprintln!("unique state, 8 extras at one ts: {per:.1} B/series (accounted {} B)", t.bytes());
    }
}
