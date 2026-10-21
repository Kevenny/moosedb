//! Tombstones: logical deletes that hide rows of sealed chunks until a
//! compaction (or the chunk's expiry) removes them physically.
//!
//! Two kinds, both kept in memory in a [`TombSet`] and persisted in the
//! MANIFEST (v3) and, at the moment they are created, in the WAL:
//!
//! * **range tombstone** ([`RangeTomb`]): "every row of these series with
//!   `lo <= ts <= hi`". It hides the rows that existed when it was created, not
//!   the ones inserted afterwards, so it carries a sequence number `seq` and
//!   each chunk a `born` (the highest `seq` assigned when the chunk was
//!   created; the rows it holds already reflect every tombstone up to `born`).
//!   A range tombstone applies to a chunk iff `seq > chunk.born`. MemTable rows
//!   are removed physically when the tombstone is created, so only chunks need
//!   the rule. A tombstone costs O(series selected) memory and nothing per row.
//! * **row tombstone** ([`RowsTomb`]): "these ordinals of this series entry of
//!   this chunk". Chunk files are immutable and ids never repeat, so the pair
//!   `(chunk id, entry index)` plus an ordinal names a row for good. Used by
//!   `DELETE` forms that cannot be expressed as a range (any other `WHERE`).
//!
//! Sequence numbers are per table, strictly increasing, never reused (the
//! MANIFEST keeps the highest one even after every tombstone is gone).
//!
//! Reads apply tombstones through [`TombSet::plan`]: the timestamp interval a
//! scan wants is cut into the intervals no range tombstone covers, and the
//! ordinals of row tombstones become holes inside the decoded window.
//! Compaction removes the hidden rows physically and drops the tombstones no
//! chunk needs any more (see `Table::prune_tombs`).

use std::collections::HashSet;
use std::sync::Arc;

use crate::bytes::{put_i64, put_u32, put_u64, put_u8, ByteReader};
use crate::chunk::ChunkMeta;
use crate::error::{corrupt, Result};
use crate::fasthash::FastMap;

/// Series a range tombstone selects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Sel {
    All,
    /// Series ids, sorted and unique.
    Ids(Vec<u64>),
}

impl Sel {
    pub(crate) fn from_ids(mut ids: Vec<u64>) -> Sel {
        ids.sort_unstable();
        ids.dedup();
        Sel::Ids(ids)
    }

    pub(crate) fn contains(&self, id: u64) -> bool {
        match self {
            Sel::All => true,
            Sel::Ids(v) => v.binary_search(&id).is_ok(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RangeTomb {
    pub seq: u64,
    pub lo: i64,
    pub hi: i64,
    pub sel: Sel,
    /// Rows this tombstone hid in each chunk when it was created, `(chunk id,
    /// rows)`; sums to the dead-row count of the chunk (statistics).
    pub hits: Vec<(u64, u64)>,
}

/// Dead ordinals of one series entry of one chunk, as `(start, len)` runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowsTomb {
    pub seq: u64,
    pub chunk: u64,
    pub entry: u32,
    pub runs: Vec<(u32, u32)>,
}

impl RowsTomb {
    pub(crate) fn rows(&self) -> u64 {
        self.runs.iter().map(|&(_, n)| u64::from(n)).sum()
    }
}

/// What a scan of one chunk series entry has to respect.
#[derive(Debug, Default)]
pub(crate) struct EntryPlan {
    /// Disjoint, ascending timestamp intervals (inclusive) still alive within
    /// the wanted range.
    pub ivs: Vec<(i64, i64)>,
    /// Dead ordinals of the entry as sorted, disjoint `(start, len)` runs.
    pub holes: Vec<(u32, u32)>,
}

#[derive(Clone, Default)]
pub(crate) struct TombSet {
    ranges: Vec<Arc<RangeTomb>>,
    /// Series id -> indexes into `ranges` (ascending).
    by_series: FastMap<u64, Vec<u32>>,
    /// Indexes into `ranges` of the tombstones selecting every series.
    global: Vec<u32>,
    rows: Vec<Arc<RowsTomb>>,
    /// `(chunk, entry)` -> merged dead runs.
    row_index: FastMap<(u64, u32), Vec<(u32, u32)>>,
    /// Dead rows per chunk.
    dead: FastMap<u64, u64>,
}

/// Union of two sorted, disjoint run lists.
fn merge_runs(a: &[(u32, u32)], b: &[(u32, u32)]) -> Vec<(u32, u32)> {
    let mut all: Vec<(u32, u32)> = a.iter().chain(b).copied().filter(|r| r.1 > 0).collect();
    all.sort_unstable();
    let mut out: Vec<(u32, u32)> = Vec::with_capacity(all.len());
    for (s, n) in all {
        let end = u64::from(s) + u64::from(n);
        match out.last_mut() {
            Some(last) if u64::from(s) <= u64::from(last.0) + u64::from(last.1) => {
                let new_end = (u64::from(last.0) + u64::from(last.1)).max(end);
                last.1 = u32::try_from(new_end - u64::from(last.0)).unwrap_or(u32::MAX);
            }
            _ => out.push((s, n)),
        }
    }
    out
}

/// Whether ordinal `i` lies in one of the sorted, disjoint `runs` (binary search).
pub(crate) fn in_runs(runs: &[(u32, u32)], i: usize) -> bool {
    let i = i as u64;
    let p = runs.partition_point(|&(s, _)| u64::from(s) <= i);
    p > 0 && i < u64::from(runs[p - 1].0) + u64::from(runs[p - 1].1)
}

/// Cuts `[lo, hi)` around the dead `runs`: the alive sub-ranges, ascending.
pub(crate) fn split_by_holes(lo: usize, hi: usize, runs: &[(u32, u32)]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut cur = lo;
    for &(s, n) in runs {
        let (s, e) = (s as usize, s as usize + n as usize);
        if e <= cur {
            continue;
        }
        if s >= hi {
            break;
        }
        if s > cur {
            out.push((cur, s.min(hi)));
        }
        cur = cur.max(e);
        if cur >= hi {
            break;
        }
    }
    if cur < hi {
        out.push((cur, hi));
    }
    out
}

impl TombSet {
    pub(crate) fn is_empty(&self) -> bool {
        self.ranges.is_empty() && self.rows.is_empty()
    }

    pub(crate) fn ranges(&self) -> &[Arc<RangeTomb>] {
        &self.ranges
    }

    pub(crate) fn rows_tombs(&self) -> &[Arc<RowsTomb>] {
        &self.rows
    }

    /// Rows of `chunk` hidden by tombstones.
    pub(crate) fn dead_rows(&self, chunk: u64) -> u64 {
        self.dead.get(&chunk).copied().unwrap_or(0)
    }

    pub(crate) fn dead_total(&self) -> u64 {
        self.dead.values().fold(0u64, |a, &b| a.saturating_add(b))
    }

    /// Highest sequence number present.
    pub(crate) fn max_seq(&self) -> u64 {
        let r = self.ranges.last().map_or(0, |t| t.seq);
        let w = self.rows.last().map_or(0, |t| t.seq);
        r.max(w)
    }

    fn index_range(&mut self, i: u32) {
        let t = self.ranges[i as usize].clone();
        match &t.sel {
            Sel::All => self.global.push(i),
            Sel::Ids(ids) => {
                for id in ids {
                    self.by_series.entry(*id).or_default().push(i);
                }
            }
        }
        for &(c, n) in &t.hits {
            let d = self.dead.entry(c).or_default();
            *d = d.saturating_add(n);
        }
    }

    pub(crate) fn add_range(&mut self, t: RangeTomb) {
        let i = self.ranges.len() as u32;
        self.ranges.push(Arc::new(t));
        self.index_range(i);
    }

    pub(crate) fn add_rows(&mut self, t: RowsTomb) {
        let rows = t.rows();
        let key = (t.chunk, t.entry);
        let merged = merge_runs(self.row_index.get(&key).map_or(&[][..], Vec::as_slice), &t.runs);
        self.row_index.insert(key, merged);
        let d = self.dead.entry(t.chunk).or_default();
        *d = d.saturating_add(rows);
        self.rows.push(Arc::new(t));
    }

    fn rebuild(&mut self) {
        let ranges = std::mem::take(&mut self.ranges);
        let rows = std::mem::take(&mut self.rows);
        self.by_series.clear();
        self.global.clear();
        self.row_index.clear();
        self.dead.clear();
        for t in ranges {
            let i = self.ranges.len() as u32;
            self.ranges.push(t);
            self.index_range(i);
        }
        for t in rows {
            let key = (t.chunk, t.entry);
            let merged = merge_runs(self.row_index.get(&key).map_or(&[][..], Vec::as_slice), &t.runs);
            self.row_index.insert(key, merged);
            let d = self.dead.entry(t.chunk).or_default();
            *d = d.saturating_add(t.rows());
            self.rows.push(t);
        }
    }

    /// Range tombstones that apply to rows of series `sid` held by a chunk
    /// whose `born` is given.
    fn applicable(&self, sid: u64, born: u64) -> impl Iterator<Item = &RangeTomb> + '_ {
        let own = self.by_series.get(&sid).map_or(&[][..], Vec::as_slice);
        own.iter().chain(&self.global).map(|&i| &*self.ranges[i as usize]).filter(move |t| t.seq > born)
    }

    /// Whether row `(sid, ts)` of a chunk born at `born` is hidden by a range
    /// tombstone (row tombstones are not consulted).
    pub(crate) fn range_hides(&self, sid: u64, born: u64, ts: i64) -> bool {
        self.applicable(sid, born).any(|t| t.lo <= ts && ts <= t.hi)
    }

    /// Dead runs of one series entry of a chunk.
    pub(crate) fn holes(&self, chunk: u64, entry: usize) -> &[(u32, u32)] {
        match u32::try_from(entry) {
            Ok(e) => self.row_index.get(&(chunk, e)).map_or(&[][..], Vec::as_slice),
            Err(_) => &[],
        }
    }

    /// The parts of `[qlo, qhi]` that no range tombstone covers for series
    /// `sid` in a chunk born at `born`.
    pub(crate) fn alive_intervals(&self, sid: u64, born: u64, qlo: i64, qhi: i64) -> Vec<(i64, i64)> {
        if qlo > qhi {
            return Vec::new();
        }
        let mut cuts: Vec<(i64, i64)> = self
            .applicable(sid, born)
            .filter(|t| t.hi >= qlo && t.lo <= qhi)
            .map(|t| (t.lo.max(qlo), t.hi.min(qhi)))
            .collect();
        if cuts.is_empty() {
            return vec![(qlo, qhi)];
        }
        cuts.sort_unstable();
        let mut out = Vec::new();
        let mut cur = i128::from(qlo);
        for (a, b) in cuts {
            if i128::from(a) > cur {
                out.push((cur as i64, (i128::from(a) - 1) as i64));
            }
            cur = cur.max(i128::from(b) + 1);
        }
        if cur <= i128::from(qhi) {
            out.push((cur as i64, qhi));
        }
        out
    }

    /// Everything a scan needs to know about entry `entry` (series `sid`) of
    /// `chunk` for the wanted range.
    pub(crate) fn plan(&self, chunk: &ChunkMeta, entry: usize, sid: u64, qlo: i64, qhi: i64) -> EntryPlan {
        EntryPlan {
            ivs: self.alive_intervals(sid, chunk.born, qlo, qhi),
            holes: self.holes(chunk.id, entry).to_vec(),
        }
    }

    /// Forgets what the chunks in `gone` held: their row tombstones, their hit
    /// counts. Range tombstones that no longer hit anything stay until
    /// [`TombSet::prune`] decides.
    pub(crate) fn forget_chunks(&mut self, gone: &HashSet<u64>) {
        if gone.is_empty() {
            return;
        }
        let touched = self.rows.iter().any(|t| gone.contains(&t.chunk))
            || self.ranges.iter().any(|t| t.hits.iter().any(|(c, _)| gone.contains(c)));
        if !touched {
            return;
        }
        self.rows.retain(|t| !gone.contains(&t.chunk));
        for t in &mut self.ranges {
            if t.hits.iter().any(|(c, _)| gone.contains(c)) {
                Arc::make_mut(t).hits.retain(|(c, _)| !gone.contains(c));
            }
        }
        self.rebuild();
    }

    /// Drops the range tombstones no live chunk can still be affected by: one
    /// applies to a chunk born before it whose timestamps it reaches.
    pub(crate) fn prune(&mut self, chunks: &[Arc<ChunkMeta>]) -> usize {
        let before = self.ranges.len();
        self.ranges.retain(|t| chunks.iter().any(|c| c.born < t.seq && c.overlaps(t.lo, t.hi)));
        let dropped = before - self.ranges.len();
        if dropped > 0 {
            self.rebuild();
        }
        dropped
    }

    /// A compaction replaced `inputs` by new chunks, applying every tombstone
    /// up to `upto` physically. Range tombstones newer than that still hide
    /// rows of the output: `fresh` holds, per such tombstone (by `seq`), the
    /// rows it hides in each output chunk (counted by the caller); they replace
    /// the hits on the inputs.
    pub(crate) fn rehit(&mut self, inputs: &HashSet<u64>, upto: u64, fresh: &[(u64, Vec<(u64, u64)>)]) {
        let mut changed = false;
        for t in &mut self.ranges {
            if t.seq <= upto {
                continue;
            }
            let Some((_, hits)) = fresh.iter().find(|(s, _)| *s == t.seq) else { continue };
            let tomb = Arc::make_mut(t);
            tomb.hits.retain(|(c, _)| !inputs.contains(c));
            tomb.hits.extend(hits.iter().copied());
            changed = true;
        }
        if changed {
            self.rebuild();
        }
    }
}

// ─── persistence ─────────────────────────────────────────────────────────────

/// Most `(chunk, rows)` pairs, runs or series ids a decoder accepts before it
/// looks at how many bytes are left (the list is also bounded by those bytes).
const MAX_LIST: usize = 1 << 28;

pub(crate) fn encode_sel(buf: &mut Vec<u8>, sel: &Sel) {
    match sel {
        Sel::All => put_u8(buf, 0),
        Sel::Ids(ids) => {
            put_u8(buf, 1);
            put_u32(buf, ids.len() as u32);
            for id in ids {
                put_u64(buf, *id);
            }
        }
    }
}

pub(crate) fn decode_sel(r: &mut ByteReader<'_>) -> Result<Sel> {
    match r.u8()? {
        0 => Ok(Sel::All),
        1 => {
            let n = r.u32()? as usize;
            if n > MAX_LIST || n.checked_mul(8).map_or(true, |b| b > r.remaining()) {
                return Err(corrupt("tombstone series list longer than its data"));
            }
            let ids = (0..n).map(|_| r.u64()).collect::<Result<Vec<_>>>()?;
            if ids.windows(2).any(|w| w[0] >= w[1]) {
                return Err(corrupt("tombstone series list not sorted"));
            }
            Ok(Sel::Ids(ids))
        }
        k => Err(corrupt(format!("unknown tombstone selection {k}"))),
    }
}

pub(crate) fn encode_range(buf: &mut Vec<u8>, t: &RangeTomb) {
    put_u64(buf, t.seq);
    put_i64(buf, t.lo);
    put_i64(buf, t.hi);
    encode_sel(buf, &t.sel);
    put_u32(buf, t.hits.len() as u32);
    for &(c, n) in &t.hits {
        put_u64(buf, c);
        put_u64(buf, n);
    }
}

pub(crate) fn decode_range(r: &mut ByteReader<'_>) -> Result<RangeTomb> {
    let seq = r.u64()?;
    let lo = r.i64()?;
    let hi = r.i64()?;
    let sel = decode_sel(r)?;
    let n = r.u32()? as usize;
    if n > MAX_LIST || n.checked_mul(16).map_or(true, |b| b > r.remaining()) {
        return Err(corrupt("tombstone hit list longer than its data"));
    }
    let hits = (0..n).map(|_| Ok((r.u64()?, r.u64()?))).collect::<Result<Vec<_>>>()?;
    Ok(RangeTomb { seq, lo, hi, sel, hits })
}

pub(crate) fn encode_rows(buf: &mut Vec<u8>, t: &RowsTomb) {
    put_u64(buf, t.seq);
    put_u64(buf, t.chunk);
    put_u32(buf, t.entry);
    put_u32(buf, t.runs.len() as u32);
    for &(s, n) in &t.runs {
        put_u32(buf, s);
        put_u32(buf, n);
    }
}

pub(crate) fn decode_rows(r: &mut ByteReader<'_>) -> Result<RowsTomb> {
    let seq = r.u64()?;
    let chunk = r.u64()?;
    let entry = r.u32()?;
    let n = r.u32()? as usize;
    if n > MAX_LIST || n.checked_mul(8).map_or(true, |b| b > r.remaining()) {
        return Err(corrupt("row tombstone run list longer than its data"));
    }
    let runs = (0..n).map(|_| Ok((r.u32()?, r.u32()?))).collect::<Result<Vec<_>>>()?;
    let mut prev_end = 0u64;
    for &(s, len) in &runs {
        if len == 0 || u64::from(s) < prev_end || u64::from(s) + u64::from(len) > u64::from(u32::MAX) {
            return Err(corrupt("row tombstone runs overlap or are empty"));
        }
        prev_end = u64::from(s) + u64::from(len);
    }
    Ok(RowsTomb { seq, chunk, entry, runs })
}

/// Turns sorted ordinals into runs.
pub(crate) fn runs_of(ordinals: &[u32]) -> Vec<(u32, u32)> {
    let mut out: Vec<(u32, u32)> = Vec::new();
    for &o in ordinals {
        match out.last_mut() {
            Some(last) if u64::from(o) == u64::from(last.0) + u64::from(last.1) => last.1 += 1,
            _ => out.push((o, 1)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(seq: u64, lo: i64, hi: i64, ids: &[u64]) -> RangeTomb {
        RangeTomb { seq, lo, hi, sel: Sel::from_ids(ids.to_vec()), hits: vec![] }
    }

    #[test]
    fn alive_intervals_cut_around_the_tombstones_that_apply() {
        let mut s = TombSet::default();
        s.add_range(range(5, 10, 20, &[1]));
        s.add_range(range(6, 15, 30, &[1, 2]));
        s.add_range(RangeTomb { seq: 7, lo: 100, hi: 200, sel: Sel::All, hits: vec![] });
        // series 1, chunk older than every tombstone
        assert_eq!(s.alive_intervals(1, 0, 0, 300), vec![(0, 9), (31, 99), (201, 300)]);
        // series 3 only sees the global one
        assert_eq!(s.alive_intervals(3, 0, 0, 300), vec![(0, 99), (201, 300)]);
        // a chunk born after tombstone 5 is not hidden by it
        assert_eq!(s.alive_intervals(1, 5, 0, 20), vec![(0, 14)]);
        // born after everything: nothing applies
        assert_eq!(s.alive_intervals(1, 7, 0, 300), vec![(0, 300)]);
        // fully covered, extremes
        assert_eq!(s.alive_intervals(1, 0, 12, 18), Vec::<(i64, i64)>::new());
        let mut e = TombSet::default();
        e.add_range(RangeTomb { seq: 1, lo: i64::MIN, hi: 50, sel: Sel::All, hits: vec![] });
        assert_eq!(e.alive_intervals(9, 0, i64::MIN, i64::MAX), vec![(51, i64::MAX)]);
        let mut e = TombSet::default();
        e.add_range(RangeTomb { seq: 1, lo: 50, hi: i64::MAX, sel: Sel::All, hits: vec![] });
        assert_eq!(e.alive_intervals(9, 0, i64::MIN, i64::MAX), vec![(i64::MIN, 49)]);
        assert!(s.range_hides(1, 0, 12) && !s.range_hides(1, 7, 12) && !s.range_hides(3, 0, 12));
    }

    #[test]
    fn row_runs_merge_and_split_windows() {
        let mut s = TombSet::default();
        s.add_rows(RowsTomb { seq: 1, chunk: 4, entry: 2, runs: vec![(3, 2), (10, 1)] });
        s.add_rows(RowsTomb { seq: 2, chunk: 4, entry: 2, runs: vec![(5, 1), (20, 4)] });
        assert_eq!(s.holes(4, 2), &[(3, 3), (10, 1), (20, 4)]);
        assert_eq!(s.holes(4, 3), &[] as &[(u32, u32)]);
        assert_eq!(s.dead_rows(4), 2 + 1 + 1 + 4);
        assert_eq!(split_by_holes(0, 30, s.holes(4, 2)), vec![(0, 3), (6, 10), (11, 20), (24, 30)]);
        assert_eq!(split_by_holes(4, 8, s.holes(4, 2)), vec![(6, 8)]);
        assert_eq!(split_by_holes(3, 6, s.holes(4, 2)), Vec::<(usize, usize)>::new());
        assert_eq!(runs_of(&[1, 2, 3, 7, 9, 10]), vec![(1, 3), (7, 1), (9, 2)]);
    }

    #[test]
    fn encoding_roundtrips_and_rejects_forged_lengths() {
        let t = RangeTomb { seq: 9, lo: -5, hi: 77, sel: Sel::from_ids(vec![9, 3, 3, 5]), hits: vec![(1, 10), (4, 2)] };
        let mut b = Vec::new();
        encode_range(&mut b, &t);
        assert_eq!(decode_range(&mut ByteReader::new(&b)).unwrap(), t);
        let w = RowsTomb { seq: 3, chunk: 8, entry: 1, runs: vec![(0, 2), (9, 4)] };
        let mut b = Vec::new();
        encode_rows(&mut b, &w);
        assert_eq!(decode_rows(&mut ByteReader::new(&b)).unwrap(), w);
        // a series list that claims more ids than there are bytes
        let mut b = vec![1u8];
        put_u32(&mut b, u32::MAX);
        assert!(decode_sel(&mut ByteReader::new(&b)).is_err());
        // overlapping runs
        let bad = RowsTomb { seq: 1, chunk: 1, entry: 0, runs: vec![(0, 5), (3, 2)] };
        let mut b = Vec::new();
        encode_rows(&mut b, &bad);
        assert!(decode_rows(&mut ByteReader::new(&b)).is_err());
    }

    #[test]
    fn in_runs_is_a_binary_search_over_sorted_runs() {
        let runs = [(3u32, 3u32), (10, 1), (20, 4)];
        for i in 0..30usize {
            let want = runs.iter().any(|&(s, n)| (s as usize..s as usize + n as usize).contains(&i));
            assert_eq!(in_runs(&runs, i), want, "{i}");
        }
        assert!(!in_runs(&[], 0));
    }

    #[test]
    fn rehit_replaces_only_newer_tombstones_hits() {
        let mut s = TombSet::default();
        s.add_range(RangeTomb { seq: 1, lo: 0, hi: 9, sel: Sel::All, hits: vec![(1, 5), (2, 7)] });
        s.add_range(RangeTomb { seq: 4, lo: 0, hi: 9, sel: Sel::All, hits: vec![(1, 2), (3, 1)] });
        let inputs: HashSet<u64> = [1, 2].into_iter().collect();
        s.rehit(&inputs, 3, &[(4, vec![(9, 2)])]);
        assert_eq!(s.dead_rows(1), 5, "older tombstones keep theirs until the caller forgets the inputs");
        assert_eq!(s.dead_rows(2), 7, "tombstone 1 is applied physically by the caller, which drops its hits");
        assert_eq!(s.dead_rows(9), 2);
        assert_eq!(s.dead_rows(3), 1);
    }
}
