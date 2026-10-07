//! Columnar reads: projection, timestamp pruning inside a series, batches.
//!
//! Every result is compared with what the row-at-a-time, all-columns scan of
//! the same data returns, and that with a naive filter of what was written.

use super::helpers::*;
use moosedb_core::{Error, Position, RawOptions, Row, Scan, ScanFilter, Table, Value};

fn lcg(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
    *seed >> 33
}

fn ts(r: &[Value]) -> i64 {
    match r[0] {
        Value::Timestamp(t) => t,
        _ => panic!("no timestamp"),
    }
}

/// Five hourly buckets over four hosts: four sealed chunks (NULLs in `value`
/// and `note`, duplicate timestamps) and a MemTable tail.
fn build(dir: &std::path::Path) -> Table {
    let t = create_open(&dir.join("t"), RawOptions { chunk_interval: Some("1 HOUR"), ..Default::default() });
    let mut seed = 11;
    for round in 0..5 {
        for _ in 0..600 {
            let sec = (lcg(&mut seed) % 18_000) as i64;
            let host = ["a", "b", "c", "d"][(lcg(&mut seed) % 4) as usize];
            let mut r = row(sec, host, round as f64 + (sec % 100) as f64 / 4.0);
            if sec % 7 == 0 {
                r[3] = Value::Null;
            }
            if sec % 11 == 0 {
                r[4] = Value::Null;
            }
            t.write(r).unwrap();
        }
        if round < 4 {
            t.flush().unwrap();
        }
    }
    t
}

fn masked(r: &Row, mask: &[bool]) -> Row {
    r.iter().zip(mask).map(|(v, &m)| if m { v.clone() } else { Value::Null }).collect()
}

fn drain_rows(s: &mut Scan, backward: bool) -> Vec<(Position, Row)> {
    let mut out = Vec::new();
    loop {
        let next = if backward { s.prev_row() } else { s.next_row() };
        match next.unwrap() {
            Some(x) => out.push(x),
            None => return out,
        }
    }
}

fn drain_batches(s: &mut Scan, max: usize, backward: bool) -> Vec<(Position, Row)> {
    let mut out = Vec::new();
    loop {
        let b = s.next_batch(max, backward).unwrap();
        if b.rows() == 0 {
            return out;
        }
        assert!(b.rows() <= max.max(1));
        assert_eq!(b.positions().len(), b.rows());
        for i in 0..b.rows() {
            out.push((Position::from_bytes(&b.positions()[i]), b.row_at(i)));
        }
    }
}

fn masks(n: usize) -> Vec<Vec<bool>> {
    let mut v = vec![vec![true; n], vec![false; n]];
    for only in 0..n {
        v.push((0..n).map(|c| c == only).collect());
    }
    v.push(vec![false, true, true, false, false, false]); // tags only
    v.push(vec![false, false, false, true, true, true]); // data, no ts
    v.push(vec![true, false, false, false, false, true]);
    v
}

fn filters() -> Vec<ScanFilter> {
    let h = |n: i64| base_ts() + n * 1_000_000;
    let mut tagged = ScanFilter::range(h(3_000), h(15_000));
    tagged.tags.push((1, Value::Bytes(b"b".to_vec())));
    vec![
        ScanFilter::default(),
        ScanFilter::range(h(4_000), h(4_000 + 3_599)),
        ScanFilter::range(h(3_599), h(3_601)),
        ScanFilter::range(h(7_200), h(7_200)),
        ScanFilter::range(h(-50), h(10)),
        ScanFilter::range(h(17_000), h(99_000)),
        ScanFilter::range(h(9), h(8)),
        tagged,
    ]
}

#[test]
fn projection_returns_requested_columns_and_nulls_elsewhere() {
    let dir = tempfile::tempdir().unwrap();
    let t = build(dir.path());
    for f in filters() {
        for sorted in [false, true] {
            let full = drain_rows(&mut t.scan(&f, sorted).unwrap(), false);
            for mask in masks(6) {
                let want: Vec<_> = full.iter().map(|(p, r)| (*p, masked(r, &mask))).collect();
                let got = drain_rows(&mut t.scan_projected(&f, sorted, Some(&mask)).unwrap(), false);
                assert_eq!(got, want, "mask {mask:?} sorted {sorted} range {}..{}", f.ts_min, f.ts_max);
            }
        }
    }
}

#[test]
fn range_scans_match_a_naive_filter() {
    let dir = tempfile::tempdir().unwrap();
    let t = build(dir.path());
    let all = all_rows(&t);
    for f in filters() {
        let naive: Vec<Row> = all
            .iter()
            .filter(|r| (f.ts_min..=f.ts_max).contains(&ts(r)) && f.tags.iter().all(|(c, v)| &r[*c] == v))
            .cloned()
            .collect();
        let stream: Vec<Row> = drain_rows(&mut t.scan(&f, false).unwrap(), false).into_iter().map(|x| x.1).collect();
        assert_eq!(sorted(stream), sorted(naive.clone()));
        let mut s = t.scan(&f, true).unwrap();
        let asc = drain_rows(&mut s, false);
        assert!(asc.windows(2).all(|w| ts(&w[0].1) <= ts(&w[1].1)));
        assert_eq!(sorted(asc.iter().map(|x| x.1.clone()).collect()), sorted(naive));
        let mut s = t.scan(&f, true).unwrap();
        s.seek_end().unwrap();
        let desc = drain_rows(&mut s, true);
        let rev: Vec<_> = asc.iter().rev().cloned().collect();
        assert_eq!(desc, rev, "descending is exactly the reverse of ascending");
    }
}

#[test]
fn batches_equal_rows_for_every_size_mode_and_projection() {
    let dir = tempfile::tempdir().unwrap();
    let t = build(dir.path());
    let snap = t.snapshot().unwrap();
    for f in filters() {
        for sorted in [false, true] {
            for mask in masks(6).into_iter().take(5) {
                let by_row = drain_rows(&mut t.scan_projected(&f, sorted, Some(&mask)).unwrap(), false);
                for size in [1, 2, 7, 64, 1000, 1 << 20] {
                    let by_batch = drain_batches(&mut t.scan_projected(&f, sorted, Some(&mask)).unwrap(), size, false);
                    assert_eq!(by_batch, by_row, "size {size} sorted {sorted} mask {mask:?}");
                }
                // Positions resolve to the same rows (rnd_pos).
                for (p, r) in by_row.iter().step_by(37) {
                    assert_eq!(masked(&snap.fetch(*p).unwrap(), &mask), *r);
                }
                if sorted {
                    for size in [1, 5, 1000] {
                        let mut s = t.scan_projected(&f, true, Some(&mask)).unwrap();
                        s.seek_end().unwrap();
                        let desc = drain_batches(&mut s, size, true);
                        let rev: Vec<_> = by_row.iter().rev().cloned().collect();
                        assert_eq!(desc, rev, "backward size {size} mask {mask:?}");
                    }
                }
            }
        }
    }
}

#[test]
fn a_batch_continues_from_its_last_row_when_the_direction_changes() {
    let dir = tempfile::tempdir().unwrap();
    let t = build(dir.path());
    let f = ScanFilter::default();
    let asc = drain_rows(&mut t.scan(&f, true).unwrap(), false);
    let mut s = t.scan(&f, true).unwrap();
    assert_eq!(s.next_batch(10, false).unwrap().rows(), 10);
    // The cursor is on the 10th row: backwards continues with the 9th.
    let b = s.next_batch(4, true).unwrap();
    let got: Vec<Position> = (0..b.rows()).map(|i| Position::from_bytes(&b.positions()[i])).collect();
    let want: Vec<Position> = asc[..9].iter().rev().take(4).map(|x| x.0).collect();
    assert_eq!(got, want);
    // And forwards again from the 6th.
    let b = s.next_batch(3, false).unwrap();
    let got: Vec<Position> = (0..b.rows()).map(|i| Position::from_bytes(&b.positions()[i])).collect();
    assert_eq!(got, asc[6..9].iter().map(|x| x.0).collect::<Vec<_>>());
}

#[test]
fn batch_edge_cases() {
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), RawOptions::default());
    // Empty table: an empty batch, repeatedly, in both modes.
    for sorted in [false, true] {
        let mut s = t.scan(&ScanFilter::default(), sorted).unwrap();
        assert_eq!(s.next_batch(10, false).unwrap().rows(), 0);
        assert_eq!(s.next_batch(10, false).unwrap().rows(), 0);
    }
    for i in 0..5 {
        t.write(row(i, "a", 1.0)).unwrap();
    }
    // Unsorted scans cannot go backwards, and the failed call leaves it usable.
    let mut s = t.scan(&ScanFilter::default(), false).unwrap();
    assert!(matches!(s.next_batch(10, true), Err(Error::Unsupported(_))));
    assert_eq!(s.next_batch(100, false).unwrap().rows(), 5);
    assert_eq!(s.next_batch(100, false).unwrap().rows(), 0);
    // max_rows = 0 selects the default size.
    let mut s = t.scan(&ScanFilter::default(), false).unwrap();
    assert_eq!(s.next_batch(0, false).unwrap().rows(), 5);
    // A projection of the wrong length is refused.
    assert!(matches!(t.scan_projected(&ScanFilter::default(), false, Some(&[true])), Err(Error::InvalidArg(_))));
    // Columns that were not requested are absent from the batch.
    let mut s =
        t.scan_projected(&ScanFilter::default(), false, Some(&[false, false, false, true, false, false])).unwrap();
    let b = s.next_batch(10, false).unwrap();
    let present: Vec<bool> = b.columns().iter().map(|c| c.is_present()).collect();
    assert_eq!(present, [false, false, false, true, false, false]);
    assert_eq!(b.columns()[3].values_w64().len(), 5);
    assert!(b.columns()[3].nulls().is_none());
}

#[test]
fn sealed_chunks_and_memtable_rows_share_one_batch() {
    let dir = tempfile::tempdir().unwrap();
    let t = create_open(&dir.path().join("t"), RawOptions::default());
    for i in 0..10 {
        t.write(row(i, "a", i as f64)).unwrap();
    }
    t.flush().unwrap();
    for i in 10..20 {
        t.write(row(i, "b", i as f64)).unwrap();
    }
    let mut s = t.scan(&ScanFilter::default(), false).unwrap();
    let b = s.next_batch(100, false).unwrap();
    assert_eq!(b.rows(), 20);
    let host = &b.columns()[1];
    let h: Vec<&[u8]> = (0..20).map(|i| &host.data()[host.starts()[i] as usize..][..host.lens()[i] as usize]).collect();
    assert!(h[..10].iter().all(|x| *x == b"a") && h[10..].iter().all(|x| *x == b"b"));
    assert_eq!(host.starts()[0], host.starts()[9], "a TAG is stored once per series run");
}

/// Batches that stop at either end of a materialized scan leave the cursor on
/// their last row: the next call in the other direction neither repeats nor
/// skips a row. Random walks over sorted scans (chunks + MemTable), compared
/// with a model cursor.
#[test]
fn partial_batches_at_the_ends_keep_the_cursor_on_the_last_returned_row() {
    let dir = tempfile::tempdir().unwrap();
    let t = build(dir.path());
    let all = drain_rows(&mut t.scan(&ScanFilter::default(), true).unwrap(), false);
    let n = all.len() as isize;
    let mut seed = 99;
    for walk in 0..20 {
        let mut s = t.scan(&ScanFilter::default(), true).unwrap();
        let mut cur: isize = -1; // index of the row returned last
        if walk % 2 == 1 {
            s.seek_end().unwrap();
            cur = n;
        }
        for step in 0..60 {
            let sizes = [1usize, 2, 5, 50, 100_000];
            let size = sizes[(lcg(&mut seed) % 5) as usize];
            let mut backward = lcg(&mut seed) % 2 == 0;
            // Skip walks into an empty direction (parking semantics differ at the ends).
            if backward && cur <= 0 || !backward && cur >= n - 1 {
                backward = !backward;
            }
            if backward && cur <= 0 || !backward && cur >= n - 1 {
                break;
            }
            let b = s.next_batch(size, backward).unwrap();
            let got: Vec<Position> = (0..b.rows()).map(|i| Position::from_bytes(&b.positions()[i])).collect();
            let want: Vec<Position> = if backward {
                (0..cur).rev().take(size).map(|i| all[i as usize].0).collect()
            } else {
                (cur + 1..n).take(size).map(|i| all[i as usize].0).collect()
            };
            assert_eq!(got, want, "walk {walk} step {step} backward {backward} size {size} cur {cur}");
            cur = if backward { cur - got.len() as isize } else { cur + got.len() as isize };
        }
    }
}
