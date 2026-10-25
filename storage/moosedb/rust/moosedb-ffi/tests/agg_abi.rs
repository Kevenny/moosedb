//! The aggregation entry points as `ha_moosedb.cc` will call them.

use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr;

use moosedb::*;

type FfiResult<T> = Result<T, (TFStatus, String)>;

fn check(s: TFStatus) -> FfiResult<()> {
    if s == TFStatus::TF_OK {
        return Ok(());
    }
    let p = moosedb_last_error();
    let msg = if p.is_null() { String::new() } else { unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned() };
    unsafe { moosedb_free_str(p) };
    Err((s, msg))
}

fn status(r: FfiResult<()>) -> TFStatus {
    r.err().map_or(TFStatus::TF_OK, |e| e.0)
}

/// `(ts, host TAG, n INT64, name VARCHAR, f DOUBLE)`.
fn open_table(path: &str) -> *mut MooseDBTable {
    let strings: Vec<CString> =
        ["ts", "host", "n", "name", "f", "1 DAY"].iter().map(|s| CString::new(*s).unwrap()).collect();
    let names: Vec<*const c_char> = strings[..5].iter().map(|c| c.as_ptr()).collect();
    let types = [
        TFColumnType::TF_COL_TIMESTAMP as u8,
        TFColumnType::TF_COL_TAG as u8,
        TFColumnType::TF_COL_INT64 as u8,
        TFColumnType::TF_COL_VARCHAR as u8,
        TFColumnType::TF_COL_FLOAT64 as u8,
    ];
    let cfg = TFTableConfig {
        data_dir: ptr::null(),
        chunk_interval: strings[5].as_ptr(),
        retention_period: ptr::null(),
        compression: ptr::null(),
        compression_level: 0,
        hot_threshold: ptr::null(),
        memtable_size_bytes: 0,
        ts_column_index: 0,
        column_count: 5,
        column_names: names.as_ptr(),
        column_types: types.as_ptr(),
        encryption_key_id: 0,
        unique: false,
        unique_extra_count: 0,
        unique_extra_cols: ptr::null(),
    };
    let p = CString::new(path).unwrap();
    check(unsafe { moosedb_table_create(p.as_ptr(), &cfg) }).unwrap();
    let mut t = ptr::null_mut();
    check(unsafe { moosedb_table_open(p.as_ptr(), &cfg, &mut t) }).unwrap();
    t
}

fn text(kind: TFColumnType, s: &str) -> TFValue {
    TFValue {
        kind: kind as u8,
        is_null: false,
        data: TFValueData { str_val: TFStr { ptr: s.as_ptr().cast(), len: s.len() as u32 } },
    }
}

fn write(t: *mut MooseDBTable, ts: i64, host: &str, n: Option<i64>, name: Option<&str>) {
    let null = |kind: TFColumnType| TFValue { kind: kind as u8, is_null: true, data: TFValueData { int_val: 0 } };
    let mut vals = [
        TFValue { kind: TFColumnType::TF_COL_TIMESTAMP as u8, is_null: false, data: TFValueData { ts_us: ts } },
        text(TFColumnType::TF_COL_TAG, host),
        n.map_or(null(TFColumnType::TF_COL_INT64), |n| TFValue {
            kind: TFColumnType::TF_COL_INT64 as u8,
            is_null: false,
            data: TFValueData { int_val: n },
        }),
        name.map_or(null(TFColumnType::TF_COL_VARCHAR), |s| text(TFColumnType::TF_COL_VARCHAR, s)),
        TFValue { kind: TFColumnType::TF_COL_FLOAT64 as u8, is_null: false, data: TFValueData { float_val: 1.0 } },
    ];
    let row = TFRow { col_count: 5, values: vals.as_mut_ptr() };
    check(unsafe { moosedb_write_row(t, &row) }).unwrap();
}

fn spec(keys: &[TFAggKey], funcs: &[TFAggFunc], filters: &[TFAggFilter], max_memory: u64, threads: u32) -> TFAggSpec {
    TFAggSpec {
        keys: keys.as_ptr(),
        key_count: keys.len() as u32,
        funcs: funcs.as_ptr(),
        func_count: funcs.len() as u32,
        filters: filters.as_ptr(),
        filter_count: filters.len() as u32,
        max_memory,
        threads,
        top_mode: TF_AGG_TOP_NONE,
        top_func: 0,
        top_limit: 0,
        preds: ptr::null(),
        pred_count: 0,
        float_filters: ptr::null(),
        float_filter_count: 0,
    }
}

/// A dictionary-mask filter (the tail of the struct zero).
fn mask_filter(col: u32, mask: *const u8, mask_len: u64) -> TFAggFilter {
    TFAggFilter {
        col,
        mask,
        mask_len,
        mode: TF_AGG_FILTER_MASK,
        int_values: ptr::null(),
        int_count: 0,
        value_starts: ptr::null(),
        value_lens: ptr::null(),
        value_count: 0,
        value_data: ptr::null(),
        value_data_len: 0,
        pad_space: 0,
    }
}

const SERIES: TFAggKey = TFAggKey { kind: TF_AGG_KEY_SERIES, col: 0, width_us: 0 };
const fn column(col: u32) -> TFAggKey {
    TFAggKey { kind: TF_AGG_KEY_COLUMN, col, width_us: 0 }
}
const fn func(op: u8, col: u32) -> TFAggFunc {
    TFAggFunc { op, col }
}

unsafe extern "C" fn always(_: *mut c_void) -> i32 {
    1
}

unsafe extern "C" fn count_calls(ctx: *mut c_void) -> i32 {
    unsafe { *ctx.cast::<u32>() += 1 };
    0
}

#[test]
fn aggregation_lifecycle_results_dictionaries_and_errors_through_c_abi() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path().join("t").to_str().unwrap());
    // 3 hosts x 40 rows over 2 days; names a/b/NULL, n NULL every 5th row
    for i in 0..120i64 {
        let host = ["h0", "h1", "h2"][(i % 3) as usize];
        let name = match i % 4 {
            0 => Some("a"),
            1 => Some("b"),
            _ => None,
        };
        write(t, i * 1_440_000_000, host, (i % 5 != 0).then_some(i), name);
        if i == 60 {
            check(unsafe { moosedb_flush(t) }).unwrap();
        }
    }
    let mut agg = ptr::null_mut();
    check(unsafe { moosedb_agg_open(t, i64::MIN, i64::MAX, ptr::null(), -1, &mut agg) }).unwrap();
    let mut batch = TFAggBatch {
        row_count: 0,
        total_groups: 0,
        key_count: 0,
        func_count: 0,
        keys: ptr::null(),
        funcs: ptr::null(),
    };
    // before any run the result is empty
    check(unsafe { moosedb_agg_result(agg, 0, 10, &mut batch) }).unwrap();
    assert_eq!((batch.row_count, batch.total_groups), (0, 0));

    // per series: COUNT(*), COUNT(n), SUM(n), MIN(ts), MAX(n)
    let funcs = [
        func(TF_AGG_COUNT_STAR, 0),
        func(TF_AGG_COUNT, 2),
        func(TF_AGG_SUM, 2),
        func(TF_AGG_MIN, 0),
        func(TF_AGG_MAX, 2),
    ];
    let s = spec(&[SERIES], &funcs, &[], 1 << 28, 2);
    check(unsafe { moosedb_agg_run(agg, &s, None, ptr::null_mut()) }).unwrap();
    // read it in two pieces
    let (mut stars, mut sums, mut ids) = (0u64, 0i128, Vec::new());
    for start in [0u64, 2] {
        check(unsafe { moosedb_agg_result(agg, start, 2, &mut batch) }).unwrap();
        assert_eq!(batch.total_groups, 3);
        assert_eq!((batch.key_count, batch.func_count), (1, 5));
        let (k, f) = unsafe { (&*batch.keys, std::slice::from_raw_parts(batch.funcs, 5)) };
        assert_eq!(k.kind, TF_AGG_KEYCOL_SERIES);
        let n = batch.row_count as usize;
        ids.extend_from_slice(unsafe { std::slice::from_raw_parts(k.values.cast::<u64>(), n) });
        stars += unsafe { std::slice::from_raw_parts(f[0].count, n) }.iter().sum::<u64>();
        let (lo, hi) =
            unsafe { (std::slice::from_raw_parts(f[2].sum_lo, n), std::slice::from_raw_parts(f[2].sum_hi, n)) };
        sums += (0..n).map(|g| (i128::from(hi[g]) << 64) | i128::from(lo[g])).sum::<i128>();
        assert!(f[0].val.is_null() && f[3].sum_lo.is_null() && !f[3].val.is_null());
    }
    ids.sort_unstable();
    ids.dedup();
    assert_eq!((stars, ids.len()), (120, 3));
    assert_eq!(sums, (0..120i64).filter(|i| i % 5 != 0).map(i128::from).sum::<i128>());
    // past the end: an empty batch, not an error
    check(unsafe { moosedb_agg_result(agg, 99, 10, &mut batch) }).unwrap();
    assert_eq!((batch.row_count, batch.total_groups), (0, 3));

    // dictionary of `name`, then a filtered COUNT(*)
    let dict_keys = [column(3)];
    let dict = spec(&dict_keys, &[], &[], 1 << 28, 1);
    check(unsafe { moosedb_agg_run(agg, &dict, None, ptr::null_mut()) }).unwrap();
    check(unsafe { moosedb_agg_result(agg, 0, 100, &mut batch) }).unwrap();
    assert_eq!(batch.row_count, 3, "a, b, NULL");
    let k = unsafe { &*batch.keys };
    assert_eq!(k.kind, TF_AGG_KEYCOL_BYTES);
    let n = 3;
    let (nulls, starts, lens) = unsafe {
        (
            std::slice::from_raw_parts(k.nulls, n),
            std::slice::from_raw_parts(k.starts, n),
            std::slice::from_raw_parts(k.lens, n),
        )
    };
    let data = unsafe { std::slice::from_raw_parts(k.data, k.data_len as usize) };
    let mask: Vec<u8> = (0..n)
        .map(|i| {
            u8::from(nulls[i] == 0 && &data[starts[i] as usize..(starts[i] + u64::from(lens[i])) as usize] == b"b")
        })
        .collect();
    let filtered = [mask_filter(3, mask.as_ptr(), 3)];
    let s = spec(&[], &[func(TF_AGG_COUNT_STAR, 0)], &filtered, 1 << 28, 3);
    check(unsafe { moosedb_agg_run(agg, &s, None, ptr::null_mut()) }).unwrap();
    check(unsafe { moosedb_agg_result(agg, 0, 10, &mut batch) }).unwrap();
    assert_eq!((batch.row_count, batch.key_count), (1, 0));
    assert_eq!(unsafe { *(*batch.funcs).count }, 30, "rows named b");

    // exact byte filter: no dictionary needed, same answer as the mask
    let (vdata, vstarts, vlens) = (b"bzb", [0u64, 1, 0], [1u32, 1, 1]);
    let exact = |vs: &[u64], ls: &[u32], n: u64, extra: u64| TFAggFilter {
        col: 3,
        mask: ptr::null(),
        mask_len: 0,
        mode: TF_AGG_FILTER_VALUES,
        int_values: ptr::null(),
        int_count: 0,
        value_starts: vs.as_ptr(),
        value_lens: ls.as_ptr(),
        value_count: n,
        value_data: vdata.as_ptr(),
        value_data_len: 3 + extra,
        pad_space: 0,
    };
    let run_count = |f: TFAggFilter| {
        let fs = [f];
        let fns = [func(TF_AGG_COUNT_STAR, 0)];
        let s = spec(&[], &fns, &fs, 1 << 28, 3);
        check(unsafe { moosedb_agg_run(agg, &s, None, ptr::null_mut()) }).map(|()| {
            let mut b = TFAggBatch {
                row_count: 0,
                total_groups: 0,
                key_count: 0,
                func_count: 0,
                keys: ptr::null(),
                funcs: ptr::null(),
            };
            check(unsafe { moosedb_agg_result(agg, 0, 10, &mut b) }).unwrap();
            unsafe { *(*b.funcs).count }
        })
    };
    assert_eq!(run_count(exact(&vstarts, &vlens, 3, 0)).unwrap(), 30, "b, z, b (duplicate) -> rows named b");
    assert_eq!(run_count(exact(&vstarts, &vlens, 0, 0)).unwrap(), 0, "empty set accepts nothing");
    let oob = run_count(exact(&[2], &[5], 1, 0));
    assert_eq!(status(oob.map(|_| ())), TFStatus::TF_ERR_INVALID_ARG, "value out of range");
    let mut bad_mode = exact(&vstarts, &vlens, 3, 0);
    bad_mode.mode = 7;
    assert_eq!(status(run_count(bad_mode).map(|_| ())), TFStatus::TF_ERR_INVALID_ARG);
    // integer set on n (col 2): rows with n = 1 or n = 7
    let ints = [1i64, 7, 1];
    let mut int_filter = exact(&vstarts, &vlens, 0, 0);
    int_filter.col = 2;
    int_filter.int_values = ints.as_ptr();
    int_filter.int_count = 3;
    assert_eq!(run_count(int_filter).unwrap(), 2);

    // float filter: f (col 4) is 1.0 in every row
    let total = {
        let fns = [func(TF_AGG_COUNT_STAR, 0)];
        let s = spec(&[], &fns, &[], 1 << 28, 3);
        check(unsafe { moosedb_agg_run(agg, &s, None, ptr::null_mut()) }).unwrap();
        check(unsafe { moosedb_agg_result(agg, 0, 10, &mut batch) }).unwrap();
        unsafe { *(*batch.funcs).count }
    };
    assert!(total > 0);
    let mut run_float = |col: u32, lo: f64, hi: f64, flags: u8, points: &[f64]| {
        let fns = [func(TF_AGG_COUNT_STAR, 0)];
        let ffs = [TFAggFloatFilter { col, lo, hi, flags, points: points.as_ptr(), point_count: points.len() as u64 }];
        let mut s = spec(&[], &fns, &[], 1 << 28, 3);
        s.float_filters = ffs.as_ptr();
        s.float_filter_count = 1;
        check(unsafe { moosedb_agg_run(agg, &s, None, ptr::null_mut()) }).map(|()| {
            check(unsafe { moosedb_agg_result(agg, 0, 10, &mut batch) }).unwrap();
            if batch.row_count == 0 { 0 } else { unsafe { *(*batch.funcs).count } }
        })
    };
    let (ninf, inf) = (f64::NEG_INFINITY, f64::INFINITY);
    assert_eq!(run_float(4, 1.0, inf, 0, &[]).unwrap(), total, "f >= 1.0");
    assert_eq!(run_float(4, 1.0, inf, TF_FLOAT_LO_OPEN, &[]).unwrap(), 0, "f > 1.0");
    assert_eq!(run_float(4, ninf, 1.0, TF_FLOAT_HI_OPEN, &[]).unwrap(), 0, "f < 1.0");
    assert_eq!(run_float(4, ninf, inf, TF_FLOAT_POINTS, &[2.0, 1.0]).unwrap(), total, "f IN (2, 1)");
    assert_eq!(run_float(4, ninf, inf, TF_FLOAT_POINTS, &[]).unwrap(), 0, "empty IN keeps nothing");
    for (col, lo, pts) in [(2, 0.0, vec![]), (4, f64::NAN, vec![]), (4, 0.0, vec![f64::INFINITY])] {
        let flags = if pts.is_empty() { 0 } else { TF_FLOAT_POINTS };
        let st = status(run_float(col, lo, inf, flags, &pts).map(|_| ()));
        assert_eq!(st, TFStatus::TF_ERR_INVALID_ARG, "col {col} lo {lo} pts {pts:?}");
    }

    // top-N through the ABI: series by COUNT(*) is 40 each, so ties come by id
    let top_funcs = [func(TF_AGG_COUNT_STAR, 0), func(TF_AGG_SUM, 2)];
    let mut top = spec(&[SERIES], &top_funcs, &[], 1 << 28, 2);
    top.top_mode = TF_AGG_TOP_DESC;
    top.top_func = 1;
    top.top_limit = 2;
    check(unsafe { moosedb_agg_run(agg, &top, None, ptr::null_mut()) }).unwrap();
    check(unsafe { moosedb_agg_result(agg, 0, 10, &mut batch) }).unwrap();
    assert_eq!((batch.row_count, batch.total_groups), (2, 2));
    let (k, f) = unsafe { (&*batch.keys, std::slice::from_raw_parts(batch.funcs, 2)) };
    let (lo, hi) = unsafe { (std::slice::from_raw_parts(f[1].sum_lo, 2), std::slice::from_raw_parts(f[1].sum_hi, 2)) };
    let sums: Vec<i128> = (0..2).map(|g| (i128::from(hi[g]) << 64) | i128::from(lo[g])).collect();
    assert!(sums[0] >= sums[1], "best first: {sums:?}");
    let _ = k;
    top.top_limit = 0;
    check(unsafe { moosedb_agg_run(agg, &top, None, ptr::null_mut()) }).unwrap();
    check(unsafe { moosedb_agg_result(agg, 0, 10, &mut batch) }).unwrap();
    assert_eq!(batch.total_groups, 0);
    top.keys = ptr::null();
    top.key_count = 0;
    assert_eq!(status(check(unsafe { moosedb_agg_run(agg, &top, None, ptr::null_mut()) })), TFStatus::TF_ERR_INVALID_ARG);
    top.top_mode = 9;
    assert_eq!(status(check(unsafe { moosedb_agg_run(agg, &top, None, ptr::null_mut()) })), TFStatus::TF_ERR_INVALID_ARG);

    // errors: nothing panics, nothing leaks, the handle stays usable
    let bad = |keys: &[TFAggKey], funcs: &[TFAggFunc], filters: &[TFAggFilter], mem: u64| {
        let s = spec(keys, funcs, filters, mem, 1);
        status(check(unsafe { moosedb_agg_run(agg, &s, None, ptr::null_mut()) }))
    };
    let star = [func(TF_AGG_COUNT_STAR, 0)];
    assert_eq!(bad(&[column(1)], &star, &[], 1 << 20), TFStatus::TF_ERR_INVALID_ARG, "TAG column key");
    assert_eq!(bad(&[column(77)], &star, &[], 1 << 20), TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(bad(&[TFAggKey { kind: 9, col: 0, width_us: 0 }], &star, &[], 1 << 20), TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(bad(&[], &[func(42, 0)], &[], 1 << 20), TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(bad(&[], &[func(TF_AGG_SUM, 4)], &[], 1 << 20), TFStatus::TF_ERR_INVALID_ARG, "SUM of a double");
    let short = [mask_filter(3, mask.as_ptr(), 2)];
    assert_eq!(bad(&[], &star, &short, 1 << 20), TFStatus::TF_ERR_INVALID_ARG, "mask of the wrong length");
    let no_dict = [mask_filter(2, mask.as_ptr(), 3)];
    assert_eq!(bad(&[], &star, &no_dict, 1 << 20), TFStatus::TF_ERR_INVALID_ARG, "no dictionary for n");
    let null_mask = [mask_filter(3, ptr::null(), 3)];
    assert_eq!(bad(&[], &star, &null_mask, 1 << 20), TFStatus::TF_ERR_INVALID_ARG, "NULL mask");
    assert_eq!(bad(&[SERIES], &star, &[], 100), TFStatus::TF_ERR_FULL, "budget");
    let msg = check(unsafe { moosedb_agg_run(agg, &spec(&[SERIES], &star, &[], 100, 1), None, ptr::null_mut()) })
        .unwrap_err()
        .1;
    assert!(msg.contains("moosedb_agg_max_memory"), "{msg}");
    check(unsafe { moosedb_agg_result(agg, 0, 10, &mut batch) }).unwrap();
    assert_eq!(batch.total_groups, 0, "a failed run leaves no result");

    // cancellation and the callback contract
    let s = spec(&[SERIES], &star, &[], 1 << 28, 1);
    assert_eq!(
        status(check(unsafe { moosedb_agg_run(agg, &s, Some(always), ptr::null_mut()) })),
        TFStatus::TF_ERR_CANCELLED
    );
    let mut calls = 0u32;
    check(unsafe { moosedb_agg_run(agg, &s, Some(count_calls), ptr::addr_of_mut!(calls).cast()) }).unwrap();
    assert!(calls >= 1);

    // NULL arguments
    assert_eq!(unsafe { moosedb_agg_run(ptr::null_mut(), &s, None, ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(unsafe { moosedb_agg_run(agg, ptr::null(), None, ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(unsafe { moosedb_agg_result(agg, 0, 1, ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(unsafe { moosedb_agg_result(ptr::null_mut(), 0, 1, &mut batch) }, TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(
        unsafe { moosedb_agg_open(ptr::null_mut(), 0, 1, ptr::null(), -1, &mut agg.clone()) },
        TFStatus::TF_ERR_INVALID_ARG
    );
    assert_eq!(unsafe { moosedb_agg_open(t, 0, 1, ptr::null(), -1, ptr::null_mut()) }, TFStatus::TF_ERR_INVALID_ARG);
    assert_eq!(
        unsafe { moosedb_agg_open(t, 0, 1, ptr::null(), 3, &mut ptr::null_mut()) },
        TFStatus::TF_ERR_INVALID_ARG
    );
    assert_eq!(unsafe { moosedb_agg_close(ptr::null_mut()) }, TFStatus::TF_OK);

    // a snapshot outlives writes; an empty series list means no rows
    write(t, 1, "h0", Some(1), Some("a"));
    check(unsafe { moosedb_agg_run(agg, &spec(&[], &star, &[], 1 << 28, 1), None, ptr::null_mut()) }).unwrap();
    check(unsafe { moosedb_agg_result(agg, 0, 1, &mut batch) }).unwrap();
    assert_eq!(unsafe { *(*batch.funcs).count }, 120);
    check(unsafe { moosedb_agg_close(agg) }).unwrap();
    let mut none = ptr::null_mut();
    let ids: [u64; 0] = [];
    check(unsafe { moosedb_agg_open(t, i64::MIN, i64::MAX, ids.as_ptr(), 0, &mut none) }).unwrap();
    check(unsafe { moosedb_agg_run(none, &spec(&[], &star, &[], 1 << 28, 1), None, ptr::null_mut()) }).unwrap();
    check(unsafe { moosedb_agg_result(none, 0, 1, &mut batch) }).unwrap();
    assert_eq!(unsafe { *(*batch.funcs).count }, 0);
    check(unsafe { moosedb_agg_close(none) }).unwrap();
    check(unsafe { moosedb_table_close(t) }).unwrap();
}
