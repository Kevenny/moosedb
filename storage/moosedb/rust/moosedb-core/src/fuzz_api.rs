//! Entry points for the cargo-fuzz targets in `rust/fuzz` (feature `fuzz`).
//!
//! Every function takes arbitrary bytes and must return without panicking, hanging or
//! allocating beyond a bound set by the input: the decoders read untrusted disk data.

use std::path::PathBuf;

use crate::block;
use crate::bytes::ByteReader;
use crate::cache::BlockPayload;
use crate::chunk::{decode_series_entry, ChunkHeader, HEADER_LEN};
use crate::chunk_reader;
use crate::codec::{decode_row, decode_value};
use crate::compression::{self, delta, gorilla, rle, simple8b};
use crate::index::bloom::Bloom;
use crate::manifest::Manifest;
use crate::schema::{Column, ColumnType, Schema};
use crate::tomb::{decode_range, decode_rows, decode_sel};
use crate::wal::{self, ReplayLimits, TornTail};

/// Schema the seed generator (`fuzz/src/bin/gen_seeds.rs`) writes chunks with.
pub fn seed_schema() -> Schema {
    let c = |n: &str, ty| Column { name: n.into(), ty };
    Schema::new(
        vec![
            c("ts", ColumnType::Timestamp),
            c("host", ColumnType::Tag),
            c("metric", ColumnType::Tag),
            c("value", ColumnType::Float64),
            c("value_int", ColumnType::Int64),
            c("note", ColumnType::Varchar),
        ],
        0,
    )
    .expect("seed schema")
}

/// A scratch directory private to this process (targets run single-threaded).
fn scratch(kind: &str) -> PathBuf {
    let base = if std::path::Path::new("/dev/shm").is_dir() { PathBuf::from("/dev/shm") } else { std::env::temp_dir() };
    let d = base.join(format!("moosedb-fuzz-{kind}-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&d);
    d
}

const TYPES: [ColumnType; 8] = [
    ColumnType::Timestamp,
    ColumnType::Int64,
    ColumnType::Float64,
    ColumnType::Float32,
    ColumnType::Bool,
    ColumnType::Varchar,
    ColumnType::Tag,
    ColumnType::Decimal,
];

fn n24(b: &[u8]) -> usize {
    (u32::from_le_bytes([b[0], b[1], b[2], 0]) as usize) & 0xF_FFFF
}

/// `[type:1][n:3][encoding:1][payload..]`: decodes a column block body as every decoder sees it
/// (n is capped at 1M rows so an execution stays fast; the real cap is `MAX_DECODE_BYTES`).
pub fn column_block(data: &[u8]) {
    if data.len() < 5 {
        return;
    }
    let ty = TYPES[usize::from(data[0]) % TYPES.len()];
    let n = n24(&data[1..4]);
    let block = BlockPayload { encoding: data[4], data: data[5..].to_vec() };
    let _ = block::decode(ty, n, &block);
    let _ = block::decode_compact(ty, n, &block);
    let _ = block::decode_compact_prefix(ty, n, n / 3, &block);
}

/// `[codec:1][raw_len:3][payload..]`: the LZ4/ZSTD wrapper plus the block envelope (`unpack`).
pub fn block_envelope(data: &[u8]) {
    if data.len() < 4 {
        return;
    }
    let raw = u32::from_le_bytes([data[1], data[2], data[3], 0]) as usize;
    let _ = compression::decompress(data[0], &data[4..], raw);
    let _ = block::unpack(data);
}

/// `[kind:1][count:3][width:1][payload..]`: one integer/float/bool codec in isolation.
pub fn codec_stream(data: &[u8]) {
    if data.len() < 5 {
        return;
    }
    let count = n24(&data[1..4]);
    let body = &data[5..];
    let mut r = ByteReader::new(body);
    match data[0] % 7 {
        0 => drop(simple8b::decode(&mut r, count)),
        1 => drop(simple8b::decode_prefix(&mut r, count, count / 2)),
        2 => drop(delta::decode_delta_prefix(&mut r, count, count)),
        3 => drop(delta::decode_delta_of_delta_prefix(&mut r, count, count / 2)),
        4 => drop(gorilla::decode_prefix(body, count, count, if data[4] & 1 == 0 { 64 } else { 32 })),
        5 => drop(rle::decode(&mut r, count)),
        _ => drop(r.varint()),
    }
}

/// Whole `.tfl` file. Header CRC is repaired so mutations reach the index parser.
pub fn chunk_file(data: &[u8]) {
    let mut file = data.to_vec();
    if file.len() >= HEADER_LEN {
        let crc = crc32fast::hash(&file[..60]).to_le_bytes();
        file[60..64].copy_from_slice(&crc);
    }
    let id = if file.len() >= 48 { u64::from_le_bytes(file[40..48].try_into().unwrap()) } else { 0 };
    let path = scratch("chunk").join("chunk_x.tfl");
    if std::fs::write(&path, &file).is_err() {
        return;
    }
    let _ = ChunkHeader::decode(&file);
    let _ = chunk_reader::verify_file(&path);
    let Ok(meta) = chunk_reader::read_meta(&path, id) else { return };
    let schema = seed_schema();
    for (i, e) in meta.series.iter().enumerate().take(8) {
        let _ = chunk_reader::decode_series(&schema, &meta, i);
        let _ = chunk_reader::fetch_row(&schema, &meta, i, (e.row_count / 2) as usize);
        for b in e.blocks.iter().take(8) {
            let _ = chunk_reader::read_block(&meta, *b);
        }
    }
}

/// Series-index entries, tag values/rows, bloom filter, tombstone records: the small parsers.
pub fn small_records(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut r = ByteReader::new(&data[1..]);
    match data[0] % 8 {
        0 => drop(decode_series_entry(&mut r)),
        1 => drop(decode_value(&mut r)),
        2 => drop(decode_row(&mut r)),
        3 => drop(Bloom::decode(&mut r).map(|b| b.may_contain(42))),
        4 => drop(decode_sel(&mut r)),
        5 => drop(decode_range(&mut r)),
        6 => drop(decode_rows(&mut r)),
        _ => drop(crate::crypto::CipherParams::decode(&data[1..])),
    }
}

/// MANIFEST bytes, as-is and with the trailing CRC repaired (so the body parser is reached).
pub fn manifest(data: &[u8]) {
    let dir = scratch("manifest");
    let _ = std::fs::write(dir.join("MANIFEST"), data);
    let _ = Manifest::load(&dir);
    if data.len() >= 4 {
        let mut fixed = data.to_vec();
        let n = fixed.len() - 4;
        let crc = crc32fast::hash(&fixed[..n]).to_le_bytes();
        fixed[n..].copy_from_slice(&crc);
        let _ = std::fs::write(dir.join("MANIFEST"), &fixed);
        let _ = Manifest::load(&dir);
    }
}

/// A WAL segment file, as-is and with every entry CRC repaired; replayed in each mode.
pub fn wal_segment(data: &[u8]) {
    let dir = scratch("wal");
    let mut fixed = data.to_vec();
    let mut pos = 8usize;
    while pos + 8 <= fixed.len() {
        let len = u32::from_le_bytes(fixed[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let Some(end) = (pos + 8).checked_add(len).filter(|&e| e <= fixed.len()) else { break };
        let crc = crc32fast::hash(&fixed[pos + 4..end]).to_le_bytes();
        fixed[pos..pos + 4].copy_from_slice(&crc);
        pos = end;
    }
    for bytes in [data, &fixed[..]] {
        for recover in [false, true] {
            if std::fs::write(wal::segment_path(&dir, 1), bytes).is_err() {
                return;
            }
            let _ = wal::replay_events(&dir, &[1], 0, recover, None, &ReplayLimits::standard(), &|_| false, |_, ev| {
                if let wal::ReplayEvent::Delete(p) = ev {
                    let _ = wal::decode_delete(p);
                }
                Ok(())
            });
        }
        for torn in [TornTail::Error, TornTail::Ignore] {
            let _ = std::fs::write(wal::segment_path(&dir, 1), bytes);
            let _ = wal::replay(&dir, 1, torn, None, |_, p| {
                let _ = wal::decode_delete(p);
                let _ = decode_row(&mut ByteReader::new(p));
                Ok(())
            });
        }
    }
}

/// Raw WAL entry payloads (after the framing): row/commit/delete bodies.
pub fn wal_payload(data: &[u8]) {
    let _ = wal::decode_delete(data);
    let _ = decode_row(&mut ByteReader::new(data));
}

/// Valid inputs for the stream-level targets: `(target, file name, bytes)`.
/// Chunk, WAL and MANIFEST seeds come from a real table (see `gen_seeds`).
pub fn seeds() -> Vec<(&'static str, String, Vec<u8>)> {
    use crate::bytes::{put_u16, put_u32, put_u64};
    use crate::compression::Codec;
    use crate::schema::Value;
    use crate::tomb::{encode_range, encode_rows, encode_sel, RangeTomb, RowsTomb, Sel};

    let mut out: Vec<(&'static str, String, Vec<u8>)> = Vec::new();
    let nulls = |i: usize| i % 4 == 0;
    let samples: Vec<(ColumnType, Vec<Value>)> = vec![
        (ColumnType::Timestamp, (0..300).map(|i| Value::Timestamp(1_700_000_000_000_000 + i * 10_000_000)).collect()),
        (ColumnType::Int64, (0..300).map(|i| if nulls(i as usize) { Value::Null } else { Value::Int(i * i - 7) }).collect()),
        (ColumnType::Float64, (0..300).map(|i| Value::Float64(20.0 + (i as f64 * 0.37).sin())).collect()),
        (ColumnType::Float32, (0..300).map(|i| Value::Float32(i as f32 * 0.5)).collect()),
        (ColumnType::Bool, (0..300).map(|i| Value::Bool(i % 7 < 3)).collect()),
        (ColumnType::Varchar, (0..300).map(|i| if nulls(i as usize) { Value::Null } else { Value::Bytes(format!("n{i}").into_bytes()) }).collect()),
        (ColumnType::Decimal, (0..300).map(|i| Value::Bytes(format!("{i}.25").into_bytes())).collect()),
    ];
    for (ty, vals) in &samples {
        let refs: Vec<&Value> = vals.iter().collect();
        for codec in [Codec::None, Codec::Lz4, Codec::Zstd(3)] {
            let Ok(enc) = block::encode(*ty, &refs, codec) else { continue };
            let n = vals.len() as u32;
            if let Ok(p) = block::unpack(&enc.bytes) {
                let mut s = vec![*ty as u8];
                s.extend_from_slice(&n.to_le_bytes()[..3]);
                s.push(p.encoding);
                s.extend_from_slice(&p.data);
                out.push(("column_block", format!("{ty:?}_{}", codec.id()), s));
            }
            out.push(("block_envelope", format!("env_{ty:?}_{}", codec.id()), enc.bytes.clone()));
            let mut d = vec![codec.id()];
            d.extend_from_slice(&enc.plain_len.to_le_bytes()[..3]);
            d.extend_from_slice(&enc.bytes[block::HEADER_LEN..]);
            out.push(("block_envelope", format!("dec_{ty:?}_{}", codec.id()), d));
        }
    }
    let ints: Vec<i64> = (0..300).map(|i| i * 3 - 100).collect();
    let bits: Vec<u64> = (0..300u64).map(|i| (20.0f64 + i as f64 * 0.1).to_bits()).collect();
    let mut streams: Vec<(u8, u8, Vec<u8>)> = Vec::new();
    let mut b = Vec::new();
    simple8b::encode(&bits.iter().map(|x| x & 0xFFFF).collect::<Vec<_>>(), &mut b);
    streams.push((0, 0, b));
    let mut b = Vec::new();
    delta::encode_delta(&ints, &mut b);
    streams.push((2, 0, b));
    let mut b = Vec::new();
    delta::encode_delta_of_delta(&ints, &mut b);
    streams.push((3, 0, b));
    let mut b = Vec::new();
    gorilla::encode(&bits, 64, &mut b);
    streams.push((4, 0, b));
    let mut b = Vec::new();
    rle::encode((0..300).map(|i| i % 7 < 3), &mut b);
    streams.push((5, 0, b));
    for (kind, w, body) in streams {
        let mut s = vec![kind];
        s.extend_from_slice(&300u32.to_le_bytes()[..3]);
        s.push(w);
        s.extend_from_slice(&body);
        out.push(("codec_stream", format!("kind{kind}"), s));
    }

    let tags = vec![Value::Bytes(b"h1".to_vec()), Value::Bytes(b"cpu".to_vec())];
    let entry = crate::chunk::SeriesEntry {
        series_id: 7,
        row_count: 10,
        ts_min: 0,
        ts_max: 9,
        tags: tags.clone(),
        blocks: vec![crate::chunk::BlockRef { offset: 100, len: 40 }, crate::chunk::BlockRef { offset: 140, len: 40 }],
    };
    let mut rec = |kind: u8, name: &str, body: &[u8]| {
        let mut s = vec![kind];
        s.extend_from_slice(body);
        out.push(("small_records", name.to_string(), s));
    };
    let mut b = Vec::new();
    crate::chunk::encode_series_entry(&mut b, &entry);
    rec(0, "series_entry", &b);
    let mut b = Vec::new();
    crate::codec::encode_value(&mut b, &Value::Bytes(b"abc".to_vec()));
    rec(1, "value", &b);
    let mut b = Vec::new();
    crate::codec::encode_row(&mut b, &[Value::Timestamp(5), Value::Null, Value::Float64(1.5), Value::Bool(true)]);
    rec(2, "row", &b);
    let mut bloom = Bloom::with_capacity(50, 0.01);
    bloom.insert(1);
    let mut b = Vec::new();
    bloom.encode(&mut b);
    rec(3, "bloom", &b);
    let sel = Sel::from_ids(vec![1, 4, 9]);
    let mut b = Vec::new();
    encode_sel(&mut b, &sel);
    rec(4, "sel", &b);
    let range = RangeTomb { seq: 3, lo: 1, hi: 99, sel, hits: vec![(2, 5)] };
    let mut b = Vec::new();
    encode_range(&mut b, &range);
    rec(5, "range", &b);
    let rows = RowsTomb { seq: 2, chunk: 4, entry: 1, runs: vec![(0, 2), (8, 1)] };
    let mut b = Vec::new();
    encode_rows(&mut b, &rows);
    rec(6, "rows", &b);

    let row = [Value::Timestamp(5), Value::Bytes(b"h1".to_vec()), Value::Float64(1.5)];
    let mut p = Vec::new();
    wal::row_entry_into(&mut p, 1, &row);
    out.push(("wal_payload", "row".into(), p));
    out.push(("wal_payload", "commit".into(), wal::commit_entry(1)));
    out.push(("wal_payload", "row_commit".into(), wal::row_commit_entry(&row)));
    out.push(("wal_payload", "del_range".into(), wal::range_delete_entry(3, 0, 10, Some(&[&tags[..]]))));
    out.push(("wal_payload", "del_rows".into(), wal::rows_delete_entry(4, &[&row[..]], std::slice::from_ref(&rows))));

    // MANIFEST v3 with tombstones (the writer is private to its module; same layout).
    let mut m = Vec::new();
    m.extend_from_slice(b"TFLM");
    put_u16(&mut m, 3);
    put_u16(&mut m, 0);
    put_u64(&mut m, 9); // wal_seq
    put_u64(&mut m, 20); // next_chunk_id
    put_u64(&mut m, 9); // replay_seq
    put_u64(&mut m, 6); // tseq
    put_u32(&mut m, 2);
    for (c, born) in [(3u64, 2u64), (7, 5)] {
        put_u64(&mut m, c);
        put_u64(&mut m, born);
    }
    put_u32(&mut m, 1);
    encode_range(&mut m, &RangeTomb { seq: 6, lo: 1, hi: 99, sel: Sel::from_ids(vec![4, 2]), hits: vec![(3, 10)] });
    put_u32(&mut m, 1);
    encode_rows(&mut m, &RowsTomb { seq: 4, chunk: 7, entry: 3, runs: vec![(0, 2), (8, 1)] });
    let crc = crc32fast::hash(&m);
    put_u32(&mut m, crc);
    out.push(("manifest", "v3".into(), m));
    out
}
