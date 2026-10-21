//! Reading chunk metadata and individual column blocks.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::block;
use crate::bytes::ByteReader;
use crate::cache::{self, BlockPayload, DecodedSeries};
use crate::chunk::{
    decode_series_entry, index_ctx, UNIT_BLOCK, series_offsets, BlockRef, ChunkFile, ChunkHeader, ChunkMeta, ENC_HEADER_LEN, FOOTER_LEN,
    FOOTER_MAGIC, HEADER_LEN,
};
use crate::compression::{MAX_BLOCK_RAW, MAX_SERIES_ROWS};
use crate::crypto::{CipherParams, TAG_LEN};
use crate::error::{corrupt, Error, Result};
use crate::index::bloom::Bloom;
use crate::schema::{ColumnType, Row, Schema};

struct Footer {
    index_offset: u64,
    bloom_offset: u64,
    file_crc: u32,
}

fn decode_footer(b: &[u8]) -> Result<Footer> {
    let mut r = ByteReader::new(b);
    let f = Footer { index_offset: r.u64()?, bloom_offset: r.u64()?, file_crc: r.u32()? };
    if r.take(4)? != FOOTER_MAGIC {
        return Err(corrupt("chunk bad footer magic"));
    }
    Ok(f)
}

/// Reads header, series index and Bloom filter (decrypting them if needed)
/// without touching data blocks. Validates header CRC and the structure of
/// the index; full-file CRC is checked by [`verify_file`].
pub(crate) fn read_meta(path: &Path, id: u64) -> Result<ChunkMeta> {
    let mut f = File::open(path)?;
    let file_size = f.metadata()?.len();
    if file_size < (HEADER_LEN + FOOTER_LEN) as u64 {
        return Err(corrupt(format!("{} is too small to be a chunk", path.display())));
    }
    let mut hb = [0u8; HEADER_LEN];
    f.read_exact(&mut hb)?;
    let header = ChunkHeader::decode(&hb)?;
    if header.chunk_id != id {
        return Err(corrupt(format!("{} claims chunk id {}", path.display(), header.chunk_id)));
    }
    let cipher = if header.is_encrypted() {
        crate::crypto::ensure_available()?;
        let mut eb = [0u8; ENC_HEADER_LEN];
        f.read_exact(&mut eb)?;
        let c = CipherParams::decode(&eb)?;
        Some(if header.is_authenticated() { c.authenticated(hb.to_vec()) } else { c })
    } else {
        None
    };
    let data_start = header.data_start();

    let footer_start = file_size - FOOTER_LEN as u64;
    let mut fb = [0u8; FOOTER_LEN];
    f.seek(SeekFrom::Start(footer_start))?;
    f.read_exact(&mut fb)?;
    let footer = decode_footer(&fb)?;
    if !(data_start <= footer.index_offset
        && footer.index_offset <= footer.bloom_offset
        && footer.bloom_offset <= footer_start)
    {
        return Err(corrupt(format!("{} has invalid section offsets", path.display())));
    }

    let mut meta_bytes = vec![0u8; (footer_start - footer.index_offset) as usize];
    f.seek(SeekFrom::Start(footer.index_offset))?;
    f.read_exact(&mut meta_bytes)?;
    let encrypted = cipher.is_some();
    // Index/bloom bytes are only meaningful after decryption: under a wrong
    // key they are garbage, which looks like corruption but is not provable
    // corruption, so encrypted chunks report `Crypto` (fail closed).
    let bad_index = |what: &str| {
        let msg = format!("{}: {what} (corrupt file or wrong encryption key)", path.display());
        if encrypted {
            Error::Crypto(msg)
        } else {
            corrupt(msg)
        }
    };
    if let Some(c) = &cipher {
        if c.is_authenticated() {
            let ctx = index_ctx(footer.bloom_offset);
            let pos = footer.index_offset - data_start;
            let Some(n) = meta_bytes.len().checked_sub(TAG_LEN) else { return Err(bad_index("unreadable series index")) };
            let (body, tag) = meta_bytes.split_at_mut(n);
            if !c.open(body, tag, pos, &ctx) {
                return Err(bad_index("series index failed authentication"));
            }
            meta_bytes.truncate(n);
        } else {
            c.apply_ctr(&mut meta_bytes, footer.index_offset - data_start);
        }
        if !c.is_authenticated() {
            crate::log::warn(&format!(
                "{} is encrypted without authentication (legacy format); OPTIMIZE TABLE rewrites it authenticated",
                path.display()
            ));
        }
    }
    let split = (footer.bloom_offset - footer.index_offset) as usize;
    if split > meta_bytes.len() {
        return Err(bad_index("unreadable series index"));
    }
    let (index_bytes, bloom_bytes) = meta_bytes.split_at(split);

    let mut r = ByteReader::new(index_bytes);
    let raw_bytes = r.u64().map_err(|_| bad_index("unreadable series index"))?;
    let (bucket_lo, bucket_hi) = match (r.i64(), r.i64()) {
        (Ok(a), Ok(b)) => (a, b),
        _ => return Err(bad_index("unreadable series index")),
    };
    let bucket = (bucket_lo, bucket_hi);
    let n = r.u32().map_err(|_| bad_index("unreadable series index"))? as usize;
    if n != header.series_count as usize || n > r.remaining() {
        return Err(bad_index("series count mismatch"));
    }
    let series = (0..n)
        .map(|_| decode_series_entry(&mut r))
        .collect::<Result<Vec<_>>>()
        .map_err(|_| bad_index("unreadable series index"))?;
    let mut total_rows = 0u64;
    for e in &series {
        if e.row_count == 0 || u64::from(e.row_count) > MAX_SERIES_ROWS {
            return Err(bad_index("series row count out of range"));
        }
        total_rows += u64::from(e.row_count);
        for b in &e.blocks {
            let end = b.offset.checked_add(u64::from(b.len)).and_then(|e| e.checked_add(header.block_overhead() as u64));
            if b.offset < data_start
                || end.map_or(true, |end| end > footer.index_offset)
                || b.len as usize > block::HEADER_LEN + MAX_BLOCK_RAW
            {
                return Err(bad_index("block outside data section"));
            }
        }
    }
    if total_rows != header.row_count {
        return Err(bad_index("series row counts do not add up to the chunk row count"));
    }
    let bloom = Bloom::decode(&mut ByteReader::new(bloom_bytes)).map_err(|_| bad_index("unreadable bloom filter"))?;

    Ok(ChunkMeta {
        id,
        file: ChunkFile::new(path.to_path_buf()),
        header,
        bucket,
        raw_bytes,
        file_size,
        series_offsets: series_offsets(&series),
        series,
        bloom,
        cipher,
        by_id: Default::default(),
        born: 0,
    })
}

/// Full-file CRC32 verification (CHECK TABLE, recovery of the newest chunk).
/// Works on the stored bytes, so it needs no encryption key.
pub(crate) fn verify_file(path: &Path) -> Result<()> {
    let data = std::fs::read(path)?;
    if data.len() < HEADER_LEN + FOOTER_LEN {
        return Err(corrupt(format!("{} truncated", path.display())));
    }
    let footer_start = data.len() - FOOTER_LEN;
    let footer = decode_footer(&data[footer_start..])?;
    if crc32fast::hash(&data[..footer_start + 16]) != footer.file_crc {
        return Err(corrupt(format!("{} checksum mismatch", path.display())));
    }
    Ok(())
}

/// Reads, decrypts, verifies and decompresses one column block, through the
/// process-wide block cache.
pub(crate) fn read_block(meta: &ChunkMeta, b: BlockRef) -> Result<Arc<BlockPayload>> {
    let uid = meta.file.uid;
    if let Some(p) = cache::get_block(uid, b.offset) {
        return Ok(p);
    }
    let file = cache::open_file(uid, meta.path())?;
    if b.len as usize > block::HEADER_LEN + MAX_BLOCK_RAW {
        return Err(corrupt(format!("block of {} bytes is too large", b.len)));
    }
    let len = b.len as usize;
    let mut bytes = vec![0u8; len + meta.header.block_overhead()];
    cache::read_at(&file, &mut bytes, b.offset)?;
    if let Some(c) = &meta.cipher {
        let pos = b.offset - meta.header.data_start();
        if c.is_authenticated() {
            // Verified before anything is decoded; a failure is never data.
            let (body, tag) = bytes.split_at_mut(len);
            if !c.open(body, tag, pos, &[UNIT_BLOCK]) {
                return Err(corrupt(format!(
                    "{} at offset {}: block failed authentication (altered, moved or wrong key)",
                    meta.path().display(),
                    b.offset
                )));
            }
            bytes.truncate(len);
        } else {
            c.apply_ctr(&mut bytes, pos);
        }
    }
    let payload = Arc::new(
        block::unpack(&bytes).map_err(|e| corrupt(format!("{} at offset {}: {e}", meta.path().display(), b.offset)))?,
    );
    cache::put_block(uid, b.offset, payload.clone());
    Ok(payload)
}

/// Decodes all rows of series entry `idx`.
pub(crate) fn decode_series(schema: &Schema, meta: &ChunkMeta, idx: usize) -> Result<Vec<Row>> {
    let entry = meta.series.get(idx).ok_or_else(|| corrupt("series entry index out of range"))?;
    let n = entry.row_count as usize;
    if n == 0 || entry.row_count as u64 > MAX_SERIES_ROWS {
        return Err(corrupt(format!("{} has a series with {n} rows", meta.path().display())));
    }
    if entry.blocks.len() != schema.data_indices().len() || entry.tags.len() != schema.tag_indices().len() {
        return Err(corrupt(format!("{} does not match the table schema", meta.path().display())));
    }
    // Bound what we materialize (rows of Values) by the claimed row count,
    // before allocating anything.
    block::check_materialization(
        n,
        schema.columns().len() * std::mem::size_of::<crate::schema::Value>() + std::mem::size_of::<Row>(),
    )
    .map_err(|e| corrupt(format!("{}: {e}", meta.path().display())))?;
    let mut columns: Vec<Option<std::vec::IntoIter<crate::schema::Value>>> = vec![None; schema.columns().len()];
    for (&col, &b) in schema.data_indices().iter().zip(&entry.blocks) {
        let payload = read_block(meta, b)?;
        let values = block::decode(schema.columns()[col].ty, n, &payload)
            .map_err(|e| corrupt(format!("{} at offset {}: {e}", meta.path().display(), b.offset)))?;
        columns[col] = Some(values.into_iter());
    }
    let tag_of: Vec<Option<usize>> = (0..schema.columns().len()).map(|c| schema.tag_position(c)).collect();
    // `n` is bounded but file-derived: grow past 1M rows on demand instead of reserving up front.
    let mut rows = Vec::with_capacity(n.min(1 << 20));
    for _ in 0..n {
        let mut row = Vec::with_capacity(columns.len());
        for (col, it) in columns.iter_mut().enumerate() {
            let v = match (it, tag_of[col]) {
                (Some(it), _) => it.next().ok_or_else(|| corrupt("column block too short"))?,
                (None, Some(t)) => entry.tags[t].clone(),
                (None, None) => return Err(corrupt("column without data")),
            };
            row.push(v);
        }
        rows.push(row);
    }
    Ok(rows)
}

/// The part of one chunk series a scan needs: rows `[lo, hi)` of the series,
/// as typed columns (see [`load_window`]).
#[derive(Clone)]
pub(crate) struct Window {
    pub lo: usize,
    pub hi: usize,
    /// By schema column. Each present column is indexed by the row number
    /// *within the series* and holds at least rows `[0, hi)`.
    pub cols: Vec<Option<Arc<block::ColumnData>>>,
    ts_index: usize,
    /// A narrow read: columns decoded later are kept whole in the cache.
    narrow: bool,
}

impl Window {
    /// The timestamps of rows `[0, hi)` of the series, when decoded.
    pub(crate) fn ts_slice(&self) -> Option<&[i64]> {
        self.cols.get(self.ts_index).and_then(Option::as_ref).and_then(|c| c.ints())
    }

    /// Timestamp of row `i` of the series (`lo <= i < hi`), when the window
    /// decoded the timestamp column (always, if `need_ts` was set).
    pub(crate) fn ts_at(&self, i: usize) -> Result<i64> {
        self.cols
            .get(self.ts_index)
            .and_then(Option::as_ref)
            .and_then(|c| c.ints())
            .and_then(|v| v.get(i))
            .copied()
            .ok_or_else(|| corrupt("timestamp column missing from a decoded series"))
    }
}

/// Series up to this many rows keep their decoded columns in the cache when a
/// read touches a small part of them (see [`load_window`]).
const COLUMN_CACHE_MAX_ROWS: usize = 1 << 16;

/// Decodes the rows of series `idx` whose timestamp lies in `[ts_min, ts_max]`
/// and only the columns flagged in `want`.
///
/// * The timestamp column is decoded first (it is cheap and rows of a series
///   are sorted by it), and the range is found by binary search. When the
///   series index says every row is inside the range and the timestamp is
///   neither wanted nor `need_ts`, it is not decoded at all.
/// * Other columns decode only rows `[0, hi)`: Delta and Gorilla streams
///   cannot be entered in the middle, but nothing past the range is expanded.
/// * A **narrow** read (the range holds at most 1/8 of a series of up to
///   [`COLUMN_CACHE_MAX_ROWS`] rows: a point lookup, the last hour of a day)
///   decodes the whole column once and keeps it in the shared cache, so the
///   next read of the same series (joins, repeated lookups, the same graph
///   refreshed) does not decode again. A wide read decodes only what it needs
///   the first time it meets a series and keeps the whole column from the
///   second time on (`cache::touched_before`), so a scan that visits each
///   series once cannot flush the cache while repeated queries stop decoding.
///
/// Returns `None` when no row of the series is in range.
pub(crate) fn load_window(
    schema: &Schema,
    meta: &ChunkMeta,
    idx: usize,
    (ts_min, ts_max): (i64, i64),
    want: &[bool],
    need_ts: bool,
) -> Result<Option<Window>> {
    let entry = meta.series.get(idx).ok_or_else(|| corrupt("series entry index out of range"))?;
    let n = entry.row_count as usize;
    if n == 0 || u64::from(entry.row_count) > MAX_SERIES_ROWS {
        return Err(corrupt(format!("{} has a series with {n} rows", meta.path().display())));
    }
    if entry.blocks.len() != schema.data_indices().len() || entry.tags.len() != schema.tag_indices().len() {
        return Err(corrupt(format!("{} does not match the table schema", meta.path().display())));
    }
    block::check_materialization(n, 9 * schema.data_indices().len().max(1))
        .map_err(|e| corrupt(format!("{}: {e}", meta.path().display())))?;
    let ts_idx = schema.ts_index();
    let uid = meta.file.uid;
    let series_key = u32::try_from(idx).map_err(|_| corrupt("series entry index out of range"))?;
    let column_key = |col: usize| u32::try_from(col).unwrap_or(u32::MAX);
    let cacheable = n <= COLUMN_CACHE_MAX_ROWS;
    let mut cols: Vec<Option<Arc<block::ColumnData>>> = (0..schema.columns().len()).map(|_| None).collect();
    let decode_err = |b: BlockRef, e: Error| corrupt(format!("{} at offset {}: {e}", meta.path().display(), b.offset));

    let covers_all = entry.ts_min >= ts_min && entry.ts_max <= ts_max;
    let want_ts = need_ts || want.get(ts_idx).copied().unwrap_or(false);
    // The timestamp column when it had to be decoded here (not found in the cache).
    let mut fresh_ts: Option<Arc<block::ColumnData>> = None;
    let (lo, hi) = if covers_all && !want_ts {
        (0, n)
    } else {
        let cached = if cacheable { cache::get_column(uid, series_key, column_key(ts_idx)) } else { None };
        let data = match cached.filter(|c| c.len() == n) {
            Some(c) => c,
            None => {
                let pos = schema
                    .data_indices()
                    .iter()
                    .position(|&c| c == ts_idx)
                    .ok_or_else(|| corrupt("no timestamp block"))?;
                let b = entry.blocks[pos];
                let payload = read_block(meta, b)?;
                let data =
                    Arc::new(block::decode_compact(ColumnType::Timestamp, n, &payload).map_err(|e| decode_err(b, e))?);
                let ts = data.ints().filter(|t| t.len() == n && data.null_flags().is_none());
                let ts = ts.ok_or_else(|| corrupt(format!("{}: bad timestamp block", meta.path().display())))?;
                if !ts.windows(2).all(|w| w[0] <= w[1]) {
                    return Err(corrupt(format!("{}: series rows are not sorted by timestamp", meta.path().display())));
                }
                fresh_ts = Some(data.clone());
                data
            }
        };
        // Only a validated column (here or when it was cached) is ever searched.
        let ts = data
            .ints()
            .filter(|t| t.len() == n)
            .ok_or_else(|| corrupt(format!("{}: bad timestamp block", meta.path().display())))?;
        let lo = ts.partition_point(|&t| t < ts_min);
        let hi = ts.partition_point(|&t| t <= ts_max);
        if want_ts {
            cols[ts_idx] = Some(data);
        }
        (lo, hi)
    };
    if lo >= hi {
        return Ok(None);
    }
    let narrow = cacheable && (hi - lo).saturating_mul(8) <= n;
    if let Some(ts) = fresh_ts {
        // The whole timestamp column is in hand anyway: keep it when the read is
        // narrow or the series was read before.
        if narrow || (cacheable && cache::touched_before(uid, series_key, column_key(ts_idx))) {
            cache::put_column(uid, series_key, column_key(ts_idx), ts);
        }
    }
    let mut window = Window { lo, hi, cols, ts_index: ts_idx, narrow };
    for &col in schema.data_indices() {
        if col != ts_idx && want.get(col).copied().unwrap_or(false) {
            load_column(schema, meta, idx, &mut window, col)?;
        }
    }
    Ok(Some(window))
}

/// Decodes data column `col` of series `idx` into `win` (rows `[0, hi)` at
/// least), through the column cache. See [`load_window`] for what is kept.
fn load_column(schema: &Schema, meta: &ChunkMeta, idx: usize, win: &mut Window, col: usize) -> Result<()> {
    let entry = meta.series.get(idx).ok_or_else(|| corrupt("series entry index out of range"))?;
    let n = entry.row_count as usize;
    let pos = schema.data_indices().iter().position(|&c| c == col).ok_or_else(|| corrupt("not a data column"))?;
    let b = *entry.blocks.get(pos).ok_or_else(|| corrupt("series entry has too few blocks"))?;
    let uid = meta.file.uid;
    let series_key = u32::try_from(idx).map_err(|_| corrupt("series entry index out of range"))?;
    let column_key = u32::try_from(col).unwrap_or(u32::MAX);
    let cacheable = n <= COLUMN_CACHE_MAX_ROWS;
    if cacheable {
        if let Some(c) = cache::get_column(uid, series_key, column_key).filter(|c| c.len() == n) {
            win.cols[col] = Some(c);
            return Ok(());
        }
    }
    let decode_err = |e: Error| corrupt(format!("{} at offset {}: {e}", meta.path().display(), b.offset));
    let payload = read_block(meta, b)?;
    let ty = schema.columns()[col].ty;
    let keep = cacheable && (win.narrow || cache::touched_before(uid, series_key, column_key));
    let data = if keep {
        let data = block::decode_compact(ty, n, &payload).map_err(decode_err)?;
        if data.len() != n {
            return Err(corrupt("column block too short"));
        }
        let data = Arc::new(data);
        cache::put_column(uid, series_key, column_key, data.clone());
        data
    } else {
        let data = block::decode_compact_prefix(ty, n, win.hi, &payload).map_err(decode_err)?;
        if data.len() != win.hi {
            return Err(corrupt("column block too short"));
        }
        Arc::new(data)
    };
    win.cols[col] = Some(data);
    Ok(())
}

/// Decodes the data columns of `extra` that `win` does not hold yet. A sorted
/// scan of a UNIQUE table calls this the first time a run meets rows of the
/// same timestamp: only then does it need the key's extra columns, which a
/// scan without ties never decodes.
pub(crate) fn load_more_columns(
    schema: &Schema,
    meta: &ChunkMeta,
    idx: usize,
    win: &mut Window,
    extra: &[usize],
) -> Result<()> {
    for &col in extra {
        if win.cols.get(col).is_some_and(Option::is_none) {
            load_column(schema, meta, idx, win, col)?;
        }
    }
    Ok(())
}

/// Decodes series `idx` into compact columns, through the cache.
fn decoded_series(schema: &Schema, meta: &ChunkMeta, idx: usize) -> Result<Arc<DecodedSeries>> {
    let uid = meta.file.uid;
    let key = u32::try_from(idx).map_err(|_| corrupt("series entry index out of range"))?;
    if let Some(s) = cache::get_series(uid, key) {
        return Ok(s);
    }
    let entry = meta.series.get(idx).ok_or_else(|| corrupt("series entry index out of range"))?;
    let n = entry.row_count as usize;
    if n == 0 || u64::from(entry.row_count) > MAX_SERIES_ROWS {
        return Err(corrupt(format!("{} has a series with {n} rows", meta.path().display())));
    }
    if entry.blocks.len() != schema.data_indices().len() || entry.tags.len() != schema.tag_indices().len() {
        return Err(corrupt(format!("{} does not match the table schema", meta.path().display())));
    }
    block::check_materialization(n, 9 * schema.data_indices().len().max(1))
        .map_err(|e| corrupt(format!("{}: {e}", meta.path().display())))?;
    let mut columns: Vec<Option<block::ColumnData>> = (0..schema.columns().len()).map(|_| None).collect();
    for (&col, &b) in schema.data_indices().iter().zip(&entry.blocks) {
        let payload = read_block(meta, b)?;
        let data = block::decode_compact(schema.columns()[col].ty, n, &payload)
            .map_err(|e| corrupt(format!("{} at offset {}: {e}", meta.path().display(), b.offset)))?;
        if data.len() != n {
            return Err(corrupt("column block too short"));
        }
        columns[col] = Some(data);
    }
    let series = Arc::new(DecodedSeries { columns });
    cache::put_series(uid, key, series.clone());
    Ok(series)
}

/// Reads one row of series `idx` by its position inside the series. The
/// series is decoded once into compact columns kept in the shared cache, so
/// the next fetch of any row of it costs O(columns), not O(series length).
pub(crate) fn fetch_row(schema: &Schema, meta: &ChunkMeta, idx: usize, ordinal: usize) -> Result<Option<Row>> {
    let entry = meta.series.get(idx).ok_or_else(|| corrupt("series entry index out of range"))?;
    if ordinal >= entry.row_count as usize {
        return Ok(None);
    }
    let series = decoded_series(schema, meta, idx)?;
    let mut row = Vec::with_capacity(series.columns.len());
    for (col, data) in series.columns.iter().enumerate() {
        let v = match (data, schema.tag_position(col)) {
            (Some(d), _) => d.value_at(ordinal).ok_or_else(|| corrupt("column block too short"))?,
            (None, Some(t)) => entry.tags[t].clone(),
            (None, None) => return Err(corrupt("column without data")),
        };
        row.push(v);
    }
    Ok(Some(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_writer::ChunkBuilder;
    use crate::compression::Codec;
    use crate::schema::{Column, ColumnType, Value};

    fn schema() -> Schema {
        Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "v".into(), ty: ColumnType::Float64 },
            ],
            0,
        )
        .unwrap()
    }

    /// Writes a 2-series chunk (3 + 2 rows) and returns its path.
    fn build(dir: &Path) -> std::path::PathBuf {
        let schema = schema();
        let mut b = ChunkBuilder::new(&schema, Codec::None, None);
        for (sid, host, n) in [(1u64, "a", 3i64), (2, "b", 2)] {
            let rows: Vec<Row> = (0..n)
                .map(|i| vec![Value::Timestamp(i), Value::Bytes(host.as_bytes().to_vec()), Value::Float64(i as f64)])
                .collect();
            let refs: Vec<&Row> = rows.iter().collect();
            b.add_series(sid, &[Value::Bytes(host.as_bytes().to_vec())], &refs).unwrap();
        }
        b.finish(dir, 1, 0, (0, 10)).unwrap().path().to_path_buf()
    }

    /// Overwrites the `row_count` of series entry 0 inside the (plaintext) index.
    fn set_first_row_count(path: &Path, rows: u32) {
        let mut data = std::fs::read(path).unwrap();
        let footer = data.len() - FOOTER_LEN;
        let index_offset = u64::from_le_bytes(data[footer..footer + 8].try_into().unwrap()) as usize;
        // raw_bytes:u64 bucket:i64x2 n:u32, then the entry: series_id:u64 row_count:u32
        let at = index_offset + 8 + 16 + 4 + 8;
        data[at..at + 4].copy_from_slice(&rows.to_le_bytes());
        std::fs::write(path, data).unwrap();
    }

    /// One series of `n` rows (ts = 10 * i, v = i), written to a chunk.
    fn build_long(dir: &Path, n: i64) -> (Schema, ChunkMeta) {
        let schema = schema();
        let mut b = ChunkBuilder::new(&schema, Codec::Lz4, None);
        let rows: Vec<Row> = (0..n)
            .map(|i| vec![Value::Timestamp(10 * i), Value::Bytes(b"a".to_vec()), Value::Float64(i as f64)])
            .collect();
        let refs: Vec<&Row> = rows.iter().collect();
        b.add_series(1, &[Value::Bytes(b"a".to_vec())], &refs).unwrap();
        let meta = b.finish(dir, 7, 0, (0, 100_000)).unwrap();
        (schema, meta)
    }

    fn floats(w: &Window) -> Vec<f64> {
        let col = w.cols[2].as_ref().unwrap();
        (w.lo..w.hi)
            .map(|i| match col.value_at(i) {
                Some(Value::Float64(f)) => f,
                other => panic!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn narrow_and_repeated_reads_decode_once_and_return_the_same_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (schema, meta) = build_long(dir.path(), 400);
        let want = [true, false, true];
        // Narrow (3 of 400 rows): the whole column is kept for the next read.
        let first = load_window(&schema, &meta, 0, (1000, 1020), &want, true).unwrap().unwrap();
        assert_eq!(floats(&first), vec![100.0, 101.0, 102.0]);
        assert!(
            cache::get_column(meta.file.uid, 0, 2).is_some() || crate::settings::get().chunk_cache_bytes() == 0,
            "a narrow read leaves its columns in the cache"
        );
        let again = load_window(&schema, &meta, 0, (1000, 1020), &want, true).unwrap().unwrap();
        assert_eq!((again.lo, again.hi), (first.lo, first.hi));
        assert_eq!(floats(&again), floats(&first));
        // A different narrow range of the same series is served from the same columns.
        let other = load_window(&schema, &meta, 0, (3500, 3520), &want, true).unwrap().unwrap();
        assert_eq!(floats(&other), vec![350.0, 351.0, 352.0]);
        // A wide read agrees with the data whatever the cache holds.
        let wide = load_window(&schema, &meta, 0, (i64::MIN, i64::MAX), &want, true).unwrap().unwrap();
        assert_eq!(floats(&wide), (0..400).map(f64::from).collect::<Vec<_>>());
        let wide2 = load_window(&schema, &meta, 0, (500, 3000), &want, true).unwrap().unwrap();
        assert_eq!(floats(&wide2), (50..=300).map(f64::from).collect::<Vec<_>>());
        // Nothing in range.
        assert!(load_window(&schema, &meta, 0, (100_000, 200_000), &want, true).unwrap().is_none());
    }

    #[test]
    fn inconsistent_series_row_counts_are_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let path = build(dir.path());
        assert_eq!(read_meta(&path, 1).unwrap().header.row_count, 5);
        for forged in [0u32, 4, 1, u32::MAX, 1 << 27, (1 << 27) + 1] {
            set_first_row_count(&path, forged);
            assert!(matches!(read_meta(&path, 1), Err(Error::Corrupt(_))), "row_count {forged}");
        }
        set_first_row_count(&path, 3);
        assert!(read_meta(&path, 1).is_ok());
    }
}
