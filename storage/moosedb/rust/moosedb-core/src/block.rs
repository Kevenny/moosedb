//! Column blocks: one per (series, data column) inside a chunk.
//!
//! ```text
//! block   := encoding:u8 codec:u8 reserved:u16 raw_len:u32 crc32:u32 stored
//! stored  := codec(payload)       crc32 covers the 8 bytes before it + `stored`
//! payload := 0 values                            (no NULLs)
//!          | 1 varint(len) rle(null flags) values
//! ```
//!
//! `values` holds only the non-NULL values, encoded by type:
//!
//! | type             | encoding                         |
//! |------------------|----------------------------------|
//! | TIMESTAMP        | delta-of-delta + Simple8b        |
//! | INT64            | delta + Simple8b                 |
//! | FLOAT64 / FLOAT32| Gorilla XOR                      |
//! | BOOL             | RLE                              |
//! | VARCHAR / DECIMAL| length-prefixed bytes            |
//!
//! The codec (LZ4/ZSTD) is applied only when it makes the block smaller.

use crate::bytes::{put_bytes, put_u16, put_u32, put_u8, put_varint, ByteReader};
use crate::cache::BlockPayload;
use crate::compression::{self, delta, gorilla, rle, Codec, CODEC_NONE};
use crate::error::{corrupt, invalid, Result};
use crate::schema::{ColumnType, Value};

pub(crate) const HEADER_LEN: usize = 12;

const ENC_PLAIN: u8 = 0;
const ENC_DELTA_OF_DELTA: u8 = 1;
const ENC_DELTA: u8 = 2;
const ENC_GORILLA64: u8 = 3;
const ENC_GORILLA32: u8 = 4;
const ENC_BOOL_RLE: u8 = 5;

pub(crate) struct EncodedBlock {
    pub bytes: Vec<u8>,
    /// Size the column would take as a plain, uncompressed array (statistics).
    pub plain_len: usize,
}

fn type_mismatch(ty: ColumnType, v: &Value) -> crate::error::Error {
    invalid(format!("cannot encode {v:?} in a {ty:?} block"))
}

/// Encodes `values` (all of type `ty`, NULLs allowed) and compresses with `codec`.
pub(crate) fn encode(ty: ColumnType, values: &[&Value], codec: Codec) -> Result<EncodedBlock> {
    let mut payload = Vec::new();
    let has_nulls = values.iter().any(|v| v.is_null());
    if has_nulls {
        put_u8(&mut payload, 1);
        let mut runs = Vec::new();
        rle::encode(values.iter().map(|v| v.is_null()), &mut runs);
        put_varint(&mut payload, runs.len() as u64);
        payload.extend_from_slice(&runs);
    } else {
        put_u8(&mut payload, 0);
    }
    let present: Vec<&Value> = values.iter().copied().filter(|v| !v.is_null()).collect();
    let mut plain_len = values.len().div_ceil(8);

    let encoding = match ty {
        ColumnType::Timestamp | ColumnType::Int64 => {
            let ints = present
                .iter()
                .map(|v| match (ty, v) {
                    (ColumnType::Timestamp, Value::Timestamp(x)) | (ColumnType::Int64, Value::Int(x)) => Ok(*x),
                    _ => Err(type_mismatch(ty, v)),
                })
                .collect::<Result<Vec<i64>>>()?;
            plain_len += ints.len() * 8;
            if ty == ColumnType::Timestamp {
                delta::encode_delta_of_delta(&ints, &mut payload);
                ENC_DELTA_OF_DELTA
            } else {
                delta::encode_delta(&ints, &mut payload);
                ENC_DELTA
            }
        }
        ColumnType::Float64 => {
            let bits = present
                .iter()
                .map(|v| if let Value::Float64(f) = v { Ok(f.to_bits()) } else { Err(type_mismatch(ty, v)) })
                .collect::<Result<Vec<u64>>>()?;
            plain_len += bits.len() * 8;
            gorilla::encode(&bits, 64, &mut payload);
            ENC_GORILLA64
        }
        ColumnType::Float32 => {
            let bits = present
                .iter()
                .map(|v| if let Value::Float32(f) = v { Ok(u64::from(f.to_bits())) } else { Err(type_mismatch(ty, v)) })
                .collect::<Result<Vec<u64>>>()?;
            plain_len += bits.len() * 4;
            gorilla::encode(&bits, 32, &mut payload);
            ENC_GORILLA32
        }
        ColumnType::Bool => {
            let bools = present
                .iter()
                .map(|v| if let Value::Bool(b) = v { Ok(*b) } else { Err(type_mismatch(ty, v)) })
                .collect::<Result<Vec<bool>>>()?;
            plain_len += bools.len();
            rle::encode(bools, &mut payload);
            ENC_BOOL_RLE
        }
        ColumnType::Varchar | ColumnType::Decimal => {
            for v in &present {
                let Value::Bytes(b) = v else { return Err(type_mismatch(ty, v)) };
                plain_len += 4 + b.len();
                put_bytes(&mut payload, b);
            }
            ENC_PLAIN
        }
        ColumnType::Tag => return Err(invalid("TAG columns are not stored in data blocks")),
    };

    if payload.len() > compression::MAX_BLOCK_RAW {
        return Err(invalid(format!(
            "column block of {} bytes exceeds the {} byte limit",
            payload.len(),
            compression::MAX_BLOCK_RAW
        )));
    }
    let raw_len = u32::try_from(payload.len()).map_err(|_| invalid("column block exceeds 4GiB"))?;
    let (codec_id, stored) = match codec {
        Codec::None => (CODEC_NONE, payload),
        c => {
            let compressed = compression::compress(c, &payload)?;
            if compressed.len() < payload.len() {
                (c.id(), compressed)
            } else {
                (CODEC_NONE, payload)
            }
        }
    };
    let mut bytes = Vec::with_capacity(HEADER_LEN + stored.len());
    put_u8(&mut bytes, encoding);
    put_u8(&mut bytes, codec_id);
    put_u16(&mut bytes, 0);
    put_u32(&mut bytes, raw_len);
    let mut h = crc32fast::Hasher::new();
    h.update(&bytes);
    h.update(&stored);
    put_u32(&mut bytes, h.finalize());
    bytes.extend_from_slice(&stored);
    Ok(EncodedBlock { bytes, plain_len })
}

/// Verifies and decompresses a block read from disk (already decrypted).
pub(crate) fn unpack(bytes: &[u8]) -> Result<BlockPayload> {
    let mut r = ByteReader::new(bytes);
    let encoding = r.u8()?;
    let codec = r.u8()?;
    r.u16()?;
    let raw_len = r.u32()? as usize;
    let crc = r.u32()?;
    let stored = r.take(r.remaining())?;
    let mut h = crc32fast::Hasher::new();
    h.update(&bytes[..8]);
    h.update(stored);
    if h.finalize() != crc {
        return Err(corrupt("column block checksum mismatch (corrupt data or wrong key)"));
    }
    let data = compression::decompress(codec, stored, raw_len)?;
    Ok(BlockPayload { encoding, data })
}

/// Rejects a decode that would materialize more than `MAX_DECODE_BYTES`.
/// `n` comes from the file and a few bytes can encode a huge run (RLE booleans,
/// Simple8b zeros), so the size of what we are about to allocate is bounded by
/// the count itself, before anything is allocated.
pub(crate) fn check_materialization(n: usize, bytes_per_value: usize) -> Result<()> {
    match n.checked_mul(bytes_per_value) {
        Some(b) if b <= compression::MAX_DECODE_BYTES => Ok(()),
        _ => Err(corrupt(format!(
            "column block claims {n} values ({bytes_per_value} bytes each in memory): over the {} byte decode limit",
            compression::MAX_DECODE_BYTES
        ))),
    }
}

/// The present (non-NULL) values of a block, still in their encoded domain.
enum Present<'a> {
    Ints(Vec<i64>),
    Bits(Vec<u64>),
    Bools(Vec<bool>),
    Bytes(Vec<&'a [u8]>),
}

/// Null map, value count and the reader positioned at the values.
struct Parsed<'a> {
    r: ByteReader<'a>,
    nulls: Option<Vec<bool>>,
    present: usize,
}

fn parse_prefix(n: usize, block: &BlockPayload) -> Result<Parsed<'_>> {
    let mut r = ByteReader::new(&block.data);
    let nulls = match r.u8()? {
        0 => None,
        1 => {
            let len = usize::try_from(r.varint()?).map_err(|_| corrupt("null map too large"))?;
            let mut nr = ByteReader::new(r.take(len)?);
            Some(rle::decode(&mut nr, n)?)
        }
        f => return Err(corrupt(format!("invalid null flag {f}"))),
    };
    let present = nulls.as_ref().map_or(n, |m| m.iter().filter(|b| !**b).count());
    Ok(Parsed { r, nulls, present })
}

fn decode_present<'a>(ty: ColumnType, encoding: u8, r: &mut ByteReader<'a>, present: usize) -> Result<Present<'a>> {
    decode_present_prefix(ty, encoding, r, present, present)
}

/// Like [`decode_present`] but materializes only the first `take` of the
/// `present` values; `take < present` skips the tail (and its strict checks).
fn decode_present_prefix<'a>(
    ty: ColumnType,
    encoding: u8,
    r: &mut ByteReader<'a>,
    present: usize,
    take: usize,
) -> Result<Present<'a>> {
    let expect = |e: u8| {
        if encoding == e {
            Ok(())
        } else {
            Err(corrupt(format!("encoding {encoding} does not match column type {ty:?}")))
        }
    };
    let values = match ty {
        ColumnType::Timestamp => {
            expect(ENC_DELTA_OF_DELTA)?;
            Present::Ints(delta::decode_delta_of_delta_prefix(r, present, take)?)
        }
        ColumnType::Int64 => {
            expect(ENC_DELTA)?;
            Present::Ints(delta::decode_delta_prefix(r, present, take)?)
        }
        ColumnType::Float64 => {
            expect(ENC_GORILLA64)?;
            Present::Bits(gorilla::decode_prefix(r.take(r.remaining())?, present, take, 64)?)
        }
        ColumnType::Float32 => {
            expect(ENC_GORILLA32)?;
            Present::Bits(gorilla::decode_prefix(r.take(r.remaining())?, present, take, 32)?)
        }
        ColumnType::Bool => {
            expect(ENC_BOOL_RLE)?;
            let mut v = rle::decode(r, present)?;
            v.truncate(take);
            Present::Bools(v)
        }
        ColumnType::Varchar | ColumnType::Decimal => {
            expect(ENC_PLAIN)?;
            // Every value takes at least its 4-byte length prefix.
            if present > r.remaining() / 4 {
                return Err(corrupt("column block holds fewer values than expected"));
            }
            let mut v = Vec::with_capacity(take);
            for _ in 0..take {
                v.push(r.bytes()?);
            }
            Present::Bytes(v)
        }
        ColumnType::Tag => return Err(corrupt("tag column stored as data block")),
    };
    if take == present && r.remaining() != 0 {
        return Err(corrupt("trailing bytes in column block"));
    }
    let got = match &values {
        Present::Ints(v) => v.len(),
        Present::Bits(v) => v.len(),
        Present::Bools(v) => v.len(),
        Present::Bytes(v) => v.len(),
    };
    if got != take {
        return Err(corrupt("column block value count mismatch"));
    }
    Ok(values)
}

/// Decodes `n` values of type `ty` from an unpacked block.
pub(crate) fn decode(ty: ColumnType, n: usize, block: &BlockPayload) -> Result<Vec<Value>> {
    check_materialization(n, std::mem::size_of::<Value>())?;
    let Parsed { mut r, nulls, present } = parse_prefix(n, block)?;
    let values: Vec<Value> = match decode_present(ty, block.encoding, &mut r, present)? {
        Present::Ints(v) if ty == ColumnType::Timestamp => v.into_iter().map(Value::Timestamp).collect(),
        Present::Ints(v) => v.into_iter().map(Value::Int).collect(),
        Present::Bits(v) if ty == ColumnType::Float64 => {
            v.into_iter().map(|b| Value::Float64(f64::from_bits(b))).collect()
        }
        Present::Bits(v) => v.into_iter().map(|b| Value::Float32(f32::from_bits(b as u32))).collect(),
        Present::Bools(v) => v.into_iter().map(Value::Bool).collect(),
        Present::Bytes(v) => v.into_iter().map(|b| Value::Bytes(b.to_vec())).collect(),
    };
    let Some(nulls) = nulls else { return Ok(values) };
    let mut it = values.into_iter();
    nulls
        .into_iter()
        .map(|is_null| if is_null { Ok(Value::Null) } else { it.next().ok_or_else(|| corrupt("null map mismatch")) })
        .collect()
}

/// One decoded column in its native width (8 bytes per number instead of the
/// 24 of a `Value`), with NULL slots filled by a default and flagged in
/// `nulls`. Random access by ordinal is O(1), which is what `rnd_pos` needs.
pub(crate) struct ColumnData {
    ty: ColumnType,
    nulls: Option<Vec<bool>>,
    values: Dense,
}

enum Dense {
    Ints(Vec<i64>),
    Bits64(Vec<u64>),
    Bits32(Vec<u32>),
    Bools(Vec<bool>),
    /// `n + 1` offsets into `data`.
    Bytes {
        offsets: Vec<u32>,
        data: Vec<u8>,
    },
}

fn spread<T: Copy + Default>(present: Vec<T>, nulls: Option<&[bool]>) -> Result<Vec<T>> {
    let Some(nulls) = nulls else { return Ok(present) };
    let mut it = present.into_iter();
    nulls
        .iter()
        .map(|&is_null| if is_null { Ok(T::default()) } else { it.next().ok_or_else(|| corrupt("null map mismatch")) })
        .collect()
}

/// Like [`decode`], but keeps the column compact (see [`ColumnData`]).
pub(crate) fn decode_compact(ty: ColumnType, n: usize, block: &BlockPayload) -> Result<ColumnData> {
    decode_compact_prefix(ty, n, n, block)
}

/// Like [`decode_compact`] for the first `upto` of the block's `n` values: the
/// column holds `upto` rows and the codec stops there. Delta and Gorilla
/// streams cannot be entered in the middle, so a leading part is the cheapest
/// way to reach rows `[a, b)`: decode `[0, b)` and use `[a, b)`.
pub(crate) fn decode_compact_prefix(ty: ColumnType, n: usize, upto: usize, block: &BlockPayload) -> Result<ColumnData> {
    check_materialization(n, 9)?;
    let upto = upto.min(n);
    let Parsed { mut r, mut nulls, present } = parse_prefix(n, block)?;
    let take = if upto == n {
        present
    } else {
        nulls.as_mut().map_or(upto, |m| {
            m.truncate(upto);
            m.iter().filter(|b| !**b).count()
        })
    };
    let n = upto;
    let nl = nulls.as_deref();
    let values = match decode_present_prefix(ty, block.encoding, &mut r, present, take)? {
        Present::Ints(v) => Dense::Ints(spread(v, nl)?),
        Present::Bits(v) if ty == ColumnType::Float64 => Dense::Bits64(spread(v, nl)?),
        Present::Bits(v) => Dense::Bits32(spread(v.into_iter().map(|b| b as u32).collect(), nl)?),
        Present::Bools(v) => Dense::Bools(spread(v, nl)?),
        Present::Bytes(v) => {
            let total: usize = v.iter().map(|b| b.len()).sum();
            let mut data = Vec::with_capacity(total);
            let mut offsets = Vec::with_capacity(n + 1);
            offsets.push(0u32);
            let mut it = v.into_iter();
            for k in 0..n {
                if !nl.is_some_and(|m| m[k]) {
                    data.extend_from_slice(it.next().ok_or_else(|| corrupt("null map mismatch"))?);
                }
                offsets.push(u32::try_from(data.len()).map_err(|_| corrupt("column block too large"))?);
            }
            Dense::Bytes { offsets, data }
        }
    };
    Ok(ColumnData { ty, nulls, values })
}

/// Borrowed view of a column's values, for bulk copies (scan batches).
pub(crate) enum DenseRef<'a> {
    Ints(&'a [i64]),
    Bits64(&'a [u64]),
    Bits32(&'a [u32]),
    Bools(&'a [bool]),
    /// `len + 1` offsets into `data`.
    Bytes {
        offsets: &'a [u32],
        data: &'a [u8],
    },
}

impl ColumnData {
    pub(crate) fn dense(&self) -> DenseRef<'_> {
        match &self.values {
            Dense::Ints(v) => DenseRef::Ints(v),
            Dense::Bits64(v) => DenseRef::Bits64(v),
            Dense::Bits32(v) => DenseRef::Bits32(v),
            Dense::Bools(v) => DenseRef::Bools(v),
            Dense::Bytes { offsets, data } => DenseRef::Bytes { offsets, data },
        }
    }

    /// NULL flags (`true` = NULL), if the column has any NULL.
    pub(crate) fn null_flags(&self) -> Option<&[bool]> {
        self.nulls.as_deref()
    }

    /// The values of an integer-like column (TIMESTAMP, INT64).
    pub(crate) fn ints(&self) -> Option<&[i64]> {
        match &self.values {
            Dense::Ints(v) => Some(v),
            _ => None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.values {
            Dense::Ints(v) => v.len(),
            Dense::Bits64(v) => v.len(),
            Dense::Bits32(v) => v.len(),
            Dense::Bools(v) => v.len(),
            Dense::Bytes { offsets, .. } => offsets.len() - 1,
        }
    }

    /// Heap bytes held (cache accounting).
    pub(crate) fn heap_bytes(&self) -> usize {
        let nulls = self.nulls.as_ref().map_or(0, Vec::len);
        nulls
            + match &self.values {
                Dense::Ints(v) => v.len() * 8,
                Dense::Bits64(v) => v.len() * 8,
                Dense::Bits32(v) => v.len() * 4,
                Dense::Bools(v) => v.len(),
                Dense::Bytes { offsets, data } => offsets.len() * 4 + data.len(),
            }
    }

    /// The value at `i`, or `None` past the end.
    pub(crate) fn value_at(&self, i: usize) -> Option<Value> {
        if i >= self.len() {
            return None;
        }
        if self.nulls.as_ref().is_some_and(|m| m[i]) {
            return Some(Value::Null);
        }
        Some(match &self.values {
            Dense::Ints(v) if self.ty == ColumnType::Timestamp => Value::Timestamp(v[i]),
            Dense::Ints(v) => Value::Int(v[i]),
            Dense::Bits64(v) => Value::Float64(f64::from_bits(v[i])),
            Dense::Bits32(v) => Value::Float32(f32::from_bits(v[i])),
            Dense::Bools(v) => Value::Bool(v[i]),
            Dense::Bytes { offsets, data } => Value::Bytes(data[offsets[i] as usize..offsets[i + 1] as usize].to_vec()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(ty: ColumnType, values: Vec<Value>, codec: Codec) -> EncodedBlock {
        let refs: Vec<&Value> = values.iter().collect();
        let b = encode(ty, &refs, codec).unwrap();
        let p = unpack(&b.bytes).unwrap();
        assert_eq!(decode(ty, values.len(), &p).unwrap(), values, "{ty:?} {codec:?}");
        b
    }

    #[test]
    fn a_leading_part_of_a_column_decodes_like_the_whole() {
        let n = 300usize;
        let nullable = |f: &dyn Fn(usize) -> Value| -> Vec<Value> {
            (0..n).map(|i| if i % 5 == 2 || (100..110).contains(&i) { Value::Null } else { f(i) }).collect()
        };
        let cases: Vec<(ColumnType, Vec<Value>)> = vec![
            (ColumnType::Timestamp, (0..n).map(|i| Value::Timestamp(i as i64 * 10 + (i % 3) as i64)).collect()),
            (ColumnType::Int64, nullable(&|i| Value::Int(i as i64 * i as i64))),
            (ColumnType::Float64, nullable(&|i| Value::Float64(i as f64 / 3.0))),
            (ColumnType::Float32, nullable(&|i| Value::Float32(i as f32 * 0.5))),
            (ColumnType::Bool, nullable(&|i| Value::Bool(i % 3 == 0))),
            (ColumnType::Varchar, nullable(&|i| Value::Bytes(vec![b'a'; i % 9]))),
        ];
        for codec in [Codec::None, Codec::Zstd(3)] {
            for (ty, values) in &cases {
                let refs: Vec<&Value> = values.iter().collect();
                let p = unpack(&encode(*ty, &refs, codec).unwrap().bytes).unwrap();
                for upto in [0, 1, 2, 3, 99, 100, 105, 110, 111, 299, 300, 400] {
                    let col = decode_compact_prefix(*ty, n, upto, &p).unwrap();
                    let k = upto.min(n);
                    assert_eq!(col.len(), k, "{ty:?} upto {upto}");
                    for (i, v) in values.iter().enumerate().take(k) {
                        assert_eq!(col.value_at(i).as_ref(), Some(v), "{ty:?} upto {upto} row {i}");
                    }
                }
            }
        }
    }

    #[test]
    fn every_type_roundtrips_with_every_codec() {
        for codec in [Codec::None, Codec::Lz4, Codec::Zstd(3)] {
            rt(
                ColumnType::Timestamp,
                vec![Value::Timestamp(5), Value::Timestamp(-7), Value::Timestamp(i64::MAX)],
                codec,
            );
            rt(ColumnType::Int64, vec![Value::Int(1), Value::Null, Value::Int(i64::MIN), Value::Null], codec);
            rt(ColumnType::Float64, vec![Value::Float64(1.5), Value::Null, Value::Float64(-0.0)], codec);
            rt(ColumnType::Float32, vec![Value::Float32(1.5), Value::Float32(f32::MAX)], codec);
            rt(ColumnType::Bool, vec![Value::Bool(true), Value::Null, Value::Bool(false), Value::Bool(false)], codec);
            rt(ColumnType::Varchar, vec![Value::Bytes(b"abc".to_vec()), Value::Null, Value::Bytes(vec![])], codec);
            rt(ColumnType::Decimal, vec![Value::Bytes(b"-1.50".to_vec())], codec);
            rt(ColumnType::Float64, vec![Value::Null, Value::Null], codec);
            rt(ColumnType::Int64, vec![], codec);
        }
    }

    #[test]
    fn typical_metrics_compress_well() {
        let n = 10_000;
        let ts: Vec<Value> = (0..n).map(|i| Value::Timestamp(1_700_000_000_000_000 + i * 10_000_000)).collect();
        let vals: Vec<Value> = (0..n).map(|i| Value::Float64(50.0 + ((i / 7) % 20) as f64 * 0.5)).collect();
        let t = rt(ColumnType::Timestamp, ts, Codec::Zstd(3));
        let v = rt(ColumnType::Float64, vals, Codec::Zstd(3));
        assert!(t.bytes.len() * 100 < t.plain_len, "timestamps: {} of {}", t.bytes.len(), t.plain_len);
        assert!(v.bytes.len() * 10 < v.plain_len, "values: {} of {}", v.bytes.len(), v.plain_len);
    }

    /// Builds a block with a valid CRC around arbitrary header fields.
    fn forge(codec: u8, raw_len: u32, stored: &[u8]) -> Vec<u8> {
        let mut b = vec![ENC_DELTA, codec, 0, 0];
        put_u32(&mut b, raw_len);
        let mut h = crc32fast::Hasher::new();
        h.update(&b);
        h.update(stored);
        put_u32(&mut b, h.finalize());
        b.extend_from_slice(stored);
        b
    }

    #[test]
    fn forged_raw_len_is_corrupt_not_oom() {
        let small = [1u8; 32];
        let lz4 = compression::compress(Codec::Lz4, &small).unwrap();
        let zstd = compression::compress(Codec::Zstd(3), &small).unwrap();
        for (codec, stored) in [(compression::CODEC_LZ4, &lz4), (compression::CODEC_ZSTD, &zstd)] {
            for raw in [0xFFFF_FFF0u32, u32::MAX, 1 << 30] {
                assert!(matches!(unpack(&forge(codec, raw, stored)), Err(crate::error::Error::Corrupt(_))));
            }
        }
        // A consistent block still unpacks.
        assert!(unpack(&forge(compression::CODEC_ZSTD, small.len() as u32, &zstd)).is_ok());
    }

    /// A block whose valid RLE stream (a handful of bytes) claims 2^27 values.
    fn forged_bool_block() -> BlockPayload {
        let mut data = vec![0u8]; // no NULLs
        put_varint(&mut data, 0);
        put_varint(&mut data, (1 << 27) - 1);
        BlockPayload { encoding: ENC_BOOL_RLE, data }
    }

    #[test]
    fn a_tiny_block_cannot_make_decode_materialize_gigabytes() {
        // Fuzz finding: BOOL, RLE, 2^27 rows ⇒ 2^27 × 24 B of `Value`.
        let block = forged_bool_block();
        for n in [1usize << 27, (1 << 27) - 1, 100_000_000, 30_000_000, usize::MAX] {
            assert!(matches!(decode(ColumnType::Bool, n, &block), Err(crate::error::Error::Corrupt(_))), "{n}");
            assert!(matches!(decode_compact(ColumnType::Bool, n, &block), Err(crate::error::Error::Corrupt(_))), "{n}");
        }
        // Delta streams: 4.5 MB of zero words claim 2^27 integers.
        let mut ints = vec![0u8];
        ints.extend_from_slice(&0i64.to_le_bytes());
        ints.push(0);
        for _ in 0..(1usize << 27).div_ceil(240) {
            ints.extend_from_slice(&0u64.to_le_bytes());
        }
        for (ty, enc) in [(ColumnType::Int64, ENC_DELTA), (ColumnType::Timestamp, ENC_DELTA_OF_DELTA)] {
            let block = BlockPayload { encoding: enc, data: ints.clone() };
            assert!(matches!(decode(ty, 1 << 27, &block), Err(crate::error::Error::Corrupt(_))));
            assert!(matches!(decode_compact(ty, 1 << 27, &block), Err(crate::error::Error::Corrupt(_))));
        }
        // What a writer produces still decodes: 1M identical booleans are a few bytes.
        let vals = vec![Value::Bool(true); 1_000_000];
        let refs: Vec<&Value> = vals.iter().collect();
        let enc = encode(ColumnType::Bool, &refs, Codec::None).unwrap();
        assert!(enc.bytes.len() < 64);
        let p = unpack(&enc.bytes).unwrap();
        assert_eq!(decode(ColumnType::Bool, vals.len(), &p).unwrap().len(), 1_000_000);
    }

    #[test]
    fn compact_columns_return_exactly_what_the_value_decoder_does() {
        let n = 1000;
        let cases: Vec<(ColumnType, Vec<Value>)> = vec![
            (ColumnType::Timestamp, (0..n).map(|i| Value::Timestamp(1_700_000_000 + i * 7)).collect()),
            (
                ColumnType::Int64,
                (0..n).map(|i| if i % 5 == 0 { Value::Null } else { Value::Int(i * i - 99) }).collect(),
            ),
            (
                ColumnType::Float64,
                (0..n).map(|i| if i % 3 == 0 { Value::Null } else { Value::Float64(i as f64 / 7.0) }).collect(),
            ),
            (ColumnType::Float32, (0..n).map(|i| Value::Float32(i as f32 * 0.5)).collect()),
            (
                ColumnType::Bool,
                (0..n).map(|i| if i % 11 == 0 { Value::Null } else { Value::Bool(i % 4 < 2) }).collect(),
            ),
            (
                ColumnType::Varchar,
                (0..n)
                    .map(|i| match i % 4 {
                        0 => Value::Null,
                        1 => Value::Bytes(vec![]),
                        _ => Value::Bytes(format!("v{i}").into_bytes()),
                    })
                    .collect(),
            ),
            (ColumnType::Decimal, vec![Value::Null, Value::Null]),
            (ColumnType::Int64, vec![Value::Int(5)]),
        ];
        for (ty, vals) in cases {
            let refs: Vec<&Value> = vals.iter().collect();
            let p = unpack(&encode(ty, &refs, Codec::Lz4).unwrap().bytes).unwrap();
            let compact = decode_compact(ty, vals.len(), &p).unwrap();
            assert_eq!(compact.len(), vals.len(), "{ty:?}");
            for (i, v) in vals.iter().enumerate() {
                assert_eq!(&compact.value_at(i).unwrap(), v, "{ty:?} #{i}");
            }
            assert!(compact.value_at(vals.len()).is_none());
            assert_eq!(decode(ty, vals.len(), &p).unwrap(), vals);
        }
    }

    #[test]
    fn corruption_and_type_errors() {
        let vals = [Value::Int(1), Value::Int(2)];
        let refs: Vec<&Value> = vals.iter().collect();
        let mut b = encode(ColumnType::Int64, &refs, Codec::None).unwrap().bytes;
        let last = b.len() - 1;
        b[last] ^= 1;
        assert!(unpack(&b).is_err());
        assert!(encode(ColumnType::Int64, &[&Value::Float64(1.0)], Codec::None).is_err());
        let good = encode(ColumnType::Int64, &refs, Codec::None).unwrap().bytes;
        assert!(decode(ColumnType::Float64, 2, &unpack(&good).unwrap()).is_err());
        assert!(decode(ColumnType::Int64, 3, &unpack(&good).unwrap()).is_err());
    }
}
