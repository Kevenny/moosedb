//! Column types, values and table schema.

use crate::error::{invalid, Result};

/// Logical column types. Discriminants are part of the FFI contract
/// (`TFColumnType`) and of the on-disk format — never renumber.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ColumnType {
    Timestamp = 0,
    Int64 = 1,
    Float64 = 2,
    Float32 = 3,
    Bool = 4,
    Varchar = 5,
    /// Label column: part of the series identity, stored once per series.
    Tag = 6,
    /// Exact decimal, transported as its canonical string representation.
    Decimal = 7,
}

impl ColumnType {
    pub fn from_u8(v: u8) -> Option<ColumnType> {
        Some(match v {
            0 => ColumnType::Timestamp,
            1 => ColumnType::Int64,
            2 => ColumnType::Float64,
            3 => ColumnType::Float32,
            4 => ColumnType::Bool,
            5 => ColumnType::Varchar,
            6 => ColumnType::Tag,
            7 => ColumnType::Decimal,
            _ => return None,
        })
    }

    /// Whether values of this type travel as byte strings.
    pub fn is_bytes(self) -> bool {
        matches!(self, ColumnType::Varchar | ColumnType::Tag | ColumnType::Decimal)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    /// Microseconds since the Unix epoch.
    Timestamp(i64),
    Int(i64),
    Float64(f64),
    Float32(f32),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Whether this value can be stored in a column of type `ty`.
    pub fn fits(&self, ty: ColumnType) -> bool {
        matches!(
            (self, ty),
            (Value::Null, _)
                | (Value::Timestamp(_), ColumnType::Timestamp)
                | (Value::Int(_), ColumnType::Int64)
                | (Value::Float64(_), ColumnType::Float64)
                | (Value::Float32(_), ColumnType::Float32)
                | (Value::Bool(_), ColumnType::Bool)
                | (Value::Bytes(_), ColumnType::Varchar | ColumnType::Tag | ColumnType::Decimal)
        )
    }

    /// Approximate heap + inline footprint, used for MemTable accounting.
    pub(crate) fn mem_size(&self) -> usize {
        let inline = std::mem::size_of::<Value>();
        match self {
            Value::Bytes(b) => inline + b.len(),
            _ => inline,
        }
    }
}

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
}

/// Largest single value (VARCHAR/TAG/DECIMAL bytes) a write accepts. A flush
/// encodes each column of a series into one block of at most
/// `compression::MAX_BLOCK_RAW` (256 MiB) and cannot split one value, so the
/// ceiling must leave room for the block overhead and for neighbours.
pub const MAX_VALUE_BYTES: usize = 64 << 20;
/// Largest sum of the byte values of one row a write accepts. Well below the
/// WAL entry limit (1 GiB), so an accepted row always fits one WAL entry.
pub const MAX_ROW_BYTES: usize = 128 << 20;

/// Upper bound on columns per table; also keeps row encodings within `u16` counts.
pub const MAX_COLUMNS: usize = 4096;

#[derive(Clone, Debug)]
pub struct Schema {
    columns: Vec<Column>,
    ts_index: usize,
    /// Indices of `Tag` columns, in schema order. Defines the series key.
    tag_indices: Vec<usize>,
    /// Indices of every non-tag column (including the timestamp), in schema
    /// order. Each one gets its own data block per series in a chunk.
    data_indices: Vec<usize>,
    /// See [`Schema::fingerprint`]; computed once (every scan asks for it).
    fingerprint: u32,
}

impl Schema {
    pub fn new(columns: Vec<Column>, ts_index: usize) -> Result<Schema> {
        if columns.is_empty() || columns.len() > MAX_COLUMNS {
            return Err(invalid(format!("column count must be between 1 and {MAX_COLUMNS}, got {}", columns.len())));
        }
        match columns.get(ts_index) {
            Some(c) if c.ty == ColumnType::Timestamp => {}
            Some(c) => return Err(invalid(format!("timestamp column '{}' must have type TIMESTAMP", c.name))),
            None => return Err(invalid(format!("timestamp column index {ts_index} out of range"))),
        }
        let tag_indices = (0..columns.len()).filter(|&i| columns[i].ty == ColumnType::Tag).collect();
        let data_indices = (0..columns.len()).filter(|&i| columns[i].ty != ColumnType::Tag).collect();
        let fingerprint = Self::compute_fingerprint(&columns, ts_index);
        Ok(Schema { columns, ts_index, tag_indices, data_indices, fingerprint })
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn ts_index(&self) -> usize {
        self.ts_index
    }

    pub fn tag_indices(&self) -> &[usize] {
        &self.tag_indices
    }

    pub fn data_indices(&self) -> &[usize] {
        &self.data_indices
    }

    /// Position of schema column `col` inside the tag list, if it is a tag.
    pub fn tag_position(&self, col: usize) -> Option<usize> {
        self.tag_indices.iter().position(|&i| i == col)
    }

    /// Timestamp of a row that already passed `validate_row`.
    pub fn row_ts(&self, row: &[Value]) -> i64 {
        match row.get(self.ts_index) {
            Some(Value::Timestamp(t)) => *t,
            _ => i64::MIN,
        }
    }

    pub fn validate_row(&self, row: &[Value]) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(invalid(format!("row has {} values, table has {} columns", row.len(), self.columns.len())));
        }
        for (v, c) in row.iter().zip(&self.columns) {
            if !v.fits(c.ty) {
                return Err(invalid(format!("value {v:?} does not fit column '{}' ({:?})", c.name, c.ty)));
            }
            if let Value::Bytes(b) = v {
                if u32::try_from(b.len()).is_err() {
                    return Err(invalid(format!("value for column '{}' exceeds 4GiB", c.name)));
                }
            }
        }
        if row[self.ts_index].is_null() {
            return Err(invalid(format!("timestamp column '{}' cannot be NULL", self.columns[self.ts_index].name)));
        }
        Ok(())
    }

    /// [`Schema::validate_row`] plus the size limits of the *write* path.
    /// WAL replay uses `validate_row` alone: data accepted by an older
    /// version (which allowed up to 1 GiB per entry) must stay readable.
    pub fn validate_for_write(&self, row: &[Value]) -> Result<()> {
        self.validate_row(row)?;
        let mut total = 0usize;
        for (v, c) in row.iter().zip(&self.columns) {
            if let Value::Bytes(b) = v {
                if b.len() > MAX_VALUE_BYTES {
                    return Err(invalid(format!(
                        "value of {} bytes exceeds the {} MiB limit (column '{}')",
                        b.len(),
                        MAX_VALUE_BYTES >> 20,
                        c.name
                    )));
                }
                total += b.len();
            }
        }
        if total > MAX_ROW_BYTES {
            return Err(invalid(format!("row of {total} bytes exceeds the {} MiB limit", MAX_ROW_BYTES >> 20)));
        }
        Ok(())
    }

    /// Stable fingerprint of the column type layout, stored in every chunk to
    /// detect a chunk being opened with an incompatible schema.
    pub fn fingerprint(&self) -> u32 {
        self.fingerprint
    }

    fn compute_fingerprint(columns: &[Column], ts_index: usize) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.update(&(ts_index as u32).to_le_bytes());
        for c in columns {
            h.update(&[c.ty as u8]);
        }
        h.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn sample_schema() -> Schema {
        Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "host".into(), ty: ColumnType::Tag },
                Column { name: "value".into(), ty: ColumnType::Float64 },
            ],
            0,
        )
        .unwrap()
    }

    #[test]
    fn tag_and_data_split() {
        let s = sample_schema();
        assert_eq!(s.tag_indices(), &[1]);
        assert_eq!(s.data_indices(), &[0, 2]);
        assert_eq!(s.tag_position(1), Some(0));
        assert_eq!(s.tag_position(2), None);
    }

    #[test]
    fn rejects_bad_rows() {
        let s = sample_schema();
        assert!(s.validate_row(&[Value::Timestamp(1), Value::Bytes(b"a".to_vec())]).is_err());
        assert!(s.validate_row(&[Value::Null, Value::Bytes(b"a".to_vec()), Value::Float64(1.0)]).is_err());
        assert!(s.validate_row(&[Value::Timestamp(1), Value::Int(3), Value::Float64(1.0)]).is_err());
        assert!(s.validate_row(&[Value::Timestamp(1), Value::Bytes(b"a".to_vec()), Value::Null]).is_ok());
    }

    #[test]
    fn write_limits_apply_per_value_and_per_row_but_not_to_validate_row() {
        let s = Schema::new(
            vec![
                Column { name: "ts".into(), ty: ColumnType::Timestamp },
                Column { name: "a".into(), ty: ColumnType::Varchar },
                Column { name: "b".into(), ty: ColumnType::Varchar },
                Column { name: "c".into(), ty: ColumnType::Varchar },
            ],
            0,
        )
        .unwrap();
        let big = |n: usize| Value::Bytes(vec![0u8; n]);
        let row = |a, b, c| vec![Value::Timestamp(1), a, b, c];
        assert!(s.validate_for_write(&row(big(MAX_VALUE_BYTES), big(MAX_VALUE_BYTES), Value::Null)).is_ok());
        let err = s.validate_for_write(&row(big(MAX_VALUE_BYTES + 1), Value::Null, Value::Null)).unwrap_err();
        assert!(err.to_string().contains("exceeds the 64 MiB limit"), "{err}");
        let err = s.validate_for_write(&row(big(MAX_VALUE_BYTES), big(MAX_VALUE_BYTES), big(1))).unwrap_err();
        assert!(err.to_string().contains("128 MiB"), "{err}");
        // Replay accepts what older versions wrote.
        assert!(s.validate_row(&row(big(MAX_VALUE_BYTES + 1), Value::Null, Value::Null)).is_ok());
    }

    #[test]
    fn ts_column_must_be_timestamp() {
        let cols = vec![Column { name: "x".into(), ty: ColumnType::Int64 }];
        assert!(Schema::new(cols, 0).is_err());
    }
}
