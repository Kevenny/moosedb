use std::fmt;
use std::io;

/// Errors produced by the storage core. Each variant maps 1:1 onto a
/// `TFStatus` code at the FFI boundary.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    /// On-disk data failed validation (bad magic, CRC mismatch, truncated record).
    Corrupt(String),
    /// A resource limit was hit (e.g. MemTable cannot accept more rows because
    /// flushing keeps failing).
    Full(String),
    NotFound(String),
    InvalidArg(String),
    /// The table refuses writes, usually because an earlier failure left the
    /// in-memory state unable to guarantee durability.
    ReadOnly(String),
    /// The operation exists in the API but is not implemented yet.
    Unsupported(String),
    /// An encrypted structure did not decode: wrong key or damaged data. The
    /// two cannot be told apart, so callers must fail closed (never quarantine
    /// or rewrite metadata because of it).
    Crypto(String),
    /// A UNIQUE/PRIMARY key is already present (`INSERT` of an existing key).
    Duplicate(String),
    /// Another statement holds the series this row needs; nothing was changed
    /// and the caller may retry the same write (see `unique`).
    Busy(String),
    /// Waiting for that series would close a cycle of waiting statements.
    Deadlock(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corrupt(m) => write!(f, "corrupt data: {m}"),
            Error::Full(m) => write!(f, "full: {m}"),
            Error::NotFound(m) => write!(f, "not found: {m}"),
            Error::InvalidArg(m) => write!(f, "invalid argument: {m}"),
            Error::ReadOnly(m) => write!(f, "read-only: {m}"),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
            Error::Crypto(m) => write!(f, "encryption: {m}"),
            Error::Duplicate(m) => write!(f, "duplicate key: {m}"),
            Error::Busy(m) => write!(f, "busy: {m}"),
            Error::Deadlock(m) => write!(f, "deadlock: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

pub(crate) fn corrupt(msg: impl Into<String>) -> Error {
    Error::Corrupt(msg.into())
}

pub(crate) fn invalid(msg: impl Into<String>) -> Error {
    Error::InvalidArg(msg.into())
}
