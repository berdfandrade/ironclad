use std::io;

/// Errors produced by the storage engine and the server.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// On-disk data failed an integrity check (bad checksum, bad magic,
    /// truncated structure). Never silently ignored outside of the
    /// documented torn-tail rule for the active log.
    #[error("corruption: {0}")]
    Corruption(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The engine hit a fatal background error earlier and is now
    /// refusing writes (fail-stop).
    #[error("engine is in a failed state: {0}")]
    Poisoned(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(e) => e,
            other => io::Error::other(other.to_string()),
        }
    }
}
