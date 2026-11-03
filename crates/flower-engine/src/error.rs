//! Engine errors.
//!
//! Storage failures capture the underlying message as text rather than
//! naming redb error types, so the error surface stays independent of
//! redb's. The one distinction kept is whether the disk failed
//! ([`Error::Io`]): after that redb refuses every later transaction of
//! the handle, so callers stop writing rather than retry.

use std::fmt;

/// Engine failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The store rejected an operation: lock contention, corruption
    /// reported by the page layer, or a failed commit for a reason other
    /// than the disk.
    Storage(String),
    /// The disk failed under the store: an I/O error (a full disk, a
    /// failing device), or redb refusing the handle after one. Permanent
    /// for the process: the file must be reopened to write again.
    Io(String),
    /// Stored bytes fail their codec: truncated values, unknown tags or
    /// invalid lengths. Indicates a bug or a foreign file, never valid data.
    Corrupt(String),
    /// A bounded counter or ID space is exhausted.
    Exhausted(String),
    /// A deployment manifest or descriptor failed validation.
    Manifest(String),
    /// The invocation budget is exhausted. Fatal and never savepoint-caught.
    Budget,
    /// The caller's deadline passed.
    Deadline,
    /// The caller's deadline passed first: the caller's, not the call's.
    CallerDeadline,
}

impl Error {
    /// Wrap a storage-layer failure, keeping its message.
    pub fn storage(message: impl Into<String>) -> Self {
        Error::Storage(message.into())
    }

    /// Report undecodable stored bytes.
    pub fn corrupt(message: impl Into<String>) -> Self {
        Error::Corrupt(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Storage(message) => write!(f, "storage failure: {message}"),
            Error::Io(message) => write!(f, "storage I/O failure: {message}"),
            Error::Corrupt(message) => write!(f, "corrupt stored data: {message}"),
            Error::Exhausted(message) => write!(f, "exhausted ID space: {message}"),
            Error::Manifest(message) => write!(f, "invalid manifest: {message}"),
            Error::Budget => write!(f, "budget exhausted"),
            Error::Deadline => write!(f, "deadline exceeded"),
            Error::CallerDeadline => write!(f, "the caller's deadline passed"),
        }
    }
}

impl std::error::Error for Error {}

/// The engine result.
pub type Result<T> = std::result::Result<T, Error>;

/// The failure code of [`Error::Io`]: the disk failed under the store.
pub const IO_FAILURE: &str = "host_io";

/// Execution layers report guest-visible failures, converting engine
/// errors at the boundary: infrastructure failures keep distinct codes
/// and never masquerade as business outcomes.
pub type HostResult<T> = std::result::Result<T, flower_sdk::Failure>;

impl From<Error> for flower_sdk::Failure {
    fn from(error: Error) -> Self {
        match error {
            Error::Storage(message) => flower_sdk::Failure::new("host_storage", message),
            Error::Io(message) => flower_sdk::Failure::new(IO_FAILURE, message),
            Error::Corrupt(message) => flower_sdk::Failure::new("host_corrupt", message),
            Error::Exhausted(message) => flower_sdk::Failure::new("host_exhausted", message),
            Error::Manifest(message) => flower_sdk::Failure::new("invalid_manifest", message),
            Error::Budget => flower_sdk::Failure::new("budget_exhausted", "budget exhausted"),
            Error::Deadline => flower_sdk::Failure::new(
                crate::runtime::wasm::DEADLINE_EXCEEDED,
                "the call ran past its own deadline",
            ),
            Error::CallerDeadline => flower_sdk::Failure::new(
                crate::runtime::wasm::CALLER_DEADLINE_EXCEEDED,
                "the caller's deadline passed",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_names_the_kind() {
        assert_eq!(
            Error::storage("locked").to_string(),
            "storage failure: locked"
        );
        assert_eq!(
            Error::corrupt("short read").to_string(),
            "corrupt stored data: short read"
        );
        assert_eq!(
            Error::Exhausted("intern IDs".to_string()).to_string(),
            "exhausted ID space: intern IDs"
        );
        assert_eq!(
            Error::Io("disk full".to_string()).to_string(),
            "storage I/O failure: disk full"
        );
        assert_eq!(
            flower_sdk::Failure::from(Error::Io("disk full".to_string())).code,
            IO_FAILURE
        );
    }
}
