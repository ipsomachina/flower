//! Backup errors.

use flower_crypto::aead::CryptoError;
use flower_sdk::Failure;

/// A backup, archive, restore or PITR failure.
#[derive(Clone, Debug)]
pub enum BackupError {
    /// Envelope or keystore failure.
    Crypto(CryptoError),
    /// Object storage failure.
    Store(String),
    /// Engine failure as `code: message`.
    Engine(String),
    /// Malformed manifest, segment or pointer.
    Format(String),
    /// Missing backup, manifest or object.
    NotFound(String),
    /// Refused: retention footguns, archive gaps, or missing bases.
    Refused(String),
    /// Compression IO failure.
    Io(String),
    /// The store holds objects in a format this version cannot read.
    Foreign(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupError::Crypto(error) => write!(f, "crypto: {error}"),
            BackupError::Store(error) => write!(f, "store: {error}"),
            BackupError::Engine(error) => write!(f, "engine: {error}"),
            BackupError::Format(error) => write!(f, "format: {error}"),
            BackupError::NotFound(error) => write!(f, "not found: {error}"),
            BackupError::Refused(error) => write!(f, "refused: {error}"),
            BackupError::Io(error) => write!(f, "io: {error}"),
            BackupError::Foreign(error) => write!(f, "foreign store: {error}"),
        }
    }
}

impl std::error::Error for BackupError {}

impl BackupError {
    /// Wrap an engine-typed error.
    pub(crate) fn engine<E>(error: E) -> Self
    where
        Failure: From<E>,
    {
        let failure = Failure::from(error);
        BackupError::Engine(format!("{}: {}", failure.code, failure.message))
    }
}
