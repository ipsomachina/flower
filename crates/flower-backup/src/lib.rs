//! `flower-backup`: continuous encrypted backup, retention and
//! point-in-time recovery over object storage.
//!
//! Every object at rest is ciphertext under the deployment backup
//! key with opaque names; only ciphertext is ever uploaded. Full
//! backups snapshot every replicated database, the archive loop
//! seals committed log entries into segments while every member holds
//! its log until they are archived, and restore replays a base plus
//! segments to a time or a log index. Rotation never rewrites
//! objects; retirement is gated on live references.
//!
//! Module map:
//!
//! - [`error`]: backup failures.
//! - [`object`]: opaque naming, the backup key, compression and part
//!   sizes.
//! - [`manifest`]: backup manifests and log segments.
//! - `transfer`: sealed objects streamed up in parts and down frame by
//!   frame.
//! - [`driver`]: the backup driver.

pub mod driver;
pub mod error;
pub mod manifest;
pub mod object;
mod transfer;

pub use driver::{
    ArchiveSummary, BackupSummary, ContinuousSettings, Driver, GcSummary, IndexRestoreSummary,
    Leadership, ListedBackup, PitrSummary, ReencryptSummary, RestoreSummary, RetentionSummary,
    RuntimeBuilder,
};
pub use error::BackupError;
