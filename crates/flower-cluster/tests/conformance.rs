//! OpenRaft's storage conformance suite (`openraft::testing::Suite`)
//! against Flower's log store and state machine, as a member opens
//! them: a Raft file and a group file in a fresh directory each.

use std::sync::Arc;

use flower_cluster::logstore::LogStore;
use flower_cluster::statemachine::{RuntimeFactory, StateMachine};
use flower_cluster::types::{NodeId, TypeConfig};
use flower_engine::engine::Engine;
use flower_engine::runtime::Runtime;
use flower_engine::storage::GroupStore;
use flower_sdk::Value;
use openraft::testing::{StoreBuilder, Suite};
use openraft::{StorageError, StorageIOError};

/// The suite applies blank and membership entries only.
struct NoRuntimes;

impl RuntimeFactory for NoRuntimes {
    fn build(
        &self,
        _db: &[u8],
        module: &str,
        _: &[u8],
        _: &Value,
    ) -> Result<Box<dyn Runtime>, String> {
        Err(format!(
            "no runtime for `{module}` in the conformance suite"
        ))
    }
}

/// A fresh store pair per suite case, in a directory the guard removes.
struct Member;

/// Wrap a setup failure.
fn io<E: std::error::Error + 'static>(error: E) -> StorageError<NodeId> {
    StorageIOError::write(&error).into()
}

impl StoreBuilder<TypeConfig, LogStore, StateMachine, tempfile::TempDir> for Member {
    async fn build(
        &self,
    ) -> Result<(tempfile::TempDir, LogStore, StateMachine), StorageError<NodeId>> {
        let dir = tempfile::tempdir().map_err(io)?;
        let group = GroupStore::create(dir.path().join("group.redb")).map_err(io)?;
        let logs = LogStore::open(&dir.path().join("raft.redb"))?;
        let (commits, _) = tokio::sync::broadcast::channel(8);
        let machine = StateMachine::new(
            Arc::new(tokio::sync::Mutex::new(Engine::new(group))),
            logs.clone(),
            Arc::new(NoRuntimes),
            commits,
        );
        Ok((dir, logs, machine))
    }
}

/// Every case of the suite: initial state, votes, log reads, purge and
/// truncation, committed re-apply, apply, snapshot metadata and a
/// snapshot built on one member and installed on another.
#[test]
fn openraft_storage_conformance() {
    Suite::test_all(Member).unwrap();
}

/// A member whose log holds every purged entry for a backup that has
/// archived none: what Raft sees must not change.
struct HeldMember;

impl StoreBuilder<TypeConfig, LogStore, StateMachine, tempfile::TempDir> for HeldMember {
    async fn build(
        &self,
    ) -> Result<(tempfile::TempDir, LogStore, StateMachine), StorageError<NodeId>> {
        let (dir, logs, machine) = Member.build().await?;
        logs.hold_purges(None).await?;
        Ok((dir, logs, machine))
    }
}

/// The whole suite again with the backup's purge hold on.
#[test]
fn openraft_storage_conformance_with_a_backup_hold() {
    Suite::test_all(HeldMember).unwrap();
}
