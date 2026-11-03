//! Replicated database registry and history labels, shared by servers and devices.
use crate::{
    batch::Applied,
    error::Result,
    storage::{WriteTxn, meta},
};
pub const CLUSTER_DB: &[u8] = b"$cluster";
pub const GROUP_DB_PREFIX: &[u8] = b"group_db\0";

pub fn registry_name(db: &[u8]) -> Vec<u8> {
    let mut name = GROUP_DB_PREFIX.to_vec();
    name.extend_from_slice(db);
    name
}
pub fn register(write: &WriteTxn, db: &[u8]) -> Result<()> {
    write.set_meta(CLUSTER_DB, &registry_name(db), b"1")?;
    let incarnation = write.meta_u64(CLUSTER_DB, meta::HISTORY_INCARNATION)?;
    crate::placement::label(write, db, incarnation)
}
pub fn establish(write: &WriteTxn, incarnation: u64) -> Result<u64> {
    let mut applied = write.meta_u64(CLUSTER_DB, meta::HISTORY_INCARNATION)?;
    if applied == 0 && incarnation != 0 {
        applied = incarnation;
        write.set_meta_u64(CLUSTER_DB, meta::HISTORY_INCARNATION, applied)?;
        for (name, _) in write.scan_meta_prefix(CLUSTER_DB, GROUP_DB_PREFIX)? {
            crate::placement::label(write, &name[GROUP_DB_PREFIX.len()..], applied)?;
        }
    }
    Ok(applied)
}
pub fn register_applied(write: &WriteTxn, item: &Applied) -> Result<()> {
    match item {
        Applied::Patch { checked, .. } => register(write, &checked.patch.db),
        Applied::Multi { outcome, .. } => {
            for (db, _) in &outcome.revisions {
                register(write, db)?;
            }
            Ok(())
        }
        Applied::Retention { db, .. }
        | Applied::Placement { db, .. }
        | Applied::Purge { db, .. }
        | Applied::Staging { db, .. } => register(write, db),
        Applied::Catalog { .. } => register(write, crate::catalog::CATALOG_DB),
        Applied::Txn { .. } => Ok(()),
    }
}
