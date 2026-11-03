//! Operator views of the engine: the modules each database runs, and
//! the staging rows deployments leave.
//!
//! Everything here reads the published catalog and one store snapshot
//! on the read side, so operators inspect a member without taking the
//! writer.

use std::collections::BTreeMap;

use flower_sdk::Failure;

use super::{EngineReader, StagingStatus};
use crate::descriptors::DefKind;
use crate::error::{Error, HostResult};
use crate::retry::HASH_LEN;
use crate::staging::{StagingPhase, code_identity_key, decode_state, staging_key};
use crate::storage::ReadTxn;

/// One active module as operators see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleSummary {
    /// Module name.
    pub name: String,
    /// The active code identity, if recorded.
    pub identity: Option<[u8; HASH_LEN]>,
    /// Definition name to kind: `query`, `mutation`, `derived` or
    /// `transaction`.
    pub definitions: BTreeMap<String, &'static str>,
    /// Collection names.
    pub collections: Vec<String>,
    /// HTTP alias to the definition it calls.
    pub http: BTreeMap<String, String>,
    /// Whether the module declares an authorization hook.
    pub authorize: bool,
    /// Its maintenance tasks with their failure states.
    pub tasks: Vec<crate::task::TaskStatus>,
    /// The module's staging row, if a deployment left one.
    pub staging: Option<StagingStatus>,
}

/// A staged target's code, for its activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedCode {
    /// Target code identity.
    pub identity: [u8; HASH_LEN],
    /// Whether the backfill finished.
    pub ready: bool,
    /// Wasm bundle bytes.
    pub wasm: Vec<u8>,
    /// Canonical manifest JSON bytes.
    pub manifest: Vec<u8>,
}

/// A definition kind's operator name.
fn kind_name(kind: DefKind) -> &'static str {
    match kind {
        DefKind::Query => "query",
        DefKind::Mutation => "mutation",
        DefKind::Derived => "derived",
        DefKind::Transaction => "transaction",
    }
}

/// A module's staging row in `read`, if any.
pub(super) fn staging_in(
    read: &ReadTxn,
    db: &[u8],
    module: &str,
) -> HostResult<Option<StagingStatus>> {
    let Some(bytes) = read.meta(db, &staging_key(module)).map_err(Failure::from)? else {
        return Ok(None);
    };
    let state = decode_state(&bytes).map_err(Failure::from)?;
    Ok(Some(StagingStatus::of(&state)))
}

impl EngineReader {
    /// The databases with active modules, sorted.
    pub fn databases(&self) -> Vec<Vec<u8>> {
        let (catalog, _, _) = match self.capture() {
            Ok(captured) => captured,
            Err(_) => return Vec::new(),
        };
        let mut dbs: Vec<Vec<u8>> = catalog.modules.keys().cloned().collect();
        dbs.sort();
        dbs
    }

    /// The modules active in `db`, by name, from one capture.
    pub fn modules(&self, db: &[u8]) -> HostResult<Vec<ModuleSummary>> {
        let (catalog, read, _) = self.capture()?;
        let modules = catalog.modules(db);
        let mut names: Vec<&String> = modules.descriptors.keys().collect();
        names.sort();
        let mut summaries = Vec::with_capacity(names.len());
        for name in names {
            let def = &modules.descriptors[name];
            let identity = read
                .meta(db, &code_identity_key(name))
                .map_err(Failure::from)?
                .and_then(|bytes| <[u8; HASH_LEN]>::try_from(bytes.as_slice()).ok());
            summaries.push(ModuleSummary {
                name: name.clone(),
                identity,
                definitions: def
                    .def_kinds
                    .iter()
                    .map(|(name, kind)| (name.clone(), kind_name(*kind)))
                    .collect(),
                collections: def
                    .collections
                    .iter()
                    .map(|collection| collection.name.clone())
                    .collect(),
                http: def.http.clone(),
                authorize: def.authorize,
                tasks: super::tasks::task_statuses(&read, db, name, def)?,
                staging: staging_in(&read, db, name)?,
            });
        }
        Ok(summaries)
    }

    /// A module's staging row, if a deployment left one.
    pub fn staging_status(&self, db: &[u8], module: &str) -> HostResult<Option<StagingStatus>> {
        let (_, read, _) = self.capture()?;
        staging_in(&read, db, module)
    }

    /// The code a module's open staging targets, if one is building or
    /// ready: what its activation proposes.
    pub fn staged_code(&self, db: &[u8], module: &str) -> HostResult<Option<StagedCode>> {
        let (_, read, _) = self.capture()?;
        let Some(bytes) = read.meta(db, &staging_key(module)).map_err(Failure::from)? else {
            return Ok(None);
        };
        let state = decode_state(&bytes).map_err(Failure::from)?;
        if state.phase == StagingPhase::Gc {
            return Ok(None);
        }
        let (wasm, manifest) = read
            .get_code(db, &state.identity)
            .map_err(Failure::from)?
            .ok_or_else(|| Error::corrupt("a staging's code artifact is missing"))?;
        Ok(Some(StagedCode {
            identity: state.identity,
            ready: state.phase == StagingPhase::Ready,
            wasm,
            manifest,
        }))
    }
}
