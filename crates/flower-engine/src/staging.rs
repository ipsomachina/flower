//! Staged deployment: build the next bundle while the old one serves.
//!
//! Deploying a module with a new index cannot cut over instantly: the new
//! index has no entries for pre-existing rows, so range queries would miss
//! them until every row is rewritten. Staging splits deployment into
//! phases:
//!
//! - `stage` validates the target bundle, interns its new names, stores
//!   the artifact and records a backfill plan. Traffic still serves the
//!   active bundle.
//! - `drain` builds the added index entries in bounded resumable pages
//!   while the active bundle serves. Writes during the drain maintain
//!   both the active and the added indexes (dual-write), so no write is
//!   lost between the backfill scan and activation.
//! - a replicated staging then rebuilds the derived graph under the
//!   target code (below).
//! - `activate` switches descriptors, runtime and code identity
//!   atomically. Removed indexes and collections retire to lazy
//!   garbage collection, so activation itself stays fast.
//!
//! All staging state lives in `meta` rows, never in engine memory, so a
//! restart resumes the drain, the activation and the collection exactly
//! where they stopped.
//!
//! A staging is *replicated* or *local*. A replicated one (what
//! operators stage) is a replicated job: every step is a
//! [`StagingCommand`] the log orders, which every member, device and
//! restore replay applies alike, so each holds the same staging row,
//! backfill and staged graph at every position, and a new leader
//! continues the job where it stood. Staging carries the intern bindings
//! of every name the target uses; advancing the backfill is
//! deterministic from the replicated state; graph pages carry outcomes
//! the leader computed on exactly the state they apply to (their batch
//! applies only after the entry it was prepared after); cancel and
//! collection apply alike everywhere.
//!
//! A local staging is the leader's own: a one-call deployment stages and
//! drains on the leader, and only its activation entry replicates,
//! carrying the intern bindings the staging allocated. Until then those
//! IDs are known to the staging member alone, and a staging row records
//! them ([`StagingState::local`]). A leader can lose leadership
//! mid-backfill and later apply an activation (or a replicated staging)
//! from elsewhere whose bindings claim the same IDs for other names:
//! those bindings are authoritative, so the abandoned staging gives way.
//! Its backfill under local IDs purges, the IDs release, and anything it
//! retired under replicated IDs goes to the collector. Cancelling does
//! the same.
//!
//! A replicated staging rebuilds the derived graph before it is ready
//! (the *rebuilding* phase, after the index backfill): every pinned root
//! of the module's active derived definitions evaluates again under the
//! target code, in bounded pages, into *staged definitions*
//! (`$staged\0<tag>\0<module>\0<name>`, interned with the staging), so
//! the active outcomes keep serving. Staged outcomes record their
//! dependencies like any other, so writes during the staging invalidate
//! them exactly as they invalidate active ones. Activation moves the
//! staged outcomes over the active definitions in its one transaction:
//! roots read after the cutover serve the target's values without
//! evaluating again; a staged root a write invalidated after its page
//! evaluates on its next read, as every root did before.
//!
//! Index fields are immutable per index name: staging rejects a target
//! whose index keeps its name but changes its fields. Rename the index
//! instead. Collection schemas may change freely; old rows stay readable
//! and new writes validate against the target schema, so arbitrary
//! record transformations remain explicit application migrations.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::descriptors::{DefKind, ModuleDef};
use crate::error::{Error, Result};
use crate::host::index_components;
use crate::storage::keys::{HASH_LEN, Reader, push_count, push_sized};
use crate::storage::{ReadTxn, WriteTxn};

/// Meta-name prefix for a module's staging row: `staging\0<module>`.
pub(crate) const STAGING_PREFIX: &[u8] = b"staging\x00";
/// Meta-name prefix for a module's active code identity: `code_identity\0<module>`.
pub const CODE_IDENTITY_PREFIX: &[u8] = b"code_identity\x00";
/// Meta-name prefix for a module's code era: `code_era\0<module>` (see
/// [`code_era`]).
pub const CODE_ERA_PREFIX: &[u8] = b"code_era\x00";
/// Module-name prefix of staged derived definitions: no deployable
/// module starts with `$`, so these names never collide with a
/// module's own.
pub const STAGED_DEFINITION_PREFIX: &str = "$staged";

/// Staging row tag: backfill in progress.
const TAG_BUILDING: u8 = 0x01;
/// Staging row tag: backfill complete, ready to activate.
const TAG_READY: u8 = 0x02;
/// Staging row tag: activated, retiring removed structures.
const TAG_GC: u8 = 0x03;
/// Staging row phase (extended rows only): rebuilding the staged graph.
const TAG_REBUILDING: u8 = 0x04;
/// First byte of an extended staging row: a phase byte, flags, the
/// legacy body, then the staged graph. Rows a one-call deployment
/// leaves keep the legacy encoding, which starts with the phase tag.
const TAG_EXTENDED: u8 = 0x10;
/// Extended-row flag: the staging replicates (see the module docs).
const FLAG_REPLICATED: u8 = 0x01;

/// Retired entry tag: index entries.
const RETIRED_INDEX: u8 = 0x00;
/// Retired entry tag: collection records.
const RETIRED_COLLECTION: u8 = 0x01;
/// Retired entry tag: derived outcomes of one (staged) definition.
const RETIRED_DERIVED: u8 = 0x02;

/// Meta name for a module's staging row.
pub fn staging_key(module: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(STAGING_PREFIX.len() + module.len());
    key.extend_from_slice(STAGING_PREFIX);
    key.extend_from_slice(module.as_bytes());
    key
}

/// Every staging row of one database not yet activated, by
/// module, from `(name, value)` meta rows below [`staging_key`]'s
/// prefix. Collection-only rows are left out.
pub(crate) fn open_stagings(rows: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Vec<(String, StagingState)>> {
    let mut open = Vec::new();
    for (name, bytes) in rows {
        let state = decode_state(&bytes)?;
        if state.phase == StagingPhase::Gc {
            continue;
        }
        let module = String::from_utf8(name[STAGING_PREFIX.len()..].to_vec())
            .map_err(|_| Error::corrupt("staged module name is not UTF-8".to_string()))?;
        open.push((module, state));
    }
    Ok(open)
}

/// The modules of one database whose staging rows hold retired
/// structures awaiting collection, as `read` sees them, by name.
pub fn retiring_modules(read: &ReadTxn, db: &[u8]) -> Result<Vec<String>> {
    let mut modules = Vec::new();
    for (name, bytes) in read.scan_meta_prefix(db, STAGING_PREFIX)? {
        if decode_state(&bytes)?.retired.is_empty() {
            continue;
        }
        modules.push(
            String::from_utf8(name[STAGING_PREFIX.len()..].to_vec())
                .map_err(|_| Error::corrupt("staged module name is not UTF-8".to_string()))?,
        );
    }
    Ok(modules)
}

/// Meta name for a module's active code identity.
pub fn code_identity_key(module: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(CODE_IDENTITY_PREFIX.len() + module.len());
    key.extend_from_slice(CODE_IDENTITY_PREFIX);
    key.extend_from_slice(module.as_bytes());
    key
}

/// Meta name for a module's code era: the code identity its derived
/// outcomes carry, minted afresh by every activation.
pub fn code_era_key(module: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(CODE_ERA_PREFIX.len() + module.len());
    key.extend_from_slice(CODE_ERA_PREFIX);
    key.extend_from_slice(module.as_bytes());
    key
}

/// The code identity the `minted`-th activation of a database stamps on
/// its module's derived outcomes: a domain-separated hash, so it never
/// equals a module's content identity or an aggregate lineage. The same
/// code activated again (A, then B, then A) gets a new era: what A
/// cached the first time read structures B retired, collected or left
/// unmaintained, and what A's backfill and purges rewrote since
/// invalidated nothing.
pub fn code_era(minted: u64) -> [u8; HASH_LEN] {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"flower code era\0");
    hasher.update(minted.to_be_bytes());
    hasher.finalize().into()
}

/// Meta name prefix of the aggregate lineages: per versioned aggregate,
/// the code identity minted when its current version began ([`aggregate_lineage`]).
pub const AGGREGATE_LINEAGE_PREFIX: &[u8] = b"aggregate_lineage\x00";

/// Meta name for one versioned aggregate's lineage. Its cached values
/// carry the lineage as their code identity, so they outlive every
/// activation that keeps the aggregate's version, source and index
/// fields, and no value from an earlier run of the same version (one
/// removed and declared again, say) ever serves.
pub fn aggregate_lineage_key(module: &str, name: &str) -> Vec<u8> {
    let mut key =
        Vec::with_capacity(AGGREGATE_LINEAGE_PREFIX.len() + module.len() + name.len() + 1);
    key.extend_from_slice(AGGREGATE_LINEAGE_PREFIX);
    key.extend_from_slice(module.as_bytes());
    key.push(0);
    key.extend_from_slice(name.as_bytes());
    key
}

/// The code identity the `begun`-th aggregate lineage of a database
/// stamps on its values: a domain-separated hash, so it never equals a
/// module's code identity.
pub fn aggregate_lineage(begun: u64) -> [u8; HASH_LEN] {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"flower aggregate lineage\0");
    hasher.update(begun.to_be_bytes());
    hasher.finalize().into()
}

/// Every module's active code identity in one database, sorted by
/// module: what a patch pins at prepare and what an applier compares.
pub fn deployed_versions(
    read: &crate::storage::ReadTxn,
    db: &[u8],
) -> Result<Vec<(String, [u8; HASH_LEN])>> {
    let mut versions = Vec::new();
    for (name, identity) in read.scan_meta_prefix(db, CODE_IDENTITY_PREFIX)? {
        let module = String::from_utf8(name[CODE_IDENTITY_PREFIX.len()..].to_vec())
            .map_err(|_| Error::corrupt("deployed module name is not UTF-8".to_string()))?;
        let identity: [u8; HASH_LEN] = identity
            .try_into()
            .map_err(|_| Error::corrupt("code identity has the wrong length".to_string()))?;
        versions.push((module, identity));
    }
    versions.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(versions)
}

/// One added index to backfill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddedIndex {
    /// Index name.
    pub name: String,
    /// Interned index ID.
    pub id: u32,
    /// Indexed wire fields in order.
    pub fields: Vec<String>,
}

/// Backfill work for one collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionPlan {
    /// Collection name.
    pub name: String,
    /// Interned collection ID.
    pub id: u32,
    /// Added indexes to build over its rows.
    pub added: Vec<AddedIndex>,
}

/// What a retired structure holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetiredKind {
    /// Index entries.
    Index,
    /// Collection records.
    Collection,
    /// Derived outcomes (and their edges) of one definition: a staged
    /// graph a cancelled or superseded staging left.
    Derived,
}

/// One retired structure awaiting collection, with a resume cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retired {
    /// What the structure holds.
    pub kind: RetiredKind,
    /// Interned collection, index or definition ID.
    pub id: u32,
    /// Last collected key (full key for records and index entries,
    /// argument bytes for outcomes), empty to start at the prefix.
    pub cursor: Vec<u8>,
}

impl Retired {
    /// A retired structure collection starts at the beginning of.
    pub fn new(kind: RetiredKind, id: u32) -> Self {
        Retired {
            kind,
            id,
            cursor: Vec::new(),
        }
    }
}

/// Staging phase: building the backfill, rebuilding the staged graph,
/// ready to activate, or only retiring removed structures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StagingPhase {
    /// Backfill in progress; the active bundle serves.
    Building,
    /// Backfill complete; the staged graph rebuilds (replicated
    /// stagings only).
    Rebuilding,
    /// Backfill (and graph) complete; activation swaps the bundles.
    Ready,
    /// Activated; only garbage collection remains.
    Gc,
}

impl StagingPhase {
    /// The phase's operator name.
    pub fn name(self) -> &'static str {
        match self {
            StagingPhase::Building => "building",
            StagingPhase::Rebuilding => "rebuilding",
            StagingPhase::Ready => "ready",
            StagingPhase::Gc => "collecting",
        }
    }
}

/// One derived definition of a replicated staging's target: its active
/// definition and the staged twin its graph rebuilds into.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphDef {
    /// Definition name within the module.
    pub name: String,
    /// The definition's own interned ID, which serves.
    pub active: u32,
    /// The staged definition's interned ID.
    pub staged: u32,
    /// Whether the active module has the definition as derived, so its
    /// pinned roots rebuild.
    pub walk: bool,
}

/// Durable staging state for one module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagingState {
    /// Target code identity bytes.
    pub identity: [u8; HASH_LEN],
    /// Current phase.
    pub phase: StagingPhase,
    /// Backfill plan in drain order.
    pub plan: Vec<CollectionPlan>,
    /// Plan position of the drain cursor.
    pub plan_pos: usize,
    /// Last backfilled record key within the cursor collection.
    pub key_cursor: Vec<u8>,
    /// Retired structures awaiting collection.
    pub retired: Vec<Retired>,
    /// Intern counter before staging interned its names, for
    /// activation bindings.
    pub counter_before: u32,
    /// Intern IDs this staging allocated that no applied activation
    /// has bound yet: names only this member knows, which a replicated
    /// activation staged elsewhere may claim for other names. Empty on
    /// members that never staged, once an activation carries them, and
    /// always for a replicated staging.
    pub local: Vec<u32>,
    /// Whether the staging replicates: every member holds it alike (see
    /// the module docs).
    pub replicated: bool,
    /// The target's derived definitions with their staged twins, in name
    /// order: what the graph phase rebuilds and activation moves.
    pub graph: Vec<GraphDef>,
    /// Graph position of the rebuild cursor.
    pub graph_pos: usize,
    /// Last rebuilt root's argument bytes within the cursor definition.
    pub graph_cursor: Vec<u8>,
    /// Roots the graph phase rebuilt so far.
    pub rebuilt: u64,
}

impl StagingState {
    /// Whether the row needs the extended encoding: rows a one-call
    /// deployment leaves keep the legacy one.
    fn extended(&self) -> bool {
        self.replicated
            || self.phase == StagingPhase::Rebuilding
            || !self.graph.is_empty()
            || self
                .retired
                .iter()
                .any(|retired| retired.kind == RetiredKind::Derived)
    }

    /// The phase the backfill ends in: rebuilding while a staged graph
    /// has roots to walk, else ready.
    pub(crate) fn after_backfill(&self) -> StagingPhase {
        if self.graph_pos < self.graph.len() && self.graph.iter().any(|def| def.walk) {
            StagingPhase::Rebuilding
        } else {
            StagingPhase::Ready
        }
    }

    /// A collection-only row retiring `retired`.
    pub(crate) fn collecting(
        identity: [u8; HASH_LEN],
        retired: Vec<Retired>,
        replicated: bool,
    ) -> Self {
        StagingState {
            identity,
            phase: StagingPhase::Gc,
            plan: Vec::new(),
            plan_pos: 0,
            key_cursor: Vec::new(),
            retired,
            counter_before: 0,
            local: Vec::new(),
            replicated,
            graph: Vec::new(),
            graph_pos: 0,
            graph_cursor: Vec::new(),
            rebuilt: 0,
        }
    }
}

/// The name a staged twin of `module`'s derived definition `name`
/// interns under: tagged with the intern counter its staging began at,
/// so every staging's twins are fresh names whatever an earlier
/// staging's collection still holds.
pub fn staged_definition(tag: u32, module: &str, name: &str) -> String {
    format!("{STAGED_DEFINITION_PREFIX}\0{tag}\0{module}\0{name}")
}

/// Every name a staging of `target` as `module` interns, in the order it
/// interns them, so staging the same bundle from the same intern state
/// allocates the same IDs on any member: collections and their indexes
/// in manifest order, definitions sorted, the shared collections, then
/// (replicated stagings, `tag` given) the staged twins of its derived
/// definitions, sorted.
pub fn target_names(module: &str, target: &ModuleDef, tag: Option<u32>) -> Vec<Vec<u8>> {
    let mut names = Vec::new();
    for collection in &target.collections {
        names.push(crate::host::qualify_collection(module, &collection.name).into_bytes());
        for index in &collection.indexes {
            names.push(
                crate::host::qualify_index(module, &collection.name, &index.name).into_bytes(),
            );
        }
    }
    let mut definitions: Vec<&String> = target.def_kinds.keys().collect();
    definitions.sort();
    for name in &definitions {
        names.push(crate::host::qualify_definition(module, name).into_bytes());
    }
    names.push(crate::host::RAW_COLLECTION.to_vec());
    names.push(crate::host::QUEUE_COLLECTION.to_vec());
    names.push(crate::host::TIMER_COLLECTION.to_vec());
    // Only a module with tasks interns their state collection, so other
    // deployments allocate the same names as before.
    if !target.tasks.is_empty() {
        names.push(crate::host::TASK_COLLECTION.to_vec());
    }
    if let Some(tag) = tag {
        for name in definitions {
            if target.def_kinds.get(name) == Some(&DefKind::Derived) {
                names.push(staged_definition(tag, module, name).into_bytes());
            }
        }
    }
    names
}

/// Where a replicated staging's graph rebuild stands: a definition
/// position and the last rebuilt root's argument bytes within it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GraphCursor {
    /// Position in the staging's graph definitions.
    pub position: u32,
    /// Last rebuilt root's argument bytes, empty at a definition's start.
    pub args: Vec<u8>,
}

/// One staged outcome a graph page installs, with its edge
/// replacement (`None` keeps the stored edges).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GraphOutcome {
    /// Staged definition ID.
    pub definition: u32,
    /// Canonical argument bytes.
    pub args: Vec<u8>,
    /// Encoded outcome.
    pub outcome: Vec<u8>,
    /// Observed dependency keys, replacing the stored edges.
    pub observed: Option<Vec<Vec<u8>>>,
}

/// One step of a replicated staging, a batch item every member, device
/// and restore replay applies alike (see the module docs). A step that
/// does not fit the staging it names (another target, a moved cursor)
/// is refused, changing nothing, alike everywhere.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StagingCommand {
    /// Stage a target without activating it: bind every name it uses,
    /// store its code and plan its backfill and graph. Abandons the
    /// module's open staging of another target first.
    Stage {
        /// Module name.
        module: String,
        /// Target code identity.
        identity: [u8; HASH_LEN],
        /// Wasm bundle bytes.
        wasm: Vec<u8>,
        /// Canonical manifest JSON bytes.
        manifest: Vec<u8>,
        /// Authoritative `(name, ID)` binding of every name the target
        /// uses, in interning order.
        bindings: Vec<(Vec<u8>, u32)>,
        /// The intern counter the leader staged at: the staged twins'
        /// tag and the activation bindings' floor.
        counter_before: u32,
    },
    /// Build up to `records` more of the index backfill, from the
    /// replicated cursor.
    Advance {
        /// Module name.
        module: String,
        /// The staging's target identity.
        identity: [u8; HASH_LEN],
        /// Most records to scan.
        records: u32,
    },
    /// Install one graph page the leader evaluated on the state this
    /// item applies to.
    Graph {
        /// Module name.
        module: String,
        /// The staging's target identity.
        identity: [u8; HASH_LEN],
        /// The rebuild cursor the page started at.
        from: GraphCursor,
        /// The rebuild cursor after the page.
        to: GraphCursor,
        /// Whether the page finished the graph.
        done: bool,
        /// Roots the page rebuilt.
        roots: u32,
        /// The staged outcomes it computed.
        outcomes: Vec<GraphOutcome>,
    },
    /// Abandon the staging: its backfill and staged graph retire.
    Cancel {
        /// Module name.
        module: String,
        /// The staging's target identity.
        identity: [u8; HASH_LEN],
    },
    /// Collect up to `keys` of what the module's deployments retired.
    Collect {
        /// Module name.
        module: String,
        /// Most keys to delete.
        keys: u32,
    },
}

impl StagingCommand {
    /// The module the step names.
    pub fn module(&self) -> &str {
        match self {
            StagingCommand::Stage { module, .. }
            | StagingCommand::Advance { module, .. }
            | StagingCommand::Graph { module, .. }
            | StagingCommand::Cancel { module, .. }
            | StagingCommand::Collect { module, .. } => module,
        }
    }

    /// Encoded bytes the step carries, roughly.
    pub fn len(&self) -> usize {
        match self {
            StagingCommand::Stage {
                module,
                wasm,
                manifest,
                bindings,
                ..
            } => {
                module.len()
                    + wasm.len()
                    + manifest.len()
                    + bindings
                        .iter()
                        .map(|(name, _)| name.len() + 8)
                        .sum::<usize>()
                    + 64
            }
            StagingCommand::Graph {
                module,
                from,
                to,
                outcomes,
                ..
            } => {
                module.len()
                    + from.args.len()
                    + to.args.len()
                    + outcomes
                        .iter()
                        .map(|outcome| {
                            outcome.args.len()
                                + outcome.outcome.len()
                                + outcome
                                    .observed
                                    .iter()
                                    .flatten()
                                    .map(|edge| edge.len() + 8)
                                    .sum::<usize>()
                                + 16
                        })
                        .sum::<usize>()
                    + 64
            }
            StagingCommand::Advance { module, .. }
            | StagingCommand::Cancel { module, .. }
            | StagingCommand::Collect { module, .. } => module.len() + 48,
        }
    }

    /// Whether the step carries no bytes; never.
    pub fn is_empty(&self) -> bool {
        false
    }
}

/// Diff the active and target collection sets into a backfill plan plus
/// removals. Index IDs resolve through `intern_id`: every name the plan
/// or the removals reference must already be interned.
pub fn diff_plan(
    active: Option<&ModuleDef>,
    target: &ModuleDef,
    intern_id: &dyn Fn(&str) -> Result<Option<u32>>,
    qualify_collection: &dyn Fn(&str) -> String,
    qualify_index: &dyn Fn(&str, &str) -> String,
) -> Result<(Vec<CollectionPlan>, Vec<Retired>)> {
    let mut plan = Vec::new();
    let mut retired = Vec::new();
    for collection in &target.collections {
        let active_collection = active.and_then(|defs| defs.collection(&collection.name));
        let id = intern_id(&qualify_collection(&collection.name))?
            .ok_or_else(|| Error::corrupt("staged collection is not interned".to_string()))?;
        let mut added = Vec::new();
        for index in &collection.indexes {
            let active_index = active_collection
                .and_then(|active| active.indexes.iter().find(|known| known.name == index.name));
            match active_index {
                None => {
                    let index_id = intern_id(&qualify_index(&collection.name, &index.name))?
                        .ok_or_else(|| {
                            Error::corrupt("staged index is not interned".to_string())
                        })?;
                    added.push(AddedIndex {
                        name: index.name.clone(),
                        id: index_id,
                        fields: index.fields.clone(),
                    });
                }
                Some(known) if known.fields != index.fields => {
                    return Err(Error::Manifest(format!(
                        "index `{}` changed its fields; rename the index instead",
                        index.name
                    )));
                }
                Some(_) => {}
            }
        }
        if !added.is_empty() {
            plan.push(CollectionPlan {
                name: collection.name.clone(),
                id,
                added,
            });
        }
    }
    if let Some(active) = active {
        for collection in &active.collections {
            match target.collection(&collection.name) {
                None => {
                    let id =
                        intern_id(&qualify_collection(&collection.name))?.ok_or_else(|| {
                            Error::corrupt("retired collection is not interned".to_string())
                        })?;
                    for index in &collection.indexes {
                        let index_id = intern_id(&qualify_index(&collection.name, &index.name))?
                            .ok_or_else(|| {
                                Error::corrupt("retired index is not interned".to_string())
                            })?;
                        retired.push(Retired::new(RetiredKind::Index, index_id));
                    }
                    retired.push(Retired::new(RetiredKind::Collection, id));
                }
                Some(next) => {
                    for index in &collection.indexes {
                        if next.indexes.iter().any(|kept| kept.name == index.name) {
                            continue;
                        }
                        let index_id = intern_id(&qualify_index(&collection.name, &index.name))?
                            .ok_or_else(|| {
                                Error::corrupt("retired index is not interned".to_string())
                            })?;
                        retired.push(Retired::new(RetiredKind::Index, index_id));
                    }
                }
            }
        }
    }
    Ok((plan, retired))
}

/// Encode staging state: tag, identity, counter, plan, cursor, retired;
/// extended rows (replicated stagings, staged graphs) lead with
/// [`TAG_EXTENDED`], the phase and flags, and end with the graph.
pub fn encode_state(state: &StagingState) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    let tag = match state.phase {
        StagingPhase::Building => TAG_BUILDING,
        StagingPhase::Rebuilding => TAG_REBUILDING,
        StagingPhase::Ready => TAG_READY,
        StagingPhase::Gc => TAG_GC,
    };
    let extended = state.extended();
    if extended {
        encoded.push(TAG_EXTENDED);
        encoded.push(tag);
        encoded.push(if state.replicated { FLAG_REPLICATED } else { 0 });
    } else {
        encoded.push(tag);
    }
    encoded.extend_from_slice(&state.identity);
    encoded.extend_from_slice(&state.counter_before.to_be_bytes());
    push_count(&mut encoded, state.local.len())?;
    for id in &state.local {
        encoded.extend_from_slice(&id.to_be_bytes());
    }
    push_count(&mut encoded, state.plan.len())?;
    for collection in &state.plan {
        push_sized(&mut encoded, collection.name.as_bytes())?;
        encoded.extend_from_slice(&collection.id.to_be_bytes());
        push_count(&mut encoded, collection.added.len())?;
        for index in &collection.added {
            push_sized(&mut encoded, index.name.as_bytes())?;
            encoded.extend_from_slice(&index.id.to_be_bytes());
            push_count(&mut encoded, index.fields.len())?;
            for field in &index.fields {
                push_sized(&mut encoded, field.as_bytes())?;
            }
        }
    }
    push_count(&mut encoded, state.plan_pos)?;
    push_sized(&mut encoded, &state.key_cursor)?;
    push_count(&mut encoded, state.retired.len())?;
    for retired in &state.retired {
        encoded.push(match retired.kind {
            RetiredKind::Index => RETIRED_INDEX,
            RetiredKind::Collection => RETIRED_COLLECTION,
            RetiredKind::Derived => RETIRED_DERIVED,
        });
        encoded.extend_from_slice(&retired.id.to_be_bytes());
        push_sized(&mut encoded, &retired.cursor)?;
    }
    if extended {
        push_count(&mut encoded, state.graph.len())?;
        for def in &state.graph {
            push_sized(&mut encoded, def.name.as_bytes())?;
            encoded.extend_from_slice(&def.active.to_be_bytes());
            encoded.extend_from_slice(&def.staged.to_be_bytes());
            encoded.push(u8::from(def.walk));
        }
        push_count(&mut encoded, state.graph_pos)?;
        push_sized(&mut encoded, &state.graph_cursor)?;
        encoded.extend_from_slice(&state.rebuilt.to_be_bytes());
    }
    Ok(encoded)
}

/// Decode staging state, rejecting unknown tags and trailing bytes.
pub fn decode_state(bytes: &[u8]) -> Result<StagingState> {
    let mut reader = Reader::new(bytes);
    let mut tag = reader.byte()?;
    let mut replicated = false;
    let extended = tag == TAG_EXTENDED;
    if extended {
        tag = reader.byte()?;
        let flags = reader.byte()?;
        if flags & !FLAG_REPLICATED != 0 {
            return Err(Error::corrupt("unknown staging flags".to_string()));
        }
        replicated = flags & FLAG_REPLICATED != 0;
    }
    let phase = match tag {
        TAG_BUILDING => StagingPhase::Building,
        TAG_READY => StagingPhase::Ready,
        TAG_GC => StagingPhase::Gc,
        TAG_REBUILDING if extended => StagingPhase::Rebuilding,
        _ => return Err(Error::corrupt("unknown staging tag".to_string())),
    };
    let identity: [u8; HASH_LEN] = reader
        .bytes(HASH_LEN)?
        .try_into()
        .map_err(|_| Error::corrupt("truncated staging identity".to_string()))?;
    let counter_before = reader.u32()?;
    let locals = reader.u32()? as usize;
    let mut local = Vec::with_capacity(locals.min(1024));
    for _ in 0..locals {
        local.push(reader.u32()?);
    }
    let plans = reader.u32()? as usize;
    let mut plan = Vec::with_capacity(plans.min(1024));
    for _ in 0..plans {
        let name = String::from_utf8(reader.sized()?)
            .map_err(|_| Error::corrupt("staging collection name is not UTF-8".to_string()))?;
        let id = reader.u32()?;
        let added_count = reader.u32()? as usize;
        let mut added = Vec::with_capacity(added_count.min(1024));
        for _ in 0..added_count {
            let name = String::from_utf8(reader.sized()?)
                .map_err(|_| Error::corrupt("staging index name is not UTF-8".to_string()))?;
            let id = reader.u32()?;
            let fields_count = reader.u32()? as usize;
            let mut fields = Vec::with_capacity(fields_count.min(1024));
            for _ in 0..fields_count {
                fields.push(
                    String::from_utf8(reader.sized()?).map_err(|_| {
                        Error::corrupt("staging index field is not UTF-8".to_string())
                    })?,
                );
            }
            added.push(AddedIndex { name, id, fields });
        }
        plan.push(CollectionPlan { name, id, added });
    }
    let plan_pos = reader.u32()? as usize;
    let key_cursor = reader.sized()?;
    let retired_count = reader.u32()? as usize;
    let mut retired = Vec::with_capacity(retired_count.min(1024));
    for _ in 0..retired_count {
        let kind = match reader.byte()? {
            RETIRED_INDEX => RetiredKind::Index,
            RETIRED_COLLECTION => RetiredKind::Collection,
            RETIRED_DERIVED if extended => RetiredKind::Derived,
            _ => return Err(Error::corrupt("unknown retired tag".to_string())),
        };
        retired.push(Retired {
            kind,
            id: reader.u32()?,
            cursor: reader.sized()?,
        });
    }
    let mut graph = Vec::new();
    let mut graph_pos = 0;
    let mut graph_cursor = Vec::new();
    let mut rebuilt = 0;
    if extended {
        let count = reader.u32()? as usize;
        graph.reserve(count.min(1024));
        for _ in 0..count {
            let name = String::from_utf8(reader.sized()?)
                .map_err(|_| Error::corrupt("staged definition name is not UTF-8".to_string()))?;
            let active = reader.u32()?;
            let staged = reader.u32()?;
            let walk = match reader.byte()? {
                0 => false,
                1 => true,
                _ => return Err(Error::corrupt("bad staged definition flag".to_string())),
            };
            graph.push(GraphDef {
                name,
                active,
                staged,
                walk,
            });
        }
        graph_pos = reader.u32()? as usize;
        graph_cursor = reader.sized()?;
        rebuilt = reader.u64()?;
    }
    if reader.pos() != bytes.len() {
        return Err(Error::corrupt("trailing staging bytes".to_string()));
    }
    Ok(StagingState {
        identity,
        phase,
        plan,
        plan_pos,
        key_cursor,
        retired,
        counter_before,
        local,
        replicated,
        graph,
        graph_pos,
        graph_cursor,
        rebuilt,
    })
}

/// This node's staged indexes in one database, by collection ID: the
/// added indexes of every staging not yet activated, which record
/// writes maintain as they apply (dual-write).
///
/// A local staging lives on its node alone: the staging node backfills
/// ahead of time, and every other member builds the same indexes inside
/// the activation entry. A replicated one lives on every member alike.
/// Either way dual-write runs where a write applies, against the stored
/// old image, and never travels inside a patch: a patch replicates to
/// members with no local staging, and may have been prepared before the
/// staging began. Rows the backfill scan already passed stay current
/// this way; rows it has yet to reach it rewrites when it gets there.
pub(crate) struct StagedIndexes(HashMap<u32, Vec<AddedIndex>>);

impl StagedIndexes {
    /// Load the staged indexes from inside a write transaction.
    pub(crate) fn load(write: &WriteTxn, db: &[u8]) -> Result<Self> {
        let mut added: HashMap<u32, Vec<AddedIndex>> = HashMap::new();
        for (_, bytes) in write.scan_meta_prefix(db, STAGING_PREFIX)? {
            let state = decode_state(&bytes)?;
            // Once activated, the indexes are active and patches
            // maintain them like any other.
            if state.phase == StagingPhase::Gc {
                continue;
            }
            for collection in state.plan {
                added
                    .entry(collection.id)
                    .or_default()
                    .extend(collection.added);
            }
        }
        Ok(StagedIndexes(added))
    }

    /// Maintain the staged indexes for one record write about to apply
    /// in `write`: drop entries derived from the stored image, add
    /// entries for the new one. Call before the record itself applies.
    pub(crate) fn maintain(
        &self,
        write: &WriteTxn,
        db: &[u8],
        collection: u32,
        key: &[u8],
        new: Option<&[u8]>,
    ) -> Result<()> {
        let Some(indexes) = self.0.get(&collection) else {
            return Ok(());
        };
        let old = write.get_record(db, collection, key)?;
        for index in indexes {
            if let Some(old) = &old
                && let Some(components) = index_components(old, &index.fields)?
            {
                let refs: Vec<&[u8]> = components.iter().map(Vec::as_slice).collect();
                write.delete_index_entry(db, index.id, &refs, key)?;
            }
            if let Some(new) = new
                && let Some(components) = index_components(new, &index.fields)?
            {
                let refs: Vec<&[u8]> = components.iter().map(Vec::as_slice).collect();
                write.put_index_entry(db, index.id, &refs, key)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptors::load_module;
    use flower_sdk::Value;

    fn collection(name: &str, indexes: Vec<(&str, Vec<&str>)>) -> Value {
        let mut map = std::collections::BTreeMap::new();
        map.insert("name".to_string(), Value::String(name.to_string()));
        map.insert(
            "key".to_string(),
            Value::Object(std::collections::BTreeMap::from([(
                "type".to_string(),
                Value::String("string".to_string()),
            )])),
        );
        map.insert(
            "value".to_string(),
            Value::Object(std::collections::BTreeMap::from([(
                "type".to_string(),
                Value::String("json".to_string()),
            )])),
        );
        map.insert(
            "indexes".to_string(),
            Value::Array(
                indexes
                    .into_iter()
                    .map(|(name, fields)| {
                        let mut index = std::collections::BTreeMap::new();
                        index.insert("name".to_string(), Value::String(name.to_string()));
                        index.insert(
                            "fields".to_string(),
                            Value::Array(
                                fields
                                    .into_iter()
                                    .map(|field| Value::String(field.to_string()))
                                    .collect(),
                            ),
                        );
                        Value::Object(index)
                    })
                    .collect(),
            ),
        );
        map.insert("references".to_string(), Value::Array(Vec::new()));
        Value::Object(map)
    }

    fn manifest(collections: Vec<Value>) -> Value {
        let mut map = std::collections::BTreeMap::new();
        map.insert("collections".to_string(), Value::Array(collections));
        map.insert("definitions".to_string(), Value::Array(Vec::new()));
        map.insert("triggers".to_string(), Value::Array(Vec::new()));
        Value::Object(map)
    }

    fn ids(names: &[&str]) -> HashMap<String, u32> {
        names
            .iter()
            .enumerate()
            .map(|(index, name)| ((*name).to_string(), index as u32 + 1))
            .collect()
    }

    #[test]
    fn plan_diffs_added_and_removed() {
        let active = load_module(&manifest(vec![collection(
            "tasks",
            vec![("by_owner", vec!["owner"]), ("by_title", vec!["title"])],
        )]))
        .unwrap();
        let target = load_module(&manifest(vec![collection(
            "tasks",
            vec![("by_owner", vec!["owner"]), ("by_tag", vec!["tag"])],
        )]))
        .unwrap();
        let known = ids(&[
            "app\x00tasks",
            "app\x00tasks\x00by_tag",
            "app\x00tasks\x00by_title",
        ]);
        let (plan, retired) = diff_plan(
            Some(&active),
            &target,
            &|name| Ok(known.get(name).copied()),
            &|collection| format!("app\x00{collection}"),
            &|collection, index| format!("app\x00{collection}\x00{index}"),
        )
        .unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].name, "tasks");
        assert_eq!(plan[0].added.len(), 1);
        assert_eq!(plan[0].added[0].name, "by_tag");
        assert_eq!(plan[0].added[0].fields, vec!["tag".to_string()]);
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].kind, RetiredKind::Index);
    }

    #[test]
    fn plan_rejects_changed_index_fields() {
        let active = load_module(&manifest(vec![collection(
            "tasks",
            vec![("by_owner", vec!["owner"])],
        )]))
        .unwrap();
        let target = load_module(&manifest(vec![collection(
            "tasks",
            vec![("by_owner", vec!["owner", "tag"])],
        )]))
        .unwrap();
        let known = ids(&["app\x00tasks", "app\x00tasks\x00by_owner"]);
        let error = diff_plan(
            Some(&active),
            &target,
            &|name| Ok(known.get(name).copied()),
            &|collection| format!("app\x00{collection}"),
            &|collection, index| format!("app\x00{collection}\x00{index}"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("changed its fields"), "{error}");
    }

    #[test]
    fn state_round_trips() {
        let state = StagingState {
            identity: [9u8; HASH_LEN],
            phase: StagingPhase::Building,
            plan: vec![CollectionPlan {
                name: "tasks".to_string(),
                id: 3,
                added: vec![AddedIndex {
                    name: "by_tag".to_string(),
                    id: 7,
                    fields: vec!["tag".to_string()],
                }],
            }],
            plan_pos: 0,
            key_cursor: vec![1, 2, 3],
            retired: vec![Retired::new(RetiredKind::Index, 5)],
            counter_before: 11,
            local: vec![12, 14],
            replicated: false,
            graph: Vec::new(),
            graph_pos: 0,
            graph_cursor: Vec::new(),
            rebuilt: 0,
        };
        let encoded = encode_state(&state).unwrap();
        // A one-call deployment's row keeps the legacy encoding.
        assert_eq!(encoded[0], TAG_BUILDING);
        let decoded = decode_state(&encoded).unwrap();
        assert_eq!(decoded, state);
        assert_eq!(decoded.phase, StagingPhase::Building);
    }

    #[test]
    fn replicated_state_round_trips_with_its_graph() {
        let state = StagingState {
            identity: [3u8; HASH_LEN],
            phase: StagingPhase::Rebuilding,
            plan: Vec::new(),
            plan_pos: 0,
            key_cursor: Vec::new(),
            retired: vec![
                Retired::new(RetiredKind::Collection, 4),
                Retired {
                    kind: RetiredKind::Derived,
                    id: 9,
                    cursor: b"args".to_vec(),
                },
            ],
            counter_before: 20,
            local: Vec::new(),
            replicated: true,
            graph: vec![GraphDef {
                name: "total".to_string(),
                active: 6,
                staged: 21,
                walk: true,
            }],
            graph_pos: 0,
            graph_cursor: b"{\"key\":1}".to_vec(),
            rebuilt: 7,
        };
        let encoded = encode_state(&state).unwrap();
        assert_eq!(encoded[0], TAG_EXTENDED);
        assert_eq!(decode_state(&encoded).unwrap(), state);
        // Legacy rows never carry the extended phase or kinds.
        let mut legacy = encoded[2..].to_vec();
        legacy[0] = TAG_REBUILDING;
        assert!(decode_state(&legacy).is_err());
    }

    #[test]
    fn target_names_list_staged_twins_only_when_tagged() {
        let mut map = std::collections::BTreeMap::new();
        map.insert("collections".to_string(), Value::Array(Vec::new()));
        map.insert(
            "definitions".to_string(),
            Value::Array(vec![
                Value::Object(std::collections::BTreeMap::from([
                    ("name".to_string(), Value::String("total".to_string())),
                    ("kind".to_string(), Value::String("derived".to_string())),
                    (
                        "args".to_string(),
                        Value::Object(std::collections::BTreeMap::from([(
                            "type".to_string(),
                            Value::String("json".to_string()),
                        )])),
                    ),
                ])),
                Value::Object(std::collections::BTreeMap::from([
                    ("name".to_string(), Value::String("put".to_string())),
                    ("kind".to_string(), Value::String("mutation".to_string())),
                    (
                        "args".to_string(),
                        Value::Object(std::collections::BTreeMap::from([(
                            "type".to_string(),
                            Value::String("json".to_string()),
                        )])),
                    ),
                ])),
            ]),
        );
        map.insert("triggers".to_string(), Value::Array(Vec::new()));
        let target = load_module(&Value::Object(map)).unwrap();
        let local = target_names("app", &target, None);
        let replicated = target_names("app", &target, Some(12));
        assert_eq!(replicated.len(), local.len() + 1);
        assert_eq!(
            replicated.last().unwrap(),
            &b"$staged\x0012\x00app\x00total".to_vec()
        );
    }
}
