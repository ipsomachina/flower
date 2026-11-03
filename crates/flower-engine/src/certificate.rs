//! Dependency certificates: what a read observed, so a later commit can
//! tell whether it could change the result.
//!
//! An evaluation's session records a dependency for every read (records
//! present or absent, collection scans, index ranges, derived instances).
//! [`Certificate::observe`] flattens them: derived-instance reads expand,
//! transitively, into the edges their outcomes recorded (staged in the
//! session, else committed), so a certificate names only records and
//! ranges. A read that leaves no precise dependency (retained history,
//! timer rows) records [`volatile_dep`]; reading the clock through `now`
//! records [`time_dep`]; resolving a managed key records [`keys_dep`].
//! All three are reserved record dependencies that no write ever names,
//! so derived outcomes keep them in their persisted edges and older
//! members decode them as ordinary records. A certificate that met the
//! first is *broad*: every change to its database may affect it. One
//! that met the second is [`Validity::Polled`]; one whose evaluation
//! declared `changes_at` holds [`Validity::Until`] that instant. One
//! that met the third [reads keys](Certificate::reads_keys): a change to
//! the key catalog, which moves no database, may affect it. A fourth,
//! [`module_dep`], names another module's surface for derived outcomes
//! only: certificates leave it out, since their code version covers it.
//!
//! A [`ChangeSet`] names one commit's writes to one database: records by
//! key and index entries by components. [`Certificate::touched_by`]
//! decides whether a change set can affect a certificate: an exact
//! record, a record inside a scanned collection range, or an index entry
//! inside a read index range. Code changes touch everything; callers
//! compare code versions besides.
//!
//! The [`ChangeJournal`] retains recent change sets per database as one
//! contiguous chain of revisions, recorded by the commit before it
//! becomes visible, so a certificate taken at revision `r` stays valid at
//! a later revision `R` exactly when no retained change in `(r, R]`
//! touches it. A revision that skips ahead (a bump nobody recorded, a
//! snapshot install) or goes back starts a new chain, and certificates
//! anchored to the old one never validate again.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

mod graph;

pub(crate) use graph::served;
pub use graph::{Graph, Instance};

use crate::derived::IndexChange;
use crate::error::{Error, Result};
use crate::patch::Patch;
use crate::session::Staged;
use crate::storage::ReadTxn;
use crate::storage::keys::{
    DepKey, RangeDep, collection_range_matches, decode_dep_key, encode_derived_key,
    encode_record_dep, encode_record_key, index_range_matches,
};

/// Reserved collection ID of the marker dependencies: interning assigns
/// IDs upward from one, so no collection reaches it.
const RESERVED_COLLECTION: u32 = u32::MAX;
/// Marker key: the evaluation polled the clock.
const TIME_KEY: &[u8] = b"$time";
/// Marker key: the evaluation read something no dependency names.
const VOLATILE_KEY: &[u8] = b"$volatile";
/// Marker key: the evaluation resolved a managed key.
const KEYS_KEY: &[u8] = b"$keys";
/// Marker key prefix: the evaluation reached another module's surface.
const MODULE_KEY: &[u8] = b"$module\0";
/// Most dependencies one certificate keeps before it turns broad.
pub const MAX_DEPENDENCIES: usize = 4096;
/// Most change sets the journal retains, across databases.
pub const JOURNAL_CHANGES: usize = 4096;
/// Most record and index changes the journal retains, across databases.
pub const JOURNAL_KEYS: usize = 1 << 16;

/// The dependency a clock poll records.
pub fn time_dep() -> Vec<u8> {
    encode_record_dep(RESERVED_COLLECTION, TIME_KEY).unwrap_or_default()
}

/// The dependency an untracked read records.
pub fn volatile_dep() -> Vec<u8> {
    encode_record_dep(RESERVED_COLLECTION, VOLATILE_KEY).unwrap_or_default()
}

/// The dependency resolving a managed key records: the whole key catalog.
pub fn keys_dep() -> Vec<u8> {
    encode_record_dep(RESERVED_COLLECTION, KEYS_KEY).unwrap_or_default()
}

/// The dependency reading `module`'s collections or derived values
/// from another module records, refused reads included: what its
/// exports, read rules, contracts and structures say, which change
/// only when it activates, and its activation drops every outcome that
/// recorded this. Certificates leave it out: their code version covers
/// every module.
pub fn module_dep(module: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(MODULE_KEY.len() + module.len());
    key.extend_from_slice(MODULE_KEY);
    key.extend_from_slice(module.as_bytes());
    encode_record_dep(RESERVED_COLLECTION, &key).unwrap_or_default()
}

/// How long a result stays valid without commits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Validity {
    /// Until a commit changes what it observed.
    #[default]
    Stable,
    /// Until the wall clock reaches this instant (milliseconds since
    /// the Unix epoch), as the evaluation declared with `changes_at`.
    Until(u64),
    /// The evaluation polled the clock: any later instant may differ.
    Polled,
}

impl Validity {
    /// What an evaluation at `timestamp_ms` declared: a poll wins over a
    /// declaration, and a declared instant not after the evaluation's
    /// own time counts as a poll, so a stale declaration never spins.
    pub fn declared(polled: bool, changes_at: Option<f64>, timestamp_ms: u64) -> Self {
        match changes_at {
            _ if polled => Validity::Polled,
            None => Validity::Stable,
            Some(at) if at.is_nan() || at <= timestamp_ms as f64 => Validity::Polled,
            Some(at) if at >= u64::MAX as f64 => Validity::Stable,
            Some(at) => Validity::Until(at.ceil() as u64),
        }
    }

    /// The earlier-expiring of two validities.
    pub fn min(self, other: Validity) -> Validity {
        match (self, other) {
            (Validity::Polled, _) | (_, Validity::Polled) => Validity::Polled,
            (Validity::Until(a), Validity::Until(b)) => Validity::Until(a.min(b)),
            (Validity::Until(at), Validity::Stable) | (Validity::Stable, Validity::Until(at)) => {
                Validity::Until(at)
            }
            (Validity::Stable, Validity::Stable) => Validity::Stable,
        }
    }

    /// Whether a result evaluated at `evaluated_ms` still holds at
    /// `now_ms`.
    pub fn holds(self, evaluated_ms: u64, now_ms: u64) -> bool {
        match self {
            Validity::Stable => true,
            Validity::Until(at) => evaluated_ms <= now_ms && now_ms < at,
            Validity::Polled => false,
        }
    }
}

/// What one evaluation observed, flattened to records and ranges.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Certificate {
    /// Exact record dependencies, as encoded record dependency keys.
    #[serde(serialize_with = "crate::storage::overlay::serialize_set")]
    records: HashSet<Vec<u8>>,
    /// Collection scans: collection ID and bounds over full record keys.
    scans: Vec<(u32, RangeDep)>,
    /// Index range reads: index ID and bounds.
    ranges: Vec<(u32, RangeDep)>,
    /// Some read named no dependency, or there were too many: every
    /// change counts.
    broad: bool,
    /// Some read resolved a managed key: key-catalog changes count.
    keys: bool,
    /// How long the result holds without commits.
    validity: Validity,
    /// Approximate retained bytes.
    bytes: usize,
}

impl Certificate {
    /// A certificate every change touches.
    pub fn broad() -> Self {
        Certificate {
            broad: true,
            ..Certificate::default()
        }
    }

    /// Flatten an evaluation's recorded dependencies: derived-instance
    /// reads expand into their outcomes' edges, staged in `staged` or
    /// else committed in `read`, transitively; markers decide breadth and
    /// validity. Past [`MAX_DEPENDENCIES`] the certificate turns broad.
    pub fn observe(
        read: &ReadTxn,
        staged: &Staged,
        db: &[u8],
        deps: Vec<Vec<u8>>,
        changes_at: Option<f64>,
        timestamp_ms: u64,
    ) -> Result<Self> {
        let time = time_dep();
        let volatile = volatile_dep();
        let keys = keys_dep();
        let mut certificate = Certificate::default();
        let mut polled = false;
        let mut expanded = HashSet::new();
        let mut queue: VecDeque<Vec<u8>> = deps.into();
        let mut seen = 0usize;
        while let Some(dep) = queue.pop_front() {
            seen += 1;
            if seen > MAX_DEPENDENCIES {
                certificate.broad = true;
                break;
            }
            if dep == time {
                polled = true;
                continue;
            }
            if dep == volatile {
                certificate.broad = true;
                continue;
            }
            if dep == keys {
                certificate.keys = true;
                continue;
            }
            let (key, used) = decode_dep_key(&dep)?;
            if used != dep.len() {
                return Err(Error::corrupt("trailing dependency bytes"));
            }
            // A module marker: the code version stands for it.
            if let DepKey::Record { collection, .. } = &key
                && *collection == RESERVED_COLLECTION
            {
                continue;
            }
            certificate.bytes += dep.len() + 32;
            match key {
                DepKey::Record { .. } => {
                    certificate.records.insert(dep);
                }
                DepKey::CollectionRange { collection, range } => {
                    certificate.scans.push((collection, range));
                }
                DepKey::Range { index, range } => certificate.ranges.push((index, range)),
                DepKey::Derived { definition, args } => {
                    if !expanded.insert((definition, args.clone())) {
                        continue;
                    }
                    let key = encode_derived_key(db, definition, &args)?;
                    match staged.staged_observed(&key) {
                        Some(edges) => queue.extend(edges.iter().cloned()),
                        None => queue.extend(read.dependent_edges(db, definition, &args)?),
                    }
                }
            }
        }
        certificate.validity = Validity::declared(polled, changes_at, timestamp_ms);
        Ok(certificate)
    }

    /// What one evaluation recorded itself, without expanding derived
    /// reads, and the derived instances it read (see [`Graph`]). A
    /// clock poll makes it [`Validity::Polled`]; it declares no instant.
    pub fn direct(deps: &[Vec<u8>]) -> Result<(Self, Vec<graph::Instance>)> {
        let time = time_dep();
        let volatile = volatile_dep();
        let keys = keys_dep();
        let mut certificate = Certificate::default();
        let mut instances = Vec::new();
        let mut polled = false;
        for dep in deps {
            if *dep == time {
                polled = true;
                continue;
            }
            if *dep == volatile {
                certificate.broad = true;
                continue;
            }
            if *dep == keys {
                certificate.keys = true;
                continue;
            }
            let (key, used) = decode_dep_key(dep)?;
            if used != dep.len() {
                return Err(Error::corrupt("trailing dependency bytes"));
            }
            match key {
                DepKey::Derived { definition, args } => {
                    instances.push((definition, args));
                    continue;
                }
                DepKey::Record { .. } => {
                    certificate.records.insert(dep.clone());
                }
                DepKey::CollectionRange { collection, range } => {
                    certificate.scans.push((collection, range));
                }
                DepKey::Range { index, range } => certificate.ranges.push((index, range)),
            }
            certificate.bytes += dep.len() + 32;
        }
        certificate.validity = Validity::declared(polled, None, 0);
        Ok((certificate, instances))
    }

    /// How many records and ranges it names.
    pub fn dependencies(&self) -> usize {
        self.records.len() + self.scans.len() + self.ranges.len()
    }

    /// Whether every change counts.
    pub fn is_broad(&self) -> bool {
        self.broad
    }

    /// Whether it resolved a managed key, so a key-catalog change may
    /// affect it though no database moved.
    pub fn reads_keys(&self) -> bool {
        self.keys
    }

    /// Whether it observed nothing: only code changes and nothing on
    /// the clock or the key catalog can affect it.
    pub fn is_empty(&self) -> bool {
        !self.broad
            && !self.keys
            && self.validity == Validity::Stable
            && self.records.is_empty()
            && self.scans.is_empty()
            && self.ranges.is_empty()
    }

    /// How long the result holds without commits.
    pub fn validity(&self) -> Validity {
        self.validity
    }

    /// Approximate retained bytes.
    pub fn allocation_cost(&self) -> usize {
        self.bytes + 64
    }

    /// Whether a commit's changes can affect the result.
    pub fn touched_by(&self, changes: &ChangeSet) -> bool {
        if self.broad || changes.everything {
            return true;
        }
        if changes.records.iter().any(|record| {
            self.records.contains(&record.dep)
                || self.scans.iter().any(|(collection, range)| {
                    *collection == record.collection
                        && collection_range_matches(range, &record.full)
                })
        }) {
            return true;
        }
        changes.indexes.iter().any(|(index, components, primary)| {
            self.ranges
                .iter()
                .any(|(id, range)| id == index && index_range_matches(range, components, primary))
        })
    }

    /// What two evaluations observed together: touched when either is,
    /// valid while both are.
    pub fn merge(&mut self, other: &Certificate) {
        self.broad |= other.broad;
        self.keys |= other.keys;
        self.records.extend(other.records.iter().cloned());
        self.scans.extend(other.scans.iter().cloned());
        self.ranges.extend(other.ranges.iter().cloned());
        self.validity = self.validity.min(other.validity);
        self.bytes += other.bytes;
    }
}

/// One changed record, keyed every way a certificate looks it up.
#[derive(Clone, Debug)]
struct ChangedRecord {
    collection: u32,
    /// Encoded record dependency.
    dep: Vec<u8>,
    /// Full encoded record key, for collection-range bounds.
    full: Vec<u8>,
}

/// One commit's writes to one database.
#[derive(Clone, Debug)]
pub struct ChangeSet {
    db: Vec<u8>,
    /// Code or everything changed.
    everything: bool,
    records: Vec<ChangedRecord>,
    indexes: Vec<IndexChange>,
}

impl ChangeSet {
    /// Writes naming nothing: a commit that changed no data.
    pub fn none(db: &[u8]) -> Self {
        ChangeSet {
            db: db.to_vec(),
            everything: false,
            records: Vec::new(),
            indexes: Vec::new(),
        }
    }

    /// A change that may affect everything in the database, such as a
    /// deployment.
    pub fn everything(db: &[u8]) -> Self {
        ChangeSet {
            everything: true,
            ..ChangeSet::none(db)
        }
    }

    /// The writes of an applied patch: its records and index entries.
    pub fn of_patch(patch: &Patch) -> Result<Self> {
        let mut changes = ChangeSet::none(&patch.db);
        for record in &patch.records {
            changes.records.push(ChangedRecord {
                collection: record.collection,
                dep: encode_record_dep(record.collection, &record.key)?,
                full: encode_record_key(&patch.db, record.collection, &record.key)?,
            });
        }
        changes.indexes = patch
            .indexes
            .iter()
            .map(|entry| (entry.index, entry.components.clone(), entry.primary.clone()))
            .collect();
        Ok(changes)
    }

    /// The database written.
    pub fn db(&self) -> &[u8] {
        &self.db
    }

    /// Whether the change may affect everything.
    pub fn is_everything(&self) -> bool {
        self.everything
    }

    /// Whether the commit wrote nothing.
    pub fn is_empty(&self) -> bool {
        !self.everything && self.records.is_empty() && self.indexes.is_empty()
    }

    /// Changed records and index entries, the journal's unit of size.
    fn keys(&self) -> usize {
        self.records.len() + self.indexes.len()
    }
}

/// Where a certificate's evaluation sits in the journal: the chain of
/// its database and the revision it read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Anchor {
    /// The chain's identity.
    pub chain: u64,
    /// The revision the evaluation read.
    pub revision: u64,
}

/// One database's contiguous run of recorded changes.
#[derive(Debug)]
struct Chain {
    id: u64,
    /// Every change after this revision, through `last`, is retained.
    from: u64,
    /// The newest recorded revision.
    last: u64,
    /// `(revision, changes)` for revisions in `(from, last]`.
    changes: VecDeque<(u64, Arc<ChangeSet>)>,
}

#[derive(Debug, Default)]
struct Journal {
    chains: HashMap<Vec<u8>, Chain>,
    /// Retained change sets, oldest first, as `(database, revision)`.
    order: VecDeque<(Vec<u8>, u64)>,
    /// Retained record and index changes.
    keys: usize,
    /// Bumped whenever a chain breaks or the journal resets: an anchor
    /// whose mark predates the bump may straddle the break.
    epoch: u64,
    /// The last chain identity handed out.
    next_chain: u64,
}

impl Journal {
    /// A fresh chain at `revision`.
    fn chain(&mut self, revision: u64) -> Chain {
        self.next_chain += 1;
        Chain {
            id: self.next_chain,
            from: revision,
            last: revision,
            changes: VecDeque::new(),
        }
    }

    /// Drop the oldest retained change sets past the bounds.
    fn trim(&mut self) {
        while self.order.len() > JOURNAL_CHANGES || self.keys > JOURNAL_KEYS {
            let Some((db, revision)) = self.order.pop_front() else {
                break;
            };
            let Some(chain) = self.chains.get_mut(&db) else {
                continue;
            };
            while let Some((at, _)) = chain.changes.front() {
                if *at > revision {
                    break;
                }
                let (at, changes) = chain.changes.pop_front().expect("front exists");
                self.keys = self.keys.saturating_sub(changes.keys());
                chain.from = chain.from.max(at);
            }
        }
    }
}

/// Recent commits' change sets, per database, as contiguous chains.
#[derive(Debug, Default)]
pub struct ChangeJournal {
    inner: Mutex<Journal>,
}

impl ChangeJournal {
    fn lock(&self) -> std::sync::MutexGuard<'_, Journal> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a commit that moved `changes.db()` to `revision`. Callers
    /// record before the commit becomes visible, so every revision a
    /// reader can see is recorded or breaks a chain. A commit always
    /// applies to the revision before it; one that does not continue
    /// its database's chain starts a new one.
    pub fn record(&self, revision: u64, changes: Arc<ChangeSet>) {
        let mut journal = self.lock();
        let db = changes.db.clone();
        let continues = journal
            .chains
            .get(&db)
            .is_some_and(|chain| chain.last.checked_add(1) == Some(revision));
        if !continues {
            if journal.chains.contains_key(&db) {
                journal.epoch += 1;
            }
            let chain = journal.chain(revision.saturating_sub(1));
            if let Some(old) = journal.chains.insert(db.clone(), chain) {
                let dropped: usize = old.changes.iter().map(|(_, changes)| changes.keys()).sum();
                journal.keys = journal.keys.saturating_sub(dropped);
            }
        }
        journal.keys += changes.keys();
        journal.order.push_back((db.clone(), revision));
        let chain = journal.chains.get_mut(&db).expect("chain exists");
        chain.last = revision;
        chain.changes.push_back((revision, changes));
        journal.trim();
    }

    /// Forget every chain: the store was replaced wholesale.
    pub fn reset(&self) {
        let mut journal = self.lock();
        journal.chains.clear();
        journal.order.clear();
        journal.keys = 0;
        journal.epoch += 1;
    }

    /// A mark to take before capturing a snapshot, for
    /// [`ChangeJournal::anchor`].
    pub fn mark(&self) -> u64 {
        self.lock().epoch
    }

    /// Anchor an evaluation that read `db` at `revision` on a snapshot
    /// captured after `mark`: `None` when a chain broke since the mark
    /// or the chain does not reach that revision. A database with no
    /// chain yet starts one there: nothing was recorded for it since the
    /// mark, so no change since that revision is missing.
    pub fn anchor(&self, db: &[u8], mark: u64, revision: u64) -> Option<Anchor> {
        let mut journal = self.lock();
        if journal.epoch != mark {
            return None;
        }
        if !journal.chains.contains_key(db) {
            let chain = journal.chain(revision);
            journal.chains.insert(db.to_vec(), chain);
        }
        let chain = &journal.chains[db];
        (chain.from <= revision && revision <= chain.last).then_some(Anchor {
            chain: chain.id,
            revision,
        })
    }

    /// The change sets of `db` after `anchor` through `revision`, when
    /// its chain still holds every one of them.
    pub fn since(&self, db: &[u8], anchor: Anchor, revision: u64) -> Option<Vec<Arc<ChangeSet>>> {
        let journal = self.lock();
        let chain = journal.chains.get(db)?;
        if chain.id != anchor.chain
            || anchor.revision < chain.from
            || revision > chain.last
            || revision < anchor.revision
        {
            return None;
        }
        Some(
            chain
                .changes
                .iter()
                .filter(|(at, _)| *at > anchor.revision && *at <= revision)
                .map(|(_, changes)| changes.clone())
                .collect(),
        )
    }

    /// Whether nothing recorded for `db` after `anchor` through
    /// `revision` touches `certificate`; `false` when the chain cannot
    /// tell.
    pub fn unchanged(
        &self,
        db: &[u8],
        anchor: Anchor,
        revision: u64,
        certificate: &Certificate,
    ) -> bool {
        self.since(db, anchor, revision).is_some_and(|changes| {
            changes
                .iter()
                .all(|changes| !certificate.touched_by(changes))
        })
    }
}

/// This member's managed-key catalog changes, as a sequence: odd while
/// one is in flight (from before its apply transaction commits until
/// the member's crypto host serves the new catalog), even between
/// them. A reader takes a [`KeySequence::mark`] before its snapshot;
/// what it computed from a key holds only while the sequence is
/// [`KeySequence::settled`] at that mark, since a change in flight or
/// finished since may have served it the old catalog over state the
/// change already invalidated, or the reverse.
#[derive(Debug, Default)]
pub struct KeySequence {
    value: AtomicU64,
}

impl KeySequence {
    /// A mark to take before capturing a snapshot.
    pub fn mark(&self) -> u64 {
        self.value.load(Ordering::SeqCst)
    }

    /// A change begins: the sequence turns odd.
    pub fn begin(&self) {
        self.value.fetch_add(1, Ordering::SeqCst);
    }

    /// The change ends: the sequence turns even.
    pub fn end(&self) {
        self.value.fetch_add(1, Ordering::SeqCst);
    }

    /// Whether no change began since `mark`, which no change was in
    /// flight at.
    pub fn settled(&self, mark: u64) -> bool {
        mark.is_multiple_of(2) && self.mark() == mark
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patch::{IndexWrite, RecordWrite};
    use crate::storage::keys::{encode_collection_range_dep, encode_range_dep};

    fn patch(db: &[u8], records: &[(u32, &[u8])], indexes: Vec<IndexWrite>) -> Patch {
        Patch {
            failed: false,
            db: db.to_vec(),
            versions: Vec::new(),
            records: records
                .iter()
                .map(|(collection, key)| RecordWrite {
                    collection: *collection,
                    key: key.to_vec(),
                    value: None,
                })
                .collect(),
            indexes,
            derived: Vec::new(),
            drops: Vec::new(),
            receipts: Vec::new(),
            result: Vec::new(),
            expected_revision: None,
            base_revision: 0,
        }
    }

    fn changes(records: &[(u32, &[u8])]) -> Arc<ChangeSet> {
        Arc::new(ChangeSet::of_patch(&patch(b"db", records, Vec::new())).unwrap())
    }

    fn certificate(deps: Vec<Vec<u8>>) -> Certificate {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        let store = crate::storage::GroupStore::from_database(db, None).unwrap();
        let read = store.begin_read().unwrap();
        Certificate::observe(&read, &Staged::default(), b"db", deps, None, 0).unwrap()
    }

    #[test]
    fn records_scans_and_ranges_decide_what_touches() {
        let record = certificate(vec![encode_record_dep(1, b"a").unwrap()]);
        assert!(record.touched_by(&changes(&[(1, b"a")])));
        assert!(!record.touched_by(&changes(&[(1, b"b")])));
        assert!(!record.touched_by(&changes(&[(2, b"a")])));
        assert!(!record.touched_by(&ChangeSet::none(b"db")));
        assert!(record.touched_by(&ChangeSet::everything(b"db")));

        let scan = certificate(vec![
            encode_collection_range_dep(3, &RangeDep::default()).unwrap(),
        ]);
        assert!(scan.touched_by(&changes(&[(3, b"anything")])));
        assert!(!scan.touched_by(&changes(&[(4, b"anything")])));

        let range = RangeDep {
            prefix: vec![b"red".to_vec()],
            ..RangeDep::default()
        };
        let indexed = certificate(vec![encode_range_dep(7, &range).unwrap()]);
        let entry = |index: u32, color: &[u8]| IndexWrite {
            index,
            components: vec![color.to_vec()],
            primary: b"k".to_vec(),
            present: true,
        };
        let touched = ChangeSet::of_patch(&patch(b"db", &[], vec![entry(7, b"red")])).unwrap();
        let elsewhere = ChangeSet::of_patch(&patch(b"db", &[], vec![entry(7, b"blue")])).unwrap();
        assert!(indexed.touched_by(&touched));
        assert!(!indexed.touched_by(&elsewhere));
    }

    #[test]
    fn markers_make_certificates_broad_or_polled() {
        let polled = certificate(vec![time_dep()]);
        assert_eq!(polled.validity(), Validity::Polled);
        assert!(!polled.is_broad());
        let volatile = certificate(vec![volatile_dep()]);
        assert!(volatile.is_broad());
        assert!(volatile.touched_by(&changes(&[(9, b"unrelated")])));
        let many = certificate(
            (0..=MAX_DEPENDENCIES as u32)
                .map(|n| encode_record_dep(1, &n.to_be_bytes()).unwrap())
                .collect(),
        );
        assert!(many.is_broad());
    }

    #[test]
    fn module_markers_stay_out_of_certificates() {
        // The code version covers them: a query that only reached
        // another module, refused, observed nothing a commit can touch.
        let reached = certificate(vec![module_dep("app"), module_dep("board")]);
        assert!(reached.is_empty());
        assert_eq!(reached.bytes, 0);
        assert_ne!(module_dep("app"), module_dep("app2"));
        assert_ne!(module_dep("keys"), keys_dep());
    }

    #[test]
    fn declared_instants_bound_validity() {
        assert_eq!(Validity::declared(false, None, 10), Validity::Stable);
        assert_eq!(
            Validity::declared(false, Some(20.5), 10),
            Validity::Until(21)
        );
        assert_eq!(Validity::declared(true, Some(20.0), 10), Validity::Polled);
        // A declaration already past polls instead of spinning.
        assert_eq!(Validity::declared(false, Some(10.0), 10), Validity::Polled);
        assert!(Validity::Until(21).holds(10, 20));
        assert!(!Validity::Until(21).holds(10, 21));
        assert!(!Validity::Until(21).holds(10, 9));
        assert_eq!(Validity::Until(5).min(Validity::Stable), Validity::Until(5));
    }

    #[test]
    fn journals_validate_only_contiguous_unrelated_changes() {
        let journal = ChangeJournal::default();
        let mark = journal.mark();
        let anchor = journal.anchor(b"db", mark, 4).unwrap();
        journal.record(5, changes(&[(1, b"b")]));
        journal.record(6, changes(&[(1, b"c")]));
        let reads_a = certificate(vec![encode_record_dep(1, b"a").unwrap()]);
        let reads_c = certificate(vec![encode_record_dep(1, b"c").unwrap()]);
        assert!(journal.unchanged(b"db", anchor, 6, &reads_a));
        assert!(!journal.unchanged(b"db", anchor, 6, &reads_c));
        assert!(journal.unchanged(b"db", anchor, 5, &reads_c));
        // Past the newest recorded revision the journal cannot tell.
        assert!(!journal.unchanged(b"db", anchor, 7, &reads_a));
        // A revision that skips ahead breaks the chain.
        journal.record(9, changes(&[(1, b"z")]));
        assert!(!journal.unchanged(b"db", anchor, 9, &reads_a));
        let fresh = journal.anchor(b"db", journal.mark(), 8).unwrap();
        assert!(journal.unchanged(b"db", fresh, 9, &reads_a));
        // An anchor whose mark predates a break never anchors.
        let stale = journal.mark();
        journal.record(3, changes(&[]));
        assert!(journal.anchor(b"db", stale, 3).is_none());
    }

    #[test]
    fn journals_trim_old_changes_and_reset() {
        let journal = ChangeJournal::default();
        let anchor = journal.anchor(b"db", journal.mark(), 0).unwrap();
        for revision in 1..=(JOURNAL_CHANGES as u64 + 1) {
            journal.record(revision, changes(&[(1, b"x")]));
        }
        let reads = certificate(vec![encode_record_dep(2, b"y").unwrap()]);
        // The oldest change aged out: the anchor can no longer validate.
        assert!(!journal.unchanged(b"db", anchor, 2, &reads));
        let recent = journal.anchor(b"db", journal.mark(), 2).unwrap();
        assert!(journal.unchanged(b"db", recent, JOURNAL_CHANGES as u64 + 1, &reads));
        journal.reset();
        assert!(!journal.unchanged(b"db", recent, JOURNAL_CHANGES as u64 + 1, &reads));
    }
}
