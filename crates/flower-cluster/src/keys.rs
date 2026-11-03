//! The replicated managed-key catalogs on a member.
//!
//! Every logical database keeps its own catalog in its meta row
//! [`KEY_CATALOG`], so it rides every snapshot and backup like the rest
//! of the database, and a move carries it to the destination with the
//! database's other rows: key identities, envelopes, bindings and
//! request receipts all arrive as they were. Operators change a
//! database's catalog through [`crate::node::Node::manage_keys`]: the
//! leader prepares the next catalog with its mounted wrapping key (see
//! [`flower_crypto::replicated::ManagedKeys::prepare`]) and proposes it
//! as a [`crate::types::ClusterWrite::Keys`] entry naming the database.
//! Every member applies the entry with
//! [`flower_crypto::catalog::apply_change`], a pure function of the
//! stored row and the entry, so members converge without generating
//! material or holding the wrapping key; a change prepared against a
//! revision that moved meanwhile is a conflict the leader prepares
//! again.
//!
//! [`KeyCatalogCell`] publishes each database's applied revision to
//! this member's [`ManagedKeys`] — the runtime guests resolve keys
//! through — on every change to the row (key changes, a move's imports,
//! an abandoned import, a retirement), snapshot install and startup,
//! whichever comes first relative to
//! [`crate::node::Node::with_key_catalog`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use flower_crypto::replicated::ManagedKeys;

/// The meta row of a database holding its encoded catalog.
pub const KEY_CATALOG: &[u8] = b"key_catalog";

/// Stored catalogs by database: each database's name and its encoded
/// catalog.
pub type KeyCatalogs = Vec<(Vec<u8>, Vec<u8>)>;

/// Failure code: a move's destination cannot unwrap the keys of the
/// database it would take over (see [`crate::moves`]).
pub const KEYS_LOCKED: &str = "KEYS_LOCKED";

/// Why `db` cannot hold a key catalog, if it cannot: a catalog belongs
/// to a database, named as a partition is named (UTF-8, not empty,
/// without control characters), and names starting with `$` are the
/// group's own (`$cluster` among them, which every snapshot already
/// carries). Both the leader and apply check it, so an entry naming
/// one changes nothing on any member.
pub fn invalid_database(db: &[u8]) -> Option<String> {
    match std::str::from_utf8(db) {
        Ok(name) if flower_engine::catalog::valid_partition_name(name) => None,
        _ => Some(format!(
            "{:?} cannot hold a key catalog: a database name is not empty, has no control              characters and does not start with `$`",
            String::from_utf8_lossy(db)
        )),
    }
}

/// The latest applied catalogs and the runtime they feed.
pub struct KeyCatalogCell {
    inner: Mutex<CellState>,
}

struct CellState {
    /// Every database's latest applied catalog bytes.
    latest: BTreeMap<Vec<u8>, Arc<Vec<u8>>>,
    /// The runtime guests resolve keys through.
    keys: Arc<ManagedKeys>,
}

impl Default for KeyCatalogCell {
    fn default() -> Self {
        KeyCatalogCell {
            inner: Mutex::new(CellState {
                latest: BTreeMap::new(),
                keys: Arc::new(ManagedKeys::new(None, Vec::new())),
            }),
        }
    }
}

impl KeyCatalogCell {
    /// Publish database `db`'s applied catalog (`None`: it has none) to
    /// the attached runtime.
    pub fn publish(&self, db: &[u8], catalog: Option<Vec<u8>>) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        match catalog {
            Some(catalog) => state.latest.insert(db.to_vec(), Arc::new(catalog)),
            None => state.latest.remove(db),
        };
        let bytes = state.latest.get(db).cloned();
        if let Err(error) = state.keys.install(db, bytes.as_deref().map(Vec::as_slice)) {
            corrupt(db, &error);
        }
    }

    /// Publish every database's applied catalog at once, replacing what
    /// was published: startup and snapshot installs.
    pub fn publish_all(&self, catalogs: KeyCatalogs) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.latest = catalogs
            .into_iter()
            .map(|(db, catalog)| (db, Arc::new(catalog)))
            .collect();
        install(&state);
    }

    /// Attach a runtime, installing the latest applied catalogs in it.
    pub fn attach(&self, keys: Arc<ManagedKeys>) {
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.keys = keys;
        install(&state);
    }

    /// The attached runtime: a locked one until a member attaches its
    /// own.
    pub fn keys(&self) -> Arc<ManagedKeys> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys
            .clone()
    }
}

/// Install every latest catalog. Applied rows decoded once already, on
/// apply, so a failure here is corruption the state machine would have
/// refused: log it and serve that database no keys.
fn install(state: &CellState) {
    let catalogs = state
        .latest
        .iter()
        .map(|(db, catalog)| (db.clone(), catalog.as_ref().clone()))
        .collect();
    for error in state.keys.install_all(catalogs) {
        tracing::error!(target: "flower::keys", %error, "applied key catalog does not decode");
    }
}

/// Log a published row that does not decode.
fn corrupt(db: &[u8], error: &str) {
    tracing::error!(
        target: "flower::keys",
        db = %String::from_utf8_lossy(db),
        %error,
        "applied key catalog does not decode"
    );
}
