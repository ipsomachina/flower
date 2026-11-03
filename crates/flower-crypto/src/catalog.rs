//! The replicated managed-key catalog.
//!
//! Every logical database keeps a catalog of its own, stored with its
//! rows, so it moves, snapshots and restores with them. The catalog
//! names keys, their algorithms and immutable versions, and the
//! operator bindings that grant the database's guest aliases usages
//! over them. Key material never appears in clear: every version carries
//! an [`Envelope`], its material sealed under a fresh data key that
//! is itself wrapped under the operator's wrapping key, with the
//! catalog domain, key identity, version, algorithm and purpose bound
//! as associated data.
//!
//! Changes replicate through the Raft log as a [`CatalogChange`]: the
//! leader prepares the next catalog (generating or unwrapping
//! material with its wrapping key, see
//! [`crate::managed::ManagedKeys::prepare`]) and proposes it beside
//! the revision it started from. Every member applies the change with
//! [`apply_change`], a pure function of the stored catalog bytes and
//! the entry, so members converge without generating key material or
//! holding the wrapping key. A change whose base revision moved is a
//! conflict and changes nothing; the proposer prepares again.
//!
//! Every change carries the operator's request ID and a digest of the
//! request. The catalog keeps the newest [`MAX_RECEIPTS`] receipts, so
//! a retried request returns its committed outcome instead of, say,
//! rotating twice; reusing an ID for different contents fails.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Receipts the catalog keeps for request-ID retries.
pub const MAX_RECEIPTS: usize = 256;
/// Longest key name, binding alias or request ID.
pub const MAX_NAME: usize = 128;

/// A managed-key algorithm.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Algorithm {
    /// Ed25519 signatures: JWT EdDSA and NaCl signing.
    Ed25519,
    /// P-256 ECDSA: JWT ES256 with RFC 6979 deterministic signing.
    P256,
    /// RSA PKCS#1 v1.5 SHA-256: JWT RS256.
    #[serde(rename = "RSA")]
    Rsa,
    /// HMAC-SHA-256: JWT HS256.
    HS256,
    /// AES-256-GCM: compact dir/A256GCM JWE.
    A256GCM,
    /// NaCl secretbox.
    XSalsa20Poly1305,
    /// NaCl box: X25519 key agreement.
    X25519,
}

impl Algorithm {
    /// Every algorithm, in catalog order.
    pub const ALL: [Algorithm; 7] = [
        Algorithm::Ed25519,
        Algorithm::P256,
        Algorithm::Rsa,
        Algorithm::HS256,
        Algorithm::A256GCM,
        Algorithm::XSalsa20Poly1305,
        Algorithm::X25519,
    ];

    /// The usages a binding over this algorithm may grant.
    pub fn usages(self) -> &'static [&'static str] {
        match self {
            Self::Ed25519 | Self::P256 | Self::Rsa => &["sign", "verify", "publicKey"],
            Self::HS256 => &["sign", "verify"],
            Self::A256GCM | Self::XSalsa20Poly1305 => &["encrypt", "decrypt"],
            Self::X25519 => &["encrypt", "decrypt", "derive", "publicKey"],
        }
    }

    /// The algorithm's catalog name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Ed25519 => "Ed25519",
            Self::P256 => "P256",
            Self::Rsa => "RSA",
            Self::HS256 => "HS256",
            Self::A256GCM => "A256GCM",
            Self::XSalsa20Poly1305 => "XSalsa20Poly1305",
            Self::X25519 => "X25519",
        }
    }

    /// Parse a catalog name.
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|algorithm| algorithm.name() == name)
    }
}

/// Sealed key material: the material under a fresh data key (DEK),
/// the DEK wrapped under the wrapping key `wrapping_id` names. Binary
/// fields are unpadded base64url.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Envelope {
    /// Identity of the wrapping key: base64url SHA-256 of its bytes.
    pub wrapping_id: String,
    /// The DEK sealed under the wrapping key.
    pub wrapped_dek: String,
    /// Nonce of the DEK wrap.
    pub wrapping_nonce: String,
    /// The material sealed under the DEK.
    pub ciphertext: String,
    /// Nonce of the material seal.
    pub nonce: String,
}

/// One immutable key version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Version {
    /// Sealed material; `None` once destroyed.
    pub envelope: Option<Envelope>,
    /// Revoked versions serve nothing.
    #[serde(default)]
    pub revoked: bool,
    /// Retired versions verify and decrypt but never sign or encrypt.
    #[serde(default)]
    pub retired: bool,
}

/// One named key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeyRecord {
    /// Stable random identity, bound into envelopes and JOSE `kid`s.
    pub id: String,
    /// The key's algorithm, fixed for every version.
    pub algorithm: Algorithm,
    /// The version new signatures and ciphertexts use.
    pub active_version: u64,
    /// Every version, destroyed ones included.
    pub versions: BTreeMap<u64, Version>,
}

/// An operator binding: a guest alias granted usages over a key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Binding {
    /// The bound key's name.
    pub key: String,
    /// Granted usages.
    pub usages: BTreeSet<String>,
}

/// A committed request: its ID, request digest and the catalog
/// revision it produced.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Receipt {
    /// The operator's request ID.
    pub request_id: String,
    /// Hex SHA-256 of the canonical request.
    pub digest: String,
    /// Catalog revision after the request.
    pub revision: u64,
}

/// The catalog: one per logical database.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Catalog {
    /// Random security domain, fixed at the first change and bound
    /// into every envelope: envelopes copied from another catalog (of
    /// another database, or another database's later one) fail
    /// authentication.
    pub domain: String,
    /// Catalog revision: one per committed change.
    pub revision: u64,
    /// Keys by name.
    pub keys: BTreeMap<String, KeyRecord>,
    /// Bindings by guest alias.
    pub bindings: BTreeMap<String, Binding>,
    /// Newest committed requests, oldest first.
    #[serde(default)]
    pub receipts: Vec<Receipt>,
}

/// A replicated catalog change: the full next catalog beside the
/// revision it was prepared from.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogChange {
    /// The operator's request ID.
    pub request_id: String,
    /// Hex SHA-256 of the canonical request.
    pub digest: String,
    /// The catalog revision the change was prepared from.
    pub base_revision: u64,
    /// The next catalog. Its receipts are ignored: apply records them.
    pub catalog: Catalog,
}

/// The outcome of applying a change.
#[derive(Clone, Debug, PartialEq)]
pub struct Applied {
    /// The catalog bytes to store, or `None` when nothing changed (a
    /// retried request).
    pub catalog: Option<Vec<u8>>,
    /// The operator response: `{revision, value, duplicate}` with the
    /// public catalog as the value.
    pub response: Value,
}

/// Decode stored catalog bytes; absent bytes are the empty catalog.
pub fn decode(bytes: Option<&[u8]>) -> Result<Catalog, String> {
    match bytes {
        None => Ok(Catalog::default()),
        Some(bytes) => {
            serde_json::from_slice(bytes).map_err(|error| format!("corrupt key catalog: {error}"))
        }
    }
}

/// Encode a catalog for storage.
pub fn encode(catalog: &Catalog) -> Vec<u8> {
    serde_json::to_vec(catalog).expect("catalogs serialize")
}

/// Encode a change for the log.
pub fn encode_change(change: &CatalogChange) -> Vec<u8> {
    serde_json::to_vec(change).expect("catalog changes serialize")
}

/// The receipt a request ID left, if the catalog still holds it.
pub fn receipt<'a>(catalog: &'a Catalog, request_id: &str) -> Option<&'a Receipt> {
    catalog
        .receipts
        .iter()
        .find(|receipt| receipt.request_id == request_id)
}

/// Apply a change to the stored catalog: deterministic on every
/// member. Errors are deterministic rejections — a moved base
/// revision, a reused request ID or a malformed catalog — and change
/// nothing.
pub fn apply_change(current: Option<&[u8]>, change: &[u8]) -> Result<Applied, String> {
    let current = decode(current)?;
    let change: CatalogChange = serde_json::from_slice(change)
        .map_err(|error| format!("malformed key catalog change: {error}"))?;
    if let Some(receipt) = receipt(&current, &change.request_id) {
        if receipt.digest != change.digest {
            return Err(format!(
                "KEY_REQUEST_REUSED: request ID {:?} already committed different contents",
                change.request_id
            ));
        }
        return Ok(Applied {
            catalog: None,
            response: response(&current, receipt.revision, true),
        });
    }
    if change.base_revision != current.revision {
        return Err(format!(
            "KEY_CATALOG_CONFLICT: prepared at revision {} but the catalog is at {}",
            change.base_revision, current.revision
        ));
    }
    let mut next = change.catalog;
    check(&current, &next)?;
    next.receipts = current.receipts;
    next.receipts.push(Receipt {
        request_id: change.request_id,
        digest: change.digest,
        revision: next.revision,
    });
    if next.receipts.len() > MAX_RECEIPTS {
        let excess = next.receipts.len() - MAX_RECEIPTS;
        next.receipts.drain(..excess);
    }
    let revision = next.revision;
    let response = response(&next, revision, false);
    Ok(Applied {
        catalog: Some(encode(&next)),
        response,
    })
}

/// Check a proposed catalog against the one it replaces.
fn check(current: &Catalog, next: &Catalog) -> Result<(), String> {
    let fail = |message: &str| Err(format!("invalid key catalog change: {message}"));
    if Some(next.revision) != current.revision.checked_add(1) {
        return fail("revision must advance by one");
    }
    if next.domain.is_empty() || (current.revision != 0 && next.domain != current.domain) {
        return fail("the security domain is fixed");
    }
    for (name, key) in &next.keys {
        safe_name(name).map_err(|message| format!("invalid key catalog change: {message}"))?;
        if !key.versions.contains_key(&key.active_version) {
            return fail("a key's active version must exist");
        }
        if let Some(old) = current.keys.get(name) {
            if old.id != key.id || old.algorithm != key.algorithm {
                return fail("key identity and algorithm are fixed");
            }
            // Versions are immutable: they gain flags and lose
            // envelopes, never come back or change material.
            for (number, version) in &old.versions {
                let Some(now) = key.versions.get(number) else {
                    return fail("versions are never removed");
                };
                if (version.revoked && !now.revoked)
                    || (version.retired && !now.retired)
                    || (version.envelope.is_none() && now.envelope.is_some())
                {
                    return fail("revocation, retirement and destruction are permanent");
                }
            }
        }
    }
    for (alias, binding) in &next.bindings {
        safe_name(alias).map_err(|message| format!("invalid key catalog change: {message}"))?;
        let Some(key) = next.keys.get(&binding.key) else {
            return fail("a binding names an unknown key");
        };
        if binding.usages.is_empty()
            || !binding
                .usages
                .iter()
                .all(|usage| key.algorithm.usages().contains(&usage.as_str()))
        {
            return fail("a binding grants usages its key's algorithm lacks");
        }
    }
    Ok(())
}

/// Key names, aliases and request IDs: non-empty, bounded, no control
/// characters.
pub fn safe_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_NAME || name.chars().any(char::is_control) {
        return Err(format!(
            "names must hold 1 to {MAX_NAME} bytes without control characters"
        ));
    }
    Ok(())
}

/// The JOSE key ID of a key version.
pub fn kid(id: &str, version: u64) -> String {
    format!("flower.{id}.{version}")
}

/// The operator response for a catalog.
fn response(catalog: &Catalog, revision: u64, duplicate: bool) -> Value {
    json!({
        "revision": revision,
        "value": public(catalog),
        "duplicate": duplicate,
    })
}

/// The catalog without envelopes: what operators may read.
pub fn public(catalog: &Catalog) -> Value {
    let keys: serde_json::Map<String, Value> = catalog
        .keys
        .iter()
        .map(|(name, key)| {
            let versions: Vec<Value> = key
                .versions
                .iter()
                .map(|(number, version)| {
                    json!({
                        "version": number,
                        "revoked": version.revoked,
                        "retired": version.retired,
                        "destroyed": version.envelope.is_none(),
                        "wrappingId": version.envelope.as_ref().map(|envelope| &envelope.wrapping_id),
                        "kid": kid(&key.id, *number),
                    })
                })
                .collect();
            (
                name.clone(),
                json!({
                    "id": key.id,
                    "algorithm": key.algorithm,
                    "activeVersion": key.active_version,
                    "versions": versions,
                }),
            )
        })
        .collect();
    json!({
        "domain": if catalog.domain.is_empty() { Value::Null } else { Value::String(catalog.domain.clone()) },
        "revision": catalog.revision,
        "keys": keys,
        "bindings": catalog.bindings,
    })
}
