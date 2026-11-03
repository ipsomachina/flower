//! The replicated catalog's runtime: wrapping keys, operator
//! preparation, guest resolution and the prepared-key cache.
//!
//! Every logical database has a catalog of its own: aliases, keys and
//! request receipts never reach across databases, so two databases
//! sharing a group that bind the same alias resolve it to their own
//! keys. [`ManagedKeys`] holds this member's view of every database's
//! applied [`Catalog`] (the state machine installs each applied
//! revision, see [`ManagedKeys::install`]), the mounted wrapping keys
//! and a node-local cache of prepared native contexts. Only the
//! catalogs replicate; the cache and the wrapping keys are per member,
//! and every member must mount the same wrapping key to serve keys.
//!
//! Operators change the catalog through [`ManagedKeys::prepare`] on
//! the leader: generation, import unsealing and rewrapping happen
//! there, before the change is proposed, so followers apply sealed
//! envelopes without generating material.
//!
//! Guests reach keys through bindings: an operator binds an alias to
//! a key with usages, the guest module declares the alias with the
//! same algorithm and the usages it needs, and every use checks the
//! intersection against the applied catalog before the cache is
//! consulted. The cache never authorizes and never holds validation
//! outcomes: it holds unwrapped, parsed key contexts keyed by the
//! complete authenticated envelope, so a replaced envelope can never
//! reuse an older context. It retains contexts up to a byte budget,
//! evicting the least recently used, and optionally expires them.
//! Concurrent cold loads of the same envelope share one preparation,
//! including when retention is disabled; authorization still runs per use.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use aws_lc_rs::encoding::{AsBigEndian, AsDer, Pkcs8V1Der, PublicKeyX509Der};
use aws_lc_rs::hmac;
use aws_lc_rs::signature::{self, Ed25519KeyPair, KeyPair, ParsedPublicKey, RsaKeyPair};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use rand::RngCore;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::catalog::{
    self, Algorithm, Catalog, CatalogChange, Envelope, KeyRecord, Version, kid, safe_name,
};
use crate::{jwt, nacl};

/// Default prepared-context retention budget: 16 MiB.
pub const DEFAULT_CACHE_BYTES: usize = 16 * 1024 * 1024;

/// A mounted 32-byte AES-256 wrapping key.
pub struct WrappingKey {
    id: String,
    cipher: LessSafeKey,
}

impl WrappingKey {
    /// Mount raw key bytes.
    pub fn new(bytes: &[u8; 32]) -> Self {
        WrappingKey {
            id: URL_SAFE_NO_PAD.encode(Sha256::digest(bytes)),
            cipher: cipher(bytes).expect("32 bytes are an AES-256 key"),
        }
    }

    /// Read a wrapping-key file: exactly 32 raw bytes in a regular
    /// file that, on Unix, no group or other user may access.
    pub fn read(path: &Path) -> Result<Self> {
        let metadata = std::fs::metadata(path).context("read wrapping-key file metadata")?;
        ensure!(
            metadata.is_file(),
            "the wrapping-key path must be a regular file"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                "the wrapping-key file must not be accessible by group or other users (chmod 600)"
            );
        }
        let bytes = Zeroizing::new(std::fs::read(path).context("read wrapping-key file")?);
        let bytes: &[u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("the wrapping-key file must hold exactly 32 raw bytes"))?;
        Ok(Self::new(bytes))
    }

    /// The key's identity: base64url SHA-256 of its bytes.
    pub fn id(&self) -> &str {
        &self.id
    }
}

fn cipher(bytes: &[u8]) -> Result<LessSafeKey> {
    Ok(LessSafeKey::new(
        UnboundKey::new(&AES_256_GCM, bytes).map_err(|_| anyhow!("invalid AES-256 key"))?,
    ))
}

fn seal(key: &LessSafeKey, nonce: [u8; 12], aad: &[u8], bytes: &[u8]) -> Result<Vec<u8>> {
    let mut output = Zeroizing::new(bytes.to_vec());
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(aad),
        &mut *output,
    )
    .map_err(|_| anyhow!("key envelope encryption failed"))?;
    Ok(std::mem::take(&mut *output))
}

fn open(
    key: &LessSafeKey,
    nonce: [u8; 12],
    aad: &[u8],
    bytes: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    let mut output = Zeroizing::new(bytes.to_vec());
    let len = key
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad),
            &mut output,
        )
        .map_err(|_| anyhow!("key envelope authentication failed"))?
        .len();
    output.truncate(len);
    Ok(output)
}

fn random<const N: usize>() -> [u8; N] {
    let mut bytes = [0; N];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes
}

fn decode(text: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(text)
        .context("invalid key envelope base64url")
}

fn nonce(text: &str) -> Result<[u8; 12]> {
    decode(text)?
        .try_into()
        .map_err(|_| anyhow!("invalid key envelope nonce"))
}

/// Associated data binding an envelope to its catalog position.
fn aad(domain: &str, id: &str, version: u64, algorithm: Algorithm, purpose: &str) -> Vec<u8> {
    serde_json::to_vec(&json!([
        "flower-key-v1",
        domain,
        id,
        version,
        algorithm,
        purpose
    ]))
    .expect("associated data serializes")
}

/// A prepared native key context. Deliberately neither `Debug` nor
/// serializable, with no accessor for private bytes.
pub struct PreparedKey {
    algorithm: Algorithm,
    inner: Material,
    retained_bytes: usize,
    created_at: Instant,
}

enum Material {
    Ed {
        sign: Ed25519KeyPair,
        strict: ed25519_dalek::VerifyingKey,
        verify: ParsedPublicKey,
    },
    P256 {
        sign: Box<SigningKey>,
        verify: ParsedPublicKey,
    },
    Rsa {
        sign: RsaKeyPair,
        verify: ParsedPublicKey,
    },
    Hmac(Box<hmac::Key>),
    Aes(LessSafeKey),
    Secret(Zeroizing<[u8; 32]>),
}

impl PreparedKey {
    fn prepare(algorithm: Algorithm, bytes: &[u8]) -> Result<Self> {
        let inner = match algorithm {
            Algorithm::Ed25519 => {
                let sign = Ed25519KeyPair::from_seed_unchecked(bytes)
                    .map_err(|_| anyhow!("invalid Ed25519 seed"))?;
                let public: [u8; 32] = sign
                    .public_key()
                    .as_ref()
                    .try_into()
                    .map_err(|_| anyhow!("invalid Ed25519 public key"))?;
                let strict = ed25519_dalek::VerifyingKey::from_bytes(&public)
                    .map_err(|_| anyhow!("invalid Ed25519 public key"))?;
                let verify = ParsedPublicKey::new(&signature::ED25519, public)
                    .map_err(|_| anyhow!("invalid Ed25519 public key"))?;
                Material::Ed {
                    sign,
                    strict,
                    verify,
                }
            }
            Algorithm::P256 => {
                let sign = SigningKey::from_pkcs8_der(bytes)
                    .map_err(|_| anyhow!("invalid P256 private key"))?;
                let public = sign.verifying_key().to_sec1_point(false);
                let verify =
                    ParsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, public.as_bytes())
                        .map_err(|_| anyhow!("invalid P256 public key"))?;
                Material::P256 {
                    sign: Box::new(sign),
                    verify,
                }
            }
            Algorithm::Rsa => {
                let sign = RsaKeyPair::from_pkcs8(bytes)
                    .map_err(|_| anyhow!("invalid RSA PKCS#8 private key"))?;
                ensure!(
                    (256..=1024).contains(&sign.public_modulus_len()),
                    "RSA keys must have 2048 to 8192 bits"
                );
                let verify = ParsedPublicKey::new(
                    &signature::RSA_PKCS1_2048_8192_SHA256,
                    sign.public_key().as_ref(),
                )
                .map_err(|_| anyhow!("invalid RSA public key"))?;
                Material::Rsa { sign, verify }
            }
            Algorithm::HS256 => {
                ensure!(bytes.len() >= 32, "HS256 needs at least 32 bytes");
                Material::Hmac(Box::new(hmac::Key::new(hmac::HMAC_SHA256, bytes)))
            }
            Algorithm::A256GCM => Material::Aes(cipher(bytes)?),
            Algorithm::XSalsa20Poly1305 | Algorithm::X25519 => Material::Secret(Zeroizing::new(
                bytes
                    .try_into()
                    .map_err(|_| anyhow!("the key needs exactly 32 bytes"))?,
            )),
        };
        Ok(Self {
            algorithm,
            inner,
            retained_bytes: bytes.len().saturating_mul(8).saturating_add(8192),
            created_at: Instant::now(),
        })
    }

    /// Conservative retained size: what the cache budget charges.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn sign(&self, message: &[u8]) -> Result<Vec<u8>> {
        Ok(match &self.inner {
            Material::Ed { sign, .. } => sign.sign(message).as_ref().to_vec(),
            Material::P256 { sign, .. } => {
                // RFC 6979: deterministic, so queries stay pure.
                let signature: Signature = sign
                    .try_sign(message)
                    .map_err(|_| anyhow!("P256 signing failed"))?;
                signature.to_bytes().to_vec()
            }
            Material::Rsa { sign, .. } => {
                let mut signature = vec![0; sign.public_modulus_len()];
                sign.sign(
                    &signature::RSA_PKCS1_SHA256,
                    &aws_lc_rs::rand::SystemRandom::new(),
                    message,
                    &mut signature,
                )
                .map_err(|_| anyhow!("RSA signing failed"))?;
                signature
            }
            Material::Hmac(key) => hmac::sign(key, message).as_ref().to_vec(),
            _ => bail!("this key cannot sign"),
        })
    }

    fn verify(&self, message: &[u8], signature: &[u8], strict: bool) -> bool {
        match &self.inner {
            Material::Ed { strict: key, .. } if strict => <&[u8; 64]>::try_from(signature)
                .ok()
                .is_some_and(|signature| {
                    key.verify_strict(message, &ed25519_dalek::Signature::from_bytes(signature))
                        .is_ok()
                }),
            Material::Ed { verify, .. }
            | Material::P256 { verify, .. }
            | Material::Rsa { verify, .. } => verify.verify_sig(message, signature).is_ok(),
            Material::Hmac(key) => hmac::verify(key, message, signature).is_ok(),
            _ => false,
        }
    }

    fn public(&self) -> Result<Vec<u8>> {
        Ok(match &self.inner {
            Material::Ed { sign, .. } => sign.public_key().as_ref().to_vec(),
            Material::P256 { sign, .. } => sign
                .verifying_key()
                .to_public_key_der()
                .map_err(|_| anyhow!("encode P256 public key"))?
                .as_bytes()
                .to_vec(),
            Material::Rsa { sign, .. } => AsDer::<PublicKeyX509Der>::as_der(sign.public_key())
                .map_err(|_| anyhow!("encode RSA public key"))?
                .as_ref()
                .to_vec(),
            Material::Secret(secret) if self.algorithm == Algorithm::X25519 => {
                x25519_dalek::x25519(**secret, x25519_dalek::X25519_BASEPOINT_BYTES).to_vec()
            }
            _ => bail!("this key has no public component"),
        })
    }

    fn modulus_bits(&self) -> Option<u64> {
        match &self.inner {
            Material::Rsa { sign, .. } => Some(sign.public_modulus_len() as u64 * 8),
            _ => None,
        }
    }
}

/// A resolved, authorized key version.
struct Resolved {
    domain: String,
    id: String,
    version: u64,
    algorithm: Algorithm,
    envelope: Envelope,
}

/// What the prepared-key cache knows a resolved version by: the whole
/// authenticated envelope and what it belongs to.
fn cache_identity(resolved: &Resolved) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(
        serde_json::to_vec(&json!([
            resolved.domain,
            resolved.id,
            resolved.version,
            resolved.algorithm,
            resolved.envelope,
        ]))
        .expect("cache identity serializes"),
    );
    digest.finalize().into()
}

/// One module's key declaration, from its manifest's `keys` member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Declaration {
    /// The declared alias.
    pub name: String,
    /// The declared algorithm.
    pub algorithm: Algorithm,
    /// The usages the module needs.
    pub usages: BTreeSet<String>,
}

/// Parse a manifest's `keys` member: `[{name, algorithm, usages}]`.
/// Declarations that repeat a name must agree.
pub fn declarations(keys: &flower_sdk::Value) -> Result<BTreeMap<String, Declaration>> {
    use flower_sdk::Value as V;
    let V::Array(items) = keys else {
        bail!("manifest keys must be an array");
    };
    let mut declared: BTreeMap<String, Declaration> = BTreeMap::new();
    for item in items {
        let V::Object(map) = item else {
            bail!("a key declaration must be an object");
        };
        let Some(V::String(name)) = map.get("name") else {
            bail!("a key declaration needs a name");
        };
        safe_name(name).map_err(anyhow::Error::msg)?;
        let Some(V::String(algorithm)) = map.get("algorithm") else {
            bail!("a key declaration needs an algorithm");
        };
        let algorithm = Algorithm::parse(algorithm)
            .with_context(|| format!("unknown key algorithm {algorithm:?}"))?;
        let Some(V::Array(usages)) = map.get("usages") else {
            bail!("a key declaration needs usages");
        };
        let mut set = BTreeSet::new();
        for usage in usages {
            let V::String(usage) = usage else {
                bail!("key usages are strings");
            };
            ensure!(
                algorithm.usages().contains(&usage.as_str()),
                "{} keys cannot {usage}",
                algorithm.name()
            );
            set.insert(usage.clone());
        }
        ensure!(!set.is_empty(), "a key declaration needs usages");
        let declaration = Declaration {
            name: name.clone(),
            algorithm,
            usages: set,
        };
        if let Some(existing) = declared.get(name) {
            ensure!(
                *existing == declaration,
                "conflicting declarations of key {name:?}"
            );
        }
        declared.insert(name.clone(), declaration);
    }
    Ok(declared)
}

/// What a managed operation returns.
pub enum ManagedOutput {
    /// Non-exportable shared material, held only by the invocation host.
    Shared(SharedKey),
    /// Bytes.
    Bytes(Vec<u8>),
    /// Text: a compact JOSE token.
    Text(String),
    /// JSON: verified claims and headers, or version metadata.
    Json(Value),
    /// A verification bit.
    Bool(bool),
    /// An authentication failure that opens nothing.
    Null,
}

/// A derived key with its origin pinned for authorization on every use.
/// There is deliberately no accessor for its secret material.
pub struct SharedKey {
    origin: Resolved,
    secret: Zeroizing<Vec<u8>>,
}

impl SharedKey {
    /// Conservative native retention charge, including the origin envelope.
    pub fn retained_bytes(&self) -> usize {
        self.secret.len()
            + serde_json::to_vec(&self.origin.envelope).map_or(0, |v| v.len())
            + self.origin.domain.len()
            + self.origin.id.len()
            + 1024
    }
}

struct CacheEntry {
    value: Arc<PreparedKey>,
    bytes: usize,
    touched: u64,
}

type ColdLoad = OnceLock<std::result::Result<Arc<PreparedKey>, String>>;
// Identity, weak/strong handles, synchronization state and map overhead.
const COLD_LOAD_BYTES: usize = 512;

#[derive(Default)]
struct Cache {
    entries: HashMap<[u8; 32], CacheEntry>,
    served: HashSet<[u8; 32]>,
    // Weak references retain neither material nor failures after the
    // last waiter leaves. Dead slots are pruned on the next cold load.
    loading: HashMap<[u8; 32], Weak<ColdLoad>>,
    bytes: usize,
    tick: u64,
    hits: u64,
    misses: u64,
    loads: u64,
    evictions: u64,
    coalesced: u64,
}

impl Cache {
    fn flight_bytes(&mut self) -> usize {
        self.loading.retain(|_, load| load.strong_count() != 0);
        self.loading.len().saturating_mul(COLD_LOAD_BYTES)
    }

    fn flight(&mut self, identity: [u8; 32], budget: usize) -> Option<Arc<ColdLoad>> {
        let reserved = self.flight_bytes();
        if let Some(load) = self.loading.get(&identity).and_then(Weak::upgrade) {
            self.coalesced += 1;
            return Some(load);
        }
        let reserved = reserved.saturating_add(COLD_LOAD_BYTES);
        if reserved > budget {
            return None;
        }
        while self.bytes.saturating_add(reserved) > budget && self.evict_oldest() {}
        let load = Arc::new(OnceLock::new());
        self.loading.insert(identity, Arc::downgrade(&load));
        Some(load)
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(oldest) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(key, _)| *key)
        else {
            return false;
        };
        if let Some(old) = self.entries.remove(&oldest) {
            self.bytes -= old.bytes;
            self.evictions += 1;
        }
        true
    }
}

/// What preparing an operator request produced.
pub enum Prepared {
    /// A change to propose through the log.
    Change(Vec<u8>),
    /// A response needing no proposal: reads, and retries of requests
    /// already committed.
    Response(Value),
}

/// One member's managed-key runtime over the replicated catalogs.
pub struct ManagedKeys {
    /// Mounted wrapping keys; the first wraps new material.
    wrapping: Vec<WrappingKey>,
    /// Whether a current wrapping key is mounted.
    active: bool,
    /// Applied catalogs by database; a database without one has the
    /// empty catalog.
    catalogs: RwLock<HashMap<Vec<u8>, Arc<Catalog>>>,
    cache: Mutex<Cache>,
    budget: usize,
    ttl: Option<Duration>,
}

impl ManagedKeys {
    /// A runtime wrapping new material under `active`, also unwrapping
    /// envelopes `previous` keys wrapped (for wrapping-key rotation).
    /// Without an active key the member applies and reports the
    /// catalog but serves and prepares nothing that needs material.
    pub fn new(active: Option<WrappingKey>, previous: Vec<WrappingKey>) -> Self {
        let has_active = active.is_some();
        let mut wrapping: Vec<WrappingKey> = active.into_iter().collect();
        wrapping.extend(previous);
        ManagedKeys {
            wrapping,
            active: has_active,
            catalogs: RwLock::new(HashMap::new()),
            cache: Mutex::new(Cache::default()),
            budget: DEFAULT_CACHE_BYTES,
            ttl: None,
        }
    }

    /// Set the cache's context and flight budget (zero disables both, not
    /// crypto) and context lifetime (`None` never expires).
    pub fn with_cache(mut self, budget_bytes: usize, ttl: Option<Duration>) -> Self {
        self.budget = budget_bytes;
        self.ttl = ttl;
        self
    }

    /// Install database `db`'s applied catalog revision: the state
    /// machine calls this whenever the database's catalog row may have
    /// changed (a key change, a move's cutover, a retirement). Absent
    /// bytes are the empty catalog, and so are bytes that do not decode:
    /// the database then serves no keys, never the revision before.
    /// Prepared contexts no installed catalog serves any more leave the
    /// cache (see [`ManagedKeys::forget_unserved`]).
    pub fn install(&self, db: &[u8], bytes: Option<&[u8]>) -> std::result::Result<(), String> {
        let (catalog, outcome) = match bytes.map(|bytes| catalog::decode(Some(bytes))) {
            Some(Ok(catalog)) => (Some(catalog), Ok(())),
            Some(Err(error)) => (None, Err(error)),
            None => (None, Ok(())),
        };
        let mut catalogs = self
            .catalogs
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        match catalog {
            Some(catalog) => catalogs.insert(db.to_vec(), Arc::new(catalog)),
            None => catalogs.remove(db),
        };
        self.forget_unserved(&catalogs);
        outcome
    }

    /// Install every database's applied catalog at once, replacing
    /// whatever was installed: snapshot installs and startup. Rows that
    /// do not decode are left out, and named in the answer.
    pub fn install_all(&self, catalogs: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<String> {
        let mut decoded = HashMap::with_capacity(catalogs.len());
        let mut failures = Vec::new();
        for (db, bytes) in catalogs {
            match catalog::decode(Some(&bytes)) {
                Ok(catalog) => {
                    decoded.insert(db, Arc::new(catalog));
                }
                Err(error) => failures.push(format!("{}: {error}", String::from_utf8_lossy(&db))),
            }
        }
        let mut catalogs = self
            .catalogs
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        *catalogs = decoded;
        self.forget_unserved(&catalogs);
        failures
    }

    /// Drop every prepared context that no unrevoked version of an
    /// installed catalog holds: a database's catalog that went (a move's
    /// source retiring it), a version destroyed, revoked or rewrapped.
    /// Resolution never reaches such a context, but its unwrapped
    /// material would otherwise stay in memory until evicted. Called
    /// with the catalogs' write lock held, so no install interleaves.
    fn forget_unserved(&self, catalogs: &HashMap<Vec<u8>, Arc<Catalog>>) {
        let mut served = std::collections::HashSet::new();
        for catalog in catalogs.values() {
            for key in catalog.keys.values() {
                for (version, data) in &key.versions {
                    if let (false, Some(envelope)) = (data.revoked, &data.envelope) {
                        served.insert(cache_identity(&Resolved {
                            domain: catalog.domain.clone(),
                            id: key.id.clone(),
                            version: *version,
                            algorithm: key.algorithm,
                            envelope: envelope.clone(),
                        }));
                    }
                }
            }
        }
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        let mut freed = 0;
        cache.entries.retain(|identity, entry| {
            let keep = served.contains(identity);
            if !keep {
                freed += entry.bytes;
            }
            keep
        });
        cache.bytes -= freed;
        cache.served = served;
    }

    /// Database `db`'s applied catalog.
    pub fn catalog(&self, db: &[u8]) -> Arc<Catalog> {
        self.catalogs
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(db)
            .cloned()
            .unwrap_or_default()
    }

    /// Database `db`'s applied catalog without envelopes.
    pub fn public_catalog(&self, db: &[u8]) -> Value {
        catalog::public(&self.catalog(db))
    }

    /// Node-local cache statistics.
    pub fn cache_stats(&self) -> Value {
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        let flight_bytes = cache.flight_bytes();
        json!({
            "entries": cache.entries.len(),
            "bytes": cache.bytes,
            "budgetBytes": self.budget,
            "ttlMs": self.ttl.map(|ttl| ttl.as_millis() as u64),
            "hits": cache.hits,
            "misses": cache.misses,
            "loads": cache.loads,
            "evictions": cache.evictions,
            "flightBytes": flight_bytes,
            "coalesced": cache.coalesced,
        })
    }

    /// Check that every unrevoked version of every installed catalog
    /// unwraps and prepares under the mounted wrapping keys: a member
    /// about to serve keys checks its mount first.
    pub fn validate_ready(&self) -> Result<()> {
        let catalogs: Vec<(Vec<u8>, Arc<Catalog>)> = self
            .catalogs
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(db, catalog)| (db.clone(), catalog.clone()))
            .collect();
        for (db, catalog) in catalogs {
            self.ready(&catalog)
                .with_context(|| format!("database {:?}", String::from_utf8_lossy(&db)))?;
        }
        Ok(())
    }

    /// Check that stored catalog bytes would serve here: every
    /// unrevoked version unwraps and prepares under the mounted
    /// wrapping keys. A move's destination answers this before the
    /// move freezes its source and again before its cutover, so a
    /// database never lands where its keys stay locked.
    pub fn validate(&self, bytes: Option<&[u8]>) -> Result<()> {
        self.ready(&catalog::decode(bytes).map_err(anyhow::Error::msg)?)
    }

    fn ready(&self, catalog: &Catalog) -> Result<()> {
        for key in catalog.keys.values() {
            for (version, data) in &key.versions {
                if let (false, Some(envelope)) = (data.revoked, &data.envelope) {
                    self.prepared(&Resolved {
                        domain: catalog.domain.clone(),
                        id: key.id.clone(),
                        version: *version,
                        algorithm: key.algorithm,
                        envelope: envelope.clone(),
                    })?;
                }
            }
        }
        Ok(())
    }

    fn active_key(&self) -> Result<&WrappingKey> {
        if !self.active {
            bail!("KEYS_LOCKED: no wrapping key is mounted");
        }
        self.wrapping.first().context("no wrapping key is mounted")
    }

    fn wrapping_key(&self, id: &str) -> Result<&WrappingKey> {
        self.wrapping
            .iter()
            .find(|key| key.id == id)
            .context("KEYS_LOCKED: the envelope's wrapping key is not mounted")
    }

    /// Seal material in a fresh envelope under the active wrapping key.
    fn envelope(
        &self,
        domain: &str,
        id: &str,
        version: u64,
        algorithm: Algorithm,
        bytes: &[u8],
    ) -> Result<Envelope> {
        let provider = self.active_key()?;
        let dek = Zeroizing::new(random::<32>());
        let nonce = random::<12>();
        let ciphertext = seal(
            &cipher(&*dek)?,
            nonce,
            &aad(domain, id, version, algorithm, "material"),
            bytes,
        )?;
        let wrapping_nonce = random::<12>();
        let wrapped = seal(
            &provider.cipher,
            wrapping_nonce,
            &aad(domain, id, version, algorithm, "dek"),
            &*dek,
        )?;
        Ok(Envelope {
            wrapping_id: provider.id.clone(),
            wrapped_dek: URL_SAFE_NO_PAD.encode(wrapped),
            wrapping_nonce: URL_SAFE_NO_PAD.encode(wrapping_nonce),
            ciphertext: URL_SAFE_NO_PAD.encode(ciphertext),
            nonce: URL_SAFE_NO_PAD.encode(nonce),
        })
    }

    /// Unwrap a resolved version's material.
    fn unwrap(&self, resolved: &Resolved) -> Result<Zeroizing<Vec<u8>>> {
        let envelope = &resolved.envelope;
        let provider = self.wrapping_key(&envelope.wrapping_id)?;
        let dek = open(
            &provider.cipher,
            nonce(&envelope.wrapping_nonce)?,
            &aad(
                &resolved.domain,
                &resolved.id,
                resolved.version,
                resolved.algorithm,
                "dek",
            ),
            &decode(&envelope.wrapped_dek)?,
        )?;
        ensure!(dek.len() == 32, "invalid wrapped data key");
        open(
            &cipher(&dek)?,
            nonce(&envelope.nonce)?,
            &aad(
                &resolved.domain,
                &resolved.id,
                resolved.version,
                resolved.algorithm,
                "material",
            ),
            &decode(&envelope.ciphertext)?,
        )
    }

    /// The prepared context for a resolved version: from the cache when
    /// retained and fresh, else unwrapped and parsed outside the lock.
    fn prepared(&self, resolved: &Resolved) -> Result<Arc<PreparedKey>> {
        let identity = cache_identity(resolved);
        let loading = {
            let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            cache.tick += 1;
            let tick = cache.tick;
            let fresh = |prepared: &Arc<PreparedKey>| {
                self.ttl
                    .is_none_or(|ttl| prepared.created_at.elapsed() < ttl)
            };
            match cache.entries.get_mut(&identity) {
                Some(entry) if fresh(&entry.value) => {
                    entry.touched = tick;
                    let value = entry.value.clone();
                    cache.hits += 1;
                    return Ok(value);
                }
                Some(_) => {
                    if let Some(old) = cache.entries.remove(&identity) {
                        cache.bytes -= old.bytes;
                        cache.evictions += 1;
                    }
                }
                None => {}
            }
            cache.misses += 1;
            cache.flight(identity, self.budget)
        };
        let Some(loading) = loading else {
            // A full or disabled flight registry never refuses valid crypto.
            return self.load_prepared(resolved, identity);
        };
        loading
            .get_or_init(|| {
                self.load_prepared(resolved, identity)
                    .map_err(|error| format!("{error:#}"))
            })
            .clone()
            .map_err(anyhow::Error::msg)
    }

    fn load_prepared(&self, resolved: &Resolved, identity: [u8; 32]) -> Result<Arc<PreparedKey>> {
        let bytes = self.unwrap(resolved)?;
        let prepared = Arc::new(PreparedKey::prepare(resolved.algorithm, &bytes)?);
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        cache.loads += 1;
        let weight = prepared.retained_bytes();
        let flight_bytes = cache.flight_bytes();
        // A catalog install may revoke or rewrap this envelope while
        // it loads. Never reinsert material the installer just evicted.
        if weight.saturating_add(flight_bytes) <= self.budget && cache.served.contains(&identity) {
            cache.tick += 1;
            let tick = cache.tick;
            if let Some(old) = cache.entries.insert(
                identity,
                CacheEntry {
                    value: prepared.clone(),
                    bytes: weight,
                    touched: tick,
                },
            ) {
                cache.bytes -= old.bytes;
            }
            cache.bytes += weight;
            while cache.bytes.saturating_add(flight_bytes) > self.budget && cache.evict_oldest() {}
        }
        Ok(prepared)
    }

    /// Prepare an operator request against the stored catalog bytes.
    ///
    /// Requests are JSON objects with an `operation`: `list` and
    /// `cache` read; `generate` (`name`, `algorithm`, RSA `bits`),
    /// `import` (`name`, `algorithm`, a `sealed` envelope from
    /// [`seal_import`]), `bind` (alias `name`, `key`, `usages`),
    /// `unbind`, `rotate` (RSA `bits`), `retire`, `revoke` and
    /// `destroy` (optional `version`, else every version) and `rewrap`
    /// (optional `name`, else every key) change the catalog and need a
    /// `requestId`. A request ID already committed with the same
    /// contents returns its receipt as a duplicate.
    pub fn prepare(&self, current: Option<&[u8]>, request: &Value) -> Result<Prepared> {
        let base = catalog::decode(current).map_err(anyhow::Error::msg)?;
        let operation = text(request, "operation")?;
        match operation {
            "list" => {
                return Ok(Prepared::Response(json!({
                    "revision": base.revision,
                    "value": catalog::public(&base),
                    "duplicate": false,
                })));
            }
            "cache" => return Ok(Prepared::Response(self.cache_stats())),
            _ => {}
        }
        let request_id = text(request, "requestId")?;
        safe_name(request_id).map_err(anyhow::Error::msg)?;
        let digest: String = Sha256::digest(serde_json::to_vec(request)?)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if let Some(receipt) = catalog::receipt(&base, request_id) {
            ensure!(
                receipt.digest == digest,
                "KEY_REQUEST_REUSED: request ID {request_id:?} already committed different contents"
            );
            return Ok(Prepared::Response(json!({
                "revision": receipt.revision,
                "value": catalog::public(&base),
                "duplicate": true,
            })));
        }
        let mut next = base.clone();
        next.receipts.clear();
        if next.domain.is_empty() {
            next.domain = URL_SAFE_NO_PAD.encode(random::<16>());
        }
        let bits = request
            .get("bits")
            .map(|value| value.as_u64().context("bits must be a whole number"))
            .transpose()?;
        ensure!(
            bits.is_none() || matches!(operation, "generate" | "rotate"),
            "bits is only valid when generating or rotating"
        );
        match operation {
            "generate" | "import" => {
                let name = text(request, "name")?;
                safe_name(name).map_err(anyhow::Error::msg)?;
                ensure!(!next.keys.contains_key(name), "key {name:?} already exists");
                let algorithm = Algorithm::parse(text(request, "algorithm")?)
                    .context("unknown key algorithm")?;
                let bytes = if operation == "generate" {
                    generate(algorithm, bits)?
                } else {
                    let sealed = request
                        .get("sealed")
                        .context("import needs a sealed envelope")?;
                    let (bytes, format) = self.open_import(sealed)?;
                    canonical(algorithm, &bytes, &format)?
                };
                PreparedKey::prepare(algorithm, &bytes)?;
                let id = URL_SAFE_NO_PAD.encode(random::<16>());
                let envelope = self.envelope(&next.domain, &id, 1, algorithm, &bytes)?;
                next.keys.insert(
                    name.to_string(),
                    KeyRecord {
                        id,
                        algorithm,
                        active_version: 1,
                        versions: BTreeMap::from([(
                            1,
                            Version {
                                envelope: Some(envelope),
                                revoked: false,
                                retired: false,
                            },
                        )]),
                    },
                );
            }
            "bind" => {
                let name = text(request, "name")?;
                safe_name(name).map_err(anyhow::Error::msg)?;
                let key_name = text(request, "key")?;
                let key = next
                    .keys
                    .get(key_name)
                    .with_context(|| format!("unknown key {key_name:?}"))?;
                let usages: BTreeSet<String> =
                    serde_json::from_value(request.get("usages").cloned().unwrap_or(Value::Null))
                        .context("usages must be an array of strings")?;
                ensure!(
                    !usages.is_empty()
                        && usages
                            .iter()
                            .all(|usage| key.algorithm.usages().contains(&usage.as_str())),
                    "{} keys support only {:?}",
                    key.algorithm.name(),
                    key.algorithm.usages()
                );
                next.bindings.insert(
                    name.to_string(),
                    catalog::Binding {
                        key: key_name.to_string(),
                        usages,
                    },
                );
            }
            "unbind" => {
                let name = text(request, "name")?;
                ensure!(
                    next.bindings.remove(name).is_some(),
                    "unknown key binding {name:?}"
                );
            }
            "rotate" => {
                let name = text(request, "name")?;
                let domain = next.domain.clone();
                let key = next
                    .keys
                    .get_mut(name)
                    .with_context(|| format!("unknown key {name:?}"))?;
                let version = key
                    .versions
                    .last_key_value()
                    .and_then(|(version, _)| version.checked_add(1))
                    .context("key versions exhausted")?;
                let bits = match (key.algorithm, bits) {
                    (Algorithm::Rsa, None) => {
                        // Keep the current size: read it from the
                        // active version, which needs its material.
                        let active = key
                            .versions
                            .get(&key.active_version)
                            .and_then(|version| version.envelope.clone())
                            .context("rotating a destroyed RSA key needs explicit bits")?;
                        let prepared = self.prepared(&Resolved {
                            domain: domain.clone(),
                            id: key.id.clone(),
                            version: key.active_version,
                            algorithm: key.algorithm,
                            envelope: active,
                        })?;
                        prepared.modulus_bits()
                    }
                    (_, bits) => bits,
                };
                let bytes = generate(key.algorithm, bits)?;
                let envelope = self.envelope(&domain, &key.id, version, key.algorithm, &bytes)?;
                key.versions.insert(
                    version,
                    Version {
                        envelope: Some(envelope),
                        revoked: false,
                        retired: false,
                    },
                );
                key.active_version = version;
            }
            "retire" | "revoke" | "destroy" | "rewrap" => {
                let selected = request
                    .get("version")
                    .map(|value| {
                        value
                            .as_u64()
                            .filter(|version| *version > 0)
                            .context("version must be a positive whole number")
                    })
                    .transpose()?;
                let names: Vec<String> = match request.get("name") {
                    Some(_) => {
                        let name = text(request, "name")?;
                        ensure!(next.keys.contains_key(name), "unknown key {name:?}");
                        vec![name.to_string()]
                    }
                    None if operation == "rewrap" => next.keys.keys().cloned().collect(),
                    None => bail!("{operation} needs a key name"),
                };
                ensure!(
                    selected.is_none() || names.len() == 1,
                    "a version selects within one key"
                );
                let domain = next.domain.clone();
                for name in names {
                    let key = next.keys.get_mut(&name).expect("checked above");
                    if let Some(version) = selected {
                        ensure!(
                            key.versions.contains_key(&version),
                            "unknown version {version} of key {name:?}"
                        );
                    }
                    for (number, version) in &mut key.versions {
                        if selected.is_some_and(|selected| selected != *number) {
                            continue;
                        }
                        match operation {
                            "retire" => version.retired = true,
                            "revoke" => version.revoked = true,
                            "destroy" => {
                                version.revoked = true;
                                version.retired = true;
                                version.envelope = None;
                            }
                            _ => {
                                let Some(envelope) = version.envelope.as_mut() else {
                                    continue;
                                };
                                let provider = self.active_key()?;
                                let old = self.wrapping_key(&envelope.wrapping_id)?;
                                let aad = aad(&domain, &key.id, *number, key.algorithm, "dek");
                                let dek = open(
                                    &old.cipher,
                                    nonce(&envelope.wrapping_nonce)?,
                                    &aad,
                                    &decode(&envelope.wrapped_dek)?,
                                )?;
                                ensure!(dek.len() == 32, "invalid wrapped data key");
                                let wrapping_nonce = random::<12>();
                                envelope.wrapped_dek = URL_SAFE_NO_PAD.encode(seal(
                                    &provider.cipher,
                                    wrapping_nonce,
                                    &aad,
                                    &dek,
                                )?);
                                envelope.wrapping_nonce = URL_SAFE_NO_PAD.encode(wrapping_nonce);
                                envelope.wrapping_id = provider.id.clone();
                            }
                        }
                    }
                }
            }
            other => bail!("unknown key operation {other:?}"),
        }
        next.revision = base
            .revision
            .checked_add(1)
            .context("key catalog revision exhausted")?;
        Ok(Prepared::Change(catalog::encode_change(&CatalogChange {
            request_id: request_id.to_string(),
            digest,
            base_revision: base.revision,
            catalog: next,
        })))
    }

    fn open_import(&self, sealed: &Value) -> Result<(Zeroizing<Vec<u8>>, String)> {
        ensure!(
            sealed.get("version") == Some(&json!(1)),
            "unsupported import envelope version"
        );
        let provider = self.wrapping_key(text(sealed, "wrappingId")?)?;
        let mut bytes = open(
            &provider.cipher,
            nonce(text(sealed, "nonce")?)?,
            IMPORT_AAD,
            &decode(text(sealed, "ciphertext")?)?,
        )?;
        let format = match bytes.first() {
            Some(0) => "raw",
            Some(1) => "pem",
            Some(2) => "der",
            _ => bail!("invalid import envelope format"),
        };
        bytes.remove(0);
        Ok((bytes, format.to_string()))
    }

    /// Resolve an alias for an operation against database `db`'s
    /// applied catalog: the binding must grant, and the module's
    /// declaration must ask for, the operation's usage over the
    /// declared algorithm. `selector` pins a version by number or JOSE
    /// `kid`; historical and retired versions only verify, decrypt and
    /// disclose.
    fn resolve(
        &self,
        db: &[u8],
        declaration: &Declaration,
        operation: &str,
        selector: Selector<'_>,
    ) -> Result<(Resolved, String)> {
        let catalog = self.catalog(db);
        let binding = catalog.bindings.get(&declaration.name).with_context(|| {
            format!(
                "UNKNOWN_KEY: managed key {:?} is not bound",
                declaration.name
            )
        })?;
        let key = catalog
            .keys
            .get(&binding.key)
            .context("UNKNOWN_KEY: the binding names a missing key")?;
        ensure!(
            key.algorithm == declaration.algorithm,
            "FORBIDDEN_USAGE: the bound key is {} but the module declared {}",
            key.algorithm.name(),
            declaration.algorithm.name()
        );
        let usage = operation_usage(operation)?;
        let mut permitted: BTreeSet<String> = binding
            .usages
            .intersection(&declaration.usages)
            .cloned()
            .collect();
        let allowed = |permitted: &BTreeSet<String>| {
            if usage == "metadata" {
                !permitted.is_empty()
            } else {
                permitted.contains(usage)
            }
        };
        ensure!(
            allowed(&permitted),
            "FORBIDDEN_USAGE: {operation} needs the {usage} usage on {:?}",
            declaration.name
        );
        let version = match selector {
            Selector::Active => key.active_version,
            Selector::Version(version) => version,
            Selector::Kid(selector) => {
                let prefix = format!("flower.{}.", key.id);
                let version = selector
                    .strip_prefix(&prefix)
                    .context("UNKNOWN_KEY: the token's kid is outside the bound key")?
                    .parse::<u64>()
                    .context("UNKNOWN_KEY: invalid managed key version")?;
                ensure!(
                    selector == kid(&key.id, version),
                    "UNKNOWN_KEY: invalid managed key version"
                );
                version
            }
        };
        let data = key
            .versions
            .get(&version)
            .with_context(|| format!("UNKNOWN_VERSION: unknown key version {version}"))?;
        ensure!(
            !data.revoked,
            "KEY_REVOKED: key version {version} is revoked"
        );
        let envelope = data
            .envelope
            .clone()
            .with_context(|| format!("KEY_DESTROYED: key version {version} is destroyed"))?;
        if data.retired || version != key.active_version {
            permitted.retain(|usage| !matches!(usage.as_str(), "sign" | "encrypt" | "derive"));
            ensure!(
                allowed(&permitted),
                "FORBIDDEN_USAGE: historical or retired key versions cannot sign, encrypt or derive"
            );
        }
        let token = kid(&key.id, version);
        Ok((
            Resolved {
                domain: catalog.domain.clone(),
                id: key.id.clone(),
                version,
                algorithm: key.algorithm,
                envelope,
            },
            token,
        ))
    }

    /// Run a managed operation for the declaration of a module in
    /// database `db`, against that database's catalog. `version` pins a
    /// version (`None` is the active one; JWT verification and
    /// decryption select by the token's `kid`). Arguments are raw
    /// bytes; `options` is a JSON object; `now_ms` is the invocation
    /// clock JWT validation reads.
    #[allow(clippy::too_many_arguments)]
    pub fn execute(
        &self,
        db: &[u8],
        declaration: &Declaration,
        operation: &str,
        version: Option<u64>,
        args: &[&[u8]],
        options: &Value,
        now_ms: u64,
    ) -> Result<ManagedOutput> {
        let requested;
        let selector = if matches!(operation, "jwt.verify" | "jwt.decrypt") {
            ensure!(version.is_none(), "the token's kid selects the version");
            let token = std::str::from_utf8(args.first().copied().unwrap_or_default())
                .context("JWT tokens are UTF-8")?;
            let header = jwt::managed_header(token, operation == "jwt.decrypt")?;
            requested = header
                .get("kid")
                .and_then(Value::as_str)
                .context("UNKNOWN_KEY: managed JWT requires a versioned kid")?
                .to_string();
            Selector::Kid(&requested)
        } else {
            version.map_or(Selector::Active, Selector::Version)
        };
        let (resolved, kid) = self.resolve(db, declaration, operation, selector)?;
        if operation == "version" {
            return Ok(ManagedOutput::Json(json!({
                "version": resolved.version,
                "kid": kid,
            })));
        }
        let key = self.prepared(&resolved)?;
        if operation == "nacl.box.before" {
            ensure!(
                key.algorithm == Algorithm::X25519,
                "NaCl box needs an X25519 key"
            );
            let Material::Secret(secret) = &key.inner else {
                unreachable!()
            };
            let peer = args.first().context("missing peer public key")?;
            let nacl::Output::Bytes(secret) = nacl::execute(5, &[peer, secret.as_ref()])? else {
                bail!("invalid shared-key result")
            };
            return Ok(ManagedOutput::Shared(SharedKey {
                origin: resolved,
                secret: Zeroizing::new(secret),
            }));
        }
        execute_key(&key, operation, args, options, now_ms, &kid)
    }

    /// Use a shared handle after rechecking the original key's current
    /// binding, declaration and lifecycle. A derive grant alone never
    /// grants encryption or decryption.
    pub fn execute_shared(
        &self,
        db: &[u8],
        declaration: &Declaration,
        shared: &SharedKey,
        operation: &str,
        message: &[u8],
        nonce: &[u8],
    ) -> Result<ManagedOutput> {
        let op = match operation {
            "nacl.box.after" => 1,
            "nacl.box.open.after" => 2,
            _ => bail!("FORBIDDEN_USAGE: a shared key only encrypts or decrypts"),
        };
        let (current, _) = self.resolve(
            db,
            declaration,
            operation,
            Selector::Version(shared.origin.version),
        )?;
        ensure!(
            current.domain == shared.origin.domain
                && current.id == shared.origin.id
                && current.algorithm == shared.origin.algorithm,
            "FORBIDDEN_USAGE: shared key origin no longer matches the binding"
        );
        Ok(from_nacl(nacl::execute(
            op,
            &[message, nonce, &shared.secret],
        )?))
    }
}

/// How an operation selects a version.
#[derive(Clone, Copy)]
enum Selector<'a> {
    Active,
    Version(u64),
    Kid(&'a str),
}

/// The usage an operation needs.
fn operation_usage(operation: &str) -> Result<&'static str> {
    Ok(match operation {
        "sign" | "jwt.sign" | "nacl.sign" | "nacl.sign.detached" => "sign",
        "verify" | "jwt.verify" | "nacl.sign.open" | "nacl.sign.detached.verify" => "verify",
        "jwt.encrypt" | "nacl.secretbox" | "nacl.box" | "nacl.box.after" => "encrypt",
        "jwt.decrypt" | "nacl.secretbox.open" | "nacl.box.open" | "nacl.box.open.after" => {
            "decrypt"
        }
        "nacl.box.before" => "derive",
        "publicKey" | "nacl.scalarMult.base" => "publicKey",
        "version" => "metadata",
        "nacl.scalarMult" => {
            bail!("FORBIDDEN_USAGE: managed shared secrets cannot be exported")
        }
        other => bail!("unsupported managed crypto operation {other:?}"),
    })
}

fn execute_key(
    key: &PreparedKey,
    operation: &str,
    args: &[&[u8]],
    options: &Value,
    now_ms: u64,
    kid: &str,
) -> Result<ManagedOutput> {
    let arg = |index: usize| {
        args.get(index)
            .copied()
            .with_context(|| format!("missing managed crypto argument {index}"))
    };
    let mut options = if options.is_null() {
        json!({})
    } else {
        options.clone()
    };
    ensure!(
        options.is_object(),
        "managed crypto options must be an object"
    );
    let jwt_algorithm = || -> Result<jwt::Algorithm> {
        Ok(match key.algorithm {
            Algorithm::Ed25519 => jwt::Algorithm::EdDSA,
            Algorithm::P256 => jwt::Algorithm::ES256,
            Algorithm::Rsa => jwt::Algorithm::RS256,
            Algorithm::HS256 => jwt::Algorithm::HS256,
            _ => bail!("this key cannot sign a JWT"),
        })
    };
    let key_format = |algorithm| {
        json!(if algorithm == jwt::Algorithm::HS256 {
            "raw"
        } else {
            "der"
        })
    };
    Ok(match operation {
        "sign" => ManagedOutput::Bytes(key.sign(arg(0)?)?),
        "verify" => ManagedOutput::Bool(key.verify(arg(0)?, arg(1)?, true)),
        "publicKey" | "nacl.scalarMult.base" => {
            if operation == "nacl.scalarMult.base" {
                ensure!(
                    key.algorithm == Algorithm::X25519,
                    "NaCl scalar multiplication needs an X25519 key"
                );
            }
            ManagedOutput::Bytes(key.public()?)
        }
        "jwt.sign" => {
            let algorithm = jwt_algorithm()?;
            ensure!(
                options.get("keyFormat").is_none(),
                "managed keys take no keyFormat"
            );
            if let Some(value) = options.get("algorithm") {
                ensure!(
                    *value == serde_json::to_value(algorithm)?,
                    "the JWT algorithm does not match the managed key"
                );
            }
            if let Some(value) = options.get("kid") {
                ensure!(
                    value.as_str() == Some(kid),
                    "the managed key version fixes the JWT kid"
                );
            }
            options["algorithm"] = serde_json::to_value(algorithm)?;
            options["keyFormat"] = key_format(algorithm);
            options["kid"] = json!(kid);
            let options: jwt::SignOptions = serde_json::from_value(options)?;
            ManagedOutput::Text(jwt::sign_with(
                &jwt::parse_json(arg(0)?)?,
                &options,
                |message| key.sign(message),
            )?)
        }
        "jwt.verify" => {
            let algorithm = jwt_algorithm()?;
            ensure!(
                options.get("keyFormat").is_none(),
                "managed keys take no keyFormat"
            );
            if options.get("algorithms").is_none() {
                options["algorithms"] = json!([algorithm]);
            }
            options["keyFormat"] = key_format(algorithm);
            let options: jwt::VerifyOptions = serde_json::from_value(options)?;
            ensure!(
                options.algorithms == vec![algorithm],
                "managed JWT algorithms must match the key"
            );
            let verified = jwt::verify_with(
                std::str::from_utf8(arg(0)?)?,
                &options,
                now_ms,
                |_, message, signature| Ok(key.verify(message, signature, false)),
            )?;
            ManagedOutput::Json(serde_json::to_value(verified)?)
        }
        "jwt.encrypt" => {
            let Material::Aes(cipher) = &key.inner else {
                bail!("JWE needs an A256GCM key")
            };
            if let Some(value) = options.get("kid") {
                ensure!(
                    value.as_str() == Some(kid),
                    "the managed key version fixes the JWT kid"
                );
            }
            options["kid"] = json!(kid);
            ManagedOutput::Text(jwt::encrypt_with(
                &jwt::parse_json(arg(0)?)?,
                cipher,
                arg(1)?,
                &serde_json::from_value(options)?,
            )?)
        }
        "jwt.decrypt" => {
            let Material::Aes(cipher) = &key.inner else {
                bail!("JWE needs an A256GCM key")
            };
            ManagedOutput::Json(serde_json::to_value(jwt::decrypt_with(
                std::str::from_utf8(arg(0)?)?,
                cipher,
                &serde_json::from_value(options)?,
                now_ms,
            )?)?)
        }
        "nacl.sign" | "nacl.sign.detached" => {
            ensure!(
                key.algorithm == Algorithm::Ed25519,
                "NaCl signatures need an Ed25519 key"
            );
            let mut signature = key.sign(arg(0)?)?;
            if operation == "nacl.sign" {
                signature.extend_from_slice(arg(0)?);
            }
            ManagedOutput::Bytes(signature)
        }
        "nacl.sign.detached.verify" => {
            ensure!(
                key.algorithm == Algorithm::Ed25519,
                "NaCl signatures need an Ed25519 key"
            );
            ensure!(arg(1)?.len() == 64, "invalid NaCl signature length");
            ManagedOutput::Bool(key.verify(arg(0)?, arg(1)?, true))
        }
        "nacl.sign.open" => {
            ensure!(
                key.algorithm == Algorithm::Ed25519,
                "NaCl signatures need an Ed25519 key"
            );
            let bytes = arg(0)?;
            if bytes.len() >= 64 && key.verify(&bytes[64..], &bytes[..64], true) {
                ManagedOutput::Bytes(bytes[64..].to_vec())
            } else {
                ManagedOutput::Null
            }
        }
        "nacl.secretbox" | "nacl.secretbox.open" => {
            ensure!(
                key.algorithm == Algorithm::XSalsa20Poly1305,
                "NaCl secretbox needs an XSalsa20Poly1305 key"
            );
            let Material::Secret(secret) = &key.inner else {
                bail!("NaCl secretbox needs an XSalsa20Poly1305 key")
            };
            let op = if operation == "nacl.secretbox" { 1 } else { 2 };
            from_nacl(nacl::execute(op, &[arg(0)?, arg(1)?, secret.as_ref()])?)
        }
        "nacl.box" | "nacl.box.open" => {
            ensure!(
                key.algorithm == Algorithm::X25519,
                "NaCl box needs an X25519 key"
            );
            let Material::Secret(secret) = &key.inner else {
                bail!("NaCl box needs an X25519 key")
            };
            let op = if operation == "nacl.box" { 6 } else { 7 };
            from_nacl(nacl::execute(
                op,
                &[arg(0)?, arg(1)?, arg(2)?, secret.as_ref()],
            )?)
        }
        other => bail!("unsupported managed crypto operation {other:?}"),
    })
}

fn from_nacl(value: nacl::Output) -> ManagedOutput {
    match value {
        nacl::Output::Bytes(bytes) => ManagedOutput::Bytes(bytes),
        nacl::Output::Bool(value) => ManagedOutput::Bool(value),
        nacl::Output::Null => ManagedOutput::Null,
    }
}

fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value
        .get(name)
        .and_then(Value::as_str)
        .with_context(|| format!("missing or invalid {name}"))
}

/// Generate fresh material for an algorithm.
fn generate(algorithm: Algorithm, bits: Option<u64>) -> Result<Zeroizing<Vec<u8>>> {
    if algorithm != Algorithm::Rsa {
        ensure!(bits.is_none(), "bits is only valid for RSA");
    }
    Ok(Zeroizing::new(match algorithm {
        Algorithm::P256 => aws_lc_rs::signature::EcdsaKeyPair::generate(
            &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
        )
        .map_err(|_| anyhow!("P256 key generation failed"))?
        .to_pkcs8v1()
        .map_err(|_| anyhow!("encode P256 key"))?
        .as_ref()
        .to_vec(),
        Algorithm::Rsa => {
            use aws_lc_rs::rsa::KeySize;
            let size = match bits.unwrap_or(2048) {
                2048 => KeySize::Rsa2048,
                3072 => KeySize::Rsa3072,
                4096 => KeySize::Rsa4096,
                8192 => KeySize::Rsa8192,
                _ => bail!("RSA generation supports 2048, 3072, 4096 or 8192 bits"),
            };
            let key =
                RsaKeyPair::generate(size).map_err(|_| anyhow!("RSA key generation failed"))?;
            AsDer::<Pkcs8V1Der>::as_der(&key)
                .map_err(|_| anyhow!("encode RSA key"))?
                .as_ref()
                .to_vec()
        }
        _ => random::<32>().to_vec(),
    }))
}

/// Canonicalize imported material into the catalog's stored form.
fn canonical(algorithm: Algorithm, bytes: &[u8], format: &str) -> Result<Zeroizing<Vec<u8>>> {
    ensure!(
        matches!(format, "raw" | "pem" | "der"),
        "key format must be raw, pem or der"
    );
    if matches!(
        algorithm,
        Algorithm::HS256 | Algorithm::A256GCM | Algorithm::XSalsa20Poly1305 | Algorithm::X25519
    ) {
        ensure!(
            format == "raw",
            "symmetric and X25519 keys import in raw format"
        );
        if algorithm == Algorithm::HS256 {
            let start = bytes
                .iter()
                .position(|byte| !byte.is_ascii_whitespace())
                .unwrap_or(bytes.len());
            ensure!(
                !bytes[start..].starts_with(b"-----BEGIN "),
                "PEM key material cannot be an HS256 secret"
            );
            ensure!(bytes.len() >= 32, "HS256 needs at least 32 bytes");
        } else {
            ensure!(bytes.len() == 32, "the key needs exactly 32 bytes");
        }
        return Ok(Zeroizing::new(bytes.to_vec()));
    }
    let der = if format == "pem" {
        crate::der::validate_pem(bytes)?;
        let pem = pem::parse(bytes).context("invalid private PEM")?;
        ensure!(
            pem.tag() == "PRIVATE KEY"
                || (algorithm == Algorithm::Rsa && pem.tag() == "RSA PRIVATE KEY"),
            "import needs a private PKCS#8 or RSA private key"
        );
        Zeroizing::new(pem.into_contents())
    } else {
        Zeroizing::new(bytes.to_vec())
    };
    if format != "raw" {
        crate::der::validate_der(&der)?;
    }
    Ok(Zeroizing::new(match algorithm {
        Algorithm::Ed25519 => {
            if format == "raw" {
                if let Ok(secret) = <&[u8; 64]>::try_from(bytes) {
                    ed25519_dalek::SigningKey::from_keypair_bytes(secret)
                        .map_err(|_| {
                            anyhow!("the Ed25519 secret key has inconsistent public bytes")
                        })?
                        .to_bytes()
                        .to_vec()
                } else {
                    ensure!(
                        bytes.len() == 32,
                        "raw Ed25519 imports are a 32-byte seed or a 64-byte secret key"
                    );
                    bytes.to_vec()
                }
            } else {
                Ed25519KeyPair::from_pkcs8(&der)
                    .map_err(|_| anyhow!("invalid Ed25519 private key"))?
                    .seed()
                    .map_err(|_| anyhow!("read Ed25519 seed"))?
                    .as_be_bytes()
                    .map_err(|_| anyhow!("encode Ed25519 seed"))?
                    .as_ref()
                    .to_vec()
            }
        }
        Algorithm::P256 => {
            let key = if format == "raw" {
                SigningKey::from_slice(bytes).map_err(|_| anyhow!("invalid P256 private key"))?
            } else {
                SigningKey::from_pkcs8_der(&der).map_err(|_| anyhow!("invalid P256 private key"))?
            };
            key.to_pkcs8_der()
                .map_err(|_| anyhow!("encode P256 private key"))?
                .as_bytes()
                .to_vec()
        }
        Algorithm::Rsa => {
            ensure!(format != "raw", "RSA imports in PEM or DER");
            let key = RsaKeyPair::from_pkcs8(&der)
                .or_else(|_| RsaKeyPair::from_der(&der))
                .map_err(|_| anyhow!("invalid RSA private key"))?;
            AsDer::<Pkcs8V1Der>::as_der(&key)
                .map_err(|_| anyhow!("encode RSA private key"))?
                .as_ref()
                .to_vec()
        }
        _ => bail!("unsupported import algorithm"),
    }))
}

const IMPORT_AAD: &[u8] = b"flower-sealed-key-import-v1";

/// Seal private material for `import` under a wrapping key, offline:
/// requests then carry only ciphertext. `format` is `raw`, `pem` or
/// `der`.
pub fn seal_import(wrapping: &WrappingKey, bytes: &[u8], format: &str) -> Result<Value> {
    let tag = match format {
        "raw" => 0,
        "pem" => 1,
        "der" => 2,
        _ => bail!("key format must be raw, pem or der"),
    };
    let nonce = random::<12>();
    let mut plaintext = Zeroizing::new(vec![tag]);
    plaintext.extend_from_slice(bytes);
    Ok(json!({
        "version": 1,
        "wrappingId": wrapping.id,
        "nonce": URL_SAFE_NO_PAD.encode(nonce),
        "ciphertext": URL_SAFE_NO_PAD.encode(seal(&wrapping.cipher, nonce, IMPORT_AAD, &plaintext)?),
    }))
}

#[cfg(test)]
mod flight_tests {
    use super::*;

    #[test]
    fn cold_load_registry_is_bounded_and_releases_successes_and_failures() {
        let mut cache = Cache::default();
        assert!(cache.flight([0; 32], 0).is_none());
        assert!(cache.flight([0; 32], COLD_LOAD_BYTES - 1).is_none());
        let first = cache.flight([0; 32], COLD_LOAD_BYTES).unwrap();
        let waiter = cache.flight([0; 32], COLD_LOAD_BYTES).unwrap();
        assert!(Arc::ptr_eq(&first, &waiter));
        assert!(cache.flight([1; 32], COLD_LOAD_BYTES).is_none());
        assert_eq!(cache.flight_bytes(), COLD_LOAD_BYTES);
        assert_eq!(cache.coalesced, 1);
        first.set(Err("unwrap failed".into())).ok().unwrap();
        drop(first);
        assert_eq!(cache.flight_bytes(), COLD_LOAD_BYTES);
        drop(waiter);
        assert_eq!(cache.flight_bytes(), 0);
        let next = cache.flight([0; 32], COLD_LOAD_BYTES).unwrap();
        assert!(next.get().is_none(), "a failed load must be retried");
        let key = PreparedKey::prepare(Algorithm::Ed25519, &[1; 32]).unwrap();
        next.set(Ok(Arc::new(key))).ok().unwrap();
        drop(next);
        assert_eq!(cache.flight_bytes(), 0);
    }
}
