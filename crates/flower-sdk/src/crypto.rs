//! Guest crypto: managed keys, NaCl, JOSE, WebAuthn, hashing and
//! randomness over the `crypto_call` host import.
//!
//! No primitive runs in the guest: every operation is a host call, so
//! the guest links no crypto library and managed secrets never enter
//! guest memory. Binary payloads cross as lowercase hex strings
//! ([`Value`] has no byte string, and hex needs no guest
//! dependencies); claims, options and WebAuthn responses cross as
//! JSON text. Host failures arrive as ordinary envelopes, so crypto
//! errors are catchable business failures, never traps.
//!
//! - [`Crypto`]: randomness, SHA-256, managed signing and the
//!   catalog's generic [`Crypto::managed_sign`] family.
//! - [`nacl`]: TweetNaCl's high-level operations over raw or managed
//!   keys.
//! - [`jwt`]: compact JWS (HS256, RS256, ES256, EdDSA) and dir/A256GCM
//!   JWE over raw or managed keys, validated at the invocation clock.
//! - [`webauthn`]: passkey relying-party verification.
//!
//! Managed keys are declared, never held: a module lists the aliases
//! it uses with [`KeyDeclaration`]s from
//! [`Component::keys`](crate::app::Component::keys), an operator
//! binds each alias to a key in the replicated catalog with granted
//! usages, and the host checks both on every use.

use alloc::string::String;
use alloc::vec::Vec;

use crate::ctx::Ctx;
use crate::error::{Result, codes, fail};
use crate::value::Value;

pub mod jwt;
pub mod nacl;
pub mod webauthn;

/// Failure code of crypto failures without a more specific code:
/// malformed keys, failed decryption, invalid tokens.
pub const CRYPTO_ERROR: &str = "CRYPTO_ERROR";

/// Crypto host operations. Discriminants are the ABI: never reorder,
/// only append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum CryptoOp {
    /// Fresh random bytes.
    RandomBytes = 1,
    /// SHA-256 digest.
    Sha256 = 2,
    /// Sign with a managed key.
    Sign = 3,
    /// Verify against a managed key.
    Verify = 4,
    /// Read a managed public key.
    PublicKey = 5,
    /// Report a binding's current version.
    KeyVersion = 6,
    /// NaCl over raw keys: `[op, hex...]`.
    Nacl = 7,
    /// JOSE over raw keys: `[operation, ...]`.
    Jwt = 8,
    /// An operation on a catalog key:
    /// `[operation, alias, version, [hex...], options, now]`.
    Managed = 9,
    /// WebAuthn verification: `[operation, response, options]`.
    WebAuthn = 10,
}

/// A managed-key algorithm guests may declare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Algorithm {
    /// Ed25519 signing: JWT EdDSA and NaCl signatures.
    Ed25519,
    /// P-256 ECDSA: JWT ES256.
    P256,
    /// RSA PKCS#1 v1.5 SHA-256: JWT RS256.
    Rsa,
    /// HMAC-SHA-256: JWT HS256.
    Hs256,
    /// AES-256-GCM: dir/A256GCM JWE.
    A256Gcm,
    /// NaCl secretbox.
    XSalsa20Poly1305,
    /// NaCl box.
    X25519,
}

impl Algorithm {
    /// The catalog name the host and manifests use.
    pub const fn name(self) -> &'static str {
        match self {
            Algorithm::Ed25519 => "Ed25519",
            Algorithm::P256 => "P256",
            Algorithm::Rsa => "RSA",
            Algorithm::Hs256 => "HS256",
            Algorithm::A256Gcm => "A256GCM",
            Algorithm::XSalsa20Poly1305 => "XSalsa20Poly1305",
            Algorithm::X25519 => "X25519",
        }
    }
}

/// A key usage a binding grants and a declaration asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Usage {
    /// Sign messages.
    Sign,
    /// Verify signatures. Reading a public key needs
    /// [`Usage::PublicKey`].
    Verify,
    /// Encrypt: JWE, secretbox and box.
    Encrypt,
    /// Decrypt: JWE, secretbox and box.
    Decrypt,
    /// Derive a non-exportable, invocation-local NaCl shared-key handle.
    Derive,
    /// Read the public component.
    PublicKey,
}

impl Usage {
    /// The catalog name the host and manifests use.
    pub const fn name(self) -> &'static str {
        match self {
            Usage::Sign => "sign",
            Usage::Verify => "verify",
            Usage::Encrypt => "encrypt",
            Usage::Decrypt => "decrypt",
            Usage::Derive => "derive",
            Usage::PublicKey => "publicKey",
        }
    }
}

/// A module's declaration of a catalog key alias: the algorithm it
/// expects and the usages it needs. Declarations grant nothing; the
/// operator's binding must grant the usages too. Listed by
/// [`Component::keys`](crate::app::Component::keys) and carried in the
/// manifest's `keys` member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyDeclaration {
    /// The alias operators bind.
    pub name: &'static str,
    /// The expected algorithm: a binding to another algorithm fails.
    pub algorithm: Algorithm,
    /// The usages the module needs.
    pub usages: &'static [Usage],
}

/// A declared key is a component: list it among the app's components.
impl crate::app::Component for KeyDeclaration {
    fn keys(&self) -> &[KeyDeclaration] {
        core::slice::from_ref(self)
    }
}

impl KeyDeclaration {
    /// Declare an alias.
    pub const fn new(name: &'static str, algorithm: Algorithm, usages: &'static [Usage]) -> Self {
        KeyDeclaration {
            name,
            algorithm,
            usages,
        }
    }

    /// The reference guest code passes to operations.
    pub fn key(&self) -> Result<ManagedKey> {
        ManagedKey::new(self.name, self.algorithm, self.usages)
    }

    /// The manifest form: `{name, algorithm, usages}`.
    pub fn to_value(&self) -> Value {
        let mut map = alloc::collections::BTreeMap::new();
        map.insert(String::from("name"), Value::String(String::from(self.name)));
        map.insert(
            String::from("algorithm"),
            Value::String(String::from(self.algorithm.name())),
        );
        let mut usages = Vec::with_capacity(self.usages.len());
        for usage in self.usages.iter() {
            usages.push(Value::String(String::from(usage.name())));
        }
        map.insert(String::from("usages"), Value::Array(usages));
        Value::Object(map)
    }
}

/// A declared managed-key reference: metadata only, never authority.
/// The host enforces the binding behind the name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedKey {
    name: String,
    algorithm: Algorithm,
    usages: Vec<Usage>,
}

impl ManagedKey {
    /// Declare a key reference. Empty names and empty usages fail;
    /// unknown keys fail at use, when the host resolves the binding.
    pub fn new(name: &str, algorithm: Algorithm, usages: &[Usage]) -> Result<Self> {
        if name.is_empty() {
            return Err(fail(codes::INVALID_VALUE, "key name is empty"));
        }
        if usages.is_empty() {
            return Err(fail(codes::INVALID_VALUE, "key declares no usages"));
        }
        Ok(ManagedKey {
            name: String::from(name),
            algorithm,
            usages: usages.to_vec(),
        })
    }

    /// The referenced binding name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The declared algorithm.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }
}

/// Raw key bytes or a managed key. There is no conversion of a
/// managed key into bytes: its material never enters the guest.
#[derive(Clone, Copy, Debug)]
pub enum Key<'k> {
    /// Caller-held bytes.
    Raw(&'k [u8]),
    /// A catalog key, at a version.
    Managed(&'k ManagedKey, ManagedKeyRef),
}

impl<'k> From<&'k [u8]> for Key<'k> {
    fn from(bytes: &'k [u8]) -> Self {
        Key::Raw(bytes)
    }
}

impl<'k, const N: usize> From<&'k [u8; N]> for Key<'k> {
    fn from(bytes: &'k [u8; N]) -> Self {
        Key::Raw(bytes)
    }
}

impl<'k> From<&'k ManagedKey> for Key<'k> {
    fn from(key: &'k ManagedKey) -> Self {
        Key::Managed(key, ManagedKeyRef::Current)
    }
}

impl<'k> From<(&'k ManagedKey, ManagedKeyRef)> for Key<'k> {
    fn from((key, reference): (&'k ManagedKey, ManagedKeyRef)) -> Self {
        Key::Managed(key, reference)
    }
}

/// A version pin: current or a historical version. History verifies
/// and discloses; only current signs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedKeyRef {
    /// The binding's current version.
    Current,
    /// A pinned historical version.
    Version(u32),
}

/// Encode bytes as lowercase hex.
pub fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0F) as usize] as char);
    }
    out
}

/// Decode lowercase or uppercase hex.
pub fn hex_decode(text: &str) -> Result<Vec<u8>> {
    fn nibble(byte: u8) -> Result<u8> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(fail(codes::INVALID_VALUE, "hex has bad digits")),
        }
    }
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(fail(codes::INVALID_VALUE, "hex has odd length"));
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
        out.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Ok(out)
}

/// A guest-local random source (v1's `Prng`): it fills the buffer or
/// fails. Its author owns entropy, nonce safety and determinism.
pub type Prng = fn(&mut [u8]) -> Result<()>;

/// Host crypto primitives over the invocation: `ctx.crypto()` from any
/// method context, or [`Crypto::new`] over the raw one. The borrow is a
/// capability token: crypto calls stay on the invocation thread for the
/// invocation's lifetime, and JWT validation reads the invocation clock
/// through it.
pub struct Crypto<'a, 'c, 'r> {
    ctx: &'a mut Ctx<'c, 'r>,
    prng: Option<Prng>,
}

impl<'a, 'c, 'r> Crypto<'a, 'c, 'r> {
    /// Borrow the invocation for crypto calls.
    pub fn new(ctx: &'a mut Ctx<'c, 'r>) -> Self {
        Crypto { ctx, prng: None }
    }

    /// Draw [`Crypto::random_bytes`] from a guest-local source, or
    /// from host entropy again with `None`. Host entropy serves
    /// mutations only; a guest source serves any invocation, so NaCl
    /// key pairs and WebAuthn challenges can come from it anywhere.
    /// JWE nonces the SDK draws never use it.
    pub fn set_prng(&mut self, source: Option<Prng>) {
        self.prng = source;
    }

    /// NaCl operations.
    pub fn nacl(&mut self) -> nacl::Nacl<'_, 'a, 'c, 'r> {
        nacl::Nacl(self)
    }

    /// JOSE operations.
    pub fn jwt(&mut self) -> jwt::Jwt<'_, 'a, 'c, 'r> {
        jwt::Jwt(self)
    }

    /// WebAuthn relying-party operations.
    pub fn webauthn(&mut self) -> webauthn::WebAuthn<'_, 'a, 'c, 'r> {
        webauthn::WebAuthn(self)
    }

    /// The invocation clock, in whole milliseconds, without marking
    /// the result time-dependent: token checks declare when their
    /// outcome changes instead (see [`jwt`]).
    pub(crate) fn clock_ms(&mut self) -> Result<f64> {
        Ok(self.ctx.clock()? as u64 as f64)
    }

    /// Sign with a catalog key of any signing algorithm: Ed25519 and
    /// P256 (64-byte `r || s`) signatures, RSA PKCS#1 v1.5 and HS256
    /// MACs. Historical pins never sign.
    pub fn managed_sign(
        &mut self,
        key: &ManagedKey,
        reference: ManagedKeyRef,
        message: &[u8],
    ) -> Result<Vec<u8>> {
        let value = self.managed("sign", key, reference, &[message], "{}", false)?;
        bytes_of(value)
    }

    /// Verify a signature from [`Crypto::managed_sign`]. Mismatches
    /// report `false`; Ed25519 verification is strict.
    pub fn managed_verify(
        &mut self,
        key: &ManagedKey,
        reference: ManagedKeyRef,
        message: &[u8],
        signature: &[u8],
    ) -> Result<bool> {
        match self.managed("verify", key, reference, &[message, signature], "{}", false)? {
            Value::Bool(matched) => Ok(matched),
            _ => Err(fail(codes::TYPE_MISMATCH, "verification is a bool")),
        }
    }

    /// A catalog key's public component: raw 32 bytes for Ed25519 and
    /// X25519, SPKI DER for P256 and RSA.
    pub fn managed_public_key(
        &mut self,
        key: &ManagedKey,
        reference: ManagedKeyRef,
    ) -> Result<Vec<u8>> {
        let value = self.managed("publicKey", key, reference, &[], "{}", false)?;
        bytes_of(value)
    }

    /// A catalog key's active version and its JOSE `kid`. Save the
    /// version beside NaCl ciphertexts to pin it when opening them.
    pub fn managed_version(&mut self, key: &ManagedKey) -> Result<(u32, String)> {
        let value = self.managed("version", key, ManagedKeyRef::Current, &[], "{}", false)?;
        let Value::Object(map) = value else {
            return Err(fail(codes::TYPE_MISMATCH, "key version is an object"));
        };
        match (map.get("version"), map.get("kid")) {
            (Some(Value::Number(version)), Some(Value::String(kid))) => {
                Ok((*version as u32, kid.clone()))
            }
            _ => Err(fail(codes::TYPE_MISMATCH, "key version is malformed")),
        }
    }

    /// Run a catalog operation. `timed` passes the invocation clock;
    /// the caller declares what the host says about it.
    pub(crate) fn managed(
        &mut self,
        operation: &str,
        key: &ManagedKey,
        reference: ManagedKeyRef,
        args: &[&[u8]],
        options: &str,
        timed: bool,
    ) -> Result<Value> {
        let now = if timed { self.clock_ms()? } else { 0.0 };
        self.managed_at(operation, key, reference, args, options, now)
    }

    /// Run a catalog operation at `now`, a clock its caller read.
    pub(crate) fn managed_at(
        &mut self,
        operation: &str,
        key: &ManagedKey,
        reference: ManagedKeyRef,
        args: &[&[u8]],
        options: &str,
        now: f64,
    ) -> Result<Value> {
        self.call(
            CryptoOp::Managed,
            alloc::vec![
                Value::String(String::from(operation)),
                Value::String(key.name.clone()),
                reference_value(reference),
                Value::Array(
                    args.iter()
                        .map(|arg| Value::String(hex_encode(arg)))
                        .collect()
                ),
                Value::String(String::from(options)),
                Value::Number(now),
            ],
        )
    }

    /// Fresh random bytes: from the guest-local source when one is set
    /// (see [`Crypto::set_prng`]), else from host entropy, in mutations
    /// only. A failing source leaves nothing behind.
    pub fn random_bytes(&mut self, len: u32) -> Result<Vec<u8>> {
        let Some(source) = self.prng else {
            return self.host_random(len);
        };
        let mut bytes = alloc::vec![0u8; len as usize];
        if let Err(failure) = source(&mut bytes) {
            nacl::wipe(&mut bytes);
            return Err(failure);
        }
        Ok(bytes)
    }

    /// Fresh random bytes from host entropy, whatever source is set.
    pub(crate) fn host_random(&mut self, len: u32) -> Result<Vec<u8>> {
        let value = self.call(
            CryptoOp::RandomBytes,
            alloc::vec![Value::Number(len as f64)],
        )?;
        match value {
            Value::String(hex) => hex_decode(&hex),
            _ => Err(fail(codes::TYPE_MISMATCH, "random bytes are hex")),
        }
    }

    /// SHA-256 over bytes.
    pub fn sha256(&mut self, bytes: &[u8]) -> Result<[u8; 32]> {
        let value = self.call(
            CryptoOp::Sha256,
            alloc::vec![Value::String(hex_encode(bytes))],
        )?;
        let Value::String(hex) = value else {
            return Err(fail(codes::TYPE_MISMATCH, "digest is hex"));
        };
        let decoded = hex_decode(&hex)?;
        decoded
            .try_into()
            .map_err(|_| fail(codes::INVALID_VALUE, "digest is 32 bytes"))
    }

    /// Sign a message with a managed key. Historical pins never sign.
    pub fn sign(
        &mut self,
        key: &ManagedKey,
        reference: ManagedKeyRef,
        message: &[u8],
    ) -> Result<[u8; 64]> {
        self.require_algorithm(key)?;
        let value = self.call(
            CryptoOp::Sign,
            alloc::vec![
                Value::String(key.name.clone()),
                reference_value(reference),
                Value::String(hex_encode(message)),
            ],
        )?;
        let Value::String(hex) = value else {
            return Err(fail(codes::TYPE_MISMATCH, "signature is hex"));
        };
        let decoded = hex_decode(&hex)?;
        decoded
            .try_into()
            .map_err(|_| fail(codes::INVALID_VALUE, "signature is 64 bytes"))
    }

    /// Verify a message against a managed key. Mismatches report
    /// `false`; unknown keys, versions and usages fail.
    pub fn verify(
        &mut self,
        key: &ManagedKey,
        reference: ManagedKeyRef,
        message: &[u8],
        signature: &[u8; 64],
    ) -> Result<bool> {
        self.require_algorithm(key)?;
        let value = self.call(
            CryptoOp::Verify,
            alloc::vec![
                Value::String(key.name.clone()),
                reference_value(reference),
                Value::String(hex_encode(message)),
                Value::String(hex_encode(signature)),
            ],
        )?;
        match value {
            Value::Bool(matched) => Ok(matched),
            _ => Err(fail(codes::TYPE_MISMATCH, "verification is a bool")),
        }
    }

    /// Read a managed Ed25519 or X25519 public key; see
    /// [`Crypto::managed_public_key`] for SPKI keys.
    pub fn public_key(&mut self, key: &ManagedKey, reference: ManagedKeyRef) -> Result<[u8; 32]> {
        if key.algorithm != Algorithm::X25519 {
            self.require_algorithm(key)?;
        }
        let value = self.call(
            CryptoOp::PublicKey,
            alloc::vec![Value::String(key.name.clone()), reference_value(reference),],
        )?;
        let Value::String(hex) = value else {
            return Err(fail(codes::TYPE_MISMATCH, "public key is hex"));
        };
        let decoded = hex_decode(&hex)?;
        decoded
            .try_into()
            .map_err(|_| fail(codes::INVALID_VALUE, "public key is 32 bytes"))
    }

    /// Report a binding's current version.
    pub fn key_version(&mut self, key: &ManagedKey) -> Result<u32> {
        let value = self.call(
            CryptoOp::KeyVersion,
            alloc::vec![Value::String(key.name.clone())],
        )?;
        match value {
            Value::Number(version) => Ok(version as u32),
            _ => Err(fail(codes::TYPE_MISMATCH, "version is a number")),
        }
    }

    /// These typed operations take 64-byte Ed25519 signatures; other
    /// algorithms use the `managed_*` family.
    fn require_algorithm(&self, key: &ManagedKey) -> Result<()> {
        if key.algorithm != Algorithm::Ed25519 {
            return Err(fail(codes::INVALID_VALUE, "unsupported key algorithm"));
        }
        Ok(())
    }

    /// Encode arguments, call the host and decode the envelope.
    pub(crate) fn call(&mut self, op: CryptoOp, args: Vec<Value>) -> Result<Value> {
        let input = Value::Array(args).encode_binary()?;
        let output = crate::abi::crypto_call(op, &input)?;
        let response = Value::decode_binary(&output)?;
        crate::abi::decode_envelope(response)
    }
}

/// Decode a hex result.
pub(crate) fn bytes_of(value: Value) -> Result<Vec<u8>> {
    match value {
        Value::String(hex) => hex_decode(&hex),
        _ => Err(fail(codes::TYPE_MISMATCH, "crypto bytes are hex")),
    }
}

/// Encode a version pin: null for current, a number for history.
fn reference_value(reference: ManagedKeyRef) -> Value {
    match reference {
        ManagedKeyRef::Current => Value::Null,
        ManagedKeyRef::Version(version) => Value::Number(version as f64),
    }
}

/// The host side of the crypto ABI: dispatch an op over binary
/// argument bytes and return binary envelope bytes. Implemented by
/// the crypto lane; the engine links it behind `crypto_call`.
pub trait CryptoBackend: Send + Sync {
    /// Run one crypto op.
    fn crypto_call(&self, op: u32, input: &[u8]) -> Result<Vec<u8>>;

    /// Fresh invocation-local state, such as non-exportable shared-key
    /// handles. Runtimes drop it when the invocation ends. Stateless
    /// backends can keep the default and reuse themselves.
    fn for_invocation(&self) -> Option<alloc::boxed::Box<dyn CryptoBackend>> {
        None
    }

    /// This backend scoped to one module of database `db`, from its
    /// manifest: runtime factories call it per module so the host
    /// resolves keys in that database's catalog and checks the
    /// module's key declarations. `Ok(None)` keeps this backend
    /// unscoped; failures reject the module.
    fn for_module(
        &self,
        _db: &[u8],
        _module: &str,
        _manifest: &Value,
    ) -> Result<Option<alloc::boxed::Box<dyn CryptoBackend>>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn hex_roundtrips() {
        assert_eq!(hex_encode(b""), "");
        assert_eq!(hex_encode(b"\x00\xff\x10"), "00ff10");
        assert_eq!(hex_decode("00ff10").unwrap(), vec![0u8, 255u8, 16u8]);
        assert_eq!(hex_decode("00FF10").unwrap(), vec![0u8, 255u8, 16u8]);
        assert!(hex_decode("abc").is_err());
        assert!(hex_decode("zz").is_err());
    }

    #[test]
    fn managed_key_declarations_validate() {
        assert!(ManagedKey::new("", Algorithm::Ed25519, &[Usage::Sign]).is_err());
        assert!(ManagedKey::new("k", Algorithm::Ed25519, &[]).is_err());
        let key = ManagedKey::new("k", Algorithm::Ed25519, &[Usage::Sign]).unwrap();
        assert_eq!(key.name(), "k");
        assert_eq!(key.algorithm(), Algorithm::Ed25519);
    }

    #[test]
    fn manifests_carry_key_declarations_only_when_declared() {
        use crate::app::{App, Component};
        static DECLARED: [KeyDeclaration; 1] = [KeyDeclaration::new(
            "sessions",
            Algorithm::P256,
            &[Usage::Sign, Usage::PublicKey],
        )];
        struct Keys;
        impl Component for Keys {
            fn keys(&self) -> &[KeyDeclaration] {
                &DECLARED
            }
        }
        struct Plain;
        impl Component for Plain {}
        static KEYS: Keys = Keys;
        static PLAIN: Plain = Plain;
        static WITH: [&dyn Component; 2] = [&PLAIN, &KEYS];
        static WITHOUT: [&dyn Component; 1] = [&PLAIN];
        let app = App::new;
        let Value::Object(manifest) = crate::abi::manifest_value(&app(&WITH)).unwrap() else {
            panic!("manifests are objects");
        };
        let expected = r#"[{"algorithm":"P256","name":"sessions","usages":["sign","publicKey"]}]"#;
        assert_eq!(manifest["keys"].canonical_json().unwrap(), expected);
        let Value::Object(plain) = crate::abi::manifest_value(&app(&WITHOUT)).unwrap() else {
            panic!("manifests are objects");
        };
        assert!(!plain.contains_key("keys"));
        let key = DECLARED[0].key().unwrap();
        assert_eq!((key.name(), key.algorithm()), ("sessions", Algorithm::P256));
    }
}
