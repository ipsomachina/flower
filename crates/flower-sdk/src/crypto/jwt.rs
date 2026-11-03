//! Compact JWS and dir/A256GCM JWE, run by the host.
//!
//! Signing takes an explicit algorithm with raw keys (HS256 secrets
//! of at least 32 bytes, DER or PEM private keys for RS256, ES256 and
//! EdDSA); a managed key's binding decides its algorithm, and its
//! immutable version becomes the token's `kid`. Verification never
//! trusts the token's `alg` alone: raw keys need an allowlist, and a
//! managed key verifies only tokens whose `kid` names one of its
//! unrevoked versions. ES256 signs deterministically (RFC 6979).
//!
//! Validation runs at the invocation clock: `exp` is required unless
//! disabled, present `exp` and `nbf` are always checked, and an `aud`
//! claim must be validated explicitly. Only standard claims are
//! validated; check custom claims yourself.
//!
//! Validation does not poll the clock. It declares, as
//! [`Ctx::changes_at`](crate::ctx::Ctx::changes_at) does, when time alone
//! next changes its outcome: a token not active yet at its `nbf` less the
//! tolerance, a valid one at its `exp` plus the tolerance, an expired one
//! never. The earliest instant declared stands, so nothing the guest declares
//! later lifts it, and a derived value keeps it: cached results, derived
//! values and watches hold until then. A JWE that fails to decrypt cannot
//! say, so that outcome polls the clock as `now()` does.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use super::{Crypto, CryptoOp, Key, ManagedKey, ManagedKeyRef, hex_encode};
use crate::error::{Result, codes, fail};
use crate::value::Value;

/// A JWS algorithm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Algorithm {
    /// HMAC-SHA-256.
    Hs256,
    /// RSA PKCS#1 v1.5 SHA-256.
    Rs256,
    /// P-256 ECDSA SHA-256.
    Es256,
    /// Ed25519.
    EdDsa,
}

impl Algorithm {
    /// The JOSE name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Algorithm::Hs256 => "HS256",
            Algorithm::Rs256 => "RS256",
            Algorithm::Es256 => "ES256",
            Algorithm::EdDsa => "EdDSA",
        }
    }
}

/// The encoding of raw key bytes. PEM is text: see [`JwtKey::Pem`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyFormat {
    /// Raw secret bytes: HS256 only.
    Raw,
    /// PKCS#8 private or SPKI public DER.
    Der,
}

/// A JWS key: raw bytes in a format, PEM text or a managed key.
#[derive(Clone, Copy, Debug)]
pub enum JwtKey<'a> {
    /// Raw or DER bytes.
    Bytes(&'a [u8], KeyFormat),
    /// PEM text.
    Pem(&'a str),
    /// A catalog key at a version.
    Managed(&'a ManagedKey, ManagedKeyRef),
}

impl<'a> From<&'a ManagedKey> for JwtKey<'a> {
    fn from(key: &'a ManagedKey) -> Self {
        JwtKey::Managed(key, ManagedKeyRef::Current)
    }
}

impl<'a> From<Key<'a>> for JwtKey<'a> {
    fn from(key: Key<'a>) -> Self {
        match key {
            Key::Raw(bytes) => JwtKey::Bytes(bytes, KeyFormat::Raw),
            Key::Managed(key, reference) => JwtKey::Managed(key, reference),
        }
    }
}

/// Signing options.
#[derive(Clone, Debug, Default)]
pub struct SignOptions {
    /// Required with raw keys; with a managed key, an optional check
    /// against the binding's algorithm.
    pub algorithm: Option<Algorithm>,
    /// The `typ` header; `JWT` by default.
    pub typ: Option<String>,
    /// The `kid` header, raw keys only: a managed key's version fixes
    /// it.
    pub kid: Option<String>,
}

/// Standard-claim validation.
#[derive(Clone, Debug, Default)]
pub struct ValidationOptions {
    /// Required `iss`.
    pub issuer: Option<String>,
    /// Accepted audiences; required whenever the token has `aud`.
    pub audience: Option<Vec<String>>,
    /// Required `sub`.
    pub subject: Option<String>,
    /// Clock skew allowance in seconds.
    pub clock_tolerance_seconds: Option<f64>,
    /// Whether `exp` must be present; true by default.
    pub require_expiration: Option<bool>,
    /// Required `typ` header.
    pub typ: Option<String>,
}

impl ValidationOptions {
    fn fill(&self, map: &mut BTreeMap<String, Value>) -> Result<()> {
        if let Some(issuer) = &self.issuer {
            map.insert(String::from("issuer"), Value::String(issuer.clone()));
        }
        if let Some(audience) = &self.audience {
            map.insert(
                String::from("audience"),
                Value::Array(audience.iter().cloned().map(Value::String).collect()),
            );
        }
        if let Some(subject) = &self.subject {
            map.insert(String::from("subject"), Value::String(subject.clone()));
        }
        if let Some(tolerance) = self.clock_tolerance_seconds {
            if !tolerance.is_finite() || tolerance < 0.0 {
                return Err(fail(
                    codes::INVALID_VALUE,
                    "clock tolerance must be finite and non-negative",
                ));
            }
            map.insert(
                String::from("clockToleranceSeconds"),
                Value::Number(tolerance),
            );
        }
        if let Some(required) = self.require_expiration {
            map.insert(String::from("requireExpiration"), Value::Bool(required));
        }
        if let Some(typ) = &self.typ {
            map.insert(String::from("typ"), Value::String(typ.clone()));
        }
        Ok(())
    }
}

/// Verification options.
#[derive(Clone, Debug, Default)]
pub struct VerifyOptions {
    /// Required non-empty allowlist with raw keys; with a managed key,
    /// an optional restriction that must match its binding.
    pub algorithms: Option<Vec<Algorithm>>,
    /// Standard-claim validation.
    pub validation: ValidationOptions,
}

/// Encryption options.
#[derive(Clone, Debug, Default)]
pub struct EncryptOptions {
    /// The `typ` header; `JWT` by default.
    pub typ: Option<String>,
    /// The `kid` header, raw keys only.
    pub kid: Option<String>,
}

/// A verified or decrypted token.
#[derive(Clone, Debug, PartialEq)]
pub struct Verified {
    /// The claims. Only standard claims were validated.
    pub claims: Value,
    /// The protected header.
    pub protected_header: ProtectedHeader,
}

/// A token's protected header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtectedHeader {
    /// `HS256`, `RS256`, `ES256`, `EdDSA` or `dir`.
    pub algorithm: String,
    /// `A256GCM` for JWE.
    pub encryption: Option<String>,
    /// The key ID: `flower.<key id>.<version>` for managed keys.
    pub kid: Option<String>,
    /// The type.
    pub typ: Option<String>,
}

/// JOSE operations over an invocation's crypto capability.
pub struct Jwt<'s, 'a, 'c, 'r>(pub(super) &'s mut Crypto<'a, 'c, 'r>);

impl Jwt<'_, '_, '_, '_> {
    /// Sign claims (a JSON object) into a compact JWS.
    pub fn sign<'k>(
        &mut self,
        claims: &Value,
        key: impl Into<JwtKey<'k>>,
        options: &SignOptions,
    ) -> Result<String> {
        let claims = object_json(claims)?;
        let mut settings = BTreeMap::new();
        if let Some(algorithm) = options.algorithm {
            settings.insert(
                String::from("algorithm"),
                Value::String(String::from(algorithm.as_str())),
            );
        }
        if let Some(typ) = &options.typ {
            settings.insert(String::from("typ"), Value::String(typ.clone()));
        }
        if let Some(kid) = &options.kid {
            settings.insert(String::from("kid"), Value::String(kid.clone()));
        }
        match key.into() {
            JwtKey::Managed(key, reference) => {
                if options.kid.is_some() {
                    return Err(fail(
                        codes::INVALID_VALUE,
                        "a managed key's version fixes the kid",
                    ));
                }
                let options = json(settings)?;
                text(self.0.managed(
                    "jwt.sign",
                    key,
                    reference,
                    &[claims.as_bytes()],
                    &options,
                    false,
                )?)
            }
            raw => {
                if options.algorithm.is_none() {
                    return Err(fail(
                        codes::INVALID_VALUE,
                        "raw JWT signing needs an algorithm",
                    ));
                }
                let bytes = raw_key(raw, &mut settings);
                let options = json(settings)?;
                text(self.0.call(
                    CryptoOp::Jwt,
                    alloc::vec![
                        Value::String(String::from("sign")),
                        Value::String(claims),
                        Value::String(hex_encode(bytes)),
                        Value::String(options),
                    ],
                )?)
            }
        }
    }

    /// Verify a compact JWS and its standard claims at the invocation
    /// clock.
    pub fn verify<'k>(
        &mut self,
        token: &str,
        key: impl Into<JwtKey<'k>>,
        options: &VerifyOptions,
    ) -> Result<Verified> {
        self.verify_with_clock(token, key.into(), options, None)
    }

    /// [`Jwt::verify`] at `clock`, a clock its caller read and answers
    /// for (declaring when the outcome changes), or at the invocation
    /// clock, declaring that here as the module docs describe.
    pub(crate) fn verify_with_clock(
        &mut self,
        token: &str,
        key: JwtKey<'_>,
        options: &VerifyOptions,
        clock: Option<f64>,
    ) -> Result<Verified> {
        let mut settings = BTreeMap::new();
        options.validation.fill(&mut settings)?;
        if let Some(algorithms) = &options.algorithms {
            if algorithms.is_empty() {
                return Err(fail(
                    codes::INVALID_VALUE,
                    "the JWT algorithm allowlist is empty",
                ));
            }
            settings.insert(
                String::from("algorithms"),
                Value::Array(
                    algorithms
                        .iter()
                        .map(|algorithm| Value::String(String::from(algorithm.as_str())))
                        .collect(),
                ),
            );
        }
        let now = match clock {
            Some(clock) => clock,
            None => self.0.clock_ms()?,
        };
        let outcome = match key {
            JwtKey::Managed(key, reference) => {
                if reference != ManagedKeyRef::Current {
                    return Err(fail(
                        codes::INVALID_VALUE,
                        "the token's kid selects the managed key version",
                    ));
                }
                let options = json(settings)?;
                self.0.managed_at(
                    "jwt.verify",
                    key,
                    reference,
                    &[token.as_bytes()],
                    &options,
                    now,
                )
            }
            raw => {
                if options.algorithms.is_none() {
                    return Err(fail(
                        codes::INVALID_VALUE,
                        "raw JWT verification needs an algorithm allowlist",
                    ));
                }
                let bytes = raw_key(raw, &mut settings);
                let options = json(settings)?;
                self.0.call(
                    CryptoOp::Jwt,
                    alloc::vec![
                        Value::String(String::from("verify")),
                        Value::String(String::from(token)),
                        Value::String(hex_encode(bytes)),
                        Value::String(options),
                        Value::Number(now),
                    ],
                )
            }
        };
        if clock.is_none() {
            // The host read the token's `nbf` and `exp`, verified or not.
            self.declare_change(&outcome)?;
        }
        verified(outcome?)
    }

    /// Declare when time alone next changes a token check's outcome, as
    /// the host found it: `changesAt` beside a success's claims or in a
    /// failure's details. Nothing found means time changes nothing.
    fn declare_change(&mut self, outcome: &Result<Value>) -> Result<()> {
        let found = match outcome {
            Ok(value) => value.as_object(),
            Err(failure) => failure.details.as_ref().and_then(Value::as_object),
        };
        match found.and_then(|map| map.get("changesAt")) {
            Some(Value::Number(at)) => self.0.ctx.changes_at(Some(*at)),
            _ => Ok(()),
        }
    }

    /// Encrypt claims into a compact dir/A256GCM JWE under a 32-byte
    /// raw key or a managed A256GCM key. `nonce` must be unique under
    /// the key; `None` draws 12 bytes of host entropy, in mutations only:
    /// never from a guest-local source ([`Crypto::set_prng`]).
    pub fn encrypt<'k>(
        &mut self,
        claims: &Value,
        key: impl Into<Key<'k>>,
        nonce: Option<&[u8; 12]>,
        options: &EncryptOptions,
    ) -> Result<String> {
        let key = key.into();
        if matches!(key, Key::Raw(bytes) if bytes.len() != 32) {
            return Err(fail(
                codes::INVALID_VALUE,
                "JWT encryption keys hold 32 bytes",
            ));
        }
        if matches!(key, Key::Managed(..)) && options.kid.is_some() {
            return Err(fail(
                codes::INVALID_VALUE,
                "a managed key's version fixes the kid",
            ));
        }
        let claims = object_json(claims)?;
        let mut settings = BTreeMap::new();
        if let Some(typ) = &options.typ {
            settings.insert(String::from("typ"), Value::String(typ.clone()));
        }
        if let Some(kid) = &options.kid {
            settings.insert(String::from("kid"), Value::String(kid.clone()));
        }
        let options = json(settings)?;
        let mut generated = match nonce {
            Some(_) => Vec::new(),
            None => self.0.host_random(12)?,
        };
        let chosen: &[u8] = match nonce {
            Some(nonce) => nonce,
            None => &generated,
        };
        let result = match key {
            Key::Managed(key, reference) => self.0.managed(
                "jwt.encrypt",
                key,
                reference,
                &[claims.as_bytes(), chosen],
                &options,
                false,
            ),
            Key::Raw(bytes) => self.0.call(
                CryptoOp::Jwt,
                alloc::vec![
                    Value::String(String::from("encrypt")),
                    Value::String(claims),
                    Value::String(hex_encode(bytes)),
                    Value::String(hex_encode(chosen)),
                    Value::String(options),
                ],
            ),
        };
        super::nacl::wipe(&mut generated);
        text(result?)
    }

    /// Decrypt a compact dir/A256GCM JWE and validate its standard
    /// claims at the invocation clock.
    pub fn decrypt<'k>(
        &mut self,
        token: &str,
        key: impl Into<Key<'k>>,
        options: &ValidationOptions,
    ) -> Result<Verified> {
        let mut settings = BTreeMap::new();
        options.fill(&mut settings)?;
        let options = json(settings)?;
        let outcome = match key.into() {
            Key::Managed(key, reference) => {
                if reference != ManagedKeyRef::Current {
                    return Err(fail(
                        codes::INVALID_VALUE,
                        "the token's kid selects the managed key version",
                    ));
                }
                self.0.managed(
                    "jwt.decrypt",
                    key,
                    reference,
                    &[token.as_bytes()],
                    &options,
                    true,
                )
            }
            Key::Raw(bytes) => {
                let now = self.0.clock_ms()?;
                self.0.call(
                    CryptoOp::Jwt,
                    alloc::vec![
                        Value::String(String::from("decrypt")),
                        Value::String(String::from(token)),
                        Value::String(hex_encode(bytes)),
                        Value::String(options),
                        Value::Number(now),
                    ],
                )
            }
        };
        match &outcome {
            Ok(_) => self.declare_change(&outcome)?,
            // Only the key opens the claims, so a failure cannot say
            // when it would decrypt: the result polls, as `now()` does.
            Err(_) => {
                self.0.ctx.now()?;
            }
        }
        verified(outcome?)
    }
}

/// Note the key format and return the raw bytes.
fn raw_key<'a>(key: JwtKey<'a>, settings: &mut BTreeMap<String, Value>) -> &'a [u8] {
    let (bytes, format) = match key {
        JwtKey::Bytes(bytes, KeyFormat::Raw) => (bytes, "raw"),
        JwtKey::Bytes(bytes, KeyFormat::Der) => (bytes, "der"),
        JwtKey::Pem(text) => (text.as_bytes(), "pem"),
        JwtKey::Managed(..) => (&[][..], "raw"),
    };
    settings.insert(
        String::from("keyFormat"),
        Value::String(String::from(format)),
    );
    bytes
}

/// Canonical JSON of a claims object.
fn object_json(claims: &Value) -> Result<String> {
    if !matches!(claims, Value::Object(_)) {
        return Err(fail(codes::INVALID_VALUE, "JWT claims must be an object"));
    }
    claims.canonical_json()
}

fn json(settings: BTreeMap<String, Value>) -> Result<String> {
    Value::Object(settings).canonical_json()
}

fn text(value: Value) -> Result<String> {
    match value {
        Value::String(text) => Ok(text),
        _ => Err(fail(codes::TYPE_MISMATCH, "a token is text")),
    }
}

/// Decode `{claims, protectedHeader}`.
fn verified(value: Value) -> Result<Verified> {
    let malformed = || fail(codes::TYPE_MISMATCH, "malformed verified token");
    let Value::Object(mut map) = value else {
        return Err(malformed());
    };
    let claims = map.remove("claims").ok_or_else(malformed)?;
    let Some(Value::Object(header)) = map.remove("protectedHeader") else {
        return Err(malformed());
    };
    let field = |name: &str| -> Result<Option<String>> {
        match header.get(name) {
            None => Ok(None),
            Some(Value::String(text)) => Ok(Some(text.clone())),
            Some(_) => Err(malformed()),
        }
    };
    Ok(Verified {
        claims,
        protected_header: ProtectedHeader {
            algorithm: field("alg")?.ok_or_else(malformed)?,
            encryption: field("enc")?,
            kid: field("kid")?,
            typ: field("typ")?,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_options_render_canonically_and_check_tolerance() {
        let mut map = BTreeMap::new();
        ValidationOptions {
            issuer: Some(String::from("shop")),
            require_expiration: Some(false),
            ..Default::default()
        }
        .fill(&mut map)
        .unwrap();
        assert_eq!(
            json(map).unwrap(),
            r#"{"issuer":"shop","requireExpiration":false}"#
        );
        for tolerance in [f64::NAN, -1.0, f64::INFINITY] {
            let options = ValidationOptions {
                clock_tolerance_seconds: Some(tolerance),
                ..Default::default()
            };
            assert!(options.fill(&mut BTreeMap::new()).is_err());
        }
    }

    #[test]
    fn verified_tokens_decode_and_reject_wrong_shapes() {
        let mut header = BTreeMap::new();
        header.insert(String::from("alg"), Value::String(String::from("HS256")));
        header.insert(String::from("kid"), Value::String(String::from("k")));
        let mut map = BTreeMap::new();
        map.insert(String::from("claims"), Value::Object(BTreeMap::new()));
        map.insert(String::from("protectedHeader"), Value::Object(header));
        let decoded = verified(Value::Object(map.clone())).unwrap();
        assert_eq!(decoded.protected_header.algorithm, "HS256");
        assert_eq!(decoded.protected_header.kid.as_deref(), Some("k"));
        map.remove("protectedHeader");
        assert!(verified(Value::Object(map)).is_err());
        assert!(verified(Value::Null).is_err());
        assert!(object_json(&Value::Null).is_err());
    }
}
