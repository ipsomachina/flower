//! WebAuthn relying-party verification for passkeys, run by the host.
//!
//! Responses are the JSON browsers produce with
//! `PublicKeyCredential.toJSON()`: base64url for every binary field.
//! Attestation statements are not evaluated; trust rests on the
//! ceremony: the challenge and origin in the client data, and the RP
//! ID hash, flags and counter in the authenticator data. Supported
//! credential algorithms are EdDSA (-8), ES256 (-7), ES384 (-35),
//! ES512 (-36), PS256 (-37) and RS256 (-257).
//!
//! A relying party issues a fresh [`WebAuthn::challenge`] per
//! ceremony (stored server-side until used), verifies the response
//! against it, stores the [`Credential`] on registration and updates
//! its `sign_count` after every authentication. A counter that fails
//! to increase fails with `WEBAUTHN_COUNTER`: the authenticator may be
//! cloned.
//!
//! [`WebAuthn::registration_options`] and
//! [`WebAuthn::authentication_options`] build the options a browser passes to
//! `navigator.credentials.create()` and `.get()`, each with a fresh
//! challenge: render them with `to_value()`, store the challenge, and verify
//! under the same user verification policy. Building them is pure guest code;
//! only the challenge comes from [`Crypto::random_bytes`], so host entropy
//! limits it to mutations unless a guest-local source is set.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use super::{Crypto, CryptoOp};
use crate::error::{Result, codes, fail};
use crate::value::Value;

/// Ceremony timeout the builders default to, in milliseconds.
pub const DEFAULT_TIMEOUT_MS: u64 = 300_000;
/// The largest integer a browser's JSON numbers hold exactly.
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
/// Credential algorithms the builders offer by default: EdDSA, ES256,
/// RS256.
pub const DEFAULT_ALGORITHMS: [i64; 3] = [-8, -7, -257];
/// Every COSE algorithm the host verifies.
const SUPPORTED_ALGORITHMS: [i64; 6] = [-8, -7, -35, -36, -37, -257];

/// Whether the ceremony requires user verification. The builders default to
/// `Required`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UserVerification {
    /// The UV flag must be set.
    #[default]
    Required,
    /// Reported, not required.
    Preferred,
    /// Reported, not required.
    Discouraged,
}

impl UserVerification {
    /// The WebAuthn name.
    pub fn as_str(self) -> &'static str {
        match self {
            UserVerification::Required => "required",
            UserVerification::Preferred => "preferred",
            UserVerification::Discouraged => "discouraged",
        }
    }
}

/// Whether the authenticator must store a discoverable credential.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResidentKey {
    /// A discoverable passkey: the default.
    #[default]
    Required,
    /// Discoverable when the authenticator can.
    Preferred,
    /// Server-side only.
    Discouraged,
}

impl ResidentKey {
    /// The WebAuthn name.
    pub fn as_str(self) -> &'static str {
        match self {
            ResidentKey::Required => "required",
            ResidentKey::Preferred => "preferred",
            ResidentKey::Discouraged => "discouraged",
        }
    }
}

/// The relying party a registration names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelyingParty {
    /// Its ID, such as `example.com`.
    pub id: String,
    /// Its display name.
    pub name: String,
}

/// The account a registration creates a passkey for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegistrationUser {
    /// An opaque base64url handle of 1 to 64 bytes, never an email or
    /// a name.
    pub id: String,
    /// The account name.
    pub name: String,
    /// Defaults to `name`; an explicitly empty one is refused.
    pub display_name: Option<String>,
}

/// A passkey ID with optional browser transport hints, which nothing
/// authenticates.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CredentialDescriptor {
    /// Base64url of 1 to 1,023 bytes; padding is kept as given.
    pub id: String,
    /// Omitted and empty stay distinct in the browser JSON.
    pub transports: Option<Vec<String>>,
}

impl CredentialDescriptor {
    fn to_value(&self) -> Value {
        let mut map = BTreeMap::new();
        map.insert(String::from("type"), text("public-key"));
        map.insert(String::from("id"), Value::String(self.id.clone()));
        if let Some(transports) = &self.transports {
            map.insert(String::from("transports"), strings(transports));
        }
        Value::Object(map)
    }
}

/// What [`WebAuthn::registration_options`] builds from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegistrationInit {
    /// The relying party.
    pub rp: RelyingParty,
    /// The account.
    pub user: RegistrationUser,
    /// Passkeys the account holds already, which the browser skips.
    pub exclude: Option<Vec<CredentialDescriptor>>,
    /// Defaults to required: a discoverable passkey.
    pub resident_key: Option<ResidentKey>,
    /// Defaults to required; verification must expect the same.
    pub user_verification: Option<UserVerification>,
    /// COSE identifiers in preference order; defaults to
    /// [`DEFAULT_ALGORITHMS`]. Empty and unsupported ones are refused.
    pub algorithms: Option<Vec<i64>>,
    /// Defaults to [`DEFAULT_TIMEOUT_MS`]; positive and at most
    /// 2^53 - 1.
    pub timeout_ms: Option<u64>,
}

/// What [`WebAuthn::authentication_options`] builds from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthenticationInit {
    /// The relying-party ID.
    pub rp_id: String,
    /// Omitted, the user picks any passkey of the relying party.
    pub allow: Option<Vec<CredentialDescriptor>>,
    /// Defaults to required.
    pub user_verification: Option<UserVerification>,
    /// Defaults to [`DEFAULT_TIMEOUT_MS`].
    pub timeout_ms: Option<u64>,
}

/// The account as creation options name it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreationUser {
    /// The base64url handle.
    pub id: String,
    /// The account name.
    pub name: String,
    /// The display name.
    pub display_name: String,
}

/// Registration ceremony options: `to_value()` is the browser's
/// `PublicKeyCredentialCreationOptionsJSON`, defaults included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreationOptions {
    /// The fresh base64url challenge: store it until the response
    /// arrives, and accept it once.
    pub challenge: String,
    /// The relying party.
    pub rp: RelyingParty,
    /// The account.
    pub user: CreationUser,
    /// COSE identifiers in preference order.
    pub algorithms: Vec<i64>,
    /// The ceremony timeout.
    pub timeout_ms: u64,
    /// Passkeys the browser skips.
    pub exclude_credentials: Vec<CredentialDescriptor>,
    /// The resident-key requirement.
    pub resident_key: ResidentKey,
    /// The user-verification requirement.
    pub user_verification: UserVerification,
}

impl CreationOptions {
    /// The browser JSON. Attestation is always `none`: statements are
    /// never evaluated.
    pub fn to_value(&self) -> Value {
        let params = self
            .algorithms
            .iter()
            .map(|algorithm| {
                object(alloc::vec![
                    ("type", text("public-key")),
                    ("alg", Value::Number(*algorithm as f64)),
                ])
            })
            .collect();
        object(alloc::vec![
            ("challenge", Value::String(self.challenge.clone())),
            (
                "rp",
                object(alloc::vec![
                    ("id", Value::String(self.rp.id.clone())),
                    ("name", Value::String(self.rp.name.clone())),
                ]),
            ),
            (
                "user",
                object(alloc::vec![
                    ("id", Value::String(self.user.id.clone())),
                    ("name", Value::String(self.user.name.clone())),
                    ("displayName", Value::String(self.user.display_name.clone())),
                ]),
            ),
            ("pubKeyCredParams", Value::Array(params)),
            ("timeout", Value::Number(self.timeout_ms as f64)),
            (
                "excludeCredentials",
                descriptors_value(&self.exclude_credentials),
            ),
            (
                "authenticatorSelection",
                object(alloc::vec![
                    ("residentKey", text(self.resident_key.as_str())),
                    (
                        "requireResidentKey",
                        Value::Bool(self.resident_key == ResidentKey::Required),
                    ),
                    ("userVerification", text(self.user_verification.as_str())),
                ]),
            ),
            ("attestation", text("none")),
        ])
    }
}

/// Authentication ceremony options: `to_value()` is the browser's
/// `PublicKeyCredentialRequestOptionsJSON`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestOptions {
    /// The fresh base64url challenge: store it, accept it once.
    pub challenge: String,
    /// The relying-party ID.
    pub rp_id: String,
    /// The ceremony timeout.
    pub timeout_ms: u64,
    /// The user-verification requirement.
    pub user_verification: UserVerification,
    /// Passkeys the user may pick from; empty lets them pick any.
    pub allow_credentials: Vec<CredentialDescriptor>,
}

impl RequestOptions {
    /// The browser JSON.
    pub fn to_value(&self) -> Value {
        object(alloc::vec![
            ("challenge", Value::String(self.challenge.clone())),
            ("rpId", Value::String(self.rp_id.clone())),
            ("timeout", Value::Number(self.timeout_ms as f64)),
            ("userVerification", text(self.user_verification.as_str())),
            (
                "allowCredentials",
                descriptors_value(&self.allow_credentials),
            ),
        ])
    }
}

/// What a registration must match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistrationOptions {
    /// The base64url challenge issued for the ceremony.
    pub challenge: String,
    /// Allowed origins, such as `https://example.com`.
    pub origins: Vec<String>,
    /// The relying-party ID, such as `example.com`.
    pub rp_id: String,
    /// The user-verification requirement.
    pub user_verification: UserVerification,
    /// Accepted COSE algorithm identifiers.
    pub algorithms: Vec<i64>,
}

impl RegistrationOptions {
    /// Options for one origin, preferring user verification and
    /// accepting EdDSA, ES256 and RS256.
    pub fn new(challenge: &str, origin: &str, rp_id: &str) -> Self {
        RegistrationOptions {
            challenge: String::from(challenge),
            origins: alloc::vec![String::from(origin)],
            rp_id: String::from(rp_id),
            user_verification: UserVerification::Preferred,
            algorithms: alloc::vec![-8, -7, -257],
        }
    }
}

/// A stored credential, as an authentication checks it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredCredential {
    /// Base64url credential ID.
    pub id: String,
    /// Base64url COSE public key.
    pub public_key: String,
    /// The last accepted signature counter.
    pub sign_count: u32,
    /// The base64url user handle, when known.
    pub user_handle: Option<String>,
}

/// What an authentication must match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticationOptions {
    /// The base64url challenge issued for the ceremony.
    pub challenge: String,
    /// Allowed origins.
    pub origins: Vec<String>,
    /// The relying-party ID.
    pub rp_id: String,
    /// The user-verification requirement.
    pub user_verification: UserVerification,
    /// The credential the response must be for.
    pub credential: StoredCredential,
}

/// A registered credential: store it with the account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    /// Base64url credential ID.
    pub id: String,
    /// Base64url COSE public key.
    pub public_key: String,
    /// COSE algorithm identifier.
    pub algorithm: i64,
    /// The authenticator's signature counter.
    pub sign_count: u32,
    /// Transports the browser reported.
    pub transports: Vec<String>,
    /// Whether the credential may be backed up (synced passkeys).
    pub backup_eligible: bool,
    /// Whether it is backed up now.
    pub backup_state: bool,
    /// The authenticator model's AAGUID.
    pub aaguid: String,
}

impl Credential {
    /// The stored form an authentication checks.
    pub fn stored(&self, user_handle: Option<&str>) -> StoredCredential {
        StoredCredential {
            id: self.id.clone(),
            public_key: self.public_key.clone(),
            sign_count: self.sign_count,
            user_handle: user_handle.map(String::from),
        }
    }
}

/// A verified registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    /// The new credential.
    pub credential: Credential,
    /// Whether the user was verified.
    pub user_verified: bool,
    /// The attestation statement format, unevaluated.
    pub attestation_format: String,
}

/// A verified authentication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authentication {
    /// Base64url credential ID.
    pub credential_id: String,
    /// The new signature counter: store it.
    pub sign_count: u32,
    /// Whether the user was verified.
    pub user_verified: bool,
    /// Whether the credential may be backed up.
    pub backup_eligible: bool,
    /// Whether it is backed up now.
    pub backup_state: bool,
    /// The base64url user handle, when the authenticator returned one.
    pub user_handle: Option<String>,
}

/// WebAuthn operations over an invocation's crypto capability.
pub struct WebAuthn<'s, 'a, 'c, 'r>(pub(super) &'s mut Crypto<'a, 'c, 'r>);

impl WebAuthn<'_, '_, '_, '_> {
    /// A fresh base64url challenge of 32 random bytes: mutations only,
    /// unless a guest-local source is set.
    pub fn challenge(&mut self) -> Result<String> {
        let bytes = self.0.random_bytes(32)?;
        Ok(base64url_encode(&bytes))
    }

    /// Registration options with a fresh challenge, for
    /// `navigator.credentials.create()`. Invalid fields fail with
    /// `INVALID_VALUE`; the user name is checked before the challenge is
    /// drawn.
    pub fn registration_options(&mut self, init: &RegistrationInit) -> Result<CreationOptions> {
        nonempty(&init.user.name, "user.name")?;
        let challenge = self.challenge()?;
        creation_options(init, challenge)
    }

    /// Authentication options with a fresh challenge, for
    /// `navigator.credentials.get()`.
    pub fn authentication_options(&mut self, init: &AuthenticationInit) -> Result<RequestOptions> {
        let challenge = self.challenge()?;
        request_options(init, challenge)
    }

    /// Verify a registration response (`PublicKeyCredential.toJSON()`
    /// text).
    pub fn verify_registration(
        &mut self,
        response: &str,
        options: &RegistrationOptions,
    ) -> Result<Registration> {
        let mut map = expectations(
            &options.challenge,
            &options.origins,
            &options.rp_id,
            options.user_verification,
        );
        map.insert(
            String::from("algorithms"),
            Value::Array(
                options
                    .algorithms
                    .iter()
                    .map(|algorithm| Value::Number(*algorithm as f64))
                    .collect(),
            ),
        );
        let value = self.call("registration", response, map)?;
        let malformed = || fail(codes::TYPE_MISMATCH, "malformed registration");
        let Value::Object(root) = value else {
            return Err(malformed());
        };
        let Some(Value::Object(credential)) = root.get("credential") else {
            return Err(malformed());
        };
        let format = root
            .get("attestation")
            .and_then(|attestation| attestation.as_object())
            .and_then(|attestation| attestation.get("format"))
            .and_then(Value::as_str)
            .ok_or_else(malformed)?;
        let transports = match credential.get("transports") {
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| item.as_str().map(String::from).ok_or_else(malformed))
                .collect::<Result<Vec<_>>>()?,
            _ => return Err(malformed()),
        };
        Ok(Registration {
            credential: Credential {
                id: string(credential, "id")?,
                public_key: string(credential, "publicKey")?,
                algorithm: number(credential, "algorithm")? as i64,
                sign_count: number(credential, "signCount")? as u32,
                transports,
                backup_eligible: boolean(credential, "backupEligible")?,
                backup_state: boolean(credential, "backupState")?,
                aaguid: string(credential, "aaguid")?,
            },
            user_verified: boolean(&root, "userVerified")?,
            attestation_format: String::from(format),
        })
    }

    /// Verify an authentication response against a stored credential.
    pub fn verify_authentication(
        &mut self,
        response: &str,
        options: &AuthenticationOptions,
    ) -> Result<Authentication> {
        let mut map = expectations(
            &options.challenge,
            &options.origins,
            &options.rp_id,
            options.user_verification,
        );
        let stored = &options.credential;
        let mut credential = BTreeMap::new();
        credential.insert(String::from("id"), Value::String(stored.id.clone()));
        credential.insert(
            String::from("publicKey"),
            Value::String(stored.public_key.clone()),
        );
        credential.insert(
            String::from("signCount"),
            Value::Number(f64::from(stored.sign_count)),
        );
        if let Some(handle) = &stored.user_handle {
            credential.insert(String::from("userHandle"), Value::String(handle.clone()));
        }
        map.insert(String::from("credential"), Value::Object(credential));
        let value = self.call("authentication", response, map)?;
        let Value::Object(root) = value else {
            return Err(fail(codes::TYPE_MISMATCH, "malformed authentication"));
        };
        Ok(Authentication {
            credential_id: string(&root, "credentialId")?,
            sign_count: number(&root, "signCount")? as u32,
            user_verified: boolean(&root, "userVerified")?,
            backup_eligible: boolean(&root, "backupEligible")?,
            backup_state: boolean(&root, "backupState")?,
            user_handle: match root.get("userHandle") {
                None | Some(Value::Null) => None,
                Some(Value::String(handle)) => Some(handle.clone()),
                Some(_) => return Err(fail(codes::TYPE_MISMATCH, "malformed user handle")),
            },
        })
    }

    fn call(
        &mut self,
        operation: &str,
        response: &str,
        options: BTreeMap<String, Value>,
    ) -> Result<Value> {
        let options = Value::Object(options).canonical_json()?;
        self.0.call(
            CryptoOp::WebAuthn,
            alloc::vec![
                Value::String(String::from(operation)),
                Value::String(String::from(response)),
                Value::String(options),
            ],
        )
    }
}

/// Validate a registration init and fill its defaults.
fn creation_options(init: &RegistrationInit, challenge: String) -> Result<CreationOptions> {
    nonempty(&init.user.name, "user.name")?;
    nonempty(&init.rp.id, "rp.id")?;
    nonempty(&init.rp.name, "rp.name")?;
    handle(&init.user.id, "user.id", 64)?;
    let display_name = init.user.display_name.as_ref().unwrap_or(&init.user.name);
    nonempty(display_name, "user.displayName")?;
    Ok(CreationOptions {
        challenge,
        rp: init.rp.clone(),
        user: CreationUser {
            id: init.user.id.clone(),
            name: init.user.name.clone(),
            display_name: display_name.clone(),
        },
        algorithms: algorithms(init.algorithms.as_deref())?,
        timeout_ms: timeout(init.timeout_ms)?,
        exclude_credentials: descriptors(init.exclude.as_deref(), "exclude")?,
        resident_key: init.resident_key.unwrap_or_default(),
        user_verification: init.user_verification.unwrap_or_default(),
    })
}

/// Validate an authentication init and fill its defaults.
fn request_options(init: &AuthenticationInit, challenge: String) -> Result<RequestOptions> {
    nonempty(&init.rp_id, "rpId")?;
    Ok(RequestOptions {
        challenge,
        rp_id: init.rp_id.clone(),
        timeout_ms: timeout(init.timeout_ms)?,
        user_verification: init.user_verification.unwrap_or_default(),
        allow_credentials: descriptors(init.allow.as_deref(), "allow")?,
    })
}

fn invalid(message: String) -> crate::error::Failure {
    fail(codes::INVALID_VALUE, message)
}

fn nonempty(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        return Err(invalid(format!("{label} must be a nonempty string")));
    }
    Ok(())
}

/// A base64url handle of 1 to `max_bytes` bytes.
fn handle(value: &str, label: &str, max_bytes: usize) -> Result<()> {
    nonempty(value, label)?;
    let length = base64url_decode(value)
        .map_err(|_| invalid(format!("{label} must be base64url")))?
        .len();
    if !(1..=max_bytes).contains(&length) {
        return Err(invalid(format!(
            "{label} must be base64url of 1 to {max_bytes} bytes"
        )));
    }
    Ok(())
}

fn algorithms(value: Option<&[i64]>) -> Result<Vec<i64>> {
    let list = value.unwrap_or(&DEFAULT_ALGORITHMS);
    if list.is_empty()
        || list
            .iter()
            .any(|algorithm| !SUPPORTED_ALGORITHMS.contains(algorithm))
    {
        return Err(invalid(String::from(
            "algorithms must be a nonempty list of supported COSE identifiers",
        )));
    }
    Ok(list.to_vec())
}

fn timeout(value: Option<u64>) -> Result<u64> {
    let value = value.unwrap_or(DEFAULT_TIMEOUT_MS);
    if value == 0 || value > MAX_SAFE_INTEGER {
        return Err(invalid(String::from(
            "timeoutMs must be a positive safe integer",
        )));
    }
    Ok(value)
}

fn descriptors(
    list: Option<&[CredentialDescriptor]>,
    label: &str,
) -> Result<Vec<CredentialDescriptor>> {
    let list = list.unwrap_or_default();
    for entry in list {
        handle(&entry.id, &format!("{label} id"), 1023)?;
    }
    Ok(list.to_vec())
}

fn object(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(name, value)| (String::from(name), value))
            .collect(),
    )
}

fn text(value: &str) -> Value {
    Value::String(String::from(value))
}

fn strings(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}

fn descriptors_value(descriptors: &[CredentialDescriptor]) -> Value {
    Value::Array(
        descriptors
            .iter()
            .map(CredentialDescriptor::to_value)
            .collect(),
    )
}

fn expectations(
    challenge: &str,
    origins: &[String],
    rp_id: &str,
    user_verification: UserVerification,
) -> BTreeMap<String, Value> {
    let mut map = BTreeMap::new();
    map.insert(
        String::from("challenge"),
        Value::String(String::from(challenge)),
    );
    map.insert(
        String::from("origins"),
        Value::Array(origins.iter().cloned().map(Value::String).collect()),
    );
    map.insert(String::from("rpId"), Value::String(String::from(rp_id)));
    map.insert(
        String::from("userVerification"),
        Value::String(String::from(user_verification.as_str())),
    );
    map
}

fn string(map: &BTreeMap<String, Value>, name: &str) -> Result<String> {
    map.get(name)
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or_else(|| fail(codes::TYPE_MISMATCH, "malformed WebAuthn result"))
}

fn number(map: &BTreeMap<String, Value>, name: &str) -> Result<f64> {
    map.get(name)
        .and_then(Value::as_f64)
        .ok_or_else(|| fail(codes::TYPE_MISMATCH, "malformed WebAuthn result"))
}

fn boolean(map: &BTreeMap<String, Value>, name: &str) -> Result<bool> {
    map.get(name)
        .and_then(Value::as_bool)
        .ok_or_else(|| fail(codes::TYPE_MISMATCH, "malformed WebAuthn result"))
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Unpadded base64url, as WebAuthn and JOSE use.
pub fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let word = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for index in 0..=chunk.len() {
            out.push(ALPHABET[(word >> (18 - 6 * index) & 0x3F) as usize] as char);
        }
    }
    out
}

/// Decode base64url, with or without padding: padding counts only where the
/// whole text is a multiple of four long.
pub fn base64url_decode(text: &str) -> Result<Vec<u8>> {
    let invalid = || fail(codes::INVALID_VALUE, "invalid base64url");
    let trimmed = if text.len().is_multiple_of(4) {
        text.strip_suffix("==")
            .or_else(|| text.strip_suffix('='))
            .unwrap_or(text)
    } else {
        text
    };
    if trimmed.len() % 4 == 1 {
        return Err(invalid());
    }
    let mut out = Vec::with_capacity(trimmed.len() * 3 / 4);
    let mut word = 0u32;
    let mut bits = 0;
    for byte in trimmed.bytes() {
        let digit = ALPHABET
            .iter()
            .position(|candidate| *candidate == byte)
            .ok_or_else(invalid)? as u32;
        word = (word << 6) | digit;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((word >> bits) as u8);
            word &= (1 << bits) - 1;
        }
    }
    // Canonical encodings leave no stray bits.
    if word != 0 {
        return Err(invalid());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64url_matches_rfc4648_vectors() {
        for (bytes, text) in [
            (&b""[..], ""),
            (b"f", "Zg"),
            (b"fo", "Zm8"),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg"),
            (b"fooba", "Zm9vYmE"),
            (b"foobar", "Zm9vYmFy"),
            (&[0xfb, 0xff], "-_8"),
        ] {
            assert_eq!(base64url_encode(bytes), text);
            assert_eq!(base64url_decode(text).unwrap(), bytes);
        }
        assert_eq!(base64url_decode("Zg==").unwrap(), b"f");
        assert_eq!(base64url_decode("Zm8=").unwrap(), b"fo");
        assert!(base64url_decode("Zm9v!").is_err());
        // padding only where the length is a multiple of four, no stray bits,
        // the URL alphabet only.
        for invalid in [
            "====", "Z", "Zh", "Zm9", "Zg=", "Zg===", "Zg==\n", "Zm8+", "/w", "é", " ",
        ] {
            assert!(base64url_decode(invalid).is_err(), "{invalid:?}");
        }
    }

    fn registration_init() -> RegistrationInit {
        RegistrationInit {
            rp: RelyingParty {
                id: String::from("example.com"),
                name: String::from("Example"),
            },
            user: RegistrationUser {
                id: String::from("AQ=="),
                name: String::from("alice"),
                display_name: None,
            },
            ..RegistrationInit::default()
        }
    }

    fn member<'v>(value: &'v Value, path: &[&str]) -> &'v Value {
        path.iter().fold(value, |value, name| {
            value
                .as_object()
                .and_then(|map| map.get(*name))
                .unwrap_or_else(|| panic!("no {name} in {value:?}"))
        })
    }

    fn at(value: &Value, index: usize) -> &Value {
        &value.as_array().unwrap()[index]
    }

    #[test]
    fn creation_defaults_match_browser_canonical_json() {
        let options = creation_options(&registration_init(), String::from("challenge")).unwrap();
        assert_eq!(
            options.to_value().canonical_json().unwrap(),
            concat!(
                "{\"attestation\":\"none\",\"authenticatorSelection\":{\"requireResidentKey\":true,",
                "\"residentKey\":\"required\",\"userVerification\":\"required\"},",
                "\"challenge\":\"challenge\",\"excludeCredentials\":[],\"pubKeyCredParams\":[",
                "{\"alg\":-8,\"type\":\"public-key\"},{\"alg\":-7,\"type\":\"public-key\"},",
                "{\"alg\":-257,\"type\":\"public-key\"}],\"rp\":{\"id\":\"example.com\",",
                "\"name\":\"Example\"},\"timeout\":300000,\"user\":{\"displayName\":\"alice\",",
                "\"id\":\"AQ==\",\"name\":\"alice\"}}"
            )
        );
    }

    #[test]
    fn explicit_creation_settings_preserve_algorithms_and_transport_omission() {
        let mut init = registration_init();
        init.resident_key = Some(ResidentKey::Preferred);
        init.user_verification = Some(UserVerification::Discouraged);
        init.algorithms = Some(alloc::vec![-37, -36, -37]);
        init.timeout_ms = Some(MAX_SAFE_INTEGER);
        init.user.display_name = Some(String::from("Alice"));
        let descriptor = |id: &str, transports: Option<Vec<&str>>| CredentialDescriptor {
            id: String::from(id),
            transports: transports.map(|list| list.into_iter().map(String::from).collect()),
        };
        init.exclude = Some(alloc::vec![
            descriptor("AQ", None),
            descriptor("Ag", Some(alloc::vec![])),
            descriptor("Aw", Some(alloc::vec!["hybrid", ""])),
        ]);
        let value = creation_options(&init, String::from("x"))
            .unwrap()
            .to_value();
        assert_eq!(
            member(&value, &["timeout"]),
            &Value::Number(MAX_SAFE_INTEGER as f64)
        );
        assert_eq!(member(&value, &["user", "displayName"]), &text("Alice"));
        let selection = member(&value, &["authenticatorSelection"]);
        assert_eq!(
            member(selection, &["requireResidentKey"]),
            &Value::Bool(false)
        );
        assert_eq!(
            member(selection, &["userVerification"]),
            &text("discouraged")
        );
        let params = member(&value, &["pubKeyCredParams"]);
        assert_eq!(member(at(params, 0), &["alg"]), &Value::Number(-37.0));
        assert_eq!(member(at(params, 2), &["alg"]), &Value::Number(-37.0));
        let exclude = member(&value, &["excludeCredentials"]);
        assert!(
            !at(exclude, 0)
                .as_object()
                .unwrap()
                .contains_key("transports")
        );
        assert_eq!(
            member(at(exclude, 1), &["transports"]),
            &Value::Array(alloc::vec![])
        );
        assert_eq!(at(member(at(exclude, 2), &["transports"]), 1), &text(""));
    }

    #[test]
    fn authentication_defaults_and_explicit_allow_list_match_browser_json() {
        let mut init = AuthenticationInit {
            rp_id: String::from("example.com"),
            ..AuthenticationInit::default()
        };
        let value = request_options(&init, String::from("AA"))
            .unwrap()
            .to_value();
        assert_eq!(
            value.canonical_json().unwrap(),
            concat!(
                "{\"allowCredentials\":[],\"challenge\":\"AA\",\"rpId\":\"example.com\",",
                "\"timeout\":300000,\"userVerification\":\"required\"}"
            )
        );
        init.allow = Some(alloc::vec![CredentialDescriptor {
            id: String::from("AQ=="),
            transports: Some(alloc::vec![String::from("internal")]),
        }]);
        init.timeout_ms = Some(1);
        init.user_verification = Some(UserVerification::Preferred);
        let value = request_options(&init, String::from("AA"))
            .unwrap()
            .to_value();
        let allowed = at(member(&value, &["allowCredentials"]), 0);
        assert_eq!(member(allowed, &["type"]), &text("public-key"));
        assert_eq!(member(allowed, &["id"]), &text("AQ=="));
        assert_eq!(member(&value, &["timeout"]), &Value::Number(1.0));
        assert_eq!(member(&value, &["userVerification"]), &text("preferred"));
    }

    #[test]
    fn handle_bounds_and_base64url_validation_match_v1() {
        assert!(handle("AQ", "id", 64).is_ok());
        assert!(handle("AQ==", "id", 64).is_ok());
        for invalid in [
            "", "A", "AR", "AQ=", "AQ===", " A Q ", "a+b/", "====", "AA\n",
        ] {
            assert!(handle(invalid, "id", 64).is_err(), "{invalid:?}");
        }
        for limit in [64, 1023, 1024] {
            let at_limit = base64url_encode(&alloc::vec![0; limit]);
            let too_long = base64url_encode(&alloc::vec![0; limit + 1]);
            assert!(handle(&at_limit, "id", limit).is_ok());
            assert!(handle(&too_long, "id", limit).is_err());
        }
    }

    #[test]
    fn rejects_invalid_option_fields() {
        let refused = |init: RegistrationInit| {
            let failure = creation_options(&init, String::from("x")).unwrap_err();
            assert_eq!(failure.code, codes::INVALID_VALUE, "{failure:?}");
        };
        let mut init = registration_init();
        init.user.display_name = Some(String::new());
        refused(init);
        for algorithms in [alloc::vec![], alloc::vec![-8, 0]] {
            let mut init = registration_init();
            init.algorithms = Some(algorithms);
            refused(init);
        }
        let mut init = registration_init();
        init.user.id = base64url_encode(&[0; 65]);
        refused(init);
        let mut init = registration_init();
        init.rp.id.clear();
        refused(init);
        let mut init = registration_init();
        init.rp.name.clear();
        refused(init);
        let mut init = registration_init();
        init.user.name.clear();
        refused(init);
        assert!(request_options(&AuthenticationInit::default(), String::from("x")).is_err());
        for invalid in [0, MAX_SAFE_INTEGER + 1, u64::MAX] {
            assert!(timeout(Some(invalid)).is_err());
        }
        assert_eq!(timeout(None).unwrap(), 300_000);
        assert_eq!(timeout(Some(MAX_SAFE_INTEGER)).unwrap(), MAX_SAFE_INTEGER);
        assert!(descriptors(Some(&[CredentialDescriptor::default()]), "allow").is_err());
    }
}
