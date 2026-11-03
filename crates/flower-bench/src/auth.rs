//! `--auth`: end-user credentials for the authenticated kitchen.
//!
//! As v1's `bench/pizza-auth.mjs`: each run makes its own Ed25519
//! signing key and mints one EdDSA JWT per tenant (`sub:
//! customer@<tenant>`, `tenant`) and one administrator token (`sub:
//! goblin-admin`, `role: admin`), issuer and audience `goblin-pizza`,
//! valid two days. The public half goes to `pizza.setup`, which only a
//! trusted gateway (the holder of the server's admin token, asserting
//! the administrator principal) may call before a key exists, so only
//! the deployer decides whose tokens the kitchen accepts. The private
//! half never leaves this process; tokens travel only in request bodies
//! (`credentials: {bearer}`) and watch `Authorization` headers, never in
//! argv, reports or logs.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use ed25519_dalek::{Signer, SigningKey};
use serde_json::{Value, json};

/// Issuer and audience of every kitchen token: the guest's `ISSUER`.
pub const ISSUER: &str = "goblin-pizza";
/// How long tokens stay valid: any run or demo ends sooner.
pub const TOKEN_SECONDS: u64 = 2 * 86_400;
/// The administrator's subject.
pub const ADMIN_SUBJECT: &str = "goblin-admin";

/// SPKI DER prefix of an Ed25519 public key (RFC 8410).
const SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// This run's signing key and the tokens minted with it.
pub struct Identities {
    key: SigningKey,
    admin: String,
    tenants: BTreeMap<String, String>,
}

impl std::fmt::Debug for Identities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print a token or the key.
        f.debug_struct("Identities")
            .field("tenants", &self.tenants.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let triple = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for index in 0..=chunk.len() {
            out.push(char::from(
                ALPHABET[(triple >> (18 - 6 * index) & 63) as usize],
            ));
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_secs())
}

/// A compact EdDSA JWS of `claims` under `key`.
pub fn mint(key: &SigningKey, claims: &Value) -> String {
    let header = base64url(br#"{"alg":"EdDSA","typ":"JWT"}"#);
    let payload = base64url(&serde_json::to_vec(claims).expect("JSON values serialize"));
    let signed = format!("{header}.{payload}");
    let signature = key.sign(signed.as_bytes());
    format!("{signed}.{}", base64url(&signature.to_bytes()))
}

/// The standard claims every kitchen token carries, plus `extra`.
pub fn claims(subject: &str, extra: &[(&str, &str)], issued_at: u64) -> Value {
    let mut claims = json!({
        "iss": ISSUER,
        "aud": ISSUER,
        "sub": subject,
        "iat": issued_at,
        "exp": issued_at + TOKEN_SECONDS,
    });
    for (name, value) in extra {
        claims[*name] = Value::String((*value).to_string());
    }
    claims
}

impl Identities {
    /// A fresh signing key from the operating system's randomness, and
    /// tokens for `tenants` and the administrator.
    pub fn generate<'a>(tenants: impl IntoIterator<Item = &'a String>) -> Result<Self, String> {
        let mut seed = [0u8; 32];
        getrandom::getrandom(&mut seed).map_err(|error| format!("randomness: {error}"))?;
        Ok(Self::from_seed(seed, tenants, now_seconds()))
    }

    /// Identities from a known seed, issued at `issued_at` (seconds).
    pub fn from_seed<'a>(
        seed: [u8; 32],
        tenants: impl IntoIterator<Item = &'a String>,
        issued_at: u64,
    ) -> Self {
        let key = SigningKey::from_bytes(&seed);
        let admin = mint(
            &key,
            &claims(ADMIN_SUBJECT, &[("role", "admin")], issued_at),
        );
        let tenants = tenants
            .into_iter()
            .map(|tenant| {
                let subject = format!("customer@{tenant}");
                let token = mint(&key, &claims(&subject, &[("tenant", tenant)], issued_at));
                (tenant.clone(), token)
            })
            .collect();
        Identities {
            key,
            admin,
            tenants,
        }
    }

    /// The public key `pizza.setup` installs: SPKI DER, hex.
    pub fn public_key_hex(&self) -> String {
        let mut der = SPKI_PREFIX.to_vec();
        der.extend_from_slice(self.key.verifying_key().as_bytes());
        hex(&der)
    }

    /// The administrator's token: setup by gateway aside, the world
    /// audit's.
    pub fn admin(&self) -> &str {
        &self.admin
    }

    /// One tenant's token.
    pub fn tenant(&self, tenant: &str) -> Option<&str> {
        self.tenants.get(tenant).map(String::as_str)
    }

    /// A token for `tenant` signed by a key the kitchen never trusted:
    /// the audit's forgery probe.
    pub fn forged(tenant: &str) -> String {
        let stranger = SigningKey::from_bytes(&[7u8; 32]);
        let subject = format!("customer@{tenant}");
        mint(
            &stranger,
            &claims(&subject, &[("tenant", tenant)], now_seconds()),
        )
    }

    /// The principal the bench asserts as a gateway to open the
    /// kitchens: the administrator.
    pub fn gateway_principal() -> Value {
        json!({"subject": ADMIN_SUBJECT, "claims": {"role": "admin"}})
    }
}

/// `{bearer: token}`: the call body's credentials.
pub fn bearer(token: &str) -> Value {
    json!({"bearer": token})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64url_is_unpadded_rfc4648() {
        assert_eq!(base64url(b""), "");
        assert_eq!(base64url(b"f"), "Zg");
        assert_eq!(base64url(b"fo"), "Zm8");
        assert_eq!(base64url(b"foo"), "Zm9v");
        assert_eq!(base64url(&[0xfb, 0xff]), "-_8");
    }

    #[test]
    fn tokens_verify_with_the_published_key_and_name_their_tenant() {
        let tenants = vec!["tenant-0".to_string(), "tenant-1".to_string()];
        let issued = now_seconds();
        let identities = Identities::from_seed([42u8; 32], &tenants, issued);
        let key = (0..identities.public_key_hex().len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&identities.public_key_hex()[at..at + 2], 16).unwrap())
            .collect::<Vec<u8>>();
        assert_eq!(&key[..12], &SPKI_PREFIX);
        // The server's own verifier, as the guest's hook calls it.
        let options = flower_crypto::jwt::VerifyOptions {
            algorithms: vec![flower_crypto::jwt::Algorithm::EdDSA],
            key_format: flower_crypto::jwt::KeyFormat::Der,
            issuer: Some(ISSUER.to_string()),
            audience: Some(vec![ISSUER.to_string()]),
            subject: None,
            clock_tolerance_seconds: 0.0,
            require_expiration: true,
            typ: None,
        };
        let now_ms = issued * 1000;
        let token = identities.tenant("tenant-1").unwrap();
        let verified = flower_crypto::jwt::verify(token, &key, &options, now_ms)
            .expect("the server's verifier accepts the bench's token");
        assert_eq!(verified.claims["tenant"], "tenant-1");
        assert_eq!(verified.claims["sub"], "customer@tenant-1");
        let admin = flower_crypto::jwt::verify(identities.admin(), &key, &options, now_ms)
            .expect("the admin token verifies");
        assert_eq!(admin.claims["role"], "admin");
        assert!(admin.claims.get("tenant").is_none());
        // A token under another key does not.
        assert!(
            flower_crypto::jwt::verify(&Identities::forged("tenant-1"), &key, &options, now_ms)
                .is_err()
        );
        // Nor once it has expired.
        let late = (issued + TOKEN_SECONDS + 3_600) * 1000;
        assert!(flower_crypto::jwt::verify(token, &key, &options, late).is_err());
        // Debug output never shows a token.
        assert!(!format!("{identities:?}").contains(token));
    }
}
