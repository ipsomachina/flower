//! A real Flower guest exercising guest crypto through the SDK: NaCl
//! and JOSE over raw keys, and a declared catalog key issuing and
//! checking session tokens and signing messages. Compiled to `wasm32`
//! and driven through the wasmtime backend by `flower-engine`'s
//! `wasm_crypto` tests.

extern crate alloc;

use flower_sdk::crypto::jwt::{
    Algorithm as JwtAlgorithm, JwtKey, KeyFormat, SignOptions, ValidationOptions, Verified,
    VerifyOptions,
};
use flower_sdk::crypto::{
    Algorithm, Crypto, KeyDeclaration, ManagedKeyRef, Usage, hex_decode, hex_encode,
};
use flower_sdk::{App, MutationCtx, QueryCtx, Value, export, fail, mutation, query};

/// The catalog key alias this guest declares: the host resolves a
/// catalog key only for a module that declares it.
static SESSION_KEYS: [KeyDeclaration; 2] = [
    KeyDeclaration::new(
        "sessions",
        Algorithm::Ed25519,
        &[Usage::Sign, Usage::Verify],
    ),
    KeyDeclaration::new(
        "boxes",
        Algorithm::X25519,
        &[Usage::Derive, Usage::Encrypt, Usage::Decrypt],
    ),
];

struct Keys;

impl flower_sdk::app::Component for Keys {
    fn keys(&self) -> &[KeyDeclaration] {
        &SESSION_KEYS
    }
}

static KEYS: Keys = Keys;

/// Secretbox then open under a raw key, through the host: returns the
/// sealed hex and the opened text.
#[query]
pub fn nacl_roundtrip(
    ctx: &mut QueryCtx<'_, '_>,
    key: String,
    nonce: String,
    message: String,
) -> flower_sdk::Result<String> {
    let key = hex_decode(&key)?;
    let nonce: [u8; 24] = hex_decode(&nonce)?
        .try_into()
        .map_err(|_| fail("INVALID_VALUE", "nonce is 24 bytes"))?;
    let mut crypto = Crypto::new(ctx.ctx());
    let sealed = crypto
        .nacl()
        .secretbox(message.as_bytes(), &nonce, key.as_slice())?;
    let opened = crypto
        .nacl()
        .secretbox_open(&sealed, &nonce, key.as_slice())?
        .ok_or_else(|| fail("CRYPTO_ERROR", "secretbox did not open"))?;
    let opened =
        String::from_utf8(opened).map_err(|_| fail("INVALID_VALUE", "opened text is UTF-8"))?;
    Ok(format!("{}:{opened}", hex_encode(&sealed)))
}

/// Claims `{sub, exp}`.
fn session_claims(subject: String, exp: f64) -> Value {
    let mut claims = std::collections::BTreeMap::new();
    claims.insert(String::from("sub"), Value::String(subject));
    claims.insert(String::from("exp"), Value::Number(exp));
    Value::Object(claims)
}

/// The `sub` claim of a verified token.
fn subject_of(verified: Verified) -> flower_sdk::Result<String> {
    match verified.claims {
        Value::Object(mut claims) => match claims.remove("sub") {
            Some(Value::String(subject)) => Ok(subject),
            _ => Err(fail("INVALID_VALUE", "no subject")),
        },
        _ => Err(fail("INVALID_VALUE", "claims are an object")),
    }
}

/// Sign and verify an HS256 token under a raw secret, at the
/// invocation clock: returns the verified subject.
#[query]
pub fn jwt_roundtrip(
    ctx: &mut QueryCtx<'_, '_>,
    secret: String,
    subject: String,
    exp: f64,
) -> flower_sdk::Result<String> {
    let secret = hex_decode(&secret)?;
    let key = JwtKey::Bytes(&secret, KeyFormat::Raw);
    let mut crypto = Crypto::new(ctx.ctx());
    let token = crypto.jwt().sign(
        &session_claims(subject.clone(), exp),
        key,
        &SignOptions {
            algorithm: Some(JwtAlgorithm::Hs256),
            ..SignOptions::default()
        },
    )?;
    let verified = crypto.jwt().verify(
        &token,
        key,
        &VerifyOptions {
            algorithms: Some(vec![JwtAlgorithm::Hs256]),
            validation: ValidationOptions {
                subject: Some(subject),
                ..ValidationOptions::default()
            },
        },
    )?;
    subject_of(verified)
}

/// Issue a session token under the declared catalog key.
#[mutation]
pub fn issue_session(
    ctx: &mut MutationCtx<'_, '_>,
    subject: String,
    exp: f64,
) -> flower_sdk::Result<String> {
    let key = SESSION_KEYS[0].key()?;
    let mut crypto = Crypto::new(ctx.ctx());
    crypto
        .jwt()
        .sign(&session_claims(subject, exp), &key, &SignOptions::default())
}

/// Verify a session token under the declared catalog key.
#[query]
pub fn check_session(ctx: &mut QueryCtx<'_, '_>, token: String) -> flower_sdk::Result<String> {
    let key = SESSION_KEYS[0].key()?;
    let mut crypto = Crypto::new(ctx.ctx());
    let verified = crypto
        .jwt()
        .verify(&token, &key, &VerifyOptions::default())?;
    subject_of(verified)
}

/// Sign `message` under the declared catalog key's active version,
/// without reading the clock: hex.
#[query]
pub fn sign_message(ctx: &mut QueryCtx<'_, '_>, message: String) -> flower_sdk::Result<String> {
    let key = SESSION_KEYS[0].key()?;
    let signature =
        Crypto::new(ctx.ctx()).managed_sign(&key, ManagedKeyRef::Current, message.as_bytes())?;
    Ok(hex_encode(&signature))
}

/// Derive and use a non-exportable host handle in a real Wasm invocation.
#[query]
pub fn shared_roundtrip(ctx: &mut QueryCtx<'_, '_>, peer: String) -> flower_sdk::Result<String> {
    let peer: [u8; 32] = hex_decode(&peer)?
        .try_into()
        .map_err(|_| fail("INVALID_VALUE", "peer is 32 bytes"))?;
    let mut crypto = ctx.crypto();
    let shared =
        crypto
            .nacl()
            .box_before_managed(&peer, &SESSION_KEYS[1].key()?, ManagedKeyRef::Current)?;
    let sealed = crypto.nacl().box_after(b"shared", &[2; 24], &shared)?;
    let opened = crypto
        .nacl()
        .box_open_after(&sealed, &[2; 24], &shared)?
        .ok_or_else(|| fail("CRYPTO_ERROR", "shared box did not open"))?;
    Ok(hex_encode(&opened))
}

static DEFS: [&dyn flower_sdk::Component; 6] = [
    &NACL_ROUNDTRIP,
    &JWT_ROUNDTRIP,
    &ISSUE_SESSION,
    &CHECK_SESSION,
    &SIGN_MESSAGE,
    &SHARED_ROUNDTRIP,
];
static APP: App = App::new(&[&KEYS, &DEFS]);

export!(APP);

/// The aliases a guest's manifest exposes over HTTP.
#[cfg(test)]
fn http_aliases(app: &App) -> Vec<String> {
    use flower_sdk::Value;
    let bytes = flower_sdk::abi::manifest_native(app);
    let Value::Array(envelope) = Value::decode_binary(&bytes).unwrap() else {
        panic!("manifest envelope");
    };
    let Some(Value::Array(http)) = envelope[1].as_object().unwrap().get("http").cloned() else {
        panic!("manifest without http");
    };
    http.iter()
        .map(|entry| match entry.as_object().unwrap().get("alias") {
            Some(Value::String(alias)) => alias.clone(),
            other => panic!("alias not a string: {other:?}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    /// No methods are exposed over HTTP: engine tests call these methods, and
    /// `issue_session` signs a session for any subject, so none answers over
    /// HTTP.
    #[test]
    fn nothing_answers_over_http() {
        assert_eq!(super::http_aliases(&super::APP), Vec::<String>::new());
    }
}
