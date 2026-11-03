//! Guides › Guest cryptography.

use alloc::collections::BTreeMap;
use alloc::string::String;

use flower_sdk::crypto::jwt::{SignOptions, ValidationOptions, VerifyOptions};
use flower_sdk::crypto::webauthn::{
    AuthenticationInit, AuthenticationOptions, RegistrationInit, RegistrationOptions,
    RegistrationUser, RelyingParty, StoredCredential, UserVerification, base64url_encode,
};
use flower_sdk::crypto::{Algorithm, KeyDeclaration, ManagedKeyRef, Usage, hex_encode};
use flower_sdk::{
    App, FlowerType, MutationCtx, QueryCtx, ReadCtx, Result, Value, collection, fail, mutation,
    query,
};

// ANCHOR: digest
/// Hashing is deterministic, so queries may do it.
#[query(http)]
pub fn fingerprint(ctx: &mut QueryCtx<'_, '_>, text: String) -> Result<String> {
    let digest = ctx.crypto().sha256(text.as_bytes())?;
    Ok(hex_encode(&digest))
}

/// Entropy is for mutations only: their outcome is decided once, on
/// the leader, and replicated.
#[mutation(http)]
pub fn new_api_key(ctx: &mut MutationCtx<'_, '_>) -> Result<String> {
    let bytes = ctx.crypto().random_bytes(32)?;
    Ok(base64url_encode(&bytes))
}
// ANCHOR_END: digest

// ANCHOR: declare
/// The keys this module uses, by the alias an operator binds. Each is a
/// component: list it among the app's components. A declaration grants
/// nothing by itself: the binding must exist and grant the same usages.
pub static TICKETS: KeyDeclaration = KeyDeclaration::new(
    "tickets",
    Algorithm::Ed25519,
    &[Usage::Sign, Usage::Verify, Usage::PublicKey],
);

pub static VAULT: KeyDeclaration = KeyDeclaration::new(
    "vault",
    Algorithm::A256Gcm,
    &[Usage::Encrypt, Usage::Decrypt],
);
// ANCHOR_END: declare

const ISSUER: &str = "https://tickets.example.com";

// ANCHOR: jwt
/// Issue a signed ticket under the `tickets` key's active version.
#[mutation(http)]
pub fn issue_ticket(ctx: &mut MutationCtx<'_, '_>, guest: String) -> Result<String> {
    let now = ctx.clock()?;
    let mut claims = BTreeMap::new();
    claims.insert(String::from("iss"), Value::String(String::from(ISSUER)));
    claims.insert(String::from("sub"), Value::String(guest));
    claims.insert(String::from("iat"), Value::Number((now / 1000.0).floor()));
    claims.insert(
        String::from("exp"),
        Value::Number((now / 1000.0).floor() + 3600.0),
    );
    let key = TICKETS.key()?;
    // The binding fixes the algorithm (EdDSA) and the `kid` header.
    ctx.crypto()
        .jwt()
        .sign(&Value::Object(claims), &key, &SignOptions::default())
}

/// Check a ticket: the token's `kid` picks the key version, so tickets
/// issued before a rotation keep verifying until that version is
/// revoked. The answer is cached until the ticket expires.
#[query(http)]
pub fn ticket_holder(ctx: &mut QueryCtx<'_, '_>, ticket: String) -> Result<String> {
    let key = TICKETS.key()?;
    let options = VerifyOptions {
        validation: ValidationOptions {
            issuer: Some(String::from(ISSUER)),
            ..ValidationOptions::default()
        },
        ..VerifyOptions::default()
    };
    let verified = ctx
        .crypto()
        .jwt()
        .verify(&ticket, &key, &options)
        .map_err(|failure| fail("INVALID_TICKET", failure.message))?;
    match verified.claims {
        Value::Object(claims) => match claims.get("sub") {
            Some(Value::String(subject)) => Ok(subject.clone()),
            _ => Err(fail("INVALID_TICKET", "the ticket names no guest")),
        },
        _ => Err(fail("INVALID_TICKET", "the ticket has no claims")),
    }
}

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct PublicKey {
    pub kid: String,
    /// Ed25519: the raw 32-byte key, base64url.
    pub x: String,
}

/// Publish the verification key so others can check tickets with any
/// JOSE library (wrap it in a JWKS document as they expect).
#[query(http)]
pub fn ticket_key(ctx: &mut QueryCtx<'_, '_>) -> Result<PublicKey> {
    let key = TICKETS.key()?;
    let mut crypto = ctx.crypto();
    let (_, kid) = crypto.managed_version(&key)?;
    let public = crypto.managed_public_key(&key, ManagedKeyRef::Current)?;
    Ok(PublicKey {
        kid,
        x: base64url_encode(&public),
    })
}
// ANCHOR_END: jwt

// ANCHOR: jwe
/// Seal a value for later, readable only through this module.
#[mutation(http)]
pub fn seal(ctx: &mut MutationCtx<'_, '_>, secret: String) -> Result<String> {
    let now = ctx.clock()?;
    let mut claims = BTreeMap::new();
    claims.insert(String::from("secret"), Value::String(secret));
    claims.insert(
        String::from("exp"),
        Value::Number((now / 1000.0).floor() + 86_400.0),
    );
    let key = VAULT.key()?;
    // `None`: a fresh random nonce, which is why this is a mutation.
    ctx.crypto()
        .jwt()
        .encrypt(&Value::Object(claims), &key, None, &Default::default())
}

#[query(http)]
pub fn unseal(ctx: &mut QueryCtx<'_, '_>, token: String) -> Result<Value> {
    let key = VAULT.key()?;
    let opened = ctx
        .crypto()
        .jwt()
        .decrypt(&token, &key, &ValidationOptions::default())?;
    Ok(opened.claims)
}
// ANCHOR_END: jwe

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Challenge {
    pub value: String,
    pub expires_at: f64,
}

#[collection(name = "passkey_challenges", key = String, value = Challenge)]
pub struct Challenges;

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Passkey {
    pub user: String,
    pub public_key: String,
    pub sign_count: u32,
}

#[collection(name = "passkeys", key = String, value = Passkey)]
pub struct Passkeys;

// ANCHOR: webauthn
const RP_ID: &str = "example.com";
const ORIGIN: &str = "https://example.com";

/// Step 1: options for `navigator.credentials.create()`. The challenge
/// is random, so this is a mutation, and it is stored for step 2.
#[mutation(http)]
pub fn begin_registration(
    ctx: &mut MutationCtx<'_, '_>,
    user_handle: String,
    name: String,
) -> Result<Value> {
    let init = RegistrationInit {
        rp: RelyingParty {
            id: String::from(RP_ID),
            name: String::from("Example"),
        },
        user: RegistrationUser {
            // An opaque base64url handle (1–64 bytes), never an email.
            id: user_handle.clone(),
            name,
            display_name: None,
        },
        ..RegistrationInit::default()
    };
    let options = ctx.crypto().webauthn().registration_options(&init)?;
    let expires_at = ctx.clock()? + 300_000.0;
    let challenge = Challenge {
        value: options.challenge.clone(),
        expires_at,
    };
    Challenges::set(ctx, &user_handle, &challenge)?;
    Ok(options.to_value())
}

/// Step 2: verify the browser's `PublicKeyCredential.toJSON()` text,
/// consuming the challenge, and store the credential.
#[mutation(http)]
pub fn finish_registration(
    ctx: &mut MutationCtx<'_, '_>,
    user_handle: String,
    response: String,
) -> Result<String> {
    let Some(challenge) = Challenges::get(ctx, &user_handle)? else {
        return Err(fail("NO_CHALLENGE", "start the registration again"));
    };
    Challenges::delete(ctx, &user_handle)?;
    if ctx.clock()? > challenge.expires_at {
        return Err(fail("CHALLENGE_EXPIRED", "start the registration again"));
    }
    let options = RegistrationOptions {
        // Verify under the policy the options asked for.
        user_verification: UserVerification::Required,
        ..RegistrationOptions::new(&challenge.value, ORIGIN, RP_ID)
    };
    let registration = ctx
        .crypto()
        .webauthn()
        .verify_registration(&response, &options)?;
    let credential = registration.credential;
    let passkey = Passkey {
        user: user_handle,
        public_key: credential.public_key.clone(),
        sign_count: credential.sign_count,
    };
    Passkeys::set(ctx, &credential.id, &passkey)?;
    Ok(credential.id)
}

/// Sign-in, step 2 (step 1 stores a challenge from
/// `authentication_options`, as above): check the assertion and keep
/// the new signature counter.
#[mutation(http)]
pub fn finish_sign_in(
    ctx: &mut MutationCtx<'_, '_>,
    credential_id: String,
    challenge: String,
    response: String,
) -> Result<String> {
    let Some(mut passkey) = Passkeys::get(ctx, &credential_id)? else {
        return Err(fail("UNKNOWN_PASSKEY", "no such passkey"));
    };
    let options = AuthenticationOptions {
        challenge,
        origins: alloc::vec![String::from(ORIGIN)],
        rp_id: String::from(RP_ID),
        user_verification: UserVerification::Required,
        credential: StoredCredential {
            id: credential_id.clone(),
            public_key: passkey.public_key.clone(),
            sign_count: passkey.sign_count,
            user_handle: Some(passkey.user.clone()),
        },
    };
    let signed_in = ctx
        .crypto()
        .webauthn()
        .verify_authentication(&response, &options)?;
    passkey.sign_count = signed_in.sign_count;
    let user = passkey.user.clone();
    Passkeys::set(ctx, &credential_id, &passkey)?;
    Ok(user)
}
// ANCHOR_END: webauthn

/// Sign-in, step 1.
#[mutation(http)]
pub fn begin_sign_in(ctx: &mut MutationCtx<'_, '_>) -> Result<Value> {
    let init = AuthenticationInit {
        rp_id: String::from(RP_ID),
        ..AuthenticationInit::default()
    };
    let options = ctx.crypto().webauthn().authentication_options(&init)?;
    Ok(options.to_value())
}

// ANCHOR: app
pub static APP: App = App::new(&[
    &TICKETS,
    &VAULT,
    &Challenges,
    &Passkeys,
    &FINGERPRINT,
    &NEW_API_KEY,
    &ISSUE_TICKET,
    &TICKET_HOLDER,
    &TICKET_KEY,
    &SEAL,
    &UNSEAL,
    &BEGIN_REGISTRATION,
    &FINISH_REGISTRATION,
    &BEGIN_SIGN_IN,
    &FINISH_SIGN_IN,
]);
// ANCHOR_END: app
