//! The ticket booth of v1's `examples/crypto-rs`, on v2's guest crypto:
//! every primitive runs in the host, keys never enter the guest, and
//! entropy is mutation-only.
//!
//! A ticket is a compact JWS (EdDSA) signed by the managed key
//! `tickets`, which the module declares (a `KeyDeclaration` component)
//! and its database's catalog binds to an Ed25519 key (`cargo flower
//! keys --db` `generate`, then `bind --alias tickets`). v1 signed HS256
//! tokens with key material it kept in a collection; here the key stays
//! in the host and standard JOSE libraries verify the tickets against
//! its public key.
//! Issued tickets are recorded by ID and can be revoked.
//!
//! - `ticket.issue` (mutation, `{guest, lifetimeSeconds}`): a fresh
//!   random ID, claims `{sub, iss, aud, iat, exp, jti}`, the signed
//!   ticket.
//! - `ticket.check` (query, `{ticket}`): the claims when the signature
//!   holds, the issuer and audience match, the ticket has not expired
//!   (`TICKET_EXPIRED`, at the invocation clock) and it was not revoked
//!   (`TICKET_REVOKED`); anything else is `INVALID_TICKET`.
//! - `ticket.revoke` (mutation, `{jti}`): whether a ticket was revoked.
//! - `crypto.digest` (query, `{text}`): SHA-256 of the text, hex.
//! - `crypto.randomQuery` (query): asks for entropy, which the host
//!   refuses outside mutations.

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec;

use flower_sdk::crypto::jwt::{SignOptions, ValidationOptions, VerifyOptions};
use flower_sdk::crypto::{Algorithm, KeyDeclaration, Usage, hex_encode};
use flower_sdk::{
    App, FlowerType, MutationCtx, QueryCtx, ReadCtx, Result, collection, fail, mutation, query,
};

/// The managed signing key this module declares.
static TICKET_KEY: KeyDeclaration =
    KeyDeclaration::new("tickets", Algorithm::Ed25519, &[Usage::Sign, Usage::Verify]);

const ISSUER: &str = "flower-garden";
const AUDIENCE: &str = "greenhouse";

/// What the booth keeps of an issued ticket.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Issued {
    /// The guest it was issued to.
    pub sub: String,
    /// When it expires, in seconds since the epoch.
    pub exp: f64,
}

/// Issued tickets by ID; revoking one deletes it.
#[collection(name = "tickets", key = String, value = Issued)]
pub struct Tickets;

/// A ticket's claims.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub iat: f64,
    pub exp: f64,
    pub jti: String,
}

/// A signed ticket with its claims.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Ticket {
    pub ticket: String,
    pub claims: Claims,
}

#[mutation(name = "ticket.issue", http, rename_all = "camelCase")]
fn issue(ctx: &mut MutationCtx<'_, '_>, guest: String, lifetime_seconds: f64) -> Result<Ticket> {
    if guest.is_empty() || !lifetime_seconds.is_finite() || lifetime_seconds < 1.0 {
        return Err(fail(
            "INVALID_ARGUMENT",
            "a guest and a lifetime of at least a second are required",
        ));
    }
    let now = (ctx.now()? / 1000.0).floor();
    let jti = hex_encode(&ctx.crypto().random_bytes(16)?);
    let claims = Claims {
        sub: guest.clone(),
        iss: ISSUER.to_string(),
        aud: AUDIENCE.to_string(),
        iat: now,
        exp: now + lifetime_seconds.ceil(),
        jti: jti.clone(),
    };
    let ticket = ctx.crypto().jwt().sign(
        &claims.encode()?,
        &TICKET_KEY.key()?,
        &SignOptions::default(),
    )?;
    Tickets::set(
        ctx,
        &jti,
        &Issued {
            sub: guest,
            exp: claims.exp,
        },
    )?;
    Ok(Ticket { ticket, claims })
}

#[query(name = "ticket.check", http)]
fn check(ctx: &mut QueryCtx<'_, '_>, ticket: String) -> Result<Claims> {
    let options = VerifyOptions {
        validation: ValidationOptions {
            issuer: Some(ISSUER.to_string()),
            audience: Some(vec![AUDIENCE.to_string()]),
            ..ValidationOptions::default()
        },
        ..VerifyOptions::default()
    };
    let invalid = || {
        fail(
            "INVALID_TICKET",
            "the ticket is malformed, forged or foreign",
        )
    };
    let verified = ctx
        .crypto()
        .jwt()
        .verify(&ticket, &TICKET_KEY.key()?, &options)
        .map_err(|failure| {
            if failure.message.contains("expired") {
                fail("TICKET_EXPIRED", "the ticket has expired")
            } else {
                invalid()
            }
        })?;
    let claims = Claims::decode(verified.claims).map_err(|_| invalid())?;
    if Tickets::get(ctx, &claims.jti)?.is_none() {
        return Err(fail("TICKET_REVOKED", "the ticket was revoked"));
    }
    Ok(claims)
}

/// Whether a ticket was revoked.
#[mutation(name = "ticket.revoke", http)]
fn revoke(ctx: &mut MutationCtx<'_, '_>, jti: String) -> Result<bool> {
    let existed = Tickets::get(ctx, &jti)?.is_some();
    Tickets::delete(ctx, &jti)?;
    Ok(existed)
}

/// SHA-256 of the text, hex.
#[query(name = "crypto.digest", http)]
fn digest(ctx: &mut QueryCtx<'_, '_>, text: String) -> Result<String> {
    Ok(hex_encode(&ctx.crypto().sha256(text.as_bytes())?))
}

/// Asks for entropy, which the host refuses: queries never get any.
#[query(name = "crypto.randomQuery", http)]
fn random_query(ctx: &mut QueryCtx<'_, '_>) -> Result<String> {
    Ok(hex_encode(&ctx.crypto().random_bytes(32)?))
}

pub static APP: App = App::new(&[
    &TICKET_KEY,
    &Tickets,
    &ISSUE,
    &CHECK,
    &REVOKE,
    &DIGEST,
    &RANDOM_QUERY,
]);

flower_sdk::export!(APP);

#[cfg(test)]
mod tests {
    use alloc::string::String;
    use alloc::vec::Vec;

    use flower_sdk::Value;
    use flower_sdk::abi::{InvocationKind, invoke_native, manifest_native};

    fn object(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(name, value)| (String::from(*name), value.clone()))
                .collect(),
        )
    }

    /// The code a mutation fails with before it touches the host.
    fn refused(name: &str, args: Value) -> String {
        let input = Value::Array(vec![Value::from(name), args])
            .encode_binary()
            .unwrap();
        let output = invoke_native(&super::APP, InvocationKind::Mutation as u32, &input);
        let Value::Array(envelope) = Value::decode_binary(&output).unwrap() else {
            panic!("response envelope");
        };
        assert_eq!(envelope[0], Value::from("error"), "{name} succeeded");
        match envelope[1].as_object().unwrap().get("code") {
            Some(Value::String(code)) => code.clone(),
            other => panic!("code not a string: {other:?}"),
        }
    }

    /// v1's and parity's wire: `{guest, lifetimeSeconds}`, and bad
    /// arguments are `INVALID_ARGUMENT`.
    #[test]
    fn tickets_take_v1s_arguments() {
        let issue = |guest: &str, lifetime: Value| {
            refused(
                "ticket.issue",
                object(&[("guest", Value::from(guest)), ("lifetimeSeconds", lifetime)]),
            )
        };
        // Past the argument contract, into the booth's own checks.
        assert_eq!(issue("ada", Value::Number(0.5)), "INVALID_ARGUMENT");
        assert_eq!(issue("", Value::Number(60.0)), "INVALID_ARGUMENT");
        // Refused by the contract.
        assert_eq!(issue("ada", Value::from("soon")), "INVALID_ARGUMENT");
        assert_eq!(
            refused(
                "ticket.issue",
                object(&[
                    ("guest", Value::from("ada")),
                    ("lifetime_seconds", Value::Number(60.0)),
                ]),
            ),
            "INVALID_ARGUMENT"
        );
        let decoded = <super::IssueArgs as flower_sdk::FlowerType>::decode(object(&[
            ("guest", Value::from("ada")),
            ("lifetimeSeconds", Value::Number(60.0)),
        ]))
        .unwrap();
        assert_eq!(decoded.lifetime_seconds, 60.0);
    }

    /// Exactly what parity's `http` list exposed.
    #[test]
    fn the_booth_answers_over_http_as_on_parity() {
        let bytes = manifest_native(&super::APP);
        let Value::Array(envelope) = Value::decode_binary(&bytes).unwrap() else {
            panic!("manifest envelope");
        };
        let Some(Value::Array(http)) = envelope[1].as_object().unwrap().get("http").cloned() else {
            panic!("manifest without http");
        };
        let mut aliases: Vec<String> = http
            .iter()
            .map(|entry| match entry.as_object().unwrap().get("alias") {
                Some(Value::String(alias)) => alias.clone(),
                other => panic!("alias not a string: {other:?}"),
            })
            .collect();
        aliases.sort();
        assert_eq!(
            aliases,
            [
                "crypto.digest",
                "crypto.randomQuery",
                "ticket.check",
                "ticket.issue",
                "ticket.revoke",
            ]
        );
    }
}
