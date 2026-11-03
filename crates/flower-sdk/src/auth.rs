//! Authentication: the [`Principal`] wire type and the [`Authorization`]
//! component behind a module's authorization hook.
//!
//! A module that lists an [`Authorization`] among its components gets an
//! authorization hook: before every call to the module from outside (an
//! HTTP alias, a local engine call, a watch delivery), and before any
//! idempotency replay, the host invokes the hook as a query named
//! [`AUTHORIZE`] with `{credentials, method, args, principal}`. The hook
//! answers `{principal}` (`null` for anonymous), which the call then runs
//! as, or fails, which denies the call. Credentials stay outside the
//! business arguments, fingerprints and receipts.
//!
//! The host keeps a decision and reuses it for later calls with the same
//! credentials and method while nothing it read changes, its code stays
//! deployed and its time is not up. The hook says what that needs: its
//! answer carries `reads_args: false` when neither the authentication nor
//! the method's access looked at the arguments (a [typed](Authenticator)
//! or gateway authenticator with public or authenticated access), so the
//! decision holds whatever the arguments, and `until` when the
//! authentication expires (a bearer token's `exp`), so the decision is
//! kept until then instead of rechecked on every call.
//!
//! Who may call each method is declared on the method itself
//! ([`Method::access`][crate::app::Method::access], or
//! `#[mutation(access = ...)]`) or transaction
//! ([`Transaction::access`][crate::app::Transaction::access]). Definitions
//! another macro declares (an [`external!`](crate::external!) value's
//! operations, an interface provider's methods) get theirs from the
//! component's [`methods`](Authorization::methods); its `default` covers
//! the rest.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;

use crate::crypto::jwt::{
    Algorithm as JwsAlgorithm, JwtKey, KeyFormat, ValidationOptions, VerifyOptions,
};
use crate::crypto::{CRYPTO_ERROR, Crypto, KeyDeclaration};
use crate::ctx::{Ctx, QueryCtx, wrap_query};
use crate::error::{Result, codes, fail};
use crate::schema::{Schema, v};
use crate::typed::FlowerType;
use crate::value::Value;

/// Reserved subject no principal may carry.
pub const ANONYMOUS_SUBJECT: &str = "$anonymous";

/// The hook's invocation name. The host calls it as a query; it is not a
/// definition, so neither aliases nor guest calls reach it.
pub const AUTHORIZE: &str = "$flower.authorize";

/// The methods retry sessions present to the hook: `$flower.session.open`,
/// `.status`, `.ack` and `.close`. [`Authorization::sessions`] decides
/// them.
pub const SESSION_METHODS: &str = "$flower.session.";

/// What the hook decides about, besides the credentials.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthRequest {
    /// The HTTP alias called, or the definition name for local calls.
    pub method: String,
    /// The call's arguments.
    pub args: Value,
    /// The principal a trusted gateway (or a local embedder) asserted.
    pub principal: Option<Principal>,
}

/// Credential verification: credentials plus request in, principal out
/// (`None` for anonymous). Failing denies the call. It sees the whole
/// request, arguments included, so its decisions hold per arguments;
/// an [`Authenticator`] sees the credentials alone.
pub type Authenticate =
    for<'a, 'c, 'r> fn(&'a mut QueryCtx<'c, 'r>, &Value, &AuthRequest) -> Result<Option<Principal>>;

/// A per-method permission check over the authenticated principal and the
/// call's arguments.
pub type Permission =
    for<'a, 'c, 'r> fn(&'a mut QueryCtx<'c, 'r>, Option<&Principal>, &Value) -> Result<bool>;

/// Who an [`Authenticator`] found, and until when that holds.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Authenticated {
    /// The caller; `None` for anonymous.
    pub principal: Option<Principal>,
    /// When the authentication stops holding, in milliseconds since the
    /// Unix epoch (a token's expiry); `None` while nothing it read
    /// changes.
    pub until: Option<f64>,
}

/// A typed authenticator: verifies credentials alone, never the call's
/// arguments, so its decisions hold for any arguments. One that reads
/// the clock reports the instant its outcome changes in
/// [`Authenticated::until`] instead of polling, so the host keeps its
/// decisions until then. [`JwtBearer`] is one.
pub trait Authenticator: Sync {
    /// Verify `credentials`: who presented them, or a failure that denies
    /// the call.
    fn authenticate(
        &self,
        ctx: &mut QueryCtx<'_, '_>,
        credentials: &Value,
    ) -> Result<Authenticated>;

    /// Catalog keys it verifies with: they join the module's keys.
    fn keys(&self) -> &'static [KeyDeclaration] {
        &[]
    }
}

/// How the hook finds the caller.
#[derive(Clone, Copy)]
pub enum Authentication {
    /// Verify the presented credentials with the whole request.
    Callback(Authenticate),
    /// Verify the presented credentials alone.
    Typed(&'static dyn Authenticator),
    /// Trust the principal a gateway holding the admin credential
    /// asserted, as is.
    Gateway,
    /// Every caller is anonymous.
    Anonymous,
}

/// Who may call a method.
#[derive(Clone, Copy)]
pub enum Access {
    /// Everyone, anonymous callers included.
    Public,
    /// Authenticated callers only.
    Authenticated,
    /// Callers the check accepts. It sees the arguments, so its
    /// decisions hold per arguments.
    Check(Permission),
}

impl core::fmt::Debug for Access {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Access::Public => "Public",
            Access::Authenticated => "Authenticated",
            Access::Check(_) => "Check",
        })
    }
}

/// The authorization component: authentication, the access of methods
/// that declare none, and access to retry sessions. List it among the
/// app's components; at most one per module.
///
/// ```ignore
/// static AUTH: Authorization = Authorization {
///     authenticate: Authentication::Typed(&BEARER),
///     default: Access::Check(owns),
///     sessions: Access::Authenticated,
///     // `external!(static WORDS = "words", ...)` declares `words.publish`.
///     methods: &[("words.publish", Access::Check(worker))],
/// };
/// ```
pub struct Authorization {
    /// How callers authenticate.
    pub authenticate: Authentication,
    /// Access for methods that declare none.
    pub default: Access,
    /// Access to retry sessions: opening, reading, acknowledging and
    /// closing them ([`SESSION_METHODS`]).
    pub sessions: Access,
    /// Access for definitions declared where no `access` option reaches,
    /// by name: an [`external!`](crate::external!) value's operations
    /// (`words.publish`), an interface provider's methods. The manifest
    /// refuses a name the app does not define, one named twice, and one
    /// whose definition declares its own access.
    pub methods: &'static [(&'static str, Access)],
}

impl Authorization {
    /// Run the hook: decode the request, authenticate, check the method's
    /// access (`declared` answers what a method alias declares, else
    /// [`methods`](Self::methods) does) and answer `{principal,
    /// reads_args, until?}`.
    pub fn decide<'c, 'r>(
        &self,
        ctx: &'c mut Ctx<'c, 'r>,
        request: Value,
        declared: &dyn Fn(&str) -> Option<Access>,
    ) -> Result<Value> {
        let mut fields = match request {
            Value::Object(fields) => fields,
            _ => {
                return Err(fail(
                    codes::TYPE_MISMATCH,
                    "authorization request not an object",
                ));
            }
        };
        let method = match fields.remove("method") {
            Some(Value::String(method)) => method,
            _ => {
                return Err(fail(
                    codes::TYPE_MISMATCH,
                    "authorization request needs method",
                ));
            }
        };
        let credentials = fields.remove("credentials").unwrap_or(Value::Null);
        let asserted = match fields.remove("principal") {
            None | Some(Value::Null) => None,
            Some(value) => Some(Principal::decode(value)?),
        };
        let request = AuthRequest {
            method,
            args: fields.remove("args").unwrap_or(Value::Null),
            principal: asserted,
        };
        let ctx = wrap_query(ctx);
        let (principal, until, mut reads_args) = match self.authenticate {
            Authentication::Callback(authenticate) => {
                (authenticate(&mut *ctx, &credentials, &request)?, None, true)
            }
            Authentication::Typed(authenticator) => {
                let found = authenticator.authenticate(&mut *ctx, &credentials)?;
                (found.principal, found.until, false)
            }
            Authentication::Gateway => (request.principal.clone(), None, false),
            Authentication::Anonymous => (None, None, false),
        };
        let access = if request.method.starts_with(SESSION_METHODS) {
            self.sessions
        } else {
            declared(&request.method)
                .or_else(|| {
                    self.methods
                        .iter()
                        .find(|(name, _)| *name == request.method)
                        .map(|(_, access)| *access)
                })
                .unwrap_or(self.default)
        };
        match access {
            Access::Public => {}
            Access::Authenticated if principal.is_none() => {
                return Err(fail(codes::UNAUTHENTICATED, "authentication required"));
            }
            Access::Authenticated => {}
            Access::Check(check) => {
                reads_args = true;
                if !check(&mut *ctx, principal.as_ref(), &request.args)? {
                    return Err(fail(codes::FORBIDDEN, "access denied"));
                }
            }
        }
        let mut decision = BTreeMap::new();
        decision.insert(
            String::from("principal"),
            match &principal {
                Some(principal) => principal.encode()?,
                None => Value::Null,
            },
        );
        decision.insert(String::from("reads_args"), Value::Bool(reads_args));
        if let Some(until) = until {
            decision.insert(String::from("until"), Value::Number(until));
        }
        Ok(Value::Object(decision))
    }
}

impl crate::app::Component for Authorization {
    fn authorizes(&self) -> bool {
        true
    }

    fn authorize<'c, 'r>(
        &self,
        ctx: &'c mut Ctx<'c, 'r>,
        request: Value,
        declared: &dyn Fn(&str) -> Option<Access>,
    ) -> Option<Result<Value>> {
        Some(self.decide(ctx, request, declared))
    }

    fn granted_access(&self) -> &[(&'static str, Access)] {
        self.methods
    }

    fn keys(&self) -> &[KeyDeclaration] {
        match self.authenticate {
            Authentication::Typed(authenticator) => authenticator.keys(),
            _ => &[],
        }
    }
}

/// The key a [`JwtBearer`] verifies with.
#[derive(Clone, Copy)]
pub enum BearerKey {
    /// A catalog key: its binding decides the algorithm, and it joins the
    /// module's keys.
    Managed(&'static KeyDeclaration),
    /// Raw HS256 secret or DER public key bytes, with the algorithms
    /// tokens may use.
    Bytes(&'static [u8], KeyFormat, &'static [JwsAlgorithm]),
    /// A PEM public key, with the algorithms tokens may use.
    Pem(&'static str, &'static [JwsAlgorithm]),
}

/// Bearer JWT authentication, verified natively. Credentials may be the
/// token, `"Bearer <token>"`, `{"bearer": …}` (what the server makes of an
/// `Authorization: Bearer` header) or `{"token": …}`; `null` is anonymous and
/// anything else fails `UNAUTHENTICATED`, as does a token that fails
/// verification. Key trouble (an unknown or forbidden catalog key) keeps
/// its own code: the token is not at fault. Tokens need an `exp`, and
/// the decision holds until then (less the clock tolerance it allows
/// past it), so the host keeps it instead of verifying every call.
///
/// Build one with [`JwtBearer::new`] and struct update syntax:
/// `JwtBearer { audience: &["api"], ..JwtBearer::new(key) }`.
pub struct JwtBearer {
    /// What tokens verify with.
    pub key: BearerKey,
    /// The required `iss`, if any.
    pub issuer: Option<&'static str>,
    /// Accepted audiences; a token with an `aud` needs one of them.
    pub audience: &'static [&'static str],
    /// Clock skew allowed on `exp` and `nbf`, in seconds.
    pub clock_tolerance_seconds: f64,
    /// Who verified claims name; [`claims_principal`] by default.
    pub principal: fn(&Value) -> Result<Option<Principal>>,
}

impl JwtBearer {
    /// Verify with `key`, no issuer or audience, no clock tolerance, and
    /// [`claims_principal`].
    pub const fn new(key: BearerKey) -> Self {
        JwtBearer {
            key,
            issuer: None,
            audience: &[],
            clock_tolerance_seconds: 0.0,
            principal: claims_principal,
        }
    }
}

/// The principal verified claims name: `sub`, a `tenant` claim when
/// present, and all claims. A token without a subject fails
/// `UNAUTHENTICATED`.
pub fn claims_principal(claims: &Value) -> Result<Option<Principal>> {
    let claim = |name: &str| claims.as_object().and_then(|map| map.get(name));
    let Some(Value::String(subject)) = claim("sub").filter(|subject| match subject {
        Value::String(subject) => !subject.is_empty(),
        _ => false,
    }) else {
        return Err(fail(codes::UNAUTHENTICATED, "the token has no subject"));
    };
    let tenant = match claim("tenant") {
        Some(Value::String(tenant)) if !tenant.is_empty() => Some(tenant.clone()),
        _ => None,
    };
    Principal::new(subject.clone(), tenant, Some(claims.clone()))
        .map(Some)
        .map_err(|_| fail(codes::UNAUTHENTICATED, "the token's subject is reserved"))
}

/// The token in bearer credentials; `None` for anonymous.
fn bearer_token(credentials: &Value) -> Result<Option<&str>> {
    let token = match credentials {
        Value::Null => return Ok(None),
        Value::String(token) => Some(token),
        // The server presents an `Authorization: Bearer` header as
        // `{"bearer": token}`; a body may say `{"token": …}`.
        Value::Object(map) => match (map.get("bearer"), map.get("token")) {
            (Some(Value::String(token)), None) | (None, Some(Value::String(token))) => Some(token),
            _ => None,
        },
        _ => None,
    };
    match token {
        Some(token) => Ok(Some(token.strip_prefix("Bearer ").unwrap_or(token))),
        None => Err(fail(
            codes::UNAUTHENTICATED,
            "credentials must be a bearer token, {bearer} or {token}",
        )),
    }
}

impl Authenticator for JwtBearer {
    fn authenticate(
        &self,
        ctx: &mut QueryCtx<'_, '_>,
        credentials: &Value,
    ) -> Result<Authenticated> {
        let Some(token) = bearer_token(credentials)? else {
            return Ok(Authenticated::default());
        };
        // Verify at the invocation clock without polling it: the decision
        // reports when the token expires instead.
        let now = ctx.ctx().clock()? as u64 as f64;
        let algorithms = match self.key {
            BearerKey::Managed(_) => None,
            BearerKey::Bytes(_, _, algorithms) | BearerKey::Pem(_, algorithms) => {
                Some(algorithms.to_vec())
            }
        };
        let options = VerifyOptions {
            algorithms,
            validation: ValidationOptions {
                issuer: self.issuer.map(String::from),
                audience: (!self.audience.is_empty())
                    .then(|| self.audience.iter().map(|aud| String::from(*aud)).collect()),
                clock_tolerance_seconds: Some(self.clock_tolerance_seconds),
                ..ValidationOptions::default()
            },
        };
        let managed;
        let key = match self.key {
            BearerKey::Managed(declaration) => {
                managed = declaration.key()?;
                JwtKey::from(&managed)
            }
            BearerKey::Bytes(bytes, format, _) => JwtKey::Bytes(bytes, format),
            BearerKey::Pem(text, _) => JwtKey::Pem(text),
        };
        let verified = Crypto::new(ctx.ctx())
            .jwt()
            .verify_with_clock(token, key, &options, Some(now))
            .map_err(|failure| {
                if failure.code == CRYPTO_ERROR {
                    fail(
                        codes::UNAUTHENTICATED,
                        format!("invalid bearer token: {}", failure.message),
                    )
                } else {
                    failure
                }
            })?;
        let principal = (self.principal)(&verified.claims)?;
        // Verification passed `now - tolerance < exp`: it holds until
        // `(exp + tolerance)` seconds, a millisecond early against
        // rounding.
        let until = match verified.claims.as_object().and_then(|map| map.get("exp")) {
            Some(Value::Number(exp)) if exp.is_finite() => {
                Some((exp + self.clock_tolerance_seconds) * 1000.0 - 1.0)
            }
            _ => None,
        };
        Ok(Authenticated { principal, until })
    }

    fn keys(&self) -> &'static [KeyDeclaration] {
        match self.key {
            BearerKey::Managed(declaration) => core::slice::from_ref(declaration),
            _ => &[],
        }
    }
}

/// Authenticated identity: subject, optional tenant and optional claims.
#[derive(Clone, Debug, PartialEq)]
pub struct Principal {
    /// Non-empty, never [`ANONYMOUS_SUBJECT`].
    pub subject: String,
    /// Business tenant, if any.
    pub tenant: Option<String>,
    /// Opaque claim set, if any.
    pub claims: Option<Value>,
}

impl Principal {
    /// Build a principal, rejecting empty and reserved-anonymous subjects.
    pub fn new(
        subject: String,
        tenant: Option<String>,
        claims: Option<Value>,
    ) -> Result<Principal> {
        if subject.is_empty() || subject == ANONYMOUS_SUBJECT {
            return Err(fail(codes::INVALID_VALUE, "invalid subject"));
        }
        Ok(Principal {
            subject,
            tenant,
            claims,
        })
    }
}

impl FlowerType for Principal {
    const SCHEMA: Schema = v::object(&[
        v::field("subject", v::string().build()),
        v::optional("tenant", v::string().build()),
        v::optional("claims", v::json()),
    ]);

    fn encode(&self) -> Result<Value> {
        let mut map = alloc::collections::BTreeMap::new();
        map.insert(String::from("subject"), Value::String(self.subject.clone()));
        map.insert(
            String::from("tenant"),
            match &self.tenant {
                Some(tenant) => Value::String(tenant.clone()),
                None => Value::Null,
            },
        );
        map.insert(
            String::from("claims"),
            match &self.claims {
                Some(claims) => claims.clone(),
                None => Value::Null,
            },
        );
        Ok(Value::Object(map))
    }

    fn decode(value: Value) -> Result<Self> {
        // Lenient on absent members: principals are host-produced
        // infrastructure, always sent complete, read defensively.
        let map = match value {
            Value::Object(map) => map,
            _ => return Err(fail(codes::TYPE_MISMATCH, "expected principal object")),
        };
        let subject = match map.get("subject") {
            Some(Value::String(subject)) => subject.clone(),
            _ => return Err(fail(codes::TYPE_MISMATCH, "principal needs a subject")),
        };
        let tenant = match map.get("tenant") {
            None | Some(Value::Null) => None,
            Some(Value::String(tenant)) => Some(tenant.clone()),
            Some(_) => return Err(fail(codes::TYPE_MISMATCH, "principal tenant not a string")),
        };
        let claims = match map.get("claims") {
            None | Some(Value::Null) => None,
            Some(claims) => Some(claims.clone()),
        };
        Principal::new(subject, tenant, claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;

    #[test]
    fn principal_validates_subject() {
        assert!(Principal::new(String::from("ada"), None, None).is_ok());
        assert!(Principal::new(String::from(""), None, None).is_err());
        assert!(Principal::new(String::from(ANONYMOUS_SUBJECT), None, None).is_err());
    }

    #[test]
    fn principal_round_trip() {
        let principal = Principal::new(
            String::from("ada"),
            Some(String::from("acme")),
            Some(Value::Bool(true)),
        )
        .unwrap();
        let encoded = principal.encode().unwrap();
        assert!(Principal::SCHEMA.is(&encoded));
        assert_eq!(Principal::decode(encoded).unwrap(), principal);
    }

    #[test]
    fn principal_decode_tolerates_absent_members() {
        let mut map = alloc::collections::BTreeMap::new();
        map.insert(String::from("subject"), Value::String(String::from("ada")));
        let principal = Principal::decode(Value::Object(map)).unwrap();
        assert_eq!(principal.tenant, None);
        assert_eq!(principal.claims, None);
    }

    #[test]
    fn bearer_credentials_come_in_every_shape_callers_send() {
        let object = |pairs: &[(&str, &str)]| {
            Value::Object(
                pairs
                    .iter()
                    .map(|(name, value)| (String::from(*name), Value::from(*value)))
                    .collect(),
            )
        };
        for credentials in [
            Value::from("t"),
            Value::from("Bearer t"),
            // What the server makes of an `Authorization: Bearer` header.
            object(&[("bearer", "t")]),
            object(&[("token", "t")]),
        ] {
            assert_eq!(
                bearer_token(&credentials).unwrap(),
                Some("t"),
                "{credentials:?}"
            );
        }
        assert_eq!(bearer_token(&Value::Null).unwrap(), None);
        for credentials in [
            Value::Bool(true),
            object(&[]),
            object(&[("bearer", "t"), ("token", "u")]),
            object(&[("password", "t")]),
        ] {
            let refused = bearer_token(&credentials).unwrap_err();
            assert_eq!(refused.code, codes::UNAUTHENTICATED, "{credentials:?}");
        }
    }
}
