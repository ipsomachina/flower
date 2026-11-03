//! Guides › Authentication and authorization.

use alloc::string::String;

use flower_sdk::access::{FieldAccess, Operand, Policy, Rule};
use flower_sdk::auth::{
    Access, AuthRequest, Authentication, Authorization, BearerKey, JwtBearer, Principal,
};
use flower_sdk::crypto::jwt::{Algorithm, KeyFormat};
use flower_sdk::typed::Field;
use flower_sdk::{
    App, CollectionRef, FlowerType, MethodCtx, MutationCtx, QueryCtx, ReadCtx, Result, Value,
    collection, fail, mutation, query,
};

// ANCHOR: jwt-bearer
/// An ES256 public key from your identity provider, as PEM.
const IDP_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----\n";

static BEARER: JwtBearer = JwtBearer {
    issuer: Some("https://auth.example.com/"),
    audience: &["notes-api"],
    clock_tolerance_seconds: 30.0,
    ..JwtBearer::new(BearerKey::Pem(IDP_PUBLIC_KEY, &[Algorithm::Es256]))
};

/// List it among the app's components, at most one per module.
pub static AUTHORIZATION: Authorization = Authorization {
    authenticate: Authentication::Typed(&BEARER),
    // Methods that declare no `access` need a signed-in caller...
    default: Access::Authenticated,
    // ...as do retry sessions.
    sessions: Access::Authenticated,
    // Access for what other macros declare (`external!` operations,
    // interface providers), by name.
    methods: &[],
};

/// A method declares its own access where it differs from the default.
#[query(http, access = public)]
pub fn health(ctx: &mut QueryCtx<'_, '_>) -> String {
    let _ = ctx;
    String::from("ok")
}
// ANCHOR_END: jwt-bearer

/// A shared-secret variant, for tokens your own backend signs.
pub static HS256_BEARER: JwtBearer = JwtBearer {
    audience: &["notes-api"],
    ..JwtBearer::new(BearerKey::Bytes(
        b"0123456789abcdef0123456789abcdef",
        KeyFormat::Raw,
        &[Algorithm::Hs256],
    ))
};

// ANCHOR: whoami
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Me {
    pub subject: Option<String>,
    pub tenant: Option<String>,
}

#[query(http, access = authenticated)]
pub fn whoami(ctx: &mut QueryCtx<'_, '_>) -> Result<Me> {
    let principal = ctx.principal()?;
    Ok(Me {
        subject: principal.as_ref().map(|p| p.subject.clone()),
        tenant: principal.and_then(|p| p.tenant),
    })
}
// ANCHOR_END: whoami

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Session {
    pub user: String,
    pub tenant: String,
    pub expires_at: f64,
}

#[collection(name = "sessions", key = String, value = Session)]
pub struct Sessions;

// ANCHOR: callback
/// Opaque session tokens: the session row decides who the caller is,
/// until the session expires or the row changes.
fn by_session(
    ctx: &mut QueryCtx<'_, '_>,
    credentials: &Value,
    _request: &AuthRequest,
) -> Result<Option<Principal>> {
    let token = match credentials {
        Value::Null => return Ok(None),
        Value::Object(fields) => match fields.get("bearer") {
            Some(Value::String(token)) => token.clone(),
            _ => return Err(fail("UNAUTHENTICATED", "expected a bearer token")),
        },
        _ => return Err(fail("UNAUTHENTICATED", "expected a bearer token")),
    };
    let Some(session) = Sessions::get(ctx, &token)? else {
        return Err(fail("UNAUTHENTICATED", "unknown session"));
    };
    // `clock`, not `now`: the decision can then be kept until expiry.
    if ctx.clock()? >= session.expires_at {
        return Err(fail("UNAUTHENTICATED", "session expired"));
    }
    ctx.changes_at(Some(session.expires_at))?;
    Principal::new(session.user, Some(session.tenant), None).map(Some)
}

/// Only callers of the tenant the arguments name.
fn same_tenant(
    _ctx: &mut QueryCtx<'_, '_>,
    principal: Option<&Principal>,
    args: &Value,
) -> Result<bool> {
    let named = match args {
        Value::Object(fields) => fields.get("tenant"),
        _ => None,
    };
    Ok(match (principal.and_then(|p| p.tenant.as_deref()), named) {
        (Some(mine), Some(Value::String(named))) => mine == named,
        _ => false,
    })
}

/// Every method checks the tenant unless it declares otherwise, as
/// `whoami` does.
pub static SESSION_AUTHORIZATION: Authorization = Authorization {
    authenticate: Authentication::Callback(by_session),
    default: Access::Check(same_tenant),
    sessions: Access::Authenticated,
    methods: &[],
};
// ANCHOR_END: callback

// ANCHOR: policy
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Note {
    pub owner: String,
    pub title: String,
    /// Hidden from everyone but the owner: absent when redacted.
    pub private: Field<String>,
}

/// The collection with its access policy.
#[collection(
    name = "notes",
    key = String,
    value = Note,
    index(name = "by_owner", fields(owner), types(String)),
    access = notes_policy,
)]
pub struct Notes;

fn notes_policy() -> Policy {
    let mine = Operand::row(&["owner"]).eq(Operand::subject());
    let admin = Operand::claim(&["role"]).eq(Operand::literal("admin"));
    Policy::new()
        .read(Rule::authenticated())
        .insert(Operand::next(&["owner"]).eq(Operand::subject()))
        .update(
            mine.clone()
                .and(Operand::next(&["owner"]).eq(Operand::row(&["owner"]))),
        )
        .delete(mine.clone().or(admin))
        .field("private", FieldAccess::new().read(mine))
}
// ANCHOR_END: policy

// ANCHOR: redaction
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct NoteView {
    pub title: String,
    pub private: Option<String>,
}

#[query(http)]
pub fn note(ctx: &mut QueryCtx<'_, '_>, id: String) -> Result<Option<NoteView>> {
    // Rows the caller may not read come back as `None`; fields it may
    // not read come back as `Field::Redacted`.
    Ok(Notes::get(ctx, &id)?.map(|note| NoteView {
        title: note.title,
        private: match note.private {
            Field::Visible(text) => Some(text),
            Field::Redacted => None,
        },
    }))
}

#[mutation(http)]
pub fn retitle(ctx: &mut MutationCtx<'_, '_>, id: String, title: String) -> Result<()> {
    let Some(note) = Notes::get(ctx, &id)? else {
        return Err(fail("NOT_FOUND", "no such note"));
    };
    // A redacted field cannot be encoded, so write the fields you may
    // see; the engine keeps the stored values of the ones you may not.
    let mut row = alloc::collections::BTreeMap::new();
    row.insert(String::from("owner"), Value::String(note.owner));
    row.insert(String::from("title"), Value::String(title));
    if let Field::Visible(private) = note.private {
        row.insert(String::from("private"), Value::String(private));
    }
    ctx.set(
        CollectionRef::local(Notes::NAME),
        &Value::String(id),
        Value::Object(row),
    )
}

#[mutation(http)]
pub fn forget_private(ctx: &mut MutationCtx<'_, '_>, id: String) -> Result<()> {
    let Some(note) = Notes::get(ctx, &id)? else {
        return Ok(());
    };
    let mut row = alloc::collections::BTreeMap::new();
    row.insert(String::from("owner"), Value::String(note.owner));
    row.insert(String::from("title"), Value::String(note.title));
    // Remove the field outright, even if this caller cannot see it:
    // that takes the field's write rule.
    ctx.set_clearing(
        CollectionRef::local(Notes::NAME),
        &Value::String(id),
        Value::Object(row),
        &["private"],
    )
}
// ANCHOR_END: redaction

// ANCHOR: app
pub static APP: App = App::new(&[
    &AUTHORIZATION,
    &Notes,
    &HEALTH,
    &WHOAMI,
    &NOTE,
    &RETITLE,
    &FORGET_PRIVATE,
]);
// ANCHOR_END: app
