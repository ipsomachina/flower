//! The authorization hook, HTTP alias resolution and receipt owners.
//!
//! Every call into a module from outside it decides who it runs as before
//! anything else happens: before idempotency replay, before scoped retry
//! admission, before query evaluation and before each watch delivery. The
//! decision, an [`Authorized`], comes from [`authorize`]:
//!
//! 1. An external call (an HTTP alias) resolves through the module's alias
//!    allowlist; a name it does not list is `UNKNOWN_METHOD`, internal
//!    definition or not.
//! 2. A module that declares an authorization hook gets it invoked as the
//!    query [`AUTHORIZE`] on the call's own snapshot, with definer rights
//!    and without a principal, on `{credentials, method, args, principal}`
//!    where `principal` is the one the caller asserted (a trusted gateway,
//!    or a local embedder). Its answer `{principal}` is the principal the
//!    call runs as; its failure denies the call with [`UNAUTHORIZED`]. The
//!    answer may also say `reads_args: false` (the decision holds for any
//!    arguments) and `until` (the instant, in milliseconds, it stops
//!    holding, such as a token's expiry).
//! 3. A module without a hook runs the call as the asserted principal.
//!
//! On a committed snapshot, a decision kept in the engine's memo answers
//! while it holds, without running the hook (see [`crate::decisions`]).
//!
//! Credentials reach the hook only: never the business arguments, the
//! retry fingerprint or any receipt. Unscoped idempotency receipts belong
//! to the stable part of the principal that produced them, subject and
//! tenant ([`receipt_owner`]), so a key reused by another principal never
//! replays its result.

use std::sync::Arc;

use flower_sdk::auth::AUTHORIZE;
use flower_sdk::error::codes;
use flower_sdk::{Failure, FlowerType, Principal, Value, fail};

use crate::certificate::{Certificate, ChangeJournal, KeySequence};
use crate::decisions::{DecisionMemo, Standing, call_key};
use crate::error::{HostResult, IO_FAILURE};
use crate::host::{Env, Host};
use crate::query_cache::CodeVersion;
use crate::retry::HASH_LEN;
use crate::runtime::CallKind;
use crate::session::{CallerKind, Invocation};
use crate::storage::ReadTxn;
use crate::storage::keys::push_sized;

/// Failure code of a call the authorization hook denied. The message
/// carries the hook's own code and message.
pub const UNAUTHORIZED: &str = "unauthorized";

/// Who a call runs as, decided before replay, caches and execution.
/// Only [`authorize`] builds one.
#[derive(Clone, Debug, PartialEq)]
pub struct Authorized {
    module: String,
    definition: String,
    principal: Option<Principal>,
}

impl Authorized {
    /// A system call's decision (timer fires): no caller, no hook.
    pub(crate) fn system(module: &str, definition: &str) -> Self {
        Authorized {
            module: module.to_string(),
            definition: definition.to_string(),
            principal: None,
        }
    }

    /// The called module.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// The definition the call resolved to.
    pub fn definition(&self) -> &str {
        &self.definition
    }

    /// The principal the call runs as; `None` for anonymous.
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }
}

/// One call's authentication input.
pub(crate) struct Presented<'a> {
    /// The caller-asserted principal.
    pub principal: Option<&'a Principal>,
    /// Opaque credentials for the hook.
    pub credentials: Option<&'a Value>,
    /// Whether `name` is an external HTTP alias.
    pub external: bool,
    /// Invocation timestamp for the hook.
    pub timestamp_ms: u64,
    /// When the caller stops waiting.
    pub deadline: Option<std::time::Instant>,
}

/// What a decision on a committed snapshot may be recalled from and
/// kept in: the engine's memo, what validates it, the code the snapshot
/// came with and the marks taken before it.
pub(crate) struct Recall<'a> {
    pub memo: &'a DecisionMemo,
    pub journal: &'a ChangeJournal,
    pub keys: &'a KeySequence,
    pub code: CodeVersion,
    pub journal_mark: u64,
    pub keys_mark: u64,
}

impl Recall<'_> {
    fn standing<'s>(&'s self, db: &'s [u8], revision: u64, timestamp_ms: u64) -> Standing<'s> {
        Standing {
            journal: self.journal,
            keys: self.keys,
            db,
            code: &self.code,
            revision,
            journal_mark: self.journal_mark,
            keys_mark: self.keys_mark,
            timestamp_ms,
        }
    }
}

/// Decide who a call runs as on `read`: resolve an external alias, then
/// run the module's hook if it has one, or recall a decision that still
/// holds when `recall` is given and `read` is a committed snapshot.
#[allow(clippy::too_many_arguments)]
pub(crate) fn authorize(
    recall: Option<&Recall<'_>>,
    read: &ReadTxn,
    env: &Env<'_>,
    db: &[u8],
    module: &str,
    name: &str,
    args: &Value,
    presented: &Presented<'_>,
) -> HostResult<Authorized> {
    decide(recall, read, env, db, module, name, args, presented, false)
        .map(|(authorized, _)| authorized)
}

/// [`authorize`], also returning the certificate of what the decision
/// read: the hook's own reads, bounded by its expiry, or nothing for a
/// module without one. Alias resolution reads the module's code, which
/// callers compare separately.
#[allow(clippy::too_many_arguments)]
pub(crate) fn authorize_certified(
    recall: Option<&Recall<'_>>,
    read: &ReadTxn,
    env: &Env<'_>,
    db: &[u8],
    module: &str,
    name: &str,
    args: &Value,
    presented: &Presented<'_>,
) -> HostResult<(Authorized, Certificate)> {
    let (authorized, certificate) =
        decide(recall, read, env, db, module, name, args, presented, true)?;
    Ok((
        authorized,
        certificate.map(Arc::unwrap_or_clone).unwrap_or_default(),
    ))
}

/// One decision, certified when asked or when it may be kept.
#[allow(clippy::too_many_arguments)]
fn decide(
    recall: Option<&Recall<'_>>,
    read: &ReadTxn,
    env: &Env<'_>,
    db: &[u8],
    module: &str,
    name: &str,
    args: &Value,
    presented: &Presented<'_>,
    certify: bool,
) -> HostResult<(Authorized, Option<Arc<Certificate>>)> {
    let defs = env.descriptors.get(module);
    let hooked = defs.is_some_and(|defs| defs.authorize);
    // Only a hook's decision is worth keeping, and only one made on a
    // state the change journal follows.
    let memo = match recall {
        Some(recall) if hooked && recall.memo.enabled() && read.is_committed_snapshot() => {
            let revision = read
                .meta_u64(db, crate::storage::meta::REVISION)
                .map_err(Failure::from)?;
            call_key(
                db,
                module,
                name,
                presented.external,
                presented.credentials,
                presented.principal,
            )
            .map(|call| (recall, revision, call))
        }
        _ => None,
    };
    if let Some((recall, revision, call)) = &memo
        && let Some((authorized, certificate)) = recall.memo.recall(
            call,
            args,
            &recall.standing(db, *revision, presented.timestamp_ms),
        )
    {
        return Ok((authorized, Some(certificate)));
    }
    let definition = if presented.external {
        defs.and_then(|defs| defs.resolve_alias(name))
            .ok_or_else(|| unexposed(env, db, module, name))?
            .to_string()
    } else {
        name.to_string()
    };
    if let Some((recall, _, _)) = &memo {
        recall.memo.evaluated();
    }
    let decided = match defs {
        Some(defs) if defs.authorize => Some(run_hook(
            read,
            env,
            db,
            module,
            name,
            args,
            presented,
            certify || memo.is_some(),
        )?),
        // Unknown modules fail later, as unknown definitions do.
        _ => None,
    };
    let (principal, reads_args, certificate) = match decided {
        Some((decided, certificate)) => (
            decided.principal,
            decided.reads_args,
            certificate.map(Arc::new),
        ),
        None => (presented.principal.cloned(), true, None),
    };
    let authorized = Authorized {
        module: module.to_string(),
        definition,
        principal,
    };
    if let Some((recall, revision, call)) = memo
        && let Some(certificate) = &certificate
    {
        recall.memo.keep(
            call,
            args,
            reads_args,
            &authorized,
            certificate.clone(),
            &recall.standing(db, revision, presented.timestamp_ms),
        );
    }
    Ok((authorized, certificate))
}

/// A hook's answer.
#[derive(Debug, PartialEq)]
struct Decided {
    /// Who the call runs as; `None` for anonymous.
    principal: Option<Principal>,
    /// Whether the decision may depend on the call's arguments.
    reads_args: bool,
    /// When it stops holding, in milliseconds since the Unix epoch.
    until: Option<f64>,
}

/// Invoke the module's hook as a query with definer rights and decode its
/// decision, certified when asked. Storage failures pass through;
/// anything else that stops the hook denies.
#[allow(clippy::too_many_arguments)]
fn run_hook(
    read: &ReadTxn,
    env: &Env<'_>,
    db: &[u8],
    module: &str,
    method: &str,
    args: &Value,
    presented: &Presented<'_>,
    certify: bool,
) -> HostResult<(Decided, Option<Certificate>)> {
    let mut request = std::collections::BTreeMap::new();
    request.insert(
        "credentials".to_string(),
        presented.credentials.cloned().unwrap_or(Value::Null),
    );
    request.insert("method".to_string(), Value::String(method.to_string()));
    request.insert("args".to_string(), args.clone());
    request.insert(
        "principal".to_string(),
        match presented.principal {
            Some(principal) => principal.encode()?,
            None => Value::Null,
        },
    );
    let mut session = Invocation::new(
        module.to_string(),
        db.to_vec(),
        CallerKind::Query,
        None,
        None,
        presented.timestamp_ms,
        crate::engine::DEFAULT_FUEL,
    );
    session.budget.caller = presented.deadline;
    session.enter_definer();
    // Caches the hook computes never persist: it is not a query of record.
    session.set_ephemeral(true);
    let mut host = Host::new(read, &mut session, env);
    let outcome = host.invoke_runtime(CallKind::Query, module, AUTHORIZE, &Value::Object(request));
    let decision = match outcome {
        Ok(Ok(value)) => match session.take_poison() {
            Some(poison) => Err(poison),
            None => Ok(value),
        },
        Ok(Err(failure)) => Err(failure.failure),
        Err(error) => Err(error.failure),
    };
    let decision = decision.map_err(deny)?;
    let decided = decode_decision(decision).map_err(|failure| {
        fail(
            UNAUTHORIZED,
            format!(
                "authorization hook returned an invalid decision: {}",
                failure.message
            ),
        )
    })?;
    let certificate = if certify {
        // The decision's own expiry bounds it like a declared instant.
        if let Some(until) = decided.until {
            session.declare_change(until)?;
        }
        let mut host = Host::new(read, &mut session, env);
        Some(crate::lane::observed(&mut host).map_err(Failure::from)?)
    } else {
        None
    };
    Ok((decided, certificate))
}

/// Why an external call finds nothing to run: no such database here, no
/// such module in it, or nothing of that name its module lets callers
/// reach over HTTP.
fn unexposed(env: &Env<'_>, db: &[u8], module: &str, name: &str) -> Failure {
    let db = String::from_utf8_lossy(db);
    let message = if env.descriptors.is_empty() {
        format!("no database `{db}` here")
    } else if !env.descriptors.contains_key(module) {
        format!("database `{db}` has no module `{module}`")
    } else {
        format!("`{module}` has no `{name}` callable over HTTP (declared `http`)")
    };
    fail(codes::UNKNOWN_METHOD, message)
}

/// Map a hook failure to the call's failure: disk failures stay what they
/// are, so readiness sees them, and so does running out of time or
/// budget, never a denial telling the caller to authenticate again (the
/// caller's deadline passing is a retryable `caller_deadline_exceeded`;
/// the hook's own limits, its module's failure); anything else denies.
fn deny(failure: Failure) -> Failure {
    use crate::runtime::wasm::{CALLER_DEADLINE_EXCEEDED, DEADLINE_EXCEEDED, FUEL_EXHAUSTED};
    if [
        IO_FAILURE,
        CALLER_DEADLINE_EXCEEDED,
        DEADLINE_EXCEEDED,
        FUEL_EXHAUSTED,
    ]
    .contains(&failure.code.as_str())
    {
        return failure;
    }
    fail(
        UNAUTHORIZED,
        format!("{}: {}", failure.code, failure.message),
    )
}

/// Decode `{principal, reads_args?, until?}`: `null` is anonymous; a
/// decision that does not say otherwise read the arguments.
fn decode_decision(decision: Value) -> HostResult<Decided> {
    let Value::Object(mut map) = decision else {
        return Err(fail(codes::TYPE_MISMATCH, "decision is not an object"));
    };
    let principal = match map.remove("principal") {
        None => return Err(fail(codes::INVALID_VALUE, "decision needs `principal`")),
        Some(Value::Null) => None,
        Some(value) => Some(Principal::decode(value)?),
    };
    let reads_args = match map.remove("reads_args") {
        None => true,
        Some(Value::Bool(reads)) => reads,
        Some(_) => return Err(fail(codes::TYPE_MISMATCH, "`reads_args` is a boolean")),
    };
    let until = match map.remove("until") {
        None | Some(Value::Null) => None,
        Some(Value::Number(until)) if until.is_finite() => Some(until),
        Some(_) => return Err(fail(codes::TYPE_MISMATCH, "`until` is a finite number")),
    };
    if let Some(other) = map.keys().next() {
        return Err(fail(
            codes::INVALID_VALUE,
            format!("decision has unknown member `{other}`"),
        ));
    }
    Ok(Decided {
        principal,
        reads_args,
        until,
    })
}

/// The owner of an unscoped idempotency receipt: zeros for anonymous
/// callers (the fingerprint such receipts always carried), else SHA-256
/// over the framed subject and tenant. Claims own nothing: they change as
/// tokens refresh, and a refreshed token must still recover its result.
pub fn receipt_owner(principal: Option<&Principal>) -> HostResult<[u8; HASH_LEN]> {
    let Some(principal) = principal else {
        return Ok([0u8; HASH_LEN]);
    };
    let mut framed = Vec::new();
    push_sized(&mut framed, principal.subject.as_bytes()).map_err(Failure::from)?;
    match &principal.tenant {
        None => framed.push(0),
        Some(tenant) => {
            framed.push(1);
            push_sized(&mut framed, tenant.as_bytes()).map_err(Failure::from)?;
        }
    }
    use sha2::Digest;
    Ok(sha2::Sha256::digest(&framed).into())
}

/// The failure of a replay attempt by another owner: neither the result
/// nor whether it succeeded leaks.
pub(crate) fn foreign_receipt() -> Failure {
    fail(
        "identity_conflict",
        "idempotency key belongs to another principal",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(subject: &str, tenant: Option<&str>, claims: Option<Value>) -> Principal {
        Principal::new(subject.to_string(), tenant.map(str::to_string), claims).unwrap()
    }

    #[test]
    fn owners_ignore_claims_and_frame_tenants() {
        assert_eq!(receipt_owner(None).unwrap(), [0u8; HASH_LEN]);
        let plain = receipt_owner(Some(&principal("ada", None, None))).unwrap();
        let claimed =
            receipt_owner(Some(&principal("ada", None, Some(Value::Bool(true))))).unwrap();
        assert_eq!(plain, claimed);
        assert_ne!(plain, [0u8; HASH_LEN]);
        let tenant = receipt_owner(Some(&principal("ada", Some("acme"), None))).unwrap();
        assert_ne!(plain, tenant);
        // Framing keeps subject and tenant apart.
        let shifted = receipt_owner(Some(&principal("adaacme", None, None))).unwrap();
        assert_ne!(tenant, shifted);
    }

    #[test]
    fn decisions_decode_principals_or_anonymous() {
        let mut map = std::collections::BTreeMap::new();
        map.insert("principal".to_string(), Value::Null);
        let anonymous = decode_decision(Value::Object(map.clone())).unwrap();
        assert_eq!(anonymous.principal, None);
        // A decision that does not say otherwise read the arguments.
        assert!(anonymous.reads_args && anonymous.until.is_none());
        map.insert(
            "principal".to_string(),
            principal("ada", None, None).encode().unwrap(),
        );
        map.insert("reads_args".to_string(), Value::Bool(false));
        map.insert("until".to_string(), Value::Number(5.0));
        assert_eq!(
            decode_decision(Value::Object(map.clone())).unwrap(),
            Decided {
                principal: Some(principal("ada", None, None)),
                reads_args: false,
                until: Some(5.0),
            }
        );
        for (name, bad) in [
            ("reads_args", Value::Null),
            ("until", Value::String("soon".into())),
            ("extra", Value::Null),
        ] {
            let mut map = map.clone();
            map.insert(name.to_string(), bad);
            assert!(decode_decision(Value::Object(map)).is_err(), "{name}");
        }
        assert!(decode_decision(Value::Object(Default::default())).is_err());
        assert!(decode_decision(Value::Bool(true)).is_err());
    }
}
