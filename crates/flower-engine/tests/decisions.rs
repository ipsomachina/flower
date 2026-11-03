//! Authorization decisions kept in the engine's memo: repeated calls with
//! the same inputs skip the hook, and no kept decision ever outlives
//! what it depended on — a revoked session row, a redeploy, its expiry,
//! a key-catalog change, different credentials, principal, fuel, alias or
//! arguments — nor answers a writer's staged state or a speculation.
//!
//! The hook maps `{"token": …}` credentials to the session row's subject;
//! `mode` makes it report that it read no arguments (`any-args`), poll
//! the clock (`poll`), resolve a managed key (`keys`) or read something
//! no dependency names (`volatile`); `until` becomes its expiry.

mod fixtures;

use fixtures::{hc, obj, s};

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use flower_engine::batch::BatchItem;
use flower_engine::engine::{CallOptions, Engine, MutationOptions, PrepareOutcome};
use flower_engine::runtime::{CallKind, HostCall, Runtime};
use flower_engine::speculation::{PrepareMutation, Verdict};
use flower_engine::storage::{GroupStore, Overlay};
use flower_sdk::abi::HostOp;
use flower_sdk::{CallFailure, Failure, Principal, Value, fail};

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

fn session_key(token: &str) -> String {
    format!("\"{token}\"")
}

/// The hook: who a token's session row names, as `mode` says.
fn hook(args: &Value, host: &mut dyn HostCall, subject_prefix: &str) -> Result<Value, Failure> {
    let method = field(args, "method").as_str().unwrap_or_default();
    if method == "grant" || method == "revoke" {
        return Ok(obj(vec![("principal", Value::Null)]));
    }
    let credentials = field(args, "credentials");
    let Some(token) = field(credentials, "token").as_str() else {
        return Err(fail("UNAUTHENTICATED", "no token"));
    };
    let session = hc(
        host,
        HostOp::Get,
        vec![s(""), s("sessions"), s(&session_key(token))],
    )?;
    if session.is_null() {
        return Err(fail("UNAUTHENTICATED", "unknown session"));
    }
    let mode = field(credentials, "mode").as_str().unwrap_or_default();
    match mode {
        "poll" => {
            hc(host, HostOp::Now, vec![])?;
        }
        "keys" => host.read_keys(),
        "volatile" => host.read_untracked(),
        _ => {}
    }
    let subject = format!(
        "{subject_prefix}{}",
        field(&session, "subject").as_str().unwrap_or_default()
    );
    let mut decision = vec![("principal", obj(vec![("subject", s(&subject))]))];
    if mode == "any-args" {
        decision.push(("reads_args", Value::Bool(false)));
    }
    if !field(credentials, "until").is_null() {
        decision.push(("until", field(credentials, "until").clone()));
    }
    Ok(obj(decision))
}

/// A closure backend counting hook runs.
struct TestRuntime {
    hooks: Arc<AtomicUsize>,
    /// Prefix the hook puts before subjects: a redeploy changes it.
    prefix: &'static str,
}

impl Runtime for TestRuntime {
    fn call(
        &self,
        kind: CallKind,
        _module: &str,
        name: &str,
        args: &Value,
        host: &mut dyn HostCall,
    ) -> Result<Value, CallFailure> {
        let outcome = match (kind, name) {
            (CallKind::Query, "$flower.authorize") => {
                self.hooks.fetch_add(1, Ordering::SeqCst);
                hook(args, host, self.prefix)
            }
            (CallKind::Query, "whoami") => hc(host, HostOp::Principal, vec![]),
            (CallKind::Mutation, "bump") => hc(
                host,
                HostOp::Set,
                vec![s(""), s("counters"), s("\"c\""), args.clone()],
            )
            .map(|_| Value::Null),
            (CallKind::Mutation, "grant") => {
                let token = field(args, "token")
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                hc(
                    host,
                    HostOp::Set,
                    vec![
                        s(""),
                        s("sessions"),
                        s(&session_key(&token)),
                        obj(vec![("subject", field(args, "subject").clone())]),
                    ],
                )
                .map(|_| Value::Null)
            }
            (CallKind::Mutation, "revoke") => {
                let token = field(args, "token")
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                hc(
                    host,
                    HostOp::Delete,
                    vec![s(""), s("sessions"), s(&session_key(&token))],
                )
                .map(|_| Value::Null)
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        };
        outcome.map_err(|failure| CallFailure {
            failure,
            recovered: true,
        })
    }

    fn plan(&self, _module: &str, _name: &str, _args: &Value) -> Result<Value, CallFailure> {
        Err(CallFailure {
            failure: fail("UNKNOWN_METHOD", "no planner"),
            recovered: false,
        })
    }
}

fn manifest() -> Value {
    let json = obj(vec![("type", s("json"))]);
    let collection = |name: &str| {
        obj(vec![
            ("name", s(name)),
            ("key", obj(vec![("type", s("string"))])),
            ("value", json.clone()),
            ("indexes", Value::Array(vec![])),
            ("references", Value::Array(vec![])),
        ])
    };
    let definition = |name: &str, kind: &str| {
        obj(vec![
            ("name", s(name)),
            ("kind", s(kind)),
            ("args", json.clone()),
        ])
    };
    obj(vec![
        (
            "collections",
            Value::Array(vec![collection("sessions"), collection("counters")]),
        ),
        (
            "definitions",
            Value::Array(vec![
                definition("whoami", "query"),
                definition("bump", "mutation"),
                definition("grant", "mutation"),
                definition("revoke", "mutation"),
            ]),
        ),
        ("triggers", Value::Array(vec![])),
        (
            "http",
            Value::Array(vec![obj(vec![
                ("alias", s("who")),
                ("definition", s("whoami")),
            ])]),
        ),
        ("authorize", Value::Bool(true)),
    ])
}

struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
    hooks: Arc<AtomicUsize>,
    position: u64,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn fixture(test: &str) -> Fixture {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-decisions-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let hooks = Arc::new(AtomicUsize::new(0));
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            b"",
            &manifest(),
            Box::new(TestRuntime {
                hooks: hooks.clone(),
                prefix: "",
            }),
        )
        .unwrap();
    Fixture {
        engine,
        path,
        hooks,
        position: 0,
    }
}

fn token(token: &str) -> CallOptions {
    credentials(obj(vec![("token", s(token))]))
}

fn credentials(credentials: Value) -> CallOptions {
    CallOptions {
        credentials: Some(credentials),
        ..CallOptions::default()
    }
}

impl Fixture {
    /// Prepare a write the way a leader does and apply its patch the way
    /// members do, so the change journal follows it.
    fn replicate(&mut self, name: &str, args: Value, call: &CallOptions) -> Result<(), Failure> {
        let options = MutationOptions {
            call: call.clone(),
            ..MutationOptions::default()
        };
        match self
            .engine
            .prepare_mutation(b"db", "app", name, &args, &options)?
        {
            PrepareOutcome::Patch(prepared) => {
                self.position += 1;
                self.engine
                    .apply_patch(&prepared.patch, self.position)?
                    .result
                    .map(|_| ())
            }
            PrepareOutcome::Done(result) | PrepareOutcome::Replayed(result, _) => {
                result.map(|_| ())
            }
        }
    }

    fn grant(&mut self, token: &str, subject: &str) {
        let args = obj(vec![("token", s(token)), ("subject", s(subject))]);
        self.replicate("grant", args, &CallOptions::default())
            .unwrap();
    }

    fn revoke(&mut self, token: &str) {
        let args = obj(vec![("token", s(token))]);
        self.replicate("revoke", args, &CallOptions::default())
            .unwrap();
    }

    /// Who a reader query runs as.
    fn whoami(&self, options: &CallOptions) -> Result<String, Failure> {
        self.whoami_with(&Value::Null, options)
    }

    fn whoami_with(&self, args: &Value, options: &CallOptions) -> Result<String, Failure> {
        let name = if options.external { "who" } else { "whoami" };
        self.engine
            .reader()
            .invoke_query(b"db", "app", name, args, options)
            .map(|value| {
                field(&value, "subject")
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            })
    }

    fn hooks(&self) -> usize {
        self.hooks.load(Ordering::SeqCst)
    }
}

#[test]
fn repeated_calls_reuse_the_decision_instead_of_running_the_hook() {
    let mut fixture = fixture("reuse");
    fixture.grant("t1", "ada");
    let before = fixture.hooks();
    let start = fixture.engine.reader().decision_memo_stats();
    for _ in 0..10 {
        assert_eq!(fixture.whoami(&token("t1")).unwrap(), "ada");
    }
    assert_eq!(fixture.hooks(), before + 1, "one evaluation for ten calls");
    let stats = fixture.engine.reader().decision_memo_stats();
    assert_eq!(
        (
            stats.hits - start.hits,
            stats.evaluations - start.evaluations,
            stats.entries - start.entries
        ),
        (9, 1, 1)
    );
    // So do mutations prepared on committed state; each method has its
    // own decision. A write elsewhere leaves the decision standing.
    for _ in 0..3 {
        fixture
            .replicate("bump", Value::Null, &token("t1"))
            .unwrap();
    }
    assert_eq!(fixture.hooks(), before + 2);
    // Off, every call runs the hook.
    fixture.engine.reader().set_decision_memo_bytes(0);
    for _ in 0..3 {
        assert_eq!(fixture.whoami(&token("t1")).unwrap(), "ada");
    }
    assert_eq!(fixture.hooks(), before + 5);
    assert_eq!(fixture.engine.reader().decision_memo_stats().entries, 0);
}

#[test]
fn a_revoked_session_is_never_answered_from_the_memo() {
    let mut fixture = fixture("revoked");
    fixture.grant("t1", "ada");
    fixture.grant("t2", "bob");
    assert_eq!(fixture.whoami(&token("t1")).unwrap(), "ada");
    assert_eq!(fixture.whoami(&token("t2")).unwrap(), "bob");
    // Another session's change leaves Ada's decision standing.
    fixture.revoke("t2");
    let before = fixture.hooks();
    assert_eq!(fixture.whoami(&token("t1")).unwrap(), "ada");
    assert_eq!(fixture.hooks(), before, "untouched decisions stay");
    // Her own revocation retires it: readers, writers and watches
    // decide again, and deny.
    fixture.revoke("t1");
    let denied = fixture.whoami(&token("t1")).unwrap_err();
    assert_eq!(denied.code, "unauthorized");
    let write = fixture
        .replicate("bump", Value::Null, &token("t1"))
        .unwrap_err();
    assert_eq!(write.code, "unauthorized");
    let view = fixture.engine.reader().view().unwrap();
    assert!(
        view.authorize_certified(b"db", "app", "whoami", &Value::Null, &token("t1"))
            .is_err()
    );
    // A new session under the same token is a new decision.
    fixture.grant("t1", "eve");
    assert_eq!(fixture.whoami(&token("t1")).unwrap(), "eve");
}

#[test]
fn a_redeploy_retires_every_decision() {
    let mut fixture = fixture("redeploy");
    fixture.grant("t1", "ada");
    assert_eq!(fixture.whoami(&token("t1")).unwrap(), "ada");
    // New code decides differently: no decision of the old code answers.
    fixture
        .engine
        .deploy(
            b"db",
            "app",
            [8u8; 32],
            b"",
            &manifest(),
            Box::new(TestRuntime {
                hooks: fixture.hooks.clone(),
                prefix: "v2:",
            }),
        )
        .unwrap();
    assert_eq!(fixture.whoami(&token("t1")).unwrap(), "v2:ada");
}

#[test]
fn decisions_hold_until_their_expiry_and_never_when_they_poll_the_clock() {
    let mut fixture = fixture("expiry");
    fixture.grant("t1", "ada");
    let at = |options: CallOptions, timestamp: u64| CallOptions {
        timestamp_ms: Some(timestamp),
        ..options
    };
    let expiring = credentials(obj(vec![
        ("token", s("t1")),
        ("until", Value::Number(1_000_000.0)),
    ]));
    let before = fixture.hooks();
    fixture.whoami(&at(expiring.clone(), 999_000)).unwrap();
    fixture.whoami(&at(expiring.clone(), 999_999)).unwrap();
    assert_eq!(fixture.hooks(), before + 1, "valid until its expiry");
    fixture.whoami(&at(expiring.clone(), 1_000_000)).unwrap();
    assert_eq!(fixture.hooks(), before + 2, "decided again at its expiry");
    // A decision is never recalled for an instant before it was made.
    fixture.whoami(&at(expiring, 998_000)).unwrap();
    assert_eq!(fixture.hooks(), before + 3);
    let polling = credentials(obj(vec![("token", s("t1")), ("mode", s("poll"))]));
    for _ in 0..3 {
        fixture.whoami(&polling).unwrap();
    }
    assert_eq!(fixture.hooks(), before + 6, "a clock poll is never kept");
    let volatile = credentials(obj(vec![("token", s("t1")), ("mode", s("volatile"))]));
    for _ in 0..2 {
        fixture.whoami(&volatile).unwrap();
    }
    assert_eq!(
        fixture.hooks(),
        before + 8,
        "untracked reads are never kept"
    );
}

#[test]
fn a_key_change_retires_decisions_that_resolved_a_key() {
    let mut fixture = fixture("keys");
    fixture.grant("t1", "ada");
    let keyed = credentials(obj(vec![("token", s("t1")), ("mode", s("keys"))]));
    let before = fixture.hooks();
    fixture.whoami(&keyed).unwrap();
    fixture.whoami(&keyed).unwrap();
    assert_eq!(fixture.hooks(), before + 1);
    // Decisions that resolved no key survive a key change.
    fixture.whoami(&token("t1")).unwrap();
    let change = fixture.engine.begin_key_change();
    fixture.whoami(&keyed).unwrap();
    assert_eq!(
        fixture.hooks(),
        before + 3,
        "not while a change is in flight"
    );
    drop(change);
    fixture.whoami(&keyed).unwrap();
    fixture.whoami(&keyed).unwrap();
    assert_eq!(fixture.hooks(), before + 4, "decided once after it");
    fixture.whoami(&token("t1")).unwrap();
    assert_eq!(fixture.hooks(), before + 4);
}

#[test]
fn decisions_that_read_the_arguments_hold_for_those_arguments_only() {
    let mut fixture = fixture("arguments");
    fixture.grant("t1", "ada");
    let a = obj(vec![("id", s("a"))]);
    let b = obj(vec![("id", s("b"))]);
    let before = fixture.hooks();
    // The default: a decision read the arguments.
    fixture.whoami_with(&a, &token("t1")).unwrap();
    fixture.whoami_with(&a, &token("t1")).unwrap();
    assert_eq!(fixture.hooks(), before + 1);
    fixture.whoami_with(&b, &token("t1")).unwrap();
    assert_eq!(fixture.hooks(), before + 2);
    // One that says it read none holds for any.
    let any = credentials(obj(vec![("token", s("t1")), ("mode", s("any-args"))]));
    fixture.whoami_with(&a, &any).unwrap();
    fixture.whoami_with(&b, &any).unwrap();
    fixture.whoami_with(&Value::Null, &any).unwrap();
    assert_eq!(fixture.hooks(), before + 3);
}

#[test]
fn every_input_of_the_call_is_part_of_its_key() {
    let mut fixture = fixture("inputs");
    fixture.grant("t1", "ada");
    fixture.grant("t2", "bob");
    let any = |token: &str| credentials(obj(vec![("token", s(token)), ("mode", s("any-args"))]));
    assert_eq!(fixture.whoami(&any("t1")).unwrap(), "ada");
    let before = fixture.hooks();
    // Other credentials.
    assert_eq!(fixture.whoami(&any("t2")).unwrap(), "bob");
    assert!(fixture.whoami(&any("t3")).is_err());
    // Another asserted principal or an alias instead of the name.
    let asserted = CallOptions {
        principal: Some(Principal::new("mallory".to_string(), None, None).unwrap()),
        ..any("t1")
    };
    assert_eq!(fixture.whoami(&asserted).unwrap(), "ada");
    let external = CallOptions {
        external: true,
        ..any("t1")
    };
    assert_eq!(fixture.whoami(&external).unwrap(), "ada");
    assert_eq!(fixture.hooks(), before + 4);
    // Each now holds its own decision.
    for options in [any("t1"), any("t2"), asserted, external] {
        fixture.whoami(&options).unwrap();
    }
    assert_eq!(fixture.hooks(), before + 4);
    // Denials are never kept.
    assert!(fixture.whoami(&any("t3")).is_err());
    assert_eq!(fixture.hooks(), before + 5);
}

/// A speculative preparation runs the hook even when a decision is kept:
/// what the decision read lands in the speculation's certificate, so a
/// revocation staged before the candidate in its batch conflicts with it
/// and the candidate is prepared again, and denied.
#[test]
fn speculations_and_staged_preparations_run_the_hook() {
    let mut fixture = fixture("speculation");
    fixture.grant("t1", "ada");
    // The memo holds a decision for Ada's write.
    for _ in 0..2 {
        fixture
            .replicate("bump", Value::Null, &token("t1"))
            .unwrap();
    }
    let kept = fixture.engine.reader().decision_memo_stats().hits;
    assert!(kept >= 1, "{kept}");
    let staged = fixture.engine.store().begin_scratch().unwrap();
    staged.log_writes();
    let mut overlay = Overlay::default();
    let mark = staged.extend_overlay(&mut overlay, 0).unwrap();
    let speculation = fixture.engine.speculation(Arc::new(overlay), None);
    let mut speculator = speculation.speculator();
    let options = MutationOptions {
        call: token("t1"),
        ..MutationOptions::default()
    };
    let before = fixture.hooks();
    let candidate = speculator
        .prepare_mutation(b"db", "app", "bump", &Value::Null, &options)
        .unwrap();
    assert!(matches!(candidate, PrepareOutcome::Patch(_)));
    assert_eq!(fixture.hooks(), before + 1, "the speculation ran the hook");
    let certificate = speculator.certificate();
    drop(speculation);
    // Earlier in the batch: Ada's session is revoked.
    let revoke = fixture
        .engine
        .on_staged(&staged, |engine| {
            PrepareMutation::prepare_mutation(
                engine,
                b"db",
                "app",
                "revoke",
                &obj(vec![("token", s("t1"))]),
                &MutationOptions::default(),
            )
        })
        .unwrap();
    let PrepareOutcome::Patch(revoke) = revoke else {
        panic!("revoke proposed nothing");
    };
    let item = BatchItem::Patch {
        db: b"db".to_vec(),
        patch: revoke.patch,
    };
    fixture
        .engine
        .commit_batch(&staged, &[item], fixture.position + 1, 0)
        .unwrap();
    let verdict = staged.logged(mark, |writes| certificate.verdict(writes));
    assert!(matches!(verdict, Verdict::Conflicts(_)), "{verdict:?}");
    // Prepared again in its turn, on the staged state, it is denied.
    let again = fixture.engine.on_staged(&staged, |engine| {
        PrepareMutation::prepare_mutation(engine, b"db", "app", "bump", &Value::Null, &options)
    });
    assert_eq!(again.unwrap_err().code, "unauthorized");
}
