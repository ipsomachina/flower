//! The authorization hook and HTTP aliases at the engine: who a call runs
//! as is decided first, before idempotency replay, scoped retry admission,
//! planning and evaluation; receipts belong to the principal that made
//! them; derived values never see their caller; system calls skip the hook.
//!
//! The test module keeps sessions in a `sessions` collection. Its hook
//! maps `{"token": …}` credentials to the session's subject, tenant and
//! role and refuses everything else, except the public `grant`/`revoke`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use flower_engine::engine::{
    CallOptions, Engine, FirePrepare, MultiStep, MutationOptions, PrepareOutcome, RetryIdentity,
};
use flower_engine::retry::ScopedIdentity;
use flower_engine::runtime::{CallKind, HostCall, Runtime};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{CallFailure, Failure, Principal, Value, fail};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn n(num: f64) -> Value {
    Value::Number(num)
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn k(name: &str) -> String {
    format!("\"{name}\"")
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn get(host: &mut dyn HostCall, collection: &str, key: &str) -> Result<Value, Failure> {
    hc(host, HostOp::Get, vec![s(""), s(collection), s(key)])
}

fn set(host: &mut dyn HostCall, collection: &str, key: &str, value: Value) -> Result<(), Failure> {
    hc(host, HostOp::Set, vec![s(""), s(collection), s(key), value]).map(|_| ())
}

type Guest =
    dyn Fn(CallKind, &str, &Value, &mut dyn HostCall) -> Result<Value, Failure> + Send + Sync;

/// A closure backend that also counts hook runs and planner runs.
struct TestRuntime {
    guest: Arc<Guest>,
    hooks: Arc<AtomicUsize>,
    plans: Arc<AtomicUsize>,
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
        if name == flower_sdk::auth::AUTHORIZE {
            self.hooks.fetch_add(1, Ordering::SeqCst);
        }
        (self.guest)(kind, name, args, host).map_err(|failure| CallFailure {
            failure,
            recovered: true,
        })
    }

    fn plan(&self, _module: &str, _name: &str, args: &Value) -> Result<Value, CallFailure> {
        self.plans.fetch_add(1, Ordering::SeqCst);
        let call = obj(vec![
            ("module", s("app")),
            ("name", s("bump")),
            ("args", args.clone()),
            ("savepoint", Value::Bool(false)),
        ]);
        Ok(Value::Array(vec![call]))
    }
}

fn guest() -> Arc<Guest> {
    Arc::new(|kind, name, args, host| match (kind, name) {
        (CallKind::Query, "$flower.authorize") => {
            let method = field(args, "method").as_str().unwrap_or_default();
            if method == "grant" || method == "revoke" {
                return Ok(obj(vec![("principal", Value::Null)]));
            }
            let Some(token) = field(field(args, "credentials"), "token").as_str() else {
                return Err(fail("UNAUTHENTICATED", "no token"));
            };
            let session = get(host, "sessions", &k(token))?;
            if session.is_null() {
                return Err(fail("UNAUTHENTICATED", "unknown session"));
            }
            let mut principal = vec![
                ("subject", field(&session, "subject").clone()),
                (
                    "claims",
                    obj(vec![("role", field(&session, "role").clone())]),
                ),
            ];
            if !field(&session, "tenant").is_null() {
                principal.push(("tenant", field(&session, "tenant").clone()));
            }
            Ok(obj(vec![("principal", obj(principal))]))
        }
        (CallKind::Query, "whoami") => hc(host, HostOp::Principal, vec![]),
        (CallKind::Query, "peek") => hc(
            host,
            HostOp::GetDerived,
            vec![s(""), s("cached"), Value::Null],
        ),
        (CallKind::Derived, "cached") => hc(host, HostOp::Principal, vec![]),
        (CallKind::Mutation, "bump") => {
            let current = get(host, "counters", &k("c"))?.as_f64().unwrap_or(0.0);
            set(host, "counters", &k("c"), n(current + 1.0))?;
            Ok(n(current + 1.0))
        }
        (CallKind::Mutation, "grant") => {
            let token = field(args, "token")
                .as_str()
                .unwrap_or_default()
                .to_string();
            let mut session = args.clone();
            if let Value::Object(map) = &mut session {
                map.remove("token");
            }
            set(host, "sessions", &k(&token), session)?;
            Ok(Value::Null)
        }
        (CallKind::Mutation, "revoke") => {
            let token = field(args, "token")
                .as_str()
                .unwrap_or_default()
                .to_string();
            hc(
                host,
                HostOp::Delete,
                vec![s(""), s("sessions"), s(&k(&token))],
            )?;
            Ok(Value::Null)
        }
        (CallKind::Mutation, "remind") => {
            hc(
                host,
                HostOp::TimerSchedule,
                vec![s("r1"), n(0.0), s("bump"), Value::Null, n(3.0)],
            )?;
            Ok(Value::Null)
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    })
}

fn manifest(authorize: bool, http: Value) -> Value {
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
                definition("peek", "query"),
                definition("cached", "derived"),
                definition("bump", "mutation"),
                definition("grant", "mutation"),
                definition("revoke", "mutation"),
                definition("remind", "mutation"),
                definition("batch", "transaction"),
            ]),
        ),
        ("triggers", Value::Array(vec![])),
        ("http", http),
        ("authorize", Value::Bool(authorize)),
    ])
}

fn alias(name: &str, definition: &str) -> Value {
    obj(vec![("alias", s(name)), ("definition", s(definition))])
}

struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
    hooks: Arc<AtomicUsize>,
    plans: Arc<AtomicUsize>,
}

impl Fixture {
    fn runtime(&self) -> Box<dyn Runtime> {
        Box::new(TestRuntime {
            guest: guest(),
            hooks: self.hooks.clone(),
            plans: self.plans.clone(),
        })
    }

    fn new(test: &str, manifest: Value) -> Fixture {
        let mut path = std::env::temp_dir();
        path.push(format!("flower-authz-{test}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut fixture = Fixture {
            engine: Engine::new(GroupStore::create(&path).unwrap()),
            path,
            hooks: Arc::new(AtomicUsize::new(0)),
            plans: Arc::new(AtomicUsize::new(0)),
        };
        for db in [&b"db"[..], &b"db2"[..]] {
            let runtime = fixture.runtime();
            fixture
                .engine
                .deploy(db, "app", [7u8; 32], b"", &manifest, runtime)
                .unwrap();
        }
        fixture
    }

    /// Forget every database's memory state and reload its module from
    /// the store, as a restart would: only durable receipts answer.
    fn reopen(&mut self) {
        for db in [&b"db"[..], &b"db2"[..]] {
            self.engine.forget_database(db);
            let runtime = self.runtime();
            self.engine.reload_module(db, "app", runtime).unwrap();
        }
    }

    fn grant(&mut self, db: &[u8], token: &str, subject: &str, role: &str) {
        let args = obj(vec![
            ("token", s(token)),
            ("subject", s(subject)),
            ("role", s(role)),
        ]);
        self.engine
            .invoke_mutation(db, "app", "grant", &args, &MutationOptions::default())
            .unwrap();
    }

    fn revoke(&mut self, db: &[u8], token: &str) {
        let args = obj(vec![("token", s(token))]);
        self.engine
            .invoke_mutation(db, "app", "revoke", &args, &MutationOptions::default())
            .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn token(token: &str) -> CallOptions {
    CallOptions {
        credentials: Some(obj(vec![("token", s(token))])),
        ..CallOptions::default()
    }
}

fn keyed(token_name: &str, key: &str) -> MutationOptions {
    MutationOptions {
        call: token(token_name),
        idempotency_key: Some(key.as_bytes().to_vec()),
        ..MutationOptions::default()
    }
}

fn subject_of(value: &Value) -> Option<&str> {
    field(value, "subject").as_str()
}

#[test]
fn the_hook_decides_who_a_call_runs_as() {
    let mut fixture = Fixture::new("decides", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "reader");
    let ada = fixture
        .engine
        .invoke_query(b"db", "app", "whoami", &Value::Null, &token("t1"))
        .unwrap();
    assert_eq!(subject_of(&ada), Some("ada"));
    // An asserted principal is the hook's input, not its answer.
    let asserted = CallOptions {
        principal: Some(Principal::new("mallory".to_string(), None, None).unwrap()),
        ..token("t1")
    };
    let decided = fixture
        .engine
        .invoke_query(b"db", "app", "whoami", &Value::Null, &asserted)
        .unwrap();
    assert_eq!(subject_of(&decided), Some("ada"));
    // No credentials, or unknown ones: denied, with the hook's reason.
    for options in [CallOptions::default(), token("nope")] {
        let denied = fixture
            .engine
            .invoke_query(b"db", "app", "whoami", &Value::Null, &options)
            .unwrap_err();
        assert_eq!(denied.code, "unauthorized");
        assert!(denied.message.contains("UNAUTHENTICATED"), "{denied:?}");
    }
    // The reader side decides the same way.
    let reader = fixture
        .engine
        .reader()
        .invoke_query(b"db", "app", "whoami", &Value::Null, &token("t1"))
        .unwrap();
    assert_eq!(subject_of(&reader), Some("ada"));
}

#[test]
fn a_deadline_passing_in_the_hook_is_no_denial() {
    // The caller stopped waiting while the hook read its session: that
    // is the caller's deadline, which a client may retry, not a refusal
    // telling it to authenticate again.
    let mut fixture = Fixture::new("hook-deadline", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "reader");
    let late = CallOptions {
        deadline: std::time::Instant::now().checked_sub(std::time::Duration::from_millis(1)),
        ..token("t1")
    };
    let failure = fixture
        .engine
        .invoke_query(b"db", "app", "whoami", &Value::Null, &late)
        .unwrap_err();
    assert_eq!(failure.code, "caller_deadline_exceeded", "{failure:?}");
    let failure = fixture
        .engine
        .invoke_mutation(
            b"db",
            "app",
            "bump",
            &Value::Null,
            &MutationOptions {
                call: late.clone(),
                ..MutationOptions::default()
            },
        )
        .unwrap_err();
    assert_eq!(failure.code, "caller_deadline_exceeded", "{failure:?}");
}

#[test]
fn modules_without_a_hook_run_as_the_asserted_principal() {
    let fixture = Fixture::new("asserted", manifest(false, Value::Array(vec![])));
    let options = CallOptions {
        principal: Some(Principal::new("ada".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    let value = fixture
        .engine
        .invoke_query(b"db", "app", "whoami", &Value::Null, &options)
        .unwrap();
    assert_eq!(subject_of(&value), Some("ada"));
    assert_eq!(fixture.hooks.load(Ordering::SeqCst), 0);
}

#[test]
fn authorization_precedes_idempotent_replay() {
    let mut fixture = Fixture::new("replay", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "writer");
    let first = fixture
        .engine
        .invoke_mutation(b"db", "app", "bump", &Value::Null, &keyed("t1", "k1"))
        .unwrap();
    assert_eq!(first, n(1.0));
    // The retry replays from memory while the session holds.
    let replayed = fixture
        .engine
        .invoke_mutation(b"db", "app", "bump", &Value::Null, &keyed("t1", "k1"))
        .unwrap();
    assert_eq!(replayed, n(1.0));
    fixture.revoke(b"db", "t1");
    // Revoked: the memory path refuses before answering...
    let refused = fixture
        .engine
        .invoke_mutation(b"db", "app", "bump", &Value::Null, &keyed("t1", "k1"))
        .unwrap_err();
    assert_eq!(refused.code, "unauthorized");
    // ...and so does the durable receipt after a restart.
    fixture.reopen();
    let refused = fixture
        .engine
        .invoke_mutation(b"db", "app", "bump", &Value::Null, &keyed("t1", "k1"))
        .unwrap_err();
    assert_eq!(refused.code, "unauthorized");
}

#[test]
fn authorization_precedes_scoped_retry_replay() {
    let mut fixture = Fixture::new("scoped", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "writer");
    let retry = MutationOptions {
        call: token("t1"),
        retry: Some(RetryIdentity {
            identity: ScopedIdentity {
                incarnation: 0,
                epoch: 0,
                intent: b"bump-1".to_vec(),
            },
            fingerprint: [1u8; 32],
        }),
        ..MutationOptions::default()
    };
    assert_eq!(
        fixture
            .engine
            .invoke_mutation(b"db", "app", "bump", &Value::Null, &retry)
            .unwrap(),
        n(1.0)
    );
    fixture.revoke(b"db", "t1");
    let refused = fixture
        .engine
        .invoke_mutation(b"db", "app", "bump", &Value::Null, &retry)
        .unwrap_err();
    assert_eq!(refused.code, "unauthorized");
}

#[test]
fn idempotency_keys_belong_to_their_principal() {
    let mut fixture = Fixture::new("owners", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "writer");
    fixture.grant(b"db", "t2", "bob", "writer");
    // Ada's token refreshed with other claims is still Ada.
    fixture.grant(b"db", "t3", "ada", "admin");
    assert_eq!(
        fixture
            .engine
            .invoke_mutation(b"db", "app", "bump", &Value::Null, &keyed("t1", "k1"))
            .unwrap(),
        n(1.0)
    );
    for restarted in [false, true] {
        if restarted {
            fixture.reopen();
        }
        // Bob reusing Ada's key learns nothing and runs nothing.
        let conflict = fixture
            .engine
            .invoke_mutation(b"db", "app", "bump", &Value::Null, &keyed("t2", "k1"))
            .unwrap_err();
        assert_eq!(conflict.code, "identity_conflict", "restarted={restarted}");
        // Ada with refreshed claims recovers her result.
        assert_eq!(
            fixture
                .engine
                .invoke_mutation(b"db", "app", "bump", &Value::Null, &keyed("t3", "k1"))
                .unwrap(),
            n(1.0),
            "restarted={restarted}"
        );
    }
    // Nothing ran twice: one more bump lands on 2.
    assert_eq!(
        fixture
            .engine
            .invoke_mutation(b"db", "app", "bump", &Value::Null, &token_options("t1"))
            .unwrap(),
        n(2.0)
    );
}

fn token_options(token_name: &str) -> MutationOptions {
    MutationOptions {
        call: token(token_name),
        ..MutationOptions::default()
    }
}

#[test]
fn racing_principals_never_receive_each_others_results() {
    let mut fixture = Fixture::new("race", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "writer");
    fixture.grant(b"db", "t2", "bob", "writer");
    fixture.engine.set_replicated(b"db");
    // Both prepare before either applies: both miss replay.
    let prepare = |engine: &mut Engine, token_name: &str| match engine
        .prepare_mutation(b"db", "app", "bump", &Value::Null, &keyed(token_name, "k1"))
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        other => panic!("unexpected admission answer {other:?}"),
    };
    let ada = prepare(&mut fixture.engine, "t1");
    let bob = prepare(&mut fixture.engine, "t2");
    let first = fixture.engine.apply_patch(&ada, 100).unwrap();
    assert_eq!(first.result, Ok(n(1.0)));
    let second = fixture.engine.apply_patch(&bob, 101).unwrap();
    let failure = second.result.unwrap_err();
    assert_eq!(failure.code, "identity_conflict");
    // A refusal, not a replay of the winner's outcome.
    assert!(!second.duplicate);
}

#[test]
fn external_calls_resolve_through_the_alias_allowlist() {
    let fixture = Fixture::new(
        "aliases",
        manifest(
            false,
            Value::Array(vec![alias("me", "whoami"), alias("bump", "bump")]),
        ),
    );
    let external = CallOptions {
        principal: Some(Principal::new("ada".to_string(), None, None).unwrap()),
        external: true,
        ..CallOptions::default()
    };
    let me = fixture
        .engine
        .invoke_query(b"db", "app", "me", &Value::Null, &external)
        .unwrap();
    assert_eq!(subject_of(&me), Some("ada"));
    // The internal name is not an alias, even though it exists.
    let internal = fixture
        .engine
        .invoke_query(b"db", "app", "whoami", &Value::Null, &external)
        .unwrap_err();
    assert_eq!(internal.code, "UNKNOWN_METHOD");
    // Local embedders still call internal names.
    let local = CallOptions {
        external: false,
        ..external.clone()
    };
    assert!(
        fixture
            .engine
            .invoke_query(b"db", "app", "whoami", &Value::Null, &local)
            .is_ok()
    );
}

#[test]
fn calls_to_unknown_databases_and_modules_say_so() {
    let fixture = Fixture::new("unknown-db", manifest(false, Value::Array(vec![])));
    let external = CallOptions {
        external: true,
        ..CallOptions::default()
    };
    let missing = fixture
        .engine
        .invoke_query(b"nowhere", "app", "whoami", &Value::Null, &external)
        .unwrap_err();
    assert!(
        missing.message.contains("database `nowhere`"),
        "{missing:?}"
    );
    let missing = fixture
        .engine
        .invoke_query(b"db", "other", "whoami", &Value::Null, &external)
        .unwrap_err();
    assert!(missing.message.contains("module `other`"), "{missing:?}");
    // A definition the module keeps off HTTP: said in the SDK's words.
    let hidden = fixture
        .engine
        .invoke_query(b"db", "app", "whoami", &Value::Null, &external)
        .unwrap_err();
    assert!(hidden.message.contains("over HTTP"), "{hidden:?}");
    assert!(!hidden.message.contains("alias"), "{hidden:?}");
}

#[test]
fn deploys_reject_aliases_to_anything_but_callables() {
    let mut fixture = Fixture::new("bad-aliases", manifest(false, Value::Array(vec![])));
    for http in [
        Value::Array(vec![alias("x", "cached")]),
        Value::Array(vec![alias("x", "missing")]),
        Value::Array(vec![alias("x", "bump"), alias("x", "whoami")]),
        Value::Array(vec![alias("", "bump")]),
    ] {
        let runtime = fixture.runtime();
        let error = fixture
            .engine
            .deploy(
                b"db3",
                "app",
                [8u8; 32],
                b"",
                &manifest(false, http.clone()),
                runtime,
            )
            .unwrap_err();
        assert!(error.message.contains("alias"), "{http:?}: {error:?}");
    }
}

#[test]
fn derived_values_never_see_their_caller() {
    let mut fixture = Fixture::new("derived", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "reader");
    fixture.grant(b"db", "t2", "bob", "reader");
    let first = fixture
        .engine
        .invoke_query(b"db", "app", "peek", &Value::Null, &token("t1"))
        .unwrap();
    // The outcome caches for everyone: it can't be Ada's.
    assert_eq!(first, Value::Null);
    let second = fixture
        .engine
        .invoke_query(b"db", "app", "peek", &Value::Null, &token("t2"))
        .unwrap();
    assert_eq!(second, Value::Null);
}

#[test]
fn transactions_authorize_before_planning() {
    let mut fixture = Fixture::new("plans", manifest(true, Value::Array(vec![])));
    let denied = fixture
        .engine
        .invoke_transaction(
            b"db",
            "app",
            "batch",
            &Value::Null,
            &MutationOptions::default(),
        )
        .unwrap_err();
    assert_eq!(denied.code, "unauthorized");
    assert_eq!(fixture.plans.load(Ordering::SeqCst), 0);
    fixture.grant(b"db", "t1", "ada", "writer");
    fixture
        .engine
        .invoke_transaction(b"db", "app", "batch", &Value::Null, &token_options("t1"))
        .unwrap();
    assert_eq!(fixture.plans.load(Ordering::SeqCst), 1);
}

#[test]
fn every_transaction_step_authorizes_before_replay() {
    let mut fixture = Fixture::new("multi", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "writer");
    fixture.grant(b"db2", "t1", "ada", "writer");
    let steps = vec![
        MultiStep {
            db: b"db".to_vec(),
            module: "app".to_string(),
            method: "bump".to_string(),
            args: Value::Null,
        },
        MultiStep {
            db: b"db2".to_vec(),
            module: "app".to_string(),
            method: "bump".to_string(),
            args: Value::Null,
        },
    ];
    let done = fixture
        .engine
        .invoke_transaction_multi(&steps, &keyed("t1", "km"))
        .unwrap();
    assert_eq!(done, Value::Array(vec![n(1.0), n(1.0)]));
    assert_eq!(
        fixture
            .engine
            .invoke_transaction_multi(&steps, &keyed("t1", "km"))
            .unwrap(),
        done
    );
    // The second database no longer accepts the session: no replay.
    fixture.revoke(b"db2", "t1");
    let refused = fixture
        .engine
        .invoke_transaction_multi(&steps, &keyed("t1", "km"))
        .unwrap_err();
    assert_eq!(refused.code, "unauthorized");
}

#[test]
fn timer_fires_skip_the_hook() {
    let mut fixture = Fixture::new("timers", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "writer");
    fixture
        .engine
        .invoke_mutation(b"db", "app", "remind", &Value::Null, &token_options("t1"))
        .unwrap();
    fixture.engine.set_replicated(b"db");
    let hooks = fixture.hooks.load(Ordering::SeqCst);
    match fixture.engine.prepare_timer_fire(b"db", "r1").unwrap() {
        FirePrepare::Patch(prepared) => {
            let outcome = fixture.engine.apply_patch(&prepared.patch, 50).unwrap();
            assert_eq!(outcome.result, Ok(n(1.0)));
        }
        FirePrepare::Retry(_) => panic!("the handler failed"),
        FirePrepare::Hold(_) => panic!("the module was not removed"),
        FirePrepare::Skip => panic!("the timer was not due"),
    }
    assert_eq!(fixture.hooks.load(Ordering::SeqCst), hooks);
}

#[test]
fn previews_authorize_on_committed_state() {
    let mut fixture = Fixture::new("preview", manifest(true, Value::Array(vec![])));
    fixture.grant(b"db", "t1", "ada", "reader");
    let overlay = flower_engine::session::Staged::default();
    let denied = fixture
        .engine
        .invoke_query_preview(
            b"db",
            "app",
            "whoami",
            &Value::Null,
            &overlay,
            &CallOptions::default(),
        )
        .unwrap_err();
    assert_eq!(denied.code, "unauthorized");
    let ada = fixture
        .engine
        .invoke_query_preview(b"db", "app", "whoami", &Value::Null, &overlay, &token("t1"))
        .unwrap();
    assert_eq!(subject_of(&ada), Some("ada"));
}
