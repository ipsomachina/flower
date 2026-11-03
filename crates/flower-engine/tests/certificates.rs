//! Query results reused across revisions: certificates name what a
//! query observed, replicated patches record what they changed, and a
//! cached result answers only while no change since touches it.

extern crate alloc;

mod fixtures;

use fixtures::{arr, hc, k, n, obj, s};

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use flower_crypto::catalog::apply_change;
use flower_crypto::host::CryptoHost;
use flower_crypto::replicated::{ManagedKeys, Prepared, WrappingKey};
use flower_engine::batch::BatchItem;
use flower_engine::engine::{CallOptions, Engine, MutationOptions, PrepareOutcome};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, NativeRuntime};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Principal, Value, fail};

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", obj(vec![("type", s("json"))])),
    ])
}

fn manifest() -> Value {
    obj(vec![
        (
            "collections",
            arr(vec![obj(vec![
                ("name", s("cells")),
                ("key", obj(vec![("type", s("string"))])),
                (
                    "value",
                    obj(vec![
                        ("type", s("object")),
                        (
                            "fields",
                            arr(vec![obj(vec![
                                ("name", s("v")),
                                ("schema", obj(vec![("type", s("number"))])),
                                ("optional", Value::Bool(false)),
                            ])]),
                        ),
                    ]),
                ),
                ("indexes", arr(vec![])),
                ("references", arr(vec![])),
            ])]),
        ),
        (
            "definitions",
            arr(vec![
                definition("put", "mutation"),
                definition("get", "query"),
                definition("count", "query"),
                definition("via", "query"),
                definition("gate", "query"),
                definition("now", "query"),
                definition("history", "query"),
                definition("timer", "query"),
                definition("timers", "query"),
                definition("cell", "derived"),
                definition("parity", "derived"),
                definition("top", "derived"),
                definition("report", "query"),
                definition("choose", "derived"),
                definition("chosen", "query"),
                definition("stamp", "derived"),
                definition("opens", "derived"),
                definition("read_stamp", "query"),
                definition("read_opens", "query"),
                definition("pin_stamp", "mutation"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
        // Other modules read `cell` as their import.
        (
            "exports",
            obj(vec![
                ("collections", arr(vec![])),
                ("derived", arr(vec![s("cell")])),
                ("methods", arr(vec![])),
            ]),
        ),
    ])
}

fn field(args: &Value, name: &str) -> Value {
    match args {
        Value::Object(map) => map.get(name).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn key_of(args: &Value) -> Result<String, Failure> {
    match field(args, "key") {
        Value::String(key) => Ok(k(&key)),
        _ => Err(fail("TYPE_MISMATCH", "needs a key")),
    }
}

/// One cell's `v`, or null.
fn cell(host: &mut dyn HostCall, key: &str) -> Result<Value, Failure> {
    match hc(host, HostOp::Get, vec![s(""), s("cells"), s(key)])? {
        Value::Object(map) => Ok(map.get("v").cloned().unwrap_or(Value::Null)),
        _ => Ok(Value::Null),
    }
}

/// Evaluations of the derived `top`.
static TOPS: AtomicUsize = AtomicUsize::new(0);

/// Read derived `name` of this module with `args`.
fn derived(host: &mut dyn HostCall, name: &str, args: &Value) -> Result<Value, Failure> {
    hc(
        host,
        HostOp::GetDerived,
        vec![s("app"), s(name), args.clone()],
    )
}

/// A runtime counting the queries it evaluates.
fn runtime(evaluated: Arc<AtomicUsize>) -> Box<dyn flower_engine::runtime::Runtime> {
    Box::new(ClosureRuntime::new(
        move |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            if kind == CallKind::Query {
                evaluated.fetch_add(1, Ordering::SeqCst);
            }
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let key = key_of(args)?;
                    hc(
                        host,
                        HostOp::Set,
                        vec![
                            s(""),
                            s("cells"),
                            s(&key),
                            obj(vec![("v", field(args, "v"))]),
                        ],
                    )?;
                    Ok(s("ok"))
                }
                (CallKind::Query, "get") => cell(host, &key_of(args)?),
                (CallKind::Derived, "cell") => cell(host, &key_of(args)?),
                (CallKind::Query, "count") => {
                    let page = hc(
                        host,
                        HostOp::Scan,
                        vec![s(""), s("cells"), n(100.0), Value::Null],
                    )?;
                    match field(&page, "rows") {
                        Value::Array(rows) => Ok(n(rows.len() as f64)),
                        other => Err(fail("TYPE_MISMATCH", format!("page rows {other:?}"))),
                    }
                }
                // `report` reads `top`, which reads `parity`, which reads a
                // row: a chain for unchanged outputs.
                (CallKind::Derived, "parity") => match cell(host, &key_of(args)?)? {
                    Value::Number(v) => Ok(n(v % 2.0)),
                    _ => Ok(Value::Null),
                },
                (CallKind::Derived, "top") => {
                    TOPS.fetch_add(1, Ordering::SeqCst);
                    let parity = derived(host, "parity", args)?;
                    Ok(obj(vec![("parity", parity)]))
                }
                (CallKind::Query, "report") => derived(host, "top", args),
                // The parity of row `x` or of row `y`, as row `branch` says.
                (CallKind::Derived, "choose") => {
                    let row = if cell(host, &k("branch"))? == n(1.0) {
                        "x"
                    } else {
                        "y"
                    };
                    match cell(host, &k(row))? {
                        Value::Number(v) => Ok(n(v % 2.0)),
                        _ => Ok(Value::Null),
                    }
                }
                (CallKind::Query, "chosen") => derived(host, "choose", &Value::Null),
                (CallKind::Query, "via") => hc(
                    host,
                    HostOp::GetDerived,
                    vec![s("app"), s("cell"), obj(vec![("key", field(args, "key"))])],
                ),
                (CallKind::Query, "gate") => {
                    // Opens at `at`, declaring that instant until then.
                    let Value::Number(at) = field(args, "at") else {
                        return Err(fail("TYPE_MISMATCH", "gate needs at"));
                    };
                    let Value::Number(clock) = hc(host, HostOp::Clock, vec![])? else {
                        return Err(fail("TYPE_MISMATCH", "clock"));
                    };
                    if clock < at {
                        hc(host, HostOp::ChangesAt, vec![n(at)])?;
                    }
                    Ok(Value::Bool(clock >= at))
                }
                (CallKind::Query, "now") => hc(host, HostOp::Now, vec![]),
                // A derived value polling the clock, and one declaring
                // when it opens, as `gate` does.
                (CallKind::Derived, "stamp") => hc(host, HostOp::Now, vec![]),
                (CallKind::Derived, "opens") => {
                    let Value::Number(at) = field(args, "at") else {
                        return Err(fail("TYPE_MISMATCH", "opens needs at"));
                    };
                    let Value::Number(clock) = hc(host, HostOp::Clock, vec![])? else {
                        return Err(fail("TYPE_MISMATCH", "clock"));
                    };
                    if clock < at {
                        hc(host, HostOp::ChangesAt, vec![n(at)])?;
                    }
                    Ok(Value::Bool(clock >= at))
                }
                (CallKind::Query, "read_stamp") => hc(
                    host,
                    HostOp::GetDerived,
                    vec![s(""), s("stamp"), Value::Null],
                ),
                (CallKind::Query, "read_opens") => hc(
                    host,
                    HostOp::GetDerived,
                    vec![s(""), s("opens"), args.clone()],
                ),
                (CallKind::Mutation, "pin_stamp") => {
                    hc(
                        host,
                        HostOp::Materialize,
                        vec![s(""), s("stamp"), Value::Null],
                    )?;
                    Ok(Value::Null)
                }
                (CallKind::Query, "history") => hc(
                    host,
                    HostOp::History,
                    vec![s(""), s("cells"), s(&key_of(args)?), n(10.0)],
                ),
                (CallKind::Query, "timer") => hc(host, HostOp::TimerGet, vec![s("t1")]),
                (CallKind::Query, "timers") => {
                    hc(host, HostOp::TimerScan, vec![Value::Null, n(10.0)])
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    ))
}

struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
    evaluated: Arc<AtomicUsize>,
    position: u64,
}

impl Fixture {
    fn new(test: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!("flower-certificates-{test}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut engine = Engine::new(GroupStore::create(&path).unwrap());
        let evaluated = Arc::new(AtomicUsize::new(0));
        engine
            .deploy(
                b"db",
                "app",
                [1u8; 32],
                b"v1",
                &manifest(),
                runtime(evaluated.clone()),
            )
            .unwrap();
        Fixture {
            engine,
            path,
            evaluated,
            position: 0,
        }
    }

    /// Replicate a write the way members apply one: prepare, then apply
    /// the patch at the next log position.
    fn put(&mut self, key: &str, v: f64) {
        let patch = self.prepare(key, v);
        self.apply(&patch);
    }

    /// Prepare a write's patch without applying it.
    fn prepare(&mut self, key: &str, v: f64) -> Vec<u8> {
        let args = obj(vec![("key", s(key)), ("v", n(v))]);
        match self
            .engine
            .prepare_mutation(b"db", "app", "put", &args, &MutationOptions::default())
            .unwrap()
        {
            PrepareOutcome::Patch(prepared) => prepared.patch,
            _ => panic!("prepare short-circuited"),
        }
    }

    /// Apply a prepared patch at the next log position.
    fn apply(&mut self, patch: &[u8]) {
        self.position += 1;
        self.engine.apply_patch(patch, self.position).unwrap();
    }

    fn query_with(&self, name: &str, args: Value, options: &CallOptions) -> Value {
        self.engine
            .reader()
            .invoke_query(b"db", "app", name, &args, options)
            .unwrap()
    }

    fn query(&self, name: &str, args: Value) -> Value {
        self.query_with(name, args, &CallOptions::default())
    }

    fn evaluated(&self) -> usize {
        self.evaluated.load(Ordering::SeqCst)
    }

    /// Evaluations and cache hits since `before`.
    fn since(&self, before: (usize, u64)) -> (usize, u64) {
        (
            self.evaluated() - before.0,
            self.engine.reader().query_cache_stats().hits - before.1,
        )
    }

    fn mark(&self) -> (usize, u64) {
        (
            self.evaluated(),
            self.engine.reader().query_cache_stats().hits,
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn key(name: &str) -> Value {
    obj(vec![("key", s(name))])
}

#[test]
fn results_outlive_commits_to_rows_they_never_read() {
    let mut fixture = Fixture::new("unrelated");
    fixture.put("a", 1.0);
    assert_eq!(fixture.query("get", key("a")), n(1.0));
    let mark = fixture.mark();
    assert_eq!(fixture.query("get", key("a")), n(1.0));
    fixture.put("b", 2.0);
    fixture.put("c", 3.0);
    assert_eq!(fixture.query("get", key("a")), n(1.0));
    // Two answers, no evaluation: the commits to b and c touched nothing
    // the query read.
    assert_eq!(fixture.since(mark), (0, 2));
    fixture.put("a", 4.0);
    assert_eq!(fixture.query("get", key("a")), n(4.0));
    assert_eq!(fixture.since(mark), (1, 2));
    // A key read as absent counts too: writing it changes the answer.
    assert_eq!(fixture.query("get", key("z")), Value::Null);
    fixture.put("z", 5.0);
    assert_eq!(fixture.query("get", key("z")), n(5.0));
}

#[test]
fn scans_see_rows_inserted_into_their_range() {
    let mut fixture = Fixture::new("phantoms");
    assert_eq!(fixture.query("count", Value::Null), n(0.0));
    fixture.put("a", 1.0);
    assert_eq!(fixture.query("count", Value::Null), n(1.0));
    fixture.put("b", 1.0);
    assert_eq!(fixture.query("count", Value::Null), n(2.0));
    let mark = fixture.mark();
    assert_eq!(fixture.query("count", Value::Null), n(2.0));
    assert_eq!(fixture.since(mark), (0, 1));
}

#[test]
fn derived_reads_expire_with_what_they_read() {
    let mut fixture = Fixture::new("derived");
    fixture.put("a", 1.0);
    assert_eq!(fixture.query("via", key("a")), n(1.0));
    // Another key for the same query reads the derived outcome the first
    // committed: its certificate expands through the committed edges.
    let other = CallOptions {
        principal: Some(flower_sdk::Principal::new("other".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    assert_eq!(fixture.query_with("via", key("a"), &other), n(1.0));
    fixture.put("b", 2.0);
    let mark = fixture.mark();
    assert_eq!(fixture.query("via", key("a")), n(1.0));
    assert_eq!(fixture.query_with("via", key("a"), &other), n(1.0));
    assert_eq!(fixture.since(mark), (0, 2));
    fixture.put("a", 3.0);
    assert_eq!(fixture.query("via", key("a")), n(3.0));
    assert_eq!(fixture.query_with("via", key("a"), &other), n(3.0));
}

#[test]
fn declared_instants_bound_reuse_and_polls_never_enter() {
    let fixture = Fixture::new("clock");
    let at = |ms: u64| CallOptions {
        timestamp_ms: Some(ms),
        ..CallOptions::default()
    };
    let gate = obj(vec![("at", n(1000.0))]);
    assert_eq!(
        fixture.query_with("gate", gate.clone(), &at(500)),
        Value::Bool(false)
    );
    let mark = fixture.mark();
    // Before the declared instant the closed gate answers from the
    // cache; at it, the query runs again and opens.
    assert_eq!(
        fixture.query_with("gate", gate.clone(), &at(999)),
        Value::Bool(false)
    );
    assert_eq!(fixture.since(mark), (0, 1));
    assert_eq!(
        fixture.query_with("gate", gate.clone(), &at(1000)),
        Value::Bool(true)
    );
    assert_eq!(fixture.since(mark), (1, 1));
    // Open, it declares nothing: reading `clock` alone does not poll.
    assert_eq!(
        fixture.query_with("gate", gate, &at(5000)),
        Value::Bool(true)
    );
    assert_eq!(fixture.since(mark), (1, 2));
    // `now` polls: every call evaluates.
    let mark = fixture.mark();
    fixture.query_with("now", Value::Null, &at(1));
    fixture.query_with("now", Value::Null, &at(1));
    assert_eq!(fixture.since(mark), (2, 0));
}

#[test]
fn derived_values_that_read_the_clock_answer_at_each_readers_clock() {
    let fixture = Fixture::new("derived-clock");
    let at = |ms: u64| CallOptions {
        timestamp_ms: Some(ms),
        ..CallOptions::default()
    };
    // A value that polled the clock serves its own instant alone: a later
    // reader evaluates it again.
    assert_eq!(
        fixture.query_with("read_stamp", Value::Null, &at(100)),
        n(100.0)
    );
    assert_eq!(
        fixture.query_with("read_stamp", Value::Null, &at(200)),
        n(200.0)
    );
    // One that declared an instant holds until it: its reader answers
    // from the query cache meanwhile, and evaluates again at it.
    let gate = obj(vec![("at", n(1000.0))]);
    assert_eq!(
        fixture.query_with("read_opens", gate.clone(), &at(500)),
        Value::Bool(false)
    );
    let mark = fixture.mark();
    assert_eq!(
        fixture.query_with("read_opens", gate.clone(), &at(999)),
        Value::Bool(false)
    );
    assert_eq!(fixture.since(mark), (0, 1));
    assert_eq!(
        fixture.query_with("read_opens", gate, &at(1000)),
        Value::Bool(true)
    );
}

#[test]
fn pinned_values_that_read_the_clock_evaluate_again_and_stay_pinned() {
    let mut fixture = Fixture::new("pinned-clock");
    // A mutation at 100 pins `stamp`; its patch carries the outcome
    // to the applier.
    let options = MutationOptions {
        call: CallOptions {
            timestamp_ms: Some(100),
            ..CallOptions::default()
        },
        ..MutationOptions::default()
    };
    let patch = match fixture
        .engine
        .prepare_mutation(b"db", "app", "pin_stamp", &Value::Null, &options)
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("prepare short-circuited"),
    };
    fixture.apply(&patch);
    let stored = |fixture: &Fixture| {
        let read = fixture.engine.store().begin_read().unwrap();
        let qualified = flower_engine::host::qualify_definition("app", "stamp");
        let id = read
            .intern_id(b"db", qualified.as_bytes())
            .unwrap()
            .unwrap();
        read.get_derived(b"db", id, b"null").unwrap().unwrap()
    };
    assert!(stored(&fixture).pinned);
    let at = |ms: u64| CallOptions {
        timestamp_ms: Some(ms),
        ..CallOptions::default()
    };
    assert_eq!(
        fixture.query_with("read_stamp", Value::Null, &at(100)),
        n(100.0)
    );
    // Later, the root evaluates again and keeps its pin.
    assert_eq!(
        fixture.query_with("read_stamp", Value::Null, &at(300)),
        n(300.0)
    );
    assert!(stored(&fixture).pinned);
}

#[test]
fn untracked_reads_never_enter() {
    // Retained history and timer rows name no dependency a commit can
    // touch: their results never answer from the cache.
    let fixture = Fixture::new("untracked");
    for (name, args) in [
        ("history", key("a")),
        ("timer", Value::Null),
        ("timers", Value::Null),
    ] {
        let mark = fixture.mark();
        fixture.query(name, args.clone());
        fixture.query(name, args);
        assert_eq!(fixture.since(mark), (2, 0), "{name}");
    }
}

#[test]
fn code_principals_and_budgets_separate_results() {
    let mut fixture = Fixture::new("keys");
    fixture.put("a", 1.0);
    fixture.query("get", key("a"));
    let mark = fixture.mark();
    // Another principal is another result.
    let ada = CallOptions {
        principal: Some(Principal::new("ada".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    fixture.query_with("get", key("a"), &ada);
    fixture.query_with("get", key("a"), &ada);
    assert_eq!(fixture.since(mark), (1, 1));
    // New code is a new result, though no revision moved.
    let evaluated = fixture.evaluated.clone();
    fixture
        .engine
        .deploy(
            b"db",
            "app",
            [2u8; 32],
            b"v2",
            &manifest(),
            runtime(evaluated),
        )
        .unwrap();
    let mark = fixture.mark();
    fixture.query("get", key("a"));
    assert_eq!(fixture.since(mark), (1, 0));
    // A zero budget turns the cache off.
    fixture.engine.reader().set_query_cache_bytes(0);
    let mark = fixture.mark();
    fixture.query("get", key("a"));
    fixture.query("get", key("a"));
    assert_eq!(fixture.since(mark), (2, 0));
}

#[test]
fn scratch_staging_reaches_no_journal() {
    // A leader's writer stages the writes queued before the one it
    // prepares in a scratch transaction it drops. Only the commit that
    // follows may reach the journal: a staged revision would break the
    // chain that commit continues, and no result would outlive a write.
    let mut fixture = Fixture::new("scratch");
    fixture.put("a", 1.0);
    fixture.query("get", key("a"));
    let patch = fixture.prepare("b", 2.0);
    let scratch = fixture.engine.store().begin_scratch().unwrap();
    let item = BatchItem::Patch {
        db: b"db".to_vec(),
        patch: patch.clone(),
    };
    fixture
        .engine
        .commit_batch(&scratch, &[item], fixture.position + 1, 0)
        .unwrap();
    assert!(scratch.commit().is_err(), "scratch writes never commit");
    fixture.apply(&patch);
    let mark = fixture.mark();
    assert_eq!(fixture.query("get", key("a")), n(1.0));
    assert_eq!(fixture.since(mark), (0, 1));
}

/// Signing and verifying under a declared catalog key, as a guest does.
mod keys_app {
    use flower_sdk::crypto::{
        Algorithm, Crypto, KeyDeclaration, ManagedKeyRef, Usage, hex_decode, hex_encode,
    };
    use flower_sdk::{App, DerivedCtx, QueryCtx, ReadCtx, Value, derived, query};

    static SESSION_KEYS: [KeyDeclaration; 1] = [KeyDeclaration::new(
        "sessions",
        Algorithm::Ed25519,
        &[Usage::Sign, Usage::Verify],
    )];

    struct Keys;

    impl flower_sdk::app::Component for Keys {
        fn keys(&self) -> &[KeyDeclaration] {
            &SESSION_KEYS
        }
    }

    static KEYS: Keys = Keys;

    /// `message` signed under the binding's active version, in hex.
    #[query]
    pub fn sign(ctx: &mut QueryCtx<'_, '_>, message: String) -> flower_sdk::Result<String> {
        let key = SESSION_KEYS[0].key()?;
        let signature = Crypto::new(ctx.ctx()).managed_sign(
            &key,
            ManagedKeyRef::Current,
            message.as_bytes(),
        )?;
        Ok(hex_encode(&signature))
    }

    /// Whether `signature` (hex) verifies `message` under version 1.
    #[query]
    pub fn verify(
        ctx: &mut QueryCtx<'_, '_>,
        message: String,
        signature: String,
    ) -> flower_sdk::Result<bool> {
        let key = SESSION_KEYS[0].key()?;
        Crypto::new(ctx.ctx()).managed_verify(
            &key,
            ManagedKeyRef::Version(1),
            message.as_bytes(),
            &hex_decode(&signature)?,
        )
    }

    /// [`sign`], kept as a derived value.
    #[derived]
    pub fn signed(ctx: &mut DerivedCtx<'_, '_>, message: String) -> flower_sdk::Result<String> {
        let key = SESSION_KEYS[0].key()?;
        let signature = Crypto::new(ctx.ctx()).managed_sign(
            &key,
            ManagedKeyRef::Current,
            message.as_bytes(),
        )?;
        Ok(hex_encode(&signature))
    }

    /// [`verify`], kept as a derived value.
    #[derived]
    pub fn checked(
        ctx: &mut DerivedCtx<'_, '_>,
        message: String,
        signature: String,
    ) -> flower_sdk::Result<bool> {
        let key = SESSION_KEYS[0].key()?;
        Crypto::new(ctx.ctx()).managed_verify(
            &key,
            ManagedKeyRef::Version(1),
            message.as_bytes(),
            &hex_decode(&signature)?,
        )
    }

    fn text(value: &str) -> Value {
        Value::String(String::from(value))
    }

    /// Read [`signed`].
    #[query]
    pub fn read_signed(ctx: &mut QueryCtx<'_, '_>, message: String) -> flower_sdk::Result<Value> {
        let args = Value::Object([(String::from("message"), text(&message))].into());
        ctx.get_derived("app", "signed", &args)
    }

    /// Read [`checked`].
    #[query]
    pub fn read_checked(
        ctx: &mut QueryCtx<'_, '_>,
        message: String,
        signature: String,
    ) -> flower_sdk::Result<Value> {
        let args = Value::Object(
            [
                (String::from("message"), text(&message)),
                (String::from("signature"), text(&signature)),
            ]
            .into(),
        );
        ctx.get_derived("app", "checked", &args)
    }

    static DEFS: [&dyn flower_sdk::Component; 6] = [
        &SIGN,
        &VERIFY,
        &SIGNED,
        &CHECKED,
        &READ_SIGNED,
        &READ_CHECKED,
    ];

    pub static APP: App = App::new(&[&KEYS, &DEFS]);
}

/// A member's catalog runtime and the `$cluster` row it installs,
/// changed as the leader would prepare and every member apply.
struct KeyCatalog {
    keys: Arc<ManagedKeys>,
    stored: Option<Vec<u8>>,
}

impl KeyCatalog {
    fn new() -> Self {
        KeyCatalog {
            keys: Arc::new(ManagedKeys::new(
                Some(WrappingKey::new(&[6; 32])),
                Vec::new(),
            )),
            stored: None,
        }
    }

    fn change(&mut self, request: serde_json::Value) {
        let Prepared::Change(change) = self.keys.prepare(self.stored.as_deref(), &request).unwrap()
        else {
            panic!("{request} proposes a change");
        };
        if let Some(catalog) = apply_change(self.stored.as_deref(), &change)
            .unwrap()
            .catalog
        {
            self.stored = Some(catalog);
        }
        self.keys.install(b"db", self.stored.as_deref()).unwrap();
    }
}

/// The signing app deployed over a catalog with `sessions` bound to a
/// fresh Ed25519 key.
struct KeyFixture {
    engine: Engine,
    catalog: KeyCatalog,
    path: std::path::PathBuf,
}

impl KeyFixture {
    fn new(test: &str) -> Self {
        let mut catalog = KeyCatalog::new();
        catalog.change(serde_json::json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}));
        catalog.change(serde_json::json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign","verify"]}));
        let mut path = std::env::temp_dir();
        path.push(format!(
            "flower-certificates-keys-{test}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut engine = Engine::new(GroupStore::create(&path).unwrap());
        let manifest = NativeRuntime::new(&keys_app::APP).manifest().unwrap();
        let crypto = CryptoHost::new()
            .with_catalog(catalog.keys.clone())
            .for_module(b"db", "app", &manifest)
            .unwrap();
        let runtime = NativeRuntime::new(&keys_app::APP).with_crypto(Arc::new(crypto));
        engine
            .deploy(b"db", "app", [7u8; 32], b"", &manifest, Box::new(runtime))
            .unwrap();
        KeyFixture {
            engine,
            catalog,
            path,
        }
    }

    fn query(&self, name: &str, args: Value) -> Result<Value, Failure> {
        self.engine
            .reader()
            .invoke_query(b"db", "app", name, &args, &CallOptions::default())
    }

    /// Apply a catalog change as a member does: in flight from before
    /// its transaction, which drops what resolved a key, commits until
    /// the key runtime serves the new catalog.
    fn apply(&mut self, request: serde_json::Value) {
        let change = self.engine.begin_key_change();
        let write = self.engine.store().begin_write().unwrap();
        self.engine.drop_key_readers(&write, b"db").unwrap();
        write.commit().unwrap();
        self.catalog.change(request);
        drop(change);
    }
}

impl Drop for KeyFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn rotate() -> serde_json::Value {
    serde_json::json!({"operation":"rotate","requestId":"r","name":"signing"})
}

fn revoke_first() -> serde_json::Value {
    serde_json::json!({"operation":"revoke","requestId":"v","name":"signing","version":1})
}

#[test]
fn key_reads_never_outlive_a_key_change() {
    // Catalog changes move no database revision, so a result that
    // signed or verified under a key never answers from the cache.
    let mut fixture = KeyFixture::new("cache");
    let message = obj(vec![("message", s("m"))]);
    let first = fixture.query("sign", message.clone()).unwrap();
    let Value::String(signature) = &first else {
        panic!("a hex signature: {first:?}");
    };
    let check = obj(vec![("message", s("m")), ("signature", s(signature))]);
    assert_eq!(
        fixture.query("verify", check.clone()).unwrap(),
        Value::Bool(true)
    );
    // A rotation signs under the new version.
    fixture.catalog.change(rotate());
    assert_ne!(fixture.query("sign", message).unwrap(), first);
    // A revocation stops verification under the revoked version.
    fixture.catalog.change(revoke_first());
    let revoked = fixture.query("verify", check).unwrap_err();
    assert_eq!(revoked.code, "KEY_REVOKED", "{revoked:?}");
    assert_eq!(fixture.engine.reader().query_cache_stats().hits, 0);
}

#[test]
fn key_changes_drop_derived_values_that_resolved_a_key() {
    // A derived value keeps its outcome until something it read
    // changes; a key change moves no record, so it drops them itself.
    let mut fixture = KeyFixture::new("derived");
    let message = obj(vec![("message", s("m"))]);
    let first = fixture.query("read_signed", message.clone()).unwrap();
    let Value::String(signature) = &first else {
        panic!("a hex signature: {first:?}");
    };
    let check = obj(vec![("message", s("m")), ("signature", s(signature))]);
    assert_eq!(
        fixture.query("read_checked", check.clone()).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        fixture.query("read_signed", message.clone()).unwrap(),
        first
    );
    fixture.apply(rotate());
    assert_ne!(fixture.query("read_signed", message).unwrap(), first);
    // A signature under a revoked version stops verifying.
    fixture.apply(revoke_first());
    let revoked = fixture.query("read_checked", check).unwrap_err();
    assert_eq!(revoked.code, "KEY_REVOKED", "{revoked:?}");
    // Results over those values never enter the query cache: every
    // call counts as evaluated, and nothing is held or charged.
    let stats = fixture.engine.reader().query_cache_stats();
    assert_eq!(
        (stats.hits, stats.misses, stats.entries, stats.bytes),
        (0, 5, 0, 0)
    );
}

#[test]
fn derived_values_computed_across_a_key_change_never_persist() {
    // A reader whose snapshot predates a key change can still compute
    // a derived value from the old catalog after the change dropped
    // the old outcomes; its outcome must not land on top.
    let mut fixture = KeyFixture::new("race");
    let message = obj(vec![("message", s("m"))]);
    let view = fixture.engine.reader().view().unwrap();
    let change = fixture.engine.begin_key_change();
    let write = fixture.engine.store().begin_write().unwrap();
    fixture.engine.drop_key_readers(&write, b"db").unwrap();
    write.commit().unwrap();
    let options = CallOptions::default();
    let authorized = view
        .authorize(b"db", "app", "read_signed", &message, &options)
        .unwrap();
    let stale = view
        .query_as(b"db", &authorized, &message, &options)
        .unwrap();
    fixture.catalog.change(rotate());
    drop(change);
    drop(view);
    assert_ne!(fixture.query("read_signed", message).unwrap(), stale);
}

#[test]
fn imported_derived_values_certify_their_provider_reads() {
    // Another module's derived value evaluates as its provider: the
    // reader's certificate names the provider's rows it read, through
    // the value's edges, and nothing else.
    let mut fixture = Fixture::new("imports");
    let evaluated = fixture.evaluated.clone();
    fixture
        .engine
        .deploy(
            b"db",
            "reader",
            [3u8; 32],
            b"r1",
            &manifest(),
            runtime(evaluated),
        )
        .unwrap();
    fixture.put("a", 1.0);
    let via = |fixture: &Fixture| {
        fixture
            .engine
            .reader()
            .invoke_query(b"db", "reader", "via", &key("a"), &CallOptions::default())
            .unwrap()
    };
    assert_eq!(via(&fixture), n(1.0));
    fixture.put("b", 2.0);
    let mark = fixture.mark();
    assert_eq!(via(&fixture), n(1.0));
    assert_eq!(fixture.since(mark), (0, 1));
    fixture.put("a", 3.0);
    assert_eq!(via(&fixture), n(3.0));
}

#[test]
fn hits_answer_with_the_rendering_their_evaluation_kept() {
    let mut fixture = Fixture::new("encoded");
    fixture.put("a", 1.5);
    let reader = fixture.engine.reader();
    let options = CallOptions::default();
    let encoded = |name: &str, args: Value, options: &CallOptions| {
        reader
            .query_encoded_at(b"db", "app", name, &args, options)
            .unwrap()
    };
    let (fresh, revision) = encoded("get", key("a"), &options);
    assert_eq!(fresh.bytes(), b"1.5");
    let mark = fixture.mark();
    let (hit, again) = encoded("get", key("a"), &options);
    assert_eq!(again, revision);
    assert_eq!(fixture.since(mark), (0, 1));
    // The hit is the rendering the evaluation kept, not a new one: the
    // value is neither cloned nor rendered again.
    assert!(hit.shares(&fresh));
    // Callers wanting the value get the same result from the entry.
    assert_eq!(fixture.query("get", key("a")), n(1.5));
    assert!(encoded("get", key("a"), &options).0.shares(&fresh));
    // Results that never enter render for their caller alone.
    let at = CallOptions {
        timestamp_ms: Some(5),
        ..CallOptions::default()
    };
    let (first, _) = encoded("now", Value::Null, &at);
    let (second, _) = encoded("now", Value::Null, &at);
    assert_eq!((first.bytes(), second.bytes()), (&b"5"[..], &b"5"[..]));
    assert!(!first.shares(&second));
    // A write to what it read renders the new result afresh.
    fixture.put("a", 2.0);
    let reader = fixture.engine.reader();
    let (changed, moved) = reader
        .query_encoded_at(b"db", "app", "get", &key("a"), &options)
        .unwrap();
    assert_eq!((changed.bytes(), moved), (&b"2"[..], revision + 1));
    assert!(!changed.shares(&fresh));
}

#[test]
fn derived_values_that_come_out_the_same_keep_the_results_above_them() {
    let mut fixture = Fixture::new("unchanged");
    let parity = |p: f64| obj(vec![("parity", n(p))]);
    let stats = |fixture: &Fixture| fixture.engine.reader().query_cache_stats();
    fixture.put("a", 2.0);
    assert_eq!(fixture.query("report", key("a")), parity(0.0));
    let tops = TOPS.load(Ordering::SeqCst);
    let mark = fixture.mark();
    // 2 to 4: `parity` read the row and derives again, but comes out the
    // same, so neither `top` nor the query run: the cached result answers.
    fixture.put("a", 4.0);
    assert_eq!(fixture.query("report", key("a")), parity(0.0));
    assert_eq!(fixture.since(mark), (0, 1));
    assert_eq!(TOPS.load(Ordering::SeqCst), tops);
    assert_eq!(stats(&fixture).rederived, 1);
    // Kept, the result answers again without deriving anything.
    assert_eq!(fixture.query("report", key("a")), parity(0.0));
    assert_eq!(fixture.since(mark), (0, 2));
    assert_eq!(stats(&fixture).rederived, 1);
    // 4 to 5 changes `parity`: `top` and the query evaluate.
    fixture.put("a", 5.0);
    assert_eq!(fixture.query("report", key("a")), parity(1.0));
    assert_eq!(fixture.since(mark), (1, 2));
    assert_eq!(TOPS.load(Ordering::SeqCst), tops + 1);
    // The entry kept still follows the row: 5 to 7 keeps it, 7 to 8
    // does not.
    fixture.put("a", 7.0);
    assert_eq!(fixture.query("report", key("a")), parity(1.0));
    assert_eq!(fixture.since(mark), (1, 3));
    fixture.put("a", 8.0);
    assert_eq!(fixture.query("report", key("a")), parity(0.0));
    assert_eq!(fixture.since(mark), (2, 3));
    assert_eq!(stats(&fixture).rederived, 2);
}

#[test]
fn a_kept_result_follows_what_its_values_read_once_derived_again() {
    let mut fixture = Fixture::new("rederived-reads");
    fixture.put("x", 2.0);
    fixture.put("y", 4.0);
    fixture.put("branch", 0.0);
    assert_eq!(fixture.query("chosen", Value::Null), n(0.0));
    let mark = fixture.mark();
    // `choose` now reads `x`, and still comes out even: kept.
    fixture.put("branch", 1.0);
    assert_eq!(fixture.query("chosen", Value::Null), n(0.0));
    assert_eq!(fixture.since(mark), (0, 1));
    // `y` is no longer read: a write to it touches nothing.
    fixture.put("y", 5.0);
    assert_eq!(fixture.query("chosen", Value::Null), n(0.0));
    assert_eq!(fixture.since(mark), (0, 2));
    // `x` is: a write that makes it odd evaluates the query again. A
    // result kept with the graph of its first derivation would miss it.
    fixture.put("x", 3.0);
    assert_eq!(fixture.query("chosen", Value::Null), n(1.0));
    assert_eq!(fixture.since(mark), (1, 2));
}
