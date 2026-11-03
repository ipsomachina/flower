//! External value pools against the engine, with a real SDK application
//! run in-process through `NativeRuntime`: leased pools (claim, renew,
//! release, ready, stats, a lapsed lease reclaimed), sharded `next`, and
//! typed external handles.

extern crate alloc;

use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::NativeRuntime;
use flower_engine::storage::GroupStore;
use flower_sdk::{Failure, Value};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn n(number: f64) -> Value {
    Value::Number(number)
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

fn list(value: Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items,
        other => panic!("expected a list, got {other:?}"),
    }
}

/// A pool of document texts, leased for 1 s by default and 5 s at
/// most, with typed handles over it.
mod pool_app {
    use flower_sdk::external::{ClaimOptions, Lease, Next, State, TypedExternal};
    use flower_sdk::{
        App, Collection, CollectionRef, Ctx, FlowerType, Method, MutationCtx, QueryCtx, Result,
        Value, external, fail, mutation, query,
    };

    static DOCS: Collection = Collection::new("docs");

    fn text_input(ctx: &mut Ctx<'_, '_>, id: Value) -> Result<Value> {
        match ctx.get(CollectionRef::local("docs"), &id)? {
            Some(Value::String(text)) if !text.is_empty() => Ok(Value::String(text)),
            _ => Ok(Value::Null),
        }
    }

    external!(pub static TEXTS = "texts", input = text_input, tracks = "docs", lease = (1_000, 5_000));

    static TYPED: TypedExternal<String, String, u64> = TEXTS.typed();

    static PUT: Method = Method::mutation("put", |ctx, args| {
        let (Some(id), Some(text)) = (
            args.as_object().and_then(|map| map.get("id")).cloned(),
            args.as_object().and_then(|map| map.get("text")).cloned(),
        ) else {
            return Err(fail("INVALID_ARGUMENT", "put takes {id, text}"));
        };
        ctx.set(CollectionRef::local("docs"), &id, text)?;
        Ok(Value::Null)
    });

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Seen {
        pub status: String,
        pub count: Option<u64>,
    }

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Job {
        pub args: String,
        pub key: String,
        pub input: String,
        pub attempt: u64,
        pub expires_ms: u64,
    }

    #[query]
    pub fn typed_state(ctx: &mut QueryCtx<'_, '_>, id: String) -> Result<Option<Seen>> {
        Ok(TYPED.state(ctx.ctx(), &id)?.map(|state| match state {
            State::Pending => Seen {
                status: "pending".into(),
                count: None,
            },
            State::Ready(count) => Seen {
                status: "ready".into(),
                count: Some(count),
            },
        }))
    }

    #[query]
    pub fn typed_next(ctx: &mut QueryCtx<'_, '_>) -> Result<Vec<String>> {
        Ok(TYPED
            .next(ctx.ctx(), Next::default())?
            .into_iter()
            .map(|work| work.input)
            .collect())
    }

    /// Claim typed work, count its words, publish half of the claims
    /// and hand the rest back; renewals of the published ones are lost.
    #[mutation]
    pub fn typed_round(ctx: &mut MutationCtx<'_, '_>, owner: String) -> Result<Vec<Job>> {
        let claims = TYPED.claim(ctx, &owner, ClaimOptions::default())?;
        let mut jobs = Vec::new();
        for (index, claim) in claims.iter().enumerate() {
            if TYPED.pending(ctx.ctx(), &claim.lease.args)?.is_none() {
                return Err(fail("BROKEN", "claimed work has no pending work"));
            }
            if index % 2 == 0 {
                let count = claim.input.split_whitespace().count() as u64;
                if !TYPED.publish(ctx, &claim.lease.args, &claim.lease.key, &count)? {
                    return Err(fail("BROKEN", "a current key was rejected"));
                }
            } else if !TYPED.release(ctx, &claim.lease, 0)? {
                return Err(fail("BROKEN", "a held lease did not release"));
            }
            jobs.push(Job {
                args: claim.lease.args.clone(),
                key: claim.lease.key.clone(),
                input: claim.input.clone(),
                attempt: claim.lease.attempt,
                expires_ms: claim.expires_ms,
            });
        }
        let leases: Vec<Lease<String>> = claims.into_iter().map(|claim| claim.lease).collect();
        if TYPED.renew(ctx, &leases, None)?.iter().any(Option::is_some) {
            return Err(fail("BROKEN", "a published or released lease renewed"));
        }
        let stats = TYPED.stats(ctx.ctx())?;
        if stats.ready != TYPED.ready(ctx.ctx())? {
            return Err(fail("BROKEN", "stats and ready disagree"));
        }
        Ok(jobs)
    }

    pub static APP: App = App::new(&[&DOCS, &TEXTS, &PUT, &TYPED_STATE, &TYPED_NEXT, &TYPED_ROUND]);
}

struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Fixture {
    fn new(test: &str) -> Fixture {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "flower-external-pool-{test}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut fixture = Fixture {
            engine: Engine::new(GroupStore::create(&path).unwrap()),
            path,
        };
        let app = &pool_app::APP;
        let manifest = NativeRuntime::new(app).manifest().unwrap();
        let identity = flower_crypto::hash::content_identity(
            b"app",
            manifest.canonical_json().unwrap().as_bytes(),
        );
        fixture
            .engine
            .deploy(
                b"db",
                "app",
                identity,
                b"",
                &manifest,
                Box::new(NativeRuntime::new(app)),
            )
            .unwrap();
        fixture
    }

    fn at(now: u64) -> CallOptions {
        CallOptions {
            timestamp_ms: Some(now),
            ..CallOptions::default()
        }
    }

    fn mutate(&mut self, name: &str, args: Value, now: u64) -> Result<Value, Failure> {
        let options = MutationOptions {
            call: Fixture::at(now),
            ..MutationOptions::default()
        };
        self.engine
            .invoke_mutation(b"db", "app", name, &args, &options)
    }

    fn query(&self, name: &str, args: Value, now: u64) -> Result<Value, Failure> {
        self.engine
            .invoke_query(b"db", "app", name, &args, &Fixture::at(now))
    }

    fn put(&mut self, id: &str, text: &str, now: u64) {
        self.mutate("put", obj(vec![("id", s(id)), ("text", s(text))]), now)
            .unwrap();
    }

    fn claim(&mut self, owner: &str, limit: u32, now: u64) -> Vec<Value> {
        let args = obj(vec![("owner", s(owner)), ("limit", n(f64::from(limit)))]);
        list(self.mutate("texts.claim", args, now).unwrap())
    }

    fn renew(&mut self, leases: &[&Value], now: u64) -> Value {
        let leases = leases.iter().map(|claim| lease(claim)).collect();
        let args = obj(vec![("leases", Value::Array(leases))]);
        self.mutate("texts.renew", args, now).unwrap()
    }

    fn release(&mut self, claim: &Value, delay_ms: u64, now: u64) -> Value {
        let mut args = lease(claim);
        if let Value::Object(map) = &mut args {
            map.insert("delayMs".to_string(), n(delay_ms as f64));
        }
        self.mutate("texts.release", args, now).unwrap()
    }
}

/// A claim's lease: `{args, key, owner, attempt}`.
fn lease(claim: &Value) -> Value {
    obj(vec![
        ("args", field(claim, "args").clone()),
        ("key", field(claim, "key").clone()),
        ("owner", field(claim, "owner").clone()),
        ("attempt", field(claim, "attempt").clone()),
    ])
}

fn args_of(claims: &[Value]) -> Vec<String> {
    claims
        .iter()
        .map(|claim| field(claim, "args").as_str().unwrap().to_string())
        .collect()
}

#[test]
fn a_leased_pool_claims_renews_releases_and_reclaims_lapsed_leases() {
    let mut fixture = Fixture::new("leases");
    fixture.put("a", "one", 1_000);
    fixture.put("b", "two words", 1_001);
    fixture.put("c", "three more words", 1_002);
    assert_eq!(
        fixture.query("texts.ready", Value::Null, 1_500).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        fixture.query("texts.stats", Value::Null, 1_500).unwrap(),
        obj(vec![
            ("ready", Value::Bool(true)),
            ("oldestReadyAt", n(1_000.0)),
            ("nextAvailableAt", Value::Null),
        ])
    );

    // Oldest markers first; a leased key is nobody else's.
    let w1 = fixture.claim("w1", 2, 2_000);
    assert_eq!(args_of(&w1), ["a", "b"]);
    assert_eq!(field(&w1[0], "attempt"), &n(1.0));
    assert_eq!(field(&w1[0], "expiresAt"), &n(3_000.0));
    assert_eq!(field(&w1[0], "input"), &s("one"));
    let w2 = fixture.claim("w2", 10, 2_000);
    assert_eq!(args_of(&w2), ["c"]);
    assert_eq!(
        fixture.query("texts.ready", Value::Null, 2_001).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        field(
            &fixture.query("texts.stats", Value::Null, 2_001).unwrap(),
            "nextAvailableAt"
        ),
        &n(3_000.0)
    );
    // Leased markers still show in `next`, for unleased workers.
    assert_eq!(
        list(fixture.query("texts.next", Value::Null, 2_001).unwrap()).len(),
        3
    );

    // Renewal extends only leases still held, by their own owner.
    let mut stranger = w1[0].clone();
    if let Value::Object(map) = &mut stranger {
        map.insert("owner".to_string(), s("w9"));
    }
    assert_eq!(
        fixture.renew(&[&w1[0], &stranger], 2_500),
        Value::Array(vec![n(3_500.0), Value::Null])
    );

    // A release hands the key back after its delay; attempts carry on.
    assert_eq!(fixture.release(&w1[1], 200, 2_600), Value::Bool(true));
    assert_eq!(fixture.release(&w1[1], 0, 2_601), Value::Bool(false));
    assert!(fixture.claim("w2", 10, 2_700).is_empty());
    let again = fixture.claim("w2", 10, 2_800);
    assert_eq!(args_of(&again), ["b"]);
    assert_eq!(field(&again[0], "attempt"), &n(2.0));

    // An input change drops the lease with the work it was for: the
    // key is claimable at once, its attempts counting afresh.
    fixture.put("c", "changed", 2_900);
    assert_eq!(
        fixture.renew(&[&w2[0]], 2_950),
        Value::Array(vec![Value::Null])
    );
    let fresh = fixture.claim("w4", 10, 2_950);
    assert_eq!(args_of(&fresh), ["c"]);
    assert_eq!(field(&fresh[0], "attempt"), &n(1.0));
    assert_eq!(field(&fresh[0], "input"), &s("changed"));

    // `a`'s lease lapses unrenewed at 3,500: another owner reclaims it,
    // and the old holder can neither renew nor release it any more.
    assert!(fixture.claim("w3", 10, 3_499).is_empty());
    let reclaimed = fixture.claim("w3", 10, 3_500);
    assert_eq!(args_of(&reclaimed), ["a"]);
    assert_eq!(field(&reclaimed[0], "attempt"), &n(2.0));
    assert_eq!(
        fixture.renew(&[&w1[0]], 3_600),
        Value::Array(vec![Value::Null])
    );
    assert_eq!(fixture.release(&w1[0], 0, 3_600), Value::Bool(false));

    // Publishing takes the marker, lease and all.
    let published = fixture
        .mutate(
            "texts.publish",
            obj(vec![
                ("args", s("a")),
                ("key", field(&reclaimed[0], "key").clone()),
                ("value", n(1.0)),
            ]),
            3_700,
        )
        .unwrap();
    assert_eq!(published, Value::Bool(true));
    assert_eq!(
        fixture.renew(&[&reclaimed[0]], 3_701),
        Value::Array(vec![Value::Null])
    );
    assert!(
        fixture
            .claim("w5", 10, 100_000)
            .iter()
            .all(|claim| field(claim, "args") != &s("a"))
    );
}

#[test]
fn leases_are_bounded_and_owned() {
    let mut fixture = Fixture::new("bounds");
    fixture.put("a", "one", 1_000);
    let too_long = fixture
        .mutate(
            "texts.claim",
            obj(vec![("owner", s("w")), ("leaseMs", n(5_001.0))]),
            2_000,
        )
        .unwrap_err();
    assert_eq!(too_long.code, "LEASE_TOO_LONG");
    let nameless = fixture
        .mutate("texts.claim", obj(vec![("owner", s(""))]), 2_000)
        .unwrap_err();
    assert_eq!(nameless.code, "INVALID_VALUE");
}

#[test]
fn next_shards_partition_a_pool() {
    let mut fixture = Fixture::new("shards");
    let ids: Vec<String> = (0..12).map(|index| format!("doc-{index}")).collect();
    for (index, id) in ids.iter().enumerate() {
        fixture.put(id, &format!("text {index}"), 1_000 + index as u64);
    }
    let mut seen: Vec<String> = Vec::new();
    for index in 0..3 {
        let shard = obj(vec![
            ("limit", n(100.0)),
            ("shard", Value::Array(vec![n(f64::from(index)), n(3.0)])),
        ]);
        let work = list(fixture.query("texts.next", shard, 2_000).unwrap());
        for item in &work {
            let args = field(item, "args").as_str().unwrap().to_string();
            let row = Value::String(args.clone()).canonical_json().unwrap();
            assert_eq!(flower_sdk::external::shard_of(&row, 3), index);
            seen.push(args);
        }
    }
    seen.sort();
    let mut expected = ids.clone();
    expected.sort();
    assert_eq!(seen, expected, "the shards cover the pool exactly once");
    // Without a shard, `next` lists the oldest markers first.
    let first = list(
        fixture
            .query("texts.next", obj(vec![("limit", n(2.0))]), 2_000)
            .unwrap(),
    );
    assert_eq!(args_of(&first), ["doc-0", "doc-1"]);
    let bad = fixture
        .query(
            "texts.next",
            obj(vec![("shard", Value::Array(vec![n(3.0), n(3.0)]))]),
            2_000,
        )
        .unwrap_err();
    assert_eq!(bad.code, "INVALID_VALUE");
}

#[test]
fn typed_external_handles_round_trip() {
    let mut fixture = Fixture::new("typed");
    fixture.put("a", "one", 1_000);
    fixture.put("b", "two words", 1_001);
    fixture.put("c", "three more words", 1_002);
    assert_eq!(
        fixture
            .query("typed_state", obj(vec![("id", s("a"))]), 1_500)
            .unwrap(),
        obj(vec![("status", s("pending")), ("count", Value::Null)])
    );
    assert_eq!(
        fixture.query("typed_next", obj(vec![]), 1_500).unwrap(),
        Value::Array(vec![s("one"), s("two words"), s("three more words")])
    );
    let jobs = list(
        fixture
            .mutate("typed_round", obj(vec![("owner", s("typed"))]), 2_000)
            .unwrap(),
    );
    assert_eq!(jobs.len(), 3);
    assert_eq!(field(&jobs[0], "args"), &s("a"));
    assert_eq!(field(&jobs[0], "expires_ms"), &n(3_000.0));
    assert_eq!(field(&jobs[1], "attempt"), &n(1.0));
    // a and c published their counts; b went back to the pool.
    assert_eq!(
        fixture
            .query("typed_state", obj(vec![("id", s("a"))]), 2_001)
            .unwrap(),
        obj(vec![("status", s("ready")), ("count", n(1.0))])
    );
    assert_eq!(
        fixture
            .query("typed_state", obj(vec![("id", s("c"))]), 2_001)
            .unwrap(),
        obj(vec![("status", s("ready")), ("count", n(3.0))])
    );
    assert_eq!(
        fixture.query("typed_next", obj(vec![]), 2_001).unwrap(),
        Value::Array(vec![s("two words")])
    );
    assert_eq!(
        fixture
            .query("typed_state", obj(vec![("id", s("missing"))]), 2_001)
            .unwrap(),
        Value::Null
    );
}
