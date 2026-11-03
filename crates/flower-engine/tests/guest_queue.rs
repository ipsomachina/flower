//! Guest queue claims against the engine, with a real SDK application
//! run in-process through `NativeRuntime`: claim order by priority and
//! group turn, scopes kept apart, named jobs and replacement, kept
//! results, the waiter line and admission, declared contracts, fencing
//! tokens, leases that lapse (or die on their last attempt) and fence
//! the old holder out, leases bound to the history incarnation, guest
//! and native claims on one queue, and rows stored before scopes.

extern crate alloc;

use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::NativeRuntime;
use flower_engine::storage::GroupStore;
use flower_engine::work_queue::{ClaimRequest, ItemState, NewItem};
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

fn text(value: &Value, name: &str) -> String {
    field(value, name).as_str().unwrap().to_string()
}

fn number(value: &Value, name: &str) -> f64 {
    match field(value, name) {
        Value::Number(number) => *number,
        other => panic!("{name} is {other:?}"),
    }
}

/// A queue module: enqueues, guest claims, renewals and reports, the
/// line and admission, typed payloads and results, and two declared
/// queues.
mod queue_app {
    use flower_sdk::queue::{
        ClaimOptions, EnqueueOptions, Job, Lease, Queue, QueueDef, TypedQueue,
    };
    use flower_sdk::schema::v;
    use flower_sdk::{
        App, Component, FlowerType, MutationCtx, QueryCtx, Value, fail, mutation, query,
    };

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Taken {
        pub id: String,
        pub name: Option<String>,
        pub token: String,
        pub fence: u64,
        pub scope: String,
        pub priority: i64,
        pub group: Option<String>,
        pub attempts: u32,
        pub payload: Value,
        pub owner: Option<String>,
        pub expires_ms: u64,
    }

    impl Taken {
        fn of(claim: flower_sdk::queue::Claim) -> Taken {
            Taken {
                id: claim.id,
                name: claim.name,
                token: claim.token,
                fence: claim.fence,
                scope: claim.scope,
                priority: claim.priority,
                group: claim.group,
                attempts: claim.attempts,
                payload: claim.payload,
                owner: claim.owner,
                expires_ms: claim.expires_ms,
            }
        }
    }

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Mail {
        pub to: String,
        pub retries: u32,
    }

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Sent {
        pub to: String,
        pub accepted: bool,
    }

    static MAIL: TypedQueue<Mail, Sent> = TypedQueue::new("mail");

    /// `jobs` declares a half-second line turn; `checked` declares its
    /// payload and result contracts.
    pub static JOBS: QueueDef = QueueDef::new("jobs").turn_ms(500);
    pub static CHECKED: QueueDef = QueueDef::new("checked")
        .payload(v::object(&[v::field("n", v::int().min(0).build())]))
        .result(v::string().build());

    fn job<'a>(id: &'a Option<String>, scope: &'a str, name: &'a Option<String>) -> Job<'a> {
        match (name, id) {
            (Some(name), _) => Job::Named { scope, name },
            (None, Some(id)) => Job::Id(id),
            (None, None) => Job::Id(""),
        }
    }

    #[mutation]
    #[allow(clippy::too_many_arguments)]
    pub fn put(
        ctx: &mut MutationCtx<'_, '_>,
        queue: Option<String>,
        payload: Value,
        scope: String,
        priority: i64,
        delay_ms: u64,
        group: Option<String>,
        name: Option<String>,
        replace: bool,
        max_attempts: Option<u32>,
    ) -> flower_sdk::Result<String> {
        let mut options = EnqueueOptions::default()
            .scope(&scope)
            .priority(priority)
            .delay_ms(delay_ms)
            .max_attempts(max_attempts.unwrap_or(5));
        if let Some(group) = &group {
            options = options.group(group);
        }
        if let Some(name) = &name {
            options = options.named(name);
        }
        if replace {
            options = options.replace();
        }
        Queue::enqueue(ctx, queue.as_deref().unwrap_or("jobs"), payload, &options)
    }

    #[mutation]
    pub fn put_at(
        ctx: &mut MutationCtx<'_, '_>,
        payload: Value,
        at_ms: u64,
    ) -> flower_sdk::Result<String> {
        let options = EnqueueOptions::default().at_ms(at_ms);
        Queue::enqueue(ctx, "jobs", payload, &options)
    }

    #[mutation]
    pub fn take(
        ctx: &mut MutationCtx<'_, '_>,
        queue: Option<String>,
        scope: String,
        owner: String,
        limit: u32,
        lease_ms: u64,
        wait_ms: Option<u64>,
    ) -> flower_sdk::Result<Vec<Taken>> {
        let mut options = ClaimOptions::new(&owner)
            .scope(&scope)
            .limit(limit as usize)
            .lease_ms(lease_ms);
        if let Some(wait_ms) = wait_ms {
            options = options.wait_ms(wait_ms);
        }
        let queue = queue.as_deref().unwrap_or("jobs");
        Ok(Queue::claim(ctx, queue, &options)?
            .into_iter()
            .map(Taken::of)
            .collect())
    }

    /// Claim admitting only even payloads: odd ones are deleted.
    #[mutation]
    pub fn take_even(
        ctx: &mut MutationCtx<'_, '_>,
        owner: String,
        limit: u32,
        wait_ms: Option<u64>,
    ) -> flower_sdk::Result<Vec<Taken>> {
        let mut options = ClaimOptions::new(&owner).limit(limit as usize);
        if let Some(wait_ms) = wait_ms {
            options = options.wait_ms(wait_ms);
        }
        let admitted = Queue::claim_admitting(ctx, "jobs", &options, |_, claim| {
            Ok(matches!(claim.payload, Value::Number(n) if n % 2.0 == 0.0))
        })?;
        Ok(admitted.into_iter().map(Taken::of).collect())
    }

    #[mutation]
    #[allow(clippy::too_many_arguments)]
    pub fn report(
        ctx: &mut MutationCtx<'_, '_>,
        queue: Option<String>,
        id: String,
        token: String,
        action: String,
        delay_ms: u64,
        result: Value,
    ) -> flower_sdk::Result<()> {
        let lease = Lease { id, token };
        let queue = queue.as_deref().unwrap_or("jobs");
        match action.as_str() {
            "complete" => Queue::complete(ctx, queue, &lease, result),
            "finish" => Queue::finish(ctx, queue, &lease),
            "fail" => Queue::fail(ctx, queue, &lease, delay_ms),
            "kill" => Queue::kill(ctx, queue, &lease),
            "release" => Queue::release(ctx, queue, &lease, delay_ms),
            _ => Err(fail("INVALID_ARGUMENT", "unknown report")),
        }
    }

    #[mutation]
    pub fn renew(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        token: String,
        lease_ms: u64,
    ) -> flower_sdk::Result<Option<u64>> {
        let mut renewed = Queue::renew(ctx, "jobs", &[Lease { id, token }], lease_ms)?;
        Ok(renewed.remove(0))
    }

    /// Enqueue `count` jobs and claim them one at a time in the same
    /// mutation: the claims see the enqueues and never take one twice.
    #[mutation]
    pub fn put_and_take(ctx: &mut MutationCtx<'_, '_>, count: u32) -> flower_sdk::Result<Vec<u64>> {
        for index in 0..count {
            let options = EnqueueOptions::default().scope("inline");
            Queue::enqueue(ctx, "jobs", Value::Number(f64::from(index)), &options)?;
        }
        let mut fences = Vec::new();
        for _ in 0..=count {
            let options = ClaimOptions::new("inline-worker").scope("inline");
            for claim in Queue::claim(ctx, "jobs", &options)? {
                fences.push(claim.fence);
            }
        }
        Ok(fences)
    }

    #[mutation]
    pub fn cancel(
        ctx: &mut MutationCtx<'_, '_>,
        id: Option<String>,
        scope: String,
        name: Option<String>,
    ) -> flower_sdk::Result<bool> {
        Queue::cancel(ctx, "jobs", job(&id, &scope, &name))
    }

    #[mutation]
    pub fn retry(ctx: &mut MutationCtx<'_, '_>, id: String) -> flower_sdk::Result<()> {
        Queue::retry(ctx, "jobs", Job::Id(&id), 0)
    }

    #[mutation]
    pub fn purge(ctx: &mut MutationCtx<'_, '_>, older_than_ms: u64) -> flower_sdk::Result<u64> {
        Queue::purge(ctx, "jobs", older_than_ms)
    }

    #[mutation]
    pub fn send(ctx: &mut MutationCtx<'_, '_>, to: String) -> flower_sdk::Result<String> {
        let mail = Mail { to, retries: 0 };
        MAIL.enqueue(ctx, &mail, &EnqueueOptions::default())
    }

    /// Claim typed mail and complete it with a typed result, returning
    /// the recipients.
    #[mutation]
    pub fn relay(ctx: &mut MutationCtx<'_, '_>) -> flower_sdk::Result<Vec<String>> {
        let mut sent = Vec::new();
        for claim in MAIL.claim(ctx, &ClaimOptions::new("relay").limit(10))? {
            let receipt = Sent {
                to: claim.payload.to.clone(),
                accepted: true,
            };
            MAIL.complete(ctx, &claim.lease(), &receipt)?;
            sent.push(claim.id);
        }
        Ok(sent)
    }

    #[query]
    pub fn receipt(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<Sent>> {
        MAIL.result(ctx.ctx(), Job::Id(&id))
    }

    #[query]
    pub fn stats(
        ctx: &mut QueryCtx<'_, '_>,
        scope: Option<String>,
        owner: Option<String>,
    ) -> flower_sdk::Result<Value> {
        let stats = match &owner {
            None => Queue::stats(ctx.ctx(), "jobs", scope.as_deref())?,
            Some(owner) => {
                let scope = scope.as_deref().unwrap_or("");
                let stats = Queue::stats_for(ctx.ctx(), "jobs", scope, owner)?;
                if Queue::ready(ctx.ctx(), "jobs", scope, owner)? != stats.ready {
                    return Err(fail("BROKEN", "ready and stats disagree"));
                }
                stats
            }
        };
        let mut map = alloc::collections::BTreeMap::new();
        map.insert("pending".into(), Value::Number(stats.pending as f64));
        map.insert("claimed".into(), Value::Number(stats.claimed as f64));
        map.insert("dead".into(), Value::Number(stats.dead as f64));
        map.insert("completed".into(), Value::Number(stats.completed as f64));
        map.insert("ready".into(), Value::Bool(stats.ready));
        map.insert(
            "next_ms".into(),
            stats
                .next_ms
                .map_or(Value::Null, |next| Value::Number(next as f64)),
        );
        Ok(Value::Object(map))
    }

    #[query]
    pub fn item(
        ctx: &mut QueryCtx<'_, '_>,
        id: Option<String>,
        scope: String,
        name: Option<String>,
    ) -> flower_sdk::Result<Value> {
        let Some(item) = Queue::get(ctx.ctx(), "jobs", job(&id, &scope, &name))? else {
            return Ok(Value::Null);
        };
        let text = |text: Option<String>| text.map_or(Value::Null, Value::String);
        let mut map = alloc::collections::BTreeMap::new();
        map.insert("id".into(), Value::String(item.id));
        map.insert("name".into(), text(item.name));
        map.insert("state".into(), Value::String(item.state));
        map.insert("scope".into(), Value::String(item.scope));
        map.insert("priority".into(), Value::Number(item.priority as f64));
        map.insert("fence".into(), Value::Number(item.fence as f64));
        map.insert("attempts".into(), Value::Number(f64::from(item.attempts)));
        map.insert("owner".into(), text(item.owner));
        map.insert("result".into(), item.result.unwrap_or(Value::Null));
        Ok(Value::Object(map))
    }

    static USES: [&dyn Component; 3] = [&JOBS, &CHECKED, &MAIL];
    static DEFINITIONS: [&dyn flower_sdk::Component; 15] = [
        &PUT,
        &PUT_AT,
        &TAKE,
        &TAKE_EVEN,
        &REPORT,
        &RENEW,
        &PUT_AND_TAKE,
        &CANCEL,
        &RETRY,
        &PURGE,
        &SEND,
        &RELAY,
        &RECEIPT,
        &STATS,
        &ITEM,
    ];
    pub static APP: App = App::new(&[&USES, &DEFINITIONS]);
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

/// Arguments for `put`: the defaults, with `pairs` over them.
fn put_args(pairs: Vec<(&str, Value)>) -> Value {
    let mut args = obj(vec![
        ("queue", Value::Null),
        ("payload", Value::Null),
        ("scope", s("")),
        ("priority", n(0.0)),
        ("delay_ms", n(0.0)),
        ("group", Value::Null),
        ("name", Value::Null),
        ("replace", Value::Bool(false)),
        ("max_attempts", Value::Null),
    ]);
    if let Value::Object(map) = &mut args {
        for (key, value) in pairs {
            map.insert(key.to_string(), value);
        }
    }
    args
}

impl Fixture {
    fn new(test: &str) -> Fixture {
        let mut path = std::env::temp_dir();
        path.push(format!("flower-guest-queue-{test}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut fixture = Fixture {
            engine: Engine::new(GroupStore::create(&path).unwrap()),
            path,
        };
        let app = &queue_app::APP;
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

    fn query(&self, name: &str, args: Value, now: u64) -> Value {
        self.engine
            .invoke_query(b"db", "app", name, &args, &Fixture::at(now))
            .unwrap()
    }

    fn put_with(&mut self, pairs: Vec<(&str, Value)>, now: u64) -> Result<Value, Failure> {
        self.mutate("put", put_args(pairs), now)
    }

    fn put(
        &mut self,
        payload: &str,
        scope: &str,
        priority: i64,
        delay_ms: u64,
        now: u64,
    ) -> String {
        let pairs = vec![
            ("payload", s(payload)),
            ("scope", s(scope)),
            ("priority", n(priority as f64)),
            ("delay_ms", n(delay_ms as f64)),
        ];
        self.put_with(pairs, now)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }

    #[allow(clippy::too_many_arguments)]
    fn take_in(
        &mut self,
        queue: &str,
        scope: &str,
        owner: &str,
        limit: u32,
        lease_ms: u64,
        wait_ms: Option<u64>,
        now: u64,
    ) -> Vec<Value> {
        let args = obj(vec![
            ("queue", s(queue)),
            ("scope", s(scope)),
            ("owner", s(owner)),
            ("limit", n(f64::from(limit))),
            ("lease_ms", n(lease_ms as f64)),
            (
                "wait_ms",
                wait_ms.map_or(Value::Null, |wait| n(wait as f64)),
            ),
        ]);
        match self.mutate("take", args, now).unwrap() {
            Value::Array(claims) => claims,
            other => panic!("take answered {other:?}"),
        }
    }

    fn take(
        &mut self,
        scope: &str,
        owner: &str,
        limit: u32,
        lease_ms: u64,
        now: u64,
    ) -> Vec<Value> {
        self.take_in("jobs", scope, owner, limit, lease_ms, None, now)
    }

    fn report_in(
        &mut self,
        queue: &str,
        claim: &Value,
        action: &str,
        result: Value,
        now: u64,
    ) -> Result<Value, Failure> {
        let args = obj(vec![
            ("queue", s(queue)),
            ("id", field(claim, "id").clone()),
            ("token", field(claim, "token").clone()),
            ("action", s(action)),
            ("delay_ms", n(0.0)),
            ("result", result),
        ]);
        self.mutate("report", args, now)
    }

    fn report(&mut self, claim: &Value, action: &str, now: u64) -> Result<Value, Failure> {
        self.report_in("jobs", claim, action, Value::Null, now)
    }

    fn renew(&mut self, claim: &Value, lease_ms: u64, now: u64) -> Value {
        let args = obj(vec![
            ("id", field(claim, "id").clone()),
            ("token", field(claim, "token").clone()),
            ("lease_ms", n(lease_ms as f64)),
        ]);
        self.mutate("renew", args, now).unwrap()
    }

    fn item(&self, id: &str, now: u64) -> Value {
        let args = obj(vec![("id", s(id)), ("scope", s("")), ("name", Value::Null)]);
        self.query("item", args, now)
    }

    fn stats(&self, scope: Option<&str>, owner: Option<&str>, now: u64) -> Value {
        let text = |text: Option<&str>| text.map_or(Value::Null, s);
        self.query(
            "stats",
            obj(vec![("scope", text(scope)), ("owner", text(owner))]),
            now,
        )
    }
}

fn payloads(claims: &[Value]) -> Vec<String> {
    claims.iter().map(|claim| text(claim, "payload")).collect()
}

fn fences(claims: &[Value]) -> Vec<f64> {
    claims.iter().map(|claim| number(claim, "fence")).collect()
}

fn request<'a>(scope: &'a str, owner: Option<&'a str>, limit: usize) -> ClaimRequest<'a> {
    ClaimRequest {
        scope,
        owner,
        limit,
        lease_ms: 60_000,
        wait_ms: None,
    }
}

#[test]
fn claims_take_the_lowest_priority_first_then_the_longest_waiting() {
    let mut fixture = Fixture::new("priority");
    fixture.put("low", "", 5, 0, 1_000);
    fixture.put("urgent-old", "", -1, 0, 1_001);
    fixture.put("normal", "", 0, 0, 1_002);
    fixture.put("urgent-new", "", -1, 0, 1_003);
    // The most urgent job is not due yet: it waits its turn in time,
    // then goes before everything due.
    fixture.put("delayed", "", -5, 9_000, 1_004);
    fixture.put("later", "", 5, 0, 1_005);

    let first = fixture.take("", "w1", 2, 60_000, 2_000);
    assert_eq!(payloads(&first), ["urgent-old", "urgent-new"]);
    let rest = fixture.take("", "w1", 10, 60_000, 2_001);
    assert_eq!(payloads(&rest), ["normal", "low", "later"]);
    assert_eq!(number(&rest[1], "priority"), 5.0);
    // Fencing tokens increase strictly across the claims of the scope.
    assert_eq!(fences(&first), [1.0, 2.0]);
    assert_eq!(fences(&rest), [3.0, 4.0, 5.0]);

    // Once due, the delayed job is claimed first, and a native claim
    // follows the same order and the same fence sequence.
    fixture.put("normal-again", "", 0, 0, 10_005);
    let native = fixture
        .engine
        .claim_queue(b"db", "jobs", &request("", Some("native"), 10), 10_010)
        .unwrap();
    let order: Vec<String> = native
        .iter()
        .map(|claim| claim.payload.as_str().unwrap().to_string())
        .collect();
    assert_eq!(order, ["delayed", "normal-again"]);
    assert_eq!(native[0].fence, 6);
    assert_eq!(native[0].priority, -5);
    assert_eq!(native[0].owner.as_deref(), Some("native"));

    // Native enqueues take priorities too.
    for (payload, priority) in [("n5", 5), ("n-9", -9)] {
        let payload = s(payload);
        let item = NewItem {
            priority,
            ..NewItem::new("jobs", &payload, 0, 5)
        };
        fixture.engine.enqueue_item(b"db", &item, 20_000).unwrap();
    }
    let polled = fixture
        .engine
        .poll_queue(b"db", "jobs", 20_001, 1, 60_000)
        .unwrap();
    assert_eq!(polled[0].payload, s("n-9"));
}

#[test]
fn groups_take_turns_within_a_priority() {
    let mut fixture = Fixture::new("groups");
    for (payload, group, at) in [
        ("a1", Some("acme"), 1_000),
        ("a2", Some("acme"), 1_001),
        ("a3", Some("acme"), 1_002),
        ("b1", Some("bolt"), 1_003),
        ("b2", Some("bolt"), 1_004),
        ("solo", None, 1_005),
    ] {
        let group = group.map_or(Value::Null, s);
        fixture
            .put_with(vec![("payload", s(payload)), ("group", group)], at)
            .unwrap();
    }
    // Each ready group's next job goes before any group's second: acme
    // enqueued three first, yet bolt's first and the ungrouped job come
    // before acme's second.
    let claims = fixture.take("", "w", 10, 60_000, 2_000);
    assert_eq!(payloads(&claims), ["a1", "b1", "solo", "a2", "b2", "a3"]);
    assert_eq!(text(&claims[0], "group"), "acme");
    // A higher priority still goes first, whatever the turns.
    fixture
        .put_with(vec![("payload", s("c1")), ("group", s("acme"))], 3_000)
        .unwrap();
    fixture
        .put_with(
            vec![
                ("payload", s("urgent")),
                ("group", s("acme")),
                ("priority", n(-1.0)),
            ],
            3_001,
        )
        .unwrap();
    assert_eq!(
        payloads(&fixture.take("", "w", 10, 60_000, 4_000)),
        ["urgent", "c1"]
    );
}

#[test]
fn scopes_keep_claims_counts_and_fences_apart() {
    let mut fixture = Fixture::new("scopes");
    fixture.put("a1", "tenant:a", 0, 0, 1_000);
    fixture.put("a2", "tenant:a", 0, 0, 1_001);
    fixture.put("b1", "tenant:b", 0, 0, 1_002);
    fixture.put("d1", "", 0, 0, 1_003);

    let a = fixture.take("tenant:a", "wa", 10, 60_000, 2_000);
    assert_eq!(payloads(&a), ["a1", "a2"]);
    assert!(a.iter().all(|claim| text(claim, "scope") == "tenant:a"));
    assert_eq!(fences(&a), [1.0, 2.0]);
    // Each scope counts its own fences.
    let b = fixture.take("tenant:b", "wb", 10, 60_000, 2_000);
    assert_eq!(payloads(&b), ["b1"]);
    assert_eq!(fences(&b), [1.0]);
    assert!(fixture.take("tenant:a", "wa", 10, 60_000, 2_001).is_empty());
    let d = fixture.take("", "wd", 10, 60_000, 2_002);
    assert_eq!(payloads(&d), ["d1"]);

    let stats = fixture.stats(Some("tenant:a"), None, 2_003);
    assert_eq!(number(&stats, "claimed"), 2.0);
    assert_eq!(number(&stats, "pending"), 0.0);
    let all = fixture.stats(None, None, 2_003);
    assert_eq!(number(&all, "claimed"), 4.0);
    let native = fixture
        .engine
        .queue_stats_in(b"db", "jobs", Some("tenant:b"))
        .unwrap();
    assert_eq!((native.pending, native.claimed), (0, 1));
    assert_eq!(
        fixture.engine.queue_stats(b"db", "jobs").unwrap().claimed,
        4
    );
    // A native claim on another scope sees none of these.
    let other = fixture
        .engine
        .claim_queue(b"db", "jobs", &request("tenant:c", None, 10), 100_000)
        .unwrap();
    assert!(other.is_empty());
}

#[test]
fn named_jobs_are_unique_per_scope_and_replace_finished_ones() {
    let mut fixture = Fixture::new("names");
    let named = |name: &str, scope: &str, replace: bool| {
        vec![
            ("payload", s("invoice")),
            ("name", s(name)),
            ("scope", s(scope)),
            ("replace", Value::Bool(replace)),
        ]
    };
    let id = fixture
        .put_with(named("inv-1", "t1", false), 1_000)
        .unwrap();
    let taken = fixture
        .put_with(named("inv-1", "t1", false), 1_001)
        .unwrap_err();
    assert_eq!(taken.code, "JOB_EXISTS");
    // A pending job is not replaced either.
    let pending = fixture
        .put_with(named("inv-1", "t1", true), 1_002)
        .unwrap_err();
    assert_eq!(pending.code, "JOB_EXISTS");
    // The same name in another scope is another job.
    let other = fixture
        .put_with(named("inv-1", "t2", false), 1_003)
        .unwrap();
    assert_ne!(other, id);
    assert_eq!(
        id.as_str().unwrap(),
        flower_engine::work_queue::named_id("t1", "inv-1")
    );
    // Found by name; claimed, it carries its name.
    let by_name = fixture.query(
        "item",
        obj(vec![
            ("id", Value::Null),
            ("scope", s("t1")),
            ("name", s("inv-1")),
        ]),
        1_004,
    );
    assert_eq!(field(&by_name, "id"), &id);
    let claim = fixture.take("t1", "w", 1, 60_000, 2_000);
    assert_eq!(text(&claim[0], "name"), "inv-1");
    assert_eq!(
        fixture
            .put_with(named("inv-1", "t1", true), 2_001)
            .unwrap_err()
            .code,
        "JOB_EXISTS"
    );
    // Completed, it may be replaced, under the same ID; replace is
    // needed.
    fixture
        .report_in("jobs", &claim[0], "complete", s("paid"), 2_002)
        .unwrap();
    assert_eq!(
        fixture
            .put_with(named("inv-1", "t1", false), 2_003)
            .unwrap_err()
            .code,
        "JOB_EXISTS"
    );
    let replaced = fixture.put_with(named("inv-1", "t1", true), 2_004).unwrap();
    assert_eq!(replaced, id);
    let item = fixture.item(id.as_str().unwrap(), 2_005);
    assert_eq!(text(&item, "state"), "pending");
    assert_eq!(field(&item, "result"), &Value::Null);
    // Cancelled by name.
    let cancelled = fixture
        .mutate(
            "cancel",
            obj(vec![
                ("id", Value::Null),
                ("scope", s("t1")),
                ("name", s("inv-1")),
            ]),
            2_006,
        )
        .unwrap();
    assert_eq!(cancelled, Value::Bool(true));
    assert_eq!(fixture.item(id.as_str().unwrap(), 2_007), Value::Null);
}

#[test]
fn completed_jobs_keep_their_results_until_purged() {
    let mut fixture = Fixture::new("results");
    let kept = fixture.put("kept", "", 0, 0, 1_000);
    let gone = fixture.put("gone", "", 0, 0, 1_001);
    let claims = fixture.take("", "w", 2, 60_000, 2_000);
    fixture.report(&claims[1], "finish", 2_001).unwrap();
    fixture
        .report_in(
            "jobs",
            &claims[0],
            "complete",
            obj(vec![("ok", Value::Bool(true))]),
            2_002,
        )
        .unwrap();
    let item = fixture.item(&kept, 2_003);
    assert_eq!(text(&item, "state"), "completed");
    assert_eq!(
        field(&item, "result"),
        &obj(vec![("ok", Value::Bool(true))])
    );
    assert_eq!(fixture.item(&gone, 2_003), Value::Null);
    assert_eq!(number(&fixture.stats(None, None, 2_003), "completed"), 1.0);
    // A worker keeps a result over the native path too.
    fixture.put("native", "", 0, 0, 3_000);
    let claim = fixture
        .engine
        .poll_queue(b"db", "jobs", 3_001, 1, 60_000)
        .unwrap();
    fixture
        .engine
        .complete_queue_with(b"db", "jobs", &claim[0].id, &claim[0].token, &n(7.0), 3_002)
        .unwrap();
    let native = fixture
        .engine
        .get_queue_item(b"db", "jobs", &claim[0].id)
        .unwrap()
        .unwrap();
    assert_eq!(native.state, ItemState::Completed);
    assert_eq!(native.result, Some(n(7.0)));
    // Purges bound them: enqueued before the cutoff, they go.
    let purged = fixture
        .mutate("purge", obj(vec![("older_than_ms", n(2_500.0))]), 4_000)
        .unwrap();
    assert_eq!(purged, n(1.0));
    assert_eq!(fixture.item(&kept, 4_001), Value::Null);
    assert_eq!(fixture.engine.purge_queue(b"db", "jobs", 4_000).unwrap(), 1);
    assert_eq!(
        fixture.engine.queue_stats(b"db", "jobs").unwrap().completed,
        0
    );
}

#[test]
fn typed_queues_round_trip_payloads_and_results() {
    let mut fixture = Fixture::new("typed");
    for (to, at) in [("ada", 1_000), ("bob", 1_001)] {
        fixture
            .mutate("send", obj(vec![("to", s(to))]), at)
            .unwrap();
    }
    let sent = fixture.mutate("relay", obj(vec![]), 1_002).unwrap();
    let Value::Array(ids) = sent else {
        panic!("relay answered {sent:?}");
    };
    let receipt = fixture.query("receipt", obj(vec![("id", ids[1].clone())]), 1_003);
    assert_eq!(
        receipt,
        obj(vec![("to", s("bob")), ("accepted", Value::Bool(true))])
    );
    assert_eq!(
        fixture.engine.queue_stats(b"db", "mail").unwrap().completed,
        2
    );
}

#[test]
fn payloads_and_results_meet_the_queues_declared_contracts() {
    let mut fixture = Fixture::new("contracts");
    let checked = |payload: Value| vec![("queue", s("checked")), ("payload", payload)];
    let refused = fixture
        .put_with(checked(obj(vec![("n", n(-1.0))])), 1_000)
        .unwrap_err();
    assert_eq!(refused.code, "SCHEMA_MISMATCH");
    fixture
        .put_with(checked(obj(vec![("n", n(3.0))])), 1_001)
        .unwrap();
    let bad = obj(vec![("n", s("three"))]);
    let native = fixture
        .engine
        .enqueue_item(b"db", &NewItem::new("checked", &bad, 0, 5), 1_002)
        .unwrap_err();
    assert_eq!(native.code, "SCHEMA_MISMATCH");
    let claim = fixture.take_in("checked", "", "w", 1, 60_000, None, 2_000);
    let wrong = fixture
        .report_in("checked", &claim[0], "complete", n(3.0), 2_001)
        .unwrap_err();
    assert_eq!(wrong.code, "SCHEMA_MISMATCH");
    let wrong = fixture
        .engine
        .complete_queue_with(
            b"db",
            "checked",
            field(&claim[0], "id").as_str().unwrap(),
            field(&claim[0], "token").as_str().unwrap(),
            &n(3.0),
            2_002,
        )
        .unwrap_err();
    assert_eq!(wrong.code, "SCHEMA_MISMATCH");
    fixture
        .report_in("checked", &claim[0], "complete", s("three"), 2_003)
        .unwrap();
    // A queue declared without contracts takes anything.
    fixture.put_with(vec![("payload", n(-1.0))], 3_000).unwrap();
}

#[test]
fn the_line_gives_its_first_owner_new_work_for_its_turn() {
    let mut fixture = Fixture::new("line");
    // Two owners wait in line, first w1 then w2.
    assert!(
        fixture
            .take_in("jobs", "", "w1", 1, 60_000, Some(10_000), 1_000)
            .is_empty()
    );
    assert!(
        fixture
            .take_in("jobs", "", "w2", 1, 60_000, Some(10_000), 1_100)
            .is_empty()
    );
    let id = fixture.put("job", "", 0, 0, 2_000);
    // The job is w1's for the queue's half-second turn: w2 is told to
    // wait until the turn ends.
    let first = fixture.stats(Some(""), Some("w1"), 2_000);
    assert_eq!(field(&first, "ready"), &Value::Bool(true));
    let second = fixture.stats(Some(""), Some("w2"), 2_100);
    assert_eq!(field(&second, "ready"), &Value::Bool(false));
    assert_eq!(field(&second, "next_ms"), &n(2_500.0));
    // Without a line position, anyone may claim.
    assert_eq!(
        field(&fixture.stats(Some(""), None, 2_100), "ready"),
        &Value::Bool(true)
    );
    // w1 let the turn pass: w2 is ready, and claiming takes w1's place.
    let late = fixture.stats(Some(""), Some("w2"), 2_500);
    assert_eq!(field(&late, "ready"), &Value::Bool(true));
    let claim = fixture.take_in("jobs", "", "w2", 1, 60_000, Some(10_000), 2_500);
    assert_eq!(text(&claim[0], "id"), id);
    // w1 lost its place; w2 claimed what it wanted and left the line.
    fixture.put("next", "", 0, 0, 3_000);
    let other = fixture.stats(Some(""), Some("w3"), 3_000);
    assert_eq!(field(&other, "ready"), &Value::Bool(true));
}

#[test]
fn admission_deletes_refused_jobs_and_waits_in_line_when_short() {
    let mut fixture = Fixture::new("admission");
    for value in 1u32..=4 {
        let pairs = vec![("payload", n(f64::from(value)))];
        fixture.put_with(pairs, 1_000 + u64::from(value)).unwrap();
    }
    let args = obj(vec![
        ("owner", s("picky")),
        ("limit", n(5.0)),
        ("wait_ms", n(10_000.0)),
    ]);
    let admitted = fixture.mutate("take_even", args, 2_000).unwrap();
    let Value::Array(admitted) = admitted else {
        panic!("take_even answered {admitted:?}");
    };
    let values: Vec<Value> = admitted
        .iter()
        .map(|claim| field(claim, "payload").clone())
        .collect();
    assert_eq!(values, [n(2.0), n(4.0)]);
    // The odd jobs were deleted, not left for others.
    let stats = fixture.stats(None, None, 2_001);
    assert_eq!(
        (number(&stats, "pending"), number(&stats, "claimed")),
        (0.0, 2.0)
    );
    // It came back short, so it waits first in line: new work is its
    // own for the turn.
    fixture.put_with(vec![("payload", n(6.0))], 2_002).unwrap();
    let rival = fixture.stats(Some(""), Some("rival"), 2_003);
    assert_eq!(field(&rival, "ready"), &Value::Bool(false));
    let own = fixture.stats(Some(""), Some("picky"), 2_003);
    assert_eq!(field(&own, "ready"), &Value::Bool(true));
}

#[test]
fn a_lapsed_guest_claim_is_fenced_out() {
    let mut fixture = Fixture::new("fenced");
    let id = fixture.put("job", "", 0, 0, 1_000);
    let first = fixture.take("", "w1", 1, 1_000, 1_000);
    assert_eq!(text(&first[0], "id"), id);
    assert_eq!(number(&first[0], "expires_ms"), 2_000.0);
    assert_eq!(text(&first[0], "owner"), "w1");
    // Renewing within the lease extends it and keeps the fence.
    assert_eq!(fixture.renew(&first[0], 1_000, 1_500), n(2_500.0));
    assert!(fixture.take("", "w2", 1, 1_000, 2_400).is_empty());
    let stats = fixture.stats(Some(""), None, 2_400);
    assert_eq!(
        stats,
        obj(vec![
            ("pending", n(0.0)),
            ("claimed", n(1.0)),
            ("dead", n(0.0)),
            ("completed", n(0.0)),
            ("ready", Value::Bool(false)),
            ("next_ms", n(2_500.0)),
        ])
    );

    // The lease lapses: another owner takes the job over with a larger
    // fence and a new token.
    let second = fixture.take("", "w2", 1, 1_000, 2_500);
    assert_eq!(text(&second[0], "id"), id);
    assert_eq!(number(&second[0], "fence"), 2.0);
    assert_eq!(number(&second[0], "attempts"), 2.0);
    assert_ne!(text(&second[0], "token"), text(&first[0], "token"));

    // The old holder is fenced out: no renewal, no report.
    assert_eq!(fixture.renew(&first[0], 1_000, 2_600), Value::Null);
    for action in ["complete", "finish", "fail", "kill", "release"] {
        let refused = fixture.report(&first[0], action, 2_600).unwrap_err();
        assert_eq!(refused.code, "INVALID_VALUE", "{action}: {refused:?}");
    }
    let item = fixture.item(&id, 2_600);
    assert_eq!(text(&item, "state"), "claimed");
    assert_eq!(text(&item, "owner"), "w2");
    assert_eq!(number(&item, "fence"), 2.0);

    // The new holder finishes; a later claim still fences above both.
    fixture.report(&second[0], "finish", 2_700).unwrap();
    assert_eq!(fixture.item(&id, 2_700), Value::Null);
    fixture.put("next", "", 0, 0, 3_000);
    let third = fixture.take("", "w3", 1, 1_000, 3_000);
    assert_eq!(number(&third[0], "fence"), 3.0);
}

#[test]
fn a_claim_lapsing_on_its_last_attempt_dies() {
    let mut fixture = Fixture::new("last");
    let id = fixture
        .put_with(
            vec![("payload", s("once")), ("max_attempts", n(1.0))],
            1_000,
        )
        .unwrap();
    let id = id.as_str().unwrap().to_string();
    assert_eq!(fixture.take("", "w", 1, 100, 1_000).len(), 1);
    // The lease lapses on the only attempt: the job dies rather than
    // redelivering, as v1 fails it.
    assert!(fixture.take("", "w", 1, 100, 2_000).is_empty());
    assert_eq!(text(&fixture.item(&id, 2_001), "state"), "dead");
}

#[test]
fn a_lapse_on_the_last_attempt_reads_dead_before_any_claim() {
    let mut fixture = Fixture::new("lapse-reads");
    let named = |replace: bool| {
        vec![
            ("payload", s("nightly")),
            ("name", s("nightly")),
            ("max_attempts", n(1.0)),
            ("replace", Value::Bool(replace)),
        ]
    };
    let id = fixture.put_with(named(false), 1_000).unwrap();
    let id = id.as_str().unwrap().to_string();
    let claim = fixture.take("", "w", 1, 100, 1_000);
    assert_eq!(text(&fixture.item(&id, 1_050), "state"), "claimed");
    // The lease ends at 1,100 on the only attempt, and nothing claims
    // since: stats and reads see the job dead all the same (v1's
    // `effective`), and its holder can no longer report.
    let stats = fixture.stats(None, None, 2_000);
    assert_eq!(number(&stats, "claimed"), 0.0);
    assert_eq!(number(&stats, "dead"), 1.0);
    assert_eq!(text(&fixture.item(&id, 2_000), "state"), "dead");
    let late = fixture.report(&claim[0], "finish", 2_000).unwrap_err();
    assert!(late.message.contains("lapsed"), "{late:?}");
    // A retry takes it.
    fixture
        .mutate("retry", obj(vec![("id", s(&id))]), 2_000)
        .unwrap();
    assert_eq!(text(&fixture.item(&id, 2_000), "state"), "pending");
    assert_eq!(fixture.take("", "w", 1, 100, 2_000).len(), 1);
    // Lapsed on its last attempt again, the name is taken until a
    // replace takes it over.
    let taken = fixture.put_with(named(false), 3_000).unwrap_err();
    assert_eq!(taken.code, "JOB_EXISTS");
    assert_eq!(
        fixture.put_with(named(true), 3_000).unwrap().as_str(),
        Some(id.as_str())
    );
    let stats = fixture.stats(None, None, 3_000);
    assert_eq!(number(&stats, "pending"), 1.0);
    assert_eq!(number(&stats, "claimed"), 0.0);
    assert_eq!(number(&stats, "dead"), 0.0);
}

#[test]
fn guest_report_delays_are_bounded() {
    let mut fixture = Fixture::new("report-delays");
    fixture.put("job", "", 0, 0, 1_000);
    let claim = fixture.take("", "w", 1, 1_000, 1_000);
    for action in ["fail", "release"] {
        let args = obj(vec![
            ("queue", s("jobs")),
            ("id", field(&claim[0], "id").clone()),
            ("token", field(&claim[0], "token").clone()),
            ("action", s(action)),
            ("delay_ms", n(30.0 * 24.0 * 60.0 * 60.0 * 1000.0 + 1.0)),
            ("result", Value::Null),
        ]);
        let refused = fixture.mutate("report", args, 1_000).unwrap_err();
        assert_eq!(refused.code, "INVALID_VALUE", "{action}: {refused:?}");
    }
    // Nothing changed: the claim still reports.
    fixture.report(&claim[0], "finish", 1_000).unwrap();
}

/// Set the database's history incarnation, as establishing one or a
/// restore does.
fn set_incarnation(fixture: &Fixture, incarnation: u64) {
    let write = fixture.engine.store().begin_write().unwrap();
    write
        .set_meta_u64(
            b"db",
            flower_engine::storage::meta::HISTORY_INCARNATION,
            incarnation,
        )
        .unwrap();
    write.commit().unwrap();
}

#[test]
fn claims_from_another_history_are_fenced_out() {
    // Incarnations are random 64-bit values: one past what a double
    // holds exactly must survive the claim's row.
    let incarnation = 10_253_275_921_945_582_101;
    let mut fixture = Fixture::new("history");
    set_incarnation(&fixture, incarnation);
    let id = fixture.put("job", "", 0, 0, 1_000);
    let claim = fixture.take("", "w", 1, 1_000, 1_000);
    // In its own history, the holder renews.
    assert_ne!(fixture.renew(&claim[0], 1_000, 1_000), Value::Null);
    // A restore brings the database to another history incarnation.
    set_incarnation(&fixture, incarnation + 1);
    assert_eq!(fixture.renew(&claim[0], 1_000, 1_100), Value::Null);
    let refused = fixture.report(&claim[0], "finish", 1_100).unwrap_err();
    assert!(refused.message.contains("history"), "{refused:?}");
    let native = fixture
        .engine
        .complete_queue(
            b"db",
            "jobs",
            &id,
            field(&claim[0], "token").as_str().unwrap(),
            1_100,
        )
        .unwrap_err();
    assert!(native.message.contains("history"), "{native:?}");
    // Once the lease lapses, the job is claimed again in the new history.
    let again = fixture.take("", "w", 1, 1_000, 2_000);
    assert_eq!(text(&again[0], "id"), id);
    fixture.report(&again[0], "finish", 2_001).unwrap();
}

#[test]
fn a_claim_taken_before_the_first_incarnation_stays_good() {
    // A new cluster's leader establishes the history incarnation from
    // its maintenance shortly after it starts: claims taken before that
    // were taken in this history, which the first incarnation names.
    let mut fixture = Fixture::new("first");
    let id = fixture.put("job", "", 0, 0, 1_000);
    let claim = fixture.take("", "w", 1, 1_000, 1_000);
    set_incarnation(&fixture, 5);
    assert_ne!(fixture.renew(&claim[0], 1_000, 1_100), Value::Null);
    fixture.report(&claim[0], "finish", 1_200).unwrap();
    assert_eq!(fixture.item(&id, 1_200), Value::Null);
}

#[test]
fn guest_reports_fail_kill_release_cancel_and_retry() {
    let mut fixture = Fixture::new("reports");
    let id = fixture.put("job", "", 0, 0, 1_000);
    let claim = fixture.take("", "w", 1, 1_000, 1_000);
    fixture.report(&claim[0], "release", 1_100).unwrap();
    let item = fixture.item(&id, 1_100);
    assert_eq!(text(&item, "state"), "pending");
    assert_eq!(field(&item, "owner"), &Value::Null);
    let claim = fixture.take("", "w", 1, 1_000, 1_200);
    fixture.report(&claim[0], "kill", 1_300).unwrap();
    let item = fixture.item(&id, 1_300);
    assert_eq!(text(&item, "state"), "dead");
    fixture
        .mutate("retry", obj(vec![("id", s(&id))]), 1_400)
        .unwrap();
    let claim = fixture.take("", "w", 1, 1_000, 1_500);
    assert_eq!(number(&claim[0], "attempts"), 1.0);
    fixture.report(&claim[0], "fail", 1_600).unwrap();
    let cancel = obj(vec![
        ("id", s(&id)),
        ("scope", s("")),
        ("name", Value::Null),
    ]);
    assert_eq!(
        fixture.mutate("cancel", cancel, 1_700).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        fixture.engine.queue_stats(b"db", "jobs").unwrap().pending,
        0
    );
}

#[test]
fn a_mutation_sees_its_own_enqueues_and_never_takes_one_item_twice() {
    let mut fixture = Fixture::new("inline");
    let fences = fixture
        .mutate("put_and_take", obj(vec![("count", n(3.0))]), 1_000)
        .unwrap();
    assert_eq!(fences, Value::Array(vec![n(1.0), n(2.0), n(3.0)]));
    let stats = fixture
        .engine
        .queue_stats_in(b"db", "jobs", Some("inline"))
        .unwrap();
    assert_eq!((stats.pending, stats.claimed), (0, 3));
}

#[test]
fn items_enqueued_for_a_time_wait_for_it() {
    let mut fixture = Fixture::new("at");
    let args = obj(vec![("payload", s("timed")), ("at_ms", n(5_000.0))]);
    fixture.mutate("put_at", args, 1_000).unwrap();
    // A time already past is due at once.
    let args = obj(vec![("payload", s("past")), ("at_ms", n(10.0))]);
    fixture.mutate("put_at", args, 1_001).unwrap();
    assert_eq!(payloads(&fixture.take("", "w", 10, 1_000, 4_999)), ["past"]);
    assert_eq!(
        payloads(&fixture.take("", "w", 10, 1_000, 5_000)),
        ["timed"]
    );
}

/// The state-row key of the layout before scopes: tag, queue padded to
/// 128 bytes, big-endian time, ID.
fn legacy_key(tag: u8, queue: &str, time: u64, id: &[u8; 16]) -> Vec<u8> {
    let mut key = vec![tag];
    let mut padded = queue.as_bytes().to_vec();
    padded.resize(128, 0);
    key.extend_from_slice(&padded);
    key.extend_from_slice(&time.to_be_bytes());
    key.extend_from_slice(id);
    key
}

#[test]
fn rows_stored_before_scopes_still_claim_and_move() {
    let mut fixture = Fixture::new("legacy");
    let id = [0xAB; 16];
    let hex: String = id.iter().map(|byte| format!("{byte:02x}")).collect();
    {
        let write = fixture.engine.store().begin_write().unwrap();
        let collection = write.intern(b"db", b"$queue").unwrap();
        let value = br#"{"attempts":0,"claimed_until_ms":0,"due_ms":500,"enqueued_ms":500,"max_attempts":3,"payload":"old","state":"pending","token":null,"v":1}"#;
        write
            .put_record(
                b"db",
                collection,
                &legacy_key(b'p', "jobs", 500, &id),
                value,
            )
            .unwrap();
        let mut pointer = vec![b'i'];
        let mut padded = b"jobs".to_vec();
        padded.resize(128, 0);
        pointer.extend_from_slice(&padded);
        pointer.extend_from_slice(&id);
        write
            .put_record(b"db", collection, &pointer, br#"["pending","500"]"#)
            .unwrap();
        write.commit().unwrap();
    }
    let item = fixture
        .engine
        .get_queue_item(b"db", "jobs", &hex)
        .unwrap()
        .expect("the legacy item reads");
    assert_eq!((item.scope.as_str(), item.priority, item.fence), ("", 0, 0));
    assert_eq!(
        fixture.engine.queue_stats(b"db", "jobs").unwrap().pending,
        1
    );
    // Another scope never sees it; the default scope claims it before a
    // newer job of the same priority.
    assert!(fixture.take("other", "w", 10, 1_000, 1_000).is_empty());
    fixture.put("new", "", 0, 0, 900);
    let claims = fixture.take("", "w", 10, 1_000, 1_000);
    assert_eq!(payloads(&claims), ["old", "new"]);
    assert_eq!(text(&claims[0], "id"), hex);
    // Claimed, it moved to the current layout: reports and lookups find
    // it.
    let item = fixture
        .engine
        .get_queue_item(b"db", "jobs", &hex)
        .unwrap()
        .unwrap();
    assert_eq!(item.state, ItemState::Claimed);
    assert_eq!(item.fence, 1);
    fixture.report(&claims[0], "finish", 1_100).unwrap();
    assert!(
        fixture
            .engine
            .get_queue_item(b"db", "jobs", &hex)
            .unwrap()
            .is_none()
    );
    let stats = fixture.engine.queue_stats(b"db", "jobs").unwrap();
    assert_eq!((stats.pending, stats.claimed), (0, 1));
}
