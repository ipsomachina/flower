//! Module contracts at the engine, with real SDK applications run
//! in-process through `NativeRuntime`: exports keep everything else
//! private, imported collections are read-only, caller allowlists hold
//! at deployment and at the call, slots bind per deployment, and
//! deployment refuses links that would break either side.

extern crate alloc;

use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::NativeRuntime;
use flower_engine::storage::GroupStore;
use flower_sdk::error::codes;
use flower_sdk::{Failure, Value};

/// The provider: members (exported), secrets (private), a derived count
/// over its own members, and methods with and without exports.
mod core_app {
    use flower_sdk::{
        App, Collection, Component, Derived, DerivedCtx, FlowerType, Method, MutationCtx, QueryCtx,
        ReadCtx, collection, derived, mutation, query,
    };

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Member {
        pub name: String,
    }

    #[collection(name = "members", key = String, value = Member, export)]
    pub struct Members;

    #[collection(name = "secrets", key = String, value = String)]
    pub struct Secrets;

    #[derived(export)]
    pub fn member_count(ctx: &mut DerivedCtx<'_, '_>) -> flower_sdk::Result<u64> {
        let page = ctx.scan(flower_sdk::CollectionRef::local("members"), 100, None)?;
        Ok(page.rows.len() as u64)
    }

    #[query(export)]
    pub fn count(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<u64> {
        let page = ctx.scan(flower_sdk::CollectionRef::local("members"), 100, None)?;
        Ok(page.rows.len() as u64)
    }

    #[mutation(callers("crm"))]
    pub fn add_member(ctx: &mut MutationCtx<'_, '_>, name: String) -> flower_sdk::Result<u64> {
        Members::set(ctx, &name.clone(), &Member { name })?;
        Ok(1)
    }

    #[mutation]
    pub fn internal(ctx: &mut MutationCtx<'_, '_>) -> flower_sdk::Result<()> {
        Secrets::set(ctx, &String::from("s"), &String::from("hidden"))
    }

    #[mutation]
    pub fn seed(ctx: &mut MutationCtx<'_, '_>) -> flower_sdk::Result<()> {
        Secrets::set(ctx, &String::from("s"), &String::from("hidden"))?;
        Members::set(
            ctx,
            &String::from("ada"),
            &Member {
                name: String::from("ada"),
            },
        )
    }

    /// Promises a whole number, writes, then answers text: a contract
    /// breach the host catches where another module calls.
    fn liar(
        ctx: &mut flower_sdk::Ctx<'_, '_>,
        _args: flower_sdk::Value,
    ) -> flower_sdk::Result<flower_sdk::Value> {
        ctx.set(
            flower_sdk::CollectionRef::local("members"),
            &flower_sdk::Value::from("liar"),
            Member {
                name: String::from("liar"),
            }
            .encode()?,
        )?;
        Ok(flower_sdk::Value::from("not a number"))
    }

    pub static LIAR: Method = Method::mutation("liar", liar)
        .result(<u64 as FlowerType>::SCHEMA)
        .export();

    static PARTS: [&dyn Component; 8] = [
        &Members,
        &Secrets,
        &MEMBER_COUNT,
        &COUNT,
        &ADD_MEMBER,
        &INTERNAL,
        &SEED,
        &LIAR,
    ];
    pub static APP: App = App::new(&PARTS);

    /// The same module after a careless refactor: `count` now answers
    /// text, which importers expecting a number would not accept.
    #[query]
    pub fn count_text(ctx: &mut QueryCtx<'_, '_>) -> String {
        let _ = ctx;
        String::from("many")
    }

    pub static COUNT_AS_TEXT: Method = Method::query("count", __flower_export_count_text)
        .args(<CountTextArgs as FlowerType>::SCHEMA)
        .result(<String as FlowerType>::SCHEMA)
        .export();
    static TEXT_PARTS: [&dyn Component; 8] = [
        &Members,
        &Secrets,
        &MEMBER_COUNT,
        &COUNT_AS_TEXT,
        &ADD_MEMBER,
        &INTERNAL,
        &SEED,
        &LIAR,
    ];
    pub static TEXT_APP: App = App::new(&TEXT_PARTS);

    /// The same module exporting nothing.
    static PRIVATE_MEMBERS: Collection = Collection {
        export: false,
        ..MEMBERS_COLLECTION
    };
    static PRIVATE_MEMBER_COUNT: Derived = Derived {
        export: false,
        ..MEMBER_COUNT
    };
    static PRIVATE_COUNT: Method = Method {
        export: flower_sdk::Export::Private,
        ..COUNT.method
    };
    static PRIVATE_ADD_MEMBER: Method = Method {
        export: flower_sdk::Export::Private,
        ..ADD_MEMBER.method
    };
    static PRIVATE_LIAR: Method = Method {
        export: flower_sdk::Export::Private,
        ..LIAR
    };
    static PRIVATE_PARTS: [&dyn Component; 8] = [
        &PRIVATE_MEMBERS,
        &Secrets,
        &PRIVATE_MEMBER_COUNT,
        &PRIVATE_COUNT,
        &PRIVATE_ADD_MEMBER,
        &INTERNAL,
        &SEED,
        &PRIVATE_LIAR,
    ];
    pub static PRIVATE_APP: App = App::new(&PRIVATE_PARTS);
}

/// The consumer: imports `$core`'s members, count, derived count and
/// add_member, then reaches into everything, allowed or not.
mod crm_app {
    use flower_sdk::{
        App, CallOutcome, CollectionRef, Component, Contract, FlowerType, Import,
        ImportedCollection, ImportedDerived, ImportedMethod, MethodCtx, MethodKind, MutationCtx,
        QueryCtx, ReadCtx, Value, mutation, query,
    };

    use super::core_app::Member;

    #[query]
    pub fn read(
        ctx: &mut QueryCtx<'_, '_>,
        collection: String,
        key: Value,
    ) -> flower_sdk::Result<Value> {
        Ok(ctx
            .get(CollectionRef::new("$core", &collection), &key)?
            .unwrap_or(Value::Null))
    }

    #[query]
    pub fn read_count(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Value> {
        ctx.get_derived("$core", "member_count", &Value::Object(Default::default()))
    }

    #[mutation]
    pub fn write(ctx: &mut MutationCtx<'_, '_>, collection: String) -> flower_sdk::Result<()> {
        ctx.set(
            CollectionRef::new("$core", &collection),
            &Value::from("eve"),
            Member {
                name: String::from("eve"),
            }
            .encode()?,
        )
    }

    #[mutation]
    pub fn call(
        ctx: &mut MutationCtx<'_, '_>,
        name: String,
        args: Value,
    ) -> flower_sdk::Result<Value> {
        ctx.call_mutation("$core", &name, &args)
    }

    /// Savepoint call: the failure comes back as a value.
    #[mutation]
    pub fn try_call(ctx: &mut MutationCtx<'_, '_>, name: String) -> flower_sdk::Result<String> {
        match ctx.try_mutation("$core", &name, &Value::Object(Default::default())) {
            Ok(CallOutcome::Success(_)) => Ok(String::from("ok")),
            Ok(CallOutcome::Failure(failure)) => Ok(failure.code),
            Err(flower_sdk::CallError::Host(failure)) => Err(failure),
        }
    }

    #[query]
    pub fn count(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Value> {
        ctx.call("$core", "count", &Value::Object(Default::default()))
    }

    pub static IMPORT: Import = Import {
        module: "$core",
        collections: &[ImportedCollection {
            name: "members",
            key: <String as FlowerType>::SCHEMA,
            row: <Member as FlowerType>::SCHEMA,
        }],
        derived: &[ImportedDerived {
            name: "member_count",
            contract: Some(Contract {
                args: flower_sdk::schema::v::object(&[]),
                result: <u64 as FlowerType>::SCHEMA,
                ..Contract::ANY
            }),
        }],
        methods: &[
            ImportedMethod {
                name: "count",
                kind: MethodKind::Query,
                contract: Some(Contract {
                    args: flower_sdk::schema::v::object(&[]),
                    result: <u64 as FlowerType>::SCHEMA,
                    ..Contract::ANY
                }),
            },
            ImportedMethod {
                name: "add_member",
                kind: MethodKind::Mutation,
                contract: None,
            },
        ],
    };
    static USES: [&dyn Component; 1] = [&IMPORT];
    static DEFINITIONS: [&dyn flower_sdk::Component; 6] =
        [&READ, &READ_COUNT, &WRITE, &CALL, &TRY_CALL, &COUNT];
    pub static APP: App = App::new(&[&USES, &DEFINITIONS]);

    /// The same consumer without imports: it may still name `$core` in
    /// code, but nothing binds it.
    pub static UNLINKED: App = App::new(&[&DEFINITIONS]);
}

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

/// The app's manifest, with deployment bindings when given.
fn manifest(app: &'static flower_sdk::App, bindings: &[(&str, &str)]) -> Value {
    let mut manifest = NativeRuntime::new(app).manifest().unwrap();
    if !bindings.is_empty()
        && let Value::Object(root) = &mut manifest
    {
        root.insert(
            "bindings".to_string(),
            obj(bindings
                .iter()
                .map(|(slot, module)| (*slot, s(module)))
                .collect()),
        );
    }
    manifest
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
        path.push(format!("flower-modules-{test}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        Fixture {
            engine: Engine::new(GroupStore::create(&path).unwrap()),
            path,
        }
    }

    fn deploy(
        &mut self,
        module: &str,
        app: &'static flower_sdk::App,
        bindings: &[(&str, &str)],
    ) -> Result<(), Failure> {
        let manifest = manifest(app, bindings);
        let identity = flower_crypto::hash::content_identity(
            module.as_bytes(),
            manifest.canonical_json().unwrap().as_bytes(),
        );
        self.engine.deploy(
            b"db",
            module,
            identity,
            b"",
            &manifest,
            Box::new(NativeRuntime::new(app)),
        )
    }

    fn query(&self, module: &str, name: &str, args: Value) -> Result<Value, Failure> {
        self.engine
            .invoke_query(b"db", module, name, &args, &CallOptions::default())
    }

    fn mutate(&mut self, module: &str, name: &str, args: Value) -> Result<Value, Failure> {
        self.engine
            .invoke_mutation(b"db", module, name, &args, &MutationOptions::default())
    }

    /// Provider and consumer deployed, provider seeded.
    fn linked(test: &str) -> Fixture {
        let mut fixture = Fixture::new(test);
        fixture.deploy("core", &core_app::APP, &[]).unwrap();
        fixture
            .deploy("crm", &crm_app::APP, &[("core", "core")])
            .unwrap();
        fixture.mutate("core", "seed", none()).unwrap();
        fixture
    }
}

fn none() -> Value {
    obj(vec![])
}

fn read_args(collection: &str, key: &str) -> Value {
    obj(vec![("collection", s(collection)), ("key", s(key))])
}

#[test]
fn native_runtime_runs_sdk_apps() {
    let mut fixture = Fixture::new("native");
    fixture.deploy("core", &core_app::APP, &[]).unwrap();
    fixture.mutate("core", "seed", none()).unwrap();
    assert_eq!(
        fixture.mutate("core", "add_member", obj(vec![("name", s("bob"))])),
        Ok(Value::Number(1.0))
    );
    assert_eq!(
        fixture.query("core", "count", none()),
        Ok(Value::Number(2.0))
    );
}

#[test]
fn exported_collections_read_and_private_ones_do_not() {
    let fixture = Fixture::linked("reads");
    let ada = fixture.query("crm", "read", read_args("members", "ada"));
    assert_eq!(ada, Ok(obj(vec![("name", s("ada"))])));
    let secret = fixture
        .query("crm", "read", read_args("secrets", "s"))
        .unwrap_err();
    assert_eq!(secret.code, codes::MODULE_PRIVATE);
    // Raw namespaces are never exported, whatever their spelling.
    let raw = fixture
        .query("crm", "read", read_args("$raw/members", "k"))
        .unwrap_err();
    assert_eq!(raw.code, codes::MODULE_PRIVATE);
}

#[test]
fn imported_collections_are_read_only() {
    let mut fixture = Fixture::linked("writes");
    let exported = fixture
        .mutate("crm", "write", obj(vec![("collection", s("members"))]))
        .unwrap_err();
    assert_eq!(exported.code, codes::MODULE_READ_ONLY);
    let private = fixture
        .mutate("crm", "write", obj(vec![("collection", s("secrets"))]))
        .unwrap_err();
    assert_eq!(private.code, codes::MODULE_PRIVATE);
    // Nothing reached the provider.
    assert_eq!(
        fixture.query("core", "count", none()),
        Ok(Value::Number(1.0))
    );
}

#[test]
fn exported_derived_values_compute_as_their_own_module() {
    // The count reads `""` members: the provider's, not the reader's
    // (which has no such collection at all).
    let fixture = Fixture::linked("derived");
    assert_eq!(
        fixture.query("crm", "read_count", none()),
        Ok(Value::Number(1.0))
    );
}

#[test]
fn calls_reach_exported_methods_for_allowed_callers_only() {
    let mut fixture = Fixture::linked("calls");
    let call = |name: &str, args: Value| obj(vec![("name", s(name)), ("args", args)]);
    assert_eq!(
        fixture.mutate(
            "crm",
            "call",
            call("add_member", obj(vec![("name", s("bob"))]))
        ),
        Ok(Value::Number(1.0))
    );
    let private = fixture
        .mutate("crm", "call", call("internal", none()))
        .unwrap_err();
    assert_eq!(private.code, codes::MODULE_PRIVATE);
    // A refusal recovers like an unknown method: the caller sees it as
    // a value through a savepoint.
    assert_eq!(
        fixture.mutate("crm", "try_call", obj(vec![("name", s("internal"))])),
        Ok(s(codes::MODULE_PRIVATE))
    );
    assert_eq!(
        fixture.query("crm", "count", none()),
        Ok(Value::Number(2.0))
    );

    // The same consumer binary as `crm2`: `add_member` lets only `crm`
    // call it, so the deployment that imports it is refused...
    let refused = fixture
        .deploy("crm2", &crm_app::APP, &[("core", "core")])
        .unwrap_err();
    assert_eq!(refused.code, codes::MODULE_CONFLICT);
    assert!(refused.message.contains("lets only"), "{}", refused.message);
    // ...and a module that doesn't import it can't call it either.
    fixture.deploy("crm2", &crm_app::UNLINKED, &[]).unwrap();
    let unbound = fixture
        .mutate(
            "crm2",
            "call",
            call("add_member", obj(vec![("name", s("eve"))])),
        )
        .unwrap_err();
    assert_eq!(unbound.code, codes::INVALID_VALUE);
    assert!(unbound.message.contains("not bound"), "{}", unbound.message);
}

#[test]
fn slots_must_be_bound_and_bindings_used() {
    let mut fixture = Fixture::new("slots");
    fixture.deploy("core", &core_app::APP, &[]).unwrap();
    let unbound = fixture.deploy("crm", &crm_app::APP, &[]).unwrap_err();
    assert!(unbound.message.contains("not bound"), "{}", unbound.message);
    let unused = fixture
        .deploy("crm", &crm_app::APP, &[("core", "core"), ("extra", "core")])
        .unwrap_err();
    assert!(
        unused.message.contains("names no imported slot"),
        "{}",
        unused.message
    );
    let missing = fixture
        .deploy("crm", &crm_app::APP, &[("core", "nowhere")])
        .unwrap_err();
    assert_eq!(missing.code, codes::MODULE_CONFLICT);
    assert!(
        missing.message.contains("not deployed"),
        "{}",
        missing.message
    );
    let slotted = fixture.deploy("$core", &core_app::APP, &[]).unwrap_err();
    assert!(
        slotted.message.contains("module identity"),
        "{}",
        slotted.message
    );
}

#[test]
fn slots_bind_the_same_binary_to_different_providers() {
    let mut fixture = Fixture::new("rebind");
    fixture.deploy("core", &core_app::APP, &[]).unwrap();
    fixture.deploy("core-b", &core_app::APP, &[]).unwrap();
    fixture.mutate("core", "seed", none()).unwrap();
    fixture
        .deploy("crm", &crm_app::APP, &[("core", "core")])
        .unwrap();
    assert_eq!(
        fixture.query("crm", "count", none()),
        Ok(Value::Number(1.0))
    );
    // Rebinding is configuration: the same binary now reads `core-b`.
    fixture
        .deploy("crm", &crm_app::APP, &[("core", "core-b")])
        .unwrap();
    assert_eq!(
        fixture.query("crm", "count", none()),
        Ok(Value::Number(0.0))
    );
}

#[test]
fn deployment_refuses_breaking_a_deployed_importer() {
    let mut fixture = Fixture::linked("reverse");
    // `count` answering text breaks crm's import of a number.
    let text = fixture
        .deploy("core", &core_app::TEXT_APP, &[])
        .unwrap_err();
    assert_eq!(text.code, codes::MODULE_CONFLICT);
    assert!(text.message.contains("`crm`"), "{}", text.message);
    assert!(text.message.contains("results"), "{}", text.message);
    // Exporting nothing breaks every import.
    let private = fixture
        .deploy("core", &core_app::PRIVATE_APP, &[])
        .unwrap_err();
    assert_eq!(private.code, codes::MODULE_CONFLICT);
    // The provider kept running as it was.
    assert_eq!(
        fixture.query("crm", "count", none()),
        Ok(Value::Number(1.0))
    );
    // A consumer importing the incompatible shape is refused up front.
    let mut fresh = Fixture::new("forward");
    fresh.deploy("core", &core_app::TEXT_APP, &[]).unwrap();
    let forward = fresh
        .deploy("crm", &crm_app::APP, &[("core", "core")])
        .unwrap_err();
    assert_eq!(forward.code, codes::MODULE_CONFLICT);
}

#[test]
fn a_broken_promise_fails_at_the_call_and_rolls_back() {
    let mut fixture = Fixture::linked("breach");
    let breach = fixture
        .mutate(
            "crm",
            "call",
            obj(vec![("name", s("liar")), ("args", Value::Null)]),
        )
        .unwrap_err();
    assert_eq!(breach.code, codes::MODULE_CONTRACT_VIOLATION);
    // Through a savepoint the breach is a value, and the callee's write
    // is taken back.
    assert_eq!(
        fixture.mutate("crm", "try_call", obj(vec![("name", s("liar"))])),
        Ok(s(codes::MODULE_CONTRACT_VIOLATION))
    );
    assert_eq!(
        fixture.query("crm", "read", read_args("members", "liar")),
        Ok(Value::Null)
    );
    // Within its own module the method answers as it likes.
    assert_eq!(
        fixture.mutate("core", "liar", Value::Null),
        Ok(s("not a number"))
    );
}

/// An interface crate's contents: shared types and the contract.
mod tasks_v1 {
    use flower_sdk::{DerivedCtx, FlowerError, FlowerType, MutationCtx, QueryCtx, interface};

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Task {
        pub title: String,
    }

    #[derive(FlowerError, Clone, Debug, PartialEq)]
    pub enum TaskError {
        #[flower(code = "TASK_EXISTS")]
        Exists,
        #[flower(code = "TASK_STORAGE")]
        Storage(String),
    }

    #[interface(name = "tasks.v1", handle = TasksApi)]
    pub trait Tasks {
        /// One task by ID.
        #[query]
        fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<Task>>;
        #[mutation]
        fn add_task(
            ctx: &mut MutationCtx<'_, '_>,
            id: String,
            title: String,
        ) -> Result<(), TaskError>;
        #[derived]
        fn task_count(ctx: &mut DerivedCtx<'_, '_>) -> flower_sdk::Result<u64>;
    }
}

/// The same interface after an incompatible change: titles became numbers.
mod tasks_v2 {
    use flower_sdk::{DerivedCtx, FlowerType, MutationCtx, QueryCtx, interface};

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Task {
        pub title: u32,
    }

    #[interface(name = "tasks.v2", handle = TasksApi)]
    pub trait Tasks {
        #[query]
        fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<Task>>;
        #[mutation]
        fn add_task(
            ctx: &mut MutationCtx<'_, '_>,
            id: String,
            title: u32,
        ) -> flower_sdk::Result<()>;
        #[derived]
        fn task_count(ctx: &mut DerivedCtx<'_, '_>) -> flower_sdk::Result<u64>;
    }
}

/// A provider implementing `tasks.v1` over its own collection.
mod tasks_provider {
    use flower_sdk::{App, CollectionRef, DerivedCtx, MutationCtx, QueryCtx, ReadCtx, collection};

    use super::tasks_v1::{Task, TaskError, Tasks, TasksProvider};

    #[collection(name = "rows", key = String, value = Task)]
    pub struct Rows;

    pub struct Impl;

    impl Tasks for Impl {
        fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<Task>> {
            Rows::get(ctx, &id)
        }

        fn add_task(
            ctx: &mut MutationCtx<'_, '_>,
            id: String,
            title: String,
        ) -> Result<(), TaskError> {
            let storage = |failure: flower_sdk::Failure| TaskError::Storage(failure.message);
            if Rows::get(ctx, &id).map_err(storage)?.is_some() {
                return Err(TaskError::Exists);
            }
            Rows::set(ctx, &id, &Task { title }).map_err(storage)
        }

        fn task_count(ctx: &mut DerivedCtx<'_, '_>) -> flower_sdk::Result<u64> {
            Ok(ctx
                .scan(CollectionRef::local("rows"), 100, None)?
                .rows
                .len() as u64)
        }
    }

    pub static APP: App = App::new(&[&Rows, &TasksProvider::<Impl>::COMPONENT]);
}

/// A provider of the incompatible `tasks.v2`.
mod tasks_provider_v2 {
    use flower_sdk::{App, DerivedCtx, MutationCtx, QueryCtx};

    use super::tasks_v2::{Task, Tasks, TasksProvider};

    pub struct Impl;

    impl Tasks for Impl {
        fn get_task(_ctx: &mut QueryCtx<'_, '_>, _id: String) -> flower_sdk::Result<Option<Task>> {
            Ok(None)
        }

        fn add_task(
            _ctx: &mut MutationCtx<'_, '_>,
            _id: String,
            _title: u32,
        ) -> flower_sdk::Result<()> {
            Ok(())
        }

        fn task_count(_ctx: &mut DerivedCtx<'_, '_>) -> flower_sdk::Result<u64> {
            Ok(0)
        }
    }

    pub static APP: App = App::new(&[&TasksProvider::<Impl>::COMPONENT]);
}

/// A consumer of `tasks.v1` through the `$tasks` slot, sharing no code
/// with the provider but the interface.
mod tasks_consumer {
    use flower_sdk::{App, CallOutcome, MutationCtx, QueryCtx, mutation, query};

    use super::tasks_v1::{Task, TaskError, TasksApi};

    static TASKS: TasksApi = TasksApi::import("$tasks");

    #[mutation]
    pub fn plan(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        title: String,
    ) -> flower_sdk::Result<String> {
        match TASKS.try_add_task(ctx, id, title) {
            Ok(CallOutcome::Success(())) => Ok(String::from("added")),
            Ok(CallOutcome::Failure(TaskError::Exists)) => Ok(String::from("exists")),
            Ok(CallOutcome::Failure(TaskError::Storage(message))) => Ok(message),
            Err(flower_sdk::CallError::Host(failure)) => Err(failure),
        }
    }

    #[query]
    pub fn show(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<Task>> {
        TASKS.get_task(ctx, id)
    }

    #[query]
    pub fn count(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<u64> {
        TASKS.task_count(ctx)
    }

    pub static APP: App = App::new(&[&TASKS, &PLAN, &SHOW, &COUNT]);
}

#[test]
fn interfaces_link_providers_and_consumers_through_one_contract() {
    let mut fixture = Fixture::new("interface");
    fixture
        .deploy("tasks-a", &tasks_provider::APP, &[])
        .unwrap();
    fixture
        .deploy("planner", &tasks_consumer::APP, &[("tasks", "tasks-a")])
        .unwrap();
    let plan = |title: &str| obj(vec![("id", s("t1")), ("title", s(title))]);
    assert_eq!(
        fixture.mutate("planner", "plan", plan("write")),
        Ok(s("added"))
    );
    // The declared failure decodes on the consumer's side.
    assert_eq!(
        fixture.mutate("planner", "plan", plan("again")),
        Ok(s("exists"))
    );
    assert_eq!(
        fixture.query("planner", "show", obj(vec![("id", s("t1"))])),
        Ok(obj(vec![("title", s("write"))]))
    );
    assert_eq!(
        fixture.query("planner", "count", none()),
        Ok(Value::Number(1.0))
    );
    // A provider of the incompatible next version doesn't link.
    fixture
        .deploy("tasks-b", &tasks_provider_v2::APP, &[])
        .unwrap();
    let refused = fixture
        .deploy("planner", &tasks_consumer::APP, &[("tasks", "tasks-b")])
        .unwrap_err();
    assert_eq!(refused.code, codes::MODULE_CONFLICT);
    // Nor may `tasks-a` turn into it under the consumer.
    let replaced = fixture
        .deploy("tasks-a", &tasks_provider_v2::APP, &[])
        .unwrap_err();
    assert_eq!(replaced.code, codes::MODULE_CONFLICT);
}
