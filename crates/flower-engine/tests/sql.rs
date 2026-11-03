//! SQL through the real SDK ABI, manifest loader, host, policy and derived graph.
extern crate alloc;

use flower_engine::{
    engine::{CallOptions, Engine, MutationOptions},
    runtime::NativeRuntime,
    storage::GroupStore,
};
use flower_sdk::{FlowerType, Principal, Value};

mod app {
    use flower_sdk::{
        ReadCtx, SqlBudget, SqlEndpoint, SqlQuery, Value,
        access::{FieldAccess, Operand, Policy, Rule},
        prelude::*,
        typed::DecimalI64,
    };

    #[derive(FlowerType, Clone)]
    pub struct Item {
        pub owner: String,
        pub visible: bool,
        pub name: String,
        pub money: DecimalI64,
    }
    fn policy() -> Policy {
        Policy::new()
            .read(Operand::row(&["visible"]).eq(Operand::literal(true)))
            .write(Rule::allow())
            .field(
                "money",
                FieldAccess::new()
                    .read(Operand::row(&["owner"]).eq(Operand::subject()))
                    .write(Rule::allow()),
            )
            .field(
                "owner",
                FieldAccess::new()
                    .read(Operand::row(&["owner"]).eq(Operand::subject()))
                    .write(Rule::allow()),
            )
    }
    #[collection(name = "items", key = String, value = Item, access = policy,
                 index(name = "by_money", fields(money)),
                 index(name = "by_owner", fields(owner)),
                 index(name = "by_name", fields(name)))]
    pub struct Items;
    #[collection(name = "audit", key = String, value = Item)]
    pub struct Audit;
    pub static AD_HOC: SqlEndpoint = SqlEndpoint::new(
        "items.sql",
        &[Items::NAME],
        SqlBudget::scan(100, 1_000_000, 1_000_000),
    );
    pub static AD_HOC_INDEX: SqlEndpoint = SqlEndpoint::new(
        "items.indexed",
        &[Items::NAME],
        SqlBudget::scan(100, 1_000_000, 1_000_000),
    )
    .require_index();
    pub static LOW_BIND: SqlEndpoint = SqlEndpoint::new(
        "items.low",
        &[Items::NAME],
        SqlBudget::scan(100, 1_000_000, 64),
    );
    #[derive(FlowerType)]
    pub struct Projection {
        pub id: String,
        pub name: Omittable<String>,
        pub money: Omittable<DecimalI64>,
    }
    #[derive(FlowerType)]
    pub struct Filter {
        pub min: DecimalI64,
        pub limit: u32,
    }
    #[derive(FlowerType)]
    pub struct OwnerParams {
        pub owner: Option<String>,
    }
    #[derive(FlowerType)]
    pub struct IdParams {
        pub id: String,
    }
    pub static OWNER: SqlQuery<OwnerParams, Projection> = SqlQuery::new(
        "owner",
        "SELECT _key AS id, name, money FROM items WHERE owner = :owner ORDER BY money LIMIT 100",
        SqlBudget::scan(3, 1_000_000, 1_000_000),
    )
    .require_index();
    pub static OWNER_SCAN: SqlQuery<OwnerParams, Projection> = SqlQuery::new(
        "owner_scan",
        "SELECT _key AS id, name, money FROM items WHERE owner = :owner OR 1 = 0 ORDER BY money LIMIT 100",
        SqlBudget::scan(100, 1_000_000, 1_000_000),
    );
    pub static ONE: SqlQuery<IdParams, Projection> = SqlQuery::new(
        "one",
        "SELECT _key AS id, name, money FROM items WHERE _key = :id LIMIT 1",
        SqlBudget::scan(1, 1_000_000, 1_000_000),
    )
    .require_index();
    pub static SMALL_OWNER: SqlQuery<OwnerParams, Projection> = SqlQuery::new(
        "owner_bytes",
        "SELECT _key AS id, name, money FROM items WHERE owner = :owner ORDER BY money LIMIT 100",
        SqlBudget::scan(3, 1_024, 1_000_000),
    )
    .require_index();
    const BUDGET: SqlBudget = SqlBudget::scan(100, 1_000_000, 1_000_000);
    pub static LIST: SqlQuery<(), Projection> = SqlQuery::new(
        "list",
        "SELECT _key AS id, name, money FROM items ORDER BY money, _key LIMIT 100",
        BUDGET,
    );
    pub static FILTERED: SqlQuery<Filter, Projection> = SqlQuery::new(
        "filtered",
        "SELECT _key AS id, name, money FROM items WHERE money >= :min ORDER BY money LIMIT :limit",
        BUDGET,
    );
    pub static TINY: SqlQuery<(), Projection> = SqlQuery::new(
        "tiny",
        "SELECT _key AS id, name, money FROM items LIMIT 1",
        SqlBudget::scan(2, 1_000_000, 1_000_000),
    );
    pub static BYTES: SqlQuery<(), Projection> = SqlQuery::new(
        "bytes",
        "SELECT _key AS id, name, money FROM items LIMIT 1",
        SqlBudget::scan(100, 10, 1_000_000),
    );
    pub static WORK: SqlQuery<(), Projection> = SqlQuery::new(
        "work",
        "SELECT _key AS id, name, money FROM items LIMIT 1",
        SqlBudget::scan(100, 1_000_000, 1),
    );

    #[mutation]
    pub fn put(ctx: &mut MutationCtx<'_, '_>, id: String, item: Item) -> Result<()> {
        Items::set(ctx, &id, &item)
    }
    #[mutation]
    pub fn put_and_read(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        item: Item,
    ) -> Result<Vec<Projection>> {
        Items::set(ctx, &id, &item)?;
        LIST.run(ctx, &())
    }
    #[mutation]
    pub fn edit_owner(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        item: Option<Item>,
        owner: Option<String>,
    ) -> Result<Vec<Projection>> {
        match item {
            Some(item) => Items::set(ctx, &id, &item)?,
            None => Items::delete(ctx, &id)?,
        }
        OWNER.run(ctx, &OwnerParams { owner })
    }
    #[mutation]
    pub fn edit_one(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        item: Option<Item>,
    ) -> Result<Vec<Projection>> {
        match item {
            Some(item) => Items::set(ctx, &id, &item)?,
            None => Items::delete(ctx, &id)?,
        }
        ONE.run(ctx, &IdParams { id })
    }
    #[mutation]
    pub fn remove(ctx: &mut MutationCtx<'_, '_>, id: String) -> Result<()> {
        Items::delete(ctx, &id)
    }
    #[query(name = "list")]
    pub fn list_method(ctx: &mut QueryCtx<'_, '_>) -> Result<Vec<Projection>> {
        LIST.run(ctx, &())
    }
    #[query(name = "filtered")]
    pub fn filtered_method(ctx: &mut QueryCtx<'_, '_>, filter: Filter) -> Result<Vec<Projection>> {
        FILTERED.run(ctx, &filter)
    }
    #[query(name = "tiny")]
    pub fn tiny_method(ctx: &mut QueryCtx<'_, '_>) -> Result<Vec<Projection>> {
        TINY.run(ctx, &())
    }
    #[query(name = "bytes")]
    pub fn bytes_method(ctx: &mut QueryCtx<'_, '_>) -> Result<Vec<Projection>> {
        BYTES.run(ctx, &())
    }
    #[query(name = "work")]
    pub fn work_method(ctx: &mut QueryCtx<'_, '_>) -> Result<Vec<Projection>> {
        WORK.run(ctx, &())
    }
    #[query]
    pub fn repeat_budget(ctx: &mut QueryCtx<'_, '_>) -> u32 {
        for _ in 0..10 {
            let _ = TINY.run(ctx, &());
        }
        10
    }
    #[query]
    pub fn explain(ctx: &mut QueryCtx<'_, '_>) -> Result<Value> {
        LIST.explain(ctx)
    }
    #[query]
    pub fn raw(ctx: &mut QueryCtx<'_, '_>, plan: String, params: Value) -> Result<Value> {
        ctx.host_op(
            flower_sdk::HostOp::Sql,
            Value::Array(vec![Value::from(plan), params, Value::Bool(false)]),
        )
    }
    #[query]
    pub fn dynamic(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Vec<Value>> {
        AD_HOC.run(ctx, &statement, &params)
    }
    #[query]
    pub fn dynamic_index(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Vec<Value>> {
        AD_HOC_INDEX.run(ctx, &statement, &params)
    }
    #[query]
    pub fn dynamic_explain(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Value> {
        AD_HOC.explain(ctx, &statement, &params)
    }
    #[query]
    pub fn dynamic_typed(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Vec<Projection>> {
        AD_HOC.run_as(ctx, &statement, &params)
    }
    #[query]
    pub fn dynamic_low(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Vec<Value>> {
        LOW_BIND.run(ctx, &statement, &params)
    }
    #[query]
    pub fn dynamic_spoof(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Vec<Value>> {
        SqlEndpoint::new(
            "items.sql",
            &[Audit::NAME],
            SqlBudget::scan(10_000, 64 * 1024 * 1024, 10_000_000),
        )
        .run(ctx, &statement, &params)
    }
    #[query]
    pub fn dynamic_undeclared(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Vec<Value>> {
        SqlEndpoint::new(
            "undeclared",
            &[Items::NAME],
            SqlBudget::scan(100, 1_000_000, 1_000_000),
        )
        .run(ctx, &statement, &params)
    }
    #[mutation]
    pub fn dynamic_overlay(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        item: Item,
        statement: String,
        params: Value,
    ) -> Result<Vec<Value>> {
        Items::set(ctx, &id, &item)?;
        AD_HOC.run(ctx, &statement, &params)
    }
    #[derived]
    pub fn dynamic_cache(
        ctx: &mut DerivedCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Vec<Value>> {
        AD_HOC.run(ctx, &statement, &params)
    }
    #[query]
    pub fn dynamic_derived(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Value> {
        ctx.get_derived(
            "",
            "dynamic_cache",
            &DynamicCacheArgs { statement, params }.encode()?,
        )
    }
    #[query]
    pub fn explain_plan(ctx: &mut QueryCtx<'_, '_>, plan: String) -> Result<Value> {
        ctx.host_op(
            flower_sdk::HostOp::Sql,
            Value::Array(vec![Value::from(plan), Value::Null, Value::Bool(true)]),
        )
    }
    #[query]
    pub fn native(ctx: &mut QueryCtx<'_, '_>) -> Result<Value> {
        let page = ctx.scan(flower_sdk::CollectionRef::local("items"), 100, None)?;
        Ok(Value::Array(
            page.rows
                .into_iter()
                .map(|row| {
                    let mut value = row.value.as_object().unwrap().clone();
                    value.remove("owner");
                    value.remove("visible");
                    value.insert("id".into(), row.key);
                    Value::Object(value)
                })
                .collect(),
        ))
    }
    std::thread_local! { pub static EVALUATIONS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) }; }
    #[derived(materialize = always)]
    pub fn cached(ctx: &mut DerivedCtx<'_, '_>) -> Result<Vec<Projection>> {
        EVALUATIONS.with(|n| n.set(n.get() + 1));
        LIST.run(ctx, &())
    }
    #[query]
    pub fn derived(ctx: &mut QueryCtx<'_, '_>) -> Result<Value> {
        ctx.get_derived("", "cached", &Value::Object(Default::default()))
    }
    #[derived]
    pub fn cached_tiny(ctx: &mut DerivedCtx<'_, '_>) -> Result<Vec<Projection>> {
        TINY.run(ctx, &())
    }
    #[query]
    pub fn derived_tiny(ctx: &mut QueryCtx<'_, '_>) -> Result<Value> {
        Ok(ctx
            .get_derived("", "cached_tiny", &Value::Object(Default::default()))
            .unwrap_or_else(|e| Value::String(e.code)))
    }
    std::thread_local! { pub static OWNER_EVALUATIONS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) }; }
    #[derived]
    pub fn cached_owner(
        ctx: &mut DerivedCtx<'_, '_>,
        owner: Option<String>,
    ) -> Result<Vec<Projection>> {
        OWNER_EVALUATIONS.with(|n| n.set(n.get() + 1));
        OWNER.run(ctx, &OwnerParams { owner })
    }
    #[query]
    pub fn derived_owner(ctx: &mut QueryCtx<'_, '_>, owner: Option<String>) -> Result<Value> {
        Ok(ctx
            .get_derived("", "cached_owner", &OwnerParams { owner }.encode()?)
            .unwrap_or_else(|e| Value::String(e.code)))
    }
    #[derived]
    pub fn cached_one(ctx: &mut DerivedCtx<'_, '_>, id: String) -> Result<Vec<Projection>> {
        ONE.run(ctx, &IdParams { id })
    }
    #[query]
    pub fn derived_one(ctx: &mut QueryCtx<'_, '_>, id: String) -> Result<Value> {
        ctx.get_derived("", "cached_one", &IdParams { id }.encode()?)
    }
    #[derived]
    pub fn cached_small(
        ctx: &mut DerivedCtx<'_, '_>,
        owner: Option<String>,
    ) -> Result<Vec<Projection>> {
        SMALL_OWNER.run(ctx, &OwnerParams { owner })
    }
    #[query]
    pub fn derived_small(ctx: &mut QueryCtx<'_, '_>, owner: Option<String>) -> Result<Value> {
        Ok(ctx
            .get_derived("", "cached_small", &OwnerParams { owner }.encode()?)
            .unwrap_or_else(|e| Value::String(e.code)))
    }
    pub static APP: App = App::new(&[
        &Audit,
        &AD_HOC,
        &AD_HOC_INDEX,
        &LOW_BIND,
        &DYNAMIC,
        &DYNAMIC_INDEX,
        &DYNAMIC_EXPLAIN,
        &DYNAMIC_TYPED,
        &DYNAMIC_LOW,
        &DYNAMIC_SPOOF,
        &DYNAMIC_UNDECLARED,
        &DYNAMIC_OVERLAY,
        &DYNAMIC_CACHE,
        &DYNAMIC_DERIVED,
        &Items,
        &LIST,
        &FILTERED,
        &TINY,
        &BYTES,
        &WORK,
        &PUT,
        &PUT_AND_READ,
        &REMOVE,
        &LIST_METHOD,
        &FILTERED_METHOD,
        &TINY_METHOD,
        &BYTES_METHOD,
        &WORK_METHOD,
        &REPEAT_BUDGET,
        &EXPLAIN,
        &RAW,
        &NATIVE,
        &CACHED,
        &DERIVED,
        &CACHED_TINY,
        &DERIVED_TINY,
        &OWNER,
        &OWNER_SCAN,
        &ONE,
        &EDIT_OWNER,
        &EDIT_ONE,
        &EXPLAIN_PLAN,
        &CACHED_OWNER,
        &DERIVED_OWNER,
        &SMALL_OWNER,
        &CACHED_ONE,
        &DERIVED_ONE,
        &CACHED_SMALL,
        &DERIVED_SMALL,
    ]);
}

struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new(name: &str) -> Self {
        Self::with_manifest(name, NativeRuntime::new(&app::APP).manifest().unwrap())
    }
    fn with_manifest(name: &str, manifest: Value) -> Self {
        let path = std::env::temp_dir().join(format!("flower-sql-{name}-{}", std::process::id()));
        let mut engine = Engine::new(GroupStore::create(&path).unwrap());
        engine
            .deploy(
                b"db",
                "sql",
                [91; 32],
                b"",
                &manifest,
                Box::new(NativeRuntime::new(&app::APP)),
            )
            .unwrap();
        Self { engine, path }
    }
    fn who() -> CallOptions {
        CallOptions {
            principal: Some(Principal::new("ada".into(), None, None).unwrap()),
            ..Default::default()
        }
    }
    fn query(&self, name: &str, args: Value) -> flower_sdk::Result<Value> {
        self.engine
            .invoke_query(b"db", "sql", name, &args, &Self::who())
    }
    fn mutate(&mut self, name: &str, args: Value) -> flower_sdk::Result<Value> {
        self.engine.invoke_mutation(
            b"db",
            "sql",
            name,
            &args,
            &MutationOptions {
                call: Self::who(),
                ..Default::default()
            },
        )
    }
    fn put(&mut self, id: &str, owner: &str, visible: bool, money: i64) {
        let args = app::PutArgs {
            id: id.into(),
            item: app::Item {
                owner: owner.into(),
                visible,
                name: id.into(),
                money: flower_sdk::typed::DecimalI64(money),
            },
        }
        .encode()
        .unwrap();
        self.mutate("put", args).unwrap();
    }
    fn sql(&self, plan: &str, params: Value) -> flower_sdk::Result<Value> {
        self.query(
            "raw",
            app::RawArgs {
                plan: plan.into(),
                params,
            }
            .encode()
            .unwrap(),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
fn empty() -> Value {
    Value::Object(Default::default())
}
fn ids(value: &Value) -> Vec<&str> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_object().unwrap()["id"].as_str().unwrap())
        .collect()
}

fn owner_params(owner: Option<&str>) -> Value {
    app::OwnerParams {
        owner: owner.map(Into::into),
    }
    .encode()
    .unwrap()
}

fn dynamic_args(statement: &str, params: Value) -> Value {
    app::DynamicArgs {
        statement: statement.into(),
        params,
    }
    .encode()
    .unwrap()
}

#[test]
fn arbitrary_statements_change_projection_without_redeployment() {
    let mut f = Fixture::new("dynamic-shapes");
    f.put("a", "ada", true, 9_007_199_254_740_992);
    f.put("b", "ada", true, 9_007_199_254_740_993);
    let params = Value::Object([("min".into(), Value::from("9007199254740992"))].into());
    let result = f.query("dynamic",dynamic_args("SELECT _key AS id, money FROM items WHERE money >= :min ORDER BY money DESC LIMIT 1",params)).unwrap();
    assert_eq!(ids(&result), ["b"]);
    let result = f
        .query(
            "dynamic",
            dynamic_args(
                "SELECT name AS title FROM items WHERE _key = 'a' LIMIT 1",
                Value::Null,
            ),
        )
        .unwrap();
    assert_eq!(
        result.as_array().unwrap()[0].as_object().unwrap()["title"],
        Value::from("a")
    );
}

#[test]
fn arbitrary_sql_keeps_named_policy_semantics_and_optional_typed_decoding() {
    let mut f = Fixture::new("dynamic-policy");
    f.put("ours", "ada", true, 9);
    f.put("hidden", "ada", false, 8);
    f.put("theirs", "bob", true, 7);
    let sql = "SELECT _key AS id, name, money FROM items ORDER BY money, _key LIMIT 100";
    let named = f.query("list", empty()).unwrap();
    assert_eq!(
        f.query("dynamic", dynamic_args(sql, Value::Null)).unwrap(),
        named
    );
    assert_eq!(
        f.query("dynamic_typed", dynamic_args(sql, Value::Null))
            .unwrap(),
        named
    );
    let result = f
        .query(
            "dynamic",
            dynamic_args(
                "SELECT _key AS id FROM items WHERE owner = :owner LIMIT 100",
                owner_params(Some("bob")),
            ),
        )
        .unwrap();
    assert!(result.as_array().unwrap().is_empty());
}

#[test]
fn arbitrary_sql_scope_and_limits_come_from_the_deployed_endpoint() {
    let f = Fixture::new("dynamic-capability");
    for query in ["dynamic", "dynamic_spoof"] {
        assert_eq!(
            f.query(
                query,
                dynamic_args("SELECT _key AS id FROM audit LIMIT 1", Value::Null)
            )
            .unwrap_err()
            .code,
            "ACCESS_DENIED"
        );
    }
    assert_eq!(
        f.query(
            "dynamic_undeclared",
            dynamic_args("SELECT _key AS id FROM items LIMIT 1", Value::Null)
        )
        .unwrap_err()
        .code,
        flower_sql::SQL_INVALID
    );
    assert_eq!(
        f.query(
            "dynamic_index",
            dynamic_args("SELECT _key AS id FROM items LIMIT 1", Value::Null)
        )
        .unwrap_err()
        .code,
        flower_sql::SQL_INVALID
    );
    assert_eq!(
        f.query(
            "dynamic_low",
            dynamic_args(
                "SELECT _key AS id FROM items WHERE owner = 'ada' LIMIT 1",
                Value::Null
            )
        )
        .unwrap_err()
        .code,
        flower_sql::SQL_BUDGET
    );
    let injected = Value::Object([("owner".into(), Value::from("ada' OR 1 = 1"))].into());
    assert!(
        f.query(
            "dynamic",
            dynamic_args(
                "SELECT owner FROM items WHERE owner = :owner LIMIT 1",
                injected
            )
        )
        .unwrap()
        .as_array()
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        f.query(
            "dynamic",
            dynamic_args("DELETE FROM items LIMIT 1", Value::Null)
        )
        .unwrap_err()
        .code,
        flower_sql::SQL_INVALID
    );
    let large = Value::Object([("value".into(), Value::from("x".repeat(1_000_000)))].into());
    assert_eq!(
        f.query(
            "dynamic_spoof",
            dynamic_args("SELECT :value AS value FROM items LIMIT 1", large)
        )
        .unwrap_err()
        .code,
        flower_sql::SQL_BUDGET
    );
}

#[test]
fn endpoint_manifests_validate_scopes_uniqueness_and_ceiling_types() {
    let original = NativeRuntime::new(&app::APP).manifest().unwrap();
    for edit in [
        "duplicate",
        "unknown-source",
        "empty-scope",
        "duplicate-source",
        "bad-budget",
        "bad-scan",
    ] {
        let mut manifest = original.clone();
        let Value::Object(root) = &mut manifest else {
            panic!()
        };
        let Some(Value::Array(endpoints)) = root.get_mut("sql_endpoints") else {
            panic!()
        };
        if edit == "duplicate" {
            endpoints.push(endpoints[0].clone());
        } else {
            let Value::Object(endpoint) = &mut endpoints[0] else {
                panic!()
            };
            match edit {
                "unknown-source" => {
                    endpoint.insert(
                        "collections".into(),
                        Value::Array(vec![Value::from("not_deployed")]),
                    );
                }
                "empty-scope" => {
                    endpoint.insert("collections".into(), Value::Array(Vec::new()));
                }
                "duplicate-source" => {
                    endpoint.insert(
                        "collections".into(),
                        Value::Array(vec![Value::from("items"), Value::from("items")]),
                    );
                }
                "bad-budget" => {
                    endpoint.insert("work".into(), Value::Number(0.0));
                }
                "bad-scan" => {
                    endpoint.insert("allow_scan".into(), Value::Null);
                }
                _ => unreachable!(),
            }
        }
        assert!(
            flower_engine::descriptors::load_module(&manifest).is_err(),
            "{edit}"
        );
    }
}

#[test]
fn arbitrary_explain_uses_inferred_types_and_fixed_endpoint_policy() {
    let f = Fixture::new("dynamic-explain");
    let result = f
        .query(
            "dynamic_explain",
            dynamic_args(
                "SELECT money FROM items WHERE owner = :owner LIMIT 10",
                owner_params(Some("ada")),
            ),
        )
        .unwrap();
    let info = result.as_object().unwrap();
    assert_eq!(info["access"], Value::from("index_equality"));
    assert_eq!(info["endpoint_max_work"], Value::Number(1_000_000.0));
    assert!(matches!(&info["binding_work"],Value::Number(n) if *n > 0.0));
    assert!(!result.canonical_json().unwrap().contains("ada"));
}

#[test]
fn arbitrary_sql_reads_staged_writes_and_retains_derived_dependencies() {
    let mut f = Fixture::new("dynamic-overlay-deps");
    let sql = "SELECT _key AS id, money FROM items WHERE owner = :owner ORDER BY money LIMIT 10";
    let params = owner_params(Some("ada"));
    let result = f
        .mutate(
            "dynamic_overlay",
            app::DynamicOverlayArgs {
                id: "a".into(),
                item: app::Item {
                    owner: "ada".into(),
                    visible: true,
                    name: "a".into(),
                    money: flower_sdk::typed::DecimalI64(1),
                },
                statement: sql.into(),
                params: params.clone(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    assert_eq!(ids(&result), ["a"]);
    assert_eq!(
        f.query("dynamic_derived", dynamic_args(sql, params.clone()))
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    f.put("b", "ada", true, 2);
    assert_eq!(
        f.query("dynamic_derived", dynamic_args(sql, params))
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn indexed_and_primary_reads_work_beyond_the_collection_scan_cap() {
    let mut f = Fixture::new("indexed-cap");
    f.put("selected", "ada", true, 10);
    for i in 0..20 {
        f.put(&format!("other-{i}"), "other", true, i);
    }
    let selected = f.sql("owner", owner_params(Some("ada"))).unwrap();
    assert_eq!(ids(&selected), ["selected"]);
    assert_eq!(
        selected,
        f.sql("owner_scan", owner_params(Some("ada"))).unwrap()
    );
    assert_eq!(
        selected,
        f.sql(
            "one",
            app::IdParams {
                id: "selected".into()
            }
            .encode()
            .unwrap()
        )
        .unwrap()
    );
    assert_eq!(
        f.query("tiny", empty()).unwrap_err().code,
        flower_sql::SQL_BUDGET
    );
    for (name, path, bound) in [
        ("owner", "index_equality", false),
        ("one", "primary_key_lookup", true),
    ] {
        let explain = f
            .query(
                "explain_plan",
                app::ExplainPlanArgs { plan: name.into() }.encode().unwrap(),
            )
            .unwrap();
        assert_eq!(explain.as_object().unwrap()["access"], Value::from(path));
        assert_eq!(
            explain.as_object().unwrap()["input_bound_proven"],
            Value::Bool(bound)
        );
    }
}

#[test]
fn index_candidates_obey_row_visibility_field_redaction_and_null_logic() {
    let mut f = Fixture::new("index-policy");
    f.put("ours", "ada", true, 9);
    f.put("hidden", "ada", false, 10);
    f.put("theirs", "bob", true, 100);
    for owner in [Some("ada"), Some("bob"), None] {
        assert_eq!(
            f.sql("owner", owner_params(owner)).unwrap(),
            f.sql("owner_scan", owner_params(owner)).unwrap()
        );
    }
    assert!(
        f.sql("owner", owner_params(Some("bob")))
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    let theirs = f
        .sql(
            "one",
            app::IdParams {
                id: "theirs".into(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    assert_eq!(ids(&theirs), ["theirs"]);
    assert!(
        !theirs.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .contains_key("money")
    );
    assert!(
        f.sql(
            "one",
            app::IdParams {
                id: "hidden".into()
            }
            .encode()
            .unwrap()
        )
        .unwrap()
        .as_array()
        .unwrap()
        .is_empty()
    );
}

#[test]
fn indexed_mutation_reads_merge_bucket_moves_updates_and_deletions() {
    let mut f = Fixture::new("index-overlay");
    f.put("move", "bob", true, 10);
    let item = |owner: &str, money| {
        Some(app::Item {
            owner: owner.into(),
            visible: true,
            name: "move".into(),
            money: flower_sdk::typed::DecimalI64(money),
        })
    };
    let edit = |item, owner: &str| {
        app::EditOwnerArgs {
            id: "move".into(),
            item,
            owner: Some(owner.into()),
        }
        .encode()
        .unwrap()
    };
    assert_eq!(
        ids(&f.mutate("edit_owner", edit(item("ada", 9), "ada")).unwrap()),
        ["move"]
    );
    assert_eq!(
        f.mutate("edit_owner", edit(item("ada", 8), "ada"))
            .unwrap()
            .as_array()
            .unwrap()[0]
            .as_object()
            .unwrap()["money"],
        Value::from("8")
    );
    assert!(
        f.mutate("edit_owner", edit(item("bob", 7), "ada"))
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        f.mutate("edit_owner", edit(None, "ada"))
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    let edit_one = |item| {
        app::EditOneArgs {
            id: "point".into(),
            item,
        }
        .encode()
        .unwrap()
    };
    assert_eq!(
        ids(&f.mutate("edit_one", edit_one(item("ada", 1))).unwrap()),
        ["point"]
    );
    assert_eq!(
        ids(&f.mutate("edit_one", edit_one(item("ada", 2))).unwrap()),
        ["point"]
    );
    assert!(
        f.mutate("edit_one", edit_one(None))
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn index_dependencies_ignore_other_buckets_but_observe_row_edits_and_new_members() {
    let mut f = Fixture::new("index-deps");
    app::OWNER_EVALUATIONS.with(|n| n.set(0));
    f.put("a", "ada", true, 1);
    let query = |f: &Fixture| f.query("derived_owner", owner_params(Some("ada"))).unwrap();
    assert_eq!(ids(&query(&f)), ["a"]);
    assert_eq!(app::OWNER_EVALUATIONS.with(std::cell::Cell::get), 1);
    f.put("b", "bob", true, 2);
    assert_eq!(ids(&query(&f)), ["a"]);
    assert_eq!(app::OWNER_EVALUATIONS.with(std::cell::Cell::get), 1);
    f.put("a", "ada", true, 9); // same index entry, changed projected field
    assert_eq!(
        query(&f).as_array().unwrap()[0].as_object().unwrap()["money"],
        Value::from("9")
    );
    assert_eq!(app::OWNER_EVALUATIONS.with(std::cell::Cell::get), 2);
    f.put("b", "ada", true, 2); // joins the observed equality bucket
    assert_eq!(ids(&query(&f)), ["b", "a"]);
    assert_eq!(app::OWNER_EVALUATIONS.with(std::cell::Cell::get), 3);
    f.mutate(
        "remove",
        app::RemoveArgs { id: "a".into() }.encode().unwrap(),
    )
    .unwrap();
    assert_eq!(ids(&query(&f)), ["b"]);
    assert_eq!(app::OWNER_EVALUATIONS.with(std::cell::Cell::get), 4);
}

#[test]
fn nonunique_bucket_exhaustion_is_cached_and_repairs_when_a_member_leaves() {
    let mut f = Fixture::new("bucket-budget");
    for id in ["a", "b", "c", "d"] {
        f.put(id, "ada", false, 1);
    }
    assert_eq!(
        f.sql("owner", owner_params(Some("ada"))).unwrap_err().code,
        flower_sql::SQL_BUDGET
    );
    assert_eq!(
        f.query("derived_owner", owner_params(Some("ada"))).unwrap(),
        Value::from(flower_sql::SQL_BUDGET)
    );
    f.mutate(
        "remove",
        app::RemoveArgs { id: "d".into() }.encode().unwrap(),
    )
    .unwrap();
    assert_eq!(
        f.query("derived_owner", owner_params(Some("ada")))
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert!(
        f.sql("owner", owner_params(Some("ada")))
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn absent_point_dependencies_observe_insertion_and_deletion() {
    let mut f = Fixture::new("point-deps");
    let params = app::IdParams { id: "new".into() }.encode().unwrap();
    assert!(
        f.query("derived_one", params.clone())
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    f.put("new", "ada", true, 1);
    assert_eq!(
        ids(&f.query("derived_one", params.clone()).unwrap()),
        ["new"]
    );
    f.mutate(
        "remove",
        app::RemoveArgs { id: "new".into() }.encode().unwrap(),
    )
    .unwrap();
    assert!(
        f.query("derived_one", params)
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn byte_exhaustion_on_an_indexed_row_repairs_after_a_nonindexed_edit() {
    let mut f = Fixture::new("index-byte-deps");
    let args = app::PutArgs {
        id: "big".into(),
        item: app::Item {
            owner: "ada".into(),
            visible: true,
            name: "x".repeat(5_000),
            money: flower_sdk::typed::DecimalI64(1),
        },
    }
    .encode()
    .unwrap();
    f.mutate("put", args).unwrap();
    assert_eq!(
        f.query("derived_small", owner_params(Some("ada"))).unwrap(),
        Value::from(flower_sql::SQL_BUDGET)
    );
    f.put("big", "ada", true, 1); // changes name, not the owner index entry
    assert_eq!(
        ids(&f.query("derived_small", owner_params(Some("ada"))).unwrap()),
        ["big"]
    );
}

#[test]
fn newly_deployed_indexes_backfill_existing_rows_before_plans_use_them() {
    let manifest = NativeRuntime::new(&app::APP).manifest().unwrap();
    let mut old = manifest.clone();
    let Value::Object(root) = &mut old else {
        panic!()
    };
    let Some(Value::Array(collections)) = root.get_mut("collections") else {
        panic!()
    };
    for collection in collections {
        let Value::Object(collection) = collection else {
            panic!()
        };
        collection.insert("indexes".into(), Value::Array(Vec::new()));
    }
    let Some(Value::Array(plans)) = root.get_mut("sql") else {
        panic!()
    };
    plans.retain(|p| {
        !matches!(
            p.as_object().unwrap()["name"].as_str(),
            Some("owner" | "owner_bytes")
        )
    });
    let mut f = Fixture::with_manifest("backfill", old);
    f.put("a", "ada", true, 1);
    f.put("b", "ada", true, 2);
    f.engine
        .deploy(
            b"db",
            "sql",
            [94; 32],
            b"",
            &manifest,
            Box::new(NativeRuntime::new(&app::APP)),
        )
        .unwrap();
    assert_eq!(
        ids(&f.sql("owner", owner_params(Some("ada"))).unwrap()),
        ["a", "b"]
    );
}

#[test]
fn patches_prepared_with_indexed_sql_reads_apply_identically_on_a_replica() {
    let mut leader = Fixture::new("patch-leader");
    let mut replica = Fixture::new("patch-replica");
    let item = |owner: &str, money| {
        Some(app::Item {
            owner: owner.into(),
            visible: true,
            name: "value".into(),
            money: flower_sdk::typed::DecimalI64(money),
        })
    };
    let calls = [
        (
            "edit_one",
            app::EditOneArgs {
                id: "a".into(),
                item: item("ada", 9),
            }
            .encode()
            .unwrap(),
        ),
        (
            "edit_one",
            app::EditOneArgs {
                id: "b".into(),
                item: item("bob", 2),
            }
            .encode()
            .unwrap(),
        ),
        (
            "edit_owner",
            app::EditOwnerArgs {
                id: "b".into(),
                item: item("ada", 2),
                owner: Some("ada".into()),
            }
            .encode()
            .unwrap(),
        ),
    ];
    for (i, (name, args)) in calls.into_iter().enumerate() {
        let prepared = leader
            .engine
            .prepare_mutation(
                b"db",
                "sql",
                name,
                &args,
                &MutationOptions {
                    call: Fixture::who(),
                    ..Default::default()
                },
            )
            .unwrap();
        let flower_engine::engine::PrepareOutcome::Patch(prepared) = prepared else {
            panic!()
        };
        let own = leader
            .engine
            .apply_patch(&prepared.patch, 100 + i as u64)
            .unwrap()
            .result
            .unwrap();
        let applied = replica
            .engine
            .apply_patch(&prepared.patch, 100 + i as u64)
            .unwrap()
            .result
            .unwrap();
        assert_eq!(own, applied);
        assert_eq!(
            leader.sql("owner", owner_params(Some("ada"))).unwrap(),
            replica.sql("owner", owner_params(Some("ada"))).unwrap()
        );
    }
}

#[test]
fn require_index_is_revalidated_when_a_catalog_index_is_removed() {
    let mut manifest = NativeRuntime::new(&app::APP).manifest().unwrap();
    let Value::Object(root) = &mut manifest else {
        panic!()
    };
    let Some(Value::Array(collections)) = root.get_mut("collections") else {
        panic!()
    };
    let collection = collections
        .iter_mut()
        .find(|c| c.as_object().unwrap()["name"] == Value::from("items"))
        .unwrap();
    let Value::Object(collection) = collection else {
        panic!()
    };
    let Some(Value::Array(indexes)) = collection.get_mut("indexes") else {
        panic!()
    };
    indexes.retain(|i| i.as_object().unwrap()["name"] != Value::from("by_owner"));
    let error = flower_engine::descriptors::load_module(&manifest).unwrap_err();
    assert!(error.to_string().contains("requires an index"));
}

#[test]
fn caught_input_budget_failures_still_exhaust_shared_invocation_fuel() {
    let mut f = Fixture::new("failed-input-fuel");
    assert_eq!(
        f.query("repeat_budget", empty()).unwrap(),
        Value::Number(10.0)
    );
    for id in ["a", "b", "c"] {
        f.put(id, "ada", false, 1);
    }
    assert_eq!(
        f.query("repeat_budget", empty()).unwrap_err().code,
        "budget_exhausted"
    );
}

#[test]
fn policy_equivalence_with_native_scan_and_exact_value_order() {
    let mut f = Fixture::new("policy");
    f.put("a", "ada", true, 10);
    f.put("z", "ada", true, 9);
    f.put("redacted", "bob", true, 100);
    f.put("hidden", "ada", false, 0);
    let result = f.query("list", empty()).unwrap();
    assert_eq!(ids(&result), ["redacted", "z", "a"]);
    assert!(
        !result.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .contains_key("money")
    );
    let mut sql = result
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.canonical_json().unwrap())
        .collect::<Vec<_>>();
    let mut native = f
        .query("native", empty())
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.canonical_json().unwrap())
        .collect::<Vec<_>>();
    sql.sort();
    native.sort();
    assert_eq!(sql, native);
    let args = app::FilteredMethodArgs {
        filter: app::Filter {
            min: flower_sdk::typed::DecimalI64(10),
            limit: 1,
        },
    }
    .encode()
    .unwrap();
    assert_eq!(ids(&f.query("filtered", args).unwrap()), ["a"]);
}

#[test]
fn hidden_rows_consume_candidate_budget_and_limit_does_not_bound_work() {
    let mut f = Fixture::new("candidate-cap");
    for id in ["a", "b", "c"] {
        f.put(id, "ada", false, 1);
    }
    assert_eq!(
        f.query("tiny", empty()).unwrap_err().code,
        flower_sql::SQL_BUDGET
    );
    assert_eq!(
        f.query("derived_tiny", empty()).unwrap(),
        Value::from(flower_sql::SQL_BUDGET)
    );
    f.mutate(
        "remove",
        app::RemoveArgs { id: "c".into() }.encode().unwrap(),
    )
    .unwrap();
    assert!(
        f.query("derived_tiny", empty())
            .unwrap()
            .as_array()
            .is_some()
    );
    assert!(
        f.query("list", empty())
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        f.query("bytes", empty()).unwrap_err().code,
        flower_sql::SQL_BUDGET
    );
    f.put("visible", "ada", true, 1);
    assert_eq!(
        f.query("work", empty()).unwrap_err().code,
        flower_sql::SQL_BUDGET
    );
    assert_eq!(
        f.query("explain", empty()).unwrap().as_object().unwrap()["input_bound_proven"],
        Value::Bool(false)
    );
}

#[test]
fn staged_writes_are_visible_and_collection_changes_invalidate_derived_reads() {
    let mut f = Fixture::new("overlay-dependencies");
    let args = app::PutAndReadArgs {
        id: "new".into(),
        item: app::Item {
            owner: "ada".into(),
            visible: true,
            name: "new".into(),
            money: flower_sdk::typed::DecimalI64(1),
        },
    }
    .encode()
    .unwrap();
    assert_eq!(ids(&f.mutate("put_and_read", args).unwrap()), ["new"]);
    let first = f.query("derived", empty()).unwrap();
    assert_eq!(f.query("derived", empty()).unwrap(), first);
    assert_eq!(app::EVALUATIONS.with(std::cell::Cell::get), 1);
    f.put("another", "ada", true, 2);
    assert_eq!(
        f.query("derived", empty())
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(app::EVALUATIONS.with(std::cell::Cell::get), 2);
    f.mutate(
        "remove",
        app::RemoveArgs { id: "new".into() }.encode().unwrap(),
    )
    .unwrap();
    assert_eq!(
        f.query("derived", empty())
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn undeclared_plans_and_invalid_parameters_fail_before_execution() {
    let f = Fixture::new("parameter-checks");
    let raw = |plan: &str, params| {
        app::RawArgs {
            plan: plan.into(),
            params,
        }
        .encode()
        .unwrap()
    };
    assert_eq!(
        f.query("raw", raw("undeclared", Value::Null))
            .unwrap_err()
            .code,
        flower_sql::SQL_INVALID
    );
    assert_eq!(
        f.query("raw", raw("filtered", empty())).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    let mut params = std::collections::BTreeMap::new();
    params.insert("min".into(), Value::from("9223372036854775808"));
    params.insert("limit".into(), Value::Number(1.0));
    assert_eq!(
        f.query("raw", raw("filtered", Value::Object(params)))
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
}

#[test]
fn manifest_rebinding_rejects_schema_drift_duplicates_and_invalid_source() {
    let original = NativeRuntime::new(&app::APP).manifest().unwrap();
    for edit in ["unknown-field", "unknown-source", "duplicate", "bad-budget"] {
        let mut manifest = original.clone();
        let Value::Object(root) = &mut manifest else {
            panic!()
        };
        let Some(Value::Array(plans)) = root.get_mut("sql") else {
            panic!()
        };
        match edit {
            "duplicate" => plans.push(plans[0].clone()),
            "bad-budget" => {
                let Value::Object(plan) = &mut plans[0] else {
                    panic!()
                };
                plan.insert("candidates".into(), Value::Number(0.0));
            }
            _ => {
                let Value::Object(plan) = &mut plans[0] else {
                    panic!()
                };
                plan.insert(
                    "statement".into(),
                    Value::from(if edit == "unknown-source" {
                        "SELECT _key AS id, name, money FROM missing LIMIT 1"
                    } else {
                        "SELECT _key AS id, missing AS name, money FROM items LIMIT 1"
                    }),
                );
            }
        }
        assert!(
            flower_engine::descriptors::load_module(&manifest).is_err(),
            "{edit}"
        );
    }
}
