//! Deployed application descriptors.
//!
//! The engine resolves names, validates writes and routes calls from owned
//! descriptor mirrors loaded out of the guest manifest. SDK [`Schema`]s are
//! `const`-oriented with `&'static` interiors, so the loader intentionally
//! leaks the small owned backing stores once per deployment; names stay
//! owned and references resolve by name at use time.
//!
//! [`Schema`]: flower_sdk::schema::Schema

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use flower_sdk::Value;
use flower_sdk::schema::{Field as SchemaField, Schema, v};

use crate::access::{CollectionPolicy, Rule};
use crate::error::{Error, Result};

/// Leak a string for a `'static` schema interior. Called once per
/// deployment per name; the few leaked bytes live as long as the process.
fn leak_str(text: &str) -> &'static str {
    Box::leak(text.to_owned().into_boxed_str())
}

/// Leak one schema for a `'static` interior reference.
fn leak_schema(schema: Schema) -> &'static Schema {
    Box::leak(Box::new(schema))
}

/// An owned index descriptor: name plus wire fields in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDescriptor {
    /// Index name.
    pub name: String,
    /// Indexed wire fields in order.
    pub fields: Vec<String>,
}

/// Where an owned foreign key lives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlaceDescriptor {
    /// Named value fields.
    Fields(Vec<String>),
    /// The whole key.
    Key,
    /// A key prefix.
    KeyPrefix,
}

/// An owned foreign-key descriptor. One target means a single reference;
/// several with a discriminator mean a polymorphic reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceDescriptor {
    /// Target collection names.
    pub targets: Vec<String>,
    /// Discriminator field holding a target name (poly only).
    pub discriminator: Option<String>,
    /// Key placement.
    pub place: PlaceDescriptor,
    /// Delete behavior.
    pub on_delete: OnDelete,
}

/// Foreign-key delete behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnDelete {
    /// Block deletes of referenced rows.
    Restrict,
    /// Delete referencing rows.
    Cascade,
    /// Clear referencing fields.
    SetNull,
}

/// An owned collection descriptor.
#[derive(Clone, Debug, PartialEq)]
pub struct CollectionDef {
    /// Collection name.
    pub name: String,
    /// Key contract.
    pub key_schema: Schema,
    /// Value contract.
    pub value_schema: Schema,
    /// Index descriptors.
    pub indexes: Vec<IndexDescriptor>,
    /// Foreign keys out of this collection.
    pub references: Vec<ReferenceDescriptor>,
    /// Access policy guarding callers, if declared.
    pub access: Option<Arc<CollectionPolicy>>,
}

/// A definition kind for call routing and capability checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefKind {
    /// Query definition.
    Query,
    /// Mutation definition.
    Mutation,
    /// Derived definition.
    Derived,
    /// Transaction planner.
    Transaction,
}

/// Most staleness a query may declare, in milliseconds: 2^31 - 1.
pub const MAX_STALENESS_MS: u32 = (1 << 31) - 1;
/// Longest deadline a definition may declare: a day, as the longest
/// caller deadline a server honors.
pub const MAX_TIMEOUT_MS: u64 = 86_400_000;

/// A query's declared read consistency.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Fresh: reflects every write completed before the read began.
    /// Manifests that declare nothing read this way.
    #[default]
    Linearizable,
    /// The serving replica's own applied state, at most
    /// `max_staleness_ms` behind when declared, however far otherwise.
    ReplicaLocal {
        /// The declared bound; a caller may only tighten it.
        max_staleness_ms: Option<u32>,
    },
}

/// A source-change trigger: method name plus source collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TriggerDef {
    /// Trigger (method) name.
    pub name: String,
    /// Source collection name.
    pub source: String,
}

/// A deletion-retention hook: the mutation the host hands each removed
/// row of `source` to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionDef {
    /// Hook (mutation) name.
    pub name: String,
    /// Source collection name.
    pub source: String,
}

/// An aggregate's delta maintenance: the derived value follows the
/// equality groups of `index` over `collection`, and the host hands a
/// cached value its group's row changes through the reducer instead of
/// building it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateDef {
    /// Source collection, the module's own.
    pub collection: String,
    /// Index whose equality groups the values follow.
    pub index: String,
    /// Keeps cached values across deployments while unchanged.
    pub version: Option<String>,
}

/// Longest maintenance-task name, in bytes.
pub const MAX_TASK_NAME: usize = 128;
/// Longest aggregate version, in bytes.
pub const MAX_AGGREGATE_VERSION: usize = 128;

/// A maintenance task: the host's leader runs it whenever it is due.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskDef {
    /// Task name, unique among the module's definitions and hooks.
    pub name: String,
    /// Whether a failed run goes to the task's error callback rather
    /// than backing off.
    pub on_error: bool,
}

/// A derived definition's root-retention policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MaterializePolicy {
    /// Evaluations cache unpinned unless explicitly materialized.
    #[default]
    Never,
    /// Every evaluation pins its outcome.
    Always,
    /// One root per row of a source collection ([`ModuleDef::each`]),
    /// with arguments `{"key": <row key>}`: a mutation inserting a row
    /// pins its root, deleting it unpins, and rows that predate the
    /// deployment pin at their first evaluation. Manifests from before
    /// the source was encoded carry none and behave as
    /// [`Never`][Self::Never].
    Each,
}

/// What a module exports to other modules. Everything else is private
/// to it; exported collections are read-only to other modules.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExportsDef {
    /// Exported collections.
    pub collections: BTreeSet<String>,
    /// Exported derived values.
    pub derived: BTreeSet<String>,
    /// Exported queries and mutations.
    pub methods: BTreeSet<String>,
    /// Method to the modules allowed to call it; methods without an
    /// entry are open to every module.
    pub callers: BTreeMap<String, BTreeSet<String>>,
}

/// An import's expected argument, result and failure contract.
#[derive(Clone, Debug, PartialEq)]
pub struct ContractDef {
    /// Arguments the importer sends.
    pub args: Schema,
    /// Results the importer accepts.
    pub result: Schema,
    /// Failures the importer accepts.
    pub errors: Schema,
}

/// One imported method or derived value.
#[derive(Clone, Debug, PartialEq)]
pub struct ImportedDef {
    /// Exported name in the provider.
    pub name: String,
    /// Expected kind: query, mutation or derived.
    pub kind: DefKind,
    /// Expected contract; `None` links without a compatibility proof.
    pub contract: Option<ContractDef>,
}

/// One imported collection.
#[derive(Clone, Debug, PartialEq)]
pub struct ImportedCollectionDef {
    /// Exported name in the provider.
    pub name: String,
    /// Expected key schema.
    pub key: Schema,
    /// Rows the importer accepts.
    pub row: Schema,
}

/// What a module imports from one provider.
#[derive(Clone, Debug, PartialEq)]
pub struct ImportDef {
    /// Concrete module, or `$slot`.
    pub module: String,
    /// Imported collections.
    pub collections: Vec<ImportedCollectionDef>,
    /// Imported methods and derived values.
    pub definitions: Vec<ImportedDef>,
}

/// One module's deployed descriptors.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ModuleDef {
    /// Collection-scoped capabilities for request-time SQL.
    pub sql_endpoints: HashMap<String, SqlEndpointDef>,
    /// Collection descriptors.
    pub collections: Vec<CollectionDef>,
    /// Definition name to kind.
    pub def_kinds: HashMap<String, DefKind>,
    /// Definition name to invocation-argument schema. Dispatch validates
    /// arguments before running any callback.
    pub args: HashMap<String, Schema>,
    /// Derived name to root-retention policy.
    pub materialize: HashMap<String, MaterializePolicy>,
    /// `Each` derived name to its source collection: one root per row.
    pub each: HashMap<String, String>,
    /// Registered triggers.
    pub triggers: Vec<TriggerDef>,
    /// Deletion-retention hooks.
    pub retention_hooks: Vec<RetentionDef>,
    /// Aggregate derived name to its delta maintenance.
    pub aggregates: HashMap<String, AggregateDef>,
    /// Maintenance tasks, in manifest order.
    pub tasks: Vec<TaskDef>,
    /// External HTTP alias allowlist: alias to definition name. Only
    /// these names are callable from outside the module over HTTP.
    pub http: BTreeMap<String, String>,
    /// Whether the module declares an authorization hook.
    pub authorize: bool,
    /// Derived name to the access rule methods must satisfy to read it.
    pub derived_access: HashMap<String, Rule>,
    /// Query name to declared read consistency.
    pub consistency: HashMap<String, ReadConsistency>,
    /// Method or transaction name to the deadline it declares, in
    /// milliseconds; absent takes the runtime's default.
    pub timeouts: HashMap<String, u32>,
    /// Definition name to its result contract; absent is `json`.
    pub results: HashMap<String, Schema>,
    /// Definition name to its business-failure contract; absent is
    /// `json`.
    pub errors: HashMap<String, Schema>,
    /// What other modules may read and call.
    pub exports: ExportsDef,
    /// What this module expects of other modules.
    pub imports: Vec<ImportDef>,
    /// Deployment configuration: slot name (without `$`) to the module
    /// it binds. Deployers add it to the manifest as `bindings`.
    pub bindings: BTreeMap<String, String>,
    /// The queues it declares: contracts and line turns.
    pub queues: Vec<crate::work_queue::QueueDecl>,
    /// Named SQL plans, rebound against local collection schemas at deployment.
    pub sql: HashMap<String, flower_sql::Plan>,
}

/// A deployed capability whose limits cannot be supplied by a SQL caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlEndpointDef {
    pub collections: BTreeSet<String>,
    pub budget: flower_sdk::SqlBudget,
    pub allow_scan: bool,
}

impl ModuleDef {
    /// A definition's result contract.
    pub fn result_schema(&self, name: &str) -> Schema {
        self.results.get(name).copied().unwrap_or(Schema::Json)
    }

    /// A definition's business-failure contract.
    pub fn errors_schema(&self, name: &str) -> Schema {
        self.errors.get(name).copied().unwrap_or(Schema::Json)
    }

    /// The module an import names: a concrete module as is, a `$slot`
    /// through this deployment's bindings.
    pub fn resolve_module<'a>(&'a self, module: &'a str) -> Option<&'a str> {
        match module.strip_prefix('$') {
            Some(slot) => self.bindings.get(slot).map(String::as_str),
            None => Some(module),
        }
    }

    /// A query's declared read consistency; `None` when the module has
    /// no such query.
    pub fn read_consistency(&self, name: &str) -> Option<ReadConsistency> {
        if self.def_kinds.get(name) != Some(&DefKind::Query) {
            return None;
        }
        Some(self.consistency.get(name).copied().unwrap_or_default())
    }

    /// The read consistency of the query a call names: through the alias
    /// allowlist when the call is external, as the call itself resolves.
    pub fn call_consistency(&self, name: &str, external: bool) -> Option<ReadConsistency> {
        let name = if external {
            self.resolve_alias(name)?
        } else {
            name
        };
        self.read_consistency(name)
    }

    /// Look up a collection by name.
    pub fn collection(&self, name: &str) -> Option<&CollectionDef> {
        self.collections.iter().find(|def| def.name == name)
    }

    /// Look up a definition's argument schema.
    pub fn args_schema(&self, name: &str) -> Option<&Schema> {
        self.args.get(name)
    }

    /// Look up a derived definition's retention policy, defaulting to
    /// [`Never`][MaterializePolicy::Never] when the manifest omits it.
    pub fn retention(&self, name: &str) -> MaterializePolicy {
        self.materialize.get(name).copied().unwrap_or_default()
    }
}

fn manifest_error(what: &str) -> Error {
    Error::Manifest(format!("invalid manifest: {what}"))
}

fn get<'a>(map: &'a std::collections::BTreeMap<String, Value>, key: &str) -> Result<&'a Value> {
    map.get(key)
        .ok_or_else(|| manifest_error(&format!("missing member `{key}`")))
}

fn as_object<'a>(
    value: &'a Value,
    what: &str,
) -> Result<&'a std::collections::BTreeMap<String, Value>> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(manifest_error(what)),
    }
}

fn as_str<'a>(value: &'a Value, what: &str) -> Result<&'a str> {
    match value {
        Value::String(text) => Ok(text.as_str()),
        _ => Err(manifest_error(what)),
    }
}

fn as_array<'a>(value: &'a Value, what: &str) -> Result<&'a Vec<Value>> {
    match value {
        Value::Array(items) => Ok(items),
        _ => Err(manifest_error(what)),
    }
}

fn as_u32(value: &Value, what: &str) -> Result<u32> {
    match value {
        Value::Number(number) if number.is_finite() && number.fract() == 0.0 && *number >= 0.0 => {
            u32::try_from(*number as u64).map_err(|_| manifest_error(what))
        }
        _ => Err(manifest_error(what)),
    }
}

fn as_f64(value: &Value, what: &str) -> Result<f64> {
    match value {
        Value::Number(number) if number.is_finite() => Ok(*number),
        _ => Err(manifest_error(what)),
    }
}

/// Load owned module descriptors out of a manifest value, validating
/// references, discriminators, covering indexes and trigger sources.
pub fn load_module(manifest: &Value) -> Result<ModuleDef> {
    let root = as_object(manifest, "manifest is not an object")?;
    let mut collections = Vec::new();
    for item in as_array(get(root, "collections")?, "collections is not an array")? {
        collections.push(load_collection(item)?);
    }
    let mut def_kinds = HashMap::new();
    let mut args = HashMap::new();
    let mut aggregates = HashMap::new();
    let mut materialize = HashMap::new();
    let mut each = HashMap::new();
    let mut derived_access = HashMap::new();
    let mut consistency = HashMap::new();
    let mut timeouts = HashMap::new();
    let mut results = HashMap::new();
    let mut errors = HashMap::new();
    for item in as_array(get(root, "definitions")?, "definitions is not an array")? {
        let def = as_object(item, "definition is not an object")?;
        let name = as_str(get(def, "name")?, "definition without a name")?.to_string();
        let kind = match as_str(get(def, "kind")?, "definition without a kind")? {
            "query" => DefKind::Query,
            "mutation" => DefKind::Mutation,
            "derived" => DefKind::Derived,
            "transaction" => DefKind::Transaction,
            _ => return Err(manifest_error("unknown definition kind")),
        };
        if def_kinds.insert(name.clone(), kind).is_some() {
            return Err(manifest_error("duplicate definition name"));
        }
        let schema = load_schema(get(def, "args")?)?;
        args.insert(name.clone(), schema);
        if let Some(result) = def.get("result") {
            results.insert(name.clone(), load_schema(result)?);
        }
        if let Some(failures) = def.get("errors") {
            errors.insert(name.clone(), load_schema(failures)?);
        }
        if let Some(timeout) = def.get("timeout_ms") {
            let timeout = as_u32(timeout, "timeout_ms is not a whole number")?;
            if timeout == 0 || u64::from(timeout) > MAX_TIMEOUT_MS {
                return Err(manifest_error("timeout_ms is 1 ms to a day"));
            }
            if kind == DefKind::Derived {
                return Err(manifest_error("derived values declare no timeout"));
            }
            timeouts.insert(name.clone(), timeout);
        }
        if kind == DefKind::Query
            && let Some(declared) = def.get("consistency")
        {
            let bound = def
                .get("max_staleness_ms")
                .map(|bound| as_u32(bound, "max_staleness_ms is not a whole number"))
                .transpose()?;
            if bound.is_some_and(|bound| bound > MAX_STALENESS_MS) {
                return Err(manifest_error("max_staleness_ms exceeds 2^31 - 1"));
            }
            let declared = match as_str(declared, "query consistency is not a string")? {
                "linearizable" if bound.is_none() => ReadConsistency::Linearizable,
                "linearizable" => {
                    return Err(manifest_error("a linearizable query declares no staleness"));
                }
                "replica_local" => ReadConsistency::ReplicaLocal {
                    max_staleness_ms: bound,
                },
                _ => return Err(manifest_error("unknown query consistency")),
            };
            consistency.insert(name.clone(), declared);
        }
        if kind == DefKind::Derived
            && let Some(policy) = def.get("materialize")
        {
            let policy = match as_str(policy, "derived materialize is not a string")? {
                "never" => MaterializePolicy::Never,
                "always" => MaterializePolicy::Always,
                "each" => MaterializePolicy::Each,
                _ => return Err(manifest_error("unknown materialize policy")),
            };
            if policy == MaterializePolicy::Each
                && let Some(source) = def.get("source")
            {
                let source = as_str(source, "each source is not a string")?;
                each.insert(name.clone(), source.to_string());
            }
            materialize.insert(name.clone(), policy);
        }
        if let Some(aggregate) = def.get("aggregate") {
            if kind != DefKind::Derived {
                return Err(manifest_error("only derived definitions aggregate"));
            }
            aggregates.insert(name.clone(), load_aggregate(aggregate)?);
        }
        if let Some(rule) = def.get("access") {
            if kind != DefKind::Derived {
                return Err(manifest_error(
                    "only derived definitions take an access rule",
                ));
            }
            let rule = crate::access::parse_derived_rule(rule)
                .map_err(|error| manifest_error(&format!("access of `{name}`: {error}")))?;
            derived_access.insert(name, rule);
        }
    }
    let mut triggers = Vec::new();
    for item in as_array(get(root, "triggers")?, "triggers is not an array")? {
        let def = as_object(item, "trigger is not an object")?;
        triggers.push(TriggerDef {
            name: as_str(get(def, "name")?, "trigger without a name")?.to_string(),
            source: as_str(get(def, "source")?, "trigger without a source")?.to_string(),
        });
    }
    let mut retention = Vec::new();
    if let Some(items) = root.get("retention") {
        for item in as_array(items, "retention is not an array")? {
            let def = as_object(item, "retention hook is not an object")?;
            retention.push(RetentionDef {
                name: as_str(get(def, "name")?, "retention hook without a name")?.to_string(),
                source: as_str(get(def, "source")?, "retention hook without a source")?.to_string(),
            });
        }
    }
    let mut tasks = Vec::new();
    if let Some(items) = root.get("tasks") {
        for item in as_array(items, "tasks is not an array")? {
            let def = as_object(item, "task is not an object")?;
            if def.contains_key("timeout_ms") {
                // Tasks run with the host's defaults: refused rather than
                // silently ignored.
                return Err(manifest_error("tasks declare no timeout"));
            }
            let on_error = match def.get("on_error") {
                None => false,
                Some(Value::Bool(on_error)) => *on_error,
                Some(_) => return Err(manifest_error("task on_error is not a boolean")),
            };
            tasks.push(TaskDef {
                name: as_str(get(def, "name")?, "task without a name")?.to_string(),
                on_error,
            });
        }
    }
    // Older manifests predate aliases and hooks: no member means none.
    let mut http = BTreeMap::new();
    if let Some(aliases) = root.get("http") {
        for item in as_array(aliases, "http is not an array")? {
            let entry = as_object(item, "http alias is not an object")?;
            let alias = as_str(get(entry, "alias")?, "http alias without a name")?;
            let definition = as_str(get(entry, "definition")?, "http alias without a definition")?;
            if alias.is_empty() {
                return Err(manifest_error("empty http alias"));
            }
            if http
                .insert(alias.to_string(), definition.to_string())
                .is_some()
            {
                return Err(manifest_error("duplicate http alias"));
            }
        }
    }
    let authorize = match root.get("authorize") {
        None => false,
        Some(Value::Bool(authorize)) => *authorize,
        Some(_) => return Err(manifest_error("authorize is not a boolean")),
    };
    // No `exports` member exports nothing; no `imports`, nothing.
    let exports = root
        .get("exports")
        .map(load_exports)
        .transpose()?
        .unwrap_or_default();
    let mut imports = Vec::new();
    if let Some(items) = root.get("imports") {
        for item in as_array(items, "imports is not an array")? {
            imports.push(load_import(item)?);
        }
    }
    let mut bindings = BTreeMap::new();
    if let Some(items) = root.get("bindings") {
        for (slot, module) in as_object(items, "bindings is not an object")? {
            let module = as_str(module, "binding is not a string")?;
            bindings.insert(slot.clone(), module.to_string());
        }
    }
    // Queue declarations: payload and result contracts and the line's
    // turn. Older manifests declare none.
    let mut queues = Vec::new();
    if let Some(items) = root.get("queues") {
        for item in as_array(items, "queues is not an array")? {
            let def = as_object(item, "queue declaration is not an object")?;
            let name = as_str(get(def, "name")?, "queue declaration without a name")?;
            crate::work_queue::validate_queue(name)
                .map_err(|_| manifest_error("queue declaration has a bad name"))?;
            let turn_ms = match def.get("turn_ms") {
                None => crate::work_queue::DEFAULT_TURN_MS,
                Some(turn) => u64::from(as_u32(turn, "queue turn is not a whole number")?),
            };
            queues.push(crate::work_queue::QueueDecl {
                name: name.to_string(),
                payload: def
                    .get("payload")
                    .map(load_schema)
                    .transpose()?
                    .unwrap_or(Schema::Json),
                result: def
                    .get("result")
                    .map(load_schema)
                    .transpose()?
                    .unwrap_or(Schema::Json),
                turn_ms,
            });
        }
    }
    let mut module = ModuleDef {
        collections,
        def_kinds,
        args,
        materialize,
        each,
        triggers,
        retention_hooks: retention,
        aggregates,
        tasks,
        http,
        authorize,
        derived_access,
        consistency,
        timeouts,
        results,
        errors,
        exports,
        imports,
        bindings,
        queues,
        sql: HashMap::new(),
        sql_endpoints: HashMap::new(),
    };
    if let Some(plans) = root.get("sql") {
        for enc in as_array(plans, "sql must be an array")? {
            let def = as_object(enc, "SQL declaration must be an object")?;
            let name = as_str(get(def, "name")?, "SQL plan name must be text")?.to_string();
            if name.is_empty() || module.sql.contains_key(&name) {
                return Err(manifest_error("SQL plan names must be nonempty and unique"));
            }
            let statement = as_str(get(def, "statement")?, "SQL statement must be text")?;
            let collection =
                flower_sql::collection(statement).map_err(|e| manifest_error(&e.message))?;
            let source = module
                .collection(&collection)
                .ok_or_else(|| manifest_error("SQL source must be a declared local collection"))?;
            let budget = flower_sdk::SqlBudget::scan(
                as_u32(get(def, "candidates")?, "SQL candidates must be u32")?,
                as_u32(get(def, "bytes")?, "SQL bytes must be u32")?,
                as_u32(get(def, "work")?, "SQL work must be u32")?,
            );
            let plan = flower_sql::Plan::bind(
                statement,
                source.key_schema,
                source.value_schema,
                load_schema(get(def, "params")?)?,
                load_schema(get(def, "row")?)?,
                budget,
                source.access.is_some(),
            )
            .and_then(|plan| {
                let allow_scan = match def.get("allow_scan") {
                    None | Some(Value::Bool(true)) => true,
                    Some(Value::Bool(false)) => false,
                    _ => {
                        return Err(flower_sdk::fail(
                            flower_sql::SQL_INVALID,
                            "SQL allow_scan must be boolean",
                        ));
                    }
                };
                let indexes = source
                    .indexes
                    .iter()
                    .map(|i| flower_sql::Index {
                        name: i.name.clone(),
                        fields: i.fields.clone(),
                    })
                    .collect::<Vec<_>>();
                plan.with_indexes(&indexes, allow_scan)
            })
            .map_err(|e| manifest_error(&format!("SQL `{name}`: {}", e.message)))?;
            module.sql.insert(name, plan);
        }
    }
    if let Some(endpoints) = root.get("sql_endpoints") {
        for entry in as_array(endpoints, "sql_endpoints must be an array")? {
            let entry = as_object(entry, "SQL endpoint must be an object")?;
            let name = as_str(get(entry, "name")?, "SQL endpoint name must be text")?.to_string();
            if name.is_empty() || module.sql_endpoints.contains_key(&name) {
                return Err(manifest_error(
                    "SQL endpoint names must be nonempty and unique",
                ));
            }
            let collections = as_array(
                get(entry, "collections")?,
                "SQL endpoint collections must be an array",
            )?;
            if collections.is_empty() || collections.len() > 64 {
                return Err(manifest_error(
                    "SQL endpoints require 1..=64 explicit local collections",
                ));
            }
            let mut allowed = BTreeSet::new();
            for collection in collections {
                let collection = as_str(collection, "SQL endpoint collection must be text")?;
                if module.collection(collection).is_none()
                    || !allowed.insert(collection.to_string())
                {
                    return Err(manifest_error(
                        "SQL endpoint source must be a unique declared local collection",
                    ));
                }
            }
            let budget = flower_sdk::SqlBudget::scan(
                as_u32(
                    get(entry, "candidates")?,
                    "SQL endpoint candidates must be u32",
                )?,
                as_u32(get(entry, "bytes")?, "SQL endpoint bytes must be u32")?,
                as_u32(get(entry, "work")?, "SQL endpoint work must be u32")?,
            );
            flower_sql::validate_budget(budget).map_err(|e| manifest_error(&e.message))?;
            let allow_scan = match entry.get("allow_scan") {
                None | Some(Value::Bool(true)) => true,
                Some(Value::Bool(false)) => false,
                _ => return Err(manifest_error("SQL endpoint allow_scan must be boolean")),
            };
            module.sql_endpoints.insert(
                name,
                SqlEndpointDef {
                    collections: allowed,
                    budget,
                    allow_scan,
                },
            );
        }
    }
    module.validate()?;
    Ok(module)
}

/// Load a derived definition's `aggregate` member `{collection, index,
/// version?}`.
fn load_aggregate(value: &Value) -> Result<AggregateDef> {
    let map = as_object(value, "aggregate is not an object")?;
    let version = match map.get("version") {
        None => None,
        Some(version) => {
            let version = as_str(version, "aggregate version is not a string")?;
            // 1 to 128 bytes, no control characters.
            if version.is_empty()
                || version.len() > MAX_AGGREGATE_VERSION
                || version.chars().any(char::is_control)
            {
                return Err(manifest_error(
                    "an aggregate version is 1 to 128 bytes without control characters",
                ));
            }
            Some(version.to_string())
        }
    };
    Ok(AggregateDef {
        collection: as_str(get(map, "collection")?, "aggregate without a collection")?.to_string(),
        index: as_str(get(map, "index")?, "aggregate without an index")?.to_string(),
        version,
    })
}

fn string_set(value: &Value, what: &str) -> Result<BTreeSet<String>> {
    let mut set = BTreeSet::new();
    for item in as_array(value, what)? {
        set.insert(as_str(item, what)?.to_string());
    }
    Ok(set)
}

fn load_exports(value: &Value) -> Result<ExportsDef> {
    let map = as_object(value, "exports is not an object")?;
    let mut callers = BTreeMap::new();
    if let Some(allowlists) = map.get("callers") {
        for (method, modules) in as_object(allowlists, "export callers is not an object")? {
            callers.insert(
                method.clone(),
                string_set(modules, "export callers lists module names")?,
            );
        }
    }
    Ok(ExportsDef {
        collections: string_set(get(map, "collections")?, "exported collections list names")?,
        derived: string_set(get(map, "derived")?, "exported derived values list names")?,
        methods: string_set(get(map, "methods")?, "exported methods list names")?,
        callers,
    })
}

fn load_contract(value: &Value) -> Result<ContractDef> {
    let map = as_object(value, "contract is not an object")?;
    Ok(ContractDef {
        args: load_schema(get(map, "args")?)?,
        result: load_schema(get(map, "result")?)?,
        errors: load_schema(get(map, "errors")?)?,
    })
}

fn load_import(value: &Value) -> Result<ImportDef> {
    let map = as_object(value, "import is not an object")?;
    let module = as_str(get(map, "module")?, "import module is not a string")?.to_string();
    let mut collections = Vec::new();
    for item in as_array(
        get(map, "collections")?,
        "imported collections is not an array",
    )? {
        let entry = as_object(item, "imported collection is not an object")?;
        collections.push(ImportedCollectionDef {
            name: as_str(get(entry, "name")?, "imported collection without a name")?.to_string(),
            key: load_schema(get(entry, "key")?)?,
            row: load_schema(get(entry, "row")?)?,
        });
    }
    let mut definitions = Vec::new();
    for item in as_array(get(map, "derived")?, "imported derived is not an array")? {
        let entry = as_object(item, "imported derived value is not an object")?;
        definitions.push(ImportedDef {
            name: as_str(get(entry, "name")?, "imported derived without a name")?.to_string(),
            kind: DefKind::Derived,
            contract: entry.get("contract").map(load_contract).transpose()?,
        });
    }
    for item in as_array(get(map, "methods")?, "imported methods is not an array")? {
        let entry = as_object(item, "imported method is not an object")?;
        let kind = match as_str(get(entry, "kind")?, "imported method without a kind")? {
            "query" => DefKind::Query,
            "mutation" => DefKind::Mutation,
            _ => return Err(manifest_error("imported methods are queries or mutations")),
        };
        definitions.push(ImportedDef {
            name: as_str(get(entry, "name")?, "imported method without a name")?.to_string(),
            kind,
            contract: entry.get("contract").map(load_contract).transpose()?,
        });
    }
    Ok(ImportDef {
        module,
        collections,
        definitions,
    })
}

fn load_collection(item: &Value) -> Result<CollectionDef> {
    let def = as_object(item, "collection is not an object")?;
    let name = as_str(get(def, "name")?, "collection without a name")?.to_string();
    if name.is_empty() {
        return Err(manifest_error("empty collection name"));
    }
    let mut indexes = Vec::new();
    for item in as_array(get(def, "indexes")?, "indexes is not an array")? {
        let index = as_object(item, "index is not an object")?;
        let mut fields = Vec::new();
        for field in as_array(get(index, "fields")?, "index fields is not an array")? {
            fields.push(as_str(field, "index field is not a string")?.to_string());
        }
        if fields.is_empty() {
            return Err(manifest_error("index without fields"));
        }
        indexes.push(IndexDescriptor {
            name: as_str(get(index, "name")?, "index without a name")?.to_string(),
            fields,
        });
    }
    let mut references = Vec::new();
    for item in as_array(get(def, "references")?, "references is not an array")? {
        references.push(load_reference(item)?);
    }
    let access = def
        .get("access")
        .map(|policy| {
            crate::access::parse_policy(policy)
                .map(Arc::new)
                .map_err(|error| manifest_error(&format!("access of `{name}`: {error}")))
        })
        .transpose()?;
    Ok(CollectionDef {
        name,
        key_schema: load_schema(get(def, "key")?)?,
        value_schema: load_schema(get(def, "value")?)?,
        indexes,
        references,
        access,
    })
}

fn load_reference(item: &Value) -> Result<ReferenceDescriptor> {
    let def = as_object(item, "reference is not an object")?;
    let mut targets = Vec::new();
    for target in as_array(get(def, "targets")?, "reference targets is not an array")? {
        targets.push(as_str(target, "reference target is not a string")?.to_string());
    }
    if targets.is_empty() {
        return Err(manifest_error("reference without targets"));
    }
    let discriminator = match get(def, "discriminator")? {
        Value::Null => None,
        Value::String(name) => Some(name.clone()),
        _ => return Err(manifest_error("discriminator is not a string")),
    };
    let place = match get(def, "place")? {
        Value::String(place) if place == "key" => PlaceDescriptor::Key,
        Value::String(place) if place == "key_prefix" => PlaceDescriptor::KeyPrefix,
        Value::Object(map) => {
            let mut fields = Vec::new();
            for field in as_array(get(map, "fields")?, "reference fields is not an array")? {
                fields.push(as_str(field, "reference field is not a string")?.to_string());
            }
            if fields.is_empty() {
                return Err(manifest_error("reference without fields"));
            }
            PlaceDescriptor::Fields(fields)
        }
        _ => return Err(manifest_error("bad reference place")),
    };
    let on_delete = match as_str(get(def, "on_delete")?, "reference without on_delete")? {
        "restrict" => OnDelete::Restrict,
        "cascade" => OnDelete::Cascade,
        "set_null" => OnDelete::SetNull,
        _ => return Err(manifest_error("unknown on_delete")),
    };
    Ok(ReferenceDescriptor {
        targets,
        discriminator,
        place,
        on_delete,
    })
}

/// Load a schema out of its structural manifest encoding, leaking the
/// nested interiors the `const`-oriented [`Schema`] type requires.
pub fn load_schema(enc: &Value) -> Result<Schema> {
    let def = as_object(enc, "schema is not an object")?;
    let kind = as_str(get(def, "type")?, "schema without a type")?;
    match kind {
        "decimal_i64" => Ok(Schema::DecimalI64),
        "decimal_u64" => Ok(Schema::DecimalU64),
        "string" => {
            let mut schema = v::string();
            if let Some(bound) = def.get("min") {
                schema = schema.min(as_u32(bound, "bad string min")?);
            }
            if let Some(bound) = def.get("max") {
                schema = schema.max(as_u32(bound, "bad string max")?);
            }
            if let Some(pattern) = def.get("pattern") {
                schema = schema.pattern(leak_str(as_str(pattern, "bad string pattern")?));
            }
            Ok(schema.build())
        }
        "number" => {
            let mut schema = v::number();
            if let Some(bound) = def.get("min") {
                schema = schema.min(as_f64(bound, "bad number min")?);
            }
            if let Some(bound) = def.get("max") {
                schema = schema.max(as_f64(bound, "bad number max")?);
            }
            Ok(schema.build())
        }
        "int" => {
            let mut schema = v::int();
            if let Some(bound) = def.get("min") {
                schema = schema.min(as_i64(bound, "bad int min")?);
            }
            if let Some(bound) = def.get("max") {
                schema = schema.max(as_i64(bound, "bad int max")?);
            }
            Ok(schema.build())
        }
        "boolean" => Ok(v::boolean()),
        "null" => Ok(v::null()),
        "json" => Ok(v::json()),
        "array" => Ok(v::array(leak_schema(load_schema(get(def, "element")?)?))),
        "tuple" => {
            let mut elements = Vec::new();
            for item in as_array(get(def, "elements")?, "tuple without elements")? {
                elements.push(load_schema(item)?);
            }
            Ok(v::tuple(Box::leak(elements.into_boxed_slice())))
        }
        "object" => {
            let mut fields: Vec<SchemaField> = Vec::new();
            for item in as_array(get(def, "fields")?, "object without fields")? {
                let field = as_object(item, "object field is not an object")?;
                let name = leak_str(as_str(get(field, "name")?, "field without a name")?);
                let schema = load_schema(get(field, "schema")?)?;
                let optional = match get(field, "optional")? {
                    Value::Bool(optional) => *optional,
                    _ => return Err(manifest_error("field optional is not a boolean")),
                };
                fields.push(if optional {
                    v::optional(name, schema)
                } else {
                    v::field(name, schema)
                });
            }
            Ok(v::object(Box::leak(fields.into_boxed_slice())))
        }
        "nullable" => Ok(v::nullable(leak_schema(load_schema(get(def, "of")?)?))),
        "enum" => {
            let mut variants: Vec<&str> = Vec::new();
            for item in as_array(get(def, "variants")?, "enum without variants")? {
                variants.push(leak_str(as_str(item, "enum variant is not a string")?));
            }
            if variants.is_empty() {
                return Err(manifest_error("enum without variants"));
            }
            Ok(v::str_enum(Box::leak(variants.into_boxed_slice())))
        }
        _ => Err(manifest_error("unknown schema type")),
    }
}

fn as_i64(value: &Value, what: &str) -> Result<i64> {
    match value {
        Value::Number(number) if number.is_finite() && number.fract() == 0.0 => {
            const MAX: f64 = i64::MAX as f64;
            const MIN: f64 = i64::MIN as f64;
            if *number > MAX || *number < MIN {
                return Err(manifest_error(what));
            }
            Ok(*number as i64)
        }
        _ => Err(manifest_error(what)),
    }
}

impl ModuleDef {
    /// Validate cross-references: targets exist, discriminators match
    /// poly shape, key places stay single-target, every field reference
    /// has a covering source index, and trigger sources exist.
    pub fn validate(&self) -> Result<()> {
        for collection in &self.collections {
            if collection.name.starts_with('$') {
                return Err(manifest_error(
                    "collection names starting with `$` are reserved",
                ));
            }
            if self
                .collections
                .iter()
                .filter(|def| def.name == collection.name)
                .count()
                != 1
            {
                return Err(manifest_error("duplicate collection name"));
            }
            for reference in &collection.references {
                for target in &reference.targets {
                    if self.collection(target).is_none() {
                        return Err(manifest_error("reference to unknown collection"));
                    }
                }
                let poly = reference.targets.len() > 1;
                if poly != reference.discriminator.is_some() {
                    return Err(manifest_error("discriminator must match poly shape"));
                }
                match &reference.place {
                    PlaceDescriptor::Fields(fields) => {
                        // Single-target references need a fields-leading
                        // covering index; poly references need a
                        // discriminator-leading one over discriminator
                        // plus key fields.
                        let covered = match &reference.discriminator {
                            None => collection.indexes.iter().any(|index| {
                                index.fields.len() >= fields.len()
                                    && index.fields[..fields.len()] == fields[..]
                            }),
                            Some(discriminator) => collection.indexes.iter().any(|index| {
                                index.fields.len() > fields.len()
                                    && index.fields[0] == *discriminator
                                    && index.fields[1..1 + fields.len()] == fields[..]
                            }),
                        };
                        if !covered {
                            return Err(manifest_error("field reference without a covering index"));
                        }
                    }
                    PlaceDescriptor::Key | PlaceDescriptor::KeyPrefix => {
                        if poly {
                            return Err(manifest_error("only field references can be poly"));
                        }
                        if reference.on_delete == OnDelete::SetNull {
                            return Err(manifest_error("set_null needs a field reference"));
                        }
                    }
                }
            }
        }
        for trigger in &self.triggers {
            if trigger.name.is_empty() {
                return Err(manifest_error("empty trigger name"));
            }
            if self.collection(&trigger.source).is_none() {
                return Err(manifest_error("trigger on unknown collection"));
            }
        }
        for hook in &self.retention_hooks {
            if hook.name.is_empty() {
                return Err(manifest_error("empty retention hook name"));
            }
            if self.collection(&hook.source).is_none() {
                return Err(manifest_error("retention on unknown collection"));
            }
        }
        // An aggregate's groups are the equality groups of one of its
        // own module's indexes.
        for (name, aggregate) in &self.aggregates {
            let indexed = self
                .collection(&aggregate.collection)
                .is_some_and(|collection| {
                    collection
                        .indexes
                        .iter()
                        .any(|index| index.name == aggregate.index)
                });
            if !indexed {
                return Err(manifest_error(&format!(
                    "aggregate `{name}` names no index `{}` of a collection `{}`",
                    aggregate.index, aggregate.collection
                )));
            }
        }
        // The host runs a task as a query (`due`) and a mutation (`run`)
        // of its name, which no definition or hook may share.
        let mut task_names = BTreeSet::new();
        for task in &self.tasks {
            if task.name.is_empty() || task.name.len() > MAX_TASK_NAME {
                return Err(manifest_error("a task name is 1 to 128 bytes"));
            }
            if !task_names.insert(task.name.as_str()) {
                return Err(manifest_error("duplicate task name"));
            }
            if self.def_kinds.contains_key(&task.name)
                || self
                    .triggers
                    .iter()
                    .any(|trigger| trigger.name == task.name)
                || self
                    .retention_hooks
                    .iter()
                    .any(|hook| hook.name == task.name)
            {
                return Err(manifest_error(&format!(
                    "task `{}` shares its name with a definition or hook",
                    task.name
                )));
            }
        }
        // An `Each` root takes `{"key": <row key>}`, so its arguments
        // must accept the source's keys.
        for (name, source) in &self.each {
            let Some(collection) = self.collection(source) else {
                return Err(manifest_error(&format!(
                    "`{name}` materializes each row of unknown collection `{source}`"
                )));
            };
            let row_key = [v::field("key", collection.key_schema)];
            let args = v::object(Box::leak(Box::new(row_key)));
            if !self
                .args_schema(name)
                .is_some_and(|schema| schema.accepts(&args))
            {
                return Err(manifest_error(&format!(
                    "`{name}` must take `{{\"key\": ...}}` with `{source}`'s keys"
                )));
            }
        }
        // Aliases expose callable definitions only: derived values are
        // read through methods, never invoked from outside.
        for (alias, definition) in &self.http {
            match self.def_kinds.get(definition) {
                Some(DefKind::Query | DefKind::Mutation | DefKind::Transaction) => {}
                Some(DefKind::Derived) => {
                    return Err(manifest_error(&format!(
                        "http alias `{alias}` names a derived value"
                    )));
                }
                None => {
                    return Err(manifest_error(&format!(
                        "http alias `{alias}` names an unknown definition"
                    )));
                }
            }
        }
        let declared: BTreeSet<&str> = self
            .collections
            .iter()
            .map(|def| def.name.as_str())
            .collect();
        let policies: BTreeMap<&str, &CollectionPolicy> = self
            .collections
            .iter()
            .filter_map(|def| {
                def.access
                    .as_deref()
                    .map(|policy| (def.name.as_str(), policy))
            })
            .collect();
        crate::access::validate_targets(
            &declared,
            &policies,
            self.derived_access
                .iter()
                .map(|(name, rule)| (name.as_str(), rule)),
        )
        .map_err(|error| manifest_error(&format!("access: {error}")))?;
        self.validate_links()
    }

    /// Exports name what the module declares, allowlists name exported
    /// methods and real modules, and every slot an import names is bound
    /// while every binding is used.
    fn validate_links(&self) -> Result<()> {
        let exports = &self.exports;
        for name in &exports.collections {
            if self.collection(name).is_none() {
                return Err(manifest_error(&format!(
                    "exported collection `{name}` is not declared"
                )));
            }
        }
        for name in &exports.derived {
            if self.def_kinds.get(name) != Some(&DefKind::Derived) {
                return Err(manifest_error(&format!(
                    "exported derived value `{name}` is not declared"
                )));
            }
        }
        for name in &exports.methods {
            if !matches!(
                self.def_kinds.get(name),
                Some(DefKind::Query | DefKind::Mutation)
            ) {
                return Err(manifest_error(&format!(
                    "exported method `{name}` is not a declared query or mutation"
                )));
            }
        }
        for (method, modules) in &exports.callers {
            if !exports.methods.contains(method) {
                return Err(manifest_error(&format!(
                    "callers of `{method}`, which is not exported"
                )));
            }
            if modules
                .iter()
                .any(|module| module.is_empty() || module.starts_with('$'))
            {
                return Err(manifest_error(&format!(
                    "callers of `{method}` must name deployed modules, not slots"
                )));
            }
        }
        let mut used = BTreeSet::new();
        for import in &self.imports {
            if import.module.is_empty() || import.module == "$" {
                return Err(manifest_error("import without a module"));
            }
            if let Some(slot) = import.module.strip_prefix('$') {
                if !self.bindings.contains_key(slot) {
                    return Err(manifest_error(&format!("slot `${slot}` is not bound")));
                }
                used.insert(slot);
            }
        }
        for (slot, module) in &self.bindings {
            if !used.contains(slot.as_str()) {
                return Err(manifest_error(&format!(
                    "binding `{slot}` names no imported slot"
                )));
            }
            if module.is_empty() || module.starts_with('$') {
                return Err(manifest_error(&format!(
                    "binding `{slot}` must name a module"
                )));
            }
        }
        Ok(())
    }

    /// Resolve an external HTTP alias to its definition name.
    pub fn resolve_alias(&self, alias: &str) -> Option<&str> {
        self.http.get(alias).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    fn schema_enc(kind: &str) -> BTreeMap<String, Value> {
        let mut map = BTreeMap::new();
        map.insert("type".to_string(), text(kind));
        map
    }

    fn manifest() -> Value {
        let key = Value::Object(schema_enc("string"));
        let mut value = schema_enc("object");
        let mut title = BTreeMap::new();
        title.insert("name".to_string(), text("title"));
        title.insert("schema".to_string(), Value::Object(schema_enc("string")));
        title.insert("optional".to_string(), Value::Bool(false));
        value.insert(
            "fields".to_string(),
            Value::Array(vec![Value::Object(title)]),
        );
        let value = Value::Object(value);

        let mut index = BTreeMap::new();
        index.insert("name".to_string(), text("by_owner"));
        index.insert("fields".to_string(), Value::Array(vec![text("owner")]));

        let mut reference = BTreeMap::new();
        reference.insert("targets".to_string(), Value::Array(vec![text("posts")]));
        reference.insert("discriminator".to_string(), Value::Null);
        let mut place = BTreeMap::new();
        place.insert("fields".to_string(), Value::Array(vec![text("owner")]));
        reference.insert("place".to_string(), Value::Object(place));
        reference.insert("on_delete".to_string(), text("cascade"));

        let mut tasks = BTreeMap::new();
        tasks.insert("name".to_string(), text("tasks"));
        tasks.insert("key".to_string(), key.clone());
        tasks.insert("value".to_string(), value);
        tasks.insert(
            "indexes".to_string(),
            Value::Array(vec![Value::Object(index)]),
        );
        tasks.insert(
            "references".to_string(),
            Value::Array(vec![Value::Object(reference)]),
        );

        let mut posts = BTreeMap::new();
        posts.insert("name".to_string(), text("posts"));
        posts.insert("key".to_string(), key);
        posts.insert("value".to_string(), Value::Object(schema_enc("json")));
        posts.insert("indexes".to_string(), Value::Array(vec![]));
        posts.insert("references".to_string(), Value::Array(vec![]));

        let mut query = BTreeMap::new();
        query.insert("name".to_string(), text("get_task"));
        query.insert("kind".to_string(), text("query"));
        query.insert("args".to_string(), Value::Object(schema_enc("json")));

        let mut derived = BTreeMap::new();
        derived.insert("name".to_string(), text("total"));
        derived.insert("kind".to_string(), text("derived"));
        derived.insert("args".to_string(), Value::Object(schema_enc("string")));
        derived.insert("materialize".to_string(), text("always"));

        let mut trigger = BTreeMap::new();
        trigger.insert("name".to_string(), text("on_task"));
        trigger.insert("source".to_string(), text("tasks"));

        let mut root = BTreeMap::new();
        root.insert(
            "collections".to_string(),
            Value::Array(vec![Value::Object(tasks), Value::Object(posts)]),
        );
        root.insert(
            "definitions".to_string(),
            Value::Array(vec![Value::Object(query), Value::Object(derived)]),
        );
        root.insert(
            "triggers".to_string(),
            Value::Array(vec![Value::Object(trigger)]),
        );
        root.insert("http".to_string(), Value::Array(vec![]));
        Value::Object(root)
    }

    #[test]
    fn modules_load_and_validate() {
        let module = load_module(&manifest()).unwrap();
        assert_eq!(module.collections.len(), 2);
        assert_eq!(module.def_kinds.get("get_task"), Some(&DefKind::Query));
        assert_eq!(module.def_kinds.get("total"), Some(&DefKind::Derived));
        assert!(module.args_schema("get_task").unwrap().is(&Value::Null));
        assert!(module.args_schema("total").unwrap().is(&text("x")));
        assert!(!module.args_schema("total").unwrap().is(&Value::Null));
        assert_eq!(module.retention("total"), MaterializePolicy::Always);
        assert_eq!(module.retention("get_task"), MaterializePolicy::Never);
        let tasks = module.collection("tasks").unwrap();
        assert_eq!(tasks.references.len(), 1);
        assert_eq!(tasks.references[0].on_delete, OnDelete::Cascade);
        // Loaded schemas validate.
        let ok = Value::Object(BTreeMap::from([(
            "title".to_string(),
            Value::String("t".to_string()),
        )]));
        assert!(tasks.value_schema.is(&ok));
        assert!(!tasks.value_schema.is(&Value::Null));
    }

    /// The manifest with these root `tasks` and `total` aggregating
    /// over `aggregate` (`[collection, index, version?]`), when given.
    fn maintained(tasks: Vec<(&str, bool)>, aggregate: Option<Vec<&str>>) -> Value {
        let Value::Object(mut root) = manifest() else {
            unreachable!("manifest is an object");
        };
        let declared = tasks
            .into_iter()
            .map(|(name, on_error)| {
                Value::Object(BTreeMap::from([
                    ("name".to_string(), text(name)),
                    ("on_error".to_string(), Value::Bool(on_error)),
                ]))
            })
            .collect();
        root.insert("tasks".to_string(), Value::Array(declared));
        if let Some(aggregate) = aggregate
            && let Some(Value::Array(definitions)) = root.get_mut("definitions")
        {
            let mut member = BTreeMap::from([
                ("collection".to_string(), text(aggregate[0])),
                ("index".to_string(), text(aggregate[1])),
            ]);
            if let Some(version) = aggregate.get(2) {
                member.insert("version".to_string(), text(version));
            }
            for definition in definitions {
                if let Value::Object(map) = definition
                    && map.get("name") == Some(&text("total"))
                {
                    map.insert("aggregate".to_string(), Value::Object(member.clone()));
                }
            }
        }
        Value::Object(root)
    }

    #[test]
    fn tasks_and_aggregates_load_and_validate() {
        let module = load_module(&maintained(
            vec![("purge", false), ("report", true)],
            Some(vec!["tasks", "by_owner", "2"]),
        ))
        .unwrap();
        assert_eq!(
            module.tasks,
            vec![
                TaskDef {
                    name: "purge".to_string(),
                    on_error: false,
                },
                TaskDef {
                    name: "report".to_string(),
                    on_error: true,
                },
            ]
        );
        assert_eq!(
            module.aggregates.get("total"),
            Some(&AggregateDef {
                collection: "tasks".to_string(),
                index: "by_owner".to_string(),
                version: Some("2".to_string()),
            })
        );
        // A task may not share a definition's or a hook's name, nor
        // another task's.
        for clash in ["get_task", "on_task"] {
            assert!(load_module(&maintained(vec![(clash, false)], None)).is_err());
        }
        assert!(load_module(&maintained(vec![("a", false), ("a", true)], None)).is_err());
        // A task runs with the host's defaults: it declares no timeout,
        // rather than one nothing would honour.
        let Value::Object(mut timed) = maintained(vec![("sweep", false)], None) else {
            unreachable!("manifest is an object");
        };
        if let Some(Value::Array(tasks)) = timed.get_mut("tasks")
            && let Some(Value::Object(task)) = tasks.first_mut()
        {
            task.insert("timeout_ms".to_string(), Value::Number(100.0));
        }
        assert!(load_module(&Value::Object(timed)).is_err());
        assert!(load_module(&maintained(vec![("", false)], None)).is_err());
        let long = "t".repeat(MAX_TASK_NAME + 1);
        assert!(load_module(&maintained(vec![(&long, false)], None)).is_err());
        // An aggregate names an index of one of its module's collections,
        // and a well-formed version.
        assert!(load_module(&maintained(vec![], Some(vec!["tasks", "by_title"]))).is_err());
        assert!(load_module(&maintained(vec![], Some(vec!["posts", "by_owner"]))).is_err());
        assert!(load_module(&maintained(vec![], Some(vec!["tasks", "by_owner", ""]))).is_err());
        assert!(load_module(&maintained(vec![], Some(vec!["tasks", "by_owner", "a\nb"]))).is_err());
        // Manifests without either member load as before.
        let plain = load_module(&manifest()).unwrap();
        assert!(plain.tasks.is_empty() && plain.aggregates.is_empty());
    }

    /// The manifest with `get_task` declaring `consistency`.
    fn declaring(consistency: &str) -> Value {
        let Value::Object(mut root) = manifest() else {
            unreachable!("manifest is an object");
        };
        let Some(Value::Array(definitions)) = root.get_mut("definitions") else {
            unreachable!("manifest has definitions");
        };
        for definition in definitions {
            if let Value::Object(map) = definition
                && map.get("name") == Some(&text("get_task"))
            {
                map.insert("consistency".to_string(), text(consistency));
            }
        }
        Value::Object(root)
    }

    #[test]
    fn queries_declare_their_read_consistency() {
        // Undeclared reads linearizably; only queries have one.
        let module = load_module(&manifest()).unwrap();
        assert_eq!(
            module.read_consistency("get_task"),
            Some(ReadConsistency::Linearizable)
        );
        assert_eq!(module.read_consistency("total"), None);
        assert_eq!(module.read_consistency("missing"), None);
        let module = load_module(&declaring("replica_local")).unwrap();
        assert_eq!(
            module.read_consistency("get_task"),
            Some(ReadConsistency::ReplicaLocal {
                max_staleness_ms: None
            })
        );
        let module = load_module(&declaring("linearizable")).unwrap();
        assert_eq!(
            module.read_consistency("get_task"),
            Some(ReadConsistency::Linearizable)
        );
        assert!(load_module(&declaring("eventual")).is_err());
        // A replica-local query may declare its own bound, up to 2^31 - 1.
        let bounded = |consistency: &str, bound: Value| {
            let Value::Object(mut root) = declaring(consistency) else {
                unreachable!("manifest is an object");
            };
            if let Some(Value::Array(definitions)) = root.get_mut("definitions") {
                for definition in definitions {
                    if let Value::Object(map) = definition
                        && map.get("name") == Some(&text("get_task"))
                    {
                        map.insert("max_staleness_ms".to_string(), bound.clone());
                    }
                }
            }
            load_module(&Value::Object(root))
        };
        let module = bounded("replica_local", Value::Number(250.0)).unwrap();
        assert_eq!(
            module.read_consistency("get_task"),
            Some(ReadConsistency::ReplicaLocal {
                max_staleness_ms: Some(250)
            })
        );
        assert!(bounded("replica_local", Value::Number(f64::from(MAX_STALENESS_MS))).is_ok());
        for bad in [
            Value::Number(f64::from(MAX_STALENESS_MS) + 1.0),
            Value::Number(1.5),
            Value::Number(-1.0),
            text("250"),
        ] {
            assert!(bounded("replica_local", bad.clone()).is_err(), "{bad:?}");
        }
        assert!(bounded("linearizable", Value::Number(250.0)).is_err());
    }

    #[test]
    fn loader_rejects_bad_manifests() {
        // Unknown reference target.
        let mut bad = manifest();
        if let Value::Object(root) = &mut bad
            && let Some(Value::Array(collections)) = root.get_mut("collections")
            && let Value::Object(tasks) = &mut collections[0]
            && let Some(Value::Array(references)) = tasks.get_mut("references")
            && let Value::Object(reference) = &mut references[0]
        {
            reference.insert("targets".to_string(), Value::Array(vec![text("missing")]));
        }
        assert!(load_module(&bad).is_err());
        // Missing covering index.
        let mut bad = manifest();
        if let Value::Object(root) = &mut bad
            && let Some(Value::Array(collections)) = root.get_mut("collections")
            && let Value::Object(tasks) = &mut collections[0]
        {
            tasks.insert("indexes".to_string(), Value::Array(vec![]));
        }
        assert!(load_module(&bad).is_err());
        // set_null on a key place.
        let mut bad = manifest();
        if let Value::Object(root) = &mut bad
            && let Some(Value::Array(collections)) = root.get_mut("collections")
            && let Value::Object(tasks) = &mut collections[0]
            && let Some(Value::Array(references)) = tasks.get_mut("references")
            && let Value::Object(reference) = &mut references[0]
        {
            reference.insert("place".to_string(), text("key"));
            reference.insert("on_delete".to_string(), text("set_null"));
        }
        assert!(load_module(&bad).is_err());
        // Poly reference without a discriminator-leading index.
        let mut bad = manifest();
        if let Value::Object(root) = &mut bad
            && let Some(Value::Array(collections)) = root.get_mut("collections")
            && let Value::Object(tasks) = &mut collections[0]
            && let Some(Value::Array(references)) = tasks.get_mut("references")
            && let Value::Object(reference) = &mut references[0]
        {
            reference.insert(
                "targets".to_string(),
                Value::Array(vec![text("tasks"), text("posts")]),
            );
            reference.insert("discriminator".to_string(), text("target_type"));
        }
        assert!(load_module(&bad).is_err());
        // Not an object.
        assert!(load_module(&Value::Null).is_err());
    }

    #[test]
    fn poly_references_need_disc_leading_indexes() {
        // The same poly reference loads once the covering index leads
        // with the discriminator plus key fields.
        let mut good = manifest();
        if let Value::Object(root) = &mut good
            && let Some(Value::Array(collections)) = root.get_mut("collections")
            && let Value::Object(tasks) = &mut collections[0]
        {
            if let Some(Value::Array(references)) = tasks.get_mut("references")
                && let Value::Object(reference) = &mut references[0]
            {
                reference.insert(
                    "targets".to_string(),
                    Value::Array(vec![text("tasks"), text("posts")]),
                );
                reference.insert("discriminator".to_string(), text("target_type"));
            }
            if let Some(Value::Array(indexes)) = tasks.get_mut("indexes")
                && let Value::Object(index) = &mut indexes[0]
            {
                index.insert(
                    "fields".to_string(),
                    Value::Array(vec![text("target_type"), text("owner")]),
                );
            }
        }
        let module = load_module(&good).unwrap();
        let tasks = module.collection("tasks").unwrap();
        assert_eq!(tasks.references[0].targets.len(), 2);
    }

    #[test]
    fn schemas_load_all_kinds() {
        let mut nullable = schema_enc("nullable");
        nullable.insert("of".to_string(), Value::Object(schema_enc("string")));
        let schema = load_schema(&Value::Object(nullable)).unwrap();
        assert!(schema.is(&Value::Null));

        let mut str_enum = schema_enc("enum");
        str_enum.insert(
            "variants".to_string(),
            Value::Array(vec![text("a"), text("b")]),
        );
        let schema = load_schema(&Value::Object(str_enum)).unwrap();
        assert!(schema.is(&text("a")));
        assert!(!schema.is(&text("c")));

        let mut bounded = schema_enc("string");
        bounded.insert("min".to_string(), Value::Number(1.0));
        bounded.insert("max".to_string(), Value::Number(4.0));
        let schema = load_schema(&Value::Object(bounded)).unwrap();
        assert!(!schema.is(&text("toolong")));
    }
}
