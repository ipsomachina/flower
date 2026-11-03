//! Application assembly: [`App`], methods, definitions, collections,
//! references, triggers, transactions and components.
//!
//! Every operation is declared once, with everything about it: a
//! [`Method`] carries its kind, read consistency, HTTP exposure, access,
//! export and declared deadline. An [`App`] is one flat list of
//! [`Component`]s (methods, derived values, collections, triggers,
//! queues, the authorization hook, composite components such as an
//! external value or an interface provider); the manifest's definitions,
//! HTTP aliases, exports and per-method access are generated from them.
//! The `#[query]`, `#[mutation]`, `#[derived]` and `#[collection]`
//! attributes build these descriptors from typed Rust items.

use alloc::string::String;
use alloc::vec::Vec;

use crate::error::{Result, codes, fail};
use crate::schema::{Schema, v};
use crate::typed::FlowerType;
use crate::value::Value;

/// Read consistency of a query definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Linearizable read.
    Linearizable,
    /// Replica-local read with explicitly unbounded lag.
    ReplicaLocal,
    /// Replica-local read at most this many milliseconds stale (at most
    /// 2^31 - 1): a replica past it proves a fresh read first. A caller
    /// may only tighten it.
    ReplicaLocalWithin(u32),
}

/// Raw method callback: arguments in, result out.
pub type RawCallback = for<'c, 'r> fn(&'c mut crate::ctx::Ctx<'c, 'r>, Value) -> Result<Value>;

/// Raw derived callback: same shape as [`RawCallback`]; the host gates
/// derived invocations to the readable subset of operations.
pub type RawDerivedCallback =
    for<'c, 'r> fn(&'c mut crate::ctx::Ctx<'c, 'r>, Value) -> Result<Value>;

/// Raw trigger callback: change in, no result out.
pub type RawTriggerCallback =
    for<'c, 'r> fn(&'c mut crate::ctx::Ctx<'c, 'r>, &Change) -> Result<()>;

/// Raw aggregate reducer: a [`ReduceCall`][crate::index::ReduceCall]
/// in, a [`Reduced`][crate::index::Reduced] out. It gets no context:
/// the host refuses every host call while it runs, so a reducer is a
/// pure function of the accumulator and the row changes.
pub type RawReduceCallback = fn(Value) -> Result<Value>;

/// Raw maintenance-task `due` callback: when the task next has work,
/// in epoch milliseconds, or `None` when it has none. It runs as a
/// query: reads only.
pub type RawDueCallback = for<'c, 'r> fn(&'c mut crate::ctx::Ctx<'c, 'r>) -> Result<Option<f64>>;

/// Raw maintenance-task `run` callback: one bounded unit of work, as a
/// mutation.
pub type RawRunCallback = for<'c, 'r> fn(&'c mut crate::ctx::Ctx<'c, 'r>) -> Result<Value>;

/// Raw maintenance-task error callback: a failed run's failure in, as
/// a mutation over the state the run saw (its writes rolled back).
pub type RawTaskErrorCallback =
    for<'c, 'r> fn(&'c mut crate::ctx::Ctx<'c, 'r>, &TaskFailure) -> Result<Value>;

/// Image-preparation callback: no invocation, configuration only.
pub type InitCallback = fn() -> Result<()>;

/// Named query or mutation descriptor: everything about one operation,
/// declared in one place.
///
/// A method answers over HTTP under its name only when it says so
/// ([`http`][Self::http]), as v1's `http` allowlist decided: timer and
/// queue handlers, trigger targets and methods other modules call need
/// no alias, and a method nobody listed is never public by accident. It
/// is private to its module unless [`export`][Self::export]ed, and
/// decided by the authorization component's default unless it declares
/// its own [`access`][Self::access].
#[derive(Clone, Copy, Debug)]
pub struct Method {
    /// Stable method name: the definition and its HTTP alias.
    pub name: &'static str,
    /// Query or mutation.
    pub kind: MethodKind,
    /// Raw callback.
    pub callback: RawCallback,
    /// Argument contract, checked on dispatch.
    pub args: Schema,
    /// Result contract other modules may rely on; the host checks it
    /// where another module calls.
    pub result: Schema,
    /// Business-failure contract over `{code, message, details?}`;
    /// checked where another module calls.
    pub errors: Schema,
    /// Read consistency; queries only.
    pub consistency: Consistency,
    /// Whether callers reach it over HTTP under its name.
    pub http: bool,
    /// Who may call it, decided by the module's authorization component;
    /// `None` takes the component's default. Declaring access without an
    /// authorization component fails the manifest.
    pub access: Option<crate::auth::Access>,
    /// Which other modules may call it.
    pub export: Export,
    /// The deadline the method declares for itself, in milliseconds;
    /// `None` takes the operator's default. The operator's ceiling and
    /// the caller's own deadline can only shorten it.
    pub timeout_ms: Option<u32>,
}

impl Method {
    const fn new(name: &'static str, kind: MethodKind, callback: RawCallback) -> Self {
        Method {
            name,
            kind,
            callback,
            args: v::json(),
            result: v::json(),
            errors: v::json(),
            consistency: Consistency::Linearizable,
            http: false,
            access: None,
            export: Export::Private,
            timeout_ms: None,
        }
    }

    /// Declare a linearizable query, off HTTP and private, with
    /// unconstrained arguments, result and errors.
    pub const fn query(name: &'static str, callback: RawCallback) -> Self {
        Method::new(name, MethodKind::Query, callback)
    }

    /// Declare a mutation, off HTTP and private, with unconstrained
    /// arguments, result and errors.
    pub const fn mutation(name: &'static str, callback: RawCallback) -> Self {
        Method::new(name, MethodKind::Mutation, callback)
    }

    /// Set the argument contract.
    pub const fn args(mut self, args: Schema) -> Self {
        self.args = args;
        self
    }

    /// Set the result contract.
    pub const fn result(mut self, result: Schema) -> Self {
        self.result = result;
        self
    }

    /// Set the business-failure contract.
    pub const fn errors(mut self, errors: Schema) -> Self {
        self.errors = errors;
        self
    }

    /// Set a query's read consistency.
    pub const fn consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// The same method under another name: a twin of one callback, such
    /// as a replica-local read of a linearizable query.
    pub const fn named(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    /// Let callers reach the method over HTTP under its name.
    pub const fn http(mut self) -> Self {
        self.http = true;
        self
    }

    /// Set whether the method answers over HTTP: for descriptors other
    /// macros build ([`external!`](crate::external!)).
    #[doc(hidden)]
    pub const fn with_http(mut self, http: bool) -> Self {
        self.http = http;
        self
    }

    /// Declare who may call the method.
    pub const fn access(mut self, access: crate::auth::Access) -> Self {
        self.access = Some(access);
        self
    }

    /// Export the method to every other module.
    pub const fn export(mut self) -> Self {
        self.export = Export::All;
        self
    }

    /// Export the method to these module identities only (never import
    /// slots). An empty list admits no module.
    pub const fn export_to(mut self, modules: &'static [&'static str]) -> Self {
        self.export = Export::Only(modules);
        self
    }

    /// Declare the method's own deadline, in milliseconds.
    pub const fn timeout_ms(mut self, timeout_ms: u32) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Whether this is a query.
    pub const fn is_query(&self) -> bool {
        matches!(self.kind, MethodKind::Query)
    }
}

impl Component for Method {
    fn definition(&'static self) -> Option<Definition> {
        Some(Definition::Method(self))
    }
}

/// Which other modules may call a method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Export {
    /// None: the method is private to its module.
    Private,
    /// Every other module.
    All,
    /// These module identities only (never import slots); an empty list
    /// admits none.
    Only(&'static [&'static str]),
}

/// A method with its types: what `#[query]` and `#[mutation]` declare.
/// It is the component apps list, and typed callers take its argument
/// type `A`, result type `T` and declared error type `E` from it: timers
/// (`Timers::after(ctx, id, delay, &FINISH_BAKING, &FinishBakingArgs { .. })`)
/// and the native client (`client.call(db, module, &ORDER, &args)`).
/// It dereferences to its [`Method`].
pub struct Op<A, T, E = crate::error::Failure> {
    /// The untyped descriptor.
    pub method: Method,
    _types: Types<A, T, E>,
}

/// The types an [`Op`] carries, owning none of them.
type Types<A, T, E> = core::marker::PhantomData<fn() -> (A, T, E)>;

impl<A, T, E> Op<A, T, E> {
    /// Type a method descriptor.
    pub const fn new(method: Method) -> Self {
        Op {
            method,
            _types: core::marker::PhantomData,
        }
    }

    /// The same operation under another name, with the same types: a
    /// twin of one callback.
    ///
    /// ```ignore
    /// static SHOP_LOCAL: Op<ShopArgs, Summary> = SHOP
    ///     .named("pizza.shop.local")
    ///     .consistency(Consistency::ReplicaLocal);
    /// ```
    pub const fn named(mut self, name: &'static str) -> Self {
        self.method = self.method.named(name);
        self
    }

    /// The same operation with another read consistency.
    pub const fn consistency(mut self, consistency: Consistency) -> Self {
        self.method = self.method.consistency(consistency);
        self
    }
}

impl<A, T, E> Clone for Op<A, T, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<A, T, E> Copy for Op<A, T, E> {}

impl<A, T, E> core::ops::Deref for Op<A, T, E> {
    type Target = Method;

    fn deref(&self) -> &Method {
        &self.method
    }
}

impl<A, T, E> core::fmt::Debug for Op<A, T, E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.method.fmt(f)
    }
}

impl<A: 'static, T: 'static, E: 'static> Component for Op<A, T, E> {
    fn definition(&'static self) -> Option<Definition> {
        Some(Definition::Method(&self.method))
    }
}

/// Registered definition: query or mutation, derived value or
/// transaction.
#[derive(Clone, Copy, Debug)]
pub enum Definition {
    /// Query or mutation.
    Method(&'static Method),
    /// Derived value.
    Derived(&'static Derived),
    /// Cross-database transaction.
    Transaction(&'static Transaction),
}

impl Definition {
    /// Stable definition name.
    pub const fn name(&self) -> &'static str {
        match self {
            Definition::Method(method) => method.name,
            Definition::Derived(derived) => derived.name,
            Definition::Transaction(transaction) => transaction.name,
        }
    }
}

/// Root retention of a derived definition.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Materialize {
    /// Never retain roots.
    Never,
    /// Always retain roots.
    Always,
    /// Retain one root per row of the source collection.
    Each(&'static Collection),
}

/// Derived value descriptor.
#[derive(Clone, Copy, Debug)]
pub struct Derived {
    /// Stable definition name.
    pub name: &'static str,
    /// Raw computation.
    pub callback: RawDerivedCallback,
    /// Argument contract, checked on dispatch.
    pub args: Schema,
    /// Root retention.
    pub materialize: Materialize,
    /// Who may read it from a method, over `principal`, `args` and `now`;
    /// `None` leaves it open. Derived values compute with definer rights,
    /// past collection policies, so expose guarded data deliberately.
    pub access: Option<fn() -> crate::access::Rule>,
    /// Result contract other modules may rely on.
    pub result: Schema,
    /// Business-failure contract over `{code, message, details?}`.
    pub errors: Schema,
    /// Delta maintenance, for an aggregate over an index group: the
    /// host hands each cached value its group's row changes through
    /// the reducer instead of building the group again.
    pub aggregate: Option<Aggregation>,
    /// Whether other modules may read it.
    pub export: bool,
}

/// How the host maintains an aggregate: one value per equality group
/// of `index` over `collection`, read as `{"group": [<one value per
/// indexed field>]}`. The derived callback builds a group from its
/// rows; afterwards, every write that changes rows of a group whose
/// value is cached hands the reducer the accumulator and those rows'
/// changes (`old` image out of the group, `new` image into it), in the
/// writing transaction, so the value never builds again while the
/// reducer keeps it. A reducer that cannot take a change out (its
/// accumulator is not invertible) answers
/// [`Reduced::Rebuild`][crate::index::Reduced::Rebuild], and a failing
/// one fails the same way: the group then builds from its rows.
#[derive(Clone, Copy, Debug)]
pub struct Aggregation {
    /// The source collection, the calling module's.
    pub collection: &'static str,
    /// The index whose equality groups the values follow.
    pub index: &'static str,
    /// The reducer.
    pub reduce: RawReduceCallback,
    /// Keeps cached values across deployments while it stays the same
    /// (1 to 128 bytes, no control characters) and the source and index
    /// do too; `None` builds every group again under new code. Change it
    /// whenever the build or the reducer would compute differently.
    pub version: Option<&'static str>,
}

impl Derived {
    /// Declare a derived value with unconstrained arguments and no retention.
    pub const fn new(name: &'static str, callback: RawDerivedCallback) -> Self {
        Derived {
            name,
            callback,
            args: v::json(),
            materialize: Materialize::Never,
            access: None,
            result: v::json(),
            errors: v::json(),
            aggregate: None,
            export: false,
        }
    }

    /// Let other modules read the value.
    pub const fn export(mut self) -> Self {
        self.export = true;
        self
    }

    /// Maintain the value by deltas, as an aggregate.
    pub const fn aggregate(mut self, aggregation: Aggregation) -> Self {
        self.aggregate = Some(aggregation);
        self
    }

    /// Set the result contract.
    pub const fn result(mut self, result: Schema) -> Self {
        self.result = result;
        self
    }

    /// Set the business-failure contract.
    pub const fn errors(mut self, errors: Schema) -> Self {
        self.errors = errors;
        self
    }

    /// Set the access rule methods must satisfy to read this value.
    pub const fn access(mut self, rule: fn() -> crate::access::Rule) -> Self {
        self.access = Some(rule);
        self
    }

    /// Set the argument contract.
    pub const fn args(mut self, args: Schema) -> Self {
        self.args = args;
        self
    }

    /// Set root retention.
    pub const fn materialize(mut self, materialize: Materialize) -> Self {
        self.materialize = materialize;
        self
    }
}

impl Component for Derived {
    fn definition(&'static self) -> Option<Definition> {
        Some(Definition::Derived(self))
    }
}

/// Transaction planner: arguments in, instruction list out. Planning runs
/// without a context: no database reads, no network, no branching on results.
pub type PlannerFn = fn(Value) -> Result<Plan>;

/// Cross-database transaction descriptor.
#[derive(Clone, Copy, Debug)]
pub struct Transaction {
    /// Stable transaction name.
    pub name: &'static str,
    /// Pure planner.
    pub planner: PlannerFn,
    /// Argument contract, checked on dispatch.
    pub args: Schema,
    /// Whether callers reach it over HTTP under its name.
    pub http: bool,
    /// Who may call it, decided by the module's authorization component;
    /// `None` takes the component's default.
    pub access: Option<crate::auth::Access>,
}

impl Transaction {
    /// Declare a transaction with unconstrained arguments, off HTTP.
    pub const fn new(name: &'static str, planner: PlannerFn) -> Self {
        Transaction {
            name,
            planner,
            args: v::json(),
            http: false,
            access: None,
        }
    }

    /// Let callers reach the transaction over HTTP under its name.
    pub const fn http(mut self) -> Self {
        self.http = true;
        self
    }

    /// Declare who may call the transaction.
    pub const fn access(mut self, access: crate::auth::Access) -> Self {
        self.access = Some(access);
        self
    }

    /// Set the argument contract.
    pub const fn args(mut self, args: Schema) -> Self {
        self.args = args;
        self
    }
}

impl Component for Transaction {
    fn definition(&'static self) -> Option<Definition> {
        Some(Definition::Transaction(self))
    }
}

/// Transaction participant: a named partition or a group's root database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    kind: TargetKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TargetKind {
    Partition(String),
    Group(String),
}

impl Target {
    /// Address a named partition. Names must be non-empty.
    pub fn partition(name: &str) -> Result<Target> {
        if name.is_empty() {
            return Err(fail(codes::INVALID_VALUE, "empty partition name"));
        }
        Ok(Target {
            kind: TargetKind::Partition(String::from(name)),
        })
    }

    /// Address a group's root database. Names must be non-empty.
    pub fn group(name: &str) -> Result<Target> {
        if name.is_empty() {
            return Err(fail(codes::INVALID_VALUE, "empty group name"));
        }
        Ok(Target {
            kind: TargetKind::Group(String::from(name)),
        })
    }

    /// Whether this target addresses a partition (`true`) or group root.
    pub fn is_partition(&self) -> bool {
        match self.kind {
            TargetKind::Partition(_) => true,
            TargetKind::Group(_) => false,
        }
    }

    /// The addressed name.
    pub fn name(&self) -> &str {
        match &self.kind {
            TargetKind::Partition(name) => name,
            TargetKind::Group(name) => name,
        }
    }

    /// Build one call instruction against an explicit HTTP alias.
    pub fn call(&self, alias: &str, args: Value) -> Result<Call> {
        if alias.is_empty() {
            return Err(fail(codes::INVALID_VALUE, "empty method alias"));
        }
        Ok(Call {
            target: self.clone(),
            alias: String::from(alias),
            args,
        })
    }
}

/// One planned participant call.
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    /// Called participant.
    pub target: Target,
    /// Explicit HTTP alias on the participant.
    pub alias: String,
    /// Call arguments.
    pub args: Value,
}

/// Planned instructions plus an optional typed plan value. A plan is a list
/// of instructions, not executed calls; order is significant.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan<V = Value> {
    /// Calls in execution order.
    pub calls: Vec<Call>,
    /// Optional plan value. `None` differs from explicit null.
    pub value: Option<V>,
}

/// Committed transaction outcome: raw participant results plus the optional
/// typed plan value.
#[derive(Clone, Debug, PartialEq)]
pub struct Committed<V = Value> {
    /// Raw results in call order.
    pub results: Vec<Value>,
    /// Typed plan value.
    pub value: Option<V>,
}

impl<V: FlowerType> Committed<V> {
    /// Decode a committed outcome object `{"results": [...], "value": ...}`.
    /// A missing `value` member decodes to `None`; a present member —
    /// including explicit null — decodes to `Some`.
    pub fn decode(raw: Value) -> Result<Self> {
        let map = match raw {
            Value::Object(map) => map,
            _ => return Err(fail(codes::TYPE_MISMATCH, "expected committed object")),
        };
        let results = match map.get("results") {
            Some(Value::Array(results)) => results.clone(),
            _ => return Err(fail(codes::TYPE_MISMATCH, "committed results not an array")),
        };
        let value = match map.get("value") {
            None => None,
            Some(value) => Some(V::decode(value.clone())?),
        };
        Ok(Committed { results, value })
    }
}

/// Source-change hook inside a mutation.
#[derive(Clone, Copy, Debug)]
pub struct Trigger {
    /// Stable trigger name.
    pub name: &'static str,
    /// Source collection name.
    pub source: &'static str,
    /// Raw hook.
    pub callback: RawTriggerCallback,
}

impl Trigger {
    /// Declare a trigger on a source collection.
    pub const fn new(
        name: &'static str,
        source: &'static str,
        callback: RawTriggerCallback,
    ) -> Self {
        Trigger {
            name,
            source,
            callback,
        }
    }
}

impl Component for Trigger {
    fn trigger(&self) -> Option<&Trigger> {
        Some(self)
    }
}

/// One net source change delivered to triggers.
#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    /// Changed row key.
    pub key: Value,
    /// Value before the change, if any.
    pub before: Option<Value>,
    /// Value after the change, if any.
    pub after: Option<Value>,
}

/// Anything an [`App`] lists: a method, derived value, transaction,
/// collection or trigger, a queue, task, retention hook, key
/// declaration, import or authorization hook, or a composite of them
/// (an external value, an interface provider).
///
/// Every accessor defaults to "nothing", so a component implements
/// only what it provides; a composite lists its parts through
/// [`components`][Self::components].
pub trait Component: Sync {
    /// A collection-scoped capability for request-time SQL.
    fn sql_endpoint(&self) -> Option<&crate::sql::SqlEndpoint> {
        None
    }
    /// A named, read-only SQL plan, validated against this module's catalog.
    fn sql(&self) -> Option<&crate::sql::SqlDef> {
        None
    }

    /// The collection this component declares, if any.
    fn collection(&self) -> Option<&Collection> {
        None
    }

    /// The definition this component declares, if any.
    fn definition(&'static self) -> Option<Definition> {
        None
    }

    /// The trigger this component declares, if any.
    fn trigger(&self) -> Option<&Trigger> {
        None
    }

    /// The components this one is made of, each visited once.
    fn components(&'static self, _visit: &mut dyn FnMut(&'static dyn Component)) {}

    /// Whether this component provides the module's authorization hook.
    fn authorizes(&self) -> bool {
        false
    }

    /// Run the authorization hook, if this component provides it.
    /// `declared` answers the access a method alias declares (`None`:
    /// the component's default). Reached through the vtable, so guests
    /// without one don't link it.
    fn authorize<'c, 'r>(
        &self,
        _ctx: &'c mut crate::ctx::Ctx<'c, 'r>,
        _request: Value,
        _declared: &dyn Fn(&str) -> Option<crate::auth::Access>,
    ) -> Option<Result<Value>> {
        None
    }

    /// The access the authorization component grants definitions that
    /// declare none of their own, by name: those other macros declare
    /// (an [`external!`](crate::external!) value's operations, an
    /// interface provider's methods).
    #[doc(hidden)]
    fn granted_access(&self) -> &[(&'static str, crate::auth::Access)] {
        &[]
    }

    /// What this component imports from another module, if anything.
    fn imports(&self) -> Option<&Import> {
        None
    }

    /// The deletion-retention hook this component provides, if any.
    fn retention(&self) -> Option<&Retention> {
        None
    }

    /// The maintenance task this component provides, if any.
    fn task(&self) -> Option<&Task> {
        None
    }

    /// Catalog key aliases this component uses, carried in the
    /// manifest's `keys` member. The host resolves a catalog key only
    /// for a module that declares it, within both the declared and the
    /// operator-granted usages.
    fn keys(&self) -> &[crate::crypto::KeyDeclaration] {
        &[]
    }

    /// The queue this component declares, if any: the host checks every
    /// enqueued payload and kept result against its contracts, and
    /// gives the first owner in its lines new work for its turn.
    fn queue(&self) -> Option<&crate::queue::QueueDef> {
        None
    }
}

/// A static array of components is one component: group parts once and
/// list the group.
impl<const N: usize> Component for [&'static dyn Component; N] {
    fn components(&'static self, visit: &mut dyn FnMut(&'static dyn Component)) {
        for component in self {
            visit(*component);
        }
    }
}

/// One successfully deleted row, as a retention hook sees it: the whole
/// stored row, fields hidden from the deleting caller included.
#[derive(Clone, Debug, PartialEq)]
pub struct DeletedRow {
    /// Source collection name.
    pub collection: String,
    /// Decoded key.
    pub key: Value,
    /// The removed row.
    pub value: Value,
    /// The deleting invocation's time, milliseconds since the epoch.
    pub deleted_at: f64,
}

/// Raw retention callback: one deleted row in, no result out.
pub type RetentionCallback =
    for<'c, 'r> fn(&'c mut crate::ctx::Ctx<'c, 'r>, &DeletedRow) -> Result<()>;

/// A deletion-retention hook, listed among the app's components: the host hands
/// it every successful delete of a `source` row (delete-then-recreate
/// included, unlike net-change triggers) inside the deleting transaction,
/// with definer rights and under its own savepoint; a failure fails the
/// transaction. Deletes of missing rows, deletes an access policy skips
/// and permanent deletes (`delete_permanently`, and what they cascade
/// into) emit nothing. The hook chooses where the row goes; there is no
/// implicit restore.
#[derive(Clone, Copy, Debug)]
pub struct Retention {
    /// Stable hook name: the host runs it as a mutation of this name.
    pub name: &'static str,
    /// Source collection name.
    pub source: &'static str,
    /// The hook.
    pub retain: RetentionCallback,
}

impl Component for Retention {
    fn retention(&self) -> Option<&Retention> {
        Some(self)
    }
}

/// A failed maintenance run, as the task's error callback sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskFailure {
    /// The run's failure: its business failure, or the budget or trap
    /// that ended it.
    pub error: crate::error::Failure,
    /// When the run failed, epoch milliseconds: the time it ran at.
    pub failed_at: f64,
}

/// Maintenance work, listed among the app's components: the host's leader runs
/// it whenever it is due, as a replicated mutation, so it runs once
/// whichever member leads and survives a change of leader.
///
/// `due` answers when the task next has work (epoch milliseconds), or
/// `None` when it has none; it runs as a query and must depend only on
/// data and time. `run` performs one bounded unit of work as a mutation
/// whose own transaction first checks `due` again, so a unit never runs
/// twice for one piece of work; the task stays eligible while `due`
/// stays in the past. A failed run rolls its writes back. With
/// `on_error`, the host then hands it the failure in a mutation of its
/// own; without it, or when it fails too, the task backs off for one
/// second, doubling up to a minute, while other tasks stay eligible.
/// Its failure count, retry time and last failure are kept by the host
/// (operators read them in the module listing) until the next success.
#[derive(Clone, Copy, Debug)]
pub struct Task {
    /// Stable task name, unique in the module among definitions and
    /// hooks.
    pub name: &'static str,
    /// When the task next has work.
    pub due: RawDueCallback,
    /// One unit of work.
    pub run: RawRunCallback,
    /// Handles a failed run instead of backing off, if set.
    pub on_error: Option<RawTaskErrorCallback>,
}

impl Task {
    /// Declare a task without an error callback.
    pub const fn new(name: &'static str, due: RawDueCallback, run: RawRunCallback) -> Self {
        Task {
            name,
            due,
            run,
            on_error: None,
        }
    }

    /// Handle failed runs with `on_error` instead of backing off.
    pub const fn on_error(mut self, on_error: RawTaskErrorCallback) -> Self {
        self.on_error = Some(on_error);
        self
    }
}

impl Component for Task {
    fn task(&self) -> Option<&Task> {
        Some(self)
    }
}

/// The argument, result and business-failure contract of an imported
/// method or derived value, as the importer expects it. Compatibility is
/// structural: the provider must accept every argument the importer
/// sends and produce only results and failures the importer accepts.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Contract {
    /// Arguments the importer sends.
    pub args: Schema,
    /// Results the importer accepts.
    pub result: Schema,
    /// Business failures (`{code, message, details?}`) the importer
    /// accepts.
    pub errors: Schema,
}

impl Contract {
    /// Accept anything both ways: arguments must then be accepted as
    /// `json` by the provider, results and failures are unconstrained.
    pub const ANY: Contract = Contract {
        args: v::json(),
        result: v::json(),
        errors: v::json(),
    };
}

/// Kind of an imported method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MethodKind {
    /// Query.
    Query,
    /// Mutation.
    Mutation,
}

/// One imported method.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ImportedMethod {
    /// Exported name in the provider.
    pub name: &'static str,
    /// Expected kind.
    pub kind: MethodKind,
    /// Expected contract; `None` is an explicit untyped import, linked
    /// without a compatibility proof (the provider's own contract still
    /// holds at runtime).
    pub contract: Option<Contract>,
}

/// One imported derived value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ImportedDerived {
    /// Exported name in the provider.
    pub name: &'static str,
    /// Expected contract; `None` is untyped.
    pub contract: Option<Contract>,
}

/// One imported collection: read-only to the importer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ImportedCollection {
    /// Exported name in the provider.
    pub name: &'static str,
    /// Expected key schema; key spaces must agree both ways.
    pub key: Schema,
    /// Rows the importer accepts.
    pub row: Schema,
}

/// What a module imports from one other module. `module` names a
/// concrete module, or a logical slot `$name` that each deployment binds
/// to a module (the deployed manifest's `bindings`), so one binary can
/// run against several providers. Deployment fails `MODULE_CONFLICT`
/// unless every import is exported by the bound module, to this module
/// when the provider names callers, with compatible contracts; and a
/// provider redeploy that would break a deployed importer fails alike.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Import {
    /// Concrete module, or `$slot`.
    pub module: &'static str,
    /// Imported collections.
    pub collections: &'static [ImportedCollection],
    /// Imported derived values.
    pub derived: &'static [ImportedDerived],
    /// Imported methods.
    pub methods: &'static [ImportedMethod],
}

impl Component for Import {
    fn imports(&self) -> Option<&Import> {
        Some(self)
    }
}

/// Index descriptor: name plus indexed wire-field names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexDef {
    /// Index name.
    pub name: &'static str,
    /// Indexed fields.
    pub fields: &'static [&'static str],
}

impl IndexDef {
    /// Declare an index over wire fields.
    pub const fn new(name: &'static str, fields: &'static [&'static str]) -> Self {
        IndexDef { name, fields }
    }
}

/// Raw collection descriptor.
#[derive(Clone, Copy, Debug)]
pub struct Collection {
    /// Collection name.
    pub name: &'static str,
    /// Key contract.
    pub key_schema: Schema,
    /// Value contract.
    pub value_schema: Schema,
    /// Index descriptors.
    pub indexes: &'static [IndexDef],
    /// Foreign keys out of this collection.
    pub references: &'static [Reference],
    /// Access policy for callers, if any; `None` leaves the collection
    /// open.
    pub access: Option<fn() -> crate::access::Policy>,
    /// Whether other modules may read it (never write: changes go
    /// through the owner's exported mutations).
    pub export: bool,
}

// Access policies compare by the policy they build: function addresses
// say nothing about equality.
impl PartialEq for Collection {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.key_schema == other.key_schema
            && self.value_schema == other.value_schema
            && self.indexes == other.indexes
            && self.references == other.references
            && self.access.map(|policy| policy()) == other.access.map(|policy| policy())
            && self.export == other.export
    }
}

impl Collection {
    /// Declare a collection with unconstrained key/value contracts.
    pub const fn new(name: &'static str) -> Self {
        Collection {
            name,
            key_schema: v::json(),
            value_schema: v::json(),
            indexes: &[],
            references: &[],
            access: None,
            export: false,
        }
    }

    /// Let other modules read the collection.
    pub const fn export(mut self) -> Self {
        self.export = true;
        self
    }

    /// Set the access policy enforced on callers' operations.
    pub const fn access(mut self, policy: fn() -> crate::access::Policy) -> Self {
        self.access = Some(policy);
        self
    }

    /// Set the key contract.
    pub const fn key_schema(mut self, schema: Schema) -> Self {
        self.key_schema = schema;
        self
    }

    /// Set the value contract.
    pub const fn value_schema(mut self, schema: Schema) -> Self {
        self.value_schema = schema;
        self
    }

    /// Set the index descriptors.
    pub const fn indexes(mut self, indexes: &'static [IndexDef]) -> Self {
        self.indexes = indexes;
        self
    }

    /// Set foreign keys out of this collection.
    pub const fn references(mut self, references: &'static [Reference]) -> Self {
        self.references = references;
        self
    }
}

impl Component for Collection {
    fn collection(&self) -> Option<&Collection> {
        Some(self)
    }
}

/// Where a foreign key lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Place {
    /// Named value fields.
    Fields(&'static [&'static str]),
    /// The whole key.
    Key,
    /// A key prefix.
    KeyPrefix,
}

/// Delete behavior of a foreign key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnDelete {
    /// Block deletes of referenced rows.
    Restrict,
    /// Delete referencing rows.
    Cascade,
    /// Clear referencing fields.
    SetNull,
}

/// Foreign-key targets: one collection, or several behind a discriminator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReferenceTargets {
    /// Single target.
    One(&'static Collection),
    /// Many targets; the discriminator field names one per row.
    Poly {
        /// Declared targets.
        targets: &'static [&'static Collection],
        /// Discriminator field holding a target name.
        discriminator: &'static str,
    },
}

/// Foreign key descriptor.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Reference {
    /// Targets.
    pub targets: ReferenceTargets,
    /// Key placement.
    pub place: Place,
    /// Delete behavior.
    pub on_delete: OnDelete,
}

impl Reference {
    /// Declare a single-target reference with `Restrict` behavior.
    pub const fn new(target: &'static Collection, place: Place) -> Self {
        Reference {
            targets: ReferenceTargets::One(target),
            place,
            on_delete: OnDelete::Restrict,
        }
    }

    /// Declare a polymorphic reference with `Restrict` behavior. Only
    /// [`Place::Fields`] places can be polymorphic.
    pub const fn poly(
        targets: &'static [&'static Collection],
        discriminator: &'static str,
        place: Place,
    ) -> Self {
        Reference {
            targets: ReferenceTargets::Poly {
                targets,
                discriminator,
            },
            place,
            on_delete: OnDelete::Restrict,
        }
    }

    /// Set delete behavior.
    pub const fn on_delete(mut self, on_delete: OnDelete) -> Self {
        self.on_delete = on_delete;
        self
    }
}

/// An application: one flat list of components, plus an optional
/// image-preparation callback.
///
/// ```ignore
/// pub static APP: App = App::new(&[&Shops, &Orders, &ORDER, &SHOP, &DELIVERIES, &AUTH]);
/// ```
///
/// `App` deliberately implements neither `Debug` nor `PartialEq`: its
/// component list is trait objects, which support neither bound.
#[derive(Clone, Copy)]
pub struct App {
    /// Every component, composites included.
    pub components: &'static [&'static dyn Component],
    /// Image-preparation callback, if any.
    pub init: Option<InitCallback>,
}

impl App {
    /// Assemble an application from its components.
    pub const fn new(components: &'static [&'static dyn Component]) -> Self {
        App {
            components,
            init: None,
        }
    }

    /// Run `init` once while the image is prepared.
    pub const fn init(mut self, init: InitCallback) -> Self {
        self.init = Some(init);
        self
    }

    /// Visit every component, composites before their parts, in
    /// declaration order.
    pub fn each(&self, visit: &mut dyn FnMut(&'static dyn Component)) {
        fn walk(component: &'static dyn Component, visit: &mut dyn FnMut(&'static dyn Component)) {
            visit(component);
            component.components(&mut |part| walk(part, visit));
        }
        for component in self.components.iter() {
            walk(*component, visit);
        }
    }

    /// Every component, flattened, in declaration order.
    pub fn flattened(&self) -> Vec<&'static dyn Component> {
        let mut all = Vec::new();
        self.each(&mut |component| all.push(component));
        all
    }

    /// Every declared collection.
    pub fn collections(&self) -> Vec<&'static Collection> {
        let mut found = Vec::new();
        self.each(&mut |component| {
            if let Some(collection) = component.collection() {
                found.push(collection);
            }
        });
        found
    }

    /// Every declared definition.
    pub fn definitions(&self) -> Vec<Definition> {
        let mut found = Vec::new();
        self.each(&mut |component| {
            if let Some(definition) = component.definition() {
                found.push(definition);
            }
        });
        found
    }

    /// Every declared trigger.
    pub fn triggers(&self) -> Vec<&'static Trigger> {
        let mut found = Vec::new();
        self.each(&mut |component| {
            if let Some(trigger) = component.trigger() {
                found.push(trigger);
            }
        });
        found
    }

    /// The definition named `name`, if any.
    pub fn definition(&self, name: &str) -> Option<Definition> {
        let mut found = None;
        self.each(&mut |component| {
            if found.is_none()
                && let Some(definition) = component.definition()
                && definition.name() == name
            {
                found = Some(definition);
            }
        });
        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;

    static POSTS: Collection = Collection::new("posts");
    static PHOTOS: Collection = Collection::new("photos");
    static TARGETS: [&Collection; 2] = [&POSTS, &PHOTOS];

    const SINGLE: Reference = Reference::new(&POSTS, Place::Fields(&["parent"]));
    const POLY: Reference = Reference::poly(&TARGETS, "target_type", Place::Fields(&["target_id"]));

    #[test]
    fn references_construct_in_const_context() {
        assert_eq!(SINGLE.on_delete, OnDelete::Restrict);
        assert_eq!(
            SINGLE.on_delete(OnDelete::Cascade).on_delete,
            OnDelete::Cascade
        );
        match POLY.targets {
            ReferenceTargets::Poly {
                targets,
                discriminator,
            } => {
                assert_eq!(targets.len(), 2);
                assert_eq!(discriminator, "target_type");
            }
            ReferenceTargets::One(_) => panic!("expected poly targets"),
        }
    }

    #[test]
    fn targets_validate_names() {
        assert!(Target::partition("").is_err());
        assert!(Target::group("").is_err());
        let target = Target::partition("acme").unwrap();
        assert!(target.is_partition());
        assert_eq!(target.name(), "acme");
        assert!(target.call("", Value::Null).is_err());
        let call = target.call("put", Value::Null).unwrap();
        assert_eq!(call.alias, "put");
    }

    #[test]
    fn committed_distinguishes_missing_from_null() {
        let mut missing = alloc::collections::BTreeMap::new();
        missing.insert(String::from("results"), Value::Array(alloc::vec![]));
        let decoded = Committed::<Value>::decode(Value::Object(missing)).unwrap();
        assert_eq!(decoded.value, None);

        let mut explicit = alloc::collections::BTreeMap::new();
        explicit.insert(String::from("results"), Value::Array(alloc::vec![]));
        explicit.insert(String::from("value"), Value::Null);
        let decoded = Committed::<Value>::decode(Value::Object(explicit)).unwrap();
        assert_eq!(decoded.value, Some(Value::Null));
    }

    #[test]
    fn app_assembles_from_one_component_list() {
        fn echo<'c, 'r>(_: &'c mut crate::ctx::Ctx<'c, 'r>, args: Value) -> Result<Value> {
            Ok(args)
        }
        static ECHO: Method = Method::query("echo", echo);
        struct Pair;
        impl Component for Pair {
            fn components(&'static self, visit: &mut dyn FnMut(&'static dyn Component)) {
                visit(&PHOTOS);
                visit(&ECHO);
            }
        }
        static PAIR: Pair = Pair;
        static APP: App = App::new(&[&POSTS, &PAIR]);
        let names: Vec<&str> = APP.collections().iter().map(|c| c.name).collect();
        assert_eq!(names, ["posts", "photos"]);
        assert!(matches!(APP.definition("echo"), Some(Definition::Method(m)) if m.is_query()));
        assert!(APP.definition("missing").is_none());
        assert!(APP.init.is_none());
    }
}
