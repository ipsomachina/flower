//! Host-call implementation: the guest operations.
//!
//! [`Host`] answers guest `host_call`s against a pinned read snapshot plus
//! the session's staged overlay. Reads record dependencies; writes stage
//! and maintain indexes; calls reborrow the session into nested hosts so
//! nested invocations share the snapshot, the overlay and the final atomic
//! publication.
//!
//! Names intern per database qualified by module (`module\0name`; indexes
//! additionally by collection), all pre-interned at deploy so reads never
//! allocate IDs. Raw string storage lives under the reserved `$raw`
//! collection with namespace-folded keys, verbatim values and no schemas.
//! Keys on descriptor collections normalize to canonical bytes; non-JSON
//! keys reject, as do integer keys beyond the safe range. Host iteration
//! order is length-prefixed byte order (length first, then bytes, per
//! component), and range bounds compare in that same order, so a bounded
//! range is one contiguous key range the storage scan reads directly.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

mod sql;

use flower_sdk::abi::{HostOp, encode_err, encode_ok, encode_recovered};
use flower_sdk::error::codes;
use flower_sdk::{CallFailure, Failure, FlowerType, Principal, Value, fail};

use crate::descriptors::{CollectionDef, DefKind, ModuleDef};
use crate::error::{Error, HostResult};
use crate::history::{HistoryProvider, encode_versions};
use crate::json::{canonicalize_key, parse_value, render};
use crate::runtime::{CallKind, HostCall, Runtime};
use crate::session::{CallerKind, Invocation};
use crate::storage::keys::{
    RangeDep, decode_record_suffix, encode_collection_range_dep, encode_index_key,
    encode_range_dep, encode_record_dep, encode_record_key, push_sized,
};
use crate::storage::{IndexEntry, ReadTxn, RowWrite, ScanRange};

/// Reserved collection holding raw string namespaces.
pub const RAW_COLLECTION: &[u8] = b"$raw";
/// Reserved collection holding durable queue items. Names are
/// deployment-global: modules namespace by convention (`billing.email`).
pub const QUEUE_COLLECTION: &[u8] = b"$queue";
/// Reserved collection holding durable timers, keyed due-first.
pub const TIMER_COLLECTION: &[u8] = b"$timer";
/// Reserved collection holding maintenance tasks' failure states, keyed
/// by module and task (see [`crate::task`]).
pub const TASK_COLLECTION: &[u8] = b"$task";
/// Largest page a single scan or range call returns.
pub const MAX_SCAN_LIMIT: u32 = 10_000;
/// Fuel charged per host operation.
pub const FUEL_OP: u64 = 1;
/// Fuel charged per returned row.
pub const FUEL_ROW: u64 = 1;
/// Fuel charged per nested call.
pub const FUEL_CALL: u64 = 10;
/// Deepest nesting of guest calls (cross-module calls, triggers and
/// planned transaction calls). Every level holds a fresh guest instance
/// and native stack, so the cap fails long before fuel would.
pub const MAX_CALL_DEPTH: usize = 32;
/// Failure code: this member holds no runtime for the called module (it
/// is loading or was removed meanwhile). The host's, never the call's:
/// another attempt or member may serve it.
pub const MODULE_UNAVAILABLE: &str = "module_unavailable";

/// Authorization policy. The core ships [`PermissivePolicy`]; real
/// deployments plug authorization in here. Policies are `Sync`:
/// concurrent queries share one.
pub trait Policy: Send + Sync {
    /// Authorize invoking a definition.
    fn allow_invoke(
        &self,
        principal: Option<&Principal>,
        module: &str,
        name: &str,
    ) -> HostResult<()>;
    /// Authorize a cross- or self-module call.
    fn allow_call(
        &self,
        principal: Option<&Principal>,
        from_module: &str,
        to_module: &str,
        name: &str,
    ) -> HostResult<()>;
    /// Authorize writing a collection.
    fn allow_write(
        &self,
        principal: Option<&Principal>,
        module: &str,
        collection: &str,
    ) -> HostResult<()>;
}

/// Policy that allows everything.
#[derive(Clone, Copy, Debug, Default)]
pub struct PermissivePolicy;

impl Policy for PermissivePolicy {
    fn allow_invoke(
        &self,
        _principal: Option<&Principal>,
        _module: &str,
        _name: &str,
    ) -> HostResult<()> {
        Ok(())
    }

    fn allow_call(
        &self,
        _principal: Option<&Principal>,
        _from_module: &str,
        _to_module: &str,
        _name: &str,
    ) -> HostResult<()> {
        Ok(())
    }

    fn allow_write(
        &self,
        _principal: Option<&Principal>,
        _module: &str,
        _collection: &str,
    ) -> HostResult<()> {
        Ok(())
    }
}

/// Qualify a collection name for interning.
pub fn qualify_collection(module: &str, name: &str) -> String {
    format!("{module}\0{name}")
}

/// Qualify an index name for interning.
pub fn qualify_index(module: &str, collection: &str, index: &str) -> String {
    format!("{module}\0{collection}\0{index}")
}

/// Qualify a definition name for interning.
pub fn qualify_definition(module: &str, name: &str) -> String {
    format!("{module}\0{name}")
}

/// Qualify a raw namespace for interning. Guests address raw storage as
/// `$raw/<namespace>` for writes and either spelling for reads; both
/// spellings fold to the same namespace.
pub fn qualify_raw(module: &str, name: &str) -> String {
    qualify_collection(module, name.strip_prefix("$raw/").unwrap_or(name))
}

/// Split a qualified `module\0name` back apart.
pub fn split_qualified(qualified: &[u8]) -> Result<(String, String), Error> {
    let text = std::str::from_utf8(qualified)
        .map_err(|_| Error::corrupt("qualified name is not UTF-8"))?;
    let (module, name) = text
        .split_once('\0')
        .ok_or_else(|| Error::corrupt("qualified name without separator"))?;
    Ok((module.to_string(), name.to_string()))
}

/// One operation's failure: guest failures return envelopes, fatal ones
/// poison the invocation first.
#[derive(Debug)]
pub(crate) struct OpError {
    pub(crate) failure: Failure,
    pub(crate) fatal: bool,
}

impl OpError {
    pub(crate) fn fatal(failure: Failure) -> Self {
        OpError {
            failure,
            fatal: true,
        }
    }
}

impl From<Failure> for OpError {
    fn from(failure: Failure) -> Self {
        OpError {
            failure,
            fatal: false,
        }
    }
}

impl From<Error> for OpError {
    fn from(error: Error) -> Self {
        OpError {
            failure: error.into(),
            fatal: true,
        }
    }
}

pub(crate) type OpResult<T> = Result<T, OpError>;

/// Envelope payload: a plain value wraps in `ok`, a prebuilt envelope
/// (savepoint calls) passes through.
enum Payload {
    Value(Value),
    Envelope(Value),
}

/// Which surface of another module an operation reaches.
#[derive(Clone, Copy)]
enum Surface {
    Collection,
    Derived,
    Method,
}

/// Another module's name is not exported to the calling module.
fn private(module: &str, name: &str) -> Failure {
    fail(
        codes::MODULE_PRIVATE,
        format!("`{module}` does not export `{name}`"),
    )
}

/// A failure as its error contract sees it: `{code, message, details?}`.
fn failure_value(failure: &Failure) -> Value {
    let mut map = BTreeMap::new();
    map.insert("code".to_string(), Value::String(failure.code.clone()));
    map.insert(
        "message".to_string(),
        Value::String(failure.message.clone()),
    );
    if let Some(details) = &failure.details {
        map.insert("details".to_string(), details.clone());
    }
    Value::Object(map)
}

/// The deployed environment an invocation runs against: one database's
/// module runtimes and descriptors plus the engine's history provider
/// and authorization policy.
#[derive(Clone, Copy)]
pub struct Env<'a> {
    /// The database's module runtimes, by module.
    pub runtimes: &'a HashMap<String, Arc<dyn Runtime>>,
    /// The database's module descriptors, by module.
    pub descriptors: &'a HashMap<String, ModuleDef>,
    /// Retained-version provider.
    pub history: &'a dyn HistoryProvider,
    /// Authorization policy.
    pub policy: &'a dyn Policy,
}

/// The host-call handler: snapshot reads, staged overlay, nested calls.
pub struct Host<'a> {
    /// Pinned read snapshot.
    pub read: &'a ReadTxn,
    /// The invocation session.
    pub session: &'a mut Invocation,
    /// Module runtimes for nested calls. Shared: backends invoke through
    /// `&self`, so nested calls (including same-module recursion) look up
    /// without borrowing the host.
    pub runtimes: &'a HashMap<String, Arc<dyn Runtime>>,
    /// Module descriptors for routing and validation.
    pub descriptors: &'a HashMap<String, ModuleDef>,
    /// Retained-version provider.
    pub history: &'a dyn HistoryProvider,
    /// Authorization policy.
    pub policy: &'a dyn Policy,
}

impl<'a> Host<'a> {
    /// A host over a pinned snapshot and a session in one environment.
    pub fn new(read: &'a ReadTxn, session: &'a mut Invocation, env: &Env<'a>) -> Self {
        Host {
            read,
            session,
            runtimes: env.runtimes,
            descriptors: env.descriptors,
            history: env.history,
            policy: env.policy,
        }
    }

    /// Invoke a module's runtime with this host serving callbacks. The
    /// registry lookup copies the shared reference, so the host stays
    /// mutably available as the callback handler.
    pub(crate) fn invoke_runtime(
        &mut self,
        kind: CallKind,
        module: &str,
        name: &str,
        args: &Value,
    ) -> OpResult<Result<Value, CallFailure>> {
        let runtimes: &'a HashMap<String, Arc<dyn Runtime>> = self.runtimes;
        let runtime = runtimes.get(module).ok_or_else(|| {
            OpError::fatal(fail(
                MODULE_UNAVAILABLE,
                format!("no runtime for `{module}`"),
            ))
        })?;
        // Only the invocation's outermost guest call takes the budget;
        // nested ones share the scope it sets.
        let budget = self.session.budget;
        Ok(crate::runtime::wasm::with_budget(budget, || {
            runtime.call(kind, module, name, args, self)
        }))
    }

    /// Invoke a nested definition with callee module identity: `""`
    /// resolves to the callee and `caller` reports the caller. Restores
    /// both afterwards, so nesting stacks correctly. Nesting past
    /// [`MAX_CALL_DEPTH`] fails fatally (never savepoint-recoverable),
    /// deterministically and before any native stack runs out.
    pub(crate) fn invoke_nested(
        &mut self,
        kind: CallKind,
        module: &str,
        name: &str,
        args: &Value,
    ) -> OpResult<Result<Value, CallFailure>> {
        if !self.session.enter_call(MAX_CALL_DEPTH) {
            return Err(OpError::fatal(fail(
                "nesting_too_deep",
                format!("guest calls nest deeper than {MAX_CALL_DEPTH} levels"),
            )));
        }
        let saved_module = std::mem::replace(&mut self.session.module, module.to_string());
        let saved_caller = self.session.caller.replace(saved_module.clone());
        let outcome = self.invoke_runtime(kind, module, name, args);
        self.session.caller = saved_caller;
        self.session.module = saved_module;
        self.session.exit_call();
        outcome
    }

    /// Resolve the shared `$raw` collection, failing when uninitialized.
    fn raw_collection(&self) -> OpResult<u32> {
        self.read
            .intern_id(&self.session.db, RAW_COLLECTION)?
            .ok_or_else(|| {
                OpError::from(fail(codes::INVALID_VALUE, "raw storage is not initialized"))
            })
    }

    fn dispatch(&mut self, op: u8, input: &[u8]) -> Value {
        let outcome = self.route(op, input);
        match outcome {
            Ok(Payload::Value(value)) => encode_ok(value),
            Ok(Payload::Envelope(envelope)) => envelope,
            Err(error) => {
                if error.fatal {
                    self.session.poison(error.failure.clone());
                }
                encode_err(&error.failure)
            }
        }
    }

    fn route(&mut self, op: u8, input: &[u8]) -> OpResult<Payload> {
        self.session.spend(FUEL_OP)?;
        let args = Value::decode_binary(input)
            .map_err(|_| OpError::from(fail(codes::INVALID_VALUE, "bad host-call input")))?;
        let op = HostOp::from_u32(op as u32)
            .ok_or_else(|| OpError::from(fail(codes::INVALID_VALUE, "unknown host op")))?;
        let payload = match op {
            HostOp::Get => Payload::Value(self.op_get(args, false)?),
            HostOp::RawGet => Payload::Value(self.op_get(args, true)?),
            HostOp::GetDerived => Payload::Value(self.op_get_derived(args)?),
            HostOp::Scan => Payload::Value(self.op_scan(args, false)?),
            HostOp::RawScan => Payload::Value(self.op_scan(args, true)?),
            HostOp::Range => Payload::Value(self.op_range(args)?),
            HostOp::Sql => Payload::Value(self.op_sql(args)?),
            HostOp::SqlDynamic => Payload::Value(self.op_sql_dynamic(args)?),
            HostOp::Call => Payload::Envelope(self.op_call(args)?),
            HostOp::Set => {
                self.op_set(args)?;
                Payload::Value(Value::Null)
            }
            HostOp::Delete => {
                self.op_delete(args)?;
                Payload::Value(Value::Null)
            }
            HostOp::DeleteIfMatch => Payload::Value(self.op_delete_if_match(args)?),
            HostOp::Materialize => {
                self.op_materialize(args, true)?;
                Payload::Value(Value::Null)
            }
            HostOp::Unmaterialize => {
                self.op_materialize(args, false)?;
                Payload::Value(Value::Null)
            }
            HostOp::Settle => {
                crate::lane::settle(self)?;
                Payload::Value(Value::Null)
            }
            HostOp::History => Payload::Value(self.op_history(args)?),
            HostOp::Now => Payload::Value(self.op_time()?),
            HostOp::Clock => Payload::Value(self.op_clock()),
            HostOp::ChangesAt => {
                self.op_changes_at(args)?;
                Payload::Value(Value::Null)
            }
            HostOp::Principal => Payload::Value(self.op_principal()?),
            HostOp::Caller => Payload::Value(self.op_caller()),
            HostOp::Enqueue => Payload::Value(self.op_enqueue(args)?),
            HostOp::TimerSchedule => {
                self.op_timer_schedule(args)?;
                Payload::Value(Value::Null)
            }
            HostOp::TimerCancel => Payload::Value(self.op_timer_cancel(args)?),
            HostOp::TimerGet => Payload::Value(self.op_timer_get(args)?),
            HostOp::TimerScan => Payload::Value(self.op_timer_scan(args)?),
            HostOp::TimerRetry => {
                self.op_timer_retry(args)?;
                Payload::Value(Value::Null)
            }
            HostOp::QueueClaim => Payload::Value(self.op_queue_claim(args)?),
            HostOp::QueueRenew => Payload::Value(self.op_queue_renew(args)?),
            HostOp::QueueReport => {
                self.op_queue_report(args)?;
                Payload::Value(Value::Null)
            }
            HostOp::QueueCancel => Payload::Value(self.op_queue_cancel(args)?),
            HostOp::QueueRetry => {
                self.op_queue_retry(args)?;
                Payload::Value(Value::Null)
            }
            HostOp::QueueGet => Payload::Value(self.op_queue_get(args)?),
            HostOp::QueueStats => Payload::Value(self.op_queue_stats(args)?),
            HostOp::QueueWait => {
                self.op_queue_wait(args)?;
                Payload::Value(Value::Null)
            }
            HostOp::QueuePurge => Payload::Value(self.op_queue_purge(args)?),
        };
        Ok(payload)
    }

    /// Resolve a guest-named module: `""` is the calling module and
    /// `$slot` the module the calling module's deployment binds it to.
    fn normalize(&self, module: &str) -> OpResult<String> {
        if module.is_empty() {
            return Ok(self.session.module.clone());
        }
        match module.strip_prefix('$') {
            None => Ok(module.to_string()),
            Some(slot) => self
                .descriptors
                .get(&self.session.module)
                .and_then(|defs| defs.bindings.get(slot))
                .cloned()
                .ok_or_else(|| {
                    OpError::from(fail(
                        codes::INVALID_VALUE,
                        format!("slot `{module}` is not bound"),
                    ))
                }),
        }
    }

    /// Whether `module` exports `name` on `surface`.
    fn exported(&self, module: &str, surface: Surface, name: &str) -> bool {
        self.descriptors
            .get(module)
            .is_some_and(|defs| match surface {
                Surface::Collection => defs.exports.collections.contains(name),
                Surface::Derived => defs.exports.derived.contains(name),
                Surface::Method => defs.exports.methods.contains(name),
            })
    }

    /// Record that the invocation reaches `module`'s surface from
    /// another module, whatever the access check decides: an activation
    /// of `module` drops every derived outcome that did.
    fn reach(&mut self, module: &str) {
        if module != self.session.module {
            self.session
                .record_dep(crate::certificate::module_dep(module));
        }
    }

    /// Reading another module's collection needs it exported; raw
    /// namespaces never are.
    fn check_read(&mut self, module: &str, name: &str, raw: bool) -> OpResult<()> {
        self.reach(module);
        if module == self.session.module
            || (!raw
                && !name.starts_with("$raw/")
                && self.exported(module, Surface::Collection, name))
        {
            return Ok(());
        }
        Err(OpError::from(private(module, name)))
    }

    /// Other modules' collections are never written here: exported ones
    /// are read-only, the rest private.
    fn check_write_target(&self, module: &str, name: &str) -> OpResult<()> {
        if module == self.session.module {
            return Ok(());
        }
        if !name.starts_with("$raw/") && self.exported(module, Surface::Collection, name) {
            return Err(OpError::from(fail(
                codes::MODULE_READ_ONLY,
                format!("`{module}`'s collection `{name}` is read-only to other modules"),
            )));
        }
        Err(OpError::from(private(module, name)))
    }

    /// Reading or materializing another module's derived value needs it
    /// exported.
    fn check_derived(&mut self, module: &str, name: &str) -> OpResult<()> {
        self.reach(module);
        if module == self.session.module || self.exported(module, Surface::Derived, name) {
            return Ok(());
        }
        Err(OpError::from(private(module, name)))
    }

    /// Why the calling module may not call `module`'s `name`, if it may
    /// not: unexported, or exported to other callers only.
    fn call_refusal(&self, module: &str, name: &str) -> Option<Failure> {
        let caller = &self.session.module;
        if module == caller {
            return None;
        }
        if !self.exported(module, Surface::Method, name) {
            return Some(private(module, name));
        }
        let callers = self
            .descriptors
            .get(module)
            .and_then(|defs| defs.exports.callers.get(name))?;
        if callers.contains(caller) {
            return None;
        }
        Some(fail(
            codes::MODULE_PRIVATE,
            format!("`{module}` lets only its allowed callers call `{name}`, not `{caller}`"),
        ))
    }

    /// Check a value another module returned against the contract its
    /// provider exports: the result schema for successes, the error
    /// schema for business failures.
    fn contract_breach(
        &self,
        module: &str,
        name: &str,
        outcome: &Result<Value, Failure>,
    ) -> Option<Failure> {
        let defs = self.descriptors.get(module)?;
        let (fits, what) = match outcome {
            Ok(value) => (defs.result_schema(name).is(value), "result"),
            Err(failure) => (
                defs.errors_schema(name).is(&failure_value(failure)),
                "failure",
            ),
        };
        (!fits).then(|| {
            fail(
                codes::MODULE_CONTRACT_VIOLATION,
                format!("`{module}`'s `{name}` broke its exported contract: {what}"),
            )
        })
    }

    /// Resolve a collection to its interned ID.
    pub(crate) fn collection_id(&self, module: &str, name: &str) -> OpResult<u32> {
        let qualified = qualify_collection(module, name);
        self.read
            .intern_id(&self.session.db, qualified.as_bytes())?
            .ok_or_else(|| {
                OpError::from(fail(
                    codes::INVALID_VALUE,
                    format!("unknown collection `{name}`"),
                ))
            })
    }

    /// Resolve an index to its ID plus arity.
    fn index_id(&self, module: &str, collection: &str, index: &str) -> OpResult<(u32, usize)> {
        let qualified = qualify_index(module, collection, index);
        let id = self
            .read
            .intern_id(&self.session.db, qualified.as_bytes())?
            .ok_or_else(|| {
                OpError::from(fail(
                    codes::INVALID_VALUE,
                    format!("unknown index `{index}`"),
                ))
            })?;
        let arity = self
            .descriptors
            .get(module)
            .and_then(|defs| defs.collection(collection))
            .and_then(|def| def.indexes.iter().find(|entry| entry.name == index))
            .map(|entry| entry.fields.len())
            .ok_or_else(|| {
                OpError::from(fail(
                    codes::INVALID_VALUE,
                    format!("unknown index `{index}`"),
                ))
            })?;
        Ok((id, arity))
    }

    /// Look up a collection descriptor.
    pub(crate) fn collection_def(&self, module: &str, name: &str) -> OpResult<&CollectionDef> {
        self.descriptors
            .get(module)
            .and_then(|defs| defs.collection(name))
            .ok_or_else(|| {
                OpError::from(fail(
                    codes::INVALID_VALUE,
                    format!("unknown collection `{name}`"),
                ))
            })
    }

    /// Read one record staged-or-committed: canonical JSON bytes.
    pub(crate) fn read_record(&self, collection: u32, key: &[u8]) -> OpResult<Option<Vec<u8>>> {
        if let Some(staged) = self.session.staged.staged_record(collection, key) {
            return Ok(staged);
        }
        Ok(self.read.get_record(&self.session.db, collection, key)?)
    }

    /// Normalize key text to canonical bytes, rejecting non-JSON and
    /// integers beyond the safe range, which would collapse onto their
    /// neighbours' keys.
    fn canon_key(&self, key: &str) -> OpResult<Vec<u8>> {
        match canonicalize_key(key.as_bytes()) {
            Ok(Some(canonical)) => Ok(canonical),
            Ok(None) => Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "record key holds an integer beyond the safe range",
            ))),
            Err(_) => Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "record key is not canonical JSON",
            ))),
        }
    }

    fn op_get(&mut self, args: Value, raw: bool) -> OpResult<Value> {
        let args = expect_array(args, 3, "get takes [module, name, key]")?;
        let module = self.normalize(expect_str(&args[0], "get module")?)?;
        let name = expect_str(&args[1], "get collection")?.to_string();
        self.check_read(&module, &name, raw)?;
        let key_text = expect_str(&args[2], "get key")?.to_string();
        let (collection, key) = self.row_key(&module, &name, &key_text, raw)?;
        let dep = encode_record_dep(collection, &key)?;
        self.session.record_dep(dep);
        self.session.spend(FUEL_ROW)?;
        let row = match self.read_record(collection, &key)? {
            None => return Ok(Value::Null),
            Some(bytes) => self.decode_row(&bytes, raw)?,
        };
        // Raw namespaces carry no policies; a hidden row reads as absent.
        match (raw, self.guard(&module, &name)) {
            (false, Some(policy)) => {
                let key = parse_value(&key)?;
                Ok(self
                    .shown(&module, &policy, &key, row, &[])?
                    .unwrap_or(Value::Null))
            }
            _ => Ok(row),
        }
    }

    /// Resolve a row address: collection ID plus stored key bytes. Raw
    /// namespaces fold into the shared `$raw` collection; descriptor keys
    /// normalize to canonical bytes.
    fn row_key(&self, module: &str, name: &str, key: &str, raw: bool) -> OpResult<(u32, Vec<u8>)> {
        if raw {
            let collection = self.raw_collection()?;
            let namespace = qualify_raw(module, name);
            let key = raw_key(namespace.as_bytes(), key.as_bytes())?;
            Ok((collection, key))
        } else {
            let collection = self.collection_id(module, name)?;
            let key = self.canon_key(key)?;
            Ok((collection, key))
        }
    }

    /// Decode stored row bytes: verbatim strings for raw rows, parsed
    /// canonical JSON otherwise.
    fn decode_row(&self, bytes: &[u8], raw: bool) -> OpResult<Value> {
        if raw {
            let text =
                std::str::from_utf8(bytes).map_err(|_| Error::corrupt("raw value is not UTF-8"))?;
            Ok(Value::String(text.to_string()))
        } else {
            Ok(parse_value(bytes)?)
        }
    }

    fn op_get_derived(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 3, "get_derived takes [module, name, args]")?;
        let module = self.normalize(expect_str(&args[0], "derived module")?)?;
        let name = expect_str(&args[1], "derived name")?.to_string();
        self.check_derived(&module, &name)?;
        let call_args = args[2].clone();
        self.admit_derived_read(&module, &name, &call_args)?;
        let outcome = self.get_or_evaluate(&module, &name, &call_args);
        if module == self.session.module {
            return outcome;
        }
        // Another module's value: hold it to the contract it exports.
        let checked = match &outcome {
            Ok(value) => Ok(value.clone()),
            Err(error) if !error.fatal => Err(error.failure.clone()),
            Err(_) => return outcome,
        };
        match self.contract_breach(&module, &name, &checked) {
            Some(breach) => Err(OpError::from(breach)),
            None => outcome,
        }
    }

    fn op_scan(&mut self, args: Value, raw: bool) -> OpResult<Value> {
        let args = expect_array(args, 4, "scan takes [module, name, limit, after]")?;
        let module = self.normalize(expect_str(&args[0], "scan module")?)?;
        let name = expect_str(&args[1], "scan collection")?.to_string();
        self.check_read(&module, &name, raw)?;
        let limit = scan_limit(&args[2])?;
        let after = opt_str(&args[3], "scan cursor")?;
        let (collection, scope_extra) = if raw {
            let namespace = qualify_raw(&module, &name);
            (
                self.raw_collection()?,
                Some(raw_namespace_prefix(namespace.as_bytes())?),
            )
        } else {
            (self.collection_id(&module, &name)?, None)
        };
        // Collect committed rows above the cursor, overlaid with staged
        // writes, ordered by encoded key bytes. Record keys frame the
        // key bytes with a length prefix, so a raw namespace is not a
        // byte prefix of the full key: scope checks run against the
        // decoded suffix, and committed raw rows come from a key-prefix
        // scan that visits the namespace's rows only.
        let record_scope = crate::storage::keys::record_prefix(&self.session.db, collection)?;
        let db_prefix = record_scope.len();
        let in_scope = |full: &[u8]| -> OpResult<bool> {
            if !full.starts_with(&record_scope) {
                return Ok(false);
            }
            match scope_extra.as_ref() {
                None => Ok(true),
                Some(namespace) => {
                    Ok(decode_record_suffix(full, db_prefix)?.starts_with(namespace))
                }
            }
        };
        let after_bytes = after
            .as_ref()
            .map(|cursor| match cursor {
                Value::String(text) => decode_cursor(text, "scan cursor"),
                _ => Err(OpError::from(fail(
                    codes::TYPE_MISMATCH,
                    "scan cursor is not text",
                ))),
            })
            .transpose()?;
        if let Some(after) = after_bytes.as_ref() {
            // A malformed cursor reads as out of scope (guest error),
            // never as corruption: only engine-built bytes reach the
            // merge loops below. A record cursor must be exactly the full
            // key of a record of this collection, as scans hand them out:
            // owner-bucket scans decode it, and a full scan of the same
            // collection must refuse what they refuse.
            let whole = scope_extra.is_some()
                || decode_record_suffix(after, db_prefix)
                    .and_then(|key| encode_record_key(&self.session.db, collection, &key))
                    .is_ok_and(|full| full == *after);
            if !whole || !in_scope(after).unwrap_or(false) {
                return Err(OpError::from(fail(
                    codes::INVALID_VALUE,
                    "scan cursor is out of scope",
                )));
            }
        }
        let staged_extra: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .session
            .staged
            .staged_records_for(collection)
            .into_iter()
            .filter(|(key, _)| {
                scope_extra
                    .as_ref()
                    .is_none_or(|namespace| key.starts_with(namespace))
            })
            .collect();
        // Hidden rows drop out before the limit and the cursor.
        let guard = if raw {
            None
        } else {
            self.guard(&module, &name)
        };
        if let Some(policy) = guard.as_deref()
            && let Some(page) = self.bucket_scan(
                &module,
                &name,
                policy,
                collection,
                limit,
                after_bytes.as_deref(),
                &staged_extra,
            )?
        {
            return Ok(page);
        }
        // The cap covers the page, every staged displacement and the
        // cursor row itself, which the merge below drops. A guarded scan
        // also drops hidden rows: short of a full page it reads on with a
        // doubled cap until the page fills or the scope ends.
        let mut cap = limit as usize + staged_extra.len() + 1;
        let (rows, cursor) = loop {
            let committed = match scope_extra.as_ref() {
                Some(namespace) => {
                    let after_key = after_bytes
                        .as_ref()
                        .map(|after| decode_record_suffix(after, db_prefix))
                        .transpose()?;
                    self.read.scan_records_with_key_prefix(
                        &self.session.db,
                        collection,
                        namespace,
                        after_key.as_deref(),
                        cap,
                    )?
                }
                None => self.read.scan_records(
                    &self.session.db,
                    collection,
                    after_bytes.as_deref().or(Some(record_scope.as_slice())),
                    None,
                    Some(cap),
                    false,
                )?,
            };
            let truncated = committed.len() >= cap;
            // Past the last committed row a truncated read loaded, staged
            // rows would jump the committed ones not yet read.
            let mut horizon: Option<Vec<u8>> = None;
            let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
            for (key, json) in committed {
                let full = encode_record_key(&self.session.db, collection, &key)?;
                if truncated {
                    horizon = Some(full.clone());
                }
                if !in_scope(&full)? {
                    continue;
                }
                merged.insert(full, Some(json));
            }
            for (key, value) in &staged_extra {
                let full = encode_record_key(&self.session.db, collection, key)?;
                if !in_scope(&full)? {
                    continue;
                }
                match value {
                    Some(json) => {
                        merged.insert(full, Some(json.clone()));
                    }
                    None => {
                        merged.remove(&full);
                    }
                }
            }
            let mut rows = Vec::new();
            let mut cursor: Option<String> = None;
            for (full, json) in merged {
                if let Some(after) = after_bytes.as_ref()
                    && full <= *after
                {
                    continue;
                }
                if horizon.as_ref().is_some_and(|horizon| full > *horizon) {
                    break;
                }
                let Some(json) = json else { continue };
                if rows.len() >= limit as usize {
                    break;
                }
                let key = decode_record_suffix(&full, db_prefix)?;
                let (key_text, value) = if raw {
                    let (_, original) = decode_raw_key(&key)?;
                    (original, self.decode_row(&json, true)?)
                } else {
                    (key.clone(), self.decode_row(&json, false)?)
                };
                let key_value = if raw {
                    Value::String(
                        std::str::from_utf8(&key_text)
                            .map_err(|_| Error::corrupt("raw key is not UTF-8"))?
                            .to_string(),
                    )
                } else {
                    parse_value(&key_text)?
                };
                let value = match guard.as_deref() {
                    None => value,
                    Some(policy) => {
                        self.session.spend(FUEL_ROW)?;
                        match self.shown(&module, policy, &key_value, value, &[])? {
                            None => continue,
                            Some(value) => value,
                        }
                    }
                };
                rows.push(Value::Array(vec![key_value, value]));
                cursor = Some(crate::hex::encode(&full));
                self.session.spend(FUEL_ROW)?;
            }
            if guard.is_none() || rows.len() >= limit as usize || !truncated {
                break (rows, cursor);
            }
            cap = cap.saturating_mul(2);
        };
        // Reads record one collection-range dependency: the scanned scope
        // past the cursor, limits excluded (coarse but correct). The
        // prefix stays empty even for raw scans: a raw namespace is not
        // a byte prefix of full keys, so narrowing by it would match
        // nothing and miss invalidations.
        let range = RangeDep {
            prefix: Vec::new(),
            gt: after_bytes,
            gte: None,
            lt: None,
            lte: None,
        };
        let dep = encode_collection_range_dep(collection, &range)?;
        self.session.record_dep(dep);
        let full = rows_full(&rows, limit);
        Ok(encode_page(rows, cursor.filter(|_| full)))
    }

    /// A caller's plain scan of a collection whose read rule, folded for
    /// it, pins one top-level field to one string (`row.owner ==
    /// principal.subject`), when an index covers exactly that field: it
    /// reads that index bucket, plus the rows the invocation itself wrote,
    /// instead of the whole collection. Every row it may show holds that
    /// string there, so the bucket has them all, and the page, cursor and
    /// redaction are the plain scan's. It depends on the bucket's entries
    /// and the rows it examined, not on the collection: other callers'
    /// writes elsewhere leave it valid. `None` when no such rule and index
    /// apply.
    #[allow(clippy::too_many_arguments)]
    fn bucket_scan(
        &mut self,
        module: &str,
        name: &str,
        policy: &crate::access::Folded,
        collection: u32,
        limit: u32,
        after: Option<&[u8]>,
        staged: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> OpResult<Option<Value>> {
        let Some((field, value)) = policy.bucket() else {
            return Ok(None);
        };
        let Some(index_name) = self
            .collection_def(module, name)?
            .indexes
            .iter()
            .find(|index| index.fields.len() == 1 && index.fields[0] == field)
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };
        let qualified = qualify_index(module, name, &index_name);
        let db = self.session.db.clone();
        let Some(index) = self.read.intern_id(&db, qualified.as_bytes())? else {
            return Ok(None);
        };
        let component = render(&Value::String(value.to_string()))?;
        let components = [component.as_slice()];
        let db_prefix = crate::storage::keys::record_prefix(&db, collection)?.len();
        // Bucket entries order by the length-prefixed primary key, as
        // record keys do, so the cursor's row is where the bucket resumes.
        let lower = after
            .map(|after| {
                let primary = decode_record_suffix(after, db_prefix)?;
                encode_index_key(&db, index, &components, &primary)
            })
            .transpose()?;
        let mut cap = limit as usize + staged.len() + 1;
        let (rows, cursor) = loop {
            let range = ScanRange {
                lower: lower.as_deref(),
                upper: None,
                limit: Some(cap),
                reverse: false,
            };
            let committed = self.read.scan_index(&db, index, 1, &components, range)?;
            let truncated = committed.len() >= cap;
            // Past the last entry a truncated read loaded, staged rows
            // would jump the entries not yet read.
            let mut horizon: Option<Vec<u8>> = None;
            let mut candidates = std::collections::BTreeSet::new();
            for (_, primary) in committed {
                let full = encode_record_key(&db, collection, &primary)?;
                if truncated {
                    horizon = Some(full.clone());
                }
                candidates.insert(full);
            }
            // The invocation's own writes may have moved rows in or out.
            for (key, _) in staged {
                candidates.insert(encode_record_key(&db, collection, key)?);
            }
            let mut rows = Vec::new();
            let mut cursor: Option<String> = None;
            for full in candidates {
                if after.is_some_and(|after| full.as_slice() <= after) {
                    continue;
                }
                if horizon.as_ref().is_some_and(|horizon| full > *horizon) {
                    break;
                }
                if rows.len() >= limit as usize {
                    break;
                }
                let key = decode_record_suffix(&full, db_prefix)?;
                // Whether it shows follows its content.
                self.session
                    .record_dep(encode_record_dep(collection, &key)?);
                let Some(json) = self.read_record(collection, &key)? else {
                    continue;
                };
                let key_value = parse_value(&key)?;
                self.session.spend(FUEL_ROW)?;
                let Some(row) = self.shown(module, policy, &key_value, parse_value(&json)?, &[])?
                else {
                    continue;
                };
                rows.push(Value::Array(vec![key_value, row]));
                cursor = Some(crate::hex::encode(&full));
                self.session.spend(FUEL_ROW)?;
            }
            if rows.len() >= limit as usize || !truncated {
                break (rows, cursor);
            }
            cap = cap.saturating_mul(2);
        };
        // Rows entering or leaving the bucket.
        let bucket = RangeDep {
            prefix: vec![component],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        };
        self.session.record_dep(encode_range_dep(index, &bucket)?);
        let full = rows_full(&rows, limit);
        Ok(Some(encode_page(rows, cursor.filter(|_| full))))
    }

    fn op_range(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 4, "range takes [module, name, index, bounds]")?;
        let module = self.normalize(expect_str(&args[0], "range module")?)?;
        let name = expect_str(&args[1], "range collection")?.to_string();
        self.check_read(&module, &name, false)?;
        let index_name = expect_str(&args[2], "range index")?.to_string();
        let (index, arity) = self.index_id(&module, &name, &index_name)?;
        let bounds = as_object(&args[3], "range bounds")?;
        let prefix = as_array(get_member(bounds, "prefix")?, "range prefix")?
            .iter()
            .map(|item| as_str(item, "range prefix component"))
            .collect::<OpResult<Vec<_>>>()?;
        let mut canon_prefix = Vec::with_capacity(prefix.len());
        for component in prefix {
            canon_prefix.push(self.canon_key(component)?);
        }
        let bound = |key: &str| -> OpResult<Option<Vec<u8>>> {
            match get_member(bounds, key)? {
                Value::Null => Ok(None),
                Value::String(text) => Ok(Some(self.canon_key(text)?)),
                _ => Err(OpError::from(fail(
                    codes::TYPE_MISMATCH,
                    "range bound is not text",
                ))),
            }
        };
        let range = RangeDep {
            prefix: canon_prefix.clone(),
            gt: bound("gt")?,
            gte: bound("gte")?,
            lt: bound("lt")?,
            lte: bound("lte")?,
        };
        let limit = scan_limit(get_member(bounds, "limit")?)?;
        let after = match get_member(bounds, "after")? {
            Value::Null => None,
            Value::String(cursor) => Some(decode_cursor(cursor, "range cursor")?),
            _ => {
                return Err(OpError::from(fail(
                    codes::TYPE_MISMATCH,
                    "range cursor is not text",
                )));
            }
        };
        let reverse = match get_member(bounds, "reverse")? {
            Value::Bool(reverse) => *reverse,
            _ => {
                return Err(OpError::from(fail(
                    codes::TYPE_MISMATCH,
                    "range reverse is not a boolean",
                )));
            }
        };
        let prefix_refs: Vec<&[u8]> = canon_prefix.iter().map(Vec::as_slice).collect();
        let index_scope =
            crate::storage::keys::index_prefix(&self.session.db, index, &prefix_refs)?;
        if let Some(after) = after.as_ref()
            && !after.starts_with(&index_scope)
        {
            return Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "range cursor is out of scope",
            )));
        }
        let staged: Vec<(Vec<Vec<u8>>, Vec<u8>, bool)> = self
            .session
            .staged
            .staged_index_prefix(index, &canon_prefix)
            .into_iter()
            .filter(|(components, primary, _)| range_within(&range, components, primary))
            .collect();
        // Hidden rows drop out before the limit and the cursor, and so do
        // rows found through an index field the caller can't read.
        let guard = self.guard(&module, &name);
        let index_fields: Vec<String> = match guard {
            None => Vec::new(),
            Some(_) => self
                .collection_def(&module, &name)?
                .indexes
                .iter()
                .find(|entry| entry.name == index_name)
                .map(|entry| entry.fields.clone())
                .unwrap_or_default(),
        };
        let collection = self.collection_id(&module, &name)?;
        // The bounds select one contiguous key range (they compare in
        // stored component order), so the committed scan reads exactly
        // the entries inside them, narrowed further by the cursor:
        // forward pages read up from it, reverse pages down from it. The
        // cap covers the page plus every staged displacement plus the
        // cursor row itself, which the in-memory filter drops (the scan
        // bound is inclusive forward and exclusive reverse). A guarded
        // range also drops hidden rows: short of a full page it reads on
        // with a doubled cap until the page fills or the range ends.
        let mut cap = limit as usize + staged.len() + 1;
        let (rows, cursor) = loop {
            let committed = match crate::storage::keys::index_scan_bounds(&index_scope, &range)? {
                None => Vec::new(),
                Some((mut lower, mut upper)) => {
                    if let Some(after) = after.as_ref() {
                        if reverse {
                            if upper.as_ref().is_none_or(|upper| after < upper) {
                                upper = Some(after.clone());
                            }
                        } else if lower.as_ref().is_none_or(|lower| after > lower) {
                            lower = Some(after.clone());
                        }
                    }
                    let range = ScanRange {
                        lower: lower.as_deref(),
                        upper: upper.as_deref(),
                        limit: Some(cap),
                        reverse,
                    };
                    self.read
                        .scan_index(&self.session.db, index, arity, &prefix_refs, range)?
                }
            };
            let truncated = committed.len() >= cap;
            // Past the last committed entry a truncated read loaded (in
            // scan direction), staged entries would jump unread ones.
            let mut horizon: Option<Vec<u8>> = None;
            // Merge committed and staged entries by full encoded bytes.
            let mut merged: BTreeMap<Vec<u8>, (IndexEntry, bool)> = BTreeMap::new();
            for (components, primary) in committed {
                let refs: Vec<&[u8]> = components.iter().map(Vec::as_slice).collect();
                let full = encode_index_key(&self.session.db, index, &refs, &primary)?;
                if truncated {
                    horizon = Some(full.clone());
                }
                merged.insert(full, ((components, primary), true));
            }
            for (components, primary, present) in &staged {
                let refs: Vec<&[u8]> = components.iter().map(Vec::as_slice).collect();
                let full = encode_index_key(&self.session.db, index, &refs, primary)?;
                merged.insert(full, ((components.clone(), primary.clone()), *present));
            }
            let mut ordered: Vec<(Vec<u8>, IndexEntry)> = merged
                .into_iter()
                .filter(|(_, (_, present))| *present)
                .map(|(full, (entry, _))| (full, entry))
                .collect();
            if reverse {
                ordered.reverse();
            }
            let mut rows = Vec::new();
            let mut cursor: Option<String> = None;
            for (full, (components, primary)) in ordered {
                if let Some(after) = after.as_ref() {
                    let past = if reverse {
                        full < *after
                    } else {
                        full > *after
                    };
                    if !past {
                        continue;
                    }
                }
                if let Some(horizon) = horizon.as_ref()
                    && (if reverse {
                        full < *horizon
                    } else {
                        full > *horizon
                    })
                {
                    break;
                }
                if !range_within(&range, &components, &primary) {
                    continue;
                }
                if rows.len() >= limit as usize {
                    break;
                }
                let record = match self.read_record(collection, &primary)? {
                    None => continue,
                    Some(json) => parse_value(&json)?,
                };
                let key = parse_value(&primary)?;
                let record = match guard.as_deref() {
                    None => record,
                    Some(policy) => {
                        self.session.spend(FUEL_ROW)?;
                        match self.shown(&module, policy, &key, record, &index_fields)? {
                            None => continue,
                            Some(record) => record,
                        }
                    }
                };
                rows.push(Value::Array(vec![key, record]));
                cursor = Some(crate::hex::encode(&full));
                self.session.spend(FUEL_ROW)?;
            }
            if guard.is_none() || rows.len() >= limit as usize || !truncated {
                break (rows, cursor);
            }
            cap = cap.saturating_mul(2);
        };
        let dep = encode_range_dep(index, &range)?;
        self.session.record_dep(dep);
        let full = rows_full(&rows, limit);
        Ok(encode_page(rows, cursor.filter(|_| full)))
    }

    fn op_call(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 4, "call takes [module, name, args, savepoint]")?;
        let module = self.normalize(expect_str(&args[0], "call module")?)?;
        let name = expect_str(&args[1], "call name")?.to_string();
        let call_args = args[2].clone();
        let savepoint = match &args[3] {
            Value::Bool(savepoint) => *savepoint,
            _ => {
                return Err(OpError::from(fail(
                    codes::TYPE_MISMATCH,
                    "call savepoint is not a boolean",
                )));
            }
        };
        // Unknown and non-callable targets are deterministic and
        // side-effect-free, so they recover even without a savepoint: there
        // is nothing to roll back and no poison to record. So do methods
        // another module keeps private, unknown or not.
        if let Some(refusal) = self.call_refusal(&module, &name) {
            return Ok(encode_recovered(&refusal));
        }
        let kind = self
            .descriptors
            .get(&module)
            .and_then(|defs| defs.def_kinds.get(&name))
            .copied();
        let Some(kind) = kind else {
            return Ok(encode_recovered(&fail(
                codes::UNKNOWN_METHOD,
                format!("unknown definition `{name}`"),
            )));
        };
        let kind = match kind {
            DefKind::Query | DefKind::Mutation => kind,
            DefKind::Derived | DefKind::Transaction => {
                return Ok(encode_recovered(&fail(
                    codes::INVALID_INVOCATION,
                    format!("`{name}` is not callable"),
                )));
            }
        };
        // Dispatch validates arguments before running the callee. Invalid
        // arguments are deterministic with no effects, so they recover
        // even without a savepoint, like unknown targets.
        if let Some(schema) = self
            .descriptors
            .get(&module)
            .and_then(|defs| defs.args_schema(&name))
            && let Err(error) = schema.parse(&call_args)
        {
            return Ok(encode_recovered(
                &error.invalid_argument(&format!("arguments for `{name}`")),
            ));
        }
        self.session.check_call(kind)?;
        self.policy
            .allow_call(
                self.session.principal.as_ref(),
                &self.session.module.clone(),
                &module,
                &name,
            )
            .map_err(OpError::from)?;
        self.session.spend(FUEL_CALL)?;
        self.session.push_savepoint();
        let call_kind = match kind {
            DefKind::Query => CallKind::Query,
            _ => CallKind::Mutation,
        };
        // Definer rights stay in their module: called elsewhere, the
        // callee runs as an anonymous caller under its own policies.
        let cross = module != self.session.module;
        // Policies folded for the definer's principal leave with it.
        let lowered = (cross && !self.session.guarded()).then(|| {
            (
                self.session.replace_definer(0),
                self.session.principal.take(),
                std::mem::take(&mut self.session.folds),
            )
        });
        let mut outcome = self.invoke_nested(call_kind, &module, &name, &call_args);
        if let Some((definer, principal, folds)) = lowered {
            self.session.replace_definer(definer);
            self.session.principal = principal;
            self.session.folds = folds;
        }
        // Another module's answer must keep the contract it exports. A
        // breach is the callee's failure: rolled back, recoverable by a
        // savepoint, otherwise poisoning.
        if cross && let Ok(answer) = &outcome {
            let checked = match answer {
                Ok(value) => Some(Ok(value.clone())),
                Err(failure) if failure.recovered => Some(Err(failure.failure.clone())),
                Err(_) => None,
            };
            if let Some(breach) =
                checked.and_then(|checked| self.contract_breach(&module, &name, &checked))
            {
                outcome = Ok(Err(CallFailure {
                    failure: breach,
                    recovered: true,
                }));
            }
        }
        match outcome {
            Ok(Ok(value)) => {
                self.session.pop_savepoint(false);
                Ok(encode_ok(value))
            }
            Ok(Err(failure)) => {
                self.session.pop_savepoint(true);
                if savepoint && failure.recovered {
                    Ok(encode_recovered(&failure.failure))
                } else {
                    Err(OpError::fatal(failure.failure))
                }
            }
            Err(error) => {
                self.session.pop_savepoint(true);
                Err(error)
            }
        }
    }

    /// Upsert one row. An optional fifth member names fields to clear:
    /// hidden fields a caller leaves out otherwise keep their stored
    /// values (see [`crate::access`]).
    fn op_set(&mut self, args: Value) -> OpResult<()> {
        let args = match args {
            Value::Array(items) if items.len() == 5 => items,
            other => expect_array(other, 4, "set takes [module, name, key, value, clear?]")?,
        };
        let module = self.normalize(expect_str(&args[0], "set module")?)?;
        let name = expect_str(&args[1], "set collection")?.to_string();
        self.check_write_target(&module, &name)?;
        let key_text = expect_str(&args[2], "set key")?.to_string();
        let value = args[3].clone();
        let clear = crate::access::cleared_fields(args.get(4))?;
        self.session.check_write()?;
        self.policy
            .allow_write(self.session.principal.as_ref(), &module, &name)
            .map_err(OpError::from)?;
        if name.starts_with("$raw/") {
            let Value::String(text) = value else {
                return Err(OpError::from(fail(
                    codes::INVALID_VALUE,
                    "raw values must be strings",
                )));
            };
            let (collection, framed) = self.row_key(&module, &name, &key_text, true)?;
            self.session
                .staged
                .stage_record(collection, framed, Some(text.into_bytes()));
            return Ok(());
        }
        let def = self.collection_def(&module, &name)?.clone();
        let key = self.canon_key(&key_text)?;
        let parsed_key = parse_value(&key)?;
        if !def.key_schema.is(&parsed_key) {
            return Err(OpError::from(schema_failure("key", &name)));
        }
        // Policy admission first: it may carry hidden fields over from the
        // stored row, and the schema then checks the value as stored.
        let value = match self.guard(&module, &name) {
            None => value,
            Some(policy) => {
                let collection = self.collection_id(&module, &name)?;
                let previous = self
                    .read_record(collection, &key)?
                    .map(|json| parse_value(&json))
                    .transpose()?;
                self.admit_put(
                    &module,
                    &name,
                    &policy,
                    &parsed_key,
                    previous.as_ref(),
                    value,
                    &clear,
                )?
            }
        };
        if let Err(error) = def.value_schema.parse(&value) {
            return Err(OpError::from(fail(
                codes::SCHEMA_MISMATCH,
                format!(
                    "value for `{name}` invalid at {}: {}",
                    error.path, error.message
                ),
            )));
        }
        // Guest values that cannot render (non-finite numbers) are guest
        // errors, not store corruption.
        let json = render(&value)
            .map_err(|_| OpError::from(fail(codes::INVALID_VALUE, "value is not storable")))?;
        let collection = self.collection_id(&module, &name)?;
        self.maintain_indexes(&module, &def, collection, &key, Some(&json))?;
        self.session
            .staged
            .stage_record(collection, key, Some(json));
        Ok(())
    }

    /// Stage the delete of a descriptor row, recording the removed row
    /// for retention hooks when one existed: every successful delete is
    /// an event, delete-then-recreate included. A permanent delete is
    /// recorded as such, so its hooks (and its cascades') skip it.
    pub(crate) fn stage_deletion(
        &mut self,
        collection: u32,
        key: Vec<u8>,
        permanent: bool,
    ) -> OpResult<()> {
        if let Some(value) = self.read_record(collection, &key)? {
            self.session
                .staged
                .record_deletion(crate::session::Deletion {
                    collection,
                    key: key.clone(),
                    value,
                    permanent,
                });
        }
        self.session.staged.stage_record(collection, key, None);
        Ok(())
    }

    /// Stage index maintenance for a record write: drop entries derived
    /// from the old image, add entries for the new one. Missing and null
    /// fields stay unindexed.
    pub(crate) fn maintain_indexes(
        &mut self,
        module: &str,
        def: &CollectionDef,
        collection: u32,
        key: &[u8],
        new: Option<&[u8]>,
    ) -> OpResult<()> {
        let old = self.read_record(collection, key)?;
        for index in &def.indexes {
            let qualified = qualify_index(module, &def.name, &index.name);
            let Some(id) = self
                .read
                .intern_id(&self.session.db, qualified.as_bytes())?
            else {
                continue;
            };
            if let Some(old) = old.as_ref()
                && let Some(components) = index_components(old, &index.fields)?
            {
                self.session
                    .staged
                    .stage_index(id, components, key.to_vec(), false);
            }
            if let Some(new) = new
                && let Some(components) = index_components(new, &index.fields)?
            {
                self.session
                    .staged
                    .stage_index(id, components, key.to_vec(), true);
            }
        }
        Ok(())
    }

    fn op_delete(&mut self, args: Value) -> OpResult<()> {
        // An optional fourth member marks the delete permanent: it skips
        // retention hooks, and so do the deletes it cascades into.
        let args = match args {
            Value::Array(items) if items.len() == 4 => items,
            other => expect_array(other, 3, "delete takes [module, name, key, permanent?]")?,
        };
        let permanent = match args.get(3) {
            None => false,
            Some(Value::Bool(permanent)) => *permanent,
            Some(_) => {
                return Err(OpError::from(fail(
                    codes::TYPE_MISMATCH,
                    "delete permanence is not a boolean",
                )));
            }
        };
        let module = self.normalize(expect_str(&args[0], "delete module")?)?;
        let name = expect_str(&args[1], "delete collection")?.to_string();
        self.check_write_target(&module, &name)?;
        let key_text = expect_str(&args[2], "delete key")?.to_string();
        self.session.check_write()?;
        self.policy
            .allow_write(self.session.principal.as_ref(), &module, &name)
            .map_err(OpError::from)?;
        if name.starts_with("$raw/") {
            let (collection, framed) = self.row_key(&module, &name, &key_text, true)?;
            self.session.staged.stage_record(collection, framed, None);
            return Ok(());
        }
        let def = self.collection_def(&module, &name)?.clone();
        let key = self.canon_key(&key_text)?;
        let collection = self.collection_id(&module, &name)?;
        if let Some(policy) = self.guard(&module, &name) {
            let previous = self
                .read_record(collection, &key)?
                .map(|json| parse_value(&json))
                .transpose()?;
            let parsed_key = parse_value(&key)?;
            if !self.admit_delete(&module, &name, &policy, &parsed_key, previous.as_ref())? {
                return Ok(());
            }
        }
        self.maintain_indexes(&module, &def, collection, &key, None)?;
        self.stage_deletion(collection, key, permanent)?;
        Ok(())
    }

    fn op_delete_if_match(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(
            args,
            4,
            "delete_if_match takes [module, name, key, expected]",
        )?;
        let module = self.normalize(expect_str(&args[0], "delete_if_match module")?)?;
        let name = expect_str(&args[1], "delete_if_match collection")?.to_string();
        self.check_write_target(&module, &name)?;
        let key_text = expect_str(&args[2], "delete_if_match key")?.to_string();
        let expected = args[3].clone();
        self.session.check_write()?;
        self.policy
            .allow_write(self.session.principal.as_ref(), &module, &name)
            .map_err(OpError::from)?;
        if name.starts_with("$raw/") {
            let Value::String(text) = expected else {
                return Err(OpError::from(fail(
                    codes::INVALID_VALUE,
                    "raw expected values must be strings",
                )));
            };
            let (collection, framed) = self.row_key(&module, &name, &key_text, true)?;
            let matched = self.read_record(collection, &framed)? == Some(text.into_bytes());
            if matched {
                self.session.staged.stage_record(collection, framed, None);
            }
            return Ok(Value::Bool(matched));
        }
        let def = self.collection_def(&module, &name)?.clone();
        let key = self.canon_key(&key_text)?;
        let expected = render(&expected).map_err(|_| {
            OpError::from(fail(codes::INVALID_VALUE, "expected value is not storable"))
        })?;
        let collection = self.collection_id(&module, &name)?;
        let stored = self.read_record(collection, &key)?;
        let matched = match self.guard(&module, &name) {
            None => stored == Some(expected),
            // A caller matches against the row as it may see it: a
            // hidden row is a missing one, and hidden fields aren't there.
            Some(policy) => match stored.map(|json| parse_value(&json)).transpose()? {
                None => false,
                Some(previous) => {
                    let parsed_key = parse_value(&key)?;
                    match self.shown(&module, &policy, &parsed_key, previous.clone(), &[])? {
                        None => false,
                        Some(seen) => {
                            render(&seen)? == expected
                                && self.admit_delete(
                                    &module,
                                    &name,
                                    &policy,
                                    &parsed_key,
                                    Some(&previous),
                                )?
                        }
                    }
                }
            },
        };
        if matched {
            self.maintain_indexes(&module, &def, collection, &key, None)?;
            self.stage_deletion(collection, key, false)?;
        }
        Ok(Value::Bool(matched))
    }

    /// Stage a durable queue item: the enqueue commits atomically with
    /// the mutation and replicates as ordinary records. Takes `[queue,
    /// payload, delay_ms, max_attempts]` (guests built before scopes: the
    /// default scope, priority 0) or those plus `[scope, priority,
    /// group-or-null, name-or-null, replace]`. The payload must render
    /// and meet every declared payload contract of the queue; a name
    /// already taken in the scope fails `JOB_EXISTS` unless `replace` and
    /// that job is completed or dead.
    fn op_enqueue(&mut self, args: Value) -> OpResult<Value> {
        let args = match args {
            Value::Array(items) if items.len() == 4 || items.len() == 9 => items,
            _ => {
                return Err(OpError::from(fail(
                    codes::INVALID_VALUE,
                    "enqueue takes [queue, payload, delay_ms, max_attempts, scope, priority, group, name, replace]",
                )));
            }
        };
        let queue = expect_str(&args[0], "enqueue queue")?.to_string();
        let payload = args[1].clone();
        let delay_ms = expect_u64(&args[2], "enqueue delay")?;
        let max_attempts = expect_u32(&args[3], "enqueue max attempts")?;
        let optional = |value: &Value, what: &str| -> OpResult<Option<String>> {
            match value {
                Value::Null => Ok(None),
                value => Ok(Some(expect_str(value, what)?.to_string())),
            }
        };
        let (scope, priority, group, name, replace) = if args.len() == 9 {
            (
                expect_str(&args[4], "enqueue scope")?.to_string(),
                expect_i64(&args[5], "enqueue priority")?,
                optional(&args[6], "enqueue group")?,
                optional(&args[7], "enqueue name")?,
                match &args[8] {
                    Value::Bool(replace) => *replace,
                    _ => {
                        return Err(OpError::from(fail(
                            codes::TYPE_MISMATCH,
                            "enqueue replace takes a boolean",
                        )));
                    }
                },
            )
        } else {
            (String::new(), 0, None, None, false)
        };
        self.session.check_write()?;
        let item = crate::work_queue::NewItem {
            queue: &queue,
            payload: &payload,
            delay_ms,
            max_attempts,
            scope: &scope,
            priority,
            group: group.as_deref(),
            name: name.as_deref(),
            replace,
        };
        item.validate()?;
        render(&payload)
            .map_err(|_| OpError::from(fail(codes::INVALID_VALUE, "payload is not storable")))?;
        self.queue_collection()?;
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        let random = crate::work_queue::random_128();
        let id = self.queue_plan(&queue, |view| view.enqueue(&item, now_ms, &random))?;
        Ok(Value::String(id))
    }

    /// Resolve the queue collection, or fail when uninitialized.
    fn queue_collection(&self) -> OpResult<u32> {
        self.read
            .intern_id(&self.session.db, QUEUE_COLLECTION)?
            .ok_or_else(|| OpError::from(fail(codes::INVALID_VALUE, "queues are not initialized")))
    }

    /// Plan one queue operation over the invocation's staged overlay
    /// and stage its writes: the mutation sees its own enqueues and
    /// claims, and they commit with it. Plans see the queue's declared
    /// contracts and the database's history incarnation. The calling
    /// module must declare the queue: a queue's work belongs to its
    /// declarers, so a purge never deletes another module's (see
    /// [`crate::work_queue`]).
    fn queue_plan<T>(
        &mut self,
        queue: &str,
        plan: impl FnOnce(
            &crate::work_queue::QueueView<crate::work_queue::Overlay>,
        ) -> crate::work_queue::Plan<T>,
    ) -> OpResult<T> {
        crate::work_queue::validate_queue(queue)?;
        let module = &self.session.module;
        let declared = self
            .descriptors
            .get(module)
            .is_some_and(|defs| defs.queues.iter().any(|decl| decl.name == queue));
        if !declared {
            return Err(OpError::from(fail(
                crate::work_queue::QUEUE_UNDECLARED,
                format!(
                    "module `{module}` does not declare queue `{queue}`: list its \
                     `TypedQueue` (or `QueueDef`) among the app's components"
                ),
            )));
        }
        let collection = self.read.intern_id(&self.session.db, QUEUE_COLLECTION)?;
        let incarnation = self
            .read
            .meta_u64(&self.session.db, crate::storage::meta::HISTORY_INCARNATION)?;
        let lineage = crate::placement::lineage(self.read, &self.session.db)?;
        let config = crate::work_queue::QueueConfig::of(self.descriptors.values(), queue);
        let (outcome, writes) = {
            let overlay = crate::work_queue::Overlay {
                read: self.read,
                staged: &self.session.staged,
            };
            let view = crate::work_queue::QueueView {
                read: &overlay,
                db: &self.session.db,
                collection,
                queue,
                incarnation,
                lineage,
                config,
            };
            plan(&view)?
        };
        for write in writes {
            self.session
                .staged
                .stage_record(write.collection, write.key, write.value);
        }
        Ok(outcome)
    }

    /// Claim up to `limit` claimable items of one scope under fenced
    /// leases: lowest priority first, then group turns, then the longest
    /// waiting. Takes `[queue, scope, owner-or-null, limit, lease_ms,
    /// wait_ms-or-null]`; with a wait, a claim that comes back short
    /// keeps the owner's place in the scope's line.
    fn op_queue_claim(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(
            args,
            6,
            "queue claim takes [queue, scope, owner, limit, lease_ms, wait_ms]",
        )?;
        let queue = expect_str(&args[0], "queue claim queue")?.to_string();
        let scope = expect_str(&args[1], "queue claim scope")?.to_string();
        let owner = match &args[2] {
            Value::Null => None,
            owner => Some(expect_str(owner, "queue claim owner")?.to_string()),
        };
        let limit = expect_u64(&args[3], "queue claim limit")? as usize;
        let lease_ms = expect_u64(&args[4], "queue claim lease")?;
        let wait_ms = match &args[5] {
            Value::Null => None,
            wait => Some(expect_u64(wait, "queue claim wait")?),
        };
        self.session.check_write()?;
        if limit == 0 || limit > crate::work_queue::MAX_POLL_LIMIT {
            return Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "claim limit out of range",
            )));
        }
        let request = crate::work_queue::ClaimRequest {
            scope: &scope,
            owner: owner.as_deref(),
            limit,
            lease_ms,
            wait_ms,
        };
        request.validate()?;
        self.queue_collection()?;
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        let tokens: Vec<String> = (0..limit).map(|_| crate::work_queue::new_token()).collect();
        let claimed = self.queue_plan(&queue, |view| view.claim(&request, now_ms, &tokens))?;
        self.session.spend(FUEL_ROW * claimed.len() as u64)?;
        Ok(Value::Array(
            claimed
                .iter()
                .map(crate::work_queue::ClaimedItem::to_value)
                .collect(),
        ))
    }

    /// Take, keep or leave a place in a scope's line without claiming:
    /// `[queue, scope, owner, room, wait_ms]`, leaving for a room or wait
    /// of 0.
    fn op_queue_wait(&mut self, args: Value) -> OpResult<()> {
        let args = expect_array(
            args,
            5,
            "queue wait takes [queue, scope, owner, room, wait_ms]",
        )?;
        let queue = expect_str(&args[0], "queue wait queue")?.to_string();
        let scope = expect_str(&args[1], "queue wait scope")?.to_string();
        let owner = expect_str(&args[2], "queue wait owner")?.to_string();
        let room = expect_u64(&args[3], "queue wait room")?;
        let wait_ms = expect_u64(&args[4], "queue wait")?;
        self.session.check_write()?;
        self.queue_collection()?;
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        self.queue_plan(&queue, |view| {
            view.wait(&scope, &owner, room, wait_ms, now_ms)
        })
    }

    /// Renew claims, one new expiry per lease, or null for a lease no
    /// longer held (reported, lapsed, claimed again, or taken in another
    /// history).
    fn op_queue_renew(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 3, "queue renew takes [queue, leases, lease_ms]")?;
        let queue = expect_str(&args[0], "queue renew queue")?.to_string();
        let leases = as_array(&args[1], "queue renew leases")?.clone();
        let lease_ms = expect_u64(&args[2], "queue renew lease")?;
        self.session.check_write()?;
        if leases.len() > crate::work_queue::MAX_RENEWALS {
            return Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "renewals per call are at most 1024",
            )));
        }
        crate::work_queue::validate_lease(lease_ms)?;
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        let mut renewed = Vec::with_capacity(leases.len());
        for lease in leases {
            let lease = expect_array(lease, 2, "a lease is [id, token]")?;
            let id = expect_str(&lease[0], "lease id")?;
            let token = expect_str(&lease[1], "lease token")?;
            let held = self.queue_plan(&queue, |view| {
                Ok(view
                    .heartbeat(id, token, lease_ms, now_ms)
                    .ok()
                    .map_or((None, Vec::new()), |(expires, writes)| {
                        (Some(expires), writes)
                    }))
            })?;
            renewed.push(held.map_or(Value::Null, |expires| Value::Number(expires as f64)));
        }
        Ok(Value::Array(renewed))
    }

    /// Report on a claim: `[queue, id, token, action, delay_ms,
    /// result]`. `complete` keeps the item with its result (checked
    /// against the queue's declared result contracts), `finish` deletes
    /// it, and `fail`, `kill` and `release` act as their worker
    /// operations. A wrong token, a claim that settled or one taken in
    /// another history fails as a business failure.
    fn op_queue_report(&mut self, args: Value) -> OpResult<()> {
        let args = expect_array(
            args,
            6,
            "queue report takes [queue, id, token, action, delay_ms, result]",
        )?;
        let queue = expect_str(&args[0], "queue report queue")?.to_string();
        let id = expect_str(&args[1], "queue report id")?.to_string();
        let token = expect_str(&args[2], "queue report token")?.to_string();
        let action = expect_str(&args[3], "queue report action")?.to_string();
        let delay_ms = expect_u64(&args[4], "queue report delay")?;
        let result = args[5].clone();
        self.session.check_write()?;
        if delay_ms > crate::work_queue::MAX_DELAY_MS {
            return Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "report delay is too long",
            )));
        }
        render(&result)
            .map_err(|_| OpError::from(fail(codes::INVALID_VALUE, "result is not storable")))?;
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        match action.as_str() {
            "complete" => self.queue_plan(&queue, |view| {
                view.complete(&id, &token, Some(&result), now_ms)
            }),
            "finish" => self.queue_plan(&queue, |view| view.complete(&id, &token, None, now_ms)),
            "fail" => self.queue_plan(&queue, |view| view.fail(&id, &token, now_ms, delay_ms)),
            "kill" => self.queue_plan(&queue, |view| view.kill(&id, &token, now_ms)),
            "release" => {
                self.queue_plan(&queue, |view| view.release(&id, &token, now_ms, delay_ms))
            }
            _ => Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "queue report action is complete, finish, fail, kill or release",
            ))),
        }
    }

    /// A job named by its ID (text) or by `{scope, name}`.
    fn job_ref(value: &Value) -> OpResult<(String, Option<String>)> {
        match value {
            Value::String(id) => Ok((id.clone(), None)),
            Value::Object(fields) => match (fields.get("scope"), fields.get("name")) {
                (Some(Value::String(scope)), Some(Value::String(name))) => {
                    Ok((scope.clone(), Some(name.clone())))
                }
                _ => Err(OpError::from(fail(
                    codes::TYPE_MISMATCH,
                    "a job is an ID or {scope, name}",
                ))),
            },
            _ => Err(OpError::from(fail(
                codes::TYPE_MISMATCH,
                "a job is an ID or {scope, name}",
            ))),
        }
    }

    /// Delete a queue item in any state, reporting whether one existed:
    /// `[queue, job]`.
    fn op_queue_cancel(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 2, "queue cancel takes [queue, job]")?;
        let queue = expect_str(&args[0], "queue cancel queue")?.to_string();
        let (first, name) = Self::job_ref(&args[1])?;
        self.session.check_write()?;
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        self.queue_plan(&queue, |view| view.cancel(&job(&first, &name), now_ms))
            .map(Value::Bool)
    }

    /// Requeue a dead queue item with a fresh attempt budget: `[queue,
    /// job, delay_ms]`.
    fn op_queue_retry(&mut self, args: Value) -> OpResult<()> {
        let args = expect_array(args, 3, "queue retry takes [queue, job, delay_ms]")?;
        let queue = expect_str(&args[0], "queue retry queue")?.to_string();
        let (first, name) = Self::job_ref(&args[1])?;
        let delay_ms = expect_u64(&args[2], "queue retry delay")?;
        self.session.check_write()?;
        if delay_ms > crate::work_queue::MAX_DELAY_MS {
            return Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "retry delay is too long",
            )));
        }
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        self.queue_plan(&queue, |view| {
            view.retry(&job(&first, &name), now_ms, delay_ms)
        })
    }

    /// Delete dead and completed items enqueued before a cutoff:
    /// `[queue, older_than_ms]`, answering how many.
    fn op_queue_purge(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 2, "queue purge takes [queue, older_than_ms]")?;
        let queue = expect_str(&args[0], "queue purge queue")?.to_string();
        let older_than_ms = expect_u64(&args[1], "queue purge cutoff")?;
        self.session.check_write()?;
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        let purged = self.queue_plan(&queue, |view| view.purge(older_than_ms, now_ms))?;
        self.session.spend(FUEL_ROW * purged as u64)?;
        Ok(Value::Number(purged as f64))
    }

    /// Read one queue item, in queries and mutations alike: `[queue,
    /// job]`. A claim on its last attempt reads dead once its lease
    /// lapses, so a query's result changes then.
    fn op_queue_get(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 2, "queue get takes [queue, job]")?;
        let queue = expect_str(&args[0], "queue get queue")?.to_string();
        let (first, name) = Self::job_ref(&args[1])?;
        // Queue rows invalidate no derived outcome (see
        // `patch::apply_journal`), so reading them names no dependency.
        self.session.record_volatile();
        let now_ms = self.session.timestamp_ms;
        let item = self.queue_plan(&queue, |view| view.get(&job(&first, &name), now_ms))?;
        if let Some(item) = &item
            && item.state == crate::work_queue::ItemState::Claimed
            && item.attempts >= item.max_attempts
        {
            self.changes_by(item.claimed_until_ms)?;
        }
        Ok(item.map_or(Value::Null, |item| item.to_value()))
    }

    /// Declare that a query's result changes at `at_ms`, unless it
    /// already changes sooner.
    fn changes_by(&mut self, at_ms: u64) -> OpResult<()> {
        Ok(self.session.declare_change(at_ms as f64)?)
    }

    /// Count one scope's items (or every scope's, for a null scope) and
    /// say whether a claim would take one now — with an owner, only once
    /// it is that owner's turn in the scope's line: `[queue, scope,
    /// owner]`. Claims that lapsed on their last attempt count as dead. A
    /// query's result changes when the next delayed item falls due, a
    /// lease lapses or the line's turn passes.
    fn op_queue_stats(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 3, "queue stats takes [queue, scope, owner]")?;
        let queue = expect_str(&args[0], "queue stats queue")?.to_string();
        let scope = match &args[1] {
            Value::Null => None,
            scope => Some(expect_str(scope, "queue stats scope")?.to_string()),
        };
        let owner = match &args[2] {
            Value::Null => None,
            owner => Some(expect_str(owner, "queue stats owner")?.to_string()),
        };
        self.session.record_volatile();
        let now_ms = self.session.timestamp_ms;
        let ((stats, lapse), readiness) = self.queue_plan(&queue, |view| {
            let counts = view.counts(scope.as_deref(), now_ms)?;
            let readiness =
                view.readiness(scope.as_deref().unwrap_or(""), owner.as_deref(), now_ms)?;
            Ok(((counts, readiness), Vec::new()))
        })?;
        self.session.spend(
            FUEL_ROW * (stats.pending + stats.claimed + stats.dead + stats.completed) as u64,
        )?;
        for next in [readiness.next_ms, lapse].into_iter().flatten() {
            self.changes_by(next)?;
        }
        let mut map = BTreeMap::new();
        for (name, count) in [
            ("pending", stats.pending),
            ("claimed", stats.claimed),
            ("dead", stats.dead),
            ("completed", stats.completed),
        ] {
            map.insert(name.to_string(), Value::Number(count as f64));
        }
        map.insert("ready".to_string(), Value::Bool(readiness.ready));
        map.insert(
            "next_ms".to_string(),
            readiness
                .next_ms
                .map_or(Value::Null, |next| Value::Number(next as f64)),
        );
        Ok(Value::Object(map))
    }

    /// Stage record writes in order.
    fn stage_rows(&mut self, collection: u32, rows: Vec<RowWrite>) {
        for (key, value) in rows {
            self.session.staged.stage_record(collection, key, value);
        }
    }

    /// Resolve the timer collection, or fail when uninitialized.
    fn timer_collection(&self) -> OpResult<u32> {
        self.read
            .intern_id(&self.session.db, TIMER_COLLECTION)?
            .ok_or_else(|| OpError::from(fail(codes::INVALID_VALUE, "timers are not initialized")))
    }

    /// Find one timer by ID, staged-or-committed: guests see their own
    /// schedules.
    fn timer_by_id(&self, collection: u32, id: &str) -> OpResult<Option<crate::timer::Timer>> {
        crate::timer::find(|key| self.read_record(collection, key), id)
    }

    /// Stage a timer, replacing any same-ID timer. The handler must
    /// name a same-module mutation and the arguments must validate
    /// against its schema now; firing uses deployed code then.
    fn op_timer_schedule(&mut self, args: Value) -> OpResult<()> {
        let args = expect_array(
            args,
            5,
            "schedule takes [id, due_ms, handler, args, max_attempts]",
        )?;
        let id = expect_str(&args[0], "schedule id")?.to_string();
        let due_ms = expect_u64(&args[1], "schedule due")?;
        let handler = expect_str(&args[2], "schedule handler")?.to_string();
        let timer_args = args[3].clone();
        let max_attempts = expect_u32(&args[4], "schedule max attempts")?;
        self.session.check_write()?;
        crate::timer::validate_schedule(&id, &handler, max_attempts)?;
        render(&timer_args)
            .map_err(|_| OpError::from(fail(codes::INVALID_VALUE, "args are not storable")))?;
        let module = self.session.module.clone();
        let defs = self
            .descriptors
            .get(&module)
            .ok_or_else(|| OpError::from(fail(codes::INVALID_VALUE, "unknown module")))?;
        match defs.def_kinds.get(&handler) {
            Some(DefKind::Mutation) => {}
            Some(_) => {
                return Err(OpError::from(fail(
                    codes::INVALID_VALUE,
                    format!("handler `{handler}` must be a mutation"),
                )));
            }
            None => {
                return Err(OpError::from(fail(
                    codes::INVALID_VALUE,
                    format!("unknown handler `{handler}`"),
                )));
            }
        }
        if let Some(schema) = defs.args_schema(&handler)
            && let Err(error) = schema.parse(&timer_args)
        {
            return Err(OpError::from(
                error.invalid_argument(&format!("arguments for `{handler}`")),
            ));
        }
        let collection = self.timer_collection()?;
        self.session.poll_time();
        let now_ms = self.session.timestamp_ms;
        // Same-ID scheduling replaces: drop the old state row first.
        if let Some(old) = self.timer_by_id(collection, &id)? {
            self.stage_rows(collection, crate::timer::removal(&old));
        }
        let timer = crate::timer::Timer {
            id,
            module,
            handler,
            args: timer_args,
            state: crate::timer::TimerState::Pending,
            attempts: 0,
            max_attempts,
            enqueued_ms: now_ms,
            due_ms,
        };
        self.stage_rows(collection, crate::timer::timer_rows(&timer)?);
        Ok(())
    }

    /// Cancel a timer, reporting whether one existed.
    fn op_timer_cancel(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 1, "timer cancel takes [id]")?;
        let id = expect_str(&args[0], "timer cancel id")?.to_string();
        self.session.check_write()?;
        crate::timer::validate_id(&id)?;
        let collection = self.timer_collection()?;
        match self.timer_by_id(collection, &id)? {
            Some(timer) => {
                self.stage_rows(collection, crate::timer::removal(&timer));
                Ok(Value::Bool(true))
            }
            None => Ok(Value::Bool(false)),
        }
    }

    /// Read one timer, in queries and mutations alike.
    fn op_timer_get(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 1, "timer get takes [id]")?;
        let id = expect_str(&args[0], "timer get id")?;
        crate::timer::validate_id(id)?;
        // Timer rows invalidate no derived outcome (see
        // `patch::apply_journal`), so reading them names no dependency.
        self.session.record_volatile();
        let Some(collection) = self.read.intern_id(&self.session.db, TIMER_COLLECTION)? else {
            return Ok(Value::Null);
        };
        match self.timer_by_id(collection, id)? {
            Some(timer) => Ok(crate::timer::timer_value(&timer)),
            None => Ok(Value::Null),
        }
    }

    /// Scan timers due-first, in queries and mutations alike. A null
    /// state scans pending timers; `"dead"` scans dead letters. Each
    /// state is one key range in due order, merged with the overlay.
    fn op_timer_scan(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 2, "timer scan takes [state, limit]")?;
        let wanted = match &args[0] {
            Value::Null => crate::timer::TimerState::Pending,
            Value::String(state) if state == "pending" => crate::timer::TimerState::Pending,
            Value::String(state) if state == "dead" => crate::timer::TimerState::Dead,
            _ => {
                return Err(OpError::from(fail(
                    codes::INVALID_VALUE,
                    "timer scan state is null, pending or dead",
                )));
            }
        };
        let limit = expect_u32(&args[1], "timer scan limit")? as usize;
        if limit == 0 || limit > crate::timer::MAX_SCAN_LIMIT {
            return Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "scan limit out of range",
            )));
        }
        self.session.record_volatile();
        let Some(collection) = self.read.intern_id(&self.session.db, TIMER_COLLECTION)? else {
            return Ok(Value::Array(Vec::new()));
        };
        let staged: Vec<(Vec<u8>, Option<Vec<u8>>)> = self
            .session
            .staged
            .staged_records_for(collection)
            .into_iter()
            .filter(|(key, _)| crate::timer::in_class(key, wanted))
            .collect();
        // Every staged row can displace at most one committed row, so
        // the page plus the staged count covers the merged page.
        let (lower, upper) =
            crate::timer::class_bounds(&self.session.db, collection, wanted, None)?;
        let committed = self.read.scan_records(
            &self.session.db,
            collection,
            Some(&lower),
            Some(&upper),
            Some(limit + staged.len()),
            false,
        )?;
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = committed.into_iter().collect();
        for (key, value) in staged {
            match value {
                Some(value) => {
                    merged.insert(key, value);
                }
                None => {
                    merged.remove(&key);
                }
            }
        }
        let mut out = Vec::new();
        for (key, value) in merged.into_iter().take(limit) {
            out.push(crate::timer::timer_value(&crate::timer::decode_timer(
                &key, &value,
            )?));
        }
        Ok(Value::Array(out))
    }

    /// Requeue a dead timer with a fresh attempt budget.
    fn op_timer_retry(&mut self, args: Value) -> OpResult<()> {
        let args = expect_array(args, 2, "timer retry takes [id, due_ms]")?;
        let id = expect_str(&args[0], "timer retry id")?.to_string();
        let due_ms = expect_u64(&args[1], "timer retry due")?;
        self.session.check_write()?;
        crate::timer::validate_id(&id)?;
        let collection = self.timer_collection()?;
        let Some(mut timer) = self.timer_by_id(collection, &id)? else {
            return Err(OpError::from(fail(codes::INVALID_VALUE, "unknown timer")));
        };
        if timer.state != crate::timer::TimerState::Dead {
            return Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "only dead timers retry",
            )));
        }
        self.stage_rows(collection, crate::timer::removal(&timer));
        timer.state = crate::timer::TimerState::Pending;
        timer.attempts = 0;
        timer.due_ms = due_ms;
        self.stage_rows(collection, crate::timer::timer_rows(&timer)?);
        Ok(())
    }

    fn op_materialize(&mut self, args: Value, pin: bool) -> OpResult<()> {
        let args = expect_array(args, 3, "materialize takes [module, name, args]")?;
        let module = self.normalize(expect_str(&args[0], "materialize module")?)?;
        let name = expect_str(&args[1], "materialize name")?.to_string();
        self.check_derived(&module, &name)?;
        let call_args = args[2].clone();
        self.session.check_write()?;
        if pin {
            self.materialize(&module, &name, &call_args)?;
        } else {
            self.unmaterialize(&module, &name, &call_args)?;
        }
        Ok(())
    }

    fn op_history(&mut self, args: Value) -> OpResult<Value> {
        let args = expect_array(args, 4, "history takes [module, name, key, limit]")?;
        let module = self.normalize(expect_str(&args[0], "history module")?)?;
        let name = expect_str(&args[1], "history collection")?.to_string();
        self.check_read(&module, &name, false)?;
        let key = self.canon_key(expect_str(&args[2], "history key")?)?;
        let limit = scan_limit(&args[3])?;
        let collection = self.collection_id(&module, &name)?;
        // Retained versions also age out of the log tail without any
        // commit to the row.
        self.session.record_volatile();
        self.read.record_untracked();
        let versions = self
            .history
            .history(&self.session.db, collection, &key, limit)?;
        let Some(policy) = self.guard(&module, &name) else {
            return Ok(encode_versions(&versions)?);
        };
        // Callers see the versions their read rule shows, redacted, and
        // a deletion only of a version they saw (versions run newest
        // first, so a deletion removed the next one).
        let key = parse_value(&key)?;
        let mut shown: Vec<Option<Option<Vec<u8>>>> = Vec::with_capacity(versions.len());
        for version in &versions {
            shown.push(match &version.value {
                None => Some(None),
                Some(bytes) => {
                    match self.shown(&module, &policy, &key, parse_value(bytes)?, &[])? {
                        None => None,
                        Some(row) => Some(Some(render(&row)?)),
                    }
                }
            });
        }
        let mut visible = Vec::with_capacity(versions.len());
        for (index, version) in versions.iter().enumerate() {
            let keep = match &shown[index] {
                None => None,
                Some(Some(row)) => Some(Some(row.clone())),
                Some(None) => match shown.get(index + 1) {
                    Some(Some(Some(_))) => Some(None),
                    _ => None,
                },
            };
            if let Some(value) = keep {
                visible.push(crate::history::Version {
                    revision: version.revision,
                    value,
                });
            }
        }
        Ok(encode_versions(&visible)?)
    }

    fn op_time(&mut self) -> OpResult<Value> {
        self.session.poll_time();
        Ok(Value::Number(self.session.timestamp_ms as f64))
    }

    /// The invocation timestamp without polling: the guest declares
    /// with `changes_at` when its result next depends on it.
    fn op_clock(&self) -> Value {
        Value::Number(self.session.timestamp_ms as f64)
    }

    /// `[instant]`, as v1's `changesAt`: the earliest instant declared
    /// stands, so no later declaration lifts one (a token's expiry
    /// that crypto declared, say), and null declares nothing. Inside a
    /// derived value, the instant goes with its outcome to every reader.
    fn op_changes_at(&mut self, args: Value) -> OpResult<()> {
        let args = expect_array(args, 1, "changes_at takes [instant]")?;
        match &args[0] {
            Value::Null => Ok(()),
            Value::Number(at) => Ok(self.session.declare_change(*at)?),
            _ => Err(OpError::from(fail(
                codes::TYPE_MISMATCH,
                "changes_at takes a number or null",
            ))),
        }
    }

    fn op_principal(&mut self) -> OpResult<Value> {
        // Derived values run without a caller: their outcomes cache and
        // serve every caller, so none may depend on who asked first.
        if self.session.kind == CallerKind::Derived {
            return Ok(Value::Null);
        }
        match &self.session.principal {
            None => Ok(Value::Null),
            Some(principal) => Principal::encode(principal).map_err(OpError::fatal),
        }
    }

    fn op_caller(&self) -> Value {
        match &self.session.caller {
            None => Value::Null,
            Some(caller) => Value::String(caller.clone()),
        }
    }
}

impl HostCall for Host<'_> {
    fn op(&mut self, op: u8, input: &[u8]) -> Vec<u8> {
        let envelope = self.dispatch(op, input);
        envelope.encode_binary().unwrap_or_default()
    }

    fn read_untracked(&mut self) {
        self.session.record_volatile();
        self.read.record_untracked();
    }

    fn read_keys(&mut self) {
        self.session.record_keys();
    }
}

/// Raw storage key: length-prefixed namespace plus length-prefixed key.
fn raw_key(namespace: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let mut framed = Vec::with_capacity(namespace.len() + key.len() + 8);
    push_sized(&mut framed, namespace)?;
    push_sized(&mut framed, key)?;
    Ok(framed)
}

/// Byte prefix selecting one raw namespace below the record prefix.
fn raw_namespace_prefix(namespace: &[u8]) -> Result<Vec<u8>, Error> {
    let mut prefix = Vec::with_capacity(namespace.len() + 4);
    let len = u32::try_from(namespace.len()).map_err(|_| Error::corrupt("component too long"))?;
    prefix.extend_from_slice(&len.to_be_bytes());
    prefix.extend_from_slice(namespace);
    Ok(prefix)
}

/// Split a framed raw key back into namespace and original key.
fn decode_raw_key(key: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let mut reader = crate::storage::keys::Reader::new(key);
    let namespace = reader.sized()?;
    let original = reader.sized()?;
    if reader.pos() != key.len() {
        return Err(Error::corrupt("trailing raw key bytes"));
    }
    Ok((namespace, original))
}

/// Extract index components from canonical JSON: missing and null fields
/// stay unindexed, so the whole entry is skipped.
pub(crate) fn index_components(
    json: &[u8],
    fields: &[String],
) -> Result<Option<Vec<Vec<u8>>>, Error> {
    let value = parse_value(json)?;
    let map = match value {
        Value::Object(map) => map,
        _ => return Err(Error::corrupt("indexed row is not an object")),
    };
    let mut components = Vec::with_capacity(fields.len());
    for field in fields {
        match map.get(field) {
            None | Some(Value::Null) => return Ok(None),
            Some(value) => components.push(render(value)?),
        }
    }
    Ok(Some(components))
}

/// Whether ordered components plus primary fall inside range bounds.
fn range_within(range: &RangeDep, components: &[Vec<u8>], primary: &[u8]) -> bool {
    crate::storage::keys::index_range_matches(range, components, primary)
}

/// Decode a hex cursor back to entry bytes; garbage is a guest error.
fn decode_cursor(text: &str, what: &str) -> OpResult<Vec<u8>> {
    crate::hex::decode(text).ok_or_else(|| OpError::from(fail(codes::INVALID_VALUE, what)))
}

fn opt_str(value: &Value, what: &str) -> OpResult<Option<Value>> {
    match value {
        Value::Null => Ok(None),
        Value::String(_) => Ok(Some(value.clone())),
        _ => Err(OpError::from(fail(codes::TYPE_MISMATCH, what))),
    }
}

/// Page envelope: rows plus continuation cursor.
fn encode_page(rows: Vec<Value>, cursor: Option<String>) -> Value {
    let mut map = BTreeMap::new();
    map.insert("rows".to_string(), Value::Array(rows));
    map.insert(
        "cursor".to_string(),
        cursor.map(Value::String).unwrap_or(Value::Null),
    );
    Value::Object(map)
}

/// Whether a page filled its limit (and so may continue).
fn rows_full(rows: &[Value], limit: u32) -> bool {
    rows.len() >= limit as usize && limit > 0
}

fn scan_limit(value: &Value) -> OpResult<u32> {
    let limit = match value {
        Value::Number(limit) if limit.is_finite() && limit.fract() == 0.0 && *limit > 0.0 => {
            u32::try_from(*limit as u64)
                .map_err(|_| OpError::from(fail(codes::INVALID_VALUE, "limit out of range")))?
        }
        _ => {
            return Err(OpError::from(fail(
                codes::INVALID_VALUE,
                "limit must be positive",
            )));
        }
    };
    Ok(limit.min(MAX_SCAN_LIMIT))
}

fn schema_failure(what: &str, name: &str) -> Failure {
    fail(
        codes::SCHEMA_MISMATCH,
        format!("{what} for `{name}` violates its schema"),
    )
}

fn expect_array(value: Value, len: usize, what: &str) -> OpResult<Vec<Value>> {
    match value {
        Value::Array(items) if items.len() == len => Ok(items),
        _ => Err(OpError::from(fail(codes::INVALID_VALUE, what))),
    }
}

fn expect_str<'a>(value: &'a Value, what: &str) -> OpResult<&'a str> {
    match value {
        Value::String(text) => Ok(text.as_str()),
        _ => Err(OpError::from(fail(codes::TYPE_MISMATCH, what))),
    }
}

fn expect_u64(value: &Value, what: &str) -> OpResult<u64> {
    match value {
        Value::Number(n) if n.is_finite() && n.fract() == 0.0 && *n >= 0.0 => Ok(*n as u64),
        _ => Err(OpError::from(fail(
            codes::TYPE_MISMATCH,
            format!("{what} takes a non-negative integer"),
        ))),
    }
}

/// A job reference from `Host::job_ref`'s parts: an ID, or a scope
/// and a name.
fn job<'a>(first: &'a str, name: &'a Option<String>) -> crate::work_queue::JobRef<'a> {
    match name {
        Some(name) => crate::work_queue::JobRef::Named { scope: first, name },
        None => crate::work_queue::JobRef::Id(first),
    }
}

fn expect_i64(value: &Value, what: &str) -> OpResult<i64> {
    match value {
        Value::Number(n)
            if n.is_finite() && n.fract() == 0.0 && n.abs() < 9_007_199_254_740_992.0 =>
        {
            Ok(*n as i64)
        }
        _ => Err(OpError::from(fail(
            codes::TYPE_MISMATCH,
            format!("{what} takes a safe integer"),
        ))),
    }
}

fn expect_u32(value: &Value, what: &str) -> OpResult<u32> {
    let value = expect_u64(value, what)?;
    u32::try_from(value).map_err(|_| {
        OpError::from(fail(
            codes::TYPE_MISMATCH,
            format!("{what} takes a non-negative integer"),
        ))
    })
}

fn as_object<'a>(value: &'a Value, what: &str) -> OpResult<&'a BTreeMap<String, Value>> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(OpError::from(fail(codes::TYPE_MISMATCH, what))),
    }
}

fn as_array<'a>(value: &'a Value, what: &str) -> OpResult<&'a Vec<Value>> {
    match value {
        Value::Array(items) => Ok(items),
        _ => Err(OpError::from(fail(codes::TYPE_MISMATCH, what))),
    }
}

fn as_str<'a>(value: &'a Value, what: &str) -> OpResult<&'a str> {
    expect_str(value, what)
}

fn get_member<'a>(map: &'a BTreeMap<String, Value>, key: &str) -> OpResult<&'a Value> {
    map.get(key)
        .ok_or_else(|| OpError::from(fail(codes::TYPE_MISMATCH, "range bounds are incomplete")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_decode_as_hex_or_fail_as_guest_errors() {
        assert_eq!(
            decode_cursor("00abff10", "cursor").unwrap(),
            vec![0x00, 0xAB, 0xFF, 0x10]
        );
        let error = decode_cursor("AB", "cursor").unwrap_err();
        assert!(!error.fatal);
        assert_eq!(error.failure.code, codes::INVALID_VALUE);
    }

    #[test]
    fn raw_names_fold_to_one_namespace() {
        assert_eq!(qualify_raw("m", "$raw/ns"), "m\0ns");
        assert_eq!(qualify_raw("m", "ns"), "m\0ns");
    }

    #[test]
    fn qualified_names_split_or_reject() {
        assert_eq!(
            split_qualified(b"m\0tasks").unwrap(),
            ("m".to_string(), "tasks".to_string())
        );
        assert!(split_qualified(b"tasks").is_err());
        assert!(split_qualified(b"\xff\xfe").is_err());
    }

    #[test]
    fn raw_keys_frame_namespace_and_key() {
        let framed = raw_key(b"m\0ns", b"k").unwrap();
        let (namespace, key) = decode_raw_key(&framed).unwrap();
        assert_eq!(namespace, b"m\0ns");
        assert_eq!(key, b"k");
        assert!(decode_raw_key(b"short").is_err());
    }

    #[test]
    fn limits_must_be_positive_and_capped() {
        assert_eq!(scan_limit(&Value::Number(10.0)).unwrap(), 10);
        assert_eq!(
            scan_limit(&Value::Number(1_000_000.0)).unwrap(),
            MAX_SCAN_LIMIT
        );
        assert!(scan_limit(&Value::Number(0.0)).is_err());
        assert!(scan_limit(&Value::Number(-1.0)).is_err());
        assert!(scan_limit(&Value::Number(1.5)).is_err());
        assert!(scan_limit(&Value::Null).is_err());
    }
}
