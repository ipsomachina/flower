//! Raw contexts: [`Ctx`], capability traits and typed context wrappers.
//!
//! [`Ctx`] is the low-level `Ctx`/`Value` layer: every method takes and
//! returns [`Value`], addressed by [`CollectionRef`]. The host enforces
//! capabilities per invocation kind; Rust types document but never replace
//! that enforcement.
//!
//! Keys are values: every keyed operation takes the key as a [`Value`]
//! (typed handles take it typed) and renders the canonical text the host
//! compares itself; raw `$raw/` namespaces take string keys verbatim.
//! Scan and range responses carry keys back as decoded [`Value`]s.
//!
//! Capability traits ([`ReadCtx`], [`MethodCtx`]) expose the same operations
//! as generic bounds for typed handles. [`QueryCtx`], [`MutationCtx`] and
//! [`DerivedCtx`] are transparent wrappers a callback receives through the
//! [`wrap_query`], [`wrap_mutation`] and [`wrap_derived`] adapters. Sealed
//! traits plus crate-private constructors keep user code from manufacturing
//! capabilities: only dispatch builds [`Ctx`], and wrappers need one to wrap.

use alloc::string::String;
use alloc::vec::Vec;
use core::marker::PhantomData;

use crate::abi::HostOp;
use crate::auth::Principal;
use crate::error::{Failure, Result, codes, fail};
use crate::typed::FlowerType;
use crate::value::Value;

/// Module-local collection reference. `""` addresses the calling module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectionRef<'a> {
    /// Calling module is `""`.
    pub module: &'a str,
    /// Collection name.
    pub name: &'a str,
}

impl<'a> CollectionRef<'a> {
    /// Reference a collection in a module.
    pub const fn new(module: &'a str, name: &'a str) -> Self {
        CollectionRef { module, name }
    }

    /// Reference a collection in the calling module.
    pub const fn local(name: &'a str) -> Self {
        CollectionRef { module: "", name }
    }
}

/// One raw scan row: decoded key plus value.
#[derive(Clone, Debug, PartialEq)]
pub struct RawRow {
    /// Decoded key.
    pub key: Value,
    /// Stored value.
    pub value: Value,
}

/// Bounded scan page with a host-opaque continuation cursor.
#[derive(Clone, Debug, PartialEq)]
pub struct RawPage {
    /// Rows in host order.
    pub rows: Vec<RawRow>,
    /// Continuation cursor, if more rows may follow.
    pub cursor: Option<String>,
}

/// Raw index range: prefix components and bounds are values, rendered to
/// canonical text at the call, so the host compares bytes opaquely.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeBounds {
    /// Leading prefix components, one per indexed field.
    pub prefix: Vec<Value>,
    /// Exclusive lower bound, if any.
    pub gt: Option<Value>,
    /// Inclusive lower bound, if any.
    pub gte: Option<Value>,
    /// Exclusive upper bound, if any.
    pub lt: Option<Value>,
    /// Inclusive upper bound, if any.
    pub lte: Option<Value>,
    /// Maximum rows; must be positive.
    pub limit: u32,
    /// Continuation cursor, if resuming.
    pub after: Option<String>,
    /// Scan descending when true.
    pub reverse: bool,
}

impl RangeBounds {
    /// Empty bounds with a row limit. The call still rejects a zero limit.
    pub fn limited(limit: u32) -> Self {
        RangeBounds {
            prefix: Vec::new(),
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            limit,
            after: None,
            reverse: false,
        }
    }
}

/// The text a host operation addresses `key` by in `collection`: its
/// canonical JSON, or a raw `$raw/` namespace's verbatim string.
fn key_arg(collection: CollectionRef<'_>, key: &Value) -> Result<Value> {
    if collection.name.starts_with("$raw/") {
        return match key {
            Value::String(text) => Ok(Value::String(text.clone())),
            _ => Err(fail(codes::TYPE_MISMATCH, "raw storage keys are strings")),
        };
    }
    Ok(Value::String(key.canonical_json()?))
}

/// One bound or prefix component as the host compares it.
fn component(value: Value) -> Result<Value> {
    Ok(Value::String(value.canonical_json()?))
}

/// One retained row version, newest first as retained.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryEntry {
    /// Committing revision. Revisions stay within the safe-integer interval.
    pub revision: u64,
    /// Row value, or `None` for a deletion marker.
    pub value: Option<Value>,
}

/// Method-call failure with savepoint recovery state.
#[derive(Clone, Debug, PartialEq)]
pub struct CallFailure {
    /// The failure.
    pub failure: Failure,
    /// True when a savepoint call rolled back and the caller may continue.
    pub recovered: bool,
}

/// Savepoint call outcome.
#[derive(Clone, Debug, PartialEq)]
pub enum CallOutcome<T, E = Failure> {
    /// Call succeeded.
    Success(T),
    /// Call failed and the host confirmed recovery.
    Failure(E),
}

/// Unrecoverable call error: fatal, budget-exhausted or non-savepoint.
#[derive(Clone, Debug, PartialEq)]
pub enum CallError {
    /// Host-side failure.
    Host(Failure),
}

mod seal {
    pub trait Sealed {}
}

/// Read capability: collections, indexes, derived values and time.
#[allow(private_bounds)]
pub trait ReadCtx: seal::Sealed {
    /// Read one row by key, a value (a `$raw/` namespace takes its key
    /// text verbatim).
    fn get(&mut self, collection: CollectionRef<'_>, key: &Value) -> Result<Option<Value>>;
    /// Read one row from unkeyed raw string storage.
    fn raw_get(&mut self, collection: CollectionRef<'_>, key: &str) -> Result<Option<Value>>;
    /// Read a derived instance, computing on demand. Stored computation
    /// errors surface as failures.
    fn get_derived(&mut self, module: &str, name: &str, args: &Value) -> Result<Value>;
    /// Scan canonical keys in host order from an optional cursor.
    fn scan(
        &mut self,
        collection: CollectionRef<'_>,
        limit: u32,
        after: Option<&str>,
    ) -> Result<RawPage>;
    /// Scan unkeyed raw string storage.
    fn raw_scan(
        &mut self,
        collection: CollectionRef<'_>,
        limit: u32,
        after: Option<&str>,
    ) -> Result<RawPage>;
    /// Bounded index range scan.
    fn range(
        &mut self,
        collection: CollectionRef<'_>,
        index: &str,
        bounds: RangeBounds,
    ) -> Result<RawPage>;
    /// Invocation timestamp in milliseconds since the Unix epoch. Implies
    /// time-polled behavior.
    fn now(&mut self) -> Result<f64>;
    /// Same timestamp as [`now`][ReadCtx::now] without the polling
    /// implication; pair with [`changes_at`][ReadCtx::changes_at].
    fn clock(&mut self) -> Result<f64>;
    /// Declare an instant when the result may next change: the earliest
    /// one declared stands, and `None` declares nothing.
    fn changes_at(&mut self, at: Option<f64>) -> Result<()>;
    /// One host operation, for the SDK's own read handles (queues,
    /// timers): the host still gates it by the invocation's kind.
    #[doc(hidden)]
    fn host_op(&mut self, op: HostOp, args: Value) -> Result<Value>;
}

/// Method capability: identity plus exported method calls.
#[allow(private_bounds)]
pub trait MethodCtx: ReadCtx {
    /// Calling principal, or `None` for anonymous invocations.
    fn principal(&mut self) -> Result<Option<Principal>>;
    /// Calling module identity, or `None` for external entry.
    fn caller(&mut self) -> Result<Option<String>>;
    /// Call an exported method. Failures poison the invocation.
    fn call(&mut self, module: &str, name: &str, args: &Value) -> Result<Value>;
    /// Call with a savepoint: recoverable failures roll back and report
    /// [`CallOutcome::Failure`]; fatal failures stay fatal.
    fn try_call(
        &mut self,
        module: &str,
        name: &str,
        args: &Value,
    ) -> core::result::Result<CallOutcome<Value>, CallError>;
}

/// Raw invocation context. Constructed only by dispatch; neither `Send` nor
/// `Sync`; never returned as an owned result.
pub struct Ctx<'c, 'r> {
    _borrow: PhantomData<&'c mut &'r mut ()>,
    _pinned: PhantomData<*mut ()>,
}

impl<'c, 'r> Ctx<'c, 'r> {
    pub(crate) fn new() -> Self {
        Ctx {
            _borrow: PhantomData,
            _pinned: PhantomData,
        }
    }

    pub(crate) fn call(&mut self, op: HostOp, args: Value) -> Result<Value> {
        let input = args.encode_binary()?;
        let output = crate::abi::host_call(op, &input)?;
        let response = Value::decode_binary(&output)?;
        crate::abi::decode_envelope(response)
    }

    fn call_savepoint(
        &mut self,
        op: HostOp,
        args: Value,
    ) -> core::result::Result<Value, CallFailure> {
        let input = match args.encode_binary() {
            Ok(input) => input,
            Err(failure) => {
                return Err(CallFailure {
                    failure,
                    recovered: false,
                });
            }
        };
        let output = match crate::abi::host_call(op, &input) {
            Ok(output) => output,
            Err(failure) => {
                return Err(CallFailure {
                    failure,
                    recovered: false,
                });
            }
        };
        let response = match Value::decode_binary(&output) {
            Ok(response) => response,
            Err(failure) => {
                return Err(CallFailure {
                    failure,
                    recovered: false,
                });
            }
        };
        crate::abi::decode_call_envelope(response)
    }

    /// Read one row by key.
    pub fn get(&mut self, collection: CollectionRef<'_>, key: &Value) -> Result<Option<Value>> {
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            key_arg(collection, key)?,
        ]);
        match self.call(HostOp::Get, args)? {
            Value::Null => Ok(None),
            value => Ok(Some(value)),
        }
    }

    /// Read one row from unkeyed raw string storage.
    pub fn raw_get(&mut self, collection: CollectionRef<'_>, key: &str) -> Result<Option<Value>> {
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            Value::String(String::from(key)),
        ]);
        match self.call(HostOp::RawGet, args)? {
            Value::Null => Ok(None),
            value => Ok(Some(value)),
        }
    }

    /// Read a derived instance, computing on demand.
    pub fn get_derived(&mut self, module: &str, name: &str, args: &Value) -> Result<Value> {
        let call_args = Value::Array(alloc::vec![
            Value::String(String::from(module)),
            Value::String(String::from(name)),
            args.clone(),
        ]);
        self.call(HostOp::GetDerived, call_args)
    }

    /// Scan canonical keys in host order from an optional cursor.
    pub fn scan(
        &mut self,
        collection: CollectionRef<'_>,
        limit: u32,
        after: Option<&str>,
    ) -> Result<RawPage> {
        check_limit(limit)?;
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            Value::Number(limit as f64),
            match after {
                Some(cursor) => Value::String(String::from(cursor)),
                None => Value::Null,
            },
        ]);
        decode_page(self.call(HostOp::Scan, args)?)
    }

    /// Scan unkeyed raw string storage.
    pub fn raw_scan(
        &mut self,
        collection: CollectionRef<'_>,
        limit: u32,
        after: Option<&str>,
    ) -> Result<RawPage> {
        check_limit(limit)?;
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            Value::Number(limit as f64),
            match after {
                Some(cursor) => Value::String(String::from(cursor)),
                None => Value::Null,
            },
        ]);
        decode_page(self.call(HostOp::RawScan, args)?)
    }

    /// Bounded index range scan.
    pub fn range(
        &mut self,
        collection: CollectionRef<'_>,
        index: &str,
        bounds: RangeBounds,
    ) -> Result<RawPage> {
        check_limit(bounds.limit)?;
        let mut spec = alloc::collections::BTreeMap::new();
        let mut prefix = Vec::with_capacity(bounds.prefix.len());
        for value in bounds.prefix {
            prefix.push(component(value)?);
        }
        spec.insert(String::from("prefix"), Value::Array(prefix));
        for (name, bound) in [
            ("gt", bounds.gt),
            ("gte", bounds.gte),
            ("lt", bounds.lt),
            ("lte", bounds.lte),
        ] {
            let bound = match bound {
                Some(value) => component(value)?,
                None => Value::Null,
            };
            spec.insert(String::from(name), bound);
        }
        spec.insert(String::from("limit"), Value::Number(bounds.limit as f64));
        spec.insert(
            String::from("after"),
            match bounds.after {
                Some(cursor) => Value::String(cursor),
                None => Value::Null,
            },
        );
        spec.insert(String::from("reverse"), Value::Bool(bounds.reverse));
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            Value::String(String::from(index)),
            Value::Object(spec),
        ]);
        decode_page(self.call(HostOp::Range, args)?)
    }

    /// Call an exported method of any kind; the host routes by target kind
    /// and enforces caller capability. `savepoint` requests rollback instead
    /// of poisoning on recoverable failure.
    pub fn method_call(
        &mut self,
        module: &str,
        name: &str,
        args: &Value,
        savepoint: bool,
    ) -> core::result::Result<Value, CallFailure> {
        let call_args = Value::Array(alloc::vec![
            Value::String(String::from(module)),
            Value::String(String::from(name)),
            args.clone(),
            Value::Bool(savepoint),
        ]);
        self.call_savepoint(HostOp::Call, call_args)
    }

    /// Upsert one row.
    pub fn set(&mut self, collection: CollectionRef<'_>, key: &Value, value: Value) -> Result<()> {
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            key_arg(collection, key)?,
            value,
        ]);
        self.call(HostOp::Set, args)?;
        Ok(())
    }

    /// Upsert one row, removing the fields `clear` names where `value`
    /// leaves them out. A caller's write otherwise keeps the stored values
    /// of fields its access policy hides from it, so a read-modify-write
    /// of a redacted row loses nothing; clearing a hidden field takes the
    /// field's write rule, whether or not the row holds it. At most
    /// [`MAX_CLEARED_FIELDS`](crate::access::MAX_CLEARED_FIELDS) names.
    pub fn set_clearing(
        &mut self,
        collection: CollectionRef<'_>,
        key: &Value,
        value: Value,
        clear: &[&str],
    ) -> Result<()> {
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            key_arg(collection, key)?,
            value,
            Value::Array(
                clear
                    .iter()
                    .map(|field| Value::String(String::from(*field)))
                    .collect()
            ),
        ]);
        self.call(HostOp::Set, args)?;
        Ok(())
    }

    /// Delete one row.
    pub fn delete(&mut self, collection: CollectionRef<'_>, key: &Value) -> Result<()> {
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            key_arg(collection, key)?,
        ]);
        self.call(HostOp::Delete, args)?;
        Ok(())
    }

    /// Delete one row permanently: retention hooks skip it, and the
    /// deletes it cascades into.
    pub fn delete_permanently(&mut self, collection: CollectionRef<'_>, key: &Value) -> Result<()> {
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            key_arg(collection, key)?,
            Value::Bool(true),
        ]);
        self.call(HostOp::Delete, args)?;
        Ok(())
    }

    /// Delete one row only when its current value equals `expected`.
    pub fn delete_if_match(
        &mut self,
        collection: CollectionRef<'_>,
        key: &Value,
        expected: &Value,
    ) -> Result<bool> {
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            key_arg(collection, key)?,
            expected.clone(),
        ]);
        match self.call(HostOp::DeleteIfMatch, args)? {
            Value::Bool(matched) => Ok(matched),
            _ => Err(fail(codes::TYPE_MISMATCH, "delete_if_match not a boolean")),
        }
    }

    /// Retain a materialized derived root.
    pub fn materialize(&mut self, module: &str, name: &str, args: &Value) -> Result<()> {
        let call_args = Value::Array(alloc::vec![
            Value::String(String::from(module)),
            Value::String(String::from(name)),
            args.clone(),
        ]);
        self.call(HostOp::Materialize, call_args)?;
        Ok(())
    }

    /// Release a materialized derived root.
    pub fn unmaterialize(&mut self, module: &str, name: &str, args: &Value) -> Result<()> {
        let call_args = Value::Array(alloc::vec![
            Value::String(String::from(module)),
            Value::String(String::from(name)),
            args.clone(),
        ]);
        self.call(HostOp::Unmaterialize, call_args)?;
        Ok(())
    }

    /// Run pending trigger settlement and derived propagation to a fixpoint
    /// within budgets.
    pub fn settle(&mut self) -> Result<()> {
        self.call(HostOp::Settle, Value::Array(Vec::new()))?;
        Ok(())
    }

    /// Retained versions of one row, newest first.
    pub fn history(
        &mut self,
        collection: CollectionRef<'_>,
        key: &Value,
        limit: u32,
    ) -> Result<Vec<HistoryEntry>> {
        check_limit(limit)?;
        let args = Value::Array(alloc::vec![
            Value::String(String::from(collection.module)),
            Value::String(String::from(collection.name)),
            key_arg(collection, key)?,
            Value::Number(limit as f64),
        ]);
        let response = self.call(HostOp::History, args)?;
        let items = match response {
            Value::Array(items) => items,
            _ => return Err(fail(codes::TYPE_MISMATCH, "history not an array")),
        };
        let mut entries = Vec::with_capacity(items.len());
        for item in items {
            let (revision, value) = match item {
                Value::Object(mut map) => {
                    let revision = match map.remove("revision") {
                        Some(Value::Number(revision))
                            if revision.is_finite()
                                && revision % 1.0 == 0.0
                                && (0.0..18_446_744_073_709_551_616.0).contains(&revision) =>
                        {
                            revision as u64
                        }
                        _ => {
                            return Err(fail(
                                codes::TYPE_MISMATCH,
                                "history revision not a number",
                            ));
                        }
                    };
                    let value = match map.remove("value") {
                        None | Some(Value::Null) => None,
                        Some(value) => Some(value),
                    };
                    (revision, value)
                }
                _ => return Err(fail(codes::TYPE_MISMATCH, "history entry not an object")),
            };
            entries.push(HistoryEntry { revision, value });
        }
        Ok(entries)
    }

    /// Invocation timestamp in milliseconds since the Unix epoch.
    pub fn now(&mut self) -> Result<f64> {
        match self.call(HostOp::Now, Value::Array(Vec::new()))? {
            Value::Number(now) if now.is_finite() => Ok(now),
            _ => Err(fail(codes::TYPE_MISMATCH, "now not a number")),
        }
    }

    /// Same timestamp as [`now`][Ctx::now] without the polling implication.
    pub fn clock(&mut self) -> Result<f64> {
        match self.call(HostOp::Clock, Value::Array(Vec::new()))? {
            Value::Number(now) if now.is_finite() => Ok(now),
            _ => Err(fail(codes::TYPE_MISMATCH, "clock not a number")),
        }
    }

    /// Declare an instant when the result may next change: the earliest one
    /// declared stands (so no later declaration lifts a token check's
    /// expiry), and `None` declares nothing.
    pub fn changes_at(&mut self, at: Option<f64>) -> Result<()> {
        let args = Value::Array(alloc::vec![match at {
            Some(at) => Value::Number(at),
            None => Value::Null,
        }]);
        self.call(HostOp::ChangesAt, args)?;
        Ok(())
    }

    /// Calling principal, or `None` for anonymous invocations.
    pub fn principal(&mut self) -> Result<Option<Principal>> {
        match self.call(HostOp::Principal, Value::Array(Vec::new()))? {
            Value::Null => Ok(None),
            value => Ok(Some(Principal::decode(value)?)),
        }
    }

    /// Calling module identity, or `None` for external entry.
    pub fn caller(&mut self) -> Result<Option<String>> {
        match self.call(HostOp::Caller, Value::Array(Vec::new()))? {
            Value::Null => Ok(None),
            Value::String(caller) => Ok(Some(caller)),
            _ => Err(fail(codes::TYPE_MISMATCH, "caller not a string")),
        }
    }
}

fn check_limit(limit: u32) -> Result<()> {
    if limit == 0 {
        return Err(fail(codes::INVALID_VALUE, "limit must be positive"));
    }
    Ok(())
}

fn decode_page(response: Value) -> Result<RawPage> {
    let mut map = match response {
        Value::Object(map) => map,
        _ => return Err(fail(codes::TYPE_MISMATCH, "page not an object")),
    };
    let rows = match map.remove("rows") {
        Some(Value::Array(rows)) => rows,
        _ => return Err(fail(codes::TYPE_MISMATCH, "page rows not an array")),
    };
    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        match row {
            Value::Array(pair) if pair.len() == 2 => {
                let mut pair = pair.into_iter();
                let key = pair.next();
                let value = pair.next();
                match (key, value) {
                    (Some(key), Some(value)) => decoded.push(RawRow { key, value }),
                    _ => return Err(fail(codes::TYPE_MISMATCH, "page row not a pair")),
                }
            }
            _ => return Err(fail(codes::TYPE_MISMATCH, "page row not a pair")),
        }
    }
    let cursor = match map.remove("cursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(cursor)) => Some(cursor),
        Some(_) => return Err(fail(codes::TYPE_MISMATCH, "page cursor not a string")),
    };
    Ok(RawPage {
        rows: decoded,
        cursor,
    })
}

/// Query callback context: reads, identity and query calls.
#[repr(transparent)]
pub struct QueryCtx<'c, 'r>(Ctx<'c, 'r>);

/// Mutation callback context: reads, writes, identity and all calls.
#[repr(transparent)]
pub struct MutationCtx<'c, 'r>(Ctx<'c, 'r>);

/// Derived callback context: reads only.
#[repr(transparent)]
pub struct DerivedCtx<'c, 'r>(Ctx<'c, 'r>);

macro_rules! impl_raw_ctx {
    ($($t:ident),*) => {
        $(
            impl<'c, 'r> $t<'c, 'r> {
                /// Borrow the raw invocation context.
                pub fn ctx(&mut self) -> &mut Ctx<'c, 'r> {
                    &mut self.0
                }

                /// The host's cryptography for this invocation.
                pub fn crypto(&mut self) -> crate::crypto::Crypto<'_, 'c, 'r> {
                    crate::crypto::Crypto::new(&mut self.0)
                }
            }
        )*
    };
}

impl_raw_ctx!(QueryCtx, MutationCtx, DerivedCtx);

impl seal::Sealed for Ctx<'_, '_> {}
impl seal::Sealed for QueryCtx<'_, '_> {}
impl seal::Sealed for MutationCtx<'_, '_> {}
impl seal::Sealed for DerivedCtx<'_, '_> {}

/// Wrap a raw context for a query callback. Hidden adapter used by generated
/// shims; safe because callers cannot construct [`Ctx`] themselves, and
/// sound by `repr(transparent)` with identical lifetimes.
#[doc(hidden)]
pub fn wrap_query<'a, 'c, 'r>(ctx: &'a mut Ctx<'c, 'r>) -> &'a mut QueryCtx<'c, 'r> {
    // SAFETY: `QueryCtx` is `repr(transparent)` over `Ctx` with the same
    // lifetimes, and the result reborrows `ctx` exclusively for `'a`.
    unsafe { &mut *(ctx as *mut Ctx<'c, 'r> as *mut QueryCtx<'c, 'r>) }
}

/// Wrap a raw context for a mutation callback. See [`wrap_query`].
#[doc(hidden)]
pub fn wrap_mutation<'a, 'c, 'r>(ctx: &'a mut Ctx<'c, 'r>) -> &'a mut MutationCtx<'c, 'r> {
    // SAFETY: as in `wrap_query`: `repr(transparent)`, same lifetimes.
    unsafe { &mut *(ctx as *mut Ctx<'c, 'r> as *mut MutationCtx<'c, 'r>) }
}

/// Wrap a raw context for a derived callback. See [`wrap_query`].
#[doc(hidden)]
pub fn wrap_derived<'a, 'c, 'r>(ctx: &'a mut Ctx<'c, 'r>) -> &'a mut DerivedCtx<'c, 'r> {
    // SAFETY: as in `wrap_query`: `repr(transparent)`, same lifetimes.
    unsafe { &mut *(ctx as *mut Ctx<'c, 'r> as *mut DerivedCtx<'c, 'r>) }
}

macro_rules! impl_read_ctx {
    ($($t:ty),*) => {
        $(
            impl ReadCtx for $t {
                fn get(&mut self, collection: CollectionRef<'_>, key: &Value) -> Result<Option<Value>> {
                    self.0.get(collection, key)
                }
                fn raw_get(&mut self, collection: CollectionRef<'_>, key: &str) -> Result<Option<Value>> {
                    self.0.raw_get(collection, key)
                }
                fn get_derived(&mut self, module: &str, name: &str, args: &Value) -> Result<Value> {
                    self.0.get_derived(module, name, args)
                }
                fn scan(
                    &mut self,
                    collection: CollectionRef<'_>,
                    limit: u32,
                    after: Option<&str>,
                ) -> Result<RawPage> {
                    self.0.scan(collection, limit, after)
                }
                fn raw_scan(
                    &mut self,
                    collection: CollectionRef<'_>,
                    limit: u32,
                    after: Option<&str>,
                ) -> Result<RawPage> {
                    self.0.raw_scan(collection, limit, after)
                }
                fn range(
                    &mut self,
                    collection: CollectionRef<'_>,
                    index: &str,
                    bounds: RangeBounds,
                ) -> Result<RawPage> {
                    self.0.range(collection, index, bounds)
                }
                fn now(&mut self) -> Result<f64> {
                    self.0.now()
                }
                fn clock(&mut self) -> Result<f64> {
                    self.0.clock()
                }
                fn changes_at(&mut self, at: Option<f64>) -> Result<()> {
                    self.0.changes_at(at)
                }
                fn host_op(&mut self, op: HostOp, args: Value) -> Result<Value> {
                    self.0.call(op, args)
                }
            }
        )*
    };
}

impl_read_ctx!(QueryCtx<'_, '_>, MutationCtx<'_, '_>, DerivedCtx<'_, '_>);

/// The raw context reads like the typed ones, so typed handles work in
/// raw callbacks too; the host still gates each operation by the
/// invocation's kind.
impl ReadCtx for Ctx<'_, '_> {
    fn get(&mut self, collection: CollectionRef<'_>, key: &Value) -> Result<Option<Value>> {
        Ctx::get(self, collection, key)
    }
    fn raw_get(&mut self, collection: CollectionRef<'_>, key: &str) -> Result<Option<Value>> {
        Ctx::raw_get(self, collection, key)
    }
    fn get_derived(&mut self, module: &str, name: &str, args: &Value) -> Result<Value> {
        Ctx::get_derived(self, module, name, args)
    }
    fn scan(
        &mut self,
        collection: CollectionRef<'_>,
        limit: u32,
        after: Option<&str>,
    ) -> Result<RawPage> {
        Ctx::scan(self, collection, limit, after)
    }
    fn raw_scan(
        &mut self,
        collection: CollectionRef<'_>,
        limit: u32,
        after: Option<&str>,
    ) -> Result<RawPage> {
        Ctx::raw_scan(self, collection, limit, after)
    }
    fn range(
        &mut self,
        collection: CollectionRef<'_>,
        index: &str,
        bounds: RangeBounds,
    ) -> Result<RawPage> {
        Ctx::range(self, collection, index, bounds)
    }
    fn now(&mut self) -> Result<f64> {
        Ctx::now(self)
    }
    fn clock(&mut self) -> Result<f64> {
        Ctx::clock(self)
    }
    fn changes_at(&mut self, at: Option<f64>) -> Result<()> {
        Ctx::changes_at(self, at)
    }
    fn host_op(&mut self, op: HostOp, args: Value) -> Result<Value> {
        Ctx::call(self, op, args)
    }
}

macro_rules! impl_method_ctx {
    ($($t:ty),*) => {
        $(
            impl MethodCtx for $t {
                fn principal(&mut self) -> Result<Option<Principal>> {
                    self.0.principal()
                }
                fn caller(&mut self) -> Result<Option<String>> {
                    self.0.caller()
                }
                fn call(&mut self, module: &str, name: &str, args: &Value) -> Result<Value> {
                    match self.0.method_call(module, name, args, false) {
                        Ok(value) => Ok(value),
                        Err(failure) => Err(failure.failure),
                    }
                }
                fn try_call(
                    &mut self,
                    module: &str,
                    name: &str,
                    args: &Value,
                ) -> core::result::Result<CallOutcome<Value>, CallError> {
                    match self.0.method_call(module, name, args, true) {
                        Ok(value) => Ok(CallOutcome::Success(value)),
                        Err(failure) if failure.recovered => {
                            Ok(CallOutcome::Failure(failure.failure))
                        }
                        Err(failure) => Err(CallError::Host(failure.failure)),
                    }
                }
            }
        )*
    };
}

impl_method_ctx!(QueryCtx<'_, '_>, MutationCtx<'_, '_>);

impl<'c, 'r> MutationCtx<'c, 'r> {
    /// Upsert one local row.
    pub fn set(&mut self, collection: CollectionRef<'_>, key: &Value, value: Value) -> Result<()> {
        self.0.set(collection, key, value)
    }

    /// Upsert one local row, removing the hidden fields `clear` names
    /// (see [`Ctx::set_clearing`]).
    pub fn set_clearing(
        &mut self,
        collection: CollectionRef<'_>,
        key: &Value,
        value: Value,
        clear: &[&str],
    ) -> Result<()> {
        self.0.set_clearing(collection, key, value, clear)
    }

    /// Delete one local row.
    pub fn delete(&mut self, collection: CollectionRef<'_>, key: &Value) -> Result<()> {
        self.0.delete(collection, key)
    }

    /// Delete one local row permanently: retention hooks skip it, and
    /// the deletes it cascades into.
    pub fn delete_permanently(&mut self, collection: CollectionRef<'_>, key: &Value) -> Result<()> {
        self.0.delete_permanently(collection, key)
    }

    /// Delete one local row only when its current value equals `expected`.
    pub fn delete_if_match(
        &mut self,
        collection: CollectionRef<'_>,
        key: &Value,
        expected: &Value,
    ) -> Result<bool> {
        self.0.delete_if_match(collection, key, expected)
    }

    /// Call an exported mutation. Failures poison the invocation.
    pub fn call_mutation(&mut self, module: &str, name: &str, args: &Value) -> Result<Value> {
        match self.0.method_call(module, name, args, false) {
            Ok(value) => Ok(value),
            Err(failure) => Err(failure.failure),
        }
    }

    /// Call an exported mutation with a savepoint.
    pub fn try_mutation(
        &mut self,
        module: &str,
        name: &str,
        args: &Value,
    ) -> core::result::Result<CallOutcome<Value>, CallError> {
        match self.0.method_call(module, name, args, true) {
            Ok(value) => Ok(CallOutcome::Success(value)),
            Err(failure) if failure.recovered => Ok(CallOutcome::Failure(failure.failure)),
            Err(failure) => Err(CallError::Host(failure.failure)),
        }
    }

    /// Retain a materialized derived root.
    pub fn materialize(&mut self, module: &str, name: &str, args: &Value) -> Result<()> {
        self.0.materialize(module, name, args)
    }

    /// Release a materialized derived root.
    pub fn unmaterialize(&mut self, module: &str, name: &str, args: &Value) -> Result<()> {
        self.0.unmaterialize(module, name, args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_reject_zero_without_host_calls() {
        let mut ctx = Ctx::new();
        let collection = CollectionRef::local("tasks");
        assert!(ctx.scan(collection, 0, None).is_err());
        assert!(ctx.raw_scan(collection, 0, None).is_err());
        assert!(ctx.history(collection, &Value::from("k"), 0).is_err());
        let bounds = RangeBounds {
            prefix: alloc::vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            limit: 0,
            after: None,
            reverse: false,
        };
        assert!(ctx.range(collection, "byOwner", bounds).is_err());
    }

    #[test]
    fn wrappers_preserve_capability_types() {
        fn assert_read<T: ReadCtx>() {}
        fn assert_method<T: MethodCtx>() {}
        assert_read::<QueryCtx<'_, '_>>();
        assert_read::<MutationCtx<'_, '_>>();
        assert_read::<DerivedCtx<'_, '_>>();
        assert_method::<QueryCtx<'_, '_>>();
        assert_method::<MutationCtx<'_, '_>>();
    }

    #[test]
    fn page_decoding_rejects_malformed_shapes() {
        assert!(decode_page(Value::Null).is_err());
        let mut map = alloc::collections::BTreeMap::new();
        map.insert(String::from("rows"), Value::Null);
        assert!(decode_page(Value::Object(map)).is_err());
    }
}
