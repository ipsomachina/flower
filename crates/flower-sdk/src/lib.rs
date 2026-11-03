//! `flower-sdk`: guest runtime, codecs, descriptors and transactional components.
//!
//! The SDK links `core` plus `alloc` only and has no runtime dependencies.
//! The `flower-sdk-macros` dependency expands on the host at build time and
//! emits no guest code of its own. Native interface and client code reuses
//! the descriptors and codecs without becoming a Wasm application.
//!
//! Module map:
//!
//! - [`error`]: [`Failure`] and the [`Result`] alias plus the [`fail`] helpers.
//! - [`value`]: the canonical [`Value`] model, validation, canonical JSON and
//!   the tagged binary codec used across the guest/host ABI.
//! - [`schema`]: validatable [`schema::Schema`] contracts built with
//!   [`schema::v`].
//! - [`typed`]: the [`typed::FlowerType`] wire contract and built-in types.
//! - [`auth`]: the [`auth::Principal`] identity wire type and the
//!   [`auth::Authorization`] component behind a module's authorization hook.
//! - [`access`]: declarative collection and derived access rules.
//! - [`ctx`]: the raw [`ctx::Ctx`] layer, capability traits and typed
//!   context wrappers.
//! - [`features`]: explicit raw bridges for mutation callbacks, typed
//!   triggers, retention hooks and maintenance tasks.
//! - [`external`]: values computed outside Flower and published back by a
//!   reconcile worker.
//! - [`index`]: typed index handles, typed source pages and delta-maintained
//!   aggregates.
//! - [`app`]: application assembly ([`App`], methods, definitions,
//!   collections, references, triggers, transactions).
//! - [`abi`]: guest exports, invocation dispatch, manifests and [`export!`].
//!
//! Depth and validity rule used throughout: untrusted input is validated
//! iteratively before any recursive traversal, so recursion below always runs
//! under the [`value::MAX_VALUE_DEPTH`] bound.

#![cfg_attr(not(test), no_std)]

extern crate alloc;

pub mod abi;
pub mod access;
pub mod app;
pub mod auth;
pub mod crypto;
pub mod ctx;
pub mod error;
pub mod external;
pub mod features;
pub mod index;
pub mod queue;
pub mod remote;
pub mod schema;
pub mod sql;
pub mod timer;
pub mod typed;
pub mod value;

pub use abi::{HostOp, InvocationKind};
pub use app::{
    Aggregation, App, Call, Change, Collection, Committed, Component, Consistency, Contract,
    Definition, DeletedRow, Derived, Export, Import, ImportedCollection, ImportedDerived,
    ImportedMethod, IndexDef, InitCallback, Materialize, Method, MethodKind, OnDelete, Op, Place,
    Plan, PlannerFn, RawCallback, RawDerivedCallback, RawDueCallback, RawReduceCallback,
    RawRunCallback, RawTaskErrorCallback, RawTriggerCallback, Reference, ReferenceTargets,
    Retention, RetentionCallback, Target, Task, TaskFailure, Transaction, Trigger,
};
pub use auth::Principal;
pub use ctx::{
    CallError, CallFailure, CallOutcome, CollectionRef, Ctx, DerivedCtx, HistoryEntry, MethodCtx,
    MutationCtx, QueryCtx, RangeBounds, RawPage, RawRow, ReadCtx, wrap_derived, wrap_mutation,
    wrap_query,
};
pub use error::{Failure, Result, fail, fail_with};
pub use remote::{MutationOp, QueryOp};
pub use sql::{SqlBudget, SqlDef, SqlEndpoint, SqlExplain, SqlQuery};
pub use typed::{BusinessError, FlowerType, Nullable, Omittable};
pub use value::Value;

// Derive and attribute macros. The `FlowerType` derive shares its name with
// the trait above; they live in different namespaces, so one import serves
// both `impl FlowerType` and `#[derive(FlowerType)]`.
pub use flower_sdk_macros::{
    FlowerError, FlowerType, application, collection, derived, interface, mutation, query,
};

pub mod prelude {
    pub use crate::{
        App, DerivedCtx, FlowerType, MutationCtx, Nullable, Omittable, QueryCtx, Result,
        collection, derived, export, fail, mutation, query,
    };
}
