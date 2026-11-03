//! Explicit raw bridges for mutation callbacks, and the typed hooks:
//! triggers, deletion retention and maintenance tasks.
//!
//! Typed handles encode through [`FlowerType`][crate::typed::FlowerType]
//! before writing; these bridges accept caller-built [`Value`]s instead.
//! There is no typed-context escape into the raw [`Ctx`][crate::ctx::Ctx]:
//! host schemas, policies, triggers, references and retention still apply,
//! exactly as for typed writes.

use crate::ctx::{CollectionRef, MutationCtx};
use crate::error::Result;
use crate::value::Value;

/// Upsert a raw row. The host validates the value against the collection
/// schemas and runs the usual mutation effects.
pub fn raw_set(
    ctx: &mut MutationCtx<'_, '_>,
    collection: CollectionRef<'_>,
    key: &Value,
    value: Value,
) -> Result<()> {
    ctx.set(collection, key, value)
}

/// Delete a raw row. References, triggers and retention still apply.
pub fn raw_delete(
    ctx: &mut MutationCtx<'_, '_>,
    collection: CollectionRef<'_>,
    key: &Value,
) -> Result<()> {
    ctx.delete(collection, key)
}

/// A typed source-change hook on the collection `Source`: the host fires
/// it inside the mutation, once per net change of a row, with the key
/// and the row before and after decoded (`None` where the row is
/// absent). It runs with definer rights under its own savepoint; a
/// failure rolls its writes back and fails the mutation. Register
/// [`TRIGGER`][Self::TRIGGER] in `App::triggers`.
///
/// ```ignore
/// struct Audit;
/// impl TypedTrigger for Audit {
///     type Source = Tasks;
///     const NAME: &'static str = "audit_tasks";
///     fn run(ctx: &mut MutationCtx<'_, '_>, key: String, before: Option<Task>, after: Option<Task>) -> Result<()> {
///         /* ... */
///     }
/// }
/// static TRIGGERS: [&Trigger; 1] = [&Audit::TRIGGER];
/// ```
pub trait TypedTrigger: Sized + 'static {
    /// The watched collection's marker.
    type Source: crate::typed::TypedCollection;
    /// Stable trigger name: the host fires it as a mutation of this name.
    const NAME: &'static str;

    /// Handle one change.
    fn run(
        ctx: &mut MutationCtx<'_, '_>,
        key: <Self::Source as crate::typed::TypedCollection>::Key,
        before: Option<<Self::Source as crate::typed::TypedCollection>::Value>,
        after: Option<<Self::Source as crate::typed::TypedCollection>::Value>,
    ) -> Result<()>;

    /// The raw descriptor to register in `App::triggers`.
    const TRIGGER: crate::app::Trigger = crate::app::Trigger::new(
        Self::NAME,
        <Self::Source as crate::typed::TypedCollection>::NAME,
        typed_trigger::<Self>,
    );
}

fn typed_trigger<'c, 'r, T: TypedTrigger>(
    ctx: &'c mut crate::ctx::Ctx<'c, 'r>,
    change: &crate::app::Change,
) -> Result<()> {
    use crate::typed::{FlowerType, TypedCollection};
    let decode = |image: &Option<Value>| -> Result<Option<<T::Source as TypedCollection>::Value>> {
        image
            .clone()
            .map(<T::Source as TypedCollection>::Value::decode)
            .transpose()
    };
    let key = <T::Source as TypedCollection>::Key::decode(change.key.clone())?;
    let before = decode(&change.before)?;
    let after = decode(&change.after)?;
    T::run(crate::ctx::wrap_mutation(ctx), key, before, after)
}

/// A typed deletion-retention hook on the collection `Source` (see
/// [`Retention`][crate::app::Retention]): the host hands it each removed
/// row, key and value decoded. Register [`RETENTION`][Self::RETENTION]
/// among the app's components.
pub trait TypedRetention: Sized + 'static {
    /// The watched collection's marker.
    type Source: crate::typed::TypedCollection;
    /// Stable hook name: the host runs it as a mutation of this name.
    const NAME: &'static str;

    /// Keep, archive or drop one deleted row.
    fn retain(
        ctx: &mut MutationCtx<'_, '_>,
        key: <Self::Source as crate::typed::TypedCollection>::Key,
        value: <Self::Source as crate::typed::TypedCollection>::Value,
        deleted_at: f64,
    ) -> Result<()>;

    /// The component to register among the app's components.
    const RETENTION: crate::app::Retention = crate::app::Retention {
        name: Self::NAME,
        source: <Self::Source as crate::typed::TypedCollection>::NAME,
        retain: typed_retention::<Self>,
    };
}

fn typed_retention<'c, 'r, T: TypedRetention>(
    ctx: &'c mut crate::ctx::Ctx<'c, 'r>,
    row: &crate::app::DeletedRow,
) -> Result<()> {
    use crate::typed::{FlowerType, TypedCollection};
    let key = <T::Source as TypedCollection>::Key::decode(row.key.clone())?;
    let value = <T::Source as TypedCollection>::Value::decode(row.value.clone())?;
    T::retain(crate::ctx::wrap_mutation(ctx), key, value, row.deleted_at)
}

/// A typed maintenance task (see [`Task`][crate::app::Task]): the
/// host's leader runs [`run`][Self::run] whenever [`due`][Self::due]
/// says it has work, once per unit, as a replicated mutation. Register
/// [`TASK`][Self::TASK] among the app's components.
///
/// ```ignore
/// struct Purge;
/// impl TypedTask for Purge {
///     const NAME: &'static str = "sessions.purge";
///     fn due(ctx: &mut QueryCtx<'_, '_>) -> Result<Option<f64>> {
///         Ok(Sessions::BY_EXPIRY.query().page(ctx, 1)?.rows.first().map(|row| row.value.expires_at))
///     }
///     fn run(ctx: &mut MutationCtx<'_, '_>) -> Result<Value> {
///         /* delete the expired sessions, a page at a time */
///     }
/// }
/// static USES: [&dyn Component; 1] = [&Purge::TASK];
/// ```
pub trait TypedTask: Sized + 'static {
    /// Stable task name.
    const NAME: &'static str;
    /// Whether [`on_error`][Self::on_error] handles failed runs; without
    /// it they back off (one second, doubling up to a minute).
    const HANDLES_FAILURES: bool = false;

    /// When the task next has work, epoch milliseconds, or `None` when
    /// it has none. Depends only on data and time.
    fn due(ctx: &mut crate::ctx::QueryCtx<'_, '_>) -> Result<Option<f64>>;

    /// Perform one bounded unit of work.
    fn run(ctx: &mut MutationCtx<'_, '_>) -> Result<Value>;

    /// Handle a failed run, over the state the run saw (its writes
    /// rolled back), when [`HANDLES_FAILURES`][Self::HANDLES_FAILURES]
    /// is set. A failure here backs the task off.
    fn on_error(ctx: &mut MutationCtx<'_, '_>, failure: &crate::app::TaskFailure) -> Result<Value> {
        let _ = ctx;
        Err(failure.error.clone())
    }

    /// The component to register among the app's components.
    const TASK: crate::app::Task = crate::app::Task {
        name: Self::NAME,
        due: typed_due::<Self>,
        run: typed_run::<Self>,
        on_error: if Self::HANDLES_FAILURES {
            Some(typed_on_error::<Self>)
        } else {
            None
        },
    };
}

fn typed_due<'c, 'r, T: TypedTask>(ctx: &'c mut crate::ctx::Ctx<'c, 'r>) -> Result<Option<f64>> {
    T::due(crate::ctx::wrap_query(ctx))
}

fn typed_run<'c, 'r, T: TypedTask>(ctx: &'c mut crate::ctx::Ctx<'c, 'r>) -> Result<Value> {
    T::run(crate::ctx::wrap_mutation(ctx))
}

fn typed_on_error<'c, 'r, T: TypedTask>(
    ctx: &'c mut crate::ctx::Ctx<'c, 'r>,
    failure: &crate::app::TaskFailure,
) -> Result<Value> {
    T::on_error(crate::ctx::wrap_mutation(ctx), failure)
}
