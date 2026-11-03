//! Guest timers: durable not-before deadlines that fire
//! module mutations.
//!
//! Mutations schedule timers by stable ID; the leader fires due
//! timers by running their handler with the scheduled arguments,
//! using currently deployed handler code. Scheduling the same ID
//! replaces (debounces) the timer. A deadline means not-before,
//! never exact execution time.
//!
//! The handler is a mutation of the same module, named by its
//! descriptor, and the arguments are checked against its contract when
//! the timer is scheduled:
//!
//! ```ignore
//! #[mutation]
//! fn finish_baking(ctx: &mut MutationCtx<'_, '_>, order: OrderId) -> Result<()> { ... }
//!
//! Timers::after(ctx, &id, Duration::from_secs(30), &FINISH_BAKING, &FinishBakingArgs { order })?;
//! ```

use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::abi::HostOp;
use crate::app::{Method, MethodKind, Op};
use crate::ctx::{MutationCtx, ReadCtx};
use crate::error::{Failure, Result, codes, fail};
use crate::typed::FlowerType;
use crate::value::Value;

/// Longest timer ID, in bytes: the timer index keeps IDs in fixed-width
/// keys.
pub const MAX_TIMER_ID: usize = 128;
/// Default attempts before a timer dies.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Most attempts before a timer dies.
pub const MAX_ATTEMPTS: u32 = 100;
/// Most timers one scan returns.
pub const MAX_SCAN_LIMIT: usize = 100;

/// Schedule options.
#[derive(Clone, Debug)]
pub struct ScheduleOptions {
    /// Attempts before the timer dies. Defaults to 3.
    pub max_attempts: u32,
}

impl Default for ScheduleOptions {
    fn default() -> Self {
        ScheduleOptions {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }
}

/// One stored timer as guests see it.
#[derive(Clone, Debug, PartialEq)]
pub struct Timer {
    /// Stable timer ID.
    pub id: String,
    /// Handler definition name.
    pub handler: String,
    /// Scheduled arguments.
    pub args: Value,
    /// Due time in milliseconds.
    pub due_ms: u64,
    /// Fires so far.
    pub attempts: u32,
    /// Attempts before death.
    pub max_attempts: u32,
    /// `pending`, `dead` or `held` (came due while its module was
    /// removed).
    pub state: String,
}

/// Timer states guests can scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerState {
    /// Awaiting its due time.
    Pending,
    /// Exhausted its attempts.
    Dead,
}

impl TimerState {
    fn name(&self) -> &'static str {
        match self {
            TimerState::Pending => "pending",
            TimerState::Dead => "dead",
        }
    }
}

/// Durable timer handles.
pub struct Timers;

impl Timers {
    /// Run `handler` with `args` (its own argument type) once `delay`
    /// has passed, replacing any timer with the same ID.
    pub fn after<A: FlowerType, T, E>(
        ctx: &mut MutationCtx<'_, '_>,
        id: &str,
        delay: Duration,
        handler: &Op<A, T, E>,
        args: &A,
    ) -> Result<()> {
        let now = ctx.ctx().clock()? as u64;
        let due_ms = now.saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
        let options = ScheduleOptions::default();
        Self::schedule(ctx, id, due_ms, handler, args.encode()?, &options)
    }

    /// Run `handler` with `args` at an epoch-millisecond time, replacing
    /// any timer with the same ID.
    pub fn at<A: FlowerType, T, E>(
        ctx: &mut MutationCtx<'_, '_>,
        id: &str,
        due_ms: u64,
        handler: &Op<A, T, E>,
        args: &A,
    ) -> Result<()> {
        let options = ScheduleOptions::default();
        Self::schedule(ctx, id, due_ms, handler, args.encode()?, &options)
    }

    /// The full form of [`at`][Self::at], with options, over any mutation
    /// descriptor and raw arguments: they are checked against its
    /// argument contract here.
    pub fn schedule(
        ctx: &mut MutationCtx<'_, '_>,
        id: &str,
        due_ms: u64,
        handler: &Method,
        args: Value,
        options: &ScheduleOptions,
    ) -> Result<()> {
        validate_id(id)?;
        if handler.kind != MethodKind::Mutation {
            return Err(fail(
                codes::INVALID_VALUE,
                "a timer's handler is a mutation",
            ));
        }
        handler.args.parse(&args).map_err(Failure::from)?;
        if options.max_attempts == 0 || options.max_attempts > MAX_ATTEMPTS {
            return Err(fail(codes::INVALID_VALUE, "max attempts out of range"));
        }
        let call = Value::Array(Vec::from([
            Value::String(String::from(id)),
            Value::Number(due_ms as f64),
            Value::String(String::from(handler.name)),
            args,
            Value::Number(options.max_attempts as f64),
        ]));
        ctx.ctx().call(HostOp::TimerSchedule, call)?;
        Ok(())
    }

    /// Cancel a timer, returning whether one existed.
    pub fn cancel(ctx: &mut MutationCtx<'_, '_>, id: &str) -> Result<bool> {
        validate_id(id)?;
        let call = Value::Array(Vec::from([Value::String(String::from(id))]));
        match ctx.ctx().call(HostOp::TimerCancel, call)? {
            Value::Bool(removed) => Ok(removed),
            _ => Err(fail(codes::TYPE_MISMATCH, "cancel returns whether removed")),
        }
    }

    /// Read one timer, in queries and mutations alike.
    pub fn get(ctx: &mut impl ReadCtx, id: &str) -> Result<Option<Timer>> {
        validate_id(id)?;
        let call = Value::Array(Vec::from([Value::String(String::from(id))]));
        match ctx.host_op(HostOp::TimerGet, call)? {
            Value::Null => Ok(None),
            value => Ok(Some(decode_timer(&value)?)),
        }
    }

    /// Scan timers due-first, in queries and mutations alike.
    pub fn scan(
        ctx: &mut impl ReadCtx,
        state: Option<TimerState>,
        limit: usize,
    ) -> Result<Vec<Timer>> {
        if limit == 0 || limit > MAX_SCAN_LIMIT {
            return Err(fail(codes::INVALID_VALUE, "scan limit out of range"));
        }
        let state = match state {
            Some(state) => Value::String(String::from(state.name())),
            None => Value::Null,
        };
        let call = Value::Array(Vec::from([state, Value::Number(limit as f64)]));
        match ctx.host_op(HostOp::TimerScan, call)? {
            Value::Array(items) => items.iter().map(decode_timer).collect(),
            _ => Err(fail(codes::TYPE_MISMATCH, "scan returns timers")),
        }
    }

    /// Requeue a dead timer with a fresh attempt budget, due after
    /// `delay`.
    pub fn retry(ctx: &mut MutationCtx<'_, '_>, id: &str, delay: Duration) -> Result<()> {
        validate_id(id)?;
        let now = ctx.ctx().clock()? as u64;
        let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        let call = Value::Array(Vec::from([
            Value::String(String::from(id)),
            Value::Number(now.saturating_add(delay_ms) as f64),
        ]));
        ctx.ctx().call(HostOp::TimerRetry, call)?;
        Ok(())
    }
}

/// Validate a timer ID on both sides of the ABI: 1 to [`MAX_TIMER_ID`]
/// bytes of text without control characters.
pub fn validate_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > MAX_TIMER_ID {
        return Err(fail(codes::INVALID_VALUE, "timer ID has a bad length"));
    }
    if id.chars().any(char::is_control) {
        return Err(fail(
            codes::INVALID_VALUE,
            "timer IDs hold no control characters",
        ));
    }
    Ok(())
}

/// Validate a handler name on both sides of the ABI.
pub fn validate_handler(handler: &str) -> Result<()> {
    if handler.is_empty() || handler.len() > MAX_TIMER_ID {
        return Err(fail(codes::INVALID_VALUE, "handler name has a bad length"));
    }
    Ok(())
}

/// Read one timer field.
fn field<'a>(
    map: &'a alloc::collections::BTreeMap<String, Value>,
    name: &str,
) -> Result<&'a Value> {
    map.get(name)
        .ok_or_else(|| fail(codes::TYPE_MISMATCH, "timer misses a field"))
}

/// Read one timer text field.
fn text_field(map: &alloc::collections::BTreeMap<String, Value>, name: &str) -> Result<String> {
    match field(map, name)? {
        Value::String(text) => Ok(text.clone()),
        _ => Err(fail(codes::TYPE_MISMATCH, "timer field is text")),
    }
}

/// Read one timer integer field.
fn number_field(map: &alloc::collections::BTreeMap<String, Value>, name: &str) -> Result<u64> {
    let number = match field(map, name)? {
        Value::Number(number) => *number,
        _ => return Err(fail(codes::TYPE_MISMATCH, "timer field is a number")),
    };
    if number.is_finite() && number % 1.0 == 0.0 && number >= 0.0 {
        Ok(number as u64)
    } else {
        Err(fail(codes::TYPE_MISMATCH, "timer field is a number"))
    }
}

/// Decode a timer object from the host.
fn decode_timer(value: &Value) -> Result<Timer> {
    let Value::Object(map) = value else {
        return Err(fail(codes::TYPE_MISMATCH, "timer is an object"));
    };
    Ok(Timer {
        id: text_field(map, "id")?,
        handler: text_field(map, "handler")?,
        args: field(map, "args")?.clone(),
        due_ms: number_field(map, "due_ms")?,
        attempts: number_field(map, "attempts")? as u32,
        max_attempts: number_field(map, "max_attempts")? as u32,
        state: text_field(map, "state")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_ids_validate() {
        assert!(validate_id("reminder-1_x").is_ok());
        assert!(validate_id("").is_err());
        // Any text: an order's canonical key, spaces and all.
        assert!(validate_id(r#"bake:["t-1","store 0","p-1"]"#).is_ok());
        assert!(validate_id("tab\there").is_err());
        assert!(validate_id("nul\0").is_err());
        assert!(validate_id(&"t".repeat(129)).is_err());
        assert!(validate_handler("send_email").is_ok());
        assert!(validate_handler("").is_err());
    }
}
