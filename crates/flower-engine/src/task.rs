//! Maintenance tasks: background work a module declares, run by its
//! group's leader whenever it is due.
//!
//! A module declares tasks in its manifest (`tasks: [{name,
//! on_error}]`). Each has two guest entry points under its name: `due`,
//! a query answering when it next has work (epoch milliseconds) or
//! `null`, and `run`, a mutation performing one bounded unit. The
//! leader plans on a fresh snapshot ([`crate::engine::EngineReader::due_tasks`]):
//! every task whose `due` is at or before now and that is not backing
//! off, earliest first. It then prepares each run on the writer
//! ([`crate::engine::Engine::prepare_task_run`]): `due` again, in the
//! run's own transaction, and the run only when that still says due.
//! The run's patch replicates like any write, guarded by the revision
//! it was prepared at, so no member runs guest code to apply it, a
//! second preparation sees the first's effects, and whichever member
//! leads next picks up from the replicated state: a unit of work runs
//! once, on the leader, across leader changes.
//!
//! A failed run (its business failure, a trap or an exhausted budget)
//! rolls back. A task declaring `on_error` then gets the failure in a
//! mutation of its own; without it, or when that fails too, a planned
//! patch records the task's failure state (failures so far, when it may
//! run again, the last failure) in the reserved `$task` collection: it
//! backs off one second, doubling to a minute, as v1's tasks do, while
//! other tasks stay eligible. Its next successful run deletes the state
//! in the run's own patch. Operators read the states in the module
//! listing ([`TaskStatus`]).

use std::collections::BTreeMap;

use flower_sdk::{Failure, Value};

use crate::error::{Error, Result};
use crate::json::{parse_value, render};
use crate::lane::PreparedPatch;

/// First backoff after a failure, in milliseconds.
pub const BACKOFF_BASE_MS: u64 = 1_000;
/// Longest backoff, in milliseconds.
pub const BACKOFF_MAX_MS: u64 = 60_000;

/// A task's failure state, kept until its next successful run.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskState {
    /// Consecutive failed runs.
    pub failures: u64,
    /// When the task may run again, epoch milliseconds.
    pub retry_at_ms: u64,
    /// The last failure.
    pub error: Failure,
}

impl TaskState {
    /// The state after one more failure at `now_ms`: one second of
    /// backoff after the first, doubling up to a minute.
    pub fn after(previous: Option<&TaskState>, error: Failure, now_ms: u64) -> TaskState {
        let failures = previous.map_or(0, |state| state.failures).saturating_add(1);
        TaskState {
            failures,
            retry_at_ms: now_ms.saturating_add(backoff_ms(failures)),
            error,
        }
    }

    /// Canonical JSON: `{failures, retry_at, error: {code, message,
    /// details}}`.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut error = BTreeMap::new();
        error.insert("code".to_string(), Value::String(self.error.code.clone()));
        error.insert(
            "message".to_string(),
            Value::String(self.error.message.clone()),
        );
        error.insert(
            "details".to_string(),
            self.error.details.clone().unwrap_or(Value::Null),
        );
        let mut map = BTreeMap::new();
        map.insert("failures".to_string(), Value::Number(self.failures as f64));
        map.insert(
            "retry_at".to_string(),
            Value::Number(self.retry_at_ms as f64),
        );
        map.insert("error".to_string(), Value::Object(error));
        render(&Value::Object(map))
    }

    /// Decode a stored state.
    pub fn decode(bytes: &[u8]) -> Result<TaskState> {
        let corrupt = || Error::corrupt("a task state is malformed");
        let Value::Object(mut map) = parse_value(bytes)? else {
            return Err(corrupt());
        };
        let number = |value: Option<Value>| match value {
            Some(Value::Number(number)) if number >= 0.0 && number.fract() == 0.0 => {
                Some(number as u64)
            }
            _ => None,
        };
        let failures = number(map.remove("failures")).ok_or_else(corrupt)?;
        let retry_at_ms = number(map.remove("retry_at")).ok_or_else(corrupt)?;
        let Some(Value::Object(mut error)) = map.remove("error") else {
            return Err(corrupt());
        };
        let text = |value: Option<Value>| match value {
            Some(Value::String(text)) => Some(text),
            _ => None,
        };
        let code = text(error.remove("code")).ok_or_else(corrupt)?;
        let message = text(error.remove("message")).ok_or_else(corrupt)?;
        let mut failure = flower_sdk::fail(code, message);
        failure.details = match error.remove("details") {
            None | Some(Value::Null) => None,
            Some(details) => Some(details),
        };
        Ok(TaskState {
            failures,
            retry_at_ms,
            error: failure,
        })
    }
}

/// The backoff after `failures` consecutive failures: one second,
/// doubling, at most a minute.
pub fn backoff_ms(failures: u64) -> u64 {
    let doublings = failures.saturating_sub(1).min(6) as u32;
    BACKOFF_MAX_MS.min(BACKOFF_BASE_MS << doublings)
}

/// A task's row key in `$task`: module, a zero byte, task name.
pub fn state_key(module: &str, task: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(module.len() + task.len() + 1);
    key.extend_from_slice(module.as_bytes());
    key.push(0);
    key.extend_from_slice(task.as_bytes());
    key
}

/// What the planner found in one database.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskPlan {
    /// The tasks due now, earliest first.
    pub due: Vec<DueTask>,
    /// The earliest later time a task may be due, epoch milliseconds:
    /// a `due` answering a later time, or a backoff ending. `None` when
    /// no task has work in sight; a write can still make one due.
    pub next_ms: Option<u64>,
}

/// One task the planner found due.
#[derive(Clone, Debug, PartialEq)]
pub struct DueTask {
    /// Declaring module.
    pub module: String,
    /// Task name.
    pub task: String,
    /// When it became due, epoch milliseconds, as `due` answered (the
    /// end of its backoff when later).
    pub due_ms: f64,
}

/// One task as operators see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskStatus {
    /// Task name.
    pub name: String,
    /// Whether failed runs go to its error callback.
    pub on_error: bool,
    /// Consecutive failed runs, zero when its last run succeeded.
    pub failures: u64,
    /// When it may run again after its failures, epoch milliseconds.
    pub retry_at_ms: Option<u64>,
    /// Its last failure, while it is failing.
    pub error: Option<TaskError>,
}

/// A task's last failure, as operators see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskError {
    /// Failure code.
    pub code: String,
    /// Failure message.
    pub message: String,
}

impl TaskStatus {
    /// A declared task with its failure state, if any.
    pub fn of(name: &str, on_error: bool, state: Option<&TaskState>) -> TaskStatus {
        TaskStatus {
            name: name.to_string(),
            on_error,
            failures: state.map_or(0, |state| state.failures),
            retry_at_ms: state.map(|state| state.retry_at_ms),
            error: state.map(|state| TaskError {
                code: state.error.code.clone(),
                message: state.error.message.clone(),
            }),
        }
    }
}

/// What preparing one task run decided.
#[derive(Debug)]
pub enum TaskPrepare {
    /// Nothing to run: not due, backing off, or no longer declared.
    Skip,
    /// The run's patch, which also clears any failure state.
    Ran(PreparedPatch),
    /// The run failed: the error callback's patch, or the patch
    /// recording the failure state.
    Failed {
        /// The patch to propose.
        prepared: PreparedPatch,
        /// The run's failure.
        failure: Failure,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_a_minute() {
        assert_eq!(backoff_ms(1), 1_000);
        assert_eq!(backoff_ms(2), 2_000);
        assert_eq!(backoff_ms(6), 32_000);
        assert_eq!(backoff_ms(7), 60_000);
        assert_eq!(backoff_ms(u64::MAX), 60_000);
    }

    #[test]
    fn states_round_trip_and_count_failures() {
        let mut error = flower_sdk::fail("BOOM", "no");
        error.details = Some(Value::Number(3.0));
        let first = TaskState::after(None, error.clone(), 10_000);
        assert_eq!(first.failures, 1);
        assert_eq!(first.retry_at_ms, 11_000);
        let second = TaskState::after(Some(&first), flower_sdk::fail("BOOM", "again"), 20_000);
        assert_eq!(second.failures, 2);
        assert_eq!(second.retry_at_ms, 22_000);
        assert_eq!(TaskState::decode(&first.encode().unwrap()).unwrap(), first);
        assert_eq!(
            TaskState::decode(&second.encode().unwrap()).unwrap(),
            second
        );
        assert!(TaskState::decode(b"{}").is_err());
        assert!(TaskState::decode(b"[]").is_err());
    }

    #[test]
    fn state_keys_separate_module_and_task() {
        assert_eq!(state_key("app", "purge"), b"app\0purge".to_vec());
        assert_ne!(state_key("a", "bc"), state_key("ab", "c"));
    }
}
