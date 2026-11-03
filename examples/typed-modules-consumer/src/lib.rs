//! The typed-modules consumer reaches the task provider only through the
//! `tasks.v1` contract and the logical slot `$tasks`, sharing no code with
//! the provider but the contract crate. Each deployment binds the slot to a
//! concrete module (`cargo flower deploy --bind tasks=<module>`); deploying
//! without the binding is refused before anything is sent.
//!
//! - `assign` (mutation, `{id, owner, title}`): `"assigned"`, or
//!   `"done"` when the provider refuses a done task (its declared
//!   `TASK_DONE` failure, decoded through a savepoint call).
//! - `complete` (mutation, `{id, owner, title}`): the task, done.
//! - `show` (query, `{id}`): the task, or null.
//! - `count` (query): the provider's task count, a derived value.

extern crate alloc;

use alloc::string::String;

use flower_sdk::{App, CallError, CallOutcome, MutationCtx, QueryCtx, fail, mutation, query};
use typed_modules_contract::{Task, TaskRejection, TasksApi};

/// The provider, wherever this deployment's `tasks` binding points: the
/// typed calls and the import declaring the contract it must meet.
static TASKS: TasksApi = TasksApi::import("$tasks");

/// Write a task through the provider, decoding its declared refusals.
fn put(ctx: &mut MutationCtx<'_, '_>, id: String, task: Task) -> flower_sdk::Result<String> {
    match TASKS.try_put_task(ctx, id, task) {
        Ok(CallOutcome::Success(())) => Ok(String::from("assigned")),
        Ok(CallOutcome::Failure(TaskRejection::AlreadyDone)) => Ok(String::from("done")),
        Ok(CallOutcome::Failure(TaskRejection::ProviderWriteFailed(message))) => {
            Err(fail("PROVIDER_WRITE_FAILED", &message))
        }
        Err(CallError::Host(failure)) => Err(failure),
    }
}

#[mutation(http)]
pub fn assign(
    ctx: &mut MutationCtx<'_, '_>,
    id: String,
    owner: String,
    title: String,
) -> flower_sdk::Result<String> {
    let task = Task {
        owner,
        title,
        done: false,
    };
    put(ctx, id, task)
}

#[mutation(http)]
pub fn complete(
    ctx: &mut MutationCtx<'_, '_>,
    id: String,
    owner: String,
    title: String,
) -> flower_sdk::Result<String> {
    let task = Task {
        owner,
        title,
        done: true,
    };
    put(ctx, id, task)
}

#[query(http)]
pub fn show(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<Task>> {
    TASKS.get_task(ctx, id)
}

#[query(http)]
pub fn count(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<u64> {
    TASKS.task_count(ctx)
}

pub static APP: App = App::new(&[&TASKS, &ASSIGN, &COMPLETE, &SHOW, &COUNT]);

flower_sdk::export!(APP);

#[cfg(test)]
mod tests {
    use alloc::string::String;
    use alloc::vec::Vec;

    use flower_sdk::Value;

    /// Only explicitly declared HTTP methods are exposed.
    #[test]
    fn it_answers_over_http_as_declared() {
        let bytes = flower_sdk::abi::manifest_native(&super::APP);
        let Value::Array(envelope) = Value::decode_binary(&bytes).unwrap() else {
            panic!("manifest envelope");
        };
        let Some(Value::Array(http)) = envelope[1].as_object().unwrap().get("http").cloned() else {
            panic!("manifest without http");
        };
        let mut aliases: Vec<String> = http
            .iter()
            .map(|entry| match entry.as_object().unwrap().get("alias") {
                Some(Value::String(alias)) => alias.clone(),
                other => panic!("alias not a string: {other:?}"),
            })
            .collect();
        aliases.sort();
        assert_eq!(aliases, ["assign", "complete", "count", "show"]);
    }
}
