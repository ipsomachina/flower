//! The task provider of v1's `examples/typed-modules-provider`: it
//! implements `tasks.v1` over its own `tasks` collection and exports the
//! interface, so a consumer deployed with its `tasks` slot bound to this
//! module calls it. Modules are private by default: the interface's
//! methods are exported because the provider component declares them
//! so, and they answer over HTTP under their names (`get_task`,
//! `put_task`), so operators can read and write tasks directly.

extern crate alloc;

use alloc::string::String;

use flower_sdk::{App, DerivedCtx, MutationCtx, QueryCtx, collection};
use typed_modules_contract::{Task, TaskRejection, Tasks, TasksProvider};

#[collection(name = "tasks", key = String, value = Task)]
pub struct Rows;

/// The implementation behind the interface.
pub struct Provider;

impl Tasks for Provider {
    fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<Task>> {
        Rows::get(ctx, &id)
    }

    fn put_task(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        task: Task,
    ) -> Result<(), TaskRejection> {
        let failed =
            |failure: flower_sdk::Failure| TaskRejection::ProviderWriteFailed(failure.message);
        if Rows::get(ctx, &id)
            .map_err(failed)?
            .is_some_and(|current| current.done)
        {
            return Err(TaskRejection::AlreadyDone);
        }
        Rows::set(ctx, &id, &task).map_err(failed)
    }

    fn task_count(ctx: &mut DerivedCtx<'_, '_>) -> flower_sdk::Result<u64> {
        let mut count = 0;
        let mut after = None;
        loop {
            let page = Rows::page(ctx, 1_000, after.as_deref())?;
            count += page.rows.len() as u64;
            match page.cursor {
                Some(cursor) => after = Some(cursor),
                None => return Ok(count),
            }
        }
    }
}

/// The tasks and the whole interface, exported and callable over HTTP.
pub static APP: App = App::new(&[&Rows, &TasksProvider::<Provider>::COMPONENT.http()]);

flower_sdk::export!(APP);

#[cfg(test)]
mod tests {
    use alloc::string::String;
    use alloc::vec::Vec;

    use flower_sdk::Value;

    /// Exactly what parity's `http` list exposed: the interface's methods, for operators.
    #[test]
    fn it_answers_over_http_as_on_parity() {
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
        assert_eq!(aliases, ["get_task", "put_task"]);
        // Without `.http()`, a provider serves other modules only.
        static QUIET: flower_sdk::App = flower_sdk::App::new(&[
            &super::Rows,
            &typed_modules_contract::TasksProvider::<super::Provider>::COMPONENT,
        ]);
        let bytes = flower_sdk::abi::manifest_native(&QUIET);
        let Value::Array(envelope) = Value::decode_binary(&bytes).unwrap() else {
            panic!("manifest envelope");
        };
        assert_eq!(
            envelope[1].as_object().unwrap().get("http"),
            Some(&Value::Array(Vec::new()))
        );
    }
}
