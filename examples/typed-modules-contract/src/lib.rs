//! The typed-modules contract: the wire types and the `tasks.v1` interface
//! that the task provider implements and its consumers call, with no
//! dependency on either application.
//!
//! Both sides take their contracts from these signatures, so a deployment
//! links them structurally: a consumer imports the interface through a
//! logical slot, and each deployment binds the slot to a concrete
//! provider module.

extern crate alloc;

use alloc::string::String;

use flower_sdk::{DerivedCtx, FlowerError, FlowerType, MutationCtx, QueryCtx, interface};

/// A task.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Task {
    /// Who the task is assigned to.
    pub owner: String,
    /// What to do.
    pub title: String,
    /// Done tasks no longer change.
    pub done: bool,
}

/// Why the provider refused a write.
#[derive(FlowerError, Clone, Debug, PartialEq)]
pub enum TaskRejection {
    /// The task is already done.
    #[flower(code = "TASK_DONE")]
    AlreadyDone,
    /// The provider's own write failed.
    #[flower(code = "PROVIDER_WRITE_FAILED")]
    ProviderWriteFailed(String),
}

/// The task service.
#[interface(name = "tasks.v1", handle = TasksApi, client = TasksClient)]
pub trait Tasks {
    /// One task by ID.
    #[query]
    fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<Task>>;
    /// Create or replace a task; a done task refuses.
    #[mutation]
    fn put_task(ctx: &mut MutationCtx<'_, '_>, id: String, task: Task)
    -> Result<(), TaskRejection>;
    /// How many tasks there are.
    #[derived]
    fn task_count(ctx: &mut DerivedCtx<'_, '_>) -> flower_sdk::Result<u64>;
}
