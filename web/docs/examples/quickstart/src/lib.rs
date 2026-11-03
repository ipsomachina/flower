//! A to-do list as a Flower guest: one collection, one index, two
//! mutations and a query, exposed over HTTP.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use flower_sdk::{
    App, FlowerType, MutationCtx, QueryCtx, Result, collection, export, fail, mutation, query,
};

/// A task, stored as the JSON object `{"title": ..., "done": ...}`.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Task {
    pub title: String,
    pub done: bool,
}

/// Tasks by ID, indexed by whether they are done.
#[collection(
    name = "tasks",
    key = String,
    value = Task,
    index(name = "by_done", fields(done), types(bool)),
)]
pub struct Tasks;

/// What `open_tasks` returns for each task.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct OpenTask {
    pub id: String,
    pub title: String,
}

#[mutation(http)]
pub fn add_task(ctx: &mut MutationCtx<'_, '_>, id: String, title: String) -> Result<Task> {
    if Tasks::get(ctx, &id)?.is_some() {
        return Err(fail("TASK_EXISTS", "a task with this ID already exists"));
    }
    let task = Task { title, done: false };
    Tasks::set(ctx, &id, &task)?;
    Ok(task)
}

#[mutation(http)]
pub fn complete_task(ctx: &mut MutationCtx<'_, '_>, id: String) -> Result<Task> {
    let Some(mut task) = Tasks::get(ctx, &id)? else {
        return Err(fail("TASK_NOT_FOUND", "no task has this ID"));
    };
    task.done = true;
    Tasks::set(ctx, &id, &task)?;
    Ok(task)
}

#[query(http)]
pub fn open_tasks(ctx: &mut QueryCtx<'_, '_>) -> Result<Vec<OpenTask>> {
    let page = Tasks::BY_DONE.prefix((false,)).page(ctx, 100)?;
    Ok(page
        .rows
        .into_iter()
        .map(|row| OpenTask {
            id: row.key,
            title: row.value.title,
        })
        .collect())
}

pub static APP: App = App::new(&[&Tasks, &ADD_TASK, &COMPLETE_TASK, &OPEN_TASKS]);

export!(APP);
