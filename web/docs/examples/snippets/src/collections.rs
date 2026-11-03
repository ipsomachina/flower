//! Concepts › Collections and indexes.

use alloc::string::String;
use alloc::vec::Vec;

use flower_sdk::{FlowerType, MutationCtx, QueryCtx, Result, collection, mutation, query};

// ANCHOR: types
/// A project. Wire names are the Rust field names.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Project {
    pub name: String,
}

/// A unit enum is a closed string enum: `"low"`, `"normal"` or `"high"`.
#[derive(FlowerType, Clone, Copy, Debug, PartialEq)]
pub enum Priority {
    Low,
    Normal,
    High,
}

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Task {
    pub project: String,
    pub owner: String,
    pub title: String,
    pub priority: Priority,
    /// Due time, epoch milliseconds.
    pub due: u64,
    /// `null` is a visible value: the member must be present.
    pub notes: Option<String>,
    /// Absent members decode as `Vec::new()`.
    #[flower(default)]
    pub tags: Vec<String>,
}
// ANCHOR_END: types

// ANCHOR: collections
#[collection(name = "projects", key = String, value = Project)]
pub struct Projects;

#[collection(
    name = "tasks",
    key = String,
    value = Task,
    index(name = "by_owner_due", fields(owner, due), types(String, u64)),
    // A field reference needs an index whose fields start with its own.
    index(name = "by_project", fields(project), types(String)),
    reference(target = Projects, fields(project), on_delete = cascade),
)]
pub struct Tasks;
// ANCHOR_END: collections

// ANCHOR: read-write
#[mutation]
pub fn rename_task(ctx: &mut MutationCtx<'_, '_>, id: String, title: String) -> Result<bool> {
    let Some(mut task) = Tasks::get(ctx, &id)? else {
        return Ok(false);
    };
    task.title = title;
    Tasks::set(ctx, &id, &task)?;
    Ok(true)
}
// ANCHOR_END: read-write

// ANCHOR: range
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct DuePage {
    pub titles: Vec<String>,
    /// Pass back as `after` for the next page; `null` after the last.
    pub next: Option<String>,
}

#[query]
pub fn due_between(
    ctx: &mut QueryCtx<'_, '_>,
    owner: String,
    since: u64,
    until: u64,
    after: Option<String>,
) -> Result<DuePage> {
    let mut range = Tasks::BY_OWNER_DUE.prefix((owner,)).gte(&since).lt(&until);
    if let Some(cursor) = &after {
        range = range.after(cursor);
    }
    let page = range.page(ctx, 50)?;
    Ok(DuePage {
        titles: page.rows.into_iter().map(|row| row.value.title).collect(),
        next: page.cursor,
    })
}
// ANCHOR_END: range

// ANCHOR: scan
#[query]
pub fn project_names(ctx: &mut QueryCtx<'_, '_>) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = Projects::page(ctx, 100, after.as_deref())?;
        names.extend(page.rows.into_iter().map(|row| row.value.name));
        match page.cursor {
            Some(cursor) => after = Some(cursor),
            None => return Ok(names),
        }
    }
}
// ANCHOR_END: scan
