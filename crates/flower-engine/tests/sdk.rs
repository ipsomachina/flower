//! SDK components against the engine, with real SDK applications run
//! in-process through `NativeRuntime`: typed index handles and source
//! pages, aggregates maintained by deltas, and maintenance tasks.

extern crate alloc;

use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::NativeRuntime;
use flower_engine::storage::GroupStore;
use flower_engine::task::{TaskPrepare, TaskStatus};
use flower_sdk::error::codes;
use flower_sdk::{Failure, Value};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn n(number: f64) -> Value {
    Value::Number(number)
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn none() -> Value {
    obj(vec![])
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Fixture {
    fn new(test: &str) -> Fixture {
        let mut path = std::env::temp_dir();
        path.push(format!("flower-sdk-{test}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        Fixture {
            engine: Engine::new(GroupStore::create(&path).unwrap()),
            path,
        }
    }

    fn deploy(&mut self, module: &str, app: &'static flower_sdk::App) -> Result<(), Failure> {
        let manifest = NativeRuntime::new(app).manifest().unwrap();
        let identity = flower_crypto::hash::content_identity(
            module.as_bytes(),
            manifest.canonical_json().unwrap().as_bytes(),
        );
        self.engine.deploy(
            b"db",
            module,
            identity,
            b"",
            &manifest,
            Box::new(NativeRuntime::new(app)),
        )
    }

    fn query(&self, module: &str, name: &str, args: Value) -> Result<Value, Failure> {
        self.engine
            .invoke_query(b"db", module, name, &args, &CallOptions::default())
    }

    fn mutate(&mut self, module: &str, name: &str, args: Value) -> Result<Value, Failure> {
        self.engine
            .invoke_mutation(b"db", module, name, &args, &MutationOptions::default())
    }
}

/// Jobs indexed by owner then due time, with typed handles over the
/// index, and a second module reading them through an import.
mod jobs_app {
    use flower_sdk::{App, FlowerType, MutationCtx, QueryCtx, collection, mutation, query};

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Job {
        pub owner: String,
        pub due: u64,
    }

    #[collection(
        name = "jobs",
        key = String,
        value = Job,
        index(name = "by_owner_due", fields(owner, due), types(String, u64)),
        index(name = "by_due", fields(due)),
        export,
    )]
    pub struct Jobs;

    #[collection(name = "notes", key = String, value = String)]
    pub struct Notes;

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Listing {
        pub ids: Vec<String>,
        pub cursor: Option<String>,
    }

    impl Listing {
        pub fn of(page: flower_sdk::typed::Page<String, Job>) -> Listing {
            Listing {
                ids: page.rows.into_iter().map(|row| row.key).collect(),
                cursor: page.cursor,
            }
        }
    }

    #[mutation]
    pub fn add(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        owner: String,
        due: u64,
    ) -> flower_sdk::Result<()> {
        Jobs::set(ctx, &id, &Job { owner, due })?;
        Notes::set(ctx, &id, &String::from("note"))
    }

    /// Owner's jobs due in `[from, to)`, a page at a time.
    #[query]
    pub fn due(
        ctx: &mut QueryCtx<'_, '_>,
        owner: String,
        from: u64,
        to: u64,
        limit: u32,
        after: Option<String>,
        reverse: bool,
    ) -> flower_sdk::Result<Listing> {
        let mut range = Jobs::BY_OWNER_DUE.prefix((owner,)).gte(&from).lt(&to);
        if let Some(cursor) = &after {
            range = range.after(cursor);
        }
        if reverse {
            range = range.reverse();
        }
        Ok(Listing::of(range.page(ctx, limit)?))
    }

    /// The owner's job due exactly at `due`: a full prefix.
    #[query]
    pub fn exactly(
        ctx: &mut QueryCtx<'_, '_>,
        owner: String,
        due: u64,
    ) -> flower_sdk::Result<Listing> {
        Ok(Listing::of(
            Jobs::BY_OWNER_DUE.prefix((owner, due)).page(ctx, 10)?,
        ))
    }

    /// Every job by due time, through the untyped-field handle.
    #[query]
    pub fn by_due(ctx: &mut QueryCtx<'_, '_>, from: u64) -> flower_sdk::Result<Listing> {
        let from = flower_sdk::Value::Number(from as f64);
        Ok(Listing::of(Jobs::BY_DUE.query().gte(&from).page(ctx, 10)?))
    }

    /// One source page of every job.
    #[query]
    pub fn inventory(
        ctx: &mut QueryCtx<'_, '_>,
        limit: u32,
        after: Option<String>,
    ) -> flower_sdk::Result<Listing> {
        Ok(Listing::of(Jobs::page(ctx, limit, after.as_deref())?))
    }

    /// One source page of notes, from a cursor issued for anything.
    #[query]
    pub fn notes(ctx: &mut QueryCtx<'_, '_>, after: String) -> flower_sdk::Result<u32> {
        Ok(Notes::page(ctx, 10, Some(&after))?.rows.len() as u32)
    }

    static COLLECTIONS: [&dyn flower_sdk::Component; 2] = [&JOBS_COLLECTION, &NOTES_COLLECTION];
    static DEFINITIONS: [&dyn flower_sdk::Component; 6] =
        [&ADD, &DUE, &EXACTLY, &BY_DUE, &INVENTORY, &NOTES];
    pub static APP: App = App::new(&[&COLLECTIONS, &DEFINITIONS]);
}

/// Reads `jobs`' exported collection and index through typed handles.
mod board_app {
    use flower_sdk::{App, QueryCtx, query};

    use super::jobs_app::{Jobs, Listing};

    #[query]
    pub fn board(ctx: &mut QueryCtx<'_, '_>, owner: String) -> flower_sdk::Result<Listing> {
        let index = Jobs::BY_OWNER_DUE.in_module("jobs");
        Ok(Listing::of(index.prefix((owner,)).page(ctx, 10)?))
    }

    #[query]
    pub fn first(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Option<u64>> {
        Ok(Jobs::SOURCE
            .in_module("jobs")
            .get(ctx, &id)?
            .map(|job| job.due))
    }

    static DEFINITIONS: [&dyn flower_sdk::Component; 2] = [&BOARD, &FIRST];
    pub static APP: App = App::new(&[&DEFINITIONS]);
}

fn jobs(test: &str) -> Fixture {
    let mut fixture = Fixture::new(test);
    fixture.deploy("jobs", &jobs_app::APP).unwrap();
    for (id, owner, due) in [
        ("a1", "ada", 1.0),
        ("a2", "ada", 2.0),
        ("a3", "ada", 3.0),
        ("a4", "ada", 4.0),
        ("a5", "ada", 5.0),
        ("b3", "bob", 3.0),
    ] {
        fixture
            .mutate(
                "jobs",
                "add",
                obj(vec![("id", s(id)), ("owner", s(owner)), ("due", n(due))]),
            )
            .unwrap();
    }
    fixture
}

fn ids(listing: &Value) -> Vec<String> {
    field(listing, "ids")
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect()
}

fn due(owner: &str, limit: u32, after: Value, reverse: bool) -> Value {
    obj(vec![
        ("owner", s(owner)),
        ("from", n(2.0)),
        ("to", n(5.0)),
        ("limit", n(f64::from(limit))),
        ("after", after),
        ("reverse", Value::Bool(reverse)),
    ])
}

#[test]
fn typed_index_ranges_fix_prefixes_and_bound_the_next_field() {
    let fixture = jobs("ranges");
    let forward = fixture
        .query("jobs", "due", due("ada", 10, Value::Null, false))
        .unwrap();
    assert_eq!(ids(&forward), ["a2", "a3", "a4"]);
    assert!(field(&forward, "cursor").is_null());
    let backward = fixture
        .query("jobs", "due", due("ada", 10, Value::Null, true))
        .unwrap();
    assert_eq!(ids(&backward), ["a4", "a3", "a2"]);
    // Pages continue from the host's cursor.
    let first = fixture
        .query("jobs", "due", due("ada", 2, Value::Null, false))
        .unwrap();
    assert_eq!(ids(&first), ["a2", "a3"]);
    let cursor = field(&first, "cursor").clone();
    assert!(cursor.as_str().is_some());
    let second = fixture
        .query("jobs", "due", due("ada", 2, cursor, false))
        .unwrap();
    assert_eq!(ids(&second), ["a4"]);
    // A full prefix selects the exact entry.
    let exact = fixture
        .query(
            "jobs",
            "exactly",
            obj(vec![("owner", s("bob")), ("due", n(3.0))]),
        )
        .unwrap();
    assert_eq!(ids(&exact), ["b3"]);
    // Untyped fields compare as values.
    let by_due = fixture
        .query("jobs", "by_due", obj(vec![("from", n(4.0))]))
        .unwrap();
    assert_eq!(ids(&by_due), ["a4", "a5"]);
}

#[test]
fn source_pages_walk_the_whole_collection() {
    let fixture = jobs("pages");
    let mut seen = Vec::new();
    let mut after = Value::Null;
    loop {
        let page = fixture
            .query(
                "jobs",
                "inventory",
                obj(vec![("limit", n(4.0)), ("after", after.clone())]),
            )
            .unwrap();
        seen.extend(ids(&page));
        after = field(&page, "cursor").clone();
        if after.is_null() {
            break;
        }
    }
    seen.sort();
    assert_eq!(seen, ["a1", "a2", "a3", "a4", "a5", "b3"]);
    // A cursor belongs to its collection: reusing one elsewhere fails.
    let page = fixture
        .query(
            "jobs",
            "inventory",
            obj(vec![("limit", n(1.0)), ("after", Value::Null)]),
        )
        .unwrap();
    let cursor = field(&page, "cursor").clone();
    let failure = fixture
        .query("jobs", "notes", obj(vec![("after", cursor)]))
        .unwrap_err();
    assert_eq!(failure.code, codes::INVALID_VALUE);
    assert!(
        failure.message.contains("out of scope"),
        "{}",
        failure.message
    );
}

#[test]
fn typed_handles_read_other_modules_exports() {
    let mut fixture = jobs("imports");
    fixture.deploy("board", &board_app::APP).unwrap();
    let board = fixture
        .query("board", "board", obj(vec![("owner", s("bob"))]))
        .unwrap();
    assert_eq!(ids(&board), ["b3"]);
    assert_eq!(
        fixture.query("board", "first", obj(vec![("id", s("a2"))])),
        Ok(n(2.0))
    );
    let _ = none();
}

/// Tasks with a raw trigger (no same-named mutation), a typed trigger,
/// and a derived value materialized for each task.
mod hooks_app {
    use std::cell::Cell;

    use flower_sdk::features::TypedTrigger;
    use flower_sdk::{
        App, Change, CollectionRef, Ctx, Derived, DerivedCtx, FlowerType, Materialize, MutationCtx,
        QueryCtx, ReadCtx, Trigger, Value, collection, derived, mutation, query,
    };

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Task {
        pub title: String,
    }

    #[collection(name = "tasks", key = String, value = Task)]
    pub struct Tasks;

    #[collection(name = "log", key = u32, value = String)]
    pub struct Log;

    #[collection(name = "counts", key = String, value = u32)]
    pub struct Counts;

    thread_local! {
        /// Evaluations of `title_len` on this test's thread.
        pub static EVALUATIONS: Cell<u32> = const { Cell::new(0) };
    }

    /// One root per task: the title's length.
    #[derived]
    pub fn title_len(ctx: &mut DerivedCtx<'_, '_>, key: String) -> flower_sdk::Result<u32> {
        EVALUATIONS.with(|count| count.set(count.get() + 1));
        let title = Tasks::get(ctx, &key)?
            .map(|task| task.title.len() as u32)
            .unwrap_or(0);
        Ok(title)
    }

    pub static TITLE_LEN_EACH: Derived = Derived::new("title_len", __flower_export_title_len)
        .args(<TitleLenArgs as FlowerType>::SCHEMA)
        .materialize(Materialize::Each(&TASKS_COLLECTION));

    #[mutation]
    pub fn put(ctx: &mut MutationCtx<'_, '_>, id: String, title: String) -> flower_sdk::Result<()> {
        Tasks::set(ctx, &id, &Task { title })
    }

    #[mutation]
    pub fn remove(ctx: &mut MutationCtx<'_, '_>, id: String) -> flower_sdk::Result<()> {
        Tasks::delete(ctx, &id)
    }

    #[query]
    pub fn length(ctx: &mut QueryCtx<'_, '_>, id: String) -> flower_sdk::Result<Value> {
        let mut args = std::collections::BTreeMap::new();
        args.insert(String::from("key"), Value::String(id));
        ctx.get_derived("", "title_len", &Value::Object(args))
    }

    #[query]
    pub fn log(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Vec<String>> {
        Ok(Log::page(ctx, 100, None)?
            .rows
            .into_iter()
            .map(|row| row.value)
            .collect())
    }

    #[query]
    pub fn count(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Option<u32>> {
        Counts::get(ctx, &String::from("changes"))
    }

    /// A raw trigger: counts every change, through no mutation of its own.
    fn count_changes(ctx: &mut Ctx<'_, '_>, _change: &Change) -> flower_sdk::Result<()> {
        let counts = CollectionRef::local("counts");
        let changes = Value::from("changes");
        let current = match ctx.get(counts, &changes)? {
            Some(Value::Number(count)) => count,
            _ => 0.0,
        };
        ctx.set(counts, &changes, Value::Number(current + 1.0))
    }

    pub static COUNT_CHANGES: Trigger = Trigger::new("count_changes", "tasks", count_changes);

    /// A typed trigger: logs each change with its decoded images.
    pub struct Audit;

    impl TypedTrigger for Audit {
        type Source = Tasks;
        const NAME: &'static str = "audit";

        fn run(
            ctx: &mut MutationCtx<'_, '_>,
            key: String,
            before: Option<Task>,
            after: Option<Task>,
        ) -> flower_sdk::Result<()> {
            let next = Log::page(ctx, 1000, None)?.rows.len() as u32;
            let image = |task: &Option<Task>| match task {
                Some(task) => task.title.clone(),
                None => String::from("-"),
            };
            let line = format!("{key}: {} -> {}", image(&before), image(&after));
            Log::set(ctx, &next, &line)
        }
    }

    static COLLECTIONS: [&dyn flower_sdk::Component; 3] =
        [&TASKS_COLLECTION, &LOG_COLLECTION, &COUNTS_COLLECTION];
    static TRIGGERS: [&dyn flower_sdk::Component; 2] = [&COUNT_CHANGES, &Audit::TRIGGER];
    static DEFINITIONS: [&dyn flower_sdk::Component; 6] =
        [&TITLE_LEN_EACH, &PUT, &REMOVE, &LENGTH, &LOG, &COUNT];
    pub static APP: App = App::new(&[&COLLECTIONS, &DEFINITIONS, &TRIGGERS]);

    /// The same app without the hooks, materializing nothing.
    static PLAIN_DEFINITIONS: [&dyn flower_sdk::Component; 6] =
        [&TITLE_LEN, &PUT, &REMOVE, &LENGTH, &LOG, &COUNT];
    pub static PLAIN: App = App::new(&[&COLLECTIONS, &PLAIN_DEFINITIONS]);
}

fn evaluations() -> u32 {
    hooks_app::EVALUATIONS.with(std::cell::Cell::get)
}

/// Whether `title_len({"key": id})` holds a pinned outcome.
fn pinned(fixture: &Fixture, id: &str) -> Option<bool> {
    let read = fixture.engine.store().begin_read().unwrap();
    let definition = read.intern_id(b"db", b"hooks\0title_len").unwrap().unwrap();
    let args = format!("{{\"key\":\"{id}\"}}");
    read.get_derived(b"db", definition, args.as_bytes())
        .unwrap()
        .map(|outcome| outcome.pinned)
}

#[test]
fn triggers_run_raw_and_typed_callbacks() {
    let mut fixture = Fixture::new("triggers");
    // A raw trigger needs no mutation of its own name to deploy.
    fixture.deploy("hooks", &hooks_app::APP).unwrap();
    let put = |id: &str, title: &str| obj(vec![("id", s(id)), ("title", s(title))]);
    fixture.mutate("hooks", "put", put("t1", "draft")).unwrap();
    fixture.mutate("hooks", "put", put("t1", "final")).unwrap();
    fixture
        .mutate("hooks", "remove", obj(vec![("id", s("t1"))]))
        .unwrap();
    assert_eq!(fixture.query("hooks", "count", none()), Ok(n(3.0)));
    let log = fixture.query("hooks", "log", none()).unwrap();
    assert_eq!(
        log,
        Value::Array(vec![
            s("t1: - -> draft"),
            s("t1: draft -> final"),
            s("t1: final -> -"),
        ])
    );
}

#[test]
fn each_roots_follow_their_source_rows() {
    let mut fixture = Fixture::new("each");
    fixture.deploy("hooks", &hooks_app::APP).unwrap();
    let put = |id: &str, title: &str| obj(vec![("id", s(id)), ("title", s(title))]);
    let before = evaluations();
    fixture.mutate("hooks", "put", put("t1", "draft")).unwrap();
    // Inserting the row evaluated and pinned its root in the mutation.
    assert_eq!(evaluations(), before + 1);
    assert_eq!(pinned(&fixture, "t1"), Some(true));
    assert_eq!(
        fixture.query("hooks", "length", obj(vec![("id", s("t1"))])),
        Ok(n(5.0))
    );
    assert_eq!(evaluations(), before + 1, "a pinned root serves from cache");
    // A change refreshes it eagerly; the read costs nothing.
    fixture.mutate("hooks", "put", put("t1", "longer")).unwrap();
    assert_eq!(evaluations(), before + 2);
    assert_eq!(
        fixture.query("hooks", "length", obj(vec![("id", s("t1"))])),
        Ok(n(6.0))
    );
    assert_eq!(evaluations(), before + 2);
    // Deleting the row releases its root.
    fixture
        .mutate("hooks", "remove", obj(vec![("id", s("t1"))]))
        .unwrap();
    assert_ne!(pinned(&fixture, "t1"), Some(true));
}

#[test]
fn each_roots_cover_rows_older_than_the_deployment() {
    let mut fixture = Fixture::new("each-old");
    fixture.deploy("hooks", &hooks_app::PLAIN).unwrap();
    fixture
        .mutate(
            "hooks",
            "put",
            obj(vec![("id", s("old")), ("title", s("ab"))]),
        )
        .unwrap();
    assert_eq!(pinned(&fixture, "old"), None);
    fixture.deploy("hooks", &hooks_app::APP).unwrap();
    // The first evaluation of an older row's root pins it.
    assert_eq!(
        fixture.query("hooks", "length", obj(vec![("id", s("old"))])),
        Ok(n(2.0))
    );
    assert_eq!(pinned(&fixture, "old"), Some(true));
    // A key with no row stays unpinned.
    assert_eq!(
        fixture.query("hooks", "length", obj(vec![("id", s("nowhere"))])),
        Ok(n(0.0))
    );
    assert_eq!(pinned(&fixture, "nowhere"), Some(false));
}

/// Docs whose deletes go to an archive, with children cascading.
mod archive_app {
    use flower_sdk::features::TypedRetention;
    use flower_sdk::{
        App, CollectionRef, Component, Ctx, DeletedRow, FlowerType, MutationCtx, QueryCtx,
        Retention, Value, collection, mutation, query,
    };

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Doc {
        pub title: String,
    }

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Child {
        pub doc: String,
    }

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Archived {
        pub title: String,
        pub deleted_at: f64,
    }

    #[collection(name = "docs", key = String, value = Doc)]
    pub struct Docs;

    #[collection(
        name = "children",
        key = String,
        value = Child,
        index(name = "by_doc", fields(doc)),
        reference(target = Docs, fields(doc), on_delete = cascade),
    )]
    pub struct Children;

    #[collection(name = "archive", key = String, value = Archived)]
    pub struct Archive;

    #[collection(name = "orphans", key = String, value = String)]
    pub struct Orphans;

    /// Typed: archive the removed doc with its deletion time.
    pub struct KeepDocs;

    impl TypedRetention for KeepDocs {
        type Source = Docs;
        const NAME: &'static str = "keep_docs";

        fn retain(
            ctx: &mut MutationCtx<'_, '_>,
            key: String,
            value: Doc,
            deleted_at: f64,
        ) -> flower_sdk::Result<()> {
            let archived = Archived {
                title: value.title,
                deleted_at,
            };
            Archive::set(ctx, &key, &archived)
        }
    }

    /// Raw: note each removed child's parent.
    fn keep_children(ctx: &mut Ctx<'_, '_>, row: &DeletedRow) -> flower_sdk::Result<()> {
        let key = row.key.clone();
        let parent = row
            .value
            .as_object()
            .and_then(|map| map.get("doc"))
            .cloned()
            .unwrap_or(Value::Null);
        ctx.set(CollectionRef::local("orphans"), &key, parent)
    }

    pub static KEEP_CHILDREN: Retention = Retention {
        name: "keep_children",
        source: "children",
        retain: keep_children,
    };

    #[mutation]
    pub fn put(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        title: String,
        child: Option<String>,
    ) -> flower_sdk::Result<()> {
        Docs::set(ctx, &id, &Doc { title })?;
        if let Some(child) = child {
            Children::set(ctx, &child, &Child { doc: id })?;
        }
        Ok(())
    }

    #[mutation]
    pub fn remove(ctx: &mut MutationCtx<'_, '_>, id: String) -> flower_sdk::Result<()> {
        Docs::delete(ctx, &id)
    }

    #[mutation]
    pub fn purge(ctx: &mut MutationCtx<'_, '_>, id: String) -> flower_sdk::Result<()> {
        ctx.delete_permanently(CollectionRef::local("docs"), &Value::from(id))
    }

    /// Delete and recreate in one transaction: the net change is nil,
    /// the deletion still happened.
    #[mutation]
    pub fn recreate(ctx: &mut MutationCtx<'_, '_>, id: String) -> flower_sdk::Result<()> {
        let doc = Docs::get(ctx, &id)?;
        Docs::delete(ctx, &id)?;
        if let Some(doc) = doc {
            Docs::set(ctx, &id, &doc)?;
        }
        Ok(())
    }

    #[query]
    pub fn archived(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Vec<String>> {
        Ok(Archive::page(ctx, 100, None)?
            .rows
            .into_iter()
            .map(|row| {
                format!(
                    "{}:{}:{}",
                    row.key,
                    row.value.title,
                    row.value.deleted_at > 0.0
                )
            })
            .collect())
    }

    #[query]
    pub fn orphans(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Vec<String>> {
        Ok(Orphans::page(ctx, 100, None)?
            .rows
            .into_iter()
            .map(|row| format!("{}<-{}", row.key, row.value))
            .collect())
    }

    static COLLECTIONS: [&dyn flower_sdk::Component; 4] = [
        &DOCS_COLLECTION,
        &CHILDREN_COLLECTION,
        &ARCHIVE_COLLECTION,
        &ORPHANS_COLLECTION,
    ];
    static USES: [&dyn Component; 2] = [&KeepDocs::RETENTION, &KEEP_CHILDREN];
    static DEFINITIONS: [&dyn flower_sdk::Component; 6] =
        [&PUT, &REMOVE, &PURGE, &RECREATE, &ARCHIVED, &ORPHANS];
    pub static APP: App = App::new(&[&USES, &COLLECTIONS, &DEFINITIONS]);
}

#[test]
fn retention_hooks_see_every_successful_delete() {
    let mut fixture = Fixture::new("retention");
    fixture.deploy("archive", &archive_app::APP).unwrap();
    let put = |id: &str, child: Value| {
        obj(vec![
            ("id", s(id)),
            ("title", s(&format!("{id}!"))),
            ("child", child),
        ])
    };
    let id = |id: &str| obj(vec![("id", s(id))]);
    fixture
        .mutate("archive", "put", put("d1", s("c1")))
        .unwrap();
    fixture
        .mutate("archive", "put", put("d2", s("c2")))
        .unwrap();
    fixture
        .mutate("archive", "put", put("d3", Value::Null))
        .unwrap();
    // A delete archives the row, and its cascade reaches the child's hook.
    fixture.mutate("archive", "remove", id("d1")).unwrap();
    // A permanent delete skips retention, cascades included.
    fixture.mutate("archive", "purge", id("d2")).unwrap();
    // Delete-then-recreate is still a deletion.
    fixture.mutate("archive", "recreate", id("d3")).unwrap();
    // Deleting nothing is no event.
    fixture.mutate("archive", "remove", id("missing")).unwrap();
    assert_eq!(
        fixture.query("archive", "archived", none()),
        Ok(Value::Array(vec![s("d1:d1!:true"), s("d3:d3!:true")]))
    );
    assert_eq!(
        fixture.query("archive", "orphans", none()),
        Ok(Value::Array(vec![s("c1<-d1")]))
    );
}

/// Orders with per-shop aggregates over the `by_shop` index: an
/// invertible, versioned total, maintained by deltas, and the largest
/// order, which no delta can take out.
mod totals_app {
    use std::cell::Cell;

    use flower_sdk::index::{Index, TypedAggregate};
    use flower_sdk::{
        App, Collection, Derived, FlowerType, Materialize, MutationCtx, QueryCtx, ReadCtx, Value,
        collection, mutation, query,
    };

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Order {
        pub shop: String,
        pub cents: u64,
    }

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Total {
        pub orders: u64,
        pub cents: u64,
    }

    #[collection(
        name = "orders",
        key = String,
        value = Order,
        index(name = "by_shop", fields(shop), types(String)),
    )]
    pub struct Orders;

    thread_local! {
        /// Total group builds on this test's thread.
        pub static BUILDS: Cell<u32> = const { Cell::new(0) };
        /// Rows the total's reducer folded in.
        pub static ADDS: Cell<u32> = const { Cell::new(0) };
        /// Rows the total's reducer took out.
        pub static REMOVES: Cell<u32> = const { Cell::new(0) };
        /// Largest-order group builds.
        pub static TOP_BUILDS: Cell<u32> = const { Cell::new(0) };
    }

    fn bump(counter: &'static std::thread::LocalKey<Cell<u32>>) {
        counter.with(|count| count.set(count.get() + 1));
    }

    fn empty() -> Total {
        Total {
            orders: 0,
            cents: 0,
        }
    }

    fn plus(total: Total, row: &Order) -> Total {
        Total {
            orders: total.orders + 1,
            cents: total.cents + row.cents,
        }
    }

    fn minus(total: Total, row: &Order) -> Total {
        Total {
            orders: total.orders - 1,
            cents: total.cents - row.cents,
        }
    }

    pub struct ShopTotal;

    impl TypedAggregate for ShopTotal {
        type Source = Orders;
        type Fields = (String,);
        type Group = (String,);
        type Accumulator = Total;
        const NAME: &'static str = "shop_total";
        const INDEX: &'static str = "by_shop";
        const PAGE: u32 = 2;
        const VERSION: Option<&'static str> = Some("1");

        fn initial(_group: &(String,)) -> flower_sdk::Result<Total> {
            bump(&BUILDS);
            Ok(empty())
        }

        fn add(
            total: Total,
            _key: &String,
            row: &Order,
            _group: &(String,),
        ) -> flower_sdk::Result<Total> {
            bump(&ADDS);
            Ok(plus(total, row))
        }

        fn remove(
            total: Total,
            _key: &String,
            row: &Order,
            _group: &(String,),
        ) -> flower_sdk::Result<Option<Total>> {
            bump(&REMOVES);
            Ok(Some(minus(total, row)))
        }
    }

    /// The same total under a new version: its accumulators start over.
    pub struct BumpedTotal;

    impl TypedAggregate for BumpedTotal {
        type Source = Orders;
        type Fields = (String,);
        type Group = (String,);
        type Accumulator = Total;
        const NAME: &'static str = "shop_total";
        const INDEX: &'static str = "by_shop";
        const VERSION: Option<&'static str> = Some("2");

        fn initial(group: &(String,)) -> flower_sdk::Result<Total> {
            ShopTotal::initial(group)
        }

        fn add(
            total: Total,
            key: &String,
            row: &Order,
            group: &(String,),
        ) -> flower_sdk::Result<Total> {
            ShopTotal::add(total, key, row, group)
        }

        fn remove(
            total: Total,
            key: &String,
            row: &Order,
            group: &(String,),
        ) -> flower_sdk::Result<Option<Total>> {
            ShopTotal::remove(total, key, row, group)
        }
    }

    /// The largest order of a shop: a row leaving cannot be taken out,
    /// so the default `remove` builds the group again.
    pub struct ShopTop;

    impl TypedAggregate for ShopTop {
        type Source = Orders;
        type Fields = (String,);
        type Group = (String,);
        type Accumulator = u64;
        const NAME: &'static str = "shop_top";
        const INDEX: &'static str = "by_shop";

        fn initial(_group: &(String,)) -> flower_sdk::Result<u64> {
            bump(&TOP_BUILDS);
            Ok(0)
        }

        fn add(
            top: u64,
            _key: &String,
            row: &Order,
            _group: &(String,),
        ) -> flower_sdk::Result<u64> {
            Ok(top.max(row.cents))
        }
    }

    /// Every evaluated group of the largest order stays pinned.
    pub static TOP_DERIVED: Derived = ShopTop::DERIVED.materialize(Materialize::Always);

    fn group(shop: String) -> Value {
        let mut args = std::collections::BTreeMap::new();
        args.insert(
            String::from("group"),
            Value::Array(vec![Value::String(shop)]),
        );
        Value::Object(args)
    }

    #[mutation]
    pub fn order(
        ctx: &mut MutationCtx<'_, '_>,
        id: String,
        shop: String,
        cents: u64,
    ) -> flower_sdk::Result<()> {
        Orders::set(ctx, &id, &Order { shop, cents })
    }

    #[mutation]
    pub fn cancel(ctx: &mut MutationCtx<'_, '_>, id: String) -> flower_sdk::Result<()> {
        Orders::delete(ctx, &id)
    }

    /// Several changes to one group in one transaction, settled in
    /// between: a row inserted then updated, another deleted. Answers
    /// the total the settle left.
    #[mutation]
    pub fn churn(ctx: &mut MutationCtx<'_, '_>, shop: String) -> flower_sdk::Result<Value> {
        let key = String::from;
        Orders::set(
            ctx,
            &key("c1"),
            &Order {
                shop: shop.clone(),
                cents: 10,
            },
        )?;
        ctx.ctx().settle()?;
        let settled = ctx.get_derived("", "shop_total", &group(shop.clone()))?;
        Orders::set(
            ctx,
            &key("c1"),
            &Order {
                shop: shop.clone(),
                cents: 25,
            },
        )?;
        Orders::delete(ctx, &key("o1"))?;
        Ok(settled)
    }

    #[query]
    pub fn total(ctx: &mut QueryCtx<'_, '_>, shop: String) -> flower_sdk::Result<Value> {
        ctx.get_derived("", "shop_total", &group(shop))
    }

    #[query]
    pub fn top(ctx: &mut QueryCtx<'_, '_>, shop: String) -> flower_sdk::Result<Value> {
        ctx.get_derived("", "shop_top", &group(shop))
    }

    /// The total built from the group's rows, as a rebuild would.
    #[query]
    pub fn fold(ctx: &mut QueryCtx<'_, '_>, shop: String) -> flower_sdk::Result<Total> {
        let index: Index<String, Order, (String,)> = Index::new("orders", "by_shop");
        let mut total = empty();
        for row in index.prefix((shop,)).page(ctx, 1000)?.rows {
            total = plus(total, &row.value);
        }
        Ok(total)
    }

    /// A query the other bundles lack: the same aggregates under new code.
    #[query]
    pub fn noop(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<()> {
        let _ = ctx;
        Ok(())
    }

    static COLLECTIONS: [&dyn flower_sdk::Component; 1] = [&ORDERS_COLLECTION];
    static DEFINITIONS: [&dyn flower_sdk::Component; 8] = [
        &ShopTotal::DERIVED,
        &TOP_DERIVED,
        &ORDER,
        &CANCEL,
        &CHURN,
        &TOTAL,
        &TOP,
        &FOLD,
    ];
    pub static APP: App = App::new(&[&COLLECTIONS, &DEFINITIONS]);

    /// New code keeping the total's version.
    static NEXT_DEFINITIONS: [&dyn flower_sdk::Component; 9] = [
        &ShopTotal::DERIVED,
        &TOP_DERIVED,
        &ORDER,
        &CANCEL,
        &CHURN,
        &TOTAL,
        &TOP,
        &FOLD,
        &NOOP,
    ];
    pub static NEXT: App = App::new(&[&COLLECTIONS, &NEXT_DEFINITIONS]);

    /// New code with a new version of the total.
    static BUMPED_DEFINITIONS: [&dyn flower_sdk::Component; 8] = [
        &BumpedTotal::DERIVED,
        &TOP_DERIVED,
        &ORDER,
        &CANCEL,
        &CHURN,
        &TOTAL,
        &TOP,
        &FOLD,
    ];
    pub static BUMPED: App = App::new(&[&COLLECTIONS, &BUMPED_DEFINITIONS]);

    /// The orders without their index, and nothing aggregating them.
    static BARE_ORDERS: Collection = Collection::new("orders")
        .key_schema(<String as FlowerType>::SCHEMA)
        .value_schema(<Order as FlowerType>::SCHEMA);
    static BARE_COLLECTIONS: [&dyn flower_sdk::Component; 1] = [&BARE_ORDERS];
    static BARE_DEFINITIONS: [&dyn flower_sdk::Component; 2] = [&ORDER, &CANCEL];
    pub static BARE: App = App::new(&[&BARE_COLLECTIONS, &BARE_DEFINITIONS]);
}

fn counter(key: &'static std::thread::LocalKey<std::cell::Cell<u32>>) -> u32 {
    key.with(std::cell::Cell::get)
}

fn order(id: &str, shop: &str, cents: f64) -> Value {
    obj(vec![("id", s(id)), ("shop", s(shop)), ("cents", n(cents))])
}

fn shop(name: &str) -> Value {
    obj(vec![("shop", s(name))])
}

/// A shop's total: `{orders, cents}`.
fn total_of(orders: f64, cents: f64) -> Value {
    obj(vec![("orders", n(orders)), ("cents", n(cents))])
}

#[test]
fn aggregates_fold_one_group_and_follow_only_its_rows() {
    let mut fixture = Fixture::new("aggregates");
    fixture.deploy("totals", &totals_app::APP).unwrap();
    for (id, shop, cents) in [
        ("o1", "north", 100.0),
        ("o2", "north", 250.0),
        ("o3", "north", 50.0),
        ("o4", "south", 7.0),
    ] {
        fixture
            .mutate("totals", "order", order(id, shop, cents))
            .unwrap();
    }
    let total =
        |fixture: &Fixture, name: &str| fixture.query("totals", "total", shop(name)).unwrap();
    let builds = || counter(&totals_app::BUILDS);
    // Folded across several range pages.
    assert_eq!(total(&fixture, "north"), total_of(3.0, 400.0));
    assert_eq!(total(&fixture, "south"), total_of(1.0, 7.0));
    let built = builds();
    // Another group's write leaves this one cached.
    fixture
        .mutate("totals", "order", order("o5", "south", 3.0))
        .unwrap();
    assert_eq!(total(&fixture, "north"), total_of(3.0, 400.0));
    assert_eq!(builds(), built);
    // Its own write, or a row moving out, refreshes it.
    fixture
        .mutate("totals", "order", order("o2", "south", 250.0))
        .unwrap();
    assert_eq!(total(&fixture, "north"), total_of(2.0, 150.0));
    assert_eq!(total(&fixture, "south"), total_of(3.0, 260.0));
}

/// A cached total takes its group's inserts, updates, moves and deletes
/// as deltas, in the writing transaction: its accumulator goes through
/// `remove` and `add` and never builds again, and after every change it
/// equals the total built from the rows. Without delta maintenance each
/// of these writes rebuilds the group (`BUILDS` moves).
#[test]
fn aggregates_take_row_changes_as_deltas() {
    let mut fixture = Fixture::new("aggregate-deltas");
    let before = counter(&totals_app::BUILDS);
    fixture.deploy("totals", &totals_app::APP).unwrap();
    for (id, shop, cents) in [
        ("o1", "north", 100.0),
        ("o2", "north", 250.0),
        ("o4", "south", 7.0),
    ] {
        fixture
            .mutate("totals", "order", order(id, shop, cents))
            .unwrap();
    }
    let check = |fixture: &Fixture, name: &str, expected: Value| {
        assert_eq!(
            fixture.query("totals", "total", shop(name)).unwrap(),
            expected
        );
        assert_eq!(
            fixture.query("totals", "fold", shop(name)).unwrap(),
            expected
        );
    };
    // Both groups build once, and cache.
    check(&fixture, "north", total_of(2.0, 350.0));
    check(&fixture, "south", total_of(1.0, 7.0));
    let built = counter(&totals_app::BUILDS);
    assert_eq!(built, before + 2);
    let (adds, removes) = (counter(&totals_app::ADDS), counter(&totals_app::REMOVES));

    // An insert folds one row in.
    fixture
        .mutate("totals", "order", order("o3", "north", 50.0))
        .unwrap();
    assert_eq!(counter(&totals_app::ADDS), adds + 1);
    assert_eq!(counter(&totals_app::REMOVES), removes);
    check(&fixture, "north", total_of(3.0, 400.0));
    // An update within the group takes the old image out and the new in.
    fixture
        .mutate("totals", "order", order("o1", "north", 120.0))
        .unwrap();
    assert_eq!(counter(&totals_app::ADDS), adds + 2);
    assert_eq!(counter(&totals_app::REMOVES), removes + 1);
    check(&fixture, "north", total_of(3.0, 420.0));
    // A move leaves one group and enters the other.
    fixture
        .mutate("totals", "order", order("o2", "south", 250.0))
        .unwrap();
    assert_eq!(counter(&totals_app::ADDS), adds + 3);
    assert_eq!(counter(&totals_app::REMOVES), removes + 2);
    check(&fixture, "north", total_of(2.0, 170.0));
    check(&fixture, "south", total_of(2.0, 257.0));
    // A delete takes it out.
    fixture
        .mutate("totals", "cancel", obj(vec![("id", s("o3"))]))
        .unwrap();
    assert_eq!(counter(&totals_app::REMOVES), removes + 3);
    check(&fixture, "north", total_of(1.0, 120.0));
    // Changes settled mid-transaction fold in once each: the value read
    // after the settle has the insert, the commit the rest.
    let settled = fixture.mutate("totals", "churn", shop("north")).unwrap();
    assert_eq!(settled, total_of(2.0, 130.0));
    check(&fixture, "north", total_of(1.0, 25.0));
    // Another group's write reached no reducer for this one.
    let (adds, removes) = (counter(&totals_app::ADDS), counter(&totals_app::REMOVES));
    fixture
        .mutate("totals", "order", order("o9", "west", 1.0))
        .unwrap();
    assert_eq!(counter(&totals_app::ADDS), adds);
    assert_eq!(counter(&totals_app::REMOVES), removes);
    // Not one rebuild.
    assert_eq!(counter(&totals_app::BUILDS), built);
}

/// An accumulator the reducer cannot invert builds again from its rows,
/// with the right value: a pinned group at once, in the writing
/// transaction.
#[test]
fn aggregates_rebuild_where_the_reducer_cannot_invert() {
    let mut fixture = Fixture::new("aggregate-rebuilds");
    fixture.deploy("totals", &totals_app::APP).unwrap();
    for (id, cents) in [("o1", 100.0), ("o2", 250.0)] {
        fixture
            .mutate("totals", "order", order(id, "north", cents))
            .unwrap();
    }
    let top = |fixture: &Fixture| fixture.query("totals", "top", shop("north")).unwrap();
    assert_eq!(top(&fixture), n(250.0));
    let built = counter(&totals_app::TOP_BUILDS);
    // A row coming in folds through `add`, without a build.
    fixture
        .mutate("totals", "order", order("o3", "north", 300.0))
        .unwrap();
    assert_eq!(counter(&totals_app::TOP_BUILDS), built);
    assert_eq!(top(&fixture), n(300.0));
    // A row leaving cannot be taken out of a maximum: the pinned group
    // builds again inside the write.
    fixture
        .mutate("totals", "cancel", obj(vec![("id", s("o3"))]))
        .unwrap();
    assert_eq!(counter(&totals_app::TOP_BUILDS), built + 1);
    assert_eq!(top(&fixture), n(250.0));
    assert_eq!(counter(&totals_app::TOP_BUILDS), built + 1);
}

/// A versioned aggregate keeps its accumulators across a deployment
/// that keeps its version, and goes on from them; a new version, or an
/// aggregate without one, builds its groups again.
#[test]
fn aggregate_versions_keep_accumulators_across_deployments() {
    let mut fixture = Fixture::new("aggregate-versions");
    fixture.deploy("totals", &totals_app::APP).unwrap();
    for (id, cents) in [("o1", 100.0), ("o2", 250.0)] {
        fixture
            .mutate("totals", "order", order(id, "north", cents))
            .unwrap();
    }
    let total = |fixture: &Fixture| fixture.query("totals", "total", shop("north")).unwrap();
    let top = |fixture: &Fixture| fixture.query("totals", "top", shop("north")).unwrap();
    assert_eq!(total(&fixture), total_of(2.0, 350.0));
    assert_eq!(top(&fixture), n(250.0));
    let (builds, top_builds) = (
        counter(&totals_app::BUILDS),
        counter(&totals_app::TOP_BUILDS),
    );

    // New code, same version: the total serves its accumulator and takes
    // deltas from it; the unversioned maximum builds again.
    fixture.deploy("totals", &totals_app::NEXT).unwrap();
    assert_eq!(total(&fixture), total_of(2.0, 350.0));
    assert_eq!(counter(&totals_app::BUILDS), builds);
    assert_eq!(top(&fixture), n(250.0));
    assert_eq!(counter(&totals_app::TOP_BUILDS), top_builds + 1);
    fixture
        .mutate("totals", "order", order("o3", "north", 5.0))
        .unwrap();
    assert_eq!(total(&fixture), total_of(3.0, 355.0));
    assert_eq!(counter(&totals_app::BUILDS), builds);

    // A new version starts over.
    fixture.deploy("totals", &totals_app::BUMPED).unwrap();
    assert_eq!(total(&fixture), total_of(3.0, 355.0));
    assert_eq!(counter(&totals_app::BUILDS), builds + 1);

    // The first code again: a new run of version 1, whose values are
    // computed afresh.
    fixture.deploy("totals", &totals_app::APP).unwrap();
    assert_eq!(total(&fixture), total_of(3.0, 355.0));
    assert_eq!(counter(&totals_app::BUILDS), builds + 2);
    // Code without the aggregate or its index, during which no index
    // entry tracks the group's writes; then the same code as before the
    // gap. Its values from before the gap never serve.
    fixture.deploy("totals", &totals_app::BARE).unwrap();
    fixture
        .mutate("totals", "order", order("o4", "north", 1000.0))
        .unwrap();
    fixture.deploy("totals", &totals_app::APP).unwrap();
    assert_eq!(total(&fixture), total_of(4.0, 1355.0));
    assert_eq!(counter(&totals_app::BUILDS), builds + 3);
}

/// Maintenance tasks: a typed queue drain, a raw task that backs off
/// while it fails, and a typed task handling its own failures.
mod tasks_app {
    use std::cell::Cell;

    use flower_sdk::features::TypedTask;
    use flower_sdk::{
        App, CollectionRef, Component, Ctx, FlowerType, MutationCtx, QueryCtx, ReadCtx, Task,
        Value, collection, fail, mutation, query,
    };

    #[derive(FlowerType, Clone, Debug, PartialEq)]
    pub struct Work {
        pub due: u64,
    }

    #[collection(
        name = "work",
        key = String,
        value = Work,
        index(name = "by_due", fields(due), types(u64)),
    )]
    pub struct Queue;

    #[collection(name = "done", key = String, value = u64)]
    pub struct Done;

    #[collection(name = "flags", key = String, value = String)]
    pub struct Flags;

    thread_local! {
        /// Drain runs on this test's thread.
        pub static RUNS: Cell<u32> = const { Cell::new(0) };
    }

    /// Drains `work` one item per run, earliest first, once it is due.
    pub struct Drain;

    impl TypedTask for Drain {
        const NAME: &'static str = "drain";

        fn due(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Option<f64>> {
            Ok(Queue::BY_DUE
                .query()
                .page(ctx, 1)?
                .rows
                .first()
                .map(|row| row.value.due as f64))
        }

        fn run(ctx: &mut MutationCtx<'_, '_>) -> flower_sdk::Result<Value> {
            RUNS.with(|runs| runs.set(runs.get() + 1));
            let now = ctx.now()? as u64;
            let page = Queue::BY_DUE.query().lte(&now).page(ctx, 1)?;
            let Some(row) = page.rows.into_iter().next() else {
                return Ok(Value::Null);
            };
            Queue::delete(ctx, &row.key)?;
            Done::set(ctx, &row.key, &now)?;
            Ok(Value::String(row.key))
        }
    }

    fn flag(ctx: &mut Ctx<'_, '_>, name: &str) -> flower_sdk::Result<bool> {
        Ok(ctx
            .get(CollectionRef::local("flags"), &Value::from(name))?
            .is_some())
    }

    /// Due while `todo` is flagged; fails while `broken` is too.
    fn flaky_due(ctx: &mut Ctx<'_, '_>) -> flower_sdk::Result<Option<f64>> {
        Ok(flag(ctx, "todo")?.then_some(0.0))
    }

    fn flaky_run(ctx: &mut Ctx<'_, '_>) -> flower_sdk::Result<Value> {
        if flag(ctx, "broken")? {
            ctx.delete(CollectionRef::local("flags"), &Value::from("todo"))?;
            return Err(fail("BOOM", "the flaky task is broken"));
        }
        ctx.delete(CollectionRef::local("flags"), &Value::from("todo"))?;
        Ok(Value::Null)
    }

    pub static FLAKY: Task = Task::new("flaky", flaky_due, flaky_run);

    /// Always fails, and handles its failures: records them and stops.
    pub struct Guarded;

    impl TypedTask for Guarded {
        const NAME: &'static str = "guarded";
        const HANDLES_FAILURES: bool = true;

        fn due(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Option<f64>> {
            Ok(Flags::get(ctx, &String::from("guard"))?.map(|_| 0.0))
        }

        fn run(ctx: &mut MutationCtx<'_, '_>) -> flower_sdk::Result<Value> {
            Flags::set(ctx, &String::from("partial"), &String::from("written"))?;
            Err(fail("NOPE", "the guarded task always fails"))
        }

        fn on_error(
            ctx: &mut MutationCtx<'_, '_>,
            failure: &flower_sdk::TaskFailure,
        ) -> flower_sdk::Result<Value> {
            Flags::delete(ctx, &String::from("guard"))?;
            Flags::set(ctx, &String::from("handled"), &failure.error.code)?;
            Ok(Value::Null)
        }
    }

    pub static GUARDED: Task = Guarded::TASK;
    pub static DRAIN: Task = Drain::TASK;

    #[mutation]
    pub fn push(ctx: &mut MutationCtx<'_, '_>, id: String, due: u64) -> flower_sdk::Result<()> {
        Queue::set(ctx, &id, &Work { due })
    }

    #[mutation]
    pub fn raise(ctx: &mut MutationCtx<'_, '_>, name: String) -> flower_sdk::Result<()> {
        Flags::set(ctx, &name, &name)
    }

    #[mutation]
    pub fn lower(ctx: &mut MutationCtx<'_, '_>, name: String) -> flower_sdk::Result<()> {
        Flags::delete(ctx, &name)
    }

    #[query]
    pub fn done(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Vec<String>> {
        Ok(Done::page(ctx, 100, None)?
            .rows
            .into_iter()
            .map(|row| row.key)
            .collect())
    }

    /// Every flag as `name=value`, sorted.
    #[query]
    pub fn flags(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Vec<String>> {
        let mut flags: Vec<String> = Flags::page(ctx, 100, None)?
            .rows
            .into_iter()
            .map(|row| format!("{}={}", row.key, row.value))
            .collect();
        flags.sort();
        Ok(flags)
    }

    static USES: [&dyn Component; 3] = [&DRAIN, &FLAKY, &GUARDED];
    static COLLECTIONS: [&dyn flower_sdk::Component; 3] =
        [&QUEUE_COLLECTION, &DONE_COLLECTION, &FLAGS_COLLECTION];
    static DEFINITIONS: [&dyn flower_sdk::Component; 5] = [&PUSH, &RAISE, &LOWER, &DONE, &FLAGS];
    pub static APP: App = App::new(&[&USES, &COLLECTIONS, &DEFINITIONS]);
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// The tasks due now, as `module/task`.
fn due_now(fixture: &Fixture) -> Vec<String> {
    fixture
        .engine
        .reader()
        .due_tasks(b"db", now())
        .unwrap()
        .into_iter()
        .map(|due| format!("{}/{}", due.module, due.task))
        .collect()
}

/// Prepare one run of `task` and apply its patch at `position`.
fn run_task(fixture: &mut Fixture, task: &str, position: u64) -> &'static str {
    match fixture
        .engine
        .prepare_task_run(b"db", "tasks", task)
        .unwrap()
    {
        TaskPrepare::Skip => "skip",
        TaskPrepare::Ran(prepared) => {
            fixture
                .engine
                .apply_patch(&prepared.patch, position)
                .unwrap()
                .result
                .unwrap();
            "ran"
        }
        TaskPrepare::Failed { prepared, .. } => {
            fixture
                .engine
                .apply_patch(&prepared.patch, position)
                .unwrap()
                .result
                .unwrap();
            "failed"
        }
    }
}

fn status(fixture: &Fixture, task: &str) -> TaskStatus {
    fixture
        .engine
        .reader()
        .tasks(b"db", "tasks")
        .unwrap()
        .into_iter()
        .find(|status| status.name == task)
        .unwrap()
}

/// A task runs only when due, one unit per run, until `due` says it has
/// no more work; two runs prepared on the same state commit once.
#[test]
fn tasks_run_when_due_and_once() {
    let mut fixture = Fixture::new("tasks-due");
    fixture.deploy("tasks", &tasks_app::APP).unwrap();
    // The manifest declares them; the listing reports them, healthy.
    let listed = fixture.engine.reader().modules(b"db").unwrap();
    let names: Vec<&str> = listed[0]
        .tasks
        .iter()
        .map(|task| task.name.as_str())
        .collect();
    assert_eq!(names, ["drain", "flaky", "guarded"]);
    assert!(listed[0].tasks.iter().all(|task| task.failures == 0));
    // Nothing is due without work.
    assert!(due_now(&fixture).is_empty());
    assert_eq!(run_task(&mut fixture, "drain", 1), "skip");
    let runs = || tasks_app::RUNS.with(std::cell::Cell::get);
    assert_eq!(runs(), 0);

    for (id, due) in [("w1", 0.0), ("w2", 1.0), ("w3", 9e15)] {
        fixture
            .mutate("tasks", "push", obj(vec![("id", s(id)), ("due", n(due))]))
            .unwrap();
    }
    assert_eq!(due_now(&fixture), ["tasks/drain"]);
    assert_eq!(run_task(&mut fixture, "drain", 2), "ran");
    assert_eq!(
        fixture.query("tasks", "done", none()),
        Ok(Value::Array(vec![s("w1")]))
    );
    // Two runs prepared on one state: the second, guarded by the
    // revision both saw, is refused once the first commits.
    let first = match fixture
        .engine
        .prepare_task_run(b"db", "tasks", "drain")
        .unwrap()
    {
        TaskPrepare::Ran(prepared) => prepared,
        other => panic!("expected a run, got {other:?}"),
    };
    let second = match fixture
        .engine
        .prepare_task_run(b"db", "tasks", "drain")
        .unwrap()
    {
        TaskPrepare::Ran(prepared) => prepared,
        other => panic!("expected a run, got {other:?}"),
    };
    fixture
        .engine
        .apply_patch(&first.patch, 3)
        .unwrap()
        .result
        .unwrap();
    let stale = fixture.engine.apply_patch(&second.patch, 4).unwrap().result;
    assert_eq!(stale.unwrap_err().code, "stale_revision");
    assert_eq!(
        fixture.query("tasks", "done", none()),
        Ok(Value::Array(vec![s("w1"), s("w2")]))
    );
    // The rest is not due yet: nothing runs, and the plan says when
    // the leader should look again.
    assert!(due_now(&fixture).is_empty());
    let plan = fixture.engine.reader().plan_tasks(b"db", now()).unwrap();
    assert_eq!(plan.next_ms, Some(9e15 as u64));
    let ran = runs();
    assert_eq!(run_task(&mut fixture, "drain", 5), "skip");
    assert_eq!(runs(), ran);
}

/// A failing run rolls back and backs the task off, its failure kept
/// for operators, while other tasks stay eligible; its next success
/// clears the state. A task with an error callback handles its failures
/// instead.
#[test]
fn failing_tasks_back_off_or_handle_their_failures() {
    let mut fixture = Fixture::new("tasks-failures");
    fixture.deploy("tasks", &tasks_app::APP).unwrap();
    let raise = |fixture: &mut Fixture, name: &str| {
        fixture
            .mutate("tasks", "raise", obj(vec![("name", s(name))]))
            .unwrap();
    };
    raise(&mut fixture, "todo");
    raise(&mut fixture, "broken");
    fixture
        .mutate("tasks", "push", obj(vec![("id", s("w1")), ("due", n(0.0))]))
        .unwrap();
    assert_eq!(due_now(&fixture), ["tasks/drain", "tasks/flaky"]);
    let started = now();
    assert_eq!(run_task(&mut fixture, "flaky", 1), "failed");
    // Its writes rolled back (`todo` is still there), and it backs off.
    assert_eq!(
        fixture.query("tasks", "flags", none()),
        Ok(Value::Array(vec![s("broken=broken"), s("todo=todo")]))
    );
    let failing = status(&fixture, "flaky");
    assert_eq!(failing.failures, 1);
    let retry = failing.retry_at_ms.unwrap();
    assert!(
        retry >= started + 1_000 && retry <= now() + 1_000,
        "{retry}"
    );
    assert_eq!(failing.error.unwrap().code, "BOOM");
    assert_eq!(due_now(&fixture), ["tasks/drain"]);
    assert_eq!(run_task(&mut fixture, "flaky", 2), "skip");
    // Another task still runs meanwhile.
    assert_eq!(run_task(&mut fixture, "drain", 3), "ran");
    // Fixed, and past its backoff: it runs and its state is gone.
    fixture
        .mutate("tasks", "lower", obj(vec![("name", s("broken"))]))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(
        retry.saturating_sub(now()) + 20,
    ));
    assert_eq!(due_now(&fixture), ["tasks/flaky"]);
    assert_eq!(run_task(&mut fixture, "flaky", 4), "ran");
    let healed = status(&fixture, "flaky");
    assert_eq!(
        (healed.failures, healed.retry_at_ms, healed.error),
        (0, None, None)
    );
    assert!(due_now(&fixture).is_empty());

    // The guarded task's error callback sees the failure over the state
    // the run saw: the run's write is gone, the callback's stays.
    raise(&mut fixture, "guard");
    assert_eq!(run_task(&mut fixture, "guarded", 5), "failed");
    assert_eq!(
        fixture.query("tasks", "flags", none()),
        Ok(Value::Array(vec![s("handled=NOPE")]))
    );
    assert_eq!(status(&fixture, "guarded").failures, 0);
    assert!(due_now(&fixture).is_empty());
}
