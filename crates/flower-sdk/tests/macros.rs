//! End-to-end macro tests: real derive/attribute expansion driving guest
//! paths through the native `invoke_native` and `manifest_native` doors.
//!
//! Method bodies stay pure: off-Wasm host calls panic, so anything touching
//! the host (typed handles, context reads) cannot run here. Dispatch,
//! argument schemas, envelopes and descriptors are fully covered.

extern crate alloc;

use flower_sdk::abi::{InvocationKind, invoke_native, manifest_native};
use flower_sdk::{
    App, BusinessError, DerivedCtx, FlowerError, FlowerType, MutationCtx, OnDelete, Place,
    QueryCtx, Reference, Value, collection, derived, mutation, query,
};
use std::collections::BTreeMap;

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Task {
    pub title: String,
    pub owner: String,
    #[flower(default)]
    pub count: u64,
    #[flower(skip)]
    pub cache: u64,
}

#[derive(FlowerType, Clone, Copy, Debug, PartialEq)]
pub enum Status {
    Open,
    Closed,
}

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub enum Event {
    Created(Task),
    Closed(String),
}

#[derive(FlowerError, Clone, Debug, PartialEq)]
pub enum TaskError {
    #[flower(code = "NOT_FOUND")]
    Missing,
    #[flower(code = "INVALID", retryable)]
    Invalid(String),
}

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Link {
    pub task: String,
}

#[collection(
    name = "tasks",
    key = String,
    value = Task,
    index(name = "by_owner", fields(owner)),
)]
pub struct Tasks;

#[collection(
    name = "links",
    key = String,
    value = Link,
    index(name = "by_task", fields(task)),
    reference(target = Tasks, fields(task), on_delete = cascade),
)]
pub struct Links;

#[query]
pub fn task_title(ctx: &mut QueryCtx<'_, '_>, task: Task) -> String {
    let _ = ctx;
    task.title.clone()
}

#[mutation]
pub fn bump_count(ctx: &mut MutationCtx<'_, '_>, task: Task) -> Result<Task, TaskError> {
    let _ = ctx;
    let mut task = task;
    task.count += 1;
    Ok(task)
}

#[mutation]
pub fn fail_closed(ctx: &mut MutationCtx<'_, '_>, reason: String) -> Result<String, TaskError> {
    let _ = ctx;
    Err(TaskError::Invalid(reason))
}

#[derived]
pub fn task_count(ctx: &mut DerivedCtx<'_, '_>, tasks: Vec<Task>) -> u64 {
    let _ = ctx;
    tasks.len() as u64
}

static DEFS: [&dyn flower_sdk::Component; 4] =
    [&TASK_TITLE, &BUMP_COUNT, &FAIL_CLOSED, &TASK_COUNT];
static COLLS: [&dyn flower_sdk::Component; 2] = [&TASKS_COLLECTION, &LINKS_COLLECTION];
static APP: App = App::new(&[&COLLS, &DEFS]);

fn task() -> Task {
    Task {
        title: String::from("Buy milk"),
        owner: String::from("ada"),
        count: 2,
        cache: 0,
    }
}

fn invoke(kind: InvocationKind, name: &str, args: Value) -> Value {
    let request = Value::Array(vec![Value::String(String::from(name)), args]);
    let input = request.encode_binary().unwrap();
    let output = invoke_native(&APP, kind as u32, &input);
    assert!(!output.is_empty(), "guest failed to respond");
    Value::decode_binary(&output).unwrap()
}

fn unwrap_ok(response: Value) -> Value {
    match response {
        Value::Array(pair) if pair.len() == 2 => {
            let mut pair = pair.into_iter();
            let tag = pair.next().unwrap();
            let payload = pair.next().unwrap();
            assert_eq!(tag, Value::String(String::from("ok")));
            payload
        }
        other => panic!("expected ok envelope, got {other:?}"),
    }
}

fn unwrap_err(response: Value) -> (String, String, Value) {
    match response {
        Value::Array(pair) if pair.len() == 2 => {
            let mut pair = pair.into_iter();
            let tag = pair.next().unwrap();
            let payload = pair.next().unwrap();
            assert_eq!(tag, Value::String(String::from("error")));
            match payload {
                Value::Object(map) => {
                    let code = match &map["code"] {
                        Value::String(code) => code.clone(),
                        other => panic!("error code not a string: {other:?}"),
                    };
                    let message = match &map["message"] {
                        Value::String(message) => message.clone(),
                        other => panic!("error message not a string: {other:?}"),
                    };
                    let details = map.get("details").cloned().unwrap_or(Value::Null);
                    (code, message, details)
                }
                other => panic!("error payload not an object: {other:?}"),
            }
        }
        other => panic!("expected error envelope, got {other:?}"),
    }
}

fn member<'a>(value: &'a Value, key: &str) -> &'a Value {
    match value {
        Value::Object(map) => &map[key],
        other => panic!("expected object for member {key}: {other:?}"),
    }
}

fn object(args: Vec<(&str, Value)>) -> Value {
    Value::Object(
        args.into_iter()
            .map(|(key, value)| (String::from(key), value))
            .collect::<BTreeMap<_, _>>(),
    )
}

#[test]
fn structs_round_trip_with_defaults_and_skips() {
    let encoded = task().encode().unwrap();
    assert!(Task::SCHEMA.is(&encoded));
    assert_eq!(Task::decode(encoded).unwrap(), task());

    let skipped = Task { cache: 7, ..task() };
    let encoded = skipped.encode().unwrap();
    assert!(!encoded.canonical_json().unwrap().contains("cache"));
    assert_eq!(Task::decode(encoded).unwrap().cache, 0);

    let without_count = object(vec![
        ("title", Value::String(String::from("t"))),
        ("owner", Value::String(String::from("o"))),
    ]);
    assert!(Task::SCHEMA.is(&without_count));
    assert_eq!(Task::decode(without_count).unwrap().count, 0);

    let unknown = object(vec![
        ("title", Value::String(String::from("t"))),
        ("owner", Value::String(String::from("o"))),
        ("bogus", Value::Null),
    ]);
    assert_eq!(Task::decode(unknown).unwrap_err().code, "INVALID_VALUE");

    let missing = object(vec![("title", Value::String(String::from("t")))]);
    assert_eq!(Task::decode(missing).unwrap_err().code, "MISSING_FIELD");
}

#[test]
fn enums_round_trip() {
    assert_eq!(
        Status::Open.encode().unwrap(),
        Value::String(String::from("Open"))
    );
    assert!(Status::SCHEMA.is(&Status::Closed.encode().unwrap()));
    assert_eq!(
        Status::decode(Value::String(String::from("Closed"))).unwrap(),
        Status::Closed
    );
    assert_eq!(
        Status::decode(Value::String(String::from("Bogus")))
            .unwrap_err()
            .code,
        "INVALID_VALUE"
    );

    let created = Event::Created(task());
    let encoded = created.encode().unwrap();
    assert!(Event::SCHEMA.is(&encoded));
    assert_eq!(Event::decode(encoded).unwrap(), created);

    let tagless = object(vec![("value", Value::Null)]);
    assert_eq!(Event::decode(tagless).unwrap_err().code, "MISSING_FIELD");
}

#[test]
fn errors_convert_both_ways() {
    let missing = BusinessError::into_failure(TaskError::Missing);
    assert_eq!(missing.code, "NOT_FOUND");
    assert_eq!(missing.details, None);
    assert!(!TaskError::Missing.is_retryable());

    let invalid = BusinessError::into_failure(TaskError::Invalid(String::from("bad")));
    assert_eq!(invalid.code, "INVALID");
    assert_eq!(invalid.details, Some(Value::String(String::from("bad"))));
    assert!(TaskError::Invalid(String::new()).is_retryable());

    assert_eq!(
        TaskError::from_failure(&invalid).unwrap(),
        TaskError::Invalid(String::from("bad"))
    );
    let foreign = flower_sdk::Failure::new("other", "x");
    assert_eq!(TaskError::from_failure(&foreign).unwrap_err(), foreign);
}

/// Declared failures with messages, and a catch-all that takes the rest.
#[derive(FlowerError, Clone, Debug, PartialEq)]
pub enum OrderError {
    #[flower(code = "OUT_OF_STOCK", message = "The goblins have run out of dough")]
    OutOfStock,
    #[flower(other)]
    Host(flower_sdk::Failure),
}

fn order(stock: u32) -> Result<u32, OrderError> {
    // `?` converts any SDK failure into the catch-all.
    let parsed: u32 = flower_sdk::FlowerType::decode(Value::Number(f64::from(stock)))?;
    if parsed == 0 {
        return Err(OrderError::OutOfStock);
    }
    Ok(parsed)
}

#[test]
fn errors_carry_messages_and_a_catch_all() {
    let failure = BusinessError::into_failure(order(0).unwrap_err());
    assert_eq!(failure.code, "OUT_OF_STOCK");
    assert_eq!(failure.message, "The goblins have run out of dough");
    assert_eq!(order(2), Ok(2));
    let host = flower_sdk::Failure::new("TYPE_MISMATCH", "expected a number");
    assert_eq!(
        OrderError::from_failure(&host).unwrap(),
        OrderError::Host(host.clone())
    );
    assert_eq!(
        BusinessError::into_failure(OrderError::from(host.clone())),
        host
    );
}

#[test]
fn methods_invoke_end_to_end() {
    let args = object(vec![("task", task().encode().unwrap())]);
    let title = unwrap_ok(invoke(InvocationKind::Query, "task_title", args.clone()));
    assert_eq!(title, Value::String(String::from("Buy milk")));

    let bumped = unwrap_ok(invoke(InvocationKind::Mutation, "bump_count", args));
    let bumped = Task::decode(bumped).unwrap();
    assert_eq!(bumped.count, 3);

    let failed = invoke(
        InvocationKind::Mutation,
        "fail_closed",
        object(vec![("reason", Value::String(String::from("nope")))]),
    );
    let (code, _, details) = unwrap_err(failed);
    assert_eq!(code, "INVALID");
    assert_eq!(details, Value::String(String::from("nope")));

    let counted = unwrap_ok(invoke(
        InvocationKind::Derived,
        "task_count",
        object(vec![(
            "tasks",
            Value::Array(vec![task().encode().unwrap(), task().encode().unwrap()]),
        )]),
    ));
    assert_eq!(counted, Value::Number(2.0));
}

#[test]
fn dispatch_rejects_bad_invocations() {
    // v1's rule: arguments the contract refuses are INVALID_ARGUMENT, with
    // the path to the offending value.
    let (code, _, details) = unwrap_err(invoke(InvocationKind::Query, "task_title", Value::Null));
    assert_eq!(code, "INVALID_ARGUMENT");
    assert_eq!(
        details,
        object(vec![("path", Value::Array(vec![]))]),
        "{details:?}"
    );
    let (code, _, details) = unwrap_err(invoke(
        InvocationKind::Query,
        "task_title",
        object(vec![(
            "task",
            object(vec![
                ("title", Value::Number(1.0)),
                ("owner", Value::String(String::from("ada"))),
            ]),
        )]),
    ));
    assert_eq!(code, "INVALID_ARGUMENT");
    assert_eq!(
        details,
        object(vec![(
            "path",
            Value::Array(vec![
                Value::String(String::from("task")),
                Value::String(String::from("title")),
            ]),
        )])
    );

    let (code, _, _) = unwrap_err(invoke(InvocationKind::Query, "missing", Value::Null));
    assert_eq!(code, "UNKNOWN_METHOD");

    let (code, _, _) = unwrap_err(invoke(
        InvocationKind::Mutation,
        "task_title",
        object(vec![("task", task().encode().unwrap())]),
    ));
    assert_eq!(code, "INVALID_INVOCATION");
}

#[test]
fn collection_statics_carry_raw_descriptors() {
    assert_eq!(Tasks::NAME, "tasks");
    assert_eq!(Tasks::raw(), &TASKS_COLLECTION);
    assert_eq!(TASKS_COLLECTION.name, "tasks");
    assert_eq!(TASKS_COLLECTION.key_schema, String::SCHEMA);
    assert_eq!(TASKS_COLLECTION.value_schema, Task::SCHEMA);
    assert_eq!(TASKS_COLLECTION.indexes.len(), 1);
    assert_eq!(TASKS_COLLECTION.indexes[0].name, "by_owner");
    assert_eq!(TASKS_COLLECTION.indexes[0].fields, &["owner"]);
    assert!(TASKS_COLLECTION.references.is_empty());

    assert_eq!(Links::NAME, "links");
    assert_eq!(
        LINKS_COLLECTION.references,
        &[
            Reference::new(&TASKS_COLLECTION, Place::Fields(&["task"]))
                .on_delete(OnDelete::Cascade)
        ]
    );
}

#[test]
fn manifest_lists_generated_definitions() {
    let output = manifest_native(&APP);
    assert!(!output.is_empty(), "guest failed to respond");
    let manifest = unwrap_ok(Value::decode_binary(&output).unwrap());

    let names: Vec<String> = match member(&manifest, "definitions") {
        Value::Array(definitions) => definitions
            .iter()
            .map(|definition| match member(definition, "name") {
                Value::String(name) => name.clone(),
                other => panic!("definition name not a string: {other:?}"),
            })
            .collect(),
        other => panic!("definitions not an array: {other:?}"),
    };
    assert_eq!(
        names,
        vec![
            String::from("task_title"),
            String::from("bump_count"),
            String::from("fail_closed"),
            String::from("task_count"),
        ]
    );

    let kinds: Vec<String> = match member(&manifest, "definitions") {
        Value::Array(definitions) => definitions
            .iter()
            .map(|definition| match member(definition, "kind") {
                Value::String(kind) => kind.clone(),
                other => panic!("definition kind not a string: {other:?}"),
            })
            .collect(),
        other => panic!("definitions not an array: {other:?}"),
    };
    assert_eq!(
        kinds,
        vec![
            String::from("query"),
            String::from("mutation"),
            String::from("mutation"),
            String::from("derived"),
        ]
    );

    let collections = match member(&manifest, "collections") {
        Value::Array(collections) => collections.clone(),
        other => panic!("collections not an array: {other:?}"),
    };
    assert_eq!(collections.len(), 2);
    let tasks = collections
        .iter()
        .find(|collection| member(collection, "name") == &Value::String(String::from("tasks")))
        .unwrap();
    let indexes = match member(tasks, "indexes") {
        Value::Array(indexes) => indexes.clone(),
        other => panic!("indexes not an array: {other:?}"),
    };
    assert_eq!(indexes.len(), 1);
    assert_eq!(
        member(&indexes[0], "name"),
        &Value::String(String::from("by_owner"))
    );
}

/// One root per task, the collection named by its marker (v1's
/// `materialize: {each: tasks}`).
#[derived(materialize = each(Tasks))]
pub fn per_task(ctx: &mut DerivedCtx<'_, '_>, id: String) -> u64 {
    let _ = (ctx, id);
    0
}

/// The same by hand: the marker's descriptor is usable in a static.
static PER_TASK_RAW: flower_sdk::Derived =
    flower_sdk::Derived::new("per_task_raw", __flower_export_per_task)
        .materialize(flower_sdk::Materialize::Each(Tasks::raw()));

#[test]
fn derived_values_materialize_per_row_of_a_marker() {
    static PARTS: [&dyn flower_sdk::Component; 3] = [&TASKS_COLLECTION, &PER_TASK, &PER_TASK_RAW];
    static PER_ROW: App = App::new(&PARTS);
    let output = manifest_native(&PER_ROW);
    let Value::Array(envelope) = Value::decode_binary(&output).unwrap() else {
        panic!("manifest envelope");
    };
    let definitions = envelope[1].as_object().unwrap()["definitions"]
        .as_array()
        .unwrap()
        .clone();
    for name in ["per_task", "per_task_raw"] {
        let definition = definitions
            .iter()
            .find(|definition| definition.as_object().unwrap()["name"] == Value::from(name))
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(definition["materialize"], Value::from("each"), "{name}");
        assert_eq!(definition["source"], Value::from("tasks"), "{name}");
    }
}
