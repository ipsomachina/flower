//! Testing a module in-process: the real engine, the module's own Rust
//! code, no Wasm build and no server.

// ANCHOR: test
use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::NativeRuntime;
use flower_engine::storage::GroupStore;
use flower_sdk::{FlowerType, Value};
use tasks::{ADD_TASK, AddTaskArgs, COMPLETE_TASK, CompleteTaskArgs, OPEN_TASKS, OpenTask};

fn open(engine: &mut Engine) -> Vec<OpenTask> {
    let args = Value::Object(Default::default());
    let open = engine
        .invoke_query(
            b"demo",
            "tasks",
            OPEN_TASKS.name,
            &args,
            &CallOptions::default(),
        )
        .unwrap();
    Vec::decode(open).unwrap()
}

#[test]
fn completing_a_task_closes_it() {
    let path = std::env::temp_dir().join(format!("tasks-{}.redb", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    let runtime = NativeRuntime::new(&tasks::APP);
    let manifest = runtime.manifest().unwrap();
    engine
        .deploy(b"demo", "tasks", [0; 32], b"", &manifest, Box::new(runtime))
        .unwrap();

    // The argument structs `#[mutation]` derived encode the calls.
    let add = AddTaskArgs {
        id: "milk".to_string(),
        title: "Buy milk".to_string(),
    }
    .encode()
    .unwrap();
    let options = MutationOptions::default();
    engine
        .invoke_mutation(b"demo", "tasks", ADD_TASK.name, &add, &options)
        .unwrap();
    let milk = OpenTask {
        id: "milk".to_string(),
        title: "Buy milk".to_string(),
    };
    assert_eq!(open(&mut engine), [milk]);

    let again = engine.invoke_mutation(b"demo", "tasks", ADD_TASK.name, &add, &options);
    assert_eq!(again.unwrap_err().code, "TASK_EXISTS");

    let done = CompleteTaskArgs {
        id: "milk".to_string(),
    }
    .encode()
    .unwrap();
    engine
        .invoke_mutation(b"demo", "tasks", COMPLETE_TASK.name, &done, &options)
        .unwrap();
    assert!(open(&mut engine).is_empty());

    drop(engine);
    let _ = std::fs::remove_file(&path);
}
// ANCHOR_END: test
