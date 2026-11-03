use flower_engine::{
    engine::{CallOptions, Engine, MutationOptions},
    json::parse_value,
    runtime::wasm::WasmBackend,
    storage::GroupStore,
};
use flower_sdk::Value;

#[test]
fn typed_sql_example_executes_through_real_wasm() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let target = root.join("target/sql-wasm-tests");
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "build",
            "--offline",
            "--quiet",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "sql-queries",
        ])
        .current_dir(root)
        .env("CARGO_TARGET_DIR", &target)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let wasm = std::fs::read(target.join("wasm32-unknown-unknown/debug/sql_queries.wasm")).unwrap();
    let mut runtime = WasmBackend::new().unwrap();
    runtime.register("sql", &wasm).unwrap();
    let manifest = runtime.manifest("sql").unwrap();
    let module = flower_engine::descriptors::load_module(&manifest).unwrap();
    assert_eq!(module.sql.len(), 1);
    assert_eq!(module.sql_endpoints.len(), 1);
    assert!(module.http.contains_key("sql"));
    assert_eq!(
        module.collections[0]
            .value_schema
            .to_value()
            .unwrap()
            .as_object()
            .unwrap()["type"],
        Value::from("object")
    );
    let path = std::env::temp_dir().join(format!("flower-sql-wasm-{}", std::process::id()));
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    engine
        .deploy(b"db", "sql", [92; 32], &wasm, &manifest, Box::new(runtime))
        .unwrap();
    for (id, total) in [
        ("a", "9007199254740992"),
        ("b", "9007199254740993"),
        ("c", "10"),
    ] {
        let args = parse_value(
            format!(
                r#"{{"id":"{id}","order":{{"owner":"ada","status":"open","total":"{total}"}}}}"#
            )
            .as_bytes(),
        )
        .unwrap();
        engine
            .invoke_mutation(
                b"db",
                "sql",
                "put_order",
                &args,
                &MutationOptions::default(),
            )
            .unwrap();
    }
    let args = parse_value(br#"{"owner":"ada","min_total":"9007199254740992","limit":1}"#).unwrap();
    let result = engine
        .invoke_query(b"db", "sql", "open_orders", &args, &CallOptions::default())
        .unwrap();
    assert_eq!(
        result,
        parse_value(br#"[{"id":"b","total":"9007199254740993"}]"#).unwrap()
    );
    let dynamic = parse_value(br#"{"statement":"SELECT _key AS id, total FROM orders WHERE owner = :owner AND total >= :min ORDER BY total DESC LIMIT 1","params":{"owner":"ada","min":"9007199254740992"}}"#).unwrap();
    let external = CallOptions {
        external: true,
        ..Default::default()
    };
    let result = engine
        .invoke_query(b"db", "sql", "sql", &dynamic, &external)
        .unwrap();
    assert_eq!(
        result,
        parse_value(br#"[{"id":"b","total":"9007199254740993"}]"#).unwrap()
    );
    let other_projection = parse_value(br#"{"statement":"SELECT status AS state FROM orders WHERE _key = :id LIMIT 1","params":{"id":"a"}}"#).unwrap();
    assert_eq!(
        engine
            .invoke_query(b"db", "sql", "sql", &other_projection, &external)
            .unwrap(),
        parse_value(br#"[{"state":"open"}]"#).unwrap()
    );
    let explained = engine
        .invoke_query(b"db", "sql", "sql_explain", &other_projection, &external)
        .unwrap();
    assert_eq!(
        explained.as_object().unwrap()["access"],
        Value::from("primary_key_lookup")
    );
    let explain = engine
        .invoke_query(
            b"db",
            "sql",
            "explain_open_orders",
            &Value::Object(Default::default()),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(
        explain.as_object().unwrap()["access"],
        Value::from("index_equality")
    );
    assert_eq!(
        explain.as_object().unwrap()["index"],
        Value::from("by_owner_status")
    );
    drop(engine);
    std::fs::remove_file(path).unwrap();
}
