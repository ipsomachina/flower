//! Every App the pages show deploys: the engine checks each manifest
//! (names, HTTP aliases, access, exports, queues, keys) as a server
//! would.

use flower_engine::engine::Engine;
use flower_engine::runtime::NativeRuntime;
use flower_engine::storage::GroupStore;
use flower_sdk::{App, FlowerType, Value};

use doc_snippets::{auth, collections, crypto, derived, methods, modules, queues, time};

static COLLECTIONS: App = App::new(&[
    &collections::Projects,
    &collections::Tasks,
    &collections::RENAME_TASK,
    &collections::DUE_BETWEEN,
    &collections::PROJECT_NAMES,
]);

static TIME: App = App::new(&[&time::Coupons, &time::DISCOUNT, &time::SECONDS_LEFT]);

fn http_aliases(app: &'static App) -> Vec<String> {
    let bytes = flower_sdk::abi::manifest_native(app);
    let Value::Array(envelope) = Value::decode_binary(&bytes).unwrap() else {
        panic!("manifest envelope");
    };
    let Some(Value::Object(manifest)) = envelope.get(1) else {
        panic!("manifests are objects");
    };
    let Some(Value::Array(http)) = manifest.get("http") else {
        return Vec::new();
    };
    let mut aliases: Vec<String> = http
        .iter()
        .map(|entry| match entry.as_object().unwrap().get("alias") {
            Some(Value::String(alias)) => alias.clone(),
            other => panic!("alias not a string: {other:?}"),
        })
        .collect();
    aliases.sort();
    aliases
}

#[test]
fn every_app_deploys() {
    let path = std::env::temp_dir().join(format!("doc-snippets-{}.redb", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    let apps: [(&str, &'static App); 10] = [
        ("sql", &doc_snippets::sql::shop::APP),
        ("collections", &COLLECTIONS),
        ("methods", &methods::APP),
        ("derived", &derived::APP),
        ("time", &TIME),
        ("queues", &queues::APP),
        ("auth", &auth::APP),
        ("crypto", &crypto::APP),
        ("inventory", &modules::PROVIDER_APP),
        ("prices", &PRICES),
    ];
    for (module, app) in apps {
        let runtime = NativeRuntime::new(app);
        let manifest = runtime.manifest().unwrap();
        engine
            .deploy(b"docs", module, [0; 32], b"", &manifest, Box::new(runtime))
            .unwrap_or_else(|failure| panic!("{module}: {failure:?}"));
        if module == "sql" {
            use doc_snippets::sql::shop::{Order, PutOrderArgs, SqlArgs};
            use flower_engine::engine::{CallOptions, MutationOptions};
            let put = PutOrderArgs {
                id: "one".into(),
                order: Order {
                    owner: "ada".into(),
                    total: flower_sdk::typed::DecimalI64(9_007_199_254_740_993),
                },
            }
            .encode()
            .unwrap();
            engine
                .invoke_mutation(
                    b"docs",
                    module,
                    "put_order",
                    &put,
                    &MutationOptions::default(),
                )
                .unwrap();
            let query = SqlArgs {
                statement: "SELECT _key AS id, total FROM orders WHERE owner = :owner ORDER BY total DESC LIMIT :limit".into(),
                params: Value::Object(std::collections::BTreeMap::from([
                    ("owner".into(), Value::from("ada")),
                    ("limit".into(), Value::Number(10.0)),
                ])),
            }.encode().unwrap();
            let result = engine
                .invoke_query(b"docs", module, "sql", &query, &CallOptions::default())
                .unwrap();
            assert_eq!(
                result.canonical_json().unwrap(),
                r#"[{"id":"one","total":"9007199254740993"}]"#
            );
        }
    }
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

static PRICES: App = App::new(&[&modules::Prices, &modules::PRICE]);

#[test]
fn only_what_says_http_answers_over_http() {
    assert_eq!(
        http_aliases(&queues::APP),
        ["convert", "invitation_sent", "invite", "start_trial"]
    );
    assert_eq!(
        http_aliases(&methods::APP),
        ["balance", "balance_nearby", "greet", "transfer"]
    );
    // Derived values are read by queries, never called over HTTP.
    assert_eq!(
        http_aliases(&derived::APP),
        ["customer_summary", "put_order", "totals"]
    );
    assert!(http_aliases(&modules::PROVIDER_APP).is_empty());
    assert_eq!(http_aliases(&modules::CONSUMER_APP), ["checkout"]);
}
