//! In-process measurements of kept authorization decisions and owner
//! bucket scans against what they replace: a reader query whose module
//! verifies an HS256 bearer token with `JwtBearer`, with the decision
//! memo on and off; and a plain scan of one owner's rows under an
//! owner-pinned read rule, through the owner's index bucket and through
//! the whole collection. Ignored by default: run them in release with
//!
//! ```text
//! cargo test --release -p flower-engine --test auth_policy_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! `FLOWER_BENCH_PASSES` (interleaved passes, default 5) tunes them; each
//! reports the median pass.

extern crate alloc;

mod fixtures;

use fixtures::{hc, s};

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use flower_crypto::host::CryptoHost;
use flower_crypto::jwt::{Algorithm, KeyFormat, SignOptions};
use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, NativeRuntime};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Principal, Value, fail};

mod app {
    use flower_sdk::auth::{Access, Authentication, Authorization, BearerKey, JwtBearer};
    use flower_sdk::crypto::jwt::{Algorithm, KeyFormat};
    use flower_sdk::{App, Component, MethodCtx, QueryCtx, Value, query};

    pub const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    static BEARER: JwtBearer = JwtBearer {
        audience: &["api"],
        ..JwtBearer::new(BearerKey::Bytes(
            SECRET,
            KeyFormat::Raw,
            &[Algorithm::Hs256],
        ))
    };

    static AUTH: Authorization = Authorization {
        authenticate: Authentication::Typed(&BEARER),
        default: Access::Authenticated,
        sessions: Access::Authenticated,
        methods: &[],
    };

    /// Who the call runs as.
    #[query]
    pub fn whoami(ctx: &mut QueryCtx<'_, '_>, id: f64) -> flower_sdk::Result<Value> {
        let _ = id;
        Ok(ctx
            .principal()?
            .map_or(Value::Null, |principal| Value::String(principal.subject)))
    }

    static DEFS: [&dyn flower_sdk::Component; 1] = [&WHOAMI];
    static USES: [&dyn Component; 1] = [&AUTH];

    pub static APP: App = App::new(&[&USES, &DEFS]);
}

fn passes() -> usize {
    std::env::var("FLOWER_BENCH_PASSES")
        .ok()
        .and_then(|passes| passes.parse().ok())
        .unwrap_or(5)
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn temp(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-bench-{name}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

#[test]
#[ignore = "a measurement: run in release on a quiet machine"]
fn kept_decisions_against_running_the_hook() {
    let path = temp("decisions");
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    let manifest = NativeRuntime::new(&app::APP).manifest().unwrap();
    let runtime = NativeRuntime::new(&app::APP).with_crypto(Arc::new(CryptoHost::new()));
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, Box::new(runtime))
        .unwrap();
    let claims = serde_json::json!({"sub": "ada", "aud": "api", "exp": 4_000_000_000u64});
    let options = SignOptions {
        algorithm: Algorithm::HS256,
        key_format: KeyFormat::Raw,
        kid: None,
        typ: None,
    };
    let token = flower_crypto::jwt::sign(&claims, app::SECRET, &options).unwrap();
    let call = CallOptions {
        credentials: Some(Value::String(format!("Bearer {token}"))),
        ..CallOptions::default()
    };
    let reader = engine.reader();
    let calls = 20_000;
    let run = |memo: bool| {
        reader.set_decision_memo_bytes(if memo { 4 << 20 } else { 0 });
        let started = Instant::now();
        for id in 0..calls {
            let args = Value::Object([("id".to_string(), Value::Number(id as f64))].into());
            black_box(
                reader
                    .invoke_query(b"db", "app", "whoami", &args, &call)
                    .unwrap(),
            );
        }
        started.elapsed().as_secs_f64() * 1e6 / calls as f64
    };
    let (mut kept, mut evaluated) = (Vec::new(), Vec::new());
    for _ in 0..passes() {
        evaluated.push(run(false));
        kept.push(run(true));
    }
    let (kept, evaluated) = (median(kept), median(evaluated));
    println!(
        "bearer-token query, {calls} calls with distinct arguments: hook every call {evaluated:.1} us/call, kept decision {kept:.1} us/call ({:.1}x)",
        evaluated / kept
    );
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

fn scans(
    kind: CallKind,
    _module: &str,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    match (kind, name) {
        // `owners` owners' rows, `rows` in all, into both collections.
        (CallKind::Mutation, "seed") => {
            let rows = field(args, "rows").as_f64().unwrap() as usize;
            let owners = field(args, "owners").as_f64().unwrap() as usize;
            for row in 0..rows {
                let value = Value::Object(
                    [
                        ("owner".to_string(), s(&format!("o{}", row % owners))),
                        ("title".to_string(), s(&format!("row {row}"))),
                    ]
                    .into(),
                );
                for coll in ["plain", "indexed"] {
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s(coll), s(&format!("\"r{row:06}\"")), value.clone()],
                    )?;
                }
            }
            Ok(Value::Null)
        }
        (CallKind::Query, "scan") => {
            let page = hc(
                host,
                HostOp::Scan,
                vec![
                    s(""),
                    field(args, "coll").clone(),
                    Value::Number(1000.0),
                    Value::Null,
                ],
            )?;
            Ok(Value::Number(
                field(&page, "rows").as_array().unwrap().len() as f64,
            ))
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    }
}

fn scan_manifest() -> Value {
    let owned = flower_engine::json::parse_value(
        br#"{"read": {"eq": [{"ref": ["row", "owner"]}, {"ref": ["principal", "subject"]}]},
             "insert": {"const": true}}"#,
    )
    .unwrap();
    let collection = |name: &str, indexes: Vec<Value>| {
        Value::Object(
            [
                ("name".to_string(), s(name)),
                (
                    "key".to_string(),
                    Value::Object([("type".to_string(), s("string"))].into()),
                ),
                (
                    "value".to_string(),
                    Value::Object([("type".to_string(), s("json"))].into()),
                ),
                ("indexes".to_string(), Value::Array(indexes)),
                ("references".to_string(), Value::Array(vec![])),
                ("access".to_string(), owned.clone()),
            ]
            .into(),
        )
    };
    let by_owner = Value::Object(
        [
            ("name".to_string(), s("by_owner")),
            ("fields".to_string(), Value::Array(vec![s("owner")])),
        ]
        .into(),
    );
    let definition = |name: &str, kind: &str| {
        Value::Object(
            [
                ("name".to_string(), s(name)),
                ("kind".to_string(), s(kind)),
                (
                    "args".to_string(),
                    Value::Object([("type".to_string(), s("json"))].into()),
                ),
                // A measurement's long runs declare the longest deadline,
                // and the fuel that comes with it.
                ("timeout_ms".to_string(), Value::Number(86_400_000.0)),
            ]
            .into(),
        )
    };
    Value::Object(
        [
            (
                "collections".to_string(),
                Value::Array(vec![
                    collection("plain", vec![]),
                    collection("indexed", vec![by_owner]),
                ]),
            ),
            (
                "definitions".to_string(),
                Value::Array(vec![
                    definition("seed", "mutation"),
                    definition("scan", "query"),
                ]),
            ),
            ("triggers".to_string(), Value::Array(vec![])),
            ("http".to_string(), Value::Array(vec![])),
        ]
        .into(),
    )
}

#[test]
#[ignore = "a measurement: run in release on a quiet machine"]
fn owner_bucket_scans_against_full_scans() {
    let path = temp("buckets");
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            b"",
            &scan_manifest(),
            Box::new(ClosureRuntime::new(scans)),
        )
        .unwrap();
    let (rows, owners) = (20_000, 200);
    let seed = Value::Object(
        [
            ("rows".to_string(), Value::Number(rows as f64)),
            ("owners".to_string(), Value::Number(owners as f64)),
        ]
        .into(),
    );
    let options = MutationOptions::default();
    engine
        .invoke_mutation(b"db", "app", "seed", &seed, &options)
        .unwrap();
    let owner = CallOptions {
        principal: Some(Principal::new("o7".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    let scans = 50;
    let run = |coll: &str| {
        let args = Value::Object([("coll".to_string(), s(coll))].into());
        let started = Instant::now();
        for _ in 0..scans {
            // The writer lane evaluates every query: no cache answers.
            let found = engine
                .invoke_query(b"db", "app", "scan", &args, &owner)
                .unwrap();
            assert_eq!(found, Value::Number((rows / owners) as f64));
        }
        started.elapsed().as_secs_f64() * 1e6 / scans as f64
    };
    let (mut bucket, mut full) = (Vec::new(), Vec::new());
    for _ in 0..passes() {
        full.push(run("plain"));
        bucket.push(run("indexed"));
    }
    let (bucket, full) = (median(bucket), median(full));
    println!(
        "owner scan of {} rows among {rows}: full scan {full:.0} us, bucket scan {bucket:.0} us ({:.1}x)",
        rows / owners,
        full / bucket
    );
    drop(engine);
    let _ = std::fs::remove_file(&path);
}
