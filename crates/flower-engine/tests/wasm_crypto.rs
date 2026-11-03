//! Guest crypto through a real `wasm32` guest: the SDK's NaCl and JOSE
//! calls reach the host, and a declared catalog key signs and
//! verifies session tokens only for the module that declares it.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use flower_crypto::catalog::apply_change;
use flower_crypto::host::CryptoHost;
use flower_crypto::replicated::{ManagedKeys, Prepared, WrappingKey};
use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::wasm::WasmBackend;
use flower_engine::storage::GroupStore;
use flower_sdk::Value;
use flower_sdk::crypto::hex_encode;

fn cargo() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .or_else(|| std::env::var("CARGO").ok())
        .unwrap_or_else(|| "cargo".to_string())
}

/// Build the guest crate for `wasm32` in an isolated target dir, as
/// `wasm_guest.rs` does.
fn guest_wasm() -> Vec<u8> {
    let root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let target = root.join("target").join("wasm-guest");
    let status = Command::new(cargo())
        .args([
            "build",
            "--offline",
            "--quiet",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "flower-guest-crypto",
        ])
        .current_dir(&root)
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .unwrap();
    assert!(status.success(), "guest build failed");
    std::fs::read(
        target
            .join("wasm32-unknown-unknown")
            .join("debug")
            .join("flower_guest_crypto.wasm"),
    )
    .unwrap()
}

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn args(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

/// Generate an Ed25519 key and bind `sessions` to it, as the leader
/// would prepare and every member apply. Returns the runtime and the
/// stored row, for later changes.
fn catalog_with_sessions() -> (Arc<ManagedKeys>, Option<Vec<u8>>) {
    let keys = Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[3; 32])),
        Vec::new(),
    ));
    let mut stored: Option<Vec<u8>> = None;
    for request in [
        serde_json::json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
        serde_json::json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign","verify"]}),
        serde_json::json!({"operation":"generate","requestId":"gx","name":"boxes","algorithm":"X25519"}),
        serde_json::json!({"operation":"bind","requestId":"bx","name":"boxes","key":"boxes","usages":["derive","encrypt","decrypt"]}),
    ] {
        change(&keys, &mut stored, request);
    }
    (keys, stored)
}

/// Prepare `request` as the leader would, apply it to the stored row
/// as every member would, and install the result.
fn change(keys: &ManagedKeys, stored: &mut Option<Vec<u8>>, request: serde_json::Value) {
    let Prepared::Change(change) = keys.prepare(stored.as_deref(), &request).unwrap() else {
        panic!("changes propose");
    };
    if let Some(catalog) = apply_change(stored.as_deref(), &change).unwrap().catalog {
        *stored = Some(catalog);
    }
    keys.install(b"db", stored.as_deref()).unwrap();
}

/// An engine running the guest with `crypto` behind `crypto_call`.
fn engine_with(path: &std::path::Path, wasm: &[u8], crypto: CryptoHost) -> Engine {
    let _ = std::fs::remove_file(path);
    let mut engine = Engine::new(GroupStore::create(path).unwrap());
    let mut backend = WasmBackend::new().unwrap();
    backend.set_crypto(Arc::new(crypto));
    backend.register("app", wasm).unwrap();
    let manifest = backend.manifest("app").unwrap();
    engine
        .deploy(b"db", "app", [5u8; 32], wasm, &manifest, Box::new(backend))
        .unwrap();
    engine
}

#[test]
fn real_guest_runs_nacl_jose_and_declared_catalog_keys() {
    let wasm = guest_wasm();
    let (keys, _) = catalog_with_sessions();
    let base = CryptoHost::new().with_catalog(keys);

    // The guest's manifest carries its key declarations.
    let mut probe = WasmBackend::new().unwrap();
    probe.set_crypto(Arc::new(base.clone()));
    probe.register("app", &wasm).unwrap();
    let manifest = probe.manifest("app").unwrap();
    let Value::Object(map) = &manifest else {
        panic!("manifest is an object");
    };
    let Some(Value::Array(declared)) = map.get("keys") else {
        panic!("the guest declares keys: {manifest:?}");
    };
    assert_eq!(declared.len(), 2);

    let mut path = std::env::temp_dir();
    path.push(format!("flower-wasm-guest-crypto-{}", std::process::id()));
    let scoped = base.for_module(b"db", "app", &manifest).unwrap();
    let mut engine = engine_with(&path, &wasm, scoped);

    for _ in 0..2 {
        assert_eq!(
            engine
                .invoke_query(
                    b"db",
                    "app",
                    "shared_roundtrip",
                    &args(vec![("peer", s(&hex_encode(&[9; 32])))]),
                    &CallOptions::default()
                )
                .unwrap(),
            s(&hex_encode(b"shared"))
        );
    }

    // NaCl through the SDK matches the host's raw secretbox.
    let key = hex_encode(&[7u8; 32]);
    let nonce = hex_encode(&[9u8; 24]);
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "nacl_roundtrip",
            &args(vec![
                ("key", s(&key)),
                ("nonce", s(&nonce)),
                ("message", s("hello")),
            ]),
            &CallOptions::default(),
        )
        .unwrap();
    let raw = flower_crypto::nacl::execute(1, &[b"hello", &[9u8; 24], &[7u8; 32]]).unwrap();
    let flower_crypto::nacl::Output::Bytes(raw) = raw else {
        panic!("secretbox returns bytes");
    };
    assert_eq!(out, s(&format!("{}:hello", hex_encode(&raw))));

    // HS256 sign and verify at the invocation clock; expired tokens
    // fail as catchable crypto failures.
    let secret = hex_encode(&[1u8; 32]);
    let jwt_args = |exp: f64| {
        args(vec![
            ("secret", s(&secret)),
            ("subject", s("ada")),
            ("exp", Value::Number(exp)),
        ])
    };
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "jwt_roundtrip",
            &jwt_args(4_102_444_800.0),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("ada"));
    let expired = engine
        .invoke_query(
            b"db",
            "app",
            "jwt_roundtrip",
            &jwt_args(1.0),
            &CallOptions::default(),
        )
        .unwrap_err();
    assert_eq!(expired.code, "CRYPTO_ERROR", "{expired:?}");

    // The declared catalog key issues and checks session tokens.
    let token = engine
        .invoke_mutation(
            b"db",
            "app",
            "issue_session",
            &args(vec![
                ("subject", s("ada")),
                ("exp", Value::Number(4_102_444_800.0)),
            ]),
            &MutationOptions::default(),
        )
        .unwrap();
    let Value::String(token) = token else {
        panic!("tokens are text");
    };
    assert!(token.split('.').count() == 3);
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "check_session",
            &args(vec![("token", s(&token))]),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("ada"));
    drop(engine);

    // The same guest without its declarations resolves no catalog key.
    let mut engine = engine_with(&path, &wasm, base);
    let refused = engine
        .invoke_mutation(
            b"db",
            "app",
            "issue_session",
            &args(vec![
                ("subject", s("ada")),
                ("exp", Value::Number(4_102_444_800.0)),
            ]),
            &MutationOptions::default(),
        )
        .unwrap_err();
    assert_eq!(refused.code, "FORBIDDEN_USAGE", "{refused:?}");
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn catalog_key_results_never_answer_from_the_cache() {
    // Through the Wasm crypto import as through the native one: a key
    // change moves no revision, so the reader's query cache never keeps
    // a result that resolved a catalog key. (Signing reads no clock,
    // and token checks only declare their expiry: nothing else keeps
    // them out.)
    let wasm = guest_wasm();
    let (keys, mut stored) = catalog_with_sessions();
    let base = CryptoHost::new().with_catalog(keys.clone());
    let mut probe = WasmBackend::new().unwrap();
    probe.set_crypto(Arc::new(base.clone()));
    probe.register("app", &wasm).unwrap();
    let manifest = probe.manifest("app").unwrap();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "flower-wasm-guest-crypto-cache-{}",
        std::process::id()
    ));
    let engine = engine_with(
        &path,
        &wasm,
        base.for_module(b"db", "app", &manifest).unwrap(),
    );
    let sign = || {
        engine
            .reader()
            .invoke_query(
                b"db",
                "app",
                "sign_message",
                &args(vec![("message", s("m"))]),
                &CallOptions::default(),
            )
            .unwrap()
    };
    let before = sign();
    change(
        &keys,
        &mut stored,
        serde_json::json!({"operation":"rotate","requestId":"r","name":"signing"}),
    );
    assert_ne!(sign(), before, "a rotation signs under the new version");
    assert_eq!(engine.reader().query_cache_stats().hits, 0);
    drop(engine);
    let _ = std::fs::remove_file(&path);
}
