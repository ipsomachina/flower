//! The pooling allocator and the image cache. Both are process-wide, so
//! these tests run in a process of their own, against an engine with
//! four instance slots and a one-byte image cache, one at a time.

mod support;

use std::sync::{Mutex, MutexGuard, PoisonError};

use flower_engine::runtime::wasm::{self, EngineSettings, Limits, WasmBackend};
use flower_engine::runtime::{CallKind, HostCall, Runtime};
use flower_sdk::{CallFailure, Value};
use support::{ALLOC_AT, Code, Guest, NoHost};

const SLOTS: u32 = 4;

fn settings() -> EngineSettings {
    EngineSettings {
        pool_slots: SLOTS,
        // Every image is over the budget: the cache holds the latest.
        image_cache_bytes: 1,
        ..EngineSettings::default()
    }
}

/// Configure the engine and run one test at a time: slots and cache
/// counts are shared by the whole process.
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    let guard = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
    wasm::configure(settings()).unwrap();
    guard
}

/// A guest answering ok; `variant` only changes its bytes.
fn plain(variant: i32) -> Vec<u8> {
    Guest::new(Code::new().i32(variant).raw(&[0x1a]).ok()).encode()
}

/// A guest whose every call makes one host call before answering ok.
fn calling() -> Vec<u8> {
    Guest::new(
        Code::new()
            .i32(7)
            .i32(ALLOC_AT as i32)
            .i32(0)
            .raw(&[0x10, 0x00, 0x1a]) // call host_call; drop
            .ok(),
    )
    .encode()
}

fn backend(wasm: &[u8]) -> WasmBackend {
    let mut backend = WasmBackend::with_limits(Limits::default()).unwrap();
    backend.register("raw", wasm).unwrap();
    backend
}

fn call(backend: &WasmBackend) -> Result<Value, CallFailure> {
    backend.call(CallKind::Query, "raw", "any", &Value::Null, &mut NoHost)
}

/// Serves every host call by calling the guest again: each level holds
/// an instance until the innermost returns.
struct Recurse<'a> {
    backend: &'a WasmBackend,
}

impl HostCall for Recurse<'_> {
    fn op(&mut self, _op: u8, _input: &[u8]) -> Vec<u8> {
        let backend = self.backend;
        let envelope = match backend.call(CallKind::Query, "raw", "any", &Value::Null, self) {
            Ok(value) => flower_sdk::abi::encode_ok(value),
            Err(failure) => flower_sdk::abi::encode_err(&failure.failure),
        };
        envelope.encode_binary().unwrap()
    }
}

#[test]
fn instances_live_in_recycled_pool_slots() {
    let _serial = serial();
    let backend = backend(&plain(1));
    let stats = wasm::stats().unwrap();
    assert!(stats.pooling, "{stats:?}");
    // Far more calls than slots: each released slot serves the next.
    for _ in 0..50 {
        assert_eq!(call(&backend), Ok(Value::Null));
    }
    let stats = wasm::stats().unwrap();
    assert_eq!(stats.instances, 0, "{stats:?}");
    assert!((1..=SLOTS).contains(&stats.warm_slots), "{stats:?}");
}

#[test]
fn a_full_pool_fails_the_call_cleanly() {
    let _serial = serial();
    let backend = backend(&calling());
    // Each nesting level holds a slot: the fifth finds none.
    let failure = backend
        .call(
            CallKind::Query,
            "raw",
            "any",
            &Value::Null,
            &mut Recurse { backend: &backend },
        )
        .unwrap_err();
    assert_eq!(failure.failure.code, wasm::INSTANCES_EXHAUSTED);
    assert!(!failure.recovered);
    // Every slot came back.
    assert_eq!(wasm::stats().unwrap().instances, 0);
    assert_eq!(call(&backend), Ok(Value::Null));
}

#[test]
fn the_image_cache_evicts_and_prepares_again() {
    let _serial = serial();
    let first = backend(&plain(2));
    let before = wasm::stats().unwrap().images;
    // Another module's image pushes the first one out.
    let second = backend(&plain(3));
    let after = wasm::stats().unwrap().images;
    assert_eq!(after.images, 1, "{after:?}");
    assert_eq!(after.evicted, before.evicted + 1, "{after:?}");
    assert_eq!(after.prepared, before.prepared + 1, "{after:?}");
    // The evicted module prepares again on its next call, which evicts
    // the second; registering the same bytes again is a cache hit.
    assert_eq!(call(&first), Ok(Value::Null));
    let again = backend(&plain(2));
    assert!(again.shares_image(&first, "raw"));
    assert!(!second.shares_image(&first, "raw"));
    let last = wasm::stats().unwrap().images;
    assert_eq!(last.prepared, after.prepared + 1, "{last:?}");
    assert_eq!(last.images, 1, "{last:?}");
    assert_eq!(call(&second), Ok(Value::Null));
}

#[test]
fn images_outlive_the_backends_that_prepared_them() {
    let _serial = serial();
    // Deploy validation registers a module in a backend it then drops;
    // registering the deployment when it applies finds the image.
    let before = {
        let _validated = backend(&plain(5));
        let _latest = backend(&plain(6));
        wasm::stats().unwrap().images.prepared
    };
    let _applied = backend(&plain(6));
    assert_eq!(wasm::stats().unwrap().images.prepared, before);
}

#[test]
fn settings_hold_for_the_engine_life() {
    let _serial = serial();
    let _backend = backend(&plain(4));
    let other = EngineSettings {
        pool_slots: SLOTS + 1,
        ..settings()
    };
    assert!(wasm::configure(other).is_err());
    assert!(wasm::configure(settings()).is_ok());
}
