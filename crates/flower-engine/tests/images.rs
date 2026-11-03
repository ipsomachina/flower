//! Initialized images and call isolation.
//!
//! A hand-assembled guest drives these tests adversarially. Its
//! `flower_init` sets an exported and two hidden mutable globals, writes
//! memory and grows it by a page; an optional start function counts its
//! own runs in memory. Every call first checks that it sees exactly the
//! initialized state — globals, bytes, memory size, nothing a previous
//! call wrote — and answers a distinct failure code otherwise. Then it
//! overwrites all of it, grows memory again, and ends by kind: queries
//! return, mutations trap, derived calls spin and planners grow past
//! the memory limit. Whatever ran before, the next call must find the
//! image untouched.

mod support;

use std::time::Duration;

use flower_engine::runtime::wasm::{self, Limits, WasmBackend};
use flower_engine::runtime::{CallKind, Runtime};
use flower_sdk::{CallFailure, Value};
use support::{Code, F64, Func, Guest, I32, I64, NoHost, PACKED_TYPE};

/// Where the start function counts its runs.
const START_AT: u32 = 300;
/// Where `flower_init` leaves a byte.
const INIT_AT: u32 = 301;
/// Where `flower_init` leaves a byte in the page it grows.
const GROWN_AT: u32 = 64 * 1024 + 100;
/// Where calls leave a byte.
const DIRTY_AT: u32 = 40_000;
/// The hidden i64 global's initialized value.
const HIDDEN: i64 = 0x1234_5678_9abc;

/// The adversarial guest. `start` adds a start function; `init_burn`
/// makes `flower_init` spin that many iterations first.
fn stateful(start: bool, init_burn: i32) -> Vec<u8> {
    let invoke = Code::new()
        // Initialized state, exactly.
        .global_get(0)
        .i32(7)
        .raw(&[0x47]) // i32.ne
        .fail_if("LEAK_COUNTER")
        .global_get(1)
        .i64(HIDDEN)
        .raw(&[0x52]) // i64.ne
        .fail_if("LEAK_HIDDEN")
        .global_get(2)
        .i32(5)
        .raw(&[0x47])
        .fail_if("LEAK_CONSTANT")
        .global_get(3)
        .f64(2.5)
        .raw(&[0x62]) // f64.ne
        .fail_if("LEAK_FLOAT")
        .load8(INIT_AT)
        .i32(0x42)
        .raw(&[0x47])
        .fail_if("LEAK_INIT")
        .load8(START_AT)
        .i32(i32::from(start))
        .raw(&[0x47])
        .fail_if("LEAK_START")
        .load8(GROWN_AT)
        .i32(0x99)
        .raw(&[0x47])
        .fail_if("LEAK_GROWN")
        .raw(&[0x3f, 0x00]) // memory.size
        .i32(2)
        .raw(&[0x47])
        .fail_if("LEAK_SIZE")
        .load8(DIRTY_AT)
        .fail_if("LEAK_DIRTY")
        // Then overwrite all of it.
        .i32(1000)
        .global_set(0)
        .i64(0)
        .global_set(1)
        .f64(0.0)
        .global_set(3)
        .store8(INIT_AT, 0)
        .store8(START_AT, 77)
        .store8(GROWN_AT, 0)
        .store8(DIRTY_AT, 0xab)
        .grow(1)
        // Mutations trap.
        .local_get(0)
        .i32(2)
        .raw(&[0x46]) // i32.eq
        .when(Code::new().raw(&[0x00]))
        // Planners grow past any instance limit.
        .local_get(0)
        .i32(3)
        .raw(&[0x46])
        .when(Code::new().grow(32 * 1024))
        // Derived calls spin.
        .local_get(0)
        .i32(4)
        .raw(&[0x46])
        .when(Code::new().raw(&[0x03, 0x40, 0x0c, 0x00, 0x0b]))
        .ok();
    let mut init = Code::new();
    if init_burn > 0 {
        // local 0 counts down to zero.
        init = init
            .i32(init_burn)
            .raw(&[0x21, 0x00]) // local.set 0
            .raw(&[0x03, 0x40]) // loop
            .local_get(0)
            .i32(1)
            .raw(&[0x6b]) // i32.sub
            .raw(&[0x22, 0x00]) // local.tee 0
            .raw(&[0x0d, 0x00]) // br_if 0
            .raw(&[0x0b]);
    }
    let init = init
        .i32(7)
        .global_set(0)
        .i64(HIDDEN)
        .global_set(1)
        .f64(2.5)
        .global_set(3)
        .store8(INIT_AT, 0x42)
        .grow(1)
        .store8(GROWN_AT, 0x99)
        .ok();
    let mut guest = Guest::new(invoke);
    guest.init = Some(Func {
        ty: PACKED_TYPE,
        locals: 1,
        body: init,
    });
    if start {
        // Count runs: an image that ran its start again would see 2.
        guest.start = Some(
            Code::new()
                .i32(START_AT as i32)
                .load8(START_AT)
                .i32(1)
                .raw(&[
                    0x6a, // i32.add
                    0x3a, 0x00, 0x00, // i32.store8
                ]),
        );
    }
    guest.globals = vec![
        (I32, true, Code::new().i32(0)),
        (I64, true, Code::new().i64(0)),
        (I32, false, Code::new().i32(5)),
        (F64, true, Code::new().f64(0.0)),
    ];
    // Global 0 is exported; 1 and 3 are hidden.
    guest.exports = vec![("counter".to_string(), 3, 0)];
    guest.encode()
}

fn backend(limits: Limits, wasm: &[u8]) -> WasmBackend {
    let mut backend = WasmBackend::with_limits(limits).unwrap();
    backend.register("raw", wasm).unwrap();
    backend
}

fn call(backend: &WasmBackend, kind: CallKind) -> Result<Value, CallFailure> {
    backend.call(kind, "raw", "any", &Value::Null, &mut NoHost)
}

/// Each call must find the initialized image untouched.
fn assert_pristine(backend: &WasmBackend) {
    assert_eq!(call(backend, CallKind::Query), Ok(Value::Null));
}

#[test]
fn calls_start_from_the_image_and_leave_nothing_behind() {
    for start in [false, true] {
        let backend = backend(Limits::default(), &stateful(start, 0));
        // Each query checks the image, then overwrites it.
        for _ in 0..4 {
            assert_pristine(&backend);
        }
        assert!(backend.manifest("raw").is_ok());
        assert_pristine(&backend);
    }
}

#[test]
fn traps_and_limit_failures_leave_nothing_behind() {
    let wasm = stateful(true, 0);
    let roomy = backend(Limits::default(), &wasm);
    let failure = call(&roomy, CallKind::Mutation).unwrap_err();
    assert_eq!(failure.failure.code, wasm::HOST_TRAP);
    assert!(!failure.recovered);
    assert_pristine(&roomy);

    let failure = roomy.plan("raw", "any", &Value::Null).unwrap_err();
    assert_eq!(failure.failure.code, wasm::OUT_OF_MEMORY);
    assert_pristine(&roomy);

    let fuel = backend(
        Limits {
            fuel: 1_000_000,
            timeout: Duration::from_secs(600),
            ..Limits::default()
        },
        &wasm,
    );
    let failure = call(&fuel, CallKind::Derived).unwrap_err();
    assert_eq!(failure.failure.code, wasm::FUEL_EXHAUSTED);
    assert_pristine(&fuel);

    let deadline = backend(
        Limits {
            fuel: u64::MAX,
            timeout: Duration::from_millis(50),
            ..Limits::default()
        },
        &wasm,
    );
    let failure = call(&deadline, CallKind::Derived).unwrap_err();
    assert_eq!(failure.failure.code, wasm::DEADLINE_EXCEEDED);
    assert_pristine(&deadline);
}

#[test]
fn databases_share_the_image_but_never_an_instance() {
    // Two databases deploying the same bytes: two registrations.
    let wasm = stateful(false, 0);
    let first = backend(Limits::default(), &wasm);
    let second = backend(Limits::default(), &wasm);
    assert!(first.shares_image(&second, "raw"));
    let threads = (0..4)
        .map(|thread| {
            let backends = [first.clone(), second.clone()];
            std::thread::spawn(move || {
                for round in 0..25 {
                    let backend = &backends[(thread + round) % 2];
                    let kind = if round % 3 == 0 {
                        CallKind::Mutation
                    } else {
                        CallKind::Query
                    };
                    match call(backend, kind) {
                        Ok(value) => assert_eq!(value, Value::Null),
                        // Mutations trap after writing; nothing else may fail.
                        Err(failure) => {
                            assert_eq!(kind, CallKind::Mutation);
                            assert_eq!(failure.failure.code, wasm::HOST_TRAP);
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_pristine(&first);
    assert_pristine(&second);
}

#[test]
fn init_runs_once_per_image_not_per_call() {
    // A `flower_init` burning a few million instructions: once at
    // registration, never on a call.
    let wasm = stateful(false, 2_000_000);
    let mut backend = backend(Limits::default(), &wasm);
    backend.set_limits(Limits {
        fuel: 100_000,
        ..Limits::default()
    });
    for _ in 0..3 {
        assert_pristine(&backend);
    }
    // Registering under less fuel than `flower_init` needs fails the
    // same way whether the image is cached ...
    let mut tight = WasmBackend::with_limits(Limits {
        fuel: 100_000,
        ..Limits::default()
    })
    .unwrap();
    let failure = tight.register("raw", &wasm).unwrap_err();
    assert_eq!(failure.code, wasm::FUEL_EXHAUSTED);
    // ... or prepared afresh.
    let failure = tight
        .register("raw", &stateful(false, 2_000_001))
        .unwrap_err();
    assert_eq!(failure.code, wasm::FUEL_EXHAUSTED);
}

#[test]
fn modules_with_state_an_image_cannot_hold_are_refused() {
    // A valid guest that grows its table: tables are not in the image.
    let mut grows = Guest::new(
        Code::new()
            .raw(&[0xd0, 0x70]) // ref.null func
            .i32(1)
            .raw(&[0xfc, 0x0f, 0x00]) // table.grow 0
            .raw(&[0x1a]) // drop
            .ok(),
    );
    grows.table = true;
    let failure = WasmBackend::new()
        .unwrap()
        .register("raw", &grows.encode())
        .unwrap_err();
    assert_eq!(failure.code, wasm::HOST_TRAP);
    assert!(failure.message.contains("TableGrow"), "{failure:?}");
    // The same guest without the instruction registers.
    let mut plain = Guest::new(Code::new().ok());
    plain.table = true;
    assert!(
        WasmBackend::new()
            .unwrap()
            .register("raw", &plain.encode())
            .is_ok()
    );
}
