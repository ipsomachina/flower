//! Guest runtime limits: memory, wall-clock deadlines, Wasm stack,
//! nesting, `flower_init`, deploy-time ABI validation, entropy gating
//! and isolation between calls.
//!
//! Two kinds of guest drive these tests. Hand-assembled modules pin
//! the ABI edge cases exactly (a missing export, a failing init, an
//! endless loop, unbounded recursion, a memory grow) without building
//! anything. The real SDK guest (`flower-guest-test` with its `limits`
//! feature) proves the same limits hold for ordinary Rust guests.
//! Deadline tests never assert elapsed time: they run endless loops
//! under unlimited fuel, so only the deadline can end them.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use flower_engine::runtime::wasm::{self, Limits, WasmBackend};
use flower_engine::runtime::{CallKind, HostCall, Runtime};
use flower_sdk::{CallFailure, Value, fail};

// ---------------------------------------------------------------------
// Hand-assembled modules
// ---------------------------------------------------------------------

fn uleb(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn sleb(mut value: i64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
        out.push(if done { byte } else { byte | 0x80 });
        if done {
            return;
        }
    }
}

fn name(text: &str, out: &mut Vec<u8>) {
    uleb(text.len() as u64, out);
    out.extend_from_slice(text.as_bytes());
}

fn section(id: u8, body: Vec<u8>, out: &mut Vec<u8>) {
    out.push(id);
    uleb(body.len() as u64, out);
    out.extend(body);
}

/// `i64.const value`.
fn i64_const(value: u64) -> Vec<u8> {
    let mut out = vec![0x42];
    sleb(value as i64, &mut out);
    out
}

/// Where the ok envelope lives in a raw module's memory.
const OK_AT: u32 = 1024;
/// Where the error envelope lives in a raw module's memory.
const ERR_AT: u32 = 8192;

fn ok_envelope() -> Vec<u8> {
    flower_sdk::abi::encode_ok(Value::Null)
        .encode_binary()
        .unwrap()
}

fn err_envelope() -> Vec<u8> {
    flower_sdk::abi::encode_err(&fail("BROKEN_CONFIG", "init refused"))
        .encode_binary()
        .unwrap()
}

fn packed(at: u32, bytes: &[u8]) -> u64 {
    (u64::from(at) << 32) | bytes.len() as u64
}

/// `return` the packed ok envelope.
fn returns_ok() -> Vec<u8> {
    i64_const(packed(OK_AT, &ok_envelope()))
}

/// `return` the packed error envelope.
fn returns_err() -> Vec<u8> {
    i64_const(packed(ERR_AT, &err_envelope()))
}

/// `loop br 0 end unreachable`: spins forever.
fn spins() -> Vec<u8> {
    vec![0x03, 0x40, 0x0c, 0x00, 0x0b, 0x00]
}

/// A raw guest: imports `flower.host_call`, exports the ABI. Bodies
/// are instruction bytes without the trailing `end`.
struct Raw {
    memory: bool,
    manifest: bool,
    alloc: Vec<u8>,
    invoke: Vec<u8>,
    init: Option<Vec<u8>>,
}

impl Raw {
    fn new(invoke: Vec<u8>) -> Self {
        Raw {
            memory: true,
            manifest: true,
            alloc: vec![0x41, 0x80, 0x80, 0x01], // i32.const 16384
            invoke,
            init: None,
        }
    }

    fn encode(&self) -> Vec<u8> {
        const I32: u8 = 0x7f;
        const I64: u8 = 0x7e;
        let mut out = b"\0asm\x01\0\0\0".to_vec();
        // Types: 0 = (i32 i32 i32) -> i64, 1 = (i32) -> i32, 2 = () -> i64.
        let mut types = vec![3];
        types.extend([0x60, 3, I32, I32, I32, 1, I64]);
        types.extend([0x60, 1, I32, 1, I32]);
        types.extend([0x60, 0, 1, I64]);
        section(1, types, &mut out);
        // Import function 0: flower.host_call.
        let mut imports = vec![1];
        name("flower", &mut imports);
        name("host_call", &mut imports);
        imports.extend([0x00, 0]);
        section(2, imports, &mut out);
        // Functions 1 alloc, 2 invoke, 3 manifest, 4 init.
        let mut bodies: Vec<(u8, Vec<u8>)> = vec![
            (1, self.alloc.clone()),
            (0, self.invoke.clone()),
            (2, returns_ok()),
        ];
        if let Some(init) = &self.init {
            bodies.push((2, init.clone()));
        }
        let mut functions = Vec::new();
        uleb(bodies.len() as u64, &mut functions);
        functions.extend(bodies.iter().map(|(ty, _)| *ty));
        section(3, functions, &mut out);
        // One memory of one page.
        section(5, vec![1, 0x00, 1], &mut out);
        let mut exports: Vec<(&str, u8, u8)> =
            vec![("flower_alloc", 0, 1), ("flower_invoke", 0, 2)];
        if self.memory {
            exports.push(("memory", 2, 0));
        }
        if self.manifest {
            exports.push(("flower_manifest", 0, 3));
        }
        if self.init.is_some() {
            exports.push(("flower_init", 0, 4));
        }
        let mut export_section = Vec::new();
        uleb(exports.len() as u64, &mut export_section);
        for (export, kind, index) in exports {
            name(export, &mut export_section);
            export_section.extend([kind, index]);
        }
        section(7, export_section, &mut out);
        let mut code = Vec::new();
        uleb(bodies.len() as u64, &mut code);
        for (_, instructions) in &bodies {
            let mut body = vec![0]; // no locals
            body.extend(instructions);
            body.push(0x0b);
            uleb(body.len() as u64, &mut code);
            code.extend(body);
        }
        section(10, code, &mut out);
        let mut data = vec![2];
        for (at, bytes) in [(OK_AT, ok_envelope()), (ERR_AT, err_envelope())] {
            data.push(0x00);
            data.push(0x41);
            sleb(i64::from(at), &mut data);
            data.push(0x0b);
            uleb(bytes.len() as u64, &mut data);
            data.extend(bytes);
        }
        section(11, data, &mut out);
        out
    }
}

/// A host that answers every call with an error envelope.
struct NoHost;

impl HostCall for NoHost {
    fn op(&mut self, _op: u8, _input: &[u8]) -> Vec<u8> {
        flower_sdk::abi::encode_err(&fail("NO_HOST", "no host in this test"))
            .encode_binary()
            .unwrap()
    }
}

fn raw_backend(limits: Limits, raw: &Raw) -> Result<WasmBackend, flower_sdk::Failure> {
    let mut backend = WasmBackend::with_limits(limits)?;
    backend.register("raw", &raw.encode())?;
    Ok(backend)
}

fn call(
    backend: &WasmBackend,
    module: &str,
    kind: CallKind,
    name: &str,
    args: Value,
) -> Result<Value, CallFailure> {
    backend.call(kind, module, name, &args, &mut NoHost)
}

#[test]
fn register_validates_abi_exports() {
    let ok = Raw::new(returns_ok());
    let backend = raw_backend(Limits::default(), &ok).unwrap();
    assert_eq!(
        call(&backend, "raw", CallKind::Query, "any", Value::Null).unwrap(),
        Value::Null
    );

    let mut no_manifest = Raw::new(returns_ok());
    no_manifest.manifest = false;
    let failure = raw_backend(Limits::default(), &no_manifest).err().unwrap();
    assert!(failure.message.contains("flower_manifest"), "{failure:?}");

    let mut no_memory = Raw::new(returns_ok());
    no_memory.memory = false;
    let failure = raw_backend(Limits::default(), &no_memory).err().unwrap();
    assert!(failure.message.contains("memory"), "{failure:?}");

    // An empty module imports nothing and exports nothing.
    let mut bad = WasmBackend::new().unwrap();
    assert!(bad.register("empty", b"\0asm\x01\0\0\0").is_err());
}

#[test]
fn register_runs_init_and_rejects_failing_init() {
    let mut failing = Raw::new(returns_ok());
    failing.init = Some(returns_err());
    let failure = raw_backend(Limits::default(), &failing).err().unwrap();
    assert_eq!(failure.code, wasm::INIT_FAILED);
    assert!(failure.message.contains("BROKEN_CONFIG"), "{failure:?}");

    let mut trapping = Raw::new(returns_ok());
    trapping.init = Some(vec![0x00]); // unreachable
    let failure = raw_backend(Limits::default(), &trapping).err().unwrap();
    assert_eq!(failure.code, wasm::HOST_TRAP);

    let mut silent = Raw::new(returns_ok());
    silent.init = Some(i64_const(0)); // cannot respond
    assert!(raw_backend(Limits::default(), &silent).is_err());

    let mut fine = Raw::new(returns_ok());
    fine.init = Some(returns_ok());
    let backend = raw_backend(Limits::default(), &fine).unwrap();
    assert_eq!(
        call(&backend, "raw", CallKind::Mutation, "any", Value::Null).unwrap(),
        Value::Null
    );
}

#[test]
fn endless_loop_hits_the_deadline() {
    // Unlimited fuel: only the epoch deadline can stop the guest.
    let limits = Limits {
        timeout: Duration::from_millis(50),
        fuel: u64::MAX,
        ..Limits::default()
    };
    let backend = raw_backend(limits, &Raw::new(spins())).unwrap();
    let failure = call(&backend, "raw", CallKind::Query, "spin", Value::Null).unwrap_err();
    assert_eq!(failure.failure.code, wasm::DEADLINE_EXCEEDED);
    assert!(!failure.recovered);
}

#[test]
fn a_deadline_inside_the_guests_allocator_is_a_deadline() {
    // The host writes a host call's output through the guest's
    // `flower_alloc`; the deadline may fire there, and it is the
    // deadline still, not a host trap.
    let flag = {
        let mut flag = vec![0x41];
        sleb(60_000, &mut flag);
        flag
    };
    // The first allocation (the invocation's input) succeeds; the next
    // (the host call's output) spins.
    let mut alloc = flag.clone();
    alloc.extend([0x28, 0x02, 0x00, 0x04, 0x40]); // i32.load, if
    alloc.extend(spins());
    alloc.push(0x0b); // end
    alloc.extend(flag);
    alloc.extend([0x41, 0x01, 0x36, 0x02, 0x00]); // i32.const 1, i32.store
    alloc.extend([0x41, 0x80, 0x80, 0x01]); // i32.const 16384
    // One host call, its packed output returned.
    let invoke = vec![0x41, 0x01, 0x41, 0x00, 0x41, 0x00, 0x10, 0x00];
    let limits = Limits {
        timeout: Duration::from_millis(50),
        fuel: u64::MAX,
        ..Limits::default()
    };
    let raw = Raw {
        alloc,
        ..Raw::new(invoke)
    };
    let backend = raw_backend(limits, &raw).unwrap();
    let failure = call(&backend, "raw", CallKind::Query, "any", Value::Null).unwrap_err();
    assert_eq!(failure.failure.code, wasm::DEADLINE_EXCEEDED, "{failure:?}");
}

#[test]
fn a_short_deadline_fires_on_time() {
    // A declared deadline ends the guest at most about a millisecond
    // late (one epoch tick, plus however late the OS wakes the ticker),
    // and never early. 10 ms ticks counted from the call fired a 5 ms
    // deadline after 10 to 20 ms. The best of a few runs, so a loaded
    // machine's scheduling does not decide it.
    let declared = Duration::from_millis(5);
    let limits = Limits {
        timeout: declared,
        fuel: u64::MAX,
        ..Limits::default()
    };
    let backend = raw_backend(limits, &Raw::new(spins())).unwrap();
    let mut runs = Vec::new();
    for _ in 0..5 {
        let started = std::time::Instant::now();
        let failure = call(&backend, "raw", CallKind::Query, "spin", Value::Null).unwrap_err();
        let ran = started.elapsed();
        assert_eq!(failure.failure.code, wasm::DEADLINE_EXCEEDED);
        assert!(ran >= declared, "fired early, after {ran:?}");
        runs.push(ran);
    }
    let best = runs.iter().min().unwrap();
    assert!(*best < declared + Duration::from_millis(3), "{runs:?}");
}

#[test]
fn a_callers_deadline_is_the_callers_not_the_calls() {
    // The call may run ten minutes; its caller stops waiting after 50 ms.
    // That is not the call running out of its own time (v1's
    // `EVALUATION_BUDGET`, a 422): another attempt with more time may
    // succeed.
    let limits = Limits {
        timeout: Duration::from_secs(600),
        fuel: u64::MAX,
        ..Limits::default()
    };
    let backend = raw_backend(limits, &Raw::new(spins())).unwrap();
    let budget = wasm::Budget {
        caller: Some(std::time::Instant::now() + Duration::from_millis(50)),
        ..wasm::Budget::default()
    };
    let failure = wasm::with_budget(budget, || {
        call(&backend, "raw", CallKind::Query, "spin", Value::Null)
    })
    .unwrap_err();
    assert_eq!(failure.failure.code, "caller_deadline_exceeded");
    assert!(!failure.recovered);
}

#[test]
fn a_calls_own_deadline_is_its_own_whoever_waits_longer() {
    // The call declares 50 ms and its caller would wait ten seconds: the
    // call ran out of its own time, a limit of its own (v1's
    // `EVALUATION_BUDGET`, 422), not something another attempt mends.
    let limits = Limits {
        timeout: Duration::from_secs(600),
        fuel: u64::MAX,
        ..Limits::default()
    };
    let backend = raw_backend(limits, &Raw::new(spins())).unwrap();
    let budget = wasm::Budget {
        declared: Some(Duration::from_millis(50)),
        caller: Some(std::time::Instant::now() + Duration::from_secs(10)),
        ..wasm::Budget::default()
    };
    let failure = wasm::with_budget(budget, || {
        call(&backend, "raw", CallKind::Query, "spin", Value::Null)
    })
    .unwrap_err();
    assert_eq!(failure.failure.code, wasm::DEADLINE_EXCEEDED);
    assert!(!failure.recovered);
}

#[test]
fn endless_loop_exhausts_fuel_before_a_long_deadline() {
    let limits = Limits {
        timeout: Duration::from_secs(600),
        fuel: 1_000_000,
        ..Limits::default()
    };
    let backend = raw_backend(limits, &Raw::new(spins())).unwrap();
    let failure = call(&backend, "raw", CallKind::Query, "spin", Value::Null).unwrap_err();
    assert_eq!(failure.failure.code, wasm::FUEL_EXHAUSTED);
    assert!(!failure.recovered);
}

#[test]
fn unbounded_recursion_is_a_stack_overflow() {
    // flower_invoke calls itself with its own arguments forever.
    let recursion = vec![0x20, 0, 0x20, 1, 0x20, 2, 0x10, 2];
    let backend = raw_backend(Limits::default(), &Raw::new(recursion)).unwrap();
    let failure = call(&backend, "raw", CallKind::Query, "deep", Value::Null).unwrap_err();
    assert_eq!(failure.failure.code, wasm::STACK_OVERFLOW);
    assert!(!failure.recovered);
    // The backend and the process survive: the next call runs fine.
    let failure = call(&backend, "raw", CallKind::Query, "deep", Value::Null).unwrap_err();
    assert_eq!(failure.failure.code, wasm::STACK_OVERFLOW);
}

#[test]
fn memory_growth_past_the_instance_limit_fails() {
    // memory.grow by 2048 pages (128 MiB) on top of the first page.
    let mut grow = vec![0x41];
    sleb(2048, &mut grow);
    grow.extend([0x40, 0x00, 0x1a]); // memory.grow 0; drop
    grow.extend(returns_ok());
    let limits = Limits {
        instance_memory: 64 * 1024 * 1024,
        ..Limits::default()
    };
    let backend = raw_backend(limits.clone(), &Raw::new(grow.clone())).unwrap();
    let failure = call(&backend, "raw", CallKind::Query, "grow", Value::Null).unwrap_err();
    assert_eq!(failure.failure.code, wasm::OUT_OF_MEMORY);
    assert!(!failure.recovered);
    // The same growth under a roomier limit succeeds.
    let roomy = Limits {
        instance_memory: 256 * 1024 * 1024,
        ..limits.clone()
    };
    let backend = raw_backend(roomy, &Raw::new(grow.clone())).unwrap();
    assert_eq!(
        call(&backend, "raw", CallKind::Query, "grow", Value::Null).unwrap(),
        Value::Null
    );
    // The aggregate invocation limit applies even under the instance one.
    let tight = Limits {
        instance_memory: 256 * 1024 * 1024,
        invocation_memory: 64 * 1024 * 1024,
        ..limits
    };
    let backend = raw_backend(tight, &Raw::new(grow)).unwrap();
    let failure = call(&backend, "raw", CallKind::Query, "grow", Value::Null).unwrap_err();
    assert_eq!(failure.failure.code, wasm::OUT_OF_MEMORY);
}

#[test]
fn instances_larger_than_the_limit_fail_to_register() {
    let limits = Limits {
        instance_memory: 32 * 1024,
        ..Limits::default()
    };
    let failure = raw_backend(limits, &Raw::new(returns_ok())).err().unwrap();
    assert_eq!(failure.code, wasm::OUT_OF_MEMORY);
}

// ---------------------------------------------------------------------
// The real SDK guest
// ---------------------------------------------------------------------

fn cargo() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .or_else(|| std::env::var("CARGO").ok())
        .unwrap_or_else(|| "cargo".to_string())
}

/// Build the guest crate with its `limits` probes for `wasm32`, once
/// per test process, in its own target dir: the base guest other
/// tests build must keep its exact manifest.
fn guest_wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| {
        let root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let target = root.join("target").join("wasm-guest-limits");
        let status = Command::new(cargo())
            .args([
                "build",
                "--offline",
                "--quiet",
                "--target",
                "wasm32-unknown-unknown",
                "-p",
                "flower-guest-test",
                "--features",
                "limits",
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
                .join("flower_guest_test.wasm"),
        )
        .unwrap()
    })
}

/// A backend with the guest registered under `limits`. The guest is
/// compiled once per test process: clones share the compiled module.
fn guest_backend(limits: Limits) -> WasmBackend {
    static TEMPLATE: OnceLock<WasmBackend> = OnceLock::new();
    let mut backend = TEMPLATE
        .get_or_init(|| {
            let mut backend = WasmBackend::new().unwrap();
            backend.set_crypto(Arc::new(flower_crypto::host::CryptoHost::new()));
            backend.register("app", guest_wasm()).unwrap();
            backend
        })
        .clone();
    backend.set_limits(limits);
    backend
}

/// A host that serves nested `Call` ops by re-entering the backend,
/// as the engine does. Names starting `random_query` are queries.
struct NestingHost<'a> {
    backend: &'a WasmBackend,
}

impl HostCall for NestingHost<'_> {
    fn op(&mut self, op: u8, input: &[u8]) -> Vec<u8> {
        let request = Value::decode_binary(input);
        let envelope = match (op, request) {
            (7, Ok(Value::Array(args))) => match args.as_slice() {
                [_, Value::String(name), call_args, _] => {
                    let kind = if name.starts_with("random_query") {
                        CallKind::Query
                    } else {
                        CallKind::Mutation
                    };
                    let backend = self.backend;
                    match backend.call(kind, "app", name, call_args, self) {
                        Ok(value) => flower_sdk::abi::encode_ok(value),
                        Err(failure) => flower_sdk::abi::encode_err(&failure.failure),
                    }
                }
                _ => flower_sdk::abi::encode_err(&fail("BAD_CALL", "malformed call")),
            },
            _ => flower_sdk::abi::encode_err(&fail("NO_HOST", "unsupported op")),
        };
        envelope.encode_binary().unwrap()
    }
}

fn guest_call(
    backend: &WasmBackend,
    kind: CallKind,
    name: &str,
    args: &[(&str, f64)],
) -> Result<Value, CallFailure> {
    let args = Value::Object(
        args.iter()
            .map(|(key, value)| (key.to_string(), Value::Number(*value)))
            .collect(),
    );
    let mut host = NestingHost { backend };
    backend.call(kind, "app", name, &args, &mut host)
}

#[test]
fn guest_validates_without_a_crypto_manager() {
    // Runtime builders without crypto (backup replay) still accept a
    // guest importing crypto_call; only its invocations need a manager.
    let mut backend = WasmBackend::new().unwrap();
    backend.register("app", guest_wasm()).unwrap();
    assert!(backend.manifest("app").is_ok());
    let failure = guest_call(&backend, CallKind::Query, "init_ran", &[]).unwrap_err();
    assert!(failure.failure.message.contains("crypto"), "{failure:?}");
    assert!(!failure.recovered);
}

#[test]
fn guest_init_state_is_in_every_instance() {
    let backend = guest_backend(Limits::default());
    for _ in 0..2 {
        let ran = guest_call(&backend, CallKind::Query, "init_ran", &[]).unwrap();
        assert_eq!(ran, Value::Bool(true));
    }
}

/// The guest registered anew: another database deploying the same bytes.
fn registered_backend(limits: Limits) -> WasmBackend {
    let mut backend = WasmBackend::with_limits(limits).unwrap();
    backend.set_crypto(Arc::new(flower_crypto::host::CryptoHost::new()));
    backend.register("app", guest_wasm()).unwrap();
    backend
}

fn field<'a>(state: &'a Value, name: &str) -> &'a Value {
    match state {
        Value::Object(map) => &map[name],
        _ => panic!("expected an object: {state:?}"),
    }
}

#[test]
fn a_deadlines_worth_of_host_calls_fits_its_fuel() {
    // Fuel follows the declaration (constants, the same on every
    // member), and must cover the declared time's worth of host calls:
    // a method declaring 5 ms that calls the host in a loop gets its
    // 5 ms before anything stops it (it ran out of fuel within 1.5 ms
    // of 50). What stops it after is the deadline or the fuel past it.
    let backend = guest_backend(Limits::default());
    let declared = Duration::from_millis(5);
    let budget = wasm::Budget {
        declared: Some(declared),
        ..wasm::Budget::default()
    };
    let started = std::time::Instant::now();
    let failure = wasm::with_budget(budget, || {
        guest_call(&backend, CallKind::Query, "host_loop", &[])
    })
    .unwrap_err();
    let ran = started.elapsed();
    assert!(ran >= declared, "stopped after {ran:?}: {failure:?}");
}

#[test]
fn a_long_loop_of_host_calls_runs_in_constant_memory() {
    // Two million answers of at least 40 bytes each: more than the
    // guest's 64 MiB bump region, which kept every host call's output
    // until the invocation ended. Each call now gives its output back.
    let backend = guest_backend(unmetered());
    let calls = 2_000_000.0;
    let answered = guest_call(&backend, CallKind::Query, "host_calls", &[("calls", calls)])
        .unwrap_or_else(|failure| panic!("{failure:?}"));
    assert_eq!(answered, Value::Number(calls));
}

#[test]
fn a_host_call_loop_runs_to_its_deadline() {
    // A method calling the host until stopped ends at its deadline, not
    // at a full heap (it filled 64 MiB within 20 to 70 ms).
    let backend = guest_backend(Limits::default());
    let budget = wasm::Budget {
        declared: Some(Duration::from_millis(200)),
        ..wasm::Budget::default()
    };
    let failure = wasm::with_budget(budget, || {
        guest_call(&backend, CallKind::Query, "host_loop", &[])
    })
    .unwrap_err();
    assert_eq!(failure.failure.code, wasm::DEADLINE_EXCEEDED, "{failure:?}");
}

#[test]
fn guest_calls_never_see_each_others_state() {
    // Scanning all guest memory takes a while in a debug guest.
    let backend = guest_backend(unmetered());
    let other = registered_backend(unmetered());
    assert!(backend.shares_image(&other, "app"));
    let pristine = guest_call(&backend, CallKind::Query, "leak_read", &[]).unwrap();
    assert_eq!(field(&pristine, "static"), &Value::Bool(false));
    assert_eq!(field(&pristine, "atomic"), &Value::Bool(false));
    assert_eq!(field(&pristine, "pairs"), &Value::Number(0.0));
    assert_eq!(field(&pristine, "init"), &Value::Bool(true));
    let Value::Number(pages) = field(&pristine, "pages") else {
        panic!("{pristine:?}");
    };
    // The writer sees its marks: statics, heap, a grown page and
    // host-buffer memory.
    let wrote = guest_call(
        &backend,
        CallKind::Mutation,
        "leak_write",
        &[("marker", 7.0), ("trap", 0.0)],
    )
    .unwrap();
    assert_eq!(field(&wrote, "static"), &Value::Bool(true));
    assert_eq!(field(&wrote, "atomic"), &Value::Bool(true));
    assert!(
        matches!(field(&wrote, "pairs"), Value::Number(pairs) if *pairs >= 3.0),
        "{wrote:?}"
    );
    assert!(
        matches!(field(&wrote, "pages"), Value::Number(grown) if grown > pages),
        "{wrote:?}"
    );
    // No later call, in this database or another, sees any of them.
    for reader in [&backend, &other] {
        let state = guest_call(reader, CallKind::Query, "leak_read", &[]).unwrap();
        assert_eq!(state, pristine);
    }
    // Nor after a call that wrote them and trapped.
    let failure = guest_call(
        &other,
        CallKind::Mutation,
        "leak_write",
        &[("marker", 8.0), ("trap", 1.0)],
    )
    .unwrap_err();
    assert_eq!(failure.failure.code, wasm::HOST_TRAP);
    for reader in [&other, &backend] {
        let state = guest_call(reader, CallKind::Query, "leak_read", &[]).unwrap();
        assert_eq!(state, pristine);
    }
}

#[test]
fn guest_memory_bomb_fails_out_of_memory() {
    let backend = guest_backend(unmetered());
    // A modest allocation fits under the default 128 MiB. Debug guests
    // zero it byte by byte, so these tests lift fuel and deadline.
    let touched = guest_call(
        &backend,
        CallKind::Query,
        "memory_bomb",
        &[("megabytes", 8.0)],
    )
    .unwrap();
    assert_eq!(touched, Value::Number(128.0));
    let failure = guest_call(
        &backend,
        CallKind::Query,
        "memory_bomb",
        &[("megabytes", 512.0)],
    )
    .unwrap_err();
    assert_eq!(failure.failure.code, wasm::OUT_OF_MEMORY);
    assert!(!failure.recovered);
    // The backend stays usable.
    assert!(guest_call(&backend, CallKind::Query, "init_ran", &[]).is_ok());
}

#[test]
fn guest_endless_loop_hits_the_deadline() {
    let backend = guest_backend(Limits {
        timeout: Duration::from_millis(100),
        fuel: u64::MAX,
        ..Limits::default()
    });
    let failure = guest_call(&backend, CallKind::Query, "spin", &[]).unwrap_err();
    assert_eq!(failure.failure.code, wasm::DEADLINE_EXCEEDED);
    assert!(!failure.recovered);
}

#[test]
fn guest_deep_recursion_fails_cleanly() {
    let backend = guest_backend(Limits {
        fuel: u64::MAX,
        timeout: Duration::from_secs(120),
        ..Limits::default()
    });
    // Shallow recursion works.
    let sum = guest_call(&backend, CallKind::Query, "recurse", &[("depth", 100.0)]).unwrap();
    assert_eq!(sum, Value::Number(5050.0));
    // Unbounded recursion fails the call instead of the process. Debug
    // guests may exhaust their linear-memory shadow stack (an
    // out-of-bounds trap) before the Wasm stack.
    let failure = guest_call(&backend, CallKind::Query, "recurse", &[("depth", 1e9)]).unwrap_err();
    assert!(!failure.recovered);
    assert!(
        [wasm::STACK_OVERFLOW, wasm::HOST_TRAP].contains(&failure.failure.code.as_str()),
        "{failure:?}"
    );
}

#[test]
fn guest_entropy_is_mutation_only() {
    let backend = guest_backend(Limits::default());
    let random = guest_call(&backend, CallKind::Mutation, "random_mutation", &[]).unwrap();
    assert!(
        matches!(&random, Value::String(hex) if hex.len() == 16),
        "{random:?}"
    );

    let failure = guest_call(&backend, CallKind::Query, "random_query", &[]).unwrap_err();
    assert_eq!(failure.failure.code, "INVALID_INVOCATION");
    let failure = guest_call(&backend, CallKind::Derived, "random_derived", &[]).unwrap_err();
    assert_eq!(failure.failure.code, "INVALID_INVOCATION");
    // A query called from a mutation is still a query.
    let failure = guest_call(&backend, CallKind::Mutation, "random_nested", &[]).unwrap_err();
    assert_eq!(failure.failure.code, "INVALID_INVOCATION");
}

#[test]
fn guest_memory_is_shared_across_nested_calls() {
    // Each level holds its ~65 MiB baseline plus 16 MiB: three levels
    // fit under 128 MiB per instance but not under 200 MiB in total.
    let args = [("depth", 2.0), ("megabytes", 16.0)];
    let roomy = guest_backend(unmetered());
    assert_eq!(
        guest_call(&roomy, CallKind::Mutation, "nest_alloc", &args).unwrap(),
        Value::Number(2.0)
    );
    let tight = guest_backend(Limits {
        invocation_memory: 200 * 1024 * 1024,
        ..unmetered()
    });
    let failure = guest_call(&tight, CallKind::Mutation, "nest_alloc", &args).unwrap_err();
    assert_eq!(failure.failure.code, wasm::OUT_OF_MEMORY);
    assert!(
        !failure.recovered,
        "a caught nested limit failure must stay fatal"
    );
}

#[test]
fn guest_runaway_nesting_fails_without_crashing() {
    let backend = guest_backend(Limits {
        invocation_memory: usize::MAX,
        ..Limits::default()
    });
    let failure = guest_call(&backend, CallKind::Mutation, "nest_forever", &[]).unwrap_err();
    assert!(!failure.recovered);
    assert!(
        [wasm::NESTING_TOO_DEEP, wasm::STACK_OVERFLOW].contains(&failure.failure.code.as_str()),
        "{failure:?}"
    );
}

#[test]
fn guest_fuel_is_shared_across_nested_calls() {
    const ITERATIONS: f64 = 100_000.0;
    let burn = |fuel: u64, depth: f64| {
        let backend = guest_backend(Limits {
            fuel,
            ..Limits::default()
        });
        guest_call(
            &backend,
            CallKind::Mutation,
            "burn",
            &[("depth", depth), ("iterations", ITERATIONS)],
        )
    };
    // Calibrate: the smallest doubling of fuel one level runs under.
    // Fuel metering is deterministic, so this is exact per build.
    let mut enough = 100_000u64;
    while let Err(failure) = burn(enough, 0.0) {
        assert_eq!(failure.failure.code, wasm::FUEL_EXHAUSTED);
        enough *= 2;
        assert!(enough < 1 << 40, "burn never finished");
    }
    // Twice that covers any single level, nested ones included, but
    // not seven levels drawing on one shared budget.
    let fuel = enough * 2;
    burn(fuel, 0.0).unwrap();
    let failure = burn(fuel, 6.0).unwrap_err();
    assert_eq!(failure.failure.code, wasm::FUEL_EXHAUSTED);
    assert!(!failure.recovered);
}

/// Default memory limits without fuel or a practical deadline: debug
/// guests zero fresh allocations byte by byte, which a memory test
/// should not be charged for.
fn unmetered() -> Limits {
    Limits {
        fuel: u64::MAX,
        timeout: Duration::from_secs(300),
        ..Limits::default()
    }
}
