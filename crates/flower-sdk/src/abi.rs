//! Guest ABI: invocation dispatch, envelopes, manifests and exports.
//!
//! Guests export `flower_alloc`, `flower_invoke`, `flower_manifest` and
//! `flower_init` (see [`export!`]), and import `flower.host_call` plus
//! `flower.crypto_call`. All buffers cross as packed `u64` (`ptr << 32 |
//! len`); a zero return means the guest failed to respond (out of memory or
//! an unencodable envelope), which the host treats as fatal for the
//! invocation.
//!
//! Response envelopes are binary [`Value`]s: `["ok", result]`,
//! `["error", {code, message, details}]` or, for savepoint calls only,
//! `["recovered", {code, message, details}]`. Invoke inputs are binary
//! `[name, args]` pairs. A host-call output lives in guest memory only
//! until the guest copies it, which it does before anything else runs:
//! the call then gives that memory back.
//!
//! [`HostOp`] numbers are stable wire contract: new operations append, never
//! reuse. [`InvocationKind`] distinguishes query, mutation, transaction
//! planner and derived computation.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::convert::From;

use crate::app::{
    App, Call, Collection, Definition, Materialize, Method, OnDelete, Place, ReferenceTargets,
};
use crate::ctx::CallFailure;
use crate::error::{Failure, Result, codes, fail};
use crate::schema::Schema;
use crate::value::Value;

/// Host-call operations. Numbers are stable wire contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum HostOp {
    /// Read one row by key.
    Get = 1,
    /// Read one row from unkeyed raw string storage.
    RawGet = 2,
    /// Read a derived instance.
    GetDerived = 3,
    /// Scan canonical keys.
    Scan = 4,
    /// Scan unkeyed raw string storage.
    RawScan = 5,
    /// Bounded index range scan.
    Range = 6,
    /// Call an exported method with a savepoint flag.
    Call = 7,
    /// Upsert one row.
    Set = 8,
    /// Delete one row.
    Delete = 9,
    /// Delete one row conditional on its current value.
    DeleteIfMatch = 10,
    /// Retain a materialized derived root.
    Materialize = 11,
    /// Release a materialized derived root.
    Unmaterialize = 12,
    /// Run settlement to a fixpoint.
    Settle = 13,
    /// Retained row versions.
    History = 14,
    /// Invocation timestamp.
    Now = 15,
    /// Same timestamp without the polling implication.
    Clock = 16,
    /// Declare a change instant: the earliest stands.
    ChangesAt = 17,
    /// Calling principal.
    Principal = 18,
    /// Calling module identity.
    Caller = 19,
    /// Enqueue one async work item.
    Enqueue = 20,
    /// Schedule one timer, replacing any same-ID timer.
    TimerSchedule = 21,
    /// Cancel one timer.
    TimerCancel = 22,
    /// Read one timer.
    TimerGet = 23,
    /// Scan timers due-first.
    TimerScan = 24,
    /// Requeue one dead timer.
    TimerRetry = 25,
    /// Claim queue items of one scope under fenced leases.
    QueueClaim = 26,
    /// Renew queue claims.
    QueueRenew = 27,
    /// Report on a queue claim: complete, fail, kill or release.
    QueueReport = 28,
    /// Delete one queue item in any state.
    QueueCancel = 29,
    /// Requeue one dead queue item.
    QueueRetry = 30,
    /// Read one queue item.
    QueueGet = 31,
    /// Count a queue scope's items and say whether a claim would take one.
    QueueStats = 32,
    /// Take, keep or leave a place in a queue scope's line.
    QueueWait = 33,
    /// Delete dead and completed queue items enqueued before a cutoff.
    QueuePurge = 34,
    /// Execute or explain one module-local, manifest-declared SQL plan.
    Sql = 35,
    /// Bind and execute/explain request-time SQL through a declared endpoint.
    SqlDynamic = 36,
}

impl HostOp {
    /// Decode an operation number.
    pub fn from_u32(op: u32) -> Option<Self> {
        match op {
            1 => Some(HostOp::Get),
            35 => Some(HostOp::Sql),
            36 => Some(HostOp::SqlDynamic),
            2 => Some(HostOp::RawGet),
            3 => Some(HostOp::GetDerived),
            4 => Some(HostOp::Scan),
            5 => Some(HostOp::RawScan),
            6 => Some(HostOp::Range),
            7 => Some(HostOp::Call),
            8 => Some(HostOp::Set),
            9 => Some(HostOp::Delete),
            10 => Some(HostOp::DeleteIfMatch),
            11 => Some(HostOp::Materialize),
            12 => Some(HostOp::Unmaterialize),
            13 => Some(HostOp::Settle),
            14 => Some(HostOp::History),
            15 => Some(HostOp::Now),
            16 => Some(HostOp::Clock),
            17 => Some(HostOp::ChangesAt),
            18 => Some(HostOp::Principal),
            19 => Some(HostOp::Caller),
            20 => Some(HostOp::Enqueue),
            21 => Some(HostOp::TimerSchedule),
            22 => Some(HostOp::TimerCancel),
            23 => Some(HostOp::TimerGet),
            24 => Some(HostOp::TimerScan),
            25 => Some(HostOp::TimerRetry),
            26 => Some(HostOp::QueueClaim),
            27 => Some(HostOp::QueueRenew),
            28 => Some(HostOp::QueueReport),
            29 => Some(HostOp::QueueCancel),
            30 => Some(HostOp::QueueRetry),
            31 => Some(HostOp::QueueGet),
            32 => Some(HostOp::QueueStats),
            33 => Some(HostOp::QueueWait),
            34 => Some(HostOp::QueuePurge),
            _ => None,
        }
    }
}

/// Guest invocation kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum InvocationKind {
    /// Query callback.
    Query = 1,
    /// Mutation callback.
    Mutation = 2,
    /// Transaction planner, without a context.
    Transaction = 3,
    /// Derived callback.
    Derived = 4,
    /// Aggregate reducer, without a context: a derived definition's
    /// [`Aggregation`][crate::app::Aggregation] folding row changes into
    /// its accumulator.
    Reduce = 5,
}

impl InvocationKind {
    /// Decode an invocation kind.
    pub fn from_u32(kind: u32) -> Option<Self> {
        match kind {
            1 => Some(InvocationKind::Query),
            2 => Some(InvocationKind::Mutation),
            3 => Some(InvocationKind::Transaction),
            4 => Some(InvocationKind::Derived),
            5 => Some(InvocationKind::Reduce),
            _ => None,
        }
    }
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "flower")]
unsafe extern "C" {
    #[link_name = "host_call"]
    fn host_call_import(op: u32, in_ptr: *const u8, in_len: u32) -> u64;
    #[link_name = "crypto_call"]
    fn crypto_call_import(op: u32, in_ptr: *const u8, in_len: u32) -> u64;
}

#[cfg(all(not(target_arch = "wasm32"), not(feature = "native-host")))]
unsafe extern "C" fn host_call_import(_op: u32, _in_ptr: *const u8, _in_len: u32) -> u64 {
    panic!("host calls require a wasm32 guest");
}

#[cfg(all(not(target_arch = "wasm32"), not(feature = "native-host")))]
unsafe extern "C" fn crypto_call_import(_op: u32, _in_ptr: *const u8, _in_len: u32) -> u64 {
    panic!("host calls require a wasm32 guest");
}

/// Call the host and copy the output synchronously. The output stays valid
/// until the next host call, and no other call happens before the copy.
#[cfg(not(all(feature = "native-host", not(target_arch = "wasm32"))))]
pub(crate) fn host_call(op: HostOp, input: &[u8]) -> Result<Vec<u8>> {
    host_call_raw(op as u32, input, host_call_import)
}

/// Call the crypto host the same way, over `crypto_call`.
#[cfg(not(all(feature = "native-host", not(target_arch = "wasm32"))))]
pub(crate) fn crypto_call(op: crate::crypto::CryptoOp, input: &[u8]) -> Result<Vec<u8>> {
    host_call_raw(op as u32, input, crypto_call_import)
}

/// Native builds with the bridge: the installed [`native::NativeHost`]
/// answers instead of a Wasm import.
#[cfg(all(feature = "native-host", not(target_arch = "wasm32")))]
pub(crate) fn host_call(op: HostOp, input: &[u8]) -> Result<Vec<u8>> {
    native::call(native::Import::Host, op as u32, input)
}

/// Native builds with the bridge: crypto calls reach the installed host.
#[cfg(all(feature = "native-host", not(target_arch = "wasm32")))]
pub(crate) fn crypto_call(op: crate::crypto::CryptoOp, input: &[u8]) -> Result<Vec<u8>> {
    native::call(native::Import::Crypto, op as u32, input)
}

/// In-process host bridge for native builds (feature `native-host`).
///
/// A Wasm guest reaches its host through imports; a native build of
/// the same [`App`] reaches whatever [`NativeHost`][native::NativeHost]
/// the embedder installs with [`with_host`][native::with_host] around
/// [`invoke_native`]. Installations nest per thread, so a host that
/// re-enters the app for a nested call installs itself again and the
/// outer host comes back when the nested call returns.
#[cfg(all(feature = "native-host", not(target_arch = "wasm32")))]
pub mod native {
    extern crate std;

    use alloc::vec::Vec;
    use core::cell::Cell;

    use crate::error::{Result, codes, fail};

    /// Which guest import a bridged call stands for.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Import {
        /// `flower.host_call`.
        Host,
        /// `flower.crypto_call`.
        Crypto,
    }

    /// The embedder's side of the bridge: one call in, binary envelope
    /// bytes out, exactly as the Wasm imports answer.
    pub trait NativeHost {
        /// Answer one bridged call.
        fn call(&mut self, import: Import, op: u32, input: &[u8]) -> Vec<u8>;
    }

    std::thread_local! {
        static HOST: Cell<Option<*mut (dyn NativeHost + 'static)>> = const { Cell::new(None) };
    }

    /// Restores the previous installation, on unwinding too.
    struct Restore(Option<*mut (dyn NativeHost + 'static)>);

    impl Drop for Restore {
        fn drop(&mut self) {
            HOST.with(|cell| cell.set(self.0));
        }
    }

    /// Run `body` with `host` answering this thread's host calls.
    pub fn with_host<R>(host: &mut dyn NativeHost, body: impl FnOnce() -> R) -> R {
        // SAFETY: this only erases the borrow's lifetime. The pointer is
        // installed for the duration of `body` alone (the guard restores
        // the previous one, on unwinding too), while `host` stays
        // mutably borrowed here, so it never dangles when dereferenced.
        let erased: *mut (dyn NativeHost + 'static) =
            unsafe { core::mem::transmute::<&mut dyn NativeHost, *mut dyn NativeHost>(host) };
        let previous = HOST.with(|cell| cell.replace(Some(erased)));
        let _restore = Restore(previous);
        body()
    }

    /// Forward one call to the installed host.
    pub(crate) fn call(import: Import, op: u32, input: &[u8]) -> Result<Vec<u8>> {
        match HOST.with(Cell::get) {
            // SAFETY: installed by `with_host`, whose frame is still live
            // (see there); calls on one thread never overlap.
            Some(host) => Ok(unsafe { (*host).call(import, op, input) }),
            None => Err(fail(
                codes::INVALID_INVOCATION,
                "no native host is installed on this thread",
            )),
        }
    }
}

/// Shared import-call plumbing: bound the input, call, copy the output.
#[cfg(not(all(feature = "native-host", not(target_arch = "wasm32"))))]
fn host_call_raw(
    op: u32,
    input: &[u8],
    import: unsafe extern "C" fn(u32, *const u8, u32) -> u64,
) -> Result<Vec<u8>> {
    if input.len() > u32::MAX as usize {
        return Err(fail(codes::INVALID_VALUE, "host-call input too large"));
    }
    // What the host allocates during the call, its output and any nested
    // invocation's input and response, is dead once the output is copied.
    let mark = heap_mark();
    // SAFETY: the import reads `input.len()` bytes from a live slice and
    // retains nothing; the length fits `u32` (checked above).
    let packed = unsafe { import(op, input.as_ptr(), input.len() as u32) };
    let ptr = (packed >> 32) as usize as *const u8;
    let len = (packed & 0xFFFF_FFFF) as usize;
    // SAFETY: the host ABI returns a buffer it allocated in this guest's
    // memory through `flower_alloc`, valid for `len` bytes until the next
    // host call; it is copied out before anything else runs.
    let output = unsafe { core::slice::from_raw_parts(ptr, len) };
    let mut copied = Vec::with_capacity(len);
    copied.extend_from_slice(output);
    heap_release(mark);
    Ok(copied)
}

/// Encode a success envelope.
pub fn encode_ok(value: Value) -> Value {
    Value::Array(alloc::vec![Value::String(String::from("ok")), value])
}

/// Encode a failure envelope.
pub fn encode_err(failure: &Failure) -> Value {
    let mut map = BTreeMap::new();
    map.insert(String::from("code"), Value::String(failure.code.clone()));
    map.insert(
        String::from("message"),
        Value::String(failure.message.clone()),
    );
    map.insert(
        String::from("details"),
        match &failure.details {
            Some(details) => details.clone(),
            None => Value::Null,
        },
    );
    Value::Array(alloc::vec![
        Value::String(String::from("error")),
        Value::Object(map),
    ])
}

/// Encode a recovered-savepoint envelope.
pub fn encode_recovered(failure: &Failure) -> Value {
    let mut map = BTreeMap::new();
    map.insert(String::from("code"), Value::String(failure.code.clone()));
    map.insert(
        String::from("message"),
        Value::String(failure.message.clone()),
    );
    map.insert(
        String::from("details"),
        match &failure.details {
            Some(details) => details.clone(),
            None => Value::Null,
        },
    );
    Value::Array(alloc::vec![
        Value::String(String::from("recovered")),
        Value::Object(map),
    ])
}

/// Decode an envelope, treating `recovered` as an error: plain calls never
/// legally receive one.
pub fn decode_envelope(response: Value) -> Result<Value> {
    match response {
        Value::Array(pair) if pair.len() == 2 => {
            let mut pair = pair.into_iter();
            let tag = pair.next();
            let payload = pair.next();
            match (tag, payload) {
                (Some(Value::String(tag)), Some(payload)) if tag == "ok" => Ok(payload),
                (Some(Value::String(tag)), Some(payload)) if tag == "error" => {
                    Err(decode_failure(payload)?)
                }
                _ => Err(fail(codes::INVALID_VALUE, "malformed envelope")),
            }
        }
        _ => Err(fail(codes::INVALID_VALUE, "malformed envelope")),
    }
}

/// Decode a savepoint-call envelope, preserving recovery state.
pub fn decode_call_envelope(response: Value) -> core::result::Result<Value, CallFailure> {
    let unrecovered = |failure: Failure| CallFailure {
        failure,
        recovered: false,
    };
    match response {
        Value::Array(pair) if pair.len() == 2 => {
            let mut pair = pair.into_iter();
            let tag = pair.next();
            let payload = pair.next();
            match (tag, payload) {
                (Some(Value::String(tag)), Some(payload)) if tag == "ok" => Ok(payload),
                (Some(Value::String(tag)), Some(payload)) if tag == "error" => {
                    match decode_failure(payload) {
                        Ok(failure) => Err(unrecovered(failure)),
                        Err(failure) => Err(unrecovered(failure)),
                    }
                }
                (Some(Value::String(tag)), Some(payload)) if tag == "recovered" => {
                    match decode_failure(payload) {
                        Ok(failure) => Err(CallFailure {
                            failure,
                            recovered: true,
                        }),
                        Err(failure) => Err(unrecovered(failure)),
                    }
                }
                _ => Err(unrecovered(fail(
                    codes::INVALID_VALUE,
                    "malformed envelope",
                ))),
            }
        }
        _ => Err(unrecovered(fail(
            codes::INVALID_VALUE,
            "malformed envelope",
        ))),
    }
}

fn decode_failure(payload: Value) -> Result<Failure> {
    let mut map = match payload {
        Value::Object(map) => map,
        _ => return Err(fail(codes::INVALID_VALUE, "malformed failure")),
    };
    let code = match map.remove("code") {
        Some(Value::String(code)) => code,
        _ => return Err(fail(codes::INVALID_VALUE, "failure without code")),
    };
    let message = match map.remove("message") {
        Some(Value::String(message)) => message,
        _ => return Err(fail(codes::INVALID_VALUE, "failure without message")),
    };
    let details = match map.remove("details") {
        None | Some(Value::Null) => None,
        Some(details) => Some(details),
    };
    Ok(Failure {
        code,
        message,
        details,
    })
}

/// Guest bump allocator for the buffers the host writes (invocation
/// inputs, host-call outputs) and the responses the guest returns: 64 MiB,
/// 8-byte aligned. Instances reset per invocation, so the bump pointer
/// resets with them and nothing leaks across invocations; within one,
/// every host call returns the pointer to where it found it once its
/// output is copied (see [`heap_mark`]), so a loop of host calls runs in
/// constant memory however long it lasts. Returns null on exhaustion.
/// The guest's own values live on its global allocator, which frees.
#[cfg(target_arch = "wasm32")]
static mut GUEST_HEAP: [u8; 67_108_864] = [0; 67_108_864];

#[cfg(target_arch = "wasm32")]
static mut GUEST_NEXT: usize = 0;

/// Where the bump pointer stands. Guests are single-threaded and calls
/// nest, so everything allocated after a mark is dead by the time the
/// host call that took it has copied its output, a nested invocation's
/// buffers included: [`heap_release`] gives it back.
#[cfg(not(all(feature = "native-host", not(target_arch = "wasm32"))))]
fn heap_mark() -> usize {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: single-threaded guest; a plain read of the pointer.
        unsafe { GUEST_NEXT }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

/// Return the bump pointer to `mark`, freeing what was allocated since.
#[cfg(not(all(feature = "native-host", not(target_arch = "wasm32"))))]
fn heap_release(mark: usize) {
    #[cfg(target_arch = "wasm32")]
    // SAFETY: single-threaded guest; `mark` came from `heap_mark` in the
    // host call now ending, so it never moves the pointer forward.
    unsafe {
        GUEST_NEXT = mark;
    }
    #[cfg(not(target_arch = "wasm32"))]
    let _ = mark;
}

/// Allocate guest memory. Called by the host through `flower_alloc` and by
/// generated shims; see the module documentation for the null contract.
#[allow(static_mut_refs)]
pub fn guest_alloc(size: u32) -> *mut u8 {
    #[cfg(target_arch = "wasm32")]
    {
        // Single-threaded guest: only dispatch touches the bump pointer.
        unsafe {
            let aligned = (GUEST_NEXT + 7) & !7;
            let end = match aligned.checked_add(size as usize) {
                Some(end) if end <= GUEST_HEAP.len() => end,
                _ => return core::ptr::null_mut(),
            };
            GUEST_NEXT = end;
            GUEST_HEAP.as_mut_ptr().add(aligned)
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        // Host-side execution (tests and native tooling) leaks deliberately:
        // guest buffers never free by design.
        let boxed = alloc::vec![0; size as usize].into_boxed_slice();
        alloc::boxed::Box::leak(boxed).as_mut_ptr()
    }
}

/// Invoke entry point behind generated `flower_invoke`. Returns packed
/// `ptr | len`, or zero when the guest cannot respond. The packing assumes
/// 32-bit pointers; 64-bit hosts test through [`invoke_native`] instead.
pub fn invoke_bytes(app: &App, kind: u32, in_ptr: *const u8, in_len: u32) -> u64 {
    let bytes = match invoke_inner(app, kind, in_ptr, in_len) {
        Ok(bytes) => bytes,
        Err(()) => return 0,
    };
    pack_buffer(&bytes)
}

/// Native test door: run one invocation and return the raw response bytes
/// without packing. Returns an empty vector when the guest cannot respond.
#[cfg(not(target_arch = "wasm32"))]
pub fn invoke_native(app: &App, kind: u32, input: &[u8]) -> Vec<u8> {
    let len = u32::try_from(input.len()).unwrap_or(u32::MAX);
    invoke_inner(app, kind, input.as_ptr(), len).unwrap_or_default()
}

/// Native test door for [`manifest_bytes`]: the raw manifest response
/// bytes, or an empty vector when the guest cannot respond.
#[cfg(not(target_arch = "wasm32"))]
pub fn manifest_native(app: &App) -> Vec<u8> {
    manifest_envelope(app).encode_binary().unwrap_or_default()
}

/// The manifest as an envelope: the manifest, or why the SDK refuses it
/// (an unknown name in `Authorization.methods`, say), for the build to
/// report rather than "guest cannot respond".
fn manifest_envelope(app: &App) -> Value {
    match manifest_value(app) {
        Ok(manifest) => encode_ok(manifest),
        Err(failure) => encode_err(&failure),
    }
}

/// Manifest entry point behind generated `flower_manifest`.
pub fn manifest_bytes(app: &App) -> u64 {
    match manifest_envelope(app).encode_binary() {
        Ok(bytes) => pack_buffer(&bytes),
        Err(_) => 0,
    }
}

/// Init entry point behind generated `flower_init`.
pub fn init_bytes(app: &App) -> u64 {
    let outcome = match app.init {
        None => Ok(Value::Null),
        Some(init) => init().map(|_| Value::Null),
    };
    let envelope = match outcome {
        Ok(value) => encode_ok(value),
        Err(failure) => encode_err(&failure),
    };
    let bytes = match envelope.encode_binary() {
        Ok(bytes) => bytes,
        Err(_) => return 0,
    };
    pack_buffer(&bytes)
}

fn pack_buffer(bytes: &[u8]) -> u64 {
    if bytes.len() > u32::MAX as usize {
        return 0;
    }
    let ptr = guest_alloc(bytes.len() as u32);
    if ptr.is_null() {
        return 0;
    }
    // SAFETY: `ptr` is a fresh, non-null allocation of `bytes.len()`
    // bytes, so it is valid for writes and cannot overlap `bytes`.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
    }
    ((ptr as u64) << 32) | (bytes.len() as u64)
}

fn invoke_inner(
    app: &App,
    kind: u32,
    in_ptr: *const u8,
    in_len: u32,
) -> core::result::Result<Vec<u8>, ()> {
    if in_ptr.is_null() {
        return respond(Err(fail(
            codes::INVALID_INVOCATION,
            "null invocation input",
        )));
    }
    // SAFETY: the host wrote `in_len` bytes at `in_ptr` into a buffer it
    // obtained from `flower_alloc` and keeps them alive for the call; the
    // pointer is non-null (checked above).
    let input = unsafe { core::slice::from_raw_parts(in_ptr, in_len as usize) };
    let request = match Value::decode_binary(input) {
        Ok(request) => request,
        Err(failure) => return respond(Err(failure)),
    };
    let (name, args) = match request {
        Value::Array(pair) if pair.len() == 2 => {
            let mut pair = pair.into_iter();
            let name = pair.next();
            let args = pair.next();
            match (name, args) {
                (Some(Value::String(name)), Some(args)) => (name, args),
                _ => {
                    return respond(Err(fail(
                        codes::INVALID_INVOCATION,
                        "invocation is not [name, args]",
                    )));
                }
            }
        }
        _ => {
            return respond(Err(fail(
                codes::INVALID_INVOCATION,
                "invocation is not [name, args]",
            )));
        }
    };
    let kind = match InvocationKind::from_u32(kind) {
        Some(kind) => kind,
        None => {
            return respond(Err(fail(
                codes::INVALID_INVOCATION,
                "unknown invocation kind",
            )));
        }
    };
    respond(dispatch(app, kind, &name, args))
}

fn respond(outcome: Result<Value>) -> core::result::Result<Vec<u8>, ()> {
    let envelope = match outcome {
        Ok(value) => encode_ok(value),
        Err(failure) => encode_err(&failure),
    };
    envelope.encode_binary().map_err(|_| ())
}

/// The component providing the application's authorization hook, if any;
/// more than one is ambiguous and fails.
fn authorizer(app: &App) -> Result<Option<&'static dyn crate::app::Component>> {
    let mut found = None;
    for component in app.flattened() {
        if component.authorizes() {
            if found.is_some() {
                return Err(fail(
                    codes::INVALID_VALUE,
                    "more than one authorization component",
                ));
            }
            found = Some(component);
        }
    }
    Ok(found)
}

/// The access a method or transaction declares, by name (aliases are
/// definition names).
fn declared_access(app: &App, name: &str) -> Option<crate::auth::Access> {
    match app.definition(name) {
        Some(Definition::Method(method)) => method.access,
        Some(Definition::Transaction(transaction)) => transaction.access,
        _ => None,
    }
}

/// Check the authorization component's access table: every name is a
/// method or transaction of the app that declares no access of its own,
/// named once.
fn check_granted_access(
    granted: &[(&'static str, crate::auth::Access)],
    definitions: &[Definition],
) -> Result<()> {
    for (index, (name, _)) in granted.iter().enumerate() {
        let declares = match definitions
            .iter()
            .find(|definition| definition.name() == *name)
        {
            Some(Definition::Method(method)) => method.access.is_some(),
            Some(Definition::Transaction(transaction)) => transaction.access.is_some(),
            _ => {
                return Err(fail(
                    codes::INVALID_VALUE,
                    alloc::format!(
                        "the authorization component grants access to `{name}`, which is not a \
                         method or transaction of this module"
                    ),
                ));
            }
        };
        if declares {
            return Err(fail(
                codes::INVALID_VALUE,
                alloc::format!(
                    "`{name}` declares its own access; the authorization component may not \
                     grant it another"
                ),
            ));
        }
        if granted[..index].iter().any(|(earlier, _)| earlier == name) {
            return Err(fail(
                codes::INVALID_VALUE,
                alloc::format!("the authorization component grants access to `{name}` twice"),
            ));
        }
    }
    Ok(())
}

/// Route one invocation to its definition, checking kind and arguments.
/// The authorization hook is not a definition: queries named
/// [`AUTHORIZE`][crate::auth::AUTHORIZE] reach the authorization component.
pub(crate) fn dispatch(app: &App, kind: InvocationKind, name: &str, args: Value) -> Result<Value> {
    if name == crate::auth::AUTHORIZE
        && matches!(kind, InvocationKind::Query)
        && let Some(component) = authorizer(app)?
        && let Some(decision) =
            component.authorize(&mut crate::ctx::Ctx::new(), args.clone(), &|method| {
                declared_access(app, method)
            })
    {
        return decision;
    }
    let definition = match app.definition(name) {
        Some(definition) => definition,
        // The host fires a trigger as the mutation of its name.
        None => match app
            .triggers()
            .into_iter()
            .find(|trigger| trigger.name == name)
        {
            Some(trigger) if matches!(kind, InvocationKind::Mutation) => {
                let change = decode_change(args)?;
                let mut ctx = crate::ctx::Ctx::new();
                return (trigger.callback)(&mut ctx, &change).map(|()| Value::Null);
            }
            _ => match retentions(app).find(|hook| hook.name == name) {
                Some(hook) if matches!(kind, InvocationKind::Mutation) => {
                    let row = decode_deleted(args)?;
                    let mut ctx = crate::ctx::Ctx::new();
                    return (hook.retain)(&mut ctx, &row).map(|()| Value::Null);
                }
                _ => match tasks(app).find(|task| task.name == name) {
                    Some(task) => return dispatch_task(task, kind, args),
                    None => return Err(fail(codes::UNKNOWN_METHOD, "unknown definition")),
                },
            },
        },
    };
    match (kind, definition) {
        (InvocationKind::Query, Definition::Method(method)) if method.is_query() => {
            check_args(method.args, &args)?;
            let mut ctx = crate::ctx::Ctx::new();
            (method.callback)(&mut ctx, args)
        }
        (InvocationKind::Mutation, Definition::Method(method)) if !method.is_query() => {
            check_args(method.args, &args)?;
            let mut ctx = crate::ctx::Ctx::new();
            (method.callback)(&mut ctx, args)
        }
        (InvocationKind::Derived, Definition::Derived(derived)) => {
            check_args(derived.args, &args)?;
            let mut ctx = crate::ctx::Ctx::new();
            (derived.callback)(&mut ctx, args)
        }
        // No context: the reducer folds what the host hands it.
        (InvocationKind::Reduce, Definition::Derived(derived)) => match &derived.aggregate {
            Some(aggregation) => (aggregation.reduce)(args),
            None => Err(fail(
                codes::INVALID_INVOCATION,
                "derived value is not an aggregate",
            )),
        },
        (InvocationKind::Transaction, Definition::Transaction(transaction)) => {
            check_args(transaction.args, &args)?;
            plan_value((transaction.planner)(args)?)
        }
        _ => Err(fail(codes::INVALID_INVOCATION, "definition kind mismatch")),
    }
}

/// Decode a trigger firing `{collection, key, before, after}`; null
/// images are absent.
fn decode_change(args: Value) -> Result<crate::app::Change> {
    let mut map = match args {
        Value::Object(map) => map,
        _ => return Err(fail(codes::TYPE_MISMATCH, "change is not an object")),
    };
    let image = |value: Option<Value>| match value {
        None | Some(Value::Null) => None,
        Some(value) => Some(value),
    };
    Ok(crate::app::Change {
        key: map.remove("key").unwrap_or(Value::Null),
        before: image(map.remove("before")),
        after: image(map.remove("after")),
    })
}

/// Decode a retention event `{collection, key, value, deleted_at}`.
fn decode_deleted(args: Value) -> Result<crate::app::DeletedRow> {
    let mut map = match args {
        Value::Object(map) => map,
        _ => return Err(fail(codes::TYPE_MISMATCH, "deleted row is not an object")),
    };
    let collection = match map.remove("collection") {
        Some(Value::String(collection)) => collection,
        _ => {
            return Err(fail(
                codes::TYPE_MISMATCH,
                "deleted row without a collection",
            ));
        }
    };
    let deleted_at = match map.remove("deleted_at") {
        Some(Value::Number(at)) if at.is_finite() => at,
        _ => return Err(fail(codes::TYPE_MISMATCH, "deleted row without a time")),
    };
    Ok(crate::app::DeletedRow {
        collection,
        key: map.remove("key").unwrap_or(Value::Null),
        value: map.remove("value").unwrap_or(Value::Null),
        deleted_at,
    })
}

/// Run one maintenance-task callback: `due` as a query, `run` as a
/// mutation without arguments, and the error callback as a mutation
/// handed the failure `{error, failed_at}`.
fn dispatch_task(task: &crate::app::Task, kind: InvocationKind, args: Value) -> Result<Value> {
    let mut ctx = crate::ctx::Ctx::new();
    match kind {
        InvocationKind::Query => match (task.due)(&mut ctx)? {
            Some(at) if at.is_finite() => Ok(Value::Number(at)),
            Some(_) => Err(fail(codes::INVALID_VALUE, "a task's due time is finite")),
            None => Ok(Value::Null),
        },
        InvocationKind::Mutation if matches!(args, Value::Null) => (task.run)(&mut ctx),
        InvocationKind::Mutation => match task.on_error {
            Some(on_error) => {
                let failure = decode_task_failure(args)?;
                on_error(&mut ctx, &failure)
            }
            None => Err(fail(
                codes::INVALID_INVOCATION,
                "task has no error callback",
            )),
        },
        _ => Err(fail(codes::INVALID_INVOCATION, "definition kind mismatch")),
    }
}

/// Decode a failed run `{error: {code, message, details}, failed_at}`.
fn decode_task_failure(args: Value) -> Result<crate::app::TaskFailure> {
    let mut map = match args {
        Value::Object(map) => map,
        _ => return Err(fail(codes::TYPE_MISMATCH, "task failure is not an object")),
    };
    let error = decode_failure(map.remove("error").unwrap_or(Value::Null))?;
    let failed_at = match map.remove("failed_at") {
        Some(Value::Number(at)) if at.is_finite() => at,
        _ => return Err(fail(codes::TYPE_MISMATCH, "task failure without a time")),
    };
    Ok(crate::app::TaskFailure { error, failed_at })
}

/// Check arguments against a definition's contract. A refusal is
/// `INVALID_ARGUMENT`, its details the path to the offending value as member
/// names and indexes (`{"path": ["items", 1, "count"]}`).
fn check_args(schema: Schema, args: &Value) -> Result<()> {
    schema
        .parse(args)
        .map_err(|error| error.invalid_argument("arguments"))
}

/// Encode a plan as `{"calls": [...], "value": ...}`.
pub(crate) fn plan_value(plan: crate::app::Plan) -> Result<Value> {
    let mut calls = Vec::with_capacity(plan.calls.len());
    for call in plan.calls {
        calls.push(call_value(&call)?);
    }
    let mut map = BTreeMap::new();
    map.insert(String::from("calls"), Value::Array(calls));
    map.insert(
        String::from("value"),
        match plan.value {
            Some(value) => value,
            None => Value::Null,
        },
    );
    Ok(Value::Object(map))
}

fn call_value(call: &Call) -> Result<Value> {
    let mut target = BTreeMap::new();
    if call.target.is_partition() {
        target.insert(
            String::from("partition"),
            Value::String(String::from(call.target.name())),
        );
    } else {
        target.insert(
            String::from("group"),
            Value::String(String::from(call.target.name())),
        );
    }
    let mut map = BTreeMap::new();
    map.insert(String::from("target"), Value::Object(target));
    map.insert(String::from("alias"), Value::String(call.alias.clone()));
    map.insert(String::from("args"), call.args.clone());
    Ok(Value::Object(map))
}

/// Encode the application manifest: collections, definitions, triggers,
/// the HTTP aliases, exports and imports, all generated from the
/// components.
pub(crate) fn manifest_value(app: &App) -> Result<Value> {
    let all = app.flattened();
    let mut collections = Vec::new();
    let mut definitions = Vec::new();
    let mut declared = Vec::new();
    let mut triggers = Vec::new();
    let mut retention = Vec::new();
    for component in all.iter() {
        if let Some(collection) = component.collection() {
            collections.push(collection_value(collection)?);
        }
        if let Some(definition) = component.definition() {
            definitions.push(definition_value(&definition)?);
            declared.push(definition);
        }
        if let Some(trigger) = component.trigger() {
            triggers.push(trigger);
        }
        if let Some(hook) = component.retention() {
            retention.push(hook);
        }
    }
    // The host fires each trigger and retention hook as the mutation of
    // its name; one without a same-named definition dispatches to its own
    // callback.
    let hooks = triggers
        .iter()
        .map(|trigger| trigger.name)
        .chain(retention.iter().map(|hook| hook.name));
    for hook in hooks {
        if declared.iter().any(|def| def.name() == hook) {
            continue;
        }
        let mut map = BTreeMap::new();
        map.insert(String::from("name"), Value::String(String::from(hook)));
        map.insert(
            String::from("kind"),
            Value::String(String::from("mutation")),
        );
        map.insert(String::from("args"), schema_value(&Schema::Json, 0)?);
        definitions.push(Value::Object(map));
    }
    let mut trigger_values = Vec::with_capacity(triggers.len());
    for trigger in triggers.iter() {
        let mut map = BTreeMap::new();
        map.insert(
            String::from("name"),
            Value::String(String::from(trigger.name)),
        );
        map.insert(
            String::from("source"),
            Value::String(String::from(trigger.source)),
        );
        trigger_values.push(Value::Object(map));
    }
    // Every method and transaction callable over HTTP answers to its own
    // name.
    let mut http = Vec::new();
    let mut declares_access = false;
    for definition in declared.iter() {
        let public = match definition {
            Definition::Method(method) => {
                declares_access |= method.access.is_some();
                method.http
            }
            Definition::Transaction(transaction) => {
                declares_access |= transaction.access.is_some();
                transaction.http
            }
            Definition::Derived(_) => false,
        };
        if public {
            let mut map = BTreeMap::new();
            let name = Value::String(String::from(definition.name()));
            map.insert(String::from("alias"), name.clone());
            map.insert(String::from("definition"), name);
            http.push(Value::Object(map));
        }
    }
    let authorization = authorizer(app)?;
    let authorizes = authorization.is_some();
    if declares_access && !authorizes {
        return Err(fail(
            codes::INVALID_VALUE,
            "methods declare access but no authorization component authenticates callers",
        ));
    }
    if let Some(component) = authorization {
        check_granted_access(component.granted_access(), &declared)?;
    }
    let mut map = BTreeMap::new();
    map.insert(String::from("collections"), Value::Array(collections));
    map.insert(String::from("definitions"), Value::Array(definitions));
    map.insert(String::from("triggers"), Value::Array(trigger_values));
    map.insert(String::from("http"), Value::Array(http));
    map.insert(String::from("authorize"), Value::Bool(authorizes));
    let mut sql = Vec::new();
    for plan in all.iter().filter_map(|component| component.sql()) {
        let mut entry = BTreeMap::new();
        if !plan.allow_scan {
            entry.insert(String::from("allow_scan"), Value::Bool(false));
        }
        entry.insert(String::from("name"), Value::String(String::from(plan.name)));
        entry.insert(
            String::from("statement"),
            Value::String(String::from(plan.statement)),
        );
        entry.insert(String::from("params"), schema_value(&plan.params, 0)?);
        entry.insert(String::from("row"), schema_value(&plan.row, 0)?);
        entry.insert(
            String::from("candidates"),
            Value::Number(plan.budget.candidates.into()),
        );
        entry.insert(
            String::from("bytes"),
            Value::Number(plan.budget.bytes.into()),
        );
        entry.insert(String::from("work"), Value::Number(plan.budget.work.into()));
        sql.push(Value::Object(entry));
    }
    if !sql.is_empty() {
        map.insert(String::from("sql"), Value::Array(sql));
    }
    let mut endpoints = Vec::new();
    for endpoint in all.iter().filter_map(|component| component.sql_endpoint()) {
        let mut entry = BTreeMap::new();
        entry.insert(String::from("name"), Value::from(endpoint.name));
        entry.insert(
            String::from("collections"),
            Value::Array(
                endpoint
                    .collections
                    .iter()
                    .map(|name| Value::from(*name))
                    .collect(),
            ),
        );
        entry.insert(
            String::from("candidates"),
            Value::Number(endpoint.budget.candidates.into()),
        );
        entry.insert(
            String::from("bytes"),
            Value::Number(endpoint.budget.bytes.into()),
        );
        entry.insert(
            String::from("work"),
            Value::Number(endpoint.budget.work.into()),
        );
        if !endpoint.allow_scan {
            entry.insert(String::from("allow_scan"), Value::Bool(false));
        }
        endpoints.push(Value::Object(entry));
    }
    if !endpoints.is_empty() {
        map.insert(String::from("sql_endpoints"), Value::Array(endpoints));
    }
    if let Some(exports) = exports_value(&all, &declared)? {
        map.insert(String::from("exports"), exports);
    }
    let imports = imports_value(&all)?;
    if !imports.is_empty() {
        map.insert(String::from("imports"), Value::Array(imports));
    }
    let retention: Vec<Value> = retention
        .iter()
        .map(|hook| {
            let mut map = BTreeMap::new();
            map.insert(String::from("name"), Value::String(String::from(hook.name)));
            map.insert(
                String::from("source"),
                Value::String(String::from(hook.source)),
            );
            Value::Object(map)
        })
        .collect();
    if !retention.is_empty() {
        map.insert(String::from("retention"), Value::Array(retention));
    }
    // Only modules with tasks carry the member, so other manifests and
    // code identities stay unchanged.
    let mut declared = Vec::new();
    for task in all.iter().filter_map(|component| component.task()) {
        let mut entry = BTreeMap::new();
        entry.insert(String::from("name"), Value::String(String::from(task.name)));
        entry.insert(
            String::from("on_error"),
            Value::Bool(task.on_error.is_some()),
        );
        declared.push(Value::Object(entry));
    }
    if !declared.is_empty() {
        map.insert(String::from("tasks"), Value::Array(declared));
    }
    // Only modules that declare keys carry the member, so manifests
    // and code identities without keys stay unchanged. Plain loops:
    // every guest links this, and iterator adapters cost debug guests
    // code to compile at every deploy.
    let mut keys = Vec::new();
    for component in all.iter() {
        for declaration in component.keys().iter() {
            keys.push(declaration.to_value());
        }
    }
    if !keys.is_empty() {
        map.insert(String::from("keys"), Value::Array(keys));
    }
    // Queue declarations: their contracts and line turns. Only modules
    // that declare queues carry the member.
    let mut queues = Vec::new();
    for component in all.iter() {
        if let Some(queue) = component.queue() {
            let mut entry = BTreeMap::new();
            entry.insert(
                String::from("name"),
                Value::String(String::from(queue.name)),
            );
            entry.insert(String::from("payload"), schema_value(&queue.payload, 0)?);
            entry.insert(String::from("result"), schema_value(&queue.result, 0)?);
            entry.insert(String::from("turn_ms"), Value::Number(queue.turn_ms as f64));
            queues.push(Value::Object(entry));
        }
    }
    if !queues.is_empty() {
        map.insert(String::from("queues"), Value::Array(queues));
    }
    Ok(Value::Object(map))
}

/// Every component's retention hook.
fn retentions(app: &App) -> impl Iterator<Item = &'static crate::app::Retention> {
    app.flattened()
        .into_iter()
        .filter_map(|component| component.retention())
}

/// Every component's maintenance task.
fn tasks(app: &App) -> impl Iterator<Item = &'static crate::app::Task> {
    app.flattened()
        .into_iter()
        .filter_map(|component| component.task())
}

fn names(names: impl Iterator<Item = &'static str>) -> Value {
    let mut sorted: Vec<&str> = names.collect();
    sorted.sort_unstable();
    sorted.dedup();
    Value::Array(
        sorted
            .into_iter()
            .map(|name| Value::String(String::from(name)))
            .collect(),
    )
}

/// What the module exports, generated from what each collection, derived
/// value and method declares, or `None` when nothing is.
fn exports_value(
    all: &[&'static dyn crate::app::Component],
    definitions: &[Definition],
) -> Result<Option<Value>> {
    let collections: Vec<&'static str> = all
        .iter()
        .filter_map(|component| component.collection())
        .filter(|collection| collection.export)
        .map(|collection| collection.name)
        .collect();
    let mut derived = Vec::new();
    let mut methods = Vec::new();
    let mut callers = BTreeMap::new();
    for definition in definitions {
        match definition {
            Definition::Derived(value) if value.export => derived.push(value.name),
            Definition::Method(method) => match method.export {
                crate::app::Export::Private => {}
                crate::app::Export::All => methods.push(method.name),
                // An empty list is an allowlist naming nobody.
                crate::app::Export::Only(modules) => {
                    methods.push(method.name);
                    callers.insert(String::from(method.name), names(modules.iter().copied()));
                }
            },
            _ => {}
        }
    }
    if collections.is_empty() && derived.is_empty() && methods.is_empty() {
        return Ok(None);
    }
    let mut map = BTreeMap::new();
    map.insert(String::from("collections"), names(collections.into_iter()));
    map.insert(String::from("derived"), names(derived.into_iter()));
    map.insert(String::from("methods"), names(methods.into_iter()));
    map.insert(String::from("callers"), Value::Object(callers));
    Ok(Some(Value::Object(map)))
}

fn contract_value(contract: &crate::app::Contract) -> Result<Value> {
    let mut map = BTreeMap::new();
    map.insert(String::from("args"), schema_value(&contract.args, 0)?);
    map.insert(String::from("result"), schema_value(&contract.result, 0)?);
    map.insert(String::from("errors"), schema_value(&contract.errors, 0)?);
    Ok(Value::Object(map))
}

fn imports_value(all: &[&'static dyn crate::app::Component]) -> Result<Vec<Value>> {
    let mut imports = Vec::new();
    for import in all.iter().filter_map(|component| component.imports()) {
        let mut collections = Vec::with_capacity(import.collections.len());
        for collection in import.collections.iter() {
            let mut map = BTreeMap::new();
            map.insert(
                String::from("name"),
                Value::String(String::from(collection.name)),
            );
            map.insert(String::from("key"), schema_value(&collection.key, 0)?);
            map.insert(String::from("row"), schema_value(&collection.row, 0)?);
            collections.push(Value::Object(map));
        }
        let mut derived = Vec::with_capacity(import.derived.len());
        for item in import.derived.iter() {
            let mut map = BTreeMap::new();
            map.insert(String::from("name"), Value::String(String::from(item.name)));
            if let Some(contract) = &item.contract {
                map.insert(String::from("contract"), contract_value(contract)?);
            }
            derived.push(Value::Object(map));
        }
        let mut methods = Vec::with_capacity(import.methods.len());
        for method in import.methods.iter() {
            let mut map = BTreeMap::new();
            map.insert(
                String::from("name"),
                Value::String(String::from(method.name)),
            );
            map.insert(
                String::from("kind"),
                Value::String(String::from(match method.kind {
                    crate::app::MethodKind::Query => "query",
                    crate::app::MethodKind::Mutation => "mutation",
                })),
            );
            if let Some(contract) = &method.contract {
                map.insert(String::from("contract"), contract_value(contract)?);
            }
            methods.push(Value::Object(map));
        }
        let mut map = BTreeMap::new();
        map.insert(
            String::from("module"),
            Value::String(String::from(import.module)),
        );
        map.insert(String::from("collections"), Value::Array(collections));
        map.insert(String::from("derived"), Value::Array(derived));
        map.insert(String::from("methods"), Value::Array(methods));
        imports.push(Value::Object(map));
    }
    Ok(imports)
}

/// Add a definition's result and error contracts, when constrained:
/// absent members read as `json`, so unconstrained manifests stay as
/// they were.
fn insert_contract(
    map: &mut BTreeMap<String, Value>,
    result: &Schema,
    errors: &Schema,
) -> Result<()> {
    if *result != Schema::Json {
        map.insert(String::from("result"), schema_value(result, 0)?);
    }
    if *errors != Schema::Json {
        map.insert(String::from("errors"), schema_value(errors, 0)?);
    }
    Ok(())
}

fn collection_value(collection: &Collection) -> Result<Value> {
    let mut indexes = Vec::with_capacity(collection.indexes.len());
    for index in collection.indexes.iter() {
        let mut fields = Vec::with_capacity(index.fields.len());
        for field in index.fields.iter() {
            fields.push(Value::String(String::from(*field)));
        }
        let mut map = BTreeMap::new();
        map.insert(
            String::from("name"),
            Value::String(String::from(index.name)),
        );
        map.insert(String::from("fields"), Value::Array(fields));
        indexes.push(Value::Object(map));
    }
    let mut references = Vec::with_capacity(collection.references.len());
    for reference in collection.references.iter() {
        references.push(reference_value(reference)?);
    }
    let mut map = BTreeMap::new();
    map.insert(
        String::from("name"),
        Value::String(String::from(collection.name)),
    );
    map.insert(
        String::from("key"),
        schema_value(&collection.key_schema, 0)?,
    );
    map.insert(
        String::from("value"),
        schema_value(&collection.value_schema, 0)?,
    );
    map.insert(String::from("indexes"), Value::Array(indexes));
    map.insert(String::from("references"), Value::Array(references));
    if let Some(policy) = collection.access {
        map.insert(String::from("access"), policy().to_value());
    }
    Ok(Value::Object(map))
}

fn reference_value(reference: &crate::app::Reference) -> Result<Value> {
    let mut targets = Vec::new();
    let mut discriminator = Value::Null;
    match &reference.targets {
        ReferenceTargets::One(target) => {
            targets.push(Value::String(String::from(target.name)));
        }
        ReferenceTargets::Poly {
            targets: poly,
            discriminator: name,
        } => {
            for target in poly.iter() {
                targets.push(Value::String(String::from(target.name)));
            }
            discriminator = Value::String(String::from(*name));
        }
    }
    let place = match &reference.place {
        Place::Fields(fields) => {
            let mut names = Vec::with_capacity(fields.len());
            for field in fields.iter() {
                names.push(Value::String(String::from(*field)));
            }
            let mut map = BTreeMap::new();
            map.insert(String::from("fields"), Value::Array(names));
            Value::Object(map)
        }
        Place::Key => Value::String(String::from("key")),
        Place::KeyPrefix => Value::String(String::from("key_prefix")),
    };
    let on_delete = match reference.on_delete {
        OnDelete::Restrict => "restrict",
        OnDelete::Cascade => "cascade",
        OnDelete::SetNull => "set_null",
    };
    let mut map = BTreeMap::new();
    map.insert(String::from("targets"), Value::Array(targets));
    map.insert(String::from("discriminator"), discriminator);
    map.insert(String::from("place"), place);
    map.insert(
        String::from("on_delete"),
        Value::String(String::from(on_delete)),
    );
    Ok(Value::Object(map))
}

fn definition_value(definition: &Definition) -> Result<Value> {
    let mut map = BTreeMap::new();
    map.insert(
        String::from("name"),
        Value::String(String::from(definition.name())),
    );
    // Only methods that declare a deadline carry it, so other manifests
    // and code identities stay unchanged.
    if let Definition::Method(Method {
        timeout_ms: Some(timeout_ms),
        ..
    }) = definition
    {
        map.insert(
            String::from("timeout_ms"),
            Value::Number(f64::from(*timeout_ms)),
        );
    }
    match definition {
        Definition::Method(method) if method.is_query() => {
            map.insert(String::from("kind"), Value::String(String::from("query")));
            map.insert(String::from("args"), schema_value(&method.args, 0)?);
            let consistency = method.consistency;
            map.insert(
                String::from("consistency"),
                Value::String(String::from(match consistency {
                    crate::app::Consistency::Linearizable => "linearizable",
                    crate::app::Consistency::ReplicaLocal
                    | crate::app::Consistency::ReplicaLocalWithin(_) => "replica_local",
                })),
            );
            if let crate::app::Consistency::ReplicaLocalWithin(max_staleness_ms) = consistency {
                map.insert(
                    String::from("max_staleness_ms"),
                    Value::Number(f64::from(max_staleness_ms)),
                );
            }
            insert_contract(&mut map, &method.result, &method.errors)?;
        }
        Definition::Method(method) => {
            map.insert(
                String::from("kind"),
                Value::String(String::from("mutation")),
            );
            map.insert(String::from("args"), schema_value(&method.args, 0)?);
            insert_contract(&mut map, &method.result, &method.errors)?;
        }
        Definition::Derived(derived) => {
            map.insert(String::from("kind"), Value::String(String::from("derived")));
            map.insert(String::from("args"), schema_value(&derived.args, 0)?);
            insert_contract(&mut map, &derived.result, &derived.errors)?;
            map.insert(
                String::from("materialize"),
                Value::String(String::from(match derived.materialize {
                    Materialize::Never => "never",
                    Materialize::Always => "always",
                    Materialize::Each(_) => "each",
                })),
            );
            if let Materialize::Each(source) = derived.materialize {
                map.insert(
                    String::from("source"),
                    Value::String(String::from(source.name)),
                );
            }
            if let Some(rule) = derived.access {
                map.insert(String::from("access"), rule().to_value());
            }
            if let Some(aggregation) = &derived.aggregate {
                let mut entry = BTreeMap::new();
                entry.insert(
                    String::from("collection"),
                    Value::String(String::from(aggregation.collection)),
                );
                entry.insert(
                    String::from("index"),
                    Value::String(String::from(aggregation.index)),
                );
                if let Some(version) = aggregation.version {
                    entry.insert(
                        String::from("version"),
                        Value::String(String::from(version)),
                    );
                }
                map.insert(String::from("aggregate"), Value::Object(entry));
            }
        }
        Definition::Transaction(transaction) => {
            map.insert(
                String::from("kind"),
                Value::String(String::from("transaction")),
            );
            map.insert(String::from("args"), schema_value(&transaction.args, 0)?);
        }
    }
    Ok(Value::Object(map))
}

/// Encode a schema structurally. Unset bounds are absent, not null.
pub(crate) fn schema_value(schema: &Schema, depth: usize) -> Result<Value> {
    if depth > crate::value::MAX_VALUE_DEPTH {
        return Err(fail(codes::DEPTH_LIMIT, "schema too deep"));
    }
    let mut map = BTreeMap::new();
    match schema {
        Schema::DecimalI64 => {
            map.insert(
                String::from("type"),
                Value::String(String::from("decimal_i64")),
            );
        }
        Schema::DecimalU64 => {
            map.insert(
                String::from("type"),
                Value::String(String::from("decimal_u64")),
            );
        }
        Schema::String {
            min_length,
            max_length,
            pattern,
        } => {
            map.insert(String::from("type"), Value::String(String::from("string")));
            if let Some(min) = min_length {
                map.insert(String::from("min"), Value::Number(f64::from(*min)));
            }
            if let Some(max) = max_length {
                map.insert(String::from("max"), Value::Number(f64::from(*max)));
            }
            if let Some(pattern) = pattern {
                map.insert(
                    String::from("pattern"),
                    Value::String(String::from(*pattern)),
                );
            }
        }
        Schema::Number { min, max } => {
            map.insert(String::from("type"), Value::String(String::from("number")));
            if let Some(min) = min {
                map.insert(String::from("min"), Value::Number(*min));
            }
            if let Some(max) = max {
                map.insert(String::from("max"), Value::Number(*max));
            }
        }
        Schema::Int { min, max } => {
            map.insert(String::from("type"), Value::String(String::from("int")));
            if let Some(min) = min {
                map.insert(String::from("min"), Value::Number(*min));
            }
            if let Some(max) = max {
                map.insert(String::from("max"), Value::Number(*max));
            }
        }
        Schema::Boolean => {
            map.insert(String::from("type"), Value::String(String::from("boolean")));
        }
        Schema::Null => {
            map.insert(String::from("type"), Value::String(String::from("null")));
        }
        Schema::Json => {
            map.insert(String::from("type"), Value::String(String::from("json")));
        }
        Schema::Array { element } => {
            map.insert(String::from("type"), Value::String(String::from("array")));
            map.insert(String::from("element"), schema_value(element, depth + 1)?);
        }
        Schema::Tuple { elements } => {
            map.insert(String::from("type"), Value::String(String::from("tuple")));
            let mut items = Vec::with_capacity(elements.len());
            for element in elements.iter() {
                items.push(schema_value(element, depth + 1)?);
            }
            map.insert(String::from("elements"), Value::Array(items));
        }
        Schema::Object { fields } => {
            map.insert(String::from("type"), Value::String(String::from("object")));
            let mut items = Vec::with_capacity(fields.len());
            for field in fields.iter() {
                let mut entry = BTreeMap::new();
                entry.insert(
                    String::from("name"),
                    Value::String(String::from(field.name)),
                );
                entry.insert(
                    String::from("schema"),
                    schema_value(&field.schema, depth + 1)?,
                );
                entry.insert(String::from("optional"), Value::Bool(field.optional));
                items.push(Value::Object(entry));
            }
            map.insert(String::from("fields"), Value::Array(items));
        }
        Schema::Nullable { inner } => {
            map.insert(
                String::from("type"),
                Value::String(String::from("nullable")),
            );
            map.insert(String::from("of"), schema_value(inner, depth + 1)?);
        }
        Schema::Enum { variants } => {
            map.insert(String::from("type"), Value::String(String::from("enum")));
            let mut items = Vec::with_capacity(variants.len());
            for variant in variants.iter() {
                items.push(Value::String(String::from(*variant)));
            }
            map.insert(String::from("variants"), Value::Array(items));
        }
    }
    Ok(Value::Object(map))
}

/// Emit the guest ABI for an application: `flower_alloc`, `flower_invoke`,
/// `flower_manifest` and `flower_init`.
#[macro_export]
macro_rules! export {
    ($app:expr) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn flower_alloc(size: u32) -> *mut u8 {
            $crate::abi::guest_alloc(size)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn flower_invoke(kind: u32, in_ptr: *const u8, in_len: u32) -> u64 {
            $crate::abi::invoke_bytes(&$app, kind, in_ptr, in_len)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn flower_manifest() -> u64 {
            $crate::abi::manifest_bytes(&$app)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn flower_init() -> u64 {
            $crate::abi::init_bytes(&$app)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{App, Consistency, Definition, Derived, Method, Transaction};
    use crate::ctx::{CallFailure, Ctx};
    use crate::error::{codes, fail};
    use crate::schema::v;
    use crate::value::Value;
    use alloc::string::String;

    fn echo(_ctx: &mut Ctx<'_, '_>, args: Value) -> Result<Value> {
        Ok(args)
    }

    fn failer(_ctx: &mut Ctx<'_, '_>, _args: Value) -> Result<Value> {
        Err(fail(codes::INVALID_VALUE, "always fails"))
    }

    fn planner(args: Value) -> Result<crate::app::Plan> {
        let target = crate::app::Target::partition("acme")?;
        let call = target.call("put", args)?;
        Ok(crate::app::Plan {
            calls: alloc::vec![call],
            value: None,
        })
    }

    static ECHO: Method = Method::query("echo", echo);
    static WRITE: Method = Method::mutation("write", failer);
    static DOUBLE: Derived = Derived::new("double", echo);
    static MOVE: Transaction = Transaction::new("move", planner);

    static DEFINITIONS: [&dyn crate::app::Component; 4] = [&ECHO, &WRITE, &DOUBLE, &MOVE];

    static APP: App = App::new(&[&DEFINITIONS]);

    #[test]
    fn op_numbers_are_stable() {
        assert_eq!(HostOp::Get as u32, 1);
        assert_eq!(HostOp::Caller as u32, 19);
        assert_eq!(HostOp::Enqueue as u32, 20);
        assert_eq!(HostOp::TimerSchedule as u32, 21);
        assert_eq!(HostOp::TimerRetry as u32, 25);
        assert_eq!(HostOp::QueueClaim as u32, 26);
        assert_eq!(HostOp::QueueStats as u32, 32);
        assert_eq!(HostOp::QueuePurge as u32, 34);
        for op in 1..=36 {
            let decoded = HostOp::from_u32(op).expect("every number up to the last decodes");
            assert_eq!(decoded as u32, op);
        }
        assert!(HostOp::from_u32(0).is_none());
        assert!(HostOp::from_u32(37).is_none());
        assert_eq!(
            InvocationKind::from_u32(3),
            Some(InvocationKind::Transaction)
        );
        assert_eq!(InvocationKind::Reduce as u32, 5);
        assert_eq!(InvocationKind::from_u32(5), Some(InvocationKind::Reduce));
        assert!(InvocationKind::from_u32(0).is_none());
        assert!(InvocationKind::from_u32(6).is_none());
    }

    fn reduce_twice(args: Value) -> Result<Value> {
        Ok(Value::Array(alloc::vec![args.clone(), args]))
    }

    fn due_at_five(_ctx: &mut Ctx<'_, '_>) -> Result<Option<f64>> {
        Ok(Some(5.0))
    }

    fn ran(_ctx: &mut Ctx<'_, '_>) -> Result<Value> {
        Ok(Value::String(String::from("ran")))
    }

    fn handled(_ctx: &mut Ctx<'_, '_>, failure: &crate::app::TaskFailure) -> Result<Value> {
        Ok(Value::String(alloc::format!(
            "{}@{}",
            failure.error.code,
            failure.failed_at
        )))
    }

    #[test]
    fn reducers_and_tasks_dispatch_by_kind() {
        static SUM: Derived = Derived::new("sum", echo).aggregate(crate::app::Aggregation {
            collection: "orders",
            index: "by_shop",
            reduce: reduce_twice,
            version: Some("2"),
        });
        static TICK: crate::app::Task =
            crate::app::Task::new("tick", due_at_five, ran).on_error(handled);
        static PLAIN: crate::app::Task = crate::app::Task::new("plain", due_at_five, ran);
        static USES: [&dyn crate::app::Component; 2] = [&TICK, &PLAIN];
        static DEFS: [&dyn crate::app::Component; 2] = [&SUM, &DOUBLE];
        static APP: App = App::new(&[&USES, &DEFS]);
        let one = Value::Number(1.0);
        // The reducer, not the build, answers a reduce; plain derived
        // values have none.
        assert_eq!(
            dispatch(&APP, InvocationKind::Reduce, "sum", one.clone()).unwrap(),
            Value::Array(alloc::vec![one.clone(), one.clone()])
        );
        assert_eq!(
            dispatch(&APP, InvocationKind::Derived, "sum", one.clone()).unwrap(),
            one
        );
        assert!(dispatch(&APP, InvocationKind::Reduce, "double", one.clone()).is_err());
        // A task: due as a query, run as a mutation, failures to its
        // error callback.
        assert_eq!(
            dispatch(&APP, InvocationKind::Query, "tick", Value::Null).unwrap(),
            Value::Number(5.0)
        );
        assert_eq!(
            dispatch(&APP, InvocationKind::Mutation, "tick", Value::Null).unwrap(),
            Value::String(String::from("ran"))
        );
        let mut failure = BTreeMap::new();
        failure.insert(
            String::from("error"),
            encode_err(&fail("BOOM", "no")).as_array().unwrap()[1].clone(),
        );
        failure.insert(String::from("failed_at"), Value::Number(7.0));
        assert_eq!(
            dispatch(
                &APP,
                InvocationKind::Mutation,
                "tick",
                Value::Object(failure.clone())
            )
            .unwrap(),
            Value::String(String::from("BOOM@7"))
        );
        assert!(
            dispatch(
                &APP,
                InvocationKind::Mutation,
                "plain",
                Value::Object(failure)
            )
            .is_err()
        );
        // The manifest declares both.
        let Value::Object(manifest) = manifest_value(&APP).unwrap() else {
            panic!("manifest is an object");
        };
        let Some(Value::Array(tasks)) = manifest.get("tasks") else {
            panic!("manifest without tasks");
        };
        assert_eq!(tasks.len(), 2);
        let Some(Value::Array(definitions)) = manifest.get("definitions") else {
            panic!("manifest without definitions");
        };
        let Value::Object(sum) = &definitions[0] else {
            panic!("definition is an object");
        };
        let Some(Value::Object(aggregate)) = sum.get("aggregate") else {
            panic!("aggregate without its member");
        };
        assert_eq!(
            aggregate.get("index"),
            Some(&Value::String(String::from("by_shop")))
        );
        assert_eq!(
            aggregate.get("version"),
            Some(&Value::String(String::from("2")))
        );
        // Manifests without tasks carry no member.
        let Value::Object(plain) = manifest_value(&super::tests::APP).unwrap() else {
            panic!("manifest is an object");
        };
        assert!(!plain.contains_key("tasks"));
    }

    #[test]
    fn queries_carry_their_consistency_into_the_manifest() {
        let encoded = |consistency| {
            let method: &'static Method =
                alloc::boxed::Box::leak(alloc::boxed::Box::new(ECHO.consistency(consistency)));
            let Value::Object(map) = definition_value(&Definition::Method(method)).unwrap() else {
                panic!("definition is an object");
            };
            (
                map.get("consistency").cloned(),
                map.get("max_staleness_ms").cloned(),
            )
        };
        let text = |text: &str| Some(Value::String(String::from(text)));
        assert_eq!(
            encoded(Consistency::Linearizable),
            (text("linearizable"), None)
        );
        assert_eq!(
            encoded(Consistency::ReplicaLocal),
            (text("replica_local"), None)
        );
        assert_eq!(
            encoded(Consistency::ReplicaLocalWithin(250)),
            (text("replica_local"), Some(Value::Number(250.0)))
        );
    }

    #[test]
    fn envelopes_round_trip() {
        let ok = encode_ok(Value::Number(1.0));
        assert_eq!(decode_envelope(ok).unwrap(), Value::Number(1.0));
        let failure = fail("NOPE", "no");
        let err = encode_err(&failure);
        assert_eq!(decode_envelope(err).unwrap_err(), failure);
        let recovered = encode_recovered(&failure);
        assert!(decode_envelope(recovered).is_err());
        match decode_call_envelope(encode_recovered(&failure)).unwrap_err() {
            CallFailure {
                failure: decoded,
                recovered: true,
            } => assert_eq!(decoded, failure),
            CallFailure {
                recovered: false, ..
            } => panic!("expected recovery"),
        }
        assert!(decode_envelope(Value::Null).is_err());
    }

    #[test]
    fn dispatch_routes_and_validates() {
        let args = Value::String(String::from("hi"));
        assert_eq!(
            dispatch(&APP, InvocationKind::Query, "echo", args.clone()).unwrap(),
            args
        );
        assert!(dispatch(&APP, InvocationKind::Query, "missing", args.clone()).is_err());
        assert!(dispatch(&APP, InvocationKind::Mutation, "echo", args.clone()).is_err());
        assert!(dispatch(&APP, InvocationKind::Mutation, "write", args).is_err());
        let planned = dispatch(
            &APP,
            InvocationKind::Transaction,
            "move",
            Value::String(String::from("a")),
        )
        .unwrap();
        let calls = match planned {
            Value::Object(mut map) => match map.remove("calls") {
                Some(Value::Array(calls)) => calls,
                _ => panic!("plan without calls"),
            },
            _ => panic!("plan not an object"),
        };
        assert_eq!(calls.len(), 1);
    }

    #[test]
    fn dispatch_checks_argument_schemas() {
        static STRICT: Method = Method::query("strict", echo)
            .consistency(Consistency::ReplicaLocal)
            .args(v::string().build());
        static NESTED: Method = Method::mutation("nested", echo).args(v::object(&[v::field(
            "items",
            v::array(&v::object(&[v::field("count", v::int().min(1).build())])),
        )]));
        static DEFS: [&dyn crate::app::Component; 2] = [&STRICT, &NESTED];
        static APP: App = App::new(&[&DEFS]);
        // Arguments the contract refuses answer `INVALID_ARGUMENT`, with the
        // path to the offending value as member names and indexes.
        let refused = dispatch(&APP, InvocationKind::Query, "strict", Value::Null).unwrap_err();
        assert_eq!(refused.code, "INVALID_ARGUMENT");
        let path = |failure: &Failure| {
            failure
                .details
                .as_ref()
                .and_then(|details| details.as_object())
                .and_then(|details| details.get("path"))
                .cloned()
        };
        assert_eq!(path(&refused), Some(Value::Array(alloc::vec![])));
        assert!(
            dispatch(
                &APP,
                InvocationKind::Query,
                "strict",
                Value::String(String::from("ok"))
            )
            .is_ok()
        );
        let count = |count: f64| {
            let mut item = BTreeMap::new();
            item.insert(String::from("count"), Value::Number(count));
            let mut args = BTreeMap::new();
            args.insert(
                String::from("items"),
                Value::Array(alloc::vec![
                    Value::Object(item.clone()),
                    Value::Object({
                        item.insert(String::from("count"), Value::Number(count - 1.0));
                        item
                    }),
                ]),
            );
            Value::Object(args)
        };
        assert!(dispatch(&APP, InvocationKind::Mutation, "nested", count(3.0)).is_ok());
        let refused = dispatch(&APP, InvocationKind::Mutation, "nested", count(1.0)).unwrap_err();
        assert_eq!(refused.code, "INVALID_ARGUMENT");
        assert_eq!(
            path(&refused),
            Some(Value::Array(alloc::vec![
                Value::from("items"),
                Value::Number(1.0),
                Value::from("count"),
            ]))
        );
    }

    /// The HTTP aliases a manifest declares.
    fn aliases(app: &App) -> Vec<String> {
        let manifest = manifest_value(app).unwrap();
        let http = manifest.as_object().unwrap().get("http").unwrap();
        http.as_array()
            .unwrap()
            .iter()
            .map(|entry| match entry.as_object().unwrap().get("alias") {
                Some(Value::String(alias)) => alias.clone(),
                other => panic!("alias not a string: {other:?}"),
            })
            .collect()
    }

    #[test]
    fn nothing_answers_over_http_unless_it_says_so() {
        // A method or a transaction that declares nothing is not reachable
        // over HTTP.
        static QUIET: Method = Method::query("quiet", echo);
        static STILL: Method = Method::mutation("still", echo);
        static PLANNED: Transaction = Transaction::new("planned", planner);
        static DEFS: [&dyn crate::app::Component; 3] = [&QUIET, &STILL, &PLANNED];
        static APP: App = App::new(&[&DEFS]);
        assert!(aliases(&APP).is_empty(), "{:?}", aliases(&APP));
        // Each kind opts in on its own declaration.
        static LOUD: Method = Method::query("loud", echo).http();
        static SHOUTED: Transaction = Transaction::new("shouted", planner).http();
        static OPEN: [&dyn crate::app::Component; 3] = [&QUIET, &LOUD, &SHOUTED];
        static OPENED: App = App::new(&[&OPEN]);
        assert_eq!(aliases(&OPENED), ["loud", "shouted"]);
        // A twin keeps its original's exposure under its own name.
        static TWIN: Method = LOUD.named("loud.local");
        static TWINS: [&dyn crate::app::Component; 2] = [&LOUD, &TWIN];
        static TWINNED: App = App::new(&[&TWINS]);
        assert_eq!(aliases(&TWINNED), ["loud", "loud.local"]);
    }

    #[test]
    fn an_empty_caller_list_refuses_every_module() {
        static SEALED: Method = Method::query("sealed", echo).export_to(&[]);
        static OPEN: Method = Method::query("open", echo).export();
        static DEFS: [&dyn crate::app::Component; 2] = [&SEALED, &OPEN];
        static APP: App = App::new(&[&DEFS]);
        let manifest = manifest_value(&APP).unwrap();
        let exports = manifest
            .as_object()
            .unwrap()
            .get("exports")
            .unwrap()
            .as_object()
            .unwrap();
        let callers = exports.get("callers").unwrap().as_object().unwrap();
        // An empty list is an allowlist naming nobody, not "everyone".
        assert_eq!(callers.get("sealed"), Some(&Value::Array(alloc::vec![])));
        assert_eq!(callers.get("open"), None);
    }

    #[test]
    fn manifest_lists_definitions() {
        let manifest = manifest_value(&APP).unwrap();
        let definitions = match manifest {
            Value::Object(mut map) => match map.remove("definitions") {
                Some(Value::Array(definitions)) => definitions,
                _ => panic!("manifest without definitions"),
            },
            _ => panic!("manifest not an object"),
        };
        assert_eq!(definitions.len(), 4);
    }

    #[test]
    fn schemas_encode_structurally() {
        let owned = alloc::vec::Vec::from([
            v::field("name", v::string().min(1).build()),
            v::optional("nick", v::string().build()),
        ]);
        let schema = v::object(alloc::boxed::Box::leak(owned.into_boxed_slice()));
        let encoded = schema_value(&schema, 0).unwrap();
        let rendered = encoded.canonical_json().unwrap();
        assert!(rendered.contains("\"type\":\"object\""));
        assert!(rendered.contains("\"min\":1"));
        assert!(rendered.contains("\"optional\":true"));
    }

    mod authorization {
        use super::*;
        use crate::access::{FieldAccess, Operand, Policy, Rule};
        use crate::app::{Collection, Component};
        use crate::auth::{
            AUTHORIZE, Access, AuthRequest, Authentication, Authorization, Principal,
        };
        use crate::ctx::QueryCtx;
        use crate::typed::FlowerType;

        fn by_token(
            _ctx: &mut QueryCtx<'_, '_>,
            credentials: &Value,
            _request: &AuthRequest,
        ) -> Result<Option<Principal>> {
            match credentials.as_object().and_then(|map| map.get("token")) {
                Some(Value::String(token)) => Ok(Some(Principal::new(token.clone(), None, None)?)),
                _ => Ok(None),
            }
        }

        fn only_ada(
            _ctx: &mut QueryCtx<'_, '_>,
            principal: Option<&Principal>,
            _args: &Value,
        ) -> Result<bool> {
            Ok(principal.is_some_and(|principal| principal.subject == "ada"))
        }

        static AUTH: Authorization = Authorization {
            authenticate: Authentication::Callback(by_token),
            default: Access::Authenticated,
            sessions: Access::Authenticated,
            methods: &[],
        };
        static GATEWAY: Authorization = Authorization {
            authenticate: Authentication::Gateway,
            default: Access::Public,
            sessions: Access::Public,
            methods: &[],
        };
        static USES: [&dyn Component; 1] = [&AUTH];
        static BOTH: [&dyn Component; 2] = [&AUTH, &GATEWAY];
        static GATED: [&dyn Component; 1] = [&GATEWAY];

        fn owned() -> Policy {
            Policy::new()
                .read(Operand::row(&["owner"]).eq(Operand::subject()))
                .field("secret", FieldAccess::new().read(false))
        }

        fn admins() -> Rule {
            Operand::claim(&["role"]).eq(Operand::literal("admin"))
        }

        static GUARDED: Collection = Collection::new("docs").access(owned);
        static RULED: Derived = Derived::new("ruled", echo).access(admins);

        static OPEN: Method = Method::query("open", echo).access(Access::Public);
        static ADA_ONLY: Method = Method::query("ada_only", echo).access(Access::Check(only_ada));

        fn app(uses: &'static [&'static dyn Component]) -> App {
            let mut components: Vec<&'static dyn Component> = uses.to_vec();
            components.extend([&GUARDED as &'static dyn Component, &RULED, &OPEN, &ADA_ONLY]);
            App::new(Vec::leak(components))
        }

        fn request(method: &str, credentials: Value, principal: Value) -> Value {
            let mut map = BTreeMap::new();
            map.insert(String::from("method"), Value::from(method));
            map.insert(String::from("credentials"), credentials);
            map.insert(String::from("args"), Value::Null);
            map.insert(String::from("principal"), principal);
            Value::Object(map)
        }

        fn token(token: &str) -> Value {
            let mut map = BTreeMap::new();
            map.insert(String::from("token"), Value::from(token));
            Value::Object(map)
        }

        fn subject(decision: &Value) -> Option<String> {
            match decision.as_object().and_then(|map| map.get("principal")) {
                Some(Value::Null) | None => None,
                Some(principal) => Some(Principal::decode(principal.clone()).unwrap().subject),
            }
        }

        #[test]
        fn the_hook_authenticates_then_applies_method_access() {
            let app = app(&USES);
            let hook = |method: &str, credentials: Value| {
                dispatch(
                    &app,
                    InvocationKind::Query,
                    AUTHORIZE,
                    request(method, credentials, Value::Null),
                )
            };
            let bob = hook("read", token("bob")).unwrap();
            assert_eq!(subject(&bob).as_deref(), Some("bob"));
            let anonymous = hook("read", Value::Null).unwrap_err();
            assert_eq!(anonymous.code, codes::UNAUTHENTICATED);
            assert_eq!(subject(&hook("open", Value::Null).unwrap()), None);
            let refused = hook("ada_only", token("bob")).unwrap_err();
            assert_eq!(refused.code, codes::FORBIDDEN);
            let ada = hook("ada_only", token("ada")).unwrap();
            assert_eq!(subject(&ada).as_deref(), Some("ada"));
            // Other invocation kinds never reach the hook.
            let mutation = dispatch(
                &app,
                InvocationKind::Mutation,
                AUTHORIZE,
                request("read", token("bob"), Value::Null),
            );
            assert!(mutation.is_err());
        }

        /// Typed: `{"token": t}` is subject `t`, valid until 5000 ms.
        struct Tokens;

        impl crate::auth::Authenticator for Tokens {
            fn authenticate(
                &self,
                _ctx: &mut QueryCtx<'_, '_>,
                credentials: &Value,
            ) -> Result<crate::auth::Authenticated> {
                let principal = match credentials.as_object().and_then(|map| map.get("token")) {
                    Some(Value::String(token)) => Some(Principal::new(token.clone(), None, None)?),
                    _ => None,
                };
                Ok(crate::auth::Authenticated {
                    principal,
                    until: Some(5000.0),
                })
            }
        }

        static TOKENS: Tokens = Tokens;
        static TYPED: Authorization = Authorization {
            authenticate: Authentication::Typed(&TOKENS),
            default: Access::Public,
            sessions: Access::Authenticated,
            methods: &[],
        };
        static TYPED_USES: [&dyn Component; 1] = [&TYPED];

        fn member<'v>(decision: &'v Value, name: &str) -> Option<&'v Value> {
            decision.as_object().and_then(|map| map.get(name))
        }

        #[test]
        fn decisions_say_whether_they_read_the_arguments_and_when_they_expire() {
            let reads = |app: &App, method: &str, credentials: Value| {
                let decision = dispatch(
                    app,
                    InvocationKind::Query,
                    AUTHORIZE,
                    request(method, credentials, Value::Null),
                )
                .unwrap();
                (
                    member(&decision, "reads_args").cloned(),
                    member(&decision, "until").cloned(),
                )
            };
            // A callback sees the whole request, arguments included.
            let callback = app(&USES);
            assert_eq!(
                reads(&callback, "open", token("bob")),
                (Some(Value::Bool(true)), None)
            );
            // A typed authenticator sees credentials alone and reports its
            // expiry; a check reads the arguments.
            let typed = app(&TYPED_USES);
            assert_eq!(
                reads(&typed, "read", token("bob")),
                (Some(Value::Bool(false)), Some(Value::Number(5000.0)))
            );
            assert_eq!(
                reads(&typed, "ada_only", token("ada")),
                (Some(Value::Bool(true)), Some(Value::Number(5000.0)))
            );
            let gateway = app(&GATED);
            assert_eq!(
                reads(&gateway, "read", Value::Null),
                (Some(Value::Bool(false)), None)
            );
        }

        #[test]
        fn retry_sessions_have_their_own_policy() {
            let typed = app(&TYPED_USES);
            let hook = |method: &str, credentials: Value| {
                dispatch(
                    &typed,
                    InvocationKind::Query,
                    AUTHORIZE,
                    request(method, credentials, Value::Null),
                )
            };
            // Methods are public; sessions need a caller, whatever the
            // methods list says.
            assert_eq!(subject(&hook("read", Value::Null).unwrap()), None);
            for operation in ["open", "status", "ack", "close"] {
                let method = alloc::format!("$flower.session.{operation}");
                let refused = hook(&method, Value::Null).unwrap_err();
                assert_eq!(refused.code, codes::UNAUTHENTICATED, "{method}");
                let bob = hook(&method, token("bob")).unwrap();
                assert_eq!(subject(&bob).as_deref(), Some("bob"));
            }
        }

        #[test]
        fn a_gateway_hook_takes_the_asserted_principal() {
            let app = app(&GATED);
            let asserted = Principal::new(String::from("ada"), None, None)
                .unwrap()
                .encode()
                .unwrap();
            let decision = dispatch(
                &app,
                InvocationKind::Query,
                AUTHORIZE,
                request("read", token("bob"), asserted),
            )
            .unwrap();
            assert_eq!(subject(&decision).as_deref(), Some("ada"));
        }

        fn no_input(_ctx: &mut Ctx<'_, '_>, _args: Value) -> Result<Value> {
            Ok(Value::Null)
        }

        crate::external!(static WORDS = "words", input = no_input, http);

        static MOVE_FUNDS: Transaction =
            Transaction::new("move_funds", planner).access(Access::Check(only_ada));

        /// Callers must authenticate, except where a definition or the
        /// table says otherwise.
        fn granting(methods: &'static [(&'static str, Access)]) -> App {
            let auth: &'static Authorization =
                alloc::boxed::Box::leak(alloc::boxed::Box::new(Authorization {
                    authenticate: Authentication::Callback(by_token),
                    default: Access::Authenticated,
                    sessions: Access::Authenticated,
                    methods,
                }));
            App::new(Vec::leak(alloc::vec![
                auth as &'static dyn Component,
                &WORDS,
                &MOVE_FUNDS,
                &OPEN,
            ]))
        }

        #[test]
        fn transactions_and_generated_definitions_have_their_own_access() {
            let app = granting(&[
                ("words.publish", Access::Check(only_ada)),
                ("words.get", Access::Public),
            ]);
            assert!(manifest_value(&app).is_ok());
            let hook = |method: &str, credentials: Value| {
                dispatch(
                    &app,
                    InvocationKind::Query,
                    AUTHORIZE,
                    request(method, credentials, Value::Null),
                )
            };
            // A transaction's own access, stricter than the default.
            let refused = hook("move_funds", token("bob")).unwrap_err();
            assert_eq!(refused.code, codes::FORBIDDEN);
            assert_eq!(
                subject(&hook("move_funds", token("ada")).unwrap()).as_deref(),
                Some("ada")
            );
            // An external value's operations, from the component's table.
            let refused = hook("words.publish", token("bob")).unwrap_err();
            assert_eq!(refused.code, codes::FORBIDDEN);
            assert!(hook("words.publish", token("ada")).is_ok());
            assert_eq!(subject(&hook("words.get", Value::Null).unwrap()), None);
            // The rest take the default.
            let anonymous = hook("words.claim", Value::Null).unwrap_err();
            assert_eq!(anonymous.code, codes::UNAUTHENTICATED);
        }

        #[test]
        fn a_refused_manifest_says_why() {
            // The build reads the manifest through `flower_manifest`: a
            // refusal must reach it, not an empty answer the host can only
            // call "guest cannot respond".
            let app = granting(&[("words.publsh", Access::Public)]);
            let bytes = manifest_native(&app);
            let envelope = Value::decode_binary(&bytes).expect("an envelope");
            let failure = decode_envelope(envelope).unwrap_err();
            assert!(failure.message.contains("words.publsh"), "{failure:?}");
        }

        #[test]
        fn the_access_table_names_definitions_that_declare_none() {
            let refused = |methods: &'static [(&'static str, Access)]| {
                manifest_value(&granting(methods)).unwrap_err().message
            };
            // Not a definition of the app: a typo would leave it at the
            // default.
            assert!(refused(&[("words.publsh", Access::Public)]).contains("words.publsh"));
            // Derived values and collections are not called.
            assert!(refused(&[("words.input", Access::Public)]).contains("not a method"));
            // A definition that declares its own access keeps it.
            assert!(refused(&[("move_funds", Access::Public)]).contains("its own access"));
            assert!(refused(&[("open", Access::Public)]).contains("its own access"));
            assert!(
                refused(&[
                    ("words.get", Access::Public),
                    ("words.get", Access::Authenticated),
                ])
                .contains("twice")
            );
        }

        #[test]
        fn manifests_carry_hooks_policies_and_derived_rules() {
            let manifest = manifest_value(&app(&USES)).unwrap();
            let map = manifest.as_object().unwrap();
            assert_eq!(map.get("authorize"), Some(&Value::Bool(true)));
            let collections = map.get("collections").unwrap().as_array().unwrap();
            let access = collections[0].as_object().unwrap().get("access").unwrap();
            assert_eq!(access, &owned().to_value());
            let definitions = map.get("definitions").unwrap().as_array().unwrap();
            let rule = definitions[0].as_object().unwrap().get("access").unwrap();
            assert_eq!(rule, &admins().to_value());
            // No component, no hook; two are ambiguous.
            let plain = manifest_value(&APP).unwrap();
            assert_eq!(
                plain.as_object().unwrap().get("authorize"),
                Some(&Value::Bool(false))
            );
            assert!(manifest_value(&app(&BOTH)).is_err());
            // Methods that declare access need a component to decide it.
            let refused = manifest_value(&app(&[])).unwrap_err();
            assert!(
                refused.message.contains("declare access"),
                "{}",
                refused.message
            );
        }
    }
}
