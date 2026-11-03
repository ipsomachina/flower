//! Guest invocation backends.
//!
//! [`Runtime`] invokes guest queries, mutations, derived values and
//! transaction planners; guests call back through [`HostCall`], whose
//! output is always one binary envelope. [`ClosureRuntime`] drives
//! in-process closures for tests and native workers. [`wasm`] runs real
//! Wasm guests: each call gets a new instance of the module's initialized
//! image in a recycled slot of Wasmtime's pooling allocator.

use std::collections::HashMap;

use flower_sdk::abi::{InvocationKind, decode_envelope};
use flower_sdk::{CallFailure, Failure, Value, fail};

use crate::error::HostResult;

/// Guest entry-point kind for one call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    /// Query definition.
    Query,
    /// Mutation definition.
    Mutation,
    /// Derived definition.
    Derived,
    /// An aggregate derived definition's reducer: no context, every
    /// host call refused.
    Reduce,
}

impl CallKind {
    fn invocation(self) -> InvocationKind {
        match self {
            CallKind::Query => InvocationKind::Query,
            CallKind::Mutation => InvocationKind::Mutation,
            CallKind::Derived => InvocationKind::Derived,
            CallKind::Reduce => InvocationKind::Reduce,
        }
    }
}

/// Raw host-call handler: binary request in, binary envelope out. The
/// output buffer must stay valid until the next call on the same handler.
pub trait HostCall {
    /// Handle one host call, returning binary envelope bytes.
    fn op(&mut self, op: u8, input: &[u8]) -> Vec<u8>;

    /// The guest is about to read state outside the store, which no
    /// dependency names.
    fn read_untracked(&mut self) {}

    /// The guest is about to resolve a managed key: a crypto op reading
    /// the key catalog, whose changes move no database revision.
    /// Handlers that track nothing finer treat it as untracked.
    fn read_keys(&mut self) {
        self.read_untracked();
    }
}

/// Whether a crypto op resolves keys through the database's managed-key
/// catalog rather than computing over its inputs alone: signing,
/// verifying, public keys, versions and every catalog operation.
pub fn crypto_reads_keys(op: u32) -> bool {
    use flower_sdk::crypto::CryptoOp;
    [
        CryptoOp::Sign,
        CryptoOp::Verify,
        CryptoOp::PublicKey,
        CryptoOp::KeyVersion,
        CryptoOp::Managed,
    ]
    .into_iter()
    .any(|reading| reading as u32 == op)
}

/// Guest invocation backend. Business failures return `recovered: true`
/// (savepoints may catch them); traps, budget exhaustion and missing
/// definitions return `recovered: false` and poison the invocation.
/// Invocation borrows shared: guests re-enter the same backend through
/// nested calls, so backends keep no call-scoped `&mut` state. Backends
/// are `Sync`: concurrent queries on different threads call one backend
/// at once.
pub trait Runtime: Send + Sync {
    /// Invoke a named definition with a re-entrant host handler.
    fn call(
        &self,
        kind: CallKind,
        module: &str,
        name: &str,
        args: &Value,
        host: &mut dyn HostCall,
    ) -> Result<Value, CallFailure>;

    /// Run a transaction planner without a guest context.
    fn plan(&self, module: &str, name: &str, args: &Value) -> Result<Value, CallFailure>;
}

/// In-process backend driving a closure: tests and native workers run
/// guest logic as Rust without Wasm.
pub struct ClosureRuntime<F>
where
    F: Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> HostResult<Value>,
{
    invoke: F,
}

impl<F> ClosureRuntime<F>
where
    F: Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> HostResult<Value>,
{
    /// Wrap an invocation closure. Closure failures are business
    /// failures (`recovered: true`); closures cannot trap. Plain `Fn`
    /// keeps nested calls into the same runtime re-entrant; closures
    /// needing mutable state use short interior borrows.
    pub fn new(invoke: F) -> Self {
        ClosureRuntime { invoke }
    }
}

impl<F> Runtime for ClosureRuntime<F>
where
    F: Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> HostResult<Value> + Send + Sync,
{
    fn call(
        &self,
        kind: CallKind,
        module: &str,
        name: &str,
        args: &Value,
        host: &mut dyn HostCall,
    ) -> Result<Value, CallFailure> {
        (self.invoke)(kind, module, name, args, host).map_err(|failure| CallFailure {
            failure,
            recovered: true,
        })
    }

    fn plan(&self, _module: &str, _name: &str, _args: &Value) -> Result<Value, CallFailure> {
        Err(CallFailure {
            failure: fail("invalid_invocation", "closure runtimes run no planners"),
            recovered: false,
        })
    }
}

/// In-process backend running an SDK [`App`][flower_sdk::App] natively:
/// the guest's own dispatch, codecs and host operations without Wasm.
/// Host calls reach the invoking [`HostCall`] through the SDK's native
/// bridge ([`flower_sdk::abi::native`]); nested calls re-enter here and
/// install their own host. Guest failures are business failures, as for
/// Wasm guests; a guest that cannot respond, or panics, is not caught.
pub struct NativeRuntime {
    app: &'static flower_sdk::App,
    crypto: Option<std::sync::Arc<dyn flower_sdk::crypto::CryptoBackend>>,
}

impl NativeRuntime {
    /// Run `app` in-process.
    pub fn new(app: &'static flower_sdk::App) -> Self {
        NativeRuntime { app, crypto: None }
    }

    /// Attach the crypto dispatch guests reach through `crypto_call`.
    pub fn with_crypto(
        mut self,
        crypto: std::sync::Arc<dyn flower_sdk::crypto::CryptoBackend>,
    ) -> Self {
        self.crypto = Some(crypto);
        self
    }

    /// The application's manifest, as its `flower_manifest` answers.
    pub fn manifest(&self) -> Result<Value, Failure> {
        let bytes = flower_sdk::abi::manifest_native(self.app);
        decode_envelope(Value::decode_binary(&bytes)?)
    }
}

/// One invocation's side of the native bridge.
struct NativeBridge<'a> {
    host: &'a mut dyn HostCall,
    crypto: Option<&'a dyn flower_sdk::crypto::CryptoBackend>,
    kind: InvocationKind,
}

impl flower_sdk::abi::native::NativeHost for NativeBridge<'_> {
    fn call(&mut self, import: flower_sdk::abi::native::Import, op: u32, input: &[u8]) -> Vec<u8> {
        use flower_sdk::abi::native::Import;
        let outcome = match import {
            Import::Host => match u8::try_from(op) {
                Ok(op) => return self.host.op(op, input),
                Err(_) => Err(fail(
                    flower_sdk::error::codes::INVALID_INVOCATION,
                    "bad host op",
                )),
            },
            // Entropy stays mutation-only, as for Wasm guests.
            Import::Crypto
                if op == flower_sdk::crypto::CryptoOp::RandomBytes as u32
                    && self.kind != InvocationKind::Mutation =>
            {
                Err(fail(
                    flower_sdk::error::codes::INVALID_INVOCATION,
                    "random_bytes is only available to mutations",
                ))
            }
            Import::Crypto => match self.crypto {
                Some(crypto) => {
                    if crypto_reads_keys(op) {
                        self.host.read_keys();
                    }
                    crypto.crypto_call(op, input)
                }
                None => Err(fail(
                    flower_sdk::error::codes::INVALID_INVOCATION,
                    "no crypto manager is configured",
                )),
            },
        };
        match outcome {
            Ok(bytes) => bytes,
            Err(failure) => flower_sdk::abi::encode_err(&failure)
                .encode_binary()
                .unwrap_or_default(),
        }
    }
}

impl NativeRuntime {
    fn invoke(
        &self,
        kind: InvocationKind,
        name: &str,
        args: &Value,
        host: &mut dyn HostCall,
    ) -> Result<Value, CallFailure> {
        let fatal = |message: &str| CallFailure {
            failure: fail("host_trap", message),
            recovered: false,
        };
        if let Some(init) = self.app.init
            && let Err(failure) = init()
        {
            return Err(CallFailure {
                failure: fail("init_failed", failure.message),
                recovered: false,
            });
        }
        let request = Value::Array(vec![Value::String(name.to_string()), args.clone()]);
        let input = request
            .encode_binary()
            .map_err(|failure| fatal(&failure.message))?;
        let invocation_crypto = self
            .crypto
            .as_ref()
            .and_then(|crypto| crypto.for_invocation());
        let mut bridge = NativeBridge {
            host,
            crypto: invocation_crypto.as_deref().or(self.crypto.as_deref()),
            kind,
        };
        let app = self.app;
        let bytes = flower_sdk::abi::native::with_host(&mut bridge, || {
            flower_sdk::abi::invoke_native(app, kind as u32, &input)
        });
        if bytes.is_empty() {
            return Err(fatal("guest did not respond"));
        }
        let envelope = Value::decode_binary(&bytes).map_err(|failure| fatal(&failure.message))?;
        decode_envelope(envelope).map_err(|failure| CallFailure {
            failure,
            recovered: true,
        })
    }
}

impl Runtime for NativeRuntime {
    fn call(
        &self,
        kind: CallKind,
        _module: &str,
        name: &str,
        args: &Value,
        host: &mut dyn HostCall,
    ) -> Result<Value, CallFailure> {
        self.invoke(kind.invocation(), name, args, host)
    }

    fn plan(&self, _module: &str, name: &str, args: &Value) -> Result<Value, CallFailure> {
        struct NoHost;
        impl HostCall for NoHost {
            fn op(&mut self, _op: u8, _input: &[u8]) -> Vec<u8> {
                flower_sdk::abi::encode_err(&fail(
                    flower_sdk::error::codes::INVALID_INVOCATION,
                    "planners run without a context",
                ))
                .encode_binary()
                .unwrap_or_default()
            }
        }
        self.invoke(InvocationKind::Transaction, name, args, &mut NoHost)
    }
}

/// Record a host-side envelope failure for a missing definition or module.
pub fn unknown_method(name: &str) -> CallFailure {
    CallFailure {
        failure: fail(
            flower_sdk::error::codes::UNKNOWN_METHOD,
            format!("unknown definition `{name}`"),
        ),
        recovered: false,
    }
}

/// wasmtime backend: a new instance of the module's initialized image per
/// call.
///
/// Registration prepares the module's *image* once per process: it runs
/// the optional `flower_init` in an instance without host or crypto
/// capabilities and writes the module back with the memory and mutable
/// globals that left as its initial state. Images are compiled and
/// linked once, then shared by every backend through a bounded cache
/// keyed by the module's bytes; backends keep their modules' bytes and
/// prepare again after an eviction.
///
/// Every call — nested ones included — instantiates the image in a new
/// store, allocates the request with the guest's `flower_alloc`, invokes
/// `flower_invoke`, reads the packed response out of guest memory and
/// drops the store, whatever the outcome: nothing a call writes reaches
/// another call, principal or database. Instances live in slots of
/// Wasmtime's pooling allocator, which gives an image the slots that last
/// held it and restores a slot's memory to its image when the store drops
/// ([`EngineSettings`]). A zero response or any trap is fatal.
/// Guests importing `crypto_call` fail instantiation without a crypto
/// manager. The backend links the guest's `host_call` import under
/// whatever module the guest declares it — normally `flower`.
///
/// Every call runs under [`Limits`]: instruction fuel, a wall-clock
/// deadline enforced by epoch interruption, a linear-memory and table
/// limiter and a bounded Wasm stack. Fuel, deadline and memory are shared
/// across the nested calls of one invocation, so recursing through the
/// host cannot escape them; exceeding any limit fails the invocation with
/// `recovered: false` and a distinct code. Logical budgets live in the
/// session.
pub mod wasm {
    use super::*;

    use std::cell::RefCell;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock, Weak};
    use std::time::{Duration, Instant};

    use sha2::Digest as _;

    mod cache;
    mod code;
    mod image;

    pub use cache::CacheStats;

    /// Instruction fuel a declared millisecond buys: more than a guest
    /// executes in one on a fast core (a guest calling the host in a
    /// tight loop burns three to four billion units a second), so the
    /// deadline, not the fuel, ends a method that uses its time.
    pub const INSTRUCTIONS_PER_MS: u64 = 10_000_000;
    /// Instruction fuel an invocation gets for the default deadline,
    /// shared by its nested guest calls: an invocation's fuel scales with
    /// its deadline from this ([`Limits::fuel_for`]), so nobody states
    /// fuel, and a given method always gets the same.
    pub const FUEL_PER_CALL: u64 = INSTRUCTIONS_PER_MS * 5_000;
    /// Largest host-call message accepted from a guest.
    pub const MAX_HOST_MESSAGE: usize = 64 * 1024 * 1024;
    /// Wasm stack per guest call: deeper guest recursion traps with
    /// [`STACK_OVERFLOW`] instead of exhausting the host thread's stack.
    pub const MAX_WASM_STACK: usize = 512 * 1024;
    /// Epoch tick period. Each tick checks a running guest's deadline
    /// against the clock, so a deadline ends a guest never early and at
    /// most about a tick late, plus however late the OS wakes the ticker
    /// (and, for a guest inside a host call, when the call returns to
    /// it). The ticker runs only while a guest does.
    pub const EPOCH_TICK: Duration = Duration::from_millis(1);
    /// Default linear-memory cap per guest instance. The SDK reserves a
    /// 64 MiB bump heap, so instances start just above 64 MiB.
    pub const DEFAULT_INSTANCE_MEMORY: usize = 128 * 1024 * 1024;
    /// Default linear-memory cap summed over every live instance of one
    /// invocation, nested calls included.
    pub const DEFAULT_INVOCATION_MEMORY: usize = 1024 * 1024 * 1024;
    /// Default element cap per guest table.
    pub const DEFAULT_TABLE_ELEMENTS: usize = 65_536;
    /// Default wall-clock deadline per invocation, nested calls included:
    /// what a method that declares none gets.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
    /// Default ceiling on the deadline a method may declare.
    pub const DEFAULT_MAX_TIMEOUT: Duration = Duration::from_secs(60);
    /// Deepest guest-call nesting one invocation may reach.
    pub const MAX_NESTING: usize = 32;
    /// Native stack a nested entry may find already used since the
    /// invocation's outermost entry. Each nested store may use another
    /// [`MAX_WASM_STACK`] below that point, so this keeps nesting inside
    /// a 2 MiB worker stack.
    const NESTED_STACK_BUDGET: usize = 768 * 1024;

    /// Store kind for instances outside any invocation: validation and
    /// manifests. They draw no entropy and may lack a crypto manager.
    const NO_INVOCATION: u32 = 0;

    /// Failure code: instruction fuel exhausted.
    pub const FUEL_EXHAUSTED: &str = "budget_exhausted";
    /// Failure code: the call ran past its own deadline (what it, or the
    /// transaction it runs in, declares, else the default): a limit it
    /// ran out of, its own failure, as v1's `EVALUATION_BUDGET` was.
    pub const DEADLINE_EXCEEDED: &str = "deadline_exceeded";
    /// Failure code: the caller's deadline passed first. The caller
    /// stopped waiting; the call did not run out of its own time, and an
    /// attempt with more time may succeed.
    pub const CALLER_DEADLINE_EXCEEDED: &str = "caller_deadline_exceeded";
    /// Failure code: linear memory or a table grew past its limit.
    pub const OUT_OF_MEMORY: &str = "out_of_memory";
    /// Failure code: the Wasm or native stack budget ran out.
    pub const STACK_OVERFLOW: &str = "stack_overflow";
    /// Failure code: nested guest calls went deeper than [`MAX_NESTING`].
    pub const NESTING_TOO_DEEP: &str = "nesting_too_deep";
    /// Failure code: `flower_init` reported a failure.
    pub const INIT_FAILED: &str = "init_failed";
    /// Failure code: every slot of the pooling allocator is in use.
    pub const INSTANCES_EXHAUSTED: &str = "instances_exhausted";
    /// Failure code: any other trap or ABI violation in the guest.
    pub const HOST_TRAP: &str = "host_trap";
    /// Failure code: an instance could not start on this member (no
    /// crypto manager here, an allocation that failed). The host's, not
    /// the call's: another attempt or member may start it.
    pub const INSTANTIATION_FAILED: &str = "instantiation_failed";

    /// Default slots of the pooling allocator: guest instances live at
    /// once across the process, nested calls each holding one.
    pub const DEFAULT_POOL_SLOTS: u32 = 1000;
    /// Default bytes of written memory a released slot restores in place
    /// and keeps resident where Linux reports written pages
    /// (`PAGEMAP_SCAN`); elsewhere, and past this, the kernel takes the
    /// pages back.
    pub const DEFAULT_KEEP_RESIDENT: usize = 1024 * 1024;
    /// Default weight the image cache keeps: compiled code plus image
    /// modules, in bytes.
    pub const DEFAULT_IMAGE_CACHE_BYTES: usize = 512 * 1024 * 1024;
    /// Default bytes of compiled functions kept for reuse by later
    /// compilations.
    pub const DEFAULT_CODE_CACHE_BYTES: usize = 64 * 1024 * 1024;
    /// Most tables one pooled instance may define.
    const TABLES_PER_INSTANCE: u32 = 16;
    /// Native stack for image preparation, which compiles and runs
    /// `flower_init` on a thread of its own.
    const PREPARE_STACK: usize = 8 * 1024 * 1024;

    /// Resource limits for guest calls. The outermost call of an
    /// invocation sets the shared fuel, deadline and aggregate memory;
    /// every instance, nested ones included, also obeys its own
    /// backend's per-instance memory and table caps.
    ///
    /// The deadline is what the called method declares (its manifest's
    /// `timeout_ms`), or `timeout` when it declares none, never past
    /// `max_timeout`, and cut short by the caller's own deadline. The
    /// instruction fuel follows the declared deadline alone, not the
    /// caller's or this member's configuration, so a method's outcome
    /// never depends on who calls it or which member runs it.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Limits {
        /// Linear-memory bytes one instance may reach.
        pub instance_memory: usize,
        /// Linear-memory bytes all live instances of one invocation may
        /// reach together.
        pub invocation_memory: usize,
        /// Elements one table may reach.
        pub table_elements: usize,
        /// Wall-clock deadline for one invocation whose method declares
        /// none.
        pub timeout: Duration,
        /// The longest deadline a method may declare.
        pub max_timeout: Duration,
        /// Instruction fuel for an invocation declaring nothing (the
        /// constant [`DEFAULT_TIMEOUT`]): calibration, scaled to what each
        /// method declares. The operator's `timeout` and `max_timeout`
        /// bound wall-clock time only.
        pub fuel: u64,
    }

    impl Default for Limits {
        fn default() -> Self {
            Limits {
                instance_memory: DEFAULT_INSTANCE_MEMORY,
                invocation_memory: DEFAULT_INVOCATION_MEMORY,
                table_elements: DEFAULT_TABLE_ELEMENTS,
                timeout: DEFAULT_TIMEOUT,
                max_timeout: DEFAULT_MAX_TIMEOUT,
                fuel: FUEL_PER_CALL,
            }
        }
    }

    impl Limits {
        /// The deadline an invocation gets: what its method declares,
        /// else the default, never past the ceiling.
        pub fn deadline_for(&self, declared: Option<Duration>) -> Duration {
            declared
                .unwrap_or(self.timeout)
                .min(self.max_timeout.max(self.timeout))
        }

        /// The instruction fuel an invocation gets: [`fuel`][Self::fuel]
        /// scaled by what its method declares against the constant
        /// [`DEFAULT_TIMEOUT`], whatever this member's deadlines, so every
        /// member runs a call with the same fuel.
        pub fn fuel_for(&self, declared: Option<Duration>) -> u64 {
            let timeout = declared.unwrap_or(DEFAULT_TIMEOUT);
            let scaled =
                u128::from(self.fuel) * timeout.as_millis() / DEFAULT_TIMEOUT.as_millis().max(1);
            u64::try_from(scaled).unwrap_or(u64::MAX)
        }
    }

    /// What the invocation about to run asks of the limits: the deadline
    /// its method declares, the deadline of the transaction it runs in,
    /// and its caller's own deadline.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct Budget {
        /// The called method's declared deadline; `None` takes the
        /// backend's default.
        pub declared: Option<Duration>,
        /// The deadline of the transaction the call runs in: the call's
        /// own too, like what it declares.
        pub within: Option<Instant>,
        /// When the caller stops waiting, if it said.
        pub caller: Option<Instant>,
    }

    thread_local! {
        static BUDGET: std::cell::Cell<Budget> = const {
            std::cell::Cell::new(Budget {
                declared: None,
                within: None,
                caller: None,
            })
        };
    }

    /// The budget the next outermost guest call on this thread takes:
    /// what [`with_budget`] set around it, for runtimes that enforce
    /// deadlines themselves.
    pub fn budget() -> Budget {
        BUDGET.with(std::cell::Cell::get)
    }

    /// Run `f` with `budget` for the outermost guest call it makes on this
    /// thread: nested calls share the scope that call sets.
    pub fn with_budget<R>(budget: Budget, f: impl FnOnce() -> R) -> R {
        struct Restore(Budget);
        impl Drop for Restore {
            fn drop(&mut self) {
                BUDGET.with(|cell| cell.set(self.0));
            }
        }
        let _restore = Restore(BUDGET.with(|cell| cell.replace(budget)));
        f()
    }

    /// Process-wide settings of the shared engine, its pooling allocator
    /// and its image cache. They hold for the engine's whole life: set
    /// them with [`configure`] before the first backend exists.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct EngineSettings {
        /// Pooling-allocator slots: instances, memories and tables live
        /// at once across the process. A call that finds none free fails
        /// with [`INSTANCES_EXHAUSTED`].
        pub pool_slots: u32,
        /// Elements a pooled table may hold: at least the
        /// [`Limits::table_elements`] of every backend.
        pub table_elements: usize,
        /// Bytes of written memory a released slot restores in place and
        /// keeps resident, where Linux reports written pages
        /// (`PAGEMAP_SCAN`, Linux 6.7+). Elsewhere it has no effect.
        pub keep_resident: usize,
        /// Weight the image cache keeps, in bytes.
        pub image_cache_bytes: usize,
        /// Bytes of compiled functions kept for reuse: preparing an
        /// image compiles each function twice, and the second reuses the
        /// first. Zero keeps none.
        pub code_cache_bytes: usize,
    }

    impl Default for EngineSettings {
        fn default() -> Self {
            EngineSettings {
                pool_slots: DEFAULT_POOL_SLOTS,
                table_elements: DEFAULT_TABLE_ELEMENTS,
                keep_resident: DEFAULT_KEEP_RESIDENT,
                image_cache_bytes: DEFAULT_IMAGE_CACHE_BYTES,
                code_cache_bytes: DEFAULT_CODE_CACHE_BYTES,
            }
        }
    }

    /// What the shared engine runs and holds.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct RuntimeStats {
        /// Whether instances live in the pooling allocator; false when
        /// its address space could not be reserved and the engine fell
        /// back to allocating each instance on demand.
        pub pooling: bool,
        /// Whether released slots restore only their written pages.
        pub pagemap_scan: bool,
        /// Instances live right now.
        pub instances: u64,
        /// Released memory slots that held an instance before: the next
        /// instance of the image they last held prefers them.
        pub warm_slots: u32,
        /// The image cache.
        pub images: CacheStats,
    }

    /// The process-wide wasmtime engine. Every backend shares it, so
    /// one ticker thread drives every epoch deadline and one cache holds
    /// every image.
    struct Shared {
        engine: wasmtime::Engine,
        settings: EngineSettings,
        pooling: bool,
        pagemap_scan: bool,
        images: cache::Cache<Image>,
    }

    /// The shared engine, once the first backend created it. It lives as
    /// long as the process: its images outlive any one backend, so a
    /// module validated at deploy time and registered again when the
    /// deployment applies is prepared once.
    static SHARED: Mutex<Option<Arc<Shared>>> = Mutex::new(None);
    /// Settings for the engine; `None` means the defaults.
    static SETTINGS: Mutex<Option<EngineSettings>> = Mutex::new(None);

    /// Set the settings the shared engine starts with. Fails once an
    /// engine runs with different ones.
    pub fn configure(settings: EngineSettings) -> Result<(), Failure> {
        let live = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(shared) = live.as_ref()
            && shared.settings != settings
        {
            return Err(trap(
                "the Wasm engine already runs with other settings; configure it before creating backends",
            ));
        }
        *SETTINGS.lock().unwrap_or_else(PoisonError::into_inner) = Some(settings);
        Ok(())
    }

    /// The shared engine's allocator and image cache, once one runs.
    pub fn stats() -> Option<RuntimeStats> {
        let shared = SHARED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()?;
        let pool = shared.engine.pooling_allocator_metrics();
        Some(RuntimeStats {
            pooling: shared.pooling,
            pagemap_scan: shared.pagemap_scan,
            instances: pool.as_ref().map_or(0, |pool| pool.core_instances()),
            warm_slots: pool.as_ref().map_or(0, |pool| pool.unused_warm_memories()),
            images: shared.images.stats(),
        })
    }

    /// Borrow the shared engine, creating it and its ticker on first
    /// use. Its configuration is fixed: fuel metering, epoch
    /// interruption, [`MAX_WASM_STACK`] and the pooling allocator.
    fn shared_engine() -> Result<Arc<Shared>, Failure> {
        let mut slot = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(shared) = slot.as_ref() {
            return Ok(shared.clone());
        }
        let settings = SETTINGS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_default();
        let (engine, pooling, pagemap_scan) = new_engine(&settings)?;
        let shared = Arc::new(Shared {
            engine,
            images: cache::Cache::new(settings.image_cache_bytes),
            settings,
            pooling,
            pagemap_scan,
        });
        let ticker = shared.engine.clone();
        std::thread::Builder::new()
            .name("flower-wasm-epoch".to_string())
            .spawn(move || tick(&ticker))
            .map_err(|error| trap(&format!("cannot start the epoch ticker: {error}")))?;
        *slot = Some(shared.clone());
        Ok(shared)
    }

    /// Build the engine over the pooling allocator, or allocating on
    /// demand where the pool's address space cannot be reserved.
    /// Returns the engine and whether it pools and scans pages.
    fn new_engine(settings: &EngineSettings) -> Result<(wasmtime::Engine, bool, bool), Failure> {
        let code: Option<Arc<dyn wasmtime::CacheStore>> = (settings.code_cache_bytes > 0)
            .then(|| Arc::new(code::CodeCache::new(settings.code_cache_bytes)) as _);
        let base = || -> Result<wasmtime::Config, Failure> {
            let mut config = wasmtime::Config::new();
            config.consume_fuel(true);
            config.epoch_interruption(true);
            config.max_wasm_stack(MAX_WASM_STACK);
            if let Some(code) = &code {
                config
                    .enable_incremental_compilation(code.clone())
                    .map_err(|error| trap(&error.to_string()))?;
            }
            Ok(config)
        };
        // Released slots keep the image mapped (copy-on-write where the
        // platform allows) for the next instance of the same module.
        // With PAGEMAP_SCAN they restore just the pages a call wrote and
        // keep them resident; without it, keeping memory resident would
        // mean clearing the whole kept span on every release.
        let pagemap_scan = wasmtime::PoolingAllocationConfig::is_pagemap_scan_available();
        let mut pool = wasmtime::PoolingAllocationConfig::new();
        pool.total_core_instances(settings.pool_slots)
            .total_memories(settings.pool_slots)
            .total_tables(settings.pool_slots)
            .max_memories_per_module(1)
            .max_tables_per_module(TABLES_PER_INSTANCE)
            .table_elements(settings.table_elements);
        if pagemap_scan {
            pool.pagemap_scan(wasmtime::Enabled::Yes)
                .linear_memory_keep_resident(settings.keep_resident);
        }
        let mut config = base()?;
        config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(pool));
        if let Ok(engine) = wasmtime::Engine::new(&config) {
            return Ok((engine, true, pagemap_scan));
        }
        let engine = wasmtime::Engine::new(&base()?).map_err(|error| trap(&error.to_string()))?;
        Ok((engine, false, false))
    }

    /// Advance the engine epoch every [`EPOCH_TICK`] while any guest
    /// runs, for the process's life; parked otherwise, so an idle member
    /// never wakes for it.
    fn tick(engine: &wasmtime::Engine) {
        loop {
            if RUNNING.load(Ordering::SeqCst) == 0 {
                let parked = WAKE.0.lock().unwrap_or_else(PoisonError::into_inner);
                // Checked under the lock a starting guest takes to wake
                // it, so no wake-up is missed.
                let _parked = WAKE
                    .1
                    .wait_while(parked, |_| RUNNING.load(Ordering::SeqCst) == 0)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            std::thread::sleep(EPOCH_TICK);
            engine.increment_epoch();
        }
    }

    /// Guest stores alive in the process: the ticker runs while any is.
    static RUNNING: AtomicUsize = AtomicUsize::new(0);
    /// What a parked ticker waits on.
    static WAKE: (Mutex<()>, Condvar) = (Mutex::new(()), Condvar::new());

    /// A guest store alive: the ticker runs until the last one drops.
    struct Ticking;

    impl Ticking {
        fn start() -> Ticking {
            if RUNNING.fetch_add(1, Ordering::SeqCst) == 0 {
                let _lock = WAKE.0.lock().unwrap_or_else(PoisonError::into_inner);
                WAKE.1.notify_one();
            }
            Ticking
        }
    }

    impl Drop for Ticking {
        fn drop(&mut self) {
            RUNNING.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Resources shared by the nested guest calls of one invocation on
    /// this thread. Nested calls re-enter synchronously through host
    /// calls, so a thread-local scope sees exactly one invocation.
    struct Scope {
        /// Live guest calls, outermost included.
        depth: usize,
        /// Wall-clock deadline; `None` when the timeout is unbounded.
        deadline: Option<Instant>,
        /// Whether the deadline is the caller's, earlier than the call's
        /// own: passing it is the caller's, not a limit the call ran out
        /// of.
        caller_bound: bool,
        /// Instruction fuel left, handed down to nested calls and back.
        fuel: u64,
        /// Linear-memory bytes held by live instances.
        memory_used: usize,
        /// Cap on `memory_used`.
        memory_limit: usize,
        /// Native stack position at the outermost entry.
        stack_root: usize,
        /// The first limit failure: sticky, so nothing nested or caught
        /// can turn the invocation back into a success.
        failure: Option<Failure>,
    }

    thread_local! {
        static SCOPE: RefCell<Option<Scope>> = const { RefCell::new(None) };
    }

    /// Run `f` against this thread's scope, if any.
    fn with_scope<R>(f: impl FnOnce(&mut Scope) -> R) -> Option<R> {
        SCOPE
            .try_with(|scope| scope.borrow_mut().as_mut().map(f))
            .ok()
            .flatten()
    }

    /// Record a limit failure for the whole invocation; the first wins.
    fn record_failure(failure: &Failure) {
        with_scope(|scope| {
            if scope.failure.is_none() {
                scope.failure = Some(failure.clone());
            }
        });
    }

    /// The invocation's sticky limit failure, if any.
    fn sticky_failure() -> Option<Failure> {
        with_scope(|scope| scope.failure.clone()).flatten()
    }

    /// The current native stack position.
    #[inline(never)]
    fn stack_position() -> usize {
        let marker = 0u8;
        std::hint::black_box(&raw const marker).addr()
    }

    /// Membership of one guest call in its invocation's scope: the
    /// outermost guard creates the scope and removes it on drop.
    struct ScopeGuard {
        outermost: bool,
    }

    impl ScopeGuard {
        /// Enter a guest call. Nested entries fail once the invocation
        /// already failed a limit, nests too deep, used up its native
        /// stack budget or passed its deadline.
        fn enter(limits: &Limits) -> Result<ScopeGuard, CallFailure> {
            let nested = SCOPE.with(|cell| {
                let mut slot = cell.borrow_mut();
                let Some(scope) = slot.as_mut() else {
                    let budget = BUDGET.with(std::cell::Cell::get);
                    let timeout = limits.deadline_for(budget.declared);
                    let own = earliest(Instant::now().checked_add(timeout), budget.within);
                    let caller_bound = budget
                        .caller
                        .is_some_and(|caller| own.is_none_or(|own| caller < own));
                    *slot = Some(Scope {
                        depth: 1,
                        deadline: earliest(own, budget.caller),
                        caller_bound,
                        fuel: limits.fuel_for(budget.declared),
                        memory_used: 0,
                        memory_limit: limits.invocation_memory,
                        stack_root: stack_position(),
                        failure: None,
                    });
                    return Ok(false);
                };
                if let Some(failure) = &scope.failure {
                    return Err(failure.clone());
                }
                let refused = if scope.depth >= MAX_NESTING {
                    Some(fail(
                        NESTING_TOO_DEEP,
                        format!("guest calls nest deeper than {MAX_NESTING}"),
                    ))
                } else if scope.stack_root.abs_diff(stack_position()) > NESTED_STACK_BUDGET {
                    Some(fail(
                        STACK_OVERFLOW,
                        "nested guest calls exhausted the native stack budget",
                    ))
                } else if scope
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    Some(deadline_failure(scope.caller_bound))
                } else {
                    None
                };
                if let Some(failure) = refused {
                    scope.failure = Some(failure.clone());
                    return Err(failure);
                }
                scope.depth += 1;
                Ok(true)
            });
            match nested {
                Ok(nested) => Ok(ScopeGuard { outermost: !nested }),
                Err(failure) => Err(CallFailure {
                    failure,
                    recovered: false,
                }),
            }
        }
    }

    impl Drop for ScopeGuard {
        fn drop(&mut self) {
            let outermost = self.outermost;
            let _ = SCOPE.try_with(|cell| {
                let mut slot = cell.borrow_mut();
                if outermost {
                    *slot = None;
                } else if let Some(scope) = slot.as_mut() {
                    scope.depth -= 1;
                }
            });
        }
    }

    /// The earlier of two deadlines, either possibly absent.
    fn earliest(one: Option<Instant>, other: Option<Instant>) -> Option<Instant> {
        match (one, other) {
            (Some(one), Some(other)) => Some(one.min(other)),
            (one, other) => one.or(other),
        }
    }

    /// The failure of an invocation past its deadline: the caller's when
    /// `caller`, else its own.
    fn deadline_failure(caller: bool) -> Failure {
        if caller {
            fail(
                CALLER_DEADLINE_EXCEEDED,
                "the caller's deadline passed before the guest finished",
            )
        } else {
            fail(
                DEADLINE_EXCEEDED,
                "guest exceeded the invocation's wall-clock deadline",
            )
        }
    }

    /// Store resource limiter: per-instance caps plus the invocation's
    /// aggregate memory. Refusals trap immediately with
    /// [`OUT_OF_MEMORY`] rather than handing the guest a failed grow it
    /// would abort on anyway.
    struct GuestLimiter {
        instance_memory: usize,
        table_elements: usize,
        /// Bytes this instance charged to the scope.
        charged: usize,
        /// Bytes of the last permitted growth, refunded if it fails.
        pending: usize,
        /// Why this store's last growth was refused.
        failure: Option<Failure>,
    }

    impl GuestLimiter {
        fn new(limits: &Limits) -> Self {
            GuestLimiter {
                instance_memory: limits.instance_memory,
                table_elements: limits.table_elements,
                charged: 0,
                pending: 0,
                failure: None,
            }
        }

        /// Refuse a growth: remember why and trap.
        fn refuse(&mut self, message: String) -> wasmtime::Error {
            let failure = fail(OUT_OF_MEMORY, message.clone());
            record_failure(&failure);
            self.failure = Some(failure);
            wasmtime::Error::msg(message)
        }

        /// Return charged bytes to the scope.
        fn refund(&mut self, bytes: usize) {
            self.charged -= bytes;
            with_scope(|scope| scope.memory_used = scope.memory_used.saturating_sub(bytes));
        }
    }

    impl wasmtime::ResourceLimiter for GuestLimiter {
        fn memory_growing(
            &mut self,
            current: usize,
            desired: usize,
            maximum: Option<usize>,
        ) -> wasmtime::Result<bool> {
            if maximum.is_some_and(|maximum| desired > maximum) {
                // The module's own maximum: an ordinary failed grow.
                return Ok(false);
            }
            if desired > self.instance_memory {
                return Err(self.refuse(format!(
                    "guest memory would grow to {desired} bytes, past the {} byte instance limit",
                    self.instance_memory
                )));
            }
            let additional = desired.saturating_sub(current);
            let charged = with_scope(|scope| match scope.memory_used.checked_add(additional) {
                Some(total) if total <= scope.memory_limit => {
                    scope.memory_used = total;
                    Ok(())
                }
                _ => Err(scope.memory_limit),
            })
            .unwrap_or(Ok(()));
            if let Err(limit) = charged {
                return Err(self.refuse(format!(
                    "guest memory across nested calls would pass the {limit} byte invocation limit"
                )));
            }
            self.charged += additional;
            self.pending = additional;
            Ok(true)
        }

        fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
            let pending = std::mem::take(&mut self.pending);
            self.refund(pending);
            Err(self.refuse(format!("host could not allocate guest memory: {error}")))
        }

        fn table_growing(
            &mut self,
            _current: usize,
            desired: usize,
            maximum: Option<usize>,
        ) -> wasmtime::Result<bool> {
            if maximum.is_some_and(|maximum| desired > maximum) {
                return Ok(false);
            }
            if desired > self.table_elements {
                return Err(self.refuse(format!(
                    "guest table would grow to {desired} elements, past the {} element limit",
                    self.table_elements
                )));
            }
            Ok(true)
        }

        fn memories(&self) -> usize {
            1
        }

        fn tables(&self) -> usize {
            16
        }
    }

    impl Drop for GuestLimiter {
        fn drop(&mut self) {
            self.refund(self.charged);
        }
    }

    struct State {
        // Raw pointer to the borrowed host. Every store is created
        // inside `run`, which owns the borrow for the store's whole
        // lifetime — the pointer never dangles. The borrow cannot be
        // named here: `func_wrap` closures must be `'static`, so store
        // data holding them must be `'static` too.
        host: *mut dyn HostCall,
        // Shared crypto dispatch: `'static` by construction, unlike
        // the borrowed host.
        crypto: Option<Arc<dyn flower_sdk::crypto::CryptoBackend>>,
        // Invocation kind of this guest call; zero outside invocations
        // (validation and manifests).
        kind: u32,
        limiter: GuestLimiter,
    }

    /// A module's initialized image, compiled and linked: what every
    /// call instantiates.
    struct Image {
        pre: wasmtime::InstancePre<State>,
        /// Whether the guest imports `crypto_call`.
        imports_crypto: bool,
        /// Instruction fuel `flower_init` consumed preparing the image.
        init_fuel: u64,
    }

    /// A registered module: its bytes, to prepare its image again after
    /// an eviction, and a weak handle on the image.
    struct Registered {
        wasm: Arc<[u8]>,
        key: cache::Key,
        image: RwLock<Weak<cache::Cached<Image>>>,
    }

    /// wasmtime guest backend. Clones share the engine, the image cache
    /// and every registration made before the clone; each clone carries
    /// its own limits, crypto dispatch and later registrations. A backend
    /// is one (database, module) runtime: it shares compiled images with
    /// other backends, never instances.
    #[derive(Clone)]
    pub struct WasmBackend {
        shared: Arc<Shared>,
        modules: HashMap<String, Arc<Registered>>,
        crypto: Option<Arc<dyn flower_sdk::crypto::CryptoBackend>>,
        limits: Limits,
    }

    impl WasmBackend {
        /// Create a backend over the shared engine with default
        /// [`Limits`].
        pub fn new() -> Result<Self, Failure> {
            Self::with_limits(Limits::default())
        }

        /// Create a backend over the shared engine with explicit limits.
        pub fn with_limits(limits: Limits) -> Result<Self, Failure> {
            Ok(WasmBackend {
                shared: shared_engine()?,
                modules: HashMap::new(),
                crypto: None,
                limits,
            })
        }

        /// The limits guest calls run under.
        pub fn limits(&self) -> &Limits {
            &self.limits
        }

        /// Replace the limits for later calls.
        pub fn set_limits(&mut self, limits: Limits) {
            self.limits = limits;
        }

        /// Attach the crypto dispatch guests reach through
        /// `crypto_call`. Without one, invocations of guests importing
        /// it fail to instantiate; registration still validates them.
        pub fn set_crypto(&mut self, crypto: Arc<dyn flower_sdk::crypto::CryptoBackend>) {
            self.crypto = Some(crypto);
        }

        /// Validate and register a module's Wasm bytes, replacing any
        /// previous registration under the same identity. Validation
        /// checks the import rules, the ABI exports and the state an
        /// image can hold, prepares the image — running `flower_init`
        /// under the backend's limits — unless the cache holds it, then
        /// instantiates it once under those limits, so a broken module
        /// fails here instead of at its first invocation. A cached image
        /// whose `flower_init` used more fuel than these limits allow
        /// fails as running it would.
        pub fn register(&mut self, module: &str, wasm: &[u8]) -> Result<(), Failure> {
            let key: cache::Key = sha2::Sha256::digest(wasm).into();
            let wasm: Arc<[u8]> = Arc::from(wasm);
            let image = self.shared.images.acquire(key, None, || {
                prepare(&self.shared.engine, &wasm, &self.limits)
            })?;
            if image.init_fuel > self.limits.fuel {
                return Err(fail(
                    FUEL_EXHAUSTED,
                    "flower_init needs more instruction fuel than an invocation has",
                ));
            }
            let mut noop = NoopHost;
            self.run(NO_INVOCATION, &mut noop, &image, |_, _| Ok(()))
                .map_err(|failure| failure.failure)?;
            self.modules.insert(
                module.to_string(),
                Arc::new(Registered {
                    wasm,
                    key,
                    image: RwLock::new(Arc::downgrade(&image)),
                }),
            );
            Ok(())
        }

        /// Forget a module registration.
        pub fn unregister(&mut self, module: &str) {
            self.modules.remove(module);
        }

        /// Whether two backends run `module` from the same prepared
        /// image right now.
        pub fn shares_image(&self, other: &WasmBackend, module: &str) -> bool {
            let image = |backend: &WasmBackend| {
                backend.modules.get(module).and_then(|registered| {
                    registered
                        .image
                        .read()
                        .unwrap_or_else(PoisonError::into_inner)
                        .upgrade()
                })
            };
            match (image(self), image(other)) {
                (Some(mine), Some(theirs)) => Arc::ptr_eq(&mine, &theirs),
                _ => false,
            }
        }

        /// A registered module's image: the live one while the cache
        /// holds it, else the cache's, prepared again if it was evicted.
        fn image(&self, registered: &Registered) -> Result<Arc<cache::Cached<Image>>, CallFailure> {
            let live = registered
                .image
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .upgrade();
            if let Some(image) = &live
                && self.shared.images.touch(image)
            {
                return Ok(image.clone());
            }
            let image = self
                .shared
                .images
                .acquire(registered.key, live, || {
                    prepare(&self.shared.engine, &registered.wasm, &self.limits)
                })
                .map_err(fatal_with)?;
            *registered
                .image
                .write()
                .unwrap_or_else(PoisonError::into_inner) = Arc::downgrade(&image);
            Ok(image)
        }

        /// Read a module's manifest through a throwaway instance.
        pub fn manifest(&self, module: &str) -> Result<Value, CallFailure> {
            let registered = self
                .modules
                .get(module)
                .ok_or_else(|| unknown_method(module))?;
            let image = self.image(registered)?;
            let mut noop = NoopHost;
            let bytes = self.run(NO_INVOCATION, &mut noop, &image, |store, instance| {
                let manifest = instance
                    .get_typed_func::<(), u64>(&mut *store, "flower_manifest")
                    .map_err(|error| fatal(&error.to_string()))?;
                let packed = manifest
                    .call(&mut *store, ())
                    .map_err(|error| guest_error(store, &error))?;
                read_packed(store, instance, packed)
            })?;
            decode_envelope(decode_binary(&bytes)?).map_err(|failure| CallFailure {
                failure,
                recovered: false,
            })
        }

        /// Run one guest call in a new instance of `image` under the
        /// invocation scope (see [`execute`]).
        fn run<T>(
            &self,
            kind: u32,
            host: &mut dyn HostCall,
            image: &Image,
            body: impl FnOnce(
                &mut wasmtime::Store<State>,
                &wasmtime::Instance,
            ) -> Result<T, CallFailure>,
        ) -> Result<T, CallFailure> {
            execute(
                &self.shared.engine,
                &self.limits,
                self.crypto.clone(),
                kind,
                host,
                |store| {
                    // Validation and manifests check the module, not this
                    // backend's wiring: the import is linked anyway
                    // (calls trap without a manager), so runtime builders
                    // without crypto — backup replay — still accept such
                    // modules.
                    if image.imports_crypto && self.crypto.is_none() && kind != NO_INVOCATION {
                        return Err(CallFailure {
                            failure: fail(
                                INSTANTIATION_FAILED,
                                "guest imports crypto_call but no crypto manager is configured",
                            ),
                            recovered: false,
                        });
                    }
                    image
                        .pre
                        .instantiate(&mut *store)
                        .map_err(|error| instantiation_error(store, &error))
                },
                body,
            )
        }

        fn invoke(
            &self,
            kind: u32,
            module: &str,
            name: &str,
            args: &Value,
            host: &mut dyn HostCall,
        ) -> Result<Value, CallFailure> {
            let registered = self
                .modules
                .get(module)
                .ok_or_else(|| unknown_method(module))?;
            let image = self.image(registered)?;
            let request = Value::Array(vec![Value::String(name.to_string()), args.clone()]);
            let input = request
                .encode_binary()
                .map_err(|failure| fatal(&failure.message))?;
            let in_len = u32::try_from(input.len()).map_err(|_| fatal("request too large"))?;
            let bytes = self.run(kind, host, &image, |store, instance| {
                let alloc = instance
                    .get_typed_func::<u32, u32>(&mut *store, "flower_alloc")
                    .map_err(|error| fatal(&error.to_string()))?;
                let in_ptr = alloc
                    .call(&mut *store, in_len)
                    .map_err(|error| guest_error(store, &error))?;
                if in_ptr == 0 {
                    return Err(fatal_with(fail(OUT_OF_MEMORY, "guest allocation failed")));
                }
                let memory = guest_memory(store, instance)?;
                memory
                    .write(&mut *store, in_ptr as usize, &input)
                    .map_err(|error| fatal(&error.to_string()))?;
                let invoke = instance
                    .get_typed_func::<(u32, u32, u32), u64>(&mut *store, "flower_invoke")
                    .map_err(|error| fatal(&error.to_string()))?;
                let packed = invoke
                    .call(&mut *store, (kind, in_ptr, in_len))
                    .map_err(|error| guest_error(store, &error))?;
                read_packed(store, instance, packed)
            })?;
            // Decoded guest failures are business failures, like
            // closures: savepoints may catch them and prepare forms
            // failure patches. Traps, garbage responses and exhausted
            // limits stay fatal through the paths above, and a session
            // poisoned by a fatal host op still errors when the lane
            // takes the poison.
            decode_envelope(decode_binary(&bytes)?).map_err(|failure| CallFailure {
                failure,
                recovered: true,
            })
        }
    }

    impl Runtime for WasmBackend {
        fn call(
            &self,
            kind: CallKind,
            module: &str,
            name: &str,
            args: &Value,
            host: &mut dyn HostCall,
        ) -> Result<Value, CallFailure> {
            self.invoke(kind.invocation() as u32, module, name, args, host)
        }

        fn plan(&self, module: &str, name: &str, args: &Value) -> Result<Value, CallFailure> {
            let mut noop = NoopHost;
            self.invoke(
                InvocationKind::Transaction as u32,
                module,
                name,
                args,
                &mut noop,
            )
        }
    }

    /// Run one guest call under the invocation scope: enter the scope,
    /// build the limited store, `instantiate`, run `body`, hand leftover
    /// fuel back to the scope and drop the store. A limit failure
    /// recorded anywhere in the invocation overrides whatever the guest
    /// returned.
    fn execute<T>(
        engine: &wasmtime::Engine,
        limits: &Limits,
        crypto: Option<Arc<dyn flower_sdk::crypto::CryptoBackend>>,
        kind: u32,
        host: &mut dyn HostCall,
        instantiate: impl FnOnce(&mut wasmtime::Store<State>) -> Result<wasmtime::Instance, CallFailure>,
        body: impl FnOnce(&mut wasmtime::Store<State>, &wasmtime::Instance) -> Result<T, CallFailure>,
    ) -> Result<T, CallFailure> {
        // Declared before the store so the store (and its limiter's
        // memory refund) drops while the scope still exists.
        let _scope = ScopeGuard::enter(limits)?;
        let crypto = crypto.map(|backend| backend.for_invocation().map_or(backend, Arc::from));
        // The epoch advances while the store lives.
        let _ticking = Ticking::start();
        // SAFETY: this only erases the borrow's lifetime so the pointer
        // fits `'static` store data; the fat pointer layout is
        // unchanged. The store never escapes this function, so every
        // dereference (in `host_trampoline`) happens while `host` is
        // still mutably borrowed here and nothing else touches it.
        let host_ptr: *mut dyn HostCall =
            unsafe { std::mem::transmute::<&mut dyn HostCall, *mut dyn HostCall>(host) };
        let mut store = wasmtime::Store::new(
            engine,
            State {
                host: host_ptr,
                crypto,
                kind,
                limiter: GuestLimiter::new(limits),
            },
        );
        store.limiter(|state| &mut state.limiter);
        let (fuel, deadline) =
            with_scope(|scope| (scope.fuel, scope.deadline)).unwrap_or((0, None));
        store
            .set_fuel(fuel)
            .map_err(|error| fatal(&error.to_string()))?;
        match deadline {
            // Effectively unbounded.
            None => {
                store.epoch_deadline_trap();
                store.set_epoch_deadline(u64::MAX / 2);
            }
            // Every tick checks the clock: ticks only come late, never
            // early, and late ones never add up.
            Some(deadline) => {
                store.set_epoch_deadline(1);
                store.epoch_deadline_callback(move |_| {
                    if Instant::now() >= deadline {
                        Err(wasmtime::Trap::Interrupt.into())
                    } else {
                        Ok(wasmtime::UpdateDeadline::Continue(1))
                    }
                });
            }
        }
        let outcome = instantiate(&mut store).and_then(|instance| body(&mut store, &instance));
        let left = store.get_fuel().unwrap_or(0);
        with_scope(|scope| scope.fuel = left);
        let value = outcome?;
        match sticky_failure() {
            Some(failure) => Err(CallFailure {
                failure,
                recovered: false,
            }),
            None => Ok(value),
        }
    }

    /// Link a module's optional `host_call` and `crypto_call` imports
    /// once, for every instance after. Returns whether it imports
    /// `crypto_call`.
    fn link(
        engine: &wasmtime::Engine,
        module: &wasmtime::Module,
    ) -> Result<(wasmtime::InstancePre<State>, bool), Failure> {
        let mut linker = wasmtime::Linker::new(engine);
        let mut host_module = None;
        let mut crypto_module = None;
        for import in module.imports() {
            if import.name() == "host_call" {
                host_module = Some(import.module().to_string());
            }
            if import.name() == "crypto_call" {
                crypto_module = Some(import.module().to_string());
            }
        }
        // A guest that never calls the host (a crypto-only module, a
        // pure function) has no `host_call` import: nothing to link.
        if let Some(host_module) = &host_module {
            linker
                .func_wrap(
                    host_module,
                    "host_call",
                    |mut caller: wasmtime::Caller<'_, State>, op: u32, in_ptr: u32, in_len: u32| {
                        host_trampoline(&mut caller, op, in_ptr, in_len)
                    },
                )
                .map_err(|error| trap(&error.to_string()))?;
        }
        if let Some(crypto_module) = &crypto_module {
            linker
                .func_wrap(
                    crypto_module,
                    "crypto_call",
                    |mut caller: wasmtime::Caller<'_, State>, op: u32, in_ptr: u32, in_len: u32| {
                        crypto_trampoline(&mut caller, op, in_ptr, in_len)
                    },
                )
                .map_err(|error| trap(&error.to_string()))?;
        }
        let pre = linker
            .instantiate_pre(module)
            .map_err(|error| trap(&format!("{error:#}")))?;
        Ok((pre, crypto_module.is_some()))
    }

    /// Prepare a module's initialized image under `limits`, returning it
    /// with its cache weight. Preparation runs on a thread of its own:
    /// compiling and `flower_init` get a roomy native stack, and a call
    /// that prepares an evicted image mid-invocation keeps its own
    /// invocation scope out of the image's.
    fn prepare(
        engine: &wasmtime::Engine,
        wasm: &[u8],
        limits: &Limits,
    ) -> Result<(Image, usize), Failure> {
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("flower-wasm-prepare".to_string())
                .stack_size(PREPARE_STACK)
                .spawn_scoped(scope, || prepare_here(engine, wasm, limits))
                .map_err(|error| trap(&format!("cannot start image preparation: {error}")))?
                .join()
                .unwrap_or_else(|_| Err(trap("image preparation panicked")))
        })
    }

    /// Check the module's surface, run `flower_init` in an instance of
    /// the instrumented module without host or crypto capabilities,
    /// snapshot its memory and mutable globals, then compile and link
    /// the image they make.
    fn prepare_here(
        engine: &wasmtime::Engine,
        wasm: &[u8],
        limits: &Limits,
    ) -> Result<(Image, usize), Failure> {
        let surface = image::inspect(wasm).map_err(|message| trap(&message))?;
        let instrumented = image::instrument(wasm, &surface).map_err(|message| trap(&message))?;
        let instrumented = wasmtime::Module::new(engine, &instrumented)
            .map_err(|error| trap(&error.to_string()))?;
        check_imports(&instrumented)?;
        check_exports(&instrumented)?;
        let (pre, _) = link(engine, &instrumented)?;
        let mut noop = NoopHost;
        let (snapshot, init_fuel) = execute(
            engine,
            limits,
            None,
            NO_INVOCATION,
            &mut noop,
            |store| {
                pre.instantiate(&mut *store)
                    .map_err(|error| guest_error(store, &error))
            },
            |store, instance| {
                run_init(store, instance)?;
                let memory = guest_memory(store, instance)?.data(&*store).to_vec();
                let globals = surface
                    .globals
                    .iter()
                    .map(|(index, _)| {
                        instance
                            .get_global(&mut *store, &image::global_export(*index))
                            .map(|global| global.get(&mut *store))
                            .ok_or_else(|| fatal("image snapshot lost a global"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let left = store.get_fuel().unwrap_or(0);
                Ok((
                    image::Snapshot { memory, globals },
                    limits.fuel.saturating_sub(left),
                ))
            },
        )
        .map_err(|failure| failure.failure)?;
        let bytes = image::image(wasm, &surface, &snapshot).map_err(|message| trap(&message))?;
        let module =
            wasmtime::Module::new(engine, &bytes).map_err(|error| trap(&error.to_string()))?;
        let (pre, imports_crypto) = link(engine, &module)?;
        let code = module.image_range();
        let weight = (code.end.addr() - code.start.addr()).saturating_add(bytes.len());
        Ok((
            Image {
                pre,
                imports_crypto,
                init_fuel,
            },
            weight,
        ))
    }

    /// Enforce the import rules: `host_call` and `crypto_call` at most
    /// once each (the linker drops a `host_call` the guest never uses).
    fn check_imports(module: &wasmtime::Module) -> Result<(), Failure> {
        let mut host_calls = 0;
        let mut crypto_calls = 0;
        for import in module.imports() {
            if import.name() == "host_call" {
                host_calls += 1;
            }
            if import.name() == "crypto_call" {
                crypto_calls += 1;
            }
        }
        if host_calls > 1 {
            return Err(trap("guest must import host_call at most once"));
        }
        if crypto_calls > 1 {
            return Err(trap("guest must import crypto_call at most once"));
        }
        Ok(())
    }

    /// Enforce the ABI exports: `memory`, `flower_alloc`,
    /// `flower_invoke`, `flower_manifest` and optional `flower_init`,
    /// each with its exact signature.
    fn check_exports(module: &wasmtime::Module) -> Result<(), Failure> {
        use wasmtime::{ExternType, ValType};
        fn signature(types: impl Iterator<Item = ValType>) -> String {
            types
                .map(|ty| match ty {
                    ValType::I32 => "i32",
                    ValType::I64 => "i64",
                    _ => "other",
                })
                .collect::<Vec<_>>()
                .join(",")
        }
        match module.get_export("memory") {
            Some(ExternType::Memory(_)) => {}
            Some(_) => return Err(trap("guest export `memory` is not a memory")),
            None => return Err(trap("guest does not export `memory`")),
        }
        let functions = [
            ("flower_alloc", "i32", "i32", true),
            ("flower_invoke", "i32,i32,i32", "i64", true),
            ("flower_manifest", "", "i64", true),
            ("flower_init", "", "i64", false),
        ];
        for (name, params, results, required) in functions {
            match module.get_export(name) {
                Some(ExternType::Func(ty)) => {
                    let actual = (signature(ty.params()), signature(ty.results()));
                    if actual != (params.to_string(), results.to_string()) {
                        return Err(trap(&format!(
                            "guest export `{name}` must be ({params}) -> ({results}), found ({}) -> ({})",
                            actual.0, actual.1
                        )));
                    }
                }
                Some(_) => return Err(trap(&format!("guest export `{name}` is not a function"))),
                None if required => {
                    return Err(trap(&format!("guest does not export `{name}`")));
                }
                None => {}
            }
        }
        Ok(())
    }

    /// Run the instance's optional `flower_init`: once per image, in the
    /// instance its snapshot is taken from.
    fn run_init(
        store: &mut wasmtime::Store<State>,
        instance: &wasmtime::Instance,
    ) -> Result<(), CallFailure> {
        let Some(init) = instance.get_func(&mut *store, "flower_init") else {
            return Ok(());
        };
        let init = init
            .typed::<(), u64>(&*store)
            .map_err(|error| fatal(&error.to_string()))?;
        let packed = init
            .call(&mut *store, ())
            .map_err(|error| guest_error(store, &error))?;
        let bytes = read_packed(store, instance, packed)?;
        decode_envelope(decode_binary(&bytes)?)
            .map(|_| ())
            .map_err(|failure| {
                fatal_with(fail(
                    INIT_FAILED,
                    format!("flower_init failed: {}: {}", failure.code, failure.message),
                ))
            })
    }

    fn guest_memory(
        store: &mut wasmtime::Store<State>,
        instance: &wasmtime::Instance,
    ) -> Result<wasmtime::Memory, CallFailure> {
        instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| CallFailure {
                failure: trap("guest exports no memory"),
                recovered: false,
            })
    }

    fn read_packed(
        store: &mut wasmtime::Store<State>,
        instance: &wasmtime::Instance,
        packed: u64,
    ) -> Result<Vec<u8>, CallFailure> {
        if packed == 0 {
            return Err(CallFailure {
                failure: trap("guest cannot respond"),
                recovered: false,
            });
        }
        let ptr = ((packed >> 32) & 0xFFFF_FFFF) as u32 as usize;
        let len = (packed & 0xFFFF_FFFF) as u32 as usize;
        if len > MAX_HOST_MESSAGE {
            return Err(CallFailure {
                failure: trap("guest response too large"),
                recovered: false,
            });
        }
        let memory = guest_memory(store, instance)?;
        let mut bytes = vec![0u8; len];
        memory
            .read(&*store, ptr, &mut bytes)
            .map_err(|error| fatal(&error.to_string()))?;
        Ok(bytes)
    }

    /// Classify a failed guest call: limiter refusals, fuel, deadline
    /// and stack traps and a full instance pool get their own codes and
    /// stick to the invocation; anything else is a plain trap.
    fn guest_error(store: &wasmtime::Store<State>, error: &wasmtime::Error) -> CallFailure {
        if let Some(failure) = &store.data().limiter.failure {
            return fatal_with(failure.clone());
        }
        if let Some(full) = error.downcast_ref::<wasmtime::PoolConcurrencyLimitError>() {
            let failure = fail(
                INSTANCES_EXHAUSTED,
                format!("no guest instance slot is free: {full}"),
            );
            record_failure(&failure);
            return fatal_with(failure);
        }
        let limit = match error.downcast_ref::<wasmtime::Trap>() {
            Some(wasmtime::Trap::OutOfFuel) => Some(fail(
                FUEL_EXHAUSTED,
                "guest exhausted the invocation's instruction fuel",
            )),
            Some(wasmtime::Trap::Interrupt) => Some(deadline_failure(
                with_scope(|scope| scope.caller_bound).unwrap_or(false),
            )),
            Some(wasmtime::Trap::StackOverflow) => {
                Some(fail(STACK_OVERFLOW, "guest exhausted its Wasm stack"))
            }
            _ => None,
        };
        if let Some(failure) = limit {
            record_failure(&failure);
            return fatal_with(failure);
        }
        // A host call that trapped on a nested limit failure surfaces
        // that failure, not the trampoline's message.
        if let Some(failure) = sticky_failure() {
            return fatal_with(failure);
        }
        fatal(&format!("{error:#}"))
    }

    /// Classify a failed instantiation: limits and a full pool as any
    /// guest error; anything else is this member failing to start an
    /// instance (no guest code ran), not the guest trapping.
    fn instantiation_error(store: &wasmtime::Store<State>, error: &wasmtime::Error) -> CallFailure {
        let failure = guest_error(store, error);
        if failure.failure.code != HOST_TRAP {
            return failure;
        }
        fatal_with(fail(
            INSTANTIATION_FAILED,
            format!("cannot start a guest instance: {error:#}"),
        ))
    }

    /// Host handler that refuses every call: planners, validation and
    /// manifests run without a context.
    struct NoopHost;

    impl HostCall for NoopHost {
        fn op(&mut self, _op: u8, _input: &[u8]) -> Vec<u8> {
            flower_sdk::abi::encode_err(&fail(
                "invalid_invocation",
                "planners run without a context",
            ))
            .encode_binary()
            .unwrap_or_default()
        }
    }

    /// Copy a guest buffer into the host, bounded by
    /// [`MAX_HOST_MESSAGE`].
    fn read_guest(
        caller: &mut wasmtime::Caller<'_, State>,
        what: &str,
        in_ptr: u32,
        in_len: u32,
    ) -> wasmtime::Result<(wasmtime::Memory, Vec<u8>)> {
        let memory = caller
            .get_export("memory")
            .and_then(|export| export.into_memory())
            .ok_or_else(|| wasmtime::format_err!("guest exports no memory"))?;
        if in_len as usize > MAX_HOST_MESSAGE {
            return Err(wasmtime::format_err!("{what} input too large"));
        }
        let mut input = vec![0u8; in_len as usize];
        memory
            .read(&*caller, in_ptr as usize, &mut input)
            .map_err(|_| wasmtime::format_err!("{what} input out of bounds"))?;
        Ok((memory, input))
    }

    /// Copy a host response into a guest allocation and return the
    /// packed guest address: the address the guest reads back must be
    /// a guest address, never a host one.
    fn write_guest(
        caller: &mut wasmtime::Caller<'_, State>,
        memory: wasmtime::Memory,
        what: &str,
        output: &[u8],
    ) -> wasmtime::Result<u64> {
        let len = output.len();
        if len > MAX_HOST_MESSAGE {
            return Err(wasmtime::format_err!("{what} output too large"));
        }
        let alloc = caller
            .get_export("flower_alloc")
            .and_then(|export| export.into_func())
            .ok_or_else(|| wasmtime::format_err!("guest exports no allocator"))?;
        let len_u32 =
            u32::try_from(len).map_err(|_| wasmtime::format_err!("{what} output too large"))?;
        let mut results = [wasmtime::Val::I32(0)];
        alloc
            .call(
                &mut *caller,
                &[wasmtime::Val::I32(len_u32 as i32)],
                &mut results,
            )
            .map_err(|error| {
                // A limit firing in the allocator (the deadline, the fuel,
                // the stack) stays a trap, so it is classified as that
                // limit and not as a host trap.
                if error.downcast_ref::<wasmtime::Trap>().is_some() {
                    error
                } else {
                    wasmtime::format_err!("guest allocation trapped: {error}")
                }
            })?;
        let out_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => ptr as u32,
            _ => return Err(wasmtime::format_err!("guest allocator misbehaved")),
        };
        if out_ptr == 0 {
            return Err(wasmtime::format_err!("guest allocation failed"));
        }
        memory
            .write(&mut *caller, out_ptr as usize, output)
            .map_err(|_| wasmtime::format_err!("{what} output out of bounds"))?;
        Ok(((out_ptr as u64) << 32) | len as u64)
    }

    fn host_trampoline(
        caller: &mut wasmtime::Caller<'_, State>,
        op: u32,
        in_ptr: u32,
        in_len: u32,
    ) -> wasmtime::Result<u64> {
        let (memory, input) = read_guest(caller, "host-call", in_ptr, in_len)?;
        let op = u8::try_from(op).map_err(|_| wasmtime::format_err!("bad host op"))?;
        // Lend this call's remaining fuel to nested guest calls the host
        // may run, and take back what they leave.
        let fuel = caller.get_fuel()?;
        with_scope(|scope| scope.fuel = fuel);
        // SAFETY: `run` took this pointer from a `&mut dyn HostCall` that
        // stays borrowed for the store's whole life, and guest calls are
        // synchronous on this thread, so this is the only live reference
        // to the host while `op` runs.
        let host = unsafe { &mut *caller.data_mut().host };
        let output = host.op(op, &input);
        let left = with_scope(|scope| scope.fuel).unwrap_or(fuel);
        caller.set_fuel(left)?;
        // A nested call that exhausted a shared limit dooms the whole
        // invocation: stop here instead of letting the guest continue.
        if let Some(failure) = sticky_failure() {
            return Err(wasmtime::format_err!(
                "{}: {}",
                failure.code,
                failure.message
            ));
        }
        write_guest(caller, memory, "host-call", &output)
    }

    fn crypto_trampoline(
        caller: &mut wasmtime::Caller<'_, State>,
        op: u32,
        in_ptr: u32,
        in_len: u32,
    ) -> wasmtime::Result<u64> {
        let (memory, input) = read_guest(caller, "crypto-call", in_ptr, in_len)?;
        let crypto = caller
            .data()
            .crypto
            .clone()
            .ok_or_else(|| wasmtime::format_err!("no crypto manager is configured"))?;
        // Entropy is mutation-only: queries, derived values and
        // planners must be pure functions of state. Each nested call
        // has its own store and kind, so a query called from a mutation
        // is still refused.
        let entropy_refused = op == flower_sdk::crypto::CryptoOp::RandomBytes as u32
            && caller.data().kind != InvocationKind::Mutation as u32;
        // Crypto failures are envelopes, not traps: the guest catches
        // unknown keys, forbidden usages and malformed inputs.
        let outcome = if entropy_refused {
            Err(fail(
                flower_sdk::error::codes::INVALID_INVOCATION,
                "random_bytes is only available to mutations: queries, derived values and planners must stay deterministic",
            ))
        } else {
            if crypto_reads_keys(op) {
                // SAFETY: as in `host_trampoline`: the pointer comes from
                // a `&mut dyn HostCall` borrowed for the store's whole
                // life, and this synchronous call is its only user.
                let host = unsafe { &mut *caller.data_mut().host };
                host.read_keys();
            }
            crypto.crypto_call(op, &input)
        };
        let output = match outcome {
            Ok(bytes) => bytes,
            Err(failure) => flower_sdk::abi::encode_err(&failure)
                .encode_binary()
                .unwrap_or_default(),
        };
        write_guest(caller, memory, "crypto-call", &output)
    }

    fn trap(message: &str) -> Failure {
        fail(HOST_TRAP, message)
    }

    fn fatal(message: &str) -> CallFailure {
        fatal_with(trap(message))
    }

    fn fatal_with(failure: Failure) -> CallFailure {
        CallFailure {
            failure,
            recovered: false,
        }
    }

    fn decode_binary(bytes: &[u8]) -> Result<Value, CallFailure> {
        Value::decode_binary(bytes).map_err(|failure| CallFailure {
            failure,
            recovered: false,
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn backends_share_one_engine_and_ticker() {
            let first = WasmBackend::new().unwrap();
            let second = WasmBackend::with_limits(Limits {
                timeout: Duration::from_millis(50),
                ..Limits::default()
            })
            .unwrap();
            // One engine — hence one ticker thread — however many
            // backends exist.
            assert!(Arc::ptr_eq(&first.shared, &second.shared));
            assert_eq!(second.limits().timeout, Duration::from_millis(50));
        }

        #[test]
        fn a_guest_needs_no_host_call_import() {
            let engine = shared_engine().unwrap().engine.clone();
            let module = |imports: usize| {
                let mut module = wasm_encoder::Module::new();
                let mut types = wasm_encoder::TypeSection::new();
                types.ty().function([], []);
                module.section(&types);
                let mut section = wasm_encoder::ImportSection::new();
                for _ in 0..imports {
                    section.import("flower", "host_call", wasm_encoder::EntityType::Function(0));
                }
                if imports > 0 {
                    module.section(&section);
                }
                wasmtime::Module::new(&engine, module.finish()).unwrap()
            };
            // A crypto-only or pure guest imports no `host_call`.
            assert!(check_imports(&module(0)).is_ok());
            assert!(check_imports(&module(1)).is_ok());
            assert!(check_imports(&module(2)).is_err());
        }

        #[test]
        fn declared_deadlines_set_fuel_and_callers_only_cut_them() {
            let limits = Limits::default();
            // Nothing declared: the default deadline and its fuel.
            assert_eq!(limits.deadline_for(None), DEFAULT_TIMEOUT);
            assert_eq!(limits.fuel_for(None), FUEL_PER_CALL);
            // A declaration scales the fuel; the ceiling cuts the
            // deadline, never the fuel.
            let ten = Duration::from_secs(10);
            assert_eq!(limits.deadline_for(Some(ten)), ten);
            assert_eq!(limits.fuel_for(Some(ten)), 2 * FUEL_PER_CALL);
            let day = Duration::from_secs(86_400);
            assert_eq!(limits.deadline_for(Some(day)), DEFAULT_MAX_TIMEOUT);
            // A caller's deadline cuts the scope's, never its fuel.
            let caller = Instant::now() + Duration::from_millis(5);
            let budget = Budget {
                declared: Some(ten),
                caller: Some(caller),
                ..Budget::default()
            };
            with_budget(budget, || {
                let _scope = ScopeGuard::enter(&limits).unwrap();
                let (fuel, deadline) = with_scope(|scope| (scope.fuel, scope.deadline)).unwrap();
                assert_eq!(fuel, 2 * FUEL_PER_CALL);
                assert_eq!(deadline, Some(caller));
            });
        }

        #[test]
        fn instruction_fuel_follows_the_declaration_alone() {
            // Two members configured apart: a shorter default deadline
            // and ceiling on one. Wall-clock time may differ between
            // them; the fuel a call gets may not, or the same mutation
            // would succeed on one leader and run out on another.
            let fast = Limits {
                timeout: Duration::from_secs(1),
                max_timeout: Duration::from_secs(1),
                ..Limits::default()
            };
            let fuel = |limits: &Limits, declared: Option<Duration>| {
                let budget = Budget {
                    declared,
                    ..Budget::default()
                };
                with_budget(budget, || {
                    let _scope = ScopeGuard::enter(limits).unwrap();
                    with_scope(|scope| scope.fuel).unwrap()
                })
            };
            for declared in [
                None,
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(90)),
            ] {
                assert_eq!(
                    fuel(&Limits::default(), declared),
                    fuel(&fast, declared),
                    "{declared:?}"
                );
            }
            assert_eq!(fuel(&fast, None), FUEL_PER_CALL);
            assert_eq!(
                fuel(&fast, Some(Duration::from_secs(2))),
                FUEL_PER_CALL * 2 / 5
            );
        }

        #[test]
        fn nested_scopes_share_and_release() {
            let limits = Limits {
                invocation_memory: 100,
                fuel: 7,
                ..Limits::default()
            };
            let outer = ScopeGuard::enter(&limits).unwrap();
            assert!(outer.outermost);
            let inner = ScopeGuard::enter(&Limits::default()).unwrap();
            assert!(!inner.outermost);
            // The outermost limits govern the nested call.
            assert_eq!(with_scope(|scope| (scope.fuel, scope.depth)), Some((7, 2)));
            drop(inner);
            record_failure(&fail(OUT_OF_MEMORY, "boom"));
            let refused = ScopeGuard::enter(&limits).err().unwrap();
            assert_eq!(refused.failure.code, OUT_OF_MEMORY);
            assert!(!refused.recovered);
            drop(outer);
            assert!(with_scope(|_| ()).is_none());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ScriptHost {
        calls: Vec<u8>,
    }

    impl HostCall for ScriptHost {
        fn op(&mut self, op: u8, _input: &[u8]) -> Vec<u8> {
            self.calls.push(op);
            flower_sdk::abi::encode_ok(Value::Null)
                .encode_binary()
                .unwrap_or_default()
        }
    }

    #[test]
    fn closure_runtime_drives_hosts() {
        let runtime = ClosureRuntime::new(
            |kind: CallKind, module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                assert_eq!(kind, CallKind::Query);
                assert_eq!((module, name), ("tasks", "get"));
                host.op(15, &[]);
                Ok(args.clone())
            },
        );
        let mut host = ScriptHost { calls: Vec::new() };
        let args = Value::Number(1.0);
        let out = runtime
            .call(CallKind::Query, "tasks", "get", &args, &mut host)
            .unwrap();
        assert_eq!(out, args);
        assert_eq!(host.calls, vec![15]);
    }

    #[test]
    fn closure_failures_recover() {
        let runtime = ClosureRuntime::new(
            |_: CallKind, _: &str, _: &str, _: &Value, _: &mut dyn HostCall| {
                Err(fail("busy", "try again"))
            },
        );
        let mut host = ScriptHost { calls: Vec::new() };
        let failure = runtime
            .call(CallKind::Mutation, "m", "n", &Value::Null, &mut host)
            .unwrap_err();
        assert!(failure.recovered);
        assert_eq!(failure.failure.code, "busy");
    }
}
