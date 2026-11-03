//! A real Flower guest: a tasks module compiled to `wasm32` and
//! driven through the wasmtime backend by engine tests.

extern crate alloc;

use flower_sdk::{
    App, Component, FlowerType, MutationCtx, QueryCtx, collection, export, mutation, query,
};

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Task {
    pub title: String,
}

#[collection(
    name = "tasks",
    key = String,
    value = Task,
    index(name = "by_title", fields(title)),
)]
pub struct Tasks;

#[query]
pub fn get(ctx: &mut QueryCtx<'_, '_>, key: String) -> flower_sdk::Result<Option<Task>> {
    Tasks::get(ctx, &key)
}

#[mutation]
pub fn put(
    ctx: &mut MutationCtx<'_, '_>,
    key: String,
    title: String,
) -> flower_sdk::Result<String> {
    Tasks::set(ctx, &key, &Task { title })?;
    Ok(String::from("put-ok"))
}

#[mutation]
pub fn sign_message(
    ctx: &mut MutationCtx<'_, '_>,
    key: String,
    message: String,
) -> flower_sdk::Result<String> {
    use flower_sdk::crypto::{Algorithm, Crypto, ManagedKey, ManagedKeyRef, Usage};
    let key = ManagedKey::new(&key, Algorithm::Ed25519, &[Usage::Sign])?;
    let mut crypto = Crypto::new(ctx.ctx());
    let signature = crypto.sign(&key, ManagedKeyRef::Current, message.as_bytes())?;
    Ok(flower_sdk::crypto::hex_encode(&signature))
}

#[mutation]
pub fn send_email(ctx: &mut MutationCtx<'_, '_>, to: String) -> flower_sdk::Result<String> {
    use flower_sdk::Value;
    use flower_sdk::queue::{EnqueueOptions, Queue};
    Queue::enqueue(ctx, "email", Value::String(to), &EnqueueOptions::default())
}

#[mutation]
pub fn remind_me(
    ctx: &mut MutationCtx<'_, '_>,
    id: String,
    to: String,
    delay_ms: f64,
) -> flower_sdk::Result<()> {
    use flower_sdk::timer::Timers;
    if !delay_ms.is_finite() || delay_ms < 0.0 {
        return Err(flower_sdk::fail(
            flower_sdk::error::codes::INVALID_VALUE,
            "delay must be non-negative",
        ));
    }
    Timers::after(
        ctx,
        &id,
        core::time::Duration::from_millis(delay_ms as u64),
        &SEND_EMAIL,
        &SendEmailArgs { to },
    )
}

#[query]
pub fn verify_message(
    ctx: &mut QueryCtx<'_, '_>,
    key: String,
    message: String,
    signature: String,
) -> flower_sdk::Result<bool> {
    use flower_sdk::crypto::{Algorithm, Crypto, ManagedKey, ManagedKeyRef, Usage};
    let key = ManagedKey::new(&key, Algorithm::Ed25519, &[Usage::Verify])?;
    let mut crypto = Crypto::new(ctx.ctx());
    let bytes = flower_sdk::crypto::hex_decode(&signature)?;
    let signature: [u8; 64] = bytes
        .try_into()
        .map_err(|_| flower_sdk::fail("INVALID_VALUE", "signature is 64 bytes"))?;
    crypto.verify(&key, ManagedKeyRef::Current, message.as_bytes(), &signature)
}

/// How many tasks carry a title, and how many rows left the group as
/// deltas since it was last built from its rows: a build never takes a
/// row out, so a count that keeps its removals was maintained by deltas
/// ever since.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct TitleCount {
    pub count: u64,
    pub removed: u64,
}

/// Tasks per title over `by_title`, maintained by deltas.
pub struct TitleCounts;

impl flower_sdk::index::TypedAggregate for TitleCounts {
    type Source = Tasks;
    type Fields = (String,);
    type Group = (String,);
    type Accumulator = TitleCount;
    const NAME: &'static str = "title_count";
    const INDEX: &'static str = "by_title";

    fn initial(_group: &(String,)) -> flower_sdk::Result<TitleCount> {
        Ok(TitleCount {
            count: 0,
            removed: 0,
        })
    }

    fn add(
        total: TitleCount,
        _key: &String,
        _row: &Task,
        _group: &(String,),
    ) -> flower_sdk::Result<TitleCount> {
        Ok(TitleCount {
            count: total.count + 1,
            ..total
        })
    }

    fn remove(
        total: TitleCount,
        _key: &String,
        _row: &Task,
        _group: &(String,),
    ) -> flower_sdk::Result<Option<TitleCount>> {
        Ok(Some(TitleCount {
            count: total.count - 1,
            removed: total.removed + 1,
        }))
    }
}

#[query]
pub fn titled(ctx: &mut QueryCtx<'_, '_>, title: String) -> flower_sdk::Result<TitleCount> {
    use flower_sdk::ReadCtx;
    let mut args = alloc::collections::BTreeMap::new();
    args.insert(
        String::from("group"),
        flower_sdk::Value::Array(alloc::vec![flower_sdk::Value::String(title)]),
    );
    TitleCount::decode(ctx.get_derived("", "title_count", &flower_sdk::Value::Object(args))?)
}

/// Renames one `todo` task to `done` per run, while any is left.
pub struct Tidy;

impl flower_sdk::features::TypedTask for Tidy {
    const NAME: &'static str = "tidy";

    fn due(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Option<f64>> {
        let page = Tasks::BY_TITLE
            .prefix((flower_sdk::Value::String(String::from("todo")),))
            .page(ctx, 1)?;
        Ok((!page.rows.is_empty()).then_some(0.0))
    }

    fn run(ctx: &mut MutationCtx<'_, '_>) -> flower_sdk::Result<flower_sdk::Value> {
        let page = Tasks::BY_TITLE
            .prefix((flower_sdk::Value::String(String::from("todo")),))
            .page(ctx, 1)?;
        let Some(row) = page.rows.into_iter().next() else {
            return Ok(flower_sdk::Value::Null);
        };
        Tasks::set(
            ctx,
            &row.key,
            &Task {
                title: String::from("done"),
            },
        )?;
        Ok(flower_sdk::Value::String(row.key))
    }
}

#[cfg(not(feature = "limits"))]
static TIDY: flower_sdk::Task = <Tidy as flower_sdk::features::TypedTask>::TASK;
/// `send_email`'s queue: guests use only the queues they declare.
#[cfg(not(feature = "limits"))]
static EMAIL: flower_sdk::queue::QueueDef = flower_sdk::queue::QueueDef::new("email");

#[cfg(not(feature = "limits"))]
static PARTS: [&dyn Component; 11] = [
    &Tasks,
    &TIDY,
    &EMAIL,
    &GET,
    &PUT,
    &SIGN_MESSAGE,
    &VERIFY_MESSAGE,
    &SEND_EMAIL,
    &REMIND_ME,
    &<TitleCounts as flower_sdk::index::TypedAggregate>::DERIVED,
    &TITLED,
];
#[cfg(feature = "limits")]
static PARTS: [&dyn Component; 22] = [
    &Tasks,
    &GET,
    &PUT,
    &SIGN_MESSAGE,
    &VERIFY_MESSAGE,
    &SEND_EMAIL,
    &REMIND_ME,
    &limits::INIT_RAN,
    &limits::MEMORY_BOMB,
    &limits::SPIN,
    &limits::RECURSE,
    &limits::RANDOM_QUERY,
    &limits::RANDOM_MUTATION,
    &limits::RANDOM_DERIVED,
    &limits::RANDOM_NESTED,
    &limits::NEST_ALLOC,
    &limits::NEST_FOREVER,
    &limits::BURN,
    &limits::HOST_LOOP,
    &limits::HOST_CALLS,
    &limits::LEAK_WRITE,
    &limits::LEAK_READ,
];
#[cfg(not(feature = "limits"))]
static APP: App = App::new(&PARTS);
#[cfg(feature = "limits")]
static APP: App = App::new(&PARTS).init(limits::mark_init);

export!(APP);

/// The aliases a guest's manifest exposes over HTTP.
#[cfg(test)]
fn http_aliases(app: &App) -> Vec<String> {
    use flower_sdk::Value;
    let bytes = flower_sdk::abi::manifest_native(app);
    let Value::Array(envelope) = Value::decode_binary(&bytes).unwrap() else {
        panic!("manifest envelope");
    };
    let Some(Value::Array(http)) = envelope[1].as_object().unwrap().get("http").cloned() else {
        panic!("manifest without http");
    };
    http.iter()
        .map(|entry| match entry.as_object().unwrap().get("alias") {
            Some(Value::String(alias)) => alias.clone(),
            other => panic!("alias not a string: {other:?}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    /// No methods are exposed over HTTP: engine tests call these methods
    /// directly, and `sign_message` signs for anyone while `send_email` is a
    /// timer handler, so none answers over HTTP.
    #[test]
    fn nothing_answers_over_http() {
        assert_eq!(super::http_aliases(&super::APP), Vec::<String>::new());
    }
}

/// Probes for the engine's runtime-limit tests, behind the `limits`
/// feature. Raw descriptors keep argument handling explicit; every
/// definition takes a JSON object.
#[cfg(feature = "limits")]
mod limits {
    use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use flower_sdk::crypto::Crypto;
    use flower_sdk::{Ctx, Derived, Method, Value, fail};

    /// Set by `flower_init`; instances start with it clear.
    static INITIALIZED: AtomicBool = AtomicBool::new(false);

    /// The `App::init` callback: a visible side effect.
    pub fn mark_init() -> flower_sdk::Result<()> {
        INITIALIZED.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Read a whole-number argument from an args object.
    fn number(args: &Value, field: &str) -> flower_sdk::Result<u64> {
        match args {
            Value::Object(map) => match map.get(field) {
                Some(Value::Number(number)) if *number >= 0.0 => Ok(*number as u64),
                _ => Err(fail("INVALID_VALUE", "expected a non-negative number")),
            },
            _ => Err(fail("INVALID_VALUE", "expected an object")),
        }
    }

    /// Hex entropy through the crypto lane.
    fn random(ctx: &mut Ctx<'_, '_>) -> flower_sdk::Result<Value> {
        let bytes = Crypto::new(ctx).random_bytes(8)?;
        Ok(Value::String(flower_sdk::crypto::hex_encode(&bytes)))
    }

    fn init_ran(_ctx: &mut Ctx<'_, '_>, _args: Value) -> flower_sdk::Result<Value> {
        Ok(Value::Bool(INITIALIZED.load(Ordering::SeqCst)))
    }

    /// Allocate and touch `megabytes` MiB.
    fn memory_bomb(_ctx: &mut Ctx<'_, '_>, args: Value) -> flower_sdk::Result<Value> {
        let bytes = number(&args, "megabytes")? as usize * 1024 * 1024;
        let mut block = vec![0u8; bytes];
        for page in block.chunks_mut(64 * 1024) {
            page[0] = 1;
        }
        let touched = core::hint::black_box(&block)
            .iter()
            .filter(|byte| **byte == 1)
            .count();
        Ok(Value::Number(touched as f64))
    }

    /// Never returns.
    fn spin(_ctx: &mut Ctx<'_, '_>, _args: Value) -> flower_sdk::Result<Value> {
        let mut counter = 0u64;
        loop {
            counter = core::hint::black_box(counter.wrapping_add(1));
        }
    }

    #[inline(never)]
    fn descend(depth: u64) -> u64 {
        if depth == 0 {
            return 0;
        }
        core::hint::black_box(descend(core::hint::black_box(depth - 1))).wrapping_add(depth)
    }

    /// Recurse `depth` frames deep.
    fn recurse(_ctx: &mut Ctx<'_, '_>, args: Value) -> flower_sdk::Result<Value> {
        let depth = number(&args, "depth")?;
        Ok(Value::Number(descend(depth) as f64))
    }

    fn random_any(ctx: &mut Ctx<'_, '_>, _args: Value) -> flower_sdk::Result<Value> {
        random(ctx)
    }

    /// A mutation calling the entropy query: still a query.
    fn random_nested(ctx: &mut Ctx<'_, '_>, _args: Value) -> flower_sdk::Result<Value> {
        let empty = Value::Object(Default::default());
        ctx.method_call("", "random_query", &empty, false)
            .map_err(|failure| failure.failure)
    }

    /// Allocate `megabytes` MiB, then nest `depth` more levels deep.
    fn nest_alloc(ctx: &mut Ctx<'_, '_>, args: Value) -> flower_sdk::Result<Value> {
        let depth = number(&args, "depth")?;
        let megabytes = number(&args, "megabytes")?;
        let mut block = vec![0u8; megabytes as usize * 1024 * 1024];
        for page in block.chunks_mut(64 * 1024) {
            page[0] = 1;
        }
        core::hint::black_box(&block);
        if depth > 0 {
            let mut next = std::collections::BTreeMap::new();
            next.insert(String::from("depth"), Value::Number((depth - 1) as f64));
            next.insert(String::from("megabytes"), Value::Number(megabytes as f64));
            ctx.method_call("", "nest_alloc", &Value::Object(next), false)
                .map_err(|failure| failure.failure)?;
        }
        Ok(Value::Number(depth as f64))
    }

    /// Call itself through the host without end.
    fn nest_forever(ctx: &mut Ctx<'_, '_>, args: Value) -> flower_sdk::Result<Value> {
        ctx.method_call("", "nest_forever", &args, false)
            .map_err(|failure| failure.failure)
    }

    /// Spin `iterations` times, then nest `depth` more levels deep.
    /// Call the host until the invocation stops: what a method paging
    /// rows or polling does, at its cheapest.
    fn host_loop(ctx: &mut Ctx<'_, '_>, _args: Value) -> flower_sdk::Result<Value> {
        loop {
            let _ = core::hint::black_box(ctx.now());
        }
    }

    /// Call the host `calls` times: a long loop of paging or polling,
    /// finished.
    fn host_calls(ctx: &mut Ctx<'_, '_>, args: Value) -> flower_sdk::Result<Value> {
        let calls = number(&args, "calls")?;
        for _ in 0..calls {
            let _ = core::hint::black_box(ctx.now());
        }
        Ok(Value::Number(calls as f64))
    }

    fn burn(ctx: &mut Ctx<'_, '_>, args: Value) -> flower_sdk::Result<Value> {
        let depth = number(&args, "depth")?;
        let iterations = number(&args, "iterations")?;
        let mut counter = 0u64;
        for _ in 0..iterations {
            counter = core::hint::black_box(counter.wrapping_add(1));
        }
        if depth > 0 {
            let mut next = std::collections::BTreeMap::new();
            next.insert(String::from("depth"), Value::Number((depth - 1) as f64));
            next.insert(String::from("iterations"), Value::Number(iterations as f64));
            ctx.method_call("", "burn", &Value::Object(next), false)
                .map_err(|failure| failure.failure)?;
        }
        Ok(Value::Number(counter as f64))
    }

    /// High half of the words `leak_write` leaves behind.
    const LEAK_TAG: u64 = 0xF10E_0000_0000_0000;
    /// Set by `leak_write`; every instance starts with it clear.
    static mut LEAKED: u64 = 0;
    /// Set by `leak_write`; every instance starts with it clear.
    static LEAKED_ATOMIC: AtomicU64 = AtomicU64::new(0);

    /// Tagged word pairs (`w`, `!w`) anywhere in linear memory.
    fn tagged_pairs() -> u64 {
        let end = core::arch::wasm32::memory_size(0) * 64 * 1024;
        // SAFETY: every aligned address below `memory.size` is readable
        // guest memory; volatile reads keep the scan literal.
        let word =
            |at: usize| unsafe { core::ptr::with_exposed_provenance::<u64>(at).read_volatile() };
        let mut found = 0;
        // Address 0 is never read: it is not a valid pointer.
        let mut at = 8;
        while at + 16 <= end {
            let value = word(at);
            if value & 0xFFFF_0000_0000_0000 == LEAK_TAG && word(at + 8) == !value {
                found += 1;
            }
            at += 8;
        }
        found
    }

    /// What `leak_write` leaves, as this instance sees it.
    #[allow(static_mut_refs)]
    fn leak_state() -> Value {
        let mut state = alloc::collections::BTreeMap::new();
        // SAFETY: a single-threaded guest; nothing else touches it.
        let leaked = unsafe { LEAKED } != 0;
        state.insert(String::from("static"), Value::Bool(leaked));
        let atomic = LEAKED_ATOMIC.load(Ordering::SeqCst) != 0;
        state.insert(String::from("atomic"), Value::Bool(atomic));
        let pages = core::arch::wasm32::memory_size(0) as f64;
        state.insert(String::from("pages"), Value::Number(pages));
        state.insert(String::from("pairs"), Value::Number(tagged_pairs() as f64));
        let init = INITIALIZED.load(Ordering::SeqCst);
        state.insert(String::from("init"), Value::Bool(init));
        Value::Object(state)
    }

    /// Leave a marker in statics, on the heap, in a freshly grown page
    /// and in host-call buffer memory, then report this instance's state
    /// — or trap, when `trap` is 1.
    fn leak_write(_ctx: &mut Ctx<'_, '_>, args: Value) -> flower_sdk::Result<Value> {
        let marker = LEAK_TAG | number(&args, "marker")?;
        // SAFETY: a single-threaded guest; nothing else touches it.
        unsafe { LEAKED = marker };
        LEAKED_ATOMIC.store(marker, Ordering::SeqCst);
        core::hint::black_box(alloc::vec![marker, !marker].leak());
        let page = core::arch::wasm32::memory_grow(0, 1);
        if page != usize::MAX {
            let at = core::ptr::with_exposed_provenance_mut::<u64>(page * 64 * 1024);
            // SAFETY: the page just grown belongs to no allocation.
            unsafe {
                at.write_volatile(marker);
                at.add(1).write_volatile(!marker);
            }
        }
        let buffer = flower_sdk::abi::guest_alloc(16).cast::<u64>();
        if !buffer.is_null() {
            // SAFETY: a fresh 16-byte, 8-aligned guest allocation.
            unsafe {
                buffer.write_volatile(marker);
                buffer.add(1).write_volatile(!marker);
            }
        }
        if number(&args, "trap")? == 1 {
            core::arch::wasm32::unreachable();
        }
        Ok(leak_state())
    }

    /// This instance's state: what `leak_write` left, if anything.
    fn leak_read(_ctx: &mut Ctx<'_, '_>, _args: Value) -> flower_sdk::Result<Value> {
        Ok(leak_state())
    }

    pub static INIT_RAN: Method = Method::query("init_ran", init_ran);
    pub static LEAK_WRITE: Method = Method::mutation("leak_write", leak_write);
    pub static LEAK_READ: Method = Method::query("leak_read", leak_read);
    pub static MEMORY_BOMB: Method = Method::query("memory_bomb", memory_bomb);
    pub static SPIN: Method = Method::query("spin", spin);
    pub static RECURSE: Method = Method::query("recurse", recurse);
    pub static RANDOM_QUERY: Method = Method::query("random_query", random_any);
    pub static RANDOM_MUTATION: Method = Method::mutation("random_mutation", random_any);
    pub static RANDOM_DERIVED: Derived = Derived::new("random_derived", random_any);
    pub static RANDOM_NESTED: Method = Method::mutation("random_nested", random_nested);
    pub static NEST_ALLOC: Method = Method::mutation("nest_alloc", nest_alloc);
    pub static NEST_FOREVER: Method = Method::mutation("nest_forever", nest_forever);
    pub static BURN: Method = Method::mutation("burn", burn);
    pub static HOST_LOOP: Method = Method::query("host_loop", host_loop);
    pub static HOST_CALLS: Method = Method::query("host_calls", host_calls);
}
