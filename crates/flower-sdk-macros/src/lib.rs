//! Procedural macros for Flower guest applications: type and error
//! derivation, collection declarations and method adapters.
//!
//! Every macro emits fully-qualified `::flower_sdk` and `::alloc` paths, so
//! generated code needs no imports. Guest crates use `alloc` directly.

mod application;
mod attrs;
mod collection;
mod flowererror;
mod flowertype;
mod interface;
mod method;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Assemble an inline module's methods, collections and `#[component]` statics.
/// Export the resulting `module::APP` explicitly with `flower_sdk::export!`.
#[proc_macro_attribute]
pub fn application(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "application takes no arguments",
        )
        .to_compile_error()
        .into();
    }
    match application::expand(parse_macro_input!(item as syn::ItemMod)) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derive `FlowerType`: closed objects for named structs, string enums for
/// unit enums, and `{"tag", "value"}` objects for single-payload enums.
///
/// Field options: `#[flower(skip)]` excludes a field from the schema and the
/// encoding (decoding fills `Default`), and `#[flower(default)]` (or
/// `#[flower(default = path)]`) fills absent members. Renaming, flattening
/// and redacting are rejected: wire names match Rust field names, nesting
/// stays explicit, and redaction uses `typed::Field<T>` as the field type.
#[proc_macro_derive(FlowerType, attributes(flower))]
pub fn derive_flower_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    match flowertype::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Derive `BusinessError` for failure enums: unit variants map to bare codes
/// and single-payload variants to code/message/details failures.
///
/// Every variant requires `#[flower(code = "...")]`, with an optional
/// `message = "..."` (the variant's name by default) and an optional
/// `#[flower(retryable)]` marker feeding the generated `is_retryable`
/// helper. Payloads encode into `details` and must implement `FlowerType`.
/// One `#[flower(other)] Host(Failure)` variant takes every other failure,
/// so `?` converts SDK failures through the generated `From<Failure>`.
#[proc_macro_derive(FlowerError, attributes(flower))]
pub fn derive_flower_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    match flowererror::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Declare a collection on a marker struct: the marker is the typed
/// handle and the component apps list.
///
/// ```ignore
/// #[collection(
///     name = "tasks",
///     key = TaskId,
///     value = Task,
///     index(name = "by_owner", fields(owner)),
///     reference(target = Posts, fields(post), on_delete = cascade),
///     export,
/// )]
/// pub struct Tasks;
///
/// static APP: App = App::new(&[&Tasks, ...]);
/// ```
///
/// The handle exposes `NAME`, `SOURCE`, `raw()`, `page` and typed
/// `get`/`set`/`delete` by key. Index `fields` and reference `fields`
/// name wire fields. References use `fields(...)` (also `key` or
/// `key_prefix` instead of `fields`), `target` or `targets(...)` with
/// `discriminator`, and `on_delete` (`restrict`, `cascade` or
/// `set_null`). `export` lets other modules read the collection, and
/// `access = policy` (a `fn() -> flower_sdk::access::Policy`) guards
/// callers' operations on it.
#[proc_macro_attribute]
pub fn collection(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as collection::AttributeArgs);
    let item = parse_macro_input!(item as syn::ItemStruct);
    match collection::expand(args, item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Declare a query: one declaration carries the whole operation. Keeps the
/// function, derives its public argument struct (`GetTaskArgs` for
/// `get_task`), and emits a raw-callback shim plus a `Method` static
/// named after the function in upper case, to list among the app's
/// components.
///
/// ```ignore
/// #[query(name = "tasks.get", consistency = replica_local, access = owns, timeout = "200ms")]
/// fn get_task(ctx: &mut QueryCtx<'_, '_>, id: TaskId) -> Result<Option<Task>> { ... }
/// ```
///
/// The first parameter must be the context receiver named `ctx`; the rest
/// become decoded arguments. Fallible methods return `Result<T, E>` where `E`
/// implements `BusinessError` (including `Failure` itself). Options:
/// `name = "..."` (default: the function's name), `http` (callable over
/// HTTP under that name; nothing else is), `consistency = linearizable | replica_local` or
/// `max_staleness_ms = N`, `access = public | authenticated | path`,
/// `export` or `callers("module", ...)`, and `timeout = "250ms"`.
#[proc_macro_attribute]
pub fn query(attr: TokenStream, item: TokenStream) -> TokenStream {
    method::entry(method::Kind::Query, attr, item)
}

/// Declare a mutation: same shape and options as [`query`] except the
/// consistency, wrapping the context as `MutationCtx`.
#[proc_macro_attribute]
pub fn mutation(attr: TokenStream, item: TokenStream) -> TokenStream {
    method::entry(method::Kind::Mutation, attr, item)
}

/// Declare a derived value: same shape as [`query`], wrapping the context
/// as `DerivedCtx` and emitting a `Derived` static. Options: `name`,
/// `export` and `materialize = always | never`.
#[proc_macro_attribute]
pub fn derived(attr: TokenStream, item: TokenStream) -> TokenStream {
    method::entry(method::Kind::Derived, attr, item)
}

/// Declare a module contract as a trait of `#[query]`, `#[mutation]` and
/// `#[derived]` signatures, for an interface crate that provider and
/// consumers both depend on:
/// Add `client = TasksClient` to generate a native facade bound with
/// `TasksApi::client(module)`. Queries keep their names and mutations expose
/// `prepare_*` methods; the backend controls transport, metadata and futures.
///
/// ```ignore
/// #[interface(name = "tasks.v1", handle = TasksApi)]
/// pub trait Tasks {
///     #[query]
///     fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> Option<Task>;
/// }
/// ```
///
/// Consumers declare `static TASKS: TasksApi = TasksApi::import("$tasks")`
/// once: it is the typed call handle (with `try_*` savepoint variants)
/// and the component to list. Providers implement the trait on a marker
/// type `P` and list `TasksProvider::<P>::COMPONENT`, which brings every
/// method exported (`provider = Name` renames it). Both sides derive
/// their contracts from the same signatures, so deployment links them.
#[proc_macro_attribute]
pub fn interface(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as syn::ItemTrait);
    match interface::expand(attr.into(), item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
