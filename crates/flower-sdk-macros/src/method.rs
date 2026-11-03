//! `#[query]` / `#[mutation]` / `#[derived]` method adapters.
//!
//! Each macro keeps the annotated function, derives an argument struct from
//! its parameters, and emits a raw-callback shim plus a registerable
//! descriptor static carrying every option of the operation:
//!
//! ```ignore
//! #[query(name = "tasks.get", consistency = replica_local, access = authenticated)]
//! pub fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> Task { todo!() }
//! ```
//!
//! becomes the original function plus `GetTaskArgs` (a derived
//! `FlowerType` with one field per argument), the
//! `__flower_export_get_task` shim with `RawCallback` shape, and
//! `pub static GET_TASK: Op<GetTaskArgs, Task, Failure>` (a `Method`
//! with its types; `#[derived]` emits a `Derived`). Apps list `&GET_TASK` among their
//! components. Fallible methods return `Result<T, E>` with
//! `E: BusinessError` (including `Failure` itself).
//!
//! Options (all optional):
//!
//! - `name = "..."`: the definition and HTTP name (default: the function's).
//! - `http`: callable over HTTP under its name. Without it only timers,
//!   queues, triggers, other methods and (when exported) other modules
//!   call it, as with v1's `http` allowlist.
//! - `consistency = linearizable | replica_local`, or
//!   `max_staleness_ms = N` (replica-local within a bound): queries only.
//! - `access = public | authenticated | path::to::check`: who may call,
//!   decided by the module's authorization component.
//! - `export`, or `callers("module", ...)`: callable by other modules
//!   (all of them, or only those).
//! - `timeout = "250ms"` (`ms`, `s` or `m`): the method's own deadline.
//! - `rename_all = "camelCase"`: the argument members' wire names.
//! - `bare_arg`: the only parameter is the whole argument (a string, a
//!   tuple, a struct) rather than one member of an argument object.
//!
//! An `Option` parameter is an optional member: callers may leave it out.
//! - On `#[derived]`: `name`, `export`, `materialize = always | never |
//!   each(Marker)` (one root per row of a `#[collection]`) and `access =
//!   rule` (a `fn() -> access::Rule`), as v1's `derive` options.

use crate::attrs;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::{FnArg, ItemFn, Pat, Type};

/// Which entry point an adapter wraps.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Query,
    Mutation,
    Derived,
}

impl Kind {
    pub(crate) fn wrap_fn(self) -> proc_macro2::TokenStream {
        match self {
            Kind::Query => quote!(::flower_sdk::wrap_query),
            Kind::Mutation => quote!(::flower_sdk::wrap_mutation),
            Kind::Derived => quote!(::flower_sdk::wrap_derived),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Kind::Query => "query",
            Kind::Mutation => "mutation",
            Kind::Derived => "derived",
        }
    }
}

/// Parsed operation options.
#[derive(Default)]
pub(crate) struct Options {
    name: Option<String>,
    http: bool,
    consistency: Option<proc_macro2::TokenStream>,
    access: Option<proc_macro2::TokenStream>,
    export: Option<Vec<String>>,
    timeout_ms: Option<u32>,
    materialize: Option<proc_macro2::TokenStream>,
    camel: bool,
    bare: bool,
}

/// Parse `"250ms"`, `"2s"` or `"1m"` into whole milliseconds.
fn parse_duration(text: &str) -> Option<u32> {
    // `ms` before `s`: "5ms" also ends in "s".
    let (digits, scale) = [("ms", 1), ("s", 1_000), ("m", 60_000)]
        .into_iter()
        .find_map(|(suffix, scale)| text.strip_suffix(suffix).map(|digits| (digits, scale)))?;
    let value: u32 = digits.trim().parse().ok()?;
    value.checked_mul(scale).filter(|ms| *ms > 0)
}

/// `materialize = always | never | each(Marker)`: v1's `"always"` and
/// `{each: collection}`, the collection named by its `#[collection]`
/// marker.
fn materialize(meta: &syn::Meta) -> syn::Result<proc_macro2::TokenStream> {
    let expected = || {
        syn::Error::new_spanned(
            meta,
            "expected `always`, `never` or `each(Collection)`, a `#[collection]` marker",
        )
    };
    if let syn::Meta::NameValue(value) = meta
        && let syn::Expr::Call(call) = &value.value
    {
        let syn::Expr::Path(func) = &*call.func else {
            return Err(expected());
        };
        if !func.path.is_ident("each") || call.args.len() != 1 {
            return Err(expected());
        }
        let Some(syn::Expr::Path(marker)) = call.args.first() else {
            return Err(expected());
        };
        let marker = &marker.path;
        return Ok(quote!(::flower_sdk::Materialize::Each(#marker::raw())));
    }
    match word(meta)?.as_str() {
        "always" => Ok(quote!(::flower_sdk::Materialize::Always)),
        "never" => Ok(quote!(::flower_sdk::Materialize::Never)),
        _ => Err(expected()),
    }
}

fn word(meta: &syn::Meta) -> syn::Result<String> {
    let path = attrs::path_value(meta)?;
    path.get_ident()
        .map(ToString::to_string)
        .ok_or_else(|| syn::Error::new_spanned(path, "expected a single word"))
}

pub(crate) fn options(kind: Kind, attr: proc_macro2::TokenStream) -> syn::Result<Options> {
    use syn::parse::Parser;
    let metas =
        syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated.parse2(attr)?;
    let mut options = Options::default();
    let query_only = |meta: &syn::Meta| -> syn::Result<()> {
        if kind == Kind::Query {
            Ok(())
        } else {
            Err(syn::Error::new_spanned(
                meta,
                "only queries take a consistency",
            ))
        }
    };
    let methods_only = |meta: &syn::Meta| -> syn::Result<()> {
        if kind == Kind::Derived {
            Err(syn::Error::new_spanned(
                meta,
                "derived values take only `name`, `export`, `materialize` and `access`",
            ))
        } else {
            Ok(())
        }
    };
    for meta in &metas {
        let key = meta.path().get_ident().map(ToString::to_string);
        match key.as_deref() {
            Some("name") if options.name.is_none() => {
                let name = attrs::lit_str_value(meta)?;
                if name.is_empty() {
                    return Err(syn::Error::new_spanned(meta, "names are not empty"));
                }
                options.name = Some(name);
            }
            Some("http") if matches!(meta, syn::Meta::Path(_)) && !options.http => {
                methods_only(meta)?;
                options.http = true;
            }
            Some("export") if matches!(meta, syn::Meta::Path(_)) && options.export.is_none() => {
                options.export = Some(Vec::new());
            }
            Some("callers") if options.export.is_none() => {
                methods_only(meta)?;
                let syn::Meta::List(list) = meta else {
                    return Err(syn::Error::new_spanned(
                        meta,
                        "expected `callers(\"module\", ...)`",
                    ));
                };
                let modules =
                    syn::punctuated::Punctuated::<syn::LitStr, syn::Token![,]>::parse_terminated
                        .parse2(list.tokens.clone())?;
                if modules.is_empty() {
                    return Err(syn::Error::new_spanned(
                        meta,
                        "callers names at least one module",
                    ));
                }
                options.export = Some(modules.iter().map(syn::LitStr::value).collect());
            }
            Some("consistency") if options.consistency.is_none() => {
                query_only(meta)?;
                options.consistency = Some(match word(meta)?.as_str() {
                    "linearizable" => quote!(::flower_sdk::Consistency::Linearizable),
                    "replica_local" => quote!(::flower_sdk::Consistency::ReplicaLocal),
                    _ => {
                        return Err(syn::Error::new_spanned(
                            meta,
                            "expected `linearizable` or `replica_local`",
                        ));
                    }
                });
            }
            Some("max_staleness_ms") if options.consistency.is_none() => {
                query_only(meta)?;
                let syn::Meta::NameValue(value) = meta else {
                    return Err(syn::Error::new_spanned(
                        meta,
                        "expected `max_staleness_ms = N`",
                    ));
                };
                let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(ms),
                    ..
                }) = &value.value
                else {
                    return Err(syn::Error::new_spanned(
                        &value.value,
                        "expected whole milliseconds",
                    ));
                };
                let ms: u32 = ms.base10_parse()?;
                if ms > (1 << 31) - 1 {
                    return Err(syn::Error::new_spanned(ms, "at most 2^31 - 1 milliseconds"));
                }
                options.consistency =
                    Some(quote!(::flower_sdk::Consistency::ReplicaLocalWithin(#ms)));
            }
            Some("access") if options.access.is_none() && kind == Kind::Derived => {
                // A derived value's access is a rule over principal, args
                // and now (`fn() -> access::Rule`), as v1's `derive`
                // option.
                let path = attrs::path_value(meta)?;
                options.access = Some(quote!(#path));
            }
            Some("access") if options.access.is_none() => {
                methods_only(meta)?;
                let path = attrs::path_value(meta)?;
                options.access = Some(match path.get_ident().map(ToString::to_string).as_deref() {
                    Some("public") => quote!(::flower_sdk::auth::Access::Public),
                    Some("authenticated") => quote!(::flower_sdk::auth::Access::Authenticated),
                    _ => quote!(::flower_sdk::auth::Access::Check(#path)),
                });
            }
            Some("timeout") if options.timeout_ms.is_none() => {
                methods_only(meta)?;
                let text = attrs::lit_str_value(meta)?;
                options.timeout_ms = Some(parse_duration(&text).ok_or_else(|| {
                    syn::Error::new_spanned(
                        meta,
                        "expected a positive duration such as \"250ms\", \"2s\" or \"1m\"",
                    )
                })?);
            }
            Some("rename_all") if !options.camel => {
                if attrs::lit_str_value(meta)? != "camelCase" {
                    return Err(syn::Error::new_spanned(
                        meta,
                        "rename_all supports only \"camelCase\"",
                    ));
                }
                options.camel = true;
            }
            Some("bare_arg") if matches!(meta, syn::Meta::Path(_)) && !options.bare => {
                options.bare = true;
            }
            Some("materialize") if kind == Kind::Derived && options.materialize.is_none() => {
                options.materialize = Some(materialize(meta)?);
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    meta,
                    format!(
                        "unknown or repeated #[{}] option (expected name, http, consistency, \
                         max_staleness_ms, access, export, callers, timeout, rename_all, bare_arg{})",
                        kind.as_str(),
                        if kind == Kind::Derived {
                            ", materialize"
                        } else {
                            ""
                        },
                    ),
                ));
            }
        }
    }
    Ok(options)
}

/// The `UpperCamelCase` name of a function's argument struct (`r#type`
/// gives `TypeArgs`).
pub(crate) fn args_struct(name: &syn::Ident) -> syn::Ident {
    let mut camel = String::new();
    for part in name
        .unraw()
        .to_string()
        .split('_')
        .filter(|part| !part.is_empty())
    {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            camel.extend(first.to_uppercase());
            camel.push_str(chars.as_str());
        }
    }
    format_ident!("{}Args", camel, span = name.span())
}

pub(crate) fn entry(
    kind: Kind,
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match expand(kind, attr.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

pub(crate) struct Signature {
    pub(crate) name: syn::Ident,
    pub(crate) args: Vec<(syn::Ident, Type)>,
    pub(crate) fallible: bool,
    /// The success type: `T` of `Result<T, E>`, or the whole output.
    pub(crate) output: Type,
    /// The declared error type, when the output names one.
    pub(crate) error: Option<Type>,
}

/// Split a `Result<T, E>` (or one-parameter `Result<T>` alias) output
/// into its success and error types; any other output is the success
/// type itself.
pub(crate) fn split_output(output: &syn::ReturnType) -> (bool, Type, Option<Type>) {
    let ty: Type = match output {
        syn::ReturnType::Default => syn::parse_quote!(()),
        syn::ReturnType::Type(_, ty) => (**ty).clone(),
    };
    if let Type::Path(path) = &ty
        && let Some(segment) = path.path.segments.last()
        && segment.ident == "Result"
    {
        let mut types = Vec::new();
        if let syn::PathArguments::AngleBracketed(generic) = &segment.arguments {
            for argument in &generic.args {
                if let syn::GenericArgument::Type(ty) = argument {
                    types.push(ty.clone());
                }
            }
        }
        return match types.as_slice() {
            [ok, error] => (true, ok.clone(), Some(error.clone())),
            [ok] => (true, ok.clone(), None),
            _ => (true, ty.clone(), None),
        };
    }
    (false, ty, None)
}

pub(crate) fn signature(kind: Kind, sig: &syn::Signature) -> syn::Result<Signature> {
    if sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            sig,
            format!("{} methods must be synchronous", kind.as_str()),
        ));
    }
    if sig.unsafety.is_some() || sig.abi.is_some() || sig.variadic.is_some() {
        return Err(syn::Error::new_spanned(
            sig,
            format!("{} methods must use a plain Rust signature", kind.as_str()),
        ));
    }
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &sig.generics,
            format!(
                "{} methods cannot be generic; monomorphize before registering",
                kind.as_str()
            ),
        ));
    }
    let inputs: Vec<&FnArg> = sig.inputs.iter().collect();
    let [first, rest @ ..] = inputs.as_slice() else {
        return Err(syn::Error::new_spanned(
            sig,
            format!("{} methods need a context receiver first", kind.as_str()),
        ));
    };
    match first {
        FnArg::Typed(arg) => {
            let name = match arg.pat.as_ref() {
                Pat::Ident(named) => Some(named.ident.to_string()),
                _ => None,
            }
            .unwrap_or_default();
            if name != "ctx" {
                return Err(syn::Error::new_spanned(
                    arg.pat.as_ref(),
                    "the context receiver must be named `ctx`",
                ));
            }
        }
        FnArg::Receiver(_) => {
            return Err(syn::Error::new_spanned(
                first,
                format!("{} methods cannot take `self`", kind.as_str()),
            ));
        }
    }
    let mut args = Vec::new();
    for input in rest {
        let FnArg::Typed(arg) = input else {
            return Err(syn::Error::new_spanned(
                input,
                "unexpected receiver in argument position",
            ));
        };
        let Pat::Ident(name) = arg.pat.as_ref() else {
            return Err(syn::Error::new_spanned(
                arg.pat.as_ref(),
                "arguments must be plain identifiers",
            ));
        };
        args.push((name.ident.clone(), (*arg.ty).clone()));
    }
    let (fallible, output, error) = split_output(&sig.output);
    Ok(Signature {
        name: sig.ident.clone(),
        args,
        fallible,
        output,
        error,
    })
}

fn expand(
    kind: Kind,
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let options = options(kind, attr)?;
    let fun: ItemFn = syn::parse2(item)?;
    let sig = signature(kind, &fun.sig)?;
    let wrap_fn = kind.wrap_fn();
    let error_type = match &sig.error {
        Some(error) => quote!(#error),
        None => quote!(::flower_sdk::Failure),
    };

    let method_name = options
        .name
        .clone()
        .unwrap_or_else(|| sig.name.unraw().to_string());
    let args_ident = args_struct(&sig.name);
    let export_name = format_ident!("__flower_export_{}", sig.name);
    let static_name = attrs::screaming(&sig.name, "")?;
    let arg_names: Vec<&syn::Ident> = sig.args.iter().map(|(name, _)| name).collect();
    let arg_types: Vec<&Type> = sig.args.iter().map(|(_, ty)| ty).collect();
    let vis = &fun.vis;
    let method_ident = &sig.name;
    let output = &sig.output;
    let errors = match &sig.error {
        Some(error) => quote!(<#error as ::flower_sdk::BusinessError>::SCHEMA),
        None => quote!(::flower_sdk::schema::v::json()),
    };
    let constructor = match kind {
        Kind::Query => quote!(::flower_sdk::Method::query),
        Kind::Mutation => quote!(::flower_sdk::Method::mutation),
        Kind::Derived => quote!(::flower_sdk::Derived::new),
    };
    let mut builders = Vec::new();
    if options.http {
        builders.push(quote!(.http()));
    }
    if let Some(consistency) = &options.consistency {
        builders.push(quote!(.consistency(#consistency)));
    }
    if let Some(access) = &options.access {
        builders.push(quote!(.access(#access)));
    }
    match &options.export {
        Some(modules) if modules.is_empty() => builders.push(quote!(.export())),
        Some(modules) => builders.push(quote!(.export_to(&[#(#modules),*]))),
        None => {}
    }
    if let Some(timeout_ms) = options.timeout_ms {
        builders.push(quote!(.timeout_ms(#timeout_ms)));
    }
    if let Some(materialize) = &options.materialize {
        builders.push(quote!(.materialize(#materialize)));
    }
    let arg_docs =
        format!("Arguments of [`{method_name}`]({method_ident}), one field per parameter.");
    let descriptor = if kind == Kind::Derived {
        quote!(::flower_sdk::Derived)
    } else {
        quote!(::flower_sdk::Op<#args_ident, #output, #error_type>)
    };
    if options.bare && options.camel {
        return Err(syn::Error::new_spanned(
            &sig.name,
            "a bare argument has no members to rename",
        ));
    }
    let args_item = if options.bare {
        let [(_, ty)] = sig.args.as_slice() else {
            return Err(syn::Error::new_spanned(
                &sig.name,
                "bare_arg methods take exactly one parameter after `ctx`",
            ));
        };
        let docs = format!("The argument of [`{method_name}`]({method_ident}).");
        quote! {
            #[doc = #docs]
            #vis type #args_ident = #ty;
        }
    } else {
        let rename = if options.camel {
            quote!(#[flower(rename_all = "camelCase")])
        } else {
            quote!()
        };
        // An `Option` parameter may be left out, not only sent as null.
        let optional: Vec<proc_macro2::TokenStream> = arg_types
            .iter()
            .map(|ty| match ty {
                Type::Path(path)
                    if path
                        .path
                        .segments
                        .last()
                        .is_some_and(|segment| segment.ident == "Option") =>
                {
                    quote!(#[flower(default)])
                }
                _ => quote!(),
            })
            .collect();
        quote! {
            #[doc = #arg_docs]
            #[derive(::flower_sdk::FlowerType)]
            #rename
            #vis struct #args_ident {
                #( #optional pub #arg_names: #arg_types, )*
            }
        }
    };
    // Arguments go straight from the decoded struct into the call: a
    // local named after a parameter would shadow a function of the same
    // name (`fn shop(ctx, shop: ShopId)`).
    let (unpack, passed) = if options.bare {
        (
            quote!(let __arg = <#args_ident as ::flower_sdk::FlowerType>::decode(__value)?;),
            vec![quote!(__arg)],
        )
    } else {
        (
            quote!(let __args = <#args_ident as ::flower_sdk::FlowerType>::decode(__value)?;),
            arg_names.iter().map(|name| quote!(__args.#name)).collect(),
        )
    };

    let invoke = if sig.fallible {
        quote! {
            match #method_ident(__ctx, #(#passed,)*) {
                ::core::result::Result::Ok(__output) => {
                    ::flower_sdk::FlowerType::encode(&__output)
                }
                ::core::result::Result::Err(__error) => {
                    ::core::result::Result::Err(
                        ::flower_sdk::BusinessError::into_failure(__error),
                    )
                }
            }
        }
    } else {
        quote! {
            ::flower_sdk::FlowerType::encode(
                &#method_ident(__ctx, #(#passed,)*),
            )
        }
    };

    let built = quote! {
        #constructor(#method_name, #export_name)
            .args(<#args_ident as ::flower_sdk::FlowerType>::SCHEMA)
            .result(<#output as ::flower_sdk::FlowerType>::SCHEMA)
            .errors(#errors)
            #(#builders)*
    };
    let initializer = if kind == Kind::Derived {
        built
    } else {
        quote!(::flower_sdk::Op::new(#built))
    };

    Ok(quote! {
        #fun

        #args_item

        /// Raw-callback shim registering this method with an `App`.
        #vis fn #export_name<'c, 'r>(
            __ctx: &'c mut ::flower_sdk::Ctx<'c, 'r>,
            __value: ::flower_sdk::Value,
        ) -> ::flower_sdk::Result<::flower_sdk::Value> {
            let __ctx = #wrap_fn(__ctx);
            #unpack
            #invoke
        }

        /// The operation's descriptor: list it among the app's components.
        #vis static #static_name: #descriptor = #initializer;
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand_tokens(
        kind: Kind,
        item: proc_macro2::TokenStream,
    ) -> syn::Result<proc_macro2::TokenStream> {
        expand(kind, quote!(), item)
    }

    #[test]
    fn query_adapter_has_raw_callback_shape() {
        let out = expand_tokens(
            Kind::Query,
            quote! {
                pub fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> Task {
                    todo!()
                }
            },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("wrap_query"));
        assert!(out.contains("__flower_export_get_task"));
        assert!(out.contains("static GET_TASK"));
        assert!(out.contains("Method :: query"));
        assert!(out.contains("Op < GetTaskArgs , Task , :: flower_sdk :: Failure >"));
        assert!(out.contains("struct GetTaskArgs"));
        assert!(out.contains("& 'c mut :: flower_sdk :: Ctx"));
        assert!(out.contains("derive (:: flower_sdk :: FlowerType)"));
    }

    #[test]
    fn fallible_methods_map_errors_into_failures() {
        let out = expand_tokens(
            Kind::Mutation,
            quote! {
                fn close_task(ctx: &mut MutationCtx<'_, '_>) -> Result<(), TaskError> {
                    Ok(())
                }
            },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("wrap_mutation"));
        assert!(out.contains("BusinessError :: into_failure"));
        assert!(out.contains("static CLOSE_TASK"));
    }

    #[test]
    fn derived_methods_emit_derived_statics() {
        let out = expand_tokens(
            Kind::Derived,
            quote! {
                fn task_count(ctx: &mut DerivedCtx<'_, '_>) -> u64 {
                    0
                }
            },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("wrap_derived"));
        assert!(out.contains("static TASK_COUNT"));
        assert!(out.contains("Derived :: new"));
    }

    #[test]
    fn async_methods_are_rejected() {
        let err = expand_tokens(
            Kind::Query,
            quote! {
                async fn get_task(ctx: &mut QueryCtx<'_, '_>) -> Task {
                    todo!()
                }
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("synchronous"));
    }

    #[test]
    fn generic_methods_are_rejected() {
        let err = expand_tokens(
            Kind::Query,
            quote! {
                fn get_row<T>(ctx: &mut QueryCtx<'_, '_>, id: String) -> T {
                    todo!()
                }
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot be generic"));
    }

    #[test]
    fn missing_ctx_is_rejected() {
        let err = expand_tokens(
            Kind::Query,
            quote! {
                fn get_task(id: String) -> Task {
                    todo!()
                }
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("named `ctx`"));
    }

    #[test]
    fn self_receivers_are_rejected() {
        let err = expand_tokens(
            Kind::Query,
            quote! {
                fn get_task(&self, ctx: &mut QueryCtx<'_, '_>) -> Task {
                    todo!()
                }
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot take `self`"));
    }

    #[test]
    fn options_land_on_the_descriptor() {
        let out = expand(
            Kind::Query,
            quote!(
                name = "tasks.get",
                http,
                max_staleness_ms = 500,
                access = owns,
                callers("billing"),
                timeout = "2s"
            ),
            quote! {
                fn get_task(ctx: &mut QueryCtx<'_, '_>) -> Task {
                    todo!()
                }
            },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("\"tasks.get\""));
        assert!(out.contains(". http ()"));
        assert!(out.contains("ReplicaLocalWithin (500u32)"));
        assert!(out.contains("Access :: Check (owns)"));
        assert!(out.contains("export_to (& [\"billing\"])"));
        assert!(out.contains("timeout_ms (2000u32)"));
    }

    #[test]
    fn bad_options_are_rejected() {
        let item = quote! {
            fn put_task(ctx: &mut MutationCtx<'_, '_>) -> Task {
                todo!()
            }
        };
        for (attr, message) in [
            (quote!(consistency = replica_local), "only queries"),
            (quote!(timeout = "soon"), "positive duration"),
            (quote!(name = "a", name = "b"), "unknown or repeated"),
            (quote!(color = "red"), "unknown or repeated"),
        ] {
            let err = expand(Kind::Mutation, attr, item.clone()).unwrap_err();
            assert!(err.to_string().contains(message), "{err}");
        }
    }

    #[test]
    fn bare_and_camel_case_arguments() {
        let item = quote! {
            fn shop(ctx: &mut QueryCtx<'_, '_>, shop: ShopId) -> Shop {
                todo!()
            }
        };
        let out = expand(Kind::Query, quote!(bare_arg), item.clone())
            .unwrap()
            .to_string();
        assert!(out.contains("type ShopArgs = ShopId"));
        let out = expand(Kind::Query, quote!(rename_all = "camelCase"), item.clone())
            .unwrap()
            .to_string();
        assert!(out.contains("rename_all = \"camelCase\""));
        let two = quote! {
            fn tip(ctx: &mut MutationCtx<'_, '_>, shop: ShopId, amount: u32) -> u32 {
                todo!()
            }
        };
        let err = expand(Kind::Mutation, quote!(bare_arg), two).unwrap_err();
        assert!(err.to_string().contains("exactly one parameter"));
    }

    #[test]
    fn raw_identifiers_name_plain_statics_structs_and_methods() {
        let out = expand_tokens(
            Kind::Query,
            quote! {
                pub fn r#type(ctx: &mut QueryCtx<'_, '_>, r#type: String) -> String {
                    todo!()
                }
            },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("struct TypeArgs"), "{out}");
        assert!(out.contains("static TYPE"), "{out}");
        assert!(out.contains("Method :: query (\"type\""), "{out}");
    }

    #[test]
    fn derived_values_take_v1s_options() {
        // v1's `derive(name, compute, {materialize, access})`:
        // materialization always, never, or one root per row of a
        // collection, named by its marker; and an access rule.
        let item = quote! {
            fn shop_total(ctx: &mut DerivedCtx<'_, '_>, shop: String) -> u64 {
                todo!()
            }
        };
        let out = expand(
            Kind::Derived,
            quote!(materialize = each(Orders), access = admins),
            item.clone(),
        )
        .unwrap()
        .to_string();
        assert!(
            out.contains("Materialize :: Each (Orders :: raw ())"),
            "{out}"
        );
        assert!(out.contains(". access (admins)"), "{out}");
        let out = expand(Kind::Derived, quote!(materialize = always), item)
            .unwrap()
            .to_string();
        assert!(out.contains("Materialize :: Always"), "{out}");
    }

    #[test]
    fn durations_parse_to_milliseconds() {
        assert_eq!(parse_duration("250ms"), Some(250));
        assert_eq!(parse_duration("2s"), Some(2_000));
        assert_eq!(parse_duration("1m"), Some(60_000));
        assert_eq!(parse_duration("0s"), None);
        assert_eq!(parse_duration("5"), None);
    }
}
