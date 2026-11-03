//! `#[interface]`: a module contract shared by provider and consumers.
//!
//! An interface is a trait of method signatures, each marked `#[query]`,
//! `#[mutation]` or `#[derived]`, in a crate both sides depend on:
//!
//! ```ignore
//! #[interface(name = "tasks.v1", handle = TasksApi)]
//! pub trait Tasks {
//!     #[query]
//!     fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> Option<Task>;
//!     #[mutation]
//!     fn add_task(ctx: &mut MutationCtx<'_, '_>, title: String) -> Result<String, TaskError>;
//!     #[derived]
//!     fn open_count(ctx: &mut DerivedCtx<'_, '_>) -> u64;
//! }
//! ```
//!
//! The trait stays for the provider to implement. Consumers get the
//! handle: `static TASKS: TasksApi = TasksApi::import("$tasks")` is both
//! the typed calls (`get_task`, and `try_get_task` through a savepoint)
//! and the component declaring the interface's contracts against that
//! module or slot. Providers get `TasksProvider::<P>` (or
//! `provider = Name`), whose associated consts are the exported
//! `Method`/`Derived` descriptors over `P`'s implementation and whose
//! `COMPONENT` brings them all, for other modules only;
//! `COMPONENT.http()` also lets callers reach the methods over HTTP under
//! their names. The authorization component's `methods` table says who
//! may call each. Both sides take their contracts from the same
//! signatures, so they link at deployment.

use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{Ident, ItemTrait, Meta, Token, TraitItem};

use crate::attrs;
use crate::method::{Kind, Signature, signature};

struct Args {
    name: String,
    handle: Ident,
    provider: Option<Ident>,
    client: Option<Ident>,
}

fn ident_value(meta: &Meta) -> syn::Result<Ident> {
    let path = attrs::path_value(meta)?;
    path.get_ident()
        .cloned()
        .ok_or_else(|| syn::Error::new_spanned(path, "expected an identifier"))
}

fn parse_args(attr: proc_macro2::TokenStream) -> syn::Result<Args> {
    let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(attr.clone())?;
    let (mut name, mut handle, mut provider, mut client) = (None, None, None, None);
    for meta in &metas {
        let key = meta.path().get_ident().map(Ident::to_string);
        match key.as_deref() {
            Some("name") if name.is_none() => name = Some(attrs::lit_str_value(meta)?),
            Some("handle") if handle.is_none() => handle = Some(ident_value(meta)?),
            Some("provider") if provider.is_none() => provider = Some(ident_value(meta)?),
            Some("client") if client.is_none() => client = Some(ident_value(meta)?),
            _ => {
                return Err(syn::Error::new_spanned(
                    meta,
                    "expected unique `name = \"...\"`, `handle = Ident`, `provider = Ident` or `client = Ident`",
                ));
            }
        }
    }
    let name = name
        .filter(|name| !name.is_empty())
        .ok_or_else(|| syn::Error::new_spanned(&attr, "interfaces need `name = \"...\"`"))?;
    let handle =
        handle.ok_or_else(|| syn::Error::new_spanned(&attr, "interfaces need `handle = Ident`"))?;
    Ok(Args {
        name,
        handle,
        provider,
        client,
    })
}

/// One interface method.
struct Entry {
    kind: Kind,
    sig: Signature,
    docs: Vec<syn::Attribute>,
}

pub(crate) fn expand(
    attr: proc_macro2::TokenStream,
    mut item: ItemTrait,
) -> syn::Result<proc_macro2::TokenStream> {
    let args = parse_args(attr)?;
    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &item.generics,
            "interfaces cannot be generic",
        ));
    }
    if item.unsafety.is_some() || item.auto_token.is_some() || !item.supertraits.is_empty() {
        return Err(syn::Error::new_spanned(
            &item.ident,
            "interfaces are plain traits without supertraits",
        ));
    }
    let mut entries = Vec::new();
    for member in &mut item.items {
        let TraitItem::Fn(method) = member else {
            return Err(syn::Error::new_spanned(
                member,
                "interfaces contain only #[query], #[mutation] or #[derived] signatures",
            ));
        };
        if method.default.is_some() {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "interfaces declare signatures; providers implement them",
            ));
        }
        let mut kind = None;
        let mut kept = Vec::new();
        for attr in method.attrs.drain(..) {
            let found = if attr.path().is_ident("query") {
                Some(Kind::Query)
            } else if attr.path().is_ident("mutation") {
                Some(Kind::Mutation)
            } else if attr.path().is_ident("derived") {
                Some(Kind::Derived)
            } else {
                None
            };
            match found {
                Some(_) if !matches!(attr.meta, Meta::Path(_)) => {
                    return Err(syn::Error::new_spanned(attr, "takes no arguments"));
                }
                Some(_) if kind.is_some() => {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "each interface method has exactly one kind",
                    ));
                }
                Some(found) => kind = Some(found),
                None if attr.path().is_ident("doc") || attr.path().is_ident("allow") => {
                    kept.push(attr);
                }
                None => {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "unsupported interface method attribute",
                    ));
                }
            }
        }
        method.attrs = kept;
        let kind = kind.ok_or_else(|| {
            syn::Error::new_spanned(
                &method.sig,
                "interface methods need #[query], #[mutation] or #[derived]",
            )
        })?;
        entries.push(Entry {
            kind,
            sig: signature(kind, &method.sig)?,
            docs: method
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("doc"))
                .cloned()
                .collect(),
        });
    }
    if entries.is_empty() {
        return Err(syn::Error::new_spanned(
            &item.ident,
            "interfaces declare at least one method",
        ));
    }

    let vis = &item.vis;
    let trait_ident = &item.ident;
    let interface_name = &args.name;
    let handle = &args.handle;
    let provider = args
        .provider
        .clone()
        .unwrap_or_else(|| format_ident!("{}Provider", trait_ident));
    if let Some(client) = &args.client
        && (client == handle || client == &provider || client == trait_ident)
    {
        return Err(syn::Error::new_spanned(
            client,
            "native client, handle, provider and trait names must differ",
        ));
    }

    let mut support = Vec::new();
    let mut handle_methods = Vec::new();
    let mut imported_methods = Vec::new();
    let mut imported_derived = Vec::new();
    let mut provider_consts = Vec::new();
    let mut parts = Vec::new();
    let mut http_parts = Vec::new();
    let mut client_methods = Vec::new();
    let mut client_names = std::collections::BTreeSet::new();

    for Entry { kind, sig, docs } in &entries {
        let method_ident = &sig.name;
        let method_name = method_ident.to_string();
        let args_ident = format_ident!("__Flower{}_{}Args", trait_ident, method_ident);
        let public_name = method_name
            .strip_prefix("r#")
            .unwrap_or(&method_name)
            .split('_')
            .map(|word| {
                let mut chars = word.chars();
                chars
                    .next()
                    .map(|c| c.to_uppercase().collect::<String>() + chars.as_str())
                    .unwrap_or_default()
            })
            .collect::<String>();
        let public_args_ident = format_ident!("{}{}Args", trait_ident, public_name);
        let shim = format_ident!("__flower_{}_{}", trait_ident, method_ident);
        let const_ident = crate::attrs::screaming(method_ident, "")?;
        let try_ident = format_ident!("try_{}", method_ident);
        let arg_names: Vec<&Ident> = sig.args.iter().map(|(name, _)| name).collect();
        let arg_types: Vec<&syn::Type> = sig.args.iter().map(|(_, ty)| ty).collect();
        let output = &sig.output;
        let error = match &sig.error {
            Some(error) => quote!(#error),
            None => quote!(::flower_sdk::Failure),
        };
        let wrap_fn = kind.wrap_fn();
        let invoke = if sig.fallible {
            quote! {
                match <P as #trait_ident>::#method_ident(__ctx, #(__args.#arg_names,)*) {
                    ::core::result::Result::Ok(__output) => ::flower_sdk::FlowerType::encode(&__output),
                    ::core::result::Result::Err(__error) => ::core::result::Result::Err(
                        ::flower_sdk::BusinessError::into_failure(__error),
                    ),
                }
            }
        } else {
            quote! {
                ::flower_sdk::FlowerType::encode(
                    &<P as #trait_ident>::#method_ident(__ctx, #(__args.#arg_names,)*),
                )
            }
        };
        support.push(quote! {
            #[doc(hidden)]
            #[derive(::flower_sdk::FlowerType)]
            #[allow(non_camel_case_types)]
            #vis struct #args_ident {
                #( pub #arg_names: #arg_types, )*
            }

            /// Arguments shared by native clients and interface providers.
            #vis type #public_args_ident = #args_ident;

            #[doc(hidden)]
            #[allow(non_snake_case)]
            #vis fn #shim<'c, 'r, P: #trait_ident + 'static>(
                __ctx: &'c mut ::flower_sdk::Ctx<'c, 'r>,
                __value: ::flower_sdk::Value,
            ) -> ::flower_sdk::Result<::flower_sdk::Value> {
                let __ctx = #wrap_fn(__ctx);
                let __args = <#args_ident as ::flower_sdk::FlowerType>::decode(__value)?;
                #invoke
            }
        });
        let contract = quote! {
            ::flower_sdk::Contract {
                args: <#args_ident as ::flower_sdk::FlowerType>::SCHEMA,
                result: <#output as ::flower_sdk::FlowerType>::SCHEMA,
                errors: <#error as ::flower_sdk::BusinessError>::SCHEMA,
            }
        };
        let build_args = quote! {
            let __args = ::flower_sdk::FlowerType::encode(&#args_ident { #(#arg_names,)* })?;
        };
        match kind {
            Kind::Query | Kind::Mutation => {
                if args.client.is_some() {
                    let native_ident = if *kind == Kind::Query {
                        method_ident.clone()
                    } else {
                        format_ident!(
                            "prepare_{}",
                            method_ident.to_string().trim_start_matches("r#")
                        )
                    };
                    if method_name == "client" || !client_names.insert(native_ident.to_string()) {
                        return Err(syn::Error::new_spanned(
                            method_ident,
                            "generated native client method name conflicts with another method",
                        ));
                    }
                    let (operation, invoke) = if *kind == Kind::Query {
                        (quote!(Query), quote!(query))
                    } else {
                        (quote!(Prepare), quote!(prepare))
                    };
                    client_methods.push(quote! {
                        #(#docs)*
                        pub fn #native_ident<'a>(&'a self, args: &'a #public_args_ident)
                            -> M::#operation<'a, #public_args_ident, #output, #error>
                        {
                            ::flower_sdk::remote::RemoteModule::#invoke(&self.remote, #handle::#const_ident, args)
                        }
                    });
                    if *kind == Kind::Mutation {
                        if !client_names.insert(method_ident.to_string()) {
                            return Err(syn::Error::new_spanned(
                                method_ident,
                                "generated command method name conflicts with another method",
                            ));
                        }
                        client_methods.push(quote! {
                            #(#docs)*
                            pub fn #method_ident<'a>(&'a self, args: &'a #public_args_ident)
                                -> M::Command<'a, #public_args_ident, #output, #error>
                                where M: ::flower_sdk::remote::RemoteCommands
                            {
                                ::flower_sdk::remote::RemoteCommands::command(&self.remote, #handle::#const_ident, args)
                            }
                        });
                    }
                }
                let remote = if *kind == Kind::Query {
                    quote!(::flower_sdk::QueryOp)
                } else {
                    quote!(::flower_sdk::MutationOp)
                };
                handle_methods.push(quote! {
                    /// Contract-only descriptor for native clients, requiring no provider implementation.
                    pub const #const_ident: #remote<#args_ident, #output, #error> = #remote::new(#method_name);
                });
                let (method_kind, ctx_type, call, try_call) = if *kind == Kind::Query {
                    (
                        quote!(::flower_sdk::MethodKind::Query),
                        quote!(impl ::flower_sdk::ctx::MethodCtx),
                        quote!(::flower_sdk::ctx::MethodCtx::call(
                            ctx,
                            self.import.module,
                            #method_name,
                            &__args
                        )),
                        quote!(::flower_sdk::ctx::MethodCtx::try_call(
                            ctx,
                            self.import.module,
                            #method_name,
                            &__args
                        )),
                    )
                } else {
                    (
                        quote!(::flower_sdk::MethodKind::Mutation),
                        quote!(::flower_sdk::MutationCtx<'_, '_>),
                        quote!(ctx.call_mutation(self.import.module, #method_name, &__args)),
                        quote!(ctx.try_mutation(self.import.module, #method_name, &__args)),
                    )
                };
                imported_methods.push(quote! {
                    ::flower_sdk::ImportedMethod {
                        name: #method_name,
                        kind: #method_kind,
                        contract: ::core::option::Option::Some(#contract),
                    }
                });
                handle_methods.push(quote! {
                    /// Call the provider's method; a failure poisons the
                    /// invocation.
                    pub fn #method_ident(
                        &self,
                        ctx: &mut #ctx_type,
                        #( #arg_names: #arg_types, )*
                    ) -> ::flower_sdk::Result<#output> {
                        #build_args
                        <#output as ::flower_sdk::FlowerType>::decode(#call?)
                    }

                    /// Call through a savepoint: a business failure rolls
                    /// the callee back and comes back decoded.
                    pub fn #try_ident(
                        &self,
                        ctx: &mut #ctx_type,
                        #( #arg_names: #arg_types, )*
                    ) -> ::core::result::Result<
                        ::flower_sdk::CallOutcome<#output, #error>,
                        ::flower_sdk::CallError,
                    > {
                        let __args = ::flower_sdk::FlowerType::encode(
                            &#args_ident { #(#arg_names,)* },
                        )
                        .map_err(::flower_sdk::CallError::Host)?;
                        let __outcome = #try_call?;
                        ::flower_sdk::typed::decode_outcome(__outcome)
                            .map_err(::flower_sdk::CallError::Host)
                    }
                });
                let constructor = if *kind == Kind::Query {
                    quote!(::flower_sdk::Method::query)
                } else {
                    quote!(::flower_sdk::Method::mutation)
                };
                provider_consts.push(quote! {
                    /// The exported descriptor over `P`'s implementation.
                    pub const #const_ident: ::flower_sdk::Method = #constructor(
                        #method_name,
                        #shim::<P>,
                    )
                    .args(<#args_ident as ::flower_sdk::FlowerType>::SCHEMA)
                    .result(<#output as ::flower_sdk::FlowerType>::SCHEMA)
                    .errors(<#error as ::flower_sdk::BusinessError>::SCHEMA)
                    .export();
                });
                parts.push(quote!(&Self::#const_ident));
                http_parts.push(quote!(&Self::#const_ident.http()));
            }
            Kind::Derived => {
                imported_derived.push(quote! {
                    ::flower_sdk::ImportedDerived {
                        name: #method_name,
                        contract: ::core::option::Option::Some(#contract),
                    }
                });
                handle_methods.push(quote! {
                    /// Read the provider's derived value.
                    pub fn #method_ident(
                        &self,
                        ctx: &mut impl ::flower_sdk::ctx::ReadCtx,
                        #( #arg_names: #arg_types, )*
                    ) -> ::flower_sdk::Result<#output> {
                        #build_args
                        <#output as ::flower_sdk::FlowerType>::decode(
                            ::flower_sdk::ctx::ReadCtx::get_derived(
                                ctx,
                                self.import.module,
                                #method_name,
                                &__args,
                            )?,
                        )
                    }
                });
                provider_consts.push(quote! {
                    /// The exported descriptor over `P`'s implementation.
                    pub const #const_ident: ::flower_sdk::Derived = ::flower_sdk::Derived::new(
                        #method_name,
                        #shim::<P>,
                    )
                    .args(<#args_ident as ::flower_sdk::FlowerType>::SCHEMA)
                    .result(<#output as ::flower_sdk::FlowerType>::SCHEMA)
                    .errors(<#error as ::flower_sdk::BusinessError>::SCHEMA)
                    .export();
                });
                parts.push(quote!(&Self::#const_ident));
                http_parts.push(quote!(&Self::#const_ident));
            }
        }
    }
    let imported_method_count = imported_methods.len();
    let imported_derived_count = imported_derived.len();
    let native_client = args.client.as_ref().map(|client| {
        quote! {
            /// Native client generated from the interface contract; no provider implementation is needed.
            #[derive(Clone)]
            #vis struct #client<M> {
                /// The bound module backend, used to execute prepared commands and configure watches.
                pub remote: M,
            }
            impl<M: ::flower_sdk::remote::RemoteModule> #client<M> {
                #(#client_methods)*
            }
            impl #handle {
                pub fn client<M: ::flower_sdk::remote::RemoteModule>(remote: M) -> #client<M> {
                    #client { remote }
                }
            }
        }
    });

    Ok(quote! {
        #item

        #(#support)*
        #native_client

        /// Consumer handle: typed calls into a module providing this
        /// interface, by name or through a `$slot`, and the component
        /// declaring the interface's contracts against it.
        #[derive(Clone, Copy, Debug, PartialEq)]
        #vis struct #handle {
            import: ::flower_sdk::Import,
        }

        impl #handle {
            /// The interface's stable name.
            pub const INTERFACE: &'static str = #interface_name;
            const IMPORTED_METHODS: [::flower_sdk::ImportedMethod; #imported_method_count] =
                [#(#imported_methods),*];
            const IMPORTED_DERIVED: [::flower_sdk::ImportedDerived; #imported_derived_count] =
                [#(#imported_derived),*];

            /// Import the interface from a module name or a `$slot`:
            /// list the handle among the app's components.
            pub const fn import(module: &'static str) -> Self {
                #handle {
                    import: ::flower_sdk::Import {
                        module,
                        collections: &[],
                        derived: &Self::IMPORTED_DERIVED,
                        methods: &Self::IMPORTED_METHODS,
                    },
                }
            }

            /// The addressed module or slot.
            pub const fn module(&self) -> &'static str {
                self.import.module
            }

            #(#handle_methods)*
        }

        impl ::flower_sdk::Component for #handle {
            fn imports(&self) -> ::core::option::Option<&::flower_sdk::Import> {
                ::core::option::Option::Some(&self.import)
            }
        }

        /// Provider descriptors over an implementation `P` of the
        /// interface: list `COMPONENT` among the app's components.
        #vis struct #provider<P> {
            http: bool,
            _implementation: ::core::marker::PhantomData<fn() -> P>,
        }

        impl<P: #trait_ident + 'static> #provider<P> {
            #(#provider_consts)*

            /// The whole interface, exported, as one component. Its
            /// methods serve other modules only: none answers over HTTP.
            pub const COMPONENT: Self = #provider {
                http: false,
                _implementation: ::core::marker::PhantomData,
            };

            /// The same interface, its methods also answering over HTTP
            /// under their names.
            pub const fn http(self) -> Self {
                #provider {
                    http: true,
                    _implementation: ::core::marker::PhantomData,
                }
            }

            const PARTS: &'static [&'static dyn ::flower_sdk::Component] = &[#(#parts),*];
            const HTTP_PARTS: &'static [&'static dyn ::flower_sdk::Component] =
                &[#(#http_parts),*];
        }

        impl<P: #trait_ident + 'static> ::flower_sdk::Component for #provider<P> {
            fn components(
                &'static self,
                visit: &mut dyn FnMut(&'static dyn ::flower_sdk::Component),
            ) {
                let parts = if self.http {
                    Self::HTTP_PARTS
                } else {
                    Self::PARTS
                };
                for part in parts {
                    visit(*part);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand_tokens(
        attr: proc_macro2::TokenStream,
        item: proc_macro2::TokenStream,
    ) -> syn::Result<String> {
        let item: ItemTrait = syn::parse2(item)?;
        expand(attr, item).map(|tokens| tokens.to_string())
    }

    #[test]
    fn interfaces_expand_handles_and_providers() {
        let out = expand_tokens(
            quote!(name = "tasks.v1", handle = TasksApi),
            quote! {
                pub trait Tasks {
                    #[query]
                    fn get_task(ctx: &mut QueryCtx<'_, '_>, id: String) -> Option<Task>;
                    #[mutation]
                    fn add_task(ctx: &mut MutationCtx<'_, '_>, title: String) -> Result<String, TaskError>;
                    #[derived]
                    fn open_count(ctx: &mut DerivedCtx<'_, '_>) -> u64;
                }
            },
        )
        .unwrap();
        assert!(out.contains("struct TasksApi"));
        assert!(out.contains("struct TasksProvider"));
        assert!(out.contains("fn try_add_task"));
        assert!(out.contains("const GET_TASK"));
        assert!(out.contains("const OPEN_COUNT"));
        assert!(out.contains("const COMPONENT"));
        assert!(out.contains("fn import"));
        assert!(out.contains("< TaskError as :: flower_sdk :: BusinessError > :: SCHEMA"));
        assert!(!out.contains("# [query]"));
    }

    #[test]
    fn interfaces_reject_bad_shapes() {
        let reject = |attr, item| expand_tokens(attr, item).unwrap_err().to_string();
        assert!(
            reject(
                quote!(handle = Api),
                quote!(
                    trait T {
                        #[query]
                        fn a(ctx: &mut QueryCtx<'_, '_>);
                    }
                )
            )
            .contains("name")
        );
        assert!(
            reject(
                quote!(name = "t"),
                quote!(
                    trait T {
                        #[query]
                        fn a(ctx: &mut QueryCtx<'_, '_>);
                    }
                )
            )
            .contains("handle")
        );
        assert!(
            reject(
                quote!(name = "t", handle = Api),
                quote!(
                    trait T {
                        fn a(ctx: &mut QueryCtx<'_, '_>);
                    }
                )
            )
            .contains("need #[query]")
        );
        assert!(
            reject(
                quote!(name = "t", handle = Api),
                quote!(
                    trait T {
                        #[query]
                        fn a(ctx: &mut QueryCtx<'_, '_>) {}
                    }
                )
            )
            .contains("providers implement")
        );
        assert!(
            reject(
                quote!(name = "t", handle = Api),
                quote!(
                    trait T {
                        #[query]
                        #[mutation]
                        fn a(ctx: &mut QueryCtx<'_, '_>);
                    }
                )
            )
            .contains("exactly one kind")
        );
        assert!(
            reject(
                quote!(name = "t", handle = Api),
                quote!(
                    trait T {}
                )
            )
            .contains("at least one method")
        );
        assert!(
            reject(
                quote!(name = "t", handle = Api),
                quote!(
                    trait T<X> {
                        #[query]
                        fn a(ctx: &mut QueryCtx<'_, '_>);
                    }
                )
            )
            .contains("generic")
        );
    }

    #[test]
    fn native_clients_report_name_collisions_at_the_contract() {
        for (attr, item, expected) in [
            (
                quote!(name = "t", handle = Api, client = Api),
                quote! { trait T { #[query] fn read(ctx:&mut QueryCtx<'_, '_>)->u64; } },
                "names must differ",
            ),
            (
                quote!(name = "t", handle = Api, client = Client),
                quote! { trait T { #[query] fn prepare_put(ctx:&mut QueryCtx<'_, '_>)->u64; #[mutation] fn put(ctx:&mut MutationCtx<'_, '_>)->u64; } },
                "method name conflicts",
            ),
            (
                quote!(name = "t", handle = Api, client = Client),
                quote! { trait T { #[query] fn client(ctx:&mut QueryCtx<'_, '_>)->u64; } },
                "method name conflicts",
            ),
        ] {
            assert!(
                expand_tokens(attr, item)
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }
}
