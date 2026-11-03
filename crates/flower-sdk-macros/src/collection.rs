//! `#[collection(...)]` expansion.
//!
//! ```ignore
//! #[collection(
//!     name = "tasks",
//!     key = TaskId,
//!     value = Task,
//!     index(name = "by_owner", fields(owner)),
//!     reference(target = Posts, fields(post), on_delete = cascade),
//! )]
//! pub struct Tasks;
//! ```
//!
//! The marker struct is kept and gains an inherent handle (`NAME`, `raw()`,
//! typed `get`/`set`/`delete` by key) and implements `Component`: apps
//! list the marker itself (`App::new(&[&Tasks, ...])`). `export` lets
//! other modules read the collection, and `access = policy` (a
//! `fn() -> access::Policy`) guards callers. A hidden `TASKS_COLLECTION` static
//! carries the raw descriptor; reference `target`s name marker structs,
//! which the macro resolves to that static by replacing the last path
//! segment, so renamed imports do not resolve. Markers cannot be generic.

use quote::{ToTokens, quote};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Meta, Token};

use crate::attrs;

/// Parsed `#[collection(...)]` arguments.
#[derive(Debug)]
pub(crate) struct AttributeArgs {
    name: (String, proc_macro2::Span),
    key: syn::Type,
    value: syn::Type,
    indexes: Vec<Index>,
    references: Vec<Reference>,
    export: bool,
    /// `access = path`: a `fn() -> Policy` guarding callers.
    access: Option<syn::Path>,
}

#[derive(Debug)]
struct Index {
    name: String,
    fields: Vec<String>,
    /// Field types for the typed handle, in field order.
    types: Option<Vec<syn::Type>>,
}

#[derive(Debug)]
struct Reference {
    targets: Targets,
    place: Place,
    on_delete: OnDelete,
}

#[derive(Debug)]
enum Targets {
    One(syn::Path),
    Poly {
        targets: Vec<syn::Path>,
        discriminator: String,
    },
}

#[derive(Debug)]
enum Place {
    Fields(Vec<String>),
    Key,
    KeyPrefix,
}

#[derive(Clone, Copy, Debug)]
enum OnDelete {
    Restrict,
    Cascade,
    SetNull,
}

impl syn::parse::Parse for AttributeArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let items = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut name = None;
        let mut key = None;
        let mut value = None;
        let mut indexes = Vec::new();
        let mut references = Vec::new();
        let mut export = false;
        let mut access = None;
        for meta in items {
            if let Meta::Path(path) = &meta
                && path.is_ident("export")
            {
                if export {
                    return Err(syn::Error::new_spanned(&meta, "duplicate `export`"));
                }
                export = true;
                continue;
            }
            let key_path = attrs::key_path(&meta).and_then(|path| path.get_ident());
            match key_path.map(|ident| ident.to_string()).as_deref() {
                Some("name") => {
                    if name.is_some() {
                        return Err(syn::Error::new_spanned(&meta, "duplicate `name`"));
                    }
                    let span = meta_span(&meta);
                    name = Some((attrs::lit_str_value(&meta)?, span));
                }
                Some("key") => {
                    if key.is_some() {
                        return Err(syn::Error::new_spanned(&meta, "duplicate `key`"));
                    }
                    key = Some(type_value(&meta)?);
                }
                Some("value") => {
                    if value.is_some() {
                        return Err(syn::Error::new_spanned(&meta, "duplicate `value`"));
                    }
                    value = Some(type_value(&meta)?);
                }
                Some("access") => {
                    if access.is_some() {
                        return Err(syn::Error::new_spanned(&meta, "duplicate `access`"));
                    }
                    access = Some(attrs::path_value(&meta)?);
                }
                Some("index") => indexes.push(parse_index(&meta)?),
                Some("reference") => references.push(parse_reference(&meta)?),
                _ => {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "unknown collection option (expected name, key, value, index, reference, \
                         export, access)",
                    ));
                }
            }
        }
        let Some(name) = name else {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "collections need `name = \"...\"`",
            ));
        };
        let Some(key) = key else {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "collections need `key = KeyType`",
            ));
        };
        let Some(value) = value else {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "collections need `value = ValueType`",
            ));
        };
        Ok(AttributeArgs {
            name,
            key,
            value,
            indexes,
            references,
            export,
            access,
        })
    }
}

fn meta_span(meta: &Meta) -> proc_macro2::Span {
    match meta {
        Meta::NameValue(value) => value.value.span(),
        Meta::List(list) => list.path.span(),
        Meta::Path(path) => path.span(),
    }
}

/// Read a `key = Type` value by re-parsing the expression tokens as a type.
fn type_value(meta: &Meta) -> syn::Result<syn::Type> {
    match meta {
        Meta::NameValue(value) => syn::parse2::<syn::Type>(value.value.to_token_stream())
            .map_err(|_| syn::Error::new_spanned(&value.value, "expected a type")),
        _ => Err(syn::Error::new_spanned(meta, "expected `key = Type`")),
    }
}

/// Read an identifier list `fields(a, b)` from a `Meta::List`.
fn ident_list(meta: &Meta) -> syn::Result<Vec<String>> {
    let mut names = Vec::new();
    for inner in attrs::nested_metas(meta)? {
        match inner {
            Meta::Path(path) => match path.get_ident() {
                Some(ident) => names.push(ident.to_string()),
                None => {
                    return Err(syn::Error::new_spanned(&path, "expected a field name"));
                }
            },
            _ => {
                return Err(syn::Error::new_spanned(&inner, "expected a field name"));
            }
        }
    }
    Ok(names)
}

fn parse_index(meta: &Meta) -> syn::Result<Index> {
    let mut name = None;
    let mut fields = None;
    let mut types = None;
    for inner in attrs::nested_metas(meta)? {
        let key = attrs::key_path(&inner).and_then(|path| path.get_ident());
        match key.map(|ident| ident.to_string()).as_deref() {
            Some("name") => {
                if name.is_some() {
                    return Err(syn::Error::new_spanned(&inner, "duplicate index `name`"));
                }
                name = Some(attrs::lit_str_value(&inner)?);
            }
            Some("types") => {
                let Meta::List(list) = &inner else {
                    return Err(syn::Error::new_spanned(&inner, "expected `types(...)`"));
                };
                if types.is_some() {
                    return Err(syn::Error::new_spanned(&inner, "duplicate index `types`"));
                }
                let parsed =
                    list.parse_args_with(Punctuated::<syn::Type, Token![,]>::parse_terminated)?;
                types = Some(parsed.into_iter().collect::<Vec<_>>());
            }
            Some("fields") => {
                if fields.is_some() {
                    return Err(syn::Error::new_spanned(&inner, "duplicate index `fields`"));
                }
                let names = ident_list(&inner)?;
                if names.is_empty() {
                    return Err(syn::Error::new_spanned(&inner, "indexes need fields"));
                }
                fields = Some(names);
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &inner,
                    "unknown index option (expected name, fields, types)",
                ));
            }
        }
    }
    let Some(name) = name else {
        return Err(syn::Error::new_spanned(
            meta,
            "indexes need `name = \"...\"`",
        ));
    };
    let Some(fields) = fields else {
        return Err(syn::Error::new_spanned(meta, "indexes need `fields(...)`"));
    };
    if let Some(types) = &types
        && types.len() != fields.len()
    {
        return Err(syn::Error::new_spanned(
            meta,
            "index `types(...)` needs one type per field",
        ));
    }
    Ok(Index {
        name,
        fields,
        types,
    })
}

fn parse_reference(meta: &Meta) -> syn::Result<Reference> {
    let mut target = None;
    let mut targets = None;
    let mut discriminator = None;
    let mut place = None;
    let mut on_delete = OnDelete::Restrict;
    for inner in attrs::nested_metas(meta)? {
        if let Some(flag) = attrs::flag_ident(&inner) {
            match flag.as_str() {
                "key" | "key_prefix" => {
                    if place.is_some() {
                        return Err(syn::Error::new_spanned(
                            &inner,
                            "references take one placement (fields, key or key_prefix)",
                        ));
                    }
                    place = Some(if flag == "key" {
                        Place::Key
                    } else {
                        Place::KeyPrefix
                    });
                    continue;
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        &inner,
                        "unknown reference flag (expected key, key_prefix)",
                    ));
                }
            }
        }
        let key = attrs::key_path(&inner).and_then(|path| path.get_ident());
        match key.map(|ident| ident.to_string()).as_deref() {
            Some("target") => {
                if target.is_some() || targets.is_some() {
                    return Err(syn::Error::new_spanned(
                        &inner,
                        "duplicate reference target",
                    ));
                }
                target = Some(target_path(&inner)?);
            }
            Some("targets") => {
                if target.is_some() || targets.is_some() {
                    return Err(syn::Error::new_spanned(
                        &inner,
                        "duplicate reference target",
                    ));
                }
                let nested = attrs::nested_metas(&inner)?;
                if nested.is_empty() {
                    return Err(syn::Error::new_spanned(
                        &inner,
                        "polymorphic references need at least one target",
                    ));
                }
                let mut paths = Vec::new();
                for item in nested {
                    match item {
                        Meta::Path(path) => {
                            reject_target_args(&path)?;
                            paths.push(path);
                        }
                        _ => {
                            return Err(syn::Error::new_spanned(
                                &item,
                                "expected a target collection marker",
                            ));
                        }
                    }
                }
                targets = Some(paths);
            }
            Some("discriminator") => {
                if discriminator.is_some() {
                    return Err(syn::Error::new_spanned(&inner, "duplicate `discriminator`"));
                }
                discriminator = Some(attrs::lit_str_value(&inner)?);
            }
            Some("fields") => {
                if place.is_some() {
                    return Err(syn::Error::new_spanned(
                        &inner,
                        "references take one placement (fields, key or key_prefix)",
                    ));
                }
                let names = ident_list(&inner)?;
                if names.is_empty() {
                    return Err(syn::Error::new_spanned(&inner, "references need fields"));
                }
                place = Some(Place::Fields(names));
            }
            Some("on_delete") => {
                on_delete = parse_on_delete(&inner)?;
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &inner,
                    "unknown reference option (expected target, targets, discriminator, \
                     fields, key, key_prefix, on_delete)",
                ));
            }
        }
    }
    let targets = match (target, targets, discriminator) {
        (Some(one), None, None) => Targets::One(one),
        (Some(_), _, _) => {
            return Err(syn::Error::new_spanned(
                meta,
                "single-target references take no `targets` or `discriminator`",
            ));
        }
        (None, Some(many), Some(discriminator)) => Targets::Poly {
            targets: many,
            discriminator,
        },
        (None, Some(_), None) => {
            return Err(syn::Error::new_spanned(
                meta,
                "polymorphic references need `discriminator = \"...\"`",
            ));
        }
        (None, None, _) => {
            return Err(syn::Error::new_spanned(
                meta,
                "references need `target = ...` or `targets(...)`",
            ));
        }
    };
    let Some(place) = place else {
        return Err(syn::Error::new_spanned(
            meta,
            "references need a placement (fields, key or key_prefix)",
        ));
    };
    if matches!(targets, Targets::Poly { .. }) && !matches!(place, Place::Fields(_)) {
        return Err(syn::Error::new_spanned(
            meta,
            "only field references can be polymorphic",
        ));
    }
    Ok(Reference {
        targets,
        place,
        on_delete,
    })
}

fn target_path(meta: &Meta) -> syn::Result<syn::Path> {
    let path = attrs::path_value(meta)?;
    reject_target_args(&path)?;
    Ok(path)
}

fn reject_target_args(path: &syn::Path) -> syn::Result<()> {
    if path
        .segments
        .last()
        .is_some_and(|segment| !segment.arguments.is_empty())
    {
        return Err(syn::Error::new_spanned(
            path,
            "reference targets take no generic arguments",
        ));
    }
    Ok(())
}

fn parse_on_delete(meta: &Meta) -> syn::Result<OnDelete> {
    let path = attrs::path_value(meta)?;
    let name = path
        .get_ident()
        .map(|ident| ident.to_string())
        .unwrap_or_default();
    match name.as_str() {
        "restrict" => Ok(OnDelete::Restrict),
        "cascade" => Ok(OnDelete::Cascade),
        "set_null" => Ok(OnDelete::SetNull),
        _ => Err(syn::Error::new_spanned(
            &path,
            "unknown on_delete (expected restrict, cascade or set_null)",
        )),
    }
}

/// Resolve a marker path to its sibling `*_COLLECTION` static path.
fn collection_static(marker: &syn::Path) -> syn::Result<syn::Path> {
    let mut static_path = marker.clone();
    if let Some(last) = static_path.segments.last_mut() {
        last.ident = attrs::screaming(&last.ident, "_COLLECTION")?;
    }
    Ok(static_path)
}

/// Refuse a field reference no index covers, as deploy would: a
/// single-target one needs an index leading with its fields, a
/// polymorphic one an index leading with its discriminator, then them.
fn check_covering_indexes(args: &AttributeArgs) -> syn::Result<()> {
    for reference in &args.references {
        let Place::Fields(fields) = &reference.place else {
            continue;
        };
        let (leading, shape): (Vec<&str>, String) = match &reference.targets {
            Targets::One(_) => (
                fields.iter().map(String::as_str).collect(),
                format!("({})", fields.join(", ")),
            ),
            Targets::Poly { discriminator, .. } => (
                std::iter::once(discriminator.as_str())
                    .chain(fields.iter().map(String::as_str))
                    .collect(),
                format!("({discriminator}, {})", fields.join(", ")),
            ),
        };
        let covered = args.indexes.iter().any(|index| {
            index.fields.len() >= leading.len()
                && index.fields[..leading.len()]
                    .iter()
                    .zip(&leading)
                    .all(|(field, wanted)| field == wanted)
        });
        if !covered {
            return Err(syn::Error::new(
                args.name.1,
                format!(
                    "a field reference needs a covering index: add \
                     `index(name = \"...\", fields{shape})`, leading with those fields"
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn expand(
    args: AttributeArgs,
    item: syn::ItemStruct,
) -> syn::Result<proc_macro2::TokenStream> {
    if !item.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &item.generics,
            "collection markers cannot be generic",
        ));
    }
    check_covering_indexes(&args)?;
    let (name, name_span) = args.name;
    let marker = &item.ident;
    let vis = &item.vis;
    let key_ty = &args.key;
    let value_ty = &args.value;
    let static_name = attrs::screaming(marker, "_COLLECTION")?;
    let named = syn::LitStr::new(&name, name_span);
    let export = if args.export {
        quote!(.export())
    } else {
        quote!()
    };
    let access = match &args.access {
        Some(policy) => quote!(.access(#policy)),
        None => quote!(),
    };

    let mut index_defs = Vec::new();
    let mut index_handles = Vec::new();
    for index in &args.indexes {
        let index_name = &index.name;
        let fields = &index.fields;
        index_defs.push(quote! {
            ::flower_sdk::IndexDef::new(#index_name, &[#(#fields),*])
        });
        // Typed handles for identifier-safe names; untyped fields compare
        // as plain values.
        if index_name.is_empty()
            || index_name.starts_with(|c: char| c.is_ascii_digit())
            || !index_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            continue;
        }
        let handle = quote::format_ident!("{}", index_name.to_ascii_uppercase());
        let types: Vec<proc_macro2::TokenStream> = match &index.types {
            Some(types) => types.iter().map(|ty| quote!(#ty)).collect(),
            None => fields.iter().map(|_| quote!(::flower_sdk::Value)).collect(),
        };
        if types.len() > 4 {
            continue;
        }
        index_handles.push(quote! {
            /// Typed handle on the index of the same name.
            pub const #handle: ::flower_sdk::index::Index<#key_ty, #value_ty, (#(#types,)*)> =
                ::flower_sdk::index::Index::new(#named, #index_name);
        });
    }

    let mut reference_defs = Vec::new();
    for reference in &args.references {
        let on_delete = match reference.on_delete {
            OnDelete::Restrict => quote!(::flower_sdk::OnDelete::Restrict),
            OnDelete::Cascade => quote!(::flower_sdk::OnDelete::Cascade),
            OnDelete::SetNull => quote!(::flower_sdk::OnDelete::SetNull),
        };
        let place = match &reference.place {
            Place::Fields(fields) => quote!(::flower_sdk::Place::Fields(&[#(#fields),*])),
            Place::Key => quote!(::flower_sdk::Place::Key),
            Place::KeyPrefix => quote!(::flower_sdk::Place::KeyPrefix),
        };
        match &reference.targets {
            Targets::One(target) => {
                let target_static = collection_static(target)?;
                reference_defs.push(quote! {
                    ::flower_sdk::Reference::new(&#target_static, #place).on_delete(#on_delete)
                });
            }
            Targets::Poly {
                targets,
                discriminator,
            } => {
                let statics: Vec<syn::Path> = targets
                    .iter()
                    .map(collection_static)
                    .collect::<syn::Result<_>>()?;
                reference_defs.push(quote! {
                    ::flower_sdk::Reference::poly(
                        &[#(&#statics),*],
                        #discriminator,
                        #place,
                    )
                    .on_delete(#on_delete)
                });
            }
        }
    }

    Ok(quote! {
        #item

        impl #marker {
            /// Collection name.
            pub const NAME: &'static str = #named;

            /// Typed read handle on the whole collection; `in_module`
            /// addresses another module's exported collection of this
            /// shape.
            pub const SOURCE: ::flower_sdk::index::Source<#key_ty, #value_ty> =
                ::flower_sdk::index::Source::new(#named);

            #(#index_handles)*

            /// Raw descriptor, usable in statics:
            /// `Materialize::Each(Orders::raw())`.
            pub const fn raw() -> &'static ::flower_sdk::Collection {
                &#static_name
            }

            /// One page of at most `limit` rows in host order after
            /// `after`: a complete inventory when walked to the end.
            pub fn page(
                __ctx: &mut impl ::flower_sdk::ReadCtx,
                __limit: u32,
                __after: ::core::option::Option<&str>,
            ) -> ::flower_sdk::Result<::flower_sdk::typed::Page<#key_ty, #value_ty>> {
                Self::SOURCE.page(__ctx, __limit, __after)
            }

            /// Read one row by key.
            pub fn get(
                __ctx: &mut impl ::flower_sdk::ReadCtx,
                __key: &#key_ty,
            ) -> ::flower_sdk::Result<::core::option::Option<#value_ty>> {
                Self::SOURCE.get(__ctx, __key)
            }

            /// Upsert one row under its key.
            pub fn set(
                __ctx: &mut ::flower_sdk::MutationCtx<'_, '_>,
                __key: &#key_ty,
                __value: &#value_ty,
            ) -> ::flower_sdk::Result<()> {
                let __encoded = ::flower_sdk::FlowerType::encode(__value)?;
                __ctx.set(
                    ::flower_sdk::CollectionRef::local(Self::NAME),
                    &::flower_sdk::FlowerType::encode(__key)?,
                    __encoded,
                )
            }

            /// Delete one row by key.
            pub fn delete(
                __ctx: &mut ::flower_sdk::MutationCtx<'_, '_>,
                __key: &#key_ty,
            ) -> ::flower_sdk::Result<()> {
                __ctx.delete(
                    ::flower_sdk::CollectionRef::local(Self::NAME),
                    &::flower_sdk::FlowerType::encode(__key)?,
                )
            }
        }

        impl ::flower_sdk::Component for #marker {
            fn collection(&self) -> ::core::option::Option<&::flower_sdk::Collection> {
                ::core::option::Option::Some(&#static_name)
            }
        }

        impl ::flower_sdk::typed::TypedCollection for #marker {
            type Key = #key_ty;
            type Value = #value_ty;
            const NAME: &'static str = #named;
        }

        /// Raw descriptor static behind the marker.
        #[doc(hidden)]
        #vis static #static_name: ::flower_sdk::Collection =
            ::flower_sdk::Collection::new(#named)
                .key_schema(<#key_ty as ::flower_sdk::FlowerType>::SCHEMA)
                .value_schema(<#value_ty as ::flower_sdk::FlowerType>::SCHEMA)
                .indexes(&[#(#index_defs),*])
                .references(&[#(#reference_defs),*])
                #access
                #export;
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand_args(
        attr: proc_macro2::TokenStream,
        item: proc_macro2::TokenStream,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let args: AttributeArgs = syn::parse2(attr).unwrap();
        let parsed: syn::ItemStruct = syn::parse2(item).unwrap();
        expand(args, parsed)
    }

    #[test]
    fn full_collection_expands() {
        let out = expand_args(
            quote! {
                name = "tasks",
                key = TaskId,
                value = Task,
                index(name = "by_owner", fields(owner)),
                index(name = "by_post", fields(post, owner)),
                index(name = "by_target", fields(kind, target)),
                reference(target = Posts, fields(post), on_delete = cascade),
                reference(
                    targets(Posts, Photos),
                    discriminator = "kind",
                    fields(target),
                ),
                access = owned,
                export,
            },
            quote!(
                pub struct Tasks;
            ),
        )
        .unwrap()
        .to_string();
        assert!(out.contains(". access (owned)"));
        assert!(out.contains(". export ()"));
        assert!(out.contains("static TASKS_COLLECTION"));
        assert!(out.contains("IndexDef :: new"));
        assert!(out.contains("\"by_owner\""));
        assert!(out.contains("Reference :: new"));
        assert!(out.contains("& POSTS_COLLECTION"));
        assert!(out.contains("Reference :: poly"));
        assert!(out.contains("\"kind\""));
        assert!(out.contains("OnDelete :: Cascade"));
        assert!(out.contains("SOURCE . get"));
        assert!(out.contains("impl :: flower_sdk :: Component for Tasks"));
        assert!(out.contains("CollectionRef :: local"));
    }

    #[test]
    fn field_references_need_a_covering_index() {
        // Deploy refuses a field reference no index leads with; the
        // attribute has every index, so it says so at compile time.
        let refused = |attr: proc_macro2::TokenStream| {
            expand_args(
                attr,
                quote!(
                    pub struct Links;
                ),
            )
            .unwrap_err()
            .to_string()
        };
        let message = refused(quote! {
            name = "links", key = String, value = Link,
            reference(target = Tasks, fields(task), on_delete = cascade),
        });
        assert!(message.contains("covering index"), "{message}");
        // An index over other fields first does not cover it.
        assert!(
            refused(quote! {
                name = "links", key = String, value = Link,
                index(name = "by_owner", fields(owner, task)),
                reference(target = Tasks, fields(task)),
            })
            .contains("covering index")
        );
        // A polymorphic reference needs its discriminator first.
        assert!(
            refused(quote! {
                name = "links", key = String, value = Link,
                index(name = "by_target", fields(target, kind)),
                reference(targets(Posts, Photos), discriminator = "kind", fields(target)),
            })
            .contains("covering index")
        );
        // Covered: the index leads with the fields (more may follow).
        assert!(
            expand_args(
                quote! {
                    name = "links", key = String, value = Link,
                    index(name = "by_task", fields(task, owner)),
                    reference(target = Tasks, fields(task)),
                },
                quote!(
                    pub struct Links;
                ),
            )
            .is_ok()
        );
    }

    #[test]
    fn missing_name_is_rejected() {
        let err = syn::parse2::<AttributeArgs>(quote!(key = TaskId, value = Task)).unwrap_err();
        assert!(err.to_string().contains("need `name"));
    }

    #[test]
    fn duplicate_keys_are_rejected() {
        let err = syn::parse2::<AttributeArgs>(quote!(
            name = "tasks",
            key = TaskId,
            key = Other,
            value = Task,
        ))
        .unwrap_err();
        assert!(err.to_string().contains("duplicate `key`"));
    }

    #[test]
    fn poly_without_discriminator_is_rejected() {
        let err = syn::parse2::<AttributeArgs>(quote!(
            name = "tasks",
            key = TaskId,
            value = Task,
            reference(targets(Posts), fields(post)),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("discriminator"));
    }

    #[test]
    fn poly_on_key_is_rejected() {
        let err = syn::parse2::<AttributeArgs>(quote!(
            name = "tasks",
            key = TaskId,
            value = Task,
            reference(targets(Posts), discriminator = "kind", key),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("only field references"));
    }

    #[test]
    fn empty_index_fields_are_rejected() {
        let err = syn::parse2::<AttributeArgs>(quote!(
            name = "tasks",
            key = TaskId,
            value = Task,
            index(name = "by_owner", fields()),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("need fields"));
    }

    #[test]
    fn generic_markers_are_rejected() {
        let err = expand_args(
            quote!(name = "tasks", key = TaskId, value = Task),
            quote!(
                pub struct Tasks<T>;
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot be generic"));
    }
}
