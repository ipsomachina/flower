//! `#[derive(FlowerType)]` expansion.
//!
//! Structs map to closed `Schema::Object` contracts: every named field
//! decodes through `decode_field`, so member types with `OPTIONAL_FIELD`
//! (notably `typed::Field`) tolerate absence. The schema marks exactly those
//! member types optional, plus every `#[flower(default)]` field, so
//! dispatch-time schema checks and decoding agree. Unknown members fail
//! decoding, mirroring closed-object parsing. Unit enums map to string
//! enums; single-payload enums map to `{"tag", "value"}` objects with a
//! closed tag set.
//!
//! Wire names are the Rust field names, or their `camelCase` forms under
//! the container option `#[flower(rename_all = "camelCase")]` (for APIs
//! whose clients speak JavaScript conventions); single fields are never
//! renamed.

use crate::attrs;
use quote::quote;
use syn::{Data, DeriveInput, Fields, GenericParam, Generics, Type};

/// Field-level decoding overrides.
#[derive(Default)]
struct FieldOpts {
    /// `#[flower(skip)]`: excluded from schema and encoding, filled with
    /// `Default` on decode.
    skip: bool,
    /// `#[flower(default)]` / `#[flower(default = path)]`: absent members
    /// decode to the fallback instead of failing.
    default: Option<Option<syn::Path>>,
}

fn field_opts(field: &syn::Field) -> syn::Result<FieldOpts> {
    let mut opts = FieldOpts::default();
    for meta in attrs::flower_metas(&field.attrs)? {
        if let Some(flag) = attrs::flag_ident(&meta) {
            match flag.as_str() {
                "skip" => opts.skip = true,
                "default" => opts.default = Some(None),
                "flatten" => {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "flatten is not supported; nest the struct as a named field instead",
                    ));
                }
                "redact" => {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "redact is not supported; use flower_sdk::typed::Field<T> as the field type",
                    ));
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        format!("unknown flower field option `{flag}`"),
                    ));
                }
            }
            continue;
        }
        let key = attrs::key_path(&meta).and_then(|path| path.get_ident());
        match key.map(|ident| ident.to_string()).as_deref() {
            Some("rename") => {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "rename is not supported: schema names must match Rust field names",
                ));
            }
            Some("default") => opts.default = Some(Some(attrs::path_value(&meta)?)),
            Some("flatten") => {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "flatten is not supported; nest the struct as a named field instead",
                ));
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "unknown flower field option (expected skip, default)",
                ));
            }
        }
    }
    if opts.skip && opts.default.is_some() {
        return Err(syn::Error::new_spanned(
            field,
            "a field cannot be both skipped and defaulted",
        ));
    }
    Ok(opts)
}

/// Reject `#[flower(...)]` on items that take no flower options.
fn reject_container_attrs(what: &str, attrs: &[syn::Attribute]) -> syn::Result<()> {
    if attrs::flower_metas(attrs)?.is_empty() {
        return Ok(());
    }
    Err(syn::Error::new_spanned(
        &attrs[0],
        format!("flower attributes go on fields, not on {what}"),
    ))
}

/// Add a `FlowerType` bound to every type parameter, plus `Default` for
/// parameters named in `defaulted`.
pub(crate) fn bound_generics(generics: &Generics, defaulted: &[syn::Ident]) -> Generics {
    let mut bound = generics.clone();
    for param in bound.params.iter_mut() {
        if let GenericParam::Type(ty) = param {
            ty.bounds.push(syn::parse_quote!(::flower_sdk::FlowerType));
            if defaulted.contains(&ty.ident) {
                ty.bounds.push(syn::parse_quote!(::core::default::Default));
            }
        }
    }
    bound
}

/// The bare type parameter of `field`, if the field type is exactly one.
fn bare_param(ty: &Type) -> Option<&syn::Ident> {
    match ty {
        Type::Path(path) if path.qself.is_none() && path.path.segments.len() == 1 => {
            let segment = path.path.segments.first().unwrap();
            if segment.arguments.is_empty() {
                Some(&segment.ident)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Whether a struct asks for `camelCase` wire names.
fn camel_case_container(input: &DeriveInput) -> syn::Result<bool> {
    let mut camel = false;
    for meta in attrs::flower_metas(&input.attrs)? {
        let key = attrs::key_path(&meta).and_then(|path| path.get_ident());
        match key.map(ToString::to_string).as_deref() {
            Some("rename_all") if !camel => {
                if attrs::lit_str_value(&meta)? != "camelCase" {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "rename_all supports only \"camelCase\"",
                    ));
                }
                camel = true;
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "unknown or repeated flower type option (expected rename_all = \"camelCase\")",
                ));
            }
        }
    }
    Ok(camel)
}

/// `snake_case` to `camelCase`: `lease_ms` becomes `leaseMs`.
pub(crate) fn camel_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = false;
    for ch in name.trim_start_matches("r#").chars() {
        if ch == '_' && !out.is_empty() {
            upper = true;
        } else if upper {
            out.extend(ch.to_uppercase());
            upper = false;
        } else {
            out.push(ch);
        }
    }
    out
}

pub(crate) fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    match &input.data {
        Data::Struct(data) => {
            let camel = camel_case_container(&input)?;
            expand_struct(&input, data, camel)
        }
        Data::Enum(data) => {
            reject_container_attrs("enums", &input.attrs)?;
            expand_enum(&input, data)
        }
        Data::Union(_) => Err(syn::Error::new_spanned(
            &input.ident,
            "unions cannot derive FlowerType",
        )),
    }
}

struct StructField {
    ident: syn::Ident,
    ty: Type,
    opts: FieldOpts,
}

fn struct_fields(input: &DeriveInput, data: &syn::DataStruct) -> syn::Result<Vec<StructField>> {
    match &data.fields {
        Fields::Named(fields) => fields
            .named
            .iter()
            .map(|field| {
                Ok(StructField {
                    ident: field
                        .ident
                        .clone()
                        .ok_or_else(|| syn::Error::new_spanned(field, "expected a named field"))?,
                    ty: field.ty.clone(),
                    opts: field_opts(field)?,
                })
            })
            .collect(),
        _ => Err(syn::Error::new_spanned(
            &input.ident,
            "FlowerType structs must use named fields",
        )),
    }
}

fn expand_struct(
    input: &DeriveInput,
    data: &syn::DataStruct,
    camel: bool,
) -> syn::Result<proc_macro2::TokenStream> {
    let fields = struct_fields(input, data)?;

    let mut defaulted = Vec::new();
    for field in fields
        .iter()
        .filter(|field| field.opts.skip || matches!(field.opts.default, Some(None)))
    {
        if let Some(param) = bare_param(&field.ty)
            && !defaulted.iter().any(|name| name == param)
        {
            defaulted.push(param.clone());
        }
    }

    let mut schema_fields = Vec::new();
    let mut known_keys: Vec<String> = Vec::new();
    let mut decode_fields = Vec::new();
    let mut encode_fields = Vec::new();
    for field in &fields {
        // A raw field (`r#type`) is plain on the wire (`type`).
        let plain = syn::ext::IdentExt::unraw(&field.ident).to_string();
        let name = if camel { camel_case(&plain) } else { plain };
        let ty = &field.ty;
        let var = &field.ident;
        if field.opts.skip {
            decode_fields.push(quote! {
                #var: ::core::default::Default::default()
            });
            continue;
        }
        if known_keys.contains(&name) {
            return Err(syn::Error::new_spanned(
                &field.ident,
                format!("two fields answer to the wire name `{name}`"),
            ));
        }
        known_keys.push(name.clone());
        if let Some(default) = &field.opts.default {
            schema_fields.push(quote! {
                ::flower_sdk::schema::v::optional(
                    #name,
                    <#ty as ::flower_sdk::FlowerType>::SCHEMA,
                )
            });
            let fallback = match default {
                Some(path) => quote!(#path()),
                None => quote!(::core::default::Default::default()),
            };
            decode_fields.push(quote! {
                #var: match __map.remove(#name) {
                    ::core::option::Option::None => #fallback,
                    ::core::option::Option::Some(__value) => {
                        <#ty as ::flower_sdk::FlowerType>::decode(__value)?
                    }
                }
            });
        } else {
            schema_fields.push(quote! {
                ::flower_sdk::schema::v::field_or_optional(
                    #name,
                    <#ty as ::flower_sdk::FlowerType>::SCHEMA,
                    <#ty as ::flower_sdk::FlowerType>::OPTIONAL_FIELD,
                )
            });
            decode_fields.push(quote! {
                #var: <#ty as ::flower_sdk::FlowerType>::decode_field(__map.remove(#name))?
            });
        }
        encode_fields.push(quote! {
            if let ::core::option::Option::Some(__value) =
                <#ty as ::flower_sdk::FlowerType>::encode_field(&self.#var)? {
                __map.insert(::alloc::string::String::from(#name), __value);
            }
        });
    }

    // An all-skipped struct knows no keys; the unknown-member scan then
    // rejects every member through the catch-all alone.
    let known_arm = if known_keys.is_empty() {
        quote!()
    } else {
        quote!(#(#known_keys)|* => {},)
    };

    let ident = &input.ident;
    let generics = bound_generics(&input.generics, &defaulted);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::flower_sdk::FlowerType for #ident #ty_generics #where_clause {
            const SCHEMA: ::flower_sdk::schema::Schema =
                ::flower_sdk::schema::v::object(&[ #(#schema_fields),* ]);

            fn encode(&self) -> ::flower_sdk::Result<::flower_sdk::Value> {
                let mut __map = ::alloc::collections::BTreeMap::new();
                #(#encode_fields)*
                ::core::result::Result::Ok(::flower_sdk::Value::Object(__map))
            }

            fn decode(__value: ::flower_sdk::Value) -> ::flower_sdk::Result<Self> {
                let mut __map = match __value {
                    ::flower_sdk::Value::Object(__map) => __map,
                    _ => {
                        return ::core::result::Result::Err(::flower_sdk::fail(
                            ::flower_sdk::error::codes::TYPE_MISMATCH,
                            "expected object",
                        ));
                    }
                };
                for __key in __map.keys() {
                    match __key.as_str() {
                        #known_arm
                        _ => {
                            return ::core::result::Result::Err(::flower_sdk::fail(
                                ::flower_sdk::error::codes::INVALID_VALUE,
                                ::alloc::format!("unknown member `{__key}`"),
                            ));
                        }
                    }
                }
                ::core::result::Result::Ok(#ident { #(#decode_fields),* })
            }
        }
    })
}

/// Enum shape: unit variants (`Status`) or single-payload variants
/// (`Event::Created(Task)`).
enum EnumShape {
    Unit,
    Newtype,
}

fn expand_enum(input: &DeriveInput, data: &syn::DataEnum) -> syn::Result<proc_macro2::TokenStream> {
    if data.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "FlowerType enums need at least one variant",
        ));
    }
    let shape = enum_shape(input, data)?;
    match shape {
        EnumShape::Unit => expand_unit_enum(input, data),
        EnumShape::Newtype => expand_newtype_enum(input, data),
    }
}

fn enum_shape(input: &DeriveInput, data: &syn::DataEnum) -> syn::Result<EnumShape> {
    let mut shape: Option<EnumShape> = None;
    for variant in &data.variants {
        reject_container_attrs("enum variants", &variant.attrs)?;
        for field in variant.fields.iter() {
            reject_container_attrs("enum variant fields", &field.attrs)?;
        }
        let current = match &variant.fields {
            Fields::Unit => EnumShape::Unit,
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => EnumShape::Newtype,
            _ => {
                return Err(syn::Error::new_spanned(
                    &variant.ident,
                    "FlowerType enums support only unit or single-payload variants",
                ));
            }
        };
        match &shape {
            None => shape = Some(current),
            Some(EnumShape::Unit) if matches!(current, EnumShape::Unit) => {}
            Some(EnumShape::Newtype) if matches!(current, EnumShape::Newtype) => {}
            _ => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "FlowerType enums cannot mix unit and payload variants",
                ));
            }
        }
    }
    shape.ok_or_else(|| syn::Error::new_spanned(&input.ident, "FlowerType enums need a shape"))
}

fn expand_unit_enum(
    input: &DeriveInput,
    data: &syn::DataEnum,
) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;
    let generics = bound_generics(&input.generics, &[]);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let names: Vec<String> = data
        .variants
        .iter()
        .map(|variant| variant.ident.to_string())
        .collect();
    let variants: Vec<&syn::Ident> = data.variants.iter().map(|variant| &variant.ident).collect();
    Ok(quote! {
        impl #impl_generics ::flower_sdk::FlowerType for #ident #ty_generics #where_clause {
            const SCHEMA: ::flower_sdk::schema::Schema =
                ::flower_sdk::schema::v::str_enum(&[ #(#names),* ]);

            fn encode(&self) -> ::flower_sdk::Result<::flower_sdk::Value> {
                ::core::result::Result::Ok(match self {
                    #(#ident::#variants => ::flower_sdk::Value::String(
                        ::alloc::string::String::from(#names),
                    ),)*
                })
            }

            fn decode(__value: ::flower_sdk::Value) -> ::flower_sdk::Result<Self> {
                match __value {
                    ::flower_sdk::Value::String(__text) => match __text.as_str() {
                        #(#names => ::core::result::Result::Ok(#ident::#variants),)*
                        _ => ::core::result::Result::Err(::flower_sdk::fail(
                            ::flower_sdk::error::codes::INVALID_VALUE,
                            "unknown enum variant",
                        )),
                    },
                    _ => ::core::result::Result::Err(::flower_sdk::fail(
                        ::flower_sdk::error::codes::TYPE_MISMATCH,
                        "expected enum string",
                    )),
                }
            }
        }
    })
}

fn expand_newtype_enum(
    input: &DeriveInput,
    data: &syn::DataEnum,
) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;
    let generics = bound_generics(&input.generics, &[]);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let mut names = Vec::new();
    let mut variants = Vec::new();
    let mut types = Vec::new();
    for variant in &data.variants {
        let field =
            variant.fields.iter().next().ok_or_else(|| {
                syn::Error::new_spanned(&variant.ident, "expected one payload field")
            })?;
        names.push(variant.ident.to_string());
        variants.push(variant.ident.clone());
        types.push(field.ty.clone());
    }

    Ok(quote! {
        impl #impl_generics ::flower_sdk::FlowerType for #ident #ty_generics #where_clause {
            const SCHEMA: ::flower_sdk::schema::Schema =
                ::flower_sdk::schema::v::object(&[
                    ::flower_sdk::schema::v::field(
                        "tag",
                        ::flower_sdk::schema::v::str_enum(&[ #(#names),* ]),
                    ),
                    ::flower_sdk::schema::v::field("value", ::flower_sdk::schema::v::json()),
                ]);

            fn encode(&self) -> ::flower_sdk::Result<::flower_sdk::Value> {
                let (__tag, __payload) = match self {
                    #(
                        #ident::#variants(__payload) => (
                            #names,
                            <#types as ::flower_sdk::FlowerType>::encode(__payload)?,
                        ),
                    )*
                };
                let mut __map = ::alloc::collections::BTreeMap::new();
                __map.insert(
                    ::alloc::string::String::from("tag"),
                    ::flower_sdk::Value::String(::alloc::string::String::from(__tag)),
                );
                __map.insert(::alloc::string::String::from("value"), __payload);
                ::core::result::Result::Ok(::flower_sdk::Value::Object(__map))
            }

            fn decode(__value: ::flower_sdk::Value) -> ::flower_sdk::Result<Self> {
                let mut __map = match __value {
                    ::flower_sdk::Value::Object(__map) => __map,
                    _ => {
                        return ::core::result::Result::Err(::flower_sdk::fail(
                            ::flower_sdk::error::codes::TYPE_MISMATCH,
                            "expected tagged object",
                        ));
                    }
                };
                for __key in __map.keys() {
                    match __key.as_str() {
                        "tag" | "value" => {}
                        _ => {
                            return ::core::result::Result::Err(::flower_sdk::fail(
                                ::flower_sdk::error::codes::INVALID_VALUE,
                                ::alloc::format!("unknown member `{__key}`"),
                            ));
                        }
                    }
                }
                let __tag = match __map.remove("tag") {
                    ::core::option::Option::Some(::flower_sdk::Value::String(__tag)) => __tag,
                    ::core::option::Option::Some(_) => {
                        return ::core::result::Result::Err(::flower_sdk::fail(
                            ::flower_sdk::error::codes::TYPE_MISMATCH,
                            "expected enum string",
                        ));
                    }
                    ::core::option::Option::None => {
                        return ::core::result::Result::Err(::flower_sdk::fail(
                            ::flower_sdk::error::codes::MISSING_FIELD,
                            "missing tag member",
                        ));
                    }
                };
                let __payload = match __map.remove("value") {
                    ::core::option::Option::Some(__payload) => __payload,
                    ::core::option::Option::None => {
                        return ::core::result::Result::Err(::flower_sdk::fail(
                            ::flower_sdk::error::codes::MISSING_FIELD,
                            "missing value member",
                        ));
                    }
                };
                match __tag.as_str() {
                    #(
                        #names => ::core::result::Result::Ok(#ident::#variants(
                            <#types as ::flower_sdk::FlowerType>::decode(__payload)?,
                        )),
                    )*
                    _ => ::core::result::Result::Err(::flower_sdk::fail(
                        ::flower_sdk::error::codes::INVALID_VALUE,
                        "unknown enum variant",
                    )),
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn derive(tokens: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
        let input: DeriveInput = syn::parse2(tokens).unwrap();
        expand(input)
    }

    #[test]
    fn struct_schema_and_round_trip_fragments() {
        let out = derive(quote! {
            struct Task {
                title: String,
                #[flower(default)]
                count: u64,
                #[flower(skip)]
                cache: u64,
            }
        })
        .unwrap()
        .to_string();
        assert!(out.contains("OPTIONAL_FIELD"));
        assert!(out.contains("Default :: default"));
        assert!(out.contains("decode_field"));
        assert!(out.contains("unknown member"));
        assert!(out.contains("BTreeMap"));
        assert!(
            !out.contains("\"cache\""),
            "skipped fields have no wire name: {out}"
        );
    }

    #[test]
    fn all_skipped_structs_still_scan() {
        let out = derive(quote! {
            struct Cache {
                #[flower(skip)]
                entries: u64,
            }
        })
        .unwrap()
        .to_string();
        assert!(out.contains("unknown member"));
        assert!(!out.contains("\"entries\""));
    }

    #[test]
    fn rename_is_rejected() {
        let err = derive(quote! {
            struct Task {
                #[flower(rename = "label")]
                title: String,
            }
        })
        .unwrap_err();
        assert!(err.to_string().contains("rename is not supported"));
    }

    #[test]
    fn flatten_points_at_nesting() {
        let err = derive(quote! {
            struct Task {
                #[flower(flatten)]
                meta: Meta,
            }
        })
        .unwrap_err();
        assert!(err.to_string().contains("nest the struct"));
    }

    #[test]
    fn redact_points_at_field_wrapper() {
        let err = derive(quote! {
            struct Task {
                #[flower(redact)]
                secret: String,
            }
        })
        .unwrap_err();
        assert!(err.to_string().contains("typed::Field"));
    }

    #[test]
    fn camel_case_names_members() {
        assert_eq!(camel_case("lease_ms"), "leaseMs");
        assert_eq!(camel_case("stores_per_tenant"), "storesPerTenant");
        assert_eq!(camel_case("id"), "id");
        let out = derive(quote! {
            #[flower(rename_all = "camelCase")]
            struct Setup { bake_ms: u64 }
        })
        .unwrap()
        .to_string();
        assert!(out.contains("\"bakeMs\""));
        assert!(!out.contains("\"bake_ms\""));
        assert!(
            derive(quote! {
                #[flower(rename_all = "kebab-case")]
                struct Setup { bake_ms: u64 }
            })
            .is_err()
        );
    }

    #[test]
    fn two_fields_never_share_a_wire_name() {
        // `a_b` renames to `aB`, which another field already is.
        let err = derive(quote! {
            #[flower(rename_all = "camelCase")]
            struct Clash { a_b: u64, aB: u64 }
        })
        .unwrap_err();
        assert!(err.to_string().contains("`aB`"), "{err}");
    }

    #[test]
    fn raw_field_names_are_plain_on_the_wire() {
        let out = derive(quote! {
            struct Tagged { r#type: String }
        })
        .unwrap()
        .to_string();
        assert!(out.contains("\"type\""), "{out}");
        assert!(!out.contains("\"r#type\""), "{out}");
    }

    #[test]
    fn container_attrs_are_rejected() {
        let err = derive(quote! {
            #[flower(skip)]
            struct Task {
                title: String,
            }
        })
        .unwrap_err();
        assert!(err.to_string().contains("flower type option"));
    }

    #[test]
    fn tuple_structs_are_rejected() {
        let err = derive(quote! {
            struct Key(pub String);
        })
        .unwrap_err();
        assert!(err.to_string().contains("named fields"));
    }

    #[test]
    fn unit_enum_expands_to_str_enum() {
        let out = derive(quote! {
            enum Status {
                Open,
                Closed,
            }
        })
        .unwrap()
        .to_string();
        assert!(out.contains("str_enum"));
        assert!(out.contains("unknown enum variant"));
    }

    #[test]
    fn newtype_enum_expands_to_tagged_object() {
        let out = derive(quote! {
            enum Event {
                Created(Task),
                Closed(String),
            }
        })
        .unwrap()
        .to_string();
        assert!(out.contains("\"tag\""));
        assert!(out.contains("\"value\""));
        assert!(out.contains("missing tag member"));
    }

    #[test]
    fn mixed_enum_shapes_are_rejected() {
        let err = derive(quote! {
            enum Event {
                Ping,
                Created(String),
            }
        })
        .unwrap_err();
        assert!(err.to_string().contains("cannot mix"));
    }
}
