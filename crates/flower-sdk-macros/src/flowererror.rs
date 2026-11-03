//! `#[derive(FlowerError)]` expansion.
//!
//! Each variant carries `#[flower(code = "...")]`, which becomes the
//! `Failure` code; codes discriminate variants, so
//! the schema stays `json`. Unit variants
//! encode without details, payload variants encode the payload as details
//! (an unencodable payload surfaces its codec failure instead). Decoding
//! treats absent details as null, so `Option` payloads round-trip. Unknown
//! codes convert back to the original failure as `Err`.
//!
//! `#[flower(retryable)]` marks variants safe to retry and feeds the
//! generated `is_retryable` helper. `#[flower(message = "...")]` sets a
//! variant's message (its name by default).
//!
//! One single-payload variant may be `#[flower(other)]` over
//! `flower_sdk::Failure`: every other failure converts into it (so `?`
//! works on SDK calls, through the generated `From<Failure>`), and it
//! converts back unchanged. The contract then admits any code.

use crate::{attrs, flowertype};
use quote::quote;
use syn::{Data, DeriveInput, Fields};

/// Variant-level error metadata.
struct Variant {
    ident: syn::Ident,
    ty: Option<syn::Type>,
    code: String,
    message: Option<String>,
    retryable: bool,
    other: bool,
}

fn variant(member: &syn::Variant) -> syn::Result<Variant> {
    let ty = match &member.fields {
        Fields::Unit => None,
        Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
            Some(fields.unnamed.first().unwrap().ty.clone())
        }
        _ => {
            return Err(syn::Error::new_spanned(
                &member.ident,
                "FlowerError variants must be unit or single-payload variants",
            ));
        }
    };
    let mut code = None;
    let mut message = None;
    let mut retryable = false;
    let mut other = false;
    for meta in attrs::flower_metas(&member.attrs)? {
        if let Some(flag) = attrs::flag_ident(&meta) {
            match flag.as_str() {
                "retryable" => {
                    retryable = true;
                    continue;
                }
                "other" => {
                    other = true;
                    continue;
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        format!("unknown flower error option `{flag}`"),
                    ));
                }
            }
        }
        let key = attrs::key_path(&meta).and_then(|path| path.get_ident());
        match key.map(|ident| ident.to_string()).as_deref() {
            Some("code") => code = Some(attrs::lit_str_value(&meta)?),
            Some("message") => message = Some(attrs::lit_str_value(&meta)?),
            _ => {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "unknown flower error option (expected code = \"...\", message = \"...\", \
                     retryable, other)",
                ));
            }
        }
    }
    if other {
        if ty.is_none() || code.is_some() || message.is_some() {
            return Err(syn::Error::new_spanned(
                &member.ident,
                "the `other` variant holds a `Failure` and takes no code or message",
            ));
        }
        return Ok(Variant {
            ident: member.ident.clone(),
            ty,
            code: String::new(),
            message: None,
            retryable,
            other,
        });
    }
    let Some(code) = code else {
        return Err(syn::Error::new_spanned(
            &member.ident,
            "FlowerError variants need `#[flower(code = \"...\")]`",
        ));
    };
    // Preserve the SDK's naming convention; the protocol carries error origin
    // explicitly and never uses code casing to classify failures.
    if !code.starts_with(|first: char| first.is_ascii_uppercase())
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(syn::Error::new_spanned(
            &member.ident,
            format!("failure codes use UPPER_SNAKE_CASE, not `{code}`"),
        ));
    }
    Ok(Variant {
        ident: member.ident.clone(),
        ty,
        code,
        message,
        retryable,
        other,
    })
}

pub(crate) fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let data = match &input.data {
        Data::Enum(data) => data,
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "FlowerError can only derive on enums",
            ));
        }
    };
    if data.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "FlowerError enums need at least one variant",
        ));
    }
    if !attrs::flower_metas(&input.attrs)?.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "flower attributes go on variants, not on the enum",
        ));
    }
    let variants: Vec<Variant> = data
        .variants
        .iter()
        .map(variant)
        .collect::<syn::Result<_>>()?;

    let ident = &input.ident;
    let generics = flowertype::bound_generics(&input.generics, &[]);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let others: Vec<&Variant> = variants.iter().filter(|member| member.other).collect();
    if others.len() > 1 {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "at most one variant is `other`",
        ));
    }
    let other = others.first().map(|member| &member.ident);

    let mut into_arms = Vec::new();
    let mut from_arms = Vec::new();
    let mut retry_arms = Vec::new();
    let mut codes: Vec<&String> = variants
        .iter()
        .filter(|member| !member.other)
        .map(|member| &member.code)
        .collect();
    codes.sort();
    codes.dedup();
    for member in &variants {
        let name = member
            .message
            .clone()
            .unwrap_or_else(|| member.ident.to_string());
        let code = &member.code;
        let pattern = &member.ident;
        retry_arms.push(if member.ty.is_some() {
            let retryable = member.retryable;
            quote!(#ident::#pattern(_) => #retryable)
        } else {
            let retryable = member.retryable;
            quote!(#ident::#pattern => #retryable)
        });
        if member.other {
            into_arms.push(quote!(#ident::#pattern(__failure) => __failure));
            continue;
        }
        match &member.ty {
            Some(ty) => {
                into_arms.push(quote! {
                    #ident::#pattern(__payload) => {
                        match <#ty as ::flower_sdk::FlowerType>::encode(&__payload) {
                            ::core::result::Result::Ok(__details) => {
                                ::flower_sdk::Failure::with_details(#code, #name, __details)
                            }
                            ::core::result::Result::Err(__failure) => __failure,
                        }
                    }
                });
                from_arms.push(quote! {
                    #code => ::core::result::Result::Ok(#ident::#pattern(
                        <#ty as ::flower_sdk::FlowerType>::decode(
                            __failure
                                .details
                                .clone()
                                .unwrap_or(::flower_sdk::Value::Null),
                        )?,
                    ))
                });
            }
            None => {
                into_arms.push(quote! {
                    #ident::#pattern => ::flower_sdk::Failure::new(#code, #name)
                });
                from_arms.push(quote! {
                    #code => ::core::result::Result::Ok(#ident::#pattern)
                });
            }
        }
    }

    let code_schema = if other.is_some() {
        quote!(::flower_sdk::schema::v::string().build())
    } else {
        quote!(::flower_sdk::schema::v::str_enum(&[#(#codes),*]))
    };
    let fallback = match other {
        Some(variant) => quote!(::core::result::Result::Ok(#ident::#variant(__failure.clone()))),
        None => quote!(::core::result::Result::Err(__failure.clone())),
    };
    let from_failure = match other {
        Some(variant) => quote! {
            impl #impl_generics ::core::convert::From<::flower_sdk::Failure>
                for #ident #ty_generics #where_clause
            {
                fn from(failure: ::flower_sdk::Failure) -> Self {
                    #ident::#variant(failure)
                }
            }
        },
        None => quote!(),
    };

    Ok(quote! {
        #from_failure

        impl #impl_generics ::flower_sdk::BusinessError for #ident #ty_generics #where_clause {
            /// The failure object: one of the declared codes (any code
            /// with an `other` variant), a message and optional details.
            const SCHEMA: ::flower_sdk::schema::Schema = ::flower_sdk::schema::v::object(&[
                ::flower_sdk::schema::v::field("code", #code_schema),
                ::flower_sdk::schema::v::field(
                    "message",
                    ::flower_sdk::schema::v::string().build(),
                ),
                ::flower_sdk::schema::v::optional("details", ::flower_sdk::schema::v::json()),
            ]);

            fn into_failure(self) -> ::flower_sdk::Failure {
                match self {
                    #(#into_arms,)*
                }
            }

            fn from_failure(
                __failure: &::flower_sdk::Failure,
            ) -> ::flower_sdk::Result<Self> {
                match __failure.code.as_str() {
                    #(#from_arms,)*
                    _ => #fallback,
                }
            }
        }

        impl #impl_generics #ident #ty_generics #where_clause {
            /// Whether this variant is safe to retry.
            pub fn is_retryable(&self) -> bool {
                match self {
                    #(#retry_arms,)*
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
    fn unit_and_payload_variants_expand() {
        let out = derive(quote! {
            enum TaskError {
                #[flower(code = "NOT_FOUND")]
                Missing,
                #[flower(code = "INVALID", retryable)]
                Invalid(String),
            }
        })
        .unwrap()
        .to_string();
        assert!(out.contains("BusinessError"));
        assert!(out.contains("into_failure"));
        assert!(out.contains("from_failure"));
        assert!(out.contains("\"NOT_FOUND\""));
        assert!(out.contains("with_details"));
        assert!(out.contains("is_retryable"));
    }

    #[test]
    fn messages_and_a_catch_all_variant() {
        let out = derive(quote! {
            enum OrderError {
                #[flower(code = "OUT_OF_STOCK", message = "The goblins have run out of dough")]
                OutOfStock,
                #[flower(other)]
                Host(Failure),
            }
        })
        .unwrap()
        .to_string();
        assert!(out.contains("\"The goblins have run out of dough\""));
        assert!(out.contains("From < :: flower_sdk :: Failure >"));
        assert!(out.contains("Ok (OrderError :: Host (__failure . clone ()))"));
        for bad in [
            quote! {
                enum E {
                    #[flower(other)]
                    A,
                }
            },
            quote! {
                enum E {
                    #[flower(other)]
                    A(Failure),
                    #[flower(other)]
                    B(Failure),
                }
            },
        ] {
            assert!(derive(bad).is_err());
        }
    }

    #[test]
    fn missing_code_is_rejected() {
        let err = derive(quote! {
            enum TaskError {
                Missing,
            }
        })
        .unwrap_err();
        assert!(err.to_string().contains("need `#[flower(code"));
    }

    #[test]
    fn structs_are_rejected() {
        let err = derive(quote! {
            struct TaskError {
                code: String,
            }
        })
        .unwrap_err();
        assert!(err.to_string().contains("only derive on enums"));
    }

    #[test]
    fn codes_are_upper_snake_case() {
        // v1's rule, and how the server tells a call's own failure (422)
        // from its own: a lowercase code would come back as a host error.
        for code in ["not_found", "NotFound", "1ST", "BAD-CODE", ""] {
            let err = derive(quote! {
                enum TaskError {
                    #[flower(code = #code)]
                    Missing,
                }
            })
            .unwrap_err();
            assert!(
                err.to_string().contains("UPPER_SNAKE_CASE"),
                "{code}: {err}"
            );
        }
        assert!(
            derive(quote! {
                enum TaskError {
                    #[flower(code = "NOT_FOUND_2")]
                    Missing,
                }
            })
            .is_ok()
        );
    }

    #[test]
    fn container_attrs_are_rejected() {
        let err = derive(quote! {
            #[flower(code = "ALL")]
            enum TaskError {
                #[flower(code = "ONE")]
                One,
            }
        })
        .unwrap_err();
        assert!(err.to_string().contains("not on the enum"));
    }
}
