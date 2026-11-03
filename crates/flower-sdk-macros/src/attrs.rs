//! Shared `#[flower(...)]` attribute parsing.

use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{Attribute, Expr, Lit, Meta, Path, Token};

/// Collect `#[flower(...)]` metas in order.
pub(crate) fn flower_metas(attrs: &[Attribute]) -> syn::Result<Vec<Meta>> {
    let mut metas = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("flower") {
            continue;
        }
        let items = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        metas.extend(items);
    }
    Ok(metas)
}

/// Read a `key = "literal"` string value.
pub(crate) fn lit_str_value(meta: &Meta) -> syn::Result<String> {
    match meta {
        Meta::NameValue(value) => match &value.value {
            Expr::Lit(lit) => match &lit.lit {
                Lit::Str(text) => Ok(text.value()),
                _ => Err(syn::Error::new_spanned(
                    &value.value,
                    "expected a string literal",
                )),
            },
            _ => Err(syn::Error::new_spanned(
                &value.value,
                "expected a string literal",
            )),
        },
        _ => Err(syn::Error::new_spanned(meta, "expected `key = \"...\"`")),
    }
}

/// Read a `key = Path` value.
pub(crate) fn path_value(meta: &Meta) -> syn::Result<Path> {
    match meta {
        Meta::NameValue(value) => match &value.value {
            Expr::Path(path) => Ok(path.path.clone()),
            _ => Err(syn::Error::new_spanned(&value.value, "expected a path")),
        },
        _ => Err(syn::Error::new_spanned(meta, "expected `key = Path`")),
    }
}

/// The single identifier of a bare `flag` meta, if it is one.
pub(crate) fn flag_ident(meta: &Meta) -> Option<String> {
    match meta {
        Meta::Path(path) => path.get_ident().map(|ident| ident.to_string()),
        _ => None,
    }
}

/// The key path of a `key = ...` or `key(...)` meta, if it has one.
pub(crate) fn key_path(meta: &Meta) -> Option<&Path> {
    match meta {
        Meta::NameValue(value) => Some(&value.path),
        Meta::List(list) => Some(&list.path),
        Meta::Path(_) => None,
    }
}

/// Parse the inner metas of a `key(...)` list by re-parsing its tokens.
pub(crate) fn nested_metas(meta: &Meta) -> syn::Result<Vec<Meta>> {
    match meta {
        Meta::List(list) => {
            let items =
                Punctuated::<Meta, Token![,]>::parse_terminated.parse2(list.tokens.clone())?;
            Ok(items.into_iter().collect())
        }
        _ => Err(syn::Error::new_spanned(meta, "expected `key(...)`")),
    }
}

/// Upper-case `node` plus `suffix` into a static ident, rejecting names that
/// cannot survive the conversion instead of panicking inside the macro.
pub(crate) fn screaming(node: &syn::Ident, suffix: &str) -> syn::Result<syn::Ident> {
    let base = syn::ext::IdentExt::unraw(node).to_string();
    if !base.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(syn::Error::new_spanned(
            node,
            "names must be ASCII alphanumeric to derive a static name",
        ));
    }
    Ok(quote::format_ident!(
        "{}{}",
        base.to_ascii_uppercase(),
        suffix
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn metas(tokens: proc_macro2::TokenStream) -> Vec<Meta> {
        let attr: Attribute = Attribute::parse_outer
            .parse2(quote!(#[flower(#tokens)]))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        flower_metas(&[attr]).unwrap()
    }

    #[test]
    fn reads_names_paths_and_flags() {
        let items = metas(quote!(name = "tasks", key = TaskId, index));
        assert_eq!(items.len(), 3);
        assert_eq!(lit_str_value(&items[0]).unwrap(), "tasks");
        assert!(path_value(&items[1]).unwrap().is_ident("TaskId"));
        assert_eq!(flag_ident(&items[2]).as_deref(), Some("index"));
        assert!(lit_str_value(&items[1]).is_err());
        assert!(path_value(&items[0]).is_err());
    }

    #[test]
    fn nested_lists_reparse() {
        let items = metas(quote!(fields(owner, title)));
        let inner = nested_metas(&items[0]).unwrap();
        assert_eq!(inner.len(), 2);
        assert_eq!(flag_ident(&inner[0]).as_deref(), Some("owner"));
    }
}
