use quote::{format_ident, quote};
use syn::{Item, ItemMod};

fn reference(name: &syn::Ident, attrs: &[syn::Attribute]) -> proc_macro2::TokenStream {
    let conditions = attrs.iter().filter(|attr| attr.path().is_ident("cfg"));
    quote! { #(#conditions)* &#name }
}

pub fn expand(mut module: ItemMod) -> syn::Result<proc_macro2::TokenStream> {
    let (_, items) = module.content.as_mut().ok_or_else(|| {
        syn::Error::new_spanned(&module.ident, "application requires an inline module")
    })?;
    let mut components = Vec::new();
    for item in items.iter_mut() {
        match item {
            Item::Fn(function)
                if function.attrs.iter().any(|attr| {
                    attr.path().segments.last().is_some_and(|part| {
                        matches!(
                            part.ident.to_string().as_str(),
                            "query" | "mutation" | "derived"
                        )
                    })
                }) =>
            {
                let name = function.sig.ident.to_string();
                let name = format_ident!("{}", name.trim_start_matches("r#").to_uppercase());
                components.push(reference(&name, &function.attrs));
            }
            Item::Struct(collection)
                if collection.attrs.iter().any(|attr| {
                    attr.path()
                        .segments
                        .last()
                        .is_some_and(|part| part.ident == "collection")
                }) =>
            {
                components.push(reference(&collection.ident, &collection.attrs));
            }
            Item::Static(component) => {
                if component.ident == "APP" {
                    return Err(syn::Error::new_spanned(
                        &component.ident,
                        "application reserves APP for its generated catalogue",
                    ));
                }
                if component
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("component"))
                {
                    component
                        .attrs
                        .retain(|attr| !attr.path().is_ident("component"));
                    components.push(reference(&component.ident, &component.attrs));
                }
            }
            _ => {}
        }
    }
    items.push(syn::parse_quote! {
        pub static APP: ::flower_sdk::App = ::flower_sdk::App::new(&[#(#components),*]);
    });
    Ok(quote! { #module })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registers_lexical_components_and_removes_helper_attributes() {
        let output = expand(syn::parse_quote! {
            mod shop {
                #[collection] struct Orders;
                #[query(http)] fn list() {}
                #[component] static SQL: SqlEndpoint = make();
                static ORDINARY: u32 = 1;
            }
        })
        .unwrap()
        .to_string();
        assert!(output.contains("& Orders , & LIST , & SQL"));
        assert!(!output.contains("# [component]"));
        assert!(!output.contains("& ORDINARY"));
    }
}
