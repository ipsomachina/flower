//! Links between modules: deploy-time checks of exports, imports,
//! caller allowlists and contract compatibility.
//!
//! A module imports collections, derived values and methods from other
//! modules, by name or through a `$slot` its deployment binds. Deploying
//! a module checks its own imports against the providers' active
//! exports, and every deployed importer of the module against what it is
//! about to export, so neither side can break the other. Compatibility
//! is structural ([`Schema::accepts`]): a provider accepts every
//! argument its importer sends and produces only results and failures
//! the importer accepts; collection keys agree both ways and rows fit
//! the importer's row contract. An import without a contract links
//! without that proof. Any failure is `MODULE_CONFLICT`, naming what
//! would break; the host still checks the provider's own contract on
//! every call between modules, so a link that slips past (two modules
//! deployed concurrently) fails at the call instead.

use std::collections::HashMap;

use flower_sdk::error::codes;
use flower_sdk::schema::Schema;
use flower_sdk::{Failure, fail};

use crate::descriptors::{DefKind, ImportDef, ModuleDef};

fn conflict(message: String) -> Failure {
    fail(codes::MODULE_CONFLICT, message)
}

/// Check that deploying `target` as `module` keeps every link among the
/// database's `active` modules.
pub fn check_links(
    active: &HashMap<String, ModuleDef>,
    module: &str,
    target: &ModuleDef,
) -> Result<(), Failure> {
    for import in &target.imports {
        let Some(provider_name) = target.resolve_module(&import.module) else {
            return Err(conflict(format!("slot `{}` is not bound", import.module)));
        };
        if provider_name == module {
            return Err(conflict(format!("`{module}` imports from itself")));
        }
        let Some(provider) = active.get(provider_name) else {
            return Err(conflict(format!(
                "`{module}` imports from `{provider_name}`, which is not deployed"
            )));
        };
        check_import(module, import, provider_name, provider).map_err(conflict)?;
    }
    let mut importers: Vec<&String> = active.keys().collect();
    importers.sort();
    for name in importers {
        if name == module {
            continue;
        }
        let importer = &active[name];
        for import in &importer.imports {
            if importer.resolve_module(&import.module) == Some(module) {
                check_import(name, import, module, target).map_err(|reason| {
                    conflict(format!("deployed module `{name}` would break: {reason}"))
                })?;
            }
        }
    }
    Ok(())
}

/// Check that removing `module` breaks no link among the database's
/// `active` modules: none of the others imports from it, directly or
/// through a slot it binds to it. Fails `MODULE_CONFLICT` naming them.
pub fn check_removal(active: &HashMap<String, ModuleDef>, module: &str) -> Result<(), Failure> {
    let mut importers: Vec<&String> = active
        .iter()
        .filter(|(name, importer)| {
            name.as_str() != module
                && (importer
                    .imports
                    .iter()
                    .any(|import| importer.resolve_module(&import.module) == Some(module))
                    || importer.bindings.values().any(|bound| bound == module))
        })
        .map(|(name, _)| name)
        .collect();
    if importers.is_empty() {
        return Ok(());
    }
    importers.sort();
    let named: Vec<String> = importers.iter().map(|name| format!("`{name}`")).collect();
    Err(conflict(format!(
        "`{module}` cannot be removed: deployed module {} imports from it",
        named.join(", ")
    )))
}

/// Whether `provider` (deployed as `provider_name`) satisfies what
/// `importer` imports from it.
fn check_import(
    importer: &str,
    import: &ImportDef,
    provider_name: &str,
    provider: &ModuleDef,
) -> Result<(), String> {
    for collection in &import.collections {
        let name = &collection.name;
        let def = provider
            .collection(name)
            .filter(|_| provider.exports.collections.contains(name))
            .ok_or_else(|| format!("`{provider_name}` does not export collection `{name}`"))?;
        if !(def.key_schema.accepts(&collection.key) && collection.key.accepts(&def.key_schema)) {
            return Err(format!(
                "the keys of `{provider_name}`'s collection `{name}` differ from `{importer}`'s import"
            ));
        }
        if !collection.row.accepts(&def.value_schema) {
            return Err(format!(
                "rows of `{provider_name}`'s collection `{name}` may not fit `{importer}`'s import"
            ));
        }
    }
    for definition in &import.definitions {
        let name = &definition.name;
        let exported = match definition.kind {
            DefKind::Derived => provider.exports.derived.contains(name),
            _ => provider.exports.methods.contains(name),
        };
        if !exported {
            return Err(format!("`{provider_name}` does not export `{name}`"));
        }
        let kind = provider.def_kinds.get(name).copied();
        if kind != Some(definition.kind) {
            return Err(format!(
                "`{provider_name}`'s `{name}` is {}, imported as {}",
                kind_name(kind),
                kind_name(Some(definition.kind))
            ));
        }
        if let Some(callers) = provider.exports.callers.get(name)
            && !callers.contains(importer)
        {
            return Err(format!(
                "`{provider_name}` lets only {} call `{name}`",
                callers
                    .iter()
                    .map(|caller| format!("`{caller}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let Some(contract) = &definition.contract else {
            continue;
        };
        let args = provider.args_schema(name).copied().unwrap_or(Schema::Json);
        if !args.accepts(&contract.args) {
            return Err(format!(
                "`{provider_name}`'s `{name}` may refuse arguments `{importer}` sends"
            ));
        }
        if !contract.result.accepts(&provider.result_schema(name)) {
            return Err(format!(
                "`{provider_name}`'s `{name}` may return results `{importer}` does not accept"
            ));
        }
        if !contract.errors.accepts(&provider.errors_schema(name)) {
            return Err(format!(
                "`{provider_name}`'s `{name}` may fail in ways `{importer}` does not accept"
            ));
        }
    }
    Ok(())
}

fn kind_name(kind: Option<DefKind>) -> &'static str {
    match kind {
        Some(DefKind::Query) => "a query",
        Some(DefKind::Mutation) => "a mutation",
        Some(DefKind::Derived) => "a derived value",
        Some(DefKind::Transaction) => "a transaction",
        None => "missing",
    }
}
