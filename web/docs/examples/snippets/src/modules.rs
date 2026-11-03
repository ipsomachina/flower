//! Concepts › Modules and interfaces: an interface, a provider and a
//! consumer. In a real project each lives in its own crate: the
//! interface crate is a plain library both modules depend on.

use alloc::string::String;

use flower_sdk::{
    App, CallError, CallOutcome, Failure, FlowerError, FlowerType, MutationCtx, QueryCtx, Result,
    collection, fail, interface, mutation, query,
};

// ANCHOR: interface
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Item {
    pub name: String,
    pub stock: u64,
}

#[derive(FlowerError, Clone, Debug, PartialEq)]
pub enum StockError {
    #[flower(code = "UNKNOWN_ITEM")]
    Unknown,
    #[flower(code = "OUT_OF_STOCK")]
    OutOfStock(u64),
    #[flower(other)]
    Host(Failure),
}

/// The contract, versioned in its name: an incompatible change gets a
/// new interface rather than breaking deployed consumers.
#[interface(name = "inventory.v1", handle = InventoryApi)]
pub trait Inventory {
    #[query]
    fn item(ctx: &mut QueryCtx<'_, '_>, sku: String) -> flower_sdk::Result<Option<Item>>;

    #[mutation]
    fn reserve(
        ctx: &mut MutationCtx<'_, '_>,
        sku: String,
        quantity: u64,
    ) -> core::result::Result<u64, StockError>;
}
// ANCHOR_END: interface

// ANCHOR: provider
#[collection(name = "items", key = String, value = Item)]
pub struct Items;

pub struct Warehouse;

impl Inventory for Warehouse {
    fn item(ctx: &mut QueryCtx<'_, '_>, sku: String) -> flower_sdk::Result<Option<Item>> {
        Items::get(ctx, &sku)
    }

    fn reserve(
        ctx: &mut MutationCtx<'_, '_>,
        sku: String,
        quantity: u64,
    ) -> core::result::Result<u64, StockError> {
        let mut item = Items::get(ctx, &sku)?.ok_or(StockError::Unknown)?;
        if item.stock < quantity {
            return Err(StockError::OutOfStock(item.stock));
        }
        item.stock -= quantity;
        Items::set(ctx, &sku, &item)?;
        Ok(item.stock)
    }
}

/// The provider component brings the interface's methods, exported to
/// every module. Add `.http()` to let callers reach them over HTTP too.
pub static PROVIDER_APP: App = App::new(&[&Items, &InventoryProvider::<Warehouse>::COMPONENT]);
// ANCHOR_END: provider

// ANCHOR: consumer
/// Wherever this deployment's `inventory` binding points: the typed
/// call handle and the import declaring the contract it must meet.
static INVENTORY: InventoryApi = InventoryApi::import("$inventory");

#[mutation(http)]
pub fn checkout(ctx: &mut MutationCtx<'_, '_>, sku: String, quantity: u64) -> Result<String> {
    match INVENTORY.try_reserve(ctx, sku, quantity) {
        Ok(CallOutcome::Success(left)) => Ok(alloc::format!("reserved; {left} left")),
        Ok(CallOutcome::Failure(StockError::OutOfStock(left))) => {
            Ok(alloc::format!("only {left} left"))
        }
        Ok(CallOutcome::Failure(other)) => {
            Err(fail("CHECKOUT_FAILED", alloc::format!("{other:?}")))
        }
        Err(CallError::Host(failure)) => Err(failure),
    }
}

pub static CONSUMER_APP: App = App::new(&[&INVENTORY, &CHECKOUT]);
// ANCHOR_END: consumer

// ANCHOR: exports
/// Without an interface, exports are flags on the declarations: other
/// modules may read this collection...
#[collection(name = "prices", key = String, value = u64, export)]
pub struct Prices;

/// ...and only `shop` and `reports` may call this query (`export`
/// alone would admit every module).
#[query(callers("shop", "reports"))]
pub fn price(ctx: &mut QueryCtx<'_, '_>, sku: String) -> Result<u64> {
    Ok(Prices::get(ctx, &sku)?.unwrap_or(0))
}
// ANCHOR_END: exports
