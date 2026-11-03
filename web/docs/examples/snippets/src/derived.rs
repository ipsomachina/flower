//! Concepts › Derived values and aggregates.

use alloc::string::String;

use flower_sdk::features::{TypedTask, TypedTrigger};
use flower_sdk::index::TypedAggregate;
use flower_sdk::{
    App, Derived, DerivedCtx, FlowerType, Materialize, MutationCtx, QueryCtx, ReadCtx, Result,
    Task, Trigger, Value, collection, derived, mutation, query,
};

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Order {
    pub customer: String,
    /// Whole cents: aggregates need exact, order-independent arithmetic.
    pub cents: u64,
    pub shipped: bool,
}

#[collection(
    name = "orders",
    key = String,
    value = Order,
    index(name = "by_customer", fields(customer), types(String)),
)]
pub struct Orders;

/// Writes an order: the example's writer.
#[mutation(http)]
pub fn put_order(ctx: &mut MutationCtx<'_, '_>, id: String, order: Order) -> Result<()> {
    Orders::set(ctx, &id, &order)
}

// ANCHOR: derived
/// How many of a customer's orders have not shipped yet. The engine
/// records the index range this reads, caches the result per customer
/// and drops it when an order in that range changes.
#[derived]
pub fn unshipped(ctx: &mut DerivedCtx<'_, '_>, customer: String) -> Result<u64> {
    let page = Orders::BY_CUSTOMER.prefix((customer,)).page(ctx, 1_000)?;
    Ok(page.rows.iter().filter(|row| !row.value.shipped).count() as u64)
}

/// Read it from a query (or another derived value) of the same module,
/// `""`, with the argument struct `#[derived]` generated.
#[query(http)]
pub fn customer_summary(ctx: &mut QueryCtx<'_, '_>, customer: String) -> Result<u64> {
    let args = UnshippedArgs { customer }.encode()?;
    u64::decode(ctx.get_derived("", UNSHIPPED.name, &args)?)
}
// ANCHOR_END: derived

// ANCHOR: aggregate
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Totals {
    pub orders: u64,
    pub cents: u64,
}

/// Order count and value per customer, kept by deltas: a write folds
/// the changed order's old image out and its new one in, without
/// scanning the customer's other orders.
pub struct CustomerTotals;

impl TypedAggregate for CustomerTotals {
    type Source = Orders;
    type Fields = (String,);
    type Group = (String,);
    type Accumulator = Totals;
    const NAME: &'static str = "customer_totals";
    const INDEX: &'static str = "by_customer";
    /// Keep cached groups across deployments that keep this version.
    const VERSION: Option<&'static str> = Some("1");

    fn initial(_customer: &(String,)) -> Result<Totals> {
        Ok(Totals {
            orders: 0,
            cents: 0,
        })
    }

    fn add(totals: Totals, _key: &String, order: &Order, _: &(String,)) -> Result<Totals> {
        Ok(Totals {
            orders: totals.orders + 1,
            cents: totals.cents + order.cents,
        })
    }

    fn remove(
        totals: Totals,
        _key: &String,
        order: &Order,
        _: &(String,),
    ) -> Result<Option<Totals>> {
        Ok(Some(Totals {
            orders: totals.orders - 1,
            cents: totals.cents - order.cents,
        }))
    }
}

/// Every group stays maintained, so no read ever has to build one.
pub static CUSTOMER_TOTALS: Derived = CustomerTotals::DERIVED.materialize(Materialize::Always);

#[query(http)]
pub fn totals(ctx: &mut QueryCtx<'_, '_>, customer: String) -> Result<Totals> {
    CustomerTotals::get(ctx, &(customer,))
}
// ANCHOR_END: aggregate

#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Shipment {
    pub order: String,
    pub at: f64,
}

#[collection(name = "shipments", key = String, value = Shipment)]
pub struct Shipments;

// ANCHOR: trigger
/// Records a shipment whenever an order flips to shipped, inside the
/// same mutation as the write that flipped it.
pub struct RecordShipments;

impl TypedTrigger for RecordShipments {
    type Source = Orders;
    const NAME: &'static str = "record_shipments";

    fn run(
        ctx: &mut MutationCtx<'_, '_>,
        key: String,
        before: Option<Order>,
        after: Option<Order>,
    ) -> Result<()> {
        let was = before.is_some_and(|order| order.shipped);
        let is = after.is_some_and(|order| order.shipped);
        if is && !was {
            let at = ctx.clock()?;
            let shipment = Shipment {
                order: key.clone(),
                at,
            };
            Shipments::set(ctx, &key, &shipment)?;
        }
        Ok(())
    }
}

pub static RECORD_SHIPMENTS: Trigger = RecordShipments::TRIGGER;
// ANCHOR_END: trigger

// ANCHOR: task
/// Deletes shipment records once they are a day old, a page per run.
pub struct ForgetShipments;

const DAY_MS: f64 = 86_400_000.0;

impl TypedTask for ForgetShipments {
    const NAME: &'static str = "forget_shipments";

    /// The earliest moment there is work: a day after the oldest
    /// shipment on this page, or never.
    fn due(ctx: &mut QueryCtx<'_, '_>) -> Result<Option<f64>> {
        let page = Shipments::page(ctx, 100, None)?;
        Ok(page
            .rows
            .iter()
            .map(|row| row.value.at + DAY_MS)
            .reduce(f64::min))
    }

    /// One bounded unit of work; the task stays due while `due` is past.
    fn run(ctx: &mut MutationCtx<'_, '_>) -> Result<Value> {
        let now = ctx.clock()?;
        let page = Shipments::page(ctx, 100, None)?;
        let mut forgotten = 0.0;
        for row in page.rows {
            if row.value.at + DAY_MS <= now {
                Shipments::delete(ctx, &row.key)?;
                forgotten += 1.0;
            }
        }
        Ok(Value::Number(forgotten))
    }
}

pub static FORGET_SHIPMENTS: Task = ForgetShipments::TASK;
// ANCHOR_END: task

// ANCHOR: app
/// Derived values, aggregates, triggers and tasks are components like
/// the rest.
pub static APP: App = App::new(&[
    &Orders,
    &Shipments,
    &PUT_ORDER,
    &UNSHIPPED,
    &CUSTOMER_TOTALS,
    &RECORD_SHIPMENTS,
    &FORGET_SHIPMENTS,
    &CUSTOMER_SUMMARY,
    &TOTALS,
]);
// ANCHOR_END: app
