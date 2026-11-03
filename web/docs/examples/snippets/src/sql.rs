// ANCHOR: application
use alloc::{string::String, vec::Vec};
use flower_sdk::typed::DecimalI64;
use flower_sdk::{SqlBudget, SqlEndpoint, SqlQuery, Value, prelude::*};

#[flower_sdk::application]
pub mod shop {
    use super::*;

    #[derive(FlowerType)]
    pub struct Order {
        pub owner: String,
        pub total: DecimalI64,
    }

    #[collection(name = "orders", key = String, value = Order,
                 index(name = "by_owner", fields(owner)))]
    pub struct Orders;

    #[component]
    static SQL_ENDPOINT: SqlEndpoint = SqlEndpoint::new(
        "orders.sql",
        &[Orders::NAME],
        SqlBudget::new(1_000, 1_048_576, 1_000_000),
    );

    #[query(http)]
    pub fn sql(ctx: &mut QueryCtx<'_, '_>, statement: String, params: Value) -> Result<Vec<Value>> {
        SQL_ENDPOINT.run(ctx, &statement, &params)
    }

    #[query(http)]
    pub fn sql_explain(
        ctx: &mut QueryCtx<'_, '_>,
        statement: String,
        params: Value,
    ) -> Result<Value> {
        SQL_ENDPOINT.explain(ctx, &statement, &params)
    }

    #[mutation(http)]
    pub fn put_order(ctx: &mut MutationCtx<'_, '_>, id: String, order: Order) -> Result<()> {
        Orders::set(ctx, &id, &order)
    }

    // ANCHOR: named-plan
    #[derive(FlowerType)]
    pub struct OrdersParams {
        pub owner: String,
        pub limit: u32,
    }

    #[derive(FlowerType)]
    pub struct OrderRow {
        pub id: String,
        pub total: DecimalI64,
    }

    #[component]
    static BY_OWNER: SqlQuery<OrdersParams, OrderRow> = SqlQuery::new(
        "orders.by_owner",
        "SELECT _key AS id, total FROM orders WHERE owner = :owner
         ORDER BY total DESC LIMIT :limit",
        SqlBudget::new(1_000, 1_048_576, 1_000_000),
    )
    .require_index();

    #[query(http)]
    pub fn orders_by_owner(
        ctx: &mut QueryCtx<'_, '_>,
        owner: String,
        limit: u32,
    ) -> Result<Vec<OrderRow>> {
        BY_OWNER.run(ctx, &OrdersParams { owner, limit })
    }
    // ANCHOR_END: named-plan
}
// In the guest crate's root: flower_sdk::export!(shop::APP);
// ANCHOR_END: application
