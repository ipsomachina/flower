//! A named SQL plan is a component; an ordinary query chooses its public API.
extern crate alloc;

#[flower_sdk::application]
mod shop {

    use alloc::{string::String, vec::Vec};
    use flower_sdk::{SqlBudget, SqlEndpoint, SqlQuery, Value, prelude::*, typed::DecimalI64};

    #[derive(FlowerType)]
    pub struct Order {
        pub owner: String,
        pub status: String,
        pub total: DecimalI64,
    }

    #[collection(name = "orders", key = String, value = Order,
             index(name = "by_owner_status", fields(owner, status)))]
    pub struct Orders;

    // One capability enables many request-time statements over this collection.
    #[component]
    static SQL_ENDPOINT: SqlEndpoint = SqlEndpoint::new(
        "orders.sql",
        &[Orders::NAME],
        SqlBudget::scan(1_000, 1_048_576, 1_000_000),
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

    #[derive(FlowerType)]
    pub struct OpenOrder {
        pub id: String,
        pub total: DecimalI64,
    }

    #[derive(FlowerType)]
    pub struct OpenOrdersParams {
        pub owner: String,
        pub min_total: DecimalI64,
        pub limit: u32,
    }

    #[component]
    static OPEN_ORDERS_PLAN: SqlQuery<OpenOrdersParams, OpenOrder> = SqlQuery::new(
        "orders.open",
        "SELECT _key AS id, total FROM orders
     WHERE owner = :owner AND status = 'open' AND total >= :min_total
     ORDER BY total DESC, _key ASC LIMIT :limit",
        SqlBudget::scan(1_000, 1_048_576, 1_000_000),
    )
    .require_index();

    #[query(http)]
    pub fn open_orders(
        ctx: &mut QueryCtx<'_, '_>,
        owner: String,
        min_total: DecimalI64,
        limit: u32,
    ) -> Result<Vec<OpenOrder>> {
        OPEN_ORDERS_PLAN.run(
            ctx,
            &OpenOrdersParams {
                owner,
                min_total,
                limit,
            },
        )
    }

    #[query]
    pub fn explain_open_orders(ctx: &mut QueryCtx<'_, '_>) -> Result<Value> {
        OPEN_ORDERS_PLAN.explain(ctx)
    }

    #[mutation(http)]
    pub fn put_order(ctx: &mut MutationCtx<'_, '_>, id: String, order: Order) -> Result<()> {
        Orders::set(ctx, &id, &order)
    }
}
flower_sdk::export!(shop::APP);
