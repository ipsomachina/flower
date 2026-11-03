//! Goblin Pizza, ported from v1's `examples/goblin-pizza-rs`: orders
//! counted by a reversible aggregate, oven timers, and deliveries on a
//! scoped, leased queue worked by drones. Money is whole copper coins.
//!
//! - `pizza.setup` opens the kitchens: tenants, stores per tenant,
//!   starting stock, bake time, and the drones' lease.
//! - `pizza.order` takes an order if the store has dough, and starts its
//!   oven timer; the timer runs `pizza.finishBaking` (off HTTP: only the
//!   timer calls it), which marks the
//!   pizza ready and enqueues its delivery in the same commit, so no
//!   ready pizza is ever left without one. Each tenant's deliveries are
//!   jobs in that tenant's scope of the `deliveries` queue (v1's
//!   `deliveries.scope(tenant)`), named by the order, so a delivery is
//!   queued at most once per order.
//! - Drones are `flower-worker` workers, one per tenant scope, holding
//!   fenced leases. A drone calls `pizza.deliver` with its lease: the
//!   delivery renews that lease in the paying transaction, so a drone
//!   whose lease ran out or was taken over cannot collect coins (v1
//!   validated the lease in `pizza.deliver` the same way), and a second
//!   delivery of a delivered pizza pays nothing. The worker then
//!   completes the job, claiming its next deliveries in the same commit.
//! - Each store's order counts (orders, baking, ready, delivered, and
//!   pizzas ordered and delivered) are `pizza.orderStats`, an aggregate
//!   over the orders' `byShop` index (v1's reversible reducers): an
//!   order's change folds its old image out and its new one in, so a
//!   count never scans the store's orders again. The store row keeps
//!   stock, revenue and tips.
//! - `pizza.tip` tips a kitchen; `pizza.shop` and `pizza.leaderboard`
//!   read summaries (the store row with its order counts), ranked by
//!   revenue plus tips. `pizza.shop.local` reads a store from the
//!   serving replica's own state, which may lag (v1's
//!   `pizza.shop.local`), and `pizza.world` pages the source records
//!   for an independent audit (v1's `pizza.world`); both serve the
//!   `flower-bench` stress benchmark.
//!
//! **The authenticated kitchen** (feature `auth`, v1's
//! `goblin-pizza-auth.ts`; v1's Rust port had no access rules): every
//! method sits behind an access rule, decided by the module's
//! authorization hook before the call runs. Callers present an EdDSA
//! bearer JWT (issuer and audience `goblin-pizza`) in the call's
//! credentials; the hook verifies it natively against the public key
//! `pizza.setup` installed. Methods that name a tenant (`pizza.order`,
//! `pizza.tip`, `pizza.deliver`, `pizza.shop`, `pizza.shop.local`,
//! `pizza.leaderboard`) admit only a principal whose `tenant` claim is
//! that tenant; `pizza.setup` and `pizza.world`, which see every tenant,
//! need `role: "admin"`, which no tenant token carries. Others are
//! refused `FORBIDDEN`, and a token that does not verify (another key,
//! a rewritten claim, wrong issuer or audience, expired)
//! `UNAUTHENTICATED`. The key is installed by `pizza.setup`, which only
//! a trusted gateway (the holder of the server's admin token, asserting
//! an admin principal) can call before a key exists: so only the
//! deployer decides whose tokens the kitchen accepts, as v1 compiled
//! the key into its bundle. Timer fires (the oven bell) run without a
//! caller and skip the hook.
//!
//! Every operation is one declaration (`#[mutation(name = ..., access =
//! ...)]`, `#[query(...)]`) over typed records, and the app is one list
//! of its parts. The wire contract is v1's: camelCase members, and the
//! store and tenant reads take a bare `[tenant, store]` or tenant.

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::time::Duration;

use flower_sdk::index::TypedAggregate;
use flower_sdk::queue::{EnqueueOptions, Job, Lease, TypedQueue};
use flower_sdk::timer::Timers;
use flower_sdk::{
    App, Consistency, Derived, FlowerType, Materialize, MutationCtx, Op, QueryCtx, ReadCtx, Result,
    collection, fail, mutation, query,
};

/// Copper coins per pizza.
pub const UNIT_PRICE: u64 = 7;
/// Longest drone lease, and the default (v1's `MAX_LEASE_MS`).
pub const MAX_LEASE_MS: u64 = 60_000;

/// A store: `[tenant, store]`.
pub type ShopId = (String, String);
/// An order: `[tenant, store, id]`.
pub type OrderId = (String, String, String);

/// The kitchens' settings, kept under the key `"world"`.
#[derive(FlowerType, Clone, Debug, PartialEq)]
#[flower(rename_all = "camelCase")]
pub struct Config {
    pub tenants: Vec<String>,
    pub shop_ids: Vec<ShopId>,
    pub bake_ms: u64,
    pub lease_ms: u64,
    pub unit_price: u64,
    /// The authenticated kitchen's trusted key: an Ed25519 public key as
    /// hex SPKI DER. Only the setup installs it; the unauthenticated
    /// build keeps it unused.
    #[flower(default)]
    pub auth_key: Option<String>,
}

/// A store row: stock, revenue and tips. Its order counts are the
/// `pizza.orderStats` aggregate's.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Shop {
    pub id: ShopId,
    pub key: String,
    pub name: String,
    pub stock: u64,
    pub revenue: u64,
    pub tips: u64,
}

/// One pizza order.
#[derive(FlowerType, Clone, Debug, PartialEq)]
#[flower(rename_all = "camelCase")]
pub struct Order {
    pub id: String,
    pub key: String,
    pub shop: ShopId,
    pub quantity: u64,
    /// `baking`, `ready` or `delivered`.
    pub status: String,
    pub created_at: f64,
    pub due_at: f64,
    #[flower(default)]
    pub ready_at: Option<f64>,
    #[flower(default)]
    pub delivered_at: Option<f64>,
}

/// One store's order counts: orders, and how many are baking, ready
/// and delivered, plus pizzas ordered and delivered.
#[derive(FlowerType, Clone, Debug, Default, PartialEq)]
#[flower(rename_all = "camelCase")]
pub struct Stats {
    pub orders: i64,
    pub baking: i64,
    pub ready: i64,
    pub delivered: i64,
    pub ordered_quantity: i64,
    pub delivered_quantity: i64,
}

/// A store's summary: its row with its order counts.
#[derive(FlowerType, Clone, Debug, PartialEq)]
#[flower(rename_all = "camelCase")]
pub struct Summary {
    pub id: ShopId,
    pub key: String,
    pub name: String,
    pub stock: u64,
    pub revenue: u64,
    pub tips: u64,
    pub orders: i64,
    pub baking: i64,
    pub ready: i64,
    pub delivered: i64,
    pub ordered_quantity: i64,
    pub delivered_quantity: i64,
}

/// A delivery job's payload.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Delivery {
    pub order: OrderId,
}

/// The source records an audit reconciles.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct World {
    pub config: Config,
    pub shops: Vec<Summary>,
    pub orders: Vec<Order>,
    /// The next page's cursor; null after the last.
    pub next: Option<String>,
}

#[collection(name = "pizza.config", key = String, value = Config)]
pub struct Settings;

#[collection(name = "pizza.shops", key = ShopId, value = Shop)]
pub struct Shops;

#[collection(
    name = "pizza.orders",
    key = OrderId,
    value = Order,
    index(name = "byShop", fields(shop), types(ShopId)),
)]
pub struct Orders;

/// The queue drones claim from: one scope per tenant. The host refuses
/// any payload but a [`Delivery`].
pub static DELIVERIES: TypedQueue<Delivery> = TypedQueue::new("deliveries");

/// One store's orders by status, and pizzas ordered and delivered:
/// each order's change takes its old image out and folds its new one
/// in, so no count ever scans the store's orders again (v1's
/// `pizza.orderStats`).
pub struct OrderStats;

impl TypedAggregate for OrderStats {
    type Source = Orders;
    type Fields = (ShopId,);
    type Group = (ShopId,);
    type Accumulator = Stats;
    const NAME: &'static str = "pizza.orderStats";
    const INDEX: &'static str = "byShop";
    const VERSION: Option<&'static str> = Some("1");

    fn initial(_shop: &(ShopId,)) -> Result<Stats> {
        Ok(Stats::default())
    }

    fn add(stats: Stats, _key: &OrderId, order: &Order, _shop: &(ShopId,)) -> Result<Stats> {
        Ok(adjust(stats, order, 1))
    }

    fn remove(
        stats: Stats,
        _key: &OrderId,
        order: &Order,
        _shop: &(ShopId,),
    ) -> Result<Option<Stats>> {
        Ok(Some(adjust(stats, order, -1)))
    }
}

/// Every group stays maintained, so a read never builds one again.
static ORDER_STATS: Derived = OrderStats::DERIVED.materialize(Materialize::Always);

/// Fold an order into a store's counts (`direction` 1) or take it out
/// (-1).
fn adjust(mut stats: Stats, order: &Order, direction: i64) -> Stats {
    let is = |state: &str| i64::from(order.status == state);
    let quantity = order.quantity as i64;
    stats.orders += direction;
    stats.baking += direction * is("baking");
    stats.ready += direction * is("ready");
    stats.delivered += direction * is("delivered");
    stats.ordered_quantity += direction * quantity;
    stats.delivered_quantity += direction * is("delivered") * quantity;
    stats
}

const NAMES: [&str; 6] = [
    "The Crispy Cauldron",
    "Mushroom Mayhem",
    "The Sizzling Slime",
    "Dough or Die",
    "The Goblin's Slice",
    "Dragon Breath Delivery",
];

const WORLD_KEY: &str = "world";

/// Lowercase letters, digits, `-` and `_`, 1 to 96 of them.
fn identifier(text: &str) -> bool {
    (1..=96).contains(&text.len())
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn check_shop(shop: &ShopId) -> Result<()> {
    if identifier(&shop.0) && identifier(&shop.1) {
        Ok(())
    } else {
        Err(fail("INVALID_ARGUMENT", "a store is [tenant, store]"))
    }
}

/// The canonical text of an ID: rows keep it, deliveries are named by
/// it and timers keyed by it.
fn canonical(id: &impl FlowerType) -> Result<String> {
    id.encode()?.canonical_json()
}

fn config(ctx: &mut impl ReadCtx) -> Result<Config> {
    Settings::get(ctx, &WORLD_KEY.to_string())?
        .ok_or_else(|| fail("NOT_INITIALIZED", "The goblin kitchens are not open yet"))
}

fn shop(ctx: &mut impl ReadCtx, id: &ShopId) -> Result<Shop> {
    Shops::get(ctx, id)?.ok_or_else(|| fail("SHOP_NOT_FOUND", "No goblin kitchen by that name"))
}

/// A store's summary: its row with its order counts.
fn summary(ctx: &mut impl ReadCtx, id: &ShopId) -> Result<Summary> {
    let shop = shop(ctx, id)?;
    let stats = OrderStats::get(ctx, &(id.clone(),))?;
    Ok(Summary {
        id: shop.id,
        key: shop.key,
        name: shop.name,
        stock: shop.stock,
        revenue: shop.revenue,
        tips: shop.tips,
        orders: stats.orders,
        baking: stats.baking,
        ready: stats.ready,
        delivered: stats.delivered,
        ordered_quantity: stats.ordered_quantity,
        delivered_quantity: stats.delivered_quantity,
    })
}

#[cfg_attr(
    not(feature = "auth"),
    mutation(name = "pizza.setup", http, rename_all = "camelCase")
)]
#[cfg_attr(
    feature = "auth",
    mutation(name = "pizza.setup", http, rename_all = "camelCase", access = kitchen_auth::admin)
)]
fn setup(
    ctx: &mut MutationCtx<'_, '_>,
    tenants: Vec<String>,
    stores_per_tenant: u64,
    stock_per_shop: u64,
    bake_ms: u64,
    lease_ms: Option<u64>,
    auth_key: Option<String>,
) -> Result<Config> {
    for (index, tenant) in tenants.iter().enumerate() {
        if !identifier(tenant) || tenants[..index].contains(tenant) {
            return Err(fail(
                "INVALID_ARGUMENT",
                "Tenant IDs must be distinct identifiers",
            ));
        }
    }
    let lease_ms = lease_ms.unwrap_or(MAX_LEASE_MS);
    if !(1..=64).contains(&stores_per_tenant)
        || stock_per_shop > 1_000_000
        || bake_ms > 3_600_000
        || !(1..=MAX_LEASE_MS).contains(&lease_ms)
    {
        return Err(fail(
            "INVALID_ARGUMENT",
            "stores, stock, bake time or lease out of range",
        ));
    }
    if auth_key.as_ref().is_some_and(|hex| {
        !(2..=512).contains(&hex.len()) || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
    }) {
        return Err(fail(
            "INVALID_ARGUMENT",
            "authKey is an Ed25519 public key as hex SPKI DER",
        ));
    }
    let world = WORLD_KEY.to_string();
    if Settings::get(ctx, &world)?.is_some() {
        return Err(fail(
            "ALREADY_INITIALIZED",
            "The kitchens are already open; use a fresh database for another run",
        ));
    }
    let mut shop_ids = Vec::new();
    for tenant in &tenants {
        for index in 0..stores_per_tenant {
            shop_ids.push((tenant.clone(), format!("store-{index}")));
        }
    }
    let settings = Config {
        tenants,
        shop_ids: shop_ids.clone(),
        bake_ms,
        lease_ms,
        unit_price: UNIT_PRICE,
        auth_key,
    };
    Settings::set(ctx, &world, &settings)?;
    for (index, id) in shop_ids.into_iter().enumerate() {
        let row = Shop {
            key: canonical(&id)?,
            name: format!("{} · {}", NAMES[index % NAMES.len()], id.1),
            id,
            stock: stock_per_shop,
            revenue: 0,
            tips: 0,
        };
        Shops::set(ctx, &row.id.clone(), &row)?;
    }
    Ok(settings)
}

/// Take an order if the store has dough, and start its oven timer.
#[mutation(name = "pizza.order", http)]
fn order(ctx: &mut MutationCtx<'_, '_>, id: String, shop: ShopId, quantity: u64) -> Result<Order> {
    if !identifier(&id) {
        return Err(fail("INVALID_ARGUMENT", "order IDs are identifiers"));
    }
    check_shop(&shop)?;
    if !(1..=4).contains(&quantity) {
        return Err(fail("INVALID_ARGUMENT", "one to four pizzas"));
    }
    let bake_ms = config(ctx)?.bake_ms;
    let mut store = self::shop(ctx, &shop)?;
    let order_id: OrderId = (shop.0.clone(), shop.1.clone(), id.clone());
    if Orders::get(ctx, &order_id)?.is_some() {
        return Err(fail(
            "ORDER_EXISTS",
            "That pizza order already exists in this store",
        ));
    }
    if store.stock < quantity {
        return Err(fail(
            "OUT_OF_STOCK",
            "The goblins have run out of enchanted dough",
        ));
    }
    let now = ctx.now()?;
    // The order counts follow the order row through `pizza.orderStats`.
    store.stock -= quantity;
    Shops::set(ctx, &shop, &store)?;
    let order = Order {
        id,
        key: canonical(&order_id)?,
        shop,
        quantity,
        status: "baking".to_string(),
        created_at: now,
        due_at: now + bake_ms as f64,
        ready_at: None,
        delivered_at: None,
    };
    Orders::set(ctx, &order_id, &order)?;
    // The oven bell, named by the order: a second order could not ring
    // it twice.
    Timers::after(
        ctx,
        &format!("bake:{}", order.key),
        Duration::from_millis(bake_ms),
        &FINISH_BAKING,
        &FinishBakingArgs { order: order_id },
    )?;
    Ok(order)
}

/// The oven bell: the pizza is ready and its delivery is queued in its
/// tenant's scope, in one commit. Only its timer rings it.
#[mutation(name = "pizza.finishBaking")]
fn finish_baking(ctx: &mut MutationCtx<'_, '_>, order: OrderId) -> Result<()> {
    let Some(mut row) = Orders::get(ctx, &order)? else {
        return Err(fail("ORDER_NOT_FOUND", "The oven lost its order"));
    };
    if row.status != "baking" {
        return Ok(());
    }
    row.status = "ready".to_string();
    row.ready_at = Some(ctx.now()?);
    Orders::set(ctx, &order, &row)?;
    // Named by the order: a second bell could not queue it twice.
    let options = EnqueueOptions::default().scope(&order.0).named(&row.key);
    DELIVERIES.enqueue(ctx, &Delivery { order }, &options)?;
    Ok(())
}

/// A drone hands over a ready pizza under its lease of the delivery
/// (`{order, id, token}`): the store is paid. The lease is renewed in
/// the same transaction, which fails the delivery (`LEASE_LOST`) once
/// the lease ran out, was taken over or settled: a stale drone cannot
/// collect coins. Delivering a pizza twice pays once, so a redelivered
/// job is harmless.
#[mutation(name = "pizza.deliver", http)]
fn deliver(
    ctx: &mut MutationCtx<'_, '_>,
    order: OrderId,
    id: String,
    token: String,
) -> Result<Order> {
    if id.is_empty() || token.is_empty() {
        return Err(fail("INVALID_ARGUMENT", "a delivery carries its lease"));
    }
    let Some(mut row) = Orders::get(ctx, &order)? else {
        return Err(fail("ORDER_NOT_FOUND", "This drone has no pizza"));
    };
    // The lease must be this order's delivery, in its tenant's scope.
    let job = DELIVERIES.get(
        ctx,
        Job::Named {
            scope: &order.0,
            name: &row.key,
        },
    )?;
    if job.is_none_or(|job| job.id != id) {
        return Err(fail("LEASE_LOST", "This drone carries another order"));
    }
    let lease_ms = config(ctx)?.lease_ms;
    match row.status.as_str() {
        "delivered" => {}
        "ready" => {
            let mut store = shop(ctx, &row.shop)?;
            store.revenue += row.quantity * UNIT_PRICE;
            Shops::set(ctx, &row.shop, &store)?;
            row.status = "delivered".to_string();
            row.delivered_at = Some(ctx.now()?);
            Orders::set(ctx, &order, &row)?;
        }
        _ => {
            return Err(fail(
                "ORDER_NOT_READY",
                "Only a ready pizza can be delivered",
            ));
        }
    }
    // The fence, last: a refusal rolls the payment back with the rest.
    let renewed = DELIVERIES.renew(ctx, &[Lease { id, token }], lease_ms)?;
    if renewed.first().is_none_or(Option::is_none) {
        return Err(fail(
            "LEASE_LOST",
            "This drone's lease ran out or was taken over",
        ));
    }
    Ok(row)
}

/// Tip a kitchen: its tips so far.
#[mutation(name = "pizza.tip", http)]
fn tip(ctx: &mut MutationCtx<'_, '_>, shop: ShopId, amount: u64) -> Result<u64> {
    check_shop(&shop)?;
    if !(1..=1_000_000).contains(&amount) {
        return Err(fail("INVALID_ARGUMENT", "tips are 1 to 1,000,000 coins"));
    }
    let mut store = self::shop(ctx, &shop)?;
    store.tips += amount;
    Shops::set(ctx, &shop, &store)?;
    Ok(store.tips)
}

/// A store's summary.
#[query(name = "pizza.shop", http, bare_arg)]
fn shop_summary(ctx: &mut QueryCtx<'_, '_>, shop: ShopId) -> Result<Summary> {
    check_shop(&shop)?;
    summary(ctx, &shop)
}

/// The same store, read from the serving replica's own applied state:
/// no round to the leader, so it may lag. Browsing only; stock checks
/// and money stay in mutations.
static SHOP_LOCAL: Op<ShopSummaryArgs, Summary> = SHOP_SUMMARY
    .named("pizza.shop.local")
    .consistency(Consistency::ReplicaLocal);

/// Most orders one `pizza.world` page returns.
pub const WORLD_PAGE: u32 = 500;

/// The source records an audit reconciles: the configuration, every
/// store's summary (its row with the order counts the aggregate keeps,
/// which the audit recounts from the orders) and one page of orders in
/// key order, with the cursor of the next page (`null` after the last).
/// Pages are separate reads, so a consistent audit pages a quiescent
/// kitchen.
#[cfg_attr(not(feature = "auth"), query(name = "pizza.world", http))]
#[cfg_attr(
    feature = "auth",
    query(name = "pizza.world", http, access = kitchen_auth::admin)
)]
fn world(ctx: &mut QueryCtx<'_, '_>, after: Option<String>, limit: Option<u32>) -> Result<World> {
    let limit = limit.unwrap_or(WORLD_PAGE).clamp(1, WORLD_PAGE);
    let settings = config(ctx)?;
    let mut shops = Vec::new();
    let mut cursor = None;
    loop {
        let page = Shops::page(ctx, 256, cursor.as_deref())?;
        for row in page.rows {
            shops.push(summary(ctx, &row.key)?);
        }
        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let page = Orders::page(ctx, limit, after.as_deref())?;
    Ok(World {
        config: settings,
        shops,
        orders: page.rows.into_iter().map(|row| row.value).collect(),
        next: page.cursor,
    })
}

/// One tenant's stores, by revenue plus tips, ties by key.
#[query(name = "pizza.leaderboard", http, bare_arg)]
fn leaderboard(ctx: &mut QueryCtx<'_, '_>, tenant: String) -> Result<Vec<Summary>> {
    let ids = config(ctx)?.shop_ids;
    let mut board = Vec::new();
    for id in ids.iter().filter(|id| id.0 == tenant) {
        board.push(summary(ctx, id)?);
    }
    board.sort_by(|a, b| {
        (b.revenue + b.tips)
            .cmp(&(a.revenue + a.tips))
            .then_with(|| a.key.cmp(&b.key))
    });
    Ok(board)
}

/// The authenticated kitchen's authorization hook and access rules.
#[cfg(feature = "auth")]
pub mod kitchen_auth {
    use alloc::vec;

    use flower_sdk::QueryCtx;
    use flower_sdk::auth::{Access, AuthRequest, Authentication, Authorization, Principal};
    use flower_sdk::crypto::hex_decode;
    use flower_sdk::crypto::jwt::{Algorithm, JwtKey, KeyFormat, ValidationOptions, VerifyOptions};

    use super::{Result, String, ToString, config};
    use flower_sdk::{Value, fail};

    /// Issuer and audience every kitchen token names.
    pub const ISSUER: &str = "goblin-pizza";

    /// Verify the bearer JWT in the call's credentials against the key
    /// `pizza.setup` installed. A principal a trusted gateway asserted
    /// (only the holder of the server's admin token can, over HTTP)
    /// stands as is: that is how the deployer opens the kitchens before
    /// any key exists. No credentials is an anonymous caller, whom every
    /// rule refuses.
    fn authenticate(
        ctx: &mut QueryCtx<'_, '_>,
        credentials: &Value,
        request: &AuthRequest,
    ) -> Result<Option<Principal>> {
        if let Some(asserted) = &request.principal {
            return Ok(Some(asserted.clone()));
        }
        let Some(Value::String(token)) = credentials.as_object().and_then(|c| c.get("bearer"))
        else {
            return Ok(None);
        };
        let settings =
            config(ctx).map_err(|_| fail("UNAUTHENTICATED", "the kitchens trust no key yet"))?;
        let Some(key) = settings.auth_key else {
            return Err(fail("UNAUTHENTICATED", "the kitchens trust no key"));
        };
        let der = hex_decode(&key)?;
        let options = VerifyOptions {
            algorithms: Some(vec![Algorithm::EdDsa]),
            validation: ValidationOptions {
                issuer: Some(ISSUER.to_string()),
                audience: Some(vec![ISSUER.to_string()]),
                ..ValidationOptions::default()
            },
        };
        let verified = ctx
            .crypto()
            .jwt()
            .verify(token, JwtKey::Bytes(&der, KeyFormat::Der), &options)
            .map_err(|failure| fail("UNAUTHENTICATED", failure.message))?;
        let claims = verified.claims.as_object().cloned().unwrap_or_default();
        let Some(Value::String(subject)) = claims.get("sub") else {
            return Err(fail("UNAUTHENTICATED", "tokens name a subject"));
        };
        let tenant = match claims.get("tenant") {
            Some(Value::String(tenant)) => Some(tenant.clone()),
            _ => None,
        };
        let mut kept = alloc::collections::BTreeMap::new();
        if let Some(role) = claims.get("role") {
            kept.insert(String::from("role"), role.clone());
        }
        Principal::new(subject.clone(), tenant, Some(Value::Object(kept))).map(Some)
    }

    /// The tenant a call's arguments name: a leaderboard's tenant, a
    /// store `[tenant, store]`, an order's or tip's `shop`, a delivery's
    /// `order` `[tenant, store, id]`.
    pub fn named_tenant(args: &Value) -> Option<&str> {
        fn first(parts: &[Value]) -> Option<&str> {
            match parts.first() {
                Some(Value::String(tenant)) => Some(tenant.as_str()),
                _ => None,
            }
        }
        match args {
            Value::String(tenant) => Some(tenant),
            Value::Array(parts) => first(parts),
            Value::Object(fields) => match (fields.get("shop"), fields.get("order")) {
                (Some(Value::Array(parts)), _) | (None, Some(Value::Array(parts))) => first(parts),
                _ => None,
            },
            _ => None,
        }
    }

    /// Whether a principal is the kitchens' administrator.
    pub fn is_admin(principal: Option<&Principal>) -> bool {
        matches!(
            principal.and_then(|principal| principal.claims.as_ref()),
            Some(Value::Object(claims))
                if claims.get("role") == Some(&Value::String("admin".to_string()))
        )
    }

    /// Only the tenant a call names may make it.
    fn owns(_: &mut QueryCtx<'_, '_>, principal: Option<&Principal>, args: &Value) -> Result<bool> {
        Ok(
            match (
                principal.and_then(|p| p.tenant.as_deref()),
                named_tenant(args),
            ) {
                (Some(caller), Some(named)) => caller == named,
                _ => false,
            },
        )
    }

    /// Methods that see every tenant (`pizza.setup`, `pizza.world`): the
    /// administrator only.
    pub fn admin(
        _: &mut QueryCtx<'_, '_>,
        principal: Option<&Principal>,
        _: &Value,
    ) -> Result<bool> {
        Ok(is_admin(principal))
    }

    /// Every other method admits only the tenant it names.
    pub static AUTHORIZATION: Authorization = Authorization {
        authenticate: Authentication::Callback(authenticate),
        default: Access::Check(owns),
        sessions: Access::Check(owns),
        methods: &[],
    };

    #[cfg(test)]
    mod tests {
        use alloc::string::String;
        use alloc::vec;

        use super::*;

        fn s(text: &str) -> Value {
            Value::String(String::from(text))
        }

        fn object(pairs: &[(&str, Value)]) -> Value {
            Value::Object(
                pairs
                    .iter()
                    .map(|(name, value)| (String::from(*name), value.clone()))
                    .collect(),
            )
        }

        #[test]
        fn every_tenant_method_names_its_tenant() {
            let shop = Value::Array(vec![s("tenant-1"), s("store-0")]);
            assert_eq!(named_tenant(&s("tenant-1")), Some("tenant-1"));
            assert_eq!(named_tenant(&shop), Some("tenant-1"));
            assert_eq!(
                named_tenant(&object(&[("shop", shop.clone())])),
                Some("tenant-1")
            );
            let order = Value::Array(vec![s("tenant-2"), s("store-0"), s("pizza-000001")]);
            assert_eq!(named_tenant(&object(&[("order", order)])), Some("tenant-2"));
            assert_eq!(named_tenant(&Value::Null), None);
        }

        #[test]
        fn only_the_admin_role_is_admin() {
            let admin = Principal::new(
                String::from("goblin-admin"),
                None,
                Some(object(&[("role", s("admin"))])),
            )
            .unwrap();
            assert!(is_admin(Some(&admin)));
            let customer = Principal::new(
                String::from("customer@tenant-0"),
                Some(String::from("tenant-0")),
                Some(object(&[])),
            )
            .unwrap();
            assert!(!is_admin(Some(&customer)));
            assert!(!is_admin(None));
        }
    }
}

/// Everything the kitchens are made of, listed once.
#[cfg(not(feature = "auth"))]
static PARTS: [&dyn flower_sdk::Component; 14] = [
    &Settings,
    &Shops,
    &Orders,
    &DELIVERIES,
    &ORDER_STATS,
    &SETUP,
    &ORDER,
    &FINISH_BAKING,
    &DELIVER,
    &TIP,
    &SHOP_SUMMARY,
    &SHOP_LOCAL,
    &LEADERBOARD,
    &WORLD,
];
#[cfg(feature = "auth")]
static PARTS: [&dyn flower_sdk::Component; 15] = [
    &Settings,
    &Shops,
    &Orders,
    &DELIVERIES,
    &ORDER_STATS,
    &SETUP,
    &ORDER,
    &FINISH_BAKING,
    &DELIVER,
    &TIP,
    &SHOP_SUMMARY,
    &SHOP_LOCAL,
    &LEADERBOARD,
    &WORLD,
    &kitchen_auth::AUTHORIZATION,
];

pub static APP: App = App::new(&PARTS);

flower_sdk::export!(APP);

#[cfg(test)]
mod tests {
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;

    use flower_sdk::Value;
    use flower_sdk::abi::{InvocationKind, invoke_native, manifest_native};

    fn s(text: &str) -> Value {
        Value::String(String::from(text))
    }

    fn n(number: f64) -> Value {
        Value::Number(number)
    }

    fn object(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(name, value)| (String::from(*name), value.clone()))
                .collect(),
        )
    }

    fn shop() -> Value {
        Value::Array(vec![s("tenant-1"), s("store-0")])
    }

    /// The code a mutation fails with before it touches the host.
    fn refused(name: &str, args: Value) -> String {
        let input = Value::Array(vec![s(name), args]).encode_binary().unwrap();
        let output = invoke_native(&super::APP, InvocationKind::Mutation as u32, &input);
        let Value::Array(envelope) = Value::decode_binary(&output).unwrap() else {
            panic!("response envelope");
        };
        assert_eq!(envelope[0], s("error"), "{name} succeeded");
        match envelope[1].as_object().unwrap().get("code") {
            Some(Value::String(code)) => code.clone(),
            other => panic!("code not a string: {other:?}"),
        }
    }

    #[test]
    fn bad_arguments_answer_v1s_code() {
        let order = |pairs: &[(&str, Value)]| refused("pizza.order", object(pairs));
        // Not a whole number of pizzas, no order ID, an unknown member.
        for code in [
            order(&[("id", s("o1")), ("shop", shop()), ("quantity", n(2.5))]),
            order(&[("shop", shop()), ("quantity", n(2.0))]),
            order(&[
                ("id", s("o1")),
                ("shop", shop()),
                ("quantity", n(2.0)),
                ("extra", n(1.0)),
            ]),
            order(&[("id", s("o 1")), ("shop", shop()), ("quantity", n(2.0))]),
            order(&[("id", s("o1")), ("shop", shop()), ("quantity", n(5.0))]),
            refused("pizza.tip", object(&[("shop", shop()), ("amount", n(1.5))])),
            refused("pizza.tip", object(&[("shop", shop()), ("amount", n(0.0))])),
        ] {
            assert_eq!(code, "INVALID_ARGUMENT");
        }
        let setup = |stores: f64| {
            refused(
                "pizza.setup",
                object(&[
                    ("tenants", Value::Array(vec![s("tenant-1")])),
                    ("storesPerTenant", n(stores)),
                    ("stockPerShop", n(10.0)),
                    ("bakeMs", n(10.0)),
                ]),
            )
        };
        assert_eq!(setup(0.0), "INVALID_ARGUMENT");
        assert_eq!(setup(65.0), "INVALID_ARGUMENT");
        assert_eq!(setup(1.5), "INVALID_ARGUMENT");
    }

    /// Exactly what parity's `http` list exposed: the oven timer's
    /// handler stays internal.
    #[test]
    fn the_kitchen_answers_over_http_as_on_parity() {
        let bytes = manifest_native(&super::APP);
        let Value::Array(envelope) = Value::decode_binary(&bytes).unwrap() else {
            panic!("manifest envelope");
        };
        let Some(Value::Array(http)) = envelope[1].as_object().unwrap().get("http").cloned() else {
            panic!("manifest without http");
        };
        let mut aliases: Vec<String> = http
            .iter()
            .map(|entry| match entry.as_object().unwrap().get("alias") {
                Some(Value::String(alias)) => alias.clone(),
                other => panic!("alias not a string: {other:?}"),
            })
            .collect();
        aliases.sort();
        assert_eq!(
            aliases,
            [
                "pizza.deliver",
                "pizza.leaderboard",
                "pizza.order",
                "pizza.setup",
                "pizza.shop",
                "pizza.shop.local",
                "pizza.tip",
                "pizza.world",
            ]
        );
    }
}
