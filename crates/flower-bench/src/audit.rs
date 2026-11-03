//! The end-of-run business audit: a pure function from what the
//! database holds (read fresh, after the drain) and what the bench
//! knows it was told (the ledger of acknowledged effects, replay and
//! fencing probes, queue and timer counts, every replica's own state)
//! to a checklist of verdicts.
//!
//! Derived values never serve as the oracle: stock, revenue and the
//! store counters are rebuilt from the source order rows and the
//! acknowledged tips, then compared with what the database derived.
//! Any failed check fails the run. The checks, by ID:
//!
//! - `coverage`: the run acknowledged at least one order.
//! - `acknowledged-orders`: every acknowledged order is stored, once.
//! - `no-phantom-orders`: every stored order was acknowledged (to a
//!   customer, or when the drain resolved an uncertain call), and no
//!   order ID appears twice.
//! - `orders-intact`: each order kept the store and quantity it was
//!   acknowledged with.
//! - `orders-delivered`: every order baked and was delivered, with
//!   consistent timestamps (due = created + bake, ready after due,
//!   delivered after ready).
//! - `stock`: each store's stock is its starting stock less every
//!   ordered pizza.
//! - `revenue`: each store's revenue is seven coins per delivered pizza
//!   (a double charge or a lost payment fails it).
//! - `tips`: each store's tips are exactly the acknowledged tips.
//! - `summaries`: each store's order, baking, ready and delivered
//!   counters match a recount of the source orders.
//! - `leaderboards`: each tenant's fresh leaderboard is its stores
//!   ranked by revenue plus tips, ties by key.
//! - `receipts`: every replay under an acknowledged request ID returned
//!   the original answer: value, revision and log position.
//! - `fencing`: every abandoned lease's token was refused, to complete
//!   and to renew.
//! - `drained`: the drain finished, the delivery queue holds nothing
//!   (pending, claimed or dead), no oven timer is pending or dead, and
//!   no uncertain call is left undecided.
//! - `replicas`: every replica applied the same log index, and each
//!   one's own replica-local reads of every store match the fresh rows.
//! - `customers`: no customer call failed (v1's rule: retries must
//!   carry every call through the failover).

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::Value;

use crate::drones::FenceProbe;
use crate::workload::{AckOrder, Mismatch, key};

/// Coins per pizza; the guest's `UNIT_PRICE`.
pub const UNIT_PRICE: f64 = 7.0;
/// Violations kept per check.
const MAX_VIOLATIONS: usize = 20;

/// The source records, as `pizza.world` pages returned them.
#[derive(Clone, Debug, Default)]
pub struct World {
    /// The `pizza.config` row.
    pub config: Value,
    /// Every store row.
    pub shops: Vec<Value>,
    /// Every order row.
    pub orders: Vec<Value>,
}

/// One replica's own view after the drain.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaView {
    /// Member ID.
    pub id: u64,
    /// Its applied log index.
    pub last_applied: Option<u64>,
    /// Stores read replica-local.
    pub stores_checked: usize,
    /// Stores whose local row differed from the fresh one.
    pub stores_differing: Vec<String>,
    /// Why it could not be read, if it could not.
    pub error: Option<String>,
}

/// Counts left in the queue and the ovens.
#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Leftovers {
    /// Delivery items pending.
    pub pending: usize,
    /// Delivery items claimed.
    pub claimed: usize,
    /// Delivery items dead.
    pub dead: usize,
    /// Oven timers pending.
    pub timers_pending: usize,
    /// Oven timers dead.
    pub timers_dead: usize,
}

/// Everything the audit reconciles.
#[derive(Clone, Debug, Default)]
pub struct AuditInput {
    /// Tenants set up.
    pub tenants: Vec<String>,
    /// Stores per tenant.
    pub stores: usize,
    /// Starting stock per store.
    pub stock_per_store: u64,
    /// Oven time.
    pub bake_ms: u64,
    /// The source records.
    pub world: World,
    /// Each tenant's fresh `pizza.leaderboard`.
    pub leaderboards: BTreeMap<String, Value>,
    /// Acknowledged orders by key (resolved uncertain ones included).
    pub acknowledged: BTreeMap<String, AckOrder>,
    /// Acknowledged tips by store key.
    pub tips: BTreeMap<String, u64>,
    /// Replay probes made, and those that differed.
    pub replay_checks: u64,
    /// Replays that differed.
    pub mismatches: Vec<Mismatch>,
    /// Whether any mutation was acknowledged with a nonzero replay rate,
    /// so replays were due.
    pub replays_expected: bool,
    /// Abandoned tokens presented again.
    pub fences: Vec<FenceProbe>,
    /// Whether abandonment was enabled and there was work to abandon.
    pub abandon_expected: bool,
    /// The queue and ovens after the drain; `None` when unreadable.
    pub leftovers: Option<Leftovers>,
    /// Whether the drain saw every order delivered in time.
    pub drained: bool,
    /// Uncertain calls the drain could not decide.
    pub undecided: usize,
    /// Every replica's own view.
    pub replicas: Vec<ReplicaView>,
    /// Failed customer calls.
    pub customer_failures: u64,
    /// The authenticated kitchen's access probes; `None` without
    /// `--auth`.
    pub access: Option<Vec<AccessProbe>>,
}

/// One check's verdict.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Check {
    /// Stable ID.
    pub id: &'static str,
    /// What it verifies, as the checklist says it.
    pub title: &'static str,
    /// Whether it held.
    pub passed: bool,
    /// One line of evidence.
    pub detail: String,
    /// The first violations found.
    pub violations: Vec<String>,
}

/// Business totals the audit counted from the source.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    /// Orders stored.
    pub orders: usize,
    /// Orders delivered.
    pub delivered: usize,
    /// Pizzas ordered.
    pub pizzas: u64,
    /// Coins earned from deliveries.
    pub revenue: u64,
    /// Coins tipped.
    pub tips: u64,
}

/// The audit's result.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Audit {
    /// Whether every check held.
    pub passed: bool,
    /// The checklist, in order.
    pub checks: Vec<Check>,
    /// Totals from the source.
    pub totals: Totals,
}

/// Every check's ID and title, in checklist order: the live view shows
/// them pending before the audit runs.
pub const CHECKLIST: [(&str, &str); 15] = [
    ("coverage", "The kitchens took orders"),
    (
        "acknowledged-orders",
        "Every acknowledged order is stored exactly once",
    ),
    (
        "no-phantom-orders",
        "No order exists that was never acknowledged",
    ),
    ("orders-intact", "Orders kept their store and quantity"),
    ("orders-delivered", "Every order baked and was delivered"),
    ("stock", "Stock is conserved"),
    ("revenue", "Revenue is seven coins per delivered pizza"),
    ("tips", "Tips match the acknowledged tips"),
    ("summaries", "Store counters match their source orders"),
    ("leaderboards", "Leaderboards match the source"),
    ("receipts", "Replays return the original receipt"),
    ("fencing", "Expired leases are fenced"),
    ("drained", "No delivery or oven left behind"),
    ("replicas", "Every replica converged on the same state"),
    ("customers", "Every customer call succeeded"),
];

/// Checks beyond the checklist: `access` in an authenticated run, and
/// the cross-group checks of a run with several groups.
pub const MORE_CHECKS: [(&str, &str); 4] = [
    ("access", "Access rules held"),
    ("group-audits", "Every group's books balance"),
    ("tenant-isolation", "Each tenant stayed in its own group"),
    ("placement", "Every partition stayed where it was placed"),
];

/// One call made outside the caller's rights (or inside them), and what
/// the authenticated kitchen answered.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessProbe {
    /// What was tried.
    pub probe: String,
    /// The code a correct kitchen answers: `FORBIDDEN`,
    /// `UNAUTHENTICATED`, or `ok` for a call within the caller's rights.
    pub expected: String,
    /// What it answered.
    pub answered: String,
}

impl AccessProbe {
    /// Whether the answer is the expected one.
    pub fn held(&self) -> bool {
        if self.expected == "ok" {
            self.answered == "ok"
        } else {
            self.answered.contains(&self.expected)
        }
    }
}

/// Collects one check's violations.
struct Tally {
    id: &'static str,
    violations: Vec<String>,
    count: usize,
}

impl Tally {
    fn new(id: &'static str) -> Self {
        Tally {
            id,
            violations: Vec::new(),
            count: 0,
        }
    }

    fn require(&mut self, condition: bool, message: impl FnOnce() -> String) {
        if !condition {
            self.count += 1;
            if self.violations.len() < MAX_VIOLATIONS {
                self.violations.push(message());
            }
        }
    }

    fn finish(self, detail: String) -> Check {
        let title = CHECKLIST
            .iter()
            .chain(MORE_CHECKS.iter())
            .find(|(id, _)| *id == self.id)
            .map_or("", |(_, title)| title);
        Check {
            id: self.id,
            title,
            passed: self.count == 0,
            detail: if self.count == 0 {
                detail
            } else {
                format!("{} violation(s). {detail}", self.count)
            },
            violations: self.violations,
        }
    }
}

fn number(row: &Value, field: &str) -> Option<f64> {
    row.get(field)?.as_f64().filter(|value| value.is_finite())
}

/// A whole, non-negative number field.
fn count(row: &Value, field: &str) -> Option<u64> {
    number(row, field)
        .filter(|value| *value >= 0.0 && value.fract() == 0.0 && *value < 9.0e15)
        .map(|value| value as u64)
}

fn strings(value: &Value) -> Option<Vec<String>> {
    value
        .as_array()?
        .iter()
        .map(|part| part.as_str().map(str::to_string))
        .collect()
}

/// A stored order, checked for shape.
struct Order {
    key: String,
    id: String,
    shop: String,
    quantity: u64,
    status: String,
    row: Value,
}

fn parse_order(row: &Value) -> Result<Order, String> {
    let shop = strings(&row["shop"])
        .filter(|parts| parts.len() == 2)
        .ok_or_else(|| format!("order without a [tenant, store]: {row}"))?;
    let id = row["id"]
        .as_str()
        .ok_or_else(|| format!("order without an ID: {row}"))?
        .to_string();
    let quantity = count(row, "quantity")
        .filter(|quantity| (1..=4).contains(quantity))
        .ok_or_else(|| format!("order {id} has an invalid quantity"))?;
    let order_key = key(&[&shop[0], &shop[1], &id]);
    if row["key"].as_str() != Some(order_key.as_str()) {
        return Err(format!("order {id}'s key does not match its identity"));
    }
    Ok(Order {
        key: order_key,
        id,
        shop: key(&[&shop[0], &shop[1]]),
        quantity,
        status: row["status"].as_str().unwrap_or("").to_string(),
        row: row.clone(),
    })
}

/// Counters recounted from source orders, per store.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Recount {
    orders: u64,
    baking: u64,
    ready: u64,
    delivered: u64,
    ordered_quantity: u64,
    delivered_quantity: u64,
}

/// Run every check.
pub fn audit(input: &AuditInput) -> Audit {
    let mut checks = Vec::new();
    let shop_ids: Vec<String> = input
        .tenants
        .iter()
        .flat_map(|tenant| {
            (0..input.stores).map(move |store| key(&[tenant, &format!("store-{store}")]))
        })
        .collect();

    // Source orders, shape-checked once.
    let mut malformed = Vec::new();
    let mut stored: BTreeMap<String, Order> = BTreeMap::new();
    let mut duplicates = Vec::new();
    let mut ids: BTreeMap<String, usize> = BTreeMap::new();
    for row in &input.world.orders {
        match parse_order(row) {
            Ok(order) => {
                *ids.entry(order.id.clone()).or_default() += 1;
                if stored.contains_key(&order.key) {
                    duplicates.push(order.key.clone());
                } else {
                    stored.insert(order.key.clone(), order);
                }
            }
            Err(message) => malformed.push(message),
        }
    }

    let mut coverage = Tally::new("coverage");
    coverage.require(!input.acknowledged.is_empty(), || {
        "no order was acknowledged: the run never exercised the pizza lifecycle".to_string()
    });
    checks.push(coverage.finish(format!("{} orders acknowledged", input.acknowledged.len())));

    let mut present = Tally::new("acknowledged-orders");
    for order_key in input.acknowledged.keys() {
        present.require(stored.contains_key(order_key), || {
            format!("acknowledged order {order_key} is missing")
        });
    }
    for duplicate in &duplicates {
        present.require(false, || format!("order {duplicate} is stored twice"));
    }
    checks.push(present.finish(format!(
        "{} acknowledged, {} stored",
        input.acknowledged.len(),
        input.world.orders.len()
    )));

    let mut phantoms = Tally::new("no-phantom-orders");
    for message in &malformed {
        phantoms.require(false, || message.clone());
    }
    for order_key in stored.keys() {
        phantoms.require(input.acknowledged.contains_key(order_key), || {
            format!("order {order_key} was never acknowledged")
        });
    }
    for (id, times) in &ids {
        phantoms.require(*times == 1, || {
            format!("order ID {id} appears {times} times")
        });
    }
    checks.push(phantoms.finish(format!("{} stored orders checked", stored.len())));

    let mut intact = Tally::new("orders-intact");
    for (order_key, acknowledged) in &input.acknowledged {
        if let Some(order) = stored.get(order_key) {
            let shop = key(&[&acknowledged.shop[0], &acknowledged.shop[1]]);
            intact.require(order.shop == shop, || {
                format!("order {order_key} moved from {shop} to {}", order.shop)
            });
            intact.require(order.quantity == acknowledged.quantity, || {
                format!(
                    "order {order_key} changed quantity from {} to {}",
                    acknowledged.quantity, order.quantity
                )
            });
        }
    }
    checks.push(intact.finish("store and quantity as acknowledged".to_string()));

    let mut delivered = Tally::new("orders-delivered");
    let bake = input.bake_ms as f64;
    for order in stored.values() {
        delivered.require(order.status == "delivered", || {
            format!("order {} is {}", order.id, order.status)
        });
        let times =
            ["createdAt", "dueAt", "readyAt", "deliveredAt"].map(|field| number(&order.row, field));
        match times {
            [Some(created), Some(due), Some(ready), Some(at)] => {
                delivered.require(due == created + bake, || {
                    format!("order {} is due {due}, not {}", order.id, created + bake)
                });
                delivered.require(ready >= due, || {
                    format!("order {} was ready before its oven rang", order.id)
                });
                delivered.require(at >= ready, || {
                    format!("order {} was delivered before it was ready", order.id)
                });
            }
            _ => delivered.require(false, || format!("order {} lacks a timestamp", order.id)),
        }
    }
    let delivered_count = stored
        .values()
        .filter(|order| order.status == "delivered")
        .count();
    checks.push(delivered.finish(format!("{delivered_count} of {} delivered", stored.len())));

    // Recount every store from the source orders.
    let mut recounts: BTreeMap<String, Recount> = shop_ids
        .iter()
        .map(|id| (id.clone(), Recount::default()))
        .collect();
    let mut unknown_store = Vec::new();
    for order in stored.values() {
        let Some(recount) = recounts.get_mut(&order.shop) else {
            unknown_store.push(order.key.clone());
            continue;
        };
        recount.orders += 1;
        recount.ordered_quantity += order.quantity;
        match order.status.as_str() {
            "baking" => recount.baking += 1,
            "ready" => recount.ready += 1,
            "delivered" => {
                recount.delivered += 1;
                recount.delivered_quantity += order.quantity;
            }
            _ => {}
        }
    }
    let mut rows: BTreeMap<String, Value> = BTreeMap::new();
    let mut shop_shape = Vec::new();
    for row in &input.world.shops {
        match row["key"].as_str() {
            Some(shop_key) => {
                rows.insert(shop_key.to_string(), row.clone());
            }
            None => shop_shape.push(format!("store row without a key: {row}")),
        }
    }

    let mut stock = Tally::new("stock");
    let mut revenue = Tally::new("revenue");
    let mut tips = Tally::new("tips");
    let mut summaries = Tally::new("summaries");
    for message in &shop_shape {
        stock.require(false, || message.clone());
    }
    for order_key in &unknown_store {
        stock.require(false, || format!("order {order_key} belongs to no store"));
    }
    for (shop, tipped) in &input.tips {
        tips.require(recounts.contains_key(shop), || {
            format!("tips acknowledged for unknown store {shop}")
        });
        let _ = tipped;
    }
    let mut totals = Totals {
        orders: stored.len(),
        delivered: delivered_count,
        ..Totals::default()
    };
    for (shop, recount) in &recounts {
        totals.pizzas += recount.ordered_quantity;
        let Some(row) = rows.get(shop) else {
            stock.require(false, || format!("store {shop} is missing"));
            continue;
        };
        let expected_stock = input.stock_per_store as f64 - recount.ordered_quantity as f64;
        stock.require(number(row, "stock") == Some(expected_stock), || {
            format!(
                "store {shop} has stock {:?}, expected {expected_stock}",
                number(row, "stock")
            )
        });
        let expected_revenue = recount.delivered_quantity as f64 * UNIT_PRICE;
        revenue.require(number(row, "revenue") == Some(expected_revenue), || {
            format!(
                "store {shop} earned {:?} coins for {} delivered pizzas, expected {expected_revenue}",
                number(row, "revenue"),
                recount.delivered_quantity
            )
        });
        let expected_tips = input.tips.get(shop).copied().unwrap_or(0);
        tips.require(count(row, "tips") == Some(expected_tips), || {
            format!(
                "store {shop} holds {:?} in tips, acknowledged {expected_tips}",
                number(row, "tips")
            )
        });
        totals.revenue += count(row, "revenue").unwrap_or(0);
        totals.tips += count(row, "tips").unwrap_or(0);
        for (field, want) in [
            ("orders", recount.orders),
            ("baking", recount.baking),
            ("ready", recount.ready),
            ("delivered", recount.delivered),
        ] {
            summaries.require(count(row, field) == Some(want), || {
                format!(
                    "store {shop} counts {:?} {field}, its orders say {want}",
                    number(row, field)
                )
            });
        }
    }
    for shop in rows.keys() {
        stock.require(recounts.contains_key(shop), || {
            format!("unexpected store {shop}")
        });
    }
    checks.push(stock.finish(format!(
        "{} stores, {} pizzas ordered",
        recounts.len(),
        totals.pizzas
    )));
    checks.push(revenue.finish(format!("{} coins earned", totals.revenue)));
    let acknowledged_tips: u64 = input.tips.values().sum();
    checks.push(tips.finish(format!("{acknowledged_tips} coins acknowledged")));
    checks.push(summaries.finish("orders, baking, ready, delivered".to_string()));

    let mut boards = Tally::new("leaderboards");
    for tenant in &input.tenants {
        let mut expected: Vec<&Value> = rows
            .iter()
            .filter(|(shop, _)| {
                shop.starts_with(&format!(
                    "[{}",
                    serde_json::to_string(tenant).unwrap_or_default()
                ))
            })
            .map(|(_, row)| row)
            .collect();
        let score = |row: &Value| {
            number(row, "revenue").unwrap_or(0.0) + number(row, "tips").unwrap_or(0.0)
        };
        expected.sort_by(|a, b| {
            score(b)
                .partial_cmp(&score(a))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a["key"].as_str().cmp(&b["key"].as_str()))
        });
        let expected: Vec<Value> = expected.into_iter().cloned().collect();
        match input.leaderboards.get(tenant).and_then(Value::as_array) {
            Some(board) => boards.require(*board == expected, || {
                format!("tenant {tenant}'s leaderboard differs from its stores ranked by revenue plus tips")
            }),
            None => boards.require(false, || format!("tenant {tenant} has no leaderboard")),
        }
    }
    checks.push(boards.finish(format!("{} tenants", input.tenants.len())));

    let mut receipts = Tally::new("receipts");
    for mismatch in &input.mismatches {
        receipts.require(false, || format!("{}: {}", mismatch.op, mismatch.detail));
    }
    receipts.require(!input.replays_expected || input.replay_checks > 0, || {
        "no receipt was replayed".to_string()
    });
    checks.push(receipts.finish(format!(
        "{} replays, each the original value, revision and log position",
        input.replay_checks
    )));

    let mut fencing = Tally::new("fencing");
    for probe in &input.fences {
        fencing.require(probe.rejected, || {
            format!(
                "stale token for {} (order {}): complete {}, renew {}",
                probe.item, probe.order, probe.complete, probe.heartbeat
            )
        });
    }
    fencing.require(!input.abandon_expected || !input.fences.is_empty(), || {
        "no lease was abandoned, so fencing went untested".to_string()
    });
    checks.push(fencing.finish(format!(
        "{} abandoned leases, every token refused",
        input.fences.len()
    )));

    let mut drained = Tally::new("drained");
    drained.require(input.drained, || {
        "the drain ended before every order was delivered".to_string()
    });
    drained.require(input.undecided == 0, || {
        format!("{} uncertain calls were never decided", input.undecided)
    });
    match input.leftovers {
        Some(left) => {
            drained.require(left.pending + left.claimed + left.dead == 0, || {
                format!(
                    "the delivery queue holds {} pending, {} claimed, {} dead",
                    left.pending, left.claimed, left.dead
                )
            });
            drained.require(left.timers_pending + left.timers_dead == 0, || {
                format!(
                    "{} oven timers pending, {} dead",
                    left.timers_pending, left.timers_dead
                )
            });
        }
        None => drained.require(false, || {
            "the queue and timers could not be read".to_string()
        }),
    }
    checks.push(drained.finish("queue and ovens empty".to_string()));

    let mut replicas = Tally::new("replicas");
    let applied: BTreeSet<Option<u64>> = input
        .replicas
        .iter()
        .map(|replica| replica.last_applied)
        .collect();
    replicas.require(applied.len() <= 1, || {
        format!(
            "replicas applied different log indexes: {}",
            input
                .replicas
                .iter()
                .map(|replica| format!("{}@{:?}", replica.id, replica.last_applied))
                .collect::<Vec<_>>()
                .join(", ")
        )
    });
    for replica in &input.replicas {
        if let Some(error) = &replica.error {
            replicas.require(false, || {
                format!("replica {} could not be read: {error}", replica.id)
            });
        }
        for shop in &replica.stores_differing {
            replicas.require(false, || {
                format!("replica {}'s own copy of store {shop} differs", replica.id)
            });
        }
    }
    checks.push(replicas.finish(format!(
        "{} replicas at index {}",
        input.replicas.len(),
        applied
            .iter()
            .flatten()
            .next()
            .map_or("?".to_string(), u64::to_string)
    )));

    let mut customers = Tally::new("customers");
    customers.require(input.customer_failures == 0, || {
        format!("{} customer calls failed", input.customer_failures)
    });
    checks.push(customers.finish("no failed customer call".to_string()));

    if let Some(probes) = &input.access {
        let mut access = Tally::new("access");
        access.require(!probes.is_empty(), || "no access probe ran".to_string());
        for probe in probes {
            access.require(probe.held(), || {
                format!(
                    "{}: expected {}, answered {}",
                    probe.probe, probe.expected, probe.answered
                )
            });
        }
        checks.push(access.finish(format!(
            "{} probes: other tenants, anonymous callers, forged tokens and non-admins refused; own calls served",
            probes.len()
        )));
    }

    Audit {
        passed: checks.iter().all(|check| check.passed),
        checks,
        totals,
    }
}

/// One group's side of the cross-group audit.
#[derive(Clone, Debug, Default)]
pub struct GroupSide {
    /// The group's name.
    pub name: String,
    /// Its tenants.
    pub tenants: Vec<String>,
    /// The source records it read (every database it serves).
    pub world: World,
    /// Whether its own audit held.
    pub passed: bool,
}

/// What the cross-group audit reconciles.
#[derive(Clone, Debug, Default)]
pub struct AcrossInput {
    /// Every group.
    pub groups: Vec<GroupSide>,
    /// Partitions the bench placed: (partition, group); empty when the
    /// run placed none (one group, or separate clusters).
    pub planned: Vec<(String, String)>,
    /// The catalog read after the drain; `None` when unreadable.
    pub placements: Option<Vec<crate::groups::Placement>>,
}

/// The audit across groups: every group's own audit held, every
/// tenant's stores and orders are in its own group's databases only,
/// and every partition is still active on the group it was placed on at
/// its first epoch (nothing moved under the load).
pub fn across(input: &AcrossInput) -> Audit {
    let mut checks = Vec::new();
    let mut totals = Totals::default();

    let mut groups = Tally::new("group-audits");
    for group in &input.groups {
        groups.require(group.passed, || {
            format!("{}'s own audit failed", group.name)
        });
    }
    checks.push(groups.finish(format!(
        "{} groups, each audited on its own",
        input.groups.len()
    )));

    let mut isolation = Tally::new("tenant-isolation");
    let mut owner: BTreeMap<&str, &str> = BTreeMap::new();
    for group in &input.groups {
        for tenant in &group.tenants {
            if let Some(other) = owner.insert(tenant, &group.name) {
                isolation.require(false, || {
                    format!("{tenant} was assigned to {other} and {}", group.name)
                });
            }
        }
    }
    for group in &input.groups {
        let mine: BTreeSet<&str> = group.tenants.iter().map(String::as_str).collect();
        let mut seen = BTreeSet::new();
        let tenant_of = |row: &Value, field: &str| -> Option<String> {
            row.get(field)?
                .as_array()?
                .first()?
                .as_str()
                .map(str::to_string)
        };
        for shop in &group.world.shops {
            let tenant = tenant_of(shop, "id").unwrap_or_default();
            isolation.require(mine.contains(tenant.as_str()), || {
                format!(
                    "{} holds store {} of another group's tenant",
                    group.name, shop["key"]
                )
            });
            seen.insert(tenant);
        }
        for order in &group.world.orders {
            let tenant = tenant_of(order, "shop").unwrap_or_default();
            isolation.require(mine.contains(tenant.as_str()), || {
                format!(
                    "{} holds order {} of another group's tenant",
                    group.name, order["key"]
                )
            });
            totals.orders += 1;
            if order["status"] == "delivered" {
                totals.delivered += 1;
            }
            totals.pizzas += count(order, "quantity").unwrap_or(0);
        }
        for shop in &group.world.shops {
            totals.revenue += count(shop, "revenue").unwrap_or(0);
            totals.tips += count(shop, "tips").unwrap_or(0);
        }
        for tenant in &mine {
            isolation.require(seen.contains(*tenant), || {
                format!("{}'s stores are missing from {}", tenant, group.name)
            });
        }
    }
    checks.push(isolation.finish(format!(
        "{} tenants, each in exactly one group",
        owner.len()
    )));

    if !input.planned.is_empty() {
        let mut placement = Tally::new("placement");
        match &input.placements {
            None => placement.require(false, || "the catalog could not be read".to_string()),
            Some(placed) => {
                for (partition, group) in &input.planned {
                    let found = placed.iter().find(|entry| &entry.partition == partition);
                    placement.require(
                        found.is_some_and(|entry| {
                            &entry.group == group && entry.epoch == 1 && entry.status == "active"
                        }),
                        || match found {
                            Some(entry) => format!(
                                "{partition} is {} on {} at epoch {}, placed on {group} at epoch 1",
                                entry.status, entry.group, entry.epoch
                            ),
                            None => format!("{partition} is not in the catalog"),
                        },
                    );
                }
            }
        }
        checks.push(placement.finish(format!(
            "{} partitions active where placed, epoch 1",
            input.planned.len()
        )));
    }

    Audit {
        passed: checks.iter().all(|check| check.passed),
        checks,
        totals,
    }
}

/// One audit of many: each check holds only where it held in every
/// group (the live view's checklist for a run with several groups).
pub fn combine(audits: &[&Audit]) -> Option<Audit> {
    let first = audits.first()?;
    let mut checks = Vec::new();
    for check in &first.checks {
        let all: Vec<&Check> = audits
            .iter()
            .filter_map(|audit| audit.checks.iter().find(|other| other.id == check.id))
            .collect();
        let failed = all.iter().filter(|check| !check.passed).count();
        checks.push(Check {
            id: check.id,
            title: check.title,
            passed: failed == 0 && all.len() == audits.len(),
            detail: if failed == 0 {
                format!("held in all {} groups", audits.len())
            } else {
                format!("failed in {failed} of {} groups", audits.len())
            },
            violations: all
                .iter()
                .flat_map(|check| check.violations.iter().cloned())
                .take(MAX_VIOLATIONS)
                .collect(),
        });
    }
    let mut totals = Totals::default();
    for audit in audits {
        totals.orders += audit.totals.orders;
        totals.delivered += audit.totals.delivered;
        totals.pizzas += audit.totals.pizzas;
        totals.revenue += audit.totals.revenue;
        totals.tips += audit.totals.tips;
    }
    Some(Audit {
        passed: checks.iter().all(|check| check.passed),
        checks,
        totals,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A small settled kitchen that passes every check: two tenants,
    /// one store each, three delivered orders and some tips.
    fn settled() -> AuditInput {
        let orders = [
            ("tenant-0", "store-0", "pizza-000001", 2),
            ("tenant-0", "store-0", "pizza-000002", 1),
            ("tenant-1", "store-0", "pizza-000003", 4),
        ];
        let mut acknowledged = BTreeMap::new();
        let mut rows = Vec::new();
        for (tenant, store, id, quantity) in orders {
            let order_key = key(&[tenant, store, id]);
            acknowledged.insert(
                order_key.clone(),
                AckOrder {
                    key: order_key.clone(),
                    id: id.to_string(),
                    shop: [tenant.to_string(), store.to_string()],
                    quantity,
                },
            );
            rows.push(json!({
                "id": id, "key": order_key, "shop": [tenant, store], "quantity": quantity,
                "status": "delivered", "createdAt": 1000, "dueAt": 1250, "readyAt": 1260,
                "deliveredAt": 1300,
            }));
        }
        let shop = |tenant: &str, ordered: u64, delivered: u64, count: u64, tips: u64| {
            json!({
                "id": [tenant, "store-0"], "key": key(&[tenant, "store-0"]), "name": "x",
                "stock": 100 - ordered, "revenue": delivered * 7, "tips": tips,
                "orders": count, "baking": 0, "ready": 0, "delivered": count,
            })
        };
        let shops = vec![shop("tenant-0", 3, 3, 2, 5), shop("tenant-1", 4, 4, 1, 0)];
        let mut tips = BTreeMap::new();
        tips.insert(key(&["tenant-0", "store-0"]), 5);
        let leaderboards = BTreeMap::from([
            ("tenant-0".to_string(), json!([shops[0].clone()])),
            ("tenant-1".to_string(), json!([shops[1].clone()])),
        ]);
        AuditInput {
            tenants: vec!["tenant-0".to_string(), "tenant-1".to_string()],
            stores: 1,
            stock_per_store: 100,
            bake_ms: 250,
            world: World {
                config: json!({}),
                shops,
                orders: rows,
            },
            leaderboards,
            acknowledged,
            tips,
            replay_checks: 4,
            mismatches: Vec::new(),
            replays_expected: true,
            fences: vec![FenceProbe {
                item: "item".to_string(),
                order: "pizza-000002".to_string(),
                complete: "refused: invalid_value: claim token mismatch".to_string(),
                heartbeat: "refused: invalid_value: unknown queue item".to_string(),
                rejected: true,
            }],
            abandon_expected: true,
            leftovers: Some(Leftovers::default()),
            drained: true,
            undecided: 0,
            replicas: vec![
                ReplicaView {
                    id: 1,
                    last_applied: Some(40),
                    stores_checked: 2,
                    ..ReplicaView::default()
                },
                ReplicaView {
                    id: 2,
                    last_applied: Some(40),
                    stores_checked: 2,
                    ..ReplicaView::default()
                },
            ],
            customer_failures: 0,
            access: None,
        }
    }

    /// The settled kitchen split in two groups by tenant.
    fn two_groups() -> AcrossInput {
        let world = settled().world;
        let side = |name: &str, tenant: &str| GroupSide {
            name: name.to_string(),
            tenants: vec![tenant.to_string()],
            world: World {
                config: json!({}),
                shops: world
                    .shops
                    .iter()
                    .filter(|shop| shop["id"][0] == tenant)
                    .cloned()
                    .collect(),
                orders: world
                    .orders
                    .iter()
                    .filter(|order| order["shop"][0] == tenant)
                    .cloned()
                    .collect(),
            },
            passed: true,
        };
        let placed = |partition: &str, group: &str, epoch: u64| crate::groups::Placement {
            partition: partition.to_string(),
            group: group.to_string(),
            epoch,
            status: "active".to_string(),
        };
        AcrossInput {
            groups: vec![side("g1", "tenant-0"), side("g2", "tenant-1")],
            planned: vec![
                ("pizza-tenant-0".to_string(), "g1".to_string()),
                ("pizza-tenant-1".to_string(), "g2".to_string()),
            ],
            placements: Some(vec![
                placed("pizza-tenant-0", "g1", 1),
                placed("pizza-tenant-1", "g2", 1),
            ]),
        }
    }

    #[test]
    fn groups_that_kept_to_themselves_pass_across() {
        let audit = across(&two_groups());
        assert!(audit.passed, "{:#?}", audit.checks);
        assert_eq!(audit.totals.orders, 3);
        assert_eq!(audit.totals.revenue, 49);
    }

    #[test]
    fn a_tenant_in_the_wrong_group_fails_isolation() {
        let mut input = two_groups();
        // tenant-1's order turns up in g1's database.
        let stray = input.groups[1].world.orders[0].clone();
        input.groups[0].world.orders.push(stray);
        assert_eq!(failing(&across(&input)), vec!["tenant-isolation"]);
        // One tenant assigned to two groups.
        let mut input = two_groups();
        input.groups[1].tenants.push("tenant-0".to_string());
        assert!(failing(&across(&input)).contains(&"tenant-isolation"));
    }

    #[test]
    fn a_moved_partition_or_a_failed_group_fails_across() {
        let mut input = two_groups();
        if let Some(placed) = &mut input.placements {
            placed[1].group = "g1".to_string();
            placed[1].epoch = 2;
        }
        assert_eq!(failing(&across(&input)), vec!["placement"]);
        let mut input = two_groups();
        input.placements = None;
        assert_eq!(failing(&across(&input)), vec!["placement"]);
        let mut input = two_groups();
        input.groups[0].passed = false;
        assert_eq!(failing(&across(&input)), vec!["group-audits"]);
    }

    #[test]
    fn an_access_probe_that_was_served_fails_access() {
        let probe = |expected: &str, answered: &str| AccessProbe {
            probe: "x".to_string(),
            expected: expected.to_string(),
            answered: answered.to_string(),
        };
        let mut input = settled();
        input.access = Some(vec![
            probe(
                "FORBIDDEN",
                "http-403:unauthorized: FORBIDDEN: access denied",
            ),
            probe(
                "UNAUTHENTICATED",
                "http-403:unauthorized: UNAUTHENTICATED: bad",
            ),
            probe("ok", "ok"),
        ]);
        assert!(audit(&input).passed);
        // Another tenant's store read without refusal: the rules failed.
        input.access = Some(vec![probe("FORBIDDEN", "ok")]);
        assert_eq!(failing(&audit(&input)), vec!["access"]);
        input.access = Some(Vec::new());
        assert_eq!(failing(&audit(&input)), vec!["access"]);
    }

    #[test]
    fn combined_checks_hold_only_where_every_group_held() {
        let good = audit(&settled());
        let mut input = settled();
        input.customer_failures = 1;
        let bad = audit(&input);
        let combined = combine(&[&good, &bad]).unwrap();
        assert!(!combined.passed);
        let customers = combined
            .checks
            .iter()
            .find(|check| check.id == "customers")
            .unwrap();
        assert!(!customers.passed && customers.detail.contains("1 of 2"));
        assert_eq!(combined.totals.orders, 6);
    }

    fn failing(audit: &Audit) -> Vec<&'static str> {
        audit
            .checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| check.id)
            .collect()
    }

    #[test]
    fn a_settled_kitchen_passes_every_check() {
        let audit = audit(&settled());
        assert!(audit.passed, "{:#?}", audit.checks);
        assert_eq!(audit.checks.len(), CHECKLIST.len());
        for (check, (id, title)) in audit.checks.iter().zip(CHECKLIST) {
            assert_eq!(check.id, id);
            assert_eq!(check.title, title);
        }
        assert_eq!(
            audit.totals,
            Totals {
                orders: 3,
                delivered: 3,
                pizzas: 7,
                revenue: 49,
                tips: 5
            }
        );
    }

    #[test]
    fn a_lost_order_fails() {
        let mut input = settled();
        input.world.orders.remove(1);
        // The store row still says what the lost order did to it.
        let audit = audit(&input);
        assert!(!audit.passed);
        let failed = failing(&audit);
        assert!(failed.contains(&"acknowledged-orders"), "{failed:?}");
        assert!(audit.checks[1].violations[0].contains("pizza-000002"));
    }

    #[test]
    fn a_double_charge_fails() {
        let mut input = settled();
        // One delivery paid twice: seven coins too many.
        input.world.shops[0]["revenue"] = json!(28);
        input.leaderboards.insert(
            "tenant-0".to_string(),
            json!([input.world.shops[0].clone()]),
        );
        let audit = audit(&input);
        assert_eq!(failing(&audit), vec!["revenue"]);
    }

    #[test]
    fn a_replay_mismatch_fails() {
        let mut input = settled();
        input.mismatches.push(Mismatch {
            op: "pizza.order".to_string(),
            detail: "revision Some(5) became Some(9)".to_string(),
        });
        assert_eq!(failing(&audit(&input)), vec!["receipts"]);
    }

    #[test]
    fn phantom_and_duplicate_orders_fail() {
        let mut input = settled();
        let mut phantom = input.world.orders[0].clone();
        phantom["id"] = json!("pizza-000099");
        phantom["key"] = json!(key(&["tenant-0", "store-0", "pizza-000099"]));
        input.world.orders.push(phantom);
        let mut twin = input.world.orders[2].clone();
        twin["shop"] = json!(["tenant-0", "store-0"]);
        twin["key"] = json!(key(&["tenant-0", "store-0", "pizza-000003"]));
        input.world.orders.push(twin);
        let failed = failing(&audit(&input));
        assert!(failed.contains(&"no-phantom-orders"), "{failed:?}");
        assert!(failed.contains(&"stock"), "{failed:?}");
    }

    #[test]
    fn other_drift_fails_its_own_check() {
        let mut input = settled();
        input.tips.insert(key(&["tenant-1", "store-0"]), 1);
        assert_eq!(failing(&audit(&input)), vec!["tips"]);

        let mut input = settled();
        input.world.orders[0]["status"] = json!("ready");
        let failed = failing(&audit(&input));
        assert!(failed.contains(&"orders-delivered") && failed.contains(&"summaries"));

        let mut input = settled();
        input.fences[0].rejected = false;
        assert_eq!(failing(&audit(&input)), vec!["fencing"]);

        let mut input = settled();
        input.fences.clear();
        assert_eq!(failing(&audit(&input)), vec!["fencing"]);

        let mut input = settled();
        input.leftovers = Some(Leftovers {
            dead: 1,
            ..Leftovers::default()
        });
        assert_eq!(failing(&audit(&input)), vec!["drained"]);

        let mut input = settled();
        input.replicas[1].last_applied = Some(39);
        assert_eq!(failing(&audit(&input)), vec!["replicas"]);

        let mut input = settled();
        input.leaderboards.insert(
            "tenant-0".to_string(),
            json!([input.world.shops[1].clone()]),
        );
        assert_eq!(failing(&audit(&input)), vec!["leaderboards"]);

        let mut input = settled();
        input.customer_failures = 2;
        assert_eq!(failing(&audit(&input)), vec!["customers"]);

        let mut input = settled();
        input.acknowledged.clear();
        assert!(failing(&audit(&input)).contains(&"coverage"));
    }
}
