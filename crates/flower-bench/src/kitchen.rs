//! One Raft group's kitchen: its tenants and their databases, its
//! client, accounting, ledger, drone yard and customers, and the steps
//! a run takes in it (setup, the access probes, the drain's
//! resolution, the audit's fresh reads).
//!
//! A run drives one kitchen per group, all at once. In a single-group run the
//! kitchen's one database (`pizza`) holds every tenant; with many groups
//! hosted by every process each tenant is a named partition `pizza-<tenant>`
//! placed on its group; with separate clusters each group's root holds its
//! tenants in `pizza`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flower_client::client::RequestOptions;
use serde_json::{Value, json};

use crate::audit::{AccessProbe, Leftovers, ReplicaView, World};
use crate::client::{BenchClient, CallError, Request, Who};
use crate::cluster::Cluster;
use crate::config::Settings;
use crate::drones::{self, Yard};
use crate::metrics::{Class, Recorder};
use crate::workload::{ArrivalCounters, Customers, Effect, Ledger, Mismatch, Receipt, compare};

/// The module name the guest deploys as.
pub const MODULE: &str = "pizza";
/// Rounds of resending uncertain mutations before giving up.
const RESOLVE_ROUNDS: usize = 3;
/// Most acknowledged orders the audit replays.
const AUDIT_REPLAYS: usize = 100;

/// One group's kitchen.
pub struct Kitchen {
    /// Its index among the run's groups.
    pub index: usize,
    /// The group's name: `root` for a single group, else `g1`, `g2`...
    pub name: String,
    /// The servers it runs on, when the bench started them.
    pub cluster: Option<Arc<Cluster>>,
    /// Its client (its group's leader and status).
    pub client: Arc<BenchClient>,
    /// Its accounting; counts in the run's too.
    pub recorder: Arc<Recorder>,
    /// Its tenants.
    pub tenants: Vec<String>,
    /// The database holding each tenant.
    pub dbs: Arc<BTreeMap<String, String>>,
    /// Its workload seed.
    pub seed: String,
    /// The acknowledged effects.
    pub ledger: Arc<Ledger>,
    /// Its drones.
    pub yard: Arc<Yard>,
    /// Open-loop counters.
    pub arrivals: Arc<ArrivalCounters>,
}

/// Answer an access probe the way the audit reads it.
fn answered(result: &Result<crate::client::Answer, CallError>) -> String {
    match result {
        Ok(_) => "ok".to_string(),
        Err(CallError::Refused { kind, message }) => format!("{kind}: {message}"),
        Err(CallError::Business { code, message }) => format!("business:{code}: {message}"),
        Err(error) => error.to_string(),
    }
}

impl Kitchen {
    /// Its databases, each once, in tenant order.
    pub fn databases(&self) -> Vec<String> {
        let mut databases: Vec<String> = Vec::new();
        for tenant in &self.tenants {
            let db = self.db(tenant).to_string();
            if !databases.contains(&db) {
                databases.push(db);
            }
        }
        databases
    }

    /// The database holding `tenant`.
    pub fn db(&self, tenant: &str) -> &str {
        self.dbs
            .get(tenant)
            .map_or_else(|| self.client.db(), String::as_str)
    }

    /// The tenants a database holds.
    pub fn tenants_in(&self, db: &str) -> Vec<String> {
        self.tenants
            .iter()
            .filter(|tenant| self.db(tenant) == db)
            .cloned()
            .collect()
    }

    /// Every store, as `(tenant, [tenant, store])`.
    pub fn shops(&self, stores: usize) -> Vec<(String, Value)> {
        self.tenants
            .iter()
            .flat_map(|tenant| {
                (0..stores)
                    .map(move |store| (tenant.clone(), json!([tenant, format!("store-{store}")])))
            })
            .collect()
    }

    /// Its customers.
    pub fn customers(&self, settings: &Arc<Settings>) -> Arc<Customers> {
        Arc::new(Customers {
            client: self.client.clone(),
            settings: settings.clone(),
            ledger: self.ledger.clone(),
            arrivals: self.arrivals.clone(),
            tenants: self.tenants.clone(),
            dbs: self.dbs.clone(),
            seed: self.seed.clone(),
        })
    }

    /// Deploy the guest in every database, open the kitchens (with the
    /// trusted key under `--auth`) and make the setup visible on every
    /// replica with one fresh read each.
    pub async fn setup(
        &self,
        settings: &Settings,
        wasm: &[u8],
        manifest: &Value,
    ) -> Result<(), String> {
        for db in self.databases() {
            let mut last = String::new();
            let mut deployed = false;
            for _ in 0..20 {
                let member = self
                    .client
                    .find_leader(Duration::from_secs(2))
                    .await
                    .unwrap_or(0);
                match self
                    .client
                    .admin_client(member)
                    .deploy(
                        &db,
                        MODULE,
                        None,
                        wasm,
                        manifest,
                        &RequestOptions::new().timeout(Duration::from_secs(120)),
                    )
                    .await
                {
                    Ok(_) => {
                        deployed = true;
                        break;
                    }
                    Err(error) => {
                        last = error.to_string();
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
            if !deployed {
                return Err(format!("deploying the guest in {db} failed: {last}"));
            }
            let mut args = json!({
                "tenants": self.tenants_in(&db),
                "storesPerTenant": settings.stores,
                "stockPerShop": settings.stock_per_store,
                "bakeMs": settings.bake_ms,
                "leaseMs": settings.lease_ms,
            });
            if let Some(identities) = self.client.identities() {
                args["authKey"] = Value::String(identities.public_key_hex());
            }
            let request = self.client.mutate(
                &db,
                "pizza.setup",
                &args,
                &self.client.next_id(),
                Who::Gateway,
            );
            self.client
                .call("pizza.setup", Class::Control, &request, Instant::now())
                .await
                .map_err(|error| format!("opening the kitchens in {db} failed: {error}"))?;
            let tenant = self.tenants_in(&db).into_iter().next().unwrap_or_default();
            let fresh = self.client.query(
                &db,
                "pizza.shop",
                &json!([tenant, "store-0"]),
                Who::Tenant(&tenant),
            );
            for member in 0..self.client.members().len() {
                self.client
                    .call_on(member, "setup.visible", Class::Control, &fresh)
                    .await
                    .map_err(|error| {
                        format!("member {} never saw the setup of {db}: {error}", member + 1)
                    })?;
            }
        }
        Ok(())
    }

    /// The authenticated kitchen's rules, probed before the load (the
    /// audit's `access` check): another tenant's store, an anonymous
    /// caller, a forged token, a tenant reading the world, the
    /// administrator acting for a tenant and a tenant ordering for
    /// another must each be refused; a tenant's own read and the
    /// administrator's world read must be served.
    pub async fn probe_access(&self) -> Vec<AccessProbe> {
        let Some(tenant) = self.tenants.first().cloned() else {
            return Vec::new();
        };
        let db = self.db(&tenant).to_string();
        let own = json!([tenant, "store-0"]);
        let stranger = format!("{tenant}-stranger");
        let theirs = json!([stranger, "store-0"]);
        let forged = {
            let mut body: Value = serde_json::from_slice(
                &self.client.query(&db, "pizza.shop", &own, Who::Anyone).body,
            )
            .unwrap_or(Value::Null);
            body["credentials"] = crate::auth::bearer(&crate::auth::Identities::forged(&tenant));
            Request {
                endpoint: crate::client::Endpoint::Query,
                name: "pizza.shop".to_string(),
                body: Arc::new(serde_json::to_vec(&body).unwrap_or_default()),
                gateway: false,
            }
        };
        let order_for_another = self.client.mutate(
            &db,
            "pizza.order",
            &json!({"id": "probe-000001", "shop": theirs, "quantity": 1}),
            &self.client.next_id(),
            Who::Tenant(&tenant),
        );
        let probes: Vec<(String, &str, Request)> = vec![
            (
                format!("{tenant} reads another tenant's store"),
                "FORBIDDEN",
                self.client
                    .query(&db, "pizza.shop", &theirs, Who::Tenant(&tenant)),
            ),
            (
                "an anonymous caller reads a store".to_string(),
                "FORBIDDEN",
                self.client.query(&db, "pizza.shop", &own, Who::Anyone),
            ),
            (
                format!("a token for {tenant} under a foreign key"),
                "UNAUTHENTICATED",
                forged,
            ),
            (
                format!("{tenant} reads the whole world"),
                "FORBIDDEN",
                self.client.query(
                    &db,
                    "pizza.world",
                    &json!({"limit": 1}),
                    Who::Tenant(&tenant),
                ),
            ),
            (
                "the administrator reads a tenant's store".to_string(),
                "FORBIDDEN",
                self.client.query(&db, "pizza.shop", &own, Who::Admin),
            ),
            (
                format!("{tenant} orders for another tenant"),
                "FORBIDDEN",
                order_for_another,
            ),
            (
                format!("{tenant} reads its own store"),
                "ok",
                self.client
                    .query(&db, "pizza.shop", &own, Who::Tenant(&tenant)),
            ),
            (
                "the administrator reads the world".to_string(),
                "ok",
                self.client
                    .query(&db, "pizza.world", &json!({"limit": 1}), Who::Admin),
            ),
        ];
        let mut results = Vec::new();
        for (probe, expected, request) in probes {
            let result = self
                .client
                .call("access.probe", Class::Control, &request, Instant::now())
                .await;
            results.push(AccessProbe {
                probe,
                expected: expected.to_string(),
                answered: answered(&result),
            });
        }
        results
    }

    /// Send every uncertain mutation again under its own request ID
    /// until it is decided; returns how many stayed undecided.
    pub async fn resolve(&self) -> usize {
        let ledger = &self.ledger;
        let mut pending = ledger.take_pending();
        for _ in 0..RESOLVE_ROUNDS {
            if pending.is_empty() {
                break;
            }
            let mut still = Vec::new();
            for call in pending {
                let op = format!("{}.resolve", call.op);
                match self
                    .client
                    .call(&op, Class::Control, &call.request, Instant::now())
                    .await
                {
                    Ok(answer) => {
                        let receipt = Receipt {
                            op: call.op.clone(),
                            request: call.request.clone(),
                            answer,
                        };
                        match call.effect {
                            Effect::Order(order) => ledger.order(order, receipt),
                            Effect::Tip { shop, amount } => ledger.tip(&shop, amount, receipt),
                        }
                    }
                    // Decided: it ran and failed, so nothing applied.
                    Err(error) if !error.is_uncertain() => {}
                    Err(_) => still.push(call),
                }
            }
            pending = still;
        }
        let undecided = pending.len();
        for call in pending {
            ledger.uncertain(call);
        }
        undecided
    }

    /// Wait until every acknowledged order is delivered and the queues
    /// and ovens are empty, at most `drain`.
    pub async fn wait_drained(&self, drain: Duration) -> bool {
        let deadline = Instant::now() + drain;
        while Instant::now() < deadline {
            if self.drained_once().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        false
    }

    async fn drained_once(&self) -> bool {
        let acknowledged = self.ledger.acknowledged_orders() as u64;
        let (mut orders, mut delivered) = (0, 0);
        for tenant in &self.tenants {
            let request = self.client.query(
                self.db(tenant),
                "pizza.leaderboard",
                &json!(tenant),
                Who::Tenant(tenant),
            );
            let Ok(answer) = self
                .client
                .call("drain.check", Class::Control, &request, Instant::now())
                .await
            else {
                return false;
            };
            for store in answer.value.as_array().into_iter().flatten() {
                orders += store["orders"].as_u64().unwrap_or(0);
                delivered += store["delivered"].as_u64().unwrap_or(0);
            }
        }
        if orders < acknowledged || delivered != orders {
            return false;
        }
        matches!(self.leftovers().await, Some(left) if left.pending + left.claimed + left.dead + left.timers_pending == 0)
    }

    /// Every database's delivery queue and ovens, summed.
    pub async fn leftovers(&self) -> Option<Leftovers> {
        let options = RequestOptions::new().timeout(Duration::from_secs(2));
        let mut total = Leftovers::default();
        for db in self.databases() {
            let mut read = None;
            for _ in 0..10 {
                let member = self.client.worker_target();
                let admin = self.client.admin_client(member);
                let queue = admin.queue_stats(&db, drones::QUEUE, &options).await;
                let timers = admin.timer_stats(&db, &options).await;
                match (queue, timers) {
                    (Ok(queue), Ok(timers)) => {
                        read = Some(Leftovers {
                            pending: queue.pending,
                            claimed: queue.claimed,
                            dead: queue.dead,
                            timers_pending: timers.pending,
                            timers_dead: timers.dead,
                        });
                        break;
                    }
                    _ => {
                        self.client.report_failure(member);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            let read = read?;
            total.pending += read.pending;
            total.claimed += read.claimed;
            total.dead += read.dead;
            total.timers_pending += read.timers_pending;
            total.timers_dead += read.timers_dead;
        }
        Some(total)
    }

    /// Replay up to [`AUDIT_REPLAYS`] acknowledged orders, spread over
    /// the run, and every kept tip: after any failover, receipts written
    /// before it must still answer exactly.
    pub async fn replay_receipts(&self) {
        let receipts = self.ledger.receipts();
        let orders: Vec<&Receipt> = receipts
            .iter()
            .filter(|receipt| receipt.op == "pizza.order")
            .collect();
        let step = orders.len().div_ceil(AUDIT_REPLAYS).max(1);
        let chosen = orders
            .into_iter()
            .step_by(step)
            .chain(receipts.iter().filter(|receipt| receipt.op == "pizza.tip"));
        for receipt in chosen {
            let op = format!("{}.replay", receipt.op);
            match self
                .client
                .call(&op, Class::Replay, &receipt.request, Instant::now())
                .await
            {
                Ok(answer) => self
                    .ledger
                    .replayed(compare(&receipt.op, &receipt.answer, &answer)),
                Err(error) => self.ledger.replayed(Some(Mismatch {
                    op: receipt.op.clone(),
                    detail: format!("the replay failed: {error}"),
                })),
            }
        }
    }

    /// Page `pizza.world` of every database, fresh, as the
    /// administrator, merged.
    pub async fn fetch_world(&self) -> Result<World, String> {
        let mut world = World::default();
        for db in self.databases() {
            let mut after = Value::Null;
            loop {
                let request = self.client.query(
                    &db,
                    "pizza.world",
                    &json!({"after": after, "limit": 500}),
                    Who::Admin,
                );
                let page = self
                    .client
                    .call("audit.world", Class::Control, &request, Instant::now())
                    .await
                    .map_err(|error| format!("reading the world of {db} failed: {error}"))?
                    .value;
                world.config = page["config"].clone();
                if after.is_null() {
                    world
                        .shops
                        .extend(page["shops"].as_array().cloned().unwrap_or_default());
                }
                world
                    .orders
                    .extend(page["orders"].as_array().cloned().unwrap_or_default());
                match &page["next"] {
                    Value::String(next) => after = Value::String(next.clone()),
                    _ => break,
                }
            }
        }
        Ok(world)
    }

    /// Each tenant's fresh leaderboard.
    pub async fn leaderboards(&self) -> Result<BTreeMap<String, Value>, String> {
        let mut boards = BTreeMap::new();
        for tenant in &self.tenants {
            let request = self.client.query(
                self.db(tenant),
                "pizza.leaderboard",
                &json!(tenant),
                Who::Tenant(tenant),
            );
            let answer = self
                .client
                .call(
                    "audit.leaderboard",
                    Class::Control,
                    &request,
                    Instant::now(),
                )
                .await
                .map_err(|error| format!("reading {tenant}'s leaderboard failed: {error}"))?;
            boards.insert(tenant.clone(), answer.value);
        }
        Ok(boards)
    }

    /// Every replica's applied index in this group once they agree (up
    /// to 10 s), and its replica-local copy of every store against the
    /// fresh rows.
    pub async fn replicas(&self, world: &World, stores: usize) -> Vec<ReplicaView> {
        let count = self.client.members().len();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut applied = vec![None; count];
        loop {
            for (member, slot) in applied.iter_mut().enumerate() {
                *slot = self
                    .client
                    .status(member, Duration::from_secs(1))
                    .await
                    .ok()
                    .and_then(|status| status.last_applied);
            }
            let agree = applied
                .iter()
                .all(|index| index.is_some() && *index == applied[0]);
            if agree || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let fresh: BTreeMap<&str, &Value> = world
            .shops
            .iter()
            .filter_map(|row| Some((row["key"].as_str()?, row)))
            .collect();
        let mut views = Vec::new();
        for (member, last_applied) in applied.into_iter().enumerate() {
            let mut view = ReplicaView {
                id: self.client.members()[member].id,
                last_applied,
                ..ReplicaView::default()
            };
            for (tenant, shop) in self.shops(stores) {
                let request = self.client.query(
                    self.db(&tenant),
                    "pizza.shop.local",
                    &shop,
                    Who::Tenant(&tenant),
                );
                match self
                    .client
                    .call_on(member, "audit.local", Class::Control, &request)
                    .await
                {
                    Ok(answer) => {
                        view.stores_checked += 1;
                        let key = serde_json::to_string(&shop).unwrap_or_default();
                        if fresh.get(key.as_str()) != Some(&&answer.value) {
                            view.stores_differing.push(key);
                        }
                    }
                    Err(error) => {
                        view.error = Some(error.to_string());
                        break;
                    }
                }
            }
            views.push(view);
        }
        views
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_answers_read_as_the_audit_expects() {
        let refused: Result<crate::client::Answer, CallError> = Err(CallError::Refused {
            kind: "http-403:unauthorized".to_string(),
            message: "FORBIDDEN: access denied".to_string(),
        });
        let probe = AccessProbe {
            probe: "x".to_string(),
            expected: "FORBIDDEN".to_string(),
            answered: answered(&refused),
        };
        assert!(probe.held());
        let business: Result<crate::client::Answer, CallError> = Err(CallError::Business {
            code: "SHOP_NOT_FOUND".to_string(),
            message: "no".to_string(),
        });
        let probe = AccessProbe {
            answered: answered(&business),
            ..probe
        };
        // Running the call at all is a failed rule, whatever it answered.
        assert!(!probe.held());
    }
}
