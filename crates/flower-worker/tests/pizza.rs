//! The Goblin Pizza example end to end: compiled to wasm32, deployed to
//! a live node, orders counted by an aggregate and baked by timers,
//! deliveries queued in each tenant's scope and delivered by drones that
//! are queue workers holding fenced leases, kitchens ranked by revenue
//! and tips.

mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use flower_client::client::{MutationOptions, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::{Claim, FlowerClient, QueuePoll};
use flower_cluster::node::Node;
use flower_worker::{QueueWorkerOptions, WorkError, run_queue_worker};
use serde_json::{Value, json};
use tokio::sync::Notify;

/// A drone's delivery: its order, under its lease.
fn delivery(claim: &Claim) -> Value {
    json!({"order": claim.payload["order"], "id": claim.id, "token": claim.token})
}

struct Kitchen {
    _dir: tempfile::TempDir,
    node: Arc<Node>,
    client: FlowerClient,
}

async fn open_kitchen() -> Kitchen {
    let (dir, node, client) = fixtures::deploy_example("goblin-pizza", "pizza").await;
    Kitchen {
        _dir: dir,
        node,
        client,
    }
}

impl Kitchen {
    async fn mutate(&self, name: &str, args: Value) -> Result<Value, FlowerError> {
        self.client
            .mutate_value("db", "pizza", name, &args, &MutationOptions::default())
            .await
            .map(|result| result.value)
    }

    async fn query(&self, name: &str, args: Value) -> Value {
        self.client
            .query_value("db", "pizza", name, &args, &RequestOptions::default())
            .await
            .unwrap()
            .value
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn goblin_pizza_bakes_delivers_and_ranks() {
    let kitchen = open_kitchen().await;
    let error = kitchen
        .mutate(
            "pizza.order",
            json!({"id": "o1", "shop": ["ogre", "store-0"], "quantity": 1}),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Business { ref code, .. } if code == "NOT_INITIALIZED"));
    kitchen
        .mutate(
            "pizza.setup",
            json!({"tenants": ["ogre", "troll"], "storesPerTenant": 2, "stockPerShop": 5, "bakeMs": 0}),
        )
        .await
        .unwrap();
    let error = kitchen
        .mutate(
            "pizza.setup",
            json!({"tenants": ["ogre"], "storesPerTenant": 1, "stockPerShop": 1, "bakeMs": 0}),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, FlowerError::Business { ref code, .. } if code == "ALREADY_INITIALIZED")
    );
    // Orders: three pizzas at one ogre store, one at another.
    for (id, store, quantity) in [
        ("o1", "store-0", 2),
        ("o2", "store-0", 1),
        ("o3", "store-1", 1),
    ] {
        let order = kitchen
            .mutate(
                "pizza.order",
                json!({"id": id, "shop": ["ogre", store], "quantity": quantity}),
            )
            .await
            .unwrap();
        assert_eq!(order["status"], "baking");
    }
    let error = kitchen
        .mutate(
            "pizza.order",
            json!({"id": "o1", "shop": ["ogre", "store-0"], "quantity": 1}),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Business { ref code, .. } if code == "ORDER_EXISTS"));
    let error = kitchen
        .mutate(
            "pizza.order",
            json!({"id": "o4", "shop": ["ogre", "store-0"], "quantity": 3}),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Business { ref code, .. } if code == "OUT_OF_STOCK"));
    // Ovens ring when timers sweep: every pizza is ready and queued.
    for _ in 0..50 {
        kitchen.node.sweep_timers_once().await.unwrap();
        let shop = kitchen
            .query("pizza.shop", json!(["ogre", "store-0"]))
            .await;
        if shop["ready"] == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The aggregate counted every order and followed each through the
    // oven: three pizzas ordered at store-0, two orders ready.
    let shop = kitchen
        .query("pizza.shop", json!(["ogre", "store-0"]))
        .await;
    assert_eq!(shop["ready"], 2);
    assert_eq!(shop["baking"], 0);
    assert_eq!(shop["orders"], 2);
    assert_eq!(shop["orderedQuantity"], 3);
    assert_eq!(shop["deliveredQuantity"], 0);
    assert_eq!(shop["stock"], 2);
    // Each delivery is a job named by its order in its tenant's scope.
    let options = RequestOptions::default();
    let scoped = |scope: &'static str| {
        let client = kitchen.client.clone();
        async move {
            client
                .queue_stats_in("db", "deliveries", Some(scope), &RequestOptions::default())
                .await
                .unwrap()
        }
    };
    assert_eq!(scoped("ogre").await.pending, 3);
    assert_eq!(scoped("troll").await.pending, 0);
    let o1 = r#"["ogre","store-0","o1"]"#;
    let job = kitchen
        .client
        .get_queue_job("db", "deliveries", "ogre", o1, &options)
        .await
        .unwrap()
        .expect("o1's delivery is queued under its order");
    assert_eq!(job.payload, json!({"order": ["ogre", "store-0", "o1"]}));
    // A drone whose lease ran out cannot collect coins: its delivery is
    // refused, and the store is not paid.
    let stale = kitchen
        .client
        .claim_queue(
            "db",
            "deliveries",
            &QueuePoll {
                scope: "ogre",
                owner: Some("slow-drone"),
                limit: Some(1),
                lease_ms: Some(100),
                wait_ms: None,
            },
            &options,
        )
        .await
        .unwrap()
        .remove(0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let error = kitchen
        .mutate("pizza.deliver", delivery(&stale))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, FlowerError::Business { code, .. } if code == "LEASE_LOST"),
        "{error:?}"
    );
    // Nor can a drone deliver another order than the one it carries.
    let mut wrong = delivery(&stale);
    wrong["order"] = if stale.payload["order"][2] == "o3" {
        json!(["ogre", "store-0", "o1"])
    } else {
        json!(["ogre", "store-1", "o3"])
    };
    let error = kitchen.mutate("pizza.deliver", wrong).await.unwrap_err();
    assert!(
        matches!(&error, FlowerError::Business { code, .. } if code == "LEASE_LOST"),
        "{error:?}"
    );
    let unpaid = kitchen
        .query("pizza.shop", json!(["ogre", "store-0"]))
        .await;
    assert_eq!(unpaid["revenue"], 0);
    // Drones: queue workers on the ogre scope, waiting in its line and
    // chaining each completion to their next claim, delivering each
    // claimed pizza under its lease. Every delivery is redelivered once
    // (as after a crash) and pays once.
    let stop = Arc::new(Notify::new());
    let drone_client = kitchen.client.clone();
    let drone = tokio::spawn(run_queue_worker(
        kitchen.client.clone(),
        QueueWorkerOptions::new("db", "deliveries")
            .scope("ogre")
            .owner("drone-1")
            .adaptive(1, 1, 4)
            .wait(Duration::from_secs(30))
            .chain()
            .drain(Duration::from_secs(10))
            .poll_interval(Duration::from_millis(20)),
        stop.clone(),
        move |claim| {
            let client = drone_client.clone();
            async move {
                let options = MutationOptions::retrying();
                let args = delivery(&claim);
                let deliver =
                    || client.mutate_value("db", "pizza", "pizza.deliver", &args, &options);
                deliver()
                    .await
                    .map_err(|error| WorkError::Terminal(error.to_string()))?;
                deliver()
                    .await
                    .map_err(|error| WorkError::Terminal(error.to_string()))?;
                Ok(())
            }
        },
    ));
    for _ in 0..250 {
        let shop = kitchen
            .query("pizza.shop", json!(["ogre", "store-1"]))
            .await;
        let other = kitchen
            .query("pizza.shop", json!(["ogre", "store-0"]))
            .await;
        if shop["delivered"] == 1 && other["delivered"] == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.notify_one();
    let summary = drone.await.unwrap().unwrap();
    assert_eq!(summary.completed, 3);
    assert_eq!(summary.killed, 0);
    let first = kitchen
        .query("pizza.shop", json!(["ogre", "store-0"]))
        .await;
    let second = kitchen
        .query("pizza.shop", json!(["ogre", "store-1"]))
        .await;
    assert_eq!(first["revenue"], 21.0);
    assert_eq!(second["revenue"], 7.0);
    assert_eq!(first["stock"], 2.0);
    assert_eq!(first["delivered"], 2);
    assert_eq!(first["ready"], 0);
    assert_eq!(first["deliveredQuantity"], 3);
    let drained = scoped("ogre").await;
    assert_eq!((drained.pending, drained.claimed, drained.dead), (0, 0, 0));
    // The stopping drone gave up its place in the ogre line: the next
    // pizza is any drone's at once, not the departed one's for a turn.
    kitchen
        .mutate(
            "pizza.order",
            json!({"id": "o5", "shop": ["ogre", "store-1"], "quantity": 1}),
        )
        .await
        .unwrap();
    for _ in 0..50 {
        kitchen.node.sweep_timers_once().await.unwrap();
        if scoped("ogre").await.pending == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let readiness = kitchen
        .client
        .queue_readiness("db", "deliveries", "ogre", Some("drone-2"), &options)
        .await
        .unwrap();
    assert!(readiness.ready, "{readiness:?}");
    // The world's store summaries are the aggregate's counts over the
    // source orders.
    let world = kitchen.query("pizza.world", json!({})).await;
    let shops = world["shops"].as_array().unwrap();
    assert_eq!(shops.len(), 4);
    let count =
        |field: &str| -> u64 { shops.iter().map(|shop| shop[field].as_u64().unwrap()).sum() };
    assert_eq!(
        count("orders"),
        world["orders"].as_array().unwrap().len() as u64
    );
    assert_eq!((count("delivered"), count("ready")), (3, 1));
    // The leaderboard ranks by revenue plus tips; a big tip reorders it.
    let board = kitchen.query("pizza.leaderboard", json!("ogre")).await;
    let order: Vec<&str> = board
        .as_array()
        .unwrap()
        .iter()
        .map(|shop| shop["name"].as_str().unwrap())
        .collect();
    assert!(order[0].ends_with("store-0"), "{order:?}");
    kitchen
        .mutate(
            "pizza.tip",
            json!({"shop": ["ogre", "store-1"], "amount": 100}),
        )
        .await
        .unwrap();
    let board = kitchen.query("pizza.leaderboard", json!("ogre")).await;
    assert!(board[0]["name"].as_str().unwrap().ends_with("store-1"));
    assert_eq!(board.as_array().unwrap().len(), 2);
    // The oven bell is internal: not an HTTP alias.
    let error = kitchen
        .mutate(
            "pizza.finishBaking",
            json!({"order": ["ogre", "store-0", "o1"]}),
        )
        .await
        .unwrap_err();
    assert!(error.code() == Some("UNKNOWN_METHOD"), "{error:?}");
}
