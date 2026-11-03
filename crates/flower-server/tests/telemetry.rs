//! OpenTelemetry export end to end: members serving HTTP export their
//! request spans, joined to the caller's trace, and their HTTP metrics
//! to an OTLP/HTTP collector, with v1's query, writer, evaluator,
//! storage, Raft and watch metric families and spans, and never a
//! credential, argument or stored value. Its own test binary: the
//! subscriber and meter provider are process-wide.

mod fixtures;

use fixtures::{hc, s};

use std::collections::{BTreeSet, HashSet};
use std::future::IntoFuture;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::routing::post;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Value, fail};
use flower_server::api::{AppState, serve};
use flower_server::telemetry::{OtelConfig, init_with};
use opentelemetry_otlp::Protocol;
use opentelemetry_sdk::trace::Sampler;
use serde_json::Value as Json;

/// Answers `ping` with `pong`, `put {key, title}` and `get {key}`.
struct App;

impl RuntimeFactory for App {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                let field = |name: &str| match args {
                    Value::Object(map) => map.get(name).cloned().unwrap_or(Value::Null),
                    _ => Value::Null,
                };
                match (kind, name) {
                    (CallKind::Query, "ping") => Ok(s("pong")),
                    (CallKind::Mutation, "put") => {
                        let mut row = std::collections::BTreeMap::new();
                        row.insert("title".to_string(), field("title"));
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("tasks"), field("key"), Value::Object(row)],
                        )?;
                        Ok(s("put-ok"))
                    }
                    (CallKind::Query, "get") => {
                        hc(host, HostOp::Get, vec![s(""), s("tasks"), field("key")])
                    }
                    // Maintenance task `tick`: due until it ran once.
                    (CallKind::Query, "tick") => {
                        let ran = hc(host, HostOp::Get, vec![s(""), s("tasks"), s("\"ticked\"")])?;
                        Ok(match ran {
                            Value::Null => Value::Number(0.0),
                            _ => Value::Null,
                        })
                    }
                    (CallKind::Mutation, "tick") => {
                        let mut row = std::collections::BTreeMap::new();
                        row.insert("title".to_string(), s("ticked"));
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("tasks"), s("\"ticked\""), Value::Object(row)],
                        )?;
                        Ok(Value::Null)
                    }
                    _ => Err::<Value, Failure>(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

/// What the collector received: `(path, body)`.
type Received = Arc<Mutex<Vec<(String, String)>>>;

async fn collect(
    State(received): State<Received>,
    uri: axum::http::Uri,
    body: String,
) -> &'static str {
    received
        .lock()
        .unwrap()
        .push((uri.path().to_string(), body));
    "{}"
}

/// The `service.name` of the export that carried span `span_id`.
fn service_of(received: &Received, span_id: &str) -> Option<Json> {
    for (path, body) in received.lock().unwrap().iter() {
        if path != "/v1/traces" {
            continue;
        }
        let body: Json = serde_json::from_str(body).unwrap();
        for resource in body["resourceSpans"].as_array().into_iter().flatten() {
            let carries = resource["scopeSpans"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(|scope| scope["spans"].as_array().into_iter().flatten())
                .any(|span| span["spanId"] == span_id);
            if carries {
                return attribute(&resource["resource"], "service.name").cloned();
            }
        }
    }
    None
}

/// Every data point of metric `name` exported so far.
fn data_points(received: &Received, name: &str) -> Vec<Json> {
    let mut points = Vec::new();
    for (path, body) in received.lock().unwrap().iter() {
        if path != "/v1/metrics" {
            continue;
        }
        let body: Json = serde_json::from_str(body).unwrap();
        for resource in body["resourceMetrics"].as_array().into_iter().flatten() {
            for scope in resource["scopeMetrics"].as_array().into_iter().flatten() {
                for metric in scope["metrics"].as_array().into_iter().flatten() {
                    if metric["name"] != name {
                        continue;
                    }
                    for kind in ["sum", "gauge", "histogram"] {
                        points.extend(
                            metric[kind]["dataPoints"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .cloned(),
                        );
                    }
                }
            }
        }
    }
    points
}

/// Wait until a data point of metric `name` that `pick` accepts was
/// exported.
async fn point_where(received: &Received, name: &str, pick: impl Fn(&Json) -> bool) -> Json {
    for _ in 0..300 {
        if let Some(point) = data_points(received, name)
            .into_iter()
            .find(|point| pick(point))
        {
            return point;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "no {name} data point as expected among {:#?}",
        data_points(received, name)
    );
}

/// Every span exported so far.
fn spans(received: &Received) -> Vec<Json> {
    let mut spans = Vec::new();
    for (path, body) in received.lock().unwrap().iter() {
        if path != "/v1/traces" {
            continue;
        }
        let body: Json = serde_json::from_str(body).unwrap();
        for resource in body["resourceSpans"].as_array().into_iter().flatten() {
            for scope in resource["scopeSpans"].as_array().into_iter().flatten() {
                spans.extend(scope["spans"].as_array().into_iter().flatten().cloned());
            }
        }
    }
    spans
}

/// Every metric name exported so far.
fn metric_names(received: &Received) -> HashSet<String> {
    let mut names = HashSet::new();
    for (path, body) in received.lock().unwrap().iter() {
        if path != "/v1/metrics" {
            continue;
        }
        let body: Json = serde_json::from_str(body).unwrap();
        for resource in body["resourceMetrics"].as_array().into_iter().flatten() {
            for scope in resource["scopeMetrics"].as_array().into_iter().flatten() {
                for metric in scope["metrics"].as_array().into_iter().flatten() {
                    names.insert(metric["name"].as_str().unwrap().to_string());
                }
            }
        }
    }
    names
}

/// A span attribute's value, as JSON (`{"stringValue": ..}` and the
/// like), when the span has it.
fn attribute<'a>(span: &'a Json, key: &str) -> Option<&'a Json> {
    span["attributes"]
        .as_array()?
        .iter()
        .find(|attribute| attribute["key"] == key)
        .map(|attribute| &attribute["value"])
}

/// Wait until a span `name` that `pick` accepts was exported.
async fn span_where(received: &Received, name: &str, pick: impl Fn(&Json) -> bool) -> Json {
    for _ in 0..300 {
        if let Some(span) = spans(received)
            .into_iter()
            .find(|span| span["name"] == name && pick(span))
        {
            return span;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "no {name} span as expected among {:#?}",
        names_of(&spans(received))
    );
}

fn names_of(spans: &[Json]) -> BTreeSet<String> {
    spans
        .iter()
        .map(|span| span["name"].as_str().unwrap().to_string())
        .collect()
}

fn manifest() -> serde_json::Value {
    serde_json::json!({
        "collections": [{
            "name": "tasks",
            "key": {"type": "string"},
            "value": {"type": "object", "fields": [
                {"name": "title", "schema": {"type": "string"}, "optional": false}
            ]},
            "indexes": [],
            "references": []
        }],
        "definitions": [
            {"name": "ping", "kind": "query", "args": {"type": "json"}},
            {"name": "put", "kind": "mutation", "args": {"type": "json"}},
            {"name": "get", "kind": "query", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": [
            {"alias": "ping", "definition": "ping"},
            {"alias": "put", "definition": "put"},
            {"alias": "get", "definition": "get"}
        ],
        "tasks": [{"name": "tick", "on_error": false}],
    })
}

/// Read the next SSE event, skipping comments.
async fn next_event(response: &mut reqwest::Response, buffered: &mut String) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(end) = buffered.find("\n\n") {
                let event: String = buffered.drain(..end + 2).collect();
                if event.starts_with(':') {
                    continue;
                }
                return event;
            }
            let chunk = response.chunk().await.unwrap().expect("the stream ended");
            buffered.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .expect("no event in time")
}

#[tokio::test(flavor = "multi_thread")]
async fn members_export_v1s_spans_and_metric_families_and_no_secret() {
    let received = Received::default();
    let collector = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", collector.local_addr().unwrap());
    tokio::spawn(
        axum::serve(
            collector,
            Router::new()
                .route("/v1/traces", post(collect))
                .route("/v1/metrics", post(collect))
                .with_state(received.clone()),
        )
        .into_future(),
    );
    let config = OtelConfig {
        enabled: true,
        endpoint: Some(endpoint),
        protocol: Protocol::HttpJson,
        sampler: Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        service_name: "flower-test".to_string(),
        metric_interval: Some(Duration::from_millis(200)),
        batch_delay: Some(Duration::from_millis(50)),
    };
    // The exporters' blocking HTTP clients stay off the async workers.
    let telemetry = tokio::task::spawn_blocking(move || init_with(&config, 1))
        .await
        .unwrap()
        .unwrap();
    assert!(telemetry.enabled());

    // Two members: Raft RPCs go between them.
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut nodes = Vec::new();
    for id in 1..=2u64 {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            Arc::new(openraft::Config::default()),
            InProcessFactory::new(registry.clone()).with_source(id),
            Arc::new(App),
        )
        .await
        .unwrap();
        registry.register_peer(node.peer_service());
        nodes.push(Arc::new(node));
    }
    nodes[0].initialize(BTreeSet::from([1, 2])).await.unwrap();
    let mut leader = None;
    for _ in 0..500 {
        if let Some(id) = nodes[0].status().leader
            && nodes[1].status().leader == Some(id)
        {
            leader = Some(id);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let leader = nodes[(leader.expect("no leader") - 1) as usize].clone();
    // The leader's maintenance loop runs the app's task once deployed.
    let (_stop_tasks, tasks_stopped) = tokio::sync::watch::channel(false);
    {
        let leader = leader.clone();
        tokio::spawn(async move {
            leader
                .run_tasks(Duration::from_secs(60), tasks_stopped)
                .await
        });
    }
    let state = AppState::new(leader.clone()).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let http = reqwest::Client::new();
    let deployed = http
        .post(format!("{base}/v1/deploy"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": "db", "module": "app", "identity": null, "wasm_b64": "e30=",
            "manifest": manifest(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(deployed.status(), 200);

    let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
    let answer = http
        .post(format!("{base}/v1/query"))
        .header("traceparent", format!("00-{trace}-00f067aa0ba902b7-01"))
        .header("authorization", "Bearer end-user-secret")
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "ping", "args": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(answer.status(), 200);

    // The request's span joined the caller's trace, under its span. Its
    // children end first, so an export can hold them without it: match
    // the span itself, not the export.
    let request = span_where(&received, "flower.http.request", |span| {
        span["traceId"] == trace
    })
    .await;
    assert_eq!(request["parentSpanId"], "00f067aa0ba902b7", "{request:#}");
    let route = serde_json::json!({"stringValue": "/v1/query"});
    assert_eq!(
        attribute(&request, "http.route"),
        Some(&route),
        "{request:#}"
    );
    assert_eq!(
        service_of(&received, request["spanId"].as_str().unwrap()),
        Some(serde_json::json!({"stringValue": "flower-test"}))
    );
    // HTTP metrics export too, by route and outcome.
    point_where(&received, "flower.http.server.request.duration", |point| {
        attribute(point, "http.route") == Some(&route) && attribute(point, "outcome").is_some()
    })
    .await;
    point_where(&received, "flower.http.server.active_requests", |point| {
        attribute(point, "http.route") == Some(&route)
    })
    .await;

    // A watch in a sampled trace, then a write it sees.
    let watch_trace = "0af7651916cd43dd8448eb211c80319c";
    let args: serde_json::Value =
        serde_json::from_slice(b"{\"key\":\"\\\"classified-key\\\"\"}").unwrap();
    let mut watch = http
        .post(format!("{base}/v1/watch"))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "get", "args": args}))
        .header(
            "traceparent",
            format!("00-{watch_trace}-b7ad6b7169203331-01"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(watch.status(), 200);
    let mut buffered = String::new();
    next_event(&mut watch, &mut buffered).await;
    let write_trace = "11111111111111111111111111111111";
    let put = http
        .post(format!("{base}/v1/mutate"))
        .header(
            "traceparent",
            format!("00-{write_trace}-2222222222222222-01"),
        )
        .header("authorization", "Bearer end-user-secret")
        .json(&serde_json::json!({
            "db": "db", "module": "app", "name": "put",
            "args": {"key": "\"classified-key\"", "title": "classified-title"},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200);
    let pushed = next_event(&mut watch, &mut buffered).await;
    assert!(pushed.contains("classified-title"), "{pushed}");
    // The same query twice: the second is a cache hit.
    for _ in 0..2 {
        let get = http
            .post(format!("{base}/v1/query"))
            .json(&serde_json::json!({
                "db": "db", "module": "app", "name": "get",
                "args": {"key": "\"classified-key\""},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(get.status(), 200);
    }
    // A snapshot build.
    leader.raft().trigger().snapshot().await.unwrap();

    // The write: its submit span under its request, in a batch that
    // links it, prepared, committed and replicated, the commit split as
    // `Server-Timing` splits it.
    let submit = span_where(&received, "flower.writer.submit", |span| {
        span["traceId"] == write_trace
    })
    .await;
    let batch = span_where(&received, "flower.writer.batch", |span| {
        span["links"]
            .as_array()
            .is_some_and(|links| links.iter().any(|link| link["spanId"] == submit["spanId"]))
    })
    .await;
    assert_ne!(
        batch["traceId"], write_trace,
        "a batch starts its own trace"
    );
    assert_eq!(attribute(&batch, "requests").unwrap()["intValue"], "1");
    // Alone it goes once prepared ("idle"); if a background write was
    // still committing, it went when that one resolved.
    let stop_reason = attribute(&batch, "stop_reason").unwrap()["stringValue"].clone();
    assert!(
        stop_reason == "idle" || stop_reason == "resolved",
        "{stop_reason}"
    );
    assert_eq!(
        attribute(&batch, "flower.group").unwrap()["stringValue"],
        "root"
    );
    span_where(&received, "flower.writer.prepare", |span| {
        span["traceId"] == batch["traceId"]
    })
    .await;
    let commit = span_where(&received, "flower.writer.commit", |span| {
        span["traceId"] == batch["traceId"]
    })
    .await;
    let replicate = span_where(&received, "flower.writer.replicate", |span| {
        span["parentSpanId"] == commit["spanId"]
    })
    .await;
    for key in [
        "flower.raft.log_index",
        "flower.raft.append_ms",
        "flower.raft.replicate_ms",
        "flower.raft.apply_ms",
    ] {
        assert!(attribute(&replicate, key).is_some(), "{key}: {replicate:#}");
    }
    let events: BTreeSet<&str> = replicate["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        events,
        BTreeSet::from(["appended", "committed", "applied"]),
        "{replicate:#}"
    );
    // The evaluator, inside the batch's preparation and the query's
    // evaluation stage.
    let invocation = span_where(&received, "flower.evaluator.invocation", |span| {
        span["traceId"] == batch["traceId"]
    })
    .await;
    assert_eq!(
        attribute(&invocation, "mode").unwrap()["stringValue"],
        "mutation"
    );
    assert_eq!(attribute(&invocation, "writes").unwrap()["intValue"], "1");
    let stage = span_where(&received, "flower.query.stage", |span| {
        span["traceId"] == trace
            && attribute(span, "stage").is_some_and(|stage| stage["stringValue"] == "evaluation")
    })
    .await;
    span_where(&received, "flower.evaluator.invocation", |span| {
        span["parentSpanId"] == stage["spanId"]
    })
    .await;
    span_where(&received, "flower.evaluator.bundle.prepare", |_| true).await;
    // Storage, Raft RPCs and the snapshot build.
    span_where(&received, "flower.storage.operation", |span| {
        attribute(span, "operation").is_some_and(|operation| operation["stringValue"] == "append")
    })
    .await;
    span_where(&received, "flower.storage.operation", |span| {
        attribute(span, "operation").is_some_and(|operation| operation["stringValue"] == "apply")
    })
    .await;
    span_where(&received, "flower.raft.rpc", |span| {
        attribute(span, "operation").is_some_and(|operation| operation["stringValue"] == "append")
    })
    .await;
    span_where(&received, "flower.raft.snapshot", |span| {
        attribute(span, "outcome").is_some_and(|outcome| outcome["stringValue"] == "published")
    })
    .await;
    // The watch's delivery, under its request's trace, linking the
    // batch that published the revision it carries.
    let delivery = span_where(&received, "flower.watch.deliver", |span| {
        span["traceId"] == watch_trace
    })
    .await;
    assert_eq!(
        attribute(&delivery, "outcome").unwrap()["stringValue"],
        "delivered"
    );
    assert!(
        delivery["links"]
            .as_array()
            .is_some_and(|links| links.iter().any(|link| link["spanId"] == batch["spanId"])),
        "{delivery:#}"
    );

    // The write's body reception and JSON extraction, inside its request.
    for stage in ["body_receive", "json_extract"] {
        span_where(&received, "flower.http.stage", |span| {
            span["traceId"] == write_trace
                && attribute(span, "stage").is_some_and(|name| name["stringValue"] == stage)
        })
        .await;
    }

    // Every family's metrics, labelled by group.
    let families = [
        "flower.http.server.stage.duration",
        "flower.query.stage.duration",
        "flower.query.cache.lookups",
        "flower.writer.stage.duration",
        "flower.writer.request.duration",
        "flower.writer.requests",
        "flower.writer.batches",
        "flower.writer.batch.size",
        "flower.writer.batch.bytes",
        "flower.writer.queue.depth",
        "flower.writer.replication.lag",
        "flower.writer.maintenance.runs",
        "flower.writer.maintenance.wakes",
        "flower.evaluator.invocations",
        "flower.evaluator.invocation.duration",
        "flower.evaluator.stage.duration",
        "flower.evaluator.work.count",
        "flower.evaluator.bundle.preparation.duration",
        "flower.storage.operations",
        "flower.storage.operation.duration",
        "flower.storage.stage.duration",
        "flower.storage.batch.size",
        "flower.storage.encoded.bytes",
        "flower.raft.rpc.duration",
        "flower.raft.snapshot.duration",
        "flower.raft.snapshot.in_progress",
        "flower.watch.delivery.duration",
    ];
    let mut missing = Vec::new();
    for _ in 0..300 {
        let names = metric_names(&received);
        missing = families
            .iter()
            .filter(|family| !names.contains(**family))
            .collect();
        if missing.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(missing.is_empty(), "never exported: {missing:?}");
    let hit = serde_json::json!({"stringValue": "hit"});
    let lookup = point_where(&received, "flower.query.cache.lookups", |point| {
        attribute(point, "outcome") == Some(&hit)
    })
    .await;
    assert!(attribute(&lookup, "flower.group").is_some(), "{lookup:#}");

    // Credentials, arguments and stored values never leave the process.
    for (_, body) in received.lock().unwrap().iter() {
        for secret in [
            "end-user-secret",
            "Bearer",
            "classified-key",
            "classified-title",
        ] {
            assert!(!body.contains(secret), "{secret} exported: {body}");
        }
    }
    drop(watch);
    tokio::time::timeout(Duration::from_secs(10), telemetry.shutdown())
        .await
        .unwrap()
        .unwrap();
}
