//! `--otel`: the servers' OpenTelemetry, captured and reported (v1's
//! `bench/profile-otel.mjs`, `otel-capture.mjs` and `otel-report.mjs`).
//!
//! The bench starts a bounded OTLP/HTTP JSON receiver on 127.0.0.1
//! (`/v1/traces`, `/v1/metrics`; no collector service, nothing leaves
//! the machine) and starts every server with `FLOWER_OTEL_ENABLED=1`
//! pointed at it: OTLP/HTTP JSON, parent-based trace sampling at
//! `--otel-sample` (1% by default), one-second metric exports and span
//! batches. Inherited exporter endpoints, headers and resource
//! attributes are removed from the servers' environment for the run.
//!
//! **What is kept.** Each record is sanitized as it arrives: resource
//! identity (service, instance, node ID, PID), span name, trace, span
//! and parent IDs, times, and the attributes Flower's spans carry
//! (`http.route`, `http.request.method`, `http.response.status_code`,
//! `outcome`, `flower.node.id`, `flower.raft.leader_id`); metric points
//! of `flower.http.server.request.duration` (bucket counts by route,
//! method, status and outcome) and `flower.http.server.active_requests`.
//! Bodies, headers, events, links and every other attribute are
//! dropped. At most `FLOWER_BENCH_OTEL_MAX_RECORDS` records (100,000;
//! up to 1,000,000) and `FLOWER_BENCH_OTEL_MAX_BYTES` (256 MiB; up to
//! 1 GiB) are kept, requests are limited to 4 MiB and eight are read
//! at once; exceeding a bound fails the diagnostic.
//!
//! **What is reported.** Span durations only for spans wholly inside the
//! measured window; cumulative metrics differenced between adjacent
//! exports of the same process and series inside the window (resets
//! and edges excluded), merged across nodes by bucket; percentiles are
//! bucket upper bounds. Received, kept, refused and malformed counts;
//! the slowest sampled spans with their trace IDs; every process seen
//! (a restarted member is a new one). Missing spans or metrics, no
//! metric interval inside the window, malformed requests or exceeded
//! bounds fail the run, independently of the business audit. Exporter
//! queue loss and spans lost when a member is killed are invisible to
//! the receiver and reported as unknown.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::routing::post;
use flower_client::CancellationToken;
use serde::Serialize;
use serde_json::{Value, json};

use crate::histogram::{Histogram, Percentiles};
use crate::report::{count, esc, ms};

/// Largest request body read.
const MAX_REQUEST: usize = 4 * 1_048_576;
/// Requests read at once.
const CONCURRENT: usize = 8;
/// The request-duration histogram the servers export.
pub const DURATION: &str = "flower.http.server.request.duration";
/// The in-flight gauge.
pub const ACTIVE: &str = "flower.http.server.active_requests";
/// Span attributes kept.
const SPAN_ATTRIBUTES: [&str; 6] = [
    "http.route",
    "http.request.method",
    "http.response.status_code",
    "outcome",
    "flower.node.id",
    "flower.raft.leader_id",
];
/// Metric attributes kept.
const POINT_ATTRIBUTES: [&str; 4] = [
    "http.route",
    "http.request.method",
    "http.response.status_code",
    "outcome",
];

/// Capture bounds.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Limits {
    /// Most records kept.
    pub max_records: usize,
    /// Most encoded bytes accepted.
    pub max_bytes: usize,
}

impl Limits {
    /// 100,000 records, as v1, and 256 MiB: v2's servers export the
    /// writer, storage, Raft and evaluator spans and metric families
    /// too, and a four-second smoke run with every request traced sent
    /// 67 MB, past v1's 64 MiB.
    pub const DEFAULT: Limits = Limits {
        max_records: 100_000,
        max_bytes: 256 * 1_048_576,
    };

    /// The bounds `FLOWER_BENCH_OTEL_MAX_RECORDS` and `_MAX_BYTES` set.
    pub fn from_env() -> Result<Limits, String> {
        Limits::load(|name| std::env::var(name).ok())
    }

    /// The bounds the variables `get` reads set.
    pub fn load(get: impl Fn(&str) -> Option<String>) -> Result<Limits, String> {
        let read = |name: &str, default: usize, cap: usize| -> Result<usize, String> {
            match get(name) {
                None => Ok(default),
                Some(text) => text
                    .trim()
                    .parse::<usize>()
                    .ok()
                    .filter(|value| (1..=cap).contains(value))
                    .ok_or_else(|| format!("{name} must be a whole number from 1 to {cap}")),
            }
        };
        Ok(Limits {
            max_records: read(
                "FLOWER_BENCH_OTEL_MAX_RECORDS",
                Limits::DEFAULT.max_records,
                1_000_000,
            )?,
            max_bytes: read(
                "FLOWER_BENCH_OTEL_MAX_BYTES",
                Limits::DEFAULT.max_bytes,
                1_024 * 1_048_576,
            )?,
        })
    }
}

/// One exporting process.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    /// `service.instance.id`: `<node>/<pid>`.
    pub instance: String,
    /// `service.name`.
    pub service: String,
    /// `flower.node.id`.
    pub node: Option<u64>,
    /// `process.pid`.
    pub pid: Option<u64>,
    /// `service.version`.
    pub version: Option<String>,
}

/// One sanitized span.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    /// Its process.
    pub instance: String,
    /// Span name.
    pub name: String,
    /// Trace ID, hex.
    pub trace_id: String,
    /// Span ID, hex.
    pub span_id: String,
    /// Parent span ID, if any.
    pub parent: Option<String>,
    /// Start, Unix ns.
    pub start_ns: u128,
    /// End, Unix ns.
    pub end_ns: u128,
    /// The kept attributes.
    pub attributes: BTreeMap<String, String>,
    /// OTel status code: 0 unset, 1 ok, 2 error.
    pub status: i64,
}

/// One sanitized metric point.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Point {
    /// Its process.
    pub instance: String,
    /// The instrument.
    pub metric: String,
    /// The kept attributes.
    pub attributes: BTreeMap<String, String>,
    /// Series start, Unix ns (cumulative series restart with it).
    pub start_ns: u128,
    /// Point time, Unix ns.
    pub time_ns: u128,
    /// Whether the series is cumulative (else delta).
    pub cumulative: bool,
    /// Histogram count.
    pub count: u64,
    /// Histogram bucket upper bounds.
    pub bounds: Vec<f64>,
    /// Histogram bucket counts (one more than the bounds).
    pub buckets: Vec<u64>,
    /// A gauge's or sum's value.
    pub value: Option<f64>,
}

/// What the receiver took in.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capture {
    /// Its bounds.
    pub limits: Option<Limits>,
    /// Export requests received.
    pub requests: u64,
    /// Requests refused: too large, or past the byte bound.
    pub refused: u64,
    /// Requests that were not OTLP JSON.
    pub malformed: u64,
    /// Encoded bytes accepted.
    pub bytes: u64,
    /// Records past the record bound, dropped.
    pub dropped: u64,
    /// Every process seen, by instance.
    pub resources: BTreeMap<String, Resource>,
    /// Kept spans.
    pub spans: Vec<Span>,
    /// Kept metric points.
    pub points: Vec<Point>,
    /// Points of instruments other than Flower's HTTP ones, not kept.
    pub other_points: u64,
}

struct Shared {
    capture: Mutex<Capture>,
    limits: Limits,
    reading: tokio::sync::Semaphore,
}

/// A running OTLP/HTTP JSON receiver.
pub struct Receiver {
    addr: SocketAddr,
    stop: CancellationToken,
    shared: Arc<Shared>,
    task: tokio::task::JoinHandle<()>,
}

impl Receiver {
    /// Listen on any free port of 127.0.0.1.
    pub async fn start(limits: Limits) -> Result<Receiver, String> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|error| format!("OpenTelemetry receiver: {error}"))?;
        let addr = listener.local_addr().map_err(|error| error.to_string())?;
        let shared = Arc::new(Shared {
            capture: Mutex::new(Capture {
                limits: Some(limits),
                ..Capture::default()
            }),
            limits,
            reading: tokio::sync::Semaphore::new(CONCURRENT),
        });
        let router = Router::new()
            .route("/v1/traces", post(traces))
            .route("/v1/metrics", post(metrics))
            .layer(DefaultBodyLimit::max(MAX_REQUEST))
            .with_state(shared.clone());
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await;
        });
        Ok(Receiver {
            addr,
            stop,
            shared,
            task,
        })
    }

    /// The base URL the servers export to.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Stop listening and hand over what was captured.
    pub async fn stop(self) -> Capture {
        self.stop.cancel();
        let _ = self.task.await;
        self.shared
            .capture
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

async fn traces(State(shared): State<Arc<Shared>>, body: Bytes) -> (StatusCode, &'static str) {
    accept(&shared, &body, Signal::Traces).await
}

async fn metrics(State(shared): State<Arc<Shared>>, body: Bytes) -> (StatusCode, &'static str) {
    accept(&shared, &body, Signal::Metrics).await
}

#[derive(Clone, Copy)]
enum Signal {
    Traces,
    Metrics,
}

async fn accept(shared: &Shared, body: &[u8], signal: Signal) -> (StatusCode, &'static str) {
    let Ok(_permit) = shared.reading.acquire().await else {
        return (StatusCode::SERVICE_UNAVAILABLE, "{}");
    };
    let mut capture = shared
        .capture
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    capture.requests += 1;
    if capture.bytes as usize + body.len() > shared.limits.max_bytes {
        capture.refused += 1;
        return (StatusCode::PAYLOAD_TOO_LARGE, "{}");
    }
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        capture.malformed += 1;
        return (StatusCode::BAD_REQUEST, "{}");
    };
    capture.bytes += body.len() as u64;
    let parsed = match signal {
        Signal::Traces => ingest_traces(&value),
        Signal::Metrics => ingest_metrics(&value),
    };
    let Some(parsed) = parsed else {
        capture.malformed += 1;
        return (StatusCode::BAD_REQUEST, "{}");
    };
    for resource in parsed.resources {
        // Signals of one process may carry different attributes: keep
        // whatever any of them named.
        let known = capture
            .resources
            .entry(resource.instance.clone())
            .or_insert_with(|| resource.clone());
        if known.service.is_empty() {
            known.service = resource.service;
        }
        known.node = known.node.or(resource.node);
        known.pid = known.pid.or(resource.pid);
        known.version = known.version.take().or(resource.version);
    }
    capture.other_points += parsed.other_points;
    let room = |capture: &Capture| {
        shared
            .limits
            .max_records
            .saturating_sub(capture.spans.len() + capture.points.len())
    };
    for span in parsed.spans {
        if room(&capture) == 0 {
            capture.dropped += 1;
        } else {
            capture.spans.push(span);
        }
    }
    for point in parsed.points {
        if room(&capture) == 0 {
            capture.dropped += 1;
        } else {
            capture.points.push(point);
        }
    }
    (StatusCode::OK, "{}")
}

/// What one export request held.
#[derive(Default)]
struct Parsed {
    resources: Vec<Resource>,
    spans: Vec<Span>,
    points: Vec<Point>,
    other_points: u64,
}

/// An OTLP JSON integer: a string or a number.
fn int(value: &Value) -> Option<u128> {
    match value {
        Value::String(text) => text.parse().ok(),
        Value::Number(number) => number.as_u64().map(u128::from),
        _ => None,
    }
}

/// An OTLP `AnyValue` as text.
fn any(value: &Value) -> Option<String> {
    let value = value.as_object()?;
    if let Some(text) = value.get("stringValue").and_then(Value::as_str) {
        return Some(text.to_string());
    }
    if let Some(number) = value.get("intValue") {
        return int(number)
            .map(|n| n.to_string())
            .or_else(|| number.as_i64().map(|n| n.to_string()));
    }
    if let Some(number) = value.get("doubleValue").and_then(Value::as_f64) {
        return Some(number.to_string());
    }
    value
        .get("boolValue")
        .and_then(Value::as_bool)
        .map(|flag| flag.to_string())
}

/// Kept attributes of an OTLP attribute list.
fn attributes(list: &Value, kept: &[&str]) -> BTreeMap<String, String> {
    list.as_array()
        .into_iter()
        .flatten()
        .filter_map(|attribute| {
            let key = attribute["key"].as_str()?;
            kept.contains(&key)
                .then(|| any(&attribute["value"]).map(|value| (key.to_string(), value)))?
        })
        .collect()
}

fn resource(value: &Value) -> Resource {
    let all = attributes(
        &value["attributes"],
        &[
            "service.instance.id",
            "service.name",
            "flower.node.id",
            "process.pid",
            "service.version",
        ],
    );
    Resource {
        instance: all
            .get("service.instance.id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
        service: all.get("service.name").cloned().unwrap_or_default(),
        node: all.get("flower.node.id").and_then(|id| id.parse().ok()),
        pid: all.get("process.pid").and_then(|pid| pid.parse().ok()),
        version: all.get("service.version").cloned(),
    }
}

fn ingest_traces(value: &Value) -> Option<Parsed> {
    let mut parsed = Parsed::default();
    for resource_spans in value.get("resourceSpans")?.as_array()? {
        let owner = resource(&resource_spans["resource"]);
        for scope in resource_spans["scopeSpans"]
            .as_array()
            .into_iter()
            .flatten()
        {
            for span in scope["spans"].as_array().into_iter().flatten() {
                parsed.spans.push(Span {
                    instance: owner.instance.clone(),
                    name: span["name"].as_str().unwrap_or_default().to_string(),
                    trace_id: span["traceId"].as_str().unwrap_or_default().to_string(),
                    span_id: span["spanId"].as_str().unwrap_or_default().to_string(),
                    parent: span["parentSpanId"]
                        .as_str()
                        .filter(|parent| !parent.is_empty())
                        .map(str::to_string),
                    start_ns: int(&span["startTimeUnixNano"]).unwrap_or(0),
                    end_ns: int(&span["endTimeUnixNano"]).unwrap_or(0),
                    attributes: attributes(&span["attributes"], &SPAN_ATTRIBUTES),
                    status: span["status"]["code"].as_i64().unwrap_or(0),
                });
            }
        }
        parsed.resources.push(owner);
    }
    Some(parsed)
}

fn ingest_metrics(value: &Value) -> Option<Parsed> {
    let mut parsed = Parsed::default();
    for resource_metrics in value.get("resourceMetrics")?.as_array()? {
        let owner = resource(&resource_metrics["resource"]);
        for scope in resource_metrics["scopeMetrics"]
            .as_array()
            .into_iter()
            .flatten()
        {
            for metric in scope["metrics"].as_array().into_iter().flatten() {
                let name = metric["name"].as_str().unwrap_or_default();
                let (data, histogram) = if let Some(data) = metric.get("histogram") {
                    (data, true)
                } else if let Some(data) = metric.get("sum").or_else(|| metric.get("gauge")) {
                    (data, false)
                } else {
                    continue;
                };
                let points = data["dataPoints"].as_array().cloned().unwrap_or_default();
                if name != DURATION && name != ACTIVE {
                    parsed.other_points += points.len() as u64;
                    continue;
                }
                let cumulative = data["aggregationTemporality"].as_i64() != Some(1);
                for point in points {
                    parsed.points.push(Point {
                        instance: owner.instance.clone(),
                        metric: name.to_string(),
                        attributes: attributes(&point["attributes"], &POINT_ATTRIBUTES),
                        start_ns: int(&point["startTimeUnixNano"]).unwrap_or(0),
                        time_ns: int(&point["timeUnixNano"]).unwrap_or(0),
                        cumulative,
                        count: if histogram {
                            int(&point["count"]).unwrap_or(0) as u64
                        } else {
                            0
                        },
                        bounds: point["explicitBounds"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_f64)
                            .collect(),
                        buckets: point["bucketCounts"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .map(|count| int(count).unwrap_or(0) as u64)
                            .collect(),
                        value: if histogram {
                            None
                        } else {
                            point["asDouble"].as_f64().or_else(|| {
                                int(&point["asInt"])
                                    .map(|value| value as f64)
                                    .or_else(|| point["asInt"].as_i64().map(|value| value as f64))
                            })
                        },
                    });
                }
            }
        }
        parsed.resources.push(owner);
    }
    Some(parsed)
}

/// The measured window, in Unix ns.
#[derive(Clone, Copy, Debug)]
pub struct Window {
    /// Its start.
    pub start_unix_ns: u128,
    /// Its end.
    pub end_unix_ns: u128,
}

impl Window {
    fn holds(&self, from: u128, to: u128) -> bool {
        from >= self.start_unix_ns && to <= self.end_unix_ns && from <= to
    }
}

/// Spans by route.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteSpans {
    /// Sampled spans wholly inside the window.
    pub count: u64,
    /// Their errors (status 2 or 5xx).
    pub errors: u64,
    /// Their durations, ms.
    pub duration_ms: Percentiles,
}

/// A slow sampled span.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlowSpan {
    /// Route.
    pub route: String,
    /// Node.
    pub node: Option<u64>,
    /// Duration, ms.
    pub duration_ms: f64,
    /// Trace ID.
    pub trace_id: String,
    /// HTTP status.
    pub status: Option<String>,
}

/// The span side.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanSummary {
    /// Spans received and kept.
    pub received: u64,
    /// Those wholly inside the measured window.
    pub in_window: u64,
    /// By route.
    pub routes: BTreeMap<String, RouteSpans>,
    /// The slowest in the window.
    pub slowest: Vec<SlowSpan>,
    /// Spans whose parent came from another process (forwarded calls).
    pub forwarded: u64,
}

/// Server-side request durations from the metric, by route.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteMetric {
    /// Requests counted inside the window, every node.
    pub requests: u64,
    /// Per second of the covered time.
    pub per_second: f64,
    /// Bucket-upper-bound percentiles, ms.
    pub p50_ms: Option<f64>,
    /// p90.
    pub p90_ms: Option<f64>,
    /// p99.
    pub p99_ms: Option<f64>,
}

/// The metric side.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricSummary {
    /// Points received and kept.
    pub points: u64,
    /// Adjacent-export intervals inside the window.
    pub intervals: u64,
    /// Series resets (a restart, or a new start time) skipped.
    pub resets: u64,
    /// Request durations by route.
    pub routes: BTreeMap<String, RouteMetric>,
    /// Requests per node inside the window.
    pub per_node: BTreeMap<String, u64>,
    /// Highest in-flight count any node exported in the window.
    pub max_active: Option<f64>,
}

/// The OpenTelemetry diagnostic.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OtelReport {
    /// Whether the capture is complete enough to read.
    pub passed: bool,
    /// Why not.
    pub problems: Vec<String>,
    /// The capture's bounds.
    pub limits: Option<Limits>,
    /// Requests received, refused and malformed; bytes accepted;
    /// records dropped past the bound.
    pub requests: u64,
    /// Refused requests.
    pub refused: u64,
    /// Malformed requests.
    pub malformed: u64,
    /// Accepted bytes.
    pub bytes: u64,
    /// Dropped records.
    pub dropped: u64,
    /// Every process that exported.
    pub resources: Vec<Resource>,
    /// Spans.
    pub spans: SpanSummary,
    /// Metrics.
    pub metrics: MetricSummary,
    /// Where the files went.
    pub files: Option<String>,
    /// What the receiver cannot see.
    pub unknown: Vec<String>,
}

/// Summarize a capture over the measured window.
pub fn summarize(capture: &Capture, window: Window) -> OtelReport {
    let mut report = OtelReport {
        limits: capture.limits,
        requests: capture.requests,
        refused: capture.refused,
        malformed: capture.malformed,
        bytes: capture.bytes,
        dropped: capture.dropped,
        resources: capture.resources.values().cloned().collect(),
        unknown: vec![
            "Exporter queue loss and failed transmissions happen in the servers and are not visible here.".to_string(),
            "Spans and metrics buffered in a killed member are lost with it.".to_string(),
        ],
        ..OtelReport::default()
    };
    let node_of = |instance: &str| {
        capture
            .resources
            .get(instance)
            .and_then(|resource| resource.node)
    };

    // Spans.
    let ids: std::collections::BTreeSet<(&str, &str)> = capture
        .spans
        .iter()
        .map(|span| (span.instance.as_str(), span.span_id.as_str()))
        .collect();
    let mut durations: BTreeMap<String, Histogram> = BTreeMap::new();
    let mut slow = Vec::new();
    report.spans.received = capture.spans.len() as u64;
    for span in &capture.spans {
        if let Some(parent) = &span.parent
            && !ids.contains(&(span.instance.as_str(), parent.as_str()))
            && capture
                .spans
                .iter()
                .any(|other| &other.span_id == parent && other.instance != span.instance)
        {
            report.spans.forwarded += 1;
        }
        if !window.holds(span.start_ns, span.end_ns) {
            continue;
        }
        report.spans.in_window += 1;
        let route = span
            .attributes
            .get("http.route")
            .cloned()
            .unwrap_or_else(|| span.name.clone());
        let duration = (span.end_ns - span.start_ns) as f64 / 1e6;
        durations.entry(route.clone()).or_default().record(duration);
        let entry = report.spans.routes.entry(route.clone()).or_default();
        entry.count += 1;
        let status = span.attributes.get("http.response.status_code").cloned();
        if span.status == 2 || status.as_deref().is_some_and(|code| code.starts_with('5')) {
            entry.errors += 1;
        }
        slow.push(SlowSpan {
            route,
            node: node_of(&span.instance),
            duration_ms: duration,
            trace_id: span.trace_id.clone(),
            status,
        });
    }
    for (route, histogram) in durations {
        if let Some(entry) = report.spans.routes.get_mut(&route) {
            entry.duration_ms = histogram.percentiles();
        }
    }
    slow.sort_by(|a, b| b.duration_ms.total_cmp(&a.duration_ms));
    slow.truncate(10);
    report.spans.slowest = slow;

    // Metrics: difference adjacent cumulative exports per series.
    report.metrics.points = capture.points.len() as u64;
    /// A series: its process, its instrument and its attributes.
    type Series = (String, String, Vec<(String, String)>);
    let mut series: BTreeMap<Series, Vec<&Point>> = BTreeMap::new();
    for point in &capture.points {
        let attributes = point.attributes.clone().into_iter().collect();
        series
            .entry((point.instance.clone(), point.metric.clone(), attributes))
            .or_default()
            .push(point);
    }
    let mut buckets: BTreeMap<String, (Vec<f64>, Vec<u64>, u64)> = BTreeMap::new();
    let mut covered_ns: BTreeMap<String, u128> = BTreeMap::new();
    for ((instance, metric, _), mut points) in series {
        points.sort_by_key(|point| point.time_ns);
        if metric == ACTIVE {
            for point in &points {
                if window.holds(point.time_ns, point.time_ns)
                    && let Some(value) = point.value
                {
                    report.metrics.max_active = Some(
                        report
                            .metrics
                            .max_active
                            .map_or(value, |max: f64| max.max(value)),
                    );
                }
            }
            continue;
        }
        for pair in points.windows(2) {
            let (before, after) = (pair[0], pair[1]);
            if !window.holds(before.time_ns, after.time_ns) {
                continue;
            }
            let (count, delta): (u64, Vec<u64>) = if !after.cumulative {
                (after.count, after.buckets.clone())
            } else if after.start_ns != before.start_ns
                || after.count < before.count
                || after.buckets.len() != before.buckets.len()
                || after.bounds != before.bounds
            {
                report.metrics.resets += 1;
                continue;
            } else {
                (
                    after.count - before.count,
                    after
                        .buckets
                        .iter()
                        .zip(&before.buckets)
                        .map(|(a, b)| a.saturating_sub(*b))
                        .collect(),
                )
            };
            report.metrics.intervals += 1;
            let route = after
                .attributes
                .get("http.route")
                .cloned()
                .unwrap_or_else(|| "unmatched".to_string());
            let entry = buckets
                .entry(route.clone())
                .or_insert_with(|| (after.bounds.clone(), vec![0; delta.len()], 0));
            if entry.0 == after.bounds && entry.1.len() == delta.len() {
                for (total, more) in entry.1.iter_mut().zip(&delta) {
                    *total += more;
                }
                entry.2 += count;
            }
            let node = node_of(&instance).map_or(instance.clone(), |node| format!("n{node}"));
            *report.metrics.per_node.entry(node).or_default() += count;
            let span = after.time_ns - before.time_ns;
            let covered = covered_ns.entry(route).or_default();
            *covered = (*covered).max(span);
        }
    }
    let window_s = (window.end_unix_ns.saturating_sub(window.start_unix_ns)) as f64 / 1e9;
    for (route, (bounds, counts, total)) in buckets {
        let percentile = |q: f64| -> Option<f64> {
            if total == 0 {
                return None;
            }
            let rank = ((q * total as f64).ceil() as u64).max(1);
            let mut seen = 0;
            for (index, count) in counts.iter().enumerate() {
                seen += count;
                if seen >= rank {
                    // Seconds to ms; the overflow bucket has no bound.
                    return bounds.get(index).map(|bound| bound * 1000.0);
                }
            }
            None
        };
        report.metrics.routes.insert(
            route,
            RouteMetric {
                requests: total,
                per_second: if window_s > 0.0 {
                    total as f64 / window_s
                } else {
                    0.0
                },
                p50_ms: percentile(0.5),
                p90_ms: percentile(0.9),
                p99_ms: percentile(0.99),
            },
        );
    }

    // Verdict.
    let problems = &mut report.problems;
    if capture.spans.is_empty() {
        problems.push("no spans arrived".to_string());
    }
    if capture.points.is_empty() {
        problems.push("no metric points arrived".to_string());
    } else if report.metrics.intervals == 0 {
        problems.push("no metric export interval fell inside the measured window".to_string());
    }
    if capture.malformed > 0 {
        problems.push(format!("{} malformed export requests", capture.malformed));
    }
    if capture.refused > 0 || capture.dropped > 0 {
        problems.push(format!(
            "the capture bounds were exceeded: {} requests refused, {} records dropped",
            capture.refused, capture.dropped
        ));
    }
    report.passed = report.problems.is_empty();
    report
}

/// Where the files go: `<json stem>-otel/` beside the JSON report.
pub fn directory(json: &Path) -> PathBuf {
    let stem = json.file_stem().map_or_else(
        || "report".into(),
        |stem| stem.to_string_lossy().into_owned(),
    );
    json.with_file_name(format!("{stem}-otel"))
}

/// Write `summary.json` and the sanitized `capture.ndjson`.
pub fn write(report: &OtelReport, capture: &Capture, dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|error| format!("{}: {error}", dir.display()))?;
    let summary = serde_json::to_string_pretty(report).map_err(|error| error.to_string())?;
    std::fs::write(dir.join("summary.json"), summary + "\n")
        .map_err(|error| format!("{}: {error}", dir.display()))?;
    let mut lines = String::new();
    for resource in capture.resources.values() {
        lines.push_str(&json!({"resource": resource}).to_string());
        lines.push('\n');
    }
    for span in &capture.spans {
        lines.push_str(&json!({"span": span}).to_string());
        lines.push('\n');
    }
    for point in &capture.points {
        lines.push_str(&json!({"point": point}).to_string());
        lines.push('\n');
    }
    std::fs::write(dir.join("capture.ndjson"), lines)
        .map_err(|error| format!("{}: {error}", dir.display()))
}

/// The report's OpenTelemetry section.
pub fn render(page: &mut String, report: Option<&OtelReport>) {
    let Some(report) = report else {
        return;
    };
    let _ = write!(
        page,
        "<h2>OpenTelemetry</h2><p class=\"muted small\">The servers' own traces (sampled) and metrics (every request), captured by the bench's local OTLP receiver. A diagnostic: collection perturbs the run, so its throughput is not a capacity figure. {}</p><div class=\"card\"><p>{} export requests · {} spans ({} in the window, {} forwarded across members) · {} metric points ({} intervals in the window) · {} processes{}</p>",
        if report.passed {
            "The capture is complete."
        } else {
            "The capture is incomplete, which fails the run."
        },
        count(report.requests),
        count(report.spans.received),
        count(report.spans.in_window),
        count(report.spans.forwarded),
        count(report.metrics.points),
        count(report.metrics.intervals),
        report.resources.len(),
        report
            .metrics
            .max_active
            .map_or(String::new(), |max| format!(
                " · at most {max:.0} requests in flight on one node"
            ))
    );
    for problem in &report.problems {
        let _ = write!(page, "<p class=\"small\">✕ {}</p>", esc(problem));
    }
    page.push_str("</div><div class=\"card table-wrap\"><table><thead><tr><th>Route</th><th class=\"num\">Requests (metric)</th><th class=\"num\">/s</th><th class=\"num\">p50</th><th class=\"num\">p99</th><th class=\"num\">Sampled spans</th><th class=\"num\">Span p50</th><th class=\"num\">Span p99</th><th class=\"num\">Errors</th></tr></thead><tbody>");
    let routes: std::collections::BTreeSet<&String> = report
        .metrics
        .routes
        .keys()
        .chain(report.spans.routes.keys())
        .collect();
    for route in routes {
        let metric = report.metrics.routes.get(route);
        let spans = report.spans.routes.get(route);
        let _ = write!(
            page,
            "<tr><td><code>{}</code></td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">≤ {}</td><td class=\"num\">≤ {}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td></tr>",
            esc(route),
            metric.map_or("—".to_string(), |metric| count(metric.requests)),
            metric.map_or("—".to_string(), |metric| crate::report::rate(
                metric.per_second
            )),
            ms(metric.and_then(|metric| metric.p50_ms)),
            ms(metric.and_then(|metric| metric.p99_ms)),
            spans.map_or("—".to_string(), |spans| count(spans.count)),
            ms(spans.and_then(|spans| spans.duration_ms.p50)),
            ms(spans.and_then(|spans| spans.duration_ms.p99)),
            spans.map_or("—".to_string(), |spans| count(spans.errors)),
        );
    }
    page.push_str("</tbody></table><p class=\"small muted\">Metric percentiles are bucket upper bounds of the servers' request-duration histogram, differenced between adjacent exports inside the window and merged across nodes; span percentiles cover sampled spans wholly inside the window. A forwarded request counts on both members.</p></div>");
    if !report.spans.slowest.is_empty() {
        page.push_str("<div class=\"card table-wrap\"><h3>Slowest sampled spans</h3><table><thead><tr><th>Route</th><th>Node</th><th class=\"num\">Duration</th><th>Status</th><th>Trace</th></tr></thead><tbody>");
        for span in &report.spans.slowest {
            let _ = write!(
                page,
                "<tr><td><code>{}</code></td><td>{}</td><td class=\"num\">{}</td><td>{}</td><td><code>{}</code></td></tr>",
                esc(&span.route),
                span.node.map_or("?".to_string(), |node| format!("n{node}")),
                ms(Some(span.duration_ms)),
                esc(span.status.as_deref().unwrap_or("")),
                esc(&span.trace_id)
            );
        }
        page.push_str("</tbody></table></div>");
    }
    let _ = write!(
        page,
        "<details><summary>Processes, bounds and what is unknown</summary><div class=\"card\"><p class=\"small\">{}</p><p class=\"small\">Bounds: {} records, {} bytes; {} requests refused, {} malformed, {} records dropped, {} bytes accepted.</p>{}{}</div></details>",
        esc(&report
            .resources
            .iter()
            .map(|resource| format!(
                "n{} pid {} ({})",
                resource
                    .node
                    .map_or("?".to_string(), |node| node.to_string()),
                resource.pid.map_or("?".to_string(), |pid| pid.to_string()),
                resource.instance
            ))
            .collect::<Vec<_>>()
            .join(" · ")),
        report
            .limits
            .map_or("—".to_string(), |limits| count(limits.max_records as u64)),
        report
            .limits
            .map_or("—".to_string(), |limits| count(limits.max_bytes as u64)),
        count(report.refused),
        count(report.malformed),
        count(report.dropped),
        count(report.bytes),
        report
            .unknown
            .iter()
            .map(|text| format!("<p class=\"small muted\">{}</p>", esc(text)))
            .collect::<String>(),
        report.files.as_ref().map_or(String::new(), |files| format!(
            "<p class=\"small\">Files: <code>{}</code></p>",
            esc(files)
        ))
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: u128 = 1_000_000_000;

    fn histogram_point(node: u64, pid: u64, time: u128, count: u64, buckets: &[u64]) -> Value {
        json!({"resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "flower"}},
                {"key": "service.instance.id", "value": {"stringValue": format!("{node}/{pid}")}},
                {"key": "flower.node.id", "value": {"stringValue": node.to_string()}},
                {"key": "process.pid", "value": {"intValue": pid.to_string()}},
                {"key": "secret.header", "value": {"stringValue": "Bearer abc"}},
            ]},
            "scopeMetrics": [{"scope": {"name": "flower"}, "metrics": [
                {"name": DURATION, "unit": "s", "histogram": {"aggregationTemporality": 2, "dataPoints": [{
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/v1/mutate"}},
                        {"key": "http.request.method", "value": {"stringValue": "POST"}},
                        {"key": "user.id", "value": {"stringValue": "alice"}},
                    ],
                    "startTimeUnixNano": "1000",
                    "timeUnixNano": time.to_string(),
                    "count": count.to_string(),
                    "explicitBounds": [0.001, 0.01, 0.1],
                    "bucketCounts": buckets.iter().map(|b| b.to_string()).collect::<Vec<_>>(),
                }]}},
                {"name": ACTIVE, "sum": {"aggregationTemporality": 2, "isMonotonic": false, "dataPoints": [{
                    "asInt": "4", "timeUnixNano": time.to_string(), "attributes": [],
                }]}},
                {"name": "process.memory", "gauge": {"dataPoints": [{"asDouble": 1.0}]}},
            ]}],
        }]})
    }

    fn span(start: u128, end: u128, route: &str) -> Value {
        json!({"resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.instance.id", "value": {"stringValue": "1/77"}},
                {"key": "flower.node.id", "value": {"stringValue": "1"}},
            ]},
            "scopeSpans": [{"spans": [{
                "traceId": "aa", "spanId": "bb", "name": "flower.http.request", "kind": 2,
                "startTimeUnixNano": start.to_string(), "endTimeUnixNano": end.to_string(),
                "attributes": [
                    {"key": "http.route", "value": {"stringValue": route}},
                    {"key": "http.response.status_code", "value": {"intValue": "200"}},
                    {"key": "db.statement", "value": {"stringValue": "secret"}},
                ],
                "events": [{"name": "exception", "attributes": []}],
                "status": {"code": 0},
            }]}],
        }]})
    }

    async fn collect(bodies: &[(&str, Value)], limits: Limits) -> Capture {
        let receiver = Receiver::start(limits).await.unwrap();
        let client = flower_client::transport::HttpTransport::new(
            receiver.endpoint(),
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        use flower_client::transport::Transport;
        for (path, body) in bodies {
            let reply = client
                .post(path, &serde_json::to_vec(body).unwrap(), None)
                .await
                .unwrap();
            let _ = reply.status;
        }
        let _ = client.post("/v1/traces", b"not json", None).await;
        receiver.stop().await
    }

    #[tokio::test]
    async fn the_receiver_keeps_only_sanitized_flower_records() {
        let window = Window {
            start_unix_ns: 10 * SECOND,
            end_unix_ns: 20 * SECOND,
        };
        let capture = collect(
            &[
                (
                    "/v1/traces",
                    span(11 * SECOND, 11 * SECOND + 5_000_000, "/v1/mutate"),
                ),
                // Outside the window: received, not summarized.
                ("/v1/traces", span(25 * SECOND, 26 * SECOND, "/v1/mutate")),
                (
                    "/v1/metrics",
                    histogram_point(1, 77, 11 * SECOND, 10, &[2, 6, 2, 0]),
                ),
                (
                    "/v1/metrics",
                    histogram_point(1, 77, 12 * SECOND, 110, &[12, 86, 12, 0]),
                ),
                (
                    "/v1/metrics",
                    histogram_point(1, 77, 13 * SECOND, 210, &[22, 96, 92, 0]),
                ),
            ],
            Limits::DEFAULT,
        )
        .await;
        assert_eq!(capture.requests, 6);
        assert_eq!(capture.malformed, 1);
        assert_eq!(capture.spans.len(), 2);
        // Secrets and unknown attributes never kept.
        let kept = serde_json::to_string(&capture).unwrap();
        assert!(!kept.contains("Bearer") && !kept.contains("alice") && !kept.contains("secret"));
        assert!(!kept.contains("exception"));
        assert_eq!(capture.other_points, 3);
        let resource = &capture.resources["1/77"];
        assert_eq!((resource.node, resource.pid), (Some(1), Some(77)));

        let report = summarize(&capture, window);
        assert_eq!(report.spans.received, 2);
        assert_eq!(report.spans.in_window, 1);
        let spans = &report.spans.routes["/v1/mutate"];
        assert_eq!(spans.count, 1);
        assert!((spans.duration_ms.p50.unwrap() - 5.0).abs() < 0.1);
        // Two intervals: 100 then 100 requests.
        assert_eq!(report.metrics.intervals, 2);
        let route = &report.metrics.routes["/v1/mutate"];
        assert_eq!(route.requests, 200);
        // Deltas: [10, 80, 10] then [10, 10, 80]: p50 within 10 ms,
        // p99 within 100 ms.
        assert_eq!(route.p50_ms, Some(10.0));
        assert_eq!(route.p99_ms, Some(100.0));
        assert_eq!(report.metrics.per_node["n1"], 200);
        assert_eq!(report.metrics.max_active, Some(4.0));
        // The malformed request fails the diagnostic.
        assert!(!report.passed);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.contains("malformed"))
        );
    }

    #[tokio::test]
    async fn a_reset_or_a_full_capture_is_not_merged_and_fails() {
        let window = Window {
            start_unix_ns: 10 * SECOND,
            end_unix_ns: 20 * SECOND,
        };
        let mut restarted = histogram_point(1, 78, 12 * SECOND, 5, &[5, 0, 0, 0]);
        restarted["resourceMetrics"][0]["resource"]["attributes"][1]["value"]["stringValue"] =
            json!("1/77");
        restarted["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["histogram"]["dataPoints"]
            [0]["startTimeUnixNano"] = json!("2000");
        let capture = collect(
            &[
                (
                    "/v1/metrics",
                    histogram_point(1, 77, 11 * SECOND, 10, &[2, 6, 2, 0]),
                ),
                ("/v1/metrics", restarted),
            ],
            Limits::DEFAULT,
        )
        .await;
        let report = summarize(&capture, window);
        assert_eq!(report.metrics.resets, 1);
        assert_eq!(report.metrics.intervals, 0);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.contains("no spans"))
        );
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.contains("interval"))
        );

        let tight = Limits {
            max_records: 2,
            max_bytes: Limits::DEFAULT.max_bytes,
        };
        let capture = collect(
            &[
                ("/v1/traces", span(11 * SECOND, 12 * SECOND, "/v1/query")),
                (
                    "/v1/metrics",
                    histogram_point(1, 77, 11 * SECOND, 10, &[2, 6, 2, 0]),
                ),
                (
                    "/v1/metrics",
                    histogram_point(1, 77, 12 * SECOND, 20, &[4, 12, 4, 0]),
                ),
            ],
            tight,
        )
        .await;
        assert_eq!(capture.spans.len() + capture.points.len(), 2);
        assert!(capture.dropped > 0);
        let report = summarize(&capture, window);
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.contains("bounds"))
        );
    }

    #[test]
    fn bounds_come_from_the_environment_within_caps() {
        let none = |_: &str| None;
        assert_eq!(Limits::load(none).unwrap(), Limits::DEFAULT);
        let set = |name: &str| match name {
            "FLOWER_BENCH_OTEL_MAX_RECORDS" => Some("250000".to_string()),
            _ => None,
        };
        assert_eq!(Limits::load(set).unwrap().max_records, 250_000);
        let huge = |name: &str| match name {
            "FLOWER_BENCH_OTEL_MAX_BYTES" => Some("999999999999".to_string()),
            _ => None,
        };
        assert!(Limits::load(huge).is_err());
        assert_eq!(
            directory(Path::new("out/stress.json")),
            Path::new("out/stress-otel")
        );
    }
}
