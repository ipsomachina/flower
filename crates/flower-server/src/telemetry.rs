//! Diagnostics: logs, OpenTelemetry traces and metrics, and the
//! `Server-Timing` header.
//!
//! # Logs and OpenTelemetry
//!
//! [`init`] installs the global subscriber: human-readable logs with
//! `RUST_LOG` filtering (default `info`), and, when [`OtelConfig`]
//! enables it, OTLP/HTTP export of traces and metrics as v1 does. Export
//! is off unless `FLOWER_OTEL_ENABLED` is `1` or `true`, and
//! `OTEL_SDK_DISABLED=true` overrides it. The standard variables choose
//! the collector (`OTEL_EXPORTER_OTLP_ENDPOINT`, signal-specific
//! endpoints, headers, timeouts), the protocol (`http/protobuf`, the
//! default, or `http/json`; no gRPC), the service name and the sampler
//! (`OTEL_TRACES_SAMPLER`: `always_on`, `always_off`, `traceidratio`
//! and their `parentbased_` variants, default parent-based 1%,
//! `OTEL_TRACES_SAMPLER_ARG` a ratio from 0 to 1). Malformed values fail
//! startup. Only spans of the dedicated `flower::otel` target export;
//! logs keep their own filter.
//!
//! Each HTTP request is a `flower.http.request` server span (route
//! template, method, status, outcome; the node and the leader it knew),
//! a child of the caller's W3C `traceparent` when one came, and counts
//! in `flower.http.server.request.duration` (seconds, by route, method,
//! status and outcome) and `flower.http.server.active_requests`.
//! A follower forwarding a request to the leader sends the trace context
//! on ([`inject`]), so the leader's span joins the same trace. Bodies,
//! arguments, credentials and query strings are never recorded. The
//! query, writer, evaluator, storage, Raft and watch families and their
//! spans are the cluster's ([`flower_cluster::telemetry`]), which
//! [`init_with`] turns on once the meter provider is installed.
//!
//! # `Server-Timing`
//!
//! Always on, independent of OpenTelemetry: the JSON answers of
//! `/v1/query`, `/v1/mutate` and `/v1/transaction`, errors included,
//! carry a W3C `Server-Timing` header saying where the request's time
//! went on this member, in milliseconds with one decimal, each phase
//! only when it ran (see [`flower_cluster::stages`]):
//!
//! - `forward;dur`: a follower's round trip to the leader it forwarded
//!   the request to; the phases after it, up to `total`, are the
//!   leader's;
//! - `cache;desc=hit|miss`: whether the query result cache answered
//!   the query, or it evaluated;
//! - `fence;dur`: proving a fresh read (lease, quorum round or read
//!   fence);
//! - `join;dur`: waiting for an identical query in flight to land
//!   before answering, from the result it left in the cache when that
//!   holds (see [`flower_cluster::flight`]);
//! - `admission;dur`: waiting for an evaluation or writer slot;
//! - `eval;dur`: evaluating the query, or preparing the write;
//! - `commit;dur`: committing through Raft (append, quorum, apply);
//! - `append;dur`: the part of `commit` from the entry's submission to
//!   Raft until it was durable in the leader's own log;
//! - `replicate;dur`: the part of `commit` from the submission until a
//!   quorum committed the entry (when the leader began applying it); it
//!   overlaps `append`;
//! - `apply;dur`: applying the committed entry;
//! - `total;dur`: the request's wall time on the member that answered;
//! - `node;desc`, `leader;desc`: that member's ID and the leader it knew;
//! - `snapshot;desc=building`: a snapshot build ran during the request.
//!
//! A leader's header relayed through a forward is parsed and only the
//! names above with well-formed values are kept; the header never
//! carries method names, arguments or error text.

use std::fmt::Write as _;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use flower_cluster::node::Node;
use flower_cluster::snapshot_policy::SnapshotPolicy;
use flower_cluster::stages::{self, Recorder, Stage};
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{EnvFilter, FilterExt, FilterFn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry};

use crate::api::AppState;
use crate::forward::Forwarded;

/// Response header carrying the request's phases.
pub const SERVER_TIMING: &str = "server-timing";
/// The target of the spans OpenTelemetry exports.
pub const OTEL_TARGET: &str = flower_cluster::telemetry::OTEL_TARGET;

/// How OpenTelemetry exports, from the environment ([`OtelConfig::from_env`])
/// or set directly.
#[derive(Clone, Debug)]
pub struct OtelConfig {
    /// Whether to export at all.
    pub enabled: bool,
    /// The collector's base URL; `/v1/traces` and `/v1/metrics` are
    /// appended. `None` leaves endpoints to the OTLP variables.
    pub endpoint: Option<String>,
    /// `http/protobuf` or `http/json`.
    pub protocol: Protocol,
    /// Which root spans are sampled.
    pub sampler: Sampler,
    /// The service name resources carry.
    pub service_name: String,
    /// How often metrics export; `None` leaves it to the SDK.
    pub metric_interval: Option<Duration>,
    /// How long spans wait to batch; `None` leaves it to the SDK.
    pub batch_delay: Option<Duration>,
}

impl Default for OtelConfig {
    fn default() -> Self {
        OtelConfig {
            enabled: false,
            endpoint: None,
            protocol: Protocol::HttpBinary,
            sampler: Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(0.01))),
            service_name: "flower".to_string(),
            metric_interval: None,
            batch_delay: None,
        }
    }
}

/// A boolean switch: `1`, `true`, `0` or `false`.
fn switch(name: &str, value: Option<String>) -> Result<bool, String> {
    match value.as_deref().map(str::trim) {
        None => Ok(false),
        Some("1" | "true") => Ok(true),
        Some("0" | "false") => Ok(false),
        Some(other) => Err(format!("{name} must be 1, true, 0 or false, not {other:?}")),
    }
}

impl OtelConfig {
    /// Read the configuration through `get` (the process environment in
    /// [`OtelConfig::from_env`]). Disabled export reads nothing else.
    pub fn load(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let enabled = switch("FLOWER_OTEL_ENABLED", get("FLOWER_OTEL_ENABLED"))?
            && !switch("OTEL_SDK_DISABLED", get("OTEL_SDK_DISABLED"))?;
        let mut config = OtelConfig {
            enabled,
            ..OtelConfig::default()
        };
        if !enabled {
            return Ok(config);
        }
        config.protocol = match get("OTEL_EXPORTER_OTLP_PROTOCOL").as_deref() {
            None | Some("http/protobuf") => Protocol::HttpBinary,
            Some("http/json") => Protocol::HttpJson,
            Some(other) => {
                return Err(format!(
                    "OTEL_EXPORTER_OTLP_PROTOCOL must be http/protobuf or http/json, not {other:?}"
                ));
            }
        };
        let ratio = match get("OTEL_TRACES_SAMPLER_ARG") {
            None => 0.01,
            Some(arg) => arg
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|ratio| ratio.is_finite() && (0.0..=1.0).contains(ratio))
                .ok_or_else(|| {
                    format!("OTEL_TRACES_SAMPLER_ARG must be a ratio from 0 to 1, not {arg:?}")
                })?,
        };
        let parent = |root: Sampler| Sampler::ParentBased(Box::new(root));
        config.sampler = match get("OTEL_TRACES_SAMPLER").as_deref() {
            None | Some("parentbased_traceidratio") => parent(Sampler::TraceIdRatioBased(ratio)),
            Some("traceidratio") => Sampler::TraceIdRatioBased(ratio),
            Some("always_on") => Sampler::AlwaysOn,
            Some("always_off") => Sampler::AlwaysOff,
            Some("parentbased_always_on") => parent(Sampler::AlwaysOn),
            Some("parentbased_always_off") => parent(Sampler::AlwaysOff),
            Some(other) => return Err(format!("OTEL_TRACES_SAMPLER {other:?} is not supported")),
        };
        if let Some(name) = get("OTEL_SERVICE_NAME") {
            config.service_name = name;
        }
        Ok(config)
    }

    /// [`OtelConfig::load`] from the process environment.
    pub fn from_env() -> Result<Self, String> {
        Self::load(|name| std::env::var(name).ok())
    }
}

/// The installed exporters, flushed and stopped by
/// [`Telemetry::shutdown`].
#[derive(Default)]
pub struct Telemetry {
    traces: Option<SdkTracerProvider>,
    metrics: Option<SdkMeterProvider>,
}

impl Telemetry {
    /// Whether OpenTelemetry exports.
    pub fn enabled(&self) -> bool {
        self.traces.is_some()
    }

    /// Flush buffered spans and metrics and stop exporting.
    pub async fn shutdown(self) -> Result<(), String> {
        flower_cluster::telemetry::set_enabled(false);
        tokio::task::spawn_blocking(move || {
            let traces = self
                .traces
                .map(|traces| traces.shutdown().map_err(|error| error.to_string()));
            let metrics = self
                .metrics
                .map(|metrics| metrics.shutdown().map_err(|error| error.to_string()));
            traces.transpose().and(metrics.transpose()).map(drop)
        })
        .await
        .map_err(|error| error.to_string())?
    }
}

/// Install the global subscriber with logs only, ignoring repeat
/// installs.
pub fn init() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Install the global subscriber: logs under `RUST_LOG`, plus OTLP
/// export of `flower::otel` spans and of metrics when `config` enables
/// it, for member `node`. Fails when the exporters cannot be built or a
/// subscriber is already installed.
pub fn init_with(config: &OtelConfig, node: u64) -> Result<Telemetry, String> {
    let logs = || {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
        tracing_subscriber::fmt::layer().with_filter(
            EnvFilter::new(filter).and(FilterFn::new(|meta| meta.target() != OTEL_TARGET)),
        )
    };
    if !config.enabled {
        return Registry::default()
            .with(logs())
            .try_init()
            .map(|()| Telemetry::default())
            .map_err(|error| error.to_string());
    }
    let resource = Resource::builder()
        .with_service_name(config.service_name.clone())
        .with_attributes([
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new(
                "service.instance.id",
                format!("{node}/{}", std::process::id()),
            ),
            KeyValue::new("flower.node.id", node.to_string()),
            KeyValue::new("process.pid", i64::from(std::process::id())),
        ])
        .build();
    let mut spans = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(config.protocol);
    let mut points = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(config.protocol);
    if let Some(endpoint) = &config.endpoint {
        let endpoint = endpoint.trim_end_matches('/');
        spans = spans.with_endpoint(format!("{endpoint}/v1/traces"));
        points = points.with_endpoint(format!("{endpoint}/v1/metrics"));
    }
    let spans = spans.build().map_err(|error| error.to_string())?;
    let points = points.build().map_err(|error| error.to_string())?;
    let mut batch = opentelemetry_sdk::trace::BatchConfigBuilder::default();
    if let Some(delay) = config.batch_delay {
        batch = batch.with_scheduled_delay(delay);
    }
    let processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(spans)
        .with_batch_config(batch.build())
        .build();
    let traces = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(config.sampler.clone())
        .with_span_processor(processor)
        .build();
    let mut reader = opentelemetry_sdk::metrics::PeriodicReader::builder(points);
    if let Some(interval) = config.metric_interval {
        reader = reader.with_interval(interval);
    }
    let metrics = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader.build())
        .build();
    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_meter_provider(metrics.clone());
    let otel = tracing_opentelemetry::layer()
        .with_tracer(traces.tracer("flower"))
        .with_filter(FilterFn::new(|meta| meta.target() == OTEL_TARGET));
    Registry::default()
        .with(logs())
        .with(otel)
        .try_init()
        .map_err(|error| error.to_string())?;
    // The meter provider is installed: instruments may be built now.
    flower_cluster::telemetry::set_enabled(true);
    Ok(Telemetry {
        traces: Some(traces),
        metrics: Some(metrics),
    })
}

/// Whether OpenTelemetry exports in this process.
fn enabled() -> bool {
    flower_cluster::telemetry::enabled()
}

/// The HTTP instruments, built once.
struct Instruments {
    duration: opentelemetry::metrics::Histogram<f64>,
    active: opentelemetry::metrics::UpDownCounter<i64>,
    stage: opentelemetry::metrics::Histogram<f64>,
}

fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("flower");
        Instruments {
            duration: meter
                .f64_histogram("flower.http.server.request.duration")
                .with_unit("s")
                .with_description("HTTP requests until their response headers")
                .with_boundaries(vec![
                    0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
                    2.5, 5.0, 10.0,
                ])
                .build(),
            active: meter
                .i64_up_down_counter("flower.http.server.active_requests")
                .with_description("HTTP requests being handled")
                .build(),
            stage: meter
                .f64_histogram("flower.http.server.stage.duration")
                .with_unit("s")
                .with_description(
                    "Request body reception and JSON extraction, scheduling waits included",
                )
                .with_boundaries(vec![
                    0.000_001, 0.000_005, 0.000_01, 0.000_025, 0.000_05, 0.0001, 0.000_25, 0.0005,
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ])
                .build(),
        }
    })
}

/// Run an HTTP stage (`body_receive`: buffering a request body at
/// ingress, under its lease; `json_extract`: parsing a call's JSON): a
/// `flower.http.stage` span and `flower.http.server.stage.duration` by
/// stage and outcome, when OpenTelemetry exports.
pub(crate) async fn http_stage<T, E>(
    stage: &'static str,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if !enabled() {
        return future.await;
    }
    let span = tracing::info_span!(
        target: OTEL_TARGET,
        "flower.http.stage",
        stage,
        outcome = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    let started = Instant::now();
    let result = future.instrument(span.clone()).await;
    let outcome = if result.is_ok() { "ok" } else { "error" };
    span.record("outcome", outcome);
    if result.is_err() {
        span.record("otel.status_code", "ERROR");
    }
    instruments().stage.record(
        started.elapsed().as_secs_f64(),
        &[
            KeyValue::new("stage", stage),
            KeyValue::new("outcome", outcome),
        ],
    );
    result
}

/// A call's JSON body, its extraction timed as the `json_extract` HTTP
/// stage; parsing, limits and rejections are axum's.
pub(crate) struct TimedJson<T>(pub T);

impl<S, T> axum::extract::FromRequest<S> for TimedJson<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = axum::response::Response;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        http_stage(
            "json_extract",
            axum::Json::<T>::from_request(request, state),
        )
        .await
        .map(|axum::Json(value)| TimedJson(value))
        .map_err(json_rejection)
    }
}

/// Answer a body that is not the request's JSON as every other refusal:
/// `{ok: false, code, message}`. Malformed or mistyped JSON is the
/// request's fault, 400 (never a 422, which clients read as the call's
/// own failure); a body past its limit stays 413 and a missing content
/// type 415.
fn json_rejection(rejection: axum::extract::rejection::JsonRejection) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let (status, code) = match rejection.status() {
        StatusCode::PAYLOAD_TOO_LARGE => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type")
        }
        _ => (StatusCode::BAD_REQUEST, "bad_request"),
    };
    let body = serde_json::json!({"ok": false, "code": code, "message": rejection.body_text()});
    (status, axum::Json(body)).into_response()
}

/// Request headers as a trace-context carrier.
struct Headers<'a>(&'a axum::http::HeaderMap);

impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }
}

/// Forwarded-request headers as a trace-context carrier.
struct Outgoing<'a>(&'a mut Vec<(String, String)>);

impl Injector for Outgoing<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_string(), value));
    }
}

/// Add the current span's W3C trace context (`traceparent`,
/// `tracestate`, never baggage) to a forwarded request's headers, when
/// OpenTelemetry exports.
pub(crate) fn inject(headers: &mut Vec<(String, String)>) {
    if !enabled() {
        return;
    }
    let context = tracing::Span::current().context();
    TraceContextPropagator::new().inject_context(&context, &mut Outgoing(headers));
}

/// A method, or `_OTHER`: metric attributes stay bounded.
fn method_name(method: &axum::http::Method) -> &'static str {
    match *method {
        axum::http::Method::GET => "GET",
        axum::http::Method::POST => "POST",
        axum::http::Method::PUT => "PUT",
        axum::http::Method::DELETE => "DELETE",
        axum::http::Method::HEAD => "HEAD",
        axum::http::Method::OPTIONS => "OPTIONS",
        axum::http::Method::PATCH => "PATCH",
        _ => "_OTHER",
    }
}

/// Trace and measure each HTTP request, when OpenTelemetry exports:
/// a `flower.http.request` server span, child of the caller's trace
/// context, and the request-duration and active-request instruments.
pub(crate) async fn http(State(state): State<AppState>, request: Request, next: Next) -> Response {
    if !enabled() {
        return next.run(request).await;
    }
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_string(), |path| path.as_str().to_string());
    let method = method_name(request.method());
    let span = tracing::info_span!(
        target: OTEL_TARGET,
        "flower.http.request",
        otel.kind = "server",
        http.route = %route,
        http.request.method = method,
        http.response.status_code = tracing::field::Empty,
        outcome = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        flower.node.id = flower_cluster::telemetry::int(state.node.id()),
        flower.raft.leader_id = tracing::field::Empty,
        flower.raft.snapshot_in_progress = tracing::field::Empty,
    );
    let snapshots = flower_cluster::telemetry::snapshot_activity();
    let parent = TraceContextPropagator::new().extract(&Headers(request.headers()));
    if parent.span().span_context().is_valid() {
        let _ = span.set_parent(parent);
    }
    let instruments = instruments();
    let route_attributes = [
        KeyValue::new("http.route", route.clone()),
        KeyValue::new("http.request.method", method),
    ];
    instruments.active.add(1, &route_attributes);
    let started = Instant::now();
    let response = next.run(request).instrument(span.clone()).await;
    instruments.active.add(-1, &route_attributes);
    let status = response.status();
    let outcome = if status.is_server_error() {
        "server_error"
    } else if status.is_client_error() {
        "client_error"
    } else {
        "ok"
    };
    span.record("http.response.status_code", status.as_u16());
    span.record("outcome", outcome);
    if status.is_server_error() {
        span.record("otel.status_code", "ERROR");
    }
    if let Some(leader) = state.node.status().leader {
        span.record(
            "flower.raft.leader_id",
            flower_cluster::telemetry::int(leader),
        );
    }
    // A snapshot build or log purge ran during the request.
    if snapshots.overlapped(flower_cluster::telemetry::snapshot_activity()) {
        span.record("flower.raft.snapshot_in_progress", true);
    }
    instruments.duration.record(
        started.elapsed().as_secs_f64(),
        &[
            KeyValue::new("http.route", route),
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.response.status_code", i64::from(status.as_u16())),
            KeyValue::new("outcome", outcome),
        ],
    );
    response
}

/// A member's snapshot builds, to tell whether one overlapped a request.
fn snapshot_mark(node: &Node, policy: &SnapshotPolicy) -> (usize, u64) {
    let stats = node.snapshot_policy_stats(policy);
    (stats.active, stats.builds)
}

/// A request's phases as the header carries them.
#[derive(Clone, Debug, Default, PartialEq)]
struct Phases {
    durations: Vec<(Stage, Duration)>,
    snapshot: bool,
    /// Whether the query cache answered (`hit`) or the query evaluated
    /// (`miss`); `None` when no lookup ran.
    cache: Option<bool>,
}

impl Phases {
    /// Parse a relayed header, keeping only Flower's phase names with
    /// well-formed values: durations finite, from 0 to 10^9 ms.
    fn parse(header: &str) -> Self {
        let mut phases = Phases::default();
        for metric in header.split(',') {
            let mut parts = metric.split(';').map(str::trim);
            let Some(name) = parts.next() else {
                continue;
            };
            let params: Vec<(&str, &str)> = parts
                .filter_map(|part| part.split_once('='))
                .map(|(key, value)| (key.trim(), value.trim().trim_matches('"')))
                .collect();
            if name == "snapshot" {
                phases.snapshot |= params.contains(&("desc", "building"));
                continue;
            }
            if name == "cache" {
                if params.contains(&("desc", "hit")) {
                    phases.cache = Some(true);
                } else if params.contains(&("desc", "miss")) {
                    phases.cache = Some(false);
                }
                continue;
            }
            let Some(stage) = Stage::ALL.into_iter().find(|stage| stage.name() == name) else {
                continue;
            };
            let Some(ms) = params
                .iter()
                .find(|(key, _)| *key == "dur")
                .and_then(|(_, value)| value.parse::<f64>().ok())
                .filter(|ms| ms.is_finite() && (0.0..1e9).contains(ms))
            else {
                continue;
            };
            if phases.durations.iter().all(|(seen, _)| *seen != stage) {
                phases
                    .durations
                    .push((stage, Duration::from_secs_f64(ms / 1000.0)));
            }
        }
        phases
            .durations
            .sort_by_key(|(stage, _)| Stage::ALL.iter().position(|s| s == stage));
        phases
    }
}

/// Render a header: the forward, the phases, then this member's total,
/// node, leader and snapshot overlap.
fn render(
    forward: Option<Duration>,
    phases: &Phases,
    total: Duration,
    node: u64,
    leader: Option<u64>,
) -> String {
    let ms = |elapsed: Duration| elapsed.as_secs_f64() * 1000.0;
    let mut header = String::new();
    let mut push = |metric: String| {
        if !header.is_empty() {
            header.push_str(", ");
        }
        header.push_str(&metric);
    };
    if let Some(forward) = forward {
        push(format!("forward;dur={:.1}", ms(forward)));
    }
    if let Some(hit) = phases.cache {
        push(format!("cache;desc={}", if hit { "hit" } else { "miss" }));
    }
    for (stage, elapsed) in &phases.durations {
        push(format!("{};dur={:.1}", stage.name(), ms(*elapsed)));
    }
    push(format!("total;dur={:.1}", ms(total)));
    push(format!("node;desc={node}"));
    if let Some(leader) = leader {
        let mut metric = String::new();
        let _ = write!(metric, "leader;desc={leader}");
        push(metric);
    }
    if phases.snapshot {
        push("snapshot;desc=building".to_string());
    }
    header
}

/// Time a call route's request and answer its JSON with
/// `Server-Timing`.
pub(crate) async fn server_timing(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let before = snapshot_mark(&state.node, &state.snapshot_policy);
    let recorder = Recorder::new();
    let mut response = stages::scoped(recorder.clone(), next.run(request)).await;
    let total = started.elapsed();
    let after = snapshot_mark(&state.node, &state.snapshot_policy);
    let json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    if !json {
        return response;
    }
    let forward = response
        .extensions()
        .get::<Forwarded>()
        .map(|forwarded| forwarded.elapsed);
    let mut phases = match forward {
        // The leader's phases, in place of this member's.
        Some(_) => response
            .headers()
            .get(SERVER_TIMING)
            .and_then(|value| value.to_str().ok())
            .map(Phases::parse)
            .unwrap_or_default(),
        None => Phases {
            durations: recorder.phases(),
            snapshot: false,
            cache: recorder.cache(),
        },
    };
    phases.snapshot |= before.0 > 0 || after.0 > 0 || before.1 != after.1;
    let header = render(
        forward,
        &phases,
        total,
        state.node.id(),
        state.node.status().leader,
    );
    if let Ok(value) = HeaderValue::from_str(&header) {
        response.headers_mut().insert(SERVER_TIMING, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn metrics_come_in_order_with_one_decimal_and_only_when_measured() {
        let phases = Phases {
            durations: vec![
                (Stage::Admission, Duration::from_micros(1_250)),
                (Stage::Commit, 3 * MS),
            ],
            snapshot: false,
            cache: None,
        };
        assert_eq!(
            render(None, &phases, Duration::from_micros(5_060), 2, Some(1)),
            "admission;dur=1.2, commit;dur=3.0, total;dur=5.1, node;desc=2, leader;desc=1"
        );
        // An unknown leader is left out; a snapshot build is named; the
        // query cache's answer comes first.
        let building = Phases {
            durations: Vec::new(),
            snapshot: true,
            cache: Some(true),
        };
        assert_eq!(
            render(None, &building, MS, 1, None),
            "cache;desc=hit, total;dur=1.0, node;desc=1, snapshot;desc=building"
        );
    }

    #[test]
    fn a_forwarded_request_shows_the_leaders_phases_after_the_round_trip() {
        let upstream = "cache;desc=bogus, fence;dur=0.5, admission;dur=1.0, secret;desc=\"token\", \
                        eval;dur=nan, eval;dur=2.0, commit;dur=-3, commit;dur=4.0, apply;dur=0.5, \
                        append;dur=1.5, replicate;dur=3.0, total;dur=9.0, node;desc=1, \
                        snapshot;desc=building, cache;desc=miss";
        let phases = Phases::parse(upstream);
        // The commit's parts are kept, in their order.
        assert_eq!(
            phases.durations,
            [
                (Stage::Fence, Duration::from_micros(500)),
                (Stage::Admission, MS),
                (Stage::Eval, 2 * MS),
                (Stage::Commit, 4 * MS),
                (Stage::Append, Duration::from_micros(1_500)),
                (Stage::Replicate, 3 * MS),
                (Stage::Apply, Duration::from_micros(500)),
            ]
        );
        assert!(phases.snapshot);
        // Only a well-formed cache answer is kept.
        assert_eq!(phases.cache, Some(false));
        let header = render(Some(7 * MS), &phases, 8 * MS, 2, Some(1));
        assert_eq!(
            header,
            "forward;dur=7.0, cache;desc=miss, fence;dur=0.5, admission;dur=1.0, eval;dur=2.0, \
             commit;dur=4.0, append;dur=1.5, replicate;dur=3.0, apply;dur=0.5, total;dur=8.0, \
             node;desc=2, leader;desc=1, snapshot;desc=building"
        );
        // One total and one node: this member's.
        assert_eq!(header.matches("total").count(), 1);
        assert!(!header.contains("secret"));
    }

    #[test]
    fn export_is_opt_in_and_bad_settings_fail_closed() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| value.to_string())
            }
        };
        assert!(!OtelConfig::load(env(&[])).unwrap().enabled);
        // Disabled export ignores the rest, malformed or not.
        assert!(
            !OtelConfig::load(env(&[("OTEL_TRACES_SAMPLER", "bogus")]))
                .unwrap()
                .enabled
        );
        assert!(
            !OtelConfig::load(env(&[
                ("FLOWER_OTEL_ENABLED", "1"),
                ("OTEL_SDK_DISABLED", "true")
            ]))
            .unwrap()
            .enabled
        );
        let on = OtelConfig::load(env(&[
            ("FLOWER_OTEL_ENABLED", "true"),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json"),
            ("OTEL_TRACES_SAMPLER", "always_on"),
            ("OTEL_SERVICE_NAME", "orders"),
        ]))
        .unwrap();
        assert!(on.enabled);
        assert_eq!(on.protocol, Protocol::HttpJson);
        assert_eq!(
            format!("{:?}", on.sampler),
            format!("{:?}", Sampler::AlwaysOn)
        );
        assert_eq!(on.service_name, "orders");
        let default = OtelConfig::load(env(&[("FLOWER_OTEL_ENABLED", "1")])).unwrap();
        assert_eq!(
            format!("{:?}", default.sampler),
            format!(
                "{:?}",
                Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(0.01)))
            )
        );
        for bad in [
            &[("FLOWER_OTEL_ENABLED", "yes")][..],
            &[
                ("FLOWER_OTEL_ENABLED", "1"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
            ],
            &[
                ("FLOWER_OTEL_ENABLED", "1"),
                ("OTEL_TRACES_SAMPLER", "sometimes"),
            ],
            &[
                ("FLOWER_OTEL_ENABLED", "1"),
                ("OTEL_TRACES_SAMPLER_ARG", "1.5"),
            ],
            &[
                ("FLOWER_OTEL_ENABLED", "1"),
                ("OTEL_TRACES_SAMPLER_ARG", "NaN"),
            ],
        ] {
            assert!(OtelConfig::load(env(bad)).is_err(), "{bad:?}");
        }
    }
}
