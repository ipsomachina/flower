//! `flower-server`: the HTTP API over a cluster node.
//!
//! JSON over HTTP/1.1 or HTTP/2, over in-process TLS when configured
//! ([`listener`]):
//!
//! - `POST /v1/query`, `POST /v1/mutate` and `POST /v1/transaction`
//!   run the HTTP aliases modules expose. A module's authorization hook
//!   decides who each call runs as from its credentials; otherwise
//!   calls run anonymously unless a trusted gateway (admin credential)
//!   asserts the principal and timestamp. How long guest code runs is
//!   the method's own declaration, which a caller's deadline only cuts.
//! - `POST /v1/watch` (a query's body) streams query snapshots as SSE,
//!   reauthorizing each subscriber before every delivery.
//! - `GET /v1/status` reports Raft status; `GET /health/live` and
//!   `GET /health/ready` answer liveness and readiness probes.
//! - `GET /v1/sync?since&limit` serves applied entries and snapshots
//!   to replicas (sync or admin credential).
//! - `GET /v1/retry` and `POST /v1/retry/session` serve scoped retry
//!   identities and retry sessions; a mutation whose idempotency key is
//!   an `f1:` or `f2:` request ID runs under that scoped identity.
//! - `POST /v1/queue/...` and `GET /v1/queue/stats|item` claim and
//!   report durable work through consensus (worker or admin
//!   credential).
//! - `POST /v1/deploy`, `POST /v1/admin/membership|backup|deployments`,
//!   `GET /v1/admin/backup|modules|raft|activity`, `GET` and `POST
//!   /v1/admin/keys|retention`, `GET /v1/metrics`, `POST
//!   /v1/timer/cancel|retry|purge` and `GET /v1/timer/stats|get|list`
//!   are operator surfaces (admin).
//! - `GET /v1/admin/stream` pushes this member's operator view as
//!   server-sent events (admin; see [`admin_stream`]).
//! - `GET /admin` serves the operator dashboard over those surfaces
//!   (static, credential-free assets; see [`dashboard`]).
//!
//! A call's own failure answers 422 with `ok: false`, as v1; unexposed
//! aliases answer 404, authorization denials 403, a transaction past
//! `--max-transaction-bytes` 413 and other host failures 500, each with
//! the engine code. Any member serves any
//! call: queries read as they declare, and requests only the leader can
//! serve forward to it ([`forward`]); a forward answers 503 with the
//! known leader only when no leader could be reached. Credentialed
//! endpoints answer 403 when their credential is unconfigured, missing
//! or wrong; see [`api`].
//!
//! Module map:
//!
//! - [`activity`]: member-local write and watch activity for operators.
//! - [`admin`]: operator endpoints: modules, staged deployments, keys
//!   and Raft metrics.
//! - [`admin_stream`]: the operator event stream the dashboard follows.
//! - [`api`]: routes, credentials, handlers and the JSON contract.
//! - [`catalog`]: operator routes over groups and named partitions.
//! - [`dashboard`]: the embedded admin and observability dashboard.
//! - [`groups`]: every hosted group on one listener: routing by the
//!   database a request names, forwarding to groups hosted elsewhere.
//! - [`forward`]: leader forwarding, routing headers, body leases and
//!   client deadlines.
//! - [`listener`]: HTTP/1.1 and HTTP/2, in-process TLS, inherited
//!   sockets.
//! - [`queue`]: worker-facing queue endpoints over consensus.
//! - [`retention`]: retry identities, sessions and retention control.
//! - [`timer`]: operator-facing timer endpoints over consensus.
//! - [`telemetry`]: logs, OpenTelemetry export and `Server-Timing`.
//! - [`watch`]: the watch hub sharing query evaluation.

pub mod activity;
pub mod admin;
pub mod admin_stream;
pub mod api;
pub mod catalog;
pub mod dashboard;
pub mod forward;
pub mod groups;
pub mod json_patch;
pub mod listener;
mod protocol;
pub mod queue;
pub mod retention;
pub mod telemetry;
pub mod timer;
pub mod watch;
