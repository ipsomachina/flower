# Flower client

The default client supports remote calls, typed contracts, prepared commands,
subscriptions, transactions, workers and administration. It does not link the
engine or Wasmtime. Enable `features = ["replica"]` for `SyncClient` and
`IntentQueue`.

## Typed application calls

Generated mutation builders support `tasks.put_task(&args).prepare().await?`
and `tasks.put_task(&args).enqueue(&outbox).await?`. Enqueue persists before
returning a handle; execute that handle with delivery options. Existing
`prepare_put_task` methods remain available. Custom backends opt in through
the SDK's `RemoteCommands` extension trait.

Query builders support `.watch().await` and `.subscribe()`. Credentials, timeout
and cancellation carry into the stream. Causal bounds, staleness, deadlines and
retry overrides are refused before opening a stream; configure reconnects through
the existing subscription API. Session-bound streams remain unsupported.

`client.database("demo").module("tasks")` binds the address once. Use
`QueryOp<Args, Result, Error>` and `MutationOp<Args, Result, Error>` from the
shared contract crate. `#[interface]` generates these as associated constants
on its handle, together with public argument types such as `TasksGetTaskArgs`.
No provider implementation is needed by a native caller. Existing guest `Op`
descriptors can be checked with `as_query()` or `as_mutation()`.

Add `client = TasksClient` to an interface attribute to generate native methods:
`#[interface(name = "tasks.v1", handle = TasksApi, client = TasksClient)]`.
Bind it with `let tasks = TasksApi::client(client.database("demo").module("tasks"));`.
Queries become `tasks.get_task(&args).await?`, retaining the query builder and its
metadata. Mutations become `tasks.prepare_put_task(&args).await?`; persist the
command, then use `tasks.remote.execute(&command, &options).await?`. Preparation
does not execute a mutation. The generated facade also works in shared contract
crates used by Wasm guests: its backend trait lives in the SDK and adds no HTTP
or Tokio dependency. Query names may not collide with generated `prepare_*`
methods or the `client` constructor; the macro diagnoses these at the contract.

```rust,ignore
let tasks = client.database("demo").module("tasks");
let command = tasks.prepare(&TasksApi::PUT_TASK, &args).await?;
let stored = serde_json::to_vec(&command)?;
// Durably store `stored` before sending. Recover this exact command after a crash.
let committed = tasks.execute(&command, &CommandOptions::retrying()).await?;
let current = tasks.query(&TasksApi::GET_TASK, &query_args)
    .at_least(committed.read_token.clone().ok_or("peer has no read tokens")?)
    .await?;
```

Preparation observes the database's initialized retry retention and nonzero
history incarnation. The server's maintenance initializes its default retry
window; operators can also configure retention explicitly. `prepare_with_identity`
works offline with an identity previously observed and stored by the application.
Preparation does not execute the method. Persist a command's identity and its
original revision precondition together. `if_unchanged(&read_token)` checks the
precondition's database; a database revision is never a group log position.

Scoped query builders take `QueryOptions`; prepared commands and typed
transactions take `CommandOptions`. Both carry delivery settings (retry,
credentials, timeout, deadline and cancellation), but only queries can set
staleness or causal bounds. `prepare_with_options` applies command delivery
options to retry-identity discovery. Low-level APIs retain `RequestOptions`;
`CommandOptions::try_from(legacy)` refuses query constraints rather than dropping
them. The original request ID and revision precondition remain in the command.

New snapshots use command format 2 and structural fingerprints of the argument,
result and declared-error schemas. `module.restore(&op, bytes)` checks the entire
current contract and decodes the stored arguments before allowing delivery.
Result/error fingerprints are also checked by direct execution. Fingerprints
detect wire-contract changes, not changes to business semantics. Pre-fingerprint
snapshots are refused; migrate with the original contract or reconcile their
business outcome, preserving the original identity. Never prepare replacement
work automatically when recovering an old intent.
`migrate_legacy(&original_op, bytes)` explicitly imports format-one snapshots
without changing identity, arguments or precondition. The caller must provide
the original result/error contract, which such a snapshot cannot prove itself.

## Durable command outbox

`CommandOutbox::new(Arc::new(FileCommandStore::open(path)?))` supplies persistence,
submission and recovery. `enqueue(&module, &mutation, &command)` verifies the full
contract and durably inserts it before returning a `CommandHandle`. It performs
no application mutation. Re-enqueueing the identical command returns its existing
lifecycle; changing its arguments or precondition under the same identity fails.

`handle.execute(&CommandOptions::retrying())` persists `Sending` before delivery
and the remote outcome before returning. Results, including explicit null, and
structured failure causes survive restarts. A successful remote result is kept
even if client-side result decoding fails; it is never resent for that reason.
Use `resume(&module, &mutation, request_id)` with current credentials and the
current contract after restarting. Credentials and delivery settings are not
part of the persisted command.

States are `Pending`, `Sending`, `Succeeded`, `Rejected`, `Unknown` and
`Unresolved`. An abandoned `Sending` state has no known outcome. `execute` never
automatically replays `Sending` or `Unknown`; `retry_uncertain` is the explicit
choice to resend the same command under its original ID. Earlier uncertainty
survives a later cancellation or authorization refusal. Retired epochs and lost
history become `Unresolved` and cannot be retried through the outbox. `resolve`
requires nonempty evidence establishing the business outcome, and performs no
network mutation. Establish a decided outcome before asserting rejection; the
absence of a business row while an earlier delivery can still run is not proof.

`status()` returns the durable record. `updates()` delivers coalesced snapshots
from this outbox instance and its clones; `status()` refreshes changes from other
instances or processes. This is a state subscription, not a complete event log.

`CommandStore` supports atomic insert and compare-and-swap transitions. A backend
must preserve the immutable command, increment generations exactly, and commit
durably before acknowledging writes. Custom backends can coordinate persistence
with the application's own database transaction. The built-in file store uses
OS file locks, canonical checksums and synced append-only records; it repairs
an incomplete final line and refuses complete-entry corruption. It replays the
journal per operation and limits each entry to 16 MiB. Use a database-backed
store for large histories. `MemoryCommandStore` is an ephemeral test backend.

`CommandResult` retains typed revision and log position counters, the request ID,
the replay indicator, and an optional read token. `CommandError` retains the
request ID, declared errors, the structured underlying error and execution
certainty. Cancellation or transport loss after submission can leave the result
unknown. An expired retry epoch or another history also leaves the original
outcome unknown: resolve it from a business identifier, without making a new
command automatically.

The dynamic serde and `FlowerType` APIs remain available. Their mutation results
also report the request ID. Existing `call`/`call_with` are value-only conveniences;
`call_result` retains the query or mutation metadata. Prefer persisted commands
when work must survive process restarts. Ordinary caller-chosen IDs use the
server's unscoped receipt retention and are not history fences.

## Consistency and protocol capabilities

`client.protocol(options).await?.require(&["read_tokens"])` checks optional peer
capabilities. `RequestOptions::at_least(token)` requires a query to include the
committed database state in the token. The server proves a fresh read and checks
the token's history and counters; wrong history is an explicit refusal. Tokens
encode full-width counters as decimal strings and carry their database, group
and history. Missing freshness metadata means unknown staleness.

`client.causal_session()` opts into a shared causal context. Bind a module with
`session.database(db).module(module)` and use the same scoped or generated client
methods. Successful unary reads and writes retain tokens per database; later
reads automatically require those observations. Clones share state, operations
against one database serialize, and different databases remain independent.
Cancellation and deadlines also bound time spent waiting for the session lock.
Missing token support is an error, and a history mismatch never silently resets
the session. Explicit `observe(token)` imports a commit or watch token;
`token(db)` exports the current observation. An acknowledged causal read or scoped
write can carry the context across a move. `reset(db)` deliberately starts a new
context after reconciliation. Tokens are in memory unless the application stores
them. Session-bound watches and subscriptions are refused; use a separate stream
and explicitly observe its tokens instead of assuming an unproved causal bound.

`ProtocolRequirements::requiring([Capability::ReadTokens, ...])` and
`client.negotiate` report missing capabilities, unknown extensions and version
compatibility as structured data. `inspect_protocol` can inspect a newer peer
without accepting its wire format. Widening a requested version range does not
enable a protocol the client has not implemented.

Enable `features = ["conformance"]` to use
`testing::conformance::ProbeServer`. Construct your transport against its
`base_url()`, run `server.check(&transport).await.assert_passed()`, then
`server.shutdown().await`. The suite checks method and body preservation,
credentials, deadlines, causal headers, response metadata, refusal status and
retry hints, incremental streaming and release on drop. Its loopback server
supports HTTP/1.1 and h2c; the built-in transport passes the same checks in both
modes. This feature is separate from the default lightweight client.

Custom transports must implement `post_with_headers` and `get_with_headers`,
preserving deadlines, consistency constraints and response metadata. Defaults
return `Unsupported` before sending a request with headers. They never discard
constraints. Use `TransportOptions::direct()` for direct Flower connections or
`TransportOptions::proxy()` for HTTP/1.1 over plain HTTP through a proxy. TLS uses
ALPN in either preset.

Successful calls must contain a value, including explicit JSON null where
appropriate. Writes must contain ordering metadata, and transactions must have
one result and revision per participant. Missing fields are protocol errors.
`FlowerError::Http` retains status, code, message, details, origin, retry advice
and execution certainty. `RetryFailed` preserves the structured last cause and
the uncertainty of earlier attempts. Message text and code casing do not decide
whether an application error is declared.

## Watches and transactions

Typed transaction snapshots use local format 2 with per-step argument, result
and declared-error fingerprints. Build a current typed transaction template with
the same participant operations, then call `template.restore(bytes)` to check
scope, contracts and argument decoding while preserving the saved request ID.
Execution also checks result/error contracts. Format-one snapshots require
reconciliation or explicit migration with their original contract; never prepare
a replacement identity to recover uncertain work. These local checks do not
change the server's transaction receipt-retention guarantees.

Scoped `watch` and `subscribe` accept query descriptors. Subscriptions deliver
values, revisions, history identity, reset markers and read tokens. Reconnects
start from a new snapshot; this is a value subscription, not an event-history
cursor. Monotonic suppression applies within a known history. A changed or
unknown history resets that comparison. Application and authorization failures
terminate the stream; transient infrastructure failures can reconnect using the
server's retry hint. Cancellation releases the connection. `into_stream()` adapts
typed watches and subscriptions to the standard futures stream interface.
Scoped streams retain the descriptor's declared error type as
`DeclaredWatch<T,E>` and `DeclaredSubscription<T,E>`. Both opening failures and
terminal stream failures return `RemoteError<E>` with the original source.
Use `error.declared()` to inspect a domain failure; authorization and host
failures stay `Other`. Existing `typed_watch` and `typed_subscribe` keep their
dynamic `FlowerError` APIs, with `*_declared` variants available explicitly.
Terminal SSE errors now use `FlowerError::Http` to retain the complete problem
extension. Consumers that matched `Business` for terminal events should use
`code()` or `problem()` to inspect them; opening failures retain their existing
unary error representation.

`TypedTransaction::new().step(db, module, mutation, args)?` builds a typed tuple
of up to eight results. Persist the serialized batch before `execute`; retries reuse its
request ID. Each step must target a distinct database. Compose multiple actions
against one database inside a guest mutation. Transaction errors identify the
failing step and database, preserving the original details under `details.cause`.
Cross-group decisions retain this context across recovery.
The builder also accumulates a tuple of declared error types. A two-step batch
with errors `(BillingError, TaskError)` returns a `TransactionError` whose
`declared()` is an optional `StepError<BillingError, TaskError>`: match `Step0`
or `Step1` using the same zero-based indices as `error.step`. The callee's
payload is decoded from `details.cause`, while `error.source` keeps the complete
context. To retain these types after restoring a batch, deserialize as
`TypedTransaction<(FirstResult, SecondResult), (BillingError, TaskError)>`.
The legacy one-parameter form deliberately keeps dynamic transaction errors.

`claim_typed` accepts a `TypedQueue<Payload, Result>` and returns one
`ClaimOutcome<Payload, Result>` per claim. The outer error covers the request;
an individual decoding error retains the raw payload and its lease. A malformed
item never discards the rest of the batch. Lease handles bind their database,
queue, claim token and fence. Heartbeat borrows a lease; completion, failure,
release and dead-lettering (`kill`) consume it. A `SettlementError` returns the
same handle via `into_lease()` for explicit retry, including after an uncertain
reply. Use the request cancellation token and await the error to recover
ownership; dropping the settlement future drops its owned handle. Dropping a
lease performs no network operation; its server lease expires normally.
`run_typed_queue_worker` handles
payload decoding and automatic lease renewal/settlement; incompatible payloads
go to the dead-letter queue.

## Offline queues and migration

`IntentQueue::initialize(db, options)` stores an observed retry identity before
disconnecting. A synced database can also supply its replicated identity.
`enqueue_command` queues the exact prepared command and returns an `IntentHandle`;
repeated enqueueing of that identity returns the same handle. Reusing an identity
for different work is refused. `intent(handle)` reports its lifecycle.

Retired epochs, lost history, and legacy unscoped intents become `Unresolved`.
They block later submissions and the preview prefix until the application calls
`resolve` with evidence of commitment or failure. They are not automatically
pruned or assigned new identities. Acknowledged effects remain previewed until
their receipts arrive through sync-down.

Sync batches require `version`, `since`, ordered entries, covered position and
incarnation. The shared protocol validator checks coverage even for empty
batches and snapshots. Upgrade the server before a new replica client; old
responses without the required metadata are refused explicitly.

For new contracts, `Nullable<T>` means a required member that may be null;
`Omittable<T>` means an absent or present member. Combine them when both states
are meaningful. Legacy method `Option<T>` parameters retain their omission
default for compatibility; ordinary `Option<T>` struct fields remain required.

Validation includes the default remote-only build and
`cargo test --workspace --features flower-client/replica` for offline support.
