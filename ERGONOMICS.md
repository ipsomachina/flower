# Ergonomics pass: the application developer API

This audit reads Flower v2 the way an application developer meets it:
the guest SDK (`flower-sdk`, `flower-sdk-macros`), the examples
(`examples/*`, the Goblin Pizza guest that `flower-bench` drives,
`flower-guest-test`), the HTTP API (`flower-server`), the client
(`flower-client`), the worker (`flower-worker`) and the CLI
(`cargo-flower`). Each entry says what the developer writes today, what
it costs them, the proposed shape, and how v1 (`C:\src\back\flower`)
exposed the same thing, so nothing regresses.

Status: **implemented** on `pc/ergonomics`, except where the
[status](#status) says otherwise.

## The five named papercuts

### 1. `key_text`: keys are canonical-JSON strings the developer renders

Today every keyed operation takes `&str` canonical text that the caller
renders first:

```rust
// typed-modules-provider
Rows::get(ctx, &flower_sdk::typed::key_text(&id)?)
// goblin-pizza: a raw string key needs a Value, then canonical JSON
ctx.get(CONFIG, &Value::String("world".to_string()).canonical_json()?)?
ctx.set(ORDERS_REF, &key(&order_id)?, object(order))?   // key() = canonical_json
// crypto-ticket
ctx.get(TICKETS_REF, &Value::String(jti).canonical_json()?)?
```

Cost: every call site renders keys by hand (a `?` and an allocation
each), the type system cannot tell a rendered key from any other
string, and getting it wrong (`ctx.get(C, "world")` instead of
`"\"world\""`) silently addresses a different row. `RangeBounds` and
spec examples carry the same burden (`prefix = vec![key_text(&owner)?]`).

Proposed: keys are values. Typed handles take `&K`; raw context
operations take `&Value` (anything `FlowerType`); canonical text is the
ABI's business and `key_text` leaves the public API.

```rust
Rows::get(ctx, &id)?
ctx.get(&CONFIG, &"world")?                // raw: any FlowerType key
ctx.set(&ORDERS, &order_id, &order)?
```

v1: the typed Rust SDK took `TASKS.get(ctx, &TaskId)`, the raw one
`ctx.get(&SHOPS, &Value)`; TypeScript encoded tuple keys automatically.
v2 regressed here.

### 2. Every operation is declared several times

Goblin Pizza names `pizza.order` in four places and its descriptor in
three; a query repeats its consistency in two:

```rust
static ORDER: Method = Method::new("pizza.order", |ctx, input| { ... });
static SHOP_LOCAL: Method = Method::new("pizza.shop.local", ...);
pub static APP: App = App {
    definitions: &[
        Definition::Mutation(&ORDER),
        Definition::Query(&SHOP_LOCAL, Consistency::ReplicaLocal),
        ...
    ],
    http: &[
        ("pizza.order", Definition::Mutation(&ORDER)),
        ("pizza.shop.local", Definition::Query(&SHOP_LOCAL, Consistency::ReplicaLocal)),
        ...
    ],
    ...
};
// and the authenticated build, keyed by alias string:
methods: &[("pizza.setup", Access::Check(admin)), ("pizza.world", Access::Check(admin))],
```

The typed path is no better once a module grows: `#[mutation]` takes no
options (no name, consistency, exposure, access), the interface
provider wires HTTP by position (`("get_task", Api::DEFINITIONS[0])`),
the consumer names its slot twice (`TasksApi::new("$tasks")` and
`TasksApi::import("$tasks")`), and `external!` returns fixed arrays that
`App`'s static slices cannot splice, so `external-transactions` lists
`WORDS_DEFS[0]` … `WORDS_DEFS[10]` and `WORDS_HTTP[0]` … `[8]` by hand
(44 lines of index bookkeeping).

Cost: renaming or removing a method means editing up to five places;
forgetting one is a runtime 404, an unguarded method (the access list
falls back to `default` for a misspelled alias) or a stale export. The
six parallel `App` lists also force the raw path on anything the
attributes cannot say, which is why the largest example is untyped.

Proposed: **one declaration per operation, one list per app.**

- `#[query]`, `#[mutation]` and `#[derived]` take every per-operation
  option: `name = "pizza.order"`, `http` (callable over HTTP under its
  name; see below), `consistency = replica_local` /
  `max_staleness_ms = 500`, `access = public | authenticated | path`,
  `export` / `callers("billing")`, `timeout = "250ms"` (see 5).
- Collections, queues, keys, the authorization component, externals,
  interface providers and imports are all *components*, and
  `App::new(&[...])` takes one flat list of them. Composite components
  (an external value, an interface provider) bring their own
  definitions, collections and triggers.
- The manifest's `http`, `exports` and the authorization hook's
  per-method access are generated from the declarations.
- An interface consumer's `static TASKS: TasksApi = TasksApi::import("$tasks")`
  is both the call handle and the import; a provider registers
  `TasksProvider::<P>::COMPONENT`.

```rust
#[mutation(name = "pizza.order", http, access = owns)]
fn order(ctx: &mut MutationCtx, shop: ShopId, id: String, quantity: Quantity) -> Result<Order> { ... }

// The oven timer's handler: no option, so no HTTP alias.
#[mutation]
fn finish_baking(ctx: &mut MutationCtx, order: OrderId) -> Result<()> { ... }

#[query(name = "pizza.shop.local", http, consistency = replica_local, access = owns)]
fn shop_local(ctx: &mut QueryCtx, shop: ShopId) -> Result<Summary> { ... }

pub static APP: App = App::new(&[
    &Shops, &Orders, &ORDER_STATS, &DELIVERIES, &AUTH,
    &SETUP, &ORDER, &FINISH_BAKING, &DELIVER, &TIP, &SHOP, &SHOP_LOCAL, ...
]);
```

v1: TypeScript put `args`, `access` and `consistency` on the method and
registered it by listing its alias in `http` (one declaration plus one
map entry); v1's Rust SDKs had the same multi-list problem v2 inherited.

**What answers over HTTP, and why.** Nothing, unless its declaration
says `http`: `#[query(http)]`, `Method::query(..).http()`,
`Transaction::new(..).http()`, `external!(.., http)` for an external
value's nine worker operations, `TasksProvider::<P>::COMPONENT.http()`
for an interface provider. This is v1's rule: its `http` map was an
allowlist (`define()` exposed only the aliases it listed, `{}` when
absent, and the server answered `METHOD_NOT_FOUND` 404 for the rest), and
parity's `App.http` was the same. A default of "public unless
`internal`" would make every timer and queue handler, trigger target,
method written for other modules and interface provider reachable by
anyone who can name it, and the review found it had done exactly that
(`flower-guest-crypto`'s `issue_session`, which signs a session for any
subject, and `flower-guest-test`'s `sign_message` and the `send_email`
timer handler). Opting in keeps the one declaration: the flag sits on
the operation, not in a second list. Who may call an exposed operation
is a separate question, answered by its `access` (or the authorization
component's `methods` table for what another macro declares, see 18).

### 3. Watches take base64url arguments in the query string

```
GET /v1/watch?db=shop&module=pizza&name=pizza.shop&args=WyJ0ZW5hbnQtMSIsInN0b3JlLTAiXQ&principal={...}
```

Cost: a watch cannot be written by hand or read in a log; every client
(and the docs) needs a base64url step that no other call has; the
principal rides the URL as JSON while the arguments do not; and
credentials cannot ride at all ("watches take the bearer only").

Proposed: `POST /v1/watch` with exactly the `/v1/query` body
(`{db, module, name, args, credentials?, principal?}`), answered as
server-sent events. The client streams the response body as it already
does; `cargo flower watch` prints events. The GET form goes.

v1: `POST /v1/watch` with a JSON body `{name, args?, credentials?}`; no
GET, no base64. v2 regressed here.

### 4. Arbitrary limits on transaction size

- `MAX_MULTI_STEPS = 64`: a multi-database transaction may name at most
  64 participants (`flower-engine/src/engine.rs`).
- `MAX_PLANNED_CALLS = 1024`: a planned transaction may make at most
  1,024 calls.
- What actually bounds a write, the size of its replicated entry, is
  found out late: the Raft frame cap (`--raft-max-frame-bytes`, 256 MiB)
  fails the *replication*, with an operator-facing message.

Cost: counts are neither resources nor anything the developer chose: 65
small steps fail while one enormous step passes; the real bound fails
late and opaquely.

Proposed: drop the count caps. Bound a transaction by what it costs:
its committed entry's bytes, checked when the entry is built, before it
is proposed, failing `transaction_too_large` (HTTP 413) with the
measured and allowed sizes. The bound is the operator's
`--max-transaction-bytes` (`FLOWER_MAX_TRANSACTION_BYTES`, default 32
MiB like v1), validated at startup to fit the peer frame; the request
body limit stays the outer bound on what callers send, and the
invocation deadline (5) bounds the work. Internal safety bounds that are
not developer-facing (the read set's overflow point, call nesting)
stay, documented as such.

v1: no row, write or call-count caps; byte caps set by the operator
(`FLOWER_TRANSACTION_MAX_BYTES` 32 MiB, failing `413 RESULT_TOO_LARGE`).

### 5. Fuel instead of declared limits

Three knobs today, none stated where the developer means them:

- engine "fuel" units per call (host operations cost 1, rows 1, nested
  calls 10: `DEFAULT_FUEL = 1_000_000`), overridable per call only by a
  trusted gateway's `fuel` body field (capped by `--max-fuel`);
- Wasm instruction fuel (`--guest-fuel`, 100,000,000);
- a wall-clock deadline (`--guest-timeout-ms`, 5 s).

Cost: a developer whose report query legitimately reads many rows has
no way to say so; they find out from `budget_exhausted` and must ask the
operator to raise a process-wide knob for every module, or have a
gateway send a magic number. Fuel units mean nothing to anyone, and the
query cache and single-flight key on the fuel override.

Proposed: limits are stated declaratively, as time, where they are
meant:

- the developer declares a method's budget as a deadline:
  `#[query(timeout = "2s")]` (default: the operator's default);
- the operator sets the default and the ceiling
  (`--guest-timeout-ms`, `--guest-max-timeout-ms`);
- callers state *their* deadline (`x-flower-timeout-ms`, already sent
  by the client), which can only tighten the method's;
- the engine derives the internal instruction and host-operation fuel
  from the effective deadline (a calibrated rate, operator-tunable), so
  outcomes stay deterministic for a given budget but nobody configures
  fuel. The `fuel` request field, `--max-fuel` and `--guest-fuel` go.

v1: no fuel at all; one operator wall-clock deadline (10 s); callers'
`x-flower-timeout-ms` only shed queueing; no per-method limits. The
proposal keeps v2's determinism and adds the per-method statement v1
lacked.

## Further papercuts found

### 6. The typed path cannot express what real modules need, so they go raw

Goblin Pizza, crypto-ticket and external-transactions are written
against `Method::new(name, |ctx, Value| ...)`, declare an argument
schema with `.args(v::object(...))`, then parse the same fields again by
hand through local `fields`/`number`/`text` helpers and re-check the
ranges the schema already enforced (`stores` 1..=64 appears in both).

Proposed: with 2 the typed attributes say everything the raw path did,
and `#[derive(FlowerType)]` gains field constraints so the schema comes
from the type once: `#[flower(range = 1..=64)]`,
`#[flower(length = 1..=96)]`, `#[flower(pattern = identifier)]`. The
examples move to typed structs.

v1: TypeScript inferred types from `v.*` schemas; the typed Rust SDK
used `#[derive(FlowerType)]` structs as the schema.

### 7. A collection is declared twice, and its typed form hides a static

```rust
static SHOPS: Collection = Collection::new("pizza.shops");
const SHOPS_REF: CollectionRef<'static> = CollectionRef::local("pizza.shops");
```

`#[collection] pub struct Rows;` registers through a generated
`ROWS_COLLECTION` static the developer must know by name, and
references resolve targets by rewriting the marker's path to that name.

Proposed (with 1): a `Collection` is itself the handle
(`SHOPS.get(ctx, &id)`, `ctx.get(&SHOPS, &id)`); `#[collection]` markers
register as themselves (`App::new(&[&Rows])`) and reference targets name
markers directly.

### 8. Wrapping a context consumes it

`wrap_mutation(ctx)` takes `&'c mut Ctx<'c, 'r>`, so wrapping uses the
context up for the rest of the function: Goblin Pizza's comments read
"The last use of the context: it becomes the mutation context timers
take", and its code is ordered around that. `Crypto::new(ctx.ctx())` and
`config(ctx.ctx())` need the raw escape hatch.

Proposed: typed methods receive `MutationCtx`/`QueryCtx` directly (with
2 nothing needs the raw `Ctx`), wrappers reborrow
(`&'a mut Ctx<'c, 'r> -> &'a mut MutationCtx<'c, 'r>`), and `Crypto`,
timers and queues take the typed contexts.

### 9. Reading an aggregate is untyped

```rust
let mut group = Fields::new();
group.insert("group".to_string(), Value::Array(vec![id.clone()]));
let stats = ctx.get_derived("", OrderStats::NAME, &object(group))?;
```

and its source needs a hand-written `TypedCollection` marker repeating
`"pizza.orders"`.

Proposed: `OrderStats::get(ctx, &(shop,))` decodes the accumulator; the
source is the `#[collection]` marker.

### 10. Timers: handler by string, untyped arguments, narrow ID alphabet

```rust
Timers::after(wrap_mutation(ctx), &format!("bake-{}", hex(&order_key)),
              bake_ms as u64, "pizza.finishBaking", object(bell), &ScheduleOptions::default())?;
```

IDs must match `[A-Za-z0-9._-]{1,128}`, so Goblin Pizza hex-encodes its
canonical order key (and carries a `hex` helper for it); the handler is
a string checked only when the timer fires; the arguments are a `Value`.

Proposed: `Timers::after(ctx, &id, Duration, &FINISH_BAKING, &args)`:
the handler is the method descriptor, its arguments its own argument
type, the delay a `Duration`, options defaulted. IDs are any non-empty
UTF-8 without control characters, still at most 128 bytes (the bound
the timer index's fixed-width keys need, documented).

v1: `scheduler("pizza.ovens", { bake: finishBaking }).after(ctx, id, ms, "bake", args)`
with args checked at scheduling; IDs any non-empty string.

### 11. Queues: name string beside the declaration

```rust
static DELIVERY_QUEUE: QueueDef = QueueDef::new(DELIVERIES).payload(...);
Queue::enqueue(wrap_mutation(ctx), DELIVERIES, object(payload), &EnqueueOptions::default().scope(&tenant).named(&order_key))?;
```

`TypedQueue<P, R>` exists but is separate from `QueueDef`, so the
contract and the handle are two declarations.

Proposed: one `Queue<P, R>` static is the component, the contract (from
`P`, `R`) and the handle: `DELIVERIES.enqueue(ctx, &delivery, options)`.

v1: `queue<Delivery, Result>("pizza.deliveries", {...}).scope(t).enqueue(ctx, id, payload)`.

### 12. HTTP: a query's business failure is a 500

A mutation failing `OUT_OF_STOCK` answers 200 `{"ok": false, ...}`; a
query failing `SHOP_NOT_FOUND` answers **500**, which the client reports
as `FlowerError::Server` (not `Business`) unless the failure happens to
carry details, and which proxies and retry layers treat as a server
fault.

Proposed: a call's own failure is the same everywhere: **422** with
`{"ok": false, "code", "message", "details"?}` (plus `revision` and
`position` for writes), for queries, mutations, transactions and watch
`error` events; the client maps every one to `FlowerError::Business`.

v1: 422 for every guest failure, 403 for the authorization hook.

### 13. Typed client calls repeat what the guest already declared

```rust
client.typed_query::<ShopArgs, Summary>("shop", "pizza", "pizza.shop", &args, &RequestOptions::default())
```

Five positional arguments, a turbofish, and an argument struct the
caller must write because the macro's is hidden
(`__Flowerget_taskArgs`).

Proposed: the method macros emit a public argument struct
(`OrderArgs`) and a typed descriptor the client takes:
`client.call("shop", "pizza", &pizza::ORDER, &OrderArgs { .. })`
(query or mutation picked from the descriptor, result decoded as the
method returns it, declared errors typed). The untyped JSON calls stay.

v1: `FlowerClient<typeof app>` typed every alias from the module type.

### 14. Choosing query versus mutation is the caller's job

`/v1/query` and `/v1/mutate` are separate, and `cargo flower call`
needs `--mutation`, although the module's manifest already says which a
name is.

Proposed (*small*): `cargo flower call` asks the module (`modules`
listing) or tries the declared kind; the separate routes stay because
they differ in routing and idempotency.

### 15. CLI: the endpoint on every command

Every client command requires `--endpoint`, with no environment
default, while tokens already read `FLOWER_ADMIN_TOKEN`/`FLOWER_TOKEN`.

Proposed (*small*): `FLOWER_ENDPOINT`, defaulting to
`http://127.0.0.1:8080`.

### 16. Consistency is restated on every query

`Definition::Query(&X, Consistency::Linearizable)` appears in
`definitions`, `http` and exports for every query.

Proposed: linearizable is the default (folded into 2).

### 17. Found while writing the docs (the docs session, `web/docs`)

- Guest crypto failed on a server started without key flags:
  `sha256`, and `JwtBearer` with a raw or PEM key, trapped with "no
  crypto manager is configured". **Now** `serve` always configures a
  crypto host; without key flags it holds no keys, and only a managed
  key nobody provided fails.
- A crypto-only module failed to build ("guest must import host_call
  exactly once": the import is dead-stripped). **Now** `host_call` is
  optional, as `crypto_call` was.
- `#[collection]` took no access policy, so a guarded collection had to
  be declared by hand. **Now** `#[collection(..., access = policy)]`.
- `#[derive(FlowerError)]` enums did not convert from `Failure`, so every
  `?` on an SDK call needed a `map_err`, and a variant's message was its
  name. **Now** `#[flower(message = "...")]`, and one
  `#[flower(other)] Host(Failure)` variant takes every other failure
  (generating `From<Failure>`).
- One callback under two consistencies needed a second wrapper
  function. **Now** `SHOP.named("pizza.shop.local").consistency(..)`
  makes a twin descriptor (Goblin Pizza's local read is one).
- Queue and timer reads (`Queue::get`/`stats`/`ready`,
  `Timers::get`/`scan`, external values' reads) took the raw `Ctx`, so
  callers wrote `ctx.ctx()`. **Now** they take any context.

### 18. Found in review (after the pass)

An independent review of the SDK, macros, client and examples found
seven problems; each now has a test that failed first.

1. **Exposure defaulted to public.** Methods and transactions answered
   over HTTP unless `internal`, and interface providers could not opt
   out. **Now** nothing does without `http` (see 2 for the rule and
   why); interface providers and timer/queue handlers are off HTTP, and
   the test guests expose nothing, as on parity.
2. **Per-definition access was lost** for transactions, `external!`
   operations and interface providers (`Authorization.methods` had gone,
   so a module whose default is `Check(owns)` could not give
   `words.publish` `Check(worker)`). **Now** `Transaction::access(..)`,
   and `Authorization { methods: &[("words.publish", Access::Check(worker))], .. }`
   for definitions another macro declares. The manifest refuses a name
   the app does not define, one named twice, and one whose definition
   declares its own access, so a typo cannot silently leave the default.
3. **Crypto ticket's wire name.** `ticket.issue` took `lifetime_seconds`
   instead of v1's and parity's `lifetimeSeconds` (and its tests had
   been changed to match). **Now** `rename_all = "camelCase"`, and the
   tests are parity's again.
4. **The typed client guessed** a query's declared failure from its
   code, so a 404 `UNKNOWN_METHOD` became the method's declared error.
   **Now** only a 422 is the call's own failure: the server answers every
   failure carrying details 422, and `#[derive(FlowerError)]` requires
   `UPPER_SNAKE_CASE` codes (v1's rule, which the server's 422 relies
   on), so every declared failure reaches the client as 422.
5. **`export_to(&[])` exported to everyone.** **Now** `Export::Only(&[])`
   is an allowlist naming nobody, as on parity.
6. **Bad arguments answered `SCHEMA_MISMATCH`** where v1 and parity
   answered `INVALID_ARGUMENT`. v1 had one rule (its TypeScript and Rust
   SDKs both answer `INVALID_ARGUMENT` with `details.path` when a
   definition's argument schema refuses). **Now** the generated check
   does the same, with `details.path` as member names and indexes.
   Engine-side checks (another module's call, a timer's or queue's
   arguments, a transaction plan) still answer `SCHEMA_MISMATCH`, as
   parity did.
7. **Macro hygiene.** A parameter named like its function (`fn shop(ctx,
   shop: ShopId)`) shadowed it in the generated call; a raw identifier
   (`r#type`) panicked the macro; and under `rename_all`, `a_b` and `aB`
   shared a wire name silently. **Now** the call takes its arguments
   straight from the decoded struct, raw identifiers are plain names
   (`TypeArgs`, `"type"`), and two fields with one wire name are a
   compile error.

### 19. Found in the runtime review (after the pass)

A second review, of the engine, server, cluster and client side, found
ten problems; each now has a test that failed first, except where noted.

1. **Watches kept the subscribing request's deadline** for every
   refresh, so one subscriber's `x-flower-timeout-ms` ended the shared
   evaluation for everyone. **Now** a watch's later rounds run without
   it; it bounds the first snapshot only.
2. **No transaction-wide time bound** once the count caps went: every
   step and planned call got a fresh deadline and fuel, and the step
   check compared every pair, in the writer. **Now** a transaction has
   one deadline and logical budget (its planner's declaration, or
   `TRANSACTION_TIMEOUT`, 60 s, for client-planned steps), and step
   lists are checked in linear time before the writer or any hook.
3. **The byte bound did not bound the entry proposed**: a batch of
   writes, a business failure or a cross-group decision could outgrow
   it and the peer frame, stalling the group. **Now** each is bounded,
   and the frame cap is checked where it matters: a dial's hello (peer
   protocol 4) says the dialer's largest entry and a member whose frames
   cannot carry it refuses the dial.
4. **A deadline in the authorization hook answered 403**, telling
   clients to log in again. **Now** it is never a denial: the caller's
   deadline passing is `caller_deadline_exceeded` (503, retryable), the
   hook's own limits its module's failure. As v1 told them apart, a
   call past its own deadline is `deadline_exceeded`, its own failure
   (422, as v1's `EVALUATION_BUDGET`), and one whose caller's deadline
   came first is `caller_deadline_exceeded`.
5. **Watches took a gateway's pinned clock**, freezing clock-reading
   queries and token expiry. **Now** watches read the member's clock.
6. **Fuel depended on member flags** (`--guest-timeout-ms` scaled it,
   `--guest-max-timeout-ms` capped it), so a call could run out on one
   leader and succeed on another. **Now** fuel follows the declaration
   and constants only; the flags bound wall-clock time.
7. **A cross-group prepare could outlive the caller**: delivery retried
   for 10 s without the deadline and participants restarted the clock.
   **Now** delivery runs under the caller's deadline and the request
   carries the deadline itself too.
8. **Declared timeouts ignored**: the planner ran outside any budget and
   planned calls took the planner's timeout. **Now** both run under
   theirs. Task entries declaring `timeout_ms` are refused at deploy.
   Reducers did not hold up: they are derived values, whose
   `timeout_ms` the manifest already refused.
9. **Status mapping**: an oversize cross-group transaction answered
   422, instantiation failures and a missing runtime 422, malformed
   bodies 422 in text (which the client then failed to decode). **Now**
   413, 503, and 400 with the usual envelope; the client reads a 422 as
   a reply only when it is a call envelope.
10. **Each watch held up to 2 MiB of arguments and credentials**,
    cloned on every refresh, without a bound. **Now** they are shared
    and the hub holds at most 64 MiB of them (an overload past it).

## Order of work

1. Components and the operation attributes (2, 6, 7, 16), with
   generated `http`, exports and per-method access.
2. Typed keys (1) and contexts (8, 9).
3. Watches by POST (3) and one failure status (12).
4. Declared deadlines and byte bounds (4, 5).
5. Typed timers and queues (10, 11).
6. Typed client descriptors and the CLI (13, 14, 15).

Each step updates the examples, the tests, `spec.md` and `PARITY.md`,
and bumps the compatibility contract's `state_machine` where manifests
or apply change.

## Status

| Entry | State |
|---|---|
| 1. `key_text` | Done: typed handles (`#[collection]`, `Source`, `Index`, aggregates) take keys; the raw context takes `Value` keys (see the raw-layer note below) |
| 2. One declaration per operation | Done: method options, `App::new(&[...])`, generated `http`/exports/access, interface `COMPONENT` and `import`, `external!` as a component, static component arrays as groups |
| 3. Watches by `POST` | Done: `POST /v1/watch` with a query's body; client, server tests and the dashboard demo moved |
| 4. Transaction size | Done: count caps removed; `--max-transaction-bytes` (32 MiB), 413 `transaction_too_large`, validated against the peer frame |
| 5. Fuel | Done: `timeout = "..."` per method, `--guest-timeout-ms` default and `--guest-max-timeout-ms` ceiling, the caller's `x-flower-timeout-ms` only shortens; fuel derived from the declaration; `fuel` request member, `--max-fuel` and `--guest-fuel` removed |
| 6. Typed path for real modules | Done for the examples (all typed now), with `rename_all = "camelCase"`, `bare_arg` and optional `Option` parameters so Goblin Pizza keeps v1's wire contract. Field constraints in `FlowerType` (`range`, `length`) are not built: bounds stay checked in code |
| 7. Collections declared once | Done for `#[collection]` markers (the marker is the component); raw `Collection` statics still pair with `CollectionRef` for raw calls |
| 8. Contexts | Done: wrappers reborrow, `ctx.crypto()`, the raw `Ctx` reads through `ReadCtx` |
| 9. Aggregate reads | Done: `TypedAggregate::get(ctx, &group)` |
| 10. Timers | Done |
| 11. Queues | Done: `TypedQueue<P, R>` is the component and the handle |
| 12. 422 for a call's own failure | Done |
| 13. Typed client descriptors | Done: `#[query]`/`#[mutation]` emit `Op<A, T, E>`; `FlowerClient::call(db, module, &OP, &args)` takes name, kind and types from it; typed timers take it too, so their arguments are checked at compile time |
| 14. Query or mutation chosen by the caller | Not built: `cargo flower call` still needs `--mutation`. Typed callers no longer choose (13); the dynamic CLI would need the server to resolve an alias's kind (a `POST /v1/call`), left as a follow-up |
| 15. `FLOWER_ENDPOINT` | Done |
| 16. Consistency restated | Done (folded into 2) |
| 17. Found while writing the docs | Done: keyless crypto host by default, optional `host_call`, `#[collection(access = ..)]`, `FlowerError` messages and catch-all, `named` twins, typed-context reads |
| 19. Found in the runtime review | Done: standing watches (no request deadline, the member's clock, shared and bounded arguments), one deadline and budget per transaction, linear step checks before the writer, every proposed entry bounded and the frame cap checked at the hello, the caller's deadline (`caller_deadline_exceeded`, 503) told from the call's own (`deadline_exceeded`, 422), member-independent fuel, cross-group deadlines, planner and planned-call timeouts, statuses (413, 503, 400) |
| 18. Found in review | Done: HTTP exposure opt-in everywhere (`http`), `Transaction::access` and `Authorization.methods`, crypto ticket's `lifetimeSeconds`, client failures classified by status (422 only; `UPPER_SNAKE_CASE` declared codes), fail-closed `export_to(&[])`, `INVALID_ARGUMENT` with `details.path`, macro hygiene |

Compatibility at this review's original pass was `raft_wire` 6,
peer hello version 4 and `state_machine` 8. The current contract is
`wire7-state12-snapshot1-value4-wasmtime-49.0.0`: SQL contracts, durable
participant footprints with certified derived sources, resize plans and leader preferences have since
advanced it. See `PARITY.md` for the completed parity work and
`web/docs/guides/operations.html` for the current operator interfaces.
