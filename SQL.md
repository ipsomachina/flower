# SQL: bounded, read-only queries over collections

SDK convenience APIs accept any `FlowerType` parameter object, including `Value`.
Both plan and endpoint handles offer `explain_typed`, returning `SqlExplain` with
typed resource ceilings and the original response for extension fields.
`SqlBudget::new` names ceilings independently of access policy; `scan` remains
available. Parser diagnostics carry zero-based UTF-8 `start_byte` and exclusive
`end_byte` offsets; an end-of-input error has an empty span. Evaluator budget
errors include the exhausted budget dimension in `details.budget`.

Status: **implemented for single-collection reads**. Named typed plans,
request-time SQL through scoped SDK endpoints,
deployment binding, bounded scans, primary-key lookups, complete index equality
buckets, logical value ordering and `EXPLAIN` are implemented. Stored value-ordered
indexes, joins, aggregates, cursors and administrative ad hoc SQL remain future
work, with their acceptance conditions below.

SQL is another way to write a Flower read. A plan uses the invocation's pinned
snapshot and staged overlay, existing collection policies, dependency tracking
and shared fuel. It cannot write, cross databases, confer definer authority or
create an HTTP endpoint. An ordinary query chooses the public API and access.

## Request-time SQL through a module query

Declare one capability with fixed collections and ceilings, then expose it with
an ordinary module query. SQL text and parameter values may change on every call:

```rust
use flower_sdk::{App, QueryCtx, Result, SqlBudget, SqlEndpoint, Value, query};

static SQL_ENDPOINT: SqlEndpoint = SqlEndpoint::new(
    "orders.sql", &[Orders::NAME],
    SqlBudget::scan(1_000, 1_048_576, 1_000_000),
);

#[query(http)]
pub fn sql(ctx: &mut QueryCtx<'_, '_>, statement: String,
           params: Value) -> Result<Vec<Value>> {
    SQL_ENDPOINT.run(ctx, &statement, &params)
}

static APP: App = App::new(&[&Orders, &SQL_ENDPOINT, &SQL]);
```

The [sql-queries example](examples/sql-queries/src/lib.rs) includes this endpoint,
its `sql` query and a separate `sql_explain` query. Build/deploy the module once;
then call it through the normal query interface:

```console
cargo flower call --db demo --module shop --name sql --args '{"statement":"SELECT _key AS id, total FROM orders WHERE owner = :owner AND total >= :min_total ORDER BY total DESC LIMIT 20","params":{"owner":"ada","min_total":"100"}}'
```

`run` returns projected JSON objects (`Vec<Value>`). `run_as::<Row>` optionally
decodes them into a `FlowerType` contract. `explain(ctx, statement, params)` binds
the supplied statement and parameters without reading rows. It reports binding
work, the endpoint's configured ceilings, and the plan's remaining execution
ceilings. The example exposes this through the ordinary `sql_explain` query.

Each endpoint is listed in `App`. Its manifest declaration is
`sql_endpoints: [{name, collections, candidates, bytes, work, allow_scan?}]`.
Deployment validates a nonempty, unique allowlist of 1–64 declared local
collections and positive ceilings. The host reads scope and limits from that
declaration; they are not query arguments. A same-named SDK handle with altered
fields cannot widen the deployed capability. There is no cross-module source or
raw namespace access. `SqlEndpoint::require_index()` rejects individual requests
that lack a proven point/index-equality path.

Parameters are an object with exactly the names used by the statement (at most
64), or `null`/`{}` when there are none. Values supply their initial logical types;
comparisons with columns refine them. Decimal-column comparisons require decimal
strings and preserve the full integer range. Ordinary numeric comparisons do not
inherit a column's min/max constraints. SQL parameters may be NULL, and conflicting
column type contexts fail binding. There is no string interpolation or implicit
string-to-number conversion. Result types follow the projection. Missing/redacted
members stay omitted. Request-owned maps and ASTs are dropped after execution;
no per-request static schema allocations are leaked.

The wrapping query chooses HTTP exposure and authorization. Execution inherits
the containing invocation's principal, collection policies, snapshot, staged
writes, dependencies and shared fuel. Binding costs SQL/parameter work before
reading rows, and reduces the endpoint's remaining execution budget. The 16KiB
statement/parser limits and current read-only single-collection dialect apply.
The SDK endpoint itself does not create an HTTP alias or an administrative SQL
route. Named typed plans remain useful for a stable application contract.

## Named typed plans

See the buildable [sql-queries example](examples/sql-queries/src/lib.rs).
Parameters and result rows are explicit `FlowerType` contracts:

```rust
#[collection(name = "orders", key = String, value = Order,
             index(name = "by_owner_status", fields(owner, status)))]
pub struct Orders;

#[derive(FlowerType)]
pub struct OpenOrdersParams {
    pub owner: String,
    pub min_total: DecimalI64,
    pub limit: u32,
}

#[derive(FlowerType)]
pub struct OpenOrder {
    pub id: String,
    pub total: DecimalI64,
}

static OPEN_ORDERS_PLAN: SqlQuery<OpenOrdersParams, OpenOrder> = SqlQuery::new(
    "orders.open",
    "SELECT _key AS id, total FROM orders
     WHERE owner = :owner AND status = 'open' AND total >= :min_total
     ORDER BY total DESC, _key ASC LIMIT :limit",
    SqlBudget::scan(1_000, 1_048_576, 1_000_000),
).require_index();

#[query(http)]
pub fn open_orders(ctx: &mut QueryCtx<'_, '_>, owner: String,
                   min_total: DecimalI64, limit: u32) -> Result<Vec<OpenOrder>> {
    OPEN_ORDERS_PLAN.run(ctx, &OpenOrdersParams { owner, min_total, limit })
}

static APP: App = App::new(&[&Orders, &OPEN_ORDERS_PLAN, &OPEN_ORDERS]);
```

List the plan and collection in `App`; listing the query alone does not register
its plan. `SqlQuery<P, R>` is both a component and a typed handle. It works from
queries, mutations and derived computations through `ReadCtx`.

The manifest carries `sql: [{name, statement, params, row, candidates, bytes,
work, allow_scan?}]`. Deployment parses and binds each statement against the local collection
catalog, rejects unknown fields and parameters, checks comparisons and result
contracts, and retains the bound plan. A new deployment rebinds it. Names are
nonempty and unique within the module. Calls send only `[name, params, explain]`
through `HostOp::Sql` (35); runtime SQL text and caller-supplied IR are refused.
No macro assumes it can discover another module's collection declarations or
invent a module-visible row type from an expression's expected Rust type.

`require_index()` emits `allow_scan: false` and makes deployment fail if no safe
primary-key or secondary-index path exists. Omitting this option permits bounded
scan fallback; the explicit candidate, byte and work budgets still apply.

Inspect a deployed plan with `OPEN_ORDERS_PLAN.explain(ctx)`, or inspect its
manifest offline:

```console
cargo flower sql-explain --manifest .flower/sql-queries.manifest.json --plan orders.open
```

`EXPLAIN` version 2 reports the selected path, index name/equality fields,
logical sort, dependency scope and probe ceilings without showing parameter or
sort values. `input_bound_proven: true` and `candidate_bound: 1` describe a point
lookup; index equality buckets and scans report no cardinality proof. Explaining
a plan does not read collection rows.

## Implemented dialect

One statement has this shape:

```sql
SELECT expression [AS column], ... | *
FROM local_collection
[WHERE predicate]
[ORDER BY expression [ASC | DESC], ...]
LIMIT positive_integer_or_named_parameter
```

- Expressions: schema-bound field paths, tuple elements (`_key[0]`), `_key`,
  `:named_parameters`, finite numeric literals, SQL strings with doubled quotes,
  booleans and `NULL`. Constants and tuple-element projections require `AS`.
- Predicates: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`,
  parentheses, `NOT`, `AND`, `OR`, with their usual precedence.
- Projection aliases are output names. Sort expressions name source fields or
  parameters, rather than referring to projection aliases.
- Keywords ignore case. Collection names, wire fields and parameters retain
  their exact case. Quoted identifiers and SQL comments are not supported.
- The total `LIMIT` is mandatory and lies in `1..=10_000`. There is no `OFFSET`.
  The result is a complete `Vec<R>` at one snapshot; there is no cursor.
- Parameters form a closed object, with required members; nullable members may
  carry explicit `NULL`. Use `()` when a plan has no parameters. Unknown,
  omitted, mistyped or out-of-range parameters fail before collection scanning.
- The row contract is a closed object with exactly the projected column names.
  `SELECT *` expands the collection's object fields, excluding the synthetic key.
  SQL reserves `_key`; a collection with a stored member of that name is refused.

Unsupported syntax fails deployment. This includes qualified/cross-module
sources, joins, functions, arithmetic, `IN`, `BETWEEN`, `LIKE`, `DISTINCT`,
aggregates, grouping, subqueries, CTEs, unions, windows and additional statements.
Parser ceilings are 16KiB, 2,048 tokens, 128 predicate nodes, nesting/path depth
32, 64 projected columns and eight sort expressions.

## Access planning

The planner extracts field/literal or field/parameter equality from required
`AND` conjuncts, including reversed operands. An equality inside `OR` or `NOT`
does not select an access path. Every candidate still passes the full predicate
after redaction, and sorting/limiting stays in logical value order.

1. A whole-key equality selects a point lookup when logical equality matches
   canonical byte equality. Scalar strings/enums, booleans and numbers qualify,
   as do tuples of those types. Decimal keys, including decimal tuple elements,
   keep the scan path because distinct strings such as `"07"` and `"7"` compare
   equal numerically. A staged replacement/deletion takes precedence over the
   committed row, so a point read observes at most one candidate.
2. A legacy secondary index qualifies only when **every** component has a
   required equality, with string/enum, boolean or numeric logical types.
   Decimal and JSON components are excluded. NULL equality parameters yield an
   empty result. This coverage proof works for optional/nullable fields too:
   missing or null components cannot satisfy their required equality. A partial
   prefix is not sufficient, even if the trailing schema fields are required.
3. Point lookups take priority. When several complete equality indexes qualify,
   the index name breaks ties deterministically, independently of catalog order.
   SQL considers indexes with at most 64 components. Otherwise it selects the
   explicit bounded scan, or rejects deployment for `require_index()`.

Nonunique equality buckets can be arbitrarily large. Their physical committed
and staged entries must fit the candidate cap before any row/predicate is
filtered. A single index seek and at most one record probe per admitted candidate
are sufficient; `EXPLAIN` reports both ceilings. Exhaustion fails rather than
changing access paths at runtime. Newly deployed indexes are backfilled before
the new plan activates. Removing an index rebinds/revalidates plans and fails
deployment if it would leave a required indexed plan without a safe path.

## Logical types and order

Stored index order remains canonical-JSON **length then bytes**, and still omits
missing and null components. It is not SQL order. SQL's executor uses its own
logical comparisons, including for `_key` and deterministic ties:

- Numbers compare numerically, strings by UTF-8 byte order, booleans false first.
  Scalar/tuple keys are ordered by their schemas, recursively for tuples.
- `DecimalI64` and `DecimalU64` have explicit `decimal_i64` and `decimal_u64`
  schemas. Their wire values remain decimal strings. Comparisons cover the full
  integer range exactly, without converting through floating point. Signed and
  unsigned decimal types are distinct; use a matching typed parameter rather
  than a numeric or string literal in decimal predicates.
- The new schema validates decimal syntax and range. Existing manifests whose
  schema says `string` retain string semantics; old bytes are not reinterpreted
  as decimal metadata. Redeploying a decimal schema is an explicit contract change.
- Explicit `NULL` sorts first ascending and last descending. Missing/redacted
  sort values share that position. `_key ASC` breaks all remaining ties, including
  when an explicit sort uses descending order. Without `ORDER BY`, keys sort in
  logical ascending order, independent of storage scan order.
  If distinct stored decimal keys have equal numeric values (e.g. legacy `"07"`
  and `"7"`), their canonical JSON bytes break the remaining tie.
- JSON objects and arrays permit equality where their contracts match, but have
  no SQL order. Ordered tuple keys must have ordered element types.

Missing and redacted members cannot be distinguished on the existing read wire;
both are absent. Predicates involving absent values evaluate to `UNKNOWN`,
including `IS NULL` and its negation. Explicit null satisfies `IS NULL`, but
ordinary comparisons involving it are `UNKNOWN`. Boolean operations use
three-valued logic; `WHERE` keeps only `TRUE`. An absent projection stays absent,
and an explicit null stays null. A null parent propagates null through a nested
path; an absent parent propagates absence.

## Policy equivalence is a planning requirement

The host applies the same row visibility and field redaction as an ordinary
collection scan, **before** SQL predicates, projection and ordering. SQL runs with
the authority of the containing invocation. Derived invocations keep Flower's
existing definer semantics; an invoker query does not acquire them by using SQL.

For a collection with an access policy, value projections must be optional in
the result contract, conservatively including fields the current rule allows.
Use `Omittable<T>` (or `Omittable<Option<T>>`) when returning those rows from a
typed query: it re-encodes absence correctly. `Field<T>` can decode a redacted
read internally, but its existing checked encoding rejects redacted values.
The key remains available on visible rows.

Index access selects raw candidates, then uses ordinary row/field policy with
no native range-field suppression. The full required equality for every indexed
component proves coverage: a redacted predicate field cannot become `TRUE`, and
missing/NULL fields cannot satisfy the equality. Hidden sort/projection fields
do not suppress otherwise visible rows. This preserves scan/filter/sort results
without relying on the native range operation's extra index-field visibility
guard. Logical range ordering and incomplete index prefixes remain scan paths.

The same rule excludes automatic reuse of definer-maintained aggregates. Matching
`GROUP BY` columns is insufficient: input predicates, row policies, field
visibility, null handling, authority and maintenance semantics must all match.

## Resource and dependency contracts

`SqlBudget::scan(candidates, bytes, work)` supplies explicit ceilings and permits
a bounded scan fallback. `require_index()` removes that permission. `LIMIT`
never bounds input work: all candidates of the selected path must be considered
before applying it, because key and sort order are logical rather than storage
order.

- **Candidates** count physical committed rows and staged record entries for a
  full scan, or committed/staged index entries for an equality bucket, including
  hidden rows, rejected predicates, overwritten records and staged deletions.
  One additional non-decoding probe detects overflow. Ceilings are conservative:
  a staged replacement counts alongside its committed version. The host reserves
  the declared candidate count from shared fuel, including on failed scans.
  Index buckets reserve twice that count plus one for their entry/record probes;
  a point lookup reserves one and uses the overlay before the committed record.
- **Bytes** bound decoded scan payloads and per-row framing allowances, plus
  parameter, projected-row, key and sort payloads. Stored expansion lengths are
  checked before decompression, and staged lengths before copying. This is a
  logical retained-byte budget, not an exact allocator/RSS measurement; row and
  expression ceilings also bound object/container overhead. Defaults are never
  inferred from `LIMIT`.
- **Work** charges decoded input/metadata bytes even for hidden or rejected
  rows, followed by expression steps, value bytes/nodes inspected by comparisons,
  projection input and fallible sorting. Work shares the invocation's remaining
  fuel, including failures. Input reads are restricted by remaining work/fuel
  before decompression. Sorting stops when its budget is exhausted. Host
  policies retain their own existing fuel and dependency accounting.
  An input-gathering budget failure conservatively charges the plan's full work
  ceiling: a partially decoded scan cannot be retried for free by catching it.
- Maximum declarations are 10,000 candidates, 64MiB and 10,000,000 work units.
  `SQL_BUDGET` fails the statement without returning partial rows. Syntax/binding
  failures use `SQL_INVALID`; parameter contract failures use `INVALID_ARGUMENT`.

Full scans record a whole-collection dependency. Point lookups record the exact
key, including absence. Equality reads record the whole index bucket plus every
record probed: entering/leaving the bucket invalidates the result, and so does a
change to a non-indexed projected/predicate field. Other buckets do not invalidate
it unless a policy lookup observes them. Dependencies are registered before reads,
so cached budget failures repair when relevant data changes. A NULL required
selector can produce an empty result without observing collection data. No path
narrows its dependency by output limit. Policy lookups retain their own
dependencies. Reads see staged inserts, replacements, index-bucket moves and
deletions at the same pinned snapshot as the rest of the invocation.

Implementation locations: `flower-sdk/src/sql.rs` declares handles and manifests;
`flower-sql` parses, binds, explains and evaluates; `flower-engine/src/host/sql.rs`
executes reads; the engine manifest loader rebinds at deployment.

## Compatibility and deployment

Host operations 35/36, named plans, scoped SDK endpoints and decimal schemas
were introduced in **state_machine 10**; the current build uses **state_machine 12**
and **raft_wire 7**, including durable partition plans and transaction footprints
with certified derived sources and precise receipt, timer and queue conflicts.
Peer handshakes reject older members; deployments must not run in mixed
groups. Storage-contract checks reject files claimed by earlier state machines.
Use the existing backup/restore procedure into a fresh directory for a contract
change; do not rewrite a file's marker. No index/record codec changed:
`value_format` remains 4 and the snapshot format is unchanged. No automatic
in-place contract upgrade or index migration is introduced here.

## Next increments and acceptance conditions

1. **Value-ordered indexes and native typed bounds.** Add opt-in index order;
   preserve every existing legacy definition and ID. Define escaped variable
   components, exact decimal codecs, nullable coverage, tuple/key behavior,
   dependency comparisons and staged maintenance together. Require a new index
   name and bounded backfill. Specify member compatibility, snapshot transfer,
   backup/replay and cancellation before changing the stored value format.
   Extend the implemented complete-equality paths to proven value-order ranges.
2. **Incremental execution and cursors.** Distinguish the total SQL limit from
   page size. A later-snapshot keyset cursor permits omissions/duplicates when
   rows move; snapshot-stable paging needs a retained snapshot/history contract.
   Bind continuation to plan/version, parameters, authority, schema/index
   generation, database identity and history. Carry no readable secret sort
   values; choose authenticated opaque or server-held state with explicit expiry.
3. **Indexed joins.** Prove the inner access path and cap candidates, distinct
   lookups, probes, bytes and intermediates. Nonunique equality buckets are not
   bounded. Coalesce dependencies conservatively or fall back to a broader
   dependency before reaching the certificate limit; one range per lookup alone
   does not prevent dependency exhaustion.
4. **Aggregates and functions.** Define invoker-visible input semantics, null and
   missing behavior, overflow, grouping and bounded intermediate state first.
   Treat materialized rewrites as equivalence proofs, never naming heuristics.
5. **Inline and administrative surfaces.** Request-time SDK endpoints are
   implemented. Inline syntax needs an explicit catalog
   and declared row contract/registration. Administrative SQL needs an explicit
   principal/definer choice, read capability, budgets and `EXPLAIN`; keep it out of
   the public query endpoint unless a separate capability authorizes it.
