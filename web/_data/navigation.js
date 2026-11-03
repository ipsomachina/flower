const NAV = [
  {
    section: "Get started",
    pages: [
      ["index.html", "Introduction", "What Flower is, how an application fits together, and where to go next."],
      ["quickstart.html", "Quickstart", "Install the tools, write a guest module, serve it and call it — in about ten minutes."],
    ],
  },
  {
    section: "Concepts",
    pages: [
      ["concepts/databases.html", "Databases, modules and groups", "Logical databases, module instances, named partitions and the Raft groups that host them."],
      ["concepts/collections.html", "Collections and indexes", "Typed values and keys, collections, indexes, references and range scans."],
      ["concepts/methods.html", "Queries and mutations", "Declaring methods and their options, the App, HTTP exposure, errors, deadlines and calls between modules."],
      ["concepts/derived.html", "Derived values and aggregates", "Reactive computations, materialization, delta-maintained aggregates, triggers and maintenance tasks."],
      ["concepts/consistency.html", "Revisions and consistency", "What a revision is, linearizable and replica-local reads, leader leases and read-your-writes."],
      ["concepts/retries.html", "Retries and receipts", "Retry identities, receipts, duplicate results and how long Flower remembers them."],
      ["concepts/watches.html", "Watches", "Live query results over server-sent events, and when they change."],
      ["concepts/time.html", "Time", "The invocation clock, now versus clock, and changes_at with earliest-wins."],
      ["concepts/queues-timers.html", "Queues and timers", "Durable jobs with fenced leases, and not-before timers that fire mutations."],
      ["concepts/modules.html", "Modules and interfaces", "Exports, imports, interface crates, slots and bindings."],
      ["concepts/transactions.html", "Transactions and partitions", "Atomic work across logical databases, named partitions and groups."],
    ],
  },
  {
    section: "Guides",
    pages: [
      ["guides/rust-client.html", "Calling Flower from Rust", "flower-client: calls, typed calls, scoping, credentials, watches and transactions."],
      ["guides/sql.html", "SQL queries", "Typed plans, scoped SQL endpoints, resource limits, explain and diagnostics."],
      ["guides/errors-retries.html", "Errors and retries in clients", "What can fail, what is safe to retry, deadlines, cancellation and overload."],
      ["guides/auth.html", "Authentication and authorization", "The authorization hook, typed authenticators such as JwtBearer, and access policies with redaction."],
      ["guides/crypto.html", "Guest cryptography", "Hashing, JWT and JWE, WebAuthn ceremonies and managed keys inside guest code."],
      ["guides/workers.html", "Worker queues", "Native workers with flower-worker: lines, chained claims, idle jobs and drain deadlines."],
      ["guides/deployments.html", "Deployments", "Building, planning, staging and activating modules, bindings and removal."],
      ["guides/backups.html", "Backups and restore", "Continuous encrypted backup to object storage and point-in-time restore."],
      ["guides/operations.html", "Operating a cluster", "Serving nodes, membership, health, metrics, overload and the admin dashboard."],
      ["guides/testing.html", "Testing applications", "Running guest code natively against the real engine, and mocking the client transport."],
    ],
  },
  {
    section: "Reference",
    pages: [
      ["reference/http.html", "HTTP API", "Every endpoint, header, status code and the error body."],
      ["reference/cli.html", "cargo flower", "Every subcommand and flag, including serve."],
      ["reference/sdk.html", "flower-sdk", "The guest SDK surface: types, macros, contexts and components."],
      ["reference/errors.html", "Error codes and limits", "Failure codes, their HTTP statuses, retryability and runtime limits."],
    ],
  },
];

export default NAV;
