# Flower protocol contracts

This crate contains the shared versioned DTOs, causal tokens, value conversion
and validators used by the client and server. `Capability` is the typed
vocabulary; unknown advertised names remain forward-compatible extensions.

[schema/v1.schema.json](schema/v1.schema.json) is a JSON Schema 2020-12 artifact.
Its root validates a call envelope. Select a named `$defs` schema for a read
token, query/mutation/watch/transaction request, mutation envelope, problem,
discovery response, watch frame, sync batch,
or local `PreparedCommandV2` snapshot. Root discovery remains inspectable when
its advertised protocol version is newer. Accepting that version is a separate
compatibility decision.

Full-width token counters use canonical decimal strings. Their schema pattern
checks the complete `u64` range, including the upper bound, without depending on
the JSON consumer's number precision. Legacy numeric ordering fields still
require an integer-safe JSON implementation. Runtime validators also enforce
relationships JSON Schema does not express: sync coverage, history continuity,
causal bounds and the current operation's contract fingerprints.

[fixtures/v1.json](fixtures/v1.json) contains named valid and invalid golden
cases, with the applicable `kind` and any coverage context. The Rust tests run
these cases through the shared DTO validators. They include explicit null versus
missing request fields, typed request parameters, mutation ordering metadata, maximum-width counters, unknown
versions, malformed histories and empty sync batches.

The same files are served from the documentation site's `assets/protocol/`:

```text
cargo run -p flower-protocol --example export
cargo run -p flower-protocol --example export -- --check
node web/docs/tools/check-protocol.mjs
```

The exporter writes local static assets; it does not deploy the website. CI can
use `--check` to prevent source and published artifacts from drifting.

Custom native transports can enable `flower-client/conformance` and run the
reusable loopback `ProbeServer` suite. It verifies headers and credentials at the
receiver, response metadata and retry advice, streaming before EOF, and release
of the server response after the body is dropped. It supports HTTP/1.1 and h2c.
