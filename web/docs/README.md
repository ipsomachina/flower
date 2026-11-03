# Flower documentation

Static pages served at `/docs/` next to the landing page. Open
`index.html` in a browser, or serve `web/` with any static server; nothing
needs building to read them.

The pages and examples are MIT-licensed. `tools/sync.mjs` keeps `web/LICENSE`
identical to the repository's root license and adds the license link to each
documentation footer. The bundled font retains its OFL notice in `web/fonts/`.

## Editing

- Each page's text lives between `<!-- content -->` and `<!-- /content -->`.
  Everything around it (head, header, sidebar, table of contents,
  previous/next links, footer) is generated.
- The navigation (page list, titles, descriptions) is `NAV` in
  `tools/sync.mjs`.
- After editing, run `bun web/docs/tools/sync.mjs`: it rewrites the chrome,
  gives headings ids, rebuilds the search index and checks every internal
  link and anchor. `--check` reports without writing and exits 1 when
  anything is stale or broken.

## Code examples

Code blocks written `<code class="language-rust" data-include="examples/…#anchor">`
are filled by `sync.mjs` from the lines between `// ANCHOR: anchor` and
`// ANCHOR_END: anchor` of that file. The files live in `examples/`, a
workspace of its own (not a member of the Flower workspace):

```bash
cargo clippy --manifest-path web/docs/examples/Cargo.toml -p tasks -p doc-snippets --target wasm32-unknown-unknown -- -D warnings
cargo clippy --manifest-path web/docs/examples/Cargo.toml --all-targets -- -D warnings
cargo test   --manifest-path web/docs/examples/Cargo.toml
```

- `quickstart`: the quickstart's guest crate, as printed, with the testing
  guide's in-process test (`tests/tasks.rs`).
- `snippets`: every other guest example. `tests/deploy.rs` deploys each
  page's `App` into the engine natively, so the manifest checks (names,
  HTTP exposure, access, exports, queues, keys, covering indexes) run too,
  not only the compiler.
  The SQL guide's `#[application]` catalogue is deployed here too; the test
  inserts an order and executes its dynamic SQL query, preserving an exact
  decimal integer above the JavaScript safe-integer range.
- `client`: the native client and worker examples. It depends on the two
  guest crates (both `cdylib` and `rlib`) for their typed descriptors.

When the SDK or client changes, these builds are what keep the pages
honest: fix the example, re-run `sync.mjs`, and the page follows. Shown
outputs (HTTP exchanges, CLI output) are pasted from a release
`cargo flower serve`; re-run them when behavior changes.
