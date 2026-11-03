# Flower documentation

Static pages served at `/docs/` next to the landing page. Build and preview
them with the [website's Build Awesome commands](../README.md). Deploy
`web/_site/`; the files in this directory are source templates.

The pages and examples are MIT-licensed. The build copies the repository's
root license to `web/_site/LICENSE`, linked from every documentation footer.
The bundled font retains its OFL notice in `web/fonts/`.

## Editing

- Each `.html` file contains only that page's content. Everything around it
  (head, header, sidebar, table of contents, previous/next links, footer)
  comes from `../_includes/docs.11ty.js`.
- The navigation (page list, titles, descriptions) is `NAV` in
  `../_data/navigation.js`. Add an entry there when adding a page.
- Run `bun run --cwd web dev` while editing or `bun run --cwd web check`
  before committing. Build Awesome applies the layout, gives headings IDs,
  rebuilds the search index and checks internal links, anchors and assets.
  Generated files are written only to `web/_site/`.

## Code examples

Empty code blocks written `<code class="language-rust" data-include="examples/…#anchor"></code>`
are filled at build time from the lines between `// ANCHOR: anchor` and
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
honest: fix the example, rebuild the site, and the page follows. Shown
outputs (HTTP exchanges, CLI output) are pasted from a release
`cargo flower serve`; re-run them when behavior changes.
