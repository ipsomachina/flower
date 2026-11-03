# Flower's redb 4.4.0

Published as `flower-redb`, with the Rust dependency alias `redb`. Explicit
versioned dependencies keep this fork in crates.io installations too;
they do not depend on a downstream workspace copying a `[patch]` section.
Local workspaces patch that same package name to this directory. The fork
retains the upstream MIT OR Apache-2.0 licensing.

This is the published `redb` 4.4.0 crate (its `src`, `build.rs`, licenses and
manifest, without tests, examples, benchmarks, dev-dependencies or profiles)
with four public methods added to `WriteTransaction`, in
`src/transactions.rs`, and one change to compaction. It retains 4.4.0's fixes
for page-cache growth under writes, `Durability::None` commits lost on close,
and `compact()` leaving free regions at the end of the file.

- `compact_some(max_pages)` moves at most `max_pages` of the pages nearest the
  end of the file, and the parents that point to them, to the lowest free pages,
  and sets the transaction to trim every free page at the end of the file when
  it commits. It is the body of `Database::compact`'s loop
  (`compact_pages`, now `compact_pages_limited` with a page limit), run in an
  ordinary write transaction.
- `allocated_bytes()` counts the file's allocated pages from the region
  allocators, without walking any tree (`stats()` walks every table).
- `trim_on_commit()` makes a durable commit give back every free page at the end
  of the file (`ShrinkPolicy::Maximum`, as `compact_some` does).
- `occupied_bytes()` is the file's length less the free pages at its end: what
  a commit can't give back without compaction.
- Compaction never grows the file to find a lower page
  (`try_allocate_lowest` in `src/tree_store/page_store/page_manager.rs`): a
  page with nowhere lower to go ends the step, and a path whose parent has
  nowhere to go is undone. Upstream allocated, grew the file if no page was
  free, then gave the page back; with readers holding every free page, that
  doubled the file. `Database::compact` benefits too.

Why: redb is copy-on-write and reuses a freed page only once no reader holds
it, so a busy file grows, by doubling, and keeps live pages near its end; it
gives space back only from the end. `Database::compact` moves those pages down,
but needs `&mut Database` and no open read transaction, and Flower always holds
some (it serves reads from redb snapshots). Relocation itself doesn't need
that: `relocate_helper` frees each moved page into the transaction's freed
list, which readers protect like any page a write replaces. `Database::compact`
excludes readers to give every freed page back at once; Flower's compactor
(`crates/flower-engine/src/storage/compact.rs`) instead runs bounded steps
between other writes and lets later commits give the tail back. Savepoints
would need the moved pages tracked; Flower uses none.

To move to a newer redb, apply the same diff (the four methods,
`compact_pages_limited`, `try_allocate_lowest`/`allocate_helper_growing` and
`occupied_bytes` in the page manager) to the new release's `src`, then run
the compaction tests in `flower-engine` and `flower-cluster`.
