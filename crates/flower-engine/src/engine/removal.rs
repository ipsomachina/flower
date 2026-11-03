//! Removing a module and purging its data (see [`crate::purge`]).
//!
//! A removal is one replicated entry every applier commits through
//! [`Engine::commit_removal`], in the transaction that records the
//! entry's position, and decides alike from replicated state: what is
//! deployed, the importers of what it removes, the purges in flight.
//! Purge pages are batch items the leader's writer prepares
//! ([`Engine::prepare_purge`]) and every applier commits
//! ([`Engine::commit_purge`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use flower_sdk::error::codes;
use flower_sdk::{Failure, Value, fail};

use super::{Engine, EngineReader, abandon_staging, put_staging};
use crate::batch::BatchItem;
use crate::descriptors::{ModuleDef, load_module};
use crate::error::{Error, HostResult};
use crate::json::parse_value;
use crate::placement;
use crate::purge::{self, Progress, PurgePage, Purges, Removed};
use crate::retry::HASH_LEN;
use crate::staging::{StagingPhase, code_era_key, code_identity_key, decode_state, staging_key};
use crate::storage::{ReadTxn, StagedWrite, meta};

/// Why removing `module` from `db` (with `purge`) is refused in `read`'s
/// state, given the database's active modules, if it is: a database that
/// does not serve here or that a prepared transaction holds; a module not
/// deployed, unless its data is to be purged (what a removal kept, or a
/// purge in flight, which goes on as it was); a module another one
/// imports from.
pub(crate) fn removal_refusal(
    read: &ReadTxn,
    active: &HashMap<String, ModuleDef>,
    db: &[u8],
    module: &str,
    purge: bool,
) -> HostResult<Option<Failure>> {
    if module.is_empty() || module.contains('\0') || module.starts_with('$') {
        return Ok(Some(fail(
            codes::MODULE_CONFLICT,
            "invalid module identity",
        )));
    }
    if let Some(refusal) = crate::txn::unwritable(read, db).map_err(Failure::from)? {
        return Ok(Some(refusal));
    }
    let deployed = read
        .meta(db, &code_identity_key(module))
        .map_err(Failure::from)?
        .is_some();
    if !deployed {
        if purge {
            return Ok(None);
        }
        return Ok(Some(fail(
            codes::MODULE_CONFLICT,
            format!("no module `{module}` is deployed"),
        )));
    }
    Ok(crate::contracts::check_removal(active, module).err())
}

impl Engine {
    /// Remove `module` from `db`, or purge what a removal kept, inside
    /// `staged` and commit it: the caller has staged its own bookkeeping
    /// for the entry (group head, expiry clock, position), which commits
    /// with it whether the removal applies or is refused.
    ///
    /// A deployed module loses its code identity, aliases, hooks and
    /// runtime (readers fenced out while it goes), its derived outcomes,
    /// and what other modules computed from it; a staging of it this
    /// member left open is abandoned. Without `purge`, its data and its
    /// declarations stay (`removed\0<module>` names the code it ran), so
    /// deploying it again diffs against them. With `purge`, its data
    /// and the queues only it declared purge a page at a time from here,
    /// and neither it nor a module declaring one of those queues can
    /// deploy until the purge ends. Its timers that come due meanwhile
    /// are held for it (see [`crate::timer`]). The inner `Err` is the
    /// deterministic refusal (see [`removal_refusal`]); the answer is
    /// `{removed, purging}`.
    pub fn commit_removal(
        &mut self,
        staged: StagedWrite,
        db: &[u8],
        module: &str,
        purge: bool,
    ) -> HostResult<Result<Value, Failure>> {
        let read = staged.read();
        if let Some(refusal) =
            removal_refusal(&read, &self.modules(db).descriptors, db, module, purge)?
        {
            drop(read);
            staged.commit().map_err(Failure::from)?;
            return Ok(Err(refusal));
        }
        // The queues a purge starting here deletes, from the code the
        // module ran, before its removal changes what it names.
        let purging = purge::purges(&read, db).map_err(Failure::from)?;
        let queues = match purge && !purging.contains_key(module) {
            true => self.purged_queues(&read, db, module)?,
            false => Vec::new(),
        };
        let write = staged.write();
        let deployed = read
            .meta(db, &code_identity_key(module))
            .map_err(Failure::from)?;
        // A staging of it this member left open never activates now; the
        // names only it interned are released before the purge reads
        // which names the module owns.
        if let Some(bytes) = read.meta(db, &staging_key(module)).map_err(Failure::from)? {
            let state = decode_state(&bytes).map_err(Failure::from)?;
            if state.phase != StagingPhase::Gc {
                let rest = abandon_staging(write, db, state).map_err(Failure::from)?;
                put_staging(write, db, module, rest.as_ref()).map_err(Failure::from)?;
            }
        }
        if let Some(identity) = &deployed {
            let owned = purge::module_ids(&read, db, module).map_err(Failure::from)?;
            crate::derived::drop_module(write, &read, db, &owned).map_err(Failure::from)?;
            write
                .delete_meta(db, &code_identity_key(module))
                .map_err(Failure::from)?;
            write
                .delete_meta(db, &code_era_key(module))
                .map_err(Failure::from)?;
            if !purge {
                write
                    .set_meta(db, &purge::removed_key(module), identity)
                    .map_err(Failure::from)?;
            }
        }
        if purge {
            write
                .delete_meta(db, &purge::removed_key(module))
                .map_err(Failure::from)?;
            let mut purges = purging;
            purges
                .entry(module.to_string())
                .or_insert_with(|| Progress::start(queues));
            purge::write_purges(write, db, &purges).map_err(Failure::from)?;
        }
        drop(read);
        if deployed.is_some() {
            // The removed code and the rows without it reach readers
            // together.
            self.exclusive(|engine| {
                staged.commit().map_err(Failure::from)?;
                engine.uninstall_module(db, module);
                Ok::<_, Failure>(())
            })?;
        } else {
            staged.commit().map_err(Failure::from)?;
        }
        Ok(Ok(Value::Object(BTreeMap::from([
            ("removed".to_string(), Value::String(module.to_string())),
            ("purging".to_string(), Value::Bool(purge)),
        ]))))
    }

    /// The queues a purge of `module` deletes, as of `read`: those the
    /// code it runs, or the code its removal kept, declared, but for
    /// those another module declares, deployed or removed with its data
    /// kept (queues are named by queue, not by module).
    fn purged_queues(&self, read: &ReadTxn, db: &[u8], module: &str) -> HostResult<Vec<String>> {
        let Some(def) = self.previous_def(read, db, module)? else {
            return Ok(Vec::new());
        };
        let mut taken = HashSet::new();
        for (other, def) in &self.modules(db).descriptors {
            if other != module {
                taken.extend(def.queues.iter().map(|decl| decl.name.clone()));
            }
        }
        for (other, identity) in purge::removed(read, db).map_err(Failure::from)? {
            if other == module {
                continue;
            }
            let (_, manifest) = read
                .get_code(db, &identity)
                .map_err(Failure::from)?
                .ok_or_else(|| Error::corrupt("a removed module's code is missing"))?;
            let manifest = parse_value(&manifest).map_err(Failure::from)?;
            taken.extend(purge::declared_queues(&manifest).map(str::to_string));
        }
        Ok(def
            .queues
            .into_iter()
            .map(|decl| decl.name)
            .filter(|queue| !taken.contains(queue))
            .collect())
    }

    /// Why removing `module` from `db` (with `purge`) is refused in
    /// `read`'s state, if it is: what [`Engine::commit_removal`] decides,
    /// for appliers that record more beside an applied removal.
    pub fn removal_refusal(
        &self,
        read: &ReadTxn,
        db: &[u8],
        module: &str,
        purge: bool,
    ) -> HostResult<Option<Failure>> {
        removal_refusal(read, &self.modules(db).descriptors, db, module, purge)
    }

    /// Remove `module` from a database this engine writes locally (not
    /// through consensus): [`Engine::commit_removal`] in a transaction of
    /// its own. Replicated databases remove through their group.
    pub fn remove_module(
        &mut self,
        db: &[u8],
        module: &str,
        purge: bool,
    ) -> HostResult<Result<Value, Failure>> {
        self.refuse_replicated(db)?;
        let staged = self.shared.store.begin_staged().map_err(Failure::from)?;
        self.commit_removal(staged, db, module, purge)
    }

    /// Delete one page of the first purge in flight in a database this
    /// engine writes locally, as a group's leader does through
    /// consensus: the page's answer (`{module, deleted, keys, done}`), or
    /// `None` when no purge is in flight.
    pub fn purge_local(
        &mut self,
        db: &[u8],
        limit: usize,
    ) -> HostResult<Option<Result<Value, Failure>>> {
        self.refuse_replicated(db)?;
        let Some(BatchItem::Purge { page, .. }) = self.prepare_purge(db, limit)? else {
            return Ok(None);
        };
        let staged = self.shared.store.begin_staged().map_err(Failure::from)?;
        let result =
            purge::apply_page(staged.write(), &staged.read(), db, &page).map_err(Failure::from)?;
        staged.commit().map_err(Failure::from)?;
        Ok(Some(result))
    }

    /// Refuse local writes to a replicated database.
    fn refuse_replicated(&self, db: &[u8]) -> HostResult<()> {
        if self.replicated.contains(db) {
            return Err(fail(
                "replicated_db",
                "changes to a replicated database go through consensus",
            ));
        }
        Ok(())
    }

    /// The next page of the first purge in flight in `db`, by module
    /// name, as a batch item, on the state prepares read (inside a
    /// writer's staging, what the writes before it left): up to `limit`
    /// keys. `None` when no purge is in flight, or the database does not
    /// serve here.
    pub fn prepare_purge(&self, db: &[u8], limit: usize) -> HostResult<Option<BatchItem>> {
        let read = self.snapshot().map_err(Failure::from)?;
        if placement::read_refusal(&read, db)
            .map_err(Failure::from)?
            .is_some()
        {
            return Ok(None);
        }
        let Some((module, progress)) = purge::purges(&read, db)
            .map_err(Failure::from)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let page = purge::next_page(&read, db, &module, &progress, limit, purge::SCAN.max(limit))
            .map_err(Failure::from)?;
        Ok(Some(BatchItem::Purge {
            db: db.to_vec(),
            page,
        }))
    }

    /// Commit one purge page into `staged` at `position` (see
    /// [`crate::purge`]): its answer or its refusal, plus the database's
    /// revision, which a purge never moves. The position must advance
    /// the database's unless `advance` is false.
    pub fn commit_purge(
        &self,
        staged: &StagedWrite,
        db: &[u8],
        page: &PurgePage,
        position: u64,
        advance: bool,
    ) -> HostResult<(HostResult<Value>, u64)> {
        let write = staged.write();
        let revision = write.meta_u64(db, meta::REVISION).map_err(Failure::from)?;
        if let Some(failure) = placement::refusal(write, db).map_err(Failure::from)? {
            return Ok((Err(failure), revision));
        }
        let applied = write
            .meta_u64(db, meta::APPLIED_LOG)
            .map_err(Failure::from)?;
        if position < applied || (advance && position == applied) {
            return Err(Error::corrupt(format!(
                "purge position {position} does not advance {applied}"
            ))
            .into());
        }
        let result = purge::apply_page(write, &staged.read(), db, page).map_err(Failure::from)?;
        write
            .set_meta_u64(db, meta::APPLIED_LOG, position)
            .map_err(Failure::from)?;
        Ok((result, revision))
    }

    /// Unregister a module's descriptors and runtime in one database,
    /// publishing the new catalog. The database keeps its (possibly
    /// empty) entry, so managed-key changes still reach its outcomes.
    fn uninstall_module(&mut self, db: &[u8], module: &str) {
        let catalog = Arc::make_mut(&mut self.catalog);
        let modules = Arc::make_mut(catalog.modules.entry(db.to_vec()).or_default());
        modules.descriptors.remove(module);
        modules.runtimes.remove(module);
        self.publish();
    }

    /// What a deployment of `module` diffs against: its active
    /// descriptors, or, once removed without a purge, the declarations
    /// its removal kept.
    pub(super) fn previous_def(
        &self,
        read: &ReadTxn,
        db: &[u8],
        module: &str,
    ) -> HostResult<Option<ModuleDef>> {
        if let Some(active) = self.active_def(db, module) {
            return Ok(Some(active.clone()));
        }
        let Some(identity) = read
            .meta(db, &purge::removed_key(module))
            .map_err(Failure::from)?
        else {
            return Ok(None);
        };
        let identity: [u8; HASH_LEN] = identity
            .try_into()
            .map_err(|_| Error::corrupt("a removed module's identity has the wrong length"))?;
        let (_, manifest) = read
            .get_code(db, &identity)
            .map_err(Failure::from)?
            .ok_or_else(|| Error::corrupt("a removed module's code is missing"))?;
        let manifest = parse_value(&manifest).map_err(Failure::from)?;
        Ok(Some(load_module(&manifest).map_err(Failure::from)?))
    }
}

impl EngineReader {
    /// Why removing `module` from `db` (with `purge`) would be refused
    /// now, if it would: what its replicated removal checks again.
    pub fn removal_refusal(
        &self,
        db: &[u8],
        module: &str,
        purge: bool,
    ) -> HostResult<Option<Failure>> {
        let (catalog, read, _) = self.capture()?;
        removal_refusal(&read, &catalog.modules(db).descriptors, db, module, purge)
    }

    /// `db`'s purges in flight and the modules removed from it whose
    /// data stays (with the code identity each last ran), from one
    /// snapshot.
    pub fn module_removals(&self, db: &[u8]) -> HostResult<(Purges, Removed)> {
        let (_, read, _) = self.capture()?;
        Ok((
            purge::purges(&read, db).map_err(Failure::from)?,
            purge::removed(&read, db).map_err(Failure::from)?,
        ))
    }
}
