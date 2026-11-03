//! Intent preview: read-your-own-intents over a staged overlay.
//!
//! Preview evaluates the longest successful prefix of a database's live
//! intents against the synced snapshot, staging each intent's writes
//! into an overlay. Reads in preview mode see the snapshot plus the
//! overlay, so a device reads its own queued writes before the cloud
//! confirms them.
//!
//! Preview halts at the first failed intent: later intents may depend
//! on earlier effects, so only the successful prefix is meaningful. The
//! overlay is bounded; an intent that would overflow it ends the prefix
//! without applying. Acknowledged intents stay previewed until their
//! receipt arrives in the sync stream, so committed effects never
//! flicker out between submission and sync.
//!
//! Preview is evaluation-only: no receipts, no sync advance, no external
//! effects, device-local budgets. Sync-down discards the overlay and
//! re-previews from the advanced snapshot.

use crate::descriptors::DefKind;
use crate::error::HostResult;
use crate::host::{Env, Host};
use crate::intent::Intent;
use crate::runtime::CallKind;
use crate::session::{CallerKind, Invocation, Staged};
use crate::storage::ReadTxn;

/// Preview budgets and principal.
#[derive(Clone, Debug)]
pub struct PreviewOptions {
    /// Calling principal, or `None` for anonymous preview.
    pub principal: Option<flower_sdk::Principal>,
    /// Fuel budget for the whole prefix.
    pub fuel: u64,
    /// Fixed timestamp for the whole prefix.
    pub timestamp_ms: u64,
    /// Most staged journal operations the overlay may hold.
    pub max_journal_ops: usize,
}

/// Default preview budgets: the standard fuel allowance with a
/// ten-thousand-operation overlay cap.
pub fn default_preview_options() -> PreviewOptions {
    PreviewOptions {
        principal: None,
        fuel: crate::engine::DEFAULT_FUEL,
        timestamp_ms: crate::engine::now_ms(),
        max_journal_ops: 10_000,
    }
}

/// A previewed prefix: the applied intent identities plus their
/// overlay for preview-mode reads.
#[derive(Clone, Debug)]
pub struct Preview {
    /// Applied intent identities in queue order.
    pub previewed: Vec<Vec<u8>>,
    /// The staged overlay of the prefix.
    pub overlay: Staged,
}

/// Evaluate the longest successful prefix of live intents for one
/// database, returning the applied identities plus the overlay. Only
/// queued, submitted and acknowledged intents preview; committed and
/// failed ones leave the queue through explicit drops.
pub fn preview_intents(
    read: &ReadTxn,
    env: &Env,
    db: &[u8],
    intents: &[Intent],
    options: &PreviewOptions,
) -> HostResult<Preview> {
    let mut session = Invocation::new(
        String::new(),
        db.to_vec(),
        CallerKind::Mutation,
        options.principal.clone(),
        None,
        options.timestamp_ms,
        options.fuel,
    );
    session.set_ephemeral(true);
    let mut previewed = Vec::new();
    {
        let mut host = Host::new(read, &mut session, env);
        for intent in intents {
            let Some((module, name)) = split_alias(&intent.alias) else {
                break;
            };
            let kind = match host
                .descriptors
                .get(module)
                .and_then(|defs| defs.def_kinds.get(name))
                .copied()
            {
                Some(DefKind::Query) => CallKind::Query,
                Some(DefKind::Mutation) => CallKind::Mutation,
                _ => break,
            };
            host.session.push_savepoint();
            let outcome = host.invoke_nested(kind, module, name, &intent.args);
            match outcome {
                Ok(Ok(_)) => {
                    // The bound ends the prefix at the overflowing
                    // intent: its writes stay out of the overlay.
                    if host.session.staged.journal().len() > options.max_journal_ops {
                        host.session.pop_savepoint(true);
                        break;
                    }
                    host.session.pop_savepoint(false);
                    previewed.push(intent.identity.clone());
                }
                _ => {
                    host.session.pop_savepoint(true);
                    break;
                }
            }
            if host.session.is_poisoned() {
                break;
            }
        }
    }
    let overlay = std::mem::take(&mut session.staged);
    Ok(Preview { previewed, overlay })
}

/// Split a `module/name` alias.
fn split_alias(alias: &str) -> Option<(&str, &str)> {
    let (module, name) = alias.split_once('/')?;
    if module.is_empty() || name.is_empty() {
        return None;
    }
    Some((module, name))
}
