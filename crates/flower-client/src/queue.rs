//! Offline intent queue: enqueue mutations locally, submit on
//! reconnect. Intents live in the device engine's intent log with
//! stable identities, so redelivery deduplicates server-side through
//! the request identity. Intents submit strictly in queue order: the
//! first one whose outcome stays unknown (transport failure, timeout,
//! forward) ends the run so later intents never commit ahead of it.
//! Accepted intents mark acknowledged and keep previewing until the
//! sync client observes their receipts arriving in the sync-down
//! stream, which marks them committed; business failures mark failed.

use std::sync::Arc;

use flower_engine::engine::Engine;
use flower_engine::intent::{Intent, IntentStatus};
use flower_engine::json::render;
use flower_engine::retry::unscoped_identity_key;
use flower_sdk::{Value, fail};
use sha2::{Digest, Sha256};

use crate::client::{FlowerClient, MutationOptions, RequestOptions};
use crate::error::{FlowerError, engine_err};

/// Intent log bound per device.
const INTENT_BOUND: usize = 4096;

/// One submission run's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmitSummary {
    /// Intents the server accepted. They stay acknowledged, and
    /// previewed, until sync-down delivers their receipts.
    pub submitted: usize,
    /// Intents decided as business failures.
    pub failed_business: usize,
    /// Intents still pending after the run.
    pub still_pending: usize,
}

/// Hex-encode bytes for stable request identities.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The request identity an intent submits under.
fn request_id(identity: &[u8]) -> String {
    if let Ok(text) = std::str::from_utf8(identity)
        && (text.starts_with("f1:") || text.starts_with("f2:"))
    {
        return text.to_owned();
    }
    hex(identity)
}

/// The receipt identity the cloud stores for an intent's
/// submission: the unscoped idempotency namespace over its request
/// identity. Sync-down delivers the receipt under this identity.
pub(crate) fn receipt_identity(identity: &[u8]) -> Result<Vec<u8>, FlowerError> {
    if let Ok(text) = std::str::from_utf8(identity)
        && let Some(Ok(scoped)) = flower_engine::retention::parse_request_id(text)
    {
        return scoped.encode().map_err(engine_err);
    }
    Ok(unscoped_identity_key(request_id(identity).as_bytes()))
}

/// An offline intent queue over a client and a device engine.
pub struct IntentQueue {
    client: FlowerClient,
    engine: Arc<tokio::sync::Mutex<Engine>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct IntentHandle {
    pub sequence: u64,
    pub request_id: String,
}
impl IntentHandle {
    pub fn identity(&self) -> &[u8] {
        self.request_id.as_bytes()
    }
}

impl IntentQueue {
    /// Enqueue exactly the persisted command; repeated enqueueing returns its original handle.
    pub async fn enqueue_command<T: flower_sdk::FlowerType, E: flower_sdk::BusinessError>(
        &self,
        command: &crate::PreparedCommand<T, E>,
    ) -> Result<IntentHandle, FlowerError> {
        command.validate()?;
        let args = crate::typed::json_value(&command.args)?;
        let fingerprint = flower_engine::retention::request_fingerprint(
            &command.module,
            &command.method,
            &args,
            command.expected_revision,
        )
        .map_err(engine_err)?;
        let alias = format!("{}/{}", command.module, command.method);
        let sequence = self
            .engine
            .lock()
            .await
            .enqueue_intent(
                command.database.as_bytes(),
                &alias,
                &args,
                command.request_id.as_bytes(),
                &fingerprint,
                command.expected_revision,
                INTENT_BOUND,
            )
            .map_err(engine_err)?;
        Ok(IntentHandle {
            sequence,
            request_id: command.request_id.clone(),
        })
    }

    pub async fn intent(&self, handle: &IntentHandle) -> Result<Option<Intent>, FlowerError> {
        self.engine
            .lock()
            .await
            .get_intent(handle.identity())
            .map_err(engine_err)
    }
    /// Persist an observed identity before going offline. Existing commands keep their identity.
    pub async fn bind_identity(
        &self,
        db: &[u8],
        identity: &crate::RetryIdentity,
    ) -> Result<(), FlowerError> {
        if !identity.initialized || identity.incarnation == 0 || identity.epoch < identity.min_epoch
        {
            return Err(FlowerError::Unsupported(
                "offline intents need initialized cloud retry retention".into(),
            ));
        }
        let engine = self.engine.lock().await;
        let write = engine.store().begin_write().map_err(engine_err)?;
        let mut key = b"retry_identity\0".to_vec();
        key.extend_from_slice(db);
        let bytes = serde_json::to_vec(identity).map_err(|e| FlowerError::Decode(e.to_string()))?;
        write
            .set_meta(flower_engine::intent::INTENT_DB, &key, &bytes)
            .map_err(engine_err)?;
        write.commit().map_err(engine_err)
    }

    pub async fn initialize(
        &self,
        db: &str,
        options: &RequestOptions,
    ) -> Result<crate::RetryIdentity, FlowerError> {
        let identity = self.client.retry_identity(db, options).await?;
        self.bind_identity(db.as_bytes(), &identity).await?;
        Ok(identity)
    }

    /// Queue through a client into a device engine.
    pub fn new(client: FlowerClient, engine: Arc<tokio::sync::Mutex<Engine>>) -> Self {
        IntentQueue { client, engine }
    }

    /// Enqueue an intent, returning its sequence. Duplicate
    /// identities return the existing sequence without writing.
    pub async fn enqueue(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        expected_revision: Option<u64>,
    ) -> Result<u64, FlowerError> {
        let rendered = render(args).map_err(engine_err)?;
        let mut hasher = Sha256::new();
        hasher.update(b"flower-intent-v1");
        hasher.update(db);
        hasher.update(module.as_bytes());
        hasher.update(name.as_bytes());
        hasher.update(&rendered);
        if let Some(revision) = expected_revision {
            hasher.update(revision.to_be_bytes());
        }
        let fingerprint: [u8; 32] = hasher.finalize().into();
        let alias = format!("{module}/{name}");
        let mut engine = self.engine.lock().await;
        let identity = {
            let read = engine.store().begin_read().map_err(engine_err)?;
            let history = flower_engine::placement::History::read(&read, db).map_err(engine_err)?;
            let retention =
                flower_engine::retention::Retention::read(&read, db).map_err(engine_err)?;
            let identity = if let Some(retention) = retention.filter(|_| history.incarnation != 0) {
                crate::RetryIdentity {
                    incarnation: history.incarnation,
                    epoch: retention.current_epoch,
                    min_epoch: retention.min_epoch,
                    epoch_ms: retention.rotation.map(|r| r.epoch_ms),
                    initialized: true,
                }
            } else {
                let mut key = b"retry_identity\0".to_vec();
                key.extend_from_slice(db);
                let bytes = read
                    .meta(flower_engine::intent::INTENT_DB, &key)
                    .map_err(engine_err)?
                    .ok_or_else(|| {
                        FlowerError::Unsupported(
                            "bind a cloud retry identity before enqueueing offline intents".into(),
                        )
                    })?;
                let identity: crate::RetryIdentity = serde_json::from_slice(&bytes)
                    .map_err(|e| FlowerError::Decode(e.to_string()))?;
                if history.incarnation != 0 && !history.holds(identity.incarnation) {
                    return Err(FlowerError::HistoryChanged {
                        local: identity.incarnation,
                        remote: history.incarnation,
                    });
                }
                identity
            };
            identity.request_id(uuid::Uuid::new_v4().as_bytes())
        };
        engine
            .enqueue_intent(
                db,
                &alias,
                args,
                identity.as_bytes(),
                &fingerprint,
                expected_revision,
                INTENT_BOUND,
            )
            .map_err(engine_err)
    }

    /// Submit pending intents in sequence order, up to `limit`.
    /// Accepted intents mark acknowledged: they keep previewing until
    /// [`crate::sync::SyncClient`] applies the batch carrying their
    /// receipts. Transport failures, timeouts, exhausted overload
    /// retries and forwards leave the outcome unknown: that intent
    /// stays pending and the run stops, so no later intent commits
    /// ahead of it. Auth, server and decode failures abort the run.
    pub async fn submit_pending(&self, limit: usize) -> Result<SubmitSummary, FlowerError> {
        let intents = {
            self.engine
                .lock()
                .await
                .list_intents(None)
                .map_err(engine_err)?
        };
        let pending_at_start = intents
            .iter()
            .filter(|intent| {
                matches!(
                    intent.status,
                    IntentStatus::Queued
                        | IntentStatus::Submitted
                        | IntentStatus::Unresolved { .. }
                )
            })
            .count();
        let mut submitted = 0usize;
        let mut failed_business = 0usize;
        for intent in intents
            .iter()
            .filter(|intent| {
                matches!(
                    intent.status,
                    IntentStatus::Queued
                        | IntentStatus::Submitted
                        | IntentStatus::Unresolved { .. }
                )
            })
            .take(limit)
        {
            if matches!(intent.status, IntentStatus::Unresolved { .. }) {
                break;
            }
            if !request_id(&intent.identity).starts_with("f1:")
                && !request_id(&intent.identity).starts_with("f2:")
            {
                self.mark(
                    &intent.identity,
                    IntentStatus::Unresolved {
                        failure: fail(
                            "unscoped_intent",
                            "legacy intent has no history fence; resolve it explicitly",
                        ),
                    },
                )
                .await?;
                break;
            }
            if intent.status == IntentStatus::Queued {
                self.mark(&intent.identity, IntentStatus::Submitted).await?;
            }
            let Some((module, name)) = intent.alias.split_once('/') else {
                self.mark(
                    &intent.identity,
                    IntentStatus::Failed {
                        failure: fail("corrupt", "intent alias is not module/name"),
                    },
                )
                .await?;
                failed_business += 1;
                continue;
            };
            let db = String::from_utf8(intent.db.clone())
                .map_err(|_| FlowerError::Decode("intent database is not UTF-8".to_string()))?;
            let rendered = render(&intent.args).map_err(engine_err)?;
            let args: serde_json::Value = serde_json::from_slice(&rendered)
                .map_err(|error| FlowerError::Decode(error.to_string()))?;
            let mut options = MutationOptions::new().request_id(request_id(&intent.identity));
            if let Some(revision) = intent.expected_revision {
                options = options.expected_revision(revision);
            }
            match self
                .client
                .mutate_value(&db, module, name, &args, &options)
                .await
            {
                Ok(_) => {
                    // Acknowledged, not committed: the effect stays
                    // previewed until sync-down delivers the receipt,
                    // so it never flickers out in between.
                    self.mark(&intent.identity, IntentStatus::Acknowledged)
                        .await?;
                    submitted += 1;
                }
                Err(FlowerError::Business {
                    code,
                    message,
                    details,
                    ..
                }) => {
                    let mut failure = fail(&code, &message);
                    failure.details = details.as_ref().map(crate::typed::json_value).transpose()?;
                    if flower_protocol::unresolved_outcome(&code) {
                        self.mark(&intent.identity, IntentStatus::Unresolved { failure })
                            .await?;
                        break;
                    }
                    self.mark(&intent.identity, IntentStatus::Failed { failure })
                        .await?;
                    failed_business += 1;
                }
                Err(error) if error.is_transient() => {
                    // Unknown outcome: this intent stays pending and
                    // the run stops, preserving queue order.
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(SubmitSummary {
            submitted,
            failed_business,
            still_pending: pending_at_start - submitted - failed_business,
        })
    }

    /// Resolve an unavailable outcome from application evidence. Never re-keys or resubmits it.
    pub async fn resolve(
        &self,
        identity: &[u8],
        resolution: Result<flower_protocol::Revision, flower_sdk::Failure>,
    ) -> Result<(), FlowerError> {
        let mut engine = self.engine.lock().await;
        let intent = engine
            .get_intent(identity)
            .map_err(engine_err)?
            .ok_or_else(|| FlowerError::Decode("unknown intent".into()))?;
        if !matches!(intent.status, IntentStatus::Unresolved { .. }) {
            return Err(FlowerError::Decode(
                "only unresolved intents accept explicit resolution".into(),
            ));
        }
        let status = match resolution {
            Ok(revision) => IntentStatus::Committed {
                revision: revision.0,
            },
            Err(failure) => IntentStatus::Failed { failure },
        };
        engine.mark_intent(identity, status).map_err(engine_err)
    }

    /// Drop finished intents but the newest `keep`.
    pub async fn prune_finished(&self, keep: usize) -> Result<usize, FlowerError> {
        let intents = {
            self.engine
                .lock()
                .await
                .list_intents(None)
                .map_err(engine_err)?
        };
        let mut finished: Vec<&Intent> = intents
            .iter()
            .filter(|intent| {
                matches!(
                    intent.status,
                    IntentStatus::Committed { .. } | IntentStatus::Failed { .. }
                )
            })
            .collect();
        finished.sort_by_key(|intent| intent.sequence);
        let drop_count = finished.len().saturating_sub(keep);
        let mut dropped = 0usize;
        for intent in finished.iter().take(drop_count) {
            let mut engine = self.engine.lock().await;
            if engine.drop_intent(&intent.identity).map_err(engine_err)? {
                dropped += 1;
            }
        }
        Ok(dropped)
    }

    /// Mark an intent's status.
    async fn mark(&self, identity: &[u8], status: IntentStatus) -> Result<(), FlowerError> {
        self.engine
            .lock()
            .await
            .mark_intent(identity, status)
            .map_err(engine_err)
    }
}
