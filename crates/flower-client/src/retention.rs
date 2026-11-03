//! Scoped retry identities and retry sessions.
//!
//! A caller's own request ID deduplicates while its receipt is kept and
//! runs as a new request after. A scoped request ID instead names the
//! history and the retry epoch it was minted in, so the server refuses
//! it (`epoch_retired`, `wrong_history`) rather than running it again
//! once its outcome is gone: fetch the identity with
//! [`FlowerClient::retry_identity`], mint an ID per business intent with
//! [`RetryIdentity::request_id`], persist it before sending, and pass it
//! as [`crate::MutationOptions::request_id`]. A refusal does not prove
//! the original failed: resolve it through a business identifier.
//!
//! A retry session bounds what the server keeps to what its consumer
//! has not acknowledged yet. Open one under an ID of your choosing
//! (persist it first: opening again answers the same session), send
//! requests [`RetrySession::request_id`] with sequences you allocate,
//! and acknowledge a contiguous prefix once its results are durably
//! consumed: their results go, and those sequences never run again.
//! Closing is terminal. Acknowledging past a sequence that never ran
//! takes an explicit `abandon`, never a reaction to a timeout.

use serde::Deserialize;

pub use flower_protocol::Acknowledgement;

use crate::client::{FlowerClient, Method, RequestOptions, decode_ok};
use crate::error::FlowerError;

/// The identity new scoped request IDs take in one database.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetryIdentity {
    /// The history incarnation.
    pub incarnation: u64,
    /// The current epoch.
    pub epoch: u64,
    /// Identities below it are retired.
    pub min_epoch: u64,
    /// Leader time per epoch when the database rotates.
    pub epoch_ms: Option<u64>,
    /// Whether retention is initialized for the database.
    pub initialized: bool,
}

impl RetryIdentity {
    /// The request ID of business intent `intent` in this identity's
    /// epoch: the same intent bytes always give the same ID.
    pub fn request_id(&self, intent: &[u8]) -> String {
        use sha2::Digest;
        let hash: [u8; 32] = sha2::Sha256::digest(intent).into();
        flower_protocol::request_id(self.incarnation, self.epoch, &hash)
    }
}

/// One retry session.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetrySession {
    /// The caller-chosen 128-bit ID.
    pub id: [u8; 16],
    /// The history it was opened in.
    pub incarnation: u64,
    /// The epoch its requests carry.
    pub epoch: u64,
    /// Every sequence at or below it is acknowledged.
    pub acknowledged: u64,
    /// Whether it is closed for good.
    pub closed: bool,
}

impl RetrySession {
    /// The request ID of sequence `sequence`, from 1.
    pub fn request_id(&self, sequence: u64) -> String {
        flower_protocol::session_request_id(self.incarnation, self.epoch, &self.id, sequence)
    }
}

/// `GET /v1/retry`.
#[derive(Deserialize)]
struct IdentityBody {
    incarnation: String,
    epoch: u64,
    min_epoch: u64,
    epoch_ms: Option<u64>,
    initialized: bool,
}

/// A session as the server describes it.
#[derive(Deserialize)]
struct SessionDto {
    session: String,
    incarnation: String,
    epoch: u64,
    acknowledged: u64,
    closed: bool,
}

/// The `session` field of an `ok` envelope.
#[derive(Deserialize)]
struct SessionBody {
    session: SessionDto,
}

fn incarnation(text: &str) -> Result<u64, FlowerError> {
    u64::from_str_radix(text, 16)
        .map_err(|error| FlowerError::Decode(format!("incarnation `{text}`: {error}")))
}

impl FlowerClient {
    /// The identity new scoped request IDs take in `db`, read from the
    /// leader.
    pub async fn retry_identity(
        &self,
        db: &str,
        options: &RequestOptions,
    ) -> Result<RetryIdentity, FlowerError> {
        let body = self
            .send(
                Method::Get,
                &format!("/v1/retry?db={}", crate::admin::encode_query(db)),
                None,
                options,
            )
            .await?;
        let body: IdentityBody = serde_json::from_slice(&body)
            .map_err(|error| FlowerError::Decode(format!("retry identity: {error}")))?;
        Ok(RetryIdentity {
            incarnation: incarnation(&body.incarnation)?,
            epoch: body.epoch,
            min_epoch: body.min_epoch,
            epoch_ms: body.epoch_ms,
            initialized: body.initialized,
        })
    }

    /// Open retry session `id` in `db` for the principal `module`'s
    /// authorization decides, or recover it: opening again answers the
    /// same session to its owner.
    pub async fn open_retry_session(
        &self,
        db: &str,
        module: &str,
        id: [u8; 16],
        options: &RequestOptions,
    ) -> Result<RetrySession, FlowerError> {
        self.session_operation(db, module, id, "open", serde_json::json!({}), options)
            .await
    }

    /// A session's state, for its owner.
    pub async fn retry_session_status(
        &self,
        db: &str,
        module: &str,
        id: [u8; 16],
        options: &RequestOptions,
    ) -> Result<RetrySession, FlowerError> {
        self.session_operation(db, module, id, "status", serde_json::json!({}), options)
            .await
    }

    /// Acknowledge every sequence through `ack.through` once its results
    /// are durably consumed. [`Acknowledgement::abandoning`] also fences
    /// sequences without a result: use it only to give up on them
    /// deliberately. [`Acknowledgement::limit`] bounds the work one
    /// acknowledgement does (256 by default, at most 65,536): a strict
    /// one past more sequences is refused (`retry_ack_budget`), and an
    /// abandon deletes that many results at once, leaving the rest to
    /// collection.
    pub async fn acknowledge_retry_session(
        &self,
        db: &str,
        module: &str,
        id: [u8; 16],
        ack: Acknowledgement,
        options: &RequestOptions,
    ) -> Result<RetrySession, FlowerError> {
        let extra = serde_json::json!({
            "through": ack.through,
            "abandon": ack.abandon,
            "limit": ack.limit,
        });
        self.session_operation(db, module, id, "ack", extra, options)
            .await
    }

    /// Close a session for good: none of its requests runs again, and
    /// results still in flight may become unavailable.
    pub async fn close_retry_session(
        &self,
        db: &str,
        module: &str,
        id: [u8; 16],
        options: &RequestOptions,
    ) -> Result<RetrySession, FlowerError> {
        self.session_operation(db, module, id, "close", serde_json::json!({}), options)
            .await
    }

    async fn session_operation(
        &self,
        db: &str,
        module: &str,
        id: [u8; 16],
        operation: &str,
        extra: serde_json::Value,
        options: &RequestOptions,
    ) -> Result<RetrySession, FlowerError> {
        let mut request = serde_json::json!({
            "db": db,
            "module": module,
            "session": flower_protocol::hex(&id),
            "operation": operation,
        });
        if let (Some(request), serde_json::Value::Object(extra)) = (request.as_object_mut(), extra)
        {
            request.extend(extra);
        }
        let body = self
            .send(Method::Post, "/v1/retry/session", Some(&request), options)
            .await?;
        let dto = decode_ok::<SessionBody>(&body)?.session;
        let id = flower_protocol::unhex(&dto.session)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| FlowerError::Decode(format!("session `{}`", dto.session)))?;
        Ok(RetrySession {
            id,
            incarnation: incarnation(&dto.incarnation)?,
            epoch: dto.epoch,
            acknowledged: dto.acknowledged,
            closed: dto.closed,
        })
    }

    /// `db`'s retention state for operators (admin credential): the
    /// identity fields [`FlowerClient::retry_identity`] reads plus the
    /// rotation (`keep_epochs`), the leader's maintenance counters
    /// (`stats`) and when the addressed member's unscoped receipts
    /// expire (`receipts`: `expiry_clock_ms`, `receipt_ttl_ms`).
    pub async fn retention_status(
        &self,
        db: &str,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let path = format!("/v1/admin/retention?db={}", crate::admin::encode_query(db));
        let body = self.send(Method::Get, &path, None, options).await?;
        serde_json::from_slice(&body)
            .map_err(|error| FlowerError::Decode(format!("retention status: {error}")))
    }

    /// Run an operator retention operation on `db` (admin credential):
    /// `{"operation": "rotate", "epoch_ms": .., "keep_epochs": ..}`,
    /// `{"operation": "advance", "from_epoch": .., "current_epoch": ..,
    /// "min_epoch": ..}` or `{"operation": "collect", "limit": ..}`. A
    /// refusal is [`FlowerError::Business`].
    pub async fn control_retention(
        &self,
        db: &str,
        operation: serde_json::Value,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let mut request = serde_json::json!({ "db": db });
        if let (Some(request), serde_json::Value::Object(operation)) =
            (request.as_object_mut(), operation)
        {
            request.extend(operation);
        }
        let body = self
            .send(Method::Post, "/v1/admin/retention", Some(&request), options)
            .await?;
        #[derive(Deserialize)]
        struct RetentionBody {
            retention: serde_json::Value,
        }
        decode_ok::<RetentionBody>(&body).map(|body| body.retention)
    }
}
