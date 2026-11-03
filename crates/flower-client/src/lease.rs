//! Typed claims and lease-bound operations for manually managed workers.
use crate::{FlowerClient, FlowerError, RequestOptions};
use flower_sdk::{FlowerType, queue::TypedQueue};
use std::time::Duration;

/// An owned claim. Terminal operations consume it; failed operations return it.
/// Dropping this handle does not perform network I/O: the server lease expires normally.
///
/// ```compile_fail
/// # async fn example(lease: flower_client::QueueLease<String>, options: &flower_client::RequestOptions) {
/// lease.complete(options).await.unwrap();
/// lease.release(options).await.unwrap(); // Already settled: ownership was consumed.
/// # }
/// ```
#[must_use = "settle the claim or allow its lease to expire"]
pub struct QueueLease<R = flower_sdk::Value> {
    client: FlowerClient,
    database: String,
    queue: String,
    claim: crate::Claim,
    result: std::marker::PhantomData<fn() -> R>,
}
pub struct TypedClaim<P, R = flower_sdk::Value> {
    pub payload: P,
    pub lease: QueueLease<R>,
}
/// A malformed payload keeps its original value and its claim for explicit settlement.
pub struct ClaimDecodeError<R> {
    pub payload: serde_json::Value,
    pub lease: Box<QueueLease<R>>,
    pub source: Box<FlowerError>,
}
/// A failed settlement preserves the same claim identity, including on timeout or cancellation.
pub struct SettlementError<R> {
    pub lease: Box<QueueLease<R>>,
    pub source: Box<FlowerError>,
}
impl<R> SettlementError<R> {
    pub fn into_lease(self) -> QueueLease<R> {
        *self.lease
    }
}
impl<R> std::fmt::Debug for QueueLease<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueLease")
            .field("database", &self.database)
            .field("queue", &self.queue)
            .field("id", &self.claim.id)
            .field("fence", &self.claim.fence)
            .finish_non_exhaustive()
    }
}
impl<P: std::fmt::Debug, R> std::fmt::Debug for TypedClaim<P, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedClaim")
            .field("payload", &self.payload)
            .field("lease", &self.lease)
            .finish()
    }
}
macro_rules! claim_error {
    ($name:ident) => {
        impl<R> std::fmt::Debug for $name<R> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("lease", &self.lease)
                    .field("source", &self.source)
                    .finish()
            }
        }
        impl<R> std::fmt::Display for $name<R> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "claim {}: {}", self.lease.claim.id, self.source)
            }
        }
        impl<R: 'static> std::error::Error for $name<R> {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(self.source.as_ref())
            }
        }
    };
}
claim_error!(ClaimDecodeError);
claim_error!(SettlementError);

pub type ClaimOutcome<P, R = flower_sdk::Value> = Result<TypedClaim<P, R>, ClaimDecodeError<R>>;

impl<R> QueueLease<R> {
    fn settled(self, result: Result<(), FlowerError>) -> Result<(), SettlementError<R>> {
        result.map_err(|source| SettlementError {
            lease: Box::new(self),
            source: Box::new(source),
        })
    }
}
impl<R: FlowerType> QueueLease<R> {
    pub fn id(&self) -> &str {
        &self.claim.id
    }
    pub fn fence(&self) -> u64 {
        self.claim.fence
    }
    pub fn scope(&self) -> &str {
        &self.claim.scope
    }
    /// Expiration granted by the initial claim; heartbeat does not return an updated timestamp.
    pub fn expires_ms(&self) -> u64 {
        self.claim.expires_ms
    }
    pub async fn heartbeat(
        &self,
        lease: Duration,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let ms = u64::try_from(lease.as_millis())
            .map_err(|_| FlowerError::Decode("lease duration exceeds wire range".into()))?;
        self.client
            .heartbeat_queue(
                &self.database,
                &self.queue,
                &self.claim.id,
                &self.claim.token,
                Some(ms),
                options,
            )
            .await
    }
    pub async fn complete(self, options: &RequestOptions) -> Result<(), SettlementError<R>> {
        let result = self
            .client
            .complete_queue(
                &self.database,
                &self.queue,
                &self.claim.id,
                &self.claim.token,
                options,
            )
            .await;
        self.settled(result)
    }
    pub async fn complete_with(
        self,
        result: &R,
        options: &RequestOptions,
    ) -> Result<(), SettlementError<R>> {
        let result = async {
            let value = crate::typed::value_json(
                &result
                    .encode()
                    .map_err(|e| FlowerError::Decode(e.message))?,
            )?;
            self.client
                .complete_queue_with(
                    &self.database,
                    &self.queue,
                    &self.claim.id,
                    &self.claim.token,
                    &value,
                    options,
                )
                .await
        }
        .await;
        self.settled(result)
    }
    pub async fn fail(
        self,
        delay: Duration,
        options: &RequestOptions,
    ) -> Result<(), SettlementError<R>> {
        let result = async {
            let ms = u64::try_from(delay.as_millis())
                .map_err(|_| FlowerError::Decode("delay exceeds wire range".into()))?;
            self.client
                .fail_queue(
                    &self.database,
                    &self.queue,
                    &self.claim.id,
                    &self.claim.token,
                    Some(ms),
                    options,
                )
                .await
        }
        .await;
        self.settled(result)
    }
    /// Move an incompatible or permanently failed claim to the dead-letter queue.
    pub async fn kill(self, options: &RequestOptions) -> Result<(), SettlementError<R>> {
        let result = self
            .client
            .kill_queue(
                &self.database,
                &self.queue,
                &self.claim.id,
                &self.claim.token,
                options,
            )
            .await;
        self.settled(result)
    }
    pub async fn release(self, options: &RequestOptions) -> Result<(), SettlementError<R>> {
        let result = self
            .client
            .release_queue(
                &self.database,
                &self.queue,
                &self.claim.id,
                &self.claim.token,
                None,
                options,
            )
            .await;
        self.settled(result)
    }
}
impl FlowerClient {
    pub async fn claim_typed<P: FlowerType, R: FlowerType>(
        &self,
        db: &str,
        queue: &TypedQueue<P, R>,
        poll: &crate::QueuePoll<'_>,
        options: &RequestOptions,
    ) -> Result<Vec<ClaimOutcome<P, R>>, FlowerError> {
        let claims = self.claim_queue(db, queue.name, poll, options).await?;
        Ok(claims
            .into_iter()
            .map(|claim| {
                let decoded = crate::typed::json_value(&claim.payload)
                    .and_then(|value| P::decode(value).map_err(|e| FlowerError::Decode(e.message)));
                let lease = QueueLease {
                    client: self.clone(),
                    database: db.into(),
                    queue: queue.name.into(),
                    claim,
                    result: std::marker::PhantomData,
                };
                match decoded {
                    Ok(payload) => Ok(TypedClaim { payload, lease }),
                    Err(source) => Err(ClaimDecodeError {
                        payload: lease.claim.payload.clone(),
                        lease: Box::new(lease),
                        source: Box::new(source),
                    }),
                }
            })
            .collect())
    }
}
