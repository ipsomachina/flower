//! Descriptor-based workers. The loop renews and settles leases automatically.
use crate::{JobControl, QueueClient, QueueWorkerOptions, WorkError, WorkerSummary};
use flower_client::{Claim, FlowerError};
use flower_sdk::{FlowerType, queue::TypedQueue};
use std::{future::Future, sync::Arc};

pub struct TypedJob<P> {
    pub payload: P,
    pub claim: Claim,
    pub control: JobControl,
}

/// A payload that violates the descriptor is dead-lettered with a terminal decode failure.
pub async fn run_typed_queue_worker<C, P, R, F, Fut>(
    client: C,
    queue: &TypedQueue<P, R>,
    mut options: QueueWorkerOptions,
    stop: Arc<tokio::sync::Notify>,
    handler: F,
) -> Result<WorkerSummary, FlowerError>
where
    C: QueueClient,
    P: FlowerType + Send,
    R: FlowerType,
    F: Fn(TypedJob<P>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), WorkError>> + Send + 'static,
{
    options.queue = queue.name.into();
    let handler = Arc::new(handler);
    crate::run_queue_worker_with(client, options, stop, move |claim, control| {
        let handler = handler.clone();
        async move {
            let value = flower_client::typed::json_value(&claim.payload)
                .map_err(|e| WorkError::Terminal(e.to_string()))?;
            let payload = P::decode(value)
                .map_err(|e| WorkError::Terminal(format!("{}: {}", e.code, e.message)))?;
            handler(TypedJob {
                payload,
                claim,
                control,
            })
            .await
        }
    })
    .await
}
