//! An explicit causal context, shared by clones and serialized per database.
use crate::{DatabaseClient, FlowerClient, FlowerError, QueryOptions, ReadToken, RequestOptions};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

type DatabaseState = Arc<tokio::sync::Mutex<Option<ReadToken>>>;
pub(crate) async fn acquire<'a>(
    state: &'a tokio::sync::Mutex<Option<ReadToken>>,
    options: &RequestOptions,
) -> Result<tokio::sync::MutexGuard<'a, Option<ReadToken>>, FlowerError> {
    let cancelled = async {
        match &options.cancel {
            Some(cancel) => cancel.cancelled().await,
            None => std::future::pending().await,
        }
    };
    let deadline = async {
        match options.deadline {
            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! { biased;
        _ = cancelled => Err(FlowerError::Cancelled),
        _ = deadline => Err(FlowerError::Timeout),
        held = state.lock() => Ok(held),
    }
}
#[derive(Clone)]
pub struct CausalSession {
    client: FlowerClient,
    states: Arc<Mutex<BTreeMap<String, DatabaseState>>>,
}
impl FlowerClient {
    /// Opt into read-your-writes and monotonic reads. Missing token support is an error.
    pub fn causal_session(&self) -> CausalSession {
        CausalSession {
            client: self.clone(),
            states: Arc::default(),
        }
    }
}
impl CausalSession {
    pub fn database(&self, name: impl Into<String>) -> DatabaseClient {
        let mut database = self.client.database(name);
        database.session = Some(self.clone());
        database
    }
    pub(crate) fn state(&self, db: &str) -> DatabaseState {
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(db.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone()
    }
    /// Import an observed commit or watch token; different histories require explicit reconciliation.
    pub async fn observe(&self, token: ReadToken) -> Result<(), FlowerError> {
        let state = self.state(&token.database);
        let mut held = state.lock().await;
        observe(&mut held, &token.database, Some(&token), false)
    }
    pub async fn token(&self, db: &str) -> Option<ReadToken> {
        self.state(db).lock().await.clone()
    }
    /// Deliberately begin a new causal context after reconciling a history change.
    pub async fn reset(&self, db: &str) {
        *self.state(db).lock().await = None;
    }
}
pub(crate) fn bound_options(
    held: &Option<ReadToken>,
    options: &QueryOptions,
) -> Result<RequestOptions, FlowerError> {
    let mut options = options.0.clone();
    if let Some(previous) = held {
        let mut bound = previous.clone();
        if let Some(requested) = &options.at_least {
            requested.validate().map_err(FlowerError::Decode)?;
            if requested.database != previous.database
                || requested.group != previous.group
                || requested.incarnation != previous.incarnation
            {
                return Err(FlowerError::Unsupported(
                    "explicit read bound and causal session refer to incomparable histories".into(),
                ));
            }
            bound.revision = bound.revision.max(requested.revision);
            bound.position = bound.position.max(requested.position);
        }
        options.at_least = Some(bound);
    }
    Ok(options)
}
pub(crate) fn observe(
    held: &mut Option<ReadToken>,
    db: &str,
    token: Option<&ReadToken>,
    proof: bool,
) -> Result<(), FlowerError> {
    let token = token.ok_or_else(|| {
        FlowerError::Unsupported(
            "causal sessions require peer read tokens; no weaker read was substituted".into(),
        )
    })?;
    token.validate().map_err(FlowerError::Decode)?;
    if token.database != db {
        return Err(FlowerError::Decode(
            "peer returned a read token for another database".into(),
        ));
    }
    if let Some(previous) = held {
        if previous.group == token.group && previous.incarnation == token.incarnation {
            previous.revision = previous.revision.max(token.revision);
            previous.position = previous.position.max(token.position);
            return Ok(());
        }
        if !proof {
            return Err(FlowerError::Unsupported(
                "causal session history changed; reconcile before resetting the context".into(),
            ));
        }
        // A bound query or scoped write proves carried history across a move.
        // A duplicate's old result need not advance a newer observed revision.
        if token.revision < previous.revision {
            return Ok(());
        }
    }
    *held = Some(token.clone());
    Ok(())
}
