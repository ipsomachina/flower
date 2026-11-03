//! Version-one call request DTOs. Trust and application contract checks remain host responsibilities.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CallPrincipal {
    pub subject: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claims: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub db: String,
    pub module: String,
    pub name: String,
    pub args: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<CallPrincipal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Value>,
}
impl QueryRequest {
    pub fn new(
        db: impl Into<String>,
        module: impl Into<String>,
        name: impl Into<String>,
        args: Value,
    ) -> Self {
        Self {
            db: db.into(),
            module: module.into(),
            name: name.into(),
            args,
            principal: None,
            timestamp_ms: None,
            credentials: None,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutationRequest {
    #[serde(flatten)]
    pub call: QueryRequest,
    pub idempotency_key: Option<String>,
    pub expected_revision: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WatchRequest {
    #[serde(flatten)]
    pub call: QueryRequest,
    #[serde(default)]
    pub deltas: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionStep {
    pub db: String,
    pub module: String,
    pub method: String,
    #[serde(default)]
    pub args: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionRequest {
    pub steps: Vec<TransactionStep>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<CallPrincipal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Value>,
}
