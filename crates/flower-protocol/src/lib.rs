//! Runtime-independent wire contracts shared by clients and servers.
//! Additive fields are allowed; versions and required fields are validated.
pub mod json;
pub mod request;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const VERSION: u32 = 1;
pub const TOKEN_HEADER: &str = "x-flower-read-token";
pub const AT_LEAST_HEADER: &str = "x-flower-at-least";
pub const CONSISTENCY_HEADER: &str = "x-flower-consistency";
pub const CAPABILITIES: &[&str] = &[
    "read_tokens",
    "scoped_commands",
    "watch_history",
    "sync_since",
];
pub const ACK_LIMIT: u64 = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    ReadTokens,
    ScopedCommands,
    WatchHistory,
    SyncSince,
}
impl Capability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadTokens => "read_tokens",
            Self::ScopedCommands => "scoped_commands",
            Self::WatchHistory => "watch_history",
            Self::SyncSince => "sync_since",
        }
    }
    pub const ALL: &'static [Self] = &[
        Self::ReadTokens,
        Self::ScopedCommands,
        Self::WatchHistory,
        Self::SyncSince,
    ];
}

/// The published version-one wire schema and cross-language golden cases.
pub const WIRE_SCHEMA: &str = include_str!("../schema/v1.schema.json");
pub const GOLDEN_FIXTURES: &str = include_str!("../fixtures/v1.json");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Discovery {
    pub version: u32,
    pub capabilities: Vec<String>,
}

/// These refusals cannot establish the outcome of the original delivery.
pub fn unresolved_outcome(code: &str) -> bool {
    matches!(
        code,
        "wrong_history"
            | "epoch_retired"
            | "receipt_released"
            | "already_acknowledged"
            | "retry_session_closed"
            | "retry_session_unknown"
            | "retry_session_mismatch"
            | "outcome_unavailable"
    )
}

/// One call outcome. Missing values differ from explicit JSON null.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallEnvelope {
    pub ok: bool,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revisions: Option<std::collections::BTreeMap<String, u64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub positions: Option<std::collections::BTreeMap<String, u64>>,
    #[serde(default)]
    pub duplicate: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_token: Option<ReadToken>,
}
fn present_value<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}
impl CallEnvelope {
    pub fn validate(&self, write: bool) -> Result<(), String> {
        if self.ok && self.value.is_none() {
            return Err("successful call has no value".into());
        }
        if !self.ok && (self.code.is_none() || self.message.is_none()) {
            return Err("failed call has no code or message".into());
        }
        if write && (self.revision.is_none() || self.position.is_none()) {
            return Err("write outcome has no ordering metadata".into());
        }
        if let Some(token) = &self.read_token {
            token.validate()?;
        }
        Ok(())
    }
}

macro_rules! counter {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(#[serde(with = "decimal")] pub u64);
    };
}
counter!(Revision);
counter!(LogPosition);
counter!(Incarnation);

/// Counters carry their scope and history; positions and revisions never interchange.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadToken {
    pub version: u32,
    pub database: String,
    pub group: String,
    pub incarnation: Incarnation,
    pub revision: Revision,
    pub position: LogPosition,
}

impl ReadToken {
    pub fn encode(&self) -> Result<String, String> {
        use base64::Engine;
        self.validate()?;
        serde_json::to_vec(self)
            .map(|bytes| base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(bytes))
            .map_err(|e| e.to_string())
    }
    pub fn decode(text: &str) -> Result<Self, String> {
        use base64::Engine;
        if text.len() > 8192 {
            return Err("read token too large".into());
        }
        let bytes = base64::prelude::BASE64_URL_SAFE_NO_PAD
            .decode(text)
            .map_err(|e| e.to_string())?;
        let token: Self = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        token.validate()?;
        Ok(token)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.version != VERSION
            || self.database.is_empty()
            || self.group.is_empty()
            || self.incarnation.0 == 0
        {
            return Err("invalid or unsupported read token".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Execution {
    NotStarted,
    Decided,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    Application,
    Authorization,
    Admission,
    Host,
    Protocol,
}

/// Error semantics are explicit and independent of message text or code casing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Problem {
    pub status: u16,
    pub code: String,
    pub message: String,
    pub origin: Origin,
    pub execution: Execution,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchHistory {
    pub group: String,
    pub incarnation: Incarnation,
}
impl WatchHistory {
    pub fn validate(&self) -> Result<(), String> {
        if self.group.is_empty() || self.incarnation.0 == 0 {
            return Err("watch history must be nonempty and initialized".into());
        }
        Ok(())
    }
}

impl Problem {
    pub fn validate(&self) -> Result<(), String> {
        if !(100..=999).contains(&self.status) {
            return Err("problem status must be an HTTP status code".into());
        }
        Ok(())
    }
    /// Conservative semantics for older peers that do not send extensions.
    pub fn from_body(status: u16, body: &[u8], retry_after_ms: Option<u64>) -> Self {
        let json: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let decided = status == 422
            && json["ok"] == false
            && json["code"].is_string()
            && json["message"].is_string();
        let mut problem = Self {
            status,
            code: json["code"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("status {status}")),
            message: json["message"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| String::from_utf8_lossy(body).into_owned()),
            origin: match status {
                401 | 403 => Origin::Authorization,
                422 if decided => Origin::Application,
                400 | 404 | 413 | 415 => Origin::Protocol,
                _ => Origin::Host,
            },
            execution: match status {
                400 | 401 | 403 | 404 | 413 | 415 => Execution::NotStarted,
                422 if decided => Execution::Decided,
                _ => Execution::Unknown,
            },
            retryable: matches!(status, 429 | 502 | 503 | 504) && json["code"] != "unavailable",
            details: json.get("details").cloned(),
            retry_after_ms: retry_after_ms.or_else(|| json["retry_after_ms"].as_u64()),
        };
        if let Some(value) = json.get("problem")
            && let Ok(explicit) = serde_json::from_value::<Self>(value.clone())
            && explicit.validate().is_ok()
            && explicit.status == status
        {
            problem = explicit;
            problem.retry_after_ms = retry_after_ms.or(problem.retry_after_ms);
        }
        if problem.code == "overloaded" {
            problem.origin = Origin::Admission;
            problem.execution = Execution::NotStarted;
        }
        problem
    }
}

/// Stable pagination coverage, shared even when a sync payload is opaque.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncBatch<W> {
    pub version: u32,
    pub since: u64,
    pub entries: Vec<SyncEntry<W>>,
    pub current: u64,
    pub snapshot: Option<SyncSnapshot>,
    pub incarnation: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncEntry<W> {
    pub index: u64,
    pub write: W,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncSnapshot {
    pub last_index: u64,
    pub bytes_b64: String,
}
impl<W> SyncBatch<W> {
    pub fn validate(&self, position: u64) -> Result<(), String> {
        if self.version != VERSION {
            return Err(format!("unsupported sync version {}", self.version));
        }
        if self.since != position || self.current < self.since {
            return Err("sync coverage does not continue local position".into());
        }
        let mut previous = position;
        for entry in &self.entries {
            if entry.index <= previous || entry.index > self.current {
                return Err("sync entries outside ordered coverage".into());
            }
            previous = entry.index;
        }
        if let Some(snapshot) = &self.snapshot
            && (snapshot.last_index < position
                || snapshot.last_index > self.current
                || !self.entries.is_empty())
        {
            return Err("invalid sync snapshot coverage".into());
        }
        Ok(())
    }
}

/// Retention acknowledgement shared with the engine, without an engine dependency.
#[derive(Clone, Copy, Debug)]
pub struct Acknowledgement {
    pub through: u64,
    pub abandon: bool,
    pub limit: u64,
}
impl Acknowledgement {
    pub fn through(through: u64) -> Self {
        Self {
            through,
            abandon: false,
            limit: ACK_LIMIT,
        }
    }
    pub fn abandoning(mut self) -> Self {
        self.abandon = true;
        self
    }
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = limit;
        self
    }
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let nibble = |b: u8| match b {
                b'0'..=b'9' => Some(b - b'0'),
                b'a'..=b'f' => Some(b - b'a' + 10),
                _ => None,
            };
            Some(nibble(pair[0])? * 16 + nibble(pair[1])?)
        })
        .collect()
}
pub fn request_id(incarnation: u64, epoch: u64, intent: &[u8]) -> String {
    format!("f1:{incarnation:016x}:{epoch}:{}", hex(intent))
}
pub fn session_request_id(
    incarnation: u64,
    epoch: u64,
    session: &[u8; 16],
    sequence: u64,
) -> String {
    format!("f2:{incarnation:016x}:{epoch}:{}:{sequence}", hex(session))
}

mod decimal {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(n: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&n.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        let text = String::deserialize(d)?;
        let n = text.parse::<u64>().map_err(serde::de::Error::custom)?;
        if text != n.to_string() {
            return Err(serde::de::Error::custom("noncanonical counter"));
        }
        Ok(n)
    }
}
