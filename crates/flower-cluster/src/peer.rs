//! The member channel: what one member asks another beyond OpenRaft's
//! own RPCs.
//!
//! A follower asks its leader for read fences (see [`crate::read`]),
//! forwards client requests only the leader can serve to the leader's
//! API, and asks where that API serves clients so replies can name it.
//! Requests ride the peer transport ([`crate::network`]) as `Member`
//! requests: on the connections Raft uses, authenticated by the peer
//! token and the node identity in the hello and under TLS when
//! configured, or through the in-process registry.
//! [`crate::network::Peers::member`] sends them; each member answers
//! through its [`MemberDesk`].
//!
//! A forwarded request is the client's HTTP request as it arrived:
//! method, path, the headers the leader needs (the caller's credential
//! among them) and the body. The leader runs it through its own API as
//! if the client had called it, so its credential checks, admission and
//! idempotency all apply, and the follower relays the answer verbatim.
//! A forwarded request is never forwarded again.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, PoisonError, RwLock, Weak};

use serde::{Deserialize, Serialize};

use crate::node::NodeError;
use crate::read::{ReadFence, Reads};
use crate::types::NodeId;

/// A boxed, sendable future.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Failure code: a forwarded request's outcome is unknown (the
/// exchange with the leader broke after it may have arrived). Retry it
/// with the same idempotency key.
pub const FORWARD_FAILED: &str = "forward_failed";

/// One request to another member.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MemberRequest {
    /// Prove a fresh read as leader, never asking anyone else.
    ReadFence,
    /// Serve a client request through the leader's API.
    Forward(ForwardedRequest),
    /// Where this member's API serves clients.
    Describe,
    /// Change the cluster's catalog or a group's placements, as the
    /// group's leader (see [`ControlRequest`]).
    Control(ControlRequest),
}

/// A change one group asks of another group's leader: catalog changes
/// to the root, placement changes to the group that holds a database.
/// Each is idempotent under its operation, so a sender that lost the
/// answer sends it again.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlRequest {
    /// A catalog change, on the root group.
    Catalog(flower_engine::catalog::CatalogChange),
    /// A placement change of `db`.
    Place {
        /// The database.
        db: Vec<u8>,
        /// The change.
        command: flower_engine::placement::PlacementCommand,
    },
    /// A two-phase commit step or question (see [`crate::txn`]).
    Txn(crate::txn::TxnRequest),
    /// A page of a moving database's rows, from its source's leader.
    Export {
        /// The database.
        db: Vec<u8>,
        /// The move.
        op: String,
        /// What to read.
        kind: crate::node::ExportKind,
        /// Where to resume.
        cursor: Option<flower_engine::movement::Cursor>,
        /// The page's size bound.
        max_bytes: usize,
    },
    /// The digest an import must reach, from the source's leader.
    ExportDigest {
        /// The database.
        db: Vec<u8>,
        /// The move.
        op: String,
        /// Of the base, or of the frozen state.
        kind: crate::node::ExportKind,
    },
    /// A database's placement in the group, linearizable.
    Inspect {
        /// The database.
        db: Vec<u8>,
    },
    /// Whether the group's leader can serve a database's managed keys
    /// as it holds them: a move's preflight (see [`crate::moves`]).
    Keys {
        /// The database.
        db: Vec<u8>,
    },
}

/// A control request's outcome: the answer as JSON text, or the
/// deterministic refusal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlAnswer {
    /// The change applied (or had): its answer, as JSON text.
    Applied(String),
    /// The state refused it.
    Refused {
        /// Machine-readable code.
        code: String,
        /// Human-readable message.
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<String>,
    },
}

/// One member's answer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MemberResponse {
    /// The proven fence.
    ReadFence(ReadFence),
    /// The API's answer to a forwarded request.
    Forward(ForwardedResponse),
    /// The member's advertised API URL, if any.
    Describe {
        /// Base URL clients reach this member's API at.
        api_url: Option<String>,
    },
    /// A control request's outcome.
    Control(ControlAnswer),
}

/// Answers control requests as its group's leader.
pub trait ControlHandler: Send + Sync {
    /// Apply one control request, or say this member does not lead
    /// ([`MemberError::NotLeader`]): it ran nothing.
    fn control(
        self: Arc<Self>,
        request: ControlRequest,
    ) -> BoxFuture<Result<ControlAnswer, MemberError>>;
}

/// Why a member request failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberError {
    /// The member does not lead; it ran nothing.
    NotLeader {
        /// The leader it knows, if any.
        leader: Option<NodeId>,
    },
    /// The member could not be reached: nothing was sent.
    Unreachable(String),
    /// The exchange broke or timed out after the request may have
    /// arrived: its outcome is unknown.
    Broken(String),
    /// The member refused or failed the request.
    Failed {
        /// Machine-readable code.
        code: String,
        /// Human-readable message.
        message: String,
    },
}

impl std::fmt::Display for MemberError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemberError::NotLeader { leader } => write!(f, "not the leader (leader: {leader:?})"),
            MemberError::Unreachable(message) => write!(f, "unreachable: {message}"),
            MemberError::Broken(message) => write!(f, "exchange broke: {message}"),
            MemberError::Failed { code, message } => write!(f, "{code}: {message}"),
        }
    }
}

impl std::error::Error for MemberError {}

impl From<NodeError> for MemberError {
    fn from(error: NodeError) -> Self {
        match error {
            NodeError::Forward { leader } => MemberError::NotLeader { leader },
            // Member requests answer with no call's details: queries'
            // reach callers over HTTP.
            NodeError::Failure { code, message } | NodeError::Call { code, message, .. } => {
                MemberError::Failed { code, message }
            }
            NodeError::Fatal(message) => MemberError::Failed {
                code: "fatal".to_string(),
                message,
            },
            NodeError::Overloaded { message, .. } => MemberError::Failed {
                code: "overloaded".to_string(),
                message,
            },
        }
    }
}

/// A client request forwarded to the leader's API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardedRequest {
    /// HTTP method.
    pub method: String,
    /// Path and query string.
    pub path: String,
    /// The headers the leader's API reads.
    pub headers: Vec<(String, String)>,
    /// The body: API bodies are JSON, so UTF-8.
    pub body: String,
}

/// The leader API's answer to a forwarded request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardedResponse {
    /// HTTP status.
    pub status: u16,
    /// The headers the client reads.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: String,
}

/// Serves forwarded requests through this member's API.
pub trait Forwarder: Send + Sync {
    /// Run one forwarded request as if a client had sent it here. An
    /// API that answered that it does not lead reports
    /// [`MemberError::NotLeader`] instead: it ran nothing, so the sender
    /// may try the next leader.
    fn forward(
        &self,
        request: ForwardedRequest,
    ) -> BoxFuture<Result<ForwardedResponse, MemberError>>;
}

/// A member's receiving side of the channel.
pub struct MemberDesk {
    reads: Arc<Reads>,
    /// The API forwarded requests run through, while one serves. Weak:
    /// the API holds this member, and a strong reference here would
    /// keep both alive after serving stops.
    forwarder: RwLock<Option<Weak<dyn Forwarder>>>,
    /// What answers control requests, while set. Weak, as the
    /// forwarder: the member holds this desk.
    control: RwLock<Option<Weak<dyn ControlHandler>>>,
    api_url: RwLock<Option<String>>,
}

impl MemberDesk {
    pub(crate) fn new(reads: Arc<Reads>) -> Self {
        MemberDesk {
            reads,
            forwarder: RwLock::new(None),
            control: RwLock::new(None),
            api_url: RwLock::new(None),
        }
    }

    /// Answer control requests through `handler` while it lives.
    pub fn set_control(&self, handler: Weak<dyn ControlHandler>) {
        *self.control.write().unwrap_or_else(PoisonError::into_inner) = Some(handler);
    }

    /// Serve forwarded requests through `forwarder` while it lives.
    pub fn set_forwarder(&self, forwarder: Weak<dyn Forwarder>) {
        *self
            .forwarder
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(forwarder);
    }

    /// Advertise where this member's API serves clients.
    pub fn set_api_url(&self, url: Option<String>) {
        *self.api_url.write().unwrap_or_else(PoisonError::into_inner) = url;
    }

    /// Where this member's API serves clients, if advertised.
    pub fn api_url(&self) -> Option<String> {
        self.api_url
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Answer one member request.
    pub async fn handle(&self, request: MemberRequest) -> Result<MemberResponse, MemberError> {
        match request {
            MemberRequest::ReadFence => {
                Ok(MemberResponse::ReadFence(self.reads.serve_fence().await?))
            }
            MemberRequest::Forward(request) => {
                let forwarder = self
                    .forwarder
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .as_ref()
                    .and_then(Weak::upgrade);
                let Some(forwarder) = forwarder else {
                    return Err(MemberError::Failed {
                        code: "unavailable".to_string(),
                        message: "this member serves no API".to_string(),
                    });
                };
                forwarder
                    .forward(request)
                    .await
                    .map(MemberResponse::Forward)
            }
            MemberRequest::Describe => Ok(MemberResponse::Describe {
                api_url: self.api_url(),
            }),
            MemberRequest::Control(request) => {
                let handler = self
                    .control
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .as_ref()
                    .and_then(Weak::upgrade);
                let Some(handler) = handler else {
                    return Err(MemberError::Failed {
                        code: "unavailable".to_string(),
                        message: "this member answers no control requests".to_string(),
                    });
                };
                handler.control(request).await.map(MemberResponse::Control)
            }
        }
    }

    /// A leader's append that arrived `at` succeeded, leaving this
    /// member's log ending at `end` while the leader had committed
    /// `commit` (see [`crate::read`]).
    pub fn heard(&self, at: std::time::Instant, commit: Option<u64>, end: Option<u64>) {
        self.reads.heard(at, commit, end);
    }
}
