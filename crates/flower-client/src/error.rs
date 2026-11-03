//! Client errors.

/// Refusal codes that change nothing and end on their own, so the same
/// request succeeds later: `queue_purging`, an operation on a queue
/// that a removed module's purge is still deleting.
pub const CLEARING_REFUSALS: &[&str] = &["queue_purging"];

/// A client call failure.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum FlowerError {
    /// Structured HTTP failure, including origin, certainty and retry advice.
    Http(Box<flower_protocol::Problem>),
    /// A retry policy stopped; its final structured cause remains available.
    RetryFailed {
        last: Box<FlowerError>,
        attempts: u32,
        reason: RetryStop,
        execution: flower_protocol::Execution,
    },
    /// A transport or peer cannot enforce a requested guarantee.
    Unsupported(String),
    /// Transport failure: connection, DNS, TLS or protocol.
    Transport(String),
    /// Attempt timeout elapsed.
    Timeout,
    /// Decided business failure with the server's outcome.
    Business {
        /// Machine-readable failure code.
        code: String,
        /// Human-readable message.
        message: String,
        /// Committed revision, when reported.
        revision: Option<u64>,
        /// Applied log position, when reported.
        position: Option<u64>,
        /// Structured details, when the failure carries any (a declared
        /// error variant's payload).
        details: Option<serde_json::Value>,
    },
    /// The server is not the leader.
    Forward {
        /// Known leader, if any.
        leader: Option<u64>,
    },
    /// Authentication or authorization refused.
    Auth(String),
    /// Non-retryable server failure.
    Server {
        /// Machine-readable code.
        code: String,
        /// Human-readable message.
        message: String,
    },
    /// Local device-engine failure.
    Engine(String),
    /// Response decoding failed.
    Decode(String),
    /// Retries exhausted; carries the last failure message.
    RetryExhausted(String),
    /// A sync batch came from another history incarnation than the
    /// one this replica follows: the server restored or re-bootstrapped
    /// its history, and this replica must be rebuilt.
    HistoryChanged {
        /// Incarnation this replica follows.
        local: u64,
        /// Incarnation the server serves.
        remote: u64,
    },
    /// The caller cancelled the call (`RequestOptions::cancel`). An
    /// attempt in flight was dropped: a write may still have applied,
    /// and retrying it with the same request ID is safe.
    Cancelled,
}

impl std::fmt::Display for FlowerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(problem) => write!(
                f,
                "HTTP {} {}: {}",
                problem.status, problem.code, problem.message
            ),
            Self::RetryFailed {
                last,
                attempts,
                reason,
                ..
            } => write!(f, "retry stopped ({reason:?}, {attempts} attempts): {last}"),
            Self::Unsupported(message) => write!(f, "unsupported capability: {message}"),
            FlowerError::Transport(message) => write!(f, "transport: {message}"),
            FlowerError::Timeout => write!(f, "attempt timed out"),
            FlowerError::Business { code, message, .. } => write!(f, "{code}: {message}"),
            FlowerError::Forward { leader } => write!(f, "forward to leader: {leader:?}"),
            FlowerError::Auth(message) => write!(f, "auth: {message}"),
            FlowerError::Server { code, message } => write!(f, "server {code}: {message}"),
            FlowerError::Engine(message) => write!(f, "engine: {message}"),
            FlowerError::Decode(message) => write!(f, "decode: {message}"),
            FlowerError::RetryExhausted(message) => write!(f, "retries exhausted: {message}"),
            FlowerError::HistoryChanged { local, remote } => write!(
                f,
                "history changed: this replica follows incarnation {local:016x} but the \
                 server serves {remote:016x}; the server restored or re-bootstrapped its \
                 history, so rebuild this replica"
            ),
            FlowerError::Cancelled => write!(f, "cancelled by the caller"),
        }
    }
}

impl std::error::Error for FlowerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RetryFailed { last, .. } => Some(last),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RetryStop {
    Attempts,
    Deadline,
    OverloadBudget,
}

impl FlowerError {
    /// Whether the retry loop may try again.
    pub(crate) fn retryable(&self) -> bool {
        matches!(self, FlowerError::Transport(_) | FlowerError::Timeout)
    }

    /// Whether the failure may clear if the same request is sent again:
    /// transport failures, timeouts, forwards, retryable statuses (429,
    /// 502, 503, 504, also once retries ran out on them) and the
    /// refusals that end on their own ([`CLEARING_REFUSALS`]: a queue
    /// whose purge is still deleting it). Never any other decided
    /// business failure, an auth refusal or a cancellation.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Http(problem) => problem.retryable,
            Self::RetryFailed { last, .. } => last.is_transient(),
            FlowerError::Transport(_) | FlowerError::Timeout | FlowerError::Forward { .. } => true,
            FlowerError::RetryExhausted(_) => true,
            FlowerError::Business { code, .. } => CLEARING_REFUSALS.contains(&code.as_str()),
            FlowerError::Server { code, .. } => matches!(
                code.as_str(),
                "status 429" | "status 502" | "status 503" | "status 504"
            ),
            _ => false,
        }
    }

    pub fn problem(&self) -> Option<&flower_protocol::Problem> {
        match self {
            Self::Http(problem) => Some(problem),
            Self::RetryFailed { last, .. } => last.problem(),
            _ => None,
        }
    }

    /// Certainty is separate from whether another attempt could succeed.
    pub fn execution(&self) -> flower_protocol::Execution {
        use flower_protocol::Execution;
        match self {
            Self::Http(problem) => problem.execution,
            Self::RetryFailed { execution, .. } => *execution,
            Self::Business { code, .. } if flower_protocol::unresolved_outcome(code) => {
                Execution::Unknown
            }
            Self::Business { code, .. } if CLEARING_REFUSALS.contains(&code.as_str()) => {
                Execution::NotStarted
            }
            Self::Business { .. } => Execution::Decided,
            Self::Auth(_) | Self::Unsupported(_) => Execution::NotStarted,
            _ => Execution::Unknown,
        }
    }

    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Business { code, .. } | Self::Server { code, .. } => Some(code),
            Self::Http(problem) => Some(&problem.code),
            Self::RetryFailed { last, .. } => last.code(),
            _ => None,
        }
    }

    pub fn is_auth(&self) -> bool {
        matches!(self, Self::Auth(_))
            || self
                .problem()
                .is_some_and(|p| p.origin == flower_protocol::Origin::Authorization)
    }

    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout => true,
            Self::RetryFailed { last, .. } => last.is_timeout(),
            _ => false,
        }
    }
}

/// Map a local device-engine failure.
#[cfg(feature = "replica")]
pub(crate) fn engine_err<E>(error: E) -> FlowerError
where
    flower_sdk::Failure: From<E>,
{
    let failure = flower_sdk::Failure::from(error);
    FlowerError::Engine(format!("{}: {}", failure.code, failure.message))
}

#[cfg(test)]
mod tests {
    use super::FlowerError;

    fn business(code: &str) -> FlowerError {
        FlowerError::Business {
            code: code.to_string(),
            message: String::new(),
            revision: None,
            position: None,
            details: None,
        }
    }

    #[test]
    fn only_refusals_that_end_on_their_own_are_transient_business_failures() {
        assert!(business("queue_purging").is_transient());
        assert!(!business("OUT_OF_STOCK").is_transient());
        assert!(!business("deadline_exceeded").is_transient());
    }
}
