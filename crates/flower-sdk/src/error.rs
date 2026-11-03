//! Guest errors: [`Failure`] and the [`Result`] alias.
//!
//! [`fail`] and [`fail_with`] create structured application failures.
//! Infrastructure, trap and budget failures stay distinct from
//! application-declared errors at the call boundary.

use alloc::string::String;
use core::convert::Into;

use crate::value::Value;

/// Failure codes produced below declared business errors.
///
/// Two codes separate the common codec rejections: [`TYPE_MISMATCH`][codes::TYPE_MISMATCH]
/// means the JSON kind is wrong (a string where a number belongs), while
/// [`INVALID_VALUE`][codes::INVALID_VALUE] means the kind is right but the
/// content is not (a non-integer number, malformed decimal, duplicate object
/// key, non-finite float or malformed binary encoding).
pub mod codes {
    /// Wrong JSON kind for the expected type.
    pub const TYPE_MISMATCH: &str = "TYPE_MISMATCH";
    /// Required object member absent.
    pub const MISSING_FIELD: &str = "MISSING_FIELD";
    /// Right kind, invalid content.
    pub const INVALID_VALUE: &str = "INVALID_VALUE";
    /// Nesting exceeds [`crate::value::MAX_VALUE_DEPTH`].
    pub const DEPTH_LIMIT: &str = "DEPTH_LIMIT";
    /// Redacted field has no encodable value.
    pub const REDACTED_VALUE: &str = "REDACTED_VALUE";
    /// Value rejected by its [`crate::schema::Schema`].
    pub const SCHEMA_MISMATCH: &str = "SCHEMA_MISMATCH";
    /// Arguments rejected by their definition's contract: `details.path`
    /// names the offending value.
    pub const INVALID_ARGUMENT: &str = "INVALID_ARGUMENT";
    /// Invalid SQL syntax, logical binding, plan name or limit.
    pub const SQL_INVALID: &str = "SQL_INVALID";
    /// A named SQL plan exceeded its configured resource ceilings.
    pub const SQL_BUDGET: &str = "SQL_BUDGET";
    /// Unknown method, definition or invocation kind.
    pub const UNKNOWN_METHOD: &str = "UNKNOWN_METHOD";
    /// Malformed invocation envelope or kind mismatch.
    pub const INVALID_INVOCATION: &str = "INVALID_INVOCATION";
    /// No such managed key binding.
    pub const UNKNOWN_KEY: &str = "UNKNOWN_KEY";
    /// The binding grants no such usage.
    pub const FORBIDDEN_USAGE: &str = "FORBIDDEN_USAGE";
    /// Signature bytes are malformed.
    pub const BAD_SIGNATURE: &str = "BAD_SIGNATURE";
    /// A collection or derived access rule denies the operation.
    pub const ACCESS_DENIED: &str = "ACCESS_DENIED";
    /// The authorization hook found no acceptable credentials.
    pub const UNAUTHENTICATED: &str = "UNAUTHENTICATED";
    /// The authorization hook denies the method to this caller.
    pub const FORBIDDEN: &str = "FORBIDDEN";
    /// Another module's collection, derived value or method is not
    /// exported, or not to this caller.
    pub const MODULE_PRIVATE: &str = "MODULE_PRIVATE";
    /// Another module's exported collection is read-only here.
    pub const MODULE_READ_ONLY: &str = "MODULE_READ_ONLY";
    /// A deployment would break a link between modules.
    pub const MODULE_CONFLICT: &str = "MODULE_CONFLICT";
    /// A module broke its exported contract at a call boundary.
    pub const MODULE_CONTRACT_VIOLATION: &str = "MODULE_CONTRACT_VIOLATION";
}

/// Structured application failure: code, message and optional details.
#[derive(Clone, Debug, PartialEq)]
pub struct Failure {
    /// Stable machine-readable code, for example `"TASK_DONE"`.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Optional structured details.
    pub details: Option<Value>,
}

impl Failure {
    /// Build a failure without details.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Failure {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    /// Build a failure with structured details.
    pub fn with_details(
        code: impl Into<String>,
        message: impl Into<String>,
        details: Value,
    ) -> Self {
        Failure {
            code: code.into(),
            message: message.into(),
            details: Some(details),
        }
    }
}

/// The low-level guest result.
pub type Result<T> = core::result::Result<T, Failure>;

/// Create a structured application failure.
pub fn fail(code: impl Into<String>, message: impl Into<String>) -> Failure {
    Failure::new(code, message)
}

/// Create a structured application failure with details.
pub fn fail_with(code: impl Into<String>, message: impl Into<String>, details: Value) -> Failure {
    Failure::with_details(code, message, details)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn fail_builds_plain_failure() {
        let failure = fail("TASK_DONE", "The task is already done");
        assert_eq!(failure.code, "TASK_DONE");
        assert_eq!(failure.message, "The task is already done");
        assert_eq!(failure.details, None);
    }

    #[test]
    fn fail_with_attaches_details() {
        let details = Value::String(String::from("why"));
        let failure = fail_with("REJECTED", "Update rejected", details.clone());
        assert_eq!(failure.code, "REJECTED");
        assert_eq!(failure.details, Some(details));
    }
}
