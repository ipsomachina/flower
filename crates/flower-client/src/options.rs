//! Operation-specific options for the scoped API. Delivery settings never contain identity.
use crate::{CancellationToken, Credentials, FlowerError, ReadToken, RequestOptions, Retry};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Default)]
pub struct QueryOptions(pub(crate) RequestOptions);

/// Delivery options for an already prepared command; its identity and precondition are immutable.
/// ```compile_fail
/// # fn example(token: flower_client::ReadToken) {
/// flower_client::CommandOptions::new().at_least(token);
/// # }
/// ```
/// ```compile_fail
/// # async fn example(scope: &flower_client::ModuleClient, command: &flower_client::PreparedCommand<String>) {
/// scope.execute(command, &flower_client::QueryOptions::new()).await;
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct CommandOptions(pub(crate) RequestOptions);

macro_rules! delivery_options {
    ($name:ident) => {
        impl $name {
            pub fn new() -> Self {
                Self::default()
            }
            pub fn retrying() -> Self {
                Self(RequestOptions::retrying())
            }
            pub fn retry(mut self, retry: impl Into<Retry>) -> Self {
                self.0 = self.0.retry(retry);
                self
            }
            pub fn timeout(mut self, timeout: Duration) -> Self {
                self.0 = self.0.timeout(timeout);
                self
            }
            pub fn credentials(mut self, credentials: Credentials) -> Self {
                self.0 = self.0.credentials(credentials);
                self
            }
            pub fn deadline(mut self, deadline: Instant) -> Self {
                self.0 = self.0.deadline(deadline);
                self
            }
            pub fn cancel(mut self, cancel: CancellationToken) -> Self {
                self.0 = self.0.cancel(cancel);
                self
            }
        }
        impl From<$name> for RequestOptions {
            fn from(options: $name) -> Self {
                options.0
            }
        }
    };
}
delivery_options!(QueryOptions);
delivery_options!(CommandOptions);
impl QueryOptions {
    pub fn at_least(mut self, token: ReadToken) -> Self {
        self.0.at_least = Some(token);
        self
    }
    pub fn max_staleness(mut self, staleness: Duration) -> Self {
        self.0.max_staleness = Some(staleness);
        self
    }
}
impl From<RequestOptions> for QueryOptions {
    fn from(options: RequestOptions) -> Self {
        Self(options)
    }
}
impl TryFrom<RequestOptions> for CommandOptions {
    type Error = FlowerError;
    fn try_from(options: RequestOptions) -> Result<Self, Self::Error> {
        require_command_options(&options)?;
        Ok(Self(options))
    }
}
pub(crate) fn require_command_options(options: &RequestOptions) -> Result<(), FlowerError> {
    if options.at_least.is_some() || options.max_staleness.is_some() {
        return Err(FlowerError::Unsupported("query consistency options cannot be applied to commands; persist a revision precondition instead".into()));
    }
    Ok(())
}
