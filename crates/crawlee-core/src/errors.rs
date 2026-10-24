//! Errors with a special meaning for the crawler, and storage errors.
//!
//! Request handlers return `anyhow::Result<()>`. To steer the retry logic, return (or wrap) one of
//! the marker errors below; the crawler finds them anywhere in the error's source chain:
//!
//! ```
//! use crawlee_core::errors::NonRetryableError;
//!
//! fn check(status: u16) -> anyhow::Result<()> {
//!     if status == 410 {
//!         return Err(NonRetryableError::new("the page is gone").into());
//!     }
//!     Ok(())
//! }
//! ```

macro_rules! marker_error {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(message: impl Into<String>) -> Self {
                Self(message.into())
            }

            pub fn message(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl std::error::Error for $name {}
    };
}

marker_error!(
    /// The session (and thus its proxy and cookies) got blocked. The session is retired and the
    /// request is retried with another one.
    SessionError
);
marker_error!(
    /// The request must not be retried.
    NonRetryableError
);
marker_error!(
    /// The request should be retried even if it has no retries left.
    RetryRequestError
);
marker_error!(
    /// Stops the whole crawl.
    CriticalError
);
marker_error!(
    /// The target asked us to slow down. The request goes back to the queue without costing a
    /// retry or the session's reputation.
    RequestThrottledError
);

/// Errors produced by storages and storage backends.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{0}")]
    InvalidArgument(String),
    #[error("storage not found: {0}")]
    NotFound(String),
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("storage backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    StateValidation(#[from] StateValidationError),
}

/// A state saved by a [`RecoverableState`](crate::RecoverableState) could not be restored
/// (`StateValidationError` in Crawlee for JS).
#[derive(Debug, thiserror::Error)]
#[error("State persisted under key '{persist_state_key}' failed validation: {source}")]
pub struct StateValidationError {
    pub persist_state_key: String,
    #[source]
    pub source: Box<dyn std::error::Error + Send + Sync>,
}

pub type StorageResult<T> = Result<T, StorageError>;
