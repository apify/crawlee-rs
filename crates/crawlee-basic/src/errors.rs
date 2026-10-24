//! How the crawler classifies handler errors.

use crawlee_core::errors::{CriticalError, NonRetryableError, RequestThrottledError, RetryRequestError, SessionError};

/// No route matches the request's label and there is no default route. Like in Crawlee for JS,
/// this is a critical error: it stops the crawl, because every such request would fail.
#[derive(Debug, thiserror::Error)]
#[error("Route not found for label '{label}'. You must set up a route for this label or a default route.")]
pub struct MissingRouteError {
    pub label: String,
}

/// The request was skipped by the context pipeline (for example after a redirect outside of its
/// enqueue strategy). It is marked as handled without running the request handler.
#[derive(Debug, thiserror::Error)]
#[error("request skipped: {reason}")]
pub struct RequestSkipped {
    pub reason: String,
}

/// Returned by [`BasicContext::skip_request`](crate::BasicContext::skip_request): the request is
/// settled as skipped rather than failed (`ContextPipelineInterruptedError('manual')`).
#[derive(Debug, thiserror::Error)]
#[error("{}", .message.as_deref().unwrap_or("Request was skipped"))]
pub struct SkipRequestError {
    pub message: Option<String>,
}

/// A request is pinned to a session (`session_id`) that the session pool does not have, or that is
/// no longer usable.
#[derive(Debug, thiserror::Error)]
#[error("The current SessionPool instance couldn't find a valid session for the following id: {session_id}.")]
pub struct MissingSessionError {
    pub session_id: String,
}

/// A member of the context that needs the response was used on a request with `skip_navigation`
/// (`NavigationSkippedError`, a `NonRetryableError` in JS): the request fails without retries.
///
/// Fallible members return it. Infallible ones (such as the status of an HTTP response) panic
/// with its message instead, as the getters of Crawlee for JS throw; the crawler turns that panic
/// back into this error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("The `{member}` method is not available - `skip_navigation` was used")]
pub struct NavigationSkippedError {
    pub member: String,
}

impl NavigationSkippedError {
    pub fn new(member: impl Into<String>) -> Self {
        NavigationSkippedError { member: member.into() }
    }

    /// The error whose message a panic carries, if it is one.
    pub(crate) fn from_message(message: &str) -> Option<Self> {
        let member =
            message.strip_prefix("The `")?.strip_suffix("` method is not available - `skip_navigation` was used")?;
        Some(Self::new(member))
    }
}

/// A domain kept rate-limiting every request for longer than the request manager's
/// `max_domain_stall`, and nothing else can be crawled: the crawl gives up rather than wait
/// forever (`PersistentRateLimitError`, a `CriticalError` in JS). The domain's requests stay
/// queued, so a later run can pick them up.
#[derive(Debug, thiserror::Error)]
#[error("Giving up: {reason}")]
pub struct PersistentRateLimitError {
    pub reason: String,
}

/// The request handler did not finish in time.
#[derive(Debug, thiserror::Error)]
#[error("requestHandler timed out after {secs} seconds.")]
pub struct RequestHandlerTimeout {
    pub secs: f64,
}

/// The request handler panicked. The panic is caught and handled like an error.
#[derive(Debug, thiserror::Error)]
#[error("request handler panicked: {message}")]
pub struct HandlerPanic {
    pub message: String,
}

/// What an error means for the retry logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Session,
    NonRetryable,
    RetryRequest,
    Critical,
    Throttled,
    Skipped,
    Other,
}

impl ErrorKind {
    /// Finds the first marker error anywhere in the source chain.
    pub fn of(error: &anyhow::Error) -> ErrorKind {
        for cause in error.chain() {
            if cause.is::<SessionError>() {
                return ErrorKind::Session;
            }
            if cause.is::<NonRetryableError>() || cause.is::<NavigationSkippedError>() {
                return ErrorKind::NonRetryable;
            }
            if cause.is::<RetryRequestError>() {
                return ErrorKind::RetryRequest;
            }
            if cause.is::<CriticalError>() || cause.is::<MissingRouteError>() || cause.is::<PersistentRateLimitError>()
            {
                return ErrorKind::Critical;
            }
            if cause.is::<RequestThrottledError>() {
                return ErrorKind::Throttled;
            }
            if cause.is::<RequestSkipped>() || cause.is::<SkipRequestError>() {
                return ErrorKind::Skipped;
            }
        }
        ErrorKind::Other
    }

    /// Errors that say nothing bad about the session: it was already retired, or the failure is a
    /// property of the domain.
    pub fn absolves_session(self) -> bool {
        matches!(self, ErrorKind::Session | ErrorKind::Throttled | ErrorKind::Skipped)
    }
}

/// Renders an error with its whole cause chain on one line.
pub fn error_message(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context as _;

    #[test]
    fn finds_markers_in_the_chain() {
        let err: anyhow::Result<()> = Err(SessionError::new("blocked").into());
        let wrapped = err.context("while fetching").unwrap_err();
        assert_eq!(ErrorKind::of(&wrapped), ErrorKind::Session);
        assert_eq!(ErrorKind::of(&anyhow::anyhow!("plain")), ErrorKind::Other);
        assert_eq!(ErrorKind::of(&MissingRouteError { label: "X".into() }.into()), ErrorKind::Critical);
        assert_eq!(error_message(&wrapped), "while fetching: blocked");
    }

    #[test]
    fn a_skipped_navigation_is_not_retried() {
        let error = NavigationSkippedError::new("status");
        assert_eq!(ErrorKind::of(&error.clone().into()), ErrorKind::NonRetryable);
        assert_eq!(NavigationSkippedError::from_message(&error.to_string()), Some(error));
        assert_eq!(NavigationSkippedError::from_message("The `status` method"), None);
    }
}
