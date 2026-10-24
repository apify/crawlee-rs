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
            if cause.is::<NonRetryableError>() {
                return ErrorKind::NonRetryable;
            }
            if cause.is::<RetryRequestError>() {
                return ErrorKind::RetryRequest;
            }
            if cause.is::<CriticalError>() || cause.is::<MissingRouteError>() {
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
}
