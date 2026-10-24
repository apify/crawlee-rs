//! Timeouts that a request handler can push back with
//! [`BasicContext::extend_timeout`](crate::BasicContext::extend_timeout).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

/// The extensions of one request's timeouts. Every [`TimeoutWindow`] open when the timeout is
/// extended ends that much later; windows opened afterwards are not affected.
#[derive(Debug, Default)]
pub struct TimeoutExtensions {
    extended: Mutex<Duration>,
    changed: Notify,
}

impl TimeoutExtensions {
    /// Pushes back every open window by `by`.
    pub fn extend(&self, by: Duration) {
        *self.extended.lock() += by;
        self.changed.notify_waiters();
    }

    fn total(&self) -> Duration {
        *self.extended.lock()
    }

    /// A window ending `timeout` from now, plus the extensions made while it is open.
    pub fn window(self: &Arc<Self>, timeout: Duration) -> TimeoutWindow {
        TimeoutWindow { deadline: Instant::now() + timeout, extended_at_start: self.total(), extensions: self.clone() }
    }
}

/// A timeout that [`TimeoutExtensions::extend`] can push back.
#[derive(Debug, Clone)]
pub struct TimeoutWindow {
    deadline: Instant,
    extended_at_start: Duration,
    extensions: Arc<TimeoutExtensions>,
}

impl TimeoutWindow {
    /// When the window ends, extensions included.
    pub fn deadline(&self) -> Instant {
        self.deadline + self.extensions.total().saturating_sub(self.extended_at_start)
    }

    /// What is left of the window.
    pub fn remaining(&self) -> Duration {
        self.deadline().saturating_duration_since(Instant::now())
    }

    /// Runs `work` until the window ends: its output, or `None` when the window ended first.
    pub async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        tokio::pin!(work);
        loop {
            let changed = self.extensions.changed.notified();
            tokio::pin!(changed);
            // Registered before the deadline is read, so no extension is missed.
            changed.as_mut().enable();
            tokio::select! {
                biased;
                output = &mut work => return Some(output),
                () = tokio::time::sleep_until(self.deadline()) => return None,
                () = &mut changed => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn extensions_reach_the_windows_open_at_the_time() {
        let extensions = Arc::new(TimeoutExtensions::default());
        let outer = extensions.window(Duration::from_secs(10));
        let inner = extensions.window(Duration::from_secs(5));
        let extended = outer.clone();
        let work = async {
            tokio::time::sleep(Duration::from_secs(4)).await;
            extensions.extend(Duration::from_secs(3));
            tokio::time::sleep(Duration::from_secs(4)).await;
        };
        assert_eq!(inner.run(work).await, Some(()), "8 s within 5 s + 3 s");
        assert_eq!(extended.remaining(), Duration::from_secs(5), "13 s - 8 s");

        let later = extensions.window(Duration::from_secs(1));
        assert_eq!(later.run(tokio::time::sleep(Duration::from_secs(2))).await, None, "opened after the extension");
    }
}
