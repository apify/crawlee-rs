//! Read-only request sources ([`RequestLoader`]), the [`RequestManagerTandem`] that feeds one into
//! a request queue, and the [`PacingSignal`]s a crawler sends its request manager.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use super::backend::{BatchAddRequestsResult, QueueOperationInfo};
use super::batched_adds::{AddRequestsBatchedOptions, AddRequestsBatchedResult};
use super::request_queue::RequestManager;
use crate::errors::StorageResult;
use crate::request::Request;

/// Which requests a pacing signal covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PacingScope {
    Hostname,
    RegistrableDomain,
}

/// Something a crawler learned about how fast it may hit a site, offered to its request manager
/// with [`RequestManager::record_pacing_signal`]. A manager that paces requests (such as a
/// `ThrottlingRequestManager`) takes it; a plain queue ignores it.
#[derive(Clone, Debug, PartialEq)]
pub enum PacingSignal {
    /// The site answered HTTP 429, possibly with a `Retry-After`.
    RateLimited { url: String, wait: Option<Duration> },
    /// The site asks for at least `interval` between requests (robots.txt `Crawl-delay`).
    MinInterval { url: String, interval: Duration, scope: Option<PacingScope> },
    /// At least `interval` between requests to every site (`same_domain_delay`).
    MinIntervalEverywhere { interval: Duration, scope: Option<PacingScope> },
}

/// Whether a request source has a request now, will have one later, never will, or is done: the
/// answer of [`RequestManager::check_readiness`] (`RequestSourceStatus` of `checkReadiness`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestSourceStatus {
    /// The next fetch is expected to hand a request over.
    Ready,
    /// Nothing to fetch now, but the source is not done: requests are in progress, are being
    /// added in the background, or are held back until `ready_at` (when known).
    Waiting { ready_at: Option<Instant> },
    /// The source holds requests it cannot make progress on. Only a manager that paces its own
    /// requests gets there, such as a `ThrottlingRequestManager` whose domain keeps answering 429.
    Stalled { reason: String },
    /// Everything was handled.
    Finished,
}

impl RequestSourceStatus {
    /// The status of two sources read as one (`joinRequestSourceStatuses`): `Ready` first, then
    /// `Stalled`, `Waiting` and `Finished`. Of two waiting sources, the earlier known wake-up
    /// wins.
    pub fn join(self, other: RequestSourceStatus) -> RequestSourceStatus {
        use RequestSourceStatus::*;
        match (self, other) {
            (Ready, _) | (_, Ready) => Ready,
            // A source with work masks a stalled one: the stall only matters once nothing else
            // can make progress.
            (stalled @ Stalled { .. }, _) | (_, stalled @ Stalled { .. }) => stalled,
            (Waiting { ready_at: a }, Waiting { ready_at: b }) => Waiting {
                ready_at: match (a, b) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                },
            },
            (waiting @ Waiting { .. }, Finished) | (Finished, waiting @ Waiting { .. }) => waiting,
            (Finished, Finished) => Finished,
        }
    }
}

impl From<LoaderStatus> for RequestSourceStatus {
    fn from(status: LoaderStatus) -> Self {
        match status {
            LoaderStatus::Ready => RequestSourceStatus::Ready,
            LoaderStatus::Waiting => RequestSourceStatus::Waiting { ready_at: None },
            LoaderStatus::Finished => RequestSourceStatus::Finished,
        }
    }
}

/// Whether a [`RequestLoader`] has a request now, may have one later, or is done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoaderStatus {
    Ready,
    Waiting,
    Finished,
}

/// A read-only source of requests, like a sitemap (`IRequestLoader` in JS). Crawlers use it
/// through a [`RequestManagerTandem`], which moves its requests into a queue so they can be
/// retried.
#[async_trait]
pub trait RequestLoader: Send + Sync {
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>>;
    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<()>;
    async fn status(&self) -> StorageResult<LoaderStatus>;
    async fn handled_count(&self) -> StorageResult<u64>;
    /// Requests not handled yet (buffered or in progress).
    async fn pending_count(&self) -> StorageResult<u64>;
    /// Requests handled or not (`getTotalCount`).
    async fn total_count(&self) -> StorageResult<u64> {
        Ok(self.handled_count().await? + self.pending_count().await?)
    }
    /// Saves the loader's state, if it keeps one (see [`RequestManager::persist_state`]).
    async fn persist_state(&self) -> StorageResult<()> {
        Ok(())
    }
}

/// A [`RequestLoader`] in front of a [`RequestManager`]: each fetch first moves one request from
/// the loader to the front of the manager, then fetches from the manager. Retries, new links and
/// persistence are therefore the manager's business, as in JS.
pub struct RequestManagerTandem {
    loader: Arc<dyn RequestLoader>,
    manager: Arc<dyn RequestManager>,
}

impl RequestManagerTandem {
    pub fn new(loader: Arc<dyn RequestLoader>, manager: Arc<dyn RequestManager>) -> Self {
        RequestManagerTandem { loader, manager }
    }

    /// Moves one request from the loader to the front of the manager. A request the manager
    /// refuses is dropped, and `false` returned: nothing is fetched from the manager then.
    async fn transfer_next_request(&self) -> StorageResult<bool> {
        let Some(request) = self.loader.fetch_next_request().await? else {
            return Ok(true);
        };
        let added = self.manager.add_requests(vec![request.clone()], true).await;
        // Handled either way, so that it does not stay in progress in the loader.
        self.loader.mark_request_as_handled(&request).await?;
        if let Err(err) = added {
            tracing::error!(
                url = %request.url,
                unique_key = %request.unique_key,
                "Adding request from the RequestLoader to the RequestManager failed, the request has been dropped. {err}"
            );
            return Ok(false);
        }
        Ok(true)
    }
}

#[async_trait]
impl RequestManager for RequestManagerTandem {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult> {
        self.manager.add_requests(requests, forefront).await
    }

    async fn add_requests_batched(
        &self,
        requests: Vec<Request>,
        options: AddRequestsBatchedOptions,
    ) -> StorageResult<AddRequestsBatchedResult> {
        self.manager.add_requests_batched(requests, options).await
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        // Only the loader's state decides: a manager waiting out a backoff must not hold back
        // the loader's requests.
        if self.loader.status().await? == LoaderStatus::Ready && !self.transfer_next_request().await? {
            return Ok(None);
        }
        self.manager.fetch_next_request().await
    }

    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>> {
        self.manager.mark_request_as_handled(request).await
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        self.manager.reclaim_request(request, forefront).await
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        Ok(self.loader.status().await? != LoaderStatus::Ready && self.manager.is_empty().await?)
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        Ok(self.loader.status().await? == LoaderStatus::Finished && self.manager.is_finished().await?)
    }

    /// The loader and the manager read as one source.
    async fn check_readiness(&self) -> StorageResult<RequestSourceStatus> {
        let loader = RequestSourceStatus::from(self.loader.status().await?);
        Ok(loader.join(self.manager.check_readiness().await?))
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        self.manager.handled_count().await
    }

    async fn total_count(&self) -> StorageResult<u64> {
        Ok(self.manager.total_count().await? + self.loader.pending_count().await?)
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        Ok(self.manager.pending_count().await? + self.loader.pending_count().await?)
    }

    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        self.manager.set_expected_request_processing_time(duration).await
    }

    async fn extend_request_processing_time(&self, request: &Request, by: Duration) -> StorageResult<bool> {
        self.manager.extend_request_processing_time(request, by).await
    }

    async fn persist_state(&self) -> StorageResult<()> {
        let (loader, manager) = tokio::join!(self.loader.persist_state(), self.manager.persist_state());
        loader.and(manager)
    }

    fn record_pacing_signal(&self, signal: &PacingSignal) -> bool {
        self.manager.record_pacing_signal(signal)
    }

    fn ready_at(&self) -> Option<std::time::Instant> {
        self.manager.ready_at()
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;

    use super::*;
    use crate::storage::memory::MemoryStorageBackend;
    use crate::{RequestQueue, StorageIdentifier};

    struct ListLoader {
        pending: Mutex<Vec<Request>>,
        handled: Mutex<u64>,
    }

    #[async_trait]
    impl RequestLoader for ListLoader {
        async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
            Ok(self.pending.lock().pop())
        }
        async fn mark_request_as_handled(&self, _: &Request) -> StorageResult<()> {
            *self.handled.lock() += 1;
            Ok(())
        }
        async fn status(&self) -> StorageResult<LoaderStatus> {
            Ok(if self.pending.lock().is_empty() { LoaderStatus::Finished } else { LoaderStatus::Ready })
        }
        async fn handled_count(&self) -> StorageResult<u64> {
            Ok(*self.handled.lock())
        }
        async fn pending_count(&self) -> StorageResult<u64> {
            Ok(self.pending.lock().len() as u64)
        }
    }

    #[tokio::test]
    async fn moves_loader_requests_into_the_queue() {
        let backend = MemoryStorageBackend::new();
        let queue = Arc::new(RequestQueue::open(&backend, &StorageIdentifier::Default).await.unwrap());
        queue.add_requests(vec!["https://a.test/queued".into()], false).await.unwrap();
        let loader = Arc::new(ListLoader {
            pending: Mutex::new(vec!["https://a.test/2".into(), "https://a.test/1".into()]),
            handled: Mutex::new(0),
        });
        let tandem = RequestManagerTandem::new(loader.clone(), queue.clone());

        let mut urls = Vec::new();
        while let Some(mut request) = tandem.fetch_next_request().await.unwrap() {
            urls.push(request.url.clone());
            tandem.mark_request_as_handled(&mut request).await.unwrap();
        }
        // Loader requests go to the front of the queue.
        assert_eq!(urls, ["https://a.test/1", "https://a.test/2", "https://a.test/queued"]);
        assert!(tandem.is_finished().await.unwrap());
        assert_eq!(loader.handled_count().await.unwrap(), 2);
        assert_eq!(tandem.handled_count().await.unwrap(), 3);
    }

    /// A manager that refuses every add, and counts the fetches.
    #[derive(Default)]
    struct RefusingManager {
        fetches: Mutex<u32>,
    }

    #[async_trait]
    impl RequestManager for RefusingManager {
        async fn add_requests(&self, _: Vec<Request>, _: bool) -> StorageResult<BatchAddRequestsResult> {
            Err(crate::StorageError::Backend("Add failed".into()))
        }
        async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
            *self.fetches.lock() += 1;
            Ok(None)
        }
        async fn mark_request_as_handled(&self, _: &mut Request) -> StorageResult<Option<QueueOperationInfo>> {
            Ok(None)
        }
        async fn reclaim_request(&self, _: &Request, _: bool) -> StorageResult<Option<QueueOperationInfo>> {
            Ok(None)
        }
        async fn is_empty(&self) -> StorageResult<bool> {
            Ok(true)
        }
        async fn is_finished(&self) -> StorageResult<bool> {
            Ok(true)
        }
        async fn handled_count(&self) -> StorageResult<u64> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn a_request_the_manager_refuses_is_dropped() {
        let loader = Arc::new(ListLoader {
            pending: Mutex::new(vec!["https://a.test/2".into(), "https://a.test/1".into()]),
            handled: Mutex::new(0),
        });
        let manager = Arc::new(RefusingManager::default());
        let tandem = RequestManagerTandem::new(loader.clone(), manager.clone());

        // Marked as handled on the loader, and the manager is not fetched from this round.
        assert!(tandem.fetch_next_request().await.unwrap().is_none());
        assert_eq!(loader.handled_count().await.unwrap(), 1);
        assert_eq!(*manager.fetches.lock(), 0);
    }

    #[test]
    fn statuses_join_by_precedence() {
        use RequestSourceStatus::*;
        let soon = Instant::now();
        let later = soon + Duration::from_secs(1);
        let stalled = || Stalled { reason: "x".into() };
        assert_eq!(stalled().join(Ready), Ready);
        assert_eq!(Waiting { ready_at: None }.join(stalled()), stalled());
        assert_eq!(Finished.join(Waiting { ready_at: Some(later) }), Waiting { ready_at: Some(later) });
        assert_eq!(
            Waiting { ready_at: Some(later) }.join(Waiting { ready_at: Some(soon) }),
            Waiting { ready_at: Some(soon) }
        );
        assert_eq!(Waiting { ready_at: None }.join(Waiting { ready_at: Some(soon) }), Waiting { ready_at: Some(soon) });
        assert_eq!(Finished.join(Finished), Finished);
    }
}
