//! Read-only request sources ([`RequestLoader`]), the [`RequestManagerTandem`] that feeds one into
//! a request queue, and the [`PacingSignal`]s a crawler sends its request manager.

use std::rc::Rc;
use std::time::Duration;

use async_trait::async_trait;

use super::backend::{BatchAddRequestsResult, QueueOperationInfo};
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
#[async_trait(?Send)]
pub trait RequestLoader {
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>>;
    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<()>;
    async fn status(&self) -> StorageResult<LoaderStatus>;
    async fn handled_count(&self) -> StorageResult<u64>;
    /// Requests not handled yet (buffered or in progress).
    async fn pending_count(&self) -> StorageResult<u64>;
}

/// A [`RequestLoader`] in front of a [`RequestManager`]: each fetch first moves one request from
/// the loader to the front of the manager, then fetches from the manager. Retries, new links and
/// persistence are therefore the manager's business, as in JS.
pub struct RequestManagerTandem {
    loader: Rc<dyn RequestLoader>,
    manager: Rc<dyn RequestManager>,
}

impl RequestManagerTandem {
    pub fn new(loader: Rc<dyn RequestLoader>, manager: Rc<dyn RequestManager>) -> Self {
        RequestManagerTandem { loader, manager }
    }

    async fn transfer_next_request(&self) -> StorageResult<()> {
        let Some(request) = self.loader.fetch_next_request().await? else {
            return Ok(());
        };
        let added = self.manager.add_requests(vec![request.clone()], true).await;
        self.loader.mark_request_as_handled(&request).await?;
        if let Err(err) = added {
            tracing::error!(url = %request.url, "Adding a request from the loader to the queue failed; dropped: {err}");
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl RequestManager for RequestManagerTandem {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult> {
        self.manager.add_requests(requests, forefront).await
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        if self.loader.status().await? == LoaderStatus::Ready {
            self.transfer_next_request().await?;
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

    async fn handled_count(&self) -> StorageResult<u64> {
        self.manager.handled_count().await
    }

    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        self.manager.set_expected_request_processing_time(duration).await
    }

    fn record_pacing_signal(&self, signal: &PacingSignal) -> bool {
        self.manager.record_pacing_signal(signal)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::storage::memory::MemoryStorageBackend;
    use crate::{RequestQueue, StorageIdentifier};

    struct ListLoader {
        pending: RefCell<Vec<Request>>,
        handled: RefCell<u64>,
    }

    #[async_trait(?Send)]
    impl RequestLoader for ListLoader {
        async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
            Ok(self.pending.borrow_mut().pop())
        }
        async fn mark_request_as_handled(&self, _: &Request) -> StorageResult<()> {
            *self.handled.borrow_mut() += 1;
            Ok(())
        }
        async fn status(&self) -> StorageResult<LoaderStatus> {
            Ok(if self.pending.borrow_mut().is_empty() { LoaderStatus::Finished } else { LoaderStatus::Ready })
        }
        async fn handled_count(&self) -> StorageResult<u64> {
            Ok(*self.handled.borrow_mut())
        }
        async fn pending_count(&self) -> StorageResult<u64> {
            Ok(self.pending.borrow_mut().len() as u64)
        }
    }

    #[tokio::test(flavor = "local")]
    async fn moves_loader_requests_into_the_queue() {
        let backend = MemoryStorageBackend::new();
        let queue = Rc::new(RequestQueue::open(&backend, &StorageIdentifier::Default).await.unwrap());
        queue.add_requests(vec!["https://a.test/queued".into()], false).await.unwrap();
        let loader = Rc::new(ListLoader {
            pending: RefCell::new(vec!["https://a.test/2".into(), "https://a.test/1".into()]),
            handled: RefCell::new(0),
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
}
