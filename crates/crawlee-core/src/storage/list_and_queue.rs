//! [`RequestListAndQueue`]: a [`RequestList`] and a request queue read as one request manager,
//! each keeping its own requests.
//!
//! In a [`RequestManagerTandem`](super::request_loader::RequestManagerTandem), as in JS, every
//! request of the list is added to the queue when it is fetched, then fetched from the queue and
//! marked as handled there: two queue writes per request, which a list of inputs does not need.
//! Here the list serves and tracks its requests itself:
//!
//! - **Its state is saved as JS saves it**, under the list's `persist_state_key`: the position of
//!   the next request and the requests in progress, by index into the list, plus the retry counts
//!   of the requests in progress that failed before. Handled requests and those not fetched yet
//!   are not stored.
//! - **A failed request stays in the list** and is served again, unless a handler changed its data
//!   (user data, headers, ...): that one is added to the front of the queue, which keeps a request
//!   whole, and the queue serves it from then on.
//! - **Retries are served next**, before the rest of the list, from the list or from the queue.
//!   In a tandem, a retry goes to the back of the queue, behind every list request not fetched
//!   yet: in a large list, retries would pile up until its end.
//! - **A request added during the run whose unique key is in the list is not added**: it is
//!   reported as already present (and handled if the list handled it).
//! - **Otherwise the list is served first**, then the queue, as the tandem does by adding each
//!   list request to the front of the queue.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;

use super::backend::{BatchAddRequestsResult, ProcessedRequest, QueueOperationInfo};
use super::batched_adds::{AddRequestsBatchedOptions, AddRequestsBatchedResult};
use super::request_list::{Reclaimed, RequestList};
use super::request_loader::{LoaderStatus, PacingSignal, RequestLoader, RequestSourceStatus};
use super::request_queue::RequestManager;
use crate::errors::StorageResult;
use crate::request::{Request, unique_key_to_request_id};

/// A [`RequestList`] in front of a request queue, each keeping its own requests; see the
/// [module documentation](self). Made with [`RequestList::with_queue`].
pub struct RequestListAndQueue {
    list: RequestList,
    queue: Arc<dyn RequestManager>,
    /// The list's requests handed over to the queue, which count there.
    moved: AtomicU64,
    /// Those not fetched from the queue yet: the queue goes first while there are any.
    moved_waiting: Mutex<HashSet<String>>,
}

impl RequestListAndQueue {
    pub(crate) fn new(list: RequestList, queue: Arc<dyn RequestManager>) -> Self {
        RequestListAndQueue { list, queue, moved: AtomicU64::new(0), moved_waiting: Mutex::default() }
    }

    pub fn list(&self) -> &RequestList {
        &self.list
    }

    pub fn queue(&self) -> &Arc<dyn RequestManager> {
        &self.queue
    }

    /// Splits off the requests the list has: reported as already present.
    fn without_list_requests(&self, requests: Vec<Request>) -> (Vec<Request>, Vec<ProcessedRequest>) {
        let (in_list, rest): (Vec<_>, Vec<_>) =
            requests.into_iter().partition(|request| self.list.contains(&request.unique_key));
        let present = in_list
            .into_iter()
            .map(|request| ProcessedRequest {
                request_id: unique_key_to_request_id(&request.unique_key),
                was_already_present: true,
                was_already_handled: self.list.is_handled(&request.unique_key),
                unique_key: request.unique_key,
            })
            .collect();
        (rest, present)
    }
}

#[async_trait]
impl RequestManager for RequestListAndQueue {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult> {
        let (requests, present) = self.without_list_requests(requests);
        let mut result = if requests.is_empty() {
            BatchAddRequestsResult::default()
        } else {
            self.queue.add_requests(requests, forefront).await?
        };
        result.processed_requests.extend(present);
        Ok(result)
    }

    async fn add_requests_batched(
        &self,
        requests: Vec<Request>,
        options: AddRequestsBatchedOptions,
    ) -> StorageResult<AddRequestsBatchedResult> {
        let (requests, present) = self.without_list_requests(requests);
        let mut result = self.queue.add_requests_batched(requests, options).await?;
        result.added_requests.extend(present);
        Ok(result)
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        // A retry handed over to the queue is at its front.
        if !self.moved_waiting.lock().is_empty()
            && let Some(request) = self.queue.fetch_next_request().await?
        {
            self.moved_waiting.lock().remove(&request.unique_key);
            return Ok(Some(request));
        }
        if self.list.status().await? == LoaderStatus::Ready
            && let Some(request) = self.list.fetch_next_request().await?
        {
            return Ok(Some(request));
        }
        let request = self.queue.fetch_next_request().await?;
        if let Some(request) = &request {
            self.moved_waiting.lock().remove(&request.unique_key);
        }
        Ok(request)
    }

    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>> {
        if !self.list.owns(&request.unique_key) {
            return self.queue.mark_request_as_handled(request).await;
        }
        self.list.mark_request_as_handled(request).await?;
        Ok(Some(QueueOperationInfo {
            request_id: unique_key_to_request_id(&request.unique_key),
            was_already_present: true,
            was_already_handled: false,
        }))
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        match self.list.reclaim(request, forefront) {
            Reclaimed::NotOwned => self.queue.reclaim_request(request, forefront).await,
            Reclaimed::Kept => Ok(Some(QueueOperationInfo {
                request_id: unique_key_to_request_id(&request.unique_key),
                was_already_present: true,
                was_already_handled: false,
            })),
            Reclaimed::Changed => {
                // To the front, so that it is retried next, as the list's retries are.
                let added = self.queue.add_requests(vec![request.clone()], true).await;
                let added_ok = matches!(&added, Ok(result) if result.unprocessed_requests.is_empty());
                self.list.finish_move(request, added_ok);
                if added_ok {
                    self.moved.fetch_add(1, Ordering::Relaxed);
                    self.moved_waiting.lock().insert(request.unique_key.clone());
                }
                let result = added?;
                Ok(result.processed_requests.into_iter().next().map(|processed| QueueOperationInfo {
                    request_id: processed.request_id,
                    was_already_present: processed.was_already_present,
                    was_already_handled: processed.was_already_handled,
                }))
            }
        }
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        Ok(self.list.status().await? != LoaderStatus::Ready && self.queue.is_empty().await?)
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        Ok(self.list.status().await? == LoaderStatus::Finished && self.queue.is_finished().await?)
    }

    async fn check_readiness(&self) -> StorageResult<RequestSourceStatus> {
        let list = RequestSourceStatus::from(self.list.status().await?);
        Ok(list.join(self.queue.check_readiness().await?))
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        let list = self.list.handled_count().await?.saturating_sub(self.moved.load(Ordering::Relaxed));
        Ok(list + self.queue.handled_count().await?)
    }

    async fn total_count(&self) -> StorageResult<u64> {
        let list = self.list.total_count().await?.saturating_sub(self.moved.load(Ordering::Relaxed));
        Ok(list + self.queue.total_count().await?)
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        Ok(self.list.pending_count().await? + self.queue.pending_count().await?)
    }

    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        self.queue.set_expected_request_processing_time(duration).await
    }

    async fn extend_request_processing_time(&self, request: &Request, by: Duration) -> StorageResult<bool> {
        if self.list.owns(&request.unique_key) {
            // Nothing else can take a list request in progress.
            return Ok(true);
        }
        self.queue.extend_request_processing_time(request, by).await
    }

    fn record_pacing_signal(&self, signal: &PacingSignal) -> bool {
        self.queue.record_pacing_signal(signal)
    }

    fn ready_at(&self) -> Option<Instant> {
        self.queue.ready_at()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Value, json};

    use super::*;
    use crate::storage::request_list::RequestListOptions;
    use crate::{Request, RequestQueue, Services, StorageIdentifier};

    fn url(path: &str) -> String {
        format!("https://example.com/{path}")
    }

    async fn open(services: &Services, paths: &[&str]) -> (RequestListAndQueue, Arc<RequestQueue>) {
        let list = RequestList::open(services, None, paths.iter().map(|path| url(path))).await.unwrap();
        let queue = Arc::new(services.open_request_queue(&StorageIdentifier::alias("queue")).await.unwrap());
        (list.with_queue(Some(queue.clone())).await.unwrap(), queue)
    }

    #[tokio::test]
    async fn serves_the_list_first_and_keeps_its_requests_out_of_the_queue() {
        let services = Services::in_memory();
        let (manager, queue) = open(&services, &["1", "2"]).await;
        manager.add_requests(vec![url("queued").into()], false).await.unwrap();

        let mut urls = Vec::new();
        while let Some(mut request) = manager.fetch_next_request().await.unwrap() {
            urls.push(request.url.clone());
            manager.mark_request_as_handled(&mut request).await.unwrap();
        }
        assert_eq!(urls, [url("1"), url("2"), url("queued")]);
        assert!(manager.is_finished().await.unwrap());
        assert_eq!(queue.total_count().await.unwrap(), 1, "only the request added during the run");
        assert_eq!(manager.handled_count().await.unwrap(), 3);
        assert_eq!(manager.total_count().await.unwrap(), 3);
        assert_eq!(manager.pending_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn requests_added_with_the_unique_key_of_a_list_request_are_dropped() {
        let services = Services::in_memory();
        let (manager, queue) = open(&services, &["1", "2"]).await;
        let mut first = manager.fetch_next_request().await.unwrap().unwrap();
        manager.mark_request_as_handled(&mut first).await.unwrap();

        let added =
            manager.add_requests(vec![url("1").into(), url("2").into(), url("new").into()], false).await.unwrap();
        let mut present: Vec<_> = added
            .processed_requests
            .iter()
            .map(|processed| {
                (processed.unique_key.as_str(), processed.was_already_present, processed.was_already_handled)
            })
            .collect();
        present.sort();
        assert_eq!(
            present,
            [(url("1").as_str(), true, true), (url("2").as_str(), true, false), (url("new").as_str(), false, false)]
        );

        let batched =
            manager.add_requests_batched(vec![url("2").into(), url("other").into()], Default::default()).await.unwrap();
        assert_eq!(batched.added_requests.len(), 2);
        if let Some(rest) = batched.wait_for_all_requests_to_be_added {
            rest.await.unwrap().unwrap();
        }
        assert_eq!(queue.total_count().await.unwrap(), 2, "new and other");
    }

    #[tokio::test]
    async fn an_unchanged_retry_stays_in_the_list_with_its_retry_count() {
        let services = Services::in_memory();
        let (manager, queue) = open(&services, &["1", "2"]).await;
        let mut request = manager.fetch_next_request().await.unwrap().unwrap();
        request.retry_count = 1;
        request.error_messages.push("blocked".into());
        manager.reclaim_request(&request, false).await.unwrap();
        assert_eq!(manager.list.get_state().retries, [(url("1"), 1)].into_iter().collect());

        // Retried next, before the rest of the list, so that retries never pile up.
        let mut retried = manager.fetch_next_request().await.unwrap().unwrap();
        assert_eq!((retried.url.as_str(), retried.retry_count), (url("1").as_str(), 1));
        assert_eq!(retried.error_messages, ["blocked"]);
        // A second reclaim of a request already waiting does nothing.
        manager.reclaim_request(&retried, false).await.unwrap();
        manager.reclaim_request(&retried, true).await.unwrap();
        retried = manager.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(retried.url, url("1"));
        let second = manager.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(second.url, url("2"));
        assert!(manager.fetch_next_request().await.unwrap().is_none());

        manager.mark_request_as_handled(&mut retried).await.unwrap();
        assert!(manager.list.get_state().retries.is_empty());
        assert_eq!(queue.total_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_retry_with_changed_data_moves_to_the_queue() {
        let services = Services::in_memory();
        let (manager, queue) = open(&services, &["1", "2"]).await;
        manager.add_requests(vec![url("queued").into()], false).await.unwrap();
        let mut request = manager.fetch_next_request().await.unwrap().unwrap();
        request.retry_count = 1;
        request.user_data.insert("attempts".into(), json!(1));
        manager.reclaim_request(&request, false).await.unwrap();
        assert_eq!(queue.total_count().await.unwrap(), 2);
        assert_eq!(manager.list.get_state().in_progress, Vec::<String>::new(), "handed over");

        // Retried next too, ahead of the rest of the list and of the queue.
        let mut moved = manager.fetch_next_request().await.unwrap().unwrap();
        assert_eq!((moved.url.as_str(), moved.retry_count), (url("1").as_str(), 1));
        assert_eq!(moved.user_data.get("attempts"), Some(&json!(1)));
        manager.mark_request_as_handled(&mut moved).await.unwrap();
        assert_eq!(queue.handled_count().await.unwrap(), 1, "marked in the queue that has it");
        let mut second = manager.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(second.url, url("2"));
        manager.mark_request_as_handled(&mut second).await.unwrap();
        let mut queued = manager.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(queued.url, url("queued"));
        manager.mark_request_as_handled(&mut queued).await.unwrap();
        assert!(manager.is_finished().await.unwrap());
        assert_eq!(manager.handled_count().await.unwrap(), 3, "the moved request counts once");
        assert_eq!(manager.total_count().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn retry_counts_survive_a_restart() {
        let services = Services::in_memory();
        let options = || RequestListOptions {
            sources: ["a", "b", "c"].iter().map(|path| url(path).into()).collect(),
            persist_state_key: Some("LIST".into()),
            persist_state_interval: Some(Duration::from_secs(5)),
            ..RequestListOptions::default()
        };
        let list = RequestList::open_with_options(&services, options()).await.unwrap();
        let manager = list.with_queue(None).await.unwrap();
        let mut a = manager.fetch_next_request().await.unwrap().unwrap();
        let _b = manager.fetch_next_request().await.unwrap().unwrap();
        a.retry_count = 2;
        manager.reclaim_request(&a, false).await.unwrap();
        list.persist_state().await.unwrap();

        // Handled and untouched requests are not stored, as in JS: only the position, the requests
        // in progress, and the retry counts.
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        let saved: Value = store.get_value("CRAWLEE_LIST").await.unwrap().unwrap();
        assert_eq!(
            saved,
            json!({
                "nextIndex": 2,
                "nextUniqueKey": url("c"),
                "inProgress": [url("a"), url("b")],
                "retries": { url("a"): 2 },
            })
        );
        list.teardown().await;

        let restored = RequestList::open_with_options(&services, options()).await.unwrap();
        let fetched: Vec<Request> = [
            restored.fetch_next_request().await.unwrap().unwrap(),
            restored.fetch_next_request().await.unwrap().unwrap(),
            restored.fetch_next_request().await.unwrap().unwrap(),
        ]
        .into();
        let fetched: Vec<_> = fetched.iter().map(|request| (request.url.clone(), request.retry_count)).collect();
        assert_eq!(fetched, [(url("a"), 2), (url("b"), 0), (url("c"), 0)]);
        restored.teardown().await;
    }
}
