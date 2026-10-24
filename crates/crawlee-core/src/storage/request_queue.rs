//! [`RequestQueue`] and the [`RequestManager`] trait crawlers consume requests through.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use super::backend::{
    BatchAddRequestsResult, ProcessedRequest, QueueOperationInfo, RequestQueueBackend, RequestQueueInfo,
    StorageBackend, StorageIdentifier,
};
use super::batched_adds::{
    AddRequestsBatchedOptions, AddRequestsBatchedResult, InProgressBatches, drain_request_batches,
};
use crate::errors::StorageResult;
use crate::request::{Request, unique_key_to_request_id};
use crate::services::CacheLink;

/// Source and sink of requests for a crawler. [`RequestQueue`] is the default implementation;
/// other request sources (lists, sitemaps, per-domain throttling) implement it as well.
#[async_trait]
pub trait RequestManager: Send + Sync {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult>;
    /// Adds the requests in batches: the first before returning, the rest in the background
    /// (`addRequestsBatched`). A manager that does not batch adds them all at once.
    async fn add_requests_batched(
        &self,
        requests: Vec<Request>,
        options: AddRequestsBatchedOptions,
    ) -> StorageResult<AddRequestsBatchedResult> {
        let added = self.add_requests(requests, options.forefront).await?;
        Ok(AddRequestsBatchedResult {
            added_requests: added.processed_requests,
            wait_for_all_requests_to_be_added: None,
        })
    }
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>>;
    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>>;
    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>>;
    /// No request can be fetched right now.
    async fn is_empty(&self) -> StorageResult<bool>;
    /// All work is done: nothing pending, nothing in progress.
    async fn is_finished(&self) -> StorageResult<bool>;
    async fn handled_count(&self) -> StorageResult<u64>;
    async fn set_expected_request_processing_time(&self, _duration: Duration) -> StorageResult<()> {
        Ok(())
    }
    /// Offers a pacing signal (a 429, a crawl delay). Returns `true` when the manager paces the
    /// requests it covers; a plain queue does not, and returns `false`.
    fn record_pacing_signal(&self, _signal: &super::request_loader::PacingSignal) -> bool {
        false
    }
}

/// Number of slots of the add-deduplication cache, as in Crawlee for JS.
const DEDUP_CACHE_SLOTS: usize = 1 << 20;

/// A direct-mapped cache of unique keys this client has already added, so repeated adds of the
/// same URL (the common case when every page links to the same navigation) skip the backend.
///
/// Slots hold 64-bit fingerprints rather than the keys themselves, which keeps the cache at 8 MiB
/// for a million entries. A fingerprint collision would make the cache report an unseen request
/// as present; with 64-bit fingerprints that needs about 2^32 distinct keys to become likely.
struct DedupCache {
    slots: Option<Box<[u64]>>,
}

impl DedupCache {
    fn fingerprint(unique_key: &str) -> u64 {
        // FNV-1a, 64-bit. Zero marks an empty slot.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in unique_key.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash.max(1)
    }

    fn slot(fingerprint: u64) -> usize {
        // Use the high bits for the slot and the full value for the check.
        (fingerprint >> 44) as usize % DEDUP_CACHE_SLOTS
    }

    fn contains(&self, unique_key: &str) -> bool {
        let fingerprint = Self::fingerprint(unique_key);
        self.slots.as_ref().is_some_and(|slots| slots[Self::slot(fingerprint)] == fingerprint)
    }

    fn insert(&mut self, unique_key: &str) {
        let fingerprint = Self::fingerprint(unique_key);
        let slots = self.slots.get_or_insert_with(|| vec![0u64; DEDUP_CACHE_SLOTS].into_boxed_slice());
        slots[Self::slot(fingerprint)] = fingerprint;
    }

    fn clear(&mut self) {
        self.slots = None;
    }
}

/// A queue of requests to crawl, deduplicated by unique key. Cloning is cheap and clones share
/// the queue.
#[derive(Clone)]
pub struct RequestQueue {
    backend: Arc<dyn RequestQueueBackend>,
    cache_link: CacheLink,
    added: Arc<Mutex<DedupCache>>,
    in_progress_batches: InProgressBatches,
}

impl std::fmt::Debug for RequestQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestQueue").finish_non_exhaustive()
    }
}

impl RequestQueue {
    /// Opens a new handle on `storage`, outside the instance cache of [`Services`](crate::Services);
    /// [`Services::open_request_queue`](crate::Services::open_request_queue)
    /// returns the same instance on every call.
    pub async fn open(storage: &dyn StorageBackend, id: &StorageIdentifier) -> StorageResult<Self> {
        Ok(Self::from_backend(storage.create_request_queue_backend(id).await?))
    }

    pub fn from_backend(backend: Arc<dyn RequestQueueBackend>) -> Self {
        RequestQueue {
            backend,
            cache_link: CacheLink::default(),
            added: Arc::new(Mutex::new(DedupCache { slots: None })),
            in_progress_batches: InProgressBatches::default(),
        }
    }

    pub(crate) fn with_cache_link(mut self, link: CacheLink) -> Self {
        self.cache_link = link;
        self
    }

    pub fn backend(&self) -> &Arc<dyn RequestQueueBackend> {
        &self.backend
    }

    pub async fn add_request(&self, request: impl Into<Request>, forefront: bool) -> StorageResult<ProcessedRequest> {
        let request = request.into();
        let unique_key = request.unique_key.clone();
        let mut result = self.add_requests(vec![request], forefront).await?;
        Ok(result.processed_requests.pop().unwrap_or(ProcessedRequest {
            request_id: unique_key_to_request_id(&unique_key),
            unique_key,
            was_already_present: true,
            was_already_handled: false,
        }))
    }

    pub async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>> {
        self.backend.get_request(unique_key).await
    }

    pub async fn get_info(&self) -> StorageResult<RequestQueueInfo> {
        self.backend.get_metadata().await
    }

    pub async fn drop_storage(self) -> StorageResult<()> {
        self.added.lock().clear();
        self.backend.drop_storage().await?;
        self.cache_link.evict(Arc::as_ptr(&self.backend) as *const ());
        Ok(())
    }
}

#[async_trait]
impl RequestManager for RequestQueue {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult> {
        let mut result = BatchAddRequestsResult::default();
        let mut to_add = Vec::with_capacity(requests.len());
        // A batch keeps the first request of each unique key, as in Crawlee for JS: the Apify
        // platform rejects a whole batch that repeats one.
        let mut in_batch = std::collections::HashSet::with_capacity(requests.len());
        {
            let cache = self.added.lock();
            for request in requests {
                // A forefront add of a known request is not skipped: it may be meant to reorder.
                if !forefront && cache.contains(&request.unique_key) {
                    result.processed_requests.push(ProcessedRequest {
                        request_id: unique_key_to_request_id(&request.unique_key),
                        unique_key: request.unique_key,
                        was_already_present: true,
                        was_already_handled: false,
                    });
                } else if in_batch.insert(request.unique_key.clone()) {
                    to_add.push(request);
                }
            }
        }

        if to_add.is_empty() {
            return Ok(result);
        }

        let added = self.backend.add_batch_of_requests(to_add, forefront).await?;
        {
            let mut cache = self.added.lock();
            for processed in &added.processed_requests {
                cache.insert(&processed.unique_key);
            }
        }
        result.processed_requests.extend(added.processed_requests);
        result.unprocessed_requests.extend(added.unprocessed_requests);
        Ok(result)
    }

    async fn add_requests_batched(
        &self,
        requests: Vec<Request>,
        options: AddRequestsBatchedOptions,
    ) -> StorageResult<AddRequestsBatchedResult> {
        let queue = self.clone();
        let forefront = options.forefront;
        drain_request_batches(requests, &options, &self.in_progress_batches, move |chunk, _is_initial| {
            let queue = queue.clone();
            async move {
                let added = queue.add_requests(chunk, forefront).await?;
                // What the backend reports as unprocessed is a rejection, not a transient failure
                // (the backend retries those): warned about and skipped.
                if !added.unprocessed_requests.is_empty() {
                    tracing::warn!(
                        unprocessed_requests = ?added.unprocessed_requests,
                        "Some requests were rejected by the request queue and will be skipped. This usually means \
                         the request data is malformed (e.g. an invalid 'userData' shape)."
                    );
                }
                Ok(added.processed_requests)
            }
        })
        .await
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        self.backend.fetch_next_request().await
    }

    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>> {
        if request.handled_at.is_none() {
            request.handled_at = Some(crate::now_iso());
        }
        self.backend.mark_request_as_handled(request).await
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        self.backend.reclaim_request(request, forefront).await
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        self.backend.is_empty().await
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        // Not finished while requests are still being added in the background.
        if self.in_progress_batches.any() {
            return Ok(false);
        }
        self.backend.is_finished().await
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        Ok(self.backend.get_metadata().await?.handled_request_count)
    }

    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        self.backend.set_expected_request_processing_time(duration).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::batched_adds::AddRequestsBatchedOptions;
    use crate::storage::memory::MemoryStorageBackend;

    #[tokio::test]
    async fn repeated_adds_are_served_from_the_cache() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();

        let first = queue.add_request("https://example.com/a", false).await.unwrap();
        assert!(!first.was_already_present);
        let second = queue.add_request("https://example.com/a/", false).await.unwrap();
        assert!(second.was_already_present);
        assert_eq!(first.request_id, second.request_id);

        let mut request = queue.fetch_next_request().await.unwrap().unwrap();
        queue.mark_request_as_handled(&mut request).await.unwrap();
        assert!(request.handled_at.is_some());
        assert!(queue.is_finished().await.unwrap());
        assert_eq!(queue.handled_count().await.unwrap(), 1);
    }

    fn urls(count: usize) -> Vec<Request> {
        (0..count).map(|i| Request::from(format!("https://example.com/{i}"))).collect()
    }

    #[tokio::test]
    async fn batched_adds_return_after_the_first_batch_and_keep_the_queue_unfinished() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();
        let options = AddRequestsBatchedOptions {
            batch_size: 10,
            wait_between_batches: Duration::from_millis(50),
            ..Default::default()
        };

        let result = queue.add_requests_batched(urls(25), options).await.unwrap();
        assert_eq!(result.added_requests.len(), 10);
        let remainder = result.wait_for_all_requests_to_be_added.expect("background batches");

        // While batches are still being added, the queue is not finished, even when nothing is
        // pending or in progress.
        while let Some(mut request) = queue.fetch_next_request().await.unwrap() {
            queue.mark_request_as_handled(&mut request).await.unwrap();
        }
        assert!(!queue.is_finished().await.unwrap());

        assert_eq!(remainder.await.unwrap().unwrap().len(), 15);
        while let Some(mut request) = queue.fetch_next_request().await.unwrap() {
            queue.mark_request_as_handled(&mut request).await.unwrap();
        }
        assert!(queue.is_finished().await.unwrap());
        assert_eq!(queue.handled_count().await.unwrap(), 25);
    }

    #[tokio::test]
    async fn batched_adds_can_wait_for_every_batch() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();
        let options = AddRequestsBatchedOptions {
            batch_size: 10,
            wait_between_batches: Duration::ZERO,
            wait_for_all_requests_to_be_added: true,
            ..Default::default()
        };

        let result = queue.add_requests_batched(urls(25), options).await.unwrap();
        assert_eq!(result.added_requests.len(), 25);
        assert!(result.wait_for_all_requests_to_be_added.is_none());
        assert_eq!(queue.get_info().await.unwrap().total_request_count, 25);
    }

    #[tokio::test]
    async fn a_single_batch_adds_nothing_in_the_background() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();

        let result = queue.add_requests_batched(urls(5), AddRequestsBatchedOptions::default()).await.unwrap();
        assert_eq!(result.added_requests.len(), 5);
        assert!(result.wait_for_all_requests_to_be_added.is_none());
    }

    /// A backend that records the unique keys of each batch it gets, as the Apify platform sees them
    /// (it rejects a batch that repeats one).
    struct RecordingBackend {
        inner: Arc<dyn RequestQueueBackend>,
        batches: Mutex<Vec<Vec<String>>>,
    }

    #[async_trait]
    impl RequestQueueBackend for RecordingBackend {
        async fn get_metadata(&self) -> StorageResult<RequestQueueInfo> {
            self.inner.get_metadata().await
        }
        async fn drop_storage(&self) -> StorageResult<()> {
            self.inner.drop_storage().await
        }
        async fn purge(&self) -> StorageResult<()> {
            self.inner.purge().await
        }
        async fn add_batch_of_requests(
            &self,
            requests: Vec<Request>,
            forefront: bool,
        ) -> StorageResult<BatchAddRequestsResult> {
            self.batches.lock().push(requests.iter().map(|request| request.unique_key.clone()).collect());
            self.inner.add_batch_of_requests(requests, forefront).await
        }
        async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>> {
            self.inner.get_request(unique_key).await
        }
        async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
            self.inner.fetch_next_request().await
        }
        async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<Option<QueueOperationInfo>> {
            self.inner.mark_request_as_handled(request).await
        }
        async fn reclaim_request(
            &self,
            request: &Request,
            forefront: bool,
        ) -> StorageResult<Option<QueueOperationInfo>> {
            self.inner.reclaim_request(request, forefront).await
        }
        async fn is_empty(&self) -> StorageResult<bool> {
            self.inner.is_empty().await
        }
        async fn is_finished(&self) -> StorageResult<bool> {
            self.inner.is_finished().await
        }
    }

    #[tokio::test]
    async fn a_batch_reaches_the_backend_without_repeated_unique_keys() {
        let storage = MemoryStorageBackend::new();
        let inner = storage.create_request_queue_backend(&StorageIdentifier::Default).await.unwrap();
        let backend = Arc::new(RecordingBackend { inner, batches: Mutex::new(Vec::new()) });
        let queue = RequestQueue::from_backend(backend.clone());

        for forefront in [false, true] {
            let requests = ["https://example.com/a", "https://example.com/a/", "https://example.com/b"]
                .map(|url| Request::new(format!("{url}?forefront={forefront}")));
            let result = queue.add_requests(requests.to_vec(), forefront).await.unwrap();
            assert!(result.unprocessed_requests.is_empty());
        }

        let batches = backend.batches.lock().clone();
        assert_eq!(batches.len(), 2);
        for batch in batches {
            let mut unique = batch.clone();
            unique.dedup();
            assert_eq!(batch.len(), 2, "{batch:?}");
            assert_eq!(unique, batch);
        }
    }

    #[test]
    fn dedup_cache_fingerprints() {
        let mut cache = DedupCache { slots: None };
        assert!(!cache.contains("https://a.dev"));
        cache.insert("https://a.dev");
        assert!(cache.contains("https://a.dev"));
        assert!(!cache.contains("https://b.dev"));
    }
}
