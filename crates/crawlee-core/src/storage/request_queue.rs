//! [`RequestQueue`] and the [`RequestManager`] trait crawlers consume requests through.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use regex::{Regex, RegexBuilder};

use super::backend::{
    BatchAddRequestsResult, ProcessedRequest, QueueOperationInfo, RequestQueueBackend, RequestQueueInfo,
    StorageBackend, StorageIdentifier,
};
use super::batched_adds::{
    AddRequestsBatchedOptions, AddRequestsBatchedResult, InProgressBatches, drain_request_batches,
    drain_request_batches_inline,
};
use crate::errors::{StorageError, StorageResult};
use crate::events::{EventKind, EventManager};
use crate::recoverable_state::BoxError;
use crate::request::{Request, RequestError, unique_key_to_request_id};
use crate::services::CacheLink;

/// Source and sink of requests for a crawler. [`RequestQueue`] is the default implementation;
/// other request sources (lists, sitemaps, per-domain throttling) implement it as well.
#[async_trait]
pub trait RequestManager: Send + Sync {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult>;
    /// Adds the requests in batches: the first before returning, the rest in the background
    /// (`addRequestsBatched`). A manager that does not batch in the background adds every batch
    /// before returning. Either way, at most
    /// [`max_new_requests`](AddRequestsBatchedOptions::max_new_requests) new requests are added.
    async fn add_requests_batched(
        &self,
        requests: Vec<Request>,
        options: AddRequestsBatchedOptions,
    ) -> StorageResult<AddRequestsBatchedResult> {
        let forefront = options.forefront;
        let (added_requests, requests_over_limit) = drain_request_batches_inline(
            requests,
            options.batch_size,
            options.max_new_requests,
            Duration::ZERO,
            |chunk, _| async move { Ok(self.add_requests(chunk, forefront).await?.processed_requests) },
        )
        .await?;
        Ok(AddRequestsBatchedResult { added_requests, wait_for_all_requests_to_be_added: None, requests_over_limit })
    }
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>>;
    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>>;
    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>>;
    /// No request can be fetched right now.
    async fn is_empty(&self) -> StorageResult<bool>;
    /// All work is done: nothing pending, nothing in progress.
    async fn is_finished(&self) -> StorageResult<bool>;
    async fn handled_count(&self) -> StorageResult<u64>;
    /// Requests ever added, handled ones included (`getTotalCount`). A manager that cannot tell
    /// returns 0.
    async fn total_count(&self) -> StorageResult<u64> {
        Ok(0)
    }
    /// Requests not handled yet, in progress included (`getPendingCount`). A manager that cannot
    /// tell returns 0.
    async fn pending_count(&self) -> StorageResult<u64> {
        Ok(0)
    }
    async fn set_expected_request_processing_time(&self, _duration: Duration) -> StorageResult<()> {
        Ok(())
    }
    /// Whether a request can be fetched now, later, never, or all work is done
    /// (`checkReadiness`). By default, from [`is_empty`](Self::is_empty),
    /// [`is_finished`](Self::is_finished) and [`ready_at`](Self::ready_at); a manager that can
    /// stall reports it here.
    async fn check_readiness(&self) -> StorageResult<super::request_loader::RequestSourceStatus> {
        use super::request_loader::RequestSourceStatus;
        if !self.is_empty().await? {
            return Ok(RequestSourceStatus::Ready);
        }
        if self.is_finished().await? {
            return Ok(RequestSourceStatus::Finished);
        }
        Ok(RequestSourceStatus::Waiting { ready_at: self.ready_at() })
    }
    /// When a manager that has nothing to fetch now (see [`is_empty`](Self::is_empty)) will have
    /// a request ready, if it knows: for example when a held-back domain may be fetched again. The
    /// crawler wakes up then instead of waiting for its next check (`readyAt` of `checkReadiness`).
    fn ready_at(&self) -> Option<std::time::Instant> {
        None
    }
    /// Offers a pacing signal (a 429, a crawl delay). Returns `true` when the manager paces the
    /// requests it covers; a plain queue does not, and returns `false`.
    fn record_pacing_signal(&self, _signal: &super::request_loader::PacingSignal) -> bool {
        false
    }
}

/// Downloads a URL list for [`RequestQueue::add_requests_from_url`] (the `httpClient` of
/// `downloadListOfUrls` in JS). Crawlers set one built on their HTTP client.
#[async_trait]
pub trait UrlListFetcher: Send + Sync {
    /// The body of a `GET` of `url`.
    async fn fetch(&self, url: &str, proxy_url: Option<&str>) -> Result<Vec<u8>, BoxError>;
}

/// The URLs [`RequestsFromUrl`] finds by default (`URL_NO_COMMAS_REGEX` of `@crawlee/utils`).
pub static URL_NO_COMMAS_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    RegexBuilder::new(
        r"https?://(www\.)?([\p{L}0-9]|[\p{L}0-9][-\p{L}0-9@:%._+~#=]{0,254}[\p{L}0-9])\.[a-z]{2,63}(:[0-9]{1,5})?(/[-\p{L}0-9@:%_+.~#?&/=()'*]*)?",
    )
    .case_insensitive(true)
    .size_limit(64 << 20)
    .build()
    .expect("a valid regex")
});

/// Every match of `regex` (default [`URL_NO_COMMAS_REGEX`]) in `text`, line by line
/// (`extractUrls` in JS).
pub fn extract_urls(text: &str, regex: Option<&Regex>) -> Vec<String> {
    let regex = regex.unwrap_or(&URL_NO_COMMAS_REGEX);
    text.split('\n').flat_map(|line| regex.find_iter(line).map(|found| found.as_str().to_owned())).collect()
}

type RequestTemplate = Arc<dyn Fn(&str) -> Result<Request, RequestError> + Send + Sync>;

/// Requests read from a remote list of URLs, the `{ requestsFromUrl, regex, ...options }` source
/// of Crawlee for JS.
#[derive(Clone)]
pub struct RequestsFromUrl {
    pub requests_from_url: String,
    /// What a URL looks like in the list; [`URL_NO_COMMAS_REGEX`] by default.
    pub regex: Option<Regex>,
    request: RequestTemplate,
}

impl std::fmt::Debug for RequestsFromUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestsFromUrl")
            .field("requests_from_url", &self.requests_from_url)
            .field("regex", &self.regex)
            .finish_non_exhaustive()
    }
}

impl RequestsFromUrl {
    pub fn new(requests_from_url: impl Into<String>) -> Self {
        RequestsFromUrl {
            requests_from_url: requests_from_url.into(),
            regex: None,
            request: Arc::new(|url| Ok(Request::new(url))),
        }
    }

    pub fn regex(mut self, regex: Regex) -> Self {
        self.regex = Some(regex);
        self
    }

    /// Makes the request of each URL found, for the request options the URLs share (a label,
    /// user data, headers...). `Request::new` by default.
    pub fn request(mut self, make: impl Fn(&str) -> Result<Request, RequestError> + Send + Sync + 'static) -> Self {
        self.request = Arc::new(make);
        self
    }

    /// Downloads the list with `fetcher` (through `proxy_url`, if set), and makes a request of
    /// every URL in it (`#fetchRequestsFromUrl`). A list without URLs is warned about.
    pub(crate) async fn fetch_requests(
        &self,
        fetcher: Option<&dyn UrlListFetcher>,
        proxy_url: Option<&str>,
    ) -> StorageResult<Vec<Request>> {
        let requests_from_url = &self.requests_from_url;
        let cannot_fetch = |err: &dyn std::fmt::Display| {
            StorageError::Backend(format!("Cannot fetch a request list from {requests_from_url}: {err}").into())
        };
        let fetcher = fetcher.ok_or_else(|| {
            cannot_fetch(&"no URL list fetcher was set; set one with RequestQueue::set_url_list_fetcher")
        })?;
        // A Google Sheets sharing link is fetched as the CSV export of the sheet.
        static GOOGLE_SHEETS: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^(https://docs\.google\.com/spreadsheets/d/[A-Za-z0-9_-]+)/?").unwrap());
        let url = match GOOGLE_SHEETS.captures(requests_from_url) {
            Some(sheet) => format!("{}/gviz/tq?tqx=out:csv", &sheet[1]),
            None => requests_from_url.clone(),
        };
        let body = fetcher.fetch(&url, proxy_url).await.map_err(|err| cannot_fetch(&err))?;
        let urls = extract_urls(&String::from_utf8_lossy(&body), self.regex.as_ref());
        if urls.is_empty() {
            tracing::warn!(requests_from_url, regex = ?self.regex, "The fetched list contains no valid URLs.");
            return Ok(Vec::new());
        }
        urls.iter()
            .map(|url| (self.request)(url))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| StorageError::InvalidArgument(err.to_string()))
    }

    /// The first five URLs of `requests`, for the log.
    pub(crate) fn sample(&self, requests: &[Request]) -> Vec<String> {
        requests.iter().take(5).map(|request| request.url.clone()).collect()
    }
}

/// Number of slots of the add-deduplication cache, as in Crawlee for JS.
const DEDUP_CACHE_SLOTS: usize = 1 << 20;

/// A direct-mapped cache of unique keys this client has already added, so repeated adds of the
/// same URL (the common case when every page links to the same navigation) skip the backend.
/// It also remembers whether each request was handled, as the backend last reported it (the
/// `requestCache` of `RequestQueue` in JS), so that a cached add reports it.
///
/// Slots hold 64-bit fingerprints rather than the keys themselves, which keeps the cache at 8 MiB
/// for a million entries. The lowest bit of a slot is the handled flag. A fingerprint collision
/// would make the cache report an unseen request as present; with 63-bit fingerprints that needs
/// about 2^31 distinct keys to become likely.
struct DedupCache {
    slots: Option<Box<[u64]>>,
}

impl DedupCache {
    const HANDLED: u64 = 1;

    fn fingerprint(unique_key: &str) -> u64 {
        // FNV-1a, 64-bit, without the flag bit. Zero marks an empty slot.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in unique_key.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        (hash & !Self::HANDLED).max(2)
    }

    fn slot(fingerprint: u64) -> usize {
        // FNV-1a barely moves the high bits for keys that differ in their last bytes (URLs that
        // differ in a trailing digit), so they are mixed before picking the slot from them.
        (fingerprint.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 44) as usize % DEDUP_CACHE_SLOTS
    }

    /// Whether the request was handled, or `None` if it is not cached.
    fn get(&self, unique_key: &str) -> Option<bool> {
        let fingerprint = Self::fingerprint(unique_key);
        let slot = self.slots.as_ref()?[Self::slot(fingerprint)];
        (slot & !Self::HANDLED == fingerprint).then_some(slot & Self::HANDLED != 0)
    }

    fn insert(&mut self, unique_key: &str, handled: bool) {
        let fingerprint = Self::fingerprint(unique_key);
        let slots = self.slots.get_or_insert_with(|| vec![0u64; DEDUP_CACHE_SLOTS].into_boxed_slice());
        slots[Self::slot(fingerprint)] = fingerprint | u64::from(handled);
    }

    fn clear(&mut self) {
        self.slots = None;
    }
}

/// The operations a request queue handle (and its clones) issued to the backend, counted per
/// backend call (`RequestQueueStats` in JS).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestQueueStats {
    /// Adds, `mark_request_as_handled` and `reclaim_request` calls.
    pub write_count: u64,
    /// `fetch_next_request` calls.
    pub head_item_read_count: u64,
}

#[derive(Default)]
struct RequestQueueCounters {
    write: AtomicU64,
    head_item_read: AtomicU64,
}

/// A queue of requests to crawl, deduplicated by unique key. Cloning is cheap and clones share
/// the queue.
#[derive(Clone)]
pub struct RequestQueue {
    backend: Arc<dyn RequestQueueBackend>,
    cache_link: CacheLink,
    added: Arc<Mutex<DedupCache>>,
    /// The highest expected processing time passed on to the backend.
    expected_processing_time: Arc<Mutex<Duration>>,
    counters: Arc<RequestQueueCounters>,
    url_list_fetcher: Arc<Mutex<Option<Arc<dyn UrlListFetcher>>>>,
    in_progress_batches: InProgressBatches,
    /// Set on the `Migrating` event: nothing is fetched any more (`#queuePausedForMigration`).
    paused_for_migration: Arc<AtomicBool>,
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
            expected_processing_time: Arc::default(),
            counters: Arc::default(),
            url_list_fetcher: Arc::default(),
            in_progress_batches: InProgressBatches::default(),
            paused_for_migration: Arc::default(),
        }
    }

    /// Stops fetching requests once `events` emits [`Event::Migrating`](crate::Event::Migrating),
    /// as the queues of Crawlee for JS do: the run is about to move to another host, where the
    /// requests are fetched again. Queues opened through [`Services`](crate::Services) listen to
    /// its events.
    pub fn with_events(self, events: &EventManager) -> Self {
        let paused = self.paused_for_migration.clone();
        events.on(EventKind::Migrating, move |_| {
            paused.store(true, Ordering::Release);
            async {}
        });
        self
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

    /// Removes every request, and forgets what this client cached about them, so the queue behaves
    /// as if freshly opened.
    pub async fn purge(&self) -> StorageResult<()> {
        self.backend.purge().await?;
        self.added.lock().clear();
        self.in_progress_batches.reset();
        *self.expected_processing_time.lock() = Duration::ZERO;
        Ok(())
    }

    /// Sets how [`add_requests_from_url`](Self::add_requests_from_url) downloads its lists.
    pub fn set_url_list_fetcher(&self, fetcher: Arc<dyn UrlListFetcher>) {
        *self.url_list_fetcher.lock() = Some(fetcher);
    }

    /// Whether a [`UrlListFetcher`] was set.
    pub fn has_url_list_fetcher(&self) -> bool {
        self.url_list_fetcher.lock().is_some()
    }

    /// Downloads the list at `source.requests_from_url`, and adds a request for every URL in it
    /// with [`add_requests_batched`](RequestManager::add_requests_batched): the first batch before
    /// returning, the rest in the background (`addRequest({ requestsFromUrl })` in JS). Returns
    /// the requests of the first batch.
    pub async fn add_requests_from_url(
        &self,
        source: RequestsFromUrl,
        forefront: bool,
    ) -> StorageResult<Vec<ProcessedRequest>> {
        let fetcher = self.url_list_fetcher.lock().clone();
        let requests = source.fetch_requests(fetcher.as_deref(), None).await?;
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let fetched_count = requests.len();
        let sample = source.sample(&requests);
        let options = AddRequestsBatchedOptions { forefront, ..Default::default() };
        let added = self.add_requests_batched(requests, options).await?.added_requests;
        tracing::info!(
            requests_from_url = source.requests_from_url,
            regex = ?source.regex,
            fetched_count,
            imported_count = added.len(),
            duplicate_count = fetched_count.saturating_sub(added.len()),
            sample = ?sample,
            "Fetched and loaded Requests from a remote resource."
        );
        Ok(added)
    }

    pub fn stats(&self) -> RequestQueueStats {
        RequestQueueStats {
            write_count: self.counters.write.load(Ordering::Relaxed),
            head_item_read_count: self.counters.head_item_read.load(Ordering::Relaxed),
        }
    }

    fn count_write(&self) {
        self.counters.write.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_request(&self, unique_key: &str, handled: bool) {
        self.added.lock().insert(unique_key, handled);
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
                // As in JS, a cached request is not added again, not even to the forefront.
                if let Some(handled) = cache.get(&request.unique_key) {
                    result.processed_requests.push(ProcessedRequest {
                        request_id: unique_key_to_request_id(&request.unique_key),
                        unique_key: request.unique_key,
                        was_already_present: true,
                        was_already_handled: handled,
                    });
                } else if in_batch.insert(request.unique_key.clone()) {
                    to_add.push(request);
                }
            }
        }

        if to_add.is_empty() {
            return Ok(result);
        }

        self.count_write();
        let added = self.backend.add_batch_of_requests(to_add, forefront).await?;
        {
            let mut cache = self.added.lock();
            for processed in &added.processed_requests {
                cache.insert(&processed.unique_key, processed.was_already_handled);
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
        if self.paused_for_migration.load(Ordering::Acquire) {
            return Ok(None);
        }
        self.counters.head_item_read.fetch_add(1, Ordering::Relaxed);
        self.backend.fetch_next_request().await
    }

    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>> {
        let previous = request.handled_at.clone();
        if request.handled_at.is_none() {
            request.handled_at = Some(crate::now_iso());
        }
        self.count_write();
        let info = self.backend.mark_request_as_handled(request).await?;
        match &info {
            // The request was not in progress: nothing changed, the request included.
            None => request.handled_at = previous,
            Some(info) => self.cache_request(&request.unique_key, info.was_already_handled),
        }
        Ok(info)
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        self.count_write();
        let info = self.backend.reclaim_request(request, forefront).await?;
        if let Some(info) = &info {
            self.cache_request(&request.unique_key, info.was_already_handled);
        }
        Ok(info)
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

    async fn total_count(&self) -> StorageResult<u64> {
        Ok(self.backend.get_metadata().await?.total_request_count)
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        let info = self.backend.get_metadata().await?;
        Ok(info.total_request_count.saturating_sub(info.handled_request_count))
    }

    /// Passed on to the backend only when it raises the highest time so far.
    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        {
            let mut expected = self.expected_processing_time.lock();
            if duration <= *expected {
                return Ok(());
            }
            *expected = duration;
        }
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
        expected_times: Mutex<Vec<Duration>>,
    }

    impl RecordingBackend {
        async fn new(storage: &MemoryStorageBackend) -> Arc<Self> {
            let inner = storage.create_request_queue_backend(&StorageIdentifier::Default).await.unwrap();
            Arc::new(RecordingBackend { inner, batches: Mutex::default(), expected_times: Mutex::default() })
        }
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
        async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
            self.expected_times.lock().push(duration);
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_batch_reaches_the_backend_without_repeated_unique_keys() {
        let storage = MemoryStorageBackend::new();
        let backend = RecordingBackend::new(&storage).await;
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

    #[tokio::test]
    async fn a_cached_add_reports_whether_the_request_was_handled() {
        let storage = MemoryStorageBackend::new();
        let first = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();
        first.add_request("https://example.com/a", false).await.unwrap();
        let mut request = first.fetch_next_request().await.unwrap().unwrap();
        first.mark_request_as_handled(&mut request).await.unwrap();

        // A new client learns from the backend that the request was handled, and remembers it.
        let backend = RecordingBackend::new(&storage).await;
        let queue = RequestQueue::from_backend(backend.clone());
        for _ in 0..2 {
            let added = queue.add_request("https://example.com/a", false).await.unwrap();
            assert!(added.was_already_present && added.was_already_handled);
        }
        assert_eq!(backend.batches.lock().len(), 1, "the second add is served from the cache");
    }

    #[tokio::test]
    async fn a_cached_forefront_add_is_served_from_the_cache() {
        let storage = MemoryStorageBackend::new();
        let backend = RecordingBackend::new(&storage).await;
        let queue = RequestQueue::from_backend(backend.clone());
        queue.add_request("https://example.com/a", false).await.unwrap();
        let again = queue.add_request("https://example.com/a", true).await.unwrap();
        assert!(again.was_already_present);
        assert_eq!(backend.batches.lock().len(), 1);
    }

    #[tokio::test]
    async fn marking_and_reclaiming_update_the_cache() {
        let storage = MemoryStorageBackend::new();
        let backend = RecordingBackend::new(&storage).await;
        let queue = RequestQueue::from_backend(backend.clone());
        queue.add_request("https://example.com/a", false).await.unwrap();
        let mut request = queue.fetch_next_request().await.unwrap().unwrap();
        // As in JS, the cache keeps the `was_already_handled` the backend reports: the state
        // before the request was marked.
        queue.mark_request_as_handled(&mut request).await.unwrap();
        assert!(!queue.add_request("https://example.com/a", false).await.unwrap().was_already_handled);
        queue.mark_request_as_handled(&mut request).await.unwrap();
        assert!(queue.add_request("https://example.com/a", false).await.unwrap().was_already_handled);
        assert_eq!(backend.batches.lock().len(), 1);
    }

    #[tokio::test]
    async fn a_request_the_backend_does_not_mark_keeps_no_handled_at() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();
        let mut request = Request::new("https://example.com/unknown");
        assert!(queue.mark_request_as_handled(&mut request).await.unwrap().is_none());
        assert!(request.handled_at.is_none());
    }

    #[tokio::test]
    async fn the_expected_processing_time_is_only_raised() {
        let storage = MemoryStorageBackend::new();
        let backend = RecordingBackend::new(&storage).await;
        let queue = RequestQueue::from_backend(backend.clone());
        for secs in [60, 30, 90, 90] {
            queue.set_expected_request_processing_time(Duration::from_secs(secs)).await.unwrap();
        }
        assert_eq!(*backend.expected_times.lock(), [Duration::from_secs(60), Duration::from_secs(90)]);

        queue.purge().await.unwrap();
        queue.set_expected_request_processing_time(Duration::from_secs(30)).await.unwrap();
        assert_eq!(backend.expected_times.lock().last(), Some(&Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn purge_empties_the_queue_and_its_caches() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();
        queue.add_request("https://example.com/a", false).await.unwrap();
        queue.purge().await.unwrap();
        assert!(queue.is_finished().await.unwrap());
        assert!(!queue.add_request("https://example.com/a", false).await.unwrap().was_already_present);
    }

    #[tokio::test]
    async fn purge_forgets_the_batches_still_being_added() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();
        let options = AddRequestsBatchedOptions {
            batch_size: 1,
            wait_between_batches: Duration::from_secs(60),
            ..Default::default()
        };
        let result = queue.add_requests_batched(urls(3), options).await.unwrap();
        assert!(result.wait_for_all_requests_to_be_added.is_some());
        // The second batch is added right away, the third after a minute.
        while queue.get_info().await.unwrap().total_request_count < 2 {
            tokio::task::yield_now().await;
        }
        assert!(!queue.is_finished().await.unwrap());

        // As if freshly opened: the batches of before do not keep it unfinished.
        queue.purge().await.unwrap();
        assert!(queue.is_finished().await.unwrap());
    }

    #[tokio::test]
    async fn a_migration_pauses_fetching() {
        let services = crate::Services::in_memory();
        let queue = services.open_request_queue(&StorageIdentifier::Default).await.unwrap();
        queue.add_request("https://example.com/a", false).await.unwrap();

        services.events.emit(crate::Event::Migrating);
        services.events.wait_for_all_listeners_to_complete().await;
        assert!(queue.fetch_next_request().await.unwrap().is_none());
        assert_eq!(queue.stats().head_item_read_count, 0);
        assert!(!queue.is_finished().await.unwrap());
    }

    #[tokio::test]
    async fn counts_backend_calls() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();
        queue.add_requests(urls(2), false).await.unwrap();
        queue.add_requests(urls(2), false).await.unwrap(); // served from the cache
        let mut request = queue.fetch_next_request().await.unwrap().unwrap();
        queue.reclaim_request(&request, false).await.unwrap();
        request = queue.fetch_next_request().await.unwrap().unwrap();
        queue.mark_request_as_handled(&mut request).await.unwrap();
        assert_eq!(queue.stats(), RequestQueueStats { write_count: 3, head_item_read_count: 2 });
    }

    /// Serves fixed bodies and records the URLs it fetched.
    #[derive(Default)]
    struct FixedFetcher {
        body: &'static str,
        fetched: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl UrlListFetcher for FixedFetcher {
        async fn fetch(&self, url: &str, _proxy_url: Option<&str>) -> Result<Vec<u8>, BoxError> {
            self.fetched.lock().push(url.to_owned());
            if self.body == "fail" {
                return Err("connection refused".into());
            }
            Ok(self.body.as_bytes().to_vec())
        }
    }

    #[test]
    fn extracts_urls_line_by_line() {
        let text = "https://a.com/x, https://b.com/y\nnot a url\nhttp://www.příklad.cz:8080/z?q=1";
        assert_eq!(
            extract_urls(text, None),
            ["https://a.com/x", "https://b.com/y", "http://www.příklad.cz:8080/z?q=1"]
        );
        let only_b = regex::Regex::new("https://b[^,]+").unwrap();
        assert_eq!(extract_urls(text, Some(&only_b)), ["https://b.com/y"]);
    }

    #[tokio::test]
    async fn adds_the_requests_of_a_remote_list() {
        let storage = MemoryStorageBackend::new();
        let queue = RequestQueue::open(&storage, &StorageIdentifier::Default).await.unwrap();
        let source = RequestsFromUrl::new("https://example.com/list.txt")
            .request(|url| Request::builder(url).label("LIST").build());

        let err = queue.add_requests_from_url(source.clone(), false).await.unwrap_err();
        assert!(err.to_string().contains("set_url_list_fetcher"), "{err}");

        let fetcher =
            Arc::new(FixedFetcher { body: "https://a.com/1\nhttps://a.com/2 https://a.com/1", ..Default::default() });
        queue.set_url_list_fetcher(fetcher.clone());
        let added = queue.add_requests_from_url(source, false).await.unwrap();
        assert_eq!(added.len(), 2);
        let request = queue.fetch_next_request().await.unwrap().unwrap();
        assert_eq!((request.url.as_str(), request.label()), ("https://a.com/1", Some("LIST")));

        // A Google Sheets sharing link is read as CSV.
        queue
            .add_requests_from_url(
                RequestsFromUrl::new("https://docs.google.com/spreadsheets/d/abc-1_x/edit#gid=0"),
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            fetcher.fetched.lock().last().map(String::as_str),
            Some("https://docs.google.com/spreadsheets/d/abc-1_x/gviz/tq?tqx=out:csv")
        );

        let failing = RequestQueue::open(&storage, &StorageIdentifier::alias("other")).await.unwrap();
        failing.set_url_list_fetcher(Arc::new(FixedFetcher { body: "fail", ..Default::default() }));
        let err = failing.add_requests_from_url(RequestsFromUrl::new("https://x.com/l"), false).await.unwrap_err();
        assert!(err.to_string().ends_with("Cannot fetch a request list from https://x.com/l: connection refused"));
    }

    #[test]
    fn dedup_cache_fingerprints() {
        let mut cache = DedupCache { slots: None };
        assert_eq!(cache.get("https://a.dev"), None);
        cache.insert("https://a.dev", false);
        assert_eq!(cache.get("https://a.dev"), Some(false));
        cache.insert("https://a.dev", true);
        assert_eq!(cache.get("https://a.dev"), Some(true));
        assert_eq!(cache.get("https://b.dev"), None);

        // Keys that differ only in their last character land in different slots.
        for i in 0..100 {
            cache.insert(&format!("https://example.com/{i}"), false);
        }
        assert!((0..100).all(|i| cache.get(&format!("https://example.com/{i}")).is_some()));
    }
}
