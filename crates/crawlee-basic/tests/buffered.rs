//! Buffered dataset writes, an extra of crawlee-rs: a request is marked as handled only once the
//! items pushed while handling it are stored.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde_json::{Value, json};

use crawlee_basic::{BasicContext, BasicCrawler};
use crawlee_core::errors::StorageError;
use crawlee_core::storage::backend::{
    BatchAddRequestsResult, DatasetBackend, DatasetInfo, DatasetItem, DatasetListOptions, KeyValueStoreBackend,
    PaginatedList, QueueOperationInfo, RequestQueueBackend,
};
use crawlee_core::storage::dataset_buffer::{BufferStage, DatasetBufferOptions};
use crawlee_core::{
    Configuration, MemoryStorageBackend, Request, RequestManager, RequestQueue, RequestSourceStatus, Services,
    StorageBackend, StorageIdentifier, StorageResult,
};

/// What reached the storages, in order.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn push(&self, entry: String) {
        self.0.lock().push(entry);
    }

    fn entries(&self) -> Vec<String> {
        self.0.lock().clone()
    }

    fn position(&self, entry: &str) -> usize {
        self.entries().iter().position(|logged| logged == entry).unwrap_or_else(|| panic!("{entry} not logged"))
    }

    fn count(&self, prefix: &str) -> usize {
        self.entries().iter().filter(|entry| entry.starts_with(prefix)).count()
    }
}

/// Memory storage whose datasets log what they store, and fail the pushes they are told to.
struct LoggingStorage {
    inner: MemoryStorageBackend,
    log: Log,
    failing_pushes: Arc<AtomicUsize>,
}

struct LoggingDataset {
    inner: Arc<dyn DatasetBackend>,
    log: Log,
    failing_pushes: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl DatasetBackend for LoggingDataset {
    async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
        self.inner.get_metadata().await
    }
    async fn drop_storage(&self) -> StorageResult<()> {
        self.inner.drop_storage().await
    }
    async fn purge(&self) -> StorageResult<()> {
        self.inner.purge().await
    }
    async fn push_data(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
        if self.failing_pushes.load(Ordering::SeqCst) > 0 {
            self.failing_pushes.fetch_sub(1, Ordering::SeqCst);
            self.log.push("failed push".to_owned());
            return Err(StorageError::Backend("the storage is down".into()));
        }
        self.log.push(format!("push of {}", items.len()));
        for item in &items {
            let item: Value = serde_json::from_str(item.get()).unwrap();
            self.log.push(format!("stored {} {}", item["via"].as_str().unwrap(), item["url"].as_str().unwrap()));
        }
        self.inner.push_data(items).await
    }
    async fn get_data(&self, options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>> {
        self.inner.get_data(options).await
    }
}

#[async_trait::async_trait]
impl StorageBackend for LoggingStorage {
    async fn create_dataset_backend(&self, id: &StorageIdentifier) -> StorageResult<Arc<dyn DatasetBackend>> {
        let inner = self.inner.create_dataset_backend(id).await?;
        Ok(Arc::new(LoggingDataset { inner, log: self.log.clone(), failing_pushes: self.failing_pushes.clone() }))
    }
    async fn create_key_value_store_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn KeyValueStoreBackend>> {
        self.inner.create_key_value_store_backend(id).await
    }
    async fn create_request_queue_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn RequestQueueBackend>> {
        self.inner.create_request_queue_backend(id).await
    }
}

/// The crawler's queue, logging the requests it marks as handled and the lock time it is told.
/// With `pace`, a request is handed out only `pace` after the previous one, as a manager that
/// paces a domain does.
struct LoggingManager {
    queue: RequestQueue,
    log: Log,
    lock_times: Mutex<Vec<Duration>>,
    pace: Option<Duration>,
    next_at: Mutex<Option<Instant>>,
}

impl LoggingManager {
    fn held_until(&self) -> Option<Instant> {
        self.next_at.lock().filter(|at| *at > Instant::now())
    }
}

#[async_trait::async_trait]
impl RequestManager for LoggingManager {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult> {
        self.queue.add_requests(requests, forefront).await
    }
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        if self.held_until().is_some() {
            return Ok(None);
        }
        let request = self.queue.fetch_next_request().await?;
        if let (Some(pace), Some(_)) = (self.pace, &request) {
            *self.next_at.lock() = Some(Instant::now() + pace);
        }
        Ok(request)
    }
    async fn check_readiness(&self) -> StorageResult<RequestSourceStatus> {
        match self.held_until() {
            Some(at) if !self.queue.is_empty().await? => Ok(RequestSourceStatus::Waiting { ready_at: Some(at) }),
            _ => self.queue.check_readiness().await,
        }
    }
    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>> {
        self.log.push(format!("marked {}", request.url));
        self.queue.mark_request_as_handled(request).await
    }
    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        self.log.push(format!("reclaimed {}", request.url));
        self.queue.reclaim_request(request, forefront).await
    }
    async fn is_empty(&self) -> StorageResult<bool> {
        self.queue.is_empty().await
    }
    async fn is_finished(&self) -> StorageResult<bool> {
        self.queue.is_finished().await
    }
    async fn handled_count(&self) -> StorageResult<u64> {
        self.queue.handled_count().await
    }
    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        self.lock_times.lock().push(duration);
        Ok(())
    }
}

struct Setup {
    services: Services,
    manager: Arc<LoggingManager>,
    log: Log,
    failing_pushes: Arc<AtomicUsize>,
}

async fn setup(buffer: Option<DatasetBufferOptions>) -> Setup {
    setup_paced(buffer, None).await
}

async fn setup_paced(buffer: Option<DatasetBufferOptions>, pace: Option<Duration>) -> Setup {
    let log = Log::default();
    let failing_pushes = Arc::new(AtomicUsize::new(0));
    let storage = Arc::new(LoggingStorage {
        inner: MemoryStorageBackend::new(),
        log: log.clone(),
        failing_pushes: failing_pushes.clone(),
    });
    let configuration = Configuration { dataset_buffer: buffer, ..Configuration::default() };
    let services = Services::with_configuration(configuration, storage);
    let queue = services.open_request_queue(&StorageIdentifier::Default).await.unwrap();
    let manager = Arc::new(LoggingManager {
        queue,
        log: log.clone(),
        lock_times: Mutex::default(),
        pace,
        next_at: Mutex::default(),
    });
    Setup { services, manager, log, failing_pushes }
}

/// Starts the events before the crawler does, and lets their first `PersistState` (emitted right
/// away) flush, so that no flush lands in the middle of a crawl by chance.
async fn start_events(events: &crawlee_core::EventManager) {
    let persisted = Arc::new(tokio::sync::Notify::new());
    let notify = persisted.clone();
    let listener = events.on(crawlee_core::EventKind::PersistState, move |_| {
        notify.notify_one();
        async {}
    });
    events.init().await;
    persisted.notified().await;
    events.off(listener);
    events.wait_for_all_listeners_to_complete().await;
}

fn lingering(linger: Duration) -> DatasetBufferOptions {
    DatasetBufferOptions::new().with_stages([BufferStage::new(0, linger)])
}

/// Pushes through the context, and straight to the default dataset as an SDK's
/// `Actor::push_data` does.
async fn push_both_ways(ctx: &BasicContext) -> anyhow::Result<()> {
    let url = ctx.request().url.clone();
    ctx.push_data(&json!({ "url": url, "via": "context" }))?;
    let dataset = ctx.services().open_dataset(&StorageIdentifier::Default).await?;
    dataset.push_data(&json!({ "url": url, "via": "direct" })).await?;
    Ok(())
}

fn urls(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("https://a.dev/{i}")).collect()
}

#[tokio::test]
async fn requests_are_marked_as_handled_only_after_their_items_are_stored() {
    let Setup { services, manager, log, .. } = setup(Some(lingering(Duration::from_millis(200)))).await;
    let events = services.events.clone();
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(manager)
        .max_concurrency(4)
        .request_handler(|ctx: BasicContext| async move { push_both_ways(&ctx).await })
        .build()
        .unwrap();

    start_events(&events).await;
    let stats = crawler.run(urls(12)).await.unwrap();
    assert_eq!(stats.requests_succeeded, 12);
    for url in urls(12) {
        let marked = log.position(&format!("marked {url}"));
        assert!(log.position(&format!("stored context {url}")) < marked, "{url}: {:?}", log.entries());
        assert!(log.position(&format!("stored direct {url}")) < marked, "{url}: {:?}", log.entries());
    }
    assert_eq!(log.count("stored "), 24);
    assert!(log.count("push of ") < 12, "the items were stored in batches: {:?}", log.entries());
}

#[tokio::test]
async fn a_request_frees_its_slot_while_its_items_wait_and_the_crawler_flushes_when_it_runs_out_of_work() {
    let Setup { services, manager, log, .. } = setup(Some(lingering(Duration::from_secs(3600)))).await;
    let events = services.events.clone();
    let handler_log = log.clone();
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(manager)
        .fixed_concurrency(1)
        .request_handler(move |ctx: BasicContext| {
            let log = handler_log.clone();
            async move {
                log.push(format!("handled {}", ctx.request().url));
                push_both_ways(&ctx).await
            }
        })
        .build()
        .unwrap();
    // Pushed outside any request, and stored by the first `PersistState`.
    crawler.dataset().await.unwrap().push_data(&json!({ "url": "startup", "via": "direct" })).await.unwrap();
    start_events(&events).await;
    assert_eq!(log.entries(), ["push of 1", "stored direct startup"]);

    let started = Instant::now();
    let stats = crawler.run(urls(5)).await.unwrap();
    assert_eq!(stats.requests_succeeded, 5);
    assert!(started.elapsed() < Duration::from_secs(10), "the crawler flushed instead of waiting an hour");
    // With one slot, every request was handled before the first one was marked: the requests
    // waiting for their items freed it. Their items were stored together once the queue had
    // nothing left to start.
    assert!(log.position("handled https://a.dev/4") < log.position("marked https://a.dev/0"), "{:?}", log.entries());
    assert_eq!(log.count("push of "), 2, "{:?}", log.entries());
    assert!(log.position("push of 10") < log.position("marked https://a.dev/0"), "{:?}", log.entries());
    events.close().await;
}

#[tokio::test]
async fn a_request_held_back_for_a_moment_does_not_flush_the_items_waiting() {
    // The second request is handed out 300 ms after the first: the crawler has nothing to start
    // meanwhile, but will have, so the first request's items keep waiting for their linger (1 h).
    let pace = Some(Duration::from_millis(300));
    let Setup { services, manager, log, .. } = setup_paced(Some(lingering(Duration::from_secs(3600))), pace).await;
    let events = services.events.clone();
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(manager)
        .request_handler(|ctx: BasicContext| async move { push_both_ways(&ctx).await })
        .build()
        .unwrap();
    start_events(&events).await;

    let stats = crawler.run(urls(2)).await.unwrap();
    assert_eq!(stats.requests_succeeded, 2);
    assert_eq!(log.count("push of "), 1, "stored together once the queue had nothing left: {:?}", log.entries());
    assert_eq!(log.count("stored "), 4);
    events.close().await;
}

#[tokio::test]
async fn a_failed_flush_retries_the_request_instead_of_marking_it() {
    let Setup { services, manager, log, failing_pushes } = setup(Some(lingering(Duration::from_millis(100)))).await;
    let events = services.events.clone();
    failing_pushes.store(1, Ordering::SeqCst);
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(manager)
        .request_handler(|ctx: BasicContext| async move { push_both_ways(&ctx).await })
        .build()
        .unwrap();

    start_events(&events).await;
    let stats = crawler.run(["https://a.dev/0"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed, stats.requests_retries), (1, 0, 1));
    assert_eq!(
        log.entries(),
        [
            "failed push",
            "reclaimed https://a.dev/0",
            "push of 2",
            // The direct push is made in the handler, the context's when the handler succeeded.
            "stored direct https://a.dev/0",
            "stored context https://a.dev/0",
            "marked https://a.dev/0",
        ]
    );
}

#[tokio::test]
async fn a_request_whose_items_cannot_be_stored_fails_after_its_retries() {
    let Setup { services, manager, log, failing_pushes } = setup(Some(lingering(Duration::from_millis(50)))).await;
    let events = services.events.clone();
    failing_pushes.store(usize::MAX, Ordering::SeqCst);
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(manager)
        .max_request_retries(1)
        .request_handler(|ctx: BasicContext| async move { push_both_ways(&ctx).await })
        .build()
        .unwrap();

    start_events(&events).await;
    let stats = crawler.run(["https://a.dev/0"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed, stats.requests_retries), (0, 1, 1));
    // Marked only as failed, after its retries, as any failed request is.
    assert_eq!(log.count("marked "), 1);
    assert_eq!(log.count("failed push"), 2);
    assert!(log.position("reclaimed https://a.dev/0") < log.position("marked https://a.dev/0"));
}

#[tokio::test]
async fn requests_are_locked_for_the_linger_too() {
    let Setup { services, manager, .. } = setup(Some(DatasetBufferOptions::default())).await;
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(manager.clone())
        .request_handler_timeout(Duration::from_secs(10))
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .build()
        .unwrap();
    crawler.add_requests(Vec::<Request>::new()).await.unwrap();
    // A minute, plus the longest linger of the defaults.
    assert_eq!(*manager.lock_times.lock(), [Duration::from_secs(62)]);
}

#[tokio::test]
async fn without_buffering_every_push_is_stored_at_once() {
    let Setup { services, manager, log, .. } = setup(None).await;
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(manager)
        .request_handler(|ctx: BasicContext| async move { push_both_ways(&ctx).await })
        .build()
        .unwrap();
    crawler.run(urls(3)).await.unwrap();
    assert_eq!(log.count("push of 1"), 6);
}
