//! The request timeout and how crawlers built on `BasicCrawler` take part in it.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crawlee_basic::{BasicContext, BasicCrawler, BasicCrawlerBuilder, Middleware};
use crawlee_core::storage::backend::{BatchAddRequestsResult, QueueOperationInfo};
use crawlee_core::{Request, RequestManager, RequestQueue, Services, StorageIdentifier, StorageResult};

/// A pipeline step that navigates: it takes `delay`, within a navigation timeout of `timeout`.
struct Navigating {
    delay: Duration,
    timeout: Duration,
}

#[async_trait::async_trait]
impl Middleware<BasicContext> for Navigating {
    type Out = BasicContext;

    async fn run(&self, ctx: BasicContext) -> anyhow::Result<BasicContext> {
        tokio::time::sleep(self.delay).await;
        Ok(ctx)
    }

    fn navigation_timeout(&self) -> Duration {
        self.timeout
    }
}

/// As `withRequestTimeout` in JS, the request timeout is the longer of the internal timeout and
/// the navigation timeout plus the handler timeout.
#[tokio::test(start_paused = true)]
async fn the_request_timeout_covers_the_navigation_timeout() {
    // Handler timeout 60 s, so an internal timeout of 300 s; the navigation takes 350 s of 400 s.
    let navigating = Navigating { delay: Duration::from_secs(350), timeout: Duration::from_secs(400) };
    let crawler = BasicCrawlerBuilder::with_pipeline(navigating)
        .services(Services::in_memory())
        .max_request_retries(0)
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.test/"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed), (1, 0), "{stats:#?}");
}

/// A request queue that records the lock extensions it is asked for.
struct LockExtensions(RequestQueue, Mutex<Vec<(String, Duration)>>);

#[async_trait::async_trait]
impl RequestManager for LockExtensions {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult> {
        self.0.add_requests(requests, forefront).await
    }
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        self.0.fetch_next_request().await
    }
    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>> {
        self.0.mark_request_as_handled(request).await
    }
    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        self.0.reclaim_request(request, forefront).await
    }
    async fn is_empty(&self) -> StorageResult<bool> {
        self.0.is_empty().await
    }
    async fn is_finished(&self) -> StorageResult<bool> {
        self.0.is_finished().await
    }
    async fn handled_count(&self) -> StorageResult<u64> {
        self.0.handled_count().await
    }
    async fn extend_request_processing_time(&self, request: &Request, by: Duration) -> StorageResult<bool> {
        self.1.lock().push((request.url.clone(), by));
        Ok(true)
    }
}

/// `extend_timeout` pushes back the handler's timeout and the request timeout together, and asks
/// the request manager to extend the request's lock (`context.extendTimeout`).
#[tokio::test(start_paused = true)]
async fn extend_timeout_pushes_back_the_handler_and_the_request_timeouts() {
    let services = Services::in_memory();
    let queue = services.open_request_queue(&StorageIdentifier::Default).await.unwrap();
    let manager = Arc::new(LockExtensions(queue, Mutex::new(Vec::new())));
    // Handler timeout 60 s, so a request timeout of 300 s: the handler needs 350 s.
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(manager.clone())
        .max_request_retries(0)
        .request_handler(|ctx: BasicContext| async move {
            tokio::time::sleep(Duration::from_secs(50)).await;
            ctx.extend_timeout(Duration::from_secs(400));
            tokio::time::sleep(Duration::from_secs(300)).await;
            Ok(())
        })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.test/"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed), (1, 0), "{stats:#?}");
    tokio::task::yield_now().await;
    assert_eq!(*manager.1.lock(), [("https://a.test/".to_owned(), Duration::from_secs(400))]);
}

/// Without an extension, the handler times out as before.
#[tokio::test(start_paused = true)]
async fn without_an_extension_the_handler_times_out() {
    let failed = Arc::new(Mutex::new(Vec::new()));
    let log = failed.clone();
    let crawler = BasicCrawler::builder()
        .services(Services::in_memory())
        .max_request_retries(0)
        .request_handler_timeout(Duration::from_secs(10))
        .request_handler(|ctx: BasicContext| async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            // Too little.
            ctx.extend_timeout(Duration::from_secs(2));
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        })
        .failed_request_handler(move |_ctx: BasicContext, err: Arc<anyhow::Error>| {
            let log = log.clone();
            async move {
                log.lock().push(err.to_string());
                Ok(())
            }
        })
        .build()
        .unwrap();
    crawler.run(["https://a.test/"]).await.unwrap();
    assert_eq!(*failed.lock(), ["requestHandler timed out after 10 seconds."]);
}

/// A request queue passes the extension on to a backend that locks requests.
#[tokio::test]
async fn a_request_queue_extends_the_lock_of_a_fetched_request() {
    let dir = tempfile::tempdir().unwrap();
    let configuration = crawlee_core::Configuration { storage_dir: dir.path().to_owned(), ..Default::default() };
    let services = Services::from_configuration(configuration);
    let queue = services.open_request_queue(&StorageIdentifier::Default).await.unwrap();
    queue.add_requests(vec![Request::new("https://a.test/")], false).await.unwrap();
    let request = queue.fetch_next_request().await.unwrap().unwrap();
    assert!(queue.extend_request_processing_time(&request, Duration::from_secs(60)).await.unwrap());
    let unsaved = Request::new("https://b.test/");
    assert!(!queue.extend_request_processing_time(&unsaved, Duration::from_secs(60)).await.unwrap(), "no id");
}
