//! [`BasicCrawler`]: the task loop, retries and session handling every crawler builds on.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures::FutureExt as _;
use tokio::sync::{Notify, OnceCell};
use tokio::task::JoinSet;
use tracing::Instrument as _;

use crawlee_core::storage::batched_adds::AddRequestsBatchedResult;
use crawlee_core::{
    Dataset, Event, EventKind, KeyValueStore, PacingScope, PacingSignal, RecoverableState, Request, RequestManager,
    SerdeState, Services, StatusLevel, StatusMessage, StorageIdentifier,
};
use crawlee_http_client::HttpClient;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::autoscaling::{ConcurrencyError, ConcurrencyOptions, ConcurrencySystem};
use crate::context::{BasicContext, CrawlerShared, CrawlingContext, InFlight, RequestScope};
use crate::enqueue::{CrawlerAddRequestsOptions, SkipReason, SkippedRequestCallback, skipped_request_callback};
use crate::errors::{ErrorKind, HandlerPanic, RequestHandlerTimeout, error_message};
use crate::handler::{ErrorHandler, Identity, Middleware, RequestHandler};
use crate::proxy::ProxySource;
use crate::robots::RobotsCache;
use crate::session::{Session, SessionPool, SessionPoolOptions};
use crate::statistics::{FinalStatistics, Statistics};
use crate::throttling::{ThrottleBy, ThrottledDomains, ThrottlingOptions, ThrottlingRequestManager};

/// How long the task loop waits for new requests before checking the queue again.
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How often the task loop asks the concurrency system again when it had no capacity
/// (`maybeRunIntervalSecs`).
const MAYBE_RUN_INTERVAL: Duration = Duration::from_millis(500);
/// Releases the proxy of a session nothing will use again, once the attempt (with its error
/// handlers) is over, however it ends.
struct ReleaseProxy {
    client: Arc<dyn HttpClient>,
    session: Arc<Session>,
}

impl Drop for ReleaseProxy {
    fn drop(&mut self) {
        if let Some(proxy) = self.session.proxy_info() {
            self.client.release_proxy(&proxy.url);
        }
    }
}

/// How long a migration or abort waits for running requests (`SAFE_MIGRATION_WAIT_MILLIS`).
const SAFE_MIGRATION_WAIT: Duration = Duration::from_secs(20);
const CRAWLEE_STATE_KEY: &str = "CRAWLEE_STATE";

/// Crawler settings, with the defaults of Crawlee for JS where they exist.
#[derive(Clone, Debug)]
pub struct CrawlerOptions {
    /// How many requests run in parallel: between `min_concurrency` (1) and `max_concurrency`
    /// (200), scaled by the system load. See [`crate::autoscaling`].
    pub concurrency: ConcurrencyOptions,
    /// Retries per request after the first attempt (`maxRequestRetries`).
    pub max_request_retries: u32,
    /// Stop after this many requests finished (`maxRequestsPerCrawl`).
    pub max_requests_per_crawl: Option<u64>,
    /// Links deeper than this are not enqueued (`maxCrawlDepth`).
    pub max_crawl_depth: Option<u32>,
    /// Timeout of the request handler alone (`requestHandlerTimeoutSecs`).
    pub request_handler_timeout: Duration,
    /// Timeout of `send_request` calls made from handlers.
    pub send_request_timeout: Duration,
    pub session_pool: SessionPoolOptions,
    /// Keep sessions and reuse them across requests (`useSessionPool`, default `true`). Without a
    /// pool, every attempt gets a session of its own: a new proxy and no cookies of other
    /// requests, as residential proxies, which do not keep their IP for long, are often used.
    pub use_session_pool: bool,
    /// Identifies the crawler in its persisted state (`CRAWLEE_CRAWLER_STATISTICS_{id}`,
    /// `CRAWLEE_STATE_{id}`) and in status messages. Defaults to the crawler's instantiation
    /// index in the process: `"0"` for the first one.
    pub id: Option<String>,
    /// How often the status message is updated (`statusMessageLoggingInterval`).
    pub status_message_interval: Duration,
    /// Skip what robots.txt disallows for this user agent (`respectRobotsTxtFile`); `None` to
    /// ignore robots.txt. A `Crawl-delay` is honoured when the request manager paces domains.
    pub respect_robots_txt: Option<String>,
    /// Minimum time between two requests to one site, subdomains included
    /// (`sameDomainDelaySecs`). Zero for none.
    pub same_domain_delay: Duration,
}

impl Default for CrawlerOptions {
    fn default() -> Self {
        CrawlerOptions {
            concurrency: ConcurrencyOptions::default(),
            max_request_retries: 3,
            max_requests_per_crawl: None,
            max_crawl_depth: None,
            request_handler_timeout: Duration::from_secs(60),
            send_request_timeout: Duration::from_secs(30),
            session_pool: SessionPoolOptions::default(),
            use_session_pool: true,
            id: None,
            status_message_interval: Duration::from_secs(10),
            respect_robots_txt: None,
            same_domain_delay: Duration::ZERO,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("a request handler is required: call `request_handler()` or `router()`")]
    MissingRequestHandler,
    #[error(transparent)]
    Concurrency(#[from] ConcurrencyError),
}

struct Inner<P: Middleware<BasicContext>> {
    pipeline: P,
    handler: Arc<dyn RequestHandler<P::Out>>,
    error_handler: Option<Arc<dyn ErrorHandler>>,
    failed_request_handler: Option<Arc<dyn ErrorHandler>>,
    on_skipped_request: Option<SkippedRequestCallback>,
    options: CrawlerOptions,
    /// The explicit id, or the instantiation index.
    id: String,
    has_explicit_id: bool,
    services: Services,
    request_manager: Option<Arc<dyn RequestManager>>,
    http_client: Arc<dyn HttpClient>,
    session_pool: Arc<SessionPool>,
    statistics: Arc<Statistics>,
    statistics_state: RecoverableState<Statistics>,
    session_pool_state: RecoverableState<SessionPool>,
    concurrency: Arc<ConcurrencySystem>,
    shared: OnceCell<Arc<CrawlerShared>>,
}

/// A crawler that runs a handler for every request of its queue, with retries, sessions and
/// concurrency control, but without fetching anything itself. [`HttpCrawler`] and friends in
/// `crawlee-http` are `BasicCrawler`s with a pipeline that fetches and parses the page.
///
/// ```no_run
/// use crawlee_basic::{BasicContext, BasicCrawler};
///
/// # async fn run() -> anyhow::Result<()> {
/// let crawler = BasicCrawler::builder()
///     .request_handler(|ctx: BasicContext| async move {
///         ctx.push_data(&serde_json::json!({ "url": ctx.request().url }))?;
///         Ok(())
///     })
///     .build()?;
/// let stats = crawler.run(["https://crawlee.dev"]).await?;
/// println!("{} requests succeeded", stats.requests_succeeded);
/// # Ok(()) }
/// ```
///
/// [`HttpCrawler`]: https://docs.rs/crawlee-http
pub struct BasicCrawler<P: Middleware<BasicContext> = Identity> {
    inner: Arc<Inner<P>>,
}

impl<P: Middleware<BasicContext>> Clone for BasicCrawler<P> {
    fn clone(&self) -> Self {
        BasicCrawler { inner: self.inner.clone() }
    }
}

impl BasicCrawler<Identity> {
    pub fn builder() -> BasicCrawlerBuilder<Identity> {
        BasicCrawlerBuilder::with_pipeline(Identity)
    }
}

/// Builder of [`BasicCrawler`]. Crawlers built on `BasicCrawler` start from
/// [`BasicCrawlerBuilder::with_pipeline`].
#[must_use]
pub struct BasicCrawlerBuilder<P: Middleware<BasicContext>> {
    pipeline: P,
    handler: Option<Arc<dyn RequestHandler<P::Out>>>,
    error_handler: Option<Arc<dyn ErrorHandler>>,
    failed_request_handler: Option<Arc<dyn ErrorHandler>>,
    on_skipped_request: Option<SkippedRequestCallback>,
    options: CrawlerOptions,
    services: Option<Services>,
    request_manager: Option<Arc<dyn RequestManager>>,
    http_client: Option<Arc<dyn HttpClient>>,
    proxies: Option<Arc<dyn ProxySource>>,
}

impl<P> BasicCrawlerBuilder<P>
where
    P: Middleware<BasicContext>,
    P::Out: CrawlingContext,
{
    pub fn with_pipeline(pipeline: P) -> Self {
        BasicCrawlerBuilder {
            pipeline,
            handler: None,
            error_handler: None,
            failed_request_handler: None,
            on_skipped_request: None,
            options: CrawlerOptions::default(),
            services: None,
            request_manager: None,
            http_client: None,
            proxies: None,
        }
    }

    /// The handler run for every request. A [`Router`](crate::Router) is a handler too.
    pub fn request_handler(mut self, handler: impl RequestHandler<P::Out>) -> Self {
        self.handler = Some(Arc::new(handler));
        self
    }

    /// Alias of [`request_handler`](Self::request_handler) for routers.
    pub fn router(self, router: crate::Router<P::Out>) -> Self {
        self.request_handler(router)
    }

    /// Called before each retry of a failed request.
    pub fn error_handler(mut self, handler: impl ErrorHandler) -> Self {
        self.error_handler = Some(Arc::new(handler));
        self
    }

    /// Called once a request failed for the last time.
    pub fn failed_request_handler(mut self, handler: impl ErrorHandler) -> Self {
        self.failed_request_handler = Some(Arc::new(handler));
        self
    }

    /// Called with each request that is not added or not crawled, and why: too deep, disallowed
    /// by robots.txt, filtered out, over a limit, or redirected out of its enqueue strategy
    /// (`onSkippedRequest`).
    pub fn on_skipped_request<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(Request, SkipReason) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.on_skipped_request = Some(skipped_request_callback(callback));
        self
    }

    pub fn options(mut self, options: CrawlerOptions) -> Self {
        self.options = options;
        self
    }

    /// At most this many requests in parallel (`maxConcurrency`, default 200).
    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.options.concurrency.max_concurrency = max_concurrency;
        self.options.concurrency.min_concurrency = self.options.concurrency.min_concurrency.min(max_concurrency);
        self
    }

    /// At least this many requests in parallel, whatever the load (`minConcurrency`, default 1).
    pub fn min_concurrency(mut self, min_concurrency: usize) -> Self {
        self.options.concurrency.min_concurrency = min_concurrency;
        self
    }

    /// The concurrency autoscaling starts from (`initialConcurrency`).
    pub fn desired_concurrency(mut self, desired_concurrency: usize) -> Self {
        self.options.concurrency.desired_concurrency = Some(desired_concurrency);
        self
    }

    /// A fixed concurrency: `min_concurrency = max_concurrency = concurrency`.
    pub fn fixed_concurrency(mut self, concurrency: usize) -> Self {
        self.options.concurrency.min_concurrency = concurrency;
        self.options.concurrency.max_concurrency = concurrency;
        self
    }

    /// Starts at most this many requests per minute (`maxRequestsPerMinute`).
    pub fn max_requests_per_minute(mut self, max: u64) -> Self {
        self.options.concurrency.max_tasks_per_minute = Some(max);
        self
    }

    /// All autoscaling options at once, including the load signals.
    pub fn concurrency_options(mut self, options: ConcurrencyOptions) -> Self {
        self.options.concurrency = options;
        self
    }

    pub fn max_request_retries(mut self, retries: u32) -> Self {
        self.options.max_request_retries = retries;
        self
    }

    pub fn max_requests_per_crawl(mut self, max: u64) -> Self {
        self.options.max_requests_per_crawl = Some(max);
        self
    }

    pub fn max_crawl_depth(mut self, depth: u32) -> Self {
        self.options.max_crawl_depth = Some(depth);
        self
    }

    pub fn request_handler_timeout(mut self, timeout: Duration) -> Self {
        self.options.request_handler_timeout = timeout;
        self
    }

    /// Skip what robots.txt disallows for any user agent (`*`); see
    /// [`CrawlerOptions::respect_robots_txt`].
    pub fn respect_robots_txt(mut self, respect: bool) -> Self {
        self.options.respect_robots_txt = respect.then(|| "*".to_owned());
        self
    }

    /// Like [`respect_robots_txt`](Self::respect_robots_txt), with the rules of this user agent.
    pub fn robots_txt_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.options.respect_robots_txt = Some(user_agent.into());
        self
    }

    /// See [`CrawlerOptions::same_domain_delay`].
    pub fn same_domain_delay(mut self, delay: Duration) -> Self {
        self.options.same_domain_delay = delay;
        self
    }

    /// See [`CrawlerOptions::id`].
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.options.id = Some(id.into());
        self
    }

    pub fn session_pool_options(mut self, options: SessionPoolOptions) -> Self {
        self.options.session_pool = options;
        self
    }

    /// See [`CrawlerOptions::use_session_pool`].
    pub fn use_session_pool(mut self, use_session_pool: bool) -> Self {
        self.options.use_session_pool = use_session_pool;
        self
    }

    /// Configuration and storage of this crawler. Defaults to [`Services::global`]: storages
    /// under `./storage` (or `CRAWLEE_STORAGE_DIR`), purged when the first crawler starts.
    pub fn services(mut self, services: Services) -> Self {
        self.services = Some(services);
        self
    }

    /// Where requests come from. Defaults to the default request queue of the services.
    pub fn request_manager(mut self, manager: Arc<dyn RequestManager>) -> Self {
        self.request_manager = Some(manager);
        self
    }

    pub fn http_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Proxies for new sessions, e.g. a [`ProxyConfiguration`](crate::ProxyConfiguration).
    pub fn proxy_configuration(mut self, proxies: Arc<dyn ProxySource>) -> Self {
        self.proxies = Some(proxies);
        self
    }

    pub fn build(self) -> Result<BasicCrawler<P>, BuildError> {
        let handler = self.handler.ok_or(BuildError::MissingRequestHandler)?;
        let concurrency = Arc::new(ConcurrencySystem::new(self.options.concurrency.clone())?);
        static INSTANCE_COUNT: AtomicU64 = AtomicU64::new(0);
        let instance_index = INSTANCE_COUNT.fetch_add(1, Ordering::Relaxed);
        let has_explicit_id = self.options.id.is_some();
        let id = self.options.id.clone().unwrap_or_else(|| instance_index.to_string());

        let services = self.services.unwrap_or_else(|| Services::global().clone());
        let session_pool = Arc::new(SessionPool::new(self.options.session_pool.clone(), self.proxies));
        let statistics = Arc::new(Statistics::with_id(id.clone()));
        let statistics_state =
            RecoverableState::new(&services, statistics.persist_state_key(), statistics.clone(), true);
        let session_pool_state =
            RecoverableState::new(&services, session_pool.persist_state_key(), session_pool.clone(), true);
        Ok(BasicCrawler {
            inner: Arc::new(Inner {
                pipeline: self.pipeline,
                handler,
                error_handler: self.error_handler,
                failed_request_handler: self.failed_request_handler,
                on_skipped_request: self.on_skipped_request,
                options: self.options,
                id,
                has_explicit_id,
                services,
                request_manager: self.request_manager,
                http_client: self.http_client.unwrap_or_else(crawlee_http_client::default_client),
                session_pool,
                statistics,
                statistics_state,
                session_pool_state,
                concurrency,
                shared: OnceCell::new(),
            }),
        })
    }
}

impl<P> BasicCrawler<P>
where
    P: Middleware<BasicContext>,
    P::Out: CrawlingContext,
{
    async fn shared(&self) -> anyhow::Result<&Arc<CrawlerShared>> {
        let inner = &self.inner;
        inner
            .shared
            .get_or_try_init(|| async {
                // Before anything is opened, so the storages this crawler uses start empty.
                inner.services.purge_on_start().await?;
                let mut request_manager: Arc<dyn RequestManager> = match &inner.request_manager {
                    Some(manager) => manager.clone(),
                    None => Arc::new(inner.services.open_request_queue(&StorageIdentifier::Default).await?),
                };
                let delay = inner.options.same_domain_delay;
                if !delay.is_zero() {
                    // A manager that already paces every domain takes the delay; otherwise one is added.
                    let signal = PacingSignal::MinIntervalEverywhere {
                        interval: delay,
                        scope: Some(PacingScope::RegistrableDomain),
                    };
                    if !request_manager.record_pacing_signal(&signal) {
                        let options = ThrottlingOptions {
                            domains: ThrottledDomains::All,
                            throttle_by: ThrottleBy::RegistrableDomain,
                            min_crawl_delay: delay,
                            persist_state_key: format!("CRAWLEE_THROTTLED_DOMAINS_{}", inner.id),
                            ..ThrottlingOptions::default()
                        };
                        request_manager =
                            Arc::new(ThrottlingRequestManager::new(options, &inner.services, request_manager));
                    }
                }
                let dataset = inner.services.open_dataset(&StorageIdentifier::Default).await?;
                let key_value_store = inner.services.open_key_value_store(&StorageIdentifier::Default).await?;
                request_manager.set_expected_request_processing_time(inner.options.request_handler_timeout * 2).await?;
                Ok::<_, anyhow::Error>(Arc::new(CrawlerShared {
                    services: inner.services.clone(),
                    request_manager,
                    dataset,
                    key_value_store,
                    http_client: inner.http_client.clone(),
                    statistics: inner.statistics.clone(),
                    max_crawl_depth: inner.options.max_crawl_depth,
                    max_requests_per_crawl: inner.options.max_requests_per_crawl,
                    on_skipped_request: inner.on_skipped_request.clone(),
                    send_request_timeout: inner.options.send_request_timeout,
                    requests_added: Notify::new(),
                    state_key: if inner.has_explicit_id {
                        format!("{CRAWLEE_STATE_KEY}_{}", inner.id)
                    } else {
                        CRAWLEE_STATE_KEY.to_owned()
                    },
                    paused: Default::default(),
                    resumed: Notify::new(),
                    in_flight: AtomicUsize::new(0),
                    drained: Notify::new(),
                    robots: inner.options.respect_robots_txt.clone().map(RobotsCache::new),
                    warned: Default::default(),
                }))
            })
            .await
    }

    /// Adds requests to the crawler's queue in batches, as `crawler.addRequests()` in Crawlee for
    /// JS: the first 1000 before returning, the rest in the background, 1000 at a time with a pause
    /// of 1 s after each. The crawler is not finished while they are being added.
    ///
    /// Requests deeper than `max_crawl_depth` or disallowed by robots.txt are skipped, and no more
    /// are added than `max_requests_per_crawl` leaves room for (then every batch is awaited).
    pub async fn add_requests<I, R>(&self, requests: I) -> anyhow::Result<AddRequestsBatchedResult>
    where
        I: IntoIterator<Item = R>,
        R: Into<Request>,
    {
        self.add_requests_with_options(requests, CrawlerAddRequestsOptions::default()).await
    }

    /// [`add_requests`](Self::add_requests) with filters (`include`, `exclude`, `strategy`,
    /// `transform`, `limit`), labels and batching options.
    pub async fn add_requests_with_options<I, R>(
        &self,
        requests: I,
        options: impl Into<CrawlerAddRequestsOptions>,
    ) -> anyhow::Result<AddRequestsBatchedResult>
    where
        I: IntoIterator<Item = R>,
        R: Into<Request>,
    {
        let shared = self.shared().await?;
        let requests: Vec<Request> = requests.into_iter().map(Into::into).collect();
        let (result, _) = shared.add_requests_filtered(requests, &options.into()).await?;
        Ok(result)
    }

    pub fn statistics(&self) -> &Statistics {
        &self.inner.statistics
    }

    pub fn session_pool(&self) -> &SessionPool {
        &self.inner.session_pool
    }

    /// The crawler's id; see [`CrawlerOptions::id`].
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// State shared by the crawl and saved with it; see [`BasicContext::use_state`].
    pub async fn use_state<T>(
        &self,
        default: impl Fn() -> T + Send + Sync + 'static,
    ) -> anyhow::Result<Arc<SerdeState<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let shared = self.shared().await?;
        Ok(self.inner.services.auto_saved_value(&shared.state_key, default).await?)
    }

    /// Logs a status message and emits it as [`Event::StatusMessage`] (which the Apify SDK
    /// forwards to the platform). The crawler sets one every
    /// [`status_message_interval`](CrawlerOptions::status_message_interval).
    pub fn set_status_message(&self, message: impl Into<String>, level: StatusLevel, is_terminal: bool) {
        let message = message.into();
        match level {
            StatusLevel::Debug => tracing::debug!(terminal = is_terminal, "{message}"),
            StatusLevel::Info => tracing::info!(terminal = is_terminal, "{message}"),
            StatusLevel::Warning => tracing::warn!(terminal = is_terminal, "{message}"),
            StatusLevel::Error => tracing::error!(terminal = is_terminal, "{message}"),
        }
        self.inner.services.events.emit(Event::StatusMessage(StatusMessage {
            crawler_id: self.inner.id.clone(),
            message,
            is_terminal,
            level,
        }));
    }

    /// Stops starting new requests and waits (up to `timeout`) for the running ones to finish.
    /// [`run`](Self::run) keeps going once [`resume`](Self::resume) is called.
    pub async fn pause(&self, timeout: Duration) -> anyhow::Result<()> {
        let shared = self.shared().await?;
        shared.paused.store(true, Ordering::Release);
        tokio::time::timeout(timeout, shared.wait_until_drained())
            .await
            .map_err(|_| anyhow::anyhow!("pausing the crawler timed out: running tasks did not finish in time"))
    }

    pub async fn resume(&self) -> anyhow::Result<()> {
        let shared = self.shared().await?;
        shared.paused.store(false, Ordering::Release);
        shared.resumed.notify_waiters();
        Ok(())
    }

    /// On a migration or abort: let running requests finish, then save the statistics.
    async fn pause_on_migration(&self) {
        if self.pause(SAFE_MIGRATION_WAIT).await.is_err() {
            tracing::error!(
                "The crawler was paused due to migration to another host, but some requests did not finish in \
                 time. Those requests' results may be duplicated."
            );
        }
        if let Err(err) = self.inner.statistics_state.persist().await {
            tracing::warn!("Failed to persist the statistics: {err}");
        }
    }

    /// Updates the status message every `status_message_interval` until the returned task is
    /// aborted.
    fn spawn_status_logger(&self) -> tokio::task::JoinHandle<()> {
        let crawler = self.clone();
        let interval = self.inner.options.status_message_interval;
        tokio::spawn(async move {
            let mut previous_failed = crawler.inner.statistics.requests_failed();
            let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(1)));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let (succeeded, failed) =
                    (crawler.inner.statistics.requests_succeeded(), crawler.inner.statistics.requests_failed());
                let failed_delta = failed.saturating_sub(previous_failed);
                previous_failed = failed;
                let message = if failed_delta > 0 {
                    format!(
                        "Experiencing problems, {failed_delta} failed requests in the past {} seconds.",
                        interval.as_secs()
                    )
                } else {
                    format!(
                        "Crawled {succeeded} pages, {failed} failed requests, desired concurrency {}.",
                        crawler.inner.concurrency.desired_concurrency()
                    )
                };
                crawler.set_status_message(message, StatusLevel::Debug, false);
            }
        })
    }

    pub async fn dataset(&self) -> anyhow::Result<Dataset> {
        Ok(self.shared().await?.dataset.clone())
    }

    pub async fn key_value_store(&self) -> anyhow::Result<KeyValueStore> {
        Ok(self.shared().await?.key_value_store.clone())
    }

    pub async fn request_manager(&self) -> anyhow::Result<Arc<dyn RequestManager>> {
        Ok(self.shared().await?.request_manager.clone())
    }

    /// Writes the default dataset to a file, like `crawler.exportData()` in Crawlee for JS: a
    /// JSON array for `.json`, one object per line for `.jsonl`. Returns the number of items.
    pub async fn export_data(&self, path: impl AsRef<std::path::Path>) -> anyhow::Result<usize> {
        let path = path.as_ref();
        let items: Vec<serde_json::Value> = self.dataset().await?.get_all().await?;

        let content = match path.extension().and_then(|ext| ext.to_str()) {
            Some("json") => {
                let mut text = serde_json::to_string_pretty(&items)?;
                text.push('\n');
                text
            }
            Some("jsonl") => {
                let mut text = String::new();
                for item in &items {
                    text.push_str(&serde_json::to_string(item)?);
                    text.push('\n');
                }
                text
            }
            _ => anyhow::bail!("unsupported export format of '{}': use a .json or .jsonl file", path.display()),
        };

        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, content).await?;
        Ok(items.len())
    }

    /// Adds `requests` and crawls until the queue is finished (or a limit is reached).
    ///
    /// Returns an error only for a critical failure (a [`CriticalError`], a missing route or a
    /// storage failure); failed requests are reported in the statistics.
    ///
    /// [`CriticalError`]: crawlee_core::CriticalError
    pub async fn run<I, R>(&self, requests: I) -> anyhow::Result<FinalStatistics>
    where
        I: IntoIterator<Item = R>,
        R: Into<Request>,
    {
        let inner = &self.inner;
        let shared = self.shared().await?.clone();
        let events = &inner.services.events;
        let owns_event_manager = !events.is_initialized();
        if owns_event_manager {
            events.init().await;
        }
        // Loads the state of a previous run, if the crawl is resuming.
        inner.statistics_state.initialize().await?;
        if inner.options.use_session_pool {
            inner.session_pool_state.initialize().await?;
        }

        self.add_requests(requests).await?;
        inner.statistics.start();
        inner.concurrency.start(&inner.services);
        tracing::info!(
            min_concurrency = inner.concurrency.min_concurrency(),
            max_concurrency = inner.concurrency.max_concurrency(),
            desired_concurrency = inner.concurrency.desired_concurrency(),
            "Starting the crawler."
        );
        let status_logger = self.spawn_status_logger();
        self.set_status_message("Starting the crawler.", StatusLevel::Info, false);

        let pause_listeners = [EventKind::Migrating, EventKind::Aborting].map(|kind| {
            let crawler = self.clone();
            events.on(kind, move |_| {
                let crawler = crawler.clone();
                async move { crawler.pause_on_migration().await }
            })
        });

        let mut result = self.task_loop(&shared).await;
        inner.statistics.finish();
        inner.concurrency.stop();
        for id in pause_listeners {
            events.off(id);
        }

        // Save the final state: the statistics and the sessions, then everything else that
        // listens to `PersistState` (such as `use_state`).
        inner.statistics_state.teardown().await;
        if inner.options.use_session_pool {
            inner.session_pool_state.teardown().await;
        }
        if owns_event_manager {
            events.close().await;
        } else {
            events.emit(Event::PersistState { is_migrating: false });
            events.wait_for_all_listeners_to_complete().await;
        }
        if let Err(err) = inner.services.storage.teardown().await {
            tracing::warn!("Failed to tear down the storage: {err}");
            result = result.and(Err(err.into()));
        }

        status_logger.abort();
        let stats = inner.statistics.snapshot();
        self.set_status_message(
            format!(
                "Finished! Total {} requests: {} succeeded, {} failed.",
                stats.requests_total, stats.requests_succeeded, stats.requests_failed
            ),
            StatusLevel::Info,
            true,
        );

        match result {
            Ok(()) => {
                tracing::info!(
                    requests_succeeded = stats.requests_succeeded,
                    requests_failed = stats.requests_failed,
                    requests_retries = stats.requests_retries,
                    crawler_runtime_millis = stats.crawler_runtime_millis,
                    "Crawler finished."
                );
                Ok(stats)
            }
            Err(err) => {
                tracing::error!("Crawler stopped by a critical error: {err:#}");
                Err(err)
            }
        }
    }

    async fn task_loop(&self, shared: &Arc<CrawlerShared>) -> anyhow::Result<()> {
        let options = &self.inner.options;
        let concurrency = &self.inner.concurrency;
        let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();
        let mut limit_logged = false;

        loop {
            while let Some(joined) = tasks.try_join_next() {
                Self::check_task(joined, &mut tasks)?;
            }

            if let Some(max) = options.max_requests_per_crawl {
                let in_flight = tasks.len() as u64;
                if self.inner.statistics.requests_finished() + in_flight >= max {
                    if !limit_logged {
                        tracing::info!(
                            max,
                            "Crawler reached the maxRequestsPerCrawl limit; waiting for running requests."
                        );
                        limit_logged = true;
                    }
                    match tasks.join_next().await {
                        Some(joined) => {
                            Self::check_task(joined, &mut tasks)?;
                            continue;
                        }
                        None => return Ok(()),
                    }
                }
            }

            if shared.paused.load(Ordering::Acquire) {
                let resumed = shared.resumed.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if shared.paused.load(Ordering::Acquire) {
                    tokio::select! {
                        () = resumed => {}
                        Some(joined) = tasks.join_next(), if !tasks.is_empty() => Self::check_task(joined, &mut tasks)?,
                    }
                    continue;
                }
            }

            // No capacity: wait for a task to finish, or ask again shortly (the desired
            // concurrency and the load change over time).
            if !concurrency.can_start_task() {
                tokio::select! {
                    Some(joined) = tasks.join_next(), if !tasks.is_empty() => Self::check_task(joined, &mut tasks)?,
                    () = tokio::time::sleep(MAYBE_RUN_INTERVAL) => {}
                }
                continue;
            }

            match shared.request_manager.fetch_next_request().await? {
                Some(request) => {
                    let crawler = self.clone();
                    let shared = shared.clone();
                    concurrency.register_task_start();
                    let slot = ConcurrencySlot(concurrency.clone());
                    let in_flight = InFlight::new(shared.clone());
                    let span = tracing::info_span!("request", url = %request.url);
                    tasks.spawn(
                        async move {
                            let _slot = slot;
                            let _in_flight = in_flight;
                            crawler.process_request(shared, request).await
                        }
                        .instrument(span),
                    );
                }
                None => {
                    if tasks.is_empty() && shared.request_manager.is_finished().await? {
                        return Ok(());
                    }
                    let added = shared.requests_added.notified();
                    tokio::select! {
                        Some(joined) = tasks.join_next(), if !tasks.is_empty() => Self::check_task(joined, &mut tasks)?,
                        () = added => {}
                        () = tokio::time::sleep(IDLE_POLL_INTERVAL) => {}
                    }
                }
            }
        }
    }

    /// The concurrency system this crawler scales with.
    pub fn concurrency_system(&self) -> &Arc<ConcurrencySystem> {
        &self.inner.concurrency
    }

    fn check_task(
        joined: Result<anyhow::Result<()>, tokio::task::JoinError>,
        tasks: &mut JoinSet<anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        match joined {
            Ok(Ok(())) => Ok(()),
            Ok(Err(critical)) => {
                tasks.abort_all();
                Err(critical)
            }
            Err(join_error) if join_error.is_cancelled() => Ok(()),
            Err(join_error) => {
                tasks.abort_all();
                Err(anyhow::anyhow!("request task failed: {join_error}"))
            }
        }
    }

    fn session_for(&self, request: &Request) -> Arc<Session> {
        if !self.inner.options.use_session_pool {
            return self.inner.session_pool.detached_session();
        }
        request
            .session_id()
            .and_then(|id| self.inner.session_pool.get_session_by_id(id))
            .unwrap_or_else(|| self.inner.session_pool.get_session())
    }

    /// Processes one request. Returns `Err` only for errors that must stop the crawl.
    async fn process_request(&self, shared: Arc<CrawlerShared>, mut request: Request) -> anyhow::Result<()> {
        if !shared.is_allowed_by_robots(&request.url).await {
            tracing::warn!(
                "Skipping request {} ({}) because it is disallowed based on robots.txt",
                request.url,
                request.id.as_deref().unwrap_or_default()
            );
            request.no_retry = true;
            shared.request_manager.mark_request_as_handled(&mut request).await?;
            shared.statistics.record_skipped();
            shared.handle_skipped_request(request, SkipReason::RobotsTxt).await;
            return Ok(());
        }
        let started = Instant::now();
        let session = self.session_for(&request);
        // Without a session pool, nothing uses the session's proxy after this attempt.
        let _release_proxy = (!self.inner.options.use_session_pool)
            .then(|| ReleaseProxy { client: shared.http_client.clone(), session: session.clone() });
        let scope = RequestScope::new(shared.clone(), Some(session.clone()));
        // Fallback in case the handler leaks its context into a task that outlives it.
        let snapshot = request.clone();

        let (result, handler_ran) = self.run_pipeline_and_handler(BasicContext::new(request, scope.clone())).await;
        let mut request = scope.take_returned().unwrap_or(snapshot);

        let result = match result {
            Ok(()) => scope.transaction.commit().await.map_err(anyhow::Error::from),
            Err(err) => {
                scope.transaction.rollback();
                Err(err)
            }
        };

        match result {
            Ok(()) => {
                shared.request_manager.mark_request_as_handled(&mut request).await?;
                session.mark_good();
                shared.statistics.record_success(started.elapsed(), request.retry_count);
                Ok(())
            }
            Err(err) => {
                let kind = ErrorKind::of(&err);
                if kind == ErrorKind::Skipped {
                    tracing::debug!("Skipping request: {err:#}");
                    shared.request_manager.mark_request_as_handled(&mut request).await?;
                    shared.statistics.record_skipped();
                    // Skipped by the context pipeline: redirected out of its enqueue strategy.
                    if !handler_ran {
                        shared.handle_skipped_request(request, SkipReason::Redirect).await;
                    }
                    return Ok(());
                }
                self.handle_request_error(&shared, &session, request, err, kind, started).await?;
                if !kind.absolves_session() {
                    session.mark_bad();
                }
                Ok(())
            }
        }
    }

    /// Runs the context pipeline, then the request handler. Also returns whether the handler ran.
    async fn run_pipeline_and_handler(&self, ctx: BasicContext) -> (anyhow::Result<()>, bool) {
        let ctx = match AssertUnwindSafe(self.inner.pipeline.run(ctx)).catch_unwind().await {
            Ok(Ok(ctx)) => ctx,
            Ok(Err(err)) => return (Err(err), false),
            Err(panic) => return (Err(panic_error(panic)), false),
        };

        let timeout = self.inner.options.request_handler_timeout;
        let result = match tokio::time::timeout(
            timeout,
            AssertUnwindSafe(self.inner.handler.handle(ctx)).catch_unwind(),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(panic)) => Err(panic_error(panic)),
            Err(_) => Err(RequestHandlerTimeout { secs: timeout.as_secs_f64() }.into()),
        };
        (result, true)
    }

    /// The retry logic of `requestFunctionErrorHandler` in Crawlee for JS.
    async fn handle_request_error(
        &self,
        shared: &Arc<CrawlerShared>,
        session: &Arc<Session>,
        mut request: Request,
        error: anyhow::Error,
        kind: ErrorKind,
        started: Instant,
    ) -> anyhow::Result<()> {
        let manager = &shared.request_manager;

        if kind == ErrorKind::Throttled {
            // Never really attempted: no retry is spent and the session is not blamed.
            tracing::debug!("Deferring request because its domain is rate-limiting us. {error:#}");
            manager.reclaim_request(&request, true).await?;
            return Ok(());
        }

        let message = error_message(&error);
        request.push_error_message(message.clone());

        if kind == ErrorKind::Critical {
            return Err(error);
        }

        let error = Arc::new(error);

        if self.can_be_retried(&request, kind) {
            shared.statistics.record_retry(&message);
            if let Some(handler) = &self.inner.error_handler {
                request = self.call_error_handler(handler, shared, session, request, error.clone()).await;
            }
            if kind == ErrorKind::Session {
                session.retire();
            }
            if !request.no_retry {
                request.retry_count += 1;
                tracing::warn!(
                    retry_count = request.retry_count,
                    "Reclaiming failed request back to the queue. {message}"
                );
                manager.reclaim_request(&request, false).await?;
                return Ok(());
            }
        }

        if kind == ErrorKind::Session {
            session.retire();
        }
        shared.statistics.record_error(&message);
        manager.mark_request_as_handled(&mut request).await?;
        shared.statistics.record_failure(started.elapsed(), request.retry_count);
        tracing::error!("Request failed and reached maximum retries. {message}");

        if let Some(handler) = &self.inner.failed_request_handler {
            self.call_error_handler(handler, shared, session, request, error).await;
        }
        Ok(())
    }

    fn can_be_retried(&self, request: &Request, kind: ErrorKind) -> bool {
        if request.no_retry || kind == ErrorKind::NonRetryable {
            return false;
        }
        if kind == ErrorKind::RetryRequest {
            return true;
        }
        request.retry_count < request.max_retries().unwrap_or(self.inner.options.max_request_retries)
    }

    /// Runs an error handler with a fresh context; its storage writes are committed if it
    /// succeeds. Returns the (possibly modified) request.
    async fn call_error_handler(
        &self,
        handler: &Arc<dyn ErrorHandler>,
        shared: &Arc<CrawlerShared>,
        session: &Arc<Session>,
        request: Request,
        error: Arc<anyhow::Error>,
    ) -> Request {
        let scope = RequestScope::new(shared.clone(), Some(session.clone()));
        let snapshot = request.clone();
        let outcome =
            AssertUnwindSafe(handler.handle(BasicContext::new(request, scope.clone()), error)).catch_unwind().await;
        match outcome {
            Ok(Ok(())) => {
                if let Err(err) = scope.transaction.commit().await {
                    tracing::error!("Committing the storage writes of an error handler failed: {err}");
                }
            }
            Ok(Err(err)) => tracing::error!("Error handler failed: {err:#}"),
            Err(panic) => tracing::error!("Error handler panicked: {:#}", panic_error(panic)),
        }
        scope.take_returned().unwrap_or(snapshot)
    }
}

fn panic_error(panic: Box<dyn std::any::Any + Send>) -> anyhow::Error {
    let message = panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_owned());
    HandlerPanic { message }.into()
}

/// A running task as counted by the concurrency system; the end is registered on drop, so a
/// panicking or aborted task frees its slot too.
struct ConcurrencySlot(Arc<ConcurrencySystem>);

impl Drop for ConcurrencySlot {
    fn drop(&mut self) {
        self.0.register_task_end();
    }
}
