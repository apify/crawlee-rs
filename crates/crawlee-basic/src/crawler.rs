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
    CriticalError, Dataset, Event, EventKind, KeyValueStore, PacingScope, PacingSignal, RecoverableState, Request,
    RequestManager, SerdeState, Services, StatusLevel, StatusMessage, StorageIdentifier,
};
use crawlee_http_client::HttpClient;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::autoscaling::{ConcurrencyError, ConcurrencyOptions, ConcurrencySystem};
use crate::context::{BasicContext, CrawlerShared, CrawlingContext, InFlight, RequestScope};
use crate::enqueue::{
    CrawlerAddRequestsOptions, SkipReason, SkippedRequest, SkippedRequestCallback, skipped_request_callback,
};
use crate::errors::{
    ErrorKind, HandlerPanic, MissingSessionError, RequestHandlerTimeout, RequestSkipped, SkipRequestError,
    error_message,
};
use crate::handler::{BoxFuture, ErrorHandler, Identity, Middleware, RequestHandler};
use crate::proxy::ProxySource;
use crate::robots::RobotsCache;
use crate::session::{Session, SessionPool, SessionPoolOptions};
use crate::statistics::{FinalStatistics, Statistics};
use crate::throttling::{ThrottleBy, ThrottledDomains, ThrottlingOptions, ThrottlingRequestManager};

/// How often the task loop checks whether a task can start (`maybeRunIntervalSecs`).
const MAYBE_RUN_INTERVAL: Duration = Duration::from_millis(500);
/// Releases the session-specific proxy of a session nothing will use again, once the attempt
/// (with its error handlers) is over, however it ends.
struct ReleaseProxy {
    client: Arc<dyn HttpClient>,
    session: Arc<Session>,
}

impl Drop for ReleaseProxy {
    fn drop(&mut self) {
        if let Some(proxy) = self.session.proxy_info().filter(|proxy| proxy.session_specific) {
            self.client.release_proxy(&proxy.url);
        }
    }
}

/// How long a migration or abort waits for running requests (`SAFE_MIGRATION_WAIT_MILLIS`).
const SAFE_MIGRATION_WAIT: Duration = Duration::from_secs(20);
/// How often the statistics are logged (`logIntervalSecs`).
const STATISTICS_LOG_INTERVAL: Duration = Duration::from_secs(60);
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
    /// Keep running when the queue is empty, waiting for more requests, until
    /// [`stop`](BasicCrawler::stop) is called (`keepAlive`).
    pub keep_alive: bool,
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
            keep_alive: false,
        }
    }
}

/// A predicate of the task loop: see [`TaskLoopOptions`].
pub type TaskLoopPredicate = Arc<dyn Fn() -> BoxFuture<anyhow::Result<bool>> + Send + Sync>;

/// Replacements of the task loop's predicates (`taskLoopOptions` of Crawlee for JS). The
/// `max_requests_per_crawl` limit, [`stop`](BasicCrawler::stop) and
/// [`keep_alive`](CrawlerOptions::keep_alive) still apply.
#[derive(Clone, Default)]
pub struct TaskLoopOptions {
    /// Whether a task may start now; by default, whether the request manager has a request.
    pub is_task_ready: Option<TaskLoopPredicate>,
    /// Whether the crawl is over once nothing runs and nothing is ready; by default, whether
    /// the request manager is finished.
    pub is_finished: Option<TaskLoopPredicate>,
}

impl std::fmt::Debug for TaskLoopOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskLoopOptions")
            .field("is_task_ready", &self.is_task_ready.is_some())
            .field("is_finished", &self.is_finished.is_some())
            .finish()
    }
}

/// What a [`StatusMessageCallback`] gets (`statusMessageCallback` of Crawlee for JS).
#[derive(Clone, Debug)]
pub struct StatusMessageInfo {
    /// The message the crawler would set.
    pub message: String,
    pub state: FinalStatistics,
    /// The statistics at the previous status message.
    pub previous_state: FinalStatistics,
}

/// Called instead of [`BasicCrawler::set_status_message`] every
/// [`status_message_interval`](CrawlerOptions::status_message_interval).
pub type StatusMessageCallback = Arc<dyn Fn(StatusMessageInfo) -> BoxFuture<()> + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("a request handler is required: call `request_handler()` or `router()`")]
    MissingRequestHandler,
    #[error(transparent)]
    Concurrency(#[from] ConcurrencyError),
    #[error(
        "the `min_concurrency`/`max_concurrency`/`desired_concurrency`/`max_requests_per_minute` shortcuts cannot be \
         combined with `concurrency_system()`: they configure the default ConcurrencySystem that a supplied one \
         replaces. Pass them to the ConcurrencySystem instead."
    )]
    ConcurrencyShortcutsWithSystem,
}

struct Inner<P: Middleware<BasicContext>> {
    pipeline: P,
    handler: Arc<dyn RequestHandler<P::Out>>,
    error_handler: Option<Arc<dyn ErrorHandler>>,
    failed_request_handler: Option<Arc<dyn ErrorHandler>>,
    on_skipped_request: Option<SkippedRequestCallback>,
    status_message_callback: Option<StatusMessageCallback>,
    task_loop: TaskLoopOptions,
    options: CrawlerOptions,
    /// The explicit id, or the instantiation index.
    id: String,
    has_explicit_id: bool,
    instance_index: u64,
    services: Services,
    request_manager: Option<Arc<dyn RequestManager>>,
    http_client: Arc<dyn HttpClient>,
    session_pool: Arc<SessionPool>,
    /// The session pool was built by the crawler, not supplied.
    owns_session_pool: bool,
    statistics: Arc<Statistics>,
    owns_statistics: bool,
    statistics_state: RecoverableState<Statistics>,
    session_pool_state: RecoverableState<SessionPool>,
    /// A supplied concurrency system; otherwise one is built for every run.
    injected_concurrency: Option<Arc<ConcurrencySystem>>,
    /// The concurrency system of the last run.
    concurrency: parking_lot::Mutex<Option<Arc<ConcurrencySystem>>>,
    /// The run in progress.
    run: parking_lot::Mutex<Option<Arc<RunState>>>,
    has_finished_before: std::sync::atomic::AtomicBool,
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
    status_message_callback: Option<StatusMessageCallback>,
    task_loop: TaskLoopOptions,
    options: CrawlerOptions,
    services: Option<Services>,
    request_manager: Option<Arc<dyn RequestManager>>,
    http_client: Option<Arc<dyn HttpClient>>,
    proxies: Option<Arc<dyn ProxySource>>,
    statistics: Option<Arc<Statistics>>,
    session_pool: Option<Arc<SessionPool>>,
    concurrency_system: Option<Arc<ConcurrencySystem>>,
    /// A concurrency shortcut was set, which conflicts with a supplied concurrency system.
    concurrency_shortcuts: bool,
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
            status_message_callback: None,
            task_loop: TaskLoopOptions::default(),
            options: CrawlerOptions::default(),
            services: None,
            request_manager: None,
            http_client: None,
            proxies: None,
            statistics: None,
            session_pool: None,
            concurrency_system: None,
            concurrency_shortcuts: false,
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
        F: Fn(SkippedRequest) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.on_skipped_request = Some(skipped_request_callback(callback));
        self
    }

    /// Called instead of [`set_status_message`](BasicCrawler::set_status_message) for the periodic
    /// status message (`statusMessageCallback`).
    pub fn status_message_callback<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(StatusMessageInfo) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.status_message_callback = Some(Arc::new(move |info| Box::pin(callback(info))));
        self
    }

    /// See [`TaskLoopOptions`].
    pub fn task_loop_options(mut self, options: TaskLoopOptions) -> Self {
        self.task_loop = options;
        self
    }

    /// See [`CrawlerOptions::keep_alive`].
    pub fn keep_alive(mut self, keep_alive: bool) -> Self {
        self.options.keep_alive = keep_alive;
        self
    }

    /// Statistics to record into instead of the crawler's own (`statistics`). Unlike the crawler's
    /// own, they are not reset when the crawler runs again.
    pub fn statistics(mut self, statistics: Arc<Statistics>) -> Self {
        self.statistics = Some(statistics);
        self
    }

    /// A session pool to take sessions from instead of the crawler's own (`sessionPool`). Its
    /// sessions keep their proxies: [`proxy_configuration`](Self::proxy_configuration) does not
    /// apply to it. Unlike the crawler's own, it is not reset when the crawler runs again.
    pub fn session_pool(mut self, pool: Arc<SessionPool>) -> Self {
        self.session_pool = Some(pool);
        self
    }

    /// A concurrency system to book requests against instead of one the crawler builds for every
    /// run (`concurrencySystem`), for example one shared by several crawlers. The caller starts it
    /// before running the crawler and stops it after.
    pub fn concurrency_system(mut self, system: Arc<ConcurrencySystem>) -> Self {
        self.concurrency_system = Some(system);
        self
    }

    pub fn options(mut self, options: CrawlerOptions) -> Self {
        self.options = options;
        self
    }

    /// At most this many requests in parallel (`maxConcurrency`, default 200).
    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.concurrency_shortcuts = true;
        self.options.concurrency.max_concurrency = max_concurrency;
        self.options.concurrency.min_concurrency = self.options.concurrency.min_concurrency.min(max_concurrency);
        self
    }

    /// At least this many requests in parallel, whatever the load (`minConcurrency`, default 1).
    pub fn min_concurrency(mut self, min_concurrency: usize) -> Self {
        self.concurrency_shortcuts = true;
        self.options.concurrency.min_concurrency = min_concurrency;
        self
    }

    /// The concurrency autoscaling starts from (`initialConcurrency`).
    pub fn desired_concurrency(mut self, desired_concurrency: usize) -> Self {
        self.concurrency_shortcuts = true;
        self.options.concurrency.desired_concurrency = Some(desired_concurrency);
        self
    }

    /// A fixed concurrency: `min_concurrency = max_concurrency = concurrency`.
    pub fn fixed_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency_shortcuts = true;
        self.options.concurrency.min_concurrency = concurrency;
        self.options.concurrency.max_concurrency = concurrency;
        self
    }

    /// Starts at most this many requests per minute (`maxRequestsPerMinute`).
    pub fn max_requests_per_minute(mut self, max: u64) -> Self {
        self.concurrency_shortcuts = true;
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
        if self.concurrency_system.is_some() && self.concurrency_shortcuts {
            return Err(BuildError::ConcurrencyShortcutsWithSystem);
        }
        if self.concurrency_system.is_none() {
            // Validated here; the system itself is built for every run.
            ConcurrencySystem::new(self.options.concurrency.clone())?;
        }
        static INSTANCE_COUNT: AtomicU64 = AtomicU64::new(0);
        let instance_index = INSTANCE_COUNT.fetch_add(1, Ordering::Relaxed);
        let has_explicit_id = self.options.id.is_some();
        let id = self.options.id.clone().unwrap_or_else(|| instance_index.to_string());

        let services = self.services.unwrap_or_else(|| Services::global().clone());
        if self.session_pool.is_some() && self.proxies.is_some() {
            tracing::warn!(
                "Both `session_pool` and `proxy_configuration` were provided to the crawler. The \
                 `proxy_configuration` is ignored - sessions from the supplied pool keep whatever proxy they were \
                 created with. Configure proxies on the pool instead."
            );
        }
        let owns_session_pool = self.session_pool.is_none();
        let session_pool = self
            .session_pool
            .unwrap_or_else(|| Arc::new(SessionPool::new(self.options.session_pool.clone(), self.proxies)));
        let owns_statistics = self.statistics.is_none();
        let statistics = self.statistics.unwrap_or_else(|| Arc::new(Statistics::with_id(id.clone())));
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
                status_message_callback: self.status_message_callback,
                task_loop: self.task_loop,
                options: self.options,
                id,
                has_explicit_id,
                instance_index,
                services,
                request_manager: self.request_manager,
                http_client: self.http_client.unwrap_or_else(crawlee_http_client::default_client),
                session_pool,
                owns_session_pool,
                statistics,
                owns_statistics,
                statistics_state,
                session_pool_state,
                injected_concurrency: self.concurrency_system,
                concurrency: parking_lot::Mutex::new(None),
                run: parking_lot::Mutex::new(None),
                has_finished_before: Default::default(),
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
                    // The first crawler of the process uses the default queue, the others queues of
                    // their own, so that they do not collide.
                    None => {
                        let identifier = match inner.instance_index {
                            0 => StorageIdentifier::Default,
                            _ => StorageIdentifier::Alias(format!("__default_{}__", inner.id)),
                        };
                        let queue = inner.services.open_request_queue(&identifier).await?;
                        // So that `add_requests_from_url` downloads with the crawler's HTTP client.
                        if !queue.has_url_list_fetcher() {
                            queue.set_url_list_fetcher(Arc::new(HttpUrlListFetcher(inner.http_client.clone())));
                        }
                        Arc::new(queue)
                    }
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
                // Locked for slightly longer than the longest handler timeout, and at least a minute,
                // so that a request is not handed out again while it is processed
                // (`applyRequestManagerTimeouts`).
                let handler_timeout =
                    inner.options.request_handler_timeout.max(inner.handler.max_timeout().unwrap_or_default());
                let lock_time = (handler_timeout + Duration::from_secs(5)).max(Duration::from_secs(60));
                request_manager.set_expected_request_processing_time(lock_time).await?;
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
                    robots: inner.options.respect_robots_txt.clone().map(|user_agent| {
                        RobotsCache::new(user_agent)
                            .with_sessions(inner.session_pool.clone(), inner.options.use_session_pool)
                    }),
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
        if self.live_run().is_none() {
            tracing::warn!("Cannot pause a crawler that is not running.");
            return Ok(());
        }
        let shared = self.shared().await?;
        shared.paused.store(true, Ordering::Release);
        tokio::time::timeout(timeout, shared.wait_until_drained())
            .await
            .map_err(|_| anyhow::anyhow!("pausing the crawler timed out: running tasks did not finish in time"))
    }

    pub async fn resume(&self) -> anyhow::Result<()> {
        if self.live_run().is_none() {
            tracing::warn!("Cannot resume a crawler that is not running.");
            return Ok(());
        }
        let shared = self.shared().await?;
        shared.paused.store(false, Ordering::Release);
        shared.resumed.notify_waiters();
        Ok(())
    }

    /// Stops the run gracefully: no new request starts, and the run ends once the running ones
    /// are finished. Requests still in the queue are left there. `reason` is logged, by default
    /// "The crawler has been gracefully stopped.".
    pub fn stop(&self, reason: Option<&str>) {
        match self.live_run() {
            Some(run) => run.stop(reason.unwrap_or("The crawler has been gracefully stopped.")),
            None => tracing::warn!("Cannot stop a crawler that is not running."),
        }
    }

    /// Whether [`run`](Self::run) is in progress.
    pub fn running(&self) -> bool {
        self.live_run().is_some()
    }

    fn live_run(&self) -> Option<Arc<RunState>> {
        self.inner.run.lock().clone()
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
    fn spawn_status_logger(
        &self,
        shared: Arc<CrawlerShared>,
        concurrency: Arc<ConcurrencySystem>,
    ) -> tokio::task::JoinHandle<()> {
        let crawler = self.clone();
        let interval = self.inner.options.status_message_interval;
        tokio::spawn(async move {
            let statistics = &crawler.inner.statistics;
            let mut previous = statistics.snapshot();
            let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(1)));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let state = statistics.snapshot();
                let failed_delta = state.requests_failed.saturating_sub(previous.requests_failed);
                let message = if failed_delta > 0 {
                    format!(
                        "Experiencing problems, {failed_delta} failed requests in the past {} seconds.",
                        interval.as_secs()
                    )
                } else {
                    let total = shared.request_manager.total_count().await.unwrap_or(0);
                    let total = if total > 0 { format!("/{total}") } else { String::new() };
                    format!(
                        "Crawled {}{total} pages, {} failed requests, desired concurrency {}.",
                        state.requests_succeeded,
                        state.requests_failed,
                        concurrency.desired_concurrency()
                    )
                };
                let previous_state = std::mem::replace(&mut previous, state.clone());
                match &crawler.inner.status_message_callback {
                    Some(callback) => callback(StatusMessageInfo { message, state, previous_state }).await,
                    None => crawler.set_status_message(message, StatusLevel::Debug, false),
                }
            }
        })
    }

    /// Logs the statistics every minute until the returned task is aborted (`logIntervalSecs`).
    fn spawn_statistics_logger(&self) -> tokio::task::JoinHandle<()> {
        let statistics = self.inner.statistics.clone();
        let message = format!("{} request statistics:", crawler_name::<P::Out>());
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(STATISTICS_LOG_INTERVAL);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let stats = statistics.snapshot();
                tracing::info!(
                    request_avg_failed_duration_millis = stats.request_avg_failed_duration_millis,
                    request_avg_succeeded_duration_millis = stats.request_avg_succeeded_duration_millis,
                    requests_succeeded_per_minute = stats.requests_succeeded_per_minute,
                    requests_failed_per_minute = stats.requests_failed_per_minute,
                    request_total_duration_millis = stats.request_total_duration_millis,
                    requests_total = stats.requests_total,
                    crawler_runtime_millis = stats.crawler_runtime_millis,
                    retry_histogram = ?stats.retry_histogram,
                    "{message}"
                );
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

    /// Adds `requests` and crawls until the queue is finished (or a limit is reached, or
    /// [`stop`](Self::stop) is called).
    ///
    /// Running a crawler again continues with the same request queue: requests handled before are
    /// not crawled again. The crawler's own statistics and session pool start over.
    ///
    /// Returns an error only for a critical failure (a [`CriticalError`], a missing route, an
    /// error handler that failed or a storage failure); failed requests are reported in the
    /// statistics.
    ///
    /// [`CriticalError`]: crawlee_core::CriticalError
    pub async fn run<I, R>(&self, requests: I) -> anyhow::Result<FinalStatistics>
    where
        I: IntoIterator<Item = R>,
        R: Into<Request>,
    {
        let inner = &self.inner;
        let run = {
            let mut live = inner.run.lock();
            if live.is_some() {
                anyhow::bail!(
                    "This crawler instance is already running, you can add more requests to it via \
                     `crawler.add_requests()`."
                );
            }
            // A crawler-built concurrency system is built for every run, so that it starts from a
            // clean slate; a supplied one belongs to the caller.
            let concurrency = match &inner.injected_concurrency {
                Some(system) => system.clone(),
                None => Arc::new(ConcurrencySystem::new(inner.options.concurrency.clone())?),
            };
            *inner.concurrency.lock() = Some(concurrency.clone());
            let run = Arc::new(RunState::new(concurrency));
            *live = Some(run.clone());
            run
        };
        // Lets the crawler run again however this run ends.
        let _live = LiveRun(&inner.run);
        let concurrency = run.concurrency.clone();
        let owns_concurrency = inner.injected_concurrency.is_none();
        let events = &inner.services.events;
        let owns_event_manager = !events.is_initialized();

        let started = async {
            if inner.has_finished_before.load(Ordering::Acquire) {
                // Only what the crawler built starts over; what was supplied keeps its state.
                if inner.owns_statistics {
                    inner.statistics_state.reset();
                    inner.statistics_state.reset_store().await?;
                }
                if inner.owns_session_pool {
                    inner.session_pool_state.reset();
                    inner.session_pool_state.reset_store().await?;
                }
            }
            let shared = self.shared().await?.clone();
            if owns_event_manager {
                events.init().await;
            }
            // Loads the state of a previous run, if the crawl is resuming.
            inner.statistics_state.initialize().await?;
            if inner.options.use_session_pool {
                inner.session_pool_state.initialize().await?;
            }
            self.add_requests(requests).await?;
            if owns_concurrency {
                concurrency.start(&inner.services);
            } else if !concurrency.is_running() {
                return Err(anyhow::Error::from(CriticalError::new(
                    "The ConcurrencySystem this crawler borrows has not been started, so system load would not be \
                     monitored and the concurrency would never be adjusted. Whoever creates a ConcurrencySystem owns \
                     its lifecycle: start it before running the crawlers that use it, and stop it once they are all \
                     done.",
                )));
            }
            inner.statistics.start();
            Ok::<_, anyhow::Error>(shared)
        };
        let shared = match started.await {
            Ok(shared) => shared,
            Err(err) => {
                // Cleaned up, so that a failed start does not leave anything running.
                if owns_concurrency {
                    concurrency.stop();
                }
                if owns_event_manager && events.is_initialized() {
                    events.close().await;
                }
                return Err(err);
            }
        };

        tracing::info!(
            min_concurrency = concurrency.min_concurrency(),
            max_concurrency = concurrency.max_concurrency(),
            desired_concurrency = concurrency.desired_concurrency(),
            "Starting the crawler."
        );
        let status_logger = self.spawn_status_logger(shared.clone(), concurrency.clone());
        let statistics_logger = self.spawn_statistics_logger();
        self.set_status_message("Starting the crawler.", StatusLevel::Info, false);

        let pause_listeners = [EventKind::Migrating, EventKind::Aborting].map(|kind| {
            let crawler = self.clone();
            events.on(kind, move |_| {
                let crawler = crawler.clone();
                async move { crawler.pause_on_migration().await }
            })
        });
        let sigint = sigint::listen();
        let sigint_handler = {
            let (crawler, run) = (self.clone(), run.clone());
            tokio::spawn(async move {
                sigint.notified().await;
                tracing::warn!(
                    "Pausing... Press CTRL+C again to force exit. To resume, do: CRAWLEE_PURGE_ON_START=0 cargo run"
                );
                crawler.pause_on_migration().await;
                run.abort();
            })
        };

        let mut result = self.task_loop(&shared, &run).await;
        sigint_handler.abort();
        statistics_logger.abort();
        inner.statistics.finish();
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

        let stats = inner.statistics.snapshot();
        tracing::info!(
            requests_succeeded = stats.requests_succeeded,
            requests_failed = stats.requests_failed,
            requests_retries = stats.requests_retries,
            request_avg_succeeded_duration_millis = stats.request_avg_succeeded_duration_millis,
            request_avg_failed_duration_millis = stats.request_avg_failed_duration_millis,
            requests_total = stats.requests_total,
            crawler_runtime_millis = stats.crawler_runtime_millis,
            retry_histogram = ?stats.retry_histogram,
            "Final request statistics:"
        );
        if stats.requests_total == 0 {
            // Starting against handled requests is resuming a crawl; doing nothing then is a mistake.
            let already_handled = shared.request_manager.handled_count().await.unwrap_or(0);
            static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
            if already_handled > 0 && !WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "This crawl processed no requests - the request manager holds {already_handled} request{}, all of \
                     them already handled, and a failed request counts as handled too. Nothing empties a queue \
                     between runs, so to crawl them again, purge it or use a fresh one (e.g. a request queue opened \
                     with an alias) with a freshly created crawler instance.",
                    if already_handled == 1 { "" } else { "s" }
                );
            }
        }
        let total_errors: u64 = stats.errors.values().sum();
        if total_errors > 0 {
            let mut most_common: Vec<(&String, &u64)> = stats.errors.iter().collect();
            most_common.sort_by(|a, b| b.1.cmp(a.1));
            let most_common: Vec<String> =
                most_common.iter().take(3).map(|(message, count)| format!("{count}x: {message}")).collect();
            tracing::info!(
                total_errors,
                unique_errors = stats.errors.len(),
                most_common_errors = ?most_common,
                "Error analysis:"
            );
        }

        if let Err(err) = inner.services.storage.teardown().await {
            tracing::warn!("Failed to tear down the storage: {err}");
            result = result.and(Err(err.into()));
        }

        status_logger.abort();
        self.set_status_message(
            format!(
                "Finished! Total {} requests: {} succeeded, {} failed.",
                stats.requests_total, stats.requests_succeeded, stats.requests_failed
            ),
            StatusLevel::Info,
            true,
        );
        if owns_concurrency {
            concurrency.stop();
        }
        inner.has_finished_before.store(true, Ordering::Release);

        match result {
            Ok(()) => Ok(stats),
            Err(err) => {
                tracing::error!("Crawler stopped by a critical error: {err:#}");
                Err(err)
            }
        }
    }

    /// The task loop (`AutoscaledPool` of Crawlee for JS): while there is capacity and a request
    /// is ready, a task is started, and the task fetches the request itself, so that fetches
    /// overlap. Readiness is checked again as soon as a task starts or ends, when requests are
    /// added, when the request manager said a request would be ready, and every 500 ms
    /// (`maybeRunIntervalSecs`). Once no task runs and none is ready, the crawl ends if the
    /// request manager is finished.
    async fn task_loop(&self, shared: &Arc<CrawlerShared>, run: &RunState) -> anyhow::Result<()> {
        let concurrency = &run.concurrency;
        let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();
        let mut wake_at: Option<tokio::time::Instant> = None;

        loop {
            while let Some(joined) = tasks.try_join_next() {
                Self::check_task(joined, &mut tasks)?;
            }
            if run.aborted.load(Ordering::Acquire) {
                // Ends the run without waiting for the running requests.
                tasks.abort_all();
                return Ok(());
            }

            // `maybeRunTask`; a paused crawler starts nothing and does not finish.
            if !shared.paused.load(Ordering::Acquire) {
                let ready =
                    concurrency.can_start_task() && self.is_task_ready(shared, run, tasks.len(), &mut wake_at).await?;
                if ready && concurrency.try_register_task_start() {
                    let crawler = self.clone();
                    let shared = shared.clone();
                    let slot = ConcurrencySlot(concurrency.clone());
                    let in_flight = InFlight::new(shared.clone());
                    tasks.spawn(async move {
                        let _slot = slot;
                        let _in_flight = in_flight;
                        crawler.run_task(shared).await
                    });
                    // Try the next one right away, to build up concurrency.
                    continue;
                }
                // `maybeFinish`
                if !ready && tasks.is_empty() && self.is_finished(shared, run).await? {
                    return Ok(());
                }
            }

            let added = shared.requests_added.notified();
            let resumed = shared.resumed.notified();
            let nudged = run.nudge.notified();
            let wake = async {
                match wake_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                Some(joined) = tasks.join_next(), if !tasks.is_empty() => Self::check_task(joined, &mut tasks)?,
                () = added => {}
                () = resumed => {}
                () = nudged => {}
                () = wake => wake_at = None,
                () = tokio::time::sleep(MAYBE_RUN_INTERVAL) => {}
            }
        }
    }

    /// Whether a task may start: the limits are not reached and the request manager has a
    /// request (`isTaskReadyFunction`). A manager that will have one later sets `wake_at`.
    async fn is_task_ready(
        &self,
        shared: &CrawlerShared,
        run: &RunState,
        in_flight: usize,
        wake_at: &mut Option<tokio::time::Instant>,
    ) -> anyhow::Result<bool> {
        // In-flight requests count too, so that the limit is never exceeded.
        if let Some(max) = self.inner.options.max_requests_per_crawl
            && self.inner.statistics.requests_finished() + in_flight as u64 >= max
        {
            run.log_once("shuttingDown", || {
                format!(
                    "Crawler reached the maxRequestsPerCrawl limit of {max} requests and will shut down soon. \
                     Requests that are in progress will be allowed to finish."
                )
            });
            return Ok(false);
        }
        if run.stop_requested.load(Ordering::Acquire) {
            run.log_once("shuttingDown", || {
                "No new requests are allowed because the `stop()` method has been called. Ongoing requests will be \
                 allowed to complete."
                    .to_owned()
            });
            return Ok(false);
        }
        if let Some(is_task_ready) = &self.inner.task_loop.is_task_ready {
            return is_task_ready().await;
        }
        let manager = &shared.request_manager;
        if !manager.is_empty().await? {
            return Ok(true);
        }
        if let Some(at) = manager.ready_at() {
            let at = tokio::time::Instant::from_std(at);
            // A single wake-up, which only an earlier one replaces (`scheduleWake`).
            if wake_at.is_none_or(|current| at < current) {
                *wake_at = Some(at);
            }
        }
        Ok(false)
    }

    /// Whether the crawl is over, asked once nothing runs and nothing is ready
    /// (`isFinishedFunction`).
    async fn is_finished(&self, shared: &CrawlerShared, run: &RunState) -> anyhow::Result<bool> {
        if let Some(max) = self.inner.options.max_requests_per_crawl
            && self.inner.statistics.requests_finished() >= max
        {
            tracing::info!(
                "Earlier, the crawler reached the maxRequestsPerCrawl limit of {max} requests and all requests that \
                 were in progress at that time have now finished. In total, the crawler processed {} requests and \
                 will shut down.",
                self.inner.statistics.requests_finished()
            );
            return Ok(true);
        }
        if run.stop_requested.load(Ordering::Acquire) {
            tracing::info!("The crawler has finished all the remaining ongoing requests and will shut down now.");
            return Ok(true);
        }
        // With `keep_alive`, an empty queue is waited out.
        if self.inner.options.keep_alive {
            return Ok(false);
        }
        let finished = match &self.inner.task_loop.is_finished {
            Some(is_finished) => is_finished().await?,
            None => shared.request_manager.is_finished().await?,
        };
        if finished {
            match self.inner.task_loop.is_finished {
                Some(_) => {
                    tracing::info!("Crawler's custom isFinishedFunction() returned true, the crawler will shut down.")
                }
                None => tracing::info!("All requests from the queue have been processed, the crawler will shut down."),
            }
        }
        Ok(finished)
    }

    /// One task: fetches a request and processes it (`runTaskFunction`).
    async fn run_task(&self, shared: Arc<CrawlerShared>) -> anyhow::Result<()> {
        let internal_timeout = self.internal_timeout();
        let manager = shared.request_manager.clone();
        let fetched = timeout_and_retry(
            internal_timeout,
            &format!("Fetching next request timed out after {} seconds.", internal_timeout.as_secs_f64()),
            || async { Ok(manager.fetch_next_request().await?) },
        )
        .await?;
        let Some(mut request) = fetched else { return Ok(()) };
        // An old one must not be carried over to a retry.
        request.loaded_url = None;
        let span = tracing::info_span!("request", url = %request.url);
        self.process_request(shared, request).instrument(span).await
    }

    /// The timeout of the crawler's own operations: storage calls, error handling, and the
    /// request as a whole (`internalTimeoutMillis`): twice the handler timeout, at least 5 min.
    fn internal_timeout(&self) -> Duration {
        (self.inner.options.request_handler_timeout * 2).max(Duration::from_secs(300))
    }

    /// The concurrency system of the run in progress, or of the last run; `None` before the
    /// first. A crawler-built one is built for every run, so do not keep it across runs.
    pub fn concurrency_system(&self) -> Option<Arc<ConcurrencySystem>> {
        self.inner.concurrency.lock().clone()
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

    /// The session of a request (`resolveSession`): the one it is pinned to, or one from the pool.
    /// A pinned session the pool does not have (or that is no longer usable) is an error.
    fn session_for(&self, request: &Request) -> Result<Arc<Session>, MissingSessionError> {
        if !self.inner.options.use_session_pool {
            return Ok(self.inner.session_pool.detached_session());
        }
        match request.session_id() {
            Some(id) => self
                .inner
                .session_pool
                .get_session_by_id(id)
                .ok_or_else(|| MissingSessionError { session_id: id.to_owned() }),
            None => Ok(self.inner.session_pool.get_session()),
        }
    }

    /// Processes one request. Returns `Err` only for errors that must stop the crawl.
    async fn process_request(&self, shared: Arc<CrawlerShared>, request: Request) -> anyhow::Result<()> {
        let started = Instant::now();
        let internal_timeout = self.internal_timeout();

        if !shared.is_allowed_by_robots(&request.url).await {
            tracing::warn!(
                "Skipping request {} ({}) because it is disallowed based on robots.txt",
                request.url,
                request.id.as_deref().unwrap_or_default()
            );
            let skipped = SkippedRequest { request, reason: SkipReason::RobotsTxt, message: None };
            return match self.try_skip_request(&shared, skipped).await {
                Ok(()) => Ok(()),
                Err(failed) => {
                    let (request, err) = *failed;
                    self.handle_request_failure(&shared, None, request, err, started).await
                }
            };
        }

        let session = match timeout_and_retry(
            internal_timeout,
            &format!("Fetching session timed out after {} seconds.", internal_timeout.as_secs_f64()),
            || std::future::ready(self.session_for(&request).map_err(anyhow::Error::from)),
        )
        .await
        {
            Ok(session) => session,
            Err(err) => return self.handle_request_failure(&shared, None, request, err, started).await,
        };
        // Without a session pool, nothing uses the session's proxy after this attempt.
        let _release_proxy = (!self.inner.options.use_session_pool)
            .then(|| ReleaseProxy { client: shared.http_client.clone(), session: session.clone() });
        let scope = RequestScope::new(shared.clone(), Some(session.clone()));
        // Fallback in case the handler leaks its context into a task that outlives it.
        let snapshot = request.clone();

        // The phases have timeouts of their own, but what is between them has not: the internal
        // timeout is the catch-all (`withRequestTimeout`).
        let request_timeout = internal_timeout.max(self.handler_timeout(request.label()));
        let request_id = request.id.clone().unwrap_or_default();
        let attempt = async {
            self.run_pipeline_and_handler(BasicContext::new(request, scope.clone())).await?;
            // Committed before the request is marked as handled, so that a failed commit fails it.
            Ok::<_, anyhow::Error>(scope.transaction.commit().await?)
        };
        let mut result = match tokio::time::timeout(request_timeout, attempt).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "Request timed out after {} seconds ({request_id}).",
                request_timeout.as_secs_f64()
            )),
        };
        if result.is_err() {
            scope.transaction.rollback();
        }
        let mut request = scope.take_returned().unwrap_or(snapshot);

        if result.is_ok() {
            let manager = shared.request_manager.clone();
            let marked = timeout_and_retry(
                internal_timeout,
                &format!(
                    "Marking request {} ({}) as handled timed out after {} seconds.",
                    request.url,
                    request.id.as_deref().unwrap_or_default(),
                    internal_timeout.as_secs_f64()
                ),
                || {
                    let (manager, mut request) = (manager.clone(), request.clone());
                    async move {
                        manager.mark_request_as_handled(&mut request).await?;
                        Ok(request)
                    }
                },
            )
            .await;
            match marked {
                Ok(marked) => request = marked,
                Err(err) => result = Err(err),
            }
        }

        match result {
            Ok(()) => {
                session.mark_good();
                shared.statistics.record_success(started.elapsed(), request.retry_count);
                Ok(())
            }
            Err(err) => self.handle_request_failure(&shared, Some(&session), request, err, started).await,
        }
    }

    /// The request handler timeout of a request: its route's, or the crawler's.
    fn handler_timeout(&self, label: Option<&str>) -> Duration {
        self.inner.handler.timeout_for(label).unwrap_or(self.inner.options.request_handler_timeout)
    }

    /// Runs the context pipeline, then the request handler with its timeout.
    async fn run_pipeline_and_handler(&self, ctx: BasicContext) -> anyhow::Result<()> {
        let ctx = match AssertUnwindSafe(self.inner.pipeline.run(ctx)).catch_unwind().await {
            Ok(result) => result?,
            Err(panic) => return Err(panic_error(panic)),
        };

        let timeout = self.handler_timeout(ctx.basic().request().label());
        match tokio::time::timeout(timeout, AssertUnwindSafe(self.inner.handler.handle(ctx)).catch_unwind()).await {
            Ok(Ok(result)) => result,
            Ok(Err(panic)) => Err(panic_error(panic)),
            Err(_) => Err(RequestHandlerTimeout { secs: timeout.as_secs_f64() }.into()),
        }
    }

    /// A request failed anywhere: in the pipeline, the handler, or while being marked as handled
    /// (`handleRequestFailure`). A skip is settled as such; anything else goes to the error
    /// handling.
    async fn handle_request_failure(
        &self,
        shared: &Arc<CrawlerShared>,
        session: Option<&Arc<Session>>,
        request: Request,
        err: anyhow::Error,
        started: Instant,
    ) -> anyhow::Result<()> {
        let (request, err) = match skip_of(&err) {
            Some((reason, message)) => {
                match self.try_skip_request(shared, SkippedRequest { request, reason, message }).await {
                    Ok(()) => return Ok(()),
                    // A failing skip makes this an ordinary failure, retried.
                    Err(failed) => *failed,
                }
            }
            None => (request, err),
        };
        self.run_error_handler(shared, session, request, err, started).await
    }

    /// Settles a skipped request: reports it, then marks it as handled (`trySkipRequest`).
    /// Returns the request and the error that prevented that.
    async fn try_skip_request(
        &self,
        shared: &Arc<CrawlerShared>,
        skipped: SkippedRequest,
    ) -> Result<(), Box<(Request, anyhow::Error)>> {
        let internal_timeout = self.internal_timeout();
        let mut request = skipped.request.clone();
        let description = format!("{} ({})", request.url, request.id.as_deref().unwrap_or_default());
        if tokio::time::timeout(internal_timeout, shared.handle_skipped_request(skipped)).await.is_err() {
            let message = format!(
                "onSkippedRequest for {description} timed out after {} seconds.",
                internal_timeout.as_secs_f64()
            );
            return Err(Box::new((request, anyhow::anyhow!(message))));
        }
        request.no_retry = true;
        let manager = shared.request_manager.clone();
        let marked = timeout_and_retry(
            internal_timeout,
            &format!(
                "Marking request {description} as handled timed out after {} seconds.",
                internal_timeout.as_secs_f64()
            ),
            || {
                let (manager, mut request) = (manager.clone(), request.clone());
                async move {
                    manager.mark_request_as_handled(&mut request).await?;
                    Ok(())
                }
            },
        )
        .await;
        match marked {
            Ok(()) => {
                shared.statistics.record_skipped();
                Ok(())
            }
            Err(err) => Err(Box::new((request, err))),
        }
    }

    /// Runs the error handling with the internal timeout (`runErrorHandler`). An error handling
    /// that times out retries the request; one that fails otherwise stops the crawl, after the
    /// request is given back to the queue.
    async fn run_error_handler(
        &self,
        shared: &Arc<CrawlerShared>,
        session: Option<&Arc<Session>>,
        mut request: Request,
        err: anyhow::Error,
        started: Instant,
    ) -> anyhow::Result<()> {
        let internal_timeout = self.internal_timeout();
        let kind = ErrorKind::of(&err);
        let retry_count_before = request.retry_count;
        let handled = tokio::time::timeout(
            internal_timeout,
            self.request_function_error_handler(shared, session, &mut request, err, kind, started),
        )
        .await;
        let timed_out = handled.is_err();
        let secondary = match handled {
            Ok(Ok(())) => None,
            Ok(Err(secondary)) => {
                if ErrorKind::of(&secondary) != ErrorKind::Critical {
                    tracing::error!(
                        "An exception occurred during handling of failed request. This places the crawler and its \
                         underlying storages into an unknown state and crawling will be terminated. {secondary:#}"
                    );
                }
                Some(secondary)
            }
            Err(_) => {
                // Not worth stopping the crawl for. The request is retried, and the retry counted, so that
                // an error handling that always times out does not retry it forever.
                tracing::warn!(
                    "Handling request failure of {} ({}) timed out after {} seconds.",
                    request.url,
                    request.id.as_deref().unwrap_or_default(),
                    internal_timeout.as_secs_f64()
                );
                if request.retry_count == retry_count_before {
                    request.retry_count += 1;
                }
                None
            }
        };
        if secondary.is_some() || timed_out {
            // Neither marked as handled nor reclaimed (e.g. after a `CriticalError`): given back to
            // the queue. Reclaiming a request that is no longer in progress does nothing.
            let reclaim = shared.request_manager.reclaim_request(&request, false);
            let _ = tokio::time::timeout(internal_timeout, reclaim).await;
        }
        if let Some(secondary) = secondary {
            return Err(secondary);
        }
        // An error that retires the session (or is the domain's) does not count against it.
        if !kind.absolves_session()
            && let Some(session) = session
        {
            session.mark_bad();
        }
        Ok(())
    }

    /// The retry logic of `requestFunctionErrorHandler` in Crawlee for JS. Returns `Err` for an
    /// error that stops the crawl: a `CriticalError`, or an error handler that failed.
    async fn request_function_error_handler(
        &self,
        shared: &Arc<CrawlerShared>,
        session: Option<&Arc<Session>>,
        request: &mut Request,
        error: anyhow::Error,
        kind: ErrorKind,
        started: Instant,
    ) -> anyhow::Result<()> {
        let manager = &shared.request_manager;

        if kind == ErrorKind::Throttled {
            // Never really attempted: no retry is spent and the session is not blamed.
            tracing::debug!("Deferring request because its domain is rate-limiting us. {error:#}");
            manager.reclaim_request(request, true).await?;
            return Ok(());
        }

        let message = error_message(&error);
        request.push_error_message(message.clone());

        if kind == ErrorKind::Critical {
            return Err(error);
        }

        let error = Arc::new(error);

        if self.can_be_retried(request, kind) {
            shared.statistics.record_retry(&message);
            if let Some(handler) = &self.inner.error_handler
                && let Err(handler_error) =
                    self.call_error_handler(handler, shared, session, request, error.clone()).await
            {
                let Some((reason, skip_message)) = skip_of(&handler_error) else {
                    return Err(handler_error);
                };
                let skipped = SkippedRequest { request: request.clone(), reason, message: skip_message };
                match self.try_skip_request(shared, skipped).await {
                    Ok(()) => return Ok(()),
                    // Retried for the original error: routing this one through the error handler
                    // again would loop forever with a handler that always skips.
                    Err(failed) => {
                        tracing::warn!("Skipping request {} failed, retrying it instead. {:#}", request.url, failed.1)
                    }
                }
            }
            if kind == ErrorKind::Session
                && let Some(session) = session
            {
                session.retire();
            }
            if !request.no_retry {
                request.retry_count += 1;
                tracing::warn!(
                    retry_count = request.retry_count,
                    "Reclaiming failed request back to the list or queue. {message}"
                );
                manager.reclaim_request(request, false).await?;
                return Ok(());
            }
        }

        if kind == ErrorKind::Session
            && let Some(session) = session
        {
            session.retire();
        }
        shared.statistics.record_error(&message);
        manager.mark_request_as_handled(request).await?;
        shared.statistics.record_failure(started.elapsed(), request.retry_count);
        tracing::error!("Request failed and reached maximum retries. {message}");

        if let Some(handler) = &self.inner.failed_request_handler
            && let Err(handler_error) = self.call_error_handler(handler, shared, session, request, error).await
        {
            if skip_of(&handler_error).is_none() {
                return Err(handler_error);
            }
            tracing::warn!(
                "skipRequest() has no effect in failedRequestHandler, request {} ({}) has already failed.",
                request.url,
                request.id.as_deref().unwrap_or_default()
            );
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
    /// succeeds. Changes it makes to the request are kept, whatever the outcome.
    async fn call_error_handler(
        &self,
        handler: &Arc<dyn ErrorHandler>,
        shared: &Arc<CrawlerShared>,
        session: Option<&Arc<Session>>,
        request: &mut Request,
        error: Arc<anyhow::Error>,
    ) -> anyhow::Result<()> {
        let scope = RequestScope::new(shared.clone(), session.cloned());
        let outcome = AssertUnwindSafe(handler.handle(BasicContext::new(request.clone(), scope.clone()), error))
            .catch_unwind()
            .await;
        if let Some(returned) = scope.take_returned() {
            *request = returned;
        }
        match outcome {
            Ok(Ok(())) => Ok(scope.transaction.commit().await?),
            Ok(Err(err)) => {
                scope.transaction.rollback();
                Err(err)
            }
            Err(panic) => {
                scope.transaction.rollback();
                Err(panic_error(panic))
            }
        }
    }
}

/// The reason and message of an error that skips a request: a skip by the handler, or one by
/// the context pipeline (a redirect out of the enqueue strategy).
fn skip_of(error: &anyhow::Error) -> Option<(SkipReason, Option<String>)> {
    error.chain().find_map(|cause| {
        if let Some(skip) = cause.downcast_ref::<SkipRequestError>() {
            Some((SkipReason::Manual, skip.message.clone()))
        } else if let Some(skip) = cause.downcast_ref::<RequestSkipped>() {
            tracing::debug!("Skipping request: {}", skip.reason);
            Some((SkipReason::Redirect, None))
        } else {
            None
        }
    })
}

/// Runs `operation` with `timeout`, and up to 3 more times when it fails or times out
/// (`timeoutAndRetry`).
async fn timeout_and_retry<T, F, Fut>(timeout: Duration, timeout_message: &str, mut operation: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    const MAX_RETRIES: u32 = 3;
    let mut retried = 1;
    loop {
        let result = match tokio::time::timeout(timeout, operation()).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("{timeout_message}")),
        };
        match result {
            Ok(value) => return Ok(value),
            Err(err) if retried <= MAX_RETRIES => {
                // Retried on any error, not just a timeout.
                tracing::warn!("{err:#} (retrying {retried}/{MAX_RETRIES})");
                retried += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

/// What one `run()` keeps (`CrawlerRun`).
struct RunState {
    concurrency: Arc<ConcurrencySystem>,
    stop_requested: std::sync::atomic::AtomicBool,
    /// Set by a SIGINT: the run ends without waiting for the running requests.
    aborted: std::sync::atomic::AtomicBool,
    /// Wakes the task loop.
    nudge: Notify,
    logged: parking_lot::Mutex<std::collections::HashSet<&'static str>>,
}

impl RunState {
    fn new(concurrency: Arc<ConcurrencySystem>) -> Self {
        RunState {
            concurrency,
            stop_requested: Default::default(),
            aborted: Default::default(),
            nudge: Notify::new(),
            logged: Default::default(),
        }
    }

    /// Only the first call logs `reason`.
    fn stop(&self, reason: &str) {
        if !self.stop_requested.swap(true, Ordering::AcqRel) {
            tracing::info!("{reason}");
        }
        self.nudge.notify_one();
    }

    fn abort(&self) {
        self.aborted.store(true, Ordering::Release);
        self.nudge.notify_one();
    }

    /// Logs `message` the first time this run asks for it under `key` (`logOnce`).
    fn log_once(&self, key: &'static str, message: impl FnOnce() -> String) {
        if self.logged.lock().insert(key) {
            tracing::info!("{}", message());
        }
    }
}

/// Clears the run in progress when dropped.
struct LiveRun<'a>(&'a parking_lot::Mutex<Option<Arc<RunState>>>);

impl Drop for LiveRun<'_> {
    fn drop(&mut self) {
        self.0.lock().take();
    }
}

/// `CheerioCrawler` for a pipeline that ends in `CheerioContext`, as `this.constructor.name`.
fn crawler_name<C>() -> String {
    let context = std::any::type_name::<C>().rsplit("::").next().unwrap_or("Basic");
    format!("{}Crawler", context.strip_suffix("Context").unwrap_or(context))
}

/// SIGINT (Ctrl+C) handling. The first one during a run pauses it, saves its state and ends it;
/// a second one exits the process, as does one when no crawler runs, which is what SIGINT did
/// before the handler was installed.
mod sigint {
    use std::sync::{Arc, Weak};

    use parking_lot::Mutex;
    use tokio::sync::Notify;

    /// Woken when the run that registered it gets a SIGINT.
    pub(super) struct Sigint {
        notify: Arc<Notify>,
        _registration: Registration,
    }

    impl Sigint {
        /// Waits for a SIGINT.
        pub(super) async fn notified(&self) {
            self.notify.notified().await;
        }
    }

    struct Registration(Weak<Notify>);

    impl Drop for Registration {
        fn drop(&mut self) {
            LISTENERS.lock().retain(|listener| !listener.ptr_eq(&self.0));
        }
    }

    static LISTENERS: Mutex<Vec<Weak<Notify>>> = Mutex::new(Vec::new());
    static WATCHER: Mutex<Option<tokio::task::JoinHandle<()>>> = Mutex::new(None);

    pub(super) fn listen() -> Sigint {
        let notify = Arc::new(Notify::new());
        LISTENERS.lock().push(Arc::downgrade(&notify));
        let mut watcher = WATCHER.lock();
        if watcher.as_ref().is_none_or(|task| task.is_finished()) {
            *watcher = Some(tokio::spawn(watch()));
        }
        Sigint { _registration: Registration(Arc::downgrade(&notify)), notify }
    }

    async fn watch() {
        let mut pausing = false;
        while tokio::signal::ctrl_c().await.is_ok() {
            let listeners: Vec<Arc<Notify>> = LISTENERS.lock().iter().filter_map(Weak::upgrade).collect();
            if listeners.is_empty() || pausing {
                std::process::exit(130);
            }
            pausing = true;
            for listener in listeners {
                listener.notify_one();
            }
        }
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

/// Downloads the URL lists of `RequestQueue::add_requests_from_url` with an [`HttpClient`]
/// (`downloadListOfUrls` in JS, which reads the body whatever the status).
struct HttpUrlListFetcher(Arc<dyn HttpClient>);

#[async_trait::async_trait]
impl crawlee_core::UrlListFetcher for HttpUrlListFetcher {
    async fn fetch(
        &self,
        url: &str,
        proxy_url: Option<&str>,
    ) -> Result<Vec<u8>, crawlee_core::recoverable_state::BoxError> {
        let options = crawlee_http_client::SendOptions {
            proxy_url: proxy_url.map(url::Url::parse).transpose()?,
            ..Default::default()
        };
        let request = crawlee_http_client::HttpRequest::get(url::Url::parse(url)?);
        Ok(self.0.send_request(request, &options).await?.body.to_vec())
    }
}
