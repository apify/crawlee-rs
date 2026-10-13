//! The crawling context handed to request handlers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Notify;

use crawlee_core::storage::backend::BatchAddRequestsResult;
use crawlee_core::{
    Dataset, KeyValueStore, PacingScope, PacingSignal, Request, RequestManager, SerdeState, Services,
    StorageTransaction,
};
use crawlee_http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, SendOptions};

use crate::errors::RequestSkipped;
use crate::proxy::ProxyInfo;
use crate::robots::RobotsCache;
use crate::session::Session;
use crate::statistics::Statistics;

/// Everything request tasks of one crawler share.
pub struct CrawlerShared {
    pub services: Services,
    pub request_manager: Arc<dyn RequestManager>,
    pub dataset: Dataset,
    pub key_value_store: KeyValueStore,
    pub http_client: Arc<dyn HttpClient>,
    pub statistics: Arc<Statistics>,
    pub max_crawl_depth: Option<u32>,
    /// Timeout for `send_request` from handlers.
    pub send_request_timeout: Duration,
    /// Woken when requests are added, so an idle crawler picks them up immediately.
    pub requests_added: Notify,
    /// Key of [`BasicContext::use_state`]: `CRAWLEE_STATE`, or `CRAWLEE_STATE_{id}` for a crawler
    /// with an explicit id.
    pub(crate) state_key: String,
    /// Set by `pause()`: the task loop starts no new requests.
    pub(crate) paused: AtomicBool,
    /// Woken by `resume()`.
    pub(crate) resumed: Notify,
    /// Requests being processed right now.
    pub(crate) in_flight: AtomicUsize,
    /// Woken when `in_flight` drops to zero.
    pub(crate) drained: Notify,
    /// robots.txt files, when the crawler respects them.
    pub(crate) robots: Option<RobotsCache>,
    /// Domains already warned about (unpaced 429s and crawl delays).
    pub(crate) warned: Mutex<std::collections::HashSet<String>>,
}

fn hostname_of(url: &str) -> String {
    url::Url::parse(url).ok().and_then(|u| u.host_str().map(str::to_owned)).unwrap_or_else(|| url.to_owned())
}

impl CrawlerShared {
    fn warn_once(&self, key: String, message: impl FnOnce() -> String) {
        if self.warned.lock().insert(key) {
            tracing::warn!("{}", message());
        }
    }

    /// Whether robots.txt lets the crawler visit `url` (always, when robots.txt is not
    /// respected). A `Crawl-delay` is handed to the request manager.
    pub(crate) async fn is_allowed_by_robots(&self, url: &str) -> bool {
        let Some(cache) = &self.robots else { return true };
        let Some(robots) = cache.get(url, self.http_client.as_ref()).await else { return true };
        if let Some(delay) = robots.crawl_delay(cache.user_agent()).filter(|d| d.is_finite() && *d > 0.0) {
            let signal = PacingSignal::MinInterval {
                url: url.to_owned(),
                interval: Duration::from_secs_f64(delay),
                scope: Some(PacingScope::Hostname),
            };
            if !self.request_manager.record_pacing_signal(&signal) {
                let domain = hostname_of(url);
                self.warn_once(format!("crawl-delay:{domain}"), || {
                    format!(
                        "robots.txt for \"{domain}\" defines a crawl-delay of {delay}s, but the crawler's request \
                         manager does not pace that domain, so its requests will not be paced. Set \
                         same_domain_delay, or use a ThrottlingRequestManager covering \"{domain}\"."
                    )
                });
            }
        }
        robots.is_allowed(url, cache.user_agent())
    }

    /// Offers a 429 to the request manager. `true` when it paces the domain; the request is then
    /// deferred with a `RequestThrottledError` instead of blaming the session.
    pub fn record_domain_rate_limit(&self, url: &str, wait: Option<Duration>) -> bool {
        if self.request_manager.record_pacing_signal(&PacingSignal::RateLimited { url: url.to_owned(), wait }) {
            return true;
        }
        let domain = hostname_of(url);
        self.warn_once(format!("429:{domain}"), || {
            format!(
                "\"{domain}\" responded with HTTP 429 (Too Many Requests), but the crawler's request manager does \
                 not pace that domain, so the response is handled like any other, with no per-domain delay. Set \
                 same_domain_delay, or use a ThrottlingRequestManager covering \"{domain}\", to honour \
                 Retry-After and apply exponential backoff instead."
            )
        });
        false
    }

    /// Waits until no request is being processed.
    pub(crate) async fn wait_until_drained(&self) {
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            drained.await;
        }
    }
}

/// Counts a request as in flight while alive.
pub(crate) struct InFlight(pub(crate) Arc<CrawlerShared>);

impl InFlight {
    pub(crate) fn new(shared: Arc<CrawlerShared>) -> Self {
        shared.in_flight.fetch_add(1, Ordering::AcqRel);
        InFlight(shared)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_waiters();
        }
    }
}

/// Per-request state shared between the crawler and the context.
pub struct RequestScope {
    pub(crate) shared: Arc<CrawlerShared>,
    pub(crate) session: Option<Arc<Session>>,
    pub(crate) transaction: StorageTransaction,
    /// The context hands its request back here when it is dropped.
    returned: Mutex<Option<Request>>,
}

impl RequestScope {
    pub(crate) fn new(shared: Arc<CrawlerShared>, session: Option<Arc<Session>>) -> Arc<Self> {
        Arc::new(RequestScope { shared, session, transaction: StorageTransaction::new(), returned: Mutex::new(None) })
    }

    pub(crate) fn take_returned(&self) -> Option<Request> {
        self.returned.lock().take()
    }
}

/// The context of `BasicCrawler` and the base of every other crawler's context.
///
/// Handlers receive their context by value. The context owns the [`Request`]; when the context is
/// dropped (the handler finished, failed or timed out) it hands the request back to the crawler,
/// so changes a handler makes to it (for example to `user_data`) are kept for retries.
///
/// Storage writes made through the context are part of the request's transaction: they are
/// applied only if the handler succeeds.
pub struct BasicContext {
    request: Option<Request>,
    scope: Arc<RequestScope>,
}

impl std::fmt::Debug for BasicContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicContext").field("request", &self.request).finish_non_exhaustive()
    }
}

impl Drop for BasicContext {
    fn drop(&mut self) {
        if let Some(request) = self.request.take() {
            *self.scope.returned.lock() = Some(request);
        }
    }
}

const PRESENT: &str = "the request is present until the context is dropped";

impl BasicContext {
    pub(crate) fn new(request: Request, scope: Arc<RequestScope>) -> Self {
        BasicContext { request: Some(request), scope }
    }

    pub fn request(&self) -> &Request {
        self.request.as_ref().expect(PRESENT)
    }

    /// The error that skips the current request: return it from the handler (`return
    /// Err(ctx.skip_request(None))`, or use `?`). The request is marked as handled and counted as
    /// skipped, without a retry or a hit to the session's score (`context.skipRequest()` in JS,
    /// which throws instead).
    pub fn skip_request(&self, message: Option<&str>) -> anyhow::Error {
        let request = self.request();
        tracing::debug!(
            "Skipping request {} ({}){}",
            request.url,
            request.id.as_deref().unwrap_or_default(),
            message.map(|message| format!(": {message}")).unwrap_or_default()
        );
        RequestSkipped { reason: message.unwrap_or("skipped by the request handler").to_owned() }.into()
    }

    pub fn request_mut(&mut self) -> &mut Request {
        self.request.as_mut().expect(PRESENT)
    }

    pub fn session(&self) -> Option<&Arc<Session>> {
        self.scope.session.as_ref()
    }

    pub fn proxy_info(&self) -> Option<&ProxyInfo> {
        self.session().and_then(|session| session.proxy_info())
    }

    /// The crawler's default dataset.
    pub fn dataset(&self) -> &Dataset {
        &self.scope.shared.dataset
    }

    /// The crawler's default key-value store.
    pub fn key_value_store(&self) -> &KeyValueStore {
        &self.scope.shared.key_value_store
    }

    pub fn services(&self) -> &Services {
        &self.scope.shared.services
    }

    pub fn statistics(&self) -> &Statistics {
        &self.scope.shared.statistics
    }

    pub fn http_client(&self) -> &Arc<dyn HttpClient> {
        &self.scope.shared.http_client
    }

    pub fn request_manager(&self) -> &Arc<dyn RequestManager> {
        &self.scope.shared.request_manager
    }

    /// The journal of this attempt's storage writes.
    pub fn transaction(&self) -> &StorageTransaction {
        &self.scope.transaction
    }

    pub(crate) fn scope_shared(&self) -> &Arc<CrawlerShared> {
        &self.scope.shared
    }

    pub(crate) fn max_crawl_depth(&self) -> Option<u32> {
        self.scope.shared.max_crawl_depth
    }

    /// Appends one object (or each object of an array) to the default dataset once the handler
    /// succeeds. The data is serialized immediately.
    pub fn push_data<T: Serialize + ?Sized>(&self, data: &T) -> anyhow::Result<()> {
        self.scope.transaction.push_data(&self.scope.shared.dataset, data)?;
        Ok(())
    }

    /// Like [`push_data`](Self::push_data), into another dataset.
    pub fn push_data_to<T: Serialize + ?Sized>(&self, dataset: &Dataset, data: &T) -> anyhow::Result<()> {
        self.scope.transaction.push_data(dataset, data)?;
        Ok(())
    }

    /// Stores a JSON value in the default key-value store once the handler succeeds.
    pub fn set_value<T: Serialize + ?Sized>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        self.scope.transaction.set_value(&self.scope.shared.key_value_store, key, value)?;
        Ok(())
    }

    /// Reads a JSON value from the default key-value store, seeing this attempt's own writes.
    pub async fn get_value<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<Option<T>> {
        Ok(self.scope.transaction.get_value(&self.scope.shared.key_value_store, key).await?)
    }

    /// Runs `callback` after this attempt's storage writes were committed.
    pub fn after_storage_commit(&self, callback: impl FnOnce() + Send + 'static) {
        self.scope.transaction.after_commit(callback);
    }

    /// Adds requests to the crawler's queue right away (not transactionally, as in Crawlee for
    /// JS). Requests without an explicit depth are one level deeper than the current request.
    pub async fn add_requests<I, R>(&self, requests: I) -> anyhow::Result<BatchAddRequestsResult>
    where
        I: IntoIterator<Item = R>,
        R: Into<Request>,
    {
        let depth = self.request().crawl_depth() + 1;
        let requests: Vec<Request> = requests
            .into_iter()
            .map(Into::into)
            .map(|mut request| {
                request.crawlee.crawl_depth.get_or_insert(depth);
                request
            })
            .collect();
        if requests.is_empty() {
            return Ok(BatchAddRequestsResult::default());
        }
        let result = self.scope.shared.request_manager.add_requests(requests, false).await?;
        self.notify_requests_added();
        Ok(result)
    }

    /// Wakes the task loop if it is waiting for requests.
    pub(crate) fn notify_requests_added(&self) {
        self.scope.shared.requests_added.notify_one();
    }

    /// State shared by every request of the crawl and saved with it, like `useState()` in JS.
    /// It lives in the default key-value store under `CRAWLEE_STATE` (`CRAWLEE_STATE_{id}` for a
    /// crawler with an explicit id), is saved on every `PersistState` event and loaded when a
    /// crawl resumes.
    ///
    /// ```no_run
    /// # async fn handler(ctx: crawlee_basic::BasicContext) -> anyhow::Result<()> {
    /// let state = ctx.use_state(Vec::<String>::new).await?;
    /// state.lock().push(ctx.request().url.clone());
    /// # Ok(()) }
    /// ```
    pub async fn use_state<T>(
        &self,
        default: impl Fn() -> T + Send + Sync + 'static,
    ) -> anyhow::Result<Arc<SerdeState<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        Ok(self.scope.shared.services.auto_saved_value(&self.scope.shared.state_key, default).await?)
    }

    /// See [`CrawlerShared::record_domain_rate_limit`].
    pub fn record_domain_rate_limit(&self, url: &str, wait: Option<Duration>) -> bool {
        self.scope.shared.record_domain_rate_limit(url, wait)
    }

    /// Sends an HTTP request with this request's session (cookies and proxy).
    pub async fn send_request(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        let options = SendOptions {
            proxy_url: self.proxy_info().map(|proxy| proxy.url.clone()),
            cookie_jar: self.session().map(|session| session.cookie_jar().clone()),
            timeout: Some(self.scope.shared.send_request_timeout),
            ..Default::default()
        };
        self.scope.shared.http_client.send_request(request, &options).await
    }
}

/// Implemented by every crawler's context, so generic code (such as the [`Router`](crate::Router))
/// can reach the basic context.
pub trait CrawlingContext: Send + 'static {
    fn basic(&self) -> &BasicContext;
    fn basic_mut(&mut self) -> &mut BasicContext;
}

impl CrawlingContext for BasicContext {
    fn basic(&self) -> &BasicContext {
        self
    }

    fn basic_mut(&mut self) -> &mut BasicContext {
        self
    }
}
