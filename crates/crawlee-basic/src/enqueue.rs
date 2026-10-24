//! Adding requests the way `crawler.addRequests()` of Crawlee for JS does: depth limits,
//! robots.txt, strategy and pattern filtering, transforms and the `max_requests_per_crawl`
//! budget. `enqueue_links` is the same, for the links found on a page.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use url::Url;

use crawlee_core::Request;
use crawlee_core::storage::backend::ProcessedRequest;
use crawlee_core::storage::batched_adds::{AddRequestsBatchedOptions, AddRequestsBatchedResult};
use crawlee_utils::patterns::PatternError;
use crawlee_utils::url::resolve_base_url_for_filtering;
use crawlee_utils::{EnqueueStrategy, UrlFilter, UrlPattern, matches_enqueue_strategy};

use crate::context::{BasicContext, CrawlerShared};
use crate::handler::BoxFuture;

/// Changes a request before it is enqueued, or drops it by returning `None`.
pub type RequestTransform = Arc<dyn Fn(Request) -> Option<Request> + Send + Sync>;

/// A request that was not added or not crawled, and why: the argument of
/// [`SkippedRequestCallback`].
#[derive(Clone, Debug)]
pub struct SkippedRequest {
    pub request: Request,
    pub reason: SkipReason,
    /// The message given to [`BasicContext::skip_request`], if any.
    pub message: Option<String>,
}

/// Called with each request that is not added or not crawled (`onSkippedRequest`).
pub type SkippedRequestCallback = Arc<dyn Fn(SkippedRequest) -> BoxFuture<()> + Send + Sync>;

/// A [`SkippedRequestCallback`] from an async closure.
pub fn skipped_request_callback<F, Fut>(callback: F) -> SkippedRequestCallback
where
    F: Fn(SkippedRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |skipped| Box::pin(callback(skipped)))
}

/// Options of `enqueue_links`, mirroring `EnqueueLinksOptions` of Crawlee for JS.
#[derive(Clone, Default)]
#[must_use]
pub struct EnqueueLinksOptions {
    /// CSS selector of the link elements (default `a`). Used by crawlers that extract links.
    pub selector: Option<String>,
    /// Base URL the found links are filtered against (default: the request URL, see
    /// [`resolve_base_url_for_filtering`]).
    pub base_url: Option<Url>,
    pub strategy: EnqueueStrategy,
    pub include: Vec<UrlPattern>,
    pub exclude: Vec<UrlPattern>,
    pub label: Option<String>,
    pub user_data: Option<Map<String, Value>>,
    /// Enqueue at most this many links that are new to the queue.
    pub limit: Option<usize>,
    pub forefront: bool,
    pub transform: Option<RequestTransform>,
    /// Wait until every batch is added; otherwise only the first 1000 links are added before
    /// returning, and the rest in the background.
    pub wait_for_all_requests_to_be_added: bool,
    /// Called with each link that is not enqueued, besides the crawler's own callback.
    pub on_skipped_request: Option<SkippedRequestCallback>,
}

impl std::fmt::Debug for EnqueueLinksOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnqueueLinksOptions")
            .field("selector", &self.selector)
            .field("strategy", &self.strategy)
            .field("include", &self.include)
            .field("exclude", &self.exclude)
            .field("label", &self.label)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl EnqueueLinksOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn selector(mut self, selector: impl Into<String>) -> Self {
        self.selector = Some(selector.into());
        self
    }

    pub fn base_url(mut self, base_url: Url) -> Self {
        self.base_url = Some(base_url);
        self
    }

    pub fn strategy(mut self, strategy: EnqueueStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn include<I, P>(mut self, patterns: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<UrlPattern>,
    {
        self.include.extend(patterns.into_iter().map(Into::into));
        self
    }

    pub fn exclude<I, P>(mut self, patterns: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<UrlPattern>,
    {
        self.exclude.extend(patterns.into_iter().map(Into::into));
        self
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn user_data(mut self, user_data: Map<String, Value>) -> Self {
        self.user_data = Some(user_data);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn forefront(mut self, forefront: bool) -> Self {
        self.forefront = forefront;
        self
    }

    pub fn transform(mut self, transform: impl Fn(Request) -> Option<Request> + Send + Sync + 'static) -> Self {
        self.transform = Some(Arc::new(transform));
        self
    }

    pub fn wait_for_all_requests_to_be_added(mut self, wait: bool) -> Self {
        self.wait_for_all_requests_to_be_added = wait;
        self
    }

    pub fn on_skipped_request<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(SkippedRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_skipped_request = Some(skipped_request_callback(callback));
        self
    }
}

/// Options of `crawler.add_requests_with_options()` and `ctx.add_requests_with_options()`
/// (`CrawlerAddRequestsOptions` of Crawlee for JS): batching, plus the filters of
/// `enqueue_links`. Unlike there, the strategy defaults to [`EnqueueStrategy::All`], and it only
/// filters when a `base_url` is given.
#[derive(Clone)]
#[must_use]
pub struct CrawlerAddRequestsOptions {
    pub forefront: bool,
    /// Wait for every batch to be added, not just the first.
    pub wait_for_all_requests_to_be_added: bool,
    /// Requests per batch. 1000 by default.
    pub batch_size: usize,
    /// Pause after each background batch. 1 s by default.
    pub wait_between_batches: Duration,
    /// Add at most this many requests that are new to the queue.
    pub limit: Option<usize>,
    /// The URL the strategy is checked against.
    pub base_url: Option<Url>,
    pub strategy: Option<EnqueueStrategy>,
    pub include: Vec<UrlPattern>,
    pub exclude: Vec<UrlPattern>,
    /// Sets the label of every request.
    pub label: Option<String>,
    /// The `user_data` of requests that have none.
    pub user_data: Option<Map<String, Value>>,
    pub session_id: Option<String>,
    pub skip_navigation: bool,
    pub transform: Option<RequestTransform>,
    /// Called with each request that is not added, besides the crawler's own callback.
    pub on_skipped_request: Option<SkippedRequestCallback>,
}

impl Default for CrawlerAddRequestsOptions {
    fn default() -> Self {
        AddRequestsBatchedOptions::default().into()
    }
}

impl From<AddRequestsBatchedOptions> for CrawlerAddRequestsOptions {
    fn from(options: AddRequestsBatchedOptions) -> Self {
        CrawlerAddRequestsOptions {
            forefront: options.forefront,
            wait_for_all_requests_to_be_added: options.wait_for_all_requests_to_be_added,
            batch_size: options.batch_size,
            wait_between_batches: options.wait_between_batches,
            limit: options.max_new_requests,
            base_url: None,
            strategy: None,
            include: Vec::new(),
            exclude: Vec::new(),
            label: None,
            user_data: None,
            session_id: None,
            skip_navigation: false,
            transform: None,
            on_skipped_request: None,
        }
    }
}

impl std::fmt::Debug for CrawlerAddRequestsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrawlerAddRequestsOptions")
            .field("forefront", &self.forefront)
            .field("wait_for_all_requests_to_be_added", &self.wait_for_all_requests_to_be_added)
            .field("batch_size", &self.batch_size)
            .field("limit", &self.limit)
            .field("base_url", &self.base_url)
            .field("strategy", &self.strategy)
            .field("include", &self.include)
            .field("exclude", &self.exclude)
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl CrawlerAddRequestsOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn forefront(mut self, forefront: bool) -> Self {
        self.forefront = forefront;
        self
    }

    pub fn wait_for_all_requests_to_be_added(mut self, wait: bool) -> Self {
        self.wait_for_all_requests_to_be_added = wait;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn base_url(mut self, base_url: Url) -> Self {
        self.base_url = Some(base_url);
        self
    }

    pub fn strategy(mut self, strategy: EnqueueStrategy) -> Self {
        self.strategy = Some(strategy);
        self
    }

    pub fn include<I, P>(mut self, patterns: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<UrlPattern>,
    {
        self.include.extend(patterns.into_iter().map(Into::into));
        self
    }

    pub fn exclude<I, P>(mut self, patterns: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<UrlPattern>,
    {
        self.exclude.extend(patterns.into_iter().map(Into::into));
        self
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn user_data(mut self, user_data: Map<String, Value>) -> Self {
        self.user_data = Some(user_data);
        self
    }

    pub fn transform(mut self, transform: impl Fn(Request) -> Option<Request> + Send + Sync + 'static) -> Self {
        self.transform = Some(Arc::new(transform));
        self
    }

    pub fn on_skipped_request<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn(SkippedRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_skipped_request = Some(skipped_request_callback(callback));
        self
    }
}

/// Why a request was not added, or not crawled (`SkippedRequestReason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// Disallowed by robots.txt.
    RobotsTxt,
    /// Over the `limit`, or over what `max_requests_per_crawl` leaves.
    Limit,
    /// Matched an `exclude` pattern or none of the `include` patterns, or is not an `http(s)`
    /// URL within the enqueue strategy.
    Filters,
    /// Dropped by the transform function.
    Transform,
    /// Redirected outside of the enqueue strategy it was enqueued with.
    Redirect,
    /// Deeper than `max_crawl_depth`.
    Depth,
    /// Skipped with [`BasicContext::skip_request`].
    Manual,
}

/// Result of `enqueue_links`.
#[derive(Clone, Debug, Default)]
pub struct EnqueueLinksResult {
    /// The requests added before returning; see
    /// [`EnqueueLinksOptions::wait_for_all_requests_to_be_added`].
    pub processed_requests: Vec<ProcessedRequest>,
    pub skipped: Vec<(Url, SkipReason)>,
}

impl EnqueueLinksResult {
    /// Requests that were new to the queue.
    pub fn newly_added(&self) -> usize {
        self.processed_requests.iter().filter(|p| !p.was_already_present).count()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueError {
    #[error(transparent)]
    Pattern(#[from] PatternError),
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}

impl CrawlerShared {
    /// What `max_requests_per_crawl` leaves to add: the maximum less the finished and the pending
    /// requests, and no more than `explicit_limit` (`#calculateEnqueuedRequestLimit`).
    async fn enqueued_request_limit(&self, explicit_limit: Option<usize>) -> anyhow::Result<Option<usize>> {
        let Some(max) = self.max_requests_per_crawl else {
            return Ok(explicit_limit);
        };
        let pending = self.request_manager.pending_count().await?;
        let limit = max.saturating_sub(self.statistics.requests_finished()).saturating_sub(pending);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        Ok(Some(explicit_limit.map_or(limit, |explicit| explicit.min(limit))))
    }

    /// Reports a skipped request: logs the limits once and calls the crawler's callback
    /// (`#handleSkippedRequest`).
    pub(crate) async fn handle_skipped_request(&self, skipped: SkippedRequest) {
        match skipped.reason {
            SkipReason::Limit => self.info_once("limit", || {
                format!(
                    "The number of requests enqueued by the crawler reached the maxRequestsPerCrawl limit of {} \
                     requests and no further requests will be added.",
                    self.max_requests_per_crawl.unwrap_or_default()
                )
            }),
            SkipReason::Depth => self.info_once("depth", || {
                format!(
                    "The crawler reached the maxCrawlDepth limit of {} and no further requests will be enqueued.",
                    self.max_crawl_depth.unwrap_or_default()
                )
            }),
            _ => {}
        }
        if let Some(callback) = &self.on_skipped_request {
            callback(skipped).await;
        }
    }

    /// `crawler.addRequests()`: filters `requests` by depth, robots.txt, patterns, strategy and
    /// transform, then adds them in batches within the `max_requests_per_crawl` budget. Returns
    /// the result of the add and the skipped requests, already reported.
    pub(crate) async fn add_requests_filtered(
        &self,
        requests: Vec<Request>,
        options: &CrawlerAddRequestsOptions,
    ) -> Result<(AddRequestsBatchedResult, Vec<(Request, SkipReason)>), EnqueueError> {
        let filter = UrlFilter::new(&options.include, &options.exclude)?;
        let request_limit = self.enqueued_request_limit(options.limit).await?;
        let strategy = options.strategy.unwrap_or(EnqueueStrategy::All);

        let mut skipped = Vec::new();
        let mut accepted = Vec::with_capacity(requests.len());
        for mut request in requests {
            if request.user_data.is_empty()
                && let Some(user_data) = &options.user_data
            {
                request.user_data = user_data.clone();
            }
            if let Some(label) = &options.label {
                request.set_label(label.clone());
            }
            if options.skip_navigation {
                request.crawlee.skip_navigation = Some(true);
            }
            if let Some(session_id) = &options.session_id {
                request.crawlee.session_id = Some(session_id.clone());
            }
            // A request keeps the strategy it was enqueued with.
            request.crawlee.enqueue_strategy.get_or_insert(strategy);

            if self.max_crawl_depth.is_some_and(|max| request.crawl_depth() > max) {
                skipped.push((request, SkipReason::Depth));
                continue;
            }
            if !self.is_allowed_by_robots(&request.url).await {
                skipped.push((request, SkipReason::RobotsTxt));
                continue;
            }
            let in_strategy = options.base_url.as_ref().is_none_or(|base| {
                Url::parse(&request.url).is_ok_and(|url| {
                    matches!(url.scheme(), "http" | "https") && matches_enqueue_strategy(strategy, &url, base)
                })
            });
            if !filter.is_allowed(&request.url) || !in_strategy {
                skipped.push((request, SkipReason::Filters));
                continue;
            }
            let request = match &options.transform {
                Some(transform) => match transform(request.clone()) {
                    Some(transformed) => transformed,
                    None => {
                        skipped.push((request, SkipReason::Transform));
                        continue;
                    }
                },
                None => request,
            };
            accepted.push(request);
        }

        let batched = AddRequestsBatchedOptions {
            forefront: options.forefront,
            wait_for_all_requests_to_be_added: options.wait_for_all_requests_to_be_added,
            batch_size: options.batch_size,
            wait_between_batches: options.wait_between_batches,
            max_new_requests: request_limit,
        };
        let result = if accepted.is_empty() {
            AddRequestsBatchedResult::default()
        } else {
            let result =
                self.request_manager.add_requests_batched(accepted, batched).await.map_err(anyhow::Error::from)?;
            self.requests_added.notify_one();
            result
        };
        skipped.extend(result.requests_over_limit.iter().map(|request| (request.clone(), SkipReason::Limit)));

        self.report_skipped_requests(&skipped, options, request_limit).await;
        Ok((result, skipped))
    }

    async fn report_skipped_requests(
        &self,
        skipped: &[(Request, SkipReason)],
        options: &CrawlerAddRequestsOptions,
        request_limit: Option<usize>,
    ) {
        let robots: Vec<&str> = skipped
            .iter()
            .filter(|(_, reason)| *reason == SkipReason::RobotsTxt)
            .map(|(request, _)| request.url.as_str())
            .collect();
        if !robots.is_empty() {
            tracing::warn!(
                skipped = ?robots,
                "Some requests were skipped because they were disallowed based on the robots.txt file"
            );
        }
        // Only for an explicit limit, not the one `max_requests_per_crawl` sets.
        if let Some(limit) = options.limit
            && skipped.iter().any(|(_, reason)| *reason == SkipReason::Limit)
        {
            if request_limit == Some(limit) {
                tracing::info!("Skipping requests in this call due to the enqueueLinks limit of {limit}.");
            } else {
                tracing::info!(
                    "Skipping requests in this call due to the remaining maxRequestsPerCrawl budget of {}, which is \
                     lower than the enqueueLinks limit of {limit}.",
                    request_limit.unwrap_or_default()
                );
            }
        }
        for (request, reason) in skipped {
            let skipped = SkippedRequest { request: request.clone(), reason: *reason, message: None };
            self.handle_skipped_request(skipped.clone()).await;
            if let Some(callback) = &options.on_skipped_request {
                callback(skipped).await;
            }
        }
    }
}

impl BasicContext {
    /// Adds the links found on a page, as `enqueueLinks` does: like
    /// [`add_requests_with_options`](Self::add_requests_with_options), filtered by the strategy
    /// against the base URL (by default the request's, see [`resolve_base_url_for_filtering`]).
    pub async fn enqueue_urls<I>(
        &self,
        urls: I,
        options: &EnqueueLinksOptions,
    ) -> Result<EnqueueLinksResult, EnqueueError>
    where
        I: IntoIterator<Item = Url>,
    {
        let request = self.request();
        let original_url = Url::parse(&request.url).ok();
        let loaded_url = request.loaded_url.as_deref().and_then(|u| Url::parse(u).ok());
        let base_url = original_url.as_ref().map(|original| {
            resolve_base_url_for_filtering(options.strategy, original, loaded_url.as_ref(), options.base_url.as_ref())
        });

        let add_options = CrawlerAddRequestsOptions {
            forefront: options.forefront,
            wait_for_all_requests_to_be_added: options.wait_for_all_requests_to_be_added,
            limit: options.limit,
            base_url,
            strategy: Some(options.strategy),
            include: options.include.clone(),
            exclude: options.exclude.clone(),
            label: options.label.clone(),
            user_data: options.user_data.clone(),
            transform: options.transform.clone(),
            on_skipped_request: options.on_skipped_request.clone(),
            ..CrawlerAddRequestsOptions::default()
        };
        let requests = urls.into_iter().map(|url| Request::new(url.as_str()));
        let (added, skipped) = self.add_requests_filtered(requests, &add_options).await?;
        Ok(EnqueueLinksResult {
            processed_requests: added.added_requests,
            skipped: skipped
                .into_iter()
                .filter_map(|(request, reason)| Some((Url::parse(&request.url).ok()?, reason)))
                .collect(),
        })
    }
}
