//! Enqueueing found URLs: strategy and pattern filtering, depth limits and request creation.

use std::collections::HashSet;
use std::rc::Rc;

use serde_json::{Map, Value};
use url::Url;

use crawlee_core::Request;
use crawlee_core::storage::backend::ProcessedRequest;
use crawlee_utils::patterns::PatternError;
use crawlee_utils::url::resolve_base_url_for_filtering;
use crawlee_utils::{EnqueueStrategy, UrlFilter, UrlPattern, matches_enqueue_strategy};

use crate::context::BasicContext;

/// Changes a request before it is enqueued, or drops it by returning `None`.
pub type RequestTransform = Rc<dyn Fn(Request) -> Option<Request>>;

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
    /// Enqueue at most this many links.
    pub limit: Option<usize>,
    pub forefront: bool,
    pub transform: Option<RequestTransform>,
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

    pub fn transform(mut self, transform: impl Fn(Request) -> Option<Request> + 'static) -> Self {
        self.transform = Some(Rc::new(transform));
        self
    }
}

/// Why a found URL was not enqueued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipReason {
    /// Not `http(s)` or outside the enqueue strategy.
    Strategy,
    /// Matched an `exclude` pattern or none of the `include` patterns.
    Filters,
    /// Dropped by the transform function.
    Transform,
    /// Deeper than `max_crawl_depth`.
    Depth,
    /// Over the `limit`.
    Limit,
    /// Disallowed by robots.txt.
    RobotsTxt,
}

/// Result of `enqueue_links`.
#[derive(Clone, Debug, Default)]
pub struct EnqueueLinksResult {
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

impl BasicContext {
    /// Filters `urls` the way `enqueue_links` does and adds the survivors to the queue, labeled,
    /// one level deeper than the current request and tagged with the strategy they passed.
    pub async fn enqueue_urls<I>(
        &self,
        urls: I,
        options: &EnqueueLinksOptions,
    ) -> Result<EnqueueLinksResult, EnqueueError>
    where
        I: IntoIterator<Item = Url>,
    {
        let filter = UrlFilter::new(&options.include, &options.exclude)?;
        let request = self.request();
        let depth = request.crawl_depth() + 1;

        let original_url = Url::parse(&request.url).ok();
        let loaded_url = request.loaded_url.as_deref().and_then(|u| Url::parse(u).ok());
        let filter_base = original_url.as_ref().map(|original| {
            resolve_base_url_for_filtering(options.strategy, original, loaded_url.as_ref(), options.base_url.as_ref())
        });

        let mut result = EnqueueLinksResult::default();
        let mut seen = HashSet::new();
        let mut requests = Vec::new();

        for url in urls {
            let allowed_scheme = matches!(url.scheme(), "http" | "https");
            let in_strategy =
                filter_base.as_ref().is_none_or(|base| matches_enqueue_strategy(options.strategy, &url, base));
            if !allowed_scheme || !in_strategy {
                result.skipped.push((url, SkipReason::Strategy));
                continue;
            }
            if !filter.is_allowed(url.as_str()) {
                result.skipped.push((url, SkipReason::Filters));
                continue;
            }
            if !seen.insert(url.as_str().to_owned()) {
                continue;
            }
            if self.max_crawl_depth().is_some_and(|max| depth > max) {
                result.skipped.push((url, SkipReason::Depth));
                continue;
            }
            if !self.scope_shared().is_allowed_by_robots(url.as_str()).await {
                result.skipped.push((url, SkipReason::RobotsTxt));
                continue;
            }

            let mut builder = Request::builder(url.as_str()).crawl_depth(depth).enqueue_strategy(options.strategy);
            if let Some(user_data) = &options.user_data {
                builder = builder.user_data_map(user_data.clone());
            }
            if let Some(label) = &options.label {
                builder = builder.label(label.clone());
            }
            let Ok(new_request) = builder.build() else {
                continue;
            };
            let new_request = match &options.transform {
                Some(transform) => match transform(new_request) {
                    Some(transformed) => transformed,
                    None => {
                        result.skipped.push((url, SkipReason::Transform));
                        continue;
                    }
                },
                None => new_request,
            };

            if options.limit.is_some_and(|limit| requests.len() >= limit) {
                result.skipped.push((url, SkipReason::Limit));
                continue;
            }
            requests.push(new_request);
        }

        if !requests.is_empty() {
            let added =
                self.request_manager().add_requests(requests, options.forefront).await.map_err(anyhow::Error::from)?;
            self.notify_requests_added();
            result.processed_requests = added.processed_requests;
        }
        Ok(result)
    }
}
