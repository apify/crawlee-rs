//! [`ThrottlingRequestManager`]: per-domain pacing, the counterpart of `ThrottlingRequestManager`
//! in Crawlee for JS.
//!
//! - **Queues.** Requests of throttled domains go to a queue of their own, aliased
//!   `throttled-<domain>`. Everything else goes to the inner manager.
//! - **Fetching.** A domain's queue is fetched from only when the domain is not held back. It
//!   can be held back by a 429 backoff or by its crawl delay.
//! - **429 backoff.** Starts at `base_delay` (2 s) and doubles with every consecutive 429, up to
//!   `max_delay` (60 s). A `Retry-After` header sets the delay instead. The count of consecutive
//!   429s resets once the backoff has been over for as long as it lasted.
//! - **Crawl delay.** From robots.txt, `min_crawl_delay`, or the crawler's `same_domain_delay`.
//!   It is the minimum time between two requests to the domain.
//! - **Saved domains.** With `domains: All`, the domains discovered so far are saved under
//!   `CRAWLEE_THROTTLED_DOMAINS`, so a resumed crawl finds their queues.
//!
//! The crawler offers 429s and crawl delays as [`PacingSignal`]s. A request whose domain answered
//! 429 is put back, at the front of its queue, without spending a retry.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;
use parking_lot::Mutex;
use tokio::sync::OnceCell;
use url::Url;

use crawlee_core::storage::backend::{BatchAddRequestsResult, QueueOperationInfo};
use crawlee_core::{
    KeyValueStore, PacingScope, PacingSignal, Request, RequestManager, RequestQueue, Services, StorageError,
    StorageIdentifier, StorageResult,
};
use crawlee_utils::registrable_domain;

const STATE_KEY: &str = "CRAWLEE_THROTTLED_DOMAINS";
/// Requests of held-back domains moved out of the inner manager per fetch, at most.
const MAX_INNER_MIGRATIONS: usize = 1000;

/// Which domains are throttled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThrottledDomains {
    /// Every domain the crawl meets (up to `max_throttled_domains`).
    All,
    /// Only these hostnames (or registrable domains, with [`ThrottleBy::RegistrableDomain`]).
    List(Vec<String>),
}

/// How requests are grouped into domains.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ThrottleBy {
    #[default]
    Hostname,
    /// Subdomains share the clock of their site.
    RegistrableDomain,
}

#[derive(Clone, Debug)]
pub struct ThrottlingOptions {
    pub domains: ThrottledDomains,
    pub throttle_by: ThrottleBy,
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Minimum time between two requests to a domain, whatever robots.txt says.
    pub min_crawl_delay: Duration,
    /// With `domains: All`, how many domains get a queue at most; requests of further domains
    /// are refused.
    pub max_throttled_domains: usize,
    pub persist_state_key: String,
}

impl Default for ThrottlingOptions {
    fn default() -> Self {
        ThrottlingOptions {
            domains: ThrottledDomains::All,
            throttle_by: ThrottleBy::Hostname,
            base_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(60),
            min_crawl_delay: Duration::ZERO,
            max_throttled_domains: 100,
            persist_state_key: STATE_KEY.to_owned(),
        }
    }
}

/// Time in milliseconds since the Unix epoch.
fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Clone, Debug, Default)]
struct DomainState {
    backoff_until: i64,
    crawl_delay_until: i64,
    backoff_decays_at: i64,
    consecutive_429_count: u32,
    declared_crawl_delay: Option<i64>,
}

impl DomainState {
    fn throttled_until(&self) -> i64 {
        self.backoff_until.max(self.crawl_delay_until)
    }
}

struct Inner {
    /// In the order domains were first seen; ties between fetchable domains go to the older one, as in JS.
    states: IndexMap<String, DomainState>,
    listed: HashSet<String>,
    discovered: HashSet<String>,
    queues: HashMap<String, Arc<RequestQueue>>,
    /// Requests handed out from the inner manager, by id or unique key.
    in_flight_from_inner: HashSet<String>,
    migrated_from_inner: u64,
    min_crawl_delay: i64,
}

/// Paces requests per domain; see the [module documentation](self).
pub struct ThrottlingRequestManager {
    options: ThrottlingOptions,
    services: Services,
    inner_manager: Arc<dyn RequestManager>,
    state: Mutex<Inner>,
    ready: OnceCell<Option<KeyValueStore>>,
    expected_processing_time: Mutex<Option<Duration>>,
}

impl ThrottlingRequestManager {
    /// Wraps `inner`, which keeps the requests of unthrottled domains.
    pub fn new(options: ThrottlingOptions, services: &Services, inner: Arc<dyn RequestManager>) -> Self {
        let mut state = Inner {
            states: IndexMap::new(),
            listed: HashSet::new(),
            discovered: HashSet::new(),
            queues: HashMap::new(),
            in_flight_from_inner: HashSet::new(),
            migrated_from_inner: 0,
            min_crawl_delay: options.min_crawl_delay.as_millis() as i64,
        };
        if let ThrottledDomains::List(domains) = &options.domains {
            for domain in domains {
                let key = domain_key(domain, options.throttle_by);
                state.listed.insert(key.clone());
                state.states.insert(key, DomainState::default());
            }
        }
        ThrottlingRequestManager {
            options,
            services: services.clone(),
            inner_manager: inner,
            state: Mutex::new(state),
            ready: OnceCell::new(),
            expected_processing_time: Mutex::new(None),
        }
    }

    pub fn inner(&self) -> &Arc<dyn RequestManager> {
        &self.inner_manager
    }

    fn throttles_every_domain(&self) -> bool {
        self.options.domains == ThrottledDomains::All
    }

    fn domain_of(&self, url: &str) -> Option<String> {
        let host = Url::parse(url).ok()?.host_str()?.to_owned();
        Some(domain_key(&host, self.options.throttle_by))
    }

    fn is_throttled_domain(&self, domain: &str) -> bool {
        self.throttles_every_domain() || self.state.lock().listed.contains(domain)
    }

    /// Opens the queues of the listed domains and of the domains saved by a previous run.
    async fn ensure_ready(&self) -> StorageResult<()> {
        self.ready
            .get_or_try_init(|| async {
                let store = if self.throttles_every_domain() {
                    let store = self.services.open_key_value_store(&StorageIdentifier::Default).await?;
                    let saved: Vec<String> =
                        store.get_value(&self.options.persist_state_key).await?.unwrap_or_default();
                    self.state.lock().discovered.extend(saved);
                    Some(store)
                } else {
                    None
                };
                let domains: Vec<String> = {
                    let state = self.state.lock();
                    state.listed.iter().chain(&state.discovered).cloned().collect()
                };
                for domain in domains {
                    self.queue_for(&domain).await?;
                }
                Ok::<_, StorageError>(store)
            })
            .await?;
        Ok(())
    }

    async fn queue_for(&self, domain: &str) -> StorageResult<Arc<RequestQueue>> {
        if let Some(queue) = self.state.lock().queues.get(domain) {
            return Ok(queue.clone());
        }
        let alias = format!("throttled-{}", encode_uri_component(domain));
        let queue = Arc::new(self.services.open_request_queue(&StorageIdentifier::Alias(alias)).await?);
        let expected = *self.expected_processing_time.lock();
        if let Some(duration) = expected {
            queue.set_expected_request_processing_time(duration).await?;
        }
        let mut state = self.state.lock();
        state.states.entry(domain.to_owned()).or_default();
        Ok(state.queues.entry(domain.to_owned()).or_insert(queue).clone())
    }

    /// The manager that keeps requests to `url`: a domain queue, or the inner manager.
    async fn select(&self, url: &str) -> StorageResult<Option<Arc<dyn RequestManager>>> {
        self.ensure_ready().await?;
        let Some(domain) = self.domain_of(url).filter(|domain| self.is_throttled_domain(domain)) else {
            return Ok(Some(self.inner_manager.clone()));
        };
        let newly_discovered = {
            let mut state = self.state.lock();
            if state.listed.contains(&domain) || state.discovered.contains(&domain) {
                false
            } else if state.discovered.len() >= self.options.max_throttled_domains {
                return Ok(None);
            } else {
                state.discovered.insert(domain.clone());
                true
            }
        };
        if newly_discovered && let Some(Some(store)) = self.ready.get() {
            let discovered: Vec<String> = self.state.lock().discovered.iter().cloned().collect();
            store.set_value(&self.options.persist_state_key, &discovered).await?;
        }
        let queue: Arc<dyn RequestManager> = self.queue_for(&domain).await?;
        Ok(Some(queue))
    }

    fn domain_limit_error(&self, domain: &str) -> StorageError {
        StorageError::InvalidArgument(format!(
            "Refusing to throttle \"{domain}\": {} domains are already being throttled (max_throttled_domains). \
             Each of them holds a request queue of its own. Narrow the crawl down, pace it with \
             max_requests_per_minute instead, or raise max_throttled_domains.",
            self.options.max_throttled_domains
        ))
    }

    async fn holding(&self, request: &Request) -> StorageResult<Arc<dyn RequestManager>> {
        let key = request.id.clone().unwrap_or_else(|| request.unique_key.clone());
        if self.state.lock().in_flight_from_inner.remove(&key) {
            return Ok(self.inner_manager.clone());
        }
        self.select(&request.url).await?.ok_or_else(|| self.domain_limit_error(&request.url))
    }

    fn record_rate_limit(&self, url: &str, wait: Option<Duration>) -> bool {
        let Some(domain) = self.domain_of(url) else { return false };
        let mut state = self.state.lock();
        let Some(domain_state) = state.states.get_mut(&domain) else { return false };
        let now = now_millis();
        if now < domain_state.backoff_until {
            return true;
        }
        if now >= domain_state.backoff_decays_at {
            domain_state.consecutive_429_count = 0;
        }
        domain_state.consecutive_429_count += 1;
        let base = self.options.base_delay.as_millis() as i64;
        let mut delay = match wait {
            Some(wait) => wait.as_millis() as i64,
            None => base.saturating_mul(1i64 << (domain_state.consecutive_429_count - 1).min(40)),
        };
        let max = self.options.max_delay.as_millis() as i64;
        if delay > max {
            let source = if wait.is_some() { "requested wait" } else { "exponential backoff" };
            tracing::warn!(
                "Capping {source} delay of {:.1}s for domain \"{domain}\" to max_delay ({:.1}s); the domain may \
                 continue to rate-limit. Consider increasing max_delay if this recurs.",
                delay as f64 / 1000.0,
                max as f64 / 1000.0
            );
            delay = max;
        }
        domain_state.backoff_until = now + delay;
        domain_state.backoff_decays_at = domain_state.backoff_until + delay;
        tracing::info!(
            "Rate limit (429) detected for domain \"{domain}\" (consecutive: {}, delay: {:.1}s)",
            domain_state.consecutive_429_count,
            delay as f64 / 1000.0
        );
        true
    }

    fn check_scope(&self, scope: Option<PacingScope>) -> bool {
        match (scope, self.options.throttle_by) {
            (None, _)
            | (Some(PacingScope::Hostname), _)
            | (Some(PacingScope::RegistrableDomain), ThrottleBy::RegistrableDomain) => true,
            (Some(PacingScope::RegistrableDomain), ThrottleBy::Hostname) => {
                tracing::warn!(
                    "Cannot honour a pacing signal scoped to the registrable domain: this manager groups requests by \
                     hostname. Use ThrottleBy::RegistrableDomain."
                );
                false
            }
        }
    }

    async fn fetch_from_inner(&self) -> StorageResult<Option<Request>> {
        for _ in 0..MAX_INNER_MIGRATIONS {
            let Some(mut request) = self.inner_manager.fetch_next_request().await? else { return Ok(None) };
            let held_back = self.domain_of(&request.url).is_some_and(|domain| {
                self.state.lock().states.get(&domain).is_some_and(|s| now_millis() < s.throttled_until())
            });
            if !held_back {
                return Ok(Some(request));
            }
            // Its domain is held back: move it to the domain's queue.
            let target = match self.select(&request.url).await? {
                Some(target) => target,
                None => {
                    self.inner_manager.reclaim_request(&request, true).await?;
                    return Err(self.domain_limit_error(&request.url));
                }
            };
            let mut moved = request.clone();
            moved.id = None;
            if let Err(err) = target.add_requests(vec![moved], false).await {
                self.inner_manager.reclaim_request(&request, true).await?;
                return Err(err);
            }
            self.inner_manager.mark_request_as_handled(&mut request).await?;
            self.state.lock().migrated_from_inner += 1;
        }
        Ok(None)
    }

    fn fetchable_domains(&self) -> Vec<(String, Arc<RequestQueue>)> {
        let state = self.state.lock();
        let now = now_millis();
        let mut domains: Vec<(i64, String, Arc<RequestQueue>)> = state
            .states
            .iter()
            .filter(|(_, s)| now >= s.throttled_until())
            .filter_map(|(domain, s)| Some((s.throttled_until(), domain.clone(), state.queues.get(domain)?.clone())))
            .collect();
        // Stable, so equal deadlines keep the insertion order.
        domains.sort_by_key(|(until, _, _)| *until);
        domains.into_iter().map(|(_, domain, queue)| (domain, queue)).collect()
    }

    fn all_queues(&self) -> Vec<Arc<RequestQueue>> {
        self.state.lock().queues.values().cloned().collect()
    }
}

fn domain_key(host: &str, throttle_by: ThrottleBy) -> String {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    match throttle_by {
        ThrottleBy::Hostname => host,
        ThrottleBy::RegistrableDomain => registrable_domain(&host).unwrap_or(host),
    }
}

/// `encodeURIComponent` of JS.
fn encode_uri_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[async_trait]
impl RequestManager for ThrottlingRequestManager {
    async fn add_requests(&self, requests: Vec<Request>, forefront: bool) -> StorageResult<BatchAddRequestsResult> {
        self.ensure_ready().await?;
        let mut groups: Vec<(Arc<dyn RequestManager>, Vec<Request>)> = Vec::new();
        let mut overflowing = None;
        for request in requests {
            let Some(manager) = self.select(&request.url).await? else {
                overflowing.get_or_insert_with(|| self.domain_of(&request.url).unwrap_or_default());
                continue;
            };
            match groups
                .iter_mut()
                .find(|(existing, _)| std::ptr::addr_eq(Arc::as_ptr(existing), Arc::as_ptr(&manager)))
            {
                Some((_, batch)) => batch.push(request),
                None => groups.push((manager, vec![request])),
            }
        }
        let mut result = BatchAddRequestsResult::default();
        for (manager, batch) in groups {
            let added = manager.add_requests(batch, forefront).await?;
            result.processed_requests.extend(added.processed_requests);
            result.unprocessed_requests.extend(added.unprocessed_requests);
        }
        match overflowing {
            Some(domain) => Err(self.domain_limit_error(&domain)),
            None => Ok(result),
        }
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        self.ensure_ready().await?;
        for (domain, queue) in self.fetchable_domains() {
            // Taken before fetching, so that concurrent fetches respect the delay too.
            let before = {
                let mut state = self.state.lock();
                let min = state.min_crawl_delay;
                let Some(domain_state) = state.states.get_mut(&domain) else { continue };
                let before = domain_state.crawl_delay_until;
                let delay = domain_state.declared_crawl_delay.unwrap_or(0).max(min);
                if delay > 0 {
                    domain_state.crawl_delay_until = now_millis() + delay;
                }
                before
            };
            if let Some(request) = queue.fetch_next_request().await? {
                return Ok(Some(request));
            }
            if let Some(domain_state) = self.state.lock().states.get_mut(&domain) {
                domain_state.crawl_delay_until = before;
            }
        }
        let request = self.fetch_from_inner().await?;
        if let Some(request) = &request {
            let key = request.id.clone().unwrap_or_else(|| request.unique_key.clone());
            self.state.lock().in_flight_from_inner.insert(key);
        }
        Ok(request)
    }

    async fn mark_request_as_handled(&self, request: &mut Request) -> StorageResult<Option<QueueOperationInfo>> {
        let manager = self.holding(request).await?;
        manager.mark_request_as_handled(request).await
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        let manager = self.holding(request).await?;
        manager.reclaim_request(request, forefront).await
    }

    /// Nothing can be fetched right now: held-back domains count as empty.
    async fn is_empty(&self) -> StorageResult<bool> {
        self.ensure_ready().await?;
        for (_, queue) in self.fetchable_domains() {
            if !queue.is_empty().await? {
                return Ok(false);
            }
        }
        self.inner_manager.is_empty().await
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        self.ensure_ready().await?;
        for queue in self.all_queues() {
            if !queue.is_finished().await? {
                return Ok(false);
            }
        }
        self.inner_manager.is_finished().await
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        self.ensure_ready().await?;
        let mut total = self.inner_manager.handled_count().await?;
        for queue in self.all_queues() {
            total += queue.handled_count().await?;
        }
        Ok(total.saturating_sub(self.state.lock().migrated_from_inner))
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        self.ensure_ready().await?;
        let mut total = self.inner_manager.pending_count().await?;
        for queue in self.all_queues() {
            total += queue.pending_count().await?;
        }
        Ok(total)
    }

    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        *self.expected_processing_time.lock() = Some(duration);
        for queue in self.all_queues() {
            queue.set_expected_request_processing_time(duration).await?;
        }
        self.inner_manager.set_expected_request_processing_time(duration).await
    }

    fn record_pacing_signal(&self, signal: &PacingSignal) -> bool {
        match signal {
            PacingSignal::MinIntervalEverywhere { interval, scope } => {
                if !self.throttles_every_domain() || !self.check_scope(*scope) {
                    return false;
                }
                let mut state = self.state.lock();
                state.min_crawl_delay = state.min_crawl_delay.max(interval.as_millis() as i64);
                true
            }
            PacingSignal::RateLimited { url, wait } => self.record_rate_limit(url, *wait),
            PacingSignal::MinInterval { url, interval, scope } => {
                if !self.check_scope(*scope) {
                    return false;
                }
                let Some(domain) = self.domain_of(url).filter(|domain| self.is_throttled_domain(domain)) else {
                    return false;
                };
                let mut state = self.state.lock();
                let domain_state = state.states.entry(domain).or_default();
                domain_state.declared_crawl_delay.get_or_insert(interval.as_millis() as i64);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_keys() {
        assert_eq!(domain_key("WWW.Example.co.uk.", ThrottleBy::Hostname), "www.example.co.uk");
        assert_eq!(domain_key("www.example.co.uk", ThrottleBy::RegistrableDomain), "example.co.uk");
        assert_eq!(encode_uri_component("a.b:8080/ž"), "a.b%3A8080%2F%C5%BE");
    }

    async fn manager(options: ThrottlingOptions) -> ThrottlingRequestManager {
        let services = Services::in_memory();
        let inner = Arc::new(services.open_request_queue(&StorageIdentifier::Default).await.unwrap());
        ThrottlingRequestManager::new(options, &services, inner)
    }

    #[tokio::test]
    async fn backoff_holds_a_domain_back_and_doubles() {
        let manager = manager(ThrottlingOptions::default()).await;
        manager
            .add_requests(vec!["https://a.test/1".into(), "https://a.test/2".into(), "https://b.test/1".into()], false)
            .await
            .unwrap();

        let mut first = manager.fetch_next_request().await.unwrap().unwrap();
        assert!(manager.record_pacing_signal(&PacingSignal::RateLimited { url: first.url.clone(), wait: None }));
        manager.reclaim_request(&first, true).await.unwrap();

        // a.test is held back; b.test is not.
        let next = manager.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(next.url, "https://b.test/1");
        assert!(manager.fetch_next_request().await.unwrap().is_none());
        assert!(manager.is_empty().await.unwrap());
        assert!(!manager.is_finished().await.unwrap());

        let until = manager.state.lock().states["a.test"].backoff_until;
        assert!((until - now_millis() - 2000).abs() < 200, "base delay 2 s");
        // While backing off, further 429s do not extend it.
        assert!(manager.record_pacing_signal(&PacingSignal::RateLimited { url: first.url.clone(), wait: None }));
        assert_eq!(manager.state.lock().states["a.test"].consecutive_429_count, 1);
        // The next one after the backoff doubles it.
        manager.state.lock().states.get_mut("a.test").unwrap().backoff_until = 0;
        manager.record_pacing_signal(&PacingSignal::RateLimited { url: first.url.clone(), wait: None });
        let state = manager.state.lock().states["a.test"].clone();
        assert_eq!(state.consecutive_429_count, 2);
        assert!((state.backoff_until - now_millis() - 4000).abs() < 200);

        manager.state.lock().states.get_mut("a.test").unwrap().backoff_until = 0;
        first = manager.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(first.url, "https://a.test/1", "reclaimed to the front");
    }

    #[tokio::test]
    async fn retry_after_and_crawl_delay() {
        let manager = manager(ThrottlingOptions { max_delay: Duration::from_secs(10), ..Default::default() }).await;
        manager.add_requests(vec!["https://a.test/1".into(), "https://a.test/2".into()], false).await.unwrap();
        assert!(manager.record_pacing_signal(&PacingSignal::MinInterval {
            url: "https://a.test/".into(),
            interval: Duration::from_secs(5),
            scope: Some(PacingScope::Hostname),
        }));
        assert!(manager.fetch_next_request().await.unwrap().is_some());
        assert!(manager.fetch_next_request().await.unwrap().is_none(), "crawl delay between the two");

        let state = manager.state.lock().states["a.test"].clone();
        assert!((state.crawl_delay_until - now_millis() - 5000).abs() < 200);
        manager.record_pacing_signal(&PacingSignal::RateLimited {
            url: "https://a.test/1".into(),
            wait: Some(Duration::from_secs(30)),
        });
        let state = manager.state.lock().states["a.test"].clone();
        assert!((state.backoff_until - now_millis() - 10_000).abs() < 200, "capped at max_delay");
    }

    #[tokio::test]
    async fn listed_domains_only_and_limits() {
        let manager =
            manager(ThrottlingOptions { domains: ThrottledDomains::List(vec!["a.test".into()]), ..Default::default() })
                .await;
        manager.add_requests(vec!["https://a.test/1".into(), "https://c.test/1".into()], false).await.unwrap();
        assert!(
            !manager.record_pacing_signal(&PacingSignal::RateLimited { url: "https://c.test/1".into(), wait: None })
        );
        assert!(!manager.record_pacing_signal(&PacingSignal::MinIntervalEverywhere {
            interval: Duration::from_secs(1),
            scope: None
        }));
        assert_eq!(manager.inner().handled_count().await.unwrap(), 0);

        let limited = manager_with_limit().await;
        limited.add_requests(vec!["https://a.test/1".into()], false).await.unwrap();
        assert!(limited.add_requests(vec!["https://b.test/1".into()], false).await.is_err());
    }

    async fn manager_with_limit() -> ThrottlingRequestManager {
        manager(ThrottlingOptions { max_throttled_domains: 1, ..Default::default() }).await
    }
}
