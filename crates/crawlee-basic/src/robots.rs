//! Fetching robots.txt files ([`RobotsTxtFile`]) and the per-origin cache crawlers keep of them
//! for [`respect_robots_txt`](crate::BasicCrawlerBuilder::respect_robots_txt).

use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use parking_lot::Mutex;
use url::Url;

use crawlee_http_client::{HttpClient, HttpRequest, SendOptions};
use crawlee_utils::RobotsTxt;

use crate::session::{Session, SessionPool};

/// How many origins' robots.txt files a crawler keeps (`LruCache({ maxLength: 1000 })` in JS).
const CACHE_SIZE: usize = 1000;
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum RobotsTxtError {
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    #[error("failed to load robots.txt from {url}: HTTP {status}")]
    Status { url: String, status: u16 },
    #[error("failed to load robots.txt from {url}: {source}")]
    Fetch {
        url: String,
        #[source]
        source: Box<crawlee_http_client::HttpClientError>,
    },
}

/// Loads robots.txt files.
pub struct RobotsTxtFile;

impl RobotsTxtFile {
    /// The robots.txt URL of the site `url` belongs to.
    pub fn url_for(url: &str) -> Result<Url, RobotsTxtError> {
        let mut robots = Url::parse(url).map_err(|_| RobotsTxtError::InvalidUrl(url.to_owned()))?;
        robots.set_path("/robots.txt");
        robots.set_query(None);
        robots.set_fragment(None);
        Ok(robots)
    }

    /// Fetches the robots.txt of the site `url` belongs to.
    ///
    /// As in RFC 9309 (and Crawlee for JS), a 4xx answer (no robots.txt) allows everything and a
    /// 5xx answer (unreachable) disallows everything. Other statuses are errors.
    pub async fn find(url: &str, client: &dyn HttpClient, proxy_url: Option<Url>) -> Result<RobotsTxt, RobotsTxtError> {
        let options = SendOptions { proxy_url, timeout: Some(FETCH_TIMEOUT), ..SendOptions::default() };
        Self::find_with_options(url, client, &options).await
    }

    /// Like [`find`](Self::find), but the way a page request of `session` is made: with its
    /// proxy, cookies and browser fingerprint. Sites with bot protection answer a bare request
    /// with a 4xx or 5xx, which would then decide alone whether everything is allowed.
    pub async fn find_with_session(
        url: &str,
        client: &dyn HttpClient,
        session: &Session,
    ) -> Result<RobotsTxt, RobotsTxtError> {
        let proxy = session.proxy_info();
        let options = SendOptions {
            proxy_url: proxy.map(|proxy| proxy.url.clone()),
            cookie_jar: Some(session.cookie_jar().clone()),
            timeout: Some(FETCH_TIMEOUT),
            ignore_tls_errors: proxy.is_some_and(|proxy| proxy.ignore_tls_errors),
            fingerprint: session.fingerprint_handle().cloned(),
            ..SendOptions::default()
        };
        Self::find_with_options(url, client, &options).await
    }

    async fn find_with_options(
        url: &str,
        client: &dyn HttpClient,
        options: &SendOptions,
    ) -> Result<RobotsTxt, RobotsTxtError> {
        let robots_url = Self::url_for(url)?;
        let response = client
            .send_request(HttpRequest::get(robots_url.clone()), options)
            .await
            .map_err(|source| RobotsTxtError::Fetch { url: robots_url.to_string(), source: Box::new(source) })?;
        let status = response.status.as_u16();
        match status {
            200..=299 => Ok(RobotsTxt::parse(robots_url.as_str(), &String::from_utf8_lossy(&response.body))),
            400..=499 => Ok(RobotsTxt::allow_all(robots_url.as_str())),
            500..=599 => Ok(RobotsTxt::disallow_all(robots_url.as_str())),
            _ => Err(RobotsTxtError::Status { url: robots_url.to_string(), status }),
        }
    }
}

/// One origin's robots.txt: loaded once even when several requests ask at the same time.
type Slot = Arc<tokio::sync::OnceCell<Arc<RobotsTxt>>>;

/// robots.txt files by origin, least recently used evicted first. Failed fetches are not cached,
/// so they are tried again.
pub(crate) struct RobotsCache {
    user_agent: String,
    files: Mutex<IndexMap<String, Slot>>,
    /// Where the session of a fetch comes from, and whether it is a pooled one.
    sessions: Option<(Arc<SessionPool>, bool)>,
}

impl RobotsCache {
    pub(crate) fn new(user_agent: String) -> Self {
        RobotsCache { user_agent, files: Mutex::new(IndexMap::new()), sessions: None }
    }

    /// Fetches with a session of `pool` (a new detached one without `use_session_pool`).
    pub(crate) fn with_sessions(mut self, pool: Arc<SessionPool>, use_session_pool: bool) -> Self {
        self.sessions = Some((pool, use_session_pool));
        self
    }

    async fn find(&self, url: &str, client: &dyn HttpClient) -> Result<RobotsTxt, RobotsTxtError> {
        match &self.sessions {
            Some((pool, use_session_pool)) => {
                let session = if *use_session_pool { pool.get_session() } else { pool.detached_session() };
                RobotsTxtFile::find_with_session(url, client, &session).await
            }
            None => RobotsTxtFile::find(url, client, None).await,
        }
    }

    pub(crate) fn user_agent(&self) -> &str {
        &self.user_agent
    }

    /// The robots.txt covering `url`, or `None` when it could not be loaded (then everything is
    /// allowed, as in JS).
    pub(crate) async fn get(&self, url: &str, client: &dyn HttpClient) -> Option<Arc<RobotsTxt>> {
        let origin = Url::parse(url).ok()?.origin().ascii_serialization();
        let slot: Slot = {
            let mut files = self.files.lock();
            match files.get_index_of(&origin) {
                Some(index) => {
                    let last = files.len() - 1;
                    files.move_index(index, last);
                    files[last].clone()
                }
                None => {
                    let slot = Slot::default();
                    files.insert(origin.clone(), slot.clone());
                    while files.len() > CACHE_SIZE {
                        files.shift_remove_index(0);
                    }
                    slot
                }
            }
        };
        let loaded = slot.get_or_try_init(|| async { self.find(url, client).await.map(Arc::new) }).await;
        match loaded {
            Ok(robots) => Some(robots.clone()),
            Err(err) => {
                // Left empty in the cache, so the next request tries again.
                tracing::warn!("Failed to fetch robots.txt for request {url}: {err}");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crawlee_http_client::{HttpClientError, HttpResponse};

    struct Status(u16);

    #[async_trait::async_trait]
    impl HttpClient for Status {
        async fn send_request(&self, request: HttpRequest, _: &SendOptions) -> Result<HttpResponse, HttpClientError> {
            Ok(HttpResponse {
                status: http::StatusCode::from_u16(self.0).unwrap(),
                headers: http::HeaderMap::new(),
                url: request.url,
                body: "User-agent: *\nAllow: /".into(),
                redirects: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn missing_robots_txt_allows_all_and_unreachable_one_disallows_all() {
        let page = "https://a.test/some/page.html";
        for status in [401, 403, 404, 410] {
            let robots = RobotsTxtFile::find(page, &Status(status), None).await.unwrap();
            assert_eq!(robots.check(page, "bot"), Some(true), "HTTP {status}");
        }
        for status in [500, 503] {
            let robots = RobotsTxtFile::find(page, &Status(status), None).await.unwrap();
            assert_eq!(robots.check(page, "bot"), Some(false), "HTTP {status}");
        }
        assert!(matches!(
            RobotsTxtFile::find(page, &Status(304), None).await,
            Err(RobotsTxtError::Status { status: 304, .. })
        ));
    }

    /// Records what it was asked with, and finds no robots.txt.
    #[derive(Default)]
    struct Recording(Mutex<Vec<SendOptions>>);

    #[async_trait::async_trait]
    impl HttpClient for Recording {
        async fn send_request(
            &self,
            request: HttpRequest,
            options: &SendOptions,
        ) -> Result<HttpResponse, HttpClientError> {
            self.0.lock().push(options.clone());
            Ok(HttpResponse {
                status: http::StatusCode::NOT_FOUND,
                headers: http::HeaderMap::new(),
                url: request.url,
                body: Default::default(),
                redirects: Vec::new(),
            })
        }
    }

    struct Mitm;

    impl crate::proxy::ProxySource for Mitm {
        fn new_proxy_info(&self, _: &str) -> Option<crate::proxy::ProxyInfo> {
            let url = Url::parse("http://mitm.test:8000").unwrap();
            Some(crate::proxy::ProxyInfo { ignore_tls_errors: true, ..crate::proxy::ProxyInfo::from_url(url) })
        }
    }

    /// robots.txt is fetched like a page: with a session's fingerprint, cookies and proxy
    /// (crawlee#4218).
    #[tokio::test]
    async fn robots_txt_is_fetched_with_a_session() {
        for use_session_pool in [true, false] {
            let pool = Arc::new(crate::session::SessionPool::new(Default::default(), Some(Arc::new(Mitm))));
            let cache = RobotsCache::new("bot".into()).with_sessions(pool.clone(), use_session_pool);
            let client = Recording::default();
            assert!(cache.get("https://a.test/page", &client).await.is_some());
            let sent = client.0.lock();
            assert_eq!(sent[0].proxy_url.as_ref().unwrap().host_str(), Some("mitm.test"));
            assert!(sent[0].ignore_tls_errors);
            assert!(sent[0].fingerprint.is_some());
            assert!(sent[0].cookie_jar.is_some());
            assert_eq!(sent[0].timeout, Some(FETCH_TIMEOUT));
            assert_eq!(pool.usable_count(), usize::from(use_session_pool), "a pooled session, or a detached one");
        }
    }
}
