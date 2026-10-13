//! Fetching robots.txt files ([`RobotsTxtFile`]) and the per-origin cache crawlers keep of them
//! for [`respect_robots_txt`](crate::BasicCrawlerBuilder::respect_robots_txt).

use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use parking_lot::Mutex;
use url::Url;

use crawlee_http_client::{HttpClient, HttpRequest, SendOptions};
use crawlee_utils::RobotsTxt;

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
        let robots_url = Self::url_for(url)?;
        let options = SendOptions { proxy_url, timeout: Some(FETCH_TIMEOUT), ..SendOptions::default() };
        let response = client
            .send_request(HttpRequest::get(robots_url.clone()), &options)
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
}

impl RobotsCache {
    pub(crate) fn new(user_agent: String) -> Self {
        RobotsCache { user_agent, files: Mutex::new(IndexMap::new()) }
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
        let loaded =
            slot.get_or_try_init(|| async { RobotsTxtFile::find(url, client, None).await.map(Arc::new) }).await;
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
}
