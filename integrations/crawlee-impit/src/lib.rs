//! [impit](https://github.com/apify/impit) transport for crawlee-rs.
//!
//! impit makes requests with the TLS and HTTP/2 fingerprints of real browsers (Chrome, Firefox,
//! Safari), which many anti-bot systems check. It is the default HTTP client of Crawlee for JS.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use crawlee::http_client::{HttpClient, RedirectingClient};
//! use crawlee::{HtmlContext, HtmlCrawler};
//! use crawlee_impit::ImpitTransport;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let client: Arc<dyn HttpClient> = Arc::new(RedirectingClient::new(ImpitTransport::new()));
//! let crawler = HtmlCrawler::builder()
//!     .http_client(client)
//!     .request_handler(|ctx: HtmlContext| async move { Ok(()) })
//!     .build()?;
//! # Ok(()) }
//! ```
//!
//! Each session impersonates the browser of its fingerprint, in a version picked once per session.
//! The transport does a single exchange; redirects, cookies and timeouts are handled by
//! [`RedirectingClient`](crawlee_http_client::RedirectingClient), like for every other transport.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use impit::errors::ImpitError;
use impit::fingerprint::BrowserFingerprint;
use impit::fingerprint::database as fingerprints;
use impit::impit::{Impit, RedirectBehavior};
use impit::request::{ImpitBody, RequestOptions};
use parking_lot::Mutex;
use rand::Rng as _;

use crawlee_http_client::{
    BodyStream, FingerprintBrowser, HttpClientError, HttpRequest, HttpResponse, ResponseBody, SessionFingerprint,
    StreamingResponse, Transport, TransportOptions,
};

/// The browser impit impersonates: a family (its newest version) or a version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Browser {
    Chrome,
    Firefox,
    Safari,
    Chrome100,
    Chrome101,
    Chrome104,
    Chrome107,
    Chrome110,
    Chrome116,
    Chrome124,
    Chrome125,
    Chrome131,
    Chrome136,
    Chrome142,
    Firefox128,
    Firefox133,
    Firefox135,
    Firefox144,
}

/// The versions a session's fingerprint picks from, per browser family (`IMPIT_VERSIONS_BY_BROWSER`).
pub const CHROME_VERSIONS: [Browser; 11] = [
    Browser::Chrome100,
    Browser::Chrome101,
    Browser::Chrome104,
    Browser::Chrome107,
    Browser::Chrome110,
    Browser::Chrome116,
    Browser::Chrome124,
    Browser::Chrome125,
    Browser::Chrome131,
    Browser::Chrome136,
    Browser::Chrome142,
];
pub const FIREFOX_VERSIONS: [Browser; 4] =
    [Browser::Firefox128, Browser::Firefox133, Browser::Firefox135, Browser::Firefox144];

impl Browser {
    fn fingerprint(self) -> BrowserFingerprint {
        match self {
            Browser::Chrome | Browser::Chrome142 => fingerprints::chrome_142::fingerprint(),
            Browser::Firefox | Browser::Firefox144 => fingerprints::firefox_144::fingerprint(),
            Browser::Safari => fingerprints::ios_18::fingerprint(),
            Browser::Chrome100 => fingerprints::chrome_100::fingerprint(),
            Browser::Chrome101 => fingerprints::chrome_101::fingerprint(),
            Browser::Chrome104 => fingerprints::chrome_104::fingerprint(),
            Browser::Chrome107 => fingerprints::chrome_107::fingerprint(),
            Browser::Chrome110 => fingerprints::chrome_110::fingerprint(),
            Browser::Chrome116 => fingerprints::chrome_116::fingerprint(),
            Browser::Chrome124 => fingerprints::chrome_124::fingerprint(),
            Browser::Chrome125 => fingerprints::chrome_125::fingerprint(),
            Browser::Chrome131 => fingerprints::chrome_131::fingerprint(),
            Browser::Chrome136 => fingerprints::chrome_136::fingerprint(),
            Browser::Firefox128 => fingerprints::firefox_128::fingerprint(),
            Browser::Firefox133 => fingerprints::firefox_133::fingerprint(),
            Browser::Firefox135 => fingerprints::firefox_135::fingerprint(),
        }
    }

    /// The versions a fingerprint of `family` picks from. impit only impersonates Chrome and
    /// Firefox; other families (Edge, Safari) get Chrome's realistic headers.
    fn versions_for(family: FingerprintBrowser) -> &'static [Browser] {
        match family {
            FingerprintBrowser::Firefox => &FIREFOX_VERSIONS,
            _ => &CHROME_VERSIONS,
        }
    }
}

/// Clients kept by `ImpitHttpClient` (`LruCache({ maxLength: 10 })`).
const CLIENT_CACHE_SIZE: usize = 10;

/// What a client is made for. Redirects are always handled by `RedirectingClient`, so impit never
/// follows them and that option is not part of the key.
type ClientKey = (Option<String>, bool, Option<Browser>);

type Client = Arc<Impit<reqwest::cookie::Jar>>;

/// The most recently used clients: each with the tick of its last use, so the least recently
/// used one is evicted when the cache is full.
struct ClientCache {
    clients: HashMap<ClientKey, (Client, u64)>,
    capacity: usize,
    tick: u64,
}

impl Default for ClientCache {
    fn default() -> Self {
        ClientCache { clients: HashMap::new(), capacity: CLIENT_CACHE_SIZE, tick: 0 }
    }
}

impl ClientCache {
    fn get(&mut self, key: &ClientKey) -> Option<Client> {
        self.tick += 1;
        let (client, used) = self.clients.get_mut(key)?;
        *used = self.tick;
        Some(client.clone())
    }

    fn add(&mut self, key: ClientKey, client: Client) {
        self.tick += 1;
        self.clients.insert(key, (client, self.tick));
        if self.clients.len() > self.capacity
            && let Some(oldest) = self.clients.iter().min_by_key(|(_, (_, used))| *used).map(|(key, _)| key.clone())
        {
            self.clients.remove(&oldest);
        }
    }

    fn retain(&mut self, keep: impl Fn(&ClientKey) -> bool) {
        self.clients.retain(|key, _| keep(key));
    }

    #[cfg(test)]
    fn proxies(&self) -> Vec<Option<String>> {
        self.clients.keys().map(|(proxy, _, _)| proxy.clone()).collect()
    }
}

/// The version picked for each session's fingerprint, kept while the fingerprint lives (a
/// `WeakMap` in JS), so a session keeps impersonating the same version.
#[derive(Default)]
struct VersionsByFingerprint {
    versions: HashMap<usize, (Weak<SessionFingerprint>, Browser)>,
    prune_at: usize,
}

impl VersionsByFingerprint {
    fn resolve(&mut self, fingerprint: &Arc<SessionFingerprint>) -> Option<Browser> {
        let family = fingerprint.browser?;
        let key = Arc::as_ptr(fingerprint) as usize;
        if let Some((weak, version)) = self.versions.get(&key)
            && weak.upgrade().is_some_and(|live| Arc::ptr_eq(&live, fingerprint))
        {
            return Some(*version);
        }
        let versions = Browser::versions_for(family);
        let version = versions[rand::rng().random_range(0..versions.len())];
        if self.versions.len() >= self.prune_at {
            self.versions.retain(|_, (weak, _)| weak.strong_count() > 0);
            self.prune_at = (self.versions.len() * 2).max(64);
        }
        self.versions.insert(key, (Arc::downgrade(fingerprint), version));
        Some(version)
    }
}

/// A [`Transport`] backed by impit (`ImpitHttpClient`).
///
/// Clients are kept for the 10 most recently used combinations of proxy, TLS verification and
/// browser (see [`with_max_cached_clients`](Self::with_max_cached_clients)), so connections are
/// reused.
pub struct ImpitTransport {
    browser: Option<Browser>,
    cache_clients: bool,
    clients: Mutex<ClientCache>,
    versions: Mutex<VersionsByFingerprint>,
}

impl std::fmt::Debug for ImpitTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImpitTransport").field("browser", &self.browser).finish_non_exhaustive()
    }
}

impl Default for ImpitTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ImpitTransport {
    /// Impersonates the browser of each session's fingerprint, in a version picked once per
    /// session. Requests without a fingerprint use impit's defaults.
    pub fn new() -> Self {
        ImpitTransport { browser: None, cache_clients: true, clients: Mutex::default(), versions: Mutex::default() }
    }

    /// Impersonates `browser` for every request, whatever the session's fingerprint says.
    pub fn with_browser(browser: Browser) -> Self {
        tracing::warn!(
            "The `browser` option of ImpitTransport overrides the browser fingerprint of the session. Using it with \
             crawlers can lead to incompatible session setups."
        );
        ImpitTransport { browser: Some(browser), ..Self::new() }
    }

    /// How many clients to keep: 10 by default, as in Crawlee for JS. A crawler that gives each
    /// attempt a proxy session of its own needs about as many as it runs requests at once, or the
    /// requests of an attempt open new connections when its client was evicted in between.
    /// [`release_proxy`](Transport::release_proxy) still drops the clients of finished attempts.
    pub fn with_max_cached_clients(self, max: usize) -> Self {
        self.clients.lock().capacity = max.max(1);
        self
    }

    /// Whether to keep clients between requests (default `true`). A new client per request cannot
    /// reuse connections.
    pub fn with_cache_clients(mut self, cache_clients: bool) -> Self {
        self.cache_clients = cache_clients;
        self
    }

    /// The browser set with [`with_browser`](Self::with_browser).
    pub fn browser(&self) -> Option<Browser> {
        self.browser
    }

    /// The browser a request impersonates: the transport's, else the version picked for the
    /// session's fingerprint.
    fn browser_for(&self, fingerprint: Option<&Arc<SessionFingerprint>>) -> Option<Browser> {
        self.browser.or_else(|| fingerprint.and_then(|fingerprint| self.versions.lock().resolve(fingerprint)))
    }

    fn client(&self, options: &TransportOptions) -> Result<Client, HttpClientError> {
        let browser = self.browser_for(options.fingerprint.as_ref());
        let key = (options.proxy_url.as_ref().map(|u| u.to_string()), options.ignore_tls_errors, browser);
        if self.cache_clients
            && let Some(client) = self.clients.lock().get(&key)
        {
            return Ok(client);
        }

        // Redirects and cookies are handled by `RedirectingClient`; impit does a single hop.
        let mut builder = Impit::<reqwest::cookie::Jar>::builder()
            .with_redirect(RedirectBehavior::ManualRedirect)
            .with_ignore_tls_errors(options.ignore_tls_errors);
        if let Some(browser) = browser {
            builder = builder.with_fingerprint(browser.fingerprint());
        }
        if let Some(proxy) = &options.proxy_url {
            builder = builder.with_proxy(proxy.to_string());
        }
        let client = Arc::new(builder.build().map_err(|err| map_error(err, options.proxy_url.is_some()))?);
        if self.cache_clients {
            self.clients.lock().add(key, client.clone());
        }
        Ok(client)
    }
}

fn map_error(err: ImpitError, via_proxy: bool) -> HttpClientError {
    let message = err.to_string();
    match err {
        ImpitError::ProxyError(_) | ImpitError::ProxyTunnelError(_) | ImpitError::ProxyAuthRequired => {
            HttpClientError::Proxy(message)
        }
        ImpitError::ConnectError(_) | ImpitError::ConnectTimeout if via_proxy => HttpClientError::Proxy(message),
        ImpitError::ConnectError(_) | ImpitError::ConnectTimeout | ImpitError::NetworkError => {
            HttpClientError::Connect(message)
        }
        _ => HttpClientError::Other(message),
    }
}

/// The body of an impit response, read chunk by chunk. Dropping it aborts the download.
struct ImpitResponseBody(reqwest::Response);

#[async_trait]
impl BodyStream for ImpitResponseBody {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, HttpClientError> {
        self.0.chunk().await.map_err(|err| HttpClientError::Other(err.to_string()))
    }
}

#[async_trait]
impl Transport for ImpitTransport {
    /// Drops the clients of a proxy nothing will use again.
    fn release_proxy(&self, proxy_url: &url::Url) {
        let proxy_url = proxy_url.as_str();
        self.clients.lock().retain(|(proxy, _, _)| proxy.as_deref() != Some(proxy_url));
    }

    async fn fetch(&self, request: HttpRequest, options: &TransportOptions) -> Result<HttpResponse, HttpClientError> {
        let mut response = self.fetch_streaming(request, options).await?;
        response.body.set_max_size(options.max_body_size);
        response.into_response().await
    }

    async fn fetch_streaming(
        &self,
        request: HttpRequest,
        options: &TransportOptions,
    ) -> Result<StreamingResponse, HttpClientError> {
        let client = self.client(options)?;
        let via_proxy = options.proxy_url.is_some();

        let headers: Vec<(String, String)> = request
            .headers
            .iter()
            .filter_map(|(name, value)| Some((name.as_str().to_owned(), value.to_str().ok()?.to_owned())))
            .collect();
        // Timeouts are enforced by `RedirectingClient` around the whole exchange, so impit's own
        // default must not cut a longer deadline short.
        let request_options = RequestOptions { headers, timeout: Some(None), http3_prior_knowledge: false };
        let body = request.body.map(ImpitBody::Bytes);
        let url = request.url.to_string();

        let result = match request.method {
            Method::GET => client.get(url, body, Some(request_options)).await,
            Method::POST => client.post(url, body, Some(request_options)).await,
            Method::PUT => client.put(url, body, Some(request_options)).await,
            Method::PATCH => client.patch(url, body, Some(request_options)).await,
            Method::DELETE => client.delete(url, body, Some(request_options)).await,
            Method::HEAD => client.head(url, body, Some(request_options)).await,
            Method::OPTIONS => client.options(url, body, Some(request_options)).await,
            Method::TRACE => client.trace(url, body, Some(request_options)).await,
            other => return Err(HttpClientError::Other(format!("impit does not support the {other} method"))),
        };
        let mut response = result.map_err(|err| map_error(err, via_proxy))?;

        let status = response.status();
        let headers = std::mem::take(response.headers_mut());
        let url = response.url().clone();
        let exact_len = response.content_length();
        let body = ResponseBody::from_stream(ImpitResponseBody(response), exact_len);
        Ok(StreamingResponse { status, headers, url, body, redirects: Vec::new() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crawlee_http_client::FingerprintBrowser as Family;

    fn fingerprint(family: Family) -> Arc<SessionFingerprint> {
        Arc::new(SessionFingerprint { browser: Some(family), ..Default::default() })
    }

    fn proxy(n: usize) -> TransportOptions {
        let url = url::Url::parse(&format!("http://p{n}.example:8000")).unwrap();
        TransportOptions { proxy_url: Some(url), ..Default::default() }
    }

    /// Each session keeps the version picked for it; the families map onto JS's version lists.
    #[test]
    fn a_fingerprint_keeps_its_version() {
        let transport = ImpitTransport::new();
        let firefox = fingerprint(Family::Firefox);
        let version = transport.browser_for(Some(&firefox)).unwrap();
        assert!(FIREFOX_VERSIONS.contains(&version));
        for _ in 0..20 {
            assert_eq!(transport.browser_for(Some(&firefox)), Some(version));
        }
        for family in [Family::Chrome, Family::Edge, Family::Safari] {
            assert!(CHROME_VERSIONS.contains(&transport.browser_for(Some(&fingerprint(family))).unwrap()));
        }
        assert_eq!(transport.browser_for(Some(&Arc::new(SessionFingerprint::default()))), None);
        assert_eq!(transport.browser_for(None), None);
    }

    /// A browser set on the transport wins over the session's fingerprint (crawlee#4262).
    #[test]
    fn the_transport_browser_wins() {
        let transport = ImpitTransport::with_browser(Browser::Chrome124);
        assert_eq!(transport.browser_for(Some(&fingerprint(Family::Firefox))), Some(Browser::Chrome124));
    }

    #[test]
    fn the_ten_most_recently_used_clients_are_kept() {
        let transport = ImpitTransport::new();
        let first = transport.client(&proxy(0)).unwrap();
        for n in 1..10 {
            transport.client(&proxy(n)).unwrap();
        }
        assert!(Arc::ptr_eq(&first, &transport.client(&proxy(0)).unwrap()), "reused, and now the most recent");
        transport.client(&proxy(10)).unwrap();
        let keys = transport.clients.lock().proxies();
        assert_eq!(keys.len(), 10);
        assert!(!keys.contains(&Some("http://p1.example:8000/".to_owned())), "the least recently used is gone");
        assert!(keys.contains(&Some("http://p0.example:8000/".to_owned())));

        let uncached = ImpitTransport::new().with_cache_clients(false);
        let a = uncached.client(&proxy(0)).unwrap();
        assert!(!Arc::ptr_eq(&a, &uncached.client(&proxy(0)).unwrap()));
        assert!(uncached.clients.lock().proxies().is_empty());
    }

    /// A crawler with a proxy session per attempt and high concurrency keeps more clients, so that
    /// the requests of an attempt reuse their connection.
    #[test]
    fn the_cache_can_keep_more_clients() {
        let transport = ImpitTransport::new().with_max_cached_clients(50);
        let first = transport.client(&proxy(0)).unwrap();
        for n in 1..50 {
            transport.client(&proxy(n)).unwrap();
        }
        assert!(Arc::ptr_eq(&first, &transport.client(&proxy(0)).unwrap()));
        transport.client(&proxy(50)).unwrap();
        let keys = transport.clients.lock().proxies();
        assert_eq!(keys.len(), 50);
        assert!(!keys.contains(&Some("http://p1.example:8000/".to_owned())), "the least recently used is gone");
    }
}
