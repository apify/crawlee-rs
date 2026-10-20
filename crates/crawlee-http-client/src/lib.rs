//! Pluggable HTTP client for crawlee-rs.
//!
//! [`HttpClient`] is what crawlers talk to. Implementations only have to provide a single-hop
//! [`Transport`]; [`RedirectingClient`] layers the behavior shared by every client on top of it:
//! the redirect loop, session cookies and the overall timeout, ported from `BaseHttpClient` of
//! Crawlee for JS.
//!
//! Bodies are [`Bytes`] from the socket to the crawler, so they are never copied.

pub mod cookies;
#[cfg(feature = "reqwest")]
pub mod reqwest_transport;

use std::rc::Rc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{AUTHORIZATION, COOKIE, LOCATION, PROXY_AUTHORIZATION, SET_COOKIE};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use url::Url;

pub use crate::cookies::CookieJar;
#[cfg(feature = "reqwest")]
pub use crate::reqwest_transport::ReqwestTransport;

/// Maximum number of redirects followed, as in Crawlee for JS.
pub const MAX_REDIRECTS: usize = 10;

/// An outgoing request.
#[derive(Clone, Debug)]
pub struct HttpRequest {
    pub method: Method,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Option<Bytes>,
}

impl HttpRequest {
    pub fn get(url: Url) -> Self {
        HttpRequest { method: Method::GET, url, headers: HeaderMap::new(), body: None }
    }
}

/// A fully received response.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    /// The URL the response came from, after redirects.
    pub url: Url,
    pub body: Bytes,
    /// Every URL visited before `url`, in order.
    pub redirects: Vec<Url>,
}

impl HttpResponse {
    pub fn header(&self, name: impl http::header::AsHeaderName) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

/// Per-request options.
#[derive(Clone, Debug, Default)]
pub struct SendOptions {
    pub proxy_url: Option<Url>,
    /// Cookies are read from and written to this jar. Without one, cookies still flow across
    /// redirects within the request.
    pub cookie_jar: Option<Rc<CookieJar>>,
    /// Timeout for the whole request, redirects and body included.
    pub timeout: Option<Duration>,
    pub ignore_tls_errors: bool,
    /// Responses with larger bodies fail with [`HttpClientError::BodyTooLarge`].
    pub max_body_size: Option<usize>,
}

/// Options a [`Transport`] receives for one hop.
#[derive(Clone, Debug, Default)]
pub struct TransportOptions {
    pub proxy_url: Option<Url>,
    pub ignore_tls_errors: bool,
    pub max_body_size: Option<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    #[error("Request timed out after {} seconds.", .0.as_secs_f64())]
    Timeout(Duration),
    #[error("Too many redirects ({max}) while requesting {url}")]
    TooManyRedirects { max: usize, url: Url },
    #[error("Invalid redirect location '{location}' from {url}")]
    InvalidRedirect { location: String, url: Url },
    #[error("Response body exceeds the limit of {limit} bytes")]
    BodyTooLarge { limit: usize },
    #[error("Proxy error: {0}")]
    Proxy(String),
    #[error("Connection error: {0}")]
    Connect(String),
    #[error("{0}")]
    Other(String),
}

impl HttpClientError {
    /// Errors caused by the proxy (the crawler blames the session for these).
    pub fn is_proxy_error(&self) -> bool {
        matches!(self, HttpClientError::Proxy(_))
    }
}

/// An HTTP client usable by crawlers.
#[async_trait(?Send)]
pub trait HttpClient {
    async fn send_request(&self, request: HttpRequest, options: &SendOptions) -> Result<HttpResponse, HttpClientError>;

    /// Nothing sends through this proxy anymore (the session of a crawler without a session pool
    /// finished its attempt): what is kept for it, such as its connections, can go.
    fn release_proxy(&self, _proxy_url: &Url) {}
}

/// A single request/response exchange, without following redirects or handling cookies.
#[async_trait(?Send)]
pub trait Transport {
    async fn fetch(&self, request: HttpRequest, options: &TransportOptions) -> Result<HttpResponse, HttpClientError>;

    /// See [`HttpClient::release_proxy`].
    fn release_proxy(&self, _proxy_url: &Url) {}
}

/// The redirect and cookie logic of Crawlee's `BaseHttpClient`, over any [`Transport`].
#[derive(Clone, Debug)]
pub struct RedirectingClient<T> {
    transport: T,
    max_redirects: usize,
}

impl<T: Transport> RedirectingClient<T> {
    pub fn new(transport: T) -> Self {
        RedirectingClient { transport, max_redirects: MAX_REDIRECTS }
    }

    pub fn with_max_redirects(mut self, max_redirects: usize) -> Self {
        self.max_redirects = max_redirects;
        self
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    async fn send_inner(&self, initial: HttpRequest, options: &SendOptions) -> Result<HttpResponse, HttpClientError> {
        let jar = options.cookie_jar.clone().unwrap_or_else(|| Rc::new(CookieJar::new()));
        let transport_options = TransportOptions {
            proxy_url: options.proxy_url.clone(),
            ignore_tls_errors: options.ignore_tls_errors,
            max_body_size: options.max_body_size,
        };

        let mut current = initial;
        let mut redirects = Vec::new();

        loop {
            let mut hop = current.clone();
            apply_cookies(&mut hop, &jar);

            let mut response = self.transport.fetch(hop, &transport_options).await?;
            jar.store_response_cookies(
                response.headers.get_all(SET_COOKIE).iter().filter_map(|v| v.to_str().ok()),
                &response.url,
            );

            let location =
                response.status.is_redirection().then(|| response.header(LOCATION).map(str::to_owned)).flatten();
            let Some(location) = location else {
                response.redirects = redirects;
                return Ok(response);
            };

            if redirects.len() >= self.max_redirects {
                return Err(HttpClientError::TooManyRedirects { max: self.max_redirects, url: current.url });
            }
            let next_url = response.url.join(&location).map_err(|_| HttpClientError::InvalidRedirect {
                location: location.clone(),
                url: response.url.clone(),
            })?;

            let switch_to_get = response.status == StatusCode::SEE_OTHER
                || (matches!(response.status, StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND)
                    && current.method == Method::POST);

            let mut next = current.clone();
            if switch_to_get {
                next.method = Method::GET;
                next.body = None;
            }
            // Like `fetch`, do not forward credentials to another origin.
            if next_url.origin() != current.url.origin() {
                for header in [AUTHORIZATION, PROXY_AUTHORIZATION, COOKIE] {
                    next.headers.remove(header);
                }
            }
            next.url = next_url;

            redirects.push(std::mem::replace(&mut current, next).url);
        }
    }
}

/// Sets the `Cookie` header from the jar, merged with any explicit `Cookie` header of the request.
fn apply_cookies(request: &mut HttpRequest, jar: &CookieJar) {
    let explicit = request.headers.get(COOKIE).and_then(|v| v.to_str().ok()).map(str::to_owned);
    let header = match explicit {
        Some(explicit) if !explicit.is_empty() => jar.merged_cookie_header(&request.url, &explicit),
        _ => jar.cookie_header(&request.url),
    };
    if let Some(header) = header.and_then(|h| HeaderValue::from_str(&h).ok()) {
        request.headers.insert(COOKIE, header);
    }
}

#[async_trait(?Send)]
impl<T: Transport> HttpClient for RedirectingClient<T> {
    async fn send_request(&self, request: HttpRequest, options: &SendOptions) -> Result<HttpResponse, HttpClientError> {
        match options.timeout {
            Some(timeout) => tokio::time::timeout(timeout, self.send_inner(request, options))
                .await
                .map_err(|_| HttpClientError::Timeout(timeout))?,
            None => self.send_inner(request, options).await,
        }
    }

    fn release_proxy(&self, proxy_url: &Url) {
        self.transport.release_proxy(proxy_url);
    }
}

#[async_trait(?Send)]
impl<C: HttpClient + ?Sized> HttpClient for Rc<C> {
    async fn send_request(&self, request: HttpRequest, options: &SendOptions) -> Result<HttpResponse, HttpClientError> {
        (**self).send_request(request, options).await
    }

    fn release_proxy(&self, proxy_url: &Url) {
        (**self).release_proxy(proxy_url);
    }
}

/// The default client: [`ReqwestTransport`] behind the shared redirect and cookie logic.
#[cfg(feature = "reqwest")]
pub fn default_client() -> Rc<dyn HttpClient> {
    Rc::new(RedirectingClient::new(ReqwestTransport::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    type ScriptedResponse = (u16, Vec<(&'static str, &'static str)>);

    /// Serves scripted responses and records what it was asked for.
    #[derive(Default)]
    struct Scripted {
        responses: RefCell<Vec<ScriptedResponse>>,
        seen: RefCell<Vec<HttpRequest>>,
    }

    #[async_trait(?Send)]
    impl Transport for Scripted {
        async fn fetch(&self, request: HttpRequest, _: &TransportOptions) -> Result<HttpResponse, HttpClientError> {
            let (status, headers) = self.responses.borrow_mut().remove(0);
            let mut map = HeaderMap::new();
            for (name, value) in headers {
                map.append(http::HeaderName::from_static(name), HeaderValue::from_static(value));
            }
            let url = request.url.clone();
            self.seen.borrow_mut().push(request);
            Ok(HttpResponse {
                status: StatusCode::from_u16(status).unwrap(),
                headers: map,
                url,
                body: Bytes::new(),
                redirects: Vec::new(),
            })
        }
    }

    #[tokio::test(flavor = "local")]
    async fn follows_redirects_with_cookies_and_method_rewrites() {
        let transport = Scripted::default();
        *transport.responses.borrow_mut() = vec![
            (302, vec![("location", "/next"), ("set-cookie", "sid=1; Path=/")]),
            (301, vec![("location", "https://other.dev/final")]),
            (200, vec![]),
        ];
        let client = RedirectingClient::new(transport);

        let mut request = HttpRequest::get(Url::parse("https://example.com/start").unwrap());
        request.method = Method::POST;
        request.body = Some(Bytes::from_static(b"q=1"));
        request.headers.insert(AUTHORIZATION, HeaderValue::from_static("secret"));

        let jar = Rc::new(CookieJar::new());
        let response = client
            .send_request(request, &SendOptions { cookie_jar: Some(jar.clone()), ..Default::default() })
            .await
            .unwrap();

        assert_eq!(response.url.as_str(), "https://other.dev/final");
        assert_eq!(response.redirects.len(), 2);

        let seen = client.transport().seen.borrow_mut();
        assert_eq!(seen[1].method, Method::GET, "302 after POST becomes GET");
        assert!(seen[1].body.is_none());
        assert_eq!(seen[1].headers.get(COOKIE).unwrap(), "sid=1");
        assert_eq!(seen[1].headers.get(AUTHORIZATION).unwrap(), "secret", "same origin keeps credentials");
        assert!(seen[2].headers.get(AUTHORIZATION).is_none(), "cross-origin drops credentials");
        assert!(seen[2].headers.get(COOKIE).is_none());
        assert_eq!(jar.cookie_header(&Url::parse("https://example.com/").unwrap()).as_deref(), Some("sid=1"));
    }

    #[tokio::test(flavor = "local")]
    async fn stops_after_too_many_redirects() {
        let transport = Scripted::default();
        *transport.responses.borrow_mut() = (0..=MAX_REDIRECTS).map(|_| (302, vec![("location", "/loop")])).collect();
        let client = RedirectingClient::new(transport);
        let err = client
            .send_request(HttpRequest::get(Url::parse("https://example.com/").unwrap()), &SendOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, HttpClientError::TooManyRedirects { max: MAX_REDIRECTS, .. }));
    }
}
