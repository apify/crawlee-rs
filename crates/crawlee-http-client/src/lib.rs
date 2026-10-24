//! Pluggable HTTP client for crawlee-rs.
//!
//! [`HttpClient`] is what crawlers talk to. Implementations only have to provide a single-hop
//! [`Transport`]; [`RedirectingClient`] layers the behavior shared by every client on top of it:
//! the redirect loop, session cookies and the overall timeout, ported from `BaseHttpClient` of
//! Crawlee for JS.
//!
//! Bodies are [`Bytes`] from the socket to the crawler, so they are never copied. A
//! [`StreamingResponse`] hands the status and headers over before the body is read, so a crawler
//! can reject a response without downloading it.

pub mod cookies;
pub mod fingerprint;
#[cfg(feature = "reqwest")]
pub mod reqwest_transport;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::header::{AUTHORIZATION, COOKIE, LOCATION, PROXY_AUTHORIZATION, SET_COOKIE};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use url::Url;

pub use crate::cookies::CookieJar;
pub use crate::fingerprint::{FingerprintBrowser, FingerprintDevice, FingerprintPlatform, SessionFingerprint};
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

impl From<HttpResponse> for StreamingResponse {
    fn from(response: HttpResponse) -> Self {
        StreamingResponse {
            status: response.status,
            headers: response.headers,
            url: response.url,
            redirects: response.redirects,
            body: ResponseBody::from_bytes(response.body),
        }
    }
}

/// A source of body chunks, such as a socket.
#[async_trait]
pub trait BodyStream: Send {
    /// The next chunk, or `None` at the end of the body.
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, HttpClientError>;
}

enum BodySource {
    Bytes(Option<Bytes>),
    Stream(Box<dyn BodyStream>),
}

/// The body of a [`StreamingResponse`], read on demand.
pub struct ResponseBody {
    source: BodySource,
    /// The exact length, when known (`Content-Length` of a body that is not decompressed).
    exact_len: Option<u64>,
    max_size: Option<usize>,
    read: usize,
    deadline: Option<(tokio::time::Instant, Duration)>,
}

impl std::fmt::Debug for ResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseBody").field("exact_len", &self.exact_len).finish_non_exhaustive()
    }
}

/// The buffer a body of `exact_len` bytes is read into: preallocated when the length is known and
/// within the limit.
fn body_buffer(exact_len: Option<u64>, max_size: Option<usize>) -> BytesMut {
    match exact_len.and_then(|len| usize::try_from(len).ok()) {
        Some(len) if max_size.is_none_or(|limit| len <= limit) => BytesMut::with_capacity(len),
        _ => BytesMut::new(),
    }
}

impl ResponseBody {
    /// A body that is already in memory.
    pub fn from_bytes(bytes: Bytes) -> Self {
        let exact_len = Some(bytes.len() as u64);
        ResponseBody { source: BodySource::Bytes(Some(bytes)), exact_len, max_size: None, read: 0, deadline: None }
    }

    /// A body read from `stream`. `exact_len` is its length when known in advance.
    pub fn from_stream(stream: impl BodyStream + 'static, exact_len: Option<u64>) -> Self {
        ResponseBody {
            source: BodySource::Stream(Box::new(stream)),
            exact_len,
            max_size: None,
            read: 0,
            deadline: None,
        }
    }

    /// The exact length of the body, when known in advance.
    pub fn exact_len(&self) -> Option<u64> {
        self.exact_len
    }

    /// Reading more than `max_size` bytes fails with [`HttpClientError::BodyTooLarge`].
    pub fn set_max_size(&mut self, max_size: Option<usize>) {
        self.max_size = max_size;
    }

    /// Reading after `deadline` fails with [`HttpClientError::Timeout`] of `timeout`.
    pub fn set_deadline(&mut self, deadline: tokio::time::Instant, timeout: Duration) {
        self.deadline = Some((deadline, timeout));
    }

    async fn next_raw(&mut self) -> Result<Option<Bytes>, HttpClientError> {
        match &mut self.source {
            BodySource::Bytes(bytes) => Ok(bytes.take()),
            BodySource::Stream(stream) => match self.deadline {
                Some((deadline, timeout)) => tokio::time::timeout_at(deadline, stream.next_chunk())
                    .await
                    .map_err(|_| HttpClientError::Timeout(timeout))?,
                None => stream.next_chunk().await,
            },
        }
    }

    /// The next chunk of the body, or `None` at its end.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>, HttpClientError> {
        if let Some(limit) = self.max_size
            && self.read == 0
            && self.exact_len.is_some_and(|len| len > limit as u64)
        {
            return Err(HttpClientError::BodyTooLarge { limit });
        }
        let chunk = self.next_raw().await?;
        if let Some(chunk) = &chunk {
            self.read += chunk.len();
            if let Some(limit) = self.max_size
                && self.read > limit
            {
                return Err(HttpClientError::BodyTooLarge { limit });
            }
        }
        Ok(chunk)
    }

    /// The whole body. A body of a single chunk is not copied.
    pub async fn bytes(mut self) -> Result<Bytes, HttpClientError> {
        let Some(first) = self.chunk().await? else {
            return Ok(Bytes::new());
        };
        let Some(second) = self.chunk().await? else {
            return Ok(first);
        };
        let mut buffer = body_buffer(self.exact_len, self.max_size);
        buffer.extend_from_slice(&first);
        buffer.extend_from_slice(&second);
        while let Some(chunk) = self.chunk().await? {
            buffer.extend_from_slice(&chunk);
        }
        Ok(buffer.freeze())
    }
}

/// A response whose body has not been read yet: the status and headers arrived, and the body is
/// read on demand ([`bytes`](Self::bytes)). Dropping it without reading the body aborts the
/// download.
#[derive(Debug)]
pub struct StreamingResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    /// The URL the response came from, after redirects.
    pub url: Url,
    /// Every URL visited before `url`, in order.
    pub redirects: Vec<Url>,
    pub body: ResponseBody,
}

impl StreamingResponse {
    pub fn header(&self, name: impl http::header::AsHeaderName) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    /// Reads the body.
    pub async fn bytes(self) -> Result<Bytes, HttpClientError> {
        self.body.bytes().await
    }

    /// Reads the body into an [`HttpResponse`].
    pub async fn into_response(self) -> Result<HttpResponse, HttpClientError> {
        let StreamingResponse { status, headers, url, redirects, body } = self;
        Ok(HttpResponse { status, headers, url, redirects, body: body.bytes().await? })
    }
}

/// Per-request options.
#[derive(Clone, Debug, Default)]
pub struct SendOptions {
    pub proxy_url: Option<Url>,
    /// Cookies are read from and written to this jar. Without one, cookies still flow across
    /// redirects within the request.
    pub cookie_jar: Option<Arc<CookieJar>>,
    /// Timeout for the whole request, redirects and body included.
    pub timeout: Option<Duration>,
    pub ignore_tls_errors: bool,
    /// Responses with larger bodies fail with [`HttpClientError::BodyTooLarge`].
    pub max_body_size: Option<usize>,
    /// The fingerprint of the session the request is sent for. Clients that impersonate browsers
    /// use it; others ignore it.
    pub fingerprint: Option<Arc<SessionFingerprint>>,
}

/// Options a [`Transport`] receives for one hop.
#[derive(Clone, Debug, Default)]
pub struct TransportOptions {
    pub proxy_url: Option<Url>,
    pub ignore_tls_errors: bool,
    /// For [`Transport::fetch`]; the body of a [`Transport::fetch_streaming`] response is limited
    /// by the caller.
    pub max_body_size: Option<usize>,
    /// See [`SendOptions::fingerprint`].
    pub fingerprint: Option<Arc<SessionFingerprint>>,
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
#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn send_request(&self, request: HttpRequest, options: &SendOptions) -> Result<HttpResponse, HttpClientError>;

    /// Like [`send_request`](Self::send_request), but returns once the headers arrived; the body
    /// is read from the response, within the same timeout and size limit. The default reads the
    /// whole response with `send_request` first.
    async fn send_request_streaming(
        &self,
        request: HttpRequest,
        options: &SendOptions,
    ) -> Result<StreamingResponse, HttpClientError> {
        Ok(self.send_request(request, options).await?.into())
    }

    /// Nothing sends through this proxy anymore (the session of a crawler without a session pool
    /// finished its attempt): what is kept for it, such as its connections, can go.
    fn release_proxy(&self, _proxy_url: &Url) {}
}

/// A single request/response exchange, without following redirects or handling cookies.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn fetch(&self, request: HttpRequest, options: &TransportOptions) -> Result<HttpResponse, HttpClientError>;

    /// Like [`fetch`](Self::fetch), but returns once the headers arrived, with the body unread.
    /// The default reads the whole response with `fetch` first.
    async fn fetch_streaming(
        &self,
        request: HttpRequest,
        options: &TransportOptions,
    ) -> Result<StreamingResponse, HttpClientError> {
        Ok(self.fetch(request, options).await?.into())
    }

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

    async fn send_inner(
        &self,
        initial: HttpRequest,
        options: &SendOptions,
    ) -> Result<StreamingResponse, HttpClientError> {
        let jar = options.cookie_jar.clone().unwrap_or_else(|| Arc::new(CookieJar::new()));
        let transport_options = TransportOptions {
            proxy_url: options.proxy_url.clone(),
            ignore_tls_errors: options.ignore_tls_errors,
            max_body_size: options.max_body_size,
            fingerprint: options.fingerprint.clone(),
        };

        let mut current = initial;
        let mut redirects = Vec::new();

        loop {
            let mut hop = current.clone();
            apply_cookies(&mut hop, &jar);

            let mut response = self.transport.fetch_streaming(hop, &transport_options).await?;
            jar.store_response_cookies(
                response.headers.get_all(SET_COOKIE).iter().filter_map(|v| v.to_str().ok()),
                &response.url,
            );

            let location =
                response.status.is_redirection().then(|| response.header(LOCATION).map(str::to_owned)).flatten();
            let Some(location) = location else {
                response.redirects = redirects;
                response.body.set_max_size(options.max_body_size);
                return Ok(response);
            };
            drain_redirect_body(response.body).await;

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

/// Reads a small redirect body, so that its connection can be reused; a larger one is dropped
/// and its connection closed.
async fn drain_redirect_body(mut body: ResponseBody) {
    const MAX_DRAINED: usize = 64 * 1024;
    if body.exact_len().is_some_and(|len| len > MAX_DRAINED as u64) {
        return;
    }
    body.set_max_size(Some(MAX_DRAINED));
    while let Ok(Some(_)) = body.chunk().await {}
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

#[async_trait]
impl<T: Transport> HttpClient for RedirectingClient<T> {
    async fn send_request(&self, request: HttpRequest, options: &SendOptions) -> Result<HttpResponse, HttpClientError> {
        self.send_request_streaming(request, options).await?.into_response().await
    }

    async fn send_request_streaming(
        &self,
        request: HttpRequest,
        options: &SendOptions,
    ) -> Result<StreamingResponse, HttpClientError> {
        let Some(timeout) = options.timeout else {
            return self.send_inner(request, options).await;
        };
        // One deadline for the headers (redirects included) and the body.
        let deadline = tokio::time::Instant::now() + timeout;
        let mut response = tokio::time::timeout_at(deadline, self.send_inner(request, options))
            .await
            .map_err(|_| HttpClientError::Timeout(timeout))??;
        response.body.set_deadline(deadline, timeout);
        Ok(response)
    }

    fn release_proxy(&self, proxy_url: &Url) {
        self.transport.release_proxy(proxy_url);
    }
}

#[async_trait]
impl<C: HttpClient + ?Sized> HttpClient for Arc<C> {
    async fn send_request(&self, request: HttpRequest, options: &SendOptions) -> Result<HttpResponse, HttpClientError> {
        (**self).send_request(request, options).await
    }

    async fn send_request_streaming(
        &self,
        request: HttpRequest,
        options: &SendOptions,
    ) -> Result<StreamingResponse, HttpClientError> {
        (**self).send_request_streaming(request, options).await
    }

    fn release_proxy(&self, proxy_url: &Url) {
        (**self).release_proxy(proxy_url);
    }
}

/// The default client: [`ReqwestTransport`] behind the shared redirect and cookie logic.
#[cfg(feature = "reqwest")]
pub fn default_client() -> Arc<dyn HttpClient> {
    Arc::new(RedirectingClient::new(ReqwestTransport::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    type ScriptedResponse = (u16, Vec<(&'static str, &'static str)>);

    /// Serves scripted responses and records what it was asked for.
    #[derive(Default)]
    struct Scripted {
        responses: Mutex<Vec<ScriptedResponse>>,
        seen: Mutex<Vec<HttpRequest>>,
    }

    #[async_trait]
    impl Transport for Scripted {
        async fn fetch(&self, request: HttpRequest, _: &TransportOptions) -> Result<HttpResponse, HttpClientError> {
            let (status, headers) = self.responses.lock().remove(0);
            let mut map = HeaderMap::new();
            for (name, value) in headers {
                map.append(http::HeaderName::from_static(name), HeaderValue::from_static(value));
            }
            let url = request.url.clone();
            self.seen.lock().push(request);
            Ok(HttpResponse {
                status: StatusCode::from_u16(status).unwrap(),
                headers: map,
                url,
                body: Bytes::new(),
                redirects: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn follows_redirects_with_cookies_and_method_rewrites() {
        let transport = Scripted::default();
        *transport.responses.lock() = vec![
            (302, vec![("location", "/next"), ("set-cookie", "sid=1; Path=/")]),
            (301, vec![("location", "https://other.dev/final")]),
            (200, vec![]),
        ];
        let client = RedirectingClient::new(transport);

        let mut request = HttpRequest::get(Url::parse("https://example.com/start").unwrap());
        request.method = Method::POST;
        request.body = Some(Bytes::from_static(b"q=1"));
        request.headers.insert(AUTHORIZATION, HeaderValue::from_static("secret"));

        let jar = Arc::new(CookieJar::new());
        let response = client
            .send_request(request, &SendOptions { cookie_jar: Some(jar.clone()), ..Default::default() })
            .await
            .unwrap();

        assert_eq!(response.url.as_str(), "https://other.dev/final");
        assert_eq!(response.redirects.len(), 2);

        let seen = client.transport().seen.lock();
        assert_eq!(seen[1].method, Method::GET, "302 after POST becomes GET");
        assert!(seen[1].body.is_none());
        assert_eq!(seen[1].headers.get(COOKIE).unwrap(), "sid=1");
        assert_eq!(seen[1].headers.get(AUTHORIZATION).unwrap(), "secret", "same origin keeps credentials");
        assert!(seen[2].headers.get(AUTHORIZATION).is_none(), "cross-origin drops credentials");
        assert!(seen[2].headers.get(COOKIE).is_none());
        assert_eq!(jar.cookie_header(&Url::parse("https://example.com/").unwrap()).as_deref(), Some("sid=1"));
    }

    /// A body that yields its chunks one by one, then waits forever.
    struct Chunks(Vec<&'static [u8]>);

    #[async_trait]
    impl BodyStream for Chunks {
        async fn next_chunk(&mut self) -> Result<Option<Bytes>, HttpClientError> {
            if self.0.is_empty() {
                std::future::pending::<()>().await;
            }
            Ok(Some(Bytes::from_static(self.0.remove(0))))
        }
    }

    #[test]
    fn a_known_length_within_the_limit_preallocates_the_body() {
        assert_eq!(body_buffer(Some(4096), Some(10_000)).capacity(), 4096);
        assert_eq!(body_buffer(Some(4096), None).capacity(), 4096);
        assert_eq!(body_buffer(Some(20_000), Some(10_000)).capacity(), 0, "too large: fails before reading");
    }

    #[tokio::test]
    async fn body_limits_and_deadlines_apply_when_reading() {
        let mut body = ResponseBody::from_stream(Chunks(vec![b"abc"]), Some(10));
        body.set_max_size(Some(5));
        assert!(matches!(body.chunk().await, Err(HttpClientError::BodyTooLarge { limit: 5 })), "by the length");

        let mut body = ResponseBody::from_stream(Chunks(vec![b"abc", b"def"]), None);
        body.set_max_size(Some(5));
        assert!(matches!(body.bytes().await, Err(HttpClientError::BodyTooLarge { limit: 5 })));

        let mut body = ResponseBody::from_stream(Chunks(vec![b"abc", b"def"]), None);
        let timeout = Duration::from_millis(50);
        body.set_deadline(tokio::time::Instant::now() + timeout, timeout);
        assert_eq!(body.chunk().await.unwrap().unwrap(), "abc");
        assert_eq!(body.chunk().await.unwrap().unwrap(), "def");
        assert!(matches!(body.chunk().await, Err(HttpClientError::Timeout(t)) if t == timeout));
    }

    #[tokio::test]
    async fn stops_after_too_many_redirects() {
        let transport = Scripted::default();
        *transport.responses.lock() = (0..=MAX_REDIRECTS).map(|_| (302, vec![("location", "/loop")])).collect();
        let client = RedirectingClient::new(transport);
        let err = client
            .send_request(HttpRequest::get(Url::parse("https://example.com/").unwrap()), &SendOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, HttpClientError::TooManyRedirects { max: MAX_REDIRECTS, .. }));
    }
}
