//! [`Transport`] built on `reqwest` (hyper + rustls).
//!
//! One `reqwest::Client` is kept per (proxy, TLS verification) combination, so connections,
//! TLS sessions and HTTP/2 streams are reused across requests instead of being set up per call.
//! Clients share one TLS configuration, and the clients of proxies no longer used are dropped:
//! without a session pool, every request has a proxy URL of its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::HeaderValue;
use http::header::{ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use std::cell::RefCell;

use crate::{HttpClientError, HttpRequest, HttpResponse, Transport, TransportOptions};

/// User agent sent when the request does not set one.
pub const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36";

type ClientKey = (Option<String>, bool);

struct CachedClient {
    client: reqwest::Client,
    last_used: Instant,
}

/// The TLS configuration of every client, built once. reqwest builds one per client, and with it
/// a certificate verifier that loads the system's root certificates again.
fn tls_config() -> Result<&'static rustls::ClientConfig, HttpClientError> {
    static CONFIG: OnceLock<Result<rustls::ClientConfig, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            // As reqwest: a crypto provider installed by the application, else aws-lc-rs.
            let provider = rustls::crypto::CryptoProvider::get_default()
                .cloned()
                .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
            let verifier = rustls_platform_verifier::Verifier::new(provider.clone()).map_err(|err| err.to_string())?;
            let mut config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|err| err.to_string())?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth();
            config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            Ok(config)
        })
        .as_ref()
        .map_err(|err| HttpClientError::Other(format!("TLS configuration: {err}")))
}

/// `reqwest`-based transport with transparent gzip, brotli and deflate decompression.
pub struct ReqwestTransport {
    clients: RefCell<HashMap<ClientKey, CachedClient>>,
    connect_timeout: Duration,
    pool_idle_timeout: Duration,
}

impl std::fmt::Debug for ReqwestTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestTransport").finish_non_exhaustive()
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ReqwestTransport {
    pub fn new() -> Self {
        ReqwestTransport {
            clients: RefCell::new(HashMap::new()),
            connect_timeout: Duration::from_secs(30),
            pool_idle_timeout: Duration::from_secs(90),
        }
    }

    /// How long idle connections stay open (90 seconds by default). Clients of proxies not used
    /// for that long are dropped.
    pub fn with_pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.pool_idle_timeout = timeout;
        self
    }

    fn client(&self, options: &TransportOptions) -> Result<reqwest::Client, HttpClientError> {
        let key = (options.proxy_url.as_ref().map(|u| u.to_string()), options.ignore_tls_errors);
        let now = Instant::now();
        if let Some(cached) = self.clients.borrow_mut().get_mut(&key) {
            cached.last_used = now;
            return Ok(cached.client.clone());
        }

        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.connect_timeout)
            .pool_idle_timeout(self.pool_idle_timeout)
            .tcp_nodelay(true);
        builder = if options.ignore_tls_errors {
            builder.tls_danger_accept_invalid_certs(true)
        } else {
            builder.tls_backend_preconfigured(tls_config()?.clone())
        };
        // Without an explicit proxy, the system proxy settings (`HTTPS_PROXY`, `NO_PROXY`, ...)
        // apply, as with curl.
        if let Some(proxy) = &options.proxy_url {
            builder = builder
                .proxy(reqwest::Proxy::all(proxy.as_str()).map_err(|err| HttpClientError::Proxy(err.to_string()))?);
        }
        let client = builder.build().map_err(|err| HttpClientError::Other(err.to_string()))?;

        let mut clients = self.clients.borrow_mut();
        // Clients idle for longer than their connections are kept open have nothing left to
        // reuse: the session that used the proxy was retired, or it was a single request.
        let idle_timeout = self.pool_idle_timeout;
        clients.retain(|_, cached| now.duration_since(cached.last_used) < idle_timeout);
        Ok(clients.entry(key).or_insert(CachedClient { client, last_used: now }).client.clone())
    }
}

fn map_error(err: reqwest::Error, via_proxy: bool) -> HttpClientError {
    let message = error_chain(&err);
    if via_proxy && (err.is_connect() || message.to_ascii_lowercase().contains("proxy")) {
        HttpClientError::Proxy(message)
    } else if err.is_connect() {
        HttpClientError::Connect(message)
    } else {
        HttpClientError::Other(message)
    }
}

fn error_chain(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

#[async_trait(?Send)]
impl Transport for ReqwestTransport {
    fn release_proxy(&self, proxy_url: &url::Url) {
        let proxy_url = proxy_url.as_str();
        self.clients.borrow_mut().retain(|(proxy, _), _| proxy.as_deref() != Some(proxy_url));
    }

    async fn fetch(&self, request: HttpRequest, options: &TransportOptions) -> Result<HttpResponse, HttpClientError> {
        let client = self.client(options)?;
        let via_proxy = options.proxy_url.is_some();

        let mut headers = request.headers;
        headers.entry(USER_AGENT).or_insert(HeaderValue::from_static(DEFAULT_USER_AGENT));
        headers
            .entry(ACCEPT)
            .or_insert(HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"));
        headers.entry(ACCEPT_LANGUAGE).or_insert(HeaderValue::from_static("en-US,en;q=0.9"));

        let mut builder = client.request(request.method, request.url.as_str()).headers(headers);
        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        let mut response = builder.send().await.map_err(|err| map_error(err, via_proxy))?;

        let status = response.status();
        let headers = std::mem::take(response.headers_mut());
        let url = response.url().clone();

        let body = match options.max_body_size {
            None => response.bytes().await.map_err(|err| map_error(err, via_proxy))?,
            Some(limit) => {
                if response.content_length().is_some_and(|len| len as usize > limit) {
                    return Err(HttpClientError::BodyTooLarge { limit });
                }
                let mut buffer = BytesMut::new();
                while let Some(chunk) = response.chunk().await.map_err(|err| map_error(err, via_proxy))? {
                    if buffer.len() + chunk.len() > limit {
                        return Err(HttpClientError::BodyTooLarge { limit });
                    }
                    buffer.extend_from_slice(&chunk);
                }
                Bytes::from(buffer)
            }
        };

        Ok(HttpResponse { status, headers, url, body, redirects: Vec::new() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HttpClient, RedirectingClient, SendOptions};
    use axum::Router;
    use axum::response::{IntoResponse, Redirect};
    use axum::routing::get;
    use url::Url;

    async fn serve() -> Url {
        let app = Router::new()
            .route("/", get(|| async { Redirect::to("/landing") }))
            .route(
                "/landing",
                get(|headers: http::HeaderMap| async move {
                    let ua = headers.get(USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
                    ([("set-cookie", "visited=yes")], format!("ua={ua}")).into_response()
                }),
            )
            .route("/big", get(|| async { "x".repeat(10_000) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::task::spawn_local(async move { axum::serve(listener, app).await.unwrap() });
        Url::parse(&format!("http://{addr}/")).unwrap()
    }

    fn via_proxy(n: usize) -> TransportOptions {
        let proxy = Url::parse(&format!("http://session-{n}:secret@proxy.example:8000")).unwrap();
        TransportOptions { proxy_url: Some(proxy), ..Default::default() }
    }

    /// Without a session pool every request has a proxy URL of its own: the clients of the URLs
    /// that are no longer used must not pile up.
    #[test]
    fn clients_of_idle_proxies_are_dropped() {
        // Loading the root certificates for the shared TLS configuration takes a while once.
        tls_config().unwrap();
        let transport = ReqwestTransport::new().with_pool_idle_timeout(Duration::from_millis(300));
        for n in 0..20 {
            transport.client(&via_proxy(n)).unwrap();
        }
        assert_eq!(transport.clients.borrow_mut().len(), 20);
        std::thread::sleep(Duration::from_millis(400));
        transport.client(&via_proxy(20)).unwrap();
        assert_eq!(transport.clients.borrow_mut().len(), 1, "only the client just made is left");
    }

    /// A proxy that nothing will use again (the session of a crawler without a session pool
    /// finished its attempt) gives its client up right away.
    #[test]
    fn a_released_proxy_drops_its_client() {
        let transport = ReqwestTransport::new();
        transport.client(&via_proxy(0)).unwrap();
        transport.client(&via_proxy(1)).unwrap();
        transport.release_proxy(via_proxy(0).proxy_url.as_ref().unwrap());
        let clients = transport.clients.borrow_mut();
        assert_eq!(clients.len(), 1);
        assert!(
            clients.keys().all(|(proxy, _)| proxy.as_deref() == Some("http://session-1:secret@proxy.example:8000/"))
        );
    }

    /// Clients share one TLS configuration (and so one certificate verifier with its root
    /// store), instead of loading the system's certificates once per proxy URL.
    #[test]
    fn clients_share_the_tls_configuration() {
        let first = tls_config().unwrap() as *const rustls::ClientConfig;
        assert_eq!(first, tls_config().unwrap() as *const _);
        assert_eq!(tls_config().unwrap().alpn_protocols, [b"h2".to_vec(), b"http/1.1".to_vec()]);
    }

    #[tokio::test(flavor = "local")]
    async fn real_http_roundtrip() {
        let base = serve().await;
        let client = RedirectingClient::new(ReqwestTransport::new());
        let jar = std::rc::Rc::new(crate::CookieJar::new());

        let response = client
            .send_request(
                HttpRequest::get(base.clone()),
                &SendOptions { cookie_jar: Some(jar.clone()), ..Default::default() },
            )
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.url.path(), "/landing");
        assert_eq!(response.body, Bytes::from(format!("ua={DEFAULT_USER_AGENT}")));
        assert_eq!(jar.cookie_header(&base).as_deref(), Some("visited=yes"));

        let err = client
            .send_request(
                HttpRequest::get(base.join("/big").unwrap()),
                &SendOptions { max_body_size: Some(1000), ..Default::default() },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, HttpClientError::BodyTooLarge { limit: 1000 }));
    }
}
