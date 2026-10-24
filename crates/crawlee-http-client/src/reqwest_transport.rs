//! [`Transport`] built on `reqwest` (hyper + rustls).
//!
//! One `reqwest::Client` is kept per (proxy, TLS verification) combination, so connections,
//! TLS sessions and HTTP/2 streams are reused across requests instead of being set up per call.
//! Clients share one TLS configuration per verification mode, and the clients of proxies no longer
//! used are dropped: without a session pool, every request can have a proxy URL of its own.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderValue;
use http::header::{ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use parking_lot::Mutex;

use crate::{
    BodyStream, HttpClientError, HttpRequest, HttpResponse, ResponseBody, StreamingResponse, Transport,
    TransportOptions,
};

/// User agent sent when the request does not set one.
pub const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36";

type ClientKey = (Option<String>, bool);

struct CachedClient {
    client: reqwest::Client,
    last_used: Instant,
}

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    // As reqwest: a crypto provider installed by the application, else aws-lc-rs.
    rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
}

fn build_tls_config(
    provider: Arc<rustls::crypto::CryptoProvider>,
    verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
) -> Result<rustls::ClientConfig, String> {
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| err.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

fn cached_config(
    cell: &'static OnceLock<Result<rustls::ClientConfig, String>>,
    init: impl FnOnce() -> Result<rustls::ClientConfig, String>,
) -> Result<&'static rustls::ClientConfig, HttpClientError> {
    cell.get_or_init(init).as_ref().map_err(|err| HttpClientError::Other(format!("TLS configuration: {err}")))
}

/// The TLS configuration of every client that verifies certificates, built once. reqwest builds
/// one per client, and with it a certificate verifier that loads the system's root certificates
/// again.
fn tls_config() -> Result<&'static rustls::ClientConfig, HttpClientError> {
    static CONFIG: OnceLock<Result<rustls::ClientConfig, String>> = OnceLock::new();
    cached_config(&CONFIG, || {
        let provider = crypto_provider();
        let verifier = rustls_platform_verifier::Verifier::new(provider.clone()).map_err(|err| err.to_string())?;
        build_tls_config(provider, Arc::new(verifier))
    })
}

/// The TLS configuration of every client that ignores certificate errors (the crawler's default),
/// built once too.
fn insecure_tls_config() -> Result<&'static rustls::ClientConfig, HttpClientError> {
    static CONFIG: OnceLock<Result<rustls::ClientConfig, String>> = OnceLock::new();
    cached_config(&CONFIG, || {
        let provider = crypto_provider();
        build_tls_config(provider.clone(), Arc::new(AcceptAnyCertificate(provider)))
    })
}

/// Accepts every certificate; signatures are still checked, as reqwest's own verifier does.
#[derive(Debug)]
struct AcceptAnyCertificate(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `reqwest`-based transport with transparent gzip, brotli and deflate decompression.
pub struct ReqwestTransport {
    clients: Mutex<HashMap<ClientKey, CachedClient>>,
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
            clients: Mutex::new(HashMap::new()),
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
        if let Some(cached) = self.clients.lock().get_mut(&key) {
            cached.last_used = now;
            return Ok(cached.client.clone());
        }

        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.connect_timeout)
            .pool_idle_timeout(self.pool_idle_timeout)
            .tcp_nodelay(true);
        let tls = if options.ignore_tls_errors { insecure_tls_config()? } else { tls_config()? };
        builder = builder.tls_backend_preconfigured(tls.clone());
        // Without an explicit proxy, the system proxy settings (`HTTPS_PROXY`, `NO_PROXY`, ...)
        // apply, as with curl.
        if let Some(proxy) = &options.proxy_url {
            builder = builder
                .proxy(reqwest::Proxy::all(proxy.as_str()).map_err(|err| HttpClientError::Proxy(err.to_string()))?);
        }
        let client = builder.build().map_err(|err| HttpClientError::Other(err.to_string()))?;

        let mut clients = self.clients.lock();
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

#[async_trait]
impl Transport for ReqwestTransport {
    fn release_proxy(&self, proxy_url: &url::Url) {
        let proxy_url = proxy_url.as_str();
        self.clients.lock().retain(|(proxy, _), _| proxy.as_deref() != Some(proxy_url));
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
        let exact_len = response.content_length();
        let body = ResponseBody::from_stream(ReqwestBody { response, via_proxy }, exact_len);
        Ok(StreamingResponse { status, headers, url, body, redirects: Vec::new() })
    }
}

/// The body of a reqwest response, read chunk by chunk. Dropping it aborts the download.
struct ReqwestBody {
    response: reqwest::Response,
    via_proxy: bool,
}

#[async_trait]
impl BodyStream for ReqwestBody {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, HttpClientError> {
        self.response.chunk().await.map_err(|err| map_error(err, self.via_proxy))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HttpClient, RedirectingClient, SendOptions};
    use axum::Router;
    use axum::response::{IntoResponse, Redirect};
    use axum::routing::get;
    use futures::StreamExt as _;
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
            .route("/big", get(|| async { "x".repeat(10_000) }))
            .route(
                "/slow",
                get(|| async {
                    // The headers and a first chunk now, the rest never.
                    let chunks = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"first"))])
                        .chain(futures::stream::pending());
                    axum::body::Body::from_stream(chunks)
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
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
        assert_eq!(transport.clients.lock().len(), 20);
        std::thread::sleep(Duration::from_millis(400));
        transport.client(&via_proxy(20)).unwrap();
        assert_eq!(transport.clients.lock().len(), 1, "only the client just made is left");
    }

    /// A proxy that nothing will use again (the session of a crawler without a session pool
    /// finished its attempt) gives its client up right away.
    #[test]
    fn a_released_proxy_drops_its_client() {
        let transport = ReqwestTransport::new();
        transport.client(&via_proxy(0)).unwrap();
        transport.client(&via_proxy(1)).unwrap();
        transport.release_proxy(via_proxy(0).proxy_url.as_ref().unwrap());
        let clients = transport.clients.lock();
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

        // Ignoring TLS errors (the crawler's default) costs no configuration per client either.
        let insecure = insecure_tls_config().unwrap() as *const rustls::ClientConfig;
        assert_eq!(insecure, insecure_tls_config().unwrap() as *const _);
        assert_ne!(insecure, first);
        assert_eq!(insecure_tls_config().unwrap().alpn_protocols, [b"h2".to_vec(), b"http/1.1".to_vec()]);
    }

    /// The response is handed over once the headers arrived: the body is read on demand, within
    /// the request's timeout.
    #[tokio::test]
    async fn streaming_returns_before_the_body_is_read() {
        let base = serve().await;
        let client = RedirectingClient::new(ReqwestTransport::new());
        let timeout = Duration::from_millis(300);
        let mut response = client
            .send_request_streaming(
                HttpRequest::get(base.join("/slow").unwrap()),
                &SendOptions { timeout: Some(timeout), ..Default::default() },
            )
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body.chunk().await.unwrap().unwrap(), "first");
        assert!(matches!(response.body.chunk().await, Err(HttpClientError::Timeout(t)) if t == timeout));
    }

    #[tokio::test]
    async fn real_http_roundtrip() {
        let base = serve().await;
        let client = RedirectingClient::new(ReqwestTransport::new());
        let jar = std::sync::Arc::new(crate::CookieJar::new());

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
