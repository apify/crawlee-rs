//! [impit](https://github.com/apify/impit) transport for crawlee-rs.
//!
//! impit makes requests with the TLS and HTTP/2 fingerprints of real browsers (Chrome, Firefox,
//! Safari), which many anti-bot systems check. It is the default HTTP client of Crawlee for JS.
//!
//! ```no_run
//! use std::rc::Rc;
//!
//! use crawlee::http_client::{HttpClient, RedirectingClient};
//! use crawlee::{HtmlContext, HtmlCrawler};
//! use crawlee_impit::{Browser, ImpitTransport};
//!
//! # async fn run() -> anyhow::Result<()> {
//! let client: Rc<dyn HttpClient> = Rc::new(RedirectingClient::new(ImpitTransport::new(Browser::Chrome)));
//! let crawler = HtmlCrawler::builder()
//!     .http_client(client)
//!     .request_handler(|ctx: HtmlContext| async move { Ok(()) })
//!     .build()?;
//! # Ok(()) }
//! ```
//!
//! The transport does a single exchange; redirects, cookies and timeouts are handled by
//! [`RedirectingClient`](crawlee_http_client::RedirectingClient), like for every other transport.

use std::collections::HashMap;
use std::rc::Rc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::Method;
use impit::errors::ImpitError;
use impit::fingerprint::BrowserFingerprint;
use impit::fingerprint::database as fingerprints;
use impit::impit::{Impit, RedirectBehavior};
use impit::request::{ImpitBody, RequestOptions};
use std::cell::RefCell;

use crawlee_http_client::{HttpClientError, HttpRequest, HttpResponse, Transport, TransportOptions};

/// The browser impit impersonates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Browser {
    #[default]
    Chrome,
    Firefox,
    Safari,
}

impl Browser {
    fn fingerprint(self) -> BrowserFingerprint {
        match self {
            Browser::Chrome => fingerprints::chrome_142::fingerprint(),
            Browser::Firefox => fingerprints::firefox_144::fingerprint(),
            Browser::Safari => fingerprints::ios_18::fingerprint(),
        }
    }
}

type ClientKey = (Option<String>, bool);

/// A [`Transport`] backed by impit. One impit client is kept per (proxy, TLS verification) pair,
/// so connections are reused.
pub struct ImpitTransport {
    browser: Browser,
    clients: RefCell<HashMap<ClientKey, Rc<Impit<reqwest::cookie::Jar>>>>,
}

impl std::fmt::Debug for ImpitTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImpitTransport").field("browser", &self.browser).finish_non_exhaustive()
    }
}

impl Default for ImpitTransport {
    fn default() -> Self {
        Self::new(Browser::default())
    }
}

impl ImpitTransport {
    pub fn new(browser: Browser) -> Self {
        ImpitTransport { browser, clients: RefCell::new(HashMap::new()) }
    }

    pub fn browser(&self) -> Browser {
        self.browser
    }

    fn client(&self, options: &TransportOptions) -> Result<Rc<Impit<reqwest::cookie::Jar>>, HttpClientError> {
        let key = (options.proxy_url.as_ref().map(|u| u.to_string()), options.ignore_tls_errors);
        if let Some(client) = self.clients.borrow_mut().get(&key) {
            return Ok(client.clone());
        }

        // Redirects and cookies are handled by `RedirectingClient`; impit does a single hop.
        let mut builder = Impit::<reqwest::cookie::Jar>::builder()
            .with_fingerprint(self.browser.fingerprint())
            .with_redirect(RedirectBehavior::ManualRedirect)
            .with_ignore_tls_errors(options.ignore_tls_errors);
        if let Some(proxy) = &options.proxy_url {
            builder = builder.with_proxy(proxy.to_string());
        }
        let client = Rc::new(builder.build().map_err(|err| map_error(err, options.proxy_url.is_some()))?);
        Ok(self.clients.borrow_mut().entry(key).or_insert(client).clone())
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

#[async_trait(?Send)]
impl Transport for ImpitTransport {
    fn release_proxy(&self, proxy_url: &url::Url) {
        let proxy_url = proxy_url.as_str();
        self.clients.borrow_mut().retain(|(proxy, _), _| proxy.as_deref() != Some(proxy_url));
    }

    async fn fetch(&self, request: HttpRequest, options: &TransportOptions) -> Result<HttpResponse, HttpClientError> {
        let client = self.client(options)?;
        let via_proxy = options.proxy_url.is_some();

        let headers: Vec<(String, String)> = request
            .headers
            .iter()
            .filter_map(|(name, value)| Some((name.as_str().to_owned(), value.to_str().ok()?.to_owned())))
            .collect();
        // Timeouts are enforced by `RedirectingClient` around the whole exchange.
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

        let mut buffer = BytesMut::new();
        while let Some(chunk) = response.chunk().await.map_err(|err| HttpClientError::Other(err.to_string()))? {
            if let Some(limit) = options.max_body_size
                && buffer.len() + chunk.len() > limit
            {
                return Err(HttpClientError::BodyTooLarge { limit });
            }
            buffer.extend_from_slice(&chunk);
        }

        Ok(HttpResponse { status, headers, url, body: Bytes::from(buffer), redirects: Vec::new() })
    }
}
