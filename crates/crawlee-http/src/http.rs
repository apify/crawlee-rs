//! [`HttpCrawler`]: fetches each request over HTTP and hands the response to the handler.

use std::collections::HashSet;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use encoding_rs::{Encoding, UTF_8};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use serde::de::DeserializeOwned;
use url::Url;

use crawlee_basic::{
    BasicContext, BasicCrawlerBuilder, ConcurrencyOptions, CrawlingContext, Middleware, RequestSkipped,
};
use crawlee_core::errors::{NonRetryableError, RequestThrottledError, SessionError};
use crawlee_http_client::{HttpClientError, HttpRequest, SendOptions, StreamingResponse};
use crawlee_utils::{extract_charset_from_html_bytes, matches_enqueue_strategy};

use crate::body::{Body, encoding_for_label};
use crate::html::{Document, Element, Selection};

type HookFuture<T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send>>;

/// Called before the request is sent, with the crawling context (`preNavigationHooks`). It may
/// change what is sent through `ctx.request_mut()` (URL, method, headers, payload) and hands the
/// context on; an error fails the request.
pub type PreNavigationHook = Arc<dyn Fn(BasicContext) -> HookFuture<BasicContext> + Send + Sync>;
/// Called once the response headers arrived, before its body is read (`postNavigationHooks`). It
/// may replace the response, or reject it by returning an error. Responses of an unsupported
/// content type are rejected before the hooks.
pub type PostNavigationHook = Arc<dyn Fn(PostNavigationContext) -> HookFuture<PostNavigationContext> + Send + Sync>;

/// The context of a post-navigation hook (`CrawlingContextWithResponse`): the crawling context
/// and the response, whose body is not read yet. A hook may replace `response`, as returning
/// `{ response }` does in Crawlee for JS.
pub struct PostNavigationContext {
    context: BasicContext,
    pub response: StreamingResponse,
}

impl std::fmt::Debug for PostNavigationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostNavigationContext").field("response", &self.response).finish_non_exhaustive()
    }
}

impl Deref for PostNavigationContext {
    type Target = BasicContext;
    fn deref(&self) -> &BasicContext {
        &self.context
    }
}

impl DerefMut for PostNavigationContext {
    fn deref_mut(&mut self) -> &mut BasicContext {
        &mut self.context
    }
}

/// Elements of pages that block crawlers (`RETRY_CSS_SELECTORS`), checked with
/// [`HttpCrawlerOptions::retry_on_blocked`].
pub const RETRY_CSS_SELECTORS: [&str; 3] = [
    r#"#turnstile-wrapper iframe[src^="https://challenges.cloudflare.com"]"#,
    r#"div#infoDiv0 a[href*="//www.google.com/policies/terms/"]"#,
    r#"iframe[src*="_Incapsula_Resource"]"#,
];

pub const MIME_HTML: &str = "text/html";
pub const MIME_XHTML: &str = "application/xhtml+xml";
pub const MIME_XML: &str = "application/xml";
pub const MIME_TEXT_XML: &str = "text/xml";
pub const MIME_JSON: &str = "application/json";

/// Settings of the HTTP pipeline, with the defaults of Crawlee for JS.
#[derive(Clone)]
pub struct HttpCrawlerOptions {
    /// Timeout of the navigation: the request, redirects and reading the body (30 s).
    pub navigation_timeout: Duration,
    /// MIME types accepted besides HTML, XHTML, XML and JSON. `*/*` accepts everything.
    pub additional_mime_types: Vec<String>,
    /// Status codes that fail the request although they are below 500.
    pub additional_http_error_status_codes: Vec<u16>,
    /// Status codes of 500 and above that do not fail the request.
    pub ignore_http_error_status_codes: Vec<u16>,
    /// Status codes that mean the session got blocked; its session is retired.
    pub blocked_status_codes: Vec<u16>,
    /// Retire the session and retry when the page looks blocked: an HTML page with one of the
    /// [`RETRY_CSS_SELECTORS`], or a blocked status code (`retryOnBlocked`, default `false`).
    pub retry_on_blocked: bool,
    /// Decode every body with this encoding, whatever the response says.
    pub force_response_encoding: Option<&'static Encoding>,
    /// Encoding used when the response does not declare one.
    pub suggest_response_encoding: Option<&'static Encoding>,
    /// Store `Set-Cookie` of responses in the session.
    pub save_response_cookies: bool,
    /// Ignore TLS certificate errors (default `true`, as in Crawlee for JS).
    pub ignore_tls_errors: bool,
    /// Larger bodies fail the request (without retries). No limit by default.
    pub max_body_size: Option<usize>,
    pub pre_navigation_hooks: Vec<PreNavigationHook>,
    pub post_navigation_hooks: Vec<PostNavigationHook>,
}

impl Default for HttpCrawlerOptions {
    fn default() -> Self {
        HttpCrawlerOptions {
            navigation_timeout: Duration::from_secs(30),
            additional_mime_types: Vec::new(),
            additional_http_error_status_codes: Vec::new(),
            ignore_http_error_status_codes: Vec::new(),
            blocked_status_codes: vec![401, 403, 429],
            retry_on_blocked: false,
            force_response_encoding: None,
            suggest_response_encoding: None,
            save_response_cookies: true,
            ignore_tls_errors: true,
            max_body_size: None,
            pre_navigation_hooks: Vec::new(),
            post_navigation_hooks: Vec::new(),
        }
    }
}

impl std::fmt::Debug for HttpCrawlerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpCrawlerOptions")
            .field("navigation_timeout", &self.navigation_timeout)
            .field("additional_mime_types", &self.additional_mime_types)
            .field("blocked_status_codes", &self.blocked_status_codes)
            .finish_non_exhaustive()
    }
}

impl HttpCrawlerOptions {
    /// Adds a [`PreNavigationHook`]. Hooks run in order, within the navigation timeout.
    ///
    /// ```no_run
    /// # use crawlee_http::HttpCrawlerOptions;
    /// let options = HttpCrawlerOptions::default().pre_navigation_hook(|mut ctx| async move {
    ///     ctx.request_mut().headers.insert("Custom-Header".into(), "value".into());
    ///     Ok(ctx)
    /// });
    /// ```
    pub fn pre_navigation_hook<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(BasicContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<BasicContext>> + Send + 'static,
    {
        self.pre_navigation_hooks.push(Arc::new(move |ctx| Box::pin(hook(ctx))));
        self
    }

    /// Adds a [`PostNavigationHook`]. Hooks run in order, within the navigation timeout.
    ///
    /// ```no_run
    /// # use crawlee_http::HttpCrawlerOptions;
    /// let options = HttpCrawlerOptions::default().post_navigation_hook(|ctx| async move {
    ///     if ctx.response.header("x-challenge").is_some() {
    ///         anyhow::bail!("challenge page");
    ///     }
    ///     Ok(ctx)
    /// });
    /// ```
    pub fn post_navigation_hook<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(PostNavigationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<PostNavigationContext>> + Send + 'static,
    {
        self.post_navigation_hooks.push(Arc::new(move |ctx| Box::pin(hook(ctx))));
        self
    }
}

/// The parsed `Content-Type` of a response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentType {
    /// Lower-cased essence, e.g. `text/html`.
    pub mime: String,
    /// The `charset` parameter, if any.
    pub charset: Option<String>,
}

impl ContentType {
    /// The content type of a response (`parseContentTypeFromResponse`): from a valid
    /// `Content-Type` header, else from the extension of the URL's path, else
    /// `application/octet-stream` (RFC 7231, section 3.1.1.5). As `mime.contentType` does, a type
    /// guessed from the extension gets the charset `utf-8` when it is text.
    pub fn from_response(headers: &HeaderMap, url: &Url) -> ContentType {
        // Repeated headers are read joined by ", ", as the Fetch API joins them.
        let header = headers
            .get_all(http::header::CONTENT_TYPE)
            .iter()
            .map(|value| value.as_bytes().iter().map(|&byte| byte as char).collect::<String>())
            .collect::<Vec<_>>()
            .join(", ");
        if !header.is_empty() {
            let parsed = parse_content_type(&header);
            if is_type_valid(&parsed.mime) {
                return parsed;
            }
        }
        let extension = extname(url.path()).get(1..).unwrap_or_default().to_ascii_lowercase();
        let (mime, charset) = crate::mime_types::EXTENSIONS
            .binary_search_by(|(known, _, _)| (*known).cmp(extension.as_str()))
            .map(|index| {
                let (_, mime, charset) = crate::mime_types::EXTENSIONS[index];
                (mime, charset)
            })
            .unwrap_or(("application/octet-stream", Some("utf-8")));
        ContentType { mime: mime.to_owned(), charset: charset.map(str::to_owned) }
    }

    pub fn is_html_or_xml(&self) -> bool {
        matches!(self.mime.as_str(), MIME_HTML | MIME_XHTML | MIME_XML | MIME_TEXT_XML)
    }

    pub fn is_xml(&self) -> bool {
        self.mime.contains("xml") && self.mime != MIME_XHTML
    }

    pub fn is_json(&self) -> bool {
        self.mime == MIME_JSON || self.mime.ends_with("+json")
    }
}

fn is_ows(c: char) -> bool {
    c == ' ' || c == '\t'
}

fn is_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c)
}

/// A type as RFC 9110 allows it: two tokens around one `/` (`isTypeValid`).
fn is_type_valid(mime: &str) -> bool {
    match mime.split_once('/') {
        Some((kind, subtype)) => {
            !kind.is_empty() && !subtype.is_empty() && kind.chars().chain(subtype.chars()).all(is_token_char)
        }
        None => false,
    }
}

/// Parses a `Content-Type` header as the `content-type` package (v3) does: the type
/// lower-cased, unparsable parameters skipped, the first `charset` kept.
fn parse_content_type(header: &str) -> ContentType {
    let (kind, parameters) = match header.split_once(';') {
        Some((kind, parameters)) => (kind, Some(parameters)),
        None => (header, None),
    };
    let mime = kind.trim_matches(is_ows).to_lowercase();
    let charset = parameters.and_then(|parameters| {
        parse_parameters(&parameters.chars().collect::<Vec<_>>()).find(|(key, _)| key == "charset").map(|(_, v)| v)
    });
    ContentType { mime, charset }
}

/// The `key=value` parameters after the type, in order: keys lower-cased, quoted values
/// unescaped, entries without `=` and unterminated quotes skipped.
fn parse_parameters(chars: &[char]) -> impl Iterator<Item = (String, String)> + '_ {
    let len = chars.len();
    let mut index = 0;
    std::iter::from_fn(move || {
        'parameter: while index < len {
            while index < len && is_ows(chars[index]) {
                index += 1;
            }
            let key_start = index;
            let mut key_whitespace = None;
            while index < len {
                match chars[index] {
                    ';' => {
                        index += 1;
                        continue 'parameter;
                    }
                    '=' => {
                        let key: String = chars[key_start..key_whitespace.unwrap_or(index)].iter().collect();
                        index += 1;
                        while index < len && is_ows(chars[index]) {
                            index += 1;
                        }
                        let value = if index < len && chars[index] == '"' {
                            index += 1;
                            let mut value = String::new();
                            loop {
                                match chars.get(index) {
                                    None => return None,
                                    Some('"') => break,
                                    Some('\\') if index + 1 < len => {
                                        value.push(chars[index + 1]);
                                        index += 2;
                                    }
                                    Some(&c) => {
                                        value.push(c);
                                        index += 1;
                                    }
                                }
                            }
                            // Whatever follows the closing quote, up to the next `;`, is dropped.
                            while index < len && chars[index] != ';' {
                                index += 1;
                            }
                            value
                        } else {
                            let value_start = index;
                            let mut value_whitespace = None;
                            while index < len && chars[index] != ';' {
                                if is_ows(chars[index]) {
                                    value_whitespace.get_or_insert(index);
                                } else {
                                    value_whitespace = None;
                                }
                                index += 1;
                            }
                            chars[value_start..value_whitespace.unwrap_or(index)].iter().collect()
                        };
                        index += 1;
                        return Some((key.to_lowercase(), value));
                    }
                    c => {
                        if is_ows(c) {
                            key_whitespace.get_or_insert(index);
                        } else {
                            key_whitespace = None;
                        }
                        index += 1;
                    }
                }
            }
        }
        None
    })
}

/// The extension of the last segment of `path`, dot included, as Node's `path.extname` gives it:
/// trailing slashes are ignored, and a leading dot (`.htaccess`) is no extension.
fn extname(path: &str) -> &str {
    let base = path.trim_end_matches('/');
    let base = &base[base.rfind('/').map_or(0, |slash| slash + 1)..];
    match base.rfind('.') {
        Some(0) | None => "",
        Some(_) if base == ".." => "",
        Some(dot) => &base[dot..],
    }
}

/// The context of [`HttpCrawler`]: the basic context plus the response.
pub struct HttpContext {
    basic: BasicContext,
    status: StatusCode,
    headers: HeaderMap,
    url: Url,
    redirects: Vec<Url>,
    content_type: ContentType,
    body: Arc<Body>,
    /// The parsed document, once something asked for it.
    document: Arc<Mutex<Option<Document>>>,
}

impl std::fmt::Debug for HttpContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpContext")
            .field("url", &self.url.as_str())
            .field("status", &self.status)
            .field("content_type", &self.content_type)
            .finish_non_exhaustive()
    }
}

impl Deref for HttpContext {
    type Target = BasicContext;
    fn deref(&self) -> &BasicContext {
        &self.basic
    }
}

impl DerefMut for HttpContext {
    fn deref_mut(&mut self) -> &mut BasicContext {
        &mut self.basic
    }
}

impl CrawlingContext for HttpContext {
    fn basic(&self) -> &BasicContext {
        &self.basic
    }
    fn basic_mut(&mut self) -> &mut BasicContext {
        &mut self.basic
    }
}

impl HttpContext {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The URL the body came from, after redirects (also stored as `request.loaded_url`).
    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn redirects(&self) -> &[Url] {
        &self.redirects
    }

    pub fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    pub fn body(&self) -> &Bytes {
        self.body.bytes()
    }

    pub fn body_handle(&self) -> &Arc<Body> {
        &self.body
    }

    /// The parsed document, shared with [`HtmlContext`](crate::HtmlContext).
    pub(crate) fn document_slot(&self) -> &Arc<Mutex<Option<Document>>> {
        &self.document
    }

    /// The elements matching `selector`, handed to `f` on the blocking thread pool
    /// (`querySelectorAll`). Fails with "Selector '…' not found." when nothing matches.
    pub async fn query_selector_all<R, F>(&self, selector: &str, f: F) -> anyhow::Result<R>
    where
        F: for<'a> FnOnce(Selection<'a>) -> R + Send + 'static,
        R: Send + 'static,
    {
        let selector = selector.to_owned();
        self.with_document(move |document| -> anyhow::Result<R> {
            let matches = document.select(&selector)?;
            if matches.is_empty() {
                anyhow::bail!("Selector '{selector}' not found.");
            }
            Ok(f(matches))
        })
        .await?
    }

    /// The first element matching `selector`, handed to `f` (`querySelectorOne`). Fails like
    /// [`query_selector_all`](Self::query_selector_all) when nothing matches.
    pub async fn query_selector_one<R, F>(&self, selector: &str, f: F) -> anyhow::Result<R>
    where
        F: for<'a> FnOnce(Element<'a>) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.query_selector_all(selector, |matches| f(matches.first().expect("not empty"))).await
    }

    /// Fails unless an element matches `selector` (`waitForSelector`). The document of an HTTP
    /// response does not change, so there is nothing to wait for.
    pub async fn wait_for_selector(&self, selector: &str) -> anyhow::Result<()> {
        self.query_selector_all(selector, |_| ()).await
    }

    /// Runs `f` with the parsed document (parsed once, on the blocking thread pool).
    pub(crate) async fn with_document<R, F>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&Document) -> R + Send + 'static,
        R: Send + 'static,
    {
        let body = self.body.clone();
        let slot = self.document.clone();
        Ok(tokio::task::spawn_blocking(move || crate::html::with_document(&body, &slot, f)).await?)
    }

    /// The body decoded with the response's encoding. Valid UTF-8 is not copied.
    pub fn text(&self) -> &str {
        self.body.text()
    }

    /// Deserializes the body straight into `T` (no intermediate JSON tree).
    pub fn json<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(serde_json::from_str(self.body.text())?)
    }
}

/// The HTTP step of the pipeline: `BasicContext` in, `HttpContext` out.
#[derive(Clone, Debug)]
pub struct HttpPipeline {
    options: Arc<HttpCrawlerOptions>,
    supported_mime_types: Arc<HashSet<String>>,
}

impl HttpPipeline {
    pub fn new(options: HttpCrawlerOptions) -> Self {
        let mut supported: HashSet<String> =
            [MIME_HTML, MIME_XHTML, MIME_XML, MIME_TEXT_XML, MIME_JSON].into_iter().map(str::to_owned).collect();
        supported.extend(options.additional_mime_types.iter().map(|m| m.to_ascii_lowercase()));
        HttpPipeline { options: Arc::new(options), supported_mime_types: Arc::new(supported) }
    }

    pub fn options(&self) -> &HttpCrawlerOptions {
        &self.options
    }

    fn is_error_status(&self, status: u16) -> bool {
        (status >= 500 && !self.options.ignore_http_error_status_codes.contains(&status))
            || self.options.additional_http_error_status_codes.contains(&status)
    }

    fn build_request(&self, ctx: &BasicContext) -> anyhow::Result<HttpRequest> {
        let request = ctx.request();
        let url = Url::parse(&request.url)
            .map_err(|err| NonRetryableError::new(format!("Invalid URL '{}': {err}", request.url)))?;
        let method = Method::from_bytes(request.method.as_bytes())
            .map_err(|_| NonRetryableError::new(format!("Invalid HTTP method '{}'", request.method)))?;
        let mut headers = HeaderMap::with_capacity(request.headers.len());
        for (name, value) in &request.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| NonRetryableError::new(format!("Invalid header name '{name}'")))?;
            let value = HeaderValue::from_str(value)
                .map_err(|_| NonRetryableError::new(format!("Invalid value of header '{name}'")))?;
            headers.append(name, value);
        }
        Ok(HttpRequest { method, url, headers, body: request.payload.clone().map(Bytes::from) })
    }

    /// The encoding the response declares, else the suggested one (`encodeResponse`). An encoding
    /// that does not exist fails the request.
    fn declared_encoding(&self, request_url: &str, content_type: &ContentType) -> anyhow::Result<&'static Encoding> {
        if let Some(forced) = self.options.force_response_encoding {
            return Ok(forced);
        }
        match content_type.charset.as_deref() {
            Some(charset) => encoding_for_label(charset).ok_or_else(|| {
                anyhow::anyhow!("Resource {request_url} served with unsupported charset/encoding: {charset}")
            }),
            None => Ok(self.options.suggest_response_encoding.unwrap_or(UTF_8)),
        }
    }

    /// The encoding of the body (`parseResponse`): an HTML or XML document without a declared
    /// charset is decoded with the charset of its `<meta>` tag (UTF-8 when that one is unknown).
    fn resolve_encoding(
        &self,
        request_url: &str,
        content_type: &ContentType,
        body: &[u8],
    ) -> anyhow::Result<&'static Encoding> {
        if content_type.is_html_or_xml()
            && content_type.charset.is_none()
            && self.options.force_response_encoding.is_none()
        {
            return Ok(match extract_charset_from_html_bytes(body) {
                Some(meta) => encoding_for_label(&meta).unwrap_or(UTF_8),
                None => self.options.suggest_response_encoding.unwrap_or(UTF_8),
            });
        }
        self.declared_encoding(request_url, content_type)
    }

    /// The error of an error status code, from its body.
    fn error_status(&self, status: u16, content_type: &ContentType, text: &str) -> anyhow::Error {
        // Errors are often sent as JSON, so its message comes first.
        if content_type.mime == MIME_JSON
            && let Ok(json) = serde_json::from_str::<serde_json::Value>(text)
        {
            let message =
                json.get("message").and_then(|m| m.as_str()).map(str::to_owned).unwrap_or_else(|| json.to_string());
            return anyhow::anyhow!("{status} - {message}");
        }
        if self.options.additional_http_error_status_codes.contains(&status) {
            return anyhow::anyhow!("{status} - Error status code was set by user.");
        }
        let preview: String = text.chars().take(100).collect();
        anyhow::anyhow!("{status} - Internal Server Error: {preview}")
    }

    fn navigation_timed_out(&self) -> anyhow::Error {
        anyhow::anyhow!("Navigation timed out after {} seconds.", self.options.navigation_timeout.as_secs_f64())
    }

    /// What is left of the navigation window (`remainingNavigationWindowMillis`).
    fn remaining(&self, deadline: tokio::time::Instant) -> anyhow::Result<Duration> {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(self.navigation_timed_out());
        }
        Ok(remaining)
    }

    /// Runs a step of the navigation within what is left of its window (`windowGuard`).
    async fn within_window<T>(
        &self,
        deadline: tokio::time::Instant,
        step: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        self.remaining(deadline)?;
        tokio::time::timeout_at(deadline, step).await.map_err(|_| self.navigation_timed_out())?
    }

    fn map_client_error(&self, err: HttpClientError) -> anyhow::Error {
        match err {
            HttpClientError::Proxy(message) => SessionError::new(message).into(),
            err @ HttpClientError::BodyTooLarge { .. } => NonRetryableError::new(err.to_string()).into(),
            err @ HttpClientError::Timeout(_) => anyhow::anyhow!(
                "Navigation timed out after {} seconds. ({err})",
                self.options.navigation_timeout.as_secs_f64()
            ),
            err => err.into(),
        }
    }

    /// `retryOnBlocked`: why the page looks blocked, if it does.
    async fn blocked_reason(&self, ctx: &HttpContext) -> anyhow::Result<Option<String>> {
        if ctx.content_type.is_html_or_xml() {
            let found = ctx
                .with_document(|document| {
                    RETRY_CSS_SELECTORS
                        .iter()
                        .filter(|selector| document.select(selector).is_ok_and(|matches| !matches.is_empty()))
                        .map(|selector| selector.to_string())
                        .collect::<Vec<_>>()
                })
                .await?;
            if !found.is_empty() {
                return Ok(Some(format!("Found selectors: {}", found.join(", "))));
            }
        }
        let status = ctx.status.as_u16();
        Ok(self.options.blocked_status_codes.contains(&status).then(|| format!("Blocked by status code {status}")))
    }

    fn check_content_type(&self, request_url: &str, status: u16, content_type: &ContentType) -> anyhow::Result<()> {
        let transient = status >= 500 || self.options.blocked_status_codes.contains(&status);
        let supported =
            self.supported_mime_types.contains(&content_type.mime) || self.supported_mime_types.contains("*/*");
        if !supported && !transient {
            let mut allowed: Vec<&str> = self.supported_mime_types.iter().map(String::as_str).collect();
            allowed.sort_unstable();
            return Err(NonRetryableError::new(format!(
                "Resource {request_url} served Content-Type {}, but only {} are allowed. Skipping resource.",
                content_type.mime,
                allowed.join(", ")
            ))
            .into());
        }
        Ok(())
    }
}

impl Default for HttpPipeline {
    fn default() -> Self {
        HttpPipeline::new(HttpCrawlerOptions::default())
    }
}

#[async_trait]
impl Middleware<BasicContext> for HttpPipeline {
    type Out = HttpContext;

    async fn run(&self, mut ctx: BasicContext) -> anyhow::Result<HttpContext> {
        // One navigation window covers the pre-navigation hooks, the request, the post-navigation
        // hooks and reading the body.
        let deadline = tokio::time::Instant::now() + self.options.navigation_timeout;
        for hook in &self.options.pre_navigation_hooks {
            ctx = self.within_window(deadline, hook(ctx)).await?;
        }
        let http_request = self.build_request(&ctx)?;

        let cookie_jar = ctx.session().map(|session| {
            if self.options.save_response_cookies {
                session.cookie_jar().clone()
            } else {
                Arc::new((**session.cookie_jar()).clone())
            }
        });
        let send_options = SendOptions {
            proxy_url: ctx.proxy_info().map(|proxy| proxy.url.clone()),
            cookie_jar,
            timeout: Some(self.remaining(deadline)?),
            // A man-in-the-middle proxy can only turn verification off.
            ignore_tls_errors: self.options.ignore_tls_errors
                || ctx.proxy_info().is_some_and(|proxy| proxy.ignore_tls_errors),
            max_body_size: self.options.max_body_size,
            fingerprint: ctx.session().and_then(|session| session.fingerprint_handle().cloned()),
        };

        let response = ctx
            .http_client()
            .send_request_streaming(http_request, &send_options)
            .await
            .map_err(|err| self.map_client_error(err))?;
        let request_url = ctx.request().url.clone();

        // `abortDownloadOfBody`, then the user's hooks: all before the body is read.
        let content_type = ContentType::from_response(&response.headers, &response.url);
        self.check_content_type(&request_url, response.status.as_u16(), &content_type)?;
        let mut navigated = PostNavigationContext { context: ctx, response };
        for hook in &self.options.post_navigation_hooks {
            navigated = self.within_window(deadline, hook(navigated)).await?;
        }
        // A hook may have replaced the response.
        let PostNavigationContext { context: mut ctx, response } = navigated;
        let status = response.status.as_u16();
        let content_type = ContentType::from_response(&response.headers, &response.url);

        // Before the status checks, which count 429 as blocked: when the request manager paces the
        // domain, the request is deferred without spending a retry or blaming the session. The
        // body is not read.
        if status == 429 {
            let retry_after = response.headers.get(http::header::RETRY_AFTER).and_then(|v| v.to_str().ok());
            let wait = crawlee_utils::http::parse_retry_after(retry_after, chrono::Utc::now());
            if ctx.record_domain_rate_limit(&request_url, wait) {
                return Err(RequestThrottledError::new(format!("{request_url} responded with 429.")).into());
            }
        }

        if (400..=599).contains(&status) {
            ctx.statistics().record_status_code(status);
        }

        let StreamingResponse { status: status_code, headers, url, redirects, body } = response;
        if self.is_error_status(status) {
            let encoding = self.declared_encoding(&request_url, &content_type)?;
            let body = body.bytes().await.map_err(|err| self.map_client_error(err))?;
            return Err(self.error_status(status, &content_type, &encoding.decode(&body).0));
        }
        let body = body.bytes().await.map_err(|err| self.map_client_error(err))?;
        let encoding = self.resolve_encoding(&request_url, &content_type, &body)?;

        if !self.options.retry_on_blocked && self.options.blocked_status_codes.contains(&status) {
            return Err(SessionError::new(format!("Request blocked - received {status} status code.")).into());
        }

        // A redirect may leave the scope the request was enqueued under.
        if let Some(strategy) = ctx.request().crawlee.enqueue_strategy
            && !redirects.is_empty()
            && let Ok(original) = Url::parse(&request_url)
            && !matches_enqueue_strategy(strategy, &url, &original)
        {
            return Err(RequestSkipped {
                reason: format!("redirected to {url} outside of the '{strategy}' enqueue strategy"),
            }
            .into());
        }

        ctx.request_mut().loaded_url = Some(url.to_string());
        let http_ctx = HttpContext {
            basic: ctx,
            status: status_code,
            headers,
            url,
            redirects,
            content_type,
            body: Arc::new(Body::new(body, encoding)),
            document: Arc::default(),
        };
        if self.options.retry_on_blocked
            && let Some(reason) = self.blocked_reason(&http_ctx).await?
        {
            return Err(SessionError::new(reason).into());
        }
        Ok(http_ctx)
    }
}

/// Entry point of the HTTP crawler.
///
/// ```no_run
/// use crawlee_http::{HttpContext, HttpCrawler};
///
/// #[derive(serde::Deserialize)]
/// struct Repo {
///     stargazers_count: u64,
/// }
///
/// # async fn run() -> anyhow::Result<()> {
/// let crawler = HttpCrawler::builder()
///     .request_handler(|ctx: HttpContext| async move {
///         let repo: Repo = ctx.json()?;
///         ctx.push_data(&serde_json::json!({ "stars": repo.stargazers_count }))?;
///         Ok(())
///     })
///     .build()?;
/// crawler.run(["https://api.github.com/repos/apify/crawlee"]).await?;
/// # Ok(()) }
/// ```
pub struct HttpCrawler;

impl HttpCrawler {
    pub fn builder() -> BasicCrawlerBuilder<HttpPipeline> {
        Self::builder_with_options(HttpCrawlerOptions::default())
    }

    pub fn builder_with_options(options: HttpCrawlerOptions) -> BasicCrawlerBuilder<HttpPipeline> {
        BasicCrawlerBuilder::with_pipeline(HttpPipeline::new(options))
            .concurrency_options(ConcurrencyOptions::http_optimized())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content_type(header: Option<&str>, url: &str) -> (String, Option<String>) {
        let mut headers = HeaderMap::new();
        if let Some(header) = header {
            headers.insert(http::header::CONTENT_TYPE, header.parse().unwrap());
        }
        let parsed = ContentType::from_response(&headers, &Url::parse(url).unwrap());
        (parsed.mime, parsed.charset)
    }

    fn pair(mime: &str, charset: Option<&str>) -> (String, Option<String>) {
        (mime.to_owned(), charset.map(str::to_owned))
    }

    /// Parsed like the `content-type` package (v3): the type lower-cased, unparsable parameters
    /// skipped, the first charset kept.
    #[test]
    fn the_header_is_parsed_leniently() {
        let url = "https://a.test/page.json";
        assert_eq!(content_type(Some("TEXT/HTML ; Charset=ISO-8859-2"), url), pair("text/html", Some("ISO-8859-2")));
        assert_eq!(content_type(Some("text/html; foo"), url), pair("text/html", None));
        assert_eq!(content_type(Some(r#"text/html;;charset="x""#), url), pair("text/html", Some("x")));
        assert_eq!(content_type(Some("text/html; charset=a; charset=b"), url), pair("text/html", Some("a")));
    }

    /// Without a valid `Content-Type`, the type comes from the extension of the URL's path, as
    /// `mime.contentType(extname(pathname))` gives it (`parseContentTypeFromResponse`).
    #[test]
    fn without_a_valid_header_the_extension_decides() {
        assert_eq!(content_type(None, "https://a.test/a/page.HTML?x=1"), pair("text/html", Some("utf-8")));
        assert_eq!(content_type(Some("garbage"), "https://a.test/data.json"), pair("application/json", Some("utf-8")));
        assert_eq!(content_type(None, "https://a.test/feed.xml"), pair("application/xml", None));
        assert_eq!(content_type(None, "https://a.test/archive.tar.gz"), pair("application/gzip", None));
        assert_eq!(content_type(None, "https://a.test/dir.html/"), pair("text/html", Some("utf-8")), "as path.extname");
        let fallback = pair("application/octet-stream", Some("utf-8"));
        for url in ["https://a.test/", "https://a.test/.html", "https://a.test/x.unknownext", "https://a.test/a."] {
            assert_eq!(content_type(None, url), fallback, "{url}");
        }
    }

    #[test]
    fn the_extension_table_is_sorted() {
        let table = crate::mime_types::EXTENSIONS;
        assert!(table.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert!(table.len() > 1000);
    }
}
