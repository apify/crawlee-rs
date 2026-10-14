//! [`HttpCrawler`]: fetches each request over HTTP and hands the response to the handler.

use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::str::FromStr as _;
use std::time::Duration;

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
use crawlee_http_client::{HttpClientError, HttpRequest, HttpResponse, SendOptions};
use crawlee_utils::{extract_charset_from_html_bytes, matches_enqueue_strategy};

use crate::body::{Body, encoding_for_label};

/// Called before the request is sent; may change it (headers, method, body).
pub type PreNavigationHook = Rc<dyn Fn(&BasicContext, &mut HttpRequest) -> anyhow::Result<()>>;
/// Called after the response arrived, before the handler; may reject it by returning an error.
pub type PostNavigationHook = Rc<dyn Fn(&HttpContext) -> anyhow::Result<()>>;

pub const MIME_HTML: &str = "text/html";
pub const MIME_XHTML: &str = "application/xhtml+xml";
pub const MIME_XML: &str = "application/xml";
pub const MIME_TEXT_XML: &str = "text/xml";
pub const MIME_JSON: &str = "application/json";

/// Settings of the HTTP pipeline, with the defaults of Crawlee for JS.
#[derive(Clone)]
pub struct HttpCrawlerOptions {
    /// Timeout of the navigation: the request, redirects and reading the body.
    pub navigation_timeout: Duration,
    /// MIME types accepted besides HTML, XHTML, XML and JSON. `*/*` accepts everything.
    pub additional_mime_types: Vec<String>,
    /// Status codes that fail the request although they are below 500.
    pub additional_http_error_status_codes: Vec<u16>,
    /// Status codes of 500 and above that do not fail the request.
    pub ignore_http_error_status_codes: Vec<u16>,
    /// Status codes that mean the session got blocked; its session is retired.
    pub blocked_status_codes: Vec<u16>,
    /// Decode every body with this encoding, whatever the response says.
    pub force_response_encoding: Option<&'static Encoding>,
    /// Encoding used when the response does not declare one.
    pub suggest_response_encoding: Option<&'static Encoding>,
    /// Store `Set-Cookie` of responses in the session.
    pub save_response_cookies: bool,
    pub ignore_tls_errors: bool,
    /// Larger bodies fail the request (without retries).
    pub max_body_size: Option<usize>,
    pub pre_navigation_hooks: Vec<PreNavigationHook>,
    pub post_navigation_hooks: Vec<PostNavigationHook>,
}

impl Default for HttpCrawlerOptions {
    fn default() -> Self {
        HttpCrawlerOptions {
            navigation_timeout: Duration::from_secs(60),
            additional_mime_types: Vec::new(),
            additional_http_error_status_codes: Vec::new(),
            ignore_http_error_status_codes: Vec::new(),
            blocked_status_codes: vec![401, 403, 429],
            force_response_encoding: None,
            suggest_response_encoding: None,
            save_response_cookies: true,
            ignore_tls_errors: false,
            max_body_size: Some(50 * 1024 * 1024),
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
    pub fn pre_navigation_hook(
        mut self,
        hook: impl Fn(&BasicContext, &mut HttpRequest) -> anyhow::Result<()> + 'static,
    ) -> Self {
        self.pre_navigation_hooks.push(Rc::new(hook));
        self
    }

    pub fn post_navigation_hook(mut self, hook: impl Fn(&HttpContext) -> anyhow::Result<()> + 'static) -> Self {
        self.post_navigation_hooks.push(Rc::new(hook));
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
    fn parse(headers: &HeaderMap) -> ContentType {
        headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| mime::Mime::from_str(value).ok())
            .map(|mime| ContentType {
                mime: mime.essence_str().to_ascii_lowercase(),
                charset: mime.get_param(mime::CHARSET).map(|charset| charset.as_str().to_owned()),
            })
            .unwrap_or_else(|| ContentType { mime: "application/octet-stream".to_owned(), charset: None })
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

/// The context of [`HttpCrawler`]: the basic context plus the response.
pub struct HttpContext {
    basic: BasicContext,
    status: StatusCode,
    headers: HeaderMap,
    url: Url,
    redirects: Vec<Url>,
    content_type: ContentType,
    body: Rc<Body>,
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

    pub fn body_handle(&self) -> &Rc<Body> {
        &self.body
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
    options: Rc<HttpCrawlerOptions>,
    supported_mime_types: Rc<HashSet<String>>,
}

impl HttpPipeline {
    pub fn new(options: HttpCrawlerOptions) -> Self {
        let mut supported: HashSet<String> =
            [MIME_HTML, MIME_XHTML, MIME_XML, MIME_TEXT_XML, MIME_JSON].into_iter().map(str::to_owned).collect();
        supported.extend(options.additional_mime_types.iter().map(|m| m.to_ascii_lowercase()));
        HttpPipeline { options: Rc::new(options), supported_mime_types: Rc::new(supported) }
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

    fn resolve_encoding(&self, content_type: &ContentType, body: &[u8]) -> &'static Encoding {
        if let Some(forced) = self.options.force_response_encoding {
            return forced;
        }
        if let Some(encoding) = content_type.charset.as_deref().and_then(encoding_for_label) {
            return encoding;
        }
        if content_type.is_html_or_xml()
            && let Some(encoding) = extract_charset_from_html_bytes(body).as_deref().and_then(encoding_for_label)
        {
            return encoding;
        }
        self.options.suggest_response_encoding.unwrap_or(UTF_8)
    }

    fn check_status(&self, response: &HttpResponse, content_type: &ContentType) -> anyhow::Result<()> {
        let status = response.status.as_u16();

        if self.is_error_status(status) {
            if self.options.additional_http_error_status_codes.contains(&status) {
                anyhow::bail!("{status} - Error status code was set by user.");
            }
            let text = String::from_utf8_lossy(&response.body);
            if content_type.is_json()
                && let Ok(json) = serde_json::from_str::<serde_json::Value>(&text)
            {
                let message =
                    json.get("message").and_then(|m| m.as_str()).map(str::to_owned).unwrap_or_else(|| json.to_string());
                anyhow::bail!("{status} - {message}");
            }
            let preview: String = text.chars().take(100).collect();
            anyhow::bail!("{status} - Internal Server Error: {preview}");
        }

        if self.options.blocked_status_codes.contains(&status) {
            return Err(SessionError::new(format!("Request blocked - received {status} status code.")).into());
        }
        Ok(())
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

#[async_trait(?Send)]
impl Middleware<BasicContext> for HttpPipeline {
    type Out = HttpContext;

    async fn run(&self, mut ctx: BasicContext) -> anyhow::Result<HttpContext> {
        let mut http_request = self.build_request(&ctx)?;
        for hook in &self.options.pre_navigation_hooks {
            hook(&ctx, &mut http_request)?;
        }

        let cookie_jar = ctx.session().map(|session| {
            if self.options.save_response_cookies {
                session.cookie_jar().clone()
            } else {
                Rc::new((**session.cookie_jar()).clone())
            }
        });
        let send_options = SendOptions {
            proxy_url: ctx.proxy_info().map(|proxy| proxy.url.clone()),
            cookie_jar,
            timeout: Some(self.options.navigation_timeout),
            ignore_tls_errors: self.options.ignore_tls_errors,
            max_body_size: self.options.max_body_size,
        };

        let response = match ctx.http_client().send_request(http_request, &send_options).await {
            Ok(response) => response,
            Err(HttpClientError::Proxy(message)) => return Err(SessionError::new(message).into()),
            Err(err @ HttpClientError::BodyTooLarge { .. }) => {
                return Err(NonRetryableError::new(err.to_string()).into());
            }
            Err(err @ HttpClientError::Timeout(_)) => {
                anyhow::bail!(
                    "Navigation timed out after {} seconds. ({err})",
                    self.options.navigation_timeout.as_secs_f64()
                )
            }
            Err(err) => return Err(err.into()),
        };

        let status = response.status.as_u16();
        if (400..=599).contains(&status) {
            ctx.statistics().record_status_code(status);
        }

        // Before the status checks, which count 429 as blocked: when the request manager paces the
        // domain, the request is deferred without spending a retry or blaming the session.
        if status == 429 {
            let retry_after = response.headers.get(http::header::RETRY_AFTER).and_then(|v| v.to_str().ok());
            let wait = crawlee_utils::http::parse_retry_after(retry_after, chrono::Utc::now());
            let url = ctx.request().url.clone();
            if ctx.record_domain_rate_limit(&url, wait) {
                return Err(RequestThrottledError::new(format!("{url} responded with 429.")).into());
            }
        }

        let content_type = ContentType::parse(&response.headers);
        self.check_content_type(&ctx.request().url, status, &content_type)?;
        self.check_status(&response, &content_type)?;

        // A redirect may leave the scope the request was enqueued under.
        if let Some(strategy) = ctx.request().crawlee.enqueue_strategy
            && !response.redirects.is_empty()
            && let Ok(original) = Url::parse(&ctx.request().url)
            && !matches_enqueue_strategy(strategy, &response.url, &original)
        {
            return Err(RequestSkipped {
                reason: format!("redirected to {} outside of the '{strategy}' enqueue strategy", response.url),
            }
            .into());
        }

        ctx.request_mut().loaded_url = Some(response.url.to_string());
        let encoding = self.resolve_encoding(&content_type, &response.body);

        let http_ctx = HttpContext {
            basic: ctx,
            status: response.status,
            headers: response.headers,
            url: response.url,
            redirects: response.redirects,
            content_type,
            body: Rc::new(Body::new(response.body, encoding)),
        };
        for hook in &self.options.post_navigation_hooks {
            hook(&http_ctx)?;
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
