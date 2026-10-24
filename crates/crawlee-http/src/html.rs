//! [`HtmlCrawler`]: an HTTP crawler whose handler can query the page's DOM.
//!
//! Parsing is lazy and happens at most once per page, on tokio's blocking pool, so HTML parsing
//! and the handler's own extraction run in parallel on every core without stalling the I/O
//! threads. Link extraction for `enqueue_links` does not need the DOM at all: it streams the body
//! through `lol_html`.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use parking_lot::Mutex;
use url::Url;

use crawlee_basic::enqueue::EnqueueError;
use crawlee_basic::{
    BasicContext, BasicCrawlerBuilder, ConcurrencyOptions, CrawlingContext, EnqueueLinksOptions, EnqueueLinksResult,
    Middleware, Then,
};
use crawlee_utils::links::{DEFAULT_LINK_SELECTOR, LinkExtractionError, extract_links, is_streaming_selector};

use crate::body::Body;
use crate::http::{HttpContext, HttpCrawlerOptions, HttpPipeline};

// ---------------------------------------------------------------------------------------------
// Document facade
// ---------------------------------------------------------------------------------------------

/// An invalid CSS selector.
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid CSS selector '{selector}': {message}")]
pub struct SelectorError {
    pub selector: String,
    pub message: String,
}

const SELECTOR_CACHE_CAPACITY: usize = 1024;

/// Compiled selectors, shared by every page: handlers use the same few selectors on every page,
/// so each one is compiled once per process instead of once per call.
fn compiled_selector(css: &str) -> Result<Arc<scraper::Selector>, SelectorError> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<scraper::Selector>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    if let Some(selector) = cache.lock().get(css) {
        return Ok(selector.clone());
    }
    let selector = Arc::new(
        scraper::Selector::parse(css)
            .map_err(|err| SelectorError { selector: css.to_owned(), message: err.to_string() })?,
    );
    let mut cache = cache.lock();
    if cache.len() >= SELECTOR_CACHE_CAPACITY {
        cache.clear();
    }
    cache.insert(css.to_owned(), selector.clone());
    Ok(selector)
}

/// A parsed HTML document.
///
/// A thin, cheerio-inspired facade over `scraper` (html5ever). It parses like a browser does,
/// which can differ from `htmlparser2` (used by Crawlee for JS's `CheerioCrawler`) on malformed
/// markup. [`Document::inner`] gives access to the full `scraper` API.
pub struct Document {
    html: scraper::Html,
}

impl Document {
    pub fn parse(html: &str) -> Self {
        Document { html: scraper::Html::parse_document(html) }
    }

    pub fn parse_fragment(html: &str) -> Self {
        Document { html: scraper::Html::parse_fragment(html) }
    }

    /// Every element matching `css`, in document order.
    pub fn select(&self, css: &str) -> Result<Selection<'_>, SelectorError> {
        let selector = compiled_selector(css)?;
        Ok(Selection { elements: self.html.select(&selector).collect() })
    }

    /// The first element matching `css`.
    pub fn select_first(&self, css: &str) -> Result<Option<Element<'_>>, SelectorError> {
        let selector = compiled_selector(css)?;
        Ok(self.html.select(&selector).next().map(Element))
    }

    /// Text of `<title>`, trimmed.
    pub fn title(&self) -> Option<String> {
        self.select_first("title").ok().flatten().map(|title| title.text().trim().to_owned())
    }

    pub fn root(&self) -> Element<'_> {
        Element(self.html.root_element())
    }

    pub fn inner(&self) -> &scraper::Html {
        &self.html
    }
}

/// Elements matched by a selector.
#[derive(Clone, Debug)]
pub struct Selection<'a> {
    elements: Vec<scraper::ElementRef<'a>>,
}

impl<'a> Selection<'a> {
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn first(&self) -> Option<Element<'a>> {
        self.elements.first().copied().map(Element)
    }

    pub fn iter(&self) -> impl Iterator<Item = Element<'a>> + '_ {
        self.elements.iter().copied().map(Element)
    }

    /// Concatenated text of all elements, like cheerio's `.text()`.
    pub fn text(&self) -> String {
        self.elements.iter().flat_map(|el| el.text()).collect()
    }

    /// The attribute of the first element, like cheerio's `.attr()`.
    pub fn attr(&self, name: &str) -> Option<&'a str> {
        self.elements.first().and_then(|el| el.value().attr(name))
    }

    /// Descendants of the selected elements matching `css`.
    pub fn find(&self, css: &str) -> Result<Selection<'a>, SelectorError> {
        let selector = compiled_selector(css)?;
        let mut elements = Vec::new();
        for element in &self.elements {
            elements.extend(element.select(&selector));
        }
        Ok(Selection { elements })
    }

    /// `f` of every element, like cheerio's `.map().get()`.
    pub fn map<T>(&self, f: impl FnMut(Element<'a>) -> T) -> Vec<T> {
        self.iter().map(f).collect()
    }
}

impl<'a> IntoIterator for Selection<'a> {
    type Item = Element<'a>;
    type IntoIter =
        std::iter::Map<std::vec::IntoIter<scraper::ElementRef<'a>>, fn(scraper::ElementRef<'a>) -> Element<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter().map(Element as fn(_) -> _)
    }
}

/// One element.
#[derive(Clone, Copy, Debug)]
pub struct Element<'a>(scraper::ElementRef<'a>);

impl<'a> Element<'a> {
    pub fn name(&self) -> &'a str {
        self.0.value().name()
    }

    pub fn attr(&self, name: &str) -> Option<&'a str> {
        self.0.value().attr(name)
    }

    pub fn text(&self) -> String {
        self.0.text().collect()
    }

    pub fn html(&self) -> String {
        self.0.html()
    }

    pub fn inner_html(&self) -> String {
        self.0.inner_html()
    }

    pub fn select(&self, css: &str) -> Result<Selection<'a>, SelectorError> {
        let selector = compiled_selector(css)?;
        Ok(Selection { elements: self.0.select(&selector).collect() })
    }

    pub fn inner(&self) -> scraper::ElementRef<'a> {
        self.0
    }
}

// ---------------------------------------------------------------------------------------------
// Context and pipeline
// ---------------------------------------------------------------------------------------------

/// The context of [`HtmlCrawler`]: the HTTP context plus a lazily parsed [`Document`].
///
/// The document is parsed on first use, on the blocking pool. `scraper` is built with atomic
/// tendrils, so the document is `Send` and can move between the pool and this context.
pub struct HtmlContext {
    http: HttpContext,
}

impl std::fmt::Debug for HtmlContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HtmlContext").field("http", &self.http).finish_non_exhaustive()
    }
}

impl Deref for HtmlContext {
    type Target = HttpContext;
    fn deref(&self) -> &HttpContext {
        &self.http
    }
}

impl DerefMut for HtmlContext {
    fn deref_mut(&mut self) -> &mut HttpContext {
        &mut self.http
    }
}

impl CrawlingContext for HtmlContext {
    fn basic(&self) -> &BasicContext {
        self.http.basic()
    }
    fn basic_mut(&mut self) -> &mut BasicContext {
        self.http.basic_mut()
    }
}

pub(crate) fn with_document<R>(body: &Body, slot: &Mutex<Option<Document>>, f: impl FnOnce(&Document) -> R) -> R {
    let mut slot = slot.lock();
    let document = slot.get_or_insert_with(|| Document::parse(body.text()));
    f(document)
}

impl HtmlContext {
    /// Runs `f` with the parsed document on the blocking thread pool and returns its result.
    ///
    /// ```no_run
    /// # async fn handler(ctx: crawlee_http::HtmlContext) -> anyhow::Result<()> {
    /// let (title, prices) = ctx
    ///     .with_html(|doc| {
    ///         let prices = doc.select(".price")?.map(|el| el.text());
    ///         Ok::<_, anyhow::Error>((doc.title(), prices))
    ///     })
    ///     .await??;
    /// # Ok(()) }
    /// ```
    pub async fn with_html<R, F>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&Document) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.http.with_document(f).await
    }

    /// Like [`with_html`](Self::with_html), but on the current thread. Fine for small pages;
    /// large documents are better parsed with `with_html`.
    pub fn with_html_inline<R>(&self, f: impl FnOnce(&Document) -> R) -> R {
        with_document(self.http.body_handle(), self.http.document_slot(), f)
    }

    /// Absolute URLs of the `href`s of elements matching `selector` (default `a`), honoring
    /// `<base href>`.
    pub async fn extract_links(&self, selector: Option<&str>) -> anyhow::Result<Vec<Url>> {
        let selector = selector.unwrap_or(DEFAULT_LINK_SELECTOR).to_owned();
        let base = self.http.url().clone();

        if is_streaming_selector(&selector) {
            // Streaming extraction: no DOM, linear time.
            return match extract_links(self.http.text().as_bytes(), &selector, &base) {
                Ok(links) => Ok(links),
                Err(LinkExtractionError::UnsupportedSelector { .. }) => {
                    self.extract_links_from_dom(selector, base).await
                }
                Err(err) => Err(err.into()),
            };
        }
        self.extract_links_from_dom(selector, base).await
    }

    async fn extract_links_from_dom(&self, selector: String, base: Url) -> anyhow::Result<Vec<Url>> {
        self.with_html(move |doc| -> anyhow::Result<Vec<Url>> {
            let base = doc
                .select_first("base[href]")?
                .and_then(|el| el.attr("href"))
                .and_then(|href| base.join(href.trim()).ok())
                .unwrap_or(base);
            Ok(doc
                .select(&selector)?
                .iter()
                .filter_map(|el| el.attr("href"))
                .filter(|href| !href.is_empty())
                .filter_map(|href| base.join(href).ok())
                .collect())
        })
        .await?
    }

    /// Finds links on the page and enqueues those that pass `options` (by default: same hostname).
    pub async fn enqueue_links(&self, options: EnqueueLinksOptions) -> Result<EnqueueLinksResult, EnqueueError> {
        let links = self.extract_links(options.selector.as_deref()).await?;
        self.enqueue_urls(links, &options).await
    }
}

/// The HTML step: wraps the HTTP context. Nothing is parsed until the handler asks for it.
#[derive(Clone, Copy, Debug, Default)]
pub struct HtmlLayer;

#[async_trait]
impl Middleware<HttpContext> for HtmlLayer {
    type Out = HtmlContext;
    async fn run(&self, http: HttpContext) -> anyhow::Result<HtmlContext> {
        Ok(HtmlContext { http })
    }
}

pub type HtmlPipeline = Then<HttpPipeline, HtmlLayer>;

/// Entry point of the HTML crawler, the counterpart of `CheerioCrawler` in Crawlee for JS.
///
/// ```no_run
/// use crawlee_http::{EnqueueLinksOptions, HtmlContext, HtmlCrawler};
///
/// # async fn run() -> anyhow::Result<()> {
/// let crawler = HtmlCrawler::builder()
///     .max_requests_per_crawl(50)
///     .request_handler(|ctx: HtmlContext| async move {
///         let title = ctx.with_html(|doc| doc.title()).await?;
///         ctx.push_data(&serde_json::json!({ "url": ctx.url().as_str(), "title": title }))?;
///         ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
///         Ok(())
///     })
///     .build()?;
/// crawler.run(["https://crawlee.dev"]).await?;
/// # Ok(()) }
/// ```
pub struct HtmlCrawler;

impl HtmlCrawler {
    pub fn builder() -> BasicCrawlerBuilder<HtmlPipeline> {
        Self::builder_with_options(HttpCrawlerOptions::default())
    }

    pub fn builder_with_options(options: HttpCrawlerOptions) -> BasicCrawlerBuilder<HtmlPipeline> {
        BasicCrawlerBuilder::with_pipeline(Then(HttpPipeline::new(options), HtmlLayer))
            .concurrency_options(ConcurrencyOptions::http_optimized())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_facade() {
        let doc = Document::parse(
            r#"<html><head><title> Shop </title></head><body>
                <ul><li class="p"><a href="/1">One</a><span class="price">10</span></li>
                    <li class="p"><a href="/2">Two</a><span class="price">20</span></li></ul>
            </body></html>"#,
        );
        assert_eq!(doc.title().as_deref(), Some("Shop"));
        let products = doc.select("li.p").unwrap();
        assert_eq!(products.len(), 2);
        assert_eq!(products.find(".price").unwrap().map(|el| el.text()), ["10", "20"]);
        assert_eq!(products.first().unwrap().select("a").unwrap().attr("href"), Some("/1"));
        assert_eq!(products.text(), "One10Two20");
        assert!(doc.select("li[").is_err());
    }
}
