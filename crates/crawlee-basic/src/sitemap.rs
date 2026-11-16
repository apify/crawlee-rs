//! Fetching sitemaps: [`Sitemap::load`] follows nested sitemaps and returns the page URLs, like
//! `Sitemap.load` / `parseSitemap` in Crawlee for JS. The [`SitemapRequestLoader`] crawls them.
//!
//! [`SitemapRequestLoader`]: crate::sitemap_loader::SitemapRequestLoader

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use http::header::{ACCEPT, CONTENT_TYPE, HeaderValue};
use url::Url;

use crawlee_http_client::{HttpClient, HttpRequest, SendOptions};
use crawlee_utils::sitemap::{SitemapError, SitemapItem, SitemapUrl, decode_sitemap};
use crawlee_utils::{EnqueueStrategy, filter_url};

/// How sitemaps are fetched and filtered, with the defaults of `ParseSitemapOptions` in JS.
#[derive(Clone, Debug)]
pub struct SitemapOptions {
    /// Retries after a failed fetch or a sitemap of an unsupported type. A malformed sitemap is
    /// not fetched again: the pages before the error are kept.
    pub retries: u32,
    pub timeout: Duration,
    /// Log failed fetches (malformed sitemaps are always logged).
    pub report_network_errors: bool,
    /// Pages and nested sitemaps outside this strategy, relative to the sitemap listing them,
    /// are dropped.
    pub enqueue_strategy: EnqueueStrategy,
    /// How deep nested sitemaps are followed; `None` for no limit.
    pub max_depth: Option<u32>,
    pub proxy_url: Option<Url>,
}

impl Default for SitemapOptions {
    fn default() -> Self {
        SitemapOptions {
            retries: 3,
            timeout: Duration::from_secs(30),
            report_network_errors: true,
            enqueue_strategy: EnqueueStrategy::SameHostname,
            max_depth: None,
            proxy_url: None,
        }
    }
}

/// A page from a sitemap.
#[derive(Clone, Debug, PartialEq)]
pub struct SitemapEntry {
    pub url: SitemapUrl,
    /// The sitemap that listed the page.
    pub origin_sitemap_url: String,
}

/// One fetched sitemap, filtered by the enqueue strategy.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SitemapContents {
    pub pages: Vec<SitemapUrl>,
    pub nested_sitemaps: Vec<String>,
}

/// Fetches and parses one sitemap, with retries. `None` when every attempt failed.
pub async fn fetch_sitemap(url: &str, client: &dyn HttpClient, options: &SitemapOptions) -> Option<SitemapContents> {
    let Ok(parsed) = Url::parse(url) else {
        tracing::warn!("Invalid sitemap URL: {url}");
        return None;
    };
    let send_options =
        SendOptions { proxy_url: options.proxy_url.clone(), timeout: Some(options.timeout), ..SendOptions::default() };

    for attempt in 0..=options.retries {
        let retries_left = options.retries - attempt;
        let note = if retries_left == 0 { "no retries left." } else { "retrying..." };
        let mut request = HttpRequest::get(parsed.clone());
        request.headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        let response = match client.send_request(request, &send_options).await {
            Ok(response) if response.status.is_success() => response,
            other => {
                if options.report_network_errors {
                    let status =
                        other.as_ref().map(|r| r.status.as_u16().to_string()).unwrap_or_else(|e| e.to_string());
                    tracing::warn!(
                        "Malformed sitemap content: {url}, {note} (Failed to fetch sitemap: {url}, status code: {status})"
                    );
                }
                continue;
            }
        };

        let content_type = response.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(str::to_owned);
        let mut sitemap_url = parsed.clone();
        let items = match decode_sitemap(&response.body, content_type.as_deref(), &mut sitemap_url) {
            Ok(items) => items,
            // As in JS, which reads a sitemap as a stream: the entries before the error are kept,
            // and the sitemap is not fetched again.
            Err(SitemapError::Xml { message, items_read }) => {
                tracing::warn!("Malformed sitemap content: {url} (malformed sitemap XML: {message})");
                items_read
            }
            Err(err) => {
                tracing::warn!("Malformed sitemap content: {url}, {note} ({err})");
                continue;
            }
        };

        let mut contents = SitemapContents::default();
        let mut dropped = 0;
        for item in items {
            match item {
                SitemapItem::Sitemap(nested) => {
                    match filter_url(&nested, sitemap_url.as_str(), options.enqueue_strategy) {
                        Ok(_) => contents.nested_sitemaps.push(nested),
                        Err(reason) => tracing::warn!("Skipping nested sitemap {nested} (parent {url}): {reason}."),
                    }
                }
                SitemapItem::Url(page) => match filter_url(&page.loc, sitemap_url.as_str(), options.enqueue_strategy) {
                    Ok(_) => contents.pages.push(page),
                    Err(reason) => {
                        dropped += 1;
                        tracing::debug!("Skipping sitemap URL {} (parent {url}): {reason}.", page.loc);
                    }
                },
            }
        }
        if dropped > 0 {
            tracing::warn!(
                "Skipped {dropped} URL(s) from sitemap {url} not matching enqueue strategy '{}' (or using a non-http(s) \
                 scheme). Enable debug logs to see each skipped URL.",
                options.enqueue_strategy
            );
        }
        return Some(contents);
    }
    None
}

/// Page URLs from sitemaps.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Sitemap {
    pub urls: Vec<String>,
}

impl Sitemap {
    /// Every page of the sitemaps at `urls`, following nested sitemaps (each fetched once).
    pub async fn load_entries(urls: &[String], client: &dyn HttpClient, options: &SitemapOptions) -> Vec<SitemapEntry> {
        let mut sources: VecDeque<(String, u32)> = urls.iter().map(|url| (url.clone(), 0)).collect();
        let mut visited: HashSet<String> = HashSet::new();
        let mut entries = Vec::new();
        while let Some((url, depth)) = sources.pop_front() {
            if options.max_depth.is_some_and(|max| depth > max) {
                continue;
            }
            visited.insert(Url::parse(&url).map_or_else(|_| url.clone(), |u| u.to_string()));
            let Some(contents) = fetch_sitemap(&url, client, options).await else { continue };
            for nested in contents.nested_sitemaps {
                if !visited.contains(&nested) {
                    sources.push_back((nested, depth + 1));
                }
            }
            entries.extend(
                contents.pages.into_iter().map(|page| SitemapEntry { url: page, origin_sitemap_url: url.clone() }),
            );
        }
        entries
    }

    pub async fn load(urls: &[String], client: &dyn HttpClient, options: &SitemapOptions) -> Sitemap {
        let entries = Self::load_entries(urls, client, options).await;
        Sitemap { urls: entries.into_iter().map(|entry| entry.url.loc).collect() }
    }

    /// Tries `/sitemap.xml` and `/sitemap.txt` of the site `url` belongs to, without logging
    /// failed fetches.
    pub async fn try_common_names(url: &str, client: &dyn HttpClient, options: &SitemapOptions) -> Sitemap {
        let Ok(mut base) = Url::parse(url) else { return Sitemap::default() };
        base.set_query(None);
        base.set_fragment(None);
        let candidates: Vec<String> = ["/sitemap.xml", "/sitemap.txt"]
            .into_iter()
            .map(|path| {
                base.set_path(path);
                base.to_string()
            })
            .collect();
        let options = SitemapOptions { report_network_errors: false, ..options.clone() };
        Self::load(&candidates, client, &options).await
    }
}
