//! Crawlee for Rust: fast HTTP crawling with HTML parsing and typed JSON extraction.
//!
//! This crate re-exports the `crawlee-*` crates. The main entry points are
//! [`HtmlCrawler`] (the counterpart of `CheerioCrawler`), [`HttpCrawler`] and [`BasicCrawler`].
//!
//! ```no_run
//! use crawlee::{EnqueueLinksOptions, HtmlContext, HtmlCrawler};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let crawler = HtmlCrawler::builder()
//!         .max_requests_per_crawl(20)
//!         .request_handler(|ctx: HtmlContext| async move {
//!             let title = ctx.with_html(|doc| doc.title()).await?;
//!             ctx.push_data(&serde_json::json!({ "url": ctx.url().as_str(), "title": title }))?;
//!             ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
//!             Ok(())
//!         })
//!         .build()?;
//!
//!     let stats = crawler.run(["https://crawlee.dev"]).await?;
//!     println!("{stats:#?}");
//!     Ok(())
//! }
//! ```

/// The version of crawlee-rs.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use crawlee_basic as basic;
pub use crawlee_core as core;
pub use crawlee_http as http;
pub use crawlee_http_client as http_client;
pub use crawlee_utils as utils;

pub use crawlee_basic::{
    BasicContext, BasicCrawler, CrawlerOptions, CrawlingContext, EnqueueLinksOptions, EnqueueLinksResult,
    FinalStatistics, ProxyConfiguration, Router, SessionPoolOptions,
};
pub use crawlee_core::errors::{
    CriticalError, NonRetryableError, RequestThrottledError, RetryRequestError, SessionError,
};
pub use crawlee_core::{
    BufferStage, Configuration, Dataset, DatasetBufferOptions, EnqueueStrategy, KeyValueStore, Request, RequestManager,
    RequestQueue, Services, StorageIdentifier,
};
pub use crawlee_http::{Document, HtmlContext, HtmlCrawler, HttpContext, HttpCrawler, HttpCrawlerOptions};

/// The URL type used throughout the API (re-exported from the `url` crate).
pub use url::Url;
