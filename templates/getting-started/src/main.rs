//! Getting started with Crawlee for Rust.
//!
//! Crawls https://crawlee.dev: logs the title of every page, follows the links that stay on the
//! same hostname, and saves the results to `storage/results.json`.
//!
//! Run it with `cargo run` (set `RUST_LOG=debug` for more detail, `START_URL` to crawl another site).

use crawlee::{EnqueueLinksOptions, HtmlContext, HtmlCrawler};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "local")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let start_url = std::env::var("START_URL").unwrap_or_else(|_| "https://crawlee.dev".to_owned());

    let crawler = HtmlCrawler::builder()
        // Comment this out to crawl the whole website.
        .max_requests_per_crawl(20)
        // Uncomment to route requests through your proxies:
        // .proxy_configuration(std::sync::Arc::new(crawlee::ProxyConfiguration::new(["http://user:pass@proxy:8000"])?))
        .request_handler(|ctx: HtmlContext| async move {
            // The page is parsed on first use, on a background thread.
            let title = ctx.with_html(|doc| doc.title()).await?.unwrap_or_default();
            tracing::info!("{title} ({})", ctx.url());

            ctx.push_data(&serde_json::json!({ "url": ctx.url().as_str(), "title": title }))?;

            // Follow the links on the page that stay on the same hostname.
            ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
            Ok(())
        })
        .build()?;

    let stats = crawler.run([start_url]).await?;
    let saved = crawler.export_data("storage/results.json").await?;
    tracing::info!(
        "Crawled {} pages ({} failed) in {} ms, saved {saved} items to storage/results.json",
        stats.requests_succeeded,
        stats.requests_failed,
        stats.crawler_runtime_millis
    );
    Ok(())
}
