//! `HtmlCrawler` with a router, the counterpart of Crawlee's `cheerio-ts` template.
//!
//! The start page enqueues every page of the site as a `detail` page; detail pages are scraped
//! (title, meta description, headings). Results are saved to `storage/results.json`.
//!
//! Run it with `cargo run` (set `START_URL` to crawl another site).

mod routes;

use crawlee::HtmlCrawler;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "local")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let start_url = std::env::var("START_URL").unwrap_or_else(|_| "https://crawlee.dev".to_owned());
    // Only pages of the start URL's site are enqueued, e.g. `https://crawlee.dev/**`.
    let site = format!("{}/**", crawlee::Url::parse(&start_url)?.origin().ascii_serialization());

    let crawler = HtmlCrawler::builder()
        // Uncomment to route requests through your proxies:
        // .proxy_configuration(std::sync::Arc::new(crawlee::ProxyConfiguration::new(["http://user:pass@proxy:8000"])?))
        .router(routes::router(site))
        // Comment this out to scrape the full website.
        .max_requests_per_crawl(20)
        .build()?;

    crawler.run([start_url]).await?;
    let saved = crawler.export_data("storage/results.json").await?;
    tracing::info!("Saved {saved} items to storage/results.json");
    Ok(())
}
