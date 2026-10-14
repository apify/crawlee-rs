//! An empty `BasicCrawler` project.
//!
//! `BasicCrawler` does not fetch anything by itself; the handler decides what to do with each
//! request. This one fetches the page with `send_request` (which uses the request's session:
//! its cookies and proxy) and records the status code. Switch to `HtmlCrawler` or `HttpCrawler`
//! to have pages fetched and parsed for you.

use crawlee::http_client::HttpRequest;
use crawlee::{BasicContext, BasicCrawler, Url};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "local")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let start_url = std::env::var("START_URL").unwrap_or_else(|_| "https://crawlee.dev".to_owned());

    let crawler = BasicCrawler::builder()
        .request_handler(|ctx: BasicContext| async move {
            let url: Url = ctx.request().url.parse()?;
            let response = ctx.send_request(HttpRequest::get(url)).await?;
            tracing::info!("{} -> {} ({} bytes)", ctx.request().url, response.status, response.body.len());

            ctx.push_data(&serde_json::json!({
                "url": ctx.request().url,
                "status": response.status.as_u16(),
                "bytes": response.body.len(),
            }))?;
            Ok(())
        })
        .build()?;

    crawler.run([start_url]).await?;
    crawler.export_data("storage/results.json").await?;
    Ok(())
}
