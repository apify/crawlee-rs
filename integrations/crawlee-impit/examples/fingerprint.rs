//! Shows the TLS and HTTP/2 fingerprints a server sees, with impit and with the default reqwest
//! transport, using https://tls.peet.ws.
//!
//! ```sh
//! cargo run --example fingerprint
//! ```

use std::sync::Arc;

use crawlee::http_client::{HttpClient, RedirectingClient, ReqwestTransport};
use crawlee::{HttpContext, HttpCrawler, Services};
use crawlee_impit::{Browser, ImpitTransport};

async fn fingerprint(name: &str, client: Arc<dyn HttpClient>) -> anyhow::Result<()> {
    let crawler = HttpCrawler::builder()
        .services(Services::in_memory())
        .http_client(client)
        .request_handler(|ctx: HttpContext| async move {
            let info: serde_json::Value = ctx.json()?;
            ctx.push_data(&serde_json::json!({
                "ja4": info["tls"]["ja4"],
                "akamai_h2": info["http2"]["akamai_fingerprint_hash"],
                "http_version": info["http_version"],
                "user_agent": info["user_agent"],
            }))?;
            Ok(())
        })
        .build()?;
    crawler.run(["https://tls.peet.ws/api/all"]).await?;
    let items: Vec<serde_json::Value> = crawler.dataset().await?.get_all().await?;
    println!("{name:>8}: {}", serde_json::to_string_pretty(&items.first())?);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fingerprint("reqwest", Arc::new(RedirectingClient::new(ReqwestTransport::new()))).await?;
    for browser in [Browser::Chrome, Browser::Firefox] {
        let client = Arc::new(RedirectingClient::new(ImpitTransport::with_browser(browser)));
        fingerprint(&format!("{browser:?}"), client).await?;
    }
    Ok(())
}
