//! Benchmark crawl against `bench_server`: parses every page, extracts the products and enqueues
//! the links. The same work as `conformance/bench/cheerio-crawl.mts` does with Crawlee for JS.
//!
//! ```sh
//! cargo run --release --example bench_crawl -- http://127.0.0.1:3000 10000 50
//! ```
//!
//! Prints one JSON line with throughput, CPU time and peak memory of this process.

use std::time::Instant;

use serde::Serialize;

use crawlee::{EnqueueLinksOptions, HtmlContext, HtmlCrawler, Services};

#[derive(Serialize)]
struct Product {
    id: String,
    name: String,
    price: f64,
    url: Option<String>,
}

/// User + system CPU seconds and peak RSS (MiB) of this process, from `/proc` (Linux only).
fn process_usage() -> (f64, f64) {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // Fields after the parenthesized command name; utime and stime are the 12th and 13th.
    let fields: Vec<&str> = stat.rsplit_once(") ").map(|(_, rest)| rest.split(' ').collect()).unwrap_or_default();
    let ticks = |i: usize| fields.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    const CLOCK_TICKS_PER_SEC: f64 = 100.0;
    let cpu = (ticks(11) + ticks(12)) / CLOCK_TICKS_PER_SEC;

    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let peak_kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<f64>().ok())
        .unwrap_or(0.0);
    (cpu, peak_kib / 1024.0)
}

#[tokio::main(flavor = "local")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let base = args.next().unwrap_or_else(|| "http://127.0.0.1:3000".to_owned());
    let pages: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(10_000);
    let concurrency: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(50);

    let crawler = HtmlCrawler::builder()
        .services(Services::in_memory())
        // Pinned, like the JS benchmark (min = max), to compare equal concurrency.
        .fixed_concurrency(concurrency)
        .max_requests_per_crawl(pages)
        .request_handler(|ctx: HtmlContext| async move {
            let (title, products) = ctx
                .with_html(|doc| -> anyhow::Result<_> {
                    let products = doc
                        .select("li.product")?
                        .iter()
                        .map(|el| -> anyhow::Result<Product> {
                            let price_text = el.select(".price")?.text();
                            let price = price_text.split_whitespace().next().unwrap_or("0").parse().unwrap_or(0.0);
                            Ok(Product {
                                id: el.attr("data-id").unwrap_or_default().to_owned(),
                                name: el.select(".name")?.text().trim().to_owned(),
                                price,
                                url: el.select("a.product-link")?.attr("href").map(str::to_owned),
                            })
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    Ok((doc.title(), products))
                })
                .await??;
            ctx.push_data(&serde_json::json!({ "url": ctx.url().as_str(), "title": title, "products": products }))?;
            ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
            Ok(())
        })
        .build()?;

    let started = Instant::now();
    let stats = crawler.run([format!("{base}/p/0")]).await?;
    let seconds = started.elapsed().as_secs_f64();
    let (cpu_seconds, peak_rss_mib) = process_usage();
    let items = crawler.dataset().await?.get_info().await?.item_count;

    println!(
        "{}",
        serde_json::json!({
            "implementation": "crawlee-rs",
            "pages": stats.requests_succeeded,
            "failed": stats.requests_failed,
            "items": items,
            "concurrency": concurrency,
            "seconds": (seconds * 100.0).round() / 100.0,
            "pages_per_second": (stats.requests_succeeded as f64 / seconds).round(),
            "cpu_seconds": cpu_seconds,
            "cpu_ms_per_page": (cpu_seconds * 1000.0 / stats.requests_succeeded.max(1) as f64 * 100.0).round() / 100.0,
            "peak_rss_mib": peak_rss_mib.round(),
        })
    );
    Ok(())
}
