//! robots.txt, rate limiting (429 with `Retry-After`) and crawling from sitemaps, end to end
//! against a local server.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use axum::Router as AxumRouter;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use std::sync::{Arc, Mutex};

use crawlee_basic::{SitemapRequestLoader, SitemapRequestLoaderOptions};
use crawlee_core::{RequestManagerTandem, Services, StorageIdentifier};
use crawlee_http::{EnqueueLinksOptions, HtmlContext, HtmlCrawler};

#[derive(Clone, Default)]
struct Site {
    hits: Arc<Mutex<BTreeMap<String, usize>>>,
    robots: Option<&'static str>,
    addr: Arc<Mutex<Option<SocketAddr>>>,
}

impl Site {
    fn hit(&self, path: &str) -> usize {
        let mut hits = self.hits.lock().unwrap();
        let count = hits.entry(path.to_owned()).or_default();
        *count += 1;
        *count
    }

    fn hits(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    fn base(&self) -> String {
        format!("http://{}", self.addr.lock().unwrap().unwrap())
    }
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

async fn serve(robots: Option<&'static str>) -> Site {
    let site = Site { robots, ..Site::default() };

    async fn robots_txt(State(site): State<Site>) -> Response {
        site.hit("/robots.txt");
        match site.robots {
            Some(body) => body.into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn page(State(site): State<Site>, Path(name): Path<String>, uri: axum::http::Uri) -> Response {
        let count = site.hit(uri.path());
        match name.as_str() {
            "start" => Html(
                r#"<a href="/page/public">public</a><a href="/private/secret">secret</a><a href="/page/limited">limited</a>"#,
            )
            .into_response(),
            // Rate-limited on the first visit.
            "limited" if count == 1 => (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "1")], "slow down").into_response(),
            other => Html(format!("<title>{other}</title>")).into_response(),
        }
    }

    async fn sitemap(State(site): State<Site>, Path(name): Path<String>) -> Response {
        site.hit(&format!("/sitemaps/{name}"));
        let base = site.base();
        match name.as_str() {
            "index.xml" => format!(
                "<sitemapindex><sitemap><loc>{base}/sitemaps/pages.xml.gz</loc></sitemap>\
                 <sitemap><loc>{base}/sitemaps/list.txt</loc></sitemap></sitemapindex>"
            )
            .into_response(),
            "pages.xml.gz" => {
                let xml = format!(
                    "<?xml version=\"1.0\"?><urlset><url><loc>{base}/page/s1</loc></url>\
                     <url><loc>{base}/page/s2</loc></url><url><loc>https://elsewhere.test/x</loc></url></urlset>"
                );
                ([(header::CONTENT_TYPE, "application/octet-stream")], gzip(xml.as_bytes())).into_response()
            }
            "list.txt" => format!("{base}/page/s3\n{base}/page/excluded\n").into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    let app = AxumRouter::new()
        .route("/robots.txt", get(robots_txt))
        .route("/page/{name}", get(page))
        .route("/private/{name}", get(page))
        .route("/sitemaps/{name}", get(sitemap))
        .with_state(site.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    *site.addr.lock().unwrap() = Some(listener.local_addr().unwrap());
    tokio::task::spawn_local(async move { axum::serve(listener, app).await.unwrap() });
    site
}

fn titles_crawler(services: Services) -> crawlee_basic::BasicCrawlerBuilder<crawlee_http::HtmlPipeline> {
    HtmlCrawler::builder().services(services).max_request_retries(0).request_handler(|ctx: HtmlContext| async move {
        ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
        ctx.push_data(&serde_json::json!({ "url": ctx.request().url }))?;
        Ok(())
    })
}

#[tokio::test(flavor = "local")]
async fn robots_txt_is_respected() {
    let site = serve(Some("User-agent: *\nDisallow: /private/\n")).await;
    let crawler = titles_crawler(Services::in_memory()).respect_robots_txt(true).build().unwrap();
    let base = site.base();
    let stats = crawler.run([format!("{base}/page/start"), format!("{base}/private/direct")]).await.unwrap();

    assert_eq!(site.hits("/private/secret"), 0, "a disallowed link is not enqueued");
    assert_eq!(site.hits("/private/direct"), 0, "a disallowed start URL is skipped");
    assert_eq!(stats.requests_skipped, 1);
    assert_eq!(site.hits("/page/public"), 1);
    assert_eq!(site.hits("/robots.txt"), 1, "fetched once per origin");
}

#[tokio::test(flavor = "local")]
async fn missing_robots_txt_allows_everything_and_is_cached() {
    let site = serve(None).await;
    let crawler = titles_crawler(Services::in_memory()).respect_robots_txt(true).build().unwrap();
    crawler.run([format!("{}/page/start", site.base())]).await.unwrap();
    assert_eq!(site.hits("/private/secret"), 1);
    assert_eq!(site.hits("/robots.txt"), 1, "a 404 is cached as allow-all");
}

#[tokio::test(flavor = "local")]
async fn rate_limited_requests_wait_for_retry_after() {
    let site = serve(None).await;
    let crawler = titles_crawler(Services::in_memory()).same_domain_delay(Duration::from_millis(1)).build().unwrap();
    let started = Instant::now();
    let stats = crawler.run([format!("{}/page/limited", site.base())]).await.unwrap();

    assert_eq!(site.hits("/page/limited"), 2);
    assert_eq!((stats.requests_succeeded, stats.requests_failed, stats.requests_retries), (1, 0, 0));
    assert!(started.elapsed() >= Duration::from_secs(1), "Retry-After: 1 was honoured");
}

#[tokio::test(flavor = "local")]
async fn crawls_the_pages_of_sitemaps() {
    let site = serve(None).await;
    let base = site.base();
    let services = Services::in_memory();
    let mut options = SitemapRequestLoaderOptions::new([format!("{base}/sitemaps/index.xml")]);
    options.exclude = vec![crawlee_utils::UrlPattern::glob("**/excluded")];
    let loader = SitemapRequestLoader::open(options, &services, crawlee_http_client::default_client()).await.unwrap();
    let queue = Rc::new(services.open_request_queue(&StorageIdentifier::Default).await.unwrap());

    let crawler = HtmlCrawler::builder()
        .services(services)
        .request_manager(Rc::new(RequestManagerTandem::new(loader.clone(), queue)))
        .request_handler(|ctx: HtmlContext| async move {
            ctx.push_data(&serde_json::json!({ "url": ctx.request().url }))?;
            Ok(())
        })
        .build()
        .unwrap();
    let stats = crawler.run(Vec::<String>::new()).await.unwrap();

    let mut urls: Vec<String> = crawler
        .dataset()
        .await
        .unwrap()
        .get_all::<serde_json::Value>()
        .await
        .unwrap()
        .into_iter()
        .map(|item| item["url"].as_str().unwrap().trim_start_matches(&base).to_owned())
        .collect();
    urls.sort();
    assert_eq!(urls, ["/page/s1", "/page/s2", "/page/s3"], "other hosts and excluded pages are dropped");
    assert_eq!(stats.requests_succeeded, 3);
    assert!(loader.is_sitemap_fully_loaded());
    assert_eq!(site.hits("/sitemaps/pages.xml.gz"), 1);
}
