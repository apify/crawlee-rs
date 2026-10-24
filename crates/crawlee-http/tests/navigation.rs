//! The navigation of `HttpCrawler`: what is decided from the headers before the body is read,
//! and how the body is decoded and checked afterwards.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use bytes::Bytes;
use futures::StreamExt as _;
use serde_json::{Value, json};

use crawlee_basic::{BasicContext, ProxyInfo, ProxySource};
use crawlee_core::Services;
use crawlee_http::{HttpContext, HttpCrawler, HttpCrawlerOptions};
use crawlee_http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, SendOptions};

/// A body that sends a first chunk and then never ends.
fn endless(content_type: &'static str) -> impl IntoResponse {
    let chunks = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"<html>"))])
        .chain(futures::stream::pending());
    (
        [(header::CONTENT_TYPE, content_type), (header::HeaderName::from_static("x-reject"), "yes")],
        Body::from_stream(chunks),
    )
}

/// A response without a `Content-Type` header.
fn untyped(body: &'static str) -> axum::response::Response {
    let mut response = Body::from(body).into_response();
    response.headers_mut().remove(header::CONTENT_TYPE);
    response
}

async fn serve(hits: Arc<AtomicUsize>) -> SocketAddr {
    let counted = move |response: axum::response::Response| {
        hits.fetch_add(1, Ordering::SeqCst);
        response
    };
    let count = Arc::new(counted);
    let route = |response: fn() -> axum::response::Response| {
        let count = count.clone();
        get(move || {
            let count = count.clone();
            async move { count(response()) }
        })
    };
    let app = Router::new()
        .route("/stream.bin", route(|| endless("application/octet-stream").into_response()))
        .route("/endless.html", route(|| endless("text/html").into_response()))
        .route(
            "/klingon",
            route(|| ([(header::CONTENT_TYPE, "text/html; charset=klingon")], "<p>hi</p>").into_response()),
        )
        .route("/json-404", route(|| (StatusCode::NOT_FOUND, axum::Json(json!({ "message": "nope" }))).into_response()))
        .route(
            "/incapsula",
            route(|| {
                Html(r#"<html><body><iframe src="/_Incapsula_Resource?x=1"></iframe></body></html>"#).into_response()
            }),
        )
        .route("/forbidden", route(|| (StatusCode::FORBIDDEN, Html("<p>no</p>")).into_response()))
        .route("/ok", route(|| Html("<title>ok</title>").into_response()))
        .route("/untyped/{name}", route(|| untyped("<title>untyped</title>")));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

/// Runs an `HttpCrawler` over `paths` and returns the error messages of the failed requests, by
/// URL path, with the number of requests the server got.
async fn crawl(options: HttpCrawlerOptions, retries: u32, paths: &[&str]) -> (Vec<(String, Vec<String>)>, usize) {
    let hits = Arc::new(AtomicUsize::new(0));
    let addr = serve(hits.clone()).await;
    let crawler = HttpCrawler::builder_with_options(options)
        .services(Services::in_memory())
        .max_request_retries(retries)
        .request_handler(|_ctx: HttpContext| async move { Ok(()) })
        .failed_request_handler(|ctx: BasicContext, _err: Arc<anyhow::Error>| async move {
            ctx.push_data(&json!({ "url": ctx.request().url, "errors": ctx.request().error_messages }))?;
            Ok(())
        })
        .build()
        .unwrap();
    crawler.run(paths.iter().map(|path| format!("http://{addr}{path}"))).await.unwrap();
    let mut failed: Vec<(String, Vec<String>)> = crawler
        .dataset()
        .await
        .unwrap()
        .get_all()
        .await
        .unwrap()
        .into_iter()
        .map(|item: Value| {
            let url = url::Url::parse(item["url"].as_str().unwrap()).unwrap();
            (url.path().to_owned(), serde_json::from_value(item["errors"].clone()).unwrap())
        })
        .collect();
    failed.sort();
    (failed, hits.load(Ordering::SeqCst))
}

fn options() -> HttpCrawlerOptions {
    HttpCrawlerOptions { navigation_timeout: Duration::from_secs(5), ..Default::default() }
}

#[test]
fn defaults_are_those_of_crawlee_for_js() {
    let options = HttpCrawlerOptions::default();
    assert_eq!(options.navigation_timeout, Duration::from_secs(30));
    assert!(options.ignore_tls_errors);
    assert_eq!(options.max_body_size, None);
    assert!(!options.retry_on_blocked);
}

/// `abortDownloadOfBody`: an unsupported type fails from the headers, without waiting for the
/// body, and is not retried.
#[tokio::test]
async fn unsupported_content_types_fail_before_the_body_is_read() {
    let started = Instant::now();
    let (failed, hits) = crawl(options(), 2, &["/stream.bin"]).await;
    assert!(started.elapsed() < Duration::from_secs(3), "waited for the body: {:?}", started.elapsed());
    assert_eq!(hits, 1);
    assert_eq!(failed.len(), 1);
    assert!(failed[0].1[0].contains("served Content-Type application/octet-stream"), "{failed:?}");
}

/// Post-navigation hooks see the response before its body is read, after the content type check.
#[tokio::test]
async fn post_navigation_hooks_run_before_the_body_is_read() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let options = options().post_navigation_hook(move |_ctx, response| {
        counter.fetch_add(1, Ordering::SeqCst);
        if response.header("x-reject").is_some() {
            anyhow::bail!("rejected by the hook ({})", response.status);
        }
        Ok(())
    });
    let started = Instant::now();
    let (failed, _) = crawl(options, 0, &["/endless.html", "/stream.bin"]).await;
    assert!(started.elapsed() < Duration::from_secs(3), "waited for the body: {:?}", started.elapsed());
    assert_eq!(calls.load(Ordering::SeqCst), 1, "not called for the unsupported type");
    assert_eq!(failed[0].0, "/endless.html");
    assert_eq!(failed[0].1, ["rejected by the hook (200 OK)"]);
}

#[tokio::test]
async fn an_unsupported_charset_fails_the_request() {
    let (failed, _) = crawl(options(), 0, &["/klingon"]).await;
    let message = &failed[0].1[0];
    assert!(message.starts_with("Resource http://"), "{message}");
    assert!(message.ends_with("/klingon served with unsupported charset/encoding: klingon"), "{message}");
}

/// A JSON error body gives the message, also for a status the user made an error.
#[tokio::test]
async fn json_error_messages_come_before_user_error_codes() {
    let options = HttpCrawlerOptions { additional_http_error_status_codes: vec![404], ..options() };
    let (failed, _) = crawl(options, 0, &["/json-404"]).await;
    assert_eq!(failed[0].1, ["404 - nope"]);
}

/// `retryOnBlocked`: a page with a known challenge, or a blocked status code, retires the session
/// and is retried.
#[tokio::test]
async fn retry_on_blocked_checks_the_content_and_the_status() {
    let options = HttpCrawlerOptions { retry_on_blocked: true, ..options() };
    let (failed, hits) = crawl(options, 1, &["/incapsula", "/forbidden", "/ok"]).await;
    assert_eq!(hits, 5, "both blocked pages retried once");
    assert_eq!(
        failed,
        [
            ("/forbidden".to_owned(), vec!["Blocked by status code 403".to_owned(); 2]),
            ("/incapsula".to_owned(), vec![r#"Found selectors: iframe[src*="_Incapsula_Resource"]"#.to_owned(); 2]),
        ]
    );
}

/// Records what the crawler asks the client for.
#[derive(Default)]
struct Recording(parking_lot::Mutex<Vec<SendOptions>>);

#[async_trait::async_trait]
impl HttpClient for Recording {
    async fn send_request(&self, request: HttpRequest, options: &SendOptions) -> Result<HttpResponse, HttpClientError> {
        self.0.lock().push(options.clone());
        let mut headers = http::HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "text/html".parse().unwrap());
        Ok(HttpResponse {
            status: StatusCode::OK,
            headers,
            url: request.url,
            body: Bytes::from_static(b"<title>t</title>"),
            redirects: Vec::new(),
        })
    }
}

struct MitmProxy;

impl ProxySource for MitmProxy {
    fn new_proxy_info(&self, _session_id: &str) -> Option<ProxyInfo> {
        let url = url::Url::parse("http://mitm.example:8000").unwrap();
        Some(ProxyInfo { ignore_tls_errors: true, ..ProxyInfo::from_url(url) })
    }
}

/// The session's fingerprint goes to the client, and a MITM proxy turns TLS verification off.
#[tokio::test]
async fn the_session_fingerprint_and_its_proxy_tls_mode_reach_the_client() {
    let client = Arc::new(Recording::default());
    let crawler = HttpCrawler::builder_with_options(HttpCrawlerOptions { ignore_tls_errors: false, ..options() })
        .services(Services::in_memory())
        .http_client(client.clone())
        .proxy_configuration(Arc::new(MitmProxy))
        .request_handler(|_ctx: HttpContext| async move { Ok(()) })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.example/"]).await.unwrap();
    assert_eq!(stats.requests_succeeded, 1);
    let sent = client.0.lock();
    assert!(sent[0].ignore_tls_errors);
    assert!(sent[0].fingerprint.is_some());
}

/// `querySelectorAll` / `querySelectorOne` / `waitForSelector` of the HTTP crawler's context.
#[tokio::test]
async fn query_selectors_fail_when_nothing_matches() {
    let addr = serve(Arc::new(AtomicUsize::new(0))).await;
    let crawler = HttpCrawler::builder()
        .services(Services::in_memory())
        .max_request_retries(0)
        .request_handler(|ctx: HttpContext| async move {
            let count = ctx.query_selector_all("title", |matches| matches.len()).await?;
            let title = ctx.query_selector_one("title", |element| element.text()).await?;
            ctx.wait_for_selector("title").await?;
            let missing = ctx.wait_for_selector("h1").await.unwrap_err().to_string();
            let missing_one = ctx.query_selector_one("h1", |element| element.text()).await.unwrap_err().to_string();
            ctx.push_data(&json!({ "count": count, "title": title, "missing": missing, "missing_one": missing_one }))?;
            Ok(())
        })
        .build()
        .unwrap();
    let stats = crawler.run([format!("http://{addr}/ok")]).await.unwrap();
    assert_eq!(stats.requests_succeeded, 1);
    let items: Vec<Value> = crawler.dataset().await.unwrap().get_all().await.unwrap();
    assert_eq!(
        items[0],
        json!({
            "count": 1,
            "title": "ok",
            "missing": "Selector 'h1' not found.",
            "missing_one": "Selector 'h1' not found.",
        })
    );
}

/// A page served without a `Content-Type` is typed by the extension of its URL.
#[tokio::test]
async fn without_a_content_type_the_url_extension_decides() {
    let (failed, hits) = crawl(options(), 0, &["/untyped/page.html", "/untyped/blob.bin"]).await;
    assert_eq!(hits, 2);
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].0, "/untyped/blob.bin");
    assert!(failed[0].1[0].contains("served Content-Type application/octet-stream"), "{failed:?}");
}
