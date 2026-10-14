//! End-to-end crawls against a local fixture site.
//!
//! The site is reachable as both `127.0.0.1` and `localhost`, which gives two hostnames on one
//! server for testing enqueue strategies and redirects that leave the scope.

use crawlee_core::local::CellExt as _;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;

use axum::Router as AxumRouter;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use serde::Deserialize;
use serde_json::{Value, json};

use crawlee_basic::SessionPoolOptions;
use crawlee_core::Services;
use crawlee_http::{EnqueueLinksOptions, HtmlContext, HtmlCrawler, HttpContext, HttpCrawler};

#[derive(Clone, Default)]
struct Hits(std::sync::Arc<std::sync::Mutex<BTreeMap<String, usize>>>);

impl Hits {
    fn hit(&self, path: &str) {
        *self.0.lock().unwrap().entry(path.to_owned()).or_default() += 1;
    }
    fn get(&self, path: &str) -> usize {
        self.0.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}

async fn serve() -> (SocketAddr, Hits) {
    let hits = Hits::default();

    async fn page(State(hits): State<Hits>, Path(name): Path<String>, headers: HeaderMap) -> Response {
        hits.hit(&format!("/page/{name}"));
        let port = headers
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.rsplit(':').next())
            .unwrap_or("80")
            .to_owned();
        let body = match name.as_str() {
            "start" => format!(
                r#"<html><head><title>Start</title></head><body>
                <a href="/page/a">A</a>
                <a href="b?x=1&amp;y=2">B (relative, with an entity)</a>
                <a href="/page/a#fragment">A again</a>
                <a href="http://localhost:{port}/page/other-host">other host</a>
                <a href="mailto:someone@example.com">mail</a>
                <a href="/page/error">error</a>
                </body></html>"#
            ),
            "a" => r#"<html><head><title>A</title></head><body><a href="/page/c">C</a><a href="/page/start">back</a></body></html>"#.to_owned(),
            other => format!("<html><head><title>{other}</title></head><body></body></html>"),
        };
        Html(body).into_response()
    }

    let app = AxumRouter::new()
        .route(
            "/page/error",
            get(|State(hits): State<Hits>| async move {
                hits.hit("/page/error");
                (StatusCode::INTERNAL_SERVER_ERROR, "database is down").into_response()
            }),
        )
        .route("/page/{name}", get(page))
        .route(
            "/blocked",
            get(|State(hits): State<Hits>| async move {
                hits.hit("/blocked");
                (StatusCode::FORBIDDEN, Html("<p>go away</p>")).into_response()
            }),
        )
        .route("/missing", get(|| async { (StatusCode::NOT_FOUND, Html("<title>Not found</title>")) }))
        .route(
            "/image.png",
            get(|State(hits): State<Hits>| async move {
                hits.hit("/image.png");
                ([(header::CONTENT_TYPE, "image/png")], vec![0u8, 1, 2]).into_response()
            }),
        )
        .route(
            "/czech",
            get(|| async {
                let (bytes, _, _) = encoding_rs::WINDOWS_1250
                    .encode("<html><head><meta charset=\"windows-1250\"><title>Žluťoučký kůň</title></head></html>");
                ([(header::CONTENT_TYPE, "text/html")], bytes.into_owned())
            }),
        )
        .route(
            "/api/items",
            get(|| async {
                axum::Json(json!({ "items": [{ "id": 1, "name": "one" }, { "id": 2, "name": "two" }], "total": 2 }))
            }),
        )
        .route(
            "/api/error",
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({ "message": "try later" }))) }),
        )
        .route("/login", get(|| async { ([(header::SET_COOKIE, "token=secret; Path=/")], Redirect::to("/private")) }))
        .route(
            "/private",
            get(|headers: HeaderMap| async move {
                let has_token = headers
                    .get(header::COOKIE)
                    .and_then(|c| c.to_str().ok())
                    .is_some_and(|c| c.contains("token=secret"));
                if has_token {
                    Html("<title>Private</title>").into_response()
                } else {
                    StatusCode::UNAUTHORIZED.into_response()
                }
            }),
        )
        .route(
            "/leave",
            get(|headers: HeaderMap| async move {
                let port = headers
                    .get(header::HOST)
                    .and_then(|h| h.to_str().ok())
                    .and_then(|h| h.rsplit(':').next())
                    .unwrap_or("80")
                    .to_owned();
                Redirect::temporary(&format!("http://localhost:{port}/page/elsewhere"))
            }),
        )
        .with_state(hits.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::task::spawn_local(async move { axum::serve(listener, app).await.unwrap() });
    (addr, hits)
}

async fn items(dataset: crawlee_core::Dataset) -> Vec<Value> {
    dataset.get_all().await.unwrap()
}

#[tokio::test(flavor = "local")]
async fn html_crawl_follows_same_hostname_links() {
    let (addr, hits) = serve().await;
    let crawler = HtmlCrawler::builder()
        .services(Services::in_memory())
        .max_request_retries(1)
        .request_handler(|ctx: HtmlContext| async move {
            let title = ctx.with_html(|doc| doc.title()).await?;
            ctx.push_data(&json!({ "path": ctx.url().path(), "query": ctx.url().query(), "title": title, "depth": ctx.request().crawl_depth() }))?;
            ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
            Ok(())
        })
        .build()
        .unwrap();

    let stats = crawler.run([format!("http://{addr}/page/start")]).await.unwrap();

    // start, a, b, c succeed; error fails after 1 retry; other-host and mailto are not enqueued.
    assert_eq!(stats.requests_succeeded, 4, "{stats:#?}");
    assert_eq!(stats.requests_failed, 1);
    assert_eq!(hits.get("/page/error"), 2);
    assert_eq!(hits.get("/page/other-host"), 0);
    assert_eq!(hits.get("/page/a"), 1, "the #fragment duplicate is deduplicated");
    assert_eq!(stats.status_codes.get(&500), Some(&2));

    let mut items = items(crawler.dataset().await.unwrap()).await;
    items.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    let summary: Vec<(String, u64)> =
        items.iter().map(|i| (i["title"].as_str().unwrap().to_owned(), i["depth"].as_u64().unwrap())).collect();
    assert_eq!(summary, [("A".to_owned(), 1), ("b".to_owned(), 1), ("c".to_owned(), 2), ("Start".to_owned(), 0)]);
    let b = items.iter().find(|i| i["path"] == "/page/b").unwrap();
    assert_eq!(b["query"], "x=1&y=2", "attribute entities are decoded");
}

#[tokio::test(flavor = "local")]
async fn enqueue_options_filter_and_label() {
    let (addr, hits) = serve().await;
    let seen = Rc::new(std::cell::RefCell::new(Vec::new()));
    let log = seen.clone();

    let mut router = crawlee_basic::Router::<HtmlContext>::new();
    router.add_default_handler(|ctx: HtmlContext| async move {
        let result = ctx
            .enqueue_links(
                EnqueueLinksOptions::new()
                    .strategy(crawlee_core::EnqueueStrategy::All)
                    .exclude(["**/page/error"])
                    .label("DETAIL"),
            )
            .await?;
        assert!(result.skipped.iter().any(|(url, _)| url.scheme() == "mailto"));
        Ok(())
    });
    router.add_handler("DETAIL", move |ctx: HtmlContext| {
        let log = log.clone();
        async move {
            log.borrow_mut().push(ctx.url().to_string());
            Ok(())
        }
    });

    let crawler = HtmlCrawler::builder().services(Services::in_memory()).router(router).build().unwrap();
    crawler.run([format!("http://{addr}/page/start")]).await.unwrap();

    let mut seen = seen.borrow_mut().clone();
    seen.sort();
    let port = addr.port();
    assert_eq!(
        seen,
        [
            format!("http://127.0.0.1:{port}/page/a"),
            format!("http://127.0.0.1:{port}/page/b?x=1&y=2"),
            format!("http://localhost:{port}/page/other-host"),
        ]
    );
    assert_eq!(hits.get("/page/error"), 0);
}

#[tokio::test(flavor = "local")]
async fn blocked_unsupported_and_error_statuses() {
    let (addr, hits) = serve().await;
    let crawler = HttpCrawler::builder()
        .services(Services::in_memory())
        .max_request_retries(2)
        .request_handler(|ctx: HttpContext| async move {
            ctx.push_data(&json!({ "path": ctx.url().path(), "status": ctx.status().as_u16() }))?;
            Ok(())
        })
        .failed_request_handler(|ctx: crawlee_basic::BasicContext, _e: Rc<anyhow::Error>| async move {
            ctx.push_data(&json!({ "failed": ctx.request().url, "errors": ctx.request().error_messages }))?;
            Ok(())
        })
        .build()
        .unwrap();

    let stats = crawler
        .run([
            format!("http://{addr}/blocked"),
            format!("http://{addr}/image.png"),
            format!("http://{addr}/missing"),
            format!("http://{addr}/api/error"),
        ])
        .await
        .unwrap();

    assert_eq!(hits.get("/blocked"), 3, "403 is retried with new sessions");
    assert_eq!(hits.get("/image.png"), 1, "unsupported content type is not retried");
    assert_eq!(stats.requests_succeeded, 1, "a 404 still reaches the handler");
    assert_eq!(stats.requests_failed, 3);

    let items = items(crawler.dataset().await.unwrap()).await;
    let errors_of = |suffix: &str| -> Vec<String> {
        let item = items.iter().find(|i| i["failed"].as_str().is_some_and(|u| u.ends_with(suffix))).unwrap();
        serde_json::from_value(item["errors"].clone()).unwrap()
    };
    assert_eq!(errors_of("/blocked")[0], "Request blocked - received 403 status code.");
    assert!(errors_of("/image.png")[0].contains("served Content-Type image/png"));
    assert_eq!(errors_of("/api/error")[0], "503 - try later");
    assert!(items.iter().any(|i| i["path"] == "/missing" && i["status"] == 404));
}

#[tokio::test(flavor = "local")]
async fn typed_json_legacy_charsets_and_cookies() {
    #[derive(Deserialize)]
    struct Listing {
        items: Vec<Item>,
    }
    #[derive(Deserialize)]
    struct Item {
        id: u32,
        name: String,
    }

    let (addr, _hits) = serve().await;
    let crawler = HtmlCrawler::builder()
        .services(Services::in_memory())
        // One session, so the cookie set by /login is sent to /private.
        .session_pool_options(SessionPoolOptions { max_pool_size: 1, ..Default::default() })
        .max_concurrency(1)
        .max_request_retries(0)
        .request_handler(|ctx: HtmlContext| async move {
            if ctx.content_type().is_json() {
                let listing: Listing = ctx.json()?;
                for item in listing.items {
                    ctx.push_data(&json!({ "id": item.id, "name": item.name }))?;
                }
            } else {
                let title = ctx.with_html(|doc| doc.title()).await?;
                ctx.push_data(&json!({ "title": title, "encoding": ctx.body_handle().encoding().name() }))?;
            }
            Ok(())
        })
        .build()
        .unwrap();

    let stats = crawler
        .run([format!("http://{addr}/api/items"), format!("http://{addr}/czech"), format!("http://{addr}/login")])
        .await
        .unwrap();
    assert_eq!(stats.requests_failed, 0, "{stats:#?}");

    let items = items(crawler.dataset().await.unwrap()).await;
    assert!(items.contains(&json!({ "id": 2, "name": "two" })));
    assert!(items.contains(&json!({ "title": "Žluťoučký kůň", "encoding": "windows-1250" })));
    assert!(items.contains(&json!({ "title": "Private", "encoding": "UTF-8" })));
}

#[tokio::test(flavor = "local")]
async fn redirect_outside_the_enqueue_strategy_is_skipped() {
    let (addr, hits) = serve().await;
    let handled = Rc::new(Cell::new(0));
    let counter = handled.clone();
    let crawler = HtmlCrawler::builder()
        .services(Services::in_memory())
        .request_handler(move |_ctx: HtmlContext| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1);
                Ok(())
            }
        })
        .build()
        .unwrap();

    let request = crawlee_core::Request::builder(format!("http://{addr}/leave"))
        .enqueue_strategy(crawlee_core::EnqueueStrategy::SameHostname)
        .build()
        .unwrap();
    let stats = crawler.run([request]).await.unwrap();

    assert_eq!(handled.get(), 0);
    assert_eq!(stats.requests_skipped, 1);
    assert_eq!(hits.get("/page/elsewhere"), 1, "the redirect itself was followed");
}
