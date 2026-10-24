//! Behavior of the task loop and the retry logic, mirroring the semantics of Crawlee for JS.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::{Value, json};

use crawlee_basic::{BasicContext, BasicCrawler, Router};
use crawlee_core::errors::{CriticalError, NonRetryableError, RetryRequestError, SessionError};
use crawlee_core::{Configuration, Request, Services};

fn services() -> Services {
    Services::in_memory()
}

async fn items(crawler: &BasicCrawler) -> Vec<Value> {
    crawler.dataset().await.unwrap().get_all().await.unwrap()
}

#[tokio::test]
async fn handles_every_request_once() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(move |ctx: BasicContext| {
            let log = log.clone();
            async move {
                log.lock().push(ctx.request().url.clone());
                ctx.push_data(&json!({ "url": ctx.request().url }))?;
                Ok(())
            }
        })
        .build()
        .unwrap();

    let stats =
        crawler.run(["https://a.dev/1", "https://a.dev/2", "https://a.dev/2/", "https://a.dev/3"]).await.unwrap();

    assert_eq!(stats.requests_succeeded, 3, "the trailing-slash duplicate is deduplicated");
    assert_eq!(stats.requests_failed, 0);
    let mut seen = seen.lock().clone();
    seen.sort();
    assert_eq!(seen, ["https://a.dev/1", "https://a.dev/2", "https://a.dev/3"]);
    assert_eq!(items(&crawler).await.len(), 3);
}

#[tokio::test]
async fn run_starts_crawling_after_the_first_batch_and_adds_the_rest_in_the_background() {
    // As in Crawlee for JS, `run` adds the first 1000 requests, starts crawling, and adds the rest
    // in batches of 1000 with a pause of 1 s after each.
    let handled = Arc::new(AtomicUsize::new(0));
    let first_handled_after = Arc::new(Mutex::new(None));
    let started = std::time::Instant::now();
    let (counter, first) = (handled.clone(), first_handled_after.clone());
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(move |_ctx: BasicContext| {
            let (counter, first) = (counter.clone(), first.clone());
            async move {
                first.lock().get_or_insert(started.elapsed());
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        })
        .build()
        .unwrap();

    let urls: Vec<String> = (0..2500).map(|i| format!("https://a.dev/{i}")).collect();
    let stats = crawler.run(urls).await.unwrap();

    assert_eq!(stats.requests_succeeded, 2500, "the background batches are crawled too");
    assert_eq!(handled.load(Ordering::Relaxed), 2500);
    // The last batch lands after about 1 s; the first request is handled well before.
    assert!(first_handled_after.lock().unwrap() < Duration::from_millis(900));
}

#[tokio::test]
async fn add_requests_returns_after_the_first_batch() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .build()
        .unwrap();

    let urls: Vec<String> = (0..1500).map(|i| format!("https://a.dev/{i}")).collect();
    let result = crawler.add_requests(urls).await.unwrap();
    assert_eq!(result.added_requests.len(), 1000);
    let rest = result.wait_for_all_requests_to_be_added.expect("a background batch");
    assert_eq!(rest.await.unwrap().unwrap().len(), 500);
}

#[tokio::test]
async fn skip_request_marks_the_request_handled_without_retries_or_failures() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_request_retries(3)
        .request_handler(move |ctx: BasicContext| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                if ctx.request().url.ends_with("/skip") {
                    return Err(ctx.skip_request(Some("not interesting")));
                }
                ctx.push_data(&json!({ "url": ctx.request().url }))?;
                Ok(())
            }
        })
        .build()
        .unwrap();

    let stats = crawler.run(["https://a.dev/skip", "https://a.dev/keep"]).await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2, "the skipped request is not retried");
    assert_eq!((stats.requests_succeeded, stats.requests_failed, stats.requests_skipped), (1, 0, 1));
    assert_eq!(items(&crawler).await.len(), 1);
}

#[tokio::test]
async fn retries_then_fails_and_rolls_back_writes() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(Mutex::new(None::<Request>));
    let counter = attempts.clone();
    let failed_slot = failed.clone();

    let crawler = BasicCrawler::builder()
        .services(services())
        .max_request_retries(2)
        .request_handler(move |ctx: BasicContext| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                ctx.push_data(&json!({ "never": "committed" }))?;
                anyhow::bail!("boom {}", ctx.request().retry_count)
            }
        })
        .failed_request_handler(move |ctx: BasicContext, error: Arc<anyhow::Error>| {
            let failed_slot = failed_slot.clone();
            async move {
                ctx.push_data(&json!({ "failed": ctx.request().url, "error": error.to_string() }))?;
                *failed_slot.lock() = Some(ctx.request().clone());
                Ok(())
            }
        })
        .build()
        .unwrap();

    let stats = crawler.run(["https://a.dev/x"]).await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 3, "1 attempt + 2 retries");
    assert_eq!(stats.requests_failed, 1);
    assert_eq!(stats.requests_retries, 2);
    assert_eq!(stats.retry_histogram, vec![0, 0, 1]);

    let failed = failed.lock().clone().unwrap();
    assert_eq!(failed.retry_count, 2);
    assert_eq!(failed.error_messages, ["boom 0", "boom 1", "boom 2"]);
    assert!(failed.handled_at.is_some());

    // Only the failed-request handler's write survives.
    assert_eq!(items(&crawler).await, vec![json!({ "failed": "https://a.dev/x", "error": "boom 2" })]);
}

#[tokio::test]
async fn error_handler_changes_are_kept_for_the_retry() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(|ctx: BasicContext| async move {
            if ctx.request().user_data.get("fixed").is_none() {
                anyhow::bail!("needs fixing");
            }
            ctx.push_data(&json!({ "ok": true }))?;
            Ok(())
        })
        .error_handler(|mut ctx: BasicContext, _error: Arc<anyhow::Error>| async move {
            ctx.request_mut().user_data.insert("fixed".into(), json!(true));
            Ok(())
        })
        .build()
        .unwrap();

    let stats = crawler.run(["https://a.dev/"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_retries), (1, 1));
}

#[tokio::test]
async fn marker_errors_steer_retries() {
    let attempts: Arc<Mutex<HashMap<String, usize>>> = Arc::default();
    let log = attempts.clone();

    let crawler = BasicCrawler::builder()
        .services(services())
        .max_request_retries(1)
        .request_handler(move |ctx: BasicContext| {
            let log = log.clone();
            async move {
                let path = url::Url::parse(&ctx.request().url)?.path().to_owned();
                let attempt = {
                    let mut log = log.lock();
                    let entry = log.entry(path.clone()).or_default();
                    *entry += 1;
                    *entry
                };
                match path.as_str() {
                    "/non-retryable" => Err(NonRetryableError::new("gone").into()),
                    // Retried beyond max_request_retries until it succeeds.
                    "/retry" if attempt < 4 => Err(RetryRequestError::new("again").into()),
                    "/session" => Err(SessionError::new("blocked").into()),
                    _ => Ok(()),
                }
            }
        })
        .build()
        .unwrap();

    let stats =
        crawler.run(["https://a.dev/non-retryable", "https://a.dev/retry", "https://a.dev/session"]).await.unwrap();

    let attempts = attempts.lock();
    assert_eq!(attempts["/non-retryable"], 1);
    assert_eq!(attempts["/retry"], 4);
    assert_eq!(attempts["/session"], 2);
    assert_eq!(stats.requests_succeeded, 1);
    assert_eq!(stats.requests_failed, 2);
}

#[tokio::test]
async fn session_errors_retire_the_session() {
    let sessions = Arc::new(Mutex::new(Vec::new()));
    let log = sessions.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_concurrency(1)
        .request_handler(move |ctx: BasicContext| {
            let log = log.clone();
            async move {
                let session = ctx.session().unwrap();
                log.lock().push(session.id().to_owned());
                if ctx.request().retry_count == 0 {
                    return Err(SessionError::new("blocked").into());
                }
                assert!(!session.is_retired());
                Ok(())
            }
        })
        .build()
        .unwrap();

    crawler.run(["https://a.dev/"]).await.unwrap();
    let sessions = sessions.lock();
    assert_eq!(sessions.len(), 2);
    assert_ne!(sessions[0], sessions[1], "the retry runs with a fresh session");
}

#[tokio::test]
async fn critical_errors_and_missing_routes_stop_the_crawl() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(|_ctx: BasicContext| async move { Err(CriticalError::new("stop everything").into()) })
        .build()
        .unwrap();
    let err = crawler.run(["https://a.dev/"]).await.unwrap_err();
    assert_eq!(err.to_string(), "stop everything");

    let mut router = Router::<BasicContext>::new();
    router.add_handler("KNOWN", |_ctx: BasicContext| async move { Ok(()) });
    let crawler = BasicCrawler::builder().services(services()).router(router).build().unwrap();
    let request = Request::builder("https://a.dev/").label("UNKNOWN").build().unwrap();
    let err = crawler.run([request]).await.unwrap_err();
    assert!(err.to_string().contains("Route not found for label 'UNKNOWN'"), "{err}");
}

#[tokio::test]
async fn router_dispatches_by_label_and_enqueues_deeper() {
    #[derive(serde::Deserialize)]
    struct Detail {
        id: u32,
    }

    let mut router = Router::<BasicContext>::new();
    router.add_default_handler(|ctx: BasicContext| async move {
        let requests = (1..=3).map(|id| {
            Request::builder(format!("https://a.dev/item/{id}"))
                .label("DETAIL")
                .user_data(&json!({ "id": id }))
                .unwrap()
                .build()
                .unwrap()
        });
        ctx.add_requests(requests).await?;
        Ok(())
    });
    router.add_typed_handler("DETAIL", |ctx: BasicContext, data: Detail| async move {
        ctx.push_data(&json!({ "id": data.id, "depth": ctx.request().crawl_depth() }))?;
        Ok(())
    });

    let crawler = BasicCrawler::builder().services(services()).router(router).build().unwrap();
    let stats = crawler.run(["https://a.dev/"]).await.unwrap();
    assert_eq!(stats.requests_succeeded, 4);

    let mut items = items(&crawler).await;
    items.sort_by_key(|item| item["id"].as_u64());
    assert_eq!(
        items,
        vec![json!({ "id": 1, "depth": 1 }), json!({ "id": 2, "depth": 1 }), json!({ "id": 3, "depth": 1 })]
    );
}

#[tokio::test]
async fn max_requests_per_crawl_is_respected() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_concurrency(4)
        .max_requests_per_crawl(5)
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .build()
        .unwrap();
    let urls: Vec<String> = (0..20).map(|i| format!("https://a.dev/{i}")).collect();
    let stats = crawler.run(urls).await.unwrap();
    assert_eq!(stats.requests_succeeded, 5);
}

#[tokio::test]
async fn handler_timeouts_and_panics_are_request_errors() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_request_retries(0)
        .request_handler_timeout(Duration::from_millis(50))
        .request_handler(|ctx: BasicContext| async move {
            if ctx.request().url.ends_with("slow") {
                tokio::time::sleep(Duration::from_secs(10)).await;
            } else {
                panic!("handler bug");
            }
            Ok(())
        })
        .failed_request_handler(|ctx: BasicContext, _e: Arc<anyhow::Error>| async move {
            ctx.push_data(&json!({ "url": ctx.request().url, "error": ctx.request().error_messages[0] }))?;
            Ok(())
        })
        .build()
        .unwrap();

    let stats = crawler.run(["https://a.dev/slow", "https://a.dev/panic"]).await.unwrap();
    assert_eq!(stats.requests_failed, 2);

    let mut items = items(&crawler).await;
    items.sort_by(|a, b| a["url"].as_str().cmp(&b["url"].as_str()));
    assert_eq!(items[0]["error"], "request handler panicked: handler bug");
    assert_eq!(items[1]["error"], "requestHandler timed out after 0.05 seconds.");
}

#[tokio::test]
async fn requests_added_while_running_are_picked_up() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(|ctx: BasicContext| async move {
            let depth = ctx.request().crawl_depth();
            ctx.push_data(&json!({ "depth": depth }))?;
            // Each page links to the next level, three levels deep.
            if depth < 3 {
                ctx.add_requests([format!("https://a.dev/level/{}", depth + 1)]).await?;
            }
            Ok(())
        })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.dev/level/0"]).await.unwrap();
    assert_eq!(stats.requests_succeeded, 4);
    let mut depths: Vec<u64> = items(&crawler).await.iter().map(|i| i["depth"].as_u64().unwrap()).collect();
    depths.sort();
    assert_eq!(depths, [0, 1, 2, 3]);
}

#[tokio::test]
async fn export_data_writes_json_and_jsonl() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_concurrency(1)
        .request_handler(|ctx: BasicContext| async move {
            ctx.push_data(&json!({ "url": ctx.request().url }))?;
            Ok(())
        })
        .build()
        .unwrap();
    crawler.run(["https://a.dev/1", "https://a.dev/2"]).await.unwrap();

    let dir = std::env::temp_dir().join(format!("crawlee-export-{}", std::process::id()));
    let json_path = dir.join("nested").join("results.json");
    assert_eq!(crawler.export_data(&json_path).await.unwrap(), 2);
    let exported: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(&json_path).unwrap()).unwrap();
    assert_eq!(exported, vec![json!({ "url": "https://a.dev/1" }), json!({ "url": "https://a.dev/2" })]);

    let jsonl_path = dir.join("results.jsonl");
    crawler.export_data(&jsonl_path).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(&jsonl_path).unwrap(),
        "{\"url\":\"https://a.dev/1\"}\n{\"url\":\"https://a.dev/2\"}\n"
    );

    assert!(crawler.export_data(dir.join("results.csv")).await.is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn file_system_storage_is_purged_once_per_process() {
    let dir = tempfile::tempdir().unwrap();
    let configuration = || Configuration { storage_dir: dir.path().to_owned(), ..Configuration::default() };
    let crawl = |services: Services, url: &'static str| async move {
        let crawler = BasicCrawler::builder()
            .services(services)
            .request_handler(|ctx: BasicContext| async move {
                ctx.push_data(&json!({ "url": ctx.request().url }))?;
                Ok(())
            })
            .build()
            .unwrap();
        crawler.run([url]).await.unwrap();
    };
    let files = || {
        let mut names: Vec<String> = std::fs::read_dir(dir.path().join("datasets/default"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    };

    let services = Services::from_configuration(configuration());
    crawl(services.clone(), "https://a.dev/1").await;
    assert_eq!(files(), ["000000001.json", "__metadata__.json"]);

    // A second crawler of the same process adds to the storages of the first one.
    crawl(services, "https://a.dev/2").await;
    assert_eq!(files(), ["000000001.json", "000000002.json", "__metadata__.json"]);

    // A new process starts over: the previous run's storages are purged.
    crawl(Services::from_configuration(configuration()), "https://a.dev/1").await;
    assert_eq!(files(), ["000000001.json", "__metadata__.json"]);
    let item: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("datasets/default/000000001.json")).unwrap())
            .unwrap();
    assert_eq!(item, json!({ "url": "https://a.dev/1" }));
}

#[tokio::test]
async fn a_crawl_resumes_with_its_statistics_and_state() {
    let dir = tempfile::tempdir().unwrap();
    let crawl = |purge_on_start: bool, urls: Vec<&'static str>| {
        let configuration =
            Configuration { storage_dir: dir.path().to_owned(), purge_on_start, ..Configuration::default() };
        async move {
            let crawler = BasicCrawler::builder()
                .services(Services::from_configuration(configuration))
                .id("resume")
                .request_handler(|ctx: BasicContext| async move {
                    let state = ctx.use_state(|| json!({ "pages": 0 })).await?;
                    let mut state = state.lock();
                    state["pages"] = json!(state["pages"].as_u64().unwrap() + 1);
                    Ok(())
                })
                .build()
                .unwrap();
            crawler.run(urls).await.unwrap()
        }
    };
    let read = |key: &str| -> Value {
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("key_value_stores/default").join(key)).unwrap())
            .unwrap()
    };

    let first = crawl(true, vec!["https://a.dev/1", "https://a.dev/2"]).await;
    assert_eq!(first.requests_succeeded, 2);
    assert_eq!(read("CRAWLEE_STATE_resume"), json!({ "pages": 2 }));
    let saved = read("CRAWLEE_CRAWLER_STATISTICS_resume");
    assert_eq!((saved["requestsSucceeded"].clone(), saved["statsId"].clone()), (json!(2), json!("resume")));
    assert!(saved["crawlerFinishedAt"].is_string());
    let pools: Vec<String> = std::fs::read_dir(dir.path().join("key_value_stores/default"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with("CRAWLEE_SESSION_POOL_STATE_") && !name.ends_with("__metadata__.json"))
        .collect();
    assert_eq!(pools.len(), 1, "{pools:?}");

    // Without purging, the next run continues: handled requests are not crawled again, and the
    // statistics and the state go on from the saved ones.
    let second = crawl(false, vec!["https://a.dev/1", "https://a.dev/3"]).await;
    assert_eq!(second.requests_succeeded, 3);
    assert_eq!(read("CRAWLEE_STATE_resume"), json!({ "pages": 3 }));
    assert_eq!(read("CRAWLEE_CRAWLER_STATISTICS_resume")["requestsSucceeded"], 3);
}

#[tokio::test]
async fn migrating_pauses_the_crawl_and_status_messages_are_emitted() {
    use crawlee_core::{Event, EventKind};

    let services = services();
    let messages = Arc::new(Mutex::new(Vec::new()));
    let log = messages.clone();
    services.events.on(EventKind::StatusMessage, move |event| {
        if let Event::StatusMessage(message) = event {
            log.lock().push((message.message, message.is_terminal));
        }
        async {}
    });

    let started = Arc::new(AtomicUsize::new(0));
    let counter = started.clone();
    let crawler = BasicCrawler::builder()
        .services(services.clone())
        .max_concurrency(1)
        .request_handler(move |_ctx: BasicContext| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            }
        })
        .build()
        .unwrap();

    let run = tokio::spawn({
        let crawler = crawler.clone();
        async move { crawler.run(["https://a.dev/1", "https://a.dev/2", "https://a.dev/3"]).await.unwrap() }
    });
    while started.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    services.events.emit(Event::Migrating);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(started.load(Ordering::SeqCst), 1, "no request starts while paused");
    assert!(!run.is_finished());

    crawler.resume().await.unwrap();
    let stats = run.await.unwrap();
    assert_eq!(stats.requests_succeeded, 3);

    let messages = messages.lock().clone();
    assert_eq!(messages.first().unwrap(), &("Starting the crawler.".to_owned(), false));
    assert_eq!(messages.last().unwrap(), &("Finished! Total 3 requests: 3 succeeded, 0 failed.".to_owned(), true));
}

/// A load signal the test switches between idle and overloaded.
#[derive(Default)]
struct TestSignal {
    overloaded: std::sync::atomic::AtomicBool,
}

impl crawlee_basic::LoadSignal for TestSignal {
    fn name(&self) -> &str {
        "test"
    }
    fn overloaded_ratio(&self) -> f64 {
        0.5
    }
    fn start(&self, _: &Services, _: Duration) {}
    fn stop(&self) {}
    fn sample(&self, _: Option<Duration>) -> Vec<crawlee_basic::autoscaling::LoadSnapshot> {
        vec![crawlee_basic::autoscaling::LoadSnapshot {
            created_at: chrono::Utc::now(),
            is_overloaded: self.overloaded.load(Ordering::SeqCst),
        }]
    }
}

async fn peak_concurrency(overloaded: bool) -> (usize, usize) {
    use crawlee_basic::{ConcurrencyOptions, LoadSignalsOptions};

    let signal = Arc::new(TestSignal::default());
    signal.overloaded.store(overloaded, Ordering::SeqCst);
    let custom: Arc<dyn crawlee_basic::LoadSignal> = signal;
    let (running, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (running_in, peak_in) = (running.clone(), peak.clone());
    let crawler = BasicCrawler::builder()
        .services(services())
        .concurrency_options(ConcurrencyOptions {
            min_concurrency: 1,
            max_concurrency: 8,
            scale_up_step_ratio: 0.5,
            autoscale_interval: Duration::from_millis(20),
            load_signals: LoadSignalsOptions {
                memory: None,
                event_loop: None,
                cpu: None,
                storage: None,
                custom: vec![custom],
            },
            ..ConcurrencyOptions::default()
        })
        .request_handler(move |_ctx: BasicContext| {
            let (running, peak) = (running_in.clone(), peak_in.clone());
            async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(30)).await;
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .build()
        .unwrap();
    let urls: Vec<String> = (0..80).map(|i| format!("https://a.dev/{i}")).collect();
    assert_eq!(crawler.run(urls).await.unwrap().requests_succeeded, 80);
    (peak.load(Ordering::SeqCst), crawler.concurrency_system().desired_concurrency())
}

#[tokio::test]
async fn concurrency_scales_with_the_load() {
    let (peak, desired) = peak_concurrency(false).await;
    assert_eq!(desired, 8, "scaled up to the max while idle");
    assert!(peak > 1 && peak <= 8, "peak {peak}");

    let (peak, desired) = peak_concurrency(true).await;
    assert_eq!((peak, desired), (1, 1), "overloaded: stays at min_concurrency");
}

/// A proxy per new session, numbered.
struct NumberedProxies(AtomicUsize);

impl crawlee_basic::ProxySource for NumberedProxies {
    fn new_proxy_info(&self, _session_id: &str) -> Option<crawlee_basic::ProxyInfo> {
        let n = self.0.fetch_add(1, Ordering::Relaxed);
        Some(crawlee_basic::ProxyInfo {
            session_specific: true,
            ..crawlee_basic::ProxyInfo::from_url(
                url::Url::parse(&format!("http://session-{n}@proxy.example:8000")).unwrap(),
            )
        })
    }
}

#[tokio::test]
async fn without_a_session_pool_every_attempt_has_a_proxy_of_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let configuration = Configuration { storage_dir: dir.path().to_owned(), ..Configuration::default() };
    let proxies = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let (log, counter) = (proxies.clone(), attempts.clone());
    let crawler = BasicCrawler::builder()
        .services(Services::from_configuration(configuration))
        .use_session_pool(false)
        .proxy_configuration(Arc::new(NumberedProxies(AtomicUsize::new(0))))
        .request_handler(move |ctx: BasicContext| {
            let (log, counter) = (log.clone(), counter.clone());
            async move {
                log.lock().push(ctx.proxy_info().unwrap().url.to_string());
                // The first attempt of each request fails, so that the retry needs a new proxy too.
                if counter.fetch_add(1, Ordering::Relaxed) % 2 == 0 {
                    anyhow::bail!("blocked");
                }
                Ok(())
            }
        })
        .build()
        .unwrap();

    crawler.run(["https://a.dev/1", "https://a.dev/2", "https://a.dev/3"]).await.unwrap();

    let mut proxies = proxies.lock().clone();
    let total = proxies.len();
    proxies.sort();
    proxies.dedup();
    assert!(total >= 3, "{total} attempts");
    assert_eq!(proxies.len(), total, "no proxy is used twice");
    assert_eq!(crawler.session_pool().usable_count() + crawler.session_pool().retired_count(), 0);
    let pools = std::fs::read_dir(dir.path().join("key_value_stores/default"))
        .unwrap()
        .filter(|entry| {
            entry.as_ref().unwrap().file_name().to_string_lossy().starts_with("CRAWLEE_SESSION_POOL_STATE_")
        })
        .count();
    assert_eq!(pools, 0, "no session pool is saved");
}

/// Answers every request, and records the proxies given up.
#[derive(Default)]
struct RecordingClient {
    released: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl crawlee_http_client::HttpClient for RecordingClient {
    async fn send_request(
        &self,
        request: crawlee_http_client::HttpRequest,
        _: &crawlee_http_client::SendOptions,
    ) -> Result<crawlee_http_client::HttpResponse, crawlee_http_client::HttpClientError> {
        Ok(crawlee_http_client::HttpResponse {
            status: http::StatusCode::OK,
            headers: http::HeaderMap::new(),
            url: request.url,
            body: Default::default(),
            redirects: Vec::new(),
        })
    }

    fn release_proxy(&self, proxy_url: &url::Url) {
        self.released.lock().push(proxy_url.to_string());
    }
}

#[tokio::test]
async fn without_a_session_pool_the_proxy_of_an_attempt_is_released_after_it() {
    for use_session_pool in [false, true] {
        let client = Arc::new(RecordingClient::default());
        let used = Arc::new(Mutex::new(Vec::new()));
        let (log, recorder) = (used.clone(), client.clone());
        let crawler = BasicCrawler::builder()
            .services(services())
            .use_session_pool(use_session_pool)
            .proxy_configuration(Arc::new(NumberedProxies(AtomicUsize::new(0))))
            .http_client(client.clone())
            .max_request_retries(1)
            .request_handler(move |ctx: BasicContext| {
                let (log, recorder) = (log.clone(), recorder.clone());
                async move {
                    let proxy = ctx.proxy_info().unwrap().url.to_string();
                    // Several calls of one attempt share its proxy, which is not released meanwhile.
                    for _ in 0..2 {
                        let url = url::Url::parse(&ctx.request().url)?;
                        ctx.send_request(crawlee_http_client::HttpRequest::get(url)).await?;
                    }
                    assert!(!recorder.released.lock().contains(&proxy), "released during the attempt");
                    let first_attempt = ctx.request().retry_count == 0;
                    log.lock().push(proxy);
                    if first_attempt {
                        anyhow::bail!("blocked");
                    }
                    Ok(())
                }
            })
            .build()
            .unwrap();

        crawler.run(["https://a.dev/1", "https://a.dev/2"]).await.unwrap();

        let mut used = used.lock().clone();
        let mut released = client.released.lock().clone();
        used.sort();
        released.sort();
        assert_eq!(used.len(), 4, "two attempts of each request");
        if use_session_pool {
            assert!(released.is_empty(), "pooled sessions keep their proxies: {released:?}");
        } else {
            assert_eq!(released, used, "each proxy released once");
        }
    }
}

/// The URLs of a static `ProxyConfiguration` are shared by many attempts: their clients (and
/// connections) are kept, as JS keeps its clients per proxy URL.
#[tokio::test]
async fn without_a_session_pool_shared_proxies_are_not_released() {
    let client = Arc::new(RecordingClient::default());
    let crawler = BasicCrawler::builder()
        .services(services())
        .use_session_pool(false)
        .proxy_configuration(Arc::new(
            crawlee_basic::ProxyConfiguration::new(["http://p1.example:8000", "http://p2.example:8000"]).unwrap(),
        ))
        .http_client(client.clone())
        .request_handler(|ctx: BasicContext| async move {
            let url = url::Url::parse(&ctx.request().url)?;
            ctx.send_request(crawlee_http_client::HttpRequest::get(url)).await?;
            Ok(())
        })
        .build()
        .unwrap();

    crawler.run(["https://a.dev/1", "https://a.dev/2", "https://a.dev/3"]).await.unwrap();
    assert!(client.released.lock().is_empty(), "released: {:?}", client.released.lock());
}
