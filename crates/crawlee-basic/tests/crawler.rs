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
    // One finished request that needed retries, however many (`requestsRetries` of JS).
    assert_eq!(stats.requests_retries, 1);
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
async fn skipped_requests_do_not_count_towards_max_requests_per_crawl() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_concurrency(1)
        .max_requests_per_crawl(2)
        .request_handler(|ctx: BasicContext| async move {
            if ctx.request().url.ends_with("/skip") {
                return Err(ctx.skip_request(None));
            }
            ctx.add_requests(["https://a.dev/2", "https://a.dev/3"]).await?;
            Ok(())
        })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.dev/skip", "https://a.dev/1"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_skipped), (2, 1));
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
    // Built first, so that neither crawl below is the first crawler of the process, which alone
    // uses the default request queue.
    noop_crawler().build().unwrap();
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
async fn aborting_pauses_the_crawl_and_status_messages_are_emitted() {
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
    // `Migrating` pauses the crawl too, but also the request queue, for good: the run moves.
    services.events.emit(Event::Aborting);
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
    (peak.load(Ordering::SeqCst), crawler.concurrency_system().unwrap().desired_concurrency())
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

type Skipped = Arc<Mutex<Vec<(String, crawlee_basic::SkipReason)>>>;

fn skip_log() -> (Skipped, impl Fn(crawlee_basic::SkippedRequest) -> std::future::Ready<()> + Send + Sync + 'static) {
    let skipped: Skipped = Arc::default();
    let log = skipped.clone();
    (skipped, move |skipped: crawlee_basic::SkippedRequest| {
        log.lock().push((skipped.request.url, skipped.reason));
        std::future::ready(())
    })
}

#[tokio::test]
async fn max_requests_per_crawl_caps_what_is_enqueued() {
    let (skipped, on_skipped) = skip_log();
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_requests_per_crawl(3)
        .on_skipped_request(on_skipped)
        .request_handler(|ctx: BasicContext| async move {
            if ctx.request().crawl_depth() == 0 {
                ctx.add_requests((0..10).map(|i| format!("https://a.dev/{i}"))).await?;
            }
            Ok(())
        })
        .build()
        .unwrap();

    let stats = crawler.run(["https://a.dev/"]).await.unwrap();
    assert_eq!(stats.requests_succeeded, 3);
    // 3 less the start request, which was pending: 2 were added, and the rest skipped.
    let manager = crawler.request_manager().await.unwrap();
    assert_eq!(manager.pending_count().await.unwrap(), 0);
    assert_eq!(manager.handled_count().await.unwrap(), 3);
    let skipped = skipped.lock().clone();
    assert_eq!(skipped.len(), 8);
    assert!(skipped.iter().all(|(_, reason)| *reason == crawlee_basic::SkipReason::Limit));
}

#[tokio::test]
async fn the_enqueue_limit_counts_only_new_requests() {
    let added = Arc::new(Mutex::new(Vec::new()));
    let log = added.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(move |ctx: BasicContext| {
            let log = log.clone();
            async move {
                if ctx.request().crawl_depth() > 0 {
                    return Ok(());
                }
                ctx.add_requests(["https://a.dev/a", "https://a.dev/b"]).await?;
                let urls =
                    ["a", "b", "c", "d", "e"].map(|path| url::Url::parse(&format!("https://a.dev/{path}")).unwrap());
                let result = ctx.enqueue_urls(urls, &crawlee_basic::EnqueueLinksOptions::new().limit(2)).await?;
                log.lock().push((result.newly_added(), result.skipped.len()));
                Ok(())
            }
        })
        .build()
        .unwrap();

    let stats = crawler.run(["https://a.dev/"]).await.unwrap();
    assert_eq!(*added.lock(), [(2, 1)], "c and d are added, e is over the limit");
    assert_eq!(stats.requests_succeeded, 5);
}

/// Serves a robots.txt that disallows `/private`, and an empty page for everything else.
struct RobotsClient;

#[async_trait::async_trait]
impl crawlee_http_client::HttpClient for RobotsClient {
    async fn send_request(
        &self,
        request: crawlee_http_client::HttpRequest,
        _: &crawlee_http_client::SendOptions,
    ) -> Result<crawlee_http_client::HttpResponse, crawlee_http_client::HttpClientError> {
        let body = if request.url.path() == "/robots.txt" { "User-agent: *\nDisallow: /private\n" } else { "" };
        Ok(crawlee_http_client::HttpResponse {
            status: http::StatusCode::OK,
            headers: http::HeaderMap::new(),
            url: request.url,
            body: body.into(),
            redirects: Vec::new(),
        })
    }
}

#[tokio::test]
async fn context_add_requests_skips_what_is_too_deep_disallowed_or_filtered_out() {
    let (skipped, on_skipped) = skip_log();
    let handled = Arc::new(Mutex::new(Vec::new()));
    let log = handled.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_crawl_depth(1)
        .respect_robots_txt(true)
        .http_client(Arc::new(RobotsClient))
        .on_skipped_request(on_skipped)
        .request_handler(move |ctx: BasicContext| {
            let log = log.clone();
            async move {
                log.lock().push(ctx.request().url.clone());
                let next = format!("https://a.dev/level/{}", ctx.request().crawl_depth() + 1);
                ctx.add_requests([next, "https://a.dev/private/1".to_owned()]).await?;
                if ctx.request().crawl_depth() == 0 {
                    let options = crawlee_basic::CrawlerAddRequestsOptions::new()
                        .exclude(["https://a.dev/excluded*"])
                        .label("LABELED");
                    let result = ctx
                        .add_requests_with_options(["https://a.dev/excluded", "https://a.dev/kept"], options)
                        .await?;
                    assert_eq!(result.added_requests.len(), 1);
                }
                Ok(())
            }
        })
        .build()
        .unwrap();

    crawler.run(["https://a.dev/"]).await.unwrap();
    let mut handled = handled.lock().clone();
    handled.sort();
    assert_eq!(handled, ["https://a.dev/", "https://a.dev/kept", "https://a.dev/level/1"]);
    let skipped = skipped.lock().clone();
    use crawlee_basic::SkipReason::{Depth, Filters, RobotsTxt};
    assert!(skipped.contains(&("https://a.dev/excluded".to_owned(), Filters)), "{skipped:?}");
    assert!(skipped.contains(&("https://a.dev/private/1".to_owned(), RobotsTxt)), "{skipped:?}");
    assert!(skipped.contains(&("https://a.dev/level/2".to_owned(), Depth)), "{skipped:?}");
}

/// A request queue that answers fetches after a delay, like a remote one.
struct SlowFetches(crawlee_core::RequestQueue);

#[async_trait::async_trait]
impl crawlee_core::RequestManager for SlowFetches {
    async fn add_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> crawlee_core::StorageResult<crawlee_core::storage::backend::BatchAddRequestsResult> {
        self.0.add_requests(requests, forefront).await
    }
    async fn fetch_next_request(&self) -> crawlee_core::StorageResult<Option<Request>> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.0.fetch_next_request().await
    }
    async fn mark_request_as_handled(
        &self,
        request: &mut Request,
    ) -> crawlee_core::StorageResult<Option<crawlee_core::storage::backend::QueueOperationInfo>> {
        self.0.mark_request_as_handled(request).await
    }
    async fn reclaim_request(
        &self,
        request: &Request,
        forefront: bool,
    ) -> crawlee_core::StorageResult<Option<crawlee_core::storage::backend::QueueOperationInfo>> {
        self.0.reclaim_request(request, forefront).await
    }
    async fn is_empty(&self) -> crawlee_core::StorageResult<bool> {
        self.0.is_empty().await
    }
    async fn is_finished(&self) -> crawlee_core::StorageResult<bool> {
        self.0.is_finished().await
    }
    async fn handled_count(&self) -> crawlee_core::StorageResult<u64> {
        self.0.handled_count().await
    }
}

#[tokio::test]
async fn requests_are_fetched_in_parallel_by_the_tasks() {
    let services = services();
    let queue = services.open_request_queue(&crawlee_core::StorageIdentifier::Default).await.unwrap();
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(Arc::new(SlowFetches(queue)))
        .fixed_concurrency(10)
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .build()
        .unwrap();

    let started = std::time::Instant::now();
    let urls: Vec<String> = (0..30).map(|i| format!("https://a.dev/{i}")).collect();
    let stats = crawler.run(urls).await.unwrap();
    assert_eq!(stats.requests_succeeded, 30);
    // One fetch after another would take 30 × 50 ms.
    assert!(started.elapsed() < Duration::from_millis(1000), "{:?}", started.elapsed());
}

#[tokio::test]
async fn a_paced_crawl_wakes_up_when_its_domain_is_ready() {
    let starts = Arc::new(Mutex::new(Vec::new()));
    let log = starts.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .same_domain_delay(Duration::from_millis(700))
        .request_handler(move |_ctx: BasicContext| {
            let log = log.clone();
            async move {
                log.lock().push(std::time::Instant::now());
                Ok(())
            }
        })
        .build()
        .unwrap();

    crawler.run(["https://a.dev/1", "https://a.dev/2"]).await.unwrap();
    let starts = starts.lock().clone();
    let gap = starts[1] - starts[0];
    // Polling every 500 ms would start the second one about 1 s after the first.
    assert!(gap >= Duration::from_millis(690) && gap < Duration::from_millis(900), "{gap:?}");
}

#[tokio::test]
async fn a_failing_error_handler_ends_the_run_with_its_error() {
    for failed_request_handler in [false, true] {
        let builder = BasicCrawler::builder()
            .services(services())
            .max_request_retries(1)
            .request_handler(|_ctx: BasicContext| async move { anyhow::bail!("handler failed") });
        let handler =
            |_ctx: BasicContext, _error: Arc<anyhow::Error>| async move { anyhow::bail!("error handler failed") };
        let builder = if failed_request_handler {
            builder.failed_request_handler(handler)
        } else {
            builder.error_handler(handler)
        };
        let crawler = builder.build().unwrap();
        let err = crawler.run(["https://a.dev/"]).await.unwrap_err();
        assert_eq!(err.to_string(), "error handler failed");
    }
}

#[tokio::test]
async fn a_request_that_stopped_the_crawl_is_given_back_to_the_queue() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(|_ctx: BasicContext| async move { Err(CriticalError::new("stop everything").into()) })
        .build()
        .unwrap();
    crawler.run(["https://a.dev/"]).await.unwrap_err();
    let manager = crawler.request_manager().await.unwrap();
    let request = manager.fetch_next_request().await.unwrap().expect("reclaimed, not left in progress");
    assert_eq!(request.url, "https://a.dev/");
}

#[tokio::test]
async fn a_request_pinned_to_a_missing_session_fails() {
    let sessions = Arc::new(Mutex::new(Vec::new()));
    let log = sessions.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_request_retries(1)
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .failed_request_handler(move |ctx: BasicContext, _error: Arc<anyhow::Error>| {
            let log = log.clone();
            async move {
                log.lock().push((ctx.session().is_some(), ctx.request().error_messages.clone()));
                Ok(())
            }
        })
        .build()
        .unwrap();
    let request = Request::builder("https://a.dev/").session_id("missing").build().unwrap();
    let stats = crawler.run([request]).await.unwrap();
    assert_eq!(stats.requests_failed, 1);
    let sessions = sessions.lock();
    let (has_session, messages) = &sessions[0];
    assert!(!has_session);
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0],
        "The current SessionPool instance couldn't find a valid session for the following id: missing."
    );
}

/// A request queue whose first marks of a request as handled fail.
struct FlakyMarks(crawlee_core::RequestQueue, AtomicUsize);

#[async_trait::async_trait]
impl crawlee_core::RequestManager for FlakyMarks {
    async fn add_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> crawlee_core::StorageResult<crawlee_core::storage::backend::BatchAddRequestsResult> {
        self.0.add_requests(requests, forefront).await
    }
    async fn fetch_next_request(&self) -> crawlee_core::StorageResult<Option<Request>> {
        self.0.fetch_next_request().await
    }
    async fn mark_request_as_handled(
        &self,
        request: &mut Request,
    ) -> crawlee_core::StorageResult<Option<crawlee_core::storage::backend::QueueOperationInfo>> {
        if self.1.fetch_add(1, Ordering::SeqCst) < 2 {
            return Err(crawlee_core::StorageError::InvalidArgument("flaky".into()));
        }
        self.0.mark_request_as_handled(request).await
    }
    async fn reclaim_request(
        &self,
        request: &Request,
        forefront: bool,
    ) -> crawlee_core::StorageResult<Option<crawlee_core::storage::backend::QueueOperationInfo>> {
        self.0.reclaim_request(request, forefront).await
    }
    async fn is_empty(&self) -> crawlee_core::StorageResult<bool> {
        self.0.is_empty().await
    }
    async fn is_finished(&self) -> crawlee_core::StorageResult<bool> {
        self.0.is_finished().await
    }
    async fn handled_count(&self) -> crawlee_core::StorageResult<u64> {
        self.0.handled_count().await
    }
}

#[tokio::test]
async fn storage_operations_of_a_request_are_retried() {
    let services = services();
    let queue = services.open_request_queue(&crawlee_core::StorageIdentifier::Default).await.unwrap();
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_manager(Arc::new(FlakyMarks(queue, AtomicUsize::new(0))))
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.dev/"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed, stats.requests_retries), (1, 0, 0));
}

#[tokio::test]
async fn each_attempt_starts_without_a_loaded_url() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(move |mut ctx: BasicContext| {
            let log = log.clone();
            async move {
                log.lock().push(ctx.request().loaded_url.clone());
                ctx.request_mut().loaded_url = Some("https://a.dev/loaded".into());
                if ctx.request().retry_count == 0 {
                    anyhow::bail!("again");
                }
                Ok(())
            }
        })
        .build()
        .unwrap();
    crawler.run(["https://a.dev/"]).await.unwrap();
    assert_eq!(*seen.lock(), [None, None]);
}

#[tokio::test]
async fn skipped_requests_are_reported_with_their_reason_and_message() {
    let skipped = Arc::new(Mutex::new(Vec::new()));
    let log = skipped.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .on_skipped_request(move |skipped: crawlee_basic::SkippedRequest| {
            log.lock().push((skipped.request.url, skipped.reason, skipped.message));
            std::future::ready(())
        })
        .request_handler(|ctx: BasicContext| async move {
            if ctx.request().url.ends_with("/manual") {
                return Err(ctx.skip_request(Some("not interesting")));
            }
            anyhow::bail!("fails, then skipped by the error handler")
        })
        .error_handler(|ctx: BasicContext, _error: Arc<anyhow::Error>| async move { Err(ctx.skip_request(None)) })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.dev/manual", "https://a.dev/from-error-handler"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed, stats.requests_skipped), (0, 0, 2));
    let mut skipped = skipped.lock().clone();
    skipped.sort_by(|a, b| a.0.cmp(&b.0));
    use crawlee_basic::SkipReason::Manual;
    assert_eq!(
        skipped,
        [
            ("https://a.dev/from-error-handler".to_owned(), Manual, None),
            ("https://a.dev/manual".to_owned(), Manual, Some("not interesting".to_owned())),
        ]
    );
}

#[tokio::test]
async fn a_route_can_have_a_handler_timeout_of_its_own() {
    let mut router = Router::<BasicContext>::new();
    router.add_handler_with_timeout(
        "SLOW",
        |_ctx: BasicContext| async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(())
        },
        Duration::from_millis(50),
    );
    router.add_default_handler(|_ctx: BasicContext| async move { Ok(()) });
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_request_retries(0)
        .router(router)
        .failed_request_handler(|ctx: BasicContext, _e: Arc<anyhow::Error>| async move {
            ctx.push_data(&json!({ "error": ctx.request().error_messages[0] }))?;
            Ok(())
        })
        .build()
        .unwrap();
    let slow = Request::builder("https://a.dev/slow").label("SLOW").build().unwrap();
    let stats = crawler.run([slow, Request::new("https://a.dev/fast")]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed), (1, 1));
    assert_eq!(items(&crawler).await, [json!({ "error": "requestHandler timed out after 0.05 seconds." })]);
}

/// A request queue that records the processing time it is told to expect.
struct ExpectedTime(crawlee_core::RequestQueue, Mutex<Vec<Duration>>);

#[async_trait::async_trait]
impl crawlee_core::RequestManager for ExpectedTime {
    async fn add_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> crawlee_core::StorageResult<crawlee_core::storage::backend::BatchAddRequestsResult> {
        self.0.add_requests(requests, forefront).await
    }
    async fn fetch_next_request(&self) -> crawlee_core::StorageResult<Option<Request>> {
        self.0.fetch_next_request().await
    }
    async fn mark_request_as_handled(
        &self,
        request: &mut Request,
    ) -> crawlee_core::StorageResult<Option<crawlee_core::storage::backend::QueueOperationInfo>> {
        self.0.mark_request_as_handled(request).await
    }
    async fn reclaim_request(
        &self,
        request: &Request,
        forefront: bool,
    ) -> crawlee_core::StorageResult<Option<crawlee_core::storage::backend::QueueOperationInfo>> {
        self.0.reclaim_request(request, forefront).await
    }
    async fn is_empty(&self) -> crawlee_core::StorageResult<bool> {
        self.0.is_empty().await
    }
    async fn is_finished(&self) -> crawlee_core::StorageResult<bool> {
        self.0.is_finished().await
    }
    async fn handled_count(&self) -> crawlee_core::StorageResult<u64> {
        self.0.handled_count().await
    }
    async fn set_expected_request_processing_time(&self, duration: Duration) -> crawlee_core::StorageResult<()> {
        self.1.lock().push(duration);
        Ok(())
    }
}

#[tokio::test]
async fn requests_are_locked_for_the_longest_handler_timeout_and_a_margin() {
    for (handler_timeout, route_timeout, expected) in [(10, None, 60), (100, None, 105), (10, Some(120), 125)] {
        let services = services();
        let queue = services.open_request_queue(&crawlee_core::StorageIdentifier::Default).await.unwrap();
        let manager = Arc::new(ExpectedTime(queue, Mutex::new(Vec::new())));
        let mut router = Router::<BasicContext>::new();
        let handler = |_ctx: BasicContext| async move { Ok(()) };
        match route_timeout {
            Some(secs) => router.add_handler_with_timeout("LABEL", handler, Duration::from_secs(secs)),
            None => router.add_default_handler(handler),
        };
        let crawler = BasicCrawler::builder()
            .services(services)
            .request_manager(manager.clone())
            .request_handler_timeout(Duration::from_secs(handler_timeout))
            .router(router)
            .build()
            .unwrap();
        crawler.add_requests(Vec::<Request>::new()).await.unwrap();
        assert_eq!(*manager.1.lock(), [Duration::from_secs(expected)]);
    }
}

fn noop_crawler() -> crawlee_basic::BasicCrawlerBuilder<crawlee_basic::Identity> {
    BasicCrawler::builder().services(services()).request_handler(|_ctx: BasicContext| async move { Ok(()) })
}

#[tokio::test]
async fn a_running_crawler_cannot_be_run_again() {
    let crawler = BasicCrawler::builder()
        .services(services())
        .request_handler(|_ctx: BasicContext| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(())
        })
        .build()
        .unwrap();
    let first = tokio::spawn({
        let crawler = crawler.clone();
        async move { crawler.run(["https://a.dev/1"]).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = crawler.run(["https://a.dev/2"]).await.unwrap_err();
    assert!(err.to_string().contains("already running"), "{err}");
    first.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_repeated_run_starts_with_fresh_statistics_sessions_and_concurrency() {
    let crawler = noop_crawler().build().unwrap();
    assert!(crawler.concurrency_system().is_none(), "none before the first run");
    let first = crawler.run(["https://a.dev/1", "https://a.dev/2"]).await.unwrap();
    assert_eq!(first.requests_succeeded, 2);
    let first_system = crawler.concurrency_system().unwrap();
    assert!(crawler.session_pool().usable_count() > 0);

    let second = crawler.run(["https://a.dev/3"]).await.unwrap();
    assert_eq!(second.requests_succeeded, 1, "the statistics of the first run are reset");
    assert!(!Arc::ptr_eq(&first_system, &crawler.concurrency_system().unwrap()));
}

#[tokio::test]
async fn statistics_sessions_and_concurrency_can_be_injected() {
    use crawlee_basic::{ConcurrencySystem, SessionPool, Statistics};

    let statistics = Arc::new(Statistics::with_id("injected"));
    let pool = Arc::new(SessionPool::new(Default::default(), None));
    let system = Arc::new(ConcurrencySystem::new(Default::default()).unwrap());
    let crawler = noop_crawler()
        .statistics(statistics.clone())
        .session_pool(pool.clone())
        .concurrency_system(system.clone())
        .build()
        .unwrap();

    // The caller starts and stops an injected concurrency system.
    assert!(crawler.run(["https://a.dev/1"]).await.is_err());
    let services = services();
    system.start(&services);
    crawler.run(["https://a.dev/1"]).await.unwrap();
    crawler.run(["https://a.dev/2"]).await.unwrap();
    assert!(system.is_running());
    system.stop();
    assert_eq!(statistics.requests_succeeded(), 2, "an injected instance is not reset");
    assert!(Arc::ptr_eq(&crawler.concurrency_system().unwrap(), &system));
    assert!(pool.usable_count() > 0);

    let conflict = noop_crawler().max_concurrency(3).concurrency_system(system).build();
    assert!(conflict.is_err());
}

#[tokio::test]
async fn stop_lets_running_requests_finish_and_starts_no_more() {
    let slot: Arc<Mutex<Option<BasicCrawler>>> = Arc::default();
    let handled = Arc::new(AtomicUsize::new(0));
    let (crawler_slot, counter) = (slot.clone(), handled.clone());
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_concurrency(1)
        .request_handler(move |_ctx: BasicContext| {
            let (crawler_slot, counter) = (crawler_slot.clone(), counter.clone());
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                crawler_slot.lock().as_ref().unwrap().stop(None);
                Ok(())
            }
        })
        .build()
        .unwrap();
    *slot.lock() = Some(crawler.clone());
    let stats = crawler.run((1..=5).map(|i| format!("https://a.dev/{i}"))).await.unwrap();
    assert_eq!(stats.requests_succeeded, 1);
    assert_eq!(handled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn keep_alive_waits_for_more_requests_until_stopped() {
    let crawler = noop_crawler().keep_alive(true).build().unwrap();
    let run = tokio::spawn({
        let crawler = crawler.clone();
        async move { crawler.run(Vec::<String>::new()).await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!run.is_finished(), "an empty queue does not end the run");
    crawler.add_requests(["https://a.dev/late"]).await.unwrap();
    while crawler.statistics().requests_succeeded() == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    crawler.stop(Some("done"));
    assert_eq!(run.await.unwrap().requests_succeeded, 1);
}

#[tokio::test]
async fn the_task_loop_predicates_can_be_replaced() {
    let crawler = noop_crawler()
        .task_loop_options(crawlee_basic::TaskLoopOptions {
            is_task_ready: Some(Arc::new(|| Box::pin(async { Ok(false) }))),
            is_finished: Some(Arc::new(|| Box::pin(async { Ok(true) }))),
        })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.dev/1"]).await.unwrap();
    assert_eq!(stats.requests_succeeded, 0);
}

#[tokio::test]
async fn crawlers_have_request_queues_of_their_own() {
    let services = services();
    let build = |id: &str| {
        BasicCrawler::builder()
            .services(services.clone())
            .id(id)
            .request_handler(|_ctx: BasicContext| async move { Ok(()) })
            .build()
            .unwrap()
    };
    let (first, second) = (build("first"), build("second"));
    first.add_requests(["https://a.dev/first"]).await.unwrap();
    let stats = second.run(["https://a.dev/second"]).await.unwrap();
    assert_eq!(stats.requests_succeeded, 1, "the first crawler's request is not in its queue");
}

#[tokio::test]
async fn the_status_message_counts_the_total_and_can_go_to_a_callback() {
    let messages = Arc::new(Mutex::new(Vec::new()));
    let log = messages.clone();
    let crawler = BasicCrawler::builder()
        .services(services())
        .max_concurrency(1)
        .options(crawlee_basic::CrawlerOptions {
            status_message_interval: Duration::from_millis(50),
            ..Default::default()
        })
        .status_message_callback(move |status: crawlee_basic::StatusMessageInfo| {
            log.lock().push(status.message);
            std::future::ready(())
        })
        .request_handler(|_ctx: BasicContext| async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            Ok(())
        })
        .build()
        .unwrap();
    crawler.run(["https://a.dev/1", "https://a.dev/2"]).await.unwrap();
    let messages = messages.lock().clone();
    assert!(messages.iter().any(|message| message.starts_with("Crawled 1/2 pages")), "{messages:?}");
}

#[tokio::test]
async fn the_default_queue_downloads_url_lists_with_the_crawler_http_client() {
    let services = services();
    let crawler = BasicCrawler::builder()
        .services(services.clone())
        .request_handler(|_ctx: BasicContext| async { Ok(()) })
        .build()
        .unwrap();
    crawler.run(Vec::<Request>::new()).await.unwrap();
    // The first crawler of the process uses the default queue, later ones a queue of their own;
    // other tests in this binary create crawlers too.
    let identifier = match crawler.id() {
        "0" => crawlee_core::StorageIdentifier::Default,
        id => crawlee_core::StorageIdentifier::Alias(format!("__default_{id}__")),
    };
    let queue = services.open_request_queue(&identifier).await.unwrap();
    assert!(queue.has_url_list_fetcher());
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

#[tokio::test]
async fn the_internal_timeout_comes_from_the_configuration() {
    let configuration =
        Configuration { internal_timeout: Some(Duration::from_millis(200)), ..Configuration::default() };
    let services = Services::with_configuration(configuration, Arc::new(crawlee_core::MemoryStorageBackend::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_handler(move |_ctx: BasicContext| {
            let counter = counter.clone();
            async move {
                match counter.fetch_add(1, Ordering::SeqCst) {
                    0 => anyhow::bail!("boom"),
                    _ => Ok(()),
                }
            }
        })
        // Outlasts the internal timeout of 200 ms, but not the default one (at least 5 min).
        .error_handler(|_ctx: BasicContext, _error: Arc<anyhow::Error>| async move {
            tokio::time::sleep(Duration::from_secs(600)).await;
            Ok(())
        })
        .build()
        .unwrap();

    let stats = tokio::time::timeout(Duration::from_secs(10), crawler.run(["https://a.dev/"])).await.unwrap().unwrap();
    assert_eq!(stats.requests_succeeded, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn crawls_a_request_list_through_the_queue() {
    use crawlee_core::RequestList;
    use crawlee_core::storage::request_loader::{LoaderStatus, RequestLoader};

    let services = services();
    let list = RequestList::open(&services, None, ["https://a.dev/1", "https://a.dev/2"]).await.unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let crawler = BasicCrawler::builder()
        .services(services.clone())
        .max_concurrency(1)
        .request_list(Arc::new(list.clone()))
        .request_handler(move |ctx: BasicContext| {
            let log = log.clone();
            async move {
                let url = ctx.request().url.clone();
                if url == "https://a.dev/1" {
                    ctx.add_requests(["https://a.dev/3"]).await?;
                }
                log.lock().push(url);
                Ok(())
            }
        })
        .build()
        .unwrap();

    let stats = crawler.run(Vec::<String>::new()).await.unwrap();
    assert_eq!(stats.requests_succeeded, 3);
    // The list's requests go to the front of the queue, before the added one.
    assert_eq!(*seen.lock(), ["https://a.dev/1", "https://a.dev/2", "https://a.dev/3"]);
    assert_eq!(list.status().await.unwrap(), LoaderStatus::Finished);
    let manager = crawler.request_manager().await.unwrap();
    assert_eq!(manager.handled_count().await.unwrap(), 3);

    // A request manager is combined with a list by the caller (`RequestList::to_tandem`).
    let built = BasicCrawler::builder()
        .services(services.clone())
        .request_list(Arc::new(list.clone()))
        .request_manager(manager)
        .request_handler(|_ctx: BasicContext| async { Ok(()) })
        .build();
    assert!(matches!(built, Err(crawlee_basic::BuildError::RequestListWithRequestManager)));
}

/// What the Apify SDK does on a graceful abort: `Aborting`, then the exit's last `PersistState`
/// right away, then it waits for the listeners (the crawler's pause) before the process ends.
#[tokio::test]
async fn an_abort_saves_all_state_once_the_running_requests_finished() {
    use crawlee_core::{Event, RequestList, RequestListOptions, RequestListState, StorageIdentifier};

    let services = services();
    let options = RequestListOptions {
        sources: ["https://a.dev/1", "https://a.dev/2"].into_iter().map(Into::into).collect(),
        persist_state_key: Some("INPUTS".into()),
        ..RequestListOptions::default()
    };
    let list = RequestList::open_with_options(&services, options).await.unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let counter = started.clone();
    let crawler = BasicCrawler::builder()
        .services(services.clone())
        .id("abort")
        .max_concurrency(1)
        .request_manager(Arc::new(list.with_queue(None).await.unwrap()))
        .request_handler(move |ctx: BasicContext| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(200)).await;
                let state = ctx.use_state(|| json!({ "pages": 0 })).await?;
                let mut state = state.lock();
                state["pages"] = json!(state["pages"].as_u64().unwrap() + 1);
                Ok(())
            }
        })
        .build()
        .unwrap();
    let run = tokio::spawn({
        let crawler = crawler.clone();
        async move { crawler.run(Vec::<String>::new()).await }
    });
    while started.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    services.events.emit(Event::Aborting);
    services.events.emit(Event::PersistState { is_migrating: false });
    services.events.wait_for_all_listeners_to_complete().await;

    // The request running at the abort finished during the pause, and every state was saved after
    // it: the list has it handled, not in progress, so a resurrected run does not crawl it again,
    // and the state it changed is saved too.
    let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
    let saved: RequestListState = store.get_value("CRAWLEE_INPUTS").await.unwrap().unwrap();
    assert_eq!((saved.next_index, saved.in_progress), (1, Vec::<String>::new()));
    assert_eq!(store.get_value::<Value>("CRAWLEE_STATE_abort").await.unwrap(), Some(json!({ "pages": 1 })));
    run.abort();
}

#[tokio::test]
async fn crawls_a_request_list_beside_the_queue() {
    use crawlee_core::{RequestList, RequestManager, StorageIdentifier};

    let services = services();
    let list =
        RequestList::open(&services, None, ["https://a.dev/1", "https://a.dev/2", "https://a.dev/3"]).await.unwrap();
    let queue = Arc::new(services.open_request_queue(&StorageIdentifier::alias("beside")).await.unwrap());
    let manager = Arc::new(list.with_queue(Some(queue.clone())).await.unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let crawler = BasicCrawler::builder()
        .services(services.clone())
        .max_concurrency(1)
        .request_manager(manager.clone())
        .request_handler(move |mut ctx: BasicContext| {
            let log = log.clone();
            async move {
                let (url, retry_count) = (ctx.request().url.clone(), ctx.request().retry_count);
                log.lock().push((url.clone(), retry_count));
                match (url.as_str(), retry_count) {
                    // Adds a new request and one the list has.
                    ("https://a.dev/1", 0) => {
                        ctx.add_requests(["https://a.dev/new", "https://a.dev/2"]).await?;
                        anyhow::bail!("fails once, unchanged")
                    }
                    ("https://a.dev/2", 0) => {
                        ctx.request_mut().user_data.insert("tried".into(), json!(true));
                        anyhow::bail!("fails once, changed")
                    }
                    _ => Ok(()),
                }
            }
        })
        .build()
        .unwrap();

    let stats = crawler.run(Vec::<String>::new()).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed), (4, 0));
    let seen: Vec<(String, u32)> = seen.lock().clone();
    let seen: Vec<(&str, u32)> = seen.iter().map(|(url, retries)| (url.as_str(), *retries)).collect();
    assert_eq!(
        seen,
        [
            // The queue first: what the run added, then the unchanged retry from the list.
            ("https://a.dev/1", 0),
            ("https://a.dev/new", 0),
            ("https://a.dev/1", 1),
            // The changed retry, at the front of the queue, before the rest of the list.
            ("https://a.dev/2", 0),
            ("https://a.dev/2", 1),
            ("https://a.dev/3", 0),
        ]
    );
    // The queue got only the new request and the changed retry.
    assert_eq!(queue.total_count().await.unwrap(), 2);
    assert_eq!(manager.handled_count().await.unwrap(), 4);
    assert!(manager.is_finished().await.unwrap());
}

/// A crawler whose every request to `a.dev` is answered with a 429, through a throttling manager
/// that gives up on a domain after `max_domain_stall`.
async fn always_rate_limited(services: &Services, keep_alive: bool, hits: Arc<AtomicUsize>) -> BasicCrawler {
    use crawlee_basic::{ThrottledDomains, ThrottlingOptions, ThrottlingRequestManager};
    use crawlee_core::errors::RequestThrottledError;
    use crawlee_core::{PacingSignal, RequestManager};

    let options = ThrottlingOptions {
        domains: ThrottledDomains::List(vec!["a.dev".into()]),
        base_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(100),
        max_domain_stall: Duration::from_millis(500),
        ..ThrottlingOptions::default()
    };
    let queue = services.open_request_queue(&crawlee_core::StorageIdentifier::alias("stall")).await.unwrap();
    let queue = Arc::new(queue);
    let manager = Arc::new(ThrottlingRequestManager::new(options, services, queue));
    let throttler = manager.clone();
    BasicCrawler::builder()
        .services(services.clone())
        .request_manager(manager)
        .keep_alive(keep_alive)
        .max_request_retries(0)
        .request_handler(move |ctx: BasicContext| {
            let (throttler, hits) = (throttler.clone(), hits.clone());
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                let url = ctx.request().url.clone();
                assert!(throttler.record_pacing_signal(&PacingSignal::RateLimited { url, wait: None }));
                Err(RequestThrottledError::new("429").into())
            }
        })
        .build()
        .unwrap()
}

#[tokio::test]
async fn a_domain_that_never_stops_rate_limiting_ends_the_crawl() {
    let services = services();
    let hits = Arc::new(AtomicUsize::new(0));
    let crawler = always_rate_limited(&services, false, hits.clone()).await;

    let result = tokio::time::timeout(Duration::from_secs(20), crawler.run(["https://a.dev/always-429"])).await;
    let err = result.expect("the crawl ends").unwrap_err();
    assert!(err.is::<crawlee_basic::PersistentRateLimitError>(), "{err:#}");
    assert!(err.to_string().starts_with("Giving up: \"a.dev\""), "{err}");
    assert!(hits.load(Ordering::SeqCst) > 1);
    // Left in its queue, so that a later run can pick it up.
    let manager = crawler.request_manager().await.unwrap();
    assert_eq!(manager.pending_count().await.unwrap(), 1);
}

#[tokio::test]
async fn keep_alive_outlives_a_domain_that_never_stops_rate_limiting() {
    let services = services();
    let crawler = always_rate_limited(&services, true, Arc::default()).await;
    let running = tokio::spawn({
        let crawler = crawler.clone();
        async move { crawler.run(["https://a.dev/always-429"]).await }
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!running.is_finished(), "still running");
    crawler.stop(None);
    running.await.unwrap().unwrap();
}

/// In-memory storage whose key-value stores take `delay` to save the statistics, and count it.
struct SlowStatisticsWrites {
    inner: crawlee_core::MemoryStorageBackend,
    delay: Duration,
    writes: Arc<AtomicUsize>,
}

struct SlowStatisticsStore {
    inner: Arc<dyn crawlee_core::storage::backend::KeyValueStoreBackend>,
    delay: Duration,
    writes: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl crawlee_core::storage::backend::KeyValueStoreBackend for SlowStatisticsStore {
    async fn get_metadata(&self) -> crawlee_core::StorageResult<crawlee_core::storage::backend::KeyValueStoreInfo> {
        self.inner.get_metadata().await
    }
    async fn drop_storage(&self) -> crawlee_core::StorageResult<()> {
        self.inner.drop_storage().await
    }
    async fn purge(&self) -> crawlee_core::StorageResult<()> {
        self.inner.purge().await
    }
    async fn get_value(
        &self,
        key: &str,
    ) -> crawlee_core::StorageResult<Option<crawlee_core::storage::backend::KeyValueStoreRecord>> {
        self.inner.get_value(key).await
    }
    async fn set_value(
        &self,
        record: crawlee_core::storage::backend::KeyValueStoreRecord,
    ) -> crawlee_core::StorageResult<()> {
        if record.key.contains("CRAWLER_STATISTICS") {
            self.writes.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
        }
        self.inner.set_value(record).await
    }
    async fn delete_value(&self, key: &str) -> crawlee_core::StorageResult<()> {
        self.inner.delete_value(key).await
    }
    async fn list_keys(
        &self,
        options: crawlee_core::storage::backend::KeyValueStoreListKeysOptions,
    ) -> crawlee_core::StorageResult<crawlee_core::storage::backend::KeyValueStoreListKeysResult> {
        self.inner.list_keys(options).await
    }
    async fn record_exists(&self, key: &str) -> crawlee_core::StorageResult<bool> {
        self.inner.record_exists(key).await
    }
}

#[async_trait::async_trait]
impl crawlee_core::StorageBackend for SlowStatisticsWrites {
    async fn create_dataset_backend(
        &self,
        id: &crawlee_core::StorageIdentifier,
    ) -> crawlee_core::StorageResult<Arc<dyn crawlee_core::storage::backend::DatasetBackend>> {
        self.inner.create_dataset_backend(id).await
    }
    async fn create_key_value_store_backend(
        &self,
        id: &crawlee_core::StorageIdentifier,
    ) -> crawlee_core::StorageResult<Arc<dyn crawlee_core::storage::backend::KeyValueStoreBackend>> {
        let inner = self.inner.create_key_value_store_backend(id).await?;
        Ok(Arc::new(SlowStatisticsStore { inner, delay: self.delay, writes: self.writes.clone() }))
    }
    async fn create_request_queue_backend(
        &self,
        id: &crawlee_core::StorageIdentifier,
    ) -> crawlee_core::StorageResult<Arc<dyn crawlee_core::storage::backend::RequestQueueBackend>> {
        self.inner.create_request_queue_backend(id).await
    }
}

/// The final saves at the end of a run go out together: the statistics, the sessions and
/// everything else that saves on `PersistState`, and the final status message with them. Crawlee
/// for JS makes them one after the other.
#[tokio::test]
async fn the_final_saves_go_out_together() {
    use crawlee_core::{Event, EventKind};

    let delay = Duration::from_millis(300);
    let writes = Arc::new(AtomicUsize::new(0));
    let backend =
        SlowStatisticsWrites { inner: crawlee_core::MemoryStorageBackend::new(), delay, writes: writes.clone() };
    let services = Services::new(Arc::new(backend));
    services.events.on(EventKind::PersistState, move |_| tokio::time::sleep(delay));
    let finished_at = Arc::new(Mutex::new(None));
    let set = finished_at.clone();
    services.events.on(EventKind::StatusMessage, move |event| {
        if let Event::StatusMessage(message) = event
            && message.is_terminal
        {
            *set.lock() = Some(std::time::Instant::now());
        }
        async {}
    });
    let crawler = BasicCrawler::builder()
        .services(services)
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .build()
        .unwrap();
    let started = std::time::Instant::now();
    crawler.run(["https://a.dev/1"]).await.unwrap();
    let elapsed = started.elapsed();
    let finished_at = finished_at.lock().expect("a final status message");
    assert!(
        finished_at.elapsed() >= delay - Duration::from_millis(50),
        "the final status message waited for the saves: {:?} before the end",
        finished_at.elapsed()
    );
    assert!(elapsed >= delay, "{elapsed:?}");
    assert!(elapsed < delay * 2 - Duration::from_millis(50), "the saves went out one after the other: {elapsed:?}");
    assert_eq!(writes.load(Ordering::SeqCst), 1, "the statistics are saved once");
}

/// Not in Crawlee for JS: the statistics can be saved under another key, such as the
/// `SDK_CRAWLER_STATISTICS_{id}` of Crawlee for JS 3, and a resumed crawl picks them up there.
#[tokio::test]
async fn statistics_can_be_saved_under_another_key() {
    use crawlee_basic::Statistics;

    let dir = tempfile::tempdir().unwrap();
    let crawl = |purge_on_start: bool, url: &'static str| {
        let configuration =
            Configuration { storage_dir: dir.path().to_owned(), purge_on_start, ..Configuration::default() };
        async move {
            let statistics =
                Arc::new(Statistics::with_id("keyed").with_persist_state_key("SDK_CRAWLER_STATISTICS_keyed"));
            let crawler = BasicCrawler::builder()
                .services(Services::from_configuration(configuration))
                .statistics(statistics.clone())
                .request_handler(|_ctx: BasicContext| async move { Ok(()) })
                .build()
                .unwrap();
            crawler.run([url]).await.unwrap();
            statistics
        }
    };
    let store = dir.path().join("key_value_stores/default");

    crawl(true, "https://a.dev/1").await;
    let saved: Value =
        serde_json::from_str(&std::fs::read_to_string(store.join("SDK_CRAWLER_STATISTICS_keyed")).unwrap()).unwrap();
    assert_eq!(saved["requestsSucceeded"], 1);
    assert!(!store.join("CRAWLEE_CRAWLER_STATISTICS_keyed").exists());

    let resumed = crawl(false, "https://a.dev/2").await;
    assert_eq!(resumed.requests_succeeded(), 2, "picked up from the other key");
}
