# Crawlee for Rust

A port of [Crawlee](https://crawlee.dev) to Rust, focused on what Rust is better at than
JavaScript for crawling: **fast HTTP crawling, HTML parsing and JSON processing**. It keeps
Crawlee's architecture and behavior; the JavaScript and Python versions remain the reference.

> Status: early. `BasicCrawler`, `HttpCrawler` and `HtmlCrawler` work end to end, and storage is
> written to `./storage` in the same format as Crawlee for JS and Python. See
> [What's not there yet](#whats-not-there-yet).

```rust
use crawlee::{EnqueueLinksOptions, HtmlContext, HtmlCrawler};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let crawler = HtmlCrawler::builder()
        .max_requests_per_crawl(50)
        .request_handler(|ctx: HtmlContext| async move {
            // Parsed lazily, once, on the blocking thread pool.
            let title = ctx.with_html(|doc| doc.title()).await?;
            ctx.push_data(&serde_json::json!({ "url": ctx.url().as_str(), "title": title }))?;
            // Links are extracted by streaming the body, without a DOM.
            ctx.enqueue_links(EnqueueLinksOptions::new()).await?;
            Ok(())
        })
        .build()?;

    let stats = crawler.run(["https://crawlee.dev"]).await?;
    println!("{} pages", stats.requests_succeeded);
    Ok(())
}
```

## Try it

These templates are ready to run; see [templates/README.md](templates/README.md):

```sh
cd templates/getting-started && cargo run   # HtmlCrawler on crawlee.dev → storage/results.json
cd templates/html && cargo run              # router with list and detail pages
cd templates/json-api && cargo run          # typed JSON from the Hacker News API
```

To start your own project from one of them:
`cargo generate --git https://github.com/apify-projects/crawlee-rs templates/html --name my-crawler`.

## Benchmark

This is the same workload in both implementations, crawling a local fixture site of 58 KB product
listings. For each page the handler parses the DOM, extracts the title and 20 products (3 selectors
each), pushes one dataset item and runs `enqueue_links` (about 80 links per page, most of them
duplicates). Concurrency is pinned (min = max) in both, and storage is in memory.

| Scenario | Implementation | Pages/s | CPU per page | Peak RSS |
|---|---|---:|---:|---:|
| 5,000 pages, no latency, concurrency 50 | crawlee-rs `HtmlCrawler` (reqwest) | **2,326** | **1.32 ms** | **120 MiB** |
| | Crawlee for JS `CheerioCrawler` (impit) | 87 | 13.46 ms | 386 MiB |
| 2,000 pages, 100 ms latency, concurrency 100 | crawlee-rs | **856** | **1.36 ms** | **72 MiB** |
| | Crawlee for JS | 81 | 14.47 ms | 373 MiB |

- **Setup:** 4 vCPUs, one run each. Measured with `crates/crawlee/examples/bench_*.rs` and
  `conformance/bench/cheerio-crawl.mts`; see [conformance/README.md](conformance/README.md).
- **CPU per page is the comparable metric.** Rust spends about 10× less CPU per page.
- **Throughput also reflects cores:** Rust uses every core, while Node runs the handler on one.
- **In the latency run, JS is still CPU-bound on a single core.** Rust sits close to the latency
  ceiling, which at concurrency 100 and 100 ms is 1,000 pages/s.
- **Caveats:**
  - The JS numbers come from running Crawlee from its TypeScript sources with Node 22 type
    stripping. That changes load time, not steady-state speed.
  - JS uses its default client, `impit`, which impersonates browsers. Rust uses plain reqwest.

## Crates

| Crate | Crawlee for JS counterpart | Contents |
|---|---|---|
| `crawlee` | `crawlee` | Re-exports everything below |
| `crawlee-utils` | `@crawlee/utils`, `@apify/utilities` | `normalize_url`, enqueue strategies, public-suffix domains, glob/regex URL filters, streaming link extraction (`lol_html`), HTML attribute entity decoding, charset prescan, robots.txt and sitemap parsing, `Retry-After` |
| `crawlee-core` | `@crawlee/core`, `@crawlee/types`, `@crawlee/fs-storage` | `Request` (with the JS wire format), `Dataset`, `KeyValueStore`, `RequestQueue`, the 4-trait `StorageBackend` contract, the file-system and in-memory backends, request-scoped `StorageTransaction`, `Configuration` (`CRAWLEE_*` variables, `crawlee.json`), `RequestLoader` / `RequestManagerTandem`, `EventManager`, `RecoverableState`, system info (cgroup-aware), `Services` |
| `crawlee-http-client` | `@crawlee/http-client` | The `HttpClient` / `Transport` traits, the shared redirect and cookie logic, a reqwest transport |
| `crawlee-basic` | `@crawlee/basic` | `BasicCrawler`: task loop, retries and error classification, sessions and proxies, router, statistics, `enqueue_urls` filtering, the typed context pipeline, autoscaling (`ConcurrencySystem`, `SystemStatus`, load signals), robots.txt, `Sitemap` / `SitemapRequestLoader`, `ThrottlingRequestManager` |
| `crawlee-http` | `@crawlee/http`, `@crawlee/cheerio` | `HttpCrawler` (typed JSON via serde), `HtmlCrawler` (lazy DOM, streaming `enqueue_links`), body decoding |
| `crawlee-impit` ([integrations/](integrations/crawlee-impit)) | `@crawlee/impit-client` | A transport with browser TLS and HTTP/2 fingerprints. It is kept outside the workspace because impit needs patched dependencies; see its [README](integrations/crawlee-impit/README.md) |

## Storage

Storage works as in Crawlee for JS. By default, crawlers write to `./storage`:

```text
storage/
  datasets/default/000000001.json, ...
  key_value_stores/default/<key>, <key>.__metadata__.json
  request_queues/default/<hash>.json
```

- **Shared format.** The format comes from
  [crawlee-storage](https://github.com/apify/crawlee-storage), the Rust core behind the
  file-system storage of Crawlee for JS and Python. Each of the three reads what the others wrote.
- **Purging.** When the first crawler of a process starts, the default storages and the
  unnamed (aliased) ones are emptied. Named storages are kept.
- **Restarts.** A request queue keeps its handled requests across runs. With
  `CRAWLEE_PURGE_ON_START=false`, a crawl resumes where it stopped.
- **Settings:** `CRAWLEE_STORAGE_DIR`, `CRAWLEE_PURGE_ON_START`, `CRAWLEE_PERSIST_STORAGE`
  (`false` keeps everything in memory), or the same keys in `crawlee.json`.
- **In code:** `Services::from_configuration(...)`, or `Services::in_memory()` for tests.
- **One instance per storage.** `Services::open_dataset` (and the other `open_*`) returns the
  instance already open for the same id, name or alias, as `StorageInstanceManager` does in JS.
  `Dataset::open(backend, id)` and the like open a new handle without that cache.

## Events and saved state

`Services` also holds the `EventManager`, as in JS. It emits:
- `PersistState` every 60 s and at the end of a run;
- `SystemInfo` every second: CPU and memory, read from cgroups in a container;
- `StatusMessage`.

`Migrating` and `Aborting` pause the crawl and save the statistics; the Apify SDK will emit
them. On `PersistState`, crawlers save their state to the default key-value store, under the
keys and in the record formats of Crawlee for JS:

| Key | What |
|---|---|
| `CRAWLEE_CRAWLER_STATISTICS_{id}` | Statistics. A resumed crawl continues its counters and runtime |
| `CRAWLEE_SESSION_POOL_STATE_{id}` | Sessions, with their cookies in tough-cookie's JSON |
| `CRAWLEE_STATE` / `CRAWLEE_STATE_{id}` | `ctx.use_state(default)` / `crawler.use_state(default)` |

`RecoverableState` is the building block for all three, and is public for custom state.

## Politeness: robots.txt, rate limits and sitemaps

- **robots.txt.** `.respect_robots_txt(true)` (or `.robots_txt_user_agent("MyBot")`) skips
  requests that robots.txt disallows. They are checked when enqueued and before they are fetched.
  robots.txt is cached per origin; the matching is a port of `robots-parser`.
- **Rate limits.** `ThrottlingRequestManager` keeps a request queue per domain. When a domain
  answers 429, its requests are held back: for the `Retry-After` delay, or with an exponential
  backoff of 2 s, 4 s, … up to 60 s.
  - The request is retried without spending a retry or blaming its session.
  - robots.txt `Crawl-delay` is honoured as a minimum interval between requests to the domain.
  - `.same_domain_delay(d)` sets such an interval for every site, and adds a throttling manager
    when the crawler has none.
- **Sitemaps.** `Sitemap::load` fetches sitemaps, nested and gzipped ones included, and returns
  their pages.
  - `SitemapRequestLoader` crawls them in the background. Put it in front of a request queue with
    `RequestManagerTandem`.
  - Its progress is saved, so a resumed crawl continues where it stopped.

## Design notes

The goal was to keep Crawlee v4's architecture and change it only where Rust needs a different design.

- **Context pipeline.** v4's `ContextPipeline` merges property bags at runtime. Here it is a typed
  chain of middlewares (`Middleware<In>`, composed with `Then`):
  `BasicContext → HttpContext → HtmlContext`. Each layer derefs to the one below, so
  `ctx.push_data`, `ctx.request()` and `ctx.url()` all work on an `HtmlContext`.
- **Handlers take the context by value.** They are plain `|ctx| async move { ... }` closures, with
  no lifetimes and `Send + 'static` futures.
  - The context owns its `Request` and hands it back to the crawler when dropped. Changes the
    handler (or `error_handler`) makes to the request are therefore kept for the retry, as in JS.
  - This works even when the handler times out or panics.
- **Transactions.** Storage writes made through the context are journaled and applied only when
  the handler succeeds, as in v4.
  - Items are serialized once, when pushed, into `RawValue` bytes. There is no `structuredClone`
    step, and a backend (such as a future Apify one) can send those bytes as they are.
- **HTML without `unsafe`.**
  - `scraper` is built with atomic tendrils, so the DOM is `Send`. `with_html` can parse it once
    on the blocking pool and cache it on the context.
  - Handlers stay `Send`, and parsing runs in parallel on every core without stalling the I/O
    threads.
- **Links without a DOM.** `enqueue_links` streams the body through `lol_html`. It decodes
  attribute entities with the WHATWG algorithm, using html5ever's entity table, and falls back
  to the DOM only for selectors `lol_html` cannot stream.
- **Zero-copy bodies.** A response body is `Bytes` from the socket to the handler. Valid UTF-8 is
  validated with SIMD and borrowed, not copied.
- **Errors.** Handlers return `anyhow::Result<()>`. To steer retries, return `SessionError`,
  `NonRetryableError`, `RetryRequestError`, `CriticalError` or `RequestThrottledError`; they are
  found anywhere in the error chain. The retry logic ports `requestFunctionErrorHandler`.
- **Autoscaling** ports `ConcurrencySystem` with the same defaults:
  - It runs between `min_concurrency` (1) and `max_concurrency` (200). `HttpCrawler` and
    `HtmlCrawler` start at 10.
  - Every 10 s it scales by 5 % up or down, based on memory, CPU, storage rate limits and runtime
    responsiveness.
  - Runtime responsiveness stands in for the event loop signal of Node. It measures how late a
    timer task fires on the tokio runtime.
  - `fixed_concurrency(n)` pins it, and `max_requests_per_minute` caps the rate.
- **`#![forbid(unsafe_code)]`** holds in every crate. The lint is set at the workspace level.

## Conformance with Crawlee for JS

`conformance/oracle` runs Crawlee for JS **from its sources** as an oracle and writes golden files
to `conformance/golden`. `crates/crawlee/tests/golden.rs` replays all of them against the Rust code.

- **Currently all 755 checks match**, with an empty allow-list. The cases cover:
  - `normalizeUrl` and unique keys;
  - request ids;
  - the `Request` JSON, including `userData.__crawlee`;
  - `tldts` domains;
  - enqueue strategies;
  - link extraction as `CheerioCrawler` does it;
  - the charset prescan;
  - KVS JSON formatting;
  - robots.txt matching (`robots-parser`), sitemap XML parsing and `Retry-After`.
- **Deliberate differences** are listed in
  [conformance/allowed-differences.md](conformance/allowed-differences.md). A golden-file mismatch
  that is not on that list fails CI.

## What's not there yet

These are planned in this order, following `docs/plan.md`:

1. **Remaining request sources and options:**
   - `RequestList`;
   - `extend_context`, `extend_timeout`, `skip_navigation` in `HttpCrawler`;
   - the `stalled` readiness state of throttling.
2. **Side-by-side crawl comparison** (level 3 of the conformance plan): the same crawl run by
   both implementations against the fixture server, comparing their outputs. Today the two are
   compared function by function (golden files) and by benchmark.
3. **Later:** an Apify SDK crate on top of the Rust `apify-client`; browser and adaptive crawlers.

## Development

```sh
cargo test --workspace                       # everything, including the golden files
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check

# Regenerate golden files (needs a crawlee checkout with `pnpm install` done)
CRAWLEE_JS_DIR=../crawlee node --experimental-transform-types --no-warnings conformance/oracle/generate-golden.mts
```

## License

Apache 2.0, like Crawlee.
