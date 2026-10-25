# Allowed differences from Crawlee for JS

This file lists every deliberate difference between crawlee-rs and Crawlee for JS.

- **Golden-file cases.** A golden case that behaves differently must be listed in
  `ALLOWED_DIFFERENCES` in `crates/crawlee/tests/golden.rs`, under the id of the case, and
  explained here. The test fails for unlisted mismatches. It also fails for listed cases that
  match again, so the list cannot go stale.
- **Other differences.** Differences that no golden case covers yet are documented below, so that
  reviewers and users know them.

## Golden-file cases

_None._ All 755 checks match.

## API and design differences

| Area | Crawlee for JS | crawlee-rs | Why |
|---|---|---|---|
| Crawling context | Flat object; `extendContext` merges properties | Nested typed contexts (`HtmlContext` → `HttpContext` → `BasicContext` via `Deref`); handlers take the context by value | Rust cannot grow a struct at runtime |
| Error handlers | Receive the (partial) context of the failed attempt | Receive a fresh `BasicContext` for the same request and session, plus `Arc<anyhow::Error>` | The failed attempt's context was consumed by the handler |
| HTML API | cheerio `$` (jQuery API); `querySelectorAll` / `querySelectorOne` return Cheerio objects | `Document` / `Selection` / `Element` facade over `scraper`, used through `with_html`; `query_selector_all` / `query_selector_one` hand the matches to a closure | No jQuery in Rust; parsing runs off the async threads |
| `errorMessages` | `error.stack` | The error with its cause chain (`{error:#}`) | Rust errors have no JS stack traces |
| Error grouping in statistics | By error code, name, stack path and message placeholders | By first message line with digits masked | Same reason |
| Event loop load signal | Lateness of a Node timer | Lateness of a tokio timer task | There is no single event loop; a starved tokio runtime is the equivalent |
| `maxRequestsPerCrawl` | Can overshoot by in-flight requests (5042 of 5000 in the benchmark) | Never exceeded; what is enqueued is capped as in JS | Running tasks are counted before another one starts |
| SIGINT | `process.once('SIGINT')` during a run; outside one, Node's default | The first Ctrl+C during a run pauses and ends it; a second one, or one when no crawler runs, exits the process with status 130 | tokio's handler replaces the default one for the rest of the process, so the default is emulated |
| `statusMessageCallback` | Gets the crawler and the statistics state; `previousState` is the state just read | Gets the message and statistics snapshots; `previous_state` is the one of the previous status message | No typed crawler handle in a callback shared by every pipeline; the JS value is always the current state |
| Transaction capture | Ambient (`AsyncLocalStorage`) | Only writes made through the context | No ambient context in Rust; explicit is clearer |
| Request queue order numbers | `±Date.now()` | A process-wide counter | Same order, deterministic |
| `KeyValueStore.getValue` | One method returning an object, a string or a `Buffer` by content type; JSON parsed with JSON5 | `get_record_value` returns that as a `RecordValue` enum; `get_value::<T>` deserializes `T` by the content type (JSON parsed, text as a string, other records from their bytes; a record without a content type as JSON if it parses); JSON parsed strictly | Rust has no union return type; serde_json is strict JSON |
| `Dataset.exportTo` | Opens the target store from the `toKVS` option | Takes the target `KeyValueStore` as an argument | Storages are opened through explicit `Services`, not an ambient locator |
| `requestsFromUrl` | Downloaded with the `httpClient` option of the queue or `RequestList`, `fetch` by default; `crawler.addRequests()` takes such sources but drops them (no `url`) | Downloaded with a `UrlListFetcher`: the queue's (crawlers set one on their default queue) or the `url_list_fetcher` of a `RequestList` (such as `HttpUrlListFetcher`); without one, they fail. The crawler's `add_requests` takes requests only | crawlee-core has no HTTP client |
| `RequestList` | `RequestList.open(nameOrOptions, sources, options)`; `proxyConfiguration`; `keepDuplicateUrls` leaves sources with a `uniqueKey` property alone; `requestList` with the deprecated `requestQueue` makes a tandem of them | `open(services, name, sources)` and `open_with_options(services, options)`; a `proxy_url` closure; a request counts as having a unique key set when it differs from the one its URL gives; a list and a `request_manager` are combined by the caller, with `to_tandem` | Storages are opened through explicit `Services`; proxies live in crawlee-basic; a `Request` always has a unique key; there is no `request_queue` option |
| Pausing for a migration | Every request queue stops fetching on `MIGRATING` of the global event manager | Queues opened through `Services` (or given `with_events`) stop on `Event::Migrating` of its event manager | No ambient event manager |
| `Dataset.forEach` / `values` | Async iterables (and promises of arrays) | Methods taking async closures, and `Vec`s | No async iterators in stable Rust without extra dependencies |
| Request queue caches | A 1M-slot dedup cache of keys plus a 2M-entry LRU of requests with their handled state; requests added in background batches skip the LRU, so a later add of one reports `wasAlreadyHandled: false` | One 1M-slot cache of 64-bit key fingerprints with a handled bit (8 MiB), kept for every add | Memory |
| Default HTTP client | `impit` (browser impersonation) | reqwest (rustls, h2); impit through `integrations/crawlee-impit` | impit needs patched dependencies, which cannot be forced on every user |
| `createSessionFunction` | Also recreates the sessions restored from the saved pool, from their record | Makes new sessions only; restored sessions are rebuilt from their record (their proxy through `ProxySource::restored_proxy_info`) | A Rust `Session` is not built from an options bag holding its whole saved state |
| Navigation hooks | Async; may return a partial object whose properties are merged into the crawling context | Async; take the context by value and hand it on (`BasicContext`, or `PostNavigationContext` whose `response` can be replaced); they cannot add members | Rust cannot grow a struct at runtime (see Crawling context) |
| `skipNavigation` | The response members of the context are getters that throw `NavigationSkippedError` | Infallible response members (`status()`, `text()`, …) panic with that error's message, which the crawler turns back into the error; fallible ones (`json()`, `with_html()`, `enqueue_links()`, …) return it. Either way the request fails without retries | Rust has no throwing getters, and making every response member fallible would change every handler |
| Error groups in saved statistics | `errors` / `retryErrors` hold the nested `ErrorTracker` tree | A flat `{ group: count }` map | Follows from the error grouping above; JS rebuilds its trackers from scratch on restore anyway |

## Opt-in extras

Features that Crawlee for JS does not have. They are off by default, so that the default
behaviour matches JS.

| Area | Crawlee for JS | crawlee-rs | Why |
|---|---|---|---|
| Buffered dataset writes | Every `pushData` is one write | Off by default, as in JS. With `CRAWLEE_DATASET_BUFFER=1` (or `Configuration::dataset_buffer`), pushes are collected and stored together, after the first few, with a linger that grows by stage; a request is marked as handled only after its items are stored, and retried if they cannot be (see the README) | Fewer API calls and charge calls on the Apify platform |

## Behavior differences not yet covered by golden cases

- **Parsing malformed HTML (DOM queries).**
  - `with_html` parses with html5ever, which follows the HTML5 spec like browsers and parse5.
    `CheerioCrawler` parses with htmlparser2, which does not follow the spec. Their trees differ
    on malformed markup, and `<template>` contents are not part of the scraper DOM.
  - Streaming link extraction does match htmlparser2 on every golden case. That includes
    `<noscript>` links, which are re-scanned for this reason.
- **Globs.**
  - Matched by `globset`, not `minimatch`, in both cases case-insensitively.
  - A `*` also matches segments that start with a dot, which `minimatch` does not do by default.
- **Regular expressions** use the `regex` crate syntax: no look-around and no backreferences.
- **Encoding labels.**
  - Labels are resolved with the WHATWG Encoding Standard, as browsers do. Crawlee for JS uses
    iconv-lite.
  - The difference that matters: `latin1` and `iso-8859-1` mean windows-1252 here.
- **JSON.**
  - Integers beyond 2^53 are kept exact; JS rounds them.
  - Lone UTF-16 surrogates in `\u` escapes are rejected by serde_json.
  - Object keys keep insertion order (`preserve_order`). JS moves integer-like keys first.
- **Malformed sitemaps.** A sitemap is parsed whole once fetched. A syntax error discards it and
  is retried like a failed fetch. JS streams it: it yields the entries before the error, then the
  error ends the whole `Sitemap.load`.
- **Public Suffix List version.** Domains are computed with the `psl` crate's built-in list and
  `tldts`'s default ICANN-only mode is emulated. The two list snapshots can differ for newly added
  suffixes.
