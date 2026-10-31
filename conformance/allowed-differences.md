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
| CPU load signal and start window | The CPU signal is overloaded above 40 % of the window (`CpuLoadSignalOptions`), and a request starts only if every signal was idle over the last 5 s (`currentHistorySecs`) | 95 % and 20 s. The start window applies to every signal, so a memory, runtime or storage overload now holds back request starts once it lasted its ratio of 20 s, not of 5 s. Set `overloaded_ratio: 0.4` and `current_history: 5 s` for the JS behavior | The Apify platform flags `isCpuOverloaded` once a run uses about 96 % of its CPU share, which a run near its best throughput does 74–95 % of the time. With 0.4 over 5 s, requests started in stop-and-go and the concurrency stayed far below the best throughput. Over 24 runs of the Instagram profile scraper at 1–4 GB, 0.95 with 20 s was the fastest setting, about as fast as no CPU signal with half the concurrency |
| Scaling-only load signals (extra) | Every load signal both holds back request starts (5 s window) and drives scaling (30 s window) | Same by default; a `LoadSignal` whose `gates_task_start()` returns `false` only drives scaling. The current status still lists it, with `LoadSignalInfo::gates_task_start` set to `false`, and the state log marks it `(scaling only)` | Some signals, such as throughput per added concurrency, are only meaningful over the longer window; gating request starts on them makes the crawl stop and go |
| Final saves of a run | The statistics, then the session pool, then the other `persistState` listeners, one after the other; then the final status message (`Finished! Total ...`) | All at once, with the final status message (from the same final statistics) | They write different records; one after the other, a run's end waited for each write's round trip in turn |
| Statistics key (extra) | `CRAWLEE_CRAWLER_STATISTICS_{id}` (`SDK_CRAWLER_STATISTICS_{id}` in Crawlee for JS 3) | The same by default; `Statistics::with_persist_state_key` saves them under another key and resumes from it | A scraper ported from Crawlee for JS 3 can keep its key with a single write |
| Post-quantum key exchange (extra) | Crawlee for JS has no impit transport; got-scraping's Chrome profile offers X25519, P-256 and P-384 | impit's Chrome fingerprints offer the hybrid X25519MLKEM768 first, as Chrome does; `ImpitTransport::without_post_quantum_key_exchange` leaves it out | The hybrid's key share is 1,216 bytes of every ClientHello (and about 1.1 KB of the server's answer); through a proxy billed by the byte, that is a few percent of a small response |
| Saving unchanged state | `RecoverableState` writes its record on every `persistState` event | A record equal to the one last saved (or restored) is not written again, on events, on teardown and on `persist()` | Most state does not change between two events once a crawl idles or ends; each write is a key-value store write billed on the platform. Saves are also serialized, so an older record can no longer overwrite a newer one |
| State saves on a migration or abort | The state is saved as the migration or abort starts (the Apify SDK's `persistState`), while the crawler waits for its running requests; only the statistics are saved again after them | The crawler waits for its running requests (at most 20 s), then emits `PersistState`, so every state (statistics, sessions, a request list, `use_state`, ...) is saved after them, all at once; the Apify SDK's exit and reboot wait for these saves | A request that finished during the wait stayed in progress in a saved request list, and a resurrected or migrated run crawled it again, with its item already stored. Saving all state the same way also keeps the other states in step with what was stored |
| State save interval (extra) | State is saved on `persistState` events only (every 60 s by default) | The same by default; `RecoverableState::persist_every` and `RequestListOptions::persist_state_interval` also save every interval, when the state changed | A request list saved every few seconds crawls fewer requests again after a crash, and costs nothing while nothing changes |
| Request list beside the queue (extra) | A `RequestList` is crawled through a `RequestManagerTandem`: each request is added to the front of the queue when fetched, then handled there | The same with `to_tandem`; `RequestList::with_queue` gives a `RequestListAndQueue`, in which the list serves and tracks its own requests. Its saved state is JS's (`nextIndex`, `nextUniqueKey`, `inProgress`) plus `retries`, the retry counts of the requests in progress that failed before. The queue is served first, then the list. A failed request stays in the list and is retried before its next request, unless a handler changed its data: that one is added to the front of the queue. A request added during the run with the unique key of a list request is not added, and reported as already present | The tandem costs two queue writes per list request (the add and the handled mark), which a list of inputs does not need. In a tandem, every list request goes to the front of the queue, so the requests a run adds and its retries wait behind the whole list, and pile up in a large one. A JS tandem deduplicates such a request in the queue too, but keeps whichever copy reached the queue first; here the list's copy always wins |
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
