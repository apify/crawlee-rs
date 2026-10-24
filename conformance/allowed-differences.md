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
| HTML API | cheerio `$` (jQuery API) | `Document` / `Selection` / `Element` facade over `scraper`, used through `with_html` | No jQuery in Rust; parsing runs off the async threads |
| `errorMessages` | `error.stack` | The error with its cause chain (`{error:#}`) | Rust errors have no JS stack traces |
| Error grouping in statistics | By error code, name, stack path and message placeholders | By first message line with digits masked | Same reason |
| Event loop load signal | Lateness of a Node timer | Lateness of a tokio timer task | There is no single event loop; a starved tokio runtime is the equivalent |
| `maxRequestsPerCrawl` | Can overshoot by in-flight requests (5042 of 5000 in the benchmark) | Never exceeded; what is enqueued is capped as in JS | Running tasks are counted before another one starts |
| Internal timeouts | `internalTimeoutMillis` from `CRAWLEE_INTERNAL_TIMEOUT`, or twice the handler timeout but at least 5 min; the request timeout covers the navigation timeout too | Always twice the handler timeout but at least 5 min; the request timeout is the longer of it and the request's handler timeout | No such setting in `Configuration`, and the pipeline of `BasicCrawler` does not know the navigation timeout of the crawler built on it |
| SIGINT | `process.once('SIGINT')` during a run; outside one, Node's default | The first Ctrl+C during a run pauses and ends it; a second one, or one when no crawler runs, exits the process with status 130 | tokio's handler replaces the default one for the rest of the process, so the default is emulated |
| `statusMessageCallback` | Gets the crawler and the statistics state; `previousState` is the state just read | Gets the message and statistics snapshots; `previous_state` is the one of the previous status message | No typed crawler handle in a callback shared by every pipeline; the JS value is always the current state |
| Transaction capture | Ambient (`AsyncLocalStorage`) | Only writes made through the context | No ambient context in Rust; explicit is clearer |
| Request queue order numbers | `±Date.now()` | A process-wide counter | Same order, deterministic |
| Add dedup cache | Stores keys | Stores 64-bit fingerprints of keys (8 MiB per million) | Memory |
| Default HTTP client | `impit` (browser impersonation) | reqwest (rustls, h2); impit through `integrations/crawlee-impit` | impit needs patched dependencies, which cannot be forced on every user |
| Throttling | `checkReadiness()` can report a domain as `stalled` after `maxDomainStallSecs` of 429s | Not ported: a domain that rate-limits forever is retried forever | The task loop has no readiness states yet |
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
