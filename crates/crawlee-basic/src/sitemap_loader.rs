//! [`SitemapRequestLoader`]: crawls the pages listed in sitemaps, the counterpart of
//! `SitemapRequestLoader` in Crawlee for JS.
//!
//! - **Loading.** Sitemaps load in the background, nested ones included, `concurrency` at a time
//!   (one by default). Each sitemap is loaded once, even when sitemaps list each other. Pages are
//!   buffered, up to `max_buffer_size` at once, until the crawler takes them.
//! - **Filtering.** `include` / `exclude` patterns and the enqueue strategy decide which pages
//!   are kept.
//! - **Saved state.** The loading progress and the buffered pages are saved under
//!   `SITEMAP_REQUEST_LOADER_STATE` on every `PersistState`, so a resumed crawl continues where
//!   it stopped.
//!
//! Crawl it through a [`RequestManagerTandem`](crawlee_core::RequestManagerTandem) with a request
//! queue, which handles retries:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use crawlee_basic::sitemap_loader::{SitemapRequestLoader, SitemapRequestLoaderOptions};
//! # use crawlee_core::{RequestManagerTandem, Services, StorageIdentifier};
//! # async fn run(services: Services) -> anyhow::Result<()> {
//! let loader = SitemapRequestLoader::open(
//!     SitemapRequestLoaderOptions::new(["https://crawlee.dev/sitemap.xml"]),
//!     &services,
//!     crawlee_http_client::default_client(),
//! )
//! .await?;
//! let queue = Arc::new(services.open_request_queue(&StorageIdentifier::Default).await?);
//! let manager = Arc::new(RequestManagerTandem::new(loader, queue));
//! # Ok(()) }
//! ```

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use indexmap::IndexSet;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crawlee_core::recoverable_state::{BoxError, PersistedState};
use crawlee_core::{LoaderStatus, RecoverableState, Request, RequestLoader, Services, StorageResult};
use crawlee_http_client::HttpClient;
use crawlee_utils::patterns::PatternError;
use crawlee_utils::{EnqueueStrategy, UrlFilter, UrlPattern};

use crate::sitemap::{SitemapOptions, fetch_sitemap};

const STATE_KEY: &str = "SITEMAP_REQUEST_LOADER_STATE";

#[derive(Clone, Debug)]
pub struct SitemapRequestLoaderOptions {
    pub sitemap_urls: Vec<String>,
    /// Pages must match one of these (when there are any).
    pub include: Vec<UrlPattern>,
    /// Pages matching one of these are dropped.
    pub exclude: Vec<UrlPattern>,
    /// Pages buffered at most, waiting for the crawler.
    pub max_buffer_size: usize,
    pub enqueue_strategy: EnqueueStrategy,
    /// Fetching options. Nested sitemaps are always followed, whatever `max_depth` says.
    pub sitemap: SitemapOptions,
    /// Stop loading after this long.
    pub timeout: Option<Duration>,
    /// Sitemaps loaded at once, nested ones included. 1 by default.
    pub concurrency: usize,
    pub persist_state_key: String,
    pub persistence_enabled: bool,
}

impl SitemapRequestLoaderOptions {
    pub fn new<I, S>(sitemap_urls: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        SitemapRequestLoaderOptions {
            sitemap_urls: sitemap_urls.into_iter().map(Into::into).collect(),
            include: Vec::new(),
            exclude: Vec::new(),
            max_buffer_size: 200,
            enqueue_strategy: EnqueueStrategy::SameHostname,
            sitemap: SitemapOptions::default(),
            timeout: None,
            concurrency: 1,
            persist_state_key: STATE_KEY.to_owned(),
            persistence_enabled: true,
        }
    }
}

/// The saved record, in the shape Crawlee for JS writes. Fields JS restores leniently
/// (`new Set(undefined)`, `undefined` flags) default when missing.
#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct Progress {
    pending_sitemap_urls: Vec<String>,
    in_progress_sitemap_urls: Vec<String>,
    /// The pages each sitemap being loaded has buffered already.
    in_progress_entries_per_sitemap: BTreeMap<String, Vec<String>>,
    /// `visitedSitemapUrls` in records of earlier versions of crawlee-rs.
    #[serde(alias = "visitedSitemapUrls")]
    processed_sitemap_urls: Vec<String>,
    /// The record of JS before parallel loading, which loaded one sitemap at a time; only read.
    #[serde(skip_serializing)]
    in_progress_sitemap_url: Option<String>,
    #[serde(skip_serializing)]
    in_progress_entries: Vec<String>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    sitemap_parsing_progress: Progress,
    url_queue: Vec<String>,
    #[serde(default)]
    request_data: Vec<(String, Request)>,
    #[serde(default)]
    abort_loading: bool,
    #[serde(default)]
    closed: bool,
}

#[derive(Default)]
struct LoaderState {
    queue: VecDeque<String>,
    in_progress: IndexSet<String>,
    request_data: HashMap<String, Request>,
    pending_sitemaps: IndexSet<String>,
    in_progress_sitemaps: IndexSet<String>,
    /// Pages each sitemap being loaded already buffered, so a resumed load does not buffer them
    /// twice.
    in_progress_entries: HashMap<String, HashSet<String>>,
    /// Sitemaps loaded (normalized), so that a nested sitemap listed again, as in a cycle, is not
    /// loaded again.
    processed_sitemaps: HashSet<String>,
    abort_loading: bool,
    closed: bool,
    handled: u64,
}

impl LoaderState {
    fn fully_loaded(&self) -> bool {
        self.in_progress_sitemaps.is_empty() && self.pending_sitemaps.is_empty()
    }

    /// Whether a nested sitemap is loaded, being loaded or waiting to be.
    fn knows_sitemap(&self, url: &str) -> bool {
        let normalized = normalize_url(url);
        self.processed_sitemaps.contains(&normalized)
            || self.pending_sitemaps.contains(url)
            || self.in_progress_sitemaps.iter().any(|sitemap| normalize_url(sitemap) == normalized)
    }

    /// Whether a sitemap being loaded has buffered the page already.
    fn emitted(&self, page: &str) -> bool {
        self.in_progress_entries.values().any(|entries| entries.contains(page))
    }
}

struct Shared {
    state: Mutex<LoaderState>,
    /// Woken when the buffer has room or the loader closes.
    space: Notify,
    filter: UrlFilter,
    strategy: EnqueueStrategy,
    max_buffer_size: usize,
    initial_sitemaps: Vec<String>,
    concurrency: usize,
}

impl PersistedState for Shared {
    fn to_record(&self) -> Value {
        let state = self.state.lock();
        let record = Record {
            sitemap_parsing_progress: Progress {
                pending_sitemap_urls: state.pending_sitemaps.iter().cloned().collect(),
                in_progress_sitemap_urls: state.in_progress_sitemaps.iter().cloned().collect(),
                in_progress_entries_per_sitemap: state
                    .in_progress_entries
                    .iter()
                    .map(|(sitemap, entries)| (sitemap.clone(), entries.iter().cloned().collect()))
                    .collect(),
                processed_sitemap_urls: state.processed_sitemaps.iter().cloned().collect(),
                ..Progress::default()
            },
            // In-progress pages go back to the queue on restore: they may not have finished.
            url_queue: state.in_progress.iter().chain(&state.queue).cloned().collect(),
            request_data: state.request_data.iter().map(|(url, request)| (url.clone(), request.clone())).collect(),
            abort_loading: state.abort_loading,
            closed: state.closed,
        };
        serde_json::to_value(record).unwrap_or_default()
    }

    fn restore(&self, record: Value) -> Result<(), BoxError> {
        let record: Record = serde_json::from_value(record)?;
        let progress = record.sitemap_parsing_progress;
        let mut in_progress_entries: HashMap<String, HashSet<String>> = progress
            .in_progress_entries_per_sitemap
            .into_iter()
            .map(|(sitemap, entries)| (sitemap, entries.into_iter().collect()))
            .collect();
        let mut interrupted = progress.in_progress_sitemap_urls;
        // A record of JS before parallel loading.
        if let Some(sitemap) = progress.in_progress_sitemap_url
            && interrupted.is_empty()
        {
            in_progress_entries.entry(sitemap.clone()).or_default().extend(progress.in_progress_entries);
            interrupted.push(sitemap);
        }
        // Interrupted sitemaps are loaded again; their buffered pages are skipped.
        let mut pending_sitemaps: IndexSet<String> = progress.pending_sitemap_urls.into_iter().collect();
        pending_sitemaps.extend(interrupted);

        let mut state = self.state.lock();
        *state = LoaderState {
            queue: record.url_queue.into(),
            request_data: record.request_data.into_iter().collect(),
            pending_sitemaps,
            in_progress_entries,
            processed_sitemaps: progress.processed_sitemap_urls.into_iter().collect(),
            abort_loading: record.abort_loading,
            closed: record.closed,
            ..LoaderState::default()
        };
        Ok(())
    }

    fn reset(&self) {
        *self.state.lock() =
            LoaderState { pending_sitemaps: self.initial_sitemaps.iter().cloned().collect(), ..LoaderState::default() };
    }
}

impl Shared {
    /// Buffers a page, waiting while the buffer is full.
    async fn push_url(&self, url: String) {
        if !self.filter.is_allowed(&url) {
            return;
        }
        loop {
            let space = self.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            {
                let mut state = self.state.lock();
                if state.closed {
                    return;
                }
                if state.queue.len() < self.max_buffer_size {
                    state.queue.push_back(url);
                    return;
                }
            }
            space.await;
        }
    }

    /// Loads the sitemaps, `concurrency` at a time, until none is left or loading is aborted.
    async fn load(&self, client: Arc<dyn HttpClient>, options: SitemapOptions) {
        let options = SitemapOptions { max_depth: Some(0), enqueue_strategy: self.strategy, ..options };
        let mut active = FuturesUnordered::new();
        loop {
            while active.len() < self.concurrency {
                let sitemap = {
                    let mut state = self.state.lock();
                    if state.abort_loading {
                        break;
                    }
                    let Some(sitemap) = state.pending_sitemaps.shift_remove_index(0) else { break };
                    state.in_progress_sitemaps.insert(sitemap.clone());
                    state.in_progress_entries.entry(sitemap.clone()).or_default();
                    sitemap
                };
                active.push(self.load_sitemap(sitemap, client.as_ref(), &options));
            }
            if active.next().await.is_none() {
                break;
            }
        }
    }

    /// Loads one sitemap: queues its nested sitemaps and buffers its pages.
    async fn load_sitemap(&self, sitemap: String, client: &dyn HttpClient, options: &SitemapOptions) {
        if let Some(contents) = fetch_sitemap(&sitemap, client, options).await {
            {
                let mut state = self.state.lock();
                for nested in contents.nested_sitemaps {
                    if !state.knows_sitemap(&nested) {
                        state.pending_sitemaps.insert(nested);
                    }
                }
            }
            for page in contents.pages {
                {
                    let state = self.state.lock();
                    // Left in progress, so that the loader keeps reporting it is not fully loaded.
                    if state.abort_loading {
                        return;
                    }
                    if state.emitted(&page.loc) {
                        continue;
                    }
                }
                // Recorded once buffered, as in JS: a state saved while the buffer is full does not
                // count the page as buffered, so a resumed load buffers it.
                self.push_url(page.loc.clone()).await;
                if let Some(entries) = self.state.lock().in_progress_entries.get_mut(&sitemap) {
                    entries.insert(page.loc);
                }
            }
        } else {
            tracing::error!("Error loading sitemap contents from {sitemap}");
        }

        let mut state = self.state.lock();
        state.in_progress_sitemaps.shift_remove(&sitemap);
        state.pending_sitemaps.shift_remove(&sitemap);
        state.in_progress_entries.remove(&sitemap);
        state.processed_sitemaps.insert(normalize_url(&sitemap));
    }
}

/// The URL as a loaded sitemap is remembered by, as `Sitemap::load_entries` does.
fn normalize_url(url: &str) -> String {
    url::Url::parse(url).map_or_else(|_| url.to_owned(), |url| url.to_string())
}

/// Crawls the pages of sitemaps; see the [module documentation](self).
pub struct SitemapRequestLoader {
    shared: Arc<Shared>,
    persistence: RecoverableState<Shared>,
    tasks: Vec<JoinHandle<()>>,
}

impl SitemapRequestLoader {
    /// Restores the saved state (unless the storages were purged on start) and starts loading.
    pub async fn open(
        options: SitemapRequestLoaderOptions,
        services: &Services,
        client: Arc<dyn HttpClient>,
    ) -> Result<Arc<Self>, SitemapLoaderError> {
        let shared = Arc::new(Shared {
            state: Mutex::new(LoaderState::default()),
            space: Notify::new(),
            filter: UrlFilter::new(&options.include, &options.exclude)?,
            strategy: options.enqueue_strategy,
            max_buffer_size: options.max_buffer_size.max(1),
            initial_sitemaps: options.sitemap_urls.clone(),
            concurrency: options.concurrency.max(1),
        });
        shared.reset();

        services.purge_on_start().await?;
        let persistence = RecoverableState::new(
            services,
            options.persist_state_key.clone(),
            shared.clone(),
            options.persistence_enabled,
        );
        persistence.initialize().await?;

        let mut tasks = Vec::new();
        let loader = shared.clone();
        tasks.push(tokio::spawn(async move { loader.load(client, options.sitemap).await }));
        if let Some(timeout) = options.timeout {
            let shared = shared.clone();
            tasks.push(tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                shared.state.lock().abort_loading = true;
            }));
        }
        Ok(Arc::new(SitemapRequestLoader { shared, persistence, tasks }))
    }

    /// Whether every sitemap has been loaded.
    pub fn is_sitemap_fully_loaded(&self) -> bool {
        self.shared.state.lock().fully_loaded()
    }

    /// Stops loading and saves the state.
    pub async fn teardown(&self) {
        {
            let mut state = self.shared.state.lock();
            state.closed = true;
            state.abort_loading = true;
        }
        self.shared.space.notify_waiters();
        self.persistence.teardown().await;
    }
}

impl Drop for SitemapRequestLoader {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SitemapLoaderError {
    #[error(transparent)]
    Pattern(#[from] PatternError),
    #[error(transparent)]
    Storage(#[from] crawlee_core::StorageError),
}

#[async_trait]
impl RequestLoader for SitemapRequestLoader {
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        let request = {
            let mut state = self.shared.state.lock();
            if state.closed {
                return Ok(None);
            }
            let Some(url) = state.queue.pop_front() else { return Ok(None) };
            let strategy = self.shared.strategy;
            let request = match state.request_data.get(&url) {
                Some(request) => request.clone(),
                None => match Request::builder(url.clone()).enqueue_strategy(strategy).build() {
                    Ok(request) => request,
                    Err(err) => {
                        tracing::warn!("Skipping sitemap URL {url}: {err}");
                        return Ok(None);
                    }
                },
            };
            state.request_data.insert(url.clone(), request.clone());
            state.in_progress.insert(url);
            request
        };
        self.shared.space.notify_waiters();
        Ok(Some(request))
    }

    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<()> {
        let mut state = self.shared.state.lock();
        state.handled += 1;
        state.in_progress.shift_remove(&request.url);
        state.request_data.remove(&request.url);
        Ok(())
    }

    async fn status(&self) -> StorageResult<LoaderStatus> {
        let state = self.shared.state.lock();
        Ok(if !state.queue.is_empty() && !state.closed {
            LoaderStatus::Ready
        } else if !state.fully_loaded() && !state.abort_loading {
            LoaderStatus::Waiting
        } else if state.in_progress.is_empty() {
            LoaderStatus::Finished
        } else {
            LoaderStatus::Waiting
        })
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        Ok(self.shared.state.lock().handled)
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        let state = self.shared.state.lock();
        Ok((state.queue.len() + state.in_progress.len()) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> Shared {
        let shared = Shared {
            state: Mutex::new(LoaderState::default()),
            space: Notify::new(),
            filter: UrlFilter::new(&[], &[]).unwrap(),
            strategy: EnqueueStrategy::SameHostname,
            max_buffer_size: 200,
            initial_sitemaps: vec!["https://a.test/sitemap.xml".into()],
            concurrency: 1,
        };
        shared.reset();
        shared
    }

    #[test]
    fn record_has_the_js_shape_and_round_trips() {
        let original = shared();
        {
            let mut state = original.state.lock();
            state.queue.push_back("https://a.test/2".into());
            state.in_progress.insert("https://a.test/1".into());
            state.request_data.insert("https://a.test/1".into(), Request::new("https://a.test/1"));
            state.processed_sitemaps.insert("https://a.test/index.xml".into());
            state.pending_sitemaps.shift_remove("https://a.test/sitemap.xml");
            state.in_progress_sitemaps.insert("https://a.test/sitemap.xml".into());
            state
                .in_progress_entries
                .entry("https://a.test/sitemap.xml".into())
                .or_default()
                .insert("https://a.test/0".into());
        }
        let record = original.to_record();
        let keys: Vec<&str> = record.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, ["sitemapParsingProgress", "urlQueue", "requestData", "abortLoading", "closed"]);
        assert_eq!(record["urlQueue"], serde_json::json!(["https://a.test/1", "https://a.test/2"]));
        assert_eq!(
            record["sitemapParsingProgress"],
            serde_json::json!({
                "pendingSitemapUrls": [],
                "inProgressSitemapUrls": ["https://a.test/sitemap.xml"],
                "inProgressEntriesPerSitemap": { "https://a.test/sitemap.xml": ["https://a.test/0"] },
                "processedSitemapUrls": ["https://a.test/index.xml"],
            })
        );

        let restored = shared();
        restored.restore(record).unwrap();
        let state = restored.state.lock();
        assert_eq!(state.queue, ["https://a.test/1", "https://a.test/2"], "in-progress pages are queued again");
        assert!(state.request_data.contains_key("https://a.test/1"));
        assert!(state.processed_sitemaps.contains("https://a.test/index.xml"), "loaded sitemaps are not loaded again");
        assert_eq!(
            state.pending_sitemaps.iter().collect::<Vec<_>>(),
            ["https://a.test/sitemap.xml"],
            "an interrupted sitemap is loaded again"
        );
        assert!(state.in_progress_sitemaps.is_empty());
        assert!(state.emitted("https://a.test/0"), "without buffering its pages again");
        assert!(!state.fully_loaded());
    }

    /// The record of JS before parallel loading (crawlee#3571), and of earlier crawlee-rs versions.
    #[test]
    fn restores_a_record_of_one_sitemap_at_a_time() {
        let restored = shared();
        restored
            .restore(serde_json::json!({
                "urlQueue": [],
                "sitemapParsingProgress": {
                    "pendingSitemapUrls": ["https://a.test/2.xml"],
                    "inProgressSitemapUrl": "https://a.test/1.xml",
                    "inProgressEntries": ["https://a.test/page"],
                    "visitedSitemapUrls": ["https://a.test/index.xml"],
                },
            }))
            .unwrap();
        let state = restored.state.lock();
        assert_eq!(state.pending_sitemaps.iter().collect::<Vec<_>>(), ["https://a.test/2.xml", "https://a.test/1.xml"]);
        assert!(state.emitted("https://a.test/page"));
        assert!(state.knows_sitemap("https://a.test/index.xml"));
        assert!(!state.knows_sitemap("https://a.test/3.xml"));
    }

    #[test]
    fn restores_a_record_with_only_the_url_queue() {
        let restored = shared();
        restored
            .restore(serde_json::json!({ "urlQueue": ["https://a.test/1"], "sitemapParsingProgress": {} }))
            .unwrap();
        assert_eq!(restored.state.lock().queue, ["https://a.test/1"]);
    }
}
