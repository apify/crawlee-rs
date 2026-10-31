//! [`RequestList`]: a static list of requests to crawl, the counterpart of `RequestList` in
//! Crawlee for JS.
//!
//! The list is read-only: it is a [`RequestLoader`]. A crawler reads it through a
//! [`RequestManagerTandem`] with a request queue, which takes the retries and the new links, or
//! through a [`RequestListAndQueue`], in which the list keeps its own requests and their retries.
//!
//! Its crawling position (the next request, and the requests in progress) can be saved on every
//! `PersistState` event under `persist_state_key`, and its requests once under
//! `persist_requests_key`, so that a restarted crawl continues where it left off, even when the
//! sources (such as a remote list of URLs) have changed meanwhile.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use indexmap::IndexSet;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::backend::StorageIdentifier;
use super::list_and_queue::RequestListAndQueue;
use super::request_loader::{LoaderStatus, RequestLoader, RequestManagerTandem};
use super::request_queue::{RequestManager, RequestsFromUrl, UrlListFetcher};
use crate::errors::{StateValidationError, StorageError, StorageResult};
use crate::recoverable_state::{BoxError, PersistedState, RecoverableState};
use crate::request::{CrawleeRequestData, Request, compute_unique_key};
use crate::services::Services;

/// The key the state is saved under, after the list name (`STATE_PERSISTENCE_KEY`).
pub const STATE_PERSISTENCE_KEY: &str = "REQUEST_LIST_STATE";
/// The key the requests are saved under, after the list name (`REQUESTS_PERSISTENCE_KEY`).
pub const REQUESTS_PERSISTENCE_KEY: &str = "REQUEST_LIST_REQUESTS";

const CONTENT_TYPE_BINARY: &str = "application/octet-stream";

/// A source of a [`RequestList`] (`RequestListSource`): a URL, a request, or a remote list of
/// URLs.
#[derive(Clone, Debug)]
pub enum RequestListSource {
    Url(String),
    Request(Box<Request>),
    /// Downloaded when the list is opened, with the list's
    /// [`url_list_fetcher`](RequestListOptions::url_list_fetcher).
    RequestsFromUrl(RequestsFromUrl),
}

impl From<&str> for RequestListSource {
    fn from(url: &str) -> Self {
        RequestListSource::Url(url.to_owned())
    }
}

impl From<String> for RequestListSource {
    fn from(url: String) -> Self {
        RequestListSource::Url(url)
    }
}

impl From<url::Url> for RequestListSource {
    fn from(url: url::Url) -> Self {
        RequestListSource::Url(url.into())
    }
}

impl From<Request> for RequestListSource {
    fn from(request: Request) -> Self {
        RequestListSource::Request(Box::new(request))
    }
}

impl From<RequestsFromUrl> for RequestListSource {
    fn from(source: RequestsFromUrl) -> Self {
        RequestListSource::RequestsFromUrl(source)
    }
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// See [`RequestListOptions::sources_function`].
pub type SourcesFunction = Arc<dyn Fn() -> BoxFuture<Result<Vec<RequestListSource>, BoxError>> + Send + Sync>;

/// See [`RequestListOptions::proxy_url`].
pub type ProxyUrlFunction = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// The crawling position of a [`RequestList`], as saved (`RequestListState`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestListState {
    /// Position of the next request to be processed.
    pub next_index: usize,
    /// Unique key of the next request to be processed.
    pub next_unique_key: Option<String>,
    /// Unique keys of the requests being processed.
    pub in_progress: Vec<String>,
    /// The retry counts of the requests in progress that failed before, in a
    /// [`RequestListAndQueue`] (an extra of crawlee-rs, which JS ignores).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub retries: BTreeMap<String, u32>,
}

/// Options of [`RequestList::open_with_options`] (`RequestListOptions`).
#[derive(Clone, Default)]
pub struct RequestListOptions {
    /// The URLs, requests and remote lists of URLs, in order.
    pub sources: Vec<RequestListSource>,
    /// Called for more sources, added after [`sources`](Self::sources), only when the requests
    /// were not restored from [`persist_requests_key`](Self::persist_requests_key). Remote lists
    /// are not downloaded from what it returns.
    pub sources_function: Option<SourcesFunction>,
    /// Save the crawling position under `CRAWLEE_<key>` in the default key-value store on every
    /// `PersistState` event, and continue from it when the list is opened again.
    pub persist_state_key: Option<String>,
    /// Save the requests under `CRAWLEE_<key>` when the list is opened, and open it from them the
    /// next time instead of from the sources, which may have changed.
    pub persist_requests_key: Option<String>,
    /// Where to start when no saved state is restored.
    pub state: Option<RequestListState>,
    /// With a [`persist_state_key`](Self::persist_state_key), also save the crawling position
    /// every `persist_state_interval` when it changed, not only on `PersistState` events (an extra
    /// of crawlee-rs). A shorter interval means fewer requests crawled again after a crash.
    pub persist_state_interval: Option<Duration>,
    /// Keep requests with the same URL: a unique key not set explicitly gets the position of the
    /// request appended. A request counts as having one set explicitly when its unique key is not
    /// the one its URL gives.
    pub keep_duplicate_urls: bool,
    /// Downloads the [`RequestListSource::RequestsFromUrl`] sources (`httpClient`).
    pub url_list_fetcher: Option<Arc<dyn UrlListFetcher>>,
    /// The proxy to download the [`RequestListSource::RequestsFromUrl`] sources through
    /// (`proxyConfiguration`), such as `ProxyConfiguration::new_url` of `crawlee-basic`.
    pub proxy_url: Option<ProxyUrlFunction>,
}

impl std::fmt::Debug for RequestListOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestListOptions")
            .field("sources", &self.sources)
            .field("persist_state_key", &self.persist_state_key)
            .field("persist_requests_key", &self.persist_requests_key)
            .field("state", &self.state)
            .field("persist_state_interval", &self.persist_state_interval)
            .field("keep_duplicate_urls", &self.keep_duplicate_urls)
            .finish_non_exhaustive()
    }
}

impl RequestListOptions {
    /// Sets [`sources_function`](Self::sources_function).
    pub fn sources_function<F, Fut>(mut self, function: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<RequestListSource>, BoxError>> + Send + 'static,
    {
        self.sources_function = Some(Arc::new(move || Box::pin(function())));
        self
    }

    /// Sets [`proxy_url`](Self::proxy_url).
    pub fn proxy_url(mut self, proxy_url: impl Fn() -> Option<String> + Send + Sync + 'static) -> Self {
        self.proxy_url = Some(Arc::new(proxy_url));
        self
    }
}

/// The live crawling position.
#[derive(Clone, Debug, Default)]
struct Position {
    next_index: usize,
    /// In the order they were fetched, like the `Set` of JS.
    in_progress: IndexSet<String>,
    /// The retry counts of the requests in progress that failed before.
    retries: BTreeMap<String, u32>,
    /// Requests in progress being handed over to the queue of a [`RequestListAndQueue`]: saved
    /// as in progress until the queue has them, but no longer the list's.
    moving: HashSet<String>,
}

/// The requests and the crawling position of a list: what its [`RecoverableState`] saves.
struct ListState {
    requests: Vec<Request>,
    index: HashMap<String, usize>,
    position: Mutex<Position>,
    /// Where the list starts when no saved state is restored.
    initial: Position,
}

impl ListState {
    fn encode(&self, position: &Position) -> RequestListState {
        RequestListState {
            next_index: position.next_index,
            next_unique_key: self.requests.get(position.next_index).map(|request| request.unique_key.clone()),
            in_progress: position.in_progress.iter().cloned().collect(),
            retries: position.retries.clone(),
        }
    }

    /// Checks a saved state against the requests (the decoding of the state codec in JS).
    fn decode(&self, record: RequestListState) -> Result<Position, BoxError> {
        let RequestListState { next_index, next_unique_key, in_progress, mut retries } = record;
        if next_index > self.requests.len() {
            return Err("The state object is not consistent with RequestList, too few requests loaded.".into());
        }
        if next_index < self.requests.len() && Some(&self.requests[next_index].unique_key) != next_unique_key.as_ref() {
            return Err(
                "The state object is not consistent with RequestList the order of URLs seems to have changed.".into()
            );
        }
        let mut in_progress: IndexSet<String> = in_progress.into_iter().collect();
        let mut delete_from_in_progress = Vec::new();
        for unique_key in &in_progress {
            match self.index.get(unique_key) {
                None => {
                    return Err(
                        "The state object is not consistent with RequestList. Unknown uniqueKey is present in \
                                the state."
                            .into(),
                    );
                }
                Some(&index) if index >= next_index => delete_from_in_progress.push(unique_key.clone()),
                Some(_) => {}
            }
        }
        // Requests at or after `next_index` will be crawled again anyway; marking them as handled
        // later would fail, as they are not in progress.
        if !delete_from_in_progress.is_empty() {
            tracing::warn!(
                ?delete_from_in_progress,
                "RequestList's in-progress field is not consistent, skipping invalid in-progress entries"
            );
            for unique_key in &delete_from_in_progress {
                in_progress.shift_remove(unique_key);
            }
        }
        retries.retain(|unique_key, _| in_progress.contains(unique_key));
        Ok(Position { next_index, in_progress, retries, moving: HashSet::new() })
    }
}

impl PersistedState for ListState {
    fn to_record(&self) -> Value {
        serde_json::to_value(self.encode(&self.position.lock())).unwrap_or(Value::Null)
    }

    fn restore(&self, record: Value) -> Result<(), BoxError> {
        let position = self.decode(serde_json::from_value(record)?)?;
        *self.position.lock() = position;
        Ok(())
    }

    fn reset(&self) {
        *self.position.lock() = self.initial.clone();
    }
}

/// Requests and their unique keys, as they are loaded.
#[derive(Default)]
struct Loader {
    requests: Vec<Request>,
    index: HashMap<String, usize>,
    keep_duplicate_urls: bool,
}

impl Loader {
    /// Adds a request unless one with its unique key is already there (`#addRequest`).
    fn add(&mut self, mut request: Request, has_unique_key: bool) -> StorageResult<()> {
        if self.keep_duplicate_urls && !has_unique_key {
            request.unique_key = format!("{}-{}", request.unique_key, self.requests.len());
        }
        ensure_unique_key_valid(&request.unique_key)?;
        if !self.index.contains_key(&request.unique_key) {
            self.index.insert(request.unique_key.clone(), self.requests.len());
            self.requests.push(request);
        } else if self.keep_duplicate_urls {
            tracing::warn!(
                "Duplicate uniqueKey: {} found while the keepDuplicateUrls option was set. Check your sources' unique \
                 keys.",
                request.unique_key
            );
        }
        Ok(())
    }

    fn add_source(&mut self, source: RequestListSource) -> StorageResult<()> {
        match source {
            RequestListSource::Url(url) => self.add(Request::new(url), false),
            RequestListSource::Request(request) => {
                let computed = compute_unique_key(&request.url, &request.method, None, false, false, false);
                let has_unique_key = request.unique_key != computed;
                self.add(*request, has_unique_key)
            }
            RequestListSource::RequestsFromUrl(source) => Err(StorageError::InvalidArgument(format!(
                "Cannot create Request from a requestsFromUrl source ({}) here",
                source.requests_from_url
            ))),
        }
    }
}

fn ensure_unique_key_valid(unique_key: &str) -> StorageResult<()> {
    if unique_key.is_empty() {
        return Err(StorageError::InvalidArgument("Request object's uniqueKey must be a non-empty string".into()));
    }
    Ok(())
}

/// A static list of requests to crawl; see the [module documentation](self). Cloning is cheap
/// and clones share the list.
#[derive(Clone)]
pub struct RequestList {
    services: Services,
    state: RecoverableState<ListState>,
    /// Requests to serve again first: those in progress when the state was restored, and those
    /// reclaimed to the front in a [`RequestListAndQueue`].
    requests_to_retry: Arc<Mutex<VecDeque<String>>>,
    /// Requests reclaimed (not to the front) in a [`RequestListAndQueue`]: served again after the
    /// rest of the list, as a tandem's queue serves them.
    requests_to_retry_last: Arc<Mutex<VecDeque<String>>>,
    /// The reclaimed requests, as they were reclaimed (with their error messages).
    reclaimed: Arc<Mutex<HashMap<String, Request>>>,
    persist_state_key: Option<String>,
    are_requests_persisted: Arc<AtomicBool>,
}

impl std::fmt::Debug for RequestList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestList").field("state", &self.get_state()).finish_non_exhaustive()
    }
}

impl RequestList {
    /// Opens a list of `sources` (`RequestList.open(name, sources)`). With a `list_name`, its
    /// state and its requests are saved under `<list_name>-REQUEST_LIST_STATE` and
    /// `<list_name>-REQUEST_LIST_REQUESTS` (both prefixed with `CRAWLEE_`), so that a restarted
    /// crawl continues where it left off.
    pub async fn open<I, S>(services: &Services, list_name: Option<&str>, sources: I) -> StorageResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<RequestListSource>,
    {
        let options = RequestListOptions {
            sources: sources.into_iter().map(Into::into).collect(),
            persist_state_key: list_name.map(|name| format!("{name}-{STATE_PERSISTENCE_KEY}")),
            persist_requests_key: list_name.map(|name| format!("{name}-{REQUESTS_PERSISTENCE_KEY}")),
            ..RequestListOptions::default()
        };
        Self::open_with_options(services, options).await
    }

    /// Opens a list (`RequestList.open(options)`): loads its requests (from the saved ones, or
    /// from the sources, downloading the remote lists), restores the saved state, and saves the
    /// requests if they were not saved yet.
    pub async fn open_with_options(services: &Services, options: RequestListOptions) -> StorageResult<Self> {
        let persist_state_key = options.persist_state_key.as_ref().map(|key| format!("CRAWLEE_{key}"));
        let persist_requests_key = options.persist_requests_key.as_ref().map(|key| format!("CRAWLEE_{key}"));
        services.purge_on_start().await?;

        let RequestListOptions {
            sources,
            sources_function,
            state,
            persist_state_interval,
            keep_duplicate_urls,
            url_list_fetcher,
            proxy_url,
            ..
        } = options;
        let mut loader = Loader { keep_duplicate_urls, ..Loader::default() };
        let persisted_requests = match &persist_requests_key {
            Some(key) => {
                let store = services.open_key_value_store(&StorageIdentifier::Default).await?;
                let record = store.get_record(key).await?;
                if record.is_some() {
                    tracing::debug!("Loaded requests from key value store using the persistRequestsKey.");
                }
                record
            }
            None => None,
        };
        let are_requests_persisted = persisted_requests.is_some();
        match persisted_requests {
            Some(record) => {
                for request in deserialize_requests(&record.value)? {
                    loader.add(request, true)?;
                }
            }
            None => {
                let fetch = FetchOptions { url_list_fetcher, proxy_url };
                add_requests_from_sources(&mut loader, sources, sources_function, &fetch).await?
            }
        }

        let state_key = persist_state_key.clone().unwrap_or_else(|| STATE_PERSISTENCE_KEY.to_owned());
        let Loader { requests, index, .. } = loader;
        let mut list_state =
            ListState { requests, index, position: Mutex::new(Position::default()), initial: Position::default() };
        if let Some(state) = state {
            // The `state` option is checked as a saved state would be.
            let initial = list_state
                .decode(state)
                .map_err(|source| StateValidationError { persist_state_key: state_key.clone(), source })?;
            list_state.position = Mutex::new(initial.clone());
            list_state.initial = initial;
        }
        let mut state = RecoverableState::new(services, state_key, Arc::new(list_state), persist_state_key.is_some());
        if let Some(interval) = persist_state_interval {
            state = state.persist_every(interval);
        }
        // Restores the saved position, and saves it on every `PersistState` event.
        state.initialize().await?;
        // The requests in progress were interrupted: they are crawled again.
        let requests_to_retry = state.state().position.lock().in_progress.iter().cloned().collect();

        let list = RequestList {
            services: services.clone(),
            state,
            requests_to_retry: Arc::new(Mutex::new(requests_to_retry)),
            requests_to_retry_last: Arc::default(),
            reclaimed: Arc::default(),
            persist_state_key,
            are_requests_persisted: Arc::new(AtomicBool::new(are_requests_persisted)),
        };
        if let Some(key) = &persist_requests_key
            && !are_requests_persisted
        {
            list.persist_requests(key).await?;
        }
        Ok(list)
    }

    /// Saves the requests once: the list does not change after that.
    async fn persist_requests(&self, key: &str) -> StorageResult<()> {
        let serialized = serialize_requests(&self.state.state().requests)?;
        let store = self.services.open_key_value_store(&StorageIdentifier::Default).await?;
        store.set_bytes(key, serialized, Some(CONTENT_TYPE_BINARY)).await?;
        self.are_requests_persisted.store(true, Ordering::Release);
        Ok(())
    }

    /// Whether the requests are saved under the `persist_requests_key`.
    pub fn are_requests_persisted(&self) -> bool {
        self.are_requests_persisted.load(Ordering::Acquire)
    }

    /// Saves the crawling position now. Fails without a `persist_state_key`; a failed write is
    /// only logged.
    pub async fn persist_state(&self) -> StorageResult<()> {
        if self.persist_state_key.is_none() {
            return Err(StorageError::InvalidArgument(
                "Cannot persist state. options.persistStateKey is not set.".into(),
            ));
        }
        if let Err(err) = self.state.persist().await {
            tracing::error!("Attempted to persist state, but failed. {err}");
        }
        Ok(())
    }

    /// Stops saving the position on `PersistState` events, and saves it one last time.
    pub async fn teardown(&self) {
        self.state.teardown().await;
    }

    /// The crawling position (`getState`).
    pub fn get_state(&self) -> RequestListState {
        let list = self.state.state();
        list.encode(&list.position.lock())
    }

    /// The list in front of `manager` (by default, the default request queue), which keeps the
    /// retries and the new requests (`toTandem`).
    pub async fn to_tandem(&self, manager: Option<Arc<dyn RequestManager>>) -> StorageResult<RequestManagerTandem> {
        let manager = match manager {
            Some(manager) => manager,
            None => Arc::new(self.services.open_request_queue(&StorageIdentifier::Default).await?),
        };
        Ok(RequestManagerTandem::new(Arc::new(self.clone()), manager))
    }

    /// The list in front of `manager` (by default, the default request queue), each keeping its
    /// own requests: unlike in a [tandem](Self::to_tandem), the list's requests never enter the
    /// queue, except retries a handler changed (see [`RequestListAndQueue`]).
    pub async fn with_queue(&self, manager: Option<Arc<dyn RequestManager>>) -> StorageResult<RequestListAndQueue> {
        let manager = match manager {
            Some(manager) => manager,
            None => Arc::new(self.services.open_request_queue(&StorageIdentifier::Default).await?),
        };
        Ok(RequestListAndQueue::new(self.clone(), manager))
    }

    /// Whether the list has a request with this unique key.
    pub(crate) fn contains(&self, unique_key: &str) -> bool {
        self.state.state().index.contains_key(unique_key)
    }

    /// Whether the list's request with this unique key was handled.
    pub(crate) fn is_handled(&self, unique_key: &str) -> bool {
        let list = self.state.state();
        let position = list.position.lock();
        list.index.get(unique_key).is_some_and(|&index| {
            index < position.next_index
                && (!position.in_progress.contains(unique_key) || position.moving.contains(unique_key))
        })
    }

    /// Whether the request in progress with this unique key is the list's (and not the queue's).
    pub(crate) fn owns(&self, unique_key: &str) -> bool {
        let position = self.state.state().position.lock();
        position.in_progress.contains(unique_key) && !position.moving.contains(unique_key)
    }

    /// Takes a failed request of the list back. One with the data the list has for it is kept,
    /// to be served again (`Kept`); one a handler changed is to be handed to the queue
    /// (`Changed`), and [`finish_move`](Self::finish_move) called then.
    pub(crate) fn reclaim(&self, request: &Request, forefront: bool) -> Reclaimed {
        let list = self.state.state();
        let unique_key = &request.unique_key;
        let mut position = list.position.lock();
        if !position.in_progress.contains(unique_key) || position.moving.contains(unique_key) {
            return Reclaimed::NotOwned;
        }
        if !has_same_data(&list.requests[list.index[unique_key]], request) {
            position.moving.insert(unique_key.clone());
            return Reclaimed::Changed;
        }
        let mut reclaimed = self.reclaimed.lock();
        if reclaimed.contains_key(unique_key) {
            // Already waiting to be served again.
            return Reclaimed::Kept;
        }
        if request.retry_count > 0 {
            position.retries.insert(unique_key.clone(), request.retry_count);
        } else {
            position.retries.remove(unique_key);
        }
        reclaimed.insert(unique_key.clone(), request.clone());
        if forefront {
            self.requests_to_retry.lock().push_front(unique_key.clone());
        } else {
            self.requests_to_retry_last.lock().push_back(unique_key.clone());
        }
        Reclaimed::Kept
    }

    /// Ends the hand-over of a changed request: the queue has it now, or, when adding it failed,
    /// the list serves it again.
    pub(crate) fn finish_move(&self, request: &Request, added: bool) {
        let unique_key = &request.unique_key;
        let mut position = self.state.state().position.lock();
        position.moving.remove(unique_key);
        if added {
            position.in_progress.shift_remove(unique_key);
            position.retries.remove(unique_key);
            return;
        }
        if request.retry_count > 0 {
            position.retries.insert(unique_key.clone(), request.retry_count);
        }
        self.reclaimed.lock().insert(unique_key.clone(), request.clone());
        self.requests_to_retry_last.lock().push_back(unique_key.clone());
    }
}

/// What [`RequestList::reclaim`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reclaimed {
    /// Not a request in progress of the list.
    NotOwned,
    /// Kept by the list, to be served again.
    Kept,
    /// A handler changed it: it is to be handed to the queue.
    Changed,
}

/// Whether a reclaimed request has the data of the list's request: everything but what crawling
/// it changes (its retries, errors, loaded URL and state).
fn has_same_data(original: &Request, request: &Request) -> bool {
    let crawlee = |data: &CrawleeRequestData| CrawleeRequestData { state: None, ..data.clone() };
    original.url == request.url
        && original.method == request.method
        && original.payload == request.payload
        && original.headers == request.headers
        && original.user_data == request.user_data
        && original.no_retry == request.no_retry
        && crawlee(&original.crawlee) == crawlee(&request.crawlee)
}

/// How remote lists of URLs are downloaded.
struct FetchOptions {
    url_list_fetcher: Option<Arc<dyn UrlListFetcher>>,
    proxy_url: Option<ProxyUrlFunction>,
}

/// Adds the requests of the sources, then those of the sources function (`#addRequestsFromSources`).
async fn add_requests_from_sources(
    loader: &mut Loader,
    sources: Vec<RequestListSource>,
    sources_function: Option<SourcesFunction>,
    fetch: &FetchOptions,
) -> StorageResult<()> {
    for source in sources {
        match source {
            RequestListSource::RequestsFromUrl(source) => {
                let proxy_url = fetch.proxy_url.as_ref().and_then(|proxy_url| proxy_url());
                let fetched = source.fetch_requests(fetch.url_list_fetcher.as_deref(), proxy_url.as_deref()).await?;
                let original_length = loader.requests.len();
                let fetched_count = fetched.len();
                let sample = source.sample(&fetched);
                for request in fetched {
                    loader.add(request, false)?;
                }
                let imported_count = loader.requests.len() - original_length;
                tracing::info!(
                    requests_from_url = source.requests_from_url,
                    regex = ?source.regex,
                    fetched_count,
                    imported_count,
                    duplicate_count = fetched_count - imported_count,
                    sample = ?sample,
                    "Fetched and loaded Requests from a remote resource."
                );
            }
            source => loader.add_source(source)?,
        }
    }
    if let Some(sources_function) = sources_function {
        let loaded = async {
            for source in sources_function().await? {
                loader.add_source(source)?;
            }
            Ok::<_, BoxError>(())
        };
        loaded.await.map_err(|err| {
            StorageError::InvalidArgument(format!("Loading requests with sourcesFunction failed.\nCause: {err}"))
        })?;
    }
    Ok(())
}

/// A gzipped JSON array, as `serializeArray` of JS writes it.
fn serialize_requests(requests: &[Request]) -> StorageResult<Bytes> {
    let json = serde_json::to_vec(requests)?;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(&json)
        .and_then(|()| encoder.finish())
        .map(Bytes::from)
        .map_err(|err| StorageError::Backend(err.into()))
}

fn deserialize_requests(compressed: &[u8]) -> StorageResult<Vec<Request>> {
    let mut json = Vec::new();
    flate2::read::GzDecoder::new(compressed).read_to_end(&mut json).map_err(|err| StorageError::Backend(err.into()))?;
    Ok(serde_json::from_slice(&json)?)
}

#[async_trait]
impl RequestLoader for RequestList {
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        let list = self.state.state();
        // First the requests interrupted before the state was saved, and those reclaimed to the
        // front; the other reclaimed ones after the rest of the list.
        let mut retry = self.requests_to_retry.lock().pop_front();
        if retry.is_none() && list.position.lock().next_index >= list.requests.len() {
            retry = self.requests_to_retry_last.lock().pop_front();
        }
        if let Some(unique_key) = retry {
            if let Some(request) = self.reclaimed.lock().remove(&unique_key) {
                return Ok(Some(request));
            }
            let mut request = list.requests[list.index[&unique_key]].clone();
            request.retry_count = list.position.lock().retries.get(&unique_key).copied().unwrap_or(0);
            return Ok(Some(request));
        }
        let mut position = list.position.lock();
        let Some(request) = list.requests.get(position.next_index) else { return Ok(None) };
        position.in_progress.insert(request.unique_key.clone());
        position.next_index += 1;
        Ok(Some(request.clone()))
    }

    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<()> {
        ensure_unique_key_valid(&request.unique_key)?;
        let mut position = self.state.state().position.lock();
        if !position.in_progress.shift_remove(&request.unique_key) {
            return Err(StorageError::InvalidArgument(format!(
                "The request is not being processed (uniqueKey: {})",
                request.unique_key
            )));
        }
        position.retries.remove(&request.unique_key);
        Ok(())
    }

    async fn status(&self) -> StorageResult<LoaderStatus> {
        let list = self.state.state();
        let position = list.position.lock();
        if !self.requests_to_retry.lock().is_empty()
            || position.next_index < list.requests.len()
            || !self.requests_to_retry_last.lock().is_empty()
        {
            return Ok(LoaderStatus::Ready);
        }
        // The requests to retry are in progress, so nothing in progress means nothing left.
        Ok(if position.in_progress.is_empty() { LoaderStatus::Finished } else { LoaderStatus::Waiting })
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        let position = self.state.state().position.lock();
        Ok((position.next_index - position.in_progress.len()) as u64)
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        let list = self.state.state();
        let position = list.position.lock();
        Ok((list.requests.len() - (position.next_index - position.in_progress.len())) as u64)
    }

    async fn total_count(&self) -> StorageResult<u64> {
        Ok(self.state.state().requests.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|path| format!("https://example.com/{path}")).collect()
    }

    #[tokio::test]
    async fn tracks_in_progress_requests_through_the_crawl() {
        let services = Services::in_memory();
        let list = RequestList::open(&services, None, urls(&["1", "2", "3", "1"])).await.unwrap();
        assert_eq!(list.total_count().await.unwrap(), 3, "deduplicated by unique key");

        let first = list.fetch_next_request().await.unwrap().unwrap();
        let second = list.fetch_next_request().await.unwrap().unwrap();
        assert_eq!((first.url.as_str(), second.url.as_str()), ("https://example.com/1", "https://example.com/2"));
        assert_eq!(
            list.get_state(),
            RequestListState {
                next_index: 2,
                next_unique_key: Some("https://example.com/3".into()),
                in_progress: urls(&["1", "2"]),
                retries: Default::default(),
            }
        );
        assert_eq!(list.status().await.unwrap(), LoaderStatus::Ready);
        assert_eq!(list.handled_count().await.unwrap(), 0);

        list.mark_request_as_handled(&second).await.unwrap();
        list.mark_request_as_handled(&first).await.unwrap();
        assert_eq!(list.handled_count().await.unwrap(), 2);
        let err = list.mark_request_as_handled(&first).await.unwrap_err();
        assert!(err.to_string().contains("The request is not being processed"), "{err}");

        let third = list.fetch_next_request().await.unwrap().unwrap();
        assert!(list.fetch_next_request().await.unwrap().is_none());
        assert_eq!(list.status().await.unwrap(), LoaderStatus::Waiting);
        assert_eq!(list.pending_count().await.unwrap(), 1);
        list.mark_request_as_handled(&third).await.unwrap();
        assert_eq!(list.status().await.unwrap(), LoaderStatus::Finished);
        assert_eq!(list.get_state().next_unique_key, None);
    }

    #[tokio::test]
    async fn saves_its_state_and_continues_from_it() {
        let services = Services::in_memory();
        let options = || RequestListOptions {
            sources: urls(&["1", "2", "3"]).into_iter().map(Into::into).collect(),
            persist_state_key: Some("some-key".into()),
            ..RequestListOptions::default()
        };
        let list = RequestList::open_with_options(&services, options()).await.unwrap();
        let first = list.fetch_next_request().await.unwrap().unwrap();
        let second = list.fetch_next_request().await.unwrap().unwrap();
        list.mark_request_as_handled(&second).await.unwrap();

        services.events.emit(crate::Event::PersistState { is_migrating: false });
        services.events.wait_for_all_listeners_to_complete().await;
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        let saved: RequestListState = store.get_value("CRAWLEE_some-key").await.unwrap().unwrap();
        assert_eq!(saved, list.get_state());
        list.teardown().await;
        assert_eq!(services.events.listener_count(crate::EventKind::PersistState), 0);

        // The request in progress is served again first.
        let restored = RequestList::open_with_options(&services, options()).await.unwrap();
        assert_eq!(restored.get_state(), list.get_state());
        assert_eq!(restored.fetch_next_request().await.unwrap().unwrap().url, first.url);
        assert_eq!(restored.fetch_next_request().await.unwrap().unwrap().url, "https://example.com/3");
        assert!(restored.fetch_next_request().await.unwrap().is_none());

        // Without a key, there is nothing to save.
        let unsaved = RequestList::open(&services, None, ["https://example.com/1"]).await.unwrap();
        assert!(unsaved.persist_state().await.is_err());
    }

    #[tokio::test]
    async fn a_saved_state_must_match_the_requests() {
        let services = Services::in_memory();
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        let sources = || urls(&["1", "2", "3"]).into_iter().map(RequestListSource::from).collect();
        store
            .set_value(
                "CRAWLEE_state-key",
                &RequestListState {
                    next_index: 1,
                    next_unique_key: Some("https://example.com/3".into()),
                    in_progress: vec![],
                    retries: Default::default(),
                },
            )
            .await
            .unwrap();
        let options = RequestListOptions {
            sources: sources(),
            persist_state_key: Some("state-key".into()),
            ..RequestListOptions::default()
        };
        let err = RequestList::open_with_options(&services, options).await.unwrap_err();
        assert!(matches!(err, StorageError::StateValidation(_)), "{err}");

        // A saved state takes precedence over the `state` option.
        store
            .set_value(
                "CRAWLEE_state-key",
                &RequestListState {
                    next_index: 2,
                    next_unique_key: Some("https://example.com/3".into()),
                    in_progress: vec![],
                    retries: Default::default(),
                },
            )
            .await
            .unwrap();
        let options = RequestListOptions {
            sources: sources(),
            persist_state_key: Some("state-key".into()),
            state: Some(RequestListState {
                next_index: 1,
                next_unique_key: Some("https://example.com/2".into()),
                in_progress: vec![],
                retries: Default::default(),
            }),
            ..RequestListOptions::default()
        };
        let list = RequestList::open_with_options(&services, options).await.unwrap();
        assert_eq!(list.get_state().next_index, 2);
        list.teardown().await;
    }

    #[tokio::test]
    async fn in_progress_requests_at_or_after_the_next_index_are_dropped_from_the_state() {
        let services = Services::in_memory();
        let options = RequestListOptions {
            sources: urls(&["a", "b", "c", "d"]).into_iter().map(Into::into).collect(),
            state: Some(RequestListState {
                next_index: 2,
                next_unique_key: Some("https://example.com/c".into()),
                in_progress: urls(&["a", "b", "c"]),
                retries: Default::default(),
            }),
            ..RequestListOptions::default()
        };
        let list = RequestList::open_with_options(&services, options).await.unwrap();
        let mut fetched = Vec::new();
        while let Some(request) = list.fetch_next_request().await.unwrap() {
            fetched.push(request);
        }
        let fetched_urls: Vec<&str> = fetched.iter().map(|request| request.url.as_str()).collect();
        assert_eq!(fetched_urls, urls(&["a", "b", "c", "d"]));
        for request in fetched.iter().rev() {
            list.mark_request_as_handled(request).await.unwrap();
        }
        assert_eq!(list.status().await.unwrap(), LoaderStatus::Finished);
    }

    /// Serves one list body and records what it fetched, and through which proxy.
    #[derive(Default)]
    struct FixedFetcher {
        fetched: Mutex<Vec<(String, Option<String>)>>,
    }

    #[async_trait]
    impl UrlListFetcher for FixedFetcher {
        async fn fetch(&self, url: &str, proxy_url: Option<&str>) -> Result<Vec<u8>, BoxError> {
            self.fetched.lock().push((url.to_owned(), proxy_url.map(str::to_owned)));
            Ok(b"http://example.com/3\nhttp://example.com/4 http://example.com/3".to_vec())
        }
    }

    #[tokio::test]
    async fn saves_its_requests_with_those_of_remote_lists() {
        let services = Services::in_memory();
        let fetcher = Arc::new(FixedFetcher::default());
        let options = RequestListOptions {
            sources: vec![
                "https://example.com/1".into(),
                RequestsFromUrl::new("http://example.com/list-urls.txt")
                    .request(|url| Request::builder(url).label("FROM_URL").build())
                    .into(),
                "https://example.com/5".into(),
            ],
            persist_requests_key: Some("some-key".into()),
            url_list_fetcher: Some(fetcher.clone()),
            ..RequestListOptions::default()
        }
        .proxy_url(|| Some("http://proxy.test:8000".into()));
        let list = RequestList::open_with_options(&services, options).await.unwrap();
        assert!(list.are_requests_persisted());
        assert_eq!(
            *fetcher.fetched.lock(),
            [("http://example.com/list-urls.txt".to_owned(), Some("http://proxy.test:8000".to_owned()))]
        );
        assert_eq!(list.total_count().await.unwrap(), 4);

        // Opened again, the list is made of the saved requests, not of its new sources.
        let options = RequestListOptions {
            sources: vec!["https://test.com/1".into()],
            persist_requests_key: Some("some-key".into()),
            ..RequestListOptions::default()
        };
        let restored = RequestList::open_with_options(&services, options).await.unwrap();
        assert!(restored.are_requests_persisted());
        assert_eq!(restored.state.state().requests, list.state.state().requests);
        assert_eq!(restored.state.state().requests[1].label(), Some("FROM_URL"));

        // Saved as JS saves them: a gzipped JSON array.
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        let record = store.get_record("CRAWLEE_some-key").await.unwrap().unwrap();
        assert_eq!(record.content_type.as_deref(), Some(CONTENT_TYPE_BINARY));
        let saved: Vec<Value> = serde_json::from_slice(&{
            let mut json = Vec::new();
            flate2::read::GzDecoder::new(&record.value[..]).read_to_end(&mut json).unwrap();
            json
        })
        .unwrap();
        assert_eq!(saved[1]["uniqueKey"], "http://example.com/3");

        // A remote list without a fetcher cannot be read.
        let err = RequestList::open(&services, None, [RequestsFromUrl::new("http://example.com/l")]).await.unwrap_err();
        assert!(err.to_string().contains("Cannot fetch a request list from http://example.com/l"), "{err}");
    }

    #[tokio::test]
    async fn named_lists_save_their_state_and_requests() {
        let services = Services::in_memory();
        let list = RequestList::open(&services, Some("my-list"), ["https://example.com/1"]).await.unwrap();
        list.fetch_next_request().await.unwrap();
        list.persist_state().await.unwrap();
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        assert!(store.record_exists("CRAWLEE_my-list-REQUEST_LIST_STATE").await.unwrap());
        assert!(store.record_exists("CRAWLEE_my-list-REQUEST_LIST_REQUESTS").await.unwrap());
        list.teardown().await;
    }

    #[tokio::test]
    async fn keeps_duplicate_urls_when_asked() {
        let services = Services::in_memory();
        let mut sources: Vec<RequestListSource> =
            ["https://www.example.com", "https://www.example.com", "https://www.ex2mple.com"]
                .into_iter()
                .map(Into::into)
                .collect();
        sources.push(Request::new("https://www.example.com").into());
        let options = RequestListOptions { sources: sources.clone(), keep_duplicate_urls: true, ..Default::default() };
        let list = RequestList::open_with_options(&services, options).await.unwrap();
        assert_eq!(list.total_count().await.unwrap(), 4);
        assert_eq!(list.fetch_next_request().await.unwrap().unwrap().unique_key, "https://www.example.com-0");

        // Unique keys set explicitly are kept as they are, duplicates included.
        for key in ["123", "123", "456"] {
            sources.push(Request::builder("https://www.example.com").unique_key(key).build().unwrap().into());
        }
        let options = RequestListOptions { sources, keep_duplicate_urls: true, ..Default::default() };
        let list = RequestList::open_with_options(&services, options).await.unwrap();
        assert_eq!(list.total_count().await.unwrap(), 6);
    }

    #[tokio::test]
    async fn adds_the_sources_of_the_sources_function() {
        let services = Services::in_memory();
        let options = RequestListOptions { sources: vec!["https://example.com/1".into()], ..Default::default() }
            .sources_function(|| async { Ok(vec!["https://example.com/2".into()]) });
        let list = RequestList::open_with_options(&services, options).await.unwrap();
        assert_eq!(list.total_count().await.unwrap(), 2);

        let options = RequestListOptions::default().sources_function(|| async { Err("unavailable".into()) });
        let err = RequestList::open_with_options(&services, options).await.unwrap_err();
        assert_eq!(err.to_string(), "Loading requests with sourcesFunction failed.\nCause: unavailable");
    }

    #[tokio::test]
    async fn moves_its_requests_into_a_queue_in_a_tandem() {
        let services = Services::in_memory();
        let list = RequestList::open(&services, None, urls(&["1", "2"])).await.unwrap();
        let queue = Arc::new(services.open_request_queue(&StorageIdentifier::alias("tandem")).await.unwrap());
        let tandem = list.to_tandem(Some(queue.clone())).await.unwrap();
        let mut request = tandem.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(request.url, "https://example.com/1");
        tandem.mark_request_as_handled(&mut request).await.unwrap();
        assert_eq!(list.handled_count().await.unwrap(), 1, "handed over to the queue");
        assert_eq!(tandem.pending_count().await.unwrap(), 1);
        assert_eq!(tandem.total_count().await.unwrap(), 2);
    }
}
