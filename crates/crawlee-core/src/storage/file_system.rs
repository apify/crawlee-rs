//! [`FileSystemStorageBackend`]: storages as files in a local directory, in the on-disk format of
//! Crawlee for JS and Python.
//!
//! The format, the bookkeeping and the request-queue locking all live in the
//! [`crawlee-storage`](https://github.com/apify/crawlee-storage) crate, the Rust core behind the
//! file-system storage of both Crawlee for JS and Crawlee for Python. So a `storage/` directory
//! written by any of the three can be read by the others. This module only resolves identifiers
//! to storages, caches the opened ones and maps the types, like `FileSystemStorageBackend` in
//! `@crawlee/fs-storage`.
//!
//! ```text
//! storage/
//!   datasets/default/000000001.json, ..., __metadata__.json
//!   key_value_stores/default/<key>, <key>.__metadata__.json, __metadata__.json
//!   request_queues/default/<request id>.json, ..., __metadata__.json
//! ```

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use crate::local::Mutex;
use async_trait::async_trait;
use bytes::Bytes;
use crawlee_storage::dataset::FileSystemDatasetClient;
use crawlee_storage::key_value_store::FileSystemKeyValueStoreClient;
use crawlee_storage::models::{AdoptableFile, AdoptionCandidate, AdoptionRule, StorageMetadata};
use crawlee_storage::request_queue::FileSystemRequestQueueClient;
use serde_json::Value;
use serde_json::value::RawValue;

use super::backend::{
    BatchAddRequestsResult, DatasetBackend, DatasetInfo, DatasetItem, DatasetListOptions, KeyValueStoreBackend,
    KeyValueStoreInfo, KeyValueStoreItemData, KeyValueStoreListKeysOptions, KeyValueStoreListKeysResult,
    KeyValueStoreRecord, PaginatedList, ProcessedRequest, QueueOperationInfo, RequestQueueBackend, RequestQueueInfo,
    StorageBackend, StorageIdentifier, StorageKind, UnprocessedRequest,
};
use crate::errors::{StorageError, StorageResult};
use crate::request::Request;

/// The directory of the default storage, one level below `datasets` / `key_value_stores` / ...
const DEFAULT_DIRECTORY: &str = "default";
/// The alias the default storage is also known by (the JS frontends open it under it).
const DEFAULT_ALIAS: &str = "__default__";
const METADATA_FILENAME: &str = "__metadata__.json";

/// Content types of value files found in a key-value store directory without a metadata sidecar
/// (written by hand or by another tool). They are adopted as records keyed by their filename.
const ADOPTED_JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const ADOPTED_BINARY_CONTENT_TYPE: &str = "application/octet-stream";

impl From<crawlee_storage::utils::StorageError> for StorageError {
    fn from(err: crawlee_storage::utils::StorageError) -> Self {
        use crawlee_storage::utils::StorageError as Fs;
        match err {
            Fs::NotFound(message) => StorageError::NotFound(message),
            Fs::InvalidArgs(message) => StorageError::InvalidArgument(message),
            Fs::Json(err) => StorageError::Serialization(err),
            other => StorageError::Backend(Box::new(other)),
        }
    }
}

/// How the request queues on disk are shared.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RequestQueueAccess {
    /// This process is the only consumer: requests left in progress by a previous (crashed) run
    /// are reclaimed as soon as a queue is opened.
    #[default]
    Single,
    /// Several processes share the queues: an in-progress request is only reclaimed once its lock
    /// expires, so two workers never process it at once.
    Shared,
}

/// Where a storage was opened from; used to find an already opened storage again.
#[derive(Debug)]
struct CacheEntry {
    id: String,
    name: Option<String>,
    cache_key: String,
    dropped: Cell<bool>,
}

impl CacheEntry {
    fn matches(&self, key: &str) -> bool {
        self.id == key
            || self.name.as_deref().is_some_and(|name| name.eq_ignore_ascii_case(key))
            || self.cache_key.eq_ignore_ascii_case(key)
    }
}

/// An identifier resolved to the arguments of the `crawlee-storage` clients.
struct Resolved {
    id: Option<String>,
    name: Option<String>,
    alias: Option<String>,
    cache_key: String,
}

fn resolve(identifier: &StorageIdentifier) -> Resolved {
    let (id, name, alias) = match identifier {
        StorageIdentifier::Default => (None, None, Some(DEFAULT_DIRECTORY.to_owned())),
        StorageIdentifier::Alias(alias) if alias == DEFAULT_ALIAS => (None, None, Some(DEFAULT_DIRECTORY.to_owned())),
        StorageIdentifier::Alias(alias) => (None, None, Some(alias.clone())),
        StorageIdentifier::Name(name) => (None, Some(name.clone()), None),
        StorageIdentifier::Id(id) => (Some(id.clone()), None, None),
    };
    let cache_key = alias.clone().or_else(|| name.clone()).or_else(|| id.clone()).unwrap_or_default();
    Resolved { id, name, alias, cache_key }
}

fn cache_entry(resolved: &Resolved, metadata: &StorageMetadata) -> CacheEntry {
    CacheEntry {
        id: metadata.id.clone(),
        name: metadata.name.clone(),
        cache_key: resolved.cache_key.clone(),
        dropped: Cell::new(false),
    }
}

trait Cached {
    fn entry(&self) -> &CacheEntry;
}

/// Returns the opened storage `key` refers to, pruning dropped ones.
fn find_cached<T: Cached>(cache: &mut Vec<Rc<T>>, key: &str) -> Option<Rc<T>> {
    cache.retain(|storage| !storage.entry().dropped.get());
    cache.iter().find(|storage| storage.entry().matches(key)).cloned()
}

/// Storages as files under a local directory (`./storage` by default).
pub struct FileSystemStorageBackend {
    storage_dir: PathBuf,
    request_queue_access: RequestQueueAccess,
    /// See [`with_input_keys`](Self::with_input_keys).
    input_keys: Vec<String>,
    // Held across the `open` of a storage, so that two tasks opening the same storage get the
    // same client instead of racing two clients onto one directory.
    datasets: Mutex<Vec<Rc<FsDataset>>>,
    key_value_stores: Mutex<Vec<Rc<FsKeyValueStore>>>,
    request_queues: Mutex<Vec<Rc<FsRequestQueue>>>,
}

impl std::fmt::Debug for FileSystemStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSystemStorageBackend").field("storage_dir", &self.storage_dir).finish_non_exhaustive()
    }
}

impl FileSystemStorageBackend {
    pub fn new(storage_dir: impl Into<PathBuf>) -> Self {
        Self::with_request_queue_access(storage_dir, RequestQueueAccess::default())
    }

    pub fn with_request_queue_access(storage_dir: impl Into<PathBuf>, access: RequestQueueAccess) -> Self {
        FileSystemStorageBackend {
            storage_dir: storage_dir.into(),
            request_queue_access: access,
            input_keys: Vec::new(),
            datasets: Mutex::default(),
            key_value_stores: Mutex::default(),
            request_queues: Mutex::default(),
        }
    }

    /// Keys of the default key-value store that hold the input of a run (an Actor input, for
    /// example). A bare `<key>` or `<key>.json` file there becomes the record `<key>` when the store
    /// is opened (by default it would be the record named like the file), and the records survive
    /// [`purge`](StorageBackend::purge).
    pub fn with_input_keys(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        for key in keys {
            let key = key.into();
            if !self.input_keys.contains(&key) {
                self.input_keys.push(key);
            }
        }
        self
    }

    pub fn storage_dir(&self) -> &Path {
        &self.storage_dir
    }

    fn kind_directory(&self, kind: StorageKind) -> PathBuf {
        self.storage_dir.join(match kind {
            StorageKind::Dataset => "datasets",
            StorageKind::KeyValueStore => "key_value_stores",
            StorageKind::RequestQueue => "request_queues",
        })
    }

    async fn open_dataset(&self, identifier: &StorageIdentifier) -> StorageResult<Rc<FsDataset>> {
        let resolved = resolve(identifier);
        let mut cache = self.datasets.lock().await;
        if let Some(found) = find_cached(&mut cache, &resolved.cache_key) {
            return Ok(found);
        }
        let client = FileSystemDatasetClient::open(
            resolved.id.clone(),
            resolved.name.clone(),
            resolved.alias.clone(),
            &self.storage_dir,
        )
        .await?;
        let entry = cache_entry(&resolved, &client.get_metadata().await.base);
        let dataset = Rc::new(FsDataset { client, entry });
        cache.push(dataset.clone());
        Ok(dataset)
    }

    async fn open_key_value_store(&self, identifier: &StorageIdentifier) -> StorageResult<Rc<FsKeyValueStore>> {
        let resolved = resolve(identifier);
        let mut cache = self.key_value_stores.lock().await;
        if let Some(found) = find_cached(&mut cache, &resolved.cache_key) {
            return Ok(found);
        }
        let is_default = resolved.alias.as_deref() == Some(DEFAULT_DIRECTORY);
        let mut adopt: Vec<AdoptionCandidate> = if is_default {
            self.input_keys
                .iter()
                .map(|key| AdoptionCandidate::Key {
                    key: key.clone(),
                    files: vec![
                        AdoptableFile { filename: key.clone(), content_type: ADOPTED_BINARY_CONTENT_TYPE.to_owned() },
                        AdoptableFile {
                            filename: format!("{key}.json"),
                            content_type: ADOPTED_JSON_CONTENT_TYPE.to_owned(),
                        },
                    ],
                })
                .collect()
        } else {
            Vec::new()
        };
        adopt.push(AdoptionCandidate::Sweep {
            rules: vec![
                AdoptionRule { pattern: "*.json".to_owned(), content_type: ADOPTED_JSON_CONTENT_TYPE.to_owned() },
                AdoptionRule { pattern: "*".to_owned(), content_type: ADOPTED_BINARY_CONTENT_TYPE.to_owned() },
            ],
        });
        let client = FileSystemKeyValueStoreClient::open(
            resolved.id.clone(),
            resolved.name.clone(),
            resolved.alias.clone(),
            &self.storage_dir,
            &adopt,
        )
        .await?;
        let entry = cache_entry(&resolved, &client.get_metadata().await.base);
        let store = Rc::new(FsKeyValueStore { client, entry });
        cache.push(store.clone());
        Ok(store)
    }

    async fn open_request_queue(&self, identifier: &StorageIdentifier) -> StorageResult<Rc<FsRequestQueue>> {
        let resolved = resolve(identifier);
        let mut cache = self.request_queues.lock().await;
        if let Some(found) = find_cached(&mut cache, &resolved.cache_key) {
            return Ok(found);
        }
        let client = FileSystemRequestQueueClient::open_with_clock(
            resolved.id.clone(),
            resolved.name.clone(),
            resolved.alias.clone(),
            &self.storage_dir,
            crawlee_storage::clock::system_clock(),
            self.request_queue_access == RequestQueueAccess::Single,
        )
        .await?;
        let entry = cache_entry(&resolved, &client.get_metadata().await.base);
        let queue = Rc::new(FsRequestQueue { client, entry });
        cache.push(queue.clone());
        Ok(queue)
    }

    /// The directories of the run-scoped storages of one kind: `default` plus every storage
    /// Crawlee created without a name (the aliased ones). Directories without Crawlee metadata and
    /// storages named after their own id (reachable only by id) are left alone.
    async fn run_scoped_directories(&self, kind: StorageKind) -> Vec<String> {
        let mut directories = vec![DEFAULT_DIRECTORY.to_owned()];
        let Ok(mut entries) = tokio::fs::read_dir(self.kind_directory(kind)).await else {
            return directories;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Ok(dirname) = entry.file_name().into_string() else { continue };
            if dirname == DEFAULT_DIRECTORY || !entry.file_type().await.is_ok_and(|t| t.is_dir()) {
                continue;
            }
            if let Some(metadata) = read_metadata(&entry.path()).await
                && !metadata.get("name").is_some_and(Value::is_string)
                && metadata.get("id").and_then(Value::as_str) != Some(dirname.as_str())
            {
                directories.push(dirname);
            }
        }
        directories
    }

    /// The real id of the storage `name_or_id` refers to on disk: the id in the metadata of the
    /// directory with that name, or of a directory whose metadata id equals it.
    async fn resolve_id_on_disk(&self, kind: StorageKind, name_or_id: &str) -> Option<String> {
        let base = self.kind_directory(kind);
        if let Some(id) = read_metadata(&base.join(name_or_id)).await.and_then(metadata_id) {
            return Some(id);
        }
        let mut entries = tokio::fs::read_dir(&base).await.ok()?;
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(id) = read_metadata(&entry.path()).await.and_then(metadata_id)
                && id == name_or_id
            {
                return Some(id);
            }
        }
        None
    }
}

async fn read_metadata(directory: &Path) -> Option<Value> {
    let text = tokio::fs::read_to_string(directory.join(METADATA_FILENAME)).await.ok()?;
    serde_json::from_str(&text).ok()
}

fn metadata_id(metadata: Value) -> Option<String> {
    metadata.get("id")?.as_str().map(str::to_owned)
}

#[async_trait(?Send)]
impl StorageBackend for FileSystemStorageBackend {
    async fn create_dataset_backend(&self, id: &StorageIdentifier) -> StorageResult<Rc<dyn DatasetBackend>> {
        Ok(self.open_dataset(id).await?)
    }

    async fn create_key_value_store_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Rc<dyn KeyValueStoreBackend>> {
        Ok(self.open_key_value_store(id).await?)
    }

    async fn create_request_queue_backend(&self, id: &StorageIdentifier) -> StorageResult<Rc<dyn RequestQueueBackend>> {
        Ok(self.open_request_queue(id).await?)
    }

    async fn storage_exists(&self, id: &str, kind: StorageKind) -> StorageResult<bool> {
        let cached = match kind {
            StorageKind::Dataset => self.datasets.lock().await.iter().any(|s| s.entry.id == id),
            StorageKind::KeyValueStore => self.key_value_stores.lock().await.iter().any(|s| s.entry.id == id),
            StorageKind::RequestQueue => self.request_queues.lock().await.iter().any(|s| s.entry.id == id),
        };
        // A directory named `id` is not enough: directories are named after the name or alias
        // too, so the id is read from the metadata.
        Ok(cached || self.resolve_id_on_disk(kind, id).await.as_deref() == Some(id))
    }

    /// Empties the run-scoped storages found on disk, including the ones this process has not
    /// opened yet (leftovers of a previous run).
    async fn purge(&self) -> StorageResult<()> {
        for dirname in self.run_scoped_directories(StorageKind::KeyValueStore).await {
            let keep: &[String] = if dirname == DEFAULT_DIRECTORY { &self.input_keys } else { &[] };
            self.open_key_value_store(&StorageIdentifier::Alias(dirname)).await?.client.purge(keep).await?;
        }
        for dirname in self.run_scoped_directories(StorageKind::Dataset).await {
            self.open_dataset(&StorageIdentifier::Alias(dirname)).await?.client.purge().await?;
        }
        for dirname in self.run_scoped_directories(StorageKind::RequestQueue).await {
            self.open_request_queue(&StorageIdentifier::Alias(dirname)).await?.client.purge().await?;
        }
        Ok(())
    }

    /// Persists the state of every open request queue, so requests fetched but not handled are
    /// not stuck for the next consumer of the queue.
    async fn teardown(&self) -> StorageResult<()> {
        let queues: Vec<_> = self.request_queues.lock().await.clone();
        for queue in queues {
            queue.client.persist_state().await;
        }
        Ok(())
    }
}

struct FsDataset {
    client: FileSystemDatasetClient,
    entry: CacheEntry,
}

impl Cached for FsDataset {
    fn entry(&self) -> &CacheEntry {
        &self.entry
    }
}

#[async_trait(?Send)]
impl DatasetBackend for FsDataset {
    async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
        let metadata = self.client.get_metadata().await;
        let base = metadata.base;
        Ok(DatasetInfo {
            id: base.id,
            name: base.name,
            created_at: base.created_at,
            modified_at: base.modified_at,
            accessed_at: base.accessed_at,
            item_count: metadata.item_count as u64,
        })
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.client.drop_storage().await?;
        self.entry.dropped.set(true);
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        Ok(self.client.purge().await?)
    }

    async fn push_data(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let items = items.iter().map(|item| serde_json::from_str(item.get())).collect::<Result<Vec<Value>, _>>()?;
        Ok(self.client.push_data(Value::Array(items)).await?)
    }

    async fn get_data(&self, options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>> {
        let page = self.client.get_data(options.offset, options.limit, options.desc, false).await?;
        let items =
            page.items.iter().map(serde_json::value::to_raw_value).collect::<Result<Vec<Box<RawValue>>, _>>()?;
        Ok(PaginatedList {
            total: page.total,
            count: page.count,
            offset: page.offset,
            limit: options.limit,
            desc: page.desc,
            items,
        })
    }
}

struct FsKeyValueStore {
    client: FileSystemKeyValueStoreClient,
    entry: CacheEntry,
}

impl Cached for FsKeyValueStore {
    fn entry(&self) -> &CacheEntry {
        &self.entry
    }
}

#[async_trait(?Send)]
impl KeyValueStoreBackend for FsKeyValueStore {
    async fn get_metadata(&self) -> StorageResult<KeyValueStoreInfo> {
        let base = self.client.get_metadata().await.base;
        Ok(KeyValueStoreInfo {
            id: base.id,
            name: base.name,
            created_at: base.created_at,
            modified_at: base.modified_at,
            accessed_at: base.accessed_at,
        })
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.client.drop_storage().await?;
        self.entry.dropped.set(true);
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        Ok(self.client.purge(&[]).await?)
    }

    async fn get_value(&self, key: &str) -> StorageResult<Option<KeyValueStoreRecord>> {
        Ok(self.client.read_value(key).await?.map(|record| KeyValueStoreRecord {
            key: record.key,
            value: Bytes::from(record.value),
            content_type: Some(record.content_type),
        }))
    }

    async fn set_value(&self, record: KeyValueStoreRecord) -> StorageResult<()> {
        // The frontend resolves the content type; the backend only stores bytes.
        let content_type = record.content_type.unwrap_or_else(|| ADOPTED_BINARY_CONTENT_TYPE.to_owned());
        Ok(self.client.set_value(&record.key, &record.value, content_type, None).await?)
    }

    async fn delete_value(&self, key: &str) -> StorageResult<()> {
        Ok(self.client.delete_value(key).await?)
    }

    async fn list_keys(&self, options: KeyValueStoreListKeysOptions) -> StorageResult<KeyValueStoreListKeysResult> {
        let result = self
            .client
            .list_keys(options.exclusive_start_key.as_deref(), options.limit, options.prefix.as_deref(), &[])
            .await?;
        Ok(KeyValueStoreListKeysResult {
            items: result
                .items
                .into_iter()
                .map(|item| KeyValueStoreItemData {
                    key: item.key,
                    size: item.size.unwrap_or_default(),
                    content_type: item.content_type,
                })
                .collect(),
            count: result.count,
            limit: result.limit,
            exclusive_start_key: result.exclusive_start_key,
            is_truncated: result.is_truncated,
            next_exclusive_start_key: result.next_exclusive_start_key,
        })
    }

    async fn get_public_url(&self, key: &str) -> StorageResult<Option<String>> {
        Ok(Some(self.client.get_public_url(key).await))
    }

    async fn record_exists(&self, key: &str) -> StorageResult<bool> {
        Ok(self.client.record_exists(key, true).await)
    }
}

struct FsRequestQueue {
    client: FileSystemRequestQueueClient,
    entry: CacheEntry,
}

impl Cached for FsRequestQueue {
    fn entry(&self) -> &CacheEntry {
        &self.entry
    }
}

fn operation_info(processed: crawlee_storage::models::ProcessedRequest) -> QueueOperationInfo {
    QueueOperationInfo {
        request_id: processed.request_id,
        was_already_present: processed.was_already_present,
        was_already_handled: processed.was_already_handled,
    }
}

fn to_request(value: Option<Value>) -> StorageResult<Option<Request>> {
    Ok(value.map(serde_json::from_value).transpose()?)
}

fn chrono_duration(duration: Duration) -> StorageResult<chrono::Duration> {
    chrono::Duration::from_std(duration).map_err(|err| StorageError::InvalidArgument(err.to_string()))
}

#[async_trait(?Send)]
impl RequestQueueBackend for FsRequestQueue {
    async fn get_metadata(&self) -> StorageResult<RequestQueueInfo> {
        let metadata = self.client.get_metadata().await;
        let base = metadata.base;
        Ok(RequestQueueInfo {
            id: base.id,
            name: base.name,
            created_at: base.created_at,
            modified_at: base.modified_at,
            accessed_at: base.accessed_at,
            total_request_count: metadata.total_request_count as u64,
            handled_request_count: metadata.handled_request_count as u64,
            pending_request_count: metadata.pending_request_count as u64,
        })
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.client.drop_storage().await?;
        self.entry.dropped.set(true);
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        Ok(self.client.purge().await?)
    }

    async fn add_batch_of_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> StorageResult<BatchAddRequestsResult> {
        let requests = requests.iter().map(serde_json::to_value).collect::<Result<Vec<_>, _>>()?;
        let response = self.client.add_batch_of_requests(requests, forefront).await?;
        Ok(BatchAddRequestsResult {
            processed_requests: response
                .processed_requests
                .into_iter()
                .map(|processed| ProcessedRequest {
                    unique_key: processed.unique_key,
                    request_id: processed.request_id,
                    was_already_present: processed.was_already_present,
                    was_already_handled: processed.was_already_handled,
                })
                .collect(),
            unprocessed_requests: response
                .unprocessed_requests
                .into_iter()
                .map(|unprocessed| UnprocessedRequest {
                    unique_key: unprocessed.unique_key,
                    url: unprocessed.url,
                    method: unprocessed.method,
                })
                .collect(),
        })
    }

    async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>> {
        to_request(self.client.get_request(unique_key).await?)
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        to_request(self.client.fetch_next_request().await?)
    }

    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<Option<QueueOperationInfo>> {
        let processed = self.client.mark_request_as_handled(serde_json::to_value(request)?).await?;
        Ok(processed.map(operation_info))
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        let processed = self.client.reclaim_request(serde_json::to_value(request)?, forefront).await?;
        Ok(processed.map(operation_info))
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        Ok(self.client.is_empty().await)
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        Ok(self.client.is_finished().await)
    }

    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        self.client.set_expected_request_processing_time(chrono_duration(duration)?).await;
        Ok(())
    }

    async fn extend_request_processing_time(&self, request_id: &str, duration: Duration) -> StorageResult<bool> {
        Ok(self.client.prolong_request_lock(request_id, chrono_duration(duration)?).await?)
    }
}
