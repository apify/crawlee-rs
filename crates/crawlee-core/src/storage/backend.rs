//! The storage backend contract, mirroring `StorageBackend` in `@crawlee/types`.
//!
//! A backend is four traits: a factory ([`StorageBackend`]) and one trait per storage type. The
//! traits move bytes and already-serialized JSON; parsing and serialization happen in the
//! frontends ([`Dataset`](crate::Dataset), [`KeyValueStore`](crate::KeyValueStore),
//! [`RequestQueue`](crate::RequestQueue)). That keeps the hot path copy-free: a dataset item is
//! serialized once, in the request handler, and a backend that talks to an HTTP API (such as the
//! Apify platform) can send those bytes as they are.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::errors::StorageResult;
use crate::request::Request;
use crate::storage::dataset_buffer::FlushCounters;

/// Identifies a storage. The default storage is run-scoped (emptied on start), as are aliased
/// storages; named storages persist across runs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum StorageIdentifier {
    #[default]
    Default,
    Id(String),
    Name(String),
    Alias(String),
}

impl StorageIdentifier {
    pub fn id(id: impl Into<String>) -> Self {
        StorageIdentifier::Id(id.into())
    }

    pub fn name(name: impl Into<String>) -> Self {
        StorageIdentifier::Name(name.into())
    }

    pub fn alias(alias: impl Into<String>) -> Self {
        StorageIdentifier::Alias(alias.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StorageKind {
    Dataset,
    KeyValueStore,
    RequestQueue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetInfo {
    pub id: String,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub accessed_at: DateTime<Utc>,
    pub item_count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValueStoreInfo {
    pub id: String,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub accessed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestQueueInfo {
    pub id: String,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub accessed_at: DateTime<Utc>,
    pub total_request_count: u64,
    pub handled_request_count: u64,
    pub pending_request_count: u64,
}

/// Options of [`DatasetBackend::get_data`] (`DatasetDataOptions` in JS): offset pagination, and
/// item transformations that a backend applies if it can. The Apify platform does; the in-memory
/// and file-system backends ignore them, as in JS.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DatasetListOptions {
    pub offset: usize,
    /// `None` means no limit.
    pub limit: Option<usize>,
    pub desc: bool,
    /// Only these fields of each item, in this order.
    pub fields: Option<Vec<String>>,
    /// The field to unwind: an array becomes one item per element, an object is merged into
    /// the item.
    pub unwind: Option<String>,
    /// Shortcut for `skip_hidden` and `skip_empty`.
    pub clean: bool,
    /// Leaves out the fields whose names start with `#`.
    pub skip_hidden: bool,
    /// Leaves out empty items (a page can then hold fewer items than the limit).
    pub skip_empty: bool,
}

/// One page of dataset items.
#[derive(Clone, Debug)]
pub struct PaginatedList<T> {
    pub total: usize,
    pub count: usize,
    pub offset: usize,
    pub limit: Option<usize>,
    pub desc: bool,
    pub items: Vec<T>,
}

/// A dataset item as JSON text. Backends store it verbatim.
pub type DatasetItem = Box<RawValue>;

#[async_trait]
pub trait DatasetBackend: Send + Sync {
    async fn get_metadata(&self) -> StorageResult<DatasetInfo>;
    /// Removes the dataset and its items.
    async fn drop_storage(&self) -> StorageResult<()>;
    /// Removes all items, keeping the dataset.
    async fn purge(&self) -> StorageResult<()>;
    /// Appends items. Every item is a serialized JSON object.
    async fn push_data(&self, items: Vec<DatasetItem>) -> StorageResult<()>;
    async fn get_data(&self, options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>>;

    /// Called by a buffered dataset (see [`dataset_buffer`](super::dataset_buffer)) when it takes
    /// `items` into its buffer, in the task that pushes them. A backend that accounts for what it
    /// stores (the Apify SDK's, which charges for items) may keep only some of them, the first
    /// ones, and return counts that come back, summed over the pushes of the flush, in
    /// [`push_buffered`](Self::push_buffered). By default, every item is kept and nothing counted.
    fn admit_buffered(&self, items: &mut Vec<DatasetItem>) -> FlushCounters {
        let _ = items;
        FlushCounters::default()
    }

    /// Stores the items of a flush of a buffered dataset, which [`admit_buffered`](Self::admit_buffered)
    /// admitted with `counters`. By default, [`push_data`](Self::push_data).
    async fn push_buffered(&self, items: Vec<DatasetItem>, counters: FlushCounters) -> StorageResult<()> {
        let _ = counters;
        self.push_data(items).await
    }
}

/// A key-value store record as the backend stores it: raw bytes plus a content type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyValueStoreRecord {
    pub key: String,
    pub value: Bytes,
    pub content_type: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyValueStoreListKeysOptions {
    pub prefix: Option<String>,
    pub exclusive_start_key: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValueStoreItemData {
    pub key: String,
    pub size: usize,
    pub content_type: String,
}

/// One page of keys, with a cursor for the next page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyValueStoreListKeysResult {
    pub items: Vec<KeyValueStoreItemData>,
    pub count: usize,
    pub limit: usize,
    pub exclusive_start_key: Option<String>,
    pub is_truncated: bool,
    pub next_exclusive_start_key: Option<String>,
}

#[async_trait]
pub trait KeyValueStoreBackend: Send + Sync {
    async fn get_metadata(&self) -> StorageResult<KeyValueStoreInfo>;
    async fn drop_storage(&self) -> StorageResult<()>;
    async fn purge(&self) -> StorageResult<()>;
    /// Returns the raw record, never parsing it.
    async fn get_value(&self, key: &str) -> StorageResult<Option<KeyValueStoreRecord>>;
    async fn set_value(&self, record: KeyValueStoreRecord) -> StorageResult<()>;
    async fn delete_value(&self, key: &str) -> StorageResult<()>;
    async fn list_keys(&self, options: KeyValueStoreListKeysOptions) -> StorageResult<KeyValueStoreListKeysResult>;
    /// The public URL a record with this key has, derived from the key only.
    async fn get_public_url(&self, _key: &str) -> StorageResult<Option<String>> {
        Ok(None)
    }
    async fn record_exists(&self, key: &str) -> StorageResult<bool>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedRequest {
    pub unique_key: String,
    pub request_id: String,
    pub was_already_present: bool,
    pub was_already_handled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnprocessedRequest {
    pub unique_key: String,
    pub url: String,
    pub method: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchAddRequestsResult {
    pub processed_requests: Vec<ProcessedRequest>,
    pub unprocessed_requests: Vec<UnprocessedRequest>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueOperationInfo {
    pub request_id: String,
    pub was_already_present: bool,
    pub was_already_handled: bool,
}

/// Operations on one request queue. The backend owns all bookkeeping (pending, in progress,
/// handled) and any locking needed when several clients share a queue.
#[async_trait]
pub trait RequestQueueBackend: Send + Sync {
    async fn get_metadata(&self) -> StorageResult<RequestQueueInfo>;
    async fn drop_storage(&self) -> StorageResult<()>;
    async fn purge(&self) -> StorageResult<()>;
    /// Adds requests, deduplicated by `unique_key`.
    async fn add_batch_of_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> StorageResult<BatchAddRequestsResult>;
    async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>>;
    /// Returns the next pending request and marks it in progress.
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>>;
    /// Returns `None` when the request is not known to the queue.
    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<Option<QueueOperationInfo>>;
    /// Returns an in-progress request to the queue. Returns `None` when there was nothing to reclaim.
    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>>;
    /// No request can be fetched right now (in-progress requests are not counted).
    async fn is_empty(&self) -> StorageResult<bool>;
    /// Nothing pending and nothing in progress.
    async fn is_finished(&self) -> StorageResult<bool>;
    /// Sizing hint for backends that lock fetched requests.
    async fn set_expected_request_processing_time(&self, _duration: Duration) -> StorageResult<()> {
        Ok(())
    }
    /// Prolongs the lock of a fetched request. `false` means the backend does not hold a lock.
    async fn extend_request_processing_time(&self, _request_id: &str, _duration: Duration) -> StorageResult<bool> {
        Ok(false)
    }
}

/// Factory of storage backends: opens (or creates) a storage by its identifier.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn create_dataset_backend(&self, id: &StorageIdentifier) -> StorageResult<Arc<dyn DatasetBackend>>;
    async fn create_key_value_store_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Arc<dyn KeyValueStoreBackend>>;
    async fn create_request_queue_backend(&self, id: &StorageIdentifier)
    -> StorageResult<Arc<dyn RequestQueueBackend>>;
    async fn storage_exists(&self, _id: &str, _kind: StorageKind) -> StorageResult<bool> {
        Ok(false)
    }
    /// Empties run-scoped storages (the default one and aliased ones).
    async fn purge(&self) -> StorageResult<()> {
        Ok(())
    }
    async fn teardown(&self) -> StorageResult<()> {
        Ok(())
    }
    /// Number of rate-limit errors the backend has hit, for the storage load signal.
    fn rate_limit_errors(&self) -> u64 {
        0
    }
}
