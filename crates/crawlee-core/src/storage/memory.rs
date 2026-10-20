//! In-memory storage backend, a port of `MemoryStorageBackend` from Crawlee for JS.
//!
//! State is guarded by synchronous mutexes that are never held across an `.await`, so operations
//! are short critical sections rather than an async queue.

use crate::local::CellExt as _;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::cell::RefCell;

use super::backend::*;
use crate::errors::{StorageError, StorageResult};
use crate::request::{Request, unique_key_to_request_id};

const DEFAULT_STORAGE_KEY: &str = "default";

fn now() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Clone, Debug)]
struct Timestamps {
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
    accessed_at: DateTime<Utc>,
}

impl Timestamps {
    fn new() -> Self {
        let now = now();
        Timestamps { created_at: now, modified_at: now, accessed_at: now }
    }

    fn touch(&mut self, modified: bool) {
        let now = now();
        self.accessed_at = now;
        if modified {
            self.modified_at = now;
        }
    }
}

/// Where a storage was opened from. `name` is `None` for run-scoped (default or aliased) storages.
#[derive(Clone, Debug)]
struct StorageKey {
    id: String,
    name: Option<String>,
    cache_key: String,
}

impl StorageKey {
    fn resolve(identifier: &StorageIdentifier) -> (bool, String) {
        match identifier {
            StorageIdentifier::Default => (true, DEFAULT_STORAGE_KEY.to_owned()),
            StorageIdentifier::Alias(alias) if alias == "__default__" => (true, DEFAULT_STORAGE_KEY.to_owned()),
            StorageIdentifier::Alias(alias) => (true, alias.clone()),
            StorageIdentifier::Name(name) => (false, name.clone()),
            StorageIdentifier::Id(id) => (false, id.clone()),
        }
    }

    fn matches(&self, key: &str) -> bool {
        self.id == key
            || self.name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(key))
            || self.cache_key.eq_ignore_ascii_case(key)
    }

    fn is_run_scoped(&self) -> bool {
        self.name.is_none() || self.name.as_deref() == Some(DEFAULT_STORAGE_KEY)
    }
}

type Registry<T> = RefCell<Vec<Rc<T>>>;

/// Keeps every storage in process memory. Useful for tests and for crawls whose results are
/// exported at the end.
#[derive(Default)]
pub struct MemoryStorageBackend {
    datasets: Registry<MemoryDataset>,
    key_value_stores: Registry<MemoryKeyValueStore>,
    request_queues: Registry<MemoryRequestQueue>,
}

impl MemoryStorageBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Implemented by every in-memory storage so the registry can find it and prune dropped ones.
trait Registered {
    fn key(&self) -> &StorageKey;
    fn is_dropped(&self) -> bool;
}

fn open<T: Registered>(
    registry: &Registry<T>,
    identifier: &StorageIdentifier,
    create: impl FnOnce(StorageKey) -> T,
) -> Rc<T> {
    let (is_alias, cache_key) = StorageKey::resolve(identifier);
    let mut entries = registry.borrow_mut();
    // A dropped storage is forgotten, so opening it again creates a fresh one.
    entries.retain(|entry| !entry.is_dropped());
    if let Some(found) = entries.iter().find(|entry| entry.key().matches(&cache_key)) {
        return found.clone();
    }
    let key = StorageKey {
        id: uuid::Uuid::new_v4().to_string(),
        name: if is_alias { None } else { Some(cache_key.clone()) },
        cache_key,
    };
    let created = Rc::new(create(key));
    entries.push(created.clone());
    created
}

#[async_trait(?Send)]
impl StorageBackend for MemoryStorageBackend {
    async fn create_dataset_backend(&self, id: &StorageIdentifier) -> StorageResult<Rc<dyn DatasetBackend>> {
        let backend: Rc<MemoryDataset> = open(&self.datasets, id, MemoryDataset::new);
        Ok(backend)
    }

    async fn create_key_value_store_backend(
        &self,
        id: &StorageIdentifier,
    ) -> StorageResult<Rc<dyn KeyValueStoreBackend>> {
        let backend: Rc<MemoryKeyValueStore> = open(&self.key_value_stores, id, MemoryKeyValueStore::new);
        Ok(backend)
    }

    async fn create_request_queue_backend(&self, id: &StorageIdentifier) -> StorageResult<Rc<dyn RequestQueueBackend>> {
        let backend: Rc<MemoryRequestQueue> = open(&self.request_queues, id, MemoryRequestQueue::new);
        Ok(backend)
    }

    async fn storage_exists(&self, id: &str, kind: StorageKind) -> StorageResult<bool> {
        Ok(match kind {
            StorageKind::Dataset => self.datasets.borrow_mut().iter().any(|d| d.key.id == id && !d.is_dropped()),
            StorageKind::KeyValueStore => {
                self.key_value_stores.borrow_mut().iter().any(|s| s.key.id == id && !s.is_dropped())
            }
            StorageKind::RequestQueue => {
                self.request_queues.borrow_mut().iter().any(|q| q.key.id == id && !q.is_dropped())
            }
        })
    }

    async fn purge(&self) -> StorageResult<()> {
        let datasets: Vec<_> = self.datasets.borrow_mut().iter().filter(|d| d.key.is_run_scoped()).cloned().collect();
        for dataset in datasets {
            dataset.purge_now();
        }
        let stores: Vec<_> =
            self.key_value_stores.borrow_mut().iter().filter(|s| s.key.is_run_scoped()).cloned().collect();
        for store in stores {
            store.purge_now();
        }
        let queues: Vec<_> =
            self.request_queues.borrow_mut().iter().filter(|q| q.key.is_run_scoped()).cloned().collect();
        for queue in queues {
            queue.purge_now();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------------------------

struct MemoryDataset {
    key: StorageKey,
    state: RefCell<DatasetState>,
}

struct DatasetState {
    items: Vec<DatasetItem>,
    timestamps: Timestamps,
    dropped: bool,
}

impl MemoryDataset {
    fn new(key: StorageKey) -> Self {
        MemoryDataset {
            key,
            state: RefCell::new(DatasetState { items: Vec::new(), timestamps: Timestamps::new(), dropped: false }),
        }
    }

    fn purge_now(&self) {
        let mut state = self.state.borrow_mut();
        state.items.clear();
        state.timestamps.touch(true);
    }
}

fn gone(kind: &str, key: &StorageKey) -> StorageError {
    StorageError::NotFound(format!("{kind} '{}' was dropped", key.name.as_deref().unwrap_or(&key.id)))
}

#[async_trait(?Send)]
impl DatasetBackend for MemoryDataset {
    async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
        let mut state = self.state.borrow_mut();
        if state.dropped {
            return Err(gone("Dataset", &self.key));
        }
        state.timestamps.touch(false);
        Ok(DatasetInfo {
            id: self.key.id.clone(),
            name: self.key.name.clone(),
            created_at: state.timestamps.created_at,
            modified_at: state.timestamps.modified_at,
            accessed_at: state.timestamps.accessed_at,
            item_count: state.items.len() as u64,
        })
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        let mut state = self.state.borrow_mut();
        state.dropped = true;
        state.items = Vec::new();
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        self.purge_now();
        Ok(())
    }

    async fn push_data(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
        let mut state = self.state.borrow_mut();
        if state.dropped {
            return Err(gone("Dataset", &self.key));
        }
        state.items.extend(items);
        state.timestamps.touch(true);
        Ok(())
    }

    async fn get_data(&self, options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>> {
        let mut state = self.state.borrow_mut();
        if state.dropped {
            return Err(gone("Dataset", &self.key));
        }
        state.timestamps.touch(false);
        let total = state.items.len();
        let limit = options.limit.unwrap_or(usize::MAX);
        let items: Vec<DatasetItem> = if options.desc {
            state.items.iter().rev().skip(options.offset).take(limit).cloned().collect()
        } else {
            state.items.iter().skip(options.offset).take(limit).cloned().collect()
        };
        Ok(PaginatedList {
            total,
            count: items.len(),
            offset: options.offset,
            limit: options.limit,
            desc: options.desc,
            items,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Key-value store
// ---------------------------------------------------------------------------------------------

struct MemoryKeyValueStore {
    key: StorageKey,
    state: RefCell<KeyValueStoreState>,
}

struct KeyValueStoreState {
    records: BTreeMap<String, KeyValueStoreRecord>,
    timestamps: Timestamps,
    dropped: bool,
}

impl MemoryKeyValueStore {
    fn new(key: StorageKey) -> Self {
        MemoryKeyValueStore {
            key,
            state: RefCell::new(KeyValueStoreState {
                records: BTreeMap::new(),
                timestamps: Timestamps::new(),
                dropped: false,
            }),
        }
    }

    fn purge_now(&self) {
        let mut state = self.state.borrow_mut();
        state.records.clear();
        state.timestamps.touch(true);
    }
}

#[async_trait(?Send)]
impl KeyValueStoreBackend for MemoryKeyValueStore {
    async fn get_metadata(&self) -> StorageResult<KeyValueStoreInfo> {
        let mut state = self.state.borrow_mut();
        if state.dropped {
            return Err(gone("Key-value store", &self.key));
        }
        state.timestamps.touch(false);
        Ok(KeyValueStoreInfo {
            id: self.key.id.clone(),
            name: self.key.name.clone(),
            created_at: state.timestamps.created_at,
            modified_at: state.timestamps.modified_at,
            accessed_at: state.timestamps.accessed_at,
        })
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        let mut state = self.state.borrow_mut();
        state.dropped = true;
        state.records.clear();
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        self.purge_now();
        Ok(())
    }

    async fn get_value(&self, key: &str) -> StorageResult<Option<KeyValueStoreRecord>> {
        let mut state = self.state.borrow_mut();
        state.timestamps.touch(false);
        Ok(state.records.get(key).cloned())
    }

    async fn set_value(&self, record: KeyValueStoreRecord) -> StorageResult<()> {
        let mut state = self.state.borrow_mut();
        if state.dropped {
            return Err(gone("Key-value store", &self.key));
        }
        state.records.insert(record.key.clone(), record);
        state.timestamps.touch(true);
        Ok(())
    }

    async fn delete_value(&self, key: &str) -> StorageResult<()> {
        let mut state = self.state.borrow_mut();
        state.records.remove(key);
        state.timestamps.touch(true);
        Ok(())
    }

    async fn list_keys(&self, options: KeyValueStoreListKeysOptions) -> StorageResult<KeyValueStoreListKeysResult> {
        let mut state = self.state.borrow_mut();
        state.timestamps.touch(false);
        let limit = options.limit.unwrap_or(1000).max(1);

        let mut matching = state
            .records
            .values()
            .filter(|record| options.prefix.as_deref().is_none_or(|prefix| record.key.starts_with(prefix)))
            .filter(|record| options.exclusive_start_key.as_deref().is_none_or(|start| record.key.as_str() > start));

        let items: Vec<KeyValueStoreItemData> = matching
            .by_ref()
            .take(limit)
            .map(|record| KeyValueStoreItemData {
                key: record.key.clone(),
                size: record.value.len(),
                content_type: record.content_type.clone().unwrap_or_else(|| "application/octet-stream".to_owned()),
            })
            .collect();
        let is_truncated = matching.next().is_some();

        Ok(KeyValueStoreListKeysResult {
            count: items.len(),
            limit,
            exclusive_start_key: options.exclusive_start_key,
            is_truncated,
            next_exclusive_start_key: if is_truncated { items.last().map(|item| item.key.clone()) } else { None },
            items,
        })
    }

    async fn record_exists(&self, key: &str) -> StorageResult<bool> {
        Ok(self.state.borrow_mut().records.contains_key(key))
    }
}

// ---------------------------------------------------------------------------------------------
// Request queue
// ---------------------------------------------------------------------------------------------

thread_local! {
    /// Order numbers come from a process-wide counter rather than timestamps, so ordering is
    /// deterministic. Regular requests get increasing positive numbers (FIFO); forefront requests
    /// get decreasing negative numbers, so the most recent forefront request is served first,
    /// exactly as with the `±Date.now()` order numbers in Crawlee for JS.
    static ORDER_COUNTER: Cell<i64> = const { Cell::new(1) };
}

fn next_order_no(forefront: bool) -> i64 {
    let n = ORDER_COUNTER.with(|counter| counter.fetch_add(1));
    if forefront { -n } else { n }
}

struct StoredRequest {
    /// `None` once handled.
    order_no: Option<i64>,
    request: Request,
}

struct QueueState {
    requests: HashMap<String, StoredRequest>,
    /// Unhandled requests (pending or in progress), keyed by order number.
    pending: BTreeMap<i64, String>,
    in_progress: HashSet<String>,
    handled_count: u64,
    timestamps: Timestamps,
    dropped: bool,
}

impl QueueState {
    fn next_pending(&self) -> Option<&String> {
        self.pending.values().find(|id| !self.in_progress.contains(*id))
    }
}

struct MemoryRequestQueue {
    key: StorageKey,
    state: RefCell<QueueState>,
}

impl MemoryRequestQueue {
    fn new(key: StorageKey) -> Self {
        MemoryRequestQueue {
            key,
            state: RefCell::new(QueueState {
                requests: HashMap::new(),
                pending: BTreeMap::new(),
                in_progress: HashSet::new(),
                handled_count: 0,
                timestamps: Timestamps::new(),
                dropped: false,
            }),
        }
    }

    fn purge_now(&self) {
        let mut state = self.state.borrow_mut();
        state.requests.clear();
        state.pending.clear();
        state.in_progress.clear();
        state.handled_count = 0;
        state.timestamps.touch(true);
    }
}

fn with_id(request: &Request, id: &str) -> StorageResult<Request> {
    if let Some(existing) = &request.id
        && existing != id
    {
        return Err(StorageError::InvalidArgument("Request ID does not match its uniqueKey.".to_owned()));
    }
    let mut request = request.clone();
    request.id = Some(id.to_owned());
    Ok(request)
}

#[async_trait(?Send)]
impl RequestQueueBackend for MemoryRequestQueue {
    async fn get_metadata(&self) -> StorageResult<RequestQueueInfo> {
        let mut state = self.state.borrow_mut();
        state.timestamps.touch(false);
        Ok(RequestQueueInfo {
            id: self.key.id.clone(),
            name: self.key.name.clone(),
            created_at: state.timestamps.created_at,
            modified_at: state.timestamps.modified_at,
            accessed_at: state.timestamps.accessed_at,
            total_request_count: state.requests.len() as u64,
            handled_request_count: state.handled_count,
            pending_request_count: state.pending.len() as u64,
        })
    }

    async fn drop_storage(&self) -> StorageResult<()> {
        self.purge_now();
        self.state.borrow_mut().dropped = true;
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        self.purge_now();
        Ok(())
    }

    async fn add_batch_of_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> StorageResult<BatchAddRequestsResult> {
        let mut state = self.state.borrow_mut();
        let mut result = BatchAddRequestsResult::default();

        for request in requests {
            let id = unique_key_to_request_id(&request.unique_key);
            if let Some(existing) = state.requests.get(&id) {
                result.processed_requests.push(ProcessedRequest {
                    unique_key: existing.request.unique_key.clone(),
                    request_id: id,
                    was_already_present: true,
                    was_already_handled: existing.order_no.is_none(),
                });
                continue;
            }

            let request = with_id(&request, &id)?;
            let order_no = if request.handled_at.is_some() { None } else { Some(next_order_no(forefront)) };
            match order_no {
                Some(order_no) => {
                    state.pending.insert(order_no, id.clone());
                }
                None => state.handled_count += 1,
            }
            result.processed_requests.push(ProcessedRequest {
                unique_key: request.unique_key.clone(),
                request_id: id.clone(),
                was_already_present: false,
                // Matches the platform API: newly added requests report `false` even when added
                // as handled.
                was_already_handled: false,
            });
            state.requests.insert(id, StoredRequest { order_no, request });
        }

        state.timestamps.touch(true);
        Ok(result)
    }

    async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>> {
        let id = unique_key_to_request_id(unique_key);
        let mut state = self.state.borrow_mut();
        state.timestamps.touch(false);
        Ok(state.requests.get(&id).map(|stored| stored.request.clone()))
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        let mut state = self.state.borrow_mut();
        state.timestamps.touch(false);
        let Some(id) = state.next_pending().cloned() else {
            return Ok(None);
        };
        state.in_progress.insert(id.clone());
        Ok(state.requests.get(&id).map(|stored| stored.request.clone()))
    }

    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<Option<QueueOperationInfo>> {
        let id = unique_key_to_request_id(&request.unique_key);
        let mut state = self.state.borrow_mut();
        let Some(existing) = state.requests.get(&id) else {
            return Ok(None);
        };
        let was_already_handled = existing.order_no.is_none();
        let previous_order_no = existing.order_no;

        let mut updated = with_id(request, &id)?;
        if updated.handled_at.is_none() {
            updated.handled_at = Some(crate::now_iso());
        }

        if let Some(order_no) = previous_order_no {
            state.pending.remove(&order_no);
        }
        state.in_progress.remove(&id);
        if !was_already_handled {
            state.handled_count += 1;
        }
        state.requests.insert(id.clone(), StoredRequest { order_no: None, request: updated });
        state.timestamps.touch(true);

        Ok(Some(QueueOperationInfo { request_id: id, was_already_present: true, was_already_handled }))
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        let id = unique_key_to_request_id(&request.unique_key);
        let mut state = self.state.borrow_mut();
        let Some(previous_order_no) = state.requests.get(&id).and_then(|existing| existing.order_no) else {
            return Ok(None);
        };

        let updated = with_id(request, &id)?;
        let order_no = next_order_no(forefront);
        state.pending.remove(&previous_order_no);
        state.pending.insert(order_no, id.clone());
        state.in_progress.remove(&id);
        state.requests.insert(id.clone(), StoredRequest { order_no: Some(order_no), request: updated });
        state.timestamps.touch(true);

        Ok(Some(QueueOperationInfo { request_id: id, was_already_present: true, was_already_handled: false }))
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        Ok(self.state.borrow_mut().next_pending().is_none())
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        Ok(self.state.borrow_mut().pending.is_empty())
    }
}

impl Registered for MemoryDataset {
    fn key(&self) -> &StorageKey {
        &self.key
    }
    fn is_dropped(&self) -> bool {
        self.state.borrow_mut().dropped
    }
}

impl Registered for MemoryKeyValueStore {
    fn key(&self) -> &StorageKey {
        &self.key
    }
    fn is_dropped(&self) -> bool {
        self.state.borrow_mut().dropped
    }
}

impl Registered for MemoryRequestQueue {
    fn key(&self) -> &StorageKey {
        &self.key
    }
    fn is_dropped(&self) -> bool {
        self.state.borrow_mut().dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn queue() -> Rc<dyn RequestQueueBackend> {
        MemoryStorageBackend::new().create_request_queue_backend(&StorageIdentifier::Default).await.unwrap()
    }

    #[tokio::test(flavor = "local")]
    async fn fifo_with_forefront() {
        let queue = queue().await;
        queue
            .add_batch_of_requests(vec![Request::new("https://a.dev/1"), Request::new("https://a.dev/2")], false)
            .await
            .unwrap();
        queue.add_batch_of_requests(vec![Request::new("https://a.dev/front")], true).await.unwrap();

        let order: Vec<String> = [
            queue.fetch_next_request().await.unwrap().unwrap(),
            queue.fetch_next_request().await.unwrap().unwrap(),
            queue.fetch_next_request().await.unwrap().unwrap(),
        ]
        .into_iter()
        .map(|r| r.url)
        .collect();
        assert_eq!(order, ["https://a.dev/front", "https://a.dev/1", "https://a.dev/2"]);
        assert!(queue.fetch_next_request().await.unwrap().is_none());
    }

    #[tokio::test(flavor = "local")]
    async fn dedup_handle_reclaim_and_finish() {
        let queue = queue().await;
        let added = queue
            .add_batch_of_requests(vec![Request::new("https://a.dev/x"), Request::new("https://a.dev/x/")], false)
            .await
            .unwrap();
        assert!(!added.processed_requests[0].was_already_present);
        assert!(added.processed_requests[1].was_already_present);

        let fetched = queue.fetch_next_request().await.unwrap().unwrap();
        assert!(queue.is_empty().await.unwrap());
        assert!(!queue.is_finished().await.unwrap());

        queue.reclaim_request(&fetched, false).await.unwrap().unwrap();
        let fetched = queue.fetch_next_request().await.unwrap().unwrap();
        let info = queue.mark_request_as_handled(&fetched).await.unwrap().unwrap();
        assert!(!info.was_already_handled);
        assert!(queue.is_finished().await.unwrap());

        let stored = queue.get_request(&fetched.unique_key).await.unwrap().unwrap();
        assert!(stored.handled_at.is_some());
        assert_eq!(queue.get_metadata().await.unwrap().handled_request_count, 1);

        let again = queue.add_batch_of_requests(vec![Request::new("https://a.dev/x")], false).await.unwrap();
        assert!(again.processed_requests[0].was_already_handled);
    }

    #[tokio::test(flavor = "local")]
    async fn named_storages_are_shared_and_default_is_purged() {
        let backend = MemoryStorageBackend::new();
        let a = backend.create_dataset_backend(&StorageIdentifier::name("results")).await.unwrap();
        let b = backend.create_dataset_backend(&StorageIdentifier::name("RESULTS")).await.unwrap();
        let default = backend.create_dataset_backend(&StorageIdentifier::Default).await.unwrap();
        let item = serde_json::value::to_raw_value(&serde_json::json!({ "a": 1 })).unwrap();
        a.push_data(vec![item.clone()]).await.unwrap();
        default.push_data(vec![item]).await.unwrap();
        assert_eq!(b.get_metadata().await.unwrap().item_count, 1);

        backend.purge().await.unwrap();
        assert_eq!(default.get_metadata().await.unwrap().item_count, 0);
        assert_eq!(a.get_metadata().await.unwrap().item_count, 1);
    }
}
