//! [`Dataset`]: an append-only table of JSON objects, typically the crawl results.

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

use super::backend::{DatasetBackend, DatasetInfo, DatasetItem, DatasetListOptions, StorageBackend, StorageIdentifier};
use crate::errors::{StorageError, StorageResult};
use crate::services::CacheLink;

/// Page size of the dataset iterators and exports.
pub const DATASET_ITERATORS_DEFAULT_LIMIT: usize = 10_000;

/// Handle to a dataset. Cloning is cheap and clones share the storage.
#[derive(Clone)]
pub struct Dataset {
    backend: Arc<dyn DatasetBackend>,
    cache_link: CacheLink,
}

impl std::fmt::Debug for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dataset").finish_non_exhaustive()
    }
}

/// A page of typed dataset items.
#[derive(Clone, Debug)]
pub struct DatasetPage<T> {
    pub total: usize,
    pub offset: usize,
    pub items: Vec<T>,
}

impl Dataset {
    /// Opens a new handle on `storage`, outside the instance cache of [`Services`](crate::Services);
    /// [`Services::open_dataset`](crate::Services::open_dataset)
    /// returns the same instance on every call.
    pub async fn open(storage: &dyn StorageBackend, id: &StorageIdentifier) -> StorageResult<Self> {
        Ok(Dataset::from_backend(storage.create_dataset_backend(id).await?))
    }

    pub fn from_backend(backend: Arc<dyn DatasetBackend>) -> Self {
        Dataset { backend, cache_link: CacheLink::default() }
    }

    pub(crate) fn with_cache_link(mut self, link: CacheLink) -> Self {
        self.cache_link = link;
        self
    }

    pub fn backend(&self) -> &Arc<dyn DatasetBackend> {
        &self.backend
    }

    /// Whether two handles point at the same storage.
    pub fn same_storage(&self, other: &Dataset) -> bool {
        Arc::ptr_eq(&self.backend, &other.backend)
    }

    /// Appends one object, or every object of an array.
    pub async fn push_data<T: Serialize + ?Sized>(&self, data: &T) -> StorageResult<()> {
        self.backend.push_data(serialize_items(data)?).await
    }

    /// Appends already-serialized items.
    pub async fn push_items(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
        self.backend.push_data(items).await
    }

    pub async fn get_data<T: DeserializeOwned>(&self, options: DatasetListOptions) -> StorageResult<DatasetPage<T>> {
        let page = self.backend.get_data(options).await?;
        let items = page.items.iter().map(|item| serde_json::from_str(item.get())).collect::<Result<Vec<T>, _>>()?;
        Ok(DatasetPage { total: page.total, offset: page.offset, items })
    }

    /// All items, fetched in pages of [`DATASET_ITERATORS_DEFAULT_LIMIT`].
    pub async fn get_all<T: DeserializeOwned>(&self) -> StorageResult<Vec<T>> {
        let mut out = Vec::new();
        loop {
            let page: DatasetPage<T> = self
                .get_data(DatasetListOptions {
                    offset: out.len(),
                    limit: Some(DATASET_ITERATORS_DEFAULT_LIMIT),
                    desc: false,
                })
                .await?;
            let received = page.items.len();
            out.extend(page.items);
            if received < DATASET_ITERATORS_DEFAULT_LIMIT || out.len() >= page.total {
                return Ok(out);
            }
        }
    }

    pub async fn get_info(&self) -> StorageResult<DatasetInfo> {
        self.backend.get_metadata().await
    }

    pub async fn drop_storage(self) -> StorageResult<()> {
        self.backend.drop_storage().await?;
        self.cache_link.evict(Arc::as_ptr(&self.backend) as *const ());
        Ok(())
    }
}

/// Serializes one object or an array of objects into dataset items, rejecting anything else.
pub fn serialize_items<T: Serialize + ?Sized>(data: &T) -> StorageResult<Vec<DatasetItem>> {
    let raw = serde_json::value::to_raw_value(data)?;
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'{') => Ok(vec![raw]),
        Some(b'[') => {
            let items: Vec<Box<RawValue>> = serde_json::from_str(text)?;
            if let Some(bad) = items.iter().find(|item| !item.get().starts_with('{')) {
                return Err(not_an_object(bad.get()));
            }
            Ok(items)
        }
        _ => Err(not_an_object(text)),
    }
}

fn not_an_object(json: &str) -> StorageError {
    let preview: String = json.chars().take(50).collect();
    StorageError::InvalidArgument(format!("Dataset items must be JSON objects, got: {preview}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryStorageBackend;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Item {
        title: String,
    }

    #[tokio::test]
    async fn push_and_read_typed_items() {
        let storage = MemoryStorageBackend::new();
        let dataset = Dataset::open(&storage, &StorageIdentifier::Default).await.unwrap();
        dataset.push_data(&Item { title: "a".into() }).await.unwrap();
        dataset.push_data(&json!([{ "title": "b" }, { "title": "c" }])).await.unwrap();

        let all: Vec<Item> = dataset.get_all().await.unwrap();
        assert_eq!(all.iter().map(|i| i.title.as_str()).collect::<Vec<_>>(), ["a", "b", "c"]);

        let page: DatasetPage<Item> =
            dataset.get_data(DatasetListOptions { offset: 1, limit: Some(1), desc: true }).await.unwrap();
        assert_eq!(page.items, vec![Item { title: "b".into() }]);
        assert_eq!(page.total, 3);
    }

    /// Records the options of every `get_data` call.
    struct RecordingDataset {
        inner: Arc<dyn DatasetBackend>,
        calls: parking_lot::Mutex<Vec<DatasetListOptions>>,
    }

    #[async_trait::async_trait]
    impl DatasetBackend for RecordingDataset {
        async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
            self.inner.get_metadata().await
        }
        async fn drop_storage(&self) -> StorageResult<()> {
            self.inner.drop_storage().await
        }
        async fn purge(&self) -> StorageResult<()> {
            self.inner.purge().await
        }
        async fn push_data(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
            self.inner.push_data(items).await
        }
        async fn get_data(
            &self,
            options: DatasetListOptions,
        ) -> StorageResult<crate::storage::backend::PaginatedList<DatasetItem>> {
            self.calls.lock().push(options);
            self.inner.get_data(options).await
        }
    }

    async fn recording(storage: &MemoryStorageBackend) -> (Dataset, Arc<RecordingDataset>) {
        let inner = storage.create_dataset_backend(&StorageIdentifier::Default).await.unwrap();
        let backend = Arc::new(RecordingDataset { inner, calls: parking_lot::Mutex::new(Vec::new()) });
        (Dataset::from_backend(backend.clone()), backend)
    }

    #[tokio::test]
    async fn get_all_reads_pages_of_ten_thousand_items() {
        let storage = MemoryStorageBackend::new();
        let (dataset, backend) = recording(&storage).await;
        let items: Vec<_> = (0..10_001).map(|i| json!({ "i": i })).collect();
        dataset.push_data(&items).await.unwrap();

        assert_eq!(dataset.get_all::<serde_json::Value>().await.unwrap().len(), 10_001);
        let limits: Vec<_> = backend.calls.lock().iter().map(|call| (call.offset, call.limit)).collect();
        assert_eq!(limits, [(0, Some(10_000)), (10_000, Some(10_000))]);
    }

    #[test]
    fn rejects_non_objects() {
        assert!(serialize_items(&json!(1)).is_err());
        assert!(serialize_items(&json!([{ "a": 1 }, 2])).is_err());
        assert!(serialize_items(&"text").is_err());
    }
}
