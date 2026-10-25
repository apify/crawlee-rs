//! [`Dataset`]: an append-only table of JSON objects, typically the crawl results.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use std::future::Future;

use bytes::Bytes;
use indexmap::IndexSet;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::backend::{DatasetBackend, DatasetInfo, DatasetItem, DatasetListOptions, StorageBackend, StorageIdentifier};
use super::dataset_buffer::{DatasetBuffer, DatasetBufferOptions};
use crate::errors::{StorageError, StorageResult};
use crate::events::EventManager;
use crate::services::CacheLink;
use crate::storage::key_value_store::KeyValueStore;

/// Page size of the dataset iterators and exports.
pub const DATASET_ITERATORS_DEFAULT_LIMIT: usize = 10_000;

/// Handle to a dataset. Cloning is cheap and clones share the storage.
#[derive(Clone)]
pub struct Dataset {
    backend: Arc<dyn DatasetBackend>,
    cache_link: CacheLink,
    counters: Arc<DatasetCounters>,
    /// Set for buffered writes (see [`with_buffer`](Self::with_buffer)).
    buffer: Option<Arc<DatasetBuffer>>,
}

/// The operations a dataset handle (and its clones) issued to the backend, counted per backend
/// call (`DatasetStats` in JS).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DatasetStats {
    /// `get_data` calls.
    pub read_count: u64,
    /// `push_data` calls.
    pub write_count: u64,
}

#[derive(Default)]
pub(crate) struct DatasetCounters {
    read: AtomicU64,
    pub(crate) write: AtomicU64,
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
        Dataset { backend, cache_link: CacheLink::default(), counters: Arc::default(), buffer: None }
    }

    /// This handle (and its clones) with buffered writes, an extra of crawlee-rs (see
    /// [`dataset_buffer`](super::dataset_buffer)): pushes are collected and stored together, as
    /// `options` says. With `events`, the buffer is also flushed on their `PersistState`,
    /// `Migrating`, `Aborting` and `Exit` events. Other handles of the same storage are not
    /// buffered. [`Services`](crate::Services) buffers the datasets it opens when
    /// [`Configuration::dataset_buffer`](crate::Configuration::dataset_buffer) is set.
    pub fn with_buffer(mut self, options: DatasetBufferOptions, events: Option<&EventManager>) -> Self {
        self.buffer = Some(DatasetBuffer::new(self.backend.clone(), options, self.counters.clone(), events));
        self
    }

    /// The settings of the buffered writes of this handle, if they are buffered.
    pub fn buffer_options(&self) -> Option<&DatasetBufferOptions> {
        self.buffer.as_ref().map(|buffer| buffer.options())
    }

    /// Stores the items buffered so far (see [`with_buffer`](Self::with_buffer)). Does nothing
    /// for a dataset that is not buffered.
    pub async fn flush(&self) -> StorageResult<()> {
        match &self.buffer {
            Some(buffer) => buffer.flush().await,
            None => Ok(()),
        }
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

    pub fn stats(&self) -> DatasetStats {
        DatasetStats {
            read_count: self.counters.read.load(Ordering::Relaxed),
            write_count: self.counters.write.load(Ordering::Relaxed),
        }
    }

    /// Appends one object, or every object of an array.
    pub async fn push_data<T: Serialize + ?Sized>(&self, data: &T) -> StorageResult<()> {
        self.push_items(serialize_items(data)?).await
    }

    /// Appends already-serialized items. A buffered dataset (see [`with_buffer`](Self::with_buffer))
    /// may return before they are stored.
    pub async fn push_items(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
        if let Some(buffer) = &self.buffer {
            return buffer.push(items).await;
        }
        self.counters.write.fetch_add(1, Ordering::Relaxed);
        self.backend.push_data(items).await
    }

    /// Reads a page. A buffered dataset stores its buffered items first.
    pub async fn get_data<T: DeserializeOwned>(&self, options: DatasetListOptions) -> StorageResult<DatasetPage<T>> {
        self.flush().await?;
        self.counters.read.fetch_add(1, Ordering::Relaxed);
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
                    ..Default::default()
                })
                .await?;
            let received = page.items.len();
            out.extend(page.items);
            if received < DATASET_ITERATORS_DEFAULT_LIMIT || out.len() >= page.total {
                return Ok(out);
            }
        }
    }

    /// Calls `f` with every item from `options.offset` on, and its index. As in JS
    /// (`Dataset.forEach`), `options.limit` is the page size (default
    /// [`DATASET_ITERATORS_DEFAULT_LIMIT`]), not a cap on the number of items.
    pub async fn for_each<T, F, Fut, E>(&self, options: DatasetListOptions, mut f: F) -> Result<(), E>
    where
        T: DeserializeOwned,
        F: FnMut(T, usize) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: From<StorageError>,
    {
        let mut pages = IterationPages::new(options);
        let mut index = 0;
        while let Some(items) = pages.next::<T>(self).await? {
            for item in items {
                f(item, index).await?;
                index += 1;
            }
        }
        Ok(())
    }

    /// The results of `f` for every item, as [`for_each`](Self::for_each) visits them.
    pub async fn map<T, R, F, Fut, E>(&self, options: DatasetListOptions, mut f: F) -> Result<Vec<R>, E>
    where
        T: DeserializeOwned,
        F: FnMut(T, usize) -> Fut,
        Fut: Future<Output = Result<R, E>>,
        E: From<StorageError>,
    {
        let mut pages = IterationPages::new(options);
        let mut out = Vec::new();
        while let Some(items) = pages.next::<T>(self).await? {
            for item in items {
                let index = out.len();
                out.push(f(item, index).await?);
            }
        }
        Ok(out)
    }

    /// Folds the items into `memo`, as [`for_each`](Self::for_each) visits them.
    pub async fn reduce<T, M, F, Fut, E>(&self, options: DatasetListOptions, mut memo: M, mut f: F) -> Result<M, E>
    where
        T: DeserializeOwned,
        F: FnMut(M, T, usize) -> Fut,
        Fut: Future<Output = Result<M, E>>,
        E: From<StorageError>,
    {
        let mut pages = IterationPages::new(options);
        let mut index = 0;
        while let Some(items) = pages.next::<T>(self).await? {
            for item in items {
                memo = f(memo, item, index).await?;
                index += 1;
            }
        }
        Ok(memo)
    }

    /// The items from `options.offset` on, at most `options.limit` of them, fetched in pages of
    /// [`DATASET_ITERATORS_DEFAULT_LIMIT`] (`Dataset.values` in JS).
    pub async fn values<T: DeserializeOwned>(&self, options: DatasetListOptions) -> StorageResult<Vec<T>> {
        let mut out = Vec::new();
        let mut offset = options.offset;
        loop {
            let fetch_limit = match options.limit {
                Some(limit) => DATASET_ITERATORS_DEFAULT_LIMIT.min(limit - out.len()),
                None => DATASET_ITERATORS_DEFAULT_LIMIT,
            };
            if fetch_limit == 0 {
                return Ok(out);
            }
            let page: DatasetPage<T> =
                self.get_data(DatasetListOptions { offset, limit: Some(fetch_limit), ..options.clone() }).await?;
            let received = page.items.len();
            out.extend(page.items);
            if received < fetch_limit || offset + received >= page.total {
                return Ok(out);
            }
            offset += received;
        }
    }

    /// Like [`values`](Self::values), each item with its index (starting at `options.offset`).
    pub async fn entries<T: DeserializeOwned>(&self, options: DatasetListOptions) -> StorageResult<Vec<(usize, T)>> {
        let offset = options.offset;
        Ok(self.values(options).await?.into_iter().enumerate().map(|(i, item)| (offset + i, item)).collect())
    }

    /// Every item, as `Dataset.export` in JS.
    pub async fn export<T: DeserializeOwned>(&self, options: &DatasetExportOptions) -> StorageResult<Vec<T>> {
        self.values(options.list_options()).await
    }

    /// Exports every item to `store` under `key` as `application/json` or `text/csv`, and returns
    /// them (`Dataset.exportTo` in JS, whose `toKVS` option is the `store` here).
    pub async fn export_to(
        &self,
        key: &str,
        store: &KeyValueStore,
        options: DatasetExportOptions,
        content_type: &str,
    ) -> StorageResult<Vec<Value>> {
        let items: Vec<Value> = match content_type {
            "text/csv" | "application/json" => self.export(&options).await?,
            _ => return Err(StorageError::InvalidArgument(format!("Unsupported content type: {content_type}"))),
        };
        if content_type == "application/json" {
            store.set_value(key, &items).await?;
        } else {
            store.set_bytes(key, Bytes::from(to_csv(&items, options.collect_all_keys)), Some(content_type)).await?;
        }
        Ok(items)
    }

    pub async fn export_to_json(
        &self,
        key: &str,
        store: &KeyValueStore,
        options: DatasetExportOptions,
    ) -> StorageResult<Vec<Value>> {
        self.export_to(key, store, options, "application/json").await
    }

    pub async fn export_to_csv(
        &self,
        key: &str,
        store: &KeyValueStore,
        options: DatasetExportOptions,
    ) -> StorageResult<Vec<Value>> {
        self.export_to(key, store, options, "text/csv").await
    }

    /// Removes every item, keeping the dataset.
    pub async fn purge(&self) -> StorageResult<()> {
        // Buffered items are stored first, so that a purge removes them as it would unbuffered.
        self.flush().await?;
        self.backend.purge().await
    }

    pub async fn get_info(&self) -> StorageResult<DatasetInfo> {
        self.flush().await?;
        self.backend.get_metadata().await
    }

    pub async fn drop_storage(self) -> StorageResult<()> {
        // Settles the buffered pushes, which requests may wait for.
        let _ = self.flush().await;
        self.backend.drop_storage().await?;
        self.cache_link.evict(Arc::as_ptr(&self.backend) as *const ());
        Ok(())
    }
}

/// The pages [`Dataset::for_each`] walks: `limit` items at a time from `offset`, until
/// `offset + limit` reaches the total.
struct IterationPages {
    options: Option<DatasetListOptions>,
}

impl IterationPages {
    fn new(mut options: DatasetListOptions) -> Self {
        options.limit = Some(options.limit.filter(|&limit| limit > 0).unwrap_or(DATASET_ITERATORS_DEFAULT_LIMIT));
        IterationPages { options: Some(options) }
    }

    async fn next<T: DeserializeOwned>(&mut self, dataset: &Dataset) -> StorageResult<Option<Vec<T>>> {
        let Some(options) = self.options.take() else {
            return Ok(None);
        };
        let page_size = options.limit.unwrap_or(DATASET_ITERATORS_DEFAULT_LIMIT);
        let page: DatasetPage<T> = dataset.get_data(options.clone()).await?;
        let offset = page.offset + page_size;
        if offset < page.total {
            self.options = Some(DatasetListOptions { offset, ..options });
        }
        Ok(Some(page.items))
    }
}

/// Options of [`Dataset::export`] and [`Dataset::export_to`] (`DatasetExportOptions` in JS).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DatasetExportOptions {
    pub desc: bool,
    pub fields: Option<Vec<String>>,
    pub unwind: Option<String>,
    pub clean: bool,
    pub skip_hidden: bool,
    pub skip_empty: bool,
    /// The CSV columns are the keys of all items, not only of the first one.
    pub collect_all_keys: bool,
}

impl DatasetExportOptions {
    fn list_options(&self) -> DatasetListOptions {
        DatasetListOptions {
            desc: self.desc,
            fields: self.fields.clone(),
            unwind: self.unwind.clone(),
            clean: self.clean,
            skip_hidden: self.skip_hidden,
            skip_empty: self.skip_empty,
            ..Default::default()
        }
    }
}

/// Items as CSV, as `csv-stringify` writes them with its defaults: a header row, `,` between
/// fields, `\n` after every row, a field quoted when it holds `,`, `"`, `\n` or `\r`, `true` as
/// `1`, `false`, `null` and missing fields empty, and objects as JSON.
fn to_csv(items: &[Value], collect_all_keys: bool) -> String {
    let Some(first) = items.first() else {
        return String::new();
    };
    let mut keys: IndexSet<&str> = IndexSet::new();
    for item in if collect_all_keys { items } else { std::slice::from_ref(first) } {
        if let Some(object) = item.as_object() {
            keys.extend(object.keys().map(String::as_str));
        }
    }
    let mut out = String::new();
    let mut write_row = |fields: &mut dyn Iterator<Item = String>| {
        for (i, field) in fields.enumerate() {
            if i > 0 {
                out.push(',');
            }
            if field.contains([',', '"', '\n', '\r']) {
                out.push('"');
                out.push_str(&field.replace('"', "\"\""));
                out.push('"');
            } else {
                out.push_str(&field);
            }
        }
        out.push('\n');
    };
    write_row(&mut keys.iter().map(|key| (*key).to_owned()));
    for item in items {
        write_row(&mut keys.iter().map(|key| match item.get(*key) {
            None | Some(Value::Null) | Some(Value::Bool(false)) => String::new(),
            Some(Value::Bool(true)) => "1".to_owned(),
            Some(Value::String(text)) => text.clone(),
            Some(Value::Number(number)) => number.to_string(),
            Some(other) => other.to_string(),
        }));
    }
    out
}

/// Serializes one object or an array of objects into dataset items, rejecting anything else. Each
/// element of a sequence is serialized on its own, so that items are never serialized as one
/// array and parsed again to split them.
pub fn serialize_items<T: Serialize + ?Sized>(data: &T) -> StorageResult<Vec<DatasetItem>> {
    let items = match data.serialize(SequenceItems) {
        Ok(items) => items,
        Err(SequenceError::NotASequence) => vec![serde_json::value::to_raw_value(data)?],
        Err(SequenceError::Json(err)) => return Err(err.into()),
    };
    if let Some(bad) = items.iter().find(|item| !item.get().starts_with('{')) {
        return Err(not_an_object(bad.get()));
    }
    Ok(items)
}

/// Serializes the elements of a sequence (a slice, a `Vec`, a tuple, `Value::Array`...) into one
/// dataset item each. Anything else is [`SequenceError::NotASequence`], to be serialized whole.
struct SequenceItems;

#[derive(Debug)]
enum SequenceError {
    NotASequence,
    Json(serde_json::Error),
}

impl std::fmt::Display for SequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequenceError::NotASequence => f.write_str("not a sequence"),
            SequenceError::Json(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for SequenceError {}

impl serde::ser::Error for SequenceError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        SequenceError::Json(serde_json::Error::custom(msg))
    }
}

/// The items of a sequence, serialized one by one.
struct SequenceCollector(Vec<DatasetItem>);

impl SequenceCollector {
    fn push<T: Serialize + ?Sized>(&mut self, element: &T) -> Result<(), SequenceError> {
        self.0.push(serde_json::value::to_raw_value(element).map_err(SequenceError::Json)?);
        Ok(())
    }
}

impl serde::ser::SerializeSeq for SequenceCollector {
    type Ok = Vec<DatasetItem>;
    type Error = SequenceError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, element: &T) -> Result<(), SequenceError> {
        self.push(element)
    }
    fn end(self) -> Result<Vec<DatasetItem>, SequenceError> {
        Ok(self.0)
    }
}

impl serde::ser::SerializeTuple for SequenceCollector {
    type Ok = Vec<DatasetItem>;
    type Error = SequenceError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, element: &T) -> Result<(), SequenceError> {
        self.push(element)
    }
    fn end(self) -> Result<Vec<DatasetItem>, SequenceError> {
        Ok(self.0)
    }
}

/// Every value that is not a sequence, nor a wrapper around one.
macro_rules! not_a_sequence {
    ($($method:ident($($ty:ty),*)),* $(,)?) => {
        $(fn $method(self, $(_: $ty),*) -> Result<Vec<DatasetItem>, SequenceError> {
            Err(SequenceError::NotASequence)
        })*
    };
}

impl serde::Serializer for SequenceItems {
    type Ok = Vec<DatasetItem>;
    type Error = SequenceError;
    type SerializeSeq = SequenceCollector;
    type SerializeTuple = SequenceCollector;
    type SerializeTupleStruct = serde::ser::Impossible<Vec<DatasetItem>, SequenceError>;
    type SerializeTupleVariant = serde::ser::Impossible<Vec<DatasetItem>, SequenceError>;
    type SerializeMap = serde::ser::Impossible<Vec<DatasetItem>, SequenceError>;
    type SerializeStruct = serde::ser::Impossible<Vec<DatasetItem>, SequenceError>;
    type SerializeStructVariant = serde::ser::Impossible<Vec<DatasetItem>, SequenceError>;

    fn serialize_seq(self, len: Option<usize>) -> Result<SequenceCollector, SequenceError> {
        Ok(SequenceCollector(Vec::with_capacity(len.unwrap_or(0))))
    }

    fn serialize_tuple(self, len: usize) -> Result<SequenceCollector, SequenceError> {
        Ok(SequenceCollector(Vec::with_capacity(len)))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Vec<DatasetItem>, SequenceError> {
        value.serialize(self)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Vec<DatasetItem>, SequenceError> {
        value.serialize(self)
    }

    not_a_sequence!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(&'static str),
        serialize_unit_variant(&'static str, u32, &'static str),
    );

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Vec<DatasetItem>, SequenceError> {
        Err(SequenceError::NotASequence)
    }

    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, SequenceError> {
        Err(SequenceError::NotASequence)
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, SequenceError> {
        Err(SequenceError::NotASequence)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, SequenceError> {
        Err(SequenceError::NotASequence)
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, SequenceError> {
        Err(SequenceError::NotASequence)
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, SequenceError> {
        Err(SequenceError::NotASequence)
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

        let page: DatasetPage<Item> = dataset
            .get_data(DatasetListOptions { offset: 1, limit: Some(1), desc: true, ..Default::default() })
            .await
            .unwrap();
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
            self.calls.lock().push(options.clone());
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

    #[tokio::test]
    async fn counts_backend_calls() {
        let storage = MemoryStorageBackend::new();
        let dataset = Dataset::open(&storage, &StorageIdentifier::Default).await.unwrap();
        dataset.push_data(&json!([{ "a": 1 }, { "a": 2 }])).await.unwrap();
        dataset.clone().push_data(&json!({ "a": 3 })).await.unwrap();
        dataset.get_all::<serde_json::Value>().await.unwrap();
        assert_eq!(dataset.stats(), DatasetStats { read_count: 1, write_count: 2 });
    }

    async fn numbers(count: usize) -> (Dataset, Arc<RecordingDataset>) {
        let storage = MemoryStorageBackend::new();
        let (dataset, backend) = recording(&storage).await;
        let items: Vec<_> = (0..count).map(|i| json!({ "i": i })).collect();
        dataset.push_data(&items).await.unwrap();
        (dataset, backend)
    }

    #[tokio::test]
    async fn get_data_options_reach_the_backend() {
        let (dataset, backend) = numbers(1).await;
        let options = DatasetListOptions {
            fields: Some(vec!["i".into()]),
            unwind: Some("tags".into()),
            clean: true,
            skip_hidden: true,
            skip_empty: true,
            ..Default::default()
        };
        // The memory backend ignores them, as in JS.
        let page: DatasetPage<serde_json::Value> = dataset.get_data(options.clone()).await.unwrap();
        assert_eq!(page.items, [json!({ "i": 0 })]);
        assert_eq!(backend.calls.lock().last(), Some(&options));
    }

    #[tokio::test]
    async fn for_each_pages_through_everything_from_the_offset() {
        let (dataset, backend) = numbers(5).await;
        let mut seen = Vec::new();
        let options = DatasetListOptions { offset: 1, limit: Some(2), ..Default::default() };
        dataset
            .for_each(options, |item: serde_json::Value, index| {
                seen.push((index, item["i"].as_u64().unwrap()));
                async { Ok::<_, StorageError>(()) }
            })
            .await
            .unwrap();
        // As in JS, the limit of `forEach` is the page size, not a cap.
        assert_eq!(seen, [(0, 1), (1, 2), (2, 3), (3, 4)]);
        let pages: Vec<_> = backend.calls.lock().iter().map(|call| (call.offset, call.limit)).collect();
        assert_eq!(pages, [(1, Some(2)), (3, Some(2))]);

        let doubled = dataset
            .map(DatasetListOptions::default(), |item: serde_json::Value, _| async move {
                Ok::<_, StorageError>(item["i"].as_u64().unwrap() * 2)
            })
            .await
            .unwrap();
        assert_eq!(doubled, [0, 2, 4, 6, 8]);
        let sum = dataset
            .reduce(DatasetListOptions::default(), 100, |memo, item: serde_json::Value, _| async move {
                Ok::<_, StorageError>(memo + item["i"].as_u64().unwrap())
            })
            .await
            .unwrap();
        assert_eq!(sum, 110);
    }

    #[tokio::test]
    async fn values_and_entries_take_the_limit_as_a_cap() {
        let (dataset, _) = numbers(5).await;
        let options = DatasetListOptions { offset: 1, limit: Some(3), ..Default::default() };
        let values: Vec<serde_json::Value> = dataset.values(options.clone()).await.unwrap();
        assert_eq!(values, [json!({ "i": 1 }), json!({ "i": 2 }), json!({ "i": 3 })]);
        let entries: Vec<(usize, serde_json::Value)> = dataset.entries(options).await.unwrap();
        assert_eq!(entries.iter().map(|(index, _)| *index).collect::<Vec<_>>(), [1, 2, 3]);
        let desc: Vec<serde_json::Value> =
            dataset.values(DatasetListOptions { desc: true, ..Default::default() }).await.unwrap();
        assert_eq!(desc.first(), Some(&json!({ "i": 4 })));
    }

    #[tokio::test]
    async fn exports_to_a_key_value_store_as_json_and_csv() {
        let storage = MemoryStorageBackend::new();
        let dataset = Dataset::open(&storage, &StorageIdentifier::Default).await.unwrap();
        let store = crate::KeyValueStore::open(&storage, &StorageIdentifier::Default).await.unwrap();

        dataset.export_to_csv("EMPTY", &store, DatasetExportOptions::default()).await.unwrap();
        let empty = store.get_record("EMPTY").await.unwrap().unwrap();
        assert_eq!((&empty.value[..], empty.content_type.as_deref()), (&b""[..], Some("text/csv")));

        dataset
            .push_data(&json!([
                { "a": "x,y", "b": true, "c": 1.5 },
                { "a": "say \"hi\"", "b": false, "d": { "e": [1] } },
                { "a": null, "c": "line\nbreak" },
            ]))
            .await
            .unwrap();
        let items = dataset.export_to_csv("CSV", &store, DatasetExportOptions::default()).await.unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(
            store.get_text("CSV").await.unwrap().unwrap(),
            "a,b,c\n\"x,y\",1,1.5\n\"say \"\"hi\"\"\",,\n,,\"line\nbreak\"\n"
        );
        let options = DatasetExportOptions { collect_all_keys: true, ..Default::default() };
        dataset.export_to_csv("ALL", &store, options).await.unwrap();
        assert!(store.get_text("ALL").await.unwrap().unwrap().starts_with("a,b,c,d\n"));
        assert!(store.get_text("ALL").await.unwrap().unwrap().contains(",\"{\"\"e\"\":[1]}\"\n"));

        dataset.export_to_json("JSON", &store, DatasetExportOptions::default()).await.unwrap();
        let json = store.get_record("JSON").await.unwrap().unwrap();
        assert_eq!(json.content_type.as_deref(), Some(crate::storage::key_value_store::CONTENT_TYPE_JSON));
        assert_eq!(store.get_value::<Vec<serde_json::Value>>("JSON").await.unwrap().unwrap().len(), 3);

        let err = dataset.export_to("X", &store, DatasetExportOptions::default(), "text/html").await.unwrap_err();
        assert_eq!(err.to_string(), "Unsupported content type: text/html");
    }

    #[test]
    fn splits_sequences_into_their_items() {
        #[derive(Serialize)]
        struct Item {
            a: u32,
        }
        #[derive(Serialize)]
        struct Wrapper(Vec<Item>);
        let texts = |items: Vec<DatasetItem>| items.iter().map(|item| item.get().to_owned()).collect::<Vec<_>>();

        assert_eq!(texts(serialize_items(&json!({ "a": 1 })).unwrap()), [r#"{"a":1}"#]);
        assert_eq!(texts(serialize_items(&Item { a: 1 }).unwrap()), [r#"{"a":1}"#]);
        let values = [json!({ "a": 1 }), json!({ "b": [1, 2] })];
        assert_eq!(texts(serialize_items(&values[..]).unwrap()), [r#"{"a":1}"#, r#"{"b":[1,2]}"#]);
        assert_eq!(texts(serialize_items(&json!([{ "a": 1 }, { "b": 2 }])).unwrap()), [r#"{"a":1}"#, r#"{"b":2}"#]);
        assert_eq!(texts(serialize_items(&vec![Item { a: 1 }, Item { a: 2 }]).unwrap()), [r#"{"a":1}"#, r#"{"a":2}"#]);
        assert_eq!(texts(serialize_items(&(Item { a: 1 }, json!({ "b": 2 }))).unwrap()), [r#"{"a":1}"#, r#"{"b":2}"#]);
        assert_eq!(texts(serialize_items(&Some(vec![Item { a: 1 }])).unwrap()), [r#"{"a":1}"#]);
        assert_eq!(texts(serialize_items(&Wrapper(vec![Item { a: 3 }])).unwrap()), [r#"{"a":3}"#]);
        assert!(serialize_items(&Vec::<Item>::new()).unwrap().is_empty());
    }

    #[test]
    fn rejects_non_objects() {
        assert!(serialize_items(&json!(1)).is_err());
        assert!(serialize_items(&json!([{ "a": 1 }, 2])).is_err());
        assert!(serialize_items(&"text").is_err());
    }
}
