//! Request-scoped storage transactions.
//!
//! Writes a request handler makes through its context are journaled and applied only after the
//! handler succeeds, so a failed attempt leaves nothing behind and its retry does not duplicate
//! data (at-least-once processing, exactly-once effects per successful attempt).
//!
//! Items are journaled already serialized, so committing is a move, not a copy: there is no
//! `structuredClone` equivalent on this path.

use bytes::Bytes;
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::errors::StorageResult;
use crate::storage::backend::{DatasetItem, KeyValueStoreRecord};
use crate::storage::dataset::{Dataset, serialize_items};
use crate::storage::key_value_store::{CONTENT_TYPE_JSON, KeyValueStore, serialize_json, validate_key};

/// A key-value write recorded in a transaction. `value: None` is a deletion.
#[derive(Clone, Debug)]
pub struct KeyValueWrite {
    pub store: KeyValueStore,
    pub key: String,
    pub value: Option<(Bytes, String)>,
}

/// A value to write (bytes and content type), or `None` for a deletion.
type PendingValue = Option<(Bytes, String)>;

#[derive(Default)]
struct Journal {
    datasets: Vec<(Dataset, Vec<DatasetItem>)>,
    key_value: Vec<KeyValueWrite>,
    after_commit: Vec<Box<dyn FnOnce() + Send>>,
}

/// The journal of one request handler attempt.
#[derive(Default)]
pub struct StorageTransaction {
    journal: Mutex<Journal>,
}

impl std::fmt::Debug for StorageTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageTransaction").finish_non_exhaustive()
    }
}

/// Read-only view of what a transaction would write, for comparing attempts (used by the adaptive
/// crawler to check whether an HTTP-only run produced the same result as a browser run).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransactionView {
    pub dataset_items: Vec<serde_json::Value>,
    pub key_value_writes: Vec<(String, Option<Bytes>)>,
}

impl StorageTransaction {
    pub fn new() -> Self {
        Self::default()
    }

    /// Journals items for `dataset`. The data is serialized (and validated) now.
    pub fn push_data<T: Serialize + ?Sized>(&self, dataset: &Dataset, data: &T) -> StorageResult<()> {
        let items = serialize_items(data)?;
        let mut journal = self.journal.lock();
        match journal.datasets.last_mut() {
            Some((last, pending)) if last.same_storage(dataset) => pending.extend(items),
            _ => journal.datasets.push((dataset.clone(), items)),
        }
        Ok(())
    }

    pub fn set_value<T: Serialize + ?Sized>(&self, store: &KeyValueStore, key: &str, value: &T) -> StorageResult<()> {
        validate_key(key)?;
        let bytes = serialize_json(value)?;
        self.record_write(store, key, Some((bytes, CONTENT_TYPE_JSON.to_owned())));
        Ok(())
    }

    pub fn set_bytes(&self, store: &KeyValueStore, key: &str, value: Bytes, content_type: &str) -> StorageResult<()> {
        validate_key(key)?;
        self.record_write(store, key, Some((value, content_type.to_owned())));
        Ok(())
    }

    pub fn delete_value(&self, store: &KeyValueStore, key: &str) -> StorageResult<()> {
        validate_key(key)?;
        self.record_write(store, key, None);
        Ok(())
    }

    fn record_write(&self, store: &KeyValueStore, key: &str, value: Option<(Bytes, String)>) {
        self.journal.lock().key_value.push(KeyValueWrite { store: store.clone(), key: key.to_owned(), value });
    }

    /// Reads a JSON value, seeing this transaction's own uncommitted writes first.
    pub async fn get_value<T: DeserializeOwned>(&self, store: &KeyValueStore, key: &str) -> StorageResult<Option<T>> {
        let pending = self
            .journal
            .lock()
            .key_value
            .iter()
            .rev()
            .find(|write| write.key == key && write.store.same_storage(store))
            .map(|write| write.value.clone());
        match pending {
            Some(Some((bytes, _))) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Some(None) => Ok(None),
            None => store.get_value(key).await,
        }
    }

    /// Runs `callback` once the transaction has been committed; dropped on rollback.
    pub fn after_commit(&self, callback: impl FnOnce() + Send + 'static) {
        self.journal.lock().after_commit.push(Box::new(callback));
    }

    pub fn is_empty(&self) -> bool {
        let journal = self.journal.lock();
        journal.datasets.is_empty() && journal.key_value.is_empty()
    }

    pub fn view(&self) -> TransactionView {
        let journal = self.journal.lock();
        TransactionView {
            dataset_items: journal
                .datasets
                .iter()
                .flat_map(|(_, items)| items.iter())
                .filter_map(|item| serde_json::from_str(item.get()).ok())
                .collect(),
            key_value_writes: journal
                .key_value
                .iter()
                .map(|write| (write.key.clone(), write.value.as_ref().map(|(bytes, _)| bytes.clone())))
                .collect(),
        }
    }

    /// Applies the journal to the storages: key-value stores first, one write per key (the last
    /// one), then datasets, each with one push of all its items in order. Key-value writes are
    /// idempotent under retry and dataset pushes are not, so a commit that fails half-way is
    /// retried with as little duplication as possible (`StorageTransaction.flush` in JS).
    pub async fn commit(&self) -> StorageResult<()> {
        let journal = std::mem::take(&mut *self.journal.lock());

        let mut stores: Vec<(KeyValueStore, IndexMap<String, PendingValue>)> = Vec::new();
        for write in journal.key_value {
            let position = match stores.iter().position(|(store, _)| store.same_storage(&write.store)) {
                Some(position) => position,
                None => {
                    stores.push((write.store, IndexMap::new()));
                    stores.len() - 1
                }
            };
            stores[position].1.insert(write.key, write.value);
        }
        let mut datasets: Vec<(Dataset, Vec<DatasetItem>)> = Vec::new();
        for (dataset, items) in journal.datasets {
            match datasets.iter_mut().find(|(existing, _)| existing.same_storage(&dataset)) {
                Some((_, pending)) => pending.extend(items),
                None => datasets.push((dataset, items)),
            }
        }

        for (store, writes) in stores {
            for (key, value) in writes {
                match value {
                    Some((value, content_type)) => {
                        store.write_record(KeyValueStoreRecord { key, value, content_type: Some(content_type) }).await?
                    }
                    None => store.delete_record(&key).await?,
                }
            }
        }
        for (dataset, items) in datasets {
            if !items.is_empty() {
                dataset.push_items(items).await?;
            }
        }
        for callback in journal.after_commit {
            callback();
        }
        Ok(())
    }

    /// Discards the journal.
    pub fn rollback(&self) {
        *self.journal.lock() = Journal::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::StorageIdentifier;
    use crate::storage::memory::MemoryStorageBackend;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn commit_applies_and_rollback_discards() {
        let storage = MemoryStorageBackend::new();
        let dataset = Dataset::open(&storage, &StorageIdentifier::Default).await.unwrap();
        let store = KeyValueStore::open(&storage, &StorageIdentifier::Default).await.unwrap();

        let failed = StorageTransaction::new();
        failed.push_data(&dataset, &json!({ "attempt": 1 })).unwrap();
        failed.rollback();
        failed.commit().await.unwrap();
        assert_eq!(dataset.get_info().await.unwrap().item_count, 0);

        let tx = StorageTransaction::new();
        tx.push_data(&dataset, &json!({ "attempt": 2 })).unwrap();
        tx.set_value(&store, "STATE", &json!({ "n": 1 })).unwrap();
        assert_eq!(tx.get_value::<serde_json::Value>(&store, "STATE").await.unwrap(), Some(json!({ "n": 1 })));
        assert!(store.get_record("STATE").await.unwrap().is_none());

        let committed = Arc::new(AtomicBool::new(false));
        let flag = committed.clone();
        tx.after_commit(move || flag.store(true, Ordering::SeqCst));
        assert_eq!(tx.view().dataset_items, vec![json!({ "attempt": 2 })]);

        tx.commit().await.unwrap();
        assert!(committed.load(Ordering::SeqCst));
        assert_eq!(dataset.get_all::<serde_json::Value>().await.unwrap(), vec![json!({ "attempt": 2 })]);
        assert_eq!(store.get_value::<serde_json::Value>("STATE").await.unwrap(), Some(json!({ "n": 1 })));
    }

    /// Logs every write that reaches a backend, in order.
    #[derive(Clone, Default)]
    struct WriteLog(Arc<Mutex<Vec<String>>>);

    struct LoggingDataset {
        name: &'static str,
        log: WriteLog,
    }

    #[async_trait::async_trait]
    impl crate::storage::backend::DatasetBackend for LoggingDataset {
        async fn get_metadata(&self) -> StorageResult<crate::storage::backend::DatasetInfo> {
            unimplemented!()
        }
        async fn drop_storage(&self) -> StorageResult<()> {
            Ok(())
        }
        async fn purge(&self) -> StorageResult<()> {
            Ok(())
        }
        async fn push_data(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
            let items: Vec<&str> = items.iter().map(|item| item.get()).collect();
            self.log.0.lock().push(format!("{}.push({})", self.name, items.join(",")));
            Ok(())
        }
        async fn get_data(
            &self,
            _options: crate::storage::backend::DatasetListOptions,
        ) -> StorageResult<crate::storage::backend::PaginatedList<DatasetItem>> {
            unimplemented!()
        }
    }

    struct LoggingStore {
        name: &'static str,
        log: WriteLog,
    }

    #[async_trait::async_trait]
    impl crate::storage::backend::KeyValueStoreBackend for LoggingStore {
        async fn get_metadata(&self) -> StorageResult<crate::storage::backend::KeyValueStoreInfo> {
            unimplemented!()
        }
        async fn drop_storage(&self) -> StorageResult<()> {
            Ok(())
        }
        async fn purge(&self) -> StorageResult<()> {
            Ok(())
        }
        async fn get_value(&self, _key: &str) -> StorageResult<Option<KeyValueStoreRecord>> {
            Ok(None)
        }
        async fn set_value(&self, record: KeyValueStoreRecord) -> StorageResult<()> {
            let value = String::from_utf8_lossy(&record.value).replace([' ', '\n'], "");
            self.log.0.lock().push(format!("{}.set({}={value})", self.name, record.key));
            Ok(())
        }
        async fn delete_value(&self, key: &str) -> StorageResult<()> {
            self.log.0.lock().push(format!("{}.delete({key})", self.name));
            Ok(())
        }
        async fn list_keys(
            &self,
            _options: crate::storage::backend::KeyValueStoreListKeysOptions,
        ) -> StorageResult<crate::storage::backend::KeyValueStoreListKeysResult> {
            unimplemented!()
        }
        async fn record_exists(&self, _key: &str) -> StorageResult<bool> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn commit_writes_stores_first_then_one_push_per_dataset() {
        let log = WriteLog::default();
        let a = Dataset::from_backend(Arc::new(LoggingDataset { name: "a", log: log.clone() }));
        let b = Dataset::from_backend(Arc::new(LoggingDataset { name: "b", log: log.clone() }));
        let s = KeyValueStore::from_backend(Arc::new(LoggingStore { name: "s", log: log.clone() }));
        let t = KeyValueStore::from_backend(Arc::new(LoggingStore { name: "t", log: log.clone() }));

        let tx = StorageTransaction::new();
        tx.push_data(&a, &json!({ "n": 1 })).unwrap();
        tx.set_value(&s, "X", &1).unwrap();
        tx.push_data(&b, &json!({ "n": 2 })).unwrap();
        tx.set_value(&t, "Y", &1).unwrap();
        tx.push_data(&a, &json!([{ "n": 3 }, { "n": 4 }])).unwrap();
        tx.set_value(&s, "Z", &1).unwrap();
        tx.set_value(&s, "X", &2).unwrap();
        tx.delete_value(&t, "Y").unwrap();
        tx.commit().await.unwrap();

        assert_eq!(
            *log.0.lock(),
            ["s.set(X=2)", "s.set(Z=1)", "t.delete(Y)", r#"a.push({"n":1},{"n":3},{"n":4})"#, r#"b.push({"n":2})"#,]
        );
    }
}
