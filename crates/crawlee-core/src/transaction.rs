//! Request-scoped storage transactions.
//!
//! Writes a request handler makes through its context are journaled and applied only after the
//! handler succeeds, so a failed attempt leaves nothing behind and its retry does not duplicate
//! data (at-least-once processing, exactly-once effects per successful attempt).
//!
//! Items are journaled already serialized, so committing is a move, not a copy: there is no
//! `structuredClone` equivalent on this path.

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cell::RefCell;

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

#[derive(Default)]
struct Journal {
    datasets: Vec<(Dataset, Vec<DatasetItem>)>,
    key_value: Vec<KeyValueWrite>,
    after_commit: Vec<Box<dyn FnOnce()>>,
}

/// The journal of one request handler attempt.
#[derive(Default)]
pub struct StorageTransaction {
    journal: RefCell<Journal>,
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
        let mut journal = self.journal.borrow_mut();
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
        self.journal.borrow_mut().key_value.push(KeyValueWrite { store: store.clone(), key: key.to_owned(), value });
    }

    /// Reads a JSON value, seeing this transaction's own uncommitted writes first.
    pub async fn get_value<T: DeserializeOwned>(&self, store: &KeyValueStore, key: &str) -> StorageResult<Option<T>> {
        let pending = self
            .journal
            .borrow_mut()
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
    pub fn after_commit(&self, callback: impl FnOnce() + 'static) {
        self.journal.borrow_mut().after_commit.push(Box::new(callback));
    }

    pub fn is_empty(&self) -> bool {
        let journal = self.journal.borrow_mut();
        journal.datasets.is_empty() && journal.key_value.is_empty()
    }

    pub fn view(&self) -> TransactionView {
        let journal = self.journal.borrow_mut();
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

    /// Applies the journal to the storages, in the order the writes were made.
    pub async fn commit(&self) -> StorageResult<()> {
        let journal = std::mem::take(&mut *self.journal.borrow_mut());

        for (dataset, items) in journal.datasets {
            if !items.is_empty() {
                dataset.push_items(items).await?;
            }
        }
        for write in journal.key_value {
            match write.value {
                Some((value, content_type)) => {
                    write
                        .store
                        .backend()
                        .set_value(KeyValueStoreRecord { key: write.key, value, content_type: Some(content_type) })
                        .await?
                }
                None => write.store.backend().delete_value(&write.key).await?,
            }
        }
        for callback in journal.after_commit {
            callback();
        }
        Ok(())
    }

    /// Discards the journal.
    pub fn rollback(&self) {
        *self.journal.borrow_mut() = Journal::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::StorageIdentifier;
    use crate::storage::memory::MemoryStorageBackend;
    use serde_json::json;
    use std::cell::Cell;
    use std::rc::Rc;

    #[tokio::test(flavor = "local")]
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

        let committed = Rc::new(Cell::new(false));
        let flag = committed.clone();
        tx.after_commit(move || flag.set(true));
        assert_eq!(tx.view().dataset_items, vec![json!({ "attempt": 2 })]);

        tx.commit().await.unwrap();
        assert!(committed.get());
        assert_eq!(dataset.get_all::<serde_json::Value>().await.unwrap(), vec![json!({ "attempt": 2 })]);
        assert_eq!(store.get_value::<serde_json::Value>("STATE").await.unwrap(), Some(json!({ "n": 1 })));
    }
}
