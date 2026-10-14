//! [`KeyValueStore`]: records addressed by key, each with a content type.

use std::rc::Rc;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::backend::{
    KeyValueStoreBackend, KeyValueStoreInfo, KeyValueStoreItemData, KeyValueStoreListKeysOptions, KeyValueStoreRecord,
    StorageBackend, StorageIdentifier,
};
use crate::errors::{StorageError, StorageResult};

pub const CONTENT_TYPE_JSON: &str = "application/json; charset=utf-8";
pub const CONTENT_TYPE_TEXT: &str = "text/plain; charset=utf-8";
pub const CONTENT_TYPE_BINARY: &str = "application/octet-stream";

/// Handle to a key-value store. Cloning is cheap and clones share the storage.
#[derive(Clone)]
pub struct KeyValueStore {
    backend: Rc<dyn KeyValueStoreBackend>,
}

impl std::fmt::Debug for KeyValueStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyValueStore").finish_non_exhaustive()
    }
}

/// Checks a key against the rules shared with the Apify platform: 1 to 256 characters out of
/// `a-zA-Z0-9!-_.'()`.
pub fn validate_key(key: &str) -> StorageResult<()> {
    let valid = !key.is_empty()
        && key.len() <= 256
        && key.bytes().all(|b| b.is_ascii_alphanumeric() || b"!-_.'()".contains(&b));
    if valid {
        Ok(())
    } else {
        Err(StorageError::InvalidArgument(format!(
            "The key '{key}' must be at most 256 characters long and only contain the following characters: a-zA-Z0-9!-_.'()"
        )))
    }
}

/// A record value serialized for storage.
pub fn serialize_json<T: Serialize + ?Sized>(value: &T) -> StorageResult<Bytes> {
    // Pretty-printed with two spaces, like `JSON.stringify(value, null, 2)`.
    Ok(Bytes::from(serde_json::to_vec_pretty(value)?))
}

/// Whether a content type holds text that [`KeyValueStore::get_text`] can return.
fn is_textual(content_type: &str) -> bool {
    let essence = content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    essence == "application/json"
        || essence.starts_with("text/")
        || (essence.starts_with("application/") && essence.ends_with("xml"))
}

impl KeyValueStore {
    pub async fn open(storage: &dyn StorageBackend, id: &StorageIdentifier) -> StorageResult<Self> {
        Ok(KeyValueStore { backend: storage.create_key_value_store_backend(id).await? })
    }

    pub fn from_backend(backend: Rc<dyn KeyValueStoreBackend>) -> Self {
        KeyValueStore { backend }
    }

    pub fn backend(&self) -> &Rc<dyn KeyValueStoreBackend> {
        &self.backend
    }

    pub fn same_storage(&self, other: &KeyValueStore) -> bool {
        Rc::ptr_eq(&self.backend, &other.backend)
    }

    /// Reads a JSON record into `T`. Returns `None` if the key does not exist.
    pub async fn get_value<T: DeserializeOwned>(&self, key: &str) -> StorageResult<Option<T>> {
        match self.backend.get_value(key).await? {
            Some(record) => Ok(Some(serde_json::from_slice(&record.value)?)),
            None => Ok(None),
        }
    }

    /// Reads a textual record (`text/*`, JSON or XML) as a string.
    pub async fn get_text(&self, key: &str) -> StorageResult<Option<String>> {
        let Some(record) = self.backend.get_value(key).await? else {
            return Ok(None);
        };
        if let Some(content_type) = &record.content_type
            && !is_textual(content_type)
        {
            return Err(StorageError::InvalidArgument(format!(
                "Record '{key}' has content type '{content_type}', which is not text"
            )));
        }
        String::from_utf8(record.value.to_vec())
            .map(Some)
            .map_err(|_| StorageError::InvalidArgument(format!("Record '{key}' is not valid UTF-8")))
    }

    /// Reads the raw record.
    pub async fn get_record(&self, key: &str) -> StorageResult<Option<KeyValueStoreRecord>> {
        self.backend.get_value(key).await
    }

    /// Stores `value` as pretty-printed JSON.
    pub async fn set_value<T: Serialize + ?Sized>(&self, key: &str, value: &T) -> StorageResult<()> {
        validate_key(key)?;
        self.backend
            .set_value(KeyValueStoreRecord {
                key: key.to_owned(),
                value: serialize_json(value)?,
                content_type: Some(CONTENT_TYPE_JSON.to_owned()),
            })
            .await
    }

    pub async fn set_text(&self, key: &str, text: impl Into<String>) -> StorageResult<()> {
        self.set_bytes(key, Bytes::from(text.into()), Some(CONTENT_TYPE_TEXT)).await
    }

    /// Stores raw bytes; the content type defaults to `application/octet-stream`.
    pub async fn set_bytes(&self, key: &str, value: Bytes, content_type: Option<&str>) -> StorageResult<()> {
        validate_key(key)?;
        self.backend
            .set_value(KeyValueStoreRecord {
                key: key.to_owned(),
                value,
                content_type: Some(content_type.unwrap_or(CONTENT_TYPE_BINARY).to_owned()),
            })
            .await
    }

    pub async fn delete_value(&self, key: &str) -> StorageResult<()> {
        validate_key(key)?;
        self.backend.delete_value(key).await
    }

    pub async fn record_exists(&self, key: &str) -> StorageResult<bool> {
        self.backend.record_exists(key).await
    }

    pub async fn get_public_url(&self, key: &str) -> StorageResult<Option<String>> {
        validate_key(key)?;
        self.backend.get_public_url(key).await
    }

    /// Every key (optionally with a prefix), following the backend's pagination cursor.
    pub async fn keys(&self, prefix: Option<&str>) -> StorageResult<Vec<KeyValueStoreItemData>> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let page = self
                .backend
                .list_keys(KeyValueStoreListKeysOptions {
                    prefix: prefix.map(str::to_owned),
                    exclusive_start_key: cursor.take(),
                    limit: Some(1000),
                })
                .await?;
            out.extend(page.items);
            if !page.is_truncated {
                return Ok(out);
            }
            cursor = page.next_exclusive_start_key;
            if cursor.is_none() {
                return Ok(out);
            }
        }
    }

    pub async fn get_info(&self) -> StorageResult<KeyValueStoreInfo> {
        self.backend.get_metadata().await
    }

    pub async fn drop_storage(self) -> StorageResult<()> {
        self.backend.drop_storage().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryStorageBackend;
    use serde_json::json;

    #[tokio::test(flavor = "local")]
    async fn json_text_and_bytes() {
        let storage = MemoryStorageBackend::new();
        let store = KeyValueStore::open(&storage, &StorageIdentifier::Default).await.unwrap();

        store.set_value("STATE", &json!({ "count": 2 })).await.unwrap();
        assert_eq!(store.get_value::<serde_json::Value>("STATE").await.unwrap(), Some(json!({ "count": 2 })));
        let record = store.get_record("STATE").await.unwrap().unwrap();
        assert_eq!(&record.value[..], b"{\n  \"count\": 2\n}");
        assert_eq!(record.content_type.as_deref(), Some(CONTENT_TYPE_JSON));

        store.set_text("note", "hello").await.unwrap();
        assert_eq!(store.get_text("note").await.unwrap().as_deref(), Some("hello"));

        store.set_bytes("blob", Bytes::from_static(&[0, 1, 2]), None).await.unwrap();
        assert!(store.get_text("blob").await.is_err());

        let keys: Vec<String> = store.keys(None).await.unwrap().into_iter().map(|k| k.key).collect();
        assert_eq!(keys, ["STATE", "blob", "note"]);

        assert!(store.set_value("bad key", &1).await.is_err());
        store.delete_value("note").await.unwrap();
        assert!(!store.record_exists("note").await.unwrap());
    }
}
