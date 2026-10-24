//! [`KeyValueStore`]: records addressed by key, each with a content type.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde::de::value::{BytesDeserializer, StringDeserializer};
use serde_json::Value;

use super::backend::{
    KeyValueStoreBackend, KeyValueStoreInfo, KeyValueStoreItemData, KeyValueStoreListKeysOptions, KeyValueStoreRecord,
    StorageBackend, StorageIdentifier,
};
use crate::errors::{StorageError, StorageResult};
use crate::events::{EventKind, EventManager};
use crate::recoverable_state::{PersistedState, SerdeState};
use crate::services::{CacheLink, Services};

pub const CONTENT_TYPE_JSON: &str = "application/json; charset=utf-8";
pub const CONTENT_TYPE_TEXT: &str = "text/plain; charset=utf-8";
pub const CONTENT_TYPE_BINARY: &str = "application/octet-stream";

/// Handle to a key-value store. Cloning is cheap and clones share the storage.
#[derive(Clone)]
pub struct KeyValueStore {
    backend: Arc<dyn KeyValueStoreBackend>,
    cache_link: CacheLink,
    counters: Arc<KeyValueStoreCounters>,
    /// The event manager auto-saved values are saved on; the global one if unset.
    events: Option<EventManager>,
    auto_saved: Arc<AutoSavedValues>,
}

/// A record value parsed by its content type, as `KeyValueStore.getValue` returns it in JS.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordValue {
    /// `application/json`, parsed.
    Json(Value),
    /// `text/*` and `application/*xml`, decoded with the charset of the content type.
    Text(String),
    /// Anything else, including records without a content type or with an unknown charset.
    Bytes(Bytes),
}

/// The values of [`KeyValueStore::get_auto_saved_value`], by key.
#[derive(Default)]
struct AutoSavedValues {
    values: Mutex<HashMap<String, AutoSaved>>,
    listening: Mutex<bool>,
}

#[derive(Clone)]
struct AutoSaved {
    value: Arc<dyn Any + Send + Sync>,
    state: Arc<dyn PersistedState>,
}

/// The operations a key-value store handle (and its clones) issued to the backend, counted per
/// backend call (`KeyValueStoreStats` in JS).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeyValueStoreStats {
    /// Records read.
    pub read_count: u64,
    /// Records written.
    pub write_count: u64,
    /// Records deleted.
    pub delete_count: u64,
    /// Pages of keys listed.
    pub list_count: u64,
}

#[derive(Default)]
struct KeyValueStoreCounters {
    read: AtomicU64,
    write: AtomicU64,
    delete: AtomicU64,
    list: AtomicU64,
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

/// How a record's bytes read, by content type (`parseValue` in JS).
enum Decoded {
    Json(String),
    Text(String),
    Bytes,
}

fn is_token(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+.^_`|~-".contains(&b))
}

fn decode(value: &[u8], content_type: Option<&str>) -> Decoded {
    let Some(content_type) = content_type else {
        return Decoded::Bytes;
    };
    let mut parts = content_type.split(';');
    let essence = parts.next().unwrap_or_default().trim().to_ascii_lowercase();
    let valid = essence.split_once('/').is_some_and(|(kind, subtype)| is_token(kind) && is_token(subtype));
    let stringifiable = essence == "application/json"
        || essence.starts_with("text/")
        || (essence.starts_with("application/") && essence.ends_with("xml"));
    if !valid || !stringifiable {
        return Decoded::Bytes;
    }
    let charset = parts.find_map(|param| {
        let (name, value) = param.split_once('=')?;
        name.trim().eq_ignore_ascii_case("charset").then(|| value.trim().trim_matches('"').to_ascii_lowercase())
    });
    // The encodings of Node's `Buffer`; any other charset leaves the bytes as they are.
    let text = match charset.as_deref() {
        None | Some("utf8" | "utf-8") => String::from_utf8_lossy(value).into_owned(),
        Some("latin1" | "binary") => value.iter().map(|&b| char::from(b)).collect(),
        Some("ascii") => value.iter().map(|&b| char::from(b & 0x7f)).collect(),
        Some("utf16le" | "utf-16le" | "ucs2" | "ucs-2") => {
            let units: Vec<u16> = value.as_chunks::<2>().0.iter().map(|pair| u16::from_le_bytes(*pair)).collect();
            String::from_utf16_lossy(&units)
        }
        Some("base64") => base64::engine::general_purpose::STANDARD.encode(value),
        Some("base64url") => base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value),
        Some("hex") => value.iter().map(|b| format!("{b:02x}")).collect(),
        Some(_) => return Decoded::Bytes,
    };
    if essence == "application/json" { Decoded::Json(text) } else { Decoded::Text(text) }
}

/// Deserializes a record by its content type, as [`KeyValueStore::get_value`] does.
pub(crate) fn read_typed<T: DeserializeOwned>(key: &str, value: &[u8], content_type: Option<&str>) -> StorageResult<T> {
    Ok(match decode(value, content_type) {
        Decoded::Json(text) => serde_json::from_str(&text)?,
        Decoded::Text(text) => T::deserialize(StringDeserializer::<serde_json::Error>::new(text))?,
        Decoded::Bytes => match serde_json::from_slice(value) {
            Ok(parsed) if content_type.is_none() => parsed,
            _ => T::deserialize(BytesDeserializer::<serde_json::Error>::new(value)).map_err(|_| {
                StorageError::InvalidArgument(format!(
                    "Record '{key}' has content type '{}', which is neither JSON nor text; read it with get_record_value() or get_record()",
                    content_type.unwrap_or_default()
                ))
            })?,
        },
    })
}

/// Whether a content type holds text that [`KeyValueStore::get_text`] can return.
fn is_textual(content_type: &str) -> bool {
    let essence = content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    essence == "application/json"
        || essence.starts_with("text/")
        || (essence.starts_with("application/") && essence.ends_with("xml"))
}

impl KeyValueStore {
    /// Opens a new handle on `storage`, outside the instance cache of [`Services`](crate::Services);
    /// [`Services::open_key_value_store`](crate::Services::open_key_value_store)
    /// returns the same instance on every call.
    pub async fn open(storage: &dyn StorageBackend, id: &StorageIdentifier) -> StorageResult<Self> {
        Ok(KeyValueStore::from_backend(storage.create_key_value_store_backend(id).await?))
    }

    pub fn from_backend(backend: Arc<dyn KeyValueStoreBackend>) -> Self {
        KeyValueStore {
            backend,
            cache_link: CacheLink::default(),
            counters: Arc::default(),
            events: None,
            auto_saved: Arc::default(),
        }
    }

    pub(crate) fn with_cache_link(mut self, link: CacheLink) -> Self {
        self.cache_link = link;
        self
    }

    /// Saves the auto-saved values on the `PersistState` events of `events` instead of the global
    /// event manager's.
    pub fn with_events(mut self, events: EventManager) -> Self {
        self.events = Some(events);
        self
    }

    pub fn backend(&self) -> &Arc<dyn KeyValueStoreBackend> {
        &self.backend
    }

    pub fn same_storage(&self, other: &KeyValueStore) -> bool {
        Arc::ptr_eq(&self.backend, &other.backend)
    }

    /// Reads a record into `T`, by its content type: JSON records are parsed, text records
    /// (`text/*`, `application/*xml`) deserialize from their text, so `String` and
    /// [`serde_json::Value`] read them, and other records from their bytes. A record without a
    /// content type is read as JSON if it parses. Returns `None` if the key does not exist.
    /// [`get_record_value`](Self::get_record_value) returns the value as JS `getValue` does.
    pub async fn get_value<T: DeserializeOwned>(&self, key: &str) -> StorageResult<Option<T>> {
        let Some(record) = self.read_record(key).await? else {
            return Ok(None);
        };
        read_typed(key, &record.value, record.content_type.as_deref()).map(Some)
    }

    /// Reads a record, parsed by its content type like `getValue` in JS: JSON only for
    /// `application/json`, text for `text/*` and `application/*xml`, bytes otherwise. Returns
    /// `None` if the key does not exist.
    pub async fn get_record_value(&self, key: &str) -> StorageResult<Option<RecordValue>> {
        let Some(record) = self.read_record(key).await? else {
            return Ok(None);
        };
        Ok(Some(match decode(&record.value, record.content_type.as_deref()) {
            Decoded::Json(text) => RecordValue::Json(serde_json::from_str(&text)?),
            Decoded::Text(text) => RecordValue::Text(text),
            Decoded::Bytes => RecordValue::Bytes(record.value),
        }))
    }

    /// A value saved under `key` on every `PersistState` event, like `getAutoSavedValue()` in JS.
    /// The first call loads the record (or starts from `default`); later calls with the same key
    /// return the same value, and [`set_value`](Self::set_value) on the key updates it.
    pub async fn get_auto_saved_value<T>(
        &self,
        key: &str,
        default: impl Fn() -> T + Send + Sync + 'static,
    ) -> StorageResult<Arc<SerdeState<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        if let Some(saved) = self.auto_saved_value(key) {
            return Self::downcast(key, saved);
        }
        let loaded: Option<T> = self.get_value(key).await?;
        let state = Arc::new(SerdeState::new(default));
        if let Some(loaded) = loaded {
            *state.lock() = loaded;
        }
        let saved = {
            let mut values = self.auto_saved.values.lock();
            // Another call may have loaded it meanwhile; its value is the one saved.
            values.entry(key.to_owned()).or_insert_with(|| AutoSaved { value: state.clone(), state }).clone()
        };
        self.ensure_auto_saving();
        Self::downcast(key, saved)
    }

    fn auto_saved_value(&self, key: &str) -> Option<AutoSaved> {
        self.auto_saved.values.lock().get(key).cloned()
    }

    fn downcast<T: Send + Sync + 'static>(key: &str, saved: AutoSaved) -> StorageResult<Arc<SerdeState<T>>> {
        saved.value.downcast::<SerdeState<T>>().map_err(|_| {
            StorageError::InvalidArgument(format!("the value saved under '{key}' is used with another type"))
        })
    }

    /// Starts saving the auto-saved values on `PersistState` events, once per store.
    fn ensure_auto_saving(&self) {
        let mut listening = self.auto_saved.listening.lock();
        if *listening {
            return;
        }
        *listening = true;
        let events = self.events.clone().unwrap_or_else(|| Services::global().events.clone());
        let store = self.clone();
        events.on(EventKind::PersistState, move |_| {
            let store = store.clone();
            async move { store.save_auto_saved_values().await }
        });
    }

    async fn save_auto_saved_values(&self) {
        let records: Vec<(String, Value)> =
            self.auto_saved.values.lock().iter().map(|(key, saved)| (key.clone(), saved.state.to_record())).collect();
        for (key, record) in records {
            let written = match serialize_json(&record) {
                Ok(value) => {
                    self.write_record(KeyValueStoreRecord {
                        key: key.clone(),
                        value,
                        content_type: Some(CONTENT_TYPE_JSON.to_owned()),
                    })
                    .await
                }
                Err(err) => Err(err),
            };
            if let Err(err) = written {
                tracing::warn!("Failed to persist the state value to {key}: {err}");
            }
        }
    }

    /// Points the auto-saved value of `key`, if any, at a value being written.
    pub(crate) fn update_auto_saved(&self, key: &str, value: impl FnOnce() -> Option<Value>) {
        if let Some(saved) = self.auto_saved_value(key)
            && let Some(value) = value()
            && let Err(err) = saved.state.restore(value)
        {
            tracing::warn!("The value written to {key} does not fit its auto-saved value: {err}");
        }
    }

    /// Reads a textual record (`text/*`, JSON or XML) as a string.
    pub async fn get_text(&self, key: &str) -> StorageResult<Option<String>> {
        let Some(record) = self.read_record(key).await? else {
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
        self.read_record(key).await
    }

    async fn read_record(&self, key: &str) -> StorageResult<Option<KeyValueStoreRecord>> {
        self.counters.read.fetch_add(1, Ordering::Relaxed);
        self.backend.get_value(key).await
    }

    /// Writes a record that was already validated and serialized.
    pub(crate) async fn write_record(&self, record: KeyValueStoreRecord) -> StorageResult<()> {
        self.counters.write.fetch_add(1, Ordering::Relaxed);
        self.backend.set_value(record).await
    }

    /// Deletes a record whose key was already validated.
    pub(crate) async fn delete_record(&self, key: &str) -> StorageResult<()> {
        self.counters.delete.fetch_add(1, Ordering::Relaxed);
        self.backend.delete_value(key).await
    }

    pub fn stats(&self) -> KeyValueStoreStats {
        KeyValueStoreStats {
            read_count: self.counters.read.load(Ordering::Relaxed),
            write_count: self.counters.write.load(Ordering::Relaxed),
            delete_count: self.counters.delete.load(Ordering::Relaxed),
            list_count: self.counters.list.load(Ordering::Relaxed),
        }
    }

    /// Stores `value` as pretty-printed JSON.
    pub async fn set_value<T: Serialize + ?Sized>(&self, key: &str, value: &T) -> StorageResult<()> {
        validate_key(key)?;
        let bytes = serialize_json(value)?;
        self.update_auto_saved(key, || serde_json::to_value(value).ok());
        self.write_record(KeyValueStoreRecord {
            key: key.to_owned(),
            value: bytes,
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
        self.write_record(KeyValueStoreRecord {
            key: key.to_owned(),
            value,
            content_type: Some(content_type.unwrap_or(CONTENT_TYPE_BINARY).to_owned()),
        })
        .await
    }

    pub async fn delete_value(&self, key: &str) -> StorageResult<()> {
        validate_key(key)?;
        self.delete_record(key).await
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
            self.counters.list.fetch_add(1, Ordering::Relaxed);
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

    /// Removes every record, and forgets the auto-saved values.
    pub async fn purge(&self) -> StorageResult<()> {
        self.backend.purge().await?;
        self.auto_saved.values.lock().clear();
        Ok(())
    }

    pub async fn drop_storage(self) -> StorageResult<()> {
        self.backend.drop_storage().await?;
        self.cache_link.evict(Arc::as_ptr(&self.backend) as *const ());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryStorageBackend;
    use serde_json::json;

    #[tokio::test]
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

    #[tokio::test]
    async fn counts_backend_calls() {
        let storage = MemoryStorageBackend::new();
        let store = KeyValueStore::open(&storage, &StorageIdentifier::Default).await.unwrap();
        store.set_value("a", &1).await.unwrap();
        store.set_text("b", "x").await.unwrap();
        store.get_value::<i32>("a").await.unwrap();
        store.get_record("b").await.unwrap();
        store.delete_value("a").await.unwrap();
        store.keys(None).await.unwrap();
        store.record_exists("b").await.unwrap();
        assert_eq!(store.stats(), KeyValueStoreStats { read_count: 2, write_count: 2, delete_count: 1, list_count: 1 });
    }
    #[tokio::test]
    async fn values_are_parsed_by_content_type() {
        let storage = MemoryStorageBackend::new();
        let store = KeyValueStore::open(&storage, &StorageIdentifier::Default).await.unwrap();
        store.set_value("json", &json!({ "a": 1 })).await.unwrap();
        store.set_text("text", "hello").await.unwrap();
        store.set_bytes("xml", Bytes::from_static(b"<a/>"), Some("application/rss+xml")).await.unwrap();
        store.set_bytes("png", Bytes::from_static(&[1, 2]), Some("image/png")).await.unwrap();
        store.set_bytes("latin1", Bytes::from_static(&[0xe9]), Some("text/plain; charset=latin1")).await.unwrap();
        store.set_bytes("koi8", Bytes::from_static(b"x"), Some("text/plain; charset=koi8-r")).await.unwrap();
        store.set_bytes("json-text", Bytes::from_static(b"{}"), Some("application/jsonl")).await.unwrap();

        let value = |key: &'static str| {
            let store = store.clone();
            async move { store.get_record_value(key).await.unwrap().unwrap() }
        };
        assert_eq!(value("json").await, RecordValue::Json(json!({ "a": 1 })));
        assert_eq!(value("text").await, RecordValue::Text("hello".into()));
        assert_eq!(value("xml").await, RecordValue::Text("<a/>".into()));
        assert_eq!(value("png").await, RecordValue::Bytes(Bytes::from_static(&[1, 2])));
        assert_eq!(value("latin1").await, RecordValue::Text("\u{e9}".into()));
        assert_eq!(value("koi8").await, RecordValue::Bytes(Bytes::from_static(b"x")));
        assert_eq!(value("json-text").await, RecordValue::Bytes(Bytes::from_static(b"{}")));
        assert!(store.get_record_value("missing").await.unwrap().is_none());

        // The typed getter follows the content type too.
        assert_eq!(store.get_value::<String>("text").await.unwrap().as_deref(), Some("hello"));
        assert_eq!(store.get_value::<serde_json::Value>("text").await.unwrap(), Some(json!("hello")));
        assert!(store.get_value::<serde_json::Value>("png").await.is_err());
    }

    #[tokio::test]
    async fn auto_saved_values_are_saved_on_persist_state() {
        let services = crate::Services::in_memory();
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        store.set_value("SAVED", &json!({ "n": 1 })).await.unwrap();

        let value = store.get_auto_saved_value("STATE", || json!({ "n": 0 })).await.unwrap();
        assert_eq!(*value.lock(), json!({ "n": 0 }));
        let same = store.get_auto_saved_value("STATE", || json!({ "n": 9 })).await.unwrap();
        assert!(Arc::ptr_eq(&value, &same));
        let saved = store.get_auto_saved_value("SAVED", || json!({})).await.unwrap();
        assert_eq!(*saved.lock(), json!({ "n": 1 }));

        value.lock()["n"] = json!(5);
        services.events.emit(crate::Event::PersistState { is_migrating: false });
        services.events.wait_for_all_listeners_to_complete().await;
        assert_eq!(store.get_value::<serde_json::Value>("STATE").await.unwrap(), Some(json!({ "n": 5 })));

        // Setting the key updates the auto-saved value.
        store.set_value("STATE", &json!({ "n": 7 })).await.unwrap();
        assert_eq!(*value.lock(), json!({ "n": 7 }));

        // The services' auto-saved values are the default store's.
        let through_services = services.auto_saved_value("STATE", || json!({})).await.unwrap();
        assert!(Arc::ptr_eq(&value, &through_services));

        store.purge().await.unwrap();
        assert!(store.get_value::<serde_json::Value>("STATE").await.unwrap().is_none());
        let fresh = store.get_auto_saved_value("STATE", || json!({ "n": 0 })).await.unwrap();
        assert!(!Arc::ptr_eq(&value, &fresh));
    }
}
