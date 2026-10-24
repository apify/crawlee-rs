//! [`Services`]: the configuration, the storage backend and the event manager shared by the
//! storages and crawlers of a process.
//!
//! Crawlee for JS resolves these through an ambient, `AsyncLocalStorage`-scoped service locator.
//! Here they are explicit: crawlers take a `Services` value (cheap to clone) or fall back to the
//! process-wide default.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::configuration::Configuration;
use crate::errors::{StorageError, StorageResult};
use crate::events::EventManager;
use crate::recoverable_state::{RecoverableState, SerdeState};
use crate::storage::backend::{StorageBackend, StorageIdentifier, StorageKind};
use crate::storage::dataset::Dataset;
use crate::storage::key_value_store::KeyValueStore;
use crate::storage::memory::MemoryStorageBackend;
use crate::storage::request_queue::RequestQueue;

#[derive(Clone)]
pub struct Services {
    pub configuration: Arc<Configuration>,
    pub storage: Arc<dyn StorageBackend>,
    pub events: EventManager,
    /// Values of [`auto_saved_value`](Self::auto_saved_value), by key.
    auto_saved: Arc<tokio::sync::Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>>,
    /// Set once the run-scoped storages were purged, so that they are purged once per backend
    /// even when several crawlers run one after another.
    purged: Arc<tokio::sync::OnceCell<()>>,
    /// The storages opened so far, so that every open of one storage returns the same instance.
    storages: Arc<StorageCache>,
}

impl std::fmt::Debug for Services {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Services").finish_non_exhaustive()
    }
}

static GLOBAL: OnceLock<Services> = OnceLock::new();

#[derive(Debug, thiserror::Error)]
#[error("the global services were already initialized; set them before first use")]
pub struct ServiceConflictError;

impl Services {
    /// Services with the default [`Configuration`] and the given storage.
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Services::with_configuration(Configuration::default(), storage)
    }

    /// Services with the given storage and the local event manager (see [`EventManager::local`]).
    pub fn with_configuration(configuration: Configuration, storage: Arc<dyn StorageBackend>) -> Self {
        let events = EventManager::local(&configuration);
        Services::from_parts(configuration, storage, events)
    }

    pub fn from_parts(configuration: Configuration, storage: Arc<dyn StorageBackend>, events: EventManager) -> Self {
        Services {
            configuration: Arc::new(configuration),
            storage,
            events,
            auto_saved: Arc::default(),
            purged: Arc::default(),
            storages: Arc::default(),
        }
    }

    /// Services with the storage `configuration` asks for: files under
    /// [`storage_dir`](Configuration::storage_dir) when
    /// [`persist_storage`](Configuration::persist_storage) is set (and the `fs-storage` feature
    /// is enabled), process memory otherwise.
    pub fn from_configuration(configuration: Configuration) -> Self {
        #[cfg(feature = "fs-storage")]
        if configuration.persist_storage {
            let storage =
                Arc::new(crate::storage::file_system::FileSystemStorageBackend::new(configuration.storage_dir.clone()));
            return Services::with_configuration(configuration, storage);
        }
        Services::with_configuration(configuration, Arc::new(MemoryStorageBackend::new()))
    }

    /// Services backed by a fresh in-memory storage, with the default configuration.
    pub fn in_memory() -> Self {
        Services::new(Arc::new(MemoryStorageBackend::new()))
    }

    /// The process-wide services. Unless [`set_global`](Self::set_global) was called first, they
    /// are created on first use from [`Configuration::from_env`]: with the defaults, storages are
    /// files under `./storage`, as in Crawlee for JS.
    pub fn global() -> &'static Services {
        GLOBAL.get_or_init(|| Services::from_configuration(Configuration::from_env()))
    }

    /// Whether the process-wide services exist yet (read or set). An SDK that installs its own
    /// services checks this to detect storages used before it was initialized.
    pub fn is_global_set() -> bool {
        GLOBAL.get().is_some()
    }

    /// Sets the process-wide services. Fails once they have been read or set.
    pub fn set_global(services: Services) -> Result<(), ServiceConflictError> {
        GLOBAL.set(services).map_err(|_| ServiceConflictError)
    }

    /// Empties the run-scoped storages (the default and aliased ones) if
    /// [`purge_on_start`](Configuration::purge_on_start) is set. Only the first call purges;
    /// crawlers call it before they open their storages.
    pub async fn purge_on_start(&self) -> StorageResult<()> {
        if !self.configuration.purge_on_start {
            return Ok(());
        }
        self.purged.get_or_try_init(|| self.storage.purge()).await?;
        Ok(())
    }

    /// A value saved in the default key-value store under `key` on every `PersistState` event,
    /// like `KeyValueStore.getAutoSavedValue()` in JS. The first call loads the saved record
    /// (or starts from `default`); later calls with the same key return the same value.
    pub async fn auto_saved_value<T>(
        &self,
        key: &str,
        default: impl Fn() -> T + Send + Sync + 'static,
    ) -> StorageResult<Arc<SerdeState<T>>>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let mut values = self.auto_saved.lock().await;
        if let Some(value) = values.get(key) {
            return value.clone().downcast::<SerdeState<T>>().map_err(|_| {
                StorageError::InvalidArgument(format!("the value saved under '{key}' is used with another type"))
            });
        }
        let state = Arc::new(SerdeState::new(default));
        // Never torn down: the final `PersistState` of `EventManager::close` saves it.
        RecoverableState::new(self, key, state.clone(), true).initialize().await?;
        values.insert(key.to_owned(), state.clone());
        Ok(state)
    }

    /// Opens a dataset, returning the instance already open for the same id, name or alias
    /// (`StorageInstanceManager.openStorage` in JS). The run-scoped storages are purged first if
    /// [`purge_on_start`](Configuration::purge_on_start) is set and they were not yet.
    pub async fn open_dataset(&self, id: &StorageIdentifier) -> StorageResult<Dataset> {
        let storage = self.open_storage(StorageKind::Dataset, id).await?;
        match storage {
            CachedStorage::Dataset(dataset) => Ok(dataset),
            _ => unreachable!("cached by kind"),
        }
    }

    /// Opens a key-value store, sharing one instance per storage like [`open_dataset`](Self::open_dataset).
    pub async fn open_key_value_store(&self, id: &StorageIdentifier) -> StorageResult<KeyValueStore> {
        match self.open_storage(StorageKind::KeyValueStore, id).await? {
            CachedStorage::KeyValueStore(store) => Ok(store),
            _ => unreachable!("cached by kind"),
        }
    }

    /// Opens a request queue, sharing one instance (and so its caches) per storage like
    /// [`open_dataset`](Self::open_dataset).
    pub async fn open_request_queue(&self, id: &StorageIdentifier) -> StorageResult<RequestQueue> {
        match self.open_storage(StorageKind::RequestQueue, id).await? {
            CachedStorage::RequestQueue(queue) => Ok(queue),
            _ => unreachable!("cached by kind"),
        }
    }

    async fn open_storage(&self, kind: StorageKind, identifier: &StorageIdentifier) -> StorageResult<CachedStorage> {
        self.purge_on_start().await?;
        // The same logical storage opened through another backend is another instance.
        let backend_key = Arc::as_ptr(&self.storage) as *const () as usize;
        let lookup = match identifier {
            StorageIdentifier::Default => CacheKey::Alias(DEFAULT_STORAGE_ALIAS.to_owned()),
            StorageIdentifier::Alias(alias) => CacheKey::Alias(alias.clone()),
            StorageIdentifier::Name(name) => CacheKey::Name(name.clone()),
            StorageIdentifier::Id(id) => CacheKey::Id(id.clone()),
        };
        let cache = &self.storages.instances;
        if let Some(cached) = cache.lock().get(kind, backend_key, &lookup) {
            return Ok(cached);
        }

        let _opening = self.storages.opening.lock().await;
        // Another caller may have opened it while this one waited.
        if let Some(cached) = cache.lock().get(kind, backend_key, &lookup) {
            return Ok(cached);
        }
        cache.lock().check_name_alias_conflict(kind, backend_key, &lookup)?;

        let link = CacheLink(Some(Arc::downgrade(cache)));
        let (storage, metadata) = match kind {
            StorageKind::Dataset => {
                let backend = self.storage.create_dataset_backend(identifier).await?;
                let info = backend.get_metadata().await?;
                (CachedStorage::Dataset(Dataset::from_backend(backend).with_cache_link(link)), (info.id, info.name))
            }
            StorageKind::KeyValueStore => {
                let backend = self.storage.create_key_value_store_backend(identifier).await?;
                let info = backend.get_metadata().await?;
                let store = KeyValueStore::from_backend(backend).with_cache_link(link);
                (CachedStorage::KeyValueStore(store), (info.id, info.name))
            }
            StorageKind::RequestQueue => {
                let backend = self.storage.create_request_queue_backend(identifier).await?;
                let info = backend.get_metadata().await?;
                let queue = RequestQueue::from_backend(backend).with_cache_link(link);
                (CachedStorage::RequestQueue(queue), (info.id, info.name))
            }
        };

        let (id, name) = metadata;
        let mut instances = cache.lock();
        instances.insert(kind, backend_key, CacheKey::Id(id), storage.clone());
        if let Some(name) = name {
            instances.insert(kind, backend_key, CacheKey::Name(name), storage.clone());
        }
        if let CacheKey::Alias(alias) = lookup {
            instances.insert(kind, backend_key, CacheKey::Alias(alias), storage.clone());
        }
        Ok(storage)
    }
}

/// The alias the default storage is cached under.
pub const DEFAULT_STORAGE_ALIAS: &str = "__default__";

/// How a storage was asked for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum CacheKey {
    Id(String),
    Name(String),
    Alias(String),
}

#[derive(Clone)]
enum CachedStorage {
    Dataset(Dataset),
    KeyValueStore(KeyValueStore),
    RequestQueue(RequestQueue),
}

impl CachedStorage {
    fn backend_ptr(&self) -> *const () {
        match self {
            CachedStorage::Dataset(dataset) => Arc::as_ptr(dataset.backend()) as *const (),
            CachedStorage::KeyValueStore(store) => Arc::as_ptr(store.backend()) as *const (),
            CachedStorage::RequestQueue(queue) => Arc::as_ptr(queue.backend()) as *const (),
        }
    }
}

/// The storage instances opened through one [`Services`] (and its clones), by kind, backend and
/// id, name or alias, like the three-tier cache of `StorageInstanceManager` in JS.
#[derive(Default)]
pub(crate) struct StorageInstances {
    entries: HashMap<(StorageKind, usize, CacheKey), CachedStorage>,
}

impl StorageInstances {
    fn get(&self, kind: StorageKind, backend: usize, key: &CacheKey) -> Option<CachedStorage> {
        self.entries.get(&(kind, backend, key.clone())).cloned()
    }

    fn insert(&mut self, kind: StorageKind, backend: usize, key: CacheKey, storage: CachedStorage) {
        self.entries.insert((kind, backend, key), storage);
    }

    /// The same string cannot name a storage and alias another one.
    fn check_name_alias_conflict(&self, kind: StorageKind, backend: usize, key: &CacheKey) -> StorageResult<()> {
        match key {
            CacheKey::Alias(alias) if self.get(kind, backend, &CacheKey::Name(alias.clone())).is_some() => {
                Err(StorageError::InvalidArgument(format!(
                    "Cannot open storage with alias \"{alias}\" because a named storage with the same identifier already exists."
                )))
            }
            CacheKey::Name(name) if self.get(kind, backend, &CacheKey::Alias(name.clone())).is_some() => {
                Err(StorageError::InvalidArgument(format!(
                    "Cannot open storage with name \"{name}\" because an alias storage with the same identifier already exists. \
                     If you meant to open the alias storage, use StorageIdentifier::Alias(\"{name}\") instead."
                )))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Default)]
struct StorageCache {
    instances: Arc<Mutex<StorageInstances>>,
    /// Held while a storage is created, so that concurrent opens create it once.
    opening: tokio::sync::Mutex<()>,
}

/// Lets a storage opened through [`Services`] leave the cache when it is dropped.
#[derive(Clone, Default)]
pub(crate) struct CacheLink(Option<Weak<Mutex<StorageInstances>>>);

impl CacheLink {
    /// Forgets every cache entry of the storage with this backend.
    pub(crate) fn evict(&self, backend: *const ()) {
        if let Some(instances) = self.0.as_ref().and_then(Weak::upgrade) {
            instances.lock().entries.retain(|_, storage| storage.backend_ptr() != backend);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::storage::backend::{DatasetBackend, KeyValueStoreBackend, RequestQueueBackend};
    use crate::storage::request_queue::RequestManager;

    /// Counts how often each kind of storage is opened on the backend.
    #[derive(Default)]
    struct CountingBackend {
        inner: MemoryStorageBackend,
        opened: AtomicUsize,
    }

    #[async_trait]
    impl StorageBackend for CountingBackend {
        async fn create_dataset_backend(&self, id: &StorageIdentifier) -> StorageResult<Arc<dyn DatasetBackend>> {
            self.opened.fetch_add(1, Ordering::SeqCst);
            self.inner.create_dataset_backend(id).await
        }
        async fn create_key_value_store_backend(
            &self,
            id: &StorageIdentifier,
        ) -> StorageResult<Arc<dyn KeyValueStoreBackend>> {
            self.opened.fetch_add(1, Ordering::SeqCst);
            self.inner.create_key_value_store_backend(id).await
        }
        async fn create_request_queue_backend(
            &self,
            id: &StorageIdentifier,
        ) -> StorageResult<Arc<dyn RequestQueueBackend>> {
            self.opened.fetch_add(1, Ordering::SeqCst);
            self.inner.create_request_queue_backend(id).await
        }
    }

    fn counting() -> (Services, Arc<CountingBackend>) {
        let backend = Arc::new(CountingBackend::default());
        (Services::new(backend.clone()), backend)
    }

    #[tokio::test]
    async fn every_open_of_a_storage_shares_one_instance() {
        let (services, backend) = counting();

        let default = services.open_dataset(&StorageIdentifier::Default).await.unwrap();
        let alias = services.open_dataset(&StorageIdentifier::alias("__default__")).await.unwrap();
        assert!(default.same_storage(&alias));
        assert_eq!(backend.opened.load(Ordering::SeqCst), 1);

        let named = services.open_dataset(&StorageIdentifier::name("products")).await.unwrap();
        let id = named.get_info().await.unwrap().id;
        services.open_dataset(&StorageIdentifier::name("products")).await.unwrap();
        services.open_dataset(&StorageIdentifier::id(&id)).await.unwrap();
        assert_eq!(backend.opened.load(Ordering::SeqCst), 2);

        // Other kinds are cached separately.
        services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        assert_eq!(backend.opened.load(Ordering::SeqCst), 3);

        // Clones of the services share the cache, and so do the queues' caches.
        let queue = services.open_request_queue(&StorageIdentifier::Default).await.unwrap();
        queue.add_request("https://example.com", false).await.unwrap();
        let again = services.clone().open_request_queue(&StorageIdentifier::Default).await.unwrap();
        assert_eq!(backend.opened.load(Ordering::SeqCst), 4);
        assert!(again.add_request("https://example.com", false).await.unwrap().was_already_present);
        assert!(again.is_finished().await.is_ok());
    }

    #[tokio::test]
    async fn a_dropped_storage_is_opened_anew() {
        let (services, backend) = counting();
        let named = services.open_dataset(&StorageIdentifier::name("products")).await.unwrap();
        named.drop_storage().await.unwrap();
        services.open_dataset(&StorageIdentifier::name("products")).await.unwrap();
        assert_eq!(backend.opened.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_name_cannot_also_be_an_alias() {
        let (services, _) = counting();
        services.open_dataset(&StorageIdentifier::name("shared")).await.unwrap();
        let err = services.open_dataset(&StorageIdentifier::alias("shared")).await.unwrap_err();
        assert!(err.to_string().contains("a named storage with the same identifier already exists"), "{err}");

        services.open_key_value_store(&StorageIdentifier::alias("run")).await.unwrap();
        let err = services.open_key_value_store(&StorageIdentifier::name("run")).await.unwrap_err();
        assert!(err.to_string().contains("an alias storage with the same identifier already exists"), "{err}");
    }
}
