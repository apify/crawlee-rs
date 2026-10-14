//! [`Services`]: the configuration, the storage backend and the event manager shared by the
//! storages and crawlers of a process.
//!
//! Crawlee for JS resolves these through an ambient, `AsyncLocalStorage`-scoped service locator.
//! Here they are explicit: crawlers take a `Services` value (cheap to clone) or fall back to the
//! process-wide default.

use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::configuration::Configuration;
use crate::errors::{StorageError, StorageResult};
use crate::events::EventManager;
use crate::recoverable_state::{RecoverableState, SerdeState};
use crate::storage::backend::{StorageBackend, StorageIdentifier};
use crate::storage::dataset::Dataset;
use crate::storage::key_value_store::KeyValueStore;
use crate::storage::memory::MemoryStorageBackend;
use crate::storage::request_queue::RequestQueue;

#[derive(Clone)]
pub struct Services {
    pub configuration: Rc<Configuration>,
    pub storage: Rc<dyn StorageBackend>,
    pub events: EventManager,
    /// Values of [`auto_saved_value`](Self::auto_saved_value), by key.
    auto_saved: Rc<crate::local::Mutex<HashMap<String, Rc<dyn Any>>>>,
    /// Set once the run-scoped storages were purged, so that they are purged once per backend
    /// even when several crawlers run one after another.
    purged: Rc<crate::local::OnceCell<()>>,
}

impl std::fmt::Debug for Services {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Services").finish_non_exhaustive()
    }
}

thread_local! {
    /// The services of the thread, which is the process's: everything runs on one thread. Leaked
    /// once, so that they can be handed out as `&'static`.
    static GLOBAL: std::cell::OnceCell<&'static Services> = const { std::cell::OnceCell::new() };
}

#[derive(Debug, thiserror::Error)]
#[error("the global services were already initialized; set them before first use")]
pub struct ServiceConflictError;

impl Services {
    /// Services with the default [`Configuration`] and the given storage.
    pub fn new(storage: Rc<dyn StorageBackend>) -> Self {
        Services::with_configuration(Configuration::default(), storage)
    }

    /// Services with the given storage and the local event manager (see [`EventManager::local`]).
    pub fn with_configuration(configuration: Configuration, storage: Rc<dyn StorageBackend>) -> Self {
        let events = EventManager::local(&configuration);
        Services::from_parts(configuration, storage, events)
    }

    pub fn from_parts(configuration: Configuration, storage: Rc<dyn StorageBackend>, events: EventManager) -> Self {
        Services {
            configuration: Rc::new(configuration),
            storage,
            events,
            auto_saved: Rc::default(),
            purged: Rc::default(),
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
                Rc::new(crate::storage::file_system::FileSystemStorageBackend::new(configuration.storage_dir.clone()));
            return Services::with_configuration(configuration, storage);
        }
        Services::with_configuration(configuration, Rc::new(MemoryStorageBackend::new()))
    }

    /// Services backed by a fresh in-memory storage, with the default configuration.
    pub fn in_memory() -> Self {
        Services::new(Rc::new(MemoryStorageBackend::new()))
    }

    /// The process-wide services. Unless [`set_global`](Self::set_global) was called first, they
    /// are created on first use from [`Configuration::from_env`]: with the defaults, storages are
    /// files under `./storage`, as in Crawlee for JS.
    pub fn global() -> &'static Services {
        GLOBAL.with(|global| {
            *global.get_or_init(|| Box::leak(Box::new(Services::from_configuration(Configuration::from_env()))))
        })
    }

    /// Whether the process-wide services exist yet (read or set). An SDK that installs its own
    /// services checks this to detect storages used before it was initialized.
    pub fn is_global_set() -> bool {
        GLOBAL.with(|global| global.get().is_some())
    }

    /// Sets the process-wide services. Fails once they have been read or set.
    pub fn set_global(services: Services) -> Result<(), ServiceConflictError> {
        GLOBAL.with(|global| {
            if global.get().is_some() {
                return Err(ServiceConflictError);
            }
            global.set(Box::leak(Box::new(services))).map_err(|_| ServiceConflictError)
        })
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
        default: impl Fn() -> T + 'static,
    ) -> StorageResult<Rc<SerdeState<T>>>
    where
        T: Serialize + DeserializeOwned + 'static,
    {
        let mut values = self.auto_saved.lock().await;
        if let Some(value) = values.get(key) {
            return value.clone().downcast::<SerdeState<T>>().map_err(|_| {
                StorageError::InvalidArgument(format!("the value saved under '{key}' is used with another type"))
            });
        }
        let state = Rc::new(SerdeState::new(default));
        // Never torn down: the final `PersistState` of `EventManager::close` saves it.
        RecoverableState::new(self, key, state.clone(), true).initialize().await?;
        values.insert(key.to_owned(), state.clone());
        Ok(state)
    }

    pub async fn open_dataset(&self, id: &StorageIdentifier) -> StorageResult<Dataset> {
        Dataset::open(self.storage.as_ref(), id).await
    }

    pub async fn open_key_value_store(&self, id: &StorageIdentifier) -> StorageResult<KeyValueStore> {
        KeyValueStore::open(self.storage.as_ref(), id).await
    }

    pub async fn open_request_queue(&self, id: &StorageIdentifier) -> StorageResult<RequestQueue> {
        RequestQueue::open(self.storage.as_ref(), id).await
    }
}
