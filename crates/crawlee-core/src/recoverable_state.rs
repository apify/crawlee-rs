//! [`RecoverableState`]: state that survives a restart or a migration, saved to a key-value store
//! on every [`Event::PersistState`](crate::events::Event::PersistState). It is the counterpart
//! of `RecoverableState` in Crawlee for JS.
//!
//! The state itself lives in a type implementing [`PersistedState`], which decides what the record
//! looks like. For plain serde types, [`SerdeState`] does that. The statistics and the session
//! pool implement it themselves, so their records keep the exact shape Crawlee for JS writes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Mutex, MutexGuard};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::errors::{StateValidationError, StorageError, StorageResult};
use crate::events::{EventKind, ListenerId};
use crate::services::Services;
use crate::storage::backend::StorageIdentifier;
use crate::storage::key_value_store::KeyValueStore;

/// Why a saved record could not be restored.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

const PERSISTENCE_TIMEOUT: Duration = Duration::from_secs(60);

/// State a [`RecoverableState`] can save and restore. Implementations use interior mutability:
/// the state is shared between whoever updates it and the persistence listener.
pub trait PersistedState: Send + Sync + 'static {
    /// The record to save, as JSON.
    fn to_record(&self) -> Value;
    /// Replaces the state with a saved record. An error leaves the state as it was and fails
    /// [`RecoverableState::initialize`] with a [`StateValidationError`].
    fn restore(&self, record: Value) -> Result<(), BoxError>;
    /// Back to the initial state.
    fn reset(&self);
}

/// A [`PersistedState`] for any serde type: the record is the value's JSON.
pub struct SerdeState<T> {
    value: Mutex<T>,
    default: Box<dyn Fn() -> T + Send + Sync>,
}

impl<T> SerdeState<T> {
    pub fn new(default: impl Fn() -> T + Send + Sync + 'static) -> Self {
        SerdeState { value: Mutex::new(default()), default: Box::new(default) }
    }

    /// Locks the value for reading or changing it. Keep the guard short-lived; it blocks
    /// persisting the value too.
    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.value.lock()
    }
}

impl<T: Serialize + DeserializeOwned + Send + 'static> PersistedState for SerdeState<T> {
    fn to_record(&self) -> Value {
        serde_json::to_value(&*self.value.lock()).unwrap_or(Value::Null)
    }

    fn restore(&self, record: Value) -> Result<(), BoxError> {
        *self.value.lock() = serde_json::from_value(record)?;
        Ok(())
    }

    fn reset(&self) {
        *self.value.lock() = (self.default)();
    }
}

struct Inner<S> {
    key: String,
    persistence_enabled: bool,
    state: Arc<S>,
    services: Services,
    store_identifier: StorageIdentifier,
    store: OnceCell<KeyValueStore>,
    listener: Mutex<Option<ListenerId>>,
    initialized: AtomicBool,
    loaded: AtomicBool,
}

/// State saved under `key` in a key-value store (the default one unless set otherwise).
pub struct RecoverableState<S> {
    inner: Arc<Inner<S>>,
}

impl<S> Clone for RecoverableState<S> {
    fn clone(&self) -> Self {
        RecoverableState { inner: self.inner.clone() }
    }
}

impl<S> std::fmt::Debug for RecoverableState<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoverableState").field("key", &self.inner.key).finish_non_exhaustive()
    }
}

impl<S: PersistedState> RecoverableState<S> {
    /// With `persistence_enabled: false`, the state is never saved or loaded, only kept in memory.
    pub fn new(services: &Services, key: impl Into<String>, state: Arc<S>, persistence_enabled: bool) -> Self {
        Self::in_store(services, StorageIdentifier::Default, key, state, persistence_enabled)
    }

    /// Like [`new`](Self::new), saving to the key-value store `store` instead of the default one.
    pub fn in_store(
        services: &Services,
        store: StorageIdentifier,
        key: impl Into<String>,
        state: Arc<S>,
        persistence_enabled: bool,
    ) -> Self {
        RecoverableState {
            inner: Arc::new(Inner {
                key: key.into(),
                persistence_enabled,
                state,
                services: services.clone(),
                store_identifier: store,
                store: OnceCell::new(),
                listener: Mutex::new(None),
                initialized: AtomicBool::new(false),
                loaded: AtomicBool::new(false),
            }),
        }
    }

    pub fn key(&self) -> &str {
        &self.inner.key
    }

    pub fn state(&self) -> &Arc<S> {
        &self.inner.state
    }

    async fn store(&self) -> StorageResult<&KeyValueStore> {
        let inner = &self.inner;
        inner.store.get_or_try_init(|| inner.services.open_key_value_store(&inner.store_identifier)).await
    }

    /// Loads the saved record (once), then starts saving on every `PersistState` event.
    pub async fn initialize(&self) -> StorageResult<()> {
        if self.inner.initialized.swap(true, Ordering::AcqRel) || !self.inner.persistence_enabled {
            return Ok(());
        }
        let store = self.store().await?;

        // Load first, then listen: an event arriving in between would otherwise save the
        // initial state over the record before it is read.
        let mut invalid = None;
        if !self.inner.loaded.swap(true, Ordering::AcqRel) {
            let record: Option<Value> = tokio::time::timeout(PERSISTENCE_TIMEOUT, store.get_value(&self.inner.key))
                .await
                .map_err(|_| self.timeout_error("Loading"))??;
            if let Some(record) = record.filter(|record| !record.is_null())
                && let Err(source) = self.inner.state.restore(record)
            {
                invalid = Some(StateValidationError { persist_state_key: self.inner.key.clone(), source });
            }
        }

        let this = self.clone();
        let id = self.inner.services.events.on(EventKind::PersistState, move |_| {
            let this = this.clone();
            async move { this.persist_quietly().await }
        });
        *self.inner.listener.lock() = Some(id);
        // As in JS, a record that cannot be restored fails the initialization, leaving the state
        // initialized (and saved on events) with its default value.
        match invalid {
            Some(err) => Err(err.into()),
            None => Ok(()),
        }
    }

    /// Stops saving on events and saves one last time.
    pub async fn teardown(&self) {
        self.inner.initialized.store(false, Ordering::Release);
        if let Some(id) = self.inner.listener.lock().take() {
            self.inner.services.events.off(id);
        }
        self.persist_quietly().await;
    }

    /// Resets the state (not the saved record).
    pub fn reset(&self) {
        self.inner.state.reset();
    }

    /// Deletes the saved record. Fails while the state is being saved on events, which would
    /// write it straight back.
    pub async fn reset_store(&self) -> StorageResult<()> {
        if self.inner.listener.lock().is_some() {
            return Err(StorageError::InvalidArgument(format!(
                "Cannot clear the state persisted under key '{}' while it is still being persisted; call teardown() first",
                self.inner.key
            )));
        }
        if !self.inner.persistence_enabled {
            return Ok(());
        }
        self.store().await?.delete_value(&self.inner.key).await
    }

    /// Saves the state now.
    pub async fn persist(&self) -> StorageResult<()> {
        if !self.inner.persistence_enabled {
            return Ok(());
        }
        let store = self.store().await?;
        tracing::debug!(key = %self.inner.key, "Persisting state.");
        let record = self.inner.state.to_record();
        tokio::time::timeout(PERSISTENCE_TIMEOUT, store.set_value(&self.inner.key, &record))
            .await
            .map_err(|_| self.timeout_error("Persisting"))?
    }

    async fn persist_quietly(&self) {
        if let Err(err) = self.persist().await {
            tracing::warn!(key = %self.inner.key, "Failed to persist the state: {err}");
        }
    }

    fn timeout_error(&self, action: &str) -> StorageError {
        StorageError::Backend(
            format!(
                "{action} the state under key '{}' timed out after {} seconds",
                self.inner.key,
                PERSISTENCE_TIMEOUT.as_secs()
            )
            .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct Counter {
        count: u32,
    }

    #[tokio::test]
    async fn saves_on_events_and_restores() {
        let services = Services::in_memory();
        let state = RecoverableState::new(&services, "COUNTER", Arc::new(SerdeState::new(Counter::default)), true);
        state.initialize().await.unwrap();
        state.state().lock().count = 5;

        services.events.emit(crate::events::Event::PersistState { is_migrating: false });
        services.events.wait_for_all_listeners_to_complete().await;
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        assert_eq!(store.get_value::<Counter>("COUNTER").await.unwrap(), Some(Counter { count: 5 }));

        // A second instance, as after a restart, picks the record up.
        let restored = RecoverableState::new(&services, "COUNTER", Arc::new(SerdeState::new(Counter::default)), true);
        restored.initialize().await.unwrap();
        assert_eq!(restored.state().lock().count, 5);

        assert!(state.reset_store().await.is_err(), "still listening");
        state.state().lock().count = 6;
        state.teardown().await;
        assert_eq!(store.get_value::<Counter>("COUNTER").await.unwrap(), Some(Counter { count: 6 }));
        state.reset_store().await.unwrap();
        assert!(!store.record_exists("COUNTER").await.unwrap());
    }

    /// A key-value store in which a `PersistState` event arrives while a record is being read,
    /// as the first periodic event of an `EventManager` can.
    struct EventDuringRead {
        inner: Arc<dyn crate::storage::backend::KeyValueStoreBackend>,
        events: crate::EventManager,
    }

    #[async_trait::async_trait]
    impl crate::storage::backend::KeyValueStoreBackend for EventDuringRead {
        async fn get_metadata(&self) -> StorageResult<crate::storage::backend::KeyValueStoreInfo> {
            self.inner.get_metadata().await
        }
        async fn drop_storage(&self) -> StorageResult<()> {
            self.inner.drop_storage().await
        }
        async fn purge(&self) -> StorageResult<()> {
            self.inner.purge().await
        }
        async fn get_value(&self, key: &str) -> StorageResult<Option<crate::storage::backend::KeyValueStoreRecord>> {
            self.events.emit(crate::events::Event::PersistState { is_migrating: false });
            self.events.wait_for_all_listeners_to_complete().await;
            self.inner.get_value(key).await
        }
        async fn set_value(&self, record: crate::storage::backend::KeyValueStoreRecord) -> StorageResult<()> {
            self.inner.set_value(record).await
        }
        async fn delete_value(&self, key: &str) -> StorageResult<()> {
            self.inner.delete_value(key).await
        }
        async fn list_keys(
            &self,
            options: crate::storage::backend::KeyValueStoreListKeysOptions,
        ) -> StorageResult<crate::storage::backend::KeyValueStoreListKeysResult> {
            self.inner.list_keys(options).await
        }
        async fn record_exists(&self, key: &str) -> StorageResult<bool> {
            self.inner.record_exists(key).await
        }
    }

    #[tokio::test]
    async fn an_event_during_loading_does_not_overwrite_the_saved_record() {
        let services = Services::in_memory();
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        store.set_value("COUNTER", &Counter { count: 5 }).await.unwrap();

        let racing = Arc::new(EventDuringRead { inner: store.backend().clone(), events: services.events.clone() });
        let state = RecoverableState::new(&services, "COUNTER", Arc::new(SerdeState::new(Counter::default)), true);
        state.inner.store.set(KeyValueStore::from_backend(racing)).unwrap();
        state.initialize().await.unwrap();

        assert_eq!(state.state().lock().count, 5);
        assert_eq!(store.get_value::<Counter>("COUNTER").await.unwrap(), Some(Counter { count: 5 }));
    }

    #[tokio::test]
    async fn an_invalid_saved_record_fails_initialization_on_the_default_state() {
        let services = Services::in_memory();
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        store.set_value("COUNTER", &serde_json::json!({ "count": "five" })).await.unwrap();

        let state = RecoverableState::new(&services, "COUNTER", Arc::new(SerdeState::new(Counter::default)), true);
        let err = state.initialize().await.unwrap_err();
        assert!(
            matches!(&err, StorageError::StateValidation(invalid) if invalid.persist_state_key == "COUNTER"),
            "{err}"
        );
        assert!(err.to_string().starts_with("State persisted under key 'COUNTER' failed validation"), "{err}");

        // Left running on the default state, saving it like any other.
        assert_eq!(state.state().lock().count, 0);
        assert_eq!(services.events.listener_count(EventKind::PersistState), 1);
        state.initialize().await.unwrap();
    }

    #[tokio::test]
    async fn disabled_persistence_stays_in_memory() {
        let services = Services::in_memory();
        let state = RecoverableState::new(&services, "X", Arc::new(SerdeState::new(Counter::default)), false);
        state.initialize().await.unwrap();
        state.state().lock().count = 1;
        state.teardown().await;
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        assert!(!store.record_exists("X").await.unwrap());
        assert_eq!(services.events.listener_count(EventKind::PersistState), 0);
    }
}
