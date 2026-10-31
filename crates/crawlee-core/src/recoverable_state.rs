//! [`RecoverableState`]: state that survives a restart or a migration, saved to a key-value store
//! on every [`Event::PersistState`](crate::events::Event::PersistState). It is the counterpart
//! of `RecoverableState` in Crawlee for JS.
//!
//! Unlike in JS, a record equal to the one last saved (or restored) is not written again, and the
//! state can also be saved on an interval of its own ([`RecoverableState::persist_every`]).
//!
//! The state itself lives in a type implementing [`PersistedState`], which decides what the record
//! looks like. For plain serde types, [`SerdeState`] does that. The statistics and the session
//! pool implement it themselves, so their records keep the exact shape Crawlee for JS writes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
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
    /// The record last saved or restored, which a save leaves alone when the state still gives it.
    /// Its lock also keeps saves from overtaking each other.
    saved: tokio::sync::Mutex<Option<Value>>,
    interval: Mutex<Option<Duration>>,
    ticker: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
                saved: tokio::sync::Mutex::new(None),
                interval: Mutex::new(None),
                ticker: Mutex::new(None),
            }),
        }
    }

    /// Also saves the state every `interval` from [`initialize`](Self::initialize) until
    /// [`teardown`](Self::teardown), when it changed: for state that should not wait for the next
    /// `PersistState` event (every 60 s by default).
    pub fn persist_every(self, interval: Duration) -> Self {
        *self.inner.interval.lock() = Some(interval);
        self
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
            if let Some(record) = record.filter(|record| !record.is_null()) {
                match self.inner.state.restore(record.clone()) {
                    Ok(()) => *self.inner.saved.lock().await = Some(record),
                    Err(source) => {
                        invalid = Some(StateValidationError { persist_state_key: self.inner.key.clone(), source })
                    }
                }
            }
        }

        let this = self.clone();
        let id = self.inner.services.events.on(EventKind::PersistState, move |_| {
            let this = this.clone();
            async move { this.persist_quietly().await }
        });
        *self.inner.listener.lock() = Some(id);
        let interval = *self.inner.interval.lock();
        if let Some(interval) = interval {
            let inner = Arc::downgrade(&self.inner);
            *self.inner.ticker.lock() = Some(tokio::spawn(persist_periodically(inner, interval)));
        }
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
        if let Some(ticker) = self.inner.ticker.lock().take() {
            ticker.abort();
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
        self.store().await?.delete_value(&self.inner.key).await?;
        *self.inner.saved.lock().await = None;
        Ok(())
    }

    /// Saves the state now, unless the record would be the one last saved or restored.
    pub async fn persist(&self) -> StorageResult<()> {
        if !self.inner.persistence_enabled {
            return Ok(());
        }
        let store = self.store().await?;
        let mut saved = self.inner.saved.lock().await;
        let record = self.inner.state.to_record();
        if saved.as_ref() == Some(&record) {
            return Ok(());
        }
        tracing::debug!(key = %self.inner.key, "Persisting state.");
        tokio::time::timeout(PERSISTENCE_TIMEOUT, store.set_value(&self.inner.key, &record))
            .await
            .map_err(|_| self.timeout_error("Persisting"))??;
        *saved = Some(record);
        Ok(())
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

/// Saves the state every `interval` while it exists.
async fn persist_periodically<S: PersistedState>(inner: Weak<Inner<S>>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(inner) = inner.upgrade() else { return };
        RecoverableState { inner }.persist_quietly().await;
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

    /// A key-value store that counts the records written.
    struct CountingWrites {
        inner: Arc<dyn crate::storage::backend::KeyValueStoreBackend>,
        writes: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::storage::backend::KeyValueStoreBackend for CountingWrites {
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
            self.inner.get_value(key).await
        }
        async fn set_value(&self, record: crate::storage::backend::KeyValueStoreRecord) -> StorageResult<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
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

    /// A state saving into a [`CountingWrites`] store over the default one.
    async fn counted_state(services: &Services) -> (RecoverableState<SerdeState<Counter>>, Arc<CountingWrites>) {
        let state = RecoverableState::new(services, "COUNTER", Arc::new(SerdeState::new(Counter::default)), true);
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        let counting = Arc::new(CountingWrites { inner: store.backend().clone(), writes: Default::default() });
        state.inner.store.set(KeyValueStore::from_backend(counting.clone())).unwrap();
        (state, counting)
    }

    #[tokio::test]
    async fn an_unchanged_state_is_not_saved_again() {
        let services = Services::in_memory();
        let (state, counting) = counted_state(&services).await;
        state.initialize().await.unwrap();
        let writes = || counting.writes.load(Ordering::SeqCst);

        state.persist().await.unwrap();
        assert_eq!(writes(), 1, "the first save writes the record");
        for _ in 0..3 {
            services.events.emit(crate::events::Event::PersistState { is_migrating: false });
            services.events.wait_for_all_listeners_to_complete().await;
        }
        state.persist().await.unwrap();
        assert_eq!(writes(), 1, "nothing changed since");

        state.state().lock().count = 1;
        services.events.emit(crate::events::Event::PersistState { is_migrating: false });
        services.events.wait_for_all_listeners_to_complete().await;
        assert_eq!(writes(), 2);
        state.teardown().await;
        assert_eq!(writes(), 2, "the last save has nothing new either");

        // A restored record counts as saved.
        let (restored, counting) = counted_state(&services).await;
        restored.initialize().await.unwrap();
        assert_eq!(restored.state().lock().count, 1);
        restored.persist().await.unwrap();
        assert_eq!(counting.writes.load(Ordering::SeqCst), 0);
        restored.state().lock().count = 2;
        restored.teardown().await;
        assert_eq!(counting.writes.load(Ordering::SeqCst), 1);

        // Once its record is deleted, the state is saved again.
        restored.reset_store().await.unwrap();
        restored.persist().await.unwrap();
        assert_eq!(counting.writes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn saves_on_an_interval_when_the_state_changed() {
        let services = Services::in_memory();
        let (state, counting) = counted_state(&services).await;
        let state = state.persist_every(Duration::from_secs(5));
        state.initialize().await.unwrap();
        let writes = || counting.writes.load(Ordering::SeqCst);

        tokio::time::sleep(Duration::from_millis(5100)).await;
        assert_eq!(writes(), 1, "the initial state, saved once");
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(writes(), 1, "unchanged");

        state.state().lock().count = 1;
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(writes(), 2);
        let store = services.open_key_value_store(&StorageIdentifier::Default).await.unwrap();
        assert_eq!(store.get_value::<Counter>("COUNTER").await.unwrap(), Some(Counter { count: 1 }));

        // The interval stops with the events.
        state.teardown().await;
        state.state().lock().count = 2;
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(writes(), 2);
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
