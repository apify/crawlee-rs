//! Core types of crawlee-rs: [`Request`], the storages ([`Dataset`], [`KeyValueStore`],
//! [`RequestQueue`]), the storage backend contract and request-scoped storage transactions.

pub mod configuration;
pub mod errors;
pub mod events;
pub mod recoverable_state;
pub mod request;
pub mod services;
pub mod storage;
pub mod system_info;
pub mod transaction;

pub use crate::configuration::Configuration;
pub use crate::errors::{
    CriticalError, NonRetryableError, RequestThrottledError, RetryRequestError, SessionError, StateValidationError,
    StorageError, StorageResult,
};
pub use crate::events::{Event, EventKind, EventManager, ExitOptions, StatusLevel, StatusMessage};
pub use crate::recoverable_state::{PersistedState, RecoverableState, SerdeState};
pub use crate::request::{Request, RequestBuilder};
pub use crate::services::Services;
pub use crate::storage::backend::{StorageBackend, StorageIdentifier};
pub use crate::storage::dataset::{Dataset, DatasetExportOptions, DatasetStats};
#[cfg(feature = "fs-storage")]
pub use crate::storage::file_system::FileSystemStorageBackend;
pub use crate::storage::key_value_store::{KeyValueStore, KeyValueStoreStats, RecordValue};
pub use crate::storage::memory::MemoryStorageBackend;
pub use crate::storage::request_list::{RequestList, RequestListOptions, RequestListSource, RequestListState};
pub use crate::storage::request_loader::{
    LoaderStatus, PacingScope, PacingSignal, RequestLoader, RequestManagerTandem,
};
pub use crate::storage::request_queue::{
    RequestManager, RequestQueue, RequestQueueStats, RequestsFromUrl, UrlListFetcher,
};
pub use crate::system_info::SystemInfo;
pub use crate::transaction::StorageTransaction;

pub use crawlee_utils::EnqueueStrategy;

/// The current time as an ISO 8601 string with millisecond precision, like `Date.toISOString()`.
pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
