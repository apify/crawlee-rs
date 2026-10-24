//! Storages and the storage backend contract.

pub mod backend;
pub mod batched_adds;
pub mod dataset;
#[cfg(feature = "fs-storage")]
pub mod file_system;
pub mod key_value_store;
pub mod memory;
pub mod request_loader;
pub mod request_queue;
