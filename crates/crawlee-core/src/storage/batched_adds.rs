//! Adding requests in batches (`addRequestsBatched` / `drainRequestBatches` of Crawlee for JS): the
//! first batch is added before returning, the rest in the background, so that a crawl starts on
//! the first requests while the others are still being added.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;

use super::backend::ProcessedRequest;
use crate::errors::StorageResult;
use crate::request::Request;

/// Options of [`RequestManager::add_requests_batched`](super::request_queue::RequestManager::add_requests_batched).
#[derive(Clone, Debug)]
pub struct AddRequestsBatchedOptions {
    pub forefront: bool,
    /// Wait for every batch to be added, not just the first.
    pub wait_for_all_requests_to_be_added: bool,
    /// Requests per batch. 1000 by default.
    pub batch_size: usize,
    /// Pause after each background batch. 1 s by default.
    pub wait_between_batches: Duration,
}

impl Default for AddRequestsBatchedOptions {
    fn default() -> Self {
        AddRequestsBatchedOptions {
            forefront: false,
            wait_for_all_requests_to_be_added: false,
            batch_size: 1000,
            wait_between_batches: Duration::from_millis(1000),
        }
    }
}

/// What [`RequestManager::add_requests_batched`](super::request_queue::RequestManager::add_requests_batched)
/// added before returning, and the batches still being added.
#[derive(Debug, Default)]
pub struct AddRequestsBatchedResult {
    /// The requests added so far: the first batch, or all of them when
    /// [`wait_for_all_requests_to_be_added`](AddRequestsBatchedOptions::wait_for_all_requests_to_be_added)
    /// is set.
    pub added_requests: Vec<ProcessedRequest>,
    /// The batches added in the background, with the requests they add. `None` when there are none.
    /// Dropping it leaves them running.
    pub wait_for_all_requests_to_be_added: Option<JoinHandle<StorageResult<Vec<ProcessedRequest>>>>,
}

/// Counts background batches that have not finished, so that a request manager does not report
/// itself finished while requests are still being added (`#inProgressRequestBatchCount`).
#[derive(Clone, Debug, Default)]
pub struct InProgressBatches(Arc<AtomicUsize>);

impl InProgressBatches {
    pub fn any(&self) -> bool {
        self.0.load(Ordering::Acquire) > 0
    }

    fn start(&self) -> InProgressGuard {
        self.0.fetch_add(1, Ordering::AcqRel);
        InProgressGuard(self.0.clone())
    }
}

/// Decrements the count when the background batches end, even if they panic.
struct InProgressGuard(Arc<AtomicUsize>);

impl Drop for InProgressGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Adds `requests` in batches of [`batch_size`](AddRequestsBatchedOptions::batch_size) with
/// `process_chunk`: the first before returning, the rest in a background task paced by
/// [`wait_between_batches`](AddRequestsBatchedOptions::wait_between_batches) and counted in
/// `in_progress` (`drainRequestBatches`). `process_chunk` gets whether the chunk is the first.
pub async fn drain_request_batches<F, Fut>(
    mut requests: Vec<Request>,
    options: &AddRequestsBatchedOptions,
    in_progress: &InProgressBatches,
    process_chunk: F,
) -> StorageResult<AddRequestsBatchedResult>
where
    F: Fn(Vec<Request>, bool) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = StorageResult<Vec<ProcessedRequest>>> + Send + 'static,
{
    if requests.is_empty() {
        return Ok(AddRequestsBatchedResult::default());
    }
    let batch_size = options.batch_size.max(1);
    let rest = requests.split_off(batch_size.min(requests.len()));
    let mut added_requests = process_chunk(requests, true).await?;
    if rest.is_empty() {
        return Ok(AddRequestsBatchedResult { added_requests, wait_for_all_requests_to_be_added: None });
    }

    let guard = in_progress.start();
    let wait_between_batches = options.wait_between_batches;
    let remainder = tokio::spawn(async move {
        let _guard = guard;
        let mut added = Vec::new();
        let mut rest = rest;
        while !rest.is_empty() {
            let next = rest.split_off(batch_size.min(rest.len()));
            let chunk = std::mem::replace(&mut rest, next);
            added.extend(process_chunk(chunk, false).await?);
            tokio::time::sleep(wait_between_batches).await;
        }
        Ok(added)
    });

    if options.wait_for_all_requests_to_be_added {
        let rest_added = remainder.await.map_err(|err| crate::errors::StorageError::Backend(Box::new(err)))??;
        added_requests.extend(rest_added);
        return Ok(AddRequestsBatchedResult { added_requests, wait_for_all_requests_to_be_added: None });
    }
    Ok(AddRequestsBatchedResult { added_requests, wait_for_all_requests_to_be_added: Some(remainder) })
}
