//! Adding requests in batches (`addRequestsBatched` / `drainRequestBatches` of Crawlee for JS): the
//! first batch is added before returning, the rest in the background, so that a crawl starts on
//! the first requests while the others are still being added.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
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
    /// Add at most this many requests that are new to the manager (not already present). The rest
    /// is returned in [`requests_over_limit`](AddRequestsBatchedResult::requests_over_limit).
    /// Setting it waits for every batch, as
    /// [`wait_for_all_requests_to_be_added`](Self::wait_for_all_requests_to_be_added) does.
    pub max_new_requests: Option<usize>,
}

impl Default for AddRequestsBatchedOptions {
    fn default() -> Self {
        AddRequestsBatchedOptions {
            forefront: false,
            wait_for_all_requests_to_be_added: false,
            batch_size: 1000,
            wait_between_batches: Duration::from_millis(1000),
            max_new_requests: None,
        }
    }
}

/// What [`RequestManager::add_requests_batched`](super::request_queue::RequestManager::add_requests_batched)
/// added before returning, and the batches still being added.
#[derive(Debug, Default)]
pub struct AddRequestsBatchedResult {
    /// The requests added so far: the first batch, or all of them when
    /// [`wait_for_all_requests_to_be_added`](AddRequestsBatchedOptions::wait_for_all_requests_to_be_added)
    /// or [`max_new_requests`](AddRequestsBatchedOptions::max_new_requests) is set.
    pub added_requests: Vec<ProcessedRequest>,
    /// The batches added in the background, with the requests they add. `None` when there are none.
    /// Dropping it leaves them running.
    pub wait_for_all_requests_to_be_added: Option<JoinHandle<StorageResult<Vec<ProcessedRequest>>>>,
    /// The requests not added because the
    /// [`max_new_requests`](AddRequestsBatchedOptions::max_new_requests) budget was used up.
    pub requests_over_limit: Vec<Request>,
}

/// Counts background batches that have not finished, so that a request manager does not report
/// itself finished while requests are still being added (`#inProgressRequestBatchCount`).
#[derive(Clone, Debug, Default)]
pub struct InProgressBatches(Arc<Mutex<BatchCount>>);

#[derive(Debug, Default)]
struct BatchCount {
    /// Bumped by [`reset`](InProgressBatches::reset): batches started before no longer count.
    generation: u64,
    count: usize,
}

impl InProgressBatches {
    pub fn any(&self) -> bool {
        self.0.lock().count > 0
    }

    /// Forgets the batches being added, as a purge does in JS (`#inProgressRequestBatchCount = 0`).
    /// They keep running, but no longer count, not even when they end.
    pub fn reset(&self) {
        let mut batches = self.0.lock();
        batches.generation += 1;
        batches.count = 0;
    }

    fn start(&self) -> InProgressGuard {
        let mut batches = self.0.lock();
        batches.count += 1;
        InProgressGuard { batches: self.0.clone(), generation: batches.generation }
    }
}

/// Decrements the count when the background batches end, even if they panic.
struct InProgressGuard {
    batches: Arc<Mutex<BatchCount>>,
    generation: u64,
}

impl Drop for InProgressGuard {
    fn drop(&mut self) {
        let mut batches = self.batches.lock();
        if batches.generation == self.generation {
            batches.count -= 1;
        }
    }
}

/// Takes the first `size` requests off `requests`.
fn take_chunk(requests: &mut Vec<Request>, size: usize) -> Vec<Request> {
    let rest = requests.split_off(size.min(requests.len()));
    std::mem::replace(requests, rest)
}

/// Adds every chunk before returning, within the budget of `max_new_requests` (a chunk is never
/// larger than what is left of it), pausing `wait_between_batches` after each chunk but the first.
/// Returns what was added and the requests over the budget.
pub async fn drain_request_batches_inline<F, Fut>(
    mut requests: Vec<Request>,
    batch_size: usize,
    max_new_requests: Option<usize>,
    wait_between_batches: Duration,
    mut process_chunk: F,
) -> StorageResult<(Vec<ProcessedRequest>, Vec<Request>)>
where
    F: FnMut(Vec<Request>, bool) -> Fut,
    Fut: Future<Output = StorageResult<Vec<ProcessedRequest>>>,
{
    let batch_size = batch_size.max(1);
    let mut remaining = max_new_requests.unwrap_or(usize::MAX);
    let mut added = Vec::new();
    let mut is_initial = true;
    while !requests.is_empty() {
        let size = batch_size.min(remaining);
        if size == 0 {
            break;
        }
        let processed = process_chunk(take_chunk(&mut requests, size), is_initial).await?;
        let new = processed.iter().filter(|request| !request.was_already_present).count();
        remaining = remaining.saturating_sub(new);
        added.extend(processed);
        if !is_initial {
            tokio::time::sleep(wait_between_batches).await;
        }
        is_initial = false;
    }
    let over_limit = if max_new_requests.is_some() { requests } else { Vec::new() };
    Ok((added, over_limit))
}

/// Adds `requests` in batches of [`batch_size`](AddRequestsBatchedOptions::batch_size) with
/// `process_chunk`: the first before returning, the rest in a background task paced by
/// [`wait_between_batches`](AddRequestsBatchedOptions::wait_between_batches) and counted in
/// `in_progress` (`drainRequestBatches`). `process_chunk` gets whether the chunk is the first.
///
/// With [`max_new_requests`](AddRequestsBatchedOptions::max_new_requests), only that many
/// requests new to the manager are added, all before returning, and the rest is reported as over
/// the limit.
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

    if options.max_new_requests.is_some() {
        // Whether the budget is used up is only known once the batches before it are added, so
        // every batch is awaited.
        let _guard = (requests.len() > batch_size).then(|| in_progress.start());
        let (added_requests, requests_over_limit) = drain_request_batches_inline(
            requests,
            batch_size,
            options.max_new_requests,
            options.wait_between_batches,
            &process_chunk,
        )
        .await?;
        return Ok(AddRequestsBatchedResult {
            added_requests,
            wait_for_all_requests_to_be_added: None,
            requests_over_limit,
        });
    }

    let rest = requests.split_off(batch_size.min(requests.len()));
    let mut added_requests = process_chunk(requests, true).await?;
    if rest.is_empty() {
        return Ok(AddRequestsBatchedResult { added_requests, ..Default::default() });
    }

    let guard = in_progress.start();
    let wait_between_batches = options.wait_between_batches;
    let remainder = tokio::spawn(async move {
        let _guard = guard;
        let mut added = Vec::new();
        let mut rest = rest;
        while !rest.is_empty() {
            let chunk = take_chunk(&mut rest, batch_size);
            added.extend(process_chunk(chunk, false).await?);
            tokio::time::sleep(wait_between_batches).await;
        }
        Ok(added)
    });

    if options.wait_for_all_requests_to_be_added {
        let rest_added = remainder.await.map_err(|err| crate::errors::StorageError::Backend(Box::new(err)))??;
        added_requests.extend(rest_added);
        return Ok(AddRequestsBatchedResult { added_requests, ..Default::default() });
    }
    Ok(AddRequestsBatchedResult {
        added_requests,
        wait_for_all_requests_to_be_added: Some(remainder),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::StorageIdentifier;
    use crate::storage::memory::MemoryStorageBackend;
    use crate::storage::request_queue::{RequestManager, RequestQueue};

    fn requests(paths: &[&str]) -> Vec<Request> {
        paths.iter().map(|path| Request::from(format!("https://example.com/{path}"))).collect()
    }

    fn urls(requests: &[Request]) -> Vec<&str> {
        requests.iter().map(|request| request.url.as_str()).collect()
    }

    async fn queue() -> RequestQueue {
        RequestQueue::open(&MemoryStorageBackend::new(), &StorageIdentifier::Default).await.unwrap()
    }

    #[tokio::test]
    async fn the_budget_counts_only_new_requests() {
        let queue = queue().await;
        queue.add_requests(requests(&["a", "b"]), false).await.unwrap();
        let options = AddRequestsBatchedOptions {
            batch_size: 2,
            wait_between_batches: Duration::ZERO,
            max_new_requests: Some(2),
            ..Default::default()
        };

        let result = queue.add_requests_batched(requests(&["a", "b", "c", "d", "e"]), options).await.unwrap();
        assert_eq!(result.added_requests.iter().filter(|added| !added.was_already_present).count(), 2);
        assert!(result.wait_for_all_requests_to_be_added.is_none(), "every batch is awaited");
        assert_eq!(urls(&result.requests_over_limit), ["https://example.com/e"]);
        assert_eq!(queue.get_info().await.unwrap().total_request_count, 4);
    }

    #[tokio::test]
    async fn a_batch_is_never_larger_than_the_budget_left() {
        let queue = queue().await;
        let options = AddRequestsBatchedOptions { max_new_requests: Some(3), ..Default::default() };

        let result = queue.add_requests_batched(requests(&["a", "b", "c", "d", "e"]), options).await.unwrap();
        assert_eq!(result.added_requests.len(), 3);
        assert_eq!(urls(&result.requests_over_limit), ["https://example.com/d", "https://example.com/e"]);
        assert_eq!(queue.get_info().await.unwrap().total_request_count, 3);

        let options = AddRequestsBatchedOptions { max_new_requests: Some(0), ..Default::default() };
        let result = queue.add_requests_batched(requests(&["f"]), options).await.unwrap();
        assert!(result.added_requests.is_empty());
        assert_eq!(result.requests_over_limit.len(), 1);
    }

    #[tokio::test]
    async fn pending_requests_are_counted() {
        let queue = queue().await;
        queue.add_requests(requests(&["a", "b", "c"]), false).await.unwrap();
        let mut request = queue.fetch_next_request().await.unwrap().unwrap();
        // A request in progress is still pending.
        assert_eq!(queue.pending_count().await.unwrap(), 3);
        queue.mark_request_as_handled(&mut request).await.unwrap();
        assert_eq!(queue.pending_count().await.unwrap(), 2);
    }
}
