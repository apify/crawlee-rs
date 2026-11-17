//! A request queue that starts in memory and moves ("spills") to another backend later.
//!
//! A short crawl never touches the other backend (a remote queue, where every write is an API
//! call), and a long one ends up there, where it survives a restart. [`SpillingRequestQueueBackend`]
//! keeps the requests in memory until [`SpillingOptions::spill_after`] has passed since it was
//! created (30 s by default), until [`SpillingOptions::max_requests`] requests were added, or until
//! [`spill`](SpillingRequestQueueBackend::spill) is called. Then it creates the other backend (the
//! *target*) and commits what it holds:
//!
//! - the handled requests, added as handled (with their `handled_at`), so that adding one again is
//!   still reported as handled, also by a run that resumes from the target;
//! - the pending requests, in the order they would have been fetched, those added to the forefront
//!   to the forefront;
//! - the requests in progress, last, as pending: should the process end before they finish, a run
//!   resuming from the target handles them again. When one finishes, it is marked handled (or
//!   reclaimed) on the target, under the target's id for it. If the target serves one before that
//!   (after every other pending request), the queue skips it, as it is running already, so none
//!   is handled twice.
//!
//! From then on, every call goes to the target. The switch happens once, and calls made while it
//! commits wait for it. The time is checked on each call, so a queue nobody calls does not switch
//! until it is called (or [`spill_if_due`](SpillingRequestQueueBackend::spill_if_due) is).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::time::Instant;

use super::backend::{BatchAddRequestsResult, QueueOperationInfo, RequestQueueBackend, RequestQueueInfo};
use super::memory::MemoryRequestQueue;
use crate::errors::StorageResult;
use crate::request::Request;

/// A boxed future, as the factory and the hook of a [`SpillingRequestQueueBackend`] return.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

type TargetFactory = Arc<dyn Fn() -> BoxFuture<StorageResult<Arc<dyn RequestQueueBackend>>> + Send + Sync>;
type SpillHook = Arc<dyn Fn() -> BoxFuture<StorageResult<()>> + Send + Sync>;

/// How long a [`SpillingRequestQueueBackend`] keeps its requests in memory by default.
pub const DEFAULT_SPILL_AFTER: Duration = Duration::from_secs(30);

/// How long a queue whose switch failed stays in memory before it tries again.
const RETRY_DELAY: Duration = Duration::from_secs(10);

/// When a [`SpillingRequestQueueBackend`] moves to its target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpillingOptions {
    /// Move once this long has passed since the queue was created. `None`: not on time.
    /// [`DEFAULT_SPILL_AFTER`] (30 s) by default.
    pub spill_after: Option<Duration>,
    /// Move once this many requests were added (handled ones included). `None` (the default): not
    /// on a count.
    pub max_requests: Option<u64>,
}

impl Default for SpillingOptions {
    fn default() -> Self {
        SpillingOptions { spill_after: Some(DEFAULT_SPILL_AFTER), max_requests: None }
    }
}

/// A request queue that is in memory first and moves to a target backend later (see the
/// [module documentation](self)).
pub struct SpillingRequestQueueBackend {
    memory: MemoryRequestQueue,
    factory: TargetFactory,
    on_spilled: Option<SpillHook>,
    options: SpillingOptions,
    /// The id the queue reports before the switch.
    id: Option<String>,
    created_at: Instant,
    /// Held for reading by the calls served from memory, and for writing by the switch. It holds
    /// the target once created, so that a switch tried again does not create another.
    switch: tokio::sync::RwLock<Option<Arc<dyn RequestQueueBackend>>>,
    spilled: OnceLock<Spilled>,
    /// Not before then, after a failed switch.
    retry_at: Mutex<Option<Instant>>,
    expected_processing_time: Mutex<Option<Duration>>,
}

/// The queue after the switch.
struct Spilled {
    target: Arc<dyn RequestQueueBackend>,
    /// The requests that were in progress at the switch and are not marked or reclaimed yet, by
    /// unique key. Held while such a request is fetched from, marked on or reclaimed to the target,
    /// so that the target serving it and it finishing are seen in order.
    carried: tokio::sync::Mutex<HashMap<String, Carried>>,
    carried_count: AtomicUsize,
}

/// A request in progress at the switch.
struct Carried {
    /// Its id in the target.
    target_id: String,
    /// Its id in memory, which the crawler handling it knows.
    memory_id: Option<String>,
}

impl std::fmt::Debug for SpillingRequestQueueBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillingRequestQueueBackend")
            .field("options", &self.options)
            .field("spilled", &self.is_spilled())
            .finish_non_exhaustive()
    }
}

impl SpillingRequestQueueBackend {
    /// A queue in memory that moves to the backend `create_target` makes, which is called once,
    /// at the switch (again only if it failed).
    pub fn new<F, Fut>(create_target: F, options: SpillingOptions) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = StorageResult<Arc<dyn RequestQueueBackend>>> + Send + 'static,
    {
        SpillingRequestQueueBackend {
            memory: MemoryRequestQueue::standalone(),
            factory: Arc::new(move || Box::pin(create_target())),
            on_spilled: None,
            options,
            id: None,
            created_at: Instant::now(),
            switch: tokio::sync::RwLock::new(None),
            spilled: OnceLock::new(),
            retry_at: Mutex::new(None),
            expected_processing_time: Mutex::new(None),
        }
    }

    /// Calls `hook` once the switch committed the requests to the target, for example to record
    /// that a resumed run should open the target directly. A failure is logged.
    pub fn on_spilled<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = StorageResult<()>> + Send + 'static,
    {
        self.on_spilled = Some(Arc::new(move || Box::pin(hook())));
        self
    }

    /// The id [`get_metadata`](RequestQueueBackend::get_metadata) reports before the switch (that
    /// of the target, say); a random one by default.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn options(&self) -> &SpillingOptions {
        &self.options
    }

    /// Whether the queue has moved to its target.
    pub fn is_spilled(&self) -> bool {
        self.spilled.get().is_some()
    }

    /// The number of requests in memory that are not handled yet (those in progress included):
    /// what would be lost if the process ended now. 0 after the switch.
    pub fn unhandled_in_memory(&self) -> usize {
        if self.is_spilled() { 0 } else { self.memory.unhandled_count() }
    }

    /// Moves to the target now, unless the queue has moved already. Calls made meanwhile wait for
    /// it. On failure, the queue stays in memory, and what was committed stays in the target (a
    /// switch tried again adds it again, which the target deduplicates).
    pub async fn spill(&self) -> StorageResult<()> {
        if self.is_spilled() {
            return Ok(());
        }
        let mut switch = self.switch.write().await;
        if self.is_spilled() {
            return Ok(());
        }
        let target = match &*switch {
            Some(target) => target.clone(),
            None => {
                let target = (self.factory)().await?;
                *switch = Some(target.clone());
                target
            }
        };
        let started = Instant::now();
        let (committed, carried) = self.commit(&target).await?;
        let expected = *self.expected_processing_time.lock();
        if let Some(duration) = expected {
            target.set_expected_request_processing_time(duration).await?;
        }
        let carried_count = AtomicUsize::new(carried.len());
        let spilled = Spilled { target, carried: tokio::sync::Mutex::new(carried), carried_count };
        if self.spilled.set(spilled).is_err() {
            unreachable!("the queue moves under the write lock, once");
        }
        // Nothing reads the memory any more.
        self.memory.purge_now();
        drop(switch);
        tracing::info!(
            committed,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Moved the request queue from memory to its storage"
        );
        if let Some(hook) = &self.on_spilled
            && let Err(err) = hook().await
        {
            tracing::warn!("Failed to record that the request queue moved from memory: {err}");
        }
        Ok(())
    }

    /// Moves to the target if it is time to (see [`SpillingOptions`]). Returns whether the queue
    /// is in the target now.
    pub async fn spill_if_due(&self) -> StorageResult<bool> {
        if !self.is_spilled() && self.is_due() {
            self.spill().await?;
        }
        Ok(self.is_spilled())
    }

    fn is_due(&self) -> bool {
        if self.retry_at.lock().is_some_and(|at| Instant::now() < at) {
            return false;
        }
        self.options.spill_after.is_some_and(|after| self.created_at.elapsed() >= after)
            || self.options.max_requests.is_some_and(|max| self.memory.request_count() as u64 >= max)
    }

    /// Adds what the memory holds to `target`: the handled requests as handled, the pending ones in
    /// the order they would be fetched, and those in progress last. Returns how many requests it
    /// committed, and the requests in progress the target has not handled.
    async fn commit(&self, target: &Arc<dyn RequestQueueBackend>) -> StorageResult<(usize, HashMap<String, Carried>)> {
        let snapshot = self.memory.snapshot();
        // The target assigns its own ids.
        let without_id = |mut request: Request| {
            request.id = None;
            request
        };
        let committed = snapshot.handled.len() + snapshot.pending.len() + snapshot.in_progress.len();

        if !snapshot.handled.is_empty() {
            let handled = snapshot.handled.into_iter().map(without_id).collect();
            warn_unprocessed(&target.add_batch_of_requests(handled, false).await?);
        }
        let (front, back): (Vec<_>, Vec<_>) = snapshot.pending.into_iter().partition(|(_, forefront)| *forefront);
        if !front.is_empty() {
            // Each request added to the forefront goes before those added before it, so the first
            // to fetch is added last.
            let front = front.into_iter().rev().map(|(request, _)| without_id(request)).collect();
            warn_unprocessed(&target.add_batch_of_requests(front, true).await?);
        }
        if !back.is_empty() {
            let back = back.into_iter().map(|(request, _)| without_id(request)).collect();
            warn_unprocessed(&target.add_batch_of_requests(back, false).await?);
        }

        let mut carried = HashMap::new();
        if !snapshot.in_progress.is_empty() {
            let memory_ids: HashMap<String, Option<String>> =
                snapshot.in_progress.iter().map(|r| (r.unique_key.clone(), r.id.clone())).collect();
            let in_progress = snapshot.in_progress.into_iter().map(without_id).collect();
            let added = target.add_batch_of_requests(in_progress, false).await?;
            warn_unprocessed(&added);
            for processed in added.processed_requests {
                // One the target had handled already is not served again.
                if processed.was_already_handled {
                    continue;
                }
                let memory_id = memory_ids.get(&processed.unique_key).cloned().flatten();
                carried.insert(processed.unique_key, Carried { target_id: processed.request_id, memory_id });
            }
        }
        Ok((committed, carried))
    }
}

/// Requests the target rejects are skipped, as [`RequestQueue`](super::request_queue::RequestQueue)
/// skips them when it adds them.
fn warn_unprocessed(result: &BatchAddRequestsResult) {
    if !result.unprocessed_requests.is_empty() {
        tracing::warn!(
            unprocessed_requests = ?result.unprocessed_requests,
            "Some requests were rejected by the request queue the queue in memory moved to, and are skipped."
        );
    }
}

impl Spilled {
    fn has_carried(&self) -> bool {
        self.carried_count.load(Ordering::Acquire) > 0
    }

    /// The next request of the target, skipping those in progress since before the switch.
    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        if !self.has_carried() {
            return self.target.fetch_next_request().await;
        }
        let carried = self.carried.lock().await;
        loop {
            let Some(request) = self.target.fetch_next_request().await? else { return Ok(None) };
            // Running already: now in progress in the target as well, where it finishes.
            if !carried.contains_key(&request.unique_key) {
                return Ok(Some(request));
            }
        }
    }

    /// Marks `request` handled (`reclaim` `None`) or reclaims it (to the forefront with
    /// `Some(true)`) on the target, under the target's id for it.
    async fn finish(&self, request: &Request, reclaim: Option<bool>) -> StorageResult<Option<QueueOperationInfo>> {
        let finish = async |request: &Request| match reclaim {
            None => self.target.mark_request_as_handled(request).await,
            Some(forefront) => self.target.reclaim_request(request, forefront).await,
        };
        if self.has_carried() {
            let mut carried = self.carried.lock().await;
            if let Some(entry) = carried.get(&request.unique_key) {
                let mut translated = request.clone();
                translated.id = Some(entry.target_id.clone());
                let info = finish(&translated).await?;
                carried.remove(&request.unique_key);
                self.carried_count.fetch_sub(1, Ordering::AcqRel);
                return Ok(info);
            }
        }
        finish(request).await
    }

    /// The target's id of the request with the id `request_id`.
    async fn target_id(&self, request_id: &str) -> String {
        if self.has_carried() {
            let carried = self.carried.lock().await;
            if let Some(entry) = carried.values().find(|entry| entry.memory_id.as_deref() == Some(request_id)) {
                return entry.target_id.clone();
            }
        }
        request_id.to_owned()
    }
}

enum Route<'a> {
    Memory(#[allow(dead_code)] tokio::sync::RwLockReadGuard<'a, Option<Arc<dyn RequestQueueBackend>>>),
    Target(&'a Spilled),
}

impl SpillingRequestQueueBackend {
    /// Where a call goes: the target after the switch, memory before it (and the switch waits for
    /// the call). Makes the switch first if it is due.
    async fn route(&self) -> Route<'_> {
        if let Some(spilled) = self.spilled.get() {
            return Route::Target(spilled);
        }
        if self.is_due()
            && let Err(err) = self.spill().await
        {
            tracing::warn!(
                "Failed to move the request queue from memory, it stays there and the move is tried again in {} s: \
                 {err}",
                RETRY_DELAY.as_secs()
            );
            *self.retry_at.lock() = Some(Instant::now() + RETRY_DELAY);
        }
        let guard = self.switch.read().await;
        match self.spilled.get() {
            Some(spilled) => Route::Target(spilled),
            None => Route::Memory(guard),
        }
    }
}

#[async_trait]
impl RequestQueueBackend for SpillingRequestQueueBackend {
    async fn get_metadata(&self) -> StorageResult<RequestQueueInfo> {
        match self.route().await {
            Route::Target(spilled) => spilled.target.get_metadata().await,
            Route::Memory(_guard) => {
                let mut info = self.memory.get_metadata().await?;
                if let Some(id) = &self.id {
                    info.id = id.clone();
                }
                Ok(info)
            }
        }
    }

    /// Before the switch, drops the memory and the target, which the queue then stays on.
    async fn drop_storage(&self) -> StorageResult<()> {
        if let Some(spilled) = self.spilled.get() {
            return spilled.target.drop_storage().await;
        }
        let mut switch = self.switch.write().await;
        if let Some(spilled) = self.spilled.get() {
            drop(switch);
            return spilled.target.drop_storage().await;
        }
        let target = match &*switch {
            Some(target) => target.clone(),
            None => (self.factory)().await?,
        };
        target.drop_storage().await?;
        self.memory.drop_storage().await?;
        *switch = Some(target.clone());
        let spilled = Spilled { target, carried: tokio::sync::Mutex::default(), carried_count: AtomicUsize::new(0) };
        let _ = self.spilled.set(spilled);
        Ok(())
    }

    async fn purge(&self) -> StorageResult<()> {
        match self.route().await {
            Route::Target(spilled) => spilled.target.purge().await,
            Route::Memory(_guard) => self.memory.purge().await,
        }
    }

    async fn add_batch_of_requests(
        &self,
        requests: Vec<Request>,
        forefront: bool,
    ) -> StorageResult<BatchAddRequestsResult> {
        match self.route().await {
            Route::Target(spilled) => spilled.target.add_batch_of_requests(requests, forefront).await,
            Route::Memory(_guard) => self.memory.add_batch_of_requests(requests, forefront).await,
        }
    }

    async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>> {
        match self.route().await {
            Route::Target(spilled) => spilled.target.get_request(unique_key).await,
            Route::Memory(_guard) => self.memory.get_request(unique_key).await,
        }
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        match self.route().await {
            Route::Target(spilled) => spilled.fetch_next_request().await,
            Route::Memory(_guard) => self.memory.fetch_next_request().await,
        }
    }

    async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<Option<QueueOperationInfo>> {
        match self.route().await {
            Route::Target(spilled) => spilled.finish(request, None).await,
            Route::Memory(_guard) => self.memory.mark_request_as_handled(request).await,
        }
    }

    async fn reclaim_request(&self, request: &Request, forefront: bool) -> StorageResult<Option<QueueOperationInfo>> {
        match self.route().await {
            Route::Target(spilled) => spilled.finish(request, Some(forefront)).await,
            Route::Memory(_guard) => self.memory.reclaim_request(request, forefront).await,
        }
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        match self.route().await {
            Route::Target(spilled) => spilled.target.is_empty().await,
            Route::Memory(_guard) => self.memory.is_empty().await,
        }
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        match self.route().await {
            Route::Target(spilled) => spilled.target.is_finished().await,
            Route::Memory(_guard) => self.memory.is_finished().await,
        }
    }

    async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
        {
            let mut expected = self.expected_processing_time.lock();
            *expected = Some(expected.map_or(duration, |previous| previous.max(duration)));
        }
        match self.route().await {
            Route::Target(spilled) => spilled.target.set_expected_request_processing_time(duration).await,
            Route::Memory(_guard) => Ok(()),
        }
    }

    async fn extend_request_processing_time(&self, request_id: &str, duration: Duration) -> StorageResult<bool> {
        match self.route().await {
            Route::Target(spilled) => {
                let request_id = spilled.target_id(request_id).await;
                spilled.target.extend_request_processing_time(&request_id, duration).await
            }
            Route::Memory(_guard) => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::errors::StorageError;
    use crate::storage::backend::{StorageBackend, StorageIdentifier};
    use crate::storage::memory::MemoryStorageBackend;

    /// A batch add: the unique keys, whether each was added as handled, and `forefront`.
    type Batch = (Vec<(String, bool)>, bool);

    /// A target that records the calls it gets, and whose request ids differ from the in-memory
    /// ones (`t-` and the in-memory id), as the ids of a remote queue may. A request it gets with
    /// another id fails the test.
    struct Observed {
        inner: Arc<dyn RequestQueueBackend>,
        calls: Mutex<Vec<&'static str>>,
        batches: Mutex<Vec<Batch>>,
        extended: Mutex<Vec<String>>,
        expected_times: Mutex<Vec<Duration>>,
    }

    impl Observed {
        async fn new() -> Arc<Self> {
            let inner =
                MemoryStorageBackend::new().create_request_queue_backend(&StorageIdentifier::Default).await.unwrap();
            Arc::new(Observed {
                inner,
                calls: Mutex::default(),
                batches: Mutex::default(),
                extended: Mutex::default(),
                expected_times: Mutex::default(),
            })
        }

        fn call(&self, name: &'static str) {
            self.calls.lock().push(name);
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().clone()
        }

        fn to_inner(request: &Request) -> Request {
            let mut request = request.clone();
            let id = request.id.take().expect("a request with an id");
            let inner = id.strip_prefix("t-").unwrap_or_else(|| panic!("the target got a foreign id: {id}"));
            request.id = Some(inner.to_owned());
            request
        }

        fn from_inner(mut request: Request) -> Request {
            request.id = request.id.map(|id| format!("t-{id}"));
            request
        }

        fn info(info: Option<QueueOperationInfo>) -> Option<QueueOperationInfo> {
            info.map(|info| QueueOperationInfo { request_id: format!("t-{}", info.request_id), ..info })
        }
    }

    #[async_trait]
    impl RequestQueueBackend for Observed {
        async fn get_metadata(&self) -> StorageResult<RequestQueueInfo> {
            self.call("get_metadata");
            self.inner.get_metadata().await
        }
        async fn drop_storage(&self) -> StorageResult<()> {
            self.call("drop_storage");
            self.inner.drop_storage().await
        }
        async fn purge(&self) -> StorageResult<()> {
            self.call("purge");
            self.inner.purge().await
        }
        async fn add_batch_of_requests(
            &self,
            requests: Vec<Request>,
            forefront: bool,
        ) -> StorageResult<BatchAddRequestsResult> {
            self.call("add_batch_of_requests");
            assert!(requests.iter().all(|request| request.id.is_none()), "added with an id: {requests:?}");
            let keys = requests.iter().map(|r| (r.unique_key.clone(), r.handled_at.is_some())).collect();
            self.batches.lock().push((keys, forefront));
            let mut result = self.inner.add_batch_of_requests(requests, forefront).await?;
            for processed in &mut result.processed_requests {
                processed.request_id = format!("t-{}", processed.request_id);
            }
            Ok(result)
        }
        async fn get_request(&self, unique_key: &str) -> StorageResult<Option<Request>> {
            self.call("get_request");
            Ok(self.inner.get_request(unique_key).await?.map(Self::from_inner))
        }
        async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
            self.call("fetch_next_request");
            Ok(self.inner.fetch_next_request().await?.map(Self::from_inner))
        }
        async fn mark_request_as_handled(&self, request: &Request) -> StorageResult<Option<QueueOperationInfo>> {
            self.call("mark_request_as_handled");
            Ok(Self::info(self.inner.mark_request_as_handled(&Self::to_inner(request)).await?))
        }
        async fn reclaim_request(
            &self,
            request: &Request,
            forefront: bool,
        ) -> StorageResult<Option<QueueOperationInfo>> {
            self.call("reclaim_request");
            Ok(Self::info(self.inner.reclaim_request(&Self::to_inner(request), forefront).await?))
        }
        async fn is_empty(&self) -> StorageResult<bool> {
            self.call("is_empty");
            self.inner.is_empty().await
        }
        async fn is_finished(&self) -> StorageResult<bool> {
            self.call("is_finished");
            self.inner.is_finished().await
        }
        async fn set_expected_request_processing_time(&self, duration: Duration) -> StorageResult<()> {
            self.call("set_expected_request_processing_time");
            self.expected_times.lock().push(duration);
            Ok(())
        }
        async fn extend_request_processing_time(&self, request_id: &str, _duration: Duration) -> StorageResult<bool> {
            self.call("extend_request_processing_time");
            self.extended.lock().push(request_id.to_owned());
            Ok(true)
        }
    }

    /// A spilling queue over `target`, and the number of times the target was created.
    fn spilling(target: &Arc<Observed>, options: SpillingOptions) -> (SpillingRequestQueueBackend, Arc<AtomicUsize>) {
        let created = Arc::new(AtomicUsize::new(0));
        let (target, counter) = (target.clone(), created.clone());
        let queue = SpillingRequestQueueBackend::new(
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                let target: Arc<dyn RequestQueueBackend> = target.clone();
                async move { Ok(target) }
            },
            options,
        );
        (queue, created)
    }

    fn request(path: &str) -> Request {
        Request::new(format!("https://a.dev/{path}"))
    }

    fn requests(paths: &[&str]) -> Vec<Request> {
        paths.iter().map(|path| request(path)).collect()
    }

    fn path(request: &Request) -> &str {
        request.url.strip_prefix("https://a.dev/").unwrap()
    }

    async fn add(queue: &dyn RequestQueueBackend, paths: &[&str], forefront: bool) -> BatchAddRequestsResult {
        queue.add_batch_of_requests(requests(paths), forefront).await.unwrap()
    }

    async fn fetch(queue: &dyn RequestQueueBackend) -> Option<Request> {
        queue.fetch_next_request().await.unwrap()
    }

    async fn fetch_all(queue: &dyn RequestQueueBackend) -> Vec<Request> {
        let mut fetched = Vec::new();
        while let Some(request) = fetch(queue).await {
            fetched.push(request);
        }
        fetched
    }

    fn paths(requests: &[Request]) -> Vec<&str> {
        requests.iter().map(path).collect()
    }

    async fn counts(queue: &dyn RequestQueueBackend) -> (u64, u64, u64) {
        let info = queue.get_metadata().await.unwrap();
        (info.total_request_count, info.handled_request_count, info.pending_request_count)
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_reaches_the_target_before_the_switch() {
        let target = Observed::new().await;
        let (queue, created) = spilling(&target, SpillingOptions::default());

        add(&queue, &["a", "b", "c"], false).await;
        add(&queue, &["front"], true).await;
        let first = fetch(&queue).await.unwrap();
        assert_eq!(path(&first), "front");
        queue.mark_request_as_handled(&first).await.unwrap().unwrap();
        let second = fetch(&queue).await.unwrap();
        queue.reclaim_request(&second, false).await.unwrap().unwrap();
        queue.set_expected_request_processing_time(Duration::from_secs(60)).await.unwrap();
        assert!(
            !queue.extend_request_processing_time(second.id.as_deref().unwrap(), Duration::from_secs(1)).await.unwrap()
        );
        assert!(queue.get_request(&second.unique_key).await.unwrap().is_some());
        assert!(!queue.is_empty().await.unwrap());
        assert!(!queue.is_finished().await.unwrap());
        assert_eq!(counts(&queue).await, (4, 1, 3));
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(!queue.spill_if_due().await.unwrap());
        add(&queue, &["d"], false).await;

        assert_eq!(created.load(Ordering::SeqCst), 0);
        assert!(target.calls().is_empty(), "{:?}", target.calls());
        assert!(!queue.is_spilled());
        assert_eq!(queue.unhandled_in_memory(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn the_queue_moves_once_its_time_has_passed() {
        let target = Observed::new().await;
        let options = SpillingOptions { spill_after: Some(Duration::from_secs(5)), max_requests: None };
        let (queue, created) = spilling(&target, options);
        add(&queue, &["a", "b"], false).await;

        tokio::time::advance(Duration::from_secs(5)).await;
        // The next call moves the queue, and goes to the target.
        add(&queue, &["c"], false).await;
        assert!(queue.is_spilled());
        assert_eq!(created.load(Ordering::SeqCst), 1);
        assert_eq!(queue.unhandled_in_memory(), 0);
        assert_eq!(target.batches.lock().last().unwrap().0, [("https://a.dev/c".to_owned(), false)]);
        assert_eq!(paths(&fetch_all(&queue).await), ["a", "b", "c"]);
        assert_eq!(counts(&*target).await, (3, 0, 3));

        queue.spill().await.unwrap();
        assert_eq!(created.load(Ordering::SeqCst), 1, "moved once");
    }

    #[tokio::test(start_paused = true)]
    async fn a_queue_nobody_calls_moves_when_asked_whether_it_is_due() {
        let target = Observed::new().await;
        let (queue, created) = spilling(&target, SpillingOptions::default());
        add(&queue, &["a"], false).await;
        tokio::time::advance(DEFAULT_SPILL_AFTER).await;
        assert!(queue.spill_if_due().await.unwrap());
        assert_eq!(created.load(Ordering::SeqCst), 1);
        assert_eq!(counts(&*target).await, (1, 0, 1));
    }

    #[tokio::test]
    async fn the_queue_moves_after_as_many_requests_as_allowed() {
        let target = Observed::new().await;
        let options = SpillingOptions { spill_after: None, max_requests: Some(3) };
        let (queue, _) = spilling(&target, options);
        add(&queue, &["a", "b"], false).await;
        assert!(!queue.spill_if_due().await.unwrap());
        add(&queue, &["c"], false).await;
        assert!(queue.spill_if_due().await.unwrap());
        assert_eq!(counts(&*target).await, (3, 0, 3));
    }

    #[tokio::test]
    async fn pending_requests_keep_their_order_with_the_forefront_ones_first() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        add(&queue, &["a", "b", "c"], false).await;
        add(&queue, &["f1"], true).await;
        add(&queue, &["f2", "f3"], true).await;
        // A request reclaimed to the forefront goes first, one reclaimed regularly last.
        let first = fetch(&queue).await.unwrap();
        let second = fetch(&queue).await.unwrap();
        queue.reclaim_request(&first, true).await.unwrap();
        queue.reclaim_request(&second, false).await.unwrap();
        let in_memory = ["f3", "f1", "a", "b", "c", "f2"];

        queue.spill().await.unwrap();
        let batches = target.batches.lock().clone();
        assert!(batches.iter().any(|(keys, forefront)| *forefront && keys.len() == 2), "{batches:?}");
        assert_eq!(paths(&fetch_all(&queue).await), in_memory);
    }

    #[tokio::test]
    async fn handled_requests_are_committed_as_handled() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        add(&queue, &["a", "b"], false).await;
        let a = fetch(&queue).await.unwrap();
        queue.mark_request_as_handled(&a).await.unwrap();

        queue.spill().await.unwrap();
        let stored = target.get_request(&a.unique_key).await.unwrap().unwrap();
        assert!(stored.handled_at.is_some());
        let handled_batch = target.batches.lock().iter().find(|(keys, _)| keys.iter().any(|(_, h)| *h)).cloned();
        assert_eq!(handled_batch.unwrap().0, [(a.unique_key.clone(), true)]);
        assert_eq!(counts(&*target).await, (2, 1, 1));
        assert_eq!(paths(&fetch_all(&queue).await), ["b"]);
    }

    #[tokio::test]
    async fn requests_in_progress_at_the_switch_finish_on_the_target() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        add(&queue, &["a", "b", "c"], false).await;
        let a = fetch(&queue).await.unwrap();
        let b = fetch(&queue).await.unwrap();

        queue.spill().await.unwrap();
        // They run already: the target does not hand them out again.
        assert_eq!(paths(&fetch_all(&queue).await), ["c"]);
        assert!(!queue.is_finished().await.unwrap(), "a and b are still running");
        assert_eq!(counts(&queue).await, (3, 0, 3));

        // Marked on the target under its ids (`Observed` fails on an in-memory id).
        assert!(queue.extend_request_processing_time(a.id.as_deref().unwrap(), Duration::from_secs(9)).await.unwrap());
        assert_eq!(*target.extended.lock(), [format!("t-{}", a.id.as_deref().unwrap())]);
        let info = queue.mark_request_as_handled(&a).await.unwrap().unwrap();
        assert!(!info.was_already_handled);
        assert!(info.request_id.starts_with("t-"));
        queue.mark_request_as_handled(&b).await.unwrap().unwrap();
        let c = queue.get_request("https://a.dev/c").await.unwrap().unwrap();
        queue.mark_request_as_handled(&c).await.unwrap().unwrap();
        assert!(queue.is_finished().await.unwrap());
        assert_eq!(counts(&queue).await, (3, 3, 0));
    }

    #[tokio::test]
    async fn a_request_in_progress_at_the_switch_finishing_before_the_target_serves_it_is_not_served() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        add(&queue, &["a", "b"], false).await;
        let a = fetch(&queue).await.unwrap();
        queue.spill().await.unwrap();

        queue.mark_request_as_handled(&a).await.unwrap().unwrap();
        assert_eq!(paths(&fetch_all(&queue).await), ["b"]);
    }

    #[tokio::test]
    async fn a_request_in_progress_at_the_switch_is_reclaimed_to_the_target() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        add(&queue, &["a", "b", "c"], false).await;
        let mut a = fetch(&queue).await.unwrap();
        let b = fetch(&queue).await.unwrap();
        queue.spill().await.unwrap();

        // `b` is served (and skipped) by the target before it is reclaimed, `a` after.
        assert_eq!(paths(&fetch_all(&queue).await), ["c"]);
        queue.reclaim_request(&b, false).await.unwrap().unwrap();
        a.retry_count = 1;
        queue.reclaim_request(&a, true).await.unwrap().unwrap();
        let again = fetch_all(&queue).await;
        assert_eq!(paths(&again), ["a", "b"]);
        assert_eq!(again[0].retry_count, 1);
        assert!(again.iter().all(|request| request.id.as_deref().unwrap().starts_with("t-")));
        for request in &again {
            queue.mark_request_as_handled(request).await.unwrap().unwrap();
        }
        let c = queue.get_request("https://a.dev/c").await.unwrap().unwrap();
        queue.mark_request_as_handled(&c).await.unwrap().unwrap();
        assert!(queue.is_finished().await.unwrap());
        assert_eq!(counts(&queue).await, (3, 3, 0));
    }

    #[tokio::test]
    async fn requests_added_before_the_switch_are_still_deduplicated_after_it() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        add(&queue, &["handled", "running", "pending"], false).await;
        let handled = fetch(&queue).await.unwrap();
        queue.mark_request_as_handled(&handled).await.unwrap();
        fetch(&queue).await.unwrap();

        queue.spill().await.unwrap();
        let again = add(&queue, &["handled", "running", "pending", "new"], false).await;
        let flags: Vec<(&str, bool, bool)> = again
            .processed_requests
            .iter()
            .map(|p| (p.unique_key.as_str(), p.was_already_present, p.was_already_handled))
            .collect();
        assert_eq!(
            flags,
            [
                ("https://a.dev/handled", true, true),
                ("https://a.dev/running", true, false),
                ("https://a.dev/pending", true, false),
                ("https://a.dev/new", false, false),
            ]
        );
    }

    #[tokio::test]
    async fn the_counts_stay_the_same_across_the_switch() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        add(&queue, &["a", "b", "c", "d"], false).await;
        let a = fetch(&queue).await.unwrap();
        queue.mark_request_as_handled(&a).await.unwrap();
        fetch(&queue).await.unwrap();
        let before = counts(&queue).await;
        assert_eq!(before, (4, 1, 3));

        queue.spill().await.unwrap();
        assert_eq!(counts(&queue).await, before);
        assert!(!queue.is_empty().await.unwrap());
        assert!(!queue.is_finished().await.unwrap());
    }

    #[tokio::test]
    async fn the_requests_and_the_processing_time_are_committed_once_and_the_hook_called_after() {
        let target = Observed::new().await;
        let hooked = Arc::new(AtomicBool::new(false));
        let committed = Arc::new(Mutex::new(None));
        let (queue, created) = spilling(&target, SpillingOptions::default());
        let queue = {
            let (hooked, committed, target) = (hooked.clone(), committed.clone(), target.clone());
            queue.on_spilled(move || {
                let (hooked, committed, target) = (hooked.clone(), committed.clone(), target.clone());
                async move {
                    assert!(!hooked.swap(true, Ordering::SeqCst), "called once");
                    *committed.lock() = Some(target.inner.get_metadata().await?.total_request_count);
                    Ok(())
                }
            })
        };
        queue.set_expected_request_processing_time(Duration::from_secs(90)).await.unwrap();
        add(&queue, &["a", "b"], false).await;

        let queue = Arc::new(queue);
        let mut tasks = Vec::new();
        for n in 0..10 {
            let queue = queue.clone();
            tasks.push(tokio::spawn(async move {
                if n % 2 == 0 {
                    queue.spill().await.unwrap();
                } else {
                    queue.add_batch_of_requests(vec![request(&format!("x{n}"))], false).await.unwrap();
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(created.load(Ordering::SeqCst), 1);
        assert!(hooked.load(Ordering::SeqCst));
        assert_eq!(*committed.lock(), Some(2), "called once the requests of the switch were committed");
        assert_eq!(counts(&*target).await, (7, 0, 7), "none lost");
        assert_eq!(*target.expected_times.lock(), [Duration::from_secs(90)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_switch_keeps_the_queue_in_memory_and_is_tried_again() {
        let target = Observed::new().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let queue = {
            let (target, attempts) = (target.clone(), attempts.clone());
            SpillingRequestQueueBackend::new(
                move || {
                    let target: Arc<dyn RequestQueueBackend> = target.clone();
                    let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                    async move {
                        if first {
                            return Err(StorageError::Backend("unreachable".into()));
                        }
                        Ok(target)
                    }
                },
                SpillingOptions { spill_after: Some(Duration::from_secs(1)), max_requests: None },
            )
        };
        add(&queue, &["a"], false).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        add(&queue, &["b"], false).await;
        assert!(!queue.is_spilled());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(counts(&queue).await, (2, 0, 2), "kept in memory");

        tokio::time::advance(RETRY_DELAY).await;
        add(&queue, &["c"], false).await;
        assert!(queue.is_spilled());
        assert_eq!(counts(&*target).await, (3, 0, 3));
        assert!(queue.spill().await.is_ok());
    }

    #[tokio::test]
    async fn an_explicit_spill_reports_a_failure() {
        let queue = SpillingRequestQueueBackend::new(
            || async { Err(StorageError::Backend("unreachable".into())) },
            SpillingOptions::default(),
        );
        add(&queue, &["a"], false).await;
        assert!(queue.spill().await.is_err());
        assert!(!queue.is_spilled());
        assert_eq!(paths(&fetch_all(&queue).await), ["a"]);
    }

    #[tokio::test]
    async fn the_queue_reports_its_id_before_the_switch() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        let queue = queue.with_id("default-queue");
        assert_eq!(queue.get_metadata().await.unwrap().id, "default-queue");
    }

    #[tokio::test]
    async fn dropping_the_queue_before_the_switch_drops_the_target() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions::default());
        add(&queue, &["a"], false).await;
        queue.drop_storage().await.unwrap();
        assert_eq!(target.calls(), ["drop_storage"]);
        assert!(queue.is_spilled());
    }

    /// Many concurrent clients adding, fetching and handling while the queue moves: every request
    /// is handled once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_request_is_handled_once_by_concurrent_clients_across_the_switch() {
        let target = Observed::new().await;
        let (queue, _) = spilling(&target, SpillingOptions { spill_after: None, max_requests: None });
        let queue = Arc::new(queue);
        let first: Vec<Request> = (0..50).map(|n| request(&n.to_string())).collect();
        queue.add_batch_of_requests(first, false).await.unwrap();
        let handled = Arc::new(Mutex::new(Vec::new()));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let (queue, handled) = (queue.clone(), handled.clone());
            workers.push(tokio::spawn(async move {
                loop {
                    let Some(fetched) = queue.fetch_next_request().await.unwrap() else {
                        if queue.is_finished().await.unwrap() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        continue;
                    };
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    let n: usize = path(&fetched).parse().unwrap();
                    if n < 100 {
                        queue.add_batch_of_requests(vec![request(&(n + 50).to_string())], false).await.unwrap();
                    }
                    handled.lock().push(n);
                    queue.mark_request_as_handled(&fetched).await.unwrap().unwrap();
                }
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        queue.spill().await.unwrap();
        for worker in workers {
            worker.await.unwrap();
        }
        let handled = handled.lock().clone();
        let unique: HashSet<usize> = handled.iter().copied().collect();
        assert_eq!(handled.len(), unique.len(), "handled twice: {handled:?}");
        assert_eq!(unique, (0..150).collect());
        assert_eq!(counts(&*queue).await, (150, 150, 0));
    }
}
