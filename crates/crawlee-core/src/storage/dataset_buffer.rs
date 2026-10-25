//! Buffered dataset writes, an opt-in extra of crawlee-rs that Crawlee for JS does not have.
//!
//! By default every [`Dataset::push_data`](crate::Dataset::push_data) is one backend call, as in
//! JS. On a backend behind an HTTP API (the Apify platform) every call has a cost of its own (the
//! request, its compression, a pay-per-event charge call), so a dataset can instead collect the
//! items of several pushes and store them together.
//!
//! A buffered dataset pushes the first items right away, so that the first results show at once,
//! then lets the items wait in a buffer, at most for the *linger* of the current stage, which
//! grows as more items are pushed (see [`DatasetBufferOptions`]). The buffer is also flushed when
//! it holds [`max_bytes`](DatasetBufferOptions::max_bytes), on the `PersistState`, `Migrating`,
//! `Aborting` and `Exit` events, when a crawler finishes or runs out of work, before a read of the
//! dataset, and on [`Dataset::flush`](crate::Dataset::flush). Items keep their push order.
//!
//! Data safety: a push made while a crawler handles a request (through the context or, for
//! example, an SDK's `Actor::push_data`, in the request's task) is recorded in the request's
//! [`PendingWrites`]. The crawler marks the request as handled only once the flushes holding those
//! items succeeded; when one fails, the request is retried instead. A crash between a push and its
//! flush therefore never leaves a request marked as handled without its items.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use indexmap::IndexMap;
use parking_lot::Mutex;
use tokio::sync::watch;

use super::backend::{DatasetBackend, DatasetItem};
use super::dataset::DatasetCounters;
use crate::errors::{StorageError, StorageResult};
use crate::events::{EventKind, EventManager, ListenerId};

/// The default [`max_bytes`](DatasetBufferOptions::max_bytes): 1 MiB.
pub const DEFAULT_BUFFER_MAX_BYTES: usize = 1024 * 1024;
/// The largest buffer: 8 MiB, below the 9 MB payload limit of the Apify API.
pub const MAX_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// From the `from_item`-th item pushed on (counting from 0), items wait in the buffer at most
/// `linger`. A zero linger pushes them right away.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferStage {
    pub from_item: u64,
    pub linger: Duration,
}

impl BufferStage {
    pub fn new(from_item: u64, linger: Duration) -> Self {
        BufferStage { from_item, linger }
    }
}

/// Settings of buffered dataset writes (see the [module](self) docs).
///
/// Items before the first stage are pushed right away. The defaults push the first 10 items
/// right away, let the next ones wait at most 1 s, and from the 100th item on at most 2 s, and
/// flush at 1 MiB:
///
/// ```
/// use std::time::Duration;
/// use crawlee_core::storage::dataset_buffer::{BufferStage, DatasetBufferOptions};
///
/// let options = DatasetBufferOptions::new()
///     .with_stages([BufferStage::new(5, Duration::from_millis(500)), BufferStage::new(50, Duration::from_secs(3))])
///     .with_max_bytes(512 * 1024);
/// assert_eq!(options.linger_for(0), Duration::ZERO);
/// assert_eq!(options.linger_for(5), Duration::from_millis(500));
/// assert_eq!(options.linger_for(1000), Duration::from_secs(3));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetBufferOptions {
    /// The stages, by `from_item`.
    pub stages: Vec<BufferStage>,
    /// Flush once the buffered items take this many bytes (as JSON). Capped at
    /// [`MAX_BUFFER_BYTES`].
    pub max_bytes: usize,
}

impl Default for DatasetBufferOptions {
    fn default() -> Self {
        DatasetBufferOptions {
            stages: vec![BufferStage::new(10, Duration::from_secs(1)), BufferStage::new(100, Duration::from_secs(2))],
            max_bytes: DEFAULT_BUFFER_MAX_BYTES,
        }
    }
}

impl DatasetBufferOptions {
    /// The defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the stages.
    pub fn with_stages(mut self, stages: impl IntoIterator<Item = BufferStage>) -> Self {
        self.stages = stages.into_iter().collect();
        self.stages.sort_by_key(|stage| stage.from_item);
        self
    }

    /// Adds a stage, replacing one that starts at the same item.
    pub fn with_stage(mut self, from_item: u64, linger: Duration) -> Self {
        self.stages.retain(|stage| stage.from_item != from_item);
        self.stages.push(BufferStage::new(from_item, linger));
        self.stages.sort_by_key(|stage| stage.from_item);
        self
    }

    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// How long an item may wait when `pushed` items were pushed before it.
    pub fn linger_for(&self, pushed: u64) -> Duration {
        self.stages.iter().take_while(|stage| stage.from_item <= pushed).last().map_or(Duration::ZERO, |s| s.linger)
    }

    /// The longest linger: how much later than unbuffered a request may be marked as handled.
    pub fn max_linger(&self) -> Duration {
        self.stages.iter().map(|stage| stage.linger).max().unwrap_or_default()
    }

    /// [`max_bytes`](Self::max_bytes), within 1 B and [`MAX_BUFFER_BYTES`].
    pub fn effective_max_bytes(&self) -> usize {
        self.max_bytes.clamp(1, MAX_BUFFER_BYTES)
    }

    /// Parses stages written as `FROM_ITEM:LINGER` separated by commas, such as `10:1s,100:2s`.
    /// A linger is in seconds (`2`, `1.5`, `2s`) or milliseconds (`500ms`).
    pub fn parse_stages(text: &str) -> Result<Vec<BufferStage>, String> {
        let mut stages = Vec::new();
        for part in text.split(',').map(str::trim).filter(|part| !part.is_empty()) {
            let invalid = || format!("invalid dataset buffer stage '{part}': expected FROM_ITEM:LINGER, such as 10:1s");
            let (from_item, linger) = part.split_once(':').ok_or_else(invalid)?;
            let from_item: u64 = from_item.trim().parse().map_err(|_| invalid())?;
            let linger = linger.trim();
            let linger = match linger.strip_suffix("ms") {
                Some(millis) => Duration::from_millis(millis.trim().parse().map_err(|_| invalid())?),
                None => {
                    let secs: f64 = linger.strip_suffix('s').unwrap_or(linger).trim().parse().map_err(|_| invalid())?;
                    Duration::try_from_secs_f64(secs).map_err(|_| invalid())?
                }
            };
            stages.push(BufferStage::new(from_item, linger));
        }
        stages.sort_by_key(|stage| stage.from_item);
        Ok(stages)
    }
}

/// Named counts a backend attaches to the items it admits into a buffer (see
/// [`DatasetBackend::admit_buffered`]), summed over the pushes of a flush. The Apify SDK counts
/// the pay-per-event charges of the items in them, to charge once per flush.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FlushCounters(IndexMap<String, u64>);

impl FlushCounters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, name: &str, count: u64) {
        if count > 0 {
            *self.0.entry(name.to_owned()).or_default() += count;
        }
    }

    pub fn merge(&mut self, other: FlushCounters) {
        for (name, count) in other.0 {
            self.add(&name, count);
        }
    }

    pub fn get(&self, name: &str) -> u64 {
        self.0.get(name).copied().unwrap_or(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, u64)> {
        self.0.iter().map(|(name, count)| (name.as_str(), *count))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A flush failed; every push it held fails with this.
#[derive(Clone, Debug)]
pub struct FlushError(pub Arc<StorageError>);

impl std::fmt::Display for FlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storing buffered dataset items failed: {}", self.0)
    }
}

impl std::error::Error for FlushError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

type FlushOutcome = Option<Result<(), Arc<StorageError>>>;

/// The items buffered between two flushes, and the outcome of the flush that stores them.
#[derive(Debug)]
pub struct Batch {
    outcome: watch::Sender<FlushOutcome>,
}

impl Batch {
    fn new() -> Arc<Self> {
        Arc::new(Batch { outcome: watch::Sender::new(None) })
    }

    fn settle(&self, outcome: Result<(), Arc<StorageError>>) {
        self.outcome.send_replace(Some(outcome));
    }

    /// The outcome, once the batch was flushed.
    fn settled(&self) -> Option<StorageResult<()>> {
        self.outcome.borrow().clone().map(to_storage_result)
    }

    async fn wait(&self) -> StorageResult<()> {
        let mut outcome = self.outcome.subscribe();
        let settled = outcome.wait_for(Option::is_some).await.expect("the sender lives in the batch").clone();
        to_storage_result(settled.expect("waited for"))
    }
}

fn to_storage_result(outcome: Result<(), Arc<StorageError>>) -> StorageResult<()> {
    outcome.map_err(|err| StorageError::Backend(Box::new(FlushError(err))))
}

tokio::task_local! {
    static PENDING_WRITES: Arc<PendingWrites>;
}

/// The buffered writes made in a scope, such as a crawler's request: pushes to a buffered dataset
/// made in [`scope`](Self::scope) (in its task) are recorded here, so that the scope's owner can
/// wait until they are stored.
#[derive(Debug, Default)]
pub struct PendingWrites {
    batches: Mutex<Vec<Arc<Batch>>>,
}

impl PendingWrites {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Runs `future` with its buffered pushes recorded here.
    pub fn scope<F: Future>(self: &Arc<Self>, future: F) -> impl Future<Output = F::Output> {
        PENDING_WRITES.scope(self.clone(), future)
    }

    fn current() -> Option<Arc<PendingWrites>> {
        PENDING_WRITES.try_with(Arc::clone).ok()
    }

    fn record(&self, batch: &Arc<Batch>) {
        let mut batches = self.batches.lock();
        if !batches.iter().any(|known| Arc::ptr_eq(known, batch)) {
            batches.push(batch.clone());
        }
    }

    /// Whether no buffered push was made in the scope.
    pub fn is_empty(&self) -> bool {
        self.batches.lock().is_empty()
    }

    /// `Some` once every push of the scope was flushed: `Ok` when all of them were stored.
    pub fn settled(&self) -> Option<StorageResult<()>> {
        let batches = self.batches.lock().clone();
        let mut result = Ok(());
        for batch in batches {
            match batch.settled()? {
                Ok(()) => {}
                Err(err) => result = result.and(Err(err)),
            }
        }
        Some(result)
    }

    /// Waits until every push of the scope was flushed. `Ok` when all of them were stored.
    pub async fn wait(&self) -> StorageResult<()> {
        let batches = self.batches.lock().clone();
        let mut result = Ok(());
        for batch in batches {
            if let Err(err) = batch.wait().await {
                result = result.and(Err(err));
            }
        }
        result
    }
}

#[derive(Default)]
struct BufferState {
    items: Vec<DatasetItem>,
    counters: FlushCounters,
    bytes: usize,
    /// Items admitted so far, which select the stage.
    pushed: u64,
    batch: Option<Arc<Batch>>,
    /// When the oldest buffered item has waited long enough.
    deadline: Option<tokio::time::Instant>,
}

/// The buffer of a dataset handle and its clones.
pub(crate) struct DatasetBuffer {
    backend: Arc<dyn DatasetBackend>,
    options: DatasetBufferOptions,
    counters: Arc<DatasetCounters>,
    state: Mutex<BufferState>,
    /// Held while a flush stores its items, so that flushes store them in push order.
    flushing: tokio::sync::Mutex<()>,
    listeners: Mutex<Option<(EventManager, Vec<ListenerId>)>>,
}

/// The events that flush the buffer.
const FLUSHING_EVENTS: [EventKind; 4] =
    [EventKind::PersistState, EventKind::Migrating, EventKind::Aborting, EventKind::Exit];

impl DatasetBuffer {
    pub(crate) fn new(
        backend: Arc<dyn DatasetBackend>,
        options: DatasetBufferOptions,
        counters: Arc<DatasetCounters>,
        events: Option<&EventManager>,
    ) -> Arc<Self> {
        let buffer = Arc::new(DatasetBuffer {
            backend,
            options,
            counters,
            state: Mutex::default(),
            flushing: tokio::sync::Mutex::new(()),
            listeners: Mutex::new(None),
        });
        if let Some(events) = events {
            let ids = FLUSHING_EVENTS
                .iter()
                .map(|kind| {
                    let buffer = Arc::downgrade(&buffer);
                    events.on(*kind, move |_| {
                        let buffer = buffer.clone();
                        async move {
                            if let Some(buffer) = buffer.upgrade() {
                                // A failure is logged by the flush and reported to its pushes.
                                let _ = buffer.flush().await;
                            }
                        }
                    })
                })
                .collect();
            *buffer.listeners.lock() = Some((events.clone(), ids));
        }
        buffer
    }

    pub(crate) fn options(&self) -> &DatasetBufferOptions {
        &self.options
    }

    /// Takes `items` into the buffer. Returns once they are buffered, or, when the stage pushes
    /// them right away or the buffer is full, once they are stored.
    pub(crate) async fn push(self: &Arc<Self>, mut items: Vec<DatasetItem>) -> StorageResult<()> {
        let counters = self.backend.admit_buffered(&mut items);
        if items.is_empty() {
            return Ok(());
        }
        let bytes: usize = items.iter().map(|item| item.get().len() + 1).sum();
        let (batch, flush_now) = {
            let mut state = self.state.lock();
            let linger = self.options.linger_for(state.pushed);
            state.pushed += items.len() as u64;
            state.items.extend(items);
            state.counters.merge(counters);
            state.bytes += bytes;
            let batch = state.batch.get_or_insert_with(Batch::new).clone();
            let flush_now = linger.is_zero() || state.bytes >= self.options.effective_max_bytes();
            if !flush_now {
                let deadline = tokio::time::Instant::now() + linger;
                if state.deadline.is_none_or(|current| deadline < current) {
                    state.deadline = Some(deadline);
                    let buffer = Arc::downgrade(self);
                    tokio::spawn(async move {
                        tokio::time::sleep_until(deadline).await;
                        let Some(buffer) = buffer.upgrade() else { return };
                        let due = buffer.state.lock().deadline.is_some_and(|due| due <= tokio::time::Instant::now());
                        if due {
                            let _ = buffer.flush().await;
                        }
                    });
                }
            }
            (batch, flush_now)
        };
        if let Some(writes) = PendingWrites::current() {
            writes.record(&batch);
        }
        if flush_now {
            // The items may be flushed by another caller meanwhile: their batch tells the outcome.
            let _ = self.flush().await;
            return batch.wait().await;
        }
        Ok(())
    }

    /// Stores everything buffered so far.
    pub(crate) async fn flush(&self) -> StorageResult<()> {
        let _flushing = self.flushing.lock().await;
        let (items, counters, batch) = {
            let mut state = self.state.lock();
            let Some(batch) = state.batch.take() else { return Ok(()) };
            state.bytes = 0;
            state.deadline = None;
            (std::mem::take(&mut state.items), std::mem::take(&mut state.counters), batch)
        };
        let count = items.len();
        self.counters.write.fetch_add(1, Ordering::Relaxed);
        let outcome = self.backend.push_buffered(items, counters).await.map_err(Arc::new);
        if let Err(err) = &outcome {
            tracing::warn!("Storing {count} buffered dataset items failed: {err}");
        }
        batch.settle(outcome.clone());
        to_storage_result(outcome)
    }
}

impl Drop for DatasetBuffer {
    fn drop(&mut self) {
        if let Some((events, ids)) = self.listeners.lock().take() {
            for id in ids {
                events.off(id);
            }
        }
        let left = self.state.get_mut().items.len();
        if left > 0 {
            tracing::warn!("{left} buffered dataset items were never stored: flush the dataset before dropping it");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Dataset;
    use crate::events::Event;
    use crate::storage::backend::{DatasetInfo, DatasetListOptions, PaginatedList};
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    /// Stores items in memory and logs every backend push; fails the pushes it is told to.
    #[derive(Default)]
    struct Backend {
        items: Mutex<Vec<String>>,
        pushes: Mutex<Vec<(usize, FlushCounters)>>,
        fail_next: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DatasetBackend for Backend {
        async fn get_metadata(&self) -> StorageResult<DatasetInfo> {
            let now = chrono::Utc::now();
            let item_count = self.items.lock().len() as u64;
            Ok(DatasetInfo {
                id: "d".into(),
                name: None,
                created_at: now,
                modified_at: now,
                accessed_at: now,
                item_count,
            })
        }
        async fn drop_storage(&self) -> StorageResult<()> {
            Ok(())
        }
        async fn purge(&self) -> StorageResult<()> {
            self.items.lock().clear();
            Ok(())
        }
        async fn push_data(&self, items: Vec<DatasetItem>) -> StorageResult<()> {
            self.push_buffered(items, FlushCounters::new()).await
        }
        fn admit_buffered(&self, items: &mut Vec<DatasetItem>) -> FlushCounters {
            let mut counters = FlushCounters::new();
            counters.add("item", items.len() as u64);
            counters
        }
        async fn push_buffered(&self, items: Vec<DatasetItem>, counters: FlushCounters) -> StorageResult<()> {
            tokio::task::yield_now().await;
            if self.fail_next.load(Ordering::SeqCst) > 0 {
                self.fail_next.fetch_sub(1, Ordering::SeqCst);
                return Err(StorageError::Backend("the API is down".into()));
            }
            self.pushes.lock().push((items.len(), counters));
            self.items.lock().extend(items.iter().map(|item| item.get().to_owned()));
            Ok(())
        }
        async fn get_data(&self, options: DatasetListOptions) -> StorageResult<PaginatedList<DatasetItem>> {
            let items = self.items.lock().clone();
            Ok(PaginatedList {
                total: items.len(),
                count: items.len(),
                offset: 0,
                limit: options.limit,
                desc: false,
                items: items.into_iter().map(|item| serde_json::value::RawValue::from_string(item).unwrap()).collect(),
            })
        }
    }

    impl Backend {
        fn stored(&self) -> usize {
            self.items.lock().len()
        }
        fn push_sizes(&self) -> Vec<usize> {
            self.pushes.lock().iter().map(|(count, _)| *count).collect()
        }
    }

    fn buffered(options: DatasetBufferOptions, events: Option<&EventManager>) -> (Dataset, Arc<Backend>) {
        let backend = Arc::new(Backend::default());
        (Dataset::from_backend(backend.clone()).with_buffer(options, events), backend)
    }

    fn stages(stages: &[(u64, u64)]) -> DatasetBufferOptions {
        DatasetBufferOptions::new()
            .with_stages(stages.iter().map(|(from, millis)| BufferStage::new(*from, Duration::from_millis(*millis))))
    }

    async fn push(dataset: &Dataset, n: u64) {
        dataset.push_data(&json!({ "n": n })).await.unwrap();
    }

    #[test]
    fn stages_parse_and_select_the_linger() {
        let parsed = DatasetBufferOptions::parse_stages("100:2s, 10:1 ,200:500ms,300:1.5").unwrap();
        assert_eq!(
            parsed,
            [
                BufferStage::new(10, Duration::from_secs(1)),
                BufferStage::new(100, Duration::from_secs(2)),
                BufferStage::new(200, Duration::from_millis(500)),
                BufferStage::new(300, Duration::from_millis(1500)),
            ]
        );
        assert!(DatasetBufferOptions::parse_stages("10").is_err());
        assert!(DatasetBufferOptions::parse_stages("x:1s").is_err());
        assert!(DatasetBufferOptions::parse_stages("1:-1").is_err());

        let options = DatasetBufferOptions::default();
        assert_eq!(options.linger_for(0), Duration::ZERO);
        assert_eq!(options.linger_for(9), Duration::ZERO);
        assert_eq!(options.linger_for(10), Duration::from_secs(1));
        assert_eq!(options.linger_for(99), Duration::from_secs(1));
        assert_eq!(options.linger_for(100), Duration::from_secs(2));
        assert_eq!(options.max_linger(), Duration::from_secs(2));
        assert_eq!(options.effective_max_bytes(), 1024 * 1024);
        assert_eq!(options.clone().with_max_bytes(usize::MAX).effective_max_bytes(), MAX_BUFFER_BYTES);
        let more = options.with_stage(1000, Duration::from_secs(5)).with_stage(10, Duration::from_millis(500));
        assert_eq!(more.linger_for(10), Duration::from_millis(500));
        assert_eq!(more.linger_for(5000), Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_items_are_pushed_right_away_then_they_linger_by_stage() {
        let (dataset, backend) = buffered(stages(&[(2, 1000), (5, 2000)]), None);
        push(&dataset, 0).await;
        push(&dataset, 1).await;
        assert_eq!(backend.push_sizes(), [1, 1], "the first two are stored before the push returns");

        // Items 2-4 wait at most 1 s from the first of them.
        push(&dataset, 2).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        push(&dataset, 3).await;
        push(&dataset, 4).await;
        assert_eq!(backend.stored(), 2);
        tokio::time::sleep(Duration::from_millis(599)).await;
        assert_eq!(backend.stored(), 2);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(backend.push_sizes(), [1, 1, 3]);

        // From the 5th item on, 2 s.
        push(&dataset, 5).await;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        push(&dataset, 6).await;
        assert_eq!(backend.stored(), 5);
        tokio::time::sleep(Duration::from_millis(501)).await;
        assert_eq!(backend.push_sizes(), [1, 1, 3, 2]);
        assert_eq!(dataset.stats().write_count, 4, "one write per flush");
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_buffer_is_flushed() {
        // `{"n":1}` is 7 bytes, 8 with its separator.
        let (dataset, backend) = buffered(stages(&[(0, 60_000)]).with_max_bytes(20), None);
        push(&dataset, 1).await;
        push(&dataset, 2).await;
        assert_eq!(backend.stored(), 0);
        push(&dataset, 3).await;
        assert_eq!(backend.push_sizes(), [3], "stored before the push that filled the buffer returns");
        dataset.push_data(&json!([{ "n": 4 }, { "n": 5 }, { "n": 6 }])).await.unwrap();
        assert_eq!(backend.push_sizes(), [3, 3]);
    }

    #[tokio::test]
    async fn each_flushing_event_flushes() {
        let events = EventManager::new(Duration::from_secs(3600));
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), Some(&events));
        let flushing = [
            Event::PersistState { is_migrating: false },
            Event::Migrating,
            Event::Aborting,
            Event::Exit(crate::ExitOptions::default()),
        ];
        for (n, event) in flushing.into_iter().enumerate() {
            push(&dataset, n as u64).await;
            assert_eq!(backend.stored(), n);
            events.emit(event);
            events.wait_for_all_listeners_to_complete().await;
            assert_eq!(backend.stored(), n + 1);
        }
        // Other events do not flush.
        push(&dataset, 9).await;
        events.emit(Event::Custom { name: "x".into(), data: json!(null) });
        events.wait_for_all_listeners_to_complete().await;
        assert_eq!(backend.stored(), 4);
        dataset.flush().await.unwrap();
        assert_eq!(backend.stored(), 5);

        // A dropped buffer stops listening.
        drop(dataset);
        assert_eq!(events.listener_count(EventKind::PersistState), 0);
    }

    #[tokio::test]
    async fn reads_see_the_buffered_items() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        push(&dataset, 1).await;
        assert_eq!(dataset.get_info().await.unwrap().item_count, 1);
        push(&dataset, 2).await;
        assert_eq!(dataset.get_all::<serde_json::Value>().await.unwrap().len(), 2);
        assert_eq!(backend.push_sizes(), [1, 1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn items_keep_their_push_order() {
        let (dataset, backend) = buffered(stages(&[(3, 5)]).with_max_bytes(50), None);
        let pushers = (0..8).map(|pusher| {
            let dataset = dataset.clone();
            tokio::spawn(async move {
                for i in 0..50 {
                    dataset.push_data(&json!({ "p": pusher, "i": i })).await.unwrap();
                }
            })
        });
        for pusher in pushers.collect::<Vec<_>>() {
            pusher.await.unwrap();
        }
        dataset.flush().await.unwrap();
        // Each pusher's items are stored in the order it pushed them.
        let stored = backend.items.lock().clone();
        assert_eq!(stored.len(), 400);
        for pusher in 0..8 {
            let own: Vec<&String> = stored.iter().filter(|item| item.contains(&format!("\"p\":{pusher},"))).collect();
            let expected: Vec<String> = (0..50).map(|i| json!({ "p": pusher, "i": i }).to_string()).collect();
            assert_eq!(own, expected.iter().collect::<Vec<_>>());
        }
        assert!(backend.push_sizes().len() < 400, "pushes were batched");
    }

    #[tokio::test(start_paused = true)]
    async fn pending_writes_settle_with_their_flush() {
        let (dataset, backend) = buffered(stages(&[(1, 1000)]), None);
        // Outside any scope, items are flushed as usual.
        push(&dataset, 0).await;

        let writes = PendingWrites::new();
        writes.scope(push(&dataset, 1)).await;
        assert!(!writes.is_empty());
        assert!(writes.settled().is_none(), "not flushed yet");
        let other = PendingWrites::new();
        assert!(other.is_empty());
        assert_eq!(other.settled().map(|r| r.is_ok()), Some(true));

        let waiting = tokio::spawn({
            let writes = writes.clone();
            async move { writes.wait().await }
        });
        tokio::time::sleep(Duration::from_millis(1001)).await;
        waiting.await.unwrap().unwrap();
        assert_eq!(writes.settled().map(|r| r.is_ok()), Some(true));
        assert_eq!(backend.stored(), 2);

        // A failed flush fails the writes it held, and the next flush goes on.
        backend.fail_next.store(1, Ordering::SeqCst);
        let failed = PendingWrites::new();
        failed.scope(push(&dataset, 2)).await;
        assert!(dataset.flush().await.is_err());
        let err = failed.wait().await.unwrap_err();
        assert!(err.to_string().contains("the API is down"), "{err}");
        assert!(failed.settled().unwrap().is_err());
        push(&dataset, 3).await;
        dataset.flush().await.unwrap();
        assert_eq!(backend.stored(), 3);
    }

    #[tokio::test]
    async fn a_push_stored_right_away_reports_its_failure() {
        let (dataset, backend) = buffered(stages(&[(5, 1000)]), None);
        backend.fail_next.store(1, Ordering::SeqCst);
        assert!(dataset.push_data(&json!({ "n": 1 })).await.is_err());
        push(&dataset, 2).await;
        assert_eq!(backend.stored(), 1);
    }

    #[tokio::test]
    async fn counters_are_summed_per_flush() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        push(&dataset, 1).await;
        dataset.push_data(&json!([{ "n": 2 }, { "n": 3 }])).await.unwrap();
        dataset.flush().await.unwrap();
        dataset.flush().await.unwrap();
        let pushes = backend.pushes.lock().clone();
        assert_eq!(pushes.len(), 1, "an empty buffer is not flushed");
        assert_eq!(pushes[0].1.get("item"), 3);
    }
}
