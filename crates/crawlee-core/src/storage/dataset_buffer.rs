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
//!
//! Failures: items the backend rejects ([`DatasetBackend::validate_items`]) fail their push
//! right away, as without buffering. When a flush fails, a push someone waits for (a request, or
//! a push stored right away) fails, for its owner to handle; a push nobody waits for is put back
//! at the head of the buffer and retried with a backoff, and [`Dataset::flush`](crate::Dataset::flush)
//! reports it. A flush the backend rejects as a whole is split push by push, so that one bad push
//! fails alone. A flush runs to its end even when whoever started it stops waiting.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
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

/// Named counts attached to buffered items, summed over the pushes of a flush. The Apify SDK
/// counts the pay-per-event charges of the items in them, to charge once per flush.
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

    pub fn merge(&mut self, other: &FlushCounters) {
        for (name, count) in other.iter() {
            self.add(name, count);
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

/// Settles the [`FlushCounters`] attached to buffered items: the Apify SDK reserves the
/// pay-per-event charges of items when they are pushed, and sends them (`stored`) or gives them
/// back (`dropped`) once it is known what became of the items.
#[async_trait]
pub trait FlushSink: Send + Sync {
    /// The items were stored. Called once per flush, with the counters of all its items summed.
    async fn stored(&self, counters: &FlushCounters);
    /// The items were not stored, and will not be.
    fn dropped(&self, counters: &FlushCounters);
}

/// Counters, and the sink that settles them.
pub type Accounting = (Arc<dyn FlushSink>, FlushCounters);

tokio::task_local! {
    static ACCOUNTING: Arc<Mutex<Option<Accounting>>>;
}

/// Runs `push` (a push to a dataset) with `counters` attached to its items: a buffered dataset
/// takes them over and settles them through `sink` once it is known what became of the items.
/// Returns the counters back when no buffered dataset took them, for the caller to settle.
pub async fn with_accounting<F: Future>(
    sink: Arc<dyn FlushSink>,
    counters: FlushCounters,
    push: F,
) -> (F::Output, Option<FlushCounters>) {
    let accounting = Arc::new(Mutex::new(Some((sink, counters))));
    let output = ACCOUNTING.scope(accounting.clone(), push).await;
    let left = accounting.lock().take().map(|(_, counters)| counters);
    (output, left)
}

fn take_accounting() -> Option<Accounting> {
    ACCOUNTING.try_with(|accounting| accounting.lock().take()).ok().flatten()
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

/// The outcome of one buffered push.
#[derive(Debug)]
struct Ack {
    outcome: watch::Sender<FlushOutcome>,
    /// A flush is storing the push right now.
    in_flight: AtomicBool,
}

impl Ack {
    fn new() -> Arc<Self> {
        Arc::new(Ack { outcome: watch::Sender::new(None), in_flight: AtomicBool::new(false) })
    }

    fn settle(&self, outcome: Result<(), Arc<StorageError>>) {
        self.outcome.send_replace(Some(outcome));
    }

    fn is_settled(&self) -> bool {
        self.outcome.borrow().is_some()
    }

    fn settled(&self) -> Option<StorageResult<()>> {
        self.outcome.borrow().clone().map(to_storage_result)
    }

    async fn wait(&self) -> StorageResult<()> {
        let mut outcome = self.outcome.subscribe();
        let settled = outcome.wait_for(Option::is_some).await.expect("the sender lives in the ack").clone();
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
///
/// While a push is recorded in a live `PendingWrites` (or its pusher waits for it), its owner
/// learns of a failure to store it, and the buffer gives the push up. A push nobody waits for
/// (made outside any scope, or whose scope is gone) is kept and retried instead.
#[derive(Default)]
pub struct PendingWrites {
    writes: Mutex<Vec<(Weak<DatasetBuffer>, Arc<Ack>)>>,
    immediate: bool,
}

impl std::fmt::Debug for PendingWrites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingWrites").field("immediate", &self.immediate).finish_non_exhaustive()
    }
}

impl PendingWrites {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// A scope whose pushes are stored before they return, and fail when they cannot be, as
    /// without buffering. Crawlers run their error handlers in one.
    pub fn immediate() -> Arc<Self> {
        Arc::new(PendingWrites { immediate: true, ..PendingWrites::default() })
    }

    /// Runs `future` with its buffered pushes recorded here.
    pub fn scope<F: Future>(self: &Arc<Self>, future: F) -> impl Future<Output = F::Output> {
        PENDING_WRITES.scope(self.clone(), future)
    }

    fn current() -> Option<Arc<PendingWrites>> {
        PENDING_WRITES.try_with(Arc::clone).ok()
    }

    fn record(&self, buffer: &Arc<DatasetBuffer>, ack: &Arc<Ack>) {
        self.writes.lock().push((Arc::downgrade(buffer), ack.clone()));
    }

    fn acks(&self) -> Vec<Arc<Ack>> {
        self.writes.lock().iter().map(|(_, ack)| ack.clone()).collect()
    }

    /// Whether no buffered push was made in the scope.
    pub fn is_empty(&self) -> bool {
        self.writes.lock().is_empty()
    }

    /// `Some` once every push of the scope was flushed: `Ok` when all of them were stored.
    pub fn settled(&self) -> Option<StorageResult<()>> {
        let mut result = Ok(());
        for ack in self.acks() {
            if let Err(err) = ack.settled()? {
                result = result.and(Err(err));
            }
        }
        Some(result)
    }

    /// Waits until every push of the scope was flushed. `Ok` when all of them were stored.
    pub async fn wait(&self) -> StorageResult<()> {
        let mut result = Ok(());
        for ack in self.acks() {
            if let Err(err) = ack.wait().await {
                result = result.and(Err(err));
            }
        }
        result
    }

    /// Takes the pushes of the scope that are still waiting in a buffer out of it: they will not
    /// be stored, and fail. Returns how many. Pushes a flush is storing right now are left alone:
    /// [`wait`](Self::wait) tells what became of them.
    pub fn withdraw(&self) -> usize {
        let writes = self.writes.lock().clone();
        writes.iter().filter(|(buffer, ack)| buffer.upgrade().is_some_and(|buffer| buffer.withdraw(ack))).count()
    }
}

/// One buffered push.
struct Entry {
    items: Vec<DatasetItem>,
    bytes: usize,
    accounts: Vec<Accounting>,
    ack: Arc<Ack>,
}

impl Entry {
    /// Whether someone learns what becomes of the push: a scope it is recorded in, or its pusher.
    fn is_awaited(&self) -> bool {
        Arc::strong_count(&self.ack) > 1
    }

    /// Gives the push up: it fails, and its counters are given back.
    fn fail(mut self, err: Arc<StorageError>) {
        self.ack.settle(Err(err));
        for (sink, counters) in std::mem::take(&mut self.accounts) {
            sink.dropped(&counters);
        }
    }
}

impl Drop for Entry {
    /// A push dropped unsettled (by a buffer dropped with items, or a panicking backend) fails,
    /// so that nothing waits for it forever.
    fn drop(&mut self) {
        if !self.ack.is_settled() {
            self.ack.settle(Err(Arc::new(StorageError::Backend("the buffered items were dropped unstored".into()))));
        }
        for (sink, counters) in std::mem::take(&mut self.accounts) {
            sink.dropped(&counters);
        }
    }
}

#[derive(Default)]
struct BufferState {
    entries: Vec<Entry>,
    bytes: usize,
    /// Items admitted so far, which select the stage.
    pushed: u64,
    /// When the oldest buffered item has waited long enough, or the next retry is due.
    deadline: Option<tokio::time::Instant>,
    /// Failed flushes in a row, for the backoff of the retries.
    failures: u32,
}

/// The backoff of retrying items nobody waits for: 1 s, doubled after each failure, at most 30 s.
fn retry_backoff(failures: u32) -> Duration {
    Duration::from_secs(1 << failures.saturating_sub(1).min(5)).min(Duration::from_secs(30))
}

/// The buffer of a dataset handle and its clones.
pub struct DatasetBuffer {
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
                                // Logged by the flush; items nobody waits for are retried.
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
    /// them right away, the buffer is full or the scope is [immediate](PendingWrites::immediate),
    /// once they are stored. Items the backend rejects fail here, and nothing is buffered.
    pub(crate) async fn push(self: &Arc<Self>, mut items: Vec<DatasetItem>) -> StorageResult<()> {
        self.backend.validate_items(&items)?;
        let mut accounts: Vec<Accounting> = self.backend.admit_buffered(&mut items).into_iter().collect();
        accounts.extend(take_accounting());
        let bytes: usize = items.iter().map(|item| item.get().len() + 1).sum();
        let entry = Entry { items, bytes, accounts, ack: Ack::new() };
        if entry.items.is_empty() {
            entry.ack.settle(Ok(()));
            return Ok(());
        }
        let scope = PendingWrites::current();
        let waiting = {
            let mut state = self.state.lock();
            let linger = self.options.linger_for(state.pushed);
            let flush_now = linger.is_zero()
                || state.bytes + bytes >= self.options.effective_max_bytes()
                || scope.as_ref().is_some_and(|scope| scope.immediate);
            // Recorded before the push can be flushed, so that a flush knows who waits for it.
            if let Some(scope) = &scope {
                scope.record(self, &entry.ack);
            }
            let waiting = flush_now.then(|| entry.ack.clone());
            state.pushed += entry.items.len() as u64;
            state.bytes += bytes;
            state.entries.push(entry);
            if !flush_now {
                self.schedule(&mut state, tokio::time::Instant::now() + linger);
            }
            waiting
        };
        if let Some(ack) = waiting {
            // The push may be stored by another flush meanwhile: its ack tells the outcome.
            let _ = self.flush().await;
            return ack.wait().await;
        }
        Ok(())
    }

    /// Flushes at `deadline`, unless something else flushes first.
    fn schedule(self: &Arc<Self>, state: &mut BufferState, deadline: tokio::time::Instant) {
        if state.deadline.is_some_and(|current| current <= deadline) {
            return;
        }
        state.deadline = Some(deadline);
        let buffer = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            let Some(buffer) = buffer.upgrade() else { return };
            let due = buffer.state.lock().deadline.is_some_and(|due| due <= tokio::time::Instant::now());
            if due {
                // Logged by the flush; items nobody waits for are retried.
                let _ = buffer.flush().await;
            }
        });
    }

    /// Stores everything buffered so far. The flush runs to its end even when the caller stops
    /// waiting for it. `Err` when items nobody waits for could not be stored: they stay buffered
    /// and are retried, with a backoff (or were given up, when the backend rejects them).
    pub(crate) async fn flush(self: &Arc<Self>) -> StorageResult<()> {
        let buffer = self.clone();
        match tokio::spawn(async move { buffer.store_buffered().await }).await {
            Ok(result) => result,
            Err(err) => Err(StorageError::Backend(Box::new(err))),
        }
    }

    async fn store_buffered(self: &Arc<Self>) -> StorageResult<()> {
        let _flushing = self.flushing.lock().await;
        let entries = {
            let mut state = self.state.lock();
            state.bytes = 0;
            state.deadline = None;
            std::mem::take(&mut state.entries)
        };
        if entries.is_empty() {
            return Ok(());
        }
        for entry in &entries {
            entry.ack.in_flight.store(true, Ordering::Release);
        }
        let items: Vec<DatasetItem> = entries.iter().flat_map(|entry| entry.items.iter().cloned()).collect();
        let count = items.len();
        self.counters.write.fetch_add(1, Ordering::Relaxed);
        let failed = match self.backend.push_buffered(items).await {
            Ok(()) => {
                self.settle_stored(entries).await;
                Vec::new()
            }
            Err(err) if entries.len() > 1 && self.backend.rejects_items(&err) => {
                tracing::warn!(
                    "The storage rejected {count} buffered dataset items ({err}); storing them push by push, so \
                     that the rejected push fails alone."
                );
                self.store_one_by_one(entries).await
            }
            Err(err) => {
                let err = Arc::new(err);
                entries.into_iter().map(|entry| (entry, err.clone())).collect()
            }
        };
        self.settle_failed(failed)
    }

    /// Stores the pushes one at a time. Returns the ones that failed, with their errors: after one
    /// that failed but was not rejected, the rest is not tried, to keep their order.
    async fn store_one_by_one(self: &Arc<Self>, entries: Vec<Entry>) -> Vec<(Entry, Arc<StorageError>)> {
        let mut failed = Vec::new();
        let mut entries = entries.into_iter();
        while let Some(entry) = entries.next() {
            self.counters.write.fetch_add(1, Ordering::Relaxed);
            match self.backend.push_buffered(entry.items.clone()).await {
                Ok(()) => self.settle_stored(vec![entry]).await,
                Err(err) => {
                    let rejected = self.backend.rejects_items(&err);
                    let err = Arc::new(err);
                    failed.push((entry, err.clone()));
                    if !rejected {
                        failed.extend(entries.map(|entry| (entry, err.clone())));
                        break;
                    }
                }
            }
        }
        failed
    }

    /// Settles stored pushes: their counters, summed by sink, then their acks.
    async fn settle_stored(&self, mut entries: Vec<Entry>) {
        let mut sums: Vec<Accounting> = Vec::new();
        for entry in &mut entries {
            for (sink, counters) in std::mem::take(&mut entry.accounts) {
                match sums.iter_mut().find(|(known, _)| Arc::ptr_eq(known, &sink)) {
                    Some((_, sum)) => sum.merge(&counters),
                    None => sums.push((sink, counters)),
                }
            }
        }
        for (sink, counters) in &sums {
            sink.stored(counters).await;
        }
        for entry in &entries {
            entry.ack.settle(Ok(()));
        }
        self.state.lock().failures = 0;
    }

    /// Settles pushes that could not be stored. A push someone waits for fails, so that its owner
    /// handles it (a crawler retries its request); so does one the backend rejects, which can
    /// never be stored. Pushes nobody waits for go back to the head of the buffer, in order, and
    /// are retried with a backoff.
    fn settle_failed(self: &Arc<Self>, failed: Vec<(Entry, Arc<StorageError>)>) -> StorageResult<()> {
        if failed.is_empty() {
            return Ok(());
        }
        let mut kept = Vec::new();
        let mut last_error = None;
        for (entry, err) in failed {
            if entry.is_awaited() {
                entry.fail(err);
            } else if self.backend.rejects_items(&err) {
                tracing::error!(
                    "The storage rejected {} buffered dataset items, which are lost: {err}",
                    entry.items.len()
                );
                last_error = Some(err.clone());
                entry.fail(err);
            } else {
                entry.ack.in_flight.store(false, Ordering::Release);
                last_error = Some(err);
                kept.push(entry);
            }
        }
        let Some(err) = last_error else { return Ok(()) };
        if !kept.is_empty() {
            let mut state = self.state.lock();
            state.failures += 1;
            let backoff = retry_backoff(state.failures);
            let count: usize = kept.iter().map(|entry| entry.items.len()).sum();
            tracing::warn!(
                "Storing {count} buffered dataset items failed ({err}); retrying in {} s.",
                backoff.as_secs_f64()
            );
            state.bytes += kept.iter().map(|entry| entry.bytes).sum::<usize>();
            kept.append(&mut state.entries);
            state.entries = kept;
            // Replaces an earlier deadline: the retry waits for its backoff.
            state.deadline = None;
            self.schedule(&mut state, tokio::time::Instant::now() + backoff);
        }
        to_storage_result(Err(err))
    }

    /// Takes the push of `ack` out of the buffer, if it still waits there.
    fn withdraw(&self, ack: &Arc<Ack>) -> bool {
        let entry = {
            let mut state = self.state.lock();
            let Some(position) = state.entries.iter().position(|entry| Arc::ptr_eq(&entry.ack, ack)) else {
                return false;
            };
            let entry = state.entries.remove(position);
            state.bytes -= entry.bytes;
            entry
        };
        entry.fail(Arc::new(StorageError::Backend("the buffered items were withdrawn".into())));
        true
    }
}

impl Drop for DatasetBuffer {
    fn drop(&mut self) {
        if let Some((events, ids)) = self.listeners.lock().take() {
            for id in ids {
                events.off(id);
            }
        }
        let left: usize = self.state.get_mut().entries.iter().map(|entry| entry.items.len()).sum();
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

    /// Stores items in memory and logs every backend push; fails the pushes it is told to, takes
    /// `delay` for each, rejects items holding `too_big` up front and pushes holding `poison`.
    #[derive(Default)]
    struct Backend {
        items: Mutex<Vec<String>>,
        pushes: Mutex<Vec<usize>>,
        fail_next: AtomicUsize,
        delay: Mutex<Duration>,
    }

    /// Records what it settles.
    #[derive(Default)]
    struct Sink {
        stored: Mutex<Vec<FlushCounters>>,
        dropped: Mutex<Vec<FlushCounters>>,
    }

    #[async_trait]
    impl FlushSink for Sink {
        async fn stored(&self, counters: &FlushCounters) {
            self.stored.lock().push(counters.clone());
        }
        fn dropped(&self, counters: &FlushCounters) {
            self.dropped.lock().push(counters.clone());
        }
    }

    fn counted(count: u64) -> FlushCounters {
        let mut counters = FlushCounters::new();
        counters.add("item", count);
        counters
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
            tokio::task::yield_now().await;
            let delay = *self.delay.lock();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if self.fail_next.load(Ordering::SeqCst) > 0 {
                self.fail_next.fetch_sub(1, Ordering::SeqCst);
                return Err(StorageError::Backend("the API is down".into()));
            }
            if items.iter().any(|item| item.get().contains("poison")) {
                return Err(StorageError::InvalidArgument("poisoned items".into()));
            }
            self.pushes.lock().push(items.len());
            self.items.lock().extend(items.iter().map(|item| item.get().to_owned()));
            Ok(())
        }
        fn validate_items(&self, items: &[DatasetItem]) -> StorageResult<()> {
            match items.iter().any(|item| item.get().contains("too_big")) {
                true => Err(StorageError::InvalidArgument("an item is too big".into())),
                false => Ok(()),
            }
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
            self.pushes.lock().clone()
        }
        fn values(&self) -> Vec<u64> {
            let items = self.items.lock().clone();
            items
                .iter()
                .map(|item| serde_json::from_str::<serde_json::Value>(item).unwrap()["n"].as_u64().unwrap())
                .collect()
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

        // A failed flush fails the writes it held for their owner (whose flush call succeeds: the
        // owner handles it), gives them up, and the next flush goes on.
        backend.fail_next.store(1, Ordering::SeqCst);
        let failed = PendingWrites::new();
        failed.scope(push(&dataset, 2)).await;
        dataset.flush().await.unwrap();
        let err = failed.wait().await.unwrap_err();
        assert!(err.to_string().contains("the API is down"), "{err}");
        assert!(failed.settled().unwrap().is_err());
        push(&dataset, 3).await;
        dataset.flush().await.unwrap();
        assert_eq!(backend.values(), [0, 1, 3]);
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
    async fn counters_are_settled_once_per_flush() {
        let (dataset, _) = buffered(stages(&[(0, 3_600_000)]), None);
        let sink = Arc::new(Sink::default());
        for count in [1, 2] {
            let items: Vec<_> = (0..count).map(|n| json!({ "n": n })).collect();
            let (pushed, left) = with_accounting(sink.clone(), counted(count), dataset.push_data(&items)).await;
            pushed.unwrap();
            assert!(left.is_none(), "the buffer took the counters over");
        }
        // Without accounting, nothing is counted.
        push(&dataset, 9).await;
        assert!(sink.stored.lock().is_empty());
        dataset.flush().await.unwrap();
        dataset.flush().await.unwrap();
        assert_eq!(*sink.stored.lock(), [counted(3)], "summed, once");

        // An unbuffered dataset leaves them to the caller.
        let unbuffered = Dataset::from_backend(Arc::new(Backend::default()));
        let (pushed, left) = with_accounting(sink.clone(), counted(1), unbuffered.push_data(&json!({ "n": 1 }))).await;
        pushed.unwrap();
        assert_eq!(left, Some(counted(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_flush_runs_to_its_end_when_its_caller_stops_waiting() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        *backend.delay.lock() = Duration::from_millis(100);
        let sink = Arc::new(Sink::default());
        let writes = PendingWrites::new();
        let (pushed, _) = writes.scope(with_accounting(sink.clone(), counted(1), push(&dataset, 1))).await;
        let () = pushed;
        assert!(tokio::time::timeout(Duration::from_millis(10), dataset.get_info()).await.is_err());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(backend.stored(), 1);
        assert_eq!(writes.settled().map(|result| result.is_ok()), Some(true));
        assert_eq!(*sink.stored.lock(), [counted(1)]);
    }

    #[tokio::test(start_paused = true)]
    async fn items_nobody_waits_for_are_kept_in_order_and_retried() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        let sink = Arc::new(Sink::default());
        let (pushed, _) = with_accounting(sink.clone(), counted(1), push(&dataset, 1)).await;
        let () = pushed;
        push(&dataset, 2).await;
        backend.fail_next.store(1, Ordering::SeqCst);
        let err = dataset.flush().await.unwrap_err();
        assert!(err.to_string().contains("the API is down"), "{err}");
        assert_eq!(backend.stored(), 0);
        assert!(sink.dropped.lock().is_empty(), "kept, not given up");
        push(&dataset, 3).await;
        dataset.flush().await.unwrap();
        assert_eq!(backend.values(), [1, 2, 3]);
        assert_eq!(*sink.stored.lock(), [counted(1)]);

        // Retried on their own, after a backoff.
        backend.fail_next.store(1, Ordering::SeqCst);
        push(&dataset, 4).await;
        assert!(dataset.flush().await.is_err());
        tokio::time::sleep(Duration::from_millis(999)).await;
        assert_eq!(backend.stored(), 3);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(backend.values(), [1, 2, 3, 4]);

        // So are the items of a scope that is gone.
        let writes = PendingWrites::new();
        writes.scope(push(&dataset, 5)).await;
        drop(writes);
        backend.fail_next.store(1, Ordering::SeqCst);
        assert!(dataset.flush().await.is_err());
        dataset.flush().await.unwrap();
        assert_eq!(backend.values(), [1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn items_someone_waits_for_fail_and_give_their_counters_back() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        let sink = Arc::new(Sink::default());
        let writes = PendingWrites::new();
        let (pushed, _) = writes.scope(with_accounting(sink.clone(), counted(1), push(&dataset, 1))).await;
        let () = pushed;
        backend.fail_next.store(1, Ordering::SeqCst);
        dataset.flush().await.unwrap();
        assert!(writes.wait().await.is_err());
        assert_eq!(*sink.dropped.lock(), [counted(1)]);
        dataset.flush().await.unwrap();
        assert_eq!(backend.stored(), 0, "given up");
    }

    #[tokio::test]
    async fn rejected_items_fail_their_push_alone() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        push(&dataset, 1).await;
        let err = dataset.push_data(&json!({ "n": 2, "too_big": true })).await.unwrap_err();
        assert!(matches!(err, StorageError::InvalidArgument(_)), "{err}");
        push(&dataset, 3).await;
        dataset.flush().await.unwrap();
        assert_eq!(backend.values(), [1, 3]);
    }

    #[tokio::test]
    async fn a_rejected_flush_is_split_push_by_push() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        let scopes: Vec<_> = (0..3).map(|_| PendingWrites::new()).collect();
        scopes[0].scope(push(&dataset, 1)).await;
        scopes[1].scope(dataset.push_data(&json!({ "n": 2, "poison": true }))).await.unwrap();
        scopes[2].scope(push(&dataset, 3)).await;
        dataset.flush().await.unwrap();
        assert_eq!(backend.values(), [1, 3]);
        assert_eq!(
            scopes.iter().map(|scope| scope.settled().unwrap().is_ok()).collect::<Vec<_>>(),
            [true, false, true]
        );

        // A rejected push nobody waits for is lost: the flush reports it.
        dataset.push_data(&json!({ "n": 4, "poison": true })).await.unwrap();
        push(&dataset, 5).await;
        assert!(dataset.flush().await.is_err());
        dataset.flush().await.unwrap();
        assert_eq!(backend.values(), [1, 3, 5]);
    }

    #[tokio::test]
    async fn withdrawn_items_are_not_stored() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        let sink = Arc::new(Sink::default());
        let writes = PendingWrites::new();
        let (pushed, _) = writes.scope(with_accounting(sink.clone(), counted(2), push(&dataset, 1))).await;
        let () = pushed;
        writes.scope(push(&dataset, 2)).await;
        push(&dataset, 3).await;
        assert_eq!(writes.withdraw(), 2);
        assert!(writes.wait().await.is_err());
        assert_eq!(*sink.dropped.lock(), [counted(2)]);
        dataset.flush().await.unwrap();
        assert_eq!(backend.values(), [3]);
        assert_eq!(writes.withdraw(), 0);
    }

    #[tokio::test]
    async fn pushes_of_an_immediate_scope_are_stored_before_they_return() {
        let (dataset, backend) = buffered(stages(&[(0, 3_600_000)]), None);
        push(&dataset, 1).await;
        let immediate = PendingWrites::immediate();
        immediate.scope(push(&dataset, 2)).await;
        assert_eq!(backend.values(), [1, 2]);
        backend.fail_next.store(1, Ordering::SeqCst);
        assert!(immediate.scope(dataset.push_data(&json!({ "n": 3 }))).await.is_err());
    }
}
