//! Load signals: each keeps a short history of snapshots saying whether one resource was
//! overloaded at that moment. Ported from `load_signal.ts` and the four built-in signals of
//! Crawlee for JS.

use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use std::cell::RefCell;
use tokio::task::JoinHandle;

use crawlee_core::{Event, EventKind, EventManager, Services, SystemInfo};

/// Whether a resource was overloaded at `created_at`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadSnapshot {
    pub created_at: DateTime<Utc>,
    pub is_overloaded: bool,
}

/// How loaded a resource was over a time window.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LoadSignalInfo {
    pub is_overloaded: bool,
    /// The share of overloaded time above which the resource counts as overloaded.
    pub limit_ratio: f64,
    /// The share of overloaded time in the window, rounded to 3 decimals.
    pub actual_ratio: f64,
}

/// A source of [`LoadSnapshot`]s. The built-in ones measure memory, CPU, runtime responsiveness
/// and storage rate limiting; custom ones can be added through
/// [`LoadSignalsOptions::custom`](super::LoadSignalsOptions::custom).
pub trait LoadSignal: 'static {
    /// Unique among the signals of one concurrency system.
    fn name(&self) -> &str;
    /// Overloaded when more than this share of the sampled time was overloaded.
    fn overloaded_ratio(&self) -> f64;
    /// Starts collecting snapshots. `max_sample_window` is the longest window that will be
    /// sampled; older snapshots can be dropped.
    fn start(&self, services: &Services, max_sample_window: Duration);
    fn stop(&self);
    /// Snapshots of the last `duration` before the newest one (all of them for `None`), oldest first.
    fn sample(&self, duration: Option<Duration>) -> Vec<LoadSnapshot>;
}

/// Snapshots of one signal, pruned to the longest sample window.
#[derive(Debug, Default)]
pub struct SnapshotStore {
    snapshots: VecDeque<LoadSnapshot>,
    history: Option<Duration>,
}

impl SnapshotStore {
    pub fn use_sample_window(&mut self, window: Duration) {
        self.history = Some(window);
    }

    pub fn push(&mut self, snapshot: LoadSnapshot) {
        if let Some(history) = self.history {
            let limit = chrono::Duration::from_std(history).unwrap_or(chrono::Duration::MAX);
            while self.snapshots.front().is_some_and(|oldest| snapshot.created_at - oldest.created_at > limit) {
                self.snapshots.pop_front();
            }
        }
        self.snapshots.push_back(snapshot);
    }

    /// Snapshots within `duration` of the newest one; the window is relative to the newest
    /// snapshot, not to now.
    pub fn sample(&self, duration: Option<Duration>) -> Vec<LoadSnapshot> {
        let Some(duration) = duration.filter(|duration| !duration.is_zero()) else {
            return self.snapshots.iter().copied().collect();
        };
        let Some(latest) = self.snapshots.back() else {
            return Vec::new();
        };
        let limit = chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::MAX);
        let start = self.snapshots.iter().rposition(|s| latest.created_at - s.created_at > limit).map_or(0, |i| i + 1);
        self.snapshots.range(start..).copied().collect()
    }

    pub fn last(&self) -> Option<&LoadSnapshot> {
        self.snapshots.back()
    }

    pub fn clear(&mut self) {
        self.snapshots.clear();
    }
}

/// The time-weighted share of overloaded snapshots. Each snapshot but the first weighs as much
/// as the time since the one before it; the first only anchors the timeline.
pub fn evaluate_sample(sample: &[LoadSnapshot], overloaded_ratio: f64) -> LoadSignalInfo {
    let ratio = match sample {
        [] => return LoadSignalInfo { is_overloaded: false, limit_ratio: overloaded_ratio, actual_ratio: 0.0 },
        [only] => f64::from(u8::from(only.is_overloaded)),
        _ => {
            let (mut weighted, mut total) = (0.0, 0.0);
            for pair in sample.windows(2) {
                // Snapshots taken in the same millisecond still count.
                let weight = ((pair[1].created_at - pair[0].created_at).num_milliseconds() as f64).max(0.0);
                let weight = if weight == 0.0 { 1.0 } else { weight };
                weighted += weight * f64::from(u8::from(pair[1].is_overloaded));
                total += weight;
            }
            weighted / total
        }
    };
    LoadSignalInfo {
        is_overloaded: ratio > overloaded_ratio,
        limit_ratio: overloaded_ratio,
        actual_ratio: (ratio * 1000.0).round() / 1000.0,
    }
}

/// The `SystemInfo` listener of a signal, removed on stop.
#[derive(Default)]
struct Subscription(RefCell<Option<(EventManager, crawlee_core::events::ListenerId)>>);

impl Subscription {
    fn subscribe(&self, events: &EventManager, handle: impl Fn(SystemInfo) + 'static) {
        let id = events.on(EventKind::SystemInfo, move |event| {
            if let Event::SystemInfo(info) = event {
                handle(info);
            }
            async {}
        });
        if let Some((events, previous)) = self.0.borrow_mut().replace((events.clone(), id)) {
            events.off(previous);
        }
    }

    fn unsubscribe(&self) {
        if let Some((events, id)) = self.0.borrow_mut().take() {
            events.off(id);
        }
    }
}

/// A task that takes snapshots on an interval, aborted on stop.
#[derive(Default)]
struct Ticker(RefCell<Option<JoinHandle<()>>>);

impl Ticker {
    fn start(&self, interval: Duration, mut tick: impl FnMut() + 'static) {
        let task = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(interval.max(Duration::from_millis(1)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                tick();
            }
        });
        if let Some(previous) = self.0.borrow_mut().replace(task) {
            previous.abort();
        }
    }

    fn stop(&self) {
        if let Some(task) = self.0.borrow_mut().take() {
            task.abort();
        }
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryLoadSignalOptions {
    /// A snapshot is overloaded when the used memory exceeds this share of the allowed memory.
    pub max_used_ratio: f64,
    pub overloaded_ratio: f64,
}

impl Default for MemoryLoadSignalOptions {
    fn default() -> Self {
        MemoryLoadSignalOptions { max_used_ratio: 0.9, overloaded_ratio: 0.2 }
    }
}

/// Memory of this process and its children against the allowed memory:
/// [`memory_mbytes`](crawlee_core::Configuration::memory_mbytes), or
/// [`available_memory_ratio`](crawlee_core::Configuration::available_memory_ratio) of the total.
pub struct MemoryLoadSignal {
    options: MemoryLoadSignalOptions,
    store: Rc<RefCell<SnapshotStore>>,
    subscription: Subscription,
}

const CRITICAL_OVERLOAD_LOG_INTERVAL: chrono::Duration = chrono::Duration::seconds(10);
const RESERVE_MEMORY_RATIO: f64 = 0.5;

impl MemoryLoadSignal {
    pub fn new(options: MemoryLoadSignalOptions) -> Self {
        MemoryLoadSignal { options, store: Rc::default(), subscription: Subscription::default() }
    }
}

impl LoadSignal for MemoryLoadSignal {
    fn name(&self) -> &str {
        "memInfo"
    }

    fn overloaded_ratio(&self) -> f64 {
        self.options.overloaded_ratio
    }

    fn start(&self, services: &Services, max_sample_window: Duration) {
        {
            let mut store = self.store.borrow_mut();
            store.use_sample_window(max_sample_window);
            store.clear();
        }
        let configuration = &services.configuration;
        let fixed_max_bytes = configuration.memory_mbytes.filter(|&mb| mb > 0).map(|mb| (mb * 1024 * 1024) as f64);
        let ratio = configuration.available_memory_ratio;
        if fixed_max_bytes.is_none() {
            tracing::debug!(
                "Setting max memory of this run to {} % of available memory. Use the CRAWLEE_MEMORY_MBYTES or \
                 CRAWLEE_AVAILABLE_MEMORY_RATIO environment variable to override it.",
                ratio * 100.0
            );
        }
        let max_used_ratio = self.options.max_used_ratio;
        let store = self.store.clone();
        let last_warning: RefCell<Option<DateTime<Utc>>> = RefCell::new(None);
        self.subscription.subscribe(&services.events, move |info| {
            let (Some(used), Some(total)) = (info.mem_current_bytes, info.mem_total_bytes) else {
                return;
            };
            let max_bytes = fixed_max_bytes.unwrap_or(ratio * total as f64);
            let used = used as f64;
            store.borrow_mut().push(LoadSnapshot { created_at: info.created_at, is_overloaded: used / max_bytes > max_used_ratio });

            let critical = max_bytes * max_used_ratio + max_bytes * (1.0 - max_used_ratio) * RESERVE_MEMORY_RATIO;
            let mut last_warning = last_warning.borrow_mut();
            let rate_limited = last_warning.is_some_and(|at| info.created_at < at + CRITICAL_OVERLOAD_LOG_INTERVAL);
            if used > critical && !rate_limited {
                let mb = |bytes: f64| (bytes / (1024.0 * 1024.0)).round();
                tracing::warn!(
                    "Memory is critically overloaded. Using {} MB of {} MB ({}%). Consider increasing available memory.",
                    mb(used),
                    mb(max_bytes),
                    (used / max_bytes * 100.0).round()
                );
                *last_warning = Some(info.created_at);
            }
        });
    }

    fn stop(&self) {
        self.subscription.unsubscribe();
    }

    fn sample(&self, duration: Option<Duration>) -> Vec<LoadSnapshot> {
        self.store.borrow_mut().sample(duration)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CpuLoadSignalOptions {
    pub overloaded_ratio: f64,
}

impl Default for CpuLoadSignalOptions {
    fn default() -> Self {
        CpuLoadSignalOptions { overloaded_ratio: 0.4 }
    }
}

/// CPU usage from the `SystemInfo` events: overloaded above
/// [`max_used_cpu_ratio`](crawlee_core::Configuration::max_used_cpu_ratio).
pub struct CpuLoadSignal {
    options: CpuLoadSignalOptions,
    store: Rc<RefCell<SnapshotStore>>,
    subscription: Subscription,
}

impl CpuLoadSignal {
    pub fn new(options: CpuLoadSignalOptions) -> Self {
        CpuLoadSignal { options, store: Rc::default(), subscription: Subscription::default() }
    }
}

impl LoadSignal for CpuLoadSignal {
    fn name(&self) -> &str {
        "cpuInfo"
    }

    fn overloaded_ratio(&self) -> f64 {
        self.options.overloaded_ratio
    }

    fn start(&self, services: &Services, max_sample_window: Duration) {
        {
            let mut store = self.store.borrow_mut();
            store.use_sample_window(max_sample_window);
            store.clear();
        }
        let store = self.store.clone();
        self.subscription.subscribe(&services.events, move |info| {
            store
                .borrow_mut()
                .push(LoadSnapshot { created_at: info.created_at, is_overloaded: info.is_cpu_overloaded });
        });
    }

    fn stop(&self) {
        self.subscription.unsubscribe();
    }

    fn sample(&self, duration: Option<Duration>) -> Vec<LoadSnapshot> {
        self.store.borrow_mut().sample(duration)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EventLoopLoadSignalOptions {
    pub snapshot_interval: Duration,
    /// A snapshot is overloaded when it comes this much later than scheduled.
    pub max_blocked: Duration,
    pub overloaded_ratio: f64,
}

impl Default for EventLoopLoadSignalOptions {
    fn default() -> Self {
        EventLoopLoadSignalOptions {
            snapshot_interval: Duration::from_millis(500),
            max_blocked: Duration::from_millis(50),
            overloaded_ratio: 0.6,
        }
    }
}

/// Responsiveness of the async runtime, the counterpart of the event loop signal of JS: a timer
/// task that fires late means the runtime's worker threads are busy (for example with CPU-heavy
/// handler code that does not yield).
pub struct EventLoopLoadSignal {
    options: EventLoopLoadSignalOptions,
    store: Rc<RefCell<SnapshotStore>>,
    ticker: Ticker,
}

impl EventLoopLoadSignal {
    pub fn new(options: EventLoopLoadSignalOptions) -> Self {
        EventLoopLoadSignal { options, store: Rc::default(), ticker: Ticker::default() }
    }
}

impl LoadSignal for EventLoopLoadSignal {
    fn name(&self) -> &str {
        "eventLoopInfo"
    }

    fn overloaded_ratio(&self) -> f64 {
        self.options.overloaded_ratio
    }

    fn start(&self, _services: &Services, max_sample_window: Duration) {
        {
            let mut store = self.store.borrow_mut();
            store.use_sample_window(max_sample_window);
            store.clear();
        }
        let store = self.store.clone();
        let (interval, max_blocked) = (self.options.snapshot_interval, self.options.max_blocked);
        let mut previous: Option<tokio::time::Instant> = None;
        self.ticker.start(interval, move || {
            let now = tokio::time::Instant::now();
            let late = previous.map(|previous| now.duration_since(previous).saturating_sub(interval));
            previous = Some(now);
            store.borrow_mut().push(LoadSnapshot {
                created_at: Utc::now(),
                is_overloaded: late.is_some_and(|late| late > max_blocked),
            });
        });
    }

    fn stop(&self) {
        self.ticker.stop();
    }

    fn sample(&self, duration: Option<Duration>) -> Vec<LoadSnapshot> {
        self.store.borrow_mut().sample(duration)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StorageLoadSignalOptions {
    pub snapshot_interval: Duration,
    /// A snapshot is overloaded when the storage hit more rate-limit errors than this since
    /// the previous one.
    pub max_errors: u64,
    pub overloaded_ratio: f64,
}

impl Default for StorageLoadSignalOptions {
    fn default() -> Self {
        StorageLoadSignalOptions { snapshot_interval: Duration::from_secs(1), max_errors: 3, overloaded_ratio: 0.3 }
    }
}

/// Rate-limit errors of the storage backend (from
/// [`StorageBackend::rate_limit_errors`](crawlee_core::StorageBackend::rate_limit_errors)). Local
/// storages never report any; a platform storage does.
pub struct StorageLoadSignal {
    options: StorageLoadSignalOptions,
    store: Rc<RefCell<SnapshotStore>>,
    ticker: Ticker,
}

impl StorageLoadSignal {
    pub fn new(options: StorageLoadSignalOptions) -> Self {
        StorageLoadSignal { options, store: Rc::default(), ticker: Ticker::default() }
    }
}

impl LoadSignal for StorageLoadSignal {
    fn name(&self) -> &str {
        "storageBackendInfo"
    }

    fn overloaded_ratio(&self) -> f64 {
        self.options.overloaded_ratio
    }

    fn start(&self, services: &Services, max_sample_window: Duration) {
        {
            let mut store = self.store.borrow_mut();
            store.use_sample_window(max_sample_window);
            store.clear();
        }
        let store = self.store.clone();
        let storage = services.storage.clone();
        let max_errors = self.options.max_errors;
        let mut previous: Option<u64> = None;
        self.ticker.start(self.options.snapshot_interval, move || {
            let errors = storage.rate_limit_errors();
            let delta = previous.map(|previous| errors.saturating_sub(previous));
            previous = Some(errors);
            store.borrow_mut().push(LoadSnapshot {
                created_at: Utc::now(),
                is_overloaded: delta.is_some_and(|delta| delta > max_errors),
            });
        });
    }

    fn stop(&self) {
        self.ticker.stop();
    }

    fn sample(&self, duration: Option<Duration>) -> Vec<LoadSnapshot> {
        self.store.borrow_mut().sample(duration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(millis: i64, is_overloaded: bool) -> LoadSnapshot {
        LoadSnapshot { created_at: DateTime::from_timestamp_millis(1_000_000 + millis).unwrap(), is_overloaded }
    }

    #[test]
    fn store_prunes_and_samples_relative_to_the_newest_snapshot() {
        let mut store = SnapshotStore::default();
        store.use_sample_window(Duration::from_secs(30));
        for second in 0..40 {
            store.push(at(second * 1000, second % 2 == 0));
        }
        assert_eq!(store.sample(None).len(), 31, "older than 30 s before the newest are pruned");
        let recent = store.sample(Some(Duration::from_secs(5)));
        assert_eq!(recent.first().unwrap().created_at, at(34_000, false).created_at);
        assert_eq!(recent.len(), 6);
    }

    #[test]
    fn evaluation_is_time_weighted() {
        assert_eq!(
            evaluate_sample(&[], 0.2),
            LoadSignalInfo { is_overloaded: false, limit_ratio: 0.2, actual_ratio: 0.0 }
        );
        assert!(evaluate_sample(&[at(0, true)], 0.2).is_overloaded);
        // The first snapshot only anchors: 1 s overloaded out of 4 s.
        let sample = [at(0, true), at(1000, true), at(4000, false)];
        let info = evaluate_sample(&sample, 0.2);
        assert_eq!(info.actual_ratio, 0.25);
        assert!(info.is_overloaded);
        assert!(!evaluate_sample(&sample, 0.3).is_overloaded);
    }
}
