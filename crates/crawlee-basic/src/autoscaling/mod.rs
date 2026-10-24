//! Autoscaling: the [`ConcurrencySystem`] decides how many requests run at once, from the load of
//! the system. It is a port of `ConcurrencySystem`, `SystemStatus` and the load signals of Crawlee
//! for JS, with the same defaults and the same algorithm:
//!
//! - **Starting a request.** A new request may start while fewer than `desired` requests run, and
//!   only if the system was idle over the last 5 s. The exception is below
//!   [`min_concurrency`](ConcurrencyOptions::min_concurrency), where a request may always start.
//! - **Scaling up.** Every 10 s, `desired` grows by 5 % when the system was idle over the last
//!   30 s and at least 90 % of `desired` is in use.
//! - **Scaling down.** On the same tick, `desired` shrinks by 5 % when the system was overloaded.
//! - **What "overloaded" means.** The system is overloaded when any load signal is: memory,
//!   CPU, runtime responsiveness or storage rate limits. A signal is overloaded when more than
//!   its ratio of the sampled time was.
//!
//! For a fixed concurrency, set `min_concurrency` and `max_concurrency` to the same value.

mod signals;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crawlee_core::Services;

pub use self::signals::{
    CpuLoadSignal, CpuLoadSignalOptions, EventLoopLoadSignal, EventLoopLoadSignalOptions, LoadSignal, LoadSignalInfo,
    LoadSnapshot, MemoryLoadSignal, MemoryLoadSignalOptions, SnapshotStore, StorageLoadSignal,
    StorageLoadSignalOptions, evaluate_sample,
};

/// The built-in load signals (`None` switches one off) and custom ones.
#[derive(Clone)]
pub struct LoadSignalsOptions {
    pub memory: Option<MemoryLoadSignalOptions>,
    pub event_loop: Option<EventLoopLoadSignalOptions>,
    pub cpu: Option<CpuLoadSignalOptions>,
    pub storage: Option<StorageLoadSignalOptions>,
    pub custom: Vec<Arc<dyn LoadSignal>>,
}

impl Default for LoadSignalsOptions {
    fn default() -> Self {
        LoadSignalsOptions {
            memory: Some(MemoryLoadSignalOptions::default()),
            event_loop: Some(EventLoopLoadSignalOptions::default()),
            cpu: Some(CpuLoadSignalOptions::default()),
            storage: Some(StorageLoadSignalOptions::default()),
            custom: Vec::new(),
        }
    }
}

impl std::fmt::Debug for LoadSignalsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadSignalsOptions")
            .field("memory", &self.memory)
            .field("event_loop", &self.event_loop)
            .field("cpu", &self.cpu)
            .field("storage", &self.storage)
            .field("custom", &self.custom.iter().map(|signal| signal.name().to_owned()).collect::<Vec<_>>())
            .finish()
    }
}

/// Options of a [`ConcurrencySystem`], with the defaults of Crawlee for JS.
#[derive(Clone, Debug)]
pub struct ConcurrencyOptions {
    pub min_concurrency: usize,
    pub max_concurrency: usize,
    /// Where autoscaling starts. Defaults to `min_concurrency`.
    pub desired_concurrency: Option<usize>,
    /// Scale up only when at least this share of the desired concurrency is in use.
    pub desired_concurrency_ratio: f64,
    pub scale_up_step_ratio: f64,
    pub scale_down_step_ratio: f64,
    pub autoscale_interval: Duration,
    /// How often the state is logged; `None` never.
    pub logging_interval: Option<Duration>,
    /// Requests started per minute at most (`maxRequestsPerMinute`); `None` for no limit.
    pub max_tasks_per_minute: Option<u64>,
    pub load_signals: LoadSignalsOptions,
    /// The window of the scaling decisions.
    pub snapshot_history: Duration,
    /// The window of the decision to start a request.
    pub current_history: Duration,
}

impl Default for ConcurrencyOptions {
    fn default() -> Self {
        ConcurrencyOptions {
            min_concurrency: 1,
            max_concurrency: 200,
            desired_concurrency: None,
            desired_concurrency_ratio: 0.9,
            scale_up_step_ratio: 0.05,
            scale_down_step_ratio: 0.05,
            autoscale_interval: Duration::from_secs(10),
            logging_interval: Some(Duration::from_secs(60)),
            max_tasks_per_minute: None,
            load_signals: LoadSignalsOptions::default(),
            snapshot_history: Duration::from_secs(30),
            current_history: Duration::from_secs(5),
        }
    }
}

impl ConcurrencyOptions {
    /// The defaults of HTTP crawlers (`HTTP_OPTIMIZED_CONCURRENCY_SYSTEM_OPTIONS`): start at 10,
    /// and tolerate a busier runtime, since parsing responses keeps it busy by design.
    pub fn http_optimized() -> Self {
        ConcurrencyOptions {
            desired_concurrency: Some(10),
            load_signals: LoadSignalsOptions {
                event_loop: Some(EventLoopLoadSignalOptions {
                    snapshot_interval: Duration::from_secs(2),
                    max_blocked: Duration::from_millis(100),
                    overloaded_ratio: 0.7,
                }),
                ..LoadSignalsOptions::default()
            },
            ..ConcurrencyOptions::default()
        }
    }
}

/// The load of every signal over one window.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SystemStatusInfo {
    pub is_system_idle: bool,
    /// `(signal name, info)`, e.g. `("memInfo", ...)`.
    pub signals: Vec<(String, LoadSignalInfo)>,
}

impl SystemStatusInfo {
    pub fn signal(&self, name: &str) -> Option<&LoadSignalInfo> {
        self.signals.iter().find(|(signal, _)| signal == name).map(|(_, info)| info)
    }
}

/// Evaluates the signals over the current (5 s) and historical (30 s) windows.
pub struct SystemStatus {
    signals: Vec<Arc<dyn LoadSignal>>,
    current_history: Duration,
    history: Duration,
}

impl SystemStatus {
    /// Fails when two signals share a name.
    pub fn new(
        signals: Vec<Arc<dyn LoadSignal>>,
        current_history: Duration,
        history: Duration,
    ) -> Result<Self, ConcurrencyError> {
        let mut names = std::collections::HashSet::new();
        for signal in &signals {
            if !names.insert(signal.name().to_owned()) {
                return Err(ConcurrencyError::DuplicateSignal(signal.name().to_owned()));
            }
        }
        Ok(SystemStatus { signals, current_history, history })
    }

    pub fn max_sample_window(&self) -> Duration {
        self.current_history.max(self.history)
    }

    pub fn current_status(&self) -> SystemStatusInfo {
        self.status(self.current_history)
    }

    pub fn historical_status(&self) -> SystemStatusInfo {
        self.status(self.history)
    }

    fn status(&self, window: Duration) -> SystemStatusInfo {
        let signals: Vec<(String, LoadSignalInfo)> = self
            .signals
            .iter()
            .map(|signal| {
                (signal.name().to_owned(), evaluate_sample(&signal.sample(Some(window)), signal.overloaded_ratio()))
            })
            .collect();
        SystemStatusInfo { is_system_idle: signals.iter().all(|(_, info)| !info.is_overloaded), signals }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConcurrencyError {
    #[error("min_concurrency and max_concurrency must be at least 1")]
    InvalidBounds,
    #[error("duplicate load signal name {0:?}; switch the built-in signal off to replace it, or rename yours")]
    DuplicateSignal(String),
}

#[derive(Debug)]
struct State {
    min: usize,
    max: usize,
    desired: usize,
    current: usize,
    /// Tasks started in each of the last 60 seconds, newest first.
    tasks_per_minute: VecDeque<u64>,
    last_logged: Option<Instant>,
}

impl State {
    fn clamp_desired(&mut self) {
        self.desired = self.desired.max(self.min).min(self.max);
    }

    fn over_max_tasks_per_minute(&self, limit: Option<u64>) -> bool {
        limit.is_some_and(|limit| self.tasks_per_minute.iter().sum::<u64>() >= limit)
    }
}

/// Decides how many tasks run at once; see the [module documentation](self).
pub struct ConcurrencySystem {
    options: ConcurrencyOptions,
    state: Mutex<State>,
    signals: Vec<Arc<dyn LoadSignal>>,
    status: SystemStatus,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    running: AtomicBool,
}

impl std::fmt::Debug for ConcurrencySystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrencySystem").field("state", &*self.state.lock()).finish_non_exhaustive()
    }
}

impl ConcurrencySystem {
    pub fn new(options: ConcurrencyOptions) -> Result<Self, ConcurrencyError> {
        // min > max is allowed: the desired concurrency is clamped to the max (concurrency_system.ts).
        if options.min_concurrency == 0 || options.max_concurrency == 0 {
            return Err(ConcurrencyError::InvalidBounds);
        }
        let signal_options = &options.load_signals;
        let mut signals: Vec<Arc<dyn LoadSignal>> = Vec::new();
        if let Some(memory) = signal_options.memory {
            signals.push(Arc::new(MemoryLoadSignal::new(memory)));
        }
        if let Some(event_loop) = signal_options.event_loop {
            signals.push(Arc::new(EventLoopLoadSignal::new(event_loop)));
        }
        if let Some(cpu) = signal_options.cpu {
            signals.push(Arc::new(CpuLoadSignal::new(cpu)));
        }
        if let Some(storage) = signal_options.storage {
            signals.push(Arc::new(StorageLoadSignal::new(storage)));
        }
        signals.extend(signal_options.custom.iter().cloned());
        let status = SystemStatus::new(signals.clone(), options.current_history, options.snapshot_history)?;

        let mut state = State {
            min: options.min_concurrency,
            max: options.max_concurrency,
            desired: options.desired_concurrency.unwrap_or(options.min_concurrency),
            current: 0,
            tasks_per_minute: VecDeque::from(vec![0; 60]),
            last_logged: None,
        };
        state.clamp_desired();
        Ok(ConcurrencySystem {
            options,
            state: Mutex::new(state),
            signals,
            status,
            tasks: Mutex::new(Vec::new()),
            running: AtomicBool::new(false),
        })
    }

    pub fn desired_concurrency(&self) -> usize {
        self.state.lock().desired
    }

    pub fn current_concurrency(&self) -> usize {
        self.state.lock().current
    }

    pub fn min_concurrency(&self) -> usize {
        self.state.lock().min
    }

    pub fn max_concurrency(&self) -> usize {
        self.state.lock().max
    }

    pub fn set_min_concurrency(&self, min: usize) {
        let mut state = self.state.lock();
        state.min = min.max(1);
        state.clamp_desired();
    }

    pub fn set_max_concurrency(&self, max: usize) {
        let mut state = self.state.lock();
        state.max = max.max(1);
        state.clamp_desired();
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Starts the load signals and the autoscaling. Does nothing when already running.
    pub fn start(self: &Arc<Self>, services: &Services) {
        if self.running.swap(true, Ordering::AcqRel) {
            return;
        }
        {
            let mut state = self.state.lock();
            state.tasks_per_minute = VecDeque::from(vec![0; 60]);
            state.last_logged = None;
        }
        for signal in &self.signals {
            signal.start(services, self.status.max_sample_window());
        }

        let mut tasks = self.tasks.lock();
        let system = Arc::downgrade(self);
        let interval = self.options.autoscale_interval;
        tasks.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(system) = system.upgrade() else { return };
                system.autoscale();
            }
        }));
        if self.options.max_tasks_per_minute.is_some() {
            let system = Arc::downgrade(self);
            tasks.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(1));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let Some(system) = system.upgrade() else { return };
                    let mut state = system.state.lock();
                    state.tasks_per_minute.push_front(0);
                    state.tasks_per_minute.pop_back();
                }
            }));
        }
    }

    pub fn stop(&self) {
        if !self.running.swap(false, Ordering::AcqRel) {
            return;
        }
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
        for signal in &self.signals {
            signal.stop();
        }
    }

    /// Whether a task may start now: fewer than `desired` are running, and the system is idle
    /// (or fewer than `min` are running).
    pub fn has_capacity_for_task(&self) -> bool {
        let state = self.state.lock();
        if state.current >= state.desired {
            return false;
        }
        if state.current >= state.min && !self.status.current_status().is_system_idle {
            tracing::trace!("Task will not run. System is overloaded.");
            return false;
        }
        true
    }

    /// [`has_capacity_for_task`](Self::has_capacity_for_task), and the per-minute limit is not reached.
    pub fn can_start_task(&self) -> bool {
        self.has_capacity_for_task() && !self.state.lock().over_max_tasks_per_minute(self.options.max_tasks_per_minute)
    }

    /// Counts a task as started, whatever the capacity. Pair it with
    /// [`register_task_end`](Self::register_task_end).
    pub fn register_task_start(&self) {
        let mut state = self.state.lock();
        state.current += 1;
        if let Some(this_second) = state.tasks_per_minute.front_mut() {
            *this_second += 1;
        }
    }

    /// Registers a task start if [`can_start_task`](Self::can_start_task).
    pub fn try_register_task_start(&self) -> bool {
        let can_start = self.can_start_task();
        if can_start {
            self.register_task_start();
        }
        can_start
    }

    pub fn register_task_end(&self) {
        let mut state = self.state.lock();
        state.current = state.current.saturating_sub(1);
    }

    pub fn current_status(&self) -> SystemStatusInfo {
        self.status.current_status()
    }

    /// One autoscaling step; runs every `autoscale_interval` while started.
    pub fn autoscale(&self) {
        let status = self.status.historical_status();
        let mut state = self.state.lock();
        if state.over_max_tasks_per_minute(self.options.max_tasks_per_minute) {
            return;
        }
        let reaching_desired =
            state.current as f64 >= (state.desired as f64 * self.options.desired_concurrency_ratio).floor();
        if status.is_system_idle && state.desired < state.max && reaching_desired {
            let step = (state.desired as f64 * self.options.scale_up_step_ratio).ceil() as usize;
            let old = state.desired;
            state.desired = (state.desired + step).min(state.max);
            tracing::debug!(old, new = state.desired, "Scaling up.");
        }
        if !status.is_system_idle && state.desired > state.min {
            let step = (state.desired as f64 * self.options.scale_down_step_ratio).ceil() as usize;
            let old = state.desired;
            state.desired = state.desired.saturating_sub(step).max(state.min);
            tracing::debug!(old, new = state.desired, ?status, "Scaling down.");
        }
        if let Some(interval) = self.options.logging_interval {
            let now = Instant::now();
            match state.last_logged {
                None => state.last_logged = Some(now),
                Some(last) if now.duration_since(last) > interval => {
                    state.last_logged = Some(now);
                    let overloaded: Vec<&str> = status
                        .signals
                        .iter()
                        .filter(|(_, info)| info.is_overloaded)
                        .map(|(name, _)| name.as_str())
                        .collect();
                    tracing::info!(
                        current_concurrency = state.current,
                        desired_concurrency = state.desired,
                        is_system_idle = status.is_system_idle,
                        ?overloaded,
                        "ConcurrencySystem state"
                    );
                }
                Some(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    /// A signal whose overload the test sets.
    #[derive(Default)]
    struct Switch {
        overloaded: AtomicBool,
    }

    impl LoadSignal for Switch {
        fn name(&self) -> &str {
            "switch"
        }
        fn overloaded_ratio(&self) -> f64 {
            0.5
        }
        fn start(&self, _: &Services, _: Duration) {}
        fn stop(&self) {}
        fn sample(&self, _: Option<Duration>) -> Vec<LoadSnapshot> {
            vec![LoadSnapshot { created_at: Utc::now(), is_overloaded: self.overloaded.load(Ordering::SeqCst) }]
        }
    }

    fn system(options: ConcurrencyOptions, switch: &Arc<Switch>) -> ConcurrencySystem {
        let custom: Arc<dyn LoadSignal> = switch.clone();
        ConcurrencySystem::new(ConcurrencyOptions {
            load_signals: LoadSignalsOptions {
                memory: None,
                event_loop: None,
                cpu: None,
                storage: None,
                custom: vec![custom],
            },
            ..options
        })
        .unwrap()
    }

    /// A contradictory pair (min above max) resolves in favour of the maximum, as in JS.
    #[test]
    fn min_above_max_clamps_the_desired_concurrency_to_the_max() {
        let system = ConcurrencySystem::new(ConcurrencyOptions {
            min_concurrency: 10,
            max_concurrency: 4,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(system.desired_concurrency(), 4);
    }

    #[test]
    fn capacity_follows_desired_and_load() {
        let switch = Arc::new(Switch::default());
        let system = system(
            ConcurrencyOptions { min_concurrency: 2, desired_concurrency: Some(3), ..Default::default() },
            &switch,
        );
        for _ in 0..3 {
            assert!(system.try_register_task_start());
        }
        assert!(!system.try_register_task_start(), "desired concurrency reached");
        system.register_task_end();
        system.register_task_end();
        switch.overloaded.store(true, Ordering::SeqCst);
        assert!(system.try_register_task_start(), "below min, tasks start even when overloaded");
        assert!(!system.has_capacity_for_task(), "at min and overloaded");
    }

    #[test]
    fn scales_up_when_idle_and_busy_and_down_when_overloaded() {
        let switch = Arc::new(Switch::default());
        let system = system(ConcurrencyOptions { desired_concurrency: Some(10), ..Default::default() }, &switch);

        // Idle but only 8 of 10 in use (less than 90 %): no change.
        for _ in 0..8 {
            system.register_task_start();
        }
        system.autoscale();
        assert_eq!(system.desired_concurrency(), 10);

        system.register_task_start();
        system.autoscale();
        assert_eq!(system.desired_concurrency(), 11, "ceil(10 * 0.05) = 1");

        switch.overloaded.store(true, Ordering::SeqCst);
        system.autoscale();
        assert_eq!(system.desired_concurrency(), 10);
        for _ in 0..20 {
            system.autoscale();
        }
        assert_eq!(system.desired_concurrency(), 1, "never below min");
    }

    #[test]
    fn max_tasks_per_minute() {
        let switch = Arc::new(Switch::default());
        let system = system(
            ConcurrencyOptions { desired_concurrency: Some(10), max_tasks_per_minute: Some(2), ..Default::default() },
            &switch,
        );
        assert!(system.try_register_task_start());
        system.register_task_end();
        assert!(system.try_register_task_start());
        system.register_task_end();
        assert!(!system.try_register_task_start(), "2 tasks started this minute");
        system.state.lock().tasks_per_minute.rotate_right(59);
        assert!(!system.try_register_task_start(), "still within the minute");
        system.state.lock().tasks_per_minute = VecDeque::from(vec![0; 60]);
        assert!(system.try_register_task_start());
    }

    #[test]
    fn bounds_and_duplicate_names_are_rejected() {
        assert!(matches!(
            ConcurrencySystem::new(ConcurrencyOptions { max_concurrency: 0, ..Default::default() }),
            Err(ConcurrencyError::InvalidBounds)
        ));
        let a: Arc<dyn LoadSignal> = Arc::new(Switch::default());
        let b: Arc<dyn LoadSignal> = Arc::new(Switch::default());
        let options = ConcurrencyOptions {
            load_signals: LoadSignalsOptions { custom: vec![a, b], ..Default::default() },
            ..Default::default()
        };
        assert!(
            matches!(ConcurrencySystem::new(options), Err(ConcurrencyError::DuplicateSignal(name)) if name == "switch")
        );
    }
}
