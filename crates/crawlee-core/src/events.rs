//! [`EventManager`]: process-wide events, the counterpart of `EventManager` and
//! `LocalEventManager` in Crawlee for JS.
//!
//! Once [`init`](EventManager::init)ialized, the manager emits:
//! - [`Event::PersistState`] every [`persist_state_interval`](crate::Configuration::persist_state_interval)
//!   (60 s by default), so that components save their state;
//! - [`Event::SystemInfo`] every [`system_info_interval`](crate::Configuration::system_info_interval)
//!   (1 s by default), which autoscaling reads.
//!
//! Both are also emitted right away. Other events (`Migrating`, `Aborting`, `Exit`) come from the
//! platform. A future Apify SDK crate will emit them, as the JS SDK does.
//!
//! Listeners are async. Each emitted event runs its listeners concurrently, on the tokio runtime.
//! [`close`](EventManager::close) emits a final `PersistState` and waits until every listener has
//! finished, so the final state is on disk when it returns.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::configuration::Configuration;
use crate::system_info::{SystemInfo, SystemInfoSampler};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EventKind {
    PersistState,
    SystemInfo,
    Migrating,
    Aborting,
    Exit,
    StatusMessage,
    /// Every [`Event::Custom`], whatever its name.
    Custom,
}

/// Log level of a [`StatusMessage`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatusLevel {
    #[default]
    Debug,
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StatusMessage {
    pub crawler_id: String,
    pub message: String,
    /// The last message of a run.
    pub is_terminal: bool,
    pub level: StatusLevel,
}

/// How a run ends: what [`Event::Exit`] carries (the `ExitOptions` of `Actor.exit()` in the
/// Apify SDK for JS, which emits the event with them, its defaults applied).
#[derive(Clone, Debug, PartialEq)]
pub struct ExitOptions {
    /// The last status message of the run.
    pub status_message: Option<String>,
    /// The exit code of the process, 0 by default.
    pub exit_code: i32,
    /// Whether the process exits, `true` by default.
    pub exit: bool,
    /// How long the listeners have to finish before the process exits anyway (`timeoutSecs`),
    /// 30 s by default.
    pub timeout: Duration,
}

impl Default for ExitOptions {
    fn default() -> Self {
        ExitOptions { status_message: None, exit_code: 0, exit: true, timeout: Duration::from_secs(30) }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// Save state now. `is_migrating` is set when the process is about to move to another host.
    PersistState {
        is_migrating: bool,
    },
    SystemInfo(SystemInfo),
    /// The process is about to be migrated to another host; crawlers pause and persist.
    Migrating,
    /// The run is being aborted; crawlers pause and persist.
    Aborting,
    /// The process is about to exit, as described by the options.
    Exit(ExitOptions),
    StatusMessage(StatusMessage),
    /// An event crawlee-rs does not model, such as one of the platform events an Apify SDK
    /// relays. Listeners of [`EventKind::Custom`] get all of them and match on `name`.
    Custom {
        name: String,
        data: serde_json::Value,
    },
}

impl Event {
    pub fn kind(&self) -> EventKind {
        match self {
            Event::PersistState { .. } => EventKind::PersistState,
            Event::SystemInfo(_) => EventKind::SystemInfo,
            Event::Migrating => EventKind::Migrating,
            Event::Aborting => EventKind::Aborting,
            Event::Exit(_) => EventKind::Exit,
            Event::StatusMessage(_) => EventKind::StatusMessage,
            Event::Custom { .. } => EventKind::Custom,
        }
    }
}

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type Listener = Arc<dyn Fn(Event) -> BoxFuture + Send + Sync>;

/// Returned by [`EventManager::on`]; pass it to [`EventManager::off`] to remove the listener.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ListenerId(u64);

struct Registered {
    id: ListenerId,
    kind: EventKind,
    listener: Listener,
}

#[derive(Clone, Copy, Debug)]
struct SystemInfoOptions {
    interval: Duration,
    containerized: Option<bool>,
    max_used_cpu_ratio: f64,
}

struct Inner {
    listeners: Mutex<Vec<Registered>>,
    next_id: AtomicU64,
    /// Listener invocations that have not finished yet.
    in_flight: AtomicUsize,
    idle: Notify,
    intervals: Mutex<Option<JoinHandle<()>>>,
    /// Set by `stop_periodic_persist_state`: periodic `PersistState` events stop.
    persist_state_stopped: AtomicBool,
    persist_state_interval: Duration,
    system_info: Option<SystemInfoOptions>,
}

/// Emits events to async listeners. Cloning is cheap and clones share the manager.
#[derive(Clone)]
pub struct EventManager {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for EventManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventManager").field("initialized", &self.is_initialized()).finish_non_exhaustive()
    }
}

impl EventManager {
    fn with_options(persist_state_interval: Duration, system_info: Option<SystemInfoOptions>) -> Self {
        EventManager {
            inner: Arc::new(Inner {
                listeners: Mutex::new(Vec::new()),
                next_id: AtomicU64::new(0),
                in_flight: AtomicUsize::new(0),
                idle: Notify::new(),
                intervals: Mutex::new(None),
                persist_state_stopped: AtomicBool::new(false),
                persist_state_interval,
                system_info,
            }),
        }
    }

    /// A manager that only emits `PersistState` on its own. System info comes from elsewhere,
    /// like the platform events of an Apify SDK.
    pub fn new(persist_state_interval: Duration) -> Self {
        Self::with_options(persist_state_interval, None)
    }

    /// The local manager: `PersistState`, plus `SystemInfo` measured on this machine
    /// (`LocalEventManager` in JS).
    pub fn local(configuration: &Configuration) -> Self {
        Self::with_options(
            configuration.persist_state_interval,
            Some(SystemInfoOptions {
                interval: configuration.system_info_interval,
                containerized: configuration.containerized,
                max_used_cpu_ratio: configuration.max_used_cpu_ratio,
            }),
        )
    }

    /// Adds a listener for events of `kind`.
    pub fn on<F, Fut>(&self, kind: EventKind, listener: F) -> ListenerId
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let id = ListenerId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let listener: Listener = Arc::new(move |event| Box::pin(listener(event)));
        self.inner.listeners.lock().push(Registered { id, kind, listener });
        id
    }

    pub fn off(&self, id: ListenerId) {
        self.inner.listeners.lock().retain(|registered| registered.id != id);
    }

    pub fn listener_count(&self, kind: EventKind) -> usize {
        self.inner.listeners.lock().iter().filter(|registered| registered.kind == kind).count()
    }

    /// Runs every listener of the event's kind. Returns right away; the listeners run as tasks.
    /// Outside a tokio runtime, the event is dropped.
    pub fn emit(&self, event: Event) {
        let kind = event.kind();
        let listeners: Vec<Listener> = self
            .inner
            .listeners
            .lock()
            .iter()
            .filter(|registered| registered.kind == kind)
            .map(|registered| registered.listener.clone())
            .collect();
        if listeners.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::debug!(?kind, "No tokio runtime; the event is not delivered.");
            return;
        };
        for listener in listeners {
            self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
            let inner = self.inner.clone();
            let future = listener(event.clone());
            runtime.spawn(async move {
                // Counted down even if the listener panics.
                let _done = InFlightGuard(inner);
                future.await;
            });
        }
    }

    /// Waits until every listener started so far has finished.
    pub async fn wait_for_all_listeners_to_complete(&self) {
        loop {
            let idle = self.inner.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.inner.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.inner.intervals.lock().is_some()
    }

    /// Starts the periodic events. Does nothing when already initialized.
    pub async fn init(&self) {
        let mut intervals = self.inner.intervals.lock();
        if intervals.is_some() {
            return;
        }
        let manager = self.clone();
        *intervals = Some(tokio::spawn(async move { manager.emit_periodically().await }));
    }

    async fn emit_periodically(self) {
        let mut persist = tokio::time::interval(self.inner.persist_state_interval.max(Duration::from_millis(1)));
        persist.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let system_info = self.inner.system_info;
        let mut sampler = system_info.map(|options| {
            (
                SystemInfoSampler::new(options.containerized, options.max_used_cpu_ratio),
                tokio::time::interval(options.interval.max(Duration::from_millis(1))),
            )
        });
        if let Some((_, interval)) = &mut sampler {
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        }

        loop {
            tokio::select! {
                _ = persist.tick() => {
                    if !self.inner.persist_state_stopped.load(Ordering::Acquire) {
                        self.emit(Event::PersistState { is_migrating: false });
                    }
                }
                Some(sampler) = async {
                    match &mut sampler {
                        Some((sampler, interval)) => {
                            interval.tick().await;
                            Some(sampler)
                        }
                        None => std::future::pending().await,
                    }
                } => {
                    // Reading /proc takes a few hundred microseconds; not worth a blocking thread.
                    let info = sampler.sample();
                    self.emit(Event::SystemInfo(info));
                }
            }
        }
    }

    /// Stops the periodic `PersistState` events, keeping the others. The platform does this when
    /// a run is about to migrate: state is saved once more (with `is_migrating`), then no more.
    pub fn stop_periodic_persist_state(&self) {
        self.inner.persist_state_stopped.store(true, Ordering::Release);
    }

    /// Stops the periodic events, emits a final `PersistState` and waits for all listeners.
    /// Does nothing when not initialized.
    pub async fn close(&self) {
        let Some(intervals) = self.inner.intervals.lock().take() else {
            return;
        };
        intervals.abort();
        self.emit(Event::PersistState { is_migrating: false });
        self.wait_for_all_listeners_to_complete().await;
    }
}

struct InFlightGuard(Arc<Inner>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn periodic_events_and_close() {
        let configuration = Configuration {
            persist_state_interval: Duration::from_secs(60),
            system_info_interval: Duration::from_secs(1),
            ..Configuration::default()
        };
        let events = EventManager::local(&configuration);
        let persisted = Arc::new(AtomicUsize::new(0));
        let infos = Arc::new(AtomicUsize::new(0));
        let counter = persisted.clone();
        events.on(EventKind::PersistState, move |_| {
            let counter = counter.clone();
            async move {
                // A slow listener: close() has to wait for it.
                tokio::time::sleep(Duration::from_millis(500)).await;
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        let counter = infos.clone();
        let info_listener = events.on(EventKind::SystemInfo, move |event| {
            assert!(matches!(event, Event::SystemInfo(_)));
            counter.fetch_add(1, Ordering::SeqCst);
            async {}
        });

        events.init().await;
        events.init().await;
        tokio::time::sleep(Duration::from_millis(2500)).await;
        // Emitted right away, then every second.
        assert_eq!(infos.load(Ordering::SeqCst), 3);
        assert_eq!(persisted.load(Ordering::SeqCst), 1);

        events.off(info_listener);
        assert_eq!(events.listener_count(EventKind::SystemInfo), 0);
        events.close().await;
        assert_eq!(persisted.load(Ordering::SeqCst), 2, "close() waits for the final PersistState");
        assert!(!events.is_initialized());
    }

    #[tokio::test]
    async fn the_exit_event_carries_the_exit_options() {
        let events = EventManager::new(Duration::from_secs(60));
        let seen = Arc::new(parking_lot::Mutex::new(None));
        let log = seen.clone();
        events.on(EventKind::Exit, move |event| {
            if let Event::Exit(options) = event {
                *log.lock() = Some(options);
            }
            async {}
        });

        assert_eq!(
            ExitOptions::default(),
            ExitOptions { status_message: None, exit_code: 0, exit: true, timeout: Duration::from_secs(30) }
        );
        let options = ExitOptions { status_message: Some("Failed".into()), exit_code: 1, ..ExitOptions::default() };
        events.emit(Event::Exit(options.clone()));
        events.wait_for_all_listeners_to_complete().await;
        assert_eq!(seen.lock().clone(), Some(options));
    }

    #[tokio::test(start_paused = true)]
    async fn custom_events_and_stopping_periodic_persist_state() {
        let events = EventManager::new(Duration::from_secs(10));
        let persisted = Arc::new(AtomicUsize::new(0));
        let counter = persisted.clone();
        events.on(EventKind::PersistState, move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            async {}
        });
        let custom = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let log = custom.clone();
        events.on(EventKind::Custom, move |event| {
            if let Event::Custom { name, .. } = event {
                log.lock().push(name);
            }
            async {}
        });

        events.init().await;
        tokio::time::sleep(Duration::from_secs(15)).await;
        assert_eq!(persisted.load(Ordering::SeqCst), 2, "right away and after 10 s");
        events.stop_periodic_persist_state();
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(persisted.load(Ordering::SeqCst), 2);

        events.emit(Event::Custom { name: "cpuInfo".into(), data: serde_json::json!({ "isCpuOverloaded": false }) });
        events.wait_for_all_listeners_to_complete().await;
        assert_eq!(*custom.lock(), ["cpuInfo"]);
        events.close().await;
        assert_eq!(persisted.load(Ordering::SeqCst), 3, "close() still saves once more");
    }
}
