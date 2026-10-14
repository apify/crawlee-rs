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

use crate::local::CellExt as _;
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use crate::local::Notify;
use std::cell::RefCell;
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
    Exit,
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
            Event::Exit => EventKind::Exit,
            Event::StatusMessage(_) => EventKind::StatusMessage,
            Event::Custom { .. } => EventKind::Custom,
        }
    }
}

type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;
type Listener = Rc<dyn Fn(Event) -> BoxFuture>;

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
    listeners: RefCell<Vec<Registered>>,
    next_id: Cell<u64>,
    /// Listener invocations that have not finished yet.
    in_flight: Cell<usize>,
    idle: Notify,
    intervals: RefCell<Option<JoinHandle<()>>>,
    /// Set by `stop_periodic_persist_state`: periodic `PersistState` events stop.
    persist_state_stopped: Cell<bool>,
    persist_state_interval: Duration,
    system_info: Option<SystemInfoOptions>,
}

/// Emits events to async listeners. Cloning is cheap and clones share the manager.
#[derive(Clone)]
pub struct EventManager {
    inner: Rc<Inner>,
}

impl std::fmt::Debug for EventManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventManager").field("initialized", &self.is_initialized()).finish_non_exhaustive()
    }
}

impl EventManager {
    fn with_options(persist_state_interval: Duration, system_info: Option<SystemInfoOptions>) -> Self {
        EventManager {
            inner: Rc::new(Inner {
                listeners: RefCell::new(Vec::new()),
                next_id: Cell::new(0),
                in_flight: Cell::new(0),
                idle: Notify::new(),
                intervals: RefCell::new(None),
                persist_state_stopped: Cell::new(false),
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
        F: Fn(Event) -> Fut + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let id = ListenerId(self.inner.next_id.fetch_add(1));
        let listener: Listener = Rc::new(move |event| Box::pin(listener(event)));
        self.inner.listeners.borrow_mut().push(Registered { id, kind, listener });
        id
    }

    pub fn off(&self, id: ListenerId) {
        self.inner.listeners.borrow_mut().retain(|registered| registered.id != id);
    }

    pub fn listener_count(&self, kind: EventKind) -> usize {
        self.inner.listeners.borrow_mut().iter().filter(|registered| registered.kind == kind).count()
    }

    /// Runs every listener of the event's kind. Returns right away; the listeners run as tasks.
    /// Outside a tokio runtime, the event is dropped.
    pub fn emit(&self, event: Event) {
        let kind = event.kind();
        let listeners: Vec<Listener> = self
            .inner
            .listeners
            .borrow_mut()
            .iter()
            .filter(|registered| registered.kind == kind)
            .map(|registered| registered.listener.clone())
            .collect();
        if listeners.is_empty() {
            return;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            tracing::debug!(?kind, "No tokio runtime; the event is not delivered.");
            return;
        }
        for listener in listeners {
            self.inner.in_flight.fetch_add(1);
            let inner = self.inner.clone();
            let future = listener(event.clone());
            tokio::task::spawn_local(async move {
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
            if self.inner.in_flight.get() == 0 {
                return;
            }
            idle.await;
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.inner.intervals.borrow_mut().is_some()
    }

    /// Starts the periodic events. Does nothing when already initialized.
    pub async fn init(&self) {
        let mut intervals = self.inner.intervals.borrow_mut();
        if intervals.is_some() {
            return;
        }
        let manager = self.clone();
        *intervals = Some(tokio::task::spawn_local(async move { manager.emit_periodically().await }));
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
                    if !self.inner.persist_state_stopped.get() {
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
        self.inner.persist_state_stopped.set(true);
    }

    /// Stops the periodic events, emits a final `PersistState` and waits for all listeners.
    /// Does nothing when not initialized.
    pub async fn close(&self) {
        let Some(intervals) = self.inner.intervals.borrow_mut().take() else {
            return;
        };
        intervals.abort();
        self.emit(Event::PersistState { is_migrating: false });
        self.wait_for_all_listeners_to_complete().await;
    }
}

struct InFlightGuard(Rc<Inner>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn periodic_events_and_close() {
        let configuration = Configuration {
            persist_state_interval: Duration::from_secs(60),
            system_info_interval: Duration::from_secs(1),
            ..Configuration::default()
        };
        let events = EventManager::local(&configuration);
        let persisted = Rc::new(Cell::new(0));
        let infos = Rc::new(Cell::new(0));
        let counter = persisted.clone();
        events.on(EventKind::PersistState, move |_| {
            let counter = counter.clone();
            async move {
                // A slow listener: close() has to wait for it.
                tokio::time::sleep(Duration::from_millis(500)).await;
                counter.fetch_add(1);
            }
        });
        let counter = infos.clone();
        let info_listener = events.on(EventKind::SystemInfo, move |event| {
            assert!(matches!(event, Event::SystemInfo(_)));
            counter.fetch_add(1);
            async {}
        });

        events.init().await;
        events.init().await;
        tokio::time::sleep(Duration::from_millis(2500)).await;
        // Emitted right away, then every second.
        assert_eq!(infos.get(), 3);
        assert_eq!(persisted.get(), 1);

        events.off(info_listener);
        assert_eq!(events.listener_count(EventKind::SystemInfo), 0);
        events.close().await;
        assert_eq!(persisted.get(), 2, "close() waits for the final PersistState");
        assert!(!events.is_initialized());
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn custom_events_and_stopping_periodic_persist_state() {
        let events = EventManager::new(Duration::from_secs(10));
        let persisted = Rc::new(Cell::new(0));
        let counter = persisted.clone();
        events.on(EventKind::PersistState, move |_| {
            counter.fetch_add(1);
            async {}
        });
        let custom = Rc::new(std::cell::RefCell::new(Vec::new()));
        let log = custom.clone();
        events.on(EventKind::Custom, move |event| {
            if let Event::Custom { name, .. } = event {
                log.borrow_mut().push(name);
            }
            async {}
        });

        events.init().await;
        tokio::time::sleep(Duration::from_secs(15)).await;
        assert_eq!(persisted.get(), 2, "right away and after 10 s");
        events.stop_periodic_persist_state();
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(persisted.get(), 2);

        events.emit(Event::Custom { name: "cpuInfo".into(), data: serde_json::json!({ "isCpuOverloaded": false }) });
        events.wait_for_all_listeners_to_complete().await;
        assert_eq!(*custom.borrow_mut(), ["cpuInfo"]);
        events.close().await;
        assert_eq!(persisted.get(), 3, "close() still saves once more");
    }
}
