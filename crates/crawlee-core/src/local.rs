//! Synchronization for tasks on one thread: [`Notify`], an async [`OnceCell`] and an async
//! [`Mutex`], with the semantics of tokio's, built on `Cell` and `RefCell` instead of atomics and
//! locks. None of them is `Send`: they are for the tasks of a `LocalRuntime` (`spawn_local`).

use std::cell::{Cell, RefCell, RefMut};
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// The read-modify-write operations of the atomics, for `Cell`s of integers.
pub trait CellExt<T> {
    /// Adds `n` (wrapping, as the atomics do), returning the previous value.
    fn fetch_add(&self, n: T) -> T;
    /// Subtracts `n` (wrapping), returning the previous value.
    fn fetch_sub(&self, n: T) -> T;
    /// Keeps the larger of the value and `n`, returning the previous value.
    fn fetch_max(&self, n: T) -> T;
    /// Keeps the smaller of the value and `n`, returning the previous value.
    fn fetch_min(&self, n: T) -> T;
}

macro_rules! cell_ext {
    ($($int:ty),*) => {$(
        impl CellExt<$int> for Cell<$int> {
            fn fetch_add(&self, n: $int) -> $int {
                self.replace(self.get().wrapping_add(n))
            }

            fn fetch_sub(&self, n: $int) -> $int {
                self.replace(self.get().wrapping_sub(n))
            }

            fn fetch_max(&self, n: $int) -> $int {
                self.replace(self.get().max(n))
            }

            fn fetch_min(&self, n: $int) -> $int {
                self.replace(self.get().min(n))
            }
        }
    )*};
}

cell_ext!(i32, u32, i64, u64, usize);

struct Waiter {
    id: u64,
    waker: Option<Waker>,
    /// Picked by `notify_one`.
    notified: bool,
}

/// Wakes tasks, as `tokio::sync::Notify` does: [`notify_waiters`](Self::notify_waiters) completes
/// every [`Notified`] created before the call, polled or not; [`notify_one`](Self::notify_one)
/// completes the oldest enabled one, or leaves a permit for the next.
#[derive(Default)]
pub struct Notify {
    /// How many times `notify_waiters` was called.
    generation: Cell<u64>,
    permit: Cell<bool>,
    waiters: RefCell<Vec<Waiter>>,
    next_id: Cell<u64>,
}

impl std::fmt::Debug for Notify {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notify").finish_non_exhaustive()
    }
}

impl Notify {
    pub fn new() -> Self {
        Self::default()
    }

    /// A future that completes on the next notification.
    pub fn notified(&self) -> Notified<'_> {
        Notified { notify: self, generation: self.generation.get(), id: None, done: false }
    }

    /// Completes every [`Notified`] that exists now.
    pub fn notify_waiters(&self) {
        self.generation.set(self.generation.get() + 1);
        let wakers: Vec<Waker> =
            self.waiters.borrow_mut().iter_mut().filter_map(|waiter| waiter.waker.take()).collect();
        wakers.into_iter().for_each(Waker::wake);
    }

    /// Completes the oldest enabled [`Notified`], or stores a permit for the next one.
    pub fn notify_one(&self) {
        let waker = {
            let mut waiters = self.waiters.borrow_mut();
            match waiters.iter_mut().find(|waiter| !waiter.notified) {
                Some(waiter) => {
                    waiter.notified = true;
                    Some(waiter.waker.take())
                }
                None => None,
            }
        };
        match waker {
            Some(waker) => waker.into_iter().for_each(Waker::wake),
            None => self.permit.set(true),
        }
    }

    fn register(&self, waker: Option<Waker>) -> u64 {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        self.waiters.borrow_mut().push(Waiter { id, waker, notified: false });
        id
    }

    /// Removes a waiter, returning whether `notify_one` had picked it.
    fn unregister(&self, id: u64) -> bool {
        let mut waiters = self.waiters.borrow_mut();
        match waiters.iter().position(|waiter| waiter.id == id) {
            Some(index) => waiters.remove(index).notified,
            None => false,
        }
    }
}

/// The future of [`Notify::notified`].
pub struct Notified<'a> {
    notify: &'a Notify,
    generation: u64,
    /// Set once registered as a waiter (enabled or polled).
    id: Option<u64>,
    done: bool,
}

impl Notified<'_> {
    /// Registers as a waiter without waiting, so that a `notify_one` from now on completes this
    /// future rather than leaving a permit.
    pub fn enable(self: Pin<&mut Self>) {
        let this = self.get_mut();
        this.check(None);
    }

    /// Whether the future is complete, registering it (with `waker`) when it is not.
    fn check(&mut self, waker: Option<&Waker>) -> bool {
        if self.done {
            return true;
        }
        let notify = self.notify;
        if notify.generation.get() != self.generation {
            return self.finish();
        }
        match self.id {
            None => {
                if notify.permit.replace(false) {
                    self.done = true;
                    return true;
                }
                self.id = Some(notify.register(waker.cloned()));
                false
            }
            Some(id) => {
                let mut waiters = notify.waiters.borrow_mut();
                let Some(waiter) = waiters.iter_mut().find(|waiter| waiter.id == id) else {
                    drop(waiters);
                    return self.finish();
                };
                if waiter.notified {
                    drop(waiters);
                    return self.finish();
                }
                if let Some(waker) = waker {
                    waiter.waker = Some(waker.clone());
                }
                false
            }
        }
    }

    fn finish(&mut self) -> bool {
        if let Some(id) = self.id.take() {
            self.notify.unregister(id);
        }
        self.done = true;
        true
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.get_mut().check(Some(cx.waker())) { Poll::Ready(()) } else { Poll::Pending }
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        // A `notify_one` this future did not get to consume goes to the next waiter.
        if let Some(id) = self.id.take()
            && self.notify.unregister(id)
        {
            self.notify.notify_one();
        }
    }
}

/// A value set once by an async initializer, as `tokio::sync::OnceCell`: concurrent callers wait
/// for the one initializing it, and a failed initialization lets the next caller try.
pub struct OnceCell<T> {
    value: std::cell::OnceCell<T>,
    initializing: Cell<bool>,
    finished: Notify,
}

impl<T> Default for OnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for OnceCell<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OnceCell").field(&self.value.get()).finish()
    }
}

/// Clears the initializing flag even when the initializer is cancelled.
struct Initializing<'a>(&'a Cell<bool>, &'a Notify);

impl Drop for Initializing<'_> {
    fn drop(&mut self) {
        self.0.set(false);
        self.1.notify_waiters();
    }
}

impl<T> OnceCell<T> {
    pub const fn new() -> Self {
        OnceCell {
            value: std::cell::OnceCell::new(),
            initializing: Cell::new(false),
            finished: Notify {
                generation: Cell::new(0),
                permit: Cell::new(false),
                waiters: RefCell::new(Vec::new()),
                next_id: Cell::new(0),
            },
        }
    }

    pub fn get(&self) -> Option<&T> {
        self.value.get()
    }

    pub fn initialized(&self) -> bool {
        self.value.get().is_some()
    }

    /// Sets the value, unless it is set or being initialized.
    pub fn set(&self, value: T) -> Result<(), T> {
        if self.initializing.get() {
            return Err(value);
        }
        self.value.set(value)
    }

    pub async fn get_or_init<F, Fut>(&self, init: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        match self.get_or_try_init(|| async { Ok::<T, std::convert::Infallible>(init().await) }).await {
            Ok(value) => value,
            Err(never) => match never {},
        }
    }

    pub async fn get_or_try_init<E, F, Fut>(&self, init: F) -> Result<&T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut init = Some(init);
        loop {
            if let Some(value) = self.value.get() {
                return Ok(value);
            }
            if self.initializing.get() {
                let finished = self.finished.notified();
                finished.await;
                continue;
            }
            self.initializing.set(true);
            let _initializing = Initializing(&self.initializing, &self.finished);
            let init = init.take().expect("initializes once");
            let value = init().await?;
            // Nothing else sets it while `initializing` is set.
            let _ = self.value.set(value);
            return Ok(self.value.get().expect("just set"));
        }
    }
}

/// An async lock for tasks on one thread, as `tokio::sync::Mutex`: the guard can be held across
/// awaits, and other tasks wait for it.
pub struct Mutex<T> {
    value: RefCell<T>,
    locked: Cell<bool>,
    unlocked: Notify,
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> std::fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mutex").field("locked", &self.locked.get()).finish_non_exhaustive()
    }
}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Mutex { value: RefCell::new(value), locked: Cell::new(false), unlocked: Notify::new() }
    }

    pub async fn lock(&self) -> MutexGuard<'_, T> {
        loop {
            if let Some(guard) = self.try_lock() {
                return guard;
            }
            self.unlocked.notified().await;
        }
    }

    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self.locked.replace(true) {
            return None;
        }
        Some(MutexGuard { value: self.value.borrow_mut(), mutex: self })
    }

    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

pub struct MutexGuard<'a, T> {
    value: RefMut<'a, T>,
    mutex: &'a Mutex<T>,
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        // The waiters run after the guard, and so its borrow, is gone.
        self.mutex.locked.set(false);
        self.mutex.unlocked.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::time::Duration;

    use super::*;

    #[tokio::test(flavor = "local")]
    async fn notify_waiters_completes_the_futures_created_before() {
        let notify = Notify::new();
        let before = notify.notified();
        notify.notify_waiters();
        let after = notify.notified();
        before.await;
        let mut after = std::pin::pin!(after);
        assert!(futures::poll!(after.as_mut()).is_pending());
    }

    #[tokio::test(flavor = "local")]
    async fn notify_one_wakes_an_enabled_waiter_or_leaves_a_permit() {
        let notify = Rc::new(Notify::new());
        notify.notify_one();
        notify.notified().await; // the permit

        let mut enabled = std::pin::pin!(notify.notified());
        enabled.as_mut().enable();
        let later = notify.notified();
        notify.notify_one();
        enabled.await;
        {
            let mut later = std::pin::pin!(later);
            assert!(futures::poll!(later.as_mut()).is_pending(), "the notification went to the enabled one");
        }

        let waiting = {
            let notify = notify.clone();
            tokio::task::spawn_local(async move { notify.notified().await })
        };
        tokio::task::yield_now().await;
        notify.notify_one();
        tokio::time::timeout(Duration::from_secs(1), waiting).await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "local")]
    async fn a_dropped_notification_goes_to_the_next_waiter() {
        let notify = Notify::new();
        let mut first = Box::pin(notify.notified());
        first.as_mut().enable();
        let mut second = std::pin::pin!(notify.notified());
        second.as_mut().enable();
        notify.notify_one();
        drop(first);
        assert!(futures::poll!(second.as_mut()).is_ready());
    }

    #[tokio::test(flavor = "local")]
    async fn once_cell_runs_one_initializer_and_retries_after_a_failure() {
        let cell = Rc::new(OnceCell::<u32>::new());
        let calls = Rc::new(Cell::new(0));
        let failed: Result<&u32, &str> = cell.get_or_try_init(|| async { Err("no") }).await;
        assert!(failed.is_err());

        let tasks: Vec<_> = (0..3)
            .map(|n| {
                let (cell, calls) = (cell.clone(), calls.clone());
                tokio::task::spawn_local(async move {
                    *cell
                        .get_or_init(|| async move {
                            calls.set(calls.get() + 1);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            n
                        })
                        .await
                })
            })
            .collect();
        let values: Vec<u32> = futures::future::join_all(tasks).await.into_iter().map(Result::unwrap).collect();
        assert_eq!(calls.get(), 1);
        assert!(values.iter().all(|value| *value == values[0]));
    }

    #[tokio::test(flavor = "local")]
    async fn mutex_guards_are_held_across_awaits() {
        let mutex = Rc::new(Mutex::new(Vec::new()));
        let tasks: Vec<_> = (0..3)
            .map(|n| {
                let mutex = mutex.clone();
                tokio::task::spawn_local(async move {
                    let mut guard = mutex.lock().await;
                    guard.push(n);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    guard.push(n);
                })
            })
            .collect();
        futures::future::join_all(tasks).await;
        let values = Rc::try_unwrap(mutex).unwrap().into_inner();
        assert!(values.chunks(2).all(|pair| pair[0] == pair[1]), "{values:?}");
        assert_eq!(values.len(), 6);
    }
}
