//! Sessions and the session pool.
//!
//! A session is the unit of identity the crawler rotates when it gets blocked: it carries its
//! proxy and cookies and accumulates an error score.
//!
//! The pool is a [`PersistedState`]: the crawler saves it under `CRAWLEE_SESSION_POOL_STATE_{id}`
//! in the record format of Crawlee for JS (cookies included, in tough-cookie's format), so a
//! resumed crawl keeps its sessions.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use parking_lot::Mutex;
use rand::Rng as _;
use serde_json::{Map, Value, json};
use url::Url;

use crawlee_core::recoverable_state::{BoxError, PersistedState};
use crawlee_http_client::{
    CookieJar, FingerprintBrowser as Browser, FingerprintDevice as Device, FingerprintPlatform as Platform,
    SessionFingerprint,
};

use crate::proxy::{ProxyInfo, ProxySource};

/// Session limits, with the defaults of Crawlee for JS.
#[derive(Clone, Debug)]
pub struct SessionOptions {
    /// Age after which the session is retired.
    pub max_age: Duration,
    /// The session is retired when its error score reaches this value.
    pub max_error_score: f64,
    /// How much a success lowers the error score.
    pub error_score_decrement: f64,
    /// Uses after which the session is retired.
    pub max_usage_count: u32,
    /// The fingerprint of every session. `None`: the pool gives each new session a random one
    /// ([`create_default_session_fingerprint`]).
    pub fingerprint: Option<SessionFingerprint>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        SessionOptions {
            max_age: Duration::from_secs(3000),
            max_error_score: 3.0,
            error_score_decrement: 0.5,
            max_usage_count: 50,
            fingerprint: None,
        }
    }
}

const fn profile(browser: Browser, platform: Platform, device: Device) -> SessionFingerprint {
    SessionFingerprint { browser: Some(browser), platform: Some(platform), device: Some(device) }
}

/// The (browser, platform, device) combinations people actually run (`fingerprint.ts`).
pub(crate) const PROFILES: [SessionFingerprint; 12] = [
    profile(Browser::Chrome, Platform::Windows, Device::Desktop),
    profile(Browser::Firefox, Platform::Windows, Device::Desktop),
    profile(Browser::Edge, Platform::Windows, Device::Desktop),
    profile(Browser::Chrome, Platform::Macos, Device::Desktop),
    profile(Browser::Firefox, Platform::Macos, Device::Desktop),
    profile(Browser::Safari, Platform::Macos, Device::Desktop),
    profile(Browser::Edge, Platform::Macos, Device::Desktop),
    profile(Browser::Chrome, Platform::Linux, Device::Desktop),
    profile(Browser::Firefox, Platform::Linux, Device::Desktop),
    profile(Browser::Chrome, Platform::Android, Device::Mobile),
    profile(Browser::Firefox, Platform::Android, Device::Mobile),
    profile(Browser::Safari, Platform::Ios, Device::Mobile),
];

/// A random realistic fingerprint: the default of new sessions (`createDefaultSessionFingerprint`).
pub fn create_default_session_fingerprint() -> SessionFingerprint {
    PROFILES[rand::rng().random_range(0..PROFILES.len())]
}

#[derive(Debug)]
struct SessionState {
    error_score: f64,
    usage_count: u32,
    retired: bool,
}

/// One identity: an id, a proxy, a cookie jar and a health score.
#[derive(Debug)]
pub struct Session {
    id: String,
    cookie_jar: Arc<CookieJar>,
    proxy_info: Option<ProxyInfo>,
    fingerprint: Option<Arc<SessionFingerprint>>,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    options: SessionOptions,
    user_data: Mutex<Map<String, Value>>,
    state: Mutex<SessionState>,
}

fn iso(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn proxy_info_json(proxy: &ProxyInfo) -> Value {
    let mut json = json!({
        "url": proxy.url.as_str(),
        "hostname": proxy.hostname,
        "port": proxy.port,
        "username": proxy.username,
        "password": proxy.password,
    });
    if proxy.ignore_tls_errors {
        json["ignoreTlsErrors"] = true.into();
    }
    json
}

impl Session {
    pub fn new(id: String, proxy_info: Option<ProxyInfo>, options: SessionOptions) -> Self {
        let created_at = Utc::now();
        let expires_at = created_at + chrono::Duration::from_std(options.max_age).unwrap_or(chrono::Duration::MAX);
        Session {
            id,
            cookie_jar: Arc::new(CookieJar::new()),
            proxy_info,
            fingerprint: options.fingerprint.map(Arc::new),
            created_at,
            expires_at,
            options,
            user_data: Mutex::new(Map::new()),
            state: Mutex::new(SessionState { error_score: 0.0, usage_count: 0, retired: false }),
        }
    }

    /// The session as Crawlee for JS saves it (`Session.getState()`).
    pub fn to_state(&self) -> Value {
        let state = self.state.lock();
        let mut record = json!({
            "id": self.id,
            "cookieJar": self.cookie_jar.to_json(),
            "proxyInfo": self.proxy_info.as_ref().map(proxy_info_json),
            "userData": *self.user_data.lock(),
            "maxErrorScore": self.options.max_error_score,
            "errorScoreDecrement": self.options.error_score_decrement,
            "expiresAt": iso(self.expires_at),
            "createdAt": iso(self.created_at),
            "usageCount": state.usage_count,
            "maxUsageCount": self.options.max_usage_count,
            "errorScore": state.error_score,
            "retired": state.retired,
        });
        // Left out without one, as `JSON.stringify` leaves out `undefined`.
        if let Some(fingerprint) = &self.fingerprint {
            record["fingerprint"] = fingerprint.to_json();
        }
        record
    }

    /// A session from [`to_state`](Self::to_state) (or a session saved by Crawlee for JS).
    /// Limits missing from the record, and the fingerprint, come from `options`.
    pub fn from_state(record: &Value, options: &SessionOptions) -> Result<Self, BoxError> {
        let time = |field: &str| -> Result<DateTime<Utc>, BoxError> {
            let text = record[field].as_str().ok_or_else(|| format!("the session has no `{field}`"))?;
            Ok(DateTime::parse_from_rfc3339(text)?.with_timezone(&Utc))
        };
        let id = record["id"].as_str().ok_or("the session has no `id`")?.to_owned();
        let proxy_info = record["proxyInfo"]["url"].as_str().map(Url::parse).transpose()?.map(|url| ProxyInfo {
            ignore_tls_errors: record["proxyInfo"]["ignoreTlsErrors"].as_bool().unwrap_or(false),
            ..ProxyInfo::from_url(url)
        });
        let mut options = options.clone();
        if let Some(value) = record["maxErrorScore"].as_f64() {
            options.max_error_score = value;
        }
        if let Some(value) = record["errorScoreDecrement"].as_f64() {
            options.error_score_decrement = value;
        }
        if let Some(value) = record["maxUsageCount"].as_u64() {
            options.max_usage_count = u32::try_from(value).unwrap_or(u32::MAX);
        }
        let (created_at, expires_at) = (time("createdAt")?, time("expiresAt")?);
        if let Ok(age) = (expires_at - created_at).to_std() {
            options.max_age = age;
        }
        Ok(Session {
            id,
            cookie_jar: Arc::new(CookieJar::from_json(&record["cookieJar"])),
            proxy_info,
            fingerprint: SessionFingerprint::from_json(&record["fingerprint"]).or(options.fingerprint).map(Arc::new),
            created_at,
            expires_at,
            options,
            user_data: Mutex::new(record["userData"].as_object().cloned().unwrap_or_default()),
            state: Mutex::new(SessionState {
                error_score: record["errorScore"].as_f64().unwrap_or(0.0),
                usage_count: record["usageCount"].as_u64().map_or(0, |n| u32::try_from(n).unwrap_or(u32::MAX)),
                retired: record["retired"].as_bool().unwrap_or(false),
            }),
        })
    }

    /// Custom data kept with the session (and saved with it).
    pub fn user_data(&self) -> parking_lot::MutexGuard<'_, Map<String, Value>> {
        self.user_data.lock()
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn cookie_jar(&self) -> &Arc<CookieJar> {
        &self.cookie_jar
    }

    pub fn proxy_info(&self) -> Option<&ProxyInfo> {
        self.proxy_info.as_ref()
    }

    /// The browser-like profile the session impersonates.
    pub fn fingerprint(&self) -> Option<&SessionFingerprint> {
        self.fingerprint.as_deref()
    }

    /// The fingerprint as HTTP clients receive it: clients that derive state from it (such as the
    /// impit browser version) keep that state per fingerprint, so per session.
    pub fn fingerprint_handle(&self) -> Option<&Arc<SessionFingerprint>> {
        self.fingerprint.as_ref()
    }

    pub fn error_score(&self) -> f64 {
        self.state.lock().error_score
    }

    pub fn usage_count(&self) -> u32 {
        self.state.lock().usage_count
    }

    pub fn is_retired(&self) -> bool {
        self.state.lock().retired
    }

    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }

    pub fn is_blocked(&self) -> bool {
        self.state.lock().error_score >= self.options.max_error_score
    }

    pub fn is_max_usage_reached(&self) -> bool {
        self.state.lock().usage_count >= self.options.max_usage_count
    }

    /// Whether the pool may hand this session out.
    pub fn is_usable(&self) -> bool {
        self.usable(&self.state.lock())
    }

    fn usable(&self, state: &SessionState) -> bool {
        !state.retired
            && state.error_score < self.options.max_error_score
            && state.usage_count < self.options.max_usage_count
            && Utc::now() < self.expires_at
    }

    /// The request succeeded: count the use and lower the error score. Retires the session when
    /// it is no longer usable (used up or expired). Does nothing to a retired session.
    pub fn mark_good(&self) {
        let mut state = self.state.lock();
        if state.retired {
            return;
        }
        state.usage_count += 1;
        if state.error_score > 0.0 {
            state.error_score -= self.options.error_score_decrement;
        }
        self.maybe_self_retire(&mut state);
    }

    /// The request failed: count the use and raise the error score. Retires the session when it
    /// is no longer usable.
    pub fn mark_bad(&self) {
        let mut state = self.state.lock();
        state.error_score += 1.0;
        state.usage_count += 1;
        self.maybe_self_retire(&mut state);
    }

    /// Takes the session out of rotation for good: adds the maximum error score and counts a use.
    /// Retiring a session that is already retired and blocked does nothing.
    pub fn retire(&self) {
        let mut state = self.state.lock();
        if state.retired && state.error_score >= self.options.max_error_score {
            return;
        }
        state.error_score += self.options.max_error_score;
        state.usage_count += 1;
        state.retired = true;
    }

    /// Makes an unusable session's state terminal. Unlike `retire`, the error score is left alone,
    /// so that `is_blocked` keeps telling a burned identity from one that ran out of uses.
    fn maybe_self_retire(&self, state: &mut SessionState) {
        if !self.usable(state) {
            state.retired = true;
        }
    }
}

/// How the pool picks the session for the next request (`sessionReuseStrategy`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SessionReuseStrategy {
    /// Fills the pool up to its maximum size, then picks a random usable session.
    #[default]
    Random,
    /// Fills the pool up to its maximum size, then cycles through the sessions in order.
    RoundRobin,
    /// Reuses the first usable session until it is retired, then moves on to the next one.
    UseUntilFailure,
}

/// What a [`CreateSessionFunction`] makes a session from (`{ sessionOptions }` in JS).
#[derive(Clone, Debug)]
pub struct CreateSessionOptions {
    /// A new id, `session_` and 10 random characters.
    pub id: String,
    /// The proxy the pool's proxy source gave for the id.
    pub proxy_info: Option<ProxyInfo>,
    /// The pool's session options, with the fingerprint for this session set.
    pub session_options: SessionOptions,
}

/// Makes the pool's new sessions instead of [`Session::new`] (`createSessionFunction`). Sessions
/// restored from the saved state of the pool are recreated from their record instead.
pub type CreateSessionFunction = Arc<dyn Fn(CreateSessionOptions) -> Session + Send + Sync>;

/// Pool options, with the defaults of Crawlee for JS.
#[derive(Clone)]
pub struct SessionPoolOptions {
    pub max_pool_size: usize,
    pub session_options: SessionOptions,
    pub session_reuse_strategy: SessionReuseStrategy,
    pub create_session_function: Option<CreateSessionFunction>,
}

impl Default for SessionPoolOptions {
    fn default() -> Self {
        SessionPoolOptions {
            max_pool_size: 1000,
            session_options: SessionOptions::default(),
            session_reuse_strategy: SessionReuseStrategy::default(),
            create_session_function: None,
        }
    }
}

impl std::fmt::Debug for SessionPoolOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionPoolOptions")
            .field("max_pool_size", &self.max_pool_size)
            .field("session_options", &self.session_options)
            .field("session_reuse_strategy", &self.session_reuse_strategy)
            .field("create_session_function", &self.create_session_function.is_some())
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionPoolError {
    #[error("Cannot add session with id '{0}' as it already exists in the pool")]
    AlreadyExists(String),
}

#[derive(Default)]
struct PoolState {
    sessions: Vec<Arc<Session>>,
    round_robin_index: usize,
}

impl PoolState {
    fn remove_retired_sessions(&mut self) {
        self.sessions.retain(|session| {
            let usable = session.is_usable();
            if !usable {
                tracing::debug!("Removed Session - {}", session.id());
            }
            usable
        });
    }
}

/// Hands out sessions: new ones until the pool is full, then usable ones picked by the
/// [`SessionReuseStrategy`], replacing sessions that are no longer usable.
pub struct SessionPool {
    id: String,
    options: SessionPoolOptions,
    proxies: Option<Arc<dyn ProxySource>>,
    state: Mutex<PoolState>,
}

impl std::fmt::Debug for SessionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionPool").field("options", &self.options).finish_non_exhaustive()
    }
}

impl SessionPool {
    /// A pool with the next process-wide id: `"0"` for the first pool, as in JS.
    pub fn new(options: SessionPoolOptions, proxies: Option<Arc<dyn ProxySource>>) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        Self::with_id(NEXT_ID.fetch_add(1, Ordering::Relaxed).to_string(), options, proxies)
    }

    pub fn with_id(id: impl Into<String>, options: SessionPoolOptions, proxies: Option<Arc<dyn ProxySource>>) -> Self {
        SessionPool { id: id.into(), options, proxies, state: Mutex::default() }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// The key the pool is saved under.
    pub fn persist_state_key(&self) -> String {
        format!("CRAWLEE_SESSION_POOL_STATE_{}", self.id)
    }

    /// The pool's session options with a fingerprint: the pool-wide one, else a random default.
    fn session_options(&self) -> SessionOptions {
        let mut options = self.options.session_options.clone();
        options.fingerprint = options.fingerprint.or_else(|| Some(create_default_session_fingerprint()));
        options
    }

    fn create_session(&self) -> Arc<Session> {
        let id = format!("session_{}", crawlee_core::request::crypto_random_object_id(10));
        let proxy_info = self.proxies.as_ref().and_then(|proxies| proxies.new_proxy_info(&id));
        let session_options = self.session_options();
        let session = match &self.options.create_session_function {
            Some(create) => create(CreateSessionOptions { id, proxy_info, session_options }),
            None => Session::new(id, proxy_info, session_options),
        };
        tracing::debug!("Created new Session - {}", session.id());
        Arc::new(session)
    }

    /// A new session, with a new proxy, that the pool does not keep: for crawlers without a
    /// session pool.
    pub fn detached_session(&self) -> Arc<Session> {
        self.create_session()
    }

    fn has_space_for_session(&self, state: &PoolState) -> bool {
        state.sessions.len() < self.options.max_pool_size.max(1)
    }

    /// The session the strategy reuses, or `None` when a new one should be created.
    fn pick_session(&self, state: &mut PoolState) -> Option<Arc<Session>> {
        let strategy = self.options.session_reuse_strategy;
        if strategy == SessionReuseStrategy::UseUntilFailure {
            return state.sessions.iter().find(|session| session.is_usable()).cloned();
        }
        if self.has_space_for_session(state) {
            return None;
        }
        let index = match strategy {
            SessionReuseStrategy::RoundRobin => {
                let index = state.round_robin_index % state.sessions.len();
                state.round_robin_index = index + 1;
                index
            }
            _ => rand::rng().random_range(0..state.sessions.len()),
        };
        let picked = &state.sessions[index];
        picked.is_usable().then(|| picked.clone())
    }

    /// A session for the next request.
    pub fn get_session(&self) -> Arc<Session> {
        let mut state = self.state.lock();
        if let Some(session) = self.pick_session(&mut state) {
            return session;
        }
        if !self.has_space_for_session(&state) {
            state.remove_retired_sessions();
        }
        let session = self.create_session();
        state.sessions.push(session.clone());
        session
    }

    /// Adds a session to the pool, also beyond the maximum pool size. A full pool first drops its
    /// sessions that are no longer usable.
    ///
    /// A session with the same id is replaced when it is no longer usable (retired, expired or used
    /// up), otherwise [`SessionPoolError::AlreadyExists`] is returned.
    pub fn add_session(&self, session: Session) -> Result<Arc<Session>, SessionPoolError> {
        let mut state = self.state.lock();
        if let Some(index) = state.sessions.iter().position(|existing| existing.id() == session.id()) {
            if state.sessions[index].is_usable() {
                return Err(SessionPoolError::AlreadyExists(session.id));
            }
            let removed = state.sessions.remove(index);
            tracing::debug!("Removed Session - {}", removed.id());
        }
        if !self.has_space_for_session(&state) {
            state.remove_retired_sessions();
        }
        tracing::debug!("Adding new Session - {}", session.id());
        let session = Arc::new(session);
        state.sessions.push(session.clone());
        Ok(session)
    }

    /// The usable session with this id, if any (`getSession(sessionId)` in JS). The crawler fails
    /// a request bound to a session that is missing here with `MissingSessionError`.
    pub fn get_session_by_id(&self, id: &str) -> Option<Arc<Session>> {
        self.state.lock().sessions.iter().find(|session| session.id() == id && session.is_usable()).cloned()
    }

    pub fn usable_count(&self) -> usize {
        self.state.lock().sessions.iter().filter(|session| session.is_usable()).count()
    }

    pub fn retired_count(&self) -> usize {
        self.state.lock().sessions.iter().filter(|session| !session.is_usable()).count()
    }
}

impl PersistedState for SessionPool {
    fn to_record(&self) -> Value {
        let state = self.state.lock();
        let sessions = &state.sessions;
        let usable = sessions.iter().filter(|session| session.is_usable()).count();
        json!({
            "usableSessionsCount": usable,
            "retiredSessionsCount": sessions.len() - usable,
            "sessions": sessions.iter().map(|session| session.to_state()).collect::<Vec<_>>(),
        })
    }

    /// Recreates the saved sessions, keeping only the usable ones.
    fn restore(&self, record: Value) -> Result<(), BoxError> {
        let saved = record["sessions"].as_array().ok_or("the record has no `sessions`")?;
        // A session saved without a fingerprint gets the default one, as a new session would.
        let mut sessions = saved
            .iter()
            .map(|state| Session::from_state(state, &self.session_options()))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(proxies) = &self.proxies {
            // As `createSessionFunction` does in JS: the proxy source decides what a restored proxy becomes.
            for session in &mut sessions {
                session.proxy_info = match session.proxy_info.take() {
                    Some(restored) => proxies.restored_proxy_info(&session.id, restored),
                    None => proxies.new_proxy_info(&session.id),
                };
            }
        }
        let usable: Vec<Arc<Session>> =
            sessions.into_iter().filter(|session| session.is_usable()).map(Arc::new).collect();
        tracing::debug!("{} active sessions loaded from the key-value store", usable.len());
        let mut state = self.state.lock();
        state.sessions = usable;
        state.round_robin_index = 0;
        Ok(())
    }

    fn reset(&self) {
        *self.state.lock() = PoolState::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoring_and_retirement() {
        let session = Session::new("s".into(), None, SessionOptions::default());
        session.mark_bad();
        session.mark_bad();
        assert!(session.is_usable());
        session.mark_good();
        assert_eq!(session.error_score(), 1.5);
        session.mark_bad();
        session.mark_bad();
        assert!(session.is_blocked());
        assert!(!session.is_usable());

        let other = Session::new("o".into(), None, SessionOptions::default());
        other.retire();
        assert!(!other.is_usable());
    }

    #[test]
    fn pool_fills_then_reuses_and_replaces() {
        let pool = SessionPool::new(SessionPoolOptions { max_pool_size: 2, ..Default::default() }, None);
        let a = pool.get_session();
        let b = pool.get_session();
        assert_ne!(a.id(), b.id());
        for _ in 0..10 {
            let s = pool.get_session();
            assert!(s.id() == a.id() || s.id() == b.id());
        }
        a.retire();
        b.retire();
        let c = pool.get_session();
        assert!(c.id() != a.id() && c.id() != b.id());
        assert_eq!(pool.get_session_by_id(c.id()).unwrap().id(), c.id());
        assert!(pool.get_session_by_id(a.id()).is_none());
    }

    #[test]
    fn add_session_replaces_an_unusable_session_with_the_same_id() {
        let pool = SessionPool::new(SessionPoolOptions { max_pool_size: 1, ..Default::default() }, None);
        let pinned = pool.add_session(Session::new("pinned".into(), None, SessionOptions::default())).unwrap();
        let duplicate = Session::new("pinned".into(), None, SessionOptions::default());
        assert!(matches!(pool.add_session(duplicate), Err(SessionPoolError::AlreadyExists(id)) if id == "pinned"));

        pinned.retire();
        let replacement = pool.add_session(Session::new("pinned".into(), None, SessionOptions::default())).unwrap();
        assert!(!Arc::ptr_eq(&pinned, &replacement));
        assert!(Arc::ptr_eq(&pool.get_session_by_id("pinned").unwrap(), &replacement));
        assert_eq!(pool.retired_count(), 0, "the retired session is gone");
    }

    /// A full pool makes room by dropping its retired sessions first (`removeRetiredSessions`).
    #[test]
    fn add_session_to_a_full_pool_removes_retired_sessions() {
        let pool = SessionPool::new(SessionPoolOptions { max_pool_size: 2, ..Default::default() }, None);
        pool.get_session().retire();
        let kept = pool.get_session();
        pool.add_session(Session::new("added".into(), None, SessionOptions::default())).unwrap();
        assert_eq!(pool.retired_count(), 0);
        assert_eq!(pool.usable_count(), 2);
        assert!(pool.get_session_by_id(kept.id()).is_some());
    }

    #[test]
    fn retire_adds_the_max_error_score_once() {
        let session = Session::new("s".into(), None, SessionOptions::default());
        session.mark_bad();
        session.retire();
        assert_eq!((session.error_score(), session.usage_count()), (4.0, 2));
        session.retire();
        assert_eq!((session.error_score(), session.usage_count()), (4.0, 2), "a second retire does nothing");
        session.mark_good();
        assert_eq!((session.error_score(), session.usage_count()), (4.0, 2), "a retired session is not revived");

        // Used up, not blocked: retiring it still marks it as blocked (session.ts `retire`).
        let used_up =
            Session::new("u".into(), None, SessionOptions { max_usage_count: 1, ..SessionOptions::default() });
        used_up.mark_good();
        used_up.retire();
        assert_eq!((used_up.error_score(), used_up.usage_count()), (3.0, 2));
    }

    #[test]
    fn marking_retires_a_session_that_is_no_longer_usable() {
        // Self-retirement leaves the score alone, so that blocked and used up stay apart.
        let blocked = Session::new("b".into(), None, SessionOptions::default());
        for _ in 0..3 {
            blocked.mark_bad();
        }
        assert!(blocked.is_retired());
        assert_eq!((blocked.error_score(), blocked.usage_count()), (3.0, 3));

        let used_up =
            Session::new("u".into(), None, SessionOptions { max_usage_count: 1, ..SessionOptions::default() });
        used_up.mark_good();
        assert!(used_up.is_retired());
        assert!(!used_up.is_blocked());

        // The decrement applies whenever the score is positive, without a floor at 0 (session.ts).
        let record = Session::new("d".into(), None, SessionOptions::default()).to_state();
        let mut record = record;
        record["errorScore"] = json!(0.2);
        let healed = Session::from_state(&record, &SessionOptions::default()).unwrap();
        healed.mark_good();
        assert!((healed.error_score() - -0.3).abs() < 1e-9);
    }

    #[test]
    fn round_robin_cycles_through_the_full_pool_in_order() {
        let pool = SessionPool::new(
            SessionPoolOptions {
                max_pool_size: 3,
                session_reuse_strategy: SessionReuseStrategy::RoundRobin,
                ..Default::default()
            },
            None,
        );
        let created: Vec<String> = (0..3).map(|_| pool.get_session().id().to_owned()).collect();
        let reused: Vec<String> = (0..6).map(|_| pool.get_session().id().to_owned()).collect();
        assert_eq!(reused, [created.clone(), created].concat());
    }

    #[test]
    fn use_until_failure_keeps_one_session_until_it_is_retired() {
        let pool = SessionPool::new(
            SessionPoolOptions { session_reuse_strategy: SessionReuseStrategy::UseUntilFailure, ..Default::default() },
            None,
        );
        let first = pool.get_session();
        for _ in 0..5 {
            assert!(Arc::ptr_eq(&pool.get_session(), &first));
        }
        first.retire();
        let second = pool.get_session();
        assert_ne!(second.id(), first.id());
        assert!(Arc::ptr_eq(&pool.get_session(), &second));
    }

    #[test]
    fn sessions_get_a_default_fingerprint_that_is_saved() {
        let pool = SessionPool::with_id("0", SessionPoolOptions::default(), None);
        let session = pool.get_session();
        let fingerprint = *session.fingerprint().expect("the pool gives every session a fingerprint");
        assert!(PROFILES.contains(&fingerprint));

        let restored = SessionPool::with_id("0", SessionPoolOptions::default(), None);
        restored.restore(pool.to_record()).unwrap();
        assert_eq!(restored.get_session_by_id(session.id()).unwrap().fingerprint(), Some(&fingerprint));

        // A pool-wide fingerprint replaces the random default.
        let fixed = SessionFingerprint { browser: Some(Browser::Firefox), ..Default::default() };
        let pool = SessionPool::new(
            SessionPoolOptions {
                session_options: SessionOptions { fingerprint: Some(fixed), ..Default::default() },
                ..Default::default()
            },
            None,
        );
        assert_eq!(pool.get_session().fingerprint(), Some(&fixed));
        assert!(Session::new("plain".into(), None, SessionOptions::default()).to_state().get("fingerprint").is_none());
    }

    #[test]
    fn create_session_function_makes_the_sessions() {
        let pool = SessionPool::new(
            SessionPoolOptions {
                create_session_function: Some(Arc::new(|options: CreateSessionOptions| {
                    let session =
                        Session::new(format!("custom_{}", options.id), options.proxy_info, options.session_options);
                    session.user_data().insert("custom".into(), json!(true));
                    session
                })),
                ..Default::default()
            },
            None,
        );
        let session = pool.get_session();
        assert!(session.id().starts_with("custom_session_"));
        assert_eq!(session.user_data()["custom"], true);
        assert!(session.fingerprint().is_some(), "the options carry the default fingerprint");
    }

    #[test]
    fn a_mitm_proxy_is_saved_with_the_session() {
        let proxy = ProxyInfo { ignore_tls_errors: true, ..ProxyInfo::from_url(Url::parse("http://mitm:1").unwrap()) };
        let record = Session::new("s".into(), Some(proxy), SessionOptions::default()).to_state();
        assert_eq!(record["proxyInfo"]["ignoreTlsErrors"], true);
        let restored = Session::from_state(&record, &SessionOptions::default()).unwrap();
        assert!(restored.proxy_info().unwrap().ignore_tls_errors);
    }

    #[test]
    fn restored_sessions_get_their_proxy_refreshed() {
        struct Refresh;
        impl ProxySource for Refresh {
            fn new_proxy_info(&self, _: &str) -> Option<ProxyInfo> {
                Some(ProxyInfo::from_url(Url::parse("http://fresh:1").unwrap()))
            }
            fn restored_proxy_info(&self, _: &str, restored: ProxyInfo) -> Option<ProxyInfo> {
                Some(ProxyInfo::from_url(Url::parse(&restored.url.as_str().replace("old", "new")).unwrap()))
            }
        }
        let pool = SessionPool::with_id("0", SessionPoolOptions::default(), None);
        pool.add_session(Session::new(
            "s".into(),
            Some(ProxyInfo::from_url(Url::parse("http://old:1").unwrap())),
            SessionOptions::default(),
        ))
        .unwrap();
        let record = pool.to_record();

        let restored = SessionPool::with_id("0", SessionPoolOptions::default(), Some(Arc::new(Refresh)));
        restored.restore(record).unwrap();
        assert_eq!(restored.get_session_by_id("s").unwrap().proxy_info().unwrap().hostname, "new");
    }

    #[test]
    fn pool_record_round_trips_usable_sessions() {
        let pool = SessionPool::with_id("0", SessionPoolOptions { max_pool_size: 2, ..Default::default() }, None);
        let kept = pool.get_session();
        kept.mark_bad();
        kept.user_data().insert("token".into(), json!("t"));
        kept.cookie_jar().set_cookie("sid=1; Path=/", &Url::parse("https://a.test/").unwrap());
        pool.get_session().retire();

        let record = pool.to_record();
        assert_eq!(record["usableSessionsCount"], 1);
        assert_eq!(record["retiredSessionsCount"], 1);
        let saved = &record["sessions"][0];
        for field in ["id", "cookieJar", "proxyInfo", "userData", "fingerprint", "maxErrorScore", "expiresAt"] {
            assert!(saved.get(field).is_some(), "{field}");
        }

        let restored = SessionPool::with_id("0", SessionPoolOptions::default(), None);
        restored.restore(record).unwrap();
        assert_eq!(restored.usable_count(), 1, "retired sessions are dropped");
        let session = restored.get_session_by_id(kept.id()).unwrap();
        assert_eq!(session.error_score(), 1.0);
        assert_eq!(session.usage_count(), 1);
        assert_eq!(session.user_data()["token"], "t");
        assert_eq!(
            session.cookie_jar().cookie_header(&Url::parse("https://a.test/x").unwrap()).as_deref(),
            Some("sid=1")
        );
    }
}
