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
use crawlee_http_client::CookieJar;

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
}

impl Default for SessionOptions {
    fn default() -> Self {
        SessionOptions {
            max_age: Duration::from_secs(3000),
            max_error_score: 3.0,
            error_score_decrement: 0.5,
            max_usage_count: 50,
        }
    }
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
    json!({
        "url": proxy.url.as_str(),
        "hostname": proxy.hostname,
        "port": proxy.port,
        "username": proxy.username,
        "password": proxy.password,
    })
}

impl Session {
    pub fn new(id: String, proxy_info: Option<ProxyInfo>, options: SessionOptions) -> Self {
        let created_at = Utc::now();
        let expires_at = created_at + chrono::Duration::from_std(options.max_age).unwrap_or(chrono::Duration::MAX);
        Session {
            id,
            cookie_jar: Arc::new(CookieJar::new()),
            proxy_info,
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
        json!({
            "id": self.id,
            "cookieJar": self.cookie_jar.to_json(),
            "proxyInfo": self.proxy_info.as_ref().map(proxy_info_json),
            "userData": *self.user_data.lock(),
            "fingerprint": null,
            "maxErrorScore": self.options.max_error_score,
            "errorScoreDecrement": self.options.error_score_decrement,
            "expiresAt": iso(self.expires_at),
            "createdAt": iso(self.created_at),
            "usageCount": state.usage_count,
            "maxUsageCount": self.options.max_usage_count,
            "errorScore": state.error_score,
            "retired": state.retired,
        })
    }

    /// A session from [`to_state`](Self::to_state) (or a session saved by Crawlee for JS).
    /// Limits missing from the record come from `options`.
    pub fn from_state(record: &Value, options: &SessionOptions) -> Result<Self, BoxError> {
        let time = |field: &str| -> Result<DateTime<Utc>, BoxError> {
            let text = record[field].as_str().ok_or_else(|| format!("the session has no `{field}`"))?;
            Ok(DateTime::parse_from_rfc3339(text)?.with_timezone(&Utc))
        };
        let id = record["id"].as_str().ok_or("the session has no `id`")?.to_owned();
        let proxy_info = record["proxyInfo"]["url"].as_str().map(Url::parse).transpose()?.map(ProxyInfo::from_url);
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
        let state = self.state.lock();
        !state.retired
            && state.error_score < self.options.max_error_score
            && state.usage_count < self.options.max_usage_count
            && Utc::now() < self.expires_at
    }

    /// The request succeeded: count the use and lower the error score.
    pub fn mark_good(&self) {
        let mut state = self.state.lock();
        state.usage_count += 1;
        state.error_score = (state.error_score - self.options.error_score_decrement).max(0.0);
    }

    /// The request failed: count the use and raise the error score.
    pub fn mark_bad(&self) {
        let mut state = self.state.lock();
        state.usage_count += 1;
        state.error_score += 1.0;
    }

    /// Takes the session out of rotation for good.
    pub fn retire(&self) {
        let mut state = self.state.lock();
        state.retired = true;
        state.usage_count += 1;
    }
}

/// Pool options, with the defaults of Crawlee for JS.
#[derive(Clone, Debug)]
pub struct SessionPoolOptions {
    pub max_pool_size: usize,
    pub session_options: SessionOptions,
}

impl Default for SessionPoolOptions {
    fn default() -> Self {
        SessionPoolOptions { max_pool_size: 1000, session_options: SessionOptions::default() }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionPoolError {
    #[error("Cannot add session with id '{0}' as it already exists in the pool")]
    AlreadyExists(String),
}

/// Hands out sessions: new ones until the pool is full, then random usable ones, replacing
/// sessions that are no longer usable (the `random` strategy of Crawlee for JS).
pub struct SessionPool {
    id: String,
    options: SessionPoolOptions,
    proxies: Option<Arc<dyn ProxySource>>,
    sessions: Mutex<Vec<Arc<Session>>>,
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
        SessionPool { id: id.into(), options, proxies, sessions: Mutex::new(Vec::new()) }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// The key the pool is saved under.
    pub fn persist_state_key(&self) -> String {
        format!("CRAWLEE_SESSION_POOL_STATE_{}", self.id)
    }

    fn create_session(&self) -> Arc<Session> {
        let id = format!("session_{}", crawlee_core::request::crypto_random_object_id(10));
        let proxy = self.proxies.as_ref().and_then(|proxies| proxies.new_proxy_info(&id));
        Arc::new(Session::new(id, proxy, self.options.session_options.clone()))
    }

    /// A new session, with a new proxy, that the pool does not keep: for crawlers without a
    /// session pool.
    pub fn detached_session(&self) -> Arc<Session> {
        self.create_session()
    }

    /// A session for the next request.
    pub fn get_session(&self) -> Arc<Session> {
        let mut sessions = self.sessions.lock();
        if sessions.len() < self.options.max_pool_size.max(1) {
            let session = self.create_session();
            sessions.push(session.clone());
            return session;
        }

        let index = rand::rng().random_range(0..sessions.len());
        if sessions[index].is_usable() {
            return sessions[index].clone();
        }

        sessions.retain(|session| session.is_usable());
        let session = self.create_session();
        sessions.push(session.clone());
        session
    }

    /// Adds a session to the pool, also beyond the maximum pool size.
    ///
    /// A session with the same id is replaced when it is no longer usable (retired, expired or used
    /// up), otherwise [`SessionPoolError::AlreadyExists`] is returned.
    pub fn add_session(&self, session: Session) -> Result<Arc<Session>, SessionPoolError> {
        let mut sessions = self.sessions.lock();
        if let Some(index) = sessions.iter().position(|existing| existing.id() == session.id()) {
            if sessions[index].is_usable() {
                return Err(SessionPoolError::AlreadyExists(session.id));
            }
            let removed = sessions.remove(index);
            tracing::debug!("Removed Session - {}", removed.id());
        }
        let session = Arc::new(session);
        sessions.push(session.clone());
        Ok(session)
    }

    /// The usable session with this id, if any.
    pub fn get_session_by_id(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions.lock().iter().find(|session| session.id() == id && session.is_usable()).cloned()
    }

    pub fn usable_count(&self) -> usize {
        self.sessions.lock().iter().filter(|session| session.is_usable()).count()
    }

    pub fn retired_count(&self) -> usize {
        self.sessions.lock().iter().filter(|session| !session.is_usable()).count()
    }
}

impl PersistedState for SessionPool {
    fn to_record(&self) -> Value {
        let sessions = self.sessions.lock();
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
        let mut sessions = saved
            .iter()
            .map(|state| Session::from_state(state, &self.options.session_options))
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
        *self.sessions.lock() = usable;
        Ok(())
    }

    fn reset(&self) {
        self.sessions.lock().clear();
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
