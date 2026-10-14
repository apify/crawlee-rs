//! A thread-safe cookie jar, one per session.

use std::collections::HashMap;

use chrono::{DateTime, SecondsFormat, Utc};
use cookie_store::{CookieDomain, CookieExpiration, CookieStore, RawCookie};
use serde_json::{Map, Value, json};
use std::cell::RefCell;
use url::Url;

/// `(domain, path, name)` identifies a cookie in the store.
type CookieKey = (String, String, String);

#[derive(Debug, Default, Clone)]
struct Inner {
    store: CookieStore,
    /// Creation order and time of every cookie, for the RFC 6265 ordering of the `Cookie`
    /// header. `cookie_store` does not keep them, and iterates cookies in hash order.
    created: HashMap<CookieKey, (u64, DateTime<Utc>)>,
    next_sequence: u64,
}

fn key_of(cookie: &cookie_store::Cookie<'_>) -> CookieKey {
    (
        cookie.domain.as_cow().map(|d| d.into_owned()).unwrap_or_default(),
        String::from(&cookie.path),
        cookie.name().to_owned(),
    )
}

impl Inner {
    /// Assigns a creation sequence number to cookies stored since the last call. A replaced
    /// cookie keeps the creation time of the one it replaced, as RFC 6265 requires.
    fn record_new_cookies(&mut self) {
        for cookie in self.store.iter_any() {
            let key = key_of(cookie);
            if !self.created.contains_key(&key) {
                self.created.insert(key, (self.next_sequence, Utc::now()));
                self.next_sequence += 1;
            }
        }
    }

    /// Matching cookies ordered like tough-cookie (and RFC 6265 section 5.4): longer paths first,
    /// then earlier creation.
    fn ordered_matches(&self, url: &Url) -> Vec<(&str, &str)> {
        let mut matches: Vec<(usize, u64, &cookie_store::Cookie<'static>)> = self
            .store
            .matches(url)
            .into_iter()
            .map(|cookie| {
                let sequence = self.created.get(&key_of(cookie)).map_or(u64::MAX, |created| created.0);
                (String::from(&cookie.path).len(), sequence, cookie)
            })
            .collect();
        matches.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        matches.into_iter().map(|(_, _, cookie)| cookie.name_value()).collect()
    }
}

/// Cookies of one session. Shared between concurrent requests of that session.
#[derive(Debug, Default)]
pub struct CookieJar {
    inner: RefCell<Inner>,
}

impl Clone for CookieJar {
    fn clone(&self) -> Self {
        CookieJar { inner: RefCell::new(self.inner.borrow_mut().clone()) }
    }
}

impl CookieJar {
    pub fn new() -> Self {
        Self::default()
    }

    /// The `Cookie` header value for `url`, or `None` when no cookie applies.
    pub fn cookie_header(&self, url: &Url) -> Option<String> {
        let inner = self.inner.borrow_mut();
        let mut header = String::new();
        for (name, value) in inner.ordered_matches(url) {
            if !header.is_empty() {
                header.push_str("; ");
            }
            header.push_str(name);
            header.push('=');
            header.push_str(value);
        }
        (!header.is_empty()).then_some(header)
    }

    /// Stores a cookie given in `Set-Cookie` syntax, as if `url` had set it.
    pub fn set_cookie(&self, cookie: &str, url: &Url) -> bool {
        let mut inner = self.inner.borrow_mut();
        let stored = inner.store.parse(cookie, url).is_ok();
        inner.record_new_cookies();
        stored
    }

    /// Stores every `Set-Cookie` header of a response received from `url`.
    pub fn store_response_cookies<'a>(&self, set_cookie_headers: impl Iterator<Item = &'a str>, url: &Url) {
        let cookies: Vec<RawCookie<'static>> =
            set_cookie_headers.filter_map(|header| RawCookie::parse(header.to_owned()).ok()).collect();
        if cookies.is_empty() {
            return;
        }
        let mut inner = self.inner.borrow_mut();
        // One at a time, so cookies of one response get creation times in header order.
        for cookie in cookies {
            inner.store.store_response_cookies(std::iter::once(cookie), url);
            inner.record_new_cookies();
        }
    }

    /// `(name, value)` pairs that would be sent to `url`, in `Cookie` header order.
    pub fn cookies_for(&self, url: &Url) -> Vec<(String, String)> {
        self.inner
            .borrow_mut()
            .ordered_matches(url)
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect()
    }

    pub fn clear(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.store.clear();
        inner.created.clear();
    }

    /// Merges the jar with cookies from an explicit `Cookie` header (which win on name clashes)
    /// without persisting the header's cookies into the jar, as Crawlee for JS does.
    pub fn merged_cookie_header(&self, url: &Url, request_cookie_header: &str) -> Option<String> {
        let merged = self.clone();
        for pair in request_cookie_header.split(';').map(str::trim).filter(|pair| !pair.is_empty()) {
            merged.set_cookie(pair, url);
        }
        merged.cookie_header(url)
    }
}

fn iso(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl CookieJar {
    /// The jar in the JSON format of tough-cookie's `CookieJar.toJSON()`, which Crawlee for JS
    /// persists sessions in. Expired cookies are left out.
    pub fn to_json(&self) -> Value {
        let inner = self.inner.borrow_mut();
        let mut cookies: Vec<(u64, Value)> = inner
            .store
            .iter_unexpired()
            .map(|cookie| {
                let (sequence, created) = inner.created.get(&key_of(cookie)).copied().unwrap_or((u64::MAX, Utc::now()));
                let mut out = Map::new();
                out.insert("key".into(), cookie.name().into());
                out.insert("value".into(), cookie.value().into());
                if let CookieExpiration::AtUtc(at) = &cookie.expires
                    && let Some(at) = DateTime::from_timestamp(at.unix_timestamp(), 0)
                {
                    out.insert("expires".into(), iso(at).into());
                }
                let (domain, host_only) = match &cookie.domain {
                    CookieDomain::HostOnly(domain) => (Some(domain.clone()), true),
                    CookieDomain::Suffix(domain) => (Some(domain.clone()), false),
                    CookieDomain::NotPresent | CookieDomain::Empty => (None, true),
                };
                if let Some(domain) = domain {
                    out.insert("domain".into(), domain.into());
                }
                out.insert("path".into(), String::from(&cookie.path).into());
                if cookie.secure() == Some(true) {
                    out.insert("secure".into(), true.into());
                }
                if cookie.http_only() == Some(true) {
                    out.insert("httpOnly".into(), true.into());
                }
                out.insert("hostOnly".into(), host_only.into());
                out.insert("creation".into(), iso(created).into());
                out.insert("lastAccessed".into(), iso(created).into());
                if let Some(same_site) = cookie.same_site() {
                    out.insert("sameSite".into(), same_site.to_string().to_ascii_lowercase().into());
                }
                (sequence, Value::Object(out))
            })
            .collect();
        cookies.sort_by_key(|(sequence, _)| *sequence);
        json!({
            "version": "tough-cookie@6.0.1",
            "storeType": "MemoryCookieStore",
            "rejectPublicSuffixes": true,
            "enableLooseMode": false,
            "allowSpecialUseDomain": true,
            "prefixSecurity": "silent",
            "cookies": cookies.into_iter().map(|(_, cookie)| cookie).collect::<Vec<_>>(),
        })
    }

    /// A jar from the output of [`to_json`](Self::to_json) or tough-cookie's
    /// `CookieJar.toJSON()`. Cookies that cannot be read or have expired are skipped.
    pub fn from_json(value: &Value) -> Self {
        let jar = CookieJar::new();
        let mut cookies: Vec<&Value> = value["cookies"].as_array().map(|c| c.iter().collect()).unwrap_or_default();
        // Oldest first, so the creation order survives.
        cookies.sort_by_key(|cookie| cookie["creation"].as_str().unwrap_or_default().to_owned());
        let now = Utc::now();
        let mut inner = jar.inner.borrow_mut();
        for cookie in cookies {
            let (Some(name), Some(domain)) = (cookie["key"].as_str(), cookie["domain"].as_str()) else {
                continue;
            };
            let value = cookie["value"].as_str().unwrap_or_default();
            let path = cookie["path"].as_str().unwrap_or("/");
            let mut header = format!("{name}={value}; Path={path}");
            if cookie["hostOnly"] != Value::Bool(true) {
                header.push_str(&format!("; Domain={domain}"));
            }
            let created = cookie["creation"]
                .as_str()
                .and_then(|c| DateTime::parse_from_rfc3339(c).ok())
                .map_or(now, |c| c.with_timezone(&Utc));
            // tough-cookie keeps `Max-Age` as `maxAge`, counted from the creation; `expires` otherwise.
            let expires = match (&cookie["maxAge"], cookie["expires"].as_str()) {
                (Value::Number(max_age), _) => {
                    max_age.as_i64().map(|secs| Some(created + chrono::Duration::seconds(secs)))
                }
                (_, Some(expires)) if expires != "Infinity" => {
                    DateTime::parse_from_rfc3339(expires).ok().map(|e| Some(e.with_timezone(&Utc)))
                }
                _ => Some(None),
            };
            let Some(expires) = expires else { continue };
            if let Some(expires) = expires {
                let seconds = (expires - now).num_seconds();
                if seconds <= 0 {
                    continue;
                }
                header.push_str(&format!("; Max-Age={seconds}"));
            }
            if cookie["secure"] == Value::Bool(true) {
                header.push_str("; Secure");
            }
            if cookie["httpOnly"] == Value::Bool(true) {
                header.push_str("; HttpOnly");
            }
            if let Some(same_site) = cookie["sameSite"].as_str() {
                header.push_str(&format!("; SameSite={same_site}"));
            }
            let Ok(url) = Url::parse(&format!("https://{}{path}", domain.trim_start_matches('.'))) else {
                continue;
            };
            if inner.store.parse(&header, &url).is_ok() {
                let key = (domain.trim_start_matches('.').to_ascii_lowercase(), path.to_owned(), name.to_owned());
                let sequence = inner.next_sequence;
                inner.next_sequence += 1;
                inner.created.insert(key, (sequence, created));
            }
        }
        inner.record_new_cookies();
        drop(inner);
        jar
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_and_sends_cookies() {
        let jar = CookieJar::new();
        let url = Url::parse("https://example.com/a").unwrap();
        jar.store_response_cookies(["sid=1; Path=/", "theme=dark"].into_iter(), &url);
        assert_eq!(jar.cookie_header(&url).as_deref(), Some("sid=1; theme=dark"));
        assert_eq!(jar.cookie_header(&Url::parse("https://other.com/").unwrap()), None);

        let merged = jar.merged_cookie_header(&url, "sid=override; extra=x").unwrap();
        assert!(merged.contains("sid=override") && merged.contains("extra=x") && merged.contains("theme=dark"));
        // The header's cookies were not persisted.
        assert_eq!(jar.cookie_header(&url).as_deref(), Some("sid=1; theme=dark"));
    }

    #[test]
    fn header_order_follows_rfc6265() {
        let jar = CookieJar::new();
        let url = Url::parse("https://example.com/shop/cart").unwrap();
        for header in ["z=1; Path=/", "a=2; Path=/", "deep=3; Path=/shop", "m=4; Path=/"] {
            jar.store_response_cookies(std::iter::once(header), &url);
        }
        // Longer paths first, then creation order (not alphabetical, not hash order).
        assert_eq!(jar.cookie_header(&url).as_deref(), Some("deep=3; z=1; a=2; m=4"));

        // Replacing a cookie keeps its original creation time.
        jar.store_response_cookies(std::iter::once("z=updated; Path=/"), &url);
        assert_eq!(jar.cookie_header(&url).as_deref(), Some("deep=3; z=updated; a=2; m=4"));
    }

    #[test]
    fn tough_cookie_json_round_trip() {
        let jar = CookieJar::new();
        let url = Url::parse("https://shop.example.com/cart").unwrap();
        for header in [
            "b=2; Path=/",
            "a=1; Path=/; Domain=example.com; Secure; HttpOnly; SameSite=Lax; Max-Age=3600",
            "old=x; Path=/; Max-Age=0",
        ] {
            jar.store_response_cookies(std::iter::once(header), &url);
        }

        let json = jar.to_json();
        let cookies = json["cookies"].as_array().unwrap();
        assert_eq!(cookies.len(), 2, "expired cookies are not saved");
        assert_eq!(cookies[0]["key"], "b");
        assert_eq!(cookies[0]["hostOnly"], true);
        assert_eq!(cookies[0]["domain"], "shop.example.com");
        assert_eq!(cookies[1]["domain"], "example.com");
        assert_eq!(cookies[1]["hostOnly"], false);
        assert_eq!(cookies[1]["secure"], true);
        assert_eq!(cookies[1]["sameSite"], "lax");
        assert!(cookies[1]["expires"].is_string());

        let restored = CookieJar::from_json(&json);
        assert_eq!(restored.cookie_header(&url).as_deref(), Some("b=2; a=1"));
        // The domain cookie applies to the whole domain; the host-only one does not.
        let other = Url::parse("https://www.example.com/").unwrap();
        assert_eq!(restored.cookie_header(&other).as_deref(), Some("a=1"));
    }

    #[test]
    fn reads_tough_cookie_output() {
        // Written by `new CookieJar().setCookie(...)` + `toJSON()` in Node.
        let json = serde_json::json!({
            "version": "tough-cookie@6.0.1",
            "storeType": "MemoryCookieStore",
            "rejectPublicSuffixes": true,
            "cookies": [
                { "key": "sid", "value": "abc", "domain": "example.com", "path": "/", "hostOnly": true,
                  "creation": "2026-01-01T00:00:00.000Z", "lastAccessed": "2026-01-01T00:00:00.000Z" },
                { "key": "gone", "value": "1", "expires": "2020-01-01T00:00:00.000Z", "domain": "example.com",
                  "path": "/", "hostOnly": true, "creation": "2019-01-01T00:00:00.000Z" },
                { "key": "a", "value": "1", "maxAge": 3600, "domain": "example.com", "path": "/", "secure": true,
                  "httpOnly": true, "hostOnly": false, "creation": "2099-01-01T00:00:00.000Z", "sameSite": "lax" },
                { "key": "stale", "value": "1", "maxAge": 60, "domain": "example.com", "path": "/",
                  "hostOnly": true, "creation": "2020-01-01T00:00:00.000Z" }
            ]
        });
        let jar = CookieJar::from_json(&json);
        assert_eq!(jar.cookie_header(&Url::parse("https://example.com/x").unwrap()).as_deref(), Some("sid=abc; a=1"));
        let sub = Url::parse("https://www.example.com/").unwrap();
        assert_eq!(jar.cookie_header(&sub).as_deref(), Some("a=1"));
    }
}
