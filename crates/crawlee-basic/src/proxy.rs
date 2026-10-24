//! Proxy rotation. Each new session gets the next proxy; a session keeps its proxy for life, so
//! rotating a blocked session also rotates its proxy.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use url::Url;

/// A proxy assigned to a session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyInfo {
    pub url: Url,
    pub hostname: String,
    pub port: Option<u16>,
    pub username: String,
    pub password: String,
    /// The proxy intercepts HTTPS traffic (a man-in-the-middle proxy): TLS certificate errors are
    /// ignored for the session's requests.
    pub ignore_tls_errors: bool,
    /// Only the session it was made for sends through this URL (for example, the session id is
    /// part of it). Without a session pool, the crawler then lets the HTTP client drop what it
    /// keeps for the URL once the attempt is over; shared URLs keep their client and connections.
    pub session_specific: bool,
}

impl ProxyInfo {
    /// A shared proxy at `url` that verifies TLS certificates.
    pub fn from_url(url: Url) -> Self {
        ProxyInfo {
            hostname: url.host_str().unwrap_or_default().to_owned(),
            port: url.port_or_known_default(),
            username: url.username().to_owned(),
            password: url.password().unwrap_or_default().to_owned(),
            url,
            ignore_tls_errors: false,
            session_specific: false,
        }
    }
}

/// Source of proxies for new sessions.
pub trait ProxySource: Send + Sync {
    /// The proxy for a new session, or `None` to connect directly.
    fn new_proxy_info(&self, session_id: &str) -> Option<ProxyInfo>;

    /// The proxy of a session restored from the saved state of the session pool. Return the
    /// equivalent proxy usable in the current environment (for example, the Apify Proxy endpoint
    /// after a migration), the argument itself when there is nothing to refresh, or `None` to
    /// connect directly. Does not create a new proxy for an existing session, unlike
    /// [`new_proxy_info`](Self::new_proxy_info).
    fn restored_proxy_info(&self, _session_id: &str, restored: ProxyInfo) -> Option<ProxyInfo> {
        Some(restored)
    }
}

/// An entry of [`ProxyConfiguration::new`]: a proxy URL, or `None` for no proxy.
pub trait ProxyUrlEntry {
    fn proxy_url(&self) -> Option<&str>;
}

macro_rules! proxy_url_entries {
    ($($ty:ty),*) => {$(
        impl ProxyUrlEntry for $ty {
            fn proxy_url(&self) -> Option<&str> {
                Some(AsRef::<str>::as_ref(self))
            }
        }
        impl ProxyUrlEntry for Option<$ty> {
            fn proxy_url(&self) -> Option<&str> {
                self.as_ref().map(AsRef::<str>::as_ref)
            }
        }
    )*};
}

proxy_url_entries!(&str, String, &String);

/// A function that gives the URL of each new proxy, or `None` for no proxy (`newUrlFunction`).
pub type NewUrlFunction = Arc<dyn Fn() -> Option<Url> + Send + Sync>;

enum Urls {
    /// Rotated round-robin; `None` entries mean no proxy.
    List {
        urls: Vec<Option<Url>>,
        next: AtomicUsize,
    },
    Function(NewUrlFunction),
}

/// Proxies from a fixed list of URLs rotated round-robin (`proxyUrls` in Crawlee for JS), or from
/// a function (`newUrlFunction`).
pub struct ProxyConfiguration {
    urls: Urls,
}

impl std::fmt::Debug for ProxyConfiguration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.urls {
            Urls::List { urls, .. } => f.debug_struct("ProxyConfiguration").field("urls", urls).finish(),
            Urls::Function(_) => f.debug_struct("ProxyConfiguration").field("new_url_function", &true).finish(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyConfigurationError {
    #[error("invalid proxy URL '{url}': {source}")]
    InvalidUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },
    #[error("proxy URL '{0}' must use http, https, socks4, socks4a, socks5 or socks5h")]
    UnsupportedScheme(String),
    #[error("the list of proxy URLs must not be empty")]
    Empty,
}

impl ProxyConfiguration {
    /// Rotates `urls`. Entries can be `None` (no proxy): `[Some(url), None]` alternates between
    /// the proxy and a direct connection.
    pub fn new<I>(urls: I) -> Result<Self, ProxyConfigurationError>
    where
        I: IntoIterator,
        I::Item: ProxyUrlEntry,
    {
        let urls = urls
            .into_iter()
            .map(|entry| {
                let Some(raw) = entry.proxy_url() else {
                    return Ok(None);
                };
                let url = Url::parse(raw)
                    .map_err(|source| ProxyConfigurationError::InvalidUrl { url: raw.to_owned(), source })?;
                if !matches!(url.scheme(), "http" | "https" | "socks4" | "socks4a" | "socks5" | "socks5h") {
                    return Err(ProxyConfigurationError::UnsupportedScheme(raw.to_owned()));
                }
                Ok(Some(url))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if urls.is_empty() {
            return Err(ProxyConfigurationError::Empty);
        }
        Ok(ProxyConfiguration { urls: Urls::List { urls, next: AtomicUsize::new(0) } })
    }

    /// Calls `new_url` for the URL of each new proxy; `None` means no proxy.
    pub fn with_new_url_function(new_url: impl Fn() -> Option<Url> + Send + Sync + 'static) -> Self {
        ProxyConfiguration { urls: Urls::Function(Arc::new(new_url)) }
    }

    /// The next proxy URL, or `None` for no proxy (`newUrl`).
    pub fn new_url(&self) -> Option<Url> {
        match &self.urls {
            Urls::List { urls, next } => urls[next.fetch_add(1, Ordering::Relaxed) % urls.len()].clone(),
            Urls::Function(new_url) => new_url(),
        }
    }
}

impl ProxySource for ProxyConfiguration {
    fn new_proxy_info(&self, _session_id: &str) -> Option<ProxyInfo> {
        self.new_url().map(ProxyInfo::from_url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin() {
        let config = ProxyConfiguration::new(["http://user:pass@p1:8000", "http://p2:8000"]).unwrap();
        let first = config.new_proxy_info("a").unwrap();
        assert_eq!((first.hostname.as_str(), first.username.as_str(), first.password.as_str()), ("p1", "user", "pass"));
        assert_eq!(config.new_proxy_info("b").unwrap().hostname, "p2");
        assert_eq!(config.new_proxy_info("c").unwrap().hostname, "p1");
        assert!(ProxyConfiguration::new(["ftp://p"]).is_err());
    }

    /// `null` entries rotate like URLs and mean "no proxy" (`proxyUrls: [url, null]` in JS).
    #[test]
    fn null_entries_connect_directly() {
        let config = ProxyConfiguration::new([Some("http://p1:8000"), None]).unwrap();
        assert_eq!(config.new_proxy_info("a").unwrap().hostname, "p1");
        assert!(config.new_proxy_info("b").is_none());
        assert_eq!(config.new_proxy_info("c").unwrap().hostname, "p1");
    }

    #[test]
    fn an_empty_list_is_rejected() {
        assert!(matches!(ProxyConfiguration::new(Vec::<String>::new()), Err(ProxyConfigurationError::Empty)));
    }

    #[test]
    fn new_url_function_gives_each_proxy() {
        let calls = AtomicUsize::new(0);
        let config = ProxyConfiguration::with_new_url_function(move || {
            let n = calls.fetch_add(1, Ordering::Relaxed);
            n.is_multiple_of(2).then(|| Url::parse(&format!("http://p{n}:8000")).unwrap())
        });
        assert_eq!(config.new_proxy_info("a").unwrap().hostname, "p0");
        assert!(config.new_proxy_info("b").is_none(), "None means no proxy");
        assert_eq!(config.new_url().unwrap().host_str(), Some("p2"));
    }

    /// The URLs of a `ProxyConfiguration` are shared by many sessions: the crawler must not drop
    /// their clients after an attempt.
    #[test]
    fn configured_proxies_are_not_session_specific() {
        let config = ProxyConfiguration::new(["http://p1:8000"]).unwrap();
        let info = config.new_proxy_info("a").unwrap();
        assert!(!info.session_specific);
        assert!(!info.ignore_tls_errors);
    }
}
