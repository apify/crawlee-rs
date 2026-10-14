//! Proxy rotation. Each new session gets the next proxy; a session keeps its proxy for life, so
//! rotating a blocked session also rotates its proxy.

use crawlee_core::local::CellExt as _;
use std::cell::Cell;

use url::Url;

/// A proxy assigned to a session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyInfo {
    pub url: Url,
    pub hostname: String,
    pub port: Option<u16>,
    pub username: String,
    pub password: String,
}

impl ProxyInfo {
    pub fn from_url(url: Url) -> Self {
        ProxyInfo {
            hostname: url.host_str().unwrap_or_default().to_owned(),
            port: url.port_or_known_default(),
            username: url.username().to_owned(),
            password: url.password().unwrap_or_default().to_owned(),
            url,
        }
    }
}

/// Source of proxies for new sessions.
pub trait ProxySource {
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

/// Round-robin over a fixed list of proxy URLs (`proxyUrls` in Crawlee for JS).
#[derive(Debug)]
pub struct ProxyConfiguration {
    urls: Vec<Url>,
    next: Cell<usize>,
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
}

impl ProxyConfiguration {
    pub fn new<I, S>(urls: I) -> Result<Self, ProxyConfigurationError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let urls = urls
            .into_iter()
            .map(|raw| {
                let raw = raw.as_ref();
                let url = Url::parse(raw)
                    .map_err(|source| ProxyConfigurationError::InvalidUrl { url: raw.to_owned(), source })?;
                if !matches!(url.scheme(), "http" | "https" | "socks4" | "socks4a" | "socks5" | "socks5h") {
                    return Err(ProxyConfigurationError::UnsupportedScheme(raw.to_owned()));
                }
                Ok(url)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProxyConfiguration { urls, next: Cell::new(0) })
    }
}

impl ProxySource for ProxyConfiguration {
    fn new_proxy_info(&self, _session_id: &str) -> Option<ProxyInfo> {
        if self.urls.is_empty() {
            return None;
        }
        let index = self.next.fetch_add(1) % self.urls.len();
        Some(ProxyInfo::from_url(self.urls[index].clone()))
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
}
