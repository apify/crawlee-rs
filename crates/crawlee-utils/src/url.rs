//! URL helpers: `uniqueKey` normalization and enqueue strategies.
//!
//! Both are wire-level behavior shared with Crawlee for JavaScript and Python, so they are ported
//! byte-for-byte and covered by golden tests generated from the JS implementation.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use url::Url;

/// Normalizes a URL the same way `normalizeUrl` from `@apify/utilities` does. Crawlee derives a
/// request's `uniqueKey` from this value, so any difference here breaks deduplication across runs
/// and across language implementations.
///
/// - leading/trailing whitespace is trimmed,
/// - `utm_*` query parameters are removed,
/// - query parameters are sorted by name (stable, by UTF-16 code units like `URLSearchParams.sort()`)
///   and re-serialized with `application/x-www-form-urlencoded` rules,
/// - the scheme and host are lowercased,
/// - one trailing slash is removed from the path,
/// - the fragment is dropped unless `keep_fragment` is set.
///
/// Returns `None` for an empty or unparseable input.
pub fn normalize_url(url: &str, keep_fragment: bool) -> Option<String> {
    if url.is_empty() {
        return None;
    }

    let parsed = Url::parse(url.trim()).ok()?;

    let mut params: Vec<(String, String)> = parsed
        .query_pairs()
        .filter(|(key, _)| !key.starts_with("utm_"))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    // `sort_by` is stable, which `URLSearchParams.sort()` requires as well.
    params.sort_by(|(a, _), (b, _)| cmp_utf16(a, b));

    let search = if params.is_empty() {
        String::new()
    } else {
        let mut serializer = url::form_urlencoded::Serializer::for_suffix(String::from("?"), 1);
        serializer.extend_pairs(params);
        serializer.finish()
    };

    let mut out = String::with_capacity(url.len());
    out.push_str(&parsed.scheme().to_ascii_lowercase());
    out.push(':');
    out.push_str("//");
    if let Some(host) = parsed.host_str() {
        out.push_str(&host.to_lowercase());
    }
    if let Some(port) = parsed.port() {
        out.push(':');
        out.push_str(&port.to_string());
    }

    let path = parsed.path();
    out.push_str(path.strip_suffix('/').unwrap_or(path));
    out.push_str(&search);

    if keep_fragment {
        // `URL.hash` is empty for both a missing and an empty fragment.
        if let Some(fragment) = parsed.fragment().filter(|f| !f.is_empty()) {
            out.push('#');
            out.push_str(fragment);
        }
    }

    Some(out)
}

/// Compares strings by UTF-16 code units, the order JavaScript's default string comparison uses.
/// It differs from byte (UTF-8) order only for characters above U+FFFF versus U+E000..U+FFFF.
fn cmp_utf16(a: &str, b: &str) -> Ordering {
    if a.is_ascii() && b.is_ascii() {
        return a.cmp(b);
    }
    a.encode_utf16().cmp(b.encode_utf16())
}

/// Which links `enqueue_links` follows, relative to a base URL.
///
/// ```text
/// Protocol          Domain
/// ┌────┐          ┌─────────┐
/// https://example.crawlee.dev/...
/// │       └─────────────────┤
/// │             Hostname    │
/// └─────────────────────────┘
///          Origin
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EnqueueStrategy {
    /// Matches any URL.
    All,
    /// Same hostname, any of `http`/`https`.
    #[default]
    SameHostname,
    /// Same registrable domain (subdomains included), any of `http`/`https`.
    SameDomain,
    /// Same scheme, hostname and port.
    SameOrigin,
}

impl EnqueueStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            EnqueueStrategy::All => "all",
            EnqueueStrategy::SameHostname => "same-hostname",
            EnqueueStrategy::SameDomain => "same-domain",
            EnqueueStrategy::SameOrigin => "same-origin",
        }
    }
}

impl std::fmt::Display for EnqueueStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returns the registrable domain (eTLD+1) of a hostname, like `getDomain()` from `tldts` with its
/// default options: only the ICANN section of the Public Suffix List is considered (so
/// `foo.github.io` resolves to `github.io`), and IP addresses have no registrable domain.
pub fn registrable_domain(hostname: &str) -> Option<String> {
    let hostname = strip_trailing_dot(hostname).to_ascii_lowercase();

    if hostname.is_empty() || is_ip_address(&hostname) {
        return None;
    }

    let suffix_len = icann_suffix_len(&hostname)?;
    if suffix_len >= hostname.len() {
        // The hostname itself is a public suffix.
        return None;
    }

    let rest = &hostname[..hostname.len() - suffix_len - 1];
    let label = rest.rsplit('.').next().filter(|label| !label.is_empty())?;
    Some(format!("{label}.{}", &hostname[hostname.len() - suffix_len..]))
}

/// Length in bytes of the longest ICANN public suffix of `hostname`.
fn icann_suffix_len(hostname: &str) -> Option<usize> {
    let suffix = psl::suffix(hostname.as_bytes())?;

    if suffix.typ() != Some(psl::Type::Private) {
        return Some(suffix.as_bytes().len());
    }

    // The longest match is a private rule; `tldts` ignores those by default, so fall back to the
    // longest ICANN suffix contained in it.
    let private = std::str::from_utf8(suffix.as_bytes()).ok()?;
    let mut candidate = private;
    while let Some((_, shorter)) = candidate.split_once('.') {
        candidate = shorter;
        if let Some(found) = psl::suffix(candidate.as_bytes())
            && found.as_bytes().len() == candidate.len()
            && found.typ() != Some(psl::Type::Private)
        {
            return Some(candidate.len());
        }
    }
    // A single unknown label is a public suffix under the implicit `*` rule.
    Some(candidate.len())
}

fn is_ip_address(hostname: &str) -> bool {
    let bare = hostname.trim_start_matches('[').trim_end_matches(']');
    bare.parse::<std::net::IpAddr>().is_ok()
}

fn strip_trailing_dot(hostname: &str) -> &str {
    hostname.strip_suffix('.').unwrap_or(hostname)
}

fn hostname_of(url: &Url) -> &str {
    strip_trailing_dot(url.host_str().unwrap_or_default())
}

/// Checks whether `target` matches `origin` under `strategy`. The scheme is not checked here; see
/// [`filter_url`] for the combined check.
pub fn matches_enqueue_strategy(strategy: EnqueueStrategy, target: &Url, origin: &Url) -> bool {
    match strategy {
        EnqueueStrategy::All => true,
        EnqueueStrategy::SameHostname => hostname_of(target) == hostname_of(origin),
        EnqueueStrategy::SameDomain => match registrable_domain(hostname_of(origin)) {
            Some(origin_domain) => registrable_domain(hostname_of(target)).as_deref() == Some(origin_domain.as_str()),
            // No registrable domain (e.g. an IP address): compare origins.
            None => target.origin() == origin.origin(),
        },
        EnqueueStrategy::SameOrigin => {
            target.scheme() == origin.scheme()
                && hostname_of(target) == hostname_of(origin)
                && target.port() == origin.port()
        }
    }
}

/// Why [`filter_url`] rejected a URL.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UrlRejection {
    #[error("unsupported URL scheme (only http and https are allowed)")]
    UnsupportedScheme,
    #[error("invalid origin URL")]
    InvalidOrigin,
    #[error("does not match enqueue strategy '{0}'")]
    StrategyMismatch(EnqueueStrategy),
}

/// Checks that `target` uses `http(s)` and matches `strategy` relative to `origin`.
pub fn filter_url(target: &str, origin: &str, strategy: EnqueueStrategy) -> Result<Url, UrlRejection> {
    let target = Url::parse(target).map_err(|_| UrlRejection::UnsupportedScheme)?;
    if !matches!(target.scheme(), "http" | "https") {
        return Err(UrlRejection::UnsupportedScheme);
    }
    let origin = Url::parse(origin).map_err(|_| UrlRejection::InvalidOrigin)?;
    if !matches_enqueue_strategy(strategy, &target, &origin) {
        return Err(UrlRejection::StrategyMismatch(strategy));
    }
    Ok(target)
}

/// Picks the URL that `enqueue_links` filters found links against, following
/// `resolveBaseUrlForEnqueueLinksFiltering` in Crawlee for JS: a user-provided base URL wins;
/// otherwise the original request URL is used, except after a redirect under `All` (final URL) or
/// under `SameDomain` when the redirect stayed on the same registrable domain (final URL).
pub fn resolve_base_url_for_filtering(
    strategy: EnqueueStrategy,
    original_request_url: &Url,
    final_request_url: Option<&Url>,
    user_provided_base_url: Option<&Url>,
) -> Url {
    if let Some(base) = user_provided_base_url {
        return base.clone();
    }

    let final_url = final_request_url.unwrap_or(original_request_url);
    match strategy {
        EnqueueStrategy::All => final_url.clone(),
        EnqueueStrategy::SameDomain
            if registrable_domain(hostname_of(original_request_url)) == registrable_domain(hostname_of(final_url)) =>
        {
            final_url.clone()
        }
        _ => original_request_url.clone(),
    }
}

/// `encodeURIComponent` of JavaScript: percent-encodes the UTF-8 bytes of every character but the
/// ASCII letters and digits and `-_.!~*'()`, with uppercase hex digits.
pub fn encode_uri_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The results of `encodeURIComponent` in Node.
    #[test]
    fn encode_uri_component_matches_javascript() {
        assert_eq!(encode_uri_component("AZaz09-_.!~*'()"), "AZaz09-_.!~*'()");
        assert_eq!(encode_uri_component(";/?:@&=+$,# %"), "%3B%2F%3F%3A%40%26%3D%2B%24%2C%23%20%25");
        assert_eq!(encode_uri_component("a.b:8080/ž😀"), "a.b%3A8080%2F%C5%BE%F0%9F%98%80");
    }

    #[test]
    fn normalize_url_matches_apify_utilities() {
        let cases = [
            ("HTTP://www.EXAMPLE.com/something/", "http://www.example.com/something"),
            ("https://example.com", "https://example.com"),
            ("https://example.com/", "https://example.com"),
            ("https://example.com/?b=2&a=1&utm_source=x", "https://example.com?a=1&b=2"),
            ("https://example.com/a?q=hello%20world", "https://example.com/a?q=hello+world"),
            ("https://example.com/a?flag", "https://example.com/a?flag="),
            ("https://example.com:8080/a#frag", "https://example.com:8080/a"),
            ("  https://example.com/x  ", "https://example.com/x"),
            ("https://example.com:443/x", "https://example.com/x"),
        ];
        for (input, expected) in cases {
            assert_eq!(normalize_url(input, false).as_deref(), Some(expected), "{input}");
        }
        assert_eq!(normalize_url("https://example.com/a#frag", true).as_deref(), Some("https://example.com/a#frag"));
        assert_eq!(normalize_url("not a url", false), None);
        assert_eq!(normalize_url("", false), None);
    }

    #[test]
    fn registrable_domain_ignores_private_suffixes() {
        assert_eq!(registrable_domain("www.example.com").as_deref(), Some("example.com"));
        assert_eq!(registrable_domain("a.b.example.co.uk").as_deref(), Some("example.co.uk"));
        assert_eq!(registrable_domain("foo.github.io").as_deref(), Some("github.io"));
        assert_eq!(registrable_domain("example.com.").as_deref(), Some("example.com"));
        assert_eq!(registrable_domain("com"), None);
        assert_eq!(registrable_domain("127.0.0.1"), None);
    }

    #[test]
    fn strategies() {
        let origin = Url::parse("https://www.example.com/start").unwrap();
        let check = |strategy, target: &str| matches_enqueue_strategy(strategy, &Url::parse(target).unwrap(), &origin);

        assert!(check(EnqueueStrategy::SameHostname, "http://www.example.com/x"));
        assert!(!check(EnqueueStrategy::SameHostname, "https://example.com/x"));
        assert!(check(EnqueueStrategy::SameDomain, "https://shop.example.com/x"));
        assert!(!check(EnqueueStrategy::SameDomain, "https://example.org/x"));
        assert!(check(EnqueueStrategy::SameOrigin, "https://www.example.com/x"));
        assert!(!check(EnqueueStrategy::SameOrigin, "http://www.example.com/x"));
        assert!(check(EnqueueStrategy::All, "https://other.dev"));

        assert_eq!(
            filter_url("mailto:a@b.c", origin.as_str(), EnqueueStrategy::All),
            Err(UrlRejection::UnsupportedScheme)
        );
    }
}
