//! robots.txt parsing and matching, a port of the `robots-parser` package that Crawlee for JS
//! uses (`RobotsTxtFile`).
//!
//! - **Groups.** The rules of the group whose user agent matches exactly apply (compared
//!   lowercased, up to the first `/`); otherwise the `*` group does.
//! - **Matching.** The longest matching pattern wins, and `Allow` wins a tie. Patterns support
//!   `*` wildcards and a trailing `$` anchor.
//! - **Normalization.** Patterns are normalized with `encodeURI`. Paths are compared with
//!   uppercase percent-escapes.
//! - **Other origins.** A URL of another origin (scheme, host or port) is not covered by the file,
//!   and counts as allowed.

use std::collections::HashMap;

use url::Url;

use crate::js::number;
use crate::url::{EnqueueStrategy, filter_url};

#[derive(Clone, Debug, PartialEq)]
struct Rule {
    pattern: Vec<u8>,
    allow: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Group {
    rules: Vec<Rule>,
    crawl_delay: Option<f64>,
}

/// A parsed robots.txt file.
#[derive(Clone, Debug, PartialEq)]
pub struct RobotsTxt {
    url: String,
    origin: Option<(String, String, u16)>,
    groups: HashMap<String, Group>,
    sitemaps: Vec<String>,
}

fn origin_of(url: &Url) -> Option<(String, String, u16)> {
    Some((url.scheme().to_owned(), url.host_str()?.to_owned(), url.port_or_known_default()?))
}

fn format_user_agent(user_agent: &str) -> String {
    let lower = user_agent.to_lowercase();
    let cut = lower.find('/').map_or(lower.as_str(), |slash| &lower[..slash]);
    cut.trim().to_owned()
}

/// Characters `encodeURI` leaves as they are.
fn is_uri_unescaped(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b";,/?:@&=+$-_.!~*'()#".contains(&byte)
}

fn upper_hex(byte: u8) -> [u8; 2] {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 0xf)]]
}

/// Uppercases the hex digits of percent-escapes (`%2f` → `%2F`).
fn url_encode_to_upper(path: &[u8]) -> Vec<u8> {
    let mut out = path.to_vec();
    let mut i = 0;
    while i + 2 < out.len() {
        if out[i] == b'%' && out[i + 1].is_ascii_hexdigit() && out[i + 2].is_ascii_hexdigit() {
            out[i + 1] = out[i + 1].to_ascii_uppercase();
            out[i + 2] = out[i + 2].to_ascii_uppercase();
            i += 3;
        } else {
            i += 1;
        }
    }
    out
}

/// `encodeURI(path).replace(/%25/g, '%')`, then uppercased escapes: everything but the URI
/// characters is percent-encoded, while existing `%` escapes are kept.
fn normalise_encoding(pattern: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(pattern.len());
    for &byte in pattern.as_bytes() {
        if is_uri_unescaped(byte) || byte == b'%' {
            out.push(byte);
        } else {
            out.push(b'%');
            out.extend_from_slice(&upper_hex(byte));
        }
    }
    url_encode_to_upper(&out)
}

/// Whether `pattern` (with `*` and a trailing `$`) matches the start of `path`.
fn matches(pattern: &[u8], path: &[u8]) -> bool {
    // The lengths of the path prefixes the pattern so far can match, in increasing order.
    let mut lengths: Vec<usize> = vec![0; path.len() + 1];
    let mut count = 1;
    for (p, &byte) in pattern.iter().enumerate() {
        if byte == b'$' && p + 1 == pattern.len() {
            return lengths[count - 1] == path.len();
        }
        if byte == b'*' {
            count = path.len() - lengths[0] + 1;
            for i in 1..count {
                lengths[i] = lengths[i - 1] + 1;
            }
        } else {
            let mut matched = 0;
            for i in 0..count {
                if lengths[i] < path.len() && path[lengths[i]] == byte {
                    lengths[matched] = lengths[i] + 1;
                    matched += 1;
                }
            }
            if matched == 0 {
                return false;
            }
            count = matched;
        }
    }
    true
}

impl RobotsTxt {
    /// Parses `content`, the robots.txt served at `robots_url`.
    pub fn parse(robots_url: &str, content: &str) -> Self {
        let parsed = Url::parse(robots_url).ok();
        let mut robots = RobotsTxt {
            url: robots_url.to_owned(),
            origin: parsed.as_ref().and_then(origin_of),
            groups: HashMap::new(),
            sitemaps: Vec::new(),
        };

        let mut user_agents: Vec<String> = Vec::new();
        let mut after_non_user_agent_line = true;
        for line in content.split("\r\n").flat_map(|l| l.split('\r')).flat_map(|l| l.split('\n')) {
            let line = line.find('#').map_or(line, |comment| &line[..comment]);
            let Some((key, value)) = line.split_once(':') else { continue };
            let (key, value) = (key.trim(), value.trim());
            if key.is_empty() {
                continue;
            }
            let key = key.to_lowercase();
            match key.as_str() {
                "user-agent" => {
                    if after_non_user_agent_line {
                        user_agents.clear();
                    }
                    if !value.is_empty() {
                        user_agents.push(format_user_agent(value));
                    }
                }
                "disallow" | "allow" => {
                    for agent in &user_agents {
                        let group = robots.groups.entry(agent.clone()).or_default();
                        if !value.is_empty() {
                            group.rules.push(Rule { pattern: normalise_encoding(value), allow: key == "allow" });
                        }
                    }
                }
                "crawl-delay" => {
                    for agent in &user_agents {
                        let group = robots.groups.entry(agent.clone()).or_default();
                        if let Some(delay) = number(value).filter(|delay| !delay.is_nan()) {
                            group.crawl_delay = Some(delay);
                        }
                    }
                }
                "sitemap" if !value.is_empty() => robots.sitemaps.push(value.to_owned()),
                _ => {}
            }
            after_non_user_agent_line = key != "user-agent";
        }
        robots
    }

    /// A file that allows everything, for a site without robots.txt.
    pub fn allow_all(robots_url: &str) -> Self {
        Self::parse(robots_url, "")
    }

    /// A file that disallows everything, for a site whose robots.txt is unreachable (5xx).
    pub fn disallow_all(robots_url: &str) -> Self {
        Self::parse(robots_url, "User-agent: *\nDisallow: /")
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn group(&self, user_agent: &str) -> Option<&Group> {
        self.groups.get(&format_user_agent(user_agent)).or_else(|| self.groups.get("*"))
    }

    /// `Some(allowed)` when the file covers `url`, `None` for a URL of another origin.
    pub fn check(&self, url: &str, user_agent: &str) -> Option<bool> {
        let parsed = Url::parse(url).ok()?;
        if origin_of(&parsed) != self.origin {
            return None;
        }
        let mut path = parsed.path().to_owned();
        if let Some(query) = parsed.query() {
            path.push('?');
            path.push_str(query);
        }
        let path = url_encode_to_upper(path.as_bytes());

        let rules = self.group(user_agent).map_or(&[][..], |group| &group.rules);
        let mut best: Option<&Rule> = None;
        for rule in rules.iter().filter(|rule| matches(&rule.pattern, &path)) {
            best = match best {
                None => Some(rule),
                Some(current) if rule.pattern.len() > current.pattern.len() => Some(rule),
                Some(current) if rule.pattern.len() == current.pattern.len() && rule.allow && !current.allow => {
                    Some(rule)
                }
                keep => keep,
            };
        }
        Some(best.is_none_or(|rule| rule.allow))
    }

    /// Whether `user_agent` may crawl `url`; URLs the file does not cover are allowed.
    pub fn is_allowed(&self, url: &str, user_agent: &str) -> bool {
        self.check(url, user_agent).unwrap_or(true)
    }

    /// The `Crawl-delay` for `user_agent`, in seconds.
    pub fn crawl_delay(&self, user_agent: &str) -> Option<f64> {
        self.group(user_agent)?.crawl_delay
    }

    /// Every `Sitemap:` URL, in file order.
    pub fn sitemaps(&self) -> &[String] {
        &self.sitemaps
    }

    /// The `Sitemap:` URLs that match `strategy` relative to the robots.txt URL (JS uses
    /// `same-hostname` by default); the others are logged and left out.
    pub fn sitemaps_matching(&self, strategy: EnqueueStrategy) -> Vec<String> {
        self.sitemaps
            .iter()
            .filter(|sitemap| match filter_url(sitemap, &self.url, strategy) {
                Ok(_) => true,
                Err(reason) => {
                    tracing::warn!("Skipping sitemap {sitemap} listed in robots.txt at {}: {reason}.", self.url);
                    false
                }
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROBOTS: &str = "\
User-agent: *
Disallow: /private/
Allow: /private/public
Disallow: /*.pdf$
Crawl-delay: 2

User-agent: GoodBot/1.0
User-agent: other
Disallow:

Sitemap: https://example.com/sitemap.xml
";

    #[test]
    fn groups_rules_and_crawl_delay() {
        let robots = RobotsTxt::parse("https://example.com/robots.txt", ROBOTS);
        assert!(!robots.is_allowed("https://example.com/private/x", "*"));
        assert!(robots.is_allowed("https://example.com/private/public/x", "*"), "longer allow wins");
        assert!(!robots.is_allowed("https://example.com/a/b.pdf", "*"));
        assert!(robots.is_allowed("https://example.com/a/b.pdf?x", "*"), "$ anchors the end");
        assert!(robots.is_allowed("https://example.com/private/x", "GoodBot"), "own group, empty disallow");
        assert!(!robots.is_allowed("https://example.com/private/x", "SomeBot"), "falls back to *");
        assert_eq!(robots.crawl_delay("*"), Some(2.0));
        assert_eq!(robots.crawl_delay("goodbot"), None);
        assert_eq!(robots.check("http://example.com/private/x", "*"), None, "other scheme");
        assert_eq!(robots.check("https://example.com:8443/private/x", "*"), None, "other port");
        assert_eq!(robots.sitemaps(), ["https://example.com/sitemap.xml"]);
    }

    #[test]
    fn allow_wins_ties_and_encoding_is_normalised() {
        let robots = RobotsTxt::parse(
            "https://example.com/robots.txt",
            "User-agent: *\nDisallow: /page\nAllow: /page\nDisallow: /caf\u{e9}\nDisallow: /a%2fb",
        );
        assert!(robots.is_allowed("https://example.com/page", "*"));
        assert!(!robots.is_allowed("https://example.com/caf%C3%A9", "*"));
        assert!(!robots.is_allowed("https://example.com/a%2Fb", "*"));
    }

    #[test]
    fn wildcard_matching() {
        assert!(matches(b"/*/x", b"/a/b/x"));
        assert!(matches(b"/a*", b"/a"));
        assert!(!matches(b"/a$", b"/ab"));
        assert!(matches(b"*", b""));
    }
}
