//! Streaming link extraction: pulls `href`s out of HTML without building a DOM.
//!
//! This is the fast path behind `enqueue_links`. It runs `lol_html` in linear time and small
//! constant memory, which matters because link extraction runs on every page of a crawl, while a
//! full DOM is only needed when the user's handler actually queries the document.

use std::borrow::Cow;
use std::cell::RefCell;

use lol_html::html_content::{Element, TextChunk};
use lol_html::{ElementContentHandlers, HtmlRewriter, Selector, Settings};
use url::Url;

use crate::entities::decode_attribute_value;

/// The selector `enqueue_links` uses by default, as in Crawlee for JS.
pub const DEFAULT_LINK_SELECTOR: &str = "a";

#[derive(Debug, thiserror::Error)]
pub enum LinkExtractionError {
    /// The selector uses syntax the streaming extractor does not support (for example `:has()`,
    /// sibling combinators or pseudo-classes other than `:nth-child`/`:first-child`/`:not`).
    /// Callers can fall back to extracting links from a parsed DOM.
    #[error("selector '{selector}' is not supported by the streaming link extractor: {reason}")]
    UnsupportedSelector { selector: String, reason: String },
    #[error("failed to extract links: {0}")]
    Rewriting(String),
}

/// Checks whether `selector` can be used with [`extract_links`].
pub fn is_streaming_selector(selector: &str) -> bool {
    selector.parse::<Selector>().is_ok()
}

/// Extracts the `href` of every element matching `selector` and resolves it to an absolute URL.
///
/// Mirrors `extractUrlsFromCheerio` in Crawlee for JS: the first `<base href>` in the document
/// (resolved against `base_url`) takes precedence over `base_url`, empty `href`s are skipped and
/// values that fail to resolve are dropped. Unlike the JS version the result contains `Url`s, and
/// relative links can always be resolved because `base_url` is required.
pub fn extract_links(html: &[u8], selector: &str, base_url: &Url) -> Result<Vec<Url>, LinkExtractionError> {
    let link_selector: Selector = selector.parse().map_err(|err: lol_html::errors::SelectorError| {
        LinkExtractionError::UnsupportedSelector { selector: selector.to_owned(), reason: err.to_string() }
    })?;
    let base_selector: Selector = "base[href]".parse().expect("static selector is valid");
    let noscript_selector: Selector = "noscript".parse().expect("static selector is valid");

    let collected = collect_hrefs(html, &link_selector, &base_selector, Some(&noscript_selector))?;

    let base_url = collected
        .base
        .and_then(|base| base_url.join(decode_attribute_value(&base).trim()).ok())
        .unwrap_or_else(|| base_url.clone());

    Ok(collected
        .hrefs
        .iter()
        .map(|href| decode_attribute_value(href))
        .filter(|href| !href.is_empty())
        .filter_map(|href| base_url.join(&href).ok())
        .collect())
}

/// Raw (still entity-encoded) `href`s in document order, and the first `<base href>`.
struct Collected {
    hrefs: Vec<String>,
    base: Option<String>,
}

/// One streaming pass over `html`.
///
/// `lol_html` tokenizes `<noscript>` content as raw text, as a browser with scripting enabled
/// does, whereas `htmlparser2` (behind `CheerioCrawler` in Crawlee for JS) parses it as markup and
/// finds the links inside. To match, `<noscript>` contents are buffered and scanned in a second
/// pass, and their links are spliced in at the position of the `<noscript>` element.
fn collect_hrefs(
    html: &[u8],
    link_selector: &Selector,
    base_selector: &Selector,
    noscript_selector: Option<&Selector>,
) -> Result<Collected, LinkExtractionError> {
    let hrefs: RefCell<Vec<String>> = RefCell::new(Vec::new());
    let base: RefCell<Option<String>> = RefCell::new(None);
    // (index into `hrefs` where the element started, raw content)
    let noscripts: RefCell<Vec<(usize, String)>> = RefCell::new(Vec::new());

    {
        let on_link = |el: &mut Element<'_, '_>| {
            if let Some(href) = el.get_attribute("href") {
                hrefs.borrow_mut().push(href);
            }
            Ok(())
        };
        let on_base = |el: &mut Element<'_, '_>| {
            let mut base = base.borrow_mut();
            if base.is_none() {
                *base = el.get_attribute("href");
            }
            Ok(())
        };

        let mut settings = Settings::new()
            .append_element_content_handler((
                Cow::Borrowed(link_selector),
                ElementContentHandlers::default().element(on_link),
            ))
            .append_element_content_handler((
                Cow::Borrowed(base_selector),
                ElementContentHandlers::default().element(on_base),
            ));

        if let Some(noscript_selector) = noscript_selector {
            let on_noscript = |_: &mut Element<'_, '_>| {
                noscripts.borrow_mut().push((hrefs.borrow().len(), String::new()));
                Ok(())
            };
            let on_noscript_text = |chunk: &mut TextChunk<'_>| {
                if let Some((_, content)) = noscripts.borrow_mut().last_mut() {
                    content.push_str(chunk.as_str());
                }
                Ok(())
            };
            settings = settings.append_element_content_handler((
                Cow::Borrowed(noscript_selector),
                ElementContentHandlers::default().element(on_noscript).text(on_noscript_text),
            ));
        }

        let mut rewriter = HtmlRewriter::new(settings, |_: &[u8]| {});
        rewriter.write(html).map_err(|err| LinkExtractionError::Rewriting(err.to_string()))?;
        rewriter.end().map_err(|err| LinkExtractionError::Rewriting(err.to_string()))?;
    }

    let mut hrefs = hrefs.into_inner();
    let mut base = base.into_inner();
    // Splice from the last one so earlier insertion points stay valid.
    for (at, content) in noscripts.into_inner().into_iter().rev() {
        if !content.contains('<') {
            continue;
        }
        let inner = collect_hrefs(content.as_bytes(), link_selector, base_selector, None)?;
        hrefs.splice(at..at, inner.hrefs);
        if base.is_none() {
            base = inner.base;
        }
    }

    Ok(Collected { hrefs, base })
}

/// Finds a `<meta charset>` declaration in the first 1024 bytes of an HTML document, like
/// `extractCharsetFromHtmlBytes` in Crawlee for JS. Returns the raw label, if any.
pub fn extract_charset_from_html_bytes(bytes: &[u8]) -> Option<String> {
    thread_local! {
        static META_CHARSET: regex::bytes::Regex =
            regex::bytes::Regex::new(r#"(?i-u)<meta[^>]+\bcharset\s*=\s*["']?\s*([^"'\s;>]+)"#).expect("valid regex");
    }

    let prescan = &bytes[..bytes.len().min(1024)];
    let label = META_CHARSET.with(|regex| regex.captures(prescan)?.get(1).map(|label| label.as_bytes().to_vec()))?;
    // Latin-1 decoding, as the JS version does, maps every byte to one char.
    Some(label.iter().map(|&b| b as char).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn links(html: &str, base: &str) -> Vec<String> {
        extract_links(html.as_bytes(), DEFAULT_LINK_SELECTOR, &Url::parse(base).unwrap())
            .unwrap()
            .into_iter()
            .map(String::from)
            .collect()
    }

    #[test]
    fn resolves_relative_links() {
        let html = r#"<html><body>
            <a href="/a">A</a>
            <a href="b?x=1&amp;y=2">B</a>
            <a href="">empty</a>
            <a>no href</a>
            <a href="https://other.dev/c">C</a>
            <a href="mailto:x@y.z">mail</a>
        </body></html>"#;
        assert_eq!(
            links(html, "https://example.com/dir/page"),
            vec!["https://example.com/a", "https://example.com/dir/b?x=1&y=2", "https://other.dev/c", "mailto:x@y.z",]
        );
    }

    #[test]
    fn honors_base_href_even_after_links() {
        let html = r#"<a href="x">X</a><base href="/root/"><base href="/ignored/">"#;
        assert_eq!(links(html, "https://example.com/dir/page"), vec!["https://example.com/root/x"]);
    }

    #[test]
    fn custom_selector_and_unsupported_selector() {
        let html = r#"<a class="next" href="/2">next</a><a href="/other">o</a>"#;
        let base = Url::parse("https://example.com/").unwrap();
        let found = extract_links(html.as_bytes(), "a.next", &base).unwrap();
        assert_eq!(found, vec![Url::parse("https://example.com/2").unwrap()]);

        assert!(matches!(
            extract_links(html.as_bytes(), "a:has(span)", &base),
            Err(LinkExtractionError::UnsupportedSelector { .. })
        ));
    }

    #[test]
    fn charset_prescan() {
        assert_eq!(
            extract_charset_from_html_bytes(br#"<html><head><meta charset="windows-1250">"#).as_deref(),
            Some("windows-1250")
        );
        assert_eq!(
            extract_charset_from_html_bytes(
                br#"<META http-equiv="Content-Type" content="text/html; charset=ISO-8859-2">"#
            )
            .as_deref(),
            Some("ISO-8859-2")
        );
        assert_eq!(extract_charset_from_html_bytes(b"<html>"), None);
    }
}
