//! Sitemap parsing (`urlset` and `sitemapindex` XML, plain-text lists, gzip), ported from the
//! `SitemapXmlParser` and `SitemapTxtParser` of Crawlee for JS. Fetching lives in `crawlee-basic`.

use std::io::Read as _;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::Event;
use url::Url;

use crate::js::number;

/// How often a page changes, as a sitemap declares it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeFreq {
    Always,
    Hourly,
    Daily,
    Weekly,
    Monthly,
    Yearly,
    Never,
}

impl ChangeFreq {
    fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "always" => ChangeFreq::Always,
            "hourly" => ChangeFreq::Hourly,
            "daily" => ChangeFreq::Daily,
            "weekly" => ChangeFreq::Weekly,
            "monthly" => ChangeFreq::Monthly,
            "yearly" => ChangeFreq::Yearly,
            "never" => ChangeFreq::Never,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ChangeFreq::Always => "always",
            ChangeFreq::Hourly => "hourly",
            ChangeFreq::Daily => "daily",
            ChangeFreq::Weekly => "weekly",
            ChangeFreq::Monthly => "monthly",
            ChangeFreq::Yearly => "yearly",
            ChangeFreq::Never => "never",
        }
    }
}

/// A `<url>` entry of a sitemap (or a line of a text sitemap).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SitemapUrl {
    pub loc: String,
    pub lastmod: Option<DateTime<Utc>>,
    pub changefreq: Option<ChangeFreq>,
    /// `None` when the value is not a number.
    pub priority: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SitemapItem {
    /// A page.
    Url(SitemapUrl),
    /// A nested sitemap, from a `sitemapindex`.
    Sitemap(String),
}

#[derive(Debug, thiserror::Error)]
pub enum SitemapError {
    #[error("malformed sitemap XML: {0}")]
    Xml(String),
    #[error("failed to decompress the sitemap: {0}")]
    Gzip(#[from] std::io::Error),
    #[error("unsupported sitemap content type (contentType = {content_type}, url = {url})")]
    UnsupportedContentType { content_type: String, url: String },
}

/// Dates as `new Date()` in JS reads the W3C datetime forms sitemaps use.
fn parse_lastmod(text: &str) -> Option<DateTime<Utc>> {
    if let Ok(time) = DateTime::parse_from_rfc3339(text) {
        return Some(time.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%dT%H:%M%:z", "%Y-%m-%dT%H:%M%#z"] {
        if let Ok(time) = DateTime::parse_from_str(text, format) {
            return Some(time.with_timezone(&Utc));
        }
    }
    // Without an offset, a date-time is local in JS; UTC is the portable reading.
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M"] {
        if let Ok(time) = NaiveDateTime::parse_from_str(text, format) {
            return Some(time.and_utc());
        }
    }
    // Date-only forms are UTC midnight in JS.
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(&format!("{text}-01"), "%Y-%m-%d"))
        .or_else(|_| NaiveDate::parse_from_str(&format!("{text}-01-01"), "%Y-%m-%d"))
        .ok()?;
    Some(date.and_hms_opt(0, 0, 0)?.and_utc())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Root {
    UrlSet,
    SitemapIndex,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Field {
    Loc,
    Lastmod,
    Priority,
    Changefreq,
}

fn field_of(name: &[u8]) -> Option<Field> {
    Some(match name {
        b"loc" => Field::Loc,
        b"lastmod" => Field::Lastmod,
        b"priority" => Field::Priority,
        b"changefreq" => Field::Changefreq,
        _ => return None,
    })
}

struct XmlState {
    root: Option<Root>,
    field: Option<Field>,
    text: String,
    url: SitemapUrl,
    items: Vec<SitemapItem>,
}

impl XmlState {
    /// Applies the text collected since the last tag (as one `ontext` event of sax).
    fn flush_text(&mut self) {
        let text = std::mem::take(&mut self.text);
        let Some(field) = self.field else { return };
        let trimmed = text.trim();
        match field {
            Field::Loc => match self.root {
                Some(Root::SitemapIndex) => self.items.push(SitemapItem::Sitemap(trimmed.to_owned())),
                Some(Root::UrlSet) => self.url.loc = trimmed.to_owned(),
                None => {}
            },
            Field::Lastmod => {
                if let Some(lastmod) = parse_lastmod(trimmed) {
                    self.url.lastmod = Some(lastmod);
                }
            }
            Field::Priority => self.url.priority = number(trimmed).filter(|n| !n.is_nan()),
            Field::Changefreq => {
                if let Some(changefreq) = ChangeFreq::parse(trimmed) {
                    self.url.changefreq = Some(changefreq);
                }
            }
        }
    }

    fn open(&mut self, name: &[u8]) {
        if self.root.is_some()
            && let Some(field) = field_of(name)
        {
            self.field = Some(field);
        }
        match name {
            b"urlset" => self.root = Some(Root::UrlSet),
            b"sitemapindex" => self.root = Some(Root::SitemapIndex),
            _ => {}
        }
    }

    fn close(&mut self, name: &[u8]) {
        if field_of(name).is_some() {
            self.field = None;
        }
        if name == b"url" {
            let url = std::mem::take(&mut self.url);
            if !url.loc.is_empty() {
                self.items.push(SitemapItem::Url(url));
            }
        }
    }
}

/// Parses a `urlset` (pages) or `sitemapindex` (nested sitemaps) document. Tag names are
/// compared as written, so a prefixed `<sitemap:loc>` is not read, as in JS.
pub fn parse_sitemap_xml(content: &str) -> Result<Vec<SitemapItem>, SitemapError> {
    let mut reader = quick_xml::Reader::from_str(content);
    let mut state =
        XmlState { root: None, field: None, text: String::new(), url: SitemapUrl::default(), items: Vec::new() };
    loop {
        let event = reader.read_event().map_err(|err| SitemapError::Xml(err.to_string()))?;
        match event {
            Event::Start(tag) => {
                state.flush_text();
                state.open(tag.name().as_ref().as_bytes());
            }
            Event::Empty(tag) => {
                state.flush_text();
                state.open(tag.name().as_ref().as_bytes());
                state.close(tag.name().as_ref().as_bytes());
            }
            Event::End(tag) => {
                state.flush_text();
                state.close(tag.name().as_ref().as_bytes());
            }
            Event::Text(text) => state.text.push_str(&text.xml10_content()),
            Event::CData(data) => state.text.push_str(&data.xml10_content()),
            Event::GeneralRef(reference) => {
                if let Ok(Some(c)) = reference.resolve_char_ref() {
                    state.text.push(c);
                } else {
                    let name = reference.xml10_content();
                    match resolve_predefined_entity(&name) {
                        Some(resolved) => state.text.push_str(resolved),
                        None => return Err(SitemapError::Xml(format!("unknown entity &{name};"))),
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(state.items)
}

/// One URL per line; blank lines are skipped.
pub fn parse_sitemap_txt(content: &str) -> Vec<SitemapItem> {
    content
        .split('\n')
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| SitemapItem::Url(SitemapUrl { loc: line.to_owned(), ..SitemapUrl::default() }))
        .collect()
}

fn mime_essence(content_type: &str) -> String {
    content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase()
}

fn is_xml_mime(essence: &str) -> bool {
    essence == "text/xml" || essence == "application/xml" || essence.ends_with("+xml")
}

/// Parses a fetched sitemap like Crawlee for JS:
/// - **Content type.** It is sniffed from the bytes when they are recognizable: gzip, or XML
///   starting with `<?xml`. Otherwise the header's type is used.
/// - **Gzip.** A gzipped body is decompressed, and a `.gz` suffix is dropped from the URL.
/// - **Parser.** The XML or text parser is chosen by the content type, or else by the URL's
///   `.xml` or `.txt` extension.
///
/// `url` is updated when the `.gz` suffix is dropped.
pub fn decode_sitemap(
    body: &[u8],
    header_content_type: Option<&str>,
    url: &mut Url,
) -> Result<Vec<SitemapItem>, SitemapError> {
    let is_gzip = body.starts_with(&[0x1f, 0x8b, 0x08]);
    let sniffed_xml = {
        let text = body.strip_prefix(b"\xef\xbb\xbf").unwrap_or(body);
        text.starts_with(b"<?xml ")
    };
    let content_type = if is_gzip {
        Some("application/gzip".to_owned())
    } else if sniffed_xml {
        Some("application/xml".to_owned())
    } else {
        header_content_type.map(str::to_owned)
    };

    let decompressed;
    let body = if is_gzip {
        if let Some(stripped) = url.path().strip_suffix(".gz").map(str::to_owned) {
            url.set_path(&stripped);
        }
        let mut out = Vec::new();
        flate2::read::MultiGzDecoder::new(body).read_to_end(&mut out)?;
        decompressed = out;
        &decompressed[..]
    } else {
        body
    };

    let essence = content_type.as_deref().map(mime_essence).unwrap_or_default();
    let text = String::from_utf8_lossy(body);
    if is_xml_mime(&essence) || url.path().ends_with(".xml") {
        parse_sitemap_xml(&text)
    } else if essence == "text/plain" || url.path().ends_with(".txt") {
        Ok(parse_sitemap_txt(&text))
    } else {
        Err(SitemapError::UnsupportedContentType {
            content_type: content_type.unwrap_or_default(),
            url: url.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn urlset_with_all_fields() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc> https://example.com/a?x=1&amp;y=2 </loc>
    <lastmod>2024-05-01</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
  </url>
  <url><loc><![CDATA[https://example.com/b]]></loc><changefreq>sometimes</changefreq></url>
  <url><lastmod>2024-01-01</lastmod></url>
</urlset>"#;
        let items = parse_sitemap_xml(xml).unwrap();
        assert_eq!(items.len(), 2, "an entry without loc is dropped");
        let SitemapItem::Url(first) = &items[0] else { panic!() };
        assert_eq!(first.loc, "https://example.com/a?x=1&y=2");
        assert_eq!(first.lastmod.unwrap().to_rfc3339(), "2024-05-01T00:00:00+00:00");
        assert_eq!(first.changefreq, Some(ChangeFreq::Daily));
        assert_eq!(first.priority, Some(0.8));
        assert_eq!(
            items[1],
            SitemapItem::Url(SitemapUrl { loc: "https://example.com/b".into(), ..Default::default() })
        );
    }

    #[test]
    fn cdata_surrounded_by_whitespace() {
        let xml = "<urlset><url><loc>\n  <![CDATA[https://example.com/a]]>\n</loc>\n\
<priority>\n<![CDATA[0.8]]>\n</priority></url></urlset>";
        let items = parse_sitemap_xml(xml).unwrap();
        assert_eq!(
            items,
            vec![SitemapItem::Url(SitemapUrl {
                loc: "https://example.com/a".into(),
                priority: Some(0.8),
                ..Default::default()
            })]
        );
        let index =
            "<sitemapindex><sitemap><loc>\n<![CDATA[https://example.com/s.xml]]>\n</loc></sitemap></sitemapindex>";
        assert_eq!(parse_sitemap_xml(index).unwrap(), vec![SitemapItem::Sitemap("https://example.com/s.xml".into())]);
    }

    #[test]
    fn sitemap_index_and_text() {
        let xml = "<sitemapindex><sitemap><loc>https://example.com/s1.xml</loc></sitemap>\
                   <sitemap><loc>https://example.com/s2.xml.gz</loc></sitemap></sitemapindex>";
        assert_eq!(
            parse_sitemap_xml(xml).unwrap(),
            vec![
                SitemapItem::Sitemap("https://example.com/s1.xml".into()),
                SitemapItem::Sitemap("https://example.com/s2.xml.gz".into())
            ]
        );
        assert_eq!(parse_sitemap_txt("https://a.test/1\r\n\n  https://a.test/2  \n").len(), 2);
        assert!(parse_sitemap_xml("<urlset><url><loc>x</url></urlset>").is_err());
    }

    #[test]
    fn decoding_sniffs_gzip_and_picks_the_parser() {
        let xml = b"<?xml version=\"1.0\"?><urlset><url><loc>https://a.test/1</loc></url></urlset>";
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(xml).unwrap();
        let gz = gz.finish().unwrap();

        let mut url = Url::parse("https://a.test/sitemap.xml.gz").unwrap();
        assert_eq!(decode_sitemap(&gz, Some("application/octet-stream"), &mut url).unwrap().len(), 1);
        assert_eq!(url.path(), "/sitemap.xml");

        let mut url = Url::parse("https://a.test/sitemap").unwrap();
        assert_eq!(decode_sitemap(xml, None, &mut url).unwrap().len(), 1, "sniffed as XML");
        let mut url = Url::parse("https://a.test/urls").unwrap();
        assert_eq!(decode_sitemap(b"https://a.test/1", Some("text/plain; charset=utf-8"), &mut url).unwrap().len(), 1);
        let mut url = Url::parse("https://a.test/urls").unwrap();
        assert!(matches!(
            decode_sitemap(b"https://a.test/1", Some("text/html"), &mut url),
            Err(SitemapError::UnsupportedContentType { .. })
        ));
    }
}
