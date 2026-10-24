//! HTTP crawlers for crawlee-rs.
//!
//! - [`HttpCrawler`]: fetches pages and hands the raw response to the handler; [`HttpContext::json`]
//!   deserializes JSON APIs straight into typed structs.
//! - [`HtmlCrawler`]: adds a lazily parsed DOM ([`HtmlContext::with_html`]) and
//!   [`HtmlContext::enqueue_links`], which extracts links by streaming, without a DOM.

pub mod body;
pub mod html;
pub mod http;
mod mime_types;

pub use crate::html::{Document, Element, HtmlContext, HtmlCrawler, HtmlLayer, HtmlPipeline, Selection, SelectorError};
pub use crate::http::{ContentType, HttpContext, HttpCrawler, HttpCrawlerOptions, HttpPipeline};

pub use crawlee_basic::{EnqueueLinksOptions, EnqueueLinksResult};
