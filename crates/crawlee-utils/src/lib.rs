//! Shared utilities for crawlee-rs.
//!
//! Everything here is a pure function or a small value type, and most of it has a counterpart in
//! `@crawlee/utils` or `@apify/utilities` whose behavior it reproduces.

pub mod entities;
pub mod http;
mod js;
pub mod links;
pub mod patterns;
pub mod robots;
pub mod sitemap;
pub mod url;

pub use crate::links::{DEFAULT_LINK_SELECTOR, extract_charset_from_html_bytes, extract_links};
pub use crate::patterns::{UrlFilter, UrlPattern};
pub use crate::robots::RobotsTxt;
pub use crate::sitemap::{SitemapItem, SitemapUrl};
pub use crate::url::{
    EnqueueStrategy, encode_uri_component, filter_url, matches_enqueue_strategy, normalize_url, registrable_domain,
};
