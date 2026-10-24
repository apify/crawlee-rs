//! `BasicCrawler` for crawlee-rs: the task loop, retries, sessions, router and statistics every
//! crawler builds on.

pub mod autoscaling;
pub mod context;
pub mod crawler;
pub mod enqueue;
pub mod errors;
pub mod handler;
pub mod proxy;
pub mod robots;
pub mod router;
pub mod session;
pub mod sitemap;
pub mod sitemap_loader;
pub mod statistics;
pub mod throttling;

pub use crate::autoscaling::{ConcurrencyOptions, ConcurrencySystem, LoadSignal, LoadSignalsOptions};
pub use crate::context::{BasicContext, CrawlingContext};
pub use crate::crawler::{BasicCrawler, BasicCrawlerBuilder, BuildError, CrawlerOptions};
pub use crate::enqueue::{
    CrawlerAddRequestsOptions, EnqueueLinksOptions, EnqueueLinksResult, SkipReason, SkippedRequest,
    SkippedRequestCallback, skipped_request_callback,
};
pub use crate::errors::{ErrorKind, MissingRouteError, MissingSessionError, RequestSkipped, SkipRequestError};
pub use crate::handler::{ErrorHandler, FnMiddleware, Identity, Middleware, RequestHandler, Then};
pub use crate::proxy::{ProxyConfiguration, ProxyInfo, ProxySource};
pub use crate::robots::RobotsTxtFile;
pub use crate::router::Router;
pub use crate::session::{Session, SessionOptions, SessionPool, SessionPoolError, SessionPoolOptions};
pub use crate::sitemap::{Sitemap, SitemapOptions};
pub use crate::sitemap_loader::{SitemapRequestLoader, SitemapRequestLoaderOptions};
pub use crate::statistics::{FinalStatistics, Statistics};
pub use crate::throttling::{ThrottleBy, ThrottledDomains, ThrottlingOptions, ThrottlingRequestManager};
