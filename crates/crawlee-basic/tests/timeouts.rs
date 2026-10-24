//! The request timeout and how crawlers built on `BasicCrawler` take part in it.

use std::time::Duration;

use crawlee_basic::{BasicContext, BasicCrawlerBuilder, Middleware};
use crawlee_core::Services;

/// A pipeline step that navigates: it takes `delay`, within a navigation timeout of `timeout`.
struct Navigating {
    delay: Duration,
    timeout: Duration,
}

#[async_trait::async_trait]
impl Middleware<BasicContext> for Navigating {
    type Out = BasicContext;

    async fn run(&self, ctx: BasicContext) -> anyhow::Result<BasicContext> {
        tokio::time::sleep(self.delay).await;
        Ok(ctx)
    }

    fn navigation_timeout(&self) -> Duration {
        self.timeout
    }
}

/// As `withRequestTimeout` in JS, the request timeout is the longer of the internal timeout and
/// the navigation timeout plus the handler timeout.
#[tokio::test(start_paused = true)]
async fn the_request_timeout_covers_the_navigation_timeout() {
    // Handler timeout 60 s, so an internal timeout of 300 s; the navigation takes 350 s of 400 s.
    let navigating = Navigating { delay: Duration::from_secs(350), timeout: Duration::from_secs(400) };
    let crawler = BasicCrawlerBuilder::with_pipeline(navigating)
        .services(Services::in_memory())
        .max_request_retries(0)
        .request_handler(|_ctx: BasicContext| async move { Ok(()) })
        .build()
        .unwrap();
    let stats = crawler.run(["https://a.test/"]).await.unwrap();
    assert_eq!((stats.requests_succeeded, stats.requests_failed), (1, 0), "{stats:#?}");
}
