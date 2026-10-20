//! `HttpCrawler` on a JSON API: the top stories of Hacker News.
//!
//! The first request lists the ids of the top stories; each story is then fetched as its own
//! request, labeled `ITEM`, and deserialized straight into the `Story` struct (no intermediate
//! JSON tree). Results are saved to `storage/results.json`.
//!
//! Run it with `cargo run` (set `MAX_STORIES` to change how many stories are fetched).

use crawlee::{HttpContext, HttpCrawler, Request, Router};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

const API: &str = "https://hacker-news.firebaseio.com/v0";

/// A story as the API returns it; fields we do not need are ignored.
#[derive(Debug, Deserialize, Serialize)]
struct Story {
    id: u64,
    title: Option<String>,
    url: Option<String>,
    by: Option<String>,
    score: Option<u32>,
    #[serde(rename = "descendants")]
    comments: Option<u32>,
}

/// Data carried by each `ITEM` request in its `user_data`.
#[derive(Debug, Deserialize)]
struct ItemData {
    rank: usize,
}

#[tokio::main(flavor = "local")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let max_stories: usize = std::env::var("MAX_STORIES").ok().and_then(|v| v.parse().ok()).unwrap_or(30);

    let mut router = Router::<HttpContext>::new();

    // The list of story ids.
    router.add_default_handler(move |ctx: HttpContext| async move {
        let ids: Vec<u64> = ctx.json()?;
        tracing::info!("Found {} top stories, fetching the first {max_stories}", ids.len());

        let requests = ids
            .into_iter()
            .take(max_stories)
            .enumerate()
            .map(|(index, id)| {
                Request::builder(format!("{API}/item/{id}.json"))
                    .label("ITEM")
                    .user_data(&serde_json::json!({ "rank": index + 1 }))?
                    .build()
                    .map_err(anyhow::Error::from)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        ctx.add_requests(requests).await?;
        Ok(())
    });

    // One story. `add_typed_handler` hands over the request's `user_data` as `ItemData`.
    router.add_typed_handler("ITEM", |ctx: HttpContext, data: ItemData| async move {
        let story: Story = ctx.json()?;
        tracing::info!(
            "#{} {} ({} points)",
            data.rank,
            story.title.as_deref().unwrap_or("(untitled)"),
            story.score.unwrap_or(0)
        );
        ctx.push_data(&serde_json::json!({ "rank": data.rank, "story": story }))?;
        Ok(())
    });

    let crawler = HttpCrawler::builder().router(router).max_concurrency(10).build()?;
    let stats = crawler.run([format!("{API}/topstories.json")]).await?;

    let saved = crawler.export_data("storage/results.json").await?;
    tracing::info!(
        "Fetched {} stories, saved {saved} items to storage/results.json",
        stats.requests_succeeded.saturating_sub(1)
    );
    Ok(())
}
