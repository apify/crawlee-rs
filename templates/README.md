# Templates

These are starting points for new crawlers, the Rust counterparts of the Crawlee for JS
templates.

| Template | Counterpart in JS | What it shows |
|---|---|---|
| [`getting-started`](getting-started) | `getting-started-ts` | `HtmlCrawler` on crawlee.dev. It logs page titles, follows links on the same hostname and exports the results. |
| [`html`](html) | `cheerio-ts` | `HtmlCrawler` with a `Router`. The start page enqueues `detail` pages, and detail pages are scraped (title, meta description, `h1`s). |
| [`json-api`](json-api) | — | `HttpCrawler` on the Hacker News API. It parses JSON into `serde` structs with `ctx.json()`, uses labeled requests, and takes typed `user_data` through `add_typed_handler`. |
| [`empty`](empty) | `empty-ts` | A `BasicCrawler` skeleton that fetches with `send_request`. |

Every template writes its results to `storage/results.json`.

## Run a template inside this repository

```sh
cd templates/getting-started
cargo run
```

- In this repository, the templates depend on the local crates through a path dependency, so
  nothing needs to be published first.
- Set `RUST_LOG=debug` for more detail. Most templates also take `START_URL`, to crawl another site.

## Start a new project from a template

With [cargo-generate](https://github.com/cargo-generate/cargo-generate) (`cargo install cargo-generate`):

```sh
cargo generate --git https://github.com/apify/crawlee-rs templates/html --name my-crawler
cd my-crawler
cargo run
```

- **The generated project** depends on crawlee through git, because it is not published on
  crates.io yet. A hook in `cargo-generate.toml` swaps in `Cargo.generated.toml` for this.
- **Without cargo-generate:**
  1. Copy the template folder.
  2. Delete its `Cargo.toml` and rename `Cargo.generated.toml` to `Cargo.toml`.
  3. Set the package `name`, and delete `cargo-generate.toml` and `swap-manifest.rhai`.
- **Docker:** each template has a `Dockerfile` for the generated project:
  `docker build -t my-crawler . && docker run my-crawler`.

## Maintaining the templates

- **Keep the two manifests in sync.** `Cargo.toml` and `Cargo.generated.toml` may only differ in
  `name` and in the `crawlee` line. `./check-manifests.sh` checks this, and CI runs it.
- **Avoid `{{` and `{%` in template files.** cargo-generate renders every file as a Liquid template.
