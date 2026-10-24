# crawlee-impit

An HTTP transport for crawlee-rs backed by [impit](https://github.com/apify/impit). It sends
requests with the TLS and HTTP/2 fingerprints of real browsers (Chrome, Firefox or Safari), which
many anti-bot systems check. impit is also the default HTTP client of Crawlee for JS.

```rust
use std::sync::Arc;

use crawlee::http_client::{HttpClient, RedirectingClient};
use crawlee::{HtmlContext, HtmlCrawler};
use crawlee_impit::ImpitTransport;

let client: Arc<dyn HttpClient> = Arc::new(RedirectingClient::new(ImpitTransport::new()));
let crawler = HtmlCrawler::builder()
    .http_client(client)
    .request_handler(|ctx: HtmlContext| async move { Ok(()) })
    .build()?;
```

Each session impersonates the browser of its fingerprint (Chrome or Firefox, in a version picked
once per session; Edge and Safari fingerprints get Chrome), as `ImpitHttpClient` does in Crawlee
for JS. `ImpitTransport::with_browser(Browser::Firefox)` impersonates one browser for every
request instead.

`ImpitTransport` only does single request/response exchanges. `RedirectingClient` handles
redirects, cookies, sessions and timeouts, as it does for the default reqwest transport. The
impit clients of the 10 most recently used combinations of proxy, TLS mode and browser are kept,
so connections are reused.

## Why it is not part of the main workspace

impit only builds with patched `rustls`, `h2`, `tower-http` and `hyper-util` (Apify forks), and
with `--cfg reqwest_unstable`. Cargo applies `[patch]` sections only in the workspace that
declares them. Adding the patches to the main workspace would force every crawlee-rs user onto
the forks, so this crate is its own workspace, and so is every project that uses it.

## Using it in your project

The crate is not published yet, so depend on it by git. Your project needs four things.

1. The dependency, in `Cargo.toml`:

   ```toml
   [dependencies]
   crawlee = { git = "https://github.com/apify-projects/crawlee-rs" }
   crawlee-impit = { git = "https://github.com/apify-projects/crawlee-rs" }
   ```

2. The patches, also in `Cargo.toml`, at the same revisions as in this crate's
   [`Cargo.toml`](Cargo.toml):

   ```toml
   [patch.crates-io]
   h2 = { git = "https://github.com/apify/h2", rev = "7f393a728a8db07cabb1b78d2094772b33943b9a" }
   rustls = { git = "https://github.com/apify/rustls", rev = "23b2c17427c095b768e22ccf0dadb97266860cf1" }
   tower-http = { git = "https://github.com/apify/tower-http", rev = "f9efc0d9193e774d33aedc1022b922efefc22052" }
   hyper-util = { git = "https://github.com/apify/hyper-util", rev = "9b7795dfd7158fc55e7c84b65bf1dae1d2dea67d" }
   ```

3. The cfg flag, in `.cargo/config.toml`:

   ```toml
   [build]
   rustflags = ["--cfg", "reqwest_unstable"]
   ```

4. Versions that match the forks. The `h2` fork is version 0.4.7, and newer `hyper` releases
   require a newer `h2`. Cargo then resolves `h2` from crates.io and warns that the patch is
   unused. Pin the versions this crate is tested with (see [`Cargo.lock`](Cargo.lock)):

   ```sh
   cargo update -p hyper --precise 1.8.1
   cargo update -p reqwest@0.13 --precise 0.13.2
   cargo update -p h2 --precise 0.4.7
   ```

   Then check that `cargo tree -i h2` shows `h2 v0.4.7 (https://github.com/apify/h2...)`.

## Tests

```sh
cd integrations/crawlee-impit
cargo test
```

`tests/client_hello.rs` checks the impersonation on the wire. It captures the TLS ClientHello each
transport sends to a local listener, then checks for the browser markers:
- Chrome: a GREASE cipher suite, ALPS and ECH.
- Firefox: delegated credentials and a record size limit, without GREASE.
- Plain reqwest: none of these markers.

The test runs locally because a remote fingerprint echo service only sees whatever proxy sits in
between.

`cargo run --example fingerprint` prints the JA4 and HTTP/2 fingerprints reported by
https://tls.peet.ws, for a check against the real thing.
