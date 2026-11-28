# Builds the crawler into a small image. Used by generated projects (git dependency on crawlee).
FROM rust:1.88-slim-trixie AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

# We use `debian:trixie-slim` because it is the base of the Apify Python and Node.js images, so it is often cached on the platform.
FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/{{project-name}} /usr/local/bin/crawler
CMD ["crawler"]
