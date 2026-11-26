# Builds the crawler into a small image. Used by generated projects (git dependency on crawlee).
FROM rust:1.88-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

# We use `debian:bookworm-slim` because it is widely used on the Apify platform and thus often cached.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/{{project-name}} /usr/local/bin/crawler
CMD ["crawler"]
