# Multi-stage build. One builder compiles the whole workspace once;
# the `node` and `aggregator` stages below each copy out just the one
# binary they need, so the images that actually ship don't carry a Rust
# toolchain or the other binaries' build artifacts.
FROM rust:1-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY bench ./bench

RUN cargo build --release -p raftvec-node -p raftvec-aggregator -p vecctl

FROM debian:bookworm-slim AS node
LABEL org.opencontainers.image.source="https://github.com/hiepnnguyentcu/raftvec"
LABEL org.opencontainers.image.description="RaftVec shard replica: one member of one shard's Raft group"
LABEL org.opencontainers.image.licenses="MIT"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/raftvec-node /usr/local/bin/raftvec-node
ENTRYPOINT ["/usr/local/bin/raftvec-node"]

FROM debian:bookworm-slim AS aggregator
LABEL org.opencontainers.image.source="https://github.com/hiepnnguyentcu/raftvec"
LABEL org.opencontainers.image.description="RaftVec aggregator: stateless fan-out/routing layer"
LABEL org.opencontainers.image.licenses="MIT"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/raftvec-aggregator /usr/local/bin/raftvec-aggregator
ENTRYPOINT ["/usr/local/bin/raftvec-aggregator"]

# vecctl image: for running the CLI against the cluster from inside the
# compose network (`docker compose run --rm vecctl search ...`) without
# needing a local Rust toolchain.
FROM debian:bookworm-slim AS vecctl
LABEL org.opencontainers.image.source="https://github.com/hiepnnguyentcu/raftvec"
LABEL org.opencontainers.image.description="RaftVec CLI client"
LABEL org.opencontainers.image.licenses="MIT"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/vecctl /usr/local/bin/vecctl
ENTRYPOINT ["/usr/local/bin/vecctl"]
