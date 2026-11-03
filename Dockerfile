# syntax=docker/dockerfile:1
ARG RUST_VERSION=1.99.1
FROM rust:${RUST_VERSION}-bookworm AS build
ENV CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
RUN apt-get update && apt-get install -y --no-install-recommends clang cmake pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p cargo-flower \
    && install -m 755 target/release/cargo-flower /usr/local/bin/flower

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 flower \
    && useradd --uid 10001 --gid flower --no-create-home --home-dir /data flower \
    && install -d -o flower -g flower /data
COPY --from=build /usr/local/bin/flower /usr/local/bin/flower
COPY LICENSE /usr/share/doc/flower/LICENSE
COPY vendor/redb/LICENSE-* /usr/share/doc/flower/redb/
COPY vendor/openraft/LICENSE-* /usr/share/doc/flower/openraft/
LABEL org.opencontainers.image.title="Flower" \
      org.opencontainers.image.description="Transactional data, reactive computations and durable workflows. Open source under the MIT license." \
      org.opencontainers.image.source="https://github.com/ipsomachina/flower" \
      org.opencontainers.image.licenses="MIT"
USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080 9000
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/flower"]
CMD ["serve", "--dir", "/data", "--id", "1", "--bootstrap", "1", "--http", "0.0.0.0:8080"]
