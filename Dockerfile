FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    clang \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Step 1: 只复制依赖定义，用 dummy main 编译依赖（缓存层）
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Step 2: 复制真实代码，增量编译（只编译你的代码，依赖已缓存）
COPY src/ src/
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/reth-sentry-node /usr/local/bin/
COPY sentry.toml /etc/sentry-node/sentry.toml

RUN mkdir -p /data

EXPOSE 30303/tcp 30303/udp 8546/tcp

ENTRYPOINT ["reth-sentry-node"]
CMD ["--data-dir", "/data", "--config", "/etc/sentry-node/sentry.toml"]
