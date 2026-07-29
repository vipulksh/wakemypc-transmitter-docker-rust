# ---- Build Stage ----
FROM rust:latest AS builder

WORKDIR /app

# Pre-build dependencies with a dummy main so this layer is cached
COPY Cargo.toml ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release && \
    rm -f target/release/deps/transmitter*

# Build the real binary
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release

# ---- Runtime Stage ----
FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /app/target/release/transmitter /app/transmitter

CMD ["./transmitter"]
