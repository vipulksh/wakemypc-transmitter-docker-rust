FROM rust:latest

WORKDIR /app

# Install cargo-watch
RUN cargo install cargo-watch

# Copy Cargo.toml first for dependency caching
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build && \
    rm -rf src

# Source code will be mounted from the host
CMD ["cargo", "watch", "--poll", "-x", "run"]