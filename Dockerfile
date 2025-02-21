# Use the official Rust image as a base
FROM rust:latest AS builder

# Set the working directory
WORKDIR /app

RUN mkdir -p /app/build/target

# Install sqlx-cli
RUN cargo install sqlx-cli

# Copy the entire workspace
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp -r /app/target /app/build

# Use a minimal base image for the final container
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the built binaries from the builder stage
COPY --from=builder /app/build/target/release/server /app/server
COPY --from=builder /app/build/target/release/scanner /app/scanner
COPY --from=builder /app/build/target/release/dworker /app/dworker
COPY --from=builder /app/build/target/release/prover /app/prover

# Copy the sqlx binary from the builder stage
COPY --from=builder /usr/local/cargo/bin/sqlx /app/sqlx
COPY migrations /app/migrations


# Expose necessary ports
EXPOSE 10002 9093 10001 20001

# Define the entry point for the container
CMD ["./main"]