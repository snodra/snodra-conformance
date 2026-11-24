# syntax=docker/dockerfile:1
# The Rust reference conformance service of this repository (reference-service, on the
# snodra-messaging release this repository's Cargo.lock pins). Build context: the repository root.
FROM rust:1.99-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY reference-service/Cargo.toml ./reference-service/
COPY reference-service/src ./reference-service/src
COPY violator/Cargo.toml ./violator/
COPY violator/src ./violator/src
COPY interop/driver/Cargo.toml ./interop/driver/
COPY interop/driver/src ./interop/driver/src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked -p snodra-conformance-reference \
    && cp target/release/conformance-reference-service /conformance-service

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /conformance-service /conformance-service
ENTRYPOINT ["/conformance-service"]
