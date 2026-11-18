# syntax=docker/dockerfile:1
# snodra-conformance (the conformance runner), from the repository root:
#   docker buildx build --ssh default -t snodra-conformance .
# The default image carries the runner only. `--target with-reference` adds the Rust reference
# conformance service (/app/conformance-reference-service), to try the image end to end.
# The reference service's snodra-messaging-rs is a git dependency named by its ssh URL
# (reference-service/Cargo.toml); the workspace's lock file needs it even for the runner alone.
# The builder fetches it with the git CLI; the credentials reach only the RUNs that fetch, never a
# layer: `--ssh default` with an ssh agent holding a key that can read the repository, or (CI) a
# git configuration as the secret `gitconfig` whose url.<https URL with a deploy token>.insteadOf
# rewrites the ssh URL (`--secret id=gitconfig,env=SNODRA_GITCONFIG`).
FROM rust:1.99-bookworm AS builder
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
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
# The build container has no known_hosts: the gitlab host key is accepted on first use.
ENV GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new"
RUN --mount=type=ssh \
    --mount=type=secret,id=gitconfig,target=/root/.gitconfig \
    cargo build --release --locked -p snodra-conformance --bin snodra-conformance

FROM builder AS reference-builder
RUN --mount=type=ssh \
    --mount=type=secret,id=gitconfig,target=/root/.gitconfig \
    cargo build --release --locked -p snodra-conformance-reference

# The runner and the reference service: `run --service-cmd /app/conformance-reference-service`.
FROM gcr.io/distroless/cc-debian12:nonroot AS with-reference
WORKDIR /app
COPY --from=builder /app/target/release/snodra-conformance /app/snodra-conformance
COPY --from=reference-builder /app/target/release/conformance-reference-service /app/conformance-reference-service
USER nonroot
ENTRYPOINT ["/app/snodra-conformance"]

# The runner binary on distroless (no shell: the runner starts the service command without one).
# Run it as is when --service-cmd names a program in the image (mount it, or build FROM this
# image), or copy /app/snodra-conformance into an image that can run your conformance service.
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/target/release/snodra-conformance /app/snodra-conformance
USER nonroot
ENTRYPOINT ["/app/snodra-conformance"]
