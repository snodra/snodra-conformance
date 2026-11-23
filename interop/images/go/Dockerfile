# syntax=docker/dockerfile:1
# The Go conformance service of snodra-messaging-go (examples/conformance-service).
# The source arrives as the named build context `gosrc` (compose.yaml): a pinned commit of
# https://github.com/snodra/snodra-messaging-go, or a local checkout (GO_SRC).
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY --from=gosrc . .
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -o /conformance-service ./examples/conformance-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /conformance-service /conformance-service
ENTRYPOINT ["/conformance-service"]
