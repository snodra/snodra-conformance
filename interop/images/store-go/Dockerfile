# syntax=docker/dockerfile:1
# The Go store-interop probe (examples/store-interop). The source arrives as the named build
# context `gosrc` (compose.yaml): the branch or tag of https://github.com/snodra/snodra-messaging-go,
# or a local checkout (GO_SRC). The example is a module of the repository's workspace.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY --from=gosrc . .
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -o /store-interop ./examples/store-interop

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /store-interop /store-interop
ENTRYPOINT ["/store-interop"]
