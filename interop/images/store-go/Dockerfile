# syntax=docker/dockerfile:1
# The Go store-interop probe (examples/store-interop). The source arrives as the named build
# context `gosrc` (compose.yaml): the branch or tag of https://github.com/snodra/snodra-messaging-go,
# or a local checkout (GO_SRC). The example is a module of the repository's workspace.
# Base images are pinned by digest (multi-architecture index digests), as the Python probe's are.
FROM golang:1.26-bookworm@sha256:ea47ee45f1284d60f9cc09cb2c3bb4f508c256b640bd8bb30342ff13d1adcb52 AS build
WORKDIR /src
COPY --from=gosrc . .
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath -o /store-interop ./examples/store-interop

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /store-interop /store-interop
ENTRYPOINT ["/store-interop"]
