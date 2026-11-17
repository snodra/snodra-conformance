<p align="center">
  <img alt="Snodra mascot" src="https://snodra.damoreira.com/assets/brand/svg/snodra.svg" width="160">
</p>

<h1 align="center">snodra-conformance</h1>

<p align="center">
  The conformance runner: proves that a Snodra client library, in any language, reaches feature level 1 against a real Snodra Server and fake hubs.
</p>

<p align="center">
  <a href="https://gitlab.damoreira.ml/snodra/snodra-conformance/-/pipelines"><img alt="pipeline" src="https://gitlab.damoreira.ml/snodra/snodra-conformance/badges/main/pipeline.svg"></a>
  <a href="https://gitlab.damoreira.ml/snodra/snodra-conformance/-/releases"><img alt="release" src="https://gitlab.damoreira.ml/snodra/snodra-conformance/-/badges/release.svg"></a>
  <img alt="Rust" src="https://img.shields.io/badge/Rust-1.99.1-000000?logo=rust&logoColor=white">
  <img alt="image" src="https://img.shields.io/badge/image-distroless-blue?logo=docker&logoColor=white">
  <a href="https://snodra.damoreira.com/"><img alt="Snodra docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

---

`snodra-conformance` proves that a Snodra client library, in any language, reaches feature level 1: it starts the
library's **conformance service** (the fixed `conformance.*` message types of the conformance contract,
`protocol/conformance.md`, contract version 1) once or more per check, connects it to a real Snodra Server and to fake
hubs of its own, and reports PASS, FAIL, SKIPPED or NOT IMPLEMENTED per check id of the Snodra feature levels
(`docs/libraries/feature-levels.md`). Levels 2 to 4 are proved by the libraries' event-store and message-store kits.

**Formerly kraken.** Snodra Conformance is the renamed `kraken-conformance`: the same checks, the same contract and
the same reports, with a new crate, binary, image and package names, new settings, and a protocol now named
`snodra.hub.v1`. See [Moving from kraken](#moving-from-kraken) for the old → new mapping.

Design, wire protocol, testing and repository conventions: the [Snodra docs](https://snodra.damoreira.com/).

## Table of contents

- [What is here](#what-is-here)
- [How it relates to the server and the libraries](#how-it-relates-to-the-server-and-the-libraries)
- [Running it against your library](#running-it-against-your-library)
- [Options](#options)
- [Build and test](#build-and-test)
- [The library dependency](#the-library-dependency)
- [hub.proto](#hubproto)
- [Image](#image)
- [Layout](#layout)
- [Moving from kraken](#moving-from-kraken)
- [CI/CD pipeline](#cicd-pipeline)
- [License](#license)

## What is here

One Cargo workspace, three packages:

- the runner `snodra-conformance` (binary `snodra-conformance`, at the root): the checks, the fake hubs, a raw hub
  client and a token issuer of its own; it speaks hub.proto in raw frames and links no Snodra library, so it can
  observe and violate the protocol where a check needs it;
- `reference-service/`, the reference conformance service on snodra-messaging-rs (binary
  `conformance-reference-service`): the runner's own fixture and the example for library authors; it reports the
  server's latest refusal through the library's API (`SnodraMessaging::latest_rejection()`);
- `violator/`, a raw-frame client that breaks one protocol rule per run, for the runner's own tests.

## How it relates to the server and the libraries

- **It depends on the server; the server does not depend on it.** The runner compiles the server's protocol (a vendored
  copy of snodra-server's `proto/hub.proto`, see [hub.proto](#hubproto)) and its tests run the server's released image
  (`SNODRA_SERVER_IMAGE`); it never builds the server from source. The server's repository holds no code of the runner.
- **The reference service depends on snodra-messaging-rs** (see [The library dependency](#the-library-dependency)); the
  runner and the violator do not.
- **A library under test depends on nothing here**: its conformance service is a program of its own that the runner
  starts.
- It was developed inside the kraken-hub repository (`conformance/`) and extracted at kraken-hub commit `d869416`.

## Running it against your library

What a library author needs, in order (the full guide is the Snodra docs, `docs/libraries/conformance.md`):

1. **Write the conformance service** with your library's API: a program that handles the `conformance.*` types of the
   conformance contract (`protocol/conformance.md`), reads its configuration from the `CONFORMANCE_*` environment
   variables (the hubs, the service name and instance id, the capacity, the retry policy, credentials), registers its
   handlers and stops on SIGTERM within 5 s. Examples: `reference-service/` here (Rust), and the Kotlin library's
   `examples/conformance-service` in snodra-messaging.
2. **Start one or two servers** from the server's image, plain text and access control off:

   ```sh
   docker run -d --rm -p 7000:7000 -e SNODRA_SERVER_OTLP_ENDPOINT= harbor.damoreira.ml/snodra/snodra-server:latest
   docker run -d --rm -p 7100:7000 -e SNODRA_SERVER_OTLP_ENDPOINT= harbor.damoreira.ml/snodra/snodra-server:latest
   ```

3. **Get the runner**: build it here (`cargo build --release -p snodra-conformance`, the binary is
   `target/release/snodra-conformance`) or take the image `harbor.damoreira.ml/snodra/snodra-conformance` (see
   [Image](#image)).
4. **Run it** on the machine (or in the container) where your service runs: the fake hubs and the runner's token
   issuer listen on `127.0.0.1`.

   ```sh
   snodra-conformance run \
     --service-cmd "/path/to/your-conformance-service --some-flag" \
     --service conformance-mylib \
     --hub 127.0.0.1:7000 --hub 127.0.0.1:7100 \
     --report text
   snodra-conformance list   # the checks and what each needs
   ```

5. **Read the report**: one line per check id, the contract and runner versions, and whether the level is reached
   (every check of it ran and passed). The exit code is 0 only when no check failed and none is not implemented.
6. **Get listed**: attach the text report of a full run (no `--only`) against the current server image to the request
   that lists your library (the Snodra docs, `docs/libraries/conformance.md`, "Getting listed").

The Rust reference service, built here:

```sh
export CARGO_NET_GIT_FETCH_WITH_CLI=true
cargo build --workspace
./target/debug/snodra-conformance run \
  --service-cmd "$PWD/target/debug/conformance-reference-service" \
  --service conformance-rust --hub 127.0.0.1:7000 --hub 127.0.0.1:7100
```

A pre-flight start must register on a fake hub and stop on SIGTERM within 5 s before any check runs. Each instance runs
in its own process group, stopped with SIGTERM (a kill 5 s later) when its check ends. Checks of the library's
behaviour towards the server (handshake, refusals, heartbeats, a silent or restarting server, permits, credits,
failover, credentials, TLS, mutual TLS) run on fake hubs inside the runner, so they need no server control. How the fake
hubs differ from snodra-server on purpose is listed in the Snodra docs (`docs/libraries/conformance.md`).

## Options

| Option | Meaning |
|---|---|
| `--service-cmd` | starts one instance of the conformance service, with the `CONFORMANCE_*` environment; no shell: words split at blanks, `'...'`/`"..."` quoting, `\` escapes |
| `--service-env` | `NAME=VALUE`, more environment for every instance |
| `--service` | its service name, `conformance-<library>` |
| `--hub` | a real server, `host:port`; without one the four checks that need it are SKIPPED; twice for the two-hub part of `L1-HUBS` |
| `--only`, `--level`, `--report text\|json`, `--work-dir`, `--check-timeout`, `--startup-timeout` | some ids (a partial run, no level claimed); level 1; the report's format; where each instance's log goes; the bounds of a check (180 s) and of a start (30 s) |

## Build and test

Requires Rust 1.99.1 (`rust-toolchain.toml` pins it), `protoc`, ssh access to snodra-messaging-rs for the reference
service (see [The library dependency](#the-library-dependency)) and, for two of the tests, Docker.

```sh
export CARGO_NET_GIT_FETCH_WITH_CLI=true
cargo build --workspace
cargo test --workspace -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
bash scripts/check-proto-sync.sh
```

- unit tests in `src/`;
- `tests/mutations.rs` (no Docker, about 30 minutes): the reference service broken at its contract boundary
  (`CONFORMANCE_REFERENCE_MUTATION`) and the violator breaking one protocol rule (`CONFORMANCE_VIOLATION`); each runs
  every check of its group and must fail the check that proves the rule, with the rule in its message, while every
  other check of the group keeps its verdict without the mutation (a few declared exceptions: a rule other checks
  rest on); the matrix is printed with `--nocapture`;
- `tests/consistency.rs` (Docker): one script of frames against a fake hub and against a server container (the
  handshake, heartbeats, refusals with their status messages, the server's reserved instance prefix and service name
  among them, permits and queueing, a later Register, Cancel, a half-close) must give the same answers, so the fake hub
  cannot drift from the server;
- `tests/reference.rs` (Docker, `rust_reference_passes_level_1`): the whole level against a server container, then
  `L1-HUBS` on two; it asserts exactly the known failures of the pinned library (`KNOWN_FAILURES`, none at the pinned
  commit: the Rust reference passes all 33 checks and reaches level 1).

The server of the Docker tests is a container of the server's image: `SNODRA_SERVER_IMAGE`, default `snodra-server:dev`
(`tests/support/mod.rs`). CI sets it to the server's latest release.

## The library dependency

The reference service builds on snodra-messaging-rs as a git dependency pinned to the library's release tag
(`tag = "v0.6.0"`), named by the ssh URL
`ssh://git@local.gitlab.damoreira.ml/snodra/snodra-messaging-rs.git`. The repository is private: the ssh URL works
unchanged on a developer machine with ssh access, where an https URL would need a token on every machine; CI, which
has a deploy token and no ssh key, rewrites it to https (see [CI/CD pipeline](#cicd-pipeline)), and `Cargo.lock`
records the ssh URL either way. Cargo's built-in git client reads ssh keys only from an agent, so fetch with the git
CLI: `CARGO_NET_GIT_FETCH_WITH_CLI=true` (or `net.git-fetch-with-cli = true` in your cargo configuration). The
workspace's lock file names the library, so building the runner alone fetches it too.

## hub.proto

`proto/hub.proto` is a byte-identical copy of snodra-server's `proto/hub.proto`, the source of truth, compiled by
`build.rs`. Change the protocol in the server repository first, then copy it here. `scripts/check-proto-sync.sh`
compares the two by SHA-256: the server copy is `$SNODRA_PROTO_SOURCE`, else `../snodra-server/proto/hub.proto` next to
this repository; without one the check is skipped, unless `SNODRA_PROTO_SYNC_REQUIRED=true` (the CI job, which clones
the public server repository anonymously).

## Image

The image (distroless like the server's, no shell) carries the runner binary; `--target with-reference` adds the
reference service, to try the image end to end:

```sh
docker buildx build --ssh default -t snodra-conformance .
docker buildx build --ssh default --target with-reference -t snodra-conformance:reference .
docker run --rm snodra-conformance:reference run \
  --service-cmd /app/conformance-reference-service --service conformance-rust
```

The workspace's lock file names the library even for the runner alone, so every build fetches it: `--ssh default`
with an ssh agent holding a key that can read snodra-messaging-rs, or `--secret id=gitconfig,env=SNODRA_GITCONFIG`
with a git configuration that rewrites its ssh URL:

```sh
export SNODRA_GITCONFIG="$(printf '[url "https://%s:%s@gitlab.damoreira.ml/snodra/snodra-messaging-rs.git"]\n\tinsteadOf = ssh://git@local.gitlab.damoreira.ml/snodra/snodra-messaging-rs.git\n' "$GITLAB_USER" "$GITLAB_TOKEN")"
docker buildx build --secret id=gitconfig,env=SNODRA_GITCONFIG -t snodra-conformance .
```

Neither reaches a layer (the build container accepts the gitlab host key on first use). Run the plain image with a
`--service-cmd` that names a program inside it (mount your service, or build an image `FROM` it), or copy
`/app/snodra-conformance` into an image that can run your service. CI publishes the plain image with every release of
this repository: `harbor.damoreira.ml/snodra/snodra-conformance:X.Y.Z` and `:latest`.

## Layout

```
src/lib.rs          crate root, the generated proto module
src/main.rs         the CLI: run, list
src/checks/         the level-1 checks: connection, requests, modes, credentials, hubs
src/fake_hub.rs     the fake hub (behaves as snodra-server where a check relies on it)
src/peer.rs         a raw client of a real server: the runner's own instance there
src/sut.rs          starting and stopping the service under test
src/identity.rs     the runner as identity provider: RSA key, JWKS, caller tokens, client credentials
src/rendezvous.rs   the hub choice of a keyed request (the rendezvous score)
src/report.rs       verdicts, the level reached, the text and JSON reports
src/wire.rs         building and reading hub.proto messages
reference-service/  the Rust reference conformance service on snodra-messaging-rs
violator/           the raw-frame client that breaks one rule per run
tests/              mutations.rs, consistency.rs, reference.rs (helpers in tests/support/)
proto/hub.proto     the protocol, a copy of snodra-server's proto/hub.proto
scripts/            check-proto-sync.sh
Dockerfile          the image (runner; --target with-reference adds the reference service)
```

## Moving from kraken

v0.5.1 was the last release under the old name (`kraken-conformance`). This is a clean cut: there are no aliases, and
the server and the libraries must move with it — the protocol package changed (`kraken.hub.v1` → `snodra.hub.v1`), so
the vendored `proto/hub.proto` and the frames the runner speaks changed with it.

1. **Names.** The crate, binary and image are `snodra-conformance`; the library is `snodra_conformance`. The workspace
   packages are `snodra-conformance`, `snodra-conformance-reference` (binary still
   `conformance-reference-service`) and `snodra-conformance-violator` (binary `conformance-violator`). The repository is
   `gitlab.damoreira.ml/snodra/snodra-conformance` and the image
   `harbor.damoreira.ml/snodra/snodra-conformance`.

2. **Environment.** Every `KRAKEN_*` variable is `SNODRA_*`; the hub client's `KRAKEN_HUB_*` are the server's own
   names, `SNODRA_SERVER_*` (there is no alias):

   | v0.5.1 | Now |
   |---|---|
   | `KRAKEN_HUB_IMAGE` (test server image) | `SNODRA_SERVER_IMAGE` (default `snodra-server:dev`) |
   | `KRAKEN_HUB_OTLP_ENDPOINT` | `SNODRA_SERVER_OTLP_ENDPOINT` |
   | `KRAKEN_PROTO_SOURCE`, `KRAKEN_PROTO_SYNC_REQUIRED` | `SNODRA_PROTO_SOURCE`, `SNODRA_PROTO_SYNC_REQUIRED` (default `../snodra-server/proto/hub.proto`) |
   | `KRAKEN_CONFORMANCE_KEEP_LOGS` | `SNODRA_CONFORMANCE_KEEP_LOGS` |
   | `KRAKEN_GITCONFIG` | `SNODRA_GITCONFIG` |
   | `KRAKEN_MESSAGING_RS_DEPLOY_USER`, `_TOKEN` | `SNODRA_MESSAGING_RS_DEPLOY_USER`, `_TOKEN` |

   The `CONFORMANCE_*` variables and the `conformance.*` message types belong to the contract: they are unchanged.

3. **Wire.** The protocol package is `snodra.hub.v1` (from the server's `proto/hub.proto`); the mTLS SAN scheme is
   `snodra://service/<name>`; a server's own request sessions use the instance-id prefix `snodra-server/` and the
   service name `snodra-server`.

4. **The protocol's source of truth.** `proto/hub.proto` is now copied from snodra-server (`proto/hub.proto`) instead
   of the umbrella repository. `scripts/check-proto-sync.sh` defaults to `../snodra-server/proto/hub.proto`, and the
   CI `proto-sync` job clones the public `https://gitlab.damoreira.ml/snodra/snodra-server.git` anonymously (no
   deploy token).

5. **Kept as they are:** the event source/topic `kraken.events` and every stored record — the runner never reads the
   event store, and the libraries keep the topic, so existing data needs no migration.

## CI/CD pipeline

Defined in [`.gitlab-ci.yml`](.gitlab-ci.yml); a pipeline runs for every merge request and for every push to `main`,
nothing else. The runners use the shell executor (Rust, `protoc`, Docker and buildx installed on the host): no job
image, no services; the Docker tests talk to the host's Docker daemon (`TESTCONTAINERS_RYUK_DISABLED=true`).

| Stage | Job | Runs | Produces |
|---|---|---|---|
| check | `fmt` | always | `cargo fmt --all -- --check` |
| check | `proto-sync` | always | `scripts/check-proto-sync.sh` against a fresh anonymous clone of snodra-server: `proto/hub.proto` must match `proto/hub.proto` byte for byte |
| check | `clippy` | always | `cargo clippy --workspace --all-targets -- -D warnings` |
| test | `test` | always | logs in to Harbor, pulls `SNODRA_SERVER_IMAGE` (the server's latest release), then `cargo test --workspace -- --test-threads=1` |
| release | `release` | pushes to `main` only, one at a time | the next version and its release (below) |

The cargo jobs fetch snodra-messaging-rs over https with a deploy token: they extend `.snodra-messaging-rs-fetch`,
which sets `CARGO_NET_GIT_FETCH_WITH_CLI=true` and puts git's
`url.https://<user>:<token>@$CI_SERVER_HOST/snodra/snodra-messaging-rs.git.insteadOf <ssh URL>` in the job's
environment only (`GIT_CONFIG_COUNT`, git 2.31 or later on the runner), so nothing is written to the shell runner's
git configuration and no command line shows the token. `proto-sync` clones the public server repository anonymously,
as the server's and the libraries' `proto-sync` jobs do.

`release` computes the semantic-version bump from the commits since the latest `v*` tag: `BREAKING CHANGE` or a
`type!:` subject is major, a `feat` subject minor, anything else patch (`SEMVER_BUMP` or `RELEASE_VERSION` override
it; nothing to do when `HEAD` is already tagged or no commit is new; `DRY_RUN=true` stops before publishing). Without
a tag it starts from `v0.0.0`, so the first release is `v0.1.0`. It first builds the runner image, with the deploy
token given to BuildKit as the secret `gitconfig` (a git configuration with that rewrite: never a layer, never a
command line); a failed build stops the release before anything is published. It then tags `vX.Y.Z` and pushes the
tag, pushes `<harbor>/<project>/snodra-conformance:X.Y.Z` and `:latest`, and creates the GitLab release. A failed push
after the tag is not retried by a rerun (`HEAD` is then tagged): push the image by hand or release again with
`RELEASE_VERSION`.

CI/CD variables (masked where secret, visible to this project as group or project variables):

| Variable | Used by | Purpose |
|---|---|---|
| `SNODRA_MESSAGING_RS_DEPLOY_USER`, `SNODRA_MESSAGING_RS_DEPLOY_TOKEN` | cargo jobs, `release` | a deploy token with `read_repository` on snodra-messaging-rs |
| `SNODRA_SERVER_IMAGE` | `test` | the server image the Docker tests run; the group variable the server's release sets |
| `HARBOR_HOST`, `HARBOR_PROJECT`, `HARBOR_USERNAME`, `HARBOR_PASSWORD` | `test`, `release` | the registry: pull the server image, push this image |

## License

MIT, see [LICENSE](LICENSE).
