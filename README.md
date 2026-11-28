<p align="center">
  <img alt="Snodra Conformance mascot" src="docs/assets/brand/svg/snodra-conformance.svg" width="160">
</p>

<h1 align="center">Snodra Conformance</h1>

<p align="center">
  The runner that proves a Snodra client library, in any language, reaches feature level 1 against a real server and fake hubs.
</p>

<p align="center">
  <a href="https://github.com/snodra/snodra-conformance/actions/workflows/ci.yml"><img alt="ci" src="https://github.com/snodra/snodra-conformance/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/snodra/snodra-conformance/releases"><img alt="release" src="https://img.shields.io/github/v/release/snodra/snodra-conformance"></a>
  <img alt="Rust" src="https://img.shields.io/badge/Rust-1.99.1-000000?logo=rust&logoColor=white">
  <a href="https://snodra.damoreira.com/docs/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

---

## What it is

A protocol, not a library, is what makes a Kotlin service and a Rust service interchangeable. This
runner is how a client library proves it implements that protocol.

It starts the library's **conformance service** — the fixed `conformance.*` message types of the
conformance contract — connects it to a real Snodra Server and to fake hubs of its own, and reports
**PASS**, **FAIL**, **SKIPPED** or **NOT IMPLEMENTED** per check id of the Snodra feature levels.

- **Fake hubs** prove most checks, scripting what a healthy server rarely or never produces: the
  handshake and its refusals, heartbeats, silent hubs and reconnects, timeouts, every error code,
  never-sent and unknown outcomes, retries, permits, streaming and subscription credit, lost streams,
  credentials (token, JWT, client certificates, TLS), and routing, permits and failover over two hubs
  (the same request id and the caller's full timeout on every hop).
- **Real server checks**: an idle instance stays registered, commands and point queries through the
  hub with a routing key, shared query types, scatter-gather, and the topology of two real hubs.
- **Mutation matrix**: deliberately broken services (the reference service broken at its contract
  boundary, and `violator/`, a client that breaks one protocol rule at a time) each make the check that
  proves the broken rule fail, and leave the other checks of its group with their usual verdict
  (`tests/mutations.rs`).
- **Any language**: the contract is `conformance.*` messages, so a new client is tested the same way.
- **Reports**: one line per check id, a machine-readable summary, and a non-zero exit on any failure.

Levels 2 to 4 are proved by the libraries' own event-store and message-store conformance kits.

## Cross-language interoperability

`interop/` proves the libraries **against each other** on one real hub: one conformance service per
language for the routing matrix, and a `stores` profile with one probe per language for the shared
Kafka topic, RabbitMQ super stream, PostgreSQL, MySQL and MongoDB. The stores profile also drives the
hub's dashboard HTTP API against the two services: pause and resume, a leased reset with its replay
ranges and positions, dead-letter retry (including a letter one language parks and the other retries),
`retry-all` and discard, and the event-store dead-letter mode. Its saga scenarios continue one saga
across the languages on shared PostgreSQL and MongoDB, prove the dispatch lease exclusive across
them, fire a deadline scheduled by one in the other, and deliver a command-failure notice in the
other language locally. The stores profile runs the released tags, which carry the level-4 probe and
its saga commands; `GO_SRC` and `PYTHON_SRC` run a local checkout instead. See
[interop/README.md](interop/README.md) for the command, the scenarios and the exact assertions.

## Run it

The runner has two subcommands: `run` (the checks) and `list` (the check ids and what each needs).
`run` needs `--service-cmd`, the command that starts one instance of your conformance service. Add
`--hub host:port` (twice for the two-hub check) for a real hub: without one, the checks that need a real hub
are skipped, and a skipped check leaves the level unproved.

From a checkout, against a hub on `127.0.0.1:7000`:

```sh
cargo run --release -- run --hub 127.0.0.1:7000 --service-cmd "/path/to/conformance-service"
cargo run --release -- list
```

Or build once with `cargo build --release` and run `target/release/snodra-conformance run ...`.
`run --help` lists every option. Every check starts its own service process and cleans up.

Each release also publishes binaries for Linux and macOS on the
[releases page](https://github.com/snodra/snodra-conformance/releases) and the container image
`snodra/snodra-conformance` (`docker pull snodra/snodra-conformance`). That image
is distroless and has no shell, so with it `--service-cmd` must name a program inside the image (mount your
service, or build an image `FROM` it).

## Documentation

The conformance contract (the `conformance.*` message types a conformance service handles, and what each check does to it) is [docs/contract.md](docs/contract.md), with one JSON Schema per type in [contract/](contract/).

The checks and the feature levels they prove: **<https://snodra.damoreira.com/docs/library-authors/conformance/>**
and <https://snodra.damoreira.com/docs/library-authors/feature-levels/>. The runner's page among the tools:
<https://snodra.damoreira.com/docs/tools/conformance-runner/>.

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow. A new check needs a
feature-level entry and a mutation where the protocol allows one.

## Releases

Versions are cut by CI from the commit types. Nobody pushes a tag. When the checks pass on a push to
`main`, the subjects since the last `v*` tag decide. Any `feat` is the next minor. Otherwise any `fix`
or `perf` is the next patch. Anything else (`docs`, `test`, `chore`, `ci`, `refactor`, `build`,
`style`) releases nothing. Use `feat` and `fix` for what changes the runner's checks, the contract or the
reference service.

Before anything is tagged, CI builds the new version as the release will: every binary target and the
amd64 image (the arm64 image is first built by the release, before it pushes anything). Then it commits
`release: X.Y.Z` (the version in `Cargo.toml` and `Cargo.lock`) and tags `vX.Y.Z`. The release workflow
builds the binaries again from the tag. Once all of them are built, it pushes the image
`snodra/snodra-conformance:X.Y.Z` and checks that the image reports that version. The GitHub release
comes last, with the binaries attached. Pull before you push: `main` then carries that commit.

- Below 1.0.0, any `type!:` subject and any line starting `BREAKING CHANGE:` in a message count as a
  minor, never a major. 1.0.0 is declared by hand.
- A revert (`Revert "feat: ..."`, `revert:`) is not a release reason. The reverted change stays in the
  last published version until the next `feat`, `fix` or `perf`.
- Only tags shaped `vN.N.N` count. The highest one reachable from `main` is the last release.
- `scripts/next-version.sh` holds the rule. `for t in scripts/*.test.sh; do sh "$t"; done` tests it.
- The runner's version is not the contract's. A new contract version is a change the docs announce
  ([docs/contract.md](docs/contract.md#versions)).

If the build before the tag fails, nothing is tagged: fix it, and the next push to `main` releases the
same version. If a release fails halfway (the tag exists, some artifacts do not), use **Re-run failed
jobs** on that run. Or run the `release` workflow by hand with the existing tag. Both repeat only what is
safe to repeat: the image tags are pushed again (a new digest for the same version), and the GitHub
release is edited, not duplicated. **Re-run all jobs** on a run that released does nothing, because
`main` has moved to its release commit. If the failure needs a change to the code, that version is never
finished: the next `fix` releases the next patch.

The checks run against the hub image pinned in `.github/workflows/ci.yml`. The repository variable
`SNODRA_SERVER_VERSION`, when set, overrides it.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
