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
  <a href="https://snodra.damoreira.com/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
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
  credentials (token, JWT, client certificates, TLS), and routing, permits and failover over two hubs.
- **Real server checks**: an idle instance stays registered, commands and point queries through the
  hub with a routing key, shared query types, scatter-gather, and the topology of two real hubs.
- **Mutation matrix**: deliberately broken services (the reference service broken at its contract
  boundary, and `violator/`, a client that breaks one protocol rule at a time) each make the check that
  proves the broken rule fail, and leave the other checks of its group with their usual verdict
  (`tests/mutations.rs`).
- **Any language**: the contract is `conformance.*` messages, so a new client is tested the same way.
- **Reports**: one line per check id, a machine-readable summary, and a non-zero exit on any failure.

Levels 2 to 4 are proved by the libraries' own event-store and message-store conformance kits.

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

The checks and the feature levels they prove: **<https://snodra.damoreira.com/library-authors/conformance/>**
and <https://snodra.damoreira.com/library-authors/feature-levels/>. The runner's page among the tools:
<https://snodra.damoreira.com/tools/conformance-runner/>.

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow. A new check needs a
feature-level entry and a mutation where the protocol allows one.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
