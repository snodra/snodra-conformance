//! `snodra-conformance`: proves a client library's feature level (the conformance contract,
//! `protocol/conformance.md`; the Snodra docs: <https://snodra.damoreira.com/>).
//!
//! The runner starts the library's conformance service once per check (`--service-cmd`, with the
//! `CONFORMANCE_*` environment) and connects it to a real hub (`--hub`) or to fake hubs it runs
//! itself, then drives it with requests and reads what it does: the frames it writes (a fake hub
//! sees every one), the answers it gives and what it reports through the `conformance.*` types.

pub mod proto {
    tonic::include_proto!("snodra.hub.v1");
}

pub mod checks;
pub mod fake_hub;
pub mod identity;
pub mod peer;
pub mod rendezvous;
pub mod report;
pub mod sut;
pub mod wire;

/// The version of the conformance contract the runner checks (`protocol/conformance.md`).
pub const CONFORMANCE_VERSION: u32 = 1;
/// The runner's own version.
pub const RUNNER_VERSION: &str = env!("CARGO_PKG_VERSION");
