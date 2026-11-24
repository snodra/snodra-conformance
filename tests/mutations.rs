//! Mutation evidence: a broken service fails exactly the check that proves the broken rule, with
//! that rule in its message. Each mutation runs every check of its group, and every other check of
//! the group must keep the verdict it has without the mutation (the baseline). Fake hubs only: no
//! Docker (the checks that need a real hub are left out of the groups; L1-HEARTBEAT is given an
//! unreachable hub, so its real-hub part fails alike in the baseline and the mutation).
//!
//! Two kinds of breakage: the reference service broken at its contract boundary
//! (`CONFORMANCE_REFERENCE_MUTATION`: what it configures or reports), and the violating client
//! (`violator/`, raw hub.proto frames) breaking one protocol rule a library owns
//! (`CONFORMANCE_VIOLATION`). The matrix is printed (`--nocapture`).

mod support;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use snodra_conformance::report::{Status, Verdict};

const CONNECTION: &[&str] = &[
    "L1-REGISTER-FIRST",
    "L1-HANDSHAKE-ACK",
    "L1-EARLY-REQUESTS",
    "L1-REJECTION",
    "L1-HEARTBEAT",
    "L1-SILENT-HUB",
    "L1-RECONNECT",
    "L1-STATUS",
];
const REQUESTS: &[&str] = &[
    "L1-REQUEST-ID",
    "L1-TIMEOUT",
    "L1-ERRORS",
    "L1-NEVER-SENT",
    "L1-RETRY",
    "L1-ARRIVAL-STREAM",
    "L1-ONE-TERMINAL",
    "L1-HANDLER-ERRORS",
    "L1-IDENTITY",
    "L1-PROPAGATION",
    "L1-PERMITS",
];
const MODES: &[&str] = &["L1-STREAM", "L1-SUBSCRIBE", "L1-LOST-STREAM"];
const CREDENTIALS: &[&str] = &["L1-TOKEN", "L1-JWT", "L1-MTLS", "L1-TLS"];
const HUBS: &[&str] = &[
    "L1-HUBS",
    "L1-HUB-ROUTING",
    "L1-HUB-PERMITS",
    "L1-HUB-FAILOVER",
];

/// (mutation, the check that must fail, a text of its failure, its group, the other checks of the
/// group the broken rule also breaks, with `"*"` for all).
type Mutation = (
    &'static str,
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
);

const REFERENCE: &[Mutation] = &[
    ("capacity", "L1-PERMITS", "offered 5 permits", REQUESTS, &[]),
    (
        "never-sent-as-unknown",
        "L1-NEVER-SENT",
        "expected never sent",
        REQUESTS,
        &[],
    ),
    (
        "ignore-timeout",
        "L1-TIMEOUT",
        "Request.timeout_ms 0 (expected 1200)",
        REQUESTS,
        &[],
    ),
    (
        "hide-rejection",
        "L1-REJECTION",
        "last rejection",
        CONNECTION,
        &[],
    ),
    (
        "no-guard",
        "L1-IDENTITY",
        "a caller the handler refuses",
        REQUESTS,
        &[],
    ),
    (
        "override-authorization",
        "L1-PROPAGATION",
        "authorization header",
        REQUESTS,
        &[],
    ),
    (
        "no-token",
        "L1-TOKEN",
        "without the token",
        CREDENTIALS,
        &[],
    ),
];

const VIOLATIONS: &[Mutation] = &[
    // A client that never sends a Register first registers nowhere (snodra-server refuses it too):
    // every check of its group fails with it, so only the target is asserted.
    (
        "heartbeat-first",
        "L1-REGISTER-FIRST",
        "expected Register",
        CONNECTION,
        &["*"],
    ),
    (
        "live-on-echo",
        "L1-HANDSHAKE-ACK",
        "reports itself registered",
        CONNECTION,
        &[],
    ),
    // The silent-hub window is three of the ack's intervals: ignoring the interval breaks it too.
    (
        "ignore-interval",
        "L1-HEARTBEAT",
        "expected the ack's interval",
        CONNECTION,
        &["L1-SILENT-HUB"],
    ),
    (
        "keep-silent",
        "L1-SILENT-HUB",
        "kept a stream without echoes",
        CONNECTION,
        &[],
    ),
    (
        "drop-silent-early",
        "L1-SILENT-HUB",
        "expected between three and four intervals",
        CONNECTION,
        &[],
    ),
    (
        "changed-register",
        "L1-RECONNECT",
        "differs from the first",
        CONNECTION,
        &[],
    ),
    ("no-backoff", "L1-RECONNECT", "no backoff", CONNECTION, &[]),
    // A report at once, then every 7 s: the extra report is allowed, the 7 s gap is not.
    (
        "rare-status",
        "L1-STATUS",
        "more than 6500 ms apart",
        CONNECTION,
        &[],
    ),
    // Two reports early in the window, then none: the silence until the window ends is too long.
    (
        "status-stops",
        "L1-STATUS",
        "without a report at the end of the window",
        CONNECTION,
        &[],
    ),
    // Without permits back the hub forwards nothing beyond the capacity: L1-REQUEST-ID's
    // sends and their controls stall too.
    (
        "no-permits",
        "L1-PERMITS",
        "did not come back",
        REQUESTS,
        &["L1-REQUEST-ID"],
    ),
    (
        "unbatched-permits",
        "L1-PERMITS",
        "batched: 50 ms",
        REQUESTS,
        &[],
    ),
    (
        "no-signature-check",
        "L1-IDENTITY",
        "a key the JWKS does not publish",
        REQUESTS,
        &[],
    ),
    (
        "token-after-lookup",
        "L1-IDENTITY",
        "the token is checked first",
        REQUESTS,
        &[],
    ),
    (
        "retry-after-timeout",
        "L1-RETRY",
        "answered TIMEOUT",
        REQUESTS,
        &[],
    ),
    (
        "case-sensitive-headers",
        "L1-PROPAGATION",
        "header came back as sent",
        REQUESTS,
        &[],
    ),
    ("seq-from-zero", "L1-STREAM", "parts numbered", MODES, &[]),
    (
        "keep-subscriptions",
        "L1-LOST-STREAM",
        "lives on",
        MODES,
        &[],
    ),
    (
        "no-subscription-limit",
        "L1-SUBSCRIBE",
        "was not refused",
        MODES,
        &[],
    ),
    (
        "no-subscription-types",
        "L1-SUBSCRIBE",
        "Register.subscription_query_types",
        MODES,
        &[],
    ),
    (
        "no-tls-verify",
        "L1-TLS",
        "TLS not verified",
        CREDENTIALS,
        &[],
    ),
    ("renew-late", "L1-JWT", "had expired", CREDENTIALS, &[]),
    (
        "mtls-no-reload",
        "L1-MTLS",
        "renewed certificate was not presented",
        CREDENTIALS,
        &[],
    ),
    (
        "signed-rendezvous",
        "L1-HUB-ROUTING",
        "unsigned scores",
        HUBS,
        &[],
    ),
    (
        "sticky-unkeyed",
        "L1-HUB-ROUTING",
        "expected round-robin",
        HUBS,
        &[],
    ),
];

const UNREACHABLE_HUB: &str = "127.0.0.1:9";

async fn group(service: &Path, var: &str, value: &str, ids: &[&str]) -> Vec<Verdict> {
    let env: Vec<(&str, &str)> = if value.is_empty() {
        Vec::new()
    } else {
        vec![(var, value)]
    };
    support::run_with(
        service.to_path_buf(),
        ids,
        &env,
        vec![UNREACHABLE_HUB.to_string()],
    )
    .await
}

async fn matrix(service: PathBuf, var: &str, mutations: &[Mutation]) -> Vec<String> {
    let mut baselines: HashMap<&str, Vec<Verdict>> = HashMap::new();
    let mut wrong = Vec::new();
    for (name, target, text, ids, also) in mutations {
        let key = ids[0];
        if !baselines.contains_key(key) {
            let b = group(&service, var, "", ids).await;
            for v in &b {
                println!("baseline {:<24}{:<20}{:?}", key, v.id, v.status);
                if v.status != Status::Pass {
                    println!("baseline {:<24}{:<20}{}", key, v.id, v.detail);
                }
            }
            baselines.insert(key, b);
        }
        let baseline = &baselines[key];
        let got = group(&service, var, name, ids).await;
        let t = support::verdict(&got, target);
        let b = support::verdict(baseline, target);
        if t.status != Status::Fail || !t.detail.contains(text) {
            wrong.push(format!(
                "{name}: {target} {:?} {:?} (expected FAIL with {text:?})",
                t.status, t.detail
            ));
        }
        if b.detail.contains(text) {
            wrong.push(format!(
                "{name}: the baseline already fails {target} with {text:?}"
            ));
        }
        let mut changed = Vec::new();
        for v in got.iter().filter(|v| v.id != *target) {
            let base = support::verdict(baseline, &v.id);
            if v.status != base.status {
                changed.push(format!("{} {:?}->{:?}", v.id, base.status, v.status));
                println!(
                    "MATRIX {name:<24} {:<18} {:?}: {}",
                    v.id, v.status, v.detail
                );
            }
        }
        println!(
            "MATRIX {name:<24} {target:<18} {:?} | {} others of the group: {}",
            t.status,
            got.len() - 1,
            if changed.is_empty() {
                "all as the baseline".to_string()
            } else {
                format!("{} (declared: {also:?})", changed.join(", "))
            }
        );
        let unexpected: Vec<&String> = changed
            .iter()
            .filter(|c| {
                !also.contains(&"*") && !also.iter().any(|a| c.starts_with(&format!("{a} ")))
            })
            .collect();
        if !unexpected.is_empty() {
            wrong.push(format!("{name}: other checks changed: {unexpected:?}"));
        }
    }
    wrong
}

#[tokio::test(flavor = "multi_thread")]
async fn each_reference_mutation_fails_exactly_its_check() {
    let wrong = matrix(
        support::reference_service(),
        "CONFORMANCE_REFERENCE_MUTATION",
        REFERENCE,
    )
    .await;
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn each_protocol_violation_fails_exactly_its_check() {
    let wrong = matrix(support::violator(), "CONFORMANCE_VIOLATION", VIOLATIONS).await;
    assert!(wrong.is_empty(), "{wrong:#?}");
}
