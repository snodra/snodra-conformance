//! The reference service against real hubs (`snodra-server:dev` by default, `SNODRA_SERVER_IMAGE`
//! to change it) and the runner's fake hubs: the whole level on one hub, then the two-hub part of
//! L1-HUBS on two. Needs Docker.
//!
//! It asserts exactly the known failures of the pinned library, so a gap stays visible and a fix
//! (or a new failure) changes the test.

mod support;

use snodra_conformance::report::{Report, Status};

/// Checks the reference service fails at the pinned library, with the reason (none at
/// snodra-messaging-rs v0.5.0).
const KNOWN_FAILURES: &[(&str, &str)] = &[];

fn expect(verdicts: &[snodra_conformance::report::Verdict]) -> Vec<String> {
    let mut wrong = Vec::new();
    for v in verdicts {
        let expected = if KNOWN_FAILURES.iter().any(|(id, _)| *id == v.id) {
            Status::Fail
        } else {
            Status::Pass
        };
        if v.status != expected {
            wrong.push(format!(
                "{} {:?} (expected {expected:?}): {}",
                v.id, v.status, v.detail
            ));
        }
    }
    wrong
}

#[tokio::test(flavor = "multi_thread")]
async fn rust_reference_passes_level_1() {
    support::reference_service();
    // One hub: every check (L1-HUBS on fake hubs only).
    let (h1, a) = support::hub_container().await;
    let verdicts = support::run(&[], &[], vec![a.clone()]).await;
    println!(
        "{}",
        Report::new("conformance-rust", 1, false, verdicts.clone()).text()
    );
    let mut wrong = expect(&verdicts);
    // Two hubs: L1-HUBS with its part on two real hubs.
    let (_h2, b) = support::hub_container().await;
    let two = support::run(&["L1-HUBS"], &[], vec![a, b]).await;
    println!(
        "{}",
        Report::new("conformance-rust", 1, true, two.clone()).text()
    );
    if !support::verdict(&two, "L1-HUBS")
        .detail
        .contains("registered on both real hubs")
    {
        wrong.push("L1-HUBS did not run its two-hub part".into());
    }
    wrong.extend(expect(&two));
    drop(h1);
    assert!(wrong.is_empty(), "{wrong:#?}");
}
