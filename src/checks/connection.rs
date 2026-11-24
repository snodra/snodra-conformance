//! One stream: registration, handshake, refusals, heartbeats, reconnect, status.
//!
//! Every time bound below names the rule of the Snodra protocol docs
//! (`docs/libraries/protocol.md`) it comes from, with the tolerance added for scheduling (a cold
//! or busy service must pass).

use std::time::{Duration, Instant};

use serde_json::json;
use tonic::{Code, Status};

use super::{CheckResult, OrFail, Problem, Run, ensure, ms, scripted, up};
use crate::fake_hub::{AckMode, FakeHub, Mode, Session};
use crate::peer::{self, Peer, Registration};
use crate::proto::client_message::Message as M;
use crate::proto::{InstanceStatus, RequestKind};
use crate::wire;

/// The first two frames of `s`: a `Register` (service name, instance id, permits) then a
/// `Heartbeat`.
async fn opening(s: &Session) -> Result<(), Problem> {
    let frames = s
        .until(Duration::from_secs(10), || {
            let f = s.frames();
            (f.len() >= 2 || (s.is_closed() && !f.is_empty())).then_some(f)
        })
        .await
        .or_fail(&format!(
            "stream {}: fewer than two frames in 10 s",
            s.index
        ))?;
    match &frames[0].1.message {
        Some(M::Register(r)) => {
            ensure!(
                !r.service_name.is_empty() && !r.instance_id.is_empty() && r.initial_permits >= 1,
                "stream {}: Register with service_name {:?}, instance_id {:?}, initial_permits {}",
                s.index,
                r.service_name,
                r.instance_id,
                r.initial_permits
            );
        }
        _ => {
            return Err(Problem::Fail(format!(
                "stream {}: first frame {} (expected Register)",
                s.index,
                wire::describe(&frames[0].1)
            )));
        }
    }
    ensure!(
        frames.len() >= 2 && matches!(frames[1].1.message, Some(M::Heartbeat(_))),
        "stream {}: second frame {} (expected Heartbeat)",
        s.index,
        frames
            .get(1)
            .map(|f| wire::describe(&f.1))
            .unwrap_or_else(|| "none".into())
    );
    Ok(())
}

pub async fn register_first(run: Run) -> CheckResult {
    let fake = FakeHub::start("fake-hub-1").await;
    let sut = run.launch(&run.spec(vec![fake.address()]))?;
    let s0 = run.registered(&sut, &fake, 0).await;
    let s0 = match s0 {
        Ok(s) => s,
        // A first frame that is not a Register: the session exists, without one.
        Err(e) => fake.sessions().first().cloned().ok_or(e)?,
    };
    opening(&s0).await?;
    s0.close(Status::unavailable("conformance: stream dropped"));
    let s1 = up(
        &sut,
        "no new stream within 15 s of the hub dropping the first",
        fake.session(1, Duration::from_secs(15)),
    )
    .await?;
    opening(&s1).await?;
    Ok("two streams (first, and after a drop), each opened with Register then Heartbeat".into())
}

/// The hub ids the service reports registered (`conformance.View`).
async fn view_hubs(s: &Session) -> Result<Vec<String>, Problem> {
    let v = s
        .query("conformance.View", json!({}))
        .await
        .or_fail("conformance.View")?;
    Ok(v["hubIds"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|h| h.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default())
}

pub async fn handshake_ack(run: Run) -> CheckResult {
    let fake = FakeHub::start("fake-hub-1").await;
    scripted(&fake);
    fake.set_ack_mode(AckMode::Never);
    // The check's own deviation from snodra-server: echoes without an ack ("whatever echoes it
    // got").
    fake.echo_before_ack(true);
    let sut = run.launch(&run.spec(vec![fake.address()]))?;
    let s0 = run.registered(&sut, &fake, 0).await?;
    // The opening heartbeat (L1-REGISTER-FIRST) and its echo: no interval rule applies before the
    // ack, so one is all a library must send; wait a little for its echo to be handled.
    s0.until(Duration::from_secs(8), || {
        (!s0.select(|m| matches!(m.message, Some(M::Heartbeat(_))).then_some(()))
            .is_empty())
        .then_some(())
    })
    .await
    .or_fail("no heartbeat on the unacknowledged stream in 8 s")?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let hubs = view_hubs(&s0).await?;
    ensure!(
        hubs.is_empty(),
        "without a RegisterAck (heartbeat echoes only) the service reports itself registered on {hubs:?}"
    );
    let out = super::outcome(
        s0.command(
            "conformance.Send",
            json!({ "op": "unary", "target": "probe", "timeoutMs": 2000 }),
        )
        .await,
        "a unary send",
    )?;
    ensure!(
        out["result"] == "error" && super::requests_of(&s0, "conformance.runner.Probe").is_empty(),
        "a request sent before any RegisterAck went out on the unacknowledged stream: {out}"
    );
    fake.set_ack_mode(AckMode::Auto);
    // protocol.md: "close and reopen a stream that has no ack within its own handshake timeout
    // (10 s in both official libraries)". The rule names no bound: the runner allows 30 s.
    let closed = up(
        &sut,
        "the stream without a RegisterAck was not closed within 30 s",
        s0.closed(Duration::from_secs(30)),
    )
    .await?;
    let s1 = run.registered(&sut, &fake, 1).await?;
    let mut registered = Vec::new();
    for _ in 0..10 {
        registered = view_hubs(&s1).await?;
        if registered.contains(&"fake-hub-1".to_string()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    ensure!(
        registered.contains(&"fake-hub-1".to_string()),
        "after the RegisterAck the service reports {registered:?} (expected fake-hub-1)"
    );
    Ok(format!(
        "unacknowledged stream (echoes only) not counted and not used, closed after {} ms and reopened; the \
         acknowledged one counted. Not observable: never waiting on the ack of a later Register (the \
         conformance service registers no handler after start)",
        ms(closed - s0.started)
    ))
}

fn permits_returned(s: &Session) -> u32 {
    s.select(|m| match &m.message {
        Some(M::Permits(p)) => Some(p.count),
        _ => None,
    })
    .iter()
    .map(|(_, c)| c)
    .sum()
}

pub async fn early_requests(run: Run) -> CheckResult {
    let fake = FakeHub::start("fake-hub-1").await;
    fake.set_ack_mode(AckMode::Manual);
    let mut spec = run.spec(vec![fake.address()]);
    spec.capacity = 1;
    let sut = run.launch(&spec)?;
    let s0 = run.registered(&sut, &fake, 0).await?;
    // Two requests before the ack with one permit: the fake hub (as snodra-server) forwards the
    // second only once the first one's permit came back, also before the ack.
    let (a, b) = tokio::join!(
        s0.command("conformance.Echo", json!({ "early": 1 })),
        s0.command("conformance.Echo", json!({ "early": 2 })),
    );
    let a = a.or_fail("a request before the RegisterAck was not served")?;
    let b = b.or_fail(
        "a second request before the RegisterAck (one permit) was not served: its permit did not come back",
    )?;
    ensure!(
        a["payload"] == json!({ "early": 1 }) && b["payload"] == json!({ "early": 2 }),
        "early requests answered with {a} and {b}"
    );
    ensure!(!s0.acked(), "runner bug: acknowledged too early");
    s0.until(Duration::from_secs(2), || {
        (permits_returned(&s0) >= 2).then_some(())
    })
    .await
    .or_fail("the permits of the two requests served before the ack did not both come back")?;
    s0.ack();
    s0.command("conformance.Echo", json!({ "late": true }))
        .await
        .or_fail("a request after the RegisterAck")?;
    s0.until(Duration::from_secs(2), || {
        (permits_returned(&s0) >= 3).then_some(())
    })
    .await
    .or_fail("the permit of the request after the ack did not come back")?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    ensure!(
        permits_returned(&s0) == 3,
        "{} permits came back for 3 requests",
        permits_returned(&s0)
    );
    Ok(
        "two requests before the RegisterAck (one permit) served and counted against the permits"
            .into(),
    )
}

pub async fn rejection(run: Run) -> CheckResult {
    let fake = FakeHub::start("fake-hub-1").await;
    let refusals = [
        (
            Code::AlreadyExists,
            "conformance refusal 1: type owned by another service",
        ),
        (
            Code::InvalidArgument,
            "conformance refusal 2: invalid register",
        ),
        (
            Code::Unauthenticated,
            "conformance refusal 3: unauthenticated",
        ),
        (
            Code::PermissionDenied,
            "conformance refusal 4: permission denied",
        ),
    ];
    fake.refuse_next(refusals.iter().map(|(c, m)| (*c, m.to_string())).collect());
    let sut = run.launch(&run.spec(vec![fake.address()]))?;
    let s4 = up(
        &sut,
        "the service did not register again after four refusals",
        fake.registered(4, run.startup() + Duration::from_secs(40)),
    )
    .await?;
    ensure!(sut.is_running(), "the service stopped after the refusals");
    let starts: Vec<Instant> = fake.sessions().iter().map(|s| s.started).collect();
    let gaps: Vec<u64> = starts.windows(2).map(|w| ms(w[1] - w[0])).collect();
    // protocol.md, Refusals: after a refused Register at least 1 s before the next attempt and
    // at most the backoff cap (at most 5 s); tolerance 100 ms below, 1 s above.
    ensure!(
        gaps[..4].iter().all(|g| (900..=6000).contains(g)),
        "after a refusal the next attempt came after {gaps:?} ms (expected at least 1 s, at most the 5 s cap)"
    );
    s4.until(Duration::from_secs(5), || s4.acked().then_some(()))
        .await
        .or_fail("ack")?;
    let v = s4
        .query("conformance.View", json!({}))
        .await
        .or_fail("conformance.View")?;
    let last = v["lastRejection"].as_str().unwrap_or_default();
    ensure!(
        last.contains("conformance refusal 4"),
        "the service exposes {:?} as its last rejection (expected the message of the last refusal)",
        v["lastRejection"]
    );
    Ok(format!(
        "kept running through ALREADY_EXISTS, INVALID_ARGUMENT, UNAUTHENTICATED, PERMISSION_DENIED; retried after {gaps:?} ms; reason exposed"
    ))
}

pub async fn heartbeat(run: Run) -> CheckResult {
    // On a fake hub: the cadence of RegisterAck.heartbeat_interval_ms, set to 1700 ms (neither
    // the 3 s default nor 1 s), so a library that ignores the ack is out of the bounds.
    const INTERVAL: u64 = 1700;
    let fake = FakeHub::start("fake-hub-1").await;
    fake.edit_ack(|a| a.heartbeat_interval_ms = INTERVAL as u32);
    let sut = run.launch(&run.spec(vec![fake.address()]))?;
    let s0 = run.registered(&sut, &fake, 0).await?;
    let acked_at = Instant::now();
    tokio::time::sleep(Duration::from_millis(INTERVAL * 6)).await;
    // The first heartbeat after the ack may still follow the pre-ack cadence: skip one interval.
    let beats: Vec<Instant> = s0
        .select(|m| matches!(m.message, Some(M::Heartbeat(_))).then_some(()))
        .into_iter()
        .map(|(t, _)| t)
        .filter(|t| *t > acked_at + Duration::from_millis(INTERVAL))
        .collect();
    let gaps: Vec<u64> = beats.windows(2).map(|w| ms(w[1] - w[0])).collect();
    let mean = gaps.iter().sum::<u64>() / gaps.len().max(1) as u64;
    // protocol.md, Heartbeats: "a Heartbeat every RegisterAck.heartbeat_interval_ms". The mean gap
    // within 20 % of it, no single gap above 1.5 intervals.
    ensure!(
        gaps.len() >= 3
            && (INTERVAL * 8 / 10..=INTERVAL * 12 / 10).contains(&mean)
            && gaps.iter().all(|g| *g <= INTERVAL * 3 / 2),
        "with heartbeat_interval_ms {INTERVAL}: gaps {gaps:?} ms, mean {mean} ms (expected the ack's interval)"
    );
    drop(fake);
    // On the real hub: idle for longer than its heartbeat timeout, still registered.
    let hub = run.hub();
    let runner = Peer::connect(
        &hub,
        Registration {
            service: "conformance-runner".into(),
            ..Registration::default()
        },
        |_, _| {},
    )
    .await
    .map_err(Problem::Fail)?;
    let spec = run.spec(vec![hub.clone()]);
    let sut = run.launch(&spec)?;
    up(&sut, "the service did not answer through the hub", async {
        echo_until_ready(&runner, run.startup()).await.ok()
    })
    .await?;
    let before = instance_of(&hub, &spec.instance).await?;
    let timeout = peer::topology(&hub)
        .await
        .map(|t| t.heartbeat_timeout_ms)
        .unwrap_or(10_000)
        .max(0) as u64;
    tokio::time::sleep(Duration::from_millis(timeout + 2500)).await;
    let after = instance_of(&hub, &spec.instance).await?;
    ensure!(
        !after.stale && after.connected_since_ms == before.connected_since_ms,
        "idle for {} ms on the real hub: stale {}, connected since {} (was {})",
        timeout + 2500,
        after.stale,
        after.connected_since_ms,
        before.connected_since_ms
    );
    runner
        .command("conformance.Echo", json!({}))
        .await
        .or_fail("an Echo after the idle time")?;
    Ok(format!(
        "heartbeats at the ack's {INTERVAL} ms (mean gap {mean} ms, gaps {gaps:?}); idle {} ms on the real hub without eviction",
        timeout + 2500
    ))
}

/// Sends `conformance.Echo` through the hub until it is answered (the instance is registered).
pub async fn echo_until_ready(peer: &Peer, within: Duration) -> Result<(), Problem> {
    let deadline = Instant::now() + within;
    loop {
        let r = peer
            .call(
                RequestKind::Command,
                "conformance.Echo",
                &json!({}),
                Duration::from_secs(3),
            )
            .await;
        if r.is_ok() {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(Problem::Fail(format!(
                "the service did not answer through the hub within {} s: {}",
                within.as_secs(),
                r.unwrap_err()
            )));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// The topology entry of `instance` on the hub at `hub`.
pub async fn instance_of(
    hub: &str,
    instance: &str,
) -> Result<crate::proto::InstanceTopology, Problem> {
    let t = peer::topology(hub).await.map_err(Problem::Fail)?;
    t.services
        .iter()
        .flat_map(|s| s.instances.iter())
        .find(|i| i.instance_id == instance)
        .cloned()
        .or_fail(&format!("instance {instance} is not in the hub's topology"))
}

pub async fn silent_hub(run: Run) -> CheckResult {
    const INTERVAL: u64 = 1000;
    let fake = FakeHub::start("fake-hub-1").await;
    fake.edit_ack(|a| a.heartbeat_interval_ms = INTERVAL as u32);
    let sut = run.launch(&run.spec(vec![fake.address()]))?;
    let s0 = run.registered(&sut, &fake, 0).await?;
    tokio::time::sleep(Duration::from_millis(2 * INTERVAL)).await;
    s0.set_echo(false);
    let closed = up(
        &sut,
        "the service kept a stream without echoes for 15 s (interval 1000 ms)",
        s0.closed(Duration::from_secs(15)),
    )
    .await?;
    ensure!(
        s0.closed_by_client(),
        "with heartbeat_interval_ms {INTERVAL} the silent stream was closed by the hub, not dropped by the library"
    );
    // protocol.md: on a heartbeat tick after the ack, the hub is silent when more than three
    // intervals passed since its last echo and the three heartbeats written since it are
    // unanswered: the drop comes between three and four intervals after the last echo. The
    // echo left the fake hub before the library read it, so nothing is subtracted below; 750 ms
    // above for scheduling.
    let last = s0
        .last_echo()
        .or_fail("the fake hub never echoed a heartbeat")?;
    let after = ms(closed.saturating_duration_since(last));
    let unanswered = s0
        .select(|m| match &m.message {
            Some(M::Heartbeat(_)) => Some(()),
            _ => None,
        })
        .iter()
        .filter(|(t, _)| *t > last)
        .count();
    ensure!(
        (3 * INTERVAL..=4 * INTERVAL + 750).contains(&after) && unanswered >= 3,
        "with heartbeat_interval_ms {INTERVAL} the silent stream was dropped {after} ms after the last echo, \
         with {unanswered} heartbeats unanswered (expected between three and four intervals, after three unanswered heartbeats)"
    );
    run.registered(&sut, &fake, 1).await?;
    Ok(format!(
        "silent stream dropped {after} ms after the last echo, {unanswered} heartbeats unanswered, then reconnected"
    ))
}

/// One outage of the fake hub (`down` cuts it, `back` restores it after 8 s), checking the
/// attempts, the wait to register again and the `Register`.
async fn outage(
    run: &Run,
    sut: &super::SutRef,
    fake: &FakeHub,
    first: &crate::proto::Register,
    next: usize,
    kind: &str,
) -> Result<String, Problem> {
    let down = Instant::now();
    tokio::time::sleep(Duration::from_secs(8)).await;
    let up_at = Instant::now();
    let s = up(
        sut,
        &format!("{kind}: not registered again within 12 s of the hub coming back"),
        fake.registered(next, Duration::from_secs(12)),
    )
    .await?;
    let again = s.register(Duration::ZERO).await.unwrap();
    ensure!(
        &again == first,
        "{kind}: the Register after the restart differs from the first: {again:?} vs {first:?}"
    );
    let _ = run;
    Ok(format!(
        "{kind}: registered again {} ms after the hub came back (outage {} ms)",
        ms(s.started - up_at),
        ms(up_at - down)
    ))
}

pub async fn reconnect(run: Run) -> CheckResult {
    let fake = FakeHub::start("fake-hub-1").await;
    let sut = run.launch(&run.spec(vec![fake.address()]))?;
    let s0 = run.registered(&sut, &fake, 0).await?;
    let first = s0.register(Duration::ZERO).await.unwrap();
    // A hub process that answers every Connect with UNAVAILABLE for 8 s.
    fake.set_mode(Mode::Unavailable);
    let down = Instant::now();
    s0.close(Status::unavailable("conformance: hub restarting"));
    let restart = {
        let probe = async {
            tokio::time::sleep(Duration::from_secs(8)).await;
            fake.set_hub_id("fake-hub-restarted-1");
            fake.set_mode(Mode::Accept);
        };
        let (_, note) = tokio::join!(probe, outage(&run, &sut, &fake, &first, 1, "UNAVAILABLE"));
        note?
    };
    let attempts: Vec<Instant> = fake.attempts().into_iter().filter(|t| *t >= down).collect();
    let mut times = vec![down];
    times.extend(attempts.iter().copied());
    let gaps: Vec<u64> = times.windows(2).map(|w| ms(w[1] - w[0])).collect();
    let during = attempts
        .iter()
        .filter(|t| **t < down + Duration::from_secs(8))
        .count();
    // protocol.md, Reconnect: "a backoff whose cap is at most 5 s, and never stall": at least two
    // attempts in 8 s, no wait above the cap (+600 ms tolerance), and a backoff, not a tight loop:
    // fewer than 20 attempts in 8 s.
    ensure!(
        during >= 2,
        "{during} connect attempts during an 8 s outage (the library stalls)"
    );
    ensure!(
        gaps.iter().all(|g| *g <= 5600),
        "waits between attempts {gaps:?} ms (the backoff cap is at most 5 s)"
    );
    ensure!(
        during < 20,
        "{during} attempts in 8 s, waits {gaps:?} ms: no backoff"
    );
    // A hub process that is gone: the port refuses connections for 8 s.
    let s1 = fake.live().or_fail("no live stream after the restart")?;
    let _ = s1;
    fake.go_down();
    let gone = {
        let back = async {
            tokio::time::sleep(Duration::from_secs(8)).await;
            fake.set_hub_id("fake-hub-restarted-2");
            fake.come_up().await;
        };
        let (_, note) = tokio::join!(
            back,
            outage(&run, &sut, &fake, &first, 2, "connection refused")
        );
        note?
    };
    Ok(format!(
        "{restart}; {during} attempts, waits {gaps:?} ms; {gone}; identical Register each time"
    ))
}

/// The longest time without an `InstanceStatus` on a stream: protocol.md, Instance status, "at
/// least every 5 s", with 1.5 s of tolerance.
const MAX_STATUS_GAP_MS: u64 = 6500;

/// What is wrong with a stream's reports, if anything: fewer than two in the window, two
/// consecutive ones more than [`MAX_STATUS_GAP_MS`] apart, or more than that from the last one
/// to the window's end (a library that reports twice and stops). No lower bound: a library may
/// report more often (the Kotlin one also reports at once when its set of live hubs changes).
/// `gaps` are the times between consecutive reports (none: fewer than two reports), `tail` the
/// time from the last one to the window's end, in ms.
fn status_reports_problem(gaps: &[u64], tail: u64) -> Option<String> {
    if gaps.is_empty() || gaps.iter().any(|g| *g > MAX_STATUS_GAP_MS) {
        return Some(format!(
            "gaps {gaps:?} ms (expected at least two, none more than {MAX_STATUS_GAP_MS} ms apart: \
             at least every 5 s)"
        ));
    }
    if tail > MAX_STATUS_GAP_MS {
        return Some(format!(
            "gaps {gaps:?} ms, then {tail} ms without a report at the end of the window (expected \
             at most {MAX_STATUS_GAP_MS} ms since the last: at least every 5 s)"
        ));
    }
    None
}

pub async fn status(run: Run) -> CheckResult {
    let a = FakeHub::start("hub-a").await;
    let b = FakeHub::start("hub-b").await;
    scripted(&a);
    scripted(&b);
    let mut spec = run.spec(vec![a.address(), b.address()]);
    spec.capacity = 8;
    let sut = run.launch(&spec)?;
    let sa = run.registered(&sut, &a, 0).await?;
    let _sb = run.registered(&sut, &b, 0).await?;
    let sb = b.sessions()[0].clone();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let busy_from = Instant::now();
    for _ in 0..2 {
        sa.request(wire::request(
            &wire::new_id(),
            RequestKind::Command,
            "conformance.Sleep",
            &json!({ "ms": 9000 }),
        ));
    }
    tokio::time::sleep(Duration::from_millis(11_500)).await;
    let observed_until = Instant::now();
    let busy_until = busy_from + Duration::from_millis(8500);
    let mut notes = Vec::new();
    for s in [&sa, &sb] {
        let statuses: Vec<(Instant, InstanceStatus)> = s.select(|m| match &m.message {
            Some(M::Status(st)) => Some(st.clone()),
            _ => None,
        });
        let gaps: Vec<u64> = statuses.windows(2).map(|w| ms(w[1].0 - w[0].0)).collect();
        let tail = statuses
            .last()
            .map_or(0, |(t, _)| ms(observed_until.saturating_duration_since(*t)));
        if let Some(problem) = status_reports_problem(&gaps, tail) {
            return Err(Problem::Fail(format!(
                "stream {}: {} InstanceStatus in about 12 s, {problem}",
                s.index,
                statuses.len()
            )));
        }
        let mut hubs = statuses.last().unwrap().1.connected_hub_ids.clone();
        hubs.sort();
        ensure!(
            hubs == ["hub-a", "hub-b"],
            "InstanceStatus.connected_hub_ids {hubs:?} (expected hub-a and hub-b)"
        );
        let during: Vec<u32> = statuses
            .iter()
            .filter(|(t, _)| *t > busy_from + Duration::from_millis(300) && *t < busy_until)
            .map(|(_, st)| st.handlers_in_flight)
            .collect();
        ensure!(
            !during.is_empty() && during.iter().all(|n| *n == 2),
            "InstanceStatus.handlers_in_flight while two handlers ran (both from hub-a): {during:?}"
        );
        notes.push(format!(
            "{} statuses, gaps {gaps:?} ms, {tail} ms since the last",
            statuses.len()
        ));
    }
    Ok(notes.join("; "))
}

#[cfg(test)]
mod tests {
    use super::status_reports_problem;

    #[test]
    fn status_reports_at_least_every_5_s_with_extra_reports_allowed() {
        // Every 5 s, give or take; `tail` is the time from the last report to the window's end.
        assert!(status_reports_problem(&[5000, 5001], 2000).is_none());
        assert!(status_reports_problem(&[6500], 6500).is_none());
        // Extra reports (one at once when the live hubs change) shorten a gap: allowed.
        assert!(status_reports_problem(&[0, 4712, 5010], 1500).is_none());
        assert!(status_reports_problem(&[1200, 3800, 5000], 0).is_none());
        // Too rare.
        assert!(status_reports_problem(&[6501], 1000).is_some());
        assert!(status_reports_problem(&[0, 7000], 1000).is_some());
        // Two reports early, then silence until the window ends.
        assert!(status_reports_problem(&[3000], 6501).is_some());
        assert!(status_reports_problem(&[3000], 9000).is_some());
        // Fewer than two reports in the window.
        assert!(status_reports_problem(&[], 0).is_some());
    }
}
