//! Several hubs: one stream per hub id, routing, the permit split, failover.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tonic::Status;

use super::connection::{echo_until_ready, instance_of};
use super::{CheckResult, OrFail, Problem, Run, ensure, error_of, outcome, requests_of, scripted};
use crate::fake_hub::{FakeHub, Mode, Session};
use crate::peer::{Peer, Registration};
use crate::proto::client_message::Message as M;
use crate::rendezvous;
use crate::wire;

/// Two fake hubs and one instance on both, registered and acknowledged.
async fn two(
    run: &Run,
    ids: [&str; 2],
    capacity: u32,
) -> Result<(FakeHub, FakeHub, super::SutRef, Arc<Session>, Arc<Session>), Problem> {
    let a = FakeHub::start(ids[0]).await;
    let b = FakeHub::start(ids[1]).await;
    let mut spec = run.spec(vec![a.address(), b.address()]);
    spec.capacity = capacity;
    let sut = run.launch(&spec)?;
    let sa = run.registered(&sut, &a, 0).await?;
    let sb = run.registered(&sut, &b, 0).await?;
    for s in [&sa, &sb] {
        s.until(Duration::from_secs(5), || s.acked().then_some(()))
            .await
            .or_fail("ack")?;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok((a, b, sut, sa, sb))
}

pub async fn hubs(run: Run) -> CheckResult {
    // Fake hubs: two targets answering one hub id keep one stream.
    let a = FakeHub::start("hub-a").await;
    let b = FakeHub::start("hub-b").await;
    let c = FakeHub::start("hub-a").await;
    let sut = run.launch(&run.spec(vec![a.address(), b.address(), c.address()]))?;
    let sb = run.registered(&sut, &b, 0).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let settled = Instant::now();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let same_id: Vec<Arc<Session>> = a.sessions().into_iter().chain(c.sessions()).collect();
    let live: Vec<&Arc<Session>> = same_id.iter().filter(|s| !s.is_closed()).collect();
    let late = same_id.iter().filter(|s| s.started > settled).count();
    ensure!(
        live.len() == 1 && late == 0,
        "two targets answering hub id hub-a: {} streams open, {late} opened in the last 4 s (expected one, stable)",
        live.len()
    );
    // initial_permits is the stream's share of the capacity when it opened (L1-HUB-PERMITS):
    // a stream reopened after the duplicate target was parked offers a larger share.
    let mut ra = live[0]
        .register(Duration::ZERO)
        .await
        .or_fail("no Register")?;
    let mut rb = sb.register(Duration::ZERO).await.or_fail("no Register")?;
    ra.initial_permits = 0;
    rb.initial_permits = 0;
    ensure!(
        ra == rb,
        "the Registers to the two hubs differ apart from initial_permits: {ra:?} vs {rb:?}"
    );
    let v = sb
        .query("conformance.View", json!({}))
        .await
        .or_fail("conformance.View")?;
    let mut known: Vec<String> = v["hubIds"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|h| h.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    known.sort();
    ensure!(
        known == ["hub-a", "hub-b"],
        "the service knows the hubs {known:?}"
    );
    drop((sut, a, b, c));
    if run.options().hubs.len() < 2 {
        return Ok("one stream per hub id with identical Registers apart from initial_permits, a duplicate target parked (fake hubs); \
                   the part on two real hubs did not run (give --hub twice)"
            .into());
    }
    // Two real hubs: one stream each, the instance reports both.
    let hubs = run.options().hubs.clone();
    let spec = run.spec(hubs[..2].to_vec());
    let _sut = run.launch(&spec)?;
    let mut ids = Vec::new();
    for hub in &hubs[..2] {
        let runner = Peer::connect(
            hub,
            Registration {
                service: "conformance-runner".into(),
                ..Registration::default()
            },
            |_, _| {},
        )
        .await
        .map_err(Problem::Fail)?;
        echo_until_ready(&runner, run.startup()).await?;
        ids.push(runner.hub_id.clone());
    }
    for hub in &hubs[..2] {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let me = instance_of(hub, &spec.instance).await?;
            let mut reported = me.connected_hub_ids.clone();
            reported.sort();
            let mut expected = ids.clone();
            expected.sort();
            if reported == expected {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "on {hub} the instance reports connected hubs {reported:?} (expected {expected:?})"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    Ok(format!(
        "one stream per hub id with identical Registers apart from initial_permits (a duplicate target parked); registered on both real hubs {ids:?}"
    ))
}

pub async fn hub_routing(run: Run) -> CheckResult {
    let (h1, h2, _sut, s1, _s2) = two(&run, ["hub-1", "hub-2"], 16).await?;
    scripted(&h1);
    scripted(&h2);
    // Keys for both winners, and keys whose winner a signed comparison of the scores would get
    // wrong (protocol.md: "scores compared as unsigned 64-bit numbers").
    let mut keys: Vec<(String, &str)> = vec![("order-42".into(), "hub-2")];
    let (mut per_winner, mut signed_traps) = (HashMap::<&str, usize>::new(), 0);
    for n in 0..10_000 {
        let key = format!("conformance-key-{n}");
        let winner = rendezvous::ranking(&["hub-1", "hub-2"], &key)[0];
        let trap = rendezvous::ranking_signed(&["hub-1", "hub-2"], &key)[0] != winner;
        let count = per_winner.entry(winner).or_insert(0);
        if (trap && signed_traps < 3) || *count < 3 {
            *count += 1;
            signed_traps += usize::from(trap);
            keys.push((key, winner));
        }
        if signed_traps >= 3 && per_winner.values().all(|c| *c >= 3) {
            break;
        }
    }
    for (key, winner) in &keys {
        let v = outcome(
            s1.command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe", "routingKey": key }),
            )
            .await,
            "a keyed send",
        )?;
        ensure!(
            v["payload"]["hub"] == *winner,
            "routing key {key:?} went to {} (rendezvous, unsigned scores: {winner})",
            v["payload"]["hub"]
        );
    }
    // protocol.md: "an unkeyed request round-robin over the hubs whose stream is acknowledged":
    // with two acknowledged hubs and nothing else sent, consecutive requests alternate.
    let mut order = Vec::new();
    for _ in 0..8 {
        let v = outcome(
            s1.command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe" }),
            )
            .await,
            "an unkeyed send",
        )?;
        order.push(v["payload"]["hub"].as_str().unwrap_or("?").to_string());
    }
    ensure!(
        order.windows(2).all(|w| w[0] != w[1]) && order.iter().all(|h| h.starts_with("hub-")),
        "8 unkeyed requests went to {order:?} (expected round-robin: alternating over the two hubs)"
    );
    Ok(format!(
        "{} keys on their rendezvous hub (order-42 on hub-2, {signed_traps} where a signed comparison differs); unkeyed {order:?}",
        keys.len()
    ))
}

pub async fn hub_permits(run: Run) -> CheckResult {
    let (_x, y, _sut, sx, sy) = two(&run, ["hub-x", "hub-y"], 5).await?;
    for s in [&sx, &sy] {
        let r = s.register(Duration::ZERO).await.unwrap();
        ensure!(
            r.initial_permits == 3,
            "capacity 5 over two hubs: Register.initial_permits {} (expected ceil(5/2) = 3)",
            r.initial_permits
        );
    }
    y.set_mode(Mode::Unavailable);
    sy.close(Status::unavailable("conformance: hub gone"));
    let topped = sx
        .first(Duration::from_secs(10), |m| match &m.message {
            Some(M::Permits(p)) if p.grant > 0 => Some(*p),
            _ => None,
        })
        .await
        .or_fail("hub-y went away and hub-x got no Permits with a grant in 10 s")?;
    ensure!(
        topped.count == 2 && topped.grant == 5,
        "hub-y went away: hub-x got Permits{{count {}, grant {}}} (expected count 2, grant 5)",
        topped.count,
        topped.grant
    );
    y.set_mode(Mode::Accept);
    let back = y
        .registered(1, Duration::from_secs(15))
        .await
        .or_fail("no new stream to hub-y after it came back")?;
    let r = back.register(Duration::ZERO).await.unwrap();
    ensure!(
        r.initial_permits == 3,
        "hub-y came back: Register.initial_permits {} (expected 3)",
        r.initial_permits
    );
    Ok("3 + 3 of 5; Permits{count 2, grant 5} to the survivor; 3 on the hub that came back".into())
}

pub async fn hub_failover(run: Run) -> CheckResult {
    let (h1, h2, _sut, s1, _s2) = two(&run, ["hub-1", "hub-2"], 16).await?;
    // f1: hub-2 declines, hub-1 answers; f2: the first hub to see it declines; f3: both decline.
    let counts: Arc<Mutex<HashMap<String, usize>>> = Arc::default();
    for fake in [&h1, &h2] {
        let counts = Arc::clone(&counts);
        let hub = fake.hub_id();
        fake.on_request(move |s, r| {
            let case = wire::json(&r.payload)["case"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let n = {
                let mut c = counts.lock().unwrap();
                let n = c.entry(case.clone()).or_insert(0);
                *n += 1;
                *n
            };
            let decline = match case.as_str() {
                "f1" => hub == "hub-2",
                "f2" => n == 1,
                "f3" => true,
                _ => false,
            };
            if decline {
                s.hub_error(
                    &r.id,
                    crate::proto::ErrorCode::NoHandler as i32,
                    "no handler",
                );
            } else {
                s.answer(&r.id, &json!({ "hub": hub }));
            }
        });
    }
    // (arrival, hub id, request id, Request.timeout_ms) of every try of `case`.
    let tries = |case: &str| -> Vec<(Instant, String, String, u32)> {
        let mut all: Vec<(Instant, String, String, u32)> = [&h1, &h2]
            .iter()
            .flat_map(|f| {
                let hub = f.hub_id();
                f.sessions()
                    .iter()
                    .flat_map(|s| requests_of(s, "conformance.runner.Probe"))
                    .filter(|(_, r)| wire::json(&r.payload)["case"] == case)
                    .map(move |(t, r)| (t, hub.clone(), r.id, r.timeout_ms))
                    .collect::<Vec<_>>()
            })
            .collect();
        all.sort_by_key(|(t, _, _, _)| *t);
        all
    };
    let send = |case: &str, key: Option<&str>| {
        let mut spec = json!({ "op": "unary", "target": "probe", "timeoutMs": 5000, "payload": { "case": case } });
        if let Some(k) = key {
            spec["routingKey"] = json!(k);
        }
        s1.command("conformance.Send", spec)
    };
    // protocol.md, Several hubs: every hop carries the caller's full timeout, never what is left.
    let tried = |t: &[(Instant, String, String, u32)]| {
        t.iter()
            .map(|(_, h, id, ms)| format!("{h}:{id}:{ms}"))
            .collect::<Vec<_>>()
    };
    let full = |what: &str, t: &[(Instant, String, String, u32)]| -> Result<(), Problem> {
        ensure!(
            t.iter().all(|(_, _, _, ms)| *ms == 5000),
            "{what}: a hop without the caller's full timeout (5000 ms): tried {:?} (hub:id:timeout_ms)",
            tried(t)
        );
        Ok(())
    };
    let v = outcome(send("f1", Some("order-42")).await, "a keyed send")?;
    let t = tries("f1");
    ensure!(
        v["result"] == "ok"
            && t.len() == 2
            && t[0].1 == "hub-2"
            && t[1].1 == "hub-1"
            && t[0].2 == t[1].2,
        "keyed order-42 (hub-2 first), hub-2 NO_HANDLER: tried {:?} (hub:id:timeout_ms), came back {}",
        tried(&t),
        error_of(&v)
    );
    full("keyed order-42, hub-2 NO_HANDLER", &t)?;
    let v = outcome(send("f2", None).await, "an unkeyed send")?;
    let t = tries("f2");
    ensure!(
        v["result"] == "ok" && t.len() == 2 && t[0].1 != t[1].1 && t[0].2 == t[1].2,
        "unkeyed, first hub NO_HANDLER: tried {:?} (hub:id:timeout_ms), came back {}",
        tried(&t),
        error_of(&v)
    );
    full("unkeyed, first hub NO_HANDLER", &t)?;
    let v: Value = outcome(send("f3", Some("order-42")).await, "a keyed send")?;
    let t = tries("f3");
    let elapsed = v["elapsedMs"].as_u64().unwrap_or(u64::MAX);
    ensure!(
        v["result"] == "error"
            && v["error"]["code"] == "NO_HANDLER"
            && t.len() == 2
            && t[0].1 == "hub-2"
            && elapsed < 1500,
        "every hub NO_HANDLER: tried {:?}, came back {} after {elapsed} ms (expected NO_HANDLER at once)",
        t.iter().map(|(_, h, _, _)| h.clone()).collect::<Vec<_>>(),
        error_of(&v)
    );
    Ok("NO_HANDLER moved to the next hub in rendezvous and round-robin order with the same id and the full timeout; all declining came back at once. \
        Not checked: a request never handed to the transport moving to another hub (no deterministic way to hold it in the library's buffer)".into())
}
