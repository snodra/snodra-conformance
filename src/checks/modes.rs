//! Query modes: shared types, scatter-gather, streaming, subscriptions, a lost stream.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tonic::Status;

use super::connection::{echo_until_ready, instance_of};
use super::{CheckResult, OrFail, Problem, Run, await_op, ensure, error_of, outcome};
use crate::fake_hub::Session;
use crate::peer::{self, Peer, Registration};
use crate::proto::client_message::Message as M;
use crate::proto::server_message::Message as S;
use crate::proto::{Flow, ReplyEnd, ReplyPart, Request, RequestKind, RequestMode};
use crate::wire;

fn runner(service: &str) -> Registration {
    Registration {
        service: service.into(),
        ..Registration::default()
    }
}

pub async fn shared(run: Run) -> CheckResult {
    let hub = run.hub();
    let owner = Peer::connect(
        &hub,
        Registration {
            shared: vec!["conformance.SharedQuery".into()],
            ..runner("conformance-runner")
        },
        |p, r| p.answer(&r.id, &json!({ "service": "conformance-runner" })),
    )
    .await
    .map_err(Problem::Fail)?;
    let sender = Peer::connect(&hub, runner("conformance-runner-sender"), |_, _| {})
        .await
        .map_err(Problem::Fail)?;
    let spec = run.spec(vec![hub.clone()]);
    let _sut = run.launch(&spec)?;
    echo_until_ready(&sender, run.startup()).await?;
    let service = &run.options().service;
    let me = instance_of(&hub, &spec.instance).await?;
    ensure!(
        me.shared_query_types
            .contains(&"conformance.SharedQuery".to_string()),
        "the service's Register lists shared types {:?}",
        me.shared_query_types
    );
    let topology = peer::topology(&hub).await.map_err(Problem::Fail)?;
    let ownership = topology
        .types
        .iter()
        .find(|t| t.r#type == "conformance.SharedQuery")
        .or_fail("conformance.SharedQuery is not in the hub's topology")?;
    ensure!(
        ownership.shared
            && ownership.owner_services.contains(service)
            && ownership
                .owner_services
                .contains(&"conformance-runner".to_string()),
        "conformance.SharedQuery: shared {}, owners {:?}",
        ownership.shared,
        ownership.owner_services
    );
    let mut answered = HashSet::new();
    for _ in 0..20 {
        let v = sender
            .query("conformance.SharedQuery", json!({}))
            .await
            .or_fail("a point query on the shared type")?;
        answered.insert(v["service"].as_str().unwrap_or_default().to_string());
    }
    ensure!(
        answered.contains(service),
        "20 point queries on the shared type were answered by {answered:?}, never by the library"
    );
    let gather = sender
        .request(
            wire::request_mode(
                &wire::new_id(),
                "conformance.SharedQuery",
                &json!({}),
                RequestMode::ScatterGather,
                0,
            ),
            Duration::from_secs(10),
        )
        .await;
    let services: HashSet<String> = gather
        .parts
        .iter()
        .map(|p| p.responder_service.clone())
        .collect();
    ensure!(
        services.contains(service) && services.contains("conformance-runner"),
        "a scatter-gather on the shared type reached {services:?}"
    );
    drop(owner);
    Ok(format!(
        "registered shared beside another owner; point queries answered by {answered:?}; a gather reached both"
    ))
}

pub async fn scatter(run: Run) -> CheckResult {
    let hub = run.hub();
    let p1 = Peer::connect(
        &hub,
        Registration {
            queries: vec!["conformance.runner.ProbeQuery".into()],
            ..runner("conformance-runner")
        },
        |p, r| p.answer(&r.id, &json!({ "target": "p1" })),
    )
    .await
    .map_err(Problem::Fail)?;
    let p2 = Peer::connect(
        &hub,
        Registration {
            queries: vec!["conformance.runner.ProbeQuery".into()],
            ..runner("conformance-runner")
        },
        |p, r| p.answer_error(&r.id, 4, "p2 fails"),
    )
    .await
    .map_err(Problem::Fail)?;
    let a = run.spec(vec![hub.clone()]);
    let b = run.spec(vec![hub.clone()]);
    let _sa = run.launch(&a)?;
    let _sb = run.launch(&b)?;
    echo_until_ready(&p1, run.startup()).await?;
    for spec in [&a, &b] {
        let deadline = Instant::now() + Duration::from_secs(10);
        while instance_of(&hub, &spec.instance).await.is_err() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    // Sender side: the library collects every target's answer until the end.
    let v = outcome(
        p1.command(
            "conformance.Send",
            json!({ "op": "scatter", "target": "probe-query", "payload": {} }),
        )
        .await,
        "a scatter-gather",
    )?;
    let parts = v["parts"].as_array().cloned().unwrap_or_default();
    ensure!(
        v["result"] == "ok"
            && parts.len() == 2
            && v["failed"] == 1
            && parts
                .iter()
                .any(|p| p["ok"] == true && p["payload"] == json!({ "target": "p1" })),
        "a gather over two targets (one failing) came back as {v}"
    );
    // Handler side: every instance answers its <id>#<n>.
    let gather = p2
        .request(
            wire::request_mode(
                &wire::new_id(),
                "conformance.EchoQuery",
                &json!({}),
                RequestMode::ScatterGather,
                0,
            ),
            Duration::from_secs(10),
        )
        .await;
    let instances: HashSet<String> = gather
        .parts
        .iter()
        .filter(|p| p.error.is_none())
        .map(|p| p.responder_instance.clone())
        .collect();
    let end = gather.end.clone().unwrap_or_default();
    ensure!(
        instances == HashSet::from([a.instance.clone(), b.instance.clone()])
            && end.parts == 2
            && end.failed == 0,
        "a gather on conformance.EchoQuery: answered by {instances:?}, end {end:?}"
    );
    drop((p1, p2));
    // A target cancelled by the hub stops and sends nothing more.
    let rig = run.on_fake(|_| {}).await?;
    let s = &rig.session;
    // A scatter-gather target: `<id>#<n>`, a unary query (snodra-server cancels only queries).
    s.request(wire::request(
        "gather-1#0",
        RequestKind::Query,
        "conformance.SleepQuery",
        &json!({ "ms": 1500 }),
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    ensure!(
        s.cancel("gather-1#0", "max results"),
        "runner bug: a cancel for a command"
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    ensure!(
        !s.reply_seen("gather-1#0"),
        "a scatter-gather target answered after the hub's Cancel"
    );
    s.command("conformance.Echo", json!({}))
        .await
        .or_fail("an Echo after the cancel")?;
    Ok("the library gathers every answer (one failed) to the end; each instance answers its target; a cancelled target stays silent".into())
}

/// Sends a part (as the hub relays one) for `id`.
fn part(s: &Session, id: &str, seq: u64, payload: &Value) {
    s.send(S::Part(ReplyPart {
        request_id: id.to_string(),
        payload: serde_json::to_vec(payload).unwrap(),
        seq,
        responder_service: "conformance-runner".into(),
        responder_instance: "fake".into(),
        error: None,
    }));
}

fn end(s: &Session, id: &str, parts: u32) {
    s.send(S::End(ReplyEnd {
        request_id: id.to_string(),
        parts,
        ..ReplyEnd::default()
    }));
}

/// The credits the library granted `id` so far (its `Flow`s).
fn flowed(s: &Session, id: &str) -> u32 {
    s.select(|m| match &m.message {
        Some(M::Flow(f)) if f.request_id == id => Some(f.credits),
        _ => None,
    })
    .iter()
    .map(|(_, c)| c)
    .sum()
}

fn cancelled(s: &Session, id: &str) -> bool {
    !s.select(|m| match &m.message {
        Some(M::Cancel(c)) if c.request_id == id => Some(()),
        _ => None,
    })
    .is_empty()
}

/// The library's request of `type_name` whose payload has `case`.
async fn sent(s: &Session, type_name: &str, case: &str) -> Result<Request, Problem> {
    s.first(Duration::from_secs(10), |m| match &m.message {
        Some(M::Request(r)) if r.r#type == type_name && wire::json(&r.payload)["case"] == case => {
            Some(r.clone())
        }
        _ => None,
    })
    .await
    .or_fail(&format!(
        "the library's {type_name} ({case}) never reached the hub"
    ))
}

/// Plays a hub relaying `total` parts to the library's request `r` under its credit (the
/// subscription's initial part counts), then the end; fails when the credit stops coming.
async fn relay_under_credit(s: &Session, r: &Request, total: u64) -> Result<u32, Problem> {
    let initial = if r.initial_credit == 0 {
        64
    } else {
        r.initial_credit
    };
    let mut sent = 0u64;
    loop {
        if sent == total {
            end(s, &r.id, total as u32);
            return Ok(flowed(s, &r.id));
        }
        let credit = u64::from(initial) + u64::from(flowed(s, &r.id));
        if sent < credit {
            sent += 1;
            part(s, &r.id, sent, &json!({ "n": sent }));
            continue;
        }
        let waited = s
            .until(Duration::from_secs(5), || {
                (u64::from(initial) + u64::from(flowed(s, &r.id)) > sent).then_some(())
            })
            .await;
        ensure!(
            waited.is_some(),
            "after {sent} parts (credit {initial}) the library granted no more credit in 5 s"
        );
    }
}

pub async fn stream(run: Run) -> CheckResult {
    let rig = run
        .on_fake(|spec| spec.stream_credit_timeout_ms = Some(2000))
        .await?;
    let s = Arc::clone(&rig.session);
    let seqs = |id: &str| -> Vec<u64> { s.parts(id).iter().map(|(_, p)| p.seq).collect() };
    let stream = |id: &str, count: u64, credit: u32| {
        s.request(wire::request_mode(
            id,
            "conformance.Stream",
            &json!({ "count": count }),
            RequestMode::Streaming,
            credit,
        ))
    };
    let flow = |id: &str, credits: u32| {
        s.send(S::Flow(Flow {
            request_id: id.into(),
            credits,
        }))
    };
    // Handler side: seq 1, 2, ... under the credit.
    stream("stream-1#1", 5, 2);
    s.until(Duration::from_secs(5), || {
        (seqs("stream-1#1").len() >= 2).then_some(())
    })
    .await
    .or_fail("no parts for stream-1#1")?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    ensure!(
        seqs("stream-1#1").len() == 2,
        "credit 2: {} parts sent",
        seqs("stream-1#1").len()
    );
    flow("stream-1#1", 2);
    s.until(Duration::from_secs(5), || {
        (seqs("stream-1#1").len() >= 4).then_some(())
    })
    .await
    .or_fail("no parts after a Flow of 2")?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    ensure!(
        seqs("stream-1#1").len() == 4,
        "credit 4: {} parts sent",
        seqs("stream-1#1").len()
    );
    flow("stream-1#1", 1);
    s.until(Duration::from_secs(5), || {
        s.end_seen("stream-1#1").then_some(())
    })
    .await
    .or_fail("no ReplyEnd for stream-1#1")?;
    ensure!(
        seqs("stream-1#1") == [1, 2, 3, 4, 5] && s.ends("stream-1#1")[0].error.is_none(),
        "parts numbered {:?}, end {:?}",
        seqs("stream-1#1"),
        s.ends("stream-1#1")
    );
    // Stops on Cancel (while waiting for credit: a Flow after it changes nothing).
    stream("stream-2#1", 10, 2);
    s.until(Duration::from_secs(5), || {
        (seqs("stream-2#1").len() >= 2).then_some(())
    })
    .await
    .or_fail("no parts for stream-2#1")?;
    ensure!(
        s.cancel("stream-2#1", "conformance"),
        "runner bug: a cancel for a command"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    flow("stream-2#1", 5);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    ensure!(
        seqs("stream-2#1").len() == 2 && !s.end_seen("stream-2#1"),
        "after Cancel: {} parts, {} ends",
        seqs("stream-2#1").len(),
        s.ends("stream-2#1").len()
    );
    // Ends with TIMEOUT when no credit comes (CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS 2000).
    stream("stream-3#1", 3, 1);
    let ended = s
        .until(Duration::from_secs(6), || {
            s.ends("stream-3#1").into_iter().next()
        })
        .await
        .or_fail("a stream with no credit for 6 s did not end (credit wait bound 2000 ms)")?;
    ensure!(
        ended.error.as_ref().map(|e| e.code) == Some(crate::proto::ErrorCode::Timeout as i32),
        "a stream with no credit ended with {ended:?} (expected TIMEOUT)"
    );
    // Sender side: credit granted and refilled, Cancel when the caller stops, losses detected.
    let op = s
        .command(
            "conformance.Send",
            json!({ "op": "stream", "target": "probe-stream", "initialCredit": 4, "async": true,
                    "payload": { "case": "s4" } }),
        )
        .await
        .or_fail("conformance.Send")?;
    let r = sent(&s, "conformance.runner.ProbeStream", "s4").await?;
    ensure!(
        r.mode == RequestMode::Streaming as i32 && r.initial_credit == 4,
        "the library's stream asked mode {} with initial_credit {}",
        r.mode,
        r.initial_credit
    );
    let refilled = relay_under_credit(&s, &r, 12).await?;
    ensure!(refilled > 0, "the stream's credit was never refilled");
    let v = await_op(&s, &op, Duration::from_secs(10)).await?;
    ensure!(
        v["result"] == "ok" && v["items"].as_array().map(Vec::len) == Some(12),
        "12 parts relayed under credit came back as {v}"
    );
    let op = s
        .command(
            "conformance.Send",
            json!({ "op": "stream", "target": "probe-stream", "initialCredit": 10, "read": 2,
                    "async": true, "payload": { "case": "s5" } }),
        )
        .await
        .or_fail("conformance.Send")?;
    let r5 = sent(&s, "conformance.runner.ProbeStream", "s5").await?;
    for n in 1..=5 {
        part(&s, &r5.id, n, &json!({ "n": n }));
    }
    s.until(Duration::from_secs(5), || {
        cancelled(&s, &r5.id).then_some(())
    })
    .await
    .or_fail("the caller stopped reading after 2 parts and no Cancel was sent")?;
    await_op(&s, &op, Duration::from_secs(5)).await?;
    for (case, seqs, parts) in [("s6", vec![1u64, 2, 4], 4u32), ("s7", vec![1, 2, 3], 5)] {
        let op = s
            .command(
                "conformance.Send",
                json!({ "op": "stream", "target": "probe-stream", "initialCredit": 10,
                        "async": true, "payload": { "case": case } }),
            )
            .await
            .or_fail("conformance.Send")?;
        let r = sent(&s, "conformance.runner.ProbeStream", case).await?;
        for n in &seqs {
            part(&s, &r.id, *n, &json!({ "n": n }));
        }
        end(&s, &r.id, parts);
        let v = await_op(&s, &op, Duration::from_secs(10)).await?;
        // conformance.md, conformance.Send: a detected loss is kind lost-parts.
        ensure!(
            v["result"] == "error" && v["error"]["kind"] == "lost-parts",
            "parts {seqs:?} then ReplyEnd{{parts {parts}}}: the stream came back as {} (expected lost parts)",
            if v["result"] == "ok" {
                "a success".to_string()
            } else {
                error_of(&v)
            }
        );
    }
    Ok(format!(
        "handler: seq 1..5 under credit, stops on Cancel, TIMEOUT without credit; sender: 12 parts with {refilled} credits refilled, Cancel on stop, gap and short end fail the stream"
    ))
}

pub async fn subscribe(run: Run) -> CheckResult {
    let rig = run.on_fake(|spec| spec.max_subscriptions = Some(2)).await?;
    let s = Arc::clone(&rig.session);
    let watch = |id: &str, topic: &str| {
        s.request(wire::request_mode(
            id,
            "conformance.Watch",
            &json!({ "topic": topic }),
            RequestMode::Subscription,
            0,
        ))
    };
    let values = |id: &str| -> Vec<Value> {
        s.parts(id)
            .iter()
            .map(|(_, p)| wire::json(&p.payload)["value"].clone())
            .collect()
    };
    // Handler side: registered on <id>#n, the update right after the initial result kept.
    watch("watch-1#1", "a");
    let initial = s
        .call_request(
            wire::request(
                "watch-1#0",
                RequestKind::Query,
                "conformance.Watch",
                &json!({ "topic": "a" }),
            ),
            Duration::from_secs(5),
        )
        .await
        .or_fail("the initial request watch-1#0")?;
    ensure!(initial["value"] == 0, "initial result {initial}");
    s.command(
        "conformance.Emit",
        json!({ "topic": "a", "values": [1, 2] }),
    )
    .await
    .or_fail("conformance.Emit")?;
    s.until(Duration::from_secs(5), || {
        (values("watch-1#1").len() >= 2).then_some(())
    })
    .await
    .or_fail("the updates emitted right after the initial result did not reach the subscription")?;
    s.command(
        "conformance.Emit",
        json!({ "topic": "a", "values": [], "complete": true }),
    )
    .await
    .or_fail("conformance.Emit")?;
    s.until(Duration::from_secs(5), || {
        s.end_seen("watch-1#1").then_some(())
    })
    .await
    .or_fail("a completed emitter did not end its subscription with ReplyEnd")?;
    ensure!(
        values("watch-1#1") == [json!(1), json!(2)] && s.ends("watch-1#1")[0].error.is_none(),
        "updates {:?}, end {:?}",
        values("watch-1#1"),
        s.ends("watch-1#1")
    );
    // Removed on Cancel.
    watch("watch-2#1", "b");
    tokio::time::sleep(Duration::from_millis(200)).await;
    s.command("conformance.Emit", json!({ "topic": "b", "values": [1] }))
        .await
        .or_fail("conformance.Emit")?;
    s.until(Duration::from_secs(5), || {
        (!values("watch-2#1").is_empty()).then_some(())
    })
    .await
    .or_fail("no update for watch-2#1")?;
    ensure!(
        s.cancel("watch-2#1", "conformance"),
        "runner bug: a cancel for a command"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    s.command("conformance.Emit", json!({ "topic": "b", "values": [2] }))
        .await
        .or_fail("conformance.Emit")?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    ensure!(
        values("watch-2#1").len() == 1 && !s.end_seen("watch-2#1"),
        "after Cancel: updates {:?}, {} ends",
        values("watch-2#1"),
        s.ends("watch-2#1").len()
    );
    // Refused beyond its limit (CONFORMANCE_MAX_SUBSCRIPTIONS 2).
    watch("watch-3#1", "c");
    watch("watch-4#1", "c");
    tokio::time::sleep(Duration::from_millis(500)).await;
    watch("watch-5#1", "c");
    let refused = s
        .until(Duration::from_secs(3), || {
            s.ends("watch-5#1").into_iter().next()
        })
        .await
        .or_fail("a third subscription beyond a limit of 2 was not refused")?;
    ensure!(
        !s.end_seen("watch-3#1") && !s.end_seen("watch-4#1"),
        "subscriptions within the limit were ended"
    );
    ensure!(
        refused.error.as_ref().map(|e| e.code)
            == Some(crate::proto::ErrorCode::ResourceExhausted as i32),
        "the subscription beyond the limit ended with {refused:?} (expected RESOURCE_EXHAUSTED)"
    );
    // Sender side: initial then updates, credit counting the initial part, losses detected.
    let op = s
        .command(
            "conformance.Send",
            json!({ "op": "subscribe", "target": "probe-watch", "initialCredit": 2, "async": true,
                    "payload": { "case": "u4" } }),
        )
        .await
        .or_fail("conformance.Send")?;
    let r = sent(&s, "conformance.runner.ProbeWatch", "u4").await?;
    ensure!(
        r.mode == RequestMode::Subscription as i32 && r.initial_credit == 2,
        "the library's subscription asked mode {} with initial_credit {} (the caller asked 2)",
        r.mode,
        r.initial_credit
    );
    let refilled = relay_under_credit(&s, &r, 6).await?;
    ensure!(refilled > 0, "the subscription's credit was never refilled");
    let v = await_op(&s, &op, Duration::from_secs(10)).await?;
    ensure!(
        v["result"] == "ok" && v["items"].as_array().map(Vec::len) == Some(6),
        "the initial part and 5 updates came back as {v}"
    );
    for (case, seqs, parts) in [("u5", vec![1u64, 2, 4], 4u32), ("u6", vec![1, 2], 4)] {
        let op = s
            .command(
                "conformance.Send",
                json!({ "op": "subscribe", "target": "probe-watch", "initialCredit": 10,
                        "async": true, "payload": { "case": case } }),
            )
            .await
            .or_fail("conformance.Send")?;
        let r = sent(&s, "conformance.runner.ProbeWatch", case).await?;
        for n in &seqs {
            part(&s, &r.id, *n, &json!({ "n": n }));
        }
        end(&s, &r.id, parts);
        let v = await_op(&s, &op, Duration::from_secs(10)).await?;
        ensure!(
            v["result"] == "error" && v["error"]["kind"] == "lost-parts",
            "parts {seqs:?} then ReplyEnd{{parts {parts}}}: the subscription came back as {} (expected lost parts)",
            if v["result"] == "ok" {
                "a success".to_string()
            } else {
                error_of(&v)
            }
        );
    }
    Ok(format!(
        "handler: updates after the initial result kept, ReplyEnd on completion, removed on Cancel, RESOURCE_EXHAUSTED beyond the limit; sender: initial + 5 updates with {refilled} credits refilled, gap and short end fail it"
    ))
}

pub async fn lost_stream(run: Run) -> CheckResult {
    // Capacity 1: the streaming handler holds the instance's only handler slot while it runs.
    let rig = run.on_fake(|spec| spec.capacity = 1).await?;
    let s0 = Arc::clone(&rig.session);
    s0.request(wire::request_mode(
        "lost-2#1",
        "conformance.Watch",
        &json!({ "topic": "lost" }),
        RequestMode::Subscription,
        0,
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let v = s0
        .query("conformance.View", json!({}))
        .await
        .or_fail("conformance.View")?;
    ensure!(
        v["subscriptionsServed"].as_u64().unwrap_or(0) >= 1,
        "the subscription is not served: {v}"
    );
    s0.request(wire::request_mode(
        "lost-1#1",
        "conformance.Stream",
        &json!({ "count": 5 }),
        RequestMode::Streaming,
        1,
    ));
    s0.until(Duration::from_secs(5), || {
        (!s0.parts("lost-1#1").is_empty()).then_some(())
    })
    .await
    .or_fail("no part for lost-1#1")?;
    s0.close(Status::unavailable("conformance: stream lost"));
    let s1 = rig
        .fake
        .registered(1, Duration::from_secs(15))
        .await
        .or_fail("no new stream")?;
    s1.until(Duration::from_secs(5), || s1.acked().then_some(()))
        .await
        .or_fail("ack")?;
    // Its handler slot is free again: a request on the new stream runs (capacity 1).
    s1.call(
        RequestKind::Command,
        "conformance.Sleep",
        &json!({ "ms": 0 }),
        Duration::from_secs(3),
    )
    .await
    .or_fail("with capacity 1, a request on the new stream did not run: the streaming handler of the lost stream still holds its slot")?;
    s1.command(
        "conformance.Emit",
        json!({ "topic": "lost", "values": [1] }),
    )
    .await
    .or_fail("conformance.Emit")?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let v = s1
        .query("conformance.View", json!({}))
        .await
        .or_fail("conformance.View")?;
    ensure!(
        v["subscriptionsServed"] == 0 && s1.parts("lost-2#1").is_empty(),
        "the subscription of the lost stream lives on: {v}"
    );
    // The protocol-visible proof that the lost stream's streaming handler ended: every
    // InstanceStatus after a settle time reports no handler in flight (the instance is idle; the
    // status goes every 5 s, protocol.md, Instance status).
    let settled = Instant::now() + Duration::from_millis(1000);
    let statuses = s1
        .until(Duration::from_secs(13), || {
            let after: Vec<u32> = s1
                .select(|m| match &m.message {
                    Some(M::Status(st)) => Some(st.handlers_in_flight),
                    _ => None,
                })
                .into_iter()
                .filter(|(t, _)| *t >= settled)
                .map(|(_, n)| n)
                .collect();
            (after.len() >= 2).then_some(after)
        })
        .await
        .or_fail("fewer than two InstanceStatus on the new stream in 13 s")?;
    ensure!(
        statuses.iter().all(|n| *n == 0),
        "InstanceStatus.handlers_in_flight {statuses:?} on the new stream while the instance is idle: the lost \
         stream's streaming handler is still counted (it must end with its stream)"
    );
    // Sender side: a stream and a subscription sent through the lost stream fail.
    let mut session = s1;
    for (next, (op, target, type_name)) in [
        ("stream", "probe-stream", "conformance.runner.ProbeStream"),
        ("subscribe", "probe-watch", "conformance.runner.ProbeWatch"),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, t)| (i + 2, t))
    {
        let started = session
            .command(
                "conformance.Send",
                json!({ "op": op, "target": target, "initialCredit": 10, "async": true,
                        "payload": { "case": op } }),
            )
            .await
            .or_fail("conformance.Send")?;
        let r = sent(&session, type_name, op).await?;
        part(&session, &r.id, 1, &json!({ "n": 1 }));
        tokio::time::sleep(Duration::from_millis(200)).await;
        session.close(Status::unavailable("conformance: stream lost"));
        let fresh = rig
            .fake
            .registered(next, Duration::from_secs(15))
            .await
            .or_fail("no new stream")?;
        fresh
            .until(Duration::from_secs(5), || fresh.acked().then_some(()))
            .await
            .or_fail("ack")?;
        let v = await_op(&fresh, &started, Duration::from_secs(10)).await?;
        // conformance.md: a stream or subscription failed by a lost hub stream is outcome-unknown.
        ensure!(
            v["result"] == "error" && v["error"]["kind"] == "outcome-unknown",
            "a {op} sent through a stream that was lost came back as {} (expected outcome unknown)",
            if v["result"] == "ok" {
                "a success".to_string()
            } else {
                error_of(&v)
            }
        );
        session = fresh;
    }
    Ok("served stream and subscription ended with their stream (no handler counted in the status, the \
        handler slot free, no update delivered); sent stream and subscription failed as outcome unknown"
        .into())
}
