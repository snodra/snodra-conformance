//! Requests: unary through the hub, ids, timeouts, errors, never sent, retries, the answers'
//! stream, terminal messages, handler failures, identity, propagation, permits.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tonic::Status;

use super::connection::{echo_until_ready, instance_of};
use super::{
    CheckResult, OrFail, Problem, Run, await_op, ensure, error_of, ms, outcome, requests_of,
    scripted,
};
use crate::fake_hub::{AckMode, FakeHub, Session};
use crate::peer::{Peer, Registration};
use crate::proto::client_message::Message as M;
use crate::proto::{Request, RequestKind, RequestMode, reply};
use crate::wire::{self, WireError};

/// The runner's instance on the real hub, serving the runner types: it answers the payload's
/// `reply` (or `{"probe": true}`).
async fn runner_peer(hub: &str) -> Result<Peer, Problem> {
    Peer::connect(
        hub,
        Registration {
            service: "conformance-runner".into(),
            commands: vec!["conformance.runner.Probe".into()],
            queries: vec!["conformance.runner.ProbeQuery".into()],
            ..Registration::default()
        },
        |peer, request| {
            let payload = wire::json(&request.payload);
            let answer = payload
                .get("reply")
                .cloned()
                .unwrap_or(json!({ "probe": true }));
            peer.answer(&request.id, &answer);
        },
    )
    .await
    .map_err(Problem::Fail)
}

pub async fn unary(run: Run) -> CheckResult {
    let hub = run.hub();
    let runner = runner_peer(&hub).await?;
    let a = run.spec(vec![hub.clone()]);
    let b = run.spec(vec![hub.clone()]);
    let _sa = run.launch(&a)?;
    let _sb = run.launch(&b)?;
    echo_until_ready(&runner, run.startup()).await?;
    for spec in [&a, &b] {
        let deadline = Instant::now() + Duration::from_secs(10);
        while instance_of(&hub, &spec.instance).await.is_err() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        instance_of(&hub, &spec.instance).await?;
    }
    let instances = [a.instance.clone(), b.instance.clone()];
    // Handler side: a command and a point query reach their handler, the reply comes back.
    let r = wire::with_headers(
        wire::request(
            &wire::new_id(),
            RequestKind::Command,
            "conformance.Echo",
            &json!({ "x": 1 }),
        ),
        &[("x-conformance", "unary")],
    );
    let echoed = runner
        .request(r, Duration::from_secs(10))
        .await
        .json()
        .or_fail("conformance.Echo through the hub")?;
    ensure!(
        echoed["payload"] == json!({ "x": 1 })
            && echoed["headers"]["x-conformance"] == "unary"
            && instances.iter().any(|i| echoed["instanceId"] == i.as_str()),
        "conformance.Echo answered {echoed}"
    );
    let queried = runner
        .query("conformance.EchoQuery", json!({ "q": 2 }))
        .await
        .or_fail("conformance.EchoQuery through the hub")?;
    ensure!(
        queried["payload"] == json!({ "q": 2 }),
        "conformance.EchoQuery answered {queried}"
    );
    let done = runner
        .request(
            wire::request(
                &wire::new_id(),
                RequestKind::Command,
                "conformance.Done",
                &json!({}),
            ),
            Duration::from_secs(10),
        )
        .await;
    match &done.reply {
        Some(crate::proto::Reply {
            result: Some(reply::Result::Payload(p)),
            ..
        }) => ensure!(
            serde_json::from_slice::<Value>(p).ok() == Some(json!({})),
            "a command without a result answered {:?} (expected {{}})",
            String::from_utf8_lossy(p)
        ),
        _ => return Err(Problem::Fail(format!("conformance.Done answered {done:?}"))),
    }
    // By routing key: one key, one instance (two instances run).
    let mut keyed = HashSet::new();
    for _ in 0..6 {
        let mut r = wire::request(
            &wire::new_id(),
            RequestKind::Command,
            "conformance.Echo",
            &json!({}),
        );
        r.routing_key = "conformance-key-1".into();
        let v = runner
            .request(r, Duration::from_secs(10))
            .await
            .json()
            .or_fail("a keyed conformance.Echo")?;
        keyed.insert(v["instanceId"].as_str().unwrap_or_default().to_string());
    }
    ensure!(
        keyed.len() == 1,
        "one routing key reached the instances {keyed:?}"
    );
    // Sender side: the library's command and query, with a routing key, decoded replies.
    let sent = outcome(
        runner
            .command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe", "routingKey": "rk-7",
                        "payload": { "mark": "u1", "reply": { "probe": "answer" } } }),
            )
            .await,
        "a unary command",
    )?;
    ensure!(
        sent["result"] == "ok" && sent["payload"] == json!({ "probe": "answer" }),
        "the library's command came back as {sent}"
    );
    let seen = runner.seen();
    let probe = seen
        .iter()
        .find(|r| wire::json(&r.payload)["mark"] == "u1")
        .or_fail("the runner never got the library's command")?;
    ensure!(
        probe.routing_key == "rk-7" && probe.kind == RequestKind::Command as i32,
        "the library's command arrived with routing key {:?}, kind {}",
        probe.routing_key,
        probe.kind
    );
    let query = outcome(
        runner
            .command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe-query",
                        "payload": { "mark": "u2", "reply": { "rows": [1, 2] } } }),
            )
            .await,
        "a unary query",
    )?;
    ensure!(
        query["result"] == "ok" && query["payload"] == json!({ "rows": [1, 2] }),
        "the library's query came back as {query}"
    );
    let empty = outcome(
        runner
            .command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe", "payload": { "reply": {} } }),
            )
            .await,
        "a command without a result",
    )?;
    ensure!(
        empty["result"] == "ok" && empty["payload"] == json!({}),
        "a command answered {{}} came back as {empty}"
    );
    Ok("commands and point queries both ways through the hub; one key, one instance".into())
}

pub async fn request_id(run: Run) -> CheckResult {
    let rig = run.on_fake(|_| {}).await?;
    let s = &rig.session;
    let mut ops = Vec::new();
    for i in 0..10 {
        ops.push(
            s.command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe", "async": true,
                        "payload": { "delayMs": 300, "n": i } }),
            )
            .await
            .or_fail("an asynchronous conformance.Send")?,
        );
    }
    for op in &ops {
        let v = await_op(s, op, Duration::from_secs(10)).await?;
        ensure!(v["result"] == "ok", "a concurrent send failed: {v}");
    }
    for _ in 0..5 {
        let v = outcome(
            s.command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe" }),
            )
            .await,
            "a unary send",
        )?;
        ensure!(v["result"] == "ok", "a send failed: {v}");
    }
    let ids: Vec<String> = requests_of(s, "conformance.runner.Probe")
        .into_iter()
        .map(|(_, r)| r.id)
        .collect();
    let distinct: HashSet<&String> = ids.iter().collect();
    ensure!(
        ids.len() == 15 && distinct.len() == 15 && ids.iter().all(|i| !i.is_empty()),
        "15 sends (10 pending at once) carried {} ids, {} distinct: {ids:?}",
        ids.len(),
        distinct.len()
    );
    Ok("15 sends, 10 of them pending together, 15 distinct ids".into())
}

pub async fn timeout(run: Run) -> CheckResult {
    let fake = FakeHub::start("fake-hub-1").await;
    fake.edit_ack(|a| {
        a.default_timeout_ms = 1000;
        a.max_timeout_ms = 2000;
        a.type_timeouts_ms
            .insert("conformance.runner.ProbeQuery".into(), 1500);
    });
    scripted(&fake);
    let sut = run.launch(&run.spec(vec![fake.address()]))?;
    let s = run.registered(&sut, &fake, 0).await?;
    s.until(Duration::from_secs(5), || s.acked().then_some(()))
        .await
        .or_fail("ack")?;
    // (case, target, caller timeout, expected Request.timeout_ms, the hub's bound for it)
    let cases = [
        ("t-max", "probe", None, 0u32, 2000u64),
        ("t-override", "probe-query", None, 0, 1500),
        ("t-chosen", "probe", Some(1200u64), 1200, 1200),
    ];
    let mut ops = Vec::new();
    for (case, target, chosen, _, _) in &cases {
        let mut spec = json!({ "op": "unary", "target": target, "async": true,
                               "payload": { "silent": true, "case": case } });
        if let Some(t) = chosen {
            spec["timeoutMs"] = json!(t);
        }
        ops.push(
            s.command("conformance.Send", spec)
                .await
                .or_fail("an asynchronous conformance.Send")?,
        );
    }
    let mut notes = Vec::new();
    for ((case, target, _, expected, bound), op) in cases.iter().zip(&ops) {
        let v = await_op(&s, op, Duration::from_secs(12)).await?;
        let sent = s
            .select(|m| match &m.message {
                Some(M::Request(r)) if wire::json(&r.payload)["case"] == *case => {
                    Some(r.timeout_ms)
                }
                _ => None,
            })
            .first()
            .map(|(_, t)| *t)
            .or_fail(&format!("the {target} request never reached the hub"))?;
        ensure!(
            sent == *expected,
            "{case}: Request.timeout_ms {sent} (expected {expected})"
        );
        let elapsed = v["elapsedMs"].as_u64().unwrap_or(0);
        ensure!(
            v["result"] == "error"
                && elapsed + 150 >= bound + 1000
                && elapsed <= bound + 1000 + 2500,
            "{case}: with no answer the library gave up after {elapsed} ms with {} (expected the hub's bound {bound} ms + 1 s)",
            error_of(&v)
        );
        notes.push(format!(
            "{case} timeout_ms {sent}, gave up after {elapsed} ms"
        ));
    }
    Ok(notes.join("; "))
}

pub async fn errors(run: Run) -> CheckResult {
    let rig = run.on_fake(|_| {}).await?;
    let s = &rig.session;
    let send = |payload: Value| {
        s.command(
            "conformance.Send",
            json!({ "op": "unary", "target": "probe", "payload": payload }),
        )
    };
    let mut seen = Vec::new();
    for code in [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 42] {
        let expected = match code {
            1..=9 => wire::code_name(code),
            _ => "HANDLER_ERROR".to_string(),
        };
        let v = outcome(
            send(json!({ "hubError": code, "message": format!("hub says {code}") })).await,
            "a unary send",
        )?;
        ensure!(
            v["result"] == "error" && v["error"]["code"] == expected.as_str(),
            "the hub's Error code {code} reached the caller as {} (expected {expected})",
            error_of(&v)
        );
        seen.push(format!(
            "{code}->{}",
            v["error"]["code"].as_str().unwrap_or("?")
        ));
    }
    for (code, message, details) in [
        (4, "conformance handler failed", "{\"field\":\"x\"}"),
        (5, "conformance unauthenticated", ""),
        (7, "conformance permission denied", ""),
        (6, "conformance invalid request", ""),
    ] {
        let v = outcome(
            send(json!({ "replyError": { "code": code, "message": message, "details": details } }))
                .await,
            "a unary send",
        )?;
        let name = wire::code_name(code);
        ensure!(
            v["result"] == "error"
                && v["error"]["code"] == name.as_str()
                && v["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains(message),
            "a handler's {name} \"{message}\" reached the caller as {}",
            v["error"]
        );
        if !details.is_empty() {
            let got: Option<Value> = v["error"]["details"]
                .as_str()
                .and_then(|d| serde_json::from_str(d).ok());
            ensure!(
                got == serde_json::from_str(details).ok(),
                "HANDLER_ERROR details {details} reached the caller as {}",
                v["error"]["details"]
            );
        }
    }
    Ok(format!(
        "every code its own error ({}); handler messages and details kept",
        seen.join(", ")
    ))
}

pub async fn never_sent(run: Run) -> CheckResult {
    let fake = FakeHub::start("fake-hub-1").await;
    scripted(&fake);
    fake.set_ack_mode(AckMode::Never);
    let sut = run.launch(&run.spec(vec![fake.address()]))?;
    let s0 = run.registered(&sut, &fake, 0).await?;
    let v = outcome(
        s0.command(
            "conformance.Send",
            json!({ "op": "unary", "target": "probe", "timeoutMs": 2000 }),
        )
        .await,
        "a unary send",
    )?;
    ensure!(
        v["result"] == "error" && v["error"]["kind"] == "never-sent",
        "a send with no acknowledged stream came back as {} (expected never sent)",
        error_of(&v)
    );
    fake.set_ack_mode(AckMode::Auto);
    s0.close(Status::unavailable("conformance: reconnect"));
    let s1 = fake
        .registered(1, Duration::from_secs(15))
        .await
        .or_fail("no new stream")?;
    s1.until(Duration::from_secs(5), || s1.acked().then_some(()))
        .await
        .or_fail("ack")?;
    // The hub takes the request, then the stream breaks before an answer.
    let op = s1
        .command(
            "conformance.Send",
            json!({ "op": "unary", "target": "probe", "async": true, "payload": { "close": true } }),
        )
        .await
        .or_fail("an asynchronous conformance.Send")?;
    let s2 = fake
        .registered(2, Duration::from_secs(15))
        .await
        .or_fail("no new stream after the break")?;
    s2.until(Duration::from_secs(5), || s2.acked().then_some(()))
        .await
        .or_fail("ack")?;
    let v = await_op(&s2, &op, Duration::from_secs(10)).await?;
    ensure!(
        v["result"] == "error" && v["error"]["kind"] == "outcome-unknown",
        "a request written to a stream that broke came back as {} (expected outcome unknown)",
        error_of(&v)
    );
    Ok("no acknowledged stream: never sent; written then the stream broke: outcome unknown".into())
}

/// The attempts of `case` on every session of `fake`.
fn attempts(fake: &FakeHub, case: &str) -> Vec<Request> {
    fake.sessions()
        .iter()
        .flat_map(|s| requests_of(s, "conformance.runner.Probe"))
        .map(|(_, r)| r)
        .filter(|r| wire::json(&r.payload)["case"] == case)
        .collect()
}

pub async fn retry(run: Run) -> CheckResult {
    // The library's retry policy for every send (CONFORMANCE_RETRY_*): 3 attempts, 100 ms.
    let rig = run.on_fake(|spec| spec.retry = Some((3, 100))).await?;
    let s = &rig.session;
    let view = s
        .query("conformance.View", json!({}))
        .await
        .or_fail("conformance.View")?;
    if !view["retry"].is_boolean() {
        return Err(Problem::Fail(format!(
            "conformance.View has no boolean retry field (the contract requires it): {view}"
        )));
    }
    if view["retry"] == false {
        return Err(Problem::Skip(
            "the library offers no retry policy (conformance.View retry false): the check applies when it retries".into(),
        ));
    }
    let first = s
        .command(
            "conformance.Send",
            json!({ "op": "unary", "target": "probe", "timeoutMs": 5000,
                    "payload": { "case": "a", "steps": [{ "hubError": 1 }, { "reply": { "ok": true } }] } }),
        )
        .await
        .or_fail("conformance.Send")?;
    ensure!(
        first["result"] == "ok",
        "a command retried after NO_HANDLER came back as {}",
        error_of(&first)
    );
    let a = attempts(&rig.fake, "a");
    ensure!(
        a.len() == 2 && a[0].id != a[1].id,
        "NO_HANDLER then success: {} attempts with ids {:?}",
        a.len(),
        a.iter().map(|r| &r.id).collect::<Vec<_>>()
    );
    ensure!(
        a[0].timeout_ms > 0 && a[1].timeout_ms > 0 && a[1].timeout_ms < 5000,
        "attempts sent Request.timeout_ms {} then {} (expected the remaining budget of 5000 ms, never 0)",
        a[0].timeout_ms,
        a[1].timeout_ms
    );
    for (case, code, name) in [("b", 2, "TIMEOUT"), ("c", 3, "HANDLER_DISCONNECTED")] {
        let v = outcome(
            s.command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe", "timeoutMs": 5000,
                        "payload": { "case": case, "steps": [{ "hubError": code }, { "reply": {} }] } }),
            )
            .await,
            "a unary send",
        )?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let n = attempts(&rig.fake, case).len();
        ensure!(
            n == 1 && v["result"] == "error",
            "a command answered {name} was sent {n} times and came back as {}",
            error_of(&v)
        );
    }
    let v = outcome(
        s.command(
            "conformance.Send",
            json!({ "op": "unary", "target": "probe",
                    "payload": { "case": "d", "steps": [{ "hubError": 1 }, { "reply": {} }] } }),
        )
        .await,
        "a unary send",
    )?;
    let d = attempts(&rig.fake, "d");
    ensure!(
        v["result"] == "ok" && d.len() == 2 && d.iter().all(|r| r.timeout_ms > 0),
        "without a caller timeout the attempts sent Request.timeout_ms {:?} ({}), expected a budget, never 0",
        d.iter().map(|r| r.timeout_ms).collect::<Vec<_>>(),
        error_of(&v)
    );
    let op = s
        .command(
            "conformance.Send",
            json!({ "op": "unary", "target": "probe", "timeoutMs": 5000, "async": true,
                    "payload": { "case": "e", "steps": [{ "close": true }, { "reply": {} }] } }),
        )
        .await
        .or_fail("an asynchronous conformance.Send")?;
    let next = rig
        .fake
        .registered(1, Duration::from_secs(15))
        .await
        .or_fail("no new stream after the break")?;
    next.until(Duration::from_secs(5), || next.acked().then_some(()))
        .await
        .or_fail("ack")?;
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let v = await_op(&next, &op, Duration::from_secs(10)).await?;
    let e = attempts(&rig.fake, "e");
    ensure!(
        e.len() == 1 && v["result"] == "error",
        "a command whose stream broke after it was written was sent {} times and came back as {}",
        e.len(),
        error_of(&v)
    );
    Ok(format!(
        "retried after NO_HANDLER with a fresh id and timeout_ms {} then {}; not after TIMEOUT, HANDLER_DISCONNECTED or a lost stream",
        a[0].timeout_ms, a[1].timeout_ms
    ))
}

/// Whether `s` got an answer (reply, part or end) for `id`.
fn answered(s: &Session, id: &str) -> bool {
    !s.select(|m| match &m.message {
        Some(M::Reply(r)) if r.request_id == id => Some(()),
        Some(M::Part(p)) if p.request_id == id => Some(()),
        Some(M::End(e)) if e.request_id == id => Some(()),
        _ => None,
    })
    .is_empty()
}

pub async fn arrival_stream(run: Run) -> CheckResult {
    let a = FakeHub::start("hub-a").await;
    let b = FakeHub::start("hub-b").await;
    let sut = run.launch(&run.spec(vec![a.address(), b.address()]))?;
    let sa = run.registered(&sut, &a, 0).await?;
    let sb = run.registered(&sut, &b, 0).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    sb.request(wire::request(
        "arrival-1",
        RequestKind::Command,
        "conformance.Sleep",
        &json!({ "ms": 400 }),
    ));
    sb.request(wire::request_mode(
        "arrival-2#1",
        "conformance.Stream",
        &json!({ "count": 3 }),
        RequestMode::Streaming,
        10,
    ));
    sb.until(Duration::from_secs(5), || {
        (sb.reply_seen("arrival-1") && sb.end_seen("arrival-2#1")).then_some(())
    })
    .await
    .or_fail("the requests from hub-b were not answered on hub-b's stream")?;
    ensure!(
        !answered(&sa, "arrival-1") && !answered(&sa, "arrival-2#1"),
        "answers to hub-b's requests went to hub-a's stream"
    );
    sa.request(wire::request(
        "arrival-3",
        RequestKind::Command,
        "conformance.Sleep",
        &json!({ "ms": 1500 }),
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;
    sa.close(Status::unavailable("conformance: stream lost"));
    let sa2 = a
        .registered(1, Duration::from_secs(15))
        .await
        .or_fail("no new stream to hub-a")?;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    ensure!(
        !answered(&sa2, "arrival-3") && !answered(&sb, "arrival-3"),
        "the answer to a request of a lost stream went to another stream"
    );
    Ok("answers on the arrival stream; a lost stream's answer dropped".into())
}

impl Session {
    pub fn reply_seen(&self, id: &str) -> bool {
        !self
            .select(|m| match &m.message {
                Some(M::Reply(r)) if r.request_id == id => Some(()),
                _ => None,
            })
            .is_empty()
    }

    pub fn end_seen(&self, id: &str) -> bool {
        !self.ends(id).is_empty()
    }

    pub fn ends(&self, id: &str) -> Vec<crate::proto::ReplyEnd> {
        self.select(|m| match &m.message {
            Some(M::End(e)) if e.request_id == id => Some(e.clone()),
            _ => None,
        })
        .into_iter()
        .map(|(_, e)| e)
        .collect()
    }

    pub fn parts(&self, id: &str) -> Vec<(Instant, crate::proto::ReplyPart)> {
        self.select(|m| match &m.message {
            Some(M::Part(p)) if p.request_id == id => Some(p.clone()),
            _ => None,
        })
    }

    pub fn replies(&self, id: &str) -> usize {
        self.select(|m| match &m.message {
            Some(M::Reply(r)) if r.request_id == id => Some(()),
            _ => None,
        })
        .len()
    }
}

pub async fn one_terminal(run: Run) -> CheckResult {
    let rig = run.on_fake(|_| {}).await?;
    let s = Arc::clone(&rig.session);
    for id in ["terminal-1", "terminal-2#3"] {
        s.request(wire::request(
            id,
            RequestKind::Command,
            "conformance.Echo",
            &json!({}),
        ));
        s.until(Duration::from_secs(5), || s.reply_seen(id).then_some(()))
            .await
            .or_fail(&format!("no Reply for {id}"))?;
    }
    s.request(wire::request_mode(
        "terminal-3#1",
        "conformance.Stream",
        &json!({ "count": 3 }),
        RequestMode::Streaming,
        10,
    ));
    s.until(Duration::from_secs(5), || {
        s.end_seen("terminal-3#1").then_some(())
    })
    .await
    .or_fail("no ReplyEnd for the streaming request terminal-3#1")?;
    s.request(wire::request_mode(
        "terminal-4#1",
        "conformance.Watch",
        &json!({ "topic": "terminal" }),
        RequestMode::Subscription,
        0,
    ));
    s.call_request(
        wire::request(
            "terminal-4#0",
            RequestKind::Query,
            "conformance.Watch",
            &json!({ "topic": "terminal" }),
        ),
        Duration::from_secs(5),
    )
    .await
    .or_fail("the subscription's initial request terminal-4#0")?;
    s.command(
        "conformance.Emit",
        json!({ "topic": "terminal", "values": [1], "complete": true }),
    )
    .await
    .or_fail("conformance.Emit")?;
    s.until(Duration::from_secs(5), || {
        s.end_seen("terminal-4#1").then_some(())
    })
    .await
    .or_fail("no ReplyEnd for the completed subscription target terminal-4#1")?;
    // After the hub's Cancel: nothing. A stream waiting for credit, and a running unary target.
    s.request(wire::request_mode(
        "terminal-5#1",
        "conformance.Stream",
        &json!({ "count": 5 }),
        RequestMode::Streaming,
        1,
    ));
    s.until(Duration::from_secs(5), || {
        (!s.parts("terminal-5#1").is_empty()).then_some(())
    })
    .await
    .or_fail("no part for terminal-5#1")?;
    // A scatter-gather target (`<id>#<n>`, a unary query) that the hub cancels.
    s.request(wire::request(
        "terminal-6#2",
        RequestKind::Query,
        "conformance.SleepQuery",
        &json!({ "ms": 500 }),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    for id in ["terminal-5#1", "terminal-6#2"] {
        ensure!(
            s.cancel(id, "conformance"),
            "runner bug: a cancel for a command"
        );
    }
    s.send(crate::proto::server_message::Message::Flow(
        crate::proto::Flow {
            request_id: "terminal-5#1".into(),
            credits: 5,
        },
    ));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mut wrong = Vec::new();
    for id in ["terminal-1", "terminal-2#3"] {
        if s.replies(id) != 1 {
            wrong.push(format!("{id}: {} replies", s.replies(id)));
        }
    }
    for id in ["terminal-3#1", "terminal-4#1"] {
        if s.ends(id).len() != 1 || s.replies(id) != 0 {
            wrong.push(format!(
                "{id}: {} ends, {} replies",
                s.ends(id).len(),
                s.replies(id)
            ));
        }
    }
    if s.parts("terminal-5#1").len() != 1 || s.end_seen("terminal-5#1") {
        wrong.push(format!(
            "terminal-5#1 after Cancel: {} parts, {} ends",
            s.parts("terminal-5#1").len(),
            s.ends("terminal-5#1").len()
        ));
    }
    if s.reply_seen("terminal-6#2") {
        wrong.push("terminal-6#2 answered after Cancel".into());
    }
    ensure!(wrong.is_empty(), "{}", wrong.join("; "));
    Ok("one Reply per unary id (also <id>#<n>), one ReplyEnd per stream and subscription target, nothing after Cancel".into())
}

pub async fn handler_errors(run: Run) -> CheckResult {
    let rig = run.on_fake(|_| {}).await?;
    let s = &rig.session;
    let expect =
        |r: Result<Value, WireError>, code: &str, what: &str| -> Result<WireError, Problem> {
            match r {
                Err(e) if e.code == code => Ok(e),
                other => Err(Problem::Fail(format!(
                    "{what}: {other:?} (expected {code})"
                ))),
            }
        };
    let e = expect(
        s.command(
            "conformance.Fail",
            json!({ "message": "boom", "details": { "a": 1 } }),
        )
        .await,
        "HANDLER_ERROR",
        "a failing handler",
    )?;
    ensure!(
        e.message.contains("boom") && !e.message.contains('\n'),
        "a failing handler's message {:?} (expected a short text with \"boom\", no stack trace)",
        e.message
    );
    expect(
        s.command(
            "conformance.Relay",
            json!({ "target": "missing", "payload": { "hubError": 1, "message": "no handler" } }),
        )
        .await,
        "HANDLER_ERROR",
        "a handler whose own call got NO_HANDLER",
    )?;
    expect(
        s.command("conformance.Strict", json!({ "count": "not a number" }))
            .await,
        "INVALID_REQUEST",
        "a payload that does not decode",
    )?;
    let mut raw = wire::request(
        &wire::new_id(),
        RequestKind::Command,
        "conformance.Strict",
        &json!({}),
    );
    raw.payload = b"not json".to_vec();
    expect(
        s.call_request(raw, Duration::from_secs(10)).await,
        "INVALID_REQUEST",
        "a payload that is not JSON",
    )?;
    expect(
        s.call_request(
            wire::request(
                &wire::new_id(),
                RequestKind::Query,
                "conformance.Stream",
                &json!({ "count": 1 }),
            ),
            Duration::from_secs(10),
        )
        .await,
        "INVALID_REQUEST",
        "a unary request to a streaming handler",
    )?;
    expect(
        s.call_request(
            wire::request(
                "wrong-mode-1",
                RequestKind::Query,
                "conformance.Watch",
                &json!({ "topic": "x" }),
            ),
            Duration::from_secs(10),
        )
        .await,
        "INVALID_REQUEST",
        "a unary request (not <id>#0) to a subscription handler",
    )?;
    expect(
        s.command("conformance.NotServed", json!({})).await,
        "NO_HANDLER",
        "a type the service does not serve",
    )?;
    s.command("conformance.Echo", json!({}))
        .await
        .or_fail("an Echo after the failures")?;
    ensure!(
        rig.fake.sessions().len() == 1 && !s.is_closed(),
        "the stream was dropped during the failures"
    );
    Ok("HANDLER_ERROR (nested NO_HANDLER too), INVALID_REQUEST (payload, mode), NO_HANDLER, on one live stream".into())
}

/// A `conformance.Guarded` request with `authorization` (when given).
async fn guarded(
    s: &Session,
    authorization: Option<&str>,
    payload: Value,
) -> Result<Value, WireError> {
    let r = wire::request(
        &wire::new_id(),
        RequestKind::Command,
        "conformance.Guarded",
        &payload,
    );
    let r = match authorization {
        Some(a) => wire::with_headers(r, &[("authorization", a)]),
        None => r,
    };
    s.call_request(r, Duration::from_secs(10)).await
}

pub async fn identity(run: Run) -> CheckResult {
    let issuer = run.issuer().await;
    let rig = run
        .on_fake(|spec| {
            spec.jwks_url = Some(issuer.jwks_url());
            spec.issuer = Some(issuer.issuer());
        })
        .await?;
    let s = &rig.session;
    let bearer = |t: String| format!("Bearer {t}");
    let alice = bearer(issuer.mint("alice", 300));
    let expired = bearer(issuer.mint("alice", -600));
    let refused = bearer(issuer.mint("conformance-refused", 300));
    let foreign = bearer(issuer.mint_foreign_key("alice"));
    let wrong_issuer = bearer(issuer.mint_wrong_issuer("alice"));
    let good = json!({ "count": 1 });
    let bad = json!({ "count": "not a number" });
    // messages.md, Forwarded identity: validated "exactly as the service validates a REST call
    // (signature against the issuer's JWKS, expiry, issuer)", decided before decoding.
    let cases: [(&str, Option<&str>, &Value, &str); 9] = [
        ("no token", None, &good, "UNAUTHENTICATED"),
        (
            "a token that is not a JWT",
            Some("Bearer not-a-token"),
            &good,
            "UNAUTHENTICATED",
        ),
        ("an expired token", Some(&expired), &good, "UNAUTHENTICATED"),
        (
            "a token signed by a key the JWKS does not publish",
            Some(&foreign),
            &good,
            "UNAUTHENTICATED",
        ),
        (
            "a token of another issuer",
            Some(&wrong_issuer),
            &good,
            "UNAUTHENTICATED",
        ),
        (
            "a caller the handler refuses",
            Some(&refused),
            &good,
            "PERMISSION_DENIED",
        ),
        (
            "no token and a payload that does not decode",
            None,
            &bad,
            "UNAUTHENTICATED",
        ),
        (
            "a valid token and a payload that does not decode",
            Some(&alice),
            &bad,
            "INVALID_REQUEST",
        ),
        ("a valid token", Some(&alice), &good, "OK"),
    ];
    for (what, auth, payload, expected) in cases {
        let r = guarded(s, auth, payload.clone()).await;
        let got = match &r {
            Ok(_) => "OK".to_string(),
            Err(e) => e.code.clone(),
        };
        ensure!(
            got == expected,
            "{what}: {} (expected {expected})",
            match &r {
                Ok(v) => format!("answered {v}"),
                Err(e) => e.to_string(),
            }
        );
        if let Ok(v) = &r {
            ensure!(
                v["user"] == "alice",
                "{what}: answered {v} (expected user alice)"
            );
        }
    }
    Ok("UNAUTHENTICATED without a token, with a non-JWT, expired, foreign-key and other-issuer tokens \
        (before decoding); PERMISSION_DENIED for a refused caller; a valid token answered".into())
}

/// Whether `v` is a W3C traceparent (`00-<32 hex>-<16 hex>-<2 hex>`, ids not all zero).
fn traceparent_ok(v: &str) -> bool {
    let parts: Vec<&str> = v.split('-').collect();
    let hex = |p: &str, n: usize| {
        p.len() == n
            && p.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    };
    parts.len() == 4
        && hex(parts[0], 2)
        && hex(parts[1], 32)
        && hex(parts[2], 16)
        && hex(parts[3], 2)
        && parts[1].chars().any(|c| c != '0')
        && parts[2].chars().any(|c| c != '0')
}

pub async fn propagation(run: Run) -> CheckResult {
    let issuer = run.issuer().await;
    let rig = run
        .on_fake(|spec| {
            spec.jwks_url = Some(issuer.jwks_url());
            spec.issuer = Some(issuer.issuer());
        })
        .await?;
    let s = &rig.session;
    let token = format!("Bearer {}", issuer.mint("alice", 300));
    let trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
    let traceparent = format!("00-{trace_id}-00f067aa0ba902b7-01");
    let nested = |mark: &str| {
        requests_of(s, "conformance.runner.Probe")
            .into_iter()
            .map(|(_, r)| r)
            .find(|r| wire::json(&r.payload)["mark"] == mark)
    };
    let h = |r: &Request, k: &str| r.headers.get(k).cloned().unwrap_or_default();
    let relay = |mark: &str, id: &str, headers: &[(&str, &str)]| {
        wire::with_headers(
            wire::request(
                id,
                RequestKind::Command,
                "conformance.Relay",
                &json!({ "target": "probe", "payload": { "mark": mark } }),
            ),
            headers,
        )
    };
    // With the three headers: the nested request carries them on (messages.md).
    s.call_request(
        relay(
            "p1",
            &wire::new_id(),
            &[
                ("correlationId", "conformance-correlation-1"),
                ("authorization", &token),
                ("traceparent", &traceparent),
            ],
        ),
        Duration::from_secs(10),
    )
    .await
    .or_fail("conformance.Relay")?;
    let p1 = nested("p1").or_fail("the handler's own request never reached the hub")?;
    ensure!(
        h(&p1, "correlationId") == "conformance-correlation-1",
        "the nested request's correlationId {:?} (expected the handled request's)",
        h(&p1, "correlationId")
    );
    ensure!(
        h(&p1, "authorization") == token,
        "the nested request's authorization header {:?} (expected the handled request's)",
        h(&p1, "authorization")
    );
    ensure!(
        h(&p1, "traceparent").split('-').nth(1) == Some(trace_id)
            && traceparent_ok(&h(&p1, "traceparent")),
        "the nested request's traceparent {:?} (expected trace {trace_id}, the incoming one or a child of it)",
        h(&p1, "traceparent")
    );
    // Without any: messages.md, Correlation id: "its own id stands in: Request.id for a request".
    // A traceparent is then not required (the floor is "of that message's trace"); one that is
    // sent must be a W3C traceparent.
    let handled = wire::new_id();
    s.call_request(relay("p2", &handled, &[]), Duration::from_secs(10))
        .await
        .or_fail("conformance.Relay")?;
    let p2 = nested("p2").or_fail("the handler's own request never reached the hub")?;
    ensure!(
        h(&p2, "correlationId") == handled,
        "a request sent while handling one without a correlationId carries correlationId {:?} (expected the handled Request.id {handled})",
        h(&p2, "correlationId")
    );
    ensure!(
        h(&p2, "authorization").is_empty(),
        "a request sent while handling one without a caller carries authorization {:?}",
        h(&p2, "authorization")
    );
    let tp = h(&p2, "traceparent");
    ensure!(
        tp.is_empty() || traceparent_ok(&tp),
        "an invalid traceparent {tp:?}"
    );
    // A caller cannot set the library's headers (messages.md: refused, compared ignoring case).
    let mut refused = Vec::new();
    for (name, value) in [
        ("authorization", "Bearer forged"),
        (
            "traceparent",
            "00-ffffffffffffffffffffffffffffffff-ffffffffffffffff-01",
        ),
        ("tracestate", "forged=1"),
        ("correlationId", "forged-correlation"),
        ("correlation-id", "forged-correlation"),
        ("Authorization", "Bearer forged"),
        ("CORRELATIONID", "forged-correlation"),
    ] {
        let v = outcome(
            s.command(
                "conformance.Send",
                json!({ "op": "unary", "target": "probe", "headers": { name: value },
                        "payload": { "mark": format!("p3-{name}") } }),
            )
            .await,
            "a send with headers",
        )?;
        ensure!(
            v["result"] == "error" && v["error"]["kind"] == "refused",
            "a caller-set {name:?} header came back as {} (expected refused before sending)",
            if v["result"] == "ok" {
                "sent".to_string()
            } else {
                error_of(&v)
            }
        );
        ensure!(
            nested(&format!("p3-{name}")).is_none(),
            "a request with a caller-set {name:?} header was sent"
        );
        refused.push(name);
    }
    Ok(format!(
        "correlationId, authorization and traceparent carried on; without them the handled Request.id \
         as correlationId; caller-set {refused:?} refused before sending"
    ))
}

pub async fn permits(run: Run) -> CheckResult {
    let returned = |s: &Session| -> Vec<(Instant, u32)> {
        s.select(|m| match &m.message {
            Some(M::Permits(p)) => Some(p.count),
            _ => None,
        })
    };
    let total = |s: &Session| returned(s).iter().map(|(_, c)| c).sum::<u32>();
    let reply_at = |s: &Session, id: &str| {
        s.select(|m| match &m.message {
            Some(M::Reply(r)) if r.request_id == id => Some(()),
            _ => None,
        })
        .first()
        .map(|(t, _)| *t)
    };
    // The capacity offered.
    let rig = run.on_fake(|spec| spec.capacity = 4).await?;
    let register = rig.session.register(Duration::ZERO).await.unwrap();
    ensure!(
        register.initial_permits == 4,
        "with a capacity of 4 the Register offered {} permits",
        register.initial_permits
    );
    // One permit: the hub holds none after each forward, which is at the low-water mark
    // min(16, 1 / 2) = 0, so protocol.md asks for the permit "at once" (not batched at 50 ms).
    // Measured per request on the fake hub: Reply to Permits.
    let rig = run.on_fake(|spec| spec.capacity = 1).await?;
    let s = &rig.session;
    let mut delays = Vec::new();
    for i in 0..20u32 {
        let id = format!("one-permit-{i}");
        s.call_request(
            wire::request(
                &id,
                RequestKind::Command,
                "conformance.Echo",
                &json!({ "n": i }),
            ),
            Duration::from_secs(5),
        )
        .await
        .or_fail("an Echo (the fake hub forwards it only with a permit)")?;
        let replied = reply_at(s, &id).unwrap();
        let permit = s
            .until(Duration::from_secs(2), || {
                (total(s) > i).then(|| returned(s).last().unwrap().0)
            })
            .await
            .or_fail(&format!(
                "the permit of request {} did not come back within 2 s",
                i + 1
            ))?;
        delays.push(ms(permit.saturating_duration_since(replied)));
    }
    delays.sort();
    let median = delays[delays.len() / 2];
    // "At once" against "50 ms after the first unsent one": the median below 35 ms.
    ensure!(
        median < 35,
        "with one permit the permit came back {median} ms (median) after each reply, delays {delays:?} (expected at once, not batched)"
    );
    // 64 permits: the hub holds far more than min(16, 32), so the grants are batched: protocol.md,
    // "at 16 pending grants or 50 ms after the first unsent one". Warmed up first (a cold service
    // may be slow on its first requests), then 10 completions; each Permits is judged on its own:
    // with fewer than 16 grants it must not leave before 50 ms (10 ms tolerance) after the
    // earliest completion it covers; a batch of 16 may go at once.
    let rig = run.on_fake(|spec| spec.capacity = 64).await?;
    let s = &rig.session;
    for i in 0..5 {
        s.call_request(
            wire::request(
                &format!("warm-{i}"),
                RequestKind::Command,
                "conformance.Echo",
                &json!({}),
            ),
            Duration::from_secs(10),
        )
        .await
        .or_fail("a warm-up Echo")?;
    }
    s.until(Duration::from_secs(2), || (total(s) >= 5).then_some(()))
        .await
        .or_fail("the warm-up permits did not come back within 2 s")?;
    let measured_from = returned(s).len();
    for i in 0..10 {
        s.request(wire::request(
            &format!("batch-{i}"),
            RequestKind::Command,
            "conformance.Echo",
            &json!({}),
        ));
    }
    s.until(Duration::from_secs(10), || {
        (0..10)
            .all(|i| s.reply_seen(&format!("batch-{i}")))
            .then_some(())
    })
    .await
    .or_fail("10 Echo not all answered")?;
    s.until(Duration::from_secs(2), || (total(s) >= 15).then_some(()))
        .await
        .or_fail("the permits of 10 completions did not all come back within 2 s")?;
    let mut completions: Vec<Instant> = (0..10)
        .filter_map(|i| reply_at(s, &format!("batch-{i}")))
        .collect();
    completions.sort();
    let frames: Vec<(Instant, u32)> = returned(s).into_iter().skip(measured_from).collect();
    let mut covered = 0usize;
    let mut judged = Vec::new();
    for (at, count) in &frames {
        let earliest = completions[covered.min(completions.len() - 1)];
        let after = at.saturating_duration_since(earliest);
        ensure!(
            *count >= 16 || after >= Duration::from_millis(40),
            "a Permits of {count} left {} ms after the earliest completion it covers (batched: 50 ms, or 16 grants)",
            ms(after)
        );
        judged.push(format!("{count}@{}ms", ms(after)));
        covered += *count as usize;
    }
    ensure!(
        total(s) == 15,
        "{} permits came back for 15 requests",
        total(s)
    );
    // On the arrival stream.
    let a = FakeHub::start("hub-a").await;
    let b = FakeHub::start("hub-b").await;
    let mut spec = run.spec(vec![a.address(), b.address()]);
    spec.capacity = 8;
    let sut = run.launch(&spec)?;
    let sa = run.registered(&sut, &a, 0).await?;
    let sb = run.registered(&sut, &b, 0).await?;
    for i in 0..2 {
        sb.call_request(
            wire::request(
                &format!("arrival-{i}"),
                RequestKind::Command,
                "conformance.Echo",
                &json!({}),
            ),
            Duration::from_secs(5),
        )
        .await
        .or_fail("an Echo from hub-b")?;
    }
    sb.until(Duration::from_secs(2), || (total(&sb) >= 2).then_some(()))
        .await
        .or_fail("the permits of hub-b's requests did not come back on hub-b's stream")?;
    ensure!(
        total(&sa) == 0,
        "permits of hub-b's requests went to hub-a ({})",
        total(&sa)
    );
    Ok(format!(
        "capacity offered; one permit back at once (median {median} ms after the reply); 10 completions batched as \
         {judged:?} (count@ms after the earliest grant covered); on the arrival stream"
    ))
}
