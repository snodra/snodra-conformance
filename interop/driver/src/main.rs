//! The interoperability driver. Services built on different Snodra libraries (one instance per
//! language, all named `conformance-interop`) are registered on one real hub; the driver is a
//! raw client of that hub (the conformance runner's `Peer`) that makes every service send to
//! every other and checks what arrives. See interop/README.md for what is asserted.

mod matrix;

use std::collections::BTreeMap;
use std::time::Duration;

use clap::Parser;
use serde_json::{Value, json};
use snodra_conformance::peer::{Peer, Registration, topology};
use snodra_conformance::proto::RequestKind;
use snodra_conformance::wire;

use matrix::{pairs, trace_id, traceparent};

const SERVICE: &str = "conformance-interop";

#[derive(Parser)]
#[command(about = "Cross-language interoperability driver for Snodra client libraries")]
struct Args {
    /// The hub's address (host:port)
    #[arg(long)]
    hub: String,
    /// The languages that must be registered, comma-separated; each is its instance id
    #[arg(long, value_delimiter = ',')]
    languages: Vec<String>,
    /// Languages whose service answers a unary send of a type it also handles in its own
    /// process without the hub (a service that keeps its library's default and cannot be told
    /// to route through the hub): those sends never leave the process, so the driver reports
    /// them as not exercised instead of counting them as exchanges (none of the official services)
    #[arg(long, value_delimiter = ',')]
    local_senders: Vec<String>,
    /// Exit non-zero when any required exchange could not be exercised through the hub
    #[arg(long)]
    strict: bool,
}

/// The caller's token for the relay: a signed compact JWT that no service can verify (none has a
/// JWKS), so every service only carries it on. Every library reads the `authorization` header it
/// is handed and answers `UNAUTHENTICATED` for a value that is not a signed compact JWT, whether
/// or not the handler asks for a caller, so the value cannot be free text.
fn authorization() -> String {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as b64;
    let header = b64.encode(r#"{"alg":"RS256","typ":"JWT"}"#);
    let claims = b64.encode(r#"{"sub":"interop"}"#);
    format!("Bearer {header}.{claims}.{}", b64.encode("interop"))
}

type Res<T> = Result<T, String>;

/// A call of the driver: the type, a routing key, headers.
struct Call<'a> {
    kind: RequestKind,
    type_name: &'a str,
    key: Option<&'a str>,
    headers: Vec<(&'a str, &'a str)>,
    payload: Value,
}

async fn call(peer: &Peer, c: Call<'_>) -> Res<Value> {
    let mut request = wire::with_headers(
        wire::request(&wire::new_id(), c.kind, c.type_name, &c.payload),
        &c.headers,
    );
    if let Some(key) = c.key {
        request.routing_key = key.to_string();
    }
    peer.request(request, Duration::from_secs(30))
        .await
        .json()
        .map_err(|e| format!("{}: {e:?}", c.type_name))
}

fn command<'a>(type_name: &'a str, key: Option<&'a str>, payload: Value) -> Call<'a> {
    Call {
        kind: RequestKind::Command,
        type_name,
        key,
        headers: Vec::new(),
        payload,
    }
}

fn query<'a>(type_name: &'a str, key: Option<&'a str>, payload: Value) -> Call<'a> {
    Call {
        kind: RequestKind::Query,
        ..command(type_name, key, payload)
    }
}

/// The outcome of a `conformance.Send`, which must be `ok`.
fn ok_outcome(outcome: Value, what: &str) -> Res<Value> {
    if outcome["result"] == "ok" {
        Ok(outcome)
    } else {
        Err(format!("{what}: the send did not succeed: {outcome}"))
    }
}

fn expect(condition: bool, what: &str, got: &Value) -> Res<()> {
    if condition {
        Ok(())
    } else {
        Err(format!("{what}; got {got}"))
    }
}

fn header<'a>(headers: &'a Value, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(Value::as_str)
}

fn same_trace(headers: &Value, trace: &str) -> bool {
    header(headers, "traceparent").and_then(trace_id) == Some(trace)
}

/// 1. The topology shows one instance per language of the service.
async fn wait_for_instances(hub: &str, langs: &[String]) -> Res<()> {
    let mut want: Vec<String> = langs.to_vec();
    want.sort();
    let mut last = Vec::new();
    for _ in 0..120 {
        if let Ok(t) = topology(hub).await {
            last = t
                .services
                .iter()
                .filter(|s| s.service_name == SERVICE)
                .flat_map(|s| s.instances.iter().map(|i| i.instance_id.clone()))
                .collect();
            last.sort();
            if last == want {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "the topology shows instances {last:?} of {SERVICE}, expected {want:?}"
    ))
}

/// Polls `conformance.View` (a query: the hub gives each call to some instance) until every
/// language has reported a `subscriptionsServed` for which `ok` holds, or `limit` passes.
async fn wait_for_subscriptions(
    peer: &Peer,
    langs: &[String],
    ok: impl Fn(u64) -> bool,
    limit: Duration,
) -> Res<()> {
    let deadline = tokio::time::Instant::now() + limit;
    let mut seen: BTreeMap<String, u64> = BTreeMap::new();
    while tokio::time::Instant::now() < deadline {
        let view = call(peer, query("conformance.View", None, json!({}))).await?;
        if let (Some(i), Some(n)) = (
            view["instanceId"].as_str(),
            view["subscriptionsServed"].as_u64(),
        ) {
            seen.insert(i.to_string(), n);
        }
        if langs.iter().all(|l| seen.get(l).is_some_and(|n| ok(*n))) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!(
        "the instances' subscriptionsServed did not reach the expected state within {limit:?}: {seen:?}"
    ))
}

/// 2. For each instance, a routing key whose keyed `conformance.Echo` answers that instance.
async fn find_keys(peer: &Peer, langs: &[String]) -> Res<BTreeMap<String, String>> {
    let mut keys = BTreeMap::new();
    for n in 0..500 {
        if keys.len() == langs.len() {
            break;
        }
        let key = format!("key-{n}");
        let answer = call(peer, command("conformance.Echo", Some(&key), json!({}))).await?;
        let instance = answer["instanceId"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if langs.contains(&instance) {
            keys.entry(instance).or_insert(key);
        }
    }
    for l in langs {
        if !keys.contains_key(l) {
            return Err(format!("no routing key of key-0..key-499 reaches {l}"));
        }
    }
    Ok(keys)
}

/// A `conformance.Send` keyed `key` (so it is served by the instance that owns the key).
async fn send(
    peer: &Peer,
    key: &str,
    headers: Vec<(&str, &str)>,
    payload: Value,
    what: &str,
) -> Res<Value> {
    let outcome = call(
        peer,
        Call {
            headers,
            ..command("conformance.Send", Some(key), payload)
        },
    )
    .await?;
    ok_outcome(outcome, what)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match run(&args).await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("interop: FAILED: {e}");
            std::process::exit(1);
        }
    }
}

async fn run(args: &Args) -> Res<()> {
    let langs = &args.languages;
    if langs.len() < 2 {
        return Err("at least two languages are needed".into());
    }
    let hub = &args.hub;
    let trace = uuid::Uuid::new_v4().simple().to_string();
    let mut exchanges = 0usize;
    let mut unexercised: Vec<String> = Vec::new();
    let mut pass = |what: &str| {
        exchanges += 1;
        println!("ok   {what}");
    };

    wait_for_instances(hub, langs).await?;
    println!("ok   topology: one instance of {SERVICE} per language {langs:?}");

    // The driver is itself a service of the hub: it serves conformance.runner.Probe.
    let peer = Peer::connect(
        hub,
        Registration {
            service: "conformance-runner".into(),
            commands: vec!["conformance.runner.Probe".into()],
            ..Registration::default()
        },
        |peer, request| peer.answer(&request.id, &json!({ "probe": true })),
    )
    .await?;

    let keys = find_keys(&peer, langs).await?;
    println!("ok   routing keys: {keys:?}");

    // 3. Every ordered pair: X sends a keyed command to Y.
    for (x, y) in pairs(langs) {
        let correlation = wire::new_id();
        let tp = traceparent(&trace);
        let token = authorization();
        let sent = json!({"from": x, "to": y, "nonce": wire::new_id()});
        let what = format!("{x} -> {y} (unary, keyed)");
        let outcome = send(
            &peer,
            &keys[&x],
            vec![
                ("correlationId", &correlation),
                ("traceparent", &tp),
                ("authorization", &token),
            ],
            json!({"op": "unary", "target": "echo", "routingKey": keys[&y], "payload": sent}),
            &what,
        )
        .await?;
        let p = &outcome["payload"];
        if p["instanceId"] == x.as_str() && args.local_senders.contains(&x) {
            // The library answered from its own handler: the request never reached the hub.
            println!(
                "note {what}: served by {x} itself, not through the hub (local short-circuit)"
            );
            unexercised.push(format!("{x} -> {y}"));
            continue;
        }
        expect(
            p["instanceId"] == y.as_str(),
            &format!("{what}: served by {y}"),
            p,
        )?;
        expect(
            header(&p["headers"], "correlationId") == Some(&correlation),
            &format!("{what}: correlation id {correlation} reaches {y}"),
            &p["headers"],
        )?;
        expect(
            same_trace(&p["headers"], &trace),
            &format!("{what}: the trace {trace} reaches {y}"),
            &p["headers"],
        )?;
        expect(
            header(&p["headers"], "authorization") == Some(&token),
            &format!("{what}: the caller's authorization crosses from {x} to {y}"),
            &p["headers"],
        )?;
        expect(
            p["payload"] == sent,
            &format!("{what}: the payload {sent} round-trips"),
            &p["payload"],
        )?;
        pass(&what);
    }

    // 4. Scatter-gather and a unary query from every X.
    for x in langs {
        let tp = traceparent(&trace);
        let what = format!("{x} scatter echo-query");
        let outcome = send(
            &peer,
            &keys[x],
            vec![("traceparent", &tp)],
            json!({"op": "scatter", "target": "echo-query", "payload": {}, "timeoutMs": 10000}),
            &what,
        )
        .await?;
        let parts = outcome["parts"].as_array().cloned().unwrap_or_default();
        let mut got: Vec<String> = parts
            .iter()
            .map(|p| {
                p["payload"]["instanceId"]
                    .as_str()
                    .unwrap_or("?")
                    .to_string()
            })
            .collect();
        got.sort();
        let mut want = langs.clone();
        want.sort();
        expect(
            got == want,
            &format!("{what}: one part from every language"),
            &outcome,
        )?;
        expect(
            parts
                .iter()
                .all(|p| p["ok"] == true && same_trace(&p["payload"]["headers"], &trace)),
            &format!("{what}: every part ok and in trace {trace}"),
            &outcome,
        )?;
        pass(&what);

        let what = format!("{x} unary echo-query");
        let sent = json!({"from": x, "nonce": wire::new_id()});
        let outcome = send(
            &peer,
            &keys[x],
            vec![("traceparent", &tp)],
            json!({"op": "unary", "target": "echo-query", "payload": sent}),
            &what,
        )
        .await?;
        let p = &outcome["payload"];
        if args.local_senders.contains(x) {
            // The hub would pick any instance; the sender's library answers a query it handles
            // itself in-process, so this never reaches the hub.
            println!("note {what}: answered in-process by {x}, not through the hub");
            unexercised.push(format!("{x} unary echo-query"));
        } else {
            expect(
                langs.iter().any(|l| p["instanceId"] == l.as_str()),
                &format!("{what}: served by one of the languages"),
                p,
            )?;
            expect(
                p["payload"] == sent && same_trace(&p["headers"], &trace),
                &format!("{what}: the payload {sent} round-trips in trace {trace}"),
                p,
            )?;
            pass(&what);
        }
    }

    // 5. A stream from every X (the hub chooses the serving instance; it is not observable).
    for x in langs {
        let what = format!("{x} stream 5 items");
        let outcome = send(
            &peer,
            &keys[x],
            vec![],
            json!({"op": "stream", "target": "stream", "payload": {"count": 5}}),
            &what,
        )
        .await?;
        let items: Vec<Value> = (1..=5).map(|n| json!({ "n": n })).collect();
        expect(
            outcome["items"] == Value::Array(items) && outcome["end"] == "completed",
            &format!("{what}: items n=1..5 then completed"),
            &outcome,
        )?;
        pass(&what);
    }

    // 6. A subscription from every X: the hub subscribes every live instance, one of them also
    // answers the initial result; an emit keyed k_Y reaches the subscription from Y.
    for x in langs {
        let what = format!("{x} subscribes, every language emits");
        let topic = format!("t-{x}");
        // The previous subscription must be gone from every instance.
        wait_for_subscriptions(&peer, langs, |n| n == 0, Duration::from_secs(20))
            .await
            .map_err(|e| format!("{what}: {e}"))?;
        let started = call(
            &peer,
            command(
                "conformance.Send",
                Some(&keys[x]),
                json!({"op": "subscribe", "target": "watch", "payload": {"topic": topic},
                       "read": 1 + langs.len(), "async": true}),
            ),
        )
        .await?;
        let op_id = started["opId"]
            .as_str()
            .ok_or_else(|| format!("{what}: no opId in {started}"))?
            .to_string();
        // The hub subscribes every instance: emit only once each one reports serving it.
        wait_for_subscriptions(&peer, langs, |n| n >= 1, Duration::from_secs(20))
            .await
            .map_err(|e| format!("{what}: {e}"))?;
        for y in langs {
            call(
                &peer,
                command(
                    "conformance.Emit",
                    Some(&keys[y]),
                    json!({"topic": topic, "values": [y]}),
                ),
            )
            .await?;
        }
        // Outcome is a query: the hub routes it to any instance (a routing key is ignored for
        // queries) and an instance that does not own the op answers unknownOp: ask again.
        let mut outcome = Value::Null;
        for _ in 0..60 {
            outcome = call(
                &peer,
                query(
                    "conformance.Outcome",
                    None,
                    json!({"opId": op_id, "waitMs": 500}),
                ),
            )
            .await?;
            if outcome["pending"] != true && outcome["unknownOp"].is_null() {
                break;
            }
        }
        let outcome = ok_outcome(outcome, &what)?;
        let items = outcome["items"].as_array().cloned().unwrap_or_default();
        let initial = items.first().cloned().unwrap_or(Value::Null);
        expect(
            initial["topic"] == topic.as_str()
                && initial["value"] == 0
                && langs.iter().any(|l| initial["instanceId"] == l.as_str()),
            &format!("{what}: the initial result comes from one instance"),
            &initial,
        )?;
        let mut updates: Vec<String> = items
            .iter()
            .skip(1)
            .map(|i| i["value"].as_str().unwrap_or("?").to_string())
            .collect();
        updates.sort();
        let mut want = langs.clone();
        want.sort();
        expect(
            updates == want,
            &format!("{what}: one update from each language"),
            &Value::Array(items),
        )?;
        pass(&what);
    }

    // 7. Caller identity and trace cross a nested send from a handler.
    for x in langs {
        let correlation = wire::new_id();
        let tp = traceparent(&trace);
        let token = authorization();
        let what = format!("{x} relays to the driver's probe");
        let sent = json!({"from": x, "nonce": wire::new_id()});
        call(
            &peer,
            Call {
                headers: vec![
                    ("correlationId", &correlation),
                    ("authorization", &token),
                    ("traceparent", &tp),
                ],
                ..command(
                    "conformance.Relay",
                    Some(&keys[x]),
                    json!({"target": "probe", "payload": sent}),
                )
            },
        )
        .await
        .map_err(|e| format!("{what}: {e}"))?;
        let seen = peer
            .seen()
            .into_iter()
            .find(|r| {
                r.r#type == "conformance.runner.Probe"
                    && r.headers.get("correlationId") == Some(&correlation)
            })
            .ok_or_else(|| {
                format!("{what}: the probe saw no request with correlation id {correlation}")
            })?;
        expect(
            wire::json(&seen.payload) == sent,
            &format!("{what}: the payload {sent} reaches the probe unchanged"),
            &wire::json(&seen.payload),
        )?;
        let headers = json!(seen.headers);
        expect(
            header(&headers, "authorization") == Some(&token),
            &format!("{what}: the authorization reaches the probe"),
            &headers,
        )?;
        expect(
            same_trace(&headers, &trace),
            &format!("{what}: the trace {trace} reaches the probe"),
            &headers,
        )?;
        pass(&what);
    }

    let reason = |l: &[String]| {
        format!(
            "{} {} their own types in-process, so these sends never reach the hub",
            l.join(", "),
            if l.len() == 1 { "answers" } else { "answer" }
        )
    };
    if !unexercised.is_empty() {
        println!(
            "note {} required exchange(s) not exercised: {unexercised:?}",
            unexercised.len()
        );
        if args.strict {
            return Err(format!(
                "--strict: {} exchange(s) not exercised through the hub ({}): {unexercised:?}",
                unexercised.len(),
                reason(&args.local_senders)
            ));
        }
    }
    let skipped = if unexercised.is_empty() {
        String::new()
    } else {
        format!(
            ", {} not exercised ({}): {unexercised:?}",
            unexercised.len(),
            reason(&args.local_senders)
        )
    };
    println!(
        "interop: {} languages, {} exchanges passed through the hub{skipped}, trace {}",
        langs.len(),
        exchanges,
        trace
    );
    Ok(())
}
