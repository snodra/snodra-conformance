//! The reference conformance service: the contract of `protocol/conformance.md` on
//! snodra-messaging-rs. The runner (`snodra-conformance`) starts it once per check with the
//! `CONFORMANCE_*` environment, drives it through a hub and reads what it reports.
//!
//! It is written the way an application would use the library: typed messages, handlers that
//! return errors with `?`, the library's own sender. Where the library falls short of a check,
//! the service does not work around it: the runner reports the gap.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snodra_messaging::hub::{ScatterOptions, Sender};
use snodra_messaging::{
    Config, Guard, HubOAuth2, HubTls, MessageContext, MessagingError, RetryPolicy, SecretString,
    SendOptions, SnodraMessaging, StreamOptions, SubscriptionOptions, message_type,
};
use tokio::sync::Notify;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Declares a message whose payload is any JSON document.
macro_rules! json_message {
    ($name:ident, $wire:literal) => {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        #[serde(transparent)]
        struct $name(Value);
        message_type!($name, $wire);
    };
}

// The types the service handles (conformance.md, "Message types").
json_message!(Echo, "conformance.Echo");
json_message!(EchoQuery, "conformance.EchoQuery");
json_message!(Done, "conformance.Done");
json_message!(Sleep, "conformance.Sleep");
json_message!(SleepQuery, "conformance.SleepQuery");
json_message!(Fail, "conformance.Fail");
json_message!(Relay, "conformance.Relay");
json_message!(SharedQuery, "conformance.SharedQuery");
json_message!(Stream, "conformance.Stream");
json_message!(Emit, "conformance.Emit");
json_message!(Send, "conformance.Send");
json_message!(Outcome, "conformance.Outcome");
json_message!(View, "conformance.View");

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Strict {
    count: i64,
}
message_type!(Strict, "conformance.Strict");

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Guarded {
    count: i64,
}
message_type!(Guarded, "conformance.Guarded");

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Watch {
    topic: String,
}
message_type!(Watch, "conformance.Watch");

// The types the runner serves (or nobody, for Missing), sent by `conformance.Send` and `Relay`.
json_message!(Probe, "conformance.runner.Probe");
json_message!(ProbeQuery, "conformance.runner.ProbeQuery");
json_message!(ProbeStream, "conformance.runner.ProbeStream");
json_message!(ProbeWatch, "conformance.runner.ProbeWatch");
json_message!(Missing, "conformance.runner.Missing");

impl snodra_messaging::Command for Echo {
    type Reply = Value;
}
impl snodra_messaging::Query for EchoQuery {
    type Reply = Value;
}
impl snodra_messaging::Command for Done {
    type Reply = ();
}
impl snodra_messaging::Command for Sleep {
    type Reply = Value;
}
impl snodra_messaging::Query for SleepQuery {
    type Reply = Value;
}
impl snodra_messaging::Command for Fail {
    type Reply = Value;
}
impl snodra_messaging::Command for Relay {
    type Reply = Value;
}
impl snodra_messaging::Command for Strict {
    type Reply = Value;
}
impl snodra_messaging::Command for Guarded {
    type Reply = Value;
}
impl snodra_messaging::Query for SharedQuery {
    type Reply = Value;
}
impl snodra_messaging::StreamingQuery for Stream {
    type Item = Value;
}
impl snodra_messaging::SubscriptionQuery for Watch {
    type Initial = Value;
    type Update = Value;
}
impl snodra_messaging::Command for Emit {
    type Reply = Value;
}
impl snodra_messaging::Command for Send {
    type Reply = Value;
}
impl snodra_messaging::Query for Outcome {
    type Reply = Value;
}
impl snodra_messaging::Query for View {
    type Reply = Value;
}
impl snodra_messaging::Command for Probe {
    type Reply = Value;
}
impl snodra_messaging::Query for ProbeQuery {
    type Reply = Value;
}
impl snodra_messaging::StreamingQuery for ProbeStream {
    type Item = Value;
}
impl snodra_messaging::SubscriptionQuery for ProbeWatch {
    type Initial = Value;
    type Update = Value;
}
impl snodra_messaging::Command for Missing {
    type Reply = Value;
}

/// A deliberate defect, for the runner's own tests only (`CONFORMANCE_REFERENCE_MUTATION`): each
/// breaks one rule the way a faulty library would, and the runner must report exactly the check
/// that proves the rule. Never set it to prove a library.
fn mutation(name: &str) -> bool {
    static MUTATION: OnceLock<Option<String>> = OnceLock::new();
    MUTATION
        .get_or_init(|| var("CONFORMANCE_REFERENCE_MUTATION"))
        .as_deref()
        == Some(name)
}

/// The `CONFORMANCE_*` environment the runner sets (conformance.md, "Starting the service").
struct Env {
    service: String,
    instance: String,
    hubs: String,
    capacity: usize,
    max_subscriptions: Option<usize>,
    credit_timeout_ms: Option<u64>,
    token: Option<String>,
    tls_ca: Option<PathBuf>,
    tls_server_name: Option<String>,
    tls_client: Option<(PathBuf, PathBuf)>,
    retry: Option<(u32, u64)>,
    jwks_url: Option<String>,
    issuer: Option<String>,
    oauth2_token_url: Option<String>,
    oauth2_client_id: Option<String>,
    oauth2_client_secret: Option<String>,
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn number<T: std::str::FromStr>(name: &str) -> Option<T> {
    var(name).map(|v| {
        v.trim()
            .parse()
            .unwrap_or_else(|_| panic!("{name} is not a number: {v}"))
    })
}

impl Env {
    fn read() -> Env {
        Env {
            service: var("CONFORMANCE_SERVICE_NAME").expect("CONFORMANCE_SERVICE_NAME is required"),
            instance: var("CONFORMANCE_INSTANCE_ID").expect("CONFORMANCE_INSTANCE_ID is required"),
            hubs: var("CONFORMANCE_HUBS").expect("CONFORMANCE_HUBS is required"),
            capacity: number("CONFORMANCE_CAPACITY").unwrap_or(64),
            max_subscriptions: number("CONFORMANCE_MAX_SUBSCRIPTIONS"),
            credit_timeout_ms: number("CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS"),
            token: var("CONFORMANCE_HUB_TOKEN"),
            tls_ca: var("CONFORMANCE_TLS_CA").map(PathBuf::from),
            tls_server_name: var("CONFORMANCE_TLS_SERVER_NAME"),
            tls_client: var("CONFORMANCE_TLS_CLIENT_CERT")
                .zip(var("CONFORMANCE_TLS_CLIENT_KEY"))
                .map(|(c, k)| (PathBuf::from(c), PathBuf::from(k))),
            retry: number("CONFORMANCE_RETRY_MAX_ATTEMPTS").map(|n| {
                (
                    n,
                    number("CONFORMANCE_RETRY_INITIAL_BACKOFF_MS").unwrap_or(100),
                )
            }),
            jwks_url: var("CONFORMANCE_JWKS_URL"),
            issuer: var("CONFORMANCE_ISSUER"),
            oauth2_token_url: var("CONFORMANCE_OAUTH2_TOKEN_URL"),
            oauth2_client_id: var("CONFORMANCE_OAUTH2_CLIENT_ID"),
            oauth2_client_secret: var("CONFORMANCE_OAUTH2_CLIENT_SECRET"),
        }
    }

    fn config(&self) -> Config {
        let mut c = Config::new(&self.service, &self.hubs);
        c.instance_id = self.instance.clone();
        c.max_concurrent_handlers = self.capacity;
        if mutation("capacity") {
            c.max_concurrent_handlers = self.capacity + 1;
        }
        if let Some(n) = self.max_subscriptions {
            c.hub_max_subscriptions = n;
        }
        if let Some(ms) = self.credit_timeout_ms {
            c.hub_stream_credit_timeout = Duration::from_millis(ms);
        }
        c.hub_token = self.token.as_deref().map(SecretString::new);
        if mutation("no-token") {
            c.hub_token = None;
        }
        if self.tls_ca.is_some() || self.tls_server_name.is_some() || self.tls_client.is_some() {
            c.hub_tls = Some(HubTls {
                ca_file: self.tls_ca.clone(),
                server_name: self.tls_server_name.clone(),
                client_cert_file: self.tls_client.as_ref().map(|(c, _)| c.clone()),
                client_key_file: self.tls_client.as_ref().map(|(_, k)| k.clone()),
            });
        }
        // The library's retry policy for every send (conformance.md: CONFORMANCE_RETRY_*).
        if let Some((attempts, backoff)) = self.retry {
            c.hub_retry = Some(RetryPolicy {
                max_attempts: attempts,
                initial_backoff: Duration::from_millis(backoff),
                ..RetryPolicy::default()
            });
        }
        if let Some(token_uri) = &self.oauth2_token_url {
            c.hub_oauth2 = Some(HubOAuth2 {
                token_uri: token_uri.clone(),
                client_id: self
                    .oauth2_client_id
                    .clone()
                    .unwrap_or_else(|| self.service.clone()),
                client_secret: self.oauth2_client_secret.as_deref().map(SecretString::new),
                ..HubOAuth2::default()
            });
        }
        c.jwks_url = self.jwks_url.clone();
        c.issuer = self.issuer.clone();
        // Commands and queries only: no event store, no message store (level 1).
        c.consumer_enabled = false;
        c.outbox_enabled = false;
        c.sagas_enabled = false;
        c
    }
}

/// What the handlers share: this instance's identity, the library's sender and emitter, the
/// asynchronous `conformance.Send` operations, and the started app (for `conformance.View`).
struct Shared {
    retry: bool,
    service: String,
    instance: String,
    sender: Arc<Sender>,
    emitter: snodra_messaging::QueryUpdateEmitter,
    ops: Mutex<HashMap<String, Option<Value>>>,
    op_done: Notify,
    app: OnceLock<SnodraMessaging>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,snodra_messaging=debug")
            }),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();
    let env = Env::read();
    let builder = SnodraMessaging::builder(env.config())
        .await
        .expect("the library refused the configuration");
    let shared = Arc::new(Shared {
        retry: env.retry.is_some(),
        service: env.service.clone(),
        instance: env.instance.clone(),
        sender: builder.sender(),
        emitter: builder.emitter(),
        ops: Mutex::new(HashMap::new()),
        op_done: Notify::new(),
        app: OnceLock::new(),
    });
    let s = Arc::clone(&shared);
    let builder = builder
        .command::<Echo, _, _>({
            let s = s.clone();
            move |m, ctx| {
                let s = s.clone();
                async move { Ok(s.echo(m.0, &ctx)) }
            }
        })
        .query::<EchoQuery, _, _>({
            let s = s.clone();
            move |m, ctx| {
                let s = s.clone();
                async move { Ok(s.echo(m.0, &ctx)) }
            }
        })
        .command::<Done, _, _>(|_, _| async { Ok(()) })
        .command::<Sleep, _, _>({
            let s = s.clone();
            move |m, _| {
                let s = s.clone();
                async move {
                    let ms = m.0["ms"].as_u64().unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(json!({ "instanceId": s.instance, "sleptMs": ms }))
                }
            }
        })
        .query::<SleepQuery, _, _>({
            let s = s.clone();
            move |m, _| {
                let s = s.clone();
                async move {
                    let ms = m.0["ms"].as_u64().unwrap_or(0);
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(json!({ "instanceId": s.instance, "sleptMs": ms }))
                }
            }
        })
        // An application failure with details: the Rust library's handler error is
        // `MessagingError::RemoteHandler` (its dispatcher maps a handler's failure to it, and its
        // example service raises it the same way); it has no other error that carries details.
        .command::<Fail, _, _>(|m, _| async move {
            let message = m.0["message"].as_str().unwrap_or("failed").to_string();
            let details = match &m.0["details"] {
                Value::Null => Vec::new(),
                d => serde_json::to_vec(d).unwrap(),
            };
            Err::<Value, _>(MessagingError::RemoteHandler {
                type_name: "conformance.Fail".into(),
                message,
                details,
            })
        })
        .command::<Strict, _, _>(|m, _| async move { Ok(json!({ "count": m.count })) })
        .command::<Guarded, _, _>(|m, ctx| async move {
            Ok(json!({ "count": m.count, "user": ctx.user_id() }))
        })
        // An authenticated caller, and not the subject the contract names as refused.
        .requires::<Guarded>(Guard::custom(|identity, _| {
            mutation("no-guard") || identity.user_id.as_deref() != Some("conformance-refused")
        }))
        .command::<Relay, _, _>({
            let s = s.clone();
            move |m, _| {
                let s = s.clone();
                // A nested call failing with `?`, as an application writes it: the library decides
                // what the caller sees.
                async move {
                    let reply = match m.0["target"].as_str() {
                        Some("missing") => {
                            s.sender
                                .send_command(
                                    &Missing(m.0["payload"].clone()),
                                    SendOptions::default(),
                                )
                                .await?
                        }
                        _ => {
                            s.sender
                                .send_command(
                                    &Probe(m.0["payload"].clone()),
                                    SendOptions {
                                        // A caller token instead of the handled request's.
                                        authorization: mutation("override-authorization")
                                            .then(|| "Bearer mutated".to_string()),
                                        ..SendOptions::default()
                                    },
                                )
                                .await?
                        }
                    };
                    Ok(json!({ "reply": reply }))
                }
            }
        })
        .shared_query::<SharedQuery, _, _>({
            let s = s.clone();
            move |_, _| {
                let s = s.clone();
                async move { Ok(json!({ "service": s.service, "instanceId": s.instance })) }
            }
        })
        .streaming_query::<Stream, _, _, _>(|m, _| async move {
            let count = m.0["count"].as_u64().unwrap_or(0);
            let delay = Duration::from_millis(m.0["delayMs"].as_u64().unwrap_or(0));
            Ok(futures::stream::unfold(1u64, move |n| async move {
                if n > count {
                    return None;
                }
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                Some((Ok(json!({ "n": n })), n + 1))
            }))
        })
        .subscription_query::<Watch, _, _>({
            let s = s.clone();
            move |m, _| {
                let s = s.clone();
                async move { Ok(json!({ "topic": m.topic, "value": 0, "instanceId": s.instance })) }
            }
        })
        .command::<Emit, _, _>({
            let s = s.clone();
            move |m, _| {
                let s = s.clone();
                async move {
                    let topic = m.0["topic"].as_str().unwrap_or_default().to_string();
                    let matches = |w: &Watch| w.topic == topic;
                    for v in m.0["values"].as_array().cloned().unwrap_or_default() {
                        s.emitter
                            .emit::<Watch>(matches, &json!({ "topic": topic, "value": v }));
                    }
                    if let Some(reason) = m.0["fail"].as_str() {
                        s.emitter.complete_exceptionally::<Watch>(
                            matches,
                            &MessagingError::RemoteHandler {
                                type_name: "conformance.Watch".into(),
                                message: reason.to_string(),
                                details: Vec::new(),
                            },
                        );
                    } else if m.0["complete"].as_bool() == Some(true) {
                        s.emitter.complete::<Watch>(matches);
                    }
                    Ok(json!({}))
                }
            }
        })
        .command::<Send, _, _>({
            let s = s.clone();
            move |m, _| {
                let s = s.clone();
                async move { Ok(s.send(m.0).await) }
            }
        })
        .query::<Outcome, _, _>({
            let s = s.clone();
            move |m, _| {
                let s = s.clone();
                async move {
                    let id = m.0["opId"].as_str().unwrap_or_default().to_string();
                    let wait = Duration::from_millis(m.0["waitMs"].as_u64().unwrap_or(0));
                    Ok(s.outcome(&id, wait).await)
                }
            }
        })
        .query::<View, _, _>({
            let s = s.clone();
            move |_, _| {
                let s = s.clone();
                async move { Ok(s.view()) }
            }
        });
    let app = builder.start().await.expect("the service did not start");
    let _ = shared.app.set(app);
    shutdown_signal().await;
    // Hand the app back for a graceful shutdown (no handler holds it).
    if let Ok(shared) = Arc::try_unwrap(shared)
        && let Some(app) = shared.app.into_inner()
    {
        app.shutdown().await;
    }
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

impl Shared {
    fn echo(&self, payload: Value, ctx: &MessageContext) -> Value {
        json!({
            "payload": payload,
            "headers": ctx.headers,
            "instanceId": self.instance,
            "requestId": ctx.message_id,
        })
    }

    fn view(&self) -> Value {
        json!({
            "service": self.service,
            "instanceId": self.instance,
            "hubIds": self.app.get().map(|a| a.hub_ids()).unwrap_or_default(),
            "lastRejection": if mutation("hide-rejection") { None } else { self.latest_rejection() },
            "subscriptionsServed": self.emitter.open_subscriptions(),
            // A policy is configured (CONFORMANCE_RETRY_*); the Rust library also takes one per call.
            "retry": self.retry,
        })
    }

    /// The newest refusal of this instance's registration by any hub, as the library keeps it
    /// (`SnodraMessaging::latest_rejection`: never cleared by a later acceptance), `CODE: message`.
    fn latest_rejection(&self) -> Option<String> {
        let rejection = self.app.get()?.latest_rejection()?;
        Some(format!(
            "{}: {}",
            rejection.code,
            rejection.description.unwrap_or_default()
        ))
    }

    /// `conformance.Send`: synchronous, or started and reported later by `conformance.Outcome`.
    async fn send(self: &Arc<Self>, spec: Value) -> Value {
        if spec["async"].as_bool() != Some(true) {
            return self.run(&spec).await;
        }
        let id = uuid::Uuid::new_v4().to_string();
        self.ops.lock().unwrap().insert(id.clone(), None);
        let me = Arc::clone(self);
        let op = id.clone();
        tokio::spawn(async move {
            let outcome = me.run(&spec).await;
            me.ops.lock().unwrap().insert(op, Some(outcome));
            me.op_done.notify_waiters();
        });
        json!({ "opId": id })
    }

    async fn outcome(&self, id: &str, wait: Duration) -> Value {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.op_done.notified();
            match self.ops.lock().unwrap().get(id) {
                None => return json!({ "unknownOp": id }),
                Some(Some(outcome)) => return outcome.clone(),
                Some(None) => {}
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return json!({ "pending": true });
            }
        }
    }

    async fn run(&self, spec: &Value) -> Value {
        let started = Instant::now();
        let mut outcome = match spec["op"].as_str().unwrap_or("unary") {
            "unary" => self.unary(spec).await,
            "scatter" => self.scatter(spec).await,
            "stream" => self.stream(spec).await,
            "subscribe" => self.subscribe(spec).await,
            other => json!({ "result": "unsupported", "reason": format!("op {other}") }),
        };
        outcome["elapsedMs"] = json!(started.elapsed().as_millis() as u64);
        outcome
    }

    fn options(spec: &Value) -> Result<SendOptions, Value> {
        let mut options = SendOptions {
            routing_key: spec["routingKey"].as_str().map(str::to_string),
            timeout: spec["timeoutMs"]
                .as_u64()
                .map(Duration::from_millis)
                .filter(|_| !mutation("ignore-timeout")),
            ..SendOptions::default()
        };
        if let Some(headers) = spec["headers"].as_object() {
            options.headers = headers
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
        }
        if let Some(retry) = spec.get("retry").filter(|r| r.is_object()) {
            options.retry = Some(RetryPolicy {
                max_attempts: retry["maxAttempts"].as_u64().unwrap_or(1) as u32,
                initial_backoff: Duration::from_millis(
                    retry["initialBackoffMs"].as_u64().unwrap_or(100),
                ),
                ..RetryPolicy::default()
            });
        }
        Ok(options)
    }

    async fn unary(&self, spec: &Value) -> Value {
        let options = match Self::options(spec) {
            Ok(o) => o,
            Err(v) => return v,
        };
        let payload = spec["payload"].clone();
        let reply = match spec["target"].as_str().unwrap_or("probe") {
            "probe" => self.sender.send_command(&Probe(payload), options).await,
            "probe-query" => self.sender.send_query(&ProbeQuery(payload), options).await,
            "missing" => self.sender.send_command(&Missing(payload), options).await,
            "echo" => self.sender.send_command(&Echo(payload), options).await,
            "echo-query" => self.sender.send_query(&EchoQuery(payload), options).await,
            "shared-query" => self.sender.send_query(&SharedQuery(payload), options).await,
            other => return json!({ "result": "unsupported", "reason": format!("target {other}") }),
        };
        match reply {
            Ok(payload) => json!({ "result": "ok", "payload": payload }),
            Err(e) => failed(&e),
        }
    }

    async fn scatter(&self, spec: &Value) -> Value {
        let options = ScatterOptions {
            timeout: spec["timeoutMs"].as_u64().map(Duration::from_millis),
            max_results: spec["maxResults"].as_u64().unwrap_or(0) as u32,
            priority: None,
        };
        let payload = spec["payload"].clone();
        let result = match spec["target"].as_str().unwrap_or("probe-query") {
            "probe-query" => {
                self.sender
                    .scatter_gather(&ProbeQuery(payload), options)
                    .await
            }
            "echo-query" => {
                self.sender
                    .scatter_gather(&EchoQuery(payload), options)
                    .await
            }
            "shared-query" => {
                self.sender
                    .scatter_gather(&SharedQuery(payload), options)
                    .await
            }
            other => return json!({ "result": "unsupported", "reason": format!("target {other}") }),
        };
        match result {
            Ok(r) => {
                let parts: Vec<Value> = r
                    .parts
                    .into_iter()
                    .map(|p| match p.outcome {
                        Ok(payload) => json!({
                            "service": p.service, "instance": p.instance, "ok": true, "payload": payload,
                        }),
                        Err(e) => json!({
                            "service": p.service, "instance": p.instance, "ok": false,
                            "error": { "kind": "code", "code": code_name(e.code as i32), "message": e.message },
                        }),
                    })
                    .collect();
                json!({ "result": "ok", "parts": parts, "failed": r.failed, "timedOut": r.timed_out })
            }
            Err(e) => failed(&e),
        }
    }

    async fn stream(&self, spec: &Value) -> Value {
        let options = StreamOptions {
            timeout: spec["timeoutMs"].as_u64().map(Duration::from_millis),
            max_results: spec["maxResults"].as_u64().unwrap_or(0) as u32,
            priority: None,
            initial_credit: spec["initialCredit"].as_u64().unwrap_or(0) as u32,
        };
        let payload = spec["payload"].clone();
        let opened = match spec["target"].as_str().unwrap_or("probe-stream") {
            "probe-stream" => {
                self.sender
                    .stream_owned(&ProbeStream(payload), options)
                    .await
            }
            "stream" => self.sender.stream_owned(&Stream(payload), options).await,
            other => return json!({ "result": "unsupported", "reason": format!("target {other}") }),
        };
        match opened {
            Ok(items) => read_items(items, spec, Vec::new()).await,
            Err(e) => failed(&e),
        }
    }

    async fn subscribe(&self, spec: &Value) -> Value {
        let options = SubscriptionOptions {
            timeout: spec["timeoutMs"].as_u64().map(Duration::from_millis),
            max_results: spec["maxResults"].as_u64().unwrap_or(0) as u32,
            priority: None,
            initial_credit: spec["initialCredit"].as_u64().unwrap_or(0) as u32,
        };
        let payload = spec["payload"].clone();
        match spec["target"].as_str().unwrap_or("probe-watch") {
            "probe-watch" => match self.sender.subscribe(&ProbeWatch(payload), options).await {
                Ok(sub) => read_items(sub.updates, spec, vec![sub.initial]).await,
                Err(e) => failed(&e),
            },
            "watch" => {
                let query = match serde_json::from_value::<Watch>(payload) {
                    Ok(q) => q,
                    Err(e) => return json!({ "result": "unsupported", "reason": e.to_string() }),
                };
                match self.sender.subscribe(&query, options).await {
                    Ok(sub) => read_items(sub.updates, spec, vec![sub.initial]).await,
                    Err(e) => failed(&e),
                }
            }
            other => json!({ "result": "unsupported", "reason": format!("target {other}") }),
        }
    }
}

/// Reads a stream's items (after `first`, a subscription's initial result) until it ends, or
/// until `read` items were read: then the stream is dropped, which cancels it.
async fn read_items<S>(mut items: S, spec: &Value, first: Vec<Value>) -> Value
where
    S: futures::Stream<Item = Result<Value, MessagingError>> + Unpin,
{
    let read = spec["read"].as_u64().map(|n| n as usize);
    let delay = Duration::from_millis(spec["readDelayMs"].as_u64().unwrap_or(0));
    let mut got = first;
    loop {
        if read.is_some_and(|n| got.len() >= n) {
            drop(items);
            return json!({ "result": "ok", "items": got, "end": "stopped" });
        }
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        match items.next().await {
            Some(Ok(v)) => got.push(v),
            Some(Err(e)) => {
                let mut out = failed(&e);
                out["items"] = json!(got);
                return out;
            }
            None => return json!({ "result": "ok", "items": got, "end": "completed" }),
        }
    }
}

/// A failed call as the contract's outcome: the error's kind, its code and what it kept.
fn failed(e: &MessagingError) -> Value {
    let (kind, code) = match e {
        MessagingError::HubUnavailable(_) if mutation("never-sent-as-unknown") => {
            ("outcome-unknown", None)
        }
        MessagingError::HubUnavailable(_) => ("never-sent", None),
        MessagingError::HubConnectionLost(_) => ("outcome-unknown", None),
        MessagingError::NoHandler(_) => ("code", Some("NO_HANDLER")),
        MessagingError::Timeout(_) => ("code", Some("TIMEOUT")),
        MessagingError::HandlerDisconnected(_) => ("code", Some("HANDLER_DISCONNECTED")),
        MessagingError::HandlerBusy(_) => ("code", Some("RESOURCE_EXHAUSTED")),
        MessagingError::RemoteHandler { .. } => ("code", Some("HANDLER_ERROR")),
        MessagingError::Unauthenticated(_) => ("code", Some("UNAUTHENTICATED")),
        MessagingError::AccessDenied(_) => ("code", Some("PERMISSION_DENIED")),
        MessagingError::Cancelled(_) => ("code", Some("CANCELLED")),
        // The Rust library has no typed error for these two: it reports a gap or a short end as
        // `InvalidMessage` "... lost N part(s)", and a header it owns as `InvalidMessage` "... is
        // set by the library ..." before sending, so only their text tells them apart.
        MessagingError::InvalidMessage(m) if m.contains(" lost ") && m.contains("part(s)") => {
            ("lost-parts", None)
        }
        MessagingError::InvalidMessage(m) if m.contains("is set by the library") => {
            ("refused", None)
        }
        MessagingError::InvalidMessage(_) => ("code", Some("INVALID_REQUEST")),
        _ => ("local", None),
    };
    let details = match e {
        MessagingError::RemoteHandler { details, .. } if !details.is_empty() => {
            Some(String::from_utf8_lossy(details).into_owned())
        }
        _ => None,
    };
    json!({
        "result": "error",
        "error": { "kind": kind, "code": code, "message": e.to_string(), "details": details },
    })
}

fn code_name(code: i32) -> &'static str {
    match code {
        1 => "NO_HANDLER",
        2 => "TIMEOUT",
        3 => "HANDLER_DISCONNECTED",
        4 => "HANDLER_ERROR",
        5 => "UNAUTHENTICATED",
        6 => "INVALID_REQUEST",
        7 => "PERMISSION_DENIED",
        8 => "RESOURCE_EXHAUSTED",
        9 => "CANCELLED",
        _ => "HANDLER_ERROR",
    }
}
