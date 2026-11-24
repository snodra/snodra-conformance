//! The violating client: a conformance service written frame by frame on hub.proto that follows
//! the protocol closely enough for one check, and breaks exactly one rule of it, named by
//! `CONFORMANCE_VIOLATION`. The runner's tests (`tests/mutations.rs`) expect that check, and only
//! it, to fail. It is not a library and not an example: only the paths the target checks walk.
//!
//! Violations: heartbeat-first, live-on-echo, ignore-interval, keep-silent, changed-register,
//! no-backoff, signed-rendezvous, sticky-unkeyed, no-permits, unbatched-permits, seq-from-zero,
//! no-signature-check, no-tls-verify, mtls-no-reload, renew-late, keep-subscriptions,
//! no-subscription-limit, retry-after-timeout, case-sensitive-headers, no-subscription-types,
//! drop-silent-early, token-after-lookup, wrong-mode-code.
//!
//! Without a violation it is the baseline the mutation test compares against: every check of the
//! target's group keeps the verdict it has without the violation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde_json::{Value, json};
use snodra_conformance::proto::hub_client::HubClient;
use snodra_conformance::proto::{
    ClientMessage, Credentials, Error, Heartbeat, Permits, Register, Reply, ReplyEnd, ReplyPart,
    Request, RequestKind, RequestMode, ServerMessage, client_message, credentials, reply,
    server_message,
};
use snodra_conformance::{rendezvous, wire};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn violation(name: &str) -> bool {
    var("CONFORMANCE_VIOLATION").as_deref() == Some(name)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

type Out = mpsc::Sender<ClientMessage>;

/// One hub: its current stream (generation, sender), what it acknowledged.
struct Conn {
    addr: String,
    hub_id: Mutex<Option<String>>,
    acked: AtomicBool,
    echoed: AtomicBool,
    generation: AtomicU64,
    out: Mutex<Option<Out>>,
    grants: Mutex<Grants>,
}

/// Permit grants of the current stream (protocol.md, Flow control).
#[derive(Default)]
struct Grants {
    initial: u32,
    received: u32,
    returned: u32,
    pending: u32,
    timer: bool,
}

struct Sub {
    topic: String,
    conn: usize,
    generation: u64,
    seq: u64,
}

struct Running {
    credits: Arc<(Mutex<u32>, Notify)>,
    cancelled: Arc<AtomicBool>,
    conn: usize,
    generation: u64,
}

struct State {
    service: String,
    instance: String,
    capacity: u32,
    max_subs: usize,
    retry: Option<(u32, u64)>,
    conns: Vec<Arc<Conn>>,
    subs: Mutex<HashMap<String, Sub>>,
    running: Mutex<HashMap<String, Running>>,
    pending: Mutex<HashMap<String, (usize, oneshot::Sender<ServerMessage>)>>,
    round_robin: AtomicUsize,
    jwt: Mutex<Option<String>>,
    in_flight: AtomicUsize,
    /// The client certificate and key as read at start (`mtls-no-reload` never reads them again).
    client_at_start: Option<(String, String)>,
    jwks: Mutex<Option<Value>>,
    ops: Mutex<HashMap<String, Option<Value>>>,
    op_done: Notify,
}

fn client_files() -> Option<(String, String)> {
    let cert = std::fs::read_to_string(var("CONFORMANCE_TLS_CLIENT_CERT")?).ok()?;
    let key = std::fs::read_to_string(var("CONFORMANCE_TLS_CLIENT_KEY")?).ok()?;
    Some((cert, key))
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let hubs: Vec<String> = var("CONFORMANCE_HUBS")
        .unwrap_or_default()
        .split(',')
        .map(str::to_string)
        .collect();
    let st = Arc::new(State {
        service: var("CONFORMANCE_SERVICE_NAME").unwrap_or_default(),
        instance: var("CONFORMANCE_INSTANCE_ID").unwrap_or_default(),
        capacity: var("CONFORMANCE_CAPACITY")
            .and_then(|v| v.parse().ok())
            .unwrap_or(16),
        max_subs: var("CONFORMANCE_MAX_SUBSCRIPTIONS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024),
        retry: var("CONFORMANCE_RETRY_MAX_ATTEMPTS")
            .and_then(|v| v.parse().ok())
            .map(|n| {
                (
                    n,
                    var("CONFORMANCE_RETRY_INITIAL_BACKOFF_MS")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(100),
                )
            }),
        conns: hubs
            .iter()
            .map(|a| {
                Arc::new(Conn {
                    addr: a.clone(),
                    hub_id: Mutex::new(None),
                    acked: AtomicBool::new(false),
                    echoed: AtomicBool::new(false),
                    generation: AtomicU64::new(0),
                    out: Mutex::new(None),
                    grants: Mutex::new(Grants::default()),
                })
            })
            .collect(),
        subs: Mutex::new(HashMap::new()),
        running: Mutex::new(HashMap::new()),
        pending: Mutex::new(HashMap::new()),
        round_robin: AtomicUsize::new(0),
        jwt: Mutex::new(None),
        in_flight: AtomicUsize::new(0),
        client_at_start: client_files(),
        jwks: Mutex::new(None),
        ops: Mutex::new(HashMap::new()),
        op_done: Notify::new(),
    });
    if let Some(url) = var("CONFORMANCE_OAUTH2_TOKEN_URL") {
        let st = Arc::clone(&st);
        tokio::spawn(async move { renew_tokens(st, url).await });
    }
    for i in 0..st.conns.len() {
        let st = Arc::clone(&st);
        tokio::spawn(async move { run_conn(st, i).await });
    }
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    term.recv().await;
}

/// Fetches a token (OAuth2 client credentials) and renews it at 80 % of its lifetime (120 %, too
/// late, with `renew-late`), sending each new one on the live streams.
async fn renew_tokens(st: Arc<State>, url: String) {
    loop {
        let Some((token, lifetime)) = fetch_token(&url).await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let first = st.jwt.lock().unwrap().replace(token.clone()).is_none();
        if !first {
            for c in &st.conns {
                if let Some(out) = c.out.lock().unwrap().clone() {
                    let _ = out.try_send(wire::client(client_message::Message::Credentials(
                        Credentials {
                            kind: Some(credentials::Kind::Jwt(token.clone())),
                        },
                    )));
                }
            }
        }
        let factor = if violation("renew-late") { 1.2 } else { 0.8 };
        tokio::time::sleep(lifetime.mul_f64(factor)).await;
    }
}

/// A plain HTTP GET (no form) or form POST, its JSON answer.
async fn http(url: &str, form: Option<&str>) -> Option<Value> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.strip_prefix("http://")?;
    let (host, path) = rest.split_once('/').map(|(h, p)| (h, format!("/{p}")))?;
    let mut s = tokio::net::TcpStream::connect(host).await.ok()?;
    let req = match form {
        Some(body) => format!(
            "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
        None => format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"),
    };
    s.write_all(req.as_bytes()).await.ok()?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).await.ok()?;
    let text = String::from_utf8_lossy(&raw);
    serde_json::from_str(text.split_once("\r\n\r\n")?.1).ok()
}

async fn fetch_token(url: &str) -> Option<(String, Duration)> {
    let json = http(
        url,
        Some(
            "grant_type=client_credentials&client_id=conformance&client_secret=conformance-secret",
        ),
    )
    .await?;
    Some((
        json["access_token"].as_str()?.to_string(),
        Duration::from_secs(json["expires_in"].as_u64().unwrap_or(60)),
    ))
}

/// The channel to a hub, and the client certificate it presents (if any).
async fn channel(
    st: &State,
    addr: &str,
) -> Option<(tonic::transport::Channel, Option<(String, String)>)> {
    let Some(ca) = var("CONFORMANCE_TLS_CA") else {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).ok()?;
        return Some((endpoint.connect().await.ok()?, None));
    };
    if !violation("no-tls-verify") {
        let client = if violation("mtls-no-reload") {
            st.client_at_start.clone()
        } else {
            client_files()
        };
        let mut tls = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(
                std::fs::read_to_string(ca).ok()?,
            ))
            .domain_name(var("CONFORMANCE_TLS_SERVER_NAME").unwrap_or_default());
        if let Some((cert, key)) = &client {
            tls = tls.identity(tonic::transport::Identity::from_pem(cert, key));
        }
        let endpoint = tonic::transport::Endpoint::from_shared(format!("https://{addr}"))
            .ok()?
            .tls_config(tls)
            .ok()?;
        return Some((endpoint.connect().await.ok()?, client));
    }
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).ok()?;
    // TLS that trusts any certificate: only `no-tls-verify` comes here.
    let builder = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyCert));
    // The client certificate of the files, when there is one (the violation is the server's).
    let presented = client_files();
    let client = presented.clone().and_then(|(cert, key)| {
        use rustls::pki_types::pem::PemObject;
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls::pki_types::CertificateDer::pem_slice_iter(cert.as_bytes())
                .collect::<Result<_, _>>()
                .ok()?;
        let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(key.as_bytes()).ok()?;
        Some((certs, key))
    });
    let mut config = match client {
        Some((certs, key)) => builder.with_client_auth_cert(certs, key).ok()?,
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"h2".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let addr = addr.to_string();
    endpoint
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let connector = connector.clone();
            let addr = addr.clone();
            async move {
                let tcp = tokio::net::TcpStream::connect(&addr).await?;
                let name = rustls::pki_types::ServerName::try_from("conformance-hub.test")
                    .unwrap()
                    .to_owned();
                let tls = connector.connect(name, tcp).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(tls))
            }
        }))
        .await
        .ok()
        .map(|c| (c, presented))
}

#[derive(Debug)]
struct AnyCert;

impl rustls::client::danger::ServerCertVerifier for AnyCert {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

async fn run_conn(st: Arc<State>, i: usize) {
    let mut backoff = Duration::from_millis(500);
    let mut attempt = 0u32;
    loop {
        if one_stream(&st, i, attempt).await {
            backoff = Duration::from_millis(500);
        }
        attempt += 1;
        let wait = if violation("no-backoff") {
            Duration::from_millis(50)
        } else {
            backoff
        };
        tokio::time::sleep(wait).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

/// The handlers it serves, by kind (for the answers to a request in another mode).
const COMMANDS: [&str; 8] = [
    "Echo", "Sleep", "Emit", "Guarded", "Send", "Fail", "Strict", "Relay",
];
const UNARY_QUERIES: [&str; 4] = ["EchoQuery", "SleepQuery", "View", "Outcome"];
const STREAMING_QUERIES: [&str; 1] = ["Stream"];
const SUBSCRIPTION_QUERIES: [&str; 1] = ["Watch"];

/// The answer protocol.md gives a request in a mode its handler does not serve, or `None` when
/// the handler serves it. With `wrong-mode-code`, a stream of a command type is
/// INVALID_REQUEST instead of NO_HANDLER.
fn wrong_mode(r: &Request) -> Option<(i32, String)> {
    const NO_HANDLER: i32 = 1;
    const INVALID_REQUEST: i32 = 6;
    let name = r.r#type.strip_prefix("conformance.").unwrap_or("");
    let is = |set: &[&str]| set.contains(&name);
    let wrong = |what: &str| format!("{} {what}", r.r#type);
    if r.mode == RequestMode::Subscription as i32 {
        return (!is(&SUBSCRIPTION_QUERIES)).then(|| {
            (
                INVALID_REQUEST,
                format!("no subscription handler for {} on this instance", r.r#type),
            )
        });
    }
    if r.mode == RequestMode::Streaming as i32 {
        return if is(&STREAMING_QUERIES) {
            None
        } else if is(&UNARY_QUERIES) || is(&SUBSCRIPTION_QUERIES) {
            Some((INVALID_REQUEST, wrong("is not a streaming query")))
        } else if is(&COMMANDS) && violation("wrong-mode-code") {
            Some((INVALID_REQUEST, wrong("is a command")))
        } else {
            Some((NO_HANDLER, format!("no handler for type {}", r.r#type)))
        };
    }
    if r.kind == RequestKind::Command as i32 {
        return (!is(&COMMANDS)).then(|| (NO_HANDLER, format!("no handler for type {}", r.r#type)));
    }
    if r.kind != RequestKind::Query as i32 {
        return Some((
            INVALID_REQUEST,
            format!("unsupported request kind {}", r.kind),
        ));
    }
    if is(&UNARY_QUERIES) || (is(&SUBSCRIPTION_QUERIES) && r.id.ends_with("#0")) {
        None
    } else if is(&STREAMING_QUERIES) || is(&SUBSCRIPTION_QUERIES) {
        Some((INVALID_REQUEST, wrong("is not a unary query")))
    } else {
        Some((NO_HANDLER, format!("no handler for type {}", r.r#type)))
    }
}

fn all_types() -> (Vec<String>, Vec<String>) {
    let commands = COMMANDS;
    let queries = [
        "EchoQuery",
        "SleepQuery",
        "View",
        "Outcome",
        "Stream",
        "Watch",
    ];
    let full = |n: &&str| format!("conformance.{n}");
    (
        commands.iter().map(full).collect(),
        queries.iter().map(full).collect(),
    )
}

/// One stream to hub `i`; whether it reached the ack.
async fn one_stream(st: &Arc<State>, i: usize, attempt: u32) -> bool {
    let conn = Arc::clone(&st.conns[i]);
    let Some((ch, presented)) = channel(st, &conn.addr).await else {
        return false;
    };
    let (tx, rx) = mpsc::channel(256);
    let generation = conn.generation.fetch_add(1, Ordering::SeqCst) + 1;
    let (commands, queries) = all_types();
    let mut permits = st.capacity;
    if violation("changed-register") {
        permits += attempt;
    }
    // With a token endpoint, no stream before the first token.
    while var("CONFORMANCE_OAUTH2_TOKEN_URL").is_some() && st.jwt.lock().unwrap().is_none() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let jwt = st.jwt.lock().unwrap().clone();
    let token = var("CONFORMANCE_HUB_TOKEN");
    let register = Register {
        service_name: st.service.clone(),
        instance_id: st.instance.clone(),
        command_types: commands,
        query_types: queries,
        // Its one subscription type (none with `no-subscription-types`).
        subscription_query_types: if violation("no-subscription-types") {
            Vec::new()
        } else {
            vec!["conformance.Watch".into()]
        },
        initial_permits: permits,
        client_version: "conformance-violator/0".into(),
        credentials: match (jwt, token) {
            (Some(j), _) => Some(Credentials {
                kind: Some(credentials::Kind::Jwt(j)),
            }),
            (None, Some(t)) => Some(Credentials {
                kind: Some(credentials::Kind::Token(t)),
            }),
            _ => None,
        },
        ..Register::default()
    };
    let hb = || {
        wire::client(client_message::Message::Heartbeat(Heartbeat {
            sent_at_ms: now_ms(),
        }))
    };
    let reg = wire::client(client_message::Message::Register(register));
    if violation("heartbeat-first") {
        let _ = tx.send(hb()).await;
        let _ = tx.send(reg).await;
    } else {
        let _ = tx.send(reg).await;
        let _ = tx.send(hb()).await;
    }
    let Ok(resp) = HubClient::new(ch).connect(ReceiverStream::new(rx)).await else {
        return false;
    };
    let mut inbound = resp.into_inner();
    *conn.grants.lock().unwrap() = Grants {
        initial: permits,
        ..Grants::default()
    };
    *conn.out.lock().unwrap() = Some(tx.clone());
    // `rare-status`: one report at once (an extra report, allowed), then every 7 s (too rare).
    // `status-stops`: one report at once, one 3 s later, then none.
    let mut status = if violation("rare-status") {
        tokio::time::interval(Duration::from_secs(7))
    } else if violation("status-stops") {
        tokio::time::interval(Duration::from_secs(3))
    } else {
        let mut every_5_s = tokio::time::interval(Duration::from_secs(5));
        every_5_s.tick().await;
        every_5_s
    };
    let mut files = tokio::time::interval(Duration::from_secs(1));
    conn.echoed.store(false, Ordering::SeqCst);
    let mut interval = Duration::from_millis(3000);
    let mut last_echo = Instant::now();
    // Heartbeats written since the last echo (the ack counts as one).
    let mut unanswered = 0u32;
    let mut reached = false;
    let opened = Instant::now();
    let mut statuses_sent = 0u32;
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = status.tick() => {
                if violation("status-stops") && statuses_sent == 2 {
                    continue;
                }
                statuses_sent += 1;
                let hubs: Vec<String> = st
                    .conns
                    .iter()
                    .filter(|c| c.acked.load(Ordering::SeqCst))
                    .filter_map(|c| c.hub_id.lock().unwrap().clone())
                    .collect();
                let _ = tx
                    .send(wire::client(client_message::Message::Status(
                        snodra_conformance::proto::InstanceStatus {
                            connected_hub_ids: hubs,
                            handlers_in_flight: st.in_flight.load(Ordering::SeqCst) as u32,
                            processors: vec![],
                        },
                    )))
                    .await;
            }
            _ = files.tick() => {
                // A renewed client certificate: reconnect with it (not with `mtls-no-reload`).
                if presented.is_some() && !violation("mtls-no-reload") && client_files() != presented {
                    break;
                }
            }
            _ = ticker.tick() => {
                // Silent after the ack: more than three intervals since the last echo and three
                // heartbeats unanswered, looked at before this tick's heartbeat. With
                // `drop-silent-early` two unanswered heartbeats are enough: it breaks the rule
                // that three must be unanswered.
                let silent = if violation("drop-silent-early") {
                    unanswered >= 2
                } else {
                    unanswered >= 3 && last_echo.elapsed() > interval * 3
                };
                if reached && !violation("keep-silent") && silent {
                    break;
                }
                let _ = tx.send(hb()).await;
                if reached {
                    unanswered += 1;
                }
                // The handshake timeout: no ack within 10 s, close and reopen.
                if !reached && opened.elapsed() > Duration::from_secs(10) {
                    break;
                }
            }
            msg = inbound.message() => {
                let Ok(Some(msg)) = msg else { break };
                use server_message::Message as S;
                match msg.message.clone() {
                    Some(S::RegisterAck(ack)) => {
                        reached = true;
                        conn.acked.store(true, Ordering::SeqCst);
                        *conn.hub_id.lock().unwrap() = Some(ack.hub_id.clone());
                        if !violation("ignore-interval") {
                            interval = Duration::from_millis(ack.heartbeat_interval_ms.max(100).into());
                        }
                        ticker = tokio::time::interval(interval);
                        ticker.tick().await;
                        last_echo = Instant::now();
                        unanswered = 0;
                    }
                    Some(S::Heartbeat(_)) => {
                        if reached {
                            last_echo = Instant::now();
                            unanswered = 0;
                        }
                        conn.echoed.store(true, Ordering::SeqCst);
                    }
                    Some(S::Request(r)) => {
                        if r.mode != RequestMode::Subscription as i32 {
                            conn.grants.lock().unwrap().received += 1;
                            st.in_flight.fetch_add(1, Ordering::SeqCst);
                        }
                        let st = Arc::clone(st);
                        tokio::spawn(async move { handle(st, i, generation, r).await });
                    }
                    Some(S::Flow(f)) => {
                        if let Some(r) = st.running.lock().unwrap().get(&f.request_id) {
                            *r.credits.0.lock().unwrap() += f.credits;
                            r.credits.1.notify_waiters();
                        }
                    }
                    Some(S::Cancel(c)) => {
                        st.subs.lock().unwrap().remove(&c.request_id);
                        if let Some(r) = st.running.lock().unwrap().remove(&c.request_id) {
                            r.cancelled.store(true, Ordering::SeqCst);
                            r.credits.1.notify_waiters();
                        }
                    }
                    Some(S::Reply(r)) => answer(st, &r.request_id, msg),
                    Some(S::Error(e)) => answer(st, &e.request_id, msg),
                    _ => {}
                }
            }
        }
    }
    *conn.out.lock().unwrap() = None;
    conn.acked.store(false, Ordering::SeqCst);
    if !violation("keep-subscriptions") {
        st.subs
            .lock()
            .unwrap()
            .retain(|_, s| !(s.conn == i && s.generation == generation));
    }
    let lost: Vec<String> = st
        .running
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, r)| r.conn == i && r.generation == generation)
        .map(|(id, _)| id.clone())
        .collect();
    for id in lost {
        if let Some(r) = st.running.lock().unwrap().remove(&id) {
            r.cancelled.store(true, Ordering::SeqCst);
            r.credits.1.notify_waiters();
        }
    }
    st.pending.lock().unwrap().retain(|_, (c, _)| *c != i);
    reached
}

fn answer(st: &State, id: &str, msg: ServerMessage) {
    if let Some((_, tx)) = st.pending.lock().unwrap().remove(id) {
        let _ = tx.send(msg);
    }
}

/// Sends on hub `i`'s stream of `generation` (dropped when that stream is gone).
fn send_on(st: &State, i: usize, generation: u64, m: client_message::Message) {
    let c = &st.conns[i];
    if c.generation.load(Ordering::SeqCst) != generation {
        return;
    }
    if let Some(out) = c.out.lock().unwrap().clone() {
        let _ = out.try_send(wire::client(m));
    }
}

fn reply_ok(id: &str, v: &Value) -> client_message::Message {
    client_message::Message::Reply(Reply {
        request_id: id.into(),
        result: Some(reply::Result::Payload(serde_json::to_vec(v).unwrap())),
    })
}

fn reply_err(id: &str, code: i32, message: &str) -> client_message::Message {
    client_message::Message::Reply(Reply {
        request_id: id.into(),
        result: Some(reply::Result::Error(Error {
            request_id: id.into(),
            code,
            message: message.into(),
            details: vec![],
        })),
    })
}

async fn handle(st: Arc<State>, i: usize, generation: u64, r: Request) {
    let p = wire::json(&r.payload);
    let id = r.id.clone();
    let mode = r.mode;
    // The caller's token first, for every request (only for conformance.Guarded with
    // `token-after-lookup`).
    let refused = if violation("token-after-lookup") {
        None
    } else {
        caller(&st, &r).await.err().map(|why| (5, why))
    };
    // Then the handler the type has here and the request's mode.
    let refused = refused.or_else(|| wrong_mode(&r));
    let msg = if let Some((code, why)) = refused {
        let error = Error {
            request_id: id.clone(),
            code,
            message: why,
            details: vec![],
        };
        Some(if mode == RequestMode::Unary as i32 {
            client_message::Message::Reply(Reply {
                request_id: id.clone(),
                result: Some(reply::Result::Error(error)),
            })
        } else {
            client_message::Message::End(ReplyEnd {
                request_id: id.clone(),
                error: Some(error),
                ..ReplyEnd::default()
            })
        })
    } else if mode == RequestMode::Subscription as i32 {
        let mut subs = st.subs.lock().unwrap();
        if subs.len() >= st.max_subs && !violation("no-subscription-limit") {
            Some(client_message::Message::End(ReplyEnd {
                request_id: id.clone(),
                error: Some(Error {
                    request_id: id.clone(),
                    code: 8,
                    message: "subscription limit reached".into(),
                    details: vec![],
                }),
                ..ReplyEnd::default()
            }))
        } else {
            subs.insert(
                id.clone(),
                Sub {
                    topic: p["topic"].as_str().unwrap_or_default().into(),
                    conn: i,
                    generation,
                    seq: 0,
                },
            );
            None
        }
    } else if mode == RequestMode::Streaming as i32 {
        stream(&st, i, generation, &r, &p).await;
        None
    } else {
        Some(unary(&st, &r, &p).await)
    };
    if let Some(m) = msg {
        send_on(&st, i, generation, m);
    }
    if mode != RequestMode::Subscription as i32 {
        st.in_flight.fetch_sub(1, Ordering::SeqCst);
        if !violation("no-permits") {
            grant(&st, i, generation);
        }
    }
}

/// One finished request's permit: at once when the hub holds min(16, initial / 2) or fewer of
/// ours, else batched at 16 or 50 ms after the first unsent one (each at once with
/// `unbatched-permits`).
fn grant(st: &Arc<State>, i: usize, generation: u64) {
    let conn = &st.conns[i];
    if conn.generation.load(Ordering::SeqCst) != generation {
        return;
    }
    let now = {
        let mut g = conn.grants.lock().unwrap();
        g.pending += 1;
        let held = (g.initial + g.returned).saturating_sub(g.received);
        if violation("unbatched-permits") || held <= 16.min(g.initial / 2) || g.pending >= 16 {
            true
        } else if !g.timer {
            g.timer = true;
            let st = Arc::clone(st);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                flush(&st, i, generation);
            });
            false
        } else {
            false
        }
    };
    if now {
        flush(st, i, generation);
    }
}

fn flush(st: &State, i: usize, generation: u64) {
    let count = {
        let mut g = st.conns[i].grants.lock().unwrap();
        let count = g.pending;
        g.returned += count;
        g.pending = 0;
        g.timer = false;
        count
    };
    if count > 0 {
        send_on(
            st,
            i,
            generation,
            client_message::Message::Permits(Permits { count, grant: 0 }),
        );
    }
}

async fn stream(st: &Arc<State>, i: usize, generation: u64, r: &Request, p: &Value) {
    let credits = Arc::new((Mutex::new(r.initial_credit), Notify::new()));
    let cancelled = Arc::new(AtomicBool::new(false));
    st.running.lock().unwrap().insert(
        r.id.clone(),
        Running {
            credits: Arc::clone(&credits),
            cancelled: Arc::clone(&cancelled),
            conn: i,
            generation,
        },
    );
    let first = if violation("seq-from-zero") { 0 } else { 1 };
    for n in 0..p["count"].as_u64().unwrap_or(0) {
        loop {
            let notified = credits.1.notified();
            if cancelled.load(Ordering::SeqCst) {
                return;
            }
            {
                let mut c = credits.0.lock().unwrap();
                if *c > 0 {
                    *c -= 1;
                    break;
                }
            }
            notified.await;
        }
        send_on(
            st,
            i,
            generation,
            client_message::Message::Part(ReplyPart {
                request_id: r.id.clone(),
                payload: serde_json::to_vec(&json!({ "n": n + 1 })).unwrap(),
                seq: first + n,
                ..ReplyPart::default()
            }),
        );
    }
    st.running.lock().unwrap().remove(&r.id);
    send_on(
        st,
        i,
        generation,
        client_message::Message::End(ReplyEnd {
            request_id: r.id.clone(),
            ..ReplyEnd::default()
        }),
    );
}

async fn unary(st: &Arc<State>, r: &Request, p: &Value) -> client_message::Message {
    let id = r.id.as_str();
    let name = r.r#type.trim_start_matches("conformance.");
    match name {
        "Echo" | "EchoQuery" => reply_ok(
            id,
            &json!({ "payload": p, "headers": r.headers, "instanceId": st.instance, "requestId": id }),
        ),
        "Sleep" | "SleepQuery" => {
            let ms = p["ms"].as_u64().unwrap_or(0);
            tokio::time::sleep(Duration::from_millis(ms)).await;
            reply_ok(id, &json!({ "instanceId": st.instance, "sleptMs": ms }))
        }
        "View" => {
            let hubs: Vec<String> = st
                .conns
                .iter()
                .filter(|c| {
                    c.acked.load(Ordering::SeqCst)
                        || (violation("live-on-echo") && c.echoed.load(Ordering::SeqCst))
                })
                .map(|c| c.hub_id.lock().unwrap().clone().unwrap_or(c.addr.clone()))
                .collect();
            reply_ok(
                id,
                &json!({ "service": st.service, "instanceId": st.instance, "hubIds": hubs,
                         "lastRejection": null, "subscriptionsServed": st.subs.lock().unwrap().len(),
                         "retry": st.retry.is_some() }),
            )
        }
        "Watch" if id.ends_with("#0") => reply_ok(
            id,
            &json!({ "topic": p["topic"], "value": 0, "instanceId": st.instance }),
        ),
        "Emit" => {
            let topic = p["topic"].as_str().unwrap_or_default();
            let mut subs = st.subs.lock().unwrap();
            let ids: Vec<String> = subs
                .iter()
                .filter(|(_, s)| s.topic == topic)
                .map(|(i, _)| i.clone())
                .collect();
            for sid in ids {
                let s = subs.get_mut(&sid).unwrap();
                for v in p["values"].as_array().cloned().unwrap_or_default() {
                    s.seq += 1;
                    send_on(
                        st,
                        s.conn,
                        s.generation,
                        client_message::Message::Part(ReplyPart {
                            request_id: sid.clone(),
                            payload: serde_json::to_vec(&json!({ "topic": topic, "value": v }))
                                .unwrap(),
                            seq: s.seq,
                            ..ReplyPart::default()
                        }),
                    );
                }
                if p["complete"] == true {
                    let s = subs.remove(&sid).unwrap();
                    send_on(
                        st,
                        s.conn,
                        s.generation,
                        client_message::Message::End(ReplyEnd {
                            request_id: sid.clone(),
                            ..ReplyEnd::default()
                        }),
                    );
                }
            }
            reply_ok(id, &json!({}))
        }
        "Guarded" => guarded(st, r, p).await,
        "Relay" => relay(st, r, p).await,
        "Fail" => client_message::Message::Reply(Reply {
            request_id: id.into(),
            result: Some(reply::Result::Error(Error {
                request_id: id.into(),
                code: 4,
                message: p["message"].as_str().unwrap_or("failed").into(),
                details: if p["details"].is_null() {
                    vec![]
                } else {
                    p["details"].to_string().into_bytes()
                },
            })),
        }),
        "Strict" => match p["count"].as_i64() {
            Some(count) => reply_ok(id, &json!({ "count": count })),
            None => reply_err(id, 6, "payload does not decode"),
        },
        "Send" if p["async"] == true => {
            let op = wire::new_id();
            st.ops.lock().unwrap().insert(op.clone(), None);
            let (st2, spec, op2) = (Arc::clone(st), p.clone(), op.clone());
            tokio::spawn(async move {
                let outcome = send(&st2, &spec).await;
                st2.ops.lock().unwrap().insert(op2, Some(outcome));
                st2.op_done.notify_waiters();
            });
            reply_ok(id, &json!({ "opId": op }))
        }
        "Send" => reply_ok(id, &send(st, p).await),
        "Outcome" => {
            let op = p["opId"].as_str().unwrap_or_default().to_string();
            let deadline =
                Instant::now() + Duration::from_millis(p["waitMs"].as_u64().unwrap_or(0));
            loop {
                let notified = st.op_done.notified();
                match st.ops.lock().unwrap().get(&op) {
                    None => break reply_ok(id, &json!({ "unknownOp": op })),
                    Some(Some(v)) => break reply_ok(id, v),
                    Some(None) => {}
                }
                if tokio::time::timeout_at(deadline.into(), notified)
                    .await
                    .is_err()
                {
                    break reply_ok(id, &json!({ "pending": true }));
                }
            }
        }
        _ => reply_err(id, 1, "not served"),
    }
}

/// The caller of `r`: none without a token or without a JWKS, its claims when the token is
/// verified against the JWKS (RS256, the key of its kid, the issuer, the expiry), an error for a
/// header that is not `Bearer <token>`, a token that is not a signed compact JWS, or one that
/// does not verify. With `no-signature-check` the claims are read without checking the
/// signature (the expiry still counts).
async fn caller(st: &State, r: &Request) -> Result<Option<Value>, String> {
    let Some(header) = r
        .headers
        .get("authorization")
        .filter(|h| !h.trim().is_empty())
    else {
        return Ok(None);
    };
    let token = match header.get(..7) {
        Some(scheme) if scheme.eq_ignore_ascii_case("bearer ") => header[7..].trim(),
        _ => return Err("malformed authorization header".into()),
    };
    let segments: Vec<&str> = token.split('.').collect();
    let part = |s: &str| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .filter(Value::is_object)
    };
    if segments.len() != 3 {
        return Err("unreadable token".into());
    }
    let (Some(head), Some(claims)) = (part(segments[0]), part(segments[1])) else {
        return Err("unreadable token".into());
    };
    if head["alg"] == "none" || segments[2].is_empty() {
        return Err("unsigned token".into());
    }
    let Some(jwks_url) = var("CONFORMANCE_JWKS_URL") else {
        return Ok(None);
    };
    if violation("no-signature-check") {
        if claims["exp"].as_i64().unwrap_or(0) < now_ms() / 1000 {
            return Err("token expired".into());
        }
        return Ok(Some(claims));
    }
    let cached = st.jwks.lock().unwrap().clone();
    let jwks = match cached {
        Some(j) => j,
        None => {
            let fetched = http(&jwks_url, None).await.unwrap_or(Value::Null);
            *st.jwks.lock().unwrap() = Some(fetched.clone());
            fetched
        }
    };
    jsonwebtoken::decode_header(token)
        .ok()
        .and_then(|h| {
            let jwk = jwks["keys"]
                .as_array()?
                .iter()
                .find(|k| k["kid"].as_str() == h.kid.as_deref())?;
            let jwk: jsonwebtoken::jwk::Jwk = serde_json::from_value(jwk.clone()).ok()?;
            let key = jsonwebtoken::DecodingKey::from_jwk(&jwk).ok()?;
            let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
            v.set_issuer(&[var("CONFORMANCE_ISSUER").unwrap_or_default()]);
            v.validate_aud = false;
            jsonwebtoken::decode::<Value>(token, &key, &v).ok()
        })
        .map(|t| Some(t.claims))
        .ok_or_else(|| "unauthenticated".into())
}

/// `conformance.Guarded`: an authenticated caller (before decoding), not `conformance-refused`.
async fn guarded(st: &State, r: &Request, p: &Value) -> client_message::Message {
    let id = r.id.as_str();
    let claims = match caller(st, r).await {
        Ok(Some(claims)) => claims,
        Ok(None) => return reply_err(id, 5, "unauthenticated"),
        Err(why) => return reply_err(id, 5, &why),
    };
    if claims["sub"] == "conformance-refused" {
        return reply_err(id, 7, "access denied");
    }
    match p["count"].as_i64() {
        Some(count) => reply_ok(id, &json!({ "count": count, "user": claims["sub"] })),
        None => reply_err(id, 6, "payload does not decode"),
    }
}

/// `conformance.Send`, unary only: rendezvous for a key (signed with `signed-rendezvous`),
/// round-robin otherwise, the retry policy of CONFORMANCE_RETRY_* (retrying anything with
/// `retry-after-timeout`).
async fn send(st: &Arc<State>, spec: &Value) -> Value {
    let started = Instant::now();
    let type_name = match spec["target"].as_str() {
        Some("probe-query") => "conformance.runner.ProbeQuery",
        _ => "conformance.runner.Probe",
    };
    let kind = if type_name.ends_with("Query") {
        RequestKind::Query
    } else {
        RequestKind::Command
    };
    let chosen = spec["timeoutMs"].as_u64();
    // The library's own headers are refused before sending, names compared ignoring case (only
    // exactly as listed with `case-sensitive-headers`).
    const RESERVED: [&str; 5] = [
        "authorization",
        "traceparent",
        "tracestate",
        "correlationId",
        "correlation-id",
    ];
    let headers: HashMap<String, String> = spec["headers"]
        .as_object()
        .map(|h| {
            h.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect()
        })
        .unwrap_or_default();
    let reserved = headers.keys().any(|k| {
        RESERVED.iter().any(|r| {
            if violation("case-sensitive-headers") {
                k == r
            } else {
                k.eq_ignore_ascii_case(r)
            }
        })
    });
    if reserved {
        return json!({ "result": "error", "error": { "kind": "refused" } });
    }
    let (attempts, backoff) = st.retry.unwrap_or((1, 0));
    let budget = Duration::from_millis(chosen.unwrap_or(5000));
    let mut last = json!({ "result": "error", "error": { "kind": "never-sent" } });
    for n in 0..attempts {
        let live: Vec<usize> = (0..st.conns.len())
            .filter(|i| st.conns[*i].acked.load(Ordering::SeqCst))
            .collect();
        let Some(i) = pick(st, &live, spec["routingKey"].as_str()) else {
            return last;
        };
        let mut r = wire::request(&wire::new_id(), kind, type_name, &spec["payload"]);
        r.headers = headers.clone();
        r.routing_key = spec["routingKey"].as_str().unwrap_or_default().into();
        let left = budget.saturating_sub(started.elapsed());
        r.timeout_ms = if st.retry.is_some() {
            left.as_millis().max(1) as u32
        } else {
            chosen.unwrap_or(0) as u32
        };
        let (tx, rx) = oneshot::channel();
        st.pending.lock().unwrap().insert(r.id.clone(), (i, tx));
        let generation = st.conns[i].generation.load(Ordering::SeqCst);
        send_on(st, i, generation, client_message::Message::Request(r));
        let answer = tokio::time::timeout(left + Duration::from_secs(1), rx).await;
        let (code, outcome) = match answer {
            Ok(Ok(ServerMessage {
                message:
                    Some(server_message::Message::Reply(Reply {
                        result: Some(reply::Result::Payload(p)),
                        ..
                    })),
            })) => return json!({ "result": "ok", "payload": wire::json(&p) }),
            Ok(Ok(ServerMessage {
                message:
                    Some(
                        server_message::Message::Error(e)
                        | server_message::Message::Reply(Reply {
                            result: Some(reply::Result::Error(e)),
                            ..
                        }),
                    ),
            })) => (
                e.code,
                json!({ "result": "error", "error": { "kind": "code", "code": wire::code_name(e.code), "message": e.message } }),
            ),
            Ok(_) => (
                0,
                json!({ "result": "error", "error": { "kind": "outcome-unknown" } }),
            ),
            Err(_) => (
                2,
                json!({ "result": "error", "error": { "kind": "code", "code": "TIMEOUT" } }),
            ),
        };
        last = outcome;
        let retryable = violation("retry-after-timeout") || code == 1 || code == 8;
        if !retryable || n + 1 == attempts {
            break;
        }
        tokio::time::sleep(Duration::from_millis(backoff)).await;
    }
    last
}

fn pick(st: &State, live: &[usize], key: Option<&str>) -> Option<usize> {
    if live.is_empty() {
        return None;
    }
    match key.filter(|k| !k.is_empty()) {
        Some(key) => {
            let ids: Vec<String> = live
                .iter()
                .map(|i| {
                    st.conns[*i]
                        .hub_id
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap_or_default()
                })
                .collect();
            let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            let winner = if violation("signed-rendezvous") {
                rendezvous::ranking_signed(&refs, key)[0].to_string()
            } else {
                rendezvous::ranking(&refs, key)[0].to_string()
            };
            live.iter()
                .copied()
                .find(|i| st.conns[*i].hub_id.lock().unwrap().as_deref() == Some(winner.as_str()))
        }
        None if violation("sticky-unkeyed") => Some(live[0]),
        None => Some(live[st.round_robin.fetch_add(1, Ordering::SeqCst) % live.len()]),
    }
}

/// `conformance.Relay` (target probe): the handled request's correlation id (its own id when it
/// has none), authorization and traceparent carried on to a nested conformance.runner.Probe.
async fn relay(st: &Arc<State>, r: &Request, p: &Value) -> client_message::Message {
    let id = r.id.as_str();
    let mut nested = wire::request(
        &wire::new_id(),
        RequestKind::Command,
        "conformance.runner.Probe",
        &p["payload"],
    );
    let correlation = r
        .headers
        .get("correlationId")
        .cloned()
        .unwrap_or_else(|| r.id.clone());
    nested.headers.insert("correlationId".into(), correlation);
    for h in ["authorization", "traceparent"] {
        if let Some(v) = r.headers.get(h) {
            nested.headers.insert(h.into(), v.clone());
        }
    }
    let live: Vec<usize> = (0..st.conns.len())
        .filter(|i| st.conns[*i].acked.load(Ordering::SeqCst))
        .collect();
    let Some(i) = pick(st, &live, None) else {
        return reply_err(id, 4, "no hub");
    };
    let (tx, rx) = oneshot::channel();
    st.pending
        .lock()
        .unwrap()
        .insert(nested.id.clone(), (i, tx));
    let generation = st.conns[i].generation.load(Ordering::SeqCst);
    send_on(st, i, generation, client_message::Message::Request(nested));
    match tokio::time::timeout(Duration::from_secs(10), rx).await {
        Ok(Ok(ServerMessage {
            message:
                Some(server_message::Message::Reply(Reply {
                    result: Some(reply::Result::Payload(p)),
                    ..
                })),
        })) => reply_ok(id, &json!({ "reply": wire::json(&p) })),
        _ => reply_err(id, 4, "the nested call failed"),
    }
}
