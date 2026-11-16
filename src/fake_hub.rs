//! A fake hub: a gRPC server speaking hub.proto that records every frame a library writes and
//! does what a check scripts. It behaves like kraken-hub wherever a check relies on it (the
//! handshake and its refusals, the echo after the ack, permits and queueing, later and empty
//! `Register`s, `NO_HANDLER` for a type nobody serves); the differences that remain are listed in
//! kraken `docs/libraries/conformance.md` ("How the runner's hub differs from kraken-hub"), and
//! `tests/consistency.rs` runs one script of frames against both.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::Stream;
use serde_json::Value;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tonic::{Code, Request as GrpcRequest, Response, Status, Streaming};

use crate::proto::hub_server::{Hub, HubServer};
use crate::proto::{
    Cancel, ClientMessage, Heartbeat, Register, RegisterAck, Reply, Request, RequestKind,
    RequestMode, ServerMessage, TopologyRequest, TopologyResponse, client_message, reply,
    server_message,
};
use crate::wire::{self, WireError};

/// What a new `Connect` gets.
#[derive(Clone, Debug)]
pub enum Mode {
    /// A session; the `RegisterAck` per [`AckMode`].
    Accept,
    /// `UNAVAILABLE` at once, before any frame (a hub process that answers but is not ready).
    Unavailable,
}

/// When a session's `Register` is acknowledged.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AckMode {
    /// At once (kraken-hub).
    Auto,
    /// Never: the session stays unacknowledged.
    Never,
    /// When the check calls [`Session::ack`].
    Manual,
}

/// TLS of a fake hub: its certificate and key (PEM), and the CA its clients' certificates must
/// chain to (mutual TLS) when set.
#[derive(Clone, Debug)]
pub struct TlsSetup {
    pub cert_pem: String,
    pub key_pem: String,
    pub client_ca_pem: Option<String>,
}

type Responder = Arc<dyn Fn(&Arc<Session>, &Request) + Send + Sync>;

type Conn = Result<tokio::net::TcpStream, std::io::Error>;

struct Inner {
    hub_id: Mutex<String>,
    ack: Mutex<RegisterAck>,
    mode: Mutex<Mode>,
    ack_mode: Mutex<AckMode>,
    /// Echo heartbeats before the ack too (kraken-hub never has the chance: it acks at once).
    echo_before_ack: AtomicBool,
    /// Refusals for the next `Register`s, one each, in order (then sessions are accepted).
    refusals: Mutex<VecDeque<(Code, String)>>,
    sessions: Mutex<Vec<Arc<Session>>>,
    /// Every `Connect` call that reached the gRPC service, accepted or not.
    attempts: Mutex<Vec<Instant>>,
    changed: Arc<Notify>,
    responder: Mutex<Option<Responder>>,
}

/// A running fake hub; dropping it closes its sessions and stops it.
pub struct FakeHub {
    inner: Arc<Inner>,
    pub addr: SocketAddr,
    server: JoinHandle<()>,
    accept: Mutex<Option<JoinHandle<()>>>,
    conns: mpsc::Sender<Conn>,
    /// A clone of every accepted socket, to cut them all when the hub goes down.
    sockets: Arc<Mutex<Vec<std::net::TcpStream>>>,
}

/// The `RegisterAck` kraken-hub sends with its defaults (`KRAKEN_HUB_*` unset).
pub fn default_ack(hub_id: &str) -> RegisterAck {
    RegisterAck {
        hub_id: hub_id.to_string(),
        heartbeat_interval_ms: 3000,
        heartbeat_timeout_ms: 10_000,
        default_timeout_ms: 5000,
        max_timeout_ms: 60_000,
        context: "default".into(),
        type_timeouts_ms: HashMap::new(),
    }
}

/// The types the runner serves on a fake hub (the rest get `NO_HANDLER`, as from a hub where
/// nobody registered them).
pub fn served_by_runner(type_name: &str) -> bool {
    type_name.starts_with("conformance.runner.") && type_name != "conformance.runner.Missing"
}

impl FakeHub {
    /// Starts a fake hub on `127.0.0.1` (a free port), plain text.
    pub async fn start(hub_id: &str) -> FakeHub {
        Self::start_with(hub_id, None).await
    }

    /// Starts a fake hub, with TLS (and client certificates) when `tls` is set.
    pub async fn start_with(hub_id: &str, tls: Option<TlsSetup>) -> FakeHub {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let inner = Arc::new(Inner {
            hub_id: Mutex::new(hub_id.to_string()),
            ack: Mutex::new(default_ack(hub_id)),
            mode: Mutex::new(Mode::Accept),
            ack_mode: Mutex::new(AckMode::Auto),
            echo_before_ack: AtomicBool::new(false),
            refusals: Mutex::new(VecDeque::new()),
            sessions: Mutex::new(Vec::new()),
            attempts: Mutex::new(Vec::new()),
            changed: Arc::new(Notify::new()),
            responder: Mutex::new(None),
        });
        let service = HubServer::new(Service(Arc::clone(&inner)));
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = tls {
            // Already installed (by an earlier TLS fake hub) is fine.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let mut config = tonic::transport::ServerTlsConfig::new().identity(
                tonic::transport::Identity::from_pem(tls.cert_pem, tls.key_pem),
            );
            if let Some(ca) = tls.client_ca_pem {
                config = config.client_ca_root(tonic::transport::Certificate::from_pem(ca));
            }
            builder = builder.tls_config(config).expect("fake hub TLS");
        }
        let (conns, rx) = mpsc::channel::<Conn>(64);
        let incoming = tokio_stream::wrappers::ReceiverStream::new(rx);
        let server = tokio::spawn(async move {
            let _ = builder
                .add_service(service)
                .serve_with_incoming(incoming)
                .await;
        });
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let hub = FakeHub {
            inner,
            addr,
            server,
            accept: Mutex::new(None),
            conns,
            sockets,
        };
        hub.accept_on(listener);
        hub
    }

    fn accept_on(&self, listener: tokio::net::TcpListener) {
        let conns = self.conns.clone();
        let sockets = Arc::clone(&self.sockets);
        let task = tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let kept = tcp
                    .into_std()
                    .and_then(|std| Ok((std.try_clone()?, std)))
                    .and_then(|(clone, std)| {
                        std.set_nonblocking(true)?;
                        Ok((clone, tokio::net::TcpStream::from_std(std)?))
                    });
                match kept {
                    Ok((clone, tcp)) => {
                        sockets.lock().unwrap().push(clone);
                        if conns.send(Ok(tcp)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => continue,
                }
            }
        });
        *self.accept.lock().unwrap() = Some(task);
    }

    /// The hub process is gone: every connection cut, the port closed (a connect is refused).
    pub fn go_down(&self) {
        if let Some(t) = self.accept.lock().unwrap().take() {
            t.abort();
        }
        for s in self.sessions() {
            s.mark_closed(false);
        }
        for s in self.sockets.lock().unwrap().drain(..) {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
    }

    /// A new hub process on the same port.
    pub async fn come_up(&self) {
        for _ in 0..50 {
            if let Ok(l) = tokio::net::TcpListener::bind(self.addr).await {
                self.accept_on(l);
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("fake hub: cannot listen on {} again", self.addr);
    }

    /// `host:port` as the library is given it.
    pub fn address(&self) -> String {
        self.addr.to_string()
    }

    pub fn hub_id(&self) -> String {
        self.inner.hub_id.lock().unwrap().clone()
    }

    /// The hub id the next acks carry (a restarted hub has a new one).
    pub fn set_hub_id(&self, id: &str) {
        *self.inner.hub_id.lock().unwrap() = id.to_string();
        self.inner.ack.lock().unwrap().hub_id = id.to_string();
    }

    /// Changes the `RegisterAck` the next sessions get.
    pub fn edit_ack(&self, f: impl FnOnce(&mut RegisterAck)) {
        f(&mut self.inner.ack.lock().unwrap());
    }

    pub fn set_mode(&self, mode: Mode) {
        *self.inner.mode.lock().unwrap() = mode;
    }

    pub fn set_ack_mode(&self, mode: AckMode) {
        *self.inner.ack_mode.lock().unwrap() = mode;
    }

    /// Echo heartbeats also before the ack (L1-HANDSHAKE-ACK: "whatever echoes it got").
    pub fn echo_before_ack(&self, on: bool) {
        self.inner.echo_before_ack.store(on, Ordering::SeqCst);
    }

    /// The next `Register`s are refused with these statuses, one each.
    pub fn refuse_next(&self, refusals: Vec<(Code, String)>) {
        *self.inner.refusals.lock().unwrap() = refusals.into();
    }

    /// Called for every valid `Request` of a runner type (`conformance.runner.*` but Missing) the
    /// library sends to this hub, on the session's reader: it answers through the session,
    /// spawning when it waits. Other types are answered `NO_HANDLER` by the fake hub itself.
    pub fn on_request(&self, f: impl Fn(&Arc<Session>, &Request) + Send + Sync + 'static) {
        *self.inner.responder.lock().unwrap() = Some(Arc::new(f));
    }

    pub fn sessions(&self) -> Vec<Arc<Session>> {
        self.inner.sessions.lock().unwrap().clone()
    }

    /// The times of every `Connect` call that reached the gRPC service.
    pub fn attempts(&self) -> Vec<Instant> {
        self.inner.attempts.lock().unwrap().clone()
    }

    /// Waits until `f` gives a value (it is asked again whenever a frame or a session arrives).
    pub async fn until<T>(&self, timeout: Duration, f: impl FnMut() -> Option<T>) -> Option<T> {
        until(&self.inner.changed, timeout, f).await
    }

    /// The `n`th session (0-based), waiting for it up to `timeout`.
    pub async fn session(&self, n: usize, timeout: Duration) -> Option<Arc<Session>> {
        self.until(timeout, || self.sessions().get(n).cloned())
            .await
    }

    /// The `n`th session once its `Register` arrived.
    pub async fn registered(&self, n: usize, timeout: Duration) -> Option<Arc<Session>> {
        let deadline = Instant::now() + timeout;
        let s = self.session(n, timeout).await?;
        s.register(deadline.saturating_duration_since(Instant::now()))
            .await?;
        Some(s)
    }

    /// The latest session that is still open, if any.
    pub fn live(&self) -> Option<Arc<Session>> {
        self.sessions().into_iter().rev().find(|s| !s.is_closed())
    }

    /// Closes every open session with `status` (a hub going away).
    pub fn close_all(&self, status: Status) {
        for s in self.sessions() {
            s.close(status.clone());
        }
    }
}

impl Drop for FakeHub {
    fn drop(&mut self) {
        self.close_all(Status::unavailable("conformance: fake hub stopped"));
        self.go_down();
        self.server.abort();
    }
}

pub(crate) async fn until<T>(
    changed: &Notify,
    timeout: Duration,
    mut f: impl FnMut() -> Option<T>,
) -> Option<T> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if let Some(v) = f() {
            return Some(v);
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            return f();
        }
    }
}

/// Flow control as kraken-hub applies it to one session: forwards while it holds permits, queues
/// the rest in order, `Permits` raises the cap with its grant and adds its count.
#[derive(Default)]
struct Permits {
    cap: u32,
    available: u32,
    queue: VecDeque<Request>,
}

/// One `Hub.Connect` stream as the fake hub sees it.
pub struct Session {
    pub index: usize,
    pub started: Instant,
    frames: Mutex<Vec<(Instant, ClientMessage)>>,
    out: Mutex<Option<mpsc::UnboundedSender<Result<ServerMessage, Status>>>>,
    closed: AtomicBool,
    closed_at: Mutex<Option<Instant>>,
    /// Whether the library ended the stream (rather than the fake hub).
    closed_by_client: AtomicBool,
    echo: AtomicBool,
    echo_before_ack: bool,
    ack: Mutex<RegisterAck>,
    acked: AtomicBool,
    permits: Mutex<Permits>,
    /// The types of the latest `Register` (an empty one takes the instance out of the routing).
    types: Mutex<Vec<String>>,
    /// Request ids the library cancelled: a late scripted answer for them is dropped.
    cancelled: Mutex<HashSet<String>>,
    /// The kind of every request the fake hub forwarded, by id (no `Cancel` for a command).
    forwarded: Mutex<HashMap<String, i32>>,
    /// The client certificate the library presented (DER of the leaf), with mutual TLS.
    pub peer_cert: Option<Vec<u8>>,
    changed: Arc<Notify>,
}

impl Session {
    /// Every frame the library wrote, with its arrival time.
    pub fn frames(&self) -> Vec<(Instant, ClientMessage)> {
        self.frames.lock().unwrap().clone()
    }

    /// The frames that match `f`.
    pub fn select<T>(&self, mut f: impl FnMut(&ClientMessage) -> Option<T>) -> Vec<(Instant, T)> {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(t, m)| f(m).map(|v| (*t, v)))
            .collect()
    }

    pub async fn until<T>(&self, timeout: Duration, f: impl FnMut() -> Option<T>) -> Option<T> {
        until(&self.changed, timeout, f).await
    }

    /// Waits for the first frame matching `f`.
    pub async fn first<T>(
        &self,
        timeout: Duration,
        mut f: impl FnMut(&ClientMessage) -> Option<T>,
    ) -> Option<T> {
        self.until(timeout, || {
            self.frames.lock().unwrap().iter().find_map(|(_, m)| f(m))
        })
        .await
    }

    /// The session's `Register` (its first one), waiting for it.
    pub async fn register(&self, timeout: Duration) -> Option<Register> {
        self.first(timeout, |m| match &m.message {
            Some(client_message::Message::Register(r)) => Some(r.clone()),
            _ => None,
        })
        .await
    }

    pub fn send(&self, m: server_message::Message) -> bool {
        match &*self.out.lock().unwrap() {
            Some(tx) => tx.send(Ok(wire::server(m))).is_ok(),
            None => false,
        }
    }

    /// Sends the `RegisterAck` (for a session in [`AckMode::Manual`]).
    pub fn ack(&self) {
        if !self.acked.swap(true, Ordering::SeqCst) {
            let ack = self.ack.lock().unwrap().clone();
            self.send(server_message::Message::RegisterAck(ack));
        }
    }

    pub fn acked(&self) -> bool {
        self.acked.load(Ordering::SeqCst)
    }

    /// The `RegisterAck` this session got (or gets).
    pub fn ack_sent(&self) -> RegisterAck {
        self.ack.lock().unwrap().clone()
    }

    /// Stops (or resumes) echoing heartbeats: a silent hub.
    pub fn set_echo(&self, on: bool) {
        self.echo.store(on, Ordering::SeqCst);
    }

    /// Ends the stream with `status`, as a hub closing it.
    pub fn close(&self, status: Status) {
        if let Some(tx) = self.out.lock().unwrap().take() {
            let _ = tx.send(Err(status));
        }
        self.mark_closed(false);
    }

    fn mark_closed(&self, by_client: bool) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            *self.closed_at.lock().unwrap() = Some(Instant::now());
            self.closed_by_client.store(by_client, Ordering::SeqCst);
        }
        self.out.lock().unwrap().take();
        self.changed.notify_waiters();
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Waits until the stream is closed (by either side).
    pub async fn closed(&self, timeout: Duration) -> Option<Instant> {
        self.until(timeout, || self.closed_at()).await
    }

    pub fn closed_at(&self) -> Option<Instant> {
        *self.closed_at.lock().unwrap()
    }

    pub fn closed_by_client(&self) -> bool {
        self.closed_by_client.load(Ordering::SeqCst)
    }

    /// The types the instance's latest `Register` listed on this stream.
    pub fn types(&self) -> Vec<String> {
        self.types.lock().unwrap().clone()
    }

    /// Sends a request to the library as kraken-hub forwards one: a unary or streaming request
    /// takes a permit or waits in the session's queue; a subscription target takes none.
    pub fn request(&self, r: Request) -> bool {
        if self.is_closed() {
            return false;
        }
        self.forwarded.lock().unwrap().insert(r.id.clone(), r.kind);
        if r.mode == RequestMode::Subscription as i32 {
            return self.send(server_message::Message::Request(r));
        }
        let mut p = self.permits.lock().unwrap();
        if p.available > 0 && p.queue.is_empty() {
            p.available -= 1;
            drop(p);
            self.send(server_message::Message::Request(r))
        } else {
            p.queue.push_back(r);
            true
        }
    }

    /// The requests waiting for a permit.
    pub fn queued(&self) -> usize {
        self.permits.lock().unwrap().queue.len()
    }

    fn permits_returned(&self, count: u32, grant: u32) {
        let ready: Vec<Request> = {
            let mut p = self.permits.lock().unwrap();
            p.cap = p.cap.max(grant);
            p.available = (p.available + count).min(p.cap);
            let mut ready = Vec::new();
            while p.available > 0 {
                match p.queue.pop_front() {
                    Some(r) => {
                        p.available -= 1;
                        ready.push(r);
                    }
                    None => break,
                }
            }
            ready
        };
        for r in ready {
            self.send(server_message::Message::Request(r));
        }
    }

    /// Cancels a streaming request, a subscription target or a scatter-gather target the fake
    /// hub forwarded, as kraken-hub does. A command is never cancelled (kraken-hub has no cancel for
    /// one): nothing is sent and the answer is false.
    pub fn cancel(&self, id: &str, reason: &str) -> bool {
        let kind = self.forwarded.lock().unwrap().get(id).copied();
        if kind == Some(RequestKind::Command as i32) {
            return false;
        }
        self.send(server_message::Message::Cancel(Cancel {
            request_id: id.to_string(),
            reason: reason.to_string(),
        }))
    }

    /// The `Reply` to `id`, waiting for it.
    pub async fn reply(&self, id: &str, timeout: Duration) -> Option<Reply> {
        self.first(timeout, |m| match &m.message {
            Some(client_message::Message::Reply(r)) if r.request_id == id => Some(r.clone()),
            _ => None,
        })
        .await
    }

    /// Sends a unary request and reads its reply as JSON (or the error it carries).
    pub async fn call(
        &self,
        kind: RequestKind,
        type_name: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<Value, WireError> {
        self.call_request(
            wire::request(&wire::new_id(), kind, type_name, payload),
            timeout,
        )
        .await
    }

    pub async fn call_request(&self, r: Request, timeout: Duration) -> Result<Value, WireError> {
        let id = r.id.clone();
        let what = r.r#type.clone();
        if !self.request(r) {
            return Err(WireError::no_answer(&format!(
                "{what} (the stream is closed)"
            )));
        }
        match self.reply(&id, timeout).await {
            Some(Reply {
                result: Some(reply::Result::Payload(p)),
                ..
            }) => Ok(wire::json(&p)),
            Some(Reply {
                result: Some(reply::Result::Error(e)),
                ..
            }) => Err(WireError::of(&e)),
            Some(_) => Err(WireError {
                code: "EMPTY_REPLY".into(),
                message: "a Reply with neither payload nor error".into(),
                details: String::new(),
            }),
            None => Err(WireError::no_answer(&what)),
        }
    }

    /// A `conformance.*` command of the service.
    pub async fn command(&self, type_name: &str, payload: Value) -> Result<Value, WireError> {
        self.call(
            RequestKind::Command,
            type_name,
            &payload,
            Duration::from_secs(20),
        )
        .await
    }

    /// A `conformance.*` query of the service.
    pub async fn query(&self, type_name: &str, payload: Value) -> Result<Value, WireError> {
        self.call(
            RequestKind::Query,
            type_name,
            &payload,
            Duration::from_secs(20),
        )
        .await
    }

    fn dropped(&self, id: &str) -> bool {
        self.cancelled.lock().unwrap().contains(id)
    }

    /// Answers a library's request with a payload (dropped after the library's `Cancel`).
    pub fn answer(&self, id: &str, payload: &Value) {
        if self.dropped(id) {
            return;
        }
        self.send(server_message::Message::Reply(Reply {
            request_id: id.to_string(),
            result: Some(reply::Result::Payload(serde_json::to_vec(payload).unwrap())),
        }));
    }

    /// Answers a library's request with an error the hub produced.
    pub fn hub_error(&self, id: &str, code: i32, message: &str) {
        if self.dropped(id) {
            return;
        }
        self.send(server_message::Message::Error(crate::proto::Error {
            request_id: id.to_string(),
            code,
            message: message.to_string(),
            details: Vec::new(),
        }));
    }

    /// Answers a library's request with a handler's error.
    pub fn reply_error(&self, id: &str, code: i32, message: &str, details: &[u8]) {
        if self.dropped(id) {
            return;
        }
        self.send(server_message::Message::Reply(Reply {
            request_id: id.to_string(),
            result: Some(reply::Result::Error(crate::proto::Error {
                request_id: id.to_string(),
                code,
                message: message.to_string(),
                details: details.to_vec(),
            })),
        }));
    }
}

struct Service(Arc<Inner>);

type ConnectStream = Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send>>;

/// The instance-id prefix of kraken-hub's own request sessions (its hub-originated requests).
const HUB_INSTANCE_PREFIX: &str = "kraken-hub/";
/// The service name kraken-hub sends its own requests as.
const HUB_SERVICE: &str = "kraken-hub";

/// kraken-hub's checks of a stream's first `Register` (its src/service.rs), in its order, with the
/// same statuses.
fn first_register_refusal(r: &Register) -> Option<Status> {
    if r.instance_id.is_empty() || r.service_name.is_empty() {
        return Some(Status::invalid_argument(
            "Register.instance_id and Register.service_name must not be empty",
        ));
    }
    if r.instance_id.starts_with(HUB_INSTANCE_PREFIX) {
        return Some(Status::invalid_argument(format!(
            "Register.instance_id must not start with {HUB_INSTANCE_PREFIX}: the hub's own requests use it"
        )));
    }
    if r.service_name == HUB_SERVICE {
        return Some(Status::invalid_argument(format!(
            "Register.service_name must not be {HUB_SERVICE}: the hub's own requests use it"
        )));
    }
    if r.initial_permits == 0 {
        return Some(Status::invalid_argument(
            "Register.initial_permits must be at least 1",
        ));
    }
    None
}

fn register_types(r: &Register) -> Vec<String> {
    r.command_types
        .iter()
        .chain(&r.query_types)
        .chain(&r.shared_query_types)
        .cloned()
        .collect()
}

#[tonic::async_trait]
impl Hub for Service {
    type ConnectStream = ConnectStream;

    async fn connect(
        &self,
        request: GrpcRequest<Streaming<ClientMessage>>,
    ) -> Result<Response<ConnectStream>, Status> {
        let inner = Arc::clone(&self.0);
        inner.attempts.lock().unwrap().push(Instant::now());
        inner.changed.notify_waiters();
        if matches!(*inner.mode.lock().unwrap(), Mode::Unavailable) {
            return Err(Status::unavailable("conformance: hub unavailable"));
        }
        let peer_cert = request
            .peer_certs()
            .and_then(|certs| certs.first().map(|c| c.as_ref().to_vec()));
        let (tx, rx) = mpsc::unbounded_channel();
        let session = {
            let mut sessions = inner.sessions.lock().unwrap();
            let s = Arc::new(Session {
                index: sessions.len(),
                started: Instant::now(),
                frames: Mutex::new(Vec::new()),
                out: Mutex::new(Some(tx)),
                closed: AtomicBool::new(false),
                closed_at: Mutex::new(None),
                closed_by_client: AtomicBool::new(false),
                echo: AtomicBool::new(true),
                echo_before_ack: inner.echo_before_ack.load(Ordering::SeqCst),
                ack: Mutex::new(inner.ack.lock().unwrap().clone()),
                acked: AtomicBool::new(false),
                permits: Mutex::new(Permits::default()),
                types: Mutex::new(Vec::new()),
                cancelled: Mutex::new(HashSet::new()),
                forwarded: Mutex::new(HashMap::new()),
                peer_cert,
                changed: Arc::clone(&inner.changed),
            });
            sessions.push(Arc::clone(&s));
            s
        };
        let ack_mode = *inner.ack_mode.lock().unwrap();
        let mut stream = request.into_inner();
        tokio::spawn(async move {
            let mut registered = false;
            loop {
                let msg = match stream.message().await {
                    Ok(Some(m)) => m,
                    Ok(None) | Err(_) => {
                        session.mark_closed(true);
                        break;
                    }
                };
                if session.is_closed() {
                    break;
                }
                session
                    .frames
                    .lock()
                    .unwrap()
                    .push((Instant::now(), msg.clone()));
                use client_message::Message as M;
                match &msg.message {
                    Some(M::Register(r)) if !registered => {
                        registered = true;
                        let refusal = first_register_refusal(r).or_else(|| {
                            inner
                                .refusals
                                .lock()
                                .unwrap()
                                .pop_front()
                                .map(|(code, message)| Status::new(code, message))
                        });
                        if let Some(status) = refusal {
                            session.close(status);
                        } else {
                            *session.types.lock().unwrap() = register_types(r);
                            {
                                let mut p = session.permits.lock().unwrap();
                                p.cap = r.initial_permits;
                                p.available = r.initial_permits;
                            }
                            if ack_mode == AckMode::Auto {
                                session.ack();
                            }
                        }
                    }
                    _ if !registered => {
                        session.close(Status::failed_precondition(
                            "first message must be Register",
                        ));
                    }
                    Some(M::Register(r)) => {
                        // A later Register replaces the types and is acked, best effort.
                        *session.types.lock().unwrap() = register_types(r);
                        if session.acked() {
                            let ack = session.ack.lock().unwrap().clone();
                            session.send(server_message::Message::RegisterAck(ack));
                        }
                    }
                    Some(M::Heartbeat(h)) => {
                        let may = session.acked() || session.echo_before_ack;
                        if may && session.echo.load(Ordering::SeqCst) {
                            session.send(server_message::Message::Heartbeat(Heartbeat {
                                sent_at_ms: h.sent_at_ms,
                            }));
                        }
                    }
                    Some(M::Permits(p)) => session.permits_returned(p.count, p.grant),
                    Some(M::Cancel(c)) => {
                        session
                            .cancelled
                            .lock()
                            .unwrap()
                            .insert(c.request_id.clone());
                    }
                    Some(M::Request(r)) => {
                        if r.id.is_empty() || r.r#type.is_empty() || r.kind == 0 {
                            session.hub_error(
                                &r.id,
                                crate::proto::ErrorCode::InvalidRequest as i32,
                                "invalid request: id, kind and type are required",
                            );
                        } else if served_by_runner(&r.r#type) {
                            let responder = inner.responder.lock().unwrap().clone();
                            if let Some(f) = responder {
                                f(&session, r);
                            }
                        } else {
                            session.hub_error(
                                &r.id,
                                crate::proto::ErrorCode::NoHandler as i32,
                                &format!("no handler for {}", r.r#type),
                            );
                        }
                    }
                    _ => {}
                }
                inner.changed.notify_waiters();
            }
        });
        let out = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out)))
    }

    async fn topology(
        &self,
        _request: GrpcRequest<TopologyRequest>,
    ) -> Result<Response<TopologyResponse>, Status> {
        Err(Status::unimplemented(
            "conformance: the fake hub has no topology",
        ))
    }
}
