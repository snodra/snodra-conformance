//! The fake hub cannot drift from snodra-server unnoticed: one script of frames (the handshake, a
//! heartbeat, unary requests the hub answers itself, the refusals of a first frame (with their
//! status messages; the hub's own instance prefix and service name among them), permits and
//! queueing with a grant raising the cap, a later Register's ack, Cancel for a stream and none for
//! a command, a half-close) runs against a fake hub and against `snodra-server:dev` (Docker), and
//! both must yield the same observable sequence. What the fake hub deliberately does differently
//! is listed in the conformance page of the Snodra docs
//! (<https://snodra.damoreira.com/docs/library-authors/conformance/>).

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use snodra_conformance::fake_hub::FakeHub;
use snodra_conformance::peer::{Peer, Registration};
use snodra_conformance::proto::hub_client::HubClient;
use snodra_conformance::proto::{
    ClientMessage, Heartbeat, Permits, Register, RequestKind, RequestMode, ServerMessage,
    client_message, server_message,
};
use snodra_conformance::wire;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

fn register(service: &str, permits: u32) -> ClientMessage {
    let instance = if service.is_empty() {
        "consistency-1".into()
    } else {
        format!("{service}-{}", &wire::new_id()[..8])
    };
    register_as(service, &instance, permits)
}

fn register_as(service: &str, instance: &str, permits: u32) -> ClientMessage {
    wire::client(client_message::Message::Register(Register {
        service_name: service.into(),
        instance_id: instance.into(),
        query_types: vec!["consistency.Served".into()],
        initial_permits: permits,
        ..Register::default()
    }))
}

fn heartbeat(at: i64) -> ClientMessage {
    wire::client(client_message::Message::Heartbeat(Heartbeat {
        sent_at_ms: at,
    }))
}

/// What a client sees for `first` frames on a fresh stream: the server messages (described)
/// until a quiet 1.5 s, or the closing status.
async fn session(addr: &str, frames: Vec<ClientMessage>, after: Vec<ClientMessage>) -> Vec<String> {
    // A container's mapped port may refuse for a moment after the hub logged it listens.
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).unwrap();
    let mut tries = 0;
    let channel = loop {
        match endpoint.connect().await {
            Ok(c) => break c,
            Err(e) if tries < 50 => {
                tries += 1;
                let _ = e;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("cannot reach {addr}: {e}"),
        }
    };
    let (tx, rx) = mpsc::channel(16);
    for f in frames {
        tx.send(f).await.unwrap();
    }
    let mut seen = Vec::new();
    let mut inbound = match HubClient::new(channel)
        .connect(ReceiverStream::new(rx))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(status) => return vec![format!("status {:?}: {}", status.code(), status.message())],
    };
    let mut after = after.into_iter();
    loop {
        match tokio::time::timeout(Duration::from_millis(1500), inbound.message()).await {
            Ok(Ok(Some(m))) => seen.push(describe(&m)),
            Ok(Ok(None)) => {
                seen.push("end".into());
                break;
            }
            Ok(Err(status)) => {
                seen.push(format!("status {:?}: {}", status.code(), status.message()));
                break;
            }
            Err(_) => match after.next() {
                Some(f) => tx.send(f).await.unwrap(),
                None => break,
            },
        }
    }
    seen
}

fn describe(m: &ServerMessage) -> String {
    use server_message::Message as S;
    match &m.message {
        Some(S::RegisterAck(a)) => format!(
            "RegisterAck(hub id set {}, interval {}, timeout {}, default {}, max {})",
            !a.hub_id.is_empty(),
            a.heartbeat_interval_ms,
            a.heartbeat_timeout_ms,
            a.default_timeout_ms,
            a.max_timeout_ms
        ),
        Some(S::Heartbeat(h)) => format!("Heartbeat({})", h.sent_at_ms),
        Some(S::Error(e)) => format!("Error({}, {})", e.request_id, wire::code_name(e.code)),
        other => format!("{other:?}"),
    }
}

async fn script(addr: &str) -> Vec<Vec<String>> {
    let unknown = wire::request("c-1", RequestKind::Query, "consistency.Unknown", &json!({}));
    let mut no_kind = wire::request("c-2", RequestKind::Query, "consistency.Served", &json!({}));
    no_kind.kind = 0;
    vec![
        // The handshake, a heartbeat, a request nobody serves, one without a kind.
        session(
            addr,
            vec![register("consistency", 4), heartbeat(42)],
            vec![
                heartbeat(7),
                wire::client(client_message::Message::Request(unknown)),
                wire::client(client_message::Message::Request(no_kind)),
            ],
        )
        .await,
        // Refusals of the first frame.
        session(addr, vec![heartbeat(1)], vec![]).await,
        session(addr, vec![register("", 4)], vec![]).await,
        session(addr, vec![register("consistency", 0)], vec![]).await,
        // The hub's own identity (its hub-originated requests): reserved instance prefix, service.
        session(
            addr,
            vec![register_as("consistency", "snodra-server/consistency-1", 4)],
            vec![],
        )
        .await,
        session(
            addr,
            vec![register_as("snodra-server", "consistency-2", 4)],
            vec![],
        )
        .await,
    ]
}

/// A raw instance on a hub: it records what it receives (described), answers the unary queries
/// of `consistency.Served` with `{}` and leaves everything else unanswered.
struct Handler {
    tx: Option<mpsc::Sender<ClientMessage>>,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Handler {
    async fn connect(addr: &str, permits: u32) -> Handler {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let (tx, rx) = mpsc::channel(64);
        tx.send(handler_register(permits, false)).await.unwrap();
        tx.send(heartbeat(1)).await.unwrap();
        let mut inbound = HubClient::new(channel)
            .connect(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let seen: Arc<Mutex<Vec<String>>> = Arc::default();
        // A weak sender: the handler's half-close (dropping `tx`) must end the outbound stream.
        let (record, reply) = (Arc::clone(&seen), tx.downgrade());
        tokio::spawn(async move {
            loop {
                let line = match inbound.message().await {
                    Ok(Some(m)) => {
                        if let Some(server_message::Message::Request(r)) = &m.message
                            && r.mode == 0
                            && r.kind == RequestKind::Query as i32
                        {
                            let Some(reply) = reply.upgrade() else {
                                continue;
                            };
                            let _ = reply
                                .send(wire::client(client_message::Message::Reply(
                                    snodra_conformance::proto::Reply {
                                        request_id: r.id.clone(),
                                        result: Some(
                                            snodra_conformance::proto::reply::Result::Payload(
                                                b"{}".to_vec(),
                                            ),
                                        ),
                                    },
                                )))
                                .await;
                        }
                        handler_describe(&m)
                    }
                    Ok(None) => {
                        record.lock().unwrap().push("end".into());
                        break;
                    }
                    Err(status) => {
                        record
                            .lock()
                            .unwrap()
                            .push(format!("status {:?}", status.code()));
                        break;
                    }
                };
                if let Some(line) = line {
                    record.lock().unwrap().push(line);
                }
            }
        });
        let h = Handler { tx: Some(tx), seen };
        h.settle().await;
        h
    }

    async fn send(&self, m: ClientMessage) {
        if self.tx.as_ref().unwrap().send(m).await.is_err() {
            self.seen
                .lock()
                .unwrap()
                .push("send failed: stream closed".into());
        }
    }

    async fn settle(&self) {
        tokio::time::sleep(Duration::from_millis(800)).await;
    }

    fn requests(&self) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.starts_with("Request"))
            .count()
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

const SERVED: &str = "consistency.flow.Served";
const COMMAND: &str = "consistency.flow.Command";

fn handler_register(permits: u32, later: bool) -> ClientMessage {
    let mut queries = vec![SERVED.to_string()];
    if later {
        queries.push("consistency.flow.Added".into());
    }
    wire::client(client_message::Message::Register(Register {
        service_name: "consistency-handler".into(),
        instance_id: "consistency-handler-1".into(),
        query_types: queries,
        command_types: vec![COMMAND.into()],
        initial_permits: permits,
        ..Register::default()
    }))
}

/// What the handler sees, without ids (only the `#<n>` of a derived one) and heartbeats.
fn handler_describe(m: &ServerMessage) -> Option<String> {
    use server_message::Message as S;
    let suffix = |id: &str| {
        id.rsplit_once('#')
            .map(|(_, n)| format!("#{n}"))
            .unwrap_or_default()
    };
    match &m.message {
        Some(S::RegisterAck(_)) => Some("RegisterAck".into()),
        Some(S::Heartbeat(_)) => None,
        Some(S::Request(r)) => Some(format!(
            "Request({}, mode {}, {}{})",
            r.r#type,
            r.mode,
            if r.kind == RequestKind::Command as i32 {
                "command"
            } else {
                "query"
            },
            suffix(&r.id)
        )),
        Some(S::Cancel(c)) => Some(format!("Cancel({})", suffix(&c.request_id))),
        other => Some(format!("{other:?}")),
    }
}

/// Who sends to the handler: snodra-server's sender instance, or the fake hub itself.
enum Driver {
    Fake(Arc<FakeHub>),
    Real(Peer),
}

impl Driver {
    async fn request(&self, id: &str, kind: RequestKind, type_name: &str, mode: RequestMode) {
        let mut r = wire::request(id, kind, type_name, &json!({}));
        r.mode = mode as i32;
        r.initial_credit = 1;
        r.timeout_ms = 30_000;
        match self {
            Driver::Fake(f) => {
                // snodra-server forwards a stream under `<id>#1`.
                if mode == RequestMode::Streaming {
                    r.id = format!("{id}#1");
                }
                f.live().unwrap().request(r);
            }
            Driver::Real(p) => p.send(client_message::Message::Request(r)),
        }
    }

    async fn cancel(&self, id: &str, streaming: bool) {
        match self {
            Driver::Fake(f) => {
                let id = if streaming {
                    format!("{id}#1")
                } else {
                    id.to_string()
                };
                f.live().unwrap().cancel(&id, "consistency");
            }
            Driver::Real(p) => p.send(client_message::Message::Cancel(
                snodra_conformance::proto::Cancel {
                    request_id: id.into(),
                    reason: "consistency".into(),
                },
            )),
        }
    }
}

/// Permits and queueing, a grant raising the cap, a later Register, Cancel, a half-close.
async fn flow(addr: &str, driver: &Driver) -> Vec<String> {
    let mut out = Vec::new();
    let h = Handler::connect(addr, 1).await;
    out.push(format!("acked {:?}", h.seen()));
    driver
        .request("p-1", RequestKind::Query, SERVED, RequestMode::Unary)
        .await;
    driver
        .request("p-2", RequestKind::Query, SERVED, RequestMode::Unary)
        .await;
    h.settle().await;
    out.push(format!(
        "one permit, two requests: {} forwarded",
        h.requests()
    ));
    h.send(wire::client(client_message::Message::Permits(Permits {
        count: 1,
        grant: 0,
    })))
    .await;
    h.settle().await;
    out.push(format!("Permits{{1}}: {} forwarded", h.requests()));
    driver
        .request("p-3", RequestKind::Query, SERVED, RequestMode::Unary)
        .await;
    driver
        .request("p-4", RequestKind::Query, SERVED, RequestMode::Unary)
        .await;
    h.settle().await;
    out.push(format!("two more, no permit: {} forwarded", h.requests()));
    h.send(wire::client(client_message::Message::Permits(Permits {
        count: 2,
        grant: 2,
    })))
    .await;
    h.settle().await;
    out.push(format!("Permits{{2, grant 2}}: {} forwarded", h.requests()));
    h.send(handler_register(1, true)).await;
    h.settle().await;
    let acks = h.seen().iter().filter(|l| *l == "RegisterAck").count();
    out.push(format!("a later Register: {acks} acks"));
    h.send(wire::client(client_message::Message::Permits(Permits {
        count: 2,
        grant: 0,
    })))
    .await;
    driver
        .request("s", RequestKind::Query, SERVED, RequestMode::Streaming)
        .await;
    driver
        .request("c", RequestKind::Command, COMMAND, RequestMode::Unary)
        .await;
    h.settle().await;
    driver.cancel("s", true).await;
    driver.cancel("c", false).await;
    h.settle().await;
    let tail: Vec<String> = h
        .seen()
        .into_iter()
        .skip_while(|l| !l.contains("mode 2"))
        .collect();
    out.push(format!(
        "a stream and a command, both cancelled by their sender: {tail:?}"
    ));
    let mut h = h;
    h.tx = None;
    h.settle().await;
    out.push(format!("half-close: {:?}", h.seen().last()));
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn the_fake_hub_answers_the_script_as_snodra_server_does() {
    let fake = FakeHub::start("fake-hub-consistency").await;
    let from_fake = script(&fake.address()).await;
    let fake = Arc::new(FakeHub::start("fake-hub-consistency-2").await);
    let fake_flow = flow(&fake.address(), &Driver::Fake(Arc::clone(&fake))).await;
    let (_hub, addr) = support::hub_container().await;
    let from_hub = script(&addr).await;
    let driver = Peer::connect(
        &addr,
        Registration {
            service: "consistency-driver".into(),
            ..Registration::default()
        },
        |_, _| {},
    )
    .await
    .unwrap();
    let hub_flow = flow(&addr, &Driver::Real(driver)).await;
    println!("fake flow: {fake_flow:#?}\nhub flow:  {hub_flow:#?}");
    assert_eq!(fake_flow, hub_flow);
    println!("fake: {from_fake:#?}\nhub:  {from_hub:#?}");
    assert_eq!(from_fake, from_hub);
}
