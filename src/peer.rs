//! A raw client of a real hub: the runner's own instance, registered as `conformance-runner`, that
//! sends requests to the library under test and serves the `conformance.runner.*` types the
//! library sends to. It speaks hub.proto frame by frame, so it sees what the hub forwards
//! (the library's `Request.id`, `Request.timeout_ms`, headers, routing key) unchanged.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;

use crate::proto::hub_client::HubClient;
use crate::proto::{
    Heartbeat, Register, Reply, ReplyEnd, ReplyPart, Request, RequestKind, ServerMessage,
    TopologyRequest, TopologyResponse, client_message, reply, server_message,
};
use crate::wire::{self, WireError};

type Handler = Arc<dyn Fn(&Peer, &Request) + Send + Sync>;

/// Everything that came back for one request this peer sent.
#[derive(Clone, Debug, Default)]
pub struct Answer {
    pub reply: Option<Reply>,
    pub error: Option<crate::proto::Error>,
    pub parts: Vec<ReplyPart>,
    pub end: Option<ReplyEnd>,
}

impl Answer {
    /// The unary answer as JSON, or the error it carries.
    pub fn json(&self) -> Result<Value, WireError> {
        if let Some(e) = &self.error {
            return Err(WireError::of(e));
        }
        match &self.reply {
            Some(Reply {
                result: Some(reply::Result::Payload(p)),
                ..
            }) => Ok(wire::json(p)),
            Some(Reply {
                result: Some(reply::Result::Error(e)),
                ..
            }) => Err(WireError::of(e)),
            _ => Err(WireError::no_answer("the request")),
        }
    }
}

/// Ends the peer's stream when the last handle the check holds is dropped.
struct Closer(tokio::sync::watch::Sender<bool>);

impl Drop for Closer {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

#[derive(Clone)]
pub struct Peer {
    /// Held by the check's handles only (not by the peer's own tasks).
    _closer: Option<Arc<Closer>>,
    tx: mpsc::Sender<crate::proto::ClientMessage>,
    waiting: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<ServerMessage>>>>,
    /// Every request the hub forwarded to this peer.
    seen: Arc<Mutex<Vec<Request>>>,
    pub instance: String,
    pub hub_id: String,
}

/// What a peer registers.
#[derive(Clone, Debug, Default)]
pub struct Registration {
    pub service: String,
    pub commands: Vec<String>,
    pub queries: Vec<String>,
    pub shared: Vec<String>,
}

pub fn endpoint(address: &str) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

impl Peer {
    /// Connects to the hub at `address`, registers and waits for the `RegisterAck`.
    pub async fn connect(
        address: &str,
        registration: Registration,
        handler: impl Fn(&Peer, &Request) + Send + Sync + 'static,
    ) -> Result<Peer, String> {
        let channel = Endpoint::from_shared(endpoint(address))
            .map_err(|e| e.to_string())?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| format!("cannot reach the hub at {address}: {e}"))?;
        let mut client = HubClient::new(channel);
        let (tx, rx) = mpsc::channel(1024);
        let instance = format!("{}-{}", registration.service, &wire::new_id()[..8]);
        tx.send(wire::client(client_message::Message::Register(Register {
            service_name: registration.service.clone(),
            instance_id: instance.clone(),
            command_types: registration.commands.clone(),
            query_types: registration.queries.clone(),
            shared_query_types: registration.shared.clone(),
            initial_permits: 4096,
            client_version: format!("kraken-conformance/{}", crate::RUNNER_VERSION),
            ..Register::default()
        })))
        .await
        .unwrap();
        tx.send(heartbeat()).await.unwrap();
        let (close_tx, mut close_rx) = tokio::sync::watch::channel(false);
        let outbound = futures::StreamExt::take_until(ReceiverStream::new(rx), async move {
            let _ = close_rx.wait_for(|closed| *closed).await;
        });
        let mut inbound = client
            .connect(outbound)
            .await
            .map_err(|e| format!("the hub refused the runner's stream: {e}"))?
            .into_inner();
        let hub_id = loop {
            match tokio::time::timeout(Duration::from_secs(10), inbound.message()).await {
                Ok(Ok(Some(ServerMessage {
                    message: Some(server_message::Message::RegisterAck(ack)),
                }))) => break ack.hub_id,
                Ok(Ok(Some(_))) => continue,
                Ok(Ok(None)) => return Err("the hub closed the runner's stream".into()),
                Ok(Err(e)) => return Err(format!("the hub refused the runner's Register: {e}")),
                Err(_) => return Err("no RegisterAck for the runner in 10 s".into()),
            }
        };
        let peer = Peer {
            _closer: Some(Arc::new(Closer(close_tx))),
            tx: tx.clone(),
            waiting: Arc::new(Mutex::new(HashMap::new())),
            seen: Arc::new(Mutex::new(Vec::new())),
            instance,
            hub_id,
        };
        let beat = tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if beat.send(heartbeat()).await.is_err() {
                    break;
                }
            }
        });
        let handler: Handler = Arc::new(handler);
        let me = Peer {
            _closer: None,
            ..peer.clone()
        };
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                use server_message::Message as M;
                let id = match &msg.message {
                    Some(M::Request(r)) => {
                        me.seen.lock().unwrap().push(r.clone());
                        handler(&me, r);
                        continue;
                    }
                    Some(M::Reply(r)) => r.request_id.clone(),
                    Some(M::Error(e)) => e.request_id.clone(),
                    Some(M::Part(p)) => p.request_id.clone(),
                    Some(M::End(e)) => e.request_id.clone(),
                    _ => continue,
                };
                if let Some(w) = me.waiting.lock().unwrap().get(&id) {
                    let _ = w.send(msg);
                }
            }
        });
        Ok(peer)
    }

    /// Sends `request` and collects its answers until the terminal one (a reply or an error for
    /// a unary request; an end or an error otherwise), or `timeout`.
    pub async fn request(&self, request: Request, timeout: Duration) -> Answer {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let id = request.id.clone();
        let unary = request.mode == 0;
        self.waiting.lock().unwrap().insert(id.clone(), tx);
        let _ = self
            .tx
            .send(wire::client(client_message::Message::Request(request)))
            .await;
        let mut answer = Answer::default();
        let deadline = tokio::time::Instant::now() + timeout;
        while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            use server_message::Message as M;
            match msg.message {
                Some(M::Reply(r)) => {
                    answer.reply = Some(r);
                    if unary {
                        break;
                    }
                }
                Some(M::Error(e)) => {
                    answer.error = Some(e);
                    break;
                }
                Some(M::Part(p)) => answer.parts.push(p),
                Some(M::End(e)) => {
                    answer.end = Some(e);
                    break;
                }
                _ => {}
            }
        }
        self.waiting.lock().unwrap().remove(&id);
        answer
    }

    /// A unary request, its answer as JSON.
    pub async fn call(
        &self,
        kind: RequestKind,
        type_name: &str,
        payload: &Value,
        timeout: Duration,
    ) -> Result<Value, WireError> {
        self.request(
            wire::request(&wire::new_id(), kind, type_name, payload),
            timeout,
        )
        .await
        .json()
    }

    /// A `conformance.*` command of the service under test.
    pub async fn command(&self, type_name: &str, payload: Value) -> Result<Value, WireError> {
        self.call(
            RequestKind::Command,
            type_name,
            &payload,
            Duration::from_secs(20),
        )
        .await
    }

    /// A `conformance.*` query of the service under test.
    pub async fn query(&self, type_name: &str, payload: Value) -> Result<Value, WireError> {
        self.call(
            RequestKind::Query,
            type_name,
            &payload,
            Duration::from_secs(20),
        )
        .await
    }

    /// The requests the hub forwarded to this peer so far.
    pub fn seen(&self) -> Vec<Request> {
        self.seen.lock().unwrap().clone()
    }

    /// Answers a forwarded request with a payload.
    pub fn answer(&self, id: &str, payload: &Value) {
        self.send(client_message::Message::Reply(Reply {
            request_id: id.to_string(),
            result: Some(reply::Result::Payload(serde_json::to_vec(payload).unwrap())),
        }));
    }

    /// Answers a forwarded request with a handler error.
    pub fn answer_error(&self, id: &str, code: i32, message: &str) {
        self.send(client_message::Message::Reply(Reply {
            request_id: id.to_string(),
            result: Some(reply::Result::Error(crate::proto::Error {
                request_id: id.to_string(),
                code,
                message: message.to_string(),
                details: Vec::new(),
            })),
        }));
    }

    pub fn send(&self, m: client_message::Message) {
        let _ = self.tx.try_send(wire::client(m));
    }
}

fn heartbeat() -> crate::proto::ClientMessage {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    wire::client(client_message::Message::Heartbeat(Heartbeat {
        sent_at_ms: now,
    }))
}

/// The hub's topology snapshot.
pub async fn topology(address: &str) -> Result<TopologyResponse, String> {
    let channel = Endpoint::from_shared(endpoint(address))
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    HubClient::new(channel)
        .topology(TopologyRequest {})
        .await
        .map(|r| r.into_inner())
        .map_err(|e| e.to_string())
}
