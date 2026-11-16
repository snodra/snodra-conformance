//! Building and reading hub.proto messages.

use std::collections::HashMap;

use serde_json::Value;

use crate::proto::{
    ClientMessage, Error, ErrorCode, Request, RequestKind, RequestMode, ServerMessage,
    client_message, server_message,
};

pub fn client(m: client_message::Message) -> ClientMessage {
    ClientMessage { message: Some(m) }
}

pub fn server(m: server_message::Message) -> ServerMessage {
    ServerMessage { message: Some(m) }
}

/// A fresh request id.
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// A unary request.
pub fn request(id: &str, kind: RequestKind, type_name: &str, payload: &Value) -> Request {
    Request {
        id: id.to_string(),
        kind: kind as i32,
        r#type: type_name.to_string(),
        payload: serde_json::to_vec(payload).unwrap(),
        ..Request::default()
    }
}

/// A request in `mode` (streaming and subscription requests carry `initial_credit`).
pub fn request_mode(
    id: &str,
    type_name: &str,
    payload: &Value,
    mode: RequestMode,
    initial_credit: u32,
) -> Request {
    Request {
        mode: mode as i32,
        initial_credit,
        ..request(id, RequestKind::Query, type_name, payload)
    }
}

pub fn with_headers(mut r: Request, headers: &[(&str, &str)]) -> Request {
    r.headers = headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<_, _>>();
    r
}

/// The name of an `ErrorCode` value (`UNKNOWN(<n>)` for one hub.proto does not define).
pub fn code_name(code: i32) -> String {
    match ErrorCode::try_from(code) {
        Ok(c) => c.as_str_name().to_string(),
        Err(_) => format!("UNKNOWN({code})"),
    }
}

/// An error the runner received: its code, message and details (as text).
#[derive(Clone, Debug, PartialEq)]
pub struct WireError {
    pub code: String,
    pub message: String,
    pub details: String,
}

impl WireError {
    pub fn of(e: &Error) -> WireError {
        WireError {
            code: code_name(e.code),
            message: e.message.clone(),
            details: String::from_utf8_lossy(&e.details).into_owned(),
        }
    }

    /// Nothing came back in time.
    pub fn no_answer(what: &str) -> WireError {
        WireError {
            code: "NO_ANSWER".into(),
            message: format!("no answer to {what} in time"),
            details: String::new(),
        }
    }
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} \"{}\"", self.code, self.message)?;
        if !self.details.is_empty() {
            write!(f, " details {}", self.details)?;
        }
        Ok(())
    }
}

/// A JSON payload, or `{"undecodable": "<text>"}` for bytes that are not JSON.
pub fn json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| serde_json::json!({ "undecodable": String::from_utf8_lossy(bytes) }))
}

/// A short description of a client message, for reports.
pub fn describe(m: &ClientMessage) -> String {
    use client_message::Message as M;
    match &m.message {
        Some(M::Register(r)) => format!(
            "Register{{service {}, instance {}, permits {}}}",
            r.service_name, r.instance_id, r.initial_permits
        ),
        Some(M::Heartbeat(_)) => "Heartbeat".into(),
        Some(M::Request(r)) => format!("Request{{{} {}}}", r.id, r.r#type),
        Some(M::Reply(r)) => format!("Reply{{{}}}", r.request_id),
        Some(M::Part(p)) => format!("ReplyPart{{{}, seq {}}}", p.request_id, p.seq),
        Some(M::End(e)) => format!("ReplyEnd{{{}}}", e.request_id),
        Some(M::Flow(f)) => format!("Flow{{{}, {}}}", f.request_id, f.credits),
        Some(M::Cancel(c)) => format!("Cancel{{{}}}", c.request_id),
        Some(M::Permits(p)) => format!("Permits{{count {}, grant {}}}", p.count, p.grant),
        Some(M::Status(_)) => "InstanceStatus".into(),
        Some(M::Credentials(_)) => "Credentials".into(),
        Some(M::ControlReply(_)) => "ControlReply".into(),
        None => "empty".into(),
    }
}
