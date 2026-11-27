//! The hub's dashboard API, as the dashboard itself calls it: the processor status it aggregates
//! from every instance, the pause/resume fan-out, the leased reset and the dead-letter calls.
//!
//! The driver asserts the hub's answer, not an instance's: the counts, the instance ids, the
//! report of the reset and the letter a retry named. A call that the hub refused answers with its
//! status and body, so a scenario can prove a refusal (an event store without the replay
//! capability, a dead-letter call outside the collection mode).

use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::Res;

/// The write credential the harness gives the hub (`SNODRA_SERVER_DASHBOARD_WRITE_TOKEN`).
const WRITE_TOKEN_ENV: &str = "INTEROP_HUB_WRITE_TOKEN";
/// The token the compose file uses when the harness sets none.
const DEFAULT_WRITE_TOKEN: &str = "snodra-interop-write-token";

/// The dashboard API of one hub.
pub struct Api {
    base: String,
    token: String,
    client: reqwest::Client,
}

/// One processor of one service as `GET /api/v1/processors` aggregates it.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ProcessorAggregate {
    pub name: String,
    pub subscription: String,
    pub source: String,
    pub event_store: String,
    pub capabilities: Vec<String>,
    pub dead_letter_mode: String,
    pub state: String,
    pub instances_reporting: u32,
    pub assigned_segments: u32,
    pub lag: Option<i64>,
    pub dead_letters: u64,
    pub processed_total: u64,
    pub failed_total: u64,
    pub event_types: Vec<String>,
    pub replaying: bool,
    pub operation_id: String,
    pub instances: Vec<ProcessorInstance>,
}

impl ProcessorAggregate {
    /// The report of `instance`, when it runs the processor.
    pub fn instance(&self, instance: &str) -> Option<&ProcessorInstance> {
        self.instances.iter().find(|i| i.instance_id == instance)
    }

    /// Whether the event store declared `capability`.
    pub fn can(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }
}

/// One instance's report of a processor and the age of that report.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ProcessorInstance {
    pub instance_id: String,
    pub status_age_ms: i64,
    #[serde(flatten)]
    pub report: ProcessorReport,
}

/// The fields of one instance's `ProcessorStatus` the driver reads.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ProcessorReport {
    pub name: String,
    pub subscription: String,
    pub source: String,
    pub event_store: String,
    pub capabilities: Vec<String>,
    pub state: String,
    pub segments: Vec<SegmentReport>,
    pub dead_letters: u64,
    pub dead_letter_mode: String,
    pub replaying: bool,
    pub operation_id: String,
}

/// One assigned segment: positions are opaque strings only the add-on reads.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct SegmentReport {
    pub segment: String,
    pub position: String,
    pub committed: String,
    pub head: String,
    pub lag: i64,
    /// The end (exclusive) of the segment's open replay range; empty when none.
    pub replay_until: String,
}

/// The answer of `GET /api/v1/processors`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ProcessorsView {
    pub now_ms: i64,
    pub services: Vec<ServiceProcessors>,
}

/// One service's processors in the view.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ServiceProcessors {
    pub service_name: String,
    pub dead_letters: u64,
    pub processors: Vec<ProcessorAggregate>,
}

/// The answer of a pause or resume: the fan-out over every instance that reports the processor.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct PauseReport {
    pub service: String,
    pub processor: String,
    pub action: String,
    pub acknowledged: u32,
    pub failed: u32,
    pub instances: Vec<PauseInstance>,
}

/// One instance's answer to a pause or resume.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct PauseInstance {
    pub instance_id: String,
    pub result: String,
    pub code: Option<String>,
    pub message: Option<String>,
}

/// The answer of a reset: the leased operation's report. On a refusal it is the error body with
/// the refusal code and the operation's outcome (`aborted` or `failed`).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ResetReport {
    pub service: String,
    pub processor: String,
    pub action: String,
    pub operation_id: String,
    pub target: Value,
    pub outcome: String,
    pub executed_by: Option<String>,
    pub replay: bool,
    pub ranges: Vec<ResetRange>,
    pub markers_deleted: u64,
    pub reset_handlers: u32,
    pub error: Option<Value>,
    pub instances: Vec<ResetInstance>,
    /// The refusal body's `result` (`refused`), when the hub refused before asking an instance.
    pub result: Option<String>,
    pub code: Option<String>,
    pub message: Option<String>,
}

/// One range of a reset: the records of `segment` at `from <= position < until` replay.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ResetRange {
    pub segment: String,
    pub from: String,
    pub until: String,
}

/// One instance in a reset: its prepare and, when it was involved, its finish answer.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ResetInstance {
    pub instance_id: String,
    pub prepare: AnswerView,
    pub finish: Option<AnswerView>,
}

/// An instance's answer wherever the hub shows one.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct AnswerView {
    pub result: String,
    pub code: Option<String>,
    pub message: Option<String>,
}

/// The answer of a one-letter retry.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct RetryReport {
    pub service: String,
    pub processor: String,
    pub instance_id: String,
    pub letter_id: String,
    pub result: String,
    pub detail: String,
}

/// The answer of `POST dead-letters/retry-all`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct RetryAllReport {
    pub service: String,
    pub processor: String,
    pub instance_id: String,
    pub retried: u32,
    pub failed: u32,
    pub blocked: u32,
    pub skipped: u32,
}

/// The answer of `DELETE dead-letters/{id}`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct DiscardReport {
    pub service: String,
    pub processor: String,
    pub instance_id: String,
    pub letter_id: String,
    pub discarded: bool,
}

impl Api {
    /// The API of the hub at `host:port`, with the harness's write credential.
    pub fn new(hub: &str) -> Self {
        let token =
            std::env::var(WRITE_TOKEN_ENV).unwrap_or_else(|_| DEFAULT_WRITE_TOKEN.to_string());
        Self {
            base: format!("http://{hub}"),
            token,
            client: reqwest::Client::new(),
        }
    }

    /// `GET /api/v1/processors`.
    pub async fn processors(&self) -> Res<ProcessorsView> {
        let (status, body) = self
            .call(reqwest::Method::GET, "/api/v1/processors", None)
            .await?;
        if status != 200 {
            return Err(format!("GET /api/v1/processors: {status} {body}"));
        }
        serde_json::from_value(body).map_err(|e| format!("the processors view: {e}"))
    }

    /// One processor of the probe service as the hub aggregates it.
    pub async fn processor(&self, service: &str, processor: &str) -> Res<ProcessorAggregate> {
        let view = self.processors().await?;
        view.services
            .iter()
            .find(|s| s.service_name == service)
            .and_then(|s| s.processors.iter().find(|p| p.name == processor))
            .cloned()
            .ok_or_else(|| format!("the hub reports no processor {processor} of service {service}"))
    }

    /// `POST .../pause`.
    pub async fn pause(&self, processor: &str) -> Res<(u16, PauseReport, Value)> {
        self.action(processor, "pause", None).await
    }

    /// `POST .../resume`.
    pub async fn resume(&self, processor: &str) -> Res<(u16, PauseReport, Value)> {
        self.action(processor, "resume", None).await
    }

    /// The common shape of pause and resume: the status, the report and the raw body.
    async fn action(
        &self,
        processor: &str,
        action: &str,
        body: Option<Value>,
    ) -> Res<(u16, PauseReport, Value)> {
        let path = format!("/api/v1/services/{SERVICE}/processors/{processor}/{action}");
        let (status, body) = self.call(reqwest::Method::POST, &path, body).await?;
        let report = serde_json::from_value(body.clone())
            .map_err(|e| format!("POST {path}: {status} {e}: {body}"))?;
        Ok((status, report, body))
    }

    /// `POST .../reset` to `target` (`{"to":"earliest"}`, ...).
    pub async fn reset(&self, processor: &str, target: Value) -> Res<(u16, ResetReport, Value)> {
        let path = format!("/api/v1/services/{SERVICE}/processors/{processor}/reset");
        let (status, body) = self
            .call(reqwest::Method::POST, &path, Some(json!({"to": target})))
            .await?;
        let report = serde_json::from_value(body.clone())
            .map_err(|e| format!("POST {path}: {status} {e}: {body}"))?;
        Ok((status, report, body))
    }

    /// `GET .../dead-letters?limit=n`.
    pub async fn letters(&self, processor: &str, limit: u32) -> Res<(u16, Value)> {
        let path =
            format!("/api/v1/services/{SERVICE}/processors/{processor}/dead-letters?limit={limit}");
        self.call(reqwest::Method::GET, &path, None).await
    }

    /// `POST .../dead-letters/{letter}/retry`.
    pub async fn retry(&self, processor: &str, letter: &str) -> Res<(u16, RetryReport, Value)> {
        let path = format!(
            "/api/v1/services/{SERVICE}/processors/{processor}/dead-letters/{}/retry",
            segment(letter)
        );
        let (status, body) = self.call(reqwest::Method::POST, &path, None).await?;
        let report = serde_json::from_value(body.clone())
            .map_err(|e| format!("POST {path}: {status} {e}: {body}"))?;
        Ok((status, report, body))
    }

    /// `POST .../dead-letters/retry-all`.
    pub async fn retry_all(&self, processor: &str) -> Res<(u16, RetryAllReport, Value)> {
        let path =
            format!("/api/v1/services/{SERVICE}/processors/{processor}/dead-letters/retry-all");
        let (status, body) = self.call(reqwest::Method::POST, &path, None).await?;
        let report = serde_json::from_value(body.clone())
            .map_err(|e| format!("POST {path}: {status} {e}: {body}"))?;
        Ok((status, report, body))
    }

    /// `DELETE .../dead-letters/{letter}`.
    pub async fn discard(&self, processor: &str, letter: &str) -> Res<(u16, DiscardReport, Value)> {
        let path = format!(
            "/api/v1/services/{SERVICE}/processors/{processor}/dead-letters/{}",
            segment(letter)
        );
        let (status, body) = self.call(reqwest::Method::DELETE, &path, None).await?;
        let report = serde_json::from_value(body.clone())
            .map_err(|e| format!("DELETE {path}: {status} {e}: {body}"))?;
        Ok((status, report, body))
    }

    /// One call: the write credential, the optional JSON body, and the status and body whatever
    /// the status (a refusal is data for a scenario).
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Res<(u16, Value)> {
        let mut request = self
            .client
            .request(method.clone(), format!("{}{path}", self.base))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            );
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("{method} {path}: {e}"))?;
        let status = response.status().as_u16();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| format!("{method} {path}: reading the body: {e}"))?;
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()))
        };
        Ok((status, body))
    }

    /// Waits until `instance`'s report is the freshest of the processor's by at least `margin_ms`:
    /// the hub picks that instance for a reset execute and a dead-letter call.
    pub async fn wait_freshest(
        &self,
        processor: &str,
        instance: &str,
        margin_ms: i64,
        limit: Duration,
    ) -> Res<()> {
        let deadline = tokio::time::Instant::now() + limit;
        let mut last = String::from("no report");
        loop {
            let aggregate = self.processor(SERVICE, processor).await?;
            let mine = aggregate.instance(instance);
            let others: Vec<i64> = aggregate
                .instances
                .iter()
                .filter(|i| i.instance_id != instance)
                .map(|i| i.status_age_ms)
                .collect();
            if let (Some(mine), Some(other)) = (mine, others.iter().min()) {
                if mine.status_age_ms + margin_ms < *other {
                    return Ok(());
                }
                last = format!(
                    "{instance} reported {} ms ago, the freshest other {} ms ago",
                    mine.status_age_ms, other
                );
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "{instance}'s report never became the freshest of {processor} by {margin_ms} ms within {limit:?} ({last})"
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// The service every probe registers; the control paths name it.
pub const SERVICE: &str = "store-interop";

/// One path segment: the ids the hub accepts carry no slash, but a letter id is opaque text.
fn segment(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace('/', "%2F")
        .replace('?', "%3F")
        .replace('#', "%23")
}
