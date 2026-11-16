//! The level-1 checks, one function per id of kraken `docs/libraries/feature-levels.md`.
//!
//! Every check starts its own instances of the service under test (fresh processes, fresh
//! instance ids), its own fake hubs, and is bounded in time; nothing is shared between checks
//! but the run's options and the identity provider.

mod connection;
mod credentials;
mod hubs;
mod modes;
mod requests;

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::OnceCell;
use tonic::Status;

use crate::fake_hub::{FakeHub, Session};
use crate::identity::Issuer;
use crate::proto::{Request, RequestMode};
use crate::report::{self, Verdict};
use crate::sut::{Sut, SutSpec};

/// An instance a check started. The runner stops it when the check ends (SIGTERM, then a kill
/// after 5 s, as the contract says), whatever the check did with its handle.
#[derive(Clone)]
pub struct SutRef(Arc<Mutex<Option<Sut>>>);

impl SutRef {
    pub fn is_running(&self) -> bool {
        self.0.lock().unwrap().as_mut().is_some_and(Sut::is_running)
    }

    /// Why the instance is gone, if it is.
    pub fn death(&self) -> Option<String> {
        self.0.lock().unwrap().as_mut().and_then(Sut::death)
    }
}

/// Waits for `fut` while the instance lives: fails at once, with its exit status and log, when
/// it dies first.
pub async fn up<T>(
    sut: &SutRef,
    what: &str,
    fut: impl Future<Output = Option<T>>,
) -> Result<T, Problem> {
    tokio::pin!(fut);
    loop {
        tokio::select! {
            v = &mut fut => return v.ok_or_else(|| Problem::Fail(what.to_string())),
            _ = tokio::time::sleep(Duration::from_millis(250)) => {
                if let Some(death) = sut.death() {
                    return Err(Problem::Fail(format!("{what}: {death}")));
                }
            }
        }
    }
}

/// What the runner was told.
#[derive(Clone, Debug)]
pub struct Options {
    /// The command that starts one instance of the conformance service.
    pub service_cmd: String,
    /// `Register.service_name` of the instances it starts.
    pub service: String,
    /// The real hubs (`host:port`), the first for the one-hub checks, two for L1-HUBS.
    pub hubs: Vec<String>,
    /// Where the instances' logs go.
    pub work_dir: PathBuf,
    /// The longest one check may take.
    pub check_timeout: Duration,
    /// How long an instance may take to start and register.
    pub startup: Duration,
    /// More variables for every instance (`--service-env NAME=VALUE`).
    pub extra_env: Vec<(String, String)>,
}

/// A check failed (what was expected, what was seen) or could not run here (why).
#[derive(Debug)]
pub enum Problem {
    Fail(String),
    Skip(String),
}

pub type CheckResult = Result<String, Problem>;

pub fn fail<T>(text: impl Into<String>) -> Result<T, Problem> {
    Err(Problem::Fail(text.into()))
}

macro_rules! ensure {
    ($cond:expr, $($text:tt)*) => {
        if !$cond {
            return Err($crate::checks::Problem::Fail(format!($($text)*)));
        }
    };
}
pub(crate) use ensure;

/// `?` on an `Option` or a `Result` that fails the check with `what`.
pub trait OrFail<T> {
    fn or_fail(self, what: &str) -> Result<T, Problem>;
}

impl<T> OrFail<T> for Option<T> {
    fn or_fail(self, what: &str) -> Result<T, Problem> {
        self.ok_or_else(|| Problem::Fail(what.to_string()))
    }
}

impl<T, E: std::fmt::Display> OrFail<T> for Result<T, E> {
    fn or_fail(self, what: &str) -> Result<T, Problem> {
        self.map_err(|e| Problem::Fail(format!("{what}: {e}")))
    }
}

/// One check id, its runner, and whether it needs something the run may not have.
pub struct Check {
    pub id: &'static str,
    pub needs: Needs,
    run: fn(Run) -> Pin<Box<dyn Future<Output = CheckResult> + Send>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Needs {
    /// Fake hubs only.
    Nothing,
    /// One real hub (`--hub`).
    Hub,
    /// Two real hubs (`--hub` twice).
    TwoHubs,
    /// Not implemented by this runner version: the reason.
    NotImplemented(&'static str),
}

macro_rules! check {
    ($id:literal, $needs:expr, $f:path) => {
        Check {
            id: $id,
            needs: $needs,
            run: |r| Box::pin($f(r)),
        }
    };
}

/// Every level-1 check, in the order of feature-levels.md.
pub fn level_1() -> Vec<Check> {
    use Needs::*;
    vec![
        check!("L1-REGISTER-FIRST", Nothing, connection::register_first),
        check!("L1-HANDSHAKE-ACK", Nothing, connection::handshake_ack),
        check!("L1-EARLY-REQUESTS", Nothing, connection::early_requests),
        check!("L1-REJECTION", Nothing, connection::rejection),
        check!("L1-HEARTBEAT", Hub, connection::heartbeat),
        check!("L1-SILENT-HUB", Nothing, connection::silent_hub),
        check!("L1-RECONNECT", Nothing, connection::reconnect),
        check!("L1-UNARY", Hub, requests::unary),
        check!("L1-REQUEST-ID", Nothing, requests::request_id),
        check!("L1-TIMEOUT", Nothing, requests::timeout),
        check!("L1-ERRORS", Nothing, requests::errors),
        check!("L1-NEVER-SENT", Nothing, requests::never_sent),
        check!("L1-RETRY", Nothing, requests::retry),
        check!("L1-ARRIVAL-STREAM", Nothing, requests::arrival_stream),
        check!("L1-ONE-TERMINAL", Nothing, requests::one_terminal),
        check!("L1-HANDLER-ERRORS", Nothing, requests::handler_errors),
        check!("L1-IDENTITY", Nothing, requests::identity),
        check!("L1-PROPAGATION", Nothing, requests::propagation),
        check!("L1-PERMITS", Nothing, requests::permits),
        check!("L1-SHARED", Hub, modes::shared),
        check!("L1-SCATTER", Hub, modes::scatter),
        check!("L1-STREAM", Nothing, modes::stream),
        check!("L1-SUBSCRIBE", Nothing, modes::subscribe),
        check!("L1-LOST-STREAM", Nothing, modes::lost_stream),
        check!("L1-TOKEN", Nothing, credentials::token),
        check!("L1-JWT", Nothing, credentials::jwt),
        check!("L1-MTLS", Nothing, credentials::mtls),
        check!("L1-TLS", Nothing, credentials::tls),
        check!("L1-HUBS", Nothing, hubs::hubs),
        check!("L1-HUB-ROUTING", Nothing, hubs::hub_routing),
        check!("L1-HUB-PERMITS", Nothing, hubs::hub_permits),
        check!("L1-HUB-FAILOVER", Nothing, hubs::hub_failover),
        check!("L1-STATUS", Nothing, connection::status),
    ]
}

/// The text of a caught panic.
fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "(no message)".into())
}

/// Before any check: one instance starts, registers on a fake hub and stops on SIGTERM within
/// 5 s. A service that cannot do that makes every check meaningless, so the run stops here.
pub async fn preflight(options: &Options) -> Result<String, String> {
    let fake = FakeHub::start("fake-hub-preflight").await;
    let spec = SutSpec {
        service: options.service.clone(),
        instance: format!("{}-preflight", options.service),
        hubs: vec![fake.address()],
        capacity: 16,
        extra: options.extra_env.clone(),
        ..SutSpec::default()
    };
    let log = options.work_dir.join(format!("{}.log", spec.instance));
    let mut sut = Sut::start(&options.service_cmd, &spec, log)?;
    let started = Instant::now();
    loop {
        if fake.sessions().iter().any(|s| s.acked()) {
            break;
        }
        if let Some(death) = sut.death() {
            return Err(format!("pre-flight: {death}"));
        }
        if started.elapsed() > options.startup {
            return Err(format!(
                "pre-flight: the service did not register within {} s (log {})",
                options.startup.as_secs(),
                sut.log.display()
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let registered = started.elapsed();
    if !sut.stop(Duration::from_secs(5)).await {
        return Err(format!(
            "pre-flight: the service did not stop within 5 s of SIGTERM (log {})",
            sut.log.display()
        ));
    }
    Ok(format!(
        "pre-flight: registered after {} ms, stopped on SIGTERM",
        registered.as_millis()
    ))
}

/// Runs `checks` one after the other (each bounded by `check_timeout`), stopping the instances
/// each check started when it ends.
pub async fn run_all(options: &Options, checks: Vec<Check>) -> Vec<Verdict> {
    let shared = Arc::new(Shared {
        options: options.clone(),
        issuer: OnceCell::new(),
        counter: AtomicUsize::new(0),
        launched: Mutex::new(Vec::new()),
    });
    let mut verdicts = Vec::new();
    for check in checks {
        let started = Instant::now();
        let (status, detail) = match check.needs {
            Needs::NotImplemented(reason) => (report::Status::NotImplemented, reason.to_string()),
            Needs::Hub if options.hubs.is_empty() => (
                report::Status::Skipped,
                "needs a real hub (--hub)".to_string(),
            ),
            Needs::TwoHubs if options.hubs.len() < 2 => (
                report::Status::Skipped,
                "needs a second hub (--hub twice)".to_string(),
            ),
            _ => {
                eprintln!("[kraken-conformance] {} ...", check.id);
                let run = Run {
                    shared: Arc::clone(&shared),
                    id: check.id,
                };
                // A panic inside a check (a runner bug or an unexpected frame) is that check's
                // FAIL with the panic's message; the run goes on.
                let guarded = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                    (check.run)(run),
                ));
                match tokio::time::timeout(options.check_timeout, guarded).await {
                    Ok(Ok(Ok(notes))) => (report::Status::Pass, notes),
                    Ok(Ok(Err(Problem::Fail(text)))) => (report::Status::Fail, text),
                    Ok(Ok(Err(Problem::Skip(text)))) => (report::Status::Skipped, text),
                    Ok(Err(panic)) => (
                        report::Status::Fail,
                        format!("the check panicked: {}", panic_message(&panic)),
                    ),
                    Err(_) => (
                        report::Status::Fail,
                        format!(
                            "did not finish within {} s",
                            options.check_timeout.as_secs()
                        ),
                    ),
                }
            }
        };
        let launched: Vec<SutRef> = shared.launched.lock().unwrap().drain(..).collect();
        for sut in launched {
            let taken = sut.0.lock().unwrap().take();
            if let Some(mut sut) = taken {
                sut.stop(Duration::from_secs(5)).await;
            }
        }
        eprintln!("[kraken-conformance] {} {:?}", check.id, status);
        verdicts.push(Verdict {
            id: check.id.to_string(),
            status,
            detail,
            millis: started.elapsed().as_millis() as u64,
        });
    }
    verdicts
}

struct Shared {
    options: Options,
    issuer: OnceCell<Arc<Issuer>>,
    counter: AtomicUsize,
    launched: Mutex<Vec<SutRef>>,
}

/// One check's run: what it starts is named after it.
pub struct Run {
    shared: Arc<Shared>,
    pub id: &'static str,
}

impl Run {
    pub fn options(&self) -> &Options {
        &self.shared.options
    }

    pub fn hub(&self) -> String {
        self.shared.options.hubs[0].clone()
    }

    /// A spec for one instance on `hubs`, capacity 16.
    pub fn spec(&self, hubs: Vec<String>) -> SutSpec {
        let n = self.shared.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let short = self.id.trim_start_matches("L1-").to_lowercase();
        SutSpec {
            service: self.shared.options.service.clone(),
            instance: format!("{}-{short}-{n}", self.shared.options.service),
            hubs,
            capacity: 16,
            extra: self.shared.options.extra_env.clone(),
            ..SutSpec::default()
        }
    }

    pub fn launch(&self, spec: &SutSpec) -> Result<SutRef, Problem> {
        let log = self
            .shared
            .options
            .work_dir
            .join(format!("{}.log", spec.instance));
        let sut = SutRef(Arc::new(Mutex::new(Some(
            Sut::start(&self.shared.options.service_cmd, spec, log).map_err(Problem::Fail)?,
        ))));
        self.shared.launched.lock().unwrap().push(sut.clone());
        Ok(sut)
    }

    pub fn startup(&self) -> Duration {
        self.shared.options.startup
    }

    /// The run's identity provider, started on first use.
    pub async fn issuer(&self) -> Arc<Issuer> {
        self.shared
            .issuer
            .get_or_init(|| async { Issuer::start().await })
            .await
            .clone()
    }

    /// The `n`th session of `fake` once its `Register` arrived, failing at once if the instance
    /// dies first (its exit status and log in the failure).
    pub async fn registered(
        &self,
        sut: &SutRef,
        fake: &FakeHub,
        n: usize,
    ) -> Result<Arc<Session>, Problem> {
        let within = if n == 0 {
            self.startup()
        } else {
            Duration::from_secs(20)
        };
        up(
            sut,
            &format!(
                "no stream {n} with a Register to {} within {} s",
                fake.hub_id(),
                within.as_secs()
            ),
            fake.registered(n, within),
        )
        .await
    }

    /// One fake hub and one instance on it, registered and acknowledged.
    pub async fn on_fake(&self, spec: impl FnOnce(&mut SutSpec)) -> Result<Rig, Problem> {
        let fake = FakeHub::start("fake-hub-1").await;
        scripted(&fake);
        let mut s = self.spec(vec![fake.address()]);
        spec(&mut s);
        let sut = self.launch(&s)?;
        let session = self.registered(&sut, &fake, 0).await?;
        session
            .until(Duration::from_secs(5), || session.acked().then_some(()))
            .await
            .or_fail("the fake hub did not acknowledge the Register")?;
        Ok(Rig {
            sut,
            session,
            fake,
            spec: s,
        })
    }
}

/// An instance on one fake hub (fields drop in order: the instance first).
pub struct Rig {
    pub sut: SutRef,
    pub session: Arc<Session>,
    pub fake: FakeHub,
    pub spec: SutSpec,
}

/// The fake hub's answers to the runner types the service sends (`conformance.runner.*`),
/// scripted by the payload the runner gave `conformance.Send` (conformance.md, "Scripted
/// answers"): `reply`, `hubError` / `replyError`, `silent`, `close`, `delayMs`; `steps` (one per
/// attempt of a `case`), `byHub` (by the fake hub's id). Without a script: `{"hub", "id"}`.
pub fn scripted(fake: &FakeHub) {
    let attempts: Arc<Mutex<HashMap<String, usize>>> = Arc::default();
    let hub_id = fake.hub_id();
    fake.on_request(move |session, request| {
        if request.mode != RequestMode::Unary as i32 {
            return;
        }
        let payload = crate::wire::json(&request.payload);
        let mut step = payload.clone();
        if let Some(by_hub) = step.get("byHub").and_then(|b| b.get(&hub_id)) {
            step = by_hub.clone();
        }
        if let Some(steps) = step.get("steps").and_then(Value::as_array).cloned() {
            let case = payload["case"].as_str().unwrap_or("default").to_string();
            let mut counts = attempts.lock().unwrap();
            let n = counts.entry(case).or_insert(0);
            step = steps
                .get(*n)
                .or(steps.last())
                .cloned()
                .unwrap_or(Value::Null);
            *n += 1;
        }
        apply(session, request, step, &hub_id);
    });
}

fn apply(session: &Arc<Session>, request: &Request, step: Value, hub_id: &str) {
    if step["silent"].as_bool() == Some(true) {
        return;
    }
    let session = Arc::clone(session);
    let id = request.id.clone();
    let default = json!({ "hub": hub_id, "id": request.id });
    tokio::spawn(async move {
        if let Some(ms) = step["delayMs"].as_u64() {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if step["close"].as_bool() == Some(true) {
            session.close(Status::unavailable("conformance: stream lost"));
        } else if let Some(code) = step["hubError"].as_i64() {
            session.hub_error(
                &id,
                code as i32,
                step["message"].as_str().unwrap_or("hub error"),
            );
        } else if let Some(e) = step.get("replyError") {
            let details = e["details"]
                .as_str()
                .unwrap_or_default()
                .as_bytes()
                .to_vec();
            session.reply_error(
                &id,
                e["code"].as_i64().unwrap_or(4) as i32,
                e["message"].as_str().unwrap_or_default(),
                &details,
            );
        } else if let Some(reply) = step.get("reply") {
            session.answer(&id, reply);
        } else {
            session.answer(&id, &default);
        }
    });
}

/// The outcome of a `conformance.Send`, failing the check when the service did not answer it.
pub fn outcome(r: Result<Value, crate::wire::WireError>, what: &str) -> Result<Value, Problem> {
    let v = r.or_fail(&format!("conformance.Send ({what}) failed"))?;
    if v["result"] == "unsupported" {
        return Err(Problem::Fail(format!(
            "the service does not support {what}: {}",
            v["reason"]
        )));
    }
    Ok(v)
}

/// Waits for an asynchronous `conformance.Send` started with `opId` (asking on `session`).
pub async fn await_op(session: &Session, op: &Value, wait: Duration) -> Result<Value, Problem> {
    let id = op["opId"]
        .as_str()
        .or_fail("conformance.Send did not return an opId")?;
    let v = session
        .call(
            crate::proto::RequestKind::Query,
            "conformance.Outcome",
            &json!({ "opId": id, "waitMs": wait.as_millis() as u64 }),
            wait + Duration::from_secs(10),
        )
        .await
        .or_fail("conformance.Outcome failed")?;
    ensure!(
        v["pending"] != true,
        "operation {id} still pending after {} ms",
        wait.as_millis()
    );
    Ok(v)
}

/// The error kind and code of a failed outcome, as `kind[/code]`.
pub fn error_of(v: &Value) -> String {
    match (v["error"]["kind"].as_str(), v["error"]["code"].as_str()) {
        (Some(k), Some(c)) => format!("{k}/{c}"),
        (Some(k), None) => k.to_string(),
        _ => format!("{v}"),
    }
}

/// The requests of `type_name` the library sent to `session`.
pub fn requests_of(session: &Session, type_name: &str) -> Vec<(Instant, Request)> {
    session.select(|m| match &m.message {
        Some(crate::proto::client_message::Message::Request(r)) if r.r#type == type_name => {
            Some(r.clone())
        }
        _ => None,
    })
}

pub fn ms(d: Duration) -> u64 {
    d.as_millis() as u64
}
