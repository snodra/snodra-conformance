//! What the runner's own tests share: the binaries of the workspace, built once, and a run of
//! some checks.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use kraken_conformance::checks::{self, Options};
use kraken_conformance::report::{Status, Verdict};

/// A binary of the workspace's package `package`, built (once per package) with cargo.
pub fn binary(package: &str, bin: &str) -> PathBuf {
    static BUILT: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut built = BUILT.lock().unwrap();
    if !built.iter().any(|p| p == package) {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let status = std::process::Command::new(cargo)
            .args(["build", "-p", package])
            .current_dir(&root)
            .status()
            .expect("cargo build");
        assert!(status.success(), "{package} did not build");
        built.push(package.to_string());
    }
    root.join("target/debug").join(bin)
}

pub fn reference_service() -> PathBuf {
    binary(
        "kraken-conformance-reference",
        "conformance-reference-service",
    )
}

pub fn violator() -> PathBuf {
    binary("kraken-conformance-violator", "conformance-violator")
}

/// Runs the checks `ids` (all of level 1 when empty) against `service`, with `env` for every
/// instance and `hubs` as the real hubs. The work directory is removed afterwards.
pub async fn run_with(
    service: PathBuf,
    ids: &[&str],
    env: &[(&str, &str)],
    hubs: Vec<String>,
) -> Vec<Verdict> {
    let work_dir =
        std::env::temp_dir().join(format!("kraken-conformance-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&work_dir).unwrap();
    let options = Options {
        service_cmd: format!("'{}'", service.display()),
        service: "conformance-rust".into(),
        hubs,
        work_dir: work_dir.clone(),
        check_timeout: Duration::from_secs(150),
        startup: Duration::from_secs(30),
        extra_env: env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    let mut all = checks::level_1();
    if !ids.is_empty() {
        all.retain(|c| ids.contains(&c.id));
    }
    let verdicts = checks::run_all(&options, all).await;
    if std::env::var("KRAKEN_CONFORMANCE_KEEP_LOGS").is_err() {
        let _ = std::fs::remove_dir_all(&work_dir);
    }
    verdicts
}

/// The reference service, with `env`.
pub async fn run(ids: &[&str], env: &[(&str, &str)], hubs: Vec<String>) -> Vec<Verdict> {
    run_with(reference_service(), ids, env, hubs).await
}

pub fn verdict<'a>(verdicts: &'a [Verdict], id: &str) -> &'a Verdict {
    verdicts
        .iter()
        .find(|v| v.id == id)
        .unwrap_or_else(|| panic!("no verdict for {id}"))
}

pub fn status_of(verdicts: &[Verdict], id: &str) -> Status {
    verdict(verdicts, id).status.clone()
}

/// A `kraken-hub` container (`KRAKEN_HUB_IMAGE`, default `kraken-hub:modular`) and its gRPC
/// address. Needs Docker.
pub async fn hub_container() -> (
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
    String,
) {
    use testcontainers::ImageExt;
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    let image = std::env::var("KRAKEN_HUB_IMAGE").unwrap_or_else(|_| "kraken-hub:modular".into());
    let (name, tag) = image.split_once(':').unwrap_or((&image, "latest"));
    let container = testcontainers::GenericImage::new(name, tag)
        .with_exposed_port(7000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("hub listening"))
        .with_env_var("KRAKEN_HUB_OTLP_ENDPOINT", "")
        .start()
        .await
        .expect("hub container");
    let port = container.get_host_port_ipv4(7000.tcp()).await.unwrap();
    (container, format!("127.0.0.1:{port}"))
}
