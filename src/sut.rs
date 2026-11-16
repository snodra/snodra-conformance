//! The service under test: the library's conformance service, started by the runner with the
//! `CONFORMANCE_*` environment (`protocol/conformance.md`, "Starting the service").

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::process::{Child, Command};

/// How one instance is started.
#[derive(Clone, Debug, Default)]
pub struct SutSpec {
    pub service: String,
    pub instance: String,
    /// `host:port` of every hub, in order.
    pub hubs: Vec<String>,
    /// Handlers at once (`Register.initial_permits` with one hub).
    pub capacity: u32,
    pub max_subscriptions: Option<u32>,
    pub stream_credit_timeout_ms: Option<u64>,
    pub hub_token: Option<String>,
    pub tls_ca: Option<PathBuf>,
    pub tls_server_name: Option<String>,
    /// The client certificate and key files (mutual TLS).
    pub tls_client: Option<(PathBuf, PathBuf)>,
    pub jwks_url: Option<String>,
    pub issuer: Option<String>,
    /// The service's own JWT (OAuth2 client credentials): token endpoint, client id, secret.
    pub oauth2: Option<(String, String, String)>,
    /// The library's retry policy for every send: attempts, initial backoff (ms).
    pub retry: Option<(u32, u64)>,
    /// More variables (`--service-env`, the runner's own tests).
    pub extra: Vec<(String, String)>,
}

impl SutSpec {
    pub fn env(&self) -> Vec<(String, String)> {
        let mut env: Vec<(String, String)> = vec![
            ("CONFORMANCE_SERVICE_NAME".into(), self.service.clone()),
            ("CONFORMANCE_INSTANCE_ID".into(), self.instance.clone()),
            ("CONFORMANCE_HUBS".into(), self.hubs.join(",")),
            ("CONFORMANCE_CAPACITY".into(), self.capacity.to_string()),
        ];
        let mut opt = |name: &str, v: Option<String>| {
            if let Some(v) = v {
                env.push((name.to_string(), v));
            }
        };
        let path = |p: &PathBuf| p.display().to_string();
        opt(
            "CONFORMANCE_MAX_SUBSCRIPTIONS",
            self.max_subscriptions.map(|n| n.to_string()),
        );
        opt(
            "CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS",
            self.stream_credit_timeout_ms.map(|n| n.to_string()),
        );
        opt("CONFORMANCE_HUB_TOKEN", self.hub_token.clone());
        opt("CONFORMANCE_TLS_CA", self.tls_ca.as_ref().map(path));
        opt("CONFORMANCE_TLS_SERVER_NAME", self.tls_server_name.clone());
        opt(
            "CONFORMANCE_TLS_CLIENT_CERT",
            self.tls_client.as_ref().map(|(c, _)| path(c)),
        );
        opt(
            "CONFORMANCE_TLS_CLIENT_KEY",
            self.tls_client.as_ref().map(|(_, k)| path(k)),
        );
        opt("CONFORMANCE_JWKS_URL", self.jwks_url.clone());
        opt("CONFORMANCE_ISSUER", self.issuer.clone());
        if let Some((url, id, secret)) = &self.oauth2 {
            opt("CONFORMANCE_OAUTH2_TOKEN_URL", Some(url.clone()));
            opt("CONFORMANCE_OAUTH2_CLIENT_ID", Some(id.clone()));
            opt("CONFORMANCE_OAUTH2_CLIENT_SECRET", Some(secret.clone()));
        }
        if let Some((attempts, backoff)) = self.retry {
            opt("CONFORMANCE_RETRY_MAX_ATTEMPTS", Some(attempts.to_string()));
            opt(
                "CONFORMANCE_RETRY_INITIAL_BACKOFF_MS",
                Some(backoff.to_string()),
            );
        }
        env.extend(self.extra.iter().cloned());
        env
    }
}

/// Splits a command line into arguments, without a shell: words separated by blanks, `'...'`
/// taken literally, `"..."` with `\"` and `\\` escapes, a backslash outside quotes escaping the
/// next character. No variables, globs, pipes or redirections.
pub fn split_command(line: &str) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut in_word = false;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' | '\n' => {
                if in_word {
                    args.push(std::mem::take(&mut cur));
                    in_word = false;
                }
            }
            '\'' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(c) => cur.push(c),
                        None => return Err("unterminated ' in --service-cmd".into()),
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some(c @ ('"' | '\\')) => cur.push(c),
                            Some(c) => {
                                cur.push('\\');
                                cur.push(c);
                            }
                            None => return Err("unterminated \" in --service-cmd".into()),
                        },
                        Some(c) => cur.push(c),
                        None => return Err("unterminated \" in --service-cmd".into()),
                    }
                }
            }
            '\\' => {
                in_word = true;
                match chars.next() {
                    Some(c) => cur.push(c),
                    None => return Err("a trailing \\ in --service-cmd".into()),
                }
            }
            c => {
                in_word = true;
                cur.push(c);
            }
        }
    }
    if in_word {
        args.push(cur);
    }
    if args.is_empty() {
        return Err("--service-cmd is empty".into());
    }
    Ok(args)
}

/// A running instance, in a process group of its own; dropping it kills the group.
pub struct Sut {
    child: Child,
    pgid: Option<i32>,
    pub instance: String,
    pub log: PathBuf,
    exited: Option<ExitStatus>,
}

const SIGTERM: i32 = 15;
const SIGKILL: i32 = 9;

unsafe extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

fn signal_group(pgid: i32, sig: i32) {
    // SAFETY: a signal to the process group the runner created for this instance.
    unsafe {
        libc_kill(-pgid, sig);
    }
}

impl Sut {
    /// Runs `command` (split by [`split_command`], no shell) with the spec's environment, its
    /// output in `log`, as the leader of a new process group (so wrapper scripts and build tools
    /// are stopped with it).
    pub fn start(command: &str, spec: &SutSpec, log: PathBuf) -> Result<Sut, String> {
        let argv = split_command(command)?;
        let out = std::fs::File::create(&log).map_err(|e| format!("{}: {e}", log.display()))?;
        let err = out.try_clone().map_err(|e| e.to_string())?;
        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .process_group(0)
            .kill_on_drop(true);
        for (k, v) in spec.env() {
            cmd.env(k, v);
        }
        let child = cmd
            .spawn()
            .map_err(|e| format!("cannot start the service ({}): {e}", argv[0]))?;
        let pgid = child.id().map(|p| p as i32);
        Ok(Sut {
            child,
            pgid,
            instance: spec.instance.clone(),
            log,
            exited: None,
        })
    }

    /// `None` while the process runs, else how it ended.
    pub fn exit_status(&mut self) -> Option<ExitStatus> {
        if self.exited.is_none() {
            self.exited = self.child.try_wait().ok().flatten();
        }
        self.exited
    }

    pub fn is_running(&mut self) -> bool {
        self.exit_status().is_none()
    }

    /// Why the instance is gone, with the end of its log.
    pub fn death(&mut self) -> Option<String> {
        let status = self.exit_status()?;
        let tail: String = std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .rev()
            .take(5)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join(" | ");
        Some(format!(
            "the service instance {} exited ({status}); log {}: {tail}",
            self.instance,
            self.log.display()
        ))
    }

    /// Stops the instance as the contract says: SIGTERM to its group, a kill after `grace`.
    /// Returns whether it stopped on SIGTERM alone.
    pub async fn stop(&mut self, grace: Duration) -> bool {
        if self.exit_status().is_some() {
            return true;
        }
        if let Some(pgid) = self.pgid {
            signal_group(pgid, SIGTERM);
        }
        match tokio::time::timeout(grace, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.exited = Some(status);
                true
            }
            _ => {
                if let Some(pgid) = self.pgid {
                    signal_group(pgid, SIGKILL);
                }
                let _ = self.child.kill().await;
                false
            }
        }
    }
}

impl Drop for Sut {
    fn drop(&mut self) {
        if self.exited.is_none()
            && let Some(pgid) = self.pgid
        {
            signal_group(pgid, SIGKILL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::split_command;

    #[test]
    fn commands_split_without_a_shell() {
        assert_eq!(
            split_command(r#"java -jar "my service.jar" --x='a b' c\ d"#).unwrap(),
            ["java", "-jar", "my service.jar", "--x=a b", "c d"]
        );
        assert!(split_command("'open").is_err());
        assert!(split_command("  ").is_err());
    }
}
