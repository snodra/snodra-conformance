//! Credentials: a static token, a JWT and its renewal, client certificates, TLS. All on fake
//! hubs: what the library presents and when, not a real hub's verdict on it.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use serde_json::json;
use tonic::Status;

use super::{CheckResult, OrFail, Problem, Run, ensure, ms, up};
use crate::fake_hub::{FakeHub, Session, TlsSetup};
use crate::proto::client_message::Message as M;
use crate::proto::{Credentials, credentials};

const HUB_NAME: &str = "conformance-hub.test";

fn token_of(c: &Option<Credentials>) -> Option<String> {
    match c.as_ref().and_then(|c| c.kind.as_ref()) {
        Some(credentials::Kind::Token(t)) => Some(t.clone()),
        _ => None,
    }
}

fn jwt_of(c: &Option<Credentials>) -> Option<String> {
    match c.as_ref().and_then(|c| c.kind.as_ref()) {
        Some(credentials::Kind::Jwt(t)) => Some(t.clone()),
        _ => None,
    }
}

pub async fn token(run: Run) -> CheckResult {
    let a = FakeHub::start("hub-a").await;
    let b = FakeHub::start("hub-b").await;
    let token = format!("conformance-token-{}", &crate::wire::new_id()[..8]);
    let mut spec = run.spec(vec![a.address(), b.address()]);
    spec.hub_token = Some(token.clone());
    let sut = run.launch(&spec)?;
    let mut sessions = vec![
        run.registered(&sut, &a, 0).await?,
        run.registered(&sut, &b, 0).await?,
    ];
    sessions[0].close(Status::unavailable("conformance: reconnect"));
    sessions.push(run.registered(&sut, &a, 1).await?);
    for s in &sessions {
        let r = s.register(Duration::ZERO).await.unwrap();
        ensure!(
            token_of(&r.credentials).as_deref() == Some(token.as_str()),
            "stream {} registered without the token in Register.credentials.token",
            s.index
        );
    }
    Ok("the token on every stream to both hubs, also after a reconnect. Proved on the frames to the runner's \
        fake hubs, not against a hub in mode enforce; never logging the token is the library's own test"
        .into())
}

pub async fn jwt(run: Run) -> CheckResult {
    const LIFETIME: Duration = Duration::from_secs(10);
    let issuer = run.issuer().await;
    issuer.set_lifetime(LIFETIME);
    let fake = FakeHub::start("fake-hub-1").await;
    let mut spec = run.spec(vec![fake.address()]);
    spec.oauth2 = Some((
        issuer.token_url(),
        "conformance".into(),
        "conformance-secret".into(),
    ));
    let sut = run.launch(&spec)?;
    let s0 = run.registered(&sut, &fake, 0).await?;
    let first = jwt_of(&s0.register(Duration::ZERO).await.unwrap().credentials)
        .or_fail("the Register carries no Register.credentials.jwt")?;
    let issued_at = |t: &str| {
        issuer
            .issued()
            .iter()
            .find(|(_, i)| i == t)
            .map(|(at, _)| *at)
    };
    let first_at =
        issued_at(&first).or_fail("the Register's JWT was not issued by the token endpoint")?;
    let refreshes = |s: &Session| -> Vec<(Instant, String)> {
        s.select(|m| match &m.message {
            Some(M::Credentials(c)) => jwt_of(&Some(c.clone())),
            _ => None,
        })
    };
    // Two lifetimes and a half: at least two renewals for a library renewing before expiry.
    tokio::time::sleep(LIFETIME * 5 / 2).await;
    let end = Instant::now();
    let sent = refreshes(&s0);
    ensure!(
        !sent.is_empty(),
        "a {} s token was not renewed on the live stream in {} s (ClientMessage.credentials)",
        LIFETIME.as_secs(),
        (LIFETIME * 5 / 2).as_secs()
    );
    // protocol.md, Credentials: "renew it before it expires ... send each new token on the live
    // streams as ClientMessage.credentials, at most one per renewal". Each token in force must be
    // replaced before its own expiry (issued + lifetime; the fake's clock is the issuer's).
    let mut in_force = (first.clone(), first_at);
    let mut distinct = std::collections::HashSet::new();
    for (at, token) in &sent {
        let issued =
            issued_at(token).or_fail("a renewal sent a token the endpoint never issued")?;
        ensure!(
            distinct.insert(token.clone()) && *token != in_force.0,
            "a token was sent twice as ClientMessage.credentials"
        );
        ensure!(
            *at < in_force.1 + LIFETIME,
            "a renewal came {} ms after the token it replaced had expired",
            ms(*at - (in_force.1 + LIFETIME))
        );
        in_force = (token.clone(), issued);
    }
    ensure!(
        end < in_force.1 + LIFETIME,
        "the token in force expired {} ms before the end of the observation without a renewal",
        ms(end - (in_force.1 + LIFETIME))
    );
    let closed = Instant::now();
    s0.close(Status::unauthenticated("credentials expired"));
    let s1 = run.registered(&sut, &fake, 1).await?;
    let next = jwt_of(&s1.register(Duration::ZERO).await.unwrap().credentials)
        .or_fail("the new stream registered without a JWT")?;
    ensure!(
        issued_at(&next).is_some_and(|t| t >= closed),
        "after an UNAUTHENTICATED close the next stream registered with a token fetched before the close"
    );
    Ok(format!(
        "registered with a fetched JWT; {} renewals, each before the previous token expired, sent once; a newly \
         fetched token after UNAUTHENTICATED. Proved on the frames to the runner's fake hub and its token endpoint, \
         not against a hub in mode enforce",
        sent.len()
    ))
}

/// A certificate authority of the runner's own.
struct Ca {
    params: rcgen::CertificateParams,
    key: rcgen::KeyPair,
    pem: String,
}

/// A certificate and its key, PEM.
struct Pair {
    cert: String,
    key: String,
    der: Vec<u8>,
}

impl Ca {
    fn new(name: &str) -> Result<Ca, Problem> {
        let key = rcgen::KeyPair::generate().or_fail("CA key")?;
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).or_fail("CA")?;
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let pem = params.self_signed(&key).or_fail("CA certificate")?.pem();
        Ok(Ca { params, key, pem })
    }

    fn issue(&self, params: rcgen::CertificateParams) -> Result<Pair, Problem> {
        let key = rcgen::KeyPair::generate().or_fail("key")?;
        let issuer = rcgen::Issuer::from_params(&self.params, &self.key);
        let cert = params.signed_by(&key, &issuer).or_fail("certificate")?;
        Ok(Pair {
            cert: cert.pem(),
            key: key.serialize_pem(),
            der: cert.der().to_vec(),
        })
    }

    fn server(&self, name: &str) -> Result<Pair, Problem> {
        self.issue(
            rcgen::CertificateParams::new(vec![name.to_string()]).or_fail("hub certificate")?,
        )
    }

    /// A client certificate naming `service` (`kraken://service/<name>`), valid until `not_after`.
    fn client(&self, service: &str, not_after: SystemTime) -> Result<Pair, Problem> {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).or_fail("client")?;
        params.subject_alt_names = vec![rcgen::SanType::URI(
            rcgen::string::Ia5String::try_from(format!("kraken://service/{service}"))
                .or_fail("URI SAN")?,
        )];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, service);
        params.not_before = time::OffsetDateTime::now_utc() - time::Duration::minutes(5);
        params.not_after = time::OffsetDateTime::from(not_after);
        self.issue(params)
    }
}

fn write(path: &Path, content: &str) -> Result<(), Problem> {
    // Replaced atomically, as a secret mount or cert-manager does.
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, content).or_fail("write")?;
    std::fs::rename(&tmp, path).or_fail("rename")?;
    Ok(())
}

/// The client certificate files of one instance, under the work directory.
fn client_files(run: &Run, name: &str, pair: &Pair) -> Result<(PathBuf, PathBuf), Problem> {
    let cert = run
        .options()
        .work_dir
        .join(format!("{}-{name}.crt", run.id));
    let key = run
        .options()
        .work_dir
        .join(format!("{}-{name}.key", run.id));
    write(&cert, &pair.cert)?;
    write(&key, &pair.key)?;
    Ok((cert, key))
}

pub async fn mtls(run: Run) -> CheckResult {
    let ca = Ca::new("kraken conformance CA")?;
    let hub = ca.server(HUB_NAME)?;
    let ca_file = run.options().work_dir.join(format!("{}-ca.pem", run.id));
    write(&ca_file, &ca.pem)?;
    let tls = TlsSetup {
        cert_pem: hub.cert.clone(),
        key_pem: hub.key.clone(),
        client_ca_pem: Some(ca.pem.clone()),
    };
    let service = run.options().service.clone();
    // The runner's own fake hub verifies client certificates: one from another CA never gets a
    // stream. This proves the fixture (the presented certificate below is really verified), not
    // a property of the library.
    let other = Ca::new("another CA")?;
    let refused_hub = FakeHub::start_with("fake-hub-mtls-0", Some(tls.clone())).await;
    let mut spec = run.spec(vec![refused_hub.address()]);
    spec.tls_ca = Some(ca_file.clone());
    spec.tls_server_name = Some(HUB_NAME.into());
    let stranger = other.client(&service, SystemTime::now() + Duration::from_secs(3600))?;
    spec.tls_client = Some(client_files(&run, "stranger", &stranger)?);
    let stranger_sut = run.launch(&spec)?;
    tokio::time::sleep(Duration::from_secs(6)).await;
    ensure!(
        refused_hub.sessions().is_empty(),
        "runner fixture broken: a client certificate of another CA reached the fake hub"
    );
    let _ = stranger_sut;
    // A certificate valid for 60 s, renewed in the files 2 s after the first stream: the library
    // reconnects with the renewed pair before the first one's notAfter (protocol.md,
    // Credentials, L1-MTLS), without a restart.
    let fake = FakeHub::start_with("fake-hub-mtls", Some(tls)).await;
    let not_after = SystemTime::now() + Duration::from_secs(60);
    let deadline = Instant::now() + Duration::from_secs(59);
    let first = ca.client(&service, not_after)?;
    let mut spec = run.spec(vec![fake.address()]);
    spec.tls_ca = Some(ca_file);
    spec.tls_server_name = Some(HUB_NAME.into());
    let files = client_files(&run, "client", &first)?;
    spec.tls_client = Some(files.clone());
    let sut = run.launch(&spec)?;
    let s0 = run.registered(&sut, &fake, 0).await?;
    ensure!(
        s0.peer_cert.as_deref() == Some(&first.der[..]),
        "the first stream did not present the certificate of the files"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let renewed = ca.client(&service, SystemTime::now() + Duration::from_secs(3600))?;
    write(&files.1, &renewed.key)?;
    write(&files.0, &renewed.cert)?;
    let renewed_at = Instant::now();
    let rolled = up(
        &sut,
        "the renewed certificate was not presented before the first one's notAfter",
        fake.until(deadline.saturating_duration_since(Instant::now()), || {
            fake.sessions()
                .into_iter()
                .find(|s| s.peer_cert.as_deref() == Some(&renewed.der[..]) && s.acked())
        }),
    )
    .await?;
    // The hub's close at notAfter ("certificate expired"): an ordinary reconnect at the normal
    // backoff (at most the 5 s cap), not a registration refusal.
    let n = fake.sessions().len();
    let closed = Instant::now();
    rolled.close(Status::unauthenticated("certificate expired"));
    let again = up(
        &sut,
        "no new stream within 6 s of the hub's certificate-expired close (the backoff cap is at most 5 s)",
        fake.registered(n, Duration::from_secs(6)),
    )
    .await?;
    again
        .until(Duration::from_secs(5), || again.acked().then_some(()))
        .await
        .or_fail("ack")?;
    let v = again
        .query("conformance.View", json!({}))
        .await
        .or_fail("conformance.View")?;
    ensure!(
        !v["lastRejection"]
            .as_str()
            .unwrap_or_default()
            .contains("certificate expired"),
        "the certificate-expired close was handled as a registration refusal: {}",
        v["lastRejection"]
    );
    Ok(format!(
        "the renewed pair (60 s certificate replaced in the files) was presented {} ms after the files changed, \
         before notAfter, without a restart; after the certificate-expired close, a new stream {} ms later, not a \
         refusal. On the runner's fake hub, which verified the certificates (one of another CA got no stream: the \
         fixture, not the library), without kraken-hub's check of the service name",
        ms(rolled.started - renewed_at),
        ms(again.started - closed)
    ))
}

pub async fn tls(run: Run) -> CheckResult {
    let ca = Ca::new("kraken conformance CA")?;
    let ca_file = run.options().work_dir.join(format!("{}-ca.pem", run.id));
    write(&ca_file, &ca.pem)?;
    let start = |pair: &Pair, id: &str| {
        let tls = TlsSetup {
            cert_pem: pair.cert.clone(),
            key_pem: pair.key.clone(),
            client_ca_pem: None,
        };
        let id = id.to_string();
        async move { FakeHub::start_with(&id, Some(tls)).await }
    };
    // The operator's CA and server name: registered.
    let good = start(&ca.server(HUB_NAME)?, "fake-hub-tls").await;
    let mut spec = run.spec(vec![good.address()]);
    spec.tls_ca = Some(ca_file.clone());
    spec.tls_server_name = Some(HUB_NAME.into());
    let sut = run.launch(&spec)?;
    let s = run.registered(&sut, &good, 0).await?;
    s.until(Duration::from_secs(5), || s.acked().then_some(()))
        .await
        .or_fail("ack")?;
    s.command("conformance.Echo", json!({ "tls": true }))
        .await
        .or_fail("an Echo over TLS")?;
    // A hub certificate of another CA, and one for another name: no stream may be opened.
    let other = Ca::new("another CA")?;
    let wrong_ca = start(&other.server(HUB_NAME)?, "fake-hub-tls-other-ca").await;
    let wrong_name = start(&ca.server("another-host.test")?, "fake-hub-tls-other-name").await;
    for (hub, what) in [
        (&wrong_ca, "signed by another CA"),
        (&wrong_name, "for another host name"),
    ] {
        let mut spec = run.spec(vec![hub.address()]);
        spec.tls_ca = Some(ca_file.clone());
        spec.tls_server_name = Some(HUB_NAME.into());
        let _sut = run.launch(&spec)?;
        tokio::time::sleep(Duration::from_secs(6)).await;
        ensure!(
            hub.sessions().is_empty(),
            "the library registered with a hub whose certificate is {what} (TLS not verified)"
        );
    }
    Ok(format!(
        "registered over TLS with a CA file and server name {HUB_NAME}; refused a hub certificate of another CA \
         and one for another name. On the runner's fake hubs"
    ))
}
