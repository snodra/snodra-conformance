//! The runner as an identity provider: an RSA key made at start, a JWKS endpoint, signed caller
//! tokens (L1-IDENTITY, L1-PROPAGATION) and an OAuth2 client-credentials token endpoint whose
//! tokens it records (L1-JWT). Plain HTTP on `127.0.0.1`, enough for the service under test.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct Issuer {
    key: EncodingKey,
    /// A key the JWKS does not publish (tokens signed with it must be refused).
    other: EncodingKey,
    kid: String,
    jwks: String,
    pub addr: SocketAddr,
    /// Lifetime of the tokens the token endpoint issues.
    lifetime: Mutex<Duration>,
    /// Every token the token endpoint issued, with when.
    issued: Mutex<Vec<(Instant, String)>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Issuer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

impl Issuer {
    pub async fn start() -> Arc<Issuer> {
        let (private, other) = tokio::task::spawn_blocking(|| {
            let mut rng = rand::thread_rng();
            (
                rsa::RsaPrivateKey::new(&mut rng, 2048).expect("RSA key"),
                rsa::RsaPrivateKey::new(&mut rng, 2048).expect("RSA key"),
            )
        })
        .await
        .unwrap();
        let other =
            EncodingKey::from_rsa_der(other.to_pkcs1_der().expect("RSA key DER").as_bytes());
        let der = private.to_pkcs1_der().expect("RSA key DER");
        let key = EncodingKey::from_rsa_der(der.as_bytes());
        let kid = "conformance-key".to_string();
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let jwks = json!({ "keys": [{
            "kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
            "n": b64.encode(private.n().to_bytes_be()),
            "e": b64.encode(private.e().to_bytes_be()),
        }]})
        .to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        Arc::new_cyclic(|weak: &std::sync::Weak<Issuer>| {
            let weak = weak.clone();
            let server = tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let weak = weak.clone();
                    tokio::spawn(async move {
                        if let Some(issuer) = weak.upgrade() {
                            issuer.serve(socket).await;
                        }
                    });
                }
            });
            Issuer {
                key,
                other,
                kid,
                jwks,
                addr,
                lifetime: Mutex::new(Duration::from_secs(300)),
                issued: Mutex::new(Vec::new()),
                server,
            }
        })
    }

    pub fn jwks_url(&self) -> String {
        format!("http://{}/jwks", self.addr)
    }

    pub fn issuer(&self) -> String {
        format!("http://{}/conformance", self.addr)
    }

    pub fn token_url(&self) -> String {
        format!("http://{}/token", self.addr)
    }

    pub fn set_lifetime(&self, lifetime: Duration) {
        *self.lifetime.lock().unwrap() = lifetime;
    }

    pub fn issued(&self) -> Vec<(Instant, String)> {
        self.issued.lock().unwrap().clone()
    }

    /// A caller token for `sub`, expiring `expires_in` seconds from now (negative: expired).
    pub fn mint(&self, sub: &str, expires_in: i64) -> String {
        let iat = now() as i64;
        self.sign(json!({
            "iss": self.issuer(), "sub": sub, "iat": iat, "exp": iat + expires_in,
            "azp": "conformance", "realm_access": { "roles": [] },
        }))
    }

    /// A token like [`mint`](Self::mint) signed by a key the JWKS does not publish, under the
    /// published key id.
    pub fn mint_foreign_key(&self, sub: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(self.kid.clone());
        jsonwebtoken::encode(&header, &self.claims(sub, &self.issuer()), &self.other)
            .expect("token")
    }

    /// A valid signature from another issuer.
    pub fn mint_wrong_issuer(&self, sub: &str) -> String {
        self.sign(self.claims(sub, "http://conformance.invalid/other-issuer"))
    }

    fn claims(&self, sub: &str, iss: &str) -> Value {
        let iat = now() as i64;
        json!({ "iss": iss, "sub": sub, "iat": iat, "exp": iat + 300,
                "azp": "conformance", "realm_access": { "roles": [] } })
    }

    fn sign(&self, claims: Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(self.kid.clone());
        jsonwebtoken::encode(&header, &claims, &self.key).expect("token")
    }

    async fn serve(&self, mut socket: tokio::net::TcpStream) {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        // The head, then a body of Content-Length.
        let (head, mut body) = loop {
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break (
                    String::from_utf8_lossy(&buf[..i]).into_owned(),
                    buf[i + 4..].to_vec(),
                );
            }
        };
        let length = head
            .lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(n, _)| n.trim().eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        while body.len() < length {
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => body.extend_from_slice(&chunk[..n]),
            }
        }
        let line = head.lines().next().unwrap_or_default().to_string();
        let mut words = line.split_whitespace();
        let method = words.next().unwrap_or_default();
        let path = words.next().unwrap_or_default();
        let (status, json) = match (method, path) {
            ("GET", p) if p.starts_with("/jwks") => ("200 OK", self.jwks.clone()),
            ("POST", p) if p.starts_with("/token") => {
                let lifetime = *self.lifetime.lock().unwrap();
                let iat = now();
                let token = self.sign(json!({
                    "iss": self.issuer(), "sub": "service-account", "azp": "conformance",
                    "iat": iat, "exp": iat + lifetime.as_secs(), "jti": crate::wire::new_id(),
                }));
                self.issued
                    .lock()
                    .unwrap()
                    .push((Instant::now(), token.clone()));
                (
                    "200 OK",
                    json!({ "access_token": token, "token_type": "Bearer",
                            "expires_in": lifetime.as_secs() })
                    .to_string(),
                )
            }
            _ => ("404 Not Found", "{}".to_string()),
        };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",
            json.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
    }
}
