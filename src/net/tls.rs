//! TLS transport layer — optional encryption for the resync wire protocol.
//!
//!  When `--tls` is enabled, TCP streams are wrapped in a TLS layer *before*
//!  the `MsgCodec` framing, so all protocol traffic is encrypted transparently.
//!
//!  Server behaviour:
//!  - On first run, auto-generates a self-signed certificate and saves it to
//!    `~/.resync/cert.pem` + `~/.resync/key.pem`.
//!  - Custom certs can be supplied with `--tls-cert` and `--tls-key`.
//!
//!  Client behaviour:
//!  - By default, accepts any server certificate (self-signed OK).
//!  - `--tls-verify` enforces full certificate verification against system CAs.

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::info;

// ─── Configuration ───────────────────────────────────────────────────────────

/// TLS configuration parsed from CLI flags.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Whether TLS is enabled (`--tls`).
    pub enabled: bool,
    /// Path to a PEM-encoded certificate chain (`--tls-cert`).
    pub cert_path: Option<PathBuf>,
    /// Path to a PEM-encoded private key (`--tls-key`).
    pub key_path: Option<PathBuf>,
    /// Verify the server's certificate against system CAs (`--tls-verify`).
    /// When false (default), self-signed certificates are accepted.
    pub verify: bool,
}

impl TlsConfig {
    /// Default certificate and key paths: `~/.resync/cert.pem`, `~/.resync/key.pem`.
    pub fn default_paths() -> (PathBuf, PathBuf) {
        let base = home_dir().join(".resync");
        (base.join("cert.pem"), base.join("key.pem"))
    }

    /// Generate a self-signed certificate + private key using `rcgen`.
    ///
    /// The certificate includes SANs for `localhost`, `127.0.0.1`, and `::1`.
    /// For production or LAN use, supply proper certs via `--tls-cert`/`--tls-key`.
    ///
    /// Returns `(cert_pem_bytes, key_pem_bytes)`.
    pub fn generate_self_signed() -> Result<(Vec<u8>, Vec<u8>)> {
        let subject_alt_names = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
        ];

        let certified_key = rcgen::generate_simple_self_signed(subject_alt_names)
            .map_err(|e| anyhow::anyhow!("self-signed cert generation failed: {e}"))?;

        let cert_pem = certified_key.cert.pem().into_bytes();
        let key_pem = certified_key.key_pair.serialize_pem().into_bytes();

        Ok((cert_pem, key_pem))
    }

    /// Ensure certificate and key files exist on disk.
    ///
    /// - If custom paths are configured, those are returned as-is (caller is
    ///   responsible for ensuring they exist).
    /// - Otherwise, checks `~/.resync/cert.pem` and `~/.resync/key.pem`;
    ///   auto-generates a self-signed pair if either is missing.
    ///
    /// Returns `(cert_path, key_path)`.
    pub fn ensure_certs(&self) -> Result<(PathBuf, PathBuf)> {
        let (cert_path, key_path) = match (&self.cert_path, &self.key_path) {
            (Some(c), Some(k)) => return Ok((c.clone(), k.clone())),
            _ => Self::default_paths(),
        };

        if cert_path.exists() && key_path.exists() {
            return Ok((cert_path, key_path));
        }

        // Auto-generate self-signed certificate
        if let Some(parent) = cert_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }

        info!("generating self-signed TLS certificate …");
        let (cert_pem, key_pem) = Self::generate_self_signed()?;

        fs::write(&cert_path, &cert_pem)
            .with_context(|| format!("write {}", cert_path.display()))?;
        fs::write(&key_path, &key_pem).with_context(|| format!("write {}", key_path.display()))?;

        // Restrict private key permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600));
        }

        info!("TLS cert → {}", cert_path.display());
        info!("TLS key  → {}", key_path.display());

        Ok((cert_path, key_path))
    }
}

// ─── Server-side TLS ─────────────────────────────────────────────────────────

/// Perform a TLS handshake on an accepted TCP connection (server side).
///
/// Loads the certificate chain and private key from the paths in `config`,
/// auto-generating a self-signed pair if necessary.
pub async fn tls_accept(
    stream: TcpStream,
    config: &TlsConfig,
) -> Result<tokio_rustls::server::TlsStream<TcpStream>> {
    let (cert_path, key_path) = config.ensure_certs()?;
    let certs = load_certs(&cert_path)?;
    let key = load_private_key(&key_path)?;

    let tls_config = rustls::ServerConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .context("TLS protocol version setup")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("TLS server config")?;

    let acceptor = TlsAcceptor::from(Arc::new(tls_config));
    let tls_stream = acceptor.accept(stream).await.context("TLS accept")?;
    Ok(tls_stream)
}

// ─── Client-side TLS ─────────────────────────────────────────────────────────

/// Perform a TLS handshake on an outgoing TCP connection (client side).
///
/// - If `config.verify` is `true`, the server certificate is validated against
///   the system CA trust store (via `webpki-roots`).
/// - If `config.verify` is `false` (default), any certificate is accepted —
///   suitable for self-signed setups.
pub async fn tls_connect(
    stream: TcpStream,
    host: &str,
    config: &TlsConfig,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls_config = if config.verify {
        // Full verification against system CA roots
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        rustls::ClientConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()
            .context("TLS protocol version setup")?
            .with_root_certificates(root_store)
            .with_no_client_auth()
    } else {
        // Accept any certificate (self-signed OK)
        rustls::ClientConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()
            .context("TLS protocol version setup")?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoServerVerification(crypto_provider())))
            .with_no_client_auth()
    };

    let connector = TlsConnector::from(Arc::new(tls_config));

    let server_name = ServerName::try_from(host.to_owned())
        .map_err(|_| anyhow::anyhow!("invalid TLS server name: {host}"))?;

    let tls_stream = connector
        .connect(server_name, stream)
        .await
        .context("TLS connect")?;
    Ok(tls_stream)
}

// ─── MaybeTlsStream ─────────────────────────────────────────────────────────
//
// An enum that unifies plain TCP and TLS-wrapped streams behind the
// `AsyncRead + AsyncWrite` traits so that `Framed<MaybeTlsStream, MsgCodec>`
// works regardless of whether TLS is enabled.

/// A TCP stream that may or may not be wrapped in TLS.
pub enum MaybeTlsStream {
    /// Plain, unencrypted TCP.
    Plain(TcpStream),
    /// Server-side TLS (post-accept).
    ServerTls(tokio_rustls::server::TlsStream<TcpStream>),
    /// Client-side TLS (post-connect).
    ClientTls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::ServerTls(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::ClientTls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::ServerTls(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::ClientTls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::ServerTls(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::ClientTls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::ServerTls(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::ClientTls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

// ─── High-level helpers ──────────────────────────────────────────────────────
//
// These return `MaybeTlsStream` so callers don't need to branch on `tls.enabled`.

/// Server side: optionally wrap an accepted TCP stream in TLS.
///
/// Returns `MaybeTlsStream::Plain` when TLS is disabled,
/// `MaybeTlsStream::ServerTls` otherwise.
pub async fn accept_stream(stream: TcpStream, config: &TlsConfig) -> Result<MaybeTlsStream> {
    if config.enabled {
        let tls = tls_accept(stream, config).await?;
        Ok(MaybeTlsStream::ServerTls(tls))
    } else {
        Ok(MaybeTlsStream::Plain(stream))
    }
}

/// Client side: optionally wrap an outgoing TCP stream in TLS.
///
/// Returns `MaybeTlsStream::Plain` when TLS is disabled,
/// `MaybeTlsStream::ClientTls` otherwise.
pub async fn connect_stream(
    stream: TcpStream,
    host: &str,
    config: &TlsConfig,
) -> Result<MaybeTlsStream> {
    if config.enabled {
        let tls = tls_connect(stream, host, config).await?;
        Ok(MaybeTlsStream::ClientTls(tls))
    } else {
        Ok(MaybeTlsStream::Plain(stream))
    }
}

// ─── Internals ───────────────────────────────────────────────────────────────

/// Shared crypto provider (ring).
fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Resolve the user's home directory.
fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

/// Load PEM-encoded certificate chain from a file.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut cursor = Cursor::new(&data);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cursor)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parse certs from {}", path.display()))?;

    anyhow::ensure!(
        !certs.is_empty(),
        "no certificates found in {}",
        path.display()
    );
    Ok(certs)
}

/// Load a PEM-encoded private key from a file.
///
/// Tries PKCS#8 first (default for rcgen-generated keys), then falls back to
/// PKCS#1 (RSA) and SEC1 (EC) formats for user-provided keys.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;

    // Try PKCS#8 (generated by rcgen, most common)
    {
        let mut cursor = Cursor::new(&data);
        let keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut cursor)
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("parse PKCS#8 keys from {}", path.display()))?;
        if let Some(key) = keys.into_iter().next() {
            return Ok(PrivateKeyDer::Pkcs8(key));
        }
    }

    // Try PKCS#1 (RSA)
    {
        let mut cursor = Cursor::new(&data);
        let keys: Vec<_> = rustls_pemfile::rsa_private_keys(&mut cursor)
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("parse PKCS#1 keys from {}", path.display()))?;
        if let Some(key) = keys.into_iter().next() {
            return Ok(PrivateKeyDer::Pkcs1(key));
        }
    }

    // Try SEC1 (EC)
    {
        let mut cursor = Cursor::new(&data);
        let keys: Vec<_> = rustls_pemfile::ec_private_keys(&mut cursor)
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("parse SEC1 keys from {}", path.display()))?;
        if let Some(key) = keys.into_iter().next() {
            return Ok(PrivateKeyDer::Sec1(key));
        }
    }

    anyhow::bail!("no private key found in {}", path.display())
}

// ─── Dangerous: skip server certificate verification ─────────────────────────

/// A `ServerCertVerifier` that accepts any certificate without validation.
///
/// Used when `--tls-verify` is **not** set — the default for resync, since
/// self-signed certs are the common case for LAN file sync.
#[derive(Debug)]
struct NoServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
