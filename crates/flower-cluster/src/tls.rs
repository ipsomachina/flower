//! Peer TLS: the Raft transport inside server-authenticated TLS.
//!
//! As in v1: rustls with the AWS-LC provider, TLS 1.2 and 1.3. A
//! listener presents its certificate chain; a dialer trusts only the
//! configured CA roots and checks the certificate covers the host name
//! or IP of the member address it dialed. There is no client
//! certificate: peers authenticate with the peer token, which the hello
//! now carries inside TLS, so this is server-authenticated TLS plus
//! token authentication, not mutual TLS. The hello, identity and
//! contract checks and every frame run unchanged inside the stream.
//!
//! Every member of a group must agree on TLS mode: a TLS dialer refuses
//! a plaintext listener's answer and a TLS listener refuses a plaintext
//! hello, so enabling TLS on a running group is a coordinated cutover.
//! There is no verification bypass and no plaintext fallback.
//! Certificates load once at startup; replacing the files needs a
//! restart.

use std::fmt;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};
use zeroize::Zeroizing;

/// A member's TLS credentials and trust roots.
#[derive(Clone)]
pub struct PeerTls {
    acceptor: TlsAcceptor,
    connector: TlsConnector,
}

impl fmt::Debug for PeerTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeerTls { .. }")
    }
}

impl PeerTls {
    /// Load PEM files: the certificate chain this member presents (its
    /// leaf first, then intermediates), its private key (PKCS#8, PKCS#1
    /// or SEC1) and the CA roots it trusts for its peers.
    pub fn from_files(chain: &Path, key: &Path, roots: &Path) -> Result<Self, String> {
        let read = |path: &Path, what: &str| {
            std::fs::read(path)
                .map_err(|error| format!("read the {what} {}: {error}", path.display()))
        };
        let chain = read(chain, "peer TLS certificate chain")?;
        let key = Zeroizing::new(read(key, "peer TLS key")?);
        let roots = read(roots, "peer TLS CA roots")?;
        Self::from_pem(&chain, &key, &roots)
    }

    /// [`PeerTls::from_files`] from PEM bytes.
    pub fn from_pem(chain: &[u8], key: &[u8], roots: &[u8]) -> Result<Self, String> {
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(chain)
            .collect::<Result<_, _>>()
            .map_err(|error| format!("decode the peer TLS certificate chain: {error}"))?;
        if chain.is_empty() {
            return Err("the peer TLS certificate chain is empty".to_string());
        }
        let key = PrivateKeyDer::from_pem_slice(key)
            .map_err(|error| format!("decode the peer TLS key: {error}"))?;
        let mut trusted = RootCertStore::empty();
        for root in CertificateDer::pem_slice_iter(roots) {
            let root = root.map_err(|error| format!("decode the peer TLS CA roots: {error}"))?;
            trusted
                .add(root)
                .map_err(|error| format!("a peer TLS CA root is invalid: {error}"))?;
        }
        if trusted.is_empty() {
            return Err("the peer TLS CA roots are empty".to_string());
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let server = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|error| error.to_string())?
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(|error| format!("the peer TLS certificate and key do not match: {error}"))?;
        let client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| error.to_string())?
            .with_root_certificates(trusted)
            .with_no_client_auth();
        Ok(PeerTls {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            connector: TlsConnector::from(Arc::new(client)),
        })
    }

    /// Run the server side of a handshake on an accepted connection.
    pub(crate) async fn accept(&self, socket: TcpStream) -> io::Result<Stream> {
        let stream = self.acceptor.accept(socket).await?;
        Ok(Stream::Server(Box::new(stream)))
    }

    /// Run the client side of a handshake with the member at `addr`,
    /// verifying its certificate covers the address's host.
    pub(crate) async fn connect(&self, addr: &str, socket: TcpStream) -> io::Result<Stream> {
        let name = server_name(addr)?;
        let stream = self.connector.connect(name, socket).await?;
        Ok(Stream::Client(Box::new(stream)))
    }
}

/// The name a member address's certificate must cover: its host, a
/// DNS name or an IP address (IPv6 in brackets).
fn server_name(addr: &str) -> io::Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    ServerName::try_from(host.to_string()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{addr:?} has no host a certificate can name: {error}"),
        )
    })
}

/// One peer connection: TCP, or TLS over it.
pub(crate) enum Stream {
    /// Plaintext TCP.
    Plain(TcpStream),
    /// A dialer's TLS.
    Client(Box<client::TlsStream<TcpStream>>),
    /// A listener's TLS.
    Server(Box<server::TlsStream<TcpStream>>),
}

impl Stream {
    /// Whether an idle connection can no longer carry a request: its
    /// peer closed it (a process that died closes every one at once),
    /// or it holds bytes an idle connection never carries. Reading them
    /// spoils the connection, which is then dropped anyway.
    pub(crate) fn closed(&self) -> bool {
        let tcp = match self {
            Stream::Plain(stream) => stream,
            Stream::Client(stream) => stream.get_ref().0,
            Stream::Server(stream) => stream.get_ref().0,
        };
        !matches!(
            tcp.try_read(&mut [0u8; 1]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock
        )
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Stream::Client(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
            Stream::Server(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Stream::Client(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
            Stream::Server(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Stream::Client(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
            Stream::Server(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Stream::Client(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            Stream::Server(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_addresses_name_their_host() {
        let name = |addr: &str| server_name(addr).map(|name| name.to_str().into_owned());
        assert_eq!(name("db1.example.net:7101").unwrap(), "db1.example.net");
        assert_eq!(name("127.0.0.1:9000").unwrap(), "127.0.0.1");
        assert_eq!(name("[::1]:9000").unwrap(), "::1");
        assert!(name(":9000").is_err());
    }

    #[test]
    fn credentials_must_decode_match_and_name_roots() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let read = |name: &str| std::fs::read(fixtures.join(name)).unwrap();
        let (chain, key, roots) = (read("node.crt"), read("node.key"), read("ca.crt"));
        assert!(PeerTls::from_pem(&chain, &key, &roots).is_ok());
        assert!(
            PeerTls::from_files(
                &fixtures.join("node.crt"),
                &fixtures.join("node.key"),
                &fixtures.join("ca.crt")
            )
            .is_ok()
        );
        let error = PeerTls::from_pem(&chain, &read("wrong.key"), &roots).unwrap_err();
        assert!(error.contains("do not match"), "{error}");
        assert!(
            PeerTls::from_pem(b"", &key, &roots)
                .unwrap_err()
                .contains("empty")
        );
        assert!(
            PeerTls::from_pem(&chain, &key, b"")
                .unwrap_err()
                .contains("empty")
        );
        assert!(
            PeerTls::from_files(
                &fixtures.join("absent.crt"),
                &fixtures.join("node.key"),
                &fixtures.join("ca.crt")
            )
            .is_err()
        );
    }
}
