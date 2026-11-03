//! The API's listener: HTTP/1.1 and HTTP/2 on one port, in-process TLS,
//! and a listening socket inherited from a supervisor.
//!
//! Without TLS the listener serves cleartext HTTP/1.1 and prior-knowledge
//! HTTP/2 (h2c), told apart by the connection preface. With [`ApiTls`]
//! every connection runs server-authenticated TLS 1.2 or 1.3 (rustls with
//! the AWS-LC provider, as the peer transport) and negotiates `h2` or
//! `http/1.1` through ALPN; a client that stays silent, or fails the
//! handshake, is dropped at the handshake deadline without ever reaching
//! the router. There is no plaintext fallback on a TLS listener and no
//! client certificate: callers authenticate with their bearer tokens.
//! Certificates load once at startup.
//!
//! A supervisor that binds the port itself (systemd socket activation, a
//! zero-downtime restarter) hands the listening socket over with
//! `--listen-fd` ([`inherited`]): the member takes ownership of that one
//! descriptor (a socket handle on Windows), checks it is a listening TCP
//! socket bound where the member was told to listen, and closes every
//! other copy it holds, so the address is free again once the member
//! stops.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ServerConfig};

/// How long a TLS handshake may take by default, as v1.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The API's TLS credentials.
#[derive(Clone)]
pub struct ApiTls {
    acceptor: TlsAcceptor,
    handshake_timeout: Duration,
}

impl std::fmt::Debug for ApiTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiTls")
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

impl ApiTls {
    /// Load PEM files: the certificate chain the API presents (its leaf
    /// first, then intermediates) and its private key (PKCS#8, PKCS#1 or
    /// SEC1).
    pub fn from_files(chain: &Path, key: &Path) -> Result<Self, String> {
        let read = |path: &Path, what: &str| {
            std::fs::read(path)
                .map_err(|error| format!("read the {what} {}: {error}", path.display()))
        };
        let chain = read(chain, "API TLS certificate chain")?;
        let key = zeroize::Zeroizing::new(read(key, "API TLS key")?);
        Self::from_pem(&chain, &key)
    }

    /// [`ApiTls::from_files`] from PEM bytes.
    pub fn from_pem(chain: &[u8], key: &[u8]) -> Result<Self, String> {
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(chain)
            .collect::<Result<_, _>>()
            .map_err(|error| format!("decode the API TLS certificate chain: {error}"))?;
        if chain.is_empty() {
            return Err("the API TLS certificate chain is empty".to_string());
        }
        let key = PrivateKeyDer::from_pem_slice(key)
            .map_err(|error| format!("decode the API TLS key: {error}"))?;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| error.to_string())?
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(|error| format!("the API TLS certificate and key do not match: {error}"))?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(ApiTls {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
        })
    }

    /// Drop connections whose handshake takes longer than `timeout`.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }
}

/// How the API listener serves connections.
#[derive(Clone, Debug, Default)]
pub struct HttpOptions {
    /// TLS on every connection, when set.
    pub tls: Option<ApiTls>,
    /// Most concurrent HTTP/2 streams per connection; hyper's default
    /// when unset.
    pub http2_max_streams: Option<u32>,
}

/// Serve `router` on `listener` until `shutdown` resolves, then stop
/// accepting and give open connections `drain` to finish; connections
/// still open after it are dropped. HTTP/1.1 and HTTP/2 alike, over TLS
/// when `options` has it.
pub(crate) async fn run<F>(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    options: &HttpOptions,
    shutdown: F,
    drain: Duration,
) -> io::Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let mut builder = Builder::new(TokioExecutor::new());
    if let Some(streams) = options.http2_max_streams {
        builder.http2().max_concurrent_streams(streams);
    }
    let graceful = GracefulShutdown::new();
    let mut connections = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, address) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        // Out of descriptors or a reset before accept:
                        // back off briefly instead of spinning.
                        tracing::debug!(%error, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                if let Err(error) = stream.set_nodelay(true) {
                    tracing::debug!(%error, "could not disable TCP coalescing");
                }
                let service = TowerToHyperService::new(router.clone());
                let builder = builder.clone();
                let watcher = graceful.watcher();
                match &options.tls {
                    Some(tls) => {
                        let tls = tls.clone();
                        connections.spawn(async move {
                            let stream = match tokio::time::timeout(
                                tls.handshake_timeout,
                                tls.acceptor.accept(stream),
                            )
                            .await
                            {
                                Ok(Ok(stream)) => stream,
                                Ok(Err(error)) => {
                                    tracing::debug!(%error, %address, "TLS handshake refused");
                                    return;
                                }
                                Err(_) => {
                                    tracing::debug!(%address, "TLS handshake deadline passed");
                                    return;
                                }
                            };
                            let connection = builder
                                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                                .into_owned();
                            if let Err(error) = watcher.watch(connection).await {
                                tracing::trace!(%error, %address, "TLS connection ended");
                            }
                        });
                    }
                    None => {
                        let connection = builder
                            .serve_connection_with_upgrades(TokioIo::new(stream), service)
                            .into_owned();
                        let connection = watcher.watch(connection);
                        connections.spawn(async move {
                            if let Err(error) = connection.await {
                                tracing::trace!(%error, %address, "connection ended");
                            }
                        });
                    }
                }
            }
        }
    }
    drop(listener);
    if tokio::time::timeout(drain, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::warn!(
            ?drain,
            "in-flight requests outlived the drain; dropping them"
        );
        connections.abort_all();
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

/// Take ownership of the listening TCP socket a supervisor passed as
/// descriptor (or Windows socket handle) `raw`, which must be bound to
/// `expected`. The descriptor is duplicated first, so a bad number fails
/// with an operating-system error instead of adopting a descriptor
/// nobody holds; then the original is closed, so dropping the listener
/// releases the address. Refuses anything but a TCP stream socket bound
/// to `expected` (and, where the platform tells, listening), closing it.
pub fn inherited(raw: u64, expected: SocketAddr) -> io::Result<std::net::TcpListener> {
    let socket = adopt(raw)?;
    let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidInput, message);
    if socket.r#type()? != socket2::Type::STREAM {
        return Err(invalid(
            "--listen-fd is not a TCP stream socket".to_string(),
        ));
    }
    #[cfg(any(target_os = "android", target_os = "freebsd", target_os = "linux"))]
    {
        if !socket.is_listener()? {
            return Err(invalid("--listen-fd socket is not listening".to_string()));
        }
        if socket.protocol()? != Some(socket2::Protocol::TCP) {
            return Err(invalid("--listen-fd is not a TCP socket".to_string()));
        }
    }
    let address = socket
        .local_addr()?
        .as_socket()
        .ok_or_else(|| invalid("--listen-fd is not an Internet TCP listener".to_string()))?;
    if address != expected {
        return Err(invalid(format!(
            "--listen-fd is bound to {address}, not the listen address {expected}"
        )));
    }
    // Where the platform cannot tell a listening socket, make sure it
    // listens: on one that already does this changes nothing.
    #[cfg(not(any(target_os = "android", target_os = "freebsd", target_os = "linux")))]
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Duplicate the descriptor `raw` and close the original.
#[cfg(unix)]
fn adopt(raw: u64) -> io::Result<socket2::Socket> {
    use std::os::fd::{BorrowedFd, FromRawFd, OwnedFd, RawFd};
    let fd = RawFd::try_from(raw)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "--listen-fd is out of range"))?;
    if fd < 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--listen-fd must not be stdin, stdout or stderr",
        ));
    }
    // SAFETY: the launcher transferred `fd` to this process before any
    // other startup work could reuse its number; the borrow lasts only
    // for the duplication, which fails with EBADF if `fd` is not open.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let duplicate = socket2::SockRef::from(&borrowed).try_clone()?;
    // SAFETY: the duplication just proved `fd` open, and this process
    // owns it: closing it here leaves the duplicate the only copy.
    drop(unsafe { OwnedFd::from_raw_fd(fd) });
    Ok(duplicate)
}

/// Duplicate the socket handle `raw` and close the original.
#[cfg(windows)]
fn adopt(raw: u64) -> io::Result<socket2::Socket> {
    use std::os::windows::io::{BorrowedSocket, FromRawSocket, OwnedSocket, RawSocket};
    let handle = RawSocket::from(raw);
    // SAFETY: the launcher transferred `handle` to this process; the
    // borrow lasts only for the duplication, which fails for a handle
    // that is not an open socket.
    let borrowed = unsafe { BorrowedSocket::borrow_raw(handle) };
    let duplicate = socket2::SockRef::from(&borrowed).try_clone()?;
    // SAFETY: the duplication just proved `handle` an open socket this
    // process owns: closing it leaves the duplicate the only copy.
    drop(unsafe { OwnedSocket::from_raw_socket(handle) });
    Ok(duplicate)
}

#[cfg(not(any(unix, windows)))]
fn adopt(_: u64) -> io::Result<socket2::Socket> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "--listen-fd is not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A listener's raw descriptor or handle, given up.
    fn into_raw(listener: std::net::TcpListener) -> u64 {
        #[cfg(unix)]
        {
            use std::os::fd::IntoRawFd;
            listener.into_raw_fd() as u64
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::IntoRawSocket;
            listener.into_raw_socket()
        }
    }

    #[test]
    fn takes_the_bound_socket_and_closes_every_copy_on_drop() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let inherited = inherited(into_raw(listener), address).unwrap();
        assert_eq!(inherited.local_addr().unwrap(), address);
        assert!(
            std::net::TcpListener::bind(address).is_err(),
            "the address was free while the member held it"
        );
        drop(inherited);
        assert!(
            std::net::TcpListener::bind(address).is_ok(),
            "a copy of the inherited socket leaked"
        );
    }

    #[test]
    fn refuses_bad_numbers_datagrams_and_other_addresses() {
        let address: SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(inherited(u64::from(u32::MAX) - 7, address).is_err());
        let datagram = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        #[cfg(windows)]
        let raw = {
            use std::os::windows::io::IntoRawSocket;
            datagram.into_raw_socket()
        };
        #[cfg(unix)]
        let raw = {
            use std::os::fd::IntoRawFd;
            datagram.into_raw_fd() as u64
        };
        assert!(inherited(raw, address).is_err());
        // A listener bound elsewhere is refused, and closed.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let bound = listener.local_addr().unwrap();
        let error = inherited(into_raw(listener), address).unwrap_err();
        assert!(
            error.to_string().contains("not the listen address"),
            "{error}"
        );
        assert!(
            std::net::TcpListener::bind(bound).is_ok(),
            "a refused socket stayed open"
        );
    }

    #[test]
    fn credentials_must_decode_and_match() {
        let fixtures =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../flower-cluster/tests/fixtures/tls");
        let read = |name: &str| std::fs::read(fixtures.join(name)).unwrap();
        assert!(ApiTls::from_pem(&read("node.crt"), &read("node.key")).is_ok());
        assert!(ApiTls::from_files(&fixtures.join("node.crt"), &fixtures.join("node.key")).is_ok());
        let error = ApiTls::from_pem(&read("node.crt"), &read("wrong.key")).unwrap_err();
        assert!(error.contains("do not match"), "{error}");
        assert!(
            ApiTls::from_pem(b"", &read("node.key"))
                .unwrap_err()
                .contains("empty")
        );
        assert!(
            ApiTls::from_files(&fixtures.join("absent.crt"), &fixtures.join("node.key")).is_err()
        );
    }
}
