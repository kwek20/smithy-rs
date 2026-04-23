/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::mpsc;

use tls_listener::{AsyncAccept, AsyncTls, Error as TlsListenerError, TlsListener};

/// A wrapper around [`TlsListener`] that allows changing TLS config via a channel
/// and ignores incorrect connections so the server keeps accepting new ones.
pub struct Listener<A: AsyncAccept, T: AsyncTls<A::Connection>> {
    inner: TlsListener<A, T>,
    new_acceptor_rx: mpsc::Receiver<T>,
}

impl<A: AsyncAccept + Unpin, T: AsyncTls<A::Connection>> Listener<A, T> {
    pub fn new(tls: T, listener: A, new_acceptor_rx: mpsc::Receiver<T>) -> Self {
        Self {
            inner: TlsListener::new(tls, listener),
            new_acceptor_rx,
        }
    }

    pub fn replace_acceptor(&mut self, acceptor: T) {
        self.inner.replace_acceptor(acceptor);
    }

    pub async fn accept(
        &mut self,
    ) -> Result<
        (T::Stream, A::Address),
        TlsListenerError<A::Error, <T as AsyncTls<A::Connection>>::Error, A::Address>,
    >
    where
        A: AsyncAccept,
        T: AsyncTls<A::Connection>,
    {
        loop {
            while let Ok(acceptor) = self.new_acceptor_rx.try_recv() {
                self.inner.replace_acceptor(acceptor);
            }

            match self.inner.accept().await {
                Ok(conn) => return Ok(conn),
                Err(TlsListenerError::TlsAcceptError {
                    error,
                    peer_addr,
                    ..
                }) => {
                    tracing::debug!(error = ?error, ?peer_addr, "tls handshake error");
                }
                Err(err) => return Err(err),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::io;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::{mpsc, Arc};
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::Duration;

    use bytes::Bytes;
    use hyper::{
        body::Incoming,
        Request, Response, Uri,
    };
    use hyper_rustls::HttpsConnectorBuilder;
    use hyper_util::{
        client::legacy::Client,
        rt::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ConnBuilder,
    };
    use http_body_util::{BodyExt, Empty, Full};
    use pin_project_lite::pin_project;
    use tls_listener::AsyncAccept;
    use tokio::net::TcpListener;
    use tokio_rustls::{
        rustls::{ClientConfig, RootCertStore, ServerConfig},
        rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
        TlsAcceptor,
    };
    use hyper::service::service_fn;

    use crate::util::collect_hyper_body;

    use super::Listener;

    enum DummyListenerMode {
        Identity,
        Fail,
    }

    pin_project! {
        struct DummyListener {
            #[pin]
            inner: TcpListener,
            mode: DummyListenerMode,
        }
    }

    impl AsyncAccept for DummyListener {
        type Connection = tokio::net::TcpStream;
        type Error = io::Error;
        type Address = SocketAddr;

        fn poll_accept(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(Self::Connection, Self::Address), Self::Error>> {
            let this = self.project();
            let conn = match this.inner.poll_accept(cx) {
                Poll::Ready(Ok((conn, addr))) => (conn, addr),
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            };

            match &this.mode {
                DummyListenerMode::Identity => Poll::Ready(Ok(conn)),
                DummyListenerMode::Fail => {
                    Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()))
                }
            }
        }
    }

    #[tokio::test]
    async fn server_doesnt_shutdown_after_bad_handshake() {
        let (_new_acceptor_tx, new_acceptor_rx) = mpsc::channel();
        let cert = valid_cert();
        let acceptor = acceptor_from_cert(&cert);
        let (addr, _) = server(acceptor, new_acceptor_rx, DummyListenerMode::Identity);

        {
            let different_cert = valid_cert();
            let config = client_config_with_cert(&different_cert);
            let response = make_req(config, &addr).await;
            assert!(response.is_err());
        }

        {
            let config = client_config_with_cert(&cert);
            let response = make_req(config, &addr).await.unwrap();
            assert_eq!(
                "hello world",
                collect_hyper_body(response.into_body()).await.unwrap()
            );
        }
    }

    #[tokio::test]
    #[should_panic(expected = "server error: connection aborted")]
    async fn server_shutdown_after_listener_error() {
        let (_new_acceptor_tx, new_acceptor_rx) = mpsc::channel();
        let cert = valid_cert();
        let acceptor = acceptor_from_cert(&cert);
        let (addr, server_thread_handle) =
            server(acceptor, new_acceptor_rx, DummyListenerMode::Fail);

        let config = client_config_with_cert(&cert);
        let _ = make_req(config, &addr).await;

        std::panic::resume_unwind(server_thread_handle.join().unwrap_err());
    }

    #[tokio::test]
    async fn server_changes_tls_config() {
        let (new_acceptor_tx, new_acceptor_rx) = mpsc::channel();

        let invalid_cert = cert_with_invalid_date();
        let acceptor = acceptor_from_cert(&invalid_cert);
        let (addr, _) = server(acceptor, new_acceptor_rx, DummyListenerMode::Identity);

        {
            let config = client_config_with_cert(&invalid_cert);
            let response = make_req(config, &addr).await;
            assert!(response.is_err());
        }

        let cert = valid_cert();
        let acceptor = acceptor_from_cert(&cert);
        new_acceptor_tx.send(acceptor).unwrap();

        {
            let response = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let config = client_config_with_cert(&cert);
                    match make_req(config, &addr).await {
                        Ok(response) => break response,
                        Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                }
            })
            .await
            .expect("timed out waiting for tls config reload");
            assert_eq!(
                "hello world",
                collect_hyper_body(response.into_body()).await.unwrap()
            );
        }
    }

    fn client_config_with_cert(cert: &rcgen::Certificate) -> ClientConfig {
        use tokio_rustls::rustls::pki_types::CertificateDer;

        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates([CertificateDer::from(cert.serialize_der().unwrap())]);
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    }

    fn cert_with_invalid_date() -> rcgen::Certificate {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]);
        params.not_after = rcgen::date_time_ymd(1970, 1, 1);
        rcgen::Certificate::from_params(params).unwrap()
    }

    fn valid_cert() -> rcgen::Certificate {
        let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]);
        rcgen::Certificate::from_params(params).unwrap()
    }

    fn acceptor_from_cert(cert: &rcgen::Certificate) -> TlsAcceptor {
        TlsAcceptor::from(Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.serialize_der().unwrap().into()],
                    PrivateKeyDer::from(PrivatePkcs8KeyDer::from(
                        cert.serialize_private_key_der(),
                    )),
                )
                .unwrap(),
        ))
    }

    fn server(
        acceptor: TlsAcceptor,
        new_acceptor_rx: mpsc::Receiver<TlsAcceptor>,
        dummy_listener_mode: DummyListenerMode,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let (addr_tx, addr_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            tokio_test::block_on(async move {
                let inner = TcpListener::bind(addr).await.unwrap();
                addr_tx.send(inner.local_addr().unwrap()).unwrap();

                let incoming = DummyListener {
                    inner,
                    mode: dummy_listener_mode,
                };
                let mut listener = Listener::new(acceptor, incoming, new_acceptor_rx);
                let http = ConnBuilder::new(TokioExecutor::new());

                loop {
                    match listener.accept().await {
                        Ok((conn, _addr)) => {
                            let http = http.clone();
                            tokio::spawn(async move {
                                let svc = service_fn(|_req: Request<Incoming>| async move {
                                    Ok::<_, Infallible>(
                                        Response::new(Full::new(Bytes::from_static(
                                            b"hello world",
                                        ))
                                        .boxed()),
                                    )
                                });

                                if let Err(err) =
                                    http.serve_connection(TokioIo::new(conn), svc).await
                                {
                                    panic!("server error: {err}");
                                }
                            });
                        }
                        Err(err) => panic!("server error: {err}"),
                    }
                }
            });
        });

        (addr_rx.recv().unwrap(), handle)
    }

    async fn make_req(
        config: ClientConfig,
        addr: &SocketAddr,
    ) -> Result<Response<Incoming>, hyper_util::client::legacy::Error> {
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_only()
            .enable_http1()
            .enable_http2()
            .build();

        let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build(connector);
        client
            .get(
                Uri::builder()
                    .scheme("https")
                    .authority(format!("localhost:{}", addr.port()))
                    .path_and_query("/")
                    .build()
                    .unwrap(),
            )
            .await
    }
}
