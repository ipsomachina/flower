use flower_client::transport::StreamResponse;
use flower_client::{
    Credentials, FlowerError, HttpTransport, Transport, TransportOptions, TransportResponse,
    testing::conformance::ProbeServer,
};

#[tokio::test]
async fn built_in_transports_pass_the_same_suite_over_http1_and_h2c() {
    for options in [TransportOptions::proxy(), TransportOptions::direct()] {
        let server = ProbeServer::start().await.unwrap();
        let transport = HttpTransport::with_options(server.base_url(), options).unwrap();
        server.check(&transport).await.assert_passed();
        server.shutdown().await;
    }
}

struct Headerless(HttpTransport);
#[async_trait::async_trait]
impl Transport for Headerless {
    async fn post(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        self.0.post(path, body, credentials).await
    }
    async fn get(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        self.0.get(path, credentials).await
    }
    async fn post_stream(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<StreamResponse, FlowerError> {
        self.0.post_stream(path, body, credentials).await
    }
}
#[tokio::test]
async fn a_custom_transport_losing_headers_gets_actionable_independent_failures() {
    let server = ProbeServer::start().await.unwrap();
    let transport = Headerless(
        HttpTransport::with_options(server.base_url(), TransportOptions::proxy()).unwrap(),
    );
    let report = server.check(&transport).await;
    assert!(!report.passed());
    assert!(!report.checks[0].passed);
    assert!(!report.checks[1].passed);
    assert!(report.checks[0].detail.as_ref().unwrap().contains("header"));
    assert!(report.checks[2].passed);
    assert!(report.checks[3].passed);
    server.shutdown().await;
}
