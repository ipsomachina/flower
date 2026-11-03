//! The live view over real HTTP: every asset carries the `/admin`
//! dashboard's headers and the right content type, requests naming
//! another host are refused, the event stream streams the state, and
//! nothing it serves names a token.

use std::time::Duration;

use flower_bench::live::{self, CSP, Live, RunFacts};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// One HTTP/1.1 GET with this Host header: status line, headers (lower
/// case) and as much of the body as arrives within a second (the event
/// stream never ends).
async fn get(port: u16, path: &str, host: &str) -> (u16, Vec<(String, String)>, String) {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        match tokio::time::timeout_at(deadline, stream.read(&mut buffer)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(read)) => bytes.extend_from_slice(&buffer[..read]),
            Ok(Err(error)) => panic!("{error}"),
        }
    }
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    (status, headers, body.to_string())
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

#[tokio::test]
async fn the_live_view_serves_bench_data_under_the_admin_posture() {
    let live = Live::new(
        RunFacts {
            id: "run".to_string(),
            preset: Some("demo"),
            describe: "three replicas".to_string(),
            duration_s: 10.0,
            chaos_at_s: Some(5.0),
            max_orders: 32,
            stock_per_store: 138,
            bake_ms: 250,
            read_method: "pizza.shop.local",
        },
        &["tenant-0".to_string()],
    );
    let server = live::serve(live, 0).await.unwrap();
    let port = server.addr.port();
    let host = format!("127.0.0.1:{port}");
    for (path, content_type, _) in live::assets() {
        let (status, headers, _) = get(port, path, &host).await;
        assert_eq!(status, 200, "{path}");
        assert_eq!(
            header(&headers, "content-type"),
            Some(content_type),
            "{path}"
        );
        assert_eq!(
            header(&headers, "content-security-policy"),
            Some(CSP),
            "{path}"
        );
        assert_eq!(header(&headers, "x-content-type-options"), Some("nosniff"));
        assert_eq!(header(&headers, "referrer-policy"), Some("no-referrer"));
        assert_eq!(
            header(&headers, "cross-origin-resource-policy"),
            Some("same-origin")
        );
    }
    // Another name for this port is refused: no DNS rebinding.
    let (status, _, _) = get(port, "/state", "attacker.example").await;
    assert_eq!(status, 421);
    let (status, _, _) = get(port, "/state", &format!("localhost:{port}")).await;
    assert_eq!(status, 200);
    // The state, once and streamed.
    let (status, headers, body) = get(port, "/state", &host).await;
    assert_eq!(status, 200);
    assert_eq!(header(&headers, "content-security-policy"), Some(CSP));
    let state: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(state["run"]["facts"]["id"], "run");
    assert_eq!(state["audit"]["state"], "pending");
    assert!(!body.to_ascii_lowercase().contains("token"));
    let (status, headers, body) = get(port, "/events", &host).await;
    assert_eq!(status, 200);
    assert_eq!(header(&headers, "content-type"), Some("text/event-stream"));
    assert!(body.contains("data: {"), "{body}");
    server.stop();
}
