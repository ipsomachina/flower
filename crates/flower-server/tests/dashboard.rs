//! The admin dashboard at `/admin`: static assets served without a
//! credential under a strict CSP, loading nothing from another origin,
//! calling only endpoints that exist, and absent when turned off.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::wasm::WasmFactory;
use flower_server::api::{AppState, serve};
use flower_server::dashboard::{CSP, PAGE, assets};

/// Serve an initialized single node with `secret` as the admin token,
/// configured further by `configure`; answers its base URL.
async fn serve_node(configure: impl FnOnce(AppState) -> AppState) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(WasmFactory::default()),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let state = configure(AppState::new(Arc::new(node)).with_admin_token("secret".to_string()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    (dir, base)
}

/// A client that never follows redirects, so each answer is the route's.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

/// Every embedded asset's path and body, fetched without a credential.
async fn fetch_assets(base: &str) -> BTreeMap<&'static str, String> {
    let http = client();
    let mut bodies = BTreeMap::new();
    for (path, _) in assets() {
        let body = http
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        bodies.insert(path, body);
    }
    bodies
}

/// The values of every `name="..."` attribute in `html`.
fn attribute_values<'a>(html: &'a str, name: &str) -> Vec<&'a str> {
    let needle = format!(" {name}=\"");
    let mut values = Vec::new();
    let mut rest = html;
    while let Some(start) = rest.find(&needle) {
        let tail = &rest[start + needle.len()..];
        let end = tail.find('"').expect("unterminated attribute");
        values.push(&tail[..end]);
        rest = &tail[end..];
    }
    values
}

/// Every API path a script names in a string literal: `/v1/...` or
/// `/health/...` after a quote or backtick, up to the first character
/// that cannot be part of a path (a query string's `?` included).
fn api_paths(script: &str) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for prefix in ["/v1/", "/health/"] {
        for quote in ['\'', '"', '`'] {
            let needle = format!("{quote}{prefix}");
            let mut rest = script;
            while let Some(start) = rest.find(&needle) {
                let tail = &rest[start + 1..];
                let end = tail
                    .find(|c: char| {
                        !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '/')
                    })
                    .unwrap_or(tail.len());
                // A bare prefix is a check, not an endpoint.
                if end > prefix.len() {
                    paths.insert(tail[..end].to_string());
                }
                rest = &tail[end..];
            }
        }
    }
    paths
}

#[tokio::test]
async fn the_page_and_its_assets_answer_with_their_types_and_security_headers() {
    let (_dir, base) = serve_node(|state| state).await;
    let http = client();
    let mut served = 0;
    for (path, content_type) in assets() {
        // No credential: the assets carry no data.
        let response = http.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(response.status(), 200, "{path}");
        let headers = response.headers().clone();
        assert_eq!(headers["content-type"], content_type, "{path}");
        assert_eq!(headers["content-security-policy"], CSP, "{path}");
        assert_eq!(headers["x-content-type-options"], "nosniff", "{path}");
        assert_eq!(headers["referrer-policy"], "no-referrer", "{path}");
        assert_eq!(
            headers["cross-origin-resource-policy"], "same-origin",
            "{path}"
        );
        assert_eq!(headers["cache-control"], "no-cache", "{path}");
        assert!(!response.bytes().await.unwrap().is_empty(), "{path}");
        // Revalidation answers 304 while the asset is unchanged.
        let etag = headers["etag"].to_str().unwrap().to_string();
        let again = http
            .get(format!("{base}{path}"))
            .header("if-none-match", &etag)
            .send()
            .await
            .unwrap();
        assert_eq!(again.status(), 304, "{path}");
        assert_eq!(again.headers()["etag"], etag.as_str(), "{path}");
        assert_eq!(again.headers()["content-security-policy"], CSP, "{path}");
        served += 1;
    }
    assert!(served >= 4, "the page, its styles and scripts");
    // The CSP is the strict one: scripts, styles and connections to this
    // origin only, nothing inline, no framing.
    for directive in [
        "default-src 'none'",
        "script-src 'self'",
        "style-src 'self'",
        "connect-src 'self'",
        "frame-ancestors 'none'",
        "base-uri 'none'",
        "form-action 'none'",
    ] {
        assert!(CSP.contains(directive), "{directive}");
    }
    assert!(!CSP.contains("unsafe"));
    // A trailing slash goes to the page.
    let redirected = http.get(format!("{base}/admin/")).send().await.unwrap();
    assert!(redirected.status().is_redirection());
    assert_eq!(redirected.headers()["location"], PAGE);
    // Unknown assets are not invented.
    let missing = http
        .get(format!("{base}/admin/nope.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}

#[tokio::test]
async fn data_endpoints_still_refuse_without_the_admin_token() {
    let (_dir, base) = serve_node(|state| state).await;
    let http = client();
    for path in [
        "/v1/metrics",
        "/v1/admin/raft",
        "/v1/admin/activity",
        "/v1/admin/membership",
        "/v1/admin/backup",
    ] {
        let anonymous = http.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(anonymous.status(), 403, "{path}");
        let wrong = http
            .get(format!("{base}{path}"))
            .bearer_auth("not-the-token")
            .send()
            .await
            .unwrap();
        assert_eq!(wrong.status(), 403, "{path}");
        let admin = http
            .get(format!("{base}{path}"))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap();
        assert_eq!(admin.status(), 200, "{path}");
    }
    // Status and readiness stay open, as the page's header needs them.
    for path in ["/v1/status", "/health/ready"] {
        let open = http.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(open.status(), 200, "{path}");
    }
}

#[tokio::test]
async fn the_page_loads_only_same_origin_assets_and_nothing_inline() {
    let (_dir, base) = serve_node(|state| state).await;
    let bodies = fetch_assets(&base).await;
    let html = &bodies[PAGE];
    let served: BTreeSet<&str> = assets().map(|(path, _)| path).collect();
    let sources = attribute_values(html, "src");
    let links = attribute_values(html, "href");
    assert!(!sources.is_empty() && !links.is_empty());
    for reference in sources.iter().chain(&links) {
        // In-page anchors, or an asset this member embeds.
        if reference.starts_with('#') {
            continue;
        }
        assert!(
            served.contains(reference),
            "{reference} is not an embedded same-origin asset"
        );
    }
    // Nothing inline: every script has a source, no style element or
    // attribute, no inline handler.
    let lower = html.to_ascii_lowercase();
    assert_eq!(
        lower.matches("<script").count(),
        lower
            .matches("<script type=\"module\" src=\"/admin/")
            .count()
    );
    assert!(!lower.contains("<style"));
    assert!(!lower.contains(" style="));
    for handler in [
        " onclick=",
        " onload=",
        " onerror=",
        " onsubmit=",
        "javascript:",
    ] {
        assert!(!lower.contains(handler), "{handler}");
    }
    // Scripts import only sibling modules this member serves, and no
    // asset names another origin (the SVG namespace is a name, not a
    // load).
    for (path, body) in &bodies {
        let scrubbed = body.replace("http://www.w3.org/2000/svg", "");
        assert!(
            !scrubbed.contains("http://")
                && !scrubbed.contains("https://")
                && !scrubbed.contains("//cdn"),
            "{path} names another origin"
        );
        if path.ends_with(".js") {
            for line in body.lines().filter(|line| line.contains(" from '")) {
                let module = line
                    .split(" from '")
                    .nth(1)
                    .unwrap()
                    .split('\'')
                    .next()
                    .unwrap();
                let module = module.strip_prefix("./").expect("relative import");
                assert!(
                    served.contains(format!("/admin/{module}").as_str()),
                    "{path} imports {module}"
                );
            }
            // Server strings go in as text, never parsed as markup or
            // code; the stream is read with fetch, as EventSource
            // cannot send the token.
            for forbidden in [
                ".innerHTML",
                ".outerHTML",
                "insertAdjacentHTML",
                "document.write",
                "eval(",
                "new Function",
                "new EventSource",
            ] {
                assert!(!body.contains(forbidden), "{path} uses {forbidden}");
            }
        }
        if path.ends_with(".css") {
            assert!(
                !body.contains("@import") && !body.contains("url("),
                "{path} loads more"
            );
        }
    }
}

#[tokio::test]
async fn every_endpoint_the_dashboard_calls_routes() {
    let (_dir, base) = serve_node(|state| state).await;
    let bodies = fetch_assets(&base).await;
    let mut paths = BTreeSet::new();
    for (path, body) in &bodies {
        if path.ends_with(".js") {
            paths.extend(api_paths(body));
        }
    }
    // The extraction found what the page is built on, so a broken
    // extractor cannot pass by finding nothing.
    for core in [
        "/v1/status",
        "/health/ready",
        "/v1/metrics",
        "/v1/admin/raft",
        "/v1/admin/activity/stream",
        "/v1/admin/leadership",
        "/v1/admin/deployments",
        "/v1/admin/stream",
    ] {
        assert!(paths.contains(core), "{core} missing from {paths:?}");
    }
    assert!(paths.len() >= 12, "{paths:?}");
    // The admin stream's events the page handles are the ones the
    // server sends: renaming one fails here too.
    let api = &bodies["/admin/api.js"];
    let list = api
        .split("STREAM_EVENTS = Object.freeze([")
        .nth(1)
        .and_then(|rest| rest.split("])").next())
        .expect("api.js lists the stream's events");
    let consumed: BTreeSet<&str> = list
        .split(',')
        .map(|name| name.trim().trim_matches('\''))
        .filter(|name| !name.is_empty())
        .collect();
    let sent: BTreeSet<&str> = flower_server::admin_stream::EVENTS.into_iter().collect();
    assert_eq!(consumed, sent);
    // Each routes: any answer but 404 (a POST-only route answers 405 to
    // GET, a missing parameter 400). Renaming an endpoint fails here.
    let http = client();
    for path in &paths {
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            http.get(format!("{base}{path}"))
                .bearer_auth("secret")
                .send(),
        )
        .await
        .unwrap_or_else(|_| panic!("{path} timed out"))
        .unwrap();
        assert_ne!(response.status(), 404, "{path} does not route");
    }
    // The guard itself tells a missing route apart.
    let missing = http
        .get(format!("{base}/v1/admin/no-such-endpoint"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}

#[tokio::test]
async fn turning_the_dashboard_off_leaves_it_out() {
    let (_dir, base) = serve_node(|state| state.with_admin_ui(false)).await;
    let http = client();
    for (path, _) in assets() {
        let response = http.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(response.status(), 404, "{path}");
    }
    assert_eq!(
        http.get(format!("{base}/admin/"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    // The API itself is unchanged.
    let status = http.get(format!("{base}/v1/status")).send().await.unwrap();
    assert_eq!(status.status(), 200);
}
