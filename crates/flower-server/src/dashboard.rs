//! The admin and observability dashboard: `GET /admin` and its assets
//! under `/admin/`, on the API port.
//!
//! The page is hand-written HTML, CSS and JavaScript modules embedded
//! in the binary: no build step, and nothing loads from anywhere but
//! this member, so it works on an air-gapped cluster. Charts are
//! inline SVG.
//!
//! The page follows one connection, the admin stream
//! ([`crate::admin_stream`]): a snapshot of everything it shows, then
//! Raft transitions, readiness, catalog changes, metrics samples stamped
//! with the member's clock, backup, timer and queue counts and activity
//! as they happen. Charts take their rates from those stamps and mark
//! every leader and term change, the ones a reconnect recovered too.
//! Only when the stream is refused (403, 429) or sends no snapshot
//! within a few seconds (a buffering proxy) does the page poll the
//! `/v1` endpoints instead, saying so in its header, and it tries the
//! stream again. Actions stay POSTs; the stream then shows what they
//! changed. `serve --admin-ui false` (`FLOWER_ADMIN_UI`) leaves the
//! page's routes out: they answer 404.
//!
//! # Security model
//!
//! - **The assets hold no data**, so they need no credential. Every
//!   number the page shows comes from `/v1` endpoints (the admin stream
//!   among them), which keep their own credential checks: the dashboard
//!   adds no authority.
//! - **Strict CSP.** Every asset answers with [`CSP`]: scripts, styles
//!   and connections to this origin only, no inline script or style, no
//!   framing, no `<base>`, no form submission. Plus `nosniff`, `no-referrer`
//!   and `Cross-Origin-Resource-Policy: same-origin`.
//! - **The admin token** is asked for by the page, kept in the tab's
//!   `sessionStorage` only, and sent only as an `Authorization: Bearer`
//!   header to same-origin `/v1` paths, never in a URL. A 403 asks for
//!   it again; "forget token" drops it. The streams are read with
//!   `fetch` and a stream reader, since `EventSource` cannot send the
//!   header.
//! - **Server strings** (database, module and key names, error text)
//!   are rendered through `textContent` and DOM APIs, never parsed as
//!   markup.
//!
//! Assets revalidate on every load (`no-cache` with an `ETag`), so an
//! upgraded member never serves a page against older scripts.

use std::sync::LazyLock;

use axum::Router;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;

use crate::api::AppState;

/// The Content-Security-Policy every dashboard response carries.
pub const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
                       connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; \
                       base-uri 'none'; form-action 'none'";

/// Where the page itself is served.
pub const PAGE: &str = "/admin";

/// One embedded file.
struct Asset {
    /// Request path.
    path: &'static str,
    /// `Content-Type` it answers with.
    content_type: &'static str,
    /// The bytes.
    body: &'static [u8],
}

/// The page and every file it loads.
const ASSETS: [Asset; 9] = [
    Asset {
        path: PAGE,
        content_type: "text/html; charset=utf-8",
        body: include_bytes!("dashboard/index.html"),
    },
    Asset {
        path: "/admin/style.css",
        content_type: "text/css; charset=utf-8",
        body: include_bytes!("dashboard/style.css"),
    },
    Asset {
        path: "/admin/app.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("dashboard/app.js"),
    },
    Asset {
        path: "/admin/api.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("dashboard/api.js"),
    },
    Asset {
        path: "/admin/charts.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("dashboard/charts.js"),
    },
    Asset {
        path: "/admin/dom.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("dashboard/dom.js"),
    },
    Asset {
        path: "/admin/format.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("dashboard/format.js"),
    },
    Asset {
        path: "/admin/panels.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("dashboard/panels.js"),
    },
    Asset {
        path: "/admin/favicon.svg",
        content_type: "image/svg+xml",
        body: include_bytes!("dashboard/favicon.svg"),
    },
];

/// Every dashboard path with its content type, the page first.
pub fn assets() -> impl Iterator<Item = (&'static str, &'static str)> {
    ASSETS.iter().map(|asset| (asset.path, asset.content_type))
}

/// Each asset's entity tag: FNV-1a over its bytes, computed once.
static ETAGS: LazyLock<Vec<HeaderValue>> = LazyLock::new(|| {
    ASSETS
        .iter()
        .map(|asset| {
            let hash = asset
                .body
                .iter()
                .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                    (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
                });
            HeaderValue::from_str(&format!("\"{hash:016x}\"")).expect("hex is a header value")
        })
        .collect()
});

/// The dashboard routes: the page, its assets, and `/admin/` sent to
/// the page.
pub(crate) fn routes() -> Router<AppState> {
    ASSETS.iter().enumerate().fold(
        Router::new().route("/admin/", get(|| async { Redirect::permanent(PAGE) })),
        |router, (index, asset)| {
            router.route(
                asset.path,
                get(move |headers: HeaderMap| async move { serve(index, &headers) }),
            )
        },
    )
}

/// Answer one asset, or 304 when the client holds it already.
fn serve(index: usize, headers: &HeaderMap) -> Response {
    let asset = &ASSETS[index];
    let etag = &ETAGS[index];
    let fresh = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|tags| {
            tags.split(',')
                .any(|tag| tag.trim() == "*" || tag.trim().as_bytes() == etag.as_bytes())
        });
    let mut response = if fresh {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static(asset.content_type),
            )],
            asset.body,
        )
            .into_response()
    };
    let headers = response.headers_mut();
    headers.insert(header::ETAG, etag.clone());
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("same-origin"),
    );
    response
}
