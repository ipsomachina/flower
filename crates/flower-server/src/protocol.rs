//! Protocol negotiation and uniform error extensions, including queue refusals.
use axum::{
    body::{Body, to_bytes},
    extract::Request,
    middleware::Next,
    response::Response,
};

pub async fn decorate(request: Request, next: Next) -> Response {
    let operation = request.uri().path().to_owned();
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "x-flower-protocol-version",
        axum::http::HeaderValue::from_static("1"),
    );
    let status = response.status().as_u16();
    let inspect = status >= 400
        || operation.starts_with("/v1/queue/")
        || operation.starts_with("/v1/timer/")
        || operation.starts_with("/v1/retry/session");
    if !inspect {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    // Error bodies are small; do not buffer successful watches, sync snapshots or metrics.
    let bytes = match to_bytes(body, 2 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(error) => {
            parts.status = axum::http::StatusCode::INTERNAL_SERVER_ERROR;
            parts.headers.remove(axum::http::header::CONTENT_LENGTH);
            parts.headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            let body = serde_json::json!({"ok":false,"code":"invalid_error_response","message":error.to_string()});
            return Response::from_parts(parts, Body::from(body.to_string()));
        }
    };
    let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Response::from_parts(parts, Body::from(bytes));
    };
    if json.get("ok").and_then(serde_json::Value::as_bool) != Some(false) {
        return Response::from_parts(parts, Body::from(bytes));
    }
    let mut problem = flower_protocol::Problem::from_body(status, &bytes, None);
    if status == 200 {
        problem.origin = flower_protocol::Origin::Application;
        problem.execution = flower_protocol::Execution::Decided;
        problem.retryable = problem.code == "queue_purging";
        if problem.retryable {
            problem.execution = flower_protocol::Execution::NotStarted;
            problem.origin = flower_protocol::Origin::Host;
        }
    }
    json["problem"] = serde_json::to_value(problem).expect("problem serializes");
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(json.to_string()))
}
