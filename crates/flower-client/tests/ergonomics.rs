extern crate alloc;
use flower_client::{
    testing::{MockReply, MockTransport},
    watch::SubscribeOptions,
    *,
};
use flower_sdk::{FlowerType, MutationOp, QueryOp};
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[derive(FlowerType)]
struct Args {
    key: String,
}
const PUT: MutationOp<Args, String> = MutationOp::new("put");
const GET: QueryOp<Args, String> = QueryOp::new("get");
fn identity() -> RetryIdentity {
    RetryIdentity {
        incarnation: 9,
        epoch: 2,
        min_epoch: 1,
        epoch_ms: None,
        initialized: true,
    }
}

#[tokio::test]
async fn persisted_commands_reuse_their_identity_after_a_lost_response() {
    let mock = MockTransport::scripted([MockReply::hang(), MockReply::ok(json!("done"))]);
    let scope = mock.client().database("db").module("app");
    let command = scope
        .prepare_with_identity(&PUT, &Args { key: "a".into() }, &identity())
        .unwrap();
    let encoded = serde_json::to_vec(&command).unwrap();
    let failure = scope
        .execute(
            &command,
            &CommandOptions::new().timeout(Duration::from_millis(5)),
        )
        .await
        .unwrap_err();
    assert_eq!(failure.request_id, command.request_id());
    assert_eq!(failure.execution, Execution::Unknown);
    let restored: PreparedCommand<String> = serde_json::from_slice(&encoded).unwrap();
    let result = scope
        .execute(&restored, &CommandOptions::new())
        .await
        .unwrap();
    assert_eq!(result.value, "done");
    assert_eq!(result.request_id, command.request_id());
    let requests = mock.requests();
    assert_eq!(
        requests[0].json()["idempotency_key"],
        requests[1].json()["idempotency_key"]
    );
    assert!(command.request_id().starts_with("f1:0000000000000009:2:"));
}

#[tokio::test]
async fn cancelled_before_sending_is_distinguished_from_an_unknown_outcome() {
    let mock = MockTransport::scripted([]);
    let scope = mock.client().database("db").module("app");
    let command = scope
        .prepare_with_identity(&PUT, &Args { key: "a".into() }, &identity())
        .unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = scope
        .execute(&command, &CommandOptions::new().cancel(cancel))
        .await
        .unwrap_err();
    assert_eq!(error.execution, Execution::NotStarted);
    assert!(mock.requests().is_empty());
}

#[tokio::test]
async fn malformed_successes_are_rejected_while_explicit_null_is_valid() {
    let mock = MockTransport::scripted([
        MockReply::json(200, &json!({"ok":true})),
        MockReply::json(200, &json!({"ok":true,"value":null})),
        MockReply::json(200, &json!({"ok":true,"value":null})),
        MockReply::json(200, &json!({"ok":true,"value":null})),
    ]);
    let client = mock.client();
    let options = RequestOptions::new();
    assert!(matches!(
        client
            .query_value("db", "app", "get", &json!({}), &options)
            .await,
        Err(FlowerError::Decode(_))
    ));
    let null = client
        .query_value("db", "app", "get", &json!({}), &options)
        .await
        .unwrap();
    assert!(null.value.is_null());
    assert_eq!(null.staleness, Some(Staleness::Unknown));
    assert!(matches!(
        client
            .mutate_value("db", "app", "put", &json!({}), &MutationOptions::new())
            .await,
        Err(FlowerError::Decode(_))
    ));
    assert!(matches!(
        client
            .deploy("db", "app", None, &[], &json!({}), &options)
            .await,
        Err(FlowerError::Decode(_))
    ));
}

#[tokio::test]
async fn retries_preserve_structured_causes_and_prior_uncertainty() {
    let mock = MockTransport::scripted([
        MockReply::error(502, "upstream", "lost reply"),
        MockReply::overloaded(Some(0)),
    ]);
    let retry = RetryPolicy {
        attempts: 2,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        through_overload: false,
        ..RetryPolicy::default()
    };
    let error = mock
        .client()
        .query_value(
            "db",
            "app",
            "get",
            &json!({}),
            &RequestOptions::new().retry(retry),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FlowerError::RetryFailed { attempts: 2, .. }
    ));
    assert_eq!(error.problem().unwrap().code, "overloaded");
    assert_eq!(error.execution(), Execution::Unknown);
    let auth = MockTransport::scripted([MockReply::error(403, "ACCESS_DENIED", "tenant mismatch")]);
    let error = auth
        .client()
        .query_value("db", "app", "get", &json!({}), &RequestOptions::new())
        .await
        .unwrap_err();
    assert!(error.is_auth());
    assert_eq!(error.problem().unwrap().message, "tenant mismatch");
}

fn token() -> ReadToken {
    ReadToken {
        version: 1,
        database: "db".into(),
        group: "root".into(),
        incarnation: Incarnation(9),
        revision: Revision(7),
        position: LogPosition(12),
    }
}
#[tokio::test]
async fn causal_queries_require_an_acknowledged_token_and_preserve_it() {
    let mut reply = MockReply::ok(json!("value"));
    reply.headers.push((
        flower_protocol::TOKEN_HEADER.into(),
        token().encode().unwrap(),
    ));
    let mock = MockTransport::scripted([reply, MockReply::ok(json!("value"))]);
    let scope = mock.client().database("db").module("app");
    let args = Args { key: "a".into() };
    let read = scope.query(&GET, &args).at_least(token()).await.unwrap();
    assert_eq!(read.read_token, Some(token()));
    assert!(
        mock.requests()[0]
            .header(flower_protocol::AT_LEAST_HEADER)
            .is_some()
    );
    let error = scope
        .query(&GET, &args)
        .at_least(token())
        .await
        .unwrap_err();
    assert!(matches!(error.source(), FlowerError::Unsupported(_)));
}

#[tokio::test]
async fn monotonic_watches_reset_when_history_changes_and_surface_terminal_errors() {
    let first = "event: snapshot\ndata: {\"revision\":100,\"history\":{\"group\":\"root\",\"incarnation\":\"9\"},\"value\":\"old\"}\n\n";
    let second = "event: snapshot\ndata: {\"revision\":1,\"history\":{\"group\":\"root\",\"incarnation\":\"10\"},\"value\":\"new\"}\n\nevent: error\ndata: {\"ok\":false,\"code\":\"denied\",\"message\":\"revoked\",\"problem\":{\"status\":403,\"code\":\"denied\",\"message\":\"revoked\",\"origin\":\"authorization\",\"execution\":\"not_started\",\"retryable\":false}}\n\n";
    let mock = MockTransport::scripted([MockReply::events(first), MockReply::events(second)]);
    let options = SubscribeOptions {
        monotonic: true,
        max_reconnects: Some(2),
        ..SubscribeOptions::default()
    };
    let mut subscription =
        mock.client()
            .subscribe::<String>("db", "app", "get", &json!({}), &options);
    assert_eq!(subscription.next().await.unwrap().unwrap().revision, 100);
    let reset = subscription.next().await.unwrap().unwrap();
    assert_eq!(reset.value, "new");
    assert!(reset.reset);
    let error = subscription.next().await.unwrap().unwrap_err();
    assert!(error.is_auth());
    assert!(subscription.next().await.is_none());
}

#[tokio::test]
async fn typed_transactions_preserve_the_result_tuple_and_validate_participants() {
    let reply = MockReply::json(
        200,
        &json!({"ok":true,"value":["a",7],"revision":1,"position":2,"revisions":{"one":1,"two":1}}),
    );
    let mock = MockTransport::scripted([
        reply,
        MockReply::json(
            200,
            &json!({"ok":true,"value":[7,7],"revision":1,"position":2,"revisions":{"one":1,"two":1}}),
        ),
    ]);
    let other: MutationOp<Args, u64> = MutationOp::new("count");
    let args = Args { key: "a".into() };
    let tx = TypedTransaction::new()
        .step("one", "app", &PUT, &args)
        .unwrap()
        .step("two", "app", &other, &args)
        .unwrap();
    let encoded = serde_json::to_vec(&tx).unwrap();
    let restored: TypedTransaction<(String, u64)> = serde_json::from_slice(&encoded).unwrap();
    let result = restored
        .execute(&mock.client(), &CommandOptions::new())
        .await
        .unwrap();
    assert_eq!(result.values, ("a".into(), 7));
    assert_eq!(result.commit.request_id, tx.request_id());
    let error = restored
        .execute(&mock.client(), &CommandOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.execution, Execution::Decided);
    assert!(matches!(error.source.as_ref(), FlowerError::Decode(_)));
    assert!(
        TypedTransaction::new()
            .step("one", "app", &PUT, &args)
            .unwrap()
            .step("one", "app", &other, &args)
            .is_err()
    );
}

/// The old headerless transport must fail before discarding a consistency constraint.
struct Headerless;
#[async_trait::async_trait]
impl Transport for Headerless {
    async fn post(
        &self,
        _: &str,
        _: &[u8],
        _: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        panic!("constraint was discarded")
    }
    async fn get(
        &self,
        _: &str,
        _: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        panic!("constraint was discarded")
    }
    async fn post_stream(
        &self,
        _: &str,
        _: &[u8],
        _: Option<&Credentials>,
    ) -> Result<transport::StreamResponse, FlowerError> {
        unreachable!()
    }
}
#[tokio::test]
async fn headerless_transports_cannot_silently_drop_staleness_bounds() {
    let client = FlowerClient::builder("http://unused")
        .transport(Arc::new(Headerless))
        .build()
        .unwrap();
    let error = client
        .query_value(
            "db",
            "app",
            "get",
            &json!({}),
            &RequestOptions::new().max_staleness(Duration::ZERO),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Unsupported(_)));
}

#[tokio::test]
async fn typed_lease_operations_keep_the_queue_and_secret_bound_to_the_claim() {
    let mock = MockTransport::scripted([
        MockReply::json(
            200,
            &json!({"ok":true,"items":[{"id":"job","token":"secret","payload":"invoice","attempts":1,"fence":7}]}),
        ),
        MockReply::json(200, &json!({"ok":true})),
        MockReply::json(200, &json!({"ok":true})),
    ]);
    let queue = flower_sdk::queue::TypedQueue::<String, String>::new("mail");
    let claim = mock
        .client()
        .claim_typed("db", &queue, &QueuePoll::default(), &RequestOptions::new())
        .await
        .unwrap()
        .pop()
        .unwrap()
        .unwrap();
    assert_eq!(claim.payload, "invoice");
    assert_eq!(claim.lease.fence(), 7);
    claim
        .lease
        .heartbeat(Duration::from_secs(12), &RequestOptions::new())
        .await
        .unwrap();
    claim
        .lease
        .complete_with(&"sent".to_string(), &RequestOptions::new())
        .await
        .unwrap();
    let requests = mock.requests();
    assert_eq!(requests[1].json()["lease_ms"], 12000);
    assert_eq!(requests[2].json()["result"], "sent");
    for request in &requests[1..] {
        assert_eq!(request.json()["db"], "db");
        assert_eq!(request.json()["queue"], "mail");
        assert_eq!(request.json()["id"], "job");
        assert_eq!(request.json()["token"], "secret");
    }
}

#[tokio::test]
async fn cancelling_a_watch_closes_its_stream_and_ends_after_one_error() {
    let (reply, handle) =
        MockReply::event_stream("event: snapshot\ndata: {\"revision\":1,\"value\":\"one\"}\n\n");
    let mock = MockTransport::scripted([reply]);
    let cancel = CancellationToken::new();
    let options = watch::WatchOptions {
        cancel: Some(cancel.clone()),
        ..watch::WatchOptions::default()
    };
    let mut watch = mock
        .client()
        .watch::<String>("db", "app", "get", &json!({}), &options)
        .await
        .unwrap();
    assert_eq!(watch.next().await.unwrap().unwrap().value, "one");
    cancel.cancel();
    assert!(matches!(
        watch.next().await,
        Some(Err(FlowerError::Cancelled))
    ));
    assert!(!handle.open());
    assert!(watch.next().await.is_none());
}
