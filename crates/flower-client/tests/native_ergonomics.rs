extern crate alloc;

use flower_client::{
    testing::{MockReply, MockTransport},
    watch::{SubscribeOptions, WatchOptions},
    *,
};
use flower_sdk::{FlowerError as DeclaredError, MutationCtx, QueryCtx, interface};
use serde_json::json;
use std::time::Duration;

#[derive(DeclaredError, Debug, PartialEq, Eq)]
pub enum TaskError {
    #[flower(code = "TASK_LOCKED")]
    Locked(String),
}
#[derive(DeclaredError, Debug, PartialEq, Eq)]
pub enum CountError {
    #[flower(code = "TASK_LOCKED")]
    Locked(u64),
}

#[interface(name = "tasks.v1", handle = TasksApi, client = TasksClient)]
pub trait Tasks {
    #[query]
    fn get_task(ctx: &mut QueryCtx<'_, '_>, key: String) -> Result<String, TaskError>;
    #[mutation]
    fn put_task(ctx: &mut MutationCtx<'_, '_>, key: String) -> Result<String, TaskError>;
}

fn queue() -> flower_sdk::queue::TypedQueue<String, String> {
    flower_sdk::queue::TypedQueue::new("jobs")
}
fn claim(id: &str, payload: serde_json::Value) -> serde_json::Value {
    json!({"id":id,"token":format!("secret-{id}"),"payload":payload,"attempts":1,"fence":7,"expires_ms":123})
}

#[tokio::test]
async fn mixed_claim_batches_preserve_every_lease_and_the_bad_payload() {
    let mock = MockTransport::scripted([
        MockReply::json(
            200,
            &json!({"ok":true,"items":[claim("one",json!("a")),claim("bad",json!({"old":true})),claim("two",json!("b"))]}),
        ),
        MockReply::json(200, &json!({"ok":true})),
        MockReply::json(200, &json!({"ok":true})),
        MockReply::json(200, &json!({"ok":true})),
    ]);
    let options = RequestOptions::new();
    let items = mock
        .client()
        .claim_typed("db", &queue(), &QueuePoll::default(), &options)
        .await
        .unwrap();
    let mut items = items.into_iter();
    let one = items.next().unwrap().unwrap();
    let bad = items.next().unwrap().unwrap_err();
    let two = items.next().unwrap().unwrap();
    assert!(items.next().is_none());
    assert_eq!(one.payload, "a");
    assert_eq!(two.payload, "b");
    assert_eq!(bad.payload, json!({"old":true}));
    assert!(matches!(*bad.source, FlowerError::Decode(_)));
    one.lease
        .complete_with(&"done".into(), &options)
        .await
        .unwrap();
    (*bad.lease).kill(&options).await.unwrap();
    two.lease.release(&options).await.unwrap();
    let requests = mock.requests();
    assert_eq!(requests[2].path, "/v1/queue/kill");
    for (request, id) in requests[1..].iter().zip(["one", "bad", "two"]) {
        assert_eq!(request.json()["id"], id);
        assert_eq!(request.json()["token"], format!("secret-{id}"));
        assert_eq!(request.json()["db"], "db");
        assert_eq!(request.json()["queue"], "jobs");
    }
}

#[tokio::test]
async fn lost_settlement_returns_the_same_lease_for_retry_without_exposing_its_secret() {
    let mock = MockTransport::scripted([
        MockReply::json(200, &json!({"ok":true,"items":[claim("one",json!("a"))]})),
        MockReply::hang(),
        MockReply::json(200, &json!({"ok":true})),
    ]);
    let item = mock
        .client()
        .claim_typed(
            "db",
            &queue(),
            &QueuePoll::default(),
            &RequestOptions::new(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
        .unwrap();
    let error = item
        .lease
        .complete_with(
            &"done".into(),
            &RequestOptions::new().timeout(Duration::from_millis(5)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.source.execution(), Execution::Unknown);
    assert_eq!(error.lease.id(), "one");
    assert_eq!(error.lease.fence(), 7);
    assert!(!format!("{error:?}").contains("secret-one"));
    error
        .into_lease()
        .complete_with(&"done".into(), &RequestOptions::new())
        .await
        .unwrap();
    let requests = mock.requests();
    assert_eq!(requests[1].json(), requests[2].json());
}

#[tokio::test]
async fn cancelled_settlement_returns_ownership_before_sending() {
    let mock = MockTransport::scripted([
        MockReply::json(200, &json!({"ok":true,"items":[claim("one",json!("a"))]})),
        MockReply::json(200, &json!({"ok":true})),
    ]);
    let item = mock
        .client()
        .claim_typed(
            "db",
            &queue(),
            &QueuePoll::default(),
            &RequestOptions::new(),
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
        .unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = item
        .lease
        .release(&RequestOptions::new().cancel(cancel))
        .await
        .unwrap_err();
    assert!(matches!(*error.source, FlowerError::Cancelled));
    assert_eq!(mock.requests().len(), 1);
    error
        .into_lease()
        .release(&RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(mock.requests().len(), 2);
}

fn terminal_event(status: u16, origin: &str, details: serde_json::Value) -> String {
    let problem = json!({"status":status,"origin":origin,"execution":"decided","retryable":false,"code":"TASK_LOCKED","message":"locked","details":details});
    format!(
        "event: snapshot\ndata: {{\"revision\":1,\"value\":\"before\"}}\n\nevent: error\ndata: {}\n\n",
        json!({"ok":false,"code":"TASK_LOCKED","message":"locked","details":details,"problem":problem})
    )
}

#[tokio::test]
async fn declared_watches_decode_initial_and_terminal_errors_with_the_original_source() {
    let mock = MockTransport::scripted([
        MockReply::json(
            422,
            &json!({"ok":false,"code":"TASK_LOCKED","message":"locked","details":"ada"}),
        ),
        MockReply::events(&terminal_event(422, "application", json!("bob"))),
    ]);
    let scope = mock.client().database("db").module("tasks");
    let args = TasksGetTaskArgs { key: "one".into() };
    let error = scope
        .watch(&TasksApi::GET_TASK, &args, &WatchOptions::default())
        .await
        .err()
        .unwrap();
    assert_eq!(error.declared(), Some(&TaskError::Locked("ada".into())));
    let mut watch = scope
        .watch(&TasksApi::GET_TASK, &args, &WatchOptions::default())
        .await
        .unwrap();
    assert_eq!(watch.next().await.unwrap().unwrap().value, "before");
    let error = watch.next().await.unwrap().unwrap_err();
    assert_eq!(error.declared(), Some(&TaskError::Locked("bob".into())));
    assert_eq!(
        error.source().problem().unwrap().origin,
        Origin::Application
    );
    assert_eq!(
        error.source().problem().unwrap().details,
        Some(json!("bob"))
    );
    assert!(watch.next().await.is_none());
}

#[tokio::test]
async fn subscriptions_keep_declared_errors_and_never_decode_authorization_as_domain_errors() {
    let mock = MockTransport::scripted([
        MockReply::events(&terminal_event(422, "application", json!("ada"))),
        MockReply::events(&terminal_event(403, "authorization", json!("ada"))),
    ]);
    let scope = mock.client().database("db").module("tasks");
    let args = TasksGetTaskArgs { key: "one".into() };
    for expected_declared in [true, false] {
        let stream = scope
            .subscribe(&TasksApi::GET_TASK, &args, &SubscribeOptions::default())
            .unwrap()
            .into_stream();
        use futures_util::{StreamExt, pin_mut};
        pin_mut!(stream);
        assert_eq!(stream.next().await.unwrap().unwrap().value, "before");
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(error.declared().is_some(), expected_declared);
        if !expected_declared {
            assert!(error.source().is_auth());
        }
        assert!(stream.next().await.is_none());
    }
    assert_eq!(mock.requests().len(), 2);
}

#[tokio::test]
async fn generated_clients_retain_query_metadata_and_prepare_without_executing_a_mutation() {
    let mock = MockTransport::scripted([
        MockReply::ok(json!("current")),
        MockReply::json(
            200,
            &json!({"incarnation":"0000000000000009","epoch":2,"min_epoch":1,"epoch_ms":null,"initialized":true}),
        ),
        MockReply::ok(json!("updated")),
    ]);
    let tasks: TasksClient<_> = TasksApi::client(mock.client().database("db").module("tasks"));
    let result = tasks
        .get_task(&TasksGetTaskArgs { key: "one".into() })
        .options(QueryOptions::new())
        .await
        .unwrap();
    assert_eq!(result.value, "current");
    let command = tasks
        .prepare_put_task(&TasksPutTaskArgs { key: "one".into() })
        .await
        .unwrap();
    let encoded = serde_json::to_vec(&command).unwrap();
    assert_eq!(mock.requests().len(), 2);
    assert!(mock.requests()[1].path.starts_with("/v1/retry?"));
    let restored: PreparedCommand<String, TaskError> = serde_json::from_slice(&encoded).unwrap();
    let result = tasks
        .remote
        .execute(&restored, &CommandOptions::new())
        .await
        .unwrap();
    assert_eq!(result.value, "updated");
    assert_eq!(result.revision, Revision(1));
    assert_eq!(result.position, LogPosition(1));
    assert_eq!(result.request_id, command.request_id());
    for request in [mock.requests()[0].clone(), mock.requests()[2].clone()] {
        assert_eq!(request.json()["db"], "db");
        assert_eq!(request.json()["module"], "tasks");
        assert_eq!(request.json()["args"], json!({"key":"one"}));
    }
}

fn transaction_failure(
    status: u16,
    index: usize,
    database: &str,
    method: &str,
    cause: serde_json::Value,
) -> MockReply {
    MockReply::json(
        status,
        &json!({"ok":false,"code":"TASK_LOCKED","message":"locked","details":{"transaction":{"step":index,"database":database,"module":"tasks","method":method,"code":"TASK_LOCKED","message":"locked"},"cause":cause}}),
    )
}

#[tokio::test]
async fn generated_command_builder_persists_before_sending() {
    let mock = MockTransport::scripted([
        MockReply::json(
            200,
            &json!({"incarnation":"0000000000000009","epoch":2,"min_epoch":1,"epoch_ms":null,"initialized":true}),
        ),
        MockReply::ok(json!("saved")),
    ]);
    let tasks = TasksApi::client(mock.client().database("db").module("tasks"));
    let outbox = CommandOutbox::new(std::sync::Arc::new(outbox::MemoryCommandStore::default()));
    let handle = tasks
        .put_task(&TasksPutTaskArgs { key: "one".into() })
        .enqueue(&outbox)
        .await
        .unwrap();
    assert_eq!(mock.requests().len(), 1);
    assert_eq!(
        handle.status().await.unwrap().status,
        CommandStatus::Pending
    );
    assert_eq!(
        handle.execute(&CommandOptions::new()).await.unwrap().value,
        "saved"
    );
    assert_eq!(
        handle.status().await.unwrap().status,
        CommandStatus::Succeeded
    );
}

#[test]
fn query_stream_builder_refuses_unsupported_constraints_before_sending() {
    let mock = MockTransport::scripted([]);
    let tasks = TasksApi::client(mock.client().database("db").module("tasks"));
    assert!(
        tasks
            .get_task(&TasksGetTaskArgs { key: "one".into() })
            .options(QueryOptions::new().max_staleness(Duration::from_secs(1)))
            .subscribe()
            .is_err()
    );
    assert!(mock.requests().is_empty());
}

#[test]
fn transaction_restore_checks_contracts_and_preserves_identity() {
    let original = TypedTransaction::new()
        .step(
            "db",
            "tasks",
            &TasksApi::PUT_TASK,
            &TasksPutTaskArgs { key: "one".into() },
        )
        .unwrap();
    let bytes = serde_json::to_vec(&original).unwrap();
    let template = TypedTransaction::new()
        .step(
            "db",
            "tasks",
            &TasksApi::PUT_TASK,
            &TasksPutTaskArgs {
                key: "different".into(),
            },
        )
        .unwrap();
    assert_eq!(
        template.restore(&bytes).unwrap().request_id(),
        original.request_id()
    );
    let mut altered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    altered["contracts"][0]["result"] = json!("changed");
    assert!(
        template
            .restore(&serde_json::to_vec(&altered).unwrap())
            .is_err()
    );
    let mut altered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    altered["steps"][0]["args"] = json!({"key": 42});
    assert!(
        template
            .restore(&serde_json::to_vec(&altered).unwrap())
            .is_err()
    );
}

#[tokio::test]
async fn persisted_transactions_decode_the_correct_step_error_and_preserve_context() {
    let other = flower_sdk::MutationOp::<TasksPutTaskArgs, String, CountError>::new("count");
    let tx = TypedTransaction::new()
        .step(
            "one",
            "tasks",
            &other,
            &TasksPutTaskArgs { key: "one".into() },
        )
        .unwrap()
        .step(
            "two",
            "tasks",
            &TasksApi::PUT_TASK,
            &TasksPutTaskArgs { key: "two".into() },
        )
        .unwrap();
    let restored: TypedTransaction<(String, String), (CountError, TaskError)> =
        serde_json::from_slice(&serde_json::to_vec(&tx).unwrap()).unwrap();
    let mock = MockTransport::scripted([
        transaction_failure(422, 1, "two", "put_task", json!("ada")),
        transaction_failure(422, 0, "one", "count", json!(42)),
        transaction_failure(503, 1, "two", "put_task", json!("ada")),
        transaction_failure(422, 1, "other", "put_task", json!("ada")),
        transaction_failure(422, 9, "two", "put_task", json!("ada")),
    ]);
    let client = mock.client();
    let error = restored
        .execute(&client, &CommandOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.step, Some(1));
    assert_eq!(error.database.as_deref(), Some("two"));
    assert_eq!(
        error.declared(),
        Some(&StepError::Step1(TaskError::Locked("ada".into())))
    );
    assert_eq!(error.execution, Execution::Decided);
    assert_eq!(error.request_id, tx.request_id());
    assert!(
        matches!(error.source.as_ref(), FlowerError::Business{details:Some(d), ..} if d["cause"]=="ada")
    );
    let error = restored
        .execute(&client, &CommandOptions::new())
        .await
        .unwrap_err();
    assert_eq!(
        error.declared(),
        Some(&StepError::Step0(CountError::Locked(42)))
    );
    for _ in 0..3 {
        let error = restored
            .execute(&client, &CommandOptions::new())
            .await
            .unwrap_err();
        assert!(error.declared().is_none());
    }
}
