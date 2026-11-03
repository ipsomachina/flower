extern crate alloc;
use flower_client::{
    outbox::MemoryCommandStore,
    testing::{MockReply, MockTransport},
    *,
};
use flower_sdk::{FlowerType, MutationOp, QueryOp};
use serde_json::json;
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(FlowerType)]
struct Args {
    key: String,
}
#[derive(FlowerType)]
struct ChangedArgs {
    key: u64,
}
#[derive(FlowerType)]
struct RenamedArgs {
    key: String,
}
const PUT: MutationOp<Args, String> = MutationOp::new("put");
const GET: QueryOp<Args, String> = QueryOp::new("get");
fn prepare(scope: &ModuleClient) -> PreparedCommand<String> {
    scope
        .prepare_with_identity(
            &PUT,
            &Args { key: "one".into() },
            &RetryIdentity {
                incarnation: 9,
                epoch: 2,
                min_epoch: 1,
                epoch_ms: None,
                initialized: true,
            },
        )
        .unwrap()
}
fn token(db: &str, revision: u64) -> ReadToken {
    ReadToken {
        version: 1,
        database: db.into(),
        group: "east".into(),
        incarnation: Incarnation(9),
        revision: Revision(revision),
        position: LogPosition(revision * 2),
    }
}
fn query_reply(db: &str, revision: u64, value: &str) -> MockReply {
    MockReply::ok(json!(value)).with_header(
        flower_protocol::TOKEN_HEADER,
        &token(db, revision).encode().unwrap(),
    )
}

#[tokio::test]
async fn durable_unknown_delivery_recovers_the_exact_command_and_caches_the_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commands.journal");
    let mock = MockTransport::scripted([
        MockReply::hang(),
        MockReply::json(
            200,
            &json!({"ok":true,"value":"done","revision":7,"position":12,"duplicate":true}),
        ),
    ]);
    let scope = mock.client().database("db").module("app");
    let command = prepare(&scope);
    let outbox = CommandOutbox::new(Arc::new(FileCommandStore::open(&path).unwrap()));
    let handle = outbox.enqueue(&scope, &PUT, &command).await.unwrap();
    let mut updates = handle.updates().await.unwrap();
    assert_eq!(updates.borrow().status, CommandStatus::Pending);
    assert!(mock.requests().is_empty());
    let error = handle
        .execute(&CommandOptions::new().timeout(Duration::from_millis(5)))
        .await
        .unwrap_err();
    assert!(
        matches!(error, OutboxError::Command(ref error) if error.execution==Execution::Unknown)
    );
    updates.changed().await.unwrap();
    assert_eq!(updates.borrow().status, CommandStatus::Unknown);
    assert_eq!(handle.status().await.unwrap().attempts, 1);
    drop(handle);
    drop(outbox);
    let recovered = CommandOutbox::new(Arc::new(FileCommandStore::open(&path).unwrap()));
    let handle = recovered
        .resume(&scope, &PUT, command.request_id())
        .await
        .unwrap();
    assert!(handle.execute(&CommandOptions::new()).await.is_err());
    assert_eq!(mock.requests().len(), 1);
    let result = handle
        .retry_uncertain(&CommandOptions::new())
        .await
        .unwrap();
    assert_eq!(result.value, "done");
    assert!(result.duplicate);
    assert_eq!(result.request_id, command.request_id());
    assert_eq!(handle.status().await.unwrap().attempts, 2);
    assert_eq!(
        handle.execute(&CommandOptions::new()).await.unwrap().value,
        "done"
    );
    let requests = mock.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].json(), requests[1].json());
}

struct FailingStore {
    inner: MemoryCommandStore,
    status: CommandStatus,
    armed: AtomicBool,
}
#[async_trait::async_trait]
impl CommandStore for FailingStore {
    async fn get(&self, id: &str) -> Result<Option<CommandRecord>, StoreError> {
        self.inner.get(id).await
    }
    async fn list(&self) -> Result<Vec<CommandRecord>, StoreError> {
        self.inner.list().await
    }
    async fn insert(&self, record: CommandRecord) -> Result<CommandRecord, StoreError> {
        self.inner.insert(record).await
    }
    async fn compare_exchange(
        &self,
        expected: u64,
        record: CommandRecord,
    ) -> Result<bool, StoreError> {
        if record.status == self.status && self.armed.swap(false, Ordering::SeqCst) {
            return Err(StoreError::message("simulated durable write failure"));
        }
        self.inner.compare_exchange(expected, record).await
    }
}
#[tokio::test]
async fn a_failed_pre_send_persist_never_submits_and_a_failed_receipt_persist_is_recoverable() {
    for status in [CommandStatus::Sending, CommandStatus::Succeeded] {
        let mock =
            MockTransport::scripted([MockReply::ok(json!("done")), MockReply::ok(json!("done"))]);
        let scope = mock.client().database("db").module("app");
        let command = prepare(&scope);
        let store = Arc::new(FailingStore {
            inner: MemoryCommandStore::default(),
            status,
            armed: AtomicBool::new(true),
        });
        let outbox = CommandOutbox::new(store);
        let handle = outbox.enqueue(&scope, &PUT, &command).await.unwrap();
        assert!(matches!(
            handle.execute(&CommandOptions::new()).await,
            Err(OutboxError::Store(_))
        ));
        let expected_requests = usize::from(status == CommandStatus::Succeeded);
        assert_eq!(mock.requests().len(), expected_requests);
        assert_eq!(
            handle.status().await.unwrap().status,
            if expected_requests == 0 {
                CommandStatus::Pending
            } else {
                CommandStatus::Sending
            }
        );
        let resumed = outbox
            .resume(&scope, &PUT, command.request_id())
            .await
            .unwrap();
        assert_eq!(
            resumed
                .retry_uncertain(&CommandOptions::new())
                .await
                .unwrap()
                .value,
            "done"
        );
        for request in mock.requests() {
            assert_eq!(request.json()["idempotency_key"], command.request_id());
        }
    }
}

#[tokio::test]
async fn a_not_started_retry_cannot_erase_an_earlier_unknown_outcome() {
    let mock = MockTransport::scripted([MockReply::hang(), MockReply::ok(json!("done"))]);
    let scope = mock.client().database("db").module("app");
    let handle = CommandOutbox::new(Arc::new(MemoryCommandStore::default()))
        .enqueue(&scope, &PUT, &prepare(&scope))
        .await
        .unwrap();
    handle
        .execute(&CommandOptions::new().timeout(Duration::from_millis(5)))
        .await
        .unwrap_err();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = handle
        .retry_uncertain(&CommandOptions::new().cancel(cancel))
        .await
        .unwrap_err();
    assert!(
        matches!(error, OutboxError::Command(ref error) if error.execution == Execution::Unknown)
    );
    assert_eq!(
        handle.status().await.unwrap().status,
        CommandStatus::Unknown
    );
    assert_eq!(mock.requests().len(), 1);
    handle
        .retry_uncertain(&CommandOptions::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn expired_history_requires_evidence_and_never_rekeys_or_resubmits_automatically() {
    let mock = MockTransport::scripted([MockReply::failure("epoch_retired", "expired", None)]);
    let scope = mock.client().database("db").module("app");
    let command = prepare(&scope);
    let handle = CommandOutbox::new(Arc::new(MemoryCommandStore::default()))
        .enqueue(&scope, &PUT, &command)
        .await
        .unwrap();
    handle.execute(&CommandOptions::new()).await.unwrap_err();
    assert_eq!(
        handle.status().await.unwrap().status,
        CommandStatus::Unresolved
    );
    assert!(matches!(
        handle.retry_uncertain(&CommandOptions::new()).await,
        Err(OutboxError::Unresolved)
    ));
    let result = CommandResult {
        value: json!("found by business ID"),
        revision: Revision(7),
        position: LogPosition(12),
        duplicate: true,
        request_id: command.request_id().into(),
        read_token: None,
    };
    handle
        .resolve(
            Resolution::Succeeded(Box::new(result)),
            "business lookup invoice/123",
        )
        .await
        .unwrap();
    assert_eq!(
        handle.execute(&CommandOptions::new()).await.unwrap().value,
        "found by business ID"
    );
    assert_eq!(mock.requests().len(), 1);
    assert_eq!(
        handle.status().await.unwrap().evidence.as_deref(),
        Some("business lookup invoice/123")
    );
}

#[tokio::test]
async fn recovery_checks_structural_contracts_and_identity_conflicts_before_sending() {
    let mock = MockTransport::scripted([]);
    let scope = mock.client().database("db").module("app");
    let command = prepare(&scope);
    let outbox = CommandOutbox::new(Arc::new(MemoryCommandStore::default()));
    outbox.enqueue(&scope, &PUT, &command).await.unwrap();
    let changed = MutationOp::<ChangedArgs, String>::new("put");
    assert!(
        outbox
            .resume(&scope, &changed, command.request_id())
            .await
            .is_err()
    );
    let changed_result = MutationOp::<Args, u64>::new("put");
    assert!(
        outbox
            .resume(&scope, &changed_result, command.request_id())
            .await
            .is_err()
    );
    let renamed = MutationOp::<RenamedArgs, String>::new("put");
    outbox
        .resume(&scope, &renamed, command.request_id())
        .await
        .unwrap();
    let mut altered = serde_json::to_value(&command).unwrap();
    altered["args"]["key"] = json!("different work");
    let altered: PreparedCommand<String> = serde_json::from_value(altered).unwrap();
    assert!(matches!(
        outbox.enqueue(&scope, &PUT, &altered).await,
        Err(OutboxError::Store(_))
    ));
    let wrong: PreparedCommand<u64> =
        serde_json::from_value(serde_json::to_value(&command).unwrap()).unwrap();
    assert_eq!(
        scope
            .execute(&wrong, &CommandOptions::new())
            .await
            .unwrap_err()
            .execution,
        Execution::NotStarted
    );
    assert!(mock.requests().is_empty());
}

#[tokio::test]
async fn a_committed_badly_encoded_result_is_persisted_and_never_resent() {
    let mock = MockTransport::scripted([MockReply::ok(json!(42))]);
    let scope = mock.client().database("db").module("app");
    let handle = CommandOutbox::new(Arc::new(MemoryCommandStore::default()))
        .enqueue(&scope, &PUT, &prepare(&scope))
        .await
        .unwrap();
    for _ in 0..2 {
        let error = handle.execute(&CommandOptions::new()).await.unwrap_err();
        assert!(
            matches!(error, OutboxError::Command(ref error) if error.execution == Execution::Decided && matches!(error.source.source(), FlowerError::Decode(_)))
        );
    }
    assert_eq!(
        handle.status().await.unwrap().status,
        CommandStatus::Succeeded
    );
    assert_eq!(mock.requests().len(), 1);
}

#[tokio::test]
async fn concurrent_handles_have_one_delivery_owner() {
    let mock = MockTransport::scripted([MockReply::hang()]);
    let scope = mock.client().database("db").module("app");
    let command = prepare(&scope);
    let outbox = CommandOutbox::new(Arc::new(MemoryCommandStore::default()));
    let first = outbox.enqueue(&scope, &PUT, &command).await.unwrap();
    let second = outbox
        .resume(&scope, &PUT, command.request_id())
        .await
        .unwrap();
    let options = CommandOptions::new().timeout(Duration::from_millis(20));
    let (a, b) = tokio::join!(first.execute(&options), second.execute(&options));
    assert!(a.is_err() && b.is_err());
    assert_eq!(mock.requests().len(), 1);
    assert_eq!(first.status().await.unwrap().attempts, 1);
}

#[tokio::test]
async fn journal_repairs_torn_tails_but_refuses_complete_corruption_and_serializes_instances() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox");
    let mock = MockTransport::scripted([]);
    let scope = mock.client().database("db").module("app");
    let command = prepare(&scope);
    let store = Arc::new(FileCommandStore::open(&path).unwrap());
    let handle = CommandOutbox::new(store.clone())
        .enqueue(&scope, &PUT, &command)
        .await
        .unwrap();
    let valid_len = std::fs::metadata(&path).unwrap().len();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{torn")
        .unwrap();
    let other = FileCommandStore::open(&path).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
    let mut first = handle.status().await.unwrap();
    first.generation = 1;
    first.attempts = 1;
    first.status = CommandStatus::Sending;
    let second = first.clone();
    let (a, b) = tokio::join!(
        store.compare_exchange(0, first),
        other.compare_exchange(0, second)
    );
    assert_ne!(a.unwrap(), b.unwrap());
    let recovered = CommandOutbox::new(Arc::new(other));
    let pending = recovered
        .resume(&scope, &PUT, command.request_id())
        .await
        .unwrap();
    assert!(matches!(
        pending.execute(&CommandOptions::new()).await,
        Err(OutboxError::Busy)
    ));
    let mut bytes = std::fs::read(&path).unwrap();
    let pos = bytes
        .windows(12)
        .position(|window| window == b"\"checksum\":\"")
        .unwrap()
        + 12;
    bytes[pos] = if bytes[pos] == b'0' { b'1' } else { b'0' };
    std::fs::write(&path, &bytes).unwrap();
    assert!(FileCommandStore::open(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[tokio::test]
async fn causal_sessions_join_commit_tokens_and_keep_databases_separate() {
    let committed = token("db", 3);
    let write = MockReply::json(
        200,
        &json!({"ok":true,"value":"done","revision":3,"position":6,"read_token":committed}),
    );
    let mock = MockTransport::scripted([
        write,
        query_reply("db", 4, "after"),
        query_reply("other", 1, "separate"),
    ]);
    let session = mock.client().causal_session();
    let scope = session.database("db").module("app");
    scope
        .execute(&prepare(&scope), &CommandOptions::new())
        .await
        .unwrap();
    scope
        .query(&GET, &Args { key: "one".into() })
        .await
        .unwrap();
    session
        .database("other")
        .module("app")
        .query(&GET, &Args { key: "two".into() })
        .await
        .unwrap();
    let requests = mock.requests();
    let bound = ReadToken::decode(
        requests[1]
            .header(flower_protocol::AT_LEAST_HEADER)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(bound, token("db", 3));
    assert!(
        requests[2]
            .header(flower_protocol::AT_LEAST_HEADER)
            .is_none()
    );
    assert_eq!(session.clone().token("db").await, Some(token("db", 4)));
    assert_eq!(session.token("other").await, Some(token("other", 1)));
}

#[tokio::test]
async fn causal_sessions_refuse_missing_bounds_and_incomparable_histories_without_resetting() {
    let mock = MockTransport::scripted([
        MockReply::ok(json!("unsupported")),
        query_reply("db", 2, "too old"),
    ]);
    let session = mock.client().causal_session();
    let scope = session.database("db").module("app");
    assert!(
        scope
            .query(&GET, &Args { key: "one".into() })
            .await
            .is_err()
    );
    session.observe(token("db", 7)).await.unwrap();
    assert!(
        scope
            .query(&GET, &Args { key: "one".into() })
            .await
            .is_err()
    );
    let mut wrong = token("db", 9);
    wrong.incarnation = Incarnation(10);
    assert!(session.observe(wrong.clone()).await.is_err());
    let different = scope
        .prepare_with_identity(
            &PUT,
            &Args { key: "one".into() },
            &RetryIdentity {
                incarnation: 10,
                epoch: 2,
                min_epoch: 1,
                epoch_ms: None,
                initialized: true,
            },
        )
        .unwrap();
    assert_eq!(
        scope
            .execute(&different, &CommandOptions::new())
            .await
            .unwrap_err()
            .execution,
        Execution::NotStarted
    );
    assert_eq!(mock.requests().len(), 2);
    assert_eq!(session.token("db").await, Some(token("db", 7)));
    session.reset("db").await;
    session.observe(wrong.clone()).await.unwrap();
    assert_eq!(session.token("db").await, Some(wrong));
}

async fn until_requests(mock: &MockTransport, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while mock.requests().len() < count {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn causal_locking_is_per_database_and_honors_cancellation_and_deadlines() {
    let (reply, stream) = MockReply::event_stream("");
    let mock = MockTransport::scripted([
        reply.with_header(
            flower_protocol::TOKEN_HEADER,
            &token("db", 10).encode().unwrap(),
        ),
        query_reply("other", 1, "independent"),
        query_reply("db", 11, "second"),
    ]);
    let session = mock.client().causal_session();
    let first = session.database("db").module("app");
    let running = tokio::spawn(async move { first.query(&GET, &Args { key: "one".into() }).await });
    until_requests(&mock, 1).await;
    let blocked = session.database("db").module("app");
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        blocked
            .query(&GET, &Args { key: "one".into() })
            .options(QueryOptions::new().cancel(cancel)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error.source(), FlowerError::Cancelled));
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        blocked
            .query(&GET, &Args { key: "one".into() })
            .options(QueryOptions::new().deadline(Instant::now() + Duration::from_millis(5))),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.source().is_timeout());
    session
        .database("other")
        .module("app")
        .query(&GET, &Args { key: "two".into() })
        .await
        .unwrap();
    assert_eq!(mock.requests().len(), 2);
    assert!(stream.push(r#"{"ok":true,"value":"first"}"#));
    drop(stream);
    running.await.unwrap().unwrap();
    blocked
        .query(&GET, &Args { key: "one".into() })
        .await
        .unwrap();
    assert_eq!(
        ReadToken::decode(
            mock.requests()[2]
                .header(flower_protocol::AT_LEAST_HEADER)
                .unwrap()
        )
        .unwrap(),
        token("db", 10)
    );
}

#[tokio::test]
async fn negotiation_reports_missing_capabilities_unknown_extensions_and_unsupported_versions() {
    use flower_client::protocol::{Capability, NegotiationError, ProtocolRequirements};
    let mock = MockTransport::scripted([
        MockReply::json(
            200,
            &json!({"version":1,"capabilities":["read_tokens","future_extension"]}),
        ),
        MockReply::json(
            200,
            &json!({"version":2,"capabilities":["read_tokens","scoped_commands"]}),
        ),
    ]);
    let client = mock.client();
    let requirements =
        ProtocolRequirements::requiring([Capability::ReadTokens, Capability::ScopedCommands]);
    let error = client
        .negotiate(&requirements, &RequestOptions::new())
        .await
        .unwrap_err();
    let NegotiationError::Incompatible(report) = error else {
        panic!("expected compatibility report");
    };
    assert_eq!(report.missing, vec![Capability::ScopedCommands]);
    assert_eq!(report.unknown, vec!["future_extension"]);
    let info = client
        .inspect_protocol(&RequestOptions::new())
        .await
        .unwrap();
    let permissive = ProtocolRequirements {
        versions: 0..=9,
        ..requirements
    };
    assert!(!info.compatibility(&permissive).compatible);
    assert_eq!(info.compatibility(&permissive).implemented_version, 1);
}

#[test]
fn legacy_command_options_refuse_query_constraints() {
    assert!(CommandOptions::try_from(RequestOptions::new().at_least(token("db", 7))).is_err());
    assert!(CommandOptions::try_from(RequestOptions::new().max_staleness(Duration::ZERO)).is_err());
}

#[test]
fn explicit_snapshot_migration_preserves_the_entire_original_intent() {
    let mock = MockTransport::scripted([]);
    let scope = mock.client().database("db").module("app");
    let command = prepare(&scope).expected_revision(Revision(7));
    let mut legacy = serde_json::to_value(&command).unwrap();
    legacy.as_object_mut().unwrap().remove("contract");
    legacy["version"] = json!(1);
    let bytes = serde_json::to_vec(&legacy).unwrap();
    assert!(scope.restore(&PUT, &bytes).is_err());
    let migrated = serde_json::to_value(scope.migrate_legacy(&PUT, &bytes).unwrap()).unwrap();
    for field in [
        "database",
        "module",
        "method",
        "args",
        "request_id",
        "expected_revision",
    ] {
        assert_eq!(migrated[field], legacy[field]);
    }
    assert_eq!(migrated["version"], 2);
    assert_eq!(
        migrated["contract"],
        serde_json::to_value(command.contract()).unwrap()
    );
    assert!(mock.requests().is_empty());
}
