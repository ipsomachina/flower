//! The native examples the guides show: `flower-client` calls, errors,
//! watches and transactions, and `flower-worker` workers.

extern crate alloc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use flower_client::watch::SubscribeOptions;
use flower_client::{
    CancellationToken, CommandOptions, Credentials, FlowerClient, FlowerError, MutationOptions,
    MutationResult, QueryResult, RemoteError, RequestOptions, Retry, TransactionOptions,
    TransactionResult, TransactionStep,
};
use flower_worker::{
    JobControl, QueueWorkerOptions, WorkError, WorkerSummary, run_queue_worker,
    run_queue_worker_with,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Notify;

// ANCHOR: connect
pub fn connect(token: &str) -> Result<FlowerClient, FlowerError> {
    FlowerClient::builder("http://127.0.0.1:8080")
        .credentials(Credentials::token(token))
        // Retry transient failures, with backoff, and ride out overload.
        .retry(Retry::Default)
        // Send writes straight to the leader each reply names.
        .leader_routing(true)
        // Each attempt's timeout (the default is 20 s).
        .timeout(Duration::from_secs(10))
        .build()
}
// ANCHOR_END: connect

// ANCHOR: calls
#[derive(Serialize)]
struct AddTask<'a> {
    id: &'a str,
    title: &'a str,
}

#[derive(Deserialize, Debug)]
pub struct Task {
    pub title: String,
    pub done: bool,
}

#[derive(Deserialize, Debug)]
pub struct OpenTask {
    pub id: String,
    pub title: String,
}

pub async fn add_and_list(client: &FlowerClient) -> Result<Vec<OpenTask>, FlowerError> {
    // A request ID chosen per intent: retrying it can never add twice.
    let options = MutationOptions::new().request_id("add-milk");
    let args = AddTask {
        id: "milk",
        title: "Buy milk",
    };
    let added: MutationResult<Task> = client
        .mutate("demo", "tasks", "add_task", &args, &options)
        .await?;
    println!(
        "{:?} at revision {} (duplicate: {})",
        added.value, added.revision, added.duplicate
    );

    let open: QueryResult<Vec<OpenTask>> = client
        .query(
            "demo",
            "tasks",
            "open_tasks",
            &json!({}),
            &RequestOptions::new(),
        )
        .await?;
    Ok(open.value)
}
// ANCHOR_END: calls

// ANCHOR: typed
use doc_snippets::methods::{TRANSFER, TransferArgs, TransferError};
use tasks::{ADD_TASK, AddTaskArgs, OPEN_TASKS, OpenTasksArgs};

/// The guest crate is a dependency (built as an `rlib` too): each
/// `#[query]` or `#[mutation]` static carries its name, its kind, its
/// argument struct, its result type and its declared errors.
pub async fn add_milk(client: &FlowerClient) -> Result<Vec<tasks::OpenTask>, FlowerError> {
    let args = AddTaskArgs {
        id: "milk".to_string(),
        title: "Buy milk".to_string(),
    };
    client
        .call("demo", "tasks", &ADD_TASK, &args)
        .await
        .map_err(|error| error.source().clone())?;
    client
        .call("demo", "tasks", &OPEN_TASKS, &OpenTasksArgs {})
        .await
        .map_err(|error| error.source().clone())
}

/// A failure the method declares comes back typed.
pub async fn pay(client: &FlowerClient, from: &str, amount: u64) -> Result<String, FlowerError> {
    let args = TransferArgs {
        from: from.to_string(),
        to: "merchant".to_string(),
        amount,
    };
    match client.call("bank", "bank", &TRANSFER, &args).await {
        Ok(left) => Ok(format!("paid; {left} left")),
        Err(RemoteError::Declared { error, source }) => match *error {
            TransferError::InsufficientFunds(balance) => {
                Ok(format!("declined: balance is {balance}"))
            }
            _ => Err(*source),
        },
        Err(other) => Err(other.source().clone()),
    }
}

/// `call` sends a mutation under a fresh request ID; `call_with` takes
/// options. Here the request ID names the payment, so sending it again
/// answers the first attempt's outcome instead of paying twice.
pub async fn pay_once(
    client: &FlowerClient,
    from: &str,
    amount: u64,
) -> Result<u64, RemoteError<TransferError>> {
    let args = TransferArgs {
        from: from.to_string(),
        to: "merchant".to_string(),
        amount,
    };
    let options = MutationOptions::retrying().request_id(format!("pay:{from}:{amount}"));
    client
        .call_with("bank", "bank", &TRANSFER, &args, &options)
        .await
}
// ANCHOR_END: typed

// ANCHOR: durable
/// Prepare once; durably store the serialized command before its first execution.
pub async fn prepare_task(
    client: &FlowerClient,
    id: String,
    title: String,
) -> Result<flower_client::PreparedCommand<tasks::Task>, FlowerError> {
    let operation = ADD_TASK
        .as_mutation()
        .map_err(|failure| FlowerError::Decode(failure.message))?;
    client
        .database("demo")
        .module("tasks")
        .prepare(&operation, &AddTaskArgs { id, title })
        .await
}

/// A recovered command retains its identity and returns all commit metadata.
pub async fn execute_task(
    client: &FlowerClient,
    command: &flower_client::PreparedCommand<tasks::Task>,
) -> Result<
    flower_client::CommandResult<tasks::Task>,
    flower_client::CommandError<flower_sdk::Failure>,
> {
    client
        .database("demo")
        .module("tasks")
        .execute(command, &CommandOptions::retrying())
        .await
}
// ANCHOR_END: durable

// ANCHOR: generated-client
#[flower_sdk::interface(name = "tasks.v1", handle = TasksApi, client = TasksClient)]
pub trait TasksContract {
    #[query]
    fn get_task(
        ctx: &mut flower_sdk::QueryCtx<'_, '_>,
        id: String,
    ) -> flower_sdk::Result<Option<tasks::Task>>;
    #[mutation]
    fn add_task(
        ctx: &mut flower_sdk::MutationCtx<'_, '_>,
        id: String,
        title: String,
    ) -> flower_sdk::Result<tasks::Task>;
}

pub async fn prepare_with_generated_client(
    client: &FlowerClient,
) -> Result<flower_client::PreparedCommand<tasks::Task>, FlowerError> {
    let tasks = TasksApi::client(client.database("demo").module("tasks"));
    let current = tasks
        .get_task(&TasksContractGetTaskArgs { id: "milk".into() })
        .await
        .map_err(|error| error.source().clone())?;
    println!("current value: {:?}", current.value);
    // Persist the returned command before tasks.remote.execute(...).
    tasks
        .prepare_add_task(&TasksContractAddTaskArgs {
            id: "milk".into(),
            title: "Buy milk".into(),
        })
        .await
}
// ANCHOR_END: generated-client

// ANCHOR: workflow-builders
pub async fn enqueue_with_generated_client(
    client: &FlowerClient,
    outbox: &flower_client::CommandOutbox,
    args: &TasksContractAddTaskArgs,
) -> Result<
    flower_client::CommandHandle<tasks::Task>,
    flower_client::OutboxError<flower_sdk::Failure>,
> {
    let tasks = TasksApi::client(client.database("demo").module("tasks"));
    // Returns only after persistence. Execute this handle to deliver the mutation.
    tasks.add_task(args).enqueue(outbox).await
}

pub fn subscribe_with_generated_client(
    client: &FlowerClient,
    id: String,
) -> Result<
    flower_client::DeclaredSubscription<Option<tasks::Task>, flower_sdk::Failure>,
    RemoteError<flower_sdk::Failure>,
> {
    let tasks = TasksApi::client(client.database("demo").module("tasks"));
    tasks.get_task(&TasksContractGetTaskArgs { id }).subscribe()
}
// ANCHOR_END: workflow-builders

// ANCHOR: transaction-restore
pub fn restore_archive_batch(
    saved: &[u8],
) -> Result<flower_client::TypedTransaction<(tasks::Task,), (flower_sdk::Failure,)>, FlowerError> {
    // A freshly built template supplies the current operation and argument decoder.
    // Its placeholder arguments and fresh ID do not replace the saved work.
    let template = flower_client::TypedTransaction::new().step(
        "archive",
        "tasks",
        &TasksApi::ADD_TASK,
        &TasksContractAddTaskArgs {
            id: String::new(),
            title: String::new(),
        },
    )?;
    template.restore(saved)
}
// ANCHOR_END: transaction-restore

// ANCHOR: outbox
pub async fn persist_task(
    client: &FlowerClient,
    journal: &std::path::Path,
    args: &TasksContractAddTaskArgs,
) -> Result<flower_client::CommandHandle<tasks::Task>, Box<dyn std::error::Error>> {
    let outbox = flower_client::CommandOutbox::new(Arc::new(
        flower_client::FileCommandStore::open(journal)?,
    ));
    let module = client.database("demo").module("tasks");
    let command = module.prepare(&TasksApi::ADD_TASK, args).await?;
    // No mutation has been sent. The returned handle owns the durable workflow.
    Ok(outbox
        .enqueue(&module, &TasksApi::ADD_TASK, &command)
        .await?)
}

pub async fn recover_task(
    client: &FlowerClient,
    journal: &std::path::Path,
    request_id: &str,
) -> Result<flower_client::CommandHandle<tasks::Task>, Box<dyn std::error::Error>> {
    let outbox = flower_client::CommandOutbox::new(Arc::new(
        flower_client::FileCommandStore::open(journal)?,
    ));
    let module = client.database("demo").module("tasks");
    // Checks the current complete contract; keeps the original request ID.
    Ok(outbox
        .resume(&module, &TasksApi::ADD_TASK, request_id)
        .await?)
}
// ANCHOR_END: outbox

// ANCHOR: causal-session
pub async fn causal_roundtrip(
    client: &FlowerClient,
    args: &TasksContractAddTaskArgs,
) -> Result<QueryResult<Option<tasks::Task>>, Box<dyn std::error::Error>> {
    let session = client.causal_session();
    let tasks = TasksApi::client(session.database("demo").module("tasks"));
    let command = tasks.prepare_add_task(args).await?;
    tasks
        .remote
        .execute(&command, &CommandOptions::retrying())
        .await?;
    // The session automatically bounds this query by the committed write token.
    Ok(tasks
        .get_task(&TasksContractGetTaskArgs {
            id: args.id.clone(),
        })
        .options(
            flower_client::QueryOptions::new().deadline(Instant::now() + Duration::from_secs(5)),
        )
        .await?)
}
// ANCHOR_END: causal-session

// ANCHOR: compatibility
pub async fn require_causal_peer(
    client: &FlowerClient,
) -> Result<flower_client::protocol::ProtocolInfo, flower_client::protocol::NegotiationError> {
    use flower_client::protocol::{Capability, ProtocolRequirements};
    client
        .negotiate(
            &ProtocolRequirements::requiring([Capability::ReadTokens, Capability::ScopedCommands]),
            &RequestOptions::new(),
        )
        .await
}
// ANCHOR_END: compatibility

// ANCHOR: transport-conformance
#[tokio::test]
async fn transport_passes_the_probe_suite() -> Result<(), Box<dyn std::error::Error>> {
    use flower_client::{HttpTransport, TransportOptions, testing::conformance::ProbeServer};
    let server = ProbeServer::start().await?;
    let transport = HttpTransport::with_options(server.base_url(), TransportOptions::direct())?;
    // Substitute your own Transport here; the same checks apply.
    server.check(&transport).await.assert_passed();
    server.shutdown().await;
    Ok(())
}
// ANCHOR_END: transport-conformance

// ANCHOR: owned-claim
pub async fn settle_claim(
    outcome: flower_client::ClaimOutcome<String, String>,
    options: &RequestOptions,
) -> Result<(), flower_client::SettlementError<String>> {
    match outcome {
        Ok(claim) => {
            claim
                .lease
                .complete_with(&"processed".into(), options)
                .await
        }
        // Bad payloads retain their raw value and their lease for explicit handling.
        Err(bad) => (*bad.lease).kill(options).await,
    }
}

pub async fn retry_completion(
    error: flower_client::SettlementError<String>,
    original_result: &String,
    options: &RequestOptions,
) -> Result<(), flower_client::SettlementError<String>> {
    error
        .into_lease()
        .complete_with(original_result, options)
        .await
}
// ANCHOR_END: owned-claim

// ANCHOR: errors
/// What a failed call means for the caller.
pub fn advice(error: &FlowerError) -> &'static str {
    if error.is_auth() {
        return "credentials refused";
    }
    if error.execution() == flower_client::Execution::Unknown {
        return "outcome unknown: reuse the persisted command and its request ID";
    }
    if error.is_transient() {
        return "retry with the same identity after the server's retry hint";
    }
    match error {
        // The call's own failure (HTTP 422): a declared error, bad
        // arguments (INVALID_ARGUMENT), its own deadline passing
        // (deadline_exceeded). Final; show it, don't retry it.
        FlowerError::Business { .. } => "the application said no",
        // 401/403: the hook or an access rule refused the caller.
        FlowerError::Auth(_) => "credentials refused",
        // The write may or may not have committed.
        FlowerError::Timeout | FlowerError::Transport(_) | FlowerError::Cancelled => {
            "unknown outcome: retry with the same request ID"
        }
        // Legacy transport errors; structured HTTP errors retain certainty separately.
        FlowerError::RetryExhausted(_) | FlowerError::Forward { .. } => "try again later",
        // A refused request (unknown alias, malformed body, too large)
        // or a server failure: a bug or a misconfiguration, not a
        // transient state.
        FlowerError::Server { .. } | FlowerError::Decode(_) => "fix the call",
        _ => "see the error",
    }
}
// ANCHOR_END: errors

// ANCHOR: deadline
pub async fn open_tasks_soon(
    client: &FlowerClient,
    cancel: CancellationToken,
) -> Result<serde_json::Value, FlowerError> {
    let options = RequestOptions::retrying()
        // No attempt starts after this, and each is cut to what is left;
        // the server sheds work queued past it (x-flower-timeout-ms).
        .deadline(Instant::now() + Duration::from_secs(2))
        .cancel(cancel);
    let open = client
        .query_value("demo", "tasks", "open_tasks", &json!({}), &options)
        .await?;
    Ok(open.value)
}
// ANCHOR_END: deadline

// ANCHOR: scoped
pub async fn checkout(
    client: &FlowerClient,
    cart: &str,
) -> Result<MutationResult<serde_json::Value>, FlowerError> {
    let identity = client
        .retry_identity("shop", &RequestOptions::new())
        .await?;
    // The same intent gives the same ID within the retry window; store
    // it with the cart before sending, and resend it after a crash.
    let id = identity.request_id(format!("checkout:{cart}").as_bytes());
    let options = MutationOptions::retrying().request_id(id);
    client
        .mutate_value("shop", "shop", "checkout", &json!({"cart": cart}), &options)
        .await
}
// ANCHOR_END: scoped

// ANCHOR: watch
pub async fn follow_open_tasks(client: &FlowerClient) -> Result<(), FlowerError> {
    let mut subscription = client.subscribe::<Vec<OpenTask>>(
        "demo",
        "tasks",
        "open_tasks",
        &json!({}),
        &SubscribeOptions::default(),
    );
    while let Some(update) = subscription.next().await {
        let update = update?;
        // `reset` marks the first value after (re)connecting.
        println!(
            "revision {}: {} open{}",
            update.revision,
            update.value.len(),
            if update.reset {
                " (fresh snapshot)"
            } else {
                ""
            }
        );
    }
    Ok(())
}

pub async fn until_all_done(client: &FlowerClient) -> Result<u64, FlowerError> {
    let done = client
        .wait_until(
            "demo",
            "tasks",
            "open_tasks",
            &json!({}),
            |open: &Vec<OpenTask>| open.is_empty(),
            &SubscribeOptions::default(),
        )
        .await?;
    Ok(done.revision)
}
// ANCHOR_END: watch

// ANCHOR: transaction
pub async fn archive(client: &FlowerClient, id: &str) -> Result<TransactionResult, FlowerError> {
    let steps = vec![
        TransactionStep {
            db: "demo".to_string(),
            module: "tasks".to_string(),
            method: "complete_task".to_string(),
            args: json!({"id": id}),
        },
        TransactionStep {
            db: "archive".to_string(),
            module: "tasks".to_string(),
            method: "add_task".to_string(),
            args: json!({"id": id, "title": "archived"}),
        },
    ];
    let options = TransactionOptions {
        request_id: Some(format!("archive:{id}")),
        ..TransactionOptions::new()
    };
    client.transaction(&steps, &options).await
}
// ANCHOR_END: transaction

// ANCHOR: worker
#[derive(Deserialize)]
struct Email {
    to: String,
    subject: String,
}

/// Stand-in for your mail provider's client.
async fn send_email(to: &str, subject: &str, fence: u64) -> Result<(), String> {
    println!("to {to}: {subject} (fence {fence})");
    Ok(())
}

pub async fn run_mailer() -> Result<WorkerSummary, FlowerError> {
    let token = std::env::var("FLOWER_WORKER_TOKEN").unwrap_or_default();
    let client = FlowerClient::builder("http://127.0.0.1:8080")
        .credentials(Credentials::token(token))
        .build()?;
    let options = QueueWorkerOptions::new("crm", "mail")
        .scope("acme")
        .owner("mailer-1")
        // Between 1 and 32 handlers at once, following demand and load.
        .adaptive(1, 4, 32)
        .lease(Duration::from_secs(60))
        // Keep a place in the scope's line instead of polling.
        .wait(Duration::from_secs(30))
        // Each report claims the next jobs in the same commit.
        .chain()
        // On stop, give handlers 20 s, then fail what is left.
        .drain(Duration::from_secs(20));

    let stop = Arc::new(Notify::new());
    let on_signal = stop.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        on_signal.notify_one();
    });

    run_queue_worker(client, options, stop, |claim| async move {
        let email: Email = serde_json::from_value(claim.payload)
            .map_err(|error| WorkError::Terminal(error.to_string()))?;
        send_email(&email.to, &email.subject, claim.fence)
            .await
            // Fail the claim; it comes back after the default backoff.
            .map_err(|_| WorkError::Retry(None))
    })
    .await
}
// ANCHOR_END: worker

// ANCHOR: job-control
pub async fn run_renderer(
    client: FlowerClient,
    stop: Arc<Notify>,
) -> Result<WorkerSummary, FlowerError> {
    let options = QueueWorkerOptions::new("media", "render").concurrency(8);
    run_queue_worker_with(client, options, stop, |claim, job: JobControl| async move {
        if claim.payload["slow"] == json!(true) {
            // Mostly waiting on another service from here: stop counting
            // against the limit, so the worker takes other jobs.
            job.idle();
        }
        match render(&claim.payload).await {
            Ok(()) => Ok(()),
            Err(RenderError::Busy) => {
                // The service behind us is overloaded: pause claiming.
                job.throttle(Duration::from_secs(5), "renderer is busy");
                Err(WorkError::Retry(Some(Duration::from_secs(5))))
            }
            Err(RenderError::Invalid(why)) => Err(WorkError::Terminal(why)),
        }
    })
    .await
}

enum RenderError {
    Busy,
    Invalid(String),
}

async fn render(job: &serde_json::Value) -> Result<(), RenderError> {
    match job.get("source") {
        Some(_) => Ok(()),
        None => Err(RenderError::Invalid("no source".to_string())),
    }
}
// ANCHOR_END: job-control

/// Keeps `RenderError::Busy` constructed.
pub fn _busy() -> bool {
    matches!(RenderError::Busy, RenderError::Busy)
}

// ANCHOR: mock
pub async fn counts_with_a_mock() {
    use flower_client::testing::{MockReply, MockTransport};

    let mock = MockTransport::scripted([MockReply::ok(json!(3))]);
    let client = mock.client();
    let count = client
        .query_value("db", "app", "count", &json!({}), &RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(count.value, 3);
    assert_eq!(mock.requests()[0].path, "/v1/query");
}
// ANCHOR_END: mock

// ANCHOR: fake-queue
pub async fn worker_against_a_fake_queue() {
    use flower_worker::testing::FakeQueue;

    let queue = FakeQueue::new();
    queue.enqueue("crm", "mail", json!({"to": "ada@example.com"}), 5);
    let stop = Arc::new(Notify::new());
    let worker = tokio::spawn(run_queue_worker(
        queue.clone(),
        QueueWorkerOptions::new("crm", "mail")
            .concurrency(1)
            .poll_interval(Duration::from_millis(10)),
        stop.clone(),
        |_claim| async move { Ok(()) },
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    assert_eq!(summary.completed, 1);
    assert_eq!(queue.stats("crm", "mail").pending, 0);
}
// ANCHOR_END: fake-queue
