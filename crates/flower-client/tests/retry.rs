//! The retry loop's overload budget, `Retry-After` floor, deadline and
//! cancellation, over a scripted transport.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flower_client::CancellationToken;
use flower_client::client::{FlowerClient, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::retry::{Retry, RetryPolicy};
use flower_client::transport::{Credentials, StreamResponse, Transport, TransportResponse};

/// One scripted answer.
#[derive(Clone)]
enum Step {
    /// Answer with a status, a `Retry-After` and a JSON body.
    Reply(u16, Option<u64>, serde_json::Value),
    /// Never answer.
    Hang,
}

fn ok() -> Step {
    Step::Reply(200, None, serde_json::json!({"ok": true, "value": 7}))
}

fn overloaded(retry_after_ms: Option<u64>) -> Step {
    Step::Reply(
        503,
        retry_after_ms,
        serde_json::json!({"ok": false, "code": "overloaded", "message": "busy"}),
    )
}

fn unavailable() -> Step {
    Step::Reply(
        503,
        None,
        serde_json::json!({"ok": false, "code": "quorum", "message": "no quorum"}),
    )
}

/// Answers in script order, repeating the last step; records when each
/// request arrived and the deadline header it carried.
struct Scripted {
    steps: Mutex<VecDeque<Step>>,
    last: Step,
    sent: Mutex<Vec<(Instant, Option<String>)>>,
}

impl Scripted {
    fn new(steps: Vec<Step>) -> Arc<Self> {
        let last = steps.last().cloned().unwrap_or(Step::Hang);
        Arc::new(Scripted {
            steps: Mutex::new(steps.into()),
            last,
            sent: Mutex::new(Vec::new()),
        })
    }

    fn sent(&self) -> Vec<(Instant, Option<String>)> {
        self.sent.lock().unwrap().clone()
    }

    async fn answer(
        &self,
        headers: &[(&'static str, String)],
    ) -> Result<TransportResponse, FlowerError> {
        let deadline = headers
            .iter()
            .find(|(name, _)| *name == "x-flower-timeout-ms")
            .map(|(_, value)| value.clone());
        self.sent.lock().unwrap().push((Instant::now(), deadline));
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.last.clone());
        match step {
            Step::Reply(status, retry_after_ms, body) => Ok(TransportResponse {
                status,
                retry_after_ms,
                body: body.to_string().into_bytes(),
            }),
            Step::Hang => std::future::pending().await,
        }
    }
}

#[async_trait::async_trait]
impl Transport for Scripted {
    async fn get_with_headers(
        &self,
        _path: &str,
        _credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Vec<(String, String)>), FlowerError> {
        Ok((self.answer(headers).await?, vec![]))
    }

    async fn post(
        &self,
        _path: &str,
        _body: &[u8],
        _credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        self.answer(&[]).await
    }

    async fn post_with_headers(
        &self,
        _path: &str,
        _body: &[u8],
        _credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Vec<(String, String)>), FlowerError> {
        Ok((self.answer(headers).await?, Vec::new()))
    }

    async fn get(
        &self,
        _path: &str,
        _credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        self.answer(&[]).await
    }

    async fn post_stream(
        &self,
        _path: &str,
        _body: &[u8],
        _credentials: Option<&Credentials>,
    ) -> Result<StreamResponse, FlowerError> {
        Err(FlowerError::Transport("no watches".to_string()))
    }
}

fn client(transport: Arc<Scripted>) -> FlowerClient {
    FlowerClient::builder("http://scripted")
        .transport(transport)
        .build()
        .unwrap()
}

fn fast(attempts: u32) -> RetryPolicy {
    RetryPolicy::default()
        .attempts(attempts)
        .delays(Duration::from_millis(1), Duration::from_millis(5))
}

async fn query(client: &FlowerClient, options: &RequestOptions) -> Result<u64, FlowerError> {
    let result = client
        .query_value("db", "app", "get", &serde_json::json!({}), options)
        .await?;
    Ok(result.value.as_u64().unwrap())
}

#[tokio::test]
async fn refusals_for_load_do_not_spend_attempts() {
    let transport = Scripted::new(vec![
        overloaded(None),
        overloaded(None),
        overloaded(None),
        overloaded(None),
        overloaded(None),
        ok(),
    ]);
    let client = client(transport.clone());
    let options = RequestOptions::default().retry(fast(2));
    assert_eq!(query(&client, &options).await.unwrap(), 7);
    assert_eq!(transport.sent().len(), 6);
    assert_eq!(client.overloads(), 5);
    // Counting them as failures stops at the attempt cap.
    let transport = Scripted::new(vec![overloaded(None), overloaded(None), ok()]);
    let client = self::client(transport.clone());
    let options = RequestOptions::default().retry(fast(2).through_overload(false));
    let error = query(&client, &options).await.unwrap_err();
    assert!(
        matches!(error, FlowerError::RetryFailed { .. }),
        "{error:?}"
    );
    assert_eq!(transport.sent().len(), 2);
    // Other retryable failures still count.
    let transport = Scripted::new(vec![unavailable(), unavailable(), ok()]);
    let client = self::client(transport.clone());
    let error = query(&client, &RequestOptions::default().retry(fast(2)))
        .await
        .unwrap_err();
    assert!(
        matches!(error, FlowerError::RetryFailed { .. }),
        "{error:?}"
    );
    assert_eq!(transport.sent().len(), 2);
    assert_eq!(client.overloads(), 0);
    // The error that ends them keeps the refusal's own reason.
    assert_eq!(error.problem().unwrap().code, "quorum");
    assert_eq!(error.problem().unwrap().message, "no quorum");
}

#[tokio::test(start_paused = true)]
async fn the_overload_budget_ends_refusals_for_load() {
    let transport = Scripted::new(vec![overloaded(None)]);
    let client = client(transport.clone());
    let started = tokio::time::Instant::now();
    let policy = fast(2).overload_budget(Duration::from_secs(10));
    let error = query(&client, &RequestOptions::default().retry(policy))
        .await
        .unwrap_err();
    assert!(
        matches!(error, FlowerError::RetryFailed { .. }),
        "{error:?}"
    );
    let spent = started.elapsed();
    assert!(spent < Duration::from_secs(10), "{spent:?}");
    assert!(spent >= Duration::from_secs(9), "{spent:?}");
    // Many refusals, far past the two attempts.
    assert!(transport.sent().len() > 100, "{}", transport.sent().len());
    // An `until` replaces the budget.
    let transport = Scripted::new(vec![overloaded(None)]);
    let client = self::client(transport);
    let started = tokio::time::Instant::now();
    let policy = fast(2).until(Instant::now() + Duration::from_millis(100));
    query(&client, &RequestOptions::default().retry(policy))
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn retries_wait_at_least_what_the_server_asks() {
    let transport = Scripted::new(vec![overloaded(Some(1_000)), ok()]);
    let client = client(transport.clone());
    assert_eq!(
        query(&client, &RequestOptions::default().retry(fast(3)))
            .await
            .unwrap(),
        7
    );
    let sent = transport.sent();
    let waited = sent[1].0 - sent[0].0;
    assert!(waited >= Duration::from_secs(1), "{waited:?}");
}

#[tokio::test]
async fn a_deadline_bounds_the_whole_call() {
    // Every attempt hangs: the deadline, not the 20 s attempt timeout,
    // ends the call.
    let transport = Scripted::new(vec![Step::Hang]);
    let client = client(transport.clone());
    let started = Instant::now();
    let options = RequestOptions::default()
        .retry(Retry::Default)
        .deadline(Instant::now() + Duration::from_millis(300));
    let error = query(&client, &options).await.unwrap_err();
    assert!(error.is_timeout(), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
    let told: u64 = transport.sent()[0].1.as_deref().unwrap().parse().unwrap();
    assert!((1..=300).contains(&told), "{told}");
    // No retry starts past the deadline.
    let transport = Scripted::new(vec![overloaded(Some(5_000)), ok()]);
    let client = self::client(transport.clone());
    let options = RequestOptions::default()
        .retry(fast(3))
        .deadline(Instant::now() + Duration::from_secs(2));
    let error = query(&client, &options).await.unwrap_err();
    assert!(
        matches!(error, FlowerError::RetryFailed { .. }),
        "{error:?}"
    );
    assert_eq!(transport.sent().len(), 1);
    // A deadline already past sends nothing.
    let transport = Scripted::new(vec![ok()]);
    let client = self::client(transport.clone());
    let options = RequestOptions::default().deadline(Instant::now() - Duration::from_millis(1));
    let error = query(&client, &options).await.unwrap_err();
    assert!(error.is_timeout(), "{error:?}");
    assert!(transport.sent().is_empty());
}

#[tokio::test]
async fn cancellation_drops_the_attempt_and_stops_retrying() {
    // In flight.
    let transport = Scripted::new(vec![Step::Hang]);
    let client = client(transport.clone());
    let cancel = CancellationToken::new();
    let options = RequestOptions::default()
        .retry(Retry::Default)
        .cancel(cancel.clone());
    let trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        trigger.cancel();
    });
    let started = Instant::now();
    let error = query(&client, &options).await.unwrap_err();
    assert!(matches!(error, FlowerError::Cancelled), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(transport.sent().len(), 1);
    // During the wait before a retry.
    let transport = Scripted::new(vec![overloaded(Some(10_000)), ok()]);
    let client = self::client(transport.clone());
    let cancel = CancellationToken::new();
    let options = RequestOptions::default()
        .retry(Retry::Default)
        .cancel(cancel.clone());
    let trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        trigger.cancel();
    });
    let error = query(&client, &options).await.unwrap_err();
    assert!(matches!(error, FlowerError::Cancelled), "{error:?}");
    assert_eq!(transport.sent().len(), 1);
    // Already cancelled: nothing is sent.
    let transport = Scripted::new(vec![ok()]);
    let client = self::client(transport.clone());
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = query(&client, &RequestOptions::default().cancel(cancel))
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Cancelled), "{error:?}");
    assert!(transport.sent().is_empty());
}

#[tokio::test(start_paused = true)]
async fn operator_calls_ride_the_same_retry_loop_and_deadline() {
    use flower_client::testing::{MockReply, MockTransport};
    let mock = MockTransport::scripted([
        MockReply::overloaded(Some(10)),
        MockReply::ok(serde_json::json!(null)),
        MockReply::json(
            200,
            &serde_json::json!({"ok": true, "raft": {"state": "Leader"}}),
        ),
    ]);
    let client = FlowerClient::builder("http://mock")
        .transport(mock.clone())
        .retry(Retry::Policy(fast(1)))
        .build()
        .unwrap();
    // A refusal for load is retried without spending the one attempt.
    let options = RequestOptions::default().timeout(Duration::from_secs(3));
    client.activity(&options).await.unwrap();
    assert_eq!(client.overloads(), 1);
    let raft = client.raft_metrics(&options).await.unwrap();
    assert_eq!(raft["state"], "Leader");
    let requests = mock.requests();
    let paths: Vec<_> = requests.iter().map(|seen| seen.path.as_str()).collect();
    assert_eq!(
        paths,
        ["/v1/admin/activity", "/v1/admin/activity", "/v1/admin/raft"]
    );
    for seen in &requests {
        assert_eq!(seen.header("x-flower-timeout-ms"), Some("3000"), "{seen:?}");
    }
    // Deadlines and cancellation stop operator calls as they stop others.
    mock.push(MockReply::hang());
    let error = client
        .keys(
            "db",
            &RequestOptions::default().deadline(Instant::now() + Duration::from_millis(50)),
        )
        .await
        .unwrap_err();
    assert!(error.is_timeout(), "{error:?}");
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = client
        .node_metrics(&RequestOptions::default().cancel(cancel))
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Cancelled), "{error:?}");
    assert_eq!(mock.requests().len(), 4);
}

#[tokio::test(start_paused = true)]
async fn the_deadline_header_is_one_servers_accept() {
    use flower_client::testing::{MockReply, MockTransport};
    // The client and the server name the header in different crates.
    assert_eq!(
        flower_client::TIMEOUT_HEADER,
        flower_cluster::deadline::TIMEOUT_HEADER
    );
    let mock = MockTransport::scripted([]);
    let client = mock.client();
    let budgets = [
        RequestOptions::default(),
        RequestOptions::default().timeout(Duration::from_micros(1_500)),
        RequestOptions::default().deadline(Instant::now() + Duration::from_micros(300)),
        RequestOptions::default().timeout(Duration::from_secs(400_000)),
    ];
    for options in &budgets {
        mock.push(MockReply::ok(serde_json::json!(1)));
        query(&client, options).await.unwrap();
    }
    for seen in mock.requests() {
        let value = seen.header(flower_client::TIMEOUT_HEADER).unwrap();
        let budget = flower_cluster::deadline::parse(value);
        assert!(budget.is_some_and(|budget| !budget.is_zero()), "{value}");
    }
}
