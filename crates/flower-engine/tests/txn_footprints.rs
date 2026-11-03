//! Durable participant fences, including cached derived sources on replicas
//! with different caches, retry receipts and snapshot-backed queue/timer reads.
use flower_engine::engine::{CallOptions, Engine, MultiStep, MutationOptions, PrepareOutcome};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::GroupStore;
use flower_engine::txn::{Decision, PlacedDb, PreparedRecord, TXN_PREPARED, TxnCommand};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use serde_json::json;

fn v(json: serde_json::Value) -> Value {
    flower_engine::json::parse_value(&serde_json::to_vec(&json).unwrap()).unwrap()
}
fn s(text: &str) -> Value {
    Value::String(text.into())
}
fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let bytes = host.op(
        op as u32 as u8,
        &Value::Array(args).encode_binary().unwrap(),
    );
    decode_envelope(Value::decode_binary(&bytes).unwrap())
}
fn set(host: &mut dyn HostCall, key: &str, value: Value) -> Result<Value, Failure> {
    hc(
        host,
        HostOp::Set,
        vec![
            s(""),
            s("rows"),
            s(&serde_json::to_string(key).unwrap()),
            value,
        ],
    )
}
fn app(
    kind: CallKind,
    _: &str,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    if name == flower_sdk::auth::AUTHORIZE {
        host.read_untracked();
        return Ok(v(json!({"principal":null})));
    }
    let key = args
        .as_object()
        .and_then(|map| map.get("key"))
        .and_then(Value::as_str)
        .unwrap_or("a");
    match (kind, name) {
        (CallKind::Mutation, "put") => set(host, key, Value::Number(7.0)),
        (CallKind::Mutation, "fail") => Err(fail("EXPECTED", "record this failure")),
        (CallKind::Query, "get") | (CallKind::Derived, "source") => hc(
            host,
            HostOp::Get,
            vec![s(""), s("rows"), s(&serde_json::to_string(key).unwrap())],
        ),
        (CallKind::Derived, "nested") => hc(
            host,
            HostOp::GetDerived,
            vec![s(""), s("source"), args.clone()],
        ),
        (CallKind::Query, "cached") | (CallKind::Mutation, "touch") => hc(
            host,
            HostOp::GetDerived,
            vec![s(""), s("nested"), args.clone()],
        ),
        (CallKind::Mutation, "copy") => {
            let result = hc(
                host,
                HostOp::GetDerived,
                vec![s(""), s("nested"), args.clone()],
            )?;
            set(host, "output", result.clone())?;
            Ok(result)
        }
        (CallKind::Mutation, "timer_read") => {
            let result = hc(host, HostOp::TimerGet, vec![s(key)])?;
            set(host, "output", result.clone())?;
            Ok(result)
        }
        (CallKind::Mutation, "timer_scan") => {
            let result = hc(
                host,
                HostOp::TimerScan,
                vec![Value::Null, Value::Number(10.0)],
            )?;
            set(host, "output", result.clone())?;
            Ok(result)
        }
        (CallKind::Mutation, "timer_write") => hc(
            host,
            HostOp::TimerSchedule,
            vec![
                s(key),
                Value::Number(100_000.0),
                s("put"),
                args.clone(),
                Value::Number(3.0),
            ],
        ),
        (CallKind::Mutation, "queue_read") => {
            let result = hc(
                host,
                HostOp::QueueStats,
                vec![s(key), Value::Null, Value::Null],
            )?;
            set(host, "output", result.clone())?;
            Ok(result)
        }
        (CallKind::Mutation, "enqueue") => hc(
            host,
            HostOp::Enqueue,
            vec![s(key), Value::Null, Value::Number(0.0), Value::Number(3.0)],
        ),
        (CallKind::Mutation, "untracked") => {
            host.read_untracked();
            set(host, "output", Value::Null)
        }
        (CallKind::Mutation, "history") => {
            let result = hc(
                host,
                HostOp::History,
                vec![
                    s(""),
                    s("rows"),
                    s(&serde_json::to_string(key).unwrap()),
                    Value::Number(10.0),
                ],
            )?;
            set(host, "output", result)
        }
        _ => Err(fail("UNKNOWN_METHOD", name)),
    }
}
fn options() -> MutationOptions {
    MutationOptions {
        call: CallOptions {
            timestamp_ms: Some(1000),
            ..CallOptions::default()
        },
        ..MutationOptions::default()
    }
}
struct Member {
    engine: Engine,
    position: u64,
    _dir: tempfile::TempDir,
}
impl Member {
    fn new(authorize: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(GroupStore::create(dir.path().join("db")).unwrap());
        let definitions: Vec<_> = [
            ("put", "mutation"),
            ("fail", "mutation"),
            ("copy", "mutation"),
            ("touch", "mutation"),
            ("timer_read", "mutation"),
            ("timer_scan", "mutation"),
            ("timer_write", "mutation"),
            ("queue_read", "mutation"),
            ("enqueue", "mutation"),
            ("untracked", "mutation"),
            ("history", "mutation"),
            ("source", "derived"),
            ("nested", "derived"),
            ("get", "query"),
            ("cached", "query"),
        ]
        .into_iter()
        .map(|(name, kind)| json!({"name":name,"kind":kind,"args":{"type":"json"}}))
        .collect();
        let manifest = v(json!({
            "collections":[{"name":"rows","key":{"type":"string"},"value":{"type":"json"},"indexes":[],"references":[]}],
            "definitions":definitions,"triggers":[],"http":[],"authorize":authorize,
            "queues":[{"name":"jobs"},{"name":"other"}]
        }));
        engine
            .deploy(
                b"db",
                "app",
                [1; 32],
                b"",
                &manifest,
                Box::new(ClosureRuntime::new(app)),
            )
            .unwrap();
        Self {
            engine,
            position: 0,
            _dir: dir,
        }
    }
    fn patch(&mut self, method: &str, key: &str, options: &MutationOptions) -> Vec<u8> {
        match self
            .engine
            .prepare_mutation(b"db", "app", method, &v(json!({"key":key})), options)
            .unwrap()
        {
            PrepareOutcome::Patch(prepared) => prepared.patch,
            other => panic!("expected patch: {other:?}"),
        }
    }
    fn reopen(self) -> Self {
        let Self {
            engine,
            position,
            _dir,
        } = self;
        drop(engine);
        let mut engine = Engine::new(GroupStore::open(_dir.path().join("db")).unwrap());
        engine
            .reload_module(b"db", "app", Box::new(ClosureRuntime::new(app)))
            .unwrap();
        Self {
            engine,
            position,
            _dir,
        }
    }
    fn apply(&mut self, patch: &[u8]) -> flower_engine::engine::ApplyOutcome {
        self.position += 1;
        self.engine
            .apply_patch_at(patch, self.position, 1000)
            .unwrap()
    }
    fn command(&mut self, command: &TxnCommand) {
        self.position += 1;
        let staged = self.engine.store().begin_staged().unwrap();
        self.engine
            .commit_txn(&staged, command, self.position, 1000, &mut |_| true)
            .unwrap()
            .0
            .unwrap();
        staged.commit().unwrap();
    }
    fn prepare(&mut self, method: &str, key: &str) -> TxnCommand {
        let (prepared, reads) = self
            .engine
            .prepare_transaction_multi_locked(
                &[MultiStep {
                    db: b"db".to_vec(),
                    module: "app".into(),
                    method: method.into(),
                    args: v(json!({"key":key})),
                }],
                &options(),
            )
            .unwrap();
        let PrepareOutcome::Patch(prepared) = prepared else {
            panic!("expected participant patch")
        };
        let command = TxnCommand::Prepare {
            record: PreparedRecord {
                id: "held".into(),
                coordinator: "root".into(),
                coordinator_uid: 0,
                dbs: vec![PlacedDb {
                    db: "db".into(),
                    epoch: 0,
                    steps: vec![0],
                }],
                patch: flower_engine::hex::encode(&prepared.patch),
                results: vec![],
                prepared_ms: 0,
                reads: Some(Box::new(reads)),
            },
        };
        // All assertions exercise the persisted wire representation.
        serde_json::from_slice(&serde_json::to_vec(&command).unwrap()).unwrap()
    }
    fn finish(&mut self) {
        self.command(&TxnCommand::Finish {
            id: "held".into(),
            decision: Decision::Commit,
        });
    }
    fn query(&mut self, method: &str, key: &str) -> Value {
        self.engine
            .invoke_query(
                b"db",
                "app",
                method,
                &v(json!({"key":key})),
                &options().call,
            )
            .unwrap()
    }
}

#[test]
fn cached_transitive_sources_and_receipts_commute_on_a_cold_restarted_replica() {
    let mut leader = Member::new(false);
    let mut follower = Member::new(false);
    let initial = leader.patch("put", "a", &options());
    leader.apply(&initial).result.unwrap();
    follower.apply(&initial).result.unwrap();
    // Only the leader populates its nested derived cache.
    assert_eq!(leader.query("cached", "a"), Value::Number(7.0));
    let prepare = leader.prepare("copy", "a");
    leader.command(&prepare);
    follower.command(&prepare);
    follower = follower.reopen();
    let retry = MutationOptions {
        idempotency_key: Some(b"independent".to_vec()),
        ..options()
    };
    let independent = leader.patch("put", "b", &retry);
    assert!(
        !flower_engine::patch::decode_patch(&independent)
            .unwrap()
            .receipts
            .is_empty()
    );
    for member in [&mut leader, &mut follower] {
        member.apply(&independent).result.unwrap();
        assert!(member.apply(&independent).duplicate);
    }
    let failure = leader.patch(
        "fail",
        "unused",
        &MutationOptions {
            idempotency_key: Some(b"failed-independent".to_vec()),
            ..options()
        },
    );
    assert!(
        !flower_engine::patch::decode_patch(&failure)
            .unwrap()
            .receipts
            .is_empty()
    );
    for member in [&mut leader, &mut follower] {
        assert_eq!(member.apply(&failure).result.unwrap_err().code, "EXPECTED");
        assert!(member.apply(&failure).duplicate);
    }
    let conflict = leader.patch("put", "a", &options());
    for member in [&mut leader, &mut follower] {
        assert_eq!(
            member.apply(&conflict).result.unwrap_err().code,
            TXN_PREPARED
        );
        member.finish();
        assert_eq!(member.query("get", "output"), Value::Number(7.0));
        assert_eq!(member.query("get", "b"), Value::Number(7.0));
    }
}

#[test]
fn a_cold_derived_preparation_allows_unrelated_derived_writes() {
    let mut member = Member::new(false);
    let prepare = member.prepare("copy", "a");
    member.command(&prepare);
    // Computing b's caches has no dependency on a or the output row.
    let bytes = member.patch("touch", "b", &options());
    let patch = flower_engine::patch::decode_patch(&bytes).unwrap();
    assert!(patch.records.is_empty());
    assert!(!patch.derived.is_empty());
    member.apply(&bytes).result.unwrap();
    let conflict = member.patch("put", "a", &options());
    assert_eq!(
        member.apply(&conflict).result.unwrap_err().code,
        TXN_PREPARED
    );
    member.finish();
    assert_eq!(member.query("get", "output"), Value::Null);
}

#[test]
fn timer_and_queue_reads_fence_their_rows_and_ranges_only() {
    for (read, watched, write, unrelated) in [
        ("timer_read", "watched", "timer_write", Some("other")),
        ("timer_scan", "unused", "timer_write", None),
        ("queue_read", "jobs", "enqueue", Some("other")),
    ] {
        let mut member = Member::new(false);
        let prepare = member.prepare(read, watched);
        member.command(&prepare);
        let independent = member.patch("put", "b", &options());
        member.apply(&independent).result.unwrap();
        if let Some(other) = unrelated {
            let independent = member.patch(write, other, &options());
            member.apply(&independent).result.unwrap();
        }
        let conflict = member.patch(write, watched, &options());
        assert_eq!(
            member.apply(&conflict).result.unwrap_err().code,
            TXN_PREPARED,
            "{read}"
        );
        member.finish();
        assert_eq!(member.query("get", "b"), Value::Number(7.0));
    }
}

#[test]
fn untracked_reads_in_both_mutations_and_authorization_keep_the_fence() {
    for (authorize, method) in [(false, "untracked"), (false, "history"), (true, "put")] {
        let mut member = Member::new(authorize);
        let prepare = member.prepare(method, "a");
        member.command(&prepare);
        let independent = member.patch("put", "b", &options());
        assert_eq!(
            member.apply(&independent).result.unwrap_err().code,
            TXN_PREPARED
        );
        member.finish();
    }
}
