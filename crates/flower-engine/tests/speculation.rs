//! Speculative preparation: a mutation prepared on a snapshot of a
//! writer's staged state carries a certificate of every row it read;
//! it holds after later staged writes exactly when none of them touches
//! one, and a held candidate's patch, moved to where it lands, is the
//! one preparing it there would have produced.

use std::sync::Arc;

use flower_engine::batch::BatchItem;
use flower_engine::engine::{CallOptions, Engine, MutationOptions, PrepareOutcome};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::speculation::{PrepareMutation, Verdict, rebase};
use flower_engine::storage::{GroupStore, Overlay, SnapshotTable, StagedWrite};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

const CLOCK: u64 = 1_700_000_000_000;

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn n(num: f64) -> Value {
    Value::Number(num)
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

fn k(name: &str) -> String {
    format!("\"{name}\"")
}

fn schema(kind: &str) -> Value {
    obj(vec![("type", s(kind))])
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", schema("json")),
    ])
}

/// `tasks`, keyed by string, `{title, owner}`, indexed by owner; a
/// derived `title` of one task; mutations reading a task, the whole
/// collection, an owner's index range or a derived title, each writing
/// what it read into another task.
fn manifest() -> Value {
    let field = |name: &str| {
        obj(vec![
            ("name", s(name)),
            ("schema", schema("string")),
            ("optional", Value::Bool(false)),
        ])
    };
    let tasks = obj(vec![
        ("name", s("tasks")),
        ("key", schema("string")),
        (
            "value",
            obj(vec![
                ("type", s("object")),
                ("fields", arr(vec![field("title"), field("owner")])),
            ]),
        ),
        (
            "indexes",
            arr(vec![obj(vec![
                ("name", s("by_owner")),
                ("fields", arr(vec![s("owner")])),
            ])]),
        ),
        ("references", arr(vec![])),
    ]);
    obj(vec![
        ("collections", arr(vec![tasks])),
        (
            "definitions",
            arr(vec![
                definition("put", "mutation"),
                definition("copy", "mutation"),
                definition("count", "mutation"),
                definition("owned", "mutation"),
                definition("derived", "mutation"),
                definition("warm", "mutation"),
                definition("title", "derived"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn arg(args: &Value, name: &str) -> Result<String, Failure> {
    match args {
        Value::Object(map) => match map.get(name) {
            Some(Value::String(text)) => Ok(text.clone()),
            _ => Err(fail("TYPE_MISMATCH", format!("needs {name}"))),
        },
        _ => Err(fail("TYPE_MISMATCH", "takes an object")),
    }
}

fn title(host: &mut dyn HostCall, key: &str) -> Result<String, Failure> {
    match hc(host, HostOp::Get, vec![s(""), s("tasks"), s(&k(key))])? {
        Value::Object(map) => match map.get("title") {
            Some(Value::String(title)) => Ok(title.clone()),
            _ => Ok(String::new()),
        },
        _ => Ok("none".to_string()),
    }
}

fn rows(page: &Value) -> usize {
    match page {
        Value::Object(map) => match map.get("rows") {
            Some(Value::Array(rows)) => rows.len(),
            _ => 0,
        },
        _ => 0,
    }
}

fn write(host: &mut dyn HostCall, key: &str, title: &str, owner: &str) -> Result<Value, Failure> {
    hc(
        host,
        HostOp::Set,
        vec![
            s(""),
            s("tasks"),
            s(&k(key)),
            obj(vec![("title", s(title)), ("owner", s(owner))]),
        ],
    )?;
    Ok(s(title))
}

fn runtime() -> Box<dyn flower_engine::runtime::Runtime> {
    Box::new(ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let key = arg(args, "key")?;
                    let owner = arg(args, "owner")?;
                    write(host, &key, &key, &owner)
                }
                (CallKind::Mutation, "copy") => {
                    let from = title(host, &arg(args, "from")?)?;
                    write(host, &arg(args, "to")?, &from, "w")
                }
                (CallKind::Mutation, "count") => {
                    let page = hc(
                        host,
                        HostOp::Scan,
                        vec![s(""), s("tasks"), n(100.0), Value::Null],
                    )?;
                    write(host, &arg(args, "to")?, &rows(&page).to_string(), "w")
                }
                (CallKind::Mutation, "owned") => {
                    let bounds = obj(vec![
                        ("prefix", arr(vec![s(&k(&arg(args, "owner")?))])),
                        ("gt", Value::Null),
                        ("gte", Value::Null),
                        ("lt", Value::Null),
                        ("lte", Value::Null),
                        ("limit", n(100.0)),
                        ("after", Value::Null),
                        ("reverse", Value::Bool(false)),
                    ]);
                    let page = hc(
                        host,
                        HostOp::Range,
                        vec![s(""), s("tasks"), s("by_owner"), bounds],
                    )?;
                    write(host, &arg(args, "to")?, &rows(&page).to_string(), "w")
                }
                (CallKind::Mutation, "derived") => {
                    let derived = hc(
                        host,
                        HostOp::GetDerived,
                        vec![
                            s("app"),
                            s("title"),
                            obj(vec![("key", s(&arg(args, "key")?))]),
                        ],
                    )?;
                    let text = match derived {
                        Value::String(text) => text,
                        other => format!("{other:?}"),
                    };
                    write(host, &arg(args, "to")?, &text, "w")
                }
                // Compute a derived title and keep it committed, writing
                // nothing else.
                (CallKind::Mutation, "warm") => hc(
                    host,
                    HostOp::GetDerived,
                    vec![
                        s("app"),
                        s("title"),
                        obj(vec![("key", s(&arg(args, "key")?))]),
                    ],
                ),
                (CallKind::Derived, "title") => Ok(s(&title(host, &arg(args, "key")?)?)),
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    ))
}

struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn fixture(test: &str) -> Fixture {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-speculation-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest(), runtime())
        .unwrap();
    let mut fixture = Fixture { engine, path };
    for (key, owner) in [("t1", "a"), ("t2", "a"), ("t3", "b")] {
        fixture.commit("put", obj(vec![("key", s(key)), ("owner", s(owner))]));
    }
    fixture
}

fn options() -> MutationOptions {
    MutationOptions {
        call: CallOptions {
            timestamp_ms: Some(CLOCK),
            ..CallOptions::default()
        },
        ..MutationOptions::default()
    }
}

/// A call: method and arguments.
type Call = (&'static str, Value);

/// What a preparation decided: the patch it proposes, or its answer.
#[derive(Debug, PartialEq)]
enum Decided {
    Patch(Vec<u8>),
    Answer(String),
}

fn decided(outcome: Result<PrepareOutcome, Failure>) -> Decided {
    match outcome {
        Ok(PrepareOutcome::Patch(prepared)) => Decided::Patch(prepared.patch),
        Ok(other) => Decided::Answer(format!("{other:?}")),
        Err(failure) => Decided::Answer(format!("{failure:?}")),
    }
}

impl Fixture {
    /// Commit a call locally.
    fn commit(&mut self, name: &str, args: Value) {
        self.engine
            .invoke_mutation(b"db", "app", name, &args, &options())
            .unwrap();
    }

    /// A scratch staging logging its writes.
    fn staging(&self) -> StagedWrite {
        let staged = self.engine.store().begin_scratch().unwrap();
        staged.log_writes();
        staged
    }

    /// Prepare `call` in its turn on `staged`, and stage what it
    /// proposes at `position`.
    fn serial(&mut self, staged: &StagedWrite, call: &Call, position: u64) -> Decided {
        let outcome = self.engine.on_staged(staged, |engine| {
            PrepareMutation::prepare_mutation(engine, b"db", "app", call.0, &call.1, &options())
        });
        let decided = decided(outcome);
        self.stage(staged, &decided, position);
        decided
    }

    fn stage(&self, staged: &StagedWrite, decided: &Decided, position: u64) {
        if let Decided::Patch(patch) = decided {
            let item = BatchItem::Patch {
                db: b"db".to_vec(),
                patch: patch.clone(),
            };
            self.engine
                .commit_batch(staged, &[item], position, CLOCK)
                .unwrap();
        }
    }

    /// Prepare `calls` speculatively on the staged state as it stands.
    fn speculate(
        &self,
        staged: &StagedWrite,
        overlay: &mut Overlay,
        mark: &mut usize,
        calls: &[Call],
    ) -> Vec<(Decided, flower_engine::speculation::MutationCertificate)> {
        *mark = staged.extend_overlay(overlay, *mark).unwrap();
        let shared = Arc::new(std::mem::take(overlay));
        let speculation = self.engine.speculation(shared.clone(), None);
        let prepared = calls
            .iter()
            .map(|(name, args)| {
                let mut speculator = speculation.speculator();
                let outcome = speculator.prepare_mutation(b"db", "app", name, args, &options());
                (decided(outcome), speculator.certificate())
            })
            .collect();
        drop(speculation);
        *overlay = Arc::try_unwrap(shared).expect("no speculation outlives its wave");
        prepared
    }
}

fn verdict(
    staged: &StagedWrite,
    since: usize,
    certificate: &flower_engine::speculation::MutationCertificate,
) -> Verdict {
    staged.logged(since, |writes| certificate.verdict(writes))
}

fn tables(verdict: &Verdict) -> Vec<SnapshotTable> {
    match verdict {
        Verdict::Conflicts(rows) => rows.iter().map(|(table, _)| *table).collect(),
        other => panic!("expected conflicts, got {other:?}"),
    }
}

/// Speculate `call`, then stage `earlier` before it: the verdict.
fn after(test: &str, call: Call, earlier: Call) -> Verdict {
    let mut fixture = fixture(test);
    fixture.commit("warm", obj(vec![("key", s("t1"))]));
    let staged = fixture.staging();
    let (mut overlay, mut mark) = (Overlay::default(), 0);
    let mut speculated = fixture.speculate(&staged, &mut overlay, &mut mark, &[call]);
    let (candidate, certificate) = speculated.pop().unwrap();
    match candidate {
        Decided::Patch(patch) => {
            let patch = flower_engine::patch::decode_patch(&patch).unwrap();
            assert!(
                !patch.failed,
                "{test}: the candidate failed: {}",
                String::from_utf8_lossy(&patch.result)
            );
        }
        other => panic!("{test}: the candidate proposed nothing: {other:?}"),
    }
    fixture.serial(&staged, &earlier, 1 << 40);
    verdict(&staged, mark, &certificate)
}

fn put(key: &str, owner: &str) -> Call {
    ("put", obj(vec![("key", s(key)), ("owner", s(owner))]))
}

#[test]
fn certificates_catch_reads_of_keys_present_or_absent() {
    let copy = ("copy", obj(vec![("from", s("t1")), ("to", s("x"))]));
    assert!(tables(&after("key", copy.clone(), put("t1", "a"))).contains(&SnapshotTable::Records));
    assert_eq!(after("key-other", copy, put("t9", "a")), Verdict::Holds);
    let absent = ("copy", obj(vec![("from", s("t7")), ("to", s("x"))]));
    assert!(
        tables(&after("absent", absent, put("t7", "z"))).contains(&SnapshotTable::Records),
        "a row inserted where a read found none"
    );
}

#[test]
fn certificates_catch_rows_inserted_into_scans() {
    let count = ("count", obj(vec![("to", s("x"))]));
    assert!(tables(&after("scan", count, put("t5", "c"))).contains(&SnapshotTable::Records));
}

#[test]
fn certificates_catch_index_ranges_but_not_entries_outside_them() {
    let owned = ("owned", obj(vec![("owner", s("a")), ("to", s("x"))]));
    // A new task of owner `a` lands in the range read.
    assert!(
        tables(&after("index", owned.clone(), put("t5", "a")))
            .contains(&SnapshotTable::IndexEntries)
    );
    // One of owner `q` lands outside it, and the read touched no row.
    assert_eq!(after("index-other", owned, put("t5", "q")), Verdict::Holds);
}

#[test]
fn certificates_catch_derived_values_through_their_outcomes() {
    // `warm` committed `title(t1)`: the candidate reads the outcome, not
    // the task, and a write to the task drops the outcome.
    let derived = ("derived", obj(vec![("key", s("t1")), ("to", s("x"))]));
    let verdict = after("derived", derived.clone(), put("t1", "z"));
    assert!(
        tables(&verdict).contains(&SnapshotTable::Derived)
            || tables(&verdict).contains(&SnapshotTable::DependentEdges),
        "{verdict:?}"
    );
    assert_eq!(
        after("derived-other", derived, put("t2", "z")),
        Verdict::Holds
    );
}

/// A tiny deterministic generator.
struct Random(u64);

impl Random {
    fn next(&mut self, below: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % below
    }

    fn key(&mut self) -> Value {
        s(&format!("t{}", self.next(6)))
    }

    fn call(&mut self) -> Call {
        match self.next(5) {
            0 => (
                "put",
                obj(vec![
                    ("key", self.key()),
                    ("owner", s(["a", "b", "c"][self.next(3) as usize])),
                ]),
            ),
            1 => ("copy", obj(vec![("from", self.key()), ("to", self.key())])),
            2 => ("count", obj(vec![("to", self.key())])),
            3 => (
                "owned",
                obj(vec![
                    ("owner", s(["a", "b"][self.next(2) as usize])),
                    ("to", self.key()),
                ]),
            ),
            _ => (
                "derived",
                obj(vec![("key", self.key()), ("to", self.key())]),
            ),
        }
    }
}

/// Random calls in random waves: every candidate that holds is adopted
/// with its patch moved to where it lands, every other one prepared
/// again in its turn, and the run proposes byte for byte what preparing
/// every call in its turn proposes.
#[test]
fn speculation_proposes_exactly_what_serial_preparation_does() {
    let (mut reused, mut prepared_again) = (0, 0);
    for seed in 1..=12u64 {
        let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let calls: Vec<Call> = (0..40).map(|_| random.call()).collect();
        let mut fixture = fixture(&format!("random-{seed}"));
        fixture.commit("warm", obj(vec![("key", s("t2"))]));
        // Serially, each on what the ones before staged.
        let staged = fixture.staging();
        let serial: Vec<Decided> = calls
            .iter()
            .enumerate()
            .map(|(index, call)| fixture.serial(&staged, call, (1 << 40) + index as u64))
            .collect();
        drop(staged);
        // Speculatively, in waves of 1 to 8.
        let staged = fixture.staging();
        let (mut overlay, mut mark) = (Overlay::default(), 0);
        let mut speculative = Vec::new();
        let mut next = 0;
        while next < calls.len() {
            let end = (next + 1 + random.next(8) as usize).min(calls.len());
            let wave = fixture.speculate(&staged, &mut overlay, &mut mark, &calls[next..end]);
            for (offset, (candidate, certificate)) in wave.into_iter().enumerate() {
                let index = next + offset;
                let position = (1 << 40) + index as u64;
                let decided = match verdict(&staged, mark, &certificate) {
                    Verdict::Holds => {
                        reused += 1;
                        let decided = match candidate {
                            Decided::Patch(patch) => {
                                let item = BatchItem::Patch {
                                    db: b"db".to_vec(),
                                    patch,
                                };
                                match rebase(&staged.read(), item).unwrap() {
                                    BatchItem::Patch { patch, .. } => Decided::Patch(patch),
                                    other => panic!("rebased into {other:?}"),
                                }
                            }
                            answer => answer,
                        };
                        fixture.stage(&staged, &decided, position);
                        decided
                    }
                    _ => {
                        prepared_again += 1;
                        fixture.serial(&staged, &calls[index], position)
                    }
                };
                assert_eq!(
                    decided, serial[index],
                    "seed {seed}, call {index} ({:?}) proposed otherwise",
                    calls[index]
                );
                speculative.push(decided);
            }
            next = end;
        }
        assert_eq!(speculative, serial);
    }
    assert!(
        reused > 0 && prepared_again > 0,
        "{reused} reused, {prepared_again} again"
    );
}
