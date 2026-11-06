//! Declarative access policies at the engine: hidden rows read as absent
//! and drop out of pages before limits and cursors, unreadable fields are
//! redacted and survive callers' writes, denied writes fail, unseen
//! deletes skip, lookups follow other rows, derived values have their own
//! rules, and definer code (triggers, timers) writes past policies but
//! loses those rights in other modules.
//!
//! `docs` belong to their `owner`; members of a doc's `team` (rows of
//! `members` keyed `[team, subject]`) and admins read them too; only the
//! owner reads `secret`; owners and editors update them.

mod fixtures;

use fixtures::{arr, hc, k, n, obj, s};

use flower_engine::engine::{CallOptions, Engine, FirePrepare, MutationOptions};
use flower_engine::history::{HistoryProvider, Version};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Principal, Value, fail};

fn json(text: &str) -> Value {
    flower_engine::json::parse_value(text.as_bytes()).unwrap()
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

fn text(value: &Value, name: &str) -> String {
    field(value, name).as_str().unwrap_or_default().to_string()
}

/// Page keys from a scan or range page.
fn page_keys(page: &Value) -> Value {
    let rows = field(page, "rows").as_array().cloned().unwrap_or_default();
    arr(rows
        .into_iter()
        .map(|row| row.as_array().unwrap()[0].clone())
        .collect())
}

fn app(
    module: &'static str,
) -> impl Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> Result<Value, Failure> + Send + Sync
{
    move |kind, _module, name, args, host| match (kind, name) {
        (CallKind::Query, "get") => hc(
            host,
            HostOp::Get,
            vec![
                s(""),
                field(args, "coll").clone(),
                field(args, "key").clone(),
            ],
        ),
        (CallKind::Query, "scan") => {
            let page = hc(
                host,
                HostOp::Scan,
                vec![
                    s(""),
                    field(args, "coll").clone(),
                    field(args, "limit").clone(),
                    field(args, "after").clone(),
                ],
            )?;
            Ok(obj(vec![
                ("keys", page_keys(&page)),
                ("rows", field(&page, "rows").clone()),
                ("cursor", field(&page, "cursor").clone()),
            ]))
        }
        (CallKind::Query, "range") => {
            let bounds = obj(vec![
                ("prefix", arr(vec![])),
                ("gt", Value::Null),
                ("gte", Value::Null),
                ("lt", Value::Null),
                ("lte", Value::Null),
                ("limit", field(args, "limit").clone()),
                ("after", field(args, "after").clone()),
                ("reverse", Value::Bool(false)),
            ]);
            let page = hc(
                host,
                HostOp::Range,
                vec![s(""), s("docs"), field(args, "index").clone(), bounds],
            )?;
            Ok(obj(vec![
                ("keys", page_keys(&page)),
                ("cursor", field(&page, "cursor").clone()),
            ]))
        }
        (CallKind::Query, "count") => hc(
            host,
            HostOp::GetDerived,
            vec![s(""), s("doc_count"), Value::Null],
        ),
        (CallKind::Query, "history") => hc(
            host,
            HostOp::History,
            vec![s(""), s("docs"), field(args, "key").clone(), n(10.0)],
        ),
        (CallKind::Derived, "doc_count") => {
            let page = hc(
                host,
                HostOp::Scan,
                vec![s(""), s("docs"), n(1000.0), Value::Null],
            )?;
            Ok(n(field(&page, "rows").as_array().unwrap().len() as f64))
        }
        (CallKind::Mutation, "put") => hc(
            host,
            HostOp::Set,
            vec![
                s(""),
                field(args, "coll").clone(),
                field(args, "key").clone(),
                field(args, "value").clone(),
            ],
        ),
        (CallKind::Mutation, "del") => hc(
            host,
            HostOp::Delete,
            vec![
                s(""),
                field(args, "coll").clone(),
                field(args, "key").clone(),
            ],
        ),
        (CallKind::Mutation, "cas") => hc(
            host,
            HostOp::DeleteIfMatch,
            vec![
                s(""),
                s("docs"),
                field(args, "key").clone(),
                field(args, "expected").clone(),
            ],
        ),
        (CallKind::Mutation, "retitle") => {
            // Read-modify-write through the caller's view of the row.
            let key = field(args, "key").clone();
            let mut row = hc(host, HostOp::Get, vec![s(""), s("docs"), key.clone()])?;
            if let Value::Object(map) = &mut row {
                map.insert("title".to_string(), field(args, "title").clone());
            }
            hc(host, HostOp::Set, vec![s(""), s("docs"), key, row])
        }
        // Trigger on docs: definer code writing a deny-all collection.
        (CallKind::Mutation, "audit") => hc(
            host,
            HostOp::Set,
            vec![
                s(""),
                s("audits"),
                s(&k("last")),
                field(args, "key").clone(),
            ],
        ),
        // Trigger on log: definer code calling into another module.
        (CallKind::Mutation, "relay") => hc(
            host,
            HostOp::Call,
            vec![s("vault"), s("stash"), Value::Null, Value::Bool(false)],
        ),
        (CallKind::Mutation, "stash") if module == "vault" => hc(
            host,
            HostOp::Set,
            vec![s(""), s("secrets"), s(&k("x")), s("stashed")],
        ),
        (CallKind::Mutation, "remind") => hc(
            host,
            HostOp::TimerSchedule,
            vec![s("r1"), n(0.0), s("stamp"), Value::Null, n(3.0)],
        ),
        (CallKind::Mutation, "stamp") => hc(
            host,
            HostOp::Set,
            vec![s(""), s("audits"), s(&k("timer")), s("fired")],
        ),
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    }
}

const DOCS_POLICY: &str = r#"{
    "read": {"any": [
        {"eq": [{"ref": ["row", "owner"]}, {"ref": ["principal", "subject"]}]},
        {"stored": ["members", {"list": [{"ref": ["row", "team"]}, {"ref": ["principal", "subject"]}]}]},
        {"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "admin"}]}
    ]},
    "insert": {"eq": [{"ref": ["next", "owner"]}, {"ref": ["principal", "subject"]}]},
    "update": {"all": [
        {"any": [
            {"eq": [{"ref": ["row", "owner"]}, {"ref": ["principal", "subject"]}]},
            {"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "editor"}]}
        ]},
        {"eq": [{"ref": ["next", "owner"]}, {"ref": ["row", "owner"]}]}
    ]},
    "delete": {"eq": [{"ref": ["row", "owner"]}, {"ref": ["principal", "subject"]}]},
    "fields": {
        "secret": {"read": {"eq": [{"ref": ["row", "owner"]}, {"ref": ["principal", "subject"]}]}}
    }
}"#;

const NOTES_POLICY: &str = r#"{
    "read": {"readable": ["docs", {"ref": ["row", "doc"]}]},
    "insert": {"const": true}
}"#;

const DENY_ALL: &str = r#"{}"#;

fn collection(name: &str, key: &str, access: Option<&str>, indexes: Value) -> Value {
    let mut pairs = vec![
        ("name", s(name)),
        ("key", obj(vec![("type", s(key))])),
        ("value", obj(vec![("type", s("json"))])),
        ("indexes", indexes),
        ("references", arr(vec![])),
    ];
    if let Some(access) = access {
        pairs.push(("access", json(access)));
    }
    obj(pairs)
}

fn index(name: &str, field: &str) -> Value {
    obj(vec![("name", s(name)), ("fields", arr(vec![s(field)]))])
}

fn definition(name: &str, kind: &str, access: Option<&str>) -> Value {
    let mut pairs = vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", obj(vec![("type", s("json"))])),
    ];
    if let Some(access) = access {
        pairs.push(("access", json(access)));
    }
    obj(pairs)
}

fn app_manifest() -> Value {
    obj(vec![
        (
            "collections",
            arr(vec![
                collection(
                    "docs",
                    "string",
                    Some(DOCS_POLICY),
                    arr(vec![
                        index("by_title", "title"),
                        index("by_secret", "secret"),
                    ]),
                ),
                collection("members", "json", None, arr(vec![])),
                collection("notes", "string", Some(NOTES_POLICY), arr(vec![])),
                collection("audits", "string", Some(DENY_ALL), arr(vec![])),
                collection("log", "string", None, arr(vec![])),
            ]),
        ),
        (
            "definitions",
            arr(vec![
                definition("get", "query", None),
                definition("scan", "query", None),
                definition("range", "query", None),
                definition("count", "query", None),
                definition("history", "query", None),
                definition(
                    "doc_count",
                    "derived",
                    Some(
                        r#"{"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "auditor"}]}"#,
                    ),
                ),
                definition("put", "mutation", None),
                definition("del", "mutation", None),
                definition("cas", "mutation", None),
                definition("retitle", "mutation", None),
                definition("audit", "mutation", None),
                definition("relay", "mutation", None),
                definition("remind", "mutation", None),
                definition("stamp", "mutation", None),
            ]),
        ),
        (
            "triggers",
            arr(vec![
                obj(vec![("name", s("audit")), ("source", s("docs"))]),
                obj(vec![("name", s("relay")), ("source", s("log"))]),
            ]),
        ),
        ("http", arr(vec![])),
    ])
}

fn vault_manifest() -> Value {
    obj(vec![
        (
            "collections",
            arr(vec![collection(
                "secrets",
                "string",
                Some(DENY_ALL),
                arr(vec![]),
            )]),
        ),
        (
            "definitions",
            arr(vec![definition("stash", "mutation", None)]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
        // The app's trigger calls `stash`: other modules reach exports only.
        (
            "exports",
            obj(vec![
                ("collections", arr(vec![])),
                ("derived", arr(vec![])),
                ("methods", arr(vec![s("stash")])),
            ]),
        ),
    ])
}

/// Retained versions of every row: newest first, a deletion of Ada's
/// version, then Bob's original.
struct FixedHistory;

impl HistoryProvider for FixedHistory {
    fn history(
        &self,
        _db: &[u8],
        _collection: u32,
        _key: &[u8],
        _limit: u32,
    ) -> flower_engine::error::Result<Vec<Version>> {
        Ok(vec![
            Version {
                revision: 3,
                value: None,
            },
            Version {
                revision: 2,
                value: Some(br#"{"owner":"ada","secret":"s","title":"b"}"#.to_vec()),
            },
            Version {
                revision: 1,
                value: Some(br#"{"owner":"bob","title":"a"}"#.to_vec()),
            },
        ])
    }
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
    path.push(format!("flower-access-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut engine =
        Engine::new(GroupStore::create(&path).unwrap()).with_history(Box::new(FixedHistory));
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            b"",
            &app_manifest(),
            Box::new(ClosureRuntime::new(app("app"))),
        )
        .unwrap();
    engine
        .deploy(
            b"db",
            "vault",
            [8u8; 32],
            b"",
            &vault_manifest(),
            Box::new(ClosureRuntime::new(app("vault"))),
        )
        .unwrap();
    Fixture { engine, path }
}

fn as_(subject: &str, role: &str) -> CallOptions {
    CallOptions {
        principal: Some(
            Principal::new(
                subject.to_string(),
                None,
                Some(obj(vec![("role", s(role))])),
            )
            .unwrap(),
        ),
        ..CallOptions::default()
    }
}

impl Fixture {
    fn query(&self, who: &CallOptions, name: &str, args: Value) -> Result<Value, Failure> {
        self.engine.invoke_query(b"db", "app", name, &args, who)
    }

    fn mutate(&mut self, who: &CallOptions, name: &str, args: Value) -> Result<Value, Failure> {
        let options = MutationOptions {
            call: who.clone(),
            ..MutationOptions::default()
        };
        self.engine
            .invoke_mutation(b"db", "app", name, &args, &options)
    }

    fn put(
        &mut self,
        who: &CallOptions,
        coll: &str,
        key: &str,
        value: Value,
    ) -> Result<Value, Failure> {
        self.mutate(
            who,
            "put",
            obj(vec![("coll", s(coll)), ("key", s(key)), ("value", value)]),
        )
    }

    fn get(&self, who: &CallOptions, coll: &str, key: &str) -> Value {
        self.query(who, "get", obj(vec![("coll", s(coll)), ("key", s(key))]))
            .unwrap()
    }

    /// Thirty docs, `d00`…`d29`, all Bob's but `d00`, `d15` and `d29`.
    fn seed(&mut self) {
        for index in 0..30 {
            let owner = if matches!(index, 0 | 15 | 29) {
                "ada"
            } else {
                "bob"
            };
            let key = k(&format!("d{index:02}"));
            let doc = obj(vec![
                ("owner", s(owner)),
                ("title", s(&format!("t{index:02}"))),
                ("secret", s(&format!("s{index:02}"))),
            ]);
            self.put(&as_(owner, "user"), "docs", &key, doc).unwrap();
        }
    }
}

#[test]
fn hidden_rows_read_as_absent_and_drop_out_of_pages() {
    let mut fixture = fixture("pages");
    fixture.seed();
    let ada = as_("ada", "user");
    assert_eq!(fixture.get(&ada, "docs", &k("d01")), Value::Null);
    assert_eq!(text(&fixture.get(&ada, "docs", &k("d00")), "owner"), "ada");
    // Pages fill with visible rows however many hidden rows sit between.
    let scan = |cursor: Value| {
        fixture
            .query(
                &ada,
                "scan",
                obj(vec![
                    ("coll", s("docs")),
                    ("limit", n(2.0)),
                    ("after", cursor),
                ]),
            )
            .unwrap()
    };
    let first = scan(Value::Null);
    assert_eq!(field(&first, "keys"), &arr(vec![s("d00"), s("d15")]));
    let second = scan(field(&first, "cursor").clone());
    assert_eq!(field(&second, "keys"), &arr(vec![s("d29")]));
    assert_eq!(field(&second, "cursor"), &Value::Null);
    // Index ranges too.
    let range = fixture
        .query(
            &ada,
            "range",
            obj(vec![
                ("index", s("by_title")),
                ("limit", n(2.0)),
                ("after", Value::Null),
            ]),
        )
        .unwrap();
    assert_eq!(field(&range, "keys"), &arr(vec![s("d00"), s("d15")]));
    // Anonymous callers match no owner rule: they see nothing.
    assert_eq!(
        fixture.get(&CallOptions::default(), "docs", &k("d00")),
        Value::Null
    );
    // Admins see every row.
    let admin = fixture
        .query(
            &as_("root", "admin"),
            "scan",
            obj(vec![
                ("coll", s("docs")),
                ("limit", n(100.0)),
                ("after", Value::Null),
            ]),
        )
        .unwrap();
    assert_eq!(field(&admin, "keys").as_array().unwrap().len(), 30);
}

#[test]
fn hidden_fields_are_redacted_and_survive_writes() {
    let mut fixture = fixture("redaction");
    let ada = as_("ada", "user");
    let carol = as_("carol", "editor");
    let doc = obj(vec![
        ("owner", s("ada")),
        ("title", s("plan")),
        ("team", s("core")),
        ("secret", s("s1")),
    ]);
    fixture.put(&ada, "docs", &k("d1"), doc).unwrap();
    fixture
        .put(&ada, "members", r#"["core","carol"]"#, Value::Bool(true))
        .unwrap();
    // Carol reads the row through her team, without the owner's field.
    let seen = fixture.get(&carol, "docs", &k("d1"));
    assert_eq!(text(&seen, "title"), "plan");
    assert!(
        seen.as_object().unwrap().get("secret").is_none(),
        "{seen:?}"
    );
    assert_eq!(text(&fixture.get(&ada, "docs", &k("d1")), "secret"), "s1");
    // An editor's read-modify-write keeps the field it could not see.
    fixture
        .mutate(
            &carol,
            "retitle",
            obj(vec![("key", s(&k("d1"))), ("title", s("plan b"))]),
        )
        .unwrap();
    let stored = fixture.get(&ada, "docs", &k("d1"));
    assert_eq!(text(&stored, "title"), "plan b");
    assert_eq!(text(&stored, "secret"), "s1");
    // Changing a field she cannot read is denied.
    let mut overwrite = seen.clone();
    if let Value::Object(map) = &mut overwrite {
        map.insert("secret".to_string(), s("mine"));
    }
    let denied = fixture
        .put(&carol, "docs", &k("d1"), overwrite)
        .unwrap_err();
    assert_eq!(denied.code, "ACCESS_DENIED");
    // Rows found through a field the caller can't read stay hidden.
    let by_secret = |who: &CallOptions| {
        fixture
            .query(
                who,
                "range",
                obj(vec![
                    ("index", s("by_secret")),
                    ("limit", n(10.0)),
                    ("after", Value::Null),
                ]),
            )
            .unwrap()
    };
    assert_eq!(field(&by_secret(&carol), "keys"), &arr(vec![]));
    assert_eq!(field(&by_secret(&ada), "keys"), &arr(vec![s("d1")]));
}

#[test]
fn denied_writes_fail_and_unseen_deletes_skip() {
    let mut fixture = fixture("writes");
    let ada = as_("ada", "user");
    let bob = as_("bob", "user");
    let carol = as_("carol", "user");
    let doc = obj(vec![
        ("owner", s("ada")),
        ("title", s("plan")),
        ("team", s("core")),
        ("secret", s("s1")),
    ]);
    fixture.put(&ada, "docs", &k("d1"), doc.clone()).unwrap();
    fixture
        .put(&ada, "members", r#"["core","carol"]"#, Value::Bool(true))
        .unwrap();
    // Inserting someone else's row is denied; so is taking a row over.
    let forged = obj(vec![("owner", s("ada")), ("title", s("x"))]);
    assert_eq!(
        fixture
            .put(&bob, "docs", &k("d2"), forged)
            .unwrap_err()
            .code,
        "ACCESS_DENIED"
    );
    let stolen = obj(vec![("owner", s("bob")), ("title", s("plan"))]);
    assert_eq!(
        fixture
            .put(&ada, "docs", &k("d1"), stolen)
            .unwrap_err()
            .code,
        "ACCESS_DENIED"
    );
    // Bob can't see the row: his delete acts on a missing key.
    let del = |key: &str| obj(vec![("coll", s("docs")), ("key", s(key))]);
    fixture.mutate(&bob, "del", del(&k("d1"))).unwrap();
    assert_eq!(text(&fixture.get(&ada, "docs", &k("d1")), "owner"), "ada");
    // Carol sees it but may not delete it.
    assert_eq!(
        fixture
            .mutate(&carol, "del", del(&k("d1")))
            .unwrap_err()
            .code,
        "ACCESS_DENIED"
    );
    // Conditional deletes compare against the row the caller may see.
    let cas = |expected: Value| obj(vec![("key", s(&k("d1"))), ("expected", expected)]);
    assert_eq!(
        fixture.mutate(&bob, "cas", cas(doc.clone())).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        fixture.mutate(&ada, "cas", cas(doc)).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(fixture.get(&ada, "docs", &k("d1")), Value::Null);
}

#[test]
fn readable_follows_another_collections_read_rule() {
    let mut fixture = fixture("readable");
    let ada = as_("ada", "user");
    let bob = as_("bob", "user");
    let doc = obj(vec![
        ("owner", s("ada")),
        ("title", s("plan")),
        ("team", s("core")),
    ]);
    fixture.put(&ada, "docs", &k("d1"), doc).unwrap();
    fixture
        .put(&ada, "notes", &k("n1"), obj(vec![("doc", s("d1"))]))
        .unwrap();
    assert_eq!(text(&fixture.get(&ada, "notes", &k("n1")), "doc"), "d1");
    assert_eq!(fixture.get(&bob, "notes", &k("n1")), Value::Null);
    // Membership is a row like any other: adding Bob shows him the note.
    fixture
        .put(&ada, "members", r#"["core","bob"]"#, Value::Bool(true))
        .unwrap();
    assert_eq!(text(&fixture.get(&bob, "notes", &k("n1")), "doc"), "d1");
}

#[test]
fn derived_values_have_their_own_rules_and_definer_rights() {
    let mut fixture = fixture("derived");
    fixture.seed();
    let denied = fixture
        .query(&as_("ada", "user"), "count", Value::Null)
        .unwrap_err();
    assert_eq!(denied.code, "ACCESS_DENIED");
    // An auditor reads it, though no doc's read rule admits auditors:
    // the value counted with definer rights, every row.
    assert_eq!(
        fixture
            .query(&as_("eve", "auditor"), "count", Value::Null)
            .unwrap(),
        n(30.0)
    );
    assert_eq!(
        fixture.get(&as_("eve", "auditor"), "docs", &k("d00")),
        Value::Null
    );
}

#[test]
fn triggers_and_timers_write_past_policies() {
    let mut fixture = fixture("definer");
    let ada = as_("ada", "user");
    // The docs trigger writes the deny-all audits collection: had it run
    // as Ada, the whole mutation would fail.
    fixture
        .put(&ada, "docs", &k("d1"), obj(vec![("owner", s("ada"))]))
        .unwrap();
    // Callers still can't write it themselves.
    assert_eq!(
        fixture
            .put(&ada, "audits", &k("x"), s("forged"))
            .unwrap_err()
            .code,
        "ACCESS_DENIED"
    );
    // Timer handlers have no caller either.
    fixture.mutate(&ada, "remind", Value::Null).unwrap();
    fixture.engine.set_replicated(b"db");
    match fixture.engine.prepare_timer_fire(b"db", "r1").unwrap() {
        FirePrepare::Patch(prepared) => {
            let outcome = fixture.engine.apply_patch(&prepared.patch, 9).unwrap();
            assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        }
        FirePrepare::Retry(_) => panic!("the timer handler was denied"),
        FirePrepare::Hold(_) => panic!("the module was not removed"),
        FirePrepare::Skip => panic!("the timer was not due"),
    }
}

#[test]
fn definer_rights_stay_in_their_module() {
    let mut fixture = fixture("modules");
    // The log trigger calls the vault, whose deny-all policy now guards
    // an anonymous caller: the trigger fails, and the write with it.
    let failure = fixture
        .put(&as_("ada", "admin"), "log", &k("l1"), s("entry"))
        .unwrap_err();
    assert_eq!(failure.code, "ACCESS_DENIED");
}

#[test]
fn history_shows_only_what_the_read_rule_shows() {
    let mut fixture = fixture("history");
    let ada = as_("ada", "user");
    fixture
        .put(&ada, "docs", &k("d1"), obj(vec![("owner", s("ada"))]))
        .unwrap();
    let revisions = |who: &CallOptions| -> Vec<(f64, Value)> {
        let versions = fixture
            .query(who, "history", obj(vec![("key", s(&k("d1")))]))
            .unwrap();
        versions
            .as_array()
            .unwrap()
            .iter()
            .map(|version| {
                (
                    field(version, "revision").as_f64().unwrap(),
                    field(version, "value").clone(),
                )
            })
            .collect()
    };
    // Bob sees his own version only: neither Ada's nor its deletion.
    let bob = revisions(&as_("bob", "user"));
    assert_eq!(bob.len(), 1);
    assert_eq!(bob[0].0, 1.0);
    // Ada sees her version, its secret and its deletion, not Bob's.
    let ada_view = revisions(&ada);
    assert_eq!(
        ada_view,
        vec![
            (3.0, Value::Null),
            (2.0, json(r#"{"owner":"ada","secret":"s","title":"b"}"#)),
        ]
    );
    // An admin sees everything, but not Ada's secret.
    let admin = revisions(&as_("root", "admin"));
    assert_eq!(admin.len(), 3);
    assert!(field(&admin[1].1, "secret").is_null());
}

#[test]
fn deploys_reject_invalid_policies() {
    let mut fixture = fixture("invalid");
    let manifests = [
        // `next` only exists while writing.
        r#"{"read": {"exists": {"ref": ["next", "a"]}}}"#,
        // A readable target needs a policy (members has none).
        r#"{"read": {"readable": ["members", {"ref": ["key"]}]}}"#,
        // Lookups name declared collections.
        r#"{"read": {"stored": ["missing", {"ref": ["key"]}]}}"#,
        // A readable target's read rule can't use readable itself.
        r#"{"read": {"readable": ["notes", {"ref": ["key"]}]}}"#,
    ];
    for policy in manifests {
        let mut manifest = app_manifest();
        if let Value::Object(root) = &mut manifest
            && let Some(Value::Array(collections)) = root.get_mut("collections")
        {
            collections.push(collection("extra", "string", Some(policy), arr(vec![])));
        }
        let error = fixture
            .engine
            .deploy(
                b"db2",
                "app",
                [9u8; 32],
                b"",
                &manifest,
                Box::new(ClosureRuntime::new(app("app"))),
            )
            .unwrap_err();
        assert!(error.message.contains("access"), "{policy}: {error:?}");
    }
    // Only derived definitions take access rules.
    let mut manifest = app_manifest();
    if let Value::Object(root) = &mut manifest
        && let Some(Value::Array(definitions)) = root.get_mut("definitions")
    {
        definitions.push(definition("ruled", "query", Some(r#"{"const": true}"#)));
    }
    assert!(
        fixture
            .engine
            .deploy(
                b"db2",
                "app",
                [9u8; 32],
                b"",
                &manifest,
                Box::new(ClosureRuntime::new(app("app"))),
            )
            .is_err()
    );
}
