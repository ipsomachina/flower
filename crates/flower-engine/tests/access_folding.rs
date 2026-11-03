//! Folded access policies at the engine: a plain scan whose read rule
//! pins the owner to the caller reads the owner's index bucket and
//! answers exactly what a full scan answers, depending on that bucket
//! alone; rules about the caller alone cost no lookups; `clear` removes
//! hidden fields only with the right to change them and leaks nothing
//! through receipts or cached answers.
//!
//! Every policy here sits on two collections, `plain{n}` without an
//! index and `indexed{n}` with one on `owner`, so a scan of one is the
//! full scan the other's bucket scan must agree with.

mod fixtures;

use fixtures::{arr, hc, n, obj, s};

use flower_engine::engine::{CallOptions, Engine, MutationOptions, PrepareOutcome};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Principal, Value, fail};
use rand::{Rng, SeedableRng};

fn json(text: &str) -> Value {
    flower_engine::json::parse_value(text.as_bytes()).unwrap()
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

/// Every page of `coll` at `limit`, as `[rows, more]` pairs.
fn pages(host: &mut dyn HostCall, coll: &Value, limit: &Value) -> Result<Value, Failure> {
    let mut pages = Vec::new();
    let mut after = Value::Null;
    loop {
        let page = hc(
            host,
            HostOp::Scan,
            vec![s(""), coll.clone(), limit.clone(), after.clone()],
        )?;
        let cursor = field(&page, "cursor").clone();
        pages.push(arr(vec![
            field(&page, "rows").clone(),
            Value::Bool(!cursor.is_null()),
        ]));
        if cursor.is_null() {
            return Ok(arr(pages));
        }
        after = cursor;
    }
}

fn guest(
    kind: CallKind,
    _module: &str,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    match (kind, name) {
        (CallKind::Query, "get") => hc(
            host,
            HostOp::Get,
            vec![
                s(""),
                field(args, "coll").clone(),
                field(args, "key").clone(),
            ],
        ),
        (CallKind::Query, "scan") => hc(
            host,
            HostOp::Scan,
            vec![s(""), field(args, "coll").clone(), n(100.0), Value::Null],
        ),
        (CallKind::Query, "pages") => pages(host, field(args, "coll"), field(args, "limit")),
        (CallKind::Query, "page") => hc(
            host,
            HostOp::Scan,
            vec![
                s(""),
                field(args, "coll").clone(),
                field(args, "limit").clone(),
                field(args, "after").clone(),
            ],
        ),
        // Stage writes to every collection named, then page through each
        // inside the same invocation.
        (CallKind::Mutation, "mix") => {
            for write in field(args, "writes").as_array().unwrap() {
                let write = write.as_array().unwrap();
                for coll in field(args, "colls").as_array().unwrap() {
                    if write[1].is_null() {
                        hc(
                            host,
                            HostOp::Delete,
                            vec![s(""), coll.clone(), write[0].clone()],
                        )?;
                    } else {
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), coll.clone(), write[0].clone(), write[1].clone()],
                        )?;
                    }
                }
            }
            let mut out = Vec::new();
            for coll in field(args, "colls").as_array().unwrap() {
                out.push(pages(host, coll, field(args, "limit"))?);
            }
            Ok(arr(out))
        }
        (CallKind::Mutation, "put") => {
            let mut call = vec![
                s(""),
                field(args, "coll").clone(),
                field(args, "key").clone(),
                field(args, "value").clone(),
            ];
            if !field(args, "clear").is_null() {
                call.push(field(args, "clear").clone());
            }
            hc(host, HostOp::Set, call)?;
            // Answer the row as the caller now sees it.
            hc(
                host,
                HostOp::Get,
                vec![
                    s(""),
                    field(args, "coll").clone(),
                    field(args, "key").clone(),
                ],
            )
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    }
}

/// Read rules the equivalence covers; every one pins `owner` to the
/// caller for some callers.
const READS: &[&str] = &[
    r#"{"eq": [{"ref": ["row", "owner"]}, {"ref": ["principal", "subject"]}]}"#,
    r#"{"all": [{"eq": [{"ref": ["principal", "subject"]}, {"ref": ["row", "owner"]}]},
                {"exists": {"ref": ["row", "title"]}}]}"#,
    r#"{"any": [{"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "admin"}]},
                {"eq": [{"ref": ["row", "owner"]}, {"ref": ["principal", "subject"]}]}]}"#,
    r#"{"all": [{"eq": [{"ref": ["row", "owner"]}, {"ref": ["principal", "subject"]}]},
                {"not": {"eq": [{"ref": ["row", "hidden"]}, {"value": true}]}},
                {"any": [{"lt": [{"ref": ["row", "rank"]}, {"value": 5}]},
                         {"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "vip"}]}]}]}"#,
];

fn policy(read: &str) -> String {
    format!(
        r#"{{"read": {read}, "insert": {{"const": true}}, "update": {{"const": true}},
            "delete": {{"const": true}},
            "fields": {{"secret": {{"read": {{"eq": [{{"ref": ["principal", "claims", "role"]}}, {{"value": "admin"}}]}}}}}}}}"#
    )
}

fn collection(name: &str, access: Option<&str>, indexes: Vec<Value>) -> Value {
    let mut pairs = vec![
        ("name", s(name)),
        ("key", obj(vec![("type", s("json"))])),
        ("value", obj(vec![("type", s("json"))])),
        ("indexes", arr(indexes)),
        ("references", arr(vec![])),
    ];
    if let Some(access) = access {
        pairs.push(("access", json(access)));
    }
    obj(pairs)
}

fn by_owner() -> Value {
    obj(vec![
        ("name", s("by_owner")),
        ("fields", arr(vec![s("owner")])),
    ])
}

/// `token`: admins read it, its owner writes it. `audit`: admins only.
const VAULT: &str = r#"{
    "read": {"const": true}, "insert": {"const": true}, "update": {"const": true},
    "fields": {
        "token": {"read": {"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "admin"}]},
                  "write": {"eq": [{"ref": ["next", "owner"]}, {"ref": ["principal", "subject"]}]}},
        "audit": {"read": {"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "admin"}]}}
    }
}"#;

/// Docs readable by members of their team (rows of `members` keyed
/// `[team, subject]`) or admins, the lookup first; notes readable where
/// their parent is, and parents by admins only.
const MEMBERSHIP: &str = r#"{"read": {"any": [
    {"stored": ["members", {"list": [{"ref": ["row", "team"]}, {"ref": ["principal", "subject"]}]}]},
    {"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "admin"}]}
]}, "insert": {"const": true}}"#;
const PARENTS: &str = r#"{"read": {"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "admin"}]},
    "insert": {"const": true}, "update": {"const": true}}"#;
const CHILDREN: &str =
    r#"{"read": {"readable": ["parents", {"ref": ["row", "parent"]}]}, "insert": {"const": true}}"#;

fn manifest() -> Value {
    let mut collections = Vec::new();
    for (index, read) in READS.iter().enumerate() {
        let access = policy(read);
        collections.push(collection(&format!("plain{index}"), Some(&access), vec![]));
        collections.push(collection(
            &format!("indexed{index}"),
            Some(&access),
            vec![by_owner()],
        ));
    }
    collections.push(collection("vault", Some(VAULT), vec![]));
    collections.push(collection("members", None, vec![]));
    collections.push(collection("docs", Some(MEMBERSHIP), vec![]));
    collections.push(collection("parents", Some(PARENTS), vec![]));
    collections.push(collection("children", Some(CHILDREN), vec![]));
    let definition = |name: &str, kind: &str| {
        obj(vec![
            ("name", s(name)),
            ("kind", s(kind)),
            ("args", obj(vec![("type", s("json"))])),
        ])
    };
    obj(vec![
        ("collections", arr(collections)),
        (
            "definitions",
            arr(vec![
                definition("get", "query"),
                definition("scan", "query"),
                definition("pages", "query"),
                definition("page", "query"),
                definition("mix", "mutation"),
                definition("put", "mutation"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
    position: u64,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn fixture(test: &str) -> Fixture {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-folding-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            b"",
            &manifest(),
            Box::new(ClosureRuntime::new(guest)),
        )
        .unwrap();
    Fixture {
        engine,
        path,
        position: 0,
    }
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
    fn read(&self, who: &CallOptions, name: &str, args: Value) -> Value {
        self.engine
            .reader()
            .invoke_query(b"db", "app", name, &args, who)
            .unwrap()
    }

    fn mutate(&mut self, who: &CallOptions, name: &str, args: Value) -> Result<Value, Failure> {
        self.mutate_keyed(who, name, args, None)
    }

    fn mutate_keyed(
        &mut self,
        who: &CallOptions,
        name: &str,
        args: Value,
        key: Option<&str>,
    ) -> Result<Value, Failure> {
        let options = MutationOptions {
            call: who.clone(),
            idempotency_key: key.map(|key| key.as_bytes().to_vec()),
            ..MutationOptions::default()
        };
        // Replicate the write the way members apply one, so the change
        // journal follows it and cached answers can tell.
        match self
            .engine
            .prepare_mutation(b"db", "app", name, &args, &options)?
        {
            PrepareOutcome::Patch(prepared) => {
                self.position += 1;
                self.engine
                    .apply_patch(&prepared.patch, self.position)?
                    .result
            }
            PrepareOutcome::Done(result) | PrepareOutcome::Replayed(result, _) => result,
        }
    }

    fn put(&mut self, who: &CallOptions, coll: &str, key: Value, value: Value) {
        let key = String::from_utf8(flower_engine::json::render(&key).unwrap()).unwrap();
        self.mutate(
            who,
            "put",
            obj(vec![("coll", s(coll)), ("key", s(&key)), ("value", value)]),
        )
        .unwrap();
    }

    fn hits(&self) -> u64 {
        self.engine.reader().query_cache_stats().hits
    }
}

fn callers() -> Vec<CallOptions> {
    vec![
        CallOptions::default(),
        as_("a", "user"),
        as_("b", "user"),
        as_("b", "vip"),
        as_("c", "user"),
        as_("root", "admin"),
    ]
}

fn random_key(rng: &mut rand::rngs::StdRng) -> String {
    // Keys of several lengths: records order by length first.
    let key = match rng.random_range(0..3) {
        0 => s(&format!("k{}", rng.random_range(0..40))),
        1 => arr(vec![
            s(["a", "b"][rng.random_range(0..2)]),
            n(rng.random_range(0..9) as f64),
        ]),
        _ => n(rng.random_range(0..200) as f64),
    };
    String::from_utf8(flower_engine::json::render(&key).unwrap()).unwrap()
}

fn random_row(rng: &mut rand::rngs::StdRng, with_secret: bool) -> Value {
    let mut pairs = Vec::new();
    let owner = match rng.random_range(0..10) {
        0 => None,
        1 => Some(Value::Null),
        2 => Some(n(1.0)),
        3 => Some(arr(vec![s("a")])),
        4 => Some(s("A")),
        _ => Some(s(["a", "b", "c"][rng.random_range(0..3)])),
    };
    if let Some(owner) = owner {
        pairs.push(("owner", owner));
    }
    if rng.random_bool(0.7) {
        pairs.push(("title", s("t")));
    }
    if rng.random_bool(0.3) {
        pairs.push(("hidden", Value::Bool(rng.random_bool(0.5))));
    }
    if rng.random_bool(0.8) {
        pairs.push(("rank", n(rng.random_range(0..10) as f64)));
    }
    if with_secret && rng.random_bool(0.5) {
        pairs.push(("secret", s("s")));
    }
    obj(pairs)
}

/// A scan of the indexed collection (a bucket scan for every caller
/// whose read rule pins the owner) answers page for page, row for row and
/// cursor for cursor what the full scan of the plain one answers, over
/// random rows (owners that are no string, null or missing included),
/// callers, page sizes and rules, on committed state and on an
/// invocation's own staged writes.
#[test]
fn bucket_scans_answer_exactly_what_full_scans_answer() {
    let mut fixture = fixture("equivalence");
    let mut rng = rand::rngs::StdRng::seed_from_u64(0xb0c4e7);
    let admin = as_("root", "admin");
    let mut compared = 0;
    for (index, _) in READS.iter().enumerate() {
        let colls = arr(vec![
            s(&format!("plain{index}")),
            s(&format!("indexed{index}")),
        ]);
        // Seed both alike, hidden fields included.
        let seed: Vec<Value> = (0..60)
            .map(|_| arr(vec![s(&random_key(&mut rng)), random_row(&mut rng, true)]))
            .collect();
        fixture
            .mutate(
                &admin,
                "mix",
                obj(vec![
                    ("writes", arr(seed)),
                    ("colls", colls.clone()),
                    ("limit", n(1.0)),
                ]),
            )
            .unwrap();
        for who in callers() {
            for limit in [1.0, 2.0, 3.0, 7.0, 100.0] {
                let plain = fixture.read(
                    &who,
                    "pages",
                    obj(vec![
                        ("coll", s(&format!("plain{index}"))),
                        ("limit", n(limit)),
                    ]),
                );
                let indexed = fixture.read(
                    &who,
                    "pages",
                    obj(vec![
                        ("coll", s(&format!("indexed{index}"))),
                        ("limit", n(limit)),
                    ]),
                );
                assert_eq!(plain, indexed, "rule {index}, limit {limit}, {who:?}");
                compared += 1;
            }
            // Writes staged earlier in the same invocation count, moving
            // rows into and out of the caller's bucket and deleting some.
            let writes: Vec<Value> = (0..8)
                .map(|_| {
                    let value = if rng.random_bool(0.25) {
                        Value::Null
                    } else {
                        random_row(&mut rng, false)
                    };
                    arr(vec![s(&random_key(&mut rng)), value])
                })
                .collect();
            let limit = [1.0, 2.0, 5.0][rng.random_range(0..3)];
            let both = fixture
                .mutate(
                    &who,
                    "mix",
                    obj(vec![
                        ("writes", arr(writes)),
                        ("colls", colls.clone()),
                        ("limit", n(limit)),
                    ]),
                )
                .unwrap();
            let both = both.as_array().unwrap();
            assert_eq!(both[0], both[1], "staged, rule {index}, {who:?}");
            compared += 1;
        }
    }
    assert_eq!(compared, READS.len() * callers().len() * 6);
}

/// A bucket scan depends on its bucket alone: another owner's write
/// leaves its cached answer valid, where a full scan's answer, which
/// depends on the whole collection, must evaluate again. A row moving
/// into the bucket, or a change to one in it, does invalidate it.
#[test]
fn bucket_scans_depend_on_their_bucket_alone() {
    let mut fixture = fixture("bucket-deps");
    let a = as_("a", "user");
    let b = as_("b", "user");
    for coll in ["plain0", "indexed0"] {
        fixture.put(&a, coll, s("x"), obj(vec![("owner", s("a"))]));
        fixture.put(&b, coll, s("y"), obj(vec![("owner", s("b"))]));
    }
    let scan = |fixture: &Fixture, coll: &str| {
        let page = fixture.read(&a, "scan", obj(vec![("coll", s(coll))]));
        field(&page, "rows").as_array().unwrap().len()
    };
    for coll in ["plain0", "indexed0"] {
        assert_eq!(scan(&fixture, coll), 1);
    }
    // Bob writes his own row in both collections.
    for coll in ["plain0", "indexed0"] {
        fixture.put(
            &b,
            coll,
            s("y"),
            obj(vec![("owner", s("b")), ("v", n(2.0))]),
        );
    }
    let before = fixture.hits();
    assert_eq!(scan(&fixture, "indexed0"), 1);
    assert_eq!(
        fixture.hits(),
        before + 1,
        "bob's write left ada's bucket valid"
    );
    assert_eq!(scan(&fixture, "plain0"), 1);
    assert_eq!(
        fixture.hits(),
        before + 1,
        "the full scan read the whole collection"
    );
    // Bob hands his row to Ada: it enters her bucket.
    fixture.put(&b, "indexed0", s("y"), obj(vec![("owner", s("a"))]));
    assert_eq!(scan(&fixture, "indexed0"), 2);
    // A change to a row in her bucket is seen too.
    fixture.put(&a, "indexed0", s("x"), obj(vec![("owner", s("c"))]));
    assert_eq!(scan(&fixture, "indexed0"), 1);
    assert_eq!(fixture.hits(), before + 1);
}

/// Rules about the caller alone fold before any row: an admin's reads of
/// docs (members first, then admins) look no membership up, so a
/// membership change leaves the admin's cached answer valid while a
/// member's must evaluate again; and a `readable` target the caller can
/// never read costs no lookup, so its rows changing changes nothing.
#[test]
fn rules_about_the_caller_alone_cost_no_lookups() {
    let mut fixture = fixture("constants");
    let admin = as_("root", "admin");
    let carol = as_("carol", "user");
    fixture.put(&admin, "docs", s("d1"), obj(vec![("team", s("core"))]));
    fixture.put(
        &admin,
        "members",
        json(r#"["core","carol"]"#),
        Value::Bool(true),
    );
    fixture.put(&admin, "parents", s("p"), obj(vec![]));
    fixture.put(&admin, "children", s("c1"), obj(vec![("parent", s("p"))]));
    let count = |fixture: &Fixture, who: &CallOptions, coll: &str| {
        let page = fixture.read(who, "scan", obj(vec![("coll", s(coll))]));
        field(&page, "rows").as_array().unwrap().len()
    };
    assert_eq!(count(&fixture, &admin, "docs"), 1);
    assert_eq!(count(&fixture, &carol, "docs"), 1);
    assert_eq!(count(&fixture, &carol, "children"), 0);
    // The admin joins the team (the row a per-row check would look up
    // for them), Carol leaves it, and the parent changes.
    fixture.put(
        &admin,
        "members",
        json(r#"["core","root"]"#),
        Value::Bool(true),
    );
    fixture
        .mutate(
            &admin,
            "mix",
            obj(vec![
                (
                    "writes",
                    arr(vec![arr(vec![s(r#"["core","carol"]"#), Value::Null])]),
                ),
                ("colls", arr(vec![s("members")])),
                ("limit", n(10.0)),
            ]),
        )
        .unwrap();
    fixture.put(&admin, "parents", s("p"), obj(vec![("v", n(1.0))]));
    let before = fixture.hits();
    assert_eq!(count(&fixture, &admin, "docs"), 1);
    assert_eq!(count(&fixture, &carol, "children"), 0);
    assert_eq!(fixture.hits(), before + 2, "no lookup, no dependency");
    // Carol's answer looked her membership up, and follows it.
    assert_eq!(count(&fixture, &carol, "docs"), 0);
    assert_eq!(fixture.hits(), before + 2);
}

fn put_args(key: &str, value: Value, clear: &[&str]) -> Value {
    obj(vec![
        ("coll", s("vault")),
        ("key", s(&format!("\"{key}\""))),
        ("value", value),
        ("clear", arr(clear.iter().map(|name| s(name)).collect())),
    ])
}

/// `clear` removes a hidden field only with the right to change it,
/// whether or not the row holds it, so a denied clear never tells; what
/// a clear answers and replays is the caller's own redacted view; and no
/// cached answer keeps showing a cleared field.
#[test]
fn clear_takes_the_right_to_change_and_leaks_nothing() {
    let mut fixture = fixture("clear");
    let ada = as_("ada", "user");
    let bob = as_("bob", "user");
    let admin = as_("root", "admin");
    let get = |fixture: &Fixture, who: &CallOptions, key: &str| {
        fixture.read(
            who,
            "get",
            obj(vec![
                ("coll", s("vault")),
                ("key", s(&format!("\"{key}\""))),
            ]),
        )
    };
    // Ada's rows: one holding a token, one without; an admin adds audits.
    for (key, token) in [("full", true), ("empty", false)] {
        let mut row = vec![("owner", s("ada")), ("text", s("t"))];
        if token {
            row.push(("token", s("secret-token")));
        }
        fixture
            .mutate(&ada, "put", put_args(key, obj(row), &[]))
            .unwrap();
        let mut seen = get(&fixture, &admin, key);
        if let Value::Object(map) = &mut seen {
            map.insert("audit".to_string(), s("x"));
        }
        fixture
            .mutate(&admin, "put", put_args(key, seen, &[]))
            .unwrap();
    }
    // An admin's cached answer shows the token.
    assert_eq!(
        field(&get(&fixture, &admin, "full"), "token"),
        &s("secret-token")
    );
    // Bob may change neither hidden field: denied alike, held or not,
    // first time and on replay.
    for key in ["full", "empty"] {
        for clear in [&["token"][..], &["audit"][..]] {
            let args = put_args(key, obj(vec![("owner", s("ada")), ("text", s("t"))]), clear);
            let idempotency = format!("bob-{key}-{}", clear[0]);
            let first = fixture
                .mutate_keyed(&bob, "put", args.clone(), Some(&idempotency))
                .unwrap_err();
            assert_eq!(first.code, "ACCESS_DENIED", "{key} {clear:?}");
            let replay = fixture
                .mutate_keyed(&bob, "put", args, Some(&idempotency))
                .unwrap_err();
            assert_eq!((replay.code, replay.message), (first.code, first.message));
        }
    }
    // Ada may clear her token but not the audit.
    let mine = obj(vec![("owner", s("ada")), ("text", s("t2"))]);
    let denied = fixture
        .mutate(&ada, "put", put_args("full", mine.clone(), &["audit"]))
        .unwrap_err();
    assert_eq!(denied.code, "ACCESS_DENIED");
    let answered = fixture
        .mutate_keyed(
            &ada,
            "put",
            put_args("full", mine.clone(), &["token"]),
            Some("k"),
        )
        .unwrap();
    // She answers her own view, which never held either field, and the
    // receipt replays exactly that.
    assert_eq!(answered, obj(vec![("owner", s("ada")), ("text", s("t2"))]));
    let replayed = fixture
        .mutate_keyed(&ada, "put", put_args("full", mine, &["token"]), Some("k"))
        .unwrap();
    assert_eq!(replayed, answered);
    // The admin's cached answer does not outlive the token.
    let after = get(&fixture, &admin, "full");
    assert_eq!(field(&after, "token"), &Value::Null, "{after:?}");
    assert_eq!(field(&after, "audit"), &s("x"));
    assert_eq!(field(&after, "text"), &s("t2"));
}

/// A scan cursor that does not decode to a record key of the collection
/// (here, cut short inside its key) is the caller's mistake: an
/// `INVALID_VALUE` the guest can recover from, on the owner-bucket path
/// as on the full scan, never corrupt storage.
#[test]
fn malformed_cursors_are_invalid_values_on_every_scan_path() {
    let mut fixture = fixture("cursors");
    let a = as_("a", "user");
    for coll in ["plain0", "indexed0"] {
        for key in ["x1", "x2"] {
            fixture.put(&a, coll, s(key), obj(vec![("owner", s("a"))]));
        }
    }
    for coll in ["plain0", "indexed0"] {
        let page = |after: Value| {
            fixture.engine.reader().invoke_query(
                b"db",
                "app",
                "page",
                &obj(vec![("coll", s(coll)), ("limit", n(1.0)), ("after", after)]),
                &a,
            )
        };
        let first = page(Value::Null).unwrap();
        let cursor = field(&first, "cursor").as_str().unwrap().to_string();
        // Dropping the last byte leaves the prefix but not the key.
        let cut = s(&cursor[..cursor.len() - 2]);
        let refused = page(cut).unwrap_err();
        assert_eq!(refused.code, "INVALID_VALUE", "{coll}: {refused:?}");
        // So does a byte too many.
        let long = s(&format!("{cursor}00"));
        assert_eq!(page(long).unwrap_err().code, "INVALID_VALUE", "{coll}");
        // The cursor as given still pages on.
        let second = page(s(&cursor)).unwrap();
        assert_eq!(
            field(&second, "rows").as_array().unwrap().len(),
            1,
            "{coll}"
        );
    }
}
