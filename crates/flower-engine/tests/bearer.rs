//! Typed bearer authentication with kept decisions, through a real SDK
//! app: a hook that verifies HS256 bearer tokens with `JwtBearer` decides
//! once per token, whatever the arguments, and that decision holds until
//! the token expires, a declared instant rather than a clock poll; forged
//! and missing tokens are denied every time; retry sessions follow their
//! own policy.

extern crate alloc;

use std::sync::Arc;

use flower_crypto::host::CryptoHost;
use flower_crypto::jwt::{Algorithm, KeyFormat, SignOptions};
use flower_engine::certificate::Validity;
use flower_engine::engine::{CallOptions, Engine};
use flower_engine::runtime::NativeRuntime;
use flower_engine::storage::GroupStore;
use flower_sdk::{Failure, Value};

mod app {
    use flower_sdk::auth::{Access, Authentication, Authorization, BearerKey, JwtBearer};
    use flower_sdk::crypto::jwt::{Algorithm, KeyFormat};
    use flower_sdk::{App, Component, MethodCtx, QueryCtx, Value, query};

    pub const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    static BEARER: JwtBearer = JwtBearer {
        audience: &["api"],
        ..JwtBearer::new(BearerKey::Bytes(
            SECRET,
            KeyFormat::Raw,
            &[Algorithm::Hs256],
        ))
    };

    static AUTH: Authorization = Authorization {
        authenticate: Authentication::Typed(&BEARER),
        default: Access::Authenticated,
        sessions: Access::Authenticated,
        methods: &[],
    };

    /// Who the call runs as, and the argument it was given.
    #[query]
    pub fn whoami(ctx: &mut QueryCtx<'_, '_>, id: f64) -> flower_sdk::Result<Value> {
        let subject = ctx
            .principal()?
            .map_or(Value::Null, |principal| Value::String(principal.subject));
        Ok(Value::Array(alloc::vec![subject, Value::Number(id)]))
    }

    /// Anyone's.
    #[query(access = public)]
    pub fn open(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<bool> {
        let _ = ctx;
        Ok(true)
    }

    static DEFS: [&dyn flower_sdk::Component; 2] = [&WHOAMI, &OPEN];

    static USES: [&dyn Component; 1] = [&AUTH];

    pub static APP: App = App::new(&[&USES, &DEFS]);
}

/// Expiry of the tokens these tests sign: seconds since the epoch.
const EXP: u64 = 2_000_000;

fn token(subject: &str, secret: &[u8]) -> String {
    let claims = serde_json::json!({"sub": subject, "aud": "api", "exp": EXP});
    let options = SignOptions {
        algorithm: Algorithm::HS256,
        key_format: KeyFormat::Raw,
        kid: None,
        typ: None,
    };
    flower_crypto::jwt::sign(&claims, secret, &options).unwrap()
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
    path.push(format!("flower-bearer-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    let manifest = NativeRuntime::new(&app::APP).manifest().unwrap();
    let runtime = NativeRuntime::new(&app::APP).with_crypto(Arc::new(CryptoHost::new()));
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, Box::new(runtime))
        .unwrap();
    Fixture { engine, path }
}

/// Credentials as the server presents an `Authorization: Bearer` header.
fn bearer(credentials: Option<String>, at_ms: u64) -> CallOptions {
    CallOptions {
        credentials: credentials
            .map(|token| Value::Object([("bearer".to_string(), Value::String(token))].into())),
        timestamp_ms: Some(at_ms),
        ..CallOptions::default()
    }
}

impl Fixture {
    fn whoami(&self, id: f64, options: &CallOptions) -> Result<Value, Failure> {
        let args = Value::Object([("id".to_string(), Value::Number(id))].into());
        self.engine
            .reader()
            .invoke_query(b"db", "app", "whoami", &args, options)
    }

    fn evaluations(&self) -> u64 {
        self.engine.reader().decision_memo_stats().evaluations
    }
}

fn subject(answer: &Value) -> &str {
    match answer {
        Value::Array(items) => items[0].as_str().unwrap_or_default(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_bearer_decision_holds_for_any_arguments_until_the_token_expires() {
    let fixture = fixture("expiry");
    let ada = token("ada", app::SECRET);
    let before = fixture.evaluations();
    for id in 0..5 {
        let at = 1_999_000_000 + id as u64;
        let answer = fixture
            .whoami(id as f64, &bearer(Some(ada.clone()), at))
            .unwrap();
        assert_eq!(subject(&answer), "ada");
    }
    assert_eq!(fixture.evaluations(), before + 1, "verified once");
    // The decision says when it expires; it polled no clock.
    let view = fixture.engine.reader().view().unwrap();
    let (_, certificate) = view
        .authorize_certified(
            b"db",
            "app",
            "whoami",
            &Value::Null,
            &bearer(Some(ada.clone()), 1_999_000_000),
        )
        .unwrap();
    assert_eq!(
        certificate.validity(),
        Validity::Until(EXP * 1000 - 1),
        "{certificate:?}"
    );
    // Up to its expiry it answers; at it the token is verified again,
    // and refused.
    fixture
        .whoami(9.0, &bearer(Some(ada.clone()), EXP * 1000 - 2))
        .unwrap();
    assert_eq!(fixture.evaluations(), before + 1);
    let expired = fixture
        .whoami(9.0, &bearer(Some(ada), EXP * 1000))
        .unwrap_err();
    assert_eq!(expired.code, "unauthorized");
    assert!(expired.message.contains("UNAUTHENTICATED"), "{expired:?}");
    assert_eq!(fixture.evaluations(), before + 2);
}

#[test]
fn forged_and_missing_tokens_are_denied_every_time() {
    let fixture = fixture("forged");
    let forged = token("ada", b"another-secret-of-thirty-two-byte");
    let before = fixture.evaluations();
    for _ in 0..2 {
        let denied = fixture
            .whoami(1.0, &bearer(Some(forged.clone()), 1_999_000_000))
            .unwrap_err();
        assert_eq!(denied.code, "unauthorized");
        let missing = fixture
            .whoami(1.0, &bearer(None, 1_999_000_000))
            .unwrap_err();
        assert_eq!(missing.code, "unauthorized");
    }
    assert_eq!(fixture.evaluations(), before + 4, "denials are never kept");
    // Public methods admit anonymous callers.
    let open = fixture
        .engine
        .reader()
        .invoke_query(
            b"db",
            "app",
            "open",
            &Value::Object(Default::default()),
            &bearer(None, 1_999_000_000),
        )
        .unwrap();
    assert_eq!(open, Value::Bool(true));
}

#[test]
fn retry_sessions_need_an_authenticated_caller() {
    let fixture = fixture("sessions");
    for operation in ["open", "status", "ack", "close"] {
        let method = format!("$flower.session.{operation}");
        let refused = fixture
            .engine
            .session_owner(b"db", "app", &method, &bearer(None, 1_999_000_000))
            .unwrap_err();
        assert_eq!(refused.code, "unauthorized", "{method}");
        let ada = token("ada", app::SECRET);
        fixture
            .engine
            .session_owner(b"db", "app", &method, &bearer(Some(ada), 1_999_000_000))
            .unwrap();
    }
}
