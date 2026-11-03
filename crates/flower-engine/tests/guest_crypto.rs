//! Guest crypto through the SDK on the native bridge: token checks
//! declare when time alone next changes their outcome instead of
//! polling the clock, as v1's `changesAt`, and a guest-local random
//! source serves randomness where host entropy may not, JWE nonces
//! aside.

extern crate alloc;

mod fixtures;

use fixtures::{n, obj, s};

use std::sync::Arc;

use flower_crypto::host::CryptoHost;
use flower_engine::certificate::Validity;
use flower_engine::engine::{CallOptions, Engine};
use flower_engine::runtime::NativeRuntime;
use flower_engine::storage::GroupStore;
use flower_sdk::crypto::hex_encode;
use flower_sdk::crypto::webauthn::base64url_encode;
use flower_sdk::error::codes;
use flower_sdk::{Failure, Value};

/// HS256 tokens and dir/A256GCM JWEs under fixed raw keys, checked in
/// queries and in a derived value, plus randomness from chosen sources.
mod app {
    use flower_sdk::crypto::jwt::{
        Algorithm, EncryptOptions, JwtKey, KeyFormat, SignOptions, ValidationOptions, VerifyOptions,
    };
    use flower_sdk::crypto::webauthn::{RegistrationInit, RegistrationUser, RelyingParty};
    use flower_sdk::crypto::{Crypto, hex_encode};
    use flower_sdk::{App, DerivedCtx, QueryCtx, ReadCtx, Value, derived, fail, query};

    const SECRET: [u8; 32] = [1; 32];
    pub const SEALING: [u8; 32] = [2; 32];
    const OTHER: [u8; 32] = [3; 32];

    fn hs256() -> JwtKey<'static> {
        JwtKey::Bytes(&SECRET, KeyFormat::Raw)
    }

    /// `{exp}`, plus `nbf` when positive (seconds).
    fn claims(nbf: f64, exp: f64) -> Value {
        let mut claims = std::collections::BTreeMap::new();
        claims.insert(String::from("exp"), Value::Number(exp));
        if nbf > 0.0 {
            claims.insert(String::from("nbf"), Value::Number(nbf));
        }
        Value::Object(claims)
    }

    /// `ok`, or the failure's message.
    fn outcome<T>(checked: flower_sdk::Result<T>) -> String {
        match checked {
            Ok(_) => String::from("ok"),
            Err(failure) => failure.message,
        }
    }

    fn verify(ctx: &mut flower_sdk::Ctx<'_, '_>, token: &str, tolerance: f64) -> String {
        let options = VerifyOptions {
            algorithms: Some(vec![Algorithm::Hs256]),
            validation: ValidationOptions {
                clock_tolerance_seconds: Some(tolerance),
                ..ValidationOptions::default()
            },
        };
        outcome(Crypto::new(ctx).jwt().verify(token, hs256(), &options))
    }

    /// An HS256 token: signing reads no clock.
    #[query]
    pub fn issue(ctx: &mut QueryCtx<'_, '_>, nbf: f64, exp: f64) -> flower_sdk::Result<String> {
        let options = SignOptions {
            algorithm: Some(Algorithm::Hs256),
            ..SignOptions::default()
        };
        Crypto::new(ctx.ctx())
            .jwt()
            .sign(&claims(nbf, exp), hs256(), &options)
    }

    /// Declare `at` when positive, nothing (`None`) when negative, and
    /// make no call at zero.
    fn declare(ctx: &mut QueryCtx<'_, '_>, at: f64) -> flower_sdk::Result<()> {
        match at {
            at if at > 0.0 => ctx.changes_at(Some(at)),
            at if at < 0.0 => ctx.changes_at(None),
            _ => Ok(()),
        }
    }

    /// Check a token, with `before` and `after` declared around it
    /// (see [`declare`]), as ported v1 code declares a session row's
    /// expiry.
    #[query]
    pub fn check(
        ctx: &mut QueryCtx<'_, '_>,
        token: String,
        tolerance: f64,
        before: f64,
        after: f64,
    ) -> flower_sdk::Result<String> {
        declare(ctx, before)?;
        let outcome = verify(ctx.ctx(), &token, tolerance);
        declare(ctx, after)?;
        Ok(outcome)
    }

    /// A JWE under [`SEALING`] with an explicit nonce.
    #[query]
    pub fn seal(ctx: &mut QueryCtx<'_, '_>, exp: f64) -> flower_sdk::Result<String> {
        Crypto::new(ctx.ctx()).jwt().encrypt(
            &claims(0.0, exp),
            &SEALING,
            Some(&[9; 12]),
            &EncryptOptions::default(),
        )
    }

    /// Decrypt a JWE under [`SEALING`], or under another key.
    #[query]
    pub fn open(
        ctx: &mut QueryCtx<'_, '_>,
        token: String,
        right: bool,
    ) -> flower_sdk::Result<String> {
        let key = if right { &SEALING } else { &OTHER };
        Ok(outcome(Crypto::new(ctx.ctx()).jwt().decrypt(
            &token,
            key,
            &ValidationOptions::default(),
        )))
    }

    /// [`check`], kept as a derived value.
    #[derived]
    pub fn checked(ctx: &mut DerivedCtx<'_, '_>, token: String) -> flower_sdk::Result<String> {
        Ok(verify(ctx.ctx(), &token, 0.0))
    }

    /// Read [`checked`].
    #[query]
    pub fn read_checked(ctx: &mut QueryCtx<'_, '_>, token: String) -> flower_sdk::Result<Value> {
        let args = Value::Object([(String::from("token"), Value::String(token))].into());
        ctx.get_derived("app", "checked", &args)
    }

    fn sevens(bytes: &mut [u8]) -> flower_sdk::Result<()> {
        bytes.fill(7);
        Ok(())
    }

    fn broken(_: &mut [u8]) -> flower_sdk::Result<()> {
        Err(fail("PRNG_DOWN", "no entropy here"))
    }

    fn sourced<'a, 'c, 'r>(
        ctx: &'a mut flower_sdk::Ctx<'c, 'r>,
        source: &str,
    ) -> Crypto<'a, 'c, 'r> {
        let mut crypto = Crypto::new(ctx);
        match source {
            "sevens" => crypto.set_prng(Some(sevens)),
            "broken" => crypto.set_prng(Some(broken)),
            _ => {}
        }
        crypto
    }

    /// A random signing key pair's public key, hex, from `source`:
    /// `host`, `sevens` or `broken`.
    #[query]
    pub fn key_pair(ctx: &mut QueryCtx<'_, '_>, source: String) -> flower_sdk::Result<String> {
        let pair = sourced(ctx.ctx(), &source).nacl().sign_key_pair()?;
        Ok(hex_encode(&pair.public_key))
    }

    /// Passkey registration options, their challenge from `source`.
    #[query]
    pub fn passkey(ctx: &mut QueryCtx<'_, '_>, source: String) -> flower_sdk::Result<Value> {
        let init = RegistrationInit {
            rp: RelyingParty {
                id: String::from("example.com"),
                name: String::from("Example"),
            },
            user: RegistrationUser {
                id: String::from("AQ"),
                name: String::from("alice"),
                display_name: None,
            },
            ..RegistrationInit::default()
        };
        let options = sourced(ctx.ctx(), &source)
            .webauthn()
            .registration_options(&init)?;
        Ok(options.to_value())
    }

    /// A JWE whose nonce the SDK draws, `source` set.
    #[query]
    pub fn seal_drawn(ctx: &mut QueryCtx<'_, '_>, source: String) -> flower_sdk::Result<String> {
        sourced(ctx.ctx(), &source).jwt().encrypt(
            &claims(0.0, 4_102_444_800.0),
            &SEALING,
            None,
            &EncryptOptions::default(),
        )
    }

    static DEFS: [&dyn flower_sdk::Component; 9] = [
        &ISSUE,
        &CHECK,
        &SEAL,
        &OPEN,
        &CHECKED,
        &READ_CHECKED,
        &KEY_PAIR,
        &PASSKEY,
        &SEAL_DRAWN,
    ];

    pub static APP: App = App::new(&[&DEFS]);
}

/// The app deployed with an unscoped crypto host.
struct Fixture {
    engine: Engine,
    path: std::path::PathBuf,
}

impl Fixture {
    fn new(test: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!("flower-guest-crypto-{test}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut engine = Engine::new(GroupStore::create(&path).unwrap());
        let manifest = NativeRuntime::new(&app::APP).manifest().unwrap();
        let crypto = CryptoHost::new();
        let runtime = NativeRuntime::new(&app::APP).with_crypto(Arc::new(crypto));
        engine
            .deploy(b"db", "app", [4u8; 32], b"", &manifest, Box::new(runtime))
            .unwrap();
        Fixture { engine, path }
    }

    /// Run a query at `now_ms` on a reader view: its value and how
    /// long its certificate holds.
    fn certified(&self, name: &str, args: Value, now_ms: u64) -> (Value, Validity) {
        let view = self.engine.reader().view().unwrap();
        let options = CallOptions {
            timestamp_ms: Some(now_ms),
            ..CallOptions::default()
        };
        let authorized = view.authorize(b"db", "app", name, &args, &options).unwrap();
        let (value, certificate) = view
            .query_certified(b"db", &authorized, &args, &options)
            .unwrap();
        (value, certificate.validity())
    }

    fn query(&self, name: &str, args: Value) -> Result<Value, Failure> {
        self.engine
            .reader()
            .invoke_query(b"db", "app", name, &args, &CallOptions::default())
    }

    fn token(&self, nbf: f64, exp: f64) -> String {
        match self.query("issue", obj(vec![("nbf", n(nbf)), ("exp", n(exp))])) {
            Ok(Value::String(token)) => token,
            other => panic!("tokens are text: {other:?}"),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[test]
fn token_checks_declare_when_time_changes_their_outcome() {
    let fixture = Fixture::new("expiry");
    let token = fixture.token(0.0, 2_000.0);
    let check = |tolerance: f64, before: f64, after: f64, now_ms: u64| {
        fixture.certified(
            "check",
            obj(vec![
                ("token", s(&token)),
                ("tolerance", n(tolerance)),
                ("before", n(before)),
                ("after", n(after)),
            ]),
            now_ms,
        )
    };
    // A valid token holds until its `exp`, plus the tolerance.
    assert_eq!(
        check(0.0, 0.0, 0.0, 1_000_000),
        (s("ok"), Validity::Until(2_000_000))
    );
    assert_eq!(
        check(2.5, 0.0, 0.0, 1_000_000),
        (s("ok"), Validity::Until(2_002_500))
    );
    // The earliest declaration stands, whichever comes first: a later
    // instant (a session row's expiry) or `None` declared after the
    // check never lifts the token's expiry, as in v1.
    assert_eq!(
        check(0.0, 1_500_000.0, 0.0, 1_000_000),
        (s("ok"), Validity::Until(1_500_000))
    );
    assert_eq!(
        check(0.0, 3_000_000.0, 0.0, 1_000_000),
        (s("ok"), Validity::Until(2_000_000))
    );
    assert_eq!(
        check(0.0, 0.0, 3_000_000.0, 1_000_000),
        (s("ok"), Validity::Until(2_000_000))
    );
    assert_eq!(
        check(0.0, 0.0, -1.0, 1_000_000),
        (s("ok"), Validity::Until(2_000_000))
    );
    assert_eq!(
        check(0.0, 0.0, 1_500_000.0, 1_000_000),
        (s("ok"), Validity::Until(1_500_000))
    );
    // An expired token stays expired: nothing to wait for.
    assert_eq!(
        check(0.0, 0.0, 0.0, 2_000_000),
        (s("JWT expired"), Validity::Stable)
    );
    // One not active yet changes at its `nbf`, less the tolerance.
    let early = fixture.token(1_500.0, 2_000.0);
    let early = |tolerance: f64| {
        fixture.certified(
            "check",
            obj(vec![
                ("token", s(&early)),
                ("tolerance", n(tolerance)),
                ("before", n(0.0)),
                ("after", n(0.0)),
            ]),
            1_000_000,
        )
    };
    assert_eq!(
        early(0.0),
        (s("JWT is not active yet"), Validity::Until(1_500_000))
    );
    assert_eq!(
        early(2.0),
        (s("JWT is not active yet"), Validity::Until(1_498_000))
    );

    // A decrypted JWE holds until its `exp`; one that fails to
    // decrypt cannot say when it would, so it polls.
    let sealed = match fixture.query("seal", obj(vec![("exp", n(2_000.0))])) {
        Ok(Value::String(sealed)) => sealed,
        other => panic!("a JWE is text: {other:?}"),
    };
    let open = |right: bool| {
        fixture.certified(
            "open",
            obj(vec![("token", s(&sealed)), ("right", Value::Bool(right))]),
            1_000_000,
        )
    };
    assert_eq!(open(true), (s("ok"), Validity::Until(2_000_000)));
    let (failed, validity) = open(false);
    assert_ne!(failed, s("ok"));
    assert_eq!(validity, Validity::Polled);
}

#[test]
fn token_results_answer_from_the_cache_until_they_expire() {
    let fixture = Fixture::new("cache");
    let token = fixture.token(0.0, 2_000.0);
    // Declaring a later instant after the check must not stretch it.
    for after in [0.0, 3_000_000.0, -1.0] {
        let args = obj(vec![
            ("token", s(&token)),
            ("tolerance", n(0.0)),
            ("before", n(0.0)),
            ("after", n(after)),
        ]);
        let reader = fixture.engine.reader();
        let hits = reader.query_cache_stats().hits;
        let at = |now_ms: u64| {
            let options = CallOptions {
                timestamp_ms: Some(now_ms),
                ..CallOptions::default()
            };
            reader
                .invoke_query(b"db", "app", "check", &args, &options)
                .unwrap()
        };
        assert_eq!(at(1_000_000), s("ok"));
        assert_eq!(at(1_999_999), s("ok"));
        let cached = reader.query_cache_stats().hits - hits;
        assert_eq!(cached, 1, "valid until its exp ({after})");
        assert_eq!(at(2_000_000), s("JWT expired"), "{after}");
        let cached = reader.query_cache_stats().hits - hits;
        assert_eq!(cached, 1, "not at its exp ({after})");
    }
}

#[test]
fn a_derived_token_check_answers_at_each_readers_clock() {
    // The outcome keeps the instant its check declared: readers hold
    // until it, served from the cache or not, and past it the value
    // evaluates again, as v1's cells over their advancing clock.
    let fixture = Fixture::new("derived");
    let token = fixture.token(0.0, 2_000.0);
    let args = obj(vec![("token", s(&token))]);
    let at = |now_ms: u64| fixture.certified("read_checked", args.clone(), now_ms);
    assert_eq!(at(1_000_000), (s("ok"), Validity::Until(2_000_000)));
    assert_eq!(at(1_000_001), (s("ok"), Validity::Until(2_000_000)));
    assert_eq!(at(2_000_000), (s("JWT expired"), Validity::Stable));
    assert_eq!(at(3_000_000), (s("JWT expired"), Validity::Stable));
    // One not active yet answers so until its `nbf`, then holds.
    let early = fixture.token(1_500.0, 2_000.0);
    let args = obj(vec![("token", s(&early))]);
    let at = |now_ms: u64| fixture.certified("read_checked", args.clone(), now_ms);
    assert_eq!(
        at(1_000_000),
        (s("JWT is not active yet"), Validity::Until(1_500_000))
    );
    assert_eq!(
        at(1_499_999),
        (s("JWT is not active yet"), Validity::Until(1_500_000))
    );
    assert_eq!(at(1_600_000), (s("ok"), Validity::Until(2_000_000)));
}

#[test]
fn a_guest_local_source_draws_randomness_anywhere_but_jwe_nonces() {
    let fixture = Fixture::new("prng");
    let source = |name: &str| obj(vec![("source", s(name))]);
    // Host entropy serves mutations only.
    let refused = fixture.query("key_pair", source("host")).unwrap_err();
    assert_eq!(refused.code, codes::INVALID_INVOCATION, "{refused:?}");
    // A guest-local source serves a query: the key pair is the one its
    // bytes seed.
    let flower_crypto::nacl::Output::Bytes(packed) =
        flower_crypto::nacl::execute(12, &[&[7u8; 32]]).unwrap()
    else {
        panic!("a seeded key pair is bytes");
    };
    assert_eq!(
        fixture.query("key_pair", source("sevens")),
        Ok(s(&hex_encode(&packed[..32])))
    );
    let failed = fixture.query("key_pair", source("broken")).unwrap_err();
    assert_eq!(failed.code, "PRNG_DOWN");
    // WebAuthn challenges draw from it too.
    let options = fixture.query("passkey", source("sevens")).unwrap();
    let Value::Object(options) = options else {
        panic!("options are an object");
    };
    assert_eq!(options["challenge"], s(&base64url_encode(&[7u8; 32])));
    let refused = fixture.query("passkey", source("host")).unwrap_err();
    assert_eq!(refused.code, codes::INVALID_INVOCATION);
    // A JWE nonce the SDK draws never comes from it, as v1's.
    let refused = fixture.query("seal_drawn", source("sevens")).unwrap_err();
    assert_eq!(refused.code, codes::INVALID_INVOCATION, "{refused:?}");
}
