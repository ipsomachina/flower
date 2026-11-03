//! The replicated managed-key catalog and guest crypto dispatch:
//! members applying the same changes answer the same, databases keep
//! catalogs of their own, every algorithm works through bindings and
//! declarations, the lifecycle is permanent, the prepared-key cache
//! stays within its budget, and raw NaCl, JOSE and WebAuthn reach
//! guests through the host.

use std::sync::Arc;

use flower_crypto::catalog::{self, CatalogChange, apply_change};
use flower_crypto::host::CryptoHost;
use flower_crypto::replicated::{ManagedKeys, Prepared, WrappingKey, seal_import};
use flower_sdk::abi::InvocationKind;
use flower_sdk::crypto::{hex_decode, hex_encode};
use flower_sdk::{Failure, Value};
use serde_json::json;

/// The database the tests' modules live in.
const DB: &[u8] = b"app";

/// Members applying the same log: `stored` plays database `db`'s
/// replicated catalog row, and every member installs each applied
/// revision.
struct Group {
    db: &'static [u8],
    stored: Option<Vec<u8>>,
    members: Vec<Arc<ManagedKeys>>,
}

impl Group {
    fn new(members: Vec<Arc<ManagedKeys>>) -> Self {
        Group::of(DB, members)
    }

    /// Members applying database `db`'s catalog.
    fn of(db: &'static [u8], members: Vec<Arc<ManagedKeys>>) -> Self {
        Group {
            db,
            stored: None,
            members,
        }
    }

    /// Prepare on `leader`, apply as every member would, install.
    fn run(
        &mut self,
        leader: usize,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        match self.members[leader].prepare(self.stored.as_deref(), &request) {
            Ok(Prepared::Response(response)) => Ok(response),
            Ok(Prepared::Change(change)) => {
                let applied = apply_change(self.stored.as_deref(), &change)?;
                if let Some(catalog) = applied.catalog {
                    self.stored = Some(catalog);
                }
                for member in &self.members {
                    member.install(self.db, self.stored.as_deref()).unwrap();
                }
                Ok(applied.response)
            }
            Err(error) => Err(format!("{error:#}")),
        }
    }

    fn ok(&mut self, leader: usize, request: serde_json::Value) -> serde_json::Value {
        self.run(leader, request.clone())
            .unwrap_or_else(|error| panic!("{request}: {error}"))
    }
}

fn wrapping(byte: u8) -> WrappingKey {
    WrappingKey::new(&[byte; 32])
}

fn member(active: Option<u8>, previous: &[u8]) -> Arc<ManagedKeys> {
    Arc::new(ManagedKeys::new(
        active.map(wrapping),
        previous.iter().map(|byte| wrapping(*byte)).collect(),
    ))
}

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

/// A manifest declaring `(alias, algorithm, usages)`.
fn manifest(keys: &[(&str, &str, &[&str])]) -> Value {
    let keys = keys
        .iter()
        .map(|(name, algorithm, usages)| {
            Value::Object(
                [
                    ("name".to_string(), s(name)),
                    ("algorithm".to_string(), s(algorithm)),
                    (
                        "usages".to_string(),
                        Value::Array(usages.iter().map(|usage| s(usage)).collect()),
                    ),
                ]
                .into_iter()
                .collect(),
            )
        })
        .collect();
    Value::Object(
        [("keys".to_string(), Value::Array(keys))]
            .into_iter()
            .collect(),
    )
}

/// A host over a member's catalogs, scoped to a module of [`DB`] and
/// its declarations.
fn host(keys: &Arc<ManagedKeys>, declared: &[(&str, &str, &[&str])]) -> CryptoHost {
    host_in(DB, keys, declared)
}

/// [`host`] for a module of database `db`.
fn host_in(db: &[u8], keys: &Arc<ManagedKeys>, declared: &[(&str, &str, &[&str])]) -> CryptoHost {
    CryptoHost::new()
        .with_catalog(keys.clone())
        .for_module(db, "app", &manifest(declared))
        .unwrap()
}

fn call(host: &CryptoHost, op: u32, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host
        .crypto_call_for(InvocationKind::Mutation, op, &input)
        .unwrap();
    flower_sdk::abi::decode_envelope(Value::decode_binary(&output).unwrap())
}

/// Op 9: `[operation, alias, version, [hex...], options, now]`.
fn managed(
    host: &CryptoHost,
    operation: &str,
    alias: &str,
    version: Option<u32>,
    args: &[&[u8]],
    options: &str,
    now_ms: f64,
) -> Result<Value, Failure> {
    call(
        host,
        9,
        vec![
            s(operation),
            s(alias),
            version.map_or(Value::Null, |version| Value::Number(f64::from(version))),
            Value::Array(args.iter().map(|arg| s(&hex_encode(arg))).collect()),
            s(options),
            Value::Number(now_ms),
        ],
    )
}

fn text(value: Value) -> String {
    match value {
        Value::String(text) => text,
        other => panic!("expected text, got {other:?}"),
    }
}

fn bytes(value: Value) -> Vec<u8> {
    hex_decode(&text(value)).unwrap()
}

fn claims(value: &Value) -> &Value {
    match value {
        Value::Object(map) => &map["claims"],
        other => panic!("expected a verified token, got {other:?}"),
    }
}

fn header(value: &Value, name: &str) -> Option<String> {
    let Value::Object(map) = value else {
        panic!("expected a verified token, got {value:?}");
    };
    let Value::Object(header) = &map["protectedHeader"] else {
        panic!("no protected header");
    };
    header.get(name).map(|value| text(value.clone()))
}

const NOW: f64 = 1_700_000_000_000.0;
/// The `exp` of the claims tokens here carry, in milliseconds.
const EXP_MS: f64 = 1_700_000_900_000.0;

/// When a token check says its outcome next changes.
fn changes_at(value: &Value) -> Option<f64> {
    match value {
        Value::Object(map) => map.get("changesAt").and_then(Value::as_f64),
        _ => None,
    }
}
const SIGNING: &[&str] = &["sign", "verify", "publicKey"];

#[test]
fn every_member_answers_the_same() {
    // Two members mount the wrapping key, a third does not.
    let mut group = Group::new(vec![
        member(Some(1), &[]),
        member(Some(1), &[]),
        member(None, &[]),
    ]);
    group.ok(
        0,
        json!({"operation":"generate","requestId":"r1","name":"signing","algorithm":"Ed25519"}),
    );
    let bound = group.ok(
        1,
        json!({"operation":"bind","requestId":"r2","name":"sessions","key":"signing","usages":["sign","verify","publicKey"]}),
    );
    assert_eq!(bound["revision"], 2);
    assert_eq!(bound["duplicate"], false);
    assert_eq!(bound["value"]["bindings"]["sessions"]["key"], "signing");
    // No envelope reaches operators.
    assert!(!bound.to_string().contains("wrappedDek"));
    let views: Vec<serde_json::Value> = group
        .members
        .iter()
        .map(|keys| keys.public_catalog(DB))
        .collect();
    assert!(views.iter().all(|view| *view == views[0]));

    let declared: &[(&str, &str, &[&str])] = &[("sessions", "Ed25519", SIGNING)];
    let hosts: Vec<CryptoHost> = group
        .members
        .iter()
        .map(|keys| host(keys, declared))
        .collect();
    let message = s(&hex_encode(b"release"));
    // The typed Ed25519 ops resolve catalog aliases too.
    let signatures: Vec<Value> = hosts[..2]
        .iter()
        .map(|host| call(host, 3, vec![s("sessions"), Value::Null, message.clone()]).unwrap())
        .collect();
    assert_eq!(signatures[0], signatures[1]);
    let public: Vec<Value> = hosts[..2]
        .iter()
        .map(|host| call(host, 5, vec![s("sessions"), Value::Null]).unwrap())
        .collect();
    assert_eq!(public[0], public[1]);
    assert_eq!(
        call(
            &hosts[1],
            4,
            vec![
                s("sessions"),
                Value::Null,
                message.clone(),
                signatures[0].clone()
            ]
        )
        .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        call(&hosts[0], 6, vec![s("sessions")]).unwrap(),
        Value::Number(1.0)
    );

    // A token signed on one member verifies on another, with the
    // version's kid.
    let token = text(
        managed(
            &hosts[0],
            "jwt.sign",
            "sessions",
            None,
            &[br#"{"sub":"ada","exp":1700000900}"#],
            "{}",
            0.0,
        )
        .unwrap(),
    );
    let verified = managed(
        &hosts[1],
        "jwt.verify",
        "sessions",
        None,
        &[token.as_bytes()],
        "{}",
        NOW,
    )
    .unwrap();
    let Value::Object(claims_map) = claims(&verified) else {
        panic!("claims are an object");
    };
    assert_eq!(claims_map["sub"], s("ada"));
    let id = bound["value"]["keys"]["signing"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(header(&verified, "kid"), Some(format!("flower.{id}.1")));
    assert_eq!(header(&verified, "alg").as_deref(), Some("EdDSA"));

    // The member without the wrapping key applies the catalog but
    // serves no material.
    let locked = call(&hosts[2], 3, vec![s("sessions"), Value::Null, message]).unwrap_err();
    assert_eq!(locked.code, "KEYS_LOCKED", "{locked:?}");
    assert!(group.members[2].validate_ready().is_err());
    assert!(group.members[0].validate_ready().is_ok());
}

#[test]
fn lifecycle_rotates_retires_revokes_destroys_and_rewraps() {
    let mut group = Group::new(vec![member(Some(1), &[])]);
    group.ok(
        0,
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    );
    group.ok(0, json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign","verify"]}));
    let declared: &[(&str, &str, &[&str])] = &[("sessions", "Ed25519", &["sign", "verify"])];
    let leader = host(&group.members[0], declared);
    let claims = br#"{"sub":"ada","exp":1700000900}"#;
    let token_v1 =
        text(managed(&leader, "jwt.sign", "sessions", None, &[claims], "{}", 0.0).unwrap());
    let signature_v1 =
        bytes(managed(&leader, "sign", "sessions", None, &[b"m"], "{}", 0.0).unwrap());

    // Rotation: the new version signs, history still verifies by kid
    // or pin, and history never signs.
    let rotated = group.ok(
        0,
        json!({"operation":"rotate","requestId":"rot","name":"signing"}),
    );
    assert_eq!(rotated["value"]["keys"]["signing"]["activeVersion"], 2);
    let leader = host(&group.members[0], declared);
    managed(
        &leader,
        "jwt.verify",
        "sessions",
        None,
        &[token_v1.as_bytes()],
        "{}",
        NOW,
    )
    .unwrap();
    let token_v2 =
        text(managed(&leader, "jwt.sign", "sessions", None, &[claims], "{}", 0.0).unwrap());
    assert_ne!(token_v1, token_v2);
    assert_eq!(
        managed(
            &leader,
            "verify",
            "sessions",
            Some(1),
            &[b"m", &signature_v1],
            "{}",
            0.0
        )
        .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        managed(
            &leader,
            "verify",
            "sessions",
            None,
            &[b"m", &signature_v1],
            "{}",
            0.0
        )
        .unwrap(),
        Value::Bool(false)
    );
    let historical = managed(&leader, "sign", "sessions", Some(1), &[b"m"], "{}", 0.0).unwrap_err();
    assert_eq!(historical.code, "FORBIDDEN_USAGE");

    // Retiring the active version stops signing until a rotation.
    group.ok(
        0,
        json!({"operation":"retire","requestId":"ret","name":"signing","version":2}),
    );
    let retired = managed(&leader, "sign", "sessions", None, &[b"m"], "{}", 0.0).unwrap_err();
    assert_eq!(retired.code, "FORBIDDEN_USAGE");
    managed(
        &leader,
        "jwt.verify",
        "sessions",
        None,
        &[token_v2.as_bytes()],
        "{}",
        NOW,
    )
    .unwrap();
    group.ok(
        0,
        json!({"operation":"rotate","requestId":"rot2","name":"signing"}),
    );
    managed(&leader, "sign", "sessions", None, &[b"m"], "{}", 0.0).unwrap();

    // Revocation stops verification; destruction drops the envelope.
    group.ok(
        0,
        json!({"operation":"revoke","requestId":"rev","name":"signing","version":1}),
    );
    let revoked = managed(
        &leader,
        "jwt.verify",
        "sessions",
        None,
        &[token_v1.as_bytes()],
        "{}",
        NOW,
    )
    .unwrap_err();
    assert_eq!(revoked.code, "KEY_REVOKED", "{revoked:?}");
    let destroyed = group.ok(
        0,
        json!({"operation":"destroy","requestId":"des","name":"signing","version":2}),
    );
    let versions = destroyed["value"]["keys"]["signing"]["versions"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(versions[1]["destroyed"], true);
    assert_eq!(versions[1]["revoked"], true);
    assert!(
        managed(
            &leader,
            "jwt.verify",
            "sessions",
            None,
            &[token_v2.as_bytes()],
            "{}",
            NOW
        )
        .is_err()
    );
    // A malicious change cannot bring history back.
    let mut forged: catalog::Catalog = catalog::decode(group.stored.as_deref()).unwrap();
    forged.revision += 1;
    forged
        .keys
        .get_mut("signing")
        .unwrap()
        .versions
        .get_mut(&1)
        .unwrap()
        .revoked = false;
    let change = catalog::encode_change(&CatalogChange {
        request_id: "forge".to_string(),
        digest: "00".to_string(),
        base_revision: forged.revision - 1,
        catalog: forged,
    });
    let refused = apply_change(group.stored.as_deref(), &change).unwrap_err();
    assert!(refused.contains("permanent"), "{refused}");

    // Wrapping-key rotation: mount the new key with the old one as
    // previous, rewrap, and only the new key is needed afterwards.
    let rewrapping = member(Some(2), &[1]);
    rewrapping.install(DB, group.stored.as_deref()).unwrap();
    group.members.push(rewrapping);
    let before = group.members[0].public_catalog(DB);
    let rewrapped = group.ok(1, json!({"operation":"rewrap","requestId":"wrap"}));
    let new_id = WrappingKey::new(&[2; 32]).id().to_string();
    let active = &rewrapped["value"]["keys"]["signing"]["versions"][2];
    assert_eq!(active["wrappingId"], new_id.as_str());
    assert_eq!(
        active["kid"],
        before["keys"]["signing"]["versions"][2]["kid"]
    );
    let fresh = member(Some(2), &[]);
    fresh.install(DB, group.stored.as_deref()).unwrap();
    managed(
        &host(&fresh, declared),
        "sign",
        "sessions",
        None,
        &[b"m"],
        "{}",
        0.0,
    )
    .unwrap();
    let old_only = member(Some(1), &[]);
    old_only.install(DB, group.stored.as_deref()).unwrap();
    let locked = managed(
        &host(&old_only, declared),
        "sign",
        "sessions",
        None,
        &[b"m"],
        "{}",
        0.0,
    )
    .unwrap_err();
    assert_eq!(locked.code, "KEYS_LOCKED");
}

#[test]
fn changes_conflict_retry_and_refuse_reused_request_ids() {
    let leader = member(Some(1), &[]);
    let request_a = json!({"operation":"generate","requestId":"a","name":"a","algorithm":"HS256"});
    let request_b = json!({"operation":"generate","requestId":"b","name":"b","algorithm":"HS256"});
    let Prepared::Change(a) = leader.prepare(None, &request_a).unwrap() else {
        panic!("generate proposes a change");
    };
    let Prepared::Change(b) = leader.prepare(None, &request_b).unwrap() else {
        panic!("generate proposes a change");
    };
    let applied = apply_change(None, &a).unwrap();
    let stored = applied.catalog.unwrap();
    // Prepared against a revision that moved: a conflict that changes
    // nothing, for the leader to prepare again.
    let conflict = apply_change(Some(&stored), &b).unwrap_err();
    assert!(conflict.starts_with("KEY_CATALOG_CONFLICT"), "{conflict}");
    let Prepared::Change(b) = leader.prepare(Some(&stored), &request_b).unwrap() else {
        panic!("generate proposes a change");
    };
    let stored = apply_change(Some(&stored), &b).unwrap().catalog.unwrap();

    // A retried request returns its receipt, before and after the log.
    let Prepared::Response(retried) = leader.prepare(Some(&stored), &request_a).unwrap() else {
        panic!("a committed request ID answers from its receipt");
    };
    assert_eq!(retried["duplicate"], true);
    assert_eq!(retried["revision"], 1);
    let replayed = apply_change(Some(&stored), &a).unwrap();
    assert!(replayed.catalog.is_none());
    assert_eq!(replayed.response["duplicate"], true);

    // The same ID with other contents is refused, never applied.
    let reused = json!({"operation":"generate","requestId":"a","name":"c","algorithm":"HS256"});
    let error = leader.prepare(Some(&stored), &reused).err().unwrap();
    assert!(
        format!("{error:#}").starts_with("KEY_REQUEST_REUSED"),
        "{error:#}"
    );

    // Malformed catalogs never apply: a binding to a missing key.
    let mut bad = catalog::decode(Some(&stored)).unwrap();
    bad.revision += 1;
    bad.bindings.insert(
        "ghost".to_string(),
        catalog::Binding {
            key: "missing".to_string(),
            usages: ["sign".to_string()].into_iter().collect(),
        },
    );
    let change = catalog::encode_change(&CatalogChange {
        request_id: "bad".to_string(),
        digest: "00".to_string(),
        base_revision: bad.revision - 1,
        catalog: bad,
    });
    assert!(apply_change(Some(&stored), &change).is_err());
    // Usages outside the algorithm never bind.
    let error = leader
        .prepare(
            Some(&stored),
            &json!({"operation":"bind","requestId":"x","name":"x","key":"a","usages":["encrypt"]}),
        )
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("HS256"), "{error:#}");
}

#[test]
fn every_algorithm_serves_through_bindings() {
    let mut group = Group::new(vec![member(Some(7), &[])]);
    let all: &[(&str, &str, &[&str])] = &[
        ("ed", "Ed25519", SIGNING),
        ("p256", "P256", SIGNING),
        ("rsa", "RSA", SIGNING),
        ("hmac", "HS256", &["sign", "verify"]),
        ("aes", "A256GCM", &["encrypt", "decrypt"]),
        ("box", "XSalsa20Poly1305", &["encrypt", "decrypt"]),
        ("dh", "X25519", &["encrypt", "decrypt", "publicKey"]),
    ];
    for (index, (alias, algorithm, usages)) in all.iter().enumerate() {
        group.ok(0, json!({"operation":"generate","requestId":format!("g{index}"),"name":alias,"algorithm":algorithm}));
        group.ok(0, json!({"operation":"bind","requestId":format!("b{index}"),"name":alias,"key":alias,"usages":usages}));
    }
    let host = host(&group.members[0], all);
    let claims = br#"{"iss":"shop","exp":1700000900}"#;
    let options = r#"{"issuer":"shop"}"#;
    for (alias, expected) in [
        ("ed", "EdDSA"),
        ("p256", "ES256"),
        ("rsa", "RS256"),
        ("hmac", "HS256"),
    ] {
        let token = text(managed(&host, "jwt.sign", alias, None, &[claims], "{}", 0.0).unwrap());
        let verified = managed(
            &host,
            "jwt.verify",
            alias,
            None,
            &[token.as_bytes()],
            options,
            NOW,
        )
        .unwrap();
        assert_eq!(header(&verified, "alg").as_deref(), Some(expected));
        // The guest declares when the outcome changes: at `exp`.
        assert_eq!(changes_at(&verified), Some(EXP_MS));
        // Expiry is checked at the clock the guest passes.
        let expired = managed(
            &host,
            "jwt.verify",
            alias,
            None,
            &[token.as_bytes()],
            options,
            NOW + 1e6,
        )
        .unwrap_err();
        assert!(expired.message.contains("expired"), "{expired:?}");
        // An expired token stays expired: nothing to declare.
        assert_eq!(expired.details, None);
        let signature =
            bytes(managed(&host, "sign", alias, None, &[b"payload"], "{}", 0.0).unwrap());
        assert_eq!(
            managed(
                &host,
                "verify",
                alias,
                None,
                &[b"payload", &signature],
                "{}",
                0.0
            )
            .unwrap(),
            Value::Bool(true)
        );
    }
    // ES256 signs deterministically (RFC 6979), so queries stay pure.
    let first = managed(&host, "jwt.sign", "p256", None, &[claims], "{}", 0.0).unwrap();
    assert_eq!(
        first,
        managed(&host, "jwt.sign", "p256", None, &[claims], "{}", 0.0).unwrap()
    );
    // Managed tokens verify with standard JOSE over the SPKI public key.
    for (alias, algorithm) in [("rsa", "RS256"), ("p256", "ES256")] {
        let spki = bytes(managed(&host, "publicKey", alias, None, &[], "{}", 0.0).unwrap());
        assert_eq!(spki[0], 0x30, "{alias} public keys are SPKI DER");
        let token = text(managed(&host, "jwt.sign", alias, None, &[claims], "{}", 0.0).unwrap());
        let options =
            format!(r#"{{"algorithms":["{algorithm}"],"keyFormat":"der","issuer":"shop"}}"#);
        call(
            &host,
            8,
            vec![
                s("verify"),
                s(&token),
                s(&hex_encode(&spki)),
                s(&options),
                Value::Number(NOW),
            ],
        )
        .unwrap();
    }
    assert_eq!(
        bytes(managed(&host, "publicKey", "ed", None, &[], "{}", 0.0).unwrap()).len(),
        32
    );

    // A256GCM: compact JWE with the version kid; tampering fails.
    let token = text(
        managed(
            &host,
            "jwt.encrypt",
            "aes",
            None,
            &[claims, &[9u8; 12]],
            "{}",
            0.0,
        )
        .unwrap(),
    );
    let decrypted = managed(
        &host,
        "jwt.decrypt",
        "aes",
        None,
        &[token.as_bytes()],
        options,
        NOW,
    )
    .unwrap();
    assert_eq!(header(&decrypted, "enc").as_deref(), Some("A256GCM"));
    assert!(header(&decrypted, "kid").unwrap().starts_with("flower."));
    assert_eq!(changes_at(&decrypted), Some(EXP_MS));
    let mut tampered = token.clone().into_bytes();
    let last = tampered.len() - 2;
    tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
    assert!(
        managed(
            &host,
            "jwt.decrypt",
            "aes",
            None,
            &[&tampered],
            options,
            NOW
        )
        .is_err()
    );

    // Secretbox over XSalsa20Poly1305.
    let nonce = [3u8; 24];
    let sealed = bytes(
        managed(
            &host,
            "nacl.secretbox",
            "box",
            None,
            &[b"secret", &nonce],
            "{}",
            0.0,
        )
        .unwrap(),
    );
    assert_eq!(
        bytes(
            managed(
                &host,
                "nacl.secretbox.open",
                "box",
                None,
                &[&sealed, &nonce],
                "{}",
                0.0
            )
            .unwrap()
        ),
        b"secret"
    );
    let mut flipped = sealed.clone();
    flipped[0] ^= 1;
    assert_eq!(
        managed(
            &host,
            "nacl.secretbox.open",
            "box",
            None,
            &[&flipped, &nonce],
            "{}",
            0.0
        )
        .unwrap(),
        Value::Null
    );

    // Box over X25519 interoperates with raw NaCl on the other side.
    let peer_secret = [5u8; 32];
    let peer_public = bytes(
        call(
            &host,
            7,
            vec![Value::Number(4.0), s(&hex_encode(&peer_secret))],
        )
        .unwrap(),
    );
    let managed_public =
        bytes(managed(&host, "nacl.scalarMult.base", "dh", None, &[], "{}", 0.0).unwrap());
    let boxed = bytes(
        managed(
            &host,
            "nacl.box",
            "dh",
            None,
            &[b"hello", &nonce, &peer_public],
            "{}",
            0.0,
        )
        .unwrap(),
    );
    let opened = call(
        &host,
        7,
        vec![
            Value::Number(7.0),
            s(&hex_encode(&boxed)),
            s(&hex_encode(&nonce)),
            s(&hex_encode(&managed_public)),
            s(&hex_encode(&peer_secret)),
        ],
    )
    .unwrap();
    assert_eq!(bytes(opened), b"hello");
    // Managed shared secrets are never exported.
    let refused = managed(
        &host,
        "nacl.box.before",
        "dh",
        None,
        &[&peer_public],
        "{}",
        0.0,
    )
    .unwrap_err();
    assert_eq!(refused.code, "FORBIDDEN_USAGE");

    // Ed25519 NaCl signatures stay strict and interoperate with raw
    // verification.
    let signed = bytes(managed(&host, "nacl.sign", "ed", None, &[b"msg"], "{}", 0.0).unwrap());
    assert_eq!(
        bytes(managed(&host, "nacl.sign.open", "ed", None, &[&signed], "{}", 0.0).unwrap()),
        b"msg"
    );
    let public = bytes(managed(&host, "publicKey", "ed", None, &[], "{}", 0.0).unwrap());
    let raw = call(
        &host,
        7,
        vec![
            Value::Number(9.0),
            s(&hex_encode(&signed)),
            s(&hex_encode(&public)),
        ],
    )
    .unwrap();
    assert_eq!(bytes(raw), b"msg");
}

#[test]
fn declarations_and_bindings_both_limit_use() {
    let mut group = Group::new(vec![member(Some(1), &[])]);
    group.ok(
        0,
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    );
    group.ok(0, json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign"]}));
    let keys = &group.members[0].clone();
    let message = s(&hex_encode(b"m"));

    // The binding grants sign only: declaring verify does not add it.
    let both = host(keys, &[("sessions", "Ed25519", &["sign", "verify"])]);
    let signature = call(&both, 3, vec![s("sessions"), Value::Null, message.clone()]).unwrap();
    let refused = call(
        &both,
        4,
        vec![s("sessions"), Value::Null, message.clone(), signature],
    )
    .unwrap_err();
    assert_eq!(refused.code, "FORBIDDEN_USAGE");
    // A declaration asking for less narrows the grant.
    let verify_only = host(keys, &[("sessions", "Ed25519", &["verify"])]);
    let refused = call(
        &verify_only,
        3,
        vec![s("sessions"), Value::Null, message.clone()],
    )
    .unwrap_err();
    assert_eq!(refused.code, "FORBIDDEN_USAGE");
    // The declared algorithm must match the bound key's.
    let wrong = host(keys, &[("sessions", "P256", &["sign"])]);
    let refused = call(&wrong, 3, vec![s("sessions"), Value::Null, message.clone()]).unwrap_err();
    assert_eq!(refused.code, "FORBIDDEN_USAGE");
    // Undeclared aliases and unscoped hosts resolve no catalog key.
    let undeclared = host(keys, &[]);
    let refused = call(
        &undeclared,
        3,
        vec![s("sessions"), Value::Null, message.clone()],
    )
    .unwrap_err();
    assert_eq!(refused.code, "FORBIDDEN_USAGE");
    let unscoped = CryptoHost::new().with_catalog(keys.clone());
    let refused = call(
        &unscoped,
        3,
        vec![s("sessions"), Value::Null, message.clone()],
    )
    .unwrap_err();
    assert_eq!(refused.code, "FORBIDDEN_USAGE");
    // Conflicting or invalid declarations reject the module.
    let base = CryptoHost::new().with_catalog(keys.clone());
    let conflicting = manifest(&[
        ("sessions", "Ed25519", &["sign"]),
        ("sessions", "P256", &["sign"]),
    ]);
    assert!(base.for_module(DB, "app", &conflicting).is_err());
    assert!(
        base.for_module(DB, "app", &manifest(&[("sessions", "HS256", &["encrypt"])]))
            .is_err()
    );
    // Unbinding takes the alias away.
    group.ok(
        0,
        json!({"operation":"unbind","requestId":"u","name":"sessions"}),
    );
    let gone = call(&both, 3, vec![s("sessions"), Value::Null, message.clone()]).unwrap_err();
    assert_eq!(gone.code, "UNKNOWN_KEY");
    // Nothing else answers an alias the catalog does not bind: no
    // member-local key signs, verifies or discloses in its place.
    for (op, args) in [
        (3, vec![s("sessions"), Value::Null, message.clone()]),
        (
            4,
            vec![s("sessions"), Value::Null, message, s(&"00".repeat(64))],
        ),
        (5, vec![s("sessions"), Value::Null]),
        (6, vec![s("sessions")]),
    ] {
        assert_eq!(call(&both, op, args).unwrap_err().code, "UNKNOWN_KEY");
    }
}

#[test]
fn databases_keep_catalogs_of_their_own() {
    // One member, two databases whose operators provision the same
    // names under the same request IDs: neither is a retry of the other.
    let keys = member(Some(1), &[]);
    let mut first = Group::of(b"tenant-a", vec![keys.clone()]);
    let mut second = Group::of(b"tenant-b", vec![keys.clone()]);
    let mut ids = Vec::new();
    for group in [&mut first, &mut second] {
        let generated = group.ok(
            0,
            json!({"operation":"generate","requestId":"generate","name":"signer","algorithm":"Ed25519"}),
        );
        assert_eq!(generated["duplicate"], false, "{generated}");
        assert_eq!(generated["revision"], 1);
        ids.push(generated["value"]["keys"]["signer"]["id"].clone());
        group.ok(0, json!({"operation":"bind","requestId":"bind","name":"sessions","key":"signer","usages":["sign","verify","publicKey"]}));
    }
    assert_ne!(ids[0], ids[1], "each database generated its own key");
    assert_ne!(
        keys.public_catalog(b"tenant-a")["domain"],
        keys.public_catalog(b"tenant-b")["domain"]
    );

    let declared: &[(&str, &str, &[&str])] = &[("sessions", "Ed25519", SIGNING)];
    let (a, b) = (
        host_in(b"tenant-a", &keys, declared),
        host_in(b"tenant-b", &keys, declared),
    );
    let public = |host: &CryptoHost| call(host, 5, vec![s("sessions"), Value::Null]).unwrap();
    assert_ne!(public(&a), public(&b), "one alias, two keys");

    // A token tenant A issued verifies there and nowhere else, though
    // tenant B binds the same alias with the same usages.
    let token = text(
        managed(
            &a,
            "jwt.sign",
            "sessions",
            None,
            &[br#"{"sub":"tenant","exp":1700000900}"#],
            "{}",
            0.0,
        )
        .unwrap(),
    );
    managed(
        &a,
        "jwt.verify",
        "sessions",
        None,
        &[token.as_bytes()],
        "{}",
        NOW,
    )
    .unwrap();
    let refused = managed(
        &b,
        "jwt.verify",
        "sessions",
        None,
        &[token.as_bytes()],
        "{}",
        NOW,
    )
    .unwrap_err();
    assert_eq!(refused.code, "UNKNOWN_KEY", "{refused:?}");
    let message = s(&hex_encode(b"m"));
    let signature = call(&a, 3, vec![s("sessions"), Value::Null, message.clone()]).unwrap();
    assert_eq!(
        call(
            &b,
            4,
            vec![s("sessions"), Value::Null, message.clone(), signature]
        )
        .unwrap(),
        Value::Bool(false)
    );

    // An alias only tenant A binds is unknown to tenant B's modules,
    // and a database with no catalog resolves nothing.
    first.ok(0, json!({"operation":"bind","requestId":"only","name":"only-a","key":"signer","usages":["sign"]}));
    let only: &[(&str, &str, &[&str])] = &[("only-a", "Ed25519", &["sign"])];
    call(
        &host_in(b"tenant-a", &keys, only),
        3,
        vec![s("only-a"), Value::Null, message.clone()],
    )
    .unwrap();
    let unknown = call(
        &host_in(b"tenant-b", &keys, only),
        3,
        vec![s("only-a"), Value::Null, message.clone()],
    )
    .unwrap_err();
    assert_eq!(unknown.code, "UNKNOWN_KEY");
    let empty = call(
        &host_in(b"tenant-c", &keys, declared),
        3,
        vec![s("sessions"), Value::Null, message],
    )
    .unwrap_err();
    assert_eq!(empty.code, "UNKNOWN_KEY");

    // A database's catalog leaves with it, and only its own.
    let before = public(&b);
    keys.install(b"tenant-a", None).unwrap();
    assert_eq!(public(&b), before);
    assert_eq!(
        call(&a, 5, vec![s("sessions"), Value::Null])
            .unwrap_err()
            .code,
        "UNKNOWN_KEY"
    );
}

#[test]
fn a_destination_checks_a_catalog_before_it_serves_it() {
    let mut group = Group::new(vec![member(Some(1), &[])]);
    group.ok(
        0,
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    );
    group.ok(
        0,
        json!({"operation":"generate","requestId":"h","name":"gone","algorithm":"HS256"}),
    );
    // A destroyed version needs no material, nor does a revoked one.
    group.ok(
        0,
        json!({"operation":"destroy","requestId":"d","name":"gone"}),
    );
    let stored = group.stored.clone();
    assert!(member(Some(1), &[]).validate(stored.as_deref()).is_ok());
    assert!(member(Some(2), &[1]).validate(stored.as_deref()).is_ok());
    assert!(
        member(None, &[]).validate(None).is_ok(),
        "no catalog, nothing to unwrap"
    );
    for locked in [member(Some(2), &[]), member(None, &[])] {
        let error = format!("{:#}", locked.validate(stored.as_deref()).unwrap_err());
        assert!(error.contains("wrapping key"), "{error}");
    }
    group.ok(
        0,
        json!({"operation":"revoke","requestId":"r","name":"signing"}),
    );
    assert!(
        member(Some(2), &[])
            .validate(group.stored.as_deref())
            .is_ok()
    );
}

#[test]
fn prepared_key_cache_retains_within_its_budget() {
    let stats = |keys: &ManagedKeys, name: &str| keys.cache_stats()[name].as_u64().unwrap();
    let declared: &[(&str, &str, &[&str])] =
        &[("a", "Ed25519", &["sign"]), ("b", "Ed25519", &["sign"])];
    let setup = |keys: Arc<ManagedKeys>| {
        let mut group = Group::new(vec![keys]);
        for name in ["a", "b"] {
            group.ok(0, json!({"operation":"generate","requestId":format!("g{name}"),"name":name,"algorithm":"Ed25519"}));
            group.ok(0, json!({"operation":"bind","requestId":format!("b{name}"),"name":name,"key":name,"usages":["sign"]}));
        }
        group
    };

    // Warm uses hit the prepared context.
    let mut group = setup(member(Some(1), &[]));
    let keys = group.members[0].clone();
    let warm = host(&keys, declared);
    managed(&warm, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap();
    managed(&warm, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap();
    assert_eq!((stats(&keys, "loads"), stats(&keys, "hits")), (1, 1));
    assert_eq!(stats(&keys, "entries"), 1);
    // A new envelope for the same version (a rewrap) never reuses the
    // old context.
    group.ok(0, json!({"operation":"rewrap","requestId":"w","name":"a"}));
    managed(&warm, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap();
    assert_eq!(stats(&keys, "loads"), 2);

    // A budget of one context evicts the least recently used.
    let small = setup(Arc::new(
        ManagedKeys::new(Some(wrapping(1)), Vec::new()).with_cache(10_000, None),
    ));
    let keys = small.members[0].clone();
    let bounded = host(&keys, declared);
    managed(&bounded, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap();
    managed(&bounded, "sign", "b", None, &[b"m"], "{}", 0.0).unwrap();
    assert_eq!((stats(&keys, "entries"), stats(&keys, "evictions")), (1, 1));
    assert!(stats(&keys, "bytes") <= 10_000);

    // Zero disables retention, not crypto.
    let off = setup(Arc::new(
        ManagedKeys::new(Some(wrapping(1)), Vec::new()).with_cache(0, None),
    ));
    let keys = off.members[0].clone();
    let uncached = host(&keys, declared);
    managed(&uncached, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap();
    managed(&uncached, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap();
    assert_eq!(
        (
            stats(&keys, "entries"),
            stats(&keys, "loads"),
            stats(&keys, "hits")
        ),
        (0, 2, 0)
    );

    // Expired contexts reload.
    let expiring = setup(Arc::new(
        ManagedKeys::new(Some(wrapping(1)), Vec::new())
            .with_cache(1 << 20, Some(std::time::Duration::ZERO)),
    ));
    let keys = expiring.members[0].clone();
    let fresh = host(&keys, declared);
    managed(&fresh, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap();
    managed(&fresh, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap();
    assert_eq!((stats(&keys, "loads"), stats(&keys, "hits")), (2, 0));
}

#[test]
fn simultaneous_cold_uses_prepare_once_and_still_check_authority() {
    let keys = member(Some(1), &[]);
    let mut group = Group::new(vec![keys.clone()]);
    group.ok(
        0,
        json!({"operation":"generate","requestId":"g","name":"a","algorithm":"RSA","bits":2048}),
    );
    group.ok(
        0,
        json!({"operation":"bind","requestId":"b","name":"a","key":"a","usages":["sign"]}),
    );
    let barrier = std::sync::Barrier::new(24);
    std::thread::scope(|scope| {
        let joins: Vec<_> = (0..24)
            .map(|_| {
                scope.spawn(|| {
                    let crypto = host(&keys, &[("a", "RSA", &["sign"])]);
                    barrier.wait();
                    managed(&crypto, "sign", "a", None, &[b"m"], "{}", 0.0).unwrap()
                })
            })
            .collect();
        let results: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
        assert!(results.iter().all(|result| result == &results[0]));
    });
    assert_eq!(keys.cache_stats()["loads"], 1);
    group.ok(0, json!({"operation":"revoke","requestId":"r","name":"a"}));
    let crypto = host(&keys, &[("a", "RSA", &["sign"])]);
    assert!(managed(&crypto, "sign", "a", None, &[b"m"], "{}", 0.0).is_err());
}

#[test]
fn managed_shared_handles_are_scoped_bounded_and_reauthorized() {
    let keys = member(Some(1), &[]);
    let mut group = Group::new(vec![keys.clone()]);
    group.ok(
        0,
        json!({"operation":"generate","requestId":"g","name":"dh","algorithm":"X25519"}),
    );
    group.ok(0, json!({"operation":"bind","requestId":"b","name":"dh","key":"dh","usages":["derive","encrypt","decrypt","publicKey"]}));
    let declared: &[(&str, &str, &[&str])] = &[(
        "dh",
        "X25519",
        &["derive", "encrypt", "decrypt", "publicKey"],
    )];
    let crypto = host(&keys, declared);
    let peer = [5; 32];
    let peer_public =
        bytes(call(&crypto, 7, vec![Value::Number(4.0), s(&hex_encode(&peer))]).unwrap());
    let public = bytes(managed(&crypto, "publicKey", "dh", None, &[], "{}", 0.0).unwrap());
    assert_eq!(
        managed(
            &crypto,
            "nacl.box.before",
            "dh",
            None,
            &[&peer_public],
            "{}",
            0.0
        )
        .unwrap(),
        Value::Number(0.0)
    );
    let nonce = [3; 24];
    let cipher = bytes(
        managed(
            &crypto,
            "nacl.box.after",
            "dh",
            None,
            &[b"hello", &nonce],
            r#"{"shared":0}"#,
            0.0,
        )
        .unwrap(),
    );
    assert_eq!(
        bytes(
            call(
                &crypto,
                7,
                vec![
                    Value::Number(7.0),
                    s(&hex_encode(&cipher)),
                    s(&hex_encode(&nonce)),
                    s(&hex_encode(&public)),
                    s(&hex_encode(&peer))
                ]
            )
            .unwrap()
        ),
        b"hello"
    );
    assert_eq!(
        bytes(
            managed(
                &crypto,
                "nacl.box.open.after",
                "dh",
                None,
                &[&cipher, &nonce],
                r#"{"shared":0}"#,
                0.0
            )
            .unwrap()
        ),
        b"hello"
    );
    assert!(
        managed(
            &host(&keys, declared),
            "nacl.box.after",
            "dh",
            None,
            &[b"hello", &nonce],
            r#"{"shared":0}"#,
            0.0
        )
        .is_err()
    );
    let derive_only = host(&keys, &[("dh", "X25519", &["derive"])]);
    managed(
        &derive_only,
        "nacl.box.before",
        "dh",
        None,
        &[&peer_public],
        "{}",
        0.0,
    )
    .unwrap();
    assert!(
        managed(
            &derive_only,
            "nacl.box.after",
            "dh",
            None,
            &[b"hello", &nonce],
            r#"{"shared":0}"#,
            0.0
        )
        .is_err()
    );
    // Bounded native storage, even if the guest drops every returned slot.
    let exhausted = (0..2048).any(|_| {
        managed(
            &crypto,
            "nacl.box.before",
            "dh",
            None,
            &[&peer_public],
            "{}",
            0.0,
        )
        .is_err()
    });
    assert!(exhausted);
    group.ok(
        0,
        json!({"operation":"rotate","requestId":"rot","name":"dh"}),
    );
    assert!(
        managed(
            &crypto,
            "nacl.box.after",
            "dh",
            None,
            &[b"hello", &nonce],
            r#"{"shared":0}"#,
            0.0
        )
        .is_err()
    );
    assert_eq!(
        bytes(
            managed(
                &crypto,
                "nacl.box.open.after",
                "dh",
                None,
                &[&cipher, &nonce],
                r#"{"shared":0}"#,
                0.0
            )
            .unwrap()
        ),
        b"hello"
    );
    group.ok(
        0,
        json!({"operation":"revoke","requestId":"r","name":"dh","version":1}),
    );
    assert!(
        managed(
            &crypto,
            "nacl.box.open.after",
            "dh",
            None,
            &[&cipher, &nonce],
            r#"{"shared":0}"#,
            0.0
        )
        .is_err()
    );
}

/// A catalog that goes takes its prepared contexts with it: a database
/// whose row went (a move's source retiring it) and versions destroyed
/// or rewrapped leave no unwrapped material in the cache. A stored row
/// that does not decode serves no keys, never the revision before it.
#[test]
fn a_catalog_that_goes_takes_its_prepared_keys() {
    let entries = |keys: &ManagedKeys| keys.cache_stats()["entries"].as_u64().unwrap();
    let keys = member(Some(1), &[]);
    let declared: &[(&str, &str, &[&str])] = &[("sessions", "Ed25519", SIGNING)];
    let mut tenants = Vec::new();
    for db in [&b"tenant-a"[..], &b"tenant-b"[..]] {
        let mut group = Group::of(db, vec![keys.clone()]);
        group.ok(
            0,
            json!({"operation":"generate","requestId":"g","name":"signer","algorithm":"Ed25519"}),
        );
        group.ok(0, json!({"operation":"bind","requestId":"b","name":"sessions","key":"signer","usages":SIGNING}));
        tenants.push(group);
    }
    let (a, b) = (
        host_in(b"tenant-a", &keys, declared),
        host_in(b"tenant-b", &keys, declared),
    );
    let sign = |host: &CryptoHost| managed(host, "sign", "sessions", None, &[b"m"], "{}", 0.0);
    sign(&a).unwrap();
    sign(&b).unwrap();
    assert_eq!(entries(&keys), 2);

    // Tenant A's row goes: so does its context; tenant B's stays.
    keys.install(b"tenant-a", None).unwrap();
    assert_eq!(entries(&keys), 1);
    assert_eq!(sign(&a).unwrap_err().code, "UNKNOWN_KEY");
    sign(&b).unwrap();

    // A row that does not decode serves nothing, and keeps nothing.
    keys.install(b"tenant-b", Some(b"not a catalog"))
        .unwrap_err();
    assert_eq!(sign(&b).unwrap_err().code, "UNKNOWN_KEY");
    assert_eq!(entries(&keys), 0);
    let stored = tenants[1].stored.clone();
    keys.install(b"tenant-b", stored.as_deref()).unwrap();
    sign(&b).unwrap();
    assert_eq!(entries(&keys), 1);

    // A rewrapped envelope replaces its context; a destroyed one leaves
    // none.
    tenants[1].ok(
        0,
        json!({"operation":"rewrap","requestId":"w","name":"signer"}),
    );
    sign(&b).unwrap();
    assert_eq!(entries(&keys), 1);
    tenants[1].ok(
        0,
        json!({"operation":"rotate","requestId":"r","name":"signer"}),
    );
    sign(&b).unwrap();
    tenants[1].ok(
        0,
        json!({"operation":"destroy","requestId":"d","name":"signer","version":1}),
    );
    assert_eq!(
        entries(&keys),
        1,
        "only the rotated version's context stays"
    );
}

#[test]
fn rsa_rotation_keeps_the_key_size_unless_overridden() {
    let mut group = Group::new(vec![member(Some(1), &[])]);
    group.ok(
        0,
        json!({"operation":"generate","requestId":"g","name":"rsa","algorithm":"RSA","bits":3072}),
    );
    group.ok(
        0,
        json!({"operation":"bind","requestId":"b","name":"rsa","key":"rsa","usages":["publicKey"]}),
    );
    let host = host(&group.members[0], &[("rsa", "RSA", &["publicKey"])]);
    let spki =
        |host: &CryptoHost| bytes(managed(host, "publicKey", "rsa", None, &[], "{}", 0.0).unwrap());
    let original = spki(&host).len();
    group.ok(
        0,
        json!({"operation":"rotate","requestId":"r","name":"rsa"}),
    );
    assert_eq!(spki(&host).len(), original);
    group.ok(
        0,
        json!({"operation":"rotate","requestId":"r2","name":"rsa","bits":2048}),
    );
    assert!(spki(&host).len() < original);
    // Sizes outside RSA's set, and bits for other algorithms, refuse.
    assert!(
        group
            .run(
                0,
                json!({"operation":"rotate","requestId":"r3","name":"rsa","bits":1024})
            )
            .is_err()
    );
    assert!(group
        .run(0, json!({"operation":"generate","requestId":"e","name":"ed","algorithm":"Ed25519","bits":2048}))
        .is_err());
}

#[test]
fn imports_unseal_only_under_a_mounted_wrapping_key() {
    let mut group = Group::new(vec![member(Some(1), &[])]);
    let seed = [42u8; 32];
    let sealed = seal_import(&wrapping(1), &seed, "raw").unwrap();
    assert!(!sealed.to_string().contains(&hex_encode(&seed)));
    group.ok(0, json!({"operation":"import","requestId":"i","name":"imported","algorithm":"Ed25519","sealed":sealed}));
    group.ok(0, json!({"operation":"bind","requestId":"b","name":"imported","key":"imported","usages":["publicKey"]}));
    let host = host(
        &group.members[0],
        &[("imported", "Ed25519", &["publicKey"])],
    );
    let public = bytes(call(&host, 5, vec![s("imported"), Value::Null]).unwrap());
    // The same seed through raw NaCl yields the same public key.
    let pair = bytes(call(&host, 7, vec![Value::Number(12.0), s(&hex_encode(&seed))]).unwrap());
    assert_eq!(public, pair[..32]);

    // Envelopes sealed under an unmounted key, and short HS256
    // secrets, never import.
    let foreign = seal_import(&wrapping(9), &seed, "raw").unwrap();
    let error = group
        .run(0, json!({"operation":"import","requestId":"f","name":"foreign","algorithm":"Ed25519","sealed":foreign}))
        .unwrap_err();
    assert!(error.starts_with("KEYS_LOCKED"), "{error}");
    let short = seal_import(&wrapping(1), &[1u8; 16], "raw").unwrap();
    assert!(group
        .run(0, json!({"operation":"import","requestId":"s","name":"short","algorithm":"HS256","sealed":short}))
        .is_err());
    // Generation needs the wrapping key; binding does not.
    let mut locked = Group::new(vec![member(None, &[])]);
    let error = locked
        .run(
            0,
            json!({"operation":"generate","requestId":"g","name":"k","algorithm":"Ed25519"}),
        )
        .unwrap_err();
    assert!(error.starts_with("KEYS_LOCKED"), "{error}");
}

#[test]
fn raw_nacl_jose_and_webauthn_dispatch_through_the_host() {
    let host = CryptoHost::new();
    // TweetNaCl's secretbox.random.js vector 1.
    let key = "847310e59f2328dcdd00332dfa3c61605e78dfb3a7dad65ca2474d5f88799f12";
    let nonce = "a9e9b4b8374bd894f639b323a27c34514a8e7d64cc83b95e";
    let sealed = call(
        &host,
        7,
        vec![Value::Number(1.0), s("d5"), s(nonce), s(key)],
    )
    .unwrap();
    assert_eq!(sealed, s("54956256e80184a79a3af7d2947913cd44"));
    let opened = call(&host, 7, vec![Value::Number(2.0), sealed, s(nonce), s(key)]).unwrap();
    assert_eq!(opened, s("d5"));
    let forged = call(
        &host,
        7,
        vec![
            Value::Number(2.0),
            s("55956256e80184a79a3af7d2947913cd44"),
            s(nonce),
            s(key),
        ],
    )
    .unwrap();
    assert_eq!(forged, Value::Null);

    // Raw HS256 and A256GCM, validated at the passed clock.
    let secret = hex_encode(&[7u8; 32]);
    let sign_options = r#"{"algorithm":"HS256","keyFormat":"raw"}"#;
    let token = text(
        call(
            &host,
            8,
            vec![
                s("sign"),
                s(r#"{"sub":"ada","exp":1700000900}"#),
                s(&secret),
                s(sign_options),
            ],
        )
        .unwrap(),
    );
    let verify_options = r#"{"algorithms":["HS256"],"keyFormat":"raw","subject":"ada"}"#;
    let verified = call(
        &host,
        8,
        vec![
            s("verify"),
            s(&token),
            s(&secret),
            s(verify_options),
            Value::Number(NOW),
        ],
    )
    .unwrap();
    assert_eq!(header(&verified, "alg").as_deref(), Some("HS256"));
    let late = call(
        &host,
        8,
        vec![
            s("verify"),
            s(&token),
            s(&secret),
            s(verify_options),
            Value::Number(NOW + 1e6),
        ],
    )
    .unwrap_err();
    assert_eq!(late.code, "CRYPTO_ERROR");
    let not_allowed = r#"{"algorithms":["RS256"],"keyFormat":"der"}"#;
    assert!(
        call(
            &host,
            8,
            vec![
                s("verify"),
                s(&token),
                s(&secret),
                s(not_allowed),
                Value::Number(NOW)
            ]
        )
        .is_err()
    );
    let jwe = text(
        call(
            &host,
            8,
            vec![
                s("encrypt"),
                s(r#"{"exp":1700000900}"#),
                s(&secret),
                s(&hex_encode(&[1u8; 12])),
                s("{}"),
            ],
        )
        .unwrap(),
    );
    let decrypted = call(
        &host,
        8,
        vec![
            s("decrypt"),
            s(&jwe),
            s(&secret),
            s("{}"),
            Value::Number(NOW),
        ],
    )
    .unwrap();
    assert_eq!(header(&decrypted, "alg").as_deref(), Some("dir"));

    // WebAuthn verification failures are catchable crypto failures.
    let options = r#"{"challenge":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8","origins":["https://example.com"],"rpId":"example.com","userVerification":"preferred","algorithms":[-8,-7]}"#;
    let refused = call(&host, 10, vec![s("registration"), s("{}"), s(options)]).unwrap_err();
    assert_eq!(refused.code, "CRYPTO_ERROR");
    let bad_options = call(
        &host,
        10,
        vec![s("registration"), s("{}"), s(r#"{"challenge":1}"#)],
    )
    .unwrap_err();
    assert_eq!(bad_options.code, "INVALID_VALUE");

    // Malformed calls fail as envelopes, never panics.
    for (op, args) in [
        (7, vec![]),
        (7, vec![Value::Number(1.0)]),
        (7, vec![Value::Number(99.0)]),
        (7, vec![Value::Number(1.5), s("00")]),
        (8, vec![s("sign")]),
        (8, vec![s("nope"), s("{}"), s("00"), s("{}")]),
        (
            8,
            vec![s("verify"), s("t"), s("00"), s("{}"), Value::Number(-1.0)],
        ),
        (9, vec![s("sign")]),
        (
            9,
            vec![
                s("sign"),
                s("a"),
                Value::Null,
                Value::Array(vec![Value::Bool(true)]),
                s("{}"),
                Value::Number(0.0),
            ],
        ),
        (10, vec![s("nope"), s("{}"), s("{}")]),
        (11, vec![]),
    ] {
        let rendered = format!("op {op} {args:?}");
        let failure = call(&host, op, args).expect_err(&rendered);
        assert!(!failure.code.is_empty(), "{rendered}");
    }
}
