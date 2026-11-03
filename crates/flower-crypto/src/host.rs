//! The host side of the guest crypto ABI: dispatch [`CryptoOp`]
//! calls against the managed keys, NaCl, JOSE, WebAuthn, host hashing
//! and host entropy.
//!
//! Arguments arrive as binary [`Value`] arrays with binary payloads
//! as hex strings and JSON payloads (claims, options, WebAuthn
//! responses) as JSON text; results leave as binary envelopes, so
//! every crypto failure is a catchable business failure, never a
//! trap. Every argument is guest-controlled: dispatch never indexes,
//! slices or unwraps it, so malformed input fails as an envelope.
//!
//! Managed keys resolve through the replicated catalogs alone
//! ([`ManagedKeys`], attached with [`CryptoHost::with_catalog`]), so
//! every member signs and verifies alike. The runtime factory scopes
//! the host to each module's database and manifest with
//! [`CryptoBackend::for_module`]: an alias resolves in that database's
//! catalog only, for a module that declared it, and a use must fall
//! inside both the operator binding and the declaration. An alias the
//! database's catalog does not bind is `UNKNOWN_KEY`.
//!
//! Entropy is mutation-only: derived values and query caches must
//! be pure functions of state, so [`CryptoHost::crypto_call_for`]
//! refuses `random_bytes` to every other invocation kind. Every other
//! operation is deterministic in its inputs, the applied catalog and
//! the invocation clock the guest passes for JWT validation.
//!
//! [`CryptoOp`]: flower_sdk::crypto::CryptoOp

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use flower_sdk::abi::InvocationKind;
use flower_sdk::crypto::{CRYPTO_ERROR, CryptoBackend, CryptoOp, hex_decode, hex_encode};
use flower_sdk::error::{Result, codes, fail};
use flower_sdk::value::Value;

use crate::hash::sha256;
use crate::replicated::{Declaration, ManagedKeys, ManagedOutput, SharedKey, declarations};
use crate::{jwt, nacl, webauthn};

/// Random-bytes cap per call.
const MAX_RANDOM: u32 = 1024;

/// Native shared handles are bounded per invocation and wiped on drop.
const MAX_SHARED_BYTES: usize = 1024 * 1024;

#[derive(Default)]
struct SharedKeys {
    keys: Vec<(String, SharedKey)>,
    bytes: usize,
}

/// One module's database and key declarations.
struct Scope {
    db: Vec<u8>,
    module: String,
    declarations: BTreeMap<String, Declaration>,
}

/// The crypto host behind `crypto_call`. Clones share the catalogs
/// and the cache.
#[derive(Clone, Default)]
pub struct CryptoHost {
    catalog: Option<Arc<ManagedKeys>>,
    scope: Option<Arc<Scope>>,
    shared: Arc<Mutex<SharedKeys>>,
}

impl CryptoHost {
    /// Dispatch without managed keys: every keyed operation is
    /// `UNKNOWN_KEY` until [`CryptoHost::with_catalog`].
    pub fn new() -> Self {
        CryptoHost::default()
    }

    /// Resolve aliases through the replicated catalogs.
    pub fn with_catalog(mut self, catalog: Arc<ManagedKeys>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// The replicated catalogs' runtime, when attached.
    pub fn catalog(&self) -> Option<&Arc<ManagedKeys>> {
        self.catalog.as_ref()
    }

    /// This host scoped to one module of database `db`: keys resolve in
    /// that database's catalog only, and only for the aliases the
    /// module's manifest `keys` member declares. Malformed or
    /// conflicting declarations fail.
    pub fn for_module(&self, db: &[u8], module: &str, manifest: &Value) -> Result<CryptoHost> {
        let declared = match manifest {
            Value::Object(map) => match map.get("keys") {
                None => BTreeMap::new(),
                Some(keys) => declarations(keys).map_err(|error| {
                    fail(
                        codes::INVALID_VALUE,
                        format!("module {module:?} declares invalid keys: {error:#}"),
                    )
                })?,
            },
            _ => BTreeMap::new(),
        };
        Ok(CryptoHost {
            catalog: self.catalog.clone(),
            shared: Arc::default(),
            scope: Some(Arc::new(Scope {
                db: db.to_vec(),
                module: module.to_string(),
                declarations: declared,
            })),
        })
    }

    /// Run one crypto op on behalf of an invocation of `kind`,
    /// returning binary envelope bytes. `random_bytes` fails with
    /// `INVALID_INVOCATION` outside mutations: queries, derived values
    /// and planners stay deterministic. Nested calls pass their own
    /// kind — a query called from a mutation is a query.
    pub fn crypto_call_for(&self, kind: InvocationKind, op: u32, input: &[u8]) -> Result<Vec<u8>> {
        if op == CryptoOp::RandomBytes as u32 && !entropy_allowed(kind) {
            return err(
                codes::INVALID_INVOCATION,
                "random_bytes is only available to mutations: queries, derived values and planners must stay deterministic",
            );
        }
        let args = match Value::decode_binary(input) {
            Ok(Value::Array(args)) => args,
            Ok(_) => return err(codes::INVALID_VALUE, "crypto args take an array"),
            Err(failure) => return err(&failure.code, &failure.message),
        };
        let value = match op {
            1 => random_bytes(&args),
            2 => sha256_hex(&args),
            3 => self.sign(&args),
            4 => self.verify(&args),
            5 => self.public_key(&args),
            6 => self.key_version(&args),
            7 => nacl_call(&args),
            8 => jwt_call(&args),
            9 => self.managed_call(&args),
            10 => webauthn_call(&args),
            _ => return err(codes::UNKNOWN_METHOD, "unknown crypto op"),
        };
        match value {
            Ok(value) => ok(value),
            Err(failure) => flower_sdk::abi::encode_err(&failure).encode_binary(),
        }
    }
}

/// Whether an invocation kind may draw host entropy: mutations only.
pub fn entropy_allowed(kind: InvocationKind) -> bool {
    kind == InvocationKind::Mutation
}

impl CryptoBackend for CryptoHost {
    fn for_invocation(&self) -> Option<Box<dyn CryptoBackend>> {
        Some(Box::new(Self {
            catalog: self.catalog.clone(),
            scope: self.scope.clone(),
            shared: Arc::default(),
        }))
    }

    /// The trait carries no invocation kind, so this entry point
    /// dispatches with mutation authority: the Wasm runtime refuses
    /// `random_bytes` outside mutations before it reaches the lane.
    /// Callers that know the kind use [`CryptoHost::crypto_call_for`].
    fn crypto_call(&self, op: u32, input: &[u8]) -> Result<Vec<u8>> {
        self.crypto_call_for(InvocationKind::Mutation, op, input)
    }

    fn for_module(
        &self,
        db: &[u8],
        module: &str,
        manifest: &Value,
    ) -> Result<Option<Box<dyn CryptoBackend>>> {
        Ok(Some(Box::new(CryptoHost::for_module(
            self, db, module, manifest,
        )?)))
    }
}

/// Encode a success envelope.
fn ok(value: Value) -> Result<Vec<u8>> {
    flower_sdk::abi::encode_ok(value).encode_binary()
}

/// Encode a failure envelope.
fn err(code: &str, message: &str) -> Result<Vec<u8>> {
    flower_sdk::abi::encode_err(&fail(code, message)).encode_binary()
}

/// Map a crypto failure: a leading `CODE: ` names its code, anything
/// else is a `CRYPTO_ERROR`.
fn crypto(error: anyhow::Error) -> flower_sdk::Failure {
    let message = format!("{error:#}");
    if let Some((code, rest)) = message.split_once(": ")
        && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return fail(code, rest);
    }
    fail(CRYPTO_ERROR, message)
}

/// Take one hex argument.
fn one(args: &[Value]) -> Result<&str> {
    let [Value::String(hex)] = args else {
        return Err(fail(codes::INVALID_VALUE, "crypto args mismatch"));
    };
    Ok(hex)
}

/// A guest number that must be a whole `u32`: fractions, negatives,
/// NaN and out-of-range values fail instead of saturating.
fn whole_u32(number: f64, what: &str) -> Result<u32> {
    if number.fract() != 0.0 || !(0.0..=f64::from(u32::MAX)).contains(&number) {
        return Err(fail(
            codes::INVALID_VALUE,
            format!("{what} must be a whole number in u32 range"),
        ));
    }
    Ok(number as u32)
}

/// The invocation clock a guest passes: whole, non-negative
/// milliseconds within the safe-integer range.
fn clock(value: &Value) -> Result<u64> {
    match value {
        Value::Number(now)
            if now.fract() == 0.0 && (0.0..=9_007_199_254_740_991.0).contains(now) =>
        {
            Ok(*now as u64)
        }
        _ => Err(fail(
            codes::INVALID_VALUE,
            "the clock is whole non-negative milliseconds",
        )),
    }
}

/// Parse a version pin: null (current) or a whole version number.
fn version_ref(version: &Value) -> Result<Option<u32>> {
    match version {
        Value::Null => Ok(None),
        Value::Number(version) => whole_u32(*version, "version").map(Some),
        _ => Err(fail(codes::INVALID_VALUE, "version is null or a number")),
    }
}

/// Fresh host entropy, hex-encoded.
fn random_bytes(args: &[Value]) -> Result<Value> {
    let [Value::Number(len)] = args else {
        return Err(fail(codes::INVALID_VALUE, "random takes a length"));
    };
    let len = whole_u32(*len, "random length")?;
    if len > MAX_RANDOM {
        return Err(fail(codes::INVALID_VALUE, "random exceeds 1024 bytes"));
    }
    let mut bytes = vec![0u8; len as usize];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut bytes);
    Ok(Value::String(hex_encode(&bytes)))
}

/// SHA-256 over hex bytes.
fn sha256_hex(args: &[Value]) -> Result<Value> {
    let bytes = hex_decode(one(args)?)?;
    Ok(Value::String(hex_encode(&sha256(&bytes))))
}

/// Hex arguments as owned bytes.
fn hex_args(args: &[Value]) -> Result<Vec<Vec<u8>>> {
    args.iter()
        .map(|arg| match arg {
            Value::String(hex) => hex_decode(hex),
            _ => Err(fail(codes::INVALID_VALUE, "crypto byte arguments are hex")),
        })
        .collect()
}

/// A JSON-text argument.
fn json_text(value: &Value, what: &str) -> Result<serde_json::Value> {
    let Value::String(text) = value else {
        return Err(fail(codes::INVALID_VALUE, format!("{what} is JSON text")));
    };
    jwt::parse_json(text.as_bytes()).map_err(crypto)
}

/// JSON results as guest values: numbers become doubles, as in JS.
fn from_json(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(number) => Value::Number(number.as_f64().unwrap_or(f64::NAN)),
        serde_json::Value::String(text) => Value::String(text),
        serde_json::Value::Array(items) => Value::Array(items.into_iter().map(from_json).collect()),
        serde_json::Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, from_json(value)))
                .collect(),
        ),
    }
}

/// NaCl over raw keys: `[op, hex...]` with the operation numbers (see
/// [`nacl::execute`]).
fn nacl_call(args: &[Value]) -> Result<Value> {
    let [Value::Number(op), rest @ ..] = args else {
        return Err(fail(codes::INVALID_VALUE, "nacl takes an op and hex args"));
    };
    let op = whole_u32(*op, "nacl op")?;
    let bytes = hex_args(rest)?;
    let borrowed: Vec<&[u8]> = bytes.iter().map(Vec::as_slice).collect();
    Ok(match nacl::execute(op, &borrowed).map_err(crypto)? {
        nacl::Output::Bytes(bytes) => Value::String(hex_encode(&bytes)),
        nacl::Output::Bool(value) => Value::Bool(value),
        nacl::Output::Null => Value::Null,
    })
}

/// JOSE over raw keys:
/// `["sign", claims, key, options]`, `["verify", token, key, options, now]`,
/// `["encrypt", claims, key, nonce, options]`, `["decrypt", token, key, options, now]`.
/// Claims and options are JSON text; keys and nonces hex.
fn jwt_call(args: &[Value]) -> Result<Value> {
    let invalid = || fail(codes::INVALID_VALUE, "jwt args mismatch");
    match args {
        [
            Value::String(op),
            Value::String(claims),
            Value::String(key),
            options,
        ] if op == "sign" => {
            let claims = json_text(&Value::String(claims.clone()), "claims")?;
            let options: jwt::SignOptions = options_of(options)?;
            let token = jwt::sign(&claims, &hex_decode(key)?, &options).map_err(crypto)?;
            Ok(Value::String(token))
        }
        [
            Value::String(op),
            Value::String(token),
            Value::String(key),
            options,
            now,
        ] if op == "verify" => {
            let options: jwt::VerifyOptions = options_of(options)?;
            let now = clock(now)?;
            let change = jwt::changes_at(token, options.clock_tolerance_seconds, now);
            let verified = hex_decode(key).and_then(|key| {
                let verified = jwt::verify(token, &key, &options, now).map_err(crypto)?;
                Ok(from_json(
                    serde_json::to_value(verified)
                        .map_err(|error| fail(CRYPTO_ERROR, error.to_string()))?,
                ))
            });
            changing(verified, change)
        }
        [
            Value::String(op),
            Value::String(claims),
            Value::String(key),
            Value::String(nonce),
            options,
        ] if op == "encrypt" => {
            let claims = json_text(&Value::String(claims.clone()), "claims")?;
            let options: jwt::EncryptOptions = options_of(options)?;
            let token = jwt::encrypt(&claims, &hex_decode(key)?, &hex_decode(nonce)?, &options)
                .map_err(crypto)?;
            Ok(Value::String(token))
        }
        [
            Value::String(op),
            Value::String(token),
            Value::String(key),
            options,
            now,
        ] if op == "decrypt" => {
            let options: jwt::DecryptOptions = options_of(options)?;
            let now = clock(now)?;
            let verified = jwt::decrypt(token, &hex_decode(key)?, &options, now).map_err(crypto)?;
            let change = jwt::claims_change(&verified.claims, options.clock_tolerance_seconds, now);
            let verified = from_json(
                serde_json::to_value(verified)
                    .map_err(|error| fail(CRYPTO_ERROR, error.to_string()))?,
            );
            changing(Ok(verified), change)
        }
        _ => Err(invalid()),
    }
}

/// A token check's outcome with when time alone next changes it (see
/// [`jwt::changes_at`]): beside a success's claims, in a failure's details.
/// The SDK declares it instead of polling the clock. A failed decryption has
/// none: only the key opens its claims.
fn changing(outcome: Result<Value>, change: Option<u64>) -> Result<Value> {
    let Some(at) = change else {
        return outcome;
    };
    let at = (String::from("changesAt"), Value::Number(at as f64));
    match outcome {
        Ok(Value::Object(mut map)) => {
            map.insert(at.0, at.1);
            Ok(Value::Object(map))
        }
        Ok(other) => Ok(other),
        Err(mut failure) => {
            failure.details = Some(Value::Object(BTreeMap::from([at])));
            Err(failure)
        }
    }
}

/// Deserialize JSON-text options.
fn options_of<T: serde::de::DeserializeOwned>(options: &Value) -> Result<T> {
    serde_json::from_value(json_text(options, "options")?).map_err(|error| {
        fail(
            codes::INVALID_VALUE,
            format!("invalid crypto options: {error}"),
        )
    })
}

/// WebAuthn relying-party verification:
/// `["registration" | "authentication", response, options]`, both JSON text.
fn webauthn_call(args: &[Value]) -> Result<Value> {
    let [Value::String(op), Value::String(response), options] = args else {
        return Err(fail(codes::INVALID_VALUE, "webauthn args mismatch"));
    };
    let value = match op.as_str() {
        "registration" => {
            let options: webauthn::RegistrationOptions = options_of(options)?;
            serde_json::to_value(
                webauthn::verify_registration(response.as_bytes(), &options).map_err(crypto)?,
            )
        }
        "authentication" => {
            let options: webauthn::AuthenticationOptions = options_of(options)?;
            serde_json::to_value(
                webauthn::verify_authentication(response.as_bytes(), &options).map_err(crypto)?,
            )
        }
        _ => return Err(fail(codes::INVALID_VALUE, "unknown webauthn operation")),
    };
    Ok(from_json(
        value.map_err(|error| fail(CRYPTO_ERROR, error.to_string()))?,
    ))
}

impl CryptoHost {
    /// Run a catalog operation for this host's module, in its
    /// database's catalog.
    fn catalog_execute(
        &self,
        alias: &str,
        operation: &str,
        version: Option<u64>,
        args: &[&[u8]],
        options: &serde_json::Value,
        now_ms: u64,
    ) -> Result<ManagedOutput> {
        let keys = self.catalog.as_ref().ok_or_else(|| {
            fail(
                codes::UNKNOWN_KEY,
                "no replicated key catalog is configured",
            )
        })?;
        let Some(scope) = &self.scope else {
            return Err(fail(
                codes::FORBIDDEN_USAGE,
                "catalog keys resolve only for a module that declares them",
            ));
        };
        let declaration = scope.declarations.get(alias).ok_or_else(|| {
            fail(
                codes::FORBIDDEN_USAGE,
                format!("module {:?} does not declare key {alias:?}", scope.module),
            )
        })?;
        if matches!(operation, "nacl.box.after" | "nacl.box.open.after") {
            let slot = options
                .get("shared")
                .and_then(serde_json::Value::as_u64)
                .and_then(|slot| usize::try_from(slot).ok())
                .ok_or_else(|| fail(codes::INVALID_VALUE, "shared key handle is missing"))?;
            let shared = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
            let (_, key) = shared
                .keys
                .get(slot)
                .filter(|(origin, _)| origin == alias)
                .ok_or_else(|| {
                    fail(
                        codes::FORBIDDEN_USAGE,
                        "shared key is outside this invocation or alias",
                    )
                })?;
            let [message, nonce] = args else {
                return Err(fail(
                    codes::INVALID_VALUE,
                    "shared key needs message and nonce",
                ));
            };
            return keys
                .execute_shared(&scope.db, declaration, key, operation, message, nonce)
                .map_err(crypto);
        }
        keys.execute(
            &scope.db,
            declaration,
            operation,
            version,
            args,
            options,
            now_ms,
        )
        .map_err(crypto)
    }

    /// A managed operation against the catalog:
    /// `[operation, alias, version, args, options, now]` with hex
    /// arguments, JSON-text options and the invocation clock.
    fn managed_call(&self, args: &[Value]) -> Result<Value> {
        let [
            Value::String(operation),
            Value::String(alias),
            version,
            Value::Array(bytes),
            options,
            now,
        ] = args
        else {
            return Err(fail(codes::INVALID_VALUE, "managed crypto args mismatch"));
        };
        let version = version_ref(version)?.map(u64::from);
        let bytes = hex_args(bytes)?;
        let borrowed: Vec<&[u8]> = bytes.iter().map(Vec::as_slice).collect();
        let options = json_text(options, "options")?;
        let now = clock(now)?;
        let output = self.catalog_execute(alias, operation, version, &borrowed, &options, now);
        let tolerance = options
            .get("clockToleranceSeconds")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        let change = match (operation.as_str(), &output) {
            ("jwt.verify", _) => borrowed
                .first()
                .and_then(|token| std::str::from_utf8(token).ok())
                .and_then(|token| jwt::changes_at(token, tolerance, now)),
            ("jwt.decrypt", Ok(ManagedOutput::Json(verified))) => {
                jwt::claims_change(&verified["claims"], tolerance, now)
            }
            _ => None,
        };
        let value = output.and_then(|output| {
            Ok(match output {
                ManagedOutput::Shared(key) => {
                    let mut shared = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
                    let weight = key.retained_bytes().saturating_add(alias.len());
                    let bytes = shared.bytes.saturating_add(weight);
                    if bytes > MAX_SHARED_BYTES {
                        return Err(fail(
                            codes::INVALID_VALUE,
                            "shared-key memory budget exceeded",
                        ));
                    }
                    let slot = shared.keys.len();
                    shared.keys.push((alias.clone(), key));
                    shared.bytes = bytes;
                    Value::Number(slot as f64)
                }
                ManagedOutput::Bytes(bytes) => Value::String(hex_encode(&bytes)),
                ManagedOutput::Text(text) => Value::String(text),
                ManagedOutput::Json(value) => from_json(value),
                ManagedOutput::Bool(value) => Value::Bool(value),
                ManagedOutput::Null => Value::Null,
            })
        });
        changing(value, change)
    }

    /// Sign hex bytes under a binding: `[name, version, message]`.
    fn sign(&self, args: &[Value]) -> Result<Value> {
        let [Value::String(name), version, Value::String(message)] = args else {
            return Err(fail(
                codes::INVALID_VALUE,
                "sign takes name, version, message",
            ));
        };
        let version = version_ref(version)?;
        let message = hex_decode(message)?;
        let ManagedOutput::Bytes(signature) = self.catalog_execute(
            name,
            "sign",
            version.map(u64::from),
            &[&message],
            &serde_json::Value::Null,
            0,
        )?
        else {
            return Err(fail(CRYPTO_ERROR, "signing returned no bytes"));
        };
        Ok(Value::String(hex_encode(&signature)))
    }

    /// Verify hex bytes against a binding:
    /// `[name, version, message, signature]`.
    fn verify(&self, args: &[Value]) -> Result<Value> {
        let [
            Value::String(name),
            version,
            Value::String(message),
            Value::String(signature),
        ] = args
        else {
            return Err(fail(
                codes::INVALID_VALUE,
                "verify takes name, version, message, signature",
            ));
        };
        let version = version_ref(version)?;
        let message = hex_decode(message)?;
        let signature = hex_decode(signature)?;
        let ManagedOutput::Bool(matched) = self.catalog_execute(
            name,
            "verify",
            version.map(u64::from),
            &[&message, &signature],
            &serde_json::Value::Null,
            0,
        )?
        else {
            return Err(fail(CRYPTO_ERROR, "verification returned no bit"));
        };
        Ok(Value::Bool(matched))
    }

    /// Read a binding's public key: `[name, version]`.
    fn public_key(&self, args: &[Value]) -> Result<Value> {
        let [Value::String(name), version] = args else {
            return Err(fail(
                codes::INVALID_VALUE,
                "public key takes name and version",
            ));
        };
        let version = version_ref(version)?;
        let ManagedOutput::Bytes(key) = self.catalog_execute(
            name,
            "publicKey",
            version.map(u64::from),
            &[],
            &serde_json::Value::Null,
            0,
        )?
        else {
            return Err(fail(CRYPTO_ERROR, "the public key returned no bytes"));
        };
        Ok(Value::String(hex_encode(&key)))
    }

    /// Report a binding's current version: `[name]`.
    fn key_version(&self, args: &[Value]) -> Result<Value> {
        let [Value::String(name)] = args else {
            return Err(fail(codes::INVALID_VALUE, "key version takes a name"));
        };
        let ManagedOutput::Json(value) =
            self.catalog_execute(name, "version", None, &[], &serde_json::Value::Null, 0)?
        else {
            return Err(fail(CRYPTO_ERROR, "the version returned no metadata"));
        };
        let version = value
            .get("version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| fail(CRYPTO_ERROR, "the version returned no number"))?;
        Ok(Value::Number(version as f64))
    }
}
