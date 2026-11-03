//! Watches over managed keys on a live node: a key-catalog change moves
//! no database, yet the watches whose results resolved a key refresh,
//! derived values computed from a key drop with the change, and a
//! subscriber whose access decision checked a revoked key loses its
//! stream.

extern crate alloc;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_crypto::host::CryptoHost;
use flower_crypto::replicated::{ManagedKeys, WrappingKey};
use flower_engine::engine::CallOptions;
use flower_engine::runtime::NativeRuntime;
use flower_sdk::Value;
use flower_server::watch::{Hub, HubMessage};
use tokio::sync::{mpsc, watch};

/// Signing and verifying under a declared catalog key, directly and
/// through derived values, behind a hook that accepts a signature of
/// the caller's name under version 1 as its credential.
mod keys_app {
    use flower_sdk::auth::{Access, AuthRequest, Authentication, Authorization, Principal};
    use flower_sdk::crypto::{
        Algorithm, Crypto, KeyDeclaration, ManagedKeyRef, Usage, hex_decode, hex_encode,
    };
    use flower_sdk::{App, DerivedCtx, QueryCtx, ReadCtx, Value, derived, fail, query};
    use std::sync::atomic::{AtomicBool, Ordering};

    static SESSION_KEYS: [KeyDeclaration; 1] = [KeyDeclaration::new(
        "sessions",
        Algorithm::Ed25519,
        &[Usage::Sign, Usage::Verify],
    )];

    struct Keys;

    impl flower_sdk::app::Component for Keys {
        fn keys(&self) -> &'static [KeyDeclaration] {
            &SESSION_KEYS
        }
    }

    static KEYS: Keys = Keys;

    fn sign_with(
        ctx: &mut flower_sdk::ctx::Ctx<'_, '_>,
        message: &str,
    ) -> flower_sdk::Result<String> {
        let key = SESSION_KEYS[0].key()?;
        let signature =
            Crypto::new(ctx).managed_sign(&key, ManagedKeyRef::Current, message.as_bytes())?;
        Ok(hex_encode(&signature))
    }

    /// `{name, signature}` credentials: `signature` signs `name` under
    /// version 1 of the bound key.
    fn authenticate(
        ctx: &mut QueryCtx<'_, '_>,
        credentials: &Value,
        _request: &AuthRequest,
    ) -> flower_sdk::Result<Option<Principal>> {
        let Value::Object(fields) = credentials else {
            return Ok(None);
        };
        let (Some(Value::String(name)), Some(Value::String(signature))) =
            (fields.get("name"), fields.get("signature"))
        else {
            return Err(fail(
                "UNAUTHENTICATED",
                "credentials need name and signature",
            ));
        };
        let key = SESSION_KEYS[0].key()?;
        let verified = Crypto::new(ctx.ctx()).managed_verify(
            &key,
            ManagedKeyRef::Version(1),
            name.as_bytes(),
            &hex_decode(signature)?,
        )?;
        if !verified {
            return Err(fail("UNAUTHENTICATED", "bad signature"));
        }
        Ok(Some(Principal::new(name.clone(), None, None)?))
    }

    static AUTHORIZATION: Authorization = Authorization {
        authenticate: Authentication::Callback(authenticate),
        default: Access::Public,
        sessions: Access::Public,
        methods: &[],
    };

    /// `message` signed under the binding's active version, in hex.
    #[query]
    pub fn sign(ctx: &mut QueryCtx<'_, '_>, message: String) -> flower_sdk::Result<String> {
        sign_with(ctx.ctx(), &message)
    }

    /// [`sign`], kept as a derived value.
    #[derived]
    pub fn signed(ctx: &mut DerivedCtx<'_, '_>, message: String) -> flower_sdk::Result<String> {
        sign_with(ctx.ctx(), &message)
    }

    /// Read [`signed`].
    #[query]
    pub fn read_signed(ctx: &mut QueryCtx<'_, '_>, message: String) -> flower_sdk::Result<Value> {
        let args = Value::Object([(String::from("message"), Value::String(message))].into());
        ctx.get_derived("app", "signed", &args)
    }

    /// A constant, whoever asks.
    #[query]
    pub fn hello(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<String> {
        let _ = ctx;
        Ok(String::from("hello"))
    }

    /// Whether [`slow_sign`] stalled yet.
    pub static STALLED: AtomicBool = AtomicBool::new(false);

    /// [`sign`], stalling once after it signed, before it returns.
    #[query]
    pub fn slow_sign(ctx: &mut QueryCtx<'_, '_>, message: String) -> flower_sdk::Result<String> {
        let signature = sign_with(ctx.ctx(), &message)?;
        if !STALLED.swap(true, Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(800));
        }
        Ok(signature)
    }

    static DEFS: [&dyn flower_sdk::Component; 5] =
        [&SIGN, &SIGNED, &READ_SIGNED, &HELLO, &SLOW_SIGN];

    pub static APP: App = App::new(&[&KEYS, &AUTHORIZATION, &DEFS]);
}

/// Runs the app with crypto over the member's catalog runtime.
struct Factory {
    keys: Arc<ManagedKeys>,
}

impl RuntimeFactory for Factory {
    fn build(
        &self,
        db: &[u8],
        module: &str,
        _wasm: &[u8],
        manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        let crypto = CryptoHost::new()
            .with_catalog(self.keys.clone())
            .for_module(db, module, manifest)
            .map_err(|failure| failure.message)?;
        Ok(Box::new(
            NativeRuntime::new(&keys_app::APP).with_crypto(Arc::new(crypto)),
        ))
    }
}

struct Served {
    node: Arc<Node>,
    hub: Arc<Hub>,
    _stop: watch::Sender<bool>,
}

/// An initialized single node with the app deployed, `sessions` bound to
/// a fresh key, and its watch hub pumping.
async fn serve_node() -> (tempfile::TempDir, Served) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let keys = Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[8; 32])),
        Vec::new(),
    ));
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(Factory { keys: keys.clone() }),
    )
    .await
    .unwrap()
    .with_key_catalog(keys);
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let node = Arc::new(node);
    for request in [
        serde_json::json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
        serde_json::json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign","verify"]}),
    ] {
        node.manage_keys(b"db", request).await.unwrap();
    }
    let manifest = NativeRuntime::new(&keys_app::APP).manifest().unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"", &manifest)
        .await
        .unwrap();
    let hub = Arc::new(Hub::new(node.clone()));
    let (stop, stopped) = watch::channel(false);
    tokio::spawn(hub.clone().run(stopped));
    (
        dir,
        Served {
            node,
            hub,
            _stop: stop,
        },
    )
}

fn message() -> Value {
    Value::Object([("message".to_string(), Value::String("m".to_string()))].into())
}

impl Served {
    async fn watch(
        &self,
        name: &str,
        args: Value,
        credentials: Option<Value>,
    ) -> (serde_json::Value, mpsc::Receiver<HubMessage>) {
        let options = CallOptions {
            credentials,
            ..CallOptions::default()
        };
        let (event, rx) = self
            .hub
            .subscribe_with(b"db", "app", name, &args, &options, false)
            .await
            .unwrap();
        (
            serde_json::from_slice::<serde_json::Value>(&event.value).unwrap(),
            rx,
        )
    }

    async fn keys(&self, request: serde_json::Value) {
        self.node.manage_keys(b"db", request).await.unwrap();
    }

    /// Sign `message` the way the hook's credentials expect.
    async fn sign(&self, message: &str) -> serde_json::Value {
        let args =
            Value::Object([("message".to_string(), Value::String(message.to_string()))].into());
        self.watch("sign", args, None).await.0
    }
}

async fn next(rx: &mut mpsc::Receiver<HubMessage>) -> Option<HubMessage> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .ok()
        .flatten()
}

fn rotate() -> serde_json::Value {
    serde_json::json!({"operation":"rotate","requestId":"r","name":"signing"})
}

#[tokio::test(flavor = "multi_thread")]
async fn key_changes_refresh_watches_that_resolved_a_key() {
    let (_dir, served) = serve_node().await;
    let (first, mut direct) = served.watch("sign", message(), None).await;
    let (derived, mut through) = served.watch("read_signed", message(), None).await;
    assert_eq!(derived, first);
    served.keys(rotate()).await;
    // No database moved, yet both refresh to the new version's signature:
    // the derived value's outcome dropped with the change.
    for rx in [&mut direct, &mut through] {
        match next(rx).await {
            Some(HubMessage::Snapshot(event)) => {
                assert_ne!(
                    serde_json::from_slice::<serde_json::Value>(&event.value).unwrap(),
                    first
                );
            }
            other => panic!("expected a refreshed signature, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn revoking_the_key_a_decision_checked_ends_the_stream() {
    let (_dir, served) = serve_node().await;
    let serde_json::Value::String(signature) = served.sign("ada").await else {
        panic!("a hex signature");
    };
    let credentials = Value::Object(
        [
            ("name".to_string(), Value::String("ada".to_string())),
            ("signature".to_string(), Value::String(signature)),
        ]
        .into(),
    );
    let (hello, mut rx) = served
        .watch(
            "hello",
            Value::Object(Default::default()),
            Some(credentials),
        )
        .await;
    assert_eq!(hello, serde_json::json!("hello"));
    served.keys(rotate()).await;
    served
        .keys(
            serde_json::json!({"operation":"revoke","requestId":"v","name":"signing","version":1}),
        )
        .await;
    match next(&mut rx).await {
        Some(HubMessage::Error { code, .. }) => assert_eq!(code, "unauthorized"),
        other => panic!("expected the stream to end, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_first_evaluation_straddling_a_key_change_catches_up() {
    // The subscription's first evaluation signs under the old version,
    // then stalls while the rotation applies and the hub handles its
    // notice before the entry exists. Installing it must catch up.
    let (_dir, served) = serve_node().await;
    let before = served.sign("m").await;
    let subscribing = {
        let hub = served.hub.clone();
        tokio::spawn(async move {
            hub.subscribe_with(
                b"db",
                "app",
                "slow_sign",
                &message(),
                &CallOptions::default(),
                false,
            )
            .await
            .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    served.keys(rotate()).await;
    let (event, mut rx) = subscribing.await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&event.value).unwrap(),
        before,
        "the stalled evaluation signed under the old version"
    );
    match next(&mut rx).await {
        Some(HubMessage::Snapshot(event)) => assert_ne!(
            serde_json::from_slice::<serde_json::Value>(&event.value).unwrap(),
            before
        ),
        other => panic!("expected a catch-up refresh, got {other:?}"),
    }
}
