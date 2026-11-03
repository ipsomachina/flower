//! Crypto lane tests: hash vectors, envelope authentication, the
//! keystore lifecycle and the guest crypto host's argument checks.

use flower_crypto::aead::{
    CryptoError, FRAME_OVERHEAD, MAX_FRAME_LEN, NONCE_LEN, STREAM_HEADER_LEN, StreamOpener,
    StreamSealer, open, open_stream, peek_stream_version, peek_version, random_nonce, seal,
    seal_stream,
};
use flower_crypto::hash::{content_identity, sha256};
use flower_crypto::keys::{Keystore, kek_from_slice};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn sha256_matches_nist_vector() {
    // NIST FIPS 180-4: SHA-256("abc").
    assert_eq!(
        hex(&sha256(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn content_identity_is_stable_and_sensitive() {
    let first = content_identity(b"wasm", b"manifest");
    assert_eq!(first, content_identity(b"wasm", b"manifest"));
    assert_ne!(first, content_identity(b"wasm!", b"manifest"));
    assert_ne!(first, content_identity(b"wasm", b"manifest!"));
    // Length-prefixing separates ("ab", "c") from ("a", "bc").
    assert_ne!(content_identity(b"ab", b"c"), content_identity(b"a", b"bc"));
}

#[test]
fn envelope_roundtrips_and_binds_header() {
    let key = [7u8; 32];
    let nonce = [9u8; 12];
    let sealed = seal(&key, 3, &nonce, b"secret payload").unwrap();
    assert_eq!(&sealed[..4], b"FLB1");
    assert_eq!(peek_version(&sealed).unwrap(), 3);
    let (version, plaintext) = open(&key, &sealed).unwrap();
    assert_eq!(version, 3);
    assert_eq!(plaintext, b"secret payload");

    // Wrong key fails authentication.
    assert_eq!(
        open(&[8u8; 32], &sealed).unwrap_err(),
        CryptoError::AuthFailed
    );

    // Flipping a ciphertext byte fails authentication.
    let mut tampered = sealed.clone();
    let last = tampered.len() - 1;
    tampered[last] ^= 0xff;
    assert_eq!(open(&key, &tampered).unwrap_err(), CryptoError::AuthFailed);

    // Flipping a version byte fails: the header is associated data.
    let mut rebased = sealed.clone();
    rebased[7] ^= 0x01;
    assert_eq!(open(&key, &rebased).unwrap_err(), CryptoError::AuthFailed);

    // Bad magic and truncation fail before authentication.
    let mut magic = sealed.clone();
    magic[0] = b'X';
    assert_eq!(open(&key, &magic).unwrap_err(), CryptoError::BadMagic);
    assert_eq!(
        open(&key, &sealed[..10]).unwrap_err(),
        CryptoError::Truncated
    );

    // Nonces come out random.
    assert_ne!(random_nonce(), random_nonce());
}

#[test]
fn streams_roundtrip_frame_by_frame() {
    let key = [7u8; 32];
    for len in [0usize, 1, 3, 4, 5, 8, 9, 31] {
        let plaintext: Vec<u8> = (0..len as u8).collect();
        let sealed = seal_stream(&key, 3, 4, &plaintext).unwrap();
        assert_eq!(&sealed[..4], b"FLS1");
        assert_eq!(peek_stream_version(&sealed).unwrap(), 3);
        // Every full frame, then a shorter last one (empty when the
        // plaintext fills its frames exactly).
        let frames = len / 4 + 1;
        assert_eq!(
            sealed.len(),
            STREAM_HEADER_LEN + frames * FRAME_OVERHEAD + len
        );
        assert_eq!(open_stream(&key, &sealed).unwrap(), (3, plaintext.clone()));

        // Fed a byte at a time, each frame opens once it is whole.
        let mut opener = StreamOpener::new(&key, &sealed[..STREAM_HEADER_LEN]).unwrap();
        let mut opened = Vec::new();
        for byte in &sealed[STREAM_HEADER_LEN..] {
            opener
                .feed(std::slice::from_ref(byte), &mut opened)
                .unwrap();
        }
        assert_eq!(opened.len(), len / 4 * 4);
        opener.finish(&mut opened).unwrap();
        assert_eq!(opened, plaintext);

        // Taken as it seals, the stream is the same bytes.
        let mut sealer = StreamSealer::new(&key, 3, 4).unwrap();
        let mut taken = Vec::new();
        for byte in &plaintext {
            sealer.write(std::slice::from_ref(byte)).unwrap();
            let ready = sealer.ready();
            taken.extend(sealer.take(ready));
        }
        taken.extend(sealer.finish().unwrap());
        assert_eq!(open_stream(&key, &taken).unwrap().1, plaintext);
    }
}

#[test]
fn streams_refuse_cut_reordered_spliced_and_altered_frames() {
    let key = [7u8; 32];
    let plaintext = b"0123456789ab".to_vec();
    let sealed = seal_stream(&key, 3, 4, &plaintext).unwrap();
    let frame = FRAME_OVERHEAD + 4;
    let at = |index: usize| STREAM_HEADER_LEN + index * frame;

    // Cut anywhere, at a frame boundary or inside a frame, it refuses.
    for cut in 0..sealed.len() {
        assert!(open_stream(&key, &sealed[..cut]).is_err(), "cut at {cut}");
    }
    // Trailing bytes after the last frame refuse too.
    let mut padded = sealed.clone();
    padded.push(0);
    assert!(open_stream(&key, &padded).is_err());

    // Swapping, dropping or repeating whole frames fails authentication.
    let mut swapped = sealed[..at(0)].to_vec();
    swapped.extend_from_slice(&sealed[at(1)..at(2)]);
    swapped.extend_from_slice(&sealed[at(0)..at(1)]);
    swapped.extend_from_slice(&sealed[at(2)..]);
    assert_eq!(
        open_stream(&key, &swapped).unwrap_err(),
        CryptoError::AuthFailed
    );
    let mut dropped = sealed[..at(1)].to_vec();
    dropped.extend_from_slice(&sealed[at(2)..]);
    assert_eq!(
        open_stream(&key, &dropped).unwrap_err(),
        CryptoError::AuthFailed
    );
    let mut repeated = sealed[..at(1)].to_vec();
    repeated.extend_from_slice(&sealed[at(0)..]);
    assert!(open_stream(&key, &repeated).is_err());

    // A frame from another stream under the same key, version and frame
    // length fails: the stream IDs differ.
    let other = seal_stream(&key, 3, 4, &plaintext).unwrap();
    let mut spliced = sealed[..at(1)].to_vec();
    spliced.extend_from_slice(&other[at(1)..at(2)]);
    spliced.extend_from_slice(&sealed[at(2)..]);
    assert_eq!(
        open_stream(&key, &spliced).unwrap_err(),
        CryptoError::AuthFailed
    );

    // The header is associated data of every frame.
    for byte in 4..STREAM_HEADER_LEN - 4 {
        let mut altered = sealed.clone();
        altered[byte] ^= 0x01;
        assert_eq!(
            open_stream(&key, &altered).unwrap_err(),
            CryptoError::AuthFailed,
            "header byte {byte}"
        );
    }
    let mut ciphertext = sealed.clone();
    ciphertext[at(1) + NONCE_LEN] ^= 0x01;
    assert_eq!(
        open_stream(&key, &ciphertext).unwrap_err(),
        CryptoError::AuthFailed
    );
    assert_eq!(
        open_stream(&[8u8; 32], &sealed).unwrap_err(),
        CryptoError::AuthFailed
    );

    // Malformed headers refuse before any frame.
    let mut magic = sealed.clone();
    magic[0] = b'X';
    assert_eq!(
        open_stream(&key, &magic).unwrap_err(),
        CryptoError::BadMagic
    );
    let mut empty_frames = sealed.clone();
    empty_frames[24..28].copy_from_slice(&0u32.to_be_bytes());
    assert_eq!(
        open_stream(&key, &empty_frames).unwrap_err(),
        CryptoError::BadHeader
    );
    let mut huge_frames = sealed.clone();
    huge_frames[24..28].copy_from_slice(&(MAX_FRAME_LEN as u32 + 1).to_be_bytes());
    assert_eq!(
        open_stream(&key, &huge_frames).unwrap_err(),
        CryptoError::BadHeader
    );
    assert_eq!(
        StreamSealer::new(&key, 3, 0).err(),
        Some(CryptoError::BadHeader)
    );
    // No plaintext survives sealing.
    assert!(!sealed.windows(4).any(|window| window == b"0123"));
}

#[test]
fn keystore_lifecycle() {
    let dir = std::env::temp_dir().join(format!("flower-ks-{}", std::process::id()));
    let _ = std::fs::remove_file(&dir);
    let kek = [1u8; 32];

    let store = Keystore::open(&dir, kek).unwrap();
    assert_eq!(store.current().unwrap(), 0);
    assert_eq!(store.ensure_current().unwrap(), 1);
    let first = store.get(1).unwrap();
    assert_eq!(store.get(1).unwrap(), first);

    // Rotation advances current without touching old material.
    assert_eq!(store.rotate().unwrap(), 2);
    assert_eq!(store.current().unwrap(), 2);
    assert_eq!(store.versions().unwrap(), vec![1, 2]);
    assert_eq!(store.get(1).unwrap(), first);
    assert_ne!(store.get(2).unwrap(), first);

    // Unknown versions fail; current never retires.
    assert_eq!(store.get(9).unwrap_err(), CryptoError::UnknownVersion(9));
    assert!(!store.retire(2).unwrap());
    assert!(store.retire(1).unwrap());
    assert_eq!(store.get(1).unwrap_err(), CryptoError::UnknownVersion(1));
    assert!(!store.retire(1).unwrap());

    // State persists across reopening.
    drop(store);
    let store = Keystore::open(&dir, kek).unwrap();
    assert_eq!(store.current().unwrap(), 2);
    assert_eq!(store.versions().unwrap(), vec![2]);

    // Rewrapping keeps material readable under the new KEK only.
    let mut store = store;
    let before = std::fs::read(&dir).unwrap();
    store.rewrap([2u8; 32]).unwrap();
    let after = std::fs::read(&dir).unwrap();
    assert_ne!(before, after);
    let material = store.get(2).unwrap();
    drop(store);
    assert!(Keystore::open(&dir, kek).unwrap().get(2).is_err());
    let store = Keystore::open(&dir, [2u8; 32]).unwrap();
    assert_eq!(store.get(2).unwrap(), material);

    // Bad KEK lengths refuse.
    assert_eq!(
        kek_from_slice(b"short").unwrap_err(),
        CryptoError::BadKeyLength(5)
    );
    let _ = std::fs::remove_file(&dir);
}

/// A unique scratch path per test and process.
fn scratch(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-crypto-{name}-{}", std::process::id()));
    path
}

/// A sibling of a store path: `<file name>.<suffix>`.
fn sibling(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut sibling = path.as_os_str().to_owned();
    sibling.push(".");
    sibling.push(suffix);
    sibling.into()
}

/// Remove a store plus its lock and temp siblings.
fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(sibling(path, "lock"));
    let _ = std::fs::remove_file(sibling(path, "tmp"));
    let _ = std::fs::remove_dir_all(sibling(path, "tmp"));
}

/// Call the crypto host as `kind` and decode its envelope.
fn crypto(
    host: &flower_crypto::host::CryptoHost,
    kind: flower_sdk::abi::InvocationKind,
    op: u32,
    args: Vec<flower_sdk::Value>,
) -> Result<flower_sdk::Value, flower_sdk::Failure> {
    let input = flower_sdk::Value::Array(args).encode_binary().unwrap();
    let output = host.crypto_call_for(kind, op, &input).unwrap();
    flower_sdk::abi::decode_envelope(flower_sdk::Value::decode_binary(&output).unwrap())
}

#[test]
fn crypto_host_rejects_malformed_args_without_panicking() {
    use flower_crypto::replicated::{ManagedKeys, Prepared, WrappingKey};
    use flower_sdk::Value;
    use flower_sdk::abi::InvocationKind::Mutation;
    // An Ed25519 key bound to `deploy` in database `app`, declared by
    // its module.
    let keys = std::sync::Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[7; 32])),
        Vec::new(),
    ));
    let mut stored: Option<Vec<u8>> = None;
    for request in [
        serde_json::json!({"operation":"generate","requestId":"g","name":"k","algorithm":"Ed25519"}),
        serde_json::json!({"operation":"bind","requestId":"b","name":"deploy","key":"k","usages":["sign","verify","publicKey"]}),
    ] {
        let Prepared::Change(change) = keys.prepare(stored.as_deref(), &request).unwrap() else {
            panic!("changes propose");
        };
        stored = flower_crypto::catalog::apply_change(stored.as_deref(), &change)
            .unwrap()
            .catalog;
        keys.install(b"app", stored.as_deref()).unwrap();
    }
    let declaration = Value::Object(
        [
            ("name".to_string(), Value::String("deploy".to_string())),
            (
                "algorithm".to_string(),
                Value::String("Ed25519".to_string()),
            ),
            (
                "usages".to_string(),
                Value::Array(
                    ["sign", "verify", "publicKey"]
                        .map(|usage| Value::String(usage.to_string()))
                        .to_vec(),
                ),
            ),
        ]
        .into_iter()
        .collect(),
    );
    let manifest = Value::Object(
        [("keys".to_string(), Value::Array(vec![declaration]))]
            .into_iter()
            .collect(),
    );
    let host = flower_crypto::host::CryptoHost::new()
        .with_catalog(keys)
        .for_module(b"app", "app", &manifest)
        .unwrap();
    let name = || Value::String("deploy".to_string());
    let hex = || Value::String("00ff".to_string());
    let malformed: Vec<(u32, Vec<Value>)> = vec![
        // Sign with too few arguments used to index past the end.
        (3, vec![name(), Value::Null]),
        (3, vec![name()]),
        (3, vec![]),
        (3, vec![name(), Value::Null, hex(), hex()]),
        (3, vec![name(), Value::Number(-1.0), hex()]),
        (3, vec![name(), Value::Number(1.5), hex()]),
        (3, vec![name(), Value::Number(1e12), hex()]),
        (3, vec![name(), Value::Bool(true), hex()]),
        (
            3,
            vec![name(), Value::Null, Value::String("zz".to_string())],
        ),
        (4, vec![name(), Value::Null, hex()]),
        (4, vec![name(), Value::Number(0.5), hex(), hex()]),
        (5, vec![name()]),
        (5, vec![name(), Value::Null, hex()]),
        (5, vec![Value::Null, Value::Null]),
        (6, vec![]),
        (6, vec![Value::Number(1.0)]),
        (2, vec![]),
        (2, vec![Value::String("abc".to_string())]),
        (1, vec![]),
        (1, vec![Value::Number(-4.0)]),
        (1, vec![Value::Number(2.5)]),
        (1, vec![Value::Number(4096.0)]),
        (99, vec![]),
    ];
    for (op, args) in malformed {
        let rendered = format!("op {op} {args:?}");
        let failure = crypto(&host, Mutation, op, args).expect_err(&rendered);
        assert!(!failure.code.is_empty(), "{rendered}");
    }
    // Non-array and undecodable inputs fail as envelopes too.
    for input in [Value::Null.encode_binary().unwrap(), vec![0xff, 0x00, 0x13]] {
        let output = host.crypto_call_for(Mutation, 3, &input).unwrap();
        let decoded = Value::decode_binary(&output).unwrap();
        assert!(flower_sdk::abi::decode_envelope(decoded).is_err());
    }
    // A signature of the wrong length verifies nothing, without failing.
    assert_eq!(
        crypto(&host, Mutation, 4, vec![name(), Value::Null, hex(), hex()]).unwrap(),
        Value::Bool(false)
    );
    // Well-formed calls still work.
    let signature = crypto(&host, Mutation, 3, vec![name(), Value::Null, hex()]).unwrap();
    let verified = crypto(
        &host,
        Mutation,
        4,
        vec![name(), Value::Number(1.0), hex(), signature],
    );
    assert_eq!(verified.unwrap(), Value::Bool(true));
}

#[test]
fn random_bytes_is_mutation_only() {
    use flower_sdk::Value;
    use flower_sdk::abi::InvocationKind;
    let host = flower_crypto::host::CryptoHost::new();
    for kind in [
        InvocationKind::Query,
        InvocationKind::Derived,
        InvocationKind::Transaction,
    ] {
        let failure = crypto(&host, kind, 1, vec![Value::Number(8.0)]).unwrap_err();
        assert_eq!(failure.code, "INVALID_INVOCATION", "{kind:?}");
        // Deterministic operations stay available.
        crypto(&host, kind, 2, vec![Value::String("00".to_string())]).unwrap();
    }
    assert!(!flower_crypto::host::entropy_allowed(InvocationKind::Query));
    let random = crypto(&host, InvocationKind::Mutation, 1, vec![Value::Number(8.0)]).unwrap();
    match random {
        Value::String(hex) => assert_eq!(hex.len(), 16),
        other => panic!("random bytes are hex, got {other:?}"),
    }
}

#[test]
fn keystore_concurrent_handles_never_fork_versions() {
    const HANDLES: usize = 6;
    const ROTATIONS: usize = 8;
    let path = scratch("ks-concurrent");
    cleanup(&path);
    let kek = [6u8; 32];
    let barrier = std::sync::Barrier::new(HANDLES);

    // Every handle races ensure_current on an empty store: all must
    // agree on one version 1 with one key.
    let firsts: Vec<(u32, [u8; 32])> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..HANDLES)
            .map(|_| {
                scope.spawn(|| {
                    let store = Keystore::open(&path, kek).unwrap();
                    barrier.wait();
                    let version = store.ensure_current().unwrap();
                    (version, store.get(version).unwrap())
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect()
    });
    assert!(firsts.iter().all(|first| *first == firsts[0]));
    assert_eq!(firsts[0].0, 1);

    // Concurrent rotations through separate handles each mint a
    // distinct version, and every minted version survives.
    let minted: Vec<(u32, [u8; 32])> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..HANDLES)
            .map(|_| {
                scope.spawn(|| {
                    let store = Keystore::open(&path, kek).unwrap();
                    barrier.wait();
                    (0..ROTATIONS)
                        .map(|_| {
                            let version = store.rotate().unwrap();
                            (version, store.get(version).unwrap())
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect()
    });
    let mut versions: Vec<u32> = minted.iter().map(|(version, _)| *version).collect();
    versions.sort_unstable();
    let last = u32::try_from(1 + HANDLES * ROTATIONS).unwrap();
    let expected: Vec<u32> = (2..=last).collect();
    assert_eq!(versions, expected);
    let store = Keystore::open(&path, kek).unwrap();
    assert_eq!(store.versions().unwrap(), (1..=last).collect::<Vec<_>>());
    // Material read right after minting is what the store keeps.
    for (version, key) in minted {
        assert_eq!(store.get(version).unwrap(), key, "version {version}");
    }
    cleanup(&path);
}

#[test]
fn keystore_failed_save_keeps_live_file() {
    let path = scratch("ks-save-fail");
    cleanup(&path);
    let store = Keystore::open(&path, [8u8; 32]).unwrap();
    assert_eq!(store.ensure_current().unwrap(), 1);
    assert_eq!(store.rotate().unwrap(), 2);
    let before = std::fs::read(&path).unwrap();
    // A successful write leaves no temp file behind.
    assert!(!sibling(&path, "tmp").exists());

    std::fs::create_dir_all(sibling(&path, "tmp")).unwrap();
    assert!(store.rotate().is_err());
    assert!(store.retire(1).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(store.current().unwrap(), 2);
    assert_eq!(store.versions().unwrap(), vec![1, 2]);
    std::fs::remove_dir_all(sibling(&path, "tmp")).unwrap();

    assert_eq!(store.rotate().unwrap(), 3);
    assert!(!sibling(&path, "tmp").exists());
    cleanup(&path);
}
