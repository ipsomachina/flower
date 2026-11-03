//! SHA-256 hashing and deployment content identities.

use sha2::{Digest, Sha256};

/// SHA-256 of bytes.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// SHA-256 over bytes that arrive in pieces.
#[derive(Clone, Default)]
pub struct Hasher(Sha256);

impl Hasher {
    /// Hash the next piece.
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// The digest of every piece so far.
    pub fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

/// A deployment's content identity: domain-separated SHA-256 over
/// the Wasm bundle plus its canonical manifest. Length-prefixing
/// keeps concatenations unambiguous; the domain tag keeps deployment
/// identities distinct from plain content hashes.
pub fn content_identity(wasm: &[u8], manifest: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"flower-deploy-v1");
    hasher.update((wasm.len() as u64).to_be_bytes());
    hasher.update(wasm);
    hasher.update(manifest);
    hasher.finalize().into()
}
