//! TweetNaCl's high-level operations, run by the host.
//!
//! Wire layouts match NaCl: a secretbox is `tag || ciphertext`, a
//! signed message is `signature || message`, an Ed25519 secret key is
//! `seed || public key`. Ed25519 verification is strict: malleable
//! signatures and inconsistent secret keys fail. Nonces are explicit;
//! never reuse one under a key. Random key pairs draw host entropy, so
//! they work in mutations only, unless a guest-local source is set
//! ([`Crypto::set_prng`]).
//!
//! Managed keys work where NaCl needs no exported secret: secretbox
//! over XSalsa20Poly1305 keys, box over X25519 keys, signatures over
//! Ed25519 keys and the X25519 public key. NaCl carries no key
//! version: save [`Crypto::managed_version`] beside ciphertexts and
//! pin it with [`ManagedKeyRef::Version`] to open them after
//! rotation.

use alloc::vec::Vec;

use super::{Crypto, CryptoOp, Key, ManagedKeyRef, hex_encode};
use crate::error::{Result, codes, fail};
use crate::value::Value;

/// Secretbox key length.
pub const SECRETBOX_KEY_LENGTH: usize = 32;
/// Nonce length for secretbox and box.
pub const NONCE_LENGTH: usize = 24;
/// Authenticator overhead of secretbox and box.
pub const OVERHEAD_LENGTH: usize = 16;
/// X25519 and Ed25519 public key length.
pub const PUBLIC_KEY_LENGTH: usize = 32;
/// Ed25519 secret key length: seed plus public key.
pub const SIGN_SECRET_KEY_LENGTH: usize = 64;
/// Ed25519 signature length.
pub const SIGNATURE_LENGTH: usize = 64;

/// A managed shared key. Only the host holds secret bytes. This handle
/// is usable in the invocation that derived it and cannot be serialized.
pub struct SharedKey {
    key: super::ManagedKey,
    slot: u32,
}

/// A raw shared key or a managed shared-key handle.
pub enum BoxKey<'k> {
    /// Caller-owned raw material.
    Raw(&'k [u8; 32]),
    /// Non-exportable invocation-local material.
    Managed(&'k SharedKey),
}

impl<'k> From<&'k [u8; 32]> for BoxKey<'k> {
    fn from(key: &'k [u8; 32]) -> Self {
        Self::Raw(key)
    }
}

impl<'k> From<&'k SharedKey> for BoxKey<'k> {
    fn from(key: &'k SharedKey) -> Self {
        Self::Managed(key)
    }
}

/// A raw NaCl key pair. The secret half is wiped on drop; managed
/// keys never produce one.
pub struct KeyPair {
    /// The public key.
    pub public_key: [u8; 32],
    secret_key: Vec<u8>,
}

impl KeyPair {
    /// The secret key: 32 bytes for box, 64 for signing.
    pub fn secret_key(&self) -> &[u8] {
        &self.secret_key
    }
}

impl Drop for KeyPair {
    fn drop(&mut self) {
        wipe(&mut self.secret_key);
    }
}

/// Volatile stores keep the compiler from eliding the wipe.
pub(crate) fn wipe(bytes: &mut [u8]) {
    for byte in bytes {
        // SAFETY: every byte is a live, uniquely borrowed element.
        unsafe { core::ptr::write_volatile(byte, 0) };
    }
    core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);
}

/// NaCl operations over an invocation's crypto capability.
pub struct Nacl<'s, 'a, 'c, 'r>(pub(super) &'s mut Crypto<'a, 'c, 'r>);

impl Nacl<'_, '_, '_, '_> {
    /// Encrypt and authenticate with a secret key.
    pub fn secretbox<'k>(
        &mut self,
        message: &[u8],
        nonce: &[u8; 24],
        key: impl Into<Key<'k>>,
    ) -> Result<Vec<u8>> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(1, &[message, nonce, key])?,
            Key::Managed(key, reference) => {
                self.managed("nacl.secretbox", key, reference, &[message, nonce])?
            }
        };
        exact(bytes(value)?, message.len() + OVERHEAD_LENGTH)
    }

    /// Open a secretbox: `None` when authentication fails.
    pub fn secretbox_open<'k>(
        &mut self,
        ciphertext: &[u8],
        nonce: &[u8; 24],
        key: impl Into<Key<'k>>,
    ) -> Result<Option<Vec<u8>>> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(2, &[ciphertext, nonce, key])?,
            Key::Managed(key, reference) => {
                self.managed("nacl.secretbox.open", key, reference, &[ciphertext, nonce])?
            }
        };
        opened(value)
    }

    /// X25519 scalar multiplication over raw keys.
    pub fn scalar_mult(&mut self, secret: &[u8; 32], public_key: &[u8; 32]) -> Result<[u8; 32]> {
        fixed(bytes(self.raw(3, &[secret, public_key])?)?)
    }

    /// The X25519 public key of a secret: raw, or a managed X25519
    /// key's (needs the `publicKey` usage).
    pub fn scalar_mult_base<'k>(&mut self, key: impl Into<Key<'k>>) -> Result<[u8; 32]> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(4, &[key])?,
            Key::Managed(key, reference) => {
                self.managed("nacl.scalarMult.base", key, reference, &[])?
            }
        };
        fixed(bytes(value)?)
    }

    /// Precompute a raw box shared key. Managed keys never export one.
    pub fn box_before(&mut self, peer: &[u8; 32], secret: &[u8; 32]) -> Result<[u8; 32]> {
        fixed(bytes(self.raw(5, &[peer, secret])?)?)
    }

    /// Precompute a managed shared key without exporting its bytes.
    /// Requires `derive`; using it also requires `encrypt` or `decrypt`.
    pub fn box_before_managed(
        &mut self,
        peer: &[u8; 32],
        key: &super::ManagedKey,
        reference: ManagedKeyRef,
    ) -> Result<SharedKey> {
        match self.managed("nacl.box.before", key, reference, &[peer])? {
            Value::Number(slot)
                if (0.0..=u32::MAX as f64).contains(&slot) && slot == (slot as u32) as f64 =>
            {
                Ok(SharedKey {
                    key: key.clone(),
                    slot: slot as u32,
                })
            }
            _ => Err(fail(
                codes::TYPE_MISMATCH,
                "invalid managed shared-key handle",
            )),
        }
    }

    /// Encrypt to a peer's public key with an X25519 secret.
    pub fn box_seal<'k>(
        &mut self,
        message: &[u8],
        nonce: &[u8; 24],
        peer: &[u8; 32],
        key: impl Into<Key<'k>>,
    ) -> Result<Vec<u8>> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(6, &[message, nonce, peer, key])?,
            Key::Managed(key, reference) => {
                self.managed("nacl.box", key, reference, &[message, nonce, peer])?
            }
        };
        exact(bytes(value)?, message.len() + OVERHEAD_LENGTH)
    }

    /// Open a box from a peer: `None` when authentication fails.
    pub fn box_open<'k>(
        &mut self,
        ciphertext: &[u8],
        nonce: &[u8; 24],
        peer: &[u8; 32],
        key: impl Into<Key<'k>>,
    ) -> Result<Option<Vec<u8>>> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(7, &[ciphertext, nonce, peer, key])?,
            Key::Managed(key, reference) => {
                self.managed("nacl.box.open", key, reference, &[ciphertext, nonce, peer])?
            }
        };
        opened(value)
    }

    /// Box with a precomputed shared key: secretbox under it.
    pub fn box_after<'k>(
        &mut self,
        message: &[u8],
        nonce: &[u8; 24],
        shared: impl Into<BoxKey<'k>>,
    ) -> Result<Vec<u8>> {
        match shared.into() {
            BoxKey::Raw(key) => self.secretbox(message, nonce, key),
            BoxKey::Managed(key) => {
                let value = self.shared("nacl.box.after", key, message, nonce)?;
                exact(bytes(value)?, message.len() + OVERHEAD_LENGTH)
            }
        }
    }

    /// Open with a precomputed shared key.
    pub fn box_open_after<'k>(
        &mut self,
        ciphertext: &[u8],
        nonce: &[u8; 24],
        shared: impl Into<BoxKey<'k>>,
    ) -> Result<Option<Vec<u8>>> {
        match shared.into() {
            BoxKey::Raw(key) => self.secretbox_open(ciphertext, nonce, key),
            BoxKey::Managed(key) => {
                opened(self.shared("nacl.box.open.after", key, ciphertext, nonce)?)
            }
        }
    }

    fn shared(
        &mut self,
        operation: &str,
        key: &SharedKey,
        message: &[u8],
        nonce: &[u8; 24],
    ) -> Result<Value> {
        self.0.managed(
            operation,
            &key.key,
            ManagedKeyRef::Current,
            &[message, nonce],
            &alloc::format!("{{\"shared\":{}}}", key.slot),
            false,
        )
    }

    /// Sign: `signature || message`. Raw keys are 64-byte secret keys.
    pub fn sign<'k>(&mut self, message: &[u8], key: impl Into<Key<'k>>) -> Result<Vec<u8>> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(8, &[message, key])?,
            Key::Managed(key, reference) => {
                self.managed("nacl.sign", key, reference, &[message])?
            }
        };
        exact(bytes(value)?, message.len() + SIGNATURE_LENGTH)
    }

    /// Open a signed message: `None` unless the signature verifies.
    /// Raw keys are 32-byte public keys.
    pub fn sign_open<'k>(
        &mut self,
        signed: &[u8],
        key: impl Into<Key<'k>>,
    ) -> Result<Option<Vec<u8>>> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(9, &[signed, key])?,
            Key::Managed(key, reference) => {
                self.managed("nacl.sign.open", key, reference, &[signed])?
            }
        };
        opened(value)
    }

    /// A detached signature.
    pub fn sign_detached<'k>(
        &mut self,
        message: &[u8],
        key: impl Into<Key<'k>>,
    ) -> Result<[u8; 64]> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(10, &[message, key])?,
            Key::Managed(key, reference) => {
                self.managed("nacl.sign.detached", key, reference, &[message])?
            }
        };
        fixed(bytes(value)?)
    }

    /// Verify a detached signature.
    pub fn sign_detached_verify<'k>(
        &mut self,
        message: &[u8],
        signature: &[u8; 64],
        key: impl Into<Key<'k>>,
    ) -> Result<bool> {
        let value = match key.into() {
            Key::Raw(key) => self.raw(11, &[message, signature, key])?,
            Key::Managed(key, reference) => self.managed(
                "nacl.sign.detached.verify",
                key,
                reference,
                &[message, signature],
            )?,
        };
        match value {
            Value::Bool(matched) => Ok(matched),
            _ => Err(fail(codes::TYPE_MISMATCH, "verification is a bool")),
        }
    }

    /// The signing key pair of a 32-byte seed.
    pub fn sign_key_pair_from_seed(&mut self, seed: &[u8; 32]) -> Result<KeyPair> {
        self.key_pair(12, seed, 64)
    }

    /// The signing key pair of a 64-byte secret key, checking that its
    /// public half matches.
    pub fn sign_key_pair_from_secret_key(&mut self, secret: &[u8; 64]) -> Result<KeyPair> {
        self.key_pair(13, secret, 64)
    }

    /// The box key pair of a 32-byte secret key.
    pub fn box_key_pair_from_secret_key(&mut self, secret: &[u8; 32]) -> Result<KeyPair> {
        self.key_pair(14, secret, 32)
    }

    /// A random signing key pair: mutations only, unless a guest-local
    /// source is set.
    pub fn sign_key_pair(&mut self) -> Result<KeyPair> {
        self.random_key_pair(12, 64)
    }

    /// A random box key pair: mutations only, unless a guest-local
    /// source is set.
    pub fn box_key_pair(&mut self) -> Result<KeyPair> {
        self.random_key_pair(14, 32)
    }

    /// NaCl's SHA-512. For SHA-256 use [`Crypto::sha256`].
    pub fn hash(&mut self, message: &[u8]) -> Result<[u8; 64]> {
        fixed(bytes(self.raw(15, &[message])?)?)
    }

    /// Constant-time equality; two empty inputs compare unequal, as in
    /// NaCl.
    pub fn verify(&mut self, left: &[u8], right: &[u8]) -> Result<bool> {
        match self.raw(16, &[left, right])? {
            Value::Bool(equal) => Ok(equal),
            _ => Err(fail(codes::TYPE_MISMATCH, "verification is a bool")),
        }
    }

    fn random_key_pair(&mut self, op: u32, secret_length: usize) -> Result<KeyPair> {
        let mut seed = self.0.random_bytes(32)?;
        let pair = self.key_pair(op, &seed, secret_length);
        wipe(&mut seed);
        pair
    }

    fn key_pair(&mut self, op: u32, secret: &[u8], secret_length: usize) -> Result<KeyPair> {
        let mut packed = bytes(self.raw(op, &[secret])?)?;
        let pair = if packed.len() != 32 + secret_length {
            Err(fail(codes::INVALID_VALUE, "key pair has the wrong length"))
        } else {
            let mut public_key = [0u8; 32];
            public_key.copy_from_slice(&packed[..32]);
            Ok(KeyPair {
                public_key,
                secret_key: packed[32..].to_vec(),
            })
        };
        wipe(&mut packed);
        pair
    }

    fn raw(&mut self, op: u32, args: &[&[u8]]) -> Result<Value> {
        let mut values = Vec::with_capacity(args.len() + 1);
        values.push(Value::Number(f64::from(op)));
        values.extend(args.iter().map(|arg| Value::String(hex_encode(arg))));
        self.0.call(CryptoOp::Nacl, values)
    }

    fn managed(
        &mut self,
        operation: &str,
        key: &super::ManagedKey,
        reference: ManagedKeyRef,
        args: &[&[u8]],
    ) -> Result<Value> {
        self.0.managed(operation, key, reference, args, "{}", false)
    }
}

fn bytes(value: Value) -> Result<Vec<u8>> {
    super::bytes_of(value)
}

fn opened(value: Value) -> Result<Option<Vec<u8>>> {
    match value {
        Value::Null => Ok(None),
        value => bytes(value).map(Some),
    }
}

fn exact(bytes: Vec<u8>, length: usize) -> Result<Vec<u8>> {
    if bytes.len() != length {
        return Err(fail(
            codes::INVALID_VALUE,
            "crypto result has the wrong length",
        ));
    }
    Ok(bytes)
}

fn fixed<const N: usize>(bytes: Vec<u8>) -> Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| fail(codes::INVALID_VALUE, "crypto result has the wrong length"))
}
