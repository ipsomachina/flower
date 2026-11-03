//! `flower-crypto`: the crypto lane.
//!
//! Hashing, versioned AEAD envelopes, guest NaCl, JOSE and WebAuthn,
//! and managed keys. The backup layer seals objects through these
//! envelopes; operator tooling derives deployment content identities
//! here; guests reach the rest through the `crypto_call` import.
//!
//! Module map:
//!
//! - [`hash`]: SHA-256 and deployment content identities.
//! - [`aead`]: versioned AES-256-GCM envelopes.
//! - [`keys`]: the KEK-wrapped deployment keystore.
//! - [`catalog`]: the replicated managed-key catalog (one per logical
//!   database) and its deterministic apply.
//! - [`replicated`]: the catalogs' runtime: wrapping keys, operator
//!   preparation, guest resolution and the prepared-key cache.
//! - [`nacl`]: TweetNaCl's high-level operations over raw keys.
//! - [`jwt`]: compact JWS and dir/A256GCM JWE.
//! - [`webauthn`]: passkey relying-party verification.
//! - [`host`]: the guest crypto ABI dispatch.

pub mod aead;
pub mod catalog;
mod cbor;
mod der;
pub mod hash;
pub mod host;
pub mod jwt;
pub mod keys;
pub mod nacl;
pub mod replicated;
pub mod webauthn;

pub use aead::CryptoError;
