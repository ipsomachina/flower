//! Versioned AES-256-GCM envelopes.
//!
//! A sealed object is `FLB1 || version u32 BE || nonce 96-bit ||
//! ciphertext+tag`, with the 20-byte header bound as associated
//! data: tampering with the version or nonce fails authentication.
//! Each object carries a unique nonce and records its key version,
//! so readers select retained versions while writers always seal
//! under the current one.
//!
//! A sealed stream ([`StreamSealer`], [`StreamOpener`]) is the same
//! cipher in frames, for objects too large to seal, hold or verify in
//! one piece: `FLS1 || version u32 BE || stream ID 128-bit || frame
//! length u32 BE`, then frames of `nonce 96-bit || ciphertext+tag`.
//! Every frame but the last seals exactly the frame length of
//! plaintext, and the last seals less (possibly nothing), so a reader
//! knows which frame ends the stream from its size alone. Each frame
//! authenticates the header, its index and whether it is the last as
//! associated data: reordered, dropped, repeated or spliced frames
//! (the stream ID differs per stream), a stream cut at any byte, and
//! a changed header all fail, and a reader releases plaintext only
//! frame by authenticated frame.

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};

/// Envelope magic bytes.
pub const MAGIC: &[u8; 4] = b"FLB1";
/// Header length: magic plus version plus nonce.
pub const HEADER_LEN: usize = 20;
/// GCM nonce length in bytes.
pub const NONCE_LEN: usize = 12;
/// GCM authentication tag length in bytes.
pub const TAG_LEN: usize = 16;

/// A crypto failure: malformed envelopes, failed authentication,
/// bad key material or keystore IO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CryptoError {
    /// Envelope magic mismatch.
    BadMagic,
    /// Envelope shorter than header plus tag.
    Truncated,
    /// Authentication failed: wrong key or tampered bytes.
    AuthFailed,
    /// Sealing failed: plaintext exceeds cipher limits.
    SealFailed,
    /// Key material has the wrong length.
    BadKeyLength(usize),
    /// Keystore file IO failure.
    Io(String),
    /// Keystore JSON failure.
    Store(String),
    /// No such key version.
    UnknownVersion(u32),
    /// A stream header declares a frame length out of range.
    BadHeader,
}

impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptoError::BadMagic => write!(f, "bad envelope magic"),
            CryptoError::Truncated => write!(f, "truncated envelope"),
            CryptoError::AuthFailed => write!(f, "authentication failed"),
            CryptoError::SealFailed => write!(f, "sealing failed"),
            CryptoError::BadKeyLength(len) => write!(f, "bad key length {len}"),
            CryptoError::Io(message) => write!(f, "keystore io: {message}"),
            CryptoError::Store(message) => write!(f, "keystore: {message}"),
            CryptoError::UnknownVersion(version) => {
                write!(f, "unknown key version {version}")
            }
            CryptoError::BadHeader => write!(f, "bad stream header"),
        }
    }
}

impl std::error::Error for CryptoError {}

/// Seal plaintext under a key version with a caller-supplied nonce.
/// Nonces must never repeat for a version; objects are immutable
/// once written, so fresh random nonces per object suffice.
pub fn seal(
    key: &[u8; 32],
    version: u32,
    nonce: &[u8; NONCE_LEN],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(MAGIC);
    header.extend_from_slice(&version.to_be_bytes());
    header.extend_from_slice(nonce);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: plaintext,
                aad: &header,
            },
        )
        .map_err(|_| CryptoError::SealFailed)?;
    header.extend(ciphertext);
    Ok(header)
}

/// Open a sealed envelope: verify the magic, authenticate the
/// header-bound ciphertext and return the key version plus
/// plaintext.
pub fn open(key: &[u8; 32], sealed: &[u8]) -> Result<(u32, Vec<u8>), CryptoError> {
    if sealed.len() < HEADER_LEN + TAG_LEN {
        return Err(CryptoError::Truncated);
    }
    if &sealed[..4] != MAGIC {
        return Err(CryptoError::BadMagic);
    }
    let version = u32::from_be_bytes([sealed[4], sealed[5], sealed[6], sealed[7]]);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(&sealed[8..HEADER_LEN]),
            Payload {
                msg: &sealed[HEADER_LEN..],
                aad: &sealed[..HEADER_LEN],
            },
        )
        .map_err(|_| CryptoError::AuthFailed)?;
    Ok((version, plaintext))
}

/// Read an envelope's key version without authenticating: for
/// selecting the key before opening. Only the header is read, so
/// 20-byte range fetches suffice; the version is untrusted until
/// `open` authenticates it as associated data.
pub fn peek_version(sealed: &[u8]) -> Result<u32, CryptoError> {
    if sealed.len() < HEADER_LEN {
        return Err(CryptoError::Truncated);
    }
    if &sealed[..4] != MAGIC {
        return Err(CryptoError::BadMagic);
    }
    Ok(u32::from_be_bytes([
        sealed[4], sealed[5], sealed[6], sealed[7],
    ]))
}

/// A fresh random 96-bit nonce.
pub fn random_nonce() -> [u8; NONCE_LEN] {
    rand::random()
}

/// Sealed-stream magic bytes.
pub const STREAM_MAGIC: &[u8; 4] = b"FLS1";
/// Sealed-stream header length: magic, key version, stream ID and
/// frame length.
pub const STREAM_HEADER_LEN: usize = 28;
/// What sealing adds to each frame: its nonce and tag.
pub const FRAME_OVERHEAD: usize = NONCE_LEN + TAG_LEN;
/// The longest frame a stream may declare, so a reader buffers at
/// most this much before the first frame authenticates its header.
pub const MAX_FRAME_LEN: usize = 16 << 20;

/// Seals a stream frame by frame as plaintext arrives (see the module
/// documentation), holding at most one frame of plaintext plus the
/// sealed bytes not yet taken.
pub struct StreamSealer {
    cipher: Aes256Gcm,
    header: [u8; STREAM_HEADER_LEN],
    frame_len: usize,
    index: u64,
    plain: Vec<u8>,
    sealed: Vec<u8>,
}

impl StreamSealer {
    /// Start a stream under a key version with frames of `frame_len`
    /// plaintext bytes, at most [`MAX_FRAME_LEN`]. The header is the
    /// first sealed output.
    pub fn new(key: &[u8; 32], version: u32, frame_len: usize) -> Result<Self, CryptoError> {
        if frame_len == 0 || frame_len > MAX_FRAME_LEN {
            return Err(CryptoError::BadHeader);
        }
        let mut header = [0u8; STREAM_HEADER_LEN];
        header[..4].copy_from_slice(STREAM_MAGIC);
        header[4..8].copy_from_slice(&version.to_be_bytes());
        header[8..24].copy_from_slice(&rand::random::<[u8; 16]>());
        header[24..].copy_from_slice(&(frame_len as u32).to_be_bytes());
        Ok(StreamSealer {
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key)),
            header,
            frame_len,
            index: 0,
            plain: Vec::with_capacity(frame_len),
            sealed: header.to_vec(),
        })
    }

    /// Add plaintext, sealing every frame it fills.
    pub fn write(&mut self, mut plaintext: &[u8]) -> Result<(), CryptoError> {
        while !plaintext.is_empty() {
            let take = (self.frame_len - self.plain.len()).min(plaintext.len());
            self.plain.extend_from_slice(&plaintext[..take]);
            plaintext = &plaintext[take..];
            // A full frame is never the last: the last is shorter.
            if self.plain.len() == self.frame_len {
                self.seal_frame(false)?;
            }
        }
        Ok(())
    }

    /// Sealed bytes ready to take.
    pub fn ready(&self) -> usize {
        self.sealed.len()
    }

    /// Take the first `len` ready bytes (all of them, if fewer).
    pub fn take(&mut self, len: usize) -> Vec<u8> {
        let rest = self.sealed.split_off(len.min(self.sealed.len()));
        std::mem::replace(&mut self.sealed, rest)
    }

    /// Seal the last frame and return every byte not yet taken.
    pub fn finish(mut self) -> Result<Vec<u8>, CryptoError> {
        self.seal_frame(true)?;
        Ok(self.sealed)
    }

    /// Seal the buffered plaintext as the next frame.
    fn seal_frame(&mut self, last: bool) -> Result<(), CryptoError> {
        let nonce = random_nonce();
        let aad = frame_aad(&self.header, self.index, last);
        let ciphertext = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &self.plain,
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::SealFailed)?;
        self.sealed.extend_from_slice(&nonce);
        self.sealed.extend(ciphertext);
        self.plain.clear();
        self.index += 1;
        Ok(())
    }
}

/// Opens a sealed stream frame by frame as its bytes arrive, releasing
/// only authenticated plaintext and holding at most one frame.
pub struct StreamOpener {
    cipher: Aes256Gcm,
    header: [u8; STREAM_HEADER_LEN],
    frame_len: usize,
    index: u64,
    buffer: Vec<u8>,
}

impl StreamOpener {
    /// Start opening a stream from its header, the first
    /// [`STREAM_HEADER_LEN`] bytes, under the key its version names
    /// (see [`peek_stream_version`]). The header is untrusted until the
    /// first frame authenticates it.
    pub fn new(key: &[u8; 32], header: &[u8]) -> Result<Self, CryptoError> {
        peek_stream_version(header)?;
        let header: [u8; STREAM_HEADER_LEN] = header
            .get(..STREAM_HEADER_LEN)
            .and_then(|header| header.try_into().ok())
            .ok_or(CryptoError::Truncated)?;
        let frame_len = u32::from_be_bytes([header[24], header[25], header[26], header[27]]);
        let frame_len = frame_len as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_LEN {
            return Err(CryptoError::BadHeader);
        }
        Ok(StreamOpener {
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key)),
            header,
            frame_len,
            index: 0,
            buffer: Vec::new(),
        })
    }

    /// Add sealed bytes that follow the header, appending the plaintext
    /// of every frame they complete to `out`. A full-size frame is
    /// never the last, so each opens as soon as it is whole.
    pub fn feed(&mut self, sealed: &[u8], out: &mut Vec<u8>) -> Result<(), CryptoError> {
        let full = FRAME_OVERHEAD + self.frame_len;
        let mut sealed = sealed;
        while !sealed.is_empty() {
            let take = (full - self.buffer.len()).min(sealed.len());
            self.buffer.extend_from_slice(&sealed[..take]);
            sealed = &sealed[take..];
            if self.buffer.len() == full {
                let frame = std::mem::take(&mut self.buffer);
                out.extend(self.open_frame(&frame, false)?);
            }
        }
        Ok(())
    }

    /// End the stream: the bytes left must be its last frame, shorter
    /// than a full one. A stream cut at a frame boundary has none.
    pub fn finish(mut self, out: &mut Vec<u8>) -> Result<(), CryptoError> {
        if self.buffer.len() < FRAME_OVERHEAD {
            return Err(CryptoError::Truncated);
        }
        let frame = std::mem::take(&mut self.buffer);
        out.extend(self.open_frame(&frame, true)?);
        Ok(())
    }

    /// Authenticate and open the next frame.
    fn open_frame(&mut self, frame: &[u8], last: bool) -> Result<Vec<u8>, CryptoError> {
        let aad = frame_aad(&self.header, self.index, last);
        let plaintext = self
            .cipher
            .decrypt(
                Nonce::from_slice(&frame[..NONCE_LEN]),
                Payload {
                    msg: &frame[NONCE_LEN..],
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::AuthFailed)?;
        self.index += 1;
        Ok(plaintext)
    }
}

/// A frame's associated data: the stream header, the frame's index
/// and whether it is the last.
fn frame_aad(header: &[u8; STREAM_HEADER_LEN], index: u64, last: bool) -> [u8; 37] {
    let mut aad = [0u8; 37];
    aad[..STREAM_HEADER_LEN].copy_from_slice(header);
    aad[STREAM_HEADER_LEN..36].copy_from_slice(&index.to_be_bytes());
    aad[36] = u8::from(last);
    aad
}

/// Read a sealed stream's key version without authenticating, from
/// its first 8 bytes or more: for selecting the key before opening.
pub fn peek_stream_version(sealed: &[u8]) -> Result<u32, CryptoError> {
    if sealed.len() < 8 {
        return Err(CryptoError::Truncated);
    }
    if &sealed[..4] != STREAM_MAGIC {
        return Err(CryptoError::BadMagic);
    }
    Ok(u32::from_be_bytes([
        sealed[4], sealed[5], sealed[6], sealed[7],
    ]))
}

/// Seal a whole plaintext as a stream of `frame_len` frames.
pub fn seal_stream(
    key: &[u8; 32],
    version: u32,
    frame_len: usize,
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let mut sealer = StreamSealer::new(key, version, frame_len)?;
    sealer.write(plaintext)?;
    sealer.finish()
}

/// Open a whole sealed stream, returning its key version and
/// plaintext.
pub fn open_stream(key: &[u8; 32], sealed: &[u8]) -> Result<(u32, Vec<u8>), CryptoError> {
    let version = peek_stream_version(sealed)?;
    let mut opener = StreamOpener::new(key, sealed)?;
    let mut plaintext = Vec::new();
    opener.feed(&sealed[STREAM_HEADER_LEN..], &mut plaintext)?;
    opener.finish(&mut plaintext)?;
    Ok((version, plaintext))
}

/// Seal without an envelope header: KEK wrapping of key material.
/// The caller stores the nonce alongside the sealed bytes.
pub(crate) fn seal_raw(
    key: &[u8; 32],
    nonce: &[u8; NONCE_LEN],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher
        .encrypt(Nonce::from_slice(nonce), plaintext)
        .map_err(|_| CryptoError::SealFailed)
}

/// Open KEK-wrapped bytes.
pub(crate) fn open_raw(
    key: &[u8; 32],
    nonce: &[u8],
    sealed: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    if nonce.len() != NONCE_LEN {
        return Err(CryptoError::Truncated);
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher
        .decrypt(Nonce::from_slice(nonce), sealed)
        .map_err(|_| CryptoError::AuthFailed)
}
