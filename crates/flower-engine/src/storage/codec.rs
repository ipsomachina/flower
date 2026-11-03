//! Stored value codecs.
//!
//! Record values are canonical JSON packed with lz4; every read decompresses
//! before returning bytes to the caller. Derived outcomes, dependent edge
//! lists, code artifacts and meta scalars use the small binary codecs below.
//! Receipt values stay opaque bytes until the retry machinery defines their
//! result/fingerprint/floor layout.

use crate::error::{Error, Result};

/// Derived-outcome value tag: successful value, canonical JSON.
pub const OUTCOME_VALUE: u8 = 0x00;
/// Derived-outcome value tag: computation error, canonical JSON
/// `{"code","message","details"}`.
pub const OUTCOME_ERROR: u8 = 0x01;
/// Derived-outcome value tag: invalidated where re-evaluation cannot run
/// (replicated applies, activations); no payload, the pin survives.
pub const OUTCOME_INVALID: u8 = 0x02;
/// Derived-outcome flags bit: the root is pinned and never evicted.
pub const PINNED_FLAG: u8 = 0x01;
/// Outcome time tag: read no clock.
const TIME_ALWAYS: u8 = 0x00;
/// Outcome time tag: changes at a declared instant.
const TIME_BETWEEN: u8 = 0x01;
/// Outcome time tag: polled the clock.
const TIME_AT: u8 = 0x02;

/// Pack canonical JSON record bytes with lz4.
pub fn pack_value(json: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(json)
}

/// Most bytes one lz4 input byte can expand to: a match-length
/// extension byte adds 255 output bytes. Bounds the size prefix.
const LZ4_MAX_RATIO: usize = 256;

/// Unpack lz4-packed record bytes back to canonical JSON. The stored
/// size prefix is checked against what the payload can expand to
/// before anything is allocated, so a corrupt prefix fails as
/// [`Error::Corrupt`] instead of reserving gigabytes.
pub fn unpack_value(packed: &[u8]) -> Result<Vec<u8>> {
    let size = unpacked_size(packed)?;
    let value = lz4_flex::decompress(&packed[4..], size)
        .map_err(|error| Error::corrupt(format!("record lz4 payload rejected: {error}")))?;
    if value.len() != size {
        return Err(Error::corrupt("record lz4 payload disagrees with its size"));
    }
    Ok(value)
}

/// Validate a record's expansion size without allocating its decoded payload.
pub(crate) fn unpacked_size(packed: &[u8]) -> Result<usize> {
    let (size, payload) = lz4_flex::block::uncompressed_size(packed)
        .map_err(|error| Error::corrupt(format!("record lz4 payload rejected: {error}")))?;
    if size > payload.len().saturating_mul(LZ4_MAX_RATIO) {
        return Err(Error::corrupt("record lz4 size prefix exceeds its payload"));
    }
    Ok(size)
}

/// A stored derived outcome: pin state plus value or computation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedOutcome {
    /// True when a materialized root pins this outcome.
    pub pinned: bool,
    /// Code identity that computed this outcome. Activation changes the
    /// module's identity, so stale outcomes re-evaluate instead of
    /// serving values from retired code.
    pub code: [u8; crate::storage::keys::HASH_LEN],
    /// When the outcome serves: what its evaluation saw of the clock.
    pub time: OutcomeTime,
    /// Value or error payload, canonical JSON bytes.
    pub body: OutcomeBody,
}

/// When a derived outcome serves, from what its evaluation (and every
/// value it read) saw of the clock. A read at another time evaluates
/// again, keeping the pin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutcomeTime {
    /// Read no clock: serves at any time.
    Always,
    /// Declared (or read a value declaring) that it changes at `until`:
    /// serves from its evaluation's clock `from` until then.
    Between {
        /// The evaluation's clock, in milliseconds.
        from: u64,
        /// The declared instant, in milliseconds.
        until: u64,
    },
    /// Polled the clock (`now()`) at this instant: serves reads at it
    /// alone, as v1's cells over its advancing `clock` record.
    At(u64),
}

impl OutcomeTime {
    /// Whether an outcome with this time serves a read at `now_ms`.
    pub fn serves_at(self, now_ms: u64) -> bool {
        match self {
            OutcomeTime::Always => true,
            OutcomeTime::Between { from, until } => from <= now_ms && now_ms < until,
            OutcomeTime::At(at) => at == now_ms,
        }
    }
}

/// The payload of a derived outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutcomeBody {
    /// Successful value.
    Value(Vec<u8>),
    /// Computation error marker.
    Error(Vec<u8>),
    /// Invalidated without re-evaluation: never served, re-evaluated on
    /// the next read. Only pinned outcomes are kept this way, so the pin
    /// outlives an invalidation that could not refresh the value.
    Invalid,
}

impl DerivedOutcome {
    /// A pinned outcome invalidated without re-evaluation: the next read
    /// re-evaluates it and the pin carries over.
    pub fn invalidated_pin(code: [u8; crate::storage::keys::HASH_LEN]) -> Self {
        DerivedOutcome {
            pinned: true,
            code,
            time: OutcomeTime::Always,
            body: OutcomeBody::Invalid,
        }
    }

    /// Whether the outcome may be served as a cached value at all.
    pub fn servable(&self) -> bool {
        !matches!(self.body, OutcomeBody::Invalid)
    }

    /// Whether the outcome serves a read at `now_ms`.
    pub fn serves_at(&self, now_ms: u64) -> bool {
        self.servable() && self.time.serves_at(now_ms)
    }
}

/// Encode an outcome: flags byte, tag byte, code identity, its time,
/// payload.
pub fn encode_outcome(outcome: &DerivedOutcome) -> Vec<u8> {
    let (tag, payload): (u8, &[u8]) = match &outcome.body {
        OutcomeBody::Value(bytes) => (OUTCOME_VALUE, bytes),
        OutcomeBody::Error(bytes) => (OUTCOME_ERROR, bytes),
        OutcomeBody::Invalid => (OUTCOME_INVALID, &[]),
    };
    let mut encoded = Vec::with_capacity(payload.len() + 19 + outcome.code.len());
    encoded.push(if outcome.pinned { PINNED_FLAG } else { 0x00 });
    encoded.push(tag);
    encoded.extend_from_slice(&outcome.code);
    match outcome.time {
        OutcomeTime::Always => encoded.push(TIME_ALWAYS),
        OutcomeTime::Between { from, until } => {
            encoded.push(TIME_BETWEEN);
            encoded.extend_from_slice(&from.to_be_bytes());
            encoded.extend_from_slice(&until.to_be_bytes());
        }
        OutcomeTime::At(at) => {
            encoded.push(TIME_AT);
            encoded.extend_from_slice(&at.to_be_bytes());
        }
    }
    encoded.extend_from_slice(payload);
    encoded
}

/// Decode an outcome, rejecting unknown flags, tags and truncation.
pub fn decode_outcome(bytes: &[u8]) -> Result<DerivedOutcome> {
    let mut header = 2 + crate::storage::keys::HASH_LEN;
    if bytes.len() < header {
        return Err(Error::corrupt("truncated derived outcome"));
    }
    let flags = bytes[0];
    if flags & !PINNED_FLAG != 0 {
        return Err(Error::corrupt("unknown derived outcome flags"));
    }
    let code: [u8; crate::storage::keys::HASH_LEN] = bytes[2..header]
        .try_into()
        .map_err(|_| Error::corrupt("truncated outcome identity"))?;
    let instant = |at: usize| -> Result<u64> {
        let bytes: [u8; 8] = bytes
            .get(at..at + 8)
            .and_then(|slice| slice.try_into().ok())
            .ok_or_else(|| Error::corrupt("truncated outcome time"))?;
        Ok(u64::from_be_bytes(bytes))
    };
    let tag = *bytes
        .get(header)
        .ok_or_else(|| Error::corrupt("truncated outcome time"))?;
    header += 1;
    let time = match tag {
        TIME_ALWAYS => OutcomeTime::Always,
        TIME_BETWEEN => {
            let time = OutcomeTime::Between {
                from: instant(header)?,
                until: instant(header + 8)?,
            };
            header += 16;
            time
        }
        TIME_AT => {
            let time = OutcomeTime::At(instant(header)?);
            header += 8;
            time
        }
        _ => return Err(Error::corrupt("unknown derived outcome time")),
    };
    let body = match bytes[1] {
        OUTCOME_VALUE => OutcomeBody::Value(bytes[header..].to_vec()),
        OUTCOME_ERROR => OutcomeBody::Error(bytes[header..].to_vec()),
        OUTCOME_INVALID if bytes.len() == header => OutcomeBody::Invalid,
        OUTCOME_INVALID => return Err(Error::corrupt("invalid derived outcome has a payload")),
        _ => return Err(Error::corrupt("unknown derived outcome tag")),
    };
    Ok(DerivedOutcome {
        pinned: flags & PINNED_FLAG != 0,
        code,
        time,
        body,
    })
}

/// Encode a dependent's prior edge list: count plus length-prefixed
/// dependency keys.
pub fn encode_edge_list(edges: &[Vec<u8>]) -> Result<Vec<u8>> {
    let count = u32::try_from(edges.len()).map_err(|_| Error::corrupt("edge list too long"))?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&count.to_be_bytes());
    for edge in edges {
        let len = u32::try_from(edge.len()).map_err(|_| Error::corrupt("edge too long"))?;
        encoded.extend_from_slice(&len.to_be_bytes());
        encoded.extend_from_slice(edge);
    }
    Ok(encoded)
}

/// Decode a dependent's prior edge list.
pub fn decode_edge_list(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut pos = 0;
    let take = |pos: &mut usize, len: usize| -> Result<&[u8]> {
        let end = pos
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| Error::corrupt("truncated edge list"))?;
        let slice = &bytes[*pos..end];
        *pos = end;
        Ok(slice)
    };
    let raw = take(&mut pos, 4)?;
    let count = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let mut edges = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let raw = take(&mut pos, 4)?;
        let len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
        edges.push(take(&mut pos, len)?.to_vec());
    }
    if pos != bytes.len() {
        return Err(Error::corrupt("trailing edge-list bytes"));
    }
    Ok(edges)
}

/// Encode a code artifact: Wasm bytes plus validated manifest.
pub fn encode_code(wasm: &[u8], manifest: &[u8]) -> Result<Vec<u8>> {
    let wasm_len = u32::try_from(wasm.len()).map_err(|_| Error::corrupt("wasm too large"))?;
    let manifest_len =
        u32::try_from(manifest.len()).map_err(|_| Error::corrupt("manifest too large"))?;
    let mut encoded = Vec::with_capacity(wasm.len() + manifest.len() + 8);
    encoded.extend_from_slice(&wasm_len.to_be_bytes());
    encoded.extend_from_slice(wasm);
    encoded.extend_from_slice(&manifest_len.to_be_bytes());
    encoded.extend_from_slice(manifest);
    Ok(encoded)
}

/// Decode a code artifact into Wasm bytes and manifest.
pub fn decode_code(bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut pos = 0;
    let take = |pos: &mut usize, len: usize| -> Result<&[u8]> {
        let end = pos
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| Error::corrupt("truncated code artifact"))?;
        let slice = &bytes[*pos..end];
        *pos = end;
        Ok(slice)
    };
    let raw = take(&mut pos, 4)?;
    let wasm_len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let wasm = take(&mut pos, wasm_len)?.to_vec();
    let raw = take(&mut pos, 4)?;
    let manifest_len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let manifest = take(&mut pos, manifest_len)?.to_vec();
    if pos != bytes.len() {
        return Err(Error::corrupt("trailing code bytes"));
    }
    Ok((wasm, manifest))
}

/// Encode a meta scalar: big-endian `u64`.
pub fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Decode a meta scalar, rejecting any other length.
pub fn decode_u64(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(Error::corrupt("bad u64 length"));
    }
    Ok(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

/// Encode retention floors: count plus big-endian `u64` entries.
pub fn encode_floors(floors: &[u64]) -> Result<Vec<u8>> {
    let count = u32::try_from(floors.len()).map_err(|_| Error::corrupt("too many floors"))?;
    let mut encoded = Vec::with_capacity(floors.len() * 8 + 4);
    encoded.extend_from_slice(&count.to_be_bytes());
    for floor in floors {
        encoded.extend_from_slice(&floor.to_be_bytes());
    }
    Ok(encoded)
}

/// Decode retention floors.
pub fn decode_floors(bytes: &[u8]) -> Result<Vec<u64>> {
    if bytes.len() < 4 || !(bytes.len() - 4).is_multiple_of(8) {
        return Err(Error::corrupt("bad floors length"));
    }
    let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    // The count must agree with the bytes before it sizes anything.
    if count != (bytes.len() - 4) / 8 {
        return Err(Error::corrupt("floors count mismatch"));
    }
    let (chunks, _) = bytes[4..].as_chunks::<8>();
    Ok(chunks
        .iter()
        .map(|chunk| u64::from_be_bytes(*chunk))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_pack_and_unpack() {
        let json = br#"{"title":"Buy milk","count":2}"#;
        let packed = pack_value(json);
        assert_eq!(unpack_value(&packed).unwrap(), json);
        assert!(unpack_value(b"not lz4").is_err());
    }

    #[test]
    fn huge_size_prefixes_fail_without_allocating() {
        // A 4 GiB size prefix over a few payload bytes.
        let mut packed = u32::MAX.to_le_bytes().to_vec();
        packed.extend_from_slice(b"\x10a");
        let error = unpack_value(&packed).unwrap_err();
        assert!(matches!(error, Error::Corrupt(_)), "{error}");
        // Highly compressible values still round-trip.
        let zeros = vec![b'0'; 1 << 20];
        assert_eq!(unpack_value(&pack_value(&zeros)).unwrap(), zeros);
    }

    #[test]
    fn huge_floor_counts_fail_without_allocating() {
        assert!(decode_floors(&u32::MAX.to_be_bytes()).is_err());
        let mut short = u32::MAX.to_be_bytes().to_vec();
        short.extend_from_slice(&7u64.to_be_bytes());
        assert!(decode_floors(&short).is_err());
    }

    #[test]
    fn invalid_outcomes_keep_pins_without_payload() {
        let outcome = DerivedOutcome::invalidated_pin([4u8; crate::storage::keys::HASH_LEN]);
        assert!(!outcome.servable());
        let encoded = encode_outcome(&outcome);
        assert_eq!(decode_outcome(&encoded).unwrap(), outcome);
        let mut trailing = encoded;
        trailing.push(b'x');
        assert!(decode_outcome(&trailing).is_err());
    }

    #[test]
    fn outcomes_round_trip_with_pins_and_times() {
        for time in [
            OutcomeTime::Always,
            OutcomeTime::Between {
                from: 10,
                until: 20,
            },
            OutcomeTime::At(u64::MAX),
        ] {
            let outcome = DerivedOutcome {
                code: [3u8; crate::storage::keys::HASH_LEN],
                pinned: true,
                time,
                body: OutcomeBody::Error(br#"{"code":"x"}"#.to_vec()),
            };
            let encoded = encode_outcome(&outcome);
            assert_eq!(decode_outcome(&encoded).unwrap(), outcome);
        }
        let between = encode_outcome(&DerivedOutcome {
            code: [3u8; crate::storage::keys::HASH_LEN],
            pinned: false,
            time: OutcomeTime::Between { from: 1, until: 2 },
            body: OutcomeBody::Value(b"1".to_vec()),
        });
        // Truncated inside the time or before it, and an unknown time tag.
        assert!(decode_outcome(&between[..40]).is_err());
        assert!(decode_outcome(&between[..34]).is_err());
        let mut unknown = between.clone();
        unknown[34] = 0x07;
        assert!(decode_outcome(&unknown).is_err());
        assert!(decode_outcome(&between[..1]).is_err());
        assert!(decode_outcome(&[0x02, OUTCOME_VALUE]).is_err());
        assert!(decode_outcome(&[0x00, 0x09]).is_err());
    }

    #[test]
    fn outcome_times_bound_when_they_serve() {
        assert!(OutcomeTime::Always.serves_at(0));
        let between = OutcomeTime::Between {
            from: 10,
            until: 20,
        };
        assert!(!between.serves_at(9));
        assert!(between.serves_at(10) && between.serves_at(19));
        assert!(!between.serves_at(20));
        assert!(OutcomeTime::At(10).serves_at(10));
        assert!(!OutcomeTime::At(10).serves_at(11));
    }

    #[test]
    fn edge_lists_round_trip() {
        let edges = vec![vec![1, 2], vec![3]];
        let encoded = encode_edge_list(&edges).unwrap();
        assert_eq!(decode_edge_list(&encoded).unwrap(), edges);
        assert!(decode_edge_list(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0x00);
        assert!(decode_edge_list(&trailing).is_err());
    }

    #[test]
    fn code_artifacts_round_trip() {
        let (wasm, manifest) = (vec![0x00, 0x61], vec![0x05, 0x01]);
        let encoded = encode_code(&wasm, &manifest).unwrap();
        assert_eq!(decode_code(&encoded).unwrap(), (wasm, manifest));
        assert!(decode_code(&encoded[..3]).is_err());
    }

    #[test]
    fn scalars_and_floors_round_trip() {
        assert_eq!(decode_u64(&encode_u64(u64::MAX)).unwrap(), u64::MAX);
        assert!(decode_u64(&[1, 2, 3]).is_err());
        let floors = vec![1, 40, 300];
        let encoded = encode_floors(&floors).unwrap();
        assert_eq!(decode_floors(&encoded).unwrap(), floors);
        assert!(decode_floors(&encoded[..5]).is_err());
    }
}
