//! Canonical-JSON text handling (engine side only).
//!
//! Stored record bytes, index components and dependency keys are canonical
//! JSON text. Guests exchange binary [`Value`]s across the ABI, so the host
//! canonicalizes on write ([`Value::canonical_json`]) and parses on read.
//! Parsing normalizes foreign text into values: key order, duplicate keys
//! and integers beyond the safe range follow the rules below rather than
//! failing, because every writer the engine serves produces canonical bytes
//! and anything else is foreign data, never a second canonical form.
//! Record keys are the exception: [`canonicalize_key`] rejects integers
//! beyond the safe range, since rounding would merge distinct keys.
//!
//! [`Value`]: flower_sdk::Value

use flower_sdk::Value;
use flower_sdk::value::MAX_VALUE_DEPTH;

use crate::error::{Error, Result};

/// Parse canonical-JSON text into a value. Duplicate object keys keep the
/// last occurrence; numbers become finite `f64`s (integers beyond the safe
/// range may round); nesting beyond [`MAX_VALUE_DEPTH`] fails.
pub fn parse_value(text: &[u8]) -> Result<Value> {
    let json: serde_json::Value =
        serde_json::from_slice(text).map_err(|_| Error::corrupt("stored bytes are not JSON"))?;
    convert(&json, 0)
}

fn convert(json: &serde_json::Value, depth: usize) -> Result<Value> {
    if depth > MAX_VALUE_DEPTH {
        return Err(Error::corrupt("JSON exceeds depth bound"));
    }
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(flag) => Ok(Value::Bool(*flag)),
        serde_json::Value::Number(number) => {
            let value = number
                .as_f64()
                .ok_or_else(|| Error::corrupt("JSON number is not a finite f64"))?;
            if !value.is_finite() {
                return Err(Error::corrupt("JSON number is not a finite f64"));
            }
            Ok(Value::Number(value))
        }
        serde_json::Value::String(text) => Ok(Value::String(text.clone())),
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(convert(item, depth + 1)?);
            }
            Ok(Value::Array(out))
        }
        serde_json::Value::Object(map) => {
            let mut out = std::collections::BTreeMap::new();
            for (key, item) in map {
                out.insert(key.clone(), convert(item, depth + 1)?);
            }
            Ok(Value::Object(out))
        }
    }
}

/// Canonicalize JSON text: parse, then render through the SDK's canonical
/// renderer. Foreign key order, spacing and duplicate keys normalize to the
/// canonical bytes; invalid text fails.
pub fn canonicalize(text: &[u8]) -> Result<Vec<u8>> {
    let value = parse_value(text)?;
    value
        .canonical_json()
        .map(String::into_bytes)
        .map_err(|failure| Error::corrupt(format!("cannot canonicalize JSON: {}", failure.message)))
}

/// Render a value to canonical JSON bytes.
pub fn render(value: &Value) -> Result<Vec<u8>> {
    value
        .canonical_json()
        .map(String::into_bytes)
        .map_err(|failure| {
            Error::corrupt(format!("cannot render canonical JSON: {}", failure.message))
        })
}

/// Largest integer an `f64` represents exactly together with its
/// neighbours: `2^53 - 1`.
pub const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// Canonicalize record-key text. Unlike stored values, keys never
/// normalize lossily: an integral number outside the safe range would
/// round to the same `f64` as its neighbours, so distinct key texts
/// would collapse onto one stored key. Such keys return `None`; invalid
/// JSON fails like [`canonicalize`].
pub fn canonicalize_key(text: &[u8]) -> Result<Option<Vec<u8>>> {
    let value = parse_value(text)?;
    if !numbers_are_exact(&value) {
        return Ok(None);
    }
    render(&value).map(Some)
}

/// Whether every number inside a value is a fraction or a safe integer.
fn numbers_are_exact(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.fract() != 0.0 || number.abs() <= MAX_SAFE_INTEGER,
        Value::Array(items) => items.iter().all(numbers_are_exact),
        Value::Object(map) => map.values().all(numbers_are_exact),
        Value::Null | Value::Bool(_) | Value::String(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_round_trip_canonically() {
        for text in [
            "null",
            "true",
            "42",
            "-17",
            "1.5",
            "\"a\"",
            "[1,\"x\",null]",
            "{\"b\":2,\"a\":[true]}",
        ] {
            let value = parse_value(text.as_bytes()).unwrap();
            let rendered = render(&value).unwrap();
            assert_eq!(parse_value(&rendered).unwrap(), value);
        }
        // Canonical key order.
        assert_eq!(
            canonicalize(b"{\"b\":1,\"a\":2}").unwrap(),
            b"{\"a\":2,\"b\":1}"
        );
    }

    #[test]
    fn malformed_text_fails() {
        assert!(parse_value(b"").is_err());
        assert!(parse_value(b"{oops}").is_err());
        assert!(parse_value(b"\xff\xfe").is_err());
        assert!(parse_value(b"1e999").is_err());
    }

    #[test]
    fn keys_reject_integers_beyond_the_safe_range() {
        assert_eq!(
            canonicalize_key(b"9007199254740991").unwrap(),
            Some(b"9007199254740991".to_vec())
        );
        // 2^53 + 1 and 2^53 round to the same f64: both reject.
        assert_eq!(canonicalize_key(b"9007199254740993").unwrap(), None);
        assert_eq!(canonicalize_key(b"9007199254740992").unwrap(), None);
        assert_eq!(canonicalize_key(b"-9007199254740993").unwrap(), None);
        assert_eq!(canonicalize_key(b"1e300").unwrap(), None);
        assert_eq!(canonicalize_key(b"[\"t\",9007199254740993]").unwrap(), None);
        assert_eq!(canonicalize_key(b"1.5").unwrap(), Some(b"1.5".to_vec()));
        assert!(canonicalize_key(b"{oops}").is_err());
    }
}
