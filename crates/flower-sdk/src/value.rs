//! Canonical values: the [`Value`] model, validation, canonical JSON and the
//! tagged binary codec used across the guest/host ABI.
//!
//! Data validity: finite numbers, well-formed strings, unique object keys and
//! bounded depth. [`Value::validate`] enforces this iteratively; every entry
//! point below validates before recursing, so recursion always runs under
//! [`MAX_VALUE_DEPTH`]. Binary decoding runs iteratively with an explicit
//! stack and never pre-allocates from untrusted lengths.
//!
//! Canonical JSON is byte-deterministic: object keys sort byte-lexicographically
//! ([`BTreeMap`] order), strings use minimal JSON escaping with UTF-8 passed
//! through, and numbers render with Rust `Display` semantics (shortest
//! round-trip decimal expansion, no exponent notation). Hosts must treat
//! guest-rendered key bytes as opaque or replicate `Display` exactly; native
//! canonicalization with `ryu` differs on extreme magnitudes.
//!
//! Binary encoding (normative): one tag byte, then the payload. Integers are
//! little-endian; lengths are `u32` element or byte counts.
//!
//! | Tag | Payload |
//! |---|---|
//! | `0x00` | null: none |
//! | `0x01` | false: none |
//! | `0x02` | true: none |
//! | `0x03` | number: 8 bytes IEEE-754, must be finite |
//! | `0x04` | string: `u32` length plus UTF-8 bytes |
//! | `0x05` | array: `u32` count plus items |
//! | `0x06` | object: `u32` count plus entries; each entry is a `u32` key length, key bytes, then a value |
//!
//! Tags `0x07` and above are reserved and rejected. Object keys must be unique.
//! Decoding rejects truncated input, trailing bytes, invalid UTF-8,
//! non-finite numbers and nesting beyond [`MAX_VALUE_DEPTH`].

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::error::{Result, codes, fail};

/// Maximum nesting depth of a value: containers nested more than 128 deep are
/// rejected. Depth counts enclosing containers, so a scalar has depth 0.
pub const MAX_VALUE_DEPTH: usize = 128;

/// Canonical data value: JSON-compatible, host-validated, bounded.
#[derive(Clone, Debug, PartialEq, Default)]
pub enum Value {
    /// Explicit null.
    #[default]
    Null,
    /// Boolean.
    Bool(bool),
    /// Finite 64-bit float. Non-finite payloads fail [`Value::validate`].
    Number(f64),
    /// Well-formed string.
    String(String),
    /// Ordered items.
    Array(Vec<Value>),
    /// Unique keys in byte-lexicographic order.
    Object(BTreeMap<String, Value>),
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::Null
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(String::from(value))
    }
}

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Value::Array(value)
    }
}

impl From<BTreeMap<String, Value>> for Value {
    fn from(value: BTreeMap<String, Value>) -> Self {
        Value::Object(value)
    }
}

impl Value {
    /// True for [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Borrow as a boolean without validation.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// Borrow as a float without validation.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number(value) => Some(*value),
            _ => None,
        }
    }

    /// Borrow as a string without validation.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(value) => Some(value),
            _ => None,
        }
    }

    /// Borrow as an array without validation.
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(value) => Some(value),
            _ => None,
        }
    }

    /// Borrow as an object without validation.
    pub fn as_object(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Object(value) => Some(value),
            _ => None,
        }
    }

    /// Check finiteness and depth iteratively. Strings are always well-formed
    /// and [`BTreeMap`] keys always unique by construction.
    pub fn validate(&self) -> Result<()> {
        let mut stack: Vec<(&Value, usize)> = Vec::new();
        stack.push((self, 0));
        while let Some((node, enclosing)) = stack.pop() {
            if enclosing > MAX_VALUE_DEPTH {
                return Err(fail(codes::DEPTH_LIMIT, "value exceeds maximum depth"));
            }
            match node {
                Value::Null | Value::Bool(_) | Value::String(_) => {}
                Value::Number(value) => {
                    if !value.is_finite() {
                        return Err(fail(codes::INVALID_VALUE, "non-finite number"));
                    }
                }
                Value::Array(items) => {
                    for item in items {
                        stack.push((item, enclosing + 1));
                    }
                }
                Value::Object(map) => {
                    for value in map.values() {
                        stack.push((value, enclosing + 1));
                    }
                }
            }
        }
        Ok(())
    }

    /// Render canonical JSON. See the module documentation for the exact rules.
    pub fn canonical_json(&self) -> Result<String> {
        self.validate()?;
        let mut out = String::new();
        render_into(&mut out, self);
        Ok(out)
    }

    /// Encode the tagged binary form.
    pub fn encode_binary(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut out = Vec::new();
        encode_into(&mut out, self)?;
        Ok(out)
    }

    /// Decode the tagged binary form, rejecting hostile input without
    /// recursion or untrusted pre-allocation.
    pub fn decode_binary(bytes: &[u8]) -> Result<Value> {
        let mut cursor = Cursor {
            input: bytes,
            pos: 0,
        };
        let mut stack: Vec<Frame> = Vec::new();
        loop {
            // Object keys are length-prefixed bytes, not tagged values.
            let need_key = matches!(stack.last(), Some(Frame::Object { key: None, .. }));
            if need_key {
                let key = cursor.read_string()?;
                match stack.last_mut() {
                    Some(Frame::Object { map, key: slot, .. }) => {
                        if map.contains_key(&key) {
                            return Err(fail(codes::INVALID_VALUE, "duplicate object key"));
                        }
                        *slot = Some(key);
                    }
                    _ => unreachable!("object frame guarantees a key slot"),
                }
                continue;
            }
            let tag = cursor.read_byte()?;
            if stack.len() > MAX_VALUE_DEPTH {
                return Err(fail(codes::DEPTH_LIMIT, "value exceeds maximum depth"));
            }
            let mut value: Value = match tag {
                0x00 => Value::Null,
                0x01 => Value::Bool(false),
                0x02 => Value::Bool(true),
                0x03 => Value::Number(cursor.read_f64()?),
                0x04 => Value::String(cursor.read_string()?),
                0x05 => {
                    let count = cursor.read_u32()?;
                    if count == 0 {
                        Value::Array(Vec::new())
                    } else {
                        stack.push(Frame::Array {
                            remaining: count,
                            items: Vec::new(),
                        });
                        continue;
                    }
                }
                0x06 => {
                    let count = cursor.read_u32()?;
                    if count == 0 {
                        Value::Object(BTreeMap::new())
                    } else {
                        stack.push(Frame::Object {
                            remaining: count,
                            map: BTreeMap::new(),
                            key: None,
                        });
                        continue;
                    }
                }
                _ => return Err(fail(codes::INVALID_VALUE, "unknown value tag")),
            };
            // Attach the completed value up through finished frames.
            loop {
                let frame_complete = match stack.last_mut() {
                    None => {
                        if cursor.pos != bytes.len() {
                            return Err(fail(codes::INVALID_VALUE, "trailing bytes"));
                        }
                        return Ok(value);
                    }
                    Some(Frame::Array { remaining, items }) => {
                        items.push(value);
                        *remaining -= 1;
                        *remaining == 0
                    }
                    Some(Frame::Object {
                        remaining,
                        map,
                        key,
                    }) => {
                        match key.take() {
                            Some(k) => {
                                map.insert(k, value);
                            }
                            None => unreachable!("object values always follow their key"),
                        }
                        *remaining -= 1;
                        *remaining == 0
                    }
                };
                if !frame_complete {
                    break;
                }
                match stack.pop() {
                    Some(Frame::Array { items, .. }) => {
                        value = Value::Array(items);
                    }
                    Some(Frame::Object { map, .. }) => {
                        value = Value::Object(map);
                    }
                    None => unreachable!("frame was just observed"),
                }
            }
        }
    }
}

/// Minimal JSON string escaping into `out`: `"`, `\` and C0 controls escape;
/// everything else, including `DEL` and non-ASCII text, passes through.
pub(crate) fn escape_str_into(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let byte = c as u8;
                out.push_str("\\u00");
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0F) as usize] as char);
            }
            c => out.push(c),
        }
    }
}

fn render_into(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(value) => out.push_str(&value.to_string()),
        Value::String(value) => {
            out.push('"');
            escape_str_into(out, value);
            out.push('"');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                render_into(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (index, (key, value)) in map.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push('"');
                escape_str_into(out, key);
                out.push('"');
                out.push(':');
                render_into(out, value);
            }
            out.push('}');
        }
    }
}

fn encode_into(out: &mut Vec<u8>, value: &Value) -> Result<()> {
    match value {
        Value::Null => out.push(0x00),
        Value::Bool(false) => out.push(0x01),
        Value::Bool(true) => out.push(0x02),
        Value::Number(value) => {
            out.push(0x03);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::String(value) => {
            out.push(0x04);
            push_len(out, value.len())?;
            out.extend_from_slice(value.as_bytes());
        }
        Value::Array(items) => {
            out.push(0x05);
            push_len(out, items.len())?;
            for item in items {
                encode_into(out, item)?;
            }
        }
        Value::Object(map) => {
            out.push(0x06);
            push_len(out, map.len())?;
            for (key, value) in map {
                push_len(out, key.len())?;
                out.extend_from_slice(key.as_bytes());
                encode_into(out, value)?;
            }
        }
    }
    Ok(())
}

fn push_len(out: &mut Vec<u8>, len: usize) -> Result<()> {
    if len > u32::MAX as usize {
        return Err(fail(codes::INVALID_VALUE, "value too large to encode"));
    }
    out.extend_from_slice(&(len as u32).to_le_bytes());
    Ok(())
}

enum Frame {
    Array {
        remaining: u32,
        items: Vec<Value>,
    },
    Object {
        remaining: u32,
        map: BTreeMap<String, Value>,
        key: Option<String>,
    },
}

struct Cursor<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn read_byte(&mut self) -> Result<u8> {
        if self.pos >= self.input.len() {
            return Err(fail(codes::INVALID_VALUE, "truncated value encoding"));
        }
        let value = self.input[self.pos];
        self.pos += 1;
        Ok(value)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        // Copy the shared input reference out so the returned slice keeps the
        // input lifetime instead of the `&mut self` borrow.
        let input: &'a [u8] = self.input;
        let pos = self.pos;
        let end = match pos.checked_add(len) {
            Some(end) if end <= input.len() => end,
            _ => return Err(fail(codes::INVALID_VALUE, "truncated value encoding")),
        };
        self.pos = end;
        Ok(&input[pos..end])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_f64(&mut self) -> Result<f64> {
        let bytes = self.read_bytes(8)?;
        let mut raw = [0u8; 8];
        raw.copy_from_slice(bytes);
        let value = f64::from_le_bytes(raw);
        if !value.is_finite() {
            return Err(fail(codes::INVALID_VALUE, "non-finite number"));
        }
        Ok(value)
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        match core::str::from_utf8(bytes) {
            Ok(text) => Ok(String::from(text)),
            Err(_) => Err(fail(codes::INVALID_VALUE, "invalid string encoding")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::codes;
    use alloc::collections::BTreeMap;
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;

    fn deep_value(depth: usize) -> Value {
        let mut value = Value::Null;
        for _ in 0..depth {
            value = Value::Array(vec![value]);
        }
        value
    }

    #[test]
    fn canonical_scalars() {
        assert_eq!(Value::Null.canonical_json().unwrap(), "null");
        assert_eq!(Value::Bool(true).canonical_json().unwrap(), "true");
        assert_eq!(Value::Bool(false).canonical_json().unwrap(), "false");
        assert_eq!(Value::Number(42.0).canonical_json().unwrap(), "42");
        assert_eq!(Value::Number(0.5).canonical_json().unwrap(), "0.5");
        assert_eq!(Value::Number(-0.0).canonical_json().unwrap(), "-0");
        assert_eq!(
            Value::String(String::from("")).canonical_json().unwrap(),
            "\"\""
        );
    }

    #[test]
    fn canonical_escapes_and_passthrough() {
        let rendered = Value::String(String::from("\"\n\té𝄞\u{7f}\u{01}"))
            .canonical_json()
            .unwrap();
        assert_eq!(rendered, "\"\\\"\\n\\té𝄞\u{7f}\\u0001\"");
    }

    #[test]
    fn canonical_sorts_keys_and_nests() {
        let mut map = BTreeMap::new();
        map.insert(
            String::from("b"),
            Value::Array(vec![Value::Number(1.0), Value::Bool(true), Value::Null]),
        );
        map.insert(String::from("a"), Value::Number(2.0));
        let rendered = Value::Object(map).canonical_json().unwrap();
        assert_eq!(rendered, "{\"a\":2,\"b\":[1,true,null]}");
    }

    #[test]
    fn validate_rejects_non_finite_and_deep() {
        assert!(Value::Number(f64::NAN).validate().is_err());
        assert!(Value::Number(f64::INFINITY).validate().is_err());
        assert!(deep_value(128).validate().is_ok());
        let failure = deep_value(129).validate().unwrap_err();
        assert_eq!(failure.code, codes::DEPTH_LIMIT);
    }

    #[test]
    fn binary_round_trip() {
        let mut map = BTreeMap::new();
        map.insert(String::from("n"), Value::Number(-12.5));
        map.insert(String::from("s"), Value::String(String::from("x\"y")));
        let value = Value::Array(vec![Value::Null, Value::Bool(true), Value::Object(map)]);
        let bytes = value.encode_binary().unwrap();
        let decoded = Value::decode_binary(&bytes).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(
            decoded.canonical_json().unwrap(),
            "[null,true,{\"n\":-12.5,\"s\":\"x\\\"y\"}]"
        );
    }

    #[test]
    fn binary_rejects_hostile_input() {
        assert!(Value::decode_binary(&[]).is_err());
        assert!(Value::decode_binary(&[0x07]).is_err());
        // Truncated string.
        assert!(Value::decode_binary(&[0x04, 0x03, 0x00, 0x00, 0x00, b'a']).is_err());
        // Trailing bytes.
        assert!(Value::decode_binary(&[0x00, 0x00]).is_err());
        // Duplicate object key.
        let mut dup = vec![0x06, 0x02, 0x00, 0x00, 0x00];
        dup.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, b'k', 0x00]);
        dup.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, b'k', 0x01]);
        assert!(Value::decode_binary(&dup).is_err());
        // Non-finite number bits.
        let mut nan = vec![0x03];
        nan.extend_from_slice(&f64::NAN.to_le_bytes());
        assert!(Value::decode_binary(&nan).is_err());
        // Invalid UTF-8 string bytes.
        assert!(Value::decode_binary(&[0x04, 0x02, 0x00, 0x00, 0x00, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn binary_rejects_depth_bomb_without_crashing() {
        // 100,000 nested single-item arrays: only an iterative decoder survives.
        let mut bytes = Vec::new();
        for _ in 0..100_000 {
            bytes.extend_from_slice(&[0x05, 0x01, 0x00, 0x00, 0x00]);
        }
        bytes.push(0x00);
        let failure = Value::decode_binary(&bytes).unwrap_err();
        assert_eq!(failure.code, codes::DEPTH_LIMIT);
    }
}
