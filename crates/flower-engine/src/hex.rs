//! Lowercase hex: the one text codec for binary fields, cursors, queue
//! IDs and claim tokens. Decoding accepts only the canonical lowercase
//! form, so every binary value has exactly one spelling.

const DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Encode bytes as lowercase hex.
pub fn encode(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[(byte >> 4) as usize] as char);
        text.push(DIGITS[(byte & 0x0F) as usize] as char);
    }
    text
}

/// Decode lowercase hex, or `None` for odd lengths and any other digit.
pub fn decode(text: &str) -> Option<Vec<u8>> {
    let digits = text.as_bytes();
    if !digits.len().is_multiple_of(2) {
        return None;
    }
    let nibble = |digit: u8| match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    };
    let (pairs, _) = digits.as_chunks::<2>();
    pairs
        .iter()
        .map(|[high, low]| Some(nibble(*high)? << 4 | nibble(*low)?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips_lowercase_only() {
        let bytes = vec![0x00, 0xAB, 0xFF, 0x10];
        assert_eq!(encode(&bytes), "00abff10");
        assert_eq!(decode("00abff10").unwrap(), bytes);
        assert_eq!(decode("").unwrap(), Vec::<u8>::new());
        assert!(decode("abc").is_none());
        assert!(decode("zz").is_none());
        assert!(decode("AB").is_none());
    }
}
