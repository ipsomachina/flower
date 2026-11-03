//! Retained row versions.
//!
//! The core store keeps current values only; history comes from the
//! replication layer's retained log tail. [`HistoryProvider`] is the seam:
//! the core serves [`EmptyHistory`], and local-only databases retain
//! nothing.

use flower_sdk::Value;

/// One retained version: revision plus value, or `None` for a deletion
/// tombstone.
#[derive(Clone, Debug, PartialEq)]
pub struct Version {
    /// Revision that wrote the version.
    pub revision: u64,
    /// Canonical JSON bytes, or `None` for a deletion.
    pub value: Option<Vec<u8>>,
}

/// Serves retained row versions newest-first as retained. Provider
/// failures are engine errors: corrupt retained bytes fail the invocation
/// rather than surfacing as guest-catchable failures. Providers are
/// `Sync`: concurrent queries share one.
pub trait HistoryProvider: Send + Sync {
    /// List up to `limit` retained versions of one row.
    fn history(
        &self,
        db: &[u8],
        collection: u32,
        key: &[u8],
        limit: u32,
    ) -> crate::error::Result<Vec<Version>>;
}

/// No retained history: every row reports no versions.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyHistory;

impl HistoryProvider for EmptyHistory {
    fn history(
        &self,
        _db: &[u8],
        _collection: u32,
        _key: &[u8],
        _limit: u32,
    ) -> crate::error::Result<Vec<Version>> {
        Ok(Vec::new())
    }
}

/// Encode versions as the history response: `[{revision, value}]` with
/// values decoded and deletions as null.
pub fn encode_versions(versions: &[Version]) -> crate::error::Result<Value> {
    let mut entries = Vec::with_capacity(versions.len());
    for version in versions {
        let value = match &version.value {
            None => Value::Null,
            Some(bytes) => crate::json::parse_value(bytes)?,
        };
        let mut map = std::collections::BTreeMap::new();
        map.insert(
            "revision".to_string(),
            Value::Number(version.revision as f64),
        );
        map.insert("value".to_string(), value);
        entries.push(Value::Object(map));
    }
    Ok(Value::Array(entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_history_reports_nothing() {
        let versions = EmptyHistory.history(b"db", 1, b"k", 10).unwrap();
        assert!(versions.is_empty());
        assert_eq!(
            encode_versions(&versions).unwrap(),
            Value::Array(Vec::new())
        );
    }

    #[test]
    fn versions_encode_newest_first() {
        let versions = vec![
            Version {
                revision: 9,
                value: Some(b"{}".to_vec()),
            },
            Version {
                revision: 7,
                value: None,
            },
        ];
        let encoded = encode_versions(&versions).unwrap();
        match encoded {
            Value::Array(entries) if entries.len() == 2 => {
                match &entries[0] {
                    Value::Object(map) => {
                        assert_eq!(map["revision"], Value::Number(9.0));
                        assert_eq!(map["value"], Value::Object(Default::default()));
                    }
                    _ => panic!("entry is not an object"),
                }
                match &entries[1] {
                    Value::Object(map) => {
                        assert_eq!(map["revision"], Value::Number(7.0));
                        assert_eq!(map["value"], Value::Null);
                    }
                    _ => panic!("entry is not an object"),
                }
            }
            _ => panic!("versions are not an array"),
        }
    }
}
