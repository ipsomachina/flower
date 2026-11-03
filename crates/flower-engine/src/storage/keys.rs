//! Storage key codecs.
//!
//! Every key begins with the length-prefixed database identifier, so a
//! logical database is a key range. All lengths are `u32` big-endian, all
//! integers are big-endian fixed width, and every variable component is
//! length-prefixed — including trailing ones — so keys parse front to back
//! without external framing. Fixed-width components (32-bit IDs, 32-byte
//! hashes) are stored raw.
//!
//! Index keys need one refinement: the index-key run is a sequence of
//! length-prefixed components followed by the length-prefixed primary key.
//! The component count comes from the index descriptor, so entries decode
//! with known arity and component prefixes stay byte prefixes.

use std::cmp::Ordering;

use crate::error::{Error, Result};

/// The root database identifier: empty, and reserved.
pub const ROOT_DB: &[u8] = b"";

/// Dependency-kind tags.
pub const DEP_RECORD: u8 = 0x00;
/// Index-range dependency tag.
pub const DEP_RANGE: u8 = 0x01;
/// Derived-instance dependency tag; doubles as the dependent-key tag.
pub const DEP_DERIVED: u8 = 0x02;
/// Collection-range dependency tag: scans over the primary keyspace.
pub const DEP_COLLECTION_RANGE: u8 = 0x03;

/// Length of a SHA-256 content hash in code keys.
pub const HASH_LEN: usize = 32;

fn push_len(out: &mut Vec<u8>, len: usize) -> Result<()> {
    let len = u32::try_from(len).map_err(|_| Error::corrupt("component too long"))?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

pub(crate) fn push_sized(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    push_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

/// Big-endian `u32` for hand-rolled encodings.
pub(crate) fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Big-endian `u64` for hand-rolled encodings.
pub(crate) fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// A `usize` count as big-endian `u32`, rejecting overflow.
pub(crate) fn push_count(out: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| Error::corrupt("count too long".to_string()))?;
    push_u32(out, value);
    Ok(())
}

/// Length-prefixed database identifier: the range prefix of one database.
pub fn encode_db(db: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(db.len() + 4);
    push_sized(&mut out, db)?;
    Ok(out)
}

/// `meta`: database, well-known name.
pub fn encode_meta_key(db: &[u8], name: &[u8]) -> Result<Vec<u8>> {
    let mut key = encode_db(db)?;
    push_sized(&mut key, name)?;
    Ok(key)
}

/// `intern_id`: database, intern ID.
pub fn encode_intern_id_key(db: &[u8], id: u32) -> Result<Vec<u8>> {
    let mut key = encode_db(db)?;
    key.extend_from_slice(&id.to_be_bytes());
    Ok(key)
}

/// `intern_name`: database, name bytes.
pub fn encode_intern_name_key(db: &[u8], name: &[u8]) -> Result<Vec<u8>> {
    let mut key = encode_db(db)?;
    push_sized(&mut key, name)?;
    Ok(key)
}

/// `records`: database, collection ID, key bytes.
pub fn encode_record_key(db: &[u8], collection: u32, key: &[u8]) -> Result<Vec<u8>> {
    let mut full = record_prefix(db, collection)?;
    push_sized(&mut full, key)?;
    Ok(full)
}

/// Prefix of every record key of one collection: database plus collection ID.
pub fn record_prefix(db: &[u8], collection: u32) -> Result<Vec<u8>> {
    let mut prefix = encode_db(db)?;
    prefix.extend_from_slice(&collection.to_be_bytes());
    Ok(prefix)
}

/// Full-key prefix of every record of one collection whose key bytes
/// are exactly `key_len` long and start with `key_prefix`. Record keys
/// are length-prefixed, so one key length is one contiguous range and
/// fixed-width keys iterate in plain byte order.
pub fn record_key_class(
    db: &[u8],
    collection: u32,
    key_len: usize,
    key_prefix: &[u8],
) -> Result<Vec<u8>> {
    let mut start = record_prefix(db, collection)?;
    push_len(&mut start, key_len)?;
    start.extend_from_slice(key_prefix);
    Ok(start)
}

/// Decode the trailing key bytes of a record key below `prefix_len`.
pub fn decode_record_suffix(full: &[u8], prefix_len: usize) -> Result<Vec<u8>> {
    let mut reader = Reader::new(full);
    reader.skip(prefix_len)?;
    reader.sized()
}

/// `index_entries`: database, index ID, length-prefixed index components,
/// length-prefixed primary key.
pub fn encode_index_key(
    db: &[u8],
    index: u32,
    components: &[&[u8]],
    primary: &[u8],
) -> Result<Vec<u8>> {
    let mut full = index_prefix(db, index, components)?;
    push_sized(&mut full, primary)?;
    Ok(full)
}

/// Byte prefix of the leading index components: database, index ID, then
/// the length-prefixed components.
pub fn index_prefix(db: &[u8], index: u32, components: &[&[u8]]) -> Result<Vec<u8>> {
    let mut prefix = encode_db(db)?;
    prefix.extend_from_slice(&index.to_be_bytes());
    for component in components {
        push_sized(&mut prefix, component)?;
    }
    Ok(prefix)
}

/// Decode an index entry below `prefix_len` with known `arity`: the index
/// components plus the primary key.
pub fn decode_index_entry(
    full: &[u8],
    prefix_len: usize,
    arity: usize,
) -> Result<(Vec<Vec<u8>>, Vec<u8>)> {
    let mut reader = Reader::new(full);
    reader.skip(prefix_len)?;
    let mut components = Vec::with_capacity(arity);
    for _ in 0..arity {
        components.push(reader.sized()?);
    }
    let primary = reader.sized()?;
    reader.end()?;
    Ok((components, primary))
}

/// `derived` and dependent keys: database, definition ID, argument bytes.
pub fn encode_derived_key(db: &[u8], definition: u32, args: &[u8]) -> Result<Vec<u8>> {
    let mut key = encode_db(db)?;
    key.push(DEP_DERIVED);
    key.extend_from_slice(&definition.to_be_bytes());
    push_sized(&mut key, args)?;
    Ok(key)
}

/// Split a full `derived` key back into definition ID and argument
/// bytes, rejecting keys of another database and trailing bytes.
pub fn decode_derived_key(db: &[u8], key: &[u8]) -> Result<(u32, Vec<u8>)> {
    let prefix = encode_db(db)?;
    let body = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| Error::corrupt("derived key outside database"))?;
    decode_dependent(body)
}

/// Record dependency: tag, collection ID, key bytes.
pub fn encode_record_dep(collection: u32, key: &[u8]) -> Result<Vec<u8>> {
    let mut dep = Vec::with_capacity(key.len() + 9);
    dep.push(DEP_RECORD);
    dep.extend_from_slice(&collection.to_be_bytes());
    push_sized(&mut dep, key)?;
    Ok(dep)
}

/// Index-range dependency components: equality prefix plus optional bounds.
/// Cursors, limits and direction do not affect the dependency set.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RangeDep {
    /// Leading equality components as canonical bytes.
    pub prefix: Vec<Vec<u8>>,
    /// Exclusive lower bound, if any.
    pub gt: Option<Vec<u8>>,
    /// Inclusive lower bound, if any.
    pub gte: Option<Vec<u8>>,
    /// Exclusive upper bound, if any.
    pub lt: Option<Vec<u8>>,
    /// Inclusive upper bound, if any.
    pub lte: Option<Vec<u8>>,
}

/// Index-range dependency: tag, index ID, prefix count, length-prefixed
/// prefix components, then four flag-guarded optional bounds.
pub fn encode_range_dep(index: u32, range: &RangeDep) -> Result<Vec<u8>> {
    encode_ranged_dep(DEP_RANGE, index, range)
}

/// Collection-range dependency: same shape over the primary keyspace,
/// recording collection scans that index ranges cannot express.
pub fn encode_collection_range_dep(collection: u32, range: &RangeDep) -> Result<Vec<u8>> {
    encode_ranged_dep(DEP_COLLECTION_RANGE, collection, range)
}

fn encode_ranged_dep(tag: u8, id: u32, range: &RangeDep) -> Result<Vec<u8>> {
    let mut dep = Vec::new();
    dep.push(tag);
    dep.extend_from_slice(&id.to_be_bytes());
    push_len(&mut dep, range.prefix.len())?;
    for component in &range.prefix {
        push_sized(&mut dep, component)?;
    }
    for bound in [&range.gt, &range.gte, &range.lt, &range.lte] {
        match bound {
            None => dep.push(0x00),
            Some(bytes) => {
                dep.push(0x01);
                push_sized(&mut dep, bytes)?;
            }
        }
    }
    Ok(dep)
}

/// Derived-instance dependency: tag, definition ID, argument bytes.
pub fn encode_derived_dep(definition: u32, args: &[u8]) -> Result<Vec<u8>> {
    let mut dep = Vec::with_capacity(args.len() + 9);
    dep.push(DEP_DERIVED);
    dep.extend_from_slice(&definition.to_be_bytes());
    push_sized(&mut dep, args)?;
    Ok(dep)
}

/// `dep_edges`: database, dependency key, dependent key.
pub fn encode_dep_edge(db: &[u8], dependency: &[u8], dependent: &[u8]) -> Result<Vec<u8>> {
    let mut key = encode_db(db)?;
    key.extend_from_slice(dependency);
    key.extend_from_slice(dependent);
    Ok(key)
}

/// A decoded dependency key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DepKey {
    /// Record read, present or absent.
    Record {
        /// Interned collection ID.
        collection: u32,
        /// Record key bytes.
        key: Vec<u8>,
    },
    /// Index-range read.
    Range {
        /// Interned index ID.
        index: u32,
        /// Prefix and bounds.
        range: RangeDep,
    },
    /// Collection-range read (a scan over the primary keyspace).
    CollectionRange {
        /// Interned collection ID.
        collection: u32,
        /// Prefix and bounds over record key bytes.
        range: RangeDep,
    },
    /// Derived-instance read.
    Derived {
        /// Interned definition ID.
        definition: u32,
        /// Canonical argument bytes.
        args: Vec<u8>,
    },
}

/// Decode one dependency key, returning the key and bytes consumed.
pub fn decode_dep_key(bytes: &[u8]) -> Result<(DepKey, usize)> {
    let mut reader = Reader::new(bytes);
    let tag = reader.byte()?;
    let key = match tag {
        DEP_RECORD => {
            let collection = reader.u32()?;
            let key = reader.sized()?;
            DepKey::Record { collection, key }
        }
        DEP_RANGE | DEP_COLLECTION_RANGE => {
            let id = reader.u32()?;
            let count = reader.u32()? as usize;
            // Every component costs at least its four length bytes, so
            // a stored count never reserves more than the bytes left.
            let mut prefix = Vec::with_capacity(count.min(reader.remaining() / 4));
            for _ in 0..count {
                prefix.push(reader.sized()?);
            }
            let mut bounds = Vec::with_capacity(4);
            for _ in 0..4 {
                match reader.byte()? {
                    0x00 => bounds.push(None),
                    0x01 => bounds.push(Some(reader.sized()?)),
                    _ => return Err(Error::corrupt("bad bound flag")),
                }
            }
            let mut bounds = bounds.into_iter();
            let range = RangeDep {
                prefix,
                gt: bounds.next().unwrap_or(None),
                gte: bounds.next().unwrap_or(None),
                lt: bounds.next().unwrap_or(None),
                lte: bounds.next().unwrap_or(None),
            };
            if tag == DEP_RANGE {
                DepKey::Range { index: id, range }
            } else {
                DepKey::CollectionRange {
                    collection: id,
                    range,
                }
            }
        }
        DEP_DERIVED => {
            let definition = reader.u32()?;
            let args = reader.sized()?;
            DepKey::Derived { definition, args }
        }
        _ => return Err(Error::corrupt("unknown dependency tag")),
    };
    Ok((key, reader.pos()))
}

/// Exclusive upper bound of a byte prefix: strip trailing `0xFF`, bump
/// the last byte. `None` when every key with the prefix sorts below any
/// bound (all-`0xFF` prefix), meaning unbounded above.
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while end.last() == Some(&0xFF) {
        end.pop();
    }
    let last = end.pop()?;
    end.push(last + 1);
    Some(end)
}

/// Whether index components plus primary fall inside a range dependency:
/// the leading prefix must match exactly, then the first component past
/// the prefix (or the primary key when the prefix covers the arity)
/// must satisfy every bound. Bounds compare in stored component order
/// ([`component_cmp`]: length, then bytes), which is the index's
/// iteration order, so a bounded range is one contiguous key range.
pub fn index_range_matches(range: &RangeDep, components: &[Vec<u8>], primary: &[u8]) -> bool {
    if components.len() < range.prefix.len() {
        return false;
    }
    if components[..range.prefix.len()] != range.prefix[..] {
        return false;
    }
    let subject = components
        .get(range.prefix.len())
        .map(Vec::as_slice)
        .unwrap_or(primary);
    bounds_match(range, subject, component_cmp)
}

/// Stored order of two length-prefixed components: length, then bytes.
pub fn component_cmp(left: &[u8], right: &[u8]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

/// Whether a full encoded record key falls inside a collection-range
/// dependency: optional single byte-prefix plus bounds on the whole key.
/// Full keys compare as plain bytes, which is their storage order.
pub fn collection_range_matches(range: &RangeDep, key: &[u8]) -> bool {
    if let Some(prefix) = range.prefix.first()
        && !key.starts_with(prefix)
    {
        return false;
    }
    bounds_match(range, key, <[u8]>::cmp)
}

fn bounds_match(range: &RangeDep, subject: &[u8], cmp: impl Fn(&[u8], &[u8]) -> Ordering) -> bool {
    let holds = |bound: &Option<Vec<u8>>, accept: fn(Ordering) -> bool| {
        bound
            .as_deref()
            .is_none_or(|bound| accept(cmp(subject, bound)))
    };
    holds(&range.gt, Ordering::is_gt)
        && holds(&range.gte, Ordering::is_ge)
        && holds(&range.lt, Ordering::is_lt)
        && holds(&range.lte, Ordering::is_le)
}

/// Storage scan bounds for an index range below `scope`, the encoded
/// index prefix of its equality components: an inclusive lower and an
/// exclusive upper full key, or `None` when the bounds admit nothing.
/// Bounds compare in stored component order, so an entry satisfies the
/// bounds exactly when its full key lies inside the returned range.
pub fn index_scan_bounds(scope: &[u8], range: &RangeDep) -> Result<ScanBounds> {
    let at = |bound: &[u8]| -> Result<Vec<u8>> {
        let mut key = scope.to_vec();
        push_sized(&mut key, bound)?;
        Ok(key)
    };
    // The first key past every entry whose subject equals the bound; an
    // end outside the scope means nothing in the scope sorts above it.
    let past = |bound: &[u8]| -> Result<Option<Vec<u8>>> {
        Ok(prefix_end(&at(bound)?).filter(|end| end.starts_with(scope)))
    };
    let mut lower: Option<Vec<u8>> = None;
    let mut upper: Option<Vec<u8>> = None;
    let mut raise = |candidate: Vec<u8>| {
        if lower.as_ref().is_none_or(|current| candidate > *current) {
            lower = Some(candidate);
        }
    };
    if let Some(gte) = &range.gte {
        raise(at(gte)?);
    }
    if let Some(gt) = &range.gt {
        match past(gt)? {
            Some(start) => raise(start),
            None => return Ok(None),
        }
    }
    let mut cap = |candidate: Vec<u8>| {
        if upper.as_ref().is_none_or(|current| candidate < *current) {
            upper = Some(candidate);
        }
    };
    if let Some(lt) = &range.lt {
        cap(at(lt)?);
    }
    if let Some(lte) = &range.lte
        && let Some(end) = past(lte)?
    {
        cap(end);
    }
    if let (Some(lower), Some(upper)) = (&lower, &upper)
        && lower >= upper
    {
        return Ok(None);
    }
    Ok(Some((lower, upper)))
}

/// Inclusive lower and exclusive upper full-key bounds of a scan, or
/// `None` for a provably empty range.
pub type ScanBounds = Option<(Option<Vec<u8>>, Option<Vec<u8>>)>;

/// Decode a dependent key: definition ID plus argument bytes.
pub fn decode_dependent(bytes: &[u8]) -> Result<(u32, Vec<u8>)> {
    let (key, consumed) = decode_dep_key(bytes)?;
    if consumed != bytes.len() {
        return Err(Error::corrupt("trailing dependent bytes"));
    }
    match key {
        DepKey::Derived { definition, args } => Ok((definition, args)),
        _ => Err(Error::corrupt("dependent is not a derived instance")),
    }
}

/// `receipts`: database, retry identity bytes.
pub fn encode_receipt_key(db: &[u8], identity: &[u8]) -> Result<Vec<u8>> {
    let mut key = encode_db(db)?;
    push_sized(&mut key, identity)?;
    Ok(key)
}

/// `code`: database, 32-byte artifact content hash.
pub fn encode_code_key(db: &[u8], hash: &[u8; HASH_LEN]) -> Result<Vec<u8>> {
    let mut key = encode_db(db)?;
    key.extend_from_slice(hash);
    Ok(key)
}

/// Front-to-back binary reader over stored keys and values.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Reader { bytes, pos: 0 }
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    /// Bytes left to read.
    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| Error::corrupt("truncated encoding"))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    pub(crate) fn skip(&mut self, len: usize) -> Result<()> {
        self.take(len)?;
        Ok(())
    }

    pub(crate) fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        let raw = self.take(4)?;
        Ok(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        let raw = self.take(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(raw);
        Ok(u64::from_be_bytes(bytes))
    }

    pub(crate) fn sized(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    pub(crate) fn bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        Ok(self.take(len)?.to_vec())
    }

    fn end(&self) -> Result<()> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::corrupt("trailing key bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn databases_are_key_ranges() {
        let db = encode_db(b"tenant").unwrap();
        let other = encode_db(b"tenant2").unwrap();
        let record = encode_record_key(b"tenant", 7, b"k").unwrap();
        assert!(record.starts_with(&db));
        assert!(!record.starts_with(&other));
        assert_eq!(encode_db(ROOT_DB).unwrap(), vec![0, 0, 0, 0]);
    }

    #[test]
    fn record_keys_round_trip() {
        let full = encode_record_key(b"db", 9, b"key").unwrap();
        let prefix = record_prefix(b"db", 9).unwrap();
        assert!(full.starts_with(&prefix));
        assert_eq!(
            decode_record_suffix(&full, prefix.len()).unwrap(),
            b"key".to_vec()
        );
        assert!(decode_record_suffix(&full, full.len() + 1).is_err());
        assert!(decode_record_suffix(&full[..full.len() - 1], prefix.len()).is_err());
    }

    #[test]
    fn ids_order_lexicographically() {
        let low = encode_intern_id_key(b"db", 1).unwrap();
        let high = encode_intern_id_key(b"db", 256).unwrap();
        assert!(low < high);
    }

    #[test]
    fn index_entries_round_trip_with_arity() {
        let full = encode_index_key(b"db", 3, &[&b"a"[..], &b"bb"[..]], b"pk").unwrap();
        let prefix = index_prefix(b"db", 3, &[&b"a"[..], &b"bb"[..]]).unwrap();
        assert!(full.starts_with(&prefix));
        let (components, primary) =
            decode_index_entry(&full, encode_db(b"db").unwrap().len() + 4, 2).unwrap();
        assert_eq!(components, vec![b"a".to_vec(), b"bb".to_vec()]);
        assert_eq!(primary, b"pk".to_vec());
        // Component prefixes stay byte prefixes of the entry.
        let partial = index_prefix(b"db", 3, &[&b"a"[..]]).unwrap();
        assert!(full.starts_with(&partial));
        assert!(decode_index_entry(&full, 0, 3).is_err());
    }

    #[test]
    fn dependency_keys_round_trip() {
        let record = encode_record_dep(5, b"k").unwrap();
        let (key, consumed) = decode_dep_key(&record).unwrap();
        assert_eq!(consumed, record.len());
        assert_eq!(
            key,
            DepKey::Record {
                collection: 5,
                key: b"k".to_vec()
            }
        );

        let range = RangeDep {
            prefix: vec![b"p".to_vec()],
            gt: None,
            gte: Some(b"g".to_vec()),
            lt: None,
            lte: None,
        };
        let encoded = encode_range_dep(2, &range).unwrap();
        let (key, consumed) = decode_dep_key(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(key, DepKey::Range { index: 2, range });

        let derived = encode_derived_dep(4, b"args").unwrap();
        assert_eq!(decode_dependent(&derived).unwrap(), (4, b"args".to_vec()));
        assert!(decode_dependent(&record).is_err());
        assert!(decode_dep_key(b"\x09rest").is_err());
    }

    #[test]
    fn edge_keys_concatenate_whole_deps() {
        let dep = encode_record_dep(1, b"k").unwrap();
        let ent = encode_derived_dep(2, b"a").unwrap();
        let edge = encode_dep_edge(b"db", &dep, &ent).unwrap();
        let body = &edge[encode_db(b"db").unwrap().len()..];
        let (_, consumed) = decode_dep_key(body).unwrap();
        assert_eq!(
            decode_dependent(&body[consumed..]).unwrap(),
            (2, b"a".to_vec())
        );
    }

    #[test]
    fn prefix_ends_bound_scopes() {
        assert_eq!(prefix_end(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_end(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(b"\xff\xff"), None);
        assert_eq!(prefix_end(b""), None);
    }

    #[test]
    fn range_overlap_follows_bounds() {
        let range = RangeDep {
            prefix: vec![b"p".to_vec()],
            gt: None,
            gte: Some(b"g".to_vec()),
            lt: Some(b"zz".to_vec()),
            lte: None,
        };
        assert!(index_range_matches(
            &range,
            &[b"p".to_vec(), b"m".to_vec()],
            b"pk"
        ));
        assert!(!index_range_matches(
            &range,
            &[b"q".to_vec(), b"m".to_vec()],
            b"pk"
        ));
        assert!(!index_range_matches(
            &range,
            &[b"p".to_vec(), b"a".to_vec()],
            b"pk"
        ));
        // Bounds past the prefix cover fall back to the primary key.
        assert!(index_range_matches(&range, &[b"p".to_vec()], b"pk"));
        assert!(!index_range_matches(&range, &[b"p".to_vec()], b"0"));
        // Bounds compare in stored order: longer components sort later.
        assert!(!index_range_matches(&range, &[b"p".to_vec()], b"aaa"));
        // Bounds past the prefix cover fall back to the primary key.
        let full = RangeDep {
            prefix: vec![b"p".to_vec()],
            gt: Some(b"pk0".to_vec()),
            gte: None,
            lt: None,
            lte: None,
        };
        assert!(index_range_matches(&full, &[b"p".to_vec()], b"pk1"));
        assert!(!index_range_matches(&full, &[b"p".to_vec()], b"pk0"));
        // Collection ranges match full encoded keys.
        let scan = RangeDep {
            prefix: vec![],
            gt: Some(b"\x00\x00\x00\x01k".to_vec()),
            gte: None,
            lt: None,
            lte: None,
        };
        assert!(collection_range_matches(&scan, b"\x00\x00\x00\x01m"));
        assert!(!collection_range_matches(&scan, b"\x00\x00\x00\x01k"));
    }

    #[test]
    fn scan_bounds_cover_exactly_the_matching_entries() {
        let scope = index_prefix(b"db", 3, &[&b"p"[..]]).unwrap();
        let range = RangeDep {
            prefix: vec![b"p".to_vec()],
            gt: Some(b"9".to_vec()),
            gte: None,
            lt: None,
            lte: Some(b"100".to_vec()),
        };
        let (lower, upper) = index_scan_bounds(&scope, &range).unwrap().unwrap();
        let (lower, upper) = (lower.unwrap(), upper.unwrap());
        for subject in [&b"8"[..], b"9", b"10", b"99", b"100", b"101", b"1000"] {
            let full = encode_index_key(b"db", 3, &[&b"p"[..], subject], b"pk").unwrap();
            let inside = full >= lower && full < upper;
            let matches = index_range_matches(&range, &[b"p".to_vec(), subject.to_vec()], b"pk");
            assert_eq!(inside, matches, "{}", String::from_utf8_lossy(subject));
        }
        // Crossed bounds admit nothing.
        let empty = RangeDep {
            gte: Some(b"5".to_vec()),
            lt: Some(b"5".to_vec()),
            ..RangeDep::default()
        };
        assert_eq!(index_scan_bounds(&scope, &empty).unwrap(), None);
    }

    #[test]
    fn dep_keys_reject_huge_counts_without_reserving() {
        let mut bytes = vec![DEP_RANGE, 0, 0, 0, 1];
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_dep_key(&bytes).is_err());
    }

    #[test]
    fn derived_keys_split() {
        let key = encode_derived_key(b"db", 7, b"args").unwrap();
        assert_eq!(
            decode_derived_key(b"db", &key).unwrap(),
            (7, b"args".to_vec())
        );
        assert!(decode_derived_key(b"other", &key).is_err());
        assert!(decode_derived_key(b"db", b"short").is_err());
    }

    #[test]
    fn receipt_and_code_keys_shape() {
        let receipt = encode_receipt_key(b"db", b"id").unwrap();
        assert!(receipt.starts_with(&encode_db(b"db").unwrap()));
        let code = encode_code_key(b"db", &[7u8; HASH_LEN]).unwrap();
        assert_eq!(code.len(), encode_db(b"db").unwrap().len() + HASH_LEN);
    }
}
