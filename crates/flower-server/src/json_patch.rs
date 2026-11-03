//! Bounded RFC 6902 diffs for watch deltas: `add`, `remove` and
//! `replace` operations that turn one JSON value into another.
//!
//! Objects diff by key; arrays keep their common head and tail and edit
//! the middle by index (appends, truncations and splices without a
//! quadratic subsequence search); anything else that differs is
//! replaced. Every diff is bounded in operations, work and pointer
//! bytes, as v1's: past a bound there is no diff, and the watch sends
//! the whole value instead. No application code runs here.

use serde::Serialize;
use serde_json::Value;

/// Most operations one delta carries.
pub const MAX_OPERATIONS: usize = 256;
/// Most nodes one diff visits.
const MAX_WORK: usize = 100_000;
/// Longest JSON pointer.
const MAX_PATH_BYTES: usize = 16 * 1024;
/// Most pointer bytes one delta carries.
const MAX_TOTAL_PATH_BYTES: usize = 64 * 1024;
/// Most pointer bytes one diff builds while walking, emitted or not.
const MAX_TRAVERSED_PATH_BYTES: usize = 1024 * 1024;

/// One operation, borrowing its value from the newer document.
#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Operation<'a> {
    /// Insert at `path` (before the index, for arrays).
    Add {
        /// JSON pointer.
        path: String,
        /// Inserted value.
        value: &'a Value,
    },
    /// Remove what `path` names.
    Remove {
        /// JSON pointer.
        path: String,
    },
    /// Replace what `path` names.
    Replace {
        /// JSON pointer.
        path: String,
        /// New value.
        value: &'a Value,
    },
}

/// Past a bound: no diff.
struct Exceeded;

#[derive(Default)]
struct Diff<'a> {
    operations: Vec<Operation<'a>>,
    work: usize,
    path_bytes: usize,
    traversed: usize,
}

impl<'a> Diff<'a> {
    /// Charge one node of work.
    fn step(&mut self) -> Result<(), Exceeded> {
        self.work += 1;
        if self.work > MAX_WORK {
            return Err(Exceeded);
        }
        Ok(())
    }

    /// Emit one operation within the bounds.
    fn push(&mut self, operation: Operation<'a>) -> Result<(), Exceeded> {
        let path = match &operation {
            Operation::Add { path, .. }
            | Operation::Remove { path }
            | Operation::Replace { path, .. } => path,
        };
        self.path_bytes += path.len();
        if self.operations.len() >= MAX_OPERATIONS || self.path_bytes > MAX_TOTAL_PATH_BYTES {
            return Err(Exceeded);
        }
        self.operations.push(operation);
        Ok(())
    }

    /// The pointer of `parent`'s child `key`, escaped, within the bounds.
    fn path(&mut self, parent: &str, key: &str) -> Result<String, Exceeded> {
        // Bound before allocating: one huge key must not be copied into
        // many pointers. Escaping at most doubles the key.
        if parent.len() + key.len() + 1 > MAX_PATH_BYTES {
            return Err(Exceeded);
        }
        let path = format!("{parent}/{}", key.replace('~', "~0").replace('/', "~1"));
        if path.len() > MAX_PATH_BYTES {
            return Err(Exceeded);
        }
        // Long shared prefixes would otherwise allocate over and over
        // even for a one-leaf delta.
        self.traversed += path.len();
        if self.traversed > MAX_TRAVERSED_PATH_BYTES {
            return Err(Exceeded);
        }
        Ok(path)
    }

    /// Deep equality, charged as work.
    fn equal(&mut self, left: &Value, right: &Value) -> Result<bool, Exceeded> {
        self.step()?;
        match (left, right) {
            (Value::Array(left), Value::Array(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (left, right) in left.iter().zip(right) {
                    if !self.equal(left, right)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (Value::Object(left), Value::Object(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (key, left) in left {
                    match right.get(key) {
                        Some(right) if self.equal(left, right)? => {}
                        _ => return Ok(false),
                    }
                }
                Ok(true)
            }
            _ => Ok(left == right),
        }
    }

    /// Emit the operations turning `left` into `right` at `path`.
    fn walk(&mut self, left: &Value, right: &'a Value, path: &str) -> Result<(), Exceeded> {
        self.step()?;
        match (left, right) {
            (Value::Object(left), Value::Object(right)) => {
                for key in left.keys() {
                    self.step()?;
                    if !right.contains_key(key) {
                        let path = self.path(path, key)?;
                        self.push(Operation::Remove { path })?;
                    }
                }
                for (key, value) in right {
                    let child = self.path(path, key)?;
                    match left.get(key) {
                        Some(previous) => self.walk(previous, value, &child)?,
                        None => {
                            self.step()?;
                            self.push(Operation::Add { path: child, value })?;
                        }
                    }
                }
            }
            (Value::Array(left), Value::Array(right)) => {
                let shorter = left.len().min(right.len());
                let mut head = 0;
                while head < shorter && self.equal(&left[head], &right[head])? {
                    head += 1;
                }
                let mut tail = 0;
                while tail < shorter - head
                    && self.equal(&left[left.len() - 1 - tail], &right[right.len() - 1 - tail])?
                {
                    tail += 1;
                }
                let old_middle = left.len() - head - tail;
                let new_middle = right.len() - head - tail;
                for index in head..head + old_middle.min(new_middle) {
                    let child = self.path(path, &index.to_string())?;
                    self.walk(&left[index], &right[index], &child)?;
                }
                // Removals run from the end, so earlier indexes hold.
                for index in (head + new_middle..head + old_middle).rev() {
                    let child = self.path(path, &index.to_string())?;
                    self.push(Operation::Remove { path: child })?;
                }
                for (index, value) in right
                    .iter()
                    .enumerate()
                    .take(head + new_middle)
                    .skip(head + old_middle)
                {
                    let child = self.path(path, &index.to_string())?;
                    self.push(Operation::Add { path: child, value })?;
                }
            }
            _ if left != right => self.push(Operation::Replace {
                path: path.to_string(),
                value: right,
            })?,
            _ => {}
        }
        Ok(())
    }
}

/// The operations turning `left` into `right`, or `None` past a bound.
pub fn diff<'a>(left: &Value, right: &'a Value) -> Option<Vec<Operation<'a>>> {
    let mut diff = Diff::default();
    diff.walk(left, right, "").ok()?;
    Some(diff.operations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Apply operations the way the client does.
    fn apply(value: &mut Value, operations: &[Operation<'_>]) {
        for operation in operations {
            let (path, replacement) = match operation {
                Operation::Add { path, value } | Operation::Replace { path, value } => {
                    (path, Some(*value))
                }
                Operation::Remove { path } => (path, None),
            };
            if path.is_empty() {
                *value = replacement.unwrap().clone();
                continue;
            }
            let (parent, key) = path.rsplit_once('/').unwrap();
            let key = key.replace("~1", "/").replace("~0", "~");
            match value.pointer_mut(parent).unwrap() {
                Value::Object(object) => match replacement {
                    Some(value) => {
                        object.insert(key, value.clone());
                    }
                    None => assert!(object.remove(&key).is_some()),
                },
                Value::Array(array) => {
                    let index: usize = key.parse().unwrap();
                    match operation {
                        Operation::Add { value, .. } => array.insert(index, (*value).clone()),
                        Operation::Replace { value, .. } => array[index] = (*value).clone(),
                        Operation::Remove { .. } => {
                            array.remove(index);
                        }
                    }
                }
                _ => panic!("invalid patch target"),
            }
        }
    }

    #[test]
    fn pointers_and_splices_reconstruct_exact_values() {
        for (left, right) in [
            (
                json!({"~/": {"": 1}, "removed": null}),
                json!({"~/": {"": 2}, "added": true}),
            ),
            (json!([1, 2, 3]), json!([1, 2, 3, 4])),
            (json!([1, 2, 3]), json!([1])),
            (json!([1, 2, 3]), json!([1, 8, 9, 2, 3])),
            (json!([1, 8, 9, 2, 3]), json!([1, 2, 3])),
            (json!([{"a": 1}, {"b": 2}]), json!([{"a": 2}, {"b": 2}])),
            (json!(null), json!({"x": true})),
            (json!({"same": [1]}), json!({"same": [1]})),
        ] {
            let operations = diff(&left, &right).unwrap();
            let mut actual = left;
            apply(&mut actual, &operations);
            assert_eq!(actual, right);
        }
        assert!(diff(&json!({"a": 1}), &json!({"a": 1})).unwrap().is_empty());
    }

    #[test]
    fn random_arrays_and_objects_round_trip() {
        let mut seed = 12345_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..2000 {
            let left: Vec<_> = (0..next() % 20).map(|_| json!({"v": next() % 8})).collect();
            let right: Vec<_> = (0..next() % 20).map(|_| json!({"v": next() % 8})).collect();
            let mut actual = json!({"array": left, "before": true});
            let target = json!({"array": right, "after": false});
            let operations = diff(&actual, &target).unwrap();
            apply(&mut actual, &operations);
            assert_eq!(actual, target);
        }
    }

    #[test]
    fn excessive_operations_work_and_pointers_give_up() {
        assert!(diff(&json!([]), &json!(vec![0; MAX_OPERATIONS + 1])).is_none());
        assert!(diff(&json!(vec![0; MAX_WORK]), &json!(vec![0; MAX_WORK])).is_none());
        let huge = "x".repeat(MAX_PATH_BYTES + 1);
        assert!(diff(&json!({}), &json!({huge: 1})).is_none());
        let shared = "x".repeat(8 * 1024);
        let children: serde_json::Map<_, _> = (0..200)
            .map(|index| (index.to_string(), json!(index)))
            .collect();
        let left = json!({shared.clone(): children});
        let mut right = left.clone();
        right[&shared]["0"] = json!(-1);
        assert!(
            diff(&left, &right).is_none(),
            "traversed pointers, not just emitted ones, are bounded"
        );
    }
}
