//! Declarative access rules: [`Operand`], [`Rule`], [`Policy`] and
//! [`FieldAccess`].
//!
//! Rules are data, not guest code: the builders emit rule trees into the
//! manifest and the host evaluates them natively on every collection
//! operation a caller performs. Hidden rows read as absent and drop out of
//! scans and range pages before limits and cursors, unreadable fields are
//! redacted (the member is missing, never `null`), and denied writes fail
//! with [`ACCESS_DENIED`][crate::error::codes::ACCESS_DENIED].
//!
//! Comparisons with a missing or null side are false, including two
//! missing sides, so a row without an owner never matches an anonymous
//! caller's missing subject. Only numbers order against numbers and
//! strings against strings.
//!
//! The encoding, which the host validates at deploy:
//!
//! - rules: `{"const": bool}`, `{"all": [rule…]}`, `{"any": [rule…]}`,
//!   `{"not": rule}`, `{"eq"|"ne"|"lt"|"lte"|"gt"|"gte"|"in"|"starts_with":
//!   [operand, operand]}`, `{"exists": operand}`, `{"readable": [collection,
//!   key]}` and `{"stored": [collection, key]}`;
//! - operands: `{"value": json}`, `{"ref": [root, path…]}` with roots
//!   `principal`, `row`, `next`, `key`, `args` and `now`, and `{"list":
//!   [operand…]}` as the key of `readable` and `stored` only;
//! - policies: `{"read", "insert", "update", "delete": rule, "fields":
//!   {name: {"read"?: rule, "write"?: rule}}}`. An operation without a rule
//!   is denied.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use crate::value::Value;

/// Most parts a [`Operand::list`] key holds.
pub const MAX_KEY_PARTS: usize = 16;

/// Most fields one `set_clearing` may clear.
pub const MAX_CLEARED_FIELDS: usize = 256;

fn single(key: &str, value: Value) -> Value {
    let mut map = BTreeMap::new();
    map.insert(String::from(key), value);
    Value::Object(map)
}

/// A value a rule compares: a literal, or a path into the caller, the
/// stored row, the written row, the row key, derived arguments or the
/// clock.
#[derive(Clone, Debug, PartialEq)]
pub struct Operand(Value);

impl Operand {
    /// A literal JSON value.
    pub fn literal(value: impl Into<Value>) -> Self {
        Operand(single("value", value.into()))
    }

    fn path(root: &str, path: &[&str]) -> Self {
        let mut parts = Vec::with_capacity(path.len() + 1);
        parts.push(Value::from(root));
        parts.extend(path.iter().map(|part| Value::from(*part)));
        Operand(single("ref", Value::Array(parts)))
    }

    /// A field of the stored row (`read`, `update`, `delete` and field
    /// rules).
    pub fn row(path: &[&str]) -> Self {
        Self::path("row", path)
    }

    /// A field of the row being written (`insert`, `update` and field
    /// write rules).
    pub fn next(path: &[&str]) -> Self {
        Self::path("next", path)
    }

    /// The caller's subject; missing for anonymous callers.
    pub fn subject() -> Self {
        Self::path("principal", &["subject"])
    }

    /// The caller's tenant.
    pub fn tenant() -> Self {
        Self::path("principal", &["tenant"])
    }

    /// A path into the caller's claims.
    pub fn claim(path: &[&str]) -> Self {
        let mut parts = vec!["claims"];
        parts.extend_from_slice(path);
        Self::path("principal", &parts)
    }

    /// The row key, or a path into an array or object key.
    pub fn key(path: &[&str]) -> Self {
        Self::path("key", path)
    }

    /// A path into a derived value's arguments (derived access rules).
    pub fn args(path: &[&str]) -> Self {
        Self::path("args", path)
    }

    /// The invocation time in milliseconds.
    pub fn now() -> Self {
        Self::path("now", &[])
    }

    /// A composite key: the array of its parts' values. Only the key of
    /// [`Rule::readable`] and [`Rule::stored`]; every part must resolve to
    /// a non-null value or the lookup fails.
    pub fn list(parts: impl IntoIterator<Item = Operand>) -> Self {
        let parts: Vec<Value> = parts.into_iter().map(|part| part.0).collect();
        Operand(single("list", Value::Array(parts)))
    }

    fn compare(self, op: &str, other: Operand) -> Rule {
        Rule(single(op, Value::Array(vec![self.0, other.0])))
    }

    /// Both sides present and equal.
    pub fn eq(self, other: Operand) -> Rule {
        self.compare("eq", other)
    }

    /// Both sides present and different.
    pub fn ne(self, other: Operand) -> Rule {
        self.compare("ne", other)
    }

    /// Ordered strictly below.
    pub fn lt(self, other: Operand) -> Rule {
        self.compare("lt", other)
    }

    /// Ordered at or below.
    pub fn lte(self, other: Operand) -> Rule {
        self.compare("lte", other)
    }

    /// Ordered strictly above.
    pub fn gt(self, other: Operand) -> Rule {
        self.compare("gt", other)
    }

    /// Ordered at or above.
    pub fn gte(self, other: Operand) -> Rule {
        self.compare("gte", other)
    }

    /// Text starting with a prefix.
    pub fn starts_with(self, prefix: Operand) -> Rule {
        self.compare("starts_with", prefix)
    }

    /// A present value that equals some element of an array.
    pub fn in_list(self, list: Operand) -> Rule {
        self.compare("in", list)
    }

    /// An array holding a present value equal to `item`.
    pub fn has(self, item: Operand) -> Rule {
        item.in_list(self)
    }

    /// Present and not null.
    pub fn exists(self) -> Rule {
        Rule(single("exists", self.0))
    }

    /// The encoded operand.
    pub fn to_value(&self) -> Value {
        self.0.clone()
    }
}

/// A boolean access rule.
#[derive(Clone, Debug, PartialEq)]
pub struct Rule(Value);

impl From<bool> for Rule {
    fn from(value: bool) -> Self {
        Rule(single("const", Value::Bool(value)))
    }
}

impl Rule {
    /// Holds for every caller.
    pub fn allow() -> Self {
        Rule::from(true)
    }

    /// Holds for no caller.
    pub fn deny() -> Self {
        Rule::from(false)
    }

    /// Holds for authenticated callers.
    pub fn authenticated() -> Self {
        Operand::subject().exists()
    }

    /// A row of `collection` (same module) exists at `key` and the
    /// caller may read it by that collection's read rule, which must not
    /// use `readable` itself.
    pub fn readable(collection: &str, key: Operand) -> Self {
        Rule(single(
            "readable",
            Value::Array(vec![Value::from(collection), key.0]),
        ))
    }

    /// A row of `collection` (same module) exists at `key`, whoever may
    /// read it.
    pub fn stored(collection: &str, key: Operand) -> Self {
        Rule(single(
            "stored",
            Value::Array(vec![Value::from(collection), key.0]),
        ))
    }

    fn join(op: &str, left: Rule, right: Rule) -> Self {
        let mut parts = Vec::new();
        for rule in [left, right] {
            match rule.0 {
                Value::Object(mut map)
                    if map.len() == 1 && matches!(map.get(op), Some(Value::Array(_))) =>
                {
                    if let Some(Value::Array(nested)) = map.remove(op) {
                        parts.extend(nested);
                    }
                }
                other => parts.push(other),
            }
        }
        Rule(single(op, Value::Array(parts)))
    }

    /// Both rules hold.
    pub fn and(self, other: Rule) -> Self {
        Self::join("all", self, other)
    }

    /// Either rule holds.
    pub fn or(self, other: Rule) -> Self {
        Self::join("any", self, other)
    }

    /// The rule does not hold.
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        Rule(single("not", self.0))
    }

    /// The encoded rule.
    pub fn to_value(&self) -> Value {
        self.0.clone()
    }
}

/// Rules for one top-level field, on top of the row's. Without `write`,
/// a caller may change the field only where it may read it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FieldAccess {
    read: Option<Rule>,
    write: Option<Rule>,
}

impl FieldAccess {
    /// No field rules yet.
    pub fn new() -> Self {
        FieldAccess::default()
    }

    /// Who may read the field; others see it redacted.
    pub fn read(mut self, rule: impl Into<Rule>) -> Self {
        self.read = Some(rule.into());
        self
    }

    /// Who may change the field.
    pub fn write(mut self, rule: impl Into<Rule>) -> Self {
        self.write = Some(rule.into());
        self
    }

    fn to_value(&self) -> Value {
        let mut map = BTreeMap::new();
        if let Some(read) = &self.read {
            map.insert(String::from("read"), read.0.clone());
        }
        if let Some(write) = &self.write {
            map.insert(String::from("write"), write.0.clone());
        }
        Value::Object(map)
    }
}

/// A collection's access policy. Operations without a rule are denied.
#[derive(Clone, Debug, PartialEq)]
pub struct Policy {
    read: Rule,
    insert: Rule,
    update: Rule,
    delete: Rule,
    // A list, not a map: guests then link no map code of their own for it.
    fields: Vec<(String, FieldAccess)>,
}

impl Default for Policy {
    fn default() -> Self {
        Policy::new()
    }
}

impl Policy {
    /// A policy denying everything until rules are added.
    pub fn new() -> Self {
        Policy {
            read: Rule::deny(),
            insert: Rule::deny(),
            update: Rule::deny(),
            delete: Rule::deny(),
            fields: Vec::new(),
        }
    }

    /// Who may read rows.
    pub fn read(mut self, rule: impl Into<Rule>) -> Self {
        self.read = rule.into();
        self
    }

    /// Who may insert, update and delete rows: one rule for all three.
    pub fn write(self, rule: impl Into<Rule>) -> Self {
        let rule = rule.into();
        self.insert(rule.clone()).update(rule.clone()).delete(rule)
    }

    /// Who may insert rows; sees `next`.
    pub fn insert(mut self, rule: impl Into<Rule>) -> Self {
        self.insert = rule.into();
        self
    }

    /// Who may update rows; sees `row` and `next`.
    pub fn update(mut self, rule: impl Into<Rule>) -> Self {
        self.update = rule.into();
        self
    }

    /// Who may delete rows; sees `row`.
    pub fn delete(mut self, rule: impl Into<Rule>) -> Self {
        self.delete = rule.into();
        self
    }

    /// Rules for one top-level field.
    pub fn field(mut self, name: &str, access: FieldAccess) -> Self {
        match self.fields.iter_mut().find(|(field, _)| field == name) {
            Some((_, existing)) => *existing = access,
            None => self.fields.push((String::from(name), access)),
        }
        self
    }

    /// The encoded policy.
    pub fn to_value(&self) -> Value {
        let mut map = BTreeMap::new();
        map.insert(String::from("read"), self.read.0.clone());
        map.insert(String::from("insert"), self.insert.0.clone());
        map.insert(String::from("update"), self.update.0.clone());
        map.insert(String::from("delete"), self.delete.0.clone());
        let mut fields = BTreeMap::new();
        for (name, access) in &self.fields {
            fields.insert(name.clone(), access.to_value());
        }
        map.insert(String::from("fields"), Value::Object(fields));
        Value::Object(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(value: &Value) -> String {
        value.canonical_json().unwrap()
    }

    #[test]
    fn operands_encode_paths_and_literals() {
        assert_eq!(
            json(&Operand::row(&["owner"]).to_value()),
            r#"{"ref":["row","owner"]}"#
        );
        assert_eq!(
            json(&Operand::claim(&["role"]).to_value()),
            r#"{"ref":["principal","claims","role"]}"#
        );
        assert_eq!(json(&Operand::now().to_value()), r#"{"ref":["now"]}"#);
        assert_eq!(
            json(&Operand::literal("admin").to_value()),
            r#"{"value":"admin"}"#
        );
        assert_eq!(
            json(&Operand::list([Operand::row(&["team"]), Operand::subject()]).to_value()),
            r#"{"list":[{"ref":["row","team"]},{"ref":["principal","subject"]}]}"#
        );
    }

    #[test]
    fn rules_flatten_joins_and_negate() {
        let owner = Operand::row(&["owner"]).eq(Operand::subject());
        let admin = Operand::claim(&["role"]).eq(Operand::literal("admin"));
        let both = owner.clone().and(admin.clone()).and(Rule::authenticated());
        assert_eq!(
            json(&both.to_value()),
            r#"{"all":[{"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]},{"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]},{"exists":{"ref":["principal","subject"]}}]}"#
        );
        assert_eq!(
            json(&Rule::allow().not().to_value()),
            r#"{"not":{"const":true}}"#
        );
        assert_eq!(
            json(&Rule::stored("members", Operand::subject()).to_value()),
            r#"{"stored":["members",{"ref":["principal","subject"]}]}"#
        );
        assert_eq!(
            json(
                &Operand::row(&["tags"])
                    .has(Operand::literal("x"))
                    .to_value()
            ),
            r#"{"in":[{"value":"x"},{"ref":["row","tags"]}]}"#
        );
    }

    #[test]
    fn policies_deny_by_default_and_encode_fields() {
        let policy = Policy::new()
            .read(true)
            .field("secret", FieldAccess::new().read(false));
        assert_eq!(
            json(&policy.to_value()),
            r#"{"delete":{"const":false},"fields":{"secret":{"read":{"const":false}}},"insert":{"const":false},"read":{"const":true},"update":{"const":false}}"#
        );
        let writable = Policy::new().write(Rule::authenticated()).to_value();
        let map = writable.as_object().unwrap();
        assert_eq!(map["insert"], map["update"]);
        assert_eq!(map["update"], map["delete"]);
    }
}
