//! Schemas: validatable contracts built with [`v`].
//!
//! [`Schema`] values are `const`-constructible so [`typed::FlowerType`][crate::typed::FlowerType]
//! implementations can expose them as associated constants. [`Schema::parse`]
//! validates a [`Value`] and reports failures with a path and a message;
//! [`Schema::is`] checks membership. Paths start at `$` with `.field` for
//! object members (or `["field"]` when the name is not identifier-safe) and
//! `[index]` for array and tuple positions.
//!
//! String `pattern` constraints are carried in the schema and enforced
//! authoritatively by the host; guest-side parsing checks type and length
//! only. Length bounds count Unicode scalar values.

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::convert::From;

use crate::error::{Failure, codes};
use crate::value::{Value, escape_str_into};

/// Schema validation failure: path plus message.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaError {
    /// Path to the offending value, starting at `$`.
    pub path: String,
    /// The same path as member names (strings) and indexes (numbers),
    /// as v1's `details.path`.
    pub segments: Vec<Value>,
    /// What was expected.
    pub message: String,
}

impl SchemaError {
    /// The failure for arguments their definition's contract refuses,
    /// v1's `INVALID_ARGUMENT`: its `details.path` names the offending
    /// value. `what` says whose arguments (``arguments for `order` ``).
    pub fn invalid_argument(self, what: &str) -> Failure {
        let mut details = BTreeMap::new();
        details.insert(String::from("path"), Value::Array(self.segments));
        let mut message = String::from(what);
        message.push_str(" invalid at ");
        message.push_str(&self.path);
        message.push_str(": ");
        message.push_str(&self.message);
        Failure::with_details(codes::INVALID_ARGUMENT, message, Value::Object(details))
    }
}

impl From<SchemaError> for Failure {
    fn from(error: SchemaError) -> Failure {
        let mut message = String::from("schema mismatch at ");
        message.push_str(&error.path);
        message.push_str(": ");
        message.push_str(&error.message);
        Failure::new(codes::SCHEMA_MISMATCH, message)
    }
}

/// Validatable contract over [`Value`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Schema {
    /// Exact signed 64-bit integer carried as a decimal string.
    DecimalI64,
    /// Exact unsigned 64-bit integer carried as a decimal string.
    DecimalU64,
    /// String with optional length bounds and pattern.
    String {
        /// Minimum length in scalar values.
        min_length: Option<u32>,
        /// Maximum length in scalar values.
        max_length: Option<u32>,
        /// Host-enforced pattern; see the module documentation.
        pattern: Option<&'static str>,
    },
    /// Finite number with optional inclusive bounds.
    Number { min: Option<f64>, max: Option<f64> },
    /// Integer-valued finite number with optional inclusive bounds.
    Int { min: Option<f64>, max: Option<f64> },
    /// Boolean.
    Boolean,
    /// Null.
    Null,
    /// Any valid value.
    Json,
    /// Homogeneous array.
    Array { element: &'static Schema },
    /// Fixed-length heterogeneous array; extra items are rejected.
    Tuple { elements: &'static [Schema] },
    /// Closed object; unknown members are rejected.
    Object { fields: &'static [Field] },
    /// Null or the inner schema.
    Nullable { inner: &'static Schema },
    /// String naming one of the declared variants.
    Enum { variants: &'static [&'static str] },
}

impl Schema {
    /// The bounded structural representation used in module manifests and contract fingerprints.
    pub fn to_value(&self) -> crate::Result<Value> {
        crate::abi::schema_value(self, 0)
    }
}

/// One named object member.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Field {
    /// Wire name.
    pub name: &'static str,
    /// Member contract.
    pub schema: Schema,
    /// Whether the member may be absent.
    pub optional: bool,
}

/// `const` schema constructors and per-kind builders.
pub mod v {
    use super::{Field, Schema};

    /// String schema builder.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct StringBuilder {
        min_length: Option<u32>,
        max_length: Option<u32>,
        pattern: Option<&'static str>,
    }

    impl StringBuilder {
        /// Minimum length in scalar values.
        pub const fn min(mut self, len: u32) -> Self {
            self.min_length = Some(len);
            self
        }

        /// Maximum length in scalar values.
        pub const fn max(mut self, len: u32) -> Self {
            self.max_length = Some(len);
            self
        }

        /// Host-enforced pattern.
        pub const fn pattern(mut self, pattern: &'static str) -> Self {
            self.pattern = Some(pattern);
            self
        }

        /// Finish the schema.
        pub const fn build(self) -> Schema {
            Schema::String {
                min_length: self.min_length,
                max_length: self.max_length,
                pattern: self.pattern,
            }
        }
    }

    /// Number schema builder.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct NumberBuilder {
        min: Option<f64>,
        max: Option<f64>,
    }

    impl NumberBuilder {
        /// Inclusive minimum.
        pub const fn min(mut self, bound: f64) -> Self {
            self.min = Some(bound);
            self
        }

        /// Inclusive maximum.
        pub const fn max(mut self, bound: f64) -> Self {
            self.max = Some(bound);
            self
        }

        /// Finish the schema.
        pub const fn build(self) -> Schema {
            Schema::Number {
                min: self.min,
                max: self.max,
            }
        }
    }

    /// Integer schema builder. Bounds are `i64` and convert exactly for every
    /// bound an `f64` value can meaningfully meet.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct IntBuilder {
        min: Option<f64>,
        max: Option<f64>,
    }

    impl IntBuilder {
        /// Inclusive minimum.
        pub const fn min(mut self, bound: i64) -> Self {
            self.min = Some(bound as f64);
            self
        }

        /// Inclusive maximum.
        pub const fn max(mut self, bound: i64) -> Self {
            self.max = Some(bound as f64);
            self
        }

        /// Finish the schema.
        pub const fn build(self) -> Schema {
            Schema::Int {
                min: self.min,
                max: self.max,
            }
        }
    }

    /// String schema builder.
    pub const fn string() -> StringBuilder {
        StringBuilder {
            min_length: None,
            max_length: None,
            pattern: None,
        }
    }

    /// Number schema builder.
    pub const fn number() -> NumberBuilder {
        NumberBuilder {
            min: None,
            max: None,
        }
    }

    /// Integer schema builder.
    pub const fn int() -> IntBuilder {
        IntBuilder {
            min: None,
            max: None,
        }
    }

    /// Boolean schema.
    pub const fn boolean() -> Schema {
        Schema::Boolean
    }

    /// Null schema.
    pub const fn null() -> Schema {
        Schema::Null
    }

    /// Any-value schema.
    pub const fn json() -> Schema {
        Schema::Json
    }

    /// Homogeneous array schema.
    pub const fn array(element: &'static Schema) -> Schema {
        Schema::Array { element }
    }

    /// Fixed-length tuple schema.
    pub const fn tuple(elements: &'static [Schema]) -> Schema {
        Schema::Tuple { elements }
    }

    /// Closed object schema.
    pub const fn object(fields: &'static [Field]) -> Schema {
        Schema::Object { fields }
    }

    /// Nullable schema.
    pub const fn nullable(inner: &'static Schema) -> Schema {
        Schema::Nullable { inner }
    }

    /// Required object member.
    pub const fn field(name: &'static str, schema: Schema) -> Field {
        Field {
            name,
            schema,
            optional: false,
        }
    }

    /// Optional object member.
    pub const fn optional(name: &'static str, schema: Schema) -> Field {
        Field {
            name,
            schema,
            optional: true,
        }
    }

    /// Closed string-enum schema.
    pub const fn str_enum(variants: &'static [&'static str]) -> Schema {
        Schema::Enum { variants }
    }

    /// Object member with explicit optionality.
    pub const fn field_or_optional(name: &'static str, schema: Schema, optional: bool) -> Field {
        Field {
            name,
            schema,
            optional,
        }
    }
}

impl Schema {
    /// Validate `value`, returning the first failure with its path.
    pub fn parse(&self, value: &Value) -> core::result::Result<(), SchemaError> {
        if let Err(failure) = value.validate() {
            return Err(SchemaError {
                path: String::from("$"),
                segments: Vec::new(),
                message: failure.message,
            });
        }
        let mut path = Path {
            text: String::from("$"),
            segments: Vec::new(),
        };
        self.check(value, &mut path)
    }

    /// Whether `value` satisfies this schema.
    pub fn is(&self, value: &Value) -> bool {
        self.parse(value).is_ok()
    }

    /// Whether every value `narrower` admits, this schema admits too:
    /// conservative structural containment, the compatibility rule for
    /// module contracts. Anything it cannot prove contained is not:
    /// patterns compare by equality and enums only fit pattern-free
    /// strings, so a `false` may be too strict but a `true` never lies.
    pub fn accepts(&self, narrower: &Schema) -> bool {
        self.accepts_at(narrower, 0)
    }

    fn accepts_at(&self, narrower: &Schema, depth: usize) -> bool {
        if depth > crate::value::MAX_VALUE_DEPTH {
            return false;
        }
        let next = depth + 1;
        let within = |wide: (Option<f64>, Option<f64>), narrow: (Option<f64>, Option<f64>)| {
            let low = match (wide.0, narrow.0) {
                (None, _) => true,
                (Some(wide), Some(narrow)) => narrow >= wide,
                (Some(_), None) => false,
            };
            let high = match (wide.1, narrow.1) {
                (None, _) => true,
                (Some(wide), Some(narrow)) => narrow <= wide,
                (Some(_), None) => false,
            };
            low && high
        };
        match (self, narrower) {
            (Schema::Json, _) => true,
            (_, Schema::Json) => false,
            (Schema::Nullable { inner: wide }, Schema::Nullable { inner: narrow }) => {
                wide.accepts_at(narrow, next)
            }
            (Schema::Nullable { .. }, Schema::Null) => true,
            (Schema::Nullable { inner }, narrow) => inner.accepts_at(narrow, next),
            (wide, Schema::Nullable { inner }) => {
                wide.accepts_at(&Schema::Null, next) && wide.accepts_at(inner, next)
            }
            (Schema::Null, Schema::Null) | (Schema::Boolean, Schema::Boolean) => true,
            (Schema::DecimalI64, Schema::DecimalI64) | (Schema::DecimalU64, Schema::DecimalU64) => {
                true
            }
            (
                Schema::String {
                    min_length: wide_min,
                    max_length: wide_max,
                    pattern: wide_pattern,
                },
                Schema::String {
                    min_length,
                    max_length,
                    pattern,
                },
            ) => {
                within(
                    (wide_min.map(f64::from), wide_max.map(f64::from)),
                    (
                        Some(f64::from(min_length.unwrap_or(0))),
                        max_length.map(f64::from),
                    ),
                ) && (wide_pattern.is_none() || wide_pattern == pattern)
            }
            (Schema::String { pattern: None, .. }, Schema::Enum { variants }) => variants
                .iter()
                .all(|variant| self.is(&Value::String(String::from(*variant)))),
            (Schema::Enum { variants: wide }, Schema::Enum { variants }) => {
                variants.iter().all(|variant| wide.contains(variant))
            }
            (
                Schema::Number { min, max },
                Schema::Number {
                    min: narrow_min,
                    max: narrow_max,
                }
                | Schema::Int {
                    min: narrow_min,
                    max: narrow_max,
                },
            )
            | (
                Schema::Int { min, max },
                Schema::Int {
                    min: narrow_min,
                    max: narrow_max,
                },
            ) => within((*min, *max), (*narrow_min, *narrow_max)),
            (Schema::Array { element: wide }, Schema::Array { element }) => {
                wide.accepts_at(element, next)
            }
            (Schema::Array { element: wide }, Schema::Tuple { elements }) => elements
                .iter()
                .all(|element| wide.accepts_at(element, next)),
            (Schema::Tuple { elements: wide }, Schema::Tuple { elements }) => {
                wide.len() == elements.len()
                    && wide
                        .iter()
                        .zip(elements.iter())
                        .all(|(wide, narrow)| wide.accepts_at(narrow, next))
            }
            (Schema::Object { fields: wide }, Schema::Object { fields }) => {
                // Every member the narrower object may carry fits a wider
                // member, absent only where the wider one may be; every
                // member the wider one requires, the narrower requires.
                fields.iter().all(|field| {
                    wide.iter().any(|candidate| {
                        candidate.name == field.name
                            && (candidate.optional || !field.optional)
                            && candidate.schema.accepts_at(&field.schema, next)
                    })
                }) && wide.iter().all(|candidate| {
                    candidate.optional
                        || fields
                            .iter()
                            .any(|field| field.name == candidate.name && !field.optional)
                })
            }
            _ => false,
        }
    }

    fn check(&self, value: &Value, path: &mut Path) -> core::result::Result<(), SchemaError> {
        match self {
            Schema::DecimalI64 => match value {
                Value::String(text)
                    if crate::typed::valid_decimal(text, true) && text.parse::<i64>().is_ok() =>
                {
                    Ok(())
                }
                _ => path.fail("expected signed 64-bit decimal string"),
            },
            Schema::DecimalU64 => match value {
                Value::String(text)
                    if crate::typed::valid_decimal(text, false) && text.parse::<u64>().is_ok() =>
                {
                    Ok(())
                }
                _ => path.fail("expected unsigned 64-bit decimal string"),
            },
            Schema::String {
                min_length,
                max_length,
                ..
            } => {
                let text = match value {
                    Value::String(text) => text,
                    _ => return path.fail("expected string"),
                };
                let len = text.chars().count();
                if let Some(min) = min_length
                    && len < *min as usize
                {
                    return path.fail("string shorter than minimum");
                }
                if let Some(max) = max_length
                    && len > *max as usize
                {
                    return path.fail("string longer than maximum");
                }
                Ok(())
            }
            Schema::Number { min, max } => {
                let number = match value {
                    Value::Number(number) => number,
                    _ => return path.fail("expected number"),
                };
                if let Some(min) = min
                    && *number < *min
                {
                    return path.fail("number below minimum");
                }
                if let Some(max) = max
                    && *number > *max
                {
                    return path.fail("number above maximum");
                }
                Ok(())
            }
            Schema::Int { min, max } => {
                let number = match value {
                    Value::Number(number) if number % 1.0 == 0.0 => number,
                    Value::Number(_) => return path.fail("expected integer"),
                    _ => return path.fail("expected integer"),
                };
                if let Some(min) = min
                    && *number < *min
                {
                    return path.fail("integer below minimum");
                }
                if let Some(max) = max
                    && *number > *max
                {
                    return path.fail("integer above maximum");
                }
                Ok(())
            }
            Schema::Boolean => match value {
                Value::Bool(_) => Ok(()),
                _ => path.fail("expected boolean"),
            },
            Schema::Null => match value {
                Value::Null => Ok(()),
                _ => path.fail("expected null"),
            },
            Schema::Json => Ok(()),
            Schema::Array { element } => match value {
                Value::Array(items) => {
                    for (index, item) in items.iter().enumerate() {
                        let mark = path.text.len();
                        path.push_index(index);
                        element.check(item, path)?;
                        path.pop_to(mark);
                    }
                    Ok(())
                }
                _ => path.fail("expected array"),
            },
            Schema::Tuple { elements } => match value {
                Value::Array(items) if items.len() == elements.len() => {
                    for (index, (item, schema)) in items.iter().zip(elements.iter()).enumerate() {
                        let mark = path.text.len();
                        path.push_index(index);
                        schema.check(item, path)?;
                        path.pop_to(mark);
                    }
                    Ok(())
                }
                Value::Array(_) => path.fail("tuple arity mismatch"),
                _ => path.fail("expected tuple"),
            },
            Schema::Object { fields } => match value {
                Value::Object(map) => {
                    for (key, item) in map.iter() {
                        let field = match fields.iter().find(|field| field.name == key) {
                            Some(field) => field,
                            None => {
                                let mark = path.text.len();
                                path.push_field(key);
                                let failed = path.fail("unknown member");
                                path.pop_to(mark);
                                return failed;
                            }
                        };
                        let mark = path.text.len();
                        path.push_field(key);
                        field.schema.check(item, path)?;
                        path.pop_to(mark);
                    }
                    for field in fields.iter() {
                        if !field.optional && !map.contains_key(field.name) {
                            let mark = path.text.len();
                            path.push_field(field.name);
                            let failed = path.fail("missing required member");
                            path.pop_to(mark);
                            return failed;
                        }
                    }
                    Ok(())
                }
                _ => path.fail("expected object"),
            },
            Schema::Nullable { inner } => match value {
                Value::Null => Ok(()),
                _ => inner.check(value, path),
            },
            Schema::Enum { variants } => match value {
                Value::String(text) if variants.contains(&text.as_str()) => Ok(()),
                Value::String(_) => path.fail("unknown enum variant"),
                _ => path.fail("expected enum string"),
            },
        }
    }
}

struct Path {
    text: String,
    segments: Vec<Value>,
}

impl Path {
    fn push_field(&mut self, name: &str) {
        if !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            self.text.push('.');
            self.text.push_str(name);
        } else {
            self.text.push_str("[\"");
            escape_str_into(&mut self.text, name);
            self.text.push_str("\"]");
        }
        self.segments.push(Value::String(String::from(name)));
    }

    fn push_index(&mut self, index: usize) {
        self.text.push('[');
        self.text.push_str(&index.to_string());
        self.text.push(']');
        self.segments.push(Value::Number(index as f64));
    }

    /// Back to the text length `len`, dropping the segment pushed since.
    fn pop_to(&mut self, len: usize) {
        self.text.truncate(len);
        self.segments.pop();
    }

    fn fail(&self, message: &str) -> core::result::Result<(), SchemaError> {
        Err(SchemaError {
            path: self.text.clone(),
            segments: self.segments.clone(),
            message: String::from(message),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Failure, codes};
    use crate::value::Value;
    use alloc::collections::BTreeMap;
    use alloc::string::String;
    use alloc::vec;

    const NAME: Schema = v::string().min(1).max(8).build();
    const COUNT: Schema = v::int().min(0).max(10).build();
    const SCORE: Schema = v::number().min(0.0).build();
    const STATUS: Schema = v::str_enum(&["open", "closed"]);
    const ROW: Schema = v::object(&[
        v::field("name", NAME),
        v::field("count", COUNT),
        v::optional("nick", NAME),
    ]);

    #[test]
    fn primitives_accept_and_reject() {
        assert!(v::boolean().is(&Value::Bool(true)));
        assert!(!v::boolean().is(&Value::Null));
        assert!(v::null().is(&Value::Null));
        assert!(v::json().is(&Value::Number(1.0)));
        assert!(!NAME.is(&Value::String(String::from(""))));
        assert!(!NAME.is(&Value::String(String::from("abcdefghi"))));
        assert!(NAME.is(&Value::String(String::from("ada"))));
        assert!(!COUNT.is(&Value::Number(1.5)));
        assert!(!COUNT.is(&Value::Number(11.0)));
        assert!(COUNT.is(&Value::Number(10.0)));
        assert!(COUNT.is(&Value::Number(-0.0)));
        assert!(!SCORE.is(&Value::Number(-1.0)));
    }

    #[test]
    fn containment_is_structural_and_conservative() {
        let wide_count = v::int().min(0).build();
        // Bounds nest; ints fit numbers but not the other way round.
        assert!(wide_count.accepts(&COUNT));
        assert!(!COUNT.accepts(&wide_count));
        assert!(SCORE.accepts(&COUNT));
        assert!(!COUNT.accepts(&SCORE));
        assert!(v::json().accepts(&ROW));
        assert!(!ROW.accepts(&v::json()));
        // Strings: lengths nest, patterns must agree, enums fit free strings.
        assert!(v::string().build().accepts(&NAME));
        assert!(!NAME.accepts(&v::string().build()));
        assert!(!v::string().pattern("^a").build().accepts(&NAME));
        assert!(v::string().build().accepts(&STATUS));
        assert!(!NAME.accepts(&v::str_enum(&["toolongforname"])));
        assert!(v::str_enum(&["open", "closed", "archived"]).accepts(&STATUS));
        assert!(!STATUS.accepts(&v::str_enum(&["open", "archived"])));
        // Nullability widens only.
        const MAYBE: Schema = v::nullable(&NAME);
        assert!(MAYBE.accepts(&NAME));
        assert!(MAYBE.accepts(&v::null()));
        assert!(!NAME.accepts(&MAYBE));
        // Objects: an optional member never satisfies a required one, and
        // every member the narrower one may carry must exist wider.
        const OPTIONAL_NAME: Schema = v::object(&[
            v::optional("name", NAME),
            v::field("count", COUNT),
            v::optional("nick", NAME),
        ]);
        const SHORT: Schema = v::object(&[v::field("name", NAME), v::field("count", COUNT)]);
        assert!(OPTIONAL_NAME.accepts(&ROW));
        assert!(!ROW.accepts(&OPTIONAL_NAME));
        assert!(ROW.accepts(&SHORT));
        assert!(!SHORT.accepts(&ROW));
        // Arrays take tuples of fitting elements; tuples need equal arity.
        const PAIR: Schema = v::tuple(&[COUNT, COUNT]);
        assert!(v::array(&SCORE).accepts(&PAIR));
        assert!(!PAIR.accepts(&v::array(&COUNT)));
        assert!(!PAIR.accepts(&v::tuple(&[COUNT])));
    }

    #[test]
    fn enums_match_declared_variants_only() {
        assert!(STATUS.is(&Value::String(String::from("open"))));
        assert!(STATUS.is(&Value::String(String::from("closed"))));
        assert!(!STATUS.is(&Value::String(String::from("archived"))));
        assert!(!STATUS.is(&Value::Null));
        let err = STATUS
            .parse(&Value::String(String::from("archived")))
            .unwrap_err();
        assert_eq!(err.path, "$");
        assert_eq!(err.message, "unknown enum variant");
    }

    #[test]
    fn string_lengths_count_scalar_values() {
        // "é" is one scalar value in two bytes.
        let one = v::string().min(1).max(1).build();
        assert!(one.is(&Value::String(String::from("é"))));
        assert!(!one.is(&Value::String(String::from("ab"))));
    }

    #[test]
    fn pattern_is_carried_not_guest_enforced() {
        // Patterns ride in the schema for host enforcement; guest parsing
        // checks type and length only.
        let schema = v::string().pattern("^[0-9]+$").build();
        assert!(schema.is(&Value::String(String::from("not digits"))));
        assert!(matches_pattern_carried(&schema));
    }

    fn matches_pattern_carried(schema: &Schema) -> bool {
        match schema {
            Schema::String {
                pattern: Some(pattern),
                ..
            } => *pattern == "^[0-9]+$",
            _ => false,
        }
    }

    #[test]
    fn object_paths_point_at_offenders() {
        let mut bad = BTreeMap::new();
        bad.insert(String::from("name"), Value::String(String::from("ada")));
        bad.insert(String::from("count"), Value::Number(99.0));
        let error = ROW.parse(&Value::Object(bad)).unwrap_err();
        assert_eq!(error.path, "$.count");

        let mut missing = BTreeMap::new();
        missing.insert(String::from("name"), Value::String(String::from("ada")));
        let error = ROW.parse(&Value::Object(missing)).unwrap_err();
        assert_eq!(error.path, "$.count");

        let mut extra = BTreeMap::new();
        extra.insert(String::from("name"), Value::String(String::from("ada")));
        extra.insert(String::from("count"), Value::Number(1.0));
        extra.insert(String::from("zzz"), Value::Null);
        let error = ROW.parse(&Value::Object(extra)).unwrap_err();
        assert_eq!(error.path, "$.zzz");
    }

    #[test]
    fn nested_paths_compose() {
        const INNER: Schema = v::object(&[v::field("b", COUNT)]);
        const OUTER: Schema = v::object(&[v::field("a", v::array(&INNER))]);
        let mut inner = BTreeMap::new();
        inner.insert(String::from("b"), Value::Number(1.0));
        let mut bad_inner = BTreeMap::new();
        bad_inner.insert(String::from("b"), Value::String(String::from("no")));
        let mut outer = BTreeMap::new();
        outer.insert(
            String::from("a"),
            Value::Array(vec![Value::Object(inner), Value::Object(bad_inner)]),
        );
        let error = OUTER.parse(&Value::Object(outer)).unwrap_err();
        assert_eq!(error.path, "$.a[1].b");
    }

    #[test]
    fn tuples_reject_arity_mismatch() {
        const PAIR: Schema = v::tuple(&[COUNT, NAME]);
        assert!(PAIR.is(&Value::Array(vec![
            Value::Number(1.0),
            Value::String(String::from("x")),
        ])));
        assert!(!PAIR.is(&Value::Array(vec![Value::Number(1.0)])));
        assert!(!PAIR.is(&Value::Array(vec![
            Value::Number(1.0),
            Value::String(String::from("x")),
            Value::Null,
        ])));
    }

    #[test]
    fn schema_error_converts_to_failure() {
        let error = COUNT.parse(&Value::Bool(true)).unwrap_err();
        let failure: Failure = error.into();
        assert_eq!(failure.code, codes::SCHEMA_MISMATCH);
        assert_eq!(failure.details, None);
    }

    #[test]
    fn non_finite_fails_at_root() {
        let error = v::json().parse(&Value::Number(f64::NAN)).unwrap_err();
        assert_eq!(error.path, "$");
    }
}
