//! Typed wire contracts: [`FlowerType`] and built-in types.
//!
//! [`FlowerType`] owns decoded values. It never borrows guest ABI memory and
//! needs no serde. Its [`Schema`][crate::schema::Schema] validates values and
//! feeds canonical module contracts.
//!
//! Missing-member rule: object decoding calls [`FlowerType::decode_field`]
//! with `None` for absent members only when
//! [`OPTIONAL_FIELD`][FlowerType::OPTIONAL_FIELD] is set; otherwise absence
//! is an error before decoding runs. In particular [`Option`] leaves
//! `OPTIONAL_FIELD` false — visible null is not an absent member — while
//! [`Field`] sets it so absent data decodes to [`Field::Redacted`].
//!
//! Integer rule: `i64` and `u64` accept only the exact safe-integer interval
//! (`-(2^53 - 1)` through `2^53 - 1`); wider values fail instead of rounding.
//! [`DecimalI64`] and [`DecimalU64`] carry the full integer range as canonical
//! decimal strings, and order as strings wherever they become index keys.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::convert::From;

use crate::ctx::RawPage;
use crate::error::{Failure, Result, codes, fail};
use crate::schema::{Schema, v};
use crate::value::Value;

pub use crate::ctx::{
    CallError, CallOutcome, DerivedCtx, MethodCtx, MutationCtx, QueryCtx, ReadCtx,
};

/// Largest magnitude exactly representable for integer duty: `2^53 - 1`.
pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// Typed wire contract: schema, encoding and decoding.
pub trait FlowerType: Sized + 'static {
    /// Validatable contract for this type.
    const SCHEMA: Schema;
    /// Whether an absent object member decodes via [`decode_field`][Self::decode_field]
    /// instead of failing eagerly.
    const OPTIONAL_FIELD: bool = false;

    /// Encode into a [`Value`].
    fn encode(&self) -> Result<Value>;

    /// Object fields may explicitly omit a value. Standalone encoding stays checked.
    fn encode_field(&self) -> Result<Option<Value>> {
        self.encode().map(Some)
    }

    /// Decode from a [`Value`].
    fn decode(value: Value) -> Result<Self>;

    /// Decode an object member, rejecting missing required members.
    fn decode_field(value: Option<Value>) -> Result<Self> {
        match value {
            Some(value) => Self::decode(value),
            None => {
                let mut message = String::from("missing required field of type ");
                message.push_str(core::any::type_name::<Self>());
                Err(fail(codes::MISSING_FIELD, message))
            }
        }
    }
}

/// A required nullable member. Unlike legacy method `Option` parameters, omission is an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Nullable<T>(pub Option<T>);
impl<T: FlowerType> FlowerType for Nullable<T> {
    const SCHEMA: Schema = <Option<T> as FlowerType>::SCHEMA;
    fn encode(&self) -> Result<Value> {
        self.0.encode()
    }
    fn decode(value: Value) -> Result<Self> {
        Option::<T>::decode(value).map(Self)
    }
}

/// Absence is independent of null and redaction. Use `Omittable<Nullable<T>>` for all three states.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Omittable<T> {
    #[default]
    Missing,
    Present(T),
}
impl<T: FlowerType> FlowerType for Omittable<T> {
    const SCHEMA: Schema = T::SCHEMA;
    const OPTIONAL_FIELD: bool = true;
    fn encode(&self) -> Result<Value> {
        match self {
            Self::Present(value) => value.encode(),
            Self::Missing => Err(fail(
                codes::MISSING_FIELD,
                "absent value has no standalone JSON representation",
            )),
        }
    }
    fn encode_field(&self) -> Result<Option<Value>> {
        match self {
            Self::Missing => Ok(None),
            Self::Present(value) => value.encode().map(Some),
        }
    }
    fn decode(value: Value) -> Result<Self> {
        T::decode(value).map(Self::Present)
    }
    fn decode_field(value: Option<Value>) -> Result<Self> {
        value
            .map(T::decode)
            .transpose()
            .map(|value| value.map_or(Self::Missing, Self::Present))
    }
}

/// Declared business error: schema, conversion into [`Failure`] and checked
/// conversion back. Unrecognized failures convert back to themselves as `Err`.
pub trait BusinessError: Sized + 'static {
    /// Contract for the declared failure shapes.
    const SCHEMA: Schema;

    /// Convert into a [`Failure`].
    fn into_failure(self) -> Failure;

    /// Convert a [`Failure`] back, or return it unchanged when unrecognized.
    fn from_failure(failure: &Failure) -> Result<Self>;
}

impl BusinessError for Failure {
    const SCHEMA: Schema = v::json();

    fn into_failure(self) -> Failure {
        self
    }

    fn from_failure(failure: &Failure) -> Result<Self> {
        Ok(failure.clone())
    }
}

impl FlowerType for () {
    const SCHEMA: Schema = v::null();

    fn encode(&self) -> Result<Value> {
        Ok(Value::Null)
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(()),
            _ => Err(fail(codes::TYPE_MISMATCH, "expected null")),
        }
    }
}

impl FlowerType for bool {
    const SCHEMA: Schema = v::boolean();

    fn encode(&self) -> Result<Value> {
        Ok(Value::Bool(*self))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Bool(value) => Ok(value),
            _ => Err(fail(codes::TYPE_MISMATCH, "expected boolean")),
        }
    }
}

impl FlowerType for String {
    const SCHEMA: Schema = v::string().build();

    fn encode(&self) -> Result<Value> {
        Ok(Value::String(self.clone()))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::String(value) => Ok(value),
            _ => Err(fail(codes::TYPE_MISMATCH, "expected string")),
        }
    }
}

impl FlowerType for f64 {
    const SCHEMA: Schema = v::number().build();

    fn encode(&self) -> Result<Value> {
        if !self.is_finite() {
            return Err(fail(codes::INVALID_VALUE, "non-finite number"));
        }
        Ok(Value::Number(*self))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Number(value) if value.is_finite() => Ok(value),
            Value::Number(_) => Err(fail(codes::INVALID_VALUE, "non-finite number")),
            _ => Err(fail(codes::TYPE_MISMATCH, "expected number")),
        }
    }
}

macro_rules! impl_int {
    ($($t:ty: $min:expr, $max:expr;)*) => {
        $(
            impl FlowerType for $t {
                const SCHEMA: Schema = v::int().min($min as i64).max($max as i64).build();

                fn encode(&self) -> Result<Value> {
                    if !($min..=$max).contains(self) {
                        return Err(fail(codes::INVALID_VALUE, "integer out of range"));
                    }
                    Ok(Value::Number(*self as f64))
                }

                fn decode(value: Value) -> Result<Self> {
                    match value {
                        Value::Number(found)
                            if found.is_finite()
                                && found % 1.0 == 0.0
                                && found >= ($min as f64)
                                && found <= ($max as f64) =>
                        {
                            Ok(found as $t)
                        }
                        Value::Number(_) => {
                            Err(fail(codes::INVALID_VALUE, "not an integer in range"))
                        }
                        _ => Err(fail(codes::TYPE_MISMATCH, "expected integer")),
                    }
                }
            }
        )*
    };
}

impl_int! {
    i8: i8::MIN, i8::MAX;
    i16: i16::MIN, i16::MAX;
    i32: i32::MIN, i32::MAX;
    i64: -MAX_SAFE_INTEGER, MAX_SAFE_INTEGER;
    u8: 0, u8::MAX;
    u16: 0, u16::MAX;
    u32: 0, u32::MAX;
    u64: 0, MAX_SAFE_INTEGER as u64;
}

/// Full-range signed integer as a canonical decimal string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecimalI64(pub i64);

/// Full-range unsigned integer as a canonical decimal string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecimalU64(pub u64);

pub(crate) fn valid_decimal(text: &str, signed: bool) -> bool {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut index = 0;
    if signed && bytes[0] == b'-' {
        if bytes.len() == 1 {
            return false;
        }
        index = 1;
    }
    while index < bytes.len() {
        if !bytes[index].is_ascii_digit() {
            return false;
        }
        index += 1;
    }
    true
}

impl FlowerType for DecimalI64 {
    const SCHEMA: Schema = Schema::DecimalI64;

    fn encode(&self) -> Result<Value> {
        Ok(Value::String(self.0.to_string()))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::String(text) if valid_decimal(&text, true) => match text.parse::<i64>() {
                Ok(value) => Ok(DecimalI64(value)),
                Err(_) => Err(fail(codes::INVALID_VALUE, "decimal out of range")),
            },
            Value::String(_) => Err(fail(codes::INVALID_VALUE, "invalid decimal syntax")),
            _ => Err(fail(codes::TYPE_MISMATCH, "expected decimal string")),
        }
    }
}

impl FlowerType for DecimalU64 {
    const SCHEMA: Schema = Schema::DecimalU64;

    fn encode(&self) -> Result<Value> {
        Ok(Value::String(self.0.to_string()))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::String(text) if valid_decimal(&text, false) => match text.parse::<u64>() {
                Ok(value) => Ok(DecimalU64(value)),
                Err(_) => Err(fail(codes::INVALID_VALUE, "decimal out of range")),
            },
            Value::String(_) => Err(fail(codes::INVALID_VALUE, "invalid decimal syntax")),
            _ => Err(fail(codes::TYPE_MISMATCH, "expected decimal string")),
        }
    }
}

impl<T: FlowerType> FlowerType for Option<T> {
    const SCHEMA: Schema = v::nullable(&T::SCHEMA);

    fn encode(&self) -> Result<Value> {
        match self {
            None => Ok(Value::Null),
            Some(value) => value.encode(),
        }
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            value => Ok(Some(T::decode(value)?)),
        }
    }
}

impl<T: FlowerType> FlowerType for Vec<T> {
    const SCHEMA: Schema = v::array(&T::SCHEMA);

    fn encode(&self) -> Result<Value> {
        let mut items = Vec::with_capacity(self.len());
        for item in self.iter() {
            items.push(item.encode()?);
        }
        Ok(Value::Array(items))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(T::decode(item)?);
                }
                Ok(out)
            }
            _ => Err(fail(codes::TYPE_MISMATCH, "expected array")),
        }
    }
}

impl<T: FlowerType> FlowerType for Box<T> {
    const SCHEMA: Schema = T::SCHEMA;

    fn encode(&self) -> Result<Value> {
        (**self).encode()
    }

    fn decode(value: Value) -> Result<Self> {
        Ok(Box::new(T::decode(value)?))
    }
}

impl<A: FlowerType> FlowerType for (A,) {
    const SCHEMA: Schema = v::tuple(&[A::SCHEMA]);

    fn encode(&self) -> Result<Value> {
        Ok(Value::Array(alloc::vec![self.0.encode()?]))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Array(mut items) if items.len() == 1 => {
                let first = items.pop();
                match first {
                    Some(first) => Ok((A::decode(first)?,)),
                    None => Err(fail(codes::TYPE_MISMATCH, "expected 1-tuple")),
                }
            }
            _ => Err(fail(codes::TYPE_MISMATCH, "expected 1-tuple")),
        }
    }
}

impl<A: FlowerType, B: FlowerType> FlowerType for (A, B) {
    const SCHEMA: Schema = v::tuple(&[A::SCHEMA, B::SCHEMA]);

    fn encode(&self) -> Result<Value> {
        Ok(Value::Array(alloc::vec![
            self.0.encode()?,
            self.1.encode()?
        ]))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Array(items) if items.len() == 2 => {
                let mut items = items.into_iter();
                let first = items.next();
                let second = items.next();
                match (first, second) {
                    (Some(first), Some(second)) => Ok((A::decode(first)?, B::decode(second)?)),
                    _ => Err(fail(codes::TYPE_MISMATCH, "expected 2-tuple")),
                }
            }
            _ => Err(fail(codes::TYPE_MISMATCH, "expected 2-tuple")),
        }
    }
}

impl<A: FlowerType, B: FlowerType, C: FlowerType> FlowerType for (A, B, C) {
    const SCHEMA: Schema = v::tuple(&[A::SCHEMA, B::SCHEMA, C::SCHEMA]);

    fn encode(&self) -> Result<Value> {
        Ok(Value::Array(alloc::vec![
            self.0.encode()?,
            self.1.encode()?,
            self.2.encode()?
        ]))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Array(items) if items.len() == 3 => {
                let mut items = items.into_iter();
                let first = items.next();
                let second = items.next();
                let third = items.next();
                match (first, second, third) {
                    (Some(first), Some(second), Some(third)) => {
                        Ok((A::decode(first)?, B::decode(second)?, C::decode(third)?))
                    }
                    _ => Err(fail(codes::TYPE_MISMATCH, "expected 3-tuple")),
                }
            }
            _ => Err(fail(codes::TYPE_MISMATCH, "expected 3-tuple")),
        }
    }
}

impl<A: FlowerType, B: FlowerType, C: FlowerType, D: FlowerType> FlowerType for (A, B, C, D) {
    const SCHEMA: Schema = v::tuple(&[A::SCHEMA, B::SCHEMA, C::SCHEMA, D::SCHEMA]);

    fn encode(&self) -> Result<Value> {
        Ok(Value::Array(alloc::vec![
            self.0.encode()?,
            self.1.encode()?,
            self.2.encode()?,
            self.3.encode()?
        ]))
    }

    fn decode(value: Value) -> Result<Self> {
        match value {
            Value::Array(items) if items.len() == 4 => {
                let mut items = items.into_iter();
                let first = items.next();
                let second = items.next();
                let third = items.next();
                let fourth = items.next();
                match (first, second, third, fourth) {
                    (Some(first), Some(second), Some(third), Some(fourth)) => Ok((
                        A::decode(first)?,
                        B::decode(second)?,
                        C::decode(third)?,
                        D::decode(fourth)?,
                    )),
                    _ => Err(fail(codes::TYPE_MISMATCH, "expected 4-tuple")),
                }
            }
            _ => Err(fail(codes::TYPE_MISMATCH, "expected 4-tuple")),
        }
    }
}

impl FlowerType for Value {
    const SCHEMA: Schema = v::json();

    fn encode(&self) -> Result<Value> {
        self.validate()?;
        Ok(self.clone())
    }

    fn decode(value: Value) -> Result<Self> {
        value.validate()?;
        Ok(value)
    }
}

/// Policy-visible or policy-hidden member data.
#[derive(Clone, Debug, PartialEq)]
pub enum Field<T> {
    /// Visible data.
    Visible(T),
    /// Hidden or absent data.
    Redacted,
}

impl<T: FlowerType> FlowerType for Field<T> {
    const SCHEMA: Schema = T::SCHEMA;
    const OPTIONAL_FIELD: bool = true;

    fn encode(&self) -> Result<Value> {
        match self {
            Field::Visible(value) => value.encode(),
            Field::Redacted => Err(fail(codes::REDACTED_VALUE, "cannot encode redacted field")),
        }
    }

    fn decode(value: Value) -> Result<Self> {
        Ok(Field::Visible(T::decode(value)?))
    }

    fn decode_field(value: Option<Value>) -> Result<Self> {
        match value {
            None => Ok(Field::Redacted),
            Some(value) => Self::decode(value),
        }
    }
}

/// A collection's typed shape, implemented by `#[collection]` markers so
/// typed components (triggers, retention) name a collection by its marker.
pub trait TypedCollection: 'static {
    /// Key type.
    type Key: FlowerType;
    /// Row type.
    type Value: FlowerType;
    /// Collection name.
    const NAME: &'static str;
}

/// One typed scan row: decoded key plus decoded value.
#[derive(Clone, Debug, PartialEq)]
pub struct Row<K, V> {
    /// Decoded key.
    pub key: K,
    /// Decoded value.
    pub value: V,
}

/// Typed scan page with the host-opaque continuation cursor.
#[derive(Clone, Debug, PartialEq)]
pub struct Page<K, V> {
    /// Rows in host order.
    pub rows: Vec<Row<K, V>>,
    /// Continuation cursor, if more rows may follow.
    pub cursor: Option<String>,
}

/// Decode a raw page into typed rows. The first undecodable row fails the
/// whole page.
pub fn decode_page<K: FlowerType, V: FlowerType>(page: RawPage) -> Result<Page<K, V>> {
    let mut rows = Vec::with_capacity(page.rows.len());
    for row in page.rows {
        rows.push(Row {
            key: K::decode(row.key)?,
            value: V::decode(row.value)?,
        });
    }
    Ok(Page {
        rows,
        cursor: page.cursor,
    })
}

/// Call an exported method with typed arguments, decoding the result.
/// Failures poison the invocation, as with
/// [`MethodCtx::call`].
pub fn call_typed<A: FlowerType, T: FlowerType>(
    ctx: &mut impl MethodCtx,
    module: &str,
    name: &str,
    args: &A,
) -> Result<T> {
    let encoded = args.encode()?;
    let result = ctx.call(module, name, &encoded)?;
    T::decode(result)
}

/// Decode a savepoint-call outcome into a typed success or a declared
/// failure. Unrecognized failures convert back to themselves as `Err`.
///
/// Pairs with [`MethodCtx::try_call`], whose fatal [`CallError`] stays
/// explicit at the call site:
///
/// ```ignore
/// let outcome = ctx.try_call(module, name, &args.encode()?);
/// let outcome = match outcome {
///     Ok(outcome) => decode_outcome(outcome)?,
///     Err(CallError::Host(failure)) => return Err(failure),
/// };
/// ```
pub fn decode_outcome<T: FlowerType, E: BusinessError>(
    outcome: CallOutcome<Value>,
) -> Result<CallOutcome<T, E>> {
    match outcome {
        CallOutcome::Success(value) => Ok(CallOutcome::Success(T::decode(value)?)),
        CallOutcome::Failure(failure) => Ok(CallOutcome::Failure(E::from_failure(&failure)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Failure, Result, codes};
    use crate::schema::{Schema, v};
    use crate::value::Value;
    use alloc::boxed::Box;
    use alloc::string::String;
    use alloc::vec;

    fn round_trip<T: FlowerType + PartialEq + core::fmt::Debug>(value: T) {
        let encoded = value.encode().unwrap();
        assert!(T::SCHEMA.is(&encoded));
        assert_eq!(T::decode(encoded).unwrap(), value);
    }

    #[test]
    fn scalar_round_trips() {
        round_trip(());
        round_trip(true);
        round_trip(String::from("hello"));
        round_trip(1.5f64);
        round_trip(-0.0f64);
        round_trip(3i8);
        round_trip(-129i16 + 1);
        round_trip(i32::MIN);
        round_trip(u32::MAX);
        round_trip(MAX_SAFE_INTEGER);
        round_trip(-MAX_SAFE_INTEGER);
        round_trip(MAX_SAFE_INTEGER as u64);
    }

    #[test]
    fn wide_integers_reject_instead_of_rounding() {
        assert!(i64::encode(&MAX_SAFE_INTEGER).is_ok());
        assert!(i64::encode(&(MAX_SAFE_INTEGER + 1)).is_err());
        assert!(u64::encode(&(MAX_SAFE_INTEGER as u64)).is_ok());
        assert!(u64::encode(&(MAX_SAFE_INTEGER as u64 + 1)).is_err());
        assert!(i64::decode(Value::Number(9_007_199_254_740_992.0)).is_err());
        assert!(i32::decode(Value::Number(1.5)).is_err());
        assert!(u8::decode(Value::Number(256.0)).is_err());
    }

    #[test]
    fn decimals_cover_the_full_range() {
        round_trip(DecimalI64(i64::MIN));
        round_trip(DecimalI64(i64::MAX));
        round_trip(DecimalU64(u64::MAX));
        assert_eq!(
            DecimalI64(i64::MIN).encode().unwrap(),
            Value::String(String::from("-9223372036854775808"))
        );
        // Leading zeros decode but never encode.
        assert_eq!(
            DecimalI64::decode(Value::String(String::from("007"))).unwrap(),
            DecimalI64(7)
        );
        for bad in ["", "-", "+5", " 5", "5 ", "0x10", "1.0", "abc"] {
            assert!(DecimalI64::decode(Value::String(String::from(bad))).is_err());
        }
        assert!(DecimalI64::decode(Value::String(String::from("9223372036854775808"))).is_err());
        assert!(DecimalU64::decode(Value::String(String::from("-1"))).is_err());
    }

    #[test]
    fn non_finite_floats_fail_both_ways() {
        assert!(f64::encode(&f64::NAN).is_err());
        assert!(f64::decode(Value::Number(f64::INFINITY)).is_err());
    }

    #[test]
    fn option_models_visible_null_not_absent_members() {
        assert_eq!(<Option<i32>>::decode(Value::Null).unwrap(), None::<i32>);
        assert_eq!(<Option<i32>>::decode(Value::Number(1.0)).unwrap(), Some(1));
        assert_eq!(None::<i32>.encode().unwrap(), Value::Null);
        const { assert!(!<Option<i32>>::OPTIONAL_FIELD) };
        assert!(<Option<i32>>::decode_field(None).is_err());
    }

    #[test]
    fn field_models_redaction() {
        const { assert!(<Field<i32>>::OPTIONAL_FIELD) };
        assert_eq!(<Field<i32>>::decode_field(None).unwrap(), Field::Redacted);
        assert_eq!(
            <Field<i32>>::decode(Value::Number(2.0)).unwrap(),
            Field::Visible(2)
        );
        assert_eq!(Field::Visible(2i32).encode().unwrap(), Value::Number(2.0));
        let failure = Field::<i32>::Redacted.encode().unwrap_err();
        assert_eq!(failure.code, codes::REDACTED_VALUE);
    }

    #[test]
    fn containers_round_trip_and_check_arity() {
        round_trip(vec![1i32, 2, 3]);
        round_trip(Box::new(String::from("boxed")));
        round_trip((1i32,));
        round_trip((true, String::from("x")));
        round_trip((1u8, 2u16, 3u32));
        round_trip((1i64, 2i64, 3i64, 4i64));
        assert!(<(i32, i32)>::decode(Value::Array(vec![Value::Number(1.0)])).is_err());
        assert!(<Vec<i32>>::decode(Value::Null).is_err());
    }

    #[test]
    fn value_passthrough_validates() {
        let value = Value::Bool(true);
        assert_eq!(Value::decode(value.clone()).unwrap(), value);
        assert!(Value::decode(Value::Number(f64::NAN)).is_err());
    }

    #[test]
    fn missing_field_names_the_type() {
        let failure = <Vec<i32>>::decode_field(None).unwrap_err();
        assert_eq!(failure.code, codes::MISSING_FIELD);
        assert!(failure.message.contains("Vec"));
    }

    #[test]
    fn failure_is_its_own_business_error() {
        let failure = Failure::new("busy", "try again");
        let round = Failure::from_failure(&failure.clone().into_failure()).unwrap();
        assert_eq!(round, failure);
    }

    #[test]
    fn keys_render_canonically() {
        let text = |key: &dyn Fn() -> Result<Value>| key().and_then(|value| value.canonical_json());
        assert_eq!(text(&|| String::from("a").encode()).unwrap(), "\"a\"");
        assert_eq!(text(&|| 42u64.encode()).unwrap(), "42");
        assert_eq!(text(&|| true.encode()).unwrap(), "true");
        assert!(text(&|| f64::NAN.encode()).is_err());
    }

    #[test]
    fn pages_decode_row_wise() {
        use crate::ctx::{RawPage, RawRow};

        let page = RawPage {
            rows: alloc::vec![RawRow {
                key: Value::String(String::from("k")),
                value: Value::Number(2.0),
            },],
            cursor: Some(String::from("next")),
        };
        let decoded = decode_page::<String, f64>(page).unwrap();
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(decoded.rows[0].key, "k");
        assert_eq!(decoded.rows[0].value, 2.0);
        assert_eq!(decoded.cursor.as_deref(), Some("next"));

        let bad = RawPage {
            rows: alloc::vec![RawRow {
                key: Value::String(String::from("k")),
                value: Value::Bool(true),
            }],
            cursor: None,
        };
        assert!(decode_page::<String, f64>(bad).is_err());
    }

    #[derive(Debug, PartialEq)]
    struct TestError(String);

    impl BusinessError for TestError {
        const SCHEMA: Schema = v::json();

        fn into_failure(self) -> Failure {
            Failure::new("test", self.0)
        }

        fn from_failure(failure: &Failure) -> Result<Self> {
            if failure.code == "test" {
                Ok(TestError(failure.message.clone()))
            } else {
                Err(failure.clone())
            }
        }
    }

    #[test]
    fn outcomes_decode_successes_and_declared_failures() {
        let success: CallOutcome<f64, TestError> =
            decode_outcome(CallOutcome::Success(Value::Number(1.0))).unwrap();
        assert_eq!(success, CallOutcome::Success(1.0));

        let declared: CallOutcome<f64, TestError> =
            decode_outcome(CallOutcome::Failure(Failure::new("test", "nope"))).unwrap();
        assert_eq!(
            declared,
            CallOutcome::Failure(TestError(String::from("nope")))
        );

        let foreign: Result<CallOutcome<f64, TestError>> =
            decode_outcome(CallOutcome::Failure(Failure::new("other", "x")));
        assert_eq!(foreign.unwrap_err().code, "other");

        let bad: Result<CallOutcome<f64, TestError>> =
            decode_outcome(CallOutcome::Success(Value::Bool(true)));
        assert_eq!(bad.unwrap_err().code, codes::TYPE_MISMATCH);
    }
}
