//! Reusable values, schemas and host calls for this crate's integration tests.

// Each test binary uses a different subset of these fixtures.
#![allow(dead_code)]

use flower_engine::runtime::HostCall;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value};

pub fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

pub fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

pub fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

pub fn n(num: f64) -> Value {
    Value::Number(num)
}

pub fn k(name: &str) -> String {
    format!("\"{name}\"")
}

pub fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

pub fn index(name: &str, fields: Vec<&str>) -> Value {
    obj(vec![
        ("name", s(name)),
        ("fields", arr(fields.into_iter().map(s).collect())),
    ])
}

pub fn str_schema() -> Value {
    obj(vec![("type", s("string"))])
}

pub fn num_schema() -> Value {
    obj(vec![("type", s("number"))])
}

pub fn json_schema() -> Value {
    obj(vec![("type", s("json"))])
}

pub fn field(name: &str, schema: Value, optional: bool) -> Value {
    obj(vec![
        ("name", s(name)),
        ("schema", schema),
        ("optional", Value::Bool(optional)),
    ])
}

pub fn object_schema(fields: Vec<Value>) -> Value {
    obj(vec![("type", s("object")), ("fields", arr(fields))])
}

/// The closure signature used by native test runtimes.
pub trait Guest:
    Fn(
        flower_engine::runtime::CallKind,
        &str,
        &str,
        &Value,
        &mut dyn HostCall,
    ) -> Result<Value, Failure>
    + Send
{
}
impl<F> Guest for F where
    F: Fn(
            flower_engine::runtime::CallKind,
            &str,
            &str,
            &Value,
            &mut dyn HostCall,
        ) -> Result<Value, Failure>
        + Send
{
}
