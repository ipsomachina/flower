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

pub fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

pub fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

pub fn str_schema() -> Value {
    obj(vec![("type", s("string"))])
}

pub fn json_schema() -> Value {
    obj(vec![("type", s("json"))])
}

pub fn k(name: &str) -> String {
    format!("\"{name}\"")
}

pub fn index(name: &str, fields: Vec<&str>) -> Value {
    obj(vec![
        ("name", s(name)),
        ("fields", arr(fields.into_iter().map(s).collect())),
    ])
}

pub mod puts;
