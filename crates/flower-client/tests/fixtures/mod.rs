//! Reusable values, schemas and host calls for this crate's integration tests.

// Each test binary uses a different subset of these fixtures.
#![allow(dead_code)]

use flower_engine::runtime::HostCall;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value};

pub fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

pub fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}
