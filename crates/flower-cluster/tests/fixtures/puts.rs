//! A native key/value guest for membership and timing tests.

use super::{obj, s};
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Value, fail};

pub fn manifest() -> Value {
    let string = obj(vec![("type", s("string"))]);
    let tasks = obj(vec![
        ("name", s("tasks")),
        ("key", string),
        ("value", obj(vec![("type", s("json"))])),
        ("indexes", Value::Array(vec![])),
        ("references", Value::Array(vec![])),
    ]);
    let put = obj(vec![
        ("name", s("put")),
        ("kind", s("mutation")),
        ("args", obj(vec![("type", s("json"))])),
    ]);
    obj(vec![
        ("collections", Value::Array(vec![tasks])),
        ("definitions", Value::Array(vec![put])),
        ("triggers", Value::Array(vec![])),
        ("http", Value::Array(vec![])),
    ])
}

pub struct Puts;

impl RuntimeFactory for Puts {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                if !matches!((kind, name), (CallKind::Mutation, "put")) {
                    return Err(fail("UNKNOWN_METHOD", "unknown definition"));
                }
                let Value::Object(map) = args else {
                    return Err(fail("TYPE_MISMATCH", "args take an object"));
                };
                let key = map.get("key").cloned().unwrap_or(Value::Null);
                let value = map.get("value").cloned().unwrap_or(Value::Null);
                let input = Value::Array(vec![s(""), s("tasks"), key, value])
                    .encode_binary()
                    .unwrap();
                let output = host.op(HostOp::Set as u32 as u8, &input);
                decode_envelope(Value::decode_binary(&output).unwrap())?;
                Ok(Value::Null)
            },
        )))
    }
}
