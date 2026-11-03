//! Reusable values, schemas and host calls for this crate's integration tests.

// Each test binary uses a different subset of these fixtures.
#![allow(dead_code)]

use std::time::Duration;

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

pub fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

pub fn k(name: &str) -> String {
    format!("\"{name}\"")
}

pub fn field(args: &Value, name: &str) -> Value {
    match args {
        Value::Object(map) => map.get(name).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

pub async fn read_dispatch(response: &mut reqwest::Response) -> String {
    let mut buffered = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let Some(chunk) = response.chunk().await.unwrap() else {
                panic!("stream ended mid-dispatch");
            };
            buffered.extend_from_slice(&chunk);
            if buffered.windows(2).any(|pair| pair == b"\n\n") {
                return;
            }
        }
    })
    .await
    .unwrap();
    String::from_utf8(buffered).unwrap()
}

pub async fn read_until_end(response: &mut reqwest::Response, needle: &str) -> String {
    let mut buffered = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(chunk) = response.chunk().await.unwrap() {
            buffered.extend_from_slice(&chunk);
        }
    })
    .await
    .expect("the stream never ended");
    let text = String::from_utf8(buffered).unwrap();
    assert!(text.contains(needle), "{text}");
    text
}
