//! Conversion between JSON and the guest's bounded, canonical value model.
use flower_sdk::{Value, value::MAX_VALUE_DEPTH};

pub fn from_json(json: &serde_json::Value) -> Result<Value, String> {
    convert(json, 0)
}
fn convert(json: &serde_json::Value, depth: usize) -> Result<Value, String> {
    if depth > MAX_VALUE_DEPTH {
        return Err("JSON exceeds depth bound".into());
    }
    Ok(match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => Value::Number(
            n.as_f64()
                .filter(|n| n.is_finite())
                .ok_or("nonfinite JSON number")?,
        ),
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(v) => Value::Array(
            v.iter()
                .map(|v| convert(v, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        serde_json::Value::Object(v) => Value::Object(
            v.iter()
                .map(|(k, v)| Ok((k.clone(), convert(v, depth + 1)?)))
                .collect::<Result<_, String>>()?,
        ),
    })
}
pub fn to_json(value: &Value) -> Result<serde_json::Value, String> {
    let text = value.canonical_json().map_err(|e| e.message)?;
    serde_json::from_str(&text).map_err(|e| e.to_string())
}
