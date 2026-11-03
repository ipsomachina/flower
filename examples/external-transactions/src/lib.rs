//! External values and cross-database transactions as a Flower guest.
//!
//! - `docs` holds texts keyed by ID, `counters` named totals.
//! - `words` is an external value over one document: its input is the
//!   document's text (nothing to compute while it is empty), and its
//!   published result must be `{count}` with a whole count. A
//!   reconcile worker keeps one key current.
//! - `tracked` is a pool over `docs`: every changed document is stale
//!   work until a worker publishes for its current text; a removed
//!   document takes its result with it.
//! - `put` (`{id, text}`), `remove` (a document ID) and `add` (`{name, by}`)
//!   are plain mutations that a client transaction can combine across
//!   databases in one group (Flower runs cross-database transactions as
//!   client-planned steps, one log entry, all or nothing).
extern crate alloc;

use alloc::string::String;

use flower_sdk::schema::{Schema, v};
use flower_sdk::{
    App, Ctx, FlowerType, MutationCtx, QueryCtx, Result, Value, collection, external, fail,
    mutation, query,
};

/// A document.
#[derive(FlowerType, Clone, Debug, PartialEq)]
pub struct Doc {
    pub text: String,
}

#[collection(name = "docs", key = String, value = Doc)]
pub struct Docs;

#[collection(name = "counters", key = String, value = f64)]
pub struct Counters;

/// A document's text, or `None` when it is missing or empty.
fn text_of(ctx: &mut Ctx<'_, '_>, id: Value) -> Result<Option<String>> {
    let Value::String(id) = id else {
        return Err(fail("INVALID_ARGUMENT", "document IDs are strings"));
    };
    Ok(Docs::get(ctx, &id)?
        .map(|doc| doc.text)
        .filter(|text| !text.is_empty()))
}

/// `words` input: `{text}` while the document has text.
fn words_input(ctx: &mut Ctx<'_, '_>, id: Value) -> Result<Value> {
    match text_of(ctx, id)? {
        Some(text) => Doc { text }.encode(),
        None => Ok(Value::Null),
    }
}

static WORDS_RESULT: Schema = v::object(&[v::field("count", v::int().min(0).build())]);
external!(pub static WORDS = "words", input = words_input, result = WORDS_RESULT, http);

/// `tracked` input: the text itself.
fn tracked_input(ctx: &mut Ctx<'_, '_>, id: Value) -> Result<Value> {
    Ok(text_of(ctx, id)?.map_or(Value::Null, Value::String))
}

external!(pub static TRACKED = "tracked", input = tracked_input, tracks = "docs", http);

#[mutation(http)]
fn put(ctx: &mut MutationCtx<'_, '_>, id: String, text: String) -> Result<()> {
    Docs::set(ctx, &id, &Doc { text })
}

#[mutation(http, bare_arg)]
fn remove(ctx: &mut MutationCtx<'_, '_>, id: String) -> Result<()> {
    Docs::delete(ctx, &id)
}

/// Add `by` to a named counter, returning the new total.
#[mutation(http)]
fn add(ctx: &mut MutationCtx<'_, '_>, name: String, by: f64) -> Result<f64> {
    let total = Counters::get(ctx, &name)?.unwrap_or(0.0) + by;
    Counters::set(ctx, &name, &total)?;
    Ok(total)
}

/// A counter's total; zero before its first addition.
#[query(http, bare_arg)]
fn counter(ctx: &mut QueryCtx<'_, '_>, name: String) -> Result<f64> {
    Ok(Counters::get(ctx, &name)?.unwrap_or(0.0))
}

#[query(name = "readWords", http, bare_arg)]
fn read_words(ctx: &mut QueryCtx<'_, '_>, id: String) -> Result<Value> {
    WORDS.state(ctx, &Value::String(id))
}

#[query(name = "readTracked", http, bare_arg)]
fn read_tracked(ctx: &mut QueryCtx<'_, '_>, id: String) -> Result<Value> {
    TRACKED.state(ctx, &Value::String(id))
}

pub static APP: App = App::new(&[
    &Docs,
    &Counters,
    &WORDS,
    &TRACKED,
    &PUT,
    &REMOVE,
    &ADD,
    &COUNTER,
    &READ_WORDS,
    &READ_TRACKED,
]);

flower_sdk::export!(APP);

#[cfg(test)]
mod tests {
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;

    use flower_sdk::Value;
    use flower_sdk::abi::{InvocationKind, invoke_native, manifest_native};

    fn object(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(name, value)| (String::from(*name), value.clone()))
                .collect(),
        )
    }

    /// The code a mutation fails with before it touches the host.
    fn refused(name: &str, args: Value) -> String {
        let input = Value::Array(vec![Value::from(name), args])
            .encode_binary()
            .unwrap();
        let output = invoke_native(&super::APP, InvocationKind::Mutation as u32, &input);
        let Value::Array(envelope) = Value::decode_binary(&output).unwrap() else {
            panic!("response envelope");
        };
        assert_eq!(envelope[0], Value::from("error"), "{name} succeeded");
        match envelope[1].as_object().unwrap().get("code") {
            Some(Value::String(code)) => code.clone(),
            other => panic!("code not a string: {other:?}"),
        }
    }

    #[test]
    fn bad_arguments_answer_invalid_argument() {
        for (name, args) in [
            (
                "put",
                object(&[("id", Value::Number(1.0)), ("text", Value::from("x"))]),
            ),
            ("put", object(&[("id", Value::from("a"))])),
            (
                "add",
                object(&[("name", Value::from("a")), ("by", Value::from("x"))]),
            ),
            ("remove", Value::Number(5.0)),
        ] {
            assert_eq!(refused(name, args), "INVALID_ARGUMENT", "{name}");
        }
    }

    /// Only explicitly declared HTTP methods are exposed: the plain methods
    /// and both external values' worker operations.
    #[test]
    fn the_example_answers_over_http_as_declared() {
        let bytes = manifest_native(&super::APP);
        let Value::Array(envelope) = Value::decode_binary(&bytes).unwrap() else {
            panic!("manifest envelope");
        };
        let Some(Value::Array(http)) = envelope[1].as_object().unwrap().get("http").cloned() else {
            panic!("manifest without http");
        };
        let mut aliases: Vec<String> = http
            .iter()
            .map(|entry| match entry.as_object().unwrap().get("alias") {
                Some(Value::String(alias)) => alias.clone(),
                other => panic!("alias not a string: {other:?}"),
            })
            .collect();
        aliases.sort();
        let mut expected: Vec<String> = [
            "put",
            "remove",
            "add",
            "readWords",
            "readTracked",
            "counter",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        for value in ["words", "tracked"] {
            for operation in [
                "get", "pending", "publish", "next", "claim", "renew", "release", "ready", "stats",
            ] {
                expected.push(alloc::format!("{value}.{operation}"));
            }
        }
        expected.sort();
        assert_eq!(aliases, expected);
    }
}
