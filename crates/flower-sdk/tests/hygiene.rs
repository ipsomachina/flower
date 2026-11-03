//! The method macros' generated code compiles and dispatches whatever
//! the parameters are called: a parameter named like its function, raw
//! identifiers.

extern crate alloc;

use flower_sdk::abi::{InvocationKind, invoke_native};
use flower_sdk::{App, MutationCtx, QueryCtx, Value, mutation, query};

/// One parameter named like the function, as a bare argument.
#[query(bare_arg)]
fn shop(ctx: &mut QueryCtx<'_, '_>, shop: String) -> String {
    let _ = ctx;
    shop
}

/// A parameter named like the function among others.
#[query]
fn tip(ctx: &mut QueryCtx<'_, '_>, tip: u64, by: u64) -> u64 {
    let _ = ctx;
    tip + by
}

/// Raw identifiers for the function and a parameter.
#[mutation(rename_all = "camelCase")]
fn r#move(ctx: &mut MutationCtx<'_, '_>, r#type: String, lease_ms: u64) -> String {
    let _ = ctx;
    format!("{type}:{lease_ms}", type = r#type)
}

static APP: App = App::new(&[&SHOP, &TIP, &MOVE]);

fn invoke(kind: InvocationKind, name: &str, args: Value) -> Value {
    let input = Value::Array(vec![Value::from(name), args])
        .encode_binary()
        .unwrap();
    let output = invoke_native(&APP, kind as u32, &input);
    let Value::Array(envelope) = Value::decode_binary(&output).unwrap() else {
        panic!("response envelope");
    };
    assert_eq!(envelope[0], Value::from("ok"), "{name}: {:?}", envelope[1]);
    envelope[1].clone()
}

fn object(pairs: &[(&str, Value)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(name, value)| (String::from(*name), value.clone()))
            .collect(),
    )
}

#[test]
fn parameters_named_like_their_function_dispatch() {
    assert_eq!(
        invoke(InvocationKind::Query, "shop", Value::from("north")),
        Value::from("north")
    );
    assert_eq!(
        invoke(
            InvocationKind::Query,
            "tip",
            object(&[("tip", Value::Number(2.0)), ("by", Value::Number(3.0))]),
        ),
        Value::Number(5.0)
    );
}

#[test]
fn raw_identifiers_are_plain_names() {
    let args = MoveArgs {
        r#type: String::from("pizza"),
        lease_ms: 5,
    };
    assert_eq!(MOVE.name, "move");
    assert_eq!(
        invoke(
            InvocationKind::Mutation,
            "move",
            object(&[
                ("type", Value::from("pizza")),
                ("leaseMs", Value::Number(5.0))
            ]),
        ),
        Value::from("pizza:5")
    );
    assert_eq!(args.lease_ms, 5);
}
