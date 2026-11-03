extern crate alloc;
use flower_sdk::{FlowerType, Nullable, Omittable, Value};

#[derive(FlowerType, Debug, PartialEq)]
struct Patch {
    title: Omittable<Nullable<alloc::string::String>>,
}

#[test]
fn omission_null_and_value_are_distinct_and_round_trip() {
    let missing = Patch {
        title: Omittable::Missing,
    };
    assert_eq!(missing.encode().unwrap(), Value::Object(Default::default()));
    assert_eq!(Patch::decode(missing.encode().unwrap()).unwrap(), missing);
    let null = Patch {
        title: Omittable::Present(Nullable(None)),
    };
    assert_eq!(Patch::decode(null.encode().unwrap()).unwrap(), null);
    let present = Patch {
        title: Omittable::Present(Nullable(Some("hello".into()))),
    };
    assert_eq!(Patch::decode(present.encode().unwrap()).unwrap(), present);
    assert!(
        <Omittable<Nullable<alloc::string::String>>>::Missing
            .encode()
            .is_err()
    );
}

#[derive(FlowerType)]
struct Required {
    title: Nullable<alloc::string::String>,
}
#[test]
fn nullable_does_not_make_a_required_field_optional() {
    let _: Omittable<Required> = Default::default();
    assert!(Required::decode(Value::Object(Default::default())).is_err());
}

#[flower_sdk::interface(name="counter.v1",handle=CounterApi)]
pub trait Counter {
    #[query]
    fn read(
        ctx: &mut flower_sdk::QueryCtx<'_, '_>,
        key: alloc::string::String,
    ) -> flower_sdk::Result<u64>;
    #[mutation]
    fn add(
        ctx: &mut flower_sdk::MutationCtx<'_, '_>,
        key: alloc::string::String,
    ) -> flower_sdk::Result<u64>;
}
#[test]
fn contract_descriptors_need_no_provider_implementation() {
    let _query: flower_sdk::QueryOp<CounterReadArgs, u64> = CounterApi::READ;
    let _write: flower_sdk::MutationOp<CounterAddArgs, u64> = CounterApi::ADD;
    let args = CounterReadArgs { key: "a".into() };
    assert!(args.encode().is_ok());
}
