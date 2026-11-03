extern crate alloc;

use flower_sdk::{
    FlowerType, SqlBudget, Value,
    typed::{DecimalI64, Omittable},
};
use flower_sql::{Access, Index, Plan};

#[derive(FlowerType)]
struct Record {
    owner: String,
    status: Option<String>,
    money: DecimalI64,
}
#[derive(FlowerType)]
struct Params {
    id: String,
    owner: Option<String>,
    min: DecimalI64,
}
#[derive(FlowerType)]
struct Row {
    id: String,
    owner: Omittable<String>,
}
fn params(owner: Option<&str>) -> Value {
    Params {
        id: "id".into(),
        owner: owner.map(Into::into),
        min: DecimalI64(7),
    }
    .encode()
    .unwrap()
}
fn bind(predicate: &str) -> Plan {
    Plan::bind(
        &format!("SELECT _key AS id, owner FROM items WHERE {predicate} LIMIT 5"),
        String::SCHEMA,
        Record::SCHEMA,
        Params::SCHEMA,
        Row::SCHEMA,
        SqlBudget::scan(5, 10_000, 100_000),
        true,
    )
    .unwrap()
}
fn index(name: &str, fields: &[&str]) -> Index {
    Index {
        name: name.into(),
        fields: fields.iter().map(|f| (*f).into()).collect(),
    }
}

#[test]
fn only_required_equalities_select_primary_keys() {
    for predicate in [
        "_key = :id",
        ":id = _key AND owner = :owner",
        "(owner = :owner OR status = 'open') AND _key = :id",
    ] {
        let plan = bind(predicate).with_indexes(&[], false).unwrap();
        let p = params(Some("ada"));
        assert_eq!(
            plan.access(&p).unwrap(),
            Access::PrimaryKey(p.as_object().unwrap().get("id").unwrap())
        );
        assert_eq!(
            plan.explain().as_object().unwrap()["candidate_bound"],
            Value::Number(1.0)
        );
    }
    for predicate in [
        "_key = :id OR owner = :owner",
        "NOT _key = :id",
        "_key >= :id",
        "owner = _key",
    ] {
        assert!(
            bind(predicate).with_indexes(&[], false).is_err(),
            "{predicate}"
        );
        assert_eq!(
            bind(predicate)
                .with_indexes(&[], true)
                .unwrap()
                .access(&params(None))
                .unwrap(),
            Access::CollectionScan
        );
    }
}

#[test]
fn every_index_component_needs_an_exact_required_equality() {
    let catalog = [index("owner_status", &["owner", "status"])];
    assert!(
        bind("owner = :owner")
            .with_indexes(&catalog, false)
            .is_err()
    );
    let plan = bind("status = 'open' AND :owner = owner")
        .with_indexes(&catalog, false)
        .unwrap();
    let p = params(Some("ada"));
    let Access::IndexEquality { index, components } = plan.access(&p).unwrap() else {
        panic!()
    };
    assert_eq!(index, "owner_status");
    assert_eq!(
        components.into_iter().cloned().collect::<Vec<_>>(),
        [Value::from("ada"), Value::from("open")]
    );
    assert_eq!(plan.access(&params(None)).unwrap(), Access::Empty);
    assert_eq!(
        plan.explain().as_object().unwrap()["input_bound_proven"],
        Value::Bool(false)
    );
}

#[test]
fn decimal_byte_aliases_never_use_a_legacy_equality_index() {
    assert!(
        bind("money = :min")
            .with_indexes(&[index("money", &["money"])], false)
            .is_err()
    );
    #[derive(FlowerType)]
    struct KeyRow {
        id: DecimalI64,
    }
    #[derive(FlowerType)]
    struct KeyParam {
        id: DecimalI64,
    }
    let plan = Plan::bind(
        "SELECT _key AS id FROM items WHERE _key = :id LIMIT 5",
        DecimalI64::SCHEMA,
        Record::SCHEMA,
        KeyParam::SCHEMA,
        KeyRow::SCHEMA,
        SqlBudget::scan(5, 10_000, 100_000),
        false,
    )
    .unwrap();
    assert!(plan.with_indexes(&[], false).is_err());
}

#[test]
fn index_choice_is_catalog_order_independent_and_primary_keys_win() {
    let a = index("a_owner", &["owner"]);
    let z = index("z_owner", &["owner"]);
    let p = params(Some("ada"));
    let first = bind("owner = :owner")
        .with_indexes(&[z.clone(), a.clone()], false)
        .unwrap();
    let second = bind("owner = :owner")
        .with_indexes(&[a.clone(), z], false)
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first.explain().as_object().unwrap()["index"],
        Value::from("a_owner")
    );
    assert!(!first.explain().canonical_json().unwrap().contains("ada"));
    let point = bind("owner = :owner AND _key = :id")
        .with_indexes(&[a], false)
        .unwrap();
    assert!(matches!(point.access(&p).unwrap(), Access::PrimaryKey(_)));
}

#[test]
fn parameter_contracts_and_byte_limits_apply_before_access_binding() {
    let plan = bind("owner = :owner")
        .with_indexes(&[index("owner", &["owner"])], false)
        .unwrap();
    assert!(plan.access(&Value::Null).is_err());
    assert!(plan.access(&params(Some(&"x".repeat(10_000)))).is_err());
    let plan = bind("_key = NULL").with_indexes(&[], false).unwrap();
    assert_eq!(plan.access(&params(None)).unwrap(), Access::Empty);
}
