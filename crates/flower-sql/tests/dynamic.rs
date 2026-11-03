extern crate alloc;

use flower_sdk::{
    FlowerType, SqlBudget, Value,
    typed::{DecimalI64, Omittable},
};
use flower_sql::{Access, Index, Plan};

#[derive(FlowerType)]
struct Record {
    owner: String,
    money: DecimalI64,
    score: u32,
    note: Omittable<Option<String>>,
}
fn budget() -> SqlBudget {
    SqlBudget::scan(100, 100_000, 100_000)
}

#[test]
fn syntax_diagnostics_use_utf8_byte_offsets() {
    let sql = "SELECT 'é' AS name FROM items WHERE @ LIMIT 1";
    let error = bind(sql, &params(&[])).unwrap_err();
    let Value::Object(details) = error.details.unwrap() else {
        panic!("missing span")
    };
    assert_eq!(
        details["start_byte"],
        Value::Number(sql.find('@').unwrap() as f64)
    );
    let sql = "SELECT owner FROM items";
    let error = bind(sql, &params(&[])).unwrap_err();
    let Value::Object(details) = error.details.unwrap() else {
        panic!("missing span")
    };
    assert_eq!(details["start_byte"], Value::Number(sql.len() as f64));
}
fn params(pairs: &[(&str, Value)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect(),
    )
}
fn bind(sql: &str, params: &Value) -> flower_sdk::Result<Plan> {
    Plan::bind_dynamic(sql, String::SCHEMA, Record::SCHEMA, params, budget(), true)
}

#[test]
fn runtime_parameters_infer_exact_decimal_context_in_either_operand_order() {
    let p = params(&[
        ("min", Value::from("9007199254740992")),
        ("limit", Value::Number(2.0)),
    ]);
    for predicate in ["money >= :min", ":min <= money"] {
        let plan = bind(&format!("SELECT _key AS id, money FROM items WHERE {predicate} ORDER BY money DESC LIMIT :limit"),&p).unwrap();
        let mut eval = plan.evaluator(&p, 0).unwrap();
        for (id, money) in [
            ("a", 9_007_199_254_740_992),
            ("b", 9_007_199_254_740_993),
            ("c", 1),
        ] {
            eval.push(
                Value::from(id),
                Record {
                    owner: "ada".into(),
                    money: DecimalI64(money),
                    score: 1,
                    note: Omittable::Missing,
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        }
        let rows = eval.finish().unwrap();
        assert_eq!(rows[0].as_object().unwrap()["id"], Value::from("b"));
        assert_eq!(rows[1].as_object().unwrap()["id"], Value::from("a"));
    }
    assert!(
        bind(
            "SELECT money FROM items WHERE money = :min LIMIT 1",
            &params(&[("min", Value::Number(7.0))])
        )
        .is_err()
    );
    assert!(
        bind(
            "SELECT money FROM items WHERE money = :min LIMIT 1",
            &params(&[("min", Value::from("9223372036854775808"))])
        )
        .is_err()
    );
}

#[test]
fn runtime_projection_keeps_absence_distinct_from_null() {
    let p = Value::Null;
    let plan = bind("SELECT _key AS id, note FROM items LIMIT 5", &p).unwrap();
    let mut eval = plan.evaluator(&p, 0).unwrap();
    for (id, note) in [("a", Omittable::Missing), ("b", Omittable::Present(None))] {
        eval.push(
            Value::from(id),
            Record {
                owner: "ada".into(),
                money: DecimalI64(1),
                score: 1,
                note,
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    }
    let rows = eval.finish().unwrap();
    assert!(!rows[0].as_object().unwrap().contains_key("note"));
    assert_eq!(rows[1].as_object().unwrap()["note"], Value::Null);
}

#[test]
fn runtime_names_types_and_incompatible_parameter_contexts_fail_closed() {
    for (sql, p) in [
        (
            "SELECT owner FROM items WHERE owner = :owner LIMIT 1",
            Value::Null,
        ),
        (
            "SELECT owner FROM items LIMIT 1",
            params(&[("unused", Value::from("x"))]),
        ),
        ("SELECT unknown FROM items LIMIT 1", Value::Null),
        (
            "SELECT owner FROM items WHERE owner = :p AND money = :p LIMIT 1",
            params(&[("p", Value::from("7"))]),
        ),
        (
            "SELECT owner FROM items WHERE score >= :p LIMIT 1",
            params(&[("p", Value::from("7"))]),
        ),
        (
            "SELECT owner FROM items LIMIT :limit",
            params(&[("limit", Value::Number(0.0))]),
        ),
    ] {
        assert!(bind(sql, &p).is_err(), "{sql}");
    }
}

#[test]
fn runtime_numeric_comparison_does_not_inherit_column_range_constraints() {
    let p = params(&[("score", Value::Number(1.5))]);
    assert!(bind("SELECT score FROM items WHERE score >= :score LIMIT 1", &p).is_ok());
    let p = params(&[("score", Value::Number(1e20))]);
    assert!(bind("SELECT score FROM items WHERE score >= :score LIMIT 1", &p).is_ok());
}

#[test]
fn runtime_null_and_index_planning_reuse_the_same_coverage_rules() {
    let indexes = [Index {
        name: "by_owner".into(),
        fields: vec!["owner".into()],
    }];
    let p = params(&[("owner", Value::from("ada"))]);
    let plan = bind("SELECT owner FROM items WHERE owner = :owner LIMIT 5", &p)
        .unwrap()
        .with_indexes(&indexes, false)
        .unwrap();
    assert!(matches!(
        plan.access(&p).unwrap(),
        Access::IndexEquality { .. }
    ));
    assert_eq!(
        plan.access(&params(&[("owner", Value::Null)])).unwrap(),
        Access::Empty
    );
    assert!(
        bind("SELECT * FROM items LIMIT 5", &Value::Null)
            .unwrap()
            .with_indexes(&indexes, false)
            .is_err()
    );
}

#[test]
fn request_owned_binding_supports_repeated_different_statements() {
    // All names and maps are owned by each Plan; no Schema::Object/Box::leak
    // construction is needed for per-request contracts.
    for i in 0..256 {
        let plan = bind(
            &format!("SELECT owner AS alias_{i} FROM items LIMIT 1"),
            &Value::Null,
        )
        .unwrap();
        let mut eval = plan.evaluator(&Value::Null, 0).unwrap();
        eval.push(
            Value::from("a"),
            Record {
                owner: "ada".into(),
                money: DecimalI64(1),
                score: 1,
                note: Omittable::Missing,
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
        assert!(
            eval.finish().unwrap()[0]
                .as_object()
                .unwrap()
                .contains_key(&format!("alias_{i}"))
        );
    }
}
