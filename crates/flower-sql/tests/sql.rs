extern crate alloc;

use core::cmp::Ordering;
use flower_sdk::{
    FlowerType, SqlBudget, Value,
    typed::{DecimalI64, DecimalU64, Omittable},
};
use flower_sql::{Plan, SQL_BUDGET, SQL_INVALID, compare};

#[derive(FlowerType)]
struct Record {
    name: String,
    score: i32,
    money: DecimalI64,
    note: Omittable<Option<String>>,
}
#[derive(FlowerType, Debug, PartialEq)]
struct Row {
    id: String,
    name: String,
    money: DecimalI64,
}
#[derive(FlowerType)]
struct Params {
    min: DecimalI64,
    limit: u32,
}

fn budget() -> SqlBudget {
    SqlBudget::scan(100, 100_000, 100_000)
}
fn plan(sql: &str) -> Plan {
    Plan::bind(
        sql,
        String::SCHEMA,
        Record::SCHEMA,
        Params::SCHEMA,
        Row::SCHEMA,
        budget(),
        false,
    )
    .unwrap()
}
fn record(name: &str, score: i32, money: i64) -> Value {
    Record {
        name: name.into(),
        score,
        money: DecimalI64(money),
        note: Omittable::Missing,
    }
    .encode()
    .unwrap()
}

#[test]
fn typed_parameters_logical_sort_and_total_limit() {
    let plan = plan(
        "SELECT _key AS id, name, money FROM items WHERE money >= :min AND NOT score < 0 ORDER BY money DESC, name ASC LIMIT :limit",
    );
    let params = Params {
        min: DecimalI64(-100),
        limit: 2,
    }
    .encode()
    .unwrap();
    let mut eval = plan.evaluator(&params, 0).unwrap();
    for (id, name, score, money) in [
        ("z", "aa", 1, 9),
        ("a", "b", 1, 10),
        ("b", "x", -1, 100),
        ("c", "b", 1, 10),
        ("d", "zero", 1, -1),
    ] {
        eval.push(Value::from(id), record(name, score, money))
            .unwrap();
    }
    let result: Vec<Row> = eval
        .finish()
        .unwrap()
        .into_iter()
        .map(|v| Row::decode(v).unwrap())
        .collect();
    assert_eq!(
        result,
        vec![
            Row {
                id: "a".into(),
                name: "b".into(),
                money: DecimalI64(10)
            },
            Row {
                id: "c".into(),
                name: "b".into(),
                money: DecimalI64(10)
            }
        ]
    );
}

#[derive(FlowerType)]
struct Maybe {
    note: Omittable<Option<String>>,
}
#[test]
fn explicit_null_and_absence_have_different_predicate_semantics() {
    for (predicate, expected) in [
        ("note IS NULL", 1),
        ("note IS NOT NULL", 1),
        ("NOT note = 'x'", 0),
        ("note = 'x' OR note IS NULL", 2),
    ] {
        let plan = Plan::bind(
            &format!("SELECT note FROM items WHERE {predicate} LIMIT 10"),
            String::SCHEMA,
            Maybe::SCHEMA,
            <()>::SCHEMA,
            Maybe::SCHEMA,
            budget(),
            false,
        )
        .unwrap();
        let mut eval = plan.evaluator(&Value::Null, 0).unwrap();
        for (id, note) in [
            ("absent", Omittable::Missing),
            ("null", Omittable::Present(None)),
            ("text", Omittable::Present(Some("x".into()))),
        ] {
            eval.push(Value::from(id), Maybe { note }.encode().unwrap())
                .unwrap();
        }
        assert_eq!(eval.finish().unwrap().len(), expected, "{predicate}");
    }
}

#[test]
fn decimal_and_tuple_order_never_rounds_through_f64() {
    for (schema, a, b) in [
        (DecimalI64::SCHEMA, "-9223372036854775808", "-1"),
        (DecimalI64::SCHEMA, "9007199254740992", "9007199254740993"),
        (
            DecimalU64::SCHEMA,
            "9007199254740993",
            "18446744073709551615",
        ),
    ] {
        assert_eq!(
            compare(&Value::from(a), &Value::from(b), schema).unwrap(),
            Ordering::Less
        );
    }
    assert_eq!(
        compare(&Value::from("aa"), &Value::from("b"), String::SCHEMA).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare(&Value::Number(-1.0), &Value::Number(9.0), f64::SCHEMA).unwrap(),
        Ordering::Less
    );
    let schema = <(DecimalU64, String)>::SCHEMA;
    let a = (DecimalU64(9), String::from("z")).encode().unwrap();
    let b = (DecimalU64(10), String::from("a")).encode().unwrap();
    assert_eq!(compare(&a, &b, schema).unwrap(), Ordering::Less);
    // Old manifests that declare plain strings retain lexical semantics.
    assert_eq!(
        compare(&Value::from("10"), &Value::from("9"), String::SCHEMA).unwrap(),
        Ordering::Less
    );
}

#[test]
fn equal_logical_decimal_keys_have_a_deterministic_final_tie() {
    #[derive(FlowerType)]
    struct KeyRow {
        id: DecimalI64,
    }
    let p = Plan::bind(
        "SELECT _key AS id FROM items LIMIT 10",
        DecimalI64::SCHEMA,
        <()>::SCHEMA,
        <()>::SCHEMA,
        KeyRow::SCHEMA,
        budget(),
        false,
    )
    .unwrap();
    for keys in [["7", "07", "-1", "10"], ["10", "-1", "07", "7"]] {
        let mut eval = p.evaluator(&Value::Null, 0).unwrap();
        for key in keys {
            eval.push(Value::from(key), Value::Null).unwrap();
        }
        let result = eval.finish().unwrap();
        let ids: Vec<_> = result
            .iter()
            .map(|r| r.as_object().unwrap()["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["-1", "07", "7", "10"]);
    }
}

#[test]
fn binder_rejects_unsupported_syntax_unknown_names_and_types() {
    for sql in [
        "DELETE FROM items LIMIT 1",
        "SELECT name FROM items",
        "SELECT name FROM other.items LIMIT 1",
        "SELECT name FROM items JOIN other ON items.id = other.id LIMIT 1",
        "SELECT name FROM items LIMIT 1 OFFSET 1",
        "SELECT name FROM items LIMIT 1; SELECT name FROM items LIMIT 1",
        "SELECT name FROM items ORDER BY name NULLS LAST LIMIT 1",
        "SELECT name FROM items GROUP BY name LIMIT 1",
        "SELECT DISTINCT name FROM items LIMIT 1",
        "SELECT name, name FROM items LIMIT 1",
        "SELECT money FROM items WHERE money > 3 LIMIT 1",
        "SELECT unknown FROM items LIMIT 1",
        "SELECT name FROM items WHERE score = :unknown LIMIT 1",
        "SELECT name FROM items LIMIT 0",
        "SELECT name FROM items LIMIT -1",
        "SELECT name FROM items LIMIT 1.5",
        "SELECT name FROM items WHERE name = 'broken LIMIT 1",
        "SELECT name FROM items LIMIT 1 --comment",
    ] {
        let error = Plan::bind(
            sql,
            String::SCHEMA,
            Record::SCHEMA,
            Params::SCHEMA,
            Row::SCHEMA,
            budget(),
            false,
        )
        .unwrap_err();
        assert_eq!(error.code, SQL_INVALID, "{sql}");
    }
    // Use a matching row contract so invalid comparisons reach type checking.
    #[derive(FlowerType)]
    struct Name {
        name: String,
    }
    for sql in [
        "SELECT name FROM items WHERE score = name LIMIT 1",
        "SELECT name FROM items WHERE money > '1' LIMIT 1",
        "SELECT name FROM items ORDER BY note LIMIT 1",
    ] {
        let bound = Plan::bind(
            sql,
            String::SCHEMA,
            Record::SCHEMA,
            Params::SCHEMA,
            Name::SCHEMA,
            budget(),
            false,
        );
        if sql.contains("ORDER BY note") {
            assert!(bound.is_ok());
        } else {
            assert!(bound.is_err());
        }
    }
}

#[test]
fn budgets_fail_without_partial_results_and_explain_does_not_claim_a_proof() {
    #[derive(FlowerType)]
    struct Name {
        name: String,
    }
    let make = |budget| {
        Plan::bind(
            "SELECT name FROM items ORDER BY name LIMIT 1",
            String::SCHEMA,
            Record::SCHEMA,
            <()>::SCHEMA,
            Name::SCHEMA,
            budget,
            false,
        )
        .unwrap()
    };
    let p = make(SqlBudget::scan(1, 10_000, 10_000));
    let mut eval = p.evaluator(&Value::Null, 0).unwrap();
    eval.push(Value::from("a"), record("a", 1, 1)).unwrap();
    assert_eq!(
        eval.push(Value::from("b"), record("b", 1, 1))
            .unwrap_err()
            .code,
        SQL_BUDGET
    );
    let p = make(SqlBudget::scan(100, 100, 10_000));
    assert!(p.evaluator(&Value::Null, 101).is_err());
    let mut eval = p.evaluator(&Value::Null, 0).unwrap();
    assert_eq!(
        eval.push(Value::from("a"), record("a", 1, 1))
            .unwrap_err()
            .code,
        SQL_BUDGET
    );
    let p = make(SqlBudget::scan(100, 10_000, 1));
    let mut eval = p.evaluator(&Value::Null, 0).unwrap();
    assert_eq!(
        eval.push(Value::from("a"), record("a", 1, 1))
            .unwrap_err()
            .code,
        SQL_BUDGET
    );
    let Value::Object(explain) = p.explain() else {
        panic!()
    };
    assert_eq!(explain["input_bound_proven"], Value::Bool(false));
    assert_eq!(explain["max_candidates"], Value::Number(100.0));
}

#[test]
fn policy_redaction_requires_optional_output_and_preserves_rows_for_sort() {
    #[derive(FlowerType)]
    struct Required {
        name: String,
    }
    #[derive(FlowerType)]
    struct Redacted {
        name: flower_sdk::typed::Field<String>,
    }
    let sql = "SELECT name FROM items ORDER BY name LIMIT 10";
    assert!(
        Plan::bind(
            sql,
            String::SCHEMA,
            Record::SCHEMA,
            <()>::SCHEMA,
            Required::SCHEMA,
            budget(),
            true
        )
        .is_err()
    );
    let p = Plan::bind(
        sql,
        String::SCHEMA,
        Record::SCHEMA,
        <()>::SCHEMA,
        Redacted::SCHEMA,
        budget(),
        true,
    )
    .unwrap();
    let mut eval = p.evaluator(&Value::Null, 0).unwrap();
    eval.push(
        Value::from("hidden_field"),
        Value::Object(Default::default()),
    )
    .unwrap();
    eval.push(Value::from("visible_field"), record("visible", 1, 1))
        .unwrap();
    let result = eval.finish().unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], Value::Object(Default::default()));
}

#[test]
fn nested_nullable_parents_and_literal_projection_contracts() {
    #[derive(FlowerType)]
    struct Inner {
        name: String,
    }
    #[derive(FlowerType)]
    struct Source {
        nested: Option<Inner>,
    }
    #[derive(FlowerType)]
    struct ResultRow {
        name: Option<String>,
        answer: u32,
    }
    let p = Plan::bind(
        "SELECT nested.name AS name, 42 AS answer FROM items LIMIT 1",
        String::SCHEMA,
        Source::SCHEMA,
        <()>::SCHEMA,
        ResultRow::SCHEMA,
        budget(),
        false,
    )
    .unwrap();
    let mut eval = p.evaluator(&Value::Null, 0).unwrap();
    eval.push(Value::from("a"), Source { nested: None }.encode().unwrap())
        .unwrap();
    assert_eq!(
        eval.finish().unwrap()[0].as_object().unwrap()["name"],
        Value::Null
    );
}

#[test]
fn bounded_parser_handles_untrusted_complexity() {
    for sql in [
        "x".repeat(16385),
        format!(
            "SELECT name FROM items WHERE {}name = 'a'{} LIMIT 1",
            "(".repeat(40),
            ")".repeat(40)
        ),
        format!(
            "SELECT name FROM items WHERE {} LIMIT 1",
            vec!["name = 'a'"; 200].join(" AND ")
        ),
    ] {
        assert!(flower_sql::collection(&sql).is_err());
    }
}

#[test]
fn repeated_large_projection_is_charged_before_values_are_cloned() {
    #[derive(FlowerType)]
    struct Repeated {
        a: String,
        b: String,
    }
    let p = Plan::bind(
        "SELECT name AS a, name AS b FROM items LIMIT 1",
        String::SCHEMA,
        Record::SCHEMA,
        <()>::SCHEMA,
        Repeated::SCHEMA,
        SqlBudget::scan(10, 1_500, 10_000),
        false,
    )
    .unwrap();
    let mut eval = p.evaluator(&Value::Null, 1_000).unwrap();
    assert_eq!(
        eval.push(Value::from("a"), record(&"x".repeat(500), 1, 1))
            .unwrap_err()
            .code,
        SQL_BUDGET
    );
}
