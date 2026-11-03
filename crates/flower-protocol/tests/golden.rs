use flower_protocol::*;
use serde_json::Value;

#[test]
fn published_golden_cases_match_the_runtime_contracts() {
    let cases: Vec<Value> = serde_json::from_str(GOLDEN_FIXTURES).unwrap();
    for case in cases {
        let payload = case["payload"].clone();
        let valid = match case["kind"].as_str().unwrap() {
            "query_request" => serde_json::from_value::<request::QueryRequest>(payload).is_ok(),
            "mutation_request" => {
                serde_json::from_value::<request::MutationRequest>(payload).is_ok()
            }
            "watch_request" => serde_json::from_value::<request::WatchRequest>(payload).is_ok(),
            "transaction_request" => {
                serde_json::from_value::<request::TransactionRequest>(payload).is_ok()
            }
            "call" | "mutation" => serde_json::from_value::<CallEnvelope>(payload)
                .ok()
                .is_some_and(|value| value.validate(case["kind"] == "mutation").is_ok()),
            "token" => serde_json::from_value::<ReadToken>(payload)
                .ok()
                .is_some_and(|value| value.validate().is_ok()),
            "history" => serde_json::from_value::<WatchHistory>(payload)
                .ok()
                .is_some_and(|value| value.validate().is_ok()),
            "problem" => serde_json::from_value::<Problem>(payload)
                .ok()
                .is_some_and(|value| value.validate().is_ok()),
            "sync" => serde_json::from_value::<SyncBatch<Value>>(payload)
                .ok()
                .is_some_and(|value| value.validate(case["since"].as_u64().unwrap()).is_ok()),
            "discovery" => serde_json::from_value::<Discovery>(payload).is_ok(),
            kind => panic!("unrecognized golden fixture kind {kind}"),
        };
        assert_eq!(valid, case["valid"].as_bool().unwrap(), "{}", case["name"]);
    }
    let schema: Value = serde_json::from_str(WIRE_SCHEMA).unwrap();
    assert_eq!(
        schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
}

#[test]
fn discovery_and_typed_capabilities_share_the_advertised_vocabulary() {
    let names: Vec<_> = Capability::ALL
        .iter()
        .map(|capability| capability.as_str())
        .collect();
    assert_eq!(names, CAPABILITIES);
    for capability in Capability::ALL {
        assert_eq!(
            serde_json::to_value(capability).unwrap(),
            capability.as_str()
        );
    }
}
