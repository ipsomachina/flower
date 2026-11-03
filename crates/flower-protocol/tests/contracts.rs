use flower_protocol::*;

#[test]
fn tokens_preserve_full_width_counters_and_reject_other_versions() {
    let mut token = ReadToken {
        version: VERSION,
        database: "db".into(),
        group: "root".into(),
        incarnation: Incarnation(u64::MAX),
        revision: Revision(u64::MAX),
        position: LogPosition(u64::MAX),
    };
    assert_eq!(ReadToken::decode(&token.encode().unwrap()).unwrap(), token);
    assert_eq!(
        serde_json::to_value(&token).unwrap()["revision"],
        u64::MAX.to_string()
    );
    token.version += 1;
    assert!(token.encode().is_err());
    assert!(serde_json::from_str::<Revision>("\"01\"").is_err());
}

#[test]
fn missing_values_do_not_become_null_or_successful_writes() {
    let missing: CallEnvelope = serde_json::from_str(r#"{"ok":true}"#).unwrap();
    assert!(missing.validate(false).is_err());
    let null: CallEnvelope = serde_json::from_str(r#"{"ok":true,"value":null}"#).unwrap();
    assert!(null.validate(false).is_ok());
    assert!(null.validate(true).is_err());
    assert!(serde_json::from_str::<CallEnvelope>(r#"{"value":1}"#).is_err());
}

#[test]
fn sync_coverage_is_checked_even_for_empty_batches() {
    let mut batch: SyncBatch<serde_json::Value> = SyncBatch {
        version: VERSION,
        since: 4,
        current: 7,
        entries: vec![],
        snapshot: None,
        incarnation: 9,
    };
    assert!(batch.validate(4).is_ok());
    assert!(batch.validate(3).is_err());
    batch.entries.push(SyncEntry {
        index: 8,
        write: serde_json::Value::Null,
    });
    assert!(batch.validate(4).is_err());
    batch.entries.clear();
    batch.version += 1;
    assert!(batch.validate(4).is_err());
}

#[test]
fn retry_advice_and_execution_certainty_are_independent() {
    let proxy = Problem::from_body(422, b"proxy could not decode upstream", None);
    assert_eq!(proxy.origin, Origin::Host);
    assert_eq!(proxy.execution, Execution::Unknown);
    assert!(unresolved_outcome("receipt_released"));
    assert!(unresolved_outcome("retry_session_closed"));
    assert!(!unresolved_outcome("OUT_OF_STOCK"));
    let uncertain = Problem::from_body(
        503,
        br#"{"ok":false,"code":"forward_failed","message":"lost reply"}"#,
        None,
    );
    assert!(uncertain.retryable);
    assert_eq!(uncertain.execution, Execution::Unknown);
    let rejected = Problem::from_body(
        503,
        br#"{"ok":false,"code":"overloaded","message":"full"}"#,
        Some(1200),
    );
    assert!(rejected.retryable);
    assert_eq!(rejected.execution, Execution::NotStarted);
    assert_eq!(rejected.retry_after_ms, Some(1200));
}
