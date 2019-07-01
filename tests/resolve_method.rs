use jst::{convert_avro, convert_bigquery};
use jst::{Context, ResolveMethod};
use serde_json::Value;

fn test_data() -> Value {
    serde_json::from_str(
        r#"
    {
        "type": "object",
        "properties": {
            "empty": {},
            "int": {"type": "integer"}
        }
    }
    "#,
    )
    .unwrap()
}

#[test]
fn test_bigquery_resolve_error_cast() {
    let context = Context {
        resolve_method: ResolveMethod::Cast,
    };
    let expected: Value = serde_json::from_str(
        r#"
        [
            {
                "mode": "NULLABLE",
                "name": "empty",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "int",
                "type": "INT64"
            }
        ]
        "#,
    )
    .unwrap();

    assert_eq!(expected, convert_bigquery(&test_data(), context));
}

#[test]
fn test_bigquery_resolve_error_drop() {
    let context = Context {
        resolve_method: ResolveMethod::Drop,
    };
    let expected: Value = serde_json::from_str(
        r#"
        [
            {
                "mode": "NULLABLE",
                "name": "int",
                "type": "INT64"
            }
        ]
        "#,
    )
    .unwrap();
    assert_eq!(expected, convert_bigquery(&test_data(), context));
}

#[test]
fn test_bigquery_resolve_error_panic() {
    let context = Context {
        resolve_method: ResolveMethod::Panic,
    };
    assert!(std::panic::catch_unwind(|| convert_bigquery(&test_data(), context)).is_err());
}

#[test]
fn test_avro_resolve_error_cast() {
    let context = Context {
        resolve_method: ResolveMethod::Cast,
    };
    let expected: Value = serde_json::from_str(
        r#"
        {
            "fields": [
                {
                    "default": null,
                    "name": "empty",
                    "type": [
                        {"type": "null"},
                        {"type": "string"}
                    ]
                },
                {
                    "default": null,
                    "name": "int",
                    "type": [
                        {"type": "null"},
                        {"type": "long"}
                    ]
                }
            ],
            "name": "root",
            "type": "record"
        }
        "#,
    )
    .unwrap();

    assert_eq!(expected, convert_avro(&test_data(), context));
}

#[test]
fn test_avro_resolve_error_drop() {
    let context = Context {
        resolve_method: ResolveMethod::Drop,
    };
    let expected: Value = serde_json::from_str(
        r#"
        {
            "fields": [
                {
                    "default": null,
                    "name": "int",
                    "type": [
                        {"type": "null"},
                        {"type": "long"}
                    ]
                }
            ],
            "name": "root",
            "type": "record"
        }
        "#,
    )
    .unwrap();
    assert_eq!(expected, convert_avro(&test_data(), context));
}

#[test]
fn test_avro_resolve_error_panic() {
    let context = Context {
        resolve_method: ResolveMethod::Panic,
    };
    assert!(std::panic::catch_unwind(|| convert_avro(&test_data(), context)).is_err());
}
