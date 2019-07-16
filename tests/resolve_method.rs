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
        ..Default::default()
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
        ..Default::default()
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
#[should_panic]
fn test_bigquery_resolve_error_panic() {
    let context = Context {
        resolve_method: ResolveMethod::Panic,
        ..Default::default()
    };
    convert_bigquery(&test_data(), context);
}

#[test]
fn test_avro_resolve_error_cast() {
    let context = Context {
        resolve_method: ResolveMethod::Cast,
        ..Default::default()
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
        ..Default::default()
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
#[should_panic]
fn test_avro_resolve_error_panic() {
    let context = Context {
        resolve_method: ResolveMethod::Panic,
        ..Default::default()
    };
    convert_avro(&test_data(), context);
}
