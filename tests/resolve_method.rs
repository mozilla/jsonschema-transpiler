use jst::{convert_avro, convert_bigquery};
use jst::{Context, ResolveMethod};
use serde_json::Value;

#[test]
fn test_bigquery_object_error_resolution() {
    let mut expected: Value;
    let mut context: Option<Context>;

    let input: Value = serde_json::from_str(
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
    .unwrap();

    expected = serde_json::from_str(
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

    context = Some(Context {
        resolve_method: ResolveMethod::Cast,
    });
    assert_eq!(expected, convert_bigquery(&input, context));

    expected = serde_json::from_str(
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
    context = Some(Context {
        resolve_method: ResolveMethod::Drop,
    });
    assert_eq!(expected, convert_bigquery(&input, context));

    context = Some(Context {
        resolve_method: ResolveMethod::Panic,
    });
    assert!(std::panic::catch_unwind(|| convert_bigquery(&input, context)).is_err());
}

#[test]
fn test_avro_object_error_resolution() {
    let mut expected: Value;
    let mut context: Option<Context>;

    let input: Value = serde_json::from_str(
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
    .unwrap();

    expected = serde_json::from_str(
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

    context = Some(Context {
        resolve_method: ResolveMethod::Cast,
    });
    assert_eq!(expected, convert_avro(&input, context));

    expected = serde_json::from_str(
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
    context = Some(Context {
        resolve_method: ResolveMethod::Drop,
    });
    assert_eq!(expected, convert_avro(&input, context));

    context = Some(Context {
        resolve_method: ResolveMethod::Panic,
    });
    assert!(std::panic::catch_unwind(|| convert_avro(&input, context)).is_err());
}
