use jst::{convert_avro, convert_bigquery};
use jst::{Context, ResolveMethod};
use pretty_assertions::assert_eq;
use serde_json::Value;

fn test_data() -> Value {
    serde_json::from_str(
        r#"
    {
        "type": "object",
        "properties": {
            "test_snake_case": {"type": "boolean"},
            "testCamelCase": {"type": "boolean"},
            "TestPascalCase": {"type": "boolean"},
            "TEST_SCREAMING_SNAKE_CASE": {"type": "boolean"}
        },
        "required": [
            "test_snake_case",
            "testCamelCase",
            "TestPascalCase",
            "TEST_SCREAMING_SNAKE_CASE"
        ]
    }
    "#,
    )
    .unwrap()
}

#[test]
fn test_bigquery_normalize_snake_casing() {
    let context = Context {
        normalize_case: true,
        resolve_method: ResolveMethod::Panic,
    };
    let expected: Value = serde_json::from_str(
        r#"
        [
            {
                "mode": "REQUIRED",
                "name": "test_camel_case",
                "type": "BOOLEAN"
            },
            {
                "mode": "REQUIRED",
                "name": "test_pascal_case",
                "type": "BOOLEAN"
            },
            {
                "mode": "REQUIRED",
                "name": "test_snake_case",
                "type": "BOOLEAN"
            },
            {
                "mode": "REQUIRED",
                "name": "test_screaming_snake_case",
                "type": "BOOLEAN"
            }
        ]
        "#,
    )
    .unwrap();

    assert_eq!(expected, convert_bigquery(&test_data(), context));
}

#[test]
fn test_avro_normalize_snake_casing() {
    let context = Context {
        resolve_method: ResolveMethod::Cast,
        ..Default::default()
    };
    let expected: Value = serde_json::from_str(
        r#"
        {
        "fields": [
            {
                "name": "test_camel_case",
                "type": {"type": "boolean"}
            },
            {
                "name": "test_pascal_case",
                "type": {"type": "boolean"}
            },
            {
                "name": "test_snake_case",
                "type": {"type": "boolean"}
            },
            {
                "name": "test_screaming_snake_case",
                "type": {"type": "boolean"}
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
