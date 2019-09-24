use jst::Context;
use jst::{convert_avro, convert_bigquery};
use pretty_assertions::assert_eq;
use serde_json::Value;

fn data_atomic() -> Value {
    serde_json::from_str(
        r#"
    {
        "additionalItems": false,
        "items": [
            {"type": "boolean"},
            {"type": "string"}
        ],
        "type": "array"
    }
    "#,
    )
    .unwrap()
}

fn data_atomic_with_additional_properties() -> Value {
    serde_json::from_str(
        r#"
    {
        "additionalItems": {
        "type": "integer"
        },
        "items": [
            {"type": "boolean"},
            {"type": "string"}
        ],
        "maxItems": 4,
        "type": "array"
    }
    "#,
    )
    .unwrap()
}

#[test]
fn test_avro_tuple_atomic() {
    let context = Context {
        tuple_struct: true,
        ..Default::default()
    };
    let expected: Value = serde_json::from_str(
        r#"
    {
        "fields": [
            {
                "name": "f0_",
                "type": {"type": "boolean"}
            },
            {
                "name": "f1_",
                "type": {"type": "string"}
            }
        ],
        "name": "root",
        "type": "record"
    }
    "#,
    )
    .unwrap();
    assert_eq!(expected, convert_avro(&data_atomic(), context));
}

#[test]
fn test_bigquery_tuple_atomic() {
    let context = Context {
        tuple_struct: true,
        ..Default::default()
    };
    let expected: Value = serde_json::from_str(
        r#"
        [
          {
            "mode": "REQUIRED",
            "name": "f0_",
            "type": "BOOL"
          },
          {
            "mode": "REQUIRED",
            "name": "f1_",
            "type": "STRING"
          }
        ]
    "#,
    )
    .unwrap();
    assert_eq!(expected, convert_bigquery(&data_atomic(), context));
}

#[test]
fn test_avro_tuple_atomic_with_additional_items() {
    let context = Context {
        tuple_struct: true,
        ..Default::default()
    };
    let expected: Value = serde_json::from_str(
        r#"
        {
          "fields": [
            {
              "name": "f0_",
              "type": {"type": "boolean"}
            },
            {
              "name": "f1_",
              "type": {"type": "string"}
            },
            {
              "name": "f2_",
              "default": null,
              "type": [{"type": "null"}, {"type": "long"}]
            },
            {
              "name": "f3_",
              "default": null,
              "type": [{"type": "null"}, {"type": "long"}]
            }
          ],
          "name": "root",
          "type": "record"
        }
    "#,
    )
    .unwrap();
    assert_eq!(
        expected,
        convert_avro(&data_atomic_with_additional_properties(), context)
    );
}

#[test]
fn test_bigquery_tuple_atomic_with_additional_items() {
    let context = Context {
        tuple_struct: true,
        ..Default::default()
    };
    let expected: Value = serde_json::from_str(
        r#"
    [
        {
        "mode": "REQUIRED",
        "name": "f0_",
        "type": "BOOL"
        },
        {
        "mode": "REQUIRED",
        "name": "f1_",
        "type": "STRING"
        },
        {
        "mode": "NULLABLE",
        "name": "f2_",
        "type": "INT64"
        },
        {
        "mode": "NULLABLE",
        "name": "f3_",
        "type": "INT64"
        }
    ]
    "#,
    )
    .unwrap();
    assert_eq!(
        expected,
        convert_bigquery(&data_atomic_with_additional_properties(), context)
    );
}
