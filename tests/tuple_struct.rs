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
                "name": "_f0",
                "type": {"type": "boolean"}
            },
            {
                "name": "_f1",
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
            "mode": "NULLABLE",
            "name": "_f0",
            "type": "BOOL"
          },
          {
            "mode": "NULLABLE",
            "name": "_f1",
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
              "name": "_f0",
              "type": {"type": "boolean"}
            },
            {
              "name": "_f1",
              "type": {"type": "string"}
            },
            {
              "name": "_f2",
              "type": {"type": "long"}
            },
            {
              "name": "_f3",
              "type": {"type": "long"}
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
        "mode": "NULLABLE",
        "name": "_f0",
        "type": "BOOLEAN"
        },
        {
        "mode": "NULLABLE",
        "name": "_f1",
        "type": "STRING"
        },
        {
        "mode": "NULLABLE",
        "name": "_f2",
        "type": "INT64"
        },
        {
        "mode": "NULLABLE",
        "name": "_f3",
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
