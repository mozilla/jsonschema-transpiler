use jst::{convert_avro, convert_bigquery};
use jst::{Context, ResolveMethod};
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

fn data_object_missing() -> Value {
    // The second item has an incompatible field, but will be dropped.
    serde_json::from_str(
        r#"
    {
        "additionalItems": false,
        "items": [
            {"type": "integer"},
            {
                "type": "object",
                "properties": {
                    "first": {"type": "string"},
                    "second": {"type": ["string", "object"]}
                },
                "required": ["first"]
            }
        ],
        "type": "array"
    }
    "#,
    )
    .unwrap()
}

fn data_incompatible() -> Value {
    serde_json::from_str(
        r#"
            {
                "additionalItems": false,
                "items": [
                    {"type": ["string", "integer"]}
                ]
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

/// Objects within tuples are allowed to have extra fields. The decoding tool
/// should preserve the ordering of the items in the tuples.
#[test]
fn test_avro_tuple_object_drop() {
    let context = Context {
        tuple_struct: true,
        resolve_method: ResolveMethod::Drop,
        ..Default::default()
    };

    let expected: Value = serde_json::from_str(
        r#"
    {
        "fields": [
            {
                "name": "f0_",
                "type": {"type": "long"}
            },
            {
                "name": "f1_",
                "type": {
                    "name": "f1_",
                    "namespace": "root",
                    "type": "record",
                    "fields": [
                        {"name": "first", "type": {"type": "string"}}
                    ]
                }
            }
        ],
        "name": "root",
        "type": "record"
    }
    "#,
    )
    .unwrap();
    assert_eq!(expected, convert_avro(&data_object_missing(), context));
}

#[test]
fn test_bigquery_tuple_object_drop() {
    let context = Context {
        tuple_struct: true,
        resolve_method: ResolveMethod::Drop,
        ..Default::default()
    };

    let expected: Value = serde_json::from_str(
        r#"
        [
          {
            "mode": "REQUIRED",
            "name": "f0_",
            "type": "INT64"
          },
          {
            "mode": "REQUIRED",
            "name": "f1_",
            "type": "RECORD",
            "fields": [
                {"name": "first", "type": "STRING", "mode": "REQUIRED"}
            ]
          }
        ]
    "#,
    )
    .unwrap();
    assert_eq!(expected, convert_bigquery(&data_object_missing(), context));
}

#[test]
#[should_panic]
fn test_avro_tuple_object_incompatible() {
    let context = Context {
        tuple_struct: true,
        resolve_method: ResolveMethod::Drop,
        ..Default::default()
    };
    convert_avro(&data_incompatible(), context);
}

#[test]
#[should_panic]
fn test_bigquery_tuple_object_incompatible() {
    let context = Context {
        tuple_struct: true,
        resolve_method: ResolveMethod::Drop,
        ..Default::default()
    };
    convert_bigquery(&data_incompatible(), context);
}
