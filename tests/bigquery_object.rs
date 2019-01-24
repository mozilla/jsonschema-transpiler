
use converter::convert_bigquery_direct;
use serde_json::Value;

#[test]
fn bigquery_test_object_with_atomics_is_sorted() {
    let input_data = r#"
    {
      "properties": {
        "field_1": {
          "type": "integer"
        },
        "field_2": {
          "type": "string"
        },
        "field_3": {
          "type": "boolean"
        },
        "field_4": {
          "type": "number"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "NULLABLE",
          "name": "field_1",
          "type": "INTEGER"
        },
        {
          "mode": "NULLABLE",
          "name": "field_2",
          "type": "STRING"
        },
        {
          "mode": "NULLABLE",
          "name": "field_3",
          "type": "BOOLEAN"
        },
        {
          "mode": "NULLABLE",
          "name": "field_4",
          "type": "FLOAT"
        }
      ],
      "mode": "REQUIRED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}

#[test]
fn bigquery_test_object_with_atomics_required() {
    let input_data = r#"
    {
      "properties": {
        "field_1": {
          "type": "integer"
        },
        "field_2": {
          "type": "string"
        },
        "field_3": {
          "type": "boolean"
        }
      },
      "required": [
        "field_1",
        "field_3"
      ],
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "field_1",
          "type": "INTEGER"
        },
        {
          "mode": "NULLABLE",
          "name": "field_2",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "field_3",
          "type": "BOOLEAN"
        }
      ],
      "mode": "REQUIRED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}

#[test]
fn bigquery_test_object_with_atomics_required_with_null() {
    let input_data = r#"
    {
      "properties": {
        "field_1": {
          "type": [
            "integer",
            "null"
          ]
        },
        "field_2": {
          "type": "string"
        },
        "field_3": {
          "type": "boolean"
        }
      },
      "required": [
        "field_1",
        "field_3"
      ],
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "NULLABLE",
          "name": "field_1",
          "type": "INTEGER"
        },
        {
          "mode": "NULLABLE",
          "name": "field_2",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "field_3",
          "type": "BOOLEAN"
        }
      ],
      "mode": "REQUIRED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}

#[test]
fn bigquery_test_object_with_complex() {
    let input_data = r#"
    {
      "properties": {
        "namespace_1": {
          "properties": {
            "field_1": {
              "type": "string"
            },
            "field_2": {
              "type": "integer"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "fields": [
            {
              "mode": "NULLABLE",
              "name": "field_1",
              "type": "STRING"
            },
            {
              "mode": "NULLABLE",
              "name": "field_2",
              "type": "INTEGER"
            }
          ],
          "mode": "NULLABLE",
          "name": "namespace_1",
          "type": "RECORD"
        }
      ],
      "mode": "REQUIRED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}
