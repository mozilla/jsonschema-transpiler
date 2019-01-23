
use converter::convert_bigquery_direct;
use serde_json::Value;

#[test]
fn bigquery_test_map_with_atomics() {
    let input_data = r#"
    {
      "additionalProperties": {
        "type": "integer"
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "key",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "value",
          "type": "INTEGER"
        }
      ],
      "mode": "REPEATED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input, "root".to_string()));
}

#[test]
fn bigquery_test_map_with_complex() {
    let input_data = r#"
    {
      "additionalProperties": {
        "properties": {
          "field_1": {
            "type": "string"
          },
          "field_2": {
            "type": "integer"
          }
        },
        "type": "object"
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "key",
          "type": "STRING"
        },
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
          "mode": "REQUIRED",
          "name": "value",
          "type": "RECORD"
        }
      ],
      "mode": "REPEATED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input, "root".to_string()));
}

#[test]
fn bigquery_test_map_with_pattern_properties() {
    let input_data = r#"
    {
      "additionalProperties": false,
      "patternProperties": {
        ".+": {
          "type": "integer"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "key",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "value",
          "type": "INTEGER"
        }
      ],
      "mode": "REPEATED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input, "root".to_string()));
}

#[test]
fn bigquery_test_map_with_pattern_and_additional_properties() {
    let input_data = r#"
    {
      "additionalProperties": {
        "type": "integer"
      },
      "patternProperties": {
        ".+": {
          "type": "integer"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "key",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "value",
          "type": "INTEGER"
        }
      ],
      "mode": "REPEATED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input, "root".to_string()));
}

#[test]
fn bigquery_test_incompatible_map_with_pattern_properties() {
    let input_data = r#"
    {
      "additionalProperties": false,
      "patternProperties": {
        "^I_": {
          "type": "integer"
        },
        "^S_": {
          "type": "string"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "key",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "value",
          "type": "STRING"
        }
      ],
      "mode": "REPEATED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input, "root".to_string()));
}

#[test]
fn bigquery_test_incompatible_map_with_pattern_and_additional_properties() {
    let input_data = r#"
    {
      "additionalProperties": {
        "type": "integer"
      },
      "patternProperties": {
        ".+": {
          "type": "string"
        }
      },
      "type": "object"
    }
    "#;
    let expected_data = r#"
    {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "key",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "value",
          "type": "STRING"
        }
      ],
      "mode": "REPEATED",
      "type": "RECORD"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input, "root".to_string()));
}
