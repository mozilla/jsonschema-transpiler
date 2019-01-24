
use converter::convert_bigquery_direct;
use serde_json::Value;

#[test]
fn bigquery_test_atomic() {
    let input_data = r#"
    {
      "type": "integer"
    }
    "#;
    let expected_data = r#"
    {
      "mode": "REQUIRED",
      "type": "INTEGER"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}

#[test]
fn bigquery_test_atomic_with_null() {
    let input_data = r#"
    {
      "type": [
        "integer",
        "null"
      ]
    }
    "#;
    let expected_data = r#"
    {
      "mode": "NULLABLE",
      "type": "INTEGER"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}

#[test]
fn bigquery_test_incompatible_atomic_multitype() {
    let input_data = r#"
    {
      "type": [
        "boolean",
        "integer"
      ]
    }
    "#;
    let expected_data = r#"
    {
      "mode": "REQUIRED",
      "type": "STRING"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}

#[test]
fn bigquery_test_incompatible_atomic_multitype_with_null() {
    let input_data = r#"
    {
      "type": [
        "boolean",
        "integer",
        "null"
      ]
    }
    "#;
    let expected_data = r#"
    {
      "mode": "NULLABLE",
      "type": "STRING"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_bigquery_direct(&input));
}
