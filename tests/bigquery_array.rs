
use converter::convert_avro_direct;
use serde_json::Value;

#[test]
fn avro_test_array_with_atomics() {
    let input_data = r#"
    {
      "items": {
        "type": "integer"
      },
      "type": "array"
    }
    "#;
    let expected_data = r#"
    {
      "items": {
        "type": "int"
      },
      "name": "root",
      "type": "array"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}

#[test]
fn avro_test_array_with_complex() {
    let input_data = r#"
    {
      "items": {
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
      "type": "array"
    }
    "#;
    let expected_data = r#"
    {
      "items": {
        "fields": [
          {
            "name": "field_1",
            "type": "string"
          },
          {
            "name": "field_2",
            "type": "int"
          }
        ],
        "name": "TODO: ???",
        "type": "record"
      },
      "name": "root",
      "type": "array"
    }
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}
