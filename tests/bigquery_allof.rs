
use converter::convert_bigquery_direct;
use serde_json::Value;

#[test]
fn bigquery_test_allof_object() {
    let input_data = r#"
    {
      "allOf": [
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
          "type": "object"
        },
        {
          "required": [
            "field_1",
            "field_3"
          ]
        }
      ]
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
    assert_eq!(expected, convert_bigquery_direct(&input, "root".to_string()));
}
