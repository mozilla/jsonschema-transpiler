
use converter::convert_avro_direct;
use serde_json::Value;

#[test]
fn test_atomic() {
    let input_data = r#"
    {
		  "type": "integer"
		}
    "#;
    let expected_data = r#"
    {
		  "name": "root",
		  "type": "int"
		}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}

#[test]
fn test_atomic_with_null() {
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
		  "name": "root",
		  "type": [
		    "int",
		    "null"
		  ]
		}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}

#[test]
fn test_incompatible_atomic_multitype() {
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
		  "type": "string"
		}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}

#[test]
fn test_incompatible_atomic_multitype_with_null() {
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
		  "type": "string"
		}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}
