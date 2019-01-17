
use converter::convert_avro_direct;
use serde_json::Value;

#[test]
fn test_object_with_atomics_is_sorted() {
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
		      "name": "field_1",
		      "type": "int"
		    },
		    {
		      "name": "field_2",
		      "type": "string"
		    },
		    {
		      "name": "field_3",
		      "type": "boolean"
		    },
		    {
		      "name": "field_4",
		      "type": "float"
		    }
		  ],
		  "name": "root",
		  "type": "record"
		}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}

#[test]
fn test_object_with_atomics_required() {
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
		      "name": "field_1",
		      "type": "int"
		    },
		    {
		      "name": "field_2",
		      "type": "string"
		    },
		    {
		      "name": "field_3",
		      "type": "boolean"
		    }
		  ],
		  "name": "root",
		  "type": "record"
		}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}

#[test]
fn test_object_with_atomics_required_with_null() {
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
		      "name": "field_1",
		      "type": "int"
		    },
		    {
		      "name": "field_2",
		      "type": "string"
		    },
		    {
		      "name": "field_3",
		      "type": "boolean"
		    }
		  ],
		  "name": "root",
		  "type": "record"
		}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}

#[test]
fn test_object_with_complex() {
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
		          "name": "field_1",
		          "type": "string"
		        },
		        {
		          "name": "field_2",
		          "type": "int"
		        }
		      ],
		      "name": "namespace_1",
		      "type": "record"
		    }
		  ],
		  "name": "root",
		  "type": "record"
		}
    "#;
    let input: Value = serde_json::from_str(input_data).unwrap();
    let expected: Value = serde_json::from_str(expected_data).unwrap();
    assert_eq!(expected, convert_avro_direct(&input, "root".to_string()));
}
