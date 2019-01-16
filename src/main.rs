use serde_json::{json, Result, Value};
use std::collections::HashMap;

// Documentation references:
// https://docs.serde.rs/serde_json/value/index.html
// https://doc.rust-lang.org/1.30.0/book/second-edition/ch08-03-hash-maps.html
// http://json-schema.org/learn/miscellaneous-examples.html
// https://avro.apache.org/docs/1.8.1/spec.html#schema_primitive
// https://users.rust-lang.org/t/how-to-iterate-over-json-objects-with-hierarchy/19632

// #[derive(Serialize, Deserialize)]
// struct JsonSchemaField {
//     type: String,
//     properties: Option<HashMap<String, Box<JsonSchemaField>>>,
//     required: Option<Vec<String>>,
// }

// #[derive(Serialize, Deserialize)]
// struct AvroField {
//     type: String,
//     name: String,
//     fields: Option<List<Box<AvroField>>>
// }

pub mod converter {
    // This uses the Value interface for converting values, which is not strongly typed.
    pub fn convert_avro_direct(input: &Value, name: String) -> Value {
        let element: Value = match input["type"].as_str().unwrap() {
            "object" => {
                let mut fields = Vec::new();
                for (key, value) in input["properties"].as_object().unwrap().iter() {
                    fields.push(convert_avro_direct(value, key.to_string()));
                }
                fields.sort_by_key(|obj| obj["name"].as_str().unwrap().to_string());
                json!({
                    "type": "record",
                    "name": name,
                    "fields": fields,
                })
            }
            "integer" => json!({"name": name, "type": "int"}),
            "string" => json!({"name": name, "type": "string"}),
            "boolean" => json!({"name": name, "type": "boolean"}),
            _ => json!(null),
        };
        json!(element)
    }
}

use converter::convert_avro_direct;

fn main() {
    println!("hello world!");
}

#[test]
fn test_simple() {
    let data = r#"
    {
        "type": "object",
        "properties": {
            "field_1": {"type": "integer"},
            "field_2": {"type": "string"},
            "field_3": {"type": "boolean"}
        },
        "required": ["field_1", "field_3"]
    }"#;

    let v: Value = serde_json::from_str(data).unwrap();

    let expect = r#"
    {
        "type": "record",
        "name": "root",
        "fields": [
            {"name": "field_1", "type": "int"},
            {"name": "field_2", "type": "string"},
            {"name": "field_3", "type": "boolean"}
        ]
    }
    "#;
    let v1: Value = serde_json::from_str(expect).unwrap();

    assert_eq!(v1, convert_avro_direct(&v, "root".to_string()));
}
