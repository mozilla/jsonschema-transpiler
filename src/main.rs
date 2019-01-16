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
extern crate converter;
use converter::convert_avro_direct;
use serde_json::Value;

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
