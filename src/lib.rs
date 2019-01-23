use serde_json::{json, Value};

// This uses the Value interface for converting values, which is not strongly typed.
pub fn convert_avro_direct(input: &Value, name: String) -> Value {
    let element: Value = match &input["type"] {
        Value::String(dtype) => match dtype.as_ref() {
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
        },
        _ => json!(null),
    };
    json!(element)
}

/// Convert JSONSchema into a BigQuery compatible schema
///
/// ## Notes:
/// It's probably useful to pass the entire subtree to the current node in the
/// tree in order to make sense of context.
pub fn convert_bigquery_direct(input: &Value) -> Value {
    let (dtype, mode): (String, String) = match &input["type"] {
        // if the type is a string, the mapping is straightforward
        Value::String(dtype) => {
            // arrays are a special-case that should be returned when seen
            if dtype.as_str() == "array" {
                // TODO: what happens to arrays of optional integers?
                let value: Value = convert_bigquery_direct(&input["items"]);
                let dtype: String = value["type"].as_str().unwrap().to_owned();
                (dtype, "REPEATED".into())
            } else {
                let mapped_dtype = match dtype.as_str() {
                    "integer" => "INTEGER",
                    "number" => "FLOAT",
                    "boolean" => "BOOLEAN",
                    "string" => "STRING",
                    "object" => "RECORD",
                    _ => panic!(),
                };
                (mapped_dtype.into(), "REQUIRED".into())
            }
        }
        Value::Array(lst) => ("foo".into(), "bar".into()),
        _ => panic!(),
    };
    json!({"foo": "bar"})
}
