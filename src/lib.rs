use serde_json::{json, Value};

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
