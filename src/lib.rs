use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

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

fn match_bq_atomic_type(dtype: &str, ctx: &Value) -> (String, String) {
    // arrays are a special-case that should be returned when seen
    if dtype == "array" {
        // TODO: what happens to arrays of optional integers?
        let value: Value = convert_bigquery_direct(&ctx["items"]);
        let dtype: String = value["type"].as_str().unwrap().to_owned();
        (dtype, "REPEATED".into())
    } else {
        let mapped_dtype = match dtype {
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

fn match_bq_multi_type(vec: &Vec<Value>, ctx: &Value) -> (String, String) {
    let mut set: HashSet<&str> = HashSet::from_iter(vec.into_iter().map(|x| x.as_str().unwrap()));
    let mode = if set.contains("null") {
        set.remove("null");
        "NULLABLE"
    } else {
        "REQUIRED"
    };
    let dtype = if set.len() > 1 {
        "STRING".into()
    } else {
        let rest = set.iter().next().unwrap();
        let (dtype, _) = match_bq_atomic_type(rest, ctx);
        dtype
    };
    (dtype, mode.into())
}

fn handle_record(
    properties: &Map<String, Value>,
    dtype: String,
    mode: String,
    ctx: &Value,
) -> Value {
    let required: HashSet<String> = match ctx["required"].as_array() {
        Some(array) => HashSet::from_iter(
            array
                .to_vec()
                .into_iter()
                .map(|v| v.as_str().unwrap().to_string()),
        ),
        None => HashSet::new(),
    };
    let mut fields: Vec<Value> = Vec::from_iter(properties.into_iter().map(|(k, v)| {
        let mut field: Value = convert_bigquery_direct(v);
        let mode: String = field["mode"].as_str().unwrap().into();

        // create a mutable reference to the processed value
        let object = field.as_object_mut().unwrap();
        object.insert("name".to_string(), json!(k));

        // ignore the mode of the field unless defined in `required` keyword field
        if mode != "REPEATED" {
            if required.contains(k) && mode != "NULLABLE" {
                object.insert("mode".to_string(), json!("REQUIRED"));
            } else {
                object.insert("mode".to_string(), json!("NULLABLE"));
            }
        }
        json!(object)
    }));
    fields.sort_by_key(|x| x["name"].as_str().unwrap().to_string());
    json!({
        "type": dtype,
        "mode": mode,
        "fields": fields,
    })
}

/// Convert JSONSchema into a BigQuery compatible schema
pub fn convert_bigquery_direct(input: &Value) -> Value {
    let (dtype, mode): (String, String) = match &input["type"] {
        Value::String(dtype) => match_bq_atomic_type(dtype.as_str(), input),
        Value::Array(vec) => match_bq_multi_type(vec, input),
        _ => panic!(),
    };
    if dtype == "RECORD" {
        match &input["properties"].as_object() {
            Some(properties) => handle_record(properties, dtype, mode, input),
            None => {
                match &input["items"].as_object() {
                    Some(_) => {
                        let mut field: Value = convert_bigquery_direct(&input["items"]);
                        let object = field.as_object_mut().unwrap();
                        object.insert("mode".to_string(), json!("REPEATED"));
                        json!(object)
                    },
                    None => unimplemented!()
                }
            }
        }
    } else {
        json!({
            "type": dtype,
            "mode": mode,
        })
    }
}
