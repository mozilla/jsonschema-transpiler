#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet, VecDeque};
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

#[derive(Serialize, Deserialize, Debug)]
struct BigQueryRecord {
    #[serde(skip_serializing_if = "BigQueryRecord::is_root")]
    name: String,
    #[serde(rename = "type")]
    dtype: String,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<Vec<Box<BigQueryRecord>>>,
}

struct ConsistencyState {
    dtype: String,
    mode: String,
    consistent: bool,
}

impl Default for ConsistencyState {
    fn default() -> Self {
        ConsistencyState {
            dtype: "STRING".into(),
            mode: "NULLABLE".into(),
            consistent: true,
        }
    }
}

// Resolve the output of oneOf by finding a super-set of the schemas. This defaults to STRING otherwise.
fn handle_oneof(values: &Vec<Value>) -> Value {
    let elements: Vec<Value> = Vec::from_iter(
        values
            .into_iter()
            .map(|value| convert_bigquery_direct(&value)),
    );

    let nullable: bool = elements
        .iter()
        .all(|el| el["mode"].as_str().unwrap() == "NULLABLE");

    // filter null values and other types
    let filtered: Vec<Value> = Vec::from_iter(
        elements
            .into_iter()
            .filter_map(Option::Some)
            .filter(|x| !x["type"].is_null()),
    );

    // iterate over the entire document tree and collect values per node
    let mut resolution_table: HashMap<String, ConsistencyState> = HashMap::new();

    let mut queue: VecDeque<(String, Value)> = VecDeque::new();
    queue.extend(filtered.into_iter().map(|el| ("".into(), el)));

    while !queue.is_empty() {
        let (namespace, node) = queue.pop_front().unwrap();
        let key = match node["name"].as_str() {
            Some(name) => name.to_string(),
            None => "__ROOT__".into(),
        };
        let dtype = node["type"].as_str().unwrap().into();
        let mode = node["mode"].as_str().unwrap().into();

        // check for consistency
        if let Some(state) = resolution_table.get_mut(&key) {
            if !state.consistent
                || state.dtype != dtype
                || (state.mode == "REPEATING") ^ (mode == "REPEATING")
            {
                state.dtype = "STRING".into();
                state.mode = "NULLABLE".into();
                state.consistent = false;
            } else if dtype == "NULLABLE" {
                state.mode = dtype;
            };
        } else {
            let state = ConsistencyState {
                dtype: dtype,
                mode: mode,
                consistent: true,
            };
            resolution_table.insert(key.clone(), state);
        };

        if let Some(fields) = node["fields"].as_array() {
            for field in fields {
                let namespace = if namespace != "" {
                    format!("{}.{}", namespace, key.clone())
                } else {
                    key.clone()
                };
                queue.push_back((namespace, json!(field)));
            }
        };
    }

    // TODO: build the final document
    if resolution_table.iter().any(|(_, state)| !state.consistent) {
        return json!({"type": "STRING", "mode": "NULLABLE"});
    }

    unimplemented!()
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
                    }
                    None => {
                        // The schema doesn't contain properties or items, but
                        // contains additionalProperties and/or patternProperties.
                        // Handle this case as a map-type.
                        unimplemented!()
                    }
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


#[test]
fn test_bigquery_record_skips_root() {
    let x = BigQueryRecord {
        name: "__ROOT__".into(),
        dtype: "INTEGER".into(),
        mode: "NULLABLE".into(),
        fields: None,
    };
    assert_eq!(json!(x), json!({"type": "INTEGER", "mode": "NULLABLE"}))
}

#[test]
fn test_bigquery_record_single_level() {
    let x = BigQueryRecord {
        name: "field_name".into(),
        dtype: "INTEGER".into(),
        mode: "NULLABLE".into(),
        fields: None,
    };
    let value = json!(x);
    assert_eq!(value, json!({"name": "field_name", "type": "INTEGER", "mode": "NULLABLE"}))
}

#[test]
fn test_bigquery_record_nested() {
    let x = BigQueryRecord {
        name: "__ROOT__".into(),
        dtype: "INTEGER".into(),
        mode: "NULLABLE".into(),
        fields: Some(vec![Box::new(BigQueryRecord {
            name: "field_name".into(),
            dtype: "STRING".into(),
            mode: "NULLABLE".into(),
            fields: None,
        })]),
    };
    let value = json!(x);
    assert_eq!(
        value,
        json!({
            "type": "INTEGER",
            "mode": "NULLABLE",
            "fields": [
                {
                    "name": "field_name",
                    "type": "STRING",
                    "mode": "NULLABLE",
                }
            ]
        })
    )
}
