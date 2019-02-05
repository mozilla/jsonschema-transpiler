#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

mod ast;

use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::FromIterator;

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
enum JSONSchemaKind {
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Object,
    Array,
    AllOf,
    OneOf,
    Unknown,
}

struct JSONSchemaType {
    kind: JSONSchemaKind,
    nullable: bool,
}

impl JSONSchemaType {
    pub fn new() -> Self {
        JSONSchemaType {
            kind: JSONSchemaKind::Unknown,
            nullable: false,
        }
    }

    /// Create typing context about a JSONSchema object
    ///
    /// # Arguments
    /// * `value` - A serde_json::Value containing a schema
    pub fn from_value(node: &Value) -> JSONSchemaType {
        match &node["type"] {
            Value::String(dtype) => {
                let kind = JSONSchemaType::kind_from_string(dtype.as_str());
                JSONSchemaType {
                    kind: kind,
                    nullable: kind == JSONSchemaKind::Null,
                }
            }
            Value::Array(multitype) => {
                let mut set: HashSet<JSONSchemaKind> = HashSet::from_iter(
                    multitype
                        .into_iter()
                        .map(|s| JSONSchemaType::kind_from_string(s.as_str().unwrap().into())),
                );
                let mut json_type = JSONSchemaType::new();
                if set.contains(&JSONSchemaKind::Null) {
                    json_type.nullable = true;
                    set.remove(&JSONSchemaKind::Null);
                };
                if set.len() > 1 {
                    json_type.kind = JSONSchemaKind::Unknown;
                } else {
                    json_type.kind = set.into_iter().next().unwrap();
                }
                json_type
            }
            _ => {
                let kind = if !node["properties"].is_null() {
                    JSONSchemaKind::Object
                } else if !node["allOf"].is_null() {
                    JSONSchemaKind::AllOf
                } else if !node["oneOf"].is_null() {
                    JSONSchemaKind::OneOf
                } else {
                    JSONSchemaKind::Unknown
                };
                JSONSchemaType {
                    kind: kind,
                    nullable: false,
                }
            }
        }
    }

    fn kind_from_string(dtype: &str) -> JSONSchemaKind {
        match dtype {
            "null" => JSONSchemaKind::Null,
            "boolean" => JSONSchemaKind::Boolean,
            "integer" => JSONSchemaKind::Integer,
            "number" => JSONSchemaKind::Number,
            "string" => JSONSchemaKind::String,
            "object" => JSONSchemaKind::Object,
            "array" => JSONSchemaKind::Array,
            _ => panic!(),
        }
    }

    pub fn as_bq_type(&self) -> String {
        let type_str = match self.kind {
            JSONSchemaKind::Boolean => "BOOLEAN",
            JSONSchemaKind::Integer => "INTEGER",
            JSONSchemaKind::Number => "FLOAT",
            JSONSchemaKind::String => "STRING",
            JSONSchemaKind::Object => "RECORD",
            // Array does not have a corresponding type in bq
            _ => "STRING",
        };
        type_str.into()
    }

    pub fn as_bq_mode(&self) -> String {
        let mode = if self.kind == JSONSchemaKind::Array {
            "REPEATED"
        } else if self.nullable || self.kind == JSONSchemaKind::Null {
            "NULLABLE"
        } else {
            "REQUIRED"
        };
        mode.into()
    }
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

impl BigQueryRecord {
    fn is_root(name: &str) -> bool {
        name == "__ROOT__"
    }

    fn insert_helper(&mut self, keys: &mut VecDeque<&str>, value: BigQueryRecord) {
        // traverse the document tree, creating records if necessary
        // check if the current node has fields
        if keys.is_empty() {
            return;
        }
        let key = keys.pop_front().unwrap();
        match &mut self.fields {
            Some(fields) => {
                if let Ok(index) =
                    fields.binary_search_by_key(&key.to_string(), |x| x.name.to_string())
                {
                    // the field exists, and it's the key that we're looking for
                    let field = fields.get_mut(index).unwrap();
                    if keys.is_empty() {
                        std::mem::replace(&mut *field, Box::new(value));
                    } else {
                        field.insert_helper(keys, value);
                    }
                } else {
                    fields.push(Box::new(value));
                }
            }
            None => {
                if keys.is_empty() {
                    self.fields = Some(vec![Box::new(value)]);
                    return;
                }
                let mut node = BigQueryRecord {
                    name: key.to_string(),
                    dtype: "RECORD".into(),
                    mode: "REQUIRED".into(),
                    fields: None,
                };
                node.insert_helper(keys, value);
                self.fields = Some(vec![Box::new(node)]);
            }
        };
    }

    /// Insert a field into a BigQuery schema
    ///
    /// # Arguments
    /// * `key` - The position in the schema to insert the record
    /// * `value` - The record to insert into the schema
    ///
    /// # Example
    /// The key can take on the following shape:
    /// `__ROOT__.foo.bar`
    pub fn insert(&mut self, key: String, value: BigQueryRecord) {
        let mut keys: VecDeque<&str> = VecDeque::from_iter(key.split("."));
        // pop the __ROOT__ off the front, which may be unnecessary in the future
        keys.pop_front();
        self.insert_helper(&mut keys, value);
    }
}

impl From<ConsistencyState> for BigQueryRecord {
    fn from(state: ConsistencyState) -> Self {
        BigQueryRecord {
            name: state.name,
            dtype: state.dtype,
            mode: state.mode,
            fields: None,
        }
    }
}

impl From<&mut ConsistencyState> for BigQueryRecord {
    fn from(state: &mut ConsistencyState) -> Self {
        BigQueryRecord {
            name: state.name.to_string(),
            dtype: state.dtype.to_string(),
            mode: state.mode.to_string(),
            fields: None,
        }
    }
}

struct ConsistencyState {
    name: String,
    dtype: String,
    mode: String,
    consistent: bool,
}

impl Default for ConsistencyState {
    fn default() -> Self {
        ConsistencyState {
            name: "__TEMP__".into(),
            dtype: "STRING".into(),
            mode: "NULLABLE".into(),
            consistent: true,
        }
    }
}

/// Convert JSONSchema into a BigQuery compatible schema
pub fn convert_bigquery(input: &Value) -> Value {
    let json_type = JSONSchemaType::from_value(input);
    match json_type.kind {
        JSONSchemaKind::Object => match &input["properties"].as_object() {
            Some(properties) => handle_record(properties, json_type.as_bq_mode(), input),
            None => handle_map(input),
        },
        JSONSchemaKind::Array => handle_array(input),
        JSONSchemaKind::AllOf => unimplemented!(),
        JSONSchemaKind::OneOf => handle_oneof(&input["oneOf"].as_array().unwrap()),
        _ => json!({
            "type": json_type.as_bq_type(),
            "mode": json_type.as_bq_mode(),
        }),
    }
}

fn handle_record(properties: &Map<String, Value>, mode: String, ctx: &Value) -> Value {
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
        let mut field: Value = convert_bigquery(v);
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
        "type": "RECORD",
        "mode": mode,
        "fields": fields,
    })
}

fn handle_map(input: &Value) -> Value {
    // The schema doesn't contain properties or items, but
    // contains additionalProperties and/or patternProperties.
    // Handle this case as a map-type.
    let mut extras: Vec<Value> = Vec::new();
    if let Some(pattern_props) = &input.get("patternProperties") {
        if let Some(object) = pattern_props.as_object() {
            for value in object.values() {
                extras.push(json!(value));
            }
        }
    }
    if let Some(additional_props) = &input.get("additionalProperties") {
        if additional_props.is_object() {
            extras.push(json!(additional_props));
        }
    }
    let mut value = handle_oneof(&extras);
    if let Some(object) = value.as_object_mut() {
        object.insert("name".into(), json!("value"));
    }
    json!({
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "key", "type": "STRING", "mode": "REQUIRED"},
            value,
        ]
    })
}

fn handle_array(input: &Value) -> Value {
    let mut field: Value = convert_bigquery(&input["items"]);
    let object = field.as_object_mut().unwrap();
    object.insert("mode".to_string(), json!("REPEATED"));
    json!(object)
}

// Resolve the output of oneOf by finding a super-set of the schemas. This defaults to STRING otherwise.
fn handle_oneof(values: &Vec<Value>) -> Value {
    let nullable: bool = values
        .iter()
        .any(|v| JSONSchemaType::from_value(v).nullable);

    let elements: Vec<Value> = Vec::from_iter(
        values
            .into_iter()
            .filter(|v| JSONSchemaType::from_value(v).kind != JSONSchemaKind::Null)
            .map(|v| convert_bigquery(&v)),
    );

    // iterate over the entire document tree and collect values per node
    let mut resolution_table: HashMap<String, ConsistencyState> = HashMap::new();

    let mut queue: VecDeque<(String, Value)> = VecDeque::new();
    queue.extend(elements.into_iter().map(|el| ("".into(), el)));

    while !queue.is_empty() {
        let (namespace, node) = queue.pop_front().unwrap();
        let name = match node["name"].as_str() {
            Some(name) => name.to_string(),
            None => "__ROOT__".to_string(),
        };
        let key = if namespace != "" {
            format!("{}.{}", namespace, name.clone())
        } else {
            name.clone()
        };

        let dtype = node["type"].as_str().unwrap().into();
        let mode = node["mode"].as_str().unwrap().into();

        // check for consistency
        if let Some(state) = resolution_table.get_mut(&key) {
            if !state.consistent
                || state.dtype != dtype
                || (state.mode == "REPEATING") & (mode != "REPEATING")
            {
                state.dtype = "STRING".into();
                state.mode = "NULLABLE".into();
                state.consistent = false;
            } else if mode == "NULLABLE" {
                state.mode = mode;
            };
        } else {
            let state = ConsistencyState {
                name: name.clone(),
                dtype: dtype,
                mode: mode,
                consistent: true,
            };
            resolution_table.insert(key.clone(), state);
        };

        if let Some(fields) = node["fields"].as_array() {
            for field in fields {
                queue.push_back((key.clone(), json!(field)));
            }
        };
    }

    // build the final document
    if resolution_table.iter().any(|(_, state)| !state.consistent) {
        let mode = if nullable { "NULLABLE" } else { "REQUIRED" };
        return json!({"type": "STRING", "mode": mode.to_string()});
    }

    let mut root: BigQueryRecord = resolution_table.get_mut("__ROOT__".into()).unwrap().into();
    if root.mode != "REPEATED" && nullable {
        root.mode = "NULLABLE".into()
    }
    resolution_table.remove("__ROOT__".into());

    let mut source = Vec::from_iter(resolution_table.into_iter());
    source.sort_by_key(|(k, _)| k.to_string());

    for (key, value) in source {
        root.insert(key, value.into());
    }

    json!(root)
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
    assert_eq!(
        value,
        json!({"name": "field_name", "type": "INTEGER", "mode": "NULLABLE"})
    )
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

#[test]
fn test_bigquery_record_insertion() {
    let mut root = BigQueryRecord {
        name: "__ROOT__".into(),
        dtype: "RECORD".into(),
        mode: "NULLABLE".into(),
        fields: None,
    };
    let foo = BigQueryRecord {
        name: "foo".into(),
        dtype: "RECORD".into(),
        mode: "NULLABLE".into(),
        fields: None,
    };
    let bar = BigQueryRecord {
        name: "bar".into(),
        dtype: "INTEGER".into(),
        mode: "NULLABLE".into(),
        fields: None,
    };
    &root.insert("__ROOT__.foo".into(), foo);
    assert_eq!(
        json!(root),
        json!({
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {
                    "name": "foo",
                    "type": "RECORD",
                    "mode": "NULLABLE",
                }
            ]
        })
    );
    root.insert("__ROOT__.foo.bar".into(), bar);
    assert_eq!(
        json!(root),
        json!({
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {
                    "name": "foo",
                    "type": "RECORD",
                    "mode": "NULLABLE",
                    "fields": [
                        {
                            "name": "bar",
                            "type": "INTEGER",
                            "mode": "NULLABLE",
                        }
                    ]
                }
            ]
        })
    );
}
