#[macro_use]
extern crate serde;
extern crate serde_json;

mod ast;
mod avro;
mod bigquery;
mod jsonschema;

use serde_json::{json, Value};

fn into_ast(input: &Value) -> ast::Tag {
    let jsonschema: jsonschema::Tag = serde_json::from_value(json!(input)).unwrap();
    ast::Tag::from(jsonschema)
}

/// Convert JSON Schema into an Avro compatible schema
pub fn convert_avro(input: &Value) -> Value {
    let avro = avro::Type::from(into_ast(input));
    json!(avro)
}

/// Convert JSON Schema into a BigQuery compatible schema
pub fn convert_bigquery(input: &Value) -> Value {
    let bq = bigquery::Tag::from(into_ast(input));
    json!(bq)
}
