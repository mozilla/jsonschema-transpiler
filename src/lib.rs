#[macro_use]
extern crate serde;
extern crate serde_json;

mod ast;
mod bigquery;
mod jsonschema;

use serde_json::{json, Value};

/// Convert JSONSchema into a BigQuery compatible schema
pub fn convert_bigquery(input: &Value) -> Value {
    let jsonschema: jsonschema::Tag = serde_json::from_value(json!(input)).unwrap();
    let ast = ast::Tag::from(jsonschema);
    let bq = bigquery::Tag::from(ast);
    json!(bq)
}
