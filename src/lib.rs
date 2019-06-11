#![recursion_limit = "128"]
#[macro_use]
extern crate log;
extern crate regex;
#[macro_use]
extern crate serde;
extern crate serde_json;

mod ast;
mod avro;
mod bigquery;
mod jsonschema;
mod traits;

use serde_json::{json, Value};
use traits::Translate;

fn into_ast(input: &Value) -> ast::Tag {
    let jsonschema: jsonschema::Tag = match serde_json::from_value(json!(input)) {
        Ok(tag) => tag,
        Err(e) => panic!(format!("{:#?}", e)),
    };
    ast::Tag::translate(jsonschema).unwrap()
}

/// Convert JSON Schema into an Avro compatible schema
pub fn convert_avro(input: &Value) -> Value {
    let avro = avro::Type::translate(into_ast(input)).unwrap();
    json!(avro)
}

/// Convert JSON Schema into a BigQuery compatible schema
pub fn convert_bigquery(input: &Value) -> Value {
    let bq = bigquery::Schema::translate(into_ast(input)).unwrap();
    json!(bq)
}
