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
use traits::TranslateFrom;

#[derive(Copy, Clone)]
pub enum ResolveMethod {
    Cast,
    Drop,
    Panic,
}

#[derive(Copy, Clone)]
pub struct Context {
    pub resolve_method: ResolveMethod,
}

fn into_ast(input: &Value, context: Option<Context>) -> ast::Tag {
    let jsonschema: jsonschema::Tag = match serde_json::from_value(json!(input)) {
        Ok(tag) => tag,
        Err(e) => panic!(format!("{:#?}", e)),
    };
    ast::Tag::translate_from(jsonschema, context).unwrap()
}

/// Convert JSON Schema into an Avro compatible schema
pub fn convert_avro(input: &Value, context: Option<Context>) -> Value {
    let avro = avro::Type::translate_from(into_ast(input, context), context).unwrap();
    json!(avro)
}

/// Convert JSON Schema into a BigQuery compatible schema
pub fn convert_bigquery(input: &Value, context: Option<Context>) -> Value {
    let bq = bigquery::Schema::translate_from(into_ast(input, context), context).unwrap();
    json!(bq)
}
