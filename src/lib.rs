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

/// Options for error handling in the [`TranslateFrom`] and [`TranslateInto`]
/// interfaces for converting between schema formats.
///
/// The `Cast` method will represent under-specified (e.g. empty objects) and
/// incompatible (e.g. variant-types or conflicting oneOf definitions) as
/// strings. This behavior is useful for compacting complex types into a single
/// column. In Spark and BigQuery, a casted column can be processed via a user
/// defined function that works on JSON. This method can cause issues with
/// schema evolution that require migration work.
///
/// The `Drop` method will drop fields if they do not fall neatly into one of
/// the supported types. This method ensures forward compatibility with schemas,
/// but requires support during data processing to capture the dropped data from
/// the structured section of the schema.
///
/// The `Panic` method will panic if the JSON Schema is inconsistent in any way,
/// or uses features that the transpiler does not support. This method is useful
/// way to test for incompatible schemas.
#[derive(Copy, Clone)]
pub enum ResolveMethod {
    Cast,
    Drop,
    Panic,
}

/// Options for modifying the behavior of translating between two schema
/// formats.
///
/// This structure passes context from the command-line interface into the
/// translation logic between the various schema types in the project. In
/// particular, the context is useful for resolving edge-cases in ambiguous
/// situations. This can includes situations like casting or dropping an empty
/// object.
#[derive(Copy, Clone)]
pub struct Context {
    pub resolve_method: ResolveMethod,
}

fn into_ast(input: &Value, context: Context) -> ast::Tag {
    let jsonschema: jsonschema::Tag = match serde_json::from_value(json!(input)) {
        Ok(tag) => tag,
        Err(e) => panic!(format!("{:#?}", e)),
    };
    ast::Tag::translate_from(jsonschema, context).unwrap()
}

/// Convert JSON Schema into an Avro compatible schema
pub fn convert_avro(input: &Value, context: Context) -> Value {
    let avro = avro::Type::translate_from(into_ast(input, context), context).unwrap();
    json!(avro)
}

/// Convert JSON Schema into a BigQuery compatible schema
pub fn convert_bigquery(input: &Value, context: Context) -> Value {
    let bq = bigquery::Schema::translate_from(into_ast(input, context), context).unwrap();
    json!(bq)
}
