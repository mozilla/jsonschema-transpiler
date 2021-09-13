#![recursion_limit = "128"]
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate maplit;

mod ast;
mod avro;
mod bigquery;
pub mod casing;
mod jsonschema;
mod traits;

use serde_json::{json, Value};
use traits::TranslateFrom;

/// The error resolution method in the [`TranslateFrom`] and [`TranslateInto`]
/// interfaces when converting between schema formats.
///
/// The `Cast` method will represent under-specified (e.g. empty objects) and
/// incompatible (e.g. variant-types or conflicting oneOf definitions) as
/// strings. This behavior is useful for compacting complex types into a single
/// column. In Spark and BigQuery, a casted column can be processed via a user
/// defined function that works on JSON. However, this method may cause issues
/// with schema evolution, for example when adding properties to empty objects.
///
/// The `Drop` method will drop fields if they do not fall neatly into one of
/// the supported types. This method ensures forward compatibility with schemas,
/// but it can lose large portions of nested data. Support from the data
/// processing side can recover dropped data from the structured section of the
/// schema.
///
/// The `Panic` method will panic if the JSON Schema is inconsistent or uses
/// unsupported features. This method is a useful way to test for incompatible
/// schemas.
#[derive(Copy, Clone)]
pub enum ResolveMethod {
    Cast,
    Drop,
    Panic,
}

impl Default for ResolveMethod {
    fn default() -> Self {
        ResolveMethod::Cast
    }
}

/// Options for modifying the behavior of translating between two schema
/// formats.
///
/// This structure passes context from the command-line interface into the
/// translation logic between the various schema types in the project. In
/// particular, the context is useful for resolving edge-cases in ambiguous
/// situations. This can includes situations like casting or dropping an empty
/// object.
#[derive(Copy, Clone, Default)]
pub struct Context {
    pub resolve_method: ResolveMethod,
    pub normalize_case: bool,
    pub force_nullable: bool,
    pub tuple_struct: bool,
    pub allow_maps_without_value: bool,
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
