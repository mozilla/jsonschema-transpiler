#[macro_use]
extern crate serde;
extern crate serde_json;

use wasm_bindgen::prelude::*;

mod ast;
mod bigquery;
mod jsonschema;

/// Convert JSONSchema into a BigQuery compatible schema
#[wasm_bindgen]
pub fn convert_bigquery(input: &JsValue) -> JsValue {
    let jsonschema: jsonschema::Tag = input.into_serde().unwrap();
    let ast = ast::Tag::from(jsonschema);
    let bq = bigquery::Tag::from(ast);
    JsValue::from_serde(&bq).unwrap()
}
