use serde_json::Value;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
enum Type {
    Null,
    Boolean,
    Number,
    Integer,
    String,
    Object,
    Array,
}

#[derive(Serialize, Deserialize, Debug)]
enum AdditionalProperties {
    True,
    False,
    Object(Box<Tag>),
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
struct Object {
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<HashMap<String, Box<Tag>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    additional_properties: Option<AdditionalProperties>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pattern_properties: Option<Box<Tag>>,
}

/// Represent an array of subschemas
type TagArray = Vec<Box<Tag>>;
type OneOf = TagArray;
type AllOf = TagArray;

#[derive(Serialize, Deserialize, Debug, Default)]
struct Array {
    #[serde(skip_serializing_if = "Option::is_none")]
    items: Option<TagArray>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase", tag = "type")]
struct Tag {
    #[serde(rename = "type")]
    data_type: Value,
    #[serde(flatten)]
    object: Object,
    #[serde(flatten)]
    items: Array,
    #[serde(skip_serializing_if = "Option::is_none")]
    one_of: Option<OneOf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    all_of: Option<AllOf>,
    #[serde(flatten)]
    extra: Option<HashMap<String, Value>>,
}

pub struct JSONSchema {
    data: Tag,
}

impl JSONSchema {
    pub fn from_value(value: Value) -> Self {
        unimplemented!()
    }
}

// TODO: impl Into<ast::AST> for JSONSchema

use serde_json::json;

#[test]
fn test_serialize_type_null() {
    let schema = Tag {
        data_type: json!("integer"),
        ..Default::default()
    };
    let expect = json!({
        "type": "integer"
    });
    assert_eq!(expect, json!(schema))
}

#[test]
fn test_deserialize_type_null() {
    let data = json!({
        "type": "null"
    });
    let schema: Tag = serde_json::from_value(data).unwrap();
    assert_eq!(schema.data_type.as_str().unwrap(), "null")
}

#[test]
fn test_deserialize_type_object() {
    panic!()
}

#[test]
fn test_deserialize_type_array() {
    panic!()
}

#[test]
fn test_deserialize_type_one_of() {
    panic!()
}

#[test]
fn test_deserialize_type_all_of() {
    panic!()
}
