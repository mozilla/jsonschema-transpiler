use serde_json::Value;
use std::collections::HashMap;

/// The type enumeration does not contain any data and is used to determine
/// available fields in the flattened tag. In JSONSchema parlance, these are
/// known as `simpleTypes`.
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

/// Represent an array of subschemas. This is also known as a `schemaArray`.
type TagArray = Vec<Box<Tag>>;
type OneOf = TagArray;
type AllOf = TagArray;

#[derive(Serialize, Deserialize, Debug, Default)]
struct Array {
    // Using Option<TagArray> would support tuple validation
    #[serde(skip_serializing_if = "Option::is_none")]
    items: Option<Box<Tag>>,
}

/// Container for the main body of the schema.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase", tag = "type")]
struct Tag {
    #[serde(rename = "type", default)]
    data_type: Value,
    #[serde(flatten)]
    object: Object,
    #[serde(flatten)]
    array: Array,
    #[serde(skip_serializing_if = "Option::is_none")]
    one_of: Option<OneOf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    all_of: Option<AllOf>,
    #[serde(flatten)]
    extra: Option<HashMap<String, Value>>,
}

struct JSONSchema {
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
    let data = json!({
        "type": "object",
        "properties": {
            "test-int": {"type": "integer"},
            "test-null": {"type": "null"}
        }
    });
    let schema: Tag = serde_json::from_value(data).unwrap();
    let props = schema.object.properties.unwrap();
    assert_eq!(props.len(), 2);
    let test_int = props.get("test-int").unwrap();
    assert_eq!(test_int.data_type, json!("integer"));
}

#[test]
fn test_deserialize_type_object_additional_properties() {
    let data_true = json!({
        "type": "object",
        "additionalProperties": true
    });
    if let Ok(schema) = serde_json::from_value::<Tag>(data_true) {
        match schema.object.additional_properties {
            Some(AdditionalProperties::True) => (),
            _ => panic!(),
        }
    };
    let data_false = json!({
        "type": "object",
        "additionalProperties": false
    });
    if let Ok(schema) = serde_json::from_value::<Tag>(data_false) {
        match schema.object.additional_properties {
            Some(AdditionalProperties::False) => (),
            _ => panic!(),
        }
    };
    let data_false = json!({
        "type": "object",
        "additionalProperties": {"type": "integer"}
    });
    if let Ok(schema) = serde_json::from_value::<Tag>(data_false) {
        match schema.object.additional_properties {
            Some(AdditionalProperties::Object(object)) => {
                assert_eq!(object.data_type, json!("integer"))
            }
            _ => panic!(),
        }
    };
}

#[test]
fn test_deserialize_type_nested_object() {
    let data = json!({
        "type": "object",
        "properties": {
            "nested-object": {
                "type": "object",
                "properties": {
                    "test-int": {"type": "int"}
                }
            },
        }
    });
    let schema: Tag = serde_json::from_value(data).unwrap();
    let props = schema.object.properties.as_ref().unwrap();
    assert_eq!(props.len(), 1);
    let nested_object = *props.get("nested-object").as_ref().unwrap();
    assert_eq!(nested_object.data_type, json!("object"));
    let nested_object_props = nested_object.object.properties.as_ref().unwrap();
    assert_eq!(nested_object_props.len(), 1);
}

#[test]
fn test_deserialize_type_array() {
    let data = json!({
        "type": "array",
        "items": {
            "type": "integer"
        }
    });
    let schema: Tag = serde_json::from_value(data).unwrap();
    let items = schema.array.items.unwrap();
    assert_eq!(items.data_type, json!("integer"))
}

#[test]
fn test_deserialize_type_one_of() {
    let data = json!({
        "oneOf": [
            {"type": "integer"},
            {"type": "null"}
        ],
    });
    let schema: Tag = serde_json::from_value(data).unwrap();
    assert!(schema.data_type.is_null());
    let one_of = schema.one_of.unwrap();
    assert_eq!(one_of.len(), 2);
    assert_eq!(one_of[0].data_type, json!("integer"));
    assert_eq!(one_of[1].data_type, json!("null"));
}

#[test]
fn test_deserialize_type_all_of() {
    let data = json!({
        "allOf": [
            {"type": "integer"},
            {"type": "null"}
        ],
    });
    let schema: Tag = serde_json::from_value(data).unwrap();
    assert!(schema.data_type.is_null());
    let all_of = schema.all_of.unwrap();
    assert_eq!(all_of.len(), 2);
    assert_eq!(all_of[0].data_type, json!("integer"));
    assert_eq!(all_of[1].data_type, json!("null"));
}
