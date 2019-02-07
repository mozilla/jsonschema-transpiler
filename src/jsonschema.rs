use serde::de::{self, Deserialize, Deserializer};
use serde::ser::{self, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum SimpleType {
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

#[derive(Serialize, Deserialize, Debug)]
struct Object {
    properties: Option<HashMap<String, Box<Tag>>>,
    #[serde(flatten, rename = "camelCase")]
    additional_properties: Option<AdditionalProperties>,
    #[serde(flatten, rename = "camelCase")]
    pattern_properties: Option<Box<Tag>>,
}

/// Represent an array of subschemas
type TagArray = Vec<Box<Tag>>;

#[derive(Serialize, Deserialize, Debug)]
struct Array {
    items: TagArray,
}

type OneOf = TagArray;
type AllOf = TagArray;

#[derive(Debug)]
enum Type {
    Simple(SimpleType),
    Multi(Vec<SimpleType>),
}

impl Serialize for Type {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Type::Simple(atom) => SimpleType::serialize(atom, serializer),
            Type::Multi(list) => Vec::<SimpleType>::serialize(list, serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Type {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = Value::deserialize(deserializer)?;
        if let Ok(atom) = SimpleType::deserialize(&v) {
            return Ok(Type::Simple(atom));
        } else if let Ok(list) = Vec::<SimpleType>::deserialize(&v) {
            return Ok(Type::Multi(list));
        } else {
            return Err(de::Error::custom("Error deserializing type!"));
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
struct Tag {
    #[serde(flatten, rename = "type")]
    data_type: Type,
    #[serde(flatten)]
    object: Option<Object>,
    #[serde(flatten)]
    extra: Option<HashMap<String, Value>>,
    #[serde(flatten, rename = "camelCase")]
    one_of: Option<OneOf>,
    #[serde(flatten, rename = "camelCase")]
    all_of: Option<AllOf>,
}

use serde_json::json;

#[test]
fn test_serialize_type_null() {
    let schema = Tag {
        data_type: Type::Simple(SimpleType::Null),
        object: None,
        extra: None,
        one_of: None,
        all_of: None,
    };
    let expect = json!({
        "type": "null"
    });
    assert_eq!(expect, json!(schema))
}

#[test]
fn test_deserialize_type_null() {
    let data = json!({
        "type": "null"
    });
    let schema: Tag = serde_json::from_value(data).unwrap();
    match schema.data_type {
        Type::Simple(SimpleType::Null) => (),
        _ => panic!(),
    };
}
