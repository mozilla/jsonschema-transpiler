use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase", tag = "type")]
enum Type {
    Null,
    Boolean,
    Number,
    Integer,
    String,
    Object(Object),
    Array(Array),
    OneOf(OneOf),
}

#[derive(Serialize, Deserialize, Debug)]
enum AdditionalProperties {
    Boolean,
    Object(Tag),
}

#[derive(Serialize, Deserialize, Debug)]
struct Object {
    properties: Option<HashMap<String, Box<Tag>>>,
    additionalProperties: Option<AdditionalProperties>,
    patternProperties: Option<Box<Tag>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Array {
    items: Vec<Tag>,
}

// Needs a custom deserializer
#[derive(Serialize, Deserialize, Debug)]
struct OneOf {
    oneof: Vec<Tag>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
struct Tag {
    #[serde(flatten, rename = "type")]
    data_type: Box<Type>,
}
