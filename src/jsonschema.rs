/// A JSON Schema serde module derived from the v4 spec.
/// Refer to http://json-schema.org/draft-04/schema for spec details.
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

use super::ast;

/// The type enumeration does not contain any data and is used to determine
/// available fields in the flattened tag. In JSONSchema parlance, these are
/// known as `simpleTypes`.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
enum Atom {
    Null,
    Boolean,
    Number,
    Integer,
    String,
    Object,
    Array,
    DateTime,
}

enum Type {
    Atom(Atom),
    List(Vec<Atom>),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum AdditionalProperties {
    Bool(bool),
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
    pattern_properties: Option<HashMap<String, Box<Tag>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    required: Option<HashSet<String>>,
}

/// Represent an array of subschemas. This is also known as a `schemaArray`.
type TagArray = Vec<Box<Tag>>;
type OneOf = TagArray;
type AllOf = TagArray;

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum ArrayType {
    Tag(Box<Tag>),
    TagTuple(TagArray),
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Array {
    // Using Option<TagArray> would support tuple validation
    #[serde(skip_serializing_if = "Option::is_none")]
    items: Option<ArrayType>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum Format {
    DateTime,
    #[serde(other)]
    Other,
}

/// Container for the main body of the schema.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase", tag = "type")]
pub struct Tag {
    #[serde(rename = "type", default)]
    data_type: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<Format>,
    #[serde(flatten)]
    object: Object,
    #[serde(flatten)]
    array: Array,
    #[serde(skip_serializing_if = "Option::is_none")]
    one_of: Option<OneOf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    all_of: Option<AllOf>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

impl Tag {
    fn get_type(&self) -> Type {
        match (&self.data_type, &self.format) {
            (Value::String(string), Some(Format::DateTime))
                if string == "string" =>
            {
                Type::Atom(Atom::DateTime)
            }
            (Value::String(string), _) => {
                let atom: Atom = serde_json::from_value(json!(string)).unwrap();
                Type::Atom(atom)
            }
            (Value::Array(array), _) => {
                let list: Vec<Atom> = array
                    .iter()
                    .map(|v| serde_json::from_value(json!(v)).unwrap())
                    .collect();
                Type::List(list)
            }
            _ => Type::Atom(Atom::Object),
        }
    }

    pub fn type_into_ast(&self) -> ast::Tag {
        match self.get_type() {
            Type::Atom(atom) => self.atom_into_ast(atom),
            Type::List(list) => {
                let mut nullable: bool = false;
                let mut items: Vec<ast::Tag> = Vec::new();
                for atom in list {
                    if let Atom::Null = &atom {
                        nullable = true;
                    }
                    items.push(self.atom_into_ast(atom));
                }
                ast::Tag::new(ast::Type::Union(ast::Union::new(items)), None, nullable)
            }
        }
    }

    fn atom_into_ast(&self, data_type: Atom) -> ast::Tag {
        match data_type {
            Atom::Null => ast::Tag::new(ast::Type::Null, None, true),
            Atom::Boolean => ast::Tag::new(ast::Type::Atom(ast::Atom::Boolean), None, false),
            Atom::Number => ast::Tag::new(ast::Type::Atom(ast::Atom::Number), None, false),
            Atom::Integer => ast::Tag::new(ast::Type::Atom(ast::Atom::Integer), None, false),
            Atom::String => ast::Tag::new(ast::Type::Atom(ast::Atom::String), None, false),
            Atom::DateTime => ast::Tag::new(ast::Type::Atom(ast::Atom::Datetime), None, false),
            Atom::Object => match &self.object.properties {
                Some(properties) => {
                    let mut fields: HashMap<String, ast::Tag> = HashMap::new();
                    for (key, value) in properties {
                        fields.insert(key.to_string(), value.type_into_ast());
                    }
                    ast::Tag::new(
                        ast::Type::Object(ast::Object::new(fields, self.object.required.clone())),
                        None,
                        false,
                    )
                }
                None => {
                    // handle maps
                    match (
                        &self.object.additional_properties,
                        &self.object.pattern_properties,
                    ) {
                        (Some(AdditionalProperties::Object(add)), Some(pat)) => {
                            let mut vec: Vec<ast::Tag> = Vec::new();
                            vec.push(add.type_into_ast());
                            vec.extend(pat.values().map(|v| v.type_into_ast()));
                            let value =
                                ast::Tag::new(ast::Type::Union(ast::Union::new(vec)), None, false);

                            ast::Tag::new(ast::Type::Map(ast::Map::new(None, value)), None, false)
                        }
                        (Some(AdditionalProperties::Object(tag)), None) => ast::Tag::new(
                            ast::Type::Map(ast::Map::new(None, tag.type_into_ast())),
                            None,
                            false,
                        ),
                        (_, Some(tag)) => {
                            let union = ast::Tag::new(
                                ast::Type::Union(ast::Union::new(
                                    tag.values().map(|v| v.type_into_ast()).collect(),
                                )),
                                None,
                                false,
                            );
                            ast::Tag::new(ast::Type::Map(ast::Map::new(None, union)), None, false)
                        }
                        _ => {
                            // handle oneOf
                            match &self.one_of {
                                Some(vec) => {
                                    let items: Vec<ast::Tag> =
                                        vec.iter().map(|item| item.type_into_ast()).collect();
                                    let nullable: bool = items.iter().any(|x| x.is_null());
                                    ast::Tag::new(
                                        ast::Type::Union(ast::Union::new(items)),
                                        None,
                                        nullable,
                                    )
                                }
                                None => {
                                    ast::Tag::new(ast::Type::Atom(ast::Atom::JSON), None, false)
                                }
                            }
                        }
                    }
                }
            },
            Atom::Array => {
                if let Some(items) = &self.array.items {
                    let data_type = match items {
                        ArrayType::Tag(items) => {
                            ast::Type::Array(ast::Array::new(items.type_into_ast()))
                        }
                        ArrayType::TagTuple(items) => {
                            let items: Vec<ast::Tag> =
                                items.iter().map(|item| item.type_into_ast()).collect();
                            ast::Type::Tuple(ast::Tuple::new(items))
                        }
                    };
                    ast::Tag::new(data_type, None, false)
                } else {
                    panic!(format!("array missing item: {:#?}", self))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

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
    fn test_serialize_type_object_additional_properties_bool() {
        // check that the untagged attribute is working correctly
        let schema = Tag {
            data_type: json!("object"),
            object: Object {
                additional_properties: Some(AdditionalProperties::Bool(true)),
                ..Default::default()
            },
            ..Default::default()
        };
        let expect = json!({
            "type": "object",
            "additionalProperties": true,
        });
        assert_eq!(expect, json!(schema))
    }

    #[test]
    fn test_deserialize_type_null() {
        let data = json!({
            "type": "null"
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        assert_eq!(schema.data_type.as_str().unwrap(), "null");
        assert!(schema.extra.is_empty());
    }

    #[test]
    fn test_deserialize_type_object() {
        let data = json!({
            "type": "object",
            "properties": {
                "test-int": {"type": "integer"},
                "test-null": {"type": "null"}
            },
            "required": ["test-int"]
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let props = schema.object.properties.unwrap();
        assert_eq!(props.len(), 2);
        let test_int = props.get("test-int").unwrap();
        assert_eq!(test_int.data_type, json!("integer"));
        assert_eq!(
            schema.object.required.unwrap(),
            vec!["test-int".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn test_deserialize_type_object_additional_properties() {
        let data_true = json!({
            "type": "object",
            "additionalProperties": true
        });
        if let Ok(schema) = serde_json::from_value::<Tag>(data_true) {
            match schema.object.additional_properties {
                Some(AdditionalProperties::Bool(true)) => (),
                _ => panic!(),
            }
        } else {
            panic!()
        };
        let data_false = json!({
            "type": "object",
            "additionalProperties": false
        });
        if let Ok(schema) = serde_json::from_value::<Tag>(data_false) {
            match schema.object.additional_properties {
                Some(AdditionalProperties::Bool(false)) => (),
                _ => panic!(),
            }
        } else {
            panic!()
        };
        let data_object = json!({
            "type": "object",
            "additionalProperties": {"type": "integer"}
        });
        if let Ok(schema) = serde_json::from_value::<Tag>(data_object) {
            match schema.object.additional_properties {
                Some(AdditionalProperties::Object(object)) => {
                    assert_eq!(object.data_type, json!("integer"))
                }
                _ => panic!(),
            }
        } else {
            panic!()
        };
    }

    #[test]
    fn test_deserialize_type_object_pattern_properties() {
        let data = json!({
        "type": "object",
        "patternProperties": {
            "*": {
                "type": "integer"
            }}});
        let schema: Tag = serde_json::from_value(data).unwrap();
        let props = schema.object.pattern_properties.unwrap();
        assert_eq!(props["*"].data_type, json!("integer"))
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
        if let ArrayType::Tag(items) = schema.array.items.unwrap() {
            assert_eq!(items.data_type, json!("integer"))
        } else {
            panic!();
        }
    }

    #[test]
    fn test_deserialize_type_array_tuple() {
        let data = json!({
            "type": "array",
            "items": [
                {"type": "integer"},
                {"type": "boolean"}
            ]
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        if let ArrayType::TagTuple(items) = schema.array.items.unwrap() {
            assert_eq!(items[0].data_type, json!("integer"));
            assert_eq!(items[1].data_type, json!("boolean"));
        } else {
            panic!();
        }
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

    #[test]
    fn test_deserialize_extras() {
        let data = json!({
            "meta": "hello world!"
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        assert_eq!(schema.extra["meta"], json!("hello world!"))
    }

    #[test]
    fn test_into_ast_atom_null() {
        let data = json!({
            "type": "null"
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.into();
        let expect = json!({
            "type": "null",
            "nullable": true,
        });
        assert_eq!(expect, json!(ast))
    }

    #[test]
    fn test_into_ast_atom_integer() {
        let data = json!({
            "type": "integer"
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.into();
        let expect = json!({
            "type": {"atom": "integer"},
            "nullable": false,
        });
        assert_eq!(expect, json!(ast))
    }

    #[test]
    fn test_into_ast_list() {
        let data = json!({
            "type": ["null", "integer"]
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.into();
        let expect = json!({
            "type": {
                "union": {
                    "items": [
                        {
                            // TODO: refactor this test to avoid implementation details
                            "name": "__union__",
                            "type": "null",
                            "nullable": true},
                        {
                            "name": "__union__",
                            "type": {"atom": "integer"},
                            "nullable": false},
                    ]
                }
            },
            "nullable": true,
        });
        assert_eq!(expect, json!(ast))
    }

    #[test]
    fn test_into_ast_object() {
        // test using an atom and a nested object
        let data = json!({
        "type": "object",
        "properties": {
            "test-int": {"type": "integer"},
            "test-obj": {
                "type": "object",
                "properties": {
                    "test-null": {"type": "null"}
                }}}});
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.into();
        let expect = json!({
        "nullable": false,
        "type": {
            "object": {
                "fields": {
                    "test_int": {
                        "name": "test_int",
                        "type": {"atom": "integer"},
                        "nullable": true,
                    },
                    "test_obj": {
                        "name": "test_obj",
                        "nullable": true,
                        "type": {
                            "object": {
                                "fields": {
                                    "test_null": {
                                        "name": "test_null",
                                        "namespace": ".test_obj",
                                        "type": "null",
                                        "nullable": true,
                                    }}}}}}}}});
        assert_eq!(expect, json!(ast))
    }

    #[test]
    fn test_into_ast_object_map() {
        let data = json!({
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "test-int": {"type": "integer"}
                }
            }
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.into();
        let expect = json!({
        "nullable": false,
        "type": {
            "map": {
                "key": {
                    "name": "key",
                    "nullable": false,
                    "type": {"atom": "string"}
                },
                "value": {
                    "name": "value",
                    "nullable": false,
                    "type": {
                        "object": {
                            "fields": {
                                "test_int": {
                                    "name": "test_int",
                                    "namespace": ".value",
                                    "nullable": true,
                                    "type": {"atom": "integer"}
                                }}}}}}}});
        assert_eq!(expect, json!(ast))
    }

    #[test]
    fn test_into_ast_array() {
        let data = json!({
            "type": "array",
            "items": {
                "type": "integer"
            }
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.into();
        let expect = json!({
        "nullable": false,
        "type": {
            "array": {
                "items": {
                    "name": "items",
                    "nullable": false,
                    "type": {"atom": "integer"}
                }}}});
        assert_eq!(expect, json!(ast))
    }

    #[test]
    fn test_into_ast_one_of() {
        let data = json!({
            "oneOf": [
                {"type": "integer"},
                {"type": "null"}
            ],
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.into();
        let expect = json!({
        "nullable": true,
        "type": {
            "union": {
                "items": [
                    {
                        "name": "__union__",
                        "nullable": false,
                        "type": {"atom": "integer"},
                    },
                    {
                        "name": "__union__",
                        "nullable": true,
                        "type": "null"
                    }
                ]}}});
        assert_eq!(expect, json!(ast))
    }

    #[test]
    fn test_deserialize_type_datetime() {
        let data = json!({
            "type": "string",
            "format": "date-time"
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        assert_eq!(schema.format.unwrap(), Format::DateTime);
    }

    #[test]
    fn test_deserialize_type_alternate_format() {
        let data = json!({
            "type": "string",
            "format": "email"
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        assert_eq!(schema.format.unwrap(), Format::Other);
    }

    #[test]
    fn test_into_ast_atom_datetime() {
        let data = json!({
            "type": "string",
            "format": "date-time"
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.into();
        let expect = json!({
            "type": {"atom": "datetime"},
            "nullable": false,
        });
        assert_eq!(expect, json!(ast))
    }
}
