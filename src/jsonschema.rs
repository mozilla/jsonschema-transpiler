/// A JSON Schema serde module derived from the v4 spec.
/// Refer to http://json-schema.org/draft-04/schema for spec details.
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

use super::ast;
use super::Context;

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
    Bytes,
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
#[serde(rename_all = "camelCase")]
struct Array {
    // Using Option<TagArray> would support tuple validation
    #[serde(skip_serializing_if = "Option::is_none")]
    items: Option<ArrayType>,

    #[serde(skip_serializing_if = "Option::is_none")]
    additional_items: Option<AdditionalProperties>,

    #[serde(skip_serializing_if = "Option::is_none")]
    min_items: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    max_items: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum Format {
    DateTime,
    // Custom format value for casting strings into byte-strings
    Bytes,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
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
            (Value::String(string), Some(Format::DateTime)) if string == "string" => {
                Type::Atom(Atom::DateTime)
            }
            (Value::String(string), Some(Format::Bytes)) if string == "string" => {
                Type::Atom(Atom::Bytes)
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

    pub fn type_into_ast(&self, context: Context) -> Result<ast::Tag, &'static str> {
        match self.get_type() {
            Type::Atom(atom) => self.atom_into_ast(atom, context),
            Type::List(list) => {
                let mut nullable: bool = false;
                let mut items: Vec<ast::Tag> = Vec::new();
                for atom in list {
                    if let Atom::Null = &atom {
                        nullable = true;
                    }
                    items.push(self.atom_into_ast(atom, context)?);
                }

                let description = self.description.clone();
                let title = self.title.clone();

                Ok(ast::Tag::new(
                    ast::Type::Union(ast::Union::new(items)),
                    None,
                    nullable,
                    description,
                    title,
                ))
            }
        }
    }

    fn atom_into_ast(&self, data_type: Atom, context: Context) -> Result<ast::Tag, &'static str> {
        let description = self.description.clone();
        let title = self.title.clone();

        let result = match data_type {
            Atom::Null => ast::Tag::new(ast::Type::Null, None, true, description, title),
            Atom::Boolean => ast::Tag::new(
                ast::Type::Atom(ast::Atom::Boolean),
                None,
                false,
                description,
                title,
            ),
            Atom::Number => ast::Tag::new(
                ast::Type::Atom(ast::Atom::Number),
                None,
                false,
                description,
                title,
            ),
            Atom::Integer => ast::Tag::new(
                ast::Type::Atom(ast::Atom::Integer),
                None,
                false,
                description,
                title,
            ),
            Atom::String => ast::Tag::new(
                ast::Type::Atom(ast::Atom::String),
                None,
                false,
                description,
                title,
            ),
            Atom::DateTime => ast::Tag::new(
                ast::Type::Atom(ast::Atom::Datetime),
                None,
                false,
                description,
                title,
            ),
            Atom::Bytes => ast::Tag::new(
                ast::Type::Atom(ast::Atom::Bytes),
                None,
                false,
                description,
                title,
            ),
            Atom::Object => match &self.object.properties {
                Some(properties) => {
                    let mut fields: HashMap<String, ast::Tag> = HashMap::new();
                    for (key, value) in properties {
                        fields.insert(key.to_string(), value.type_into_ast(context)?);
                    }
                    ast::Tag::new(
                        ast::Type::Object(ast::Object::new(fields, self.object.required.clone())),
                        None,
                        false,
                        description,
                        title,
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
                            vec.push(add.type_into_ast(context)?);
                            let pat_vec: Result<Vec<_>, _> =
                                pat.values().map(|v| v.type_into_ast(context)).collect();
                            vec.extend(pat_vec?);
                            let value = ast::Tag::new(
                                ast::Type::Union(ast::Union::new(vec)),
                                None,
                                false,
                                None,
                                None,
                            );

                            ast::Tag::new(
                                ast::Type::Map(ast::Map::new(None, value)),
                                None,
                                false,
                                description,
                                title,
                            )
                        }
                        (Some(AdditionalProperties::Object(tag)), None) => ast::Tag::new(
                            ast::Type::Map(ast::Map::new(None, tag.type_into_ast(context)?)),
                            None,
                            false,
                            description,
                            title,
                        ),
                        (_, Some(tag)) => {
                            let items: Result<Vec<_>, _> =
                                tag.values().map(|v| v.type_into_ast(context)).collect();
                            let union = ast::Tag::new(
                                ast::Type::Union(ast::Union::new(items?)),
                                None,
                                false,
                                None,
                                None,
                            );
                            ast::Tag::new(
                                ast::Type::Map(ast::Map::new(None, union)),
                                None,
                                false,
                                description,
                                title,
                            )
                        }
                        _ => {
                            // handle oneOf
                            match &self.one_of {
                                Some(vec) => {
                                    let items: Result<Vec<_>, _> = vec
                                        .iter()
                                        .map(|item| item.type_into_ast(context))
                                        .collect();
                                    let unwrapped = items?;
                                    let nullable: bool = unwrapped.iter().any(ast::Tag::is_null);
                                    ast::Tag::new(
                                        ast::Type::Union(ast::Union::new(unwrapped)),
                                        None,
                                        nullable,
                                        description,
                                        title,
                                    )
                                }
                                None => ast::Tag::new(
                                    ast::Type::Atom(ast::Atom::JSON),
                                    None,
                                    false,
                                    description,
                                    title,
                                ),
                            }
                        }
                    }
                }
            },
            Atom::Array => {
                if let Some(items) = &self.array.items {
                    let data_type = match items {
                        ArrayType::Tag(items) => {
                            ast::Type::Array(ast::Array::new(items.type_into_ast(context)?))
                        }
                        ArrayType::TagTuple(items) => {
                            // Instead of expanding the definition of the AST
                            // tuple type, only a subset of tuple validation is
                            // accepted as valid. The type must be set to
                            // "array", the items a list of sub-schemas,
                            // additionalItems set to a valid type, and maxItems
                            // set to a value that is equal to or longer than
                            // the items list. Anything else will be directly
                            // translated into a JSON atom.
                            if context.tuple_struct {
                                let items: Result<Vec<_>, _> = items
                                    .iter()
                                    .map(|item| item.type_into_ast(context))
                                    .collect();
                                let mut unwrapped = items?;
                                let min_items: usize =
                                    self.array.min_items.unwrap_or_else(|| unwrapped.len());
                                // set items to optional
                                for item in unwrapped.iter_mut().skip(min_items) {
                                    item.nullable = true;
                                }
                                match &self.array.additional_items {
                                    Some(AdditionalProperties::Object(tag)) => {
                                        let max_items: usize = self.array.max_items.unwrap_or(0);
                                        if max_items < unwrapped.len() {
                                            return Err("maxItems is less than tuple length");
                                        }
                                        for _ in unwrapped.len()..max_items {
                                            let mut ast_tag = tag.type_into_ast(context)?;
                                            ast_tag.nullable = true;
                                            unwrapped.push(ast_tag);
                                        }
                                        ast::Type::Tuple(ast::Tuple::new(unwrapped))
                                    }
                                    Some(AdditionalProperties::Bool(false)) => {
                                        ast::Type::Tuple(ast::Tuple::new(unwrapped))
                                    }
                                    None => {
                                        // additionalItems may be unspecified if the max_length
                                        // matches the number of items in the tuple. This corresponds
                                        // to optional tuple values (variable length tuple).
                                        let max_items: usize = self.array.max_items.unwrap_or(0);
                                        if max_items == unwrapped.len() {
                                            ast::Type::Tuple(ast::Tuple::new(unwrapped))
                                        } else {
                                            return Err("maxItems must be set if additionalItems are allowed");
                                        }
                                    }
                                    _ => return Err("additionalItems set incorrectly"),
                                }
                            } else {
                                ast::Type::Atom(ast::Atom::JSON)
                            }
                        }
                    };
                    ast::Tag::new(data_type, None, false, None, None)
                } else {
                    return Err("array missing item");
                }
            }
        };
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::super::traits::TranslateInto;
    use super::super::Context;
    use super::*;
    use pretty_assertions::assert_eq;

    fn translate(data: Value) -> Value {
        let context = Context {
            ..Default::default()
        };
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.translate_into(context).unwrap();
        json!(ast)
    }

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
        let test_int = &props["test-int"];
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
    fn test_deserialize_type_nested_object_with_description() {
        let data = json!({
            "type": "object",
            "description": "outer object",
            "properties": {
                "nested-object": {
                    "type": "object",
                    "description": "test description",
                    "title": "test title",
                    "properties": {
                        "test-int": {"type": "int"}
                    }
                },
            }
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        let props = schema.object.properties.as_ref().unwrap();
        assert_eq!(props.len(), 1);
        assert_eq!(schema.description, Some("outer object".to_string()));
        let nested_object = *props.get("nested-object").as_ref().unwrap();
        assert_eq!(nested_object.data_type, json!("object"));
        assert_eq!(
            nested_object.description,
            Some("test description".to_string())
        );
        assert_eq!(nested_object.title, Some("test title".to_string()));
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
            ],
            "additionalItems": {"type": "string"},
            "maxItems": 4,
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        if let ArrayType::TagTuple(items) = schema.array.items.unwrap() {
            assert_eq!(items[0].data_type, json!("integer"));
            assert_eq!(items[1].data_type, json!("boolean"));
        } else {
            panic!();
        }
        if let AdditionalProperties::Object(tag) = schema.array.additional_items.unwrap() {
            assert_eq!(tag.data_type, json!("string"));
        } else {
            panic!();
        }
        assert_eq!(schema.array.max_items.unwrap(), 4);
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
        let expect = json!({
            "type": "null",
            "nullable": true,
        });
        assert_eq!(expect, translate(data))
    }

    #[test]
    fn test_into_ast_atom_integer() {
        let data = json!({
            "type": "integer"
        });
        let expect = json!({
            "type": {"atom": "integer"},
            "nullable": false,
        });
        assert_eq!(expect, translate(data))
    }

    #[test]
    fn test_into_ast_list() {
        let data = json!({
            "type": ["null", "integer"]
        });
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
        assert_eq!(expect, translate(data))
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
        assert_eq!(expect, translate(data))
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
        assert_eq!(expect, translate(data))
    }

    #[test]
    fn test_into_ast_array() {
        let data = json!({
            "type": "array",
            "items": {
                "type": "integer"
            }
        });
        let expect = json!({
        "nullable": false,
        "type": {
            "array": {
                "items": {
                    "name": "list",
                    "nullable": false,
                    "type": {"atom": "integer"}
                }}}});
        assert_eq!(expect, translate(data))
    }

    #[test]
    fn test_into_ast_one_of() {
        let data = json!({
            "oneOf": [
                {"type": "integer"},
                {"type": "null"}
            ],
        });
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
        assert_eq!(expect, translate(data))
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
    fn test_deserialize_type_bytes() {
        let data = json!({
            "type": "string",
            "format": "bytes"
        });
        let schema: Tag = serde_json::from_value(data).unwrap();
        assert_eq!(schema.format.unwrap(), Format::Bytes);
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
        let expect = json!({
            "type": {"atom": "datetime"},
            "nullable": false,
        });
        assert_eq!(expect, translate(data))
    }

    #[test]
    fn test_into_ast_atom_bytes() {
        let data = json!({
            "type": "string",
            "format": "bytes"
        });
        let expect = json!({
            "type": {"atom": "bytes"},
            "nullable": false,
        });
        assert_eq!(expect, translate(data))
    }

    fn translate_tuple(data: Value) -> Value {
        let context = Context {
            tuple_struct: true,
            ..Default::default()
        };
        let schema: Tag = serde_json::from_value(data).unwrap();
        let ast: ast::Tag = schema.translate_into(context).unwrap();
        json!(ast)
    }

    #[test]
    fn test_into_ast_tuple_default_behavior() {
        let data = json!({
            "type": "array",
            "items": [
                {"type": "boolean"},
                {"type": "integer"}
            ]
        });
        let expect = json!({"type": {"atom": "json"}, "nullable": false});
        assert_eq!(expect, translate(data))
    }

    #[test]
    #[should_panic(expected = "maxItems must be set")]
    fn test_into_ast_tuple_invalid() {
        let data = json!({
            "type": "array",
            "items": [
                {"type": "boolean"},
                {"type": "integer"}
            ]
        });
        let expect = json!({"type": {"atom": "json"}, "nullable": false});
        assert_eq!(expect, translate_tuple(data))
    }

    #[test]
    #[should_panic(expected = "maxItems")]
    fn test_into_ast_tuple_missing_max_items() {
        let data = json!({
            "type": "array",
            "items": [
                {"type": "boolean"},
                {"type": "integer"}
            ],
            "additionalItems": {"type": "string"}
        });
        let expect = json!({"type": {"atom": "json"}, "nullable": false});
        assert_eq!(expect, translate_tuple(data))
    }

    #[test]
    fn test_into_ast_tuple_static() {
        let data = json!({
            "type": "array",
            "items": [
                {"type": "boolean"},
                {"type": "integer"}
            ],
            "maxItems": 2
        });
        let expect = json!({
            "type": {"tuple": {"items": [
                {"name": "f0_", "type": {"atom": "boolean"}, "nullable": false},
                {"name": "f1_", "type": {"atom": "integer"}, "nullable": false}
            ]}},
            "nullable": false
        });
        assert_eq!(expect, translate_tuple(data))
    }

    #[test]
    fn test_into_ast_tuple_static_nullable() {
        let data = json!({
            "type": "array",
            "items": [
                {"type": "boolean"},
                {"type": "integer"}
            ],
            "minItems": 1,
            "maxItems": 2
        });
        let expect = json!({
            "type": {"tuple": {"items": [
                {"name": "f0_", "type": {"atom": "boolean"}, "nullable": false},
                {"name": "f1_", "type": {"atom": "integer"}, "nullable": true}
            ]}},
            "nullable": false
        });
        assert_eq!(expect, translate_tuple(data))
    }

    #[test]
    fn test_into_ast_tuple_valid() {
        let data = json!({
            "type": "array",
            "items": [
                {"type": "boolean"},
                {"type": "integer"}
            ],
            "additionalItems": {"type": "string"},
            "maxItems": 4
        });
        let expect = json!({
            "type": {"tuple": {"items": [
                {"name": "f0_", "type": {"atom": "boolean"}, "nullable": false},
                {"name": "f1_", "type": {"atom": "integer"}, "nullable": false},
                {"name": "f2_", "type": {"atom": "string"}, "nullable": true},
                {"name": "f3_", "type": {"atom": "string"}, "nullable": true},
            ]}},
            "nullable": false,
        });
        assert_eq!(expect, translate_tuple(data))
    }

    #[test]
    fn test_into_ast_array_of_array_of_tuples() {
        let data = json!({
            "type": "array",
            "items": {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": [
                        {"type": "integer"}
                    ],
                    "additionalItems": false
                }
            }
        });
        let expect = json!({
        "nullable": false,
        "type": {"array": {"items": {
            "nullable": false,
            "name": "list",
            "type": {"array": {"items": {
                "nullable": false,
                "name": "list",
                "namespace": ".list",
                "type": {"tuple": {"items": [
                    {
                        "name": "f0_",
                        "namespace": ".list.list",
                        "type": {"atom": "integer"},
                        "nullable": false
                    }]}}}}}}}}});
        assert_eq!(expect, translate_tuple(data))
    }
}
