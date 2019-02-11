use serde_json::json;
use std::collections::HashMap;
use super::jsonschema;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Atom {
    Boolean,
    Integer,
    Number,
    String,
    JSON,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Object {
    fields: HashMap<String, Box<Tag>>,
}

impl Object {
    pub fn new(fields: HashMap<String, Box<Tag>>) -> Self {
        Object { fields: fields }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Array {
    items: Box<Tag>,
}

impl Array {
    pub fn new(items: Tag) -> Self {
        Array {
            items: Box::new(items),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Map {
    key: Box<Tag>,
    value: Box<Tag>,
}

impl Map {
    pub fn new(key: Option<String>, value: Tag) -> Self {
        Map {
            key: Box::new(Tag {
                name: key,
                data_type: Type::Atom(Atom::String),
                nullable: false,
            }),
            value: Box::new(value),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Union {
    items: Vec<Box<Tag>>,
}

impl Union {
    pub fn new(items: Vec<Tag>) -> Self {
        Union {
            items: items.into_iter().map(Box::new).collect(),
        }
    }

    fn collapse(&self) -> Tag {
        unimplemented!()
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    Null,
    Atom(Atom),
    Object(Object),
    Map(Map),
    Array(Array),
    Union(Union),
    // Intersection
    // Not
}

impl Default for Type {
    fn default() -> Self {
        Type::Null
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(tag = "type")]
pub struct Tag {
    #[serde(rename = "type")]
    data_type: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    nullable: bool,
}

impl Tag {
    pub fn new(data_type: Type, name: Option<String>, nullable: bool) -> Self {
        Tag {
            data_type: data_type,
            name: name,
            nullable: nullable,
        }
    }

    fn infer_name(&mut self) {
        unimplemented!()
    }

    fn infer_nullability(&mut self) {
        unimplemented!()
    }
}

impl From<jsonschema::Tag> for Tag {
    fn from(tag: jsonschema::Tag) -> Self {
        tag.type_into_ast()
    }
}

#[test]
fn test_serialize_null() {
    let null_tag = Tag {
        ..Default::default()
    };
    let expect = json!({
        "type": "null",
        "nullable": false,
    });
    assert_eq!(expect, json!(null_tag))
}

#[test]
fn test_serialize_atom() {
    let atom = Tag {
        data_type: Type::Atom(Atom::Integer),
        name: Some("test-int".into()),
        nullable: true,
    };
    let expect = json!({
        "type": {"atom": "integer"},
        "name": "test-int",
        "nullable": true,
    });
    assert_eq!(expect, json!(atom));
}

#[test]
fn test_serialize_object() {
    let mut field = Tag {
        data_type: Type::Object(Object::new(HashMap::new())),
        name: Some("test-object".into()),
        nullable: false,
    };
    if let Type::Object(object) = &mut field.data_type {
        object.fields.insert(
            "test-int".into(),
            Box::new(Tag {
                data_type: Type::Atom(Atom::Integer),
                name: Some("test-int".into()),
                nullable: false,
            }),
        );
        object.fields.insert(
            "test-bool".into(),
            Box::new(Tag {
                data_type: Type::Atom(Atom::Boolean),
                name: Some("test-bool".into()),
                nullable: false,
            }),
        );
    }
    let expect = json!({
        "name": "test-object",
        "nullable": false,
        "type": {
            "object": {
                "fields": {
                    "test-int": {
                        "name": "test-int",
                        "type": {"atom": "integer"},
                        "nullable": false
                    },
                    "test-bool": {
                        "name": "test-bool",
                        "type": {"atom": "boolean"},
                        "nullable": false
                    }
                }
            }
        }
    });
    assert_eq!(expect, json!(field))
}

#[test]
fn test_serialize_map() {
    let atom = Tag {
        data_type: Type::Atom(Atom::Integer),
        name: Some("test-value".into()),
        nullable: false,
    };
    let field = Tag {
        data_type: Type::Map(Map::new(Some("test-key".into()), atom)),
        name: Some("test-map".into()),
        nullable: true,
    };
    let expect = json!({
        "name": "test-map",
        "nullable": true,
        "type": {
            "map": {
                "key": {
                    "name": "test-key",
                    "nullable": false,
                    "type": {"atom": "string"},
                },
                "value": {
                    "name": "test-value",
                    "nullable": false,
                    "type": {"atom": "integer"},
                }
            }
        }
    });
    assert_eq!(expect, json!(field));
}

#[test]
fn test_serialize_array() {
    // represent multi-set with nulls
    let atom = Tag {
        data_type: Type::Atom(Atom::Integer),
        name: Some("test-int".into()),
        nullable: true,
    };
    let field = Tag {
        data_type: Type::Array(Array::new(atom)),
        name: Some("test-array".into()),
        nullable: false,
    };
    let expect = json!({
        "type": {
            "array": {
                "items": {
                    "name": "test-int",
                    "type": {"atom": "integer"},
                    "nullable": true,
                }
            }
        },
        "name": "test-array",
        "nullable": false
    });
    assert_eq!(expect, json!(field))
}

#[test]
fn test_serialize_union() {
    let test_int = Tag {
        data_type: Type::Atom(Atom::Integer),
        ..Default::default()
    };
    let test_null = Tag {
        ..Default::default()
    };
    let union = Tag {
        data_type: Type::Union(Union {
            items: vec![Box::new(test_int), Box::new(test_null)],
        }),
        ..Default::default()
    };
    let expect = json!({
        "type": {
            "union": {
                "items": [
                    {"type": {"atom": "integer"}, "nullable": false},
                    {"type": "null", "nullable": false},
                ]
            }
        },
        "nullable": false
    });
    assert_eq!(expect, json!(union))
}
