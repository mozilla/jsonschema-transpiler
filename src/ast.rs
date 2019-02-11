use super::jsonschema;
use serde_json::json;
use std::collections::HashMap;

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
        let nullable: bool = self.items.iter().any(|x| x.is_null());

        if self.items.is_empty() {
            panic!("empty union is not allowed")
        } else if self.items.len() == 1 {
            return Tag {
                name: None,
                nullable: nullable,
                data_type: self.items[0].data_type,
            };
        }

        let items: Vec<&Box<Tag>> = self.items.iter().filter(|x| !x.is_null()).collect();

        let data_type: Type = if items.iter().all(|x| x.is_atom()) {
            let mut iter = items.into_iter();
            let head: &Box<Tag> = iter.next().unwrap();
            iter.fold(head.data_type, |acc, x| match (acc, x.data_type) {
                (Type::Atom(left), Type::Atom(right)) => {
                    let atom = match (left, right) {
                        (Atom::Boolean, Atom::Boolean) => Atom::Boolean,
                        (Atom::Integer, Atom::Integer) => Atom::Integer,
                        (Atom::Number, Atom::Number)
                        | (Atom::Integer, Atom::Number)
                        | (Atom::Number, Atom::Integer) => Atom::Number,
                        (Atom::String, Atom::String) => Atom::String,
                        _ => Atom::JSON,
                    };
                    Type::Atom(atom)
                }
                _ => Type::Atom(Atom::JSON),
            })
        } else if items.iter().all(|x| x.is_object()) {
            unimplemented!()
        } else if items.iter().all(|x| x.is_map()) {
            unimplemented!()
        } else if items.iter().all(|x| x.is_array()) {
            unimplemented!()
        } else {
            Type::Atom(Atom::JSON)
        };

        Tag {
            name: None,
            nullable: nullable,
            data_type: data_type,
        }
    }
}

#[test]
fn test_union_collapse_atom() {
    let data = json!({
    "union": {
        "items": [
            {"type": {"atom": "integer"}},
        ]}});
    let dtype: Type = serde_json::from_value(data).unwrap();
    let expect = json!({
        "atom": "integer",
    });
    if let Type::Union(union) = dtype {
        assert_eq!(expect, json!(union.collapse()))
    } else {
        panic!()
    }
}

#[test]
fn test_union_collapse_atom_repeats() {
    let data = json!({
    "union": {
        "items": [
            {"type": {"atom": "integer"}},
            {"type": {"atom": "integer"}},
            {"type": {"atom": "integer"}},
        ]}});
    let dtype: Type = serde_json::from_value(data).unwrap();
    let expect = json!({
        "atom": "integer",
    });
    if let Type::Union(union) = dtype {
        assert_eq!(expect, json!(union.collapse()))
    } else {
        panic!()
    }
}

#[test]
fn test_union_collapse_nullable_atom() {
    let data = json!({
    "union": {
        "items": [
            {"type": "null"},
            {"type": {"atom": "integer"}},
        ]}});
    let dtype: Tag = serde_json::from_value(data).unwrap();
    let expect = json!({
        "atom": "integer",
    });
    if let Type::Union(union) = dtype.data_type {
        assert_eq!(expect, json!(union.collapse()))
    } else {
        panic!()
    }
}

#[test]
fn test_union_collapse_type_conflict() {
    let data = json!({
    "union": {
        "items": [
            {"type": {"atom": "string"}},
            {"type": {"atom": "integer"}},
        ]}});
    let dtype: Type = serde_json::from_value(data).unwrap();
    let expect = json!({
        "atom": "JSON",
    });
    if let Type::Union(union) = dtype {
        assert_eq!(expect, json!(union.collapse()))
    } else {
        panic!()
    }
}

#[test]
fn test_union_collapse_object_merge() {
    let data = json!({
    "union": {
        "items": [
            {
                "type": {
                    "object": {
                        "fields": {
                            "atom_0": {"type": {"atom": "boolean"}},
                            "atom_1": {"type": {"atom": "integer"}},
                        }}}},
            {
                "type": {
                    "object": {
                        "fields": {
                            "atom_1": {"type": {"atom": "integer"}},
                            "atom_2": {"type": {"atom": "string"}},
                        }}}},
        ]}});
    let dtype: Type = serde_json::from_value(data).unwrap();
    let expect = json!({
    "object": {
        "fields": {
            "atom_0": {"type": {"atom": "boolean"}},
            "atom_1": {"type": {"atom": "integer"}},
            "atom_2": {"type": {"atom": "string"}},
        }}});
    if let Type::Union(union) = dtype {
        assert_eq!(expect, json!(union.collapse()))
    } else {
        panic!()
    }
}

#[test]
fn test_union_collapse_object_conflict() {
    let data = json!({
    "union": {
        "items": [
            {"type": {"atom": "string"}},
            {"type": {"atom": "integer"}},
        ]}});
    let dtype: Type = serde_json::from_value(data).unwrap();
    let expect = json!({
        "atom": "JSON",
    });
    if let Type::Union(union) = dtype {
        assert_eq!(expect, json!(union.collapse()))
    } else {
        panic!()
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
    #[serde(default)]
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

    pub fn is_null(&self) -> bool {
        match self.data_type {
            Type::Null => true,
            _ => false,
        }
    }

    pub fn is_atom(&self) -> bool {
        match self.data_type {
            Type::Atom(_) => true,
            _ => false,
        }
    }

    pub fn is_object(&self) -> bool {
        match self.data_type {
            Type::Object(_) => true,
            _ => false,
        }
    }

    pub fn is_map(&self) -> bool {
        match self.data_type {
            Type::Map(_) => true,
            _ => false,
        }
    }

    pub fn is_array(&self) -> bool {
        match self.data_type {
            Type::Array(_) => true,
            _ => false,
        }
    }

    pub fn is_union(&self) -> bool {
        match self.data_type {
            Type::Array(_) => true,
            _ => false,
        }
    }

    /// Assign names to tags from parent Tags.
    fn infer_name(&mut self) {
        match &mut self.data_type {
            Type::Object(object) => {
                for (key, value) in object.fields.iter_mut() {
                    if let None = value.name {
                        value.name = Some(key.to_string());
                    }
                }
            }
            _ => (),
        }
    }

    // Infer whether the current node can be set to null
    // fn infer_nullability(&mut self) {
    //     unimplemented!()
    // }
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
