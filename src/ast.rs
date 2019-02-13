use super::jsonschema;
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Atom {
    Boolean,
    Integer,
    Number,
    String,
    JSON,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Object {
    pub fields: HashMap<String, Box<Tag>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<HashSet<String>>,
}

impl Object {
    pub fn new(fields: HashMap<String, Tag>, required: Option<HashSet<String>>) -> Self {
        let boxed: HashMap<String, Box<Tag>> =
            fields.into_iter().map(|(k, v)| (k, Box::new(v))).collect();
        Object {
            fields: boxed,
            required: required,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Array {
    pub items: Box<Tag>,
}

impl Array {
    pub fn new(items: Tag) -> Self {
        Array {
            items: Box::new(items),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Map {
    pub key: Box<Tag>,
    pub value: Box<Tag>,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Union {
    items: Vec<Box<Tag>>,
}

impl Union {
    pub fn new(items: Vec<Tag>) -> Self {
        Union {
            items: items.into_iter().map(Box::new).collect(),
        }
    }

    /// Collapse a union of types into a structurally compatible type.
    ///
    /// Typically, variant types are not allowed in a table schema. If a variant type
    /// type is found, it will be converted into a JSON type. Because of the ambiguity
    /// around finding structure in a JSON blob, the union of any type with JSON will
    /// be consumed by the JSON type. In a similar fashion, a table schema is determined
    /// to be nullable or required via occurances of null types in unions.
    pub fn collapse(&self) -> Tag {
        let nullable: bool = self.items.iter().any(|x| x.is_null());

        if self.items.is_empty() {
            panic!("empty union is not allowed")
        } else if self.items.len() == 1 {
            return Tag {
                name: None,
                nullable: nullable,
                data_type: self.items[0].data_type.clone(),
            };
        }

        let items: Vec<&Box<Tag>> = self.items.iter().filter(|x| !x.is_null()).collect();

        let data_type: Type = if items.iter().all(|x| x.is_atom()) {
            items
                .into_iter()
                .fold(Type::Null, |acc, x| match (acc, &x.data_type) {
                    (Type::Null, Type::Atom(atom)) => Type::Atom(*atom),
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
            items
                .into_iter()
                .fold(Type::Null, |acc, x| match (&acc, &x.data_type) {
                    (Type::Null, Type::Object(object)) => Type::Object(object.clone()),
                    (Type::Object(left), Type::Object(right)) => {
                        // union each sub-property, recursively collapse, and rebuild
                        let mut union: HashMap<String, Vec<Tag>> = HashMap::new();
                        for (key, value) in &left.fields {
                            union.insert(key.to_string(), vec![*value.clone()]);
                        }
                        for (key, value) in &right.fields {
                            if let Some(vec) = union.get_mut(key) {
                                vec.push(*value.clone())
                            } else {
                                union.insert(key.to_string(), vec![*value.clone()]);
                            }
                        }
                        let result: HashMap<String, Tag> = union
                            .into_iter()
                            .map(|(k, v)| (k, Union::new(v).collapse()))
                            .collect();
                        // Atom::JSON is a catch-all value and makes for inconsistent objects
                        let is_consistent = !result.iter().any(|(_, v)| match v.data_type {
                            Type::Atom(Atom::JSON) => true,
                            _ => false,
                        });
                        if is_consistent {
                            let required: Option<HashSet<String>> =
                                match (&left.required, &right.required) {
                                    (Some(x), Some(y)) => {
                                        Some(x.union(&y).map(|x| x.to_string()).collect())
                                    }
                                    (Some(x), None) | (None, Some(x)) => Some(x.clone()),
                                    _ => None,
                                };
                            Type::Object(Object::new(result, required))
                        } else {
                            Type::Atom(Atom::JSON)
                        }
                    }
                    _ => Type::Atom(Atom::JSON),
                })
        } else if items.iter().all(|x| x.is_map()) {
            let tags: Vec<Tag> = items
                .into_iter()
                .map(|x| match &x.data_type {
                    Type::Map(map) => *map.value.clone(),
                    _ => panic!(),
                })
                .collect();
            Type::Map(Map::new(None, Union::new(tags).collapse()))
        } else if items.iter().all(|x| x.is_array()) {
            let tags: Vec<Tag> = items
                .into_iter()
                .map(|x| match &x.data_type {
                    Type::Array(array) => *array.items.clone(),
                    _ => panic!(),
                })
                .collect();
            Type::Array(Array::new(Union::new(tags).collapse()))
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(tag = "type")]
pub struct Tag {
    #[serde(rename = "type")]
    pub data_type: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub nullable: bool,
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
    pub fn infer_name(&mut self) {
        match &mut self.data_type {
            Type::Object(object) => {
                for (key, value) in object.fields.iter_mut() {
                    if let None = value.name {
                        value.name = Some(key.to_string());
                    }
                    value.infer_name()
                }
            }
            Type::Map(map) => {
                if let None = map.key.name {
                    map.key.name = Some("key".into());
                }
                if let None = map.value.name {
                    map.value.name = Some("value".into());
                }
                map.value.infer_name()
            }
            Type::Array(array) => array.items.infer_name(),
            _ => (),
        }
    }

    /// These rules are primarily focused on BigQuery, although they should translate
    /// into other schemas.
    pub fn infer_nullability(&mut self) {
        match &mut self.data_type {
            Type::Object(object) => {
                let required = match &object.required {
                    Some(required) => required.clone(),
                    None => HashSet::new(),
                };
                for (key, value) in &mut object.fields {
                    if required.contains(key) {
                        value.nullable = false;
                    } else {
                        value.nullable = true;
                    }
                    value.infer_nullability()
                }
            }
            Type::Map(map) => map.value.infer_nullability(),
            Type::Array(array) => array.items.infer_nullability(),
            _ => (),
        }
    }
}

impl From<jsonschema::Tag> for Tag {
    fn from(tag: jsonschema::Tag) -> Self {
        let mut tag = tag.type_into_ast();
        tag.infer_name();
        tag.infer_nullability();
        tag
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

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
            data_type: Type::Object(Object::new(HashMap::new(), None)),
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
            assert_eq!(expect, json!(union.collapse().data_type))
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
            assert_eq!(expect, json!(union.collapse().data_type))
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
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "atom": "integer",
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
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
            "atom": "json",
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
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
                "atom_0": {"type": {"atom": "boolean"}, "nullable": false},
                "atom_1": {"type": {"atom": "integer"}, "nullable": false},
                "atom_2": {"type": {"atom": "string"}, "nullable": false},
            }}});
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
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
            "atom": "json",
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_array_nullable_atom() {
        let data = json!({
        "union": {
            "items": [
                {"type": {"array": {"items": {"type": {"atom": "integer"}}}}},
                {"type": {"array": {"items": {"type": "null"}}}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "array": {"items": {"type": {"atom": "integer"}, "nullable": true}}
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_map_nullable_atom() {
        let dtype = Type::Union(Union::new(vec![
            Tag::new(
                Type::Map(Map::new(
                    None,
                    Tag::new(Type::Atom(Atom::Integer), None, false),
                )),
                None,
                false,
            ),
            Tag::new(
                Type::Map(Map::new(None, Tag::new(Type::Null, None, false))),
                None,
                false,
            ),
        ]));
        let expect = json!({
        "map": {
            "key": {
                "type": {"atom": "string"},
                "nullable": false,
            },
            "value": {
                "type": {"atom": "integer"},
                "nullable": true,
            }}});
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_tag_infer_name_object() {
        let data = json!({
        "type": {
            "object": {
                "fields": {
                    "atom_0": {"type": {"atom": "integer"}},
                    "atom_1": {"type": {"atom": "integer"}},
                    "atom_2": {"type": {"atom": "integer"}},
                }}}});
        let mut tag: Tag = serde_json::from_value(data).unwrap();
        tag.infer_name();
        let expect = json!({
        "nullable": false,
        "type": {
            "object": {
                "fields": {
                    "atom_0": {"name": "atom_0", "type": {"atom": "integer"}, "nullable": false},
                    "atom_1": {"name": "atom_1", "type": {"atom": "integer"}, "nullable": false},
                    "atom_2": {"name": "atom_2", "type": {"atom": "integer"}, "nullable": false},
                }}}});
        assert_eq!(expect, json!(tag));
    }
}
