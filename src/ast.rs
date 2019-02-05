use serde_json::json;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Atom {
    Boolean,
    Integer,
    Number,
    String,
    JSON,
}

#[derive(Serialize, Deserialize, Debug)]
struct Object {
    fields: HashMap<String, Box<Field>>,
}

impl Object {
    fn new() -> Self {
        Object {
            fields: HashMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Array {
    items: Box<Field>,
}

impl Array {
    fn new(items: Field) -> Self {
        Array {
            items: Box::new(items),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Map {
    key: Box<Field>,
    value: Box<Field>,
}

impl Map {
    fn new(key: String, value: Field) -> Self {
        Map {
            key: Box::new(Field {
                name: Some(key),
                data_type: Type::Atom(Atom::String),
                nullable: false,
            }),
            value: Box::new(value),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Type {
    Atom(Atom),
    Object(Object),
    Map(Map),
    Array(Array),
    // Union
    // Intersection
    // Not
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
struct Field {
    #[serde(rename = "type")]
    data_type: Type,
    name: Option<String>,
    nullable: bool,
}

#[test]
fn test_serialize_atom() {
    let atom = Field {
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
    let mut field = Field {
        data_type: Type::Object(Object::new()),
        name: Some("test-object".into()),
        nullable: false,
    };
    if let Type::Object(object) = &mut field.data_type {
        object.fields.insert(
            "test-int".into(),
            Box::new(Field {
                data_type: Type::Atom(Atom::Integer),
                name: Some("test-int".into()),
                nullable: false,
            }),
        );
        object.fields.insert(
            "test-bool".into(),
            Box::new(Field {
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
    let atom = Field {
        data_type: Type::Atom(Atom::Integer),
        name: Some("test-value".into()),
        nullable: false,
    };
    let field = Field {
        data_type: Type::Map(Map::new("test-key".into(), atom)),
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
    let atom = Field {
        data_type: Type::Atom(Atom::Integer),
        name: Some("test-int".into()),
        nullable: true,
    };
    let field = Field {
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
