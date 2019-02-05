use serde_json::json;
use std::collections::HashMap;

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
                data_type: Type::String,
                nullable: false,
            }),
            value: Box::new(value),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Type {
    Boolean,
    Integer,
    Number,
    String,
    Object(Object),
    Map(Map),
    Array(Array),
    Json,
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
fn test_serialize_object() {
    let mut field = Field {
        data_type: Type::Object(Object::new()),
        name: Some("test_object".into()),
        nullable: false,
    };
    if let Type::Object(object) = &mut field.data_type {
        object.fields.insert(
            "test_int".into(),
            Box::new(Field {
                data_type: Type::Integer,
                name: Some("test_int".into()),
                nullable: false,
            }),
        );
        object.fields.insert(
            "test_bool".into(),
            Box::new(Field {
                data_type: Type::Boolean,
                name: Some("test_bool".into()),
                nullable: false,
            }),
        );
    }
    let expect = json!({
        "name": "test_object",
        "nullable": false,
        "type": {
            "object": {
                "fields": {
                    "test_int": {
                        "name": "test_int",
                        "type": "integer",
                        "nullable": false
                    },
                    "test_bool": {
                        "name": "test_bool",
                        "type": "boolean",
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
        data_type: Type::Integer,
        name: Some("test_value".into()),
        nullable: false,
    };
    let field = Field {
        data_type: Type::Map(Map::new("test_key".into(), atom)),
        name: Some("test_map".into()),
        nullable: true,
    };
    let expect = json!({
        "name": "test_map",
        "nullable": true,
        "type": {
            "map": {
                "key": {
                    "name": "test_key",
                    "nullable": false,
                    "type": "string",
                },
                "value": {
                    "name": "test_value",
                    "nullable": false,
                    "type": "integer",
                }
            }
        }
    });
    assert_eq!(expect, json!(field));
}

#[test]
fn test_serialize_array() {
    unimplemented!()
}