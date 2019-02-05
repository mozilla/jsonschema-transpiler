use serde_json::json;
use std::collections::HashMap;

type Object = HashMap<String, Box<Field>>;
type Array = Box<Field>;
struct Map {
    key: String,
    value: Box<Field>,
}

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

struct Field {
    data_type: Type,
    name: Option<String>,
    nullable: bool,
}

impl Field {
    fn new() -> Self {
        Field {
            data_type: Type::Object(HashMap::new()),
            name: None,
            nullable: false,
        }
    }

    fn insert(&mut self, name: String, node: Field) {
        if let Type::Object(values) = &mut self.data_type {
            if values.contains_key(&name) {
                panic!();
            }
            values.insert(name, Box::new(node));
        }
    }
}

#[test]
fn test_object_insert() {
    let node = Field::new();
}