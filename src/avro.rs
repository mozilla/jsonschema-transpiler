/// https://avro.apache.org/docs/current/spec.html
use super::ast;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
enum Primitive {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct CommonAttributes {
    name: String,
    namespace: Option<String>,
    doc: Option<String>,
    aliases: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Record {
    #[serde(flatten)]
    common: CommonAttributes,
    fields: HashMap<String, Field>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Field {
    name: String,
    doc: Option<String>,
    data_type: Type,
    default: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Enum {
    #[serde(flatten)]
    common: CommonAttributes,
    symbols: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Array {
    items: Box<Type>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Map {
    values: Box<Type>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Fixed {
    // this field, however, does not support the doc attribute
    #[serde(flatten)]
    common: CommonAttributes,
    size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
enum Complex {
    Record(Record),
    Enum(Enum),
    Array(Array),
    Map(Map),
    Fixed(Fixed),
}

#[derive(Serialize, Deserialize, Debug)]
enum Type {
    Primitive(Primitive),
    Complex(Complex),
}

impl Default for Type {
    fn default() -> Self {
        Type::Primitive(Primitive::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assert_serialize(expect: Value, schema: Type) {
        assert_eq!(expect, json!(schema))
    }

    #[test]
    fn serialize_primitive() {
        let schema = Type::Primitive(Primitive::Null);
        let expect = json!({"type": "null"});
        assert_serialize(expect, schema);
    }

    #[test]
    fn serialize_complex_record() {
        let fields: HashMap<String, Field> = vec![
            Field {
                name: "test-bool".into(),
                data_type: Type::Primitive(Primitive::Boolean),
                ..Default::default()
            },
            Field {
                name: "test-int".into(),
                data_type: Type::Primitive(Primitive::Int),
                ..Default::default()
            },
            Field {
                name: "test-string".into(),
                data_type: Type::Primitive(Primitive::String),
                ..Default::default()
            },
        ]
        .into_iter()
        .map(|field| (field.name.to_string(), field))
        .collect();

        let schema = Type::Complex(Complex::Record(Record {
            common: CommonAttributes {
                name: "test-record".into(),
                ..Default::default()
            },
            fields,
        }));

        let expect = json!({
            "type": "record",
            "name": "test-record",
            "fields": [
                {"name": "test-bool", "type": "boolean"},
                {"name": "test-int", "type": "int"},
                {"name": "test-string", "type": "string"},
            ]
        });

        assert_serialize(expect, schema);
    }

    #[test]
    fn serialize_complex_enum() {
        let schema = Type::Complex(Complex::Enum(Enum {
            common: CommonAttributes {
                name: "test-enum".into(),
                ..Default::default()
            },
            symbols: vec!["A".into(), "B".into(), "C".into()],
        }));
        let expect = json!({
            "type": "enum",
            "name": "test-enum",
            "symbols": ["A", "B", "C"]
        });
        assert_serialize(expect, schema);
    }

    #[test]
    fn serialize_complex_array() {
        let schema = Type::Complex(Complex::Array(Array {
            items: Box::new(Type::Primitive(Primitive::String)),
        }));
        let expect = json!({
            "type": "array",
            "items": {
                "type": "string"
            }
        });
        assert_serialize(expect, schema);
    }

    #[test]
    fn serialize_complex_map() {
        let schema = Type::Complex(Complex::Map(Map {
            values: Box::new(Type::Primitive(Primitive::Long)),
        }));
        let expect = json!({
            "type": "map",
            "values": {
                "type": "long"
            }
        });
        assert_serialize(expect, schema);
    }

    #[test]
    fn serialize_complex_union() {
        unimplemented!()
    }

    #[test]
    fn serialize_complex_fixed() {
        unimplemented!()
    }

    #[test]
    fn deserialize_primitive() {
        unimplemented!()
    }

    #[test]
    fn deserialize_complex_record() {
        unimplemented!()
    }

    #[test]
    fn deserialize_complex_enum() {
        unimplemented!()
    }

    #[test]
    fn deserialize_complex_array() {
        unimplemented!()
    }

    #[test]
    fn deserialize_complex_map() {
        unimplemented!()
    }

    #[test]
    fn deserialize_complex_union() {
        unimplemented!()
    }

    #[test]
    fn deserialize_complex_fixed() {
        unimplemented!()
    }

    #[test]
    fn from_ast_null() {
        unimplemented!()
    }

    #[test]
    fn from_ast_atom() {
        unimplemented!()
    }

    #[test]
    fn from_ast_object() {
        unimplemented!()
    }

    #[test]
    fn from_ast_map() {
        unimplemented!()
    }

    #[test]
    fn from_ast_array() {
        unimplemented!()
    }

    #[test]
    fn from_ast_union() {
        unimplemented!()
    }
}
