/// https://avro.apache.org/docs/current/spec.html
use super::ast;
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase", tag = "type")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aliases: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Record {
    #[serde(flatten)]
    common: CommonAttributes,
    fields: Vec<Field>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(tag = "type")]
struct Field {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
    #[serde(flatten)]
    data_type: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
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
#[serde(tag = "type")]
struct Union {
    #[serde(rename = "type")]
    data_type: Vec<Type>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Fixed {
    // this field, however, does not support the doc attribute
    #[serde(flatten)]
    common: CommonAttributes,
    size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase", tag = "type")]
enum Complex {
    Record(Record),
    Enum(Enum),
    Array(Array),
    Map(Map),
    Fixed(Fixed),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Type {
    Primitive(Primitive),
    Complex(Complex),
    // A union is categorized as a complex type, but acts as a top-level type. It is delineated
    // by the presence of a JSON array in the type field.
    Union(Union),
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

    fn type_from_value(value: Value) -> Type {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn serialize_primitive() {
        let schema = Type::Primitive(Primitive::Null);
        let expect = json!({"type": "null"});
        assert_serialize(expect, schema);
    }

    #[test]
    fn serialize_complex_record() {
        let fields = vec![
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
        ];

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
        let schema = Type::Union(Union {
            data_type: vec![
                Type::Primitive(Primitive::Null),
                Type::Primitive(Primitive::Long),
            ],
        });
        let expect = json!({
            "type": [
                {"type": "null"},
                {"type": "long"},
            ]
        });
        assert_serialize(expect, schema);
    }

    #[test]
    fn serialize_complex_fixed() {
        let schema = Type::Complex(Complex::Fixed(Fixed {
            common: CommonAttributes {
                name: "md5".into(),
                ..Default::default()
            },
            size: 16,
        }));
        let expect = json!({
            "type": "fixed",
            "size": 16,
            "name": "md5"
        });
        assert_serialize(expect, schema);
    }

    #[test]
    fn deserialize_primitive() {
        let data = json!({
            "type": "int"
        });
        match type_from_value(data) {
            Type::Primitive(Primitive::Int) => (),
            _ => panic!(),
        }
    }

    #[test]
    fn deserialize_complex_record() {
        let data = json!({
            "type": "record",
            "name": "test-record",
            "fields": [
                {"name": "test-bool", "type": "boolean"},
                {"name": "test-int", "type": "int"},
                {"name": "test-string", "type": "string"},
            ]
        });
        match type_from_value(data) {
            Type::Complex(Complex::Record(record)) => {
                assert_eq!(record.fields[0].name, "test-bool");
                assert_eq!(record.fields[1].name, "test-int");
                assert_eq!(record.fields[2].name, "test-string");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn deserialize_complex_enum() {
        let data = json!({
            "type": "enum",
            "name": "test-enum",
            "symbols": ["A", "B", "C"]
        });
        match type_from_value(data) {
            Type::Complex(Complex::Enum(enum_type)) => {
                assert_eq!(enum_type.symbols, vec!["A", "B", "C"]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn deserialize_complex_array() {
        let data = json!({
            "type": "array",
            "items": {
                "type": "string"
            }
        });
        match type_from_value(data) {
            Type::Complex(Complex::Array(array)) => match *array.items {
                Type::Primitive(Primitive::String) => (),
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[test]
    fn deserialize_complex_map() {
        let data = json!({
            "type": "map",
            "values": {
                "type": "long"
            }
        });
        match type_from_value(data) {
            Type::Complex(Complex::Map(map)) => match *map.values {
                Type::Primitive(Primitive::Long) => (),
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[test]
    fn deserialize_complex_union() {
        let data = json!({
            "type": [
                {"type": "null"},
                {"type": "long"},
            ]
        });
        match type_from_value(data) {
            Type::Union(union) => {
                match union.data_type[0] {
                    Type::Primitive(Primitive::Null) => (),
                    _ => panic!(),
                };
                match union.data_type[1] {
                    Type::Primitive(Primitive::Long) => (),
                    _ => panic!(),
                };
            }
            _ => panic!(),
        }
    }

    #[test]
    fn deserialize_complex_fixed() {
        let data = json!({
            "type": "fixed",
            "size": 16,
            "name": "md5"
        });
        match type_from_value(data) {
            Type::Complex(Complex::Fixed(fixed)) => {
                assert_eq!(fixed.common.name, "md5");
                assert_eq!(fixed.size, 16);
            }
            _ => panic!(),
        }
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
