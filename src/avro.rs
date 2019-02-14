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

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serialize_primitive() {
        unimplemented!()
    }

    #[test]
    fn serialize_complex_record() {
        unimplemented!()
    }

    #[test]
    fn serialize_complex_enum() {
        unimplemented!()
    }

    #[test]
    fn serialize_complex_array() {
        unimplemented!()
    }

    #[test]
    fn serialize_complex_map() {
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
