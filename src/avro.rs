/// https://avro.apache.org/docs/current/spec.html
use super::ast;
use super::TranslateFrom;
use super::{Context, ResolveMethod};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum Primitive {
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
pub struct Record {
    #[serde(flatten)]
    common: CommonAttributes,
    fields: Vec<Field>,
}

// The field doesn't handle the canonical form naively e.g. a null record
// `{"name": "foo", "type": "null"}` must explicitly nest the type in the
// following form: `{"name": "foo", "type": {"type": "null"}}`. Applying
// flattening at this level will produce the wrong results for nested objects.
// We may apply an extra layer of indirection in code by using a `FieldType`,
// but this does not affect correctness of the schema.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(tag = "type")]
struct Field {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
    #[serde(rename = "type")]
    data_type: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Enum {
    #[serde(flatten)]
    common: CommonAttributes,
    symbols: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Array {
    items: Box<Type>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Map {
    values: Box<Type>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Fixed {
    // this field, however, does not support the doc attribute
    #[serde(flatten)]
    common: CommonAttributes,
    size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum Complex {
    Record(Record),
    Enum(Enum),
    Array(Array),
    Map(Map),
    Fixed(Fixed),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Type {
    Primitive(Primitive),
    Complex(Complex),
    // A union is categorized as a complex type, but acts as a top-level type.
    // It is delineated by the presence of a JSON array in the type field. This
    // particular definition allows for nested unions, which is not valid avro.
    Union(Vec<Type>),
}

impl Default for Type {
    fn default() -> Self {
        Type::Primitive(Primitive::Null)
    }
}

impl TranslateFrom<ast::Tag> for Type {
    type Error = String;

    fn translate_from(tag: ast::Tag, context: Context) -> Result<Self, Self::Error> {
        let mut tag = tag;
        if tag.is_root {
            // Name inference is run only from the root for the proper
            // construction of the namespace. Fully qualified names require a
            // top-down approach.
            tag.collapse();
            tag.name = Some("root".into());
            tag.infer_name(context.normalize_case);
        }
        tag.infer_nullability(context.force_nullable);

        let fmt_reason =
            |reason: &str| -> String { format!("{} - {}", tag.fully_qualified_name(), reason) };
        let handle_error = |reason: &str| -> Result<Type, Self::Error> {
            let message = fmt_reason(reason);
            match context.resolve_method {
                ResolveMethod::Cast => {
                    warn!("{}", message);
                    Ok(Type::Primitive(Primitive::String))
                }
                ResolveMethod::Drop => Err(message),
                ResolveMethod::Panic => panic!(message),
            }
        };

        let data_type = match &tag.data_type {
            ast::Type::Null => Type::Primitive(Primitive::Null),
            ast::Type::Atom(atom) => Type::Primitive(match atom {
                ast::Atom::Boolean => Primitive::Boolean,
                ast::Atom::Integer => Primitive::Long,
                ast::Atom::Number => Primitive::Double,
                ast::Atom::String => Primitive::String,
                ast::Atom::Datetime => Primitive::String,
                ast::Atom::Bytes => Primitive::Bytes,
                ast::Atom::JSON => match handle_error("json atom") {
                    Ok(_) => Primitive::String,
                    Err(reason) => return Err(reason),
                },
            }),
            ast::Type::Object(object) => {
                let mut fields: Vec<Field> = if object.fields.is_empty() {
                    Vec::new()
                } else {
                    object
                        .fields
                        .iter()
                        .map(|(k, v)| {
                            let default = if v.nullable { Some(json!(null)) } else { None };
                            (
                                k.to_string(),
                                Type::translate_from(*v.clone(), context),
                                default,
                            )
                        })
                        .filter(|(_, v, _)| v.is_ok())
                        .map(|(name, data_type, default)| Field {
                            name,
                            data_type: data_type.unwrap(),
                            default,
                            ..Default::default()
                        })
                        .collect()
                };

                if fields.is_empty() {
                    handle_error("empty object")?
                } else {
                    fields.sort_by_key(|v| v.name.to_string());
                    let record = Record {
                        common: CommonAttributes {
                            // This is not a safe assumption
                            name: tag.name.clone().unwrap_or_else(|| "__UNNAMED__".into()),
                            namespace: tag.namespace.clone(),
                            ..Default::default()
                        },
                        fields,
                    };
                    if record.common.name == "__UNNAMED__" {
                        warn!("{} - Unnamed field", tag.fully_qualified_name());
                    }
                    Type::Complex(Complex::Record(record))
                }
            }
            ast::Type::Tuple(tuple) => {
                let fields = tuple
                    .items
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let default = if v.nullable { Some(json!(null)) } else { None };
                        (
                            format!("f{}_", i),
                            Type::translate_from(v.clone(), context),
                            default,
                        )
                    })
                    .filter(|(_, v, _)| v.is_ok())
                    .map(|(name, data_type, default)| Field {
                        name,
                        data_type: data_type.unwrap(),
                        default,
                        ..Default::default()
                    })
                    .collect();
                let record = Record {
                    common: CommonAttributes {
                        name: tag.name.clone().unwrap_or_else(|| "__UNNAMED__".into()),
                        namespace: tag.namespace.clone(),
                        ..Default::default()
                    },
                    fields,
                };
                if record.common.name == "__UNNAMED__" {
                    warn!("{} - Unnamed field", tag.fully_qualified_name());
                }
                Type::Complex(Complex::Record(record))
            }
            ast::Type::Array(array) => {
                let child_is_array = match &array.items.data_type {
                    ast::Type::Array(_) => true,
                    _ => false,
                };
                match Type::translate_from(*array.items.clone(), context) {
                    Ok(data_type) => {
                        if child_is_array {
                            Type::Complex(Complex::Array(Array {
                                items: Box::new(Type::Complex(Complex::Record(Record {
                                    common: CommonAttributes {
                                        name: tag
                                            .name
                                            .clone()
                                            .unwrap_or_else(|| "__UNNAMED__".into()),
                                        namespace: tag.namespace.clone(),
                                        ..Default::default()
                                    },
                                    fields: vec![Field {
                                        name: "list".into(),
                                        data_type,
                                        ..Default::default()
                                    }],
                                }))),
                            }))
                        } else {
                            Type::Complex(Complex::Array(Array {
                                items: Box::new(data_type),
                            }))
                        }
                    }
                    Err(_) => return Err(fmt_reason("untyped array")),
                }
            }
            ast::Type::Map(map) => match Type::translate_from(*map.value.clone(), context) {
                Ok(data_type) => Type::Complex(Complex::Map(Map {
                    values: Box::new(data_type),
                })),
                // Err is only reachable when context.resolve_method is Drop
                Err(_) => {
                    return if context.allow_maps_without_value {
                        Err(fmt_reason("map value cannot be dropped in avro"))
                    } else {
                        Err(fmt_reason("untyped map value"))
                    }
                }
            },
            _ => handle_error("unknown type")?,
        };
        if tag.nullable && !tag.is_null() {
            Ok(Type::Union(vec![
                Type::Primitive(Primitive::Null),
                data_type,
            ]))
        } else {
            Ok(data_type)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn assert_serialize(expect: Value, schema: Type) {
        assert_eq!(expect, json!(schema))
    }

    fn type_from_value(value: Value) -> Type {
        serde_json::from_value(value).unwrap()
    }

    fn assert_from_ast_eq(ast: Value, avro: Value) {
        let context = Context {
            ..Default::default()
        };
        let tag: ast::Tag = serde_json::from_value(ast).unwrap();
        let from_tag = Type::translate_from(tag, context).unwrap();
        assert_eq!(avro, json!(from_tag))
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
                {"name": "test-bool", "type": {"type": "boolean"}},
                {"name": "test-int", "type": {"type": "int"}},
                {"name": "test-string", "type": {"type": "string"}},
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
        let schema = Type::Union(vec![
            Type::Primitive(Primitive::Null),
            Type::Primitive(Primitive::Long),
        ]);
        let expect = json!([
            {"type": "null"},
            {"type": "long"},
        ]);
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
                {"name": "test-bool", "type": {"type": "boolean"}},
                {"name": "test-int", "type": {"type": "int"}},
                {"name": "test-string", "type": {"type": "string"}},
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
        let data = json!([
            {"type": "null"},
            {"type": "long"},
        ]);
        match type_from_value(data) {
            Type::Union(union) => {
                match union[0] {
                    Type::Primitive(Primitive::Null) => (),
                    _ => panic!(),
                };
                match union[1] {
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
        let ast = json!({"type": "null"});
        let avro = json!({"type": "null"});
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    fn from_ast_atom() {
        let ast = json!({"type": {"atom": "integer"}});
        let avro = json!({"type": "long"});
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    fn from_ast_object() {
        // Note the inclusion of `is_root`, which is required for proper
        // namespace resolution. For testing purposes, this property is only
        // included when testing deeply nested data-structures.
        let ast = json!({
            "is_root": true,
            "type": {"object": {
                // An additional case could be made for the behavior of nested
                // structs and nested arrays. A nullable array for example may
                // not be a valid structure in bigquery.
                "required": ["1-test-int", "3-test-nested", "4-test-array"],
                "fields": {
                    "0-test-null": {"type": "null"},
                    "1-test-int": {"type": {"atom": "integer"}},
                    "2-test-null-int": {"type": {"atom": "integer"}, "nullable": true},
                    "3-test-nested": {"type": {"object": {"fields": {
                        "test-bool": {
                            "type": {"atom": "boolean"},
                            "nullable": true
                            }}}}},
                    "4-test-array": {"type": {"array": {
                        "items": {"type": {"atom": "integer"}}}}},
                    "$invalid-name": {"type": "null"}
            }}}
        });
        let avro = json!({
            "type": "record",
            "name": "root",
            "fields": [
                {"name": "_0_test_null", "type": {"type": "null"}, "default": null},
                {"name": "_1_test_int", "type": {"type": "long"}},
                {"name": "_2_test_null_int",
                    "type": [
                        {"type": "null"},
                        {"type": "long"},
                    ],
                    "default": null,
                },
                {"name": "_3_test_nested", "type": {
                    "name": "_3_test_nested",
                    "namespace": "root",
                    "type": "record",
                    "fields": [
                        {"name": "test_bool",
                            "type": [
                                {"type": "null"},
                                {"type": "boolean"},
                            ],
                            "default": null,
                        },
                    ]}},
                {"name": "_4_test_array", "type": {
                    "type": "array",
                    "items": {"type": "long"}
                }}
            ]
        });
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    fn from_ast_map() {
        let ast = json!({
            "type": {"map": {
                "key": {"type": {"atom": "string"}},
                "value": {"type": {"atom": "integer"}}
        }}});
        let avro = json!({
            "type": "map",
            "values": {"type": "long"}
        });
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    fn from_ast_array() {
        let ast = json!({
            "type": {"array": {"items": {
                "type": {"atom": "string"}}}}
        });
        let avro = json!({
            "type": "array",
            "items": {"type": "string"}
        });
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    fn from_ast_array_array() {
        let ast = json!({
            "is_root": true,
            "type": {"array": {"items": {
                "type": {"array": {"items":
                    {"type": {"atom": "integer"}}}}}}}
        });
        let avro = json!({
            "type": "array",
            "items":
                {
                    "type": "record",
                    "name": "root",
                    "fields": [
                        {
                            "name": "list",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "long"
                                }
                            }
                        }
                    ]
                }
        });
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    fn from_ast_tuple() {
        // This case is not handled and instead converted into an object
        let ast = json!({
            "type": {
                "tuple": {
                    "items": [
                        {"type": {"atom": "boolean"}},
                        {"type": {"atom": "integer"}},
                    ]
                }
            }
        });
        let avro = json!({
            "type": "record",
            "name": "__UNNAMED__",
            "fields": [
                {"name": "f0_", "type": {"type": "boolean"}},
                {"name": "f1_", "type": {"type": "long"}}
            ]
        });
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    /// The union type is collapsed before being reconstructed
    fn from_ast_union() {
        let ast = json!({
            // test this document as if it were root
            "is_root": true,
            "type": {"union": {"items": [
                {"type": "null"},
                {"type": {"atom": "boolean"}},
            ]}}
        });
        let avro = json!([
            {"type": "null"},
            {"type": "boolean"}
        ]);
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    fn from_ast_datetime() {
        let ast = json!({"type": {"atom": "datetime"}});
        let avro = json!({"type": "string"});
        assert_from_ast_eq(ast, avro);
    }

    #[test]
    fn from_ast_bytes() {
        let ast = json!({"type": {"atom": "bytes"}});
        let avro = json!({"type": "bytes"});
        assert_from_ast_eq(ast, avro);
    }
}
