use super::ast;
use super::TranslateFrom;
use super::{Context, ResolveMethod};
use std::collections::HashMap;

const DEFAULT_COLUMN: &str = "root";
const MAX_DESCRIPTION_LENGTH: usize = 1024;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE", tag = "type")]
pub enum Atom {
    Int64,
    Numeric,
    Float64,
    Bool,
    String,
    Bytes,
    Date,
    Datetime,
    Geography,
    Time,
    Timestamp,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub enum Mode {
    Nullable,
    Required,
    Repeated,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename = "RECORD")]
pub struct Record {
    #[serde(with = "fields_as_vec")]
    fields: HashMap<String, Box<Tag>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Type {
    Atom(Atom),
    Record(Record),
}

/// See: https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub struct Tag {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(flatten, rename = "type")]
    data_type: Box<Type>,
    mode: Mode,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl TranslateFrom<ast::Tag> for Tag {
    type Error = String;

    fn translate_from(tag: ast::Tag, context: Context) -> Result<Self, Self::Error> {
        let mut tag = tag;
        tag.collapse();
        tag.infer_name(context.normalize_case);
        tag.infer_nullability(context.force_nullable);

        let fmt_reason =
            |reason: &str| -> String { format!("{} - {}", tag.fully_qualified_name(), reason) };
        let handle_error = |reason: &str| -> Result<Type, Self::Error> {
            let message = fmt_reason(reason);
            match context.resolve_method {
                ResolveMethod::Cast => {
                    warn!("{}", message);
                    Ok(Type::Atom(Atom::String))
                }
                ResolveMethod::Drop => Err(message),
                ResolveMethod::Panic => panic!(message),
            }
        };

        let data_type = match &tag.data_type {
            ast::Type::Atom(atom) => Type::Atom(match atom {
                ast::Atom::Boolean => Atom::Bool,
                ast::Atom::Integer => Atom::Int64,
                ast::Atom::Number => Atom::Float64,
                ast::Atom::String => Atom::String,
                ast::Atom::Datetime => Atom::Timestamp,
                ast::Atom::Bytes => Atom::Bytes,
                ast::Atom::JSON => match handle_error("json atom") {
                    Ok(_) => Atom::String,
                    Err(reason) => return Err(reason),
                },
            }),
            ast::Type::Object(object) => {
                let fields: HashMap<String, Box<Tag>> = if object.fields.is_empty() {
                    HashMap::new()
                } else {
                    object
                        .fields
                        .iter()
                        .map(|(k, v)| (k.to_string(), Tag::translate_from(*v.clone(), context)))
                        .filter(|(_, v)| v.is_ok())
                        .map(|(k, v)| (k, Box::new(v.unwrap())))
                        .collect()
                };

                if fields.is_empty() {
                    handle_error("empty object")?
                } else {
                    Type::Record(Record { fields })
                }
            }
            ast::Type::Tuple(tuple) => {
                let fields: Result<Vec<_>, _> = tuple
                    .items
                    .iter()
                    .map(|v| Tag::translate_from(v.clone(), context))
                    .collect();

                let named_fields: HashMap<String, Box<Tag>> = fields?
                    .into_iter()
                    .enumerate()
                    .map(|(i, mut v)| {
                        // The name is actually derived from the value, not from
                        // the associated key. Modify the name in place.
                        let name = format!("f{}_", i);
                        v.name = Some(name.clone());
                        (name, Box::new(v))
                    })
                    .collect();

                Type::Record(Record {
                    fields: named_fields,
                })
            }
            ast::Type::Array(array) => {
                // workaround for nested lists
                let child_is_array = match &array.items.data_type {
                    ast::Type::Array(_) => true,
                    _ => false,
                };
                let sub_tag = match Tag::translate_from(*array.items.clone(), context) {
                    Ok(tag) => tag,
                    Err(_) => return Err(fmt_reason("untyped array")),
                };
                if child_is_array {
                    Type::Record(Record {
                        fields: hashmap! {"list".into() => Box::new(sub_tag)},
                    })
                } else {
                    *sub_tag.data_type
                }
            }
            ast::Type::Map(map) => {
                let key = Tag::translate_from(*map.key.clone(), context).unwrap();
                let value = match Tag::translate_from(*map.value.clone(), context) {
                    Ok(tag) => Some(tag),
                    Err(_) => {
                        if context.allow_maps_without_value {
                            None
                        } else {
                            return Err(fmt_reason("untyped map value"));
                        }
                    }
                };
                let fields: HashMap<String, Box<Tag>> =
                    vec![Some(("key", key)), value.map(|v| ("value", v))]
                        .into_iter()
                        .flatten()
                        .map(|(k, v)| (k.to_string(), Box::new(v)))
                        .collect();
                Type::Record(Record { fields })
            }
            _ => handle_error("unknown type")?,
        };

        let mode = if tag.is_array() || tag.is_map() {
            Mode::Repeated
        } else if tag.is_null() || tag.nullable {
            Mode::Nullable
        } else {
            Mode::Required
        };

        // If schema uses both title and description, format description as "<title> - <description>".
        // Otherwise set description to whatever is available.
        let description = match tag.title {
            Some(title) => match tag.description {
                Some(description) => Some(format!("{} - {}", title, description)),
                None => Some(title),
            },
            None => tag.description,
        };

        // The maximum length is 1024 characters for BigQuery schemas
        let truncated_description = match description {
            Some(d) => Some(d.chars().take(MAX_DESCRIPTION_LENGTH).collect()),
            None => None,
        };

        Ok(Tag {
            name: tag.name.clone(),
            data_type: Box::new(data_type),
            mode,
            description: truncated_description,
        })
    }
}

/// BigQuery expects a schema that begins as JSON array when creating or
/// updating a table. This enum extracts or wraps the appropriate tag generated
/// from the ast. When the root is a record, the fields will be extracted from
/// the schema that is logically equivalent to `jq '.fields'`. When it is an
/// atom, array, or map, the tag is renamed to `root` and placed as a single
/// element in a wrapped array.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Schema {
    Root(Vec<Tag>),
}

impl TranslateFrom<ast::Tag> for Schema {
    type Error = &'static str;

    fn translate_from(tag: ast::Tag, context: Context) -> Result<Self, Self::Error> {
        let mut bq_tag = Tag::translate_from(tag.clone(), context).unwrap();
        match *bq_tag.data_type {
            // Maps and arrays are both treated as a Record type with different
            // modes. These should not be extracted if they are the root-type.
            Type::Record(_) if tag.is_array() || tag.is_map() => {
                assert!(bq_tag.name.is_none());
                bq_tag.name = Some(DEFAULT_COLUMN.into());
                Ok(Schema::Root(vec![bq_tag]))
            }
            Type::Atom(_) => {
                assert!(bq_tag.name.is_none());
                bq_tag.name = Some(DEFAULT_COLUMN.into());
                Ok(Schema::Root(vec![bq_tag]))
            }
            Type::Record(record) => {
                let mut vec: Vec<_> = record.fields.into_iter().collect();
                vec.sort_by_key(|(key, _)| key.to_string());
                let columns = vec.into_iter().map(|(_, v)| *v).collect();
                Ok(Schema::Root(columns))
            }
        }
    }
}

/// Allows serialization from a HashMap to a Vector. This makes it possible to
/// traverse any given path in time linear to the depth of the schema.
///
/// See: https://serde.rs/custom-date-format.html#date-in-a-custom-format
mod fields_as_vec {
    use super::Tag;
    use serde::ser::{SerializeSeq, Serializer};
    use serde::{Deserialize, Deserializer};
    use std::collections::HashMap;
    use std::iter::FromIterator;

    pub fn serialize<S>(map: &HashMap<String, Box<Tag>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(map.len()))?;
        let mut vec: Vec<(String, &Tag)> = map.iter().map(|(k, v)| (k.to_string(), &**v)).collect();
        vec.sort_by_key(|(k, _)| k.to_string());

        for (_, element) in vec {
            seq.serialize_element(element)?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, Box<Tag>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Vec<Box<Tag>> = Vec::deserialize(deserializer)?;
        let map = HashMap::<String, Box<Tag>>::from_iter(s.into_iter().map(|record| {
            let name: String = (*record).name.clone().unwrap();
            (name, record)
        }));
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::super::traits::TranslateInto;
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::{self, json, Value};

    fn transform_tag(data: Value) -> Value {
        let context = Context {
            ..Default::default()
        };
        let ast_tag: ast::Tag = serde_json::from_value(data).unwrap();
        let bq_tag: Tag = ast_tag.translate_into(context).unwrap();
        json!(bq_tag)
    }

    fn transform_tag_with_context(data: Value, context: Context) -> Value {
        let ast_tag: ast::Tag = serde_json::from_value(data).unwrap();
        let bq_tag: Tag = ast_tag.translate_into(context).unwrap();
        json!(bq_tag)
    }

    fn transform_schema(data: Value) -> Value {
        let context = Context {
            ..Default::default()
        };
        let ast_tag: ast::Tag = serde_json::from_value(data).unwrap();
        let bq_tag: Schema = ast_tag.translate_into(context).unwrap();
        json!(bq_tag)
    }

    #[test]
    fn test_serialize_atom() {
        let atom = Tag {
            name: None,
            data_type: Box::new(Type::Atom(Atom::Bool)),
            mode: Mode::Nullable,
            description: None,
        };
        let expect = json!({
            "type": "BOOL",
            "mode": "NULLABLE",
        });
        assert_eq!(expect, json!(atom))
    }

    #[test]
    fn test_deserialize_atom() {
        let atom: Tag = serde_json::from_value(json!({
            "name": "test-int",
            "mode": "REPEATED",
            "type": "INT64"
        }))
        .unwrap();

        match atom.name {
            Some(name) => assert_eq!(name, "test-int"),
            _ => panic!(),
        };
        match *atom.data_type {
            Type::Atom(Atom::Int64) => (),
            _ => panic!(),
        };
        match atom.mode {
            Mode::Repeated => (),
            _ => panic!(),
        };
    }

    #[test]
    fn test_serialize_record() {
        let atom = Tag {
            name: Some("test-int".into()),
            data_type: Box::new(Type::Atom(Atom::Int64)),
            mode: Mode::Nullable,
            description: Some("test description".to_string()),
        };

        let mut record = Record {
            fields: HashMap::new(),
        };
        record.fields.insert("test-int".into(), Box::new(atom));

        let root = Tag {
            name: None,
            data_type: Box::new(Type::Record(record)),
            mode: Mode::Nullable,
            description: Some("test description".to_string()),
        };

        let expect = json!({
            "type": "RECORD",
            "mode": "NULLABLE",
            "description": "test description",
            "fields": [{
                "name": "test-int",
                "type": "INT64",
                "mode": "NULLABLE",
                "description": "test description"
            }]
        });

        assert_eq!(expect, json!(root))
    }

    #[test]
    fn test_deserialize_record() {
        let record: Tag = serde_json::from_value(json!({
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [{
                "name": "test-int",
                "type": "INT64",
                "mode": "NULLABLE",
                "description": "test description"
            }]
        }))
        .unwrap();

        let test_int = match &*record.data_type {
            Type::Record(record) => &record.fields["test-int"],
            _ => panic!(),
        };
        match *test_int.data_type {
            Type::Atom(Atom::Int64) => (),
            _ => panic!(),
        };

        if test_int.description.as_ref().unwrap() != "test description" {
            panic!();
        }
    }

    #[test]
    fn test_serialize_nested_record() {
        let atom = Tag {
            name: Some("test-int".into()),
            data_type: Box::new(Type::Atom(Atom::Int64)),
            mode: Mode::Nullable,
            description: Some("innermost record".to_string()),
        };

        let mut record_b = Record {
            fields: HashMap::new(),
        };
        record_b.fields.insert("test-int".into(), Box::new(atom));

        let tag_b = Tag {
            name: Some("test-record-b".into()),
            data_type: Box::new(Type::Record(record_b)),
            mode: Mode::Nullable,
            description: Some("inner record".to_string()),
        };

        let mut record_a = Record {
            fields: HashMap::new(),
        };
        record_a
            .fields
            .insert("test-record-b".into(), Box::new(tag_b));

        let root = Tag {
            name: Some("test-record-a".into()),
            data_type: Box::new(Type::Record(record_a)),
            mode: Mode::Nullable,
            description: Some("outer record".to_string()),
        };

        let expect = json!({
            "name": "test-record-a",
            "type": "RECORD",
            "mode": "NULLABLE",
            "description": "outer record",
            "fields": [{
                "name": "test-record-b",
                "type": "RECORD",
                "description": "inner record",
                "fields": [{
                    "name": "test-int",
                    "type": "INT64",
                    "mode": "NULLABLE",
                    "description": "innermost record"
                }],
                "mode": "NULLABLE"
            }]
        });

        assert_eq!(expect, json!(root))
    }

    #[test]
    fn test_deserialize_nested_record() {
        let data = json!({
            "name": "test-record-a",
            "type": "RECORD",
            "mode": "NULLABLE",
            "description": "outer record",
            "fields": [{
                "name": "test-record-b",
                "type": "RECORD",
                "description": "inner record",
                "fields": [{
                    "name": "test-int",
                    "type": "INT64",
                    "mode": "NULLABLE",
                    "description": "innermost record"
                }],
                "mode": "NULLABLE"
            }]
        });
        let record_a: Tag = serde_json::from_value(data).unwrap();
        let record_b = match &*record_a.data_type {
            Type::Record(record) => &record.fields["test-record-b"],
            _ => panic!(),
        };
        let test_int = match &*record_b.data_type {
            Type::Record(record) => &record.fields["test-int"],
            _ => panic!(),
        };
        match *test_int.data_type {
            Type::Atom(Atom::Int64) => (),
            _ => panic!(),
        }
    }

    #[test]
    fn test_from_ast_null() {
        let data = json!({
            "type": "null"
        });
        let expect = json!({
            "type": "STRING",
            "mode": "NULLABLE",
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_atom() {
        let data = json!({
            "type": {"atom": "integer"}
        });
        let expect = json!({
            "type": "INT64",
            "mode": "REQUIRED",
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_nullable_atom() {
        let data = json!({
            "type": {"atom": "integer"},
            "nullable": true,
        });
        let expect = json!({
            "type": "INT64",
            "mode": "NULLABLE",
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_union_nullable_atom() {
        let data = json!({
        "type": {
            "union": {
                "items": [
                    {"type": "null"},
                    {"type": {"atom": "integer"}},
        ]}}});
        let expect = json!({
            "type": "INT64",
            "mode": "NULLABLE",
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_object() {
        let data = json!({
        "type": {
            "object": {
                "required": ["test-atom", "test-object"],
                "fields": {
                    "test-null": {"type": "null"},
                    "test-atom": {"type": {"atom": "integer"}},
                    "test-object": {"type": {
                        "object": {
                            "fields": {
                                "test-nested-atom": {"type": {"atom": "number"}}
                            }}}}}}}});
        let expect = json!({
            "type": "RECORD",
            "mode": "REQUIRED",
            "fields": [
                {"name": "test_atom", "type": "INT64", "mode": "REQUIRED"},
                {"name": "test_null", "type": "STRING", "mode": "NULLABLE"},
                {"name": "test_object", "type": "RECORD", "mode": "REQUIRED", "fields": [
                    {"name": "test_nested_atom", "type": "FLOAT64", "mode": "NULLABLE"},
                ]},
            ]
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_array() {
        let data = json!({
        "type": {
            "array": {
                "items": {
                    "type": {"atom": "integer"}},
        }}});
        let expect = json!({
            "type": "INT64",
            "mode": "REPEATED",
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn from_bigquery_array_array() {
        let data = json!({
            "type": {"array": {"items": {
                "type": {"array": {"items":
                    {"type": {"atom": "integer"}}}}}}}
        });
        let expect = json!({
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "list",
                    "type": "INT64",
                    "mode": "REPEATED"
                }
            ]
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_tuple() {
        let data = json!({
            "type": {
                "tuple": {
                    "items": [
                        {"type": {"atom": "boolean"}},
                        {"type": {"atom": "integer"}},
                    ]
                }
            }
        });
        let expect = json!({
            "type": "RECORD",
            "mode": "REQUIRED",
            "fields": [
                {"name": "f0_", "type": "BOOL", "mode": "REQUIRED"},
                {"name": "f1_", "type": "INT64", "mode": "REQUIRED"}
            ]
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_map() {
        let data = json!({
        "type": {
            "map": {
                "key": {"type": {"atom": "string"}},
                "value": {"type": {"atom": "integer"}}
        }}});
        let expect = json!({
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "key", "type": "STRING", "mode": "REQUIRED"},
            {"name": "value", "type": "INT64", "mode": "REQUIRED"},
        ]});
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_map_without_value() {
        let data = json!({
        "description": "foo",
        "type": {
            "map": {
                "key": {"type": {"atom": "string"}},
                "value": {"type": {"atom": "json"}},
        }}});
        let expect = json!({
        "type": "RECORD",
        "mode": "REPEATED",
        "description": "foo",
        "fields": [
            {"name": "key", "type": "STRING", "mode": "REQUIRED"},
        ]});
        let context = Context {
            resolve_method: ResolveMethod::Drop,
            normalize_case: false,
            force_nullable: false,
            tuple_struct: false,
            allow_maps_without_value: true,
        };
        assert_eq!(expect, transform_tag_with_context(data, context));
    }

    #[test]
    fn test_from_ast_datetime() {
        let data = json!({
            "type": {"atom": "datetime"},
            "nullable": true
        });
        let expect = json!({
           "type": "TIMESTAMP",
           "mode": "NULLABLE",
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_from_ast_bytes() {
        let data = json!({
            "type": {"atom": "bytes"},
            "nullable": true
        });
        let expect = json!({
           "type": "BYTES",
           "mode": "NULLABLE",
        });
        assert_eq!(expect, transform_tag(data));
    }

    #[test]
    fn test_schema_from_ast_atom() {
        // Nameless tags are top-level fields that should be rooted by default
        let data = json!({"type": {"atom": "integer"}});
        let expect = json!([{
            "name": DEFAULT_COLUMN,
            "mode": "REQUIRED",
            "type": "INT64"
        }]);
        assert_eq!(expect, transform_schema(data));
    }

    #[test]
    fn test_schema_from_ast_object() {
        // The single column is extracted
        let data = json!({
        "type": {
            "object": {
                "required": ["test-object"],
                "fields": {
                    "test-object": {"type": {
                        "object": {
                            "required": ["test-nested-atom"],
                            "fields": {
                                "test-nested-atom": {"type": {"atom": "boolean"}}
                            }}}}}}}});
        let expect = json!([{
            "name": "test_object",
            "mode": "REQUIRED",
            "type": "RECORD",
            "fields": [
                {
                    "name": "test_nested_atom",
                    "mode": "REQUIRED",
                    "type": "BOOL"
                }
            ]
        }]);
        assert_eq!(expect, transform_schema(data));
    }

    #[test]
    fn test_schema_from_ast_map() {
        let data = json!({
        "type": {
            "map": {
                "key": {"type": {"atom": "string"}},
                "value": {"type": {"atom": "integer"}}
        }}});
        let expect = json!([{
            "name": "root",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "key", "type": "STRING", "mode": "REQUIRED"},
                {"name": "value", "type": "INT64", "mode": "REQUIRED"},
            ]}]
        );
        assert_eq!(expect, transform_schema(data));
    }
}
