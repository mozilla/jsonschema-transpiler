use super::ast;
use serde::de::{self, Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;

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

#[derive(Serialize, Debug)]
#[serde(rename_all = "UPPERCASE", tag = "type")]
pub enum Type {
    Atom(Atom),
    // Array(Tag)
    // Struct
    Record(Record),
}

impl<'de> Deserialize<'de> for Type {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "UPPERCASE", tag = "type")]
        enum TypeHelper {
            Record,
        };

        let v = Value::deserialize(deserializer)?;

        // Try to deserialize the type as an atom first
        if let Ok(atom) = Atom::deserialize(&v) {
            return Ok(Type::Atom(atom));
        } else if let Ok(data_type) = TypeHelper::deserialize(&v) {
            return match data_type {
                TypeHelper::Record => {
                    let record = Record::deserialize(&v).map_err(de::Error::custom)?;
                    Ok(Type::Record(record))
                }
            };
        } else {
            return Err(de::Error::custom("Error deserializing type!"));
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Record {
    #[serde(with = "fields_as_vec")]
    fields: HashMap<String, Box<Tag>>,
}

/// See: https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub struct Tag {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(flatten)]
    #[serde(rename = "type")]
    data_type: Box<Type>,
    mode: Mode,
}

impl From<ast::Tag> for Tag {
    fn from(tag: ast::Tag) -> Tag {
        let mut tag = tag;
        tag.collapse();
        tag.infer_name();
        tag.infer_nullability();
        let data_type = match &tag.data_type {
            ast::Type::Atom(atom) => Type::Atom(match atom {
                ast::Atom::Boolean => Atom::Bool,
                ast::Atom::Integer => Atom::Int64,
                ast::Atom::Number => Atom::Float64,
                ast::Atom::String => Atom::String,
                ast::Atom::JSON => {
                    warn!(
                        "{} - Treating subschema as JSON string",
                        tag.fully_qualified_name()
                    );
                    Atom::String
                }
            }),
            ast::Type::Object(object) if object.fields.is_empty() => {
                warn!(
                    "{} - Empty records are not supported, casting into a JSON string",
                    tag.fully_qualified_name()
                );
                Type::Atom(Atom::String)
            }
            ast::Type::Object(object) => {
                let fields: HashMap<String, Box<Tag>> = object
                    .fields
                    .iter()
                    .map(|(k, v)| (k.to_string(), Box::new(Tag::from(*v.clone()))))
                    .collect();
                Type::Record(Record { fields })
            }
            ast::Type::Array(array) => *Tag::from(*array.items.clone()).data_type,
            ast::Type::Map(map) => {
                let key = Tag::from(*map.key.clone());
                let value = Tag::from(*map.value.clone());
                let fields: HashMap<String, Box<Tag>> = vec![("key", key), ("value", value)]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), Box::new(v)))
                    .collect();
                Type::Record(Record { fields })
            }
            _ => {
                warn!("{} - Unsupported conversion", tag.fully_qualified_name());
                Type::Atom(Atom::String)
            }
        };

        let mode = if tag.is_array() || tag.is_map() {
            Mode::Repeated
        } else if tag.is_null() || tag.nullable {
            Mode::Nullable
        } else {
            Mode::Required
        };
        Tag {
            name: tag.name.clone(),
            data_type: Box::new(data_type),
            mode,
        }
    }
}

// See: https://serde.rs/custom-date-format.html#date-in-a-custom-format
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
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::{self, json};

    #[test]
    fn test_serialize_atom() {
        let atom = Tag {
            name: None,
            data_type: Box::new(Type::Atom(Atom::Bool)),
            mode: Mode::Nullable,
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
        };

        let mut record = Record {
            fields: HashMap::new(),
        };
        record.fields.insert("test-int".into(), Box::new(atom));

        let root = Tag {
            name: None,
            data_type: Box::new(Type::Record(record)),
            mode: Mode::Nullable,
        };

        let expect = json!({
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [{
                "name": "test-int",
                "type": "INT64",
                "mode": "NULLABLE"
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
                "mode": "NULLABLE"
            }]
        }))
        .unwrap();

        let test_int = match &*record.data_type {
            Type::Record(record) => record.fields.get("test-int").unwrap(),
            _ => panic!(),
        };
        match *test_int.data_type {
            Type::Atom(Atom::Int64) => (),
            _ => panic!(),
        };
    }

    #[test]
    fn test_serialize_nested_record() {
        let atom = Tag {
            name: Some("test-int".into()),
            data_type: Box::new(Type::Atom(Atom::Int64)),
            mode: Mode::Nullable,
        };

        let mut record_b = Record {
            fields: HashMap::new(),
        };
        record_b.fields.insert("test-int".into(), Box::new(atom));

        let tag_b = Tag {
            name: Some("test-record-b".into()),
            data_type: Box::new(Type::Record(record_b)),
            mode: Mode::Nullable,
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
        };

        let expect = json!({
            "name": "test-record-a",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [{
                "name": "test-record-b",
                "type": "RECORD",
                "fields": [{
                    "name": "test-int",
                    "type": "INT64",
                    "mode": "NULLABLE"
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
            "fields": [{
                "name": "test-record-b",
                "type": "RECORD",
                "fields": [{
                    "name": "test-int",
                    "type": "INT64",
                    "mode": "NULLABLE"
                }],
                "mode": "NULLABLE"
            }]
        });
        let record_a: Tag = serde_json::from_value(data).unwrap();
        let record_b = match &*record_a.data_type {
            Type::Record(record) => record.fields.get("test-record-b").unwrap(),
            _ => panic!(),
        };
        let test_int = match &*record_b.data_type {
            Type::Record(record) => record.fields.get("test-int").unwrap(),
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
        let jschema: ast::Tag = serde_json::from_value(data).unwrap();
        let bq: Tag = jschema.into();
        let expect = json!({
            "type": "STRING",
            "mode": "NULLABLE",
        });
        assert_eq!(expect, json!(bq));
    }

    #[test]
    fn test_from_ast_atom() {
        let data = json!({
            "type": {"atom": "integer"}
        });
        let jschema: ast::Tag = serde_json::from_value(data).unwrap();
        let bq: Tag = jschema.into();
        let expect = json!({
            "type": "INT64",
            "mode": "REQUIRED",
        });
        assert_eq!(expect, json!(bq));
    }

    #[test]
    fn test_from_ast_nullable_atom() {
        let data = json!({
            "type": {"atom": "integer"},
            "nullable": true,
        });
        let jschema: ast::Tag = serde_json::from_value(data).unwrap();
        let bq: Tag = jschema.into();
        let expect = json!({
            "type": "INT64",
            "mode": "NULLABLE",
        });
        assert_eq!(expect, json!(bq));
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
        let jschema: ast::Tag = serde_json::from_value(data).unwrap();
        let bq: Tag = jschema.into();
        let expect = json!({
            "type": "INT64",
            "mode": "NULLABLE",
        });
        assert_eq!(expect, json!(bq));
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
        let jschema: ast::Tag = serde_json::from_value(data).unwrap();
        let bq: Tag = jschema.into();
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
        assert_eq!(expect, json!(bq));
    }

    #[test]
    fn test_from_ast_array() {
        let data = json!({
        "type": {
            "array": {
                "items": {
                    "type": {"atom": "integer"}},
        }}});
        let jschema: ast::Tag = serde_json::from_value(data).unwrap();
        let bq: Tag = jschema.into();
        let expect = json!({
            "type": "INT64",
            "mode": "REPEATED",
        });
        assert_eq!(expect, json!(bq));
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
        let tag: ast::Tag = serde_json::from_value(data).unwrap();
        let bq: Tag = tag.into();
        let expect = json!({
            "type": "STRING",
            "mode": "REQUIRED",
        });
        assert_eq!(expect, json!(bq));
    }

    #[test]
    fn test_from_ast_map() {
        let data = json!({
        "type": {
            "map": {
                "key": {"type": {"atom": "string"}},
                "value": {"type": {"atom": "integer"}}
        }}});
        let jschema: ast::Tag = serde_json::from_value(data).unwrap();
        let bq: Tag = jschema.into();
        let expect = json!({
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "key", "type": "STRING", "mode": "REQUIRED"},
            {"name": "value", "type": "INT64", "mode": "REQUIRED"},
        ]});
        assert_eq!(expect, json!(bq));
    }
}
