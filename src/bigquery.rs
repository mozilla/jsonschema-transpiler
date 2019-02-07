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

// See: https://serde.rs/custom-date-format.html#date-in-a-custom-format
mod fields_as_vec {
    use super::{Record, Tag};
    use serde::ser::{self, SerializeSeq, Serializer};
    use serde::{Deserialize, Deserializer};
    use std::collections::HashMap;
    use std::iter::FromIterator;

    pub fn serialize<S>(map: &HashMap<String, Box<Tag>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(map.len()))?;
        for (_, element) in map {
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

use serde_json::json;

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
