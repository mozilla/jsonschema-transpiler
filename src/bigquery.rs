use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
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
#[serde(rename_all = "UPPERCASE")]
pub enum Type {
    Atom(Atom),
    // Array(Tag)
    // Struct
    Record(Record),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Record {
    #[serde(with = "fields_as_vec")]
    fields: HashMap<String, Box<Tag>>,
}

// See: https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types
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

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
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

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
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
fn test_serialize_record() {
    let record = Tag {
        name: None,
        data_type: Box::new(Type::Atom(Atom::Bool)),
        mode: Mode::Nullable,
    };
    let expect = json!({
        "type": "BOOL",
        "mode": "NULLABLE",
    });
    assert_eq!(expect, json!(record))
}
