#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
enum Mode {
    Nullable,
    Required,
    Repeated,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
enum Type {
    Null,
    Boolean,
    Integer,
    Float,
    Record(Record),
}

#[derive(Serialize, Deserialize, Debug)]
struct Record {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(rename = "type")]
    data_type: Box<Type>,
    fields: Vec<Box<Type>>,
    mode: Mode,
}
