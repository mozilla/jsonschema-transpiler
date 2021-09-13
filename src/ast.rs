use super::casing::to_snake_case;
use super::jsonschema;
use super::Context;
use super::TranslateFrom;
use regex::Regex;
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Atom {
    Boolean,
    Integer,
    Number,
    String,
    Datetime,
    JSON,
    Bytes,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Object {
    pub fields: HashMap<String, Box<Tag>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<HashSet<String>>,
}

impl Object {
    pub fn new(fields: HashMap<String, Tag>, required: Option<HashSet<String>>) -> Self {
        let fields: HashMap<String, Box<Tag>> =
            fields.into_iter().map(|(k, v)| (k, Box::new(v))).collect();
        Object { fields, required }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Array {
    pub items: Box<Tag>,
}

impl Array {
    pub fn new(items: Tag) -> Self {
        Array {
            items: Box::new(items),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tuple {
    pub items: Vec<Tag>,
}

impl Tuple {
    pub fn new(items: Vec<Tag>) -> Self {
        Tuple { items }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Map {
    pub key: Box<Tag>,
    pub value: Box<Tag>,
}

impl Map {
    pub fn new(key: Option<String>, value: Tag) -> Self {
        Map {
            key: Box::new(Tag {
                name: key,
                namespace: None,
                data_type: Type::Atom(Atom::String),
                nullable: false,
                is_root: false,
                description: None,
                title: None,
            }),
            value: Box::new(value),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Union {
    items: Vec<Tag>,
}

impl Union {
    pub fn new(items: Vec<Tag>) -> Self {
        Union { items }
    }

    /// Collapse a union of types into a structurally compatible type.
    ///
    /// Typically, variant types are not allowed in a table schema. If a variant type
    /// type is found, it will be converted into a JSON type. Because of the ambiguity
    /// around finding structure in a JSON blob, the union of any type with JSON will
    /// be consumed by the JSON type. In a similar fashion, a table schema is determined
    /// to be nullable or required via occurrences of null types in unions.
    pub fn collapse(&self) -> Tag {
        let is_null = self.items.iter().any(Tag::is_null);

        if self.items.is_empty() {
            panic!("empty union is not allowed")
        } else if self.items.len() == 1 {
            return Tag {
                name: None,
                namespace: None,
                nullable: is_null,
                is_root: false,
                data_type: self.items[0].data_type.clone(),
                description: None,
                title: None,
            };
        }

        let items: Vec<Tag> = self
            .items
            .iter()
            .filter(|x| !x.is_null())
            .map(|x| {
                if let Type::Union(union) = &x.data_type {
                    let mut tag = union.collapse();
                    tag.name = x.name.clone();
                    tag
                } else {
                    x.clone()
                }
            })
            .collect();

        // after collapsing nulls in the base case and collapsing nested unions in
        // the pre-processing step, check for nullability based on the immediate level of tags
        let nullable = is_null || items.iter().any(|tag| tag.nullable);

        let data_type: Type = if items.iter().all(Tag::is_atom) {
            items
                .into_iter()
                .fold(Type::Null, |acc, x| match (acc, &x.data_type) {
                    (Type::Null, Type::Atom(atom)) => Type::Atom(*atom),
                    (Type::Atom(left), Type::Atom(right)) => {
                        let atom = match (left, right) {
                            (Atom::Boolean, Atom::Boolean) => Atom::Boolean,
                            (Atom::Integer, Atom::Integer) => Atom::Integer,
                            (Atom::Number, Atom::Number)
                            | (Atom::Integer, Atom::Number)
                            | (Atom::Number, Atom::Integer) => Atom::Number,
                            (Atom::String, Atom::String) => Atom::String,
                            (lhs, rhs) => {
                                trace!("Invalid union collapse of atoms {:?} and {:?}", lhs, rhs);
                                Atom::JSON
                            }
                        };
                        Type::Atom(atom)
                    }
                    _ => {
                        trace!("Invalid union collapse of atoms found during fold");
                        Type::Atom(Atom::JSON)
                    }
                })
        } else if items.iter().all(Tag::is_object) {
            items
                .into_iter()
                .fold(Type::Null, |acc, x| match (&acc, &x.data_type) {
                    (Type::Null, Type::Object(object)) => Type::Object(object.clone()),
                    (Type::Object(left), Type::Object(right)) => {
                        // union each sub-property, recursively collapse, and rebuild
                        let mut union: HashMap<String, Vec<Tag>> = HashMap::new();
                        for (key, value) in &left.fields {
                            union.insert(key.to_string(), vec![*value.clone()]);
                        }
                        for (key, value) in &right.fields {
                            if let Some(vec) = union.get_mut(key) {
                                vec.push(*value.clone())
                            } else {
                                union.insert(key.to_string(), vec![*value.clone()]);
                            }
                        }
                        let result: HashMap<String, Tag> = union
                            .into_iter()
                            .map(|(k, v)| (k, Union::new(v).collapse()))
                            .collect();
                        // Recursively invalidate the tree if any of the subschemas are incompatible.
                        // Atom::JSON is a catch-all value and marks inconsistent objects.
                        let is_consistent = !result.iter().any(|(_, v)| match v.data_type {
                            Type::Atom(Atom::JSON) => true,
                            _ => false,
                        });
                        if is_consistent {
                            let required: Option<HashSet<String>> =
                                match (&left.required, &right.required) {
                                    (Some(x), Some(y)) => {
                                        Some(x.intersection(&y).map(ToString::to_string).collect())
                                    }
                                    (Some(x), None) | (None, Some(x)) => Some(x.clone()),
                                    _ => None,
                                };
                            Type::Object(Object::new(result, required))
                        } else {
                            trace!("Incompatible subschemas found during union collapse");
                            Type::Atom(Atom::JSON)
                        }
                    }
                    _ => {
                        trace!("Inconsistent union collapse of object");
                        Type::Atom(Atom::JSON)
                    }
                })
        } else if items.iter().all(Tag::is_map) {
            let tags: Vec<Tag> = items
                .into_iter()
                .map(|x| match &x.data_type {
                    Type::Map(map) => *map.value.clone(),
                    _ => panic!("Invalid map found during union collapse"),
                })
                .collect();
            Type::Map(Map::new(None, Union::new(tags).collapse()))
        } else if items.iter().all(Tag::is_array) {
            let tags: Vec<Tag> = items
                .into_iter()
                .map(|x| match &x.data_type {
                    Type::Array(array) => *array.items.clone(),
                    _ => panic!("Invalid array found during union collapse"),
                })
                .collect();
            Type::Array(Array::new(Union::new(tags).collapse()))
        } else {
            trace!("Incompatible union collapse found");
            Type::Atom(Atom::JSON)
        };

        let mut tag = Tag {
            name: None,
            namespace: None,
            nullable,
            data_type,
            is_root: false,
            description: None,
            title: None,
        };
        tag.infer_nullability(false);
        tag
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    Null,
    Atom(Atom),
    Object(Object),
    Map(Map),
    Array(Array),
    Tuple(Tuple),
    Union(Union),
    // Intersection
    // Not
}

impl Default for Type {
    fn default() -> Self {
        Type::Null
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(tag = "type")]
pub struct Tag {
    #[serde(rename = "type")]
    pub data_type: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    // The namespace of the current object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    #[serde(default)]
    pub nullable: bool,

    #[serde(default, skip_serializing)]
    pub is_root: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

impl Tag {
    pub fn new(
        data_type: Type,
        name: Option<String>,
        nullable: bool,
        description: Option<String>,
        title: Option<String>,
    ) -> Self {
        Tag {
            data_type,
            name,
            namespace: None,
            nullable,
            is_root: false,
            description,
            title,
        }
    }

    pub fn is_null(&self) -> bool {
        match self.data_type {
            Type::Null => true,
            _ => false,
        }
    }

    pub fn is_atom(&self) -> bool {
        match self.data_type {
            Type::Atom(_) => true,
            _ => false,
        }
    }

    pub fn is_object(&self) -> bool {
        match self.data_type {
            Type::Object(_) => true,
            _ => false,
        }
    }

    pub fn is_map(&self) -> bool {
        match self.data_type {
            Type::Map(_) => true,
            _ => false,
        }
    }

    pub fn is_array(&self) -> bool {
        match self.data_type {
            Type::Array(_) => true,
            _ => false,
        }
    }

    /// Get the path to the current tag in the context of the larger schema.
    ///
    /// Each tag in the schema can be unambiguously referenced by concatenating
    /// the name of tag with the tag's namespace. For example, a document may
    /// contain a `timestamp` field nested under different sub-documents.
    ///
    /// ```json
    /// {
    ///     "environment": { "timestamp": 64 },
    ///     "payload": {
    ///         "measurement": 10,
    ///         "timestamp": 64
    ///     }
    /// }
    /// ```
    ///
    /// The fully qualified names are as follows:
    ///
    /// * `root.environment.timestamp`
    /// * `root.payload.measurement`
    /// * `root.payload.timestamp`
    pub fn fully_qualified_name(&self) -> String {
        let name = match &self.name {
            Some(name) => name.clone(),
            None => "__unknown__".to_string(),
        };
        match &self.namespace {
            Some(ns) => format!("{}.{}", ns, name),
            None => name,
        }
    }

    /// If a name starts with a number, prefix it with an underscore.
    fn normalize_numeric_prefix(name: String) -> String {
        if name.chars().next().unwrap().is_numeric() {
            format!("_{}", name)
        } else {
            name
        }
    }

    /// Renames a column name so it contains only letters, numbers, and
    /// underscores while starting with a letter or underscore. This requirement
    /// is enforced by BigQuery during table creation.
    fn normalize_name_bigquery(string: &str) -> Option<String> {
        let re = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        let renamed = Tag::normalize_numeric_prefix(string.replace(".", "_").replace("-", "_"));
        if re.is_match(&renamed) {
            Some(renamed)
        } else {
            None
        }
    }

    /// Fix properties of an object to adhere the BigQuery column name
    /// specification.
    ///
    /// This removes invalid field names from the schema when inferring the
    /// names for the schema (e.g. `$schema`). It also applies rules to be
    /// consistent with BigQuery's naming scheme, like avoiding columns that
    /// start with a number.
    ///
    /// This modifies field names as well as required fields.
    /// See: https://cloud.google.com/bigquery/docs/schemas
    pub fn normalize_properties(&mut self, normalize_case: bool) {
        if let Type::Object(ref mut object) = self.data_type {
            let fields = &mut object.fields;
            let keys: Vec<String> = fields.keys().cloned().collect();

            for key in keys {
                // Replace property names with the normalized property name
                if let Some(mut renamed) = Tag::normalize_name_bigquery(&key) {
                    renamed = if normalize_case {
                        // snake_casing strips symbols outside of word
                        // boundaries e.g. _64bit -> 64bit
                        Tag::normalize_numeric_prefix(to_snake_case(&renamed))
                    } else {
                        renamed
                    };
                    if renamed.as_str() != key.as_str() {
                        warn!("{} replaced with {}", key, renamed);
                        fields.insert(renamed, fields[&key].clone());
                        fields.remove(&key);
                    }
                } else {
                    warn!("Omitting {} - not a valid property name", key);
                    fields.remove(&key);
                }
            }

            // Replace the corresponding names in the required field
            object.required = match &object.required {
                Some(required) => {
                    let renamed: HashSet<String> = required
                        .iter()
                        .map(String::as_str)
                        .map(Tag::normalize_name_bigquery)
                        .filter(Option::is_some)
                        .map(Option::unwrap)
                        .collect();
                    if normalize_case {
                        Some(
                            renamed
                                .iter()
                                .map(|s| Tag::normalize_numeric_prefix(to_snake_case(&s)))
                                .collect(),
                        )
                    } else {
                        Some(renamed)
                    }
                }
                None => None,
            };
        }
    }

    /// Sets a tag with references to the name and the namespace.
    fn set_name(&mut self, name: &str, namespace: &str) {
        self.name = Some(name.to_string());
        if !namespace.is_empty() {
            self.namespace = Some(namespace.to_string());
        }
    }

    /// A helper function for calculating the names and namespaces within the
    /// schema.
    ///
    /// The namespaces are built from the top-down and follows the depth-first
    /// traversal of the schema.
    fn recurse_infer_name(&mut self, namespace: String, normalize_case: bool) {
        self.normalize_properties(normalize_case);

        let set_and_recurse = |tag: &mut Tag, name: &str| {
            tag.set_name(name, &namespace);
            tag.recurse_infer_name(format!("{}.{}", &namespace, name), normalize_case)
        };

        match &mut self.data_type {
            Type::Object(object) => {
                for (key, value) in object.fields.iter_mut() {
                    set_and_recurse(value, key)
                }
            }
            Type::Map(map) => {
                set_and_recurse(&mut map.key, "key");
                set_and_recurse(&mut map.value, "value");
            }
            Type::Array(array) => {
                set_and_recurse(&mut array.items, "list");
            }
            Type::Union(union) => {
                for item in union.items.iter_mut() {
                    set_and_recurse(item, "__union__");
                }
            }
            Type::Tuple(tuple) => {
                for (i, item) in tuple.items.iter_mut().enumerate() {
                    let name = format!("f{}_", i);
                    set_and_recurse(item, &name);
                }
            }
            _ => (),
        }
    }

    /// Assign names and namespaces to tags from parent tags.
    pub fn infer_name(&mut self, normalize_case: bool) {
        let namespace = match &self.name {
            Some(name) => name.clone(),
            None => "".into(),
        };
        self.recurse_infer_name(namespace, normalize_case);
    }

    /// Infer whether the current tag in the schema allows for the value to be
    /// null.
    ///
    /// These rules are primarily focused on BigQuery, although they should
    /// translate into other schemas. This should be run after unions have been
    /// eliminated from the tree since the behavior is currently order
    /// dependent.
    pub fn infer_nullability(&mut self, force_nullable: bool) {
        match &mut self.data_type {
            Type::Null => {
                self.nullable = true;
            }
            Type::Object(object) => {
                let required = match &object.required {
                    Some(required) => required.clone(),
                    None => HashSet::new(),
                };
                for (key, value) in &mut object.fields {
                    // Infer whether the value is nullable
                    value.infer_nullability(force_nullable);
                    // A required nullable field is still nullable
                    value.nullable |= !required.contains(key);
                    // All fields are nullable if enforced
                    if force_nullable {
                        value.nullable = true;
                    }
                }
            }
            Type::Map(map) => map.value.infer_nullability(force_nullable),
            Type::Array(array) => array.items.infer_nullability(force_nullable),
            Type::Tuple(tuple) => {
                for item in tuple.items.iter_mut() {
                    item.infer_nullability(force_nullable);
                }
            }
            Type::Union(union) => {
                for item in union.items.iter_mut() {
                    item.infer_nullability(force_nullable);
                }
            }
            _ => (),
        }
        if force_nullable {
            self.nullable = true
        }
    }

    /// Factor out the shared parts of the union between two schemas.
    pub fn collapse(&mut self) {
        match &mut self.data_type {
            Type::Object(object) => {
                for value in &mut object.fields.values_mut() {
                    value.collapse()
                }
            }
            Type::Map(map) => map.value.collapse(),
            Type::Array(array) => array.items.collapse(),
            Type::Tuple(tuple) => {
                for item in tuple.items.iter_mut() {
                    item.collapse()
                }
            }
            Type::Union(union) => {
                let tag = union.collapse();
                self.data_type = tag.data_type;
                self.nullable = tag.nullable;
            }
            _ => (),
        }
    }
}

impl TranslateFrom<jsonschema::Tag> for Tag {
    type Error = &'static str;

    fn translate_from(tag: jsonschema::Tag, context: Context) -> Result<Self, Self::Error> {
        let mut tag = tag.type_into_ast(context)?;
        tag.infer_name(context.normalize_case);
        tag.infer_nullability(context.force_nullable);
        tag.is_root = true;
        Ok(tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::{json, Value};

    #[test]
    fn test_serialize_null() {
        let null_tag = Tag {
            ..Default::default()
        };
        let expect = json!({
            "type": "null",
            "nullable": false,
        });
        assert_eq!(expect, json!(null_tag))
    }

    #[test]
    fn test_serialize_atom() {
        let atom = Tag {
            data_type: Type::Atom(Atom::Integer),
            name: Some("test-int".into()),
            nullable: true,
            ..Default::default()
        };
        let expect = json!({
            "type": {"atom": "integer"},
            "name": "test-int",
            "nullable": true,

        });
        assert_eq!(expect, json!(atom));
    }

    #[test]
    fn test_serialize_object() {
        let mut field = Tag {
            data_type: Type::Object(Object::new(HashMap::new(), None)),
            name: Some("test-object".into()),
            ..Default::default()
        };
        if let Type::Object(object) = &mut field.data_type {
            object.fields.insert(
                "test-int".into(),
                Box::new(Tag {
                    data_type: Type::Atom(Atom::Integer),
                    name: Some("test-int".into()),
                    ..Default::default()
                }),
            );
            object.fields.insert(
                "test-bool".into(),
                Box::new(Tag {
                    data_type: Type::Atom(Atom::Boolean),
                    name: Some("test-bool".into()),
                    ..Default::default()
                }),
            );
        }
        let expect = json!({
            "name": "test-object",
            "nullable": false,
            "type": {
                "object": {
                    "fields": {
                        "test-int": {
                            "name": "test-int",
                            "type": {"atom": "integer"},
                            "nullable": false
                        },
                        "test-bool": {
                            "name": "test-bool",
                            "type": {"atom": "boolean"},
                            "nullable": false
                        }
                    }
                }
            }
        });
        assert_eq!(expect, json!(field))
    }

    #[test]
    fn test_serialize_map() {
        let atom = Tag {
            data_type: Type::Atom(Atom::Integer),
            name: Some("test-value".into()),
            nullable: false,
            ..Default::default()
        };
        let field = Tag {
            data_type: Type::Map(Map::new(Some("test-key".into()), atom)),
            name: Some("test-map".into()),
            nullable: true,
            ..Default::default()
        };
        let expect = json!({
            "name": "test-map",
            "nullable": true,
            "type": {
                "map": {
                    "key": {
                        "name": "test-key",
                        "nullable": false,
                        "type": {"atom": "string"},
                    },
                    "value": {
                        "name": "test-value",
                        "nullable": false,
                        "type": {"atom": "integer"},
                    }
                }
            }
        });
        assert_eq!(expect, json!(field));
    }

    #[test]
    fn test_serialize_array() {
        // represent multi-set with nulls
        let atom = Tag {
            data_type: Type::Atom(Atom::Integer),
            name: Some("test-int".into()),
            nullable: true,
            ..Default::default()
        };
        let field = Tag {
            data_type: Type::Array(Array::new(atom)),
            name: Some("test-array".into()),
            nullable: false,
            ..Default::default()
        };
        let expect = json!({
            "type": {
                "array": {
                    "items": {
                        "name": "test-int",
                        "type": {"atom": "integer"},
                        "nullable": true,
                    }
                }
            },
            "name": "test-array",
            "nullable": false
        });
        assert_eq!(expect, json!(field))
    }

    #[test]
    fn test_serialize_union() {
        let test_int = Tag {
            data_type: Type::Atom(Atom::Integer),
            ..Default::default()
        };
        let test_null = Tag {
            ..Default::default()
        };
        let union = Tag {
            data_type: Type::Union(Union {
                items: vec![test_int, test_null],
            }),
            ..Default::default()
        };
        let expect = json!({
            "type": {
                "union": {
                    "items": [
                        {"type": {"atom": "integer"}, "nullable": false},
                        {"type": "null", "nullable": false},
                    ]
                }
            },
            "nullable": false
        });
        assert_eq!(expect, json!(union))
    }

    #[test]
    fn test_union_collapse_atom() {
        let data = json!({
        "union": {
            "items": [
                {"type": {"atom": "integer"}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "atom": "integer",
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_atom_repeats() {
        let data = json!({
        "union": {
            "items": [
                {"type": {"atom": "integer"}},
                {"type": {"atom": "integer"}},
                {"type": {"atom": "integer"}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "atom": "integer",
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_nullable_atom() {
        let data = json!({
        "union": {
            "items": [
                {"type": "null"},
                {"type": {"atom": "integer"}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "atom": "integer",
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_type_conflict() {
        let data = json!({
        "union": {
            "items": [
                {"type": {"atom": "string"}},
                {"type": {"atom": "integer"}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "atom": "json",
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_object_merge() {
        let data = json!({
        "union": {
            "items": [
                {
                    "type": {
                        "object": {
                            "fields": {
                                "atom_0": {"type": {"atom": "boolean"}},
                                "atom_1": {"type": {"atom": "integer"}},
                            }}}},
                {
                    "type": {
                        "object": {
                            "fields": {
                                "atom_1": {"type": {"atom": "integer"}},
                                "atom_2": {"type": {"atom": "string"}},
                            }}}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
        "object": {
            "fields": {
                "atom_0": {"type": {"atom": "boolean"}, "nullable": true},
                "atom_1": {"type": {"atom": "integer"}, "nullable": true},
                "atom_2": {"type": {"atom": "string"}, "nullable": true},
            }}});
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_object_conflict() {
        let data = json!({
        "union": {
            "items": [
                {"type": {"atom": "string"}},
                {"type": {"atom": "integer"}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "atom": "json",
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_array_nullable_atom() {
        let data = json!({
        "union": {
            "items": [
                {"type": {"array": {"items": {"type": {"atom": "integer"}}}}},
                {"type": {"array": {"items": {"type": "null"}}}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "array": {"items": {"type": {"atom": "integer"}, "nullable": true}}
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_map_nullable_atom() {
        let dtype = Type::Union(Union::new(vec![
            Tag::new(
                Type::Map(Map::new(
                    None,
                    Tag::new(Type::Atom(Atom::Integer), None, false, None, None),
                )),
                None,
                false,
                None,
                None,
            ),
            Tag::new(
                Type::Map(Map::new(
                    None,
                    Tag::new(Type::Null, None, false, None, None),
                )),
                None,
                false,
                None,
                None,
            ),
        ]));
        let expect = json!({
        "map": {
            "key": {
                "type": {"atom": "string"},
                "nullable": false,
            },
            "value": {
                "type": {"atom": "integer"},
                "nullable": true,
            }}});
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse().data_type))
        } else {
            panic!()
        }
    }

    #[test]
    fn test_union_collapse_nested_union() {
        let data = json!({
        "union": {
            "items": [
                {"type": {"union": {"items": [
                    {"type": "null"},
                    {"type": {"atom": "number"}},
                ]}}},
                {"type": {"atom": "integer"}},
            ]}});
        let dtype: Type = serde_json::from_value(data).unwrap();
        let expect = json!({
            "nullable": true,
            "type": {"atom": "number"}
        });
        if let Type::Union(union) = dtype {
            assert_eq!(expect, json!(union.collapse()))
        } else {
            panic!()
        }
    }

    fn assert_infer_name(expect: Value, actual: Value) {
        let mut tag: Tag = serde_json::from_value(actual).unwrap();
        tag.infer_name(false);
        assert_eq!(expect, json!(tag))
    }

    #[test]
    fn test_tag_infer_name_object() {
        let data = json!({
        "type": {
            "object": {
                "fields": {
                    "atom_0": {"type": {"atom": "integer"}},
                    "atom_1": {"type": {"atom": "integer"}},
                    "atom_2": {"type": {"atom": "integer"}},
                }}}});
        let expect = json!({
        "nullable": false,
        "type": {
            "object": {
                "fields": {
                    "atom_0": {"name": "atom_0", "type": {"atom": "integer"}, "nullable": false},
                    "atom_1": {"name": "atom_1", "type": {"atom": "integer"}, "nullable": false},
                    "atom_2": {"name": "atom_2", "type": {"atom": "integer"}, "nullable": false},
                }}}});
        assert_infer_name(expect, data);
    }

    #[test]
    fn test_tag_infer_name_array_object() {
        let data = json!({
        "name": "foo",
        "type": {
            "array": {
                "items": {
                    "type": {
                        "object": {
                            "fields": {
                                "bar": {"type": {"atom": "integer"}}
                            }}}}}}});
        let expect = json!({
        "nullable": false,
        "name": "foo",
        "type": {
            "array": {
                "items": {
                    "nullable": false,
                    // array items are always named item, for the sanity of avro
                    "name": "list",
                    "namespace": "foo",
                    "type": {
                        "object": {
                            "fields": {
                                "bar": {
                                    "nullable": false,
                                    "name": "bar",
                                    "namespace": "foo.list",
                                    "type": {"atom": "integer"}}
                            }}}}}}});
        assert_infer_name(expect, data);
    }

    #[test]
    fn test_tag_infer_name_map_object() {
        let data = json!({
        "name": "foo",
        "type": {
            "map": {
                "key": {"type": {"atom": "string"}},
                "value": {
                    "type": {
                        "object": {
                            "fields": {
                                "bar": {"type": {"atom": "integer"}}
                            }}}}}}});
        let expect = json!({
        "nullable": false,
        "name": "foo",
        "type": {
            "map": {
                "key": {
                    "nullable": false,
                    "name": "key",
                    // avro doesn't allow primitives to have a namespace, but is
                    // consistent behavior within ast
                    "namespace": "foo",
                    "type": {"atom": "string"}
                },
                "value": {
                    "nullable": false,
                    // array items are always named item, for the sanity of avro
                    "name": "value",
                    "namespace": "foo",
                    "type": {
                        "object": {
                            "fields": {
                                "bar": {
                                    "nullable": false,
                                    "name": "bar",
                                    "namespace": "foo.value",
                                    "type": {"atom": "integer"}}
                            }}}}}}});
        assert_infer_name(expect, data);
    }

    fn fixture_union_object() -> Value {
        json!({
        "name": "foo",
        "type": {
            "union": {
                "items": [
                    {
                        "type": {
                            "object": {
                                "fields": {
                                    "bar": {"type": {"atom": "integer"}}
                                }}}},
                    {
                        "type": {
                            "object": {
                                "fields": {
                                    "baz": {"type": {"atom": "boolean"}}
                                }}}},
                ]}}})
    }

    #[test]
    fn test_tag_infer_name_union_object() {
        let expect = json!({
        "nullable": false,
        "name": "foo",
        "type": {
            "union": {
                "items": [
                    // Conflicting names should go away when collapsed. Variant
                    // types are generally not SQL friendly.
                    {
                        "nullable": false,
                        "name": "__union__",
                        "namespace": "foo",
                        "type": {
                            "object": {
                                "fields": {
                                    "bar": {
                                        "nullable": false,
                                        "name": "bar",
                                        "namespace": "foo.__union__",
                                        "type": {"atom": "integer"}}
                                }}}},
                    {
                        "nullable": false,
                        "name": "__union__",
                        "namespace": "foo",
                        "type": {
                            "object": {
                                "fields": {
                                    "baz": {
                                        "nullable": false,
                                        "name": "baz",
                                        "namespace": "foo.__union__",
                                        "type": {"atom": "boolean"}}
                                }}}},
                ]}}});
        assert_infer_name(expect, fixture_union_object());

        let collapse_expect = json!({
        "nullable": false,
        "name": "foo",
        "type": {
            "object": {
                "fields": {
                    "bar": {
                        // nullability is inferred when collapsed
                        "nullable": true,
                        "name": "bar",
                        "namespace": "foo",
                        "type": {"atom": "integer"}},
                    "baz": {
                        "nullable": true,
                        "name": "baz",
                        "namespace": "foo",
                        "type": {"atom": "boolean"}},
                }}}});
        // collapse and infer name
        let mut tag_collapse: Tag = serde_json::from_value(fixture_union_object()).unwrap();
        tag_collapse.collapse();
        tag_collapse.infer_name(false);
        assert_eq!(collapse_expect, json!(tag_collapse));

        // infer and then collapse
        // NOTE: The behavior is not the same, the name and namespace need to be inferred again
        tag_collapse = serde_json::from_value(fixture_union_object()).unwrap();
        tag_collapse.infer_name(false);
        tag_collapse.collapse();

        assert_ne!(collapse_expect, json!(tag_collapse));
        tag_collapse.infer_name(false);
        assert_eq!(collapse_expect, json!(tag_collapse));
    }

    #[test]
    fn test_tag_infer_name_nested_object() {
        let data = json!({
        "type": {
            "object": {
                "fields": {
                    "foo": {
                        "type": {
                            "object": {
                                "fields": {
                                    "bar": {
                                        "type": "null"
                                        }}}}}}}}});
        let expect = json!({
        "nullable": false,
        "type": {
            "object": {
                "fields": {
                    "foo": {
                        "name": "foo",
                        "nullable": false,
                        "type": {
                            "object": {
                                "fields": {
                                    "bar": {
                                        "name": "bar",
                                        // empty toplevel
                                        "namespace": ".foo",
                                        "type": "null",
                                        "nullable": false,
                                    }}}}}}}}});
        assert_infer_name(expect, data);
    }

    #[test]
    fn test_tag_normalize_properties() {
        fn assert_normalize(tag: &Tag, renamed: Vec<&str>) {
            if let Type::Object(object) = &tag.data_type {
                let expected: HashSet<String> = renamed.iter().map(|x| x.to_string()).collect();
                let actual: HashSet<String> = object.fields.keys().cloned().collect();
                assert_eq!(expected, actual);
                assert_eq!(expected, object.required.clone().unwrap());
            } else {
                panic!()
            }
        }

        let data = json!({
        "type": {
            "object": {
                "fields": {
                    "valid_name": {"type": "null"},
                    "renamed-value.0": {"type": "null"},
                    "$schema": {"type": "null"},
                    "64bit": {"type": "null"},
                },
                "required": [
                    "valid_name",
                    "renamed-value.0",
                    "$schema",
                    "64bit",
                ]}}});

        let mut tag: Tag = serde_json::from_value(data).unwrap();
        tag.normalize_properties(false);
        assert_normalize(&tag, vec!["valid_name", "renamed_value_0", "_64bit"]);

        // Test that numbers are properly prefixed with underscores after
        // normalizing the case.
        tag.normalize_properties(true);
        assert_normalize(&tag, vec!["valid_name", "renamed_value_0", "_64bit"]);
    }

    #[test]
    fn test_infer_nullability_force_nullable() {
        let data = json!({
        "nullable": false,
        "type": {
            "object": {
                "fields": {
                    "atom": {"type": {"atom": "integer"}, "nullable": false},
                    "object": {
                        "type": {"object": {"fields": {
                            "nested_atom": {"type": {"atom": "integer"}, "nullable": false}}}},
                        "nullable": false}}}}});

        let expect = json!({
        "nullable": true,
        "type": {
            "object": {
                "fields": {
                    "atom": {"type": {"atom": "integer"}, "nullable": true},
                    "object": {
                        "type": {"object": {"fields": {
                            "nested_atom": {"type": {"atom": "integer"}, "nullable": true}}}},
                        "nullable": true}}}}});
        let mut tag: Tag = serde_json::from_value(data).unwrap();
        tag.infer_nullability(true);
        assert_eq!(expect, json!(tag))
    }
}
