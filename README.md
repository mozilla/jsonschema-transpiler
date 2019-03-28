[![CircleCI](https://circleci.com/gh/acmiyaguchi/jsonschema-transpiler.svg?style=svg)](https://circleci.com/gh/acmiyaguchi/jsonschema-transpiler)
# jsonschema-transpiler

A tool for transpiling [JSON Schema](https://json-schema.org/) into schemas for
[Avro](https://avro.apache.org/docs/current/index.html#schemas) and
[BigQuery](https://cloud.google.com/bigquery/docs/schemas).

JSON Schema is primarily used to validate incoming data, but contains enough
information to describe the structure of the data. The transpiler encodes the
schema for use with data serialization and processing frameworks. The main
use-case is to enable ingestion of JSON documents into BigQuery through an Avro
intermediary.


## Installation

```
cargo install --git https://github.com/acmiyaguchi/jsonschema-transpiler
```

## Usage

```
jst 0.1
Anthony Miyaguchi <amiyaguchi@mozilla.com>

USAGE:
    jsonschema_transpiler --from-file <FILE> --type <type>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -f, --from-file <FILE>
        --type <type>          [possible values: avro, bigquery]
```

## Contributing

Contributions are welcome. The API may change significantly, but the
transformation between various source formats should remain consistent. To aid
in the development of the transpiler, tests cases are generated from a language
agnostic format under `tests/resources`.

```json
{
    "name": "test-suite",
    "tests": [
        {
            "name": "test-case",
            "description": [
                "A short description of the test case."
            ],
            "tests": {
                "avro": {...},
                "bigquery": {...},
                "json": {...}
            }
        },
        ...
    ]
}
```

Schemas provide a type system for data-structures. Most schema languages support
a similar set of primitives. There are atomic data types like booleans,
integers, and floats. These atomic data types can form compound units of
structure, such as objects, arrays, and maps. The absence of a value is usually
denoted by a null type. There are type modifiers, like the union of two types.

The following schemas are currently supported:

* JSON Schema
* Avro
* BigQuery

In the future, it may be possible to support schemas from similar systems like
Parquet and Spark, or into various interactive data languages (IDL) like
Avro IDL.

### Representation of schemas
Currently, schemas are deserialized directly from their JSON counterparts into
Rust structs and enums using `serde_json`. Enums in Rust are similar to algebraic
data types in functional languages and support robust pattern matching. As such,
a common pattern is to abstract a schema into a type and a tag.

The type forms a set of symbols and the rules for producing a sequence of those
symbols. A simple type could be defined as follows:

```rust
enum Atom {
    Boolean,
    Integer
}

enum Type {
    Null,
    Atom(Atom),
    List(Vec<Type>)
}

// [null, true, [null, -1]]
let root = Type::List(vec![
    Type::Null,
    Type::Atom(Atom::Boolean),
    Type::List(vec![
        Type::Null,
        Type::Atom(Atom::Integer)
    ])
]);
```

While it is possible to generate a schema for a document tree where the ordering
of elements are fixed (by traversing the tree top-down, left-right), schema
validators often assert other properties about the data structure. We may be
interested in asserting the existence of names in a document; to support naming,
we associate each type with a tag.

A tag is attribute data associated with a type. A tag is used as a proxy in the
recursive definition of a type. Traversing a schema can be done by iterating
through all of the tags in order. Tags may also reference other parts of the
tree, which would typically not be possible by directly defining an recursive
enum.


```rust
enum Type {
    Atom,
    List(Vec<Tag>)
}

struct Tag {
    dtype: Type,
    name: String
}

let root = Tag {
    dtype: Type::List(vec![
        Tag { dtype: Type::Atom, name: "foo" },
        Tag { dtype: Type::Atom, name: "bar" },
    ]),
    name: "object"
};
```

By annotating this with the appropriate `serde` attributes, we are able to obtain
the following schema for free:

```json
{
    "name": "object",
    "type": [
        {"name": "foo", "type": "atom"},
        {"name": "bar", "type": "atom"}
    ]
}
```