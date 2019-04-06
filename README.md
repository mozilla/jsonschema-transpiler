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

This tool can handle many of the composite types seen in modern data processing
tools that support a SQL interface such as lists, structures, key-value
maps, and type-variants.

## Installation

```
cargo install --git https://github.com/acmiyaguchi/jsonschema-transpiler
```

## Usage

```
jsonschema-transpiler 0.2.0
Anthony Miyaguchi <amiyaguchi@mozilla.com>
A tool to transpile JSON Schema into schemas for data processing

USAGE:
    jsonschema-transpiler [OPTIONS] [FILE]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -t, --type <type>    The output schema format [default: avro]  [possible values: avro, bigquery]

ARGS:
    <FILE>    Sets the input file to use
```

JSON Schemas can be read from stdin or from a file.

### Examples usage:

```bash
# An object with a single, optional boolean field
$ schema='{"type": "object", "properties": {"foo": {"type": "boolean"}}}'

$ echo $schema | jq
{
  "type": "object",
  "properties": {
    "foo": {
      "type": "boolean"
    }
  }
}

$ echo $schema | jsonschema-transpiler --type avro
{
  "fields": [
    {
      "name": "foo",
      "type": [
        {
          "type": "null"
        },
        {
          "type": "boolean"
        }
      ]
    }
  ],
  "name": "root",
  "type": "record"
}

$ echo $schema | jsonschema-transpiler --type bigquery
{
  "fields": [
    {
      "mode": "NULLABLE",
      "name": "foo",
      "type": "BOOL"
    }
  ],
  "mode": "REQUIRED",
  "type": "RECORD"
}

# A record with an event payload containing a required timestamp and optional payload.
# The schema is written to and read from a file.
$ cat > test.schema.json << EOL
{
    "type": "object",
    "properties": {
        "events": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "timestamp": {"type": "integer"},
                    "payload": {"type": "object"}
                },
                "required": ["timestamp"]
            }
        }
    },
    "required": ["events"]
}
EOL

$ jsonschema-transpiler --type avro test.schema.json
{
  "fields": [
    {
      "name": "events",
      "type": {
        "items": {
          "fields": [
            {
              "name": "payload",
              "type": [
                {
                  "type": "null"
                },
                {
                  "type": "string"
                }
              ]
            },
            {
              "name": "timestamp",
              "type": {
                "type": "int"
              }
            }
          ],
          "name": "items",
          "namespace": "root.events",
          "type": "record"
        },
        "type": "array"
      }
    }
  ],
  "name": "root",
  "type": "record"
}

$ jsonschema-transpiler --type bigquery test.schema.json
{
  "fields": [
    {
      "fields": [
        {
          "mode": "NULLABLE",
          "name": "payload",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "timestamp",
          "type": "INT64"
        }
      ],
      "mode": "REPEATED",
      "name": "events",
      "type": "RECORD"
    }
  ],
  "mode": "REQUIRED",
  "type": "RECORD"
}

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