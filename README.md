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

This tool is designed for generating new schemas from
[`mozilla-pipeline-schemas`](https://github.com/mozilla-services/mozilla-pipeline-schemas),
the canonical source of truth for JSON schemas in the Firefox Data Platform.

## Installation

```
cargo install jsonschema-transpiler
```

## Usage

```
jsonschema-transpiler 0.4.0
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
